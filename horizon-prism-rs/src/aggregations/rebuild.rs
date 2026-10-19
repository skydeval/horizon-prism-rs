//! Scope maintenance (design-v2 §9.4, §9.5; scope-recency design §3.3).
//!
//! **The daily sweep** ([`Rebuild::sweep`]), run one hour before retention
//! and by `admin scope rebuild`, one run at a time:
//! 1. Under the write guard, clear the touched set and read `T0`.
//! 2. Compute the holds `J`: members; each member's earliest follows up to
//!    the follow cap; the subjects of members' likes and reposts of the last
//!    `scope.like_repost_days` days and of their replies and quotes of the
//!    last `scope.reply_quote_days` days; and the subjects, already in S, of
//!    members' active activity subscriptions.
//! 3. Upsert `J` with `justified_at = now()`, in batches under each DID's
//!    shared lock; a DID new to S gets a `scope` backfill. A DID held only by
//!    a subscription is refreshed, never inserted.
//! 4. Finish any `out_of_scope` purge left half-done, then purge the rows no
//!    hold justified since `T0` minus the grace, each through the guarded
//!    marker (`MarkerGuard::Unjustified`), 100 ms apart, at most
//!    `scope.sweep_max_purges` per run.
//! 5. Refresh the in-memory per-member follow counts.
//!
//! **Weekly**, by the scheduler: the §9.5 counts repair and `resync` jobs,
//! 1 DID/s, for in-scope DIDs whose last re-sync is older than 7 days (§5.7).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::policy::{
    REASON_FOLLOWED, REASON_INTERACTED, REASON_MEMBER, REASON_SUBSCRIBED, grows_on_follow, grows_on_interaction,
    holds_on_subscription,
};
use super::purge::{MarkerGuard, MarkerOutcome, PurgeError, Purger};
use super::scope_state::{PurgeKind, ScopeState};
use crate::config::UtcTimeOfDay;
use crate::db::locks::lock_dids_shared;
use crate::firehose::jobs::Depth;
use crate::private_tier::PrivateTier;

/// DIDs per upsert transaction (bounded advisory-lock count, §6.11.7).
pub const UPSERT_BATCH: usize = 500;
/// The weekly schedule is checked this often; the run itself is weekly.
pub const SCHEDULE_CHECK: Duration = Duration::from_secs(3600);
/// The daily sweep's schedule is checked this often, so a run starts
/// within minutes of its slot (scope-recency design §3.3).
pub const SWEEP_CHECK: Duration = Duration::from_secs(300);
/// The least time between two scheduled sweeps.
pub const SWEEP_MIN_GAP: TimeDelta = TimeDelta::hours(23);
/// How long a failed scheduled sweep waits before it is tried again.
pub const SWEEP_RETRY: TimeDelta = TimeDelta::hours(1);
/// The pause between two purges of one sweep.
pub const PURGE_PAUSE: Duration = Duration::from_millis(100);
/// Period of the weekly counts repair and re-syncs.
pub const WEEK: TimeDelta = TimeDelta::days(7);
/// `worker_state` key recording the last weekly run.
pub const KEY_LAST_REBUILD: &str = "last_scope_rebuild";
/// `worker_state` key recording the last scheduled sweep.
pub const KEY_LAST_SWEEP: &str = "last_scope_sweep";

/// Sweeps run, by outcome (`ok`, `failed`).
pub const METRIC_SWEEP_RUNS: &str = "scope_sweep_runs_total";
/// DIDs a sweep removed from S.
pub const METRIC_SWEEP_PURGED: &str = "scope_sweep_purged_total";
/// Candidates a sweep did not remove, by reason (`touched`, `member`,
/// `refreshed`, `followed`, `subscribed`, `busy`, `capped`).
pub const METRIC_SWEEP_SKIPPED: &str = "scope_sweep_skipped_total";
/// Rows of `scope_dids` after a sweep, each by its first hold (`member`,
/// `follow`, `interaction`, `subscription`, or `none` when only the grace
/// or the cap keeps it). Named apart from the sampler's unlabelled
/// `scope_dids_total`.
pub const METRIC_SCOPE_BY_HOLD: &str = "scope_dids_by_hold";

/// A DID and its reason bits.
type Held = (String, i16);

/// What a sweep did (or, for a dry run, would do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// DIDs some hold justifies.
    pub justified: usize,
    /// DIDs the sweep added to S.
    pub added: usize,
    /// Rows no hold justifies, past the grace: the purge candidates.
    pub candidates: usize,
    /// DIDs removed from S, half-done purges finished included.
    pub purged: usize,
    /// Candidates not removed, by reason.
    pub skipped: BTreeMap<&'static str, usize>,
    /// The candidates by the reason bits their rows last had.
    pub would_purge_by_reasons: BTreeMap<i16, usize>,
}

impl SweepReport {
    /// How many sweeps the candidates take at `max_purges` per sweep.
    #[must_use]
    pub fn sweeps_needed(&self, max_purges: u32) -> usize {
        self.candidates.div_ceil(usize::try_from(max_purges.max(1)).unwrap_or(usize::MAX))
    }

    fn skip(&mut self, reason: &'static str) {
        *self.skipped.entry(reason).or_insert(0) += 1;
        metrics::counter!(METRIC_SWEEP_SKIPPED, "reason" => reason).increment(1);
    }
}

/// The start of the most recent daily slot at or before `now`.
#[must_use]
pub fn most_recent_slot(now: DateTime<Utc>, slot_secs: u32) -> DateTime<Utc> {
    let midnight = now.date_naive().and_hms_opt(0, 0, 0).map_or(now, |t| t.and_utc());
    let today = midnight + TimeDelta::seconds(i64::from(slot_secs));
    if today <= now { today } else { today - TimeDelta::days(1) }
}

/// Whether the scheduled sweep is due (scope-recency design §3.3): the last
/// run was before the most recent slot and at least 23 hours ago.
#[must_use]
pub fn sweep_due(last: DateTime<Utc>, now: DateTime<Utc>, slot_secs: u32) -> bool {
    last < most_recent_slot(now, slot_secs) && now - last >= SWEEP_MIN_GAP
}

/// The scope maintenance runner.
#[derive(Clone)]
pub struct Rebuild {
    pool: PgPool,
    scope: ScopeState,
    purger: Purger,
    grace_days: u32,
    max_purges: u32,
    /// Seconds after midnight UTC of the daily slot.
    slot_secs: u32,
    /// One sweep at a time, scheduled or `admin scope rebuild`.
    run_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Rebuild {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, private: Arc<dyn PrivateTier>, grace_days: u32) -> Self {
        let purger = Purger::new(pool.clone(), scope.clone(), private);
        Self {
            pool,
            scope,
            purger,
            grace_days,
            max_purges: 5_000,
            // One hour before the default retention run at 04:00.
            slot_secs: 3 * 3600,
            run_lock: Arc::default(),
        }
    }

    /// Set `lists.sync_min_interval_secs` on the rebuild's purges.
    #[must_use]
    pub fn with_sync_min_interval_secs(mut self, secs: u32) -> Self {
        self.purger = self.purger.with_sync_min_interval_secs(secs);
        self
    }

    /// Set `scope.sweep_max_purges`.
    #[must_use]
    pub fn with_max_purges(mut self, max: u32) -> Self {
        self.max_purges = max.max(1);
        self
    }

    /// Run the daily sweep one hour before `retention.run_at_utc`.
    #[must_use]
    pub fn with_retention_at(mut self, at: UtcTimeOfDay) -> Self {
        self.slot_secs = (at.seconds_after_midnight() + 86_400 - 3600) % 86_400;
        self
    }

    #[must_use]
    pub fn max_purges(&self) -> u32 {
        self.max_purges
    }

    /// Step 2: DID → reason bits under the active policy, the windows read
    /// against `t0`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn compute(&self, t0: DateTime<Utc>) -> Result<BTreeMap<String, i16>, sqlx::Error> {
        let policy = self.scope.policy();
        let windows = self.scope.windows();
        let mut out: BTreeMap<String, i16> = BTreeMap::new();
        let members: Vec<String> = sqlx::query_scalar("SELECT did FROM members").fetch_all(&self.pool).await?;
        for m in members {
            *out.entry(m).or_insert(0) |= REASON_MEMBER;
        }
        if grows_on_follow(policy) {
            // Each member's earliest follows up to the cap (§9.4).
            let cap = i64::from(self.scope.max_follow_scope_per_member());
            let followed: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT subject_did FROM ( \
                   SELECT f.subject_did, row_number() OVER (PARTITION BY f.author_did ORDER BY f.created_at, f.uri) AS n \
                   FROM follows f JOIN members m ON m.did = f.author_did) t WHERE n <= $1",
            )
            .bind(cap)
            .fetch_all(&self.pool)
            .await?;
            for d in followed {
                *out.entry(d).or_insert(0) |= REASON_FOLLOWED;
            }
        }
        let likes_on = windows.like_repost_days > 0;
        let replies_on = windows.reply_quote_days > 0;
        if grows_on_interaction(policy) && (likes_on || replies_on) {
            // Members' replies and quotes come from their post references,
            // which exist in every public-indexing mode (public-indexing
            // design §4.1); `posts` holds none while it is off. Only fresh
            // interactions hold (scope-recency design §3.3).
            let like_cutoff = t0 - TimeDelta::days(i64::from(windows.like_repost_days));
            let reply_cutoff = t0 - TimeDelta::days(i64::from(windows.reply_quote_days));
            let interacted: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT d FROM ( \
                   SELECT split_part(l.subject_uri, '/', 3) AS d FROM likes l JOIN members m ON m.did = l.author_did \
                     WHERE $1 AND l.created_at >= $2 AND l.subject_uri LIKE 'at://%' \
                   UNION ALL SELECT split_part(r.subject_uri, '/', 3) FROM reposts r JOIN members m ON m.did = r.author_did \
                     WHERE $1 AND r.created_at >= $2 AND r.subject_uri LIKE 'at://%' \
                   UNION ALL SELECT x FROM member_post_refs p JOIN members m ON m.did = p.author_did, \
                     LATERAL unnest(ARRAY[p.reply_parent_did, p.reply_root_did, p.quote_did]) x \
                     WHERE $3 AND p.created_at >= $4 \
                 ) t WHERE d IS NOT NULL",
            )
            .bind(likes_on)
            .bind(like_cutoff)
            .bind(replies_on)
            .bind(reply_cutoff)
            .fetch_all(&self.pool)
            .await?;
            for d in interacted.into_iter().filter(|d| d.starts_with("did:")) {
                *out.entry(d).or_insert(0) |= REASON_INTERACTED;
            }
        }
        if holds_on_subscription(policy) {
            // A member's active activity subscription holds a subject already
            // in S, and never adds one (scope-recency design §3.6).
            let subscribed: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT a.subject_did FROM activity_subscriptions a \
                   JOIN members m ON m.did = a.subscriber_did \
                  WHERE EXISTS (SELECT 1 FROM follows f WHERE f.author_did = a.subscriber_did AND f.subject_did = a.subject_did) \
                    AND EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = a.subject_did)",
            )
            .fetch_all(&self.pool)
            .await?;
            for d in subscribed {
                *out.entry(d).or_insert(0) |= REASON_SUBSCRIBED;
            }
        }
        Ok(out)
    }

    /// Run one sweep (scope-recency design §3.3). A dry run computes and
    /// counts without writing, clearing nothing; a real run waits for any
    /// sweep already running.
    ///
    /// # Errors
    ///
    /// Database errors; purges already finished stay finished.
    pub async fn sweep(&self, dry_run: bool) -> Result<SweepReport, sqlx::Error> {
        if dry_run {
            return self.sweep_once(true).await;
        }
        let _one_at_a_time = self.run_lock.lock().await;
        let result = self.sweep_once(false).await;
        let outcome = if result.is_ok() { "ok" } else { "failed" };
        metrics::counter!(METRIC_SWEEP_RUNS, "outcome" => outcome).increment(1);
        if let Err(error) = &result {
            tracing::warn!(event = "scope_sweep_failed", %error, "scope sweep failed");
        }
        result
    }

    async fn sweep_once(&self, dry_run: bool) -> Result<SweepReport, sqlx::Error> {
        let started = std::time::Instant::now();
        // Step 1. Every member write that named a hold before the clear
        // committed before the guard was granted, so step 2 sees it.
        let t0: DateTime<Utc> = if dry_run {
            sqlx::query_scalar("SELECT now()").fetch_one(&self.pool).await?
        } else {
            let guard = self.scope.write_guard().await;
            self.scope.clear_touched();
            let t0 = sqlx::query_scalar("SELECT now()").fetch_one(&self.pool).await;
            drop(guard);
            t0?
        };
        let justified = self.compute(t0).await?;
        let cutoff = t0 - TimeDelta::days(i64::from(self.grace_days));
        let mut report = SweepReport {
            justified: justified.len(),
            ..SweepReport::default()
        };
        if dry_run {
            let current: HashSet<String> = sqlx::query_scalar("SELECT did FROM scope_dids").fetch_all(&self.pool).await?.into_iter().collect();
            report.added = justified.iter().filter(|(d, bits)| **bits != REASON_SUBSCRIBED && !current.contains(*d)).count();
            self.count_candidates(&mut report, &self.candidates(cutoff, &justified).await?);
            return Ok(report);
        }

        // Step 3, in batches under shared DID locks. A DID under an
        // `account_deleted` purge is left out, so a member's follow of a
        // deleted account does not put it back while its purge runs.
        let deleting: HashSet<String> = {
            let sets = self.scope.read_guard().await;
            sets.purging.iter().filter(|(_, k)| **k == PurgeKind::AccountDeleted).map(|(d, _)| d.clone()).collect()
        };
        let (holds_only, upserts): (Vec<Held>, Vec<Held>) = justified
            .iter()
            .filter(|(d, _)| !deleting.contains(*d))
            .map(|(d, bits)| (d.clone(), *bits))
            .partition(|(_, bits)| *bits == REASON_SUBSCRIBED);
        for chunk in upserts.chunks(UPSERT_BATCH) {
            let added = self.upsert_batch(chunk).await?;
            report.added += added.len();
            if !added.is_empty() {
                let mut sets = self.scope.write_guard().await;
                for d in added {
                    sets.scope.insert(d);
                }
            }
        }
        for chunk in holds_only.chunks(UPSERT_BATCH) {
            self.refresh_batch(chunk).await?;
        }

        // Step 4: half-done purges first, then the candidates, each
        // independently.
        let mut purges: u32 = 0;
        let stuck: Vec<String> = sqlx::query_scalar("SELECT did FROM purging_dids WHERE kind = 'out_of_scope' ORDER BY started_at, did")
            .fetch_all(&self.pool)
            .await?;
        for did in stuck {
            if purges >= self.max_purges {
                report.skip("capped");
                continue;
            }
            if purges > 0 {
                tokio::time::sleep(PURGE_PAUSE).await;
            }
            purges += 1;
            match self.purger.purge(&did, PurgeKind::OutOfScope).await {
                Ok(()) => report.purged += 1,
                Err(PurgeError::Busy(_)) => report.skip("busy"),
                Err(error) => tracing::warn!(event = "scope_purge_failed", did, %error, "finishing an out-of-scope purge failed"),
            }
        }
        let candidates = self.candidates(cutoff, &justified).await?;
        self.count_candidates(&mut report, &candidates);
        #[cfg(test)]
        crate::test_support::hooks::pause_in(crate::test_support::hooks::SWEEP_AFTER_COMPUTE, &self.pool, "sweep").await;
        let guard = MarkerGuard::Unjustified { cutoff };
        for (did, _) in candidates {
            if purges >= self.max_purges {
                report.skip("capped");
                continue;
            }
            if purges > 0 {
                tokio::time::sleep(PURGE_PAUSE).await;
            }
            match self.purger.purge_guarded(&did, PurgeKind::OutOfScope, &guard).await {
                Ok(MarkerOutcome::Marked) => {
                    purges += 1;
                    report.purged += 1;
                }
                Ok(MarkerOutcome::Held(reason)) => report.skip(reason),
                Ok(MarkerOutcome::Skipped) => report.skip("refreshed"),
                // An earlier sweep's purge that failed after its marker: the
                // DID already left S there, so it is finished unguarded.
                Err(PurgeError::Busy("out_of_scope")) => {
                    purges += 1;
                    match self.purger.purge(&did, PurgeKind::OutOfScope).await {
                        Ok(()) => report.purged += 1,
                        Err(error) => tracing::warn!(event = "scope_purge_failed", did, %error, "finishing an out-of-scope purge failed"),
                    }
                }
                // Another kind of purge: never waited for; the next sweep
                // sees the DID again.
                Err(PurgeError::Busy(_)) => report.skip("busy"),
                Err(error) => {
                    purges += 1;
                    tracing::warn!(event = "scope_purge_failed", did, %error, "out-of-scope purge failed");
                }
            }
        }
        if report.purged > 0 {
            metrics::counter!(METRIC_SWEEP_PURGED).increment(u64::try_from(report.purged).unwrap_or(u64::MAX));
        }

        // Step 5, and the gauges.
        self.reload_follow_counts().await?;
        self.set_gauges(&justified).await?;
        tracing::info!(
            event = "scope_sweep",
            justified = report.justified,
            added = report.added,
            candidates = report.candidates,
            purged = report.purged,
            skipped = ?report.skipped,
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "scope sweep finished"
        );
        Ok(report)
    }

    fn count_candidates(&self, report: &mut SweepReport, candidates: &[(String, i16)]) {
        report.candidates = candidates.len();
        for (_, bits) in candidates {
            *report.would_purge_by_reasons.entry(*bits).or_insert(0) += 1;
        }
    }

    async fn upsert_batch(&self, chunk: &[(String, i16)]) -> Result<Vec<String>, sqlx::Error> {
        let dids: Vec<String> = chunk.iter().map(|(d, _)| d.clone()).collect();
        let reasons: Vec<i16> = chunk.iter().map(|(_, r)| *r).collect();
        let guard = self.scope.read_guard().await;
        let mut tx = self.pool.begin().await?;
        // Every write path that touches scope_dids takes the subject's
        // shared DID lock before inserting (§6.11.7 step 2).
        lock_dids_shared(&mut tx, &dids).await?;
        let rows: Vec<(String, bool)> = sqlx::query_as(
            "INSERT INTO scope_dids (did, reasons, justified_at) \
             SELECT d, r, now() FROM unnest($1::text[], $2::int2[]) AS t(d, r) \
             ON CONFLICT (did) DO UPDATE SET reasons = EXCLUDED.reasons, justified_at = now() \
             RETURNING did, (xmax = 0)",
        )
        .bind(&dids)
        .bind(&reasons)
        .fetch_all(&mut *tx)
        .await?;
        let added: Vec<String> = rows.into_iter().filter(|(_, new)| *new).map(|(d, _)| d).collect();
        crate::firehose::jobs::enqueue_many(&mut tx, &added, Depth::Scope).await?;
        tx.commit().await?;
        drop(guard);
        Ok(added)
    }

    /// DIDs held only by a subscription: their existing rows are refreshed;
    /// a row that is gone stays gone (scope-recency design §3.6).
    async fn refresh_batch(&self, chunk: &[(String, i16)]) -> Result<(), sqlx::Error> {
        let dids: Vec<String> = chunk.iter().map(|(d, _)| d.clone()).collect();
        let guard = self.scope.read_guard().await;
        let mut tx = self.pool.begin().await?;
        lock_dids_shared(&mut tx, &dids).await?;
        sqlx::query("UPDATE scope_dids SET reasons = $2, justified_at = now() WHERE did = ANY($1)")
            .bind(&dids)
            .bind(REASON_SUBSCRIBED)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        drop(guard);
        Ok(())
    }

    /// Rows not justified since `cutoff` and not in `justified`, members
    /// excluded, with the reason bits they last had, in DID order.
    async fn candidates(&self, cutoff: DateTime<Utc>, justified: &BTreeMap<String, i16>) -> Result<Vec<(String, i16)>, sqlx::Error> {
        let rows: Vec<(String, i16)> = sqlx::query_as(
            "SELECT s.did, s.reasons FROM scope_dids s WHERE s.justified_at < $1 \
             AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did = s.did) ORDER BY s.did",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().filter(|(d, _)| !justified.contains_key(d)).collect())
    }

    /// `scope_dids_by_hold`: each row by its first hold, and `none` for the
    /// rows only the grace or the cap keeps.
    async fn set_gauges(&self, justified: &BTreeMap<String, i16>) -> Result<(), sqlx::Error> {
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM scope_dids").fetch_one(&self.pool).await?;
        let mut by_hold: BTreeMap<&'static str, i64> = BTreeMap::new();
        for bits in justified.values() {
            let hold = [
                (REASON_MEMBER, "member"),
                (REASON_FOLLOWED, "follow"),
                (REASON_INTERACTED, "interaction"),
                (REASON_SUBSCRIBED, "subscription"),
            ]
            .into_iter()
            .find(|(bit, _)| bits & bit != 0)
            .map_or("none", |(_, name)| name);
            *by_hold.entry(hold).or_insert(0) += 1;
        }
        let held: i64 = by_hold.values().sum();
        for hold in ["member", "follow", "interaction", "subscription"] {
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!(METRIC_SCOPE_BY_HOLD, "hold" => hold).set(by_hold.get(hold).copied().unwrap_or(0) as f64);
        }
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_SCOPE_BY_HOLD, "hold" => "none").set((rows - held).max(0) as f64);
        Ok(())
    }

    /// §9.5 repair; see [`repair_counts`].
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn repair_counts(&self) -> Result<(), sqlx::Error> {
        repair_counts(&self.pool).await
    }

    async fn reload_follow_counts(&self) -> Result<(), sqlx::Error> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT f.author_did, count(*) FROM follows f \
             WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = f.author_did) GROUP BY f.author_did",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut sets = self.scope.write_guard().await;
        sets.member_follows = rows
            .into_iter()
            .map(|(d, n)| (d, u32::try_from(n).unwrap_or(u32::MAX)))
            .collect();
        Ok(())
    }

    /// Enqueue `resync` for in-scope DIDs not re-synced for 7 days, spaced
    /// 1 s apart. A DID's last re-sync is its `sync_state.last_resync_at`
    /// (tracked DIDs and DIDs in S) or its last completed
    /// `member`/`scope`/`resync` job.
    /// Jobs already queued or running are left alone.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn enqueue_stale_resyncs(&self) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query(
            "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, updated_at) \
             SELECT did, 'resync', 'queued', 0, now() + make_interval(secs => (row_number() OVER (ORDER BY did))::float8), now() \
             FROM scope_dids s \
             WHERE NOT EXISTS (SELECT 1 FROM sync_state y WHERE y.did = s.did AND y.last_resync_at > now() - interval '7 days') \
               AND NOT EXISTS (SELECT 1 FROM backfill_jobs j WHERE j.did = s.did \
                   AND j.depth IN ('member', 'scope', 'resync') AND j.state = 'done' \
                   AND j.updated_at > now() - interval '7 days') \
               AND NOT EXISTS (SELECT 1 FROM purging_dids p WHERE p.did = s.did) \
             ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
               not_before = EXCLUDED.not_before, last_error = NULL, updated_at = now() \
             WHERE backfill_jobs.state NOT IN ('queued', 'running')",
        )
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Every 5 minutes; runs the sweep when it is due (persisted, so a
    /// restart neither skips nor repeats a day). A failed run is tried
    /// again an hour later.
    #[must_use]
    pub fn spawn_daily(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SWEEP_CHECK);
            let mut retry_at: Option<DateTime<Utc>> = None;
            loop {
                tick.tick().await;
                if retry_at.is_some_and(|at| Utc::now() < at) {
                    continue;
                }
                match self.sweep_is_due().await {
                    Ok(true) => match self.sweep(false).await {
                        Ok(_) => {
                            retry_at = None;
                            if let Err(error) = self.mark_ran(KEY_LAST_SWEEP).await {
                                tracing::warn!(event = "scope_sweep_mark_failed", %error, "recording the sweep time failed");
                            }
                        }
                        Err(_) => retry_at = Some(Utc::now() + SWEEP_RETRY),
                    },
                    Ok(false) => {}
                    Err(error) => tracing::warn!(event = "scope_sweep_schedule_failed", %error, "schedule check failed"),
                }
            }
        })
    }

    /// Whether the scheduled sweep is due. With no record (a fresh install
    /// or the first start of this version) it records now instead, so the
    /// first sweep runs at least 23 hours later (scope-recency design §3.3).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn sweep_is_due(&self) -> Result<bool, sqlx::Error> {
        match self.last_ran(KEY_LAST_SWEEP).await? {
            Some(last) => Ok(sweep_due(last, Utc::now(), self.slot_secs)),
            None => {
                self.mark_ran(KEY_LAST_SWEEP).await?;
                Ok(false)
            }
        }
    }

    /// Hourly check; runs the counts repair and stale re-syncs when the last
    /// weekly run is a week old (persisted, so restarts do not postpone it).
    #[must_use]
    pub fn spawn_weekly(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SCHEDULE_CHECK);
            loop {
                tick.tick().await;
                match self.due().await {
                    Ok(true) => {
                        if let Err(error) = self.repair_counts().await {
                            tracing::warn!(event = "scope_rebuild_failed", %error, "weekly counts repair failed");
                            continue;
                        }
                        let resyncs = match self.enqueue_stale_resyncs().await {
                            Ok(n) => {
                                tracing::info!(event = "weekly_resync_enqueued", jobs = n, "stale re-syncs enqueued");
                                n
                            }
                            Err(error) => {
                                tracing::warn!(event = "weekly_resync_failed", %error, "enqueueing re-syncs failed");
                                0
                            }
                        };
                        tracing::info!(event = "scope_rebuild", resync_enqueued = resyncs, "weekly counts repair and re-syncs finished");
                        if let Err(error) = self.mark_ran(KEY_LAST_REBUILD).await {
                            tracing::warn!(event = "scope_rebuild_mark_failed", %error, "recording the rebuild time failed");
                        }
                    }
                    Ok(false) => {}
                    Err(error) => tracing::warn!(event = "scope_rebuild_schedule_failed", %error, "schedule check failed"),
                }
            }
        })
    }

    async fn due(&self) -> Result<bool, sqlx::Error> {
        match self.last_ran(KEY_LAST_REBUILD).await? {
            Some(t) => Ok(Utc::now() - t >= WEEK),
            None => {
                // First start: schedule a week from now instead of running
                // immediately.
                self.mark_ran(KEY_LAST_REBUILD).await?;
                Ok(false)
            }
        }
    }

    async fn last_ran(&self, key: &str) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar("SELECT (value #>> '{}')::timestamptz FROM worker_state WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
    }

    async fn mark_ran(&self, key: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb(now()::text), now()) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
        )
        .bind(key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// §9.5 repair: recompute both count tables from the base tables, likes
/// and reposts by account (#727). The
/// share-row-exclusive locks hold count-trigger writers for the duration,
/// so no increment is lost or doubled. A function of the pool, so the end
/// of a public-indexing fill runs it too (public-indexing design §8.5).
///
/// # Errors
///
/// Database errors.
pub async fn repair_counts(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("LOCK TABLE post_counts, actor_counts IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM post_counts").execute(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO post_counts (post_uri, like_count, repost_count, reply_count, quote_count) \
         SELECT u, sum(l)::int, sum(r)::int, sum(p)::int, sum(q)::int FROM ( \
           SELECT subject_uri AS u, count(DISTINCT author_did) AS l, 0 AS r, 0 AS p, 0 AS q FROM likes GROUP BY subject_uri \
           UNION ALL SELECT subject_uri, 0, count(DISTINCT author_did), 0, 0 FROM reposts GROUP BY subject_uri \
           UNION ALL SELECT reply_parent_uri, 0, 0, 1, 0 FROM posts WHERE reply_parent_uri IS NOT NULL \
           UNION ALL SELECT quote_uri, 0, 0, 0, 1 FROM posts WHERE quote_uri IS NOT NULL \
         ) t GROUP BY u",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM actor_counts").execute(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO actor_counts (did, followers_count, follows_count, posts_count) \
         SELECT d, sum(fr)::int, sum(fs)::int, sum(p)::int FROM ( \
           SELECT subject_did AS d, 1 AS fr, 0 AS fs, 0 AS p FROM follows \
           UNION ALL SELECT author_did, 0, 1, 0 FROM follows \
           UNION ALL SELECT author_did, 0, 0, 1 FROM posts \
         ) t GROUP BY d",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

#[cfg(test)]
#[path = "sweep_tests.rs"]
mod sweep_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    const MEMBER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const FOLLOWED: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const LIKED: &str = "did:plc:cccccccccccccccccccccccc";
    const STALE: &str = "did:plc:dddddddddddddddddddddddd";

    async fn exec(pool: &PgPool, sql: &str) {
        sqlx::query(sql).execute(pool).await.expect(sql);
    }

    /// The weekly run keeps the §9.5 repair and the stale re-syncs.
    #[tokio::test]
    async fn the_weekly_run_repairs_counts_and_enqueues_resyncs() {
        let Some(db) = fresh_db("scope_weekly").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES \
                 ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.like/1', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', \
                  'at://did:plc:cccccccccccccccccccccccc/app.bsky.feed.post/1', '3k', now())").await;
        exec(p, "UPDATE post_counts SET like_count = 99").await;
        for d in [MEMBER, FOLLOWED, LIKED] {
            exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{d}', 1)")).await;
        }
        let scope = ScopeState::new(crate::aggregations::ScopeSets::default(), crate::config::ScopePolicy::MembersAndOneHop, 10);
        let rebuild = Rebuild::new(p.clone(), scope, Arc::new(crate::private_tier::DisabledPrivateTier), 0);
        rebuild.repair_counts().await.expect("repair");
        let likes: i32 = sqlx::query_scalar("SELECT like_count FROM post_counts WHERE post_uri = $1")
            .bind(format!("at://{LIKED}/app.bsky.feed.post/1"))
            .fetch_one(p)
            .await
            .expect("post_counts");
        assert_eq!(likes, 1);
        assert_eq!(rebuild.enqueue_stale_resyncs().await.expect("resync"), 3);
        db.drop_db().await;
    }

    /// T-17: migration 0021 fills the references from members' stored
    /// posts, and the rebuild reads from them the DIDs it read from `posts`.
    #[tokio::test]
    async fn migration_0021_refs_match_the_old_query() {
        let Some(db) = crate::test_support::empty_db("migration_0021").await else {
            return;
        };
        let p = &db.pool;
        let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
        let at = migrations.iter().position(|m| m.version == 21).expect("0021");
        for m in &migrations[..at] {
            sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
        }
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin')").await;
        let post = |author: &str, rkey: &str, parent: Option<&str>, root: Option<&str>, quote: Option<&str>| {
            let q = |v: Option<&str>| v.map_or("NULL".to_string(), |u| format!("'{u}'"));
            format!(
                "INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at, reply_parent_uri, reply_root_uri, quote_uri) VALUES \
                 ('at://{author}/app.bsky.feed.post/{rkey}', '{author}', '{rkey}', 'c', '3k{rkey}', '{{}}', now(), {}, {}, {})",
                q(parent),
                q(root),
                q(quote)
            )
        };
        let liked = format!("at://{LIKED}/app.bsky.feed.post/1");
        let followed = format!("at://{FOLLOWED}/app.bsky.feed.post/1");
        let stale = format!("at://{STALE}/app.bsky.feed.post/1");
        for sql in [
            post(MEMBER, "1", Some(&liked), Some(&followed), None),
            post(MEMBER, "2", None, None, Some(&stale)),
            post(MEMBER, "3", None, None, None),
            // A handle in a reference is kept as written, as before.
            post(MEMBER, "4", Some("at://alice.example/app.bsky.feed.post/1"), None, None),
            // Not a member's: no reference.
            post(STALE, "5", Some(&liked), None, None),
        ] {
            exec(p, &sql).await;
        }
        let old: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT split_part(u, '/', 3) FROM ( \
               SELECT p.reply_parent_uri AS u FROM posts p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.reply_root_uri FROM posts p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.quote_uri FROM posts p JOIN members m ON m.did = p.author_did \
             ) t WHERE u LIKE 'at://%' ORDER BY 1",
        )
        .fetch_all(p)
        .await
        .expect("old");
        sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0021 applies over existing rows");
        let new: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT d FROM ( \
               SELECT p.reply_parent_did AS d FROM member_post_refs p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.reply_root_did FROM member_post_refs p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.quote_did FROM member_post_refs p JOIN members m ON m.did = p.author_did \
             ) t WHERE d IS NOT NULL ORDER BY 1",
        )
        .fetch_all(p)
        .await
        .expect("new");
        assert_eq!(new, old);
        assert_eq!(old.len(), 4);
        let refs: Vec<(String, String)> = sqlx::query_as("SELECT uri, rev FROM member_post_refs ORDER BY uri").fetch_all(p).await.expect("refs");
        assert_eq!(refs.len(), 4);
        assert!(refs.iter().all(|(u, r)| u.starts_with(&format!("at://{MEMBER}/")) && r.starts_with("3k")));
        db.drop_db().await;
    }

    /// #727: migration 0029 recounts the likes and reposts the old triggers
    /// counted by record, by account, and its triggers keep them so.
    #[tokio::test]
    async fn migration_0029_recounts_by_account() {
        let Some(db) = crate::test_support::empty_db("migration_0029").await else {
            return;
        };
        let p = &db.pool;
        let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
        let at = migrations.iter().position(|m| m.version == 29).expect("0029");
        for m in &migrations[..at] {
            sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
        }
        let liked = format!("at://{LIKED}/app.bsky.feed.post/1");
        let other = format!("at://{LIKED}/app.bsky.feed.post/2");
        for (table, n) in [("likes", 3), ("reposts", 2)] {
            for i in 0..n {
                exec(p, &format!(
                    "INSERT INTO {table} (uri, author_did, subject_uri, rev, created_at) VALUES \
                     ('at://{MEMBER}/{table}/{i}', '{MEMBER}', '{liked}', '3k', now())"
                ))
                .await;
            }
            exec(p, &format!(
                "INSERT INTO {table} (uri, author_did, subject_uri, rev, created_at) VALUES \
                 ('at://{FOLLOWED}/{table}/1', '{FOLLOWED}', '{liked}', '3k', now()), \
                 ('at://{FOLLOWED}/{table}/2', '{FOLLOWED}', '{other}', '3k', now())"
            ))
            .await;
        }
        let counts = || async {
            let rows: Vec<(String, i32, i32)> = sqlx::query_as("SELECT post_uri, like_count, repost_count FROM post_counts ORDER BY post_uri")
                .fetch_all(p)
                .await
                .expect("counts");
            rows
        };
        assert_eq!(counts().await, [(liked.clone(), 4, 3), (other.clone(), 1, 1)], "the old triggers count records");
        // A row the old triggers never wrote is written too.
        exec(p, &format!("DELETE FROM post_counts WHERE post_uri = '{other}'")).await;
        sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0029 applies over existing rows");
        assert_eq!(counts().await, [(liked.clone(), 2, 2), (other.clone(), 1, 1)], "by account");
        exec(p, &format!("DELETE FROM likes WHERE author_did = '{MEMBER}'")).await;
        assert_eq!(counts().await, [(liked, 1, 2), (other, 1, 1)], "one statement takes three of one account's likes");
        db.drop_db().await;
    }
}
