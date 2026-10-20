//! The never-ready expiry (membership design §3.2; design-v4 §II-9.2).
//!
//! An `auto_enroll` member whose `first_ready_at` is still NULL
//! `community.never_ready_expiry_days` after `added_at` is removed by the
//! `member_removed` purge. Admin-added members never expire, and a member
//! that was ready once never does either. `0` turns the expiry off.
//!
//! An hourly task in `serve` selects the candidates and purges each under
//! [`MarkerGuard::Expiry`]: the marker deletes the `members` row only while
//! the condition still holds, inside its own transaction, under the write
//! guard and the DID's exclusive lock. A member that became ready, or was
//! removed and added again by the operator, between the select and the
//! marker is [`MarkerOutcome::Skipped`] and nothing of it changes.
//!
//! The window is a live setting (admin-ui design §8.5): each run reads it,
//! so a change applies at the next run. Expiry is not exclusion: no denial
//! is written, and the DID may enroll again.

use std::time::Duration;

use chrono::Utc;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::purge::{MarkerGuard, MarkerOutcome, PurgeError, Purger};
use super::scope_state::PurgeKind;
use crate::admin::settings::LiveSettings;

/// Members removed by the expiry.
pub const METRIC_EXPIRED: &str = "never_ready_expired_total";
/// How often the task runs.
pub const EXPIRY_EVERY: Duration = Duration::from_secs(3600);
/// The first run, after `serve` starts: interrupted purges resume first.
const FIRST_RUN_AFTER: Duration = Duration::from_secs(60);
/// Between two purges of one run.
const PURGE_PAUSE: Duration = Duration::from_millis(100);

/// What one run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpiryReport {
    /// The window read for this run.
    pub days: u32,
    /// Members the select found.
    pub candidates: usize,
    /// Removed: the guarded marker committed.
    pub expired: usize,
    /// The condition no longer held at the marker (ready since, or
    /// re-added by the operator): nothing changed.
    pub skipped: usize,
    /// Another purge of the DID was running; the next run sees it again
    /// if it is still a candidate.
    pub busy: usize,
    /// Database errors. A purge that failed after its marker resumes at
    /// the next start, as every purge does.
    pub failed: usize,
}

/// The expiry task. Cheap to clone.
#[derive(Clone)]
pub struct NeverReadyExpiry {
    pool: PgPool,
    purger: Purger,
    live: LiveSettings,
}

impl NeverReadyExpiry {
    #[must_use]
    pub fn new(pool: PgPool, purger: Purger, live: LiveSettings) -> Self {
        Self { pool, purger, live }
    }

    /// One run. `None` when the expiry is off (`never_ready_expiry_days =
    /// 0`): nothing is read.
    ///
    /// # Errors
    ///
    /// The candidates' select failed. A purge's own error is counted in
    /// the report and logged, and the run goes on.
    pub async fn run_once(&self) -> Result<Option<ExpiryReport>, sqlx::Error> {
        let days = self.live.load().never_ready_expiry_days;
        if days == 0 {
            return Ok(None);
        }
        let cutoff = Utc::now() - chrono::Duration::days(i64::from(days));
        let candidates: Vec<String> = sqlx::query_scalar(
            "SELECT did FROM members WHERE source = 'auto_enroll' AND first_ready_at IS NULL AND added_at < $1 \
             ORDER BY added_at, did",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        let mut report = ExpiryReport { days, candidates: candidates.len(), ..ExpiryReport::default() };
        #[cfg(test)]
        crate::test_support::hooks::pause_in(crate::test_support::hooks::EXPIRY_AFTER_SELECT, &self.pool, "expiry").await;
        let guard = MarkerGuard::Expiry { cutoff };
        for (i, did) in candidates.iter().enumerate() {
            if i > 0 {
                tokio::time::sleep(PURGE_PAUSE).await;
            }
            match self.expire(did, &guard).await {
                Ok(MarkerOutcome::Marked) => report.expired += 1,
                Ok(MarkerOutcome::Skipped | MarkerOutcome::Held(_)) => report.skipped += 1,
                Err(PurgeError::Busy(_)) => report.busy += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(event = "never_ready_expiry_failed", did, %error, "expiring a never-ready member failed");
                }
            }
        }
        tracing::info!(
            event = "never_ready_expiry",
            days,
            candidates = report.candidates,
            expired = report.expired,
            skipped = report.skipped,
            busy = report.busy,
            failed = report.failed,
            "never-ready expiry ran"
        );
        Ok(Some(report))
    }

    /// The guarded purge of one candidate, as [`Purger::purge_guarded`]
    /// runs it, with the expiry counted and logged once its marker
    /// committed (the member is gone from then on, even if a later step
    /// fails and resumes at the next start).
    async fn expire(&self, did: &str, guard: &MarkerGuard) -> Result<MarkerOutcome, PurgeError> {
        let outcome = self.purger.marker_guarded(did, PurgeKind::MemberRemoved, guard).await?;
        if outcome != MarkerOutcome::Marked {
            return Ok(outcome);
        }
        metrics::counter!(METRIC_EXPIRED).increment(1);
        tracing::info!(event = "member_expired", did, "auto-enrolled member never became ready; removed");
        self.purger.run_batches(did, PurgeKind::MemberRemoved).await?;
        self.purger.end(did).await?;
        Ok(MarkerOutcome::Marked)
    }

    /// A minute after the start, then hourly. Off (`0`), a run reads
    /// nothing; turning it on applies at the next run.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + FIRST_RUN_AFTER, EXPIRY_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(error) = self.run_once().await {
                    tracing::warn!(event = "never_ready_expiry_failed", %error, "selecting never-ready members failed");
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregations::membership::{AddOutcome, add_member};
    use crate::aggregations::{ScopeSets, ScopeState};
    use crate::config::ScopePolicy;
    use crate::private_tier::{KryphocronPrivateTier, PrivateTier};
    use crate::subscriptions::references::Budget;
    use crate::test_support::{fresh_db, hooks, series};

    const BUDGET: Budget = Budget { max_references: 25, max_owners: 10 };
    const EXPIRED: &str = "did:plc:expirednever000000000000";
    const READY_ONCE: &str = "did:plc:readyonce0000000000000000";
    const ADMIN: &str = "did:plc:adminnever00000000000000";
    const RECENT: &str = "did:plc:recentnever0000000000000";
    const RACE_READY: &str = "did:plc:raceready0000000000000000";
    const RACE_READD: &str = "did:plc:racereadd0000000000000000";

    struct T {
        pool: PgPool,
        scope: ScopeState,
        private: Arc<dyn PrivateTier>,
        live: LiveSettings,
        expiry: NeverReadyExpiry,
    }

    impl T {
        fn new(pool: &PgPool) -> Self {
            let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
            let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
            let purger = Purger::new(pool.clone(), scope.clone(), Arc::clone(&private));
            let live = LiveSettings::default();
            let expiry = NeverReadyExpiry::new(pool.clone(), purger, live.clone());
            Self { pool: pool.clone(), scope, private, live, expiry }
        }

        /// A member of `source`, added `days` days ago, ready once or not,
        /// with a bookmark and a mute of its own.
        async fn member(&self, did: &str, source: &str, days: i32, ready: bool) {
            let added = add_member(&self.pool, &self.scope, &self.private, did, source, BUDGET).await.expect("add");
            assert_eq!(added, AddOutcome::Added { became_owner: false });
            sqlx::query(
                "UPDATE members SET added_at = now() - make_interval(days => $2), first_ready_at = CASE WHEN $3 THEN now() END WHERE did = $1",
            )
            .bind(did)
            .bind(days)
            .bind(ready)
            .execute(&self.pool)
            .await
            .expect("age");
            for sql in [
                "INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ($1, 'at://x/app.bsky.feed.post/1', 'c')",
                "INSERT INTO mutes (viewer_did, subject_did) VALUES ($1, 'did:plc:x')",
            ] {
                sqlx::query(sql).bind(did).execute(&self.pool).await.expect(sql);
            }
        }

        async fn count(&self, sql: &str, did: &str) -> i64 {
            sqlx::query_scalar(sql).bind(did).fetch_one(&self.pool).await.expect(sql)
        }

        async fn is_member(&self, did: &str) -> bool {
            self.count("SELECT count(*) FROM members WHERE did = $1", did).await == 1
        }

        /// Kept whole: a member, not purging, in memory, its rows there.
        async fn kept(&self, did: &str) {
            assert!(self.is_member(did).await, "{did}: member");
            assert_eq!(self.count("SELECT count(*) FROM purging_dids WHERE did = $1", did).await, 0, "{did}: purging");
            assert!(self.scope.snapshot().await.is_member(did), "{did}: in memory");
            for table in ["bookmarks", "mutes"] {
                let n = self.count(&format!("SELECT count(*) FROM {table} WHERE viewer_did = $1"), did).await;
                assert_eq!(n, 1, "{did}: {table}");
            }
        }

        async fn generations(&self, did: &str) -> Vec<(String, i64)> {
            sqlx::query_as("SELECT depth, generation FROM backfill_jobs WHERE did = $1 ORDER BY depth")
                .bind(did)
                .fetch_all(&self.pool)
                .await
                .expect("jobs")
        }
    }

    /// T-M7 (§3.2): the task removes an `auto_enroll` member never ready
    /// past the window, with its viewer-private rows, and counts it; it
    /// keeps one ready once, an `admin` one and a recent one; `0` turns it
    /// off, read live at each run.
    #[tokio::test]
    async fn t_m7_the_task_expires_only_never_ready_auto_enrolled_members() {
        let Some(db) = fresh_db("membership_t_m7").await else {
            return;
        };
        let t = T::new(&db.pool);
        t.member(EXPIRED, "auto_enroll", 8, false).await;
        t.member(READY_ONCE, "auto_enroll", 8, true).await;
        t.member(ADMIN, "admin", 30, false).await;
        t.member(RECENT, "auto_enroll", 6, false).await;

        // Off: nothing is read or removed.
        t.live.update(|v| v.never_ready_expiry_days = 0);
        assert_eq!(t.expiry.run_once().await.expect("run"), None);
        assert!(t.is_member(EXPIRED).await);

        // On again: the next run applies the window.
        t.live.update(|v| v.never_ready_expiry_days = 7);
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let report = t.expiry.run_once().await.expect("run").expect("on");
        assert_eq!(report, ExpiryReport { days: 7, candidates: 1, expired: 1, ..ExpiryReport::default() });
        assert_eq!(series(&handle.render(), METRIC_EXPIRED, &[]), Some(1.0));
        assert!(!t.is_member(EXPIRED).await);
        assert_eq!(t.count("SELECT count(*) FROM purging_dids WHERE did = $1", EXPIRED).await, 0, "the purge ran to its end");
        assert!(!t.scope.snapshot().await.is_member(EXPIRED));
        for table in ["bookmarks", "mutes"] {
            let n = t.count(&format!("SELECT count(*) FROM {table} WHERE viewer_did = $1"), EXPIRED).await;
            assert_eq!(n, 0, "{table}: the member's viewer-private rows go");
        }
        assert_eq!(t.count("SELECT count(*) FROM member_denials WHERE did = $1", EXPIRED).await, 0, "expiry is not exclusion");
        for did in [READY_ONCE, ADMIN, RECENT] {
            t.kept(did).await;
        }

        // A shorter window reaches the recent one, live.
        t.live.update(|v| v.never_ready_expiry_days = 5);
        let report = t.expiry.run_once().await.expect("run").expect("on");
        assert_eq!((report.candidates, report.expired), (1, 1));
        assert!(!t.is_member(RECENT).await);
        for did in [READY_ONCE, ADMIN] {
            t.kept(did).await;
        }
        db.drop_db().await;
    }

    /// T-M7 races (§3.2): between the select and the marker, one candidate
    /// becomes ready and another is removed and added again as `admin`.
    /// Both are `Skipped`: kept, no `purging_dids` row, bookmarks and mutes
    /// kept, no job's generation moved.
    #[tokio::test]
    async fn t_m7_races_between_the_select_and_the_marker_keep_the_member() {
        let Some(db) = fresh_db("membership_t_m7_races").await else {
            return;
        };
        let t = T::new(&db.pool);
        t.member(RACE_READY, "auto_enroll", 8, false).await;
        t.member(RACE_READD, "auto_enroll", 8, false).await;
        let held = hooks::arm(hooks::EXPIRY_AFTER_SELECT, &format!("{}/expiry", db.name()));
        let run = tokio::spawn({
            let expiry = t.expiry.clone();
            async move { expiry.run_once().await }
        });
        held.reached().await;
        // Ready since the select.
        sqlx::query("UPDATE members SET first_ready_at = now(), ready_at = now() WHERE did = $1")
            .bind(RACE_READY)
            .execute(&t.pool)
            .await
            .expect("ready");
        // Removed and added again by the operator: the new row is `admin`,
        // added now. The re-add's own rows are written afresh.
        let purger = Purger::new(t.pool.clone(), t.scope.clone(), Arc::clone(&t.private));
        purger.purge(RACE_READD, PurgeKind::MemberRemoved).await.expect("remove");
        assert_eq!(
            add_member(&t.pool, &t.scope, &t.private, RACE_READD, "admin", BUDGET).await.expect("re-add"),
            AddOutcome::Added { became_owner: false }
        );
        for sql in [
            "INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ($1, 'at://x/app.bsky.feed.post/1', 'c')",
            "INSERT INTO mutes (viewer_did, subject_did) VALUES ($1, 'did:plc:x')",
        ] {
            sqlx::query(sql).bind(RACE_READD).execute(&t.pool).await.expect(sql);
        }
        let before = (t.generations(RACE_READY).await, t.generations(RACE_READD).await);
        held.release();
        let report = run.await.expect("task").expect("run").expect("on");
        assert_eq!(report, ExpiryReport { days: 7, candidates: 2, skipped: 2, ..ExpiryReport::default() });
        for did in [RACE_READY, RACE_READD] {
            t.kept(did).await;
        }
        let after = (t.generations(RACE_READY).await, t.generations(RACE_READD).await);
        assert_eq!(after, before, "no job's generation moved");
        db.drop_db().await;
    }
}
