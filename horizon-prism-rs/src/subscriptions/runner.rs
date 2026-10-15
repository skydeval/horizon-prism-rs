//! Running `list_sync` jobs on the backfill workers (design-v3 §4.3
//! "Lanes", "A separate claim").
//!
//! A backfill worker tries the fast lane, then the slow lane, before its
//! ordinary work. Each lane has a fixed number of permits, so list syncs
//! can never take every worker, and an owner whose server answers slowly
//! can only ever hold a slow permit. A permit is tried, never waited for;
//! a permit whose lane has nothing due is given back at once, and a held
//! permit is dropped when the run ends by any path.
//!
//! A row left `running` with no run behind it (a run that panicked, or
//! whose settling failed) is found by the stuck-row check and queued again
//! at a new generation.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::Semaphore;

use super::applier::{Applier, Waiting};
use super::claim::{self, Lane, LaneTime};
use super::jobs;
use super::run::{self, Outcome, OwnerSource, Settings};
use super::triggers;
use crate::aggregations::ScopeState;

/// The weekly run enqueues one owner per this long.
const WEEKLY_SPACING: Duration = Duration::from_secs(1);

pub const METRIC_RUNNING_AGE: &str = "list_sync_running_age_seconds";
pub const METRIC_STUCK: &str = "list_sync_stuck_requeued_total";

/// The lanes' sizes and the stuck-row bound.
#[derive(Debug, Clone, Copy)]
pub struct Lanes {
    pub fast: u32,
    pub slow: u32,
    /// A row `running` this long with no live run is queued again.
    pub stuck_secs: u32,
    /// `lists.sync_max_wall_secs`: sets the fairness window.
    pub max_wall_secs: u32,
}

/// The owners with a run in progress in this process.
type Live = Arc<Mutex<HashSet<String>>>;

/// Removes its owner from the live set when dropped.
struct Registered {
    live: Live,
    owner: String,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.live.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&self.owner);
    }
}

#[derive(Clone)]
pub struct ListSync {
    pool: PgPool,
    scope: ScopeState,
    source: Arc<dyn OwnerSource>,
    /// The reference applier, whose full reconcile the recovery sweep runs.
    applier: Applier,
    waiting: Waiting,
    time: LaneTime,
    settings: Settings,
    window: Duration,
    lanes: Lanes,
    fast: Arc<Semaphore>,
    slow: Arc<Semaphore>,
    live: Live,
}

impl ListSync {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, source: Arc<dyn OwnerSource>, applier: &Applier, settings: Settings, lanes: Lanes) -> Self {
        Self {
            pool,
            scope,
            source,
            applier: applier.clone(),
            waiting: applier.waiting().clone(),
            time: applier.lane_time().clone(),
            settings,
            window: Duration::from_secs(u64::from(lanes.max_wall_secs).saturating_mul(2).max(3600)),
            lanes,
            fast: Arc::new(Semaphore::new(lanes.fast as usize)),
            slow: Arc::new(Semaphore::new(lanes.slow as usize)),
            live: Live::default(),
        }
    }

    /// Run one due sync, if a lane has both a free permit and a due row:
    /// the fast lane first, then the slow lane.
    ///
    /// # Errors
    ///
    /// The claim failed (database).
    pub async fn run_once(&self) -> Result<Option<(String, Outcome)>, sqlx::Error> {
        for (lane, permits) in [(Lane::Fast, &self.fast), (Lane::Slow, &self.slow)] {
            let Ok(_permit) = Arc::clone(permits).try_acquire_owned() else {
                continue;
            };
            let Some(claimed) = claim::claim(&self.pool, lane, &self.waiting, &self.time, self.window).await? else {
                continue;
            };
            self.live.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(claimed.owner.clone());
            let _registered = Registered { live: Arc::clone(&self.live), owner: claimed.owner.clone() };
            let _holding = self.time.begin(lane, &claimed.participant);
            let outcome = run::run(&self.pool, &self.scope, self.source.as_ref(), &claimed.owner, claimed.generation, self.settings, lane.as_str()).await;
            tracing::info!(event = "list_sync_run", owner = %claimed.owner, lane = lane.as_str(), outcome = outcome.label(), "list sync run finished");
            return Ok(Some((claimed.owner, outcome)));
        }
        Ok(None)
    }

    /// Queue again, at a new generation, every row `running` longer than
    /// the stuck bound with no live run in this process. Returns how many.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn requeue_stuck(&self) -> Result<u64, sqlx::Error> {
        let live: Vec<String> = self.live.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().cloned().collect();
        let stuck: Vec<String> = sqlx::query_scalar(
            "SELECT did FROM backfill_jobs WHERE depth = $1 AND state = 'running' \
               AND updated_at < now() - make_interval(secs => $2) AND NOT (did = ANY($3)) ORDER BY did",
        )
        .bind(jobs::DEPTH)
        .bind(f64::from(self.lanes.stuck_secs))
        .bind(&live)
        .fetch_all(&self.pool)
        .await?;
        let mut n = 0;
        for owner in stuck {
            // A run end like any other: the owner's row first, and paced.
            if jobs::interrupt(&self.pool, &owner, self.settings.min_interval_secs, Some(self.lanes.stuck_secs)).await? {
                n += 1;
            }
        }
        if n > 0 {
            tracing::warn!(event = "list_sync_stuck_requeued", jobs = n, "re-queued list sync rows left running");
            metrics::counter!(METRIC_STUCK).increment(n);
        }
        Ok(n)
    }

    /// At startup: no run of a previous process is live, so every row left
    /// `running` is queued again. Returns how many.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn requeue_interrupted(&self) -> Result<u64, sqlx::Error> {
        let running: Vec<String> = sqlx::query_scalar("SELECT did FROM backfill_jobs WHERE depth = $1 AND state = 'running' ORDER BY did")
            .bind(jobs::DEPTH)
            .fetch_all(&self.pool)
            .await?;
        let mut n = 0;
        for owner in running {
            if jobs::interrupt(&self.pool, &owner, self.settings.min_interval_secs, None).await? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// One recovery sweep (design-v4 §4.3 "Crash safety", §II-5.10 retry
    /// scheduler): the applier's full reconcile, which corrects every list
    /// whose row and references disagree and restarts any release a crash
    /// left undone, then the sweep's own enqueues
    /// ([`triggers::recovery_sweep`]). Returns how many owners the sweep
    /// enqueued.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn sweep_once(&self) -> Result<u64, sqlx::Error> {
        let busy = self.applier.reconcile().await?;
        if !busy.is_empty() {
            // The applier's own passes take a busy list that has a marker;
            // one without is looked at again by the next sweep.
            tracing::info!(event = "list_reconcile_busy", lists = busy.len(), "lists busy during the full reconcile");
        }
        triggers::recovery_sweep(&self.pool, &self.scope, &self.waiting, self.settings.min_interval_secs).await
    }

    /// The retry scheduler's re-queue of failed `list_sync` rows
    /// ([`jobs::retry_failed`]). Returns how many.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn retry_once(&self) -> Result<u64, sqlx::Error> {
        jobs::retry_failed(&self.pool, self.settings.min_interval_secs).await
    }

    /// The recovery sweep now, then the retry scheduler's `list_sync` work
    /// every `every` (its period): the re-queue of failed rows, then the
    /// sweep. Each sweep is followed by the weekly run when a week has
    /// passed.
    pub async fn sweep_loop(self, every: Duration) {
        let pause = self.settings.min_interval_secs;
        let mut first = true;
        loop {
            // The scheduler re-queues on its period, not at startup.
            if !std::mem::take(&mut first) {
                match self.retry_once().await {
                    Ok(n) => tracing::info!(event = "list_sync_retry_scheduled", jobs = n, "failed list syncs re-queued"),
                    Err(error) => tracing::warn!(event = "list_sync_retry_failed", %error, "re-queueing failed list syncs failed"),
                }
            }
            match self.sweep_once().await {
                Ok(n) => tracing::info!(event = "list_sync_recovery_sweep", owners = n, "list sync recovery sweep"),
                Err(error) => tracing::warn!(event = "list_sync_recovery_sweep_failed", %error, "the list sync recovery sweep failed"),
            }
            match triggers::weekly(&self.pool, pause, WEEKLY_SPACING).await {
                Ok(Some(n)) => tracing::info!(event = "list_sync_weekly", owners = n, "weekly list syncs enqueued"),
                Ok(None) => {}
                Err(error) => tracing::warn!(event = "list_sync_weekly_failed", %error, "enqueueing the weekly list syncs failed"),
            }
            tokio::time::sleep(every).await;
        }
    }

    /// The stuck-row check, every quarter of the stuck bound, and the age
    /// of the oldest `running` row as a gauge.
    pub async fn stuck_loop(self) {
        let every = Duration::from_secs(u64::from(self.lanes.stuck_secs / 4).max(1));
        loop {
            tokio::time::sleep(every).await;
            if let Err(error) = self.requeue_stuck().await {
                tracing::warn!(event = "list_sync_stuck_check_failed", %error, "the list sync stuck-row check failed");
            }
            let age: Result<Option<f64>, sqlx::Error> = sqlx::query_scalar(
                "SELECT EXTRACT(EPOCH FROM now() - min(updated_at))::float8 FROM backfill_jobs WHERE depth = $1 AND state = 'running'",
            )
            .bind(jobs::DEPTH)
            .fetch_one(&self.pool)
            .await;
            if let Ok(age) = age {
                metrics::gauge!(METRIC_RUNNING_AGE).set(age.unwrap_or(0.0));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;
    use crate::subscriptions::fetch::{Bounds, OwnerRecords};
    use crate::subscriptions::list_phase;
    use crate::subscriptions::references::{Budget, mute_list};
    use crate::subscriptions::run::OwnerStatus;
    use crate::test_support::fresh_db;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const M1: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmm1";

    /// An owner whose identity was deleted: the run needs no listing.
    struct Tombstoned;

    #[async_trait]
    impl OwnerSource for Tombstoned {
        async fn status(&self, _owner: &str) -> Result<OwnerStatus, String> {
            Ok(OwnerStatus::Tombstoned)
        }
        async fn records(&self, _owner: &str, _generation: i64) -> Result<Box<dyn OwnerRecords>, String> {
            Err("not listed".to_string())
        }
    }

    fn list_sync(pool: &PgPool, scope: ScopeState, fast: u32) -> ListSync {
        let bounds = Bounds { max_records: 1000, max_pages: 10, probe_call: Duration::from_secs(5), min_records_per_call_sec: 1, max_call: Duration::from_secs(60), max_wall: Duration::from_secs(60) };
        let settings = Settings { max_items: 100, tombstones_max_per_owner: 10_000, min_interval_secs: 600, bounds };
        let lanes = Lanes { fast, slow: 1, stuck_secs: 5400, max_wall_secs: 3600 };
        let applier = Applier::new(pool.clone(), scope.clone(), 600);
        ListSync::new(pool.clone(), scope, Arc::new(Tombstoned), &applier, settings, lanes)
    }

    /// A worker runs a due sync and frees its lane; a lane with no permit
    /// runs nothing; a row left `running` with no run behind it is queued
    /// again at a new generation, and a live run's row is left alone.
    #[tokio::test]
    async fn a_lane_runs_a_due_sync_and_stuck_rows_come_back() {
        let Some(db) = fresh_db("subscriptions_runner").await else {
            return;
        };
        let p = &db.pool;
        let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
        let list = format!("at://{A}/app.bsky.graph.list/3klist1");
        mute_list(p, M1, &list, Budget { max_references: 5, max_owners: 5 }).await.expect("mute");
        list_phase::apply(p, &list, 600).await.expect("apply");

        // No fast permit: nothing runs (the owner is not slow).
        assert_eq!(list_sync(p, scope.clone(), 0).run_once().await.expect("run"), None);
        let sync = list_sync(p, scope, 1);
        let (owner, outcome) = sync.run_once().await.expect("run").expect("a run");
        assert_eq!((owner.as_str(), outcome), (A, Outcome::OwnerGone));
        assert_eq!(sync.fast.available_permits(), 1);
        assert!(sync.live.lock().expect("live").is_empty());
        assert_eq!(sync.run_once().await.expect("run"), None);

        // A row left running: young, it stays; old, it comes back, unless
        // a run for it is live.
        sqlx::query("UPDATE backfill_jobs SET state = 'running', updated_at = now() - interval '1 hour'").execute(p).await.expect("running");
        assert_eq!(sync.requeue_stuck().await.expect("stuck"), 0);
        sqlx::query("UPDATE backfill_jobs SET updated_at = now() - interval '2 hours'").execute(p).await.expect("old");
        sync.live.lock().expect("live").insert(A.to_string());
        assert_eq!(sync.requeue_stuck().await.expect("stuck"), 0);
        sync.live.lock().expect("live").clear();
        let before: i64 = sqlx::query_scalar("SELECT generation FROM backfill_jobs").fetch_one(p).await.expect("generation");
        assert_eq!(sync.requeue_stuck().await.expect("stuck"), 1);
        let after: (String, i64) = sqlx::query_as("SELECT state, generation FROM backfill_jobs").fetch_one(p).await.expect("job");
        assert_eq!(after, ("queued".to_string(), before + 1));
        // At startup every running row comes back, whatever its age.
        sqlx::query("UPDATE backfill_jobs SET state = 'running', updated_at = now()").execute(p).await.expect("running");
        assert_eq!(sync.requeue_interrupted().await.expect("startup"), 1);
        assert_eq!(sync.requeue_interrupted().await.expect("startup"), 0);
        db.drop_db().await;
    }
}
