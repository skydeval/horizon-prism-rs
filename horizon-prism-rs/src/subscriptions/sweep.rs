//! The daily R4 job (design-v4 §14.1 R4; §4.3 Release, Lifecycle): a
//! safety net behind the release task, run with the other retention tasks.
//!
//! - It releases any `unreferenced` row left behind, whatever its age.
//! - It deletes `subscribed_list_items` rows whose list has no
//!   `subscribed_lists` row.
//! - It deletes `subscribed_owners` rows with no list, but only once every
//!   backoff has expired, and never while the owner's `list_sync` is
//!   `running` (LM-05, LM-09). Each delete is two statements (R3-159): the
//!   owner's row `FOR UPDATE`, then, in a new statement whose snapshot
//!   follows the lock wait, the delete with its `NOT EXISTS`. A list phase
//!   that took the owner's row first has committed its list by then, so
//!   nothing is deleted; one that comes after the delete re-creates the row
//!   through its upsert. The owner's queued `list_sync` is cancelled, at a
//!   new generation, only when this delete removed the row.
//! - It creates any missing owner row of a listed owner.
//!
//! These are internal deletes: no tombstone is written. Tombstones by age
//! are a time step of the public retention task.

use std::time::Instant;

use async_trait::async_trait;
use sqlx::PgPool;

use super::applier::{Applier, Waiting};
use super::jobs;
use crate::db::retention::{RetentionReport, RetentionSettings, RetentionTask, ScopeGuard};

/// Rows per orphan-item batch, as for every retention batch (§II-4.4).
const BATCH: i64 = 10_000;

/// The daily R4 task.
pub struct OrphanSweep {
    applier: Applier,
    sync_min_interval_secs: u32,
}

impl OrphanSweep {
    #[must_use]
    pub fn new(applier: Applier, sync_min_interval_secs: u32) -> Self {
        Self { applier, sync_min_interval_secs }
    }
}

#[async_trait]
impl RetentionTask for OrphanSweep {
    fn name(&self) -> &'static str {
        "subscriptions"
    }

    /// The scope guard is not taken: rule 4's owner-side rows do not
    /// depend on scope.
    async fn run(&self, pool: &PgPool, _scope: &dyn ScopeGuard, _settings: &RetentionSettings) -> Result<RetentionReport, sqlx::Error> {
        let released = self.applier.release_unreferenced().await?;
        if released > 0 {
            tracing::info!(event = "subscribed_lists_released_by_sweep", lists = released, "the daily sweep released unreferenced lists");
        }
        let mut report = RetentionReport::default();
        report.deleted.push(("subscribed_list_items", orphan_items(pool).await?));
        report.deleted.push(("subscribed_owners", orphan_owners(pool, self.applier.waiting(), self.sync_min_interval_secs).await?));
        let created = missing_owners(pool).await?;
        if created > 0 {
            tracing::warn!(event = "subscribed_owners_created_by_sweep", owners = created, "the daily sweep created missing owner rows");
        }
        Ok(report)
    }
}

/// Delete the items of lists that have no row, in batches. A sync writes
/// items only under its list's committed row, so no item written after
/// this statement's snapshot can be taken for an orphan.
///
/// # Errors
///
/// Database errors.
pub async fn orphan_items(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let mut total = 0;
    loop {
        let n = sqlx::query(&format!(
            "DELETE FROM subscribed_list_items WHERE ctid IN ( \
               SELECT i.ctid FROM subscribed_list_items i \
               WHERE NOT EXISTS (SELECT 1 FROM subscribed_lists l WHERE l.list_uri = i.list_uri) LIMIT {BATCH})"
        ))
        .execute(pool)
        .await?
        .rows_affected();
        total += n;
        if n == 0 {
            return Ok(total);
        }
    }
}

/// Delete the owner rows with no list whose backoffs have all expired and
/// whose `list_sync` is not running, each in its own transaction. Returns
/// how many were deleted.
///
/// # Errors
///
/// Database errors.
pub async fn orphan_owners(pool: &PgPool, waiting: &Waiting, sync_min_interval_secs: u32) -> Result<u64, sqlx::Error> {
    let owners: Vec<String> = sqlx::query_scalar(
        "SELECT o.owner_did FROM subscribed_owners o \
         WHERE NOT EXISTS (SELECT 1 FROM subscribed_lists l WHERE l.owner_did = o.owner_did) ORDER BY o.owner_did",
    )
    .fetch_all(pool)
    .await?;
    let mut n = 0;
    for owner in owners {
        if delete_owner(pool, waiting, &owner, sync_min_interval_secs).await? {
            n += 1;
        }
    }
    Ok(n)
}

/// One owner row's delete, in two statements (R3-159). Returns whether the
/// row was deleted.
async fn delete_owner(pool: &PgPool, waiting: &Waiting, owner: &str, sync_min_interval_secs: u32) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    // Lock step 3: the owner's row.
    let locked: Option<i32> = sqlx::query_scalar("SELECT 1 FROM subscribed_owners WHERE owner_did = $1 FOR UPDATE")
        .bind(owner)
        .fetch_optional(&mut *tx)
        .await?;
    if locked.is_none() {
        return Ok(false);
    }
    #[cfg(test)]
    crate::test_support::hooks::pause(crate::test_support::hooks::SWEEP_OWNER_LOCKED, owner).await;
    // A new statement: its snapshot follows the lock wait, so a list
    // committed by a list phase that held the row is seen here.
    let deleted = sqlx::query(
        "DELETE FROM subscribed_owners o WHERE o.owner_did = $1 \
           AND NOT EXISTS (SELECT 1 FROM subscribed_lists l WHERE l.owner_did = $1) \
           AND NOT EXISTS (SELECT 1 FROM backfill_jobs j WHERE j.did = $1 AND j.depth = $2 AND j.state = 'running') \
           AND (o.last_run_ended_at IS NULL OR o.last_run_ended_at + make_interval(secs => $3) <= now()) \
           AND (o.gone_suspect_at IS NULL OR o.gone_suspect_at + interval '24 hours' <= now()) \
           AND (o.slow_until IS NULL OR o.slow_until <= now()) \
           AND (o.cut_until IS NULL OR o.cut_until <= now())",
    )
    .bind(owner)
    .bind(jobs::DEPTH)
    .bind(f64::from(sync_min_interval_secs))
    .execute(&mut *tx)
    .await?
    .rows_affected()
        > 0;
    // Taken before the cancel: a pair added later belongs to an enqueue
    // that re-created the owner's row and commits after this.
    let cancelled_at = Instant::now();
    if deleted {
        // Lock step 6, and only because this delete removed the row: never
        // from the in-memory owner set, which lags a reference's commit.
        sqlx::query(
            "UPDATE backfill_jobs SET state = 'cancelled', generation = generation + 1, updated_at = now() \
             WHERE did = $1 AND depth = $2 AND state = 'queued'",
        )
        .bind(owner)
        .bind(jobs::DEPTH)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    if deleted {
        waiting.forget_before(owner, cancelled_at);
    }
    Ok(deleted)
}

/// Create the owner row of every owner with a list and none. Returns how
/// many were created.
///
/// # Errors
///
/// Database errors.
pub async fn missing_owners(pool: &PgPool) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "INSERT INTO subscribed_owners (owner_did) \
         SELECT DISTINCT l.owner_did FROM subscribed_lists l \
         WHERE NOT EXISTS (SELECT 1 FROM subscribed_owners o WHERE o.owner_did = l.owner_did) \
         ORDER BY l.owner_did \
         ON CONFLICT (owner_did) DO NOTHING",
    )
    .execute(pool)
    .await?
    .rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::{ScopeSets, ScopeState};
    use crate::config::ScopePolicy;
    use crate::subscriptions::list_phase;
    use crate::subscriptions::references::{Budget, mute_list, unmute_list};
    use crate::test_support::fresh_db;

    const M: &str = "did:plc:sweepmember";
    const BUDGET: Budget = Budget { max_references: 5, max_owners: 5 };

    fn list_of(owner: &str) -> String {
        format!("at://{owner}/app.bsky.graph.list/3klist1")
    }

    async fn owner_exists(pool: &PgPool, owner: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscribed_owners WHERE owner_did = $1)")
            .bind(owner)
            .fetch_one(pool)
            .await
            .expect("owner")
    }

    async fn job(pool: &PgPool, owner: &str) -> (String, i64) {
        sqlx::query_as("SELECT state, generation FROM backfill_jobs WHERE did = $1 AND depth = 'list_sync'")
            .bind(owner)
            .fetch_one(pool)
            .await
            .expect("job")
    }

    /// A list subscribed, then dropped and released by hand: its owner row
    /// is left with no list.
    async fn listless_owner(pool: &PgPool, owner: &str) {
        let list = list_of(owner);
        mute_list(pool, M, &list, BUDGET).await.expect("mute");
        list_phase::apply(pool, &list, 0).await.expect("list phase");
        unmute_list(pool, M, &list, BUDGET).await.expect("unmute");
        list_phase::apply(pool, &list, 0).await.expect("list phase");
        list_phase::release(pool, &list).await.expect("release");
    }

    /// §14.1 R4 safety net: an `unreferenced` row left behind is released;
    /// orphaned items go; a list-less owner row goes once its backoffs have
    /// all expired, and never while its `list_sync` is running; a queued
    /// `list_sync` is cancelled at a new generation only when its owner's
    /// row was deleted, and that owner's waiting pairs go with it; a
    /// missing owner row is created.
    #[tokio::test]
    async fn the_daily_sweep_releases_deletes_and_creates_as_the_design_says() {
        const EXPIRED: &str = "did:plc:sweepexpired";
        const CUT: &str = "did:plc:sweepcut";
        const RUNNING: &str = "did:plc:sweeprunning";
        const LEFT: &str = "did:plc:sweepleft";
        const MISSING: &str = "did:plc:sweepmissing";
        let Some(db) = fresh_db("subscriptions_daily_sweep").await else {
            return;
        };
        let p = &db.pool;
        for owner in [EXPIRED, CUT, RUNNING] {
            listless_owner(p, owner).await;
        }
        // EXPIRED's queued job (a stale enqueue), and a pair waiting on it.
        sqlx::query("UPDATE backfill_jobs SET state = 'queued' WHERE did = $1").bind(EXPIRED).execute(p).await.expect("queued");
        let (_, before) = job(p, EXPIRED).await;
        let applier = Applier::new(p.clone(), ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000), 0);
        applier.waiting().add(EXPIRED, M, "mute");
        applier.waiting().add(CUT, M, "mute");
        sqlx::query("UPDATE subscribed_owners SET cut_until = now() + interval '1 day' WHERE owner_did = $1").bind(CUT).execute(p).await.expect("cut");
        sqlx::query("UPDATE backfill_jobs SET state = 'running' WHERE did = $1").bind(RUNNING).execute(p).await.expect("running");
        // LEFT: an `unreferenced` row nothing released, with its items, and
        // items of a list that has no row at all.
        let left = list_of(LEFT);
        mute_list(p, M, &left, BUDGET).await.expect("mute");
        list_phase::apply(p, &left, 0).await.expect("list phase");
        unmute_list(p, M, &left, BUDGET).await.expect("unmute");
        list_phase::apply(p, &left, 0).await.expect("list phase");
        sqlx::query(
            "INSERT INTO subscribed_list_items (uri, owner_did, rkey, list_uri, subject_did, cid, rev) \
             SELECT 'at://' || o || '/app.bsky.graph.listitem/' || n, o, n::text, 'at://' || o || '/app.bsky.graph.list/3klist1', 'did:plc:s' || n, 'bafy', '3k' \
             FROM unnest(ARRAY[$1::text, 'did:plc:sweepnorow']) AS t(o), generate_series(1, 3) AS g(n)",
        )
        .bind(LEFT)
        .execute(p)
        .await
        .expect("items");
        // MISSING: an active list whose owner row was lost.
        let missing = list_of(MISSING);
        mute_list(p, M, &missing, BUDGET).await.expect("mute");
        list_phase::apply(p, &missing, 0).await.expect("list phase");
        sqlx::query("DELETE FROM subscribed_owners WHERE owner_did = $1").bind(MISSING).execute(p).await.expect("lost");

        let sweep = OrphanSweep::new(applier.clone(), 0);
        let guard = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
        let settings = RetentionSettings { tombstone_days: 7, notification_days: 90, max_age_days: 0, trend_aggregate_hours: 72 };
        let report = sweep.run(p, &guard, &settings).await.expect("sweep");
        // LEFT was released (its items with it) and then its owner row
        // went too; the row-less list's items were swept.
        assert_eq!(report.deleted, [("subscribed_list_items", 3), ("subscribed_owners", 2)]);
        let items: i64 = sqlx::query_scalar("SELECT count(*) FROM subscribed_list_items").fetch_one(p).await.expect("items");
        assert_eq!(items, 0);
        assert!(!owner_exists(p, EXPIRED).await && !owner_exists(p, LEFT).await);
        assert_eq!(job(p, EXPIRED).await, ("cancelled".to_string(), before + 1));
        assert!(applier.waiting().of(EXPIRED).is_empty());
        // CUT's backoff and RUNNING's run keep their rows, and their jobs.
        assert!(owner_exists(p, CUT).await && owner_exists(p, RUNNING).await);
        assert_eq!(applier.waiting().of(CUT), [(M.to_string(), "mute".to_string())]);
        assert_eq!(job(p, RUNNING).await.0, "running");
        assert!(owner_exists(p, MISSING).await);
        db.drop_db().await;
    }
}
