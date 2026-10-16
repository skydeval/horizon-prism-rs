//! The gap transaction (design-v2 §5.7, §6.11.4) and the `worker_state` keys
//! it touches.
//!
//! On a gap, before reading or acking anything further, one transaction:
//! - increments `gap_epoch`;
//! - sets `members.ready_at = NULL` for all members;
//! - deletes all owner readiness (through the private-tier facade);
//! - enqueues, resetting to `queued`, `authz` for every tracked DID and
//!   `resync` for every member;
//! - sets `last_gap`;
//! - clears every follow-verification flag (following-audience design §3.3),
//!   as its last write.
//!
//! The epoch is ordered against every settle by one transaction-scoped
//! advisory key (following-audience design §3.2, #722): the gap takes it
//! exclusively as its first statement, and a settle takes it shared and
//! reads the epoch under it ([`current_epoch_locked`]). A settle that holds
//! it commits before the gap can, and the gap then clears what it wrote; a
//! settle that takes it after the gap reads the new epoch. A request that
//! conflicts with one already waiting queues behind it, so later settles
//! wait behind a waiting gap and cannot starve it.

use chrono::Utc;
use sqlx::{PgConnection, PgPool};

use super::jobs::{self, Depth};
use crate::aggregations::ScopeState;
use crate::private_tier::PrivateTier;

/// Metric: gaps handled.
pub const METRIC_GAPS: &str = "firehose_trim_gap_total";

pub const KEY_GAP_EPOCH: &str = "gap_epoch";
pub const KEY_LAST_GAP: &str = "last_gap";
pub const KEY_CLAIM_IN_PROGRESS: &str = "claim_in_progress";

/// The gap's exclusive hold on the epoch key (§3.2), in the style of the
/// crate's other advisory keys (`db::locks`).
pub const GAP_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended('epoch:gap', 0))";
/// A settle's shared hold on the same key.
pub const SETTLE_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock_shared(hashtextextended('epoch:gap', 0))";

/// What evidence produced a gap (log and metric label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapCause {
    /// `read_checked` trim evidence.
    Trim,
    /// `XAUTOCLAIM` deleted ids or `[id, nil]` pending entries.
    Pending,
    /// `claim_in_progress` found set at startup.
    InterruptedClaim,
    /// The group was (re)created while watermarks already existed.
    GroupRecreated,
    /// `admin resync --private-and-members`.
    Operator,
}

impl GapCause {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trim => "trim",
            Self::Pending => "pending",
            Self::InterruptedClaim => "interrupted_claim",
            Self::GroupRecreated => "group_recreated",
            Self::Operator => "operator",
        }
    }
}

/// Run the gap transaction. The `ScopeState` read guard is taken before the
/// connection (§6.11.7) and held until commit, so the tracked set cannot
/// change underneath the enqueue. Returns the new epoch.
///
/// `clear_claim` also clears `claim_in_progress` in the same transaction.
///
/// # Errors
///
/// Any database error; nothing is changed on error.
pub async fn run_gap_transaction(
    pool: &PgPool,
    scope: &ScopeState,
    private: &dyn PrivateTier,
    cause: GapCause,
    clear_claim: bool,
) -> Result<i64, sqlx::Error> {
    let guard = scope.read_guard().await;
    let tracked = guard.tracked();
    let mut members: Vec<String> = guard.members.iter().cloned().collect();
    members.sort();

    let mut tx = pool.begin().await?;
    // First, before any row: no transaction holding a row lock waits for
    // this key, so it closes no cycle (§3.2 "Lock order").
    sqlx::query(GAP_LOCK_SQL).execute(&mut *tx).await?;
    let epoch = increment_epoch(&mut tx).await?;
    sqlx::query("UPDATE members SET ready_at = NULL WHERE ready_at IS NOT NULL")
        .execute(&mut *tx)
        .await?;
    private.clear_all_readiness(&mut tx).await?;
    jobs::enqueue_many(&mut tx, &tracked, Depth::Authz).await?;
    jobs::enqueue_many(&mut tx, &members, Depth::Resync).await?;
    set_json(&mut tx, KEY_LAST_GAP, &serde_json::json!(Utc::now().to_rfc3339())).await?;
    if clear_claim {
        set_json(&mut tx, KEY_CLAIM_IN_PROGRESS, &serde_json::json!(false)).await?;
    }
    // Last, in the firehose writer's order (`members` → jobs →
    // `sync_state`, §3.3). No counter: a settle holding the key commits
    // first and is cleared here; one after it reads the new epoch.
    sqlx::query("UPDATE sync_state SET follows_verified_at = NULL WHERE follows_verified_at IS NOT NULL")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    drop(guard);

    metrics::counter!(METRIC_GAPS, "cause" => cause.as_str()).increment(1);
    tracing::warn!(
        event = "firehose_trim_gap",
        cause = cause.as_str(),
        epoch,
        tracked = tracked.len(),
        members = members.len(),
        "stream gap: readiness cleared, authz and resync enqueued"
    );
    Ok(epoch)
}

async fn increment_epoch(conn: &mut PgConnection) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, '1'::jsonb, now()) \
         ON CONFLICT (key) DO UPDATE SET value = to_jsonb((worker_state.value #>> '{}')::bigint + 1), updated_at = now() \
         RETURNING (value #>> '{}')::bigint",
    )
    .bind(KEY_GAP_EPOCH)
    .fetch_one(conn)
    .await
}

/// The current gap epoch (0 before the first gap), unlocked: for readers that
/// settle nothing on it.
///
/// # Errors
///
/// Any database error.
pub async fn current_epoch(conn: &mut PgConnection) -> Result<i64, sqlx::Error> {
    let v: Option<i64> = sqlx::query_scalar("SELECT (value #>> '{}')::bigint FROM worker_state WHERE key = $1")
        .bind(KEY_GAP_EPOCH)
        .fetch_optional(conn)
        .await?;
    Ok(v.unwrap_or(0))
}

/// A settle's first two statements (following-audience design §3.2, #722):
/// the shared hold on the gap key, then the epoch read under it, which
/// lasts until the settle's transaction ends. A missing row is an error,
/// which ends the settle with nothing written: migration 0028 seeds it, so
/// only a hand-changed database lacks it.
///
/// # Errors
///
/// Any database error, the missing row included.
pub async fn current_epoch_locked(conn: &mut PgConnection) -> Result<i64, sqlx::Error> {
    sqlx::query(SETTLE_LOCK_SQL).execute(&mut *conn).await?;
    sqlx::query_scalar("SELECT (value #>> '{}')::bigint FROM worker_state WHERE key = $1")
        .bind(KEY_GAP_EPOCH)
        .fetch_one(conn)
        .await
}

async fn set_json(conn: &mut PgConnection, key: &str, value: &serde_json::Value) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(key)
    .bind(value)
    .execute(conn)
    .await?;
    Ok(())
}

/// Durably set or clear `claim_in_progress` (§6.11.4 item 2).
///
/// # Errors
///
/// Any database error.
pub async fn set_claim_in_progress(pool: &PgPool, value: bool) -> Result<(), sqlx::Error> {
    let mut conn = pool.acquire().await?;
    set_json(&mut conn, KEY_CLAIM_IN_PROGRESS, &serde_json::json!(value)).await
}

/// Whether a previous run left `claim_in_progress` set.
///
/// # Errors
///
/// Any database error.
pub async fn claim_in_progress(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let v: Option<bool> =
        sqlx::query_scalar("SELECT (value #>> '{}')::boolean FROM worker_state WHERE key = $1")
            .bind(KEY_CLAIM_IN_PROGRESS)
            .fetch_optional(pool)
            .await?;
    Ok(v.unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;
    use crate::private_tier::DisabledPrivateTier;
    use crate::test_support::fresh_db;

    const MEMBER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const OWNER: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    #[tokio::test]
    async fn gap_transaction_clears_readiness_and_enqueues() {
        let Some(db) = fresh_db("gap_transaction_clears_readiness_and_enqueues").await else {
            return;
        };
        sqlx::query("INSERT INTO members (did, source, ready_at, first_ready_at) VALUES ($1, 'admin', now(), now())")
            .bind(MEMBER)
            .execute(&db.pool)
            .await
            .expect("member");
        // A running authz job must be reset to queued (§6.11.2 re-queueing).
        sqlx::query(
            "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, updated_at) \
             VALUES ($1, 'authz', 'running', 2, now(), now())",
        )
        .bind(MEMBER)
        .execute(&db.pool)
        .await
        .expect("job");
        let mut sets = ScopeSets::default();
        sets.members.insert(MEMBER.into());
        sets.scope.insert(MEMBER.into());
        sets.private_owners.insert(OWNER.into());
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10);
        let private: Arc<dyn PrivateTier> = Arc::new(DisabledPrivateTier);

        let mut conn = db.pool.acquire().await.expect("conn");
        assert_eq!(current_epoch(&mut conn).await.expect("epoch"), 0);
        drop(conn);
        set_claim_in_progress(&db.pool, true).await.expect("claim");

        let e1 = run_gap_transaction(&db.pool, &scope, private.as_ref(), GapCause::Trim, true)
            .await
            .expect("gap");
        assert_eq!(e1, 1);
        assert!(!claim_in_progress(&db.pool).await.expect("claim"));

        let (ready, first_ready): (Option<chrono::DateTime<Utc>>, Option<chrono::DateTime<Utc>>) =
            sqlx::query_as("SELECT ready_at, first_ready_at FROM members WHERE did = $1")
                .bind(MEMBER)
                .fetch_one(&db.pool)
                .await
                .expect("member");
        assert_eq!(ready, None);
        assert!(first_ready.is_some(), "first_ready_at is never cleared");

        let jobs: Vec<(String, String, String, i32)> =
            sqlx::query_as("SELECT did, depth, state, attempts FROM backfill_jobs ORDER BY did, depth")
                .fetch_all(&db.pool)
                .await
                .expect("jobs");
        assert_eq!(
            jobs,
            [
                (MEMBER.to_string(), "authz".to_string(), "queued".to_string(), 0),
                (MEMBER.to_string(), "resync".to_string(), "queued".to_string(), 0),
                (OWNER.to_string(), "authz".to_string(), "queued".to_string(), 0),
            ]
        );
        let last_gap: Option<String> =
            sqlx::query_scalar("SELECT value #>> '{}' FROM worker_state WHERE key = 'last_gap'")
                .fetch_optional(&db.pool)
                .await
                .expect("last_gap");
        assert!(last_gap.is_some());

        // A repeated gap (crash before reanchor) only increments the epoch again.
        let e2 = run_gap_transaction(&db.pool, &scope, private.as_ref(), GapCause::Trim, false)
            .await
            .expect("gap");
        assert_eq!(e2, 2);
        db.drop_db().await;
    }
}
