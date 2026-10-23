//! Private-tier retention (design-v2 §4.4, §6.11.1, §6.11.7), registered
//! ahead of `PublicRetention` so the public `actors` step sees the private
//! row authors that remain.
//!
//! - Class S/M deletes run per DID: each batch takes the `ScopeState` read
//!   guard, then a transaction holding the DID's exclusive advisory lock. The
//!   delete re-checks scope and membership inside that transaction.
//!   - `private_posts`: author out of scope, unless the reply parent or root
//!     author, or the audience's author, is a member (§9.3 rule 2);
//!   - `private_audiences`: owner out of scope.
//! - Class T: `private_tombstones` older than `retention.tombstone_days`;
//!   `private_notifications` older than `retention.notification_days`;
//!   `private_post_fanout` rows done more than two days ago
//!   (private-activity-subs design §3.2); `private_appearance` rows past
//!   their `retained_until` (private-appearance amendment 1 §4.4).
//! - Every DID that lost `private_posts`/`private_audiences` rows is
//!   re-evaluated as an owner in the same transaction as its last batch, and
//!   the in-memory owner set follows after commit.

use std::collections::HashSet;

use async_trait::async_trait;
use sqlx::PgPool;

use super::{owners, schema};
use crate::aggregations::ScopeState;
use crate::db::retention::{
    BATCH_PAUSE, BATCH_SIZE, CANDIDATE_PAGE, LOCK_DID_SQL, METRIC_DELETED_ROWS, RetentionReport, RetentionSettings,
    RetentionTask,
};
use crate::db::ScopeGuard;

const NOT_IN_SCOPE: &str = "NOT EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = {col})";

fn not_in_scope(col: &str) -> String {
    NOT_IN_SCOPE.replace("{col}", col)
}

/// One per-DID delete. `$1` is the owner set, `$2` the DID.
struct Step {
    table: &'static str,
    candidates: String,
    delete: String,
    /// Deleting from this table can end owner status.
    owner_rows: bool,
}

fn steps() -> Vec<Step> {
    let posts = format!("{} AND NOT {}", not_in_scope("p.author_did"), super::ingest::REPLY_TO_MEMBER);
    let audiences = not_in_scope("a.owner_did");
    let step = |table: &'static str, key: &str, alias: &str, did_col: &str, pred: &str, owner_rows: bool| Step {
        table,
        candidates: format!(
            "SELECT DISTINCT {alias}.{did_col} FROM {table} {alias} WHERE ($1::text[] IS NOT NULL) AND {pred} LIMIT {CANDIDATE_PAGE}"
        ),
        delete: format!(
            "DELETE FROM {table} WHERE {key} IN (SELECT {alias}.{key} FROM {table} {alias} \
             WHERE ($1::text[] IS NOT NULL) AND {alias}.{did_col} = $2 AND {pred} LIMIT {BATCH_SIZE})"
        ),
        owner_rows,
    };
    vec![
        step("private_posts", "uri", "p", "author_did", &posts, true),
        step("private_audiences", "uri", "a", "owner_did", &audiences, true),
    ]
}

/// The private retention task.
pub struct PrivateRetention {
    scope: ScopeState,
}

impl PrivateRetention {
    #[must_use]
    pub fn new(scope: ScopeState) -> Self {
        Self { scope }
    }

    async fn run_step(
        &self,
        pool: &PgPool,
        guard_src: &dyn ScopeGuard,
        step: &Step,
        owners_now: &[String],
    ) -> Result<u64, sqlx::Error> {
        let mut total = 0u64;
        let mut seen = HashSet::new();
        loop {
            let fresh: Vec<String> = sqlx::query_scalar(&step.candidates)
                .bind(owners_now)
                .fetch_all(pool)
                .await?
                .into_iter()
                .filter(|d: &String| seen.insert(d.clone()))
                .collect();
            if fresh.is_empty() {
                return Ok(total);
            }
            for did in &fresh {
                total += self.delete_for_did(pool, guard_src, step, owners_now, did).await?;
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
    }

    async fn delete_for_did(
        &self,
        pool: &PgPool,
        guard_src: &dyn ScopeGuard,
        step: &Step,
        owners_now: &[String],
        did: &str,
    ) -> Result<u64, sqlx::Error> {
        let mut total = 0u64;
        loop {
            let guard = guard_src.read().await;
            let mut tx = pool.begin().await?;
            sqlx::query(LOCK_DID_SQL).bind(did).execute(&mut *tx).await?;
            let n = sqlx::query(&step.delete)
                .bind(owners_now)
                .bind(did)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            total += n;
            let last = n < BATCH_SIZE;
            let applied = if last && total > 0 && step.owner_rows {
                Some(owners::reevaluate(&mut tx, did).await?)
            } else {
                None
            };
            tx.commit().await?;
            drop(guard);
            if let Some(a) = applied
                && !a.owners_left.is_empty()
            {
                let mut sets = self.scope.write_guard().await;
                for d in &a.owners_left {
                    sets.private_owners.remove(d);
                }
            }
            if last {
                return Ok(total);
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
    }
}

#[async_trait]
impl RetentionTask for PrivateRetention {
    fn name(&self) -> &'static str {
        "private"
    }

    async fn run(
        &self,
        pool: &PgPool,
        scope: &dyn ScopeGuard,
        settings: &RetentionSettings,
    ) -> Result<RetentionReport, sqlx::Error> {
        let mut report = RetentionReport::default();
        let mut owners_now: Vec<String> = self.scope.snapshot().await.private_owners.into_iter().collect();
        owners_now.sort();
        for step in steps() {
            let n = self.run_step(pool, scope, &step, &owners_now).await?;
            if n > 0 {
                metrics::counter!(METRIC_DELETED_ROWS, "table" => step.table).increment(n);
            }
            report.deleted.push((step.table, n));
        }
        let mut tombstones = 0u64;
        loop {
            let guard = scope.read().await;
            let n = sqlx::query(&format!(
                "DELETE FROM private_tombstones WHERE uri IN (SELECT uri FROM private_tombstones \
                 WHERE deleted_at < now() - make_interval(days => $1) LIMIT {BATCH_SIZE})"
            ))
            .bind(i32::try_from(settings.tombstone_days).unwrap_or(i32::MAX))
            .execute(pool)
            .await?
            .rows_affected();
            drop(guard);
            tombstones += n;
            if n < BATCH_SIZE {
                break;
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
        if tombstones > 0 {
            metrics::counter!(METRIC_DELETED_ROWS, "table" => "private_tombstones").increment(tombstones);
        }
        report.deleted.push(("private_tombstones", tombstones));
        // Class T: private notifications, on the public ones' clock
        // (private posting §3.1).
        let mut notifications = 0u64;
        loop {
            let guard = scope.read().await;
            let n = sqlx::query(&format!(
                "DELETE FROM private_notifications WHERE id IN (SELECT id FROM private_notifications \
                 WHERE indexed_at < now() - make_interval(days => $1) LIMIT {BATCH_SIZE})"
            ))
            .bind(i32::try_from(settings.notification_days).unwrap_or(i32::MAX))
            .execute(pool)
            .await?
            .rows_affected();
            drop(guard);
            notifications += n;
            if n < BATCH_SIZE {
                break;
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
        if notifications > 0 {
            metrics::counter!(METRIC_DELETED_ROWS, "table" => "private_notifications").increment(notifications);
        }
        report.deleted.push(("private_notifications", notifications));
        // Class T: the subscribed-post queue's done rows, kept two days so a
        // post fans out once (private-activity-subs §3.2).
        let mut queue = 0u64;
        loop {
            let guard = scope.read().await;
            let n = sqlx::query(&format!(
                "DELETE FROM private_post_fanout WHERE uri IN (SELECT uri FROM private_post_fanout \
                 WHERE done_at < now() - make_interval(days => $1) LIMIT {BATCH_SIZE})"
            ))
            .bind(super::fanout::DONE_KEEP_DAYS)
            .execute(pool)
            .await?
            .rows_affected();
            drop(guard);
            queue += n;
            if n < BATCH_SIZE {
                break;
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
        if queue > 0 {
            metrics::counter!(METRIC_DELETED_ROWS, "table" => "private_post_fanout").increment(queue);
        }
        report.deleted.push(("private_post_fanout", queue));
        // Class T: appearance rows kept after a removal, past their
        // retention (private-appearance amendment 1 §4.4). The predicate is
        // on the outer DELETE too, so a row a member's put made live after
        // the subquery read it is re-checked and kept. A live row
        // (`retained_until` NULL) is never touched.
        let mut appearance = 0u64;
        loop {
            let guard = scope.read().await;
            let n = sqlx::query(&format!(
                "DELETE FROM private_appearance WHERE retained_until <= now() \
                   AND did IN (SELECT did FROM private_appearance WHERE retained_until <= now() LIMIT {BATCH_SIZE})"
            ))
            .execute(pool)
            .await?
            .rows_affected();
            drop(guard);
            appearance += n;
            if n < BATCH_SIZE {
                break;
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
        if appearance > 0 {
            metrics::counter!(METRIC_DELETED_ROWS, "table" => "private_appearance").increment(appearance);
        }
        report.deleted.push(("private_appearance", appearance));
        Ok(report)
    }
}

/// DIDs that still author private rows (the public `actors` step keeps
/// them, §4.4).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn row_authors(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    schema::row_authors(pool).await
}
