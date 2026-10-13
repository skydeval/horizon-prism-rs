//! The daily job's part in the memory of learned blocks (design-v3
//! §5.14.18.1 "Retention"). Two steps over the table, both nets behind
//! something that acts sooner:
//!
//! - **by time**: rows last confirmed longer ago than the retention. The
//!   copy drops such an entry within the hour and the writer deletes its
//!   row; this catches a row the writer never reached;
//! - **by membership**: rows of viewers that are not members. The purge's
//!   hook and its batches delete those; this catches what both missed.
//!
//! With the memory off the table is emptied again instead, in case the
//! startup's attempt failed.
//!
//! These are functions over the database pool, not methods of the engine:
//! with `farsight.url` unset there is no engine, and they must still run.

use std::collections::HashSet;

use async_trait::async_trait;
use sqlx::PgPool;

use super::Farsight;
use super::did::canonical;
use crate::config::FarsightMemoryConfig;
use crate::db::locks::lock_did_exclusive;
use crate::db::retention::{RetentionReport, RetentionSettings, RetentionTask, ScopeGuard};

/// Rows per time-step batch, as for every retention batch (§II-4.4).
const BATCH: i64 = 10_000;

/// The memory's retention task.
pub struct MemoryRetention {
    enabled: bool,
    retention_hours: i64,
    /// The engine, for its table lock; `None` when Farsight is not
    /// configured.
    farsight: Option<Farsight>,
}

impl MemoryRetention {
    #[must_use]
    pub fn new(cfg: &FarsightMemoryConfig, farsight: Option<Farsight>) -> Self {
        Self { enabled: cfg.enabled, retention_hours: cfg.retention_hours, farsight }
    }
}

#[async_trait]
impl RetentionTask for MemoryRetention {
    fn name(&self) -> &'static str {
        "farsight_memory"
    }

    /// The scope guard is not taken: no row here depends on scope, and
    /// nothing that holds it may wait on the table lock.
    async fn run(&self, pool: &PgPool, _scope: &dyn ScopeGuard, _settings: &RetentionSettings) -> Result<RetentionReport, sqlx::Error> {
        let mut report = RetentionReport::default();
        if !self.enabled {
            super::truncate_memory(pool).await;
            return Ok(report);
        }
        let aged = aged(pool, self.retention_hours).await?;
        let orphaned = non_members(pool, self.farsight.as_ref()).await?;
        report.deleted.push(("learned_inbound_blocks", aged + orphaned));
        Ok(report)
    }
}

/// The time step: delete rows past the retention, in batches. It takes no
/// lock of the module's. If an entry was confirmed again meanwhile, either
/// the writer's put came first and the row no longer matches, or the
/// confirm makes the pair pending and the writer puts the row back.
async fn aged(pool: &PgPool, retention_hours: i64) -> Result<u64, sqlx::Error> {
    let mut total = 0;
    loop {
        let n = sqlx::query(&format!(
            "DELETE FROM learned_inbound_blocks WHERE ctid IN ( \
               SELECT ctid FROM learned_inbound_blocks \
               WHERE confirmed_at < now() - make_interval(hours => $1) LIMIT {BATCH})"
        ))
        .bind(i32::try_from(retention_hours).unwrap_or(i32::MAX))
        .execute(pool)
        .await?
        .rows_affected();
        total += n;
        if n == 0 {
            return Ok(total);
        }
    }
}

/// The members' DIDs in the form the table holds: `members.did` may spell
/// a `did:web` otherwise.
async fn members<'e>(db: impl sqlx::PgExecutor<'e>) -> Result<HashSet<String>, sqlx::Error> {
    let dids: Vec<String> = sqlx::query_scalar("SELECT did FROM members").fetch_all(db).await?;
    Ok(dids.iter().filter_map(|did| canonical(did)).collect())
}

/// The membership step: delete the rows of every viewer that is not a
/// member, each under that DID's exclusive advisory lock, with membership
/// read again under it. The advisory lock first, the table lock second.
async fn non_members(pool: &PgPool, farsight: Option<&Farsight>) -> Result<u64, sqlx::Error> {
    let viewers: Vec<String> = sqlx::query_scalar("SELECT DISTINCT viewer_did FROM learned_inbound_blocks").fetch_all(pool).await?;
    let current = members(pool).await?;
    let mut total = 0;
    for viewer in viewers.into_iter().filter(|v| !current.contains(v)) {
        let mut tx = pool.begin().await?;
        lock_did_exclusive(&mut tx, &viewer).await?;
        if members(&mut *tx).await?.contains(&viewer) {
            continue;
        }
        let _table = match farsight {
            Some(farsight) => farsight.table_lock().await,
            None => None,
        };
        total += sqlx::query("DELETE FROM learned_inbound_blocks WHERE viewer_did = $1")
            .bind(&viewer)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMBER: &str = "did:web:example.com";
    const GONE: &str = "did:plc:gggggggggggggggggggggggg";
    const OTHER: &str = "did:plc:oooooooooooooooooooooooo";

    async fn row(pool: &PgPool, viewer: &str, other: &str, age_hours: i32) {
        sqlx::query(
            "INSERT INTO learned_inbound_blocks (viewer_did, other_did, source, confirmed_at) \
             VALUES ($1, $2, 'farsight', now() - make_interval(hours => $3))",
        )
        .bind(viewer)
        .bind(other)
        .bind(age_hours)
        .execute(pool)
        .await
        .expect("row");
    }

    /// B-93 (daily steps): rows past the retention go, rows of a viewer
    /// that is not a member go, and a member whose DID is spelled
    /// otherwise in `members` keeps its rows.
    #[tokio::test]
    async fn the_daily_steps_delete_by_time_and_by_membership() {
        let Some(db) = crate::test_support::fresh_db("farsight_memory_retention").await else {
            return;
        };
        sqlx::query("INSERT INTO members (did, source) VALUES ('did:web:Example.COM', 'admin')")
            .execute(&db.pool)
            .await
            .expect("member");
        row(&db.pool, MEMBER, OTHER, 1).await;
        row(&db.pool, MEMBER, GONE, 30).await;
        row(&db.pool, GONE, OTHER, 1).await;
        assert_eq!(aged(&db.pool, 24).await.expect("aged"), 1);
        assert_eq!(non_members(&db.pool, None).await.expect("members"), 1);
        let left: Vec<(String, String)> = sqlx::query_as("SELECT viewer_did, other_did FROM learned_inbound_blocks")
            .fetch_all(&db.pool)
            .await
            .expect("left");
        assert_eq!(left, [(MEMBER.to_string(), OTHER.to_string())]);
        db.drop_db().await;
    }
}
