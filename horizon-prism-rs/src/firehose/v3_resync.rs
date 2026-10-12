//! The one-time re-sync for the collections design-v3 added (§12.3).
//!
//! The new collections exist in the repos of DIDs that were enrolled or
//! scoped before the upgrade. After it, one pass enqueues a `resync` job for
//! every member, then for every other in-scope DID, at the weekly re-sync
//! rate (one DID a second). `resync` lists the new collections (the depth
//! table), and re-fetches every profile with its CID.
//!
//! Progress is kept in `worker_state['v3_collections_resync']`: the phase
//! and the last DID enqueued, then `done`. A restart resumes; a finished
//! pass never runs again.

use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::jobs::{self, Depth};

pub const KEY: &str = "v3_collections_resync";
/// The weekly re-sync rate (§II-5.7): one DID a second.
pub const INTERVAL: Duration = Duration::from_secs(1);
/// DIDs read per query.
const PAGE: i64 = 100;

/// Where the pass is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Members, after this DID.
    Members(String),
    /// Other in-scope DIDs, after this DID.
    Scope(String),
    Done,
}

impl Phase {
    fn to_json(&self) -> Value {
        match self {
            Self::Members(after) => json!({"phase": "members", "after": after}),
            Self::Scope(after) => json!({"phase": "scope", "after": after}),
            Self::Done => json!({"phase": "done"}),
        }
    }

    fn from_json(v: &Value) -> Self {
        let after = v.get("after").and_then(Value::as_str).unwrap_or_default().to_string();
        match v.get("phase").and_then(Value::as_str) {
            Some("scope") => Self::Scope(after),
            Some("done") => Self::Done,
            _ => Self::Members(after),
        }
    }
}

async fn load(pool: &PgPool) -> Result<Phase, sqlx::Error> {
    let value: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1")
        .bind(KEY)
        .fetch_optional(pool)
        .await?;
    Ok(value.as_ref().map_or_else(|| Phase::Members(String::new()), Phase::from_json))
}

async fn save(pool: &PgPool, phase: &Phase) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(KEY)
    .bind(phase.to_json())
    .execute(pool)
    .await?;
    Ok(())
}

/// The next DIDs of the phase, in order.
async fn next(pool: &PgPool, phase: &Phase) -> Result<Vec<String>, sqlx::Error> {
    match phase {
        Phase::Members(after) => {
            sqlx::query_scalar("SELECT did FROM members WHERE did > $1 ORDER BY did LIMIT $2")
                .bind(after)
                .bind(PAGE)
                .fetch_all(pool)
                .await
        }
        Phase::Scope(after) => {
            sqlx::query_scalar(
                "SELECT s.did FROM scope_dids s WHERE s.did > $1 \
                 AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did = s.did) ORDER BY s.did LIMIT $2",
            )
            .bind(after)
            .bind(PAGE)
            .fetch_all(pool)
            .await
        }
        Phase::Done => Ok(Vec::new()),
    }
}

/// Run the pass to its end, pausing `interval` after each DID. Returns the
/// number of jobs enqueued by this call.
///
/// # Errors
///
/// Any database error. The pass resumes from the recorded DID.
pub async fn run(pool: &PgPool, interval: Duration) -> Result<u64, sqlx::Error> {
    let mut phase = load(pool).await?;
    let mut enqueued = 0;
    while phase != Phase::Done {
        let dids = next(pool, &phase).await?;
        if dids.is_empty() {
            phase = match phase {
                Phase::Members(_) => Phase::Scope(String::new()),
                Phase::Scope(_) | Phase::Done => Phase::Done,
            };
            save(pool, &phase).await?;
            continue;
        }
        for did in dids {
            let mut conn = pool.acquire().await?;
            jobs::enqueue(&mut conn, &did, Depth::Resync).await?;
            drop(conn);
            enqueued += 1;
            phase = match phase {
                Phase::Members(_) => Phase::Members(did),
                Phase::Scope(_) | Phase::Done => Phase::Scope(did),
            };
            save(pool, &phase).await?;
            tokio::time::sleep(interval).await;
        }
    }
    Ok(enqueued)
}

/// Run the pass in the background, retrying a failed attempt after a
/// minute.
#[must_use]
pub fn spawn(pool: PgPool) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match run(&pool, INTERVAL).await {
                Ok(0) => return,
                Ok(enqueued) => {
                    tracing::info!(event = "v3_collections_resync_done", enqueued, "one-time re-sync for the v3 collections enqueued");
                    return;
                }
                Err(error) => {
                    tracing::warn!(event = "v3_collections_resync_failed", %error, "one-time re-sync pass failed; retrying");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn every_member_then_every_other_scoped_did_once() {
        let Some(db) = fresh_db("v3_collections_resync").await else {
            return;
        };
        let p = &db.pool;
        for did in ["did:plc:m1", "did:plc:m2"] {
            sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')").bind(did).execute(p).await.expect("member");
        }
        for did in ["did:plc:m1", "did:plc:s1", "did:plc:s2"] {
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 1)").bind(did).execute(p).await.expect("scope");
        }
        // Interrupted after the first member: the pass resumes behind it.
        save(p, &Phase::Members("did:plc:m1".into())).await.expect("save");
        assert_eq!(run(p, Duration::ZERO).await.expect("run"), 3);
        let jobs: Vec<(String, String)> = sqlx::query_as("SELECT did, depth FROM backfill_jobs ORDER BY did").fetch_all(p).await.expect("jobs");
        let dids: Vec<&str> = jobs.iter().map(|j| j.0.as_str()).collect();
        // A member is enqueued once, in the member phase, not again as a
        // scoped DID.
        assert_eq!(dids, ["did:plc:m2", "did:plc:s1", "did:plc:s2"]);
        assert!(jobs.iter().all(|j| j.1 == "resync"));
        assert_eq!(load(p).await.expect("load"), Phase::Done);

        // Finished: it never runs again, even with new members.
        sqlx::query("INSERT INTO members (did, source) VALUES ('did:plc:m3', 'admin')").execute(p).await.expect("member");
        assert_eq!(run(p, Duration::ZERO).await.expect("run"), 0);
        db.drop_db().await;
    }
}
