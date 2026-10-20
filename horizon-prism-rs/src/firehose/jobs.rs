//! `backfill_jobs` enqueueing (design-v2 §5.10, §6.11.2).
//!
//! Every enqueue upserts `(did, depth)` to `queued` with `attempts = 0`, even
//! when a job is `running`: a readiness-clearing event must make the running
//! job's final epoch/state check fail, so it cannot restore stale readiness.
//! Every reset of an existing row also moves its `generation` on
//! (design-v4 §II-5.10 "Generations"), so a run that began before it can
//! write nothing more to the row, even once another worker has claimed it.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

/// Backfill depth (§5.10 table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Depth {
    Member,
    Scope,
    Private,
    Authz,
    Resync,
}

impl Depth {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Scope => "scope",
            Self::Private => "private",
            Self::Authz => "authz",
            Self::Resync => "resync",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "member" => Some(Self::Member),
            "scope" => Some(Self::Scope),
            "private" => Some(Self::Private),
            "authz" => Some(Self::Authz),
            "resync" => Some(Self::Resync),
            _ => None,
        }
    }

    /// Depths that page every authorization collection to the end and may
    /// set readiness and `applied_rev` (§6.11.3).
    #[must_use]
    pub fn is_full_authorization(self) -> bool {
        matches!(self, Self::Member | Self::Private | Self::Authz)
    }
}

const UPSERT: &str = "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, last_error, updated_at) \
     VALUES ($1, $2, 'queued', 0, $3, NULL, now()) \
     ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
       not_before = EXCLUDED.not_before, last_error = NULL, updated_at = now()";

/// Enqueue (or reset to queued) one job, due now.
///
/// # Errors
///
/// Any database error.
pub async fn enqueue(conn: &mut PgConnection, did: &str, depth: Depth) -> Result<(), sqlx::Error> {
    enqueue_at(conn, did, depth, Utc::now()).await
}

/// Enqueue (or reset to queued) one job, due at `not_before`.
///
/// # Errors
///
/// Any database error.
pub async fn enqueue_at(
    conn: &mut PgConnection,
    did: &str,
    depth: Depth,
    not_before: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(UPSERT)
        .bind(did)
        .bind(depth.as_str())
        .bind(not_before)
        .execute(conn)
        .await?;
    Ok(())
}

/// Enqueue one job due at `not_before`, leaving a `queued` or `running` one
/// alone (following-audience design §3.5, §4.4): a burst of follow-ups does
/// not keep resetting it, and a running run is not superseded. A `done`,
/// `failed`, `unavailable` or `cancelled` row is queued again at a new
/// generation.
///
/// # Errors
///
/// Any database error.
pub async fn enqueue_if_idle(
    conn: &mut PgConnection,
    did: &str,
    depth: Depth,
    not_before: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, last_error, updated_at) \
         VALUES ($1, $2, 'queued', 0, $3, NULL, now()) \
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
           not_before = EXCLUDED.not_before, last_error = NULL, updated_at = now() \
         WHERE backfill_jobs.state NOT IN ('queued', 'running')",
    )
    .bind(did)
    .bind(depth.as_str())
    .bind(not_before)
    .execute(conn)
    .await?;
    Ok(())
}

/// Enqueue (or reset) one job per DID, all due now, in one statement.
///
/// # Errors
///
/// Any database error.
pub async fn enqueue_many(conn: &mut PgConnection, dids: &[String], depth: Depth) -> Result<u64, sqlx::Error> {
    if dids.is_empty() {
        return Ok(0);
    }
    let n = sqlx::query(
        "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, last_error, updated_at) \
         SELECT d, $2, 'queued', 0, now(), NULL, now() FROM unnest($1::text[]) AS d \
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
           not_before = now(), last_error = NULL, updated_at = now()",
    )
    .bind(dids)
    .bind(depth.as_str())
    .execute(conn)
    .await?
    .rows_affected();
    Ok(n)
}

/// Enqueue one `resync` per DID, the i-th due at `not_befores[i]`, in one
/// statement (the off → on fill, public-indexing §8.3). Unlike the other
/// enqueues it leaves a `queued` resync alone, due time, attempts and
/// error included: that job builds today's plan when it is claimed. A
/// `running`, `done` or `failed` one is reset. Returns the rows written.
///
/// # Errors
///
/// Any database error.
pub async fn enqueue_staggered(
    conn: &mut PgConnection,
    dids: &[String],
    not_befores: &[DateTime<Utc>],
) -> Result<u64, sqlx::Error> {
    debug_assert_eq!(dids.len(), not_befores.len(), "one due time per DID");
    if dids.is_empty() {
        return Ok(0);
    }
    let n = sqlx::query(
        "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, last_error, updated_at) \
         SELECT d, 'resync', 'queued', 0, nb, NULL, now() FROM unnest($1::text[], $2::timestamptz[]) AS t(d, nb) \
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
           not_before = EXCLUDED.not_before, last_error = NULL, updated_at = now() \
         WHERE backfill_jobs.state <> 'queued'",
    )
    .bind(dids)
    .bind(not_befores)
    .execute(conn)
    .await?
    .rows_affected();
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depths_round_trip() {
        for d in [Depth::Member, Depth::Scope, Depth::Private, Depth::Authz, Depth::Resync] {
            assert_eq!(Depth::parse(d.as_str()), Some(d));
        }
        assert_eq!(Depth::parse("full"), None);
    }

    #[test]
    fn full_authorization_depths() {
        assert!(Depth::Member.is_full_authorization());
        assert!(Depth::Private.is_full_authorization());
        assert!(Depth::Authz.is_full_authorization());
        assert!(!Depth::Scope.is_full_authorization());
        assert!(!Depth::Resync.is_full_authorization());
    }

    /// public-indexing §8.3, T-23: a queued resync keeps its due time,
    /// attempts and error; a running, done or failed one is reset; a new
    /// DID is inserted.
    #[tokio::test]
    async fn enqueue_staggered_leaves_a_queued_resync_alone() {
        let Some(db) = crate::test_support::fresh_db("jobs_enqueue_staggered").await else {
            return;
        };
        let p = &db.pool;
        sqlx::query(
            "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, last_error) VALUES \
             ('did:plc:queued', 'resync', 'queued', 2, '2026-01-01T00:00:00Z', 'boom'), \
             ('did:plc:running', 'resync', 'running', 1, '2026-01-01T00:00:00Z', NULL), \
             ('did:plc:done', 'resync', 'done', 0, '2026-01-01T00:00:00Z', NULL), \
             ('did:plc:failed', 'resync', 'failed', 5, '2026-01-01T00:00:00Z', 'gone'), \
             ('did:plc:done', 'scope', 'done', 0, '2026-01-01T00:00:00Z', NULL)",
        )
        .execute(p)
        .await
        .expect("seed");
        let dids: Vec<String> = ["did:plc:queued", "did:plc:running", "did:plc:done", "did:plc:failed", "did:plc:new"]
            .map(String::from)
            .to_vec();
        let base = DateTime::from_timestamp(1_800_000_000, 0).expect("time");
        let due: Vec<DateTime<Utc>> = (0..5).map(|i| base + chrono::TimeDelta::seconds(i)).collect();
        let mut conn = p.acquire().await.expect("conn");
        assert_eq!(enqueue_staggered(&mut conn, &dids, &due).await.expect("enqueue"), 4);
        assert_eq!(enqueue_staggered(&mut conn, &[], &[]).await.expect("empty"), 0);
        drop(conn);

        type Row = (String, String, String, i32, DateTime<Utc>, Option<String>);
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT did, depth, state, attempts, not_before, last_error FROM backfill_jobs ORDER BY did, depth",
        )
        .fetch_all(p)
        .await
        .expect("rows");
        let old = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z").expect("old").with_timezone(&Utc);
        let row = |did: &str, depth: &str, state: &str, attempts: i32, not_before: DateTime<Utc>, error: Option<&str>| {
            (did.to_string(), depth.to_string(), state.to_string(), attempts, not_before, error.map(String::from))
        };
        assert_eq!(
            rows,
            [
                row("did:plc:done", "resync", "queued", 0, due[2], None),
                // Another depth of the same DID is untouched.
                row("did:plc:done", "scope", "done", 0, old, None),
                row("did:plc:failed", "resync", "queued", 0, due[3], None),
                row("did:plc:new", "resync", "queued", 0, due[4], None),
                row("did:plc:queued", "resync", "queued", 2, old, Some("boom")),
                row("did:plc:running", "resync", "queued", 0, due[1], None),
            ]
        );
        db.drop_db().await;
    }
}
