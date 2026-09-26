//! `backfill_jobs` enqueueing (design-v2 §5.10, §6.11.2).
//!
//! Every enqueue upserts `(did, depth)` to `queued` with `attempts = 0`, even
//! when a job is `running`: a readiness-clearing event must make the running
//! job's final epoch/state check fail, so it cannot restore stale readiness.

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
     ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, \
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
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, \
           not_before = now(), last_error = NULL, updated_at = now()",
    )
    .bind(dids)
    .bind(depth.as_str())
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
}
