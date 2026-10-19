//! Private owners (design-v2 §6.11.1, as the membership design §2.3 amends
//! it).
//!
//! A DID is a private owner iff it has at least one `private_posts` or
//! `private_audiences` row **and** it is a member: membership is the only
//! eligibility, whatever the DID's host. On leaving the set, the same
//! transaction deletes `private_owner_ready` and, for a DID that is not a
//! member, its `sync_state` repo-head state (the whole row once it is
//! outside S too).

use std::collections::HashSet;

use sqlx::{PgConnection, PgPool};

use super::Applied;

/// Both halves of the owner condition, for one DID.
const OWNER_SQL: &str = "SELECT \
       (EXISTS (SELECT 1 FROM private_posts WHERE author_did = $1) \
        OR EXISTS (SELECT 1 FROM private_audiences WHERE owner_did = $1)) \
   AND EXISTS (SELECT 1 FROM members WHERE did = $1)";

/// `owners::load`: the private-owner set (DIDs only).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn load(pool: &PgPool) -> Result<HashSet<String>, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT d FROM ( \
           SELECT author_did AS d FROM private_posts UNION SELECT owner_did FROM private_audiences \
         ) r WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = r.d)",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Whether `did` is a private owner now.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn is_owner(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(OWNER_SQL).bind(did).fetch_one(conn).await
}

/// Whether `did` owns a `following` audience (following-audience design
/// §3.5): an owner now, by both halves of [`OWNER_SQL`], with a
/// `private_audiences` row whose mode is exactly `following`. Leaving the
/// owner set keeps the rows, so the membership half is what keeps a
/// non-member from "owning" one. The one definition every caller uses:
/// the pass decision, the settle re-check, catch-up and the rollout.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn owns_following_audience(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT ({OWNER_SQL}) AND EXISTS (SELECT 1 FROM private_audiences WHERE owner_did = $1 AND mode = 'following')"
    ))
    .bind(did)
    .fetch_one(conn)
    .await
}

/// The rollout (following-audience design §3.6), at each start: an `authz`
/// job, leaving a queued or running one alone, for every DID that owns a
/// `following` audience (as [`owns_following_audience`] says) and whose
/// follow-verification flag is not set. Returns the jobs written.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn enqueue_following_verification(pool: &PgPool) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, updated_at) \
         SELECT o.did, 'authz', 'queued', 0, now(), now() \
         FROM (SELECT DISTINCT owner_did AS did FROM private_audiences WHERE mode = 'following') o \
         WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = o.did) \
           AND NOT EXISTS (SELECT 1 FROM sync_state s WHERE s.did = o.did AND s.follows_verified_at IS NOT NULL) \
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
           not_before = now(), last_error = NULL, updated_at = now() \
         WHERE backfill_jobs.state NOT IN ('queued', 'running')",
    )
    .execute(pool)
    .await?
    .rows_affected())
}

/// Whether `did` is eligible to own private rows: a member (membership
/// design §2.3).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn is_eligible(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1)")
        .bind(did)
        .fetch_one(conn)
        .await
}

/// `owners::reevaluate`: report the DID's owner status after this
/// transaction's changes; on leaving, drop readiness and, unless still a
/// member, `sync_state` (no longer tracked).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn reevaluate(conn: &mut PgConnection, did: &str) -> Result<Applied, sqlx::Error> {
    if is_owner(&mut *conn, did).await? {
        return Ok(Applied {
            changed: false,
            owners_joined: vec![did.to_string()],
            owners_left: Vec::new(),
        });
    }
    let is_member = is_eligible(&mut *conn, did).await?;
    sqlx::query("DELETE FROM private_owner_ready WHERE did = $1")
        .bind(did)
        .execute(&mut *conn)
        .await?;
    if !is_member {
        forget_tracked_sync_state(&mut *conn, did).await?;
    }
    Ok(Applied {
        changed: false,
        owners_joined: Vec::new(),
        owners_left: vec![did.to_string()],
    })
}

/// The `sync_state` part of leaving the tracked set. The repo-head columns
/// belong to tracked DIDs only (§6.11.3), so they go. `last_resync_at` is
/// also design-v3 §5.6's "backfilled once" fact for every DID in S, which
/// `subscribed-post` needs: a DID that stays in S (a removed member keeps
/// its authored rows and stays until the next rebuild, §6.11.7) keeps it.
/// A DID outside S loses the whole row; a row created later has a fresh
/// follow counter and no flag. One that stays clears its flag and replaces
/// its counter (following-audience design §3.3). Migration 0031 runs the
/// same two statements for the non-members it stops tracking.
async fn forget_tracked_sync_state(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    let in_scope: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM scope_dids WHERE did = $1)")
        .bind(did)
        .fetch_one(&mut *conn)
        .await?;
    if in_scope {
        sqlx::query(
            "UPDATE sync_state SET applied_rev = NULL, head_rev = NULL, head_checked_at = NULL, \
             diverged_since = NULL, follows_verified_at = NULL, follows_clears = nextval('follows_clears_seq') \
             WHERE did = $1",
        )
        .bind(did)
        .execute(&mut *conn)
        .await?;
    } else {
        sqlx::query("DELETE FROM sync_state WHERE did = $1")
            .bind(did)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}
