//! Private owners (design-v2 §6.11.1).
//!
//! A DID is a private owner iff it has at least one `private_posts` or
//! `private_audiences` row **and** it is eligible: a community member, or a
//! DID whose community-PDS hosting was confirmed (`private_owner_eligible`).
//! On leaving the set, the same transaction deletes `private_owner_ready` and
//! `private_owner_eligible` (unless still a member) and the `sync_state` row
//! if the DID is no longer tracked.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use super::Applied;

/// Both halves of the owner condition, for one DID.
const OWNER_SQL: &str = "SELECT \
       (EXISTS (SELECT 1 FROM private_posts WHERE author_did = $1) \
        OR EXISTS (SELECT 1 FROM private_audiences WHERE owner_did = $1)) \
   AND (EXISTS (SELECT 1 FROM members WHERE did = $1) \
        OR EXISTS (SELECT 1 FROM private_owner_eligible WHERE did = $1))";

/// `owners::load`: the private-owner set (DIDs only).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn load(pool: &PgPool) -> Result<HashSet<String>, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT d FROM ( \
           SELECT author_did AS d FROM private_posts UNION SELECT owner_did FROM private_audiences \
         ) r WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = r.d) \
            OR EXISTS (SELECT 1 FROM private_owner_eligible e WHERE e.did = r.d)",
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

/// Whether `did` is eligible (member, or confirmed on a community PDS).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn is_eligible(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM members WHERE did = $1) \
             OR EXISTS (SELECT 1 FROM private_owner_eligible WHERE did = $1)",
    )
    .bind(did)
    .fetch_one(conn)
    .await
}

/// `owners::reevaluate`: report the DID's owner status after this
/// transaction's changes; on leaving, drop readiness, eligibility (unless a
/// member) and `sync_state` (if no longer tracked).
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
    let is_member: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1)")
        .bind(did)
        .fetch_one(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM private_owner_ready WHERE did = $1")
        .bind(did)
        .execute(&mut *conn)
        .await?;
    if !is_member {
        sqlx::query("DELETE FROM private_owner_eligible WHERE did = $1")
            .bind(did)
            .execute(&mut *conn)
            .await?;
        sqlx::query("DELETE FROM sync_state WHERE did = $1")
            .bind(did)
            .execute(&mut *conn)
            .await?;
    }
    Ok(Applied {
        changed: false,
        owners_joined: Vec::new(),
        owners_left: vec![did.to_string()],
    })
}

/// Record a community-PDS confirmation, or remove eligibility after a failed
/// or negative one (§6.11.1).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn record_eligibility(conn: &mut PgConnection, did: &str, confirmed: bool) -> Result<(), sqlx::Error> {
    if confirmed {
        sqlx::query(
            "INSERT INTO private_owner_eligible (did, confirmed_at) VALUES ($1, now()) \
             ON CONFLICT (did) DO UPDATE SET confirmed_at = now()",
        )
        .bind(did)
        .execute(conn)
        .await?;
    } else {
        sqlx::query("DELETE FROM private_owner_eligible WHERE did = $1")
            .bind(did)
            .execute(conn)
            .await?;
    }
    Ok(())
}

/// The last confirmation time, if any.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn eligibility_confirmed_at(
    conn: &mut PgConnection,
    did: &str,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar("SELECT confirmed_at FROM private_owner_eligible WHERE did = $1")
        .bind(did)
        .fetch_optional(conn)
        .await
}
