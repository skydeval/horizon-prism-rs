//! Adding members (design-v2 §9.2, §6.11.1): shared by `admin member add`
//! (source `admin`) and auto-enroll (source `auto_enroll`).
//!
//! Under the `ScopeState` write guard and the DID's shared lock: insert the
//! member, put it in S (reason bit 1), enqueue a `member` backfill, and
//! re-evaluate owner status (enqueueing `private` if it becomes an owner).
//! The in-memory sets follow after commit. No network call is made here.

use std::sync::Arc;

use sqlx::PgPool;

use super::ScopeState;
use super::policy::REASON_MEMBER;
use crate::db::locks::lock_dids_shared;
use crate::firehose::jobs::{self, Depth};
use crate::private_tier::PrivateTier;

/// What adding a member did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddOutcome {
    Added { became_owner: bool },
    AlreadyMember,
    /// A purge of the DID is running (§6.11.7): try again after it ends.
    Purging,
}

/// Add `did` as a member.
///
/// # Errors
///
/// Database errors (nothing is changed).
pub async fn add_member(
    pool: &PgPool,
    scope: &ScopeState,
    private: &Arc<dyn PrivateTier>,
    did: &str,
    source: &str,
) -> Result<AddOutcome, sqlx::Error> {
    let mut sets = scope.write_guard().await;
    if sets.purge_kind(did).is_some() {
        return Ok(AddOutcome::Purging);
    }
    let mut tx = pool.begin().await?;
    let purging: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)")
        .bind(did)
        .fetch_one(&mut *tx)
        .await?;
    if purging {
        return Ok(AddOutcome::Purging);
    }
    lock_dids_shared(&mut tx, &[did.to_string()]).await?;
    let inserted = sqlx::query("INSERT INTO members (did, source) VALUES ($1, $2) ON CONFLICT (did) DO NOTHING")
        .bind(did)
        .bind(source)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        > 0;
    if !inserted {
        return Ok(AddOutcome::AlreadyMember);
    }
    sqlx::query(
        "INSERT INTO scope_dids (did, reasons) VALUES ($1, $2) \
         ON CONFLICT (did) DO UPDATE SET reasons = scope_dids.reasons | EXCLUDED.reasons, justified_at = now()",
    )
    .bind(did)
    .bind(REASON_MEMBER)
    .execute(&mut *tx)
    .await?;
    jobs::enqueue(&mut tx, did, Depth::Member).await?;
    let applied = private.reevaluate_owner(&mut tx, did).await?;
    let became_owner = applied.owners_joined.iter().any(|d| d == did);
    if became_owner {
        jobs::enqueue(&mut tx, did, Depth::Private).await?;
    }
    let follows: i64 = sqlx::query_scalar("SELECT count(*) FROM follows WHERE author_did = $1")
        .bind(did)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;

    sets.members.insert(did.to_string());
    sets.scope.insert(did.to_string());
    sets.member_follows.insert(did.to_string(), u32::try_from(follows).unwrap_or(u32::MAX));
    for d in applied.owners_joined {
        sets.private_owners.insert(d);
    }
    for d in &applied.owners_left {
        sets.private_owners.remove(d);
    }
    Ok(AddOutcome::Added { became_owner })
}
