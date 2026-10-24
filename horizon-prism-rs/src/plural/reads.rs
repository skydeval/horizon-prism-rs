//! The stored state the answers read (plural-pronouns design §2.3–2.5).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;

use super::{NSID_PLURAL_MEMBER, RepoRow, RepoState, Stored};

/// A `plural_repos` row as read.
type RepoTuple = (String, String, Option<DateTime<Utc>>, Option<DateTime<Utc>>, bool);

/// The `plural_repos` rows of `dids`.
pub(super) async fn repos(pool: &PgPool, dids: &[String]) -> Result<HashMap<String, RepoRow>, sqlx::Error> {
    let rows: Vec<RepoTuple> = sqlx::query_as(
        "SELECT did, state, verified_at, next_fill_at, members_truncated FROM plural_repos WHERE did = ANY($1)",
    )
    .bind(dids)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(did, state, verified_at, next_fill_at, members_truncated)| {
            Some((did, RepoRow { state: RepoState::parse(&state)?, verified_at, next_fill_at, members_truncated }))
        })
        .collect())
}

/// The stored records at `uris`.
pub(super) async fn records(pool: &PgPool, uris: &[String]) -> Result<HashMap<String, Stored>, sqlx::Error> {
    let rows: Vec<(String, String, Option<Value>)> = sqlx::query_as("SELECT uri, cid, record FROM plural_records WHERE uri = ANY($1)")
        .bind(uris)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(uri, cid, value)| (uri, Stored { cid, value })).collect())
}

/// One page of `did`'s stored members after `cursor`, ascending rkey:
/// `(uri, rkey, cid, record)`.
pub(super) async fn member_page(
    pool: &PgPool,
    did: &str,
    cursor: Option<&str>,
    limit: i64,
) -> Result<Vec<(String, String, String, Option<Value>)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT uri, rkey, cid, record FROM plural_records \
         WHERE did = $1 AND collection = $2 AND ($3::text IS NULL OR rkey COLLATE \"C\" > $3) \
         ORDER BY rkey COLLATE \"C\" LIMIT $4",
    )
    .bind(did)
    .bind(NSID_PLURAL_MEMBER)
    .bind(cursor)
    .bind(limit)
    .fetch_all(pool)
    .await
}
