//! Stored lists and list items, as the read paths use them.
//!
//! A list is keyed `(author, rkey)`; a list item names a list in its own
//! repo, so it keeps only the list's key, `list_rkey`, beside its own author.
//! A list URI that is not a whole list reference names no stored list.

use sqlx::PgPool;

use super::ids::{AccountId, COLL_LIST, record_key};

/// The key of the list `uri` names, read-only.
///
/// # Errors
///
/// Database errors.
pub async fn key<'e, E: sqlx::PgExecutor<'e>>(e: E, uri: &str) -> Result<Option<(AccountId, i64)>, sqlx::Error> {
    record_key(e, uri, COLL_LIST).await
}

/// Whether `did` is a subject of the list `list`, by its stored items.
///
/// # Errors
///
/// Database errors.
pub async fn on_list(pool: &PgPool, list: &str, did: &str) -> Result<bool, sqlx::Error> {
    let Some((author, rkey)) = key(pool, list).await? else {
        return Ok(false);
    };
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM list_items i, dids s WHERE s.did = $3 AND i.author = $1 AND i.list_rkey = $2 AND i.subject = s.id)",
    )
    .bind(author)
    .bind(rkey)
    .bind(did)
    .fetch_one(pool)
    .await
}

/// The distinct subjects of the list `list`'s stored items.
///
/// # Errors
///
/// Database errors.
pub async fn item_count(pool: &PgPool, list: &str) -> Result<i64, sqlx::Error> {
    let Some((author, rkey)) = key(pool, list).await? else {
        return Ok(0);
    };
    sqlx::query_scalar("SELECT count(DISTINCT subject) FROM list_items WHERE author = $1 AND list_rkey = $2")
        .bind(author)
        .bind(rkey)
        .fetch_one(pool)
        .await
}

/// The DIDs of the distinct subjects of the list `list`'s stored items.
///
/// # Errors
///
/// Database errors.
pub async fn subjects(pool: &PgPool, list: &str) -> Result<Vec<String>, sqlx::Error> {
    let Some((author, rkey)) = key(pool, list).await? else {
        return Ok(Vec::new());
    };
    sqlx::query_scalar(
        "SELECT s.did FROM (SELECT DISTINCT subject FROM list_items WHERE author = $1 AND list_rkey = $2) i JOIN dids s ON s.id = i.subject",
    )
    .bind(author)
    .bind(rkey)
    .fetch_all(pool)
    .await
}

/// SQL: the URI of the record of collection `nsid` keyed `(<did>, <rkey>)`,
/// from the author's DID text and the stored key.
#[must_use]
pub fn uri_sql(did: &str, nsid: &str, rkey: &str) -> String {
    format!("'at://' || {did} || '/{nsid}/' || bigint_to_rkey({rkey})")
}
