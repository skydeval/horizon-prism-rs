//! `record_tombstones`, keyed by the deleted record's `(author, coll, rkey)`.
//!
//! A tombstone is written by URI text, read back as its key: the writer
//! interns the record's author and key before its transaction, so a URI
//! whose key has no id names no tombstone (and none can be stored for it).

use std::collections::HashSet;

use sqlx::PgExecutor;

use super::ids::rev_value;

/// SQL: whether a tombstone names the URI expression `uri`, with `extra`
/// conditions on it (alias `rt`).
#[must_use]
pub fn exists_sql(uri: &str, extra: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM ref_of({uri}) rk JOIN record_tombstones rt \
           ON rt.author = rk.author AND rt.coll = rk.coll AND rt.rkey = rk.rkey{extra})"
    )
}

/// Of `uris`, those a tombstone names.
///
/// # Errors
///
/// Database errors.
pub async fn tombstoned<'e, E: PgExecutor<'e>>(e: E, uris: &[String]) -> Result<HashSet<String>, sqlx::Error> {
    if uris.is_empty() {
        return Ok(HashSet::new());
    }
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT u.uri FROM unnest($1::text[]) AS u(uri) CROSS JOIN LATERAL ref_of(u.uri) k \
         WHERE EXISTS (SELECT 1 FROM record_tombstones t WHERE t.author = k.author AND t.coll = k.coll AND t.rkey = k.rkey)",
    )
    .bind(uris)
    .fetch_all(e)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Whether a tombstone at or above `rev` names `uri`.
///
/// # Errors
///
/// Database errors.
pub async fn tombstoned_at<'e, E: PgExecutor<'e>>(e: E, uri: &str, rev: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&format!("SELECT {}", exists_sql("$1", " AND rt.rev >= $2")))
        .bind(uri)
        .bind(rev_value(rev))
        .fetch_one(e)
        .await
}

/// How a new tombstone meets one already stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Put {
    /// Keep the higher rev, and stamp the deletion now.
    Highest,
    /// Write only over a lower rev.
    Forward,
}

/// Store a tombstone of `uri` at `rev`. A URI whose author or key has no
/// id stores nothing.
///
/// # Errors
///
/// Database errors.
pub async fn put<'e, E: PgExecutor<'e>>(e: E, uri: &str, rev: &str, how: Put) -> Result<(), sqlx::Error> {
    let on_conflict = match how {
        Put::Highest => "DO UPDATE SET rev = GREATEST(record_tombstones.rev, EXCLUDED.rev), deleted_at = now()",
        Put::Forward => "DO UPDATE SET rev = EXCLUDED.rev, deleted_at = now() WHERE record_tombstones.rev < EXCLUDED.rev",
    };
    sqlx::query(&format!(
        "INSERT INTO record_tombstones (author, coll, rkey, rev) \
         SELECT k.author, k.coll, k.rkey, $2 FROM ref_of($1) k WHERE k.coll IS NOT NULL \
         ON CONFLICT (author, coll, rkey) {on_conflict}"
    ))
    .bind(uri)
    .bind(rev_value(rev))
    .execute(e)
    .await?;
    Ok(())
}
