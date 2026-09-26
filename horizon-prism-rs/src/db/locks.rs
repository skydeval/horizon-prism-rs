//! Transaction-scoped advisory locks in the global lock order (design-v2
//! §5.5, §6.11.7):
//! 1. the `ScopeState` guard (not here: taken before any connection);
//! 2. DID-level locks, sorted: shared for ordinary writers (authors and growth
//!    subjects), exclusive for purge markers and retention;
//! 3. per-URI locks, sorted, for every URI a transaction writes;
//! 4. row locks from statements and triggers.
//!
//! Each helper takes all its keys in one statement over a sorted, de-duplicated
//! key list, so the acquisition order is the global ascending key order.

use sqlx::PgConnection;

/// Advisory key for a DID (§6.11.7).
pub const DID_KEY_SQL: &str = "hashtextextended('did:' || $1, 0)";

/// Take shared DID locks for every DID in `dids`, in ascending key order.
///
/// # Errors
///
/// Any database error, including a `lock_timeout` (55P03).
pub async fn lock_dids_shared(conn: &mut PgConnection, dids: &[String]) -> Result<(), sqlx::Error> {
    if dids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "SELECT pg_advisory_xact_lock_shared(k) FROM \
         (SELECT DISTINCT hashtextextended('did:' || d, 0) AS k FROM unnest($1::text[]) AS d ORDER BY k) AS s",
    )
    .bind(dids)
    .execute(conn)
    .await?;
    Ok(())
}

/// Take one DID's exclusive lock (purge marker, retention).
///
/// # Errors
///
/// Any database error.
pub async fn lock_did_exclusive(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('did:' || $1, 0))")
        .bind(did)
        .execute(conn)
        .await?;
    Ok(())
}

/// Take per-URI locks for every URI in `uris`, in ascending key order (§5.5).
///
/// # Errors
///
/// Any database error.
pub async fn lock_uris(conn: &mut PgConnection, uris: &[String]) -> Result<(), sqlx::Error> {
    if uris.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "SELECT pg_advisory_xact_lock(k) FROM \
         (SELECT DISTINCT hashtextextended(u, 0) AS k FROM unnest($1::text[]) AS u ORDER BY k) AS s",
    )
    .bind(uris)
    .execute(conn)
    .await?;
    Ok(())
}

/// The DID named by an at-uri's authority segment.
#[must_use]
pub fn uri_author(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("at://")?;
    let did = rest.split('/').next()?;
    (!did.is_empty()).then_some(did)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_author_extracts_the_did() {
        assert_eq!(
            uri_author("at://did:plc:abc/app.bsky.feed.post/3k"),
            Some("did:plc:abc")
        );
        assert_eq!(uri_author("at://did:web:example.com"), Some("did:web:example.com"));
        assert_eq!(uri_author("https://example.com"), None);
        assert_eq!(uri_author("at:///x"), None);
    }
}
