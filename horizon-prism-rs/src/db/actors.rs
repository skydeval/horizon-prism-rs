//! Stored profiles, as the read paths use them.
//!
//! An `actors` row is keyed by its account's `dids` id. It keeps the
//! profile record whole, through the record codec, with its CID as bytes;
//! the reads here give them back as JSON and text.

use std::collections::HashMap;

use serde_json::Value;
use sqlx::PgPool;

use super::ids::cid_from_db;
use super::records::decode_value;

/// The NSID of a profile record.
const NSID_PROFILE: &str = "app.bsky.actor.profile";

/// The URI of `did`'s profile record.
#[must_use]
pub fn profile_uri(did: &str) -> String {
    format!("at://{did}/{NSID_PROFILE}/self")
}

/// The DID whose profile record `uri` names exactly, if it names one.
#[must_use]
pub fn profile_did(uri: &str) -> Option<&str> {
    uri.strip_prefix("at://")?
        .strip_suffix("/self")?
        .strip_suffix(NSID_PROFILE)?
        .strip_suffix('/')
        .filter(|d| !d.is_empty() && !d.contains('/'))
}

/// The stored profile records of `dids`, by DID; an account with none is
/// left out.
///
/// # Errors
///
/// Database errors, and a stored record that does not decode.
pub async fn records(pool: &PgPool, dids: &[String]) -> Result<HashMap<String, Value>, sqlx::Error> {
    if dids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT d.did, a.profile_record FROM dids d JOIN actors a ON a.id = d.id \
         WHERE d.did = ANY($1) AND a.profile_record IS NOT NULL",
    )
    .bind(dids)
    .fetch_all(pool)
    .await?;
    let mut out = HashMap::with_capacity(rows.len());
    for (did, stored) in rows {
        out.insert(did, decode_value(pool, &stored).await?);
    }
    Ok(out)
}

/// `did`'s stored profile record with its CID, when both are stored.
///
/// # Errors
///
/// As [`records`].
pub async fn record_with_cid(pool: &PgPool, did: &str) -> Result<Option<(String, Value)>, sqlx::Error> {
    let row: Option<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT a.profile_cid, a.profile_record FROM actors a \
         WHERE a.id = (SELECT id FROM dids WHERE did = $1) AND a.profile_cid IS NOT NULL AND a.profile_record IS NOT NULL",
    )
    .bind(did)
    .fetch_optional(pool)
    .await?;
    match row {
        Some((cid, stored)) => Ok(Some((cid_from_db(&cid), decode_value(pool, &stored).await?))),
        None => Ok(None),
    }
}

/// The URI `did`'s stored profile pins, when it pins one.
///
/// # Errors
///
/// As [`records`].
pub async fn pinned_post(pool: &PgPool, did: &str) -> Result<Option<String>, sqlx::Error> {
    let record = records(pool, std::slice::from_ref(&did.to_string())).await?.remove(did);
    Ok(record.and_then(|r| r.get("pinnedPost")?.get("uri")?.as_str().map(str::to_string)))
}

/// The current profile CIDs of `dids`, by DID; an account with none is
/// left out.
///
/// # Errors
///
/// Database errors.
pub async fn profile_cids(pool: &PgPool, dids: &[String]) -> Result<HashMap<String, String>, sqlx::Error> {
    if dids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT d.did, a.profile_cid FROM dids d JOIN actors a ON a.id = d.id WHERE d.did = ANY($1) AND a.profile_cid IS NOT NULL",
    )
    .bind(dids)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(did, cid)| (did, cid_from_db(&cid))).collect())
}

/// The DID of the account a stored `handle` names, if any.
///
/// # Errors
///
/// Database errors.
pub async fn did_of_handle(pool: &PgPool, handle: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT d.did FROM actors a JOIN dids d ON d.id = a.id WHERE a.handle = $1")
        .bind(handle)
        .fetch_optional(pool)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_profile_uri_names_its_did() {
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(profile_did(&profile_uri(did)), Some(did));
        for other in [
            "at://did:plc:a/app.bsky.actor.profile/other",
            "at://did:plc:a/app.bsky.feed.post/self",
            "at:///app.bsky.actor.profile/self",
            "at://did:plc:a/x/app.bsky.actor.profile/self",
            "did:plc:a/app.bsky.actor.profile/self",
        ] {
            assert_eq!(profile_did(other), None, "{other}");
        }
    }
}
