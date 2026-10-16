//! The private tier's sealed cursors (private posting §3.6, P-17): a page's
//! last examined position, sealed with the private cursor key
//! (`APP_PRIVATE_CURSOR_KEY_HEX`) and bound to the method, the caller, the
//! anchor and the filter it was issued for.
//!
//! - The plaintext is `createdAt\nkey`, padded with zero bytes to a fixed
//!   [`PLAINTEXT_LEN`], so a cursor's length says nothing about the
//!   position's (a `did:web` author's URI can be long). With the nonce and
//!   tag every cursor is 720 characters.
//! - A position longer than [`PLAINTEXT_LEN`] is not sealed: that page
//!   carries no cursor (§11).
//! - A cursor that does not open (forged, another caller, method, anchor or
//!   filter, a rotated key, or an unsealed cursor from before) is the
//!   caller's error, `400 InvalidRequest`.

use chrono::{DateTime, SecondsFormat, Utc};

use crate::cursor_key::CursorKey;
use crate::xrpc::errors::XrpcError;

/// Every position is padded to this many bytes before sealing.
pub(super) const PLAINTEXT_LEN: usize = 512;

/// `"<method>|<caller DID>|<anchor>|<filter>"` (§3.6): the anchor is the
/// actor, `uri` or `q`, empty for the timeline; the filter is the normalized
/// value, empty for a method with none.
pub(super) fn aad(method: &str, caller: &str, anchor: &str, filter: &str) -> String {
    format!("{method}|{caller}|{anchor}|{filter}")
}

/// Seal `(at, key)` under `aad`. `None` when the position is over
/// [`PLAINTEXT_LEN`] bytes.
pub(super) fn seal(cursor_key: &CursorKey, aad: &str, at: DateTime<Utc>, key: &str) -> Option<String> {
    let mut plain = format!("{}\n{key}", at.to_rfc3339_opts(SecondsFormat::Micros, true)).into_bytes();
    if plain.len() > PLAINTEXT_LEN {
        return None;
    }
    plain.resize(PLAINTEXT_LEN, 0);
    cursor_key.seal_bytes(aad, &plain)
}

/// Open a cursor presented with `aad`; an absent or empty one is no
/// position.
///
/// # Errors
///
/// `400 InvalidRequest "Error: cursor is not valid"` when it does not open.
pub(super) fn open(cursor_key: &CursorKey, aad: &str, cursor: Option<&str>) -> Result<Option<(DateTime<Utc>, String)>, XrpcError> {
    let Some(cursor) = cursor.filter(|c| !c.is_empty()) else {
        return Ok(None);
    };
    position(cursor_key, aad, cursor)
        .map(Some)
        .ok_or_else(|| XrpcError::invalid("Error: cursor is not valid"))
}

fn position(cursor_key: &CursorKey, aad: &str, cursor: &str) -> Option<(DateTime<Utc>, String)> {
    let plain = cursor_key.open_bytes(aad, cursor)?;
    let end = plain.iter().position(|b| *b == 0).unwrap_or(plain.len());
    let (at, key) = std::str::from_utf8(&plain[..end]).ok()?.split_once('\n')?;
    Some((DateTime::parse_from_rfc3339(at).ok()?.with_timezone(&Utc), key.to_string()))
}

/// A search page's position (private posting §3.6, §4.17): the scan goes on
/// below `(at, uri)`, and never below `bound`, the max-age bound fixed at
/// the search's first page. `uri` is empty at a span's lower edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SearchPosition {
    pub at: DateTime<Utc>,
    pub uri: String,
    pub bound: DateTime<Utc>,
}

/// Seal a search position under `aad`, padded as [`seal`] pads. `None`
/// when it is over [`PLAINTEXT_LEN`] bytes.
pub(super) fn seal_search(cursor_key: &CursorKey, aad: &str, p: &SearchPosition) -> Option<String> {
    let bound = p.bound.to_rfc3339_opts(SecondsFormat::Micros, true);
    seal(cursor_key, aad, p.at, &format!("{bound}\n{}", p.uri))
}

/// Open a search cursor presented with `aad`, as [`open`] does.
///
/// # Errors
///
/// `400 InvalidRequest "Error: cursor is not valid"` when it does not open.
pub(super) fn open_search(cursor_key: &CursorKey, aad: &str, cursor: Option<&str>) -> Result<Option<SearchPosition>, XrpcError> {
    let Some((at, rest)) = open(cursor_key, aad, cursor)? else {
        return Ok(None);
    };
    let (bound, uri) = rest.split_once('\n').ok_or_else(|| XrpcError::invalid("Error: cursor is not valid"))?;
    let bound = DateTime::parse_from_rfc3339(bound).map_err(|_| XrpcError::invalid("Error: cursor is not valid"))?;
    Ok(Some(SearchPosition {
        at,
        uri: uri.to_string(),
        bound: bound.with_timezone(&Utc),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A position round-trips under its own binding only, and every cursor
    /// has one length whatever the position's (review 1, 12a).
    #[test]
    fn sealed_positions_open_only_where_issued_and_have_one_length() {
        let key = CursorKey::new(&[4; 32]);
        let at = DateTime::parse_from_rfc3339("2026-10-15T12:00:00.123456Z").expect("ts").with_timezone(&Utc);
        let bound = aad("app.nearhorizon.private.getQuotes", "did:plc:a", "at://x", "");
        let short = format!("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/{}", "3kaaaaaaaaaa2");
        let long = format!("at://did:web:{}/app.bsky.feed.post/3kaaaaaaaaaa2", "h".repeat(100));
        let a = seal(&key, &bound, at, &short).expect("sealed");
        let b = seal(&key, &bound, at, &long).expect("sealed");
        assert_eq!((a.len(), b.len()), (720, 720));
        assert_eq!(open(&key, &bound, Some(&a)).expect("opens"), Some((at, short)));
        assert_eq!(open(&key, &bound, Some(&b)).expect("opens"), Some((at, long)));
        assert_eq!(open(&key, &bound, None).expect("none"), None);
        assert_eq!(open(&key, &bound, Some("")).expect("empty"), None);
        // Another caller, another anchor, another key, an unsealed cursor.
        for (k, binding, cursor) in [
            (&key, aad("app.nearhorizon.private.getQuotes", "did:plc:b", "at://x", ""), a.clone()),
            (&key, aad("app.nearhorizon.private.getQuotes", "did:plc:a", "at://y", ""), a.clone()),
            (&CursorKey::new(&[5; 32]), bound.clone(), a.clone()),
            (&key, bound.clone(), "2026-10-15T12:00:00.000000Z::at://x".to_string()),
        ] {
            let e = open(k, &binding, Some(&cursor)).expect_err("does not open");
            assert_eq!(e.message, "Error: cursor is not valid");
        }
        // Over the padded length: no cursor.
        assert_eq!(seal(&key, &bound, at, &"k".repeat(PLAINTEXT_LEN)), None);
    }

    /// A search position carries its bound, at a span edge too, and opens
    /// only for the query it was issued for (§3.6).
    #[test]
    fn search_positions_carry_their_bound() {
        let key = CursorKey::new(&[4; 32]);
        let at = DateTime::parse_from_rfc3339("2026-10-15T12:00:00.123456Z").expect("ts").with_timezone(&Utc);
        let bound = at - chrono::Duration::days(365);
        let binding = aad("app.nearhorizon.private.searchPosts", "did:plc:a", "cats", "");
        for uri in ["at://did:plc:b/tools.kryphocron.feed.postPrivate/3kaaaaaaaaaa2", ""] {
            let p = SearchPosition { at, uri: uri.to_string(), bound };
            let sealed = seal_search(&key, &binding, &p).expect("sealed");
            assert_eq!(sealed.len(), 720);
            assert_eq!(open_search(&key, &binding, Some(&sealed)).expect("opens"), Some(p));
            let other = aad("app.nearhorizon.private.searchPosts", "did:plc:a", "dogs", "");
            assert!(open_search(&key, &other, Some(&sealed)).is_err(), "another query");
        }
        // A feed cursor is not a search cursor.
        let feed = seal(&key, &binding, at, "3kaaaaaaaaaa2").expect("sealed");
        assert!(open_search(&key, &binding, Some(&feed)).is_err());
    }
}
