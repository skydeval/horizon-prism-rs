//! The sealed paging cursor (design-v3 §5.15.14).
//!
//! A `moreReplies` marker can carry a cursor: where in a node's reply
//! sequence to go on reading. It is encrypted and authenticated with a
//! server key and bound to the URI of the node it was issued for, so a
//! client can neither read it, nor alter it, nor use it under another
//! anchor. A cursor grants nothing: every reply it leads to goes through
//! the same verdicts and rules as on a first request.
//!
//! The key is `APP_THREADLENS_CURSOR_KEY_HEX`. Without one, a key is made
//! at start, and cursors do not outlive the process.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A cursor is at most this long as sent.
pub const MAX_CURSOR_LEN: usize = 1024;
const NONCE_LEN: usize = 12;

/// A position in a node's reply sequence: what was read last.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Position {
    /// In the local phase, after the stored reply at this
    /// `(created_at, uri)`.
    #[serde(rename = "l")]
    Local { at: DateTime<Utc>, uri: String },
    /// In the Constellation phase, at this cursor of its listing; `None`
    /// is the listing's start.
    #[serde(rename = "c")]
    Listed { cursor: Option<String> },
}

/// Seals and opens cursors. Cheap to clone.
#[derive(Clone)]
pub struct CursorKey {
    cipher: ChaCha20Poly1305,
}

impl std::fmt::Debug for CursorKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CursorKey(<redacted>)")
    }
}

impl CursorKey {
    #[must_use]
    pub fn new(key: &[u8; 32]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(Key::from_slice(key)),
        }
    }

    /// A key that lives as long as the process.
    #[must_use]
    pub fn generated() -> Self {
        Self::new(&rand::random::<[u8; 32]>())
    }

    /// Seal `position` for the node `node_uri`. `None` when the cursor
    /// would be longer than a cursor may be: the marker then has none,
    /// and the node is read again from its start.
    #[must_use]
    pub fn seal(&self, node_uri: &str, position: &Position) -> Option<String> {
        let plain = serde_json::to_vec(position).ok()?;
        let nonce: [u8; NONCE_LEN] = rand::random();
        let sealed = self.cipher.encrypt(Nonce::from_slice(&nonce), Payload { msg: &plain, aad: node_uri.as_bytes() }).ok()?;
        let mut bytes = nonce.to_vec();
        bytes.extend(sealed);
        let cursor = URL_SAFE_NO_PAD.encode(bytes);
        (cursor.len() <= MAX_CURSOR_LEN).then_some(cursor)
    }

    /// Open a cursor presented with the anchor `node_uri`. `None` when it
    /// fails authentication, was issued for another node, or is not a
    /// cursor at all.
    #[must_use]
    pub fn open(&self, node_uri: &str, cursor: &str) -> Option<Position> {
        if cursor.len() > MAX_CURSOR_LEN {
            return None;
        }
        let bytes = URL_SAFE_NO_PAD.decode(cursor).ok()?;
        if bytes.len() <= NONCE_LEN {
            return None;
        }
        let (nonce, sealed) = bytes.split_at(NONCE_LEN);
        let plain = self.cipher.decrypt(Nonce::from_slice(nonce), Payload { msg: sealed, aad: node_uri.as_bytes() }).ok()?;
        serde_json::from_slice(&plain).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODE: &str = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/3knode";

    #[test]
    fn a_cursor_opens_only_for_its_node_and_its_key() {
        let key = CursorKey::new(&[7; 32]);
        let local = Position::Local {
            at: DateTime::parse_from_rfc3339("2026-09-26T12:00:00.123456Z").expect("time").with_timezone(&Utc),
            uri: "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.feed.post/3kreply".into(),
        };
        let listed = Position::Listed { cursor: Some("abc".into()) };
        for position in [local, listed, Position::Listed { cursor: None }] {
            let cursor = key.seal(NODE, &position).expect("sealed");
            assert!(cursor.len() <= MAX_CURSOR_LEN && !cursor.contains("3kreply"));
            assert_eq!(key.open(NODE, &cursor), Some(position.clone()));
            // Another anchor, another key, an altered cursor: refused.
            assert_eq!(key.open(&format!("{NODE}x"), &cursor), None);
            assert_eq!(CursorKey::new(&[8; 32]).open(NODE, &cursor), None);
            let mut altered = cursor.clone().into_bytes();
            altered[20] = if altered[20] == b'A' { b'B' } else { b'A' };
            assert_eq!(key.open(NODE, &String::from_utf8(altered).expect("ascii")), None);
            // Two cursors for one position differ.
            assert_ne!(key.seal(NODE, &position), Some(cursor));
        }
        assert_eq!(key.open(NODE, "not a cursor"), None);
        assert_eq!(key.open(NODE, ""), None);
        // A position too long to send has no cursor.
        assert_eq!(key.seal(NODE, &Position::Listed { cursor: Some("x".repeat(2000)) }), None);
        assert!(CursorKey::generated().open(NODE, &key.seal(NODE, &Position::Listed { cursor: None }).expect("sealed")).is_none());
    }
}
