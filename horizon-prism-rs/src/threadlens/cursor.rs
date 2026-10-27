//! The sealed paging cursor (design-v3 §5.15.14).
//!
//! A `moreReplies` marker can carry a cursor: where in a node's reply
//! sequence to go on reading. It is sealed with [`CursorKey`] and bound to
//! the URI of the node it was issued for, so a client can neither read it,
//! nor alter it, nor use it under another anchor. A cursor grants nothing:
//! every reply it leads to goes through the same verdicts and rules as on a
//! first request.
//!
//! The key is `APP_THREADLENS_CURSOR_KEY_HEX`. Without one, a key is made
//! at start, and cursors do not outlive the process.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use crate::cursor_key::{CursorKey, MAX_CURSOR_LEN};

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
    /// In the outline phase (thread outline design §5.3.4), after the
    /// outline-only reply at this `(created_at, uri)`; an empty `uri` is
    /// the phase's start. A process without this variant refuses such a
    /// cursor with a 400.
    #[serde(rename = "o")]
    Outline { at: DateTime<Utc>, uri: String },
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
        let outline = Position::Outline { at: DateTime::UNIX_EPOCH, uri: "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.feed.post/3kreply".into() };
        for position in [local, listed, Position::Listed { cursor: None }, outline] {
            let cursor = key.seal(NODE, &position).expect("sealed");
            assert!(cursor.len() <= MAX_CURSOR_LEN && !cursor.contains("3kreply"));
            assert_eq!(key.open(NODE, &cursor), Some(position.clone()));
            // Another anchor, another key, an altered cursor: refused.
            assert_eq!(key.open::<Position>(&format!("{NODE}x"), &cursor), None);
            assert_eq!(CursorKey::new(&[8; 32]).open::<Position>(NODE, &cursor), None);
            let mut altered = cursor.clone().into_bytes();
            altered[20] = if altered[20] == b'A' { b'B' } else { b'A' };
            assert_eq!(key.open::<Position>(NODE, &String::from_utf8(altered).expect("ascii")), None);
            // Two cursors for one position differ.
            assert_ne!(key.seal(NODE, &position), Some(cursor));
        }
        assert_eq!(key.open::<Position>(NODE, "not a cursor"), None);
        assert_eq!(key.open::<Position>(NODE, ""), None);
        // A position too long to send has no cursor.
        assert_eq!(key.seal(NODE, &Position::Listed { cursor: Some("x".repeat(2000)) }), None);
        assert!(CursorKey::generated().open::<Position>(NODE, &key.seal(NODE, &Position::Listed { cursor: None }).expect("sealed")).is_none());
    }
}
