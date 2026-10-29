//! The live reader's lean decode (public-page design §6.2): each stream
//! entry becomes at most one public line, plus what the figures need, from
//! the entry's own fields only. Records, CIDs and every other field are
//! scanned and skipped (`serde`'s ignored fields), never built; strings
//! borrow from the entry where they can. No DID, rev or TID check: a line
//! shows what the relay sent, bounded for display.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::firehose::classifier::classify;
use crate::firehose::feed::Kind;
use crate::firehose::stream::StreamEntry;

/// The longest NSID shown, in bytes (the NSID limit).
pub const NSID_MAX: usize = 317;
/// The longest DID shown, in bytes.
pub const DID_MAX: usize = 256;
/// The key of a relay whose URL is not a configured one.
pub const OTHER: &str = crate::config::OTHER_RELAY_LABEL;
/// The prefix whose commits are hidden from the lines;
/// they still count in the arrivals.
pub const HIDDEN_PREFIX: &str = "tools.kryphocron.";

/// A line's action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Create,
    Update,
    Delete,
    /// `#account` with `active: true`.
    Active,
    /// `#account` with `active: false`.
    Inactive,
    /// Anything else: shown as `""`.
    None,
}

impl Action {
    /// The wire letter.
    #[must_use]
    pub fn letter(self) -> &'static str {
        match self {
            Self::Create => "c",
            Self::Update => "u",
            Self::Delete => "d",
            Self::Active => "a",
            Self::Inactive => "i",
            Self::None => "",
        }
    }

    fn of_op(action: Option<&str>) -> Self {
        match action {
            Some("create") => Self::Create,
            Some("update") => Self::Update,
            Some("delete") => Self::Delete,
            _ => Self::None,
        }
    }
}

/// One public line: the event's own fields only (§6.4), no relay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicLine {
    /// The stream id's millisecond part: the sidecar's `XADD` time.
    pub entry_ms: i64,
    pub action: Action,
    /// The first op's collection, or the event type (`#identity`, …).
    pub nsid: String,
    /// The repo or DID, as sent.
    pub did: String,
}

/// What one decodable entry gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub entry_ms: i64,
    /// The relay, as the reader keys it (its URL's host, or [`OTHER`]), for
    /// the per-relay figures; never part of a line.
    pub relay: Arc<str>,
    /// Each shown op's kind; empty for an identity event.
    pub kinds: Vec<Kind>,
    /// An identity, handle, account or tombstone event.
    pub identity: bool,
    /// For a commit with a `time`: `max(0, entry_ms − time)`.
    pub delay_ms: Option<i64>,
    /// `None` for a hidden or empty commit.
    pub line: Option<PublicLine>,
}

/// A JSON value that is a string, or anything else (skipped).
#[derive(Debug)]
enum Text<'a> {
    Str(Cow<'a, str>),
    Other,
}

impl<'de: 'a, 'a> Deserialize<'de> for Text<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Text<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("any value")
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
                Ok(Text::Str(Cow::Borrowed(v)))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Text::Str(Cow::Owned(v.to_string())))
            }
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(Text::Str(Cow::Owned(v)))
            }
            fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
                Ok(Text::Other)
            }
            fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
                Ok(Text::Other)
            }
            fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
                Ok(Text::Other)
            }
            fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
                Ok(Text::Other)
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(Text::Other)
            }
            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(Text::Other)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(Text::Other)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                while map.next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?.is_some() {}
                Ok(Text::Other)
            }
        }
        d.deserialize_any(V)
    }
}

impl<'a> Text<'a> {
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            Self::Other => None,
        }
    }
}

/// A string field that must be a string when present.
#[derive(Debug)]
struct Strict<'a>(Cow<'a, str>);

impl<'de: 'a, 'a> Deserialize<'de> for Strict<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Text::deserialize(d)? {
            Text::Str(s) => Ok(Self(s)),
            Text::Other => Err(serde::de::Error::custom("not a string")),
        }
    }
}

#[derive(Deserialize)]
struct LeanOp<'a> {
    #[serde(borrow)]
    path: Strict<'a>,
    #[serde(borrow, default)]
    action: Option<Text<'a>>,
}

#[derive(Deserialize)]
struct LeanData<'a> {
    #[serde(borrow, default)]
    repo: Option<Strict<'a>>,
    #[serde(borrow, default)]
    did: Option<Strict<'a>>,
    #[serde(borrow, default)]
    time: Option<Text<'a>>,
    #[serde(borrow, default, rename = "_relay")]
    relay: Option<Text<'a>>,
    #[serde(default)]
    active: Option<serde_json::Value>,
    #[serde(borrow, default)]
    ops: Option<Vec<LeanOp<'a>>>,
}

/// Relay URL → the key the reader counts it under: the URL's host (`public_page::relay_hosts`).
#[derive(Debug, Clone, Default)]
pub struct Labels(HashMap<String, Arc<str>>, Arc<str>);

impl Labels {
    #[must_use]
    pub fn new(map: &std::collections::BTreeMap<String, String>) -> Self {
        Self(map.iter().map(|(url, label)| (url.clone(), Arc::from(label.as_str()))).collect(), Arc::from(OTHER))
    }

    fn of(&self, relay: Option<&str>) -> Arc<str> {
        relay.and_then(|r| self.0.get(r)).map_or_else(|| Arc::clone(&self.1), Arc::clone)
    }
}

/// The part of `path` before the first `/`, or the whole path.
#[must_use]
pub fn collection(path: &str) -> &str {
    path.split_once('/').map_or(path, |(c, _)| c)
}

/// Whether a collection is hidden from the lines: a prefix match,
/// case-insensitive, so unregistered names under the prefix are too.
#[must_use]
pub fn hidden(collection: &str) -> bool {
    collection.len() >= HIDDEN_PREFIX.len()
        && collection.as_bytes()[..HIDDEN_PREFIX.len()].eq_ignore_ascii_case(HIDDEN_PREFIX.as_bytes())
}

fn replaced(c: char) -> char {
    let n = u32::from(c);
    let control = n < 0x20 || (0x7f..=0x9f).contains(&n);
    let bidi = matches!(n, 0x200e..=0x200f | 0x202a..=0x202e | 0x2066..=0x2069);
    if control || bidi { '\u{fffd}' } else { c }
}

/// A field for display: control and bidi characters replaced with U+FFFD,
/// then cut to `max` bytes on a character boundary with a trailing `…`.
#[must_use]
pub fn bounded(s: &str, max: usize) -> String {
    let clean: String = s.chars().map(replaced).collect();
    if clean.len() <= max {
        return clean;
    }
    let mut end = max;
    while !clean.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = clean[..end].to_string();
    out.push('…');
    out
}

/// Decode one entry; `None` when it is undecodable (§6.2): no `type`, an
/// unknown `type`, no `data`, unparseable JSON, `ops` that is not an
/// array, a non-string `repo` or `did`, or an op without a string `path`.
#[must_use]
pub fn decode(entry: &StreamEntry, labels: &Labels) -> Option<Decoded> {
    let entry_ms = entry.id_ms()?;
    let ty = entry.field("type")?;
    let event = match ty {
        "commit" => None,
        "identity" => Some("#identity"),
        "handle" => Some("#handle"),
        "account" => Some("#account"),
        "tombstone" => Some("#tombstone"),
        _ => return None,
    };
    let data: LeanData<'_> = serde_json::from_slice(entry.raw("data")?).ok()?;
    let relay = labels.of(data.relay.as_ref().and_then(Text::as_str));
    if let Some(nsid) = event {
        let did = data.did.map(|d| d.0).unwrap_or_default();
        let action = match (nsid, data.active.as_ref().and_then(serde_json::Value::as_bool)) {
            ("#account", Some(true)) => Action::Active,
            ("#account", Some(false)) => Action::Inactive,
            _ => Action::None,
        };
        return Some(Decoded {
            entry_ms,
            relay,
            kinds: Vec::new(),
            identity: true,
            delay_ms: None,
            line: Some(PublicLine { entry_ms, action, nsid: nsid.to_string(), did: bounded(&did, DID_MAX) }),
        });
    }
    let ops = data.ops.unwrap_or_default();
    let kinds = ops
        .iter()
        .filter(|op| !hidden(collection(&op.path.0)))
        .map(|op| {
            let (nsid, rkey) = op.path.0.split_once('/').unwrap_or((&op.path.0, ""));
            Kind::from_class(classify(nsid, rkey))
        })
        .collect();
    let delay_ms = data
        .time
        .as_ref()
        .and_then(Text::as_str)
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| (entry_ms - t.timestamp_millis()).max(0));
    let line = match ops.first() {
        Some(first) if !ops.iter().any(|op| hidden(collection(&op.path.0))) => Some(PublicLine {
            entry_ms,
            action: Action::of_op(first.action.as_ref().and_then(Text::as_str)),
            nsid: bounded(collection(&first.path.0), NSID_MAX),
            did: bounded(&data.repo.map(|r| r.0).unwrap_or_default(), DID_MAX),
        }),
        _ => None,
    };
    Some(Decoded { entry_ms, relay, kinds, identity: false, delay_ms, line })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, ty: Option<&str>, data: Option<&str>) -> StreamEntry {
        let mut fields = Vec::new();
        if let Some(t) = ty {
            fields.push(("type".to_string(), t.as_bytes().to_vec()));
        }
        if let Some(d) = data {
            fields.push(("data".to_string(), d.as_bytes().to_vec()));
        }
        StreamEntry { id: id.to_string(), fields: Some(fields) }
    }

    fn labels() -> Labels {
        Labels::new(&std::collections::BTreeMap::from([("wss://relay.one".to_string(), "one".to_string())]))
    }

    /// T-P19a, undecodable and odd-but-decodable entries.
    #[test]
    fn edges() {
        let l = labels();
        for (ty, data) in [
            (None, Some(r#"{"repo":"did:plc:a","ops":[]}"#)),
            (Some("labels"), Some("{}")),
            (Some("commit"), Some("{not json")),
            (Some("commit"), Some(r#"{"repo":"did:plc:a","ops":[{"action":"create"}]}"#)),
            (Some("commit"), Some(r#"{"repo":"did:plc:a","ops":{"path":"x/y"}}"#)),
            (Some("commit"), Some(r#"{"repo":7,"ops":[]}"#)),
            (Some("identity"), Some(r#"{"did":["x"]}"#)),
            (Some("commit"), None),
        ] {
            assert!(decode(&entry("1-0", ty, data), &l).is_none(), "{ty:?} {data:?}");
        }
        let d = |data: &str| decode(&entry("1000-0", Some("commit"), Some(data)), &l).expect("decodes");
        assert_eq!(d(r#"{"repo":"did:plc:a","ops":[{"action":"create","path":"tools.kryphocron.feed.postPrivate"}]}"#).line, None);
        assert_eq!(d(r#"{"repo":"did:plc:a","ops":[{"action":"create","path":"TOOLS.Kryphocron.feed.x/1"}]}"#).line, None);
        let key = d(r#"{"repo":"did:key:z6MkTest","ops":[{"action":"create","path":"app.bsky.feed.like/1"}]}"#).line.expect("line");
        assert_eq!(key.did, "did:key:z6MkTest");
        let hello = d(r#"{"repo":"hello","ops":[{"action":"create","path":"app.bsky.feed.like/1"}],"_relay":"wss://relay.one","record":{"text":"x"}}"#);
        let line = hello.line.expect("line");
        assert_eq!((line.did.as_str(), &*hello.relay, line.nsid.as_str(), line.action), ("hello", "one", "app.bsky.feed.like", Action::Create));
        assert_eq!(hello.kinds, vec![Kind::Like]);
        let unknown = d(r#"{"repo":"did:plc:a","ops":[{"action":"frob","path":"a.b.c/1"}],"_relay":"wss://elsewhere"}"#);
        assert_eq!((unknown.line.as_ref().map(|l| l.action), &*unknown.relay), (Some(Action::None), OTHER));
        // An empty commit gives no line; it still decodes.
        assert_eq!(d(r#"{"repo":"did:plc:a","ops":[]}"#).line, None);
        // The delay never goes negative.
        assert_eq!(d(r#"{"repo":"did:plc:a","ops":[],"time":"2100-01-01T00:00:00Z"}"#).delay_ms, Some(0));
        assert_eq!(d(r#"{"repo":"did:plc:a","ops":[],"time":"1970-01-01T00:00:00.500Z"}"#).delay_ms, Some(500));
        // Identity events.
        let acct = decode(&entry("5-0", Some("account"), Some(r#"{"did":"did:plc:b","active":false}"#)), &l).expect("decodes");
        let line = acct.line.expect("line");
        assert_eq!((line.nsid.as_str(), line.action, acct.identity), ("#account", Action::Inactive, true));
    }

    /// T-P19, display bounds.
    #[test]
    fn bounds() {
        let long = "a".repeat(1000);
        let cut = bounded(&long, NSID_MAX);
        assert_eq!(cut.len(), NSID_MAX + '…'.len_utf8());
        assert!(cut.ends_with('…'));
        assert_eq!(bounded(&"d".repeat(2048), DID_MAX).trim_end_matches('…').len(), DID_MAX);
        assert_eq!(bounded("x\u{202e}y\u{0007}z\u{85}", 100), "x\u{fffd}y\u{fffd}z\u{fffd}");
        // A cut never splits a character.
        let e = "é".repeat(200);
        assert!(bounded(&e, 317).trim_end_matches('…').chars().all(|c| c == 'é'));
    }
}
