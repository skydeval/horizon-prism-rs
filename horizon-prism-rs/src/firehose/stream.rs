//! The horizon-firehose stream contract (design-v2 §5.2): raw entries as read
//! from Redis, and the typed events they carry.
//!
//! Replies are parsed from `redis::Value` by hand because a pending entry that
//! was trimmed comes back as `[id, nil]` (§5.3), which typed stream replies
//! cannot represent.

use chrono::{DateTime, Utc};
use redis::Value;
use serde::Deserialize;
use serde_json::Value as Json;

use super::writer::{Action, RawOp};

/// One stream entry. `fields` is `None` for a trimmed pending entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Option<Vec<(String, Vec<u8>)>>,
}

impl StreamEntry {
    /// A field as UTF-8 text.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&str> {
        self.raw(name).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// A field's bytes.
    #[must_use]
    pub fn raw(&self, name: &str) -> Option<&[u8]> {
        self.fields
            .as_ref()?
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_slice())
    }

    /// The stream id's millisecond part (the Redis clock at XADD).
    #[must_use]
    pub fn id_ms(&self) -> Option<i64> {
        self.id.split_once('-').and_then(|(ms, _)| ms.parse().ok())
    }
}

fn bytes(v: Value) -> Option<Vec<u8>> {
    match v {
        Value::BulkString(b) => Some(b),
        Value::SimpleString(s) => Some(s.into_bytes()),
        _ => None,
    }
}

/// Parse one `[id, fields|nil]` entry.
///
/// # Errors
///
/// A description of the unexpected shape.
pub fn parse_entry(v: Value) -> Result<StreamEntry, String> {
    let Value::Array(parts) = v else {
        return Err(format!("entry is not an array: {v:?}"));
    };
    let mut it = parts.into_iter();
    let id = it
        .next()
        .and_then(bytes)
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or("entry without id")?;
    let fields = match it.next() {
        None | Some(Value::Nil) => None,
        Some(Value::Array(kv)) => {
            let mut out = Vec::with_capacity(kv.len() / 2);
            let mut kv = kv.into_iter();
            while let (Some(k), Some(v)) = (kv.next(), kv.next()) {
                let k = bytes(k)
                    .and_then(|b| String::from_utf8(b).ok())
                    .ok_or("non-text field name")?;
                out.push((k, bytes(v).ok_or("non-bulk field value")?));
            }
            Some(out)
        }
        Some(other) => return Err(format!("entry fields: {other:?}")),
    };
    Ok(StreamEntry { id, fields })
}

/// Parse an `XREADGROUP` reply for one stream: `[[stream, [entry…]]]`, or nil.
///
/// # Errors
///
/// A description of the unexpected shape.
pub fn parse_read_reply(v: Value) -> Result<Vec<StreamEntry>, String> {
    let streams = match v {
        Value::Nil => return Ok(Vec::new()),
        Value::Array(s) => s,
        // RESP3 map form.
        Value::Map(m) => m.into_iter().map(|(k, v)| Value::Array(vec![k, v])).collect(),
        other => return Err(format!("read reply: {other:?}")),
    };
    let mut out = Vec::new();
    for s in streams {
        let Value::Array(pair) = s else {
            return Err("stream element is not an array".into());
        };
        let Some(Value::Array(entries)) = pair.into_iter().nth(1) else {
            return Err("stream element without entries".into());
        };
        for e in entries {
            out.push(parse_entry(e)?);
        }
    }
    Ok(out)
}

/// A decoded stream event.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Commit(Commit),
    /// `identity`, `account`, `handle` or `tombstone`: routed to the identity
    /// refresher (§5.6).
    Identity { kind: IdentityKind, did: String, relay: Option<String> },
}

/// The non-commit event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityKind {
    Identity,
    Account,
    Handle,
    Tombstone,
}

impl IdentityKind {
    /// The `identity_refresh.reasons` bit (§4.2).
    #[must_use]
    pub fn reason_bit(self) -> i16 {
        use crate::identity::refresher::RefreshReason;
        match self {
            Self::Identity => RefreshReason::IDENTITY,
            Self::Account => RefreshReason::ACCOUNT,
            Self::Handle => RefreshReason::HANDLE,
            Self::Tombstone => RefreshReason::TOMBSTONE,
        }
    }
}

/// A commit event.
#[derive(Debug, Clone, PartialEq)]
pub struct Commit {
    pub repo: String,
    pub rev: String,
    /// `None` when `time` is absent or not RFC 3339.
    pub time: Option<DateTime<Utc>>,
    pub relay: Option<String>,
    pub ops: Vec<RawOp>,
}

#[derive(Deserialize)]
struct CommitData {
    repo: String,
    rev: String,
    #[serde(default)]
    ops: Vec<OpData>,
    #[serde(default)]
    time: Option<String>,
    #[serde(default, rename = "_relay")]
    relay: Option<String>,
}

#[derive(Deserialize)]
struct OpData {
    action: String,
    path: String,
    #[serde(default)]
    cid: Option<Json>,
    #[serde(default)]
    record: Option<Json>,
}

#[derive(Deserialize)]
struct DidData {
    did: String,
    #[serde(default, rename = "_relay")]
    relay: Option<String>,
}

/// Decode an entry. Errors are deterministic (§5.2 contract stability).
///
/// # Errors
///
/// Unknown `type`, missing `data`, unparseable JSON, a non-DID repo, an
/// invalid rev, or a malformed op.
pub fn decode(entry: &StreamEntry) -> Result<Event, String> {
    let ty = entry.field("type").ok_or("missing type")?;
    let data = entry.raw("data").ok_or("missing data")?;
    let kind = match ty {
        "commit" => return decode_commit(data).map(Event::Commit),
        "identity" => IdentityKind::Identity,
        "account" => IdentityKind::Account,
        "handle" => IdentityKind::Handle,
        "tombstone" => IdentityKind::Tombstone,
        other => return Err(format!("unknown type {other:?}")),
    };
    let d: DidData = serde_json::from_slice(data).map_err(|e| format!("{ty} data: {e}"))?;
    if !proto_blue::syntax::Did::is_valid(&d.did) {
        return Err(format!("invalid did {:?}", d.did));
    }
    Ok(Event::Identity {
        kind,
        did: d.did,
        relay: d.relay,
    })
}

/// What a refused entry still lets Prism read (actionable-warnings design
/// §6.1), so the consumer can tell whether it could matter.
#[derive(Debug, Clone, PartialEq)]
pub enum Readable {
    /// A commit refused for its rev alone: its repo and ops, as `decode`
    /// would have given them.
    Commit { repo: String, ops: Vec<RawOp> },
    /// An identity, account, handle or tombstone event whose `did` is not a
    /// DID: it can name no account Prism knows.
    NotADid,
}

/// For an entry [`decode`] refused: what can still be read, or `None` when
/// the repo or an op cannot be (such an entry stays a whole-entry dead
/// letter). Called only after `decode` failed, so a commit that decodes
/// here was refused for its rev alone; no error text is matched.
#[must_use]
pub fn readable_despite(entry: &StreamEntry) -> Option<Readable> {
    let data = entry.raw("data")?;
    match entry.field("type")? {
        "commit" => decode_commit_checked(data, false).ok().map(|c| Readable::Commit { repo: c.repo, ops: c.ops }),
        "identity" | "account" | "handle" | "tombstone" => {
            let d: DidData = serde_json::from_slice(data).ok()?;
            (!proto_blue::syntax::Did::is_valid(&d.did)).then_some(Readable::NotADid)
        }
        _ => None,
    }
}

fn decode_commit(data: &[u8]) -> Result<Commit, String> {
    decode_commit_checked(data, true)
}

/// [`decode_commit`], with the rev check when `check_rev`.
fn decode_commit_checked(data: &[u8], check_rev: bool) -> Result<Commit, String> {
    let c: CommitData = serde_json::from_slice(data).map_err(|e| format!("commit data: {e}"))?;
    if !proto_blue::syntax::Did::is_valid(&c.repo) {
        return Err(format!("invalid repo {:?}", c.repo));
    }
    if check_rev && !proto_blue::syntax::Tid::is_valid(&c.rev) {
        return Err(format!("invalid rev {:?}", c.rev));
    }
    let ops = c
        .ops
        .into_iter()
        .map(|op| {
            let action = Action::parse(&op.action).ok_or_else(|| format!("unknown action {:?}", op.action))?;
            let (collection, rkey) = op
                .path
                .split_once('/')
                .ok_or_else(|| format!("invalid path {:?}", op.path))?;
            Ok(RawOp {
                action,
                collection: collection.to_string(),
                rkey: rkey.to_string(),
                cid: op.cid.as_ref().and_then(super::normalize::cid_link),
                record: op.record.filter(|r| !r.is_null()),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let time = c
        .time
        .as_deref()
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&Utc));
    Ok(Commit {
        repo: c.repo,
        rev: c.rev,
        time,
        relay: c.relay,
        ops,
    })
}

/// Best-effort `repo`/`rev` of an entry for dead-letter fields, when the full
/// decode failed.
#[must_use]
pub fn repo_hint(entry: &StreamEntry) -> (String, String) {
    let parsed: Option<Json> = entry.raw("data").and_then(|d| serde_json::from_slice(d).ok());
    let get = |k: &str| {
        parsed
            .as_ref()
            .and_then(|p| p.get(k))
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string()
    };
    (get("repo"), get("rev"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn entry(ty: &str, data: &Json) -> StreamEntry {
        StreamEntry {
            id: "1700000000000-3".into(),
            fields: Some(vec![
                ("type".into(), ty.as_bytes().to_vec()),
                ("data".into(), serde_json::to_vec(data).expect("json")),
                ("seq".into(), b"42".to_vec()),
            ]),
        }
    }

    #[test]
    fn parses_nil_pending_entries() {
        let v = Value::Array(vec![Value::BulkString(b"1-1".to_vec()), Value::Nil]);
        let e = parse_entry(v).expect("entry");
        assert_eq!(e.id, "1-1");
        assert_eq!(e.fields, None);
        assert_eq!(parse_read_reply(Value::Nil).expect("nil"), Vec::new());
    }

    #[test]
    fn decodes_a_commit() {
        let data = json!({
            "repo": "did:plc:aaaa", "commit": "bafycommit", "rev": "3kaaaaaaaaaaa",
            "ops": [
                {"action": "create", "path": "app.bsky.feed.post/3kbbbbbbbbbbb", "cid": "bafyop", "record": {"text": "hi"}},
                {"action": "delete", "path": "app.bsky.feed.like/3kccccccccccc", "cid": null, "record": null}
            ],
            "time": "2026-09-26T12:00:00.123Z", "_relay": "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
        });
        let e = entry("commit", &data);
        assert_eq!(e.id_ms(), Some(1_700_000_000_000));
        let Event::Commit(c) = decode(&e).expect("commit") else {
            panic!("expected commit");
        };
        assert_eq!(c.repo, "did:plc:aaaa");
        assert_eq!(c.ops.len(), 2);
        assert_eq!(c.ops[0].action, Action::Create);
        assert_eq!(c.ops[0].collection, "app.bsky.feed.post");
        assert_eq!(c.ops[0].cid.as_deref(), Some("bafyop"));
        assert_eq!(c.ops[1].record, None);
        assert!(c.time.is_some());
    }

    #[test]
    fn unparseable_time_is_none_not_an_error() {
        let data = json!({"repo": "did:plc:aaaa", "rev": "3kaaaaaaaaaaa", "ops": [], "time": "yesterday"});
        let Event::Commit(c) = decode(&entry("commit", &data)).expect("commit") else {
            panic!("expected commit");
        };
        assert_eq!(c.time, None);
    }

    #[test]
    fn deterministic_decode_failures() {
        assert!(decode(&entry("sync", &json!({}))).is_err());
        assert!(decode(&entry("commit", &json!({"repo": "bob", "rev": "3kaaaaaaaaaaa"}))).is_err());
        assert!(decode(&entry("commit", &json!({"repo": "did:plc:a", "rev": "nope"}))).is_err());
        assert!(
            decode(&entry("commit", &json!({"repo": "did:plc:a", "rev": "3kaaaaaaaaaaa",
                "ops": [{"action": "move", "path": "a/b"}]})))
            .is_err()
        );
        let bad = StreamEntry {
            id: "1-0".into(),
            fields: Some(vec![("type".into(), b"commit".to_vec()), ("data".into(), b"{".to_vec())]),
        };
        assert!(decode(&bad).is_err());
        assert_eq!(repo_hint(&bad), (String::new(), String::new()));
    }

    /// T-AW9 (actionable-warnings design §6.1): a commit refused for its
    /// rev alone gives its repo and ops; anything else unreadable gives
    /// nothing; an event naming no DID is `NotADid`.
    #[test]
    fn readable_despite_a_refusal() {
        let ops = json!([{"action": "create", "path": "app.bsky.feed.like/3kbbbbbbbbbbb", "cid": "bafyop", "record": {"subject": {}}}]);
        let bad_rev = entry("commit", &json!({"repo": "did:plc:aaaa", "rev": "3lxcday8ef4p4", "ops": ops}));
        assert!(decode(&bad_rev).is_err());
        let good = entry("commit", &json!({"repo": "did:plc:aaaa", "rev": "3kaaaaaaaaaaa", "ops": ops}));
        let Event::Commit(c) = decode(&good).expect("commit") else {
            panic!("expected a commit");
        };
        assert_eq!(readable_despite(&bad_rev), Some(Readable::Commit { repo: "did:plc:aaaa".into(), ops: c.ops }));
        for refused in [
            entry("commit", &json!({"repo": "bob", "rev": "nope"})),
            entry("commit", &json!({"repo": "did:plc:a", "rev": "nope", "ops": [{"action": "move", "path": "a/b"}]})),
            entry("commit", &json!({"repo": "did:plc:a", "rev": "nope", "ops": [{"action": "create", "path": "nopath"}]})),
            entry("sync", &json!({"did": "did:plc:a"})),
            StreamEntry { id: "1-0".into(), fields: Some(vec![("type".into(), b"commit".to_vec()), ("data".into(), b"{".to_vec())]) },
        ] {
            assert!(decode(&refused).is_err());
            assert_eq!(readable_despite(&refused), None, "{refused:?}");
        }
        let bad_did = entry("account", &json!({"did": "bob"}));
        assert!(decode(&bad_did).is_err());
        assert_eq!(readable_despite(&bad_did), Some(Readable::NotADid));
    }

    #[test]
    fn decodes_identity_events() {
        for (ty, kind) in [
            ("identity", IdentityKind::Identity),
            ("account", IdentityKind::Account),
            ("handle", IdentityKind::Handle),
            ("tombstone", IdentityKind::Tombstone),
        ] {
            let e = decode(&entry(ty, &json!({"did": "did:plc:aaaa", "_relay": "r"}))).expect("event");
            assert_eq!(
                e,
                Event::Identity {
                    kind,
                    did: "did:plc:aaaa".into(),
                    relay: Some("r".into())
                }
            );
        }
    }
}
