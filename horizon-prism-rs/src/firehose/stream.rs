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

    /// The entry's relay-scoped, monotonic `seq` field.
    #[must_use]
    pub fn seq(&self) -> Option<i64> {
        self.field("seq").and_then(|s| s.parse().ok())
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

/// Parse an `XREADGROUP` or `XREAD` reply for one stream:
/// `[[stream, [entry…]]]`, or nil.
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
    /// refresher (§5.6). `active` is an `#account` event's flag, when it
    /// carries one (thread outline design §3.1); `status` its status text.
    Identity {
        kind: IdentityKind,
        did: String,
        relay: Option<String>,
        active: Option<bool>,
        status: Option<String>,
        cap: Option<Cap>,
    },
    /// A relay's `#sync`: the repo's state is now the one at `rev`.
    Sync(RepoSync),
    /// The relay reader's `resume`: events on one relay were not published.
    Resume(Resume),
}

impl Event {
    /// The entry's `_relay`.
    #[must_use]
    pub fn relay(&self) -> Option<&str> {
        match self {
            Self::Commit(c) => c.relay.as_deref(),
            Self::Identity { relay, .. } => relay.as_deref(),
            Self::Sync(s) => s.relay.as_deref(),
            Self::Resume(r) => r.relay.as_deref(),
        }
    }

    /// The entry's capability stamp, `None` from a relay reader older than
    /// stream capabilities.
    #[must_use]
    pub fn cap(&self) -> Option<&Cap> {
        match self {
            Self::Commit(c) => c.cap.as_ref(),
            Self::Identity { cap, .. } => cap.as_ref(),
            Self::Sync(s) => s.cap.as_ref(),
            Self::Resume(r) => r.cap.as_ref(),
        }
    }
}

/// The relay reader's capability stamp (`_cap`), carried by every entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cap {
    /// The stamp's format version.
    pub version: u64,
    /// The optional behaviours that are on (`sync`, `resume`,
    /// `oplessFiltered`).
    pub caps: Vec<String>,
    /// The record filter's NSIDs; `None` when the filter is empty, so every
    /// collection passes. A stamp that cannot be read is a filter that
    /// passes nothing.
    pub record_types: Option<std::collections::BTreeSet<String>>,
}

impl Cap {
    pub const SYNC: &'static str = "sync";
    pub const RESUME: &'static str = "resume";
    pub const OPLESS_FILTERED: &'static str = "oplessFiltered";

    /// Whether the behaviour `name` is on.
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        self.caps.iter().any(|c| c == name)
    }

    /// Read a `_cap` value. Anything but an object, or a `record_types`
    /// that is not a list of strings, reads as a filter that passes
    /// nothing, so a stamp Prism cannot read never counts as whole.
    #[must_use]
    pub fn from_json(v: &Json) -> Self {
        let unreadable = Self { version: 0, caps: Vec::new(), record_types: Some(std::collections::BTreeSet::new()) };
        let Some(obj) = v.as_object() else {
            return unreadable;
        };
        let version = obj.get("v").and_then(Json::as_u64).unwrap_or(0);
        let caps = obj
            .get("caps")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        let record_types = match obj.get("record_types") {
            None | Some(Json::Null) => None,
            Some(Json::Array(items)) => {
                let names: Option<std::collections::BTreeSet<String>> =
                    items.iter().map(|i| i.as_str().map(str::to_string)).collect();
                match names {
                    Some(n) => Some(n),
                    None => return unreadable,
                }
            }
            Some(_) => return unreadable,
        };
        Self { version, caps, record_types }
    }
}

fn cap_of(v: Option<&Json>) -> Option<Cap> {
    v.map(Cap::from_json)
}

/// A `sync` entry.
#[derive(Debug, Clone, PartialEq)]
pub struct RepoSync {
    pub did: String,
    pub rev: String,
    pub time: Option<DateTime<Utc>>,
    pub relay: Option<String>,
    pub cap: Option<Cap>,
}

/// Why a `resume` entry was published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeReason {
    /// The relay refused the saved cursor and continued from its oldest
    /// retained event.
    OutdatedCursor,
    /// A dial went out with no saved cursor.
    NoSavedCursor,
    /// The relay's sequence was reset.
    RelaySeqReset,
    /// A reason this version does not know.
    Other(String),
}

impl ResumeReason {
    fn parse(s: &str) -> Self {
        match s {
            "outdated_cursor" => Self::OutdatedCursor,
            "no_saved_cursor" => Self::NoSavedCursor,
            "relay_seq_reset" => Self::RelaySeqReset,
            other => Self::Other(other.to_string()),
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::OutdatedCursor => "outdated_cursor",
            Self::NoSavedCursor => "no_saved_cursor",
            Self::RelaySeqReset => "relay_seq_reset",
            Self::Other(s) => s,
        }
    }
}

/// A `resume` entry: events with seqs in `[first_lost_seq, continues_from)`
/// on one relay were not published.
#[derive(Debug, Clone, PartialEq)]
pub struct Resume {
    pub reason: ResumeReason,
    pub saved_cursor: Option<i64>,
    pub first_lost_seq: Option<i64>,
    pub continues_from: i64,
    pub relay: Option<String>,
    pub cap: Option<Cap>,
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
    /// The rev of the repo's previous commit, from the sidecar's `since`
    /// (degraded-mode design §2.4). `None` when absent, null or not a TID.
    pub since: Option<String>,
    /// `None` when `time` is absent or not RFC 3339.
    pub time: Option<DateTime<Utc>>,
    pub relay: Option<String>,
    pub ops: Vec<RawOp>,
    /// The entry's `_cap`.
    pub cap: Option<Cap>,
}

#[derive(Deserialize)]
struct CommitData {
    repo: String,
    rev: String,
    /// Any JSON: a value that is not a TID is ignored, never an error.
    #[serde(default)]
    since: Option<Json>,
    #[serde(default)]
    ops: Vec<OpData>,
    #[serde(default)]
    time: Option<String>,
    #[serde(default, rename = "_relay")]
    relay: Option<String>,
    #[serde(default, rename = "_cap")]
    cap: Option<Json>,
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
    /// `#account` only; anything but a boolean is no flag.
    #[serde(default)]
    active: Option<Json>,
    /// `#account` only; anything but a string is no status.
    #[serde(default)]
    status: Option<Json>,
    #[serde(default, rename = "_cap")]
    cap: Option<Json>,
}

#[derive(Deserialize)]
struct SyncData {
    did: String,
    rev: String,
    #[serde(default)]
    time: Option<String>,
    #[serde(default, rename = "_relay")]
    relay: Option<String>,
    #[serde(default, rename = "_cap")]
    cap: Option<Json>,
}

#[derive(Deserialize)]
struct ResumeData {
    reason: String,
    #[serde(default)]
    saved_cursor: Option<i64>,
    #[serde(default)]
    first_lost_seq: Option<i64>,
    continues_from: i64,
    #[serde(default, rename = "_relay")]
    relay: Option<String>,
    #[serde(default, rename = "_cap")]
    cap: Option<Json>,
}

fn parse_time(t: Option<&str>) -> Option<DateTime<Utc>> {
    t.and_then(|t| DateTime::parse_from_rfc3339(t).ok()).map(|t| t.with_timezone(&Utc))
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
        "sync" => return decode_sync(data).map(Event::Sync),
        "resume" => return decode_resume(data).map(Event::Resume),
        other => return Err(format!("unknown type {other:?}")),
    };
    let d: DidData = serde_json::from_slice(data).map_err(|e| format!("{ty} data: {e}"))?;
    if !proto_blue::syntax::Did::is_valid(&d.did) {
        return Err(format!("invalid did {:?}", d.did));
    }
    let (active, status) = match kind {
        IdentityKind::Account => (
            d.active.as_ref().and_then(Json::as_bool),
            d.status.as_ref().and_then(Json::as_str).map(str::to_string),
        ),
        _ => (None, None),
    };
    Ok(Event::Identity {
        kind,
        did: d.did,
        relay: d.relay,
        active,
        status,
        cap: cap_of(d.cap.as_ref()),
    })
}

fn decode_sync(data: &[u8]) -> Result<RepoSync, String> {
    let d: SyncData = serde_json::from_slice(data).map_err(|e| format!("sync data: {e}"))?;
    if !proto_blue::syntax::Did::is_valid(&d.did) {
        return Err(format!("invalid did {:?}", d.did));
    }
    if !proto_blue::syntax::Tid::is_valid(&d.rev) {
        return Err(format!("invalid rev {:?}", d.rev));
    }
    Ok(RepoSync {
        did: d.did,
        rev: d.rev,
        time: parse_time(d.time.as_deref()),
        relay: d.relay,
        cap: cap_of(d.cap.as_ref()),
    })
}

fn decode_resume(data: &[u8]) -> Result<Resume, String> {
    let d: ResumeData = serde_json::from_slice(data).map_err(|e| format!("resume data: {e}"))?;
    Ok(Resume {
        reason: ResumeReason::parse(&d.reason),
        saved_cursor: d.saved_cursor,
        first_lost_seq: d.first_lost_seq,
        continues_from: d.continues_from,
        relay: d.relay,
        cap: cap_of(d.cap.as_ref()),
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
        "identity" | "account" | "handle" | "tombstone" | "sync" => {
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
    let time = parse_time(c.time.as_deref());
    let since = c
        .since
        .as_ref()
        .and_then(Json::as_str)
        .filter(|s| proto_blue::syntax::Tid::is_valid(s))
        .map(str::to_string);
    Ok(Commit {
        repo: c.repo,
        rev: c.rev,
        since,
        time,
        relay: c.relay,
        ops,
        cap: cap_of(c.cap.as_ref()),
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

    /// T-DM2 (degraded-mode design §2.4): `since` present, null,
    /// malformed and absent. Only a TID is kept; nothing else fails the
    /// commit.
    #[test]
    fn decodes_since() {
        let since_of = |since: Option<Json>| {
            let mut data = json!({"repo": "did:plc:aaaa", "rev": "3kaaaaaaaaaab", "ops": []});
            if let Some(v) = since {
                data["since"] = v;
            }
            let Event::Commit(c) = decode(&entry("commit", &data)).expect("commit") else {
                panic!("expected commit");
            };
            c.since
        };
        assert_eq!(since_of(Some(json!("3kaaaaaaaaaaa"))), Some("3kaaaaaaaaaaa".to_string()));
        assert_eq!(since_of(Some(Json::Null)), None);
        assert_eq!(since_of(Some(json!("yesterday"))), None);
        assert_eq!(since_of(Some(json!(42))), None);
        assert_eq!(since_of(Some(json!({"rev": "3kaaaaaaaaaaa"}))), None);
        assert_eq!(since_of(None), None);
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
                    relay: Some("r".into()),
                    active: None,
                    status: None,
                    cap: None,
                }
            );
        }
    }

    /// Thread outline §3.1: an `#account` event's `active` flag and the
    /// entry's `seq`; other kinds carry no flag.
    #[test]
    fn decodes_account_active_and_seq() {
        let account = |data: &Json| match decode(&entry("account", data)).expect("event") {
            Event::Identity { active, .. } => active,
            other => panic!("{other:?}"),
        };
        assert_eq!(account(&json!({"did": "did:plc:aaaa", "active": false, "status": "takendown"})), Some(false));
        assert_eq!(account(&json!({"did": "did:plc:aaaa", "active": true})), Some(true));
        assert_eq!(account(&json!({"did": "did:plc:aaaa", "active": "yes"})), None);
        assert_eq!(account(&json!({"did": "did:plc:aaaa"})), None);
        let Event::Identity { active, .. } = decode(&entry("identity", &json!({"did": "did:plc:aaaa", "active": false}))).expect("event") else {
            panic!("identity");
        };
        assert_eq!(active, None);
        assert_eq!(entry("account", &json!({})).seq(), Some(42));
    }

    /// The relay reader's 0.2.0 entries: `sync`, `resume`, an op-less
    /// filtered commit, and `_cap` on every entry, read from the entry
    /// itself.
    #[test]
    fn decodes_stream_capability_entries() {
        let cap = json!({"v": 1, "caps": ["sync", "resume", "oplessFiltered"], "record_types": ["app.bsky.feed.post"]});
        let Event::Sync(s) = decode(&entry(
            "sync",
            &json!({"did": "did:plc:aaaa", "rev": "3kaaaaaaaaaab", "time": "2026-11-01T00:00:00.000Z", "_relay": "r", "_cap": cap}),
        ))
        .expect("sync") else {
            panic!("expected sync");
        };
        assert_eq!((s.did.as_str(), s.rev.as_str(), s.relay.as_deref()), ("did:plc:aaaa", "3kaaaaaaaaaab", Some("r")));
        assert!(s.time.is_some());
        let c = s.cap.expect("cap");
        assert_eq!(c.version, 1);
        assert!(c.has(Cap::SYNC) && c.has(Cap::RESUME) && c.has(Cap::OPLESS_FILTERED));
        assert_eq!(c.record_types, Some(["app.bsky.feed.post".to_string()].into_iter().collect()));
        // A sync with a bad rev or DID is refused.
        assert!(decode(&entry("sync", &json!({"did": "did:plc:aaaa", "rev": "nope"}))).is_err());
        assert!(decode(&entry("sync", &json!({"did": "bob", "rev": "3kaaaaaaaaaab"}))).is_err());
        assert_eq!(readable_despite(&entry("sync", &json!({"did": "bob", "rev": "3kaaaaaaaaaab"}))), Some(Readable::NotADid));

        let Event::Resume(r) = decode(&entry(
            "resume",
            &json!({"reason": "outdated_cursor", "saved_cursor": 10, "first_lost_seq": 11, "continues_from": 40, "_relay": "r",
                    "_cap": {"v": 1, "caps": ["resume"]}}),
        ))
        .expect("resume") else {
            panic!("expected resume");
        };
        assert_eq!(r.reason, ResumeReason::OutdatedCursor);
        assert_eq!((r.saved_cursor, r.first_lost_seq, r.continues_from), (Some(10), Some(11), 40));
        assert_eq!(r.cap.as_ref().map(|c| c.record_types.is_none()), Some(true));
        let Event::Resume(r) = decode(&entry("resume", &json!({"reason": "no_saved_cursor", "saved_cursor": null, "first_lost_seq": null, "continues_from": 7})))
            .expect("resume") else {
            panic!("expected resume");
        };
        assert_eq!((r.reason, r.saved_cursor, r.cap), (ResumeReason::NoSavedCursor, None, None));
        assert!(decode(&entry("resume", &json!({"reason": "outdated_cursor"}))).is_err());

        // An op-less filtered commit is an ordinary commit with no ops.
        let Event::Commit(c) = decode(&entry(
            "commit",
            &json!({"repo": "did:plc:aaaa", "commit": "bafyc", "rev": "3kaaaaaaaaaac", "since": "3kaaaaaaaaaab", "ops": [], "time": "2026-11-01T00:00:00.000Z", "_relay": "r", "_cap": cap}),
        ))
        .expect("commit") else {
            panic!("expected commit");
        };
        assert!(c.ops.is_empty());
        assert_eq!(c.since.as_deref(), Some("3kaaaaaaaaaab"));
        assert!(c.cap.is_some_and(|c| c.record_types.is_some()));

        // `_cap` on identity events, and an `#account` status.
        let Event::Identity { status, cap, .. } = decode(&entry(
            "account",
            &json!({"did": "did:plc:aaaa", "active": false, "status": "takendown", "_relay": "r", "_cap": {"v": 1, "caps": []}}),
        ))
        .expect("account") else {
            panic!("expected account");
        };
        assert_eq!(status.as_deref(), Some("takendown"));
        assert_eq!(cap, Some(Cap { version: 1, caps: Vec::new(), record_types: None }));
        // An entry from an older reader has no stamp.
        let Event::Commit(old) = decode(&entry("commit", &json!({"repo": "did:plc:aaaa", "rev": "3kaaaaaaaaaac", "ops": []}))).expect("commit") else {
            panic!("expected commit");
        };
        assert_eq!(old.cap, None);
    }

    /// A stamp Prism cannot read is a filter that passes nothing.
    #[test]
    fn an_unreadable_cap_passes_nothing() {
        for v in [json!("x"), json!({"v": 1, "record_types": "app.bsky.feed.post"}), json!({"v": 1, "record_types": [1]})] {
            assert_eq!(Cap::from_json(&v).record_types, Some(std::collections::BTreeSet::new()), "{v}");
        }
        assert_eq!(Cap::from_json(&json!({"v": 1, "caps": [], "record_types": null})).record_types, None);
    }
}
