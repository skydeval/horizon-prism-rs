//! Outline ops (thread outline design §2, §3.1): what one stream entry adds
//! to the outline, collected after `prepare` from the firehose's own view of
//! each op, plus what a refused or dead-lettered entry still lets Prism read.
//!
//! Only `app.bsky.feed.post` is collected. A post whose own key, parent or
//! root cannot be a row records a hole where a reader would otherwise miss
//! it (§2's table).

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::firehose::classifier::NSID_POST;
use crate::firehose::stream::{Event, IdentityKind, Readable, StreamEntry};
use crate::firehose::writer::{Action, RawOp};

/// A post's place: its author's DID and its decoded TID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    pub did: String,
    pub rkey: i64,
}

/// A post create or update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostOp {
    pub author: String,
    pub rkey: i64,
    /// `None` for a thread root; with it, `root` is set too.
    pub parent: Option<Key>,
    pub root: Option<Key>,
    pub created_at: DateTime<Utc>,
    /// The commit's rev, decoded.
    pub rev: i64,
}

impl PostOp {
    #[must_use]
    pub fn key(&self) -> Key {
        Key { did: self.author.clone(), rkey: self.rkey }
    }

    /// Where a hole goes when this op cannot be written: its parent, else
    /// its root (§2's table, the flush row).
    #[must_use]
    pub fn hole_node(&self) -> Option<Key> {
        self.parent.clone().or_else(|| self.root.clone())
    }
}

/// Why a hole was recorded (`outline_holes_total{reason}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HoleReason {
    /// An op §2's table leaves out but whose thread it belongs to.
    LeftOut,
    /// A post op of a commit refused for a bad rev (R4-11).
    BadRev,
    /// A post create or update in a parseable dead letter (R5-5).
    DeadLetter,
    /// An op the flush could not write (§3.4).
    Flush,
    /// A replay range trimmed from the stream: time-wide, no row (N4).
    Trimmed,
}

impl HoleReason {
    pub const ALL: [Self; 5] = [Self::LeftOut, Self::BadRev, Self::DeadLetter, Self::Flush, Self::Trimmed];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LeftOut => "left_out",
            Self::BadRev => "bad_rev",
            Self::DeadLetter => "dead_letter",
            Self::Flush => "flush",
            Self::Trimmed => "trimmed",
        }
    }
}

/// Why a post op was missed (`outline_missed_total{reason}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MissedReason {
    /// A post create or update with no record or cid.
    NoRecord,
    /// An op the flush could not write.
    Flush,
    /// A post op of a commit refused for a bad rev.
    BadRev,
    /// A dead letter whose data does not parse.
    DeadLetter,
}

impl MissedReason {
    pub const ALL: [Self; 4] = [Self::NoRecord, Self::Flush, Self::BadRev, Self::DeadLetter];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoRecord => "no_record",
            Self::Flush => "flush",
            Self::BadRev => "bad_rev",
            Self::DeadLetter => "dead_letter",
        }
    }
}

/// One outline op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Create(PostOp),
    Update(PostOp),
    Delete { author: String, rkey: i64, rev: i64 },
    /// A node known to miss a reply under it.
    Hole { node: Key, reason: HoleReason },
    /// A post delete in a commit refused for a bad rev: a hole at the parent
    /// its existing row names, else nothing (N5).
    HoleAtParentOf { node: Key },
    /// A relay's account status for a DID (R4-2, R5-1).
    Status { did: String, relay: String, active: bool, seq: i64 },
    /// A post op left out with nothing to record but a count.
    Missed(MissedReason),
}

impl Op {
    /// The op's label in `outline_ops_total{op}`.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Create(_) => "create",
            Self::Update(_) => "update",
            Self::Delete { .. } => "delete",
            Self::Hole { .. } | Self::HoleAtParentOf { .. } => "hole",
            Self::Status { .. } => "status",
            Self::Missed(_) => "missed",
        }
    }
}

/// The labels of `outline_ops_total{op}`.
pub const OP_LABELS: [&str; 6] = ["create", "update", "delete", "hole", "status", "missed"];

/// A TID decoded to the integer it encodes, or `None` when `s` is not a TID.
#[must_use]
pub fn tid_value(s: &str) -> Option<i64> {
    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    if !proto_blue::syntax::Tid::is_valid(s) {
        return None;
    }
    let mut v: u64 = 0;
    for c in s.bytes() {
        let d = ALPHABET.iter().position(|a| *a == c)?;
        v = (v << 5) | u64::try_from(d).ok()?;
    }
    // A valid TID's first character keeps the top bit clear.
    i64::try_from(v).ok()
}

/// The time a decoded TID carries (its microseconds).
#[must_use]
pub fn tid_time(v: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_micros(v >> 10)
}

/// What an `at://` URI names as an outline node.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ref {
    /// Not an `app.bsky.feed.post` by a DID: it belongs to no post thread.
    NotPost,
    /// A post whose rkey is not a TID: it can be no row.
    NonTid,
    Post(Key),
}

fn post_ref(uri: &str) -> Ref {
    let Some(rest) = uri.strip_prefix("at://") else {
        return Ref::NotPost;
    };
    let mut parts = rest.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        // A valid DID only: raw refs come from records no parser checked,
        // and a forged authority would grow `outline_actors` (or, with a
        // NUL byte, fail every flush it is in).
        (Some(did), Some(NSID_POST), Some(rkey), None) if proto_blue::syntax::Did::is_valid(did) => match tid_value(rkey) {
            Some(rkey) => Ref::Post(Key { did: did.to_string(), rkey }),
            None => Ref::NonTid,
        },
        _ => Ref::NotPost,
    }
}

/// The TID post a raw `record.reply.<field>.uri` names.
fn raw_ref(record: &Value, field: &str) -> Option<Key> {
    let uri = record.get("reply")?.get(field)?.get("uri")?.as_str()?;
    match post_ref(uri) {
        Ref::Post(k) => Some(k),
        Ref::NotPost | Ref::NonTid => None,
    }
}

/// A hole at the raw record's parent, else its root.
fn raw_hole(record: Option<&Value>, reason: HoleReason) -> Option<Op> {
    let record = record?;
    raw_ref(record, "parent")
        .or_else(|| raw_ref(record, "root"))
        .map(|node| Op::Hole { node, reason })
}

/// How the firehose's `prepare` saw one op, for the outline.
#[derive(Debug, Clone, Copy)]
pub enum Seen<'a> {
    /// A post create or update that parsed, with its reply refs.
    Post { created: bool, parent: Option<&'a str>, root: Option<&'a str>, created_at: DateTime<Utc> },
    /// A delete `prepare` accepted.
    Delete,
    /// An op `prepare` refused as invalid.
    Invalid,
    /// Anything else.
    Other,
}

/// The outline ops of one commit's ops for `repo` at `rev`, in op order
/// (§3.1's table, §2's left-out rows).
#[must_use]
pub fn post_ops<'a>(repo: &str, rev: &str, ops: &[RawOp], seen: impl IntoIterator<Item = Seen<'a>>) -> Vec<Op> {
    let Some(rev) = tid_value(rev) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (op, seen) in ops.iter().zip(seen) {
        if op.collection != NSID_POST {
            continue;
        }
        let own = tid_value(&op.rkey);
        match seen {
            Seen::Post { created, parent, root, created_at } => {
                if let Some(o) = post_op(repo, own, rev, created, parent, root, created_at) {
                    out.push(o);
                }
            }
            Seen::Delete => {
                if let Some(rkey) = own {
                    out.push(Op::Delete { author: repo.to_string(), rkey, rev });
                }
            }
            Seen::Invalid => out.extend(invalid_post_op(op, own)),
            Seen::Other => {}
        }
    }
    out
}

/// A parsed post create or update (§2's table, rows 1–3).
fn post_op(
    repo: &str,
    own: Option<i64>,
    rev: i64,
    created: bool,
    parent: Option<&str>,
    root: Option<&str>,
    created_at: DateTime<Utc>,
) -> Option<Op> {
    let (parent, root) = match (parent, root) {
        (Some(p), Some(r)) => (post_ref(p), post_ref(r)),
        // A root post, or a reply without both refs (which `prepare`
        // refuses, so never both absent for a reply).
        (None, None) => {
            let rkey = own?;
            let p = PostOp { author: repo.to_string(), rkey, parent: None, root: None, created_at, rev };
            return Some(if created { Op::Create(p) } else { Op::Update(p) });
        }
        _ => return None,
    };
    // Row 1: a parent or root that is not a post: no post thread.
    if parent == Ref::NotPost || root == Ref::NotPost {
        return None;
    }
    let hole = |node: Key| Some(Op::Hole { node, reason: HoleReason::LeftOut });
    match (own, parent, root) {
        (Some(rkey), Ref::Post(parent), Ref::Post(root)) => {
            let p = PostOp { author: repo.to_string(), rkey, parent: Some(parent), root: Some(root), created_at, rev };
            Some(if created { Op::Create(p) } else { Op::Update(p) })
        }
        // Row 2: own rkey not a TID: a hole at the parent, else the root.
        (None, Ref::Post(parent), _) => hole(parent),
        (None, Ref::NonTid, Ref::Post(root)) => hole(root),
        // Row 3: parent not a TID: a hole at the root.
        (Some(_), Ref::NonTid, Ref::Post(root)) => hole(root),
        // The root is not a TID: the thread can have no root row, so no
        // read of it is ever complete; nothing to record.
        _ => None,
    }
}

/// A post op `prepare` refused (§2's table, rows 4–5).
fn invalid_post_op(op: &RawOp, own: Option<i64>) -> Option<Op> {
    if op.action == Action::Delete {
        // Only an invalid rkey refuses a delete; it can be no row.
        return None;
    }
    let (Some(record), Some(_)) = (&op.record, &op.cid) else {
        return Some(Op::Missed(MissedReason::NoRecord));
    };
    if own.is_none() {
        // An rkey that is no TID (or no record key): as row 2.
        return raw_hole(Some(record), HoleReason::LeftOut);
    }
    // The record did not parse: only its reply refs can refuse a post.
    // Row 4: a hole at the raw root.
    raw_ref(record, "root").map(|node| Op::Hole { node, reason: HoleReason::LeftOut })
}

/// The ops of a commit refused for a bad rev (§2's table, R4-11): a hole per
/// post create or update at its parent, else its root; a delete makes a
/// hole at the parent its stored row names (N5). Each post op is counted
/// missed.
#[must_use]
pub fn bad_rev_ops(repo: &str, ops: &[RawOp]) -> Vec<Op> {
    let mut out = Vec::new();
    for op in ops.iter().filter(|op| op.collection == NSID_POST) {
        match op.action {
            Action::Create | Action::Update => out.extend(raw_hole(op.record.as_ref(), HoleReason::BadRev)),
            Action::Delete => {
                if let Some(rkey) = tid_value(&op.rkey) {
                    out.push(Op::HoleAtParentOf { node: Key { did: repo.to_string(), rkey } });
                }
            }
        }
        out.push(Op::Missed(MissedReason::BadRev));
    }
    out
}

/// The ops of a whole-entry dead letter (R5-5): its raw `ops` scanned, a
/// hole per post create or update at its raw parent, else its root. Data
/// that does not parse as JSON is counted.
#[must_use]
pub fn dead_letter_ops(data: Option<&[u8]>) -> Vec<Op> {
    let parsed: Option<Value> = data.and_then(|d| serde_json::from_slice(d).ok());
    let Some(parsed) = parsed else {
        return vec![Op::Missed(MissedReason::DeadLetter)];
    };
    let Some(ops) = parsed.get("ops").and_then(Value::as_array) else {
        return Vec::new();
    };
    ops.iter()
        .filter(|op| matches!(op.get("action").and_then(Value::as_str), Some("create" | "update")))
        .filter(|op| {
            op.get("path")
                .and_then(Value::as_str)
                .and_then(|p| p.split_once('/'))
                .is_some_and(|(collection, _)| collection == NSID_POST)
        })
        .filter_map(|op| raw_hole(op.get("record"), HoleReason::DeadLetter))
        .collect()
}

/// An `#account` or `#tombstone` event's status (R4-2, R5-1): a tombstone
/// counts as inactive; an account event without `active`, or an entry with
/// no `seq`, says nothing.
#[must_use]
pub fn status_op(kind: IdentityKind, did: &str, relay: &str, active: Option<bool>, seq: Option<i64>) -> Option<Op> {
    let active = match kind {
        IdentityKind::Account => active?,
        IdentityKind::Tombstone => false,
        IdentityKind::Identity | IdentityKind::Handle => return None,
    };
    Some(Op::Status { did: did.to_string(), relay: relay.to_string(), active, seq: seq? })
}

/// The key `outline_gone` keeps a relay's status under: its configured
/// label, else its raw URL, so unlabelled relays never share seq counters;
/// `None` for an event that names no relay.
#[must_use]
pub fn status_relay(watermarks: &crate::firehose::watermark::Watermarks, relay: Option<&str>) -> Option<String> {
    let relay = relay?;
    let label = watermarks.label_for(Some(relay));
    Some(if label == crate::firehose::watermark::UNKNOWN_RELAY { relay } else { label }.to_string())
}

/// The outline ops of an entry `decode` refused: a bad-rev commit's
/// [`bad_rev_ops`], a whole-entry dead letter's [`dead_letter_ops`], and
/// nothing for an event naming no DID.
#[must_use]
pub fn refused_ops(entry: &StreamEntry, readable: Option<&Readable>) -> Vec<Op> {
    match readable {
        Some(Readable::Commit { repo, ops }) => bad_rev_ops(repo, ops),
        Some(Readable::NotADid) => Vec::new(),
        None => dead_letter_ops(entry.raw("data")),
    }
}

/// Every outline op of one stream entry, as the live path collects them,
/// with `now` as the clamp's clock: the replay's collection (§3.3 step 3,
/// N7). `relay_key` names an entry's relay, as [`status_relay`] does.
#[must_use]
pub fn entry_ops(entry: &StreamEntry, now: DateTime<Utc>, relay_key: &dyn Fn(Option<&str>) -> Option<String>) -> Vec<Op> {
    match crate::firehose::stream::decode(entry) {
        Ok(Event::Commit(c)) => crate::firehose::writer::outline_ops(&c.repo, &c.rev, &c.ops, now),
        Ok(Event::Identity { kind, did, relay, active }) => {
            let Some(relay) = relay_key(relay.as_deref()) else {
                return Vec::new();
            };
            status_op(kind, &did, &relay, active, entry.seq()).into_iter().collect()
        }
        Err(_) => {
            let readable = crate::firehose::stream::readable_despite(entry);
            refused_ops(entry, readable.as_ref())
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const TID1: &str = "3kaaaaaaaaaa2";
    const TID2: &str = "3kbbbbbbbbbb2";
    const REV: &str = "3kccccccccccc";

    fn uri(did: &str, collection: &str, rkey: &str) -> String {
        format!("at://{did}/{collection}/{rkey}")
    }

    fn key(did: &str, rkey: &str) -> Key {
        Key { did: did.to_string(), rkey: tid_value(rkey).expect("tid") }
    }

    fn raw(action: Action, rkey: &str, record: Option<Value>) -> RawOp {
        RawOp { action, collection: NSID_POST.to_string(), rkey: rkey.to_string(), cid: record.as_ref().map(|_| "bafy".to_string()), record }
    }

    fn reply(parent: &str, root: &str) -> Value {
        json!({"text": "", "reply": {"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}}})
    }

    fn seen_post<'a>(parent: Option<&'a str>, root: Option<&'a str>) -> Seen<'a> {
        Seen::Post { created: true, parent, root, created_at: Utc::now() }
    }

    #[test]
    fn tids_decode_in_order() {
        let a = tid_value("3kaaaaaaaaaa2").expect("tid");
        let b = tid_value("3kaaaaaaaaaa3").expect("tid");
        assert!(b > a && a > 0);
        assert_eq!(tid_value("self"), None);
        assert_eq!(tid_value("3kaaaaaaaaaa"), None);
        let t = tid_time(a).expect("time");
        assert_eq!(Some(t), crate::hydration::feed::tid_timestamp("3kaaaaaaaaaa2"));
    }

    /// T-O3: each left-out row of §2's table records what it says.
    #[test]
    fn left_out_ops_record_the_table() {
        let post_parent = uri(B, NSID_POST, TID2);
        let post_root = uri(B, NSID_POST, TID1);
        let like_parent = uri(B, "app.bsky.feed.like", TID2);
        let nontid_parent = uri(B, NSID_POST, "notatid");
        let ops = |rkey: &str| vec![raw(Action::Create, rkey, Some(json!({})))];
        let one = |rkey: &str, seen: Seen<'_>| post_ops(A, REV, &ops(rkey), [seen]);

        // A reply with TID refs: one create with its parent and root.
        let got = one(TID1, seen_post(Some(&post_parent), Some(&post_root)));
        let [Op::Create(p)] = got.as_slice() else {
            panic!("{got:?}");
        };
        assert_eq!((p.parent.clone(), p.root.clone()), (Some(key(B, TID2)), Some(key(B, TID1))));
        // Parent not a post: nothing.
        assert_eq!(one(TID1, seen_post(Some(&like_parent), Some(&post_root))), []);
        assert_eq!(one(TID1, seen_post(Some(&post_parent), Some(&like_parent))), []);
        // Own rkey not a TID, parent a TID post: a hole at the parent.
        let hole = |k: Key| Op::Hole { node: k, reason: HoleReason::LeftOut };
        assert_eq!(one("notatid", seen_post(Some(&post_parent), Some(&post_root))), [hole(key(B, TID2))]);
        // Parent not a TID, root a TID post: a hole at the root.
        assert_eq!(one(TID1, seen_post(Some(&nontid_parent), Some(&post_root))), [hole(key(B, TID1))]);
        // A root post whose rkey is not a TID: nothing.
        assert_eq!(one("notatid", seen_post(None, None)), []);

        // Invalid reply ref, raw root a TID post: a hole at the root.
        let bad_ref = json!({"text": "", "reply": {"parent": {"uri": "not a uri"}, "root": {"uri": post_root}}});
        let got = post_ops(A, REV, &[raw(Action::Create, TID1, Some(bad_ref))], [Seen::Invalid]);
        assert_eq!(got, [hole(key(B, TID1))]);
        // No record or cid: counted only.
        let got = post_ops(A, REV, &[raw(Action::Create, TID1, None)], [Seen::Invalid]);
        assert_eq!(got, [Op::Missed(MissedReason::NoRecord)]);
        // An invalid rkey with a valid reply: a hole at the raw parent.
        let got = post_ops(A, REV, &[raw(Action::Create, "bad rkey!", Some(reply(&post_parent, &post_root)))], [Seen::Invalid]);
        assert_eq!(got, [hole(key(B, TID2))]);
        // Deletes: a TID rkey only; other collections are not collected.
        let del = RawOp { action: Action::Delete, collection: NSID_POST.into(), rkey: TID1.into(), cid: None, record: None };
        assert_eq!(post_ops(A, REV, std::slice::from_ref(&del), [Seen::Delete]), [Op::Delete { author: A.into(), rkey: tid_value(TID1).expect("tid"), rev: tid_value(REV).expect("rev") }]);
        let like = RawOp { collection: "app.bsky.feed.like".into(), ..del };
        assert_eq!(post_ops(A, REV, &[like], [Seen::Delete]), []);
    }

    /// T-O3: a bad-rev commit's post ops are holes, its deletes a hole at
    /// the stored row's parent, each counted; a parseable dead letter's
    /// replies are holes, an unparseable one counted.
    #[test]
    fn refused_entries_record_holes() {
        let post_parent = uri(B, NSID_POST, TID2);
        let post_root = uri(B, NSID_POST, TID1);
        let ops = vec![
            raw(Action::Create, TID1, Some(reply(&post_parent, &post_root))),
            RawOp { action: Action::Delete, collection: NSID_POST.into(), rkey: TID2.into(), cid: None, record: None },
            raw(Action::Create, TID2, Some(json!({"text": "a root"}))),
        ];
        assert_eq!(
            bad_rev_ops(A, &ops),
            [
                Op::Hole { node: key(B, TID2), reason: HoleReason::BadRev },
                Op::Missed(MissedReason::BadRev),
                Op::HoleAtParentOf { node: key(A, TID2) },
                Op::Missed(MissedReason::BadRev),
                Op::Missed(MissedReason::BadRev),
            ]
        );
        let data = json!({"repo": A, "rev": "x", "ops": [
            {"action": "create", "path": format!("{NSID_POST}/{TID1}"), "record": reply(&post_parent, &post_root)},
            {"action": "move", "path": "a/b"},
            {"action": "create", "path": format!("app.bsky.feed.like/{TID1}"), "record": reply(&post_parent, &post_root)},
        ]})
        .to_string();
        assert_eq!(dead_letter_ops(Some(data.as_bytes())), [Op::Hole { node: key(B, TID2), reason: HoleReason::DeadLetter }]);
        assert_eq!(dead_letter_ops(Some(b"{")), [Op::Missed(MissedReason::DeadLetter)]);
        assert_eq!(dead_letter_ops(None), [Op::Missed(MissedReason::DeadLetter)]);
    }

    /// Review fix: a ref whose authority is no valid DID names no node, so a
    /// forged DID records nothing, or a hole only at a valid ref.
    #[test]
    fn forged_dids_record_nothing() {
        let good_root = uri(B, NSID_POST, TID1);
        for forged in ["did:plc:\u{0}aaaa", "did:", "did:x", "did:plc:has space"] {
            let parent = uri(forged, NSID_POST, TID2);
            let rec = reply(&parent, &uri(forged, NSID_POST, TID1));
            assert_eq!(post_ops(A, REV, &[raw(Action::Create, "bad rkey!", Some(rec.clone()))], [Seen::Invalid]), [], "{forged:?}");
            assert_eq!(bad_rev_ops(A, &[raw(Action::Create, TID1, Some(rec.clone()))]), [Op::Missed(MissedReason::BadRev)], "{forged:?}");
            let data = json!({"ops": [{"action": "create", "path": format!("{NSID_POST}/{TID1}"), "record": rec}]}).to_string();
            assert_eq!(dead_letter_ops(Some(data.as_bytes())), [], "{forged:?}");
            // A forged parent with a valid root: a hole at the root only.
            let mixed = reply(&parent, &good_root);
            assert_eq!(
                post_ops(A, REV, &[raw(Action::Create, "bad rkey!", Some(mixed))], [Seen::Invalid]),
                [Op::Hole { node: key(B, TID1), reason: HoleReason::LeftOut }],
                "{forged:?}"
            );
            // A parsed post with a forged parent is in no post thread.
            assert_eq!(post_ops(A, REV, &[raw(Action::Create, TID1, Some(json!({})))], [seen_post(Some(&parent), Some(&good_root))]), [], "{forged:?}");
        }
    }

    /// Review fix: a labelled relay's status is kept under its label, an
    /// unlabelled one's under its URL, so two unlabelled relays never share
    /// seq counters.
    #[test]
    fn status_relay_keys() {
        let labels = std::collections::BTreeMap::from([("wss://a.example".to_string(), "a".to_string())]);
        let w = crate::firehose::watermark::Watermarks::new(labels, Vec::new());
        assert_eq!(status_relay(&w, Some("wss://a.example")).as_deref(), Some("a"));
        assert_eq!(status_relay(&w, Some("wss://b.example")).as_deref(), Some("wss://b.example"));
        assert_eq!(status_relay(&w, Some("wss://c.example")).as_deref(), Some("wss://c.example"));
        assert_eq!(status_relay(&w, None), None);
    }

    #[test]
    fn account_status() {
        assert_eq!(
            status_op(IdentityKind::Account, A, "bsky", Some(false), Some(7)),
            Some(Op::Status { did: A.into(), relay: "bsky".into(), active: false, seq: 7 })
        );
        assert_eq!(
            status_op(IdentityKind::Tombstone, A, "bsky", None, Some(8)),
            Some(Op::Status { did: A.into(), relay: "bsky".into(), active: false, seq: 8 })
        );
        assert_eq!(status_op(IdentityKind::Account, A, "bsky", None, Some(7)), None);
        assert_eq!(status_op(IdentityKind::Account, A, "bsky", Some(true), None), None);
        assert_eq!(status_op(IdentityKind::Identity, A, "bsky", Some(true), Some(1)), None);
    }
}
