//! The fallback for inbound list blocks (design-v3 §5.14.17; proposed
//! locked decision 39), off by default.
//!
//! While Farsight is down, a ready member's ordinary render may ask one
//! operator-named AppView's unauthenticated
//! `app.bsky.graph.getRelationships` who, of the accounts on its page,
//! blocks the member through a list. Each call tells that AppView which
//! member is looking at which accounts, which is why nothing makes it
//! unless the operator switched it on.
//!
//! This file holds what does not touch the network: how the accounts are
//! split into calls, how a response is classified and read, and the
//! ledger.
//!
//! **A fallback result is "blocked" or "nothing".** "Blocked" becomes a
//! cache entry that names an inbound list block and never vouches.
//! "Nothing" is used for nothing: it removes no block and never stands in
//! for Farsight. Its one effect is a note in the ledger, so that the pair
//! is not sent to the other AppView again for a while.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde_json::Value;

use super::did::canonical;

/// `getRelationships` takes at most this many `others`.
pub const MAX_ACCOUNTS: usize = 30;
/// A call's request target is kept within this many bytes.
const MAX_TARGET_BYTES: usize = 6 * 1024;
/// A `blockedByList` longer than this is not accepted.
const MAX_LIST_URI_BYTES: usize = 512;

pub const METRIC_CALLS: &str = "farsight_fallback_calls_total";
pub const METRIC_SKIPPED: &str = "farsight_fallback_skipped_total";

/// The accounts of one request as calls: at most 30 each, and short
/// enough that long `did:web` identifiers do not overflow the target.
#[must_use]
pub fn calls(actor: &str, others: &[String]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    let base = "/xrpc/app.bsky.graph.getRelationships?actor=".len() + 3 * actor.len();
    let mut size = base;
    for other in others {
        // Percent-encoding at its worst triples a DID's length.
        let cost = "&others=".len() + 3 * other.len();
        let full = out.last().is_none_or(|call| call.len() >= MAX_ACCOUNTS || size + cost > MAX_TARGET_BYTES);
        if full {
            out.push(Vec::new());
            size = base;
        }
        size += cost;
        if let Some(call) = out.last_mut() {
            call.push(other.clone());
        }
    }
    out
}

/// How one fallback call ended (§5.14.17.4). Every class but `Ok` and
/// `Budget` is a failure for the fallback's breaker; `Budget` is not
/// recorded at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Ok,
    /// A `200` that is not an answer to this question, or a body over the
    /// size cap.
    Malformed,
    /// The other AppView does not know the viewer: the viewer is not sent
    /// again for a while.
    ActorNotFound,
    BadRequest,
    Refused,
    RateLimited,
    ServerError,
    Timeout,
    Transport,
    /// Cut by the request's own allowance or deadline.
    Budget,
}

impl Class {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Malformed => "malformed",
            Self::ActorNotFound => "actor_not_found",
            Self::BadRequest => "bad_request",
            Self::Refused => "refused",
            Self::RateLimited => "rate_limited",
            Self::ServerError => "server_error",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Budget => "budget",
        }
    }
}

/// What one answered call taught about one account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    /// The account blocks the viewer through this list.
    Blocked(String),
    Nothing,
}

/// A `blockedByList` value this AppView accepts from a third party: at
/// most 512 bytes, and the at-uri of a list, or of a `listblock` record
/// in `other`'s repository. Anything else, a private-tier URI included,
/// is not a block.
#[must_use]
pub fn valid_list(uri: &str, other: &str) -> bool {
    if uri.len() > MAX_LIST_URI_BYTES {
        return false;
    }
    let Some(rest) = uri.strip_prefix("at://") else {
        return false;
    };
    let mut parts = rest.split('/');
    let (Some(authority), Some(collection), Some(rkey), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    if rkey.is_empty() || !proto_blue::syntax::Did::is_valid(authority) {
        return false;
    }
    match collection {
        "app.bsky.graph.list" => true,
        "app.bsky.graph.listblock" => canonical(authority).as_deref() == Some(other),
        _ => false,
    }
}

/// Classify a response with a status, and read it when it is an answer:
/// one result per account sent (canonical DIDs).
///
/// An entry belongs to an account by its DID, never by its position. An
/// answer in which no entry matches any account sent, or whose `actor`
/// names someone else, is not an answer to this question: an AppView that
/// always said `{"relationships": []}` would otherwise look healthy and
/// keep receiving viewers and pages.
#[must_use]
pub fn read(status: u16, body: &[u8], viewer: &str, sent: &[String]) -> (Class, HashMap<String, Found>) {
    let none = |class| (class, HashMap::new());
    let json = serde_json::from_slice::<Value>(body).ok();
    match status {
        200 => {}
        400 if json.as_ref().and_then(|v| v.get("error")).and_then(Value::as_str) == Some("ActorNotFound") => return none(Class::ActorNotFound),
        400 => return none(Class::BadRequest),
        429 => return none(Class::RateLimited),
        500..=599 => return none(Class::ServerError),
        201..=299 => return none(Class::Malformed),
        _ => return none(Class::Refused),
    }
    let Some(entries) = json.as_ref().and_then(|v| v.get("relationships")).and_then(Value::as_array) else {
        return none(Class::Malformed);
    };
    if let Some(actor) = json.as_ref().and_then(|v| v.get("actor"))
        && actor.as_str().and_then(canonical).as_deref() != Some(viewer)
    {
        return none(Class::Malformed);
    }
    let mut found: HashMap<String, Found> = HashMap::new();
    for entry in entries {
        let key = match entry.get("$type").and_then(Value::as_str) {
            Some("app.bsky.graph.defs#relationship") => "did",
            Some("app.bsky.graph.defs#notFoundActor") => "actor",
            _ => continue,
        };
        let Some(did) = entry.get(key).and_then(Value::as_str).and_then(canonical) else {
            continue;
        };
        // An account that was not sent, or a second entry for one already
        // matched, is ignored.
        if !sent.contains(&did) || found.contains_key(&did) {
            continue;
        }
        let list = entry.get("blockedByList").and_then(Value::as_str).filter(|uri| key == "did" && valid_list(uri, &did));
        let result = list.map_or(Found::Nothing, |uri| Found::Blocked(uri.to_string()));
        found.insert(did, result);
    }
    if found.is_empty() {
        return none(Class::Malformed);
    }
    // An account with no entry learned nothing.
    for did in sent {
        found.entry(did.clone()).or_insert(Found::Nothing);
    }
    (Class::Ok, found)
}

#[derive(Default)]
struct ViewerLedger {
    /// When each pair was last told "nothing".
    notes: HashMap<String, Instant>,
    order: VecDeque<String>,
    /// The other AppView answered `ActorNotFound` for this viewer then.
    stop: Option<Instant>,
}

/// Per viewer: the pairs an answered call reported "nothing" for, and the
/// viewer's stop. In memory only, bounded like the pair-answer cache, and
/// cleared with it when a member is purged.
#[derive(Default)]
pub struct Ledger {
    viewers: HashMap<String, ViewerLedger>,
    total: usize,
}

impl Ledger {
    /// Whether the pair was told "nothing" within `ttl`: it is not sent
    /// again yet.
    #[must_use]
    pub fn noted(&self, viewer: &str, other: &str, now: Instant, ttl: Duration) -> bool {
        self.viewers
            .get(viewer)
            .and_then(|v| v.notes.get(other))
            .is_some_and(|at| now.saturating_duration_since(*at) < ttl)
    }

    /// Note that an answered call reported "nothing" for the pair. Past
    /// `per_viewer` notes the viewer's oldest goes; past `total`, another
    /// viewer's whole ledger does, with its stop.
    pub fn note(&mut self, viewer: &str, other: &str, now: Instant, per_viewer: usize, total: usize) {
        let v = self.viewers.entry(viewer.to_string()).or_default();
        if v.notes.insert(other.to_string(), now).is_none() {
            v.order.push_back(other.to_string());
            self.total += 1;
        }
        while v.notes.len() > per_viewer {
            let Some(oldest) = v.order.pop_front() else { break };
            if oldest != other && v.notes.remove(&oldest).is_some() {
                self.total -= 1;
            }
        }
        while self.total > total {
            let Some(victim) = self.viewers.keys().find(|k| k.as_str() != viewer).cloned() else {
                break;
            };
            if let Some(gone) = self.viewers.remove(&victim) {
                self.total -= gone.notes.len();
            }
        }
    }

    /// Whether the fallback is held off for this viewer.
    #[must_use]
    pub fn stopped(&self, viewer: &str, now: Instant, ttl: Duration) -> bool {
        self.viewers
            .get(viewer)
            .and_then(|v| v.stop)
            .is_some_and(|at| now.saturating_duration_since(at) < ttl)
    }

    /// Hold the fallback off for this viewer from now.
    pub fn stop(&mut self, viewer: &str, now: Instant) {
        self.viewers.entry(viewer.to_string()).or_default().stop = Some(now);
    }

    /// Drop a viewer's notes and stop (a purge).
    pub fn forget(&mut self, viewer: &str) {
        if let Some(gone) = self.viewers.remove(viewer) {
            self.total -= gone.notes.len();
        }
    }

    /// Notes held, over every viewer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const V: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv";
    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";
    const LIST: &str = "at://did:plc:llllllllllllllllllllllll/app.bsky.graph.list/3k";

    fn sent() -> Vec<String> {
        vec![A.to_string(), B.to_string(), C.to_string()]
    }

    fn body(v: &Value) -> Vec<u8> {
        serde_json::to_vec(v).expect("json")
    }

    #[test]
    fn accounts_are_split_into_calls_of_thirty() {
        let others: Vec<String> = (0..65).map(|i| format!("did:plc:{:a>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "b"))).collect();
        let split = calls(V, &others);
        assert_eq!(split.iter().map(Vec::len).collect::<Vec<_>>(), [30, 30, 5]);
        assert_eq!(split.concat(), others);
        assert!(calls(V, &[]).is_empty());
        // Long identifiers: fewer per call, so the target stays short.
        let long: Vec<String> = (0..30).map(|i| format!("did:web:{}.example.com", "x".repeat(150 + i))).collect();
        assert!(calls(V, &long).len() > 1);
    }

    #[test]
    fn only_a_list_or_the_blockers_own_listblock_is_a_block() {
        assert!(valid_list(LIST, A));
        assert!(valid_list(&format!("at://{A}/app.bsky.graph.listblock/3k"), A));
        // Someone else's listblock record, another collection, a
        // private-tier URI, a value that is too long, not a URI.
        assert!(!valid_list(&format!("at://{B}/app.bsky.graph.listblock/3k"), A));
        assert!(!valid_list(&format!("at://{A}/app.bsky.feed.post/3k"), A));
        assert!(!valid_list(&format!("at://{A}/tools.kryphocron.graph.block/3k"), A));
        assert!(!valid_list(&format!("{LIST}{}", "x".repeat(600)), A));
        assert!(!valid_list("https://example.com/list", A));
        assert!(!valid_list(&format!("at://{A}/app.bsky.graph.list"), A));
    }

    #[test]
    fn an_answer_is_read_by_did_and_only_for_what_was_sent() {
        let answer = json!({"actor": V, "relationships": [
            {"$type": "app.bsky.graph.defs#relationship", "did": A, "blockedByList": LIST, "following": "at://x"},
            {"$type": "app.bsky.graph.defs#relationship", "did": B},
            {"$type": "app.bsky.graph.defs#relationship", "did": A, "blockedByList": "at://second/entry/ignored"},
            {"$type": "app.bsky.graph.defs#relationship", "did": "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz", "blockedByList": LIST},
            {"$type": "something.else", "did": C, "blockedByList": LIST},
        ]});
        let (class, found) = read(200, &body(&answer), V, &sent());
        assert_eq!(class, Class::Ok);
        assert_eq!(found[A], Found::Blocked(LIST.to_string()));
        // No block named, and no entry at all: both "nothing".
        assert_eq!((&found[B], &found[C], found.len()), (&Found::Nothing, &Found::Nothing, 3));
        // A value that is not a valid list, and an account not found.
        let odd = json!({"relationships": [
            {"$type": "app.bsky.graph.defs#relationship", "did": A, "blockedByList": "at://not-a-did/app.bsky.graph.list/1"},
            {"$type": "app.bsky.graph.defs#notFoundActor", "actor": B, "notFound": true},
        ]});
        let (class, found) = read(200, &body(&odd), V, &sent());
        assert_eq!((class, &found[A], &found[B]), (Class::Ok, &Found::Nothing, &Found::Nothing));
    }

    #[test]
    fn everything_else_is_classified_and_teaches_nothing() {
        let class = |status: u16, v: Value| {
            let (class, found) = read(status, &body(&v), V, &sent());
            assert!(found.is_empty() || class == Class::Ok);
            class
        };
        let relationship = json!({"$type": "app.bsky.graph.defs#relationship", "did": A});
        // Not an answer to this question.
        assert_eq!(class(200, json!({"relationships": []})), Class::Malformed);
        assert_eq!(class(200, json!({"nope": 1})), Class::Malformed);
        assert_eq!(class(200, json!({"actor": A, "relationships": [relationship]})), Class::Malformed);
        assert_eq!(class(200, json!({"relationships": [{"$type": "app.bsky.graph.defs#relationship", "did": "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz"}]})), Class::Malformed);
        assert_eq!(read(200, b"not json", V, &sent()).0, Class::Malformed);
        assert_eq!(class(204, json!({})), Class::Malformed);
        // Refusals and failures.
        assert_eq!(class(400, json!({"error": "ActorNotFound"})), Class::ActorNotFound);
        assert_eq!(class(400, json!({"error": "InvalidRequest"})), Class::BadRequest);
        assert_eq!(class(429, json!({})), Class::RateLimited);
        assert_eq!(class(503, json!({})), Class::ServerError);
        for status in [401, 403, 404, 405, 410, 302] {
            assert_eq!(class(status, json!({})), Class::Refused, "{status}");
        }
    }

    #[test]
    fn the_ledger_holds_a_pair_back_for_a_while_and_is_bounded() {
        let ttl = Duration::from_secs(300);
        let now = Instant::now();
        let mut ledger = Ledger::default();
        assert!(!ledger.noted(V, A, now, ttl));
        ledger.note(V, A, now, 2, 10);
        assert!(ledger.noted(V, A, now, ttl) && !ledger.noted(V, B, now, ttl) && !ledger.noted(A, A, now, ttl));
        assert!(!ledger.noted(V, A, now + ttl, ttl), "a note expires");
        // One viewer's bound: the oldest note goes.
        ledger.note(V, B, now, 2, 10);
        ledger.note(V, C, now, 2, 10);
        assert!(!ledger.noted(V, A, now, ttl) && ledger.noted(V, C, now, ttl));
        assert_eq!(ledger.len(), 2);
        // The stop is a field of the viewer's ledger.
        ledger.stop(V, now);
        assert!(ledger.stopped(V, now, ttl) && !ledger.stopped(V, now + ttl, ttl) && !ledger.stopped(A, now, ttl));
        // The total bound drops another viewer's whole ledger, stop and all.
        ledger.note(A, B, now, 2, 2);
        assert!(!ledger.stopped(V, now, ttl) && !ledger.noted(V, C, now, ttl) && ledger.noted(A, B, now, ttl));
        ledger.forget(A);
        assert!(ledger.is_empty());
    }
}
