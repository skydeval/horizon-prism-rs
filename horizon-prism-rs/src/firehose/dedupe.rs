//! Dropping a relay's copy of a commit that another relay's copy already
//! applied.
//!
//! A copy's key is its repo, rev and kind (a commit or a `#sync`) and its
//! tier: a whole copy's key is that alone; a covered copy's adds a hash of
//! its filter. A whole copy is dropped only when a whole copy of the same
//! commit was applied; a covered copy when a whole copy or a covered copy
//! with the same filter was. So a narrower copy arriving first never
//! suppresses a whole one, and two covered relays with different filters
//! both apply. A set-aside copy has no key and is never dropped.
//!
//! Keys are marked seen only after their copy committed, and kept per repo
//! for the window, the least recently seen repos going first past the
//! bound. A copy the window forgot is applied again, which writing by rev
//! makes harmless.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use super::stream::Cap;
use super::tier::CopyTier;

/// Metric: copies dropped because another relay's copy was applied, by
/// the dropped copy's relay.
pub const METRIC_DROPPED: &str = "firehose_dedupe_dropped_total";
/// Metric: repos remembered.
pub const METRIC_REPOS: &str = "firehose_dedupe_repos";

/// What a copy carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CopyKind {
    Commit,
    Sync,
}

/// One copy's key within its repo: the commit (rev and kind), and its
/// filter's hash for a covered copy (`None` for a whole one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CopyKey {
    commit: u64,
    filter: Option<u64>,
}

impl CopyKey {
    /// The key of a copy of `tier` stamped `cap`, or `None` for a
    /// set-aside copy, which has none.
    #[must_use]
    pub fn of(rev: &str, kind: CopyKind, tier: CopyTier, cap: Option<&Cap>) -> Option<Self> {
        let commit = hash(&(rev, kind));
        match tier {
            CopyTier::Whole => Some(Self { commit, filter: None }),
            CopyTier::Covered => {
                let types: Vec<&String> = cap.and_then(|c| c.record_types.as_ref()).map(|t| t.iter().collect()).unwrap_or_default();
                Some(Self { commit, filter: Some(hash(&types)) })
            }
            CopyTier::Aside => None,
        }
    }

    /// Whether `seen` (a key applied before) makes this copy a duplicate:
    /// the whole key of the same commit, or this very key.
    #[must_use]
    pub fn dropped_by(&self, seen: &Self) -> bool {
        self.commit == seen.commit && (seen.filter.is_none() || seen.filter == self.filter)
    }
}

fn hash<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// A repo's identity in the map: a 128-bit hash, as the coverage tracker
/// keys commits, so 2 million repos fit in little memory.
type RepoKey = [u8; 16];

fn repo_key(repo: &str) -> RepoKey {
    let mut k = [0u8; 16];
    k.copy_from_slice(&blake3::hash(repo.as_bytes()).as_bytes()[..16]);
    k
}

#[derive(Debug, Default)]
struct Ring {
    /// The touch that put the repo last in `order`.
    touch: u64,
    keys: Vec<(CopyKey, Instant)>,
}

/// The seen copies.
#[derive(Debug)]
pub struct Dedupe {
    window: Duration,
    max_repos: usize,
    repos: HashMap<RepoKey, Ring>,
    /// Repos in the order they were last touched; an entry whose touch is
    /// not the repo's latest is stale and skipped.
    order: VecDeque<(RepoKey, u64, Instant)>,
    touches: u64,
}

impl Dedupe {
    #[must_use]
    pub fn new(window: Duration, max_repos: u64) -> Self {
        Self {
            window,
            max_repos: usize::try_from(max_repos).unwrap_or(usize::MAX).max(1),
            repos: HashMap::new(),
            order: VecDeque::new(),
            touches: 0,
        }
    }

    /// Whether a copy keyed `key` of `repo` is a duplicate at `now`.
    #[must_use]
    pub fn drops(&self, repo: &str, key: &CopyKey, now: Instant) -> bool {
        self.repos
            .get(&repo_key(repo))
            .is_some_and(|r| r.keys.iter().any(|(seen, at)| now.duration_since(*at) < self.window && key.dropped_by(seen)))
    }

    /// Mark a committed copy seen.
    pub fn insert(&mut self, repo: &str, key: CopyKey, now: Instant) {
        self.expire(now);
        let rk = repo_key(repo);
        self.touches += 1;
        let touch = self.touches;
        let window = self.window;
        let ring = self.repos.entry(rk).or_default();
        ring.keys.retain(|(k, at)| now.duration_since(*at) < window && *k != key);
        ring.keys.push((key, now));
        ring.touch = touch;
        self.order.push_back((rk, touch, now));
        while self.repos.len() > self.max_repos {
            if !self.pop_oldest() {
                break;
            }
        }
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_REPOS).set(self.repos.len() as f64);
    }

    /// Forget every repo last touched before the window.
    pub fn expire(&mut self, now: Instant) {
        while let Some(&(_, _, at)) = self.order.front() {
            if now.duration_since(at) < self.window {
                break;
            }
            self.pop_oldest();
        }
    }

    /// Drop the least recently touched repo; whether there was one.
    fn pop_oldest(&mut self) -> bool {
        while let Some((rk, touch, _)) = self.order.pop_front() {
            if self.repos.get(&rk).is_some_and(|r| r.touch == touch) {
                self.repos.remove(&rk);
                return true;
            }
        }
        false
    }

    /// Repos remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.repos.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.repos.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn cap(types: &[&str]) -> Cap {
        Cap { version: 1, caps: Vec::new(), record_types: Some(types.iter().map(|t| (*t).to_string()).collect::<BTreeSet<_>>()) }
    }

    #[test]
    fn a_whole_copy_drops_every_later_copy_and_a_narrower_one_drops_only_its_own() {
        let t = Instant::now();
        let whole = CopyKey::of("3kaaaaaaaaa22", CopyKind::Commit, CopyTier::Whole, None).expect("key");
        let a = CopyKey::of("3kaaaaaaaaa22", CopyKind::Commit, CopyTier::Covered, Some(&cap(&["app.bsky.feed.post"]))).expect("key");
        let a_again = CopyKey::of("3kaaaaaaaaa22", CopyKind::Commit, CopyTier::Covered, Some(&cap(&["app.bsky.feed.post"]))).expect("key");
        let b = CopyKey::of("3kaaaaaaaaa22", CopyKind::Commit, CopyTier::Covered, Some(&cap(&["app.bsky.feed.like"]))).expect("key");
        assert!(CopyKey::of("3kaaaaaaaaa22", CopyKind::Commit, CopyTier::Aside, None).is_none(), "a set-aside copy has no key");

        // A covered copy first: a whole copy and a covered copy with another
        // filter still apply; the same filter is a duplicate.
        let mut d = Dedupe::new(Duration::from_secs(600), 10);
        d.insert("did:plc:example1", a, t);
        assert!(!d.drops("did:plc:example1", &whole, t));
        assert!(!d.drops("did:plc:example1", &b, t));
        assert!(d.drops("did:plc:example1", &a_again, t));
        // A whole copy drops every copy of its commit.
        d.insert("did:plc:example1", whole, t);
        for k in [whole, a, b] {
            assert!(d.drops("did:plc:example1", &k, t));
        }
        // Another repo, rev or kind is another commit.
        assert!(!d.drops("did:plc:example2", &whole, t));
        let later = CopyKey::of("3kaaaaaaaaa23", CopyKind::Commit, CopyTier::Whole, None).expect("key");
        assert!(!d.drops("did:plc:example1", &later, t));
        let sync = CopyKey::of("3kaaaaaaaaa22", CopyKind::Sync, CopyTier::Whole, None).expect("key");
        assert!(!d.drops("did:plc:example1", &sync, t));
    }

    #[test]
    fn the_window_and_the_bound_forget_copies() {
        let t = Instant::now();
        let k = CopyKey::of("3kaaaaaaaaa22", CopyKind::Commit, CopyTier::Whole, None).expect("key");
        let mut d = Dedupe::new(Duration::from_secs(10), 2);
        d.insert("did:plc:example1", k, t);
        assert!(d.drops("did:plc:example1", &k, t + Duration::from_secs(9)));
        assert!(!d.drops("did:plc:example1", &k, t + Duration::from_secs(10)), "past the window it is applied again");
        // The bound drops the least recently touched repo.
        d.insert("did:plc:example2", k, t + Duration::from_secs(1));
        d.insert("did:plc:example1", k, t + Duration::from_secs(2));
        d.insert("did:plc:example3", k, t + Duration::from_secs(3));
        assert_eq!(d.len(), 2);
        let at = t + Duration::from_secs(4);
        assert!(!d.drops("did:plc:example2", &k, at), "the oldest repo went first");
        assert!(d.drops("did:plc:example1", &k, at) && d.drops("did:plc:example3", &k, at));
        // Expiry empties it.
        d.expire(t + Duration::from_secs(60));
        assert!(d.is_empty());
    }
}
