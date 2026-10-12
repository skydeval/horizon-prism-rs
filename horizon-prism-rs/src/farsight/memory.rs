//! The memory of learned inbound list blocks (design-v3 §5.14.18;
//! proposed locked decision 39), off by default.
//!
//! For each ready member, the accounts a live answer reported as blocking
//! it through a list are remembered for a bounded time, so those blocks
//! keep applying while Farsight gives no answer and across a restart.
//!
//! - **Only positives are stored**, and only what this AppView fixes
//!   itself: two canonical DIDs, a list URI that is short and parses (or
//!   none), who reported it, and this AppView's own clock.
//! - **A remembered block applies whenever the pair answer in hand does
//!   not vouch**, and is forgotten at once when Farsight vouches that the
//!   pair has no such block. A live answer always wins.
//! - **The working state is an in-memory copy**; the table is what the
//!   next start reads. Requests read only the copy, so the memory adds no
//!   query to any read. A background writer makes the table match the
//!   copy, pair by pair.
//!
//! This file holds the copy and its set of pending pairs. Everything in
//! it runs under the pair-answer cache's lock.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};

use super::answer::Source;

/// What `inbound.list` holds for a remembered block whose URI was not
/// stored. A pair answer either names a list or names no block, so a
/// block with no URI needs one. Nothing emits it; anything that ever
/// emits `inbound.list` must treat it as "URI unknown".
pub const PLACEHOLDER_LIST: &str = "at://did:web:unknown.invalid/app.bsky.graph.list/unknown";
/// A list URI longer than this is not stored: the block is, without it.
const MAX_LIST_URI_BYTES: usize = 512;
/// A confirm reaches the table at most this often.
const PERSIST_EVERY: TimeDelta = TimeDelta::minutes(10);
/// Pending pairs the copy no longer holds, at most.
const MAX_PENDING_ABSENT: usize = 100_000;
/// A put that keeps failing is given up after this many attempts.
pub const PUT_ATTEMPTS: u32 = 3;

pub const METRIC_DROPPED: &str = super::METRIC_MEMORY_DROPPED;

/// The memory's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caps {
    pub per_viewer: usize,
    pub total: usize,
    pub retention: TimeDelta,
}

/// The URI to store for a block: the one the answer named, if it is short
/// and parses as an at-uri; otherwise none. The same rule whoever
/// reported it, so nothing a remote party sends can break a write.
#[must_use]
pub fn storable_uri(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("at://")?;
    let ok = uri.len() <= MAX_LIST_URI_BYTES
        && !uri.chars().any(char::is_control)
        && rest.split('/').next().is_some_and(|authority| !authority.is_empty());
    ok.then(|| uri.to_string())
}

/// One remembered block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Remembered {
    pub list_uri: Option<String>,
    pub source: Source,
    /// When a live answer last reported the block.
    pub confirmed_at: DateTime<Utc>,
    /// The `confirmed_at` last handed to the writer.
    persisted_at: DateTime<Utc>,
    /// The table refused the row: it is never handed to the writer again.
    rejected: bool,
}

/// A pending pair's retry state.
#[derive(Debug, Clone, Copy)]
struct Attempt {
    failures: u32,
    next: Instant,
}

type Pair = (String, String);

/// The copy, and the pairs whose row may differ from it.
#[derive(Debug, Default)]
pub struct Copy {
    viewers: HashMap<String, HashMap<String, Remembered>>,
    total: usize,
    pending: HashMap<Pair, Attempt>,
}

impl Copy {
    /// Entries held, over every viewer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Viewers with at least one entry.
    #[must_use]
    pub fn viewers(&self) -> usize {
        self.viewers.len()
    }

    /// Pairs still to be written.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Entries by who reported them: `(farsight, fallback)`.
    #[must_use]
    pub fn by_source(&self) -> (usize, usize) {
        let fallback = self.viewers.values().flat_map(HashMap::values).filter(|e| e.source == Source::Fallback).count();
        (self.total - fallback, fallback)
    }

    /// The oldest confirmation held.
    #[must_use]
    pub fn oldest(&self) -> Option<DateTime<Utc>> {
        self.viewers.values().flat_map(HashMap::values).map(|e| e.confirmed_at).min()
    }

    fn mark(&mut self, viewer: &str, other: &str) {
        let held = self.viewers.get(viewer).and_then(|v| v.get(other));
        if held.is_some_and(|e| e.rejected) {
            return;
        }
        // A pair the copy no longer holds only ever needs a delete; past
        // the bound a new one is not added.
        if held.is_none() && self.pending.len() >= self.total + MAX_PENDING_ABSENT {
            metrics::counter!(METRIC_DROPPED, "reason" => "pending_full").increment(1);
            return;
        }
        // A new change resets the pair's failures and its wait.
        self.pending.insert((viewer.to_string(), other.to_string()), Attempt { failures: 0, next: Instant::now() });
    }

    fn remove(&mut self, viewer: &str, other: &str) -> bool {
        let Some(entries) = self.viewers.get_mut(viewer) else {
            return false;
        };
        let removed = entries.remove(other).is_some();
        if entries.is_empty() {
            self.viewers.remove(viewer);
        }
        if removed {
            self.total -= 1;
            self.mark(viewer, other);
        }
        removed
    }

    /// The list of a remembered block for the pair, or the placeholder.
    /// An entry past its retention is dropped here, and counts toward no
    /// cap from then on.
    pub fn get(&mut self, viewer: &str, other: &str, now: DateTime<Utc>, caps: Caps) -> Option<String> {
        let entry = self.viewers.get(viewer)?.get(other)?;
        if entry.confirmed_at + caps.retention <= now {
            self.remove(viewer, other);
            return None;
        }
        Some(entry.list_uri.clone().unwrap_or_else(|| PLACEHOLDER_LIST.to_string()))
    }

    /// Whether the copy holds the pair, expired or not.
    #[must_use]
    pub fn holds(&self, viewer: &str, other: &str) -> bool {
        self.viewers.get(viewer).is_some_and(|v| v.contains_key(other))
    }

    /// Learn or confirm a block a live answer reported.
    ///
    /// - A pair already held is confirmed. Its row is rewritten at most
    ///   every ten minutes, and at once when the source or the URI
    ///   changes: a member that keeps rendering a blocker costs one write
    ///   per ten minutes, not one per render.
    /// - A new pair with the viewer at its cap evicts one of the viewer's
    ///   entries: its oldest that the fallback reported, or, if it has
    ///   none, its oldest. So during an outage a viewer whose entries are
    ///   all Farsight's loses at most one of them to the fallback.
    /// - A new pair with the copy at its total is not learned.
    pub fn learn(&mut self, viewer: &str, other: &str, uri: Option<&str>, source: Source, now: DateTime<Utc>, caps: Caps) {
        let list_uri = uri.and_then(storable_uri);
        if let Some(entry) = self.viewers.get_mut(viewer).and_then(|v| v.get_mut(other)) {
            let changed = entry.source != source || entry.list_uri != list_uri;
            entry.confirmed_at = now;
            entry.source = source;
            entry.list_uri = list_uri;
            if changed || now - entry.persisted_at > PERSIST_EVERY {
                entry.persisted_at = now;
                self.mark(viewer, other);
            }
            return;
        }
        let held = self.viewers.get(viewer).map_or(0, HashMap::len);
        if held >= caps.per_viewer {
            let oldest = |source: Source| {
                self.viewers.get(viewer).and_then(|v| v.iter().filter(|(_, e)| e.source == source).min_by_key(|(_, e)| e.confirmed_at).map(|(o, _)| o.clone()))
            };
            let Some(evicted) = oldest(Source::Fallback).or_else(|| oldest(Source::Farsight)) else {
                return;
            };
            self.remove(viewer, &evicted);
        } else if self.total >= caps.total {
            metrics::counter!(METRIC_DROPPED, "reason" => "total_cap").increment(1);
            return;
        }
        let entry = Remembered { list_uri, source, confirmed_at: now, persisted_at: now, rejected: false };
        self.viewers.entry(viewer.to_string()).or_default().insert(other.to_string(), entry);
        self.total += 1;
        self.mark(viewer, other);
    }

    /// Forget a pair Farsight vouched as clean. Returns whether it was held.
    pub fn forget(&mut self, viewer: &str, other: &str) -> bool {
        self.remove(viewer, other)
    }

    /// Clear a viewer's part of the copy (a purge of that member, or
    /// `admin farsight forget`). Each pair becomes pending. Returns how
    /// many entries went.
    pub fn clear_viewer(&mut self, viewer: &str) -> usize {
        let others: Vec<String> = self.viewers.get(viewer).map(|v| v.keys().cloned().collect()).unwrap_or_default();
        for other in &others {
            self.remove(viewer, other);
        }
        others.len()
    }

    /// Remove every entry that names `other` as the blocker (its account
    /// was deleted). Walks the whole copy.
    pub fn clear_other(&mut self, other: &str) -> usize {
        let viewers: Vec<String> = self.viewers.iter().filter(|(_, v)| v.contains_key(other)).map(|(viewer, _)| viewer.clone()).collect();
        for viewer in &viewers {
            self.remove(viewer, other);
        }
        viewers.len()
    }

    /// Drop every entry past its retention (the hourly sweep).
    pub fn sweep(&mut self, now: DateTime<Utc>, caps: Caps) -> usize {
        let expired: Vec<Pair> = self
            .viewers
            .iter()
            .flat_map(|(viewer, v)| v.iter().filter(|(_, e)| e.confirmed_at + caps.retention <= now).map(move |(other, _)| (viewer.clone(), other.clone())))
            .collect();
        for (viewer, other) in &expired {
            self.remove(viewer, other);
        }
        expired.len()
    }

    /// Install a row read from the table at startup, within the caps.
    /// Nothing becomes pending: the row is what the table holds. Returns
    /// false for a row the caps leave out, whose row is then deleted.
    pub fn install(&mut self, viewer: &str, other: &str, list_uri: Option<String>, source: Source, confirmed_at: DateTime<Utc>, caps: Caps) -> bool {
        let held = self.viewers.get(viewer).map_or(0, HashMap::len);
        if held >= caps.per_viewer || self.total >= caps.total || self.holds(viewer, other) {
            return false;
        }
        let entry = Remembered { list_uri, source, confirmed_at, persisted_at: confirmed_at, rejected: false };
        self.viewers.entry(viewer.to_string()).or_default().insert(other.to_string(), entry);
        self.total += 1;
        true
    }

    /// Pending pairs whose next attempt is due, at most `limit`.
    #[must_use]
    pub fn due(&self, now: Instant, limit: usize) -> Vec<Pair> {
        self.pending.iter().filter(|(_, a)| a.next <= now).map(|(pair, _)| pair.clone()).take(limit).collect()
    }

    /// Take a pair out of the pending set and read what the copy holds
    /// for it now: `Some(row)` to put, `None` to delete. The writer writes
    /// that, never an older value. Also returns the failures so far.
    pub fn take(&mut self, pair: &Pair) -> Option<(Option<Remembered>, u32)> {
        let attempt = self.pending.remove(pair)?;
        let held = self.viewers.get(&pair.0).and_then(|v| v.get(&pair.1)).cloned();
        Some((held, attempt.failures))
    }

    /// A write for the pair failed on a connection or server error. A
    /// delete is never given up; a put is given up after three attempts.
    /// The pair waits 1 s, then 5 s, then 30 s, then 60 s each time. A
    /// change made to the pair meanwhile has already made it pending
    /// afresh, and is left alone.
    pub fn failed(&mut self, pair: &Pair, was_put: bool, failures: u32, now: Instant) {
        if self.pending.contains_key(pair) {
            return;
        }
        let failures = failures + 1;
        if was_put && failures >= PUT_ATTEMPTS {
            metrics::counter!(METRIC_DROPPED, "reason" => "db_error").increment(1);
            return;
        }
        let wait = match failures {
            1 => 1,
            2 => 5,
            3 => 30,
            _ => 60,
        };
        self.pending.insert(pair.clone(), Attempt { failures, next: now + Duration::from_secs(wait) });
    }

    /// The table refused the pair's row: it is never handed to the writer
    /// again, and still applies from the copy.
    pub fn rejected(&mut self, pair: &Pair) {
        metrics::counter!(METRIC_DROPPED, "reason" => "row_rejected").increment(1);
        if let Some(entry) = self.viewers.get_mut(&pair.0).and_then(|v| v.get_mut(&pair.1)) {
            entry.rejected = true;
        }
        self.pending.remove(pair);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv";
    const W: &str = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";
    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";
    const LIST: &str = "at://did:plc:llllllllllllllllllllllll/app.bsky.graph.list/3k";

    fn caps() -> Caps {
        Caps { per_viewer: 2, total: 3, retention: TimeDelta::hours(24) }
    }

    fn at(minutes: i64) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH + TimeDelta::days(20_000) + TimeDelta::minutes(minutes)
    }

    fn drain(copy: &mut Copy) -> Vec<(Pair, bool)> {
        let mut out: Vec<(Pair, bool)> = copy
            .due(Instant::now() + Duration::from_secs(1), usize::MAX)
            .into_iter()
            .map(|pair| {
                let put = copy.take(&pair).expect("pending").0.is_some();
                (pair, put)
            })
            .collect();
        out.sort();
        out
    }

    fn pair(viewer: &str, other: &str) -> Pair {
        (viewer.to_string(), other.to_string())
    }

    #[test]
    fn only_a_short_uri_that_parses_is_stored() {
        assert_eq!(storable_uri(LIST).as_deref(), Some(LIST));
        assert_eq!(storable_uri(&format!("{LIST}{}", "x".repeat(600))), None);
        assert_eq!(storable_uri("at://did:plc:x/a\0b/c"), None);
        assert_eq!(storable_uri("https://example.com"), None);
        assert_eq!(storable_uri("at:///no-authority"), None);
    }

    #[test]
    fn a_block_is_learned_confirmed_read_and_forgotten() {
        let mut copy = Copy::default();
        copy.learn(V, A, Some(LIST), Source::Farsight, at(0), caps());
        // A URI that cannot be stored: the block is, with the placeholder.
        copy.learn(V, B, Some("not a uri"), Source::Farsight, at(0), caps());
        assert_eq!(copy.get(V, A, at(1), caps()).as_deref(), Some(LIST));
        assert_eq!(copy.get(V, B, at(1), caps()).as_deref(), Some(PLACEHOLDER_LIST));
        assert_eq!(copy.get(W, A, at(1), caps()), None, "another viewer's memory is its own");
        assert_eq!(drain(&mut copy), [(pair(V, A), true), (pair(V, B), true)]);

        // A confirm within ten minutes writes nothing; a later one does;
        // one that changes the source writes at once.
        copy.learn(V, A, Some(LIST), Source::Farsight, at(5), caps());
        assert!(drain(&mut copy).is_empty());
        copy.learn(V, A, Some(LIST), Source::Farsight, at(11), caps());
        assert_eq!(drain(&mut copy), [(pair(V, A), true)]);
        copy.learn(V, A, Some(LIST), Source::Fallback, at(12), caps());
        assert_eq!(drain(&mut copy), [(pair(V, A), true)]);
        assert_eq!((copy.len(), copy.by_source()), (2, (1, 1)));

        // Forgotten: gone at once, and its row is to be deleted.
        assert!(copy.forget(V, A) && !copy.forget(V, A));
        assert_eq!(copy.get(V, A, at(13), caps()), None);
        assert_eq!(drain(&mut copy), [(pair(V, A), false)]);
        // Past its retention an entry is dropped when it is read.
        assert_eq!(copy.get(V, B, at(24 * 60), caps()), None);
        assert_eq!((copy.len(), drain(&mut copy)), (0, vec![(pair(V, B), false)]));
    }

    #[test]
    fn the_caps_evict_the_fallbacks_entries_first() {
        let mut copy = Copy::default();
        copy.learn(V, A, None, Source::Farsight, at(0), caps());
        copy.learn(V, B, None, Source::Fallback, at(1), caps());
        // At the viewer's cap: the fallback's entry goes, though it is newer.
        copy.learn(V, C, None, Source::Farsight, at(2), caps());
        assert!(copy.holds(V, A) && !copy.holds(V, B) && copy.holds(V, C));
        // All Farsight's: the oldest goes.
        copy.learn(V, B, None, Source::Fallback, at(3), caps());
        assert!(!copy.holds(V, A) && copy.holds(V, B) && copy.holds(V, C));
        // The total: a new viewer's pair is learned up to it, then not.
        copy.learn(W, A, None, Source::Farsight, at(4), caps());
        copy.learn(W, B, None, Source::Farsight, at(4), caps());
        assert_eq!((copy.len(), copy.holds(W, B)), (3, false));
        // A viewer at its own cap still learns at the total, by eviction.
        copy.learn(V, A, None, Source::Farsight, at(5), caps());
        assert!(copy.holds(V, A) && copy.len() == 3);
    }

    #[test]
    fn clearing_a_viewer_or_an_account_and_the_sweep() {
        let mut copy = Copy::default();
        let wide = Caps { per_viewer: 10, total: 10, retention: TimeDelta::hours(1) };
        for (viewer, other) in [(V, A), (V, B), (W, A)] {
            copy.learn(viewer, other, None, Source::Farsight, at(0), wide);
        }
        drain(&mut copy);
        assert_eq!(copy.clear_other(A), 2);
        assert_eq!(drain(&mut copy), [(pair(V, A), false), (pair(W, A), false)]);
        assert_eq!((copy.clear_viewer(V), copy.len(), copy.viewers()), (1, 0, 0));
        copy.learn(V, A, None, Source::Farsight, at(0), wide);
        copy.learn(V, B, None, Source::Farsight, at(30), wide);
        assert_eq!((copy.sweep(at(61), wide), copy.holds(V, B)), (1, true));
        assert_eq!(copy.oldest(), Some(at(30)));
    }

    #[test]
    fn the_writer_writes_what_the_copy_holds_now() {
        let mut copy = Copy::default();
        let now = Instant::now();
        // Rows read at startup are not pending.
        assert!(copy.install(V, A, None, Source::Farsight, at(0), caps()));
        assert!(!copy.install(V, A, None, Source::Farsight, at(0), caps()));
        assert_eq!(copy.pending(), 0);
        // Two changes to one pair leave one pending pair, with the last
        // state: learned, then forgotten, is one delete.
        copy.learn(V, B, None, Source::Farsight, at(0), caps());
        copy.forget(V, B);
        assert_eq!(drain(&mut copy), [(pair(V, B), false)]);
        // A failed delete is never given up; a put is, at the third.
        copy.failed(&pair(V, B), false, 5, now);
        assert_eq!(copy.pending(), 1);
        assert!(copy.due(now, 10).is_empty(), "it waits before the next attempt");
        copy.take(&pair(V, B));
        copy.failed(&pair(V, A), true, 0, now);
        assert_eq!(copy.take(&pair(V, A)).map(|(_, failures)| failures), Some(1));
        copy.failed(&pair(V, A), true, 2, now);
        assert_eq!(copy.pending(), 0);
        // A refused row is never pending again, and still applies.
        copy.learn(V, A, Some(LIST), Source::Fallback, at(1), caps());
        copy.rejected(&pair(V, A));
        copy.learn(V, A, None, Source::Farsight, at(60), caps());
        assert_eq!((copy.pending(), copy.get(V, A, at(61), caps()).is_some()), (0, true));
    }
}
