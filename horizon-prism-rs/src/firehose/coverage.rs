//! Relay coverage tracker (design-v2 §5.11), for Phase B check B-3.
//!
//! An in-memory map from `blake3(repo || rev)` truncated to 16 bytes to
//! `{first_relay, relays_seen bitmask, first_seen_at}`:
//! - first insert: `firehose_commits_first_seen_total{relay}`;
//! - expiry after `coverage_window_secs`: if exactly one relay bit is set,
//!   `firehose_commits_seen_only_by_total{relay}`;
//! - at `coverage_capacity` the oldest entries expire early and
//!   `firehose_coverage_evicted_total` is incremented.
//!
//! The tracker runs as its own task fed by a bounded channel. The worker
//! never waits on it, so it cannot affect writes.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::watermark::UNKNOWN_RELAY;

pub const METRIC_FIRST_SEEN: &str = "firehose_commits_first_seen_total";
pub const METRIC_SEEN_ONLY_BY: &str = "firehose_commits_seen_only_by_total";
pub const METRIC_EVICTED: &str = "firehose_coverage_evicted_total";

/// Queue depth between the worker and the tracker.
pub const CHANNEL_CAPACITY: usize = 65_536;
/// Expiry sweep interval while the stream is idle.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

type Key = [u8; 16];

#[derive(Debug, Clone, Copy)]
struct Entry {
    first_relay: u8,
    relays_seen: u8,
}

/// The tracker's state. Relays are indexed by their position among the
/// configured labels; an unknown relay is `other`, after the last label.
pub struct CoverageTracker {
    labels: Vec<String>,
    window: Duration,
    capacity: usize,
    map: HashMap<Key, Entry>,
    order: VecDeque<(Key, Instant)>,
    counts: CoverageCounts,
}

/// Running totals mirroring the metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoverageCounts {
    pub first_seen: HashMap<String, u64>,
    pub seen_only_by: HashMap<String, u64>,
    pub evicted: u64,
}

impl CoverageTracker {
    /// `labels` are the configured relay labels in `relay_labels` order.
    #[must_use]
    pub fn new(mut labels: Vec<String>, window: Duration, capacity: usize) -> Self {
        // Seven configured relays plus `other` fit the u8 bitmask.
        labels.truncate(7);
        labels.push(UNKNOWN_RELAY.to_string());
        Self {
            labels,
            window,
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
            counts: CoverageCounts::default(),
        }
    }

    fn index(&self, label: &str) -> u8 {
        let other = self.labels.len() - 1;
        let i = self.labels[..other].iter().position(|l| l == label).unwrap_or(other);
        u8::try_from(i).unwrap_or(7)
    }

    fn key(repo: &str, rev: &str) -> Key {
        let mut h = blake3::Hasher::new();
        h.update(repo.as_bytes());
        h.update(rev.as_bytes());
        let mut k = [0u8; 16];
        k.copy_from_slice(&h.finalize().as_bytes()[..16]);
        k
    }

    /// Record one commit entry seen via relay `label`.
    pub fn observe(&mut self, repo: &str, rev: &str, label: &str, now: Instant) {
        self.expire(now);
        let relay = self.index(label);
        let bit = 1u8 << relay;
        let key = Self::key(repo, rev);
        if let Some(e) = self.map.get_mut(&key) {
            e.relays_seen |= bit;
            return;
        }
        while self.map.len() >= self.capacity {
            metrics::counter!(METRIC_EVICTED).increment(1);
            self.counts.evicted += 1;
            if !self.pop_oldest() {
                break;
            }
        }
        self.map.insert(
            key,
            Entry {
                first_relay: relay,
                relays_seen: bit,
            },
        );
        self.order.push_back((key, now));
        let label = self.labels[usize::from(relay)].clone();
        *self.counts.first_seen.entry(label.clone()).or_insert(0) += 1;
        metrics::counter!(METRIC_FIRST_SEEN, "relay" => label).increment(1);
    }

    /// Expire every entry older than the window.
    pub fn expire(&mut self, now: Instant) {
        while let Some((_, at)) = self.order.front() {
            if now.duration_since(*at) < self.window {
                break;
            }
            self.pop_oldest();
        }
    }

    fn pop_oldest(&mut self) -> bool {
        let Some((key, _)) = self.order.pop_front() else {
            return false;
        };
        if let Some(e) = self.map.remove(&key)
            && e.relays_seen.count_ones() == 1
        {
            let label = self.labels[usize::from(e.first_relay)].clone();
            *self.counts.seen_only_by.entry(label.clone()).or_insert(0) += 1;
            metrics::counter!(METRIC_SEEN_ONLY_BY, "relay" => label).increment(1);
        }
        true
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    #[must_use]
    pub fn counts(&self) -> &CoverageCounts {
        &self.counts
    }

    /// Run the tracker on its own task.
    #[must_use]
    pub fn spawn(mut self) -> (CoverageHandle, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<(String, String, String)>(CHANNEL_CAPACITY);
        let task = tokio::spawn(async move {
            let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                tokio::select! {
                    msg = rx.recv() => match msg {
                        Some((repo, rev, label)) => self.observe(&repo, &rev, &label, Instant::now()),
                        None => break,
                    },
                    _ = sweep.tick() => self.expire(Instant::now()),
                }
            }
        });
        (CoverageHandle { tx }, task)
    }
}

/// The worker's side: never blocks; drops observations when the tracker lags.
#[derive(Clone)]
pub struct CoverageHandle {
    tx: mpsc::Sender<(String, String, String)>,
}

impl CoverageHandle {
    pub fn observe(&self, repo: &str, rev: &str, label: &str) {
        let _ = self.tx.try_send((repo.to_string(), rev.to_string(), label.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
    }

    #[test]
    fn first_seen_and_only_seen_counters() {
        let t0 = Instant::now();
        let labels = vec!["bsky".to_string(), "microcosm".to_string(), "community-a".to_string()];
        let mut c = CoverageTracker::new(labels, Duration::from_secs(600), 100);
        // Commit A: seen by bsky then microcosm.
        c.observe("did:plc:a", "rev1", "bsky", t0);
        c.observe("did:plc:a", "rev1", "microcosm", t0 + Duration::from_secs(1));
        // Commit B: seen only by community-a.
        c.observe("did:plc:b", "rev1", "community-a", t0 + Duration::from_secs(2));
        // Commit C: an unknown relay counts as `other`.
        c.observe("did:plc:c", "rev1", "mystery", t0 + Duration::from_secs(3));
        // The same repo at another rev is another commit.
        c.observe("did:plc:a", "rev2", "microcosm", t0 + Duration::from_secs(4));
        assert_eq!(c.len(), 4);
        assert_eq!(c.counts().first_seen, map(&[("bsky", 1), ("community-a", 1), ("other", 1), ("microcosm", 1)]));
        // Nothing expires inside the window.
        c.expire(t0 + Duration::from_secs(599));
        assert_eq!(c.len(), 4);
        assert!(c.counts().seen_only_by.is_empty());
        c.expire(t0 + Duration::from_secs(700));
        assert!(c.is_empty());
        assert_eq!(c.counts().seen_only_by, map(&[("community-a", 1), ("other", 1), ("microcosm", 1)]));
        assert_eq!(c.counts().evicted, 0);
    }

    #[test]
    fn capacity_evicts_the_oldest_early() {
        let t0 = Instant::now();
        let mut c = CoverageTracker::new(vec!["bsky".to_string()], Duration::from_secs(600), 2);
        c.observe("did:plc:a", "r", "bsky", t0);
        c.observe("did:plc:b", "r", "bsky", t0);
        c.observe("did:plc:c", "r", "bsky", t0);
        assert_eq!(c.len(), 2);
        assert_eq!(c.counts().evicted, 1);
        // An evicted single-relay entry still counts as seen only by it.
        assert_eq!(c.counts().seen_only_by, map(&[("bsky", 1)]));
        // Seeing the evicted commit again is a new first-seen.
        c.observe("did:plc:a", "r", "bsky", t0);
        assert_eq!(c.counts().first_seen, map(&[("bsky", 4)]));
    }

    #[tokio::test]
    async fn handle_never_blocks() {
        let (handle, task) = CoverageTracker::new(vec!["bsky".into()], Duration::from_secs(1), 10).spawn();
        for i in 0..(CHANNEL_CAPACITY + 10) {
            handle.observe("did:plc:a", &i.to_string(), "bsky");
        }
        drop(handle);
        task.await.expect("tracker exits when the worker drops its handle");
    }
}

