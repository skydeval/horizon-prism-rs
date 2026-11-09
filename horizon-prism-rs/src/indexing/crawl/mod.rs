//! Full indexing's history crawl: every listed account's repository,
//! fetched whole, verified, applied as [`crate::firehose::writer::WriteSource::Crawl`]
//! and reconciled; per-account continuity on live commits; crawl marks and
//! each group's history; re-lists after a gap; changes of `[full] kinds`.

pub mod apply;
pub mod client;
pub mod continuity;
pub mod groups;
pub mod groups_change;
pub mod marks;
pub mod plc_export;
pub mod relist;
pub mod state;
pub mod status;
pub mod task;
#[cfg(test)]
mod tests;

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

/// The `worker_state` key of the crawl's listing: the crawl generation it
/// belongs to, and per relay the list pass's and the status pass's cursors.
pub const LIST_KEY: &str = "full_crawl_list";
/// The `worker_state` key of the re-list pass in progress.
pub const RELIST_KEY: &str = "full_relist";

/// Metric: repositories by outcome.
pub const METRIC_REPOS: &str = "full_crawl_repos_total";
/// Metric: 1 while the crawl is paused at the free-space floor.
pub const METRIC_PAUSED: &str = "full_crawl_paused";
/// Metric: bytes of fetched repositories.
pub const METRIC_CAR_BYTES: &str = "full_crawl_car_bytes";
/// Metric: directory reads of the crawl, by source.
pub const METRIC_IDENTITY_READS: &str = "full_crawl_identity_reads_total";
/// Metric: 1 for each group's current history state (`crawling`, `paused`,
/// `complete`, `removed`), 0 for the others.
pub const METRIC_GROUP_HISTORY: &str = "full_group_history";

/// A bounded, deduplicated queue of DIDs, shared without a lock on any
/// writer's path beyond its own mutex.
#[derive(Debug, Default)]
pub struct DidQueue {
    inner: Mutex<(VecDeque<String>, HashSet<String>)>,
    cap: usize,
}

impl DidQueue {
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self { inner: Mutex::default(), cap }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, (VecDeque<String>, HashSet<String>)> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Queue `did` unless it is queued already or the queue is full.
    /// Returns whether it was queued.
    pub fn push(&self, did: &str) -> bool {
        let mut q = self.lock();
        if q.0.len() >= self.cap || q.1.contains(did) {
            return false;
        }
        q.1.insert(did.to_string());
        q.0.push_back(did.to_string());
        true
    }

    /// Take up to `n` DIDs, oldest first.
    pub fn take(&self, n: usize) -> Vec<String> {
        let mut q = self.lock();
        let k = n.min(q.0.len());
        let out: Vec<String> = q.0.drain(..k).collect();
        for d in &out {
            q.1.remove(d);
        }
        out
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PartialEq for DidQueue {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for DidQueue {}

/// Accounts full indexing saw for the first time live: their takedown
/// relays' `getRepoStatus` is read on the crawl's budget.
pub type FirstSight = DidQueue;

/// Accounts members read, moved up the crawl queue.
pub type OnDemand = DidQueue;

/// The cap of either queue.
pub const QUEUE_CAP: usize = 10_000;
