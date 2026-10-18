//! Embedded profiles of DIDs this AppView may not read (design-v4 §8.1
//! "Embedded profile views"; the profile refresh design).
//!
//! A list view's DIDs outside `scope_dids ∪ members` are handled in three
//! tiers, by [`Hydrator::remote_profiles`] alone:
//! 1. `rt` hits: `rt:profile:{did}`, then (shared mode) the negative entry
//!    `rt:profile-neg:{did}`, which renders the minimal form with no fetch;
//! 2. one synchronous upstream `getProfiles` walk for the misses, at most
//!    25 DIDs per request and mode, inside an 800 ms window that the
//!    request's first batch opens ([`Requester::reserve_profiles`]). It
//!    draws on a detached exhaustion record ([`Requester::detached`]), so it
//!    never cuts a page or turns a response into a `429`;
//! 3. the minimal form for anything still unresolved, which a public
//!    request also puts on the background refresh queue.
//!
//! A viewer-private request reads and writes only the viewer's own cache,
//! never a negative entry, and never enqueues. Private-context hydration
//! asks for nothing. The tiers compute no viewer state and make no Farsight
//! call: `profile_context` already holds the viewer state of every DID.
//!
//! The queue is drained by one background task ([`ProfileDrain`]): it
//! groups DIDs by a cache-only read of the `route` cache, never resolves
//! one, waits for each network's background bucket, and writes shared
//! entries only. Its input is the queue, which only public requests feed.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use serde_json::{Map, Value};
use tokio::sync::Notify;

use super::Hydrator;
use super::actor::ProfileKind;
use super::cache::{PROFILE_NEG_PREFIX, PROFILE_PREFIX, ReadCache};
use crate::constellation::{Cascade, ProfileWalk, Router};
use crate::xrpc::pools::Requester;

/// `profile_refresh_queue`: DIDs queued now.
pub const METRIC_QUEUE: &str = "profile_refresh_queue";
/// `profile_refresh_dropped_total`: DIDs refused because the queue was full.
pub const METRIC_DROPPED: &str = "profile_refresh_dropped_total";
/// `profile_refresh_completed_total`: drain batches whose walk ended.
pub const METRIC_COMPLETED: &str = "profile_refresh_completed_total";
pub use super::cache::METRIC_PROFILE_NEGATIVE as METRIC_NEGATIVE;
pub use crate::constellation::cascade::METRIC_PROFILE_BATCH as METRIC_BATCH_FETCH;

/// DIDs one drain batch takes.
pub const DRAIN_BATCH: usize = 25;

/// How a `profile_context` call treats the DIDs it may not read (design-v4
/// §8.1; profile refresh §3).
#[derive(Debug, Clone, Copy)]
pub enum Tiers<'a> {
    /// List views: tiers 1–3 for the DIDs that are not readable.
    List,
    /// List views plus these DIDs, which take the per-DID path
    /// (`UpstreamSource::profile`): §8.1's single-object creators and the
    /// thread anchor's author.
    ListAnd { single: &'a [String] },
    /// No upstream, no `rt`, no queue: the subject is rendered by the
    /// caller (`getProfile`, `getProfiles`), or the corpus is readable.
    Off,
}

/// Fields of a `profileViewBasic` taken from a remote view.
const BASIC_FIELDS: [&str; 9] = ["did", "handle", "displayName", "pronouns", "avatar", "associated", "createdAt", "verification", "status"];
/// What a `profileView` adds.
const VIEW_FIELDS: [&str; 2] = ["description", "indexedAt"];

/// A cached remote view projected to `kind`, without `viewer` and `labels`
/// (the caller puts in its own).
#[must_use]
pub fn project(remote: &Value, kind: ProfileKind) -> Map<String, Value> {
    let Some(fields) = remote.as_object() else {
        return Map::new();
    };
    fields
        .iter()
        .filter(|(k, _)| k.as_str() != "viewer" && k.as_str() != "labels")
        .filter(|(k, _)| match kind {
            ProfileKind::Basic => BASIC_FIELDS.contains(&k.as_str()),
            ProfileKind::View => BASIC_FIELDS.contains(&k.as_str()) || VIEW_FIELDS.contains(&k.as_str()),
            ProfileKind::Detailed => true,
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// The background refresh queue (design-v4 §8.1): a FIFO of DIDs with a
/// dedup set, capped at `cache.profile_refresh_max`. Cheap to clone.
#[derive(Clone)]
pub struct ProfileQueue {
    inner: Arc<Mutex<(VecDeque<String>, HashSet<String>)>>,
    max: usize,
    ready: Arc<Notify>,
}

impl std::fmt::Debug for ProfileQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileQueue").field("len", &self.len()).field("max", &self.max).finish()
    }
}

impl ProfileQueue {
    #[must_use]
    pub fn new(max: u64) -> Self {
        Self {
            inner: Arc::default(),
            max: usize::try_from(max).unwrap_or(usize::MAX).max(1),
            ready: Arc::new(Notify::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, (VecDeque<String>, HashSet<String>)> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queue `did` unless it is queued already. Over the cap it is dropped
    /// and counted. Only `remote_profiles`' tier 3, for a public request,
    /// calls this.
    pub(super) fn push(&self, did: &str) {
        let len = {
            let mut q = self.lock();
            if q.1.contains(did) {
                return;
            }
            if q.0.len() >= self.max {
                metrics::counter!(METRIC_DROPPED).increment(1);
                return;
            }
            q.1.insert(did.to_string());
            q.0.push_back(did.to_string());
            q.0.len()
        };
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_QUEUE).set(len as f64);
        self.ready.notify_one();
    }

    /// Take up to `n` DIDs, oldest first.
    pub fn pop(&self, n: usize) -> Vec<String> {
        let (taken, len) = {
            let mut q = self.lock();
            let k = n.min(q.0.len());
            let taken: Vec<String> = q.0.drain(..k).collect();
            for d in &taken {
                q.1.remove(d);
            }
            (taken, q.0.len())
        };
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_QUEUE).set(len as f64);
        taken
    }

    /// DIDs queued now.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Wait until something may have been queued.
    async fn wait(&self) {
        self.ready.notified().await;
    }
}

impl Hydrator {
    /// The tiers of design-v4 §8.1 for `dids` (in response order): the
    /// cached or fetched upstream views it resolved, by DID. It reads no row,
    /// label or viewer state and asks no Farsight. The caller has removed
    /// the DIDs it may read.
    pub(crate) async fn remote_profiles(&self, req: &Requester, dids: &[String]) -> HashMap<String, Value> {
        let mut out = HashMap::new();
        if req.is_local_only() || dids.is_empty() {
            return out;
        }
        let shared = !req.is_viewer_private();
        let cache = self.cache();
        // Tier 1: positive entries, then (shared mode) negative ones.
        let mut seen: HashSet<&str> = HashSet::new();
        let mut misses: Vec<String> = Vec::new();
        let mut negative: HashSet<String> = HashSet::new();
        for d in dids {
            if !seen.insert(d.as_str()) {
                continue;
            }
            if let Some(v) = cache.get_json::<Value>(req, &format!("{PROFILE_PREFIX}{d}")).await {
                out.insert(d.clone(), v);
            } else if cache.has_negative(req, d).await {
                negative.insert(d.clone());
            } else {
                misses.push(d.clone());
            }
        }
        // Tier 2: one walk for what the request's budget still covers.
        if let Some((n, window_ends)) = req.reserve_profiles(misses.len()) {
            let until = window_ends.min(req.deadline());
            let walk = req.detached();
            let fetched = self.upstream().fetch_profiles(&walk, &misses[..n], until).await;
            for (d, v) in fetched.found {
                cache.put_json(req, &format!("{PROFILE_PREFIX}{d}"), &v).await;
                out.insert(d, v);
            }
            if shared {
                for d in fetched.absent {
                    cache.put_negative(&d).await;
                    negative.insert(d);
                }
            }
        }
        // Tier 3: the minimal form, and (shared mode) the refresh queue.
        if shared {
            for d in misses.iter().filter(|d| !out.contains_key(*d) && !negative.contains(*d)) {
                self.inner.cfg.profile_queue.push(d);
            }
        }
        out
    }

    /// The remote views behind `profile_context`'s DIDs under `tiers`:
    /// none for private-context hydration or [`Tiers::Off`]; otherwise the
    /// DIDs `readable` does not hold, `single` ones by the per-DID path on
    /// a detached record, the rest through [`Hydrator::remote_profiles`].
    pub(super) async fn tier_profiles(&self, req: &Requester, dids: &[String], readable: &[String], tiers: Tiers<'_>) -> HashMap<String, Value> {
        let single: &[String] = match tiers {
            Tiers::Off => return HashMap::new(),
            Tiers::List => &[],
            Tiers::ListAnd { single } => single,
        };
        if req.is_local_only() {
            return HashMap::new();
        }
        let readable: HashSet<&str> = readable.iter().map(String::as_str).collect();
        let mut out = HashMap::new();
        let mut rest: Vec<String> = Vec::new();
        let detached = req.detached();
        for d in dids.iter().filter(|d| !readable.contains(d.as_str())) {
            if out.contains_key(d) || rest.contains(d) {
                continue;
            }
            if single.contains(d) {
                if let Some(v) = self.upstream().profile(&detached, d).await {
                    out.insert(d.clone(), v);
                }
            } else {
                rest.push(d.clone());
            }
        }
        out.extend(self.remote_profiles(req, &rest).await);
        out
    }
}

/// The background refresh (design-v4 §8.1 "Drain"): one task, one batch
/// at a time.
#[derive(Clone)]
pub struct ProfileDrain {
    queue: ProfileQueue,
    cache: ReadCache,
    router: Router,
    cascade: Cascade,
}

impl ProfileDrain {
    #[must_use]
    pub fn new(queue: ProfileQueue, cache: ReadCache, router: Router, cascade: Cascade) -> Self {
        Self { queue, cache, router, cascade }
    }

    /// Run the drain for as long as the process does.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            metrics::gauge!(METRIC_QUEUE).set(0.0);
            metrics::counter!(METRIC_DROPPED).increment(0);
            metrics::counter!(METRIC_COMPLETED).increment(0);
            metrics::counter!(METRIC_NEGATIVE).increment(0);
            loop {
                let batch = self.queue.pop(DRAIN_BATCH);
                if batch.is_empty() {
                    self.queue.wait().await;
                    continue;
                }
                self.drain(batch).await;
            }
        })
    }

    /// One batch: skip what a request filled meanwhile, group by cached
    /// route, and walk each group's plan with `origin="background"`.
    pub async fn drain(&self, dids: Vec<String>) {
        let started = Instant::now();
        let mut groups: Vec<(Option<String>, Vec<String>)> = Vec::new();
        for d in dids {
            if self.cache.contains_shared(&format!("{PROFILE_PREFIX}{d}")) || self.cache.contains_shared(&format!("{PROFILE_NEG_PREFIX}{d}")) {
                continue;
            }
            // Cache-only: a DID with no cached route, or (C-P2 b) a
            // `community` one, walks the plan of an unrouted DID.
            let home = self.router.cached_route(&d).await.and_then(|r| r.network_name);
            match groups.iter_mut().find(|(h, _)| *h == home) {
                Some((_, group)) => group.push(d),
                None => groups.push((home, vec![d])),
            }
        }
        for (home, group) in groups {
            let plan = self.cascade.table().plan(home.as_deref());
            for chunk in group.chunks(DRAIN_BATCH) {
                let fetched = self.cascade.fetch_profiles(ProfileWalk::Background, &plan, chunk).await;
                if fetched.dropped {
                    // No negative entry and no retry: a later public
                    // request queues them again.
                    continue;
                }
                for (d, v) in &fetched.found {
                    self.cache.put_shared_json(&format!("{PROFILE_PREFIX}{d}"), v).await;
                }
                for d in &fetched.absent {
                    self.cache.put_negative(d).await;
                }
                metrics::counter!(METRIC_COMPLETED).increment(1);
            }
        }
        tracing::debug!(event = "profile_refresh_batch", elapsed_ms = started.elapsed().as_millis(), "profile refresh batch done");
    }
}
