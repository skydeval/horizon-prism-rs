//! The `rt` read-time fetch cache (design-v2 §7.6): in-process, never Redis.
//! A `moka` future cache weighted by value bytes, total `cache.read_time_bytes`
//! (default 256 MiB); values over 256 KiB are not cached. Request traffic can
//! only evict entries, never grow memory past the bound.
//!
//! Each entry lives for its key's class (design-v4 §8.1, §II-7.6), from its
//! last write: `rt:profile:` for `cache.profile_ttl_secs`, the negative
//! entries `rt:profile-neg:` for `cache.profile_negative_ttl_secs`,
//! `rt:farsight-pair:` for `farsight.cache_ttl_secs`, anything else 300 s.
//!
//! Every read and write names the [`Requester`]. A viewer-private one
//! (design-v4 §8.1; R3-188, R3-208) never touches the shared entries, not
//! even to read them, since a moka `get` moves the entry's LRU and
//! frequency state and so leaves a trace an attacker who fills `rt` could
//! see through eviction. It uses the viewer's own private cache instead: a
//! separate `moka` cache keyed `(viewer, key)`, with TTL
//! `cache.viewer_private_ttl_secs`, capacity `cache.viewer_private_max`
//! entries in total (LRU), and at most `cache.viewer_private_per_viewer_max`
//! entries per viewer.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moka::Expiry;
use moka::future::Cache;
use moka::notification::RemovalCause;
use moka::policy::EvictionPolicy;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::xrpc::pools::Requester;

/// Entry lifetime of a key with no class of its own (§7.6).
pub const TTL: Duration = Duration::from_secs(300);
/// The positive profile entries (design-v4 §8.1).
pub const PROFILE_PREFIX: &str = "rt:profile:";
/// The negative profile entries (design-v4 §8.1; profile refresh D-3).
pub const PROFILE_NEG_PREFIX: &str = "rt:profile-neg:";
/// Farsight's pair answers (design-v4 §II-7.6).
pub const FARSIGHT_PAIR_PREFIX: &str = "rt:farsight-pair:";
/// `profile_refresh_negative_total`: negative profile entries written, by
/// any writer (design-v4 §8.1, §14.2).
pub const METRIC_PROFILE_NEGATIVE: &str = "profile_refresh_negative_total";

/// The lifetime of each class of `rt` key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtTtls {
    /// `cache.profile_ttl_secs`.
    pub profile: Duration,
    /// `cache.profile_negative_ttl_secs`.
    pub negative: Duration,
    /// `farsight.cache_ttl_secs`.
    pub farsight_pair: Duration,
    /// Every other key ([`TTL`]).
    pub other: Duration,
}

impl Default for RtTtls {
    fn default() -> Self {
        Self {
            profile: Duration::from_secs(3600),
            negative: Duration::from_secs(900),
            farsight_pair: Duration::from_secs(300),
            other: TTL,
        }
    }
}

impl RtTtls {
    fn of(&self, key: &str) -> Duration {
        if key.starts_with(PROFILE_PREFIX) {
            self.profile
        } else if key.starts_with(PROFILE_NEG_PREFIX) {
            self.negative
        } else if key.starts_with(FARSIGHT_PAIR_PREFIX) {
            self.farsight_pair
        } else {
            self.other
        }
    }
}

/// Per-entry expiry by key class: a write, first or not, restarts the
/// entry's clock.
struct RtExpiry(RtTtls);

impl Expiry<String, Arc<Vec<u8>>> for RtExpiry {
    fn expire_after_create(&self, key: &String, _: &Arc<Vec<u8>>, _: std::time::Instant) -> Option<Duration> {
        Some(self.0.of(key))
    }

    fn expire_after_update(&self, key: &String, _: &Arc<Vec<u8>>, _: std::time::Instant, _: Option<Duration>) -> Option<Duration> {
        Some(self.0.of(key))
    }
}

fn shared_cache(capacity_bytes: u64, ttls: RtTtls) -> Cache<String, Arc<Vec<u8>>> {
    Cache::builder()
        .max_capacity(capacity_bytes)
        .weigher(|k: &String, v: &Arc<Vec<u8>>| u32::try_from(k.len() + v.len()).unwrap_or(u32::MAX))
        .expire_after(RtExpiry(ttls))
        .build()
}
/// Largest cached value (§7.6).
pub const MAX_VALUE_BYTES: usize = 256 * 1024;
/// `viewer_private_cache_evictions_total`: per-viewer private entries
/// pushed out by a bound, the per-viewer cap or the total one (design-v4
/// §8.1, R3-208). Expiry and a purge are not evictions.
pub const METRIC_PRIVATE_EVICTIONS: &str = "viewer_private_cache_evictions_total";
/// Gauge `viewer_private_cache_entries`: entries in the per-viewer private
/// cache, over every viewer.
pub const METRIC_PRIVATE_ENTRIES: &str = "viewer_private_cache_entries";

/// The per-viewer private cache's settings (design-v4 §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrivateCacheSettings {
    /// `cache.viewer_private_ttl_secs`.
    pub ttl: Duration,
    /// `cache.viewer_private_max`: entries over all viewers.
    pub max_entries: u64,
    /// `cache.viewer_private_per_viewer_max`.
    pub per_viewer_max: usize,
}

impl Default for PrivateCacheSettings {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(3600),
            max_entries: 50_000,
            per_viewer_max: 500,
        }
    }
}

/// A viewer and one of its keys.
type PrivateKey = (Arc<str>, String);
/// Each viewer's keys, oldest first.
type Fifos = Arc<Mutex<HashMap<Arc<str>, VecDeque<String>>>>;

/// The per-viewer private cache. Only the viewer's own viewer-private
/// fetches read or write its entries.
#[derive(Clone)]
struct PrivateCache {
    entries: moka::sync::Cache<PrivateKey, Arc<Vec<u8>>>,
    /// Kept beside the cache for the per-viewer cap. The eviction listener
    /// removes evicted, expired and invalidated keys and ignores
    /// replacements; it runs at housekeeping, so the cap is soft by at most
    /// one housekeeping batch.
    fifos: Fifos,
    per_viewer_max: usize,
}

fn forget_key(fifos: &Fifos, viewer: &Arc<str>, key: &str) {
    let mut fifos = fifos.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(fifo) = fifos.get_mut(viewer) {
        fifo.retain(|k| k != key);
        if fifo.is_empty() {
            fifos.remove(viewer);
        }
    }
}

impl PrivateCache {
    fn new(settings: PrivateCacheSettings) -> Self {
        let fifos: Fifos = Arc::default();
        let listener = Arc::clone(&fifos);
        Self {
            entries: moka::sync::Cache::builder()
                .max_capacity(settings.max_entries)
                .time_to_live(settings.ttl)
                .eviction_policy(EvictionPolicy::lru())
                .eviction_listener(move |key: Arc<PrivateKey>, _, cause: RemovalCause| {
                    if cause == RemovalCause::Size {
                        metrics::counter!(METRIC_PRIVATE_EVICTIONS).increment(1);
                    }
                    if cause != RemovalCause::Replaced {
                        forget_key(&listener, &key.0, &key.1);
                    }
                })
                .build(),
            fifos,
            per_viewer_max: settings.per_viewer_max.max(1),
        }
    }

    fn get(&self, viewer: &str, key: &str) -> Option<Arc<Vec<u8>>> {
        self.entries.get(&(Arc::from(viewer), key.to_string()))
    }

    fn insert(&self, viewer: &str, key: &str, bytes: Arc<Vec<u8>>) {
        let viewer: Arc<str> = Arc::from(viewer);
        let evict = {
            let mut fifos = self.fifos.lock().unwrap_or_else(PoisonError::into_inner);
            let fifo = fifos.entry(Arc::clone(&viewer)).or_default();
            let mut evict = None;
            if !fifo.iter().any(|k| k == key) {
                if fifo.len() >= self.per_viewer_max {
                    evict = fifo.pop_front();
                }
                fifo.push_back(key.to_string());
            }
            evict
        };
        if let Some(oldest) = evict {
            metrics::counter!(METRIC_PRIVATE_EVICTIONS).increment(1);
            self.entries.invalidate(&(Arc::clone(&viewer), oldest));
        }
        self.entries.insert((viewer, key.to_string()), bytes);
    }

    /// Drop every entry of `viewer` (its own purge, design-v4 §14.1).
    fn forget(&self, viewer: &str) {
        let keys = self.fifos.lock().unwrap_or_else(PoisonError::into_inner).remove(viewer);
        let viewer: Arc<str> = Arc::from(viewer);
        for key in keys.into_iter().flatten() {
            self.entries.invalidate(&(Arc::clone(&viewer), key));
        }
    }
}

/// The read-time cache. Cheap to clone.
#[derive(Clone)]
pub struct ReadCache {
    cache: Cache<String, Arc<Vec<u8>>>,
    capacity_bytes: u64,
    private: PrivateCache,
}

impl ReadCache {
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            cache: shared_cache(capacity_bytes, RtTtls::default()),
            capacity_bytes,
            private: PrivateCache::new(PrivateCacheSettings::default()),
        }
    }

    /// Set the lifetime of each class of key. Entries already held are
    /// dropped, so this is for construction.
    #[must_use]
    pub fn with_ttls(mut self, ttls: RtTtls) -> Self {
        self.cache = shared_cache(self.capacity_bytes, ttls);
        self
    }

    /// Whether the shared entries hold `key`, without touching its LRU or
    /// frequency state. For the background refresh, which has no request.
    pub fn contains_shared(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    /// Cache a value in the shared entries, as [`Self::put_json`] does for
    /// an ordinary request. For the background refresh only, which has no
    /// request and only ever writes shared entries.
    pub async fn put_shared_json<T: Serialize>(&self, key: &str, value: &T) {
        let Ok(bytes) = serde_json::to_vec(value) else {
            return;
        };
        if bytes.len() <= MAX_VALUE_BYTES {
            self.cache.insert(key.to_string(), Arc::new(bytes)).await;
        }
    }

    /// Whether `did` has a negative profile entry, for `req`. A
    /// viewer-private request never reads one, nor touches the key
    /// (design-v4 §8.1, R3-208).
    pub async fn has_negative(&self, req: &Requester, did: &str) -> bool {
        if req.is_viewer_private() || req.is_local_only() {
            return false;
        }
        self.cache.get(&format!("{PROFILE_NEG_PREFIX}{did}")).await.is_some()
    }

    /// Write a negative profile entry for `did` in the shared entries
    /// (design-v4 §8.1 "Negative entries"), counted in
    /// `profile_refresh_negative_total`. A positive entry is left alone.
    /// The caller has checked that every callable network answered a 2xx
    /// without `did` and that its request may write shared entries.
    pub async fn put_negative(&self, did: &str) {
        self.cache.insert(format!("{PROFILE_NEG_PREFIX}{did}"), Arc::new(b"null".to_vec())).await;
        metrics::counter!(METRIC_PROFILE_NEGATIVE).increment(1);
    }

    /// Set the per-viewer private cache's limits.
    #[must_use]
    pub fn with_private(mut self, settings: PrivateCacheSettings) -> Self {
        self.private = PrivateCache::new(settings);
        self
    }

    /// A cached value for `req`: from the shared entries, or for a
    /// viewer-private request from its viewer's own cache only.
    pub async fn get_json<T: DeserializeOwned>(&self, req: &Requester, key: &str) -> Option<T> {
        let bytes = match req.private_viewer() {
            Some(viewer) => self.private.get(viewer, key)?,
            None => self.cache.get(key).await?,
        };
        serde_json::from_slice(&bytes).ok()
    }

    /// Cache a value for `req` unless it exceeds [`MAX_VALUE_BYTES`]: in
    /// the shared entries, or for a viewer-private request in its viewer's
    /// own cache only.
    pub async fn put_json<T: Serialize>(&self, req: &Requester, key: &str, value: &T) {
        let Ok(bytes) = serde_json::to_vec(value) else {
            return;
        };
        if bytes.len() > MAX_VALUE_BYTES {
            return;
        }
        match req.private_viewer() {
            Some(viewer) => self.private.insert(viewer, key, Arc::new(bytes)),
            None => self.cache.insert(key.to_string(), Arc::new(bytes)).await,
        }
    }

    /// Drop `viewer`'s private entries (its purge, design-v4 §14.1).
    pub fn forget_viewer(&self, viewer: &str) {
        self.private.forget(viewer);
    }

    /// Set `viewer_private_cache_entries` from the cache as it is now,
    /// after its pending housekeeping (which also reports evictions).
    pub fn sample_private(&self) {
        self.private.entries.run_pending_tasks();
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_PRIVATE_ENTRIES).set(self.private.entries.entry_count() as f64);
    }

    /// Current weighted size (tests).
    pub async fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.weighted_size()
    }

    /// The shared keys, without touching their LRU or frequency state
    /// (tests: B-53 "No shared-cache reads").
    #[cfg(test)]
    pub async fn shared_keys(&self) -> Vec<String> {
        self.cache.run_pending_tasks().await;
        let mut keys: Vec<String> = self.cache.iter().map(|(k, _)| k.as_ref().clone()).collect();
        keys.sort();
        keys
    }

    /// `viewer`'s private keys, oldest first (tests).
    #[cfg(test)]
    pub fn private_keys(&self, viewer: &str) -> Vec<String> {
        self.private.entries.run_pending_tasks();
        self.private
            .fifos
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(viewer)
            .map(|f| f.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv";
    const W: &str = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";

    #[tokio::test]
    async fn round_trip_bound_and_oversize() {
        let c = ReadCache::new(10 * 1024);
        let req = Requester::test();
        c.put_json(&req, "a", &vec![1u32, 2, 3]).await;
        assert_eq!(c.get_json::<Vec<u32>>(&req, "a").await, Some(vec![1, 2, 3]));
        c.put_json(&req, "big", &"x".repeat(MAX_VALUE_BYTES + 1)).await;
        assert_eq!(c.get_json::<String>(&req, "big").await, None);
        for i in 0..100 {
            c.put_json(&req, &format!("k{i}"), &"y".repeat(1000)).await;
        }
        assert!(c.weighted_size().await <= 10 * 1024);
    }

    /// B-53 "No shared-cache reads" (R3-208), in-crate: a viewer-private
    /// request neither reads nor writes the shared entries, and its own
    /// entries are never returned to another viewer or to a public request.
    #[tokio::test]
    async fn a_viewer_private_request_never_touches_the_shared_entries() {
        let c = ReadCache::new(1024 * 1024);
        let public = Requester::test();
        let mine = public.viewer_private(V);
        c.put_json(&public, "rt:profile:shared", &1u32).await;
        // A shared entry is invisible to a viewer-private read.
        assert_eq!(c.get_json::<u32>(&mine, "rt:profile:shared").await, None);
        // A viewer-private write lands in the viewer's cache only.
        c.put_json(&mine, "rt:profile:x", &2u32).await;
        assert_eq!(c.shared_keys().await, ["rt:profile:shared"]);
        assert_eq!(c.get_json::<u32>(&mine, "rt:profile:x").await, Some(2));
        assert_eq!(c.get_json::<u32>(&public, "rt:profile:x").await, None);
        assert_eq!(c.get_json::<u32>(&public.viewer_private(W), "rt:profile:x").await, None);
        // The viewer's purge drops its entries.
        c.forget_viewer(V);
        assert_eq!(c.get_json::<u32>(&mine, "rt:profile:x").await, None);
        assert!(c.private_keys(V).is_empty());
    }

    #[tokio::test]
    async fn the_per_viewer_cap_drops_the_viewers_oldest_entry() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let settings = PrivateCacheSettings { per_viewer_max: 2, ..PrivateCacheSettings::default() };
        let c = ReadCache::new(1024 * 1024).with_private(settings);
        let mine = Requester::test().viewer_private(V);
        let other = Requester::test().viewer_private(W);
        c.put_json(&other, "w", &0u32).await;
        for (i, key) in ["a", "b", "a", "c"].into_iter().enumerate() {
            c.put_json(&mine, key, &i).await;
        }
        // "a" was replaced, not added twice, so "c" pushed out "a".
        assert_eq!(c.private_keys(V), ["b", "c"]);
        assert_eq!(c.get_json::<usize>(&mine, "a").await, None);
        assert_eq!(c.get_json::<usize>(&mine, "c").await, Some(3));
        // Another viewer's entries are untouched.
        assert_eq!(c.get_json::<u32>(&other, "w").await, Some(0));
        // design-v4 §14.2: one eviction, and three entries left; a purge
        // is not an eviction.
        c.forget_viewer(W);
        c.sample_private();
        let text = handle.render();
        assert_eq!(crate::test_support::series(&text, METRIC_PRIVATE_EVICTIONS, &[]), Some(1.0), "{text}");
        assert_eq!(crate::test_support::series(&text, METRIC_PRIVATE_ENTRIES, &[]), Some(2.0), "{text}");
    }

    /// The total bound evicts too, and is counted.
    #[tokio::test]
    async fn the_total_bound_is_counted_as_an_eviction() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let settings = PrivateCacheSettings { max_entries: 4, ..PrivateCacheSettings::default() };
        let c = ReadCache::new(1024 * 1024).with_private(settings);
        let mine = Requester::test().viewer_private(V);
        for i in 0..20u32 {
            c.put_json(&mine, &format!("k{i}"), &i).await;
        }
        c.sample_private();
        let text = handle.render();
        let entries = crate::test_support::series(&text, METRIC_PRIVATE_ENTRIES, &[]).expect("entries");
        let evicted = crate::test_support::series(&text, METRIC_PRIVATE_EVICTIONS, &[]).expect("evictions");
        assert!(entries <= 4.0 && evicted + entries >= 20.0, "{text}");
    }
}
