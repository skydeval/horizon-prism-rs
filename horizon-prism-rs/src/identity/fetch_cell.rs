//! The JWT-path fetch cell and `GuardedDidFetcher` (design-v2 §6.7, rows
//! 172–173).
//!
//! kryphocron's `DefaultDidResolver` calls [`GuardedDidFetcher`] whenever its
//! own document cache misses. On that path the fetcher resolves in this order:
//! 1. a **`Fetched` cell entry** (at most 60 s old), returned even if the host
//!    row was never kept or has been evicted;
//! 2. a **fresh host row** (resolved less than 1 h ago), with no network call;
//! 3. a **`Failed` cell entry**: `UpstreamError(msg)` without fetching
//!    (fail-closed);
//! 4. otherwise (a stale row or a miss) a fetch **through the cell**.
//!
//! Implementation note (chainlink H): design-v2 §6.7 orders a `Failed` entry
//! before the fresh host row. That order lets a bad-signature token naming a
//! victim's DID, sent during an upstream outage, lock the victim out for 60 s
//! despite a fresh row, because the retry refresh always runs and always
//! invalidates kryphocron's copy. Checking the fresh row first closes that
//! without adding a network call or loosening the 1 h key-freshness rule.
//!
//! Every network fetch on this path goes through `JwtFetchCell::get_with`,
//! which coalesces concurrent initializations of one key: parallel misses for
//! one DID await a single fetch, and every outcome, success or failure, is held
//! for 60 s. The init future writes the host cache under the persistence rule
//! before it returns, so the row exists before the outcome is published. The
//! cell stores the actual outcome; nothing is inferred from host rows.
//!
//! The same cell serves the §6.7 retry-after-rotation refresh
//! ([`GuardedDidFetcher::refresh`]).

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kryphocron::Did;
use kryphocron::resolver::{ContentType, DidResolutionError, HttpDidFetcher, RawDidDoc};
use moka::future::Cache;
use moka::policy::EvictionPolicy;

use super::ResolveIntent;
use super::cache::Freshness;
use super::resolver::IdentityResolver;

/// Cell entry lifetime (§6.7).
pub const CELL_TTL: Duration = Duration::from_secs(60);
/// Cell capacity by weight (§6.7).
pub const CELL_CAPACITY_BYTES: u64 = 32 * 1024 * 1024;
/// Weight of a `Failed` entry (§6.7).
pub const FAILED_ENTRY_WEIGHT: usize = 1024;

/// The outcome of one JWT-path network fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchOutcome {
    Fetched(RawDidDoc),
    Failed(String),
}

impl FetchOutcome {
    /// What kryphocron receives for this outcome.
    ///
    /// # Errors
    ///
    /// `UpstreamError(msg)` for a failed fetch.
    pub fn to_result(&self) -> Result<RawDidDoc, DidResolutionError> {
        match self {
            Self::Fetched(doc) => Ok(doc.clone()),
            Self::Failed(msg) => Err(DidResolutionError::UpstreamError(msg.clone())),
        }
    }

    fn weight(&self) -> usize {
        match self {
            Self::Fetched(doc) => doc.bytes.len(),
            Self::Failed(_) => FAILED_ENTRY_WEIGHT,
        }
    }
}

/// Per-DID single-flight fetch cell: `moka::future::Cache` with LRU eviction,
/// a byte weigher and a 60 s TTL (§6.7, §7.6).
#[derive(Clone)]
pub struct JwtFetchCell {
    cache: Cache<String, Arc<FetchOutcome>>,
}

impl Default for JwtFetchCell {
    fn default() -> Self {
        Self::new()
    }
}

impl JwtFetchCell {
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(CELL_CAPACITY_BYTES, CELL_TTL)
    }

    #[must_use]
    pub fn with_limits(capacity_bytes: u64, ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(capacity_bytes)
            .weigher(|did: &String, outcome: &Arc<FetchOutcome>| {
                u32::try_from(did.len() + outcome.weight()).unwrap_or(u32::MAX)
            })
            .time_to_live(ttl)
            .eviction_policy(EvictionPolicy::lru())
            .build();
        Self { cache }
    }

    /// The live entry for `did`, if any. Never fetches.
    pub async fn peek(&self, did: &str) -> Option<Arc<FetchOutcome>> {
        self.cache.get(did).await
    }

    /// The entry for `did`, running `init` only if there is none. Concurrent
    /// callers for the same DID share one `init`.
    pub async fn get_with<F>(&self, did: &str, init: F) -> Arc<FetchOutcome>
    where
        F: Future<Output = FetchOutcome>,
    {
        self.cache
            .get_with(did.to_string(), async move { Arc::new(init.await) })
            .await
    }

    /// Apply pending evictions (tests and diagnostics).
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}

/// The host fetcher behind kryphocron's `DefaultDidResolver` (§6.7).
#[derive(Clone)]
pub struct GuardedDidFetcher {
    resolver: IdentityResolver,
    cell: JwtFetchCell,
}

impl GuardedDidFetcher {
    #[must_use]
    pub fn new(resolver: IdentityResolver, cell: JwtFetchCell) -> Self {
        Self { resolver, cell }
    }

    #[must_use]
    pub fn cell(&self) -> &JwtFetchCell {
        &self.cell
    }

    /// Resolve `did` for JWT verification, bounded by `deadline`.
    ///
    /// # Errors
    ///
    /// `DeadlineExceeded` past the deadline; `UpstreamError` for a failed
    /// fetch, fresh or cached in the cell.
    pub async fn fetch(&self, did: &str, deadline: Instant) -> Result<RawDidDoc, DidResolutionError> {
        let work = async {
            // 1. A fetched cell entry wins. A cached failure is held back
            //    until step 2 has had its say (see below).
            let failed = match self.cell.peek(did).await {
                Some(outcome) if matches!(outcome.as_ref(), FetchOutcome::Failed(_)) => Some(outcome),
                Some(outcome) => return outcome.to_result(),
                None => None,
            };
            // 2. A fresh host row, with no network call. Stale rows are never
            //    returned on this path (key-freshness rule).
            match self.resolver.cache().lookup(did).await {
                Ok(Some((entry, Freshness::Fresh))) => return Ok(raw_from_cached(&entry.doc)),
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(event = "did_cache_read_failed", did, %error, "did_cache read failed on the JWT path; fetching through the cell");
                }
            }
            // A cached failure fails closed, without fetching, when there is
            // no fresh host row. With a fresh row it does not: the retry
            // refresh (§6.7) runs whatever the host row's age and always
            // invalidates kryphocron's copy, so during an upstream outage a
            // bad-signature token naming a victim DID would otherwise lock the
            // victim out for 60 s although their fresh row (at most 1 h old,
            // within the key-freshness rule) can still verify them. No network
            // call is added, so the cost bound is unchanged.
            if let Some(outcome) = failed {
                return outcome.to_result();
            }
            // 3. Stale or miss: one fetch per DID per 60 s, through the cell.
            self.refresh(did).await.to_result()
        };
        match tokio::time::timeout_at(deadline.into(), work).await {
            Ok(result) => result,
            Err(_) => Err(DidResolutionError::DeadlineExceeded),
        }
    }

    /// Refresh through the cell (§6.7 retry step 1): reuse a live entry, or
    /// run exactly one fetch however many callers race.
    pub async fn refresh(&self, did: &str) -> Arc<FetchOutcome> {
        self.cell.get_with(did, self.fetch_and_store(did)).await
    }

    /// The cell's init future: fetch through `net`, write the host cache under
    /// the persistence rule, then return the outcome.
    async fn fetch_and_store(&self, did: &str) -> FetchOutcome {
        match self.resolver.fetch_document(did).await {
            Ok(fetched) => {
                if let Err(error) = self.resolver.store(did, &fetched, ResolveIntent::Lookup).await {
                    // The cell still holds the document for its 60 s.
                    tracing::warn!(event = "did_cache_write_failed", did, %error, "host cache write failed on the JWT path");
                }
                FetchOutcome::Fetched(RawDidDoc {
                    content_type: content_type(fetched.content_type.as_deref()),
                    bytes: fetched.raw,
                })
            }
            Err(error) => {
                metrics::counter!("jwt_fetch_cell_failures_total").increment(1);
                FetchOutcome::Failed(error.to_string())
            }
        }
    }
}

#[async_trait]
impl HttpDidFetcher for GuardedDidFetcher {
    async fn fetch_plc(&self, did: &Did, deadline: Instant) -> Result<RawDidDoc, DidResolutionError> {
        self.fetch(did.as_str(), deadline).await
    }

    async fn fetch_web(&self, did: &Did, deadline: Instant) -> Result<RawDidDoc, DidResolutionError> {
        self.fetch(did.as_str(), deadline).await
    }
}

/// `application/did+json` when the server said so, else `application/json`.
fn content_type(header: Option<&str>) -> ContentType {
    match header {
        Some(h) if h.to_ascii_lowercase().contains("did+json") => ContentType::ApplicationDidJson,
        _ => ContentType::ApplicationJson,
    }
}

fn raw_from_cached(doc: &serde_json::Value) -> RawDidDoc {
    RawDidDoc {
        bytes: serde_json::to_vec(doc).unwrap_or_default(),
        content_type: ContentType::ApplicationJson,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn doc(bytes: &[u8]) -> FetchOutcome {
        FetchOutcome::Fetched(RawDidDoc {
            bytes: bytes.to_vec(),
            content_type: ContentType::ApplicationJson,
        })
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_fetch() {
        let cell = JwtFetchCell::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let cell = cell.clone();
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(async move {
                cell.get_with("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    doc(b"{}")
                })
                .await
            }));
        }
        for t in tasks {
            let outcome = t.await.expect("task");
            assert_eq!(*outcome, doc(b"{}"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failures_are_held_and_not_refetched() {
        let cell = JwtFetchCell::new();
        let calls = AtomicUsize::new(0);
        let did = "did:web:down.example";
        for _ in 0..3 {
            let outcome = cell
                .get_with(did, async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    FetchOutcome::Failed("connection refused".into())
                })
                .await;
            assert_eq!(
                outcome.to_result(),
                Err(DidResolutionError::UpstreamError("connection refused".into()))
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cell.peek(did).await.is_some());
    }

    #[tokio::test]
    async fn entries_expire_after_ttl() {
        let cell = JwtFetchCell::with_limits(CELL_CAPACITY_BYTES, Duration::from_millis(100));
        let did = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        cell.get_with(did, async { doc(b"{}") }).await;
        assert!(cell.peek(did).await.is_some());
        tokio::time::sleep(Duration::from_millis(250)).await;
        cell.run_pending_tasks().await;
        assert!(cell.peek(did).await.is_none());
        let calls = AtomicUsize::new(0);
        cell.get_with(did, async {
            calls.fetch_add(1, Ordering::SeqCst);
            doc(b"{\"new\":true}")
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn weight_bounds_the_cell() {
        // Room for about three 1 KiB documents.
        let cell = JwtFetchCell::with_limits(3 * 1100, CELL_TTL);
        for i in 0..20 {
            let did = format!("did:plc:{i:024}");
            cell.get_with(&did, async { doc(&[b'x'; 1024]) }).await;
        }
        cell.run_pending_tasks().await;
        assert!(cell.cache.weighted_size() <= 3 * 1100);
    }

    #[test]
    fn content_type_mapping() {
        assert_eq!(content_type(Some("application/did+json")), ContentType::ApplicationDidJson);
        assert_eq!(content_type(Some("application/json; charset=utf-8")), ContentType::ApplicationJson);
        assert_eq!(content_type(None), ContentType::ApplicationJson);
    }

    #[test]
    fn failed_entries_weigh_one_kib() {
        assert_eq!(FetchOutcome::Failed("x".into()).weight(), 1024);
        assert_eq!(doc(b"12345").weight(), 5);
    }

    #[test]
    fn limits_match_design() {
        assert_eq!(CELL_TTL, Duration::from_secs(60));
        assert_eq!(CELL_CAPACITY_BYTES, 32 * 1024 * 1024);
    }
}
