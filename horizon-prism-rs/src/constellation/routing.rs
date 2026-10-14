//! The routing table (design-v2 §8.1, §8.2, §8.3 steps 2–5).
//!
//! Loaded from `[[constellation.networks]]` and `[constellation.cascade]`;
//! nothing about it comes from the environment. A DID's **home network** is
//! the first network whose patterns match its PDS host; the `community`
//! network means "served by this AppView", with no outbound AppView call.
//! The **call plan** for a DID is its home AppView (when enabled and not
//! local), then the cascade `order` skipping the home network and every
//! disabled network.
//!
//! Home-network decisions are cached per DID in the in-process `route`
//! cache (§7.6: 200 000 entries, TTL 1 h). Only resolved DIDs are cached; a
//! failed lookup is retried on the next request.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;

use super::matcher::{first_match, pds_host};
use crate::config::{COMMUNITY_NETWORK, ConstellationConfig, LOCAL_APPVIEW, NetworkAuth};
use crate::identity::handle_lookup::{HandleOutcome, TxtSource};
use crate::identity::resolver::Caches;
use crate::identity::{IdentityResolver, ResolveIntent};
use crate::xrpc::pools::{Requester, RequesterLimit, SharedBucket};

/// `route` cache capacity and TTL (§7.6).
pub const ROUTE_CACHE_CAPACITY: u64 = 200_000;
pub const ROUTE_CACHE_TTL: Duration = Duration::from_secs(3600);

/// One routing-table network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Network {
    pub name: String,
    /// `None` for the local (`community`) network.
    pub appview_url: Option<String>,
    pub auth: NetworkAuth,
    pub enabled: bool,
    /// Per-network token bucket (`rate_per_sec`, `burst`).
    pub rate: Option<(u32, u32)>,
}

impl Network {
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.appview_url.is_none()
    }

    /// Enabled and served by an outbound AppView.
    #[must_use]
    pub fn callable(&self) -> bool {
        self.enabled && !self.is_local()
    }
}

/// Where a DID lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingDecision {
    /// The home network's name (`community` for this AppView); `None` when
    /// the PDS is unknown or matches no network.
    pub network_name: Option<String>,
    /// The home AppView endpoint; `None` for `community` and unknown.
    pub appview_url: Option<String>,
    pub auth: Option<NetworkAuth>,
}

impl RoutingDecision {
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            network_name: None,
            appview_url: None,
            auth: None,
        }
    }

    /// The home network is this AppView: local data only, then Slingshot.
    #[must_use]
    pub fn is_community(&self) -> bool {
        self.network_name.as_deref() == Some(COMMUNITY_NETWORK)
    }
}

/// The routing table.
#[derive(Debug, Clone)]
pub struct RoutingTable {
    config: ConstellationConfig,
    networks: Vec<Network>,
    order: Vec<String>,
}

impl RoutingTable {
    #[must_use]
    pub fn from_config(cfg: &ConstellationConfig) -> Self {
        let networks = cfg
            .networks
            .iter()
            .map(|n| Network {
                name: n.name.clone(),
                appview_url: (n.appview_url != LOCAL_APPVIEW).then(|| n.appview_url.clone()),
                auth: n.auth,
                enabled: n.enabled,
                rate: n.rate_per_sec.zip(n.burst),
            })
            .collect();
        Self {
            config: cfg.clone(),
            networks,
            order: cfg.cascade.order.clone(),
        }
    }

    #[must_use]
    pub fn networks(&self) -> &[Network] {
        &self.networks
    }

    #[must_use]
    pub fn network(&self, name: &str) -> Option<&Network> {
        self.networks.iter().find(|n| n.name == name)
    }

    /// The home network for a PDS endpoint (§8.2).
    #[must_use]
    pub fn decide(&self, pds_endpoint: &str) -> RoutingDecision {
        let Some(host) = pds_host(pds_endpoint) else {
            return RoutingDecision::unknown();
        };
        match first_match(&self.config.networks, &host).and_then(|n| self.network(&n.name)) {
            Some(n) => RoutingDecision {
                network_name: Some(n.name.clone()),
                appview_url: n.appview_url.clone(),
                auth: Some(n.auth),
            },
            None => RoutingDecision::unknown(),
        }
    }

    /// Enabled outbound networks in cascade order (`resolveDid.cascade`).
    #[must_use]
    pub fn cascade(&self) -> Vec<String> {
        self.order
            .iter()
            .filter(|name| self.network(name).is_some_and(Network::callable))
            .cloned()
            .collect()
    }

    /// Networks to call, in order: the home AppView if callable, then the
    /// cascade without the home network (§8.3 steps 4–5).
    #[must_use]
    pub fn plan(&self, home: Option<&str>) -> Vec<String> {
        let mut plan: Vec<String> = home
            .and_then(|h| self.network(h))
            .filter(|n| n.callable())
            .map(|n| vec![n.name.clone()])
            .unwrap_or_default();
        for name in self.cascade() {
            if Some(name.as_str()) != home {
                plan.push(name);
            }
        }
        plan
    }
}

/// A DID's PDS endpoint and handle, as far as they resolve.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DidSummary {
    pub pds: Option<String>,
    pub handle: Option<String>,
}

/// DID resolution for routing (§6.7; stubbed in tests).
#[async_trait]
pub trait DidLookup: Send + Sync {
    /// `None` when the DID does not resolve.
    async fn summary(&self, req: &Requester, did: &str) -> Option<DidSummary>;

    /// A handle's DID, verified bidirectionally; `None` by default.
    async fn did_for_handle(&self, _req: &Requester, _handle: &str) -> Option<String> {
        None
    }

    /// A handle's DID with the reason kept when there is none
    /// (design-v3 §5.11). By default, [`DidLookup::did_for_handle`] with
    /// every miss read as "no such handle".
    async fn handle_outcome(&self, req: &Requester, handle: &str) -> HandleOutcome {
        match self.did_for_handle(req, handle).await {
            Some(did) => HandleOutcome::Found(did),
            None => HandleOutcome::NotFound,
        }
    }
}

/// `resolveHandle`'s cache (design-v3 §5.11 step 4): how many answers are
/// kept, and for how long a DID and a "no such handle" each.
#[derive(Debug, Clone, Copy)]
pub struct HandleCacheSettings {
    pub capacity: u64,
    pub ttl: Duration,
    pub negative_ttl: Duration,
}

/// The typed handle lookup of a [`RequestLookup`] and its two caches. A
/// "could not find out" is never kept.
#[derive(Clone)]
struct Handles {
    txt: Arc<dyn TxtSource>,
    found: Cache<String, String>,
    absent: Cache<String, ()>,
}

pub const METRIC_HANDLE_RESOLUTIONS: &str = "handle_resolutions_total";

#[async_trait]
impl DidLookup for IdentityResolver {
    async fn summary(&self, req: &Requester, did: &str) -> Option<DidSummary> {
        let info = if req.is_viewer_private() {
            self.resolve_info_uncached(did, &|_| true).await.ok()?
        } else {
            self.resolve_info(did, ResolveIntent::Lookup).await.ok()?
        };
        let handle = self.verify_handle(did, info.claimed_handle.as_deref()).await;
        Some(DidSummary {
            pds: info.pds_endpoint,
            handle,
        })
    }

    async fn did_for_handle(&self, req: &Requester, handle: &str) -> Option<String> {
        self.resolve_handle_with(handle, &|_| true, caches_for(req)).await
    }
}

/// The document caches `req` may use: none for a viewer-private fetch
/// (design-v4 §8.1, R3-208).
fn caches_for(req: &Requester) -> Caches {
    if req.is_viewer_private() { Caches::Bypass } else { Caches::Shared }
}

/// How long a request-side summary is reused, and how many are kept.
pub const SUMMARY_TTL: Duration = Duration::from_secs(300);
pub const SUMMARY_CAPACITY: u64 = 100_000;

/// The request path's DID lookup: [`IdentityResolver`] behind the identity
/// budget of design-v3 §II-8.3.
///
/// - A resolution is charged one token of the requester's identity share
///   (`identity.requester_resolutions_per_sec`), per DID or handle, however
///   many network calls it takes.
/// - A PLC fetch it causes also draws on PLC's shared request bucket
///   (`identity.plc_request_rate_per_sec`). A cached document costs nothing.
/// - Summaries are kept for [`SUMMARY_TTL`], so one DID named many times
///   (the author of every post in a page) is resolved and charged once.
/// - With no budget, or past the request's deadline, the answer is `None`
///   and nothing is cached.
#[derive(Clone)]
pub struct RequestLookup {
    resolver: IdentityResolver,
    own: RequesterLimit,
    plc: SharedBucket,
    summaries: Cache<String, DidSummary>,
    /// The typed handle lookup, when one is set.
    handles: Option<Handles>,
}

impl RequestLookup {
    #[must_use]
    pub fn new(resolver: IdentityResolver, own: RequesterLimit, plc: SharedBucket) -> Self {
        Self {
            resolver,
            own,
            plc,
            summaries: Cache::builder().max_capacity(SUMMARY_CAPACITY).time_to_live(SUMMARY_TTL).build(),
            handles: None,
        }
    }

    /// Resolve handles through `txt` and the well-known file, telling "no
    /// such handle" from "could not find out", with the answers cached.
    #[must_use]
    pub fn with_handle_lookup(mut self, txt: Arc<dyn TxtSource>, cache: HandleCacheSettings) -> Self {
        self.handles = Some(Handles {
            txt,
            found: Cache::builder().max_capacity(cache.capacity).time_to_live(cache.ttl).build(),
            absent: Cache::builder().max_capacity(cache.capacity).time_to_live(cache.negative_ttl).build(),
        });
        self
    }

    /// One identity token for a resolution that is about to start.
    fn admitted(&self, req: &Requester) -> bool {
        std::time::Instant::now() < req.deadline() && req.draw_own(&self.own)
    }
}

#[async_trait]
impl DidLookup for RequestLookup {
    /// A viewer-private request (design-v4 §8.1, R3-208) neither reads nor
    /// writes the summaries or the document caches.
    async fn summary(&self, req: &Requester, did: &str) -> Option<DidSummary> {
        let shared = !req.is_viewer_private();
        if shared && let Some(s) = self.summaries.get(did).await {
            return Some(s);
        }
        if !self.admitted(req) {
            return None;
        }
        let plc = |did: &str| !did.starts_with("did:plc:") || req.draw(&self.plc);
        let info = if shared {
            self.resolver.resolve_info_admitted(did, ResolveIntent::Lookup, &plc).await.ok()?
        } else {
            self.resolver.resolve_info_uncached(did, &plc).await.ok()?
        };
        let handle = self.resolver.verify_handle(did, info.claimed_handle.as_deref()).await;
        let summary = DidSummary {
            pds: info.pds_endpoint,
            handle,
        };
        if shared {
            self.summaries.insert(did.to_string(), summary.clone()).await;
        }
        Some(summary)
    }

    async fn did_for_handle(&self, req: &Requester, handle: &str) -> Option<String> {
        if !self.admitted(req) {
            return None;
        }
        let plc = |did: &str| !did.starts_with("did:plc:") || req.draw(&self.plc);
        self.resolver.resolve_handle_with(handle, &plc, caches_for(req)).await
    }

    async fn handle_outcome(&self, req: &Requester, handle: &str) -> HandleOutcome {
        let Some(handles) = &self.handles else {
            return match self.did_for_handle(req, handle).await {
                Some(did) => HandleOutcome::Found(did),
                None => HandleOutcome::NotFound,
            };
        };
        let count = |result: &'static str| metrics::counter!(METRIC_HANDLE_RESOLUTIONS, "result" => result).increment(1);
        // A viewer-private request neither reads nor writes either cache,
        // negative entries included (design-v4 §8.1, R3-208).
        let shared = !req.is_viewer_private();
        if shared && let Some(did) = handles.found.get(handle).await {
            count("cache");
            return HandleOutcome::Found(did);
        }
        if shared && handles.absent.get(handle).await.is_some() {
            count("cache");
            return HandleOutcome::NotFound;
        }
        if !self.admitted(req) {
            return HandleOutcome::Unavailable;
        }
        let plc = |did: &str| !did.starts_with("did:plc:") || req.draw(&self.plc);
        let outcome = self.resolver.resolve_handle_typed(handles.txt.as_ref(), handle, &plc, caches_for(req)).await;
        match &outcome {
            HandleOutcome::Found(did) => {
                count("network_ok");
                if shared {
                    handles.found.insert(handle.to_string(), did.clone()).await;
                }
            }
            HandleOutcome::NotFound => {
                count("network_not_found");
                if shared {
                    handles.absent.insert(handle.to_string(), ()).await;
                }
            }
            HandleOutcome::Unavailable => count("network_error"),
        }
        outcome
    }
}

/// DID → routing decision, with the `route` cache. Cheap to clone.
#[derive(Clone)]
pub struct Router {
    table: Arc<RoutingTable>,
    lookup: Arc<dyn DidLookup>,
    cache: Cache<String, RoutingDecision>,
}

impl Router {
    #[must_use]
    pub fn new(table: Arc<RoutingTable>, lookup: Arc<dyn DidLookup>) -> Self {
        Self {
            table,
            lookup,
            cache: Cache::builder()
                .max_capacity(ROUTE_CACHE_CAPACITY)
                .time_to_live(ROUTE_CACHE_TTL)
                .build(),
        }
    }

    #[must_use]
    pub fn table(&self) -> &RoutingTable {
        &self.table
    }

    #[must_use]
    pub fn lookup(&self) -> &dyn DidLookup {
        self.lookup.as_ref()
    }

    /// The home network of `did` (§8.3 steps 2–3). A viewer-private request
    /// neither reads nor writes the `route` cache (design-v4 §8.1, R3-208).
    pub async fn resolve_did(&self, req: &Requester, did: &str) -> RoutingDecision {
        let shared = !req.is_viewer_private();
        if shared && let Some(d) = self.cache.get(did).await {
            return d;
        }
        let Some(pds) = self.lookup.summary(req, did).await.and_then(|s| s.pds) else {
            return RoutingDecision::unknown();
        };
        let decision = self.table.decide(&pds);
        if shared {
            self.cache.insert(did.to_string(), decision.clone()).await;
        }
        decision
    }

    /// The `route` cache's keys, without touching their LRU or frequency
    /// state (tests).
    #[cfg(test)]
    pub(crate) async fn cached_routes(&self) -> Vec<String> {
        self.cache.run_pending_tasks().await;
        let mut keys: Vec<String> = self.cache.iter().map(|(k, _)| k.as_ref().clone()).collect();
        keys.sort();
        keys
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::config::NetworkConfig;

    /// A stub lookup serving fixed PDS endpoints.
    #[derive(Default)]
    pub(crate) struct StubLookup {
        pub pds: Mutex<HashMap<String, String>>,
        pub calls: AtomicUsize,
    }

    #[async_trait]
    impl DidLookup for StubLookup {
        async fn summary(&self, _req: &Requester, did: &str) -> Option<DidSummary> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let pds = self.pds.lock().expect("lock").get(did).cloned()?;
            Some(DidSummary {
                pds: Some(pds),
                handle: Some(format!("{}.test", &did[8..12])),
            })
        }
    }

    pub(crate) fn table_with_community(community: &[&str]) -> RoutingTable {
        let mut cfg = ConstellationConfig::default();
        if let Some(n) = cfg.networks.iter_mut().find(|n| n.name == COMMUNITY_NETWORK) {
            n.pds_patterns = community.iter().map(|p| (*p).to_string()).collect();
        }
        RoutingTable::from_config(&cfg)
    }

    struct NoHandles;

    #[async_trait]
    impl crate::identity::resolver::HandleLookup for NoHandles {
        async fn resolve_handle(&self, _handle: &str) -> Option<String> {
            None
        }
    }

    struct NeverPersist;

    impl crate::identity::PersistencePolicy for NeverPersist {
        fn in_persistence_set(&self, _did: &str) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn request_lookups_are_charged_once_per_did() {
        use axum::extract::{Path, State};
        use axum::routing::get;

        let Some(db) = crate::test_support::fresh_db("request_lookups_are_charged_once_per_did").await else {
            return;
        };
        let calls = Arc::new(AtomicUsize::new(0));
        async fn plc(State(calls): State<Arc<AtomicUsize>>, Path(did): Path<String>) -> axum::Json<serde_json::Value> {
            calls.fetch_add(1, Ordering::SeqCst);
            axum::Json(serde_json::json!({
                "id": did,
                "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example.com"}],
            }))
        }
        let base = crate::test_support::serve_fake(axum::Router::new().route("/{did}", get(plc)).with_state(Arc::clone(&calls))).await;
        let resolver = IdentityResolver::with_handles(
            crate::net::NetClient::loopback_for_tests(),
            db.pool.clone(),
            crate::identity::IdentitySettings {
                plc_url: base,
                fresh: Duration::from_secs(3600),
                stale_max: Duration::from_secs(86_400),
            },
            Arc::new(NeverPersist),
            Arc::new(NoHandles),
        );
        // Identity share: burst 3. PLC: burst 2, so one token per pool.
        let lookup = RequestLookup::new(
            resolver,
            RequesterLimit::new("identity", 1.0, 3, 1000),
            crate::xrpc::pools::Shares::for_tests().bucket("plc", 1.0, 2),
        );
        let (a, b, c, d) = (
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
            "did:plc:cccccccccccccccccccccccc",
            "did:plc:dddddddddddddddddddddddd",
        );

        let req = Requester::test();
        let found = lookup.summary(&req, a).await.expect("resolved");
        assert_eq!(found.pds.as_deref(), Some("https://pds.example.com"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // The same DID again costs nothing, for this requester or another.
        for _ in 0..5 {
            assert!(lookup.summary(&req, a).await.is_some());
        }
        let other = Requester::of(None, "198.51.100.7".parse().expect("ip"));
        assert!(lookup.summary(&other, a).await.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!req.spent() && !other.spent());

        // PLC's non-member pool is empty: no answer, no fetch, nothing cached.
        assert!(lookup.summary(&req, b).await.is_none());
        assert!(req.spent());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // That used the requester's second identity token; the third is its
        // last, and after it no PLC token is even asked for.
        assert!(lookup.summary(&req, c).await.is_none());
        assert!(lookup.summary(&req, d).await.is_none());
        assert!(lookup.did_for_handle(&req, "alice.example.com").await.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Past its deadline a request resolves nothing new, and is not charged.
        let late = Requester::of(None, "198.51.100.9".parse().expect("ip")).with_deadline(std::time::Instant::now());
        assert!(lookup.summary(&late, b).await.is_none());
        assert!(!late.spent());
        assert!(lookup.summary(&late, a).await.is_some(), "a cached summary is still served");
        db.drop_db().await;
    }

    #[test]
    fn decisions_and_cascade_order() {
        let t = table_with_community(&["*.community.example"]);
        let bsky = t.decide("https://morel.us-east.host.bsky.network");
        assert_eq!(bsky.network_name.as_deref(), Some("bluesky"));
        assert_eq!(bsky.appview_url.as_deref(), Some("https://public.api.bsky.app"));
        let local = t.decide("https://pds1.community.example");
        assert!(local.is_community());
        assert_eq!(local.appview_url, None, "community is never an outbound call");
        assert_eq!(t.decide("https://pds.unknown.example"), RoutingDecision::unknown());

        // Community B ships disabled: skipped everywhere.
        assert_eq!(t.cascade(), ["bluesky", "community-a"]);
        assert_eq!(t.plan(Some("community-a")), ["community-a", "bluesky"], "home first, then the cascade without it");
        assert_eq!(t.plan(Some("community-b")), ["bluesky", "community-a"], "a disabled home is skipped");
        assert_eq!(t.plan(Some(COMMUNITY_NETWORK)), ["bluesky", "community-a"]);
        assert_eq!(t.plan(None), ["bluesky", "community-a"], "unknown DIDs use the cascade");
    }

    #[test]
    fn enabling_a_network_adds_it_to_the_cascade() {
        let mut cfg = ConstellationConfig::default();
        for n in &mut cfg.networks {
            if n.name == "community-b" {
                n.enabled = true;
                n.appview_did = "did:web:api.community-b.example".into();
            }
        }
        cfg.networks.push(NetworkConfig {
            name: "offline".into(),
            pds_patterns: vec![],
            appview_url: "https://appview.offline.example".into(),
            auth: NetworkAuth::Unauth,
            appview_did: String::new(),
            rate_per_sec: Some(1),
            burst: Some(1),
            enabled: false,
        });
        cfg.cascade.order.push("offline".into());
        let t = RoutingTable::from_config(&cfg);
        assert_eq!(t.cascade(), ["bluesky", "community-a", "community-b"]);
        assert_eq!(t.plan(Some("community-b")), ["community-b", "bluesky", "community-a"]);
    }

    #[tokio::test]
    async fn router_caches_resolved_dids_only() {
        let lookup = Arc::new(StubLookup::default());
        lookup
            .pds
            .lock()
            .expect("lock")
            .insert("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into(), "https://pds.community-a.example".into());
        let router = Router::new(Arc::new(table_with_community(&[])), Arc::clone(&lookup) as Arc<dyn DidLookup>);
        let req = Requester::test();
        for _ in 0..3 {
            let d = router.resolve_did(&req, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").await;
            assert_eq!(d.network_name.as_deref(), Some("community-a"));
        }
        assert_eq!(lookup.calls.load(Ordering::SeqCst), 1, "cached after the first lookup");
        for _ in 0..2 {
            assert_eq!(router.resolve_did(&req, "did:plc:uuuuuuuuuuuuuuuuuuuuuuuu").await, RoutingDecision::unknown());
        }
        assert_eq!(lookup.calls.load(Ordering::SeqCst), 3, "unresolved DIDs are not cached");
    }
}
