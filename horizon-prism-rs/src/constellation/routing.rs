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
use crate::identity::handle_lookup::HandleOutcome;
use crate::identity::lodestar::Lodestar;
use crate::identity::resolver::{Caches, Upstream, admit_all};
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
    /// The background profile refresh's bucket (`background_rate_per_sec`,
    /// `background_burst`).
    pub background: (crate::config::Float, u32),
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
                background: (n.background_rate_per_sec, n.background_burst),
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

    /// Warm the summaries of `dids` a request is about to resolve, in as
    /// few calls as possible (lodestar-reads design §2.10). By default
    /// nothing: each DID is resolved when it is asked for.
    async fn prefetch(&self, _req: &Requester, _dids: &[String]) {}
}

/// A summary from lodestar's account, when it answers everything a
/// summary holds (§2.10 step 5): an active account, a PDS by the same rule
/// as a document's, and a verified handle or no claim at all.
fn batch_summary(account: &crate::identity::lodestar::Account) -> Option<DidSummary> {
    if account.status != "active" {
        return None;
    }
    let pds = crate::identity::resolver::normalize_pds(account.pds.as_deref()?)?;
    let handle = match (&account.handle, &account.claimed_handle) {
        (Some(handle), _) => {
            let handle = handle.to_ascii_lowercase();
            Some(proto_blue::syntax::Handle::is_valid(&handle).then_some(handle)?)
        }
        (None, None) => None,
        (None, Some(_)) => return None,
    };
    Some(DidSummary { pds: Some(pds), handle })
}

/// `resolveHandle`'s cache (design-v3 §5.11 step 4): how many answers are
/// kept, and for how long a DID and a "no such handle" each.
#[derive(Debug, Clone, Copy)]
pub struct HandleCacheSettings {
    pub capacity: u64,
    pub ttl: Duration,
    pub negative_ttl: Duration,
}

/// The two caches of a [`RequestLookup`]'s typed handle lookup. A "could
/// not find out" is never kept.
#[derive(Clone)]
struct Handles {
    found: Cache<String, String>,
    absent: Cache<String, ()>,
}

/// `handle_resolutions_total{intent, result}` (§5.11, R3-155): `intent` is
/// `public` for public request paths (`resolveHandle` and Thread Lens handle anchors) and `internal` for internal work, so
/// internal resolutions never move a public series.
pub const METRIC_HANDLE_RESOLUTIONS: &str = "handle_resolutions_total";

/// Every call goes through the request's `net` intent
/// ([`IdentityResolver::for_request`]; design-v4 §11 G-9).
#[async_trait]
impl DidLookup for IdentityResolver {
    async fn summary(&self, req: &Requester, did: &str) -> Option<DidSummary> {
        let resolver = self.for_request(req);
        let info = if req.is_viewer_private() {
            resolver.resolve_info_uncached(did, &admit_all).await.ok()?
        } else {
            resolver.resolve_info(did, ResolveIntent::Lookup).await.ok()?
        };
        let handle = resolver.verify_handle(did, info.claimed_handle.as_deref()).await;
        Some(DidSummary {
            pds: info.pds_endpoint,
            handle,
        })
    }

    async fn did_for_handle(&self, req: &Requester, handle: &str) -> Option<String> {
        self.for_request(req).resolve_handle_with(handle, &admit_all, caches_for(req)).await
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
/// - Every network call goes through the request's `net` intent
///   ([`IdentityResolver::for_request`]; design-v4 §11 G-9): a
///   viewer-private lookup's DNS, well-known and `did:web` fetches go
///   through the `viewer_private` transport (R3-213).
#[derive(Clone)]
pub struct RequestLookup {
    resolver: IdentityResolver,
    own: RequesterLimit,
    plc: SharedBucket,
    summaries: Cache<String, DidSummary>,
    /// The typed handle lookup, when one is set.
    handles: Option<Handles>,
    /// Lodestar's request bucket, when lodestar is set
    /// ([`RequestLookup::with_lodestar`]).
    lodestar: Option<SharedBucket>,
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
            lodestar: None,
        }
    }

    /// Read identities from `lodestar` first on public requests, drawing on
    /// `bucket`, lodestar's own (lodestar-reads design §2.2, §2.11).
    #[must_use]
    pub fn with_lodestar(self, lodestar: Arc<Lodestar>, bucket: SharedBucket) -> Self {
        let Self { resolver, own, plc, summaries, handles, .. } = self;
        Self {
            resolver: resolver.with_lodestar(lodestar),
            own,
            plc,
            summaries,
            handles,
            lodestar: Some(bucket),
        }
    }

    /// A request's admission of each network call (§2.11): a lodestar call
    /// takes a token of lodestar's bucket quietly (none: skip lodestar,
    /// never "no answer"); a PLC fetch takes one of PLC's, as before.
    fn admit<'a>(&'a self, req: &'a Requester) -> impl Fn(Upstream, &str) -> bool + Sync + 'a {
        move |upstream, did| match upstream {
            Upstream::Lodestar => self.lodestar.as_ref().is_some_and(|bucket| bucket.quiet_draw(req.key()).is_ok()),
            Upstream::Plc => !did.starts_with("did:plc:") || req.draw(&self.plc),
        }
    }

    /// Resolve handles through the TXT record and the well-known file of
    /// the request's transport, telling "no such handle" from "could not
    /// find out", with the answers cached.
    #[must_use]
    pub fn with_handle_lookup(mut self, cache: HandleCacheSettings) -> Self {
        self.handles = Some(Handles {
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
        let admit = self.admit(req);
        let resolver = self.resolver.for_request(req).with_deadline(req.deadline());
        let info = if shared {
            resolver.resolve_info_admitted(did, ResolveIntent::Lookup, &admit).await.ok()?
        } else {
            resolver.resolve_info_uncached(did, &admit).await.ok()?
        };
        let handle = resolver.verify_handle_admitted(did, info.claimed_handle.as_deref(), &admit).await;
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
        let admit = self.admit(req);
        self.resolver.for_request(req).with_deadline(req.deadline()).resolve_handle_with(handle, &admit, caches_for(req)).await
    }

    async fn handle_outcome(&self, req: &Requester, handle: &str) -> HandleOutcome {
        let Some(handles) = &self.handles else {
            return match self.did_for_handle(req, handle).await {
                Some(did) => HandleOutcome::Found(did),
                None => HandleOutcome::NotFound,
            };
        };
        let count = |result: &'static str| metrics::counter!(METRIC_HANDLE_RESOLUTIONS, "intent" => "public", "result" => result).increment(1);
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
        let admit = self.admit(req);
        let outcome = self.resolver.for_request(req).with_deadline(req.deadline()).resolve_handle_typed(handle, &admit, caches_for(req)).await;
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

    /// One `POST /accounts` per [`Lodestar::batch_max`] DIDs not yet
    /// summarized (§2.10): only for a public request, with lodestar set and
    /// its gate open, one token of lodestar's bucket per call. It draws
    /// nothing from the requester's identity share and never marks the
    /// request spent (review 1, LB1, LB2); what it cannot fill, the per-DID
    /// path resolves as before.
    async fn prefetch(&self, req: &Requester, dids: &[String]) {
        use crate::net::FetchMode;

        let Some(bucket) = &self.lodestar else {
            return;
        };
        if req.intent() != crate::net::Intent::Public {
            return;
        }
        let Some(lodestar) = self.resolver.for_request(req).public_lodestar() else {
            return;
        };
        let mut wanted: Vec<String> = Vec::new();
        for did in dids {
            if did.starts_with("did:plc:")
                && crate::identity::parse_did(did).is_ok()
                && !wanted.contains(did)
                && self.summaries.get(did).await.is_none()
            {
                wanted.push(did.clone());
            }
        }
        if wanted.is_empty() {
            return;
        }
        if !lodestar.gate().is_open() {
            metrics::counter!(crate::identity::lodestar::METRIC_SKIPPED, "reason" => "gate_closed").increment(1);
            return;
        }
        let mut filled = 0u64;
        for chunk in wanted.chunks(lodestar.batch_max()) {
            if bucket.quiet_draw(req.key()).is_err() {
                metrics::counter!(crate::identity::lodestar::METRIC_SKIPPED, "reason" => "no_budget").increment(1);
                break;
            }
            let Some(found) = lodestar.accounts(chunk, Some(req.deadline())).await else {
                break;
            };
            for did in chunk {
                if let Some(Ok(account)) = found.get(did)
                    && let Some(summary) = batch_summary(account)
                {
                    self.summaries.insert(did.clone(), summary).await;
                    filled += 1;
                }
            }
        }
        metrics::counter!(crate::identity::lodestar::METRIC_BATCH_FILLED).increment(filled);
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

    /// Warm the lookups of `dids` that the `route` cache does not answer
    /// (lodestar-reads design §2.10): a request about to route several
    /// authors calls this first. A viewer-private request's route cache is
    /// not read, and its lookup does nothing here.
    pub async fn prefetch(&self, req: &Requester, dids: &[String]) {
        if req.is_viewer_private() {
            return;
        }
        let mut missing = Vec::new();
        for did in dids {
            if self.cache.get(did).await.is_none() {
                missing.push(did.clone());
            }
        }
        if !missing.is_empty() {
            self.lookup.prefetch(req, &missing).await;
        }
    }

    /// The cached home network of `did`, if the `route` cache holds one: a
    /// cache-only read that never resolves the DID, asks PLC or writes any
    /// cache (design-v4 §8.1 "Drain").
    pub async fn cached_route(&self, did: &str) -> Option<RoutingDecision> {
        self.cache.get(did).await
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
        let mut cfg = ConstellationConfig::with_example();
        if let Some(n) = cfg.networks.iter_mut().find(|n| n.name == COMMUNITY_NETWORK) {
            n.pds_patterns = community.iter().map(|p| (*p).to_string()).collect();
        }
        RoutingTable::from_config(&cfg)
    }

    struct NoHandles;

    #[async_trait]
    impl crate::identity::resolver::HandleLookup for NoHandles {
        async fn resolve_handle(&self, _transport: &crate::net::Transport, _handle: &str) -> Option<String> {
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
            crate::net::Transports::loopback_for_tests(),
            db.pool.clone(),
            crate::identity::IdentitySettings {
                plc_url: base,
                fresh: Duration::from_secs(3600),
                stale_max: Duration::from_secs(86_400),
            },
            Arc::new(NeverPersist),
            Arc::new(NoHandles),
            crate::net::Intent::Public,
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

    /// A resolver that records the names asked of it and names `did` for
    /// every one.
    struct RecordingTxt {
        did: &'static str,
        asked: Mutex<Vec<String>>,
    }

    impl RecordingTxt {
        fn new(did: &'static str) -> Arc<Self> {
            Arc::new(Self { did, asked: Mutex::new(Vec::new()) })
        }

        fn asked(&self) -> usize {
            self.asked.lock().expect("asked").len()
        }
    }

    #[async_trait]
    impl crate::net::TxtSource for RecordingTxt {
        async fn txt(&self, name: &str) -> crate::net::resolver::TxtAnswer {
            self.asked.lock().expect("asked").push(name.to_string());
            crate::net::resolver::TxtAnswer::Records(vec![format!("did={}", self.did)])
        }
    }

    /// B-53 "Transport" and B-54 (design-v4 §8.1, §11 G-9; R3-168, R3-213),
    /// in-crate: a viewer-private handle resolution and summary ask DNS only
    /// through the `viewer_private` resolver, never the public one, and
    /// keep nothing for the next lookup; PLC, a fixed upstream, is asked
    /// through the public client. A public lookup of the same handle uses
    /// the public resolver.
    #[tokio::test]
    async fn viewer_private_lookups_use_only_the_viewer_private_resolver() {
        use axum::extract::{Path, State};
        use axum::http::HeaderMap;
        use axum::routing::get;

        use crate::net::client::test_user_agent;
        use crate::net::{Intent, Transports};

        const T: &str = "did:plc:tttttttttttttttttttttttt";
        const HANDLE: &str = "t.example.com";
        type Agents = Arc<Mutex<Vec<String>>>;

        let Some(db) = crate::test_support::fresh_db("viewer_private_lookups_resolver").await else {
            return;
        };
        async fn plc(State(agents): State<Agents>, headers: HeaderMap, Path(did): Path<String>) -> axum::Json<serde_json::Value> {
            let agent = headers.get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
            agents.lock().expect("agents").push(agent);
            axum::Json(serde_json::json!({
                "id": did,
                "alsoKnownAs": [format!("at://{HANDLE}")],
                "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example.com"}],
            }))
        }
        let agents = Agents::default();
        let base = crate::test_support::serve_fake(axum::Router::new().route("/{did}", get(plc)).with_state(Arc::clone(&agents))).await;
        let (public_dns, private_dns) = (RecordingTxt::new(T), RecordingTxt::new(T));
        let nets = Transports::loopback_for_tests()
            .with_dns_for_tests(Intent::Public, Arc::clone(&public_dns) as Arc<dyn crate::net::TxtSource>)
            .with_dns_for_tests(Intent::ViewerPrivate, Arc::clone(&private_dns) as Arc<dyn crate::net::TxtSource>);
        let resolver = IdentityResolver::new(
            nets,
            db.pool.clone(),
            crate::identity::IdentitySettings {
                plc_url: base,
                fresh: Duration::from_secs(3600),
                stale_max: Duration::from_secs(86_400),
            },
            Arc::new(NeverPersist),
            Intent::Public,
        );
        let lookup = RequestLookup::new(
            resolver,
            RequesterLimit::new("identity", 1000.0, 1000, 1000),
            crate::xrpc::pools::Shares::for_tests().bucket("plc", 1000.0, 1000),
        )
        .with_handle_lookup(HandleCacheSettings {
            capacity: 100,
            ttl: Duration::from_secs(300),
            negative_ttl: Duration::from_secs(60),
        });
        let public = Requester::test();
        let private = public.viewer_private("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");

        // Twice: nothing a viewer-private lookup did is kept for the next.
        for round in 1..=2 {
            assert_eq!(lookup.handle_outcome(&private, HANDLE).await, HandleOutcome::Found(T.to_string()));
            assert_eq!(private_dns.asked(), round);
        }
        let summary = lookup.summary(&private, T).await.expect("summary");
        assert_eq!(summary.handle.as_deref(), Some(HANDLE));
        assert_eq!(private_dns.asked(), 3, "the handle check of the summary asks the viewer_private resolver");
        assert_eq!(public_dns.asked(), 0, "a viewer-private lookup never asks the public resolver");
        let public_agent = test_user_agent(Intent::Public);
        assert_eq!(*agents.lock().expect("agents"), vec![public_agent.clone(); 3], "PLC is asked through the public client");

        // The same handle for an ordinary request: the public resolver.
        assert_eq!(lookup.handle_outcome(&public, HANDLE).await, HandleOutcome::Found(T.to_string()));
        assert_eq!((public_dns.asked(), private_dns.asked()), (1, 3));
        db.drop_db().await;
    }

    #[test]
    fn decisions_and_cascade_order() {
        let t = table_with_community(&["*.community.example"]);
        let bsky = t.decide("https://morel.us-east.host.bsky.network");
        assert_eq!(bsky.network_name.as_deref(), Some("bluesky"));
        assert_eq!(bsky.appview_url.as_deref(), Some("https://api.bsky.app"));
        let local = t.decide("https://pds1.community.example");
        assert!(local.is_community());
        assert_eq!(local.appview_url, None, "community is never an outbound call");
        assert_eq!(t.decide("https://pds.unknown.example"), RoutingDecision::unknown());

        assert_eq!(t.cascade(), ["bluesky", "community-a"]);
        assert_eq!(t.plan(Some("community-a")), ["community-a", "bluesky"], "home first, then the cascade without it");
        assert_eq!(t.plan(Some(COMMUNITY_NETWORK)), ["bluesky", "community-a"]);
        assert_eq!(t.plan(None), ["bluesky", "community-a"], "unknown DIDs use the cascade");
    }

    #[test]
    fn enabling_a_network_adds_it_to_the_cascade() {
        let mut cfg = ConstellationConfig::with_example();
        let extra = |name: &str, enabled: bool| NetworkConfig {
            name: name.into(),
            pds_patterns: vec![],
            appview_url: format!("https://appview.{name}.example"),
            auth: NetworkAuth::Unauth,
            appview_did: String::new(),
            rate_per_sec: Some(1),
            burst: Some(1),
            enabled,
            background_rate_per_sec: crate::config::background_rate_per_sec(),
            background_burst: crate::config::background_burst(),
        };
        // A disabled home is skipped everywhere.
        cfg.networks.push(extra("dormant", false));
        cfg.cascade.order.push("dormant".into());
        let t = RoutingTable::from_config(&cfg);
        assert_eq!(t.cascade(), ["bluesky", "community-a"]);
        assert_eq!(t.plan(Some("dormant")), ["bluesky", "community-a"], "a disabled home is skipped");
        cfg.networks.push(extra("second", true));
        cfg.cascade.order.push("second".into());
        cfg.networks.push(NetworkConfig {
            name: "offline".into(),
            pds_patterns: vec![],
            appview_url: "https://appview.offline.example".into(),
            auth: NetworkAuth::Unauth,
            appview_did: String::new(),
            rate_per_sec: Some(1),
            burst: Some(1),
            enabled: false,
            background_rate_per_sec: crate::config::background_rate_per_sec(),
            background_burst: crate::config::background_burst(),
        });
        cfg.cascade.order.push("offline".into());
        let t = RoutingTable::from_config(&cfg);
        assert_eq!(t.cascade(), ["bluesky", "community-a", "second"]);
        assert_eq!(t.plan(Some("second")), ["second", "bluesky", "community-a"]);
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
