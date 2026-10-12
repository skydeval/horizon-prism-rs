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
use crate::identity::{IdentityResolver, ResolveIntent};
use crate::xrpc::pools::Requester;

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
}

#[async_trait]
impl DidLookup for IdentityResolver {
    async fn summary(&self, _req: &Requester, did: &str) -> Option<DidSummary> {
        let info = self.resolve_info(did, ResolveIntent::Lookup).await.ok()?;
        let handle = self.verify_handle(did, info.claimed_handle.as_deref()).await;
        Some(DidSummary {
            pds: info.pds_endpoint,
            handle,
        })
    }

    async fn did_for_handle(&self, _req: &Requester, handle: &str) -> Option<String> {
        self.resolve_handle(handle).await
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

    /// The home network of `did` (§8.3 steps 2–3).
    pub async fn resolve_did(&self, req: &Requester, did: &str) -> RoutingDecision {
        if let Some(d) = self.cache.get(did).await {
            return d;
        }
        let Some(pds) = self.lookup.summary(req, did).await.and_then(|s| s.pds) else {
            return RoutingDecision::unknown();
        };
        let decision = self.table.decide(&pds);
        self.cache.insert(did.to_string(), decision.clone()).await;
        decision
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
