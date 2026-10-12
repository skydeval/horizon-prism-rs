//! The AppView cascade (design-v2 §8.3 steps 4–5).
//!
//! Networks are called in the routing table's plan order: the home AppView
//! (if callable), then the cascade `order` without it. Each call is
//! classified (§8.3); the first `hit` ends the cascade.
//!
//! **Limits.**
//! - 3 s per call (the client's), and the request's 6 s deadline, which
//!   every cascade of one request shares (design-v3 §II-8.3).
//! - A per-network request bucket (`rate_per_sec`/`burst`), split into a
//!   member pool and a non-member pool with per-requester shares. An empty
//!   share or pool is no answer and stops the plan; requests never queue.
//! - A per-network breaker: 5 consecutive `transient`/`auth-refused` calls
//!   open it for 30 s; then one half-open probe is let through. A probe that
//!   fails re-opens it; any other answer closes it. An open breaker skips
//!   the network as transient.
//! - The probe slot is held by a guard (design-v3 §II-8.3): a probe that
//!   ends without a recorded outcome, a request dropped mid-call included,
//!   gives the slot back, so the next call probes instead.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

use super::classifier::{Class, classify};
use super::routing::RoutingTable;
use crate::appview_clients::AppViewClient;
use crate::net::{NetError, NetResponse};
use crate::xrpc::pools::{Requester, SharedBucket, Shares};

/// Consecutive failures that open a breaker.
pub const BREAKER_THRESHOLD: u32 = 5;
/// How long an open breaker stays open.
pub const BREAKER_OPEN: Duration = Duration::from_secs(30);

pub const METRIC_CALLS: &str = "upstream_calls_total";
pub const METRIC_AUTH_REFUSED: &str = "upstream_auth_refused_total";
pub const METRIC_SKIPPED: &str = "upstream_skipped_total";
/// Calls cut short by the request's deadline, by network.
pub const METRIC_BUDGET_CUTOFF: &str = "upstream_budget_cutoff_total";

/// A per-network circuit breaker.
#[derive(Debug, Default)]
pub struct Breaker {
    failures: u32,
    open_until: Option<Instant>,
    probing: bool,
}

/// What [`Breaker::admit`] decided for one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// The breaker is open, or its half-open probe is already out.
    Refused,
    /// The breaker is closed.
    Normal,
    /// This call is the half-open probe and holds the probe slot.
    Probe,
}

impl Breaker {
    /// Decide whether a call may go out now. Past the open period, exactly
    /// one half-open probe is admitted until its result is recorded or its
    /// slot is released.
    pub fn admit(&mut self, now: Instant) -> Admit {
        match self.open_until {
            None => Admit::Normal,
            Some(until) if now < until => Admit::Refused,
            Some(_) if self.probing => Admit::Refused,
            Some(_) => {
                self.probing = true;
                Admit::Probe
            }
        }
    }

    /// Whether a call may go out now ([`Breaker::admit`] as a boolean).
    pub fn allow(&mut self, now: Instant) -> bool {
        self.admit(now) != Admit::Refused
    }

    /// Give the half-open probe slot back without a result. The breaker
    /// stays half-open, so the next call is admitted as the probe.
    pub fn release_probe(&mut self) {
        self.probing = false;
    }

    /// Record a call's class.
    pub fn record(&mut self, class: Class, now: Instant) {
        if class.is_failure() {
            self.failures += 1;
            if self.probing || self.failures >= BREAKER_THRESHOLD {
                self.open_until = Some(now + BREAKER_OPEN);
                self.probing = false;
            }
        } else {
            *self = Self::default();
        }
    }

    #[must_use]
    pub fn is_open(&self, now: Instant) -> bool {
        self.open_until.is_some_and(|until| now < until)
    }
}

/// A breaker shared by every caller of one key (a network; later Farsight's
/// read call). Calls go out through [`SharedBreaker::admit`], whose
/// [`Admission`] is the probe-slot guard of design-v3 §II-8.3.
#[derive(Debug, Default)]
pub struct SharedBreaker(Mutex<Breaker>);

impl SharedBreaker {
    fn lock(&self) -> std::sync::MutexGuard<'_, Breaker> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Ask to make a call. `None` means the breaker refuses it.
    pub fn admit(&self, now: Instant) -> Option<Admission<'_>> {
        match self.lock().admit(now) {
            Admit::Refused => None,
            Admit::Normal => Some(Admission {
                breaker: self,
                holds_probe: false,
            }),
            Admit::Probe => Some(Admission {
                breaker: self,
                holds_probe: true,
            }),
        }
    }

    #[must_use]
    pub fn is_open(&self, now: Instant) -> bool {
        self.lock().is_open(now)
    }

    /// Put the breaker in the half-open state: its open period has passed
    /// and no probe is out.
    #[cfg(test)]
    pub(crate) fn force_half_open(&self) {
        let mut b = self.lock();
        b.open_until = Some(Instant::now());
        b.probing = false;
    }
}

/// An admitted call. [`Admission::record`] reports its outcome. If it is
/// dropped without one, for any reason, the half-open probe slot it may hold
/// is released: an empty bucket, an outcome the breaker does not record, or
/// a request dropped while its call is in flight (a client that disconnects
/// drops its handler, and the call with it). Without this an unrecorded
/// probe would leave the breaker refusing every call until restart.
#[derive(Debug)]
pub struct Admission<'a> {
    breaker: &'a SharedBreaker,
    holds_probe: bool,
}

impl Admission<'_> {
    /// Record the call's class. A failure counts toward opening the breaker
    /// (a failed probe re-opens it at once); anything else closes it.
    pub fn record(mut self, class: Class, now: Instant) {
        self.breaker.lock().record(class, now);
        self.holds_probe = false;
    }
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        if self.holds_probe {
            self.breaker.lock().release_probe();
        }
    }
}

/// One upstream call, by method.
#[derive(Debug, Clone, Copy)]
pub enum Call<'a> {
    Profile(&'a str),
    Posts(&'a [String]),
    PostThread { uri: &'a str, depth: u32, parent_height: u32 },
    AuthorFeed { actor: &'a str, limit: u32, cursor: Option<&'a str>, filter: &'a str },
    Follows { actor: &'a str, limit: u32, cursor: Option<&'a str> },
    List { list: &'a str, limit: u32, cursor: Option<&'a str> },
    Lists { actor: &'a str, limit: u32, cursor: Option<&'a str>, purposes: &'a [String] },
    StarterPack(&'a str),
    ActorStarterPacks { actor: &'a str, limit: u32, cursor: Option<&'a str> },
}

impl Call<'_> {
    async fn invoke(self, client: &AppViewClient) -> Result<NetResponse, NetError> {
        match self {
            Self::Profile(actor) => client.get_profile(actor).await,
            Self::Posts(uris) => client.get_posts(uris).await,
            Self::PostThread { uri, depth, parent_height } => client.get_post_thread(uri, depth, parent_height).await,
            Self::AuthorFeed { actor, limit, cursor, filter } => client.get_author_feed(actor, limit, cursor, filter).await,
            Self::Follows { actor, limit, cursor } => client.get_follows(actor, limit, cursor).await,
            Self::List { list, limit, cursor } => client.get_list(list, limit, cursor).await,
            Self::Lists { actor, limit, cursor, purposes } => client.get_lists(actor, limit, cursor, purposes).await,
            Self::StarterPack(uri) => client.get_starter_pack(uri).await,
            Self::ActorStarterPacks { actor, limit, cursor } => client.get_actor_starter_packs(actor, limit, cursor).await,
        }
    }
}

/// The cascade's result.
#[derive(Debug, Clone, PartialEq)]
pub enum Fetched {
    Hit { network: String, body: Value },
    /// No hit; every network tried (or skipped) with its class.
    Exhausted(Vec<(String, Class)>),
    /// The plan stopped early: the requester's share or pool was empty, or
    /// the request's deadline passed. The networks tried before that are
    /// listed, but the result is no answer whatever they said.
    NoAnswer(Vec<(String, Class)>),
}

impl Fetched {
    #[must_use]
    pub fn hit(self) -> Option<Value> {
        match self {
            Self::Hit { body, .. } => Some(body),
            Self::Exhausted(_) | Self::NoAnswer(_) => None,
        }
    }

    /// No network answered with a miss or a withheld response: every one was
    /// transient, auth-refused or behind an open breaker, or the plan
    /// stopped early.
    #[must_use]
    pub fn all_transient(&self) -> bool {
        match self {
            Self::Hit { .. } => false,
            Self::Exhausted(attempts) => attempts.iter().all(|(_, c)| c.is_failure()),
            Self::NoAnswer(_) => true,
        }
    }
}

struct NetworkState {
    client: AppViewClient,
    bucket: Option<SharedBucket>,
    breaker: SharedBreaker,
}

/// The cascade. Cheap to clone.
#[derive(Clone)]
pub struct Cascade {
    table: Arc<RoutingTable>,
    networks: Arc<HashMap<String, NetworkState>>,
}

impl Cascade {
    /// One state per client; the request bucket comes from the table and is
    /// split into pools and shares (design-v3 §II-8.3).
    #[must_use]
    pub fn new(table: Arc<RoutingTable>, clients: Vec<AppViewClient>, shares: &Shares) -> Self {
        let networks = clients
            .into_iter()
            .map(|client| {
                let bucket = table
                    .network(client.network())
                    .and_then(|n| n.rate)
                    .map(|(rate, burst)| shares.bucket("network", f64::from(rate), burst));
                (
                    client.network().to_string(),
                    NetworkState {
                        client,
                        bucket,
                        breaker: SharedBreaker::default(),
                    },
                )
            })
            .collect();
        Self {
            table,
            networks: Arc::new(networks),
        }
    }

    #[must_use]
    pub fn table(&self) -> &RoutingTable {
        &self.table
    }

    /// Whether `network`'s breaker is open now (tests, metrics).
    #[must_use]
    pub fn breaker_open(&self, network: &str) -> bool {
        self.networks
            .get(network)
            .is_some_and(|s| s.breaker.is_open(Instant::now()))
    }

    #[cfg(test)]
    pub(crate) fn force_half_open(&self, network: &str) {
        if let Some(s) = self.networks.get(network) {
            s.breaker.force_half_open();
        }
    }

    /// Run `call` through the plan for a DID whose home network is `home`.
    pub async fn fetch(&self, req: &Requester, home: Option<&str>, call: Call<'_>) -> Fetched {
        let deadline = req.deadline();
        let mut attempts = Vec::new();
        for name in self.table.plan(home) {
            let Some(state) = self.networks.get(&name) else {
                continue;
            };
            let now = Instant::now();
            if now >= deadline {
                metrics::counter!(METRIC_SKIPPED, "network" => name, "reason" => "deadline").increment(1);
                return Fetched::NoAnswer(attempts);
            }
            // The admission is the probe-slot guard (design-v3 §II-8.3):
            // every exit below that does not record an outcome, and a drop
            // of this future while the call is in flight, releases the slot.
            let Some(admission) = state.breaker.admit(now) else {
                metrics::counter!(METRIC_SKIPPED, "network" => name.clone(), "reason" => "breaker_open").increment(1);
                attempts.push((name, Class::Transient));
                continue;
            };
            // An empty share or pool is "no answer" and stops the plan: it
            // is no breaker record, and no later network's miss may turn it
            // into a not-found.
            if state.bucket.as_ref().is_some_and(|b| !req.draw(b)) {
                metrics::counter!(METRIC_SKIPPED, "network" => name, "reason" => "rate_limited").increment(1);
                drop(admission);
                return Fetched::NoAnswer(attempts);
            }
            let Ok(result) = tokio::time::timeout(deadline - now, call.invoke(&state.client)).await else {
                // The request's own deadline fired before the call's 3 s
                // timeout could: a `budget` cut, not an upstream failure
                // (design-v3 §II-8.3, R3-174). It is not recorded by the
                // breaker, gives a probe slot back, and is no answer.
                metrics::counter!(METRIC_BUDGET_CUTOFF, "network" => name).increment(1);
                drop(admission);
                return Fetched::NoAnswer(attempts);
            };
            let (class, body) = classify(&result);
            admission.record(class, Instant::now());
            metrics::counter!(METRIC_CALLS, "network" => name.clone(), "class" => class.label()).increment(1);
            if class == Class::AuthRefused {
                metrics::counter!(METRIC_AUTH_REFUSED, "network" => name.clone()).increment(1);
            }
            if let (Class::Hit, Some(body)) = (class, body) {
                return Fetched::Hit { network: name, body };
            }
            attempts.push((name, class));
        }
        Fetched::Exhausted(attempts)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use serde_json::json;

    use super::*;
    use crate::appview_clients::AppViewAuth;
    use crate::config::ConstellationConfig;
    use crate::net::NetClient;
    use crate::test_support::serve_fake;

    #[test]
    fn breaker_opens_after_five_failures_and_half_opens_after_thirty_seconds() {
        let t0 = Instant::now();
        let mut b = Breaker::default();
        for _ in 0..4 {
            assert!(b.allow(t0));
            b.record(Class::Transient, t0);
        }
        assert!(!b.is_open(t0), "four failures keep it closed");
        b.record(Class::AuthRefused, t0);
        assert!(b.is_open(t0));
        assert!(!b.allow(t0 + Duration::from_secs(29)));
        // One half-open probe after 30 s; a second caller waits.
        let t1 = t0 + BREAKER_OPEN;
        assert!(b.allow(t1));
        assert!(!b.allow(t1));
        // A failed probe re-opens at once.
        b.record(Class::Transient, t1);
        assert!(b.is_open(t1));
        let t2 = t1 + BREAKER_OPEN;
        assert!(b.allow(t2));
        // A successful probe closes it.
        b.record(Class::Miss, t2);
        assert!(!b.is_open(t2));
        assert!(b.allow(t2) && b.allow(t2));
        // Non-failures reset the streak.
        for _ in 0..4 {
            b.record(Class::Transient, t2);
        }
        b.record(Class::Withheld, t2);
        b.record(Class::Transient, t2);
        assert!(!b.is_open(t2));
    }

    /// design-v3 §II-8.3, the probe-slot guard: a probe that ends without a
    /// recorded outcome gives its slot back.
    #[test]
    fn an_unrecorded_probe_releases_its_slot() {
        let b = SharedBreaker::default();
        let t0 = Instant::now();
        for _ in 0..BREAKER_THRESHOLD {
            b.admit(t0).expect("closed").record(Class::Transient, t0);
        }
        assert!(b.is_open(t0));
        assert!(b.admit(t0).is_none());

        let t1 = t0 + BREAKER_OPEN;
        let probe = b.admit(t1).expect("the probe");
        assert!(b.admit(t1).is_none(), "one probe at a time");
        drop(probe);
        // Released, not recorded: still half-open, and the next call is the
        // probe.
        let probe = b.admit(t1).expect("the slot was released");
        assert!(b.admit(t1).is_none());
        probe.record(Class::Transient, t1);
        assert!(b.is_open(t1), "a failed probe re-opens");

        let t2 = t1 + BREAKER_OPEN;
        b.admit(t2).expect("the probe").record(Class::Hit, t2);
        assert!(!b.is_open(t2));
        // Closed: any number of calls, and an unrecorded one changes nothing.
        let (first, second) = (b.admit(t2), b.admit(t2));
        assert!(first.is_some() && second.is_some());
        drop((first, second));
        assert!(b.admit(t2).is_some());
    }

    /// B-79's network-breaker case: a request dropped while it holds the
    /// half-open probe does not leave the network refused until restart.
    #[tokio::test]
    async fn a_dropped_probe_request_does_not_wedge_the_network() {
        let hang = Arc::new(AtomicBool::new(true));
        let router = Router::new()
            .route(
                "/xrpc/{method}",
                get(|State(hang): State<Arc<AtomicBool>>| async move {
                    if hang.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    axum::Json(json!({"did": "did:plc:x", "handle": "x.test"})).into_response()
                }),
            )
            .with_state(Arc::clone(&hang));
        let base = serve_fake(router).await;
        let cascade = cascade_for(&[("bluesky", &base)]);
        cascade.force_half_open("bluesky");

        // The probing request is dropped well before the per-call timeout,
        // as when its client disconnects.
        let req = Requester::test();
        let probing = cascade.fetch(&req, Some("bluesky"), Call::Profile("x.test"));
        assert!(tokio::time::timeout(Duration::from_millis(200), probing).await.is_err());

        hang.store(false, Ordering::SeqCst);
        // The next call is admitted as the probe, and its hit closes the
        // breaker.
        for _ in 0..2 {
            match cascade.fetch(&req, Some("bluesky"), Call::Profile("x.test")).await {
                Fetched::Hit { network, .. } => assert_eq!(network, "bluesky"),
                other => panic!("{other:?}"),
            }
        }
        assert!(!cascade.breaker_open("bluesky"));
    }

    /// A fake AppView answering every call with a fixed status, counting
    /// calls.
    pub(crate) async fn fake_appview(status: StatusCode, body: Value) -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = (Arc::clone(&calls), status, body);
        let router = Router::new()
            .route(
                "/xrpc/{method}",
                get(|State((calls, status, body)): State<(Arc<AtomicUsize>, StatusCode, Value)>| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    (status, axum::Json(body)).into_response()
                }),
            )
            .with_state(state);
        (serve_fake(router).await, calls)
    }

    /// A routing table whose outbound networks point at the given bases.
    pub(crate) fn cascade_for(bases: &[(&str, &str)]) -> Cascade {
        let mut cfg = ConstellationConfig::default();
        for n in &mut cfg.networks {
            if let Some((_, base)) = bases.iter().find(|(name, _)| *name == n.name) {
                n.appview_url = (*base).to_string();
            }
        }
        let table = Arc::new(RoutingTable::from_config(&cfg));
        let net = NetClient::loopback_for_tests();
        let clients = table
            .networks()
            .iter()
            .filter(|n| n.callable())
            .map(|n| AppViewClient::new(&n.name, n.appview_url.as_deref().unwrap_or_default(), AppViewAuth::Unauth, net.clone()))
            .collect();
        Cascade::new(table, clients, &Shares::for_tests())
    }

    #[tokio::test]
    async fn home_first_then_cascade_skipping_misses_and_opening_breakers() {
        let (down, down_calls) = fake_appview(StatusCode::SERVICE_UNAVAILABLE, json!({})).await;
        let (hit, hit_calls) = fake_appview(StatusCode::OK, json!({"did": "did:plc:x", "handle": "x.test"})).await;
        let cascade = cascade_for(&[("bluesky", &down), ("community-a", &hit)]);
        let req = Requester::test();

        // Home community-a answers first; bluesky is never called.
        match cascade.fetch(&req, Some("community-a"), Call::Profile("x.test")).await {
            Fetched::Hit { network, body } => assert_eq!((network.as_str(), &body["handle"]), ("community-a", &json!("x.test"))),
            other => panic!("{other:?}"),
        }
        assert_eq!(down_calls.load(Ordering::SeqCst), 0);

        // Unknown home: bluesky (transient) then community-a (hit).
        for _ in 0..5 {
            assert!(cascade.fetch(&req, None, Call::Profile("x.test")).await.hit().is_some());
        }
        assert_eq!(down_calls.load(Ordering::SeqCst), 5);
        assert!(cascade.breaker_open("bluesky"), "five transient calls open the breaker");
        // While open, bluesky is skipped without a call.
        assert!(cascade.fetch(&req, None, Call::Profile("x.test")).await.hit().is_some());
        assert_eq!(down_calls.load(Ordering::SeqCst), 5);
        assert_eq!(hit_calls.load(Ordering::SeqCst), 7);
    }

    #[tokio::test]
    async fn exhaustion_reports_classes() {
        let (missing, _) = fake_appview(StatusCode::BAD_REQUEST, json!({"error": "NotFound", "message": "Post not found"})).await;
        let (down, _) = fake_appview(StatusCode::BAD_GATEWAY, json!({})).await;
        let cascade = cascade_for(&[("bluesky", &missing), ("community-a", &down)]);
        let req = Requester::test();
        let r = cascade.fetch(&req, None, Call::Posts(&["at://did:plc:x/app.bsky.feed.post/1".into()])).await;
        assert_eq!(
            r,
            Fetched::Exhausted(vec![("bluesky".into(), Class::Miss), ("community-a".into(), Class::Transient)])
        );
        assert!(!r.all_transient(), "a miss is an answer");
        let cascade = cascade_for(&[("bluesky", &down), ("community-a", &down)]);
        assert!(cascade.fetch(&req, None, Call::Profile("x")).await.all_transient());
    }

    #[tokio::test]
    async fn an_empty_token_bucket_skips_the_network() {
        let (hit, calls) = fake_appview(StatusCode::OK, json!({})).await;
        let mut cfg = ConstellationConfig::default();
        for n in &mut cfg.networks {
            if n.name == "bluesky" {
                n.appview_url = hit.clone();
                n.rate_per_sec = Some(1);
                n.burst = Some(2);
            }
        }
        let table = Arc::new(RoutingTable::from_config(&cfg));
        let client = AppViewClient::new("bluesky", &hit, AppViewAuth::Unauth, NetClient::loopback_for_tests());
        let cascade = Cascade::new(table, vec![client], &Shares::for_tests());
        // Burst 2 is one token in each pool. A ready member takes its own
        // pool's, then the non-member pool's.
        let member = Requester::new(crate::xrpc::pools::ChargeKey::member("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"));
        let mut hits = 0;
        for _ in 0..5 {
            if cascade.fetch(&member, None, Call::Profile("x")).await.hit().is_some() {
                hits += 1;
            }
        }
        assert_eq!((hits, calls.load(Ordering::SeqCst)), (2, 2), "burst 2, then no answer without a call");
        assert!(member.spent());
        // Anyone else finds the non-member pool empty: no answer, the plan
        // stops, and the request remembers it.
        let req = Requester::test();
        assert!(!req.spent());
        let r = cascade.fetch(&req, None, Call::Profile("x")).await;
        assert_eq!(r, Fetched::NoAnswer(Vec::new()));
        assert!(r.all_transient() && req.spent());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!cascade.breaker_open("bluesky"), "rate limiting is not an upstream failure");
    }

    /// R3-174: a call cut by the request's deadline is not an upstream
    /// failure, however often it happens.
    #[tokio::test]
    async fn a_deadline_cut_is_not_a_breaker_failure() {
        let router = Router::new().route(
            "/xrpc/{method}",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                axum::Json(json!({})).into_response()
            }),
        );
        let base = serve_fake(router).await;
        let cascade = cascade_for(&[("bluesky", &base), ("community-a", &base)]);
        for _ in 0..(BREAKER_THRESHOLD + 2) {
            let req = Requester::test().with_deadline(Instant::now() + Duration::from_millis(50));
            let r = cascade.fetch(&req, None, Call::Profile("x.test")).await;
            assert_eq!(r, Fetched::NoAnswer(Vec::new()), "no answer, and the plan stops");
            assert!(r.all_transient());
            assert!(!req.spent(), "a deadline is not an empty pool");
        }
        assert!(!cascade.breaker_open("bluesky"));
    }

    #[tokio::test]
    async fn a_passed_deadline_is_no_answer() {
        let (missing, calls) = fake_appview(StatusCode::BAD_REQUEST, json!({"error": "NotFound", "message": "x"})).await;
        let cascade = cascade_for(&[("bluesky", &missing), ("community-a", &missing)]);
        let req = Requester::test().with_deadline(Instant::now());
        let r = cascade.fetch(&req, None, Call::Profile("x")).await;
        assert_eq!(r, Fetched::NoAnswer(Vec::new()));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
