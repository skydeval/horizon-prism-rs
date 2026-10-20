//! Lodestar reads through the resolver and the request path
//! (lodestar-reads design §6.1: T-L3, T-L4, T-L5, T-L7, T-L8, T-L10).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::lodestar::Lodestar;
use super::resolver::{HandleLookup, admit_all};
use super::{GuardedDidFetcher, IdentityError, IdentityResolver, IdentitySettings, JwtFetchCell, PersistencePolicy, ResolveIntent};
use crate::constellation::routing::{DidLookup, HandleCacheSettings, RequestLookup};
use crate::identity::handle_lookup::HandleOutcome;
use crate::net::{Intent, Transport, Transports};
use crate::xrpc::pools::{Requester, RequesterLimit, Shares};

const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaab";
const MOVED: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
const NEW_HOLDER: &str = "did:plc:nnnnnnnnnnnnnnnnnnnnnnnn";
const LODESTAR_PDS: &str = "https://lodestar-pds.example.com";
const PLC_PDS: &str = "https://plc-pds.example.com";

/// The claim of each DID's document: `MOVED` no longer claims the handle.
fn claim(did: &str) -> &'static str {
    if did == MOVED { "elsewhere.example.com" } else { "alice.example.com" }
}

fn doc(did: &str, pds: &str) -> Value {
    json!({
        "id": did,
        "alsoKnownAs": [format!("at://{}", claim(did))],
        "verificationMethod": [{"id": format!("{did}#atproto"), "type": "Multikey", "controller": did,
                                "publicKeyMultibase": "zQ3shQo6TF2moaqMTrUZEM1jeuYRQXeHEx4evX9751y2qPqRA"}],
        "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": pds}],
    })
}

fn live() -> Value {
    json!({"plc": {"phase": "live", "fresh": true, "cursor": 1}})
}

/// Both fakes: lodestar's answers by mode, and a PLC directory that
/// always answers.
#[derive(Clone, Default)]
struct World {
    mode: Arc<Mutex<&'static str>>,
    lodestar_calls: Arc<AtomicUsize>,
    plc_calls: Arc<AtomicUsize>,
}

impl World {
    fn set(&self, mode: &'static str) {
        *self.mode.lock().expect("mode") = mode;
    }
    fn mode(&self) -> &'static str {
        self.lodestar_calls.fetch_add(1, Ordering::SeqCst);
        *self.mode.lock().expect("mode")
    }
    fn lodestar_calls(&self) -> usize {
        self.lodestar_calls.load(Ordering::SeqCst)
    }
    fn plc_calls(&self) -> usize {
        self.plc_calls.load(Ordering::SeqCst)
    }
}

async fn l_document(State(w): State<World>, Path(did): Path<String>) -> (StatusCode, String) {
    match w.mode() {
        "unregistered" => (StatusCode::NOT_FOUND, json!({"message": format!("DID not registered: {did}")}).to_string()),
        "tombstone" => (StatusCode::NOT_FOUND, json!({"message": format!("DID not available: {did}")}).to_string()),
        "forbidden" => (StatusCode::FORBIDDEN, String::new()),
        _ => (StatusCode::OK, doc(&did, LODESTAR_PDS).to_string()),
    }
}

async fn l_account(State(w): State<World>, Path(did): Path<String>) -> (StatusCode, String) {
    let handle = match w.mode() {
        "mismatch" => None,
        "other_handle" => Some("someone.example.com"),
        "forbidden" => return (StatusCode::FORBIDDEN, String::new()),
        _ => Some(claim(&did)),
    };
    let mut account = json!({"did": did, "status": "active", "pds": LODESTAR_PDS, "claimed_handle": claim(&did),
                             "handle_status": if handle.is_some() { "verified" } else { "mismatch" }, "source": live()});
    if let Some(h) = handle {
        account["handle"] = json!(h);
    }
    (StatusCode::OK, account.to_string())
}

async fn l_handle(State(w): State<World>, Path(handle): Path<String>) -> (StatusCode, String) {
    match w.mode() {
        "moved" => (StatusCode::OK, json!({"handle": handle, "did": MOVED}).to_string()),
        "missing" => (StatusCode::NOT_FOUND, json!({"error": "not_found"}).to_string()),
        _ => (StatusCode::OK, json!({"handle": handle, "did": ALICE}).to_string()),
    }
}

async fn l_accounts(State(w): State<World>, axum::Json(body): axum::Json<Value>) -> (StatusCode, String) {
    w.mode();
    let mut out = serde_json::Map::new();
    for did in body["dids"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        out.insert(did.to_string(), json!({"did": did, "status": "active", "pds": LODESTAR_PDS, "handle": claim(did),
                                           "claimed_handle": claim(did), "handle_status": "verified"}));
    }
    (StatusCode::OK, json!({"accounts": out, "source": live()}).to_string())
}

async fn plc(State(w): State<World>, Path(did): Path<String>) -> axum::Json<Value> {
    w.plc_calls.fetch_add(1, Ordering::SeqCst);
    axum::Json(doc(&did, PLC_PDS))
}

struct NeverPersist;

impl PersistencePolicy for NeverPersist {
    fn in_persistence_set(&self, _did: &str) -> bool {
        false
    }
}

struct AlwaysPersist;

impl PersistencePolicy for AlwaysPersist {
    fn in_persistence_set(&self, _did: &str) -> bool {
        true
    }
}

/// A handle backend that counts its probes and names `NEW_HOLDER`.
#[derive(Default)]
struct CountingProbe(AtomicUsize);

#[async_trait]
impl HandleLookup for CountingProbe {
    async fn resolve_handle(&self, _transport: &Transport, _handle: &str) -> Option<String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(NEW_HOLDER.to_string())
    }
}

struct Setup {
    world: World,
    lodestar: Arc<Lodestar>,
    resolver: IdentityResolver,
    probes: Arc<CountingProbe>,
}

async fn setup(pool: PgPool, policy: Arc<dyn PersistencePolicy>, nets: Transports) -> Setup {
    let world = World::default();
    let lodestar_base = crate::test_support::serve_fake(
        axum::Router::new()
            .route("/account/{did}", get(l_account))
            .route("/accounts", post(l_accounts))
            .route("/handle/{handle}", get(l_handle))
            .route("/{did}", get(l_document))
            .with_state(world.clone()),
    )
    .await;
    let plc_base = crate::test_support::serve_fake(axum::Router::new().route("/{did}", get(plc)).with_state(world.clone())).await;
    let lodestar = Arc::new(Lodestar::new(nets.public().client().clone(), &lodestar_base, Duration::from_millis(500), Duration::from_secs(3600)));
    lodestar.gate().good_at(Instant::now());
    let settings = IdentitySettings {
        plc_url: plc_base,
        fresh: Duration::from_secs(3600),
        stale_max: Duration::from_secs(86_400),
    };
    let probes = Arc::new(CountingProbe::default());
    let resolver = IdentityResolver::with_handles(nets, pool, settings, policy, Arc::clone(&probes) as Arc<dyn HandleLookup>, Intent::Public)
        .with_lodestar(Arc::clone(&lodestar));
    Setup { world, lodestar, resolver, probes }
}

/// A fresh database, or the test passes without running.
macro_rules! db {
    ($name:expr) => {
        match crate::test_support::fresh_db($name).await {
            Some(db) => db,
            None => return,
        }
    };
}

fn did(c: char) -> String {
    format!("did:plc:{}", c.to_string().repeat(24))
}

/// T-L3: lodestar first; a document it answers is used and kept; 404
/// `DID not registered` asks the PLC directory and leaves the gate open; a
/// 403 asks the PLC directory and closes the gate, after which lodestar is
/// not asked; a lodestar tombstone is final.
#[tokio::test]
async fn reads_ask_lodestar_first_and_fall_back_to_the_directory() {
    let db = db!("lodestar_reads_first");
    let s = setup(db.pool.clone(), Arc::new(NeverPersist), Transports::loopback_for_tests()).await;
    let pds = |r: Result<super::DidDocInfo, IdentityError>| r.expect("resolved").pds_endpoint;
    s.world.set("ok");
    assert_eq!(pds(s.resolver.resolve_info(&did('b'), ResolveIntent::Lookup).await).as_deref(), Some(LODESTAR_PDS));
    assert_eq!((s.world.lodestar_calls(), s.world.plc_calls()), (1, 0));
    // Kept in lodestar's cache: no call at all.
    assert_eq!(pds(s.resolver.resolve_info(&did('b'), ResolveIntent::Lookup).await).as_deref(), Some(LODESTAR_PDS));
    assert_eq!((s.world.lodestar_calls(), s.world.plc_calls()), (1, 0));
    s.world.set("unregistered");
    assert_eq!(pds(s.resolver.resolve_info(&did('c'), ResolveIntent::Lookup).await).as_deref(), Some(PLC_PDS));
    assert!(s.lodestar.gate().is_open(), "a DID created seconds ago is about that DID");
    s.world.set("tombstone");
    assert!(matches!(s.resolver.resolve_info(&did('d'), ResolveIntent::Lookup).await, Err(IdentityError::Tombstoned(_))));
    assert_eq!(s.world.plc_calls(), 1, "a lodestar tombstone is final");
    s.world.set("forbidden");
    assert_eq!(pds(s.resolver.resolve_info(&did('e'), ResolveIntent::Lookup).await).as_deref(), Some(PLC_PDS));
    assert!(!s.lodestar.gate().is_open());
    s.world.set("ok");
    let before = s.world.lodestar_calls();
    assert_eq!(pds(s.resolver.resolve_info(&did('f'), ResolveIntent::Lookup).await).as_deref(), Some(PLC_PDS));
    assert_eq!(s.world.lodestar_calls(), before, "a closed gate asks nothing");
    db.drop_db().await;
}

/// T-L10: views of any other intent never ask lodestar.
#[tokio::test]
async fn only_public_views_ask_lodestar() {
    let db = db!("lodestar_public_only");
    let s = setup(db.pool.clone(), Arc::new(NeverPersist), Transports::loopback_for_tests()).await;
    s.world.set("ok");
    for (n, intent) in [(0, Intent::Internal), (1, Intent::ViewerPrivate)] {
        let view = s.resolver.with_intent(intent);
        let d = did(['g', 'h'][n]);
        assert_eq!(view.resolve_info(&d, ResolveIntent::Lookup).await.expect("resolved").pds_endpoint.as_deref(), Some(PLC_PDS));
        assert_eq!(view.verify_handle(&d, Some("alice.example.com")).await, None, "the probe names another DID");
    }
    assert_eq!(s.world.lodestar_calls(), 0);
    // The read methods of the key path and admin never ask it, whatever the view.
    s.resolver.fetch_document(&did('i')).await.expect("plc");
    s.resolver.resolve_fresh(&did('j')).await.expect("plc");
    assert_eq!(s.world.lodestar_calls(), 0);
    db.drop_db().await;
}

/// T-L5: the JWT path, built from a view that carries lodestar, reads the
/// PLC directory only, even for a DID lodestar just answered.
#[tokio::test]
async fn the_key_path_never_reads_lodestar() {
    let db = db!("lodestar_key_path");
    let s = setup(db.pool.clone(), Arc::new(NeverPersist), Transports::loopback_for_tests()).await;
    s.world.set("ok");
    let d = did('k');
    s.resolver.resolve_info(&d, ResolveIntent::Lookup).await.expect("lodestar");
    assert_eq!(s.world.lodestar_calls(), 1);
    assert!(s.resolver.cache().lookup(&d).await.expect("db").is_none(), "no lodestar document in did_cache or its memory layer");
    let fetcher = GuardedDidFetcher::new(s.resolver.clone(), JwtFetchCell::new());
    let raw = fetcher.fetch(&d, Instant::now() + Duration::from_secs(5)).await.expect("plc");
    let fetched: Value = serde_json::from_slice(&raw.bytes).expect("json");
    assert_eq!(fetched["service"][0]["serviceEndpoint"], PLC_PDS);
    assert_eq!((s.world.lodestar_calls(), s.world.plc_calls()), (1, 1));
    db.drop_db().await;
}

/// T-L4: a persistence-set DID read through lodestar leaves no `did_cache`
/// row; a fresh directory row is served before lodestar's cache.
#[tokio::test]
async fn lodestar_documents_never_reach_did_cache() {
    let db = db!("lodestar_did_cache");
    let s = setup(db.pool.clone(), Arc::new(AlwaysPersist), Transports::loopback_for_tests()).await;
    s.world.set("ok");
    let d = did('l');
    s.resolver.resolve_info(&d, ResolveIntent::Lookup).await.expect("lodestar");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM did_cache").fetch_one(&db.pool).await.expect("count");
    assert_eq!(rows, 0);
    // The refresher's kind of fetch stores a directory row; reads take it first.
    s.resolver.refresh_now(&d, ResolveIntent::AdmittedAuthor).await.expect("plc");
    let info = s.resolver.resolve_info(&d, ResolveIntent::Lookup).await.expect("row");
    assert_eq!(info.pds_endpoint.as_deref(), Some(PLC_PDS));
    assert_eq!(s.world.lodestar_calls(), 1);
    db.drop_db().await;
}

/// T-L7: lodestar's verified handle saves the probe only when it equals
/// the document's claim; any other answer runs the probe.
#[tokio::test]
async fn a_verified_handle_saves_the_probe_and_nothing_else_does() {
    let db = db!("lodestar_verified_handle");
    let s = setup(db.pool.clone(), Arc::new(NeverPersist), Transports::loopback_for_tests()).await;
    let probes = || s.probes.0.load(Ordering::SeqCst);
    s.world.set("ok");
    assert_eq!(s.resolver.verify_handle(ALICE, Some("Alice.Example.com")).await.as_deref(), Some("alice.example.com"));
    assert_eq!(probes(), 0);
    for mode in ["mismatch", "other_handle", "forbidden"] {
        s.lodestar.gate().good_at(Instant::now());
        s.world.set(mode);
        let before = probes();
        // The probe names another DID, so the handle is not verified.
        assert_eq!(s.resolver.verify_handle(ALICE, Some("alice.example.com")).await, None, "{mode}");
        assert_eq!(probes(), before + 1, "{mode}");
    }
    db.drop_db().await;
}

/// T-L7, handle → DID: lodestar's DID is kept when its document claims
/// the handle; when it does not (lodestar's lag), the probe's answer is
/// the answer (review 2, LB1); a lodestar 404 runs the probe.
#[tokio::test]
async fn a_handle_lodestar_holds_is_checked_against_the_document() {
    let db = db!("lodestar_handle_document");
    let s = setup(db.pool.clone(), Arc::new(NeverPersist), Transports::loopback_for_tests()).await;
    let lookup = RequestLookup::new(s.resolver.clone(), RequesterLimit::new("identity", 1000.0, 1000, 1000), Shares::for_tests().bucket("plc", 1000.0, 1000))
        .with_lodestar(Arc::clone(&s.lodestar), Shares::for_tests().bucket("lodestar", 1000.0, 1000));
    let req = Requester::test();
    s.world.set("ok");
    assert_eq!(lookup.did_for_handle(&req, "alice.example.com").await.as_deref(), Some(ALICE));
    assert_eq!(s.probes.0.load(Ordering::SeqCst), 0);
    s.world.set("moved");
    assert_eq!(lookup.did_for_handle(&req, "alice.example.com").await.as_deref(), Some(NEW_HOLDER));
    s.world.set("missing");
    assert_eq!(lookup.did_for_handle(&req, "alice.example.com").await.as_deref(), Some(NEW_HOLDER));
    assert_eq!(s.probes.0.load(Ordering::SeqCst), 2);
    db.drop_db().await;
}

/// T-L7, `resolveHandle`'s typed lookup: the same, with the probe's own
/// TXT answer.
#[tokio::test]
async fn the_typed_handle_lookup_falls_back_when_the_document_disagrees() {
    struct Txt;
    #[async_trait]
    impl crate::net::TxtSource for Txt {
        async fn txt(&self, _name: &str) -> crate::net::resolver::TxtAnswer {
            crate::net::resolver::TxtAnswer::Records(vec![format!("did={NEW_HOLDER}")])
        }
    }
    let db = db!("lodestar_typed_handle");
    let nets = Transports::loopback_for_tests().with_dns_for_tests(Intent::Public, Arc::new(Txt));
    let s = setup(db.pool.clone(), Arc::new(NeverPersist), nets).await;
    let lookup = RequestLookup::new(s.resolver.clone(), RequesterLimit::new("identity", 1000.0, 1000, 1000), Shares::for_tests().bucket("plc", 1000.0, 1000))
        .with_handle_lookup(HandleCacheSettings { capacity: 100, ttl: Duration::from_secs(300), negative_ttl: Duration::from_secs(60) })
        .with_lodestar(Arc::clone(&s.lodestar), Shares::for_tests().bucket("lodestar", 1000.0, 1000));
    let req = Requester::test();
    // Lodestar still names MOVED, whose document no longer claims the
    // handle: the TXT record's DID, which does, is the answer.
    s.world.set("moved");
    assert_eq!(lookup.handle_outcome(&req, "alice.example.com").await, HandleOutcome::Found(NEW_HOLDER.to_string()));
    // Lodestar's DID whose document claims the handle back.
    s.world.set("ok");
    assert_eq!(lookup.handle_outcome(&req, "carol.example.com").await, HandleOutcome::NotFound, "ALICE does not claim carol; nor does NEW_HOLDER");
    s.world.set("ok");
    let typed = s.resolver.resolve_handle_typed("alice.example.com", &admit_all, super::resolver::Caches::Shared).await;
    assert_eq!(typed, HandleOutcome::Found(ALICE.to_string()));
    db.drop_db().await;
}

/// T-L8: an empty lodestar bucket skips lodestar without spending the
/// request; the read reaches the PLC directory, which draws its own bucket.
#[tokio::test]
async fn an_empty_lodestar_bucket_skips_lodestar_quietly() {
    let db = db!("lodestar_empty_bucket");
    let s = setup(db.pool.clone(), Arc::new(NeverPersist), Transports::loopback_for_tests()).await;
    // Burst 2: one token in the non-member pool.
    let lookup = RequestLookup::new(s.resolver.clone(), RequesterLimit::new("identity", 1000.0, 1000, 1000), Shares::for_tests().bucket("plc", 1000.0, 1000))
        .with_lodestar(Arc::clone(&s.lodestar), Shares::for_tests().bucket("lodestar", 0.001, 2));
    let req = Requester::test();
    s.world.set("ok");
    let first = lookup.summary(&req, &did('p')).await.expect("summary");
    assert_eq!(first.pds.as_deref(), Some(LODESTAR_PDS));
    let second = lookup.summary(&req, &did('q')).await.expect("summary");
    assert_eq!(second.pds.as_deref(), Some(PLC_PDS));
    assert!(!req.spent(), "lodestar's empty bucket is not an exhaustion");
    assert!(s.world.plc_calls() >= 1);
    db.drop_db().await;
}

/// `admit_all` admits everything (background work).
#[test]
fn admit_all_admits() {
    assert!(admit_all(super::resolver::Upstream::Lodestar, "x") && admit_all(super::resolver::Upstream::Plc, "x"));
}
