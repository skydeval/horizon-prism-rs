//! Lodestar batches on the request path (lodestar-reads design §2.10,
//! §6.1 T-L9).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::routing::{get, post};
use serde_json::{Value, json};

use super::lodestar::Lodestar;
use super::{IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::constellation::routing::{DidLookup, RequestLookup};
use crate::net::{Intent, Transports};
use crate::xrpc::pools::{Requester, RequesterLimit, Shares};

const PDS: &str = "https://lodestar-pds.example.com";

fn did(n: usize) -> String {
    // 24 base32 characters: a fixed prefix and the number in letters.
    let tail: String = format!("{n:04}").chars().map(|c| char::from(b'a' + (c as u8 - b'0'))).collect();
    format!("did:plc:{}{tail}", "q".repeat(20))
}

/// What lodestar reports for each DID, by its last letter.
fn entry(d: &str) -> Value {
    match d.chars().last() {
        Some('b') => json!({"error": "not_found"}),
        Some('c') => json!({"did": d, "status": "tombstoned"}),
        Some('d') => json!({"did": d, "status": "active", "pds": PDS, "claimed_handle": "x.example.com", "handle_status": "pending"}),
        Some('e') => json!({"did": d, "status": "active", "pds": "not a url", "handle_status": "none"}),
        Some('f') => json!({"did": d, "status": "active", "pds": PDS, "handle_status": "none"}),
        _ => json!({"did": d, "status": "active", "pds": format!("{PDS}/"), "handle": "Alice.Example.com",
                    "claimed_handle": "alice.example.com", "handle_status": "verified"}),
    }
}

#[derive(Clone, Default)]
struct World {
    batches: Arc<Mutex<Vec<usize>>>,
    stale: Arc<Mutex<bool>>,
    singles: Arc<AtomicUsize>,
    plc: Arc<AtomicUsize>,
}

async fn accounts(State(w): State<World>, axum::Json(body): axum::Json<Value>) -> String {
    let dids: Vec<String> = serde_json::from_value(body["dids"].clone()).expect("dids");
    w.batches.lock().expect("batches").push(dids.len());
    let fresh = !*w.stale.lock().expect("stale");
    let out: serde_json::Map<String, Value> = dids.iter().map(|d| (d.clone(), entry(d))).collect();
    json!({"accounts": out, "source": {"plc": {"phase": "live", "fresh": fresh}}}).to_string()
}

async fn single(State(w): State<World>, Path(_rest): Path<String>) -> (axum::http::StatusCode, String) {
    w.singles.fetch_add(1, Ordering::SeqCst);
    (axum::http::StatusCode::FORBIDDEN, String::new())
}

async fn plc(State(w): State<World>, Path(d): Path<String>) -> axum::Json<Value> {
    w.plc.fetch_add(1, Ordering::SeqCst);
    axum::Json(json!({"id": d, "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://plc-pds.example.com"}]}))
}

struct NeverPersist;

impl PersistencePolicy for NeverPersist {
    fn in_persistence_set(&self, _did: &str) -> bool {
        false
    }
}

/// T-L9: chunks of at most the batch limit; filled entries need no further
/// call; what a batch cannot fill goes the per-DID path; a batch never
/// touches the identity share or marks the request spent (review 1, LB1,
/// LB2); a stale batch fills nothing and closes the gate; did:web is
/// never sent.
#[tokio::test]
async fn a_batch_fills_what_it_can_and_spends_nothing_of_the_share() {
    let Some(db) = crate::test_support::fresh_db("lodestar_batch").await else {
        return;
    };
    let w = World::default();
    let l_base = crate::test_support::serve_fake(
        axum::Router::new().route("/accounts", post(accounts)).route("/{*rest}", get(single)).with_state(w.clone()),
    )
    .await;
    let plc_base = crate::test_support::serve_fake(axum::Router::new().route("/{did}", get(plc)).with_state(w.clone())).await;
    let nets = Transports::loopback_for_tests();
    let lodestar = Arc::new(
        Lodestar::new(nets.public().client().clone(), &l_base, Duration::from_millis(500), Duration::from_secs(3600)).with_batch_max(3),
    );
    lodestar.gate().good_at(Instant::now());
    let settings = IdentitySettings { plc_url: plc_base, fresh: Duration::from_secs(3600), stale_max: Duration::from_secs(86_400) };
    let resolver = IdentityResolver::new(nets, db.pool.clone(), settings, Arc::new(NeverPersist), Intent::Public);
    // An identity share of burst 2: less than the batch's DIDs.
    let lookup = RequestLookup::new(resolver, RequesterLimit::new("identity", 0.001, 2, 1000), Shares::for_tests().bucket("plc", 1000.0, 1000))
        .with_lodestar(Arc::clone(&lodestar), Shares::for_tests().bucket("lodestar", 1000.0, 1000));
    let req = Requester::test();

    // did(0) ends in 'a' (filled, verified handle); 1 'b' not_found; 2 'c'
    // tombstoned; 3 'd' pending; 4 'e' bad PDS; 5 'f' no claim (filled).
    let mut dids: Vec<String> = (0..6).map(did).collect();
    dids.push("did:web:example.com".to_string());
    dids.push(did(0));
    lookup.prefetch(&req, &dids).await;
    assert_eq!(*w.batches.lock().expect("batches"), vec![3, 3], "six did:plc in chunks of three; did:web and the repeat not sent");
    assert!(!req.spent(), "a batch never spends the request");

    // Filled: served with no call and no share token (the share has two).
    let a = lookup.summary(&req, &did(0)).await.expect("filled");
    assert_eq!((a.pds.as_deref(), a.handle.as_deref()), (Some(PDS), Some("alice.example.com")));
    let f = lookup.summary(&req, &did(5)).await.expect("filled");
    assert_eq!((f.pds.as_deref(), f.handle), (Some(PDS), None));
    assert_eq!((w.singles.load(Ordering::SeqCst), w.plc.load(Ordering::SeqCst)), (0, 0));
    // Not filled: the per-DID path, which draws the share as before.
    lookup.summary(&req, &did(1)).await.expect("per-DID");
    lookup.summary(&req, &did(3)).await.expect("per-DID");
    assert!(w.plc.load(Ordering::SeqCst) >= 2, "lodestar's single reads answer 403 here; the PLC directory answers");
    assert!(lookup.summary(&req, &did(4)).await.is_none(), "the share's burst of two is spent by the per-DID path alone");

    // A stale batch fills nothing and closes the gate.
    lodestar.gate().good_at(Instant::now());
    *w.stale.lock().expect("stale") = true;
    let other = Requester::of(None, "198.51.100.20".parse().expect("ip"));
    lookup.prefetch(&other, &[did(10), did(11)]).await;
    assert!(!lodestar.gate().is_open());
    let before = w.batches.lock().expect("batches").len();
    lookup.prefetch(&other, &[did(12)]).await;
    assert_eq!(w.batches.lock().expect("batches").len(), before, "a closed gate sends no batch");
    db.drop_db().await;
}

/// A viewer-private request never sends a batch.
#[tokio::test]
async fn a_viewer_private_request_sends_no_batch() {
    let Some(db) = crate::test_support::fresh_db("lodestar_batch_private").await else {
        return;
    };
    let w = World::default();
    let l_base = crate::test_support::serve_fake(axum::Router::new().route("/accounts", post(accounts)).with_state(w.clone())).await;
    let nets = Transports::loopback_for_tests();
    let lodestar = Arc::new(Lodestar::new(nets.public().client().clone(), &l_base, Duration::from_millis(500), Duration::from_secs(3600)));
    lodestar.gate().good_at(Instant::now());
    let settings = IdentitySettings { plc_url: "https://plc.invalid".to_string(), fresh: Duration::from_secs(3600), stale_max: Duration::from_secs(86_400) };
    let resolver = IdentityResolver::new(nets, db.pool.clone(), settings, Arc::new(NeverPersist), Intent::Public);
    let lookup = RequestLookup::new(resolver, RequesterLimit::new("identity", 1000.0, 1000, 1000), Shares::for_tests().bucket("plc", 1000.0, 1000))
        .with_lodestar(lodestar, Shares::for_tests().bucket("lodestar", 1000.0, 1000));
    let private = Requester::test().viewer_private("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");
    lookup.prefetch(&private, &[did(0)]).await;
    assert!(w.batches.lock().expect("batches").is_empty());
    db.drop_db().await;
}
