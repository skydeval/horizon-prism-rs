//! Thread Lens end to end: viewer-relative threads, `tier-hidden` without
//! lookups, and the refusals of a member endpoint.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router as AxumRouter;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;

use super::handlers::{GET_THREAD, RESOLVE, ThreadLensState, router};
use super::ladder::{Ladder, Mode, Resolved};
use super::{Gap, thread};
use crate::aggregations::ScopeSets;
use crate::appview_clients::{AppViewAuth, AppViewClient};
use crate::config::ConstellationConfig;
use crate::constellation::routing::tests::StubLookup;
use crate::constellation::{Cascade, DidLookup, Router as DidRouter, RoutingTable};
use crate::config::ThreadLensConfig;
use crate::hydration::lens::{Bridging, LensNode};
use crate::hydration::testing::{actor, exec, hydrator, hydrator_with_farsight, post};
use crate::hydration::{Hydrator, Viewer};
use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
use crate::microcosm::SlingshotClient;
use crate::net::NetClient;
use crate::test_support::{fresh_db, serve_fake};
use crate::xrpc::pools::Requester;
use crate::xrpc::{AppState, RateLimits};

const AUTHOR: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const R1: &str = "did:plc:rrrrrrrrrrrrrrrrrrrrrrr1";
const R2: &str = "did:plc:rrrrrrrrrrrrrrrrrrrrrrr2";
const VIEWER_A: &str = crate::farsight::lookup::tests::V;
const VIEWER_B: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvb";
const STRANGER: &str = "did:plc:ssssssssssssssssssssssss";
const PRIVATE: &str = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kpriv";

/// Counters for every outbound source.
#[derive(Default)]
struct Calls {
    appview: AtomicUsize,
    slingshot: AtomicUsize,
}

/// A fake upstream: an AppView answering `getPostThread` with `thread`
/// (or NotFound) and `getPosts` with NotFound, plus a Slingshot answering
/// NotFound.
async fn fake_upstream(calls: Arc<Calls>, thread: Option<Value>) -> String {
    let state = (calls, thread);
    let app = AxumRouter::new()
        .route(
            "/xrpc/{method}",
            get(
                |State((calls, thread)): State<(Arc<Calls>, Option<Value>)>,
                 Path(method): Path<String>,
                 Query(_q): Query<HashMap<String, String>>| async move {
                    if method == "com.atproto.repo.getRecord" {
                        calls.slingshot.fetch_add(1, Ordering::SeqCst);
                        return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "RecordNotFound"}))).into_response();
                    }
                    calls.appview.fetch_add(1, Ordering::SeqCst);
                    match (method.as_str(), thread) {
                        ("app.bsky.feed.getPostThread", Some(t)) => axum::Json(json!({"thread": t})).into_response(),
                        _ => (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "NotFound", "message": "Post not found"})))
                            .into_response(),
                    }
                },
            ),
        )
        .with_state(state);
    serve_fake(app).await
}

fn ladder(hydrator: Hydrator, base: &str, lookup: Arc<StubLookup>) -> Ladder {
    let mut cfg = ConstellationConfig::default();
    for n in &mut cfg.networks {
        if n.name == "bluesky" || n.name == "community-a" {
            n.appview_url = base.to_string();
        }
    }
    let table = Arc::new(RoutingTable::from_config(&cfg));
    let net = NetClient::loopback_for_tests();
    let clients = table
        .networks()
        .iter()
        .filter(|n| n.callable())
        .map(|n| AppViewClient::new(&n.name, base, AppViewAuth::Unauth, net.clone()))
        .collect();
    Ladder::new(
        hydrator,
        DidRouter::new(Arc::clone(&table), lookup as Arc<dyn DidLookup>),
        Cascade::new(table, clients, &crate::xrpc::pools::Shares::for_tests()),
        SlingshotClient::new(net, base),
    )
}

fn member(did: &str) -> Viewer {
    Viewer {
        did: Some(did.to_string()),
        hydrate: true,
    }
}

#[tokio::test]
async fn tier_hidden_is_emitted_without_any_lookup() {
    // A pool that cannot connect: any database call would be an error.
    let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), None).await;
    let lookup = Arc::new(StubLookup::default());
    let l = ladder(hydrator(&pool, ScopeSets::default()), &base, Arc::clone(&lookup));
    let (req, viewer) = (Requester::test(), member(VIEWER_A));
    for mode in [Mode::Full, Mode::BlockedStub] {
        assert_eq!(l.resolve(&viewer, &req, PRIVATE, mode).await.expect("no database call"), Resolved::Gap(Gap::TierHidden));
    }
    let request = thread::Request { viewer: &viewer, req: &req, anchor: PRIVATE, depth: 6, parent_height: 80 };
    let got = thread::get_thread(&l, &ThreadLensConfig::default(), request).await.expect("no database call");
    assert_eq!(got.nodes.len(), 1);
    assert!(matches!(&got.nodes[0], LensNode::Gap { gap: Gap::TierHidden, .. }));
    // Decided by the URI's form: no round ran, so bridging reads "off".
    assert_eq!(got.bridging, Bridging::Off);
    let resolved = thread::resolve(&l, &ThreadLensConfig::default(), &viewer, &req, &[PRIVATE.to_string()]).await.expect("no database call");
    assert!(matches!(&resolved.nodes[0], LensNode::Gap { gap: Gap::TierHidden, .. }));
    assert_eq!(calls.appview.load(Ordering::SeqCst), 0, "no AppView call");
    assert_eq!(calls.slingshot.load(Ordering::SeqCst), 0, "no Slingshot call");
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 0, "no DID resolution");
}

async fn serve(state: ThreadLensState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>())
            .await
            .expect("serve");
    });
    format!("http://{addr}")
}

fn state(h: Hydrator, l: Ladder) -> ThreadLensState {
    let fetcher = StubFetcher::default();
    fetcher.set_key(VIEWER_A, signing_key(3));
    fetcher.set_key(VIEWER_B, signing_key(4));
    fetcher.set_key(STRANGER, signing_key(5));
    fetcher.set_key(R2, signing_key(6));
    let (auth, _d) = verifier(fetcher, 10, 4);
    ThreadLensState {
        app: AppState {
            hydrator: h,
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
        },
        ladder: l,
        cfg: ThreadLensConfig::default(),
    }
}

async fn call(base: &str, method: &str, caller: &str, key: u8, body: &Value) -> (u16, String) {
    let token = mint(&signing_key(key), caller, &format!("did:web:{AUDIENCE_HOST}"), Some(method));
    let r = reqwest::Client::new()
        .post(format!("{base}/xrpc/{method}"))
        .header("authorization", format!("Bearer {token}"))
        .json(body)
        .send()
        .await
        .expect("request");
    (r.status().as_u16(), r.text().await.expect("body"))
}

/// `(uri, gap kind or None for a post)` of each node.
fn summary(v: &Value, key: &str) -> Vec<(String, Option<String>)> {
    v[key]
        .as_array()
        .expect("nodes")
        .iter()
        .map(|n| (n["uri"].as_str().unwrap_or_default().to_string(), n["gap"].as_str().map(str::to_string)))
        .collect()
}

/// B-82, B-83: a block hides one author's posts and nothing around them;
/// nothing by a blocked author is sent; with Farsight not vouching the
/// response is the ordinary view; only a ready member may call.
#[tokio::test]
async fn threads_are_viewer_relative_and_blocks_are_enforced() {
    let Some(db) = fresh_db("threadlens_viewer_relative").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (R2, "r2.test"), (VIEWER_A, "a.test"), (VIEWER_B, "b.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now()), ('{VIEWER_B}', 'admin', now()), ('{R2}', 'admin', NULL)")).await;
    let anchor = post(p, AUTHOR, "3kanchor2222", "anchor", None, None).await;
    let reply1 = post(p, R1, "3kreply12222", "one", Some(&anchor), Some(&anchor)).await;
    let reply2 = post(p, R2, "3kreply22222", "two", Some(&anchor), Some(&anchor)).await;
    // Beneath the reply of an author viewer A blocks, a reply by someone
    // A has no block with.
    let beneath = post(p, AUTHOR, "3kbeneath222", "beneath", Some(&reply1), Some(&anchor)).await;
    // Viewer A blocks R1; R2 blocks viewer A. Viewer B has no blocks.
    for (i, (author, subject)) in [(VIEWER_A, R1), (R2, VIEWER_A)].into_iter().enumerate() {
        sqlx::query("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, '3kaaaaaaaaaa2', now())")
            .bind(format!("at://{author}/app.bsky.graph.block/3kblock{i}"))
            .bind(author)
            .bind(subject)
            .execute(p)
            .await
            .expect("block");
    }
    exec(p, "INSERT INTO record_tombstones (uri, rev) VALUES ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/3kdeleted', '3kaaaaaaaaaa2')").await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_farsight(p, ScopeSets::default(), farsight);
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), None).await;
    let url = serve(state(h.clone(), ladder(h, &base, Arc::new(StubLookup::default())))).await;

    // Farsight vouches: bridging is on. The ordinary view leaves out both
    // blocked replies and what is beneath them. Thread Lens puts a gap in
    // each one's place and keeps the reply beneath.
    let thread_input = json!({"anchor": anchor, "depth": 6});
    let (status, body) = call(&url, GET_THREAD, VIEWER_A, 3, &thread_input).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(
        summary(&v, "nodes"),
        [
            (anchor.clone(), None),
            (reply1.clone(), Some("blocked-by-you".to_string())),
            (reply2.clone(), Some("blocking-you".to_string())),
            (beneath.clone(), None),
        ],
        "{body}"
    );
    assert_eq!(v["bridging"], "full", "{body}");
    // A gap carries nothing of its author but the URI.
    assert_eq!(v["nodes"][1].as_object().map(|m| m.keys().filter(|k| !k.starts_with("more")).count()), Some(4));
    for leaked in ["\"one\"", "\"two\"", "r1.test", "r2.test"] {
        assert!(!body.contains(leaked), "{leaked} in {body}");
    }
    // The views are the caller's own.
    assert_eq!(v["nodes"][0]["post"]["author"]["viewer"]["blockedBy"], json!(false));

    // Viewer B has no block: every post, as posts.
    let (status, body_b) = call(&url, GET_THREAD, VIEWER_B, 4, &thread_input).await;
    assert_eq!(status, 200, "{body_b}");
    let b: Value = serde_json::from_str(&body_b).expect("json");
    assert!(summary(&b, "nodes").iter().all(|(_, gap)| gap.is_none()), "{body_b}");
    assert_eq!(b["nodes"].as_array().map(Vec::len), Some(4));

    // `resolve`: a post, a private URI, a blocked author, a deleted post,
    // a post nobody has.
    let resolve_input = json!({"uris": [
        anchor,
        PRIVATE,
        reply1,
        "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/3kdeleted",
        format!("at://{STRANGER}/app.bsky.feed.post/3kmissing")
    ]});
    let (status, body) = call(&url, RESOLVE, VIEWER_A, 3, &resolve_input).await;
    assert_eq!(status, 200, "{body}");
    let r: Value = serde_json::from_str(&body).expect("json");
    let gaps: Vec<Value> = r["items"].as_array().expect("items").iter().map(|i| i["gap"].clone()).collect();
    assert_eq!(gaps, [Value::Null, json!("tier-hidden"), json!("blocked-by-you"), json!("deleted"), json!("not-found")], "{body}");

    // Farsight answers without vouching: bridging is off and the response
    // is the ordinary view, which for viewer A is the anchor alone.
    *fake.level.lock().expect("level") = "partial";
    let (_, body) = call(&url, GET_THREAD, VIEWER_A, 3, &thread_input).await;
    let off: Value = serde_json::from_str(&body).expect("json");
    assert_eq!((summary(&off, "nodes"), &off["bridging"]), (vec![(anchor.clone(), None)], &json!("off")), "{body}");
    // `resolve` then fetches nothing it cannot check: a known block is
    // still a gap of its kind, everything else is "block-state-unknown".
    let (_, body) = call(&url, RESOLVE, VIEWER_A, 3, &json!({"uris": [anchor, reply1]})).await;
    let r: Value = serde_json::from_str(&body).expect("json");
    assert_eq!((&r["items"][0]["gap"], &r["items"][1]["gap"], &r["bridging"]), (&json!("block-state-unknown"), &json!("blocked-by-you"), &json!("off")), "{body}");

    // Request-level failures; a member endpoint.
    let (status, body) = call(&url, RESOLVE, VIEWER_A, 3, &json!({"uris": []})).await;
    assert!(status == 400 && body.contains("InvalidRequest"), "{body}");
    let (status, _) = call(&url, GET_THREAD, VIEWER_A, 3, &json!({"anchor": anchor, "depth": 1001})).await;
    assert_eq!(status, 400);
    let (status, body) = call(&url, GET_THREAD, STRANGER, 5, &thread_input).await;
    assert!(status == 400 && body.contains("NotEnrolled"), "{body}");
    let (status, body) = call(&url, GET_THREAD, R2, 6, &thread_input).await;
    assert!(status == 400 && body.contains("NotReady"), "{body}");
    let r = reqwest::Client::new().post(format!("{url}/xrpc/{GET_THREAD}")).json(&thread_input).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 401);
    db.drop_db().await;
}
