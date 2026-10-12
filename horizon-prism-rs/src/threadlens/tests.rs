//! Thread Lens end to end: the block-party rule, `tier-hidden` without
//! lookups, and upstream-structure classification.

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
use crate::hydration::testing::{actor, exec, hydrator, post};
use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
use crate::microcosm::SlingshotClient;
use crate::net::NetClient;
use crate::test_support::{fresh_db, serve_fake};
use crate::xrpc::pools::Requester;
use crate::xrpc::{AppState, RateLimits};

const AUTHOR: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const R1: &str = "did:plc:rrrrrrrrrrrrrrrrrrrrrrr1";
const R2: &str = "did:plc:rrrrrrrrrrrrrrrrrrrrrrr2";
const VIEWER_A: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvva";
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

fn ladder(pool: &PgPool, base: &str, lookup: Arc<StubLookup>) -> Ladder {
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
        hydrator(pool, ScopeSets::default()),
        DidRouter::new(Arc::clone(&table), lookup as Arc<dyn DidLookup>),
        Cascade::new(table, clients),
        SlingshotClient::new(net, base),
    )
}

#[tokio::test]
async fn tier_hidden_is_emitted_without_any_lookup() {
    // A pool that cannot connect: any database call would be an error.
    let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), None).await;
    let lookup = Arc::new(StubLookup::default());
    let l = ladder(&pool, &base, Arc::clone(&lookup));
    let req = Requester::test();
    for mode in [Mode::Full, Mode::BlockedStub] {
        assert_eq!(l.resolve(&req, PRIVATE, mode).await.expect("no database call"), Resolved::Gap(Gap::TierHidden));
    }
    let nodes = thread::get_thread(&l, &req, PRIVATE, 6, 30).await.expect("no database call");
    assert_eq!(nodes.len(), 1);
    assert!(matches!(&nodes[0], crate::hydration::lens::LensNode::Gap { gap: Gap::TierHidden, .. }));
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

fn state(pool: &PgPool, l: Ladder) -> ThreadLensState {
    let fetcher = StubFetcher::default();
    fetcher.set_key(VIEWER_A, signing_key(3));
    fetcher.set_key(VIEWER_B, signing_key(4));
    let (auth, _d) = verifier(fetcher, 10, 4);
    ThreadLensState {
        app: AppState {
            hydrator: hydrator(pool, ScopeSets::default()),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
        },
        ladder: l,
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

#[tokio::test]
async fn responses_are_byte_identical_whatever_the_viewer_blocks() {
    let Some(db) = fresh_db("threadlens_block_party").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (R2, "r2.test"), (VIEWER_A, "a.test"), (VIEWER_B, "b.test")] {
        actor(p, did, handle, handle).await;
    }
    let anchor = post(p, AUTHOR, "3kanchor2222", "anchor", None, None).await;
    let reply1 = post(p, R1, "3kreply12222", "one", Some(&anchor), Some(&anchor)).await;
    let _reply2 = post(p, R2, "3kreply22222", "two", Some(&anchor), Some(&anchor)).await;
    // Viewer A blocks the author and R1; R2 blocks viewer A; viewer B has
    // no blocks. The server must not care.
    for (i, (author, subject)) in [(VIEWER_A, AUTHOR), (VIEWER_A, R1), (R2, VIEWER_A)].into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) \
             VALUES ($1, $2, $3, '3kaaaaaaaaaa2', now())",
        )
        .bind(format!("at://{author}/app.bsky.graph.block/3kblock{i}"))
        .bind(author)
        .bind(subject)
        .execute(p)
        .await
        .expect("block");
    }
    exec(p, "INSERT INTO record_tombstones (uri, rev) VALUES ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/3kdeleted', '3kaaaaaaaaaa2')").await;
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), None).await;
    let url = serve(state(p, ladder(p, &base, Arc::new(StubLookup::default())))).await;

    let thread_input = json!({"anchor": anchor, "depth": 6});
    let (status_a, body_a) = call(&url, GET_THREAD, VIEWER_A, 3, &thread_input).await;
    let (status_b, body_b) = call(&url, GET_THREAD, VIEWER_B, 4, &thread_input).await;
    assert_eq!((status_a, status_b), (200, 200), "{body_a}");
    assert_eq!(body_a, body_b, "byte-identical across viewers");
    let v: Value = serde_json::from_str(&body_a).expect("json");
    let uris: Vec<&str> = v["nodes"].as_array().expect("nodes").iter().filter_map(|n| n["uri"].as_str()).collect();
    assert_eq!(uris[0], anchor);
    assert!(uris.contains(&reply1.as_str()), "a reply by someone the viewer blocks is still served");
    for forbidden in ["blocked-by-you", "blocking-you", "\"blocking\"", "\"blockedBy\"", "\"following\""] {
        assert!(!body_a.contains(forbidden), "{forbidden} in {body_a}");
    }
    assert!(body_a.contains("\"viewer\":{}"));

    let resolve_input = json!({"uris": [
        anchor,
        PRIVATE,
        "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/3kdeleted",
        format!("at://{STRANGER}/app.bsky.feed.post/3kmissing")
    ]});
    let (_, body_a) = call(&url, RESOLVE, VIEWER_A, 3, &resolve_input).await;
    let (_, body_b) = call(&url, RESOLVE, VIEWER_B, 4, &resolve_input).await;
    assert_eq!(body_a, body_b);
    let v: Value = serde_json::from_str(&body_a).expect("json");
    let gaps: Vec<Value> = v["items"].as_array().expect("items").iter().map(|i| i["gap"].clone()).collect();
    assert_eq!(gaps, [Value::Null, json!("tier-hidden"), json!("deleted"), json!("not-found")]);

    // Request-level failures are InvalidRequest; auth is required.
    let (status, body) = call(&url, RESOLVE, VIEWER_A, 3, &json!({"uris": []})).await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("InvalidRequest"));
    let (status, _) = call(&url, GET_THREAD, VIEWER_A, 3, &json!({"anchor": anchor, "depth": 11})).await;
    assert_eq!(status, 400);
    let r = reqwest::Client::new().post(format!("{url}/xrpc/{GET_THREAD}")).json(&thread_input).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 401);
    db.drop_db().await;
}

#[tokio::test]
async fn upstream_structure_stubs_become_typed_gaps() {
    let Some(db) = fresh_db("threadlens_structure").await else {
        return;
    };
    let p = &db.pool;
    let uri = |rkey: &str| format!("at://{STRANGER}/app.bsky.feed.post/{rkey}");
    let view = |rkey: &str, parent: Option<&str>, labels: Value| {
        let mut record = json!({"$type": "app.bsky.feed.post", "text": format!("text {rkey}"), "createdAt": "2026-09-26T00:00:00Z"});
        if let Some(p) = parent {
            record["reply"] = json!({"root": {"uri": uri("3kroot"), "cid": "c"}, "parent": {"uri": p, "cid": "c"}});
        }
        json!({"uri": uri(rkey), "cid": "bafy", "author": {"did": STRANGER, "handle": "s.test", "viewer": {"blockedBy": true, "muted": true}},
               "record": record, "labels": labels, "viewer": {"like": "at://upstream-like"}, "indexedAt": "2026-09-26T00:00:00Z"})
    };
    let structure = json!({
        "$type": "app.bsky.feed.defs#threadViewPost",
        "post": view("3kanchor", Some(&uri("3kroot")), json!([])),
        "parent": {"$type": "app.bsky.feed.defs#threadViewPost", "post": view("3kroot", None, json!([]))},
        "replies": [
            {"$type": "app.bsky.feed.defs#threadViewPost", "post": view("3kok", Some(&uri("3kanchor")), json!([]))},
            {"$type": "app.bsky.feed.defs#threadViewPost", "post": view("3kwarned", Some(&uri("3kanchor")), json!([{"val": "!warn", "src": "did:web:x", "uri": uri("3kwarned"), "cts": "c"}]))},
            {"$type": "app.bsky.feed.defs#blockedPost", "uri": uri("3kblocked"), "blocked": true, "author": {"did": R1}},
            {"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri("3kgone"), "notFound": true}
        ]
    });
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), Some(structure)).await;
    let l = ladder(p, &base, Arc::new(StubLookup::default()));
    let nodes = thread::get_thread(&l, &Requester::test(), &uri("3kanchor"), 6, 30).await.expect("thread");
    let summary: Vec<(String, Option<&'static str>)> = nodes
        .iter()
        .map(|n| match n {
            crate::hydration::lens::LensNode::Post { uri, .. } => (uri.clone(), None),
            crate::hydration::lens::LensNode::Gap { uri, gap, .. } => (uri.clone(), Some(gap.label())),
        })
        .collect();
    assert_eq!(
        summary,
        [
            (uri("3kroot"), None),
            (uri("3kanchor"), None),
            (uri("3kok"), None),
            (uri("3kwarned"), Some("content-stripped")),
            (uri("3kblocked"), Some("blocked")),
            (uri("3kgone"), Some("not-found")),
        ]
    );
    let out = crate::hydration::lens::lens_thread(&uri("3kanchor"), &nodes);
    let text = serde_json::to_string(out.inner()).expect("json");
    assert!(!text.contains("blockedBy") && !text.contains("upstream-like"), "{text}");
    // The blocked stub skipped the cascade (steps 1, 2, 4): Slingshot was
    // asked for it and for the not-found stub.
    assert_eq!(calls.slingshot.load(Ordering::SeqCst), 2);
    db.drop_db().await;
}
