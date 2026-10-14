//! Thread Lens end to end: viewer-relative threads, `tier-hidden` without
//! lookups, and who is served under each `threadlens.access`.

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

use super::cursor::CursorKey;
use super::handlers::{GET_THREAD, RESOLVE, ThreadLensState, router};
use super::ladder::{Ladder, Mode, Resolved};
use super::{Gap, thread};
use crate::aggregations::ScopeSets;
use crate::appview_clients::{AppViewAuth, AppViewClient};
use crate::config::ConstellationConfig;
use crate::constellation::routing::tests::StubLookup;
use crate::constellation::{Cascade, DidLookup, Router as DidRouter, RoutingTable};
use crate::config::{ThreadLensAccess, ThreadLensConfig};
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
    let got = thread::get_thread(&l, &ThreadLensConfig::default(), &CursorKey::generated(), request, None).await.expect("no database call");
    assert_eq!(got.nodes.len(), 1);
    assert!(matches!(&got.nodes[0], LensNode::Gap { gap: Gap::TierHidden, .. }));
    // Decided by the URI's form: no round ran, so bridging reads "off".
    assert_eq!(got.bridging, Bridging::Off);
    let resolved = thread::resolve(&l, &ThreadLensConfig::default(), &CursorKey::generated(), &viewer, &req, &[PRIVATE.to_string()]).await.expect("no database call");
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

fn state(h: Hydrator, l: Ladder, access: ThreadLensAccess) -> ThreadLensState {
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
        cfg: ThreadLensConfig { access, ..ThreadLensConfig::default() },
        cursor_key: CursorKey::new(&[9; 32]),
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
/// response is the ordinary view. With `access = "members"` only a ready
/// member may call. With `access = "signed_in"` every signed-in account
/// may, and one that is not a ready member is served as a visitor.
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
    // The stranger, who is no member, has the same two: rows this AppView
    // holds for it.
    for (i, (author, subject)) in [(VIEWER_A, R1), (R2, VIEWER_A), (STRANGER, R1), (R2, STRANGER)].into_iter().enumerate() {
        sqlx::query("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, '3kaaaaaaaaaa2', now())")
            .bind(format!("at://{author}/app.bsky.graph.block/3kblock{i}"))
            .bind(author)
            .bind(subject)
            .execute(p)
            .await
            .expect("block");
    }
    exec(p, "INSERT INTO record_tombstones (uri, rev) VALUES ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/3kdeleted', '3kaaaaaaaaaa2')").await;
    // A ready member and a member that is not ready both follow the
    // anchor's author: viewer state only a ready member is shown.
    for follower in [VIEWER_B, R2] {
        exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{follower}/app.bsky.graph.follow/3kf', '{follower}', '{AUTHOR}', '3k', now())")).await;
    }
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_farsight(p, ScopeSets::default(), farsight);
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), None).await;
    // Two servers over the same state: one for members only, as before
    // the setting existed, and one for every signed-in account.
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let url = serve(state(h.clone(), l.clone(), ThreadLensAccess::Members)).await;
    let open = serve(state(h, l, ThreadLensAccess::SignedIn)).await;

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
    assert!(b["nodes"][0]["post"]["author"]["viewer"]["following"].is_string(), "{body_b}");

    // `access = "signed_in"`: the stranger, who is no member, is served
    // as a visitor. Its verdicts are a member's: the author it blocks is
    // `blocked-by-you`, the one that blocks it `blocking-you`, and with
    // Farsight vouching everyone else is clear, so the reply beneath is
    // bridged in.
    let (status, body_s) = call(&open, GET_THREAD, STRANGER, 5, &thread_input).await;
    assert_eq!(status, 200, "{body_s}");
    let s: Value = serde_json::from_str(&body_s).expect("json");
    assert_eq!(
        summary(&s, "nodes"),
        [
            (anchor.clone(), None),
            (reply1.clone(), Some("blocked-by-you".to_string())),
            (reply2.clone(), Some("blocking-you".to_string())),
            (beneath.clone(), None),
        ],
        "{body_s}"
    );
    assert_eq!(s["bridging"], "full", "{body_s}");
    // Nothing by a blocked author is sent to a visitor either.
    for leaked in ["\"one\"", "\"two\"", "r1.test", "r2.test"] {
        assert!(!body_s.contains(leaked), "{leaked} in {body_s}");
    }
    // A visitor's views carry `blockedBy` and `muted: false`, and no
    // other viewer state: no `blocking`, though it blocks an author.
    assert_eq!(s["nodes"][0]["post"]["author"]["viewer"], json!({"muted": false, "blockedBy": false}), "{body_s}");
    for field in ["\"blocking\"", "\"following\"", "\"like\""] {
        assert!(!body_s.contains(field), "{field} in {body_s}");
    }
    let (status, body) = call(&open, RESOLVE, STRANGER, 5, &json!({"uris": [anchor, reply1, reply2]})).await;
    assert_eq!(status, 200, "{body}");
    let r: Value = serde_json::from_str(&body).expect("json");
    let gaps: Vec<Value> = r["items"].as_array().expect("items").iter().map(|i| i["gap"].clone()).collect();
    assert_eq!(gaps, [Value::Null, json!("blocked-by-you"), json!("blocking-you")], "{body}");
    // A member that is not ready is served as a visitor until it is: it
    // follows the author, and is shown no `following`.
    let (status, body) = call(&open, GET_THREAD, R2, 6, &thread_input).await;
    assert_eq!(status, 200, "{body}");
    let n: Value = serde_json::from_str(&body).expect("json");
    assert!(summary(&n, "nodes").iter().all(|(_, gap)| gap.is_none()), "{body}");
    assert_eq!(n["nodes"][0]["post"]["author"]["viewer"], json!({"muted": false, "blockedBy": false}), "{body}");

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
    // The same for a visitor: nobody is clear, nothing is bridged, and
    // the ordinary view is cut under the authors it has a block with.
    let (_, body) = call(&open, GET_THREAD, STRANGER, 5, &thread_input).await;
    let off: Value = serde_json::from_str(&body).expect("json");
    assert_eq!((summary(&off, "nodes"), &off["bridging"]), (vec![(anchor.clone(), None)], &json!("off")), "{body}");
    // And with no answer from Farsight at all.
    *fake.status.lock().expect("status") = 503;
    let (_, body) = call(&open, RESOLVE, STRANGER, 5, &json!({"uris": [anchor, reply1]})).await;
    *fake.status.lock().expect("status") = 200;
    let r: Value = serde_json::from_str(&body).expect("json");
    assert_eq!((&r["items"][0]["gap"], &r["items"][1]["gap"], &r["bridging"]), (&json!("block-state-unknown"), &json!("blocked-by-you"), &json!("off")), "{body}");

    // Request-level failures; with `access = "members"`, the refusals of
    // a member endpoint.
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
    // `access = "signed_in"` still needs a token.
    let r = reqwest::Client::new().post(format!("{open}/xrpc/{GET_THREAD}")).json(&thread_input).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 401);

    // A block of a visitor applies at once. Farsight vouches again, and
    // the visitor's lens caches a vouching clean answer for the anchor's
    // author. Then that author blocks the visitor: the row here decides,
    // whatever the cached answer says, and the anchor is a gap.
    *fake.level.lock().expect("level") = "complete";
    let (_, body) = call(&open, GET_THREAD, STRANGER, 5, &thread_input).await;
    let before: Value = serde_json::from_str(&body).expect("json");
    assert_eq!((summary(&before, "nodes")[0].clone(), &before["bridging"]), ((anchor.clone(), None), &json!("full")), "{body}");
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{AUTHOR}/app.bsky.graph.block/3kblocknew', '{AUTHOR}', '{STRANGER}', '3kaaaaaaaaaa2', now())")).await;
    let (status, body) = call(&open, GET_THREAD, STRANGER, 5, &thread_input).await;
    assert_eq!(status, 200, "{body}");
    let after: Value = serde_json::from_str(&body).expect("json");
    let nodes = summary(&after, "nodes");
    assert!(nodes.contains(&(anchor.clone(), Some("blocking-you".to_string()))), "{body}");
    assert!(nodes.iter().all(|(_, gap)| gap.is_some()) && !body.contains("author.test"), "{body}");
    let (_, body) = call(&open, RESOLVE, STRANGER, 5, &json!({"uris": [anchor]})).await;
    let r: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(r["items"][0]["gap"], json!("blocking-you"), "{body}");
    db.drop_db().await;
}

/// B-84: one response adds at most `page_nodes` nodes and says where it
/// stopped; the cursor resumes there, only for its own node; with
/// bridging off a cursor that cannot be read from is kept as it was.
#[tokio::test]
async fn a_response_is_paged_by_a_sealed_cursor() {
    let Some(db) = fresh_db("threadlens_paging").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    // The viewer blocks R1, whose reply has three replies beneath it.
    let anchor = post(p, AUTHOR, "3kanchor2222", "anchor", None, None).await;
    let gapped = post(p, R1, "3kgapped2222", "gapped", Some(&anchor), Some(&anchor)).await;
    let mut beneath = Vec::new();
    for rkey in ["3kbeneath222", "3kbeneath333", "3kbeneath444"] {
        beneath.push(post(p, AUTHOR, rkey, "beneath", Some(&gapped), Some(&anchor)).await);
    }
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kb', '{VIEWER_A}', '{R1}', '3kaaaaaaaaaa2', now())")).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_farsight(p, ScopeSets::default(), farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h, &base, Arc::new(StubLookup::default()));
    let (req, viewer, key) = (Requester::test(), member(VIEWER_A), CursorKey::new(&[9; 32]));
    // Three added nodes a response: the gap, and two of the three beneath.
    let cfg = ThreadLensConfig { page_nodes: 3, ..ThreadLensConfig::default() };
    let uris = |nodes: &[LensNode]| nodes.iter().map(|n| n.uri().to_string()).collect::<Vec<_>>();
    let marker = |nodes: &[LensNode], uri: &str| nodes.iter().find(|n| n.uri() == uri).and_then(|n| n.markers().more_replies.clone());

    let request = thread::Request { viewer: &viewer, req: &req, anchor: &anchor, depth: 6, parent_height: 80 };
    let first = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(uris(&first.nodes), [anchor.clone(), gapped.clone(), beneath[0].clone(), beneath[1].clone()]);
    let cursor = marker(&first.nodes, &gapped).expect("a marker on the gap").expect("with a cursor");
    // The cursor is bound to its node.
    assert!(key.open(&anchor, &cursor).is_none());
    let position = key.open(&gapped, &cursor).expect("opens for its node");

    // The continuation: the gap as the anchor, no parents, the rest of
    // its sequence, and no marker left.
    let next = thread::Request { viewer: &viewer, req: &req, anchor: &gapped, depth: 1, parent_height: 0 };
    let second = thread::get_thread(&l, &cfg, &key, next, Some((position.clone(), cursor.clone()))).await.expect("thread");
    assert_eq!(uris(&second.nodes), [gapped.clone(), beneath[2].clone()]);
    assert!(matches!(&second.nodes[0], LensNode::Gap { gap: Gap::BlockedByYou, .. }));
    assert_eq!(marker(&second.nodes, &gapped), None);

    // Farsight stops vouching: replies to a gap are bridge candidates, so
    // nothing is read, and the marker keeps the cursor it came with.
    *fake.level.lock().expect("level") = "partial";
    let off = thread::get_thread(&l, &cfg, &key, next, Some((position, cursor.clone()))).await.expect("thread");
    assert_eq!((uris(&off.nodes), off.bridging), (vec![gapped.clone()], Bridging::Off));
    assert_eq!(marker(&off.nodes, &gapped), Some(Some(cursor)));
    db.drop_db().await;
}

/// T-15 (public-indexing design §9.2): unless the mode is On, an S author's
/// thread that is not stored here takes the upstream baseline, normalized
/// for the viewer. `threadMuted` holds on the root and its posts, and
/// `bookmarked` and `activitySubscription` are left out. The chain above a
/// blocked anchor is as the baseline names it.
#[tokio::test]
async fn an_s_authors_thread_takes_the_upstream_baseline_unless_the_mode_is_on() {
    use crate::hydration::testing::{FixedUpstream, hydrator_with_upstream_and_farsight, set_mode};
    use crate::indexing::PublicMode;

    let Some(db) = fresh_db("threadlens_public_off").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let at = |did: &str, rkey: &str| format!("at://{did}/app.bsky.feed.post/{rkey}");
    let (root, anchor, blocked) = (at(AUTHOR, "3kroot222222"), at(AUTHOR, "3kanchor2222"), at(R1, "3kblocked222"));
    let view = |uri: &str, did: &str, handle: &str, parent: Option<&str>| {
        let mut record = json!({"$type": "app.bsky.feed.post", "text": uri, "createdAt": "2026-09-26T00:00:00Z"});
        if let Some(parent) = parent {
            record["reply"] = json!({"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}});
        }
        json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy",
               "author": {"did": did, "handle": handle, "viewer": {}, "labels": []},
               "record": record, "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "labels": []})
    };
    let top = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&root, AUTHOR, "author.test", None), "replies": []});
    let mut up = FixedUpstream::default();
    for (uri, did, handle) in [(&anchor, AUTHOR, "author.test"), (&blocked, R1, "r1.test")] {
        let thread = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": view(uri, did, handle, Some(&root)), "parent": top, "replies": []});
        up.threads.insert(uri.clone(), json!({"thread": thread}));
    }
    // The viewer muted the thread, bookmarked the anchor, subscribed to its
    // author, and blocks R1.
    exec(p, &format!("INSERT INTO thread_mutes (viewer_did, root_uri) VALUES ('{VIEWER_A}', '{root}')")).await;
    exec(p, &format!("INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ('{VIEWER_A}', '{anchor}', 'bafy')")).await;
    exec(p, &format!("INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply) VALUES ('{VIEWER_A}', '{AUTHOR}', true, true)")).await;
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kb', '{VIEWER_A}', '{R1}', '3kaaaaaaaaaa2', now())")).await;
    let mut sets = ScopeSets::default();
    for did in [AUTHOR, R1] {
        sets.scope.insert(did.into());
    }
    let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_upstream_and_farsight(p, sets, Arc::new(up), farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let (req, viewer, key, cfg) = (Requester::test(), member(VIEWER_A), CursorKey::generated(), ThreadLensConfig::default());

    // On: an S author's posts are all stored here, so a missing anchor has
    // no baseline. The viewer fields are there.
    assert_eq!(h.lens_baseline(&viewer, &req, &anchor, 0, 80).await.expect("db"), None);
    assert_eq!(h.post_viewer(&viewer, std::slice::from_ref(&anchor)).await.expect("db")[&anchor]["bookmarked"], true);
    assert!(h.actor_viewer(&viewer, &[AUTHOR.to_string()]).await.expect("db").view(AUTHOR).get("activitySubscription").is_some());

    set_mode(h.scope(), PublicMode::Off).await;
    let request = thread::Request { viewer: &viewer, req: &req, anchor: &anchor, depth: 0, parent_height: 80 };
    let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    let posts: Vec<&Value> = got
        .nodes
        .iter()
        .map(|n| match n {
            LensNode::Post { post, .. } => post,
            LensNode::Gap { uri, gap, .. } => panic!("{uri} is a {} gap", gap.label()),
        })
        .collect();
    assert_eq!(posts.iter().map(|v| v["uri"].clone()).collect::<Vec<_>>(), [json!(root), json!(anchor)]);
    for post in &posts {
        assert_eq!(post["viewer"]["threadMuted"], true, "{post}");
        assert!(post["viewer"].get("bookmarked").is_none(), "{post}");
        assert!(post["author"]["viewer"].get("activitySubscription").is_none(), "{post}");
    }
    assert!(h.post_viewer(&viewer, std::slice::from_ref(&anchor)).await.expect("db")[&anchor].get("bookmarked").is_none());
    assert!(h.actor_viewer(&viewer, &[AUTHOR.to_string()]).await.expect("db").view(AUTHOR).get("activitySubscription").is_none());

    // A blocked anchor: a gap, with the chain above it as the baseline
    // names it.
    let request = thread::Request { anchor: &blocked, ..request };
    let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(got.nodes.len(), 2, "{:?}", got.nodes);
    assert!(matches!(&got.nodes[0], LensNode::Post { uri, .. } if *uri == root), "{:?}", got.nodes);
    assert!(matches!(&got.nodes[1], LensNode::Gap { uri, gap: Gap::BlockedByYou, .. } if *uri == blocked), "{:?}", got.nodes);
    assert_eq!(got.nodes[1].parent_uri(), Some(root.as_str()));
    db.drop_db().await;
}

/// B-83: the walk up the parents goes through a blocked author's post,
/// taking the link above it from the stored row, and stops with a marker
/// at `parentHeight`.
#[tokio::test]
async fn the_parent_walk_goes_past_a_blocked_author() {
    let Some(db) = fresh_db("threadlens_parents").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let root = post(p, AUTHOR, "3kroot222222", "root", None, None).await;
    let middle = post(p, R1, "3kmiddle2222", "middle", Some(&root), Some(&root)).await;
    let leaf = post(p, AUTHOR, "3kleaf222222", "leaf", Some(&middle), Some(&root)).await;
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kb', '{VIEWER_A}', '{R1}', '3kaaaaaaaaaa2', now())")).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_farsight(p, ScopeSets::default(), farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h, &base, Arc::new(StubLookup::default()));
    let (req, viewer, key, cfg) = (Requester::test(), member(VIEWER_A), CursorKey::generated(), ThreadLensConfig::default());
    let shape = |nodes: &[LensNode]| -> Vec<(String, Option<&'static str>)> {
        nodes
            .iter()
            .map(|n| match n {
                LensNode::Post { uri, .. } => (uri.clone(), None),
                LensNode::Gap { uri, gap, .. } => (uri.clone(), Some(gap.label())),
            })
            .collect()
    };

    // The ordinary view ends at the blocked parent. Thread Lens goes on
    // above it: the root is still there.
    let request = thread::Request { viewer: &viewer, req: &req, anchor: &leaf, depth: 0, parent_height: 80 };
    let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(shape(&got.nodes), [(root.clone(), None), (middle.clone(), Some("blocked-by-you")), (leaf.clone(), None)]);
    assert_eq!(got.nodes[1].parent_uri(), Some(root.as_str()));
    assert!(got.nodes.iter().all(|n| !n.markers().more_parents));
    assert_eq!(got.bridging, Bridging::Full);

    // `parentHeight` bounds the chain; the top node says there is more.
    let short = thread::Request { parent_height: 1, ..request };
    let got = thread::get_thread(&l, &cfg, &key, short, None).await.expect("thread");
    assert_eq!(shape(&got.nodes), [(middle.clone(), Some("blocked-by-you")), (leaf.clone(), None)]);
    assert!(got.nodes[0].markers().more_parents);

    // With bridging off the response is the ordinary view: it ends at the
    // blocked parent, and says nothing more.
    *fake.level.lock().expect("level") = "partial";
    let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(shape(&got.nodes), [(middle, Some("blocked-by-you")), (leaf, None)]);
    assert_eq!(got.bridging, Bridging::Off);
    db.drop_db().await;
}
