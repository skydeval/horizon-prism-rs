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

/// T-2 (Thread Lens amendment 1 §1): a post an upstream labelled `!warn`
/// comes through the ladder as a post, with the upstream labels removed;
/// one labelled `!hide` or `!takedown` is still `content-stripped`.
#[tokio::test]
async fn a_warned_post_is_a_post_on_the_ladder() {
    let Some(db) = fresh_db("threadlens_warn").await else {
        return;
    };
    let p = &db.pool;
    let view = |rkey: &str, label: &str| {
        let uri = format!("at://{AUTHOR}/app.bsky.feed.post/{rkey}");
        json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy",
               "author": {"did": AUTHOR, "handle": "author.test", "viewer": {}, "labels": []},
               "record": {"$type": "app.bsky.feed.post", "text": "warned text", "createdAt": "2026-09-26T00:00:00Z"},
               "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "replyCount": 0,
               "labels": [{"src": "did:plc:upstreamlabeler", "uri": uri, "val": label, "cts": "2026-09-26T00:00:00Z"}]})
    };
    let posts: HashMap<String, Value> = [("3kwarned2222", "!warn"), ("3khidden2222", "!hide"), ("3ktakedown22", "!takedown")]
        .into_iter()
        .map(|(rkey, label)| (format!("at://{AUTHOR}/app.bsky.feed.post/{rkey}"), view(rkey, label)))
        .collect();
    let app = AxumRouter::new()
        .route(
            "/xrpc/{method}",
            get(|State(posts): State<Arc<HashMap<String, Value>>>, Path(method): Path<String>, Query(q): Query<HashMap<String, String>>| async move {
                match (method.as_str(), q.get("uris").and_then(|u| posts.get(u))) {
                    ("app.bsky.feed.getPosts", Some(post)) => axum::Json(json!({"posts": [post]})).into_response(),
                    _ => (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "NotFound"}))).into_response(),
                }
            }),
        )
        .with_state(Arc::new(posts));
    let base = serve_fake(app).await;
    let l = ladder(hydrator(p, ScopeSets::default()), &base, Arc::new(StubLookup::default()));
    let (req, viewer) = (Requester::test(), member(VIEWER_A));
    let at = |rkey: &str| format!("at://{AUTHOR}/app.bsky.feed.post/{rkey}");
    match l.resolve(&viewer, &req, &at("3kwarned2222"), Mode::Full).await.expect("db") {
        Resolved::Post(view, _) => {
            assert_eq!(view["record"]["text"], "warned text");
            assert!(!view.to_string().contains("!warn"), "{view}");
        }
        other => panic!("a !warn post is a post: {other:?}"),
    }
    for rkey in ["3khidden2222", "3ktakedown22"] {
        assert_eq!(l.resolve(&viewer, &req, &at(rkey), Mode::Full).await.expect("db"), Resolved::Gap(Gap::ContentStripped), "{rkey}");
    }
    db.drop_db().await;
}

/// The URIs of the nodes that carry `hiddenByThreadgate`.
fn flagged(nodes: &[LensNode]) -> Vec<String> {
    nodes.iter().filter(|n| n.hidden_by_threadgate()).map(|n| n.uri().to_string()).collect()
}

/// Store a threadgate on `root` whose `hiddenReplies` are `hidden`, under
/// the root's own record key and author, or replace it.
async fn gate(p: &PgPool, root: &str, hidden: &[&str]) {
    let author = crate::db::locks::uri_author(root).expect("did");
    let rkey = root.rsplit('/').next().expect("rkey");
    let uri = format!("at://{author}/app.bsky.feed.threadgate/{rkey}");
    let record = json!({"$type": "app.bsky.feed.threadgate", "post": root, "hiddenReplies": hidden, "createdAt": "2026-09-26T12:00:00.000Z"});
    sqlx::query("DELETE FROM threadgates WHERE uri = $1").bind(&uri).execute(p).await.expect("delete");
    sqlx::query(
        "INSERT INTO threadgates (uri, author_did, rkey, cid, rev, post_uri, allow, hidden_replies, record, created_at) \
         VALUES ($1, $2, $3, 'bafygate', '3k', $4, NULL, $5, $6, now())",
    )
    .bind(&uri)
    .bind(author)
    .bind(rkey)
    .bind(root)
    .bind(hidden.iter().map(ToString::to_string).collect::<Vec<_>>())
    .bind(&record)
    .execute(p)
    .await
    .expect("gate");
}

/// T-3 – T-5, T-7 – T-9 (Thread Lens amendment 1 §2): replies the root's
/// threadgate hides are kept and flagged on every path, the baseline, the
/// expansion, a known-block gap and `resolve`; never for their own author,
/// never on the root; the gate itself is never sent. A gap item of
/// `resolve` is never flagged. A change of the gate shows on the next
/// request. A private root's gate is not read.
#[tokio::test]
async fn replies_the_root_hid_are_flagged_on_every_path() {
    let Some(db) = fresh_db("threadlens_hidden_replies").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (R2, "r2.test"), (VIEWER_A, "a.test"), (VIEWER_B, "b.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now()), ('{VIEWER_B}', 'admin', now())")).await;
    // root ← hidden (R2, hidden) ← beneath (R2)
    //      ← shown (R2)
    //      ← gapped (R1, which viewer A blocks; hidden) ← deep (R2, hidden)
    let root = post(p, AUTHOR, "3kroot222222", "root text", None, None).await;
    let hidden = post(p, R2, "3khidden2222", "hidden text", Some(&root), Some(&root)).await;
    let beneath = post(p, R2, "3kbeneath222", "beneath", Some(&hidden), Some(&root)).await;
    let shown = post(p, R2, "3kshown22222", "shown", Some(&root), Some(&root)).await;
    let gapped = post(p, R1, "3kgapped2222", "gapped", Some(&root), Some(&root)).await;
    let deep = post(p, R2, "3kdeep222222", "deep", Some(&gapped), Some(&root)).await;
    let never_stored = format!("at://{R1}/app.bsky.feed.post/3knever22222");
    gate(p, &root, &[&hidden, &gapped, &deep, &root, &never_stored]).await;
    let gate_uri = format!("at://{AUTHOR}/app.bsky.feed.threadgate/3kroot222222");
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kb', '{VIEWER_A}', '{R1}', '3kaaaaaaaaaa2', now())")).await;
    // The root's author blocks viewer B.
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{AUTHOR}/app.bsky.graph.block/3kb', '{AUTHOR}', '{VIEWER_B}', '3kaaaaaaaaaa2', now())")).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_farsight(p, ScopeSets::default(), farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let (req, key, cfg) = (Requester::test(), CursorKey::generated(), ThreadLensConfig::default());
    let viewer = member(VIEWER_A);
    let request = thread::Request { viewer: &viewer, req: &req, anchor: &root, depth: 6, parent_height: 80 };
    let emitted = |t: &thread::Thread, anchor: &str| serde_json::to_string(&crate::hydration::lens::lens_thread(anchor, &t.nodes, t.bridging, true)).expect("json");

    // Bridging on. The baseline keeps the hidden reply and what is beneath
    // it; the expansion places the blocked reply as a gap and the reply
    // beneath it, which the baseline never held. Each listed node is
    // flagged; the root, which the gate also lists, is not.
    let on = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(on.bridging, Bridging::Full);
    let uris: Vec<String> = on.nodes.iter().map(|n| n.uri().to_string()).collect();
    for uri in [&root, &hidden, &beneath, &shown, &gapped, &deep] {
        assert!(uris.contains(uri), "{uri} in {uris:?}");
    }
    let mut got = flagged(&on.nodes);
    got.sort();
    let mut want = vec![hidden.clone(), gapped.clone(), deep.clone()];
    want.sort();
    assert_eq!(got, want);
    assert!(matches!(on.nodes.iter().find(|n| n.uri() == gapped), Some(LensNode::Gap { gap: Gap::BlockedByYou, .. })));
    // No gate at the response level, and none on any node but the root's
    // own post view, which carries its `threadgate` as every post view of
    // this AppView does (design-v4 §8.2): the root here is a post the
    // caller may see.
    let body = emitted(&on, &root);
    let v: Value = serde_json::from_str(&body).expect("json");
    let mut keys: Vec<&String> = v.as_object().expect("object").keys().collect();
    keys.sort();
    assert_eq!(keys, ["anchor", "bridging", "nodes", "servedAs", "truncated"]);
    assert_eq!(v["nodes"][0]["post"]["threadgate"]["uri"], json!(gate_uri), "{body}");
    for node in v["nodes"].as_array().expect("nodes").iter().skip(1) {
        let text = node.to_string();
        for leaked in ["threadgate\"", "hiddenReplies", gate_uri.as_str(), "bafygate"] {
            assert!(!text.contains(leaked), "{leaked} in {text}");
        }
    }
    assert_eq!(body.matches("\"hiddenByThreadgate\":true").count(), 3, "{body}");

    // `getPostThread` is unchanged: it leaves the hidden reply out.
    let ordinary = h.get_post_thread(&viewer, &req, &root, 6, 80).await.expect("db").expect("view");
    let replies: Vec<Value> = ordinary.inner()["thread"]["replies"].as_array().expect("replies").iter().map(|r| r["post"]["uri"].clone()).collect();
    assert_eq!(replies, [json!(shown)], "getPostThread unchanged");

    // Bridging off: the response is the baseline, which now holds the
    // hidden reply and what is beneath it, flagged.
    // Farsight not answering (a partial level keeps bridging on).
    *fake.status.lock().expect("status") = 503;
    let off = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(off.bridging, Bridging::Off);
    assert_eq!(flagged(&off.nodes), vec![hidden.clone()]);
    assert!(off.nodes.iter().any(|n| n.uri() == beneath));
    *fake.status.lock().expect("status") = 200;

    // A hidden reply is not hidden from its own author.
    let r2 = member(R2);
    let own = thread::get_thread(&l, &cfg, &key, thread::Request { viewer: &r2, ..request }, None).await.expect("thread");
    assert!(!flagged(&own.nodes).contains(&hidden), "{:?}", flagged(&own.nodes));

    // The root is a gap: its author blocks viewer B. Anchored at the hidden
    // reply, the root is a `blocking-you` gap above it, and the reply is
    // still flagged from the root's gate. Nothing of the root or its gate
    // is sent.
    let b = member(VIEWER_B);
    let at_reply = thread::get_thread(&l, &cfg, &key, thread::Request { viewer: &b, anchor: &hidden, ..request }, None).await.expect("thread");
    assert!(matches!(&at_reply.nodes[0], LensNode::Gap { gap: Gap::BlockingYou, uri, .. } if *uri == root), "{:?}", at_reply.nodes);
    assert!(flagged(&at_reply.nodes).contains(&hidden));
    let body = emitted(&at_reply, &hidden);
    for leaked in ["root text", "author.test", "hiddenReplies", gate_uri.as_str()] {
        assert!(!body.contains(leaked), "{leaked} in {body}");
    }

    // `resolve`: a post item is flagged from its root's stored gate; a
    // known-block item never is, whether or not its row exists.
    let items = thread::resolve(&l, &cfg, &key, &viewer, &req, &[hidden.clone(), gapped.clone(), never_stored.clone(), shown.clone(), PRIVATE.to_string()])
        .await
        .expect("resolve");
    assert_eq!(flagged(&items.nodes), vec![hidden.clone()]);
    assert!(matches!(&items.nodes[1], LensNode::Gap { gap: Gap::BlockedByYou, .. }));
    assert!(matches!(&items.nodes[2], LensNode::Gap { gap: Gap::BlockedByYou, .. }));

    // The root's author unhides the reply: the next request has no flag on
    // it; hidden again, flagged again.
    gate(p, &root, &[&gapped]).await;
    let t = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(flagged(&t.nodes), vec![gapped.clone()]);
    gate(p, &root, &[&hidden]).await;
    let t = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(flagged(&t.nodes), vec![hidden.clone()]);

    // A public reply whose root is private: a gate stored under the private
    // URI (as no writer stores one) is not read.
    let under_private = post(p, AUTHOR, "3kunderpriv2", "under", Some(PRIVATE), Some(PRIVATE)).await;
    let child = post(p, R2, "3kchild22222", "child", Some(&under_private), Some(PRIVATE)).await;
    gate(p, PRIVATE, &[&child]).await;
    let t = thread::get_thread(&l, &cfg, &key, thread::Request { anchor: &under_private, ..request }, None).await.expect("thread");
    assert!(t.nodes.iter().any(|n| n.uri() == child), "{:?}", t.nodes);
    assert!(flagged(&t.nodes).is_empty(), "{:?}", flagged(&t.nodes));
    db.drop_db().await;
}

/// T-6 (amendment 1 §2.3 (b)): for an anchor not stored here, the gate view
/// of the upstream thread flags the replies it lists, when it names this
/// root and its own URI matches its `post`; it is never sent.
#[tokio::test]
async fn an_upstream_baselines_gate_flags_its_hidden_replies() {
    use crate::hydration::testing::{FixedUpstream, hydrator_with_upstream_and_farsight, set_mode};
    use crate::indexing::PublicMode;

    let Some(db) = fresh_db("threadlens_upstream_gate").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let at = |did: &str, rkey: &str| format!("at://{did}/app.bsky.feed.post/{rkey}");
    let (root, anchor, hid) = (at(AUTHOR, "3kroot222222"), at(AUTHOR, "3kanchor2222"), at(R1, "3khidden2222"));
    let view = |uri: &str, did: &str, parent: Option<&str>| {
        let mut record = json!({"$type": "app.bsky.feed.post", "text": uri, "createdAt": "2026-09-26T00:00:00Z"});
        if let Some(parent) = parent {
            record["reply"] = json!({"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}});
        }
        json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy",
               "author": {"did": did, "handle": "h.test", "viewer": {}, "labels": []},
               "record": record, "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "labels": []})
    };
    let gate_view = |post: &str, rkey: &str| {
        json!({"uri": format!("at://{AUTHOR}/app.bsky.feed.threadgate/{rkey}"), "cid": "bafygate",
               "record": {"$type": "app.bsky.feed.threadgate", "post": post, "hiddenReplies": [hid], "createdAt": "2026-09-26T00:00:00Z"}, "lists": []})
    };
    let thread = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&anchor, AUTHOR, Some(&root)),
        "parent": {"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&root, AUTHOR, None), "replies": []},
        "replies": [{"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&hid, R1, Some(&anchor)), "replies": []}]});
    let mut sets = ScopeSets::default();
    for did in [AUTHOR, R1] {
        sets.scope.insert(did.into());
    }
    let cases = [
        (gate_view(&root, "3kroot222222"), true, "a valid gate for this root"),
        (gate_view(&root, "3kother22222"), false, "its key is not its post's"),
        (gate_view(&anchor, "3kanchor2222"), false, "it is not this root's"),
    ];
    for (gv, flags, why) in cases {
        let mut up = FixedUpstream::default();
        up.threads.insert(anchor.clone(), json!({"thread": thread, "threadgate": gv}));
        let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
        let h = hydrator_with_upstream_and_farsight(p, sets.clone(), Arc::new(up), farsight);
        set_mode(h.scope(), PublicMode::Off).await;
        let base = fake_upstream(Arc::new(Calls::default()), None).await;
        let l = ladder(h, &base, Arc::new(StubLookup::default()));
        let (req, viewer) = (Requester::test(), member(VIEWER_A));
        let request = thread::Request { viewer: &viewer, req: &req, anchor: &anchor, depth: 6, parent_height: 80 };
        let t = thread::get_thread(&l, &ThreadLensConfig::default(), &CursorKey::generated(), request, None).await.expect("thread");
        assert!(t.nodes.iter().any(|n| n.uri() == hid), "{why}: {:?}", t.nodes);
        assert_eq!(flagged(&t.nodes), if flags { vec![hid.clone()] } else { Vec::new() }, "{why}");
        let body = serde_json::to_string(&crate::hydration::lens::lens_thread(&anchor, &t.nodes, t.bridging, true)).expect("json");
        assert!(!body.contains("hiddenReplies") && !body.contains("bafygate") && !body.contains("threadgate\""), "{why}: {body}");
    }
    db.drop_db().await;
}

/// T-7 (threadlens-hydration design §3.4): `viewer.replyDisabled` on the
/// posts of a thread whose root is a post node, from the root's gate and
/// this AppView's own rows only; for a visitor only what the gate alone
/// decides; never when the root is not in the response; no source asked.
#[tokio::test]
async fn reply_disabled_comes_from_the_roots_gate_and_local_rows() {
    use crate::hydration::testing::{FixedUpstream, hydrator_with_upstream_and_farsight, set_mode};
    use crate::indexing::PublicMode;

    let Some(db) = fresh_db("threadlens_reply_disabled").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let at = |did: &str, rkey: &str| format!("at://{did}/app.bsky.feed.post/{rkey}");
    let (root, anchor, reply) = (at(AUTHOR, "3kroot222222"), at(AUTHOR, "3kanchor2222"), at(R1, "3kreply22222"));
    let view = |uri: &str, did: &str, parent: Option<&str>, facets: Value| {
        let mut record = json!({"$type": "app.bsky.feed.post", "text": uri, "createdAt": "2026-09-26T00:00:00Z", "facets": facets});
        if let Some(parent) = parent {
            record["reply"] = json!({"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}});
        }
        json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy",
               "author": {"did": did, "handle": "h.test", "viewer": {}, "labels": []},
               "record": record, "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "labels": [], "replyCount": 0})
    };
    let mention = json!([{"index": {"byteStart": 0, "byteEnd": 2}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": VIEWER_A}]}]);
    let thread = |with_root: bool, facets: &Value| {
        let mut t = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&anchor, AUTHOR, Some(&root), json!([])),
            "replies": [{"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&reply, R1, Some(&anchor), json!([])), "replies": []}]});
        if with_root {
            t["parent"] = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&root, AUTHOR, None, facets.clone()), "replies": []});
        }
        t
    };
    let gate = |allow: Option<Value>| {
        let mut record = json!({"$type": "app.bsky.feed.threadgate", "post": root, "hiddenReplies": [], "createdAt": "2026-09-26T00:00:00Z"});
        if let Some(allow) = allow {
            record["allow"] = allow;
        }
        json!({"uri": format!("at://{AUTHOR}/app.bsky.feed.threadgate/3kroot222222"), "cid": "bafygate", "record": record, "lists": []})
    };
    let rule = |kind: &str| json!({"$type": format!("app.bsky.feed.threadgate#{kind}")});
    let other_list = json!({"$type": "app.bsky.feed.threadgate#listRule", "list": format!("at://{R1}/app.bsky.graph.list/3klist")});
    let mut sets = ScopeSets::default();
    for did in [AUTHOR, R1] {
        sets.scope.insert(did.into());
    }
    // (allow, root in the response, root mentions the member, member follows
    // the author) → (member's answer, visitor's answer, why).
    let none = json!([]);
    type Case<'a> = (Option<Value>, bool, &'a Value, bool, Option<bool>, Option<bool>, &'a str);
    let cases: Vec<Case> = vec![
        (None, true, &none, false, Some(false), Some(false), "no allow: anyone may reply"),
        (Some(json!([])), true, &none, false, Some(true), Some(true), "empty allow: nobody else may"),
        (Some(json!([rule("followerRule")])), true, &none, true, Some(false), None, "the member follows the author"),
        (Some(json!([rule("followerRule")])), true, &none, false, Some(true), None, "it does not"),
        (Some(json!([rule("followingRule")])), true, &none, false, None, None, "no follow by an author who is no member: unknown"),
        (Some(json!([rule("mentionRule")])), true, &mention, false, Some(false), None, "the root mentions it"),
        (Some(json!([other_list.clone()])), true, &none, false, None, None, "a list no ready member owns"),
        (Some(json!([rule("followerRule"), other_list.clone()])), true, &none, true, Some(false), None, "one rule admits it"),
        (Some(json!([rule("followingRule")])), true, &none, true, Some(false), None, "the author follows it"),
        (Some(json!([])), false, &none, false, None, None, "the root is not in the response"),
    ];
    for (allow, with_root, facets, follows, member_wants, visitor_wants, why) in cases {
        exec(p, "DELETE FROM follows").await;
        if follows {
            exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.follow/3kf', '{VIEWER_A}', '{AUTHOR}', '3k', now())")).await;
            // The author follows the member too: `followingRule` admits it.
            exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{AUTHOR}/app.bsky.graph.follow/3kf', '{AUTHOR}', '{VIEWER_A}', '3k', now())")).await;
        }
        let mut up = FixedUpstream::default();
        up.threads.insert(anchor.clone(), json!({"thread": thread(with_root, facets), "threadgate": gate(allow.clone())}));
        let up = Arc::new(up);
        let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
        let h = hydrator_with_upstream_and_farsight(p, sets.clone(), Arc::clone(&up) as Arc<dyn crate::hydration::UpstreamSource>, farsight);
        set_mode(h.scope(), PublicMode::Off).await;
        let calls = Arc::new(Calls::default());
        let base = fake_upstream(Arc::clone(&calls), None).await;
        let l = ladder(h, &base, Arc::new(StubLookup::default()));
        for (viewer, wants, who) in [(member(VIEWER_A), member_wants, "member"), (Viewer { did: Some(STRANGER.into()), hydrate: false }, visitor_wants, "visitor")] {
            let req = Requester::test();
            let request = thread::Request { viewer: &viewer, req: &req, anchor: &anchor, depth: 6, parent_height: 80 };
            let t = thread::get_thread(&l, &ThreadLensConfig::default(), &CursorKey::generated(), request, None).await.expect("thread");
            for node in &t.nodes {
                let LensNode::Post { uri, post, .. } = node else { continue };
                let got = post.pointer("/viewer/replyDisabled").and_then(Value::as_bool);
                assert_eq!(got, wants, "{why}, {who}, {uri}");
            }
            assert!(t.nodes.iter().any(|n| n.uri() == reply), "{why}");
        }
        // With the root in the baseline nothing is fetched at all; without
        // it, only the parent walk asks for the root, as it always did.
        if with_root {
            assert_eq!((calls.appview.load(Ordering::SeqCst), calls.slingshot.load(Ordering::SeqCst)), (0, 0), "{why}: nothing asked for it");
        }
    }
    db.drop_db().await;
}

/// T-11 (Thread Lens amendment 1 §2A): a `blocked-by-you` gap names the
/// caller's own list when the block comes through one, the smallest URI
/// when several do, from the caller's references or from Farsight's
/// `blockedByActor`; never for a direct block, never on `blocking-you`.
#[tokio::test]
async fn a_blocked_by_you_gap_names_the_callers_list() {
    let Some(db) = fresh_db("threadlens_blocking_list").await else {
        return;
    };
    let p = &db.pool;
    const D: &str = "did:plc:dddddddddddddddddddddddd";
    const E: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (R2, "r2.test"), (D, "d.test"), (E, "e.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let owner = "did:plc:oooooooooooooooooooooooo";
    let (l1, l2) = (format!("at://{owner}/app.bsky.graph.list/3kl1"), format!("at://{owner}/app.bsky.graph.list/3kl0"));
    // The viewer list-blocks L1 (holding R1 and D) and L2 (holding R1),
    // blocks D directly too, and blocks nobody else. R2 blocks the viewer
    // through a list of R2's (per Farsight). Farsight reports E in a list
    // the viewer list-blocks that no local reference covers.
    for (list, subjects) in [(&l1, vec![R1, D]), (&l2, vec![R1])] {
        exec(p, &format!("INSERT INTO subscribed_lists (list_uri, owner_did, state, purpose) VALUES ('{list}', '{owner}', 'active', 'app.bsky.graph.defs#modlist')")).await;
        for (i, s) in subjects.iter().enumerate() {
            let rkey = format!("3ki{}{i}", list.rsplit('/').next().expect("rkey"));
            exec(p, &format!("INSERT INTO subscribed_list_items (uri, list_uri, owner_did, rkey, cid, rev, subject_did) VALUES ('at://{owner}/app.bsky.graph.listitem/{rkey}', '{list}', '{owner}', '{rkey}', 'c', '3k', '{s}')")).await;
        }
        exec(p, &format!("INSERT INTO list_references (member_did, list_uri, source, admitted, effective) VALUES ('{VIEWER_A}', '{list}', 'block', true, true)")).await;
    }
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kd', '{VIEWER_A}', '{D}', '3kaaaaaaaaaa2', now())")).await;
    let anchor = post(p, AUTHOR, "3kanchor2222", "anchor", None, None).await;
    let by_r1 = post(p, R1, "3kbyr1222222", "secret r1", Some(&anchor), Some(&anchor)).await;
    let by_d = post(p, D, "3kbyd2222222", "secret d", Some(&anchor), Some(&anchor)).await;
    let by_r2 = post(p, R2, "3kbyr2222222", "secret r2", Some(&anchor), Some(&anchor)).await;
    let by_e = post(p, E, "3kbye2222222", "secret e", Some(&anchor), Some(&anchor)).await;
    let farsight_list = format!("at://{VIEWER_A}/app.bsky.graph.list/3kmine");
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    fake.blockers.lock().expect("blockers").insert(R2.to_string());
    fake.blocked_through.lock().expect("through").insert(E.to_string(), vec![farsight_list.clone()]);
    let h = hydrator_with_farsight(p, ScopeSets::default(), farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h, &base, Arc::new(StubLookup::default()));
    let (req, key, cfg) = (Requester::test(), CursorKey::generated(), ThreadLensConfig::default());
    let list_of = |nodes: &[LensNode], uri: &str| -> (Option<&'static str>, Option<String>) {
        let v = crate::hydration::lens::lens_items(nodes, Bridging::Full, true);
        let item = v.inner()["items"].as_array().expect("items").iter().find(|i| i["uri"] == json!(uri)).cloned().expect("node");
        let gap = Gap::ALL.iter().find(|g| item["gap"] == json!(g.label())).map(|g| g.label());
        (gap, item["blockingByListUri"].as_str().map(str::to_string))
    };

    for viewer in [member(VIEWER_A), Viewer { did: Some(VIEWER_A.to_string()), hydrate: false }] {
        let request = thread::Request { viewer: &viewer, req: &req, anchor: &anchor, depth: 6, parent_height: 80 };
        let t = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
        // R1 is in L1 and L2: the smaller URI, L2.
        assert_eq!(list_of(&t.nodes, &by_r1), (Some("blocked-by-you"), Some(l2.clone())));
        // D is in L1 and blocked directly: no list.
        assert_eq!(list_of(&t.nodes, &by_d), (Some("blocked-by-you"), None));
        // R2 blocks the viewer: never a list on `blocking-you`.
        assert_eq!(list_of(&t.nodes, &by_r2), (Some("blocking-you"), None));
        // E: from Farsight alone, the caller's own list it named.
        assert_eq!(list_of(&t.nodes, &by_e), (Some("blocked-by-you"), Some(farsight_list.clone())));
        let body = serde_json::to_string(&crate::hydration::lens::lens_thread(&anchor, &t.nodes, t.bridging, true)).expect("json");
        for leaked in ["secret", "r1.test", "r2.test", "d.test", "e.test", crate::farsight::lookup::tests::LIST] {
            assert!(!body.contains(leaked), "{leaked} in {body}");
        }
        // `resolve` names the same lists.
        let items = thread::resolve(&l, &cfg, &key, &viewer, &req, &[by_r1.clone(), by_d.clone(), by_r2.clone(), by_e.clone()]).await.expect("resolve");
        assert_eq!(list_of(&items.nodes, &by_r1).1, Some(l2.clone()));
        assert_eq!(list_of(&items.nodes, &by_d).1, None);
        assert_eq!(list_of(&items.nodes, &by_r2).1, None);
        assert_eq!(list_of(&items.nodes, &by_e).1, Some(farsight_list.clone()));
    }
    db.drop_db().await;
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
    let live = crate::admin::settings::LiveSettings::default();
    live.update(|v| v.threadlens_access = access);
    ThreadLensState {
        live,
        app: AppState {
            hydrator: h,
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: Default::default(),
        },
        ladder: l,
        cfg: ThreadLensConfig { access, ..ThreadLensConfig::default() },
        cursor_key: CursorKey::new(&[9; 32]),
        outline: None,
    }
}

/// The Thread Lens class's 429 names `Retry-After` from its reset. The
/// class is charged after the token verifies and before any database read.
#[tokio::test]
async fn the_thread_lens_429_carries_retry_after() {
    let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), None).await;
    let h = hydrator(&pool, ScopeSets::default());
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let mut s = state(h, l, ThreadLensAccess::SignedIn);
    s.app.limits = RateLimits::from_config(&crate::config::RateLimitsConfig {
        thread_lens: crate::config::RateClassConfig::new(0.001, 1),
        ..crate::config::RateLimitsConfig::default()
    });
    let url = serve(s).await;
    let token = mint(&signing_key(3), VIEWER_A, &format!("did:web:{AUDIENCE_HOST}"), Some(RESOLVE));
    let send = || {
        reqwest::Client::new()
            .post(format!("{url}/xrpc/{RESOLVE}"))
            .header("authorization", format!("Bearer {token}"))
            .json(&json!({"uris": [PRIVATE]}))
            .send()
    };
    // The one token is taken; the database read after it fails here.
    assert_ne!(send().await.expect("request").status().as_u16(), 429);
    let r = send().await.expect("request");
    assert_eq!(r.status().as_u16(), 429);
    let get = |name: &str| r.headers().get(name).map(|v| v.to_str().expect("ascii").to_string());
    crate::xrpc::errors::assert_retry_after(get("retry-after").as_deref(), get("ratelimit-reset").as_deref());
}

/// T-F1 (fast-reads design §1.1, §2.2): a Thread Lens route takes a direct
/// call's token as it takes a proxied one. A bare `aud` and
/// `#bsky_appview`, an hour's window, pass authentication; another
/// fragment, another `lxm`, no `lxm` and a window past an hour are 401.
/// (A token that passes gets whatever the empty database answers, never
/// 401.)
#[tokio::test]
async fn direct_tokens_reach_thread_lens() {
    use crate::kryphocron_glue::jwt::tests::mint_claims;

    let Some(db) = fresh_db("threadlens_direct_tokens").await else {
        return;
    };
    let calls = Arc::new(Calls::default());
    let base = fake_upstream(Arc::clone(&calls), None).await;
    let h = hydrator(&db.pool, ScopeSets::default());
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let url = serve(state(h, l, ThreadLensAccess::SignedIn)).await;
    let host = format!("did:web:{AUDIENCE_HOST}");
    let now = chrono::Utc::now().timestamp();
    let token = |aud: &str, lxm: Option<&str>, window: i64| {
        let mut claims = json!({"iss": VIEWER_A, "aud": aud, "iat": now, "exp": now + window});
        if let Some(lxm) = lxm {
            claims["lxm"] = json!(lxm);
        }
        mint_claims(&signing_key(3), &claims)
    };
    let fragment = format!("{host}#bsky_appview");
    let other = format!("{host}#atproto_labeler");
    for (aud, lxm, window, passes) in [
        (host.as_str(), Some(RESOLVE), 60, true),
        (fragment.as_str(), Some(RESOLVE), 3_540, true),
        (host.as_str(), Some(RESOLVE), 3_600, true),
        (host.as_str(), Some(RESOLVE), 3_601, false),
        (other.as_str(), Some(RESOLVE), 3_540, false),
        (fragment.as_str(), Some(GET_THREAD), 3_540, false),
        (fragment.as_str(), None, 60, false),
    ] {
        let r = reqwest::Client::new()
            .post(format!("{url}/xrpc/{RESOLVE}"))
            .header("authorization", format!("Bearer {}", token(aud, lxm, window)))
            .json(&json!({"uris": [PRIVATE]}))
            .send()
            .await
            .expect("request");
        let status = r.status().as_u16();
        assert_eq!(status != 401, passes, "{aud} {lxm:?} {window}: {status}");
    }
    db.drop_db().await;
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
    // T-6 (threadlens-hydration design §3.2): whose viewer state it is.
    assert_eq!(v["servedAs"], "member", "{body}");
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
    assert_eq!(s["servedAs"], "visitor", "{body_s}");
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
    assert_eq!(r["servedAs"], "visitor", "{body}");
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
    assert_eq!(r["servedAs"], "member", "{body}");

    // Farsight answers without vouching: bridging is off and the response
    // is the ordinary view, which for viewer A is the anchor alone.
    // Farsight not answering (a partial level keeps bridging on).
    *fake.status.lock().expect("status") = 503;
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
    // T-DM9 (degraded-mode design §3.5): ready once before, its readiness
    // since cleared: `503 Reindexing`, with `retryAfter` in the body.
    exec(p, &format!("UPDATE members SET first_ready_at = now() WHERE did = '{R2}'")).await;
    let (status, body) = call(&url, GET_THREAD, R2, 6, &thread_input).await;
    assert!(status == 503 && body.contains("\"Reindexing\"") && body.contains("\"retryAfter\":30"), "{body}");
    exec(p, &format!("UPDATE members SET first_ready_at = NULL WHERE did = '{R2}'")).await;
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
    assert!(key.open::<super::cursor::Position>(&anchor, &cursor).is_none());
    let position: super::cursor::Position = key.open(&gapped, &cursor).expect("opens for its node");

    // The continuation: the gap as the anchor, no parents, the rest of
    // its sequence, and no marker left.
    let next = thread::Request { viewer: &viewer, req: &req, anchor: &gapped, depth: 1, parent_height: 0 };
    let second = thread::get_thread(&l, &cfg, &key, next, Some((position.clone(), cursor.clone()))).await.expect("thread");
    assert_eq!(uris(&second.nodes), [gapped.clone(), beneath[2].clone()]);
    assert!(matches!(&second.nodes[0], LensNode::Gap { gap: Gap::BlockedByYou, .. }));
    assert_eq!(marker(&second.nodes, &gapped), None);

    // Farsight stops answering: replies to a gap are bridge candidates, so
    // nothing is read, and the marker keeps the cursor it came with.
    // Farsight not answering (a partial level keeps bridging on).
    *fake.status.lock().expect("status") = 503;
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
    assert_eq!(h.lens_baseline(&viewer, &req, &anchor, 0, 80, &Default::default()).await.expect("db"), None);
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
        // The upstream answer named no threadgate: the root has none, so
        // the viewer may reply, and the client can skip its own read.
        assert_eq!(post["viewer"]["replyDisabled"], false, "{post}");
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

    // `parentHeight` bounds the chain; the top node says there is more,
    // and names its parent from the stored row, as it would had the walk
    // gone on. Nothing else of the blocked post is sent.
    let short = thread::Request { parent_height: 1, ..request };
    let got = thread::get_thread(&l, &cfg, &key, short, None).await.expect("thread");
    assert_eq!(shape(&got.nodes), [(middle.clone(), Some("blocked-by-you")), (leaf.clone(), None)]);
    assert!(got.nodes[0].markers().more_parents);
    assert_eq!(got.nodes[0].parent_uri(), Some(root.as_str()));
    let emitted = serde_json::to_string(&crate::hydration::lens::lens_thread(&leaf, &got.nodes, got.bridging, true)).expect("json");
    let top: Value = serde_json::from_str::<Value>(&emitted).expect("json")["nodes"][0].clone();
    assert_eq!(top, json!({"$type": "app.nearhorizon.threadLens.defs#gapNode", "uri": middle, "gap": "blocked-by-you", "parentUri": root, "moreParents": true}));
    assert!(!emitted.contains("\"middle\"") && !emitted.contains("r1.test"), "{emitted}");

    // With bridging off the response is the ordinary view: it ends at the
    // blocked parent, and says nothing more.
    // Farsight not answering (a partial level keeps bridging on).
    *fake.status.lock().expect("status") = 503;
    let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    assert_eq!(shape(&got.nodes), [(middle, Some("blocked-by-you")), (leaf, None)]);
    assert_eq!(got.bridging, Bridging::Off);
    db.drop_db().await;
}

/// Each node's URI and gap label, in order.
fn shape_of(nodes: &[LensNode]) -> Vec<(String, Option<&'static str>)> {
    nodes
        .iter()
        .map(|n| match n {
            LensNode::Post { uri, .. } => (uri.clone(), None),
            LensNode::Gap { uri, gap, .. } => (uri.clone(), Some(gap.label())),
        })
        .collect()
}

/// A post view as an upstream source builds it, replying to `parent`. A
/// blocked post's (text "secret …") author is `blocked.test`.
fn upstream_view(uri: &str, did: &str, text: &str, parent: Option<&str>, root: &str) -> Value {
    let mut record = json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-09-26T00:00:00Z"});
    if let Some(parent) = parent {
        record["reply"] = json!({"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}});
    }
    let handle = if text.starts_with("secret") { "blocked.test" } else { "shown.test" };
    json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy",
           "author": {"did": did, "handle": handle, "viewer": {}, "labels": []},
           "record": record, "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "labels": []})
}

/// Every gap node sent carries its URI, gap and links, and nothing of the
/// blocked post: no text, no author, no handle.
fn assert_bare_gaps(anchor: &str, got: &thread::Thread) {
    let emitted = serde_json::to_value(crate::hydration::lens::lens_thread(anchor, &got.nodes, got.bridging, true)).expect("json");
    for node in emitted["nodes"].as_array().expect("nodes") {
        if node["$type"] == "app.nearhorizon.threadLens.defs#gapNode" {
            let keys: Vec<&str> = node.as_object().expect("object").keys().map(String::as_str).collect();
            assert!(keys.iter().all(|k| ["$type", "uri", "gap", "parentUri", "moreParents"].contains(k)), "{node}");
        }
    }
    let body = emitted.to_string();
    assert!(!body.contains("secret") && !body.contains("blocked.test"), "{body}");
}

/// #895: the walk goes up past a blocked post that has no local row (a
/// non-member's post, public indexing off), several times, taking each link
/// from the baseline, which read the chain to build the ordinary view. The
/// same for a post by an author who blocks the viewer. No record is fetched
/// for it, nothing of the blocked posts is sent, and with bridging off the
/// response is still the ordinary view, which ends at the first block.
#[tokio::test]
async fn the_walk_goes_past_blocked_posts_with_no_local_row() {
    use crate::hydration::testing::{FixedUpstream, hydrator_with_upstream_and_farsight};

    let Some(db) = fresh_db("threadlens_walk_no_row").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(R1, "r1.test"), (R2, "r2.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    // The viewer blocks R1; R2 blocks the viewer.
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kb', '{VIEWER_A}', '{R1}', '3kaaaaaaaaaa2', now())")).await;
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{R2}/app.bsky.graph.block/3kc', '{R2}', '{VIEWER_A}', '3kaaaaaaaaaa2', now())")).await;
    let at = |did: &str, rkey: &str| format!("at://{did}/app.bsky.feed.post/{rkey}");
    let mut up = FixedUpstream::default();
    // A back-and-forth: the viewer's posts are stored, the other side's
    // are not. root ← b1 ← v1 ← b2 ← anchor.
    let mut cases = Vec::new();
    for (other, t, label) in [(R1, "a", "blocked-by-you"), (R2, "b", "blocking-you")] {
        let root = post(p, VIEWER_A, &format!("3kroot{t}"), "root", None, None).await;
        let b1 = at(other, &format!("3kb1{t}"));
        let v1 = post(p, VIEWER_A, &format!("3kv1{t}"), "v1", Some(&b1), Some(&root)).await;
        let b2 = at(other, &format!("3kb2{t}"));
        let anchor = post(p, VIEWER_A, &format!("3kanchor{t}"), "anchor", Some(&b2), Some(&root)).await;
        up.posts.insert(b1.clone(), upstream_view(&b1, other, "secret b1", Some(&root), &root));
        up.posts.insert(b2.clone(), upstream_view(&b2, other, "secret b2", Some(&v1), &root));
        cases.push((label, root, b1, v1, b2, anchor));
    }
    let up = Arc::new(up);
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_upstream_and_farsight(p, ScopeSets::default(), Arc::clone(&up) as _, farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h, &base, Arc::new(StubLookup::default()));
    let (req, viewer, key, cfg) = (Requester::test(), member(VIEWER_A), CursorKey::generated(), ThreadLensConfig::default());

    for (label, root, b1, v1, b2, anchor) in &cases {
        let request = thread::Request { viewer: &viewer, req: &req, anchor: anchor.as_str(), depth: 0, parent_height: 80 };
        up.take_modes();
        let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
        let want = [(root.clone(), None), (b1.clone(), Some(*label)), (v1.clone(), None), (b2.clone(), Some(*label)), (anchor.clone(), None)];
        assert_eq!(shape_of(&got.nodes), want, "{label}");
        assert_eq!(got.nodes[1].parent_uri(), Some(root.as_str()), "{label}");
        assert_eq!(got.nodes[3].parent_uri(), Some(v1.as_str()), "{label}");
        assert!(got.nodes.iter().all(|n| !n.markers().more_parents), "{label}");
        assert_eq!(got.bridging, Bridging::Full, "{label}");
        assert!(!up.take_modes().iter().any(|(call, _)| call.starts_with("record ")), "{label}: the baseline's links, no record fetched");
        assert_bare_gaps(anchor, &got);

        // `parentHeight` still bounds the walk: the gap names its parent and
        // says there is more.
        let short = thread::Request { parent_height: 1, ..request };
        let got = thread::get_thread(&l, &cfg, &key, short, None).await.expect("thread");
        assert_eq!(shape_of(&got.nodes), [(b2.clone(), Some(*label)), (anchor.clone(), None)], "{label}");
        let emitted = serde_json::to_value(crate::hydration::lens::lens_thread(anchor, &got.nodes, got.bridging, true)).expect("json");
        assert_eq!(
            emitted["nodes"][0],
            json!({"$type": "app.nearhorizon.threadLens.defs#gapNode", "uri": b2, "gap": label, "parentUri": v1, "moreParents": true})
        );
    }

    // Bridging off: the ordinary view, which ends at the block and names no
    // parent for it.
    // Farsight not answering (a partial level keeps bridging on).
    *fake.status.lock().expect("status") = 503;
    for (label, _, _, _, b2, anchor) in &cases {
        let request = thread::Request { viewer: &viewer, req: &req, anchor: anchor.as_str(), depth: 0, parent_height: 80 };
        let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
        assert_eq!(shape_of(&got.nodes), [(b2.clone(), Some(*label)), (anchor.clone(), None)], "{label}");
        assert_eq!(got.nodes[0].parent_uri(), None, "{label}");
        assert_eq!(got.bridging, Bridging::Off);
        assert_bare_gaps(anchor, &got);
    }
    db.drop_db().await;
}

/// #895: where the baseline does not know the link above a blocked post (an
/// upstream thread that ends at the block), the walk reads it from the
/// blocked post's record, and only that: twice here, past two blocked
/// posts. Nothing of either record is sent.
#[tokio::test]
async fn the_walk_reads_the_link_above_a_blocked_post_from_its_record() {
    use crate::hydration::testing::{FixedUpstream, hydrator_with_upstream_and_farsight};

    let Some(db) = fresh_db("threadlens_walk_record_link").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kb', '{VIEWER_A}', '{R1}', '3kaaaaaaaaaa2', now())")).await;
    let at = |did: &str, rkey: &str| format!("at://{did}/app.bsky.feed.post/{rkey}");
    // root ← b1 ← v1 ← b2 ← anchor; the anchor is not stored, so the
    // baseline is an upstream thread, which ends at b2.
    let root = post(p, VIEWER_A, "3kroot22222", "root", None, None).await;
    let b1 = at(R1, "3kb122222");
    let v1 = post(p, VIEWER_A, "3kv122222", "v1", Some(&b1), Some(&root)).await;
    let b2 = at(R1, "3kb222222");
    let anchor = at(AUTHOR, "3kanchor2222");
    let record = |parent: &str, text: &str| {
        json!({"uri": "", "cid": "c", "value": {"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-09-26T00:00:00Z",
               "reply": {"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}}}})
    };
    let mut up = FixedUpstream::default();
    up.records.insert(b1.clone(), record(&root, "secret b1"));
    up.records.insert(b2.clone(), record(&v1, "secret b2"));
    let thread = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": upstream_view(&anchor, AUTHOR, "anchor", Some(&b2), &root),
                        "parent": {"$type": "app.bsky.feed.defs#blockedPost", "uri": b2, "blocked": true, "author": {"did": R1, "handle": "blocked.test"}},
                        "replies": []});
    up.threads.insert(anchor.clone(), json!({"thread": thread}));
    let up = Arc::new(up);
    let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_upstream_and_farsight(p, ScopeSets::default(), Arc::clone(&up) as _, farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h, &base, Arc::new(StubLookup::default()));
    let (req, viewer, key, cfg) = (Requester::test(), member(VIEWER_A), CursorKey::generated(), ThreadLensConfig::default());

    let request = thread::Request { viewer: &viewer, req: &req, anchor: &anchor, depth: 0, parent_height: 80 };
    let got = thread::get_thread(&l, &cfg, &key, request, None).await.expect("thread");
    let want = [(root.clone(), None), (b1.clone(), Some("blocked-by-you")), (v1.clone(), None), (b2.clone(), Some("blocked-by-you")), (anchor.clone(), None)];
    assert_eq!(shape_of(&got.nodes), want);
    assert_eq!(got.nodes[1].parent_uri(), Some(root.as_str()));
    assert_eq!(got.nodes[3].parent_uri(), Some(v1.as_str()));
    let records: Vec<String> = up.take_modes().into_iter().map(|(call, _)| call).filter(|call| call.starts_with("record ")).collect();
    assert_eq!(records, [format!("record {b2}"), format!("record {b1}")]);
    assert_bare_gaps(&anchor, &got);
    db.drop_db().await;
}

/// One call the labelers' fake AppView received: the method, the
/// `uris`/`uri` it asked for, its `atproto-accept-labelers`, and every
/// header name and value it carried.
#[derive(Debug, Clone)]
struct LabelledCall {
    method: String,
    asked: Vec<String>,
    accept: Option<String>,
    headers: Vec<(String, String)>,
    query: String,
}

const L1: &str = "did:plc:labeler1111111111111111";
const L2: &str = "did:plc:labeler2222222222222222";
const L3: &str = "did:plc:labeler3333333333333333";

/// An AppView answering `getPosts` for any post URI with a view labelled
/// by L1 (asked for), L2 (not asked for) and Prism's own labeler (an
/// upstream copy), its author labelled by L1, and saying it applied L1 and
/// a labeler nobody asked for. Everything else is NotFound.
async fn labelling_appview(calls: Arc<std::sync::Mutex<Vec<LabelledCall>>>) -> String {
    use axum::extract::RawQuery;
    use axum::http::HeaderMap;

    let app = AxumRouter::new()
        .route(
            "/xrpc/{method}",
            get(
                |State(calls): State<Arc<std::sync::Mutex<Vec<LabelledCall>>>>, Path(method): Path<String>, headers: HeaderMap, RawQuery(query): RawQuery| async move {
                    let query = query.unwrap_or_default();
                    let asked: Vec<String> = url::form_urlencoded::parse(query.as_bytes())
                        .filter(|(k, _)| k == "uris" || k == "uri")
                        .map(|(_, v)| v.into_owned())
                        .collect();
                    calls.lock().expect("calls").push(LabelledCall {
                        method: method.clone(),
                        asked: asked.clone(),
                        accept: headers.get("atproto-accept-labelers").map(|v| v.to_str().expect("ascii").to_string()),
                        headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string())).collect(),
                        query: query.clone(),
                    });
                    if method != "app.bsky.feed.getPosts" {
                        return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "NotFound"}))).into_response();
                    }
                    let label = |src: &str, uri: &str, val: &str| json!({"src": src, "uri": uri, "val": val, "cts": "2026-10-24T00:00:00Z"});
                    let posts: Vec<Value> = asked
                        .iter()
                        .map(|uri| {
                            let did = crate::db::locks::uri_author(uri).expect("did");
                            json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy",
                                   "author": {"did": did, "handle": "upstream.test", "labels": [label(L1, did, "whimsy")]},
                                   "record": {"$type": "app.bsky.feed.post", "text": "upstream text", "createdAt": "2026-10-24T00:00:00Z"},
                                   "indexedAt": "2026-10-24T00:00:00Z", "replyCount": 0,
                                   "labels": [label(L1, uri, "meme"), label(L2, uri, "other"), label(crate::hydration::testing::LABELER, uri, "stale-own")]})
                        })
                        .collect();
                    ([("atproto-content-labelers", format!("{L1};redact, did:plc:notasked"))], axum::Json(json!({"posts": posts}))).into_response()
                },
            ),
        )
        .with_state(calls);
    serve_fake(app).await
}

/// `(src, val)` of a view's labels.
fn labels_of(v: &Value) -> Vec<(String, String)> {
    v["labels"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|l| (l["src"].as_str().unwrap_or_default().to_string(), l["val"].as_str().unwrap_or_default().to_string()))
        .collect()
}

/// #887: a
/// request's `atproto-accept-labelers`, rebuilt from its valid entries,
/// goes with the upstream calls; the labels those labelers applied are on
/// the post and author views, beside Prism's own and without its upstream
/// copy; a post Prism built from its own row gets them from one `getPosts`
/// read for labels alone; `atproto-content-labelers` names what was
/// applied. Nothing else of the request goes upstream. Without the header,
/// or with one past the bound, nothing changes.
#[tokio::test]
async fn the_readers_labelers_label_the_lens_views() {
    let Some(db) = fresh_db("threadlens_labelers").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let local = post(p, AUTHOR, "3klocal22222", "local", None, None).await;
    exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{local}', 'prism-own')")).await;
    // Not stored here: the ladder fetches it.
    let remote = format!("at://{STRANGER}/app.bsky.feed.post/3kremote2222");
    let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_farsight(p, ScopeSets::default(), farsight);
    let calls: Arc<std::sync::Mutex<Vec<LabelledCall>>> = Arc::default();
    let base = labelling_appview(Arc::clone(&calls)).await;
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let url = serve(state(h, l, ThreadLensAccess::Members)).await;
    let own = crate::hydration::testing::LABELER;
    let token = |method: &str| mint(&signing_key(3), VIEWER_A, &format!("did:web:{AUDIENCE_HOST}"), Some(method));
    let send = |method: &'static str, body: Value, accept: Option<String>| {
        let mut r = reqwest::Client::new()
            .post(format!("{url}/xrpc/{method}"))
            .header("authorization", format!("Bearer {}", token(method)))
            .header("x-reader-secret", "do-not-forward")
            .json(&body);
        if let Some(a) = accept {
            r = r.header("atproto-accept-labelers", a);
        }
        r.send()
    };
    let take = || std::mem::take(&mut *calls.lock().expect("calls"));

    // With labelers: L1 (redact), an invalid entry, Prism's own, and L3.
    let accept = format!("{L1};redact, not a did, {own}, {L3};weird");
    let r = send(RESOLVE, json!({"uris": [local, remote]}), Some(accept.clone())).await.expect("request");
    assert_eq!(r.status().as_u16(), 200);
    // L1 was applied upstream and Prism's own by Prism; L3 by nobody, and
    // the labeler nobody asked for is not named.
    let content = r.headers().get("atproto-content-labelers").map(|v| v.to_str().expect("ascii").to_string());
    assert_eq!(content.as_deref(), Some(format!("{L1};redact, {own}").as_str()));
    let v: Value = r.json().await.expect("json");
    let (local_view, remote_view) = (&v["items"][0]["post"], &v["items"][1]["post"]);
    assert_eq!((&v["items"][0]["uri"], &v["items"][1]["uri"]), (&json!(local), &json!(remote)), "{v}");
    // The local post keeps its own text and Prism's label, and gains L1's.
    assert_eq!(local_view["record"]["text"], "local", "{v}");
    assert_eq!(labels_of(local_view), [(own.to_string(), "prism-own".to_string()), (L1.to_string(), "meme".to_string())], "{v}");
    assert_eq!(labels_of(&local_view["author"]), [(L1.to_string(), "whimsy".to_string())], "{v}");
    // The fetched post keeps L1's labels only: not L2's (not asked for),
    // not the upstream copy of Prism's own.
    assert_eq!(labels_of(remote_view), [(L1.to_string(), "meme".to_string())], "{v}");
    assert_eq!(labels_of(&remote_view["author"]), [(L1.to_string(), "whimsy".to_string())], "{v}");
    // Two `getPosts`: the ladder's for the remote post, then the labels
    // read for the local one alone. Both carried the rebuilt header and
    // nothing of the caller.
    let got = take();
    let posts: Vec<&LabelledCall> = got.iter().filter(|c| c.method == "app.bsky.feed.getPosts").collect();
    assert_eq!(posts.iter().map(|c| c.asked.clone()).collect::<Vec<_>>(), [vec![remote.clone()], vec![local.clone()]], "{got:?}");
    for c in &got {
        assert_eq!(c.accept.as_deref(), Some(format!("{L1};redact, {own}, {L3}").as_str()), "{c:?}");
        for (name, value) in &c.headers {
            assert!(!["authorization", "x-reader-secret", "cookie"].contains(&name.as_str()), "{name} forwarded: {c:?}");
            assert!(!value.contains(VIEWER_A) && !value.contains("do-not-forward"), "{name}: {value}");
        }
        assert!(!c.query.contains(VIEWER_A), "{c:?}");
    }

    // `getThread` on the local post: the same labels, the same header.
    let r = send(GET_THREAD, json!({"anchor": local, "depth": 0}), Some(accept.clone())).await.expect("request");
    assert_eq!(r.headers().get("atproto-content-labelers").map(|v| v.to_str().expect("ascii").to_string()), Some(format!("{L1};redact, {own}")));
    let t: Value = r.json().await.expect("json");
    assert_eq!(labels_of(&t["nodes"][0]["post"]), [(own.to_string(), "prism-own".to_string()), (L1.to_string(), "meme".to_string())], "{t}");
    let got = take();
    assert_eq!(got.iter().map(|c| (c.method.as_str(), c.asked.clone())).collect::<Vec<_>>(), [("app.bsky.feed.getPosts", vec![local.clone()])], "{got:?}");
    assert_eq!(got[0].accept.as_deref(), Some(format!("{L1};redact, {own}, {L3}").as_str()));

    // Without the header, with one past the bound, and with one naming no
    // valid DID: no header upstream, no labels read for the local post,
    // no `atproto-content-labelers`, and only Prism's labels.
    let oversize = format!("{L1}, {}", "x".repeat(crate::appview_clients::accept_labelers::MAX_HEADER_BYTES));
    for accept in [None, Some(oversize), Some("nothing, valid;redact".to_string())] {
        let r = send(RESOLVE, json!({"uris": [local, remote]}), accept.clone()).await.expect("request");
        assert_eq!(r.status().as_u16(), 200);
        assert!(r.headers().get("atproto-content-labelers").is_none(), "{accept:?}");
        let v: Value = r.json().await.expect("json");
        assert_eq!(labels_of(&v["items"][0]["post"]), [(own.to_string(), "prism-own".to_string())], "{accept:?}: {v}");
        assert!(labels_of(&v["items"][1]["post"]).is_empty() && labels_of(&v["items"][1]["post"]["author"]).is_empty(), "{accept:?}: {v}");
        let got = take();
        assert_eq!(got.iter().map(|c| c.asked.clone()).collect::<Vec<_>>(), [vec![remote.clone()]], "{accept:?}: {got:?}");
        assert!(got.iter().all(|c| c.accept.is_none()), "{accept:?}: {got:?}");
    }
    db.drop_db().await;
}

mod handle_anchor;
mod outline;
mod quoted;
