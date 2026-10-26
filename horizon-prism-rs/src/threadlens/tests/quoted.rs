//! `embedWhole` end to end (threadlens-quoted-posts design §6): which post
//! nodes are marked whole for the reader, through the real upstream source
//! and a fake AppView, with each marked case beside its unmarked one.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use axum::Router as AxumRouter;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use serde_json::{Value, json};

use super::*;
use crate::appview_clients::accept_labelers::{AcceptLabelers, LabelerScope, with_labelers};
use crate::hydration::cache::ReadCache;
use crate::hydration::testing::{LABELER, hydrator_with_upstream_and_farsight};
use crate::hydration::upstream::{PdsLister, RealUpstreamSource, ViewSource};
use crate::xrpc::pools::{HostBuckets, Shares};

/// The quoted author: `Clear` for the viewer.
const QUOTED: &str = STRANGER;
/// An author that blocks the viewer.
const BLOCKER: &str = R1;

/// A fake AppView: `getPostThread` from `threads` (NotFound while `down`),
/// `getPosts` from `posts`, both answering `atproto-content-labelers` with
/// `applied` (or `applied_for` the first URI asked, when it is there; an
/// empty value sends no header). Slingshot and everything else: NotFound.
#[derive(Default)]
struct Fake {
    threads: Mutex<HashMap<String, Value>>,
    posts: Mutex<HashMap<String, Value>>,
    applied: Mutex<String>,
    applied_for: Mutex<HashMap<String, String>>,
    down: AtomicBool,
    thread_calls: AtomicUsize,
    /// The URIs of each `getPosts` call.
    asked: Mutex<Vec<Vec<String>>>,
}

async fn fake_appview(fake: Arc<Fake>) -> String {
    let app = AxumRouter::new()
        .route(
            "/xrpc/{method}",
            get(|State(fake): State<Arc<Fake>>, Path(method): Path<String>, RawQuery(query): RawQuery| async move {
                let query = query.unwrap_or_default();
                let asked: Vec<String> = url::form_urlencoded::parse(query.as_bytes())
                    .filter(|(k, _)| k == "uris" || k == "uri")
                    .map(|(_, v)| v.into_owned())
                    .collect();
                let applied = asked
                    .first()
                    .and_then(|u| fake.applied_for.lock().expect("applied").get(u).cloned())
                    .unwrap_or_else(|| fake.applied.lock().expect("applied").clone());
                let body = match method.as_str() {
                    "app.bsky.feed.getPostThread" => {
                        fake.thread_calls.fetch_add(1, Ordering::SeqCst);
                        let thread = asked.first().and_then(|u| fake.threads.lock().expect("threads").get(u).cloned());
                        match thread {
                            Some(t) if !fake.down.load(Ordering::SeqCst) => json!({"thread": t}),
                            _ => return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "NotFound", "message": "Post not found"}))).into_response(),
                        }
                    }
                    "app.bsky.feed.getPosts" => {
                        fake.asked.lock().expect("asked").push(asked.clone());
                        let posts = fake.posts.lock().expect("posts");
                        json!({"posts": asked.iter().filter_map(|u| posts.get(u).cloned()).collect::<Vec<_>>()})
                    }
                    _ => return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "RecordNotFound"}))).into_response(),
                };
                let mut response = axum::Json(body).into_response();
                if !applied.is_empty() {
                    response.headers_mut().insert("atproto-content-labelers", applied.parse().expect("ascii"));
                }
                response
            }),
        )
        .with_state(fake);
    serve_fake(app).await
}

/// The production upstream over the fake, with `cache` as its read-time
/// cache.
fn real_upstream(base: &str, cache: ReadCache) -> RealUpstreamSource {
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
    RealUpstreamSource::new(
        DidRouter::new(Arc::clone(&table), Arc::new(StubLookup::default()) as Arc<dyn DidLookup>),
        Cascade::new(table, clients, &Shares::for_tests()),
        SlingshotClient::new(net, base),
        PdsLister::new(crate::net::Transports::loopback_for_tests(), HostBuckets::new(&Shares::for_tests(), 200.0, 1000)),
        cache,
    )
}

fn at(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

/// An upstream `postView`, replying to `parent` when given.
fn post_view(uri: &str, parent: Option<&str>, embed: Option<Value>) -> Value {
    let did = crate::db::locks::uri_author(uri).expect("did");
    let mut record = json!({"$type": "app.bsky.feed.post", "text": format!("text of {uri}"), "createdAt": "2026-10-26T00:00:00Z"});
    if let Some(p) = parent {
        record["reply"] = json!({"parent": {"uri": p, "cid": "c"}, "root": {"uri": p, "cid": "c"}});
    }
    let mut v = json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy",
        "author": {"did": did, "handle": "h.test", "viewer": {}, "labels": []},
        "record": record, "indexedAt": "2026-10-26T00:00:00Z", "viewer": {}, "labels": [],
        "replyCount": 0, "repostCount": 0, "likeCount": 0, "quoteCount": 0});
    if let Some(e) = embed {
        v["embed"] = e;
    }
    v
}

/// An upstream `#viewRecord` of `uri`.
fn view_record(uri: &str) -> Value {
    let did = crate::db::locks::uri_author(uri).expect("did");
    json!({"$type": "app.bsky.embed.record#viewRecord", "uri": uri, "cid": "bafyq",
        "author": {"did": did, "handle": "q.test", "viewer": {}, "labels": []},
        "value": {"$type": "app.bsky.feed.post", "text": "quoted", "createdAt": "2026-10-25T00:00:00Z"},
        "labels": [], "indexedAt": "2026-10-25T00:00:00Z"})
}

fn quote(record: Value) -> Value {
    json!({"$type": "app.bsky.embed.record#view", "record": record})
}

fn tvp(post: Value, replies: Vec<Value>) -> Value {
    json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post, "replies": replies})
}

/// The URIs of the nodes marked whole.
fn marked(nodes: &[LensNode]) -> Vec<String> {
    let mut out: Vec<String> = nodes.iter().filter(|n| n.embed_whole()).map(|n| n.uri().to_string()).collect();
    out.sort();
    out
}

fn sorted(mut uris: Vec<String>) -> Vec<String> {
    uris.sort();
    uris
}

/// T-Q1, T-Q3, T-Q5, T-Q8, T-Q11, T-Q13, T-Q16 (threadlens-quoted-posts
/// design §2–§4), over HTTP: an upstream thread's quote posts are marked
/// when their cards are whole, with Prism's own labeler accepted and not
/// echoed; the quoted post's self-label is on its card; one
/// `getPostThread` serves collect and emit. Without the header, with a
/// labeler the upstream leaves out, or with the thread served from the
/// Slingshot fallback, nothing is marked, and the fallback's node gets the
/// label batch's labels.
#[tokio::test]
async fn upstream_quote_posts_are_marked_whole() {
    let Some(db) = fresh_db("threadlens_quotes_whole").await else {
        return;
    };
    let p = &db.pool;
    actor(p, VIEWER_A, "a.test", "a.test").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let anchor = at(AUTHOR, "3kanchor2222");
    let (with_media, nested, blocked, not_found, plain) =
        (at(R2, "3kmedia22222"), at(R2, "3knested2222"), at(R2, "3kblocked222"), at(R2, "3knotfound22"), at(R2, "3kplain22222"));
    // The anchor quotes QUOTED's post, which carries a self-label and a
    // label from L1.
    let quoted = at(QUOTED, "3kquoted2222");
    let mut card = view_record(&quoted);
    card["value"]["labels"] = json!({"$type": "com.atproto.label.defs#selfLabels", "values": [{"val": "graphic-media"}]});
    card["labels"] = json!([{"src": L1, "uri": quoted, "val": "quoted-l1", "cts": "2026-10-25T00:00:00Z"}]);
    let mut deeper = view_record(&at(QUOTED, "3kouter22222"));
    deeper["embeds"] = json!([quote(view_record(&at(QUOTED, "3kinner22222")))]);
    let replies = vec![
        tvp(post_view(&with_media, Some(&anchor), Some(json!({"$type": "app.bsky.embed.recordWithMedia#view",
            "media": {"$type": "app.bsky.embed.images#view", "images": []}, "record": quote(view_record(&at(QUOTED, "3kother22222")))}))), vec![]),
        tvp(post_view(&nested, Some(&anchor), Some(quote(deeper))), vec![]),
        tvp(post_view(&blocked, Some(&anchor), Some(quote(view_record(&at(BLOCKER, "3kblocker222"))))), vec![]),
        tvp(post_view(&not_found, Some(&anchor), Some(quote(json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": at(QUOTED, "3kgone222222"), "notFound": true})))), vec![]),
        tvp(post_view(&plain, Some(&anchor), None), vec![]),
    ];
    let anchor_view = post_view(&anchor, None, Some(quote(card)));
    let fake = Arc::new(Fake::default());
    fake.threads.lock().expect("threads").insert(anchor.clone(), tvp(anchor_view.clone(), replies));
    // The label batch's answer for the anchor, read only when the anchor
    // was not fetched with the reader's labelers.
    let mut batch = anchor_view.clone();
    batch["labels"] = json!([{"src": L1, "uri": anchor, "val": "batch-l1", "cts": "2026-10-26T00:00:00Z"}]);
    fake.posts.lock().expect("posts").insert(anchor.clone(), batch);
    *fake.applied.lock().expect("applied") = format!("{L1};redact");
    let base = fake_appview(Arc::clone(&fake)).await;
    let cache = ReadCache::new(1024 * 1024);
    let (farsight, _far) = crate::farsight::lookup::tests::engine("").await;
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{BLOCKER}/app.bsky.graph.block/3kb', '{BLOCKER}', '{VIEWER_A}', '3kaaaaaaaaaa2', now())")).await;
    let h = hydrator_with_upstream_and_farsight(p, ScopeSets::default(), Arc::new(real_upstream(&base, cache.clone())), farsight);
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let url = serve(state(h, l, ThreadLensAccess::Members)).await;
    let get_thread = |accept: Option<String>| {
        let token = mint(&signing_key(3), VIEWER_A, &format!("did:web:{AUDIENCE_HOST}"), Some(GET_THREAD));
        let mut r = reqwest::Client::new()
            .post(format!("{url}/xrpc/{GET_THREAD}"))
            .header("authorization", format!("Bearer {token}"))
            .json(&json!({"anchor": anchor, "depth": 6}));
        if let Some(a) = accept {
            r = r.header("atproto-accept-labelers", a);
        }
        async move {
            let r = r.send().await.expect("request");
            assert_eq!(r.status().as_u16(), 200);
            let content = r.headers().get("atproto-content-labelers").map(|v| v.to_str().expect("ascii").to_string());
            let v: Value = r.json().await.expect("json");
            (content, v)
        }
    };
    let whole_of = |v: &Value| -> Vec<String> {
        sorted(v["nodes"].as_array().expect("nodes").iter().filter(|n| n.get("embedWhole").is_some()).map(|n| {
            assert_eq!(n["embedWhole"], json!(true), "{n}");
            n["uri"].as_str().expect("uri").to_string()
        }).collect())
    };
    let node = |v: &Value, uri: &str| v["nodes"].as_array().expect("nodes").iter().find(|n| n["uri"] == uri).cloned().expect("node");
    let accept = format!("{L1};redact, {LABELER}");

    // T-Q16: Prism's own labeler accepted, not echoed by the upstream; the
    // response names both. T-Q1 / T-Q5: the record quote, the media
    // quote and the blocked author's (replaced by Prism) are marked; the
    // nested quote, the not-found one and the post without a quote are
    // not.
    let (content, v) = get_thread(Some(accept.clone())).await;
    assert_eq!(content.as_deref(), Some(accept.as_str()));
    assert_eq!(whole_of(&v), sorted(vec![anchor.clone(), with_media.clone(), blocked.clone()]), "{v}");
    assert_eq!(node(&v, &blocked)["post"]["embed"]["record"]["$type"], "app.bsky.embed.record#viewBlocked");
    assert_eq!(node(&v, &nested)["post"]["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord");
    // T-Q11: the quoted post's self-label, with its author as `src`, and
    // L1's label are on the marked card.
    let labels = labels_of(&node(&v, &anchor)["post"]["embed"]["record"]);
    assert!(labels.contains(&(QUOTED.to_string(), "graphic-media".to_string())), "{labels:?}");
    assert!(labels.contains(&(L1.to_string(), "quoted-l1".to_string())), "{labels:?}");
    // T-Q13: one `getPostThread` for collect and emit; no label batch, as
    // every node came with the reader's labelers.
    assert_eq!(fake.thread_calls.load(Ordering::SeqCst), 1);
    assert!(fake.asked.lock().expect("asked").is_empty());

    // T-Q3: no header: nothing marked, no response header.
    let (content, v) = get_thread(None).await;
    assert_eq!(content, None);
    assert!(whole_of(&v).is_empty(), "{v}");
    // A labeler the upstream leaves out (R5-5): never applied, nothing
    // marked; the response names what was applied.
    let (content, v) = get_thread(Some(format!("{accept}, {L3}"))).await;
    assert_eq!(content.as_deref(), Some(accept.as_str()));
    assert!(whole_of(&v).is_empty(), "{v}");
    assert!(fake.asked.lock().expect("asked").is_empty());

    // T-Q8: the AppView is down and the anchor's whole view, quote
    // included, is in the Slingshot cache. The thread is served from it:
    // nothing is marked, and the anchor is not taken as labelled, so the
    // label batch gives it L1's label.
    fake.down.store(true, Ordering::SeqCst);
    cache.put_json(&Requester::test(), &format!("rt:post:{anchor}"), &anchor_view).await;
    let (_, v) = get_thread(Some(accept.clone())).await;
    let served = node(&v, &anchor);
    assert_eq!(served["post"]["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord", "{v}");
    assert!(whole_of(&v).is_empty(), "{v}");
    assert!(labels_of(&served["post"]).contains(&(L1.to_string(), "batch-l1".to_string())), "{served}");
    assert_eq!(*fake.asked.lock().expect("asked"), [vec![anchor.clone()]]);
    db.drop_db().await;
}

/// T-Q2, T-Q4, T-Q10, T-Q12, T-Q18 (threadlens-quoted-posts design §3,
/// §4): `resolve` marks a quote post the ladder fetched with `getPosts`
/// (step 3) and leaves a stored one (step 2) unmarked, even when an
/// upstream post quotes it. A label batch whose answer leaves a labeler
/// out narrows the response's header to what every answer applied, and
/// then nothing is marked.
#[tokio::test]
async fn resolve_marks_by_the_ladder_step() {
    let Some(db) = fresh_db("threadlens_quotes_ladder").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R2, "r2.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    // Stored: a post, another quoting it, and a plain post that will need
    // a label batch.
    let stored_quoted = post(p, AUTHOR, "3kstoredq222", "stored quoted", None, None).await;
    let local = post(p, AUTHOR, "3klocalq2222", "local quote", None, None).await;
    exec(p, &format!("UPDATE posts SET quote_uri = '{stored_quoted}', embed = '{{\"$type\": \"app.bsky.embed.record\", \"record\": {{\"uri\": \"{stored_quoted}\", \"cid\": \"bafy3kstoredq222\"}}}}'::jsonb WHERE uri = '{local}'")).await;
    let plain = post(p, AUTHOR, "3kplain22222", "plain", None, None).await;
    // Not stored: QUOTED's post quoting the stored quote post.
    let remote = at(QUOTED, "3kremote2222");
    let fake = Arc::new(Fake::default());
    fake.posts.lock().expect("posts").insert(remote.clone(), post_view(&remote, None, Some(quote(view_record(&local)))));
    *fake.applied.lock().expect("applied") = format!("{L1};redact");
    let base = fake_appview(Arc::clone(&fake)).await;
    let (farsight, _far) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_upstream_and_farsight(p, ScopeSets::default(), Arc::new(real_upstream(&base, ReadCache::new(1024 * 1024))), farsight);
    let l = ladder(h, &base, Arc::new(StubLookup::default()));
    let (req, viewer, key, cfg) = (Requester::test(), member(VIEWER_A), CursorKey::generated(), ThreadLensConfig::default());
    let uris = [local.clone(), remote.clone(), plain.clone()];
    let accept = format!("{L1};redact, {LABELER}");
    let run = || async {
        let scope = LabelerScope::new(AcceptLabelers::parse(&accept).expect("valid"), LABELER);
        let got = with_labelers(Some(Arc::clone(&scope)), thread::resolve(&l, &cfg, &key, &viewer, &req, &uris)).await.expect("resolve");
        (scope, got)
    };

    // Every answer applied L1: the step-3 node is marked, the stored one
    // not, though both carry a whole-looking `#viewRecord` quote.
    let (scope, got) = run().await;
    assert_eq!(marked(&got.nodes), std::slice::from_ref(&remote));
    let LensNode::Post { post: local_view, .. } = &got.nodes[0] else { panic!("{:?}", got.nodes[0]) };
    assert_eq!(local_view["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord", "{local_view}");
    assert_eq!(scope.content_labelers().expect("applied"), accept.as_str());
    assert_eq!(scope.unapplied_answers(), 0);
    // The ladder's `getPosts` for the remote post, and the label batch for
    // the stored ones (the quoted stored post was taken as labelled with
    // the remote one's answer, R5-2).
    let asked = std::mem::take(&mut *fake.asked.lock().expect("asked"));
    assert_eq!(asked, [vec![remote.clone()], vec![plain.clone()]]);

    // T-Q2 / T-Q10: the label batch's answer names no labeler. The header
    // narrows to Prism's own, one answer is counted, nothing is marked.
    fake.applied_for.lock().expect("applied").insert(plain.clone(), String::new());
    let (scope, got) = run().await;
    assert!(marked(&got.nodes).is_empty(), "{:?}", got.nodes);
    assert_eq!(scope.content_labelers().expect("own"), LABELER);
    assert_eq!(scope.unapplied_answers(), 1);
    // A subset: L1 is kept only while every answer names it.
    fake.applied_for.lock().expect("applied").insert(plain.clone(), L3.to_string());
    let (scope, got) = run().await;
    assert!(marked(&got.nodes).is_empty(), "{:?}", got.nodes);
    assert_eq!(scope.content_labelers().expect("own"), LABELER);
    db.drop_db().await;
}

/// T-Q13, T-Q15, T-Q18 (threadlens-quoted-posts design §4): each baseline
/// assembly says which branch it took, from the same memo; a baseline stub
/// replaced by a stored post during bridging is not upstream, one replaced
/// from `getPosts` is.
#[tokio::test]
async fn stubs_and_the_emit_branch_decide_upstream() {
    let Some(db) = fresh_db("threadlens_quotes_stubs").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R2, "r2.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let anchor = at(AUTHOR, "3kanchor2222");
    let quoted = post(p, AUTHOR, "3kstoredq222", "stored quoted", None, None).await;
    // A stored reply quoting a stored post, and one only the AppView has.
    let stored = post(p, R2, "3kstored2222", "stored reply", Some(&anchor), Some(&anchor)).await;
    exec(p, &format!("UPDATE posts SET quote_uri = '{quoted}', embed = '{{\"$type\": \"app.bsky.embed.record\", \"record\": {{\"uri\": \"{quoted}\", \"cid\": \"bafy3kstoredq222\"}}}}'::jsonb WHERE uri = '{stored}'")).await;
    let fetched = at(QUOTED, "3kfetched222");
    let stub = |uri: &str| json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri, "notFound": true});
    let fake = Arc::new(Fake::default());
    let anchor_view = post_view(&anchor, None, Some(quote(view_record(&at(QUOTED, "3kq222222222")))));
    let mut thread = tvp(anchor_view, vec![]);
    thread["replies"] = json!([stub(&stored), stub(&fetched)]);
    fake.threads.lock().expect("threads").insert(anchor.clone(), thread);
    fake.posts.lock().expect("posts").insert(fetched.clone(), post_view(&fetched, Some(&anchor), Some(quote(view_record(&at(QUOTED, "3kq333333333"))))));
    *fake.applied.lock().expect("applied") = format!("{L1};redact");
    let base = fake_appview(Arc::clone(&fake)).await;
    let (farsight, _far) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_upstream_and_farsight(p, ScopeSets::default(), Arc::new(real_upstream(&base, ReadCache::new(1024 * 1024))), farsight);
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    let (req, viewer, key, cfg) = (Requester::test(), member(VIEWER_A), CursorKey::generated(), ThreadLensConfig::default());
    let accept = format!("{L1};redact, {LABELER}");
    let scope = LabelerScope::new(AcceptLabelers::parse(&accept).expect("valid"), LABELER);
    let request = thread::Request { viewer: &viewer, req: &req, anchor: &anchor, depth: 6, parent_height: 80 };
    let got = with_labelers(Some(Arc::clone(&scope)), thread::get_thread(&l, &cfg, &key, request, None)).await.expect("thread");
    // Both stubs became posts; the one from the stored row is not marked,
    // the anchor and the one from `getPosts` are.
    for uri in [&stored, &fetched] {
        let Some(LensNode::Post { post, .. }) = got.nodes.iter().find(|n| n.uri() == uri.as_str()) else {
            panic!("{uri}: {:?}", got.nodes);
        };
        assert_eq!(post["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord", "{post}");
    }
    assert_eq!(marked(&got.nodes), sorted(vec![anchor.clone(), fetched.clone()]));
    assert_eq!(scope.content_labelers().expect("applied"), accept.as_str());

    // T-Q13 / R4-8: from one memo, the first assembly took the cascade
    // branch; once the anchor is stored, the next one takes the local
    // branch and says so, though the memo holds the cascade answer.
    let memo = crate::hydration::thread::ThreadMemo::new();
    let first = h.lens_baseline(&viewer, &req, &anchor, 6, 80, &memo).await.expect("db").expect("tree");
    assert_eq!(first.2, ViewSource::Cascade);
    let calls = fake.thread_calls.load(Ordering::SeqCst);
    post(p, AUTHOR, "3kanchor2222", "now stored", None, None).await;
    let second = h.lens_baseline(&viewer, &req, &anchor, 6, 80, &memo).await.expect("db").expect("tree");
    assert_eq!(second.2, ViewSource::Local);
    assert_eq!(fake.thread_calls.load(Ordering::SeqCst), calls);
    // And getThread now builds the anchor from its row: nothing upstream.
    let scope = LabelerScope::new(AcceptLabelers::parse(&accept).expect("valid"), LABELER);
    let got = with_labelers(Some(scope), thread::get_thread(&l, &cfg, &key, request, None)).await.expect("thread");
    assert!(!marked(&got.nodes).contains(&anchor), "{:?}", got.nodes);
    db.drop_db().await;
}
