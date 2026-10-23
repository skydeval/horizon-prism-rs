//! A latency bench for `getThread` (threadlens-hydration design §1.3): one
//! upstream thread of 46 posts, read by a ready member with Farsight
//! vouching, against fake sources that answer after fixed delays measured
//! from the network (Bluesky's AppView, Constellation, Slingshot,
//! Farsight). It prints the wall time of cold requests (fresh caches),
//! their phases and the calls each source took.
//!
//! The bench is ignored by default. Run it in release mode:
//! `cargo test --release -- --ignored --nocapture threadlens::bench`.
//!
//! The same fixture backs the tests of the latency fixes (design §4, T-1 –
//! T-5), which run in every suite.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Router as AxumRouter;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use serde_json::{Value, json};

use super::cursor::CursorKey;
use super::ladder::Ladder;
use super::thread;
use crate::aggregations::{ScopeSets, ScopeState};
use crate::appview_clients::{AppViewAuth, AppViewClient};
use crate::config::{ConstellationConfig, ScopePolicy, ThreadLensConfig};
use crate::constellation::routing::tests::StubLookup;
use crate::constellation::{Cascade, DidLookup, Router as DidRouter, RoutingTable};
use crate::hydration::testing::{exec, test_config};
use crate::hydration::{Hydrator, HydrationConfig, ReadCache, UpstreamSource, Viewer};
use crate::microcosm::{ConstellationClient, SlingshotClient};
use crate::net::NetClient;
use crate::test_support::{fresh_db, serve_fake};
use crate::xrpc::pools::Requester;

/// How long each fake source holds its answer, in milliseconds.
#[derive(Clone, Copy)]
struct Delays {
    thread: u64,
    posts: u64,
    links: u64,
    slingshot: u64,
    farsight: u64,
}

/// From the network (silonew, 2026-10-23, warm medians; Farsight assumed).
const NETWORK: Delays = Delays { thread: 150, posts: 70, links: 130, slingshot: 290, farsight: 60 };
/// No network at all: what is left is the database and the CPU.
const NONE: Delays = Delays { thread: 0, posts: 0, links: 0, slingshot: 0, farsight: 0 };
const RUNS: usize = 5;

/// A DID Farsight's grammar accepts.
fn did(n: usize) -> String {
    let tail: String = format!("{n:0>4}").bytes().map(|d| char::from(b'a' + (d - b'0'))).collect();
    format!("did:plc:{}{tail}", "x".repeat(20))
}

fn uri(author: &str, rkey: &str) -> String {
    format!("at://{author}/app.bsky.feed.post/{rkey}")
}

fn view(u: &str, parent: Option<&str>, root: &str, replies: u64) -> Value {
    let author = crate::db::locks::uri_author(u).unwrap_or_default();
    let mut record = json!({"$type": "app.bsky.feed.post", "text": u, "createdAt": "2026-09-26T00:00:00Z"});
    if let Some(parent) = parent {
        record["reply"] = json!({"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}});
    }
    json!({"$type": "app.bsky.feed.defs#postView", "uri": u, "cid": "bafy",
           "author": {"did": author, "handle": "h.test", "labels": []},
           "record": record, "indexedAt": "2026-09-26T00:00:00Z", "labels": [],
           "replyCount": replies, "likeCount": 3, "repostCount": 1, "quoteCount": 0})
}

/// The fixture: the anchor (a reply to the root), 20 replies to it, of
/// which the first 8 have 2 replies each. Constellation lists those, and
/// 4 replies to the anchor and 1 to each of the first 4 that the upstream
/// thread does not hold: 8 bridged posts.
struct Fixture {
    anchor: String,
    thread: Value,
    /// Target → listed reply URIs.
    listed: HashMap<String, Vec<String>>,
    /// Post views `getPosts` answers.
    posts: HashMap<String, Value>,
}

fn fixture() -> Fixture {
    let op = did(1);
    let root = uri(&op, "3kroot");
    let anchor = uri(&op, "3kanchor");
    let mut listed: HashMap<String, Vec<String>> = HashMap::new();
    let mut posts = HashMap::new();
    let mut replies = Vec::new();
    for i in 0..20 {
        let r = uri(&did(100 + i), "3kreply");
        let mut node = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&r, Some(&anchor), &root, if i < 8 { 2 } else { 0 })});
        listed.entry(anchor.clone()).or_default().push(r.clone());
        if i < 8 {
            let mut kids = Vec::new();
            for j in 0..2 {
                let k = uri(&did(200 + i * 2 + j), "3kdeep");
                kids.push(json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&k, Some(&r), &root, 0), "replies": []}));
                listed.entry(r.clone()).or_default().push(k);
            }
            if i < 4 {
                let extra = uri(&did(300 + i), "3kextra");
                posts.insert(extra.clone(), view(&extra, Some(&r), &root, 0));
                listed.entry(r.clone()).or_default().push(extra);
            }
            node["replies"] = Value::Array(kids);
        } else {
            node["replies"] = json!([]);
        }
        replies.push(node);
    }
    for i in 0..4 {
        let extra = uri(&did(400 + i), "3kextra");
        posts.insert(extra.clone(), view(&extra, Some(&anchor), &root, 0));
        listed.entry(anchor.clone()).or_default().push(extra);
    }
    let thread = json!({"thread": {"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&anchor, Some(&root), &root, 24),
        "parent": {"$type": "app.bsky.feed.defs#threadViewPost", "post": view(&root, None, &root, 1)},
        "replies": replies}});
    Fixture { anchor, thread, listed, posts }
}

#[derive(Default)]
struct Calls {
    thread: AtomicUsize,
    posts: AtomicUsize,
    links: AtomicUsize,
    slingshot: AtomicUsize,
}

/// The baseline's upstream: `getPostThread` after its delay.
struct SlowUpstream {
    thread: Value,
    calls: Arc<Calls>,
    delay: u64,
}

#[async_trait::async_trait]
impl UpstreamSource for SlowUpstream {
    async fn profile(&self, _req: &Requester, _actor: &str) -> Option<Value> {
        None
    }
    async fn posts(&self, _req: &Requester, _uris: &[String]) -> Vec<Value> {
        Vec::new()
    }
    async fn post_thread(&self, _req: &Requester, _uri: &str, _depth: u32, _parent_height: u32) -> Option<Value> {
        self.calls.thread.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(self.delay)).await;
        Some(self.thread.clone())
    }
    async fn author_feed(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>, _filter: &str) -> Option<Value> {
        None
    }
    async fn follows(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
}

type Shared = (Arc<Calls>, Arc<HashMap<String, Vec<String>>>, Arc<HashMap<String, Value>>, Delays);

/// One fake for the AppView's `getPosts`, Slingshot and Constellation.
async fn sources(calls: Arc<Calls>, f: &Fixture, d: Delays) -> String {
    let state: Shared = (calls, Arc::new(f.listed.clone()), Arc::new(f.posts.clone()), d);
    let app = AxumRouter::new()
        .route(
            "/xrpc/{method}",
            get(|State((calls, _, posts, d)): State<Shared>, Path(method): Path<String>, axum::extract::RawQuery(q): axum::extract::RawQuery| async move {
                let query = q.unwrap_or_default();
                if method == "com.atproto.repo.getRecord" {
                    calls.slingshot.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(d.slingshot)).await;
                    return (axum::http::StatusCode::BAD_REQUEST, axum::Json(json!({"error": "RecordNotFound"})));
                }
                calls.posts.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(d.posts)).await;
                let found: Vec<Value> = url::form_urlencoded::parse(query.as_bytes())
                    .filter(|(k, _)| k == "uris")
                    .filter_map(|(_, v)| posts.get(v.as_ref()).cloned())
                    .collect();
                (axum::http::StatusCode::OK, axum::Json(json!({"posts": found})))
            }),
        )
        .route(
            "/links",
            get(|State((calls, listed, _, d)): State<Shared>, Query(q): Query<HashMap<String, String>>| async move {
                calls.links.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(d.links)).await;
                let records: Vec<Value> = listed
                    .get(q.get("target").map(String::as_str).unwrap_or_default())
                    .into_iter()
                    .flatten()
                    .map(|u| {
                        let parts: Vec<&str> = u.trim_start_matches("at://").split('/').collect();
                        json!({"did": parts[0], "collection": parts[1], "rkey": parts[2]})
                    })
                    .collect();
                axum::Json(json!({"total": records.len(), "linking_records": records}))
            }),
        )
        .with_state(state);
    serve_fake(app).await
}

fn ladder(hydrator: Hydrator, base: &str) -> Ladder {
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
        DidRouter::new(Arc::clone(&table), Arc::new(StubLookup::default()) as Arc<dyn DidLookup>),
        Cascade::new(table, clients, &crate::xrpc::pools::Shares::for_tests()),
        SlingshotClient::new(net, base),
    )
}

/// One cold setup over the fixture: fresh caches, a fresh Farsight.
struct Setup {
    ladder: Ladder,
    calls: Arc<Calls>,
    farsight: Arc<crate::farsight::lookup::tests::Fake>,
}

async fn setup(p: &sqlx::PgPool, f: &Fixture, d: Delays, viewer_did: &str) -> Setup {
    let calls = Arc::new(Calls::default());
    let base = sources(Arc::clone(&calls), f, d).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    *fake.delay_ms.lock().expect("delay") = d.farsight;
    let mut sets = ScopeSets::default();
    sets.members.insert(viewer_did.to_string());
    let h = Hydrator::new(
        p.clone(),
        ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000),
        Some(ConstellationClient::new(NetClient::loopback_for_tests(), &base)),
        None,
        Arc::new(SlowUpstream { thread: f.thread.clone(), calls: Arc::clone(&calls), delay: d.thread }),
        ReadCache::new(64 * 1024 * 1024),
        HydrationConfig { farsight: Some(farsight), ..test_config() },
    );
    crate::hydration::testing::set_mode(h.scope(), crate::indexing::PublicMode::Off).await;
    Setup { ladder: ladder(h, &base), calls, farsight: fake }
}

impl Setup {
    async fn read(&self, f: &Fixture, viewer_did: &str, cfg: &ThreadLensConfig) -> thread::Thread {
        let viewer = Viewer { did: Some(viewer_did.to_string()), hydrate: true };
        let req = Requester::test();
        let request = thread::Request { viewer: &viewer, req: &req, anchor: &f.anchor, depth: 6, parent_height: 80 };
        thread::get_thread(&self.ladder, cfg, &CursorKey::generated(), request, None).await.expect("thread")
    }

    /// `(getPostThread, getPosts, links, slingshot, farsight)` calls so far.
    fn counts(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.calls.thread.load(Ordering::SeqCst),
            self.calls.posts.load(Ordering::SeqCst),
            self.calls.links.load(Ordering::SeqCst),
            self.calls.slingshot.load(Ordering::SeqCst),
            self.farsight.calls.load(Ordering::SeqCst),
        )
    }
}

async fn member_db(name: &str) -> Option<(crate::test_support::TestDb, String)> {
    let db = fresh_db(name).await?;
    let viewer_did = did(9);
    exec(&db.pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{viewer_did}', 'admin', now())")).await;
    Some((db, viewer_did))
}

#[tokio::test]
#[ignore = "a latency bench: run with --release --ignored"]
async fn lens_latency_bench() {
    bench("network", NETWORK).await;
}

#[tokio::test]
#[ignore = "a latency bench: run with --release --ignored"]
async fn lens_latency_bench_no_network() {
    bench("no network", NONE).await;
}

async fn bench(label: &str, d: Delays) {
    let Some((db, viewer_did)) = member_db(&format!("threadlens_bench_{}", d.thread)).await else {
        return;
    };
    let f = fixture();
    let mut times = Vec::new();
    let mut counts = Vec::new();
    let mut phases: HashMap<&'static str, Vec<u128>> = HashMap::new();
    let mut order: Vec<&'static str> = Vec::new();
    for _ in 0..RUNS {
        let s = setup(&db.pool, &f, d, &viewer_did).await;
        let started = Instant::now();
        let t = s.read(&f, &viewer_did, &ThreadLensConfig::default()).await;
        times.push(started.elapsed());
        assert_eq!(t.nodes.len(), 46, "the whole fixture, bridged posts included");
        for (phase, took) in &t.phases {
            if !order.contains(phase) {
                order.push(phase);
            }
            phases.entry(phase).or_default().push(took.as_millis());
        }
        counts.push(s.counts());
    }
    times.sort();
    let ms: Vec<u128> = times.iter().map(Duration::as_millis).collect();
    println!("lens bench ({label}): {RUNS} cold runs, ms sorted {ms:?}, median {} ms", ms[RUNS / 2]);
    let medians: Vec<String> = order
        .iter()
        .map(|phase| {
            let mut v = phases[phase].clone();
            v.sort_unstable();
            format!("{phase} {}", v[v.len() / 2])
        })
        .collect();
    println!("lens bench ({label}): phase medians ms: {}", medians.join(", "));
    println!("lens bench ({label}): calls per run (getPostThread, getPosts, links, slingshot, farsight) {:?}", counts[0]);
    db.drop_db().await;
}

/// The URIs of `nodes`, in order.
fn uris(nodes: &[crate::hydration::lens::LensNode]) -> Vec<String> {
    nodes.iter().map(|n| n.uri().to_string()).collect()
}

/// T-1, T-2, T-3 (design §4): the upstream thread is fetched once; each
/// level's bridged posts take one `getPosts`; the nodes come in the order
/// the request always gave them: the parent chain, the anchor, the
/// baseline's replies level by level, then each level's bridged replies in
/// level order.
#[tokio::test]
async fn the_fixture_is_read_with_one_thread_fetch_and_one_get_posts_per_level() {
    let Some((db, viewer_did)) = member_db("threadlens_fixture_calls").await else {
        return;
    };
    let f = fixture();
    let s = setup(&db.pool, &f, NONE, &viewer_did).await;
    let t = s.read(&f, &viewer_did, &ThreadLensConfig::default()).await;
    let op = did(1);
    let mut want = vec![uri(&op, "3kroot"), f.anchor.clone()];
    want.extend((0..20).map(|i| uri(&did(100 + i), "3kreply")));
    for i in 0..8 {
        want.extend((0..2).map(|j| uri(&did(200 + i * 2 + j), "3kdeep")));
    }
    want.extend((0..4).map(|i| uri(&did(400 + i), "3kextra")));
    want.extend((0..4).map(|i| uri(&did(300 + i), "3kextra")));
    assert_eq!(uris(&t.nodes), want);
    // Each bridged reply sits under the parent it was listed for.
    for i in 0..4 {
        let node = t.nodes.iter().find(|n| n.uri() == uri(&did(300 + i), "3kextra")).expect("bridged");
        assert_eq!(node.parent_uri(), Some(uri(&did(100 + i), "3kreply").as_str()));
    }
    assert_eq!(t.bridging, crate::hydration::lens::Bridging::Full);
    // One thread fetch; one `getPosts` per level that bridges; one page per
    // node with replies; one Farsight round per step.
    assert_eq!(s.counts(), (1, 2, 9, 0, 3));
    assert!(t.nodes.iter().all(|n| !n.markers().any()), "nothing left to read");
    db.drop_db().await;
}

/// T-2 (design §4): one level's pages are read together, not one after
/// another. Nine pages of 300 ms each took 2.7 s end to end; read a level
/// at a time they take two pages' time.
#[tokio::test]
async fn a_levels_pages_are_read_together() {
    let Some((db, viewer_did)) = member_db("threadlens_fixture_pages").await else {
        return;
    };
    let f = fixture();
    let slow_pages = Delays { links: 300, ..NONE };
    let s = setup(&db.pool, &f, slow_pages, &viewer_did).await;
    let started = Instant::now();
    let t = s.read(&f, &viewer_did, &ThreadLensConfig::default()).await;
    let took = started.elapsed();
    assert_eq!(t.nodes.len(), 46);
    assert!(took < Duration::from_millis(1500), "{took:?}");
    db.drop_db().await;
}

/// T-5 (design §4, F5): a page slower than `listing_timeout_ms` leaves the
/// node's marker with its cursor and nothing bridged under it; the page
/// still lands in the read-time cache, so the next request reads every
/// page without a call and bridges everything.
#[tokio::test]
async fn a_slow_page_is_left_behind_its_marker_and_cached_for_the_next_request() {
    let Some((db, viewer_did)) = member_db("threadlens_fixture_slow").await else {
        return;
    };
    let f = fixture();
    // Pages far slower than the bound, so even the anchor's page, asked for
    // at the start (F7), is not in by the time the first level waits.
    let s = setup(&db.pool, &f, Delays { links: 2000, ..NONE }, &viewer_did).await;
    let cfg = ThreadLensConfig { listing_timeout_ms: 100, ..ThreadLensConfig::default() };
    let t = s.read(&f, &viewer_did, &cfg).await;
    assert_eq!(t.nodes.len(), 38, "the baseline only: no page was waited for");
    let anchor = t.nodes.iter().find(|n| n.uri() == f.anchor).expect("anchor");
    assert!(matches!(&anchor.markers().more_replies, Some(Some(_))), "the anchor keeps a cursor: {:?}", anchor.markers());
    let pages = s.counts().2;
    assert_eq!(pages, 9);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let again = s.read(&f, &viewer_did, &cfg).await;
    assert_eq!(again.nodes.len(), 46, "every page was cached in the background");
    assert_eq!(s.counts().2, pages, "no page was asked for again");
    db.drop_db().await;
}

/// T-3 (design §4, F3): `resolve_many` gives each URI what `resolve` gives
/// it alone: a private URI, a post the gate hides, a deleted post, a stored
/// post, an upstream post, a content-stripped one and one nobody has; the
/// upstream posts take one `getPosts` together.
#[tokio::test]
async fn resolve_many_answers_each_uri_as_resolve_does() {
    use super::gaps::Gap;
    use super::ladder::{Mode, Resolved};
    use crate::hydration::testing::post;

    let Some((db, viewer_did)) = member_db("threadlens_resolve_many").await else {
        return;
    };
    let p = &db.pool;
    let mut f = fixture();
    let (author, outsider) = (did(10), did(11));
    crate::hydration::testing::actor(p, &author, "author.test", "Author").await;
    let local = post(p, &author, "3klocal", "stored", None, None).await;
    let hidden = uri(&outsider, "3khidden");
    exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{hidden}', '!takedown')")).await;
    let deleted = uri(&outsider, "3kdeleted");
    exec(p, &format!("INSERT INTO record_tombstones (uri, rev) VALUES ('{deleted}', '3k')")).await;
    let upstream = uri(&outsider, "3kupstream");
    f.posts.insert(upstream.clone(), view(&upstream, None, &upstream, 0));
    let stripped = uri(&outsider, "3kstripped");
    let mut empty = view(&stripped, None, &stripped, 0);
    empty["record"] = json!({"$type": "app.bsky.feed.post"});
    f.posts.insert(stripped.clone(), empty);
    let missing = uri(&outsider, "3kmissing");
    let private = format!("at://{outsider}/tools.kryphocron.feed.postPrivate/3kpriv");
    let all = vec![private.clone(), hidden.clone(), deleted.clone(), local.clone(), upstream.clone(), stripped.clone(), missing.clone()];
    let s = setup(p, &f, NONE, &viewer_did).await;
    // `resolve` reads the author's row through the scope: put the author
    // in it, as the fixture's member is.
    s.ladder.hydrator().scope().write_guard().await.scope.insert(author.clone());
    let viewer = Viewer { did: Some(viewer_did.clone()), hydrate: true };
    let req = Requester::test();
    let many = s.ladder.resolve_many(&viewer, &req, &all, Mode::Full).await.expect("many");
    assert_eq!(s.counts().1, 1, "one getPosts for the two upstream posts and the missing one");
    for u in &all {
        let one = s.ladder.resolve(&viewer, &req, u, Mode::Full).await.expect("one");
        assert_eq!(many.get(u), Some(&one), "{u}");
    }
    assert_eq!(many[&private], Resolved::Gap(Gap::TierHidden));
    assert_eq!(many[&hidden], Resolved::Gap(Gap::NotFound));
    assert_eq!(many[&deleted], Resolved::Gap(Gap::Deleted));
    assert!(matches!(many[&local], Resolved::Post(_)), "{:?}", many[&local]);
    assert!(matches!(many[&upstream], Resolved::Post(_)), "{:?}", many[&upstream]);
    assert_eq!(many[&stripped], Resolved::Gap(Gap::ContentStripped));
    assert!(matches!(many[&missing], Resolved::Gap(_)), "{:?}", many[&missing]);
    db.drop_db().await;
}
