//! Thread Lens on the thread outline (thread outline design §5, §7): T-O7 –
//! T-O13, and the S2 parts of T-O3 and T-O14. Each test writes its outline
//! rows through the S1 writer's flush, with the handle set fresh and the
//! threads complete.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router as AxumRouter;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use axum::routing::get;
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};

use super::super::cursor::{CursorKey, Position};
use super::super::outline::{Check, Reader, Use};
use super::super::thread::{self, Thread};
use super::{member, shape_of};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::{ScopePolicy, ThreadLensConfig, ThreadOutlineConfig};
use crate::constellation::routing::tests::StubLookup;
use crate::farsight::lookup::tests::{Fake, V, engine};
use crate::hydration::lens::{Bridging, LensNode, lens_thread};
use crate::hydration::testing::{FixedUpstream, actor, exec, post, test_config};
use crate::hydration::{HydrationConfig, Hydrator, ReadCache, UpstreamSource};
use crate::microcosm::ConstellationClient;
use crate::net::NetClient;
use crate::test_support::{TestDb, fresh_db, serve_fake};
use crate::thread_outline::flush::{Progress, apply};
use crate::thread_outline::id::StreamId;
use crate::thread_outline::ops::{HoleReason, Key, Op, PostOp, tid_string, tid_value};
use crate::xrpc::pools::Requester;

const OP: &str = "did:plc:oooooooooooooooooooooooo";
const A1: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const A2: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const A3: &str = "did:plc:cccccccccccccccccccccccc";
/// The viewer blocks it.
const BLK: &str = "did:plc:kkkkkkkkkkkkkkkkkkkkkkkk";
/// The viewer blocks it too; the outline reports it gone.
const BLK2: &str = "did:plc:qqqqqqqqqqqqqqqqqqqqqqqq";
/// Farsight vouches for it; the outline reports it gone.
const GONE: &str = "did:plc:gggggggggggggggggggggggg";
/// A DID Farsight's grammar refuses: never vouched for, so undetermined.
const UND: &str = "did:plc:uuuuuuuuuuuuuuuuuuuuuuu1";

/// The fake AppView (`getPosts`), Slingshot (`getRecord`) and
/// Constellation (`/links`), counting their calls.
#[derive(Default)]
struct Sources {
    posts: Mutex<HashMap<String, Value>>,
    listed: Mutex<HashMap<String, Vec<String>>>,
    /// How long Thread Lens's own listing pages are held.
    links_delay_ms: AtomicU64,
    get_posts: AtomicUsize,
    /// Thread Lens's own reply listing pages (`limit=100`): the ordinary
    /// view's baseline lists replies too, 50 at a time, the same on either
    /// path, and is not counted.
    links: AtomicUsize,
    slingshot: AtomicUsize,
}

impl Sources {
    /// `(getPosts, Thread Lens listing pages, slingshot)` calls since the
    /// last call, reset.
    fn take(&self) -> (usize, usize, usize) {
        (self.get_posts.swap(0, Ordering::SeqCst), self.links.swap(0, Ordering::SeqCst), self.slingshot.swap(0, Ordering::SeqCst))
    }
}

async fn serve_sources(s: Arc<Sources>) -> String {
    let app = AxumRouter::new()
        .route(
            "/xrpc/{method}",
            get(|State(s): State<Arc<Sources>>, Path(method): Path<String>, RawQuery(q): RawQuery| async move {
                if method == "com.atproto.repo.getRecord" {
                    s.slingshot.fetch_add(1, Ordering::SeqCst);
                    return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "RecordNotFound"})));
                }
                s.get_posts.fetch_add(1, Ordering::SeqCst);
                let query = q.unwrap_or_default();
                let posts = s.posts.lock().expect("posts");
                let found: Vec<Value> =
                    url::form_urlencoded::parse(query.as_bytes()).filter(|(k, _)| k == "uris").filter_map(|(_, v)| posts.get(v.as_ref()).cloned()).collect();
                (StatusCode::OK, axum::Json(json!({"posts": found})))
            }),
        )
        .route(
            "/links",
            get(|State(s): State<Arc<Sources>>, Query(q): Query<HashMap<String, String>>| async move {
                // Reply listings only: anything else links to nothing.
                if q.get("path").map(String::as_str) != Some(".reply.parent.uri") {
                    return axum::Json(json!({"total": 0, "linking_records": []}));
                }
                if q.get("limit").map(String::as_str) == Some("100") {
                    s.links.fetch_add(1, Ordering::SeqCst);
                    let delay = s.links_delay_ms.load(Ordering::SeqCst);
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                }
                let listed = s.listed.lock().expect("listed").get(q.get("target").map(String::as_str).unwrap_or_default()).cloned().unwrap_or_default();
                let records: Vec<Value> = listed
                    .iter()
                    .map(|u| {
                        let parts: Vec<&str> = u.trim_start_matches("at://").split('/').collect();
                        json!({"did": parts[0], "collection": parts[1], "rkey": parts[2]})
                    })
                    .collect();
                axum::Json(json!({"total": records.len(), "linking_records": records}))
            }),
        )
        .with_state(s);
    serve_fake(app).await
}

/// A post view an upstream AppView gives, with no `replyCount`.
fn remote_view(uri: &str, parent: Option<&str>, root: &str) -> Value {
    let author = crate::db::locks::uri_author(uri).unwrap_or_default();
    let mut record = json!({"$type": "app.bsky.feed.post", "text": format!("remote {uri}"), "createdAt": "2026-10-26T00:00:00Z"});
    if let Some(parent) = parent {
        record["reply"] = json!({"parent": {"uri": parent, "cid": "c"}, "root": {"uri": root, "cid": "c"}});
    }
    json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafy", "author": {"did": author, "handle": "h.test", "labels": []},
           "record": record, "indexedAt": "2026-10-26T00:00:00Z", "labels": []})
}

/// The outline post a URI names.
fn key(uri: &str) -> Key {
    let did = crate::db::locks::uri_author(uri).expect("did");
    Key { did: did.to_string(), rkey: tid_value(uri.rsplit('/').next().expect("rkey")).expect("tid") }
}

fn reader(cfg: ThreadOutlineConfig) -> Reader {
    Reader::fresh_for_tests(cfg)
}

fn fresh() -> Reader {
    reader(ThreadOutlineConfig::default())
}

/// One request's Thread Lens: a ladder over fresh caches and a fresh
/// Farsight (or none), against the world's sources.
struct Lens {
    ladder: crate::threadlens::Ladder,
    farsight: Option<Arc<Fake>>,
    up: Arc<FixedUpstream>,
}

impl Lens {
    /// Hold Farsight's answers `ms`, so F7 is in before the candidates form.
    fn slow_farsight(&self, ms: u64) {
        *self.farsight.as_ref().expect("farsight").delay_ms.lock().expect("delay") = ms;
    }

    fn farsight_calls(&self) -> usize {
        self.farsight.as_ref().map_or(0, |f| f.calls.swap(0, Ordering::SeqCst))
    }

    /// Whether any record was read through the upstream (a blocked post's
    /// parent link).
    fn record_reads(&self) -> Vec<String> {
        self.up.take_modes().into_iter().map(|(call, _)| call).filter(|c| c.starts_with("record ")).collect()
    }

    async fn read(&self, outline: Option<&Reader>, anchor: &str, depth: u32, parent_height: u32, cursor: Option<(Position, String)>, cfg: &ThreadLensConfig) -> Thread {
        let viewer = member(V);
        let req = Requester::test();
        let r = thread::Request { viewer: &viewer, req: &req, anchor, depth, parent_height };
        thread::get_thread_outlined(&self.ladder, cfg, &cursor_key(), outline, r, cursor).await.expect("thread")
    }
}

fn cursor_key() -> CursorKey {
    CursorKey::new(&[9; 32])
}

/// A database with the accounts, the viewer a ready member that blocks
/// `BLK` and `BLK2`, the fake sources, and a connection for outline flushes.
struct World {
    db: TestDb,
    /// The time the posts' TIDs and outline rows carry: an hour ago.
    t0: DateTime<Utc>,
    conn: PgConnection,
    seq: u64,
    sources: Arc<Sources>,
    base: String,
}

impl World {
    async fn new(name: &str) -> Option<Self> {
        let db = fresh_db(name).await?;
        let p = &db.pool;
        for (did, handle) in [(V, "v.test"), (OP, "op.test"), (A1, "a1.test"), (A2, "a2.test"), (A3, "a3.test"), (BLK, "blk.test"), (BLK2, "blk2.test"), (GONE, "gone.test"), (UND, "und.test")] {
            actor(p, did, handle, handle).await;
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{V}', 'admin', now())")).await;
        for (i, blocked) in [BLK, BLK2].into_iter().enumerate() {
            exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{V}/app.bsky.graph.block/3kb{i}', '{V}', '{blocked}', '3kaaaaaaaaaa2', now())")).await;
        }
        let conn = PgConnection::connect_with(&p.connect_options()).await.expect("connect");
        let sources = Arc::new(Sources::default());
        let base = serve_sources(Arc::clone(&sources)).await;
        Some(Self { db, t0: Utc::now() - TimeDelta::hours(1), conn, seq: 0, sources, base })
    }

    /// The `n`th record key: a TID of an hour ago.
    fn rk(&self, n: u64) -> String {
        tid_string((self.t0.timestamp_micros() << 10) + i64::try_from(n).expect("n"))
    }

    fn at(&self, did: &str, n: u64) -> String {
        format!("at://{did}/app.bsky.feed.post/{}", self.rk(n))
    }

    /// A post stored here, with a reply count so its replies are read.
    async fn stored(&self, did: &str, n: u64, text: &str, parent: Option<&str>, root: Option<&str>) -> String {
        let uri = post(&self.db.pool, did, &self.rk(n), text, parent, root).await;
        exec(&self.db.pool, &format!("INSERT INTO post_counts (post_uri, reply_count) VALUES ('{uri}', 1)")).await;
        uri
    }

    /// A post an upstream AppView answers for.
    fn remote(&self, uri: &str, parent: Option<&str>, root: &str) {
        self.sources.posts.lock().expect("posts").insert(uri.to_string(), remote_view(uri, parent, root));
    }

    fn list(&self, target: &str, uris: &[&String]) {
        self.sources.listed.lock().expect("listed").insert(target.to_string(), uris.iter().map(|u| (*u).clone()).collect());
    }

    /// A create op, each later than the last.
    fn create(&mut self, uri: &str, parent: Option<&str>, root: Option<&str>) -> Op {
        self.seq += 1;
        let k = key(uri);
        Op::Create(PostOp {
            author: k.did,
            rkey: k.rkey,
            parent: parent.map(key),
            root: root.map(key),
            created_at: self.t0 + TimeDelta::seconds(i64::try_from(self.seq).expect("seq")),
            rev: 1,
        })
    }

    /// Flush `ops` as the writer does, each stamped an hour ago.
    async fn flush(&mut self, ops: Vec<Op>) {
        let ms = u64::try_from(self.t0.timestamp_millis()).expect("ms");
        let stamped: Vec<(StreamId, Op)> = ops
            .into_iter()
            .map(|op| {
                self.seq += 1;
                (StreamId::new(ms, self.seq), op)
            })
            .collect();
        apply(&mut self.conn, &stamped, &Progress::default()).await.expect("flush");
    }

    /// Outline rows for `(uri, parent)` pairs of one thread rooted at
    /// `root` (a `None` parent is the root itself).
    async fn rows(&mut self, root: &str, posts: &[(&String, Option<&String>)]) {
        let ops: Vec<Op> = posts.iter().map(|(uri, parent)| self.create(uri, parent.map(String::as_str), parent.map(|_| root))).collect();
        self.flush(ops).await;
    }

    fn delete(uri: &str) -> Op {
        let k = key(uri);
        Op::Delete { author: k.did, rkey: k.rkey, rev: 2 }
    }

    async fn hole(&mut self, uri: &str) {
        self.flush(vec![Op::Hole { node: key(uri), reason: HoleReason::LeftOut }]).await;
    }

    async fn lens(&self, with_farsight: bool) -> Lens {
        let up = Arc::new(FixedUpstream::default());
        let (farsight, fake) = if with_farsight {
            let (f, fake) = engine("").await;
            (Some(f), Some(fake))
        } else {
            (None, None)
        };
        let h = Hydrator::new(
            self.db.pool.clone(),
            ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000),
            Some(ConstellationClient::new(NetClient::loopback_for_tests(), &self.base)),
            None,
            Arc::clone(&up) as Arc<dyn UpstreamSource>,
            ReadCache::new(1 << 20),
            HydrationConfig { farsight, ..test_config() },
        );
        Lens { ladder: super::ladder(h, &self.base, Arc::new(StubLookup::default())), farsight: fake, up }
    }
}

fn uris(t: &Thread) -> Vec<String> {
    t.nodes.iter().map(|n| n.uri().to_string()).collect()
}

fn node<'a>(t: &'a Thread, uri: &str) -> Option<&'a LensNode> {
    t.nodes.iter().find(|n| n.uri() == uri)
}

/// T-O7: with a tree, a thread with blocks (in the chain and among the
/// replies), a deleted post, stored replies (an undetermined author's own
/// row among them) and bridged replies two levels deep is the response
/// today's path gives, node for node and view for view, from far fewer
/// calls: Constellation for F7 only, no record read for a blocked parent's
/// link, the baseline round plus one candidate round (and enforcement's,
/// with nothing embedded, none), and one `fetch_many` for the parents and
/// the replies together.
#[tokio::test]
async fn t_o7_the_tree_serves_todays_thread_with_fewer_calls() {
    let Some(mut w) = World::new("to7_tree").await else {
        return;
    };
    // root (OP, upstream only) ← m (BLK, stored, blocked) ← anchor (OP, stored)
    //   anchor ← s1 (A1, stored), s2 (UND, stored), b1 (BLK, stored, blocked),
    //            x1 (A1, bridged), x2 (A2, bridged), d1 (A1, deleted)
    //   b1 ← c1 (A2, stored), u1 (UND, stored: an own row)
    //   x1 ← x3 (A3, bridged)
    let root = w.at(OP, 1);
    let m = w.stored(BLK, 2, "secret m", Some(&root), Some(&root)).await;
    let anchor = w.stored(OP, 3, "anchor", Some(&m), Some(&root)).await;
    let s1 = w.stored(A1, 4, "s1", Some(&anchor), Some(&root)).await;
    let s2 = w.stored(UND, 5, "s2", Some(&anchor), Some(&root)).await;
    let b1 = w.stored(BLK, 6, "secret b1", Some(&anchor), Some(&root)).await;
    let c1 = w.stored(A2, 7, "c1", Some(&b1), Some(&root)).await;
    let u1 = w.stored(UND, 8, "u1", Some(&b1), Some(&root)).await;
    let (x1, x2, x3, d1) = (w.at(A1, 9), w.at(A2, 10), w.at(A3, 11), w.at(A1, 12));
    w.remote(&root, None, &root);
    for (x, parent) in [(&x1, &anchor), (&x2, &anchor), (&x3, &x1)] {
        w.remote(x, Some(parent), &root);
    }
    w.list(&anchor, &[&s1, &s2, &b1, &x1, &x2]);
    w.list(&b1, &[&c1, &u1]);
    w.list(&x1, &[&x3]);
    w.rows(
        &root,
        &[(&root, None), (&m, Some(&root)), (&anchor, Some(&m)), (&s1, Some(&anchor)), (&s2, Some(&anchor)), (&b1, Some(&anchor)), (&c1, Some(&b1)), (&u1, Some(&b1)), (&x1, Some(&anchor)), (&x2, Some(&anchor)), (&x3, Some(&x1))],
    )
    .await;
    let made = w.create(&d1, Some(&anchor), Some(&root));
    w.flush(vec![made, World::delete(&d1)]).await;
    let cfg = ThreadLensConfig::default();

    let today = w.lens(true).await;
    w.sources.take();
    let before = today.read(None, &anchor, 6, 80, None, &cfg).await;
    let today_calls = (w.sources.take(), today.farsight_calls());
    let lens = w.lens(true).await;
    lens.slow_farsight(300);
    let outline = fresh();
    let got = lens.read(Some(&outline), &anchor, 6, 80, None, &cfg).await;
    let tree_calls = (w.sources.take(), lens.farsight_calls());

    // Today's path, with no outline: counted `off`.
    assert_eq!((got.outline, got.crosscheck, before.outline), (Some(Use::Tree), Some(Check::Match), Some(Use::Off)));
    // The same response: nodes, gaps, links, views and markers.
    let emit = |t: &Thread| serde_json::to_value(lens_thread(&anchor, &t.nodes, t.bridging, true)).expect("json");
    assert_eq!(emit(&got), emit(&before));
    assert_eq!(got.bridging, Bridging::Full);
    let want: Vec<(String, Option<&str>)> = vec![
        (root.clone(), None),
        (m.clone(), Some("blocked-by-you")),
        (anchor.clone(), None),
        (s1.clone(), None),
        (s2.clone(), None),
        (b1.clone(), Some("blocked-by-you")),
        (x1.clone(), None),
        (x2.clone(), None),
        (c1.clone(), None),
        (u1.clone(), None),
        (x3.clone(), None),
    ];
    assert_eq!(shape_of(&got.nodes), want);
    assert_eq!(node(&got, &m).and_then(LensNode::parent_uri), Some(root.as_str()));
    assert!(node(&got, &d1).is_none());
    // Calls: (getPosts, links, slingshot), Farsight.
    assert_eq!(tree_calls, ((1, 1, 0), 2), "F7 only, one fetch, the baseline round and one candidate round");
    assert!(lens.record_reads().is_empty());
    let ((today_posts, today_links, _), today_farsight) = today_calls;
    assert!(today_posts >= 3 && today_links >= 4 && today_farsight >= 3, "today's path: {today_calls:?}");
    w.db.drop_db().await;
}

/// T-O8: wholeness. An orphan in the thread, under its own root or under
/// the root a mis-rooted reply names, a hole on the walk (even above
/// `parentHeight`) or in the region, and a stored reply with no outline row
/// indexed well before `through` each give `hole`. A create and delete
/// folded in one flush leave a deleted row, so its children are no
/// orphans; the deleted post is not placed, nor anything under it. A newer
/// stored reply with no row is placed as a stored reply.
#[tokio::test]
async fn t_o8_wholeness() {
    let Some(mut w) = World::new("to8_wholeness").await else {
        return;
    };
    let cfg = ThreadLensConfig::default();
    let lens = w.lens(true).await;
    let outline = fresh();

    // An interior reply with no row and live children: an orphan.
    let r1 = w.stored(OP, 1, "r1", None, None).await;
    let (missing, orphan) = (w.at(A1, 2), w.at(A2, 3));
    let made = w.create(&orphan, Some(&missing), Some(&r1));
    w.rows(&r1, &[(&r1, None)]).await;
    w.flush(vec![made]).await;
    assert_eq!(lens.read(Some(&outline), &r1, 6, 80, None, &cfg).await.outline, Some(Use::Hole));

    // A mis-rooted reply in the region: its own root's thread has an orphan.
    let r2 = w.stored(OP, 10, "r2", None, None).await;
    let (other_root, misrooted, stray, gap) = (w.at(A1, 11), w.at(A2, 12), w.at(A3, 13), w.at(A1, 14));
    w.rows(&r2, &[(&r2, None)]).await;
    let ops = vec![w.create(&misrooted, Some(&r2), Some(&other_root)), w.create(&other_root, None, None), w.create(&stray, Some(&gap), Some(&other_root))];
    w.flush(ops).await;
    assert_eq!(lens.read(Some(&outline), &r2, 6, 80, None, &cfg).await.outline, Some(Use::Hole));

    // A create and delete in one flush: a deleted row, so the child below
    // it is no orphan; neither is placed.
    let r3 = w.stored(OP, 20, "r3", None, None).await;
    let (folded, below, beside) = (w.at(A1, 21), w.at(A2, 22), w.at(A3, 23));
    w.rows(&r3, &[(&r3, None)]).await;
    let ops = vec![w.create(&folded, Some(&r3), Some(&r3)), World::delete(&folded), w.create(&below, Some(&folded), Some(&r3)), w.create(&beside, Some(&r3), Some(&r3))];
    w.flush(ops).await;
    for (x, parent) in [(&folded, &r3), (&below, &folded), (&beside, &r3)] {
        w.remote(x, Some(parent), &r3);
    }
    let t = lens.read(Some(&outline), &r3, 6, 80, None, &cfg).await;
    assert_eq!(t.outline, Some(Use::Tree));
    assert_eq!(uris(&t), [r3.clone(), beside.clone()], "the deleted post is not placed or walked");

    // A stored reply with no outline row: on its way while it is newer than
    // `through` less the margin; a miss once it is older.
    let r4 = w.stored(OP, 30, "r4", None, None).await;
    w.rows(&r4, &[(&r4, None)]).await;
    let late = w.stored(A1, 31, "late", Some(&r4), Some(&r4)).await;
    let t = lens.read(Some(&outline), &r4, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, uris(&t)), (Some(Use::Tree), vec![r4.clone(), late.clone()]));
    // Its own replies are not known to the reads: the client opens it.
    assert_eq!(node(&t, &late).map(|n| n.markers().more_replies.clone()), Some(Some(None)));
    exec(&w.db.pool, &format!("UPDATE posts SET indexed_at = now() - interval '10 minutes' WHERE uri = '{late}'")).await;
    assert_eq!(lens.read(Some(&outline), &r4, 6, 80, None, &cfg).await.outline, Some(Use::Hole));

    // A hole at the root, with the chain longer than `parentHeight`.
    let r5 = w.at(OP, 40);
    let (e1, e2) = (w.at(A1, 41), w.at(A2, 42));
    let e3 = w.stored(OP, 43, "e3", Some(&e2), Some(&r5)).await;
    w.rows(&r5, &[(&r5, None), (&e1, Some(&r5)), (&e2, Some(&e1)), (&e3, Some(&e2))]).await;
    assert_eq!(lens.read(Some(&outline), &e3, 6, 1, None, &cfg).await.outline, Some(Use::Tree));
    w.hole(&r5).await;
    assert_eq!(lens.read(Some(&outline), &e3, 6, 1, None, &cfg).await.outline, Some(Use::Hole));

    // A hole in the region.
    let r6 = w.stored(OP, 50, "r6", None, None).await;
    let c6 = w.at(A1, 51);
    w.rows(&r6, &[(&r6, None), (&c6, Some(&r6))]).await;
    assert_eq!(lens.read(Some(&outline), &r6, 6, 80, None, &cfg).await.outline, Some(Use::Tree));
    w.hole(&c6).await;
    assert_eq!(lens.read(Some(&outline), &r6, 6, 80, None, &cfg).await.outline, Some(Use::Hole));
    w.db.drop_db().await;
}

/// T-O9: a region over `max_tree` rows, and a thread over `max_thread`,
/// give `too_big`; the next request skips the reads (the rows are gone and
/// it is still `too_big`).
#[tokio::test]
async fn t_o9_too_big_is_remembered() {
    let Some(mut w) = World::new("to9_too_big").await else {
        return;
    };
    let cfg = ThreadLensConfig::default();
    let lens = w.lens(true).await;

    let small_tree = reader(ThreadOutlineConfig { max_tree: 2, ..ThreadOutlineConfig::default() });
    let r1 = w.stored(OP, 1, "r1", None, None).await;
    let kids: Vec<String> = (2..5).map(|n| w.at(A1, n)).collect();
    let mut rows: Vec<(&String, Option<&String>)> = vec![(&r1, None)];
    rows.extend(kids.iter().map(|k| (k, Some(&r1))));
    w.rows(&r1, &rows).await;
    assert_eq!(lens.read(Some(&small_tree), &r1, 6, 80, None, &cfg).await.outline, Some(Use::TooBig));
    exec(&w.db.pool, "DELETE FROM outline_posts WHERE parent_author IS NOT NULL").await;
    assert_eq!(lens.read(Some(&small_tree), &r1, 6, 80, None, &cfg).await.outline, Some(Use::TooBig), "remembered");
    assert_eq!(lens.read(Some(&fresh()), &r1, 6, 80, None, &cfg).await.outline, Some(Use::Tree), "per process");

    let small_thread = reader(ThreadOutlineConfig { max_thread: 2, ..ThreadOutlineConfig::default() });
    let r2 = w.at(OP, 10);
    let anchor = w.stored(OP, 11, "anchor", Some(&r2), Some(&r2)).await;
    let (c1, c2, c3) = (w.at(A1, 12), w.at(A2, 13), w.at(A3, 14));
    w.rows(&r2, &[(&r2, None), (&anchor, Some(&r2)), (&c1, Some(&anchor)), (&c2, Some(&c1)), (&c3, Some(&c2))]).await;
    assert_eq!(lens.read(Some(&small_thread), &anchor, 1, 80, None, &cfg).await.outline, Some(Use::TooBig));
    let below: Vec<i64> = [&c1, &c2, &c3].iter().map(|u| key(u).rkey).collect();
    sqlx::query("DELETE FROM outline_posts WHERE rkey = ANY($1)").bind(&below).execute(&w.db.pool).await.expect("delete");
    assert_eq!(lens.read(Some(&small_thread), &anchor, 1, 80, None, &cfg).await.outline, Some(Use::TooBig), "the root is remembered");
    assert_eq!(lens.read(Some(&fresh()), &anchor, 1, 80, None, &cfg).await.outline, Some(Use::Tree), "per process");
    w.db.drop_db().await;
}

/// T-O10: cursors. A node cut within its stored replies gets `Local`, and
/// its continuation reads the rest from the tree, then the outline-only
/// children; cut there it gets `Outline`, whose continuation reads the rest
/// with the levels below from the node's region. A failed check, or bridging
/// off, reads an `Outline` cursor as `Listed { cursor: None }`. Old cursors
/// open as today.
#[tokio::test]
async fn t_o10_cursors() {
    let Some(mut w) = World::new("to10_cursors").await else {
        return;
    };
    // anchor (OP) ← g (BLK, blocked) ← s1, s2 (A1, stored), x1, x2 (A2,
    // bridged); x2 ← x4 (A3, bridged).
    let anchor = w.stored(OP, 1, "anchor", None, None).await;
    let g = w.stored(BLK, 2, "secret g", Some(&anchor), Some(&anchor)).await;
    let s1 = w.stored(A1, 3, "s1", Some(&g), Some(&anchor)).await;
    let s2 = w.stored(A1, 4, "s2", Some(&g), Some(&anchor)).await;
    let (x1, x2, x4) = (w.at(A2, 5), w.at(A2, 6), w.at(A3, 7));
    w.rows(&anchor, &[(&anchor, None), (&g, Some(&anchor)), (&s1, Some(&g)), (&s2, Some(&g)), (&x1, Some(&g)), (&x2, Some(&g)), (&x4, Some(&x2))]).await;
    for (x, parent) in [(&x1, &g), (&x2, &g), (&x4, &x2)] {
        w.remote(x, Some(parent), &anchor);
    }
    w.list(&g, &[&s1, &s2, &x1, &x2]);
    w.list(&x2, &[&x4]);
    let lens = w.lens(true).await;
    let outline = fresh();
    let key = cursor_key();
    let cfg = ThreadLensConfig { page_nodes: 2, ..ThreadLensConfig::default() };
    let marker = |t: &Thread, uri: &str| node(t, uri).and_then(|n| n.markers().more_replies.clone());

    let first = lens.read(Some(&outline), &anchor, 6, 80, None, &cfg).await;
    assert_eq!((first.outline, uris(&first)), (Some(Use::Tree), vec![anchor.clone(), g.clone(), s1.clone()]));
    let cursor = marker(&first, &g).flatten().expect("a cursor on g");
    let local: Position = key.open(&g, &cursor).expect("opens");
    assert!(matches!(&local, Position::Local { uri, .. } if *uri == s1), "{local:?}");
    w.sources.take();

    let second = lens.read(Some(&outline), &g, 6, 0, Some((local, cursor)), &cfg).await;
    assert_eq!((second.outline, uris(&second)), (Some(Use::Tree), vec![g.clone(), s2.clone(), x1.clone()]));
    let cursor = marker(&second, &g).flatten().expect("a cursor on g");
    let at_x1: Position = key.open(&g, &cursor).expect("opens");
    assert!(matches!(&at_x1, Position::Outline { uri, .. } if *uri == x1), "{at_x1:?}");

    // From `Outline`, stored children sorting after the position are read
    // too (S2 review finding 3): s1 and s2 were stored after x1 was made,
    // so they are sent again, which the client merges by URI.
    let wide = ThreadLensConfig { page_nodes: 4, ..ThreadLensConfig::default() };
    let third = lens.read(Some(&outline), &g, 6, 0, Some((at_x1.clone(), cursor.clone())), &wide).await;
    assert_eq!(
        (third.outline, uris(&third)),
        (Some(Use::Tree), vec![g.clone(), x2.clone(), s1.clone(), s2.clone(), x4.clone()]),
        "the level below from the region"
    );
    assert_eq!(marker(&third, &g), None);
    assert_eq!(w.sources.take().1, 0, "no listing read");

    // A failed check: the listing from its start.
    w.hole(&x4).await;
    let failed = lens.read(Some(&outline), &g, 6, 0, Some((at_x1.clone(), cursor.clone())), &cfg).await;
    assert_eq!(failed.outline, Some(Use::Hole));
    assert!(uris(&failed).contains(&x1), "{:?}", uris(&failed));
    assert!(w.sources.take().1 > 0, "the listing was read");

    // Bridging off: nothing can be read under a gap; the cursor is kept.
    let off = w.lens(true).await;
    *off.farsight.as_ref().expect("farsight").status.lock().expect("status") = 503;
    let t = off.read(Some(&outline), &g, 6, 0, Some((at_x1, cursor.clone())), &cfg).await;
    assert_eq!((t.bridging, t.outline, uris(&t)), (Bridging::Off, Some(Use::BridgingOff), vec![g.clone()]));
    assert_eq!(marker(&t, &g), Some(Some(cursor)));

    // An old cursor, in the listing's phase: today's path.
    let listed = Position::Listed { cursor: None };
    let sealed = key.seal(&g, &listed).expect("sealed");
    let lens = w.lens(true).await;
    let t = lens.read(Some(&outline), &g, 6, 0, Some((listed, sealed)), &cfg).await;
    assert_eq!(t.outline, None);
    assert!(uris(&t).contains(&x1));
    w.db.drop_db().await;
}

/// T-O11: the chain. A blocked post in it is a gap with its link from the
/// walk, and nothing of its record is read; a gate-hidden chain post ends
/// the walk; an outline-only reply the ladder cannot find is skipped; a
/// mis-rooted anchor finds its root by the walk; a cycle is `hole`; a
/// missing old ancestor is `incomplete`.
#[tokio::test]
async fn t_o11_the_chain() {
    let Some(mut w) = World::new("to11_chain").await else {
        return;
    };
    let cfg = ThreadLensConfig::default();
    let lens = w.lens(true).await;
    let outline = fresh();

    // root (OP) ← m1 (BLK, not stored) ← v (OP) ← m2 (BLK, stored) ← anchor.
    let root = w.at(OP, 1);
    let m1 = w.at(BLK, 2);
    let v = w.at(OP, 3);
    let m2 = w.stored(BLK, 4, "secret m2", Some(&v), Some(&root)).await;
    let anchor = w.stored(OP, 5, "anchor", Some(&m2), Some(&root)).await;
    w.remote(&root, None, &root);
    w.remote(&v, Some(&m1), &root);
    w.rows(&root, &[(&root, None), (&m1, Some(&root)), (&v, Some(&m1)), (&m2, Some(&v)), (&anchor, Some(&m2))]).await;
    let t = lens.read(Some(&outline), &anchor, 6, 80, None, &cfg).await;
    assert_eq!(t.outline, Some(Use::Tree));
    let want = [(root.clone(), None), (m1.clone(), Some("blocked-by-you")), (v.clone(), None), (m2.clone(), Some("blocked-by-you")), (anchor.clone(), None)];
    assert_eq!(shape_of(&t.nodes), want);
    assert_eq!(node(&t, &m1).and_then(LensNode::parent_uri), Some(root.as_str()));
    assert!(lens.record_reads().is_empty(), "no record read for a link");
    let emitted = serde_json::to_string(&lens_thread(&anchor, &t.nodes, t.bridging, true)).expect("json");
    assert!(!emitted.contains("secret") && !emitted.contains("blk.test"), "{emitted}");

    // The same chain with v hidden by moderation: the walk ends at v.
    exec(&w.db.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{v}', '!takedown')")).await;
    let t = lens.read(Some(&outline), &anchor, 6, 80, None, &cfg).await;
    assert_eq!(shape_of(&t.nodes), [(v.clone(), Some("not-found")), (m2.clone(), Some("blocked-by-you")), (anchor.clone(), None)]);

    // An outline-only reply the ladder cannot find: skipped, not a gap.
    let r2 = w.stored(OP, 10, "r2", None, None).await;
    let (hidden, shown) = (w.at(A1, 11), w.at(A2, 12));
    w.rows(&r2, &[(&r2, None), (&hidden, Some(&r2)), (&shown, Some(&r2))]).await;
    w.remote(&shown, Some(&r2), &r2);
    exec(&w.db.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{hidden}', '!takedown')")).await;
    let t = lens.read(Some(&outline), &r2, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, uris(&t)), (Some(Use::Tree), vec![r2.clone(), shown.clone()]));

    // A mis-rooted anchor: its record names another root; the walk finds
    // its own.
    let r3 = w.at(OP, 20);
    let named = w.at(A3, 21);
    let q = w.stored(A1, 22, "q", Some(&r3), Some(&named)).await;
    w.remote(&r3, None, &r3);
    let ops = vec![w.create(&r3, None, None), w.create(&q, Some(&r3), Some(&named))];
    w.flush(ops).await;
    let t = lens.read(Some(&outline), &q, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, uris(&t)), (Some(Use::Tree), vec![r3.clone(), q.clone()]));

    // A cycle.
    let (c1, c2) = (w.at(A1, 30), w.at(A2, 31));
    let ops = vec![w.create(&c1, Some(&c2), Some(&c2)), w.create(&c2, Some(&c1), Some(&c1))];
    w.flush(ops).await;
    w.remote(&c1, Some(&c2), &c2);
    assert_eq!(lens.read(Some(&outline), &c1, 6, 80, None, &cfg).await.outline, Some(Use::Hole));

    // A missing old ancestor.
    let (gone_parent, e) = (w.at(A1, 40), w.at(A2, 41));
    let ops = vec![w.create(&e, Some(&gone_parent), Some(&gone_parent))];
    w.flush(ops).await;
    w.remote(&e, Some(&gone_parent), &gone_parent);
    assert_eq!(lens.read(Some(&outline), &e, 6, 80, None, &cfg).await.outline, Some(Use::Incomplete));
    w.db.drop_db().await;
}

/// T-O12: `round_prefix` judges the longest prefix whose new authors fit
/// the allowance less the enforcement reserve, never refuses the whole set
/// (where `round` would), and an unanswered round leaves bridging on and
/// its authors undetermined.
#[tokio::test]
async fn t_o12_the_candidate_round() {
    use crate::threadlens::verdict::{Judge, Verdict};
    use crate::xrpc::pools::ChargeKey;

    let Some(w) = World::new("to12_round").await else {
        return;
    };
    let p = &w.db.pool;
    let (farsight, fake) = engine("").await;
    let lookup = || Some(farsight.request(&Requester::new(ChargeKey::member(V))));
    let authors: Vec<String> = (0..8).map(|i| format!("did:plc:{}{}", "z".repeat(23), char::from(b'a' + i))).collect();
    let posts: Vec<String> = authors.iter().map(|a| format!("at://{a}/app.bsky.feed.post/3kaaaaaaaaaa2")).collect();
    let cfg = ThreadLensConfig { verdict_max_dids: 10, ..ThreadLensConfig::default() };

    // An allowance of 10 with a reserve of 4: six new authors are judged,
    // and four are left for enforcement.
    let mut judge = Judge::new(p.clone(), V, lookup(), &cfg);
    judge.baseline(&[OP.to_string()]).await.expect("baseline");
    let covered = judge.round_prefix(&posts, 4).await.expect("round");
    assert_eq!((covered, judge.dids_left()), (6, 4));
    assert_eq!(judge.verdict(&authors[5]), Some(Verdict::Clear));
    assert_eq!(judge.verdict(&authors[6]), None);
    // A reserve over half the allowance is capped at half.
    let mut judge = Judge::new(p.clone(), V, lookup(), &cfg);
    judge.baseline(&[OP.to_string()]).await.expect("baseline");
    assert_eq!(judge.round_prefix(&posts, 100).await.expect("round"), 5);

    // Where `round` refuses all eight, `round_prefix` judges a prefix.
    let small = ThreadLensConfig { verdict_max_dids: 5, ..ThreadLensConfig::default() };
    let mut judge = Judge::new(p.clone(), V, lookup(), &small);
    judge.baseline(&[OP.to_string()]).await.expect("baseline");
    assert!(!judge.round(&authors).await.expect("round"));
    assert_eq!(judge.round_prefix(&posts, 0).await.expect("round"), 5);
    // Posts by authors already judged are covered with no allowance left.
    assert_eq!(judge.round_prefix(&posts, 0).await.expect("round"), 5);

    // Farsight stops answering after the baseline: the round's authors are
    // undetermined, and bridging stays on.
    let mut judge = Judge::new(p.clone(), V, lookup(), &cfg);
    judge.baseline(&[OP.to_string()]).await.expect("baseline");
    *fake.status.lock().expect("status") = 503;
    let other: Vec<String> = (0..2).map(|i| format!("did:plc:{}{}", "y".repeat(23), char::from(b'a' + i))).collect();
    let other_posts: Vec<String> = other.iter().map(|a| format!("at://{a}/app.bsky.feed.post/3kaaaaaaaaaa2")).collect();
    assert_eq!(judge.round_prefix(&other_posts, 0).await.expect("round"), 2);
    assert!(judge.bridging());
    assert_eq!(judge.verdict(&other[0]), Some(Verdict::Undetermined));
    w.db.drop_db().await;
}

/// T-O13: the cross-check. A listed reply that is neither stored nor in the
/// outline gives `crosscheck` and today's path, which places it; one newer
/// than `through` less the margin is `pending`, not placed; a deleted
/// outline row is no miss; F7 not in by the time the candidates form is
/// `late`, and the request does not wait for it.
#[tokio::test]
async fn t_o13_the_cross_check() {
    let Some(mut w) = World::new("to13_crosscheck").await else {
        return;
    };
    let cfg = ThreadLensConfig::default();
    let outline = fresh();
    let lens = w.lens(true).await;
    lens.slow_farsight(300);

    // A miss.
    let r1 = w.stored(OP, 1, "r1", None, None).await;
    let (inside, missed) = (w.at(A1, 2), w.at(A2, 3));
    w.rows(&r1, &[(&r1, None), (&inside, Some(&r1))]).await;
    w.remote(&inside, Some(&r1), &r1);
    w.remote(&missed, Some(&r1), &r1);
    w.list(&r1, &[&inside, &missed]);
    let t = lens.read(Some(&outline), &r1, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, t.crosscheck), (Some(Use::Crosscheck), Some(Check::Miss)));
    assert!(uris(&t).contains(&missed), "today's path places it");

    // On its way: a TID of now.
    let r2 = w.stored(OP, 10, "r2", None, None).await;
    let new = format!("at://{A2}/app.bsky.feed.post/{}", tid_string(Utc::now().timestamp_micros() << 10));
    w.rows(&r2, &[(&r2, None)]).await;
    w.remote(&new, Some(&r2), &r2);
    w.list(&r2, &[&new]);
    let t = lens.read(Some(&outline), &r2, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, t.crosscheck, uris(&t)), (Some(Use::Tree), Some(Check::Pending), vec![r2.clone()]));

    // A deleted outline row is no miss.
    let r3 = w.stored(OP, 20, "r3", None, None).await;
    let deleted = w.at(A1, 21);
    w.rows(&r3, &[(&r3, None)]).await;
    let made = w.create(&deleted, Some(&r3), Some(&r3));
    w.flush(vec![made, World::delete(&deleted)]).await;
    w.remote(&deleted, Some(&r3), &r3);
    w.list(&r3, &[&deleted]);
    let t = lens.read(Some(&outline), &r3, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, t.crosscheck, uris(&t)), (Some(Use::Tree), Some(Check::Match), vec![r3.clone()]));

    // Late: F7 is slow; the miss it would show is not waited for.
    w.sources.links_delay_ms.store(3000, Ordering::SeqCst);
    let lens = w.lens(true).await;
    let started = Instant::now();
    let t = lens.read(Some(&outline), &r1, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, t.crosscheck), (Some(Use::Tree), Some(Check::Late)));
    assert!(started.elapsed() < Duration::from_millis(2500), "{:?}", started.elapsed());
    assert!(!uris(&t).contains(&missed));
    w.db.drop_db().await;
}

/// T-O3 (S2): a gone author's outline-only reply is fetched like any
/// other; a gone blocked author's is skipped, where a blocked author that
/// is not gone is a gap.
#[tokio::test]
async fn t_o3_gone_authors() {
    let Some(mut w) = World::new("to3_gone").await else {
        return;
    };
    let r = w.stored(OP, 1, "r", None, None).await;
    let (by_gone, by_gone_blocked, by_blocked) = (w.at(GONE, 2), w.at(BLK2, 3), w.at(BLK, 4));
    w.rows(&r, &[(&r, None), (&by_gone, Some(&r)), (&by_gone_blocked, Some(&r)), (&by_blocked, Some(&r))]).await;
    w.remote(&by_gone, Some(&r), &r);
    let ops: Vec<Op> = [GONE, BLK2].into_iter().map(|did| Op::Status { did: did.to_string(), relay: "bsky".into(), active: false, seq: 1 }).collect();
    w.flush(ops).await;
    let lens = w.lens(true).await;
    let t = lens.read(Some(&fresh()), &r, 6, 80, None, &ThreadLensConfig::default()).await;
    assert_eq!(t.outline, Some(Use::Tree));
    assert_eq!(shape_of(&t.nodes), [(r.clone(), None), (by_gone.clone(), None), (by_blocked.clone(), Some("blocked-by-you"))]);
    w.db.drop_db().await;
}

/// T-O14 (S2): disabled, inactive or stale, or no Farsight: no reads and
/// today's response, which does not hold an outline-only reply nobody
/// lists. Bridging off reads nothing either. With the outline fresh the
/// same request places it.
#[tokio::test]
async fn t_o14_no_reads_without_the_outline() {
    let Some(mut w) = World::new("to14_off").await else {
        return;
    };
    let r = w.stored(OP, 1, "r", None, None).await;
    let only = w.at(A1, 2);
    w.rows(&r, &[(&r, None), (&only, Some(&r))]).await;
    w.remote(&only, Some(&r), &r);
    let cfg = ThreadLensConfig::default();
    let lens = w.lens(true).await;

    let inactive = fresh();
    inactive.outline().shared().set_active(false);
    let stale = reader(ThreadOutlineConfig { fresh_secs: 30, ..ThreadOutlineConfig::default() });
    stale.outline().shared().set_through_ms(Utc::now().timestamp_millis() - 3_600_000);
    for (outline, why) in [(None, Use::Off), (Some(&inactive), Use::Off), (Some(&stale), Use::Stale)] {
        let t = lens.read(outline, &r, 6, 80, None, &cfg).await;
        assert_eq!((t.outline, uris(&t)), (Some(why), vec![r.clone()]), "{why:?}");
    }
    assert_eq!((inactive.reads(), stale.reads()), (0, 0), "no outline read ran");
    let bare = w.lens(false).await;
    let unread = fresh();
    let t = bare.read(Some(&unread), &r, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, t.bridging, uris(&t)), (None, Bridging::Off, vec![r.clone()]));
    assert_eq!(unread.reads(), 0, "no outline read ran without Farsight");
    let off = w.lens(true).await;
    *off.farsight.as_ref().expect("farsight").status.lock().expect("status") = 503;
    let t = off.read(Some(&fresh()), &r, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, t.bridging, uris(&t)), (Some(Use::BridgingOff), Bridging::Off, vec![r.clone()]));

    let used = fresh();
    let t = lens.read(Some(&used), &r, 6, 80, None, &cfg).await;
    assert_eq!((t.outline, uris(&t), used.reads()), (Some(Use::Tree), vec![r.clone(), only.clone()], 1));
    w.db.drop_db().await;
}

/// Finding 1 of the S2 review: a deleted post in the chain is placed as
/// today's walk places it, and ends the chain. A `Clear` author's is what
/// the ladder answers; a blocked author's is its gap, with no link above
/// it. The response is today's, view for view.
#[tokio::test]
async fn t_o11_a_deleted_chain_post_is_placed_as_today() {
    let Some(mut w) = World::new("to11_deleted_chain").await else {
        return;
    };
    let cfg = ThreadLensConfig::default();
    for (author, base) in [(A1, 0), (BLK2, 100)] {
        // root ← d (deleted) ← v ← m (BLK, stored) ← anchor.
        let root = w.at(OP, base + 1);
        let d = w.at(author, base + 2);
        let v = w.at(OP, base + 3);
        let m = w.stored(BLK, base + 4, "secret m", Some(&v), Some(&root)).await;
        let anchor = w.stored(OP, base + 5, "anchor", Some(&m), Some(&root)).await;
        w.remote(&root, None, &root);
        w.remote(&v, Some(&d), &root);
        w.rows(&root, &[(&root, None), (&d, Some(&root)), (&v, Some(&d)), (&m, Some(&v)), (&anchor, Some(&m))]).await;
        w.flush(vec![World::delete(&d)]).await;
        let today = w.lens(true).await.read(None, &anchor, 6, 80, None, &cfg).await;
        let lens = w.lens(true).await;
        let got = lens.read(Some(&fresh()), &anchor, 6, 80, None, &cfg).await;
        assert_eq!(got.outline, Some(Use::Tree), "{author}");
        let emit = |t: &Thread| serde_json::to_value(lens_thread(&anchor, &t.nodes, t.bridging, true)).expect("json");
        assert_eq!(emit(&got), emit(&today), "{author}");
        assert_eq!(uris(&got), [d.clone(), v.clone(), m.clone(), anchor.clone()], "{author}");
        assert!(!matches!(node(&got, &d), Some(LensNode::Post { .. })), "{author}");
        assert!(node(&got, &d).is_some_and(|n| !n.markers().more_parents), "{author}");
        assert!(lens.record_reads().is_empty(), "{author}");
    }
    w.db.drop_db().await;
}

/// Finding 3 of the S2 review: a child stored between two pages is read
/// by the `Outline` cursor's continuation, in its `(created_at, uri)`
/// place, beside the outline-only children after the cursor.
#[tokio::test]
async fn t_o10_a_child_stored_between_pages_is_read() {
    let Some(mut w) = World::new("to10_stored_between").await else {
        return;
    };
    let r = w.stored(OP, 1, "r", None, None).await;
    let (x1, x2, x3) = (w.at(A2, 2), w.at(A2, 3), w.at(A2, 4));
    w.rows(&r, &[(&r, None), (&x1, Some(&r)), (&x2, Some(&r)), (&x3, Some(&r))]).await;
    for x in [&x1, &x2, &x3] {
        w.remote(x, Some(&r), &r);
    }
    let lens = w.lens(true).await;
    let outline = fresh();
    let key = cursor_key();
    let one = ThreadLensConfig { page_nodes: 1, ..ThreadLensConfig::default() };
    let first = lens.read(Some(&outline), &r, 6, 80, None, &one).await;
    assert_eq!(uris(&first), [r.clone(), x1.clone()]);
    let cursor = node(&first, &r).and_then(|n| n.markers().more_replies.clone()).flatten().expect("a cursor");
    let position: Position = key.open(&r, &cursor).expect("opens");
    assert!(matches!(&position, Position::Outline { uri, .. } if *uri == x1), "{position:?}");
    // x2 is stored here before the next page.
    let stored = w.stored(A2, 3, "x2 stored", Some(&r), Some(&r)).await;
    assert_eq!(stored, x2);
    let two = ThreadLensConfig { page_nodes: 2, ..ThreadLensConfig::default() };
    let next = lens.read(Some(&outline), &r, 6, 0, Some((position, cursor)), &two).await;
    assert_eq!(next.outline, Some(Use::Tree));
    assert_eq!(uris(&next), [r.clone(), x3.clone(), x2.clone()], "the stored child in its createdAt place, after x3");
    w.db.drop_db().await;
}

/// Finding 4 of the S2 review: the enforcement reserve is fixed at the
/// first candidate round, so a later slice cannot eat into it, and
/// enforcement can still judge the authors bridged posts embed. Here the
/// allowance is 6 and the reserve 2: the first slice judges 4 authors, the
/// next slice none, and enforcement judges the 2 embedded authors, one of
/// whom the viewer blocks.
#[tokio::test]
async fn t_o12_enforcement_keeps_its_reserve() {
    let Some(mut w) = World::new("to12_reserve").await else {
        return;
    };
    const E: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
    let r = w.stored(OP, 1, "r", None, None).await;
    let authors: Vec<String> = (0..6).map(|i| format!("did:plc:{}{}", "w".repeat(23), char::from(b'a' + i))).collect();
    let kids: Vec<String> = authors.iter().zip(2u64..).map(|(a, n)| w.at(a, n)).collect();
    let mut rows: Vec<(&String, Option<&String>)> = vec![(&r, None)];
    rows.extend(kids.iter().map(|k| (k, Some(&r))));
    w.rows(&r, &rows).await;
    let quote = |did: &str, n: u64| {
        json!({"$type": "app.bsky.embed.record#view", "record": {"$type": "app.bsky.embed.record#viewRecord",
               "uri": format!("at://{did}/app.bsky.feed.post/{}", tid_string(n.try_into().expect("n"))), "cid": "c",
               "author": {"did": did, "handle": "quoted.test"}, "value": {"text": "secret quoted"}}})
    };
    for (i, k) in kids.iter().enumerate() {
        let mut view = remote_view(k, Some(&r), &r);
        if i == 0 {
            view["embed"] = quote(BLK2, 7);
        }
        if i == 1 {
            view["embed"] = quote(E, 8);
        }
        w.sources.posts.lock().expect("posts").insert(k.clone(), view);
    }
    let lens = w.lens(true).await;
    let outline = reader(ThreadOutlineConfig { enforce_reserve: 2, ..ThreadOutlineConfig::default() });
    let cfg = ThreadLensConfig { verdict_max_dids: 6, ..ThreadLensConfig::default() };
    let t = lens.read(Some(&outline), &r, 6, 80, None, &cfg).await;
    assert_eq!(t.outline, Some(Use::Tree));
    assert_eq!(uris(&t), [r.clone(), kids[0].clone(), kids[1].clone(), kids[2].clone(), kids[3].clone()]);
    let Some(LensNode::Post { post, .. }) = node(&t, &kids[0]) else {
        panic!("a post: {:?}", t.nodes);
    };
    assert_eq!(post["embed"]["record"]["$type"], "app.bsky.embed.record#viewBlocked", "{post}");
    let body = serde_json::to_string(&lens_thread(&r, &t.nodes, t.bridging, true)).expect("json");
    assert_eq!(body.matches("secret quoted").count(), 1, "only the unblocked quote's text: {body}");
    w.db.drop_db().await;
}

/// §1.1: bridged replies follow the stored ones in `(created_at, uri)`
/// order with the outline, where today's path takes Constellation's.
#[tokio::test]
async fn t_o7_bridged_replies_take_created_at_order() {
    let Some(mut w) = World::new("to7_order").await else {
        return;
    };
    let r = w.stored(OP, 1, "r", None, None).await;
    let (x1, x2) = (w.at(A1, 2), w.at(A2, 3));
    w.rows(&r, &[(&r, None), (&x1, Some(&r)), (&x2, Some(&r))]).await;
    w.remote(&x1, Some(&r), &r);
    w.remote(&x2, Some(&r), &r);
    w.list(&r, &[&x2, &x1]);
    let cfg = ThreadLensConfig::default();
    let today = w.lens(true).await.read(None, &r, 6, 80, None, &cfg).await;
    assert_eq!(uris(&today), [r.clone(), x2.clone(), x1.clone()], "Constellation's order");
    let tree = w.lens(true).await.read(Some(&fresh()), &r, 6, 80, None, &cfg).await;
    assert_eq!((tree.outline, uris(&tree)), (Some(Use::Tree), vec![r.clone(), x1.clone(), x2.clone()]), "createdAt order");
    w.db.drop_db().await;
}

/// The region read's plan over a sizeable outline (a million rows, a
/// thousand authors): it must walk `outline_posts_parent`. Run with
/// `-- --ignored --nocapture explain_region_read`.
#[tokio::test]
#[ignore = "prints a query plan over a million rows"]
async fn explain_region_read() {
    let Some(db) = fresh_db("to_explain_region").await else {
        return;
    };
    let p = &db.pool;
    exec(p, "INSERT INTO outline_actors (did) SELECT 'did:plc:explain' || g FROM generate_series(1, 1000) g").await;
    // Threads of ten: a root every ten rows, each reply under the row
    // before it, so the region below a root is nine levels deep.
    exec(
        p,
        "INSERT INTO outline_posts (author, rkey, parent_author, parent_rkey, root_author, root_rkey, created_at, seen_at, rev) \
         SELECT (g % 1000) + 1, g, \
                CASE WHEN g % 10 = 0 THEN NULL ELSE ((g - 1) % 1000) + 1 END, CASE WHEN g % 10 = 0 THEN NULL ELSE g - 1 END, \
                CASE WHEN g % 10 = 0 THEN NULL ELSE ((g / 10 * 10) % 1000) + 1 END, CASE WHEN g % 10 = 0 THEN NULL ELSE g / 10 * 10 END, \
                now(), now(), 1 FROM generate_series(10, 1000009) g",
    )
    .await;
    exec(p, "ANALYZE outline_posts").await;
    exec(p, "ANALYZE outline_actors").await;
    let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS) {}", crate::threadlens::outline::REGION_SQL))
        .bind(11_i32)
        .bind(10_i64)
        .bind(6_i32)
        .bind(5001_i64)
        .fetch_all(p)
        .await
        .expect("explain");
    println!("{}", plan.join("\n"));
    assert!(plan.iter().any(|l| l.contains("outline_posts_parent")), "the parent index");
    db.drop_db().await;
}
