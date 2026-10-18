//! The profile tiers, the `getProfiles` batch and the background refresh
//! (design-v4 §8.1; the profile refresh design, tests T-1 – T-28).
//!
//! Fake AppViews answer `getProfiles` from a map and record what each call
//! asked, so a test can count the calls per network and see which DIDs
//! each one named.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::cache::ReadCache;
use super::profile_refresh::{METRIC_BATCH_FETCH, METRIC_COMPLETED, METRIC_DROPPED, METRIC_NEGATIVE, METRIC_QUEUE, ProfileDrain, ProfileQueue};
use super::testing::{FixedUpstream, actor, exec, hydrator_with, hydrator_with_upstream_and_farsight, post};
use super::upstream::{PdsLister, RealUpstreamSource};
use super::{Hydrator, Tiers, UpstreamSource, Viewer};
use crate::aggregations::ScopeSets;
use crate::appview_clients::{AppViewAuth, AppViewClient};
use crate::config::ConstellationConfig;
use crate::constellation::cascade::{METRIC_BUDGET_CUTOFF, METRIC_CALLS};
use crate::constellation::routing::tests::{StubLookup, table_with_community};
use crate::constellation::{Cascade, DidLookup, ProfileWalk, Router as DidRouter, RoutingTable};
use crate::microcosm::SlingshotClient;
use crate::net::{NetClient, Transports};
use crate::test_support::{fresh_db, serve_fake, series};
use crate::xrpc::pools::{HostBuckets, Requester, Shares};

const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const FAR: &str = "did:plc:ffffffffffffffffffffffff";

/// The `i`th out-of-scope DID.
fn far(i: usize) -> String {
    let tail: String = format!("{i:04}").bytes().map(|c| char::from(b'b' + (c - b'0'))).collect();
    format!("did:plc:zzzzzzzzzzzzzzzzzzzz{tail}")
}

/// An upstream `profileViewDetailed` whose `viewer` and `labels` must never
/// reach a response.
fn remote_view(did: &str, handle: &str) -> Value {
    json!({
        "did": did, "handle": handle, "displayName": "Remote", "description": "about", "indexedAt": "2026-01-01T00:00:00.000Z",
        "banner": "https://cdn.example/banner", "followersCount": 3,
        "viewer": {"blockedBy": true, "muted": true}, "labels": [{"val": "upstream"}],
    })
}

/// A fake AppView.
#[derive(Default)]
struct Net {
    name: &'static str,
    profiles: Mutex<HashMap<String, Value>>,
    /// A status to answer `getProfiles` with instead of 200.
    status: Mutex<Option<u16>>,
    delay_ms: Mutex<u64>,
    /// The actors of each `getProfiles` call.
    asked: Mutex<Vec<Vec<String>>>,
    /// Calls of any other method.
    others: AtomicUsize,
    /// A log shared by several fakes: the name of each that was called.
    log: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl Net {
    fn named(name: &'static str) -> Self {
        Self { name, ..Self::default() }
    }

    fn with(self, dids: &[String]) -> Self {
        for d in dids {
            self.profiles.lock().expect("profiles").insert(d.clone(), remote_view(d, &format!("{}.remote", &d[8..14])));
        }
        self
    }

    fn calls(&self) -> usize {
        self.asked.lock().expect("asked").len()
    }

    fn asked(&self) -> Vec<Vec<String>> {
        self.asked.lock().expect("asked").clone()
    }
}

async fn answer(State(net): State<Arc<Net>>, Path(method): Path<String>, RawQuery(q): RawQuery) -> axum::response::Response {
    if method != "app.bsky.actor.getProfiles" {
        net.others.fetch_add(1, Ordering::SeqCst);
        return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "NotFound", "message": "Profile not found"}))).into_response();
    }
    let actors: Vec<String> = url::form_urlencoded::parse(q.unwrap_or_default().as_bytes())
        .filter(|(k, _)| k == "actors")
        .map(|(_, v)| v.into_owned())
        .collect();
    net.asked.lock().expect("asked").push(actors.clone());
    if let Some(log) = &net.log {
        log.lock().expect("log").push(net.name);
    }
    let delay = *net.delay_ms.lock().expect("delay");
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    if let Some(status) = *net.status.lock().expect("status") {
        let status = StatusCode::from_u16(status).expect("status");
        return (status, axum::Json(json!({"error": "NotFound", "message": "Profile not found"}))).into_response();
    }
    let profiles = net.profiles.lock().expect("profiles");
    let found: Vec<Value> = actors.iter().filter_map(|a| profiles.get(a).cloned()).collect();
    axum::Json(json!({ "profiles": found })).into_response()
}

async fn serve(net: Net) -> (Arc<Net>, String) {
    let net = Arc::new(net);
    let base = serve_fake(axum::Router::new().route("/xrpc/{method}", get(answer)).with_state(Arc::clone(&net))).await;
    (net, base)
}

/// A cascade over the default table with these bases, and the request
/// bucket of each named network set to `rate` (`(rate, burst)`).
fn cascade(bases: &[(&str, &str)], rate: Option<(u32, u32)>, extra: Option<(&str, &str)>, order: Option<&[&str]>) -> Cascade {
    let mut cfg = ConstellationConfig::default();
    for n in &mut cfg.networks {
        if let Some((_, base)) = bases.iter().find(|(name, _)| *name == n.name) {
            n.appview_url = (*base).to_string();
            if let Some((r, b)) = rate {
                n.rate_per_sec = Some(r);
                n.burst = Some(b);
            }
        } else if n.name != crate::config::COMMUNITY_NETWORK {
            n.enabled = false;
        }
    }
    if let Some((name, base)) = extra {
        let mut n = cfg.networks[0].clone();
        n.name = name.to_string();
        n.appview_url = base.to_string();
        n.pds_patterns = Vec::new();
        n.enabled = true;
        cfg.networks.push(n);
    }
    if let Some(order) = order {
        cfg.cascade.order = order.iter().map(|o| (*o).to_string()).collect();
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

fn router(lookup: Arc<StubLookup>, community: &[&str]) -> DidRouter {
    DidRouter::new(Arc::new(table_with_community(community)), lookup as Arc<dyn DidLookup>)
}

/// The production upstream over `cascade`, with Slingshot at `slingshot`.
fn real(cascade: Cascade, slingshot: &str) -> RealUpstreamSource {
    RealUpstreamSource::new(
        router(Arc::new(StubLookup::default()), &[]),
        cascade,
        SlingshotClient::new(NetClient::loopback_for_tests(), slingshot),
        PdsLister::new(Transports::loopback_for_tests(), HostBuckets::new(&Shares::for_tests(), 20.0, 1000)),
        ReadCache::new(1024 * 1024),
    )
}

/// ALICE and BOB are ready members, in S.
fn sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    for d in [ALICE, BOB] {
        s.scope.insert(d.into());
        s.members.insert(d.into());
    }
    s
}

async fn members(p: &sqlx::PgPool) {
    actor(p, ALICE, "alice.test", "Alice").await;
    actor(p, BOB, "bob.test", "Bob").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now()), ('{BOB}', 'admin', now())")).await;
}

async fn block(p: &sqlx::PgPool, author: &str, subjects: &[String]) {
    for (i, s) in subjects.iter().enumerate() {
        exec(
            p,
            &format!(
                "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{author}/app.bsky.graph.block/3kb{i:06}', '{author}', '{s}', '3k', now() - interval '{i} seconds')"
            ),
        )
        .await;
    }
}

fn me(did: &str) -> Viewer {
    Viewer { did: Some(did.into()), hydrate: true }
}

async fn blocks_of(h: &Hydrator, viewer: &Viewer, req: &Requester) -> Vec<Value> {
    let v = h.get_blocks(viewer, req, 100, None).await.expect("db").expect("blocks");
    v.inner()["blocks"].as_array().expect("blocks").clone()
}

fn recorder() -> (metrics_exporter_prometheus::PrometheusRecorder, metrics_exporter_prometheus::PrometheusHandle) {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    (recorder, handle)
}

// ---------------------------------------------------------------- isolation

/// T-1: private-context hydration over an out-of-scope DID asks nothing,
/// writes no `rt` key and queues nothing.
#[tokio::test]
async fn t1_private_context_hydration_takes_no_tier() {
    let Some(db) = fresh_db("profile_refresh_t1").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let (net, base) = serve(Net::named("bluesky").with(&[FAR.to_string()])).await;
    let h = hydrator_with(p, sets(), Arc::new(real(cascade(&[("bluesky", &base)], None, None, None), &base)));
    let req = Requester::test();
    let ctx = h.profile_context(&me(ALICE), &req.private_context(ALICE), &[FAR.to_string()], None, Tiers::List).await.expect("db");
    assert_eq!(ctx.view(FAR, ProfileKind::View)["handle"], "handle.invalid");
    assert!(h.cache().shared_keys().await.is_empty());
    assert!(h.inner.cfg.profile_queue.is_empty());
    assert_eq!(net.calls(), 0);
    db.drop_db().await;
}

/// T-2, T-3: a viewer-private `getMutes` (C-P1 a) makes one `getProfiles`
/// call into the viewer's own cache: no shared key, no negative entry, no
/// queue, and another viewer does not see it. A public list view of the
/// same DID afterwards still misses `rt`.
#[tokio::test]
async fn t2_t3_viewer_private_mutes_fill_only_the_viewers_cache() {
    let Some(db) = fresh_db("profile_refresh_t2").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let ghost = far(1);
    let (net, base) = serve(Net::named("bluesky").with(&[FAR.to_string()])).await;
    let h = hydrator_with(p, sets(), Arc::new(real(cascade(&[("bluesky", &base)], None, None, None), &base)));
    for viewer in [ALICE, BOB] {
        for subject in [FAR, ghost.as_str()] {
            exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{viewer}', '{subject}')")).await;
        }
    }
    let req = Requester::test();
    let mutes = h.get_mutes(&me(ALICE), &req, 50, None).await.expect("db").expect("mutes");
    let far_view = mutes.inner()["mutes"].as_array().expect("mutes").iter().find(|m| m["did"] == FAR).cloned().expect("far");
    assert_eq!(far_view["handle"], "ffffff.remote");
    assert_eq!(far_view["viewer"]["blockedBy"], false, "local viewer state, never upstream's");
    assert_eq!(net.calls(), 1);
    assert!(h.cache().shared_keys().await.is_empty(), "no shared key, positive or negative");
    assert_eq!(h.cache().private_keys(ALICE), [format!("rt:profile:{FAR}")]);
    assert!(h.inner.cfg.profile_queue.is_empty(), "a viewer-private request never queues");
    // Bob's own request makes its own call: Alice's fill was not shared.
    h.get_mutes(&me(BOB), &Requester::test(), 50, None).await.expect("db").expect("mutes");
    assert_eq!(net.calls(), 2);
    // T-3: a public list view still misses `rt`.
    block(p, ALICE, &[FAR.to_string()]).await;
    let blocks = blocks_of(&h, &me(ALICE), &Requester::test()).await;
    assert_eq!(blocks[0]["handle"], "ffffff.remote");
    assert_eq!(net.calls(), 3);
    assert_eq!(h.cache().shared_keys().await, [format!("rt:profile:{FAR}")]);
    db.drop_db().await;
}

/// T-4: the queue is fed only by `remote_profiles`' tier 3, behind both
/// mode checks; `getProfiles` never feeds it.
#[test]
fn t4_only_public_tier_three_pushes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pushes = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") && !path.ends_with("profile_refresh_tests.rs") {
                let source = std::fs::read_to_string(&path).expect("source");
                if source.contains("profile_queue.push(") {
                    pushes.push((path.clone(), source.matches("profile_queue.push(").count()));
                }
            }
        }
    }
    assert_eq!(pushes.len(), 1, "{pushes:?}");
    assert!(pushes[0].0.ends_with("hydration/profile_refresh.rs") && pushes[0].1 == 1, "{pushes:?}");
    let source = std::fs::read_to_string(&pushes[0].0).expect("source");
    let body = &source[source.find("async fn remote_profiles").expect("remote_profiles")..];
    let body = &body[..body.find("\n    }\n").expect("end")];
    let early = body.find("if req.is_local_only()").expect("local-only return");
    let shared = body.find("let shared = !req.is_viewer_private();").expect("shared");
    let push = body.find("profile_queue.push(").expect("push");
    let guard = body[..push].rfind("if shared {").expect("guarded");
    assert!(early < shared && shared < guard && guard < push);
    let actor = std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/hydration/actor.rs")).expect("actor");
    let get_profiles = &actor[actor.find("pub async fn get_profiles").expect("get_profiles")..];
    let get_profiles = &get_profiles[..get_profiles.find("\n    }\n").expect("end")];
    assert!(!get_profiles.contains("remote_profiles") && !get_profiles.contains("profile_queue"));
}

/// T-5: G-9. A stored out-of-scope row (class M) and a DID with no row give
/// the same `getBlocks` entry, with the cascade down and with it up.
#[tokio::test]
async fn t5_a_stored_row_and_no_row_answer_alike() {
    let Some(db) = fresh_db("profile_refresh_t5").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let (stored, unknown) = (far(1), far(2));
    exec(p, &format!("INSERT INTO actors (did, pds_endpoint, handle, handle_checked_at, display_name) VALUES ('{stored}', 'https://pds.example.com', 'stored.test', now(), 'Stored')")).await;
    let entry = |h: Hydrator, did: String| async move {
        exec(h.pool(), "DELETE FROM blocks").await;
        block(h.pool(), ALICE, std::slice::from_ref(&did)).await;
        let blocks = blocks_of(&h, &me(ALICE), &Requester::test()).await;
        serde_json::to_string(&blocks[0]).expect("json").replace(&did, "did:plc:subject").replace(&did[8..14], "subjec")
    };
    // Down: no view anywhere.
    let down = || hydrator_with(p, sets(), Arc::new(FixedUpstream::default()));
    assert_eq!(entry(down(), stored.clone()).await, entry(down(), unknown.clone()).await);
    // Up: both come from the batch, never from the stored row.
    let up = || {
        let mut up = FixedUpstream::default();
        for d in [&stored, &unknown] {
            up.profiles.insert(d.clone(), remote_view(d, &format!("{}.remote", &d[8..14])));
        }
        hydrator_with(p, sets(), Arc::new(up))
    };
    let (a, b) = (entry(up(), stored.clone()).await, entry(up(), unknown.clone()).await);
    assert_eq!(a, b);
    assert!(a.contains("subjec.remote") && !a.contains("Stored"), "{a}");
    db.drop_db().await;
}

/// T-5a: one budget per request and mode. A viewer-private part that
/// spends its 25 DIDs leaves the public part's untouched.
#[tokio::test]
async fn t5a_viewer_private_and_public_parts_have_their_own_budgets() {
    let Some(db) = fresh_db("profile_refresh_t5a").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let up = Arc::new(FixedUpstream::default());
    let h = hydrator_with(p, sets(), Arc::clone(&up) as Arc<dyn UpstreamSource>);
    let req = Requester::test();
    let private: Vec<String> = (0..25).map(far).collect();
    h.profile_context(&me(ALICE), &req.viewer_private(ALICE), &private, None, Tiers::List).await.expect("db");
    h.profile_context(&me(ALICE), &req, &[far(100), far(101)], None, Tiers::List).await.expect("db");
    let batches = up.batches.lock().expect("batches").clone();
    assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [25, 2]);
    // The rule, on the handles themselves.
    assert!(req.shares_profile_budget(&req.clone()));
    assert!(req.shares_profile_budget(&req.detached()));
    assert!(!req.shares_profile_budget(&req.viewer_private(ALICE)));
    assert!(!req.shares_profile_budget(&req.private_context(ALICE)));
    db.drop_db().await;
}

/// T-5b: the Thread Lens author pass computes no viewer state, so a
/// request with a Farsight handle asks Farsight nothing.
#[tokio::test]
async fn t5b_the_author_pass_asks_farsight_nothing() {
    use crate::xrpc::backstop::{self, Selected};

    let Some(db) = fresh_db("profile_refresh_t5b").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    fake.blockers.lock().expect("blockers").insert(FAR.to_string());
    let mut up = FixedUpstream::default();
    up.profiles.insert(FAR.into(), remote_view(FAR, "far.remote"));
    let h = hydrator_with_upstream_and_farsight(p, sets(), Arc::new(up), farsight.clone());
    let req = Requester::test();
    let selected = Selected::by_ip("203.0.113.9".parse().expect("ip"));
    selected.attach_farsight(farsight.request(&req));
    let found = backstop::scope(selected, h.remote_profiles(&req, &[FAR.to_string()])).await;
    assert_eq!(found[FAR]["handle"], "far.remote");
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
    db.drop_db().await;
}

// -------------------------------------------------------------------- batch

/// T-6, T-7: up to 25 uncached DIDs go in one call to the first network;
/// the second is asked only for what the first omitted. The 26th renders
/// minimal and is queued.
#[tokio::test]
async fn t6_t7_one_call_per_network_and_the_rest_queued() {
    let Some(db) = fresh_db("profile_refresh_t6").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let dids: Vec<String> = (0..26).map(far).collect();
    let (first, a) = serve(Net::named("bluesky").with(&dids[..20])).await;
    let (second, b) = serve(Net::named("community-a").with(&dids[20..])).await;
    let h = hydrator_with(p, sets(), Arc::new(real(cascade(&[("bluesky", &a), ("community-a", &b)], None, None, None), &a)));
    block(p, ALICE, &dids).await;
    let blocks = blocks_of(&h, &me(ALICE), &Requester::test()).await;
    assert_eq!(first.asked(), [dids[..25].to_vec()]);
    assert_eq!(second.asked(), [dids[20..25].to_vec()]);
    let handles: Vec<&str> = blocks.iter().map(|b| b["handle"].as_str().expect("handle")).collect();
    assert_eq!(handles.iter().filter(|h| h.ends_with(".remote")).count(), 25);
    let last = blocks.iter().find(|b| b["did"] == dids[25].as_str()).expect("26th");
    assert_eq!(last["handle"], "handle.invalid");
    assert_eq!(h.inner.cfg.profile_queue.pop(100), [dids[25].clone()]);
    db.drop_db().await;
}

/// T-8: the budget is per request: a feed's authors and reposters share
/// one batch.
#[tokio::test]
async fn t8_a_feed_makes_one_batch() {
    let Some(db) = fresh_db("profile_refresh_t8").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let up = Arc::new(FixedUpstream::default());
    let h = hydrator_with(p, sets(), Arc::clone(&up) as Arc<dyn UpstreamSource>);
    let mut items = Vec::new();
    for i in 0..15 {
        let author = far(i);
        let uri = post(p, &author, &format!("3kpost{i:06}"), "hi", None, None).await;
        items.push(super::feed::FeedItem {
            uri: format!("at://{}/app.bsky.feed.repost/3kr{i:06}", far(100 + i)),
            originator_did: far(100 + i),
            post_uri: uri,
            kind: "repost".into(),
            sort_at: chrono::Utc::now(),
        });
    }
    h.feed_views(&me(ALICE), &Requester::test(), &items).await.expect("db");
    let batches = up.batches.lock().expect("batches").clone();
    assert_eq!(batches.len(), 1, "{batches:?}");
    assert_eq!(batches[0].len(), 25);
    db.drop_db().await;
}

/// T-9, T-10: the 800 ms window cuts a slow network as `budget`, which the
/// breaker does not record; a request whose deadline is past skips tier 2.
#[tokio::test]
async fn t9_t10_the_window_and_the_deadline() {
    let Some(db) = fresh_db("profile_refresh_t9").await else {
        return;
    };
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let p = &db.pool;
    members(p).await;
    let slow = Net::named("bluesky").with(&[FAR.to_string()]);
    *slow.delay_ms.lock().expect("delay") = 900;
    let (net, base) = serve(slow).await;
    let c = cascade(&[("bluesky", &base)], None, None, None);
    let h = hydrator_with(p, sets(), Arc::new(real(c.clone(), &base)));
    let req = Requester::test();
    let started = Instant::now();
    let found = h.remote_profiles(&req, &[FAR.to_string()]).await;
    assert!(found.is_empty());
    assert!(started.elapsed() < Duration::from_millis(890), "{:?}", started.elapsed());
    let text = handle.render();
    assert_eq!(series(&text, METRIC_BATCH_FETCH, &[r#"network="bluesky""#, r#"result="budget""#]), Some(1.0), "{text}");
    assert_eq!(series(&text, METRIC_BUDGET_CUTOFF, &[r#"network="bluesky""#]), Some(1.0), "{text}");
    for _ in 0..crate::constellation::cascade::BREAKER_THRESHOLD {
        h.remote_profiles(&Requester::test(), &[far(9)]).await;
    }
    assert!(!c.breaker_open("bluesky"), "a budget cut is not a breaker failure");
    // T-10.
    let calls = net.calls();
    let late = Requester::test().with_deadline(Instant::now());
    assert!(h.remote_profiles(&late, &[far(10)]).await.is_empty());
    assert_eq!(net.calls(), calls);
    db.drop_db().await;
}

/// T-11: an empty request bucket ends the per-response walk with no
/// negative entry and never marks the request; `getProfiles` marks it.
#[tokio::test]
async fn t11_an_empty_share_ends_the_batch_without_marking_the_request() {
    let Some(db) = fresh_db("profile_refresh_t11").await else {
        return;
    };
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let p = &db.pool;
    members(p).await;
    let (net, base) = serve(Net::named("bluesky")).await;
    // Burst 2: one token in the non-member pool.
    let c = cascade(&[("bluesky", &base)], Some((1, 2)), None, None);
    let h = hydrator_with(p, sets(), Arc::new(real(c.clone(), &base)));
    let req = Requester::test();
    h.remote_profiles(&req, &[far(1)]).await;
    assert_eq!(net.calls(), 1);
    // The share is empty now.
    h.remote_profiles(&req, &[far(2)]).await;
    assert_eq!(net.calls(), 1);
    assert!(!req.spent(), "a profile batch never marks the request");
    // The first DID was answered without (a negative entry); the second
    // was never asked, and gets none.
    assert_eq!(h.cache().shared_keys().await, [format!("rt:profile-neg:{}", far(1))]);
    assert_eq!(h.inner.cfg.profile_queue.pop(10), [far(2)]);
    assert!(series(&handle.render(), crate::xrpc::pools::METRIC_EXHAUSTED, &[r#"target_kind="network""#]).is_some_and(|n| n >= 1.0));
    // On the walk's own record.
    let walk = req.detached();
    let fetched = c.fetch_profiles(ProfileWalk::Request { req: &walk, until: walk.deadline() }, &c.table().plan(None), &[far(3)]).await;
    assert!(fetched.found.is_empty() && fetched.absent.is_empty());
    assert!(walk.spent() && !req.spent());
    // `getProfiles` is the subject fetch: it marks the request.
    let out = h.get_profiles(&Viewer::anonymous(), &req, &[far(4)]).await.expect("db").expect("view");
    assert_eq!(out.inner()["profiles"], json!([]));
    assert!(req.spent());
    db.drop_db().await;
}

/// T-11a: a feed request whose profile batch found the network share empty
/// is not marked, so the PDS-fallback listing (which ends a page early or
/// answers 429 only for a marked request) gives the same page.
#[tokio::test]
async fn t11a_a_spent_profile_batch_leaves_a_feed_page_alone() {
    let Some(db) = fresh_db("profile_refresh_t11a").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let feed = json!({"feed": [{"post": {"uri": format!("at://{FAR}/app.bsky.feed.post/3k1"), "cid": "c", "author": {"did": FAR, "handle": "far.test"}, "record": {"text": "t"}, "indexedAt": "2026-01-01T00:00:00.000Z"}}], "cursor": "P:3k1:"});
    let page = |bucket: Option<crate::xrpc::pools::SharedBucket>| {
        let feed = feed.clone();
        async move {
            let mut up = FixedUpstream { batch_bucket: bucket, ..FixedUpstream::default() };
            up.feeds.insert((FAR.to_string(), None), feed);
            let h = hydrator_with(p, sets(), Arc::new(up));
            let req = Requester::test();
            // A list view in the same request first: its batch finds the
            // share empty (or not).
            block(p, ALICE, &[far(1)]).await;
            blocks_of(&h, &me(ALICE), &req).await;
            exec(p, "DELETE FROM blocks").await;
            let out = h.get_author_feed(&me(ALICE), &req, FAR, 50, None, "posts_with_replies", false).await.expect("db").expect("feed");
            (req.spent(), out.inner()["feed"].as_array().map(Vec::len), out.inner()["cursor"].clone())
        }
    };
    let empty = Shares::for_tests().bucket("network", 1.0, 2);
    let probe = Requester::test();
    while probe.draw(&empty) {}
    let spent = page(Some(empty)).await;
    assert_eq!(spent, page(None).await);
    assert!(!spent.0);
    db.drop_db().await;
}

/// T-11b: concurrent `profile_context` calls of one request reserve at
/// most 25 DIDs in total.
#[tokio::test]
async fn t11b_concurrent_calls_share_25() {
    let Some(db) = fresh_db("profile_refresh_t11b").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let up = Arc::new(FixedUpstream::default());
    let h = hydrator_with(p, sets(), Arc::clone(&up) as Arc<dyn UpstreamSource>);
    let req = Requester::test();
    let calls = (0..3).map(|k| {
        let (h, req) = (h.clone(), req.clone());
        async move {
            let dids: Vec<String> = (0..20).map(|i| far(k * 100 + i)).collect();
            h.profile_context(&me(ALICE), &req, &dids, None, Tiers::List).await.expect("db");
        }
    });
    futures::future::join_all(calls).await;
    let total: usize = up.batches.lock().expect("batches").iter().map(Vec::len).sum();
    assert_eq!(total, 25);
    db.drop_db().await;
}

/// T-11c: a thread anchor's author fetch that finds the share empty does
/// not mark the request, so a not-listable anchor is a 400, not a 429.
#[tokio::test]
async fn t11c_an_anchor_authors_empty_share_leaves_the_not_found() {
    let Some(db) = fresh_db("profile_refresh_t11c").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let anchor = post(p, FAR, "3kanchor0000", "hidden", None, None).await;
    exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{anchor}', '!hide')")).await;
    let empty = Shares::for_tests().bucket("network", 1.0, 2);
    let probe = Requester::test();
    while probe.draw(&empty) {}
    let up = Arc::new(FixedUpstream { profile_bucket: Some(empty), ..FixedUpstream::default() });
    let h = hydrator_with(p, sets(), Arc::clone(&up) as Arc<dyn UpstreamSource>);
    let req = Requester::test();
    let e = h.get_post_thread(&me(ALICE), &req, &anchor, 6, 80).await.expect("db").expect_err("not found");
    assert_eq!(e.error, "NotFound");
    assert!(up.take_modes().contains(&(format!("profile {FAR}"), false)), "the anchor's author took the per-DID path");
    assert!(!req.spent(), "a 400, not a 429");
    db.drop_db().await;
}

/// T-12: `profile_batch_fetch_total`'s results.
#[tokio::test]
async fn t12_batch_fetch_results() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let down = Net::named("bluesky");
    *down.status.lock().expect("status") = Some(503);
    let (_, a) = serve(down).await;
    let (_, b) = serve(Net::named("community-a").with(&[FAR.to_string()])).await;
    let c = cascade(&[("bluesky", &a), ("community-a", &b)], None, None, None);
    let req = Requester::test();
    let walk = ProfileWalk::Request { req: &req, until: req.deadline() };
    let got = c.fetch_profiles(walk, &c.table().plan(None), &[FAR.to_string()]).await;
    assert!(got.found.contains_key(FAR));
    let missing = Net::named("bluesky");
    *missing.status.lock().expect("status") = Some(400);
    let (_, m) = serve(missing).await;
    let c = cascade(&[("bluesky", &m)], None, None, None);
    c.fetch_profiles(walk, &c.table().plan(None), &[FAR.to_string()]).await;
    let text = handle.render();
    for (network, result) in [("bluesky", "transient"), ("community-a", "ok"), ("bluesky", "error")] {
        assert_eq!(series(&text, METRIC_BATCH_FETCH, &[&format!(r#"network="{network}""#), &format!(r#"result="{result}""#)]), Some(1.0), "{network} {result}: {text}");
    }
    assert_eq!(series(&text, METRIC_CALLS, &[r#"network="community-a""#, r#"origin="request""#]), Some(1.0));
}

// --------------------------------------------- negative entries and lifetimes

/// T-13, T-25: every network answering 2xx without a DID writes a negative
/// entry; a list view then renders it minimal with no call, while
/// `getProfile` and `getProfiles` still ask.
#[tokio::test]
async fn t13_t25_a_negative_entry_stops_list_views_only() {
    let Some(db) = fresh_db("profile_refresh_t13").await else {
        return;
    };
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let p = &db.pool;
    members(p).await;
    let (first, a) = serve(Net::named("bluesky")).await;
    let (second, b) = serve(Net::named("community-a")).await;
    let h = hydrator_with(p, sets(), Arc::new(real(cascade(&[("bluesky", &a), ("community-a", &b)], None, None, None), &a)));
    block(p, ALICE, &[FAR.to_string()]).await;
    blocks_of(&h, &me(ALICE), &Requester::test()).await;
    assert_eq!((first.calls(), second.calls()), (1, 1));
    assert!(h.cache().shared_keys().await.contains(&format!("rt:profile-neg:{FAR}")));
    assert_eq!(series(&handle.render(), METRIC_NEGATIVE, &[]), Some(1.0));
    assert!(h.inner.cfg.profile_queue.is_empty(), "a DID that just got a negative entry is not queued");
    let again = blocks_of(&h, &me(ALICE), &Requester::test()).await;
    assert_eq!(again[0]["handle"], "handle.invalid");
    assert_eq!((first.calls(), second.calls()), (1, 1), "no call for a negative entry");
    h.get_profiles(&Viewer::anonymous(), &Requester::test(), &[FAR.to_string()]).await.expect("db").expect("view");
    assert_eq!((first.calls(), second.calls()), (2, 2), "getProfiles still asks");
    let others = first.others.load(Ordering::SeqCst);
    h.get_profile(&Viewer::anonymous(), &Requester::test(), FAR).await.expect("db").expect_err("absent");
    assert!(first.others.load(Ordering::SeqCst) > others, "getProfile still asks");
    db.drop_db().await;
}

/// T-14: one network transient and the other 2xx without the DID: no
/// negative entry, and the DID is queued.
#[tokio::test]
async fn t14_a_transient_network_writes_no_negative_entry() {
    let Some(db) = fresh_db("profile_refresh_t14").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let down = Net::named("bluesky");
    *down.status.lock().expect("status") = Some(503);
    let (_, a) = serve(down).await;
    let (_, b) = serve(Net::named("community-a")).await;
    let h = hydrator_with(p, sets(), Arc::new(real(cascade(&[("bluesky", &a), ("community-a", &b)], None, None, None), &a)));
    block(p, ALICE, &[FAR.to_string()]).await;
    blocks_of(&h, &me(ALICE), &Requester::test()).await;
    assert!(h.cache().shared_keys().await.is_empty());
    assert_eq!(h.inner.cfg.profile_queue.pop(10), [FAR.to_string()]);
    db.drop_db().await;
}

/// T-15: each class of key lives its own time, and a write restarts it.
#[tokio::test]
async fn t15_key_classes_have_their_own_lifetimes() {
    use super::cache::RtTtls;

    let ttls = RtTtls {
        profile: Duration::from_millis(2500),
        negative: Duration::from_millis(1000),
        farsight_pair: Duration::from_millis(1000),
        other: Duration::from_millis(1000),
    };
    let c = ReadCache::new(1024 * 1024).with_ttls(ttls);
    let req = Requester::test();
    c.put_json(&req, "rt:profile:a", &1u32).await;
    c.put_json(&req, "rt:profile:b", &1u32).await;
    c.put_json(&req, "rt:post:x", &1u32).await;
    c.put_negative("n").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(c.get_json::<u32>(&req, "rt:post:x").await, None, "a 300 s class entry is gone");
    assert!(!c.has_negative(&req, "n").await, "a negative entry expires at its own time");
    assert_eq!(c.get_json::<u32>(&req, "rt:profile:a").await, Some(1), "a profile outlives it");
    // A re-insert restarts the clock.
    c.put_json(&req, "rt:profile:b", &2u32).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(c.get_json::<u32>(&req, "rt:profile:a").await, None);
    assert_eq!(c.get_json::<u32>(&req, "rt:profile:b").await, Some(2));
}

/// T-16, T-16a: an empty plan proves nothing, and a callable network
/// outside `order` that was never asked blocks a negative entry, for the
/// batch and for the drain.
#[tokio::test]
async fn t16_negative_entries_need_every_callable_network() {
    let req = Requester::test();
    let walk = ProfileWalk::Request { req: &req, until: req.deadline() };
    let c = cascade(&[], None, None, None);
    assert!(c.table().plan(None).is_empty());
    assert!(c.fetch_profiles(walk, &c.table().plan(None), &[FAR.to_string()]).await.absent.is_empty());
    // The same walk with every callable network asked does write one.
    let (_, a) = serve(Net::named("bluesky")).await;
    let c = cascade(&[("bluesky", &a)], None, None, None);
    assert_eq!(c.fetch_profiles(walk, &c.table().plan(None), &[FAR.to_string()]).await.absent, [FAR]);
    // T-16a: `extra` is callable and not in `order`.
    let (extra, e) = serve(Net::named("extra")).await;
    let c = cascade(&[("bluesky", &a)], None, Some(("extra", &e)), Some(&["bluesky"]));
    assert_eq!(c.table().plan(None), ["bluesky"]);
    assert!(c.fetch_profiles(walk, &c.table().plan(None), &[FAR.to_string()]).await.absent.is_empty());
    let cache = ReadCache::new(1024 * 1024);
    let queue = ProfileQueue::new(100);
    let drain = ProfileDrain::new(queue, cache.clone(), router(Arc::new(StubLookup::default()), &[]), c);
    drain.drain(vec![FAR.to_string()]).await;
    assert!(!cache.contains_shared(&format!("rt:profile-neg:{FAR}")));
    assert_eq!(extra.calls(), 0);
}

// ------------------------------------------------------------ queue and drain

/// T-17: the queue deduplicates, is capped, counts what it drops, and
/// keeps its gauge.
#[test]
fn t17_the_queue_dedups_and_caps() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let q = ProfileQueue::new(3);
    for d in ["a", "b", "a", "c", "d", "e"] {
        q.push(d);
    }
    assert_eq!(q.len(), 3);
    let text = handle.render();
    assert_eq!(series(&text, METRIC_DROPPED, &[]), Some(2.0), "{text}");
    assert_eq!(series(&text, METRIC_QUEUE, &[]), Some(3.0), "{text}");
    assert_eq!(q.pop(2), ["a", "b"]);
    assert_eq!(series(&handle.render(), METRIC_QUEUE, &[]), Some(1.0));
    // A popped DID may be queued again.
    q.push("a");
    assert_eq!(q.pop(10), ["c", "a"]);
}

/// T-18, T-21: the drain never resolves a DID (no lookup, no `route`
/// write), and a `community`-routed DID is walked like an unrouted one
/// (C-P2 b: the drain reads no mode).
#[tokio::test]
async fn t18_t21_the_drain_reads_routes_from_the_cache_only() {
    let (net, a) = serve(Net::named("bluesky").with(&[FAR.to_string()])).await;
    let lookup = Arc::new(StubLookup::default());
    let community = far(1);
    lookup.pds.lock().expect("pds").insert(community.clone(), "https://pds1.community.example".into());
    let r = router(Arc::clone(&lookup), &["*.community.example"]);
    // A public request routed the community DID earlier.
    assert!(r.resolve_did(&Requester::test(), &community).await.is_community());
    let before = (lookup.calls.load(Ordering::SeqCst), r.cached_routes().await);
    let cache = ReadCache::new(1024 * 1024);
    let drain = ProfileDrain::new(ProfileQueue::new(100), cache.clone(), r.clone(), cascade(&[("bluesky", &a)], None, None, None));
    drain.drain(vec![FAR.to_string(), community.clone()]).await;
    assert_eq!((lookup.calls.load(Ordering::SeqCst), r.cached_routes().await), before, "no lookup, no route write");
    assert!(cache.contains_shared(&format!("rt:profile:{FAR}")));
    // The community DID was asked (and, absent everywhere, got a
    // negative entry).
    assert!(net.asked().iter().flatten().any(|d| *d == community));
    assert!(cache.contains_shared(&format!("rt:profile-neg:{community}")));
}

/// T-19: the drain waits for the background bucket (1/s, burst 2: three
/// batches take a second), never draws a request bucket, and labels its
/// calls `origin="background"`.
#[tokio::test]
async fn t19_the_drain_waits_for_its_own_bucket() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let (net, a) = serve(Net::named("bluesky")).await;
    // A request bucket with no token for anyone: burst 1 per pool, spent.
    let c = cascade(&[("bluesky", &a)], Some((1, 2)), None, None);
    let spender = Requester::test();
    let walk = ProfileWalk::Request { req: &spender, until: spender.deadline() };
    c.fetch_profiles(walk, &c.table().plan(None), &[far(0)]).await;
    let drain = ProfileDrain::new(ProfileQueue::new(100), ReadCache::new(1024 * 1024), router(Arc::new(StubLookup::default()), &[]), c);
    let started = Instant::now();
    for i in 1..=3 {
        drain.drain(vec![far(i)]).await;
    }
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(900) && took < Duration::from_millis(1900), "{took:?}");
    assert_eq!(net.calls(), 4);
    let text = handle.render();
    assert_eq!(series(&text, METRIC_CALLS, &[r#"origin="background""#, r#"endpoint="""#]), Some(3.0), "{text}");
    assert_eq!(series(&text, METRIC_COMPLETED, &[]), Some(3.0), "{text}");
    // The bucket itself, in paused time.
    tokio::time::pause();
    let bucket = crate::xrpc::pools::BackgroundBucket::new(1.0, 2);
    let t0 = tokio::time::Instant::now();
    for _ in 0..3 {
        bucket.acquire().await;
    }
    let took = t0.elapsed();
    assert!(took >= Duration::from_secs(1) && took < Duration::from_millis(1010), "{took:?}");
}

/// T-20: a transient answer drops the batch's remaining DIDs: no negative
/// entry, no later network, no requeue.
#[tokio::test]
async fn t20_a_transient_drops_the_batch() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let down = Net::named("bluesky");
    *down.status.lock().expect("status") = Some(503);
    let (_, a) = serve(down).await;
    let (second, b) = serve(Net::named("community-a")).await;
    let cache = ReadCache::new(1024 * 1024);
    let queue = ProfileQueue::new(100);
    let drain = ProfileDrain::new(queue.clone(), cache.clone(), router(Arc::new(StubLookup::default()), &[]), cascade(&[("bluesky", &a), ("community-a", &b)], None, None, None));
    drain.drain(vec![FAR.to_string(), far(1)]).await;
    assert_eq!(second.calls(), 0);
    assert!(!cache.contains_shared(&format!("rt:profile-neg:{FAR}")));
    assert!(queue.is_empty());
    assert_eq!(series(&handle.render(), METRIC_COMPLETED, &[]), None);
}

/// T-22: a routed DID is asked of its home first.
#[tokio::test]
async fn t22_a_routed_did_is_asked_of_its_home_first() {
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let (_, a) = serve(Net { log: Some(Arc::clone(&log)), ..Net::named("bluesky") }).await;
    let (_, b) = serve(Net { log: Some(Arc::clone(&log)), ..Net::named("community-a") }).await;
    let lookup = Arc::new(StubLookup::default());
    lookup.pds.lock().expect("pds").insert(FAR.into(), "https://pds.community-a.example".into());
    let r = router(Arc::clone(&lookup), &[]);
    r.resolve_did(&Requester::test(), FAR).await;
    let drain = ProfileDrain::new(ProfileQueue::new(100), ReadCache::new(1024 * 1024), r, cascade(&[("bluesky", &a), ("community-a", &b)], None, None, None));
    drain.drain(vec![FAR.to_string(), far(1)]).await;
    // FAR's group (home community-a) and the unrouted group (bluesky first).
    assert_eq!(*log.lock().expect("log"), ["community-a", "bluesky", "bluesky", "community-a"]);
}

// ---------------------------------------------------------------- getProfiles

/// T-23: many out-of-scope subjects: one call per network tried and one
/// Farsight lookup for all of them.
#[tokio::test]
async fn t23_get_profiles_makes_one_call_and_one_farsight_lookup() {
    use crate::xrpc::backstop::{self, Selected};

    let Some(db) = fresh_db("profile_refresh_t23").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let viewer = crate::farsight::lookup::tests::V;
    actor(p, viewer, "v.test", "V").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{viewer}', 'admin', now())")).await;
    let dids: Vec<String> = (0..10).map(far).collect();
    let (net, a) = serve(Net::named("bluesky").with(&dids)).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    let mut s = sets();
    s.members.insert(viewer.into());
    s.scope.insert(viewer.into());
    let h = hydrator_with_upstream_and_farsight(p, s, Arc::new(real(cascade(&[("bluesky", &a)], None, None, None), &a)), farsight.clone());
    let req = Requester::test();
    let selected = Selected::by_ip("203.0.113.9".parse().expect("ip"));
    selected.attach_farsight(farsight.request(&req));
    let out = backstop::scope(selected, h.get_profiles(&me(viewer), &req, &dids)).await.expect("db").expect("view");
    let got: Vec<&str> = out.inner()["profiles"].as_array().expect("profiles").iter().map(|v| v["did"].as_str().expect("did")).collect();
    assert_eq!(got, dids.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(out.inner()["profiles"][0]["viewer"]["blockedBy"], false, "local viewer state");
    assert_eq!(net.calls(), 1);
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    assert!(h.inner.cfg.profile_queue.is_empty(), "getProfiles never queues");
    db.drop_db().await;
}

/// An upstream whose handle resolution is slow and counted.
#[derive(Default)]
struct SlowHandles {
    in_flight: AtomicUsize,
    most: AtomicUsize,
}

#[async_trait::async_trait]
impl UpstreamSource for SlowHandles {
    async fn profile(&self, _req: &Requester, _actor: &str) -> Option<Value> {
        None
    }
    async fn posts(&self, _req: &Requester, _uris: &[String]) -> Vec<Value> {
        Vec::new()
    }
    async fn post_thread(&self, _req: &Requester, _uri: &str, _depth: u32, _parent_height: u32) -> Option<Value> {
        None
    }
    async fn author_feed(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>, _filter: &str) -> Option<Value> {
        None
    }
    async fn follows(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
    async fn resolve_handle(&self, _req: &Requester, handle: &str) -> Option<String> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        // Later handles answer sooner, so order comes from the input.
        let n: u64 = handle.trim_start_matches('h').trim_end_matches(".test").parse().unwrap_or(0);
        tokio::time::sleep(Duration::from_millis(120 - 15 * n)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        (handle != "h3.test").then(|| far(usize::try_from(n).unwrap_or(0)))
    }
    async fn fetch_profiles(&self, _req: &Requester, dids: &[String], _until: Instant) -> crate::constellation::ProfilesFetched {
        let mut out = crate::constellation::ProfilesFetched::default();
        for d in dids {
            out.found.insert(d.clone(), remote_view(d, "x.remote"));
        }
        out
    }
}

/// T-24: handles resolve at most four at a time, the output follows the
/// input, and an unresolved handle is omitted.
#[tokio::test]
async fn t24_handles_resolve_four_at_a_time_in_order() {
    let Some(db) = fresh_db("profile_refresh_t24").await else {
        return;
    };
    let p = &db.pool;
    members(p).await;
    let up = Arc::new(SlowHandles::default());
    let h = hydrator_with(p, sets(), Arc::clone(&up) as Arc<dyn UpstreamSource>);
    let handles: Vec<String> = (0..6).map(|i| format!("h{i}.test")).collect();
    let out = h.get_profiles(&Viewer::anonymous(), &Requester::test(), &handles).await.expect("db").expect("view");
    let got: Vec<String> = out.inner()["profiles"].as_array().expect("profiles").iter().map(|v| v["did"].as_str().expect("did").to_string()).collect();
    assert_eq!(got, [far(0), far(1), far(2), far(4), far(5)]);
    assert_eq!(up.most.load(Ordering::SeqCst), 4);
    db.drop_db().await;
}

// ------------------------------------------------------------------ Thread Lens

/// T-26: a Slingshot node's author becomes the batch's
/// `profileViewBasic`, keeping the node's own `viewer`.
#[tokio::test]
async fn t26_a_slingshot_nodes_author_takes_the_batch() {
    use crate::config::ThreadLensConfig;
    use crate::hydration::lens::LensNode;
    use crate::threadlens::cursor::CursorKey;
    use crate::threadlens::ladder::Ladder;
    use crate::threadlens::thread;

    let Some(db) = fresh_db("profile_refresh_t26").await else {
        return;
    };
    let p = &db.pool;
    let viewer = crate::farsight::lookup::tests::V;
    actor(p, viewer, "v.test", "V").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{viewer}', 'admin', now())")).await;
    let uri = format!("at://{FAR}/app.bsky.feed.post/3kslingshot1");
    // An AppView that has no post and no profile, and a Slingshot that has
    // the record.
    let record_uri = uri.clone();
    let base = serve_fake(axum::Router::new().route(
        "/xrpc/{method}",
        get(move |Path(method): Path<String>| {
            let record_uri = record_uri.clone();
            async move {
                if method == "com.atproto.repo.getRecord" {
                    return axum::Json(json!({"uri": record_uri, "cid": "bafyrec", "value": {"$type": "app.bsky.feed.post", "text": "from the record", "createdAt": "2026-01-01T00:00:00Z"}})).into_response();
                }
                (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "NotFound", "message": "Post not found"}))).into_response()
            }
        }),
    ))
    .await;
    let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
    let mut up = FixedUpstream::default();
    up.profiles.insert(FAR.into(), remote_view(FAR, "far.remote"));
    let mut s = ScopeSets::default();
    s.members.insert(viewer.into());
    s.scope.insert(viewer.into());
    let h = hydrator_with_upstream_and_farsight(p, s, Arc::new(up), farsight);
    let c = cascade(&[("bluesky", &base), ("community-a", &base)], None, None, None);
    let l = Ladder::new(h, router(Arc::new(StubLookup::default()), &[]), c, SlingshotClient::new(NetClient::loopback_for_tests(), &base));
    let req = Requester::test();
    let got = thread::resolve(&l, &ThreadLensConfig::default(), &CursorKey::generated(), &me(viewer), &req, std::slice::from_ref(&uri)).await.expect("resolve");
    let LensNode::Post { post, .. } = &got.nodes[0] else {
        panic!("{:?}", got.nodes[0]);
    };
    let author = &post["author"];
    assert_eq!(author["handle"], "far.remote");
    assert_eq!(author["displayName"], "Remote");
    assert!(author.get("description").is_none() && author.get("banner").is_none(), "profileViewBasic: {author}");
    assert_eq!(author["viewer"]["blockedBy"], false, "the node's own viewer, not upstream's: {author}");
    assert!(!author.to_string().contains("upstream"), "upstream labels never reach the node: {author}");
    db.drop_db().await;
}

// ------------------------------------------------------------ config, metrics

/// T-27: defaults, bounds, a misspelt key, and the two network keys on the
/// local network.
#[test]
fn t27_profile_settings_are_validated() {
    use crate::config::FileConfig;

    let check = |toml: &str| {
        let cfg = FileConfig::parse(toml).expect("parse");
        let mut out = Vec::new();
        crate::xrpc::pools::problems(&cfg, &mut out);
        out
    };
    let defaults = FileConfig::parse("").expect("parse");
    assert_eq!(
        (defaults.cache.profile_ttl_secs, defaults.cache.profile_negative_ttl_secs, defaults.cache.profile_refresh_max),
        (3600, 900, 10_000)
    );
    assert!(defaults.constellation.networks.iter().all(|n| n.background_rate_per_sec.0 == 1.0 && n.background_burst == 2));
    assert!(check("").is_empty());
    for (toml, needle) in [
        ("[cache]\nprofile_ttl_secs = 59\n", "cache.profile_ttl_secs"),
        ("[cache]\nprofile_ttl_secs = 86401\n", "cache.profile_ttl_secs"),
        ("[cache]\nprofile_negative_ttl_secs = 30\n", "cache.profile_negative_ttl_secs"),
        ("[cache]\nprofile_ttl_secs = 600\nprofile_negative_ttl_secs = 900\n", "must not exceed cache.profile_ttl_secs"),
        ("[cache]\nprofile_refresh_max = 0\n", "cache.profile_refresh_max"),
        ("[cache]\nprofile_refresh_max = 1000001\n", "cache.profile_refresh_max"),
    ] {
        let out = check(toml);
        assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
    }
    assert!(check("[cache]\nprofile_ttl_secs = 60\nprofile_negative_ttl_secs = 60\nprofile_refresh_max = 1000000\n").is_empty());
    assert!(FileConfig::parse("[cache]\nprofile_tll_secs = 60\n").is_err(), "deny_unknown_fields");
    let networks = |bluesky: &str, local: &str| {
        format!(
            "[[constellation.networks]]\nname = \"bluesky\"\nappview_url = \"https://public.api.bsky.app\"\nauth = \"unauth\"\nrate_per_sec = 20\nburst = 40\nenabled = true\n{bluesky}\n\
             [[constellation.networks]]\nname = \"community\"\nappview_url = \"local\"\nauth = \"unauth\"\nenabled = true\n{local}\n\
             [constellation.cascade]\norder = [\"bluesky\"]\n"
        )
    };
    assert!(check(&networks("background_rate_per_sec = 2.5\nbackground_burst = 5", "background_rate_per_sec = 0.0\nbackground_burst = 0")).is_empty(), "ignored on the local network");
    let out = check(&networks("background_rate_per_sec = 0.0", ""));
    assert!(out.iter().any(|p| p.contains("background_rate_per_sec")), "{out:?}");
    let out = check(&networks("background_burst = 0", ""));
    assert!(out.iter().any(|p| p.contains("background_burst")), "{out:?}");
    assert!(FileConfig::parse(&networks("background_brust = 2", "")).is_err());
}

/// T-28: the five profile series exist, with their labels.
#[tokio::test]
async fn t28_the_five_series() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let q = ProfileQueue::new(1);
    q.push("a");
    q.push("b");
    let (_, a) = serve(Net::named("bluesky")).await;
    let c = cascade(&[("bluesky", &a)], None, None, None);
    let req = Requester::test();
    c.fetch_profiles(ProfileWalk::Request { req: &req, until: req.deadline() }, &c.table().plan(None), &[FAR.to_string()]).await;
    let cache = ReadCache::new(1024 * 1024);
    let drain = ProfileDrain::new(q.clone(), cache, router(Arc::new(StubLookup::default()), &[]), c);
    drain.drain(q.pop(25)).await;
    let text = handle.render();
    assert_eq!(series(&text, METRIC_QUEUE, &[]), Some(0.0), "{text}");
    assert_eq!(series(&text, METRIC_DROPPED, &[]), Some(1.0), "{text}");
    assert_eq!(series(&text, METRIC_COMPLETED, &[]), Some(1.0), "{text}");
    assert_eq!(series(&text, METRIC_NEGATIVE, &[]), Some(1.0), "{text}");
    assert_eq!(series(&text, METRIC_BATCH_FETCH, &[r#"network="bluesky""#, r#"result="ok""#]), Some(1.0), "{text}");
}
