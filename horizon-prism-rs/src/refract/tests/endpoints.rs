//! Refract's endpoints over HTTP (refract design §7.1, §13: T-12-r2, T-8s,
//! T-16 and the gate).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::harness::{Harness, OTHER, SUBJECT, tier1, tier2};
use crate::aggregations::ScopeSets;
use crate::hydration::testing::{actor, exec, post, test_config};
use crate::hydration::{HydrationConfig, Hydrator, NoUpstream, ReadCache};
use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
use crate::refract::handlers::{FILL, GET_POST_THREAD, RefractState, router};
use crate::test_support::fresh_db;
use crate::xrpc::{AppState, RateLimits};

const VISITOR: &str = crate::farsight::lookup::tests::V;
const THIRD: &str = "did:plc:thirdaaaaaaaaaaaaaaaaaaa";
const FOURTH: &str = "did:plc:fourthaaaaaaaaaaaaaaaaaa";

async fn serve(h: &Harness, farsight: Option<crate::farsight::Farsight>) -> String {
    let state = refract_state(h, farsight);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    format!("http://{addr}")
}

fn refract_state(h: &Harness, farsight: Option<crate::farsight::Farsight>) -> RefractState {
    refract_state_over(h, farsight, Arc::new(NoUpstream))
}

fn refract_state_over(h: &Harness, farsight: Option<crate::farsight::Farsight>, upstream: Arc<dyn crate::hydration::UpstreamSource>) -> RefractState {
    let fetcher = StubFetcher::default();
    fetcher.set_key(VISITOR, signing_key(3));
    let (auth, _d) = verifier(fetcher, 10, 4);
    let hydrator = Hydrator::new(
        h.pool.clone(),
        h.refract.scope().clone(),
        None,
        None,
        upstream,
        ReadCache::new(1024 * 1024),
        HydrationConfig { live: h.live.clone(), farsight, ..test_config() },
    );
    RefractState {
        app: AppState {
            hydrator,
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: Default::default(),
        },
        refract: h.refract.clone(),
        discovery: Arc::default(),
        round_timeout: Duration::from_millis(500),
    }
}

async fn call(base: &str, method: &str, query: &str, aud_fragment: Option<&str>) -> (u16, Value) {
    let aud = match aud_fragment {
        Some(f) => format!("did:web:{AUDIENCE_HOST}#{f}"),
        None => format!("did:web:{AUDIENCE_HOST}"),
    };
    let token = mint(&signing_key(3), VISITOR, &aud, Some(method));
    let r = reqwest::Client::new()
        .get(format!("{base}/xrpc/{method}?{query}"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("request");
    (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
}

async fn restored_subject(h: &Harness) -> String {
    actor(&h.pool, SUBJECT, "subject.test", "Subject").await;
    let uri = post(&h.pool, SUBJECT, "3kpost", "hello", None, None).await;
    exec(&h.pool, "INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('did:plc:subjectaaaaaaaaaaaaaaaaa', 'sky', '127.0.0.1', 'restored', 'fill')").await;
    h.refract.apply_config().await.expect("load");
    uri
}

/// The gate: `NotEnabled` while the feature is off, `AuthRequired` with no
/// token; never a readiness error for a non-member.
#[tokio::test]
async fn the_gate() {
    let Some(db) = fresh_db("refract_endpoint_gate").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    let base = serve(&h, None).await;
    let r = reqwest::Client::new().get(format!("{base}/xrpc/{FILL}")).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 401);
    let (status, body) = call(&base, FILL, "", None).await;
    assert_eq!((status, body["profiles"].clone()), (200, json!([])), "{body}");
    assert_eq!(body["servedAs"], "visitor", "{body}");
    // RF-S10: a ready member is served as one, with no viewerChecked.
    exec(&h.pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VISITOR}', 'admin', now())")).await;
    let (_, body) = call(&base, FILL, "", None).await;
    assert_eq!(body["servedAs"], "member", "{body}");
    assert!(body.get("viewerChecked").is_none(), "{body}");
    let (_, body) = call(&base, crate::refract::handlers::GET_UNREAD_COUNT, "", None).await;
    assert_eq!(body["servedAs"], "member", "{body}");
    h.live.update(|v| v.refract_enabled = false);
    let (status, body) = call(&base, FILL, "", None).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("NotEnabled")));
    db.drop_db().await;
}

/// `getCommunities`: the enabled communities in list order, their patterns
/// lowercased in list order, a disabled one left out, `max-age=3600`; the
/// gate's `AuthRequired` with no token and `NotEnabled` while off.
#[tokio::test]
async fn get_communities_lists_enabled_patterns() {
    use crate::refract::handlers::{COMMUNITIES_CACHE, GET_COMMUNITIES};
    let Some(db) = fresh_db("refract_endpoint_communities").await else {
        return;
    };
    let mut sky = tier1();
    sky.pds_patterns = vec!["*.Sky.Example".into(), "SKY.example".into()];
    let mut off = tier2();
    off.name = "off".into();
    off.pds_patterns = vec!["*.off.example".into()];
    off.enabled = false;
    let mut north = tier2();
    north.pds_patterns = vec!["127.0.0.1".into()];
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![sky, off, north]).await;
    let base = serve(&h, None).await;
    let r = reqwest::Client::new().get(format!("{base}/xrpc/{GET_COMMUNITIES}")).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 401, "the gate: a token is needed");
    let token = mint(&signing_key(3), VISITOR, &format!("did:web:{AUDIENCE_HOST}"), Some(GET_COMMUNITIES));
    let r = reqwest::Client::new()
        .get(format!("{base}/xrpc/{GET_COMMUNITIES}"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("request");
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(r.headers().get("cache-control").and_then(|v| v.to_str().ok()), Some(COMMUNITIES_CACHE));
    let body: Value = r.json().await.expect("json");
    assert_eq!(
        body,
        json!({"communities": [
            {"name": "sky", "pdsPatterns": ["*.sky.example", "sky.example"]},
            {"name": "north", "pdsPatterns": ["127.0.0.1"]},
        ]}),
    );
    // A community disabled live is left out at once.
    h.live.update(|v| {
        for c in &mut v.refract_communities {
            if c.name == "sky" {
                c.enabled = false;
            }
        }
    });
    let (status, body) = call(&base, GET_COMMUNITIES, "", None).await;
    assert_eq!((status, body), (200, json!({"communities": [{"name": "north", "pdsPatterns": ["127.0.0.1"]}]})));
    h.live.update(|v| v.refract_enabled = false);
    let (status, body) = call(&base, GET_COMMUNITIES, "", None).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("NotEnabled")), "{body}");
    db.drop_db().await;
}

/// T-12-r2, T-8s, T-16: a visitor gets a restored profile (marked) and its
/// post; a direct call makes candidates of unknown DIDs, reported in
/// `pending` the same way whether Prism holds an `actors` row for one or
/// not; a bare-`aud` visitor call makes none; `getPostThread` serves a
/// restored anchor and refuses another.
#[tokio::test]
async fn fill_serves_and_discovers() {
    let Some(db) = fresh_db("refract_endpoint_fill").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    let uri = restored_subject(&h).await;
    actor(&h.pool, FOURTH, "fourth.test", "Fourth").await;
    let base = serve(&h, None).await;
    let q = format!("dids={SUBJECT}&dids={OTHER}&dids={FOURTH}&uris={}", urlencode(&uri));
    let (status, body) = call(&base, FILL, &q, Some("bsky_appview")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["profiles"][0]["did"], SUBJECT);
    assert_eq!(body["profiles"][0]["restored"]["community"], "sky");
    assert_eq!(body["posts"][0]["uri"], json!(uri));
    assert_eq!(body["pending"], json!([OTHER, FOURTH]), "the same for a DID with an actors row");
    // RF-S10, RF-S13: a visitor's answer says so; the switch is off, so
    // nothing was checked.
    assert_eq!(body["servedAs"], "visitor", "{body}");
    assert_eq!(body["withheld"], json!([]), "{body}");
    assert_eq!(body["viewerChecked"], json!({"posts": [], "actors": [], "roots": []}), "{body}");
    // The resolves land: both are candidates.
    for _ in 0..50 {
        if h.count("SELECT count(*) FROM refract_dids WHERE state = 'candidate'").await == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(h.count("SELECT count(*) FROM refract_dids WHERE state = 'candidate'").await, 2);
    // A bare-aud visitor call answers but discovers nothing.
    let (status, body) = call(&base, FILL, &format!("dids={THIRD}"), None).await;
    assert_eq!((status, body["pending"].clone()), (200, json!([])));
    assert_eq!(body["withheld"], json!([{"id": THIRD, "reason": "proxied"}]), "RF-S11: {body}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.state(THIRD).await, None);
    // A candidate already known is pending again, with no new row.
    let (_, body) = call(&base, FILL, &format!("dids={OTHER}"), None).await;
    assert_eq!(body["pending"], json!([OTHER]));
    // getPostThread.
    let (status, body) = call(&base, GET_POST_THREAD, &format!("uri={}", urlencode(&uri)), None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["thread"]["post"]["restored"]["community"], "sky");
    let other = format!("at://{OTHER}/app.bsky.feed.post/3k");
    let (status, body) = call(&base, GET_POST_THREAD, &format!("uri={}", urlencode(&other)), None).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("NotFound")));
    db.drop_db().await;
}

/// T-12-r2 (C-9 (b)): with Farsight configured and the pair unanswered, a
/// visitor gets neither the restored profile nor its post; a restored
/// author that blocks the visitor is left out too.
#[tokio::test]
async fn visitors_unanswered_pairs_are_blocked() {
    let Some(db) = fresh_db("refract_endpoint_unanswered").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    let uri = restored_subject(&h).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    *fake.status.lock().expect("status") = 503;
    let base = serve(&h, Some(farsight)).await;
    let q = format!("dids={SUBJECT}&uris={}", urlencode(&uri));
    let (status, body) = call(&base, FILL, &q, None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!((body["profiles"].clone(), body["posts"].clone()), (json!([]), json!([])), "{body}");
    // RF-S11: both said to be held back, not final.
    assert_eq!(body["withheld"], json!([{"id": SUBJECT, "reason": "unanswered"}, {"id": uri, "reason": "unanswered"}]), "{body}");
    // Answered, the author blocking the visitor: its post is left out.
    *fake.status.lock().expect("status") = 200;
    fake.blockers.lock().expect("blockers").insert(SUBJECT.to_string());
    let (_, body) = call(&base, FILL, &q, None).await;
    assert_eq!(body["posts"], json!([]), "{body}");
    assert_eq!(body["profiles"][0]["viewer"]["blockedBy"], json!(true), "{body}");
    assert_eq!(body["withheld"], json!([{"id": uri, "reason": "blocked"}]), "{body}");
    db.drop_db().await;
}

/// RF-S6 and `fill` leniency: a malformed item is dropped and the rest
/// answered; a DID found on no listed community is left out as final,
/// already on the call that resolved it (the resolve finished inside
/// `fill`'s short wait), and stays left out (no new resolve, no cap) until
/// the communities change.
#[tokio::test]
async fn fill_remembers_not_hosted() {
    let Some(db) = fresh_db("refract_endpoint_not_hosted").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.set(|w| {
        w.pds_of.insert(THIRD.to_string(), "https://pds.elsewhere.example".to_string());
    });
    let base = serve(&h, None).await;
    let (status, body) = call(&base, FILL, &format!("dids={THIRD}&dids=bogus&uris=nope"), Some("bsky_appview")).await;
    assert_eq!((status, body["pending"].clone()), (200, json!([])), "final on the first call: {body}");
    assert_eq!(h.state(THIRD).await, None, "nothing stored about it");
    // Remembered: the next call is answered from the cache.
    let (_, body) = call(&base, FILL, &format!("dids={THIRD}"), Some("bsky_appview")).await;
    assert_eq!(body["pending"], json!([]), "{body}");
    // A change of the communities forgets it.
    let mut wide = tier1();
    wide.name = "elsewhere".into();
    wide.pds_patterns = vec!["*.elsewhere.example".into()];
    h.live.update(|v| v.refract_communities.push(wide));
    let (_, body) = call(&base, FILL, &format!("dids={THIRD}"), Some("bsky_appview")).await;
    assert_eq!(body["pending"], json!([THIRD]), "{body}");
    db.drop_db().await;
}

/// RF-S14: a restored post or profile that couldn't be fetched is
/// `withheld` as `unavailable`; a post this AppView saw deleted is left out
/// as final, not listed.
#[tokio::test]
async fn fill_names_what_it_could_not_fetch() {
    let Some(db) = fresh_db("refract_endpoint_unavailable").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    let uri = restored_subject(&h).await;
    // A second restored account with nothing stored, and no upstream.
    exec(&h.pool, &format!("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('{FOURTH}', 'sky', '127.0.0.1', 'restored', 'fill')")).await;
    h.refract.apply_config().await.expect("load");
    let unfetched = format!("at://{SUBJECT}/app.bsky.feed.post/3kunfetched");
    let deleted = format!("at://{SUBJECT}/app.bsky.feed.post/3kdeleted");
    exec(&h.pool, &format!("INSERT INTO record_tombstones (uri, rev) VALUES ('{deleted}', '3kaaaaaaaaaa9')")).await;
    let base = serve(&h, None).await;
    let q = format!("dids={SUBJECT}&dids={FOURTH}&uris={}&uris={}&uris={}", urlencode(&uri), urlencode(&unfetched), urlencode(&deleted));
    let (status, body) = call(&base, FILL, &q, Some("bsky_appview")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["posts"].as_array().map(Vec::len), Some(1), "{body}");
    assert_eq!(body["profiles"].as_array().map(Vec::len), Some(1), "{body}");
    assert_eq!(body["withheld"], json!([{"id": FOURTH, "reason": "unavailable"}, {"id": unfetched, "reason": "unavailable"}]), "{body}");
    db.drop_db().await;
}

/// RF-S16: an account the checker restores after the request's snapshot
/// was taken (the snapshot here predates the row) is reported `pending`,
/// never dropped as if final; one restored and pending removal stays out.
#[tokio::test]
async fn restored_after_the_snapshot_is_pending() {
    let Some(db) = fresh_db("refract_endpoint_snapshot_race").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    let s = refract_state(&h, None);
    // The request's snapshot, taken before the restore.
    let stale = crate::refract::RefractSnapshot::default();
    h.row(OTHER, "sky", "restored").await;
    h.row(FOURTH, "sky", "restored").await;
    exec(&h.pool, "INSERT INTO refract_pending (scope) VALUES ('test')").await;
    exec(&h.pool, &format!("INSERT INTO refract_pending_dids (did, scope) VALUES ('{FOURTH}', 'test')")).await;
    let caller = crate::refract::handlers::Caller {
        did: VISITOR.to_string(),
        viewer: crate::hydration::Viewer { did: Some(VISITOR.to_string()), hydrate: false },
        req: crate::xrpc::pools::Requester::test(),
        ip: "192.0.2.1".parse().expect("ip"),
        member: false,
        direct: true,
    };
    let found = s.discover(&caller, &[OTHER.to_string(), FOURTH.to_string()], &stale).await.expect("discover");
    assert_eq!(found.pending, [OTHER], "{found:?}");
    assert!(found.withheld.is_empty(), "{found:?}");
    db.drop_db().await;
}

fn direct_visitor() -> crate::refract::handlers::Caller {
    crate::refract::handlers::Caller {
        did: VISITOR.to_string(),
        viewer: crate::hydration::Viewer { did: Some(VISITOR.to_string()), hydrate: false },
        req: crate::xrpc::pools::Requester::test(),
        ip: "192.0.2.1".parse().expect("ip"),
        member: false,
        direct: true,
    }
}

/// RF-S15: a DID whose resolve is in flight, or failed moments ago, is
/// pending again with no cap charged and no second resolve; once the
/// moment has passed it is resolved (and charged) again.
#[tokio::test]
async fn resolves_in_flight_or_just_failed_are_not_repeated() {
    use crate::refract::handlers::{Cap, Discovery};
    let Some(db) = fresh_db("refract_endpoint_resolving").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    let mut s = refract_state(&h, None);
    s.discovery = Arc::new(Discovery::with_failure_memo(Duration::from_millis(1500)));
    let caller = direct_visitor();
    let snap = crate::refract::RefractSnapshot::default();
    let dids = [THIRD.to_string()];
    let plc_calls = || h.world.lock().expect("world").plc_calls.get(THIRD).copied().unwrap_or_default();
    // In flight: a slow directory.
    h.set(|w| {
        w.plc_delay = Some(Duration::from_millis(400));
        w.plc_fail.insert(THIRD.to_string());
    });
    let first = s.discover(&caller, &dids, &snap).await.expect("discover");
    let again = s.discover(&caller, &dids, &snap).await.expect("discover");
    assert_eq!((first.pending.as_slice(), again.pending.as_slice()), (&dids[..], &dids[..]));
    assert!(again.withheld.is_empty(), "{again:?}");
    assert_eq!(s.discovery.taken(Cap::Caller, VISITOR), 1, "the re-ask is not charged");
    assert_eq!(s.discovery.taken(Cap::Ip, "192.0.2.1"), 1);
    assert_eq!(s.discovery.taken(Cap::Global, ""), 1);
    // The resolve fails (503): remembered for a moment.
    let mut failed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if s.discovery.resolving.failed(THIRD) {
            failed = true;
            break;
        }
    }
    assert!(failed, "the resolve failed");
    let calls = plc_calls();
    assert!(calls > 0, "the resolve ran");
    let after_fail = s.discover(&caller, &dids, &snap).await.expect("discover");
    assert_eq!(after_fail.pending, dids, "{after_fail:?}");
    assert_eq!(s.discovery.taken(Cap::Caller, VISITOR), 1, "not charged while remembered");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(plc_calls(), calls, "no second resolve");
    assert_eq!(h.state(THIRD).await, None, "nothing stored");
    // Past the moment: resolved and charged again.
    h.set(|w| w.plc_delay = None);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let later = s.discover(&caller, &dids, &snap).await.expect("discover");
    assert_eq!(later.pending, dids);
    assert_eq!(s.discovery.taken(Cap::Caller, VISITOR), 2, "a new resolve is charged");
    db.drop_db().await;
}

/// An upstream whose community AppView answers every profile after a
/// pause, and one DID's never in time.
struct SlowProfiles {
    stuck: String,
}

#[async_trait::async_trait]
impl crate::hydration::UpstreamSource for SlowProfiles {
    async fn profile(&self, _req: &crate::xrpc::pools::Requester, _actor: &str) -> Option<Value> {
        None
    }
    async fn fetch_profiles(&self, _req: &crate::xrpc::pools::Requester, dids: &[String], _until: std::time::Instant) -> crate::constellation::ProfilesFetched {
        let pause = if dids.contains(&self.stuck) { 10_000 } else { 300 };
        tokio::time::sleep(Duration::from_millis(pause)).await;
        let mut out = crate::constellation::ProfilesFetched::default();
        for d in dids {
            out.found.insert(d.clone(), json!({"did": d, "handle": "restored.test"}));
        }
        out
    }
    async fn posts(&self, _req: &crate::xrpc::pools::Requester, _uris: &[String]) -> Vec<Value> {
        Vec::new()
    }
    async fn post_thread(&self, _req: &crate::xrpc::pools::Requester, _uri: &str, _depth: u32, _parent_height: u32) -> Option<(Value, crate::hydration::upstream::ViewSource)> {
        None
    }
    async fn author_feed(&self, _req: &crate::xrpc::pools::Requester, _actor: &str, _limit: u32, _cursor: Option<&str>, _filter: &str) -> Option<Value> {
        None
    }
    async fn follows(&self, _req: &crate::xrpc::pools::Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
}

/// RF-S4: a `fill`'s cold restored profiles are walked in parallel, so
/// twelve 300 ms walks end well inside a 2.5 s deadline (in turn they
/// would take 3.6 s); one not walked in time is `unavailable`.
#[tokio::test]
async fn fill_walks_cold_profiles_in_parallel() {
    let Some(db) = fresh_db("refract_endpoint_parallel_profiles").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    let dids: Vec<String> = (b'a'..b'm').map(|c| format!("did:plc:cold{}{}", char::from(c), "a".repeat(19))).collect();
    let stuck = format!("did:plc:stuck{}", "a".repeat(19));
    for d in dids.iter().chain([&stuck]) {
        exec(&h.pool, &format!("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('{d}', 'sky', '127.0.0.1', 'restored', 'fill')")).await;
    }
    h.refract.apply_config().await.expect("load");
    let s = refract_state_over(&h, None, Arc::new(SlowProfiles { stuck: stuck.clone() }));
    let mut named = dids.clone();
    named.push(stuck.clone());
    let viewer = crate::hydration::Viewer { did: Some(VISITOR.to_string()), hydrate: false };
    let started = std::time::Instant::now();
    let req = crate::xrpc::pools::Requester::test().with_deadline(started + Duration::from_millis(2500));
    let body = s.app.hydrator.refract_fill(&viewer, &req, &named, &[], &[], &[]).await.expect("fill");
    let body = body.inner().clone();
    let took = started.elapsed();
    assert!(took < Duration::from_millis(2500), "took {took:?}");
    let served: Vec<&str> = body["profiles"].as_array().expect("profiles").iter().filter_map(|p| p["did"].as_str()).collect();
    assert_eq!(served.len(), dids.len(), "{body}");
    assert!(dids.iter().all(|d| served.contains(&d.as_str())), "{body}");
    assert_eq!(body["withheld"], json!([{"id": stuck, "reason": "unavailable"}]), "{body}");
    db.drop_db().await;
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

const FIFTH: &str = "did:plc:fifthaaaaaaaaaaaaaaaaaaa";

/// Four accounts matching "ann" and one not: SUBJECT `ann.test`, OTHER
/// `annabel.test` and THIRD `aaron.test` ("Ánn.test fan") restored, FOURTH
/// `annie.test` ending, FIFTH `ann.zone` with no Refract row.
async fn search_world(h: &Harness) {
    for (did, handle, name) in [
        (SUBJECT, "ann.test", "Zed"),
        (OTHER, "annabel.test", "Bob"),
        (THIRD, "aaron.test", "Ánn.test fan"),
        (FOURTH, "annie.test", "Annie"),
        (FIFTH, "ann.zone", "Ann"),
    ] {
        actor(&h.pool, did, handle, name).await;
    }
    for (did, state) in [(SUBJECT, "restored"), (OTHER, "restored"), (THIRD, "restored"), (FOURTH, "ending")] {
        exec(&h.pool, &format!("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('{did}', 'sky', '127.0.0.1', '{state}', 'fill')")).await;
    }
    h.refract.apply_config().await.expect("load");
}

fn handles(body: &Value) -> Vec<String> {
    body["actors"].as_array().expect("actors").iter().map(|a| a["handle"].as_str().unwrap_or_default().to_string()).collect()
}

/// The actor searches: restored accounts only (not an ending one, not one
/// with no Refract row), by handle or display-name prefix, case,
/// diacritics and a leading @ ignored; exact handle, then handle prefix,
/// then display-name prefix, by handle within each; every view marked; the
/// typeahead basic and unpaged, the search a `profileView` paged by a
/// cursor; `q` checked; a ready member's full mute leaves an account out.
#[tokio::test]
async fn search_actors_order_fold_and_page() {
    use crate::refract::handlers::{SEARCH_ACTORS, SEARCH_ACTORS_TYPEAHEAD};
    let Some(db) = fresh_db("refract_endpoint_search").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    search_world(&h).await;
    let base = serve(&h, None).await;
    let (status, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, "q=ann", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(handles(&body), ["ann.test", "annabel.test", "aaron.test"], "{body}");
    for a in body["actors"].as_array().expect("actors") {
        assert_eq!(a["restored"]["community"], "sky", "{a}");
        assert!(a.get("indexedAt").is_none(), "profileViewBasic: {a}");
    }
    assert_eq!(body["servedAs"], "visitor", "{body}");
    assert_eq!(body["viewerChecked"], json!({"posts": [], "actors": [], "roots": []}), "{body}");
    assert!(body.get("cursor").is_none(), "{body}");
    // The exact handle before a display-name match whose handle sorts first.
    let (_, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, &format!("q={}", urlencode(" @ANN.Test ")), None).await;
    assert_eq!(handles(&body), ["ann.test", "aaron.test"], "{body}");
    // Diacritics folded both ways.
    let (_, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, &format!("q={}", urlencode("ånn.")), None).await;
    assert_eq!(handles(&body), ["ann.test", "aaron.test"], "{body}");
    let (_, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, "q=ann&limit=2", None).await;
    assert_eq!(handles(&body), ["ann.test", "annabel.test"], "{body}");
    // searchActors: profileViews, paged.
    let (status, body) = call(&base, SEARCH_ACTORS, "q=ann&limit=2", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(handles(&body), ["ann.test", "annabel.test"], "{body}");
    assert!(body["actors"][0].get("indexedAt").is_some(), "profileView: {body}");
    let cursor = body["cursor"].as_str().expect("cursor").to_string();
    let (_, body) = call(&base, SEARCH_ACTORS, &format!("q=ann&limit=2&cursor={}", urlencode(&cursor)), None).await;
    assert_eq!(handles(&body), ["aaron.test"], "{body}");
    assert!(body.get("cursor").is_none(), "{body}");
    // An account restored meanwhile does not move the cursor's place.
    actor(&h.pool, "did:plc:annaaaaaaaaaaaaaaaaaaaaa", "ann.a", "A").await;
    exec(&h.pool, "INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('did:plc:annaaaaaaaaaaaaaaaaaaaaa', 'sky', '127.0.0.1', 'restored', 'fill')").await;
    h.refract.apply_config().await.expect("load");
    let (_, body) = call(&base, SEARCH_ACTORS, &format!("q=ann&limit=2&cursor={}", urlencode(&cursor)), None).await;
    assert_eq!(handles(&body), ["aaron.test"], "{body}");
    // `q` checked.
    let (status, body) = call(&base, SEARCH_ACTORS, "q=%20%20", None).await;
    assert_eq!((status, body["actors"].clone()), (200, json!([])), "{body}");
    let (status, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, &format!("q={}", "a".repeat(101)), None).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("InvalidRequest")), "{body}");
    let (status, body) = call(&base, SEARCH_ACTORS, "q=ann&cursor=junk", None).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("InvalidRequest")), "{body}");
    // A ready member: served as one; its full mute applies.
    exec(&h.pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VISITOR}', 'admin', now())")).await;
    exec(&h.pool, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{VISITOR}', '{OTHER}', 'full')")).await;
    let (_, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, "q=ann", None).await;
    assert_eq!(handles(&body), ["ann.a", "ann.test", "aaron.test"], "{body}");
    assert_eq!(body["servedAs"], "member", "{body}");
    // Off: NotEnabled.
    h.live.update(|v| v.refract_enabled = false);
    let (status, body) = call(&base, SEARCH_ACTORS, "q=ann", None).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("NotEnabled")));
    db.drop_db().await;
}

/// The actor searches for a visitor (C-9 (b)): an account that blocks the
/// visitor is left out; with the pairs unanswered every match is left out
/// and listed in `withheld`.
#[tokio::test]
async fn search_actors_visitor_blocks() {
    use crate::refract::handlers::SEARCH_ACTORS_TYPEAHEAD;
    let Some(db) = fresh_db("refract_endpoint_search_blocks").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    search_world(&h).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    *fake.status.lock().expect("status") = 503;
    let base = serve(&h, Some(farsight)).await;
    let (status, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, "q=ann", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(handles(&body), Vec::<String>::new(), "{body}");
    let unanswered = |d: &str| json!({"id": d, "reason": "unanswered"});
    assert_eq!(body["withheld"], json!([unanswered(SUBJECT), unanswered(OTHER), unanswered(THIRD)]), "{body}");
    // Answered, OTHER blocking the visitor: left out, nothing withheld.
    *fake.status.lock().expect("status") = 200;
    fake.blockers.lock().expect("blockers").insert(OTHER.to_string());
    let (_, body) = call(&base, SEARCH_ACTORS_TYPEAHEAD, "q=ann", None).await;
    assert_eq!(handles(&body), ["ann.test", "aaron.test"], "{body}");
    assert_eq!(body["withheld"], json!([]), "{body}");
    db.drop_db().await;
}
