//! Refract's endpoints over HTTP (refract design §7.1, §13: T-12-r2, T-8s,
//! T-16 and the gate).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::harness::{Harness, OTHER, SUBJECT, tier1};
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
    let fetcher = StubFetcher::default();
    fetcher.set_key(VISITOR, signing_key(3));
    let (auth, _d) = verifier(fetcher, 10, 4);
    let hydrator = Hydrator::new(
        h.pool.clone(),
        h.refract.scope().clone(),
        None,
        None,
        Arc::new(NoUpstream),
        ReadCache::new(1024 * 1024),
        HydrationConfig { live: h.live.clone(), farsight, ..test_config() },
    );
    let state = RefractState {
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
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    format!("http://{addr}")
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
    h.live.update(|v| v.refract_enabled = false);
    let (status, body) = call(&base, FILL, "", None).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("NotEnabled")));
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
    // Answered, the author blocking the visitor: its post is left out.
    *fake.status.lock().expect("status") = 200;
    fake.blockers.lock().expect("blockers").insert(SUBJECT.to_string());
    let (_, body) = call(&base, FILL, &q, None).await;
    assert_eq!(body["posts"], json!([]), "{body}");
    assert_eq!(body["profiles"][0]["viewer"]["blockedBy"], json!(true), "{body}");
    db.drop_db().await;
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}
