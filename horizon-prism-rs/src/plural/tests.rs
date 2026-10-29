//! Plural-pronouns design §10: T-P4 to T-P8 and T-P10.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::fill::{Filler, LookupError, Outcome, PdsLookup};
use super::handlers::{CACHE_KNOWN, CACHE_UNKNOWN_ANON, Freshness, PluralState, router};
use super::*;
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::{PluralConfig, ScopePolicy};
use crate::hydration::testing::hydrator;
use crate::kryphocron_glue::jwt::tests::{StubFetcher, verifier};
use crate::net::client::NetClient;
use crate::test_support::fresh_db;
use crate::xrpc::{AppState, RateLimits};

const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
const TOMB: &str = "did:plc:tttttttttttttttttttttttt";

/// Records by `(did, collection)`.
type Listing = HashMap<(String, String), Vec<Value>>;

/// A PDS serving `listRecords` from a table, with an error per DID.
#[derive(Clone, Default)]
struct Pds {
    records: Arc<Mutex<Listing>>,
    errors: Arc<Mutex<HashMap<String, (u16, String)>>>,
    calls: Arc<Mutex<u32>>,
}

impl Pds {
    fn put(&self, did: &str, collection: &str, rkey: &str, value: Value) {
        let cid = format!("bafy{}", value["n"]);
        let rec = json!({"uri": record_uri(did, collection, rkey), "cid": cid, "value": value});
        let mut m = self.records.lock().expect("lock");
        let list = m.entry((did.to_string(), collection.to_string())).or_default();
        list.retain(|r| r["uri"] != rec["uri"]);
        list.push(rec);
    }

    fn clear(&self, did: &str, collection: &str) {
        self.records.lock().expect("lock").remove(&(did.to_string(), collection.to_string()));
    }

    fn fail(&self, did: &str, status: u16, name: &str) {
        self.errors.lock().expect("lock").insert(did.to_string(), (status, name.to_string()));
    }
}

async fn list(State(p): State<Pds>, Query(q): Query<HashMap<String, String>>) -> Response {
    *p.calls.lock().expect("lock") += 1;
    let did = q.get("repo").cloned().unwrap_or_default();
    if let Some((status, name)) = p.errors.lock().expect("lock").get(&did).cloned() {
        return (StatusCode::from_u16(status).expect("status"), axum::Json(json!({"error": name, "message": "x"}))).into_response();
    }
    let collection = q.get("collection").cloned().unwrap_or_default();
    let limit: usize = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(50);
    let start: usize = q.get("cursor").and_then(|c| c.parse().ok()).unwrap_or(0);
    let all = p.records.lock().expect("lock").get(&(did, collection)).cloned().unwrap_or_default();
    let page: Vec<Value> = all.iter().skip(start).take(limit).cloned().collect();
    let mut out = json!({"records": page});
    if start + limit < all.len() {
        out["cursor"] = json!((start + limit).to_string());
    }
    axum::Json(out).into_response()
}

struct Lookup(String);

#[async_trait]
impl PdsLookup for Lookup {
    async fn pds(&self, did: &str) -> Result<String, LookupError> {
        if did == TOMB { Err(LookupError::Tombstoned) } else { Ok(self.0.clone()) }
    }
}

async fn serve_pds(pds: Pds) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = axum::Router::new().route("/xrpc/com.atproto.repo.listRecords", axum::routing::get(list)).with_state(pds);
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    format!("http://{addr}")
}

fn filler(pool: &sqlx::PgPool, pds: &str, cfg: &PluralConfig) -> Filler {
    let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10);
    Filler::new(pool.clone(), NetClient::loopback_for_tests(), Arc::new(Lookup(pds.to_string())), scope, cfg)
}

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::query(sql).execute(pool).await.expect(sql);
}

async fn repo_state(pool: &sqlx::PgPool, did: &str) -> Option<(String, bool)> {
    sqlx::query_as("SELECT state, members_truncated FROM plural_repos WHERE did = $1").bind(did).fetch_optional(pool).await.expect("state")
}

async fn stored(pool: &sqlx::PgPool, did: &str) -> Vec<(String, String, String)> {
    sqlx::query_as("SELECT rkey, cid, rev FROM plural_records WHERE did = $1 ORDER BY uri").bind(did).fetch_all(pool).await.expect("rows")
}

/// T-P5 (§2.5, R2): every row of the answer table, in order.
#[test]
fn t_p5_the_answer_table() {
    let uri = "at://did:plc:x/app.nearhorizon.actor.pronouns/self";
    let row = |state: RepoState, truncated: bool| RepoRow { state, verified_at: None, next_fill_at: None, members_truncated: truncated };
    let rec = Stored { cid: "c".into(), value: Some(json!({"sets": []})) };
    let big = Stored { cid: "c".into(), value: None };
    let f = |hidden: bool, repo: Option<&RepoRow>, stored: Option<&Stored>, fresh: bool, member: bool| answer(uri, Facts { hidden, repo, stored, fresh, member });
    let complete = row(RepoState::Complete, false);
    let record = Answer::Record { uri: uri.into(), cid: "c".into(), value: json!({"sets": []}) };
    // 1. hidden
    assert_eq!(f(true, Some(&complete), Some(&rec), true, false), Answer::Unknown);
    // 2. unavailable, whatever is stored
    assert_eq!(f(false, Some(&row(RepoState::Unavailable, false)), Some(&rec), true, false), Answer::Unknown);
    // 3. gone, whatever is stored
    assert_eq!(f(false, Some(&row(RepoState::Gone, false)), Some(&rec), true, false), Answer::None);
    // 4. stored, whatever the state
    for state in [None, Some(row(RepoState::Pending, false)), Some(row(RepoState::Failed, false)), Some(complete.clone())] {
        assert_eq!(f(false, state.as_ref(), Some(&rec), false, false), record, "{state:?}");
    }
    // 5. stored without a value
    assert_eq!(f(false, Some(&complete), Some(&big), true, false), Answer::Unknown);
    // 6. none: complete, fresh, untruncated for members
    assert_eq!(f(false, Some(&complete), None, true, false), Answer::None);
    assert_eq!(f(false, Some(&complete), None, true, true), Answer::None);
    assert_eq!(f(false, Some(&row(RepoState::Complete, true)), None, true, false), Answer::None, "truncation is members' only");
    // 7. otherwise unknown
    assert_eq!(f(false, Some(&row(RepoState::Complete, true)), None, true, true), Answer::Unknown);
    assert_eq!(f(false, Some(&complete), None, false, false), Answer::Unknown, "stale firehose");
    for state in [None, Some(row(RepoState::Pending, false)), Some(row(RepoState::Failed, false))] {
        assert_eq!(f(false, state.as_ref(), None, true, false), Answer::Unknown, "{state:?}");
    }
}

#[test]
fn member_uris_parse_strictly() {
    let ok = format!("at://{ALICE}/app.nearhorizon.plural.member/3kwren");
    assert_eq!(parse_member_uri(&ok), Some((ALICE, "3kwren")));
    for bad in [
        format!("at://{ALICE}/app.nearhorizon.plural.system/self"),
        format!("at://{ALICE}/app.nearhorizon.plural.member/"),
        format!("at://{ALICE}/app.nearhorizon.plural.member/a/b"),
        "at://nope/app.nearhorizon.plural.member/3k".to_string(),
        format!("{ALICE}/app.nearhorizon.plural.member/3k"),
    ] {
        assert_eq!(parse_member_uri(&bad), None, "{bad}");
    }
    assert_eq!(fill::backoff(1).as_secs(), 60);
    assert_eq!(fill::backoff(5).as_secs(), 21_600);
    assert_eq!(fill::backoff(50).as_secs(), 21_600);
}

/// T-P4 (§2.4, R1, R3, R4): the fill.
#[tokio::test]
async fn t_p4_the_fill() {
    let Some(db) = fresh_db("plural_t_p4_fill").await else {
        return;
    };
    let p = &db.pool;
    let pds = Pds::default();
    let url = serve_pds(pds.clone()).await;
    let cfg = PluralConfig { member_pages: 2, ..PluralConfig::default() };
    let f = filler(p, &url, &cfg);
    let member = NSID_PLURAL_MEMBER;

    // All four collections listed; non-self rkeys of the self-keyed ones
    // are left out; rows at the sentinel rev.
    pds.put(ALICE, NSID_PRONOUNS, "self", json!({"n": 1, "sets": [{"forms": ["she", "her"]}]}));
    pds.put(ALICE, NSID_PLURAL_SYSTEM, "3knotself", json!({"n": 2}));
    pds.put(ALICE, member, "3kwren", json!({"n": 3, "displayName": "Wren"}));
    assert_eq!(f.fill_now(ALICE).await.expect("fill"), Outcome::Complete { truncated: false });
    assert_eq!(*pds.calls.lock().expect("lock"), 4, "one listRecords per collection, no describeRepo");
    let rows = stored(p, ALICE).await;
    assert_eq!(rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(), ["self", "3kwren"]);
    assert!(rows.iter().all(|r| r.2 == crate::firehose::writer::SENTINEL_REV));
    assert_eq!(repo_state(p, ALICE).await, Some(("complete".into(), false)));

    // A row written before the listing and gone from the repo is deleted;
    // a changed CID is updated; a row written after the listing began stays.
    pds.clear(ALICE, NSID_PRONOUNS);
    pds.put(ALICE, member, "3kwren", json!({"n": 4, "displayName": "Robin"}));
    exec(p, "UPDATE plural_records SET indexed_at = now() - interval '1 hour'").await;
    exec(
        p,
        "INSERT INTO plural_records (uri, did, rkey, cid, rev, collection, record, indexed_at) VALUES \
         ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.nearhorizon.plural.member/3klater', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '3klater', 'bafylater', '3kzzzzzzzzzzz', 'app.nearhorizon.plural.member', '{\"displayName\": \"Later\"}', now() + interval '1 minute')",
    )
    .await;
    // A delete after the listing began: its tombstone keeps the record out.
    pds.put(ALICE, member, "3kgone", json!({"n": 5}));
    exec(p, "INSERT INTO record_tombstones (uri, rev, deleted_at) VALUES ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.nearhorizon.plural.member/3kgone', '3kzzzzzzzzzzz', now() + interval '1 minute')").await;
    assert_eq!(f.fill_now(ALICE).await.expect("fill"), Outcome::Complete { truncated: false });
    let rows = stored(p, ALICE).await;
    assert_eq!(rows.iter().map(|r| (r.0.as_str(), r.1.as_str())).collect::<Vec<_>>(), [("3klater", "bafylater"), ("3kwren", "bafy4")]);

    // Truncated: more than member_pages pages; no member row is deleted.
    for i in 0..250 {
        pds.put(BOB, member, &format!("3km{i:04}"), json!({"n": 100 + i}));
    }
    exec(p, &format!("INSERT INTO plural_records (uri, did, rkey, cid, rev, collection, record, indexed_at) VALUES ('at://{BOB}/{member}/3kzold', '{BOB}', '3kzold', 'c', 'r', '{member}', '{{}}', now() - interval '1 hour')")).await;
    assert_eq!(f.fill_now(BOB).await.expect("fill"), Outcome::Complete { truncated: true });
    assert_eq!(repo_state(p, BOB).await, Some(("complete".into(), true)));
    assert_eq!(stored(p, BOB).await.len(), 201, "200 listed, the unlisted old row kept");

    // A deactivated repo: unavailable, its rows deleted, a next fill set.
    pds.fail(BOB, 400, "RepoDeactivated");
    assert_eq!(f.fill_now(BOB).await.expect("fill"), Outcome::Unavailable("RepoDeactivated".into()));
    assert!(stored(p, BOB).await.is_empty());
    let next: Option<f64> = sqlx::query_scalar("SELECT extract(epoch FROM next_fill_at - now())::float8 FROM plural_repos WHERE did = $1").bind(BOB).fetch_one(p).await.expect("next");
    assert!(next.is_some_and(|s| s > 30.0), "{next:?}");

    // A 500: failed, rows kept, nothing written.
    pds.put(CAROL, NSID_PRONOUNS, "self", json!({"n": 9}));
    exec(p, &format!("INSERT INTO plural_records (uri, did, rkey, cid, rev, collection, record) VALUES ('at://{CAROL}/{member}/3kkeep', '{CAROL}', '3kkeep', 'c', 'r', '{member}', '{{}}')")).await;
    pds.fail(CAROL, 500, "InternalServerError");
    assert!(matches!(f.fill_now(CAROL).await.expect("fill"), Outcome::Failed(_)));
    assert_eq!(stored(p, CAROL).await.len(), 1);
    assert_eq!(repo_state(p, CAROL).await.map(|s| s.0), Some("failed".into()));
    // A plain InvalidRequest is not taken for absence (o1).
    pds.fail(CAROL, 400, "InvalidRequest");
    assert!(matches!(f.fill_now(CAROL).await.expect("fill"), Outcome::Failed(_)));

    // A tombstoned DID: gone, rows deleted.
    exec(p, &format!("INSERT INTO plural_records (uri, did, rkey, cid, rev, collection, record) VALUES ('at://{TOMB}/{member}/3k', '{TOMB}', '3k', 'c', 'r', '{member}', '{{}}')")).await;
    assert_eq!(f.fill_now(TOMB).await.expect("fill"), Outcome::Gone);
    assert!(stored(p, TOMB).await.is_empty());
    assert_eq!(repo_state(p, TOMB).await.map(|s| s.0), Some("gone".into()));
    db.drop_db().await;
}

struct Served {
    base: String,
    pool: sqlx::PgPool,
    fresh: bool,
}

async fn serve_plural(pool: &sqlx::PgPool, pds_url: &str, cfg: &PluralConfig, fresh: bool, sets: ScopeSets) -> Served {
    serve_plural_limited(pool, pds_url, cfg, fresh, sets, RateLimits::new()).await
}

async fn serve_plural_limited(pool: &sqlx::PgPool, pds_url: &str, cfg: &PluralConfig, fresh: bool, sets: ScopeSets, limits: RateLimits) -> Served {
    let (auth, _d) = verifier(StubFetcher::default(), 10, 4);
    let app = AppState {
        hydrator: hydrator(pool, sets),
        auth: Arc::new(auth),
        limits,
        enroller: None,
        trusted_proxy: "172.30.0.10".parse().expect("ip"),
        did_document: Arc::new(json!({})),
        membership: Default::default(),
    };
    let state = PluralState { app, filler: filler(pool, pds_url, cfg), fresh: Freshness::Fixed(fresh) };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    Served { base: format!("http://{addr}/xrpc"), pool: pool.clone(), fresh }
}

async fn get(url: &str) -> (u16, Option<String>, Value) {
    let r = reqwest::get(url).await.expect("request");
    let cache = r.headers().get("cache-control").map(|v| v.to_str().expect("ascii").to_string());
    (r.status().as_u16(), cache, r.json().await.unwrap_or(Value::Null))
}

/// T-P6 (§2.8): getRecords.
#[tokio::test]
async fn t_p6_get_records() {
    let Some(db) = fresh_db("plural_t_p6_get_records").await else {
        return;
    };
    let p = &db.pool;
    let pds = Pds::default();
    let url = serve_pds(pds.clone()).await;
    pds.put(ALICE, NSID_PRONOUNS, "self", json!({"n": 1, "sets": [{"forms": ["they", "them"]}]}));
    pds.put(ALICE, NSID_PLURAL_SYSTEM, "self", json!({"n": 2, "name": "The Flock"}));
    pds.put(ALICE, NSID_PLURAL_MEMBER, "3kwren", json!({"n": 3, "displayName": "Wren"}));
    let s = serve_plural(p, &url, &PluralConfig::default(), true, ScopeSets::default()).await;
    assert!(s.fresh);
    let wren = format!("at://{ALICE}/{NSID_PLURAL_MEMBER}/3kwren");
    let ghost = format!("at://{ALICE}/{NSID_PLURAL_MEMBER}/3kghost");
    // A cold DID is listed inline; answers in request order, deduped.
    let (status, cache, v) = get(&format!("{}/{GET_RECORDS}?actors={ALICE}&actors={ALICE}&members={wren}&members={ghost}", s.base)).await;
    assert_eq!(status, 200, "{v}");
    assert_eq!(cache.as_deref(), Some(CACHE_KNOWN));
    assert_eq!(v["actors"].as_array().map(Vec::len), Some(1));
    let a = &v["actors"][0];
    assert_eq!(a["did"], ALICE);
    assert_eq!(a["pronouns"]["status"], "record");
    assert_eq!(a["pronouns"]["cid"], "bafy1");
    assert_eq!(a["pronouns"]["uri"], record_uri(ALICE, NSID_PRONOUNS, "self"));
    assert_eq!(a["system"]["value"]["name"], "The Flock");
    assert_eq!(a["front"], json!({"status": "none"}));
    assert_eq!(v["members"][0]["uri"], json!(wren));
    assert_eq!(v["members"][0]["value"]["displayName"], "Wren");
    assert_eq!(v["members"][1], json!({"status": "none", "uri": ghost}));

    // A gate-hidden DID answers unknown.
    exec(&s.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{ALICE}', '!takedown')")).await;
    let (_, cache, v) = get(&format!("{}/{GET_RECORDS}?actors={ALICE}", s.base)).await;
    assert_eq!(v["actors"][0]["pronouns"], json!({"status": "unknown"}));
    assert_eq!(cache.as_deref(), Some(CACHE_UNKNOWN_ANON));
    exec(&s.pool, "DELETE FROM labels").await;

    // Input errors.
    for q in [
        String::new(),
        "actors=not-a-did".to_string(),
        format!("members=at://{ALICE}/app.nearhorizon.plural.system/self"),
        (0..51).map(|i| format!("actors=did:plc:{i:024}")).collect::<Vec<_>>().join("&"),
        (0..101).map(|i| format!("members=at://{ALICE}/{NSID_PLURAL_MEMBER}/3k{i}")).collect::<Vec<_>>().join("&"),
    ] {
        let (status, _, v) = get(&format!("{}/{GET_RECORDS}?{q}", s.base)).await;
        assert_eq!((status, v["error"].as_str()), (400, Some("InvalidRequest")), "{q}");
    }
    db.drop_db().await;
}

/// T-P5 with the firehose behind: no `none`.
#[tokio::test]
async fn t_p5_stale_firehose_gives_no_none() {
    let Some(db) = fresh_db("plural_t_p5_stale").await else {
        return;
    };
    let pds = Pds::default();
    let url = serve_pds(pds.clone()).await;
    let s = serve_plural(&db.pool, &url, &PluralConfig::default(), false, ScopeSets::default()).await;
    let (_, cache, v) = get(&format!("{}/{GET_RECORDS}?actors={BOB}", s.base)).await;
    assert_eq!(v["actors"][0]["pronouns"], json!({"status": "unknown"}), "{v}");
    assert_eq!(cache.as_deref(), Some(CACHE_UNKNOWN_ANON));
    assert_eq!(repo_state(&db.pool, BOB).await.map(|r| r.0), Some("complete".into()), "listed, but not answered as absent");
    db.drop_db().await;
}

/// T-P7 (§2.8): listMembers.
#[tokio::test]
async fn t_p7_list_members() {
    let Some(db) = fresh_db("plural_t_p7_list_members").await else {
        return;
    };
    let p = &db.pool;
    let pds = Pds::default();
    let url = serve_pds(pds.clone()).await;
    for rkey in ["3kc", "3ka", "3kb"] {
        pds.put(ALICE, NSID_PLURAL_MEMBER, rkey, json!({"n": rkey, "displayName": rkey}));
    }
    let s = serve_plural(p, &url, &PluralConfig::default(), true, ScopeSets::default()).await;
    let (status, cache, v) = get(&format!("{}/{LIST_MEMBERS}?actor={ALICE}&limit=2", s.base)).await;
    assert_eq!(status, 200, "{v}");
    assert_eq!(cache.as_deref(), Some(CACHE_KNOWN));
    assert_eq!(v["status"], "complete");
    let rkeys = |v: &Value| v["members"].as_array().expect("members").iter().map(|m| m["rkey"].as_str().unwrap_or_default().to_string()).collect::<Vec<_>>();
    assert_eq!(rkeys(&v), ["3ka", "3kb"]);
    assert_eq!(v["cursor"], "3kb");
    let (_, _, v) = get(&format!("{}/{LIST_MEMBERS}?actor={ALICE}&limit=2&cursor=3kb", s.base)).await;
    assert_eq!(rkeys(&v), ["3kc"]);
    assert!(v.get("cursor").is_none());
    // A hidden member is left out; a NULL-record member makes the page partial.
    exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('at://{ALICE}/{NSID_PLURAL_MEMBER}/3ka', '!takedown')")).await;
    exec(p, "UPDATE plural_records SET record = NULL WHERE rkey = '3kb'").await;
    let (_, cache, v) = get(&format!("{}/{LIST_MEMBERS}?actor={ALICE}", s.base)).await;
    assert_eq!((v["status"].as_str(), rkeys(&v)), (Some("partial"), vec!["3kc".to_string()]));
    assert_eq!(cache.as_deref(), Some(CACHE_UNKNOWN_ANON));
    // Not listable: unknown.
    pds.fail(BOB, 500, "InternalServerError");
    let (_, _, v) = get(&format!("{}/{LIST_MEMBERS}?actor={BOB}", s.base)).await;
    assert_eq!(v, json!({"actor": BOB, "status": "unknown", "members": []}));
    let (status, _, _) = get(&format!("{}/{LIST_MEMBERS}?actor={ALICE}&limit=101", s.base)).await;
    assert_eq!(status, 400);
    db.drop_db().await;
}

/// T-P8 (§2.8): disabled, both answer 501 from their own routes.
#[tokio::test]
async fn t_p8_disabled_is_501() {
    let Some(db) = fresh_db("plural_t_p8_disabled").await else {
        return;
    };
    let s = serve_plural(&db.pool, "http://127.0.0.1:9", &PluralConfig { enabled: false, ..PluralConfig::default() }, true, ScopeSets::default()).await;
    for q in [format!("{GET_RECORDS}?actors={ALICE}"), format!("{LIST_MEMBERS}?actor={ALICE}")] {
        let (status, _, v) = get(&format!("{}/{q}", s.base)).await;
        assert_eq!((status, v["error"].as_str()), (501, Some("MethodNotImplemented")), "{q}");
    }
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM plural_repos").fetch_one(&db.pool).await.expect("count");
    assert_eq!(rows, 0, "nothing listed");
    db.drop_db().await;
}

/// T-P8: signed out, the tight per-IP class
/// (`plural_anon`, burst 10) answers 429 with `Retry-After` past its burst;
/// a token that does not verify is refused, not charged as anonymous.
#[tokio::test]
async fn t_p8_signed_out_calls_are_held_to_plural_anon() {
    let Some(db) = fresh_db("plural_t_p8_anon_limit").await else {
        return;
    };
    // The default burst of 10, refilled too slowly to matter here, so the
    // count does not depend on how fast the calls run.
    let cfg = crate::config::RateLimitsConfig {
        plural_anon: crate::config::RateClassConfig::new(0.001, 10),
        ..crate::config::RateLimitsConfig::default()
    };
    let disabled = PluralConfig { enabled: false, ..PluralConfig::default() };
    let s = serve_plural_limited(&db.pool, "http://127.0.0.1:9", &disabled, true, ScopeSets::default(), RateLimits::from_config(&cfg)).await;
    let url = format!("{}/{GET_RECORDS}?actors={ALICE}", s.base);
    let mut statuses = Vec::new();
    let mut retry_after = None;
    for _ in 0..12 {
        let r = reqwest::get(&url).await.expect("request");
        if r.status().as_u16() == 429 {
            retry_after = r.headers().get("retry-after").map(|v| v.to_str().expect("ascii").to_string());
        }
        statuses.push(r.status().as_u16());
    }
    assert_eq!(statuses.iter().filter(|s| **s == 501).count(), 10, "{statuses:?}");
    assert_eq!(crate::config::RateLimitsConfig::default().plural_anon, crate::config::RateClassConfig::new(1.0, 10));
    assert!(statuses.contains(&429), "{statuses:?}");
    assert!(retry_after.is_some_and(|v| v.parse::<u64>().is_ok_and(|n| n >= 1)), "Retry-After on the 429");
    let r = reqwest::Client::new().get(&url).header("authorization", "Bearer not-a-token").send().await.expect("request");
    assert_eq!(r.status().as_u16(), 401);
    db.drop_db().await;
}

/// T-P10 (§1.3, §2.9): the features.
#[test]
fn t_p10_features() {
    use crate::xrpc::handlers::membership::MembershipSettings;
    let on = MembershipSettings::default();
    let f = on.features();
    assert!(f.contains(&FEATURE_FRONTERS) && f.contains(&FEATURE_READ), "{f:?}");
    let off = MembershipSettings::default().with_plural_read(false);
    let f = off.features();
    assert!(f.contains(&FEATURE_FRONTERS) && !f.contains(&FEATURE_READ), "{f:?}");
}

/// T-F2 (§1.2): a public post carrying the fronters key keeps it on every
/// path Prism builds a record on: the writer's stored row, a post view, a
/// quote's `value`, a thread's post, a reply notification's `record`, a
/// Slingshot-built view (Thread Lens's ladder) and the upstream walk.
#[tokio::test]
async fn t_f2_public_posts_keep_the_fronters_key() {
    use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};
    use crate::hydration::Viewer;
    use crate::hydration::testing::actor;
    use crate::xrpc::pools::Requester;

    let Some(db) = fresh_db("plural_t_f2_public_fronters").await else {
        return;
    };
    let p = &db.pool;
    let fronters = json!({"members": [{"id": "3kwren"}]});
    let mut sets = ScopeSets::default();
    for did in [ALICE, BOB] {
        sets.members.insert(did.into());
        sets.scope.insert(did.into());
    }
    let w = Writer::new(p.clone(), ScopeState::new(sets.clone(), ScopePolicy::MembersAndOneHop, 10_000), Arc::new(crate::private_tier::DisabledPrivateTier));
    let options = ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: true, subscription_rules: false, source: WriteSource::Firehose };
    actor(p, ALICE, "alice.test", "Alice").await;
    actor(p, BOB, "bob.test", "Bob").await;
    let root = format!("at://{ALICE}/app.bsky.feed.post/3kroot");
    let reply = format!("at://{BOB}/app.bsky.feed.post/3kreply");
    let quote = format!("at://{BOB}/app.bsky.feed.post/3kquote");
    let op = |rkey: &str, record: Value| RawOp { action: Action::Create, collection: "app.bsky.feed.post".into(), rkey: rkey.into(), cid: Some(format!("bafy{rkey}")), record: Some(record) };
    let root_record = json!({"$type": "app.bsky.feed.post", "text": "we wrote this", "createdAt": "2026-10-24T00:00:00.000Z", FRONTERS_KEY: fronters});
    let r = w.apply(ALICE, "3kaaaaaaaaaa2", &[op("3kroot", root_record)], options).await.expect("root");
    assert_eq!(r.outcomes, [OpOutcome::Written]);
    let reply_record = json!({"$type": "app.bsky.feed.post", "text": "a reply", "createdAt": "2026-10-24T00:01:00.000Z", FRONTERS_KEY: fronters,
        "reply": {"root": {"uri": root, "cid": "bafy3kroot"}, "parent": {"uri": root, "cid": "bafy3kroot"}}});
    let quote_record = json!({"$type": "app.bsky.feed.post", "text": "a quote", "createdAt": "2026-10-24T00:02:00.000Z",
        "embed": {"$type": "app.bsky.embed.record", "record": {"uri": root, "cid": "bafy3kroot"}}});
    w.apply(BOB, "3kaaaaaaaaaa3", &[op("3kreply", reply_record), op("3kquote", quote_record)], options).await.expect("bob");
    // The stored row.
    let stored: Value = sqlx::query_scalar("SELECT record FROM posts WHERE uri = $1").bind(&root).fetch_one(p).await.expect("row");
    assert_eq!(stored[FRONTERS_KEY], fronters);
    let h = hydrator(p, sets);
    let req = Requester::test();
    // A post view and a quote's `value`.
    let v = h.get_posts(&Viewer::anonymous(), &req, &[root.clone(), quote.clone()]).await.expect("db").expect("view");
    let posts = v.inner()["posts"].as_array().expect("posts").clone();
    assert_eq!(posts[0]["record"][FRONTERS_KEY], fronters, "{}", posts[0]);
    assert_eq!(posts[1]["embed"]["record"]["value"][FRONTERS_KEY], fronters, "{}", posts[1]);
    // A thread.
    let t = h.get_post_thread(&Viewer::anonymous(), &req, &reply, 6, 80).await.expect("db").expect("thread");
    assert_eq!(t.inner()["thread"]["post"]["record"][FRONTERS_KEY], fronters, "{}", t.inner());
    assert_eq!(t.inner()["thread"]["parent"]["post"]["record"][FRONTERS_KEY], fronters, "{}", t.inner());
    // A reply notification to Alice, written by the writer itself.
    let me = Viewer { did: Some(ALICE.into()), hydrate: true };
    let n = h.list_notifications(&me, &req, 50, None, &[]).await.expect("db").expect("view");
    let reply_n = n.inner()["notifications"].as_array().expect("list").iter().find(|x| x["reason"] == "reply").cloned().expect("reply");
    assert_eq!(reply_n["record"][FRONTERS_KEY], fronters, "{reply_n}");
    // Slingshot-built posts (Thread Lens's ladder) and the upstream walk.
    let built = crate::hydration::upstream::post_from_record(&root, Some("bafy"), "alice.test", &stored);
    assert_eq!(built["record"][FRONTERS_KEY], fronters);
    let mut upstream = json!({"posts": [{"uri": root, "cid": "c", "author": {"did": ALICE, "handle": "alice.test"}, "record": stored.clone(), "labels": []}]});
    crate::hydration::normalize::normalize_upstream(&mut upstream, &crate::hydration::normalize::Overlay::default());
    assert_eq!(upstream["posts"][0]["record"][FRONTERS_KEY], fronters);
    db.drop_db().await;
}
