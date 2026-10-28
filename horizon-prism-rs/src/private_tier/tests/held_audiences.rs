//! Audiences held by this AppView (private-audiences design §13): the four
//! methods over HTTP through the private router, and their writes below the
//! gate. T-A1 to T-A19 (T-A10, T-A11 and T-A13 also live beside the suites
//! they extend).

use super::*;
use crate::aggregations::purge::Purger;
use crate::aggregations::scope_state::PurgeKind;
use crate::private_tier::audiences::{self, Record, Written};
use crate::test_support::hooks;

const CREATE: &str = "app.nearhorizon.private.createAudience";
const UPDATE: &str = "app.nearhorizon.private.updateAudience";
const DELETE: &str = "app.nearhorizon.private.deleteAudience";
const LIST: &str = "app.nearhorizon.private.listAudiences";

/// The served private router with roomy limits and no floor.
async fn served(f: &Fixture) -> String {
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    serve(f.state_of(app, Some(Duration::from_secs(1)), Duration::ZERO)).await
}

type Answer = (u16, Value, Option<String>);

async fn answer(r: reqwest::Response) -> Answer {
    let status = r.status().as_u16();
    let cache = r.headers().get("cache-control").and_then(|v| v.to_str().ok()).map(str::to_string);
    let text = r.text().await.expect("body");
    (status, serde_json::from_str(&text).unwrap_or(Value::Null), cache)
}

/// A POST of `method` by `who` with a raw body.
async fn post_raw(base: &str, method: &str, who: &str, body: String) -> Answer {
    let token = mint(&signing_key(key_of(who)), who, &format!("did:web:{AUDIENCE_HOST}"), Some(method));
    let r = reqwest::Client::new()
        .post(format!("{base}/xrpc/{method}"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("request");
    answer(r).await
}

async fn post_json(base: &str, method: &str, who: &str, body: &Value) -> Answer {
    post_raw(base, method, who, body.to_string()).await
}

/// `listAudiences` by `who` with `query`.
async fn list(base: &str, who: &str, query: &str) -> Answer {
    let token = mint(&signing_key(key_of(who)), who, &format!("did:web:{AUDIENCE_HOST}"), Some(LIST));
    let r = reqwest::Client::new()
        .get(format!("{base}/xrpc/{LIST}?{query}"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("request");
    answer(r).await
}

fn record(mode: Option<&str>, name: Option<&str>, members: &[&str]) -> Value {
    let mut r = json!({"$type": NSID_AUDIENCE, "members": members, "createdAt": "2026-10-01T00:00:00.000Z", "updatedAt": "2026-10-02T00:00:00.000Z"});
    if let Some(m) = mode {
        r["mode"] = json!(m);
    }
    if let Some(n) = name {
        r["name"] = json!(n);
    }
    r
}

async fn create(base: &str, who: &str, rec: &Value) -> (String, String) {
    let (status, body, cache) = post_json(base, CREATE, who, &json!({"record": rec})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(cache.as_deref(), Some("private, no-store"));
    (body["uri"].as_str().expect("uri").to_string(), body["cid"].as_str().expect("cid").to_string())
}

/// A refusal: 400 with `error` and an `"Error: "` message.
fn refused(r: &Answer, error: &str) {
    assert_eq!((r.0, r.1["error"].as_str()), (400, Some(error)), "{}", r.1);
    if error != "NotEnrolled" && error != "NotReady" {
        assert!(r.1["message"].as_str().is_some_and(|m| m.starts_with("Error: ")), "{}", r.1);
    }
}

/// The stored row: `(mode, name, members, cid, rev, display_filled,
/// created_at, updated_at)`.
type Row = (Option<String>, Option<String>, Vec<String>, String, String, bool, DateTime<Utc>, DateTime<Utc>);

async fn row(pool: &PgPool, uri: &str) -> Option<Row> {
    sqlx::query_as("SELECT mode, name, members, cid, rev, display_filled, created_at, updated_at FROM private_audiences WHERE uri = $1")
        .bind(uri)
        .fetch_optional(pool)
        .await
        .expect("row")
}

async fn count(pool: &PgPool, owner: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM private_audiences WHERE owner_did = $1").bind(owner).fetch_one(pool).await.expect("count")
}

async fn queued(pool: &PgPool, did: &str, depth: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM backfill_jobs WHERE did = $1 AND depth = $2 AND state = 'queued')")
        .bind(did)
        .bind(depth)
        .fetch_one(pool)
        .await
        .expect("jobs")
}

async fn db_name(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT current_database()::text").fetch_one(pool).await.expect("db")
}

fn checked(rec: &Value, caller: &str) -> Record {
    audiences::check_record(rec, caller).expect("valid")
}

/// T-A1: create answers `{uri, cid}` with a TID rkey; the row holds the
/// mode, the trimmed name, the members de-duplicated without the owner,
/// the input's times, `cid = rev` and `display_filled`; a post naming it is
/// served to a listed member at once, with no `requestRecord`.
#[tokio::test]
async fn t_a1_create() {
    let test = "t_a1_audience_create";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    // Alice already an owner, ready.
    let first = f.audience(ALICE, "3kaudfirst22", Some("list"), &[]).await;
    f.private_post(ALICE, "3kpostfirst2", &first, None).await;
    f.ready_all().await;
    let base = served(&f).await;
    let (uri, cid) = create(&base, ALICE, &record(Some("list"), Some("  Close friends \u{3000}"), &[BOB, ALICE, BOB])).await;
    let rkey = uri.rsplit('/').next().expect("rkey");
    assert!(uri.starts_with(&format!("at://{ALICE}/{NSID_AUDIENCE}/")), "{uri}");
    assert!(proto_blue::syntax::Tid::is_valid(rkey), "{rkey}");
    let (mode, name, members, stored_cid, rev, filled, created, updated) = row(&f.pool, &uri).await.expect("row");
    assert_eq!((mode.as_deref(), name.as_deref(), members, filled), (Some("list"), Some("Close friends"), vec![BOB.to_string()], true));
    assert_eq!((stored_cid.as_str(), rev.as_str()), (cid.as_str(), cid.as_str()));
    assert_eq!(created.to_rfc3339_opts(SecondsFormat::Secs, true), "2026-10-01T00:00:00Z");
    assert_eq!(updated.to_rfc3339_opts(SecondsFormat::Secs, true), "2026-10-02T00:00:00Z");
    let post = f.private_post(ALICE, "3kpostnew222", &uri, None).await;
    assert_eq!(readable(&base, BOB, &[&post]).await, std::slice::from_ref(&post));
    assert!(readable(&base, CAROL, &[&post]).await.is_empty());
    db.drop_db().await;
}

/// T-A2: each check refuses with its own message.
#[tokio::test]
async fn t_a2_the_checks() {
    let test = "t_a2_audience_checks";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    f.ready_all().await;
    let base = served(&f).await;
    let with = |key: &str, value: Value| {
        let mut r = record(Some("list"), None, &[]);
        r[key] = value;
        json!({"record": r})
    };
    let many: Vec<String> = (0..1001).map(|i| format!("did:plc:m{i:023}")).collect();
    let distinct: std::collections::HashSet<&String> = many.iter().collect();
    assert_eq!(distinct.len(), 1001);
    let mut thousand: Vec<Value> = many[..1000].iter().map(|d| json!(d)).collect();
    thousand.push(json!(ALICE));
    let mut no_updated = record(None, None, &[]);
    no_updated.as_object_mut().expect("object").remove("updatedAt");
    let cases = [
        (with("mode", json!("lists")), "record.mode"),
        (with("mode", json!("List")), "record.mode"),
        (with("mode", json!(1)), "record.mode"),
        (with("name", json!("x".repeat(65))), "graphemes"),
        (with("name", json!("\u{1F468}\u{200D}\u{1F469}".repeat(64))), "bytes"),
        (with("name", json!(" \t\u{2003} ")), "empty"),
        (with("members", json!(["bob.test"])), "record.members"),
        (with("members", json!([format!("did:web:{}", "a".repeat(250))])), "256 bytes"),
        (with("members", json!(many)), "1000"),
        (json!({"record": no_updated}), "record.updatedAt"),
        (with("$type", json!("app.bsky.graph.list")), "record.$type"),
        (json!({"record": record(None, None, &[]), "rkey": "3kchosen2222"}), "rkey is not accepted"),
    ];
    for (body, said) in cases {
        let r = post_json(&base, CREATE, ALICE, &body).await;
        refused(&r, "InvalidRequest");
        assert!(r.1["message"].as_str().is_some_and(|m| m.contains(said)), "{said}: {}", r.1);
    }
    // 1000 distinct members and the owner pass.
    let (uri, _) = create(&base, ALICE, &with("members", json!(thousand))["record"]).await;
    assert_eq!(row(&f.pool, &uri).await.expect("row").2.len(), 1000);
    // A body over 512 KiB is refused unparsed.
    let big = format!("{{\"record\": {{}}, \"pad\": \"{}\"}}", "x".repeat(audiences::BODY_MAX));
    let r = post_raw(&base, CREATE, ALICE, big).await;
    refused(&r, "InvalidRequest");
    assert!(r.1["message"].as_str().is_some_and(|m| m.contains("too large")), "{}", r.1);
    assert_eq!(count(&f.pool, ALICE).await, 1);
    db.drop_db().await;
}

/// T-A3: the 200th create passes and the 201st is `LimitReached`; fifty
/// concurrent creates from 180 rows leave exactly 200, called below the
/// gate so the rate limit cannot cap them.
#[tokio::test]
async fn t_a3_the_cap() {
    let test = "t_a3_audience_cap";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let scope = f.writer.scope().clone();
    let rec = checked(&record(Some("list"), None, &[BOB]), ALICE);
    for _ in 0..199 {
        assert!(matches!(audiences::create(&f.pool, &scope, ALICE, &rec).await.expect("create"), Written::Done { .. }));
    }
    let base = served(&f).await;
    f.ready_all().await;
    create(&base, ALICE, &record(None, None, &[])).await;
    let r = post_json(&base, CREATE, ALICE, &json!({"record": record(None, None, &[])})).await;
    refused(&r, "LimitReached");
    assert_eq!(r.1["message"].as_str(), Some("Error: audience limit reached (200)"));
    assert_eq!(count(&f.pool, ALICE).await, 200);

    for _ in 0..180 {
        audiences::create(&f.pool, &scope, BOB, &rec).await.expect("create");
    }
    let tasks: Vec<_> = (0..50)
        .map(|_| {
            let (pool, scope, rec) = (f.pool.clone(), scope.clone(), rec.clone());
            tokio::spawn(async move { audiences::create(&pool, &scope, BOB, &rec).await.expect("create") })
        })
        .collect();
    let mut done = 0;
    for t in tasks {
        if matches!(t.await.expect("join"), Written::Done { .. }) {
            done += 1;
        }
    }
    assert_eq!((done, count(&f.pool, BOB).await), (20, 200));
    db.drop_db().await;
}

/// T-A4: a matching swap writes and returns a new token; the old token is
/// then `InvalidSwap`; a member taken off stops reading at the next
/// request and one added starts; a missing row is `InvalidSwap`; another
/// DID's URI is `InvalidRequest` before any transaction.
#[tokio::test]
async fn t_a4_update() {
    let test = "t_a4_audience_update";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let first = f.audience(ALICE, "3kaudfirst22", Some("list"), &[]).await;
    f.private_post(ALICE, "3kpostfirst2", &first, None).await;
    f.ready_all().await;
    let base = served(&f).await;
    let (uri, cid) = create(&base, ALICE, &record(Some("list"), Some("A"), &[BOB])).await;
    let post = f.private_post(ALICE, "3kpostupd222", &uri, None).await;
    assert_eq!(readable(&base, BOB, &[&post]).await, std::slice::from_ref(&post));
    assert!(readable(&base, CAROL, &[&post]).await.is_empty());
    let (status, body, cache) =
        post_json(&base, UPDATE, ALICE, &json!({"uri": uri, "swapCid": cid, "record": record(Some("list"), Some("B"), &[CAROL])})).await;
    assert_eq!((status, cache.as_deref()), (200, Some("private, no-store")), "{body}");
    let new = body["cid"].as_str().expect("cid").to_string();
    assert_ne!(new, cid);
    assert_eq!(body["uri"].as_str(), Some(uri.as_str()));
    let (_, name, members, stored, rev, _, created, updated) = row(&f.pool, &uri).await.expect("row");
    assert_eq!((name.as_deref(), members, stored.as_str(), rev.as_str()), (Some("B"), vec![CAROL.to_string()], new.as_str(), new.as_str()));
    assert_eq!(created.to_rfc3339_opts(SecondsFormat::Secs, true), "2026-10-01T00:00:00Z", "createdAt kept");
    assert_eq!(updated.to_rfc3339_opts(SecondsFormat::Secs, true), "2026-10-02T00:00:00Z");
    assert!(readable(&base, BOB, &[&post]).await.is_empty(), "taken off");
    assert_eq!(readable(&base, CAROL, &[&post]).await, std::slice::from_ref(&post), "added");
    // The old token.
    let stale = post_json(&base, UPDATE, ALICE, &json!({"uri": uri, "swapCid": cid, "record": record(None, None, &[BOB])})).await;
    refused(&stale, "InvalidSwap");
    assert_eq!(row(&f.pool, &uri).await.expect("row").3, new);
    // A missing row.
    let gone = format!("at://{ALICE}/{NSID_AUDIENCE}/3knothere222");
    refused(&post_json(&base, UPDATE, ALICE, &json!({"uri": gone, "swapCid": new, "record": record(None, None, &[])})).await, "InvalidSwap");
    // Another DID's URI, refused by parsing: the write is never reached.
    let name = db_name(&f.pool).await;
    let reached = hooks::arm(hooks::AUDIENCE_UPDATE_REACHED, &format!("{name}/{ALICE}"));
    for other in [format!("at://{BOB}/{NSID_AUDIENCE}/3kbobs222222"), format!("at://{ALICE}/app.bsky.graph.list/3kx222222222"), "nope".into()] {
        let r = post_json(&base, UPDATE, ALICE, &json!({"uri": other, "swapCid": new, "record": record(None, None, &[])})).await;
        refused(&r, "InvalidRequest");
    }
    refused(&post_json(&base, UPDATE, ALICE, &json!({"uri": uri, "record": record(None, None, &[])})).await, "InvalidRequest");
    assert_eq!(reached.hits(), 0, "no transaction for a refused input");
    drop(reached);
    db.drop_db().await;
}

/// T-A5: a row stored as ingested before 0052 (its repo CID as `cid`)
/// lists with `createdAt = updatedAt = indexed_at`, swaps against its repo
/// CID, and its posts read as before.
#[tokio::test]
async fn t_a5_existing_rows() {
    let test = "t_a5_audience_existing";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let aud = f.audience(ALICE, "3kaudold2222", Some("list"), &[BOB]).await;
    let post = f.private_post(ALICE, "3kpostold222", &aud, None).await;
    f.ready_all().await;
    exec(&f.pool, &format!("UPDATE private_audiences SET indexed_at = '2026-09-01T00:00:00Z', created_at = now(), updated_at = now() WHERE uri = '{aud}'")).await;
    sqlx::raw_sql(include_str!("../../../migrations/0052_audiences_held.sql")).execute(&f.pool).await.expect("0052");
    let base = served(&f).await;
    assert_eq!(readable(&base, BOB, &[&post]).await, std::slice::from_ref(&post));
    let (status, body, _) = list(&base, ALICE, "").await;
    assert_eq!(status, 200, "{body}");
    let item = &body["audiences"][0];
    assert_eq!(item["uri"].as_str(), Some(aud.as_str()));
    assert_eq!(item["cid"].as_str(), Some("bafy3kaudold2222"), "the repo CID is the first token");
    assert_eq!(item["record"]["createdAt"].as_str(), Some("2026-09-01T00:00:00.000Z"));
    assert_eq!(item["record"]["updatedAt"].as_str(), Some("2026-09-01T00:00:00.000Z"));
    let back = json!({"uri": aud, "swapCid": "bafy3kaudold2222", "record": item["record"]});
    let (status, body, _) = post_json(&base, UPDATE, ALICE, &back).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(readable(&base, BOB, &[&post]).await, std::slice::from_ref(&post));
    db.drop_db().await;
}

/// T-A6: a delete makes the post author-only; a second answers `{}`; the
/// last row of a member with no posts leaves the owner set, in the database
/// and in memory.
#[tokio::test]
async fn t_a6_delete() {
    let test = "t_a6_audience_delete";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let first = f.audience(ALICE, "3kaudfirst22", Some("list"), &[]).await;
    f.private_post(ALICE, "3kpostfirst2", &first, None).await;
    f.ready_all().await;
    let base = served(&f).await;
    let (uri, _) = create(&base, ALICE, &record(Some("list"), None, &[BOB])).await;
    let post = f.private_post(ALICE, "3kpostdel222", &uri, None).await;
    assert_eq!(readable(&base, BOB, &[&post]).await, std::slice::from_ref(&post));
    for _ in 0..2 {
        let (status, body, cache) = post_json(&base, DELETE, ALICE, &json!({"uri": uri})).await;
        assert_eq!((status, body, cache.as_deref()), (200, json!({}), Some("private, no-store")));
    }
    assert!(readable(&base, BOB, &[&post]).await.is_empty());
    assert_eq!(readable(&base, ALICE, &[&post]).await, std::slice::from_ref(&post), "its author still reads it");

    // Carol: one audience, no posts. Its delete leaves the owner set.
    let scope = f.app.hydrator.scope();
    let (only, _) = create(&base, CAROL, &record(None, None, &[])).await;
    assert!(scope.read_guard().await.is_owner(CAROL));
    let mut conn = f.pool.acquire().await.expect("conn");
    assert!(owners::is_owner(&mut conn, CAROL).await.expect("owner"));
    post_json(&base, DELETE, CAROL, &json!({"uri": only})).await;
    assert!(!owners::is_owner(&mut conn, CAROL).await.expect("owner"));
    assert!(!scope.read_guard().await.is_owner(CAROL));
    drop(conn);
    db.drop_db().await;
}

/// T-A7: 120 rows page 50, 50 and 20 newest first, with no cursor on the
/// last page; another member's rows never appear.
#[tokio::test]
async fn t_a7_list() {
    let test = "t_a7_audience_list";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let scope = f.writer.scope().clone();
    let rec = checked(&record(Some("followers"), Some("x"), &[BOB]), ALICE);
    for _ in 0..120 {
        audiences::create(&f.pool, &scope, ALICE, &rec).await.expect("create");
    }
    audiences::create(&f.pool, &scope, BOB, &checked(&record(None, None, &[]), BOB)).await.expect("create");
    let base = served(&f).await;
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for want in [50, 50, 20] {
        let q = cursor.as_ref().map(|c| format!("cursor={c}")).unwrap_or_default();
        let (status, body, cache) = list(&base, ALICE, &q).await;
        assert_eq!((status, cache.as_deref()), (200, Some("private, no-store")), "{body}");
        let items = body["audiences"].as_array().expect("audiences");
        assert_eq!(items.len(), want);
        for item in items {
            let uri = item["uri"].as_str().expect("uri");
            assert!(uri.starts_with(&format!("at://{ALICE}/")), "{uri}");
            assert_eq!(item["record"]["mode"].as_str(), Some("followers"));
            assert_eq!(item["record"]["members"], json!([BOB]));
            seen.push(uri.rsplit('/').next().expect("rkey").to_string());
        }
        cursor = body["cursor"].as_str().map(str::to_string);
    }
    assert_eq!(cursor, None, "no cursor on the last page");
    let mut sorted = seen.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    sorted.dedup();
    assert_eq!(seen, sorted, "newest first, each once");
    refused(&list(&base, ALICE, "limit=101").await, "InvalidRequest");
    db.drop_db().await;
}

/// T-A8: create and update answer 401 without auth, `NotReady`, `503
/// Reindexing` with `retryAfter`, and `NotEnrolled`; a removal committed
/// between the gate and the write gets `NotEnrolled` and writes nothing.
#[tokio::test]
async fn t_a8_the_gate() {
    let test = "t_a8_audience_gate";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    f.ready_all().await;
    let base = served(&f).await;
    let (uri, cid) = create(&base, CAROL, &record(None, None, &[])).await;
    let update = json!({"uri": uri, "swapCid": cid, "record": record(None, None, &[])});
    let create_body = json!({"record": record(None, None, &[])});
    for method in [CREATE, UPDATE, DELETE, LIST] {
        let verb = if method == LIST { reqwest::Method::GET } else { reqwest::Method::POST };
        let r = reqwest::Client::new().request(verb, format!("{base}/xrpc/{method}")).send().await.expect("request");
        assert_eq!(r.status().as_u16(), 401, "{method}");
    }
    exec(&f.pool, &format!("UPDATE members SET ready_at = NULL, first_ready_at = NULL WHERE did = '{CAROL}'")).await;
    refused(&post_json(&base, CREATE, CAROL, &create_body).await, "NotReady");
    refused(&post_json(&base, UPDATE, CAROL, &update).await, "NotReady");
    exec(&f.pool, &format!("UPDATE members SET ready_at = NULL, first_ready_at = now() WHERE did = '{CAROL}'")).await;
    for (method, body) in [(CREATE, &create_body), (UPDATE, &update)] {
        let (status, body, _) = post_json(&base, method, CAROL, body).await;
        assert_eq!((status, body["error"].as_str()), (503, Some("Reindexing")), "{method}: {body}");
        assert!(body["retryAfter"].as_u64().is_some(), "{body}");
    }
    exec(&f.pool, &format!("DELETE FROM members WHERE did = '{DAVE}'")).await;
    refused(&post_json(&base, CREATE, DAVE, &create_body).await, "NotEnrolled");
    refused(&post_json(&base, UPDATE, DAVE, &update).await, "NotEnrolled");
    assert_eq!(count(&f.pool, DAVE).await, 0);

    // A removal between the gate and the write.
    let purger = Purger::new(f.pool.clone(), f.writer.scope().clone(), Arc::clone(f.writer.private()));
    let name = db_name(&f.pool).await;
    let armed = hooks::arm(hooks::AUDIENCE_BEFORE_LOCK, &format!("{name}/{BOB}"));
    let pending = tokio::spawn({
        let (base, body) = (base.clone(), create_body.clone());
        async move { post_json(&base, CREATE, BOB, &body).await }
    });
    armed.reached().await;
    purger.purge(BOB, PurgeKind::MemberRemoved).await.expect("purge");
    armed.release();
    refused(&pending.await.expect("create"), "NotEnrolled");
    assert_eq!(count(&f.pool, BOB).await, 0);
    drop(armed);
    // The same for an update: Alice, ready again, past the gate.
    let (uri, cid) = create(&base, ALICE, &record(None, Some("kept"), &[])).await;
    let armed = hooks::arm(hooks::AUDIENCE_BEFORE_LOCK, &format!("{name}/{ALICE}"));
    let pending = tokio::spawn({
        let base = base.clone();
        let body = json!({"uri": uri, "swapCid": cid, "record": record(None, Some("changed"), &[])});
        async move { post_json(&base, UPDATE, ALICE, &body).await }
    });
    armed.reached().await;
    purger.purge(ALICE, PurgeKind::MemberRemoved).await.expect("purge");
    armed.release();
    refused(&pending.await.expect("update"), "NotEnrolled");
    drop(armed);
    db.drop_db().await;
}

/// T-A9: a member's first audience makes it an owner and queues `private`;
/// a `following` create by an existing owner queues `authz`, and so does an
/// update to `following`.
#[tokio::test]
async fn t_a9_owner_status() {
    let test = "t_a9_audience_owner";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let scope = f.writer.scope().clone();
    exec(&f.pool, "DELETE FROM backfill_jobs").await;
    let mut conn = f.pool.acquire().await.expect("conn");
    assert!(!owners::is_owner(&mut conn, ALICE).await.expect("owner"));
    audiences::create(&f.pool, &scope, ALICE, &checked(&record(Some("list"), None, &[]), ALICE)).await.expect("create");
    assert!(owners::is_owner(&mut conn, ALICE).await.expect("owner"));
    assert!(scope.read_guard().await.is_owner(ALICE));
    assert!(queued(&f.pool, ALICE, "private").await);
    assert!(!queued(&f.pool, ALICE, "authz").await);
    exec(&f.pool, "DELETE FROM backfill_jobs").await;
    audiences::create(&f.pool, &scope, ALICE, &checked(&record(Some("following"), None, &[]), ALICE)).await.expect("create");
    assert!(queued(&f.pool, ALICE, "authz").await);
    assert!(!queued(&f.pool, ALICE, "private").await);
    exec(&f.pool, "DELETE FROM backfill_jobs").await;
    let Written::Done { uri, cid } = audiences::create(&f.pool, &scope, ALICE, &checked(&record(Some("list"), None, &[]), ALICE)).await.expect("create") else {
        panic!("created");
    };
    assert!(!queued(&f.pool, ALICE, "authz").await, "a list create");
    audiences::update(&f.pool, &scope, ALICE, &uri, &checked(&record(Some("following"), None, &[]), ALICE), &cid).await.expect("update");
    assert!(queued(&f.pool, ALICE, "authz").await);
    drop(conn);
    db.drop_db().await;
}

/// T-A12: `member_removed` deletes the member's audiences in batches (more
/// than one), `account_deleted` deletes them, and a rejoin lists none.
#[tokio::test]
async fn t_a12_purges() {
    let test = "t_a12_audience_purges";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let scope = f.writer.scope().clone();
    exec(
        &f.pool,
        &format!(
            "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, members) \
             SELECT 'at://{BOB}/{NSID_AUDIENCE}/3kb' || lpad(g::text, 10, '0'), '{BOB}', '3kb' || lpad(g::text, 10, '0'), 'c', 'c', '{{}}' \
             FROM generate_series(1, {}) g",
            crate::aggregations::purge::BATCH + 5
        ),
    )
    .await;
    audiences::create(&f.pool, &scope, CAROL, &checked(&record(None, None, &[]), CAROL)).await.expect("create");
    audiences::create(&f.pool, &scope, ALICE, &checked(&record(None, None, &[]), ALICE)).await.expect("create");
    let purger = Purger::new(f.pool.clone(), scope.clone(), Arc::clone(f.writer.private()));
    purger.purge(BOB, PurgeKind::MemberRemoved).await.expect("member removed");
    purger.purge(CAROL, PurgeKind::AccountDeleted).await.expect("account deleted");
    assert_eq!((count(&f.pool, BOB).await, count(&f.pool, CAROL).await, count(&f.pool, ALICE).await), (0, 0, 1));
    exec(&f.pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{BOB}', 'admin', now())")).await;
    let base = served(&f).await;
    let (status, body, _) = list(&base, BOB, "").await;
    assert_eq!((status, body), (200, json!({"audiences": []})));
    db.drop_db().await;
}

/// T-A14: a listed member's view carries `audience {uri, mode, name}` and
/// no members or count; `getRecord` of the audience URI is not found;
/// another member's `listAudiences` omits it.
#[tokio::test]
async fn t_a14_nothing_leaks() {
    let test = "t_a14_audience_leak";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let first = f.audience(ALICE, "3kaudfirst22", Some("list"), &[]).await;
    f.private_post(ALICE, "3kpostfirst2", &first, None).await;
    f.ready_all().await;
    let base = served(&f).await;
    let (uri, _) = create(&base, ALICE, &record(Some("list"), Some("Inner"), &[BOB, CAROL])).await;
    let post = f.private_post(ALICE, "3kpostleak22", &uri, None).await;
    let (_, posts) = posts_of(&base, BOB, &[&post]).await;
    let audience = &posts[0]["audience"];
    assert_eq!(audience, &json!({"uri": uri, "mode": "list", "name": "Inner"}), "{audience}");
    let text = posts[0].to_string();
    assert!(!text.contains(CAROL), "no member is named: {text}");
    let (_, body, _) = list(&base, BOB, "").await;
    assert!(!body.to_string().contains(&uri), "{body}");
    let rkey = uri.rsplit('/').next().expect("rkey");
    let got = f.app.hydrator.repo_get_record(ALICE, NSID_AUDIENCE, rkey, None).await.expect("db");
    assert!(got.is_err(), "RecordNotFound");
    db.drop_db().await;
}

/// T-A15: the four NSIDs are kept, private-posting methods, routed and
/// charged as `private`; `FEATURES` ends with `private.audiences`.
#[test]
fn t_a15_the_lists() {
    use crate::honest_501::{KEPT_METHODS, PRIVATE_POSTING_METHODS};
    use crate::xrpc::rate_limit::{Family, SERVED};
    for nsid in [CREATE, UPDATE, DELETE, LIST] {
        assert!(KEPT_METHODS.contains(&nsid), "{nsid}");
        assert!(PRIVATE_POSTING_METHODS.iter().any(|(n, _)| *n == nsid), "{nsid}");
        let s = SERVED.iter().find(|s| s.nsid == nsid).expect("served");
        assert_eq!((s.family, s.anonymous), (Family::Private, false), "{nsid}");
    }
    assert_eq!(crate::private_tier::FEATURES.last(), Some(&"private.audiences"));
}

/// T-A18: a member whose member readiness is unset can list and delete its
/// audiences, and cannot create or update one.
#[tokio::test]
async fn t_a18_list_and_delete_need_no_readiness() {
    let test = "t_a18_audience_gate_own";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    f.ready_all().await;
    let base = served(&f).await;
    let (uri, cid) = create(&base, BOB, &record(None, None, &[])).await;
    exec(&f.pool, &format!("UPDATE members SET ready_at = NULL WHERE did = '{BOB}'")).await;
    let (status, body, _) = list(&base, BOB, "").await;
    assert_eq!((status, body["audiences"].as_array().map(Vec::len)), (200, Some(1)), "{body}");
    refused(&post_json(&base, CREATE, BOB, &json!({"record": record(None, None, &[])})).await, "NotReady");
    refused(&post_json(&base, UPDATE, BOB, &json!({"uri": uri, "swapCid": cid, "record": record(None, None, &[])})).await, "NotReady");
    let (status, body, _) = post_json(&base, DELETE, BOB, &json!({"uri": uri})).await;
    assert_eq!((status, body), (200, json!({})));
    assert_eq!(count(&f.pool, BOB).await, 0);
    db.drop_db().await;
}

/// T-A19: a 257-byte member DID is `InvalidRequest`; a page whose rows sum
/// past 20 000 members ends early with a cursor, and the next continues
/// without loss.
#[tokio::test]
async fn t_a19_sizes() {
    let test = "t_a19_audience_sizes";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    f.ready_all().await;
    let base = served(&f).await;
    let long = format!("did:web:{}", "a".repeat(257 - 8));
    assert_eq!(long.len(), 257);
    refused(&post_json(&base, CREATE, ALICE, &json!({"record": record(None, None, &[&long])})).await, "InvalidRequest");
    let ok = format!("did:web:{}", "a".repeat(256 - 8));
    create(&base, ALICE, &record(None, None, &[&ok])).await;
    exec(&f.pool, &format!("DELETE FROM private_audiences WHERE owner_did = '{ALICE}'")).await;
    // 30 audiences of 1000 members: the 20th reaches 20 000, so the page
    // ends there, with a cursor.
    let scope = f.writer.scope().clone();
    let members: Vec<String> = (0..1000).map(|i| format!("did:plc:m{i:023}")).collect();
    let rec = Record { members: members.clone(), ..checked(&record(None, None, &[]), ALICE) };
    for _ in 0..30 {
        audiences::create(&f.pool, &scope, ALICE, &rec).await.expect("create");
    }
    let (status, body, _) = list(&base, ALICE, "limit=100").await;
    assert_eq!(status, 200);
    let first = body["audiences"].as_array().expect("audiences");
    assert_eq!(first.len(), 20);
    let cursor = body["cursor"].as_str().expect("a cursor");
    let (_, body, _) = list(&base, ALICE, &format!("limit=100&cursor={cursor}")).await;
    let second = body["audiences"].as_array().expect("audiences");
    assert_eq!(second.len(), 10);
    assert!(body.get("cursor").is_none(), "{}", body["cursor"]);
    let mut uris: Vec<&str> = first.iter().chain(second).map(|a| a["uri"].as_str().expect("uri")).collect();
    uris.sort_unstable();
    uris.dedup();
    assert_eq!(uris.len(), 30);
    db.drop_db().await;
}

const BLOCK: &str = "app.bsky.graph.block";

fn block_record(subject: &str) -> Value {
    json!({"$type": BLOCK, "subject": subject, "createdAt": "2026-10-01T00:00:00Z"})
}

/// T-A13 (Q-6): under a held `list` audience, a reader blocked either way
/// with its owner, by a direct block or the owner's moderation-list block,
/// is refused, and served again once unblocked; so is a reply by a blocked
/// writer. A private block (`tools.kryphocron.graph.block`) alone keeps
/// nobody off: the 2026-10-24 decision, recorded so a change is deliberate.
#[tokio::test]
async fn t_a13_blocks_keep_readers_off() {
    let Some(mut w) = Walk::new("ta13-blocks", Layout::SamePds, Some("list"), &[BOB, CAROL]).await else {
        return;
    };
    let root = w.root.clone();
    let reply = w.reply(CAROL, "3kta13repl2", &root).await;
    w.f.ready_all().await;
    w.expect("listed, unblocked", BOB, &root, true).await;
    w.expect("the reply, unblocked", BOB, &reply, true).await;
    for (i, (case, from, to, uri)) in [
        ("O blocks V", ALICE, BOB, &root),
        ("V blocks O", BOB, ALICE, &root),
        ("O blocks the writer", ALICE, CAROL, &reply),
    ]
    .into_iter()
    .enumerate()
    {
        let rkey = format!("3kta13blk{i}2");
        w.f.ingest(from, BLOCK, &rkey, block_record(to)).await;
        w.expect(case, BOB, uri, false).await;
        w.f.apply_op(from, Action::Delete, BLOCK, &rkey, REV2, None).await;
        w.expect(&format!("{case}, unblocked"), BOB, uri, true).await;
    }
    exec(&w.f.pool, &list_block_sql(ALICE, DAVE, "3kta13list2", &[BOB])).await;
    w.expect("O list-blocks V", BOB, &root, false).await;
    exec(&w.f.pool, "DELETE FROM list_references").await;
    w.expect("O's list block removed", BOB, &root, true).await;
    w.f.apply_op(ALICE, Action::Create, "tools.kryphocron.graph.block", "3kta13pblk2", REV, Some(json!({"subject": BOB}))).await;
    w.expect("only a private block", BOB, &root, true).await;
    w.finish().await;
}

/// Whether a backend of this database waits on an advisory lock.
async fn waits_on_advisory(pool: &PgPool) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND NOT l.granted AND a.datname = current_database())",
    )
    .fetch_one(pool)
    .await
    .expect("pg_locks")
}

/// Until a backend waits on an advisory lock or `task` is done.
async fn until_waiting<T>(pool: &PgPool, task: &tokio::task::JoinHandle<T>) {
    for _ in 0..500 {
        if task.is_finished() || waits_on_advisory(pool).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("neither waiting nor done");
}

/// T-A16's setup: Carol with one held audience and no post, owner-ready,
/// no job; her first private post's op, to apply through the writer.
async fn owner_race(f: &Fixture) -> (String, RawOp) {
    let scope = f.writer.scope().clone();
    let Written::Done { uri, .. } = audiences::create(&f.pool, &scope, CAROL, &checked(&record(Some("list"), None, &[BOB]), CAROL)).await.expect("create")
    else {
        panic!("created");
    };
    f.ready(CAROL).await;
    exec(&f.pool, "DELETE FROM backfill_jobs").await;
    let rkey = "3kta16post22";
    let record = f.post_record(CAROL, rkey, &uri, None).await;
    let op = RawOp { action: Action::Create, collection: NSID_POST_PRIVATE.into(), rkey: rkey.into(), cid: Some(format!("bafy{rkey}")), record: Some(record) };
    (uri, op)
}

fn spawn_post(f: &Fixture, op: RawOp) -> tokio::task::JoinHandle<()> {
    let writer = f.writer.clone();
    tokio::spawn(async move {
        let options = ApplyOptions {
            advance_applied_rev: false,
            dead_letter_readiness: false,
            attempts: 1,
            live: false,
            subscription_rules: false,
            source: WriteSource::Firehose,
        };
        let report = writer.apply(CAROL, REV2, &[op], options).await.expect("apply");
        assert_eq!(report.outcomes, [OpOutcome::Written], "{report:?}");
    })
}

fn spawn_delete(f: &Fixture, uri: &str) -> tokio::task::JoinHandle<bool> {
    let (pool, scope, uri) = (f.pool.clone(), f.writer.scope().clone(), uri.to_string());
    tokio::spawn(async move { audiences::delete(&pool, &scope, CAROL, &uri).await.expect("delete") })
}

/// `(owner in the database, in memory, owner-ready, a private job queued)`.
async fn owner_state(f: &Fixture) -> (bool, bool, bool, bool) {
    let mut conn = f.pool.acquire().await.expect("conn");
    let db = owners::is_owner(&mut conn, CAROL).await.expect("owner");
    let memory = f.writer.scope().read_guard().await.is_owner(CAROL);
    let ready = readiness::owner_ready(&f.pool, CAROL).await.expect("ready");
    (db, memory, ready, queued(&f.pool, CAROL, "private").await)
}

/// T-A16: `deleteAudience` of a member's last audience races the ingest of
/// that member's first private post. Delete first: the post waits on the
/// owner lock, then re-evaluates: an owner, readiness unset, its `private`
/// job queued. Post first: the delete waits: an owner, readiness kept, no
/// job. The in-memory step: a delete paused before its write guard applies
/// the database's answer, not its stale report (with the report taken, a
/// test switch, Carol is lost). The control: with the owner lock off, the
/// delete-first order ends with a post and no job.
#[tokio::test]
async fn t_a16_the_owner_race() {
    let test = "t_a16_audience_owner_race";
    for case in ["delete first", "post first", "in memory", "in memory from the report", "no lock"] {
        let Some(db) = fresh_db(test).await else {
            return;
        };
        let f = fixture(&db.pool, test).await;
        let name = db_name(&f.pool).await;
        let key = format!("{name}/{CAROL}");
        let (uri, op) = owner_race(&f).await;
        match case {
            "delete first" | "no lock" => {
                let _off = (case == "no lock").then(|| hooks::arm(hooks::OWNER_LOCK_OFF, &key));
                let hook = hooks::arm(hooks::AUDIENCE_DELETE_BEFORE_COMMIT, &key);
                let del = spawn_delete(&f, &uri);
                hook.reached().await;
                let post = spawn_post(&f, op);
                until_waiting(&f.pool, &post).await;
                hook.release();
                assert!(del.await.expect("delete"));
                post.await.expect("post");
                drop(hook);
                if case == "delete first" {
                    assert_eq!(owner_state(&f).await, (true, true, false, true), "{case}");
                } else {
                    let (db, _, ready, job) = owner_state(&f).await;
                    assert_eq!((db, ready, job), (true, false, false), "{case}: the lock's loss is seen");
                }
            }
            "post first" => {
                let hook = hooks::arm(hooks::WRITER_AFTER_OWNER_LOCK, &key);
                let post = spawn_post(&f, op);
                hook.reached().await;
                let del = spawn_delete(&f, &uri);
                until_waiting(&f.pool, &del).await;
                hook.release();
                post.await.expect("post");
                assert!(del.await.expect("delete"));
                drop(hook);
                assert_eq!(owner_state(&f).await, (true, true, true, false), "{case}");
            }
            _ => {
                let _report = (case == "in memory from the report").then(|| hooks::arm(hooks::OWNER_SET_FROM_REPORT, &key));
                let hook = hooks::arm(hooks::AUDIENCE_BEFORE_WRITE_GUARD, &key);
                let del = spawn_delete(&f, &uri);
                hook.reached().await;
                spawn_post(&f, op).await.expect("post");
                assert!(f.writer.scope().read_guard().await.is_owner(CAROL), "{case}: the post's own step");
                hook.release();
                assert!(del.await.expect("delete"));
                drop(hook);
                let memory = f.writer.scope().read_guard().await.is_owner(CAROL);
                assert_eq!(memory, case == "in memory", "{case}");
            }
        }
        assert_eq!(row(&f.pool, &uri).await, None, "{case}: the audience is gone");
        db.drop_db().await;
    }
}

/// T-A17: migration 0052 deletes the rows of a non-member outside S and of
/// a non-member still in S (as a removed member stays), reads an unknown
/// mode as `nobody` (still serving no reader), trims a name, drops one that
/// is only white space, and drops the audience listed rev. That a post
/// naming a deleted row stays author-only after `member add` is T-M4.
#[tokio::test]
async fn t_a17_the_migration() {
    let test = "t_a17_audience_migration";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let removed = "did:plc:rrrrrrrrrrrrrrrrrrrrrrrr";
    exec(&f.pool, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{removed}', 2)")).await;
    let seed = |owner: &str, rkey: &str, mode: &str, name: Option<&str>| {
        let name = name.map_or_else(|| "NULL".to_string(), |n| format!("'{n}'"));
        format!(
            "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, name, display_filled) \
             VALUES ('at://{owner}/{NSID_AUDIENCE}/{rkey}', '{owner}', '{rkey}', 'bafy{rkey}', '{REV}', '{mode}', ARRAY['{BOB}'], {name}, true)"
        )
    };
    for sql in [
        seed(EVE, "3kta17eve222", "list", None),
        seed(removed, "3kta17rem222", "list", None),
        seed(ALICE, "3kta17weird2", "weird", None),
        seed(ALICE, "3kta17frnd22", "list", Some("  Friends \u{3000}")),
        seed(ALICE, "3kta17blank2", "list", Some(" \t ")),
    ] {
        exec(&f.pool, &sql).await;
    }
    let weird = format!("at://{ALICE}/{NSID_AUDIENCE}/3kta17weird2");
    let post = f.private_post(ALICE, "3kta17post22", &weird, None).await;
    f.ready_all().await;
    assert!(!oracle_admits(&f.pool, BOB, &post).await, "an unknown mode serves no one before");
    sqlx::raw_sql(include_str!("../../../migrations/0052_audiences_held.sql")).execute(&f.pool).await.expect("0052");
    let rows: Vec<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT rkey, mode, name FROM private_audiences ORDER BY rkey").fetch_all(&f.pool).await.expect("rows");
    let r = |rkey: &str, mode: &str, name: Option<&str>| (rkey.to_string(), Some(mode.to_string()), name.map(str::to_string));
    assert_eq!(rows, [r("3kta17blank2", "list", None), r("3kta17frnd22", "list", Some("Friends")), r("3kta17weird2", "nobody", None)]);
    assert!(!oracle_admits(&f.pool, BOB, &post).await, "nobody, after");
    let columns: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 'sync_state' AND column_name = 'audience_listed_rev'",
    )
    .fetch_one(&f.pool)
    .await
    .expect("columns");
    assert_eq!(columns, 0);
    db.drop_db().await;
}
