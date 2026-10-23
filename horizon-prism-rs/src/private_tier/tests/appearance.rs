//! Private appearance (private-appearance design §11): `getAppearance` and
//! `putAppearance` over HTTP through the private router, T-AP1 to T-AP11
//! and T-AP15 (T-AP13 is the stats label test and the image check).

use super::*;
use crate::aggregations::purge::Purger;
use crate::aggregations::scope_state::PurgeKind;
use crate::test_support::hooks;

const GET: &str = "app.nearhorizon.private.getAppearance";
const PUT: &str = "app.nearhorizon.private.putAppearance";
const NSID: &str = "app.nearhorizon.actor.appearance";

/// The served private router with roomy limits and no floor.
async fn served(f: &Fixture) -> String {
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    serve(f.state_of(app, Some(Duration::from_secs(1)), Duration::ZERO)).await
}

/// A small valid value, with a field the record lexicon doesn't know.
fn value() -> Value {
    json!({"$type": NSID, "version": 1, "updatedAt": "2026-10-23T12:00:00.000Z", "colorMode": "dark", "futureField": {"x": [1, 2]}})
}

/// Ten presets with 64-grapheme names of 11 bytes each, eight stripes
/// everywhere.
fn maximal() -> Value {
    let stripes: Vec<Value> = (0..8).map(|i| json!(format!("#0{i}0{i}0{i}"))).collect();
    let presets: Vec<Value> = (0..10)
        .map(|i| json!({"name": format!("{i}{}", "\u{1F468}\u{200D}\u{1F469}".repeat(63)), "backdrop": {"stripes": stripes}}))
        .collect();
    json!({"$type": NSID, "version": 3, "updatedAt": "2026-10-23T12:00:00.000Z", "backdrop": "custom",
        "custom": {"mode": "stripes", "stripes": stripes, "a": 1}, "presets": presets, "colorMode": "light",
        "darkTheme": "dim", "font": "serif", "fontSize": "large"})
}

/// A legacy value the record lexicon refuses: an 11th preset, a
/// 65-grapheme name, a `custom` without `a`.
fn legacy() -> Value {
    let mut presets: Vec<Value> = (0..11).map(|i| json!({"name": format!("p{i}")})).collect();
    presets[0] = json!({"name": "x".repeat(65)});
    json!({"$type": NSID, "version": 1, "updatedAt": "2026-10-23T12:00:00Z", "presets": presets, "custom": {"mode": "solid"}})
}

/// `getAppearance` by `who`: status, body, `Cache-Control`.
async fn read(base: &str, who: &str) -> (u16, Value, Option<String>) {
    let token = mint(&signing_key(key_of(who)), who, &format!("did:web:{AUDIENCE_HOST}"), Some(GET));
    let r = reqwest::Client::new()
        .get(format!("{base}/xrpc/{GET}"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("request");
    answer(r).await
}

/// `putAppearance` by `who` with a raw body: status, body, `Cache-Control`.
async fn put_raw(base: &str, who: &str, body: String) -> (u16, Value, Option<String>) {
    let token = mint(&signing_key(key_of(who)), who, &format!("did:web:{AUDIENCE_HOST}"), Some(PUT));
    let r = reqwest::Client::new()
        .post(format!("{base}/xrpc/{PUT}"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("request");
    answer(r).await
}

async fn put(base: &str, who: &str, body: &Value) -> (u16, Value, Option<String>) {
    put_raw(base, who, body.to_string()).await
}

async fn answer(r: reqwest::Response) -> (u16, Value, Option<String>) {
    let status = r.status().as_u16();
    let cache = r.headers().get("cache-control").and_then(|v| v.to_str().ok()).map(str::to_string);
    let text = r.text().await.expect("body");
    (status, serde_json::from_str(&text).unwrap_or(Value::Null), cache)
}

/// The stored row as text, every column: `(store, appearance, rev,
/// created_at, updated_at)`.
async fn row(pool: &PgPool, did: &str) -> Option<(String, Option<String>, i64, String, String)> {
    sqlx::query_as("SELECT store, appearance::text, rev, created_at::text, updated_at::text FROM private_appearance WHERE did = $1")
        .bind(did)
        .fetch_optional(pool)
        .await
        .expect("row")
}

fn rev_of(v: &Value) -> String {
    v["rev"].as_str().expect("rev").to_string()
}

/// A refusal: 400 with `error`; the methods' own refusals carry an
/// `"Error: "`-prefixed message (T-AP11), the gate's `NotEnrolled` its
/// shared one.
fn refused(r: &(u16, Value, Option<String>), error: &str) {
    assert_eq!((r.0, r.1["error"].as_str()), (400, Some(error)), "{}", r.1);
    if error != "NotEnrolled" {
        assert!(r.1["message"].as_str().is_some_and(|m| m.starts_with("Error: ")), "{}", r.1);
    }
    assert!(r.2.is_none(), "an error carries no Cache-Control");
}

async fn db_name(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT current_database()::text").fetch_one(pool).await.expect("db")
}

/// T-AP1, T-AP2, T-AP11: no row reads `{}`; `private` with `swapRev: ""`
/// round-trips the value as sent (an unknown field included); `public`
/// likewise; `device` keeps the choice alone, `migratedAt` unchanged. Every
/// answer is `private, no-store`.
#[tokio::test]
async fn t_ap1_t_ap2_round_trip() {
    let test = "t_ap1_appearance_round_trip";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let base = served(&f).await;
    let (status, body, cache) = read(&base, BOB).await;
    assert_eq!((status, body.clone(), cache.as_deref()), (200, json!({}), Some("private, no-store")), "{body}");

    let (status, first, cache) = put(&base, BOB, &json!({"store": "private", "appearance": value(), "swapRev": ""})).await;
    assert_eq!((status, cache.as_deref()), (200, Some("private, no-store")), "{first}");
    assert_eq!((first["store"].clone(), first["appearance"].clone()), (json!("private"), value()), "{first}");
    let migrated = first["migratedAt"].as_str().expect("migratedAt").to_string();
    assert!(migrated.ends_with('Z') && migrated.len() == "2026-10-23T12:00:00.000Z".len(), "{migrated}");
    assert!(rev_of(&first).parse::<i64>().is_ok());
    let (_, got, cache) = read(&base, BOB).await;
    assert_eq!((got.clone(), cache.as_deref()), (first.clone(), Some("private, no-store")));
    assert_eq!(row(&f.pool, BOB).await.expect("row").1.as_deref(), Some(value().to_string().as_str()));

    let mut public = value();
    public["colorMode"] = json!("light");
    let (status, second, _) = put(&base, BOB, &json!({"store": "public", "appearance": public})).await;
    assert_eq!((status, second["store"].clone(), second["appearance"].clone()), (200, json!("public"), public.clone()), "{second}");
    assert_eq!(read(&base, BOB).await.1, second);

    let (status, third, _) = put(&base, BOB, &json!({"store": "device", "swapRev": rev_of(&second)})).await;
    assert_eq!(status, 200, "{third}");
    assert_eq!(third["store"], "device");
    assert!(third.get("appearance").is_none(), "{third}");
    assert_eq!(third["migratedAt"], json!(migrated), "the first time the choice was set");
    let stored = row(&f.pool, BOB).await.expect("row");
    assert_eq!((stored.0.as_str(), stored.1), ("device", None));
    assert_eq!(read(&base, BOB).await.1, third);
    assert_eq!(row(&f.pool, CAROL).await, None, "another member has none");
    db.drop_db().await;
}

/// T-AP3, T-AP6: `swapRev` the current rev writes and moves it; a stale
/// rev, a rev with no row, `""` with a row and a non-decimal are
/// `InvalidSwap` with the row unchanged; absent writes over anything. Ten
/// puts give ten strictly increasing revs.
#[tokio::test]
async fn t_ap3_t_ap6_swap_and_rev() {
    let test = "t_ap3_appearance_swap";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let base = served(&f).await;
    // A rev with no row.
    refused(&put(&base, BOB, &json!({"store": "device", "swapRev": "12345"})).await, "InvalidSwap");
    assert_eq!(row(&f.pool, BOB).await, None);
    let (_, first, _) = put(&base, BOB, &json!({"store": "private", "appearance": value(), "swapRev": ""})).await;
    let (status, second, _) = put(&base, BOB, &json!({"store": "device", "swapRev": rev_of(&first)})).await;
    assert_eq!(status, 200, "{second}");
    assert!(rev_of(&second).parse::<i64>().expect("rev") > rev_of(&first).parse::<i64>().expect("rev"));
    let before = row(&f.pool, BOB).await;
    for swap in [rev_of(&first), String::new(), "not-a-rev".to_string()] {
        let r = put(&base, BOB, &json!({"store": "public", "appearance": value(), "swapRev": swap})).await;
        refused(&r, "InvalidSwap");
        assert_eq!(r.1["message"], "Error: swapRev does not match the stored rev");
        assert_eq!(row(&f.pool, BOB).await, before, "unchanged: {swap}");
    }
    // Absent: written over any row.
    let (status, third, _) = put(&base, BOB, &json!({"store": "public", "appearance": value()})).await;
    assert_eq!((status, third["store"].as_str()), (200, Some("public")));
    // Ten in a tight loop: strictly increasing.
    let mut last = rev_of(&third).parse::<i64>().expect("rev");
    for _ in 0..10 {
        let (_, v, _) = put(&base, BOB, &json!({"store": "device"})).await;
        let rev = rev_of(&v).parse::<i64>().expect("rev");
        assert!(rev > last, "{rev} after {last}");
        last = rev;
    }
    db.drop_db().await;
}

/// T-AP4: the value's refusals write nothing; the maximal record, a legacy
/// value the record lexicon refuses, and NULs in a string and a key pass and
/// come back unchanged.
#[tokio::test]
async fn t_ap4_value_checks() {
    let test = "t_ap4_appearance_values";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let base = served(&f).await;
    let with = |k: &str, x: Value| {
        let mut v = value();
        v[k] = x;
        v
    };
    let mut no_type = value();
    no_type.as_object_mut().expect("object").remove("$type");
    let mut no_version = value();
    no_version.as_object_mut().expect("object").remove("version");
    let base_len = with("futureField", json!("")).to_string().len();
    let over = with("futureField", json!("x".repeat(16_384 - base_len + 1)));
    for bad in [
        no_type,
        with("$type", json!("app.bsky.actor.profile")),
        json!([value()]),
        no_version,
        with("version", json!(1.5)),
        with("version", json!(0)),
        with("updatedAt", json!("yesterday")),
        over,
    ] {
        refused(&put(&base, BOB, &json!({"store": "private", "appearance": bad})).await, "InvalidRequest");
    }
    assert_eq!(row(&f.pool, BOB).await, None, "nothing written");
    let nul = with("presets", json!([{"name": "a\u{0}b", "k\u{0}ey": 1}]));
    for good in [maximal(), legacy(), nul] {
        let (status, body, _) = put(&base, BOB, &json!({"store": "public", "appearance": good})).await;
        assert_eq!((status, body["appearance"].clone()), (200, good.clone()), "{body}");
        assert_eq!(read(&base, BOB).await.1["appearance"], good);
        assert_eq!(row(&f.pool, BOB).await.expect("row").1, Some(good.to_string()), "stored as sent");
    }
    db.drop_db().await;
}

/// T-AP5: malformed inputs are `InvalidRequest` and write nothing.
#[tokio::test]
async fn t_ap5_input_refusals() {
    let test = "t_ap5_appearance_inputs";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let base = served(&f).await;
    let bodies = [
        "[]".to_string(),
        String::new(),
        "{}".to_string(),
        json!({"store": "cloud"}).to_string(),
        json!({"store": "private"}).to_string(),
        json!({"store": "device", "appearance": value()}).to_string(),
        json!({"store": "public", "appearance": null}).to_string(),
        json!({"store": "device", "swapRev": null}).to_string(),
        json!({"store": "device", "swapRev": 3}).to_string(),
        json!({"store": "device", "pad": "x".repeat(65 * 1024)}).to_string(),
    ];
    for body in bodies {
        let r = put_raw(&base, BOB, body.clone()).await;
        refused(&r, "InvalidRequest");
        assert!(r.2.is_none(), "an error carries no Cache-Control");
    }
    assert_eq!(row(&f.pool, BOB).await, None);
    db.drop_db().await;
}

/// T-AP7: two puts on the same `swapRev`, and two `""` creates, started
/// together: exactly one of each wins, the other is `InvalidSwap`.
#[tokio::test]
async fn t_ap7_concurrent_puts() {
    let test = "t_ap7_appearance_concurrency";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let base = served(&f).await;
    let create = json!({"store": "device", "swapRev": ""});
    let (a, b) = tokio::join!(put(&base, BOB, &create), put(&base, BOB, &create));
    let mut statuses = [a.0, b.0];
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 400], "{} {}", a.1, b.1);
    let won = if a.0 == 200 { &a.1 } else { &b.1 };
    let swap = json!({"store": "device", "swapRev": rev_of(won)});
    let (a, b) = tokio::join!(put(&base, BOB, &swap), put(&base, BOB, &swap));
    let mut statuses = [a.0, b.0];
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 400], "{} {}", a.1, b.1);
    assert!([&a.1, &b.1].iter().any(|v| v["error"] == "InvalidSwap"));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM private_appearance").fetch_one(&f.pool).await.expect("count");
    assert_eq!(count, 1);
    db.drop_db().await;
}

/// T-AP8: no auth is 401; a non-member is `NotEnrolled` (a malformed body
/// included); a member never ready and one re-indexing are served, while
/// another private method still answers them `NotReady` and `Reindexing`.
#[tokio::test]
async fn t_ap8_the_gate() {
    let test = "t_ap8_appearance_gate";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    f.ready_all().await;
    let base = served(&f).await;
    for (method, verb) in [(GET, reqwest::Method::GET), (PUT, reqwest::Method::POST)] {
        let r = reqwest::Client::new().request(verb, format!("{base}/xrpc/{method}")).send().await.expect("request");
        assert_eq!(r.status().as_u16(), 401, "{method}");
    }
    let other = "app.nearhorizon.private.getNotificationPreferences";
    for (sql, other_status, other_error) in [
        ("ready_at = NULL, first_ready_at = NULL", 400, "NotReady"),
        ("ready_at = NULL, first_ready_at = now()", 503, "Reindexing"),
    ] {
        exec(&f.pool, &format!("UPDATE members SET {sql} WHERE did = '{CAROL}'")).await;
        let (status, body, _) = read(&base, CAROL).await;
        assert_eq!(status, 200, "{sql}: {body}");
        let (status, body, _) = put(&base, CAROL, &json!({"store": "device"})).await;
        assert_eq!((status, body["store"].as_str()), (200, Some("device")), "{sql}: {body}");
        let (status, body, _) = get(&base, other, "", CAROL, key_of(CAROL)).await;
        assert!(status == other_status && body.contains(&format!("\"{other_error}\"")), "{sql}: {body}");
    }
    exec(&f.pool, &format!("DELETE FROM members WHERE did = '{DAVE}'")).await;
    refused(&read(&base, DAVE).await, "NotEnrolled");
    refused(&put(&base, DAVE, &json!({"store": "device"})).await, "NotEnrolled");
    refused(&put_raw(&base, DAVE, "not json".to_string()).await, "NotEnrolled");
    assert_eq!(row(&f.pool, DAVE).await, None);
    db.drop_db().await;
}

/// T-AP9: `member_removed` and `account_deleted` take the member's row and
/// no other; the removed account reads `NotEnrolled`; re-added, it reads
/// `{}` and `swapRev: ""` creates (RM-1).
#[tokio::test]
async fn t_ap9_removal_and_rejoin() {
    let test = "t_ap9_appearance_removal";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let base = served(&f).await;
    let purger = Purger::new(f.pool.clone(), f.writer.scope().clone(), Arc::clone(f.writer.private()));
    let mut old = std::collections::HashMap::new();
    for who in [ALICE, BOB, CAROL] {
        let (status, body, _) = put(&base, who, &json!({"store": "public", "appearance": value()})).await;
        assert_eq!(status, 200);
        old.insert(who, rev_of(&body));
    }
    for (who, kind) in [(BOB, PurgeKind::MemberRemoved), (CAROL, PurgeKind::AccountDeleted)] {
        purger.purge(who, kind).await.expect("purge");
        assert_eq!(row(&f.pool, who).await, None, "{kind:?}");
        refused(&read(&base, who).await, "NotEnrolled");
        exec(&f.pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{who}', 'admin', now())")).await;
        let (status, body, _) = read(&base, who).await;
        assert_eq!((status, body), (200, json!({})), "{kind:?}");
        let (status, body, _) = put(&base, who, &json!({"store": "device", "swapRev": ""})).await;
        assert_eq!(status, 200, "{kind:?}: {body}");
        // T-AP6: the new row's rev is past every old one, and the old rev
        // never matches it.
        assert!(rev_of(&body).parse::<i64>().expect("rev") > old[who].parse::<i64>().expect("rev"));
        refused(&put(&base, who, &json!({"store": "device", "swapRev": old[who]})).await, "InvalidSwap");
    }
    assert_eq!(row(&f.pool, ALICE).await.expect("kept").0, "public");
    db.drop_db().await;
}

/// T-AP10: a put held before the DID's lock while the removal marker
/// commits answers `NotEnrolled` and writes nothing; one held after the
/// lock makes the marker wait, writes, and the purge then takes the row.
#[tokio::test]
async fn t_ap10_the_purge_race() {
    let test = "t_ap10_appearance_race";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let base = served(&f).await;
    let purger = Purger::new(f.pool.clone(), f.writer.scope().clone(), Arc::clone(f.writer.private()));
    let name = db_name(&f.pool).await;

    // Before the lock: the marker commits first.
    let armed = hooks::arm(hooks::APPEARANCE_BEFORE_LOCK, &format!("{name}/{BOB}"));
    let body = json!({"store": "public", "appearance": value()});
    let pending = tokio::spawn({
        let (base, body) = (base.clone(), body.clone());
        async move { put(&base, BOB, &body).await }
    });
    armed.reached().await;
    purger.purge(BOB, PurgeKind::MemberRemoved).await.expect("purge");
    armed.release();
    let r = pending.await.expect("put");
    refused(&r, "NotEnrolled");
    assert_eq!(row(&f.pool, BOB).await, None);
    drop(armed);

    // After the lock: the marker waits for the put's commit.
    let armed = hooks::arm(hooks::APPEARANCE_AFTER_LOCK, &format!("{name}/{CAROL}"));
    let pending = tokio::spawn({
        let base = base.clone();
        async move { put(&base, CAROL, &body).await }
    });
    armed.reached().await;
    let purge = tokio::spawn({
        let purger = purger.clone();
        async move { purger.purge(CAROL, PurgeKind::MemberRemoved).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let member: bool = sqlx::query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM members WHERE did = '{CAROL}')"))
        .fetch_one(&f.pool)
        .await
        .expect("member");
    assert!(member, "the marker waits on the put's lock");
    armed.release();
    let r = pending.await.expect("put");
    assert_eq!(r.0, 200, "{}", r.1);
    purge.await.expect("join").expect("purge");
    assert_eq!(row(&f.pool, CAROL).await, None, "the purge took it");
    db.drop_db().await;
}

/// T-AP15: through the final router, a get and a put each record one
/// `xrpc_request_seconds{nsid}` observation.
#[tokio::test]
async fn t_ap15_request_time_is_recorded() {
    use crate::xrpc::router::{FinalConfig, finalize};

    let test = "t_ap15_appearance_metric";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    let routes = finalize(router(f.state_of(app.clone(), Some(Duration::from_secs(1)), Duration::ZERO)), app, FinalConfig {
        not_ready_retry_secs: 5,
        public: crate::honest_501::PublicMessages::default(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, routes.into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    let base = format!("http://{addr}");
    assert_eq!(read(&base, BOB).await.0, 200);
    assert_eq!(put(&base, BOB, &json!({"store": "device"})).await.0, 200);
    let text = handle.render();
    for method in [GET, PUT] {
        let nsid = format!(r#"nsid="{method}""#);
        assert_eq!(crate::test_support::series(&text, "xrpc_request_seconds_count", &[&nsid]), Some(1.0), "{method}: {text}");
    }
    db.drop_db().await;
}

/// T-AP14: `getMembership`'s `features` ends with `private.appearance`.
#[test]
fn t_ap14_the_feature_is_listed_last() {
    assert_eq!(crate::private_tier::FEATURES.last(), Some(&"private.appearance"));
}
