//! `admin audiences check` (private-audiences design §3, §12 S0 tests),
//! against a fake PLC directory and a fake PDS.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::cli::commands::audiences_check::{Checker, Letter, Options, Surround, check};
use crate::firehose::classifier::NSID_AUDIENCE;
use crate::identity::IdentitySettings;
use crate::net::Transports;

/// How the fake PDS answers for one DID.
#[derive(Clone)]
enum Repo {
    /// Active, with these `(rkey, cid, value)` records.
    Records(Vec<(String, String, Value)>),
    Deactivated,
    /// `listRecords` waits longer than the check's timeout.
    Slow,
    /// `listRecords` answers 400.
    ListBadRequest,
}

type Repos = Arc<Mutex<HashMap<String, Repo>>>;

async fn repo_status(State(repos): State<Repos>, Query(q): Query<HashMap<String, String>>) -> Response {
    let did = q.get("did").cloned().unwrap_or_default();
    let repo = repos.lock().expect("repos").get(&did).cloned();
    match repo {
        None => (StatusCode::BAD_REQUEST, Json(json!({"error": "RepoNotFound"}))).into_response(),
        Some(Repo::Deactivated) => Json(json!({"did": did, "active": false, "status": "deactivated"})).into_response(),
        Some(_) => Json(json!({"did": did, "active": true, "rev": "3kzzzzzzzzzz2"})).into_response(),
    }
}

async fn list_records(State(repos): State<Repos>, Query(q): Query<HashMap<String, String>>) -> Response {
    let did = q.get("repo").cloned().unwrap_or_default();
    assert_eq!(q.get("collection").map(String::as_str), Some(NSID_AUDIENCE));
    let repo = repos.lock().expect("repos").get(&did).cloned();
    match repo {
        Some(Repo::Records(records)) => {
            // Pages of one record, to walk the cursor.
            let start: usize = q.get("cursor").and_then(|c| c.parse().ok()).unwrap_or(0);
            let page: Vec<Value> = records
                .iter()
                .skip(start)
                .take(1)
                .map(|(rkey, cid, value)| json!({"uri": format!("at://{did}/{NSID_AUDIENCE}/{rkey}"), "cid": cid, "value": value}))
                .collect();
            let cursor = (start + 1 < records.len()).then(|| (start + 1).to_string());
            Json(json!({"records": page, "cursor": cursor})).into_response()
        }
        Some(Repo::Slow) => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Json(json!({"records": []})).into_response()
        }
        Some(Repo::ListBadRequest) => (StatusCode::BAD_REQUEST, Json(json!({"error": "InvalidRequest"}))).into_response(),
        Some(Repo::Deactivated) | None => (StatusCode::BAD_REQUEST, Json(json!({"error": "RepoNotFound"}))).into_response(),
    }
}

fn did(c: char) -> String {
    format!("did:plc:{}", c.to_string().repeat(24))
}

fn uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/{NSID_AUDIENCE}/{rkey}")
}

fn valid() -> Value {
    json!({"$type": NSID_AUDIENCE, "mode": "list", "name": "Friends", "members": [did('z')], "createdAt": "2026-10-01T00:00:00Z"})
}

fn malformed() -> Value {
    json!({"$type": NSID_AUDIENCE, "members": "not an array"})
}

async fn member(pool: &PgPool, did: &str) {
    sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
        .bind(did)
        .execute(pool)
        .await
        .expect("member");
}

async fn row(pool: &PgPool, did: &str, rkey: &str, cid: &str) {
    sqlx::query(
        "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, display_filled) \
         VALUES ($1, $2, $3, $4, '3kaaaaaaaaaa2', 'list', '{}', true)",
    )
    .bind(uri(did, rkey))
    .bind(did)
    .bind(rkey)
    .bind(cid)
    .execute(pool)
    .await
    .expect("row");
}

async fn counts(pool: &PgPool) -> (i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM actors), (SELECT count(*) FROM identity_refresh), (SELECT count(*) FROM did_cache), \
                (SELECT count(*) FROM backfill_jobs)",
    )
        .fetch_one(pool)
        .await
        .expect("counts")
}

fn opts(dids: &[&str], accept: &[&str]) -> Options {
    Options { dids: dids.iter().map(ToString::to_string).collect(), accept: accept.iter().map(ToString::to_string).collect() }
}

/// The line of the members table for `did`.
fn line<'a>(report: &'a str, did: &str) -> &'a str {
    report.lines().find(|l| l.starts_with(did)).unwrap_or_default()
}

#[tokio::test]
async fn the_check_proves_the_rows_or_fails() {
    let Some(db) = crate::test_support::fresh_db("audiences_check").await else {
        return;
    };
    let pool = &db.pool;
    let repos: Repos = Arc::default();
    let pds = crate::test_support::serve_fake(
        axum::Router::new()
            .route("/xrpc/com.atproto.sync.getRepoStatus", get(repo_status))
            .route("/xrpc/com.atproto.repo.listRecords", get(list_records))
            .with_state(repos.clone()),
    )
    .await;
    let pds_doc = pds.clone();
    let plc = crate::test_support::serve_fake(axum::Router::new().route(
        "/{did}",
        get(move |Path(did): Path<String>| {
            let pds = pds_doc.clone();
            async move {
                if did == did_of('t') {
                    return (StatusCode::GONE, Json(json!({}))).into_response();
                }
                Json(json!({
                    "id": did,
                    "alsoKnownAs": ["at://someone.example.com"],
                    "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": pds}]
                }))
                .into_response()
            }
        }),
    ))
    .await;
    fn did_of(c: char) -> String {
        did(c)
    }
    let settings = IdentitySettings { plc_url: plc, fresh: Duration::from_secs(3600), stale_max: Duration::from_secs(86_400) };
    let checker = Checker::new(Transports::loopback_for_tests(), pool.clone(), settings, Duration::from_millis(500));

    let set = |d: &str, r: Repo| {
        repos.lock().expect("repos").insert(d.to_string(), r);
    };
    let rec = |rkey: &str, cid: &str, v: Value| (rkey.to_string(), cid.to_string(), v);

    // clean: two records, two matching rows (two pages of the listing).
    let clean = did('a');
    member(pool, &clean).await;
    set(&clean, Repo::Records(vec![rec("3kaaaaaaaaaa2", "bafya1", valid()), rec("3kaaaaaaaaab2", "bafya2", valid())]));
    row(pool, &clean, "3kaaaaaaaaaa2", "bafya1").await;
    row(pool, &clean, "3kaaaaaaaaab2", "bafya2").await;
    // A different repo CID.
    let differing = did('b');
    member(pool, &differing).await;
    set(&differing, Repo::Records(vec![rec("3kbbbbbbbbbb2", "bafynew", valid())]));
    row(pool, &differing, "3kbbbbbbbbbb2", "bafyold").await;
    // An extra repo record, with no row.
    let unstored = did('c');
    member(pool, &unstored).await;
    set(&unstored, Repo::Records(vec![rec("3kcccccccccc2", "bafyc", valid())]));
    // A row the repo no longer lists.
    let unlisted = did('d');
    member(pool, &unlisted).await;
    set(&unlisted, Repo::Records(Vec::new()));
    row(pool, &unlisted, "3kdddddddddd2", "bafyd").await;
    // A record the parser refuses, never stored.
    let refused = did('e');
    member(pool, &refused).await;
    set(&refused, Repo::Records(vec![rec("3keeeeeeeeee2", "bafye", malformed())]));
    // A valid record, then a malformed update: the earlier row stays.
    let updated_badly = did('f');
    member(pool, &updated_badly).await;
    set(&updated_badly, Repo::Records(vec![rec("3kffffffffff2", "bafyf2", malformed())]));
    row(pool, &updated_badly, "3kffffffffff2", "bafyf1").await;
    // Deactivated, no rows; and deactivated with rows.
    let gone = did('g');
    member(pool, &gone).await;
    set(&gone, Repo::Deactivated);
    let gone_rows = did('h');
    member(pool, &gone_rows).await;
    set(&gone_rows, Repo::Deactivated);
    row(pool, &gone_rows, "3khhhhhhhhhh2", "bafyh").await;
    // A PDS that times out, and one that answers listRecords with a 400.
    let slow = did('i');
    member(pool, &slow).await;
    set(&slow, Repo::Slow);
    let bad = did('j');
    member(pool, &bad).await;
    set(&bad, Repo::ListBadRequest);
    // A tombstoned DID with no rows.
    let tombstoned = did('t');
    member(pool, &tombstoned).await;
    // A repo its PDS does not know, with no rows: the path on which the
    // backfill's `prepare` writes `identity_refresh`.
    let unknown = did('k');
    member(pool, &unknown).await;
    // A running `authz` job, printed only.
    sqlx::query("INSERT INTO backfill_jobs (did, depth, state) VALUES ($1, 'authz', 'running')")
        .bind(&clean)
        .execute(pool)
        .await
        .expect("job");

    let before = counts(pool).await;
    let letters = vec![Letter {
        id: "1-0".to_string(),
        repo: clean.clone(),
        path: format!("{NSID_AUDIENCE}/3kaaaaaaaaaa2"),
        error: "boom".to_string(),
    }];
    let surround = || Surround { consumer: Some(Ok((Some(0), Some(0)))), letters: Some(Ok(letters.clone())) };

    // Clean members, with a dead letter and a running job, pass.
    let (report, pass) = check(pool, &checker, &opts(&[&clean, &tombstoned], &[]), surround()).await.expect("check");
    assert!(pass, "{report}");
    assert!(line(&report, &clean).contains("clean"), "{report}");
    assert!(line(&report, &tombstoned).contains("inactive"), "{report}");
    assert!(report.contains("audience and whole-entry dead letters: 1"), "{report}");
    assert!(report.contains(&format!("{NSID_AUDIENCE}/3kaaaaaaaaaa2")), "{report}");
    assert!(report.contains("open member, private and authz jobs: 1"), "{report}");
    assert!(report.lines().any(|l| l.starts_with(&clean) && l.contains("authz") && l.contains("running")), "{report}");
    // The start time and the duration are printed.
    assert!(report.starts_with("admin audiences check: started 20"), "{report}");
    assert!(report.contains("\nduration: "), "{report}");
    assert!(report.ends_with("result: PASS"), "{report}");

    // Each mismatch fails.
    for (d, what) in [(&differing, "differing cid"), (&unstored, "missing row"), (&unlisted, "row not listed")] {
        let (report, pass) = check(pool, &checker, &opts(&[d], &[]), Surround::default()).await.expect("check");
        assert!(!pass, "{report}");
        assert!(line(&report, d).contains("mismatch"), "{report}");
        assert!(report.lines().any(|l| l.starts_with(d.as_str()) && l.contains(what)), "{what}: {report}");
        assert!(report.ends_with("result: FAIL (1 member failing)"), "{report}");
        // A mismatch is never accepted.
        let (report, pass) = check(pool, &checker, &opts(&[d], &[d]), Surround::default()).await.expect("check");
        assert!(!pass, "{report}");
    }

    // Refused records pass; a refused record's stored row is unproven.
    let (report, pass) = check(pool, &checker, &opts(&[&refused, &updated_badly], &[]), Surround::default()).await.expect("check");
    assert!(pass, "{report}");
    assert!(line(&report, &refused).contains("refused"), "{report}");
    assert!(line(&report, &updated_badly).contains("refused"), "{report}");
    assert!(report.contains(&format!("refused record with a stored row: {}", uri(&updated_badly, "3kffffffffff2"))), "{report}");

    // Deactivated with no rows passes; with rows it fails unless accepted.
    let (report, pass) = check(pool, &checker, &opts(&[&gone], &[]), Surround::default()).await.expect("check");
    assert!(pass && line(&report, &gone).contains("inactive"), "{report}");
    let (report, pass) = check(pool, &checker, &opts(&[&unknown], &[]), Surround::default()).await.expect("check");
    assert!(pass && line(&report, &unknown).contains("repo not found"), "{report}");
    let (report, pass) = check(pool, &checker, &opts(&[&gone_rows], &[]), Surround::default()).await.expect("check");
    assert!(!pass && line(&report, &gone_rows).contains("FAIL"), "{report}");
    let (report, pass) = check(pool, &checker, &opts(&[&gone_rows], &[&gone_rows]), Surround::default()).await.expect("check");
    assert!(pass, "{report}");
    assert!(report.contains(&format!("accepted with --accept-unchecked: {gone_rows}")), "{report}");

    // A timeout and a 400 are unchecked: they fail unless accepted.
    for d in [&slow, &bad] {
        let (report, pass) = check(pool, &checker, &opts(&[d], &[]), Surround::default()).await.expect("check");
        assert!(!pass && line(&report, d).contains("unchecked"), "{report}");
        let (report, pass) = check(pool, &checker, &opts(&[d], &[d]), Surround::default()).await.expect("check");
        assert!(pass, "{report}");
        assert!(report.contains(&format!("accepted with --accept-unchecked: {d}")), "{report}");
    }

    // Every member at once: the failing ones are counted.
    let (report, pass) = check(pool, &checker, &Options::default(), Surround::default()).await.expect("check");
    assert!(!pass, "{report}");
    assert!(report.contains("members checked: 12"), "{report}");
    assert!(report.ends_with("result: FAIL (6 members failing)"), "{report}");
    // A named DID that is not a member is not checked, and fails the run.
    let (report, pass) = check(pool, &checker, &opts(&[&clean, &did('y')], &[]), Surround::default()).await.expect("check");
    assert!(!pass && report.contains(&format!("not a member, not checked: {}", did('y'))), "{report}");
    assert!(report.ends_with("result: FAIL (1 named DIDs are not members)"), "{report}");

    // The runs wrote nothing.
    assert_eq!(counts(pool).await, before);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM private_audiences").fetch_one(pool).await.expect("rows");
    assert_eq!(rows, 6);
    db.drop_db().await;
}
