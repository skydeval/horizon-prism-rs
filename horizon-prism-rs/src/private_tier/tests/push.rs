//! Push wakes for private notifications: a new row from a live write is
//! marked once at its commit, a backfill or catch-up row and a repeat are
//! not; the recipient check applies the read-time rules and `push`; a call
//! carries DIDs only, with a service JWT the push service accepts, and its
//! outcome follows the status.

use std::collections::BTreeSet;
use std::sync::Mutex as StdMutex;

use axum::extract::State as AxState;
use k256::ecdsa::SigningKey;
use sqlx::postgres::PgListener;

use super::*;
use crate::config::PrivatePushConfig;
use crate::kryphocron_glue::ServiceKey;
use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, verifier};
use crate::net::NetClient;
use crate::private_tier::push::{self, CHANNEL, LXM, PushSender};

const SECRET: [u8; 32] = [21u8; 32];

async fn listener(pool: &PgPool) -> PgListener {
    let mut l = PgListener::connect_with(pool).await.expect("connect");
    l.listen(CHANNEL).await.expect("listen");
    l
}

/// The ids heard until none arrives for `quiet`.
async fn heard(l: &mut PgListener, quiet: Duration) -> BTreeSet<i64> {
    let mut out = BTreeSet::new();
    while let Ok(Ok(n)) = tokio::time::timeout(quiet, l.recv()).await {
        out.insert(n.payload().parse().expect("an id"));
    }
    out
}

async fn row_ids(pool: &PgPool, recipient: &str) -> BTreeSet<i64> {
    sqlx::query_scalar::<_, i64>("SELECT id FROM private_notifications WHERE recipient_did = $1")
        .bind(recipient)
        .fetch_all(pool)
        .await
        .expect("ids")
        .into_iter()
        .collect()
}

fn now_at() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Alice's post T under an audience of Alice, Bob and Carol: `(T, the
/// audience)`.
async fn alice_post(f: &Fixture) -> (String, String) {
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[ALICE, BOB, CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    f.ready_all().await;
    (t, aud)
}

/// Live rows are marked once, at commit; a repeat writes and marks nothing.
#[tokio::test]
async fn live_rows_are_marked_once() {
    let test = "push_live_marked";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    push::enable();
    let (t, aud) = alice_post(&f).await;
    let mut l = listener(&f.pool).await;
    let at = now_at();
    f.ingest(CAROL, LIKE, "3klikecarol2", subject_record(LIKE, &t, &at)).await;
    f.private_post(BOB, "3kreplybob22", &aud, Some((&t, &t))).await;
    let rows = row_ids(&f.pool, ALICE).await;
    assert_eq!(rows.len(), 2, "a like and a reply");
    assert_eq!(heard(&mut l, Duration::from_secs(1)).await, rows);
    f.ingest(CAROL, LIKE, "3klikecarol2", subject_record(LIKE, &t, &at)).await;
    assert!(heard(&mut l, Duration::from_millis(500)).await.is_empty(), "a repeat marks nothing");
    db.drop_db().await;
}

/// A recent record found by backfill or catch-up notifies (the 48 h rule)
/// but is not marked; one from `requestRecord` is.
#[tokio::test]
async fn backfill_and_catch_up_rows_are_not_marked() {
    let test = "push_not_live";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    push::enable();
    let (t, _) = alice_post(&f).await;
    let mut l = listener(&f.pool).await;
    let at = now_at();
    for (source, rkey) in [(WriteSource::Backfill, "3klikeback22"), (WriteSource::CatchUp, "3klikecatch2")] {
        f.apply_from(source, CAROL, Action::Create, LIKE, rkey, REV, Some(subject_record(LIKE, &t, &at))).await;
    }
    f.apply_from(WriteSource::Backfill, BOB, Action::Create, REPOST, "3krepback222", REV, Some(subject_record(REPOST, &t, &at))).await;
    let quiet = row_ids(&f.pool, ALICE).await;
    assert_eq!(quiet.len(), 3, "Carol's two likes and Bob's repost: {quiet:?}");
    assert!(heard(&mut l, Duration::from_millis(700)).await.is_empty());
    f.apply_from(WriteSource::IndexRequest, BOB, Action::Create, LIKE, "3klikereq222", REV, Some(subject_record(LIKE, &t, &at))).await;
    let all = row_ids(&f.pool, ALICE).await;
    let new: BTreeSet<i64> = all.difference(&quiet).copied().collect();
    assert_eq!(new.len(), 1);
    assert_eq!(heard(&mut l, Duration::from_secs(1)).await, new);
    db.drop_db().await;
}

/// The recipient check: rows that exist and pass, under `push`.
#[tokio::test]
async fn recipients_follow_push_and_the_read_rules() {
    let test = "push_recipients";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    let (t, _) = alice_post(&f).await;
    let at = now_at();
    f.ingest(CAROL, LIKE, "3klikecarol2", subject_record(LIKE, &t, &at)).await;
    f.ingest(BOB, REPOST, "3krepbob2222", subject_record(REPOST, &t, &at)).await;
    let ids: Vec<i64> = row_ids(&f.pool, ALICE).await.into_iter().collect();
    assert_eq!(ids.len(), 2);
    let recipients = || schema::push_recipients(&f.pool, &ids);
    assert_eq!(recipients().await.expect("check"), [ALICE]);

    let set = |prefs: Value| {
        let pool = f.pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO private_notification_prefs (did, prefs) VALUES ($1, $2) \
                 ON CONFLICT (did) DO UPDATE SET prefs = EXCLUDED.prefs",
            )
            .bind(ALICE)
            .bind(prefs)
            .execute(&pool)
            .await
            .expect("prefs");
        }
    };
    let quiet = json!({"include": "all", "list": true, "push": false});
    set(json!({"like": quiet, "repost": quiet})).await;
    assert!(recipients().await.expect("check").is_empty(), "push off for both reasons");
    set(json!({"like": quiet})).await;
    assert_eq!(recipients().await.expect("check"), [ALICE], "the repost still pushes");
    // A listed-off reason does not push either.
    set(json!({"like": quiet, "repost": {"include": "all", "list": false, "push": true}})).await;
    assert!(recipients().await.expect("check").is_empty());
    // A full mute of the reposter leaves only the like, which is quiet.
    set(json!({"like": quiet})).await;
    exec(&f.pool, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{ALICE}', '{BOB}', 'full')")).await;
    assert!(recipients().await.expect("check").is_empty(), "muted");
    // Rows gone since: nothing.
    exec(&f.pool, "DELETE FROM mutes").await;
    exec(&f.pool, "DELETE FROM private_notifications").await;
    assert!(recipients().await.expect("check").is_empty(), "gone");
    db.drop_db().await;
}

/// One call the fake push service saw: its `Authorization` and body.
type Call = (Option<String>, Value);

#[derive(Clone, Default)]
struct Seen {
    calls: Arc<StdMutex<Vec<Call>>>,
    status: Arc<std::sync::atomic::AtomicU16>,
}

async fn notify(AxState(s): AxState<Seen>, headers: axum::http::HeaderMap, body: axum::body::Bytes) -> axum::http::StatusCode {
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).map(str::to_string);
    s.calls.lock().expect("lock").push((auth, serde_json::from_slice(&body).expect("json")));
    axum::http::StatusCode::from_u16(s.status.load(std::sync::atomic::Ordering::SeqCst)).expect("status")
}

/// A call carries `{"dids": [...]}` only, with a service JWT for `LXM`
/// that a verifier resolving this AppView's did.json accepts; its outcome
/// follows the status.
#[tokio::test]
async fn a_call_carries_dids_and_a_service_jwt() {
    let seen = Seen::default();
    seen.status.store(200, std::sync::atomic::Ordering::SeqCst);
    let app = axum::Router::new().route(&format!("/xrpc/{LXM}"), axum::routing::post(notify)).with_state(seen.clone());
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = tcp.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(tcp, app).await.expect("serve") });

    let key = ServiceKey::new("prism.example.com", &SECRET).expect("key");
    let aud = format!("did:web:{AUDIENCE_HOST}");
    let cfg = PrivatePushConfig {
        url: Some(format!("http://{addr}/")),
        service_did: Some(aud.clone()),
        ..PrivatePushConfig::default()
    };
    let pool = PgPool::connect_lazy("postgres://unused@127.0.0.1/unused").expect("lazy pool");
    let sender = PushSender::new(pool, NetClient::loopback_for_tests(), key.clone(), &cfg).expect("configured");
    let dids = vec![ALICE.to_string(), BOB.to_string()];
    assert_eq!(sender.send(&dids).await, "ok");
    let (auth, body) = seen.calls.lock().expect("lock")[0].clone();
    assert_eq!(body, json!({"dids": [ALICE, BOB]}), "DIDs only");

    let fetcher = StubFetcher::default();
    fetcher.set_key(key.did(), SigningKey::from_slice(&SECRET).expect("signing"));
    let (verifier, _drainer) = verifier(fetcher, 10, 4);
    let caller = verifier.verify_request_jwt(auth.as_deref(), LXM).await.expect("accepted");
    assert_eq!(caller.did, key.did());

    for (status, outcome) in [(401, "unauthorized"), (403, "forbidden"), (400, "rejected"), (500, "rejected")] {
        seen.status.store(status, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(sender.send(&dids).await, outcome, "{status}");
    }
    assert!(PushSender::new(
        PgPool::connect_lazy("postgres://unused@127.0.0.1/unused").expect("lazy"),
        NetClient::loopback_for_tests(),
        key,
        &PrivatePushConfig::default()
    )
    .is_none(), "no service, no sender");
}
