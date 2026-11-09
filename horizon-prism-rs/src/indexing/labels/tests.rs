//! Full indexing's labels against a fake labeler: its directory document
//! and audit log, its `queryLabels`, and its `subscribeLabels` stream, every
//! frame signed as a labeler signs it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, RawQuery, State};
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine;
use chrono::{DateTime, Utc};
use proto_blue::crypto::{K256Keypair, Keypair, Signer};
use proto_blue::lex_data::LexValue;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::subscriber::Subscriber;
use super::upstream::Upstream;
use super::{ExternalLabels, Settings};
use crate::aggregations::ScopeSets;
use crate::appview_clients::accept_labelers::{AcceptLabelers, with_requested};
use crate::config::{FullConfig, FullLabels};
use crate::hydration::Hydrator;
use crate::hydration::testing::{exec, hydrator};
use crate::identity::{IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::indexing::{Breadth, PublicMode};
use crate::net::{Intent, NetClient, Transports};
use crate::refract::labels::Labelers;
use crate::test_support::fresh_db;

/// The subscribed labeler.
const LABELER: &str = "did:plc:examplelabelerlaaaaaaaaa";
/// A labeler requests accept that is not subscribed.
const OTHER: &str = "did:plc:examplelabelerobbbbbbbbb";
const AUTHOR: &str = "did:plc:exampleauthoraaaaaaaaaaa";

/// What the fake labeler serves.
#[derive(Default)]
struct World {
    /// Base URL of the fake.
    base: String,
    /// Each labeler's current label key, multibase.
    keys: HashMap<String, String>,
    /// Each labeler's directory audit log.
    audits: HashMap<String, Value>,
    /// The stream: `(seq, labels)`, labels as JSON.
    frames: Vec<(i64, Vec<Value>)>,
    /// `queryLabels` answers, by labeler: every label it holds.
    held: HashMap<String, Vec<Value>>,
    /// The `uriPatterns` of each `queryLabels` call, by labeler.
    queried: Vec<(String, Vec<String>)>,
    /// The `cursor` of each stream connection.
    connections: Vec<Option<i64>>,
    /// Directory reads, by DID.
    doc_reads: HashMap<String, u32>,
}

type Shared = Arc<Mutex<World>>;

async fn doc(State(w): State<Shared>, Path(did): Path<String>) -> axum::response::Response {
    let mut w = w.lock().expect("world");
    *w.doc_reads.entry(did.clone()).or_default() += 1;
    let Some(key) = w.keys.get(&did).cloned() else {
        return (axum::http::StatusCode::NOT_FOUND, "no such DID").into_response();
    };
    axum::Json(json!({
        "id": did,
        "verificationMethod": [{"id": "#atproto_label", "type": "Multikey", "publicKeyMultibase": key}],
        "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler", "serviceEndpoint": w.base}]
    }))
    .into_response()
}

async fn audit(State(w): State<Shared>, Path(did): Path<String>) -> axum::Json<Value> {
    axum::Json(w.lock().expect("world").audits.get(&did).cloned().unwrap_or_else(|| json!([])))
}

async fn query_labels(State(w): State<Shared>, RawQuery(q): RawQuery) -> axum::Json<Value> {
    let mut w = w.lock().expect("world");
    let pairs: Vec<(String, String)> = url::form_urlencoded::parse(q.unwrap_or_default().as_bytes()).map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let patterns: Vec<String> = pairs.iter().filter(|(k, _)| k == "uriPatterns").map(|(_, v)| v.clone()).collect();
    let source = pairs.iter().find(|(k, _)| k == "sources").map(|(_, v)| v.clone()).unwrap_or_default();
    w.queried.push((source.clone(), patterns.clone()));
    let labels: Vec<Value> = w.held.get(&source).cloned().unwrap_or_default().into_iter().filter(|l| patterns.iter().any(|p| l["uri"] == *p)).collect();
    axum::Json(json!({"labels": labels}))
}

async fn subscribe(State(w): State<Shared>, Query(q): Query<HashMap<String, String>>, ws: WebSocketUpgrade) -> impl IntoResponse {
    let cursor: Option<i64> = q.get("cursor").and_then(|c| c.parse().ok());
    ws.on_upgrade(move |socket| stream(w, cursor, socket))
}

/// Send every frame after `cursor` (from the tip when there is none), then
/// each new one as the test adds it.
async fn stream(w: Shared, cursor: Option<i64>, mut socket: WebSocket) {
    let mut sent = {
        let mut w = w.lock().expect("world");
        w.connections.push(cursor);
        cursor.unwrap_or_else(|| w.frames.iter().map(|(s, _)| *s).max().unwrap_or(0))
    };
    loop {
        let next: Vec<(i64, Vec<Value>)> = w.lock().expect("world").frames.iter().filter(|(s, _)| *s > sent).cloned().collect();
        for (seq, labels) in next {
            let mut body = std::collections::BTreeMap::new();
            body.insert("seq".to_string(), LexValue::Integer(seq));
            body.insert("labels".to_string(), LexValue::Array(labels.iter().map(proto_blue::lex_json::json_to_lex).collect()));
            let frame = proto_blue::ws::Frame::Message(proto_blue::ws::MessageFrame { r#type: Some("#labels".into()), body: LexValue::Map(body) });
            if socket.send(Message::Binary(frame.encode().expect("frame").into())).await.is_err() {
                return;
            }
            sent = seq;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct NeverPersist;

impl PersistencePolicy for NeverPersist {
    fn in_persistence_set(&self, _did: &str) -> bool {
        false
    }
}

/// The fake, its keys, and the labels and hydrator over it.
struct Fake {
    world: Shared,
    key: K256Keypair,
    other_key: K256Keypair,
    labels: Arc<ExternalLabels>,
    hydrator: Hydrator,
    pool: PgPool,
    labelers: Labelers,
    base: String,
}

fn multikey(k: &K256Keypair) -> String {
    proto_blue::crypto::format_multikey("ES256K", &k.public_key_compressed())
}

/// A label signed with `kp`.
fn signed(kp: &K256Keypair, label: Value) -> Value {
    let bytes = proto_blue::lex_cbor::encode(&proto_blue::lex_json::json_to_lex(&label)).expect("cbor");
    let sig = kp.sign(&bytes).expect("sign");
    let mut out = label;
    out["sig"] = json!({"$bytes": base64::engine::general_purpose::STANDARD_NO_PAD.encode(sig)});
    out
}

fn label(src: &str, uri: &str, val: &str, cts: DateTime<Utc>, neg: bool) -> Value {
    let mut l = json!({"ver": 1, "src": src, "uri": uri, "val": val, "cts": cts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)});
    if neg {
        l["neg"] = json!(true);
    }
    l
}

/// A post URI whose record key is a TID of `at`.
fn post_at(at: DateTime<Utc>, n: u16) -> String {
    let micros = u64::try_from(at.timestamp_micros()).expect("after the epoch");
    format!("at://{AUTHOR}/app.bsky.feed.post/{}", proto_blue::syntax::Tid::from_timestamp(micros, n).into_inner())
}

impl Fake {
    async fn new(pool: &PgPool, backfill: bool) -> Self {
        let world: Shared = Arc::default();
        let base = crate::test_support::serve_fake(
            axum::Router::new()
                .route("/xrpc/com.atproto.label.queryLabels", get(query_labels))
                .route("/xrpc/com.atproto.label.subscribeLabels", get(subscribe))
                .route("/{did}/log/audit", get(audit))
                .route("/{did}", get(doc))
                .with_state(Arc::clone(&world)),
        )
        .await;
        let (key, other_key) = (K256Keypair::generate(), K256Keypair::generate());
        {
            let mut w = world.lock().expect("world");
            w.base = base.clone();
            w.keys.insert(LABELER.into(), multikey(&key));
            w.keys.insert(OTHER.into(), multikey(&other_key));
        }
        let settings = IdentitySettings { plc_url: base.clone(), fresh: Duration::from_secs(3600), stale_max: Duration::from_secs(86_400) };
        let resolver = IdentityResolver::new(Transports::loopback_for_tests(), pool.clone(), settings, Arc::new(NeverPersist), Intent::Internal);
        let labelers = Labelers::new(resolver);
        let full = FullConfig { label_sources: vec![LABELER.into()], label_backfill: backfill, ..FullConfig::default() };
        let labels = ExternalLabels::new(Settings::from_config(&full), Upstream::new(NetClient::loopback_for_tests(), labelers.clone(), 60));
        let mut sets = ScopeSets::default();
        sets.public.mode = PublicMode::On;
        sets.public.breadth = Breadth::Full;
        sets.full.labels = Some(Arc::clone(&labels));
        let hydrator = hydrator(pool, sets);
        Self { world, key, other_key, labels, hydrator, pool: pool.clone(), labelers, base }
    }

    fn set(&self, f: impl FnOnce(&mut World)) {
        f(&mut self.world.lock().expect("world"));
    }

    fn frame(&self, seq: i64, labels: Vec<Value>) {
        self.set(|w| w.frames.push((seq, labels)));
    }

    fn queried(&self) -> Vec<(String, Vec<String>)> {
        std::mem::take(&mut self.world.lock().expect("world").queried)
    }

    fn start(&self) {
        let sub = Subscriber::new(self.pool.clone(), self.hydrator.scope().clone(), Arc::clone(&self.labels), self.labelers.clone(), NetClient::loopback_for_tests(), &self.base);
        drop(sub.spawn());
    }

    /// The labels a view of `subjects` carries for a request accepting
    /// `accept` (none: no header).
    async fn view(&self, subjects: &[String], accept: Option<&str>) -> HashMap<String, Vec<(String, String)>> {
        let accept = accept.and_then(AcceptLabelers::parse);
        let got = with_requested(accept, self.hydrator.load_labels(subjects)).await.expect("labels");
        got.into_iter()
            .map(|(k, v)| (k, v.iter().map(|l| (l["src"].as_str().unwrap_or_default().to_string(), l["val"].as_str().unwrap_or_default().to_string())).collect()))
            .collect()
    }

    /// Wait until `sql` counts at least `n`.
    async fn until(&self, sql: &str, n: i64) {
        for _ in 0..300 {
            let have: i64 = sqlx::query_scalar(sql).fetch_one(&self.pool).await.expect(sql);
            if have >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {sql} >= {n}");
    }

    async fn connected(&self, n: usize) {
        for _ in 0..300 {
            if self.world.lock().expect("world").connections.len() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the subscriber never connected");
    }
}

fn pair(src: &str, val: &str) -> (String, String) {
    (src.to_string(), val.to_string())
}

/// From the tip: a live label is stored and merged for requests that accept
/// its labeler, and only for those; a labeler a request accepts that is not
/// subscribed is read from its `queryLabels`, once per cache period; a
/// negation removes the label; a label whose `src` is another labeler, and
/// a live label whose signature fails after the key is read again, are
/// dropped; subjects older than the subscription read the labeler too
/// (backfill off); outside breadth Full views carry no third-party label.
#[tokio::test]
async fn labels_from_the_tip_merge_by_accepted_labeler() {
    let Some(db) = fresh_db("labels_from_the_tip").await else {
        return;
    };
    let f = Fake::new(&db.pool, false).await;
    let started = Utc::now();
    f.start();
    f.connected(1).await;
    assert_eq!(f.world.lock().expect("world").connections, [None], "a subscription with backfill off starts at the tip");
    let fresh = post_at(started + chrono::TimeDelta::seconds(5), 1);
    let old = post_at(started - chrono::TimeDelta::days(400), 2);
    let now = Utc::now();
    f.frame(1, vec![signed(&f.key, label(LABELER, &fresh, "spam", now, false))]);
    f.until("SELECT count(*) FROM full_labels", 1).await;
    // A label the labeler issued before the subscription, for an older post.
    f.set(|w| {
        w.held.insert(LABELER.into(), vec![signed(&f.key, label(LABELER, &old, "rude", started - chrono::TimeDelta::days(300), false))]);
        w.held.insert(OTHER.into(), vec![signed(&f.other_key, label(OTHER, &fresh, "porn", now, false))]);
    });
    let both = vec![fresh.clone(), old.clone()];
    let got = f.view(&both, Some(LABELER)).await;
    assert_eq!(got.get(&fresh).cloned().unwrap_or_default(), [pair(LABELER, "spam")]);
    assert_eq!(got.get(&old).cloned().unwrap_or_default(), [pair(LABELER, "rude")], "an older subject reads the labeler");
    assert_eq!(f.queried(), [(LABELER.to_string(), vec![old.clone()])], "only the older subject is asked about");
    // Another labeler the request accepts: read from it, then cached.
    let got = f.view(std::slice::from_ref(&fresh), Some(&format!("{LABELER}, {OTHER}"))).await;
    let mut vals = got.get(&fresh).cloned().unwrap_or_default();
    vals.sort();
    assert_eq!(vals, [pair(LABELER, "spam"), pair(OTHER, "porn")]);
    assert_eq!(f.queried(), [(OTHER.to_string(), vec![fresh.clone()])]);
    let _ = f.view(std::slice::from_ref(&fresh), Some(OTHER)).await;
    assert!(f.queried().is_empty(), "the cache answers within its period");
    // No header, or only another labeler: none of the subscribed labels.
    assert!(f.view(std::slice::from_ref(&fresh), None).await.is_empty());
    // A forged `src` and a bad signature are dropped; the key is read
    // again for the bad signature.
    let reads = f.world.lock().expect("world").doc_reads.get(LABELER).copied().unwrap_or(0);
    f.frame(2, vec![signed(&f.key, label(OTHER, &fresh, "forged", Utc::now(), false)), signed(&f.other_key, label(LABELER, &fresh, "badsig", Utc::now(), false))]);
    f.frame(3, vec![signed(&f.key, label(LABELER, &fresh, "spam", Utc::now() + chrono::TimeDelta::seconds(1), true))]);
    f.until("SELECT count(*) FROM full_labels WHERE neg", 1).await;
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM full_labels WHERE val IN ('forged', 'badsig') OR src <> $1").bind(LABELER).fetch_one(&f.pool).await.expect("count"), 0);
    assert!(f.world.lock().expect("world").doc_reads.get(LABELER).copied().unwrap_or(0) > reads, "the key was read again");
    // The negation removed the label.
    let got = f.view(std::slice::from_ref(&fresh), Some(LABELER)).await;
    assert!(got.get(&fresh).is_none_or(Vec::is_empty), "{got:?}");
    // The cursor is stored with the labels.
    let p = super::store::load_progress(&f.pool).await.expect("progress");
    assert_eq!((p[LABELER].cursor, p[LABELER].replay, p[LABELER].caught_up), (Some(3), false, true));
    // Outside breadth Full nothing is merged and nothing more is stored.
    {
        let mut g = f.hydrator.scope().write_guard().await;
        g.public.breadth = Breadth::Community;
    }
    f.frame(4, vec![signed(&f.key, label(LABELER, &fresh, "late", Utc::now(), false))]);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(f.view(std::slice::from_ref(&fresh), Some(&format!("{LABELER}, {OTHER}"))).await.is_empty());
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM full_labels WHERE val = 'late'").fetch_one(&f.pool).await.expect("count"), 0);
    db.drop_db().await;
}

/// With backfill on, a subscription replays from the first label, and until
/// it reaches the tip the labeler is read for every subject. A replayed
/// label signed with a key the labeler has since rotated out verifies
/// against the key its directory log held at the label's `cts`; one that
/// still fails is stored unverified and never applied, and only its subject
/// reads the labeler.
#[tokio::test]
async fn a_replay_checks_old_keys_and_keeps_unverified_labels_unapplied() {
    let Some(db) = fresh_db("labels_replay").await else {
        return;
    };
    let f = Fake::new(&db.pool, true).await;
    let old_key = K256Keypair::generate();
    let stranger = K256Keypair::generate();
    let t = |s: &str| DateTime::parse_from_rfc3339(s).expect("time").with_timezone(&Utc);
    f.set(|w| {
        w.audits.insert(
            LABELER.into(),
            json!([
                {"createdAt": "2024-01-01T00:00:00Z", "operation": {"type": "plc_operation", "verificationMethods": {"atproto_label": format!("did:key:{}", multikey(&old_key))}}},
                {"createdAt": "2025-01-01T00:00:00Z", "operation": {"type": "plc_operation", "verificationMethods": {"atproto_label": format!("did:key:{}", multikey(&f.key))}}}
            ]),
        );
    });
    let p1 = post_at(t("2024-02-01T00:00:00Z"), 1);
    let p2 = post_at(t("2024-03-01T00:00:00Z"), 2);
    let p3 = post_at(t("2024-04-01T00:00:00Z"), 3);
    // Before the replay reaches the tip, the labeler answers.
    f.set(|w| {
        w.held.insert(LABELER.into(), vec![signed(&f.key, label(LABELER, &p2, "today", Utc::now(), false)), signed(&f.key, label(LABELER, &p3, "asked", Utc::now(), false))]);
    });
    let got = f.view(std::slice::from_ref(&p3), Some(LABELER)).await;
    assert_eq!(got.get(&p3).cloned().unwrap_or_default(), [pair(LABELER, "asked")]);
    assert_eq!(f.queried(), [(LABELER.to_string(), vec![p3.clone()])]);
    f.frame(1, vec![signed(&old_key, label(LABELER, &p1, "issued-before", t("2024-03-01T00:00:00Z"), false))]);
    f.frame(2, vec![signed(&stranger, label(LABELER, &p2, "forgotten", t("2024-04-01T00:00:00Z"), false))]);
    f.frame(3, vec![signed(&f.key, label(LABELER, &p3, "fresh", Utc::now(), false))]);
    f.start();
    f.connected(1).await;
    assert_eq!(f.world.lock().expect("world").connections, [Some(0)], "a replay starts at the first label");
    f.until("SELECT count(*) FROM full_labels", 3).await;
    for _ in 0..100 {
        if f.labels.progress(LABELER).is_some_and(|p| p.caught_up) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(f.labels.progress(LABELER).is_some_and(|p| p.caught_up), "a fresh label ends the replay");
    let verified: Vec<(String, bool)> = sqlx::query_as("SELECT val, verified FROM full_labels ORDER BY val").fetch_all(&f.pool).await.expect("rows");
    assert_eq!(verified, [("forgotten".to_string(), false), ("fresh".to_string(), true), ("issued-before".to_string(), true)]);
    f.queried();
    let got = f.view(&[p1.clone(), p2.clone(), p3.clone()], Some(LABELER)).await;
    assert_eq!(got.get(&p1).cloned().unwrap_or_default(), [pair(LABELER, "issued-before")], "a label issued before the switch, from the replay");
    assert_eq!(got.get(&p3).cloned().unwrap_or_default(), [pair(LABELER, "fresh")], "caught up, the stored labels answer");
    assert_eq!(got.get(&p2).cloned().unwrap_or_default(), [pair(LABELER, "today")], "the unverified label is not applied; the labeler's answer is");
    assert_eq!(f.queried(), [(LABELER.to_string(), vec![p2.clone()])], "only the subject with an unverified label reads the labeler");
    // Compared with the labeler's answer, the unverified label it no longer
    // serves is settled: the subject stops counting, and reads stop asking.
    assert!(super::store::unverified_counts(&f.pool).await.expect("counts").is_empty());
    let rows: Vec<String> = sqlx::query_scalar("SELECT val FROM full_labels ORDER BY val").fetch_all(&f.pool).await.expect("rows");
    assert_eq!(rows, ["fresh", "issued-before"]);
    f.view(std::slice::from_ref(&p2), Some(LABELER)).await;
    assert!(f.queried().is_empty(), "nothing unverified is left to ask about");
    db.drop_db().await;
}

/// `subscribe` with no source is `upstream`; under `upstream` nothing is
/// subscribed.
#[test]
fn subscribe_with_no_source_is_upstream() {
    let none = Settings::from_config(&FullConfig { label_sources: Vec::new(), ..FullConfig::default() });
    assert_eq!((none.mode, none.sources.len()), (FullLabels::Upstream, 0));
    let up = Settings::from_config(&FullConfig { labels: FullLabels::Upstream, ..FullConfig::default() });
    assert!(!up.subscribed(crate::config::BLUESKY_MODERATION_LABELER));
    let sub = Settings::from_config(&FullConfig::default());
    assert!(sub.subscribed(crate::config::BLUESKY_MODERATION_LABELER));
    assert_eq!(super::subscriber::stream_url("https://mod.example/", Some(7)).as_deref(), Some("wss://mod.example/xrpc/com.atproto.label.subscribeLabels?cursor=7"));
    assert_eq!(super::subscriber::stream_url("https://mod.example", None).as_deref(), Some("wss://mod.example/xrpc/com.atproto.label.subscribeLabels"));
}

/// The newest `cts` per key wins, within a batch and against a stored row;
/// a private-tier subject is never named to a labeler.
#[tokio::test]
async fn the_newest_label_per_key_wins() {
    let Some(db) = fresh_db("labels_newest").await else {
        return;
    };
    let p = &db.pool;
    let at = |s: i64| DateTime::from_timestamp(1_800_000_000 + s, 0).expect("time");
    let row = |val: &str, cts: i64, neg: bool| super::store::Row { src: LABELER.into(), uri: AUTHOR.into(), cid: String::new(), val: val.into(), neg, cts: at(cts), exp: None, sig: None, verified: true };
    let mut conn = p.acquire().await.expect("conn");
    super::store::upsert(&mut conn, vec![row("spam", 10, false), row("spam", 5, true), row("rude", 1, false)]).await.expect("upsert");
    super::store::upsert(&mut conn, vec![row("spam", 3, true), row("rude", 2, true)]).await.expect("upsert");
    let rows: Vec<(String, bool)> = sqlx::query_as("SELECT val, neg FROM full_labels ORDER BY val").fetch_all(p).await.expect("rows");
    assert_eq!(rows, [("rude".to_string(), true), ("spam".to_string(), false)]);
    exec(p, "UPDATE full_labels SET exp = now() - interval '1 second' WHERE val = 'spam'").await;
    assert!(super::store::applied(p, &[AUTHOR.to_string()], &[LABELER.to_string()]).await.expect("applied").is_empty(), "an expired label is not applied");
    assert!(!super::asked_about(&format!("at://{AUTHOR}/tools.kryphocron.feed.postPrivate/3k")));
    assert!(super::asked_about(&format!("at://{AUTHOR}/app.bsky.feed.post/3k")) && super::asked_about(AUTHOR));
    db.drop_db().await;
}

/// A label naming a record version applies to that version only, and one
/// naming a blob of the record applies to the record; leaving
/// full (a narrowing, a drain, a trial's exit) removes every label.
#[tokio::test]
async fn versions_and_the_narrowing() {
    let Some(db) = fresh_db("labels_narrowing").await else {
        return;
    };
    let p = &db.pool;
    let f = Fake::new(p, true).await;
    const STRANGER: &str = "did:plc:examplestrangeraaaaaaaa";
    crate::hydration::testing::actor(p, AUTHOR, "author.example.com", "Author").await;
    let post = crate::hydration::testing::post(p, AUTHOR, "3kpostaaaaaa2", "hi", None, None).await;
    let gone = format!("at://{STRANGER}/app.bsky.feed.post/3kpostbbbbbb2");
    let list = format!("at://{AUTHOR}/app.bsky.graph.list/3klistaaaaaa2");
    let at = Utc::now() - chrono::TimeDelta::minutes(1);
    let row = |uri: &str, cid: &str, val: &str| super::store::Row { src: LABELER.into(), uri: uri.into(), cid: cid.into(), val: val.into(), neg: false, cts: at, exp: None, sig: None, verified: true };
    // Another version of the record, and a blob of it.
    let older = proto_blue::lex_data::Cid::for_cbor(b"an older version").to_string_base32();
    let blob = proto_blue::lex_data::Cid::for_raw(b"a video").to_string_base32();
    let mut conn = p.acquire().await.expect("conn");
    super::store::upsert(
        &mut conn,
        vec![
            row(&post, "", "a"),
            row(&post, "bafy3kpostaaaaaa2", "b"),
            row(&post, &older, "c"),
            row(AUTHOR, "", "d"),
            row(&gone, "", "e"),
            row(STRANGER, "", "f"),
            row(&list, "", "g"),
            row(&post, &blob, "h"),
        ],
    )
    .await
    .expect("upsert");
    drop(conn);
    f.labels.set_progress(LABELER, super::Progress { cursor: Some(1), replay: true, tip_started_at: None, caught_up: true });
    let got = f.labels.for_subjects(p, std::slice::from_ref(&post), &[LABELER.to_string()]).await.expect("labels");
    let mut vals: Vec<&str> = got[&post].iter().map(|l| l["val"].as_str().unwrap_or_default()).collect();
    vals.sort_unstable();
    assert_eq!(vals, ["a", "b", "h"], "a label of another version does not apply; one naming a blob of the record does");
    assert!(f.queried().is_empty());
    let run = |list: crate::indexing::public_mode::SweepList, exit: crate::indexing::narrowing::LabelsExit| async move {
        let step = crate::indexing::narrowing::steps(list, Vec::new(), exit).into_iter().find(|s| s.table == "full_labels").expect("step");
        loop {
            if sqlx::query(&step.sql).execute(p).await.expect(&step.sql).rows_affected() == 0 {
                break;
            }
        }
    };
    let count = || async { sqlx::query_scalar::<_, i64>("SELECT count(*) FROM full_labels").fetch_one(p).await.expect("count") };
    let mut conn = p.acquire().await.expect("conn");
    let all = vec![row(&post, "", "a"), row(AUTHOR, "", "d"), row(&gone, "", "e"), row(STRANGER, "", "f")];
    // Leaving full empties the table whole, stored subjects or not.
    for list in [crate::indexing::public_mode::SweepList::Narrowing, crate::indexing::public_mode::SweepList::Full] {
        super::store::upsert(&mut conn, all.clone()).await.expect("upsert");
        assert!(count().await > 0);
        run(list, crate::indexing::narrowing::LabelsExit::Delete).await;
        assert_eq!(count().await, 0, "{list:?}");
    }
    // A trial's exit, row by row and truncated.
    for exit in [crate::indexing::narrowing::LabelsExit::Delete, crate::indexing::narrowing::LabelsExit::Truncate] {
        super::store::upsert(&mut conn, all.clone()).await.expect("upsert");
        run(crate::indexing::public_mode::SweepList::TrialNarrowing, exit).await;
        assert_eq!(count().await, 0, "{exit:?}");
    }
    drop(conn);
    db.drop_db().await;
}

/// An upstream view's labels are kept for the view's own version: a label
/// naming another record version goes, one naming a blob stays.
#[test]
fn a_view_keeps_the_labels_of_its_version() {
    let this = proto_blue::lex_data::Cid::for_cbor(b"this version").to_string_base32();
    let other = proto_blue::lex_data::Cid::for_cbor(b"another version").to_string_base32();
    let blob = proto_blue::lex_data::Cid::for_raw(b"an image").to_string_base32();
    let l = |val: &str, cid: Option<&str>| {
        let mut v = json!({"src": LABELER, "uri": "at://x", "val": val});
        if let Some(c) = cid {
            v["cid"] = json!(c);
        }
        v
    };
    let labels = [l("plain", None), l("this", Some(&this)), l("other", Some(&other)), l("blob", Some(&blob))];
    let kept: Vec<String> = crate::hydration::normalize::for_version(&labels, Some(&this)).iter().map(|v| v["val"].as_str().unwrap_or_default().to_string()).collect();
    assert_eq!(kept, ["plain", "this", "blob"]);
}
