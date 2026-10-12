//! End-to-end private reads (design-v2 §6.3, §7.9, Phase B B-5): records
//! encoded with the real at-rest hooks, ingested through the firehose writer,
//! then read over HTTP through the private router.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use chrono::Utc;
use kryphocron::{
    AtUri, CorrelationKey, Did, Nsid, RecordContentContext, RecordKey, StructuralRepresentation, TargetRepresentation,
    TraceId,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;

use super::handlers::{PrivateState, router};
use super::{KryphocronPrivateTier, PrivateTier, readiness};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::{PrivateReadConfig, PrivateReadFloors, ScopePolicy, SyncConfig};
use crate::firehose::classifier::{NSID_AUDIENCE, NSID_POST_PRIVATE};
use crate::firehose::watermark::Watermarks;
use crate::firehose::writer::{Action, ApplyOptions, RawOp, Writer};
use crate::hydration::testing::{actor, exec, hydrator};
use crate::kryphocron_glue::audit::GlueSinks;
use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
use crate::kryphocron_glue::{PipelineGlue, at_rest_hooks};
use crate::sync::{BudgetRates, HeadProber, HeadSource, HostBudgets};
use crate::test_support::fresh_db;
use crate::xrpc::{AppState, RateLimits};

const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
const REV: &str = "3kaaaaaaaaaa2";
const FLOOR: Duration = Duration::from_millis(400);
const RELAY: &str = "private-relay";

struct NoHeads;

#[async_trait]
impl HeadSource for NoHeads {
    async fn latest_rev(&self, _pds: &str, _did: &str, _timeout: Duration) -> Result<String, String> {
        Err("no network in tests".into())
    }
}

struct Fixture {
    pool: PgPool,
    writer: Writer,
    glue: PipelineGlue,
    app: AppState,
    prober: HeadProber,
}

fn data_dir(test: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hprs-{test}-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

async fn fixture(pool: &PgPool, test: &str) -> Fixture {
    for (did, handle) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test")] {
        actor(pool, did, handle, handle).await;
        sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
            .bind(did)
            .execute(pool)
            .await
            .expect("member");
        sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 1)")
            .bind(did)
            .execute(pool)
            .await
            .expect("scope");
        // Heads fresh and applied (§6.11.3).
        sqlx::query(
            "INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at) VALUES ($1, '3kzzzzzzzzzz2', $2, now())",
        )
        .bind(did)
        .bind(REV)
        .execute(pool)
        .await
        .expect("sync_state");
    }
    let mut sets = ScopeSets::default();
    for d in [ALICE, BOB, CAROL] {
        sets.scope.insert(d.into());
        sets.members.insert(d.into());
    }
    let scope = ScopeState::new(sets.clone(), ScopePolicy::MembersAndOneHop, 10_000);
    let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
    let writer = Writer::new(pool.clone(), scope.clone(), Arc::clone(&private));
    let hooks = at_rest_hooks(data_dir(test)).expect("at-rest hooks");
    let glue = PipelineGlue::new(GlueSinks::log(CorrelationKey::from_bytes([9; 32])), hooks);
    let fetcher = StubFetcher::default();
    for (i, d) in [ALICE, BOB, CAROL].into_iter().enumerate() {
        fetcher.set_key(d, signing_key(u8::try_from(i).expect("small") + 3));
    }
    let (auth, _drainer) = verifier(fetcher, 100, 16);
    let app = AppState {
        hydrator: hydrator(pool, sets),
        auth: Arc::new(auth),
        limits: RateLimits::new(),
        enroller: None,
        trusted_proxy: "172.30.0.10".parse().expect("ip"),
        did_document: Arc::new(json!({})),
    };
    let prober = HeadProber::new(
        pool.clone(),
        scope,
        private,
        Arc::new(NoHeads),
        HostBudgets::new(BudgetRates {
            max_probe_per_host: 25.0,
            head_max_age_secs: 60.0,
            read_fetch_per_host: 10.0,
            backfill_per_host: 5.0,
        }),
        SyncConfig::default(),
    );
    Fixture {
        pool: pool.clone(),
        writer,
        glue,
        app,
        prober,
    }
}

impl Fixture {
    fn state(&self, watermark_age: Option<Duration>) -> PrivateState {
        let watermarks = Watermarks::new(BTreeMap::new(), vec![RELAY.into()]);
        if let Some(age) = watermark_age {
            watermarks.observe(RELAY, Utc::now() - chrono::Duration::from_std(age).expect("age"));
        }
        let cfg = PrivateReadConfig {
            bind_concurrency: 8,
            floor_ms: PrivateReadFloors {
                get_posts: u64::try_from(FLOOR.as_millis()).expect("ms"),
                get_post_thread: u64::try_from(FLOOR.as_millis()).expect("ms"),
                get_author_feed: u64::try_from(FLOOR.as_millis()).expect("ms"),
            },
        };
        PrivateState::new(
            self.app.clone(),
            self.glue.clone(),
            self.prober.clone(),
            watermarks,
            Duration::from_secs(120),
            vec!["*.example.com".into()],
            16,
            &cfg,
        )
    }

    /// Encode `text` with the real hooks, as Aurora-Locus would write it.
    async fn encoded_post(&self, owner: &str, rkey: &str, text: &str, audience: Option<&str>) -> Value {
        let nsid = Nsid::new(NSID_POST_PRIVATE).expect("nsid");
        let did = Did::new(owner).expect("did");
        let ctx = RecordContentContext::new(
            nsid.clone(),
            RecordKey::new(rkey).expect("rkey"),
            did.clone(),
            audience.map(|a| AtUri::new(a).expect("at-uri")),
            did.clone(),
            TargetRepresentation::structural_only(StructuralRepresentation::Resource { did, nsid }),
            TraceId::from_bytes([1; 16]),
            Default::default(),
        );
        let encoded = kryphocron::encode_record_content(
            self.glue.at_rest_hooks(),
            self.glue.user_sink(),
            text.as_bytes(),
            &ctx,
            Instant::now() + Duration::from_secs(2),
            SystemTime::now(),
        )
        .await
        .expect("encode");
        let hex: String = encoded.content.iter().map(|b| format!("{b:02x}")).collect();
        let mut record = json!({
            "$type": NSID_POST_PRIVATE,
            "encodedContent": hex,
            "encodedContentCodec": encoded.codec.as_str(),
            "createdAt": "2026-09-26T12:00:00.000Z",
        });
        if let Some(g) = &encoded.generation {
            record["encodedContentGeneration"] = json!(g.as_str());
        }
        if let Some(a) = audience {
            record["audienceList"] = json!(a);
        }
        record
    }

    /// Ingest one record through the firehose writer.
    async fn ingest(&self, repo: &str, collection: &str, rkey: &str, record: Value) {
        let report = self
            .writer
            .apply(
                repo,
                REV,
                &[RawOp {
                    action: Action::Create,
                    collection: collection.into(),
                    rkey: rkey.into(),
                    cid: Some(format!("bafy{rkey}")),
                    record: Some(record),
                }],
                ApplyOptions {
                    advance_applied_rev: false,
                    dead_letter_readiness: false,
                    attempts: 1,
                    live: false,
                },
            )
            .await
            .expect("apply");
        assert!(report.failed.is_empty(), "{report:?}");
    }

    async fn ready(&self, did: &str) {
        let mut conn = self.pool.acquire().await.expect("conn");
        readiness::mark_owner_ready(&mut conn, did).await.expect("ready");
    }
}

async fn serve(state: PrivateState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>())
            .await
            .expect("serve");
    });
    format!("http://{addr}")
}

/// GET with a token for `did`, returning status, raw body and elapsed time.
async fn get(base: &str, method: &str, query: &str, did: &str, key: u8) -> (u16, String, Duration) {
    let token = mint(&signing_key(key), did, &format!("did:web:{AUDIENCE_HOST}"), Some(method));
    let start = Instant::now();
    let r = reqwest::Client::new()
        .get(format!("{base}/xrpc/{method}?{query}"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("request");
    let status = r.status().as_u16();
    let body = r.text().await.expect("body");
    (status, body, start.elapsed())
}

const THREAD: &str = "app.nearhorizon.private.getPostThread";
const POSTS: &str = "app.nearhorizon.private.getPosts";
const FEED: &str = "app.nearhorizon.private.getAuthorFeed";
const ALICE_KEY: u8 = 3;
const BOB_KEY: u8 = 4;
const CAROL_KEY: u8 = 5;

#[tokio::test]
async fn private_reads_round_trip_and_every_miss_is_uniform() {
    let Some(db) = fresh_db("private_round_trip").await else {
        return;
    };
    let f = fixture(&db.pool, "round-trip").await;
    // Alice's audience lists Bob only; her post is encoded for it.
    let audience = format!("at://{ALICE}/{NSID_AUDIENCE}/3kaudience22");
    f.ingest(ALICE, NSID_AUDIENCE, "3kaudience22", json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [BOB], "createdAt": "2026-09-26T00:00:00Z"}))
        .await;
    let record = f.encoded_post(ALICE, "3kpost222222", "hello bob", Some(&audience)).await;
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kpost222222", record).await;
    // A self-only post whose audience row does not exist.
    let orphan = format!("at://{ALICE}/{NSID_AUDIENCE}/3knoaudience");
    let record = f.encoded_post(ALICE, "3kself222222", "note to self", Some(&orphan)).await;
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kself222222", record).await;
    let post = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kpost222222");
    let own = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kself222222");
    let missing = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere222");

    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    // Owner not ready yet: a miss.
    let (status, body, _) = get(&base, THREAD, &format!("uri={post}"), BOB, BOB_KEY).await;
    assert_eq!(status, 400, "{body}");
    f.ready(ALICE).await;

    // Round trip: the audience member reads the decoded text.
    let (status, body, took) = get(&base, THREAD, &format!("uri={post}"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["thread"]["$type"], "app.nearhorizon.private.defs#privateThreadView");
    assert_eq!(v["thread"]["post"]["uri"], post.as_str());
    assert_eq!(v["thread"]["post"]["record"]["text"], "hello bob");
    assert_eq!(v["thread"]["post"]["record"]["audienceList"], audience.as_str());
    assert_eq!(v["thread"]["post"]["author"]["did"], ALICE);
    assert_eq!(v["thread"]["post"]["labels"], json!([]));
    assert!(took >= FLOOR, "hit took {took:?}, floor {FLOOR:?}");

    // getPosts and getAuthorFeed return the same item.
    let (status, body, _) = get(&base, POSTS, &format!("uris={post}&uris={missing}"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["posts"].as_array().map(Vec::len), Some(1));
    assert_eq!(v["posts"][0]["record"]["text"], "hello bob");
    let (status, body, _) = get(&base, FEED, &format!("actor={ALICE}"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["feed"].as_array().map(Vec::len), Some(1), "{v}");
    assert!(v.get("cursor").is_none());

    // Self-read with no audience row succeeds; Bob cannot read it.
    let (status, body, _) = get(&base, THREAD, &format!("uri={own}"), ALICE, ALICE_KEY).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["thread"]["post"]["record"]["text"], "note to self");

    // A reply ingested with strong refs stores and emits both cids.
    let mut reply = f.encoded_post(ALICE, "3kreply22222", "reply to self", Some(&orphan)).await;
    reply["reply"] = json!({"root": {"uri": own, "cid": "bafyroot"}, "parent": {"uri": own, "cid": "bafyparent"}});
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kreply22222", reply).await;
    let reply_uri = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kreply22222");
    let stored: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT reply_root_cid, reply_parent_cid FROM private_posts WHERE uri = $1")
            .bind(&reply_uri)
            .fetch_one(&f.pool)
            .await
            .expect("row");
    assert_eq!(stored, (Some("bafyroot".into()), Some("bafyparent".into())));
    let (status, body, _) = get(&base, POSTS, &format!("uris={reply_uri}"), ALICE, ALICE_KEY).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(
        v["posts"][0]["record"]["reply"],
        json!({"root": {"uri": own, "cid": "bafyroot"}, "parent": {"uri": own, "cid": "bafyparent"}})
    );

    // Misses are byte-identical to a nonexistent record and still floored.
    let (_, not_found, took) = get(&base, THREAD, &format!("uri={missing}"), BOB, BOB_KEY).await;
    assert!(took >= FLOOR, "miss took {took:?}");
    assert!(not_found.contains("NotFound"), "{not_found}");
    let (status, body, took) = get(&base, THREAD, &format!("uri={own}"), BOB, BOB_KEY).await;
    assert_eq!((status, body.as_str()), (400, not_found.as_str()), "orphan audience");
    assert!(took >= FLOOR, "miss took {took:?}");
    // Non-audience member.
    let (status, body, took) = get(&base, THREAD, &format!("uri={post}"), CAROL, CAROL_KEY).await;
    assert_eq!((status, body.as_str()), (400, not_found.as_str()), "non-audience");
    assert!(took >= FLOOR, "miss took {took:?}");
    let (status, body, _) = get(&base, POSTS, &format!("uris={post}"), CAROL, CAROL_KEY).await;
    assert_eq!((status, body.as_str()), (200, r#"{"posts":[]}"#));
    let (status, body, _) = get(&base, FEED, &format!("actor={ALICE}"), CAROL, CAROL_KEY).await;
    assert_eq!((status, body.as_str()), (200, r#"{"feed":[]}"#));

    // Stale private watermark: a miss for everyone, including the owner.
    let stale = serve(f.state(Some(Duration::from_secs(600)))).await;
    let (status, body, _) = get(&stale, THREAD, &format!("uri={post}"), BOB, BOB_KEY).await;
    assert_eq!((status, body.as_str()), (400, not_found.as_str()), "stale watermark");
    let never = serve(f.state(None)).await;
    let (status, body, _) = get(&never, THREAD, &format!("uri={own}"), ALICE, ALICE_KEY).await;
    assert_eq!((status, body.as_str()), (400, not_found.as_str()), "no watermark");

    // Owner readiness cleared: a miss.
    let mut conn = f.pool.acquire().await.expect("conn");
    readiness::clear_owner(&mut conn, ALICE).await.expect("clear");
    drop(conn);
    let (status, body, _) = get(&base, THREAD, &format!("uri={post}"), BOB, BOB_KEY).await;
    assert_eq!((status, body.as_str()), (400, not_found.as_str()), "owner not ready");
    f.ready(ALICE).await;

    // A stale requester head: a miss.
    exec(&f.pool, "UPDATE sync_state SET head_checked_at = now() - interval '1 hour' WHERE did = 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb'").await;
    let (status, body, _) = get(&base, THREAD, &format!("uri={post}"), BOB, BOB_KEY).await;
    assert_eq!((status, body.as_str()), (400, not_found.as_str()), "stale head");
    exec(&f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
    let (status, _, _) = get(&base, THREAD, &format!("uri={post}"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "readable again");

    // Gate errors are not floored and depend only on the caller.
    exec(&f.pool, "UPDATE members SET ready_at = NULL WHERE did = 'did:plc:cccccccccccccccccccccccc'").await;
    let (status, body, took) = get(&base, THREAD, &format!("uri={post}"), CAROL, CAROL_KEY).await;
    assert_eq!(status, 400);
    assert!(body.contains("NotReady"), "{body}");
    assert!(took < FLOOR, "gate errors are not floored: {took:?}");
    db.drop_db().await;
}

#[tokio::test]
async fn migration_0002_adds_nullable_reply_cids() {
    let Some(db) = crate::test_support::empty_db("migration_0002").await else {
        return;
    };
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().collect();
    sqlx::raw_sql(&migrations[0].sql).execute(&db.pool).await.expect("0001");
    exec(
        &db.pool,
        "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, reply_root_uri, reply_parent_uri, plaintext_text, created_at) \
         VALUES ('at://did:plc:a/tools.kryphocron.feed.postPrivate/3k', 'did:plc:a', '3k', 'bafy', '3kaaaaaaaaaa2', \
                 'at://did:plc:b/app.bsky.feed.post/1', 'at://did:plc:b/app.bsky.feed.post/1', 'x', now())",
    )
    .await;
    assert_eq!(migrations[1].version, 2);
    sqlx::raw_sql(&migrations[1].sql).execute(&db.pool).await.expect("0002 applies over existing rows");
    let cids: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT reply_root_cid, reply_parent_cid FROM private_posts").fetch_one(&db.pool).await.expect("row");
    assert_eq!(cids, (None, None));
    db.drop_db().await;
}

#[tokio::test]
async fn blocks_in_either_direction_are_misses() {
    let Some(db) = fresh_db("private_blocks_miss").await else {
        return;
    };
    let f = fixture(&db.pool, "blocks").await;
    let audience = format!("at://{ALICE}/{NSID_AUDIENCE}/3kaudience22");
    f.ingest(ALICE, NSID_AUDIENCE, "3kaudience22", json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [BOB, CAROL], "createdAt": "2026-09-26T00:00:00Z"}))
        .await;
    let record = f.encoded_post(ALICE, "3kpost222222", "hello both", Some(&audience)).await;
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kpost222222", record).await;
    f.ready(ALICE).await;
    let post = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kpost222222");
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    for (did, key) in [(BOB, BOB_KEY), (CAROL, CAROL_KEY)] {
        let (status, body, _) = get(&base, THREAD, &format!("uri={post}"), did, key).await;
        assert_eq!(status, 200, "{did} before blocks: {body}");
    }
    // The owner blocks Bob; Carol blocks the owner.
    let block = |subject: &str| json!({"$type": "app.bsky.graph.block", "subject": subject, "createdAt": "2026-09-26T00:00:00Z"});
    f.ingest(ALICE, "app.bsky.graph.block", "3kblockbob22", block(BOB)).await;
    f.ingest(CAROL, "app.bsky.graph.block", "3kblockali22", block(ALICE)).await;
    let missing = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere222");
    let (_, not_found, _) = get(&base, THREAD, &format!("uri={missing}"), BOB, BOB_KEY).await;
    for (did, key) in [(BOB, BOB_KEY), (CAROL, CAROL_KEY)] {
        let (status, body, took) = get(&base, THREAD, &format!("uri={post}"), did, key).await;
        assert_eq!((status, body.as_str()), (400, not_found.as_str()), "{did} blocked");
        assert!(took >= FLOOR);
    }
    db.drop_db().await;
}
