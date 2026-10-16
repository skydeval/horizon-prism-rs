//! End-to-end private reads (design-v2 §6.3, §7.9, Phase B B-5): records
//! encoded with the real at-rest hooks, ingested through the firehose writer,
//! then read over HTTP through the private router.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use chrono::Utc;
use kryphocron::oracle::{AudienceOracle, BlockOracle};
use kryphocron::{
    AtUri, AudienceState, BlockState, CorrelationKey, Did, Nsid, RecordContentContext, RecordKey, ResourceId,
    StructuralRepresentation, TargetRepresentation, TraceId,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;

use super::handlers::{PrivateState, router};
use super::oracle::{OracleSnapshot, SnapshotInputs};
use super::{KryphocronPrivateTier, PrivateTier, readiness, schema};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::{PrivateReadConfig, PrivateReadFloors, ScopePolicy, SyncConfig};
use crate::firehose::classifier::{NSID_AUDIENCE, NSID_POST_PRIVATE};
use crate::firehose::watermark::Watermarks;
use crate::firehose::writer::{Action, ApplyOptions, RawOp, WriteSource, Writer};
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
const DAVE: &str = "did:plc:dddddddddddddddddddddddd";
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
    for (did, handle) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test"), (DAVE, "dave.test")] {
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
    for d in [ALICE, BOB, CAROL, DAVE] {
        sets.scope.insert(d.into());
        sets.members.insert(d.into());
    }
    let scope = ScopeState::new(sets.clone(), ScopePolicy::MembersAndOneHop, 10_000);
    let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
    let writer = Writer::new(pool.clone(), scope.clone(), Arc::clone(&private));
    let hooks = at_rest_hooks(data_dir(test)).expect("at-rest hooks");
    let glue = PipelineGlue::new(GlueSinks::log(CorrelationKey::from_bytes([9; 32])), hooks);
    let fetcher = StubFetcher::default();
    for (i, d) in [ALICE, BOB, CAROL, DAVE].into_iter().enumerate() {
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
        membership: Default::default(),
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
        self.state_floored(watermark_age, FLOOR)
    }

    fn state_floored(&self, watermark_age: Option<Duration>, floor: Duration) -> PrivateState {
        let watermarks = Watermarks::new(BTreeMap::new(), vec![RELAY.into()]);
        if let Some(age) = watermark_age {
            watermarks.observe(RELAY, Utc::now() - chrono::Duration::from_std(age).expect("age"));
        }
        let ms = u64::try_from(floor.as_millis()).expect("ms");
        let cfg = PrivateReadConfig {
            bind_concurrency: 8,
            membership_retry_after_secs: 5,
            floor_ms: PrivateReadFloors {
                get_posts: ms,
                get_post_thread: ms,
                get_author_feed: ms,
                get_timeline: ms,
            },
        };
        PrivateState::new(
            self.app.clone(),
            self.glue.clone(),
            self.prober.clone(),
            watermarks,
            Duration::from_secs(120),
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
        self.apply_op(repo, Action::Create, collection, rkey, REV, Some(record)).await;
    }

    /// Apply one op through the firehose writer at `rev`.
    async fn apply_op(&self, repo: &str, action: Action, collection: &str, rkey: &str, rev: &str, record: Option<Value>) {
        let cid = record.as_ref().map(|_| if rev == REV { format!("bafy{rkey}") } else { format!("bafy{rkey}{rev}") });
        let report = self
            .writer
            .apply(
                repo,
                rev,
                &[RawOp {
                    action,
                    collection: collection.into(),
                    rkey: rkey.into(),
                    cid,
                    record,
                }],
                ApplyOptions {
                    advance_applied_rev: false,
                    dead_letter_readiness: false,
                    attempts: 1,
                    live: false,
                    subscription_rules: false,
                    source: WriteSource::Firehose,
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

    async fn ready_all(&self) {
        for did in [ALICE, BOB, CAROL, DAVE] {
            self.ready(did).await;
        }
    }

    /// An audience record by `owner`; `mode` absent when `None`.
    async fn audience(&self, owner: &str, rkey: &str, mode: Option<&str>, members: &[&str]) -> String {
        let mut record = json!({"$type": NSID_AUDIENCE, "members": members, "createdAt": "2026-09-26T00:00:00Z"});
        if let Some(m) = mode {
            record["mode"] = json!(m);
        }
        self.ingest(owner, NSID_AUDIENCE, rkey, record).await;
        format!("at://{owner}/{NSID_AUDIENCE}/{rkey}")
    }

    /// A private post by `author` under `audience` whose text is its rkey,
    /// replying to `(root, parent)` when given.
    async fn private_post(&self, author: &str, rkey: &str, audience: &str, reply: Option<(&str, &str)>) -> String {
        let record = self.post_record(author, rkey, audience, reply).await;
        self.ingest(author, NSID_POST_PRIVATE, rkey, record).await;
        format!("at://{author}/{NSID_POST_PRIVATE}/{rkey}")
    }

    async fn post_record(&self, author: &str, rkey: &str, audience: &str, reply: Option<(&str, &str)>) -> Value {
        let mut record = self.encoded_post(author, rkey, rkey, Some(audience)).await;
        if let Some((root, parent)) = reply {
            record["reply"] = json!({"root": {"uri": root, "cid": "bafyroot"}, "parent": {"uri": parent, "cid": "bafyparent"}});
        }
        record
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

const TIMELINE: &str = "app.nearhorizon.private.getTimeline";

/// Horizon R-2 (design-v3 §1.7 item 12): the private home timeline holds
/// the posts the caller may read, by the caller and by the accounts it
/// follows; a post it may not read leaves no trace; a mute applies; and
/// paging returns each post once though every post shares one `createdAt`.
#[tokio::test]
async fn the_private_timeline_holds_readable_posts_of_followed_authors() {
    let Some(db) = fresh_db("private_timeline").await else {
        return;
    };
    let f = fixture(&db.pool, "timeline").await;
    let audience = |rkey: &str| format!("at://{ALICE}/{NSID_AUDIENCE}/{rkey}");
    for (rkey, members) in [("3kaudboth222", json!([BOB, CAROL])), ("3kaudbob2222", json!([BOB]))] {
        f.ingest(ALICE, NSID_AUDIENCE, rkey, json!({"$type": NSID_AUDIENCE, "mode": "list", "members": members, "createdAt": "2026-09-26T00:00:00Z"}))
            .await;
    }
    for (rkey, text, aud) in [("3kpostboth22", "for both", "3kaudboth222"), ("3kpostbob222", "for bob alone", "3kaudbob2222")] {
        let record = f.encoded_post(ALICE, rkey, text, Some(&audience(aud))).await;
        f.ingest(ALICE, NSID_POST_PRIVATE, rkey, record).await;
    }
    // Bob's own post, to an audience that has no row: his alone.
    let orphan = format!("at://{BOB}/{NSID_AUDIENCE}/3knoaudience");
    let record = f.encoded_post(BOB, "3kbobsown222", "bob's note", Some(&orphan)).await;
    f.ingest(BOB, NSID_POST_PRIVATE, "3kbobsown222", record).await;
    f.ready(ALICE).await;
    f.ready(BOB).await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let follow = |from: &str, to: &str| {
        format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{from}/app.bsky.graph.follow/1', '{from}', '{to}', '3k', now())")
    };
    let texts = |body: &str| {
        let v: Value = serde_json::from_str(body).expect("json");
        let mut texts: Vec<String> = v["feed"].as_array().expect("feed").iter().map(|p| p["record"]["text"].as_str().expect("text").to_string()).collect();
        texts.sort();
        texts
    };

    // A member of the audience that does not follow the author: nothing,
    // in the time a page with posts takes.
    let (status, body, took) = get(&base, TIMELINE, "", CAROL, CAROL_KEY).await;
    assert_eq!((status, body.as_str()), (200, r#"{"feed":[]}"#));
    assert!(took >= FLOOR, "an empty page took {took:?}");
    // Following the author: the post Carol may read, and no trace of the
    // one she may not.
    exec(&f.pool, &follow(CAROL, ALICE)).await;
    let (status, body, _) = get(&base, TIMELINE, "", CAROL, CAROL_KEY).await;
    assert_eq!((status, texts(&body)), (200, vec!["for both".to_string()]));
    assert!(!body.contains("3kpostbob222") && !body.contains("cursor"), "{body}");

    // The author's own timeline holds its posts, followed or not.
    let (_, body, _) = get(&base, TIMELINE, "", ALICE, ALICE_KEY).await;
    assert_eq!(texts(&body), ["for bob alone", "for both"]);

    // Bob follows Alice: her two posts and his own. All three share one
    // `createdAt`; one at a time, each comes exactly once.
    exec(&f.pool, &follow(BOB, ALICE)).await;
    let mut seen: Vec<String> = Vec::new();
    let mut query = "limit=1".to_string();
    for _ in 0..5 {
        let (status, body, _) = get(&base, TIMELINE, &query, BOB, BOB_KEY).await;
        assert_eq!(status, 200, "{body}");
        let v: Value = serde_json::from_str(&body).expect("json");
        seen.extend(v["feed"].as_array().expect("feed").iter().map(|p| p["record"]["text"].as_str().expect("text").to_string()));
        match v["cursor"].as_str() {
            Some(cursor) => query = format!("limit=1&cursor={cursor}"),
            None => break,
        }
    }
    seen.sort();
    assert_eq!(seen, ["bob's note", "for bob alone", "for both"]);

    // A mute hides the author's private posts as it hides the public ones.
    exec(&f.pool, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{BOB}', '{ALICE}')")).await;
    let (_, body, _) = get(&base, TIMELINE, "", BOB, BOB_KEY).await;
    assert_eq!(texts(&body), ["bob's note"]);

    // No credentials: 401.
    let anonymous = reqwest::get(format!("{base}/xrpc/{TIMELINE}")).await.expect("request");
    assert_eq!(anonymous.status().as_u16(), 401);
    db.drop_db().await;
}

/// docs/internal/design/private-audience-modes.md §2, §5, §10: `followers`
/// admits by the reader's own follow, `everyone` admits every ready
/// member, and `nobody`, `following` and an unknown mode admit only the
/// author; a block keeps a reader out; and under another account's
/// audience that account reads, and its blocks count.
#[tokio::test]
async fn audience_modes_admit_by_follow_and_respect_blocks() {
    let Some(db) = fresh_db("private_modes").await else {
        return;
    };
    let f = fixture(&db.pool, "modes").await;
    let audience = |rkey: &str| format!("at://{ALICE}/{NSID_AUDIENCE}/{rkey}");
    for (i, mode) in ["followers", "everyone", "nobody", "following", "Followers"].into_iter().enumerate() {
        let (aud, post) = (format!("3kaudmode22{i}"), format!("3kpostmode2{i}"));
        // Bob is listed in each: `members` counts for `list` alone.
        f.ingest(ALICE, NSID_AUDIENCE, &aud, json!({"$type": NSID_AUDIENCE, "mode": mode, "members": [BOB], "createdAt": "2026-09-26T00:00:00Z"}))
            .await;
        let record = f.encoded_post(ALICE, &post, mode, Some(&audience(&aud))).await;
        f.ingest(ALICE, NSID_POST_PRIVATE, &post, record).await;
    }
    f.ready(ALICE).await;
    f.ready(BOB).await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let texts = |body: &str, key: &str| {
        let v: Value = serde_json::from_str(body).expect("json");
        let mut texts: Vec<String> = v[key].as_array().expect(key).iter().map(|p| p["record"]["text"].as_str().expect("text").to_string()).collect();
        texts.sort();
        texts
    };
    let feed = |did: &'static str, key: u8| {
        let base = base.clone();
        async move { get(&base, FEED, &format!("actor={ALICE}"), did, key).await.1 }
    };
    let follow = |from: &str, to: &str| {
        format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{from}/app.bsky.graph.follow/1', '{from}', '{to}', '3k', now())")
    };
    let block = |from: &str, to: &str| {
        format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{from}/app.bsky.graph.block/1', '{from}', '{to}', '3k', now())")
    };

    // The author reads all five. A member that does not follow reads
    // `everyone` alone, though `members` names it.
    assert_eq!(texts(&feed(ALICE, ALICE_KEY).await, "feed"), ["Followers", "everyone", "followers", "following", "nobody"]);
    assert_eq!(texts(&feed(BOB, BOB_KEY).await, "feed"), ["everyone"]);
    // Its timeline is of followed authors, so it holds nothing yet.
    let (_, body, _) = get(&base, TIMELINE, "", BOB, BOB_KEY).await;
    assert_eq!(body, r#"{"feed":[]}"#);

    // Following: `followers` joins, from every endpoint.
    exec(&f.pool, &follow(BOB, ALICE)).await;
    assert_eq!(texts(&feed(BOB, BOB_KEY).await, "feed"), ["everyone", "followers"]);
    let (_, body, _) = get(&base, TIMELINE, "", BOB, BOB_KEY).await;
    assert_eq!(texts(&body, "feed"), ["everyone", "followers"]);
    let followers_post = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kpostmode20");
    let (_, body, _) = get(&base, POSTS, &format!("uris={followers_post}"), BOB, BOB_KEY).await;
    assert_eq!(texts(&body, "posts"), ["followers"]);
    // A miss for it is the miss of a post that does not exist.
    let (_, miss, _) = get(&base, POSTS, &format!("uris={followers_post}"), CAROL, CAROL_KEY).await;
    assert_eq!(miss, r#"{"posts":[]}"#);

    // Unfollowing ends it.
    exec(&f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}'")).await;
    assert_eq!(texts(&feed(BOB, BOB_KEY).await, "feed"), ["everyone"]);
    exec(&f.pool, &follow(BOB, ALICE)).await;

    // A block either way keeps the follower out of everything.
    exec(&f.pool, &block(ALICE, BOB)).await;
    assert_eq!(feed(BOB, BOB_KEY).await, r#"{"feed":[]}"#);
    exec(&f.pool, "DELETE FROM blocks").await;
    exec(&f.pool, &block(BOB, ALICE)).await;
    assert_eq!(feed(BOB, BOB_KEY).await, r#"{"feed":[]}"#);
    exec(&f.pool, "DELETE FROM blocks").await;

    // Bob replies, in his own repository, under Alice's `followers`
    // audience. Alice reads it as the audience's owner; Carol reads it once
    // she follows Alice, and not when Alice blocks her.
    let mut reply = f.encoded_post(BOB, "3kbobreply22", "bob replies", Some(&audience("3kaudmode220"))).await;
    reply["reply"] = json!({"root": {"uri": followers_post, "cid": "bafyroot"}, "parent": {"uri": followers_post, "cid": "bafyparent"}});
    f.ingest(BOB, NSID_POST_PRIVATE, "3kbobreply22", reply).await;
    // Bob became a private owner with this record.
    f.ready(BOB).await;
    let reply_uri = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kbobreply22");
    let read = |did: &'static str, key: u8| {
        let (base, query) = (base.clone(), format!("uris={reply_uri}"));
        async move { get(&base, POSTS, &query, did, key).await.1 }
    };
    assert_eq!(texts(&read(ALICE, ALICE_KEY).await, "posts"), ["bob replies"]);
    assert_eq!(read(CAROL, CAROL_KEY).await, r#"{"posts":[]}"#);
    exec(&f.pool, &follow(CAROL, ALICE)).await;
    assert_eq!(texts(&read(CAROL, CAROL_KEY).await, "posts"), ["bob replies"]);
    exec(&f.pool, &block(ALICE, CAROL)).await;
    assert_eq!(read(CAROL, CAROL_KEY).await, r#"{"posts":[]}"#);
    exec(&f.pool, "DELETE FROM blocks").await;
    // Alice blocks the replier: his reply is his alone.
    exec(&f.pool, &block(ALICE, BOB)).await;
    assert_eq!(read(CAROL, CAROL_KEY).await, r#"{"posts":[]}"#);
    assert_eq!(texts(&read(BOB, BOB_KEY).await, "posts"), ["bob replies"]);
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

// Rule 4's anchored walk (docs/internal/design/private-posting.md §3.7,
// §7.1 T-23, T-32, T-33, T-37 to T-39; slice S0, chainlink #718).

const DAVE_KEY: u8 = 6;
/// A DID with no repository here: what a forged parent names (T-39).
const NOBODY: &str = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";
/// A rev above every fixture record's [`REV`].
const REV2: &str = "3kaaaaaaaaab2";

fn key_of(did: &str) -> u8 {
    match did {
        ALICE => ALICE_KEY,
        BOB => BOB_KEY,
        CAROL => CAROL_KEY,
        _ => DAVE_KEY,
    }
}

fn follow_sql(from: &str, to: &str) -> String {
    format!(
        "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) \
         VALUES ('at://{from}/app.bsky.graph.follow/{}', '{from}', '{to}', '3k', now())",
        &to[8..]
    )
}

fn block_sql(from: &str, to: &str) -> String {
    format!(
        "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) \
         VALUES ('at://{from}/app.bsky.graph.block/{}', '{from}', '{to}', '3k', now())",
        &to[8..]
    )
}

/// The oracle's answer alone: the audience rule and `bind`'s block test.
async fn oracle_admits(pool: &PgPool, reader: &str, uri: &str) -> bool {
    let mut parts = uri.strip_prefix("at://").expect("at-uri").splitn(3, '/');
    let (Some(owner), Some(nsid), Some(rkey)) = (parts.next(), parts.next(), parts.next()) else {
        panic!("not an at-uri: {uri}");
    };
    let resource = ResourceId::new(
        Did::new(owner).expect("did"),
        Nsid::new(nsid).expect("nsid"),
        RecordKey::new(rkey).expect("rkey"),
    );
    let snap = OracleSnapshot::load(
        pool,
        SnapshotInputs {
            requester: reader,
            resource: &resource,
            uri,
            watermark: Some(SystemTime::now()),
            bound: Duration::from_secs(120),
        },
    )
    .await
    .expect("snapshot");
    let viewer = Did::new(reader).expect("did");
    matches!(snap.audience_state(&viewer, &resource), AudienceState::InAudience)
        && matches!(snap.block_state(&viewer, resource.did()), BlockState::None)
}

/// Where the writers' repositories are, against the audience owner's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// Every account on one community PDS: the layout today's same-PDS
    /// branch served whatever the post replied to.
    SamePds,
    /// Every account but Alice, the audience owner, on another PDS.
    OffPds,
}

/// One rule-4 fixture: Alice's audience and her root post under it, with
/// Bob, Carol and Dave following her. Each check records a mismatch rather
/// than stopping, so one run shows every case that fails.
struct Walk {
    db: crate::test_support::TestDb,
    f: Fixture,
    base: String,
    aud: String,
    root: String,
    failures: Vec<String>,
}

impl Walk {
    async fn new(test: &str, layout: Layout, mode: Option<&str>, members: &[&str]) -> Option<Self> {
        let db = fresh_db(test).await?;
        let f = fixture(&db.pool, test).await;
        if layout == Layout::OffPds {
            exec(&f.pool, &format!("UPDATE actors SET pds_endpoint = 'https://pds.elsewhere.net' WHERE did <> '{ALICE}'")).await;
        }
        let aud = f.audience(ALICE, "3kaudmain222", mode, members).await;
        let root = f.private_post(ALICE, "3kroot222222", &aud, None).await;
        for did in [BOB, CAROL, DAVE] {
            exec(&f.pool, &follow_sql(did, ALICE)).await;
        }
        let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
        Some(Self {
            db,
            f,
            base,
            aud,
            root,
            failures: Vec::new(),
        })
    }

    /// `author`'s reply to `parent` under Alice's audience, claiming her root.
    async fn reply(&self, author: &str, rkey: &str, parent: &str) -> String {
        self.f.private_post(author, rkey, &self.aud, Some((&self.root, parent))).await
    }

    /// Whether the oracle, the prefilter and `getPosts` each give `reader`
    /// the `expected` answer for `uri` (T-37: one answer from all three).
    async fn expect(&mut self, case: &str, reader: &str, uri: &str, expected: bool) {
        let oracle = oracle_admits(&self.f.pool, reader, uri).await;
        let prefilter = !schema::prefilter_uris(&self.f.pool, reader, &[uri.to_string()])
            .await
            .expect("prefilter")
            .is_empty();
        let (status, body, _) = get(&self.base, POSTS, &format!("uris={uri}"), reader, key_of(reader)).await;
        assert_eq!(status, 200, "{body}");
        let served = body != r#"{"posts":[]}"#;
        if (oracle, prefilter, served) != (expected, expected, expected) {
            self.failures.push(format!(
                "{case}: {reader} on {uri}: expected {expected}, oracle {oracle}, prefilter {prefilter}, getPosts {served}"
            ));
        }
    }

    async fn finish(self) {
        let failures = self.failures;
        self.db.drop_db().await;
        assert!(failures.is_empty(), "{} failing cases:\n{}", failures.len(), failures.join("\n"));
    }
}

/// T-32, T-38 and T-39: a cross-owner post that does not hang from a post by
/// the audience's owner is read by its author only, on any PDS.
async fn unanchored_cross_owner_posts(test: &str, layout: Layout) {
    let Some(mut w) = Walk::new(test, layout, Some("followers"), &[]).await else {
        return;
    };
    let (aud, root) = (w.aud.clone(), w.root.clone());
    // T-32: Bob's top-level post naming Alice's audience.
    let top = w.f.private_post(BOB, "3ktop2222222", &aud, None).await;
    // T-38: Bob's reply to that post, and the same reply in Alice's thread.
    let own_thread = w.f.private_post(BOB, "3kownreply22", &aud, Some((&top, &top))).await;
    let in_thread = w.reply(BOB, "3kinthread22", &root).await;
    // T-39: a parent that names nothing (what aurora's participatePrivate
    // accepts for a non-local parent), and Bob's reply to that post.
    let ghost = format!("at://{NOBODY}/{NSID_POST_PRIVATE}/x");
    let forged = w.f.private_post(BOB, "3kforged2222", &aud, Some((&ghost, &ghost))).await;
    let below = w.f.private_post(BOB, "3kbelow22222", &aud, Some((&ghost, &forged))).await;
    w.f.ready_all().await;
    for (case, uri) in [("T-32", &top), ("T-38", &own_thread), ("T-39", &forged), ("T-39 reply", &below)] {
        w.expect(case, BOB, uri, true).await;
        for reader in [ALICE, CAROL, DAVE] {
            w.expect(case, reader, uri, false).await;
        }
    }
    for reader in [ALICE, BOB, CAROL, DAVE] {
        w.expect("T-38 in Alice's thread", reader, &in_thread, true).await;
    }
    w.finish().await;
}

#[tokio::test]
async fn rule4_unanchored_posts_same_pds() {
    unanchored_cross_owner_posts("rule4-unanchored-same", Layout::SamePds).await;
}

#[tokio::test]
async fn rule4_unanchored_posts_off_pds() {
    unanchored_cross_owner_posts("rule4-unanchored-off", Layout::OffPds).await;
}

/// T-33: a parent under another audience, the cap of 8, a cycle of forged
/// parents, and a reply left under the old audience when the root moved
/// (A-9).
async fn walk_limits(test: &str, layout: Layout) {
    let Some(mut w) = Walk::new(test, layout, Some("followers"), &[]).await else {
        return;
    };
    let (aud, root) = (w.aud.clone(), w.root.clone());
    let other = w.f.audience(ALICE, "3kaudother22", Some("followers"), &[]).await;
    let elsewhere = w.f.private_post(ALICE, "3kelsewhere2", &other, None).await;
    let cross = w.f.private_post(BOB, "3kcross22222", &aud, Some((&elsewhere, &elsewhere))).await;
    // Nine levels of others' replies below Alice's root.
    let mut chain = Vec::new();
    let mut parent = root.clone();
    for level in 1..=9 {
        let author = if level % 2 == 1 { BOB } else { CAROL };
        let uri = w.reply(author, &format!("3klevel{level}2222"), &parent).await;
        chain.push((author, uri.clone()));
        parent = uri;
    }
    // Two posts naming each other as parent, and a reply into the cycle.
    let cycle_a = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kcyclea2222");
    let cycle_b = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3kcycleb2222");
    w.f.private_post(BOB, "3kcyclea2222", &aud, Some((&cycle_b, &cycle_b))).await;
    w.f.private_post(CAROL, "3kcycleb2222", &aud, Some((&cycle_a, &cycle_a))).await;
    let into_cycle = w.f.private_post(CAROL, "3kintocycle2", &aud, Some((&cycle_a, &cycle_a))).await;
    // Replies to the root before it moves: Bob's, and Alice's own.
    let left = w.reply(BOB, "3kleftbehind", &root).await;
    let own = w.reply(ALICE, "3kownbefore2", &root).await;
    w.f.ready_all().await;

    w.expect("parent under another audience", BOB, &cross, true).await;
    for reader in [ALICE, DAVE] {
        w.expect("parent under another audience", reader, &cross, false).await;
    }
    for (i, (author, uri)) in chain.iter().enumerate() {
        let level = i + 1;
        w.expect(&format!("level {level}"), DAVE, uri, level <= 8).await;
        w.expect(&format!("level {level}, its author"), author, uri, true).await;
    }
    for uri in [&cycle_a, &cycle_b, &into_cycle] {
        w.expect("cycle", DAVE, uri, false).await;
    }
    w.expect("A-9, before the move", DAVE, &left, true).await;
    let moved = w.f.post_record(ALICE, "3kroot222222", &other, None).await;
    w.f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kroot222222", REV2, Some(moved)).await;
    w.expect("A-9, left under the old audience", DAVE, &left, false).await;
    w.expect("A-9, left under the old audience", BOB, &left, true).await;
    w.expect("A-9, the owner's own reply", DAVE, &own, true).await;
    w.finish().await;
}

#[tokio::test]
async fn rule4_walk_limits_same_pds() {
    walk_limits("rule4-limits-same", Layout::SamePds).await;
}

#[tokio::test]
async fn rule4_walk_limits_off_pds() {
    walk_limits("rule4-limits-off", Layout::OffPds).await;
}

/// T-37: every mode, conditions 2 to 4 for the writer, and rule 6 as today.
async fn modes_and_writers(test: &str, layout: Layout) {
    let Some(mut w) = Walk::new(test, layout, Some("followers"), &[]).await else {
        return;
    };
    let main_reply = w.reply(BOB, "3kmainreply2", &w.root.clone()).await;
    let cases: [(&str, Option<&str>, &[&str], bool); 7] = [
        ("list, writer listed", Some("list"), &[BOB, DAVE], true),
        ("NULL mode, writer listed", None, &[BOB, DAVE], true),
        ("list, writer not listed", Some("list"), &[DAVE], false),
        ("NULL mode, writer not listed", None, &[DAVE], false),
        ("everyone", Some("everyone"), &[], true),
        ("following, before S10", Some("following"), &[BOB, DAVE], false),
        ("nobody", Some("nobody"), &[BOB, DAVE], false),
    ];
    let mut replies = Vec::new();
    for (i, (case, mode, members, _)) in cases.iter().enumerate() {
        let aud = w.f.audience(ALICE, &format!("3kaudcase{i}22"), *mode, members).await;
        let root = w.f.private_post(ALICE, &format!("3krootcase{i}2"), &aud, None).await;
        let reply = w.f.private_post(BOB, &format!("3kreplycase{i}"), &aud, Some((&root, &root))).await;
        replies.push((*case, reply));
    }
    w.f.ready_all().await;

    w.expect("followers, writer follows", DAVE, &main_reply, true).await;
    for ((case, reply), (_, _, _, dave_reads)) in replies.iter().zip(cases) {
        w.expect(case, DAVE, reply, dave_reads).await;
        w.expect(case, BOB, reply, true).await;
    }
    let reply_of = |case: &str| replies.iter().find(|(c, _)| *c == case).map(|(_, r)| r.clone()).expect("case");
    let (listed, everyone) = (reply_of("list, writer listed"), reply_of("everyone"));

    // Condition 3: the writer blocks the audience's owner. It denies under
    // `followers` and `everyone`; under `list` it waits for C-6 (S9).
    exec(&w.f.pool, &block_sql(BOB, ALICE)).await;
    w.expect("writer blocks owner, followers", DAVE, &main_reply, false).await;
    w.expect("writer blocks owner, everyone", DAVE, &everyone, false).await;
    w.expect("writer blocks owner, list (S0)", DAVE, &listed, true).await;
    w.expect("writer blocks owner, its author", BOB, &main_reply, true).await;
    exec(&w.f.pool, "DELETE FROM blocks").await;

    // The owner blocks the reader: rule 6 under `followers`; under `list`
    // the owner's post cannot anchor for that reader (step test (i)).
    exec(&w.f.pool, &block_sql(ALICE, DAVE)).await;
    w.expect("owner blocks reader, followers", DAVE, &main_reply, false).await;
    w.expect("owner blocks reader, list", DAVE, &listed, false).await;
    exec(&w.f.pool, "DELETE FROM blocks").await;

    // Condition 4: an audience owner with no `actors` row is not active.
    exec(&w.f.pool, &format!("DELETE FROM actors WHERE did = '{ALICE}'")).await;
    w.expect("owner has no actors row, followers", DAVE, &main_reply, false).await;
    w.expect("owner has no actors row, everyone", DAVE, &everyone, false).await;
    w.finish().await;
}

#[tokio::test]
async fn rule4_modes_and_writers_same_pds() {
    modes_and_writers("rule4-modes-same", Layout::SamePds).await;
}

#[tokio::test]
async fn rule4_modes_and_writers_off_pds() {
    modes_and_writers("rule4-modes-off", Layout::OffPds).await;
}

/// How B's reply M, between Alice's root T and Carol's reply R, is made
/// unreadable to Dave (T-23 fixture pair 2), or, for `Throttled`, left
/// readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Middle {
    BlocksReader,
    Takedown,
    Unlisted,
    Throttled,
}

/// Dave's `getPosts([R])`, `getPostThread(R)` and timeline, as bytes.
async fn reader_outputs(w: &Walk, r: &str) -> Vec<(u16, String)> {
    let mut out = Vec::new();
    for (method, query) in [(POSTS, format!("uris={r}")), (THREAD, format!("uri={r}")), (TIMELINE, String::new())] {
        let (status, body, _) = get(&w.base, method, &query, DAVE, DAVE_KEY).await;
        out.push((status, body));
    }
    out
}

/// T-23 pair 2, S0's part: the walk is reader-relative. A step Dave cannot
/// read ends the walk exactly as a missing step does.
async fn pair_two(test: &str, middle: Middle) {
    let (mode, members): (&str, &[&str]) = match middle {
        Middle::Unlisted => ("list", &[DAVE, CAROL]),
        _ => ("followers", &[]),
    };
    let Some(mut w) = Walk::new(test, Layout::SamePds, Some(mode), members).await else {
        return;
    };
    // Dave follows Carol, so his timeline would carry R if R were served.
    exec(&w.f.pool, &follow_sql(DAVE, CAROL)).await;
    let m = w.reply(BOB, "3kmiddle2222", &w.root.clone()).await;
    let r = w.reply(CAROL, "3kleaf222222", &m).await;
    match middle {
        Middle::BlocksReader => exec(&w.f.pool, &block_sql(BOB, DAVE)).await,
        Middle::Takedown => exec(&w.f.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{m}', '!takedown')")).await,
        Middle::Unlisted => {}
        Middle::Throttled => exec(&w.f.pool, &format!("UPDATE actors SET account_status = 'throttled' WHERE did = '{BOB}'")).await,
    }
    w.f.ready_all().await;
    if middle == Middle::Throttled {
        // A status the moderation gate does not hide: M is readable, so it
        // passes the walk, and R is served by the oracle and the prefilter.
        w.expect("throttled M", DAVE, &m, true).await;
        w.expect("R below a throttled M", DAVE, &r, true).await;
        w.finish().await;
        return;
    }
    w.expect("(a) R's author", CAROL, &r, true).await;
    w.expect("(a) R", DAVE, &r, false).await;
    let with_m = reader_outputs(&w, &r).await;
    // (b): M does not exist.
    w.f.apply_op(BOB, Action::Delete, NSID_POST_PRIVATE, "3kmiddle2222", REV2, None).await;
    w.expect("(b) R's author", CAROL, &r, true).await;
    w.expect("(b) R", DAVE, &r, false).await;
    let without_m = reader_outputs(&w, &r).await;
    if with_m != without_m {
        w.failures.push(format!("(a) and (b) differ for Dave:\n{with_m:?}\n{without_m:?}"));
    }
    w.finish().await;
}

#[tokio::test]
async fn rule4_walk_step_blocked_by_reader() {
    pair_two("rule4-pair2-block", Middle::BlocksReader).await;
}

#[tokio::test]
async fn rule4_walk_step_taken_down() {
    pair_two("rule4-pair2-takedown", Middle::Takedown).await;
}

#[tokio::test]
async fn rule4_walk_step_writer_unlisted() {
    pair_two("rule4-pair2-unlisted", Middle::Unlisted).await;
}

#[tokio::test]
async fn rule4_walk_step_throttled_author() {
    pair_two("rule4-pair2-throttled", Middle::Throttled).await;
}

/// T-37: the oracle's walk statement and the prefilter's walk term are each
/// `anchored()`'s output for their arguments, with the literal cap 8.
#[test]
fn rule4_walk_texts_come_from_one_builder() {
    assert_eq!(schema::RULE4_MAX_HOPS, 8);
    let oracle = schema::walk_statement();
    assert_eq!(oracle, format!("SELECT {}", schema::anchored("$1", "$2", "$3", "$4", "$5", "$6")));
    let walk = schema::anchored("p.reply_parent_uri", "p.audience_uri", "a.owner_did", "a.mode", "a.members", "$1");
    assert_eq!(schema::prefilter().matches(&walk).count(), 1, "the prefilter carries the builder's walk once");
    for text in [&oracle, &walk] {
        assert!(text.contains("w.hop < 8 "), "{text}");
        assert_eq!(text.matches("w.hop <").count(), 1, "{text}");
    }
}

/// Prints the pre-S0 count query (private-posting design §5) for skydeval
/// to run read-only in `psql` against the deployed database:
/// `cargo test pre_s0_count_query -- --ignored --nocapture`.
#[test]
#[ignore = "prints the pre-S0 count query; run with --ignored --nocapture"]
fn pre_s0_count_query() {
    println!("-- S0 pre-release count (cap 8)\n{}\n", schema::pre_s0_count_sql());
    println!("-- The same with cap 64, to separate over-cap chains\n{}", schema::pre_s0_count_sql_with_cap(64));
}

/// The pre-S0 count query runs, and counts each kind once.
#[tokio::test]
async fn pre_s0_count_query_counts_each_kind() {
    let Some(w) = Walk::new("pre-s0-count", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let (aud, root) = (w.aud.clone(), w.root.clone());
    // Unanchored: a parent that names nothing.
    let ghost = format!("at://{NOBODY}/{NSID_POST_PRIVATE}/x");
    w.f.private_post(BOB, "3kforged2222", &aud, Some((&ghost, &ghost))).await;
    // Under `list`, a writer not listed.
    let list = w.f.audience(ALICE, "3kaudlist222", Some("list"), &[DAVE]).await;
    let list_root = w.f.private_post(ALICE, "3klistroot22", &list, None).await;
    w.f.private_post(BOB, "3klistreply2", &list, Some((&list_root, &list_root))).await;
    // Served before and after: Bob follows Alice.
    w.reply(BOB, "3kfine222222", &root).await;
    // The `followers` residual, member writer and member owner.
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{CAROL}'")).await;
    w.reply(CAROL, "3kresidual22", &root).await;
    // Every Bob post starts being served under a non-hiding status.
    exec(&w.f.pool, &format!("UPDATE actors SET account_status = 'throttled' WHERE did = '{BOB}'")).await;
    for sql in [schema::pre_s0_count_sql(), schema::pre_s0_count_sql_with_cap(64)] {
        let counts: (i64, i64, i64, i64, i64, i64, i64, i64) =
            sqlx::query_as(&sql).fetch_one(&w.f.pool).await.expect("count query");
        assert_eq!(counts, (1, 1, 1, 0, 0, 0, 1, 3));
    }
    w.finish().await;
}

/// B-5 with S0's walk: the four private reads, unfloored, over a thread of
/// 24 chains of 8 replies below one root (193 nodes, under the 200-node
/// cap), so every candidate walks the full 8 rows. The same thread written
/// by the root's author alone, where no walk runs in the prefilter and the
/// oracle's walk stops at the first row, is the baseline. Prints the numbers.
#[tokio::test]
#[ignore = "B-5 measurement; run with --ignored --nocapture"]
async fn b5_private_floors_with_eight_step_chains() {
    b5_floors("b5-floors-walk", false).await;
    b5_floors("b5-floors-baseline", true).await;
}

async fn b5_floors(test: &str, baseline: bool) {
    let Some(w) = Walk::new(test, Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let (odd, even) = if baseline { (ALICE, ALICE) } else { (BOB, CAROL) };
    let mut leaves = Vec::new();
    for chain in 0..24 {
        let mut parent = w.root.clone();
        for level in 1..=8 {
            let author = if level % 2 == 1 { odd } else { even };
            parent = w.reply(author, &format!("3kc{chain:02}l{level}22222"), &parent).await;
        }
        leaves.push(parent);
    }
    if !baseline {
        for did in [BOB, CAROL] {
            exec(&w.f.pool, &follow_sql(DAVE, did)).await;
        }
    }
    w.f.ready_all().await;
    let mut posts_query: Vec<String> = leaves.iter().map(|u| format!("uris={u}")).collect();
    posts_query.push(format!("uris={}", w.root));
    let run = if baseline { "baseline, one author" } else { "8-step walks" };
    let cases = [
        ("getPosts (24 leaves at depth 8, the root)", POSTS, posts_query.join("&"), "posts", 25),
        ("getPostThread (root, depth 10, 193 nodes)", THREAD, format!("uri={}&depth=10", w.root), "", 0),
        ("getAuthorFeed (limit 50)", FEED, format!("actor={odd}&limit=50"), "feed", 50),
        ("getTimeline (limit 50)", TIMELINE, "limit=50".to_string(), "feed", 50),
    ];
    for (name, method, query, key, items) in cases {
        // Fresh heads and watermark for each endpoint: the fixture's
        // heads age out after 60 s, and a stale head is a miss.
        exec(&w.f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
        let base = serve(w.f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
        let mut took = Vec::new();
        for _ in 0..40 {
            let (status, body, t) = get(&base, method, &query, DAVE, DAVE_KEY).await;
            assert_eq!(status, 200, "{name}: {body}");
            if !key.is_empty() {
                let v: Value = serde_json::from_str(&body).expect("json");
                assert_eq!(v[key].as_array().map(Vec::len), Some(items), "{name}");
            }
            took.push(t);
        }
        took.sort();
        println!(
            "B-5 ({run}) {name}: p50 {:?}, p95 {:?}, max {:?} over {} runs",
            took[took.len() / 2],
            took[took.len() * 95 / 100],
            took[took.len() - 1],
            took.len()
        );
    }
    w.finish().await;
}
