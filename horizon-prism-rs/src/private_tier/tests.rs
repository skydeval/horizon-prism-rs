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
use crate::config::{PrivateReadConfig, PrivateReadFloors, ScopePolicy, SyncConfig, VideoConfig, VideoPlayback};
use crate::firehose::classifier::{NSID_AUDIENCE, NSID_POST_PRIVATE};
use crate::firehose::watermark::Watermarks;
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};
use crate::hydration::HydrationConfig;
use crate::hydration::testing::{actor, exec, hydrator, hydrator_configured, test_config};
use crate::indexing::{PublicMode, PublicState};
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
    sets: ScopeSets,
}

fn data_dir(test: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("hprs-{test}-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

async fn fixture(pool: &PgPool, test: &str) -> Fixture {
    fixture_in(pool, test, PublicMode::On).await
}

/// The fixture with public indexing in `mode` (T-1: every mode).
async fn fixture_in(pool: &PgPool, test: &str, mode: PublicMode) -> Fixture {
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
    sets.public = PublicState { mode, ..PublicState::default() };
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
        hydrator: hydrator(pool, sets.clone()),
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
        sets,
    }
}

impl Fixture {
    fn state(&self, watermark_age: Option<Duration>) -> PrivateState {
        self.state_floored(watermark_age, FLOOR)
    }

    fn state_floored(&self, watermark_age: Option<Duration>, floor: Duration) -> PrivateState {
        self.state_of(self.app.clone(), watermark_age, floor)
    }

    /// A fresh watermark, the test floor, and video views built per `video`
    /// (Addendum A).
    fn state_with_video(&self, video: VideoConfig) -> PrivateState {
        let mut app = self.app.clone();
        app.hydrator = hydrator_configured(&self.pool, self.sets.clone(), HydrationConfig { video, ..test_config() });
        self.state_of(app, Some(Duration::from_secs(1)), FLOOR)
    }

    fn state_of(&self, app: AppState, watermark_age: Option<Duration>, floor: Duration) -> PrivateState {
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
                get_bookmarks: ms,
            },
        };
        PrivateState::new(
            app,
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
        self.apply_from(WriteSource::Firehose, repo, action, collection, rkey, rev, record).await;
    }

    /// [`Self::apply_op`] as `source` writes it (a backfill job writes every
    /// listed record at the repo's latest rev).
    #[allow(clippy::too_many_arguments)]
    async fn apply_from(
        &self,
        source: WriteSource,
        repo: &str,
        action: Action,
        collection: &str,
        rkey: &str,
        rev: &str,
        record: Option<Value>,
    ) {
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
                    source,
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
    b5_floors("b5-floors-walk", false, false).await;
    b5_floors("b5-floors-baseline", true, false).await;
    // Private posting S2: every reply also quotes its parent, read in the
    // page's second round: one more bind per hit, none deduplicated.
    b5_floors("b5-floors-quotes", false, true).await;
}

async fn b5_floors(test: &str, baseline: bool, quotes: bool) {
    let Some(w) = Walk::new(test, Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let (odd, even) = if baseline { (ALICE, ALICE) } else { (BOB, CAROL) };
    let mut leaves = Vec::new();
    for chain in 0..24 {
        let mut parent = w.root.clone();
        for level in 1..=8 {
            let author = if level % 2 == 1 { odd } else { even };
            let rkey = format!("3kc{chain:02}l{level}22222");
            parent = if quotes {
                let mut record = w.f.post_record(author, &rkey, &w.aud, Some((&w.root, &parent))).await;
                record["embed"] = json!({"$type": format!("{KLEX}#embedRecord"), "record": {"uri": parent, "cid": "bafyany"}});
                w.f.ingest(author, NSID_POST_PRIVATE, &rkey, record).await;
                format!("at://{author}/{NSID_POST_PRIVATE}/{rkey}")
            } else {
                w.reply(author, &rkey, &parent).await
            };
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
    let run = match (baseline, quotes) {
        (true, _) => "baseline, one author",
        (false, false) => "8-step walks",
        (false, true) => "8-step walks, each reply quoting its parent",
    };
    let cases = [
        ("getPosts (24 leaves at depth 8, the root)", POSTS, posts_query.join("&"), "posts", 25),
        ("getPostThread (root, depth 10, 193 nodes)", THREAD, format!("uri={}&depth=10", w.root), "", 0),
        ("getAuthorFeed (limit 50)", FEED, format!("actor={odd}&limit=50"), "feed", 50),
        ("getTimeline (limit 50)", TIMELINE, "limit=50".to_string(), "feed", 50),
    ];
    for (name, method, query, key, items) in cases {
        // A fresh watermark for each endpoint and fresh heads for each run:
        // the fixture's heads age out after 60 s, a stale head is a miss,
        // and 40 thread runs with quotes take longer than that.
        let base = serve(w.f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
        let mut took = Vec::new();
        for _ in 0..40 {
            exec(&w.f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
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

// ---- Private posting S2: display fields, embeds, quotes, audience (P-1, P-3) ----

/// kryphocron-lexicons 0.3.1's embed and facet namespace.
const KLEX: &str = "tools.kryphocron.feed.postPublic";
/// A rev below every fixture record's [`REV`].
const OLD_REV: &str = "3kaaaaaaaaa22";
/// A rev above [`REV2`].
const REV3: &str = "3kaaaaaaaaac2";

fn blob(cid: &str) -> Value {
    json!({"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/jpeg", "size": 1})
}

fn blob_on_pds(did: &str, cid: &str) -> String {
    format!("https://pds.example.com/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}")
}

impl Fixture {
    /// An encoded private post by `author` whose text is its rkey, with the
    /// fields of `extra` added to the record as Aurora-Locus stores them.
    async fn post_with(&self, author: &str, rkey: &str, audience: &str, extra: Value) -> String {
        let record = self.record_with(author, rkey, audience, &extra).await;
        self.ingest(author, NSID_POST_PRIVATE, rkey, record).await;
        format!("at://{author}/{NSID_POST_PRIVATE}/{rkey}")
    }

    async fn record_with(&self, author: &str, rkey: &str, audience: &str, extra: &Value) -> Value {
        let mut record = self.encoded_post(author, rkey, rkey, Some(audience)).await;
        for (k, v) in extra.as_object().expect("an object") {
            record[k] = v.clone();
        }
        record
    }
}

/// `getPosts([uri])` for `reader`: the raw body and the one post, or
/// `Null` when it was omitted.
async fn one_post(base: &str, reader: &str, uri: &str) -> (String, Value) {
    let (status, body, _) = get(base, POSTS, &format!("uris={uri}"), reader, key_of(reader)).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    let post = v["posts"].get(0).cloned().unwrap_or(Value::Null);
    (body, post)
}

/// T-4 (P-1), run in On and in Off (T-1): every `$type` form of §4.2's
/// table gives the stock view; four images give four URLs on the author's
/// PDS; facets, langs and self-labels are carried as stored; the operator's
/// labels follow the self-labels; a display field of the wrong type or over
/// 64 KiB is dropped (and counted), and the post still reads. Addendum A: a
/// video under each playback setting.
async fn display_fields_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let images: Vec<Value> = (1..=4)
        .map(|i| json!({"image": blob(&format!("bafyimg{i}")), "alt": format!("image {i}"), "aspectRatio": {"width": 4, "height": 3}}))
        .collect();
    let facets = json!([
        {"index": {"byteStart": 0, "byteEnd": 5}, "features": [{"$type": format!("{KLEX}#mention"), "did": CAROL}]},
        {"index": {"byteStart": 6, "byteEnd": 9}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": DAVE},
                                                                {"$type": "app.bsky.richtext.facet#link", "uri": "https://example.com"}]},
    ]);
    let self_labels = json!({"$type": format!("{KLEX}#selfLabels"), "values": [{"val": "nudity"}]});
    let gallery_item = |cid: &str, alt: &str| json!({"$type": format!("{KLEX}#galleryImage"), "image": blob(cid), "alt": alt, "aspectRatio": {"width": 2, "height": 1}});
    let cases: Vec<(&str, Value)> = vec![
        ("3kimagesk222", json!({"embed": {"$type": format!("{KLEX}#embedImage"), "images": images}, "facets": facets, "langs": ["en"], "labels": self_labels})),
        ("3kimagess222", json!({"embed": {"$type": "app.bsky.embed.images", "images": [images[0]]}})),
        ("3kexternal22", json!({"embed": {"$type": format!("{KLEX}#embedExternal"),
            "external": {"uri": "https://example.com", "title": "t", "description": "d", "thumb": blob("bafythumb")}}})),
        ("3kvideo22222", json!({"embed": {"$type": format!("{KLEX}#embedVideo"), "video": blob("bafyvideo"), "alt": "a clip"}})),
        ("3kgallery222", json!({"embed": {"$type": format!("{KLEX}#embedGallery"), "items": [gallery_item("bafyg1", "g1"), gallery_item("bafyg2", "g2")]}})),
        ("3kwrongtype2", json!({"embed": "not an object", "langs": [1], "facets": {"x": 1}})),
        ("3koversize22", json!({"facets": [{"index": {"byteStart": 0, "byteEnd": 1}, "features": [], "pad": "x".repeat(70_000)}]})),
    ];
    let mut uris = Vec::new();
    for (rkey, extra) in &cases {
        let record = f.record_with(ALICE, rkey, &aud, extra).await;
        // C-16: a wrong-typed or oversized field is dropped and counted;
        // the record is not malformed.
        let dropped = super::ingest::DisplayFields::parse(&record).dropped;
        match *rkey {
            "3kwrongtype2" => assert_eq!(dropped, ["facets", "embed", "langs"]),
            "3koversize22" => assert_eq!(dropped, ["facets"]),
            _ => assert!(dropped.is_empty(), "{rkey}: {dropped:?}"),
        }
        f.ingest(ALICE, NSID_POST_PRIVATE, rkey, record).await;
        uris.push(format!("at://{ALICE}/{NSID_POST_PRIVATE}/{rkey}"));
    }
    exec(&f.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{}', 'spam')", uris[0])).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    let query: Vec<String> = uris.iter().map(|u| format!("uris={u}")).collect();
    let (status, body, _) = get(&base, POSTS, &query.join("&"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body.contains("encodedContent"), "encoded fields never appear: {body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    let posts = v["posts"].as_array().expect("posts");
    assert_eq!(posts.len(), cases.len(), "{body}");

    // The kryphocron image form: four images on Alice's PDS, and the record
    // as stored.
    let p = &posts[0];
    assert_eq!(p["embed"]["$type"], "app.bsky.embed.images#view");
    let shown = p["embed"]["images"].as_array().expect("images");
    assert_eq!(shown.len(), 4);
    for (i, img) in shown.iter().enumerate() {
        let url = blob_on_pds(ALICE, &format!("bafyimg{}", i + 1));
        assert_eq!((img["thumb"].as_str(), img["fullsize"].as_str()), (Some(url.as_str()), Some(url.as_str())));
        assert_eq!(img["alt"], format!("image {}", i + 1));
        assert_eq!(img["aspectRatio"], json!({"width": 4, "height": 3}));
    }
    assert_eq!(p["record"]["embed"]["$type"], format!("{KLEX}#embedImage"), "record.embed is as stored");
    assert_eq!(p["record"]["facets"], facets);
    assert_eq!(p["record"]["langs"], json!(["en"]));
    assert_eq!(p["record"]["labels"], self_labels);
    assert_eq!(p["record"]["text"], "3kimagesk222");
    assert_eq!(
        p["labels"][0],
        json!({"src": ALICE, "uri": uris[0], "cid": "bafy3kimagesk222", "val": "nudity", "cts": "2026-09-26T12:00:00.000Z"})
    );
    assert_eq!((p["labels"][1]["val"].as_str(), p["labels"][1]["uri"].as_str()), (Some("spam"), Some(uris[0].as_str())));
    assert_ne!(p["labels"][1]["src"], ALICE, "an operator label is the AppView's");
    let derived: (Vec<String>, Option<String>) =
        sqlx::query_as("SELECT mention_dids, quote_uri FROM private_posts WHERE uri = $1")
            .bind(&uris[0])
            .fetch_one(&f.pool)
            .await
            .expect("row");
    assert_eq!(derived, (vec![CAROL.to_string(), DAVE.to_string()], None), "mentions in either form");

    // The stock image form.
    assert_eq!(posts[1]["embed"]["images"][0]["thumb"], blob_on_pds(ALICE, "bafyimg1"));
    // External, with its thumb on the PDS.
    assert_eq!(posts[2]["embed"]["$type"], "app.bsky.embed.external#view");
    assert_eq!(posts[2]["embed"]["external"]["thumb"], blob_on_pds(ALICE, "bafythumb"));
    assert_eq!(posts[2]["embed"]["external"]["title"], "t");
    // Video, by default from the CDN with a thumbnail.
    let cdn = format!("https://video.bsky.app/watch/did%3Aplc%3A{}/bafyvideo", &ALICE[8..]);
    assert_eq!(posts[3]["embed"]["$type"], "app.bsky.embed.video#view");
    assert_eq!(posts[3]["embed"]["playlist"], format!("{cdn}/playlist.m3u8"));
    assert_eq!(posts[3]["embed"]["thumbnail"], format!("{cdn}/thumbnail.jpg"));
    assert_eq!(posts[3]["embed"]["alt"], "a clip");
    // Gallery: `#galleryImage` items read as `app.bsky.embed.gallery#image`.
    assert_eq!(posts[4]["embed"]["$type"], "app.bsky.embed.gallery#view");
    let items = posts[4]["embed"]["items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    assert_eq!(items[1], json!({"$type": "app.bsky.embed.gallery#viewImage", "thumbnail": blob_on_pds(ALICE, "bafyg2"),
        "fullsize": blob_on_pds(ALICE, "bafyg2"), "alt": "g2", "aspectRatio": {"width": 2, "height": 1}}));
    // Dropped fields: the post still reads, without them.
    for p in &posts[5..] {
        assert!(p.get("embed").is_none(), "{p}");
        for key in ["embed", "langs", "facets"] {
            assert!(p["record"].get(key).is_none(), "{key}: {p}");
        }
        assert!(p["record"]["text"].is_string());
    }

    // Addendum A: `pds_blob` plays the MP4 blob, with no thumbnail.
    let blob_base = serve(f.state_with_video(VideoConfig {
        playback: VideoPlayback::PdsBlob,
        ..VideoConfig::default()
    }))
    .await;
    let (_, video) = one_post(&blob_base, BOB, &uris[3]).await;
    assert_eq!(video["embed"]["playlist"], blob_on_pds(ALICE, "bafyvideo"));
    assert!(video["embed"].get("thumbnail").is_none(), "{video}");
    db.drop_db().await;
}

#[tokio::test]
async fn display_fields_and_media_views_on() {
    display_fields_in(PublicMode::On, "s2-display-on").await;
}

#[tokio::test]
async fn display_fields_and_media_views_off() {
    display_fields_in(PublicMode::Off, "s2-display-off").await;
}

/// T-5 (P-1 quotes), in On and in Off (T-1): a quoted private post is read
/// for the same reader; unreadable, blocked and nonexistent quoted posts
/// give byte-identical answers; a quoted public post shows only in On.
async fn quoted_posts_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let for_bob = f.audience(ALICE, "3kaudbob2222", Some("list"), &[BOB]).await;
    let for_carol = f.audience(ALICE, "3kaudcarol22", Some("list"), &[CAROL]).await;
    let daves_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[BOB]).await;
    let image = |cid: &str| json!({"$type": format!("{KLEX}#embedImage"), "images": [{"image": blob(cid), "alt": cid}]});
    let quote = |uri: &str| json!({"embed": {"$type": format!("{KLEX}#embedRecord"), "record": {"uri": uri, "cid": "bafyany"}}});
    let quoted = f.post_with(ALICE, "3kquoted2222", &for_bob, json!({"embed": image("bafyq")})).await;
    let hidden = f.post_with(ALICE, "3khidden2222", &for_carol, json!({})).await;
    let daves = f.post_with(DAVE, "3kdaves22222", &daves_aud, json!({})).await;
    let public = crate::hydration::testing::post(&f.pool, BOB, "3kpublic2222", "public text", None, None).await;
    let quotes_readable = f.post_with(ALICE, "3kquotesq222", &for_bob, quote(&quoted)).await;
    let quotes_hidden = f.post_with(ALICE, "3kquotesh222", &for_bob, quote(&hidden)).await;
    let quotes_daves = f.post_with(ALICE, "3kquotesd222", &for_bob, quote(&daves)).await;
    let with_media = f
        .post_with(ALICE, "3kquotesm222", &for_bob, json!({"embed": {"$type": format!("{KLEX}#embedRecordWithMedia"),
            "record": {"record": {"uri": quoted, "cid": "bafyany"}}, "media": image("bafym")}}))
        .await;
    let quotes_public = f
        .post_with(ALICE, "3kquotesp222", &for_bob, json!({"embed": {"$type": "app.bsky.embed.record", "record": {"uri": public, "cid": "bafyany"}}}))
        .await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let stored: Option<String> = sqlx::query_scalar("SELECT quote_uri FROM private_posts WHERE uri = $1")
        .bind(&with_media)
        .fetch_one(&f.pool)
        .await
        .expect("row");
    assert_eq!(stored.as_deref(), Some(quoted.as_str()), "quote_uri from recordWithMedia");

    // Readable: `#viewRecord` with the decoded value and its media.
    let (_, p) = one_post(&base, BOB, &quotes_readable).await;
    assert_eq!(p["embed"]["$type"], "app.bsky.embed.record#view");
    let r = &p["embed"]["record"];
    assert_eq!(r["$type"], "app.bsky.embed.record#viewRecord");
    assert_eq!((r["uri"].as_str(), r["value"]["text"].as_str()), (Some(quoted.as_str()), Some("3kquoted2222")));
    assert_eq!(r["author"]["did"], ALICE);
    assert_eq!(r["embeds"][0]["images"][0]["thumb"], blob_on_pds(ALICE, "bafyq"));
    assert!(r["value"].get("encodedContent").is_none());
    // The same through the thread and the author feed.
    let (_, body, _) = get(&base, THREAD, &format!("uri={quotes_readable}"), BOB, BOB_KEY).await;
    let t: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(t["thread"]["post"]["embed"]["record"]["value"]["text"], "3kquoted2222", "{body}");
    let (_, body, _) = get(&base, FEED, &format!("actor={ALICE}"), BOB, BOB_KEY).await;
    let feed: Value = serde_json::from_str(&body).expect("json");
    let item = feed["feed"].as_array().expect("feed").iter().find(|i| i["uri"] == quotes_readable.as_str()).expect("in the feed");
    assert_eq!(item["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord");

    // recordWithMedia: the quote and the post's own media.
    let (_, p) = one_post(&base, BOB, &with_media).await;
    assert_eq!(p["embed"]["$type"], "app.bsky.embed.recordWithMedia#view");
    assert_eq!(p["embed"]["record"]["record"]["value"]["text"], "3kquoted2222");
    assert_eq!(p["embed"]["media"]["images"][0]["thumb"], blob_on_pds(ALICE, "bafym"));

    // Not readable by Bob: `#viewNotFound`, byte-identical to the same post
    // quoting a URI that names nothing.
    let (unreadable, p) = one_post(&base, BOB, &quotes_hidden).await;
    assert_eq!(p["embed"]["record"], json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": hidden, "notFound": true}));
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3khidden2222", REV2, None).await;
    let (nonexistent, _) = one_post(&base, BOB, &quotes_hidden).await;
    assert_eq!(unreadable, nonexistent);

    // A block between the reader and the quoted author: the same miss.
    let (_, p) = one_post(&base, BOB, &quotes_daves).await;
    assert_eq!(p["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord", "readable before the block");
    exec(&f.pool, &block_sql(DAVE, BOB)).await;
    let (blocked, p) = one_post(&base, BOB, &quotes_daves).await;
    assert_eq!(p["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound");
    f.apply_op(DAVE, Action::Delete, NSID_POST_PRIVATE, "3kdaves22222", REV2, None).await;
    let (gone, _) = one_post(&base, BOB, &quotes_daves).await;
    assert_eq!(blocked, gone);

    // A quoted public post: from local rows in On; in Off no `embed`, and
    // the client builds the quote from `record.embed`.
    let (_, p) = one_post(&base, BOB, &quotes_public).await;
    assert_eq!(p["record"]["embed"]["record"]["uri"], public.as_str());
    if mode.is_on() {
        assert_eq!(p["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord", "{p}");
        assert_eq!(p["embed"]["record"]["value"]["text"], "public text");
    } else {
        assert!(p.get("embed").is_none(), "{p}");
    }
    db.drop_db().await;
}

#[tokio::test]
async fn quoted_posts_are_read_for_the_same_reader_on() {
    quoted_posts_in(PublicMode::On, "s2-quotes-on").await;
}

#[tokio::test]
async fn quoted_posts_are_read_for_the_same_reader_off() {
    quoted_posts_in(PublicMode::Off, "s2-quotes-off").await;
}

/// T-6 (P-1 refill): a row stored before migration 0023 is filled by the
/// `private` job's write at the same rev; the refill is queued once; an
/// older rev never overwrites a newer row, and an equal one never
/// overwrites a filled row.
#[tokio::test]
async fn display_refill_fills_rows_stored_before_0023() {
    let Some(db) = fresh_db("s2-refill").await else {
        return;
    };
    let f = fixture(&db.pool, "s2-refill").await;
    let aud_record = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [BOB], "name": "Friends", "createdAt": "2026-09-26T00:00:00Z"});
    f.ingest(ALICE, NSID_AUDIENCE, "3kaudience22", aud_record.clone()).await;
    let aud = format!("at://{ALICE}/{NSID_AUDIENCE}/3kaudience22");
    let extra = json!({"embed": {"$type": format!("{KLEX}#embedImage"), "images": [{"image": blob("bafyimg"), "alt": ""}]}, "langs": ["en"]});
    let record = f.record_with(ALICE, "3kpost222222", &aud, &extra).await;
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kpost222222", record.clone()).await;
    // As rows stored before 0023.
    let unfill_posts = "UPDATE private_posts SET facets = NULL, embed = NULL, langs = NULL, self_labels = NULL, \
                        quote_uri = NULL, mention_dids = '{}', display_filled = false";
    exec(&f.pool, unfill_posts).await;
    exec(&f.pool, "UPDATE private_audiences SET name = NULL, display_filled = false").await;
    let post_row = async || -> (Option<Value>, Option<Vec<String>>, bool) {
        sqlx::query_as("SELECT embed, langs, display_filled FROM private_posts")
            .fetch_one(&f.pool)
            .await
            .expect("post row")
    };
    let audience_row = async || -> (Option<String>, bool) {
        sqlx::query_as("SELECT name, display_filled FROM private_audiences")
            .fetch_one(&f.pool)
            .await
            .expect("audience row")
    };

    // The refill is queued once, for the DID with unfilled rows.
    let tier = KryphocronPrivateTier;
    exec(&f.pool, "DELETE FROM backfill_jobs").await;
    assert_eq!(tier.enqueue_display_refill(&f.pool, "private_display_refill").await.expect("refill"), Some(1));
    let jobs: Vec<(String, String, String)> =
        sqlx::query_as("SELECT did, depth, state FROM backfill_jobs").fetch_all(&f.pool).await.expect("jobs");
    assert_eq!(jobs, [(ALICE.to_string(), "private".to_string(), "queued".to_string())]);
    assert_eq!(tier.enqueue_display_refill(&f.pool, "private_display_refill").await.expect("again"), None);

    // An older rev fills nothing, even an unfilled row.
    let mut older = record.clone();
    older["langs"] = json!(["fr"]);
    f.apply_from(WriteSource::Backfill, ALICE, Action::Update, NSID_POST_PRIVATE, "3kpost222222", OLD_REV, Some(older)).await;
    assert_eq!(post_row().await, (None, None, false));

    // The job writes each listed record at the repo's latest rev: here the
    // rev the rows already carry.
    f.apply_from(WriteSource::Backfill, ALICE, Action::Update, NSID_AUDIENCE, "3kaudience22", REV, Some(aud_record)).await;
    f.apply_from(WriteSource::Backfill, ALICE, Action::Update, NSID_POST_PRIVATE, "3kpost222222", REV, Some(record.clone())).await;
    let (embed, langs, filled) = post_row().await;
    assert_eq!(embed.as_ref().and_then(|e| e["images"][0]["image"]["ref"]["$link"].as_str()), Some("bafyimg"));
    assert_eq!((langs, filled), (Some(vec!["en".to_string()]), true));
    assert_eq!(audience_row().await, (Some("Friends".to_string()), true));

    // Filled: neither an equal nor an older rev overwrites it.
    for rev in [REV, OLD_REV] {
        let mut other = record.clone();
        other["langs"] = json!(["de"]);
        f.apply_from(WriteSource::Backfill, ALICE, Action::Update, NSID_POST_PRIVATE, "3kpost222222", rev, Some(other)).await;
        assert_eq!(post_row().await.1, Some(vec!["en".to_string()]), "rev {rev}");
    }
    // A newer rev does.
    let mut newer = record;
    newer["langs"] = json!(["de"]);
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kpost222222", REV2, Some(newer)).await;
    assert_eq!(post_row().await.1, Some(vec!["de".to_string()]));
    db.drop_db().await;
}

/// T-7 (P-3), in On and in Off (T-1): `audience` with `mode` and `name`; a
/// rename shows after ingest; no `audience` without a row; `members` never
/// appears.
async fn audience_view_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    exec(&f.pool, &follow_sql(BOB, ALICE)).await;
    let named = |name: Value| json!({"$type": NSID_AUDIENCE, "mode": "followers", "members": [CAROL], "name": name, "createdAt": "2026-09-26T00:00:00Z"});
    f.ingest(ALICE, NSID_AUDIENCE, "3kaudfollow2", named(json!("Close friends"))).await;
    let followers = format!("at://{ALICE}/{NSID_AUDIENCE}/3kaudfollow2");
    let listed = f.audience(ALICE, "3kaudlist222", None, &[BOB]).await;
    let orphan = format!("at://{ALICE}/{NSID_AUDIENCE}/3knoaudience");
    let p_followers = f.private_post(ALICE, "3kpostfollow", &followers, None).await;
    let p_listed = f.private_post(ALICE, "3kpostlist22", &listed, None).await;
    let p_orphan = f.private_post(ALICE, "3kpostself22", &orphan, None).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    let (body, p) = one_post(&base, BOB, &p_followers).await;
    assert_eq!(p["audience"], json!({"uri": followers, "mode": "followers", "name": "Close friends"}));
    assert!(!body.contains("members") && !body.contains(CAROL), "members never appear: {body}");
    // No mode and no name: neither field.
    let (_, p) = one_post(&base, BOB, &p_listed).await;
    assert_eq!(p["audience"], json!({"uri": listed}));
    // No audience row: no `audience`; the author still sees audienceList.
    let (_, p) = one_post(&base, ALICE, &p_orphan).await;
    assert!(p.get("audience").is_none(), "{p}");
    assert_eq!(p["record"]["audienceList"], orphan.as_str());

    // A rename reaches the view at the audience record's next ingest.
    f.apply_op(ALICE, Action::Update, NSID_AUDIENCE, "3kaudfollow2", REV2, Some(named(json!("Inner circle")))).await;
    let (_, p) = one_post(&base, BOB, &p_followers).await;
    assert_eq!(p["audience"]["name"], "Inner circle");
    // A `name` that is not a string is dropped; the audience still applies.
    assert_eq!(super::ingest::AudienceFields::parse(&named(json!(5))).expect("not malformed").name, None);
    f.apply_op(ALICE, Action::Update, NSID_AUDIENCE, "3kaudfollow2", REV3, Some(named(json!(5)))).await;
    let (_, p) = one_post(&base, BOB, &p_followers).await;
    assert_eq!(p["audience"], json!({"uri": followers, "mode": "followers"}));
    db.drop_db().await;
}

#[tokio::test]
async fn the_audience_view_carries_mode_and_name_on() {
    audience_view_in(PublicMode::On, "s2-audience-on").await;
}

#[tokio::test]
async fn the_audience_view_carries_mode_and_name_off() {
    audience_view_in(PublicMode::Off, "s2-audience-off").await;
}

const CREATE_BOOKMARK: &str = "app.nearhorizon.private.createBookmark";
const DELETE_BOOKMARK: &str = "app.nearhorizon.private.deleteBookmark";
const GET_BOOKMARKS: &str = "app.nearhorizon.private.getBookmarks";

/// Counts `private_read_outcome_total` increments: every pipeline read
/// counts one. Installed as this thread's recorder, it sees the reads of
/// the handlers a current-thread test runtime serves.
#[derive(Default)]
struct PipelineReads(Arc<std::sync::atomic::AtomicU64>);

struct ReadIncrements(Arc<std::sync::atomic::AtomicU64>);

impl metrics::CounterFn for ReadIncrements {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, std::sync::atomic::Ordering::Relaxed);
    }

    fn absolute(&self, _value: u64) {}
}

impl metrics::Recorder for PipelineReads {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() == super::pipeline::METRIC_OUTCOMES {
            metrics::Counter::from_arc(Arc::new(ReadIncrements(Arc::clone(&self.0))))
        } else {
            metrics::Counter::noop()
        }
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

impl PipelineReads {
    fn count(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Rate limits no test here reaches.
fn roomy_limits() -> RateLimits {
    let cfg = crate::config::FileConfig::parse(concat!(
        "[rate_limits.anon_read]\nper_sec = 1000.0\nburst = 10000\n",
        "[rate_limits.private]\nper_sec = 1000.0\nburst = 10000\n",
        "[rate_limits.global]\nper_sec = 1000.0\nburst = 10000\n",
    ))
    .expect("parse");
    RateLimits::from_config(&cfg.rate_limits)
}

/// POST `body` to `method` with a token for `did`: status and raw body.
async fn post(base: &str, method: &str, body: &Value, did: &str) -> (u16, String) {
    let token = mint(&signing_key(key_of(did)), did, &format!("did:web:{AUDIENCE_HOST}"), Some(method));
    let r = reqwest::Client::new()
        .post(format!("{base}/xrpc/{method}"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("request");
    let status = r.status().as_u16();
    (status, r.text().await.expect("body"))
}

/// `getBookmarks` for `reader` with `query`: the body as JSON, and the time
/// taken.
async fn bookmarks_of(base: &str, reader: &str, query: &str) -> (Value, Duration) {
    let (status, body, took) = get(base, GET_BOOKMARKS, query, reader, key_of(reader)).await;
    assert_eq!(status, 200, "{body}");
    (serde_json::from_str(&body).expect("json"), took)
}

fn subjects(page: &Value) -> Vec<String> {
    page["bookmarks"]
        .as_array()
        .expect("bookmarks")
        .iter()
        .map(|b| b["subject"]["uri"].as_str().expect("uri").to_string())
        .collect()
}

/// T-8 (P-8), in On and in Off (T-1): `createBookmark` answers an existing
/// unreadable and a nonexistent private URI with identical bytes and no
/// pipeline read; a URI that is not a private post's is 400; writes are
/// idempotent; the cap counts private bookmarks alone (C-12);
/// `getBookmarks` is newest first, floored, and omits a post once its
/// audience drops the caller while its cursor still advances;
/// `viewer.bookmarked` is the reader's own; the public `createBookmark`
/// still refuses a private URI; the purges remove a member's rows.
async fn bookmarks_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let first = f.private_post(ALICE, "3kfirst22222", &aud, None).await;
    let second = f.private_post(ALICE, "3ksecond2222", &aud, None).await;
    // Carol's post exists, and Bob is not in its audience.
    let carols = f.audience(CAROL, "3kaudcarol22", Some("list"), &[DAVE]).await;
    let hidden = f.private_post(CAROL, "3khidden2222", &carols, None).await;
    let missing = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere222");
    f.ready_all().await;
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    let base = serve(f.state_of(app, Some(Duration::from_secs(1)), FLOOR)).await;

    // The cap counts private bookmarks alone: Bob's three public ones (the
    // test cap is 3) leave room.
    let bob = crate::hydration::Viewer {
        did: Some(BOB.into()),
        hydrate: true,
    };
    for rkey in ["3kpub1", "3kpub2", "3kpub3"] {
        let public = format!("at://{ALICE}/app.bsky.feed.post/{rkey}");
        f.app.hydrator.create_bookmark(&bob, &public, "bafy").await.expect("db").expect("public bookmark");
    }
    // The public method still refuses a private URI.
    let refused = f.app.hydrator.create_bookmark(&bob, &first, "bafy").await.expect("db").expect_err("private");
    assert_eq!(refused.error, "UnsupportedCollection");

    // No post is read on a write: an existing post Bob cannot read and a
    // URI that names nothing get the same bytes.
    let reads = PipelineReads::default();
    let guard = metrics::set_default_local_recorder(&reads);
    let unreadable = post(&base, CREATE_BOOKMARK, &json!({"uri": hidden, "cid": "bafyhidden"}), BOB).await;
    let nonexistent = post(&base, CREATE_BOOKMARK, &json!({"uri": missing, "cid": "bafymissing"}), BOB).await;
    assert_eq!(unreadable, (200, "{}".to_string()));
    assert_eq!(unreadable, nonexistent);
    let readable = post(&base, CREATE_BOOKMARK, &json!({"uri": first, "cid": "bafyold"}), BOB).await;
    assert_eq!(readable, unreadable);
    assert_eq!(post(&base, DELETE_BOOKMARK, &json!({"uri": missing}), BOB).await, nonexistent);
    assert_eq!(reads.count(), 0, "no write reads a post");
    // The counter does see a read.
    one_post(&base, BOB, &first).await;
    assert!(reads.count() > 0, "getPosts reads through the pipeline");
    drop(guard);
    post(&base, CREATE_BOOKMARK, &json!({"uri": missing, "cid": "bafymissing"}), BOB).await;

    // Only a postPrivate record URI, decided from the URI alone.
    for bad in [
        format!("at://{ALICE}/app.bsky.feed.post/3kfirst22222"),
        aud.clone(),
        format!("at://alice.test/{NSID_POST_PRIVATE}/3kfirst22222"),
        format!("{first}/extra"),
        "nonsense".to_string(),
    ] {
        for method in [CREATE_BOOKMARK, DELETE_BOOKMARK] {
            let (status, body) = post(&base, method, &json!({"uri": bad, "cid": "bafy"}), BOB).await;
            assert_eq!(status, 400, "{method} {bad}: {body}");
            assert!(body.contains("InvalidRequest"), "{body}");
        }
    }
    let (status, body) = post(&base, CREATE_BOOKMARK, &json!({"uri": second}), BOB).await;
    assert_eq!((status, body.contains("cid")), (400, true), "{body}");

    // Bob holds three (hidden, missing, first): a repeat is allowed at the
    // cap and replaces the CID; a fourth post is not.
    assert_eq!(post(&base, CREATE_BOOKMARK, &json!({"uri": first, "cid": "bafyfirst"}), BOB).await.0, 200);
    let (status, body) = post(&base, CREATE_BOOKMARK, &json!({"uri": second, "cid": "bafysecond"}), BOB).await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("InvalidRequest") && body.contains("bookmark limit reached"), "{body}");
    // Deleting is idempotent and makes room.
    for _ in 0..2 {
        assert_eq!(post(&base, DELETE_BOOKMARK, &json!({"uri": missing}), BOB).await, (200, "{}".to_string()));
    }
    assert_eq!(post(&base, CREATE_BOOKMARK, &json!({"uri": second, "cid": "bafysecond"}), BOB).await.0, 200);
    // Fixed times, so the order is known: hidden, then first, then second.
    for (uri, at) in [(&hidden, "2026-10-01T00:00:00Z"), (&first, "2026-10-02T00:00:00Z"), (&second, "2026-10-03T00:00:00Z")] {
        sqlx::query("UPDATE private_bookmarks SET created_at = $2::timestamptz WHERE viewer_did = $1 AND post_uri = $3")
            .bind(BOB)
            .bind(at)
            .bind(uri)
            .execute(&f.pool)
            .await
            .expect("time");
    }

    // Newest first; the unreadable post is omitted; floored.
    let (page, took) = bookmarks_of(&base, BOB, "").await;
    assert!(took >= FLOOR, "getBookmarks took {took:?}");
    assert_eq!(subjects(&page), [second.clone(), first.clone()]);
    assert!(page.get("cursor").is_none(), "{page}");
    let b = &page["bookmarks"][1];
    assert_eq!(b["subject"], json!({"uri": first, "cid": "bafyfirst"}));
    assert_eq!(b["createdAt"], "2026-10-02T00:00:00.000Z");
    assert_eq!((&b["item"]["uri"], &b["item"]["record"]["text"]), (&json!(first), &json!("3kfirst22222")));
    assert_eq!(b["item"]["viewer"], json!({"bookmarked": true}));

    // `viewer.bookmarked` is the reader's own.
    assert_eq!(one_post(&base, BOB, &first).await.1["viewer"], json!({"bookmarked": true}));
    assert_eq!(one_post(&base, ALICE, &first).await.1["viewer"], json!({"bookmarked": false}));

    // The cursor walks Bob's rows: the page holding only the unreadable
    // row is empty and still carries a cursor; then the rows end.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let query = cursor.as_ref().map_or("limit=1".to_string(), |c| format!("limit=1&cursor={}", urlencoding(c)));
        let (page, _) = bookmarks_of(&base, BOB, &query).await;
        pages += 1;
        seen.extend(subjects(&page));
        match page.get("cursor").and_then(Value::as_str) {
            Some(c) => cursor = Some(c.to_string()),
            None => {
                assert_eq!(page["bookmarks"], json!([]), "the last page is past the last row");
                break;
            }
        }
        assert!(pages < 10, "the cursor advances");
    }
    assert_eq!((seen, pages), (vec![second.clone(), first.clone()], 4));

    // Alice moves her audience to Carol: both of Bob's posts leave his
    // list, and a full page of rows still carries a cursor.
    let moved = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [CAROL], "createdAt": "2026-09-26T00:00:00Z"});
    f.apply_op(ALICE, Action::Update, NSID_AUDIENCE, "3kaudience22", REV2, Some(moved)).await;
    let (page, _) = bookmarks_of(&base, BOB, "").await;
    assert_eq!(page, json!({"bookmarks": []}));
    let (page, _) = bookmarks_of(&base, BOB, "limit=2").await;
    assert_eq!(page["bookmarks"], json!([]));
    assert!(page["cursor"].is_string(), "{page}");
    let (page, _) = bookmarks_of(&base, BOB, &format!("limit=2&cursor={}", urlencoding(page["cursor"].as_str().expect("cursor")))).await;
    assert_eq!(page, json!({"bookmarks": []}), "one row left");

    // Bad parameters.
    for query in ["limit=0", "limit=101", "cursor=garbage"] {
        let (status, body, _) = get(&base, GET_BOOKMARKS, query, BOB, BOB_KEY).await;
        assert_eq!(status, 400, "{query}: {body}");
    }

    // Another member's bookmarks are its own; the purges remove a member's.
    let (page, _) = bookmarks_of(&base, CAROL, "").await;
    assert_eq!(page, json!({"bookmarks": []}));
    post(&base, CREATE_BOOKMARK, &json!({"uri": first, "cid": "bafy"}), CAROL).await;
    let held = |did: &'static str| {
        let pool = f.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM private_bookmarks WHERE viewer_did = $1")
                .bind(did)
                .fetch_one(&pool)
                .await
                .expect("count")
        }
    };
    assert_eq!((held(BOB).await, held(CAROL).await), (3, 1));
    let mut conn = f.pool.acquire().await.expect("conn");
    KryphocronPrivateTier.purge_member_removed_batch(&mut conn, BOB, 100).await.expect("purge");
    KryphocronPrivateTier.purge_did_batch(&mut conn, CAROL, 100).await.expect("purge");
    drop(conn);
    assert_eq!((held(BOB).await, held(CAROL).await), (0, 0));
    db.drop_db().await;
}

/// A cursor as a query value (it holds `:` and `/`).
fn urlencoding(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[tokio::test]
async fn private_bookmarks_on() {
    bookmarks_in(PublicMode::On, "s3-bookmarks-on").await;
}

#[tokio::test]
async fn private_bookmarks_off() {
    bookmarks_in(PublicMode::Off, "s3-bookmarks-off").await;
}

/// T-3 (P-19) for the methods routed here: with the private router merged
/// into the app as `serve` merges it, each private-posting method in
/// `app.nearhorizon.private.*` is kept, answers its own authentication
/// error for its verb and the wrong-verb 400 for the other, and serves a
/// ready member, in each of the five public-indexing modes.
#[tokio::test]
async fn private_router_methods_are_routed_in_every_mode() {
    use crate::hydration::testing::set_mode;
    use crate::xrpc::router::{FinalConfig, finalize};

    let Some(db) = fresh_db("s3_private_routes").await else {
        return;
    };
    let f = fixture(&db.pool, "s3-routes").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let uri = f.private_post(ALICE, "3kpost222222", &aud, None).await;
    f.ready_all().await;
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    let merged = crate::xrpc::router(app.clone()).merge(router(f.state_of(app.clone(), Some(Duration::from_secs(1)), FLOOR)));
    let routes = finalize(merged, app.clone(), FinalConfig {
        not_ready_retry_secs: 5,
        public: crate::honest_501::PublicMessages::default(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, routes.into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let methods: Vec<(&str, crate::honest_501::Verb)> = crate::honest_501::PRIVATE_POSTING_METHODS
        .iter()
        .copied()
        .filter(|(n, _)| n.starts_with("app.nearhorizon.private."))
        .collect();
    assert_eq!(methods.len(), 3);
    for mode in [PublicMode::On, PublicMode::Off, PublicMode::Draining, PublicMode::Held, PublicMode::Filling] {
        set_mode(app.hydrator.scope(), mode).await;
        for &(nsid, verb) in &methods {
            assert!(crate::honest_501::KEPT_METHODS.contains(&nsid), "{nsid}");
            let url = format!("{base}/xrpc/{nsid}");
            let (right, wrong) = match verb {
                crate::honest_501::Verb::Get => (reqwest::Method::GET, reqwest::Method::POST),
                crate::honest_501::Verb::Post => (reqwest::Method::POST, reqwest::Method::GET),
            };
            let r = client.request(right.clone(), &url).send().await.expect("request");
            let status = r.status().as_u16();
            let v: Value = r.json().await.expect("json");
            assert_eq!((status, v["error"].as_str()), (401, Some("AuthRequired")), "{mode:?} {nsid}");
            let r = client.request(wrong.clone(), &url).send().await.expect("request");
            let status = r.status().as_u16();
            let v: Value = r.json().await.expect("json");
            assert_eq!(status, 400, "{mode:?} wrong verb {nsid}");
            assert_eq!(v["message"], format!("Incorrect HTTP method ({wrong}) expected {right}"), "{mode:?} {nsid}");
            // A ready member is served.
            let status = match verb {
                crate::honest_501::Verb::Get => get(&base, nsid, "", BOB, BOB_KEY).await.0,
                crate::honest_501::Verb::Post => post(&base, nsid, &json!({"uri": uri, "cid": "bafy"}), BOB).await.0,
            };
            assert_eq!(status, 200, "{mode:?} {nsid}");
        }
    }
    db.drop_db().await;
}

const THREADGATE: &str = "app.bsky.feed.threadgate";
const POSTGATE: &str = "app.bsky.feed.postgate";
const MENTION_RULE: &str = "app.bsky.feed.threadgate#mentionRule";
const FOLLOWING_RULE: &str = "app.bsky.feed.threadgate#followingRule";
const DISABLE_RULE: &str = "app.bsky.feed.postgate#disableRule";

/// A facet mentioning `did`.
fn mention(did: &str) -> Value {
    json!({"index": {"byteStart": 0, "byteEnd": 1}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": did}]})
}

/// A stock record embed of `uri`.
fn quote_of(uri: &str) -> Value {
    json!({"embed": {"$type": "app.bsky.embed.record", "record": {"uri": uri, "cid": "bafyany"}}})
}

/// The URIs stored in a private gate table, sorted.
async fn gate_rows(pool: &PgPool, table: &str) -> Vec<String> {
    sqlx::query_scalar(&format!("SELECT uri FROM {table} ORDER BY uri"))
        .fetch_all(pool)
        .await
        .expect(table)
}

async fn public_gate_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT (SELECT count(*) FROM threadgates) + (SELECT count(*) FROM postgates)")
        .fetch_one(pool)
        .await
        .expect("count")
}

/// `getPosts(uris)` for `reader`: the raw body and the posts.
async fn posts_of(base: &str, reader: &str, uris: &[&str]) -> (String, Vec<Value>) {
    let query: Vec<String> = uris.iter().map(|u| format!("uris={u}")).collect();
    let (status, body, _) = get(base, POSTS, &query.join("&"), reader, key_of(reader)).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    (body.clone(), v["posts"].as_array().cloned().unwrap_or_default())
}

/// `getPostThread` for `reader` with `query`: the raw body and the thread.
async fn thread_of(base: &str, reader: &str, query: &str) -> (String, Value) {
    let (status, body, _) = get(base, THREAD, query, reader, key_of(reader)).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    (body, v["thread"].clone())
}

/// Every reply URI under a thread node, depth first.
fn reply_uris(node: &Value) -> Vec<String> {
    let mut out = Vec::new();
    for child in node["replies"].as_array().into_iter().flatten() {
        out.push(child["post"]["uri"].as_str().expect("uri").to_string());
        out.extend(reply_uris(child));
    }
    out
}

/// T-9 and T-10 (P-18), in On and in Off (T-1), with the request's "Done
/// when": Alice posts to her `followers` audience with a threadgate
/// `allow: [mentionRule]` naming Carol; Bob, a follower she does not
/// mention, gets `replyDisabled: true` and Carol `false`; her postgate's
/// `disableRule` gives Bob `embeddingDisabled: true`.
///
/// Storage: the gates are written by the private tier in both modes (not
/// `public_off`), a write answers as `requestRecord`'s `indexed` does, a
/// gate with another rkey or author is stored and ignored, a gate delete
/// and a post delete remove them. Reads: `replyDisabled` only when the
/// claimed root was read in the request (a feed reply carries none; a
/// thread reads its root); `hiddenReplies` hides from others, not from the
/// reply's author; `allow: []`; `followingRule` with no stored follow is
/// left out; detached quotes, checked only after the quoted read; no gate
/// admits a reader the oracle denies; and review 1's LB3 case.
async fn gates_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("followers"), &[]).await;
    for who in [BOB, CAROL] {
        exec(&f.pool, &follow_sql(who, ALICE)).await;
    }
    // Alice's root mentions Carol and Dave; Dave does not follow her.
    let root = f.post_with(ALICE, "3kroot222222", &aud, json!({"facets": [mention(CAROL), mention(DAVE)]})).await;
    let reply = f.private_post(BOB, "3kreply22222", &aud, Some((&root, &root))).await;
    let hidden = f.private_post(CAROL, "3khidden2222", &aud, Some((&root, &reply))).await;
    let bob_aud = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL, DAVE]).await;
    let detached_quote = f.post_with(BOB, "3kdetached22", &bob_aud, quote_of(&root)).await;
    let kept_quote = f.post_with(BOB, "3kkeptquote2", &bob_aud, quote_of(&root)).await;
    let following = f.post_with(ALICE, "3kfollowing2", &aud, json!({})).await;

    // The threadgate, written as `requestRecord` writes it: `indexed`.
    let gate = json!({"$type": THREADGATE, "post": root, "allow": [{"$type": MENTION_RULE}], "hiddenReplies": [hidden],
        "createdAt": "2026-09-26T12:00:00Z"});
    let report = f
        .writer
        .apply(
            ALICE,
            REV,
            &[RawOp {
                action: Action::Create,
                collection: THREADGATE.into(),
                rkey: "3kroot222222".into(),
                cid: Some("bafygate".into()),
                record: Some(gate),
            }],
            ApplyOptions {
                advance_applied_rev: false,
                dead_letter_readiness: false,
                attempts: 1,
                live: false,
                subscription_rules: false,
                source: WriteSource::IndexRequest,
            },
        )
        .await
        .expect("apply");
    assert!(matches!(report.outcomes[..], [OpOutcome::Written]), "{report:?}");
    f.ingest(ALICE, POSTGATE, "3kroot222222", json!({"$type": POSTGATE, "post": root,
        "embeddingRules": [{"$type": DISABLE_RULE}], "detachedEmbeddingUris": [detached_quote], "createdAt": "2026-09-26T12:00:00Z"}))
        .await;
    f.ingest(ALICE, THREADGATE, "3kfollowing2", json!({"$type": THREADGATE, "post": following, "allow": [{"$type": FOLLOWING_RULE}]}))
        .await;
    // Stored and ignored: another rkey, and another author.
    f.ingest(ALICE, THREADGATE, "3kotherkey22", json!({"$type": THREADGATE, "post": root, "allow": []})).await;
    f.ingest(BOB, THREADGATE, "3kroot222222", json!({"$type": THREADGATE, "post": root, "allow": []})).await;
    let canonical = format!("at://{ALICE}/{THREADGATE}/3kroot222222");
    assert_eq!(gate_rows(&f.pool, "private_threadgates").await, [
        format!("at://{ALICE}/{THREADGATE}/3kfollowing2"),
        format!("at://{ALICE}/{THREADGATE}/3kotherkey22"),
        canonical.clone(),
        format!("at://{BOB}/{THREADGATE}/3kroot222222"),
    ]);
    assert_eq!(gate_rows(&f.pool, "private_postgates").await, [format!("at://{ALICE}/{POSTGATE}/3kroot222222")]);
    assert_eq!(public_gate_count(&f.pool).await, 0, "a private post's gates are not public rows");
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    // Done when: Bob is not mentioned, Carol is; Bob may not embed.
    let (_, p) = posts_of(&base, BOB, &[&root]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false, "replyDisabled": true, "embeddingDisabled": true}), "{p:?}");
    let tg = &p[0]["threadgate"];
    assert_eq!((tg["uri"].as_str(), tg["cid"].as_str()), (Some(canonical.as_str()), Some("bafygate")));
    assert_eq!((&tg["record"]["allow"], &tg["lists"]), (&json!([{"$type": MENTION_RULE}]), &json!([])));
    let (_, p) = posts_of(&base, CAROL, &[&root]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false, "replyDisabled": false, "embeddingDisabled": true}));
    // The author: never disabled.
    let (_, p) = posts_of(&base, ALICE, &[&root]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false, "replyDisabled": false}));
    // Dave is mentioned but not a follower: the gate admits no one the
    // oracle denies, and his answer is a nonexistent post's.
    let (denied, p) = posts_of(&base, DAVE, &[&root]).await;
    assert!(p.is_empty());
    let (nothing, _) = posts_of(&base, DAVE, &[&format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere222")]).await;
    assert_eq!(denied, nothing);

    // A reply whose root was not read in the request: no field. Read with
    // its root: the root's gate applies.
    let (_, p) = posts_of(&base, CAROL, &[&reply]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false}));
    let (_, body, _) = get(&base, FEED, &format!("actor={BOB}"), CAROL, CAROL_KEY).await;
    let feed: Value = serde_json::from_str(&body).expect("json");
    let item = feed["feed"].as_array().expect("feed").iter().find(|i| i["uri"] == reply.as_str()).expect("in the feed");
    assert!(item["viewer"].get("replyDisabled").is_none(), "{item}");
    let (_, p) = posts_of(&base, BOB, &[&reply, &root]).await;
    assert_eq!(p[0]["viewer"]["replyDisabled"], true);
    assert!(p[0].get("threadgate").is_none(), "a reply carries no threadgate");
    let (_, p) = posts_of(&base, CAROL, &[&reply, &root]).await;
    assert_eq!(p[0]["viewer"]["replyDisabled"], false);

    // A thread reads its root: with no parents shown, the handler reads it
    // itself. The hidden reply is left out for Bob, not for Carol, its
    // author.
    let (_, t) = thread_of(&base, BOB, &format!("uri={reply}&parentHeight=0")).await;
    assert!(t.get("parent").is_none(), "{t}");
    assert_eq!(t["post"]["viewer"]["replyDisabled"], true);
    assert_eq!(reply_uris(&t), Vec::<String>::new(), "hidden from Bob");
    let (_, t) = thread_of(&base, CAROL, &format!("uri={reply}&parentHeight=0")).await;
    assert_eq!(t["post"]["viewer"]["replyDisabled"], false);
    assert_eq!(reply_uris(&t), std::slice::from_ref(&hidden), "not hidden from its author");
    let (_, t) = thread_of(&base, BOB, &format!("uri={root}")).await;
    assert_eq!(reply_uris(&t), std::slice::from_ref(&reply));
    assert_eq!(t["post"]["threadgate"]["uri"], canonical.as_str());
    assert_eq!(t["replies"][0]["post"]["viewer"]["replyDisabled"], true);
    let (_, t) = thread_of(&base, CAROL, &format!("uri={root}")).await;
    assert_eq!(reply_uris(&t), [reply.clone(), hidden.clone()]);

    // `followingRule` with no stored follow: left out; with one: allowed.
    let (_, p) = posts_of(&base, BOB, &[&following]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false}));
    exec(&f.pool, &follow_sql(ALICE, BOB)).await;
    let (_, p) = posts_of(&base, BOB, &[&following]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false, "replyDisabled": false}));

    // Detached: only the quote the postgate names, and only for a reader
    // who can read the quoted post. Dave cannot: both quotes are the same
    // `#viewNotFound`.
    let (_, p) = posts_of(&base, CAROL, &[&detached_quote, &kept_quote]).await;
    assert_eq!(p[0]["embed"]["record"], json!({"$type": "app.bsky.embed.record#viewDetached", "uri": root, "detached": true}));
    assert_eq!(p[1]["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord");
    let (_, p) = posts_of(&base, DAVE, &[&detached_quote, &kept_quote]).await;
    assert_eq!(p[0]["embed"], p[1]["embed"]);
    assert_eq!(p[0]["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound");

    // Deleting the postgate: quoting and embedding are open again.
    f.apply_op(ALICE, Action::Delete, POSTGATE, "3kroot222222", REV2, None).await;
    assert!(gate_rows(&f.pool, "private_postgates").await.is_empty());
    let (_, p) = posts_of(&base, BOB, &[&root]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false, "replyDisabled": true}));
    let (_, p) = posts_of(&base, CAROL, &[&detached_quote]).await;
    assert_eq!(p[0]["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord");

    // Review 1, LB3: Dave reads Carol's reply R, which claims Alice's root X
    // that Dave cannot read. X's gate (`allow: []`, hiding R's child) never
    // shows in Dave's answers: byte-identical before X is written, while it
    // and its gate exist, and after X is deleted (its gate with it).
    let list_aud = f.audience(ALICE, "3kaudlist222", Some("list"), &[BOB, CAROL]).await;
    let carol_aud = f.audience(CAROL, "3kaudcarol22", Some("list"), &[DAVE, BOB]).await;
    let x = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3klb3root222");
    let r = f.private_post(CAROL, "3klb3reply22", &carol_aud, Some((&x, &x))).await;
    let child = f.private_post(BOB, "3klb3child22", &carol_aud, Some((&x, &r))).await;
    let dave_sees = || async {
        let (posts, p) = posts_of(&base, DAVE, &[&r]).await;
        assert_eq!(p[0]["viewer"], json!({"bookmarked": false}));
        let (thread, t) = thread_of(&base, DAVE, &format!("uri={r}")).await;
        assert_eq!(reply_uris(&t), std::slice::from_ref(&child));
        assert!(t["post"]["viewer"].get("replyDisabled").is_none(), "{t}");
        (posts, thread)
    };
    let never_written = dave_sees().await;
    assert_eq!(f.private_post(ALICE, "3klb3root222", &list_aud, None).await, x);
    f.ingest(ALICE, THREADGATE, "3klb3root222", json!({"$type": THREADGATE, "post": x, "allow": [], "hiddenReplies": [child]}))
        .await;
    assert_eq!(dave_sees().await, never_written);
    // `allow: []`: disabled for others, not for the author.
    let (_, p) = posts_of(&base, BOB, &[&x]).await;
    assert_eq!(p[0]["viewer"]["replyDisabled"], true);
    let (_, p) = posts_of(&base, ALICE, &[&x]).await;
    assert_eq!(p[0]["viewer"]["replyDisabled"], false);
    // Carol reads X, so in the thread of R its gate hides Bob's reply from
    // her; Bob, its author, still sees it.
    let (_, t) = thread_of(&base, CAROL, &format!("uri={r}")).await;
    assert_eq!(reply_uris(&t), Vec::<String>::new());
    assert_eq!(t["post"]["viewer"]["replyDisabled"], true);
    let (_, t) = thread_of(&base, BOB, &format!("uri={r}")).await;
    assert_eq!(reply_uris(&t), std::slice::from_ref(&child));
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3klb3root222", REV2, None).await;
    assert!(!gate_rows(&f.pool, "private_threadgates").await.contains(&format!("at://{ALICE}/{THREADGATE}/3klb3root222")));
    assert_eq!(dave_sees().await, never_written);

    // A deleted post deletes its canonical gate; the ignored ones stay.
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3kroot222222", REV2, None).await;
    assert_eq!(gate_rows(&f.pool, "private_threadgates").await, [
        format!("at://{ALICE}/{THREADGATE}/3kfollowing2"),
        format!("at://{ALICE}/{THREADGATE}/3kotherkey22"),
        format!("at://{BOB}/{THREADGATE}/3kroot222222"),
    ]);
    // The account purge removes the rest of an author's gates.
    let mut conn = f.pool.acquire().await.expect("conn");
    KryphocronPrivateTier.purge_did_batch(&mut conn, ALICE, 100).await.expect("purge");
    drop(conn);
    assert_eq!(gate_rows(&f.pool, "private_threadgates").await, [format!("at://{BOB}/{THREADGATE}/3kroot222222")]);
    db.drop_db().await;
}

#[tokio::test]
async fn private_gates_on() {
    gates_in(PublicMode::On, "s4-gates-on").await;
}

#[tokio::test]
async fn private_gates_off() {
    gates_in(PublicMode::Off, "s4-gates-off").await;
}

/// §3.1's lifecycle: an `out_of_scope` purge takes the DID's gates with its
/// private posts, and keeps the gate of a kept class-M reply.
#[tokio::test]
async fn out_of_scope_purge_takes_gates_with_posts() {
    let Some(db) = fresh_db("s4_gates_purge").await else {
        return;
    };
    let f = fixture(&db.pool, "s4-gates-purge").await;
    let aud = f.audience(DAVE, "3kauddave222", Some("list"), &[BOB]).await;
    let own = f.private_post(DAVE, "3kown2222222", &aud, None).await;
    // A reply to a member (class M) stays, and so does its gate. The
    // fixture's members are Alice to Dave, so the parent's author is made a
    // member and Dave is not.
    exec(&f.pool, &format!("DELETE FROM members WHERE did = '{DAVE}'")).await;
    let alices = f.audience(ALICE, "3kaudalice22", Some("list"), &[DAVE]).await;
    let parent = f.private_post(ALICE, "3kparent2222", &alices, None).await;
    let kept = f.private_post(DAVE, "3kkept222222", &aud, Some((&parent, &parent))).await;
    for (rkey, post) in [("3kown2222222", &own), ("3kkept222222", &kept)] {
        f.ingest(DAVE, THREADGATE, rkey, json!({"$type": THREADGATE, "post": post, "allow": []})).await;
    }
    let mut conn = f.pool.acquire().await.expect("conn");
    while KryphocronPrivateTier.purge_out_of_scope_batch(&mut conn, DAVE, 100).await.expect("purge") > 0 {}
    drop(conn);
    assert_eq!(gate_rows(&f.pool, "private_threadgates").await, [format!("at://{DAVE}/{THREADGATE}/3kkept222222")]);
    db.drop_db().await;
}
