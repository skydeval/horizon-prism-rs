//! End-to-end private reads (design-v2 §6.3, §7.9, Phase B B-5): records
//! encoded with the real at-rest hooks, ingested through the firehose writer,
//! then read over HTTP through the private router.

// S10, the `following` audience mode, on this module's fixtures.
mod following;
// S12, private threads and branches, on this module's fixtures: the read
// rule, the one builder, the head round, and B-5's release gate.
mod b5;
// B-5 part (3) on the deploy build (private-threads amendment 1 §2).
mod b5_release;
mod builder;
// Facets and embed carried in `encodedContent` (#504).
mod embeds;
// The read-time clear-field check, the row snapshot and the author's
// `droppedFields` (private-encode design E2).
mod index_check;
// The admin UI's live feed shows no private content (T-F4).
mod live_feed;
// S4, plaintext refused while `private_any_pds.enabled` (private-encode E4).
mod plaintext;
// Media kinds and the media filters on the author feed (P-30).
mod media;
// Private blocks only with a member side (member-blocks-only design).
mod member_blocks;
// Private notification preferences (private-notification-prefs design).
mod prefs;
mod round;
mod threads;
// The shared walk (#720): T-60 to T-63.
mod walk;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
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
use crate::config::{
    PrivateReadConfig, PrivateReadFloors, PrivateSearchConfig, ScopePolicy, SyncConfig, VideoConfig, VideoPlayback,
};
use crate::cursor_key::CursorKey;
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
/// `private_read.max_chain`'s default, the walk's cap (private-threads
/// design §2.3), for the statements tests run directly.
const CAP: u32 = 16;
/// The private cursor key of every test state.
const TEST_CURSOR_KEY: [u8; 32] = [7; 32];

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
    /// The prober's per-host buckets (T-56: a refused token).
    budgets: HostBudgets,
}

/// The first signing key of a [`fixture_custom`] extra member.
const EXTRA_KEYS: u8 = 10;

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
    fixture_custom(pool, test, mode, &[], Arc::new(NoHeads)).await
}

/// The fixture with `extra` members beside the four (each its own handle),
/// and heads from `heads` (private-threads T-56: a recording source).
async fn fixture_custom(pool: &PgPool, test: &str, mode: PublicMode, extra: &[String], heads: Arc<dyn HeadSource>) -> Fixture {
    let mut everyone: Vec<(String, String)> =
        [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test"), (DAVE, "dave.test")].map(|(d, h)| (d.to_string(), h.to_string())).into();
    everyone.extend(extra.iter().map(|d| (d.clone(), format!("{}.test", &d[8..]))));
    for (did, handle) in &everyone {
        let (did, handle) = (did.as_str(), handle.as_str());
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
    for (d, _) in &everyone {
        sets.scope.insert(d.clone());
        sets.members.insert(d.clone());
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
    for (i, d) in extra.iter().enumerate() {
        fetcher.set_key(d, signing_key(u8::try_from(i).expect("small") + EXTRA_KEYS));
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
    let budgets = HostBudgets::new(BudgetRates {
        max_probe_per_host: 25.0,
        head_max_age_secs: 60.0,
        read_fetch_per_host: 10.0,
        backfill_per_host: 5.0,
        catch_up_per_host: 10.0,
    });
    let prober = HeadProber::new(pool.clone(), scope, private, heads, budgets.clone(), SyncConfig::default());
    Fixture {
        pool: pool.clone(),
        writer,
        glue,
        app,
        prober,
        sets,
        budgets,
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
        self.state_of_keyed(app, watermark_age, floor, &TEST_CURSOR_KEY)
    }

    fn state_of_keyed(&self, app: AppState, watermark_age: Option<Duration>, floor: Duration, cursor_key: &[u8; 32]) -> PrivateState {
        self.state_read(app, watermark_age, floor, cursor_key, &PrivateReadConfig::default())
    }

    /// A fresh watermark, no floor, and `[private_read]`'s walk and paging
    /// keys from `read` (private-threads design §5: the cap, the read
    /// factor, the batch bound).
    fn state_bounded(&self, read: &PrivateReadConfig) -> PrivateState {
        self.state_read(self.app.clone(), Some(Duration::from_secs(1)), Duration::ZERO, &TEST_CURSOR_KEY, read)
    }

    fn state_read(
        &self,
        app: AppState,
        watermark_age: Option<Duration>,
        floor: Duration,
        cursor_key: &[u8; 32],
        read: &PrivateReadConfig,
    ) -> PrivateState {
        let watermarks = Watermarks::new(BTreeMap::new(), vec![RELAY.into()]);
        if let Some(age) = watermark_age {
            watermarks.observe(RELAY, Utc::now() - chrono::Duration::from_std(age).expect("age"));
        }
        let ms = u64::try_from(floor.as_millis()).expect("ms");
        let cfg = PrivateReadConfig {
            bind_concurrency: 8,
            membership_retry_after_secs: 5,
            membership_valid_for_secs: 3600,
            max_chain: read.max_chain,
            page_reads_factor: read.page_reads_factor,
            walk_batch_max: read.walk_batch_max,
            floor_ms: PrivateReadFloors {
                get_posts: ms,
                get_post_thread: ms,
                get_author_feed: ms,
                get_timeline: ms,
                get_bookmarks: ms,
                get_likes: ms,
                get_reposted_by: ms,
                get_quotes: ms,
                list_notifications: ms,
                get_unread_count: ms,
                get_replies: ms,
                search_posts: ms,
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
            CursorKey::new(cursor_key),
        )
    }

    /// [`Self::state`] with another private cursor key (a rotated key, T-19).
    fn state_keyed(&self, cursor_key: &[u8; 32]) -> PrivateState {
        self.state_of_keyed(self.app.clone(), Some(Duration::from_secs(1)), FLOOR, cursor_key)
    }

    /// Encode `text` with the real hooks, as Aurora-Locus would write it.
    async fn encoded_post(&self, owner: &str, rkey: &str, text: &str, audience: Option<&str>) -> Value {
        self.encoded_bytes(owner, rkey, text.as_bytes(), audience).await
    }

    /// Encode the payload `bytes` with the real hooks: [`Self::encoded_post`]
    /// for any payload, text or not (#504's new form).
    async fn encoded_bytes(&self, owner: &str, rkey: &str, bytes: &[u8], audience: Option<&str>) -> Value {
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
            bytes,
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

/// [`serve`], with the private watermark kept a second old and fresh heads
/// kept fresh while the test runs, so a long test never reads past
/// `freshness_bound` or `head_max_age_secs`. A head a test made stale on
/// purpose (checked at the epoch) stays stale.
async fn serve_live(state: PrivateState) -> String {
    let watermarks = state.env.watermarks.clone();
    let pool = state.env.pool.clone();
    tokio::spawn(async move {
        loop {
            watermarks.observe(RELAY, Utc::now() - chrono::Duration::seconds(1));
            let _ = sqlx::query("UPDATE sync_state SET head_checked_at = now() WHERE head_checked_at > now() - interval '50 seconds'")
                .execute(&pool)
                .await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    serve(state).await
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
    oracle_admits_capped(pool, reader, uri, CAP).await
}

/// [`oracle_admits`] with the walk's cap `cap`.
async fn oracle_admits_capped(pool: &PgPool, reader: &str, uri: &str, cap: u32) -> bool {
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
            max_chain: cap,
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
    /// `private_read.max_chain` for the oracle, the prefilter and the
    /// server (T-50: a change applies on the next read).
    cap: u32,
}

impl Walk {
    async fn new(test: &str, layout: Layout, mode: Option<&str>, members: &[&str]) -> Option<Self> {
        Self::new_in(test, PublicMode::On, layout, mode, members, &[], Arc::new(NoHeads)).await
    }

    /// [`Walk::new`] with public indexing in `public`, `extra` members and
    /// heads from `heads` (private threads, T-46 to T-59), and rate limits no
    /// test reaches.
    async fn new_in(
        test: &str,
        public: PublicMode,
        layout: Layout,
        mode: Option<&str>,
        members: &[&str],
        extra: &[String],
        heads: Arc<dyn HeadSource>,
    ) -> Option<Self> {
        let db = fresh_db(test).await?;
        let mut f = fixture_custom(&db.pool, test, public, extra, heads).await;
        f.app.limits = roomy_limits();
        if layout == Layout::OffPds {
            exec(&f.pool, &format!("UPDATE actors SET pds_endpoint = 'https://pds.elsewhere.net' WHERE did <> '{ALICE}'")).await;
        }
        let aud = f.audience(ALICE, "3kaudmain222", mode, members).await;
        let root = f.private_post(ALICE, "3kroot222222", &aud, None).await;
        for did in [BOB, CAROL, DAVE] {
            exec(&f.pool, &follow_sql(did, ALICE)).await;
        }
        let base = serve_live(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
        Some(Self {
            db,
            f,
            base,
            aud,
            root,
            failures: Vec::new(),
            cap: CAP,
        })
    }

    /// Read with the walk's cap `cap` from here on: a new server, and the
    /// oracle and prefilter at the same cap.
    async fn set_cap(&mut self, cap: u32) {
        self.cap = cap;
        let read = PrivateReadConfig { max_chain: cap, ..PrivateReadConfig::default() };
        self.base = serve_live(self.f.state_bounded(&read)).await;
    }

    /// `author`'s reply to `parent` under Alice's audience, claiming her root.
    async fn reply(&self, author: &str, rkey: &str, parent: &str) -> String {
        self.f.private_post(author, rkey, &self.aud, Some((&self.root, parent))).await
    }

    /// Whether the oracle, the prefilter and `getPosts` each give `reader`
    /// the `expected` answer for `uri` (T-37: one answer from all three).
    async fn expect(&mut self, case: &str, reader: &str, uri: &str, expected: bool) {
        let oracle = oracle_admits_capped(&self.f.pool, reader, uri, self.cap).await;
        let prefilter = !schema::prefilter_uris(&self.f.pool, reader, &[uri.to_string()], self.cap)
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
    // T-38 (S5): Bob's second reply in his own thread, quoting Alice's root.
    let mut quoting = w.f.post_record(BOB, "3kownquote22", &aud, Some((&top, &top))).await;
    quoting["embed"] = json!({"$type": "app.bsky.embed.record", "record": {"uri": root, "cid": "bafyroot"}});
    w.f.ingest(BOB, NSID_POST_PRIVATE, "3kownquote22", quoting).await;
    let own_quote = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kownquote22");
    w.f.ready_all().await;
    for (case, uri) in [("T-32", &top), ("T-38", &own_thread), ("T-38 quote", &own_quote), ("T-39", &forged), ("T-39 reply", &below)] {
        w.expect(case, BOB, uri, true).await;
        for reader in [ALICE, CAROL, DAVE] {
            w.expect(case, reader, uri, false).await;
        }
    }
    for reader in [ALICE, BOB, CAROL, DAVE] {
        w.expect("T-38 in Alice's thread", reader, &in_thread, true).await;
    }
    // T-38's counts (S5): the replies in Bob's own thread count in no one
    // else's replyCount of his post, and the quote in no one else's
    // quoteCount of Alice's root; for Bob, their reader, they count.
    for reader in [ALICE, BOB, CAROL, DAVE] {
        let counts = schema::counts(&w.f.pool, reader, std::slice::from_ref(&top), CAP).await.expect("counts");
        let replies = counts.get(&top).map_or(0, |c| c.reply);
        let (_, view) = one_post(&w.base, reader, &root).await;
        let quotes = view["quoteCount"].as_i64();
        let expected = if reader == BOB { (2, Some(1)) } else { (0, Some(0)) };
        if (replies, quotes) != expected {
            w.failures.push(format!("T-38 counts for {reader}: replies to T {replies}, Alice's quoteCount {quotes:?}"));
        }
    }
    // T-38's unread counts (S6): Bob's quote of Alice's root writes her a
    // `quote` row, which she cannot read, so it counts nowhere; her one
    // unread row is Bob's reply in her thread.
    for (reader, expected) in [(ALICE, 1), (BOB, 0), (CAROL, 0), (DAVE, 0)] {
        let unread = schema::unread_count(&w.f.pool, reader, CAP).await.expect("count");
        if unread != expected {
            w.failures.push(format!("T-38 unread count for {reader}: {unread}, expected {expected}"));
        }
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

/// T-33, as S12 restates it (private-threads design §6): a parent under
/// another of the owner's audiences, the cap from config, a cycle of forged
/// parents, and a reply left under the old audience when the root moved
/// (A-9), read while its writer stands.
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
    // S10: nine levels below Alice's root under her `following` audience,
    // every step by an author she follows; Bob's step at level 3 only.
    let follow_aud = w.f.audience(ALICE, "3kaudfollow2", Some("following"), &[]).await;
    let follow_root = w.f.private_post(ALICE, "3krootfollow", &follow_aud, None).await;
    let mut follow_chain = Vec::new();
    let mut parent = follow_root.clone();
    for level in 1..=9 {
        let author = if level == 3 { BOB } else { CAROL };
        let uri = w.f.private_post(author, &format!("3kfollow{level}2222"), &follow_aud, Some((&follow_root, &parent))).await;
        follow_chain.push((author, uri.clone()));
        parent = uri;
    }
    for did in [BOB, CAROL, DAVE] {
        exec(&w.f.pool, &follow_sql(ALICE, did)).await;
    }
    set_follows_verified(&w.f.pool, ALICE, true).await;
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

    // S12 (private-threads design §2.2, C-T10 (a)): `cross` names one of
    // Alice's audiences in her thread, whose root is under `other`; its
    // writer stands under `other` (Bob follows her), so `other`'s readers
    // read it.
    for reader in [ALICE, BOB, DAVE] {
        w.expect("parent under another audience", reader, &cross, true).await;
    }
    // The cap comes from config (S12, §2.3): at the default 16 all nine
    // levels are served; at 8, levels 1 to 8.
    for (cap, served) in [(CAP, 9), (8, 8)] {
        w.set_cap(cap).await;
        for (i, (author, uri)) in chain.iter().enumerate() {
            let level = i + 1;
            w.expect(&format!("cap {cap}, level {level}"), DAVE, uri, level <= served).await;
            w.expect(&format!("cap {cap}, level {level}, its author"), author, uri, true).await;
        }
        // S10: under `following`, the same.
        for (i, (author, uri)) in follow_chain.iter().enumerate() {
            let level = i + 1;
            w.expect(&format!("cap {cap}, following, level {level}"), DAVE, uri, level <= served).await;
            w.expect(&format!("cap {cap}, following, level {level}, its author"), author, uri, true).await;
        }
    }
    w.set_cap(CAP).await;
    for uri in [&cycle_a, &cycle_b, &into_cycle] {
        w.expect("cycle", DAVE, uri, false).await;
    }
    // Once Alice unfollows the level-3 author, levels 3 to 9 are read by
    // their authors only (the step at level 3 no longer stands).
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{ALICE}' AND subject_did = '{BOB}'")).await;
    for (i, (author, uri)) in follow_chain.iter().enumerate() {
        let level = i + 1;
        w.expect(&format!("following, level-3 author unfollowed, level {level}"), DAVE, uri, level < 3).await;
        w.expect(&format!("following, level-3 author unfollowed, level {level}, its author"), author, uri, true).await;
    }
    // A-9 (S12): a reply left under the old audience when the root moves is
    // read by the new audience while its writer stands under it (Bob
    // follows Alice, `other` is `followers`).
    w.expect("A-9, before the move", DAVE, &left, true).await;
    let moved = w.f.post_record(ALICE, "3kroot222222", &other, None).await;
    w.f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kroot222222", REV2, Some(moved)).await;
    w.expect("A-9, left under the old audience, its writer standing", DAVE, &left, true).await;
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

/// T-37: every mode, conditions 2 to 4 for the writer, and rule 6, in
/// S9's C-6 = yes version (condition 3 and rule 6 under every mode).
async fn modes_and_writers(test: &str, layout: Layout) {
    let Some(mut w) = Walk::new(test, layout, Some("followers"), &[]).await else {
        return;
    };
    let main_reply = w.reply(BOB, "3kmainreply2", &w.root.clone()).await;
    let cases: [(&str, Option<&str>, &[&str], bool); 6] = [
        ("list, writer listed", Some("list"), &[BOB, DAVE], true),
        ("NULL mode, writer listed", None, &[BOB, DAVE], true),
        ("list, writer not listed", Some("list"), &[DAVE], false),
        ("NULL mode, writer not listed", None, &[DAVE], false),
        ("everyone", Some("everyone"), &[], true),
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
    let (listed, null_mode, everyone) =
        (reply_of("list, writer listed"), reply_of("NULL mode, writer listed"), reply_of("everyone"));

    // S10 (following-audience design §2, §11 T-37): `following` reads
    // Alice's own follows and her follow-verification flag, for the reader
    // (rule 7) and for the writer (condition 2). Each state has her flag
    // holding unless named; Bob, the writer, always reads his reply.
    let follow_aud = w.f.audience(ALICE, "3kaudfollow2", Some("following"), &[]).await;
    let follow_root = w.f.private_post(ALICE, "3krootfollow", &follow_aud, None).await;
    let following = w.f.private_post(BOB, "3kreplyfollo", &follow_aud, Some((&follow_root, &follow_root))).await;
    w.f.ready_all().await;
    let states: [(&str, &[&str], bool, bool, bool); 4] = [
        // (case, Alice follows, flag holds, Dave reads, Alice reads)
        ("following, Alice follows Bob and Dave", &[BOB, DAVE], true, true, true),
        ("following, writer not followed", &[DAVE], true, false, false),
        ("following, reader not followed", &[BOB], true, false, true),
        ("following, flag NULL", &[BOB, DAVE], false, false, false),
    ];
    for (case, follows, holds, dave_reads, alice_reads) in states {
        exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{ALICE}'")).await;
        for did in follows {
            exec(&w.f.pool, &follow_sql(ALICE, did)).await;
        }
        set_follows_verified(&w.f.pool, ALICE, holds).await;
        w.expect(case, DAVE, &following, dave_reads).await;
        w.expect(case, ALICE, &following, alice_reads).await;
        w.expect(case, BOB, &following, true).await;
    }
    // The blocks below run with the first state.
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{ALICE}'")).await;
    for did in [BOB, DAVE] {
        exec(&w.f.pool, &follow_sql(ALICE, did)).await;
    }
    set_follows_verified(&w.f.pool, ALICE, true).await;

    // Condition 3, either way between the writer and the audience's owner.
    // Since S9 (C-6 yes) it denies under every mode, `list` and NULL too;
    // the writer still reads its own reply.
    for (blocker, blocked, way) in [(BOB, ALICE, "writer blocks owner"), (ALICE, BOB, "owner blocks writer")] {
        exec(&w.f.pool, &block_sql(blocker, blocked)).await;
        w.expect(&format!("{way}, followers"), DAVE, &main_reply, false).await;
        w.expect(&format!("{way}, everyone"), DAVE, &everyone, false).await;
        w.expect(&format!("{way}, list (S9)"), DAVE, &listed, false).await;
        w.expect(&format!("{way}, NULL mode (S9)"), DAVE, &null_mode, false).await;
        w.expect(&format!("{way}, following (S10)"), DAVE, &following, false).await;
        for uri in [&main_reply, &everyone, &listed, &null_mode, &following] {
            w.expect(&format!("{way}, its author"), BOB, uri, true).await;
        }
        exec(&w.f.pool, "DELETE FROM blocks").await;
    }

    // Rule 6 for the reader, either way with the audience's owner: since S9
    // in every mode. (Under `list` the owner's post already could not anchor
    // for that reader, step test (i).) Bob, the writer, still reads.
    for (blocker, blocked, way) in [(ALICE, DAVE, "owner blocks reader"), (DAVE, ALICE, "reader blocks owner")] {
        exec(&w.f.pool, &block_sql(blocker, blocked)).await;
        for (mode, uri) in
            [("followers", &main_reply), ("everyone", &everyone), ("list", &listed), ("NULL mode", &null_mode), ("following", &following)]
        {
            w.expect(&format!("{way}, {mode}"), DAVE, uri, false).await;
            w.expect(&format!("{way}, {mode}, the writer"), BOB, uri, true).await;
        }
        exec(&w.f.pool, "DELETE FROM blocks").await;
    }

    // Condition 4: an audience owner with no `actors` row is not active.
    exec(&w.f.pool, &format!("DELETE FROM actors WHERE did = '{ALICE}'")).await;
    w.expect("owner has no actors row, followers", DAVE, &main_reply, false).await;
    w.expect("owner has no actors row, everyone", DAVE, &everyone, false).await;
    w.expect("owner has no actors row, following", DAVE, &following, false).await;
    w.finish().await;
}

/// Set `did`'s follow-verification flag so that it holds (verified at the
/// stored `applied_rev`, which the firehose has reached), or clear it
/// (following-audience design §3.1).
async fn set_follows_verified(pool: &PgPool, did: &str, holds: bool) {
    let sql = if holds {
        "UPDATE sync_state SET follows_verified_at = now(), follows_verified_rev = applied_rev, \
           firehose_rev = GREATEST(firehose_rev, applied_rev) WHERE did = $1"
    } else {
        "UPDATE sync_state SET follows_verified_at = NULL WHERE did = $1"
    };
    sqlx::query(sql).bind(did).execute(pool).await.expect("flag");
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
    /// T-34's intermediate step (S9, C-6 yes): under Alice's `list`
    /// audience, Bob is listed and blocks Alice.
    BlocksOwner,
}

/// Labelled outputs: label, status, body.
type Outputs = Vec<(&'static str, u16, String)>;
/// [`Outputs`] by mute state.
type MuteRuns = Vec<(String, Outputs)>;

/// T-23's outputs (§7.1), each labelled, as status and bytes: Dave's
/// `getPosts([R])`, `getPostThread(R)`, `getReplies(T)`, `getPosts([T])`
/// (with T's `replyCount` and `quoteCount`), `getPostThread(T)`, timeline,
/// **Carol's** author feed under both filters, `listNotifications` and
/// `getUnreadCount` (R mentions him); and, with `alice`, Alice's
/// notifications for R.
async fn reader_outputs(w: &Walk, r: &str, alice: bool) -> Outputs {
    let mut out = Vec::new();
    for (label, method, query) in [
        ("getPosts([R])", POSTS, format!("uris={r}")),
        ("getPostThread(R)", THREAD, format!("uri={r}")),
        ("getReplies(T)", GET_REPLIES, format!("uri={}", w.root)),
        ("getPosts([T])", POSTS, format!("uris={}", w.root)),
        ("getPostThread(T)", THREAD, format!("uri={}", w.root)),
        ("timeline", TIMELINE, String::new()),
        ("Carol's posts_and_author_threads", FEED, format!("actor={CAROL}&filter=posts_and_author_threads")),
        ("Carol's posts_with_replies", FEED, format!("actor={CAROL}&filter=posts_with_replies")),
        ("listNotifications", LIST_NOTIFICATIONS, String::new()),
        ("getUnreadCount", GET_UNREAD_COUNT, String::new()),
    ] {
        let (status, body, _) = get(&w.base, method, &query, DAVE, DAVE_KEY).await;
        out.push((label, status, body));
    }
    if alice {
        let (status, body, _) = get(&w.base, LIST_NOTIFICATIONS, "", ALICE, ALICE_KEY).await;
        let v: Value = serde_json::from_str(&body).expect("json");
        let for_r: Vec<&Value> = v["notifications"].as_array().expect("notifications").iter().filter(|n| n["uri"] == r).collect();
        out.push(("Alice's notifications for R", status, json!(for_r).to_string()));
    }
    out
}

/// One output of [`reader_outputs`] by its label.
fn output<'a>(outputs: &'a [(&'static str, u16, String)], label: &str) -> &'a str {
    outputs.iter().find(|(l, ..)| *l == label).map(|(_, _, body)| body.as_str()).expect(label)
}

/// [`reader_outputs`] under each mute state of T-23: none; Dave has muted
/// T; Dave has muted M's URI; Alice has muted M's claimed root, T. Each
/// mute is lifted after its run.
async fn outputs_under_mutes(w: &Walk, r: &str, m: &str, alice: bool) -> MuteRuns {
    let viewer = |did: &str| crate::hydration::Viewer {
        did: Some(did.into()),
        hydrate: true,
    };
    let mut runs = vec![("no mute".to_string(), reader_outputs(w, r, alice).await)];
    for (who, root, label) in [(DAVE, w.root.as_str(), "Dave mutes T"), (DAVE, m, "Dave mutes M"), (ALICE, w.root.as_str(), "Alice mutes M's root")] {
        let h = &w.f.app.hydrator;
        h.set_thread_mute(&viewer(who), root, true).await.expect("db").expect("muted");
        runs.push((label.to_string(), reader_outputs(w, r, alice).await));
        h.set_thread_mute(&viewer(who), root, false).await.expect("db").expect("unmuted");
    }
    runs
}

/// Records each output that differs between (a) and (b).
fn compare_fixtures(failures: &mut Vec<String>, a: &MuteRuns, b: &MuteRuns) {
    assert_eq!(a.len(), b.len());
    for ((state, with_m), (_, without_m)) in a.iter().zip(b) {
        assert_eq!(with_m.len(), without_m.len());
        for (x, y) in with_m.iter().zip(without_m) {
            if x != y {
                failures.push(format!("{state}, {}: (a) and (b) differ:\n{x:?}\n{y:?}", x.0));
            }
        }
    }
}

/// `reader`'s `(replyCount, quoteCount)` on the fixture's root.
async fn root_counts(w: &Walk, reader: &str) -> (Option<i64>, Option<i64>) {
    let (_, t) = one_post(&w.base, reader, &w.root).await;
    (t["replyCount"].as_i64(), t["quoteCount"].as_i64())
}

/// T-23 pair 2, run in full (S8): the walk is reader-relative. A step Dave
/// cannot read ends the walk exactly as a missing step does, in every
/// output of [`reader_outputs`] under every mute state.
///
/// Alice's notifications for R are compared only where M is unreadable to
/// her too (`Takedown`, `Unlisted`, `BlocksOwner`): when only Dave is blocked, Alice reads
/// M, so R hangs from her post for her in (a) and is hers to read, and
/// not in (b).
async fn pair_two(test: &str, middle: Middle) {
    let (mode, members): (&str, &[&str]) = match middle {
        Middle::Unlisted => ("list", &[DAVE, CAROL]),
        Middle::BlocksOwner => ("list", &[DAVE, CAROL, BOB]),
        _ => ("followers", &[]),
    };
    let Some(mut w) = Walk::new(test, Layout::SamePds, Some(mode), members).await else {
        return;
    };
    // Dave follows Carol, so his timeline would carry R if R were served.
    exec(&w.f.pool, &follow_sql(DAVE, CAROL)).await;
    // M replies to T and quotes it, so it would count in both of T's
    // counts if Dave were entitled to it (S5).
    let mut m_record = w.f.post_record(BOB, "3kmiddle2222", &w.aud, Some((&w.root, &w.root))).await;
    m_record["embed"] = json!({"$type": "app.bsky.embed.record", "record": {"uri": w.root, "cid": "bafyroot"}});
    w.f.ingest(BOB, NSID_POST_PRIVATE, "3kmiddle2222", m_record).await;
    let m = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kmiddle2222");
    // R mentions Dave, so his notifications would hold it if he could read
    // it (S6).
    let mut r_record = w.f.post_record(CAROL, "3kleaf222222", &w.aud, Some((&w.root, &m))).await;
    r_record["facets"] = json!([mention(DAVE)]);
    w.f.ingest(CAROL, NSID_POST_PRIVATE, "3kleaf222222", r_record).await;
    let r = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3kleaf222222");
    match middle {
        Middle::BlocksReader => exec(&w.f.pool, &block_sql(BOB, DAVE)).await,
        Middle::Takedown => exec(&w.f.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{m}', '!takedown')")).await,
        Middle::Unlisted => {}
        Middle::BlocksOwner => exec(&w.f.pool, &block_sql(BOB, ALICE)).await,
        Middle::Throttled => exec(&w.f.pool, &format!("UPDATE actors SET account_status = 'throttled' WHERE did = '{BOB}'")).await,
    }
    w.f.ready_all().await;
    if middle == Middle::Throttled {
        // A status the moderation gate does not hide: M is readable, so it
        // passes the walk, and R is served by the oracle and the prefilter.
        w.expect("throttled M", DAVE, &m, true).await;
        w.expect("R below a throttled M", DAVE, &r, true).await;
        // A readable M counts in T's counts, so the comparison below can
        // fail (S5).
        let counts = root_counts(&w, DAVE).await;
        if counts != (Some(1), Some(1)) {
            w.failures.push(format!("throttled M: T's (replyCount, quoteCount) for Dave {counts:?}"));
        }
        w.finish().await;
        return;
    }
    // Alice reads R in (a) only when she reads M (see above).
    let alice = middle != Middle::BlocksReader;
    w.expect("(a) R's author", CAROL, &r, true).await;
    w.expect("(a) R", DAVE, &r, false).await;
    w.expect("(a) R for Alice", ALICE, &r, !alice).await;
    let counts = root_counts(&w, DAVE).await;
    if counts != (Some(0), Some(0)) {
        w.failures.push(format!("(a): T's (replyCount, quoteCount) for Dave {counts:?}"));
    }
    let with_m = outputs_under_mutes(&w, &r, &m, alice).await;
    // R's mention of Dave is neither listed nor counted (S6), and nothing
    // of R shows in his reads of T or Carol's feed (S8).
    let unmuted = &with_m[0].1;
    let notified = (output(unmuted, "listNotifications"), output(unmuted, "getUnreadCount"));
    if notified != (r#"{"notifications":[]}"#, r#"{"count":0}"#) {
        w.failures.push(format!("(a): Dave's notifications show R: {notified:?}"));
    }
    for label in ["getReplies(T)", "Carol's posts_and_author_threads", "Carol's posts_with_replies"] {
        let body = output(unmuted, label);
        if body.contains(&r) || body.contains("cursor") {
            w.failures.push(format!("(a): {label} shows R or a cursor: {body}"));
        }
    }
    // (b): M does not exist.
    w.f.apply_op(BOB, Action::Delete, NSID_POST_PRIVATE, "3kmiddle2222", REV2, None).await;
    w.expect("(b) R's author", CAROL, &r, true).await;
    w.expect("(b) R", DAVE, &r, false).await;
    w.expect("(b) R for Alice", ALICE, &r, false).await;
    let without_m = outputs_under_mutes(&w, &r, &m, alice).await;
    compare_fixtures(&mut w.failures, &with_m, &without_m);
    w.finish().await;
}

/// T-23 pair 1, re-cast for S12 (private-threads design §6, T-51): the root
/// rule. Alice's root T is under her `followers` audience, which Dave reads.
/// Carol's reply R names that audience, with `reply.parent = M` and
/// `reply.root = T`; Carol follows Alice, so she stands.
/// - (a) M is Bob's reply to T under Bob's own `list` audience: a reply in
///   another account's thread under the replier's own audience, read by
///   Bob alone, so it is a step no reader passes and R below it is Carol's
///   alone; (b) M does not exist. Every output of [`reader_outputs`],
///   Alice's notifications for R included, is byte-identical between them
///   under every mute state.
/// - A forged root: Carol's R' replies to T claiming `reply.root = X`, Bob's
///   post, under X's audience. Bob gets the root notification. R' and
///   Carol's reply K to it never anchor (T does not agree on the root X), so
///   they are Carol's alone: a mute of T misses R' for her, and X's gate
///   (`allow: []`, hiding K) disables replies in views anchored at R' and
///   never hides K from its author.
#[tokio::test]
async fn t23_pair_one_root_rule() {
    let Some(mut w) = Walk::new("t23-pair1", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let (root, aud) = (w.root.clone(), w.aud.clone());
    for did in [BOB, CAROL] {
        exec(&w.f.pool, &follow_sql(DAVE, did)).await;
    }
    let bob_aud = w.f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL, DAVE, ALICE]).await;
    // M replies to T and quotes it, so it would count in T's counts.
    let mut m_extra = quote_of(&root);
    m_extra["reply"] = json!({"root": {"uri": root, "cid": "bafyroot"}, "parent": {"uri": root, "cid": "bafyparent"}});
    let m = w.f.post_with(BOB, "3kmiddle2222", &bob_aud, m_extra).await;
    let mut r_record = w.f.post_record(CAROL, "3kleaf222222", &aud, Some((&root, &m))).await;
    r_record["facets"] = json!([mention(DAVE)]);
    w.f.ingest(CAROL, NSID_POST_PRIVATE, "3kleaf222222", r_record).await;
    let r = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3kleaf222222");
    // The forged root: Bob's X, read by Dave and Carol, not Alice.
    let x_aud = w.f.audience(BOB, "3kaudbobx222", Some("list"), &[DAVE, CAROL]).await;
    let x = w.f.private_post(BOB, "3kxroot22222", &x_aud, None).await;
    let r_forged = w.f.private_post(CAROL, "3kforgedroot", &x_aud, Some((&x, &root))).await;
    let k = w.f.private_post(CAROL, "3kforgedkid2", &x_aud, Some((&x, &r_forged))).await;
    w.f.ingest(BOB, THREADGATE, "3kxroot22222", json!({"$type": THREADGATE, "post": x, "allow": [], "hiddenReplies": [k]})).await;
    w.f.ready_all().await;

    w.expect("(a) R's author", CAROL, &r, true).await;
    for reader in [DAVE, ALICE] {
        w.expect("(a) R", reader, &r, false).await;
        w.expect("(a) M, under Bob's own audience in Alice's thread", reader, &m, false).await;
    }
    w.expect("(a) M's author", BOB, &m, true).await;
    let with_m = outputs_under_mutes(&w, &r, &m, true).await;
    let unmuted = &with_m[0].1;
    if output(unmuted, "listNotifications").contains(&r) || output(unmuted, "Alice's notifications for R").contains(&r) {
        w.failures.push(format!("(a): R is listed for Dave or Alice: {unmuted:?}"));
    }
    // (b): M does not exist.
    w.f.apply_op(BOB, Action::Delete, NSID_POST_PRIVATE, "3kmiddle2222", REV2, None).await;
    w.expect("(b) R's author", CAROL, &r, true).await;
    w.expect("(b) R", DAVE, &r, false).await;
    let without_m = outputs_under_mutes(&w, &r, &m, true).await;
    compare_fixtures(&mut w.failures, &with_m, &without_m);

    // The forged root. X's author is notified as the root's.
    let bobs = private_rows(&w.f.pool, BOB).await;
    assert!(bobs.contains(&notified("reply", CAROL, Some(&x), &r_forged, false)), "{bobs:?}");
    for uri in [&r_forged, &k] {
        w.expect("a forged root's reply, its author", CAROL, uri, true).await;
        for reader in [DAVE, ALICE, BOB] {
            w.expect("a forged root's reply: T does not agree on the root", reader, uri, false).await;
        }
    }
    // A mute of T misses R'.
    let carol = crate::hydration::Viewer {
        did: Some(CAROL.into()),
        hydrate: true,
    };
    w.f.app.hydrator.set_thread_mute(&carol, &root, true).await.expect("db").expect("muted");
    let (_, p) = posts_of(&w.base, CAROL, &[&r_forged]).await;
    assert!(p[0]["viewer"].get("threadMuted").is_none(), "{}", p[0]);
    w.f.app.hydrator.set_thread_mute(&carol, &root, false).await.expect("db").expect("unmuted");
    // X's gate, in views anchored at R': Carol reads X, so replies are
    // disabled for her; K is hers, so it is not hidden from her.
    let (_, t) = thread_of(&w.base, CAROL, &format!("uri={r_forged}")).await;
    assert_eq!(reply_uris(&t), std::slice::from_ref(&k), "{t}");
    assert_eq!(t["post"]["viewer"]["replyDisabled"], true, "{t}");
    let (_, v, _) = json_of(&w.base, GET_REPLIES, &format!("uri={r_forged}"), CAROL).await;
    assert_eq!(v["replies"][0]["post"]["uri"], k.as_str(), "{v}");
    for reader in [DAVE, ALICE] {
        let (body, _, _) = json_of(&w.base, GET_REPLIES, &format!("uri={r_forged}"), reader).await;
        assert_eq!(body, r#"{"replies":[]}"#, "{reader}");
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

/// T-34's intermediate-step pair (S9, C-6 yes): under Alice's `list`
/// audience with Dave and Carol listed, (a) M by a listed Bob who blocks
/// Alice, with Carol's reply R to M, and (b) no M. Dave's outputs are
/// byte-identical, and R is read by Carol only in both. It fails on either
/// half-change: `step_ok` keeping S8's `list` scoping (R anchors through
/// M) or the prefilter keeping it (M counts in T's counts for Dave).
#[tokio::test]
async fn t34_list_step_by_a_writer_who_blocks_the_owner() {
    pair_two("t34-pair", Middle::BlocksOwner).await;
}

/// T-34 (S9, C-6 yes): under Alice's `list` audience, a listed reader she
/// blocks does not read Bob's reply, and a reply by a listed author she
/// blocks is read by its author only.
#[tokio::test]
async fn t34_list_blocks_with_the_owner() {
    let Some(mut w) = Walk::new("t34-blocks", Layout::SamePds, Some("list"), &[BOB, CAROL, DAVE]).await else {
        return;
    };
    let root = w.root.clone();
    let reply = w.reply(BOB, "3kt34reply22", &root).await;
    w.f.ready_all().await;
    for reader in [BOB, CAROL, DAVE] {
        w.expect("no block", reader, &reply, true).await;
    }
    exec(&w.f.pool, &block_sql(ALICE, DAVE)).await;
    for (reader, reads) in [(BOB, true), (CAROL, true), (DAVE, false)] {
        w.expect("owner blocks a listed reader", reader, &reply, reads).await;
    }
    exec(&w.f.pool, "DELETE FROM blocks").await;
    exec(&w.f.pool, &block_sql(ALICE, BOB)).await;
    for (reader, reads) in [(BOB, true), (CAROL, false), (DAVE, false)] {
        w.expect("owner blocks the listed author", reader, &reply, reads).await;
    }
    w.finish().await;
}

/// T-52 (one builder): fact 8 and every candidate shape are §3.2's one
/// statement over their own `c`, with the cap inlined from config, so the
/// oracle and the prefilter cannot drift apart.
#[test]
fn every_shape_is_one_walked_statement() {
    let fact8 = schema::fact8_statement(CAP);
    let shapes = [
        ("prefilter_uris", schema::prefilter_uris_statement(CAP)),
        ("getReplies page", schema::reply_page_statement(CAP)),
        ("search", schema::search_statement(CAP)),
        ("expansion", schema::reply_candidates_statement(CAP)),
        ("author feed", schema::author_statement(schema::AuthorFilter::Replies, CAP)),
        ("author threads", schema::author_statement(schema::AuthorFilter::OwnThreads, CAP)),
        ("author media", schema::author_statement(schema::AuthorFilter::Media, CAP)),
        ("author video", schema::author_statement(schema::AuthorFilter::Video, CAP)),
        ("timeline", schema::timeline_statement(CAP)),
        ("getQuotes", schema::quote_statement(CAP)),
        ("replyCount", schema::reply_counts_statement(CAP)),
        ("quoteCount slice", schema::quote_counts_slice_statement(CAP)),
        ("getUnreadCount slice", schema::unread_slice_statement(CAP)),
    ];
    // Fact 8 is the one-row case: its `c` is the post `$2`, in the builder's
    // one-row form (`walked_one`), the same stage 1 and final, its walk and
    // branch root inlined at the cap, and the reader's own row skipped.
    assert!(fact8.contains("WITH c AS MATERIALIZED (SELECT p.uri, p.author_did, p.audience_uri, p.reply_parent_uri, p.reply_root_uri FROM private_posts p WHERE p.uri = $2)"));
    assert_eq!(fact8.matches(&format!("u.hop < {CAP} ")).count(), 2);
    assert!(fact8.contains("FROM s1 WHERE s1.s1ok AND s1.walks AND s1.author_did <> $1"));
    assert!(schema::fact8_statement(100).contains("u.hop < 100 "));
    assert!(fact8.contains(&schema::fact8_final()));
    for (name, text) in &shapes {
        for cte in ["WITH RECURSIVE c AS MATERIALIZED (", " bcl(uri,", " bd(uri,", "s1 AS MATERIALIZED (", " tcl(uri,", " wd(uri,", "pvm AS MATERIALIZED ("] {
            assert_eq!(text.matches(cte).count(), 1, "{name}: {cte}");
        }
        // Every level and descent bound inlines the configured value (two
        // climbs, two descents), and nothing else walks.
        assert_eq!(text.matches(&format!("x.cur < {CAP} ")).count(), 2, "{name}");
        assert_eq!(text.matches(&format!("x.lvl < {CAP} ")).count(), 2, "{name}");
        assert_eq!(text.matches(&format!("x.d < {CAP} ")).count(), 2, "{name}");
        assert_eq!(text.matches("WITH RECURSIVE").count(), 1, "{name}");
        // The walk reads only rows that pass stage 1, other than the
        // reader's own.
        assert!(text.contains("FROM s1 WHERE s1.s1ok AND s1.walks AND s1.author_did <> $1"), "{name}");
        // No `:146` skip: nothing passes a post under its author's own
        // audience without stage 1.
        assert!(!text.contains("a.owner_did = p.author_did"), "{name}");
    }
    assert!(schema::prefilter_uris_statement(100).contains("x.cur < 100 "));
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

/// Prints the pre-S9 count query (private-posting design §5 S9, C-6) for
/// skydeval to run read-only in `psql` against the deployed database:
/// `cargo test pre_s9_count_query -- --ignored --nocapture`.
#[test]
#[ignore = "prints the pre-S9 count query; run with --ignored --nocapture"]
fn pre_s9_count_query() {
    println!("-- S9 pre-release count: cross-owner list/NULL posts S9 stops serving\n{}", schema::pre_s9_count_sql());
}

/// The pre-S9 count query runs, and counts each kind once.
#[tokio::test]
async fn pre_s9_count_query_counts_each_kind() {
    let Some(w) = Walk::new("pre-s9-count", Layout::SamePds, Some("list"), &[BOB, CAROL, DAVE]).await else {
        return;
    };
    let root = w.root.clone();
    // Bob, listed, blocks Alice: his reply stops, and so does Carol's reply
    // to it, whose chain passes through his.
    let blocked = w.reply(BOB, "3kblocked222", &root).await;
    w.reply(CAROL, "3kbelowblock", &blocked).await;
    // Served before and after.
    w.reply(CAROL, "3kfine222222", &root).await;
    exec(&w.f.pool, &block_sql(BOB, ALICE)).await;
    // Alice blocks Dave, a reader only: S8 already denies him the anchor,
    // so no pair (nor for Bob as a reader of Carol's replies).
    exec(&w.f.pool, &block_sql(ALICE, DAVE)).await;
    let counts: (i64, i64, i64, i64) = sqlx::query_as(&schema::pre_s9_count_sql()).fetch_one(&w.f.pool).await.expect("count query");
    assert_eq!(counts, (1, 1, 0, 0));
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
    // Video: the MP4 blob on the author's PDS, with no thumbnail, under the
    // default `bsky_cdn` (private-video-pds design).
    assert_eq!(posts[3]["embed"]["$type"], "app.bsky.embed.video#view");
    assert_eq!(posts[3]["embed"]["playlist"], blob_on_pds(ALICE, "bafyvideo"));
    assert!(posts[3]["embed"].get("thumbnail").is_none(), "{}", posts[3]);
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

    // Nor under `bsky_cdn` with another CDN base.
    let other_cdn = serve(f.state_with_video(VideoConfig {
        playback: VideoPlayback::BskyCdn,
        cdn_url: "https://cdn.example.org".into(),
    }))
    .await;
    let (body, video) = one_post(&other_cdn, BOB, &uris[3]).await;
    assert_eq!(video["embed"]["playlist"], blob_on_pds(ALICE, "bafyvideo"));
    assert!(video["embed"].get("thumbnail").is_none() && !body.contains("cdn.example.org"), "{body}");
    db.drop_db().await;
}

/// The server T-PV1 reads: `[video]` at `bsky_cdn` under a CDN base no PDS
/// URL contains, roomy rate limits, fresh heads and watermark, no floor.
async fn cdn_marked_server(f: &Fixture) -> String {
    exec(&f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
    let mut app = f.app.clone();
    app.hydrator = hydrator_configured(
        &f.pool,
        f.sets.clone(),
        HydrationConfig {
            video: VideoConfig {
                playback: VideoPlayback::BskyCdn,
                cdn_url: CDN_MARK.into(),
            },
            ..test_config()
        },
    );
    app.limits = roomy_limits();
    serve_live(f.state_of(app, Some(Duration::from_secs(1)), Duration::ZERO)).await
}

/// T-PV1's CDN base.
const CDN_MARK: &str = "https://cdn.example.org";

/// No CDN URL in `body`, under T-PV1's base or Bluesky's.
fn assert_no_cdn(what: &str, body: &str) {
    assert!(!body.contains("cdn.example.org") && !body.contains("video.bsky.app"), "{what}: {body}");
}

/// A video embed in the kryphocron form.
fn video_embed(cid: &str) -> Value {
    json!({"$type": format!("{KLEX}#embedVideo"), "video": blob(cid), "alt": format!("alt {cid}"), "aspectRatio": {"width": 16, "height": 9}})
}

/// The view T-PV1 expects of a private video `cid` by Alice.
fn pds_video_view(cid: &str) -> Value {
    json!({"$type": "app.bsky.embed.video#view", "cid": cid, "playlist": blob_on_pds(ALICE, cid),
           "alt": format!("alt {cid}"), "aspectRatio": {"width": 16, "height": 9}})
}

/// T-PV1 (private-video-pds design §8), in On and in Off (T-1): with
/// `[video]` at `bsky_cdn`, every private post's video, on every endpoint
/// that shows it and nested at every depth, plays from Alice's PDS with no
/// thumbnail, and no private view names a CDN. In On, a public post's video
/// quoted inside a private post still follows the setting. T-PV2: with no
/// PDS endpoint a private video has no view, and still no CDN URL.
async fn private_videos_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let v = f
        .post_with(ALICE, "3kvideo22222", &aud, json!({"embed": video_embed("bafyvideo"), "facets": [mention(BOB)], "createdAt": now}))
        .await;
    let quote = |uri: &str| json!({"$type": format!("{KLEX}#embedRecord"), "record": {"uri": uri, "cid": "bafyany"}});
    let rwm = |uri: &str, cid: &str| {
        json!({"$type": format!("{KLEX}#embedRecordWithMedia"), "record": {"record": {"uri": uri, "cid": "bafyany"}}, "media": video_embed(cid)})
    };
    let q = f.post_with(ALICE, "3kquotesv222", &aud, json!({"embed": quote(&v)})).await;
    let t = f.private_post(ALICE, "3ktext222222", &aud, None).await;
    let m = f.post_with(ALICE, "3kmedia22222", &aud, json!({"embed": rwm(&t, "bafymedia")})).await;
    let qm = f.post_with(ALICE, "3kquotesm222", &aud, json!({"embed": quote(&m)})).await;
    let mut reply = f.record_with(ALICE, "3kreply22222", &aud, &json!({"embed": video_embed("bafyreply")})).await;
    reply["reply"] = json!({"root": {"uri": v, "cid": "bafy3kvideo22222"}, "parent": {"uri": v, "cid": "bafy3kvideo22222"}});
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kreply22222", reply).await;
    // Row 7c: media quoting a public post this AppView does not store.
    let unstored = format!("at://{CAROL}/{PUBLIC_POST}/3knotstored2");
    let mp = f.post_with(ALICE, "3kmediapub22", &aud, json!({"embed": rwm(&unstored, "bafymediapub")})).await;
    // A stored public post with a video, quoted by a private post.
    let public = crate::hydration::testing::post(&f.pool, CAROL, "3kpublicvid2", "public video", None, None).await;
    let public_video = json!({"$type": "app.bsky.embed.video", "video": blob("bafypublic")});
    sqlx::query("UPDATE posts SET embed = $1 WHERE uri = $2")
        .bind(&public_video)
        .bind(&public)
        .execute(&f.pool)
        .await
        .expect("embed");
    // By Dave, so it stays out of Alice's feed and Bob's timeline, whose
    // bodies must name no CDN.
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[BOB]).await;
    let qp = f.post_with(DAVE, "3kquotespub2", &dave_aud, json!({"embed": quote(&public)})).await;
    exec(&f.pool, &follow_sql(BOB, ALICE)).await;
    f.ready_all().await;
    let base = cdn_marked_server(&f).await;
    assert_eq!(post(&base, CREATE_BOOKMARK, &json!({"uri": v, "cid": "bafy3kvideo22222"}), BOB).await.0, 200);

    // getPosts: the post's own video, a quote of it, recordWithMedia media,
    // and the media of a quoted recordWithMedia.
    let (body, posts) = posts_of(&base, BOB, &[&v, &q, &m, &qm]).await;
    assert_no_cdn("getPosts", &body);
    assert_eq!(posts.len(), 4, "{body}");
    assert_eq!(posts[0]["embed"], pds_video_view("bafyvideo"), "V");
    assert_eq!(posts[0]["record"]["embed"], video_embed("bafyvideo"), "record.embed as stored");
    assert_eq!(posts[1]["embed"]["record"]["embeds"][0], pds_video_view("bafyvideo"), "Q");
    assert_eq!(posts[2]["embed"]["$type"], "app.bsky.embed.recordWithMedia#view", "M");
    assert_eq!(posts[2]["embed"]["media"], pds_video_view("bafymedia"), "M");
    assert_eq!(posts[3]["embed"]["record"]["embeds"][0], pds_video_view("bafymedia"), "QM");

    // Every other endpoint that shows V or R.
    let v_url = blob_on_pds(ALICE, "bafyvideo");
    let r_url = blob_on_pds(ALICE, "bafyreply");
    let (v_url, r_url) = (v_url.as_str(), r_url.as_str());
    let reads: [(&str, &str, String, Vec<&str>); 7] = [
        ("getPostThread", THREAD, format!("uri={v}"), vec![v_url, r_url]),
        ("getAuthorFeed", FEED, format!("actor={ALICE}&limit=50"), vec![v_url, r_url]),
        ("getTimeline", TIMELINE, "limit=50".to_string(), vec![v_url]),
        ("getBookmarks", GET_BOOKMARKS, String::new(), vec![v_url]),
        ("getQuotes", GET_QUOTES, format!("uri={v}"), vec![v_url]),
        ("getReplies", GET_REPLIES, format!("uri={v}"), vec![r_url]),
        ("searchPosts", SEARCH, "q=3kvideo22222".to_string(), vec![v_url]),
    ];
    for (what, method, query, urls) in &reads {
        let (body, _, _) = json_of(&base, method, query, BOB).await;
        assert_no_cdn(what, &body);
        for url in urls {
            assert!(body.contains(*url), "{what}: {url} in {body}");
        }
    }
    // A notification carries the record, never a media view.
    let (page, _) = notifications_of(&base, BOB, "").await;
    let Some(n) = page["notifications"].as_array().expect("notifications").iter().find(|n| n["uri"] == v.as_str()) else {
        panic!("V's mention: {page}");
    };
    assert!(n.get("embed").is_none() && n["record"]["embed"] == video_embed("bafyvideo"), "{n}");
    assert_no_cdn("listNotifications", &page.to_string());

    // Row 7c: no view at all; the client builds it from `record.embed`.
    let (body, p) = one_post(&base, BOB, &mp).await;
    assert_no_cdn("row 7c", &body);
    assert!(p.get("embed").is_none(), "{p}");
    assert_eq!(p["record"]["embed"], rwm(&unstored, "bafymediapub"));

    // A quoted public post's video follows `[video]`: shown in On only.
    let (_, p) = one_post(&base, BOB, &qp).await;
    if mode.is_on() {
        let cdn = format!("{CDN_MARK}/watch/did%3Aplc%3A{}/bafypublic", &CAROL[8..]);
        assert_eq!(p["embed"]["record"]["embeds"][0]["playlist"], format!("{cdn}/playlist.m3u8"), "{p}");
        assert_eq!(p["embed"]["record"]["embeds"][0]["thumbnail"], format!("{cdn}/thumbnail.jpg"), "{p}");
    } else {
        assert!(p.get("embed").is_none(), "{p}");
    }

    // T-PV2: Alice's PDS unknown. V reads, with no media view.
    exec(&f.pool, &format!("UPDATE actors SET pds_endpoint = NULL WHERE did = '{ALICE}'")).await;
    let (body, p) = one_post(&base, BOB, &v).await;
    assert_no_cdn("no PDS", &body);
    assert_eq!(p["uri"], v.as_str(), "{body}");
    assert!(p.get("embed").is_none(), "{p}");
    assert_eq!(p["record"]["embed"], video_embed("bafyvideo"));
    db.drop_db().await;
}

#[tokio::test]
async fn private_videos_play_from_the_pds_on() {
    private_videos_in(PublicMode::On, "pv-videos-on").await;
}

#[tokio::test]
async fn private_videos_play_from_the_pds_off() {
    private_videos_in(PublicMode::Off, "pv-videos-off").await;
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
    assert_eq!(methods.len(), 13);
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
                // `q` for `searchPosts`; the other methods ignore it.
                crate::honest_501::Verb::Get => get(&base, nsid, &format!("uri={uri}&q=post"), BOB, BOB_KEY).await.0,
                crate::honest_501::Verb::Post => {
                    post(&base, nsid, &json!({"uri": uri, "cid": "bafy", "seenAt": "2026-10-01T00:00:00Z"}), BOB).await.0
                }
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

    // Review 1, LB3, as S12 restates it (private-threads design §6): Carol's
    // reply R claims Alice's root X and names X's audience; Bob's reply K to
    // R names it too. (a) Before X is written and after it is deleted, Dave's
    // answers for R are byte-identical to a nonexistent post's. (b) While X
    // exists under an audience holding Dave and Carol, Dave reads R, and X's
    // gate (`allow: []`, hiding K) applies under C-17. (c) While X exists
    // without Dave, R is a miss again.
    let list_aud = f.audience(ALICE, "3kaudlist222", Some("list"), &[BOB, CAROL, DAVE]).await;
    let x = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3klb3root222");
    let r = f.private_post(CAROL, "3klb3reply22", &list_aud, Some((&x, &x))).await;
    let child = f.private_post(BOB, "3klb3child22", &list_aud, Some((&x, &r))).await;
    let nowhere = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3knothere222");
    let dave_answers = |uri: String| {
        let base = base.clone();
        async move {
            let (posts, _) = posts_of(&base, DAVE, &[&uri]).await;
            let (status, thread, _) = get(&base, THREAD, &format!("uri={uri}"), DAVE, DAVE_KEY).await;
            let (replies, _, _) = json_of(&base, GET_REPLIES, &format!("uri={uri}"), DAVE).await;
            (posts, status, thread, replies)
        }
    };
    let nothing = dave_answers(nowhere.clone()).await;
    assert_eq!(dave_answers(r.clone()).await, nothing, "(a) before X is written");
    assert_eq!(f.private_post(ALICE, "3klb3root222", &list_aud, None).await, x);
    f.ingest(ALICE, THREADGATE, "3klb3root222", json!({"$type": THREADGATE, "post": x, "allow": [], "hiddenReplies": [child]}))
        .await;
    // (b): Dave reads R, and X's gate applies: replies disabled, K hidden.
    let (_, p) = posts_of(&base, DAVE, &[&r]).await;
    assert_eq!(p.len(), 1, "(b) Dave reads R");
    let (_, t) = thread_of(&base, DAVE, &format!("uri={r}")).await;
    assert_eq!(reply_uris(&t), Vec::<String>::new(), "(b) K hidden from Dave: {t}");
    assert_eq!(t["post"]["viewer"]["replyDisabled"], true, "{t}");
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
    // (c): X's audience no longer holds Dave.
    let without_dave = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [BOB, CAROL], "createdAt": "2026-09-26T00:00:00Z"});
    f.apply_op(ALICE, Action::Update, NSID_AUDIENCE, "3kaudlist222", REV2, Some(without_dave)).await;
    assert_eq!(dave_answers(r.clone()).await, nothing, "(c) X without Dave");
    // (a) again: X deleted, its gate with it.
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3klb3root222", REV2, None).await;
    assert!(!gate_rows(&f.pool, "private_threadgates").await.contains(&format!("at://{ALICE}/{THREADGATE}/3klb3root222")));
    assert_eq!(dave_answers(r.clone()).await, nothing, "(a) after X is deleted");

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

// ---- Private posting S5: counts, viewer state, likes, reposts, quotes,
// repost items and thread mutes (P-2, P-5, P-16) ----

/// An account in no fixture set: not a member, not in S. In On its likes
/// and reposts of a member's post are stored (rule 2); in Off they are not.
const EVE: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
const LIKE: &str = "app.bsky.feed.like";
const REPOST: &str = "app.bsky.feed.repost";
const PUBLIC_POST: &str = "app.bsky.feed.post";
const GET_LIKES: &str = "app.nearhorizon.private.getLikes";
const GET_REPOSTED_BY: &str = "app.nearhorizon.private.getRepostedBy";
const GET_QUOTES: &str = "app.nearhorizon.private.getQuotes";

/// A like or repost record of `uri`, made at `at`.
fn subject_record(collection: &str, uri: &str, at: &str) -> Value {
    json!({"$type": collection, "subject": {"uri": uri, "cid": "bafysubject"}, "createdAt": at})
}

/// A public post quoting `uri`, made at `at`.
fn public_quote(uri: &str, at: &str) -> Value {
    json!({"$type": PUBLIC_POST, "text": "quoting", "createdAt": at,
           "embed": {"$type": "app.bsky.embed.record", "record": {"uri": uri, "cid": "bafyany"}}})
}

/// GET `method?query` for `reader`: status 200, the raw body, the JSON and
/// the time taken.
async fn json_of(base: &str, method: &str, query: &str, reader: &str) -> (String, Value, Duration) {
    let (status, body, took) = get(base, method, query, reader, key_of(reader)).await;
    assert_eq!(status, 200, "{method}?{query}: {body}");
    let v = serde_json::from_str(&body).expect("json");
    (body, v, took)
}

fn counts_of(view: &Value) -> [i64; 4] {
    ["replyCount", "repostCount", "likeCount", "quoteCount"].map(|k| view[k].as_i64().unwrap_or(-1))
}

/// The actor DIDs of a `getLikes` or `getRepostedBy` page.
fn actor_dids(page: &Value, key: &str) -> Vec<String> {
    page[key]
        .as_array()
        .expect(key)
        .iter()
        .map(|i| i.get("actor").unwrap_or(i)["did"].as_str().expect("did").to_string())
        .collect()
}

/// T-11, T-12, T-13, T-17 (the read half) and T-38's counts, in On and in
/// Off (T-1). Alice's post T is under her `list` audience of Bob, Carol and
/// Dave; Carol, the main reader, blocks Dave.
///
/// - Counts disclose no hidden item: Carol's view of T and her three lists
///   are byte-identical before and after a hidden reply, a hidden private
///   quote, a taken-down reply, a blocked account's reply, like, repost and
///   public quote, a non-member's like, repost and public quote, and Bob's
///   reply and quote that only Bob reads are written. A reply that fails
///   decode counts. Other readers' counts show the same rows counted.
/// - Counts and `viewer.like`/`repost` match by URI after T's CID changes.
/// - `threadMuted` follows the claimed root; muting a private root that
///   exists and one that does not answer alike.
/// - The lists: member likers newest first, paged; misses are uniform and
///   floored; `getQuotes` mixes the two item types with a sealed cursor.
async fn interactions_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let aud_dave = f.audience(ALICE, "3kauddave222", Some("list"), &[DAVE]).await;
    let aud_bob = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    let hidden_post = f.private_post(ALICE, "3khiddenpost", &aud_dave, None).await;
    exec(&f.pool, &block_sql(CAROL, DAVE)).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    // What Carol may know of: Bob's reply, a reply that does not decode,
    // Bob's and her own likes and reposts, Bob's private and public quotes.
    let reply_ok = f.private_post(BOB, "3kreplyok222", &aud, Some((&t, &t))).await;
    let undecodable_record = f.post_record(BOB, "3kencodedfor", &aud, Some((&t, &t))).await;
    f.ingest(BOB, NSID_POST_PRIVATE, "3kundecode22", undecodable_record).await;
    let undecodable = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kundecode22");
    for (who, rkey, at) in [(BOB, "3klikebob222", "2026-10-01T10:00:00Z"), (CAROL, "3klikecarol2", "2026-10-01T12:00:00Z")] {
        f.ingest(who, LIKE, rkey, subject_record(LIKE, &t, at)).await;
        f.ingest(who, REPOST, rkey, subject_record(REPOST, &t, at)).await;
    }
    let private_quote = f.post_with(BOB, "3kprivquote2", &aud_bob, quote_of(&t)).await;
    f.ingest(BOB, PUBLIC_POST, "3kpubquote22", public_quote(&t, "2026-09-27T00:00:00Z")).await;
    let bob_public = format!("at://{BOB}/{PUBLIC_POST}/3kpubquote22");
    // S12 (private-threads design §2.2): Alice's own reply in her thread is
    // read by the root's audience, whatever audience of hers it names.
    f.private_post(ALICE, "3khiddenrep2", &aud_dave, Some((&t, &t))).await;
    f.ready_all().await;
    // The undecodable reply is stored and passes the prefilter, but no one
    // reads it, its author included.
    assert_eq!(posts_of(&base, BOB, &[&undecodable]).await.1.len(), 0, "the reply does not decode");
    assert_eq!(schema::prefilter_uris(&f.pool, CAROL, std::slice::from_ref(&undecodable), CAP).await.expect("prefilter").len(), 1);

    let carols_answers = || async {
        let mut out = Vec::new();
        for (method, query) in [
            (POSTS, format!("uris={t}")),
            (GET_LIKES, format!("uri={t}")),
            (GET_REPOSTED_BY, format!("uri={t}")),
            (GET_QUOTES, format!("uri={t}")),
        ] {
            out.push(json_of(&base, method, &query, CAROL).await.0);
        }
        out
    };
    let before = carols_answers().await;

    // What Carol may not know of.
    f.post_with(ALICE, "3khiddenquo2", &aud_dave, quote_of(&t)).await;
    let taken_down = f.private_post(BOB, "3ktakendown2", &aud, Some((&t, &t))).await;
    exec(&f.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{taken_down}', '!takedown')")).await;
    f.private_post(DAVE, "3kdavereply2", &aud, Some((&t, &t))).await;
    f.ingest(DAVE, LIKE, "3klikedave22", subject_record(LIKE, &t, "2026-10-01T11:00:00Z")).await;
    f.ingest(DAVE, REPOST, "3klikedave22", subject_record(REPOST, &t, "2026-10-01T11:00:00Z")).await;
    f.ingest(DAVE, PUBLIC_POST, "3kdavequote2", public_quote(&t, "2026-09-28T00:00:00Z")).await;
    f.ingest(EVE, LIKE, "3klikeeve222", subject_record(LIKE, &t, "2026-10-01T13:00:00Z")).await;
    f.ingest(EVE, REPOST, "3klikeeve222", subject_record(REPOST, &t, "2026-10-01T13:00:00Z")).await;
    f.ingest(EVE, PUBLIC_POST, "3kevequote22", public_quote(&t, "2026-09-29T00:00:00Z")).await;
    // T-38: Bob's top-level post under Alice's audience and his reply to it
    // quoting T are read by Bob alone.
    let own_top = f.private_post(BOB, "3kowntop2222", &aud, None).await;
    let mut own_reply = f.post_record(BOB, "3kownreply22", &aud, Some((&own_top, &own_top))).await;
    own_reply["embed"] = quote_of(&t)["embed"].clone();
    f.ingest(BOB, NSID_POST_PRIVATE, "3kownreply22", own_reply).await;
    f.ready_all().await;
    let eve_rows: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM likes WHERE author_did = $1) + (SELECT count(*) FROM reposts WHERE author_did = $1)")
        .bind(EVE)
        .fetch_one(&f.pool)
        .await
        .expect("count");
    assert_eq!(eve_rows, if mode.is_on() { 2 } else { 0 }, "a non-member's like and repost are stored in On only");
    let refs: Vec<String> = sqlx::query_scalar("SELECT uri FROM private_quote_refs ORDER BY uri").fetch_all(&f.pool).await.expect("refs");
    assert_eq!(refs, [bob_public.clone(), format!("at://{DAVE}/{PUBLIC_POST}/3kdavequote2")], "members' public quotes only");
    assert_eq!(carols_answers().await, before, "no hidden item shows in Carol's counts or lists");

    // The counts, reader by reader: [reply, repost, like, quote].
    let (_, p) = posts_of(&base, CAROL, &[&t]).await;
    assert_eq!(counts_of(&p[0]), [3, 2, 2, 2], "{}", p[0]);
    // Carol's three replies are Bob's, the undecodable one and Alice's own
    // reply under her other audience. Bob: no block; he also reads Dave's
    // reply and his own reply-quote.
    let (_, p) = posts_of(&base, BOB, &[&t]).await;
    assert_eq!(counts_of(&p[0]), [4, 3, 3, 4], "{}", p[0]);
    // Alice reads her own hidden reply and quote. She is not listed in her
    // own `list` audience, so others' replies under it are not hers to read
    // and not counted, as `getPostThread` would not show them; nor are
    // Bob's private quote (his audience is Carol) and his own-thread reply.
    let (_, p) = posts_of(&base, ALICE, &[&t]).await;
    assert_eq!(counts_of(&p[0]), [1, 3, 3, 3], "{}", p[0]);
    let (_, thread) = thread_of(&base, ALICE, &format!("uri={t}")).await;
    assert_eq!(reply_uris(&thread).len(), 1, "the count matches what Alice reads");
    // A view with nothing to count says 0.
    let (_, p) = posts_of(&base, CAROL, &[&reply_ok]).await;
    assert_eq!(counts_of(&p[0]), [0, 0, 0, 0]);

    // T-12: the reader's own like and repost, and the counts, by URI after
    // the CID changes.
    let carol_like = format!("at://{CAROL}/{LIKE}/3klikecarol2");
    let carol_repost = format!("at://{CAROL}/{REPOST}/3klikecarol2");
    let (_, p) = posts_of(&base, CAROL, &[&t]).await;
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false, "like": carol_like, "repost": carol_repost}));
    let (_, p) = posts_of(&base, DAVE, &[&t]).await;
    assert_eq!(p[0]["viewer"]["like"], format!("at://{DAVE}/{LIKE}/3klikedave22"));
    let record = f.post_record(ALICE, "3ktpost22222", &aud, None).await;
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3ktpost22222", REV2, Some(record)).await;
    let (_, p) = posts_of(&base, CAROL, &[&t]).await;
    assert_ne!(p[0]["cid"], "bafy3ktpost22222", "the CID changed");
    assert_eq!(counts_of(&p[0]), [3, 2, 2, 2]);
    assert_eq!(p[0]["viewer"], json!({"bookmarked": false, "like": carol_like, "repost": carol_repost}));

    // T-17 (the read half): a private root mutes as a public one does,
    // without a lookup; `threadMuted` follows the claimed root.
    let carol = crate::hydration::Viewer {
        did: Some(CAROL.into()),
        hydrate: true,
    };
    let nowhere = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere222");
    let existing = f.app.hydrator.set_thread_mute(&carol, &t, true).await.expect("db").expect("muted");
    let missing = f.app.hydrator.set_thread_mute(&carol, &nowhere, true).await.expect("db").expect("muted");
    assert_eq!(existing.inner(), missing.inner());
    let (_, p) = posts_of(&base, CAROL, &[&t, &reply_ok, &private_quote]).await;
    assert_eq!(p[0]["viewer"]["threadMuted"], true);
    assert_eq!(p[1]["viewer"]["threadMuted"], true, "a reply claiming T");
    assert!(p[2]["viewer"].get("threadMuted").is_none(), "another thread");
    let (_, p) = posts_of(&base, BOB, &[&t]).await;
    assert!(p[0]["viewer"].get("threadMuted").is_none(), "Carol's mute is hers");
    f.app.hydrator.set_thread_mute(&carol, &t, false).await.expect("db").expect("unmuted");
    let (_, p) = posts_of(&base, CAROL, &[&t]).await;
    assert!(p[0]["viewer"].get("threadMuted").is_none());

    // T-13: getLikes and getRepostedBy, member likers newest first, paged.
    for (method, key) in [(GET_LIKES, "likes"), (GET_REPOSTED_BY, "repostedBy")] {
        let (_, page, took) = json_of(&base, method, &format!("uri={t}"), CAROL).await;
        assert!(took >= FLOOR, "{method} took {took:?}");
        assert_eq!(page["uri"], t.as_str());
        assert_eq!(actor_dids(&page, key), [CAROL, BOB], "{method}");
        assert!(page.get("cursor").is_none());
        let (_, page, _) = json_of(&base, method, &format!("uri={t}"), BOB).await;
        assert_eq!(actor_dids(&page, key), [CAROL, DAVE, BOB], "{method}: a non-member never");
        let mut seen = Vec::new();
        let mut query = format!("uri={t}&limit=1");
        for _ in 0..5 {
            let (_, page, _) = json_of(&base, method, &query, CAROL).await;
            seen.extend(actor_dids(&page, key));
            match page["cursor"].as_str() {
                Some(c) => query = format!("uri={t}&limit=1&cursor={}", urlencoding(c)),
                None => break,
            }
        }
        assert_eq!(seen, [CAROL, BOB], "{method} paged");
    }
    let (_, page, _) = json_of(&base, GET_LIKES, &format!("uri={t}"), CAROL).await;
    let like = &page["likes"][0];
    assert_eq!(like["createdAt"], "2026-10-01T12:00:00.000Z");
    assert!(like["indexedAt"].is_string() && like["actor"]["handle"] == "carol.test", "{like}");

    // T-13: getQuotes mixes the two item types, newest first, with a
    // sealed cursor bound to its caller and its uri.
    let (_, page, took) = json_of(&base, GET_QUOTES, &format!("uri={t}"), CAROL).await;
    assert!(took >= FLOOR);
    let posts = page["posts"].as_array().expect("posts");
    assert_eq!(posts.len(), 2, "{page}");
    assert_eq!(posts[0], json!({"$type": "app.nearhorizon.private.defs#publicPostRef", "uri": bob_public, "cid": "bafy3kpubquote22"}));
    assert_eq!(posts[1]["$type"], "app.nearhorizon.private.defs#privatePostView");
    assert_eq!((posts[1]["uri"].as_str(), posts[1]["record"]["text"].as_str()), (Some(private_quote.as_str()), Some("3kprivquote2")));
    assert_eq!(posts[1]["embed"]["record"]["uri"], t.as_str(), "the quoting post's own view");
    let (_, first, _) = json_of(&base, GET_QUOTES, &format!("uri={t}&limit=1"), CAROL).await;
    let cursor = first["cursor"].as_str().expect("more candidates remain").to_string();
    assert_eq!(cursor.len(), 720, "a sealed, padded cursor");
    let (_, second, _) = json_of(&base, GET_QUOTES, &format!("uri={t}&limit=1&cursor={cursor}"), CAROL).await;
    assert_eq!(second["posts"][0]["uri"], private_quote.as_str());
    assert!(second.get("cursor").is_none(), "{second}");
    for (reader, uri) in [(BOB, t.as_str()), (CAROL, reply_ok.as_str())] {
        let (status, body, _) = get(&base, GET_QUOTES, &format!("uri={uri}&limit=1&cursor={cursor}"), reader, key_of(reader)).await;
        assert_eq!(status, 400, "{body}");
        assert!(body.contains("cursor is not valid"), "{body}");
    }
    let (_, page, _) = json_of(&base, GET_QUOTES, &format!("uri={t}"), BOB).await;
    assert_eq!(page["posts"].as_array().map(Vec::len), Some(4), "Bob reads Dave's quote and his own-thread reply: {page}");

    // T-13: misses. A post Carol cannot read gives the answer of the same
    // URI once nothing is there, floored; so does a public URI.
    f.ingest(DAVE, LIKE, "3klikehidden", subject_record(LIKE, &hidden_post, "2026-10-01T00:00:00Z")).await;
    let (_, page, _) = json_of(&base, GET_LIKES, &format!("uri={hidden_post}"), DAVE).await;
    assert_eq!(actor_dids(&page, "likes"), [DAVE], "readable to its audience");
    let misses = || async {
        let mut out = Vec::new();
        for method in [GET_LIKES, GET_REPOSTED_BY, GET_QUOTES] {
            let (body, _, took) = json_of(&base, method, &format!("uri={hidden_post}"), CAROL).await;
            assert!(took >= FLOOR, "{method} miss took {took:?}");
            out.push(body);
        }
        out
    };
    let unreadable = misses().await;
    assert_eq!(unreadable[0], format!(r#"{{"uri":"{hidden_post}","likes":[]}}"#));
    assert_eq!(unreadable[1], format!(r#"{{"uri":"{hidden_post}","repostedBy":[]}}"#));
    assert_eq!(unreadable[2], format!(r#"{{"uri":"{hidden_post}","posts":[]}}"#));
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3khiddenpost", REV3, None).await;
    assert_eq!(misses().await, unreadable, "unreadable and nonexistent alike");
    let (body, _, _) = json_of(&base, GET_LIKES, &format!("uri={bob_public}"), CAROL).await;
    assert_eq!(body, format!(r#"{{"uri":"{bob_public}","likes":[]}}"#), "not a private post");
    for query in ["", "uri=x&limit=0", "uri=x&limit=101"] {
        let (status, _, _) = get(&base, GET_LIKES, query, CAROL, CAROL_KEY).await;
        assert_eq!(status, 400, "{query}");
    }

    // The lifecycle: a public post's delete takes its quote reference; the
    // purges take a member's.
    f.apply_op(BOB, Action::Delete, PUBLIC_POST, "3kpubquote22", REV3, None).await;
    let refs: Vec<String> = sqlx::query_scalar("SELECT uri FROM private_quote_refs").fetch_all(&f.pool).await.expect("refs");
    assert_eq!(refs, [format!("at://{DAVE}/{PUBLIC_POST}/3kdavequote2")]);
    let mut conn = f.pool.acquire().await.expect("conn");
    KryphocronPrivateTier.purge_member_removed_batch(&mut conn, DAVE, 100).await.expect("purge");
    drop(conn);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM private_quote_refs").fetch_one(&f.pool).await.expect("count");
    assert_eq!(left, 0);
    db.drop_db().await;
}

#[tokio::test]
async fn private_interactions_on() {
    interactions_in(PublicMode::On, "s5-interactions-on").await;
}

#[tokio::test]
async fn private_interactions_off() {
    interactions_in(PublicMode::Off, "s5-interactions-off").await;
}

/// Every page of Carol's timeline, one item a page, as bytes.
async fn timeline_pages(base: &str) -> Vec<String> {
    let mut pages = Vec::new();
    let mut query = "limit=1".to_string();
    for _ in 0..6 {
        let (body, v, _) = json_of(base, TIMELINE, &query, CAROL).await;
        pages.push(body);
        match v["cursor"].as_str() {
            Some(c) => query = format!("limit=1&cursor={}", urlencoding(c)),
            None => break,
        }
    }
    pages
}

/// T-14 (P-5 reposts), in On and in Off (T-1): a followed member's repost
/// of a post Carol reads is a timeline item with `reason`, ordered by the
/// repost's time; a repost of a post she cannot read, and a followed
/// non-member's repost, leave no item and no cursor difference; a muted
/// reposter's item is absent. The reposter's author feed carries it too.
async fn repost_items_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let aud_dave = f.audience(ALICE, "3kauddave222", Some("list"), &[DAVE, BOB]).await;
    let aud_bob = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    let hidden = f.private_post(ALICE, "3khiddenpost", &aud_dave, None).await;
    let bobs = f.private_post(BOB, "3kbobspost22", &aud_bob, None).await;
    // Carol follows Bob and Eve, not Alice.
    exec(&f.pool, &follow_sql(CAROL, BOB)).await;
    exec(&f.pool, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) \
                            VALUES ('at://{CAROL}/app.bsky.graph.follow/eve', '{CAROL}', '{EVE}', '3k', now())")).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let before = timeline_pages(&base).await;
    assert_eq!(before.len(), 1, "Bob's post alone: {before:?}");

    // A repost of a post Carol cannot read, and a non-member's repost of
    // one she can: no item, no cursor difference.
    f.ingest(BOB, REPOST, "3krephidden2", subject_record(REPOST, &hidden, "2026-09-28T00:00:00Z")).await;
    f.ingest(EVE, REPOST, "3krepeve2222", subject_record(REPOST, &t, "2026-09-29T00:00:00Z")).await;
    assert_eq!(timeline_pages(&base).await, before);

    // Bob's repost of T, newer than his own post though T is not.
    f.ingest(BOB, REPOST, "3kreptpost22", subject_record(REPOST, &t, "2026-09-28T12:00:00Z")).await;
    let repost_uri = format!("at://{BOB}/{REPOST}/3kreptpost22");
    let (_, v, _) = json_of(&base, TIMELINE, "", CAROL).await;
    let feed = v["feed"].as_array().expect("feed");
    assert_eq!(feed.len(), 2, "{v}");
    assert_eq!(feed[0]["uri"], t.as_str());
    let reason = &feed[0]["reason"];
    assert_eq!(reason["$type"], "app.bsky.feed.defs#reasonRepost");
    assert_eq!((reason["by"]["did"].as_str(), reason["uri"].as_str()), (Some(BOB), Some(repost_uri.as_str())));
    assert_eq!(reason["indexedAt"], "2026-09-28T12:00:00.000Z");
    assert_eq!(feed[0]["record"]["text"], "3ktpost22222", "the subject read for Carol");
    assert!(feed[0]["likeCount"].is_i64(), "decorated as any item");
    assert_eq!(feed[1]["uri"], bobs.as_str());
    assert!(feed[1].get("reason").is_none());
    // Paged one at a time, each item once.
    let pages = timeline_pages(&base).await;
    assert_eq!(pages.len(), 2, "{pages:?}");
    assert!(pages[0].contains("reasonRepost") && pages[1].contains("3kbobspost22"), "{pages:?}");
    // Bob's author feed carries his repost too.
    let (_, v, _) = json_of(&base, FEED, &format!("actor={BOB}"), CAROL).await;
    let uris: Vec<&str> = v["feed"].as_array().expect("feed").iter().filter_map(|i| i["uri"].as_str()).collect();
    assert_eq!(uris, [t.as_str(), bobs.as_str()]);
    assert_eq!(v["feed"][0]["reason"]["uri"], repost_uri.as_str());
    // Reader-relative: Dave cannot read T or Bob's post, but reads the
    // post Carol cannot, so Bob's author feed gives him that repost alone.
    let (_, v, _) = json_of(&base, FEED, &format!("actor={BOB}"), DAVE).await;
    let feed = v["feed"].as_array().expect("feed");
    assert_eq!(feed.len(), 1, "{v}");
    assert_eq!((feed[0]["uri"].as_str(), feed[0]["reason"]["by"]["did"].as_str()), (Some(hidden.as_str()), Some(BOB)));

    // A muted reposter's items are absent.
    exec(&f.pool, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{CAROL}', '{BOB}')")).await;
    let (body, _, _) = json_of(&base, TIMELINE, "", CAROL).await;
    assert_eq!(body, r#"{"feed":[]}"#);
    db.drop_db().await;
}

#[tokio::test]
async fn private_repost_items_on() {
    repost_items_in(PublicMode::On, "s5-reposts-on").await;
}

#[tokio::test]
async fn private_repost_items_off() {
    repost_items_in(PublicMode::Off, "s5-reposts-off").await;
}

const LIST_NOTIFICATIONS: &str = "app.nearhorizon.private.listNotifications";
const GET_UNREAD_COUNT: &str = "app.nearhorizon.private.getUnreadCount";
const UPDATE_SEEN: &str = "app.nearhorizon.private.updateSeen";

/// A private notification row as the tests compare it: reason, author,
/// subject, record, and whether it holds the public record.
type NotifiedRow = (String, String, Option<String>, String, bool);

fn notified(reason: &str, author: &str, subject: Option<&String>, record: &str, public_record: bool) -> NotifiedRow {
    (reason.into(), author.into(), subject.cloned(), record.into(), public_record)
}

/// `recipient`'s private notification rows, in the order they were written.
async fn private_rows(pool: &PgPool, recipient: &str) -> Vec<NotifiedRow> {
    sqlx::query_as(
        "SELECT reason, author_did, reason_subject, record_uri, public_record IS NOT NULL FROM private_notifications \
         WHERE recipient_did = $1 ORDER BY id",
    )
    .bind(recipient)
    .fetch_all(pool)
    .await
    .expect("rows")
}

/// A public post replying to `parent` (its own root), made at `at`,
/// mentioning `mentioned` when given.
fn public_reply(parent: &str, at: &str, mentioned: Option<&str>) -> Value {
    let mut record = json!({"$type": PUBLIC_POST, "text": "replying", "createdAt": at,
        "reply": {"root": {"uri": parent, "cid": "bafyroot"}, "parent": {"uri": parent, "cid": "bafyparent"}}});
    if let Some(did) = mentioned {
        record["facets"] = json!([mention(did)]);
    }
    record
}

/// `who`'s public `listNotifications`, from the hydrator.
async fn public_notifications(f: &Fixture, who: &str) -> Vec<Value> {
    let viewer = crate::hydration::Viewer {
        did: Some(who.into()),
        hydrate: true,
    };
    let page = f
        .app
        .hydrator
        .list_notifications(&viewer, &crate::xrpc::pools::Requester::test(), 100, None, &[])
        .await
        .expect("db")
        .expect("view");
    page.inner()["notifications"].as_array().cloned().unwrap_or_default()
}

/// `who`'s public `getUnreadCount`, from the hydrator.
async fn public_unread(f: &Fixture, who: &str) -> i64 {
    let viewer = crate::hydration::Viewer {
        did: Some(who.into()),
        hydrate: true,
    };
    f.app.hydrator.unread_count(&viewer).await.expect("db").expect("view").inner()["count"].as_i64().expect("count")
}

/// The fields of a public notification Prism derives (T-15): `uri`,
/// `reasonSubject`, `record.subject.uri` and `record.via.uri`.
fn derived_uris(n: &Value) -> Vec<String> {
    [&n["uri"], &n["reasonSubject"], &n["record"]["subject"]["uri"], &n["record"]["via"]["uri"]]
        .into_iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect()
}

/// No member's public notifications name a private post in a field Prism
/// derives, and no member's public unread count counts one.
async fn no_public_notification_names_a_private_post(f: &Fixture, case: &str) {
    for who in [ALICE, BOB, CAROL, DAVE] {
        for n in public_notifications(f, who).await {
            for uri in derived_uris(&n) {
                assert!(!crate::hydration::normalize::is_private_uri(&uri), "{case}: {who}'s public notification names {uri}: {n}");
            }
        }
    }
}

/// T-15 (P-9 writes), T-17's notify half and chainlink #717, in On and in
/// Off (T-1). Alice's post T is under her `list` audience of Bob, Carol and
/// Dave; Eve is no member.
/// - Private posts notify: a reply its parent's and its claimed root's
///   authors, a quote the quoted post's author, a mention the mentioned
///   member; never a non-member.
/// - Members' public likes, reposts, quotes and replies of T notify Alice
///   privately, in every mode; a quote or reply keeps its public record. A
///   non-member's notify nobody. No `like-via-repost` is written.
/// - In On, no public notification names T in a field Prism derives, and
///   public rows left from before S6 that name it are neither listed nor
///   counted (#717). A public record's own content is left as written.
/// - A muted thread writes nothing; deletes remove rows; a 3-day-old like
///   written by backfill, catch-up or `requestRecord` writes nothing.
async fn notification_writes_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;

    // From private posts.
    let r1 = f.private_post(BOB, "3kreplybob22", &aud, Some((&t, &t))).await;
    let r2 = f.private_post(CAROL, "3kreplycar22", &aud, Some((&t, &r1))).await;
    let m = f.post_with(BOB, "3kmention222", &aud, json!({"facets": [mention(CAROL), mention(EVE)]})).await;
    let q = f.post_with(DAVE, "3kquote22222", &aud, quote_of(&t)).await;
    let eve_post = format!("at://{EVE}/{PUBLIC_POST}/3kevepost222");
    f.private_post(BOB, "3kreplyeve22", &aud, Some((&eve_post, &eve_post))).await;
    assert_eq!(
        private_rows(&f.pool, ALICE).await,
        [notified("reply", BOB, Some(&t), &r1, false), notified("reply", CAROL, Some(&t), &r2, false), notified("quote", DAVE, Some(&t), &q, false)]
    );
    assert_eq!(private_rows(&f.pool, BOB).await, [notified("reply", CAROL, Some(&r1), &r2, false)]);
    assert_eq!(private_rows(&f.pool, CAROL).await, [notified("mention", BOB, None, &m, false)]);
    assert!(private_rows(&f.pool, EVE).await.is_empty(), "a non-member is never notified");

    // From public records naming T.
    let at = "2026-10-01T10:00:00Z";
    f.ingest(BOB, LIKE, "3klikebob222", subject_record(LIKE, &t, at)).await;
    f.ingest(BOB, REPOST, "3krepostbob2", subject_record(REPOST, &t, at)).await;
    let (bob_like, bob_repost) = (format!("at://{BOB}/{LIKE}/3klikebob222"), format!("at://{BOB}/{REPOST}/3krepostbob2"));
    let mut through = subject_record(LIKE, &t, at);
    through["via"] = json!({"uri": bob_repost, "cid": "bafy3krepostbob2"});
    f.ingest(CAROL, LIKE, "3klikecarol2", through).await;
    let carol_like = format!("at://{CAROL}/{LIKE}/3klikecarol2");
    f.ingest(BOB, PUBLIC_POST, "3kpubquote22", public_quote(&t, at)).await;
    f.ingest(BOB, PUBLIC_POST, "3kpubreply22", public_reply(&t, at, Some(CAROL))).await;
    let (bob_quote, bob_reply) = (format!("at://{BOB}/{PUBLIC_POST}/3kpubquote22"), format!("at://{BOB}/{PUBLIC_POST}/3kpubreply22"));
    f.ingest(EVE, LIKE, "3klikeeve222", subject_record(LIKE, &t, at)).await;
    f.ingest(EVE, REPOST, "3krepeve2222", subject_record(REPOST, &t, at)).await;
    f.ingest(EVE, PUBLIC_POST, "3kevereply22", public_reply(&t, at, None)).await;
    let from_public = [
        notified("like", BOB, Some(&t), &bob_like, false),
        notified("repost", BOB, Some(&t), &bob_repost, false),
        notified("like", CAROL, Some(&t), &carol_like, false),
        notified("quote", BOB, Some(&t), &bob_quote, true),
        notified("reply", BOB, Some(&t), &bob_reply, true),
    ];
    assert_eq!(private_rows(&f.pool, ALICE).await[3..], from_public, "{mode:?}");
    assert_eq!(private_rows(&f.pool, BOB).await.len(), 1, "no via reason is written");
    let stored: Value = sqlx::query_scalar("SELECT public_record FROM private_notifications WHERE record_uri = $1")
        .bind(&bob_reply)
        .fetch_one(&f.pool)
        .await
        .expect("record");
    assert_eq!(stored["reply"]["parent"]["uri"], t.as_str(), "the public record as written");

    // #717: no public row names T, written or hydrated.
    let (written, via): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE reason_subject LIKE '%/tools.kryphocron.feed.postPrivate/%'), \
           count(*) FILTER (WHERE reason IN ('like-via-repost', 'repost-via-repost')) FROM notifications",
    )
    .fetch_one(&f.pool)
    .await
    .expect("count");
    assert_eq!((written, via), (0, 0));
    no_public_notification_names_a_private_post(&f, "written").await;
    if mode.is_on() {
        // Carol's public mention in Bob's public reply is listed, its record
        // as written, naming T in `record.reply`.
        let carols = public_notifications(&f, CAROL).await;
        let mention_row = carols.iter().find(|n| n["reason"] == "mention").expect("Carol's public mention");
        assert_eq!((mention_row["uri"].as_str(), mention_row["record"]["reply"]["parent"]["uri"].as_str()), (Some(bob_reply.as_str()), Some(t.as_str())));
    }
    // Rows left from before S6: one names T as its subject, one is a like
    // of T through Bob's repost. Neither is listed or counted.
    let unread_before = (public_unread(&f, ALICE).await, public_unread(&f, BOB).await);
    sqlx::query(
        "INSERT INTO notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         VALUES ($1, $2, 'like', $3, $4, 'bafy'), ($2, $5, 'like-via-repost', $6, $7, 'bafy')",
    )
    .bind(ALICE)
    .bind(BOB)
    .bind(&t)
    .bind(&bob_like)
    .bind(CAROL)
    .bind(&bob_repost)
    .bind(&carol_like)
    .execute(&f.pool)
    .await
    .expect("leftover rows");
    no_public_notification_names_a_private_post(&f, "leftover").await;
    assert_eq!((public_unread(&f, ALICE).await, public_unread(&f, BOB).await), unread_before, "leftover rows are not counted");
    let bobs = public_notifications(&f, BOB).await;
    assert!(bobs.iter().all(|n| n["reason"] != "like-via-repost"), "{bobs:?}");

    // T-17's notify half: once Alice mutes T, a reply in it notifies her
    // no more, private or public.
    let alice = crate::hydration::Viewer {
        did: Some(ALICE.into()),
        hydrate: true,
    };
    f.app.hydrator.set_thread_mute(&alice, &t, true).await.expect("db").expect("muted");
    let before = private_rows(&f.pool, ALICE).await;
    f.private_post(DAVE, "3kreplydave2", &aud, Some((&t, &t))).await;
    f.private_post(CAROL, "3kreplycar32", &aud, Some((&t, &r1))).await;
    f.ingest(DAVE, PUBLIC_POST, "3kpubreply32", public_reply(&t, at, None)).await;
    assert_eq!(private_rows(&f.pool, ALICE).await, before, "a muted thread writes nothing");
    assert_eq!(private_rows(&f.pool, BOB).await.len(), 2, "Bob did not mute it");
    f.app.hydrator.set_thread_mute(&alice, &t, false).await.expect("db").expect("unmuted");

    // Deletes take their rows: the public like, repost, quote and reply,
    // and a private reply (its rows for Alice and Bob).
    for (collection, rkey) in [(LIKE, "3klikebob222"), (REPOST, "3krepostbob2"), (PUBLIC_POST, "3kpubquote22"), (PUBLIC_POST, "3kpubreply22")] {
        f.apply_op(BOB, Action::Delete, collection, rkey, REV2, None).await;
    }
    f.apply_op(CAROL, Action::Delete, NSID_POST_PRIVATE, "3kreplycar22", REV2, None).await;
    assert_eq!(
        private_rows(&f.pool, ALICE).await,
        [notified("reply", BOB, Some(&t), &r1, false), notified("quote", DAVE, Some(&t), &q, false), notified("like", CAROL, Some(&t), &carol_like, false)]
    );
    assert_eq!(private_rows(&f.pool, BOB).await.len(), 1, "only Carol's second reply");

    // C-11: a 3-day-old like written by backfill, catch-up or
    // `requestRecord` writes nothing; live, or new, it notifies.
    let old = (Utc::now() - chrono::Duration::days(3)).to_rfc3339();
    let new = Utc::now().to_rfc3339();
    for (source, rkey, created) in [
        (WriteSource::Backfill, "3koldlike001", &old),
        (WriteSource::CatchUp, "3koldlike002", &old),
        (WriteSource::IndexRequest, "3koldlike003", &old),
        (WriteSource::Firehose, "3koldlike004", &old),
        (WriteSource::Backfill, "3knewlike005", &new),
    ] {
        f.apply_from(source, DAVE, Action::Create, LIKE, rkey, REV, Some(subject_record(LIKE, &t, created))).await;
    }
    let daves: Vec<String> = private_rows(&f.pool, ALICE).await.into_iter().filter(|r| r.0 == "like" && r.1 == DAVE).map(|r| r.3).collect();
    assert_eq!(daves, [format!("at://{DAVE}/{LIKE}/3koldlike004"), format!("at://{DAVE}/{LIKE}/3knewlike005")]);

    // The purges take what a member received and what it wrote.
    let mut conn = f.pool.acquire().await.expect("conn");
    KryphocronPrivateTier.purge_member_removed_batch(&mut conn, BOB, 100).await.expect("purge");
    drop(conn);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM private_notifications WHERE recipient_did = $1 OR author_did = $1")
        .bind(BOB)
        .fetch_one(&f.pool)
        .await
        .expect("count");
    assert_eq!(left, 0);
    db.drop_db().await;
}

#[tokio::test]
async fn private_notification_writes_on() {
    notification_writes_in(PublicMode::On, "s6-writes-on").await;
}

#[tokio::test]
async fn private_notification_writes_off() {
    notification_writes_in(PublicMode::Off, "s6-writes-off").await;
}

/// #725, in On and in Off: a private reply, a public like and a public
/// reply of Alice's post T, each deleted at REV2 and created again at REV3,
/// keep Alice's private notifications when a late copy of the REV2 delete
/// arrives; a delete above REV3 still takes them.
async fn late_delete_keeps_newer_notifications_in(mode: PublicMode, test: &str) {
    const REV4: &str = "3kaaaaaaaaad2";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    let at = "2026-10-01T10:00:00Z";
    let records = [
        (NSID_POST_PRIVATE, "3kreplybob22", f.post_record(BOB, "3kreplybob22", &aud, Some((&t, &t))).await),
        (LIKE, "3klikebob222", subject_record(LIKE, &t, at)),
        (PUBLIC_POST, "3kpubreply22", public_reply(&t, at, None)),
    ];
    let alices = |pool: &PgPool| {
        let pool = pool.clone();
        async move { private_rows(&pool, ALICE).await.into_iter().map(|r| (r.0, r.3)).collect::<Vec<_>>() }
    };
    for (collection, rkey, record) in &records {
        f.ingest(BOB, collection, rkey, record.clone()).await;
    }
    let written = alices(&f.pool).await;
    assert_eq!(written.len(), 3, "{mode:?}: {written:?}");
    for (collection, rkey, _) in &records {
        f.apply_op(BOB, Action::Delete, collection, rkey, REV2, None).await;
    }
    assert!(alices(&f.pool).await.is_empty(), "{mode:?}: the deletes in order take them");
    for (collection, rkey, record) in &records {
        f.apply_op(BOB, Action::Create, collection, rkey, REV3, Some(record.clone())).await;
    }
    assert_eq!(alices(&f.pool).await, written, "{mode:?}: the newer records write them again");
    for (collection, rkey, _) in &records {
        f.apply_op(BOB, Action::Delete, collection, rkey, REV2, None).await;
    }
    assert_eq!(alices(&f.pool).await, written, "{mode:?}: the late deletes leave the newer records'");
    for (collection, rkey, _) in &records {
        f.apply_op(BOB, Action::Delete, collection, rkey, REV4, None).await;
    }
    assert!(alices(&f.pool).await.is_empty(), "{mode:?}: newer deletes take them");
    db.drop_db().await;
}

#[tokio::test]
async fn late_delete_keeps_newer_private_notifications_on() {
    late_delete_keeps_newer_notifications_in(PublicMode::On, "late-delete-on").await;
}

#[tokio::test]
async fn late_delete_keeps_newer_private_notifications_off() {
    late_delete_keeps_newer_notifications_in(PublicMode::Off, "late-delete-off").await;
}

/// #727: Bob's three likes and three reposts of Alice's post T count once
/// for Carol, are listed once, and his newest is his `viewer.like` and
/// `viewer.repost`; deleting two still counts 1, the third 0.
#[tokio::test]
async fn an_accounts_duplicate_interactions_count_once() {
    let test = "duplicate-interactions";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let rkeys = ["3kdupone2222", "3kduptwo2222", "3kdupthree22"];
    for (rkey, at) in rkeys.iter().zip(["2026-10-01T10:00:00Z", "2026-10-01T12:00:00Z", "2026-10-01T11:00:00Z"]) {
        f.ingest(BOB, LIKE, rkey, subject_record(LIKE, &t, at)).await;
        f.ingest(BOB, REPOST, rkey, subject_record(REPOST, &t, at)).await;
    }
    let (_, p) = posts_of(&base, CAROL, &[&t]).await;
    assert_eq!(counts_of(&p[0]), [0, 1, 1, 0], "{}", p[0]);
    for (method, key) in [(GET_LIKES, "likes"), (GET_REPOSTED_BY, "repostedBy")] {
        let (_, page, _) = json_of(&base, method, &format!("uri={t}"), CAROL).await;
        assert_eq!(actor_dids(&page, key), [BOB], "{method}: one entry per account");
    }
    let (_, p) = posts_of(&base, BOB, &[&t]).await;
    assert_eq!(p[0]["viewer"]["like"], format!("at://{BOB}/{LIKE}/3kduptwo2222"), "the newest");
    assert_eq!(p[0]["viewer"]["repost"], format!("at://{BOB}/{REPOST}/3kduptwo2222"));
    for rkey in &rkeys[..2] {
        f.apply_op(BOB, Action::Delete, LIKE, rkey, REV2, None).await;
        f.apply_op(BOB, Action::Delete, REPOST, rkey, REV2, None).await;
    }
    let (_, p) = posts_of(&base, CAROL, &[&t]).await;
    assert_eq!(counts_of(&p[0]), [0, 1, 1, 0], "two of three deleted: {}", p[0]);
    let (_, p) = posts_of(&base, BOB, &[&t]).await;
    assert_eq!(p[0]["viewer"]["like"], format!("at://{BOB}/{LIKE}/3kdupthree22"), "the one left");
    f.apply_op(BOB, Action::Delete, LIKE, rkeys[2], REV2, None).await;
    f.apply_op(BOB, Action::Delete, REPOST, rkeys[2], REV2, None).await;
    let (_, p) = posts_of(&base, CAROL, &[&t]).await;
    assert_eq!(counts_of(&p[0]), [0, 0, 0, 0], "the last deleted: {}", p[0]);
    db.drop_db().await;
}

/// `reader`'s private notifications with `query`: the page and the time
/// taken.
async fn notifications_of(base: &str, reader: &str, query: &str) -> (Value, Duration) {
    let (_, page, took) = json_of(base, LIST_NOTIFICATIONS, query, reader).await;
    (page, took)
}

async fn unread_of(base: &str, reader: &str) -> i64 {
    let (_, v, took) = json_of(base, GET_UNREAD_COUNT, "", reader).await;
    assert!(took >= FLOOR, "getUnreadCount took {took:?}");
    v["count"].as_i64().expect("count")
}

/// T-16 (P-9 reads), in On and in Off (T-1).
/// - A mention in a post the recipient cannot read is neither listed nor
///   counted; once the post's audience admits the recipient, it is.
/// - A blocked author, either way and in either block table, is dropped.
/// - `updateSeen` clamps to now and never moves back; `isRead`, `seenAt`.
/// - A like of a post its author then deletes, and a member's public reply
///   naming a private post that never existed, are neither counted nor
///   listed (review 10).
/// - `reasons` filters; the cursor walks the caller's rows; floored.
async fn notification_reads_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let aud_bob = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    let m = f.post_with(BOB, "3kmention222", &aud_bob, json!({"facets": [mention(DAVE), mention(CAROL)]})).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    // Dave cannot read Bob's post: his mention is not listed or counted.
    let (page, took) = notifications_of(&base, DAVE, "").await;
    assert!(took >= FLOOR, "listNotifications took {took:?}");
    assert_eq!(page, json!({"notifications": []}));
    assert_eq!(unread_of(&base, DAVE).await, 0);
    // Carol can.
    let (page, _) = notifications_of(&base, CAROL, "").await;
    let n = &page["notifications"][0];
    assert_eq!((n["uri"].as_str(), n["reason"].as_str(), n["author"]["did"].as_str()), (Some(m.as_str()), Some("mention"), Some(BOB)));
    assert_eq!((&n["record"]["text"], &n["isRead"], &n["cid"]), (&json!("3kmention222"), &json!(false), &json!("bafy3kmention222")));
    assert!(n.get("reasonSubject").is_none() && n["labels"] == json!([]) && n["indexedAt"].is_string(), "{n}");
    assert_eq!(unread_of(&base, CAROL).await, 1);

    // The audience admits Dave: listed and counted.
    let widened = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [CAROL, DAVE], "createdAt": "2026-09-26T00:00:00Z"});
    f.apply_op(BOB, Action::Update, NSID_AUDIENCE, "3kaudbob2222", REV2, Some(widened)).await;
    f.ready_all().await;
    let (page, _) = notifications_of(&base, DAVE, "").await;
    assert_eq!(page["notifications"][0]["uri"], m.as_str(), "{page}");
    assert_eq!(unread_of(&base, DAVE).await, 1);

    // A blocked author is dropped, either way, public or private block.
    for sql in [
        block_sql(DAVE, BOB),
        block_sql(BOB, DAVE),
        format!("INSERT INTO private_blocks (uri, author_did, subject_did, rev, created_at) \
                 VALUES ('at://{DAVE}/tools.kryphocron.graph.block/1', '{DAVE}', '{BOB}', '3k', now())"),
    ] {
        exec(&f.pool, &sql).await;
        let (page, _) = notifications_of(&base, DAVE, "").await;
        assert_eq!(page, json!({"notifications": []}), "{sql}");
        assert_eq!(unread_of(&base, DAVE).await, 0, "{sql}");
        exec(&f.pool, "DELETE FROM blocks").await;
        exec(&f.pool, "DELETE FROM private_blocks").await;
    }
    // A full mute drops it too.
    exec(&f.pool, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{DAVE}', '{BOB}')")).await;
    assert_eq!(unread_of(&base, DAVE).await, 0);
    exec(&f.pool, "DELETE FROM mutes").await;
    assert_eq!(unread_of(&base, DAVE).await, 1);

    // updateSeen clamps to now and never moves back.
    assert_eq!(post(&base, UPDATE_SEEN, &json!({"seenAt": "2099-01-01T00:00:00Z"}), DAVE).await, (200, "{}".to_string()));
    assert_eq!(unread_of(&base, DAVE).await, 0);
    let (page, _) = notifications_of(&base, DAVE, "").await;
    assert_eq!(page["notifications"][0]["isRead"], true);
    let seen = page["seenAt"].as_str().expect("seenAt").to_string();
    assert!(seen.as_str() < "2099", "clamped to now: {seen}");
    assert_eq!(post(&base, UPDATE_SEEN, &json!({"seenAt": "2000-01-01T00:00:00Z"}), DAVE).await.0, 200);
    assert_eq!(unread_of(&base, DAVE).await, 0);
    assert_eq!(notifications_of(&base, DAVE, "").await.0["seenAt"], seen.as_str(), "never moves back");
    for bad in [json!({}), json!({"seenAt": "yesterday"})] {
        let (status, body) = post(&base, UPDATE_SEEN, &bad, DAVE).await;
        assert_eq!(status, 400, "{body}");
        assert!(body.contains("InvalidRequest"), "{body}");
    }

    // Review 10: Dave likes T and Alice deletes T; Bob's public reply names
    // a private post of Alice's that never existed.
    f.ingest(DAVE, LIKE, "3klikedave22", subject_record(LIKE, &t, "2026-10-01T10:00:00Z")).await;
    let ghost = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere222");
    f.ingest(BOB, PUBLIC_POST, "3kghostreply", public_reply(&ghost, "2026-10-01T11:00:00Z", None)).await;
    assert_eq!(private_rows(&f.pool, ALICE).await.len(), 2, "both rows are written");
    let (page, _) = notifications_of(&base, ALICE, "").await;
    let like = format!("at://{DAVE}/{LIKE}/3klikedave22");
    assert_eq!(page["notifications"].as_array().map(Vec::len), Some(1), "{page}");
    let n = &page["notifications"][0];
    assert_eq!((n["uri"].as_str(), n["reason"].as_str(), n["reasonSubject"].as_str()), (Some(like.as_str()), Some("like"), Some(t.as_str())));
    assert_eq!(n["record"], json!({"$type": LIKE, "subject": {"uri": t, "cid": "bafysubject"}, "createdAt": "2026-10-01T10:00:00.000Z"}));
    assert_eq!(unread_of(&base, ALICE).await, 1);
    // `reasons` filters.
    assert_eq!(notifications_of(&base, ALICE, "reasons=reply").await.0, json!({"notifications": []}));
    assert_eq!(notifications_of(&base, ALICE, "reasons=like&reasons=reply").await.0["notifications"].as_array().map(Vec::len), Some(1));
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3ktpost22222", REV2, None).await;
    assert_eq!(notifications_of(&base, ALICE, "").await.0, json!({"notifications": []}));
    assert_eq!(unread_of(&base, ALICE).await, 0);

    // The cursor walks the caller's rows: Bob's second mention of Carol.
    let second = f.post_with(BOB, "3kmention333", &aud_bob, json!({"facets": [mention(CAROL)]})).await;
    f.ready_all().await;
    let mut seen_uris = Vec::new();
    let mut query = "limit=1".to_string();
    for _ in 0..4 {
        let (page, _) = notifications_of(&base, CAROL, &query).await;
        seen_uris.extend(page["notifications"].as_array().into_iter().flatten().filter_map(|n| n["uri"].as_str().map(str::to_string)));
        match page["cursor"].as_str() {
            Some(c) => query = format!("limit=1&cursor={}", urlencoding(c)),
            None => break,
        }
    }
    assert_eq!(seen_uris, [second, m]);
    for query in ["limit=0", "limit=101", "cursor=garbage", "cursor=2026-10-01T00:00:00Z::x"] {
        let (status, body, _) = get(&base, LIST_NOTIFICATIONS, query, CAROL, CAROL_KEY).await;
        assert_eq!(status, 400, "{query}: {body}");
    }
    db.drop_db().await;
}

/// T-F8 (fast-reads design §5.2 B-2): Bob's author feed, read by Bob.
/// - Replies to Carol's readable post carry it as root and parent, as a
///   decorated `#privatePostView`; the shared parent is read once.
/// - A reply to an item of the page reuses that item; it is not read again.
/// - A parent Bob cannot read and one that does not exist are the same
///   `#notFoundPost` but for the URI.
/// - A public parent is `#publicPostRef` with the record's cids.
/// - A top-level post has no `reply`; the `context` stage is recorded.
/// - A page of `limit` replies to distinct parents is full and has a cursor:
///   the context reads are outside the read cap.
#[tokio::test]
async fn t_f8_feed_items_carry_their_reply_context() {
    let test = "tf8-reply-context";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, test).await;
    let aud_carol = f.audience(CAROL, "3kaudcarol22", Some("list"), &[BOB]).await;
    let aud_dave = f.audience(DAVE, "3kauddave222", Some("list"), &[]).await;
    let aud_bob = f.audience(BOB, "3kaudbob2222", Some("list"), &[ALICE]).await;
    let c1 = f.private_post(CAROL, "3kcarol22222", &aud_carol, None).await;
    let d1 = f.private_post(DAVE, "3kdave222222", &aud_dave, None).await;
    let ghost = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3knothere222");
    let public = format!("at://{DAVE}/{PUBLIC_POST}/3kpubpost222");
    let b0 = f.private_post(BOB, "3kbob0222222", &aud_bob, None).await;
    let b1 = f.private_post(BOB, "3kbob1222222", &aud_carol, Some((&c1, &c1))).await;
    let b2 = f.private_post(BOB, "3kbob2222222", &aud_carol, Some((&c1, &c1))).await;
    let b3 = f.private_post(BOB, "3kbob3222222", &aud_bob, Some((&b0, &b0))).await;
    let b4 = f.private_post(BOB, "3kbob4222222", &aud_dave, Some((&d1, &d1))).await;
    let b5 = f.private_post(BOB, "3kbob5222222", &aud_bob, Some((&ghost, &ghost))).await;
    let b6 = f.private_post(BOB, "3kbob6222222", &aud_bob, Some((&public, &public))).await;
    f.ready_all().await;
    let base = serve_live(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let hits = |text: &str| crate::test_support::series(text, "private_read_outcome_total", &[r#"outcome="hit""#]).unwrap_or(0.0);
    let before = hits(&handle.render());
    let (_, page, _) = json_of(&base, FEED, &format!("actor={BOB}"), BOB).await;
    let text = handle.render();
    let items = page["feed"].as_array().expect("feed");
    let item = |uri: &str| items.iter().find(|i| i["uri"] == uri).unwrap_or_else(|| panic!("{uri} in {page}")).clone();
    assert_eq!(items.len(), 7, "{page}");
    // Seven items and Carol's post once; Dave's and the missing one fail
    // the prefilter and are not read, Bob's own post is reused.
    assert_eq!(hits(&text) - before, 8.0, "{text}");
    for b in [&b1, &b2] {
        let reply = &item(b)["reply"];
        for side in ["root", "parent"] {
            let v = &reply[side];
            assert_eq!((v["$type"].as_str(), v["uri"].as_str()), (Some("app.nearhorizon.private.defs#privatePostView"), Some(c1.as_str())), "{b}: {reply}");
            assert!(v["likeCount"].is_number() && v["viewer"].is_object() && v["record"]["text"] == "3kcarol22222", "decorated: {v}");
        }
    }
    let reused = &item(&b3)["reply"]["parent"];
    let mut own = item(&b0);
    own.as_object_mut().expect("view").insert("$type".into(), json!("app.nearhorizon.private.defs#privatePostView"));
    assert_eq!(reused, &own, "the item's own view");
    let missing = |b: &str, uri: &str| item(b)["reply"]["parent"].to_string().replace(uri, "URI");
    assert_eq!(missing(&b4, &d1), missing(&b5, &ghost), "unreadable and nonexistent alike");
    assert_eq!(item(&b4)["reply"]["parent"], json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": d1, "notFound": true}));
    assert_eq!(
        item(&b6)["reply"],
        json!({"root": {"$type": "app.nearhorizon.private.defs#publicPostRef", "uri": public, "cid": "bafyroot"},
               "parent": {"$type": "app.nearhorizon.private.defs#publicPostRef", "uri": public, "cid": "bafyparent"}})
    );
    assert!(item(&b0).get("reply").is_none());
    let endpoint = r#"endpoint="getAuthorFeed""#;
    assert_eq!(crate::test_support::series(&text, "private_read_stage_seconds_count", &[endpoint, r#"stage="context""#]), Some(1.0), "{text}");

    // Two replies to distinct parents per page, outside the read cap of 2 × 2.
    let (_, page, _) = json_of(&base, FEED, &format!("actor={BOB}&limit=2"), BOB).await;
    assert_eq!(page["feed"].as_array().map(Vec::len), Some(2), "{page}");
    assert!(page["cursor"].is_string(), "{page}");
    db.drop_db().await;
}

/// T-F9 (fast-reads design §5.2 B-3): Bob's like and repost of Alice's
/// private post carry it as `reasonSubjectPost`, decorated as `getPosts`
/// decorates, read once for both; a mention has no subject and no field;
/// once the subject does not read for Alice (taken down), the notification
/// is what it was without the field.
#[tokio::test]
async fn t_f9_notifications_carry_their_subject() {
    let test = "tf9-subjects";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, test).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let aud_carol = f.audience(CAROL, "3kaudcarol22", Some("list"), &[ALICE]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    f.post_with(CAROL, "3kmention222", &aud_carol, json!({"facets": [mention(ALICE)]})).await;
    let at = "2026-10-01T10:00:00Z";
    f.ingest(BOB, LIKE, "3klikebob222", subject_record(LIKE, &t, at)).await;
    f.ingest(BOB, REPOST, "3krepostbob2", subject_record(REPOST, &t, at)).await;
    f.ready_all().await;
    let base = serve_live(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let hits = |text: &str| crate::test_support::series(text, "private_read_outcome_total", &[r#"outcome="hit""#]).unwrap_or(0.0);
    let before = hits(&handle.render());
    let (page, _) = notifications_of(&base, ALICE, "").await;
    // The mention's post and the one subject.
    assert_eq!(hits(&handle.render()) - before, 2.0, "{page}");
    let of = |page: &Value, reason: &str| {
        page["notifications"].as_array().expect("list").iter().find(|n| n["reason"] == reason).unwrap_or_else(|| panic!("{reason} in {page}")).clone()
    };
    let like = of(&page, "like");
    let subject = &like["reasonSubjectPost"];
    assert_eq!((subject["uri"].as_str(), subject["record"]["text"].as_str()), (Some(t.as_str()), Some("3ktpost22222")), "{like}");
    assert!(subject["likeCount"] == 1 && subject["repostCount"] == 1 && subject["viewer"].is_object(), "decorated: {subject}");
    assert_eq!(of(&page, "repost")["reasonSubjectPost"], *subject, "the same subject");
    assert!(of(&page, "mention").get("reasonSubjectPost").is_none());

    exec(&f.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{t}', '!takedown')")).await;
    let (after, _) = notifications_of(&base, ALICE, "").await;
    let mut without = like.clone();
    without.as_object_mut().expect("object").remove("reasonSubjectPost");
    assert_eq!(of(&after, "like"), without, "no field, nothing else changed");
    db.drop_db().await;
}

/// T-F7 (fast-reads design §5.2 B-1): with an unread row of every reason
/// and every private post readable, `getUnreadCount {reasons}` equals the
/// unread items `listNotifications` returns for the same `reasons`; no
/// `reasons` is the count without them; a reason no row can have counts
/// nothing, as it lists nothing.
#[tokio::test]
async fn t_f7_unread_count_takes_reasons() {
    let test = "tf7-unread-reasons";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let aud_carol = f.audience(CAROL, "3kaudcarol22", Some("list"), &[ALICE]).await;
    let aud_dave = f.audience(DAVE, "3kauddave222", Some("list"), &[ALICE]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    f.post_with(DAVE, "3kquote22222", &aud_dave, quote_of(&t)).await;
    f.post_with(CAROL, "3kmention222", &aud_carol, json!({"facets": [mention(ALICE)]})).await;
    let at = "2026-10-01T10:00:00Z";
    f.ingest(BOB, PUBLIC_POST, "3kpubreply22", public_reply(&t, at, None)).await;
    f.ingest(BOB, LIKE, "3klikebob222", subject_record(LIKE, &t, at)).await;
    f.ingest(BOB, REPOST, "3krepostbob2", subject_record(REPOST, &t, at)).await;
    f.ready_all().await;
    let base = serve_live(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let count = |query: String| {
        let base = base.clone();
        async move {
            let (_, v, _) = json_of(&base, GET_UNREAD_COUNT, &query, ALICE).await;
            v["count"].as_i64().expect("count")
        }
    };
    let listed = |query: String| {
        let base = base.clone();
        async move {
            let (page, _) = notifications_of(&base, ALICE, &query).await;
            page["notifications"].as_array().expect("list").iter().filter(|n| n["isRead"] == false).count()
        }
    };
    let (page, _) = notifications_of(&base, ALICE, "").await;
    let mut reasons: Vec<&str> = page["notifications"].as_array().expect("list").iter().filter_map(|n| n["reason"].as_str()).collect();
    reasons.sort_unstable();
    assert_eq!(reasons, ["like", "mention", "quote", "reply", "repost"], "rows {:?}", private_rows(&f.pool, ALICE).await);
    assert_eq!(count(String::new()).await, 5, "one of every reason");
    for reasons in [vec!["reply"], vec!["reply", "mention"], vec!["like", "repost"], vec!["quote", "mention", "reply"], vec!["like", "reply", "mention", "quote", "repost"]] {
        let query = reasons.iter().map(|r| format!("reasons={r}")).collect::<Vec<_>>().join("&");
        let n = count(query.clone()).await;
        assert_eq!(i64::try_from(listed(query.clone()).await).expect("small"), n, "{query}");
        assert_eq!(n, i64::try_from(reasons.len()).expect("small"), "{query}");
    }
    for query in ["reasons=follow", "reasons=nothing&reasons=other"] {
        assert_eq!((count(query.to_string()).await, listed(query.to_string()).await), (0, 0), "{query}");
    }
    assert_eq!(count("reasons=follow&reasons=like".to_string()).await, 1, "the known reasons still count");
    db.drop_db().await;
}

#[tokio::test]
async fn private_notification_reads_on() {
    notification_reads_in(PublicMode::On, "s6-reads-on").await;
}

#[tokio::test]
async fn private_notification_reads_off() {
    notification_reads_in(PublicMode::Off, "s6-reads-off").await;
}

// ---- S7: `requestRecord`'s catch-up and `caughtUp` (P-11, §3.8, T-18) ----

const REQUEST_RECORD: &str = "app.nearhorizon.index.requestRecord";
const R1: &str = "3kbbbbbbbbbb2";
const R2: &str = "3kcccccccccc2";
const R3: &str = "3kdddddddddd2";

/// Alice's repository as her PDS serves it: `latest_rev`, `listRecords` in
/// pages of `page_size`, and `getRecord`.
struct Repo {
    pool: PgPool,
    rev: std::sync::Mutex<String>,
    records: std::sync::Mutex<Vec<crate::firehose::backfill::ListedRecord>>,
    page_size: usize,
    /// `listRecords` calls.
    lists: std::sync::atomic::AtomicUsize,
    /// Bump the gap epoch on the next `listRecords` call.
    bump_epoch: std::sync::atomic::AtomicBool,
}

impl Repo {
    fn new(pool: &PgPool, page_size: usize) -> Arc<Self> {
        Arc::new(Self {
            pool: pool.clone(),
            rev: std::sync::Mutex::new(R1.into()),
            records: std::sync::Mutex::new(Vec::new()),
            page_size,
            lists: std::sync::atomic::AtomicUsize::new(0),
            bump_epoch: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Serve `record` at `at://alice/collection/rkey` with `cid`, replacing
    /// any record at that URI.
    fn put(&self, collection: &str, rkey: &str, cid: &str, value: Value) {
        let uri = format!("at://{ALICE}/{collection}/{rkey}");
        let mut records = self.records.lock().expect("records");
        records.retain(|r| r.uri != uri);
        records.push(crate::firehose::backfill::ListedRecord {
            uri,
            cid: cid.into(),
            value,
        });
    }

    fn remove(&self, collection: &str, rkey: &str) {
        let uri = format!("at://{ALICE}/{collection}/{rkey}");
        self.records.lock().expect("records").retain(|r| r.uri != uri);
    }

    fn set_rev(&self, rev: &str) {
        *self.rev.lock().expect("rev") = rev.into();
    }

    fn lists(&self) -> usize {
        self.lists.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl crate::firehose::backfill::BackfillSource for Repo {
    async fn prepare(&self, _: &str) -> Result<String, crate::firehose::backfill::PrepareError> {
        Ok("https://pds.example.com".into())
    }
    async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
        Ok(self.rev.lock().expect("rev").clone())
    }
    async fn list_records(
        &self,
        _: &str,
        did: &str,
        collection: &str,
        cursor: Option<&str>,
    ) -> Result<crate::firehose::backfill::ListPage, String> {
        self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.bump_epoch.swap(false, std::sync::atomic::Ordering::SeqCst) {
            exec(&self.pool, "INSERT INTO worker_state (key, value) VALUES ('gap_epoch', '7'::jsonb) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value").await;
        }
        let prefix = format!("at://{did}/{collection}/");
        let all: Vec<_> = self.records.lock().expect("records").iter().filter(|r| r.uri.starts_with(&prefix)).cloned().collect();
        let start = cursor.map_or(0, |c| c.parse::<usize>().expect("cursor"));
        let end = (start + self.page_size).min(all.len());
        Ok(crate::firehose::backfill::ListPage {
            records: all[start.min(end)..end].to_vec(),
            cursor: (end < all.len()).then(|| end.to_string()),
        })
    }
    async fn get_record(
        &self,
        _: &str,
        did: &str,
        collection: &str,
        rkey: &str,
    ) -> Result<Option<crate::firehose::backfill::ListedRecord>, String> {
        let uri = format!("at://{did}/{collection}/{rkey}");
        Ok(self.records.lock().expect("records").iter().find(|r| r.uri == uri).cloned())
    }
    async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<crate::microcosm::LinksPage, String> {
        Err("unused".into())
    }
}

/// `requestRecord` over `repo`, with a catch-up whose interval no earlier
/// call has started. The interval is an hour, so "a second call within the
/// interval" holds however slowly the suite runs (a 10 s interval could
/// pass under load before the second call, #772); a test that wants the
/// interval over starts a new server.
async fn index_server(f: &Fixture, repo: &Arc<Repo>) -> String {
    use crate::firehose::index_request::{CATCH_UP_DEADLINE, CatchUp, CatchUpConfig};
    let state = crate::xrpc::handlers::index::IndexState {
        app: f.app.clone(),
        source: Arc::clone(repo) as Arc<dyn crate::firehose::backfill::BackfillSource>,
        writer: f.writer.clone(),
        attempts: 1,
        catch_up: CatchUp::new(CatchUpConfig {
            min_interval: Duration::from_secs(3600),
            max_pages: 8,
            reconcile_confirm_limit: 50,
            attempts: 1,
            deadline: CATCH_UP_DEADLINE,
        }),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            crate::xrpc::router::index_router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("serve");
    });
    format!("http://{addr}")
}

/// Alice's `requestRecord` for `uri`: `(status, caughtUp)`.
async fn request_record(base: &str, uri: &str) -> (String, bool) {
    let (status, body) = post(base, REQUEST_RECORD, &json!({ "uri": uri }), ALICE).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    (v["status"].as_str().expect("status").to_string(), v["caughtUp"].as_bool().expect("caughtUp"))
}

async fn applied_rev_of(pool: &PgPool, did: &str) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>("SELECT applied_rev FROM sync_state WHERE did = $1")
        .bind(did)
        .fetch_one(pool)
        .await
        .expect("sync_state")
}

/// Force the prober's fresh head for `did` (the head check reads it).
async fn set_head(pool: &PgPool, did: &str, head: &str) {
    sqlx::query("UPDATE sync_state SET head_rev = $2, head_checked_at = now() WHERE did = $1")
        .bind(did)
        .bind(head)
        .execute(pool)
        .await
        .expect("head");
}

/// The URIs `reader`'s `getPosts(uris)` returns.
async fn readable(base: &str, reader: &str, uris: &[&str]) -> Vec<String> {
    let (_, posts) = posts_of(base, reader, uris).await;
    posts.iter().map(|p| p["uri"].as_str().expect("uri").to_string()).collect()
}

/// Alice's audience listing Bob and her post under it, both stored and
/// served by `repo` with their stored CIDs; Alice's `applied_rev` behind
/// her head, as after she writes.
async fn alice_behind(f: &Fixture, repo: &Repo) -> (String, String) {
    let aud = f.audience(ALICE, "3kaudience01", Some("list"), &[BOB]).await;
    let p1 = f.private_post(ALICE, "3kpostone001", &aud, None).await;
    f.ready_all().await;
    repo.put(NSID_AUDIENCE, "3kaudience01", "bafy3kaudience01", json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [BOB], "createdAt": "2026-09-26T00:00:00Z"}));
    repo.put(NSID_POST_PRIVATE, "3kpostone001", "bafy3kpostone001", f.post_record(ALICE, "3kpostone001", &aud, None).await);
    exec(&f.pool, &format!("UPDATE sync_state SET applied_rev = '{REV}' WHERE did = '{ALICE}'")).await;
    set_head(&f.pool, ALICE, R1).await;
    (aud, p1)
}

/// T-18 and P-11's contract (§4.9): after `requestRecord` answers `indexed`
/// or `deleted` with `caughtUp: true`, the author's and a reader's
/// `getPosts` reflect the record at once and the author's other posts stay
/// readable. A second call within the interval does not list.
#[tokio::test]
async fn request_record_catches_up_and_the_record_reads_at_once() {
    let Some(db) = fresh_db("s7_catch_up").await else {
        return;
    };
    let f = fixture(&db.pool, "s7-catch-up").await;
    let repo = Repo::new(&f.pool, 100);
    let (aud, p1) = alice_behind(&f, &repo).await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    // Alice's head moved past her applied rev: her posts miss.
    assert!(readable(&base, BOB, &[&p1]).await.is_empty());

    // A new post, indexed and caught up: both read it, and the old one.
    let p2 = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kposttwo001");
    repo.put(NSID_POST_PRIVATE, "3kposttwo001", "bafy3kposttwo001", f.post_record(ALICE, "3kposttwo001", &aud, None).await);
    let index = index_server(&f, &repo).await;
    assert_eq!(request_record(&index, &p2).await, ("indexed".to_string(), true));
    assert_eq!(applied_rev_of(&f.pool, ALICE).await.as_deref(), Some(R1));
    assert_eq!(repo.lists(), 4, "the four authorization collections, one page each");
    for reader in [BOB, ALICE] {
        assert_eq!(readable(&base, reader, &[&p1, &p2]).await, [p1.clone(), p2.clone()], "{reader}");
    }

    // A second call within the interval does not list and answers as it
    // stands.
    repo.set_rev(R2);
    set_head(&f.pool, ALICE, R2).await;
    let (_, caught_up) = request_record(&index, &p1).await;
    assert!(!caught_up);
    assert_eq!(repo.lists(), 4, "no listing");
    assert_eq!(applied_rev_of(&f.pool, ALICE).await.as_deref(), Some(R1));

    // A delete, once the interval has passed (a new process's interval).
    repo.remove(NSID_POST_PRIVATE, "3kposttwo001");
    repo.set_rev(R3);
    set_head(&f.pool, ALICE, R3).await;
    let index = index_server(&f, &repo).await;
    assert_eq!(request_record(&index, &p2).await, ("deleted".to_string(), true));
    assert_eq!(applied_rev_of(&f.pool, ALICE).await.as_deref(), Some(R3));
    for reader in [BOB, ALICE] {
        assert_eq!(readable(&base, reader, &[&p1, &p2]).await, std::slice::from_ref(&p1), "{reader}");
    }
    db.drop_db().await;
}

/// T-18, review 1 LB2: a listed audience that fails to write clears
/// readiness and queues `authz`, as on the firehose; nothing settles, and
/// Bob's read is a uniform miss, never a read under the stale audience row.
#[tokio::test]
async fn a_failed_catch_up_write_clears_readiness_and_settles_nothing() {
    let Some(db) = fresh_db("s7_catch_up_failed").await else {
        return;
    };
    let f = fixture(&db.pool, "s7-catch-up-failed").await;
    let repo = Repo::new(&f.pool, 100);
    let (_, p1) = alice_behind(&f, &repo).await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let (missing, _) = posts_of(&base, BOB, &[&format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere001")]).await;
    // Alice removes Bob, and the edit carries a malformed field.
    repo.put(NSID_AUDIENCE, "3kaudience01", "bafyaudnew", json!({"$type": NSID_AUDIENCE, "mode": "list", "members": "not a list", "createdAt": "2026-09-26T00:00:00Z"}));
    let index = index_server(&f, &repo).await;
    let (status, caught_up) = request_record(&index, &p1).await;
    assert_eq!((status.as_str(), caught_up), ("indexed", false));
    assert_eq!(applied_rev_of(&f.pool, ALICE).await.as_deref(), Some(REV), "no advance");
    let member_ready: bool = sqlx::query_scalar("SELECT ready_at IS NOT NULL FROM members WHERE did = $1")
        .bind(ALICE)
        .fetch_one(&f.pool)
        .await
        .expect("member");
    assert!(!member_ready, "ready_at cleared");
    assert!(!readiness::owner_ready(&f.pool, ALICE).await.expect("owner"), "owner readiness cleared");
    let authz: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs WHERE did = $1 AND depth = 'authz' AND state = 'queued'")
        .bind(ALICE)
        .fetch_one(&f.pool)
        .await
        .expect("jobs");
    assert_eq!(authz, 1, "authz queued");
    // Even with the head where the catch-up left it, Bob's read is a miss
    // byte-identical to a nonexistent post.
    set_head(&f.pool, ALICE, REV).await;
    let (body, posts) = posts_of(&base, BOB, &[&p1]).await;
    assert!(posts.is_empty(), "{body}");
    assert_eq!(body, missing, "a uniform miss");
    db.drop_db().await;
}

/// T-18's bounds: a DID that is not ready is not listed; over the page cap,
/// or with a gap during the catch-up, nothing settles.
#[tokio::test]
async fn catch_up_is_bounded_and_settles_only_a_clean_listing() {
    let Some(db) = fresh_db("s7_catch_up_bounds").await else {
        return;
    };
    let f = fixture(&db.pool, "s7-catch-up-bounds").await;
    let repo = Repo::new(&f.pool, 1);
    let (_, p1) = alice_behind(&f, &repo).await;

    // Not ready: no catch-up at all.
    exec(&f.pool, &format!("UPDATE members SET ready_at = NULL WHERE did = '{ALICE}'")).await;
    let index = index_server(&f, &repo).await;
    assert!(!request_record(&index, &p1).await.1);
    assert_eq!(repo.lists(), 0, "not listed");
    exec(&f.pool, &format!("UPDATE members SET ready_at = now() WHERE did = '{ALICE}'")).await;

    // A gap between the epoch read and the settle: no advance.
    repo.set_rev(R2);
    set_head(&f.pool, ALICE, R2).await;
    repo.bump_epoch.store(true, std::sync::atomic::Ordering::SeqCst);
    let index = index_server(&f, &repo).await;
    assert!(!request_record(&index, &p1).await.1);
    assert_eq!(repo.lists(), 4, "listed in pages of one");
    assert_eq!(applied_rev_of(&f.pool, ALICE).await.as_deref(), Some(REV));

    // Over the page cap (six private blocks in pages of one, nine pages in
    // all): no advance.
    for i in 0..6 {
        repo.put(
            crate::firehose::classifier::NSID_PRIVATE_BLOCK,
            &format!("3kblock00000{i}"),
            &format!("bafyblock{i}"),
            json!({"$type": crate::firehose::classifier::NSID_PRIVATE_BLOCK, "subject": CAROL, "createdAt": "2026-09-26T00:00:00Z"}),
        );
    }
    repo.set_rev(R3);
    set_head(&f.pool, ALICE, R3).await;
    let before = repo.lists();
    let index = index_server(&f, &repo).await;
    assert!(!request_record(&index, &p1).await.1);
    assert_eq!(repo.lists() - before, 8, "stopped at the cap");
    assert_eq!(applied_rev_of(&f.pool, ALICE).await.as_deref(), Some(REV));

    // Within the cap, the same repository catches up.
    let listed_before = repo.lists();
    repo.records.lock().expect("records").retain(|r| !r.uri.contains("3kblock00000"));
    let index = index_server(&f, &repo).await;
    assert!(request_record(&index, &p1).await.1);
    assert!(repo.lists() > listed_before);
    assert_eq!(applied_rev_of(&f.pool, ALICE).await.as_deref(), Some(R3));
    db.drop_db().await;
}

// Sealed feed cursors, the author feed filter and paged replies (private
// posting §3.6, §4.12-§4.14; S8: P-17, P-20, P-6/P-14).

const GET_REPLIES: &str = "app.nearhorizon.private.getReplies";

impl Fixture {
    /// A post by `author` under `audience` that no one can read, its author
    /// included: content encoded for another rkey. It passes the prefilter
    /// and misses in the pipeline.
    async fn undecodable(&self, author: &str, rkey: &str, audience: &str, reply: Option<(&str, &str)>) -> String {
        let record = self.post_record(author, "3kencodedfor", audience, reply).await;
        self.ingest(author, NSID_POST_PRIVATE, rkey, record).await;
        format!("at://{author}/{NSID_POST_PRIVATE}/{rkey}")
    }
}

/// Every page of `method?query` for `reader`, following the cursor, at
/// most `max` pages.
async fn pages(base: &str, method: &str, query: &str, reader: &str, max: usize) -> Vec<Value> {
    let mut out = Vec::new();
    let mut next = query.to_string();
    for _ in 0..max {
        let (_, page, _) = json_of(base, method, &next, reader).await;
        let cursor = page["cursor"].as_str().map(|c| format!("{query}&cursor={c}"));
        out.push(page);
        match cursor {
            Some(c) => next = c,
            None => break,
        }
    }
    out
}

/// The URIs of a feed page's items.
fn feed_uris(page: &Value) -> Vec<String> {
    page["feed"].as_array().expect("feed").iter().map(|i| i["uri"].as_str().expect("uri").to_string()).collect()
}

/// The URIs of a `getReplies` page's top-level nodes.
fn replies_uris(page: &Value) -> Vec<String> {
    page["replies"].as_array().expect("replies").iter().map(|n| n["post"]["uri"].as_str().expect("uri").to_string()).collect()
}

/// T-19 (P-17), in On and in Off (T-1).
/// - An author with `limit × 2 + 1` posts that do not decode before three
///   readable ones: on the author feed and on the timeline, the first page
///   is empty and carries a cursor, and paging reaches every readable post
///   once, in order. Exhausted candidates: no cursor. Every cursor is 720
///   characters (a `did:web` author's: `cursor.rs`'s test).
/// - A cursor opened by another caller, for another actor, method or
///   filter, under a rotated key, or an unsealed one from before: 400. An
///   absent filter and the explicit default bind alike.
/// - An author whose only posts are taken down: empty, no cursor, the
///   bytes of an unknown actor.
async fn sealed_paging_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    // Every post has one `createdAt`, so the record key orders them: the
    // undecodable ones are the newest.
    let mut readable = Vec::new();
    for i in 0..3 {
        readable.push(f.private_post(ALICE, &format!("3kread0000{i}2"), &aud, None).await);
    }
    readable.reverse();
    for i in 0..3 {
        f.undecodable(ALICE, &format!("3kundec000{i}2"), &aud, None).await;
    }
    exec(&f.pool, &follow_sql(BOB, ALICE)).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[BOB]).await;
    let mut taken_down = Vec::new();
    for rkey in ["3kdown000012", "3kdown000022"] {
        taken_down.push(f.private_post(DAVE, rkey, &dave_aud, None).await);
    }
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    for (method, query) in [(FEED, format!("actor={ALICE}&limit=1")), (TIMELINE, "limit=1".to_string())] {
        let all = pages(&base, method, &query, BOB, 20).await;
        assert!(feed_uris(&all[0]).is_empty(), "{method}: {}", all[0]);
        assert!(all[0]["cursor"].is_string(), "{method}: an empty page carries a cursor while candidates remain");
        let seen: Vec<String> = all.iter().flat_map(feed_uris).collect();
        assert_eq!(seen, readable, "{method}: paging reaches the readable posts");
        let (last, before) = all.split_last().expect("pages");
        assert!(last.get("cursor").is_none(), "{method}: exhausted, no cursor: {last}");
        for page in before {
            assert_eq!(page["cursor"].as_str().map(str::len), Some(720), "{method}: sealed and padded");
        }
    }

    // Rejected cursors.
    let (_, first, _) = json_of(&base, FEED, &format!("actor={ALICE}&limit=1"), BOB).await;
    let cursor = first["cursor"].as_str().expect("cursor").to_string();
    let rotated = serve(f.state_keyed(&[8; 32])).await;
    let unsealed = urlencoding("2026-09-26T12:00:00.000000Z::3kundec00082");
    for (case, at, method, query, reader) in [
        ("another caller", &base, FEED, format!("actor={ALICE}&limit=1&cursor={cursor}"), CAROL),
        ("another actor", &base, FEED, format!("actor={DAVE}&limit=1&cursor={cursor}"), BOB),
        ("another method", &base, TIMELINE, format!("limit=1&cursor={cursor}"), BOB),
        ("another filter", &base, FEED, format!("actor={ALICE}&limit=1&filter=posts_and_author_threads&cursor={cursor}"), BOB),
        ("a rotated key", &rotated, FEED, format!("actor={ALICE}&limit=1&cursor={cursor}"), BOB),
        ("unsealed, from before", &base, FEED, format!("actor={ALICE}&limit=1&cursor={unsealed}"), BOB),
    ] {
        let (status, body, _) = get(at, method, &query, reader, key_of(reader)).await;
        assert_eq!(status, 400, "{case}: {body}");
        assert!(body.contains("Error: cursor is not valid"), "{case}: {body}");
    }
    let (_, page, _) = json_of(&base, FEED, &format!("actor={ALICE}&limit=1&filter=posts_with_replies&cursor={cursor}"), BOB).await;
    assert!(page["feed"].is_array(), "the explicit default opens the absent filter's cursor");

    // Only taken-down posts: what an unknown actor gives, floored. Before
    // the takedown Bob reads them.
    let (_, before, _) = json_of(&base, FEED, &format!("actor={DAVE}"), BOB).await;
    assert_eq!(feed_uris(&before).len(), 2);
    for uri in &taken_down {
        exec(&f.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{uri}', '!takedown')")).await;
    }
    let (down, _, took) = json_of(&base, FEED, &format!("actor={DAVE}&limit=1"), BOB).await;
    let (unknown, _, _) = json_of(&base, FEED, &format!("actor={NOBODY}&limit=1"), BOB).await;
    assert_eq!((down.as_str(), unknown.as_str()), (r#"{"feed":[]}"#, r#"{"feed":[]}"#));
    assert!(took >= FLOOR, "{took:?}");
    db.drop_db().await;
}

#[tokio::test]
async fn sealed_feed_cursors_on() {
    sealed_paging_in(PublicMode::On, "s8-paging-on").await;
}

#[tokio::test]
async fn sealed_feed_cursors_off() {
    sealed_paging_in(PublicMode::Off, "s8-paging-off").await;
}

/// T-20 (P-20), in On and in Off (T-1): `posts_and_author_threads` gives
/// Bob's top-level posts and his replies in his own threads, never his
/// replies in Alice's thread, in full pages though nine such replies are
/// newer; his repost item shows under both filters. `posts_with_replies`,
/// and the absent filter, give everything. A cursor fails across filters;
/// an unknown filter is a 400.
async fn author_filter_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let alice_aud = f.audience(ALICE, "3kaudalice22", Some("list"), &[BOB, CAROL]).await;
    let bob_aud = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &alice_aud, None).await;
    let p1 = f.private_post(BOB, "3kbtop000012", &bob_aud, None).await;
    let p2 = f.private_post(BOB, "3kbtop000022", &bob_aud, None).await;
    let q1 = f.private_post(BOB, "3kbown000012", &bob_aud, Some((&p1, &p1))).await;
    let q2 = f.private_post(BOB, "3kbown000022", &bob_aud, Some((&p1, &q1))).await;
    // Bob's replies in Alice's thread name her audience (S12, private-threads
    // design §2.2); the one under his own audience is his alone, so absent.
    let mut others = Vec::new();
    for i in 0..9 {
        others.push(f.private_post(BOB, &format!("3kzother000{i}"), &alice_aud, Some((&t, &t))).await);
    }
    others.reverse();
    f.private_post(BOB, "3kzzownaud22", &bob_aud, Some((&t, &t))).await;
    f.ingest(BOB, REPOST, "3krepostt222", subject_record(REPOST, &t, "2026-09-25T00:00:00Z")).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    let threads = pages(&base, FEED, &format!("actor={BOB}&limit=2&filter=posts_and_author_threads"), CAROL, 10).await;
    let sizes: Vec<usize> = threads.iter().map(|p| feed_uris(p).len()).collect();
    assert_eq!(sizes, [2, 2, 1], "full pages: {threads:?}");
    let seen: Vec<String> = threads.iter().flat_map(feed_uris).collect();
    assert_eq!(seen, [p2.clone(), p1.clone(), q2.clone(), q1.clone(), t.clone()]);
    assert_eq!(threads[2]["feed"][0]["reason"]["$type"], "app.bsky.feed.defs#reasonRepost");

    let mut everything = others.clone();
    everything.extend([p2, p1, q2, q1, t]);
    let (explicit, v, _) = json_of(&base, FEED, &format!("actor={BOB}&filter=posts_with_replies"), CAROL).await;
    assert_eq!(feed_uris(&v), everything);
    let (absent, _, _) = json_of(&base, FEED, &format!("actor={BOB}"), CAROL).await;
    assert_eq!(absent, explicit);

    let cursor = threads[0]["cursor"].as_str().expect("cursor");
    for query in [format!("actor={BOB}&limit=2&filter=posts_with_replies&cursor={cursor}"), format!("actor={BOB}&limit=2&cursor={cursor}")] {
        let (status, body, _) = get(&base, FEED, &query, CAROL, CAROL_KEY).await;
        assert_eq!(status, 400, "{query}: {body}");
        assert!(body.contains("Error: cursor is not valid"), "{body}");
    }
    for filter in ["posts_no_replies", ""] {
        let (status, body, _) = get(&base, FEED, &format!("actor={BOB}&filter={filter}"), CAROL, CAROL_KEY).await;
        assert_eq!(status, 400, "{filter}: {body}");
        assert!(
            body.contains("Error: filter must be posts_with_replies, posts_and_author_threads, posts_with_media or posts_with_video"),
            "{body}"
        );
    }
    db.drop_db().await;
}

#[tokio::test]
async fn author_feed_filter_on() {
    author_filter_in(PublicMode::On, "s8-filter-on").await;
}

#[tokio::test]
async fn author_feed_filter_off() {
    author_filter_in(PublicMode::Off, "s8-filter-off").await;
}

/// T-21 (P-6/P-14, D-8) and T-10's `getReplies` case, in On and in Off
/// (T-1).
/// - A public post, never stored here: its readable private replies,
///   oldest first, paged; `limit × 2 + 1` undecodable ones first give an
///   empty first page with a cursor; one Carol cannot read never shows.
///   The cursor is bound to its `uri`.
/// - A private post Carol cannot read: `{replies: []}`, floored, the bytes
///   of a nonexistent post, though her own reply to it exists.
/// - `depth` 0, 1 and the default 2; bounds.
/// - The read root's `hiddenReplies` and `replyDisabled` apply to a reply's
///   replies, except for the hidden reply's author.
async fn replies_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let alice_aud = f.audience(ALICE, "3kaudalice22", Some("list"), &[BOB, CAROL]).await;
    let bob_aud = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;
    let carol_aud = f.audience(CAROL, "3kaudcarol22", Some("list"), &[BOB]).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[]).await;
    let public = format!("at://{ALICE}/{PUBLIC_POST}/3kpublic2222");
    for i in 0..3 {
        f.undecodable(BOB, &format!("3kaundec000{i}"), &bob_aud, Some((&public, &public))).await;
    }
    let mut readable = Vec::new();
    for i in 0..3 {
        readable.push(f.private_post(BOB, &format!("3kreply0000{i}"), &bob_aud, Some((&public, &public))).await);
    }
    f.private_post(DAVE, "3kdavereply2", &dave_aud, Some((&public, &public))).await;
    let dave_post = f.private_post(DAVE, "3kdavepost22", &dave_aud, None).await;
    let carol_reply = f.private_post(CAROL, "3kcarolreply", &carol_aud, Some((&dave_post, &dave_post))).await;
    let p = f.private_post(ALICE, "3kprootpost2", &alice_aud, None).await;
    let r1 = f.private_post(BOB, "3kpr1reply22", &alice_aud, Some((&p, &p))).await;
    let r2 = f.private_post(CAROL, "3kpr2reply22", &alice_aud, Some((&p, &r1))).await;
    let r3 = f.private_post(BOB, "3kpr3reply22", &alice_aud, Some((&p, &r2))).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    // The public post's private replies, paged past the undecodable ones.
    let all = pages(&base, GET_REPLIES, &format!("uri={public}&limit=1"), CAROL, 20).await;
    assert!(replies_uris(&all[0]).is_empty() && all[0]["cursor"].is_string(), "{}", all[0]);
    let seen: Vec<String> = all.iter().flat_map(replies_uris).collect();
    assert_eq!(seen, readable);
    assert!(all.last().expect("pages").get("cursor").is_none());
    let node = &all.iter().find(|p| !replies_uris(p).is_empty()).expect("a hit")["replies"][0];
    assert_eq!(node["$type"], "app.nearhorizon.private.defs#privateThreadView");
    assert!(node.get("parent").is_none(), "nothing about the public post: {node}");
    let cursor = all[0]["cursor"].as_str().expect("cursor");
    let (status, body, _) = get(&base, GET_REPLIES, &format!("uri={p}&limit=1&cursor={cursor}"), CAROL, CAROL_KEY).await;
    assert_eq!((status, body.contains("Error: cursor is not valid")), (400, true), "{body}");

    // A private post Carol cannot read: nonexistent, though her reply reads.
    assert_eq!(posts_of(&base, CAROL, &[&carol_reply]).await.1.len(), 1);
    let (unreadable, _, took) = json_of(&base, GET_REPLIES, &format!("uri={dave_post}"), CAROL).await;
    assert!(took >= FLOOR, "{took:?}");
    let nowhere = format!("at://{DAVE}/{NSID_POST_PRIVATE}/3knothere222");
    let (nonexistent, _, _) = json_of(&base, GET_REPLIES, &format!("uri={nowhere}"), CAROL).await;
    assert_eq!((unreadable.as_str(), nonexistent.as_str()), (r#"{"replies":[]}"#, r#"{"replies":[]}"#));

    // Depth.
    let (_, v, _) = json_of(&base, GET_REPLIES, &format!("uri={p}&depth=0"), CAROL).await;
    assert_eq!(replies_uris(&v), std::slice::from_ref(&r1));
    assert!(v["replies"][0].get("replies").is_none(), "{v}");
    let (_, v, _) = json_of(&base, GET_REPLIES, &format!("uri={p}&depth=1"), CAROL).await;
    assert_eq!(v["replies"][0]["replies"][0]["post"]["uri"], r2.as_str());
    assert!(v["replies"][0]["replies"][0].get("replies").is_none(), "{v}");
    let (_, v, _) = json_of(&base, GET_REPLIES, &format!("uri={p}"), CAROL).await;
    assert_eq!(v["replies"][0]["replies"][0]["replies"][0]["post"]["uri"], r3.as_str());
    for query in [format!("uri={p}&depth=7"), format!("uri={p}&limit=0"), format!("uri={p}&limit=51"), String::new()] {
        let (status, body, _) = get(&base, GET_REPLIES, &query, CAROL, CAROL_KEY).await;
        assert_eq!(status, 400, "{query}: {body}");
    }

    // The root's gate: `allow: []`, hiding r2. Read with r1, its claimed
    // root P is read too.
    f.ingest(ALICE, THREADGATE, "3kprootpost2", json!({"$type": THREADGATE, "post": p, "allow": [], "hiddenReplies": [r2]})).await;
    let (body, _, _) = json_of(&base, GET_REPLIES, &format!("uri={r1}"), BOB).await;
    assert_eq!(body, r#"{"replies":[]}"#, "hidden from Bob");
    let (_, v, _) = json_of(&base, GET_REPLIES, &format!("uri={r1}"), CAROL).await;
    assert_eq!(replies_uris(&v), std::slice::from_ref(&r2), "not from its author");
    assert_eq!(v["replies"][0]["post"]["viewer"]["replyDisabled"], true);
    let (_, v, _) = json_of(&base, GET_REPLIES, &format!("uri={p}"), BOB).await;
    assert_eq!(replies_uris(&v), std::slice::from_ref(&r1));
    assert_eq!(v["replies"][0]["replies"], json!([]), "expanded, r2 hidden: {v}");
    assert_eq!(v["replies"][0]["post"]["viewer"]["replyDisabled"], true);
    db.drop_db().await;
}

#[tokio::test]
async fn private_replies_on() {
    replies_in(PublicMode::On, "s8-replies-on").await;
}

#[tokio::test]
async fn private_replies_off() {
    replies_in(PublicMode::Off, "s8-replies-off").await;
}

/// T-21's caps, in On and in Off (T-1): four replies with 51 children
/// each, at `depth` 1, give at most 50 children a node and 200 nodes.
async fn replies_cap_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let root = f.private_post(ALICE, "3kcaproot222", &aud, None).await;
    for i in 0..4 {
        let hit = f.private_post(ALICE, &format!("3kcaphit{i}222"), &aud, Some((&root, &root))).await;
        for j in 0..51 {
            f.private_post(ALICE, &format!("3kcap{i}kid{j:02}2"), &aud, Some((&root, &hit))).await;
        }
    }
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let (_, v, _) = json_of(&base, GET_REPLIES, &format!("uri={root}&depth=1"), BOB).await;
    let children: Vec<usize> = v["replies"].as_array().expect("replies").iter().map(|n| n["replies"].as_array().map_or(0, Vec::len)).collect();
    assert_eq!(children, [50, 50, 50, 46], "50 a node, 200 nodes in all");
    db.drop_db().await;
}

#[tokio::test]
async fn private_replies_caps_on() {
    replies_cap_in(PublicMode::On, "s8-replies-cap-on").await;
}

#[tokio::test]
async fn private_replies_caps_off() {
    replies_cap_in(PublicMode::Off, "s8-replies-cap-off").await;
}

// Private search: a bounded scan at query time (private posting §4.17; S11:
// P-7, C-8).

const SEARCH: &str = "app.nearhorizon.private.searchPosts";
/// A floor short enough to walk a year of weekly spans quickly; the floor
/// itself is checked with [`FLOOR`].
const SEARCH_FLOOR: Duration = Duration::from_millis(30);

impl Fixture {
    /// A private post by `author` under `audience` saying `text`, created at
    /// `at`: encoded with the real hooks when `encoded`, else plaintext,
    /// which the lexicon's XOR rule permits.
    async fn post_saying(&self, author: &str, rkey: &str, audience: &str, text: &str, at: DateTime<Utc>, encoded: bool) -> String {
        let mut record = if encoded {
            self.encoded_post(author, rkey, text, Some(audience)).await
        } else {
            json!({"$type": NSID_POST_PRIVATE, "text": text, "audienceList": audience})
        };
        record["createdAt"] = json!(at.to_rfc3339_opts(SecondsFormat::Millis, true));
        self.ingest(author, NSID_POST_PRIVATE, rkey, record).await;
        format!("at://{author}/{NSID_POST_PRIVATE}/{rkey}")
    }

    /// A search server: roomy rate limits (a page costs 5 tokens), fresh
    /// heads and watermark, `floor`, and `bounds`. Plaintext posts are
    /// served (S4 off): the paging tests seed hundreds of posts with
    /// `post_saying(.., false)` to stay fast, and search decides nothing
    /// about S4 (T-S1 does).
    async fn search_server(&self, floor: Duration, bounds: &PrivateSearchConfig) -> String {
        exec(&self.pool, "UPDATE sync_state SET head_checked_at = now()").await;
        let mut app = self.app.clone();
        app.limits = roomy_limits();
        serve(self.state_of(app, Some(Duration::from_secs(1)), floor).with_search(bounds).with_plaintext_refused(false)).await
    }
}

/// The URIs of a search page's posts.
fn post_uris(page: &Value) -> Vec<String> {
    page["posts"].as_array().expect("posts").iter().map(|p| p["uri"].as_str().expect("uri").to_string()).collect()
}

/// Where a search page's cursor leads (§4.17).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Next {
    /// No cursor: the span reached the search's bound.
    End,
    /// Below a row: the last returned one, or the last of a full scan.
    Row(String),
    /// The span's lower edge.
    Edge,
}

/// Each page of a search for `q` by `reader` as `(posts, next)`. On the way
/// it checks that every cursor carries the first page's bound, that each
/// edge lies one span below where its page began (the first page at the
/// request time, `bound + max_age`), and that the last page's span reached
/// the bound. So two traces that are equal had equal cursors.
fn search_trace(pages: &[Value], q: &str, reader: &str, bounds: &PrivateSearchConfig) -> Vec<(Vec<String>, Next)> {
    let key = CursorKey::new(&TEST_CURSOR_KEY);
    let binding = super::cursor::aad(SEARCH, reader, q, "");
    let span = chrono::Duration::hours(i64::from(bounds.span_hours));
    let max_age = chrono::Duration::days(i64::from(bounds.max_age_days));
    let mut bound: Option<DateTime<Utc>> = None;
    let mut start: Option<DateTime<Utc>> = None;
    let mut trace = Vec::new();
    for page in pages {
        let next = match page["cursor"].as_str() {
            None => {
                if let (Some(s), Some(b)) = (start, bound) {
                    assert!(s - span <= b, "the last page's span reached the bound");
                }
                Next::End
            }
            Some(c) => {
                let p = super::cursor::open_search(&key, &binding, Some(c)).expect("opens").expect("a position");
                let b = *bound.get_or_insert(p.bound);
                assert_eq!(p.bound, b, "every cursor carries the first page's bound");
                let began = start.unwrap_or(b + max_age);
                start = Some(p.at);
                if p.uri.is_empty() {
                    assert_eq!(p.at, began - span, "an edge lies one span below its page's start");
                    Next::Edge
                } else {
                    Next::Row(p.uri)
                }
            }
        };
        trace.push((post_uris(page), next));
    }
    trace
}

/// T-22 (P-7), in On and in Off (T-1), at the default bounds:
/// - a match on page 1, case-folded, from encoded content; a match ten days
///   back, beyond the first week's span, reached by cursor; Dave's post,
///   which Bob cannot read, never matches for him;
/// - with nothing else to find, the search pages through every weekly span
///   down to 365 days: 53 pages, each but the last with a sealed cursor at
///   its span's edge;
/// - `top` answers as `latest`; `from:<did>`; the views are decorated;
/// - a cursor is refused for another caller, another `q` and another
///   method's cursor; bad parameters are 400; a page is floored.
async fn search_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[]).await;
    let now = Utc::now();
    let hours = |h: i64| now - chrono::Duration::hours(h);
    let cats = f.post_saying(ALICE, "3kcats000012", &aud, "Cats are GREAT", hours(1), true).await;
    let dogs = f.post_saying(ALICE, "3kdogs000012", &aud, "dogs only", hours(2), true).await;
    let secret = f.post_saying(DAVE, "3kdavecats12", &dave_aud, "cats, secretly", hours(3), true).await;
    let later = f.post_saying(ALICE, "3kcats000022", &aud, "more cats in week two", hours(240), true).await;
    f.ready_all().await;
    let bounds = PrivateSearchConfig::default();
    let base = f.search_server(SEARCH_FLOOR, &bounds).await;

    let all = pages(&base, SEARCH, "q=CATS", BOB, 80).await;
    let trace = search_trace(&all, "CATS", BOB, &bounds);
    assert_eq!(trace.len(), 53, "53 weekly spans reach back 365 days");
    assert_eq!(trace[0].0, std::slice::from_ref(&cats), "a match on page 1");
    assert_eq!(trace[1].0, std::slice::from_ref(&later), "a match beyond the first span");
    assert!(trace[2..].iter().all(|(posts, _)| posts.is_empty()));
    for (k, (_, next)) in trace.iter().enumerate() {
        assert_eq!(*next, if k < 52 { Next::Edge } else { Next::End }, "page {k}");
    }
    for page in &all[..52] {
        assert_eq!(page["cursor"].as_str().map(str::len), Some(720), "sealed and padded");
    }
    let view = &all[0]["posts"][0];
    assert_eq!(view["record"]["text"], "Cats are GREAT");
    assert_eq!(counts_of(view), [0, 0, 0, 0], "decorated as every view: {view}");
    assert_eq!(view["viewer"]["bookmarked"], false);

    // Dave reads his own post; Bob never matches it.
    let (_, v, _) = json_of(&base, SEARCH, "q=secretly", DAVE).await;
    assert_eq!(post_uris(&v), std::slice::from_ref(&secret));
    let (_, v, _) = json_of(&base, SEARCH, "q=secretly", BOB).await;
    assert!(post_uris(&v).is_empty(), "{v}");

    // `top` in `latest` order; every word; `from:`.
    let (_, latest, _) = json_of(&base, SEARCH, "q=cats", CAROL).await;
    let (_, top, _) = json_of(&base, SEARCH, "q=cats&sort=top", CAROL).await;
    assert_eq!(post_uris(&top), post_uris(&latest));
    assert_eq!(post_uris(&latest), std::slice::from_ref(&cats));
    let q = urlencoding("great CATS");
    assert_eq!(post_uris(&json_of(&base, SEARCH, &format!("q={q}"), BOB).await.1), std::slice::from_ref(&cats));
    let q = urlencoding("cats dogs");
    assert!(post_uris(&json_of(&base, SEARCH, &format!("q={q}"), BOB).await.1).is_empty());
    let q = urlencoding(&format!("cats from:{ALICE}"));
    assert_eq!(post_uris(&json_of(&base, SEARCH, &format!("q={q}"), BOB).await.1), std::slice::from_ref(&cats));
    let q = urlencoding(&format!("cats from:{DAVE}"));
    assert!(post_uris(&json_of(&base, SEARCH, &format!("q={q}"), BOB).await.1).is_empty());
    let q = urlencoding(&format!("from:{ALICE}"));
    assert_eq!(post_uris(&json_of(&base, SEARCH, &format!("q={q}"), BOB).await.1), [cats.clone(), dogs]);

    // Cursors open only where they were issued.
    let cursor = all[0]["cursor"].as_str().expect("cursor");
    let (_, feed, _) = json_of(&base, FEED, &format!("actor={ALICE}&limit=1"), BOB).await;
    let feed_cursor = feed["cursor"].as_str().expect("a feed cursor");
    for (case, query, reader) in [
        ("another caller", format!("q=CATS&cursor={cursor}"), CAROL),
        ("another q", format!("q=cats&cursor={cursor}"), BOB),
        ("another method's cursor", format!("q=CATS&cursor={feed_cursor}"), BOB),
    ] {
        let (status, body, _) = get(&base, SEARCH, &query, reader, key_of(reader)).await;
        assert_eq!((status, body.contains("Error: cursor is not valid")), (400, true), "{case}: {body}");
    }

    // Bad parameters.
    for query in ["", "q=", "q=%20%20", "q=from:alice.test", "q=cats&limit=0", "q=cats&limit=101", "q=cats&sort=hot"] {
        let (status, body, _) = get(&base, SEARCH, query, BOB, BOB_KEY).await;
        assert_eq!(status, 400, "{query}: {body}");
    }

    // Floored, hit or miss.
    let floored = f.search_server(FLOOR, &bounds).await;
    for query in ["q=cats", "q=nothinghere"] {
        let (_, _, took) = json_of(&floored, SEARCH, query, BOB).await;
        assert!(took >= FLOOR, "{query}: {took:?}");
    }
    db.drop_db().await;
}

#[tokio::test]
async fn private_search_on() {
    search_in(PublicMode::On, "s11-search-on").await;
}

#[tokio::test]
async fn private_search_off() {
    search_in(PublicMode::Off, "s11-search-off").await;
}

/// Bob's searches for `token` with `limit` 2 and 5, at `bounds`.
async fn token_traces(f: &Fixture, bounds: &PrivateSearchConfig) -> Vec<Vec<(Vec<String>, Next)>> {
    let base = f.search_server(SEARCH_FLOOR, bounds).await;
    let mut traces = Vec::new();
    for limit in [2, 5] {
        let all = pages(&base, SEARCH, &format!("q=token&limit={limit}"), BOB, 20).await;
        traces.push(search_trace(&all, "token", BOB, bounds));
    }
    traces
}

/// T-22's invariance (review 7, LB2; review 8), in On and in Off (T-1), at
/// bounds small enough to reach every cursor rule in a few pages: 3 rows a
/// page, a 24-hour span, 3 days back.
/// - With `limit` 2: a cursor at the last returned row, a span's edge, and
///   the end once the span reaches the bound. With `limit` 5: a cursor at
///   the last row of a full scan. No match is skipped or repeated.
/// - Posts Bob cannot read, added between his token posts in any number
///   (under an audience without him, by an author who blocks him), the
///   deletion of the deployment's oldest private post, and a readable
///   post backdated to year 1 change no page's contents and no cursor.
async fn search_invariance_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[]).await;
    let carol_aud = f.audience(CAROL, "3kaudcarol22", Some("list"), &[ALICE]).await;
    let now = Utc::now();
    let minutes = |m: i64| now - chrono::Duration::minutes(m);
    let mut token = Vec::new();
    for h in [1, 2, 3, 5, 30, 50] {
        token.push(f.post_saying(ALICE, &format!("3ktok{h:07}"), &aud, "a TOKEN here", minutes(h * 60), false).await);
    }
    for h in [6, 40] {
        f.post_saying(ALICE, &format!("3kplain{h:05}"), &aud, "nothing to see", minutes(h * 60), false).await;
    }
    f.ready_all().await;
    let bounds = PrivateSearchConfig {
        scan_candidates: 3,
        span_hours: 24,
        max_age_days: 3,
    };
    let row = |i: usize| Next::Row(token[i].clone());
    let expected = vec![
        vec![
            (vec![token[0].clone(), token[1].clone()], row(1)),
            (vec![token[2].clone(), token[3].clone()], row(3)),
            (vec![], Next::Edge),
            (vec![token[4].clone(), token[5].clone()], row(5)),
            (vec![], Next::End),
        ],
        vec![
            (token[..3].to_vec(), row(2)),
            (vec![token[3].clone()], Next::Edge),
            (vec![token[4].clone(), token[5].clone()], row(5)),
            (vec![], Next::End),
        ],
    ];
    let before = token_traces(&f, &bounds).await;
    assert_eq!(before, expected);

    // Posts Bob cannot read, between his: under Dave's audience, which
    // names nobody; under Carol's, without him; and by Dave under Alice's
    // audience, after Dave blocks Bob.
    for m in [90, 132, 150, 240, 270, 330, 1770, 1830, 1860, 2940, 4200, 12_000] {
        f.post_saying(DAVE, &format!("3kdave{m:06}"), &dave_aud, "token, unreadable", minutes(m), false).await;
    }
    for m in [100, 1810] {
        f.post_saying(CAROL, &format!("3kcarol{m:05}"), &carol_aud, "token for Alice", minutes(m), false).await;
    }
    exec(&f.pool, &block_sql(DAVE, BOB)).await;
    for m in [70, 175, 2950] {
        f.post_saying(DAVE, &format!("3kdblk{m:06}"), &aud, "token, blocked", minutes(m), false).await;
    }
    assert_eq!(token_traces(&f, &bounds).await, expected, "posts Bob cannot read change nothing");
    // Carol's are stored, and match for Carol.
    let carols = pages(&f.search_server(SEARCH_FLOOR, &bounds).await, SEARCH, "q=alice", CAROL, 20).await;
    assert_eq!(carols.iter().flat_map(post_uris).count(), 2, "{carols:?}");

    // The deployment's oldest private post, which Bob cannot read, and one
    // within the search's reach, deleted.
    for m in [12_000, 4200] {
        f.apply_op(DAVE, Action::Delete, NSID_POST_PRIVATE, &format!("3kdave{m:06}"), REV2, None).await;
    }
    assert_eq!(token_traces(&f, &bounds).await, expected, "a deletion Bob cannot see changes nothing");

    // A readable post backdated to year 1 is below every bound.
    let year_one = DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z").expect("ts").with_timezone(&Utc);
    let old = f.post_saying(ALICE, "3kancient222", &aud, "an ancient token", year_one, false).await;
    let stored: DateTime<Utc> = sqlx::query_scalar("SELECT created_at FROM private_posts WHERE uri = $1")
        .bind(&old)
        .fetch_one(&f.pool)
        .await
        .expect("stored");
    assert_eq!(stored, year_one);
    assert_eq!(token_traces(&f, &bounds).await, expected, "a backdated post adds no page");
    db.drop_db().await;
}

#[tokio::test]
async fn private_search_pages_ignore_unreadable_posts_on() {
    search_invariance_in(PublicMode::On, "s11-invariance-on").await;
}

#[tokio::test]
async fn private_search_pages_ignore_unreadable_posts_off() {
    search_invariance_in(PublicMode::Off, "s11-invariance-off").await;
}

/// T-22's paging at the default bounds (reviews 9, 10 and 11), in On and in
/// Off (T-1):
/// - 400 matching readable posts in the first week, `limit` 100: four pages
///   of 100, each with its cursor at its last returned row; then empty
///   pages one span on each, down to the bound, each with a cursor but the
///   last; every match once, newest first;
/// - `limit` 25: the second page starts after the 25th;
/// - a caller whose only match is 51 weeks back sees 51 empty pages with
///   cursors first, then the match.
async fn search_full_pages_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let far_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[CAROL]).await;
    let now = Utc::now();
    let mut needles = Vec::new();
    for i in 0..400 {
        let at = now - chrono::Duration::minutes(i + 1);
        needles.push(f.post_saying(ALICE, &format!("3kn{i:09}"), &aud, &format!("needle {i}"), at, false).await);
    }
    let far_at = now - chrono::Duration::hours(51 * 168 + 24);
    let far = f.post_saying(DAVE, "3kfaraway222", &far_aud, "faraway", far_at, false).await;
    f.ready_all().await;
    let bounds = PrivateSearchConfig::default();
    let base = f.search_server(SEARCH_FLOOR, &bounds).await;

    let all = pages(&base, SEARCH, "q=needle&limit=100", BOB, 80).await;
    let trace = search_trace(&all, "needle", BOB, &bounds);
    let sizes: Vec<usize> = trace.iter().map(|(p, _)| p.len()).collect();
    assert_eq!(sizes[..4], [100; 4], "{sizes:?}");
    assert!(sizes[4..].iter().all(|n| *n == 0), "{sizes:?}");
    for (posts, next) in &trace[..4] {
        assert_eq!(*next, Next::Row(posts.last().cloned().expect("full")));
    }
    let (last, middle) = trace[4..].split_last().expect("an end");
    assert!(!middle.is_empty() && middle.iter().all(|(_, n)| *n == Next::Edge), "{middle:?}");
    assert_eq!(last.1, Next::End);
    assert!(trace.len() <= 4 + 54, "{}", trace.len());
    let seen: Vec<String> = trace.iter().flat_map(|(p, _)| p.clone()).collect();
    assert_eq!(seen, needles, "every match once, newest first");

    let two = pages(&base, SEARCH, "q=needle&limit=25", BOB, 2).await;
    assert_eq!(post_uris(&two[0]), needles[..25]);
    assert_eq!(post_uris(&two[1]), needles[25..50], "the next page starts after the 25th");

    let all = pages(&base, SEARCH, "q=faraway", CAROL, 80).await;
    let trace = search_trace(&all, "faraway", CAROL, &bounds);
    assert_eq!(trace.len(), 53);
    assert!(trace[..51].iter().all(|(p, n)| p.is_empty() && *n == Next::Edge), "{trace:?}");
    assert_eq!(trace[51].0, std::slice::from_ref(&far));
    assert_eq!(trace[52], (vec![], Next::End));
    db.drop_db().await;
}

#[tokio::test]
async fn private_search_full_pages_on() {
    search_full_pages_in(PublicMode::On, "s11-full-on").await;
}

#[tokio::test]
async fn private_search_full_pages_off() {
    search_full_pages_in(PublicMode::Off, "s11-full-off").await;
}

/// B-5 for `searchPosts` (§4.17 "Cost and floor"): a seeded week of 3000
/// private posts, half readable by Bob and encoded with the real hooks,
/// half under an audience he is not in. With no floor, a page that matches
/// nothing (300 binds and decodes) and a page that fills `limit` 100 are
/// timed over 20 runs each. Prints the numbers.
#[tokio::test]
#[ignore = "B-5 measurement; run with --ignored --nocapture"]
async fn b5_search_floor_on_a_seeded_week() {
    let test = "b5-search";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[]).await;
    let now = Utc::now();
    let seeded = Instant::now();
    for i in 0..1500 {
        let at = now - chrono::Duration::seconds(i * 403 + 30);
        f.post_saying(ALICE, &format!("3kw{i:09}"), &aud, &format!("week post {i}"), at, true).await;
        let unread = at - chrono::Duration::seconds(100);
        f.post_saying(DAVE, &format!("3kx{i:09}"), &dave_aud, &format!("week post {i}"), unread, false).await;
    }
    f.ready_all().await;
    println!("B-5 searchPosts: seeded 3000 posts in one week in {:?}", seeded.elapsed());
    let bounds = PrivateSearchConfig::default();
    for (name, query) in [("no match, 300 binds and decodes", "q=zzzz&limit=100"), ("limit 100 filled", "q=week&limit=100")] {
        let base = f.search_server(Duration::ZERO, &bounds).await;
        let mut took = Vec::new();
        let mut items = Vec::new();
        for _ in 0..20 {
            exec(&f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
            let (_, v, t) = json_of(&base, SEARCH, query, BOB).await;
            items.push(post_uris(&v).len());
            took.push(t);
        }
        took.sort();
        println!(
            "B-5 searchPosts, {name}: p50 {:?}, p95 {:?}, max {:?} over {} runs; posts per page {:?}..{:?}",
            took[took.len() / 2],
            took[took.len() * 95 / 100],
            took[took.len() - 1],
            took.len(),
            items.iter().min(),
            items.iter().max()
        );
        // The same page at B-5's concurrency of 8 (#720, H-10): 200
        // requests on the same server, heads refreshed first.
        exec(&f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
        use futures::StreamExt;
        let base = &base;
        let mut c8: Vec<Duration> = futures::stream::iter(0..200)
            .map(|_| async move { json_of(base, SEARCH, query, BOB).await.2 })
            .buffer_unordered(8)
            .collect()
            .await;
        c8.sort();
        println!(
            "B-5 searchPosts, {name}, concurrency 8: p50 {:?}, p99 {:?}, max {:?} over {} requests",
            c8[c8.len() / 2],
            c8[c8.len() * 99 / 100],
            c8[c8.len() - 1],
            c8.len()
        );
    }
    db.drop_db().await;
}

/// T-TF2 (timing-floors design §6): one fixture served at floor 0 and at
/// [`FLOOR`] answers with the same content, only sooner. Bodies are
/// byte-identical, except `cursor`: a sealed cursor carries a fresh nonce
/// on every seal, so both cursors are opened with the test key and their
/// positions compared. At 0 each answer takes less than [`FLOOR`]; at
/// [`FLOOR`], at least that.
async fn floors_change_no_content_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let aud_carol = f.audience(ALICE, "3kaudcarol22", Some("list"), &[CAROL]).await;
    let aud_bob = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;
    let hit = f.private_post(ALICE, "3kread0000a2", &aud, None).await;
    f.private_post(ALICE, "3kread0000b2", &aud, None).await;
    let hidden = f.private_post(ALICE, "3khidden2222", &aud_carol, None).await;
    let missing = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knothere222");
    // Carol may read Bob's mention of her and Dave; Dave may not.
    f.post_with(BOB, "3kmention222", &aud_bob, json!({"facets": [mention(CAROL), mention(DAVE)]})).await;
    f.ready_all().await;
    let zero = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let floored = serve(f.state(Some(Duration::from_secs(1)))).await;

    let feed_aad = super::cursor::aad(FEED, BOB, ALICE, "posts_with_replies");
    let cursor_key = CursorKey::new(&TEST_CURSOR_KEY);
    let mut cursors = 0;
    for (case, method, query, reader) in [
        ("getPosts, a hit", POSTS, format!("uris={hit}"), BOB),
        ("getPosts, an unreadable post", POSTS, format!("uris={hidden}"), BOB),
        ("getPosts, a nonexistent URI", POSTS, format!("uris={missing}"), BOB),
        ("getPosts, all three", POSTS, format!("uris={hit}&uris={hidden}&uris={missing}"), BOB),
        ("getPostThread, an unreadable anchor", THREAD, format!("uri={hidden}"), BOB),
        ("getAuthorFeed, a page with a cursor", FEED, format!("actor={ALICE}&limit=1"), BOB),
        ("getUnreadCount, one readable mention", GET_UNREAD_COUNT, String::new(), CAROL),
        ("getUnreadCount, one unreadable mention", GET_UNREAD_COUNT, String::new(), DAVE),
    ] {
        exec(&f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
        let (status_0, body_0, mut took_0) = get(&zero, method, &query, reader, key_of(reader)).await;
        let (status_f, body_f, took_f) = get(&floored, method, &query, reader, key_of(reader)).await;
        assert_eq!(status_0, status_f, "{case}: {body_0} / {body_f}");
        assert!(took_f >= FLOOR, "{case}: at the floor took {took_f:?}");
        // At 0 nothing waits: the fastest of a few answers, each the same,
        // is under the floor (one answer alone can be slower on a loaded
        // host; a floor would hold every one).
        for _ in 0..4 {
            if took_0 < FLOOR {
                break;
            }
            let (status, body, took) = get(&zero, method, &query, reader, key_of(reader)).await;
            assert_eq!(status, status_0, "{case}: {body}");
            took_0 = took_0.min(took);
        }
        assert!(took_0 < FLOOR, "{case}: at 0 the fastest of five took {took_0:?}");
        let mut v_0: Value = serde_json::from_str(&body_0).expect("json");
        let mut v_f: Value = serde_json::from_str(&body_f).expect("json");
        match (v_0.as_object_mut().and_then(|o| o.remove("cursor")), v_f.as_object_mut().and_then(|o| o.remove("cursor"))) {
            (None, None) => assert_eq!(body_0, body_f, "{case}: byte-identical"),
            (Some(c_0), Some(c_f)) => {
                assert_eq!(v_0.to_string(), v_f.to_string(), "{case}: identical but for the cursor");
                let open = |c: &Value| {
                    super::cursor::open(&cursor_key, &feed_aad, c.as_str())
                        .expect("the cursor opens")
                        .expect("a position")
                };
                assert_eq!(open(&c_0), open(&c_f), "{case}: the cursors hold one position");
                cursors += 1;
            }
            (c_0, c_f) => panic!("{case}: a cursor on one side only: {c_0:?} / {c_f:?}"),
        }
        // Each case answers what it is meant to, so the comparison is not
        // of two empty answers.
        match case {
            "getPosts, a hit" | "getPosts, all three" => assert_eq!(v_0["posts"].as_array().map(Vec::len), Some(1), "{case}: {v_0}"),
            "getPosts, an unreadable post" | "getPosts, a nonexistent URI" => assert_eq!(body_0, r#"{"posts":[]}"#, "{case}"),
            "getPostThread, an unreadable anchor" => assert!(status_0 == 400 && body_0.contains("NotFound"), "{case}: {body_0}"),
            "getAuthorFeed, a page with a cursor" => assert_eq!(v_0["feed"].as_array().map(Vec::len), Some(1), "{case}: {v_0}"),
            "getUnreadCount, one readable mention" => assert_eq!(v_0["count"], 1, "{case}: {v_0}"),
            _ => assert_eq!(v_0["count"], 0, "{case}: {v_0}"),
        }
    }
    assert_eq!(cursors, 1, "the feed page's cursors were compared");
    db.drop_db().await;
}

#[tokio::test]
async fn floors_change_no_content_on() {
    floors_change_no_content_in(PublicMode::On, "tf2-identity-on").await;
}

#[tokio::test]
async fn floors_change_no_content_off() {
    floors_change_no_content_in(PublicMode::Off, "tf2-identity-off").await;
}

/// T-TF3 (timing-floors design §6): at floor 0 a private read still
/// records its time in `private_read_unfloored_seconds{endpoint}`.
#[tokio::test]
async fn unfloored_time_is_recorded_at_zero() {
    let Some(db) = fresh_db("tf3_unfloored_metric").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "tf3-metric").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let post = f.private_post(ALICE, "3kpost222222", &aud, None).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let count = |text: &str| crate::test_support::series(text, "private_read_unfloored_seconds_count", &[r#"endpoint="getPosts""#]);
    assert_eq!(count(&handle.render()), None, "nothing recorded before the read");
    let (status, body, _) = get(&base, POSTS, &format!("uris={post}"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("3kpost222222"), "{body}");
    assert_eq!(count(&handle.render()), Some(1.0), "{}", handle.render());
    db.drop_db().await;
}

/// T-F6 (fast-reads design §7.2): through the final router, one private
/// read records one `xrpc_request_seconds{nsid}` observation and one of
/// each stage (`gate`, `reads`, `rest`; no `context` on `getPosts`), whose
/// sum is the unfloored time; a preflight records nothing.
#[tokio::test]
async fn t_f6_request_and_stage_times() {
    use crate::xrpc::router::{FinalConfig, finalize};

    let Some(db) = fresh_db("tf6_stage_metrics").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "tf6-metric").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let post = f.private_post(ALICE, "3kpost222222", &aud, None).await;
    f.ready_all().await;
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
    let series = |text: &str, name: &str, labels: &[&str]| crate::test_support::series(text, name, labels);
    let nsid = r#"nsid="app.nearhorizon.private.getPosts""#;

    // A preflight is answered by CORS and timed nowhere.
    let r = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("{base}/xrpc/{POSTS}"))
        .header("origin", "https://client.example")
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "authorization")
        .send()
        .await
        .expect("preflight");
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(series(&handle.render(), "xrpc_request_seconds_count", &[nsid]), None);

    let (status, body, _) = get(&base, POSTS, &format!("uris={post}"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("3kpost222222"), "{body}");
    let text = handle.render();
    assert_eq!(series(&text, "xrpc_request_seconds_count", &[nsid]), Some(1.0), "{text}");
    let endpoint = r#"endpoint="getPosts""#;
    let unfloored = series(&text, "private_read_unfloored_seconds_sum", &[endpoint]).expect("unfloored");
    let mut sum = 0.0;
    for stage in ["gate", "reads", "rest"] {
        let label = format!(r#"stage="{stage}""#);
        assert_eq!(series(&text, "private_read_stage_seconds_count", &[endpoint, &label]), Some(1.0), "{stage}: {text}");
        sum += series(&text, "private_read_stage_seconds_sum", &[endpoint, &label]).expect("stage sum");
    }
    assert_eq!(series(&text, "private_read_stage_seconds_count", &[endpoint, r#"stage="context""#]), None);
    assert!((sum - unfloored).abs() < 0.001, "stages {sum} against {unfloored}");
    assert!(series(&text, "private_read_stage_seconds_sum", &[endpoint, r#"stage="reads""#]).expect("reads") > 0.0);
    // The request's own time holds the private read's.
    let request = series(&text, "xrpc_request_seconds_sum", &[nsid]).expect("request time");
    assert!(request >= unfloored, "{request} against {unfloored}");
    db.drop_db().await;
}

/// T-F1 (fast-reads design §2.2): a private read called directly. A bare
/// `aud` and `#bsky_appview`, with Horizon's 59-minute window or a full
/// hour, read the post; another fragment, a window past an hour and
/// another method's token are 401.
#[tokio::test]
async fn t_f1_direct_tokens_read_private_posts() {
    use crate::kryphocron_glue::jwt::tests::mint_claims;

    let Some(db) = fresh_db("tf1_direct_tokens").await else {
        return;
    };
    let f = fixture(&db.pool, "tf1-direct").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let post = f.private_post(ALICE, "3kpost222222", &aud, None).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let host = format!("did:web:{AUDIENCE_HOST}");
    let fragment = format!("{host}#bsky_appview");
    let other = format!("{host}#atproto_labeler");
    let now = Utc::now().timestamp();
    for (audience, lxm, window, status) in [
        (host.as_str(), POSTS, 60, 200),
        (fragment.as_str(), POSTS, 3_540, 200),
        (host.as_str(), POSTS, 3_600, 200),
        (fragment.as_str(), POSTS, 3_601, 401),
        (other.as_str(), POSTS, 3_540, 401),
        (fragment.as_str(), THREAD, 3_540, 401),
    ] {
        let token = mint_claims(&signing_key(BOB_KEY), &json!({"iss": BOB, "aud": audience, "lxm": lxm, "iat": now, "exp": now + window}));
        let r = reqwest::Client::new()
            .get(format!("{base}/xrpc/{POSTS}?uris={post}"))
            .header("authorization", format!("Bearer {token}"))
            .send()
            .await
            .expect("request");
        let got = r.status().as_u16();
        let body = r.text().await.expect("body");
        assert_eq!(got, status, "{audience} {lxm} {window}: {body}");
        if status == 200 {
            assert!(body.contains("3kpost222222"), "{body}");
        }
    }
    db.drop_db().await;
}
