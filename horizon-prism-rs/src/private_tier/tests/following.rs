//! S10, the `following` audience mode (docs/internal/design/following-audience.md
//! §11): rule 7 and condition 2 (T-35), the follow-verification flag's
//! lifecycle (T-36), the follows pass (T-41), catch-up (T-42), the writer's
//! side (T-43), one snapshot (T-44), the pre-release count (T-45) and the
//! isolation names (T-40). T-37's cases and T-23's pair 3 live with their
//! kin in the parent module.
//!
//! Rows are written through the writer: the firehose's own commits with the
//! consumer's options (`fire`), and jobs and catch-ups from [`Pds`], one
//! in-memory host for every repository. Each test arms its hooks for
//! `<database>/<did>`, so tests that share the fixture's DIDs never meet.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use crate::aggregations::{PurgeKind, Purger};
use crate::config::BackfillConfig;
use crate::firehose::backfill::{Backfill, BackfillSource, FOLLOWS_RETRY, JobOutcome, ListPage, ListedRecord, PrepareError};
use crate::firehose::gap::{self, GapCause};
use crate::firehose::index_request::{CATCH_UP_DEADLINE, CatchUp, CatchUpConfig, catch_up};
use crate::firehose::jobs::{self, Depth};
use crate::firehose::writer::{ApplyReport, clear_follows_verified, clear_readiness, read_follows_clears};
use crate::microcosm::LinksPage;
use crate::test_support::{TestDb, hooks, waits_on_lock};

use super::*;

// #723's tests (replay-readiness design §11), on this module's harness.
#[path = "replay.rs"]
mod replay;

const FOLLOW: &str = "app.bsky.graph.follow";
const PDS: &str = "https://pds.example.com";
/// Never a member, outside S: a subject admission rejects for a
/// non-member's follow.
const WALT: &str = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";
const R4: &str = "3keeeeeeeeee2";
const R5: &str = "3kffffffffff2";
const R6: &str = "3kgggggggggg2";

fn follow_record(subject: &str) -> Value {
    json!({"$type": FOLLOW, "subject": subject, "createdAt": "2026-10-01T00:00:00Z"})
}

fn follow_op(rkey: &str, subject: &str) -> RawOp {
    RawOp {
        action: Action::Create,
        collection: FOLLOW.into(),
        rkey: rkey.into(),
        cid: Some(format!("bafy{rkey}")),
        record: Some(follow_record(subject)),
    }
}

fn edit_op(rkey: &str, subject: &str) -> RawOp {
    RawOp { action: Action::Update, ..follow_op(rkey, subject) }
}

fn unfollow_op(rkey: &str) -> RawOp {
    RawOp { action: Action::Delete, collection: FOLLOW.into(), rkey: rkey.into(), cid: None, record: None }
}

fn follow_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/{FOLLOW}/{rkey}")
}

/// The consumer's options (`firehose/consumer.rs`): the firehose's own
/// commit, which advances `applied_rev` and `firehose_rev`.
fn firehose() -> ApplyOptions {
    ApplyOptions {
        advance_applied_rev: true,
        dead_letter_readiness: true,
        attempts: 1,
        live: true,
        subscription_rules: true,
        source: WriteSource::Firehose,
    }
}

/// A follow hold: the next follows listing of a DID builds its page, says
/// so, and waits for the test before it answers. A listing "at R1" whose
/// writes land later.
struct Hold {
    did: String,
    listed: Arc<tokio::sync::Notify>,
    gate: Arc<tokio::sync::Semaphore>,
}

/// Every test repository on one PDS: `getLatestCommit`, `listRecords` in
/// rkey order (ascending, or descending), `getRecord`, and `getRepoStatus`
/// through `prepare` (a DID marked inactive answers inactive).
struct Pds {
    revs: Mutex<HashMap<String, String>>,
    records: Mutex<BTreeMap<String, ListedRecord>>,
    page_size: usize,
    descending: bool,
    inactive: Mutex<HashSet<String>>,
    hold: Mutex<Option<Hold>>,
}

impl Pds {
    fn new(page_size: usize, descending: bool) -> Self {
        Self {
            revs: Mutex::new(HashMap::new()),
            records: Mutex::new(BTreeMap::new()),
            page_size,
            descending,
            inactive: Mutex::new(HashSet::new()),
            hold: Mutex::new(None),
        }
    }

    fn set_rev(&self, did: &str, rev: &str) {
        self.revs.lock().expect("revs").insert(did.into(), rev.into());
    }

    fn put_cid(&self, did: &str, collection: &str, rkey: &str, cid: &str, value: Value) {
        let uri = format!("at://{did}/{collection}/{rkey}");
        self.records.lock().expect("records").insert(uri.clone(), ListedRecord { uri, cid: cid.into(), value });
    }

    fn put(&self, did: &str, collection: &str, rkey: &str, value: Value) {
        self.put_cid(did, collection, rkey, &format!("bafy{rkey}"), value);
    }

    fn follow(&self, did: &str, rkey: &str, subject: &str) {
        self.put(did, FOLLOW, rkey, follow_record(subject));
    }

    fn remove(&self, did: &str, collection: &str, rkey: &str) {
        self.records.lock().expect("records").remove(&format!("at://{did}/{collection}/{rkey}"));
    }

    /// Hold the next follows listing of `did`: `(listed, gate)`.
    fn hold_follows(&self, did: &str) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Semaphore>) {
        let listed = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *self.hold.lock().expect("hold") = Some(Hold { did: did.into(), listed: Arc::clone(&listed), gate: Arc::clone(&gate) });
        (listed, gate)
    }
}

#[async_trait]
impl BackfillSource for Pds {
    async fn prepare(&self, did: &str) -> Result<String, PrepareError> {
        if self.inactive.lock().expect("inactive").contains(did) {
            return Err(PrepareError::Negative("repo inactive (deactivated)".into()));
        }
        Ok(PDS.into())
    }

    async fn latest_rev(&self, _: &str, did: &str) -> Result<String, String> {
        Ok(self.revs.lock().expect("revs").get(did).cloned().unwrap_or_else(|| REV.to_string()))
    }

    async fn list_records(&self, _: &str, did: &str, collection: &str, cursor: Option<&str>) -> Result<ListPage, String> {
        let page = {
            let prefix = format!("at://{did}/{collection}/");
            let records = self.records.lock().expect("records");
            let mut all: Vec<&ListedRecord> = records.range(prefix.clone()..).take_while(|(uri, _)| uri.starts_with(&prefix)).map(|(_, r)| r).collect();
            if self.descending {
                all.reverse();
            }
            let start = cursor.map_or(0, |c| c.parse::<usize>().expect("cursor"));
            let end = (start + self.page_size).min(all.len());
            ListPage {
                records: all[start.min(end)..end].iter().map(|r| (*r).clone()).collect(),
                cursor: (end < all.len()).then(|| end.to_string()),
            }
        };
        let hold = if collection == FOLLOW {
            let mut hold = self.hold.lock().expect("hold");
            if hold.as_ref().is_some_and(|h| h.did == did) { hold.take() } else { None }
        } else {
            None
        };
        if let Some(hold) = hold {
            hold.listed.notify_one();
            let _ = hold.gate.acquire().await;
        }
        Ok(page)
    }

    async fn get_record(&self, _: &str, did: &str, collection: &str, rkey: &str) -> Result<Option<ListedRecord>, String> {
        Ok(self.records.lock().expect("records").get(&format!("at://{did}/{collection}/{rkey}")).cloned())
    }

    async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
        Ok(LinksPage { total: 0, linking_records: Vec::new(), cursor: None })
    }
}

/// A DID's follow-verification state.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Flag {
    set: bool,
    rev: Option<String>,
    firehose: Option<String>,
    clears: i64,
    /// `follows_verified()`: set, and the firehose has reached the rev.
    holds: bool,
}

/// One S10 test: the private fixture in `mode`, every DID's `applied_rev`,
/// head and `firehose_rev` at [`REV`], a server without a floor, and the
/// host.
struct H {
    db: TestDb,
    f: Fixture,
    base: String,
    pds: Arc<Pds>,
}

impl H {
    async fn new(test: &str, mode: PublicMode) -> Option<Self> {
        Self::with_pds(test, mode, Pds::new(100, false)).await
    }

    async fn with_pds(test: &str, mode: PublicMode, pds: Pds) -> Option<Self> {
        let db = fresh_db(test).await?;
        let f = fixture_in(&db.pool, test, mode).await;
        exec(&f.pool, &format!("UPDATE sync_state SET applied_rev = '{REV}', head_rev = '{REV}', firehose_rev = '{REV}'")).await;
        let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
        Some(Self { db, f, base, pds: Arc::new(pds) })
    }

    async fn finish(self) {
        self.db.drop_db().await;
    }

    fn pool(&self) -> &PgPool {
        &self.f.pool
    }

    /// A hook key for `did` in this test's database.
    fn key(&self, did: &str) -> String {
        format!("{}/{did}", self.db.name())
    }

    fn backfill(&self) -> Backfill {
        self.backfill_with(BackfillConfig::default())
    }

    fn backfill_with(&self, cfg: BackfillConfig) -> Backfill {
        Backfill::new(self.f.writer.clone(), Arc::clone(&self.pds) as Arc<dyn BackfillSource>, cfg, 1)
            .with_community_patterns(vec!["pds.example.com".into()])
    }

    /// A commit applied by the firehose, as the consumer applies it.
    async fn fire(&self, repo: &str, rev: &str, ops: Vec<RawOp>) -> ApplyReport {
        self.f.writer.apply(repo, rev, &ops, firehose()).await.expect("apply")
    }

    /// `did` follows `subject` in its repository at `rev`, and the firehose
    /// applies it.
    async fn commit_follow(&self, did: &str, rkey: &str, subject: &str, rev: &str) {
        self.pds.follow(did, rkey, subject);
        self.pds.set_rev(did, rev);
        let report = self.fire(did, rev, vec![follow_op(rkey, subject)]).await;
        assert!(report.failed.is_empty(), "{report:?}");
    }

    /// `did` deletes a follow at `rev`, and the firehose applies it.
    async fn commit_unfollow(&self, did: &str, rkey: &str, rev: &str) {
        self.pds.remove(did, FOLLOW, rkey);
        self.pds.set_rev(did, rev);
        self.fire(did, rev, vec![unfollow_op(rkey)]).await;
    }

    /// A commit of `did` at `rev` that touches nothing indexed here.
    async fn commit_nothing(&self, did: &str, rev: &str) {
        self.pds.set_rev(did, rev);
        self.fire(did, rev, Vec::new()).await;
    }

    /// An audience by `owner` with `mode`, in its repository and stored.
    async fn audience(&self, owner: &str, rkey: &str, mode: &str) -> String {
        self.audience_of(owner, rkey, mode, &[]).await
    }

    /// [`Self::audience`] with `members`.
    async fn audience_of(&self, owner: &str, rkey: &str, mode: &str, members: &[&str]) -> String {
        let record = json!({"$type": NSID_AUDIENCE, "mode": mode, "members": members, "createdAt": "2026-09-26T00:00:00Z"});
        self.f.ingest(owner, NSID_AUDIENCE, rkey, record.clone()).await;
        self.pds.put(owner, NSID_AUDIENCE, rkey, record);
        format!("at://{owner}/{NSID_AUDIENCE}/{rkey}")
    }

    /// A private post by `author` under `audience`, in its repository and
    /// stored.
    async fn post(&self, author: &str, rkey: &str, audience: &str, reply: Option<(&str, &str)>) -> String {
        let record = self.f.post_record(author, rkey, audience, reply).await;
        self.f.ingest(author, NSID_POST_PRIVATE, rkey, record.clone()).await;
        self.pds.put(author, NSID_POST_PRIVATE, rkey, record);
        format!("at://{author}/{NSID_POST_PRIVATE}/{rkey}")
    }

    /// Alice's `following` audience and her post under it, ready.
    async fn alice_following(&self, tag: &str) -> (String, String) {
        let aud = self.audience(ALICE, &format!("3kaud{tag}"), "following").await;
        let post = self.post(ALICE, &format!("3kpst{tag}"), &aud, None).await;
        self.f.ready_all().await;
        (aud, post)
    }

    /// A record stored by the private tier directly, as an admitted write
    /// would store it (a non-member outside S has no rule admitting its
    /// own audience), and in its repository.
    async fn direct(&self, author: &str, nsid: &str, rkey: &str, record: Value) -> String {
        let uri = format!("at://{author}/{nsid}/{rkey}");
        let op = crate::private_tier::PrivateOp {
            uri: uri.clone(),
            author_did: author.into(),
            nsid: nsid.into(),
            rkey: rkey.into(),
            cid: format!("bafy{rkey}"),
            rev: REV.into(),
            record: record.clone(),
            source: WriteSource::Firehose,
            created_at: Utc::now(),
            notify_max_age: chrono::Duration::hours(48),
        };
        let mut conn = self.pool().acquire().await.expect("conn");
        KryphocronPrivateTier.apply(&mut conn, &op).await.expect("private apply");
        self.pds.put(author, nsid, rkey, record);
        uri
    }

    /// Eve: an eligible non-member owner outside S, tracked, with a
    /// `following` audience and a post under it, ready.
    async fn eve(&self, tag: &str) -> (String, String) {
        actor(self.pool(), EVE, "eve.test", "eve.test").await;
        exec(self.pool(), &format!("INSERT INTO private_owner_eligible (did, confirmed_at) VALUES ('{EVE}', now())")).await;
        exec(
            self.pool(),
            &format!("INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at, firehose_rev) VALUES ('{EVE}', '{REV}', '{REV}', now(), '{REV}')"),
        )
        .await;
        self.f.writer.scope().write_guard().await.private_owners.insert(EVE.into());
        let aud = self
            .direct(EVE, NSID_AUDIENCE, &format!("3kaud{tag}"), json!({"$type": NSID_AUDIENCE, "mode": "following", "members": [], "createdAt": "2026-09-26T00:00:00Z"}))
            .await;
        let rkey = format!("3kpst{tag}");
        let record = self.f.post_record(EVE, &rkey, &aud, None).await;
        let post = self.direct(EVE, NSID_POST_PRIVATE, &rkey, record).await;
        self.f.ready(EVE).await;
        (aud, post)
    }

    async fn fresh_heads(&self) {
        exec(self.pool(), "UPDATE sync_state SET head_checked_at = now() WHERE head_rev IS NOT NULL").await;
    }

    /// Whether `reader` reads `uri`: the oracle, the prefilter and
    /// `getPosts` give one answer (parent T-37), or the test fails.
    async fn reads(&self, reader: &str, uri: &str) -> bool {
        let served = self.served(reader, uri).await;
        let oracle = oracle_admits(self.pool(), reader, uri).await;
        let prefilter = !schema::prefilter_uris(self.pool(), reader, &[uri.to_string()], super::CAP).await.expect("prefilter").is_empty();
        assert_eq!((oracle, prefilter), (served, served), "{reader} on {uri}: oracle {oracle}, prefilter {prefilter}, getPosts {served}");
        served
    }

    /// Whether `getPosts` serves `uri` to `reader` (readiness and the head
    /// check included).
    async fn served(&self, reader: &str, uri: &str) -> bool {
        self.fresh_heads().await;
        let (status, body, _) = get(&self.base, POSTS, &format!("uris={uri}"), reader, key_of(reader)).await;
        assert_eq!(status, 200, "{body}");
        body != r#"{"posts":[]}"#
    }

    async fn flag(&self, did: &str) -> Flag {
        let (set, rev, firehose, clears, holds): (bool, Option<String>, Option<String>, i64, bool) = sqlx::query_as(
            "SELECT follows_verified_at IS NOT NULL, follows_verified_rev, firehose_rev, follows_clears, \
               COALESCE(follows_verified_at IS NOT NULL AND firehose_rev >= follows_verified_rev, false) \
             FROM sync_state WHERE did = $1",
        )
        .bind(did)
        .fetch_one(self.pool())
        .await
        .expect("sync_state");
        Flag { set, rev, firehose, clears, holds }
    }

    async fn applied(&self, did: &str) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>("SELECT applied_rev FROM sync_state WHERE did = $1")
            .bind(did)
            .fetch_optional(self.pool())
            .await
            .expect("applied_rev")
            .flatten()
    }

    /// The stored follow at `uri`: `(subject, rev)`.
    async fn stored(&self, uri: &str) -> Option<(String, String)> {
        sqlx::query_as("SELECT subject_did, rev FROM follows WHERE uri = $1").bind(uri).fetch_optional(self.pool()).await.expect("follows")
    }

    async fn tombstone(&self, uri: &str) -> Option<String> {
        sqlx::query_scalar("SELECT rev FROM record_tombstones WHERE uri = $1").bind(uri).fetch_optional(self.pool()).await.expect("tombstone")
    }

    /// The job `(did, depth)`: `(state, last_error, seconds until due,
    /// attempts, generation)`.
    async fn job(&self, did: &str, depth: Depth) -> Option<(String, Option<String>, i64, i32, i64)> {
        sqlx::query_as(
            "SELECT state, last_error, EXTRACT(EPOCH FROM not_before - now())::bigint, attempts, generation \
             FROM backfill_jobs WHERE did = $1 AND depth = $2",
        )
        .bind(did)
        .bind(depth.as_str())
        .fetch_optional(self.pool())
        .await
        .expect("job")
    }

    /// Whether an `authz` job of `did` is queued.
    async fn authz_queued(&self, did: &str) -> bool {
        self.job(did, Depth::Authz).await.is_some_and(|j| j.0 == "queued")
    }

    async fn clear_jobs(&self) {
        exec(self.pool(), "DELETE FROM backfill_jobs").await;
    }

    /// Queue `(did, depth)` (resetting it) and run it.
    async fn run(&self, b: &Backfill, did: &str, depth: Depth) -> JobOutcome {
        let mut conn = self.pool().acquire().await.expect("conn");
        jobs::enqueue(&mut conn, did, depth).await.expect("enqueue");
        drop(conn);
        self.rerun(b, did, depth).await
    }

    /// Run the queued `(did, depth)`, due or not.
    async fn rerun(&self, b: &Backfill, did: &str, depth: Depth) -> JobOutcome {
        b.run_job(did, depth).await.expect("db").expect("a queued job").1
    }

    /// `run` on a task of its own, for an interleaving.
    fn spawn_run(&self, b: &Backfill, did: &str, depth: Depth) -> tokio::task::JoinHandle<Result<Option<(crate::firehose::backfill::Job, JobOutcome)>, sqlx::Error>> {
        let (b, did) = (b.clone(), did.to_string());
        tokio::spawn(async move { b.run_job(&did, depth).await })
    }

    async fn enqueue(&self, did: &str, depth: Depth) {
        let mut conn = self.pool().acquire().await.expect("conn");
        jobs::enqueue(&mut conn, did, depth).await.expect("enqueue");
    }

    fn catch_up_cfg() -> CatchUpConfig {
        CatchUpConfig { min_interval: Duration::from_secs(10), max_pages: 8, reconcile_confirm_limit: 50, attempts: 1, deadline: CATCH_UP_DEADLINE }
    }

    /// `requestRecord`'s catch-up of `did` at `rev`: `caughtUp`.
    async fn catch_up(&self, did: &str, rev: &str) -> bool {
        catch_up(self.pds.as_ref(), &self.f.writer, &CatchUp::new(Self::catch_up_cfg()), did, PDS, rev).await
    }

    async fn gap(&self) -> Result<i64, sqlx::Error> {
        gap::run_gap_transaction(self.pool(), self.f.writer.scope(), self.f.writer.private().as_ref(), GapCause::Operator, false).await
    }

    async fn epoch(&self) -> i64 {
        let mut conn = self.pool().acquire().await.expect("conn");
        gap::current_epoch(&mut conn).await.expect("epoch")
    }

    /// Readiness for every fixture DID again, as after their jobs.
    async fn ready_again(&self) {
        exec(self.pool(), "UPDATE members SET ready_at = now()").await;
        self.f.ready_all().await;
    }

    async fn member_ready(&self, did: &str) -> bool {
        sqlx::query_scalar("SELECT ready_at IS NOT NULL FROM members WHERE did = $1").bind(did).fetch_one(self.pool()).await.expect("member")
    }

    async fn owner_ready(&self, did: &str) -> bool {
        readiness::owner_ready(self.pool(), did).await.expect("owner ready")
    }
}

/// Wait until a backend of the test's database waits on a lock in a
/// statement beginning with `statement`.
async fn until_waiting(pool: &PgPool, statement: &str) {
    for _ in 0..400 {
        if waits_on_lock(pool, statement).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nothing waited on a lock in {statement:?}");
}

/// The prefix of the gap's exclusive lock statement, which a settle's
/// shared one does not share.
const GAP_LOCK_PREFIX: &str = "SELECT pg_advisory_xact_lock(hashtextextended";
const SETTLE_LOCK_PREFIX: &str = "SELECT pg_advisory_xact_lock_shared(";

/// A host whose head is always `rev` (the prober's divergence).
struct Heads(String);

#[async_trait]
impl HeadSource for Heads {
    async fn latest_rev(&self, _pds: &str, _did: &str, _timeout: Duration) -> Result<String, String> {
        Ok(self.0.clone())
    }
}

fn prober(h: &H, head: &str) -> HeadProber {
    HeadProber::new(
        h.pool().clone(),
        h.f.writer.scope().clone(),
        Arc::clone(h.f.writer.private()),
        Arc::new(Heads(head.into())),
        HostBudgets::new(BudgetRates {
            max_probe_per_host: 25.0,
            head_max_age_secs: 60.0,
            read_fetch_per_host: 10.0,
            backfill_per_host: 5.0,
            catch_up_per_host: 10.0,
        }),
        SyncConfig::default(),
    )
}

// ---- T-35: rule 7 ----

/// Every request of `uri` for `reader` misses byte-identically to a URI that
/// does not exist, after the floor.
async fn uniform_miss(base: &str, reader: &str, uri: &str, missing: &str) {
    for (method, param) in [(POSTS, "uris"), (THREAD, "uri")] {
        let (status, body, took) = get(base, method, &format!("{param}={uri}"), reader, key_of(reader)).await;
        let (none_status, none, _) = get(base, method, &format!("{param}={missing}"), reader, key_of(reader)).await;
        assert_eq!((status, &body), (none_status, &none), "{method} for {reader}");
        assert!(took >= FLOOR, "{method}: {took:?} under the floor");
    }
}

/// T-35 (§11): O's `following` audience, O follows V (Bob) and not C
/// (Carol), flag set. V reads O's post through `getPosts`, `getAuthorFeed`
/// and `getPostThread`; C misses as a URI that does not exist. The
/// firehose's unfollow ends V's read; with the flag NULL only O reads. The
/// post reaches V's timeline only when V follows O too. A block either way
/// keeps V out.
async fn t35_rule_7(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let (_, p) = h.alice_following("t35000").await;
    h.commit_follow(ALICE, "3kt35folbob1", BOB, R1).await;
    set_follows_verified(h.pool(), ALICE, true).await;
    let floored = serve(h.f.state(Some(Duration::from_secs(1)))).await;
    let missing = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kt35nothere");
    h.fresh_heads().await;

    assert!(h.reads(BOB, &p).await);
    assert!(!h.reads(CAROL, &p).await);
    assert!(h.reads(ALICE, &p).await);
    let feed = |did: &'static str| {
        let base = h.base.clone();
        async move { get(&base, FEED, &format!("actor={ALICE}"), did, key_of(did)).await.1 }
    };
    assert!(feed(BOB).await.contains(&p));
    assert_eq!(feed(CAROL).await, r#"{"feed":[]}"#);
    let (status, body, _) = get(&h.base, THREAD, &format!("uri={p}"), BOB, BOB_KEY).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(&p));
    uniform_miss(&floored, CAROL, &p, &missing).await;

    // O's unfollow of V, applied by the firehose: V misses, uniformly. The
    // flag still holds (the firehose is past the verified rev).
    h.commit_unfollow(ALICE, "3kt35folbob1", R2).await;
    assert!(h.flag(ALICE).await.holds);
    assert!(!h.reads(BOB, &p).await);
    h.fresh_heads().await;
    uniform_miss(&floored, BOB, &p, &missing).await;

    // Followed again; with the flag NULL, only O reads.
    h.commit_follow(ALICE, "3kt35folbob2", BOB, R3).await;
    assert!(h.reads(BOB, &p).await);
    set_follows_verified(h.pool(), ALICE, false).await;
    assert!(!h.reads(BOB, &p).await);
    assert!(h.reads(ALICE, &p).await);
    set_follows_verified(h.pool(), ALICE, true).await;

    // The timeline holds it only when V also follows O; the author feed
    // holds it either way.
    let timeline = || async { get(&h.base, TIMELINE, "", BOB, BOB_KEY).await.1 };
    assert!(!timeline().await.contains(&p));
    assert!(feed(BOB).await.contains(&p));
    h.commit_follow(BOB, "3kt35bobali1", ALICE, R1).await;
    assert!(timeline().await.contains(&p));

    // Rule 6: a block either way, the follow present.
    for (blocker, blocked) in [(ALICE, BOB), (BOB, ALICE)] {
        exec(h.pool(), &block_sql(blocker, blocked)).await;
        assert!(!h.reads(BOB, &p).await, "{blocker} blocks {blocked}");
        exec(h.pool(), "DELETE FROM blocks").await;
    }
    assert!(h.reads(BOB, &p).await);
    h.finish().await;
}

#[tokio::test]
async fn t35_rule_7_on() {
    t35_rule_7("s10-t35-on", PublicMode::On).await;
}

#[tokio::test]
async fn t35_rule_7_off() {
    t35_rule_7("s10-t35-off", PublicMode::Off).await;
}

// ---- T-36: the flag's lifecycle ----

/// T-36: each clear of §3.3 sets the flag NULL in its event's own
/// transaction (a gap; a failed firehose op of O; a failed catch-up write;
/// the prober's divergence; O leaving the tracked set while in S; the purge
/// marker). Each but the gap replaces the counter.
#[tokio::test]
async fn t36_every_clear_clears_the_flag() {
    let Some(h) = H::new("s10-t36-clears", PublicMode::On).await else {
        return;
    };
    h.alice_following("t36c00").await;
    let set = |h: &H| {
        let pool = h.pool().clone();
        async move { set_follows_verified(&pool, ALICE, true).await }
    };

    // A gap: no counter.
    set(&h).await;
    let before = h.flag(ALICE).await;
    h.gap().await.expect("gap");
    let after = h.flag(ALICE).await;
    assert!(before.set && !after.set);
    assert_eq!(after.clears, before.clears, "the gap leaves the counter");

    // A failed op of O on the firehose.
    h.ready_again().await;
    set(&h).await;
    let before = h.flag(ALICE).await;
    let report = h.fire(ALICE, R1, vec![follow_op("3kt36badsub1", "not a did")]).await;
    assert!(report.readiness_cleared, "{report:?}");
    let after = h.flag(ALICE).await;
    assert!(!after.set);
    assert_ne!(after.clears, before.clears);

    // A failed catch-up write: a listed audience that does not parse.
    h.ready_again().await;
    set(&h).await;
    let before = h.flag(ALICE).await;
    h.pds.set_rev(ALICE, R2);
    h.pds.put_cid(ALICE, NSID_AUDIENCE, "3kaudt36c00", "bafybroken", json!({"$type": NSID_AUDIENCE, "mode": "following", "members": "not a list", "createdAt": "2026-09-26T00:00:00Z"}));
    assert!(!h.catch_up(ALICE, R2).await);
    let after = h.flag(ALICE).await;
    assert!(!after.set);
    assert_ne!(after.clears, before.clears);
    h.pds.put(ALICE, NSID_AUDIENCE, "3kaudt36c00", json!({"$type": NSID_AUDIENCE, "mode": "following", "members": [], "createdAt": "2026-09-26T00:00:00Z"}));

    // The prober's divergence, past the grace with no `authz` in flight.
    h.ready_again().await;
    h.clear_jobs().await;
    set(&h).await;
    let before = h.flag(ALICE).await;
    exec(h.pool(), &format!("UPDATE sync_state SET diverged_since = now() - interval '1 day' WHERE did = '{ALICE}'")).await;
    assert_eq!(prober(&h, R6).probe(ALICE, PDS).await, crate::sync::ProbeResult::DivergenceDetected);
    let after = h.flag(ALICE).await;
    assert!(!after.set);
    assert_ne!(after.clears, before.clears);

    // Leaving the tracked set while staying in S: Eve, in S, an owner
    // through her audience, deletes it.
    actor(h.pool(), EVE, "eve.test", "eve.test").await;
    exec(h.pool(), &format!("INSERT INTO private_owner_eligible (did, confirmed_at) VALUES ('{EVE}', now())")).await;
    exec(h.pool(), &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{EVE}', 1)")).await;
    h.f.writer.scope().write_guard().await.scope.insert(EVE.into());
    h.f.ingest(EVE, NSID_AUDIENCE, "3kt36eveaud1", json!({"$type": NSID_AUDIENCE, "mode": "following", "members": [], "createdAt": "2026-09-26T00:00:00Z"})).await;
    exec(h.pool(), &format!("INSERT INTO sync_state (did, applied_rev, follows_verified_at, follows_verified_rev, firehose_rev) VALUES ('{EVE}', '{REV}', now(), '{REV}', '{REV}') ON CONFLICT (did) DO UPDATE SET follows_verified_at = now()")).await;
    let before = h.flag(EVE).await;
    assert!(before.set);
    h.f.apply_op(EVE, Action::Delete, NSID_AUDIENCE, "3kt36eveaud1", REV2, None).await;
    let after = h.flag(EVE).await;
    assert!(!after.set, "Eve left the tracked set: {after:?}");
    assert_ne!(after.clears, before.clears);
    assert_eq!(h.applied(EVE).await, None, "her repo-head state goes");

    // The purge marker (last: Alice leaves S).
    set(&h).await;
    let before = h.flag(ALICE).await;
    let purger = Purger::new(h.pool().clone(), h.f.writer.scope().clone(), Arc::clone(h.f.writer.private()));
    purger.marker(ALICE, PurgeKind::OutOfScope).await.expect("marker");
    let after = h.flag(ALICE).await;
    assert!(!after.set);
    assert_ne!(after.clears, before.clears);
    h.finish().await;
}

/// T-36: a job that settles `applied_rev` without a pass writes NULL; a
/// `resync` never changes `applied_rev`, leaves the flag when it inserts no
/// follow and changes no subject, and clears it (queuing `authz`) when it
/// inserts one (§4.4).
#[tokio::test]
async fn t36_settles_without_a_pass_and_resyncs() {
    let Some(h) = H::new("s10-t36-settles", PublicMode::On).await else {
        return;
    };
    let b = h.backfill();
    // Bob owns a `list` audience only: no pass, and the settle writes NULL.
    let aud = h.audience(BOB, "3kt36boblst1", "list").await;
    h.post(BOB, "3kt36bobpst1", &aud, None).await;
    h.f.ready_all().await;
    set_follows_verified(h.pool(), BOB, true).await;
    h.pds.set_rev(BOB, R1);
    h.clear_jobs().await;
    assert_eq!(h.run(&b, BOB, Depth::Authz).await, JobOutcome::Ready);
    let flag = h.flag(BOB).await;
    assert!(!flag.set, "{flag:?}");
    assert_eq!(h.applied(BOB).await.as_deref(), Some(R1));

    // Alice, a following owner, verified.
    h.alice_following("t36s00").await;
    h.commit_follow(ALICE, "3kt36folbob1", BOB, R1).await;
    set_follows_verified(h.pool(), ALICE, true).await;
    let before = h.flag(ALICE).await;
    h.clear_jobs().await;
    h.pds.set_rev(ALICE, R2);
    // A resync that rewrites the stored follow, same subject.
    assert_eq!(h.run(&b, ALICE, Depth::Resync).await, JobOutcome::Done);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R1), "a resync never moves applied_rev");
    assert_eq!(h.flag(ALICE).await, before);
    assert!(!h.authz_queued(ALICE).await);
    // A resync that inserts a follow: cleared, and `authz` a minute on.
    h.pds.follow(ALICE, "3kt36folcar1", CAROL);
    assert_eq!(h.run(&b, ALICE, Depth::Resync).await, JobOutcome::Done);
    let after = h.flag(ALICE).await;
    assert!(!after.set);
    assert_ne!(after.clears, before.clears);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R1));
    let authz = h.job(ALICE, Depth::Authz).await.expect("authz");
    assert_eq!(authz.0, "queued");
    assert!((30..=90).contains(&authz.2), "due a minute on: {authz:?}");
    h.finish().await;
}

/// T-36: an audience written with `mode = following` for an existing owner
/// queues `authz`, whether its flag is NULL or set, unless the write's
/// source is `Backfill`; for a non-owner it queues nothing; an `authz`
/// already running is not reset; a `member` job that rewrites the audience
/// at a newer R queues no second `authz` (review 4, finding 2).
#[tokio::test]
async fn t36_a_following_audience_queues_authz() {
    let Some(h) = H::new("s10-t36-ingest", PublicMode::On).await else {
        return;
    };
    let list = h.audience(ALICE, "3kt36alilst1", "list").await;
    h.post(ALICE, "3kt36alipst1", &list, None).await;
    h.f.ready_all().await;
    h.clear_jobs().await;
    // Flag NULL.
    h.audience(ALICE, "3kt36alifol1", "following").await;
    assert!(h.authz_queued(ALICE).await);
    // Flag set.
    exec(h.pool(), "UPDATE backfill_jobs SET state = 'done'").await;
    set_follows_verified(h.pool(), ALICE, true).await;
    h.audience(ALICE, "3kt36alifol2", "following").await;
    assert!(h.authz_queued(ALICE).await);
    // A `Backfill` write, and a `list` audience: nothing.
    exec(h.pool(), "UPDATE backfill_jobs SET state = 'done'").await;
    let record = json!({"$type": NSID_AUDIENCE, "mode": "following", "members": [], "createdAt": "2026-09-26T00:00:00Z"});
    h.f.apply_from(WriteSource::Backfill, ALICE, Action::Create, NSID_AUDIENCE, "3kt36alifol3", REV2, Some(record.clone())).await;
    h.pds.put(ALICE, NSID_AUDIENCE, "3kt36alifol3", record);
    h.audience(ALICE, "3kt36alilst2", "list").await;
    assert!(!h.authz_queued(ALICE).await);
    // A non-owner: Carol's first private record makes her one and queues
    // `private`, no `authz`.
    h.audience(CAROL, "3kt36carfol1", "following").await;
    assert!(!h.authz_queued(CAROL).await);
    assert!(h.job(CAROL, Depth::Private).await.is_some());
    // A running `authz` is left alone.
    exec(h.pool(), &format!("UPDATE backfill_jobs SET state = 'running' WHERE did = '{ALICE}' AND depth = 'authz'")).await;
    let running = h.job(ALICE, Depth::Authz).await.map(|j| (j.0, j.4));
    h.audience(ALICE, "3kt36alifol4", "following").await;
    assert_eq!(h.job(ALICE, Depth::Authz).await.map(|j| (j.0, j.4)), running);
    // A member job rewriting the audiences at R1 queues no second `authz`.
    exec(h.pool(), "UPDATE backfill_jobs SET state = 'done'").await;
    h.pds.set_rev(ALICE, R1);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Member).await, JobOutcome::Ready);
    assert!(!h.authz_queued(ALICE).await);
    h.finish().await;
}

/// T-36, review 3 finding 4: a settle that writes NULL reads "no
/// `following` audience" before an ingest of one commits; the ingest,
/// testing no flag, still leaves an `authz` queued.
#[tokio::test]
async fn t36_ingest_beside_a_null_settle_still_queues_authz() {
    let Some(h) = H::new("s10-t36-interleave", PublicMode::On).await else {
        return;
    };
    let list = h.audience(ALICE, "3kt36intlst1", "list").await;
    h.post(ALICE, "3kt36intpst1", &list, None).await;
    h.f.ready_all().await;
    set_follows_verified(h.pool(), ALICE, true).await;
    h.clear_jobs().await;
    let b = h.backfill();
    let hook = hooks::arm(hooks::SETTLE_AFTER_FOLLOW_UP, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    h.audience(ALICE, "3kt36intfol1", "following").await;
    hook.release();
    let (_, outcome) = run.await.expect("task").expect("db").expect("a job");
    assert_eq!(outcome, JobOutcome::Ready);
    assert!(!h.flag(ALICE).await.set);
    assert!(h.authz_queued(ALICE).await);
    h.finish().await;
}

/// T-36, review 2 finding 4: the gap and a prober divergence of O cycle
/// (the gap holding `members`, the prober `sync_state(O)`); one side is
/// aborted with `40P01` and retried, and the end state is the gap's.
#[tokio::test]
async fn t36_gap_and_prober_deadlock_is_detected_and_retried() {
    let Some(h) = H::new("s10-t36-deadlock", PublicMode::On).await else {
        return;
    };
    h.alice_following("t36d00").await;
    set_follows_verified(h.pool(), ALICE, true).await;
    h.clear_jobs().await;
    exec(h.pool(), &format!("UPDATE sync_state SET diverged_since = now() - interval '1 day' WHERE did = '{ALICE}'")).await;
    let hook = hooks::arm(hooks::PROBER_HOLDS_SYNC_STATE, &h.key(ALICE));
    let prober = prober(&h, R6);
    let probe = {
        let prober = prober.clone();
        tokio::spawn(async move { prober.probe(ALICE, PDS).await })
    };
    hook.reached().await;
    let gap = {
        let (pool, scope, private) = (h.pool().clone(), h.f.writer.scope().clone(), Arc::clone(h.f.writer.private()));
        tokio::spawn(async move { gap::run_gap_transaction(&pool, &scope, private.as_ref(), GapCause::Operator, false).await })
    };
    until_waiting(h.pool(), "UPDATE sync_state SET follows_verified_at = NULL").await;
    hook.release();
    let (gap_result, probe_result) = (gap.await.expect("gap task"), probe.await.expect("probe task"));
    let gap_deadlocked = gap_result.as_ref().is_err_and(|e| e.to_string().contains("deadlock"));
    let probe_deadlocked = matches!(&probe_result, crate::sync::ProbeResult::Error(e) if e.contains("deadlock"));
    assert!(gap_deadlocked ^ probe_deadlocked, "one side aborted: gap {gap_result:?}, probe {probe_result:?}");
    // The aborted side is retried, as the consumer and the next probe do.
    if gap_deadlocked {
        h.gap().await.expect("gap retried");
    } else {
        prober.probe(ALICE, PDS).await;
    }
    assert!(!h.member_ready(ALICE).await);
    assert!(!h.owner_ready(ALICE).await);
    assert!(!h.flag(ALICE).await.set);
    assert_eq!(h.epoch().await, 1);
    assert!(h.authz_queued(ALICE).await);
    h.finish().await;
}

/// T-36, review 2 finding 3: a settle holds the gap lock; the gap waits; a
/// second settle waits behind the gap, not beside the first. The first
/// commits, the gap runs and clears it, and the second reads the new epoch
/// and settles nothing. With the `gap_epoch` row deleted by hand, a settle
/// ends with an error and writes nothing.
#[tokio::test]
async fn t36_a_waiting_gap_is_not_starved_and_a_missing_epoch_fails_closed() {
    let Some(h) = H::new("s10-t36-gaplock", PublicMode::On).await else {
        return;
    };
    h.alice_following("t36g00").await;
    h.clear_jobs().await;
    let b = h.backfill();
    let hook = hooks::arm(hooks::SETTLE_HOLDS_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let first = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    let gap = {
        let (pool, scope, private) = (h.pool().clone(), h.f.writer.scope().clone(), Arc::clone(h.f.writer.private()));
        tokio::spawn(async move { gap::run_gap_transaction(&pool, &scope, private.as_ref(), GapCause::Operator, false).await })
    };
    until_waiting(h.pool(), GAP_LOCK_PREFIX).await;
    // A `private` run: the gap resets every `authz` row, which would
    // supersede an `authz` run before its epoch is read.
    h.enqueue(BOB, Depth::Private).await;
    let second = h.spawn_run(&b, BOB, Depth::Private);
    until_waiting(h.pool(), SETTLE_LOCK_PREFIX).await;
    assert!(waits_on_lock(h.pool(), GAP_LOCK_PREFIX).await, "the gap still waits");
    hook.release();
    assert_eq!(first.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    assert_eq!(gap.await.expect("task").expect("gap"), 1);
    assert_eq!(second.await.expect("task").expect("db").expect("job").1, JobOutcome::Requeued("gap_epoch_changed"));
    // The gap cleared what the first settle wrote.
    assert!(!h.flag(ALICE).await.set);
    assert!(!h.owner_ready(ALICE).await);
    drop(hook);

    // No `gap_epoch` row: the settle fails, and nothing is written.
    exec(h.pool(), "DELETE FROM worker_state WHERE key = 'gap_epoch'").await;
    h.ready_again().await;
    let applied = h.applied(BOB).await;
    h.pds.set_rev(BOB, R5);
    assert!(b.run_job(BOB, Depth::Authz).await.is_err());
    assert_eq!(h.job(BOB, Depth::Authz).await.expect("job").0, "running", "the row is left as the run had it");
    assert_eq!(h.applied(BOB).await, applied);
    h.finish().await;
}

/// T-36, review 6 finding 4: O's latest commit falls in a trim gap and is
/// never applied. The gap's `authz` settles the flag at that commit's rev;
/// `firehose_rev` stays below it, so O's `following` audiences are read by
/// O alone, with no job re-queued for it. O's next commit, once applied,
/// makes the flag hold.
#[tokio::test]
async fn t36_a_trim_gap_waits_for_the_next_commit() {
    let Some(h) = H::new("s10-t36-trim", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t36t00").await;
    h.commit_follow(ALICE, "3kt36trmbob1", BOB, R1).await;
    // R2, Alice's latest commit, is lost in the gap.
    h.pds.set_rev(ALICE, R2);
    h.clear_jobs().await;
    h.gap().await.expect("gap");
    assert_eq!(h.rerun(&h.backfill(), ALICE, Depth::Authz).await, JobOutcome::Ready);
    // The readers' own jobs, as after the gap.
    h.ready_again().await;
    let flag = h.flag(ALICE).await;
    assert!(flag.set && !flag.holds, "{flag:?}");
    assert_eq!((flag.rev.as_deref(), flag.firehose.as_deref()), (Some(R2), Some(R1)));
    assert!(!h.reads(BOB, &p).await);
    assert!(h.reads(ALICE, &p).await);
    assert!(!h.authz_queued(ALICE).await, "nothing re-queued for this");
    h.commit_nothing(ALICE, R3).await;
    assert!(h.flag(ALICE).await.holds);
    assert!(h.reads(BOB, &p).await);
    h.finish().await;
}

/// The value `following_owners_waiting_firehose` renders, if sampled.
fn waiting_gauge(text: &str) -> Option<f64> {
    text.lines()
        .find_map(|l| l.strip_prefix("following_owners_waiting_firehose "))
        .and_then(|v| v.trim().parse().ok())
}

/// T-36, review 7 finding 2: the gauge. One owner whose flag holds, one
/// whose firehose never saw it (an uncrawled PDS), one whose latest change
/// the firehose missed (a dead-lettered `#sync`): a sample gives 2; once
/// the third's next commit is applied, 1. A NULL flag is never counted.
#[tokio::test]
async fn t36_the_waiting_gauge_counts_set_flags_that_do_not_hold() {
    let Some(h) = H::new("s10-t36-gauge", PublicMode::On).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    for (did, rev, firehose) in [(ALICE, R1, Some(R1)), (BOB, R1, None), (CAROL, R2, Some(R1))] {
        sqlx::query("UPDATE sync_state SET follows_verified_at = now(), follows_verified_rev = $2, firehose_rev = $3 WHERE did = $1")
            .bind(did)
            .bind(rev)
            .bind(firehose)
            .execute(h.pool())
            .await
            .expect("flag");
    }
    exec(h.pool(), &format!("UPDATE sync_state SET follows_verified_at = NULL, firehose_rev = NULL WHERE did = '{DAVE}'")).await;
    let b = h.backfill();
    b.sample_metrics().await.expect("sample");
    assert_eq!(waiting_gauge(&handle.render()), Some(2.0));
    h.commit_nothing(CAROL, R2).await;
    b.sample_metrics().await.expect("sample");
    assert_eq!(waiting_gauge(&handle.render()), Some(1.0));
    h.finish().await;
}

/// design-v4 §II-5.10, §II-6.1, §II-6.11.5, §14.2: the backfill sampler's
/// other gauges. `private_owners_not_ready_total` follows owner readiness;
/// a private op from the live firehose sets the last event, so
/// `private_tier_last_event_age_seconds` is near 0; the oldest due `authz`
/// job not completed sets `authz_daily_backlog_seconds`; and `list_sync`
/// rows are in `backfill_jobs{depth}`.
#[tokio::test]
async fn the_backfill_sampler_exports_private_readiness_ingest_age_and_job_backlogs() {
    use crate::private_tier::gauges;
    use crate::test_support::series;
    let Some(h) = H::new("metrics-sampler", PublicMode::On).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let b = h.backfill();
    let sample = || async {
        b.sample_metrics().await.expect("sample");
        handle.render()
    };
    let value = |text: &str, name: &str, labels: &[&str]| series(text, name, labels).unwrap_or_else(|| panic!("{name} {labels:?} missing:\n{text}"));

    // Alice owns an audience and a post, ready: then her readiness goes.
    let before = Utc::now().timestamp_millis();
    h.alice_following("met").await;
    assert!(gauges::last_event_ms() >= before, "a firehose private op is the last event");
    let text = sample().await;
    let ready = value(&text, gauges::METRIC_OWNERS_NOT_READY, &[]);
    assert!(value(&text, gauges::METRIC_LAST_EVENT_AGE, &[]) <= 5.0);
    exec(h.pool(), &format!("DELETE FROM private_owner_ready WHERE did = '{ALICE}'")).await;
    assert_eq!(value(&sample().await, gauges::METRIC_OWNERS_NOT_READY, &[]), ready + 1.0);
    h.f.ready_all().await;
    assert_eq!(value(&sample().await, gauges::METRIC_OWNERS_NOT_READY, &[]), ready);

    // An `authz` job due two days ago and not run: the backlog is two
    // days. Once it is done, nothing that old is left.
    let list_sync = |text: &str| value(text, "backfill_jobs", &["depth=\"list_sync\"", "state=\"queued\""]);
    let queued = list_sync(&sample().await);
    exec(
        h.pool(),
        &format!(
            "INSERT INTO backfill_jobs (did, depth, state, not_before) VALUES ('{WALT}', 'authz', 'queued', now() - interval '2 days') \
             ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', not_before = EXCLUDED.not_before"
        ),
    )
    .await;
    exec(h.pool(), &format!("INSERT INTO backfill_jobs (did, depth, state) VALUES ('{WALT}', 'list_sync', 'queued')")).await;
    let text = sample().await;
    let backlog = value(&text, "authz_daily_backlog_seconds", &[]);
    assert!((172_700.0..173_000.0).contains(&backlog), "{backlog}");
    assert_eq!(list_sync(&text), queued + 1.0);
    exec(h.pool(), &format!("UPDATE backfill_jobs SET state = 'done' WHERE did = '{WALT}' AND depth = 'authz'")).await;
    assert!(value(&sample().await, "authz_daily_backlog_seconds", &[]) < 3600.0);
    h.finish().await;
}

// ---- T-41: the pass ----

/// T-41: a complete pass that writes no row sets the flag with
/// `applied_rev`; one that inserts a row writes NULL, and its re-run a
/// minute later writes nothing and sets the flag. A listed follow stored
/// with the same subject is not rewritten (its rev and the counts stay).
#[tokio::test]
async fn t41_a_complete_pass_sets_the_flag() {
    let Some(h) = H::new("s10-t41-complete", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t41c00").await;
    h.commit_follow(ALICE, "3kt41folbob1", BOB, R1).await;
    let bob_follow = follow_uri(ALICE, "3kt41folbob1");
    h.clear_jobs().await;
    let b = h.backfill();
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    let flag = h.flag(ALICE).await;
    assert!(flag.holds, "{flag:?}");
    assert_eq!(flag.rev.as_deref(), Some(R1));
    assert!(h.reads(BOB, &p).await);
    assert!(!h.reads(CAROL, &p).await);

    // A new follow the firehose has not applied: the pass inserts it, which
    // clears the flag, and the settle writes NULL and re-queues itself a
    // minute on with the marker.
    let counts = || async {
        let n: i32 = sqlx::query_scalar(&format!("SELECT followers_count FROM actor_counts WHERE did = '{BOB}'")).fetch_one(h.pool()).await.expect("counts");
        n
    };
    let bob_count = counts().await;
    h.pds.follow(ALICE, "3kt41folcar1", CAROL);
    h.pds.set_rev(ALICE, R2);
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert!(!h.flag(ALICE).await.set);
    let job = h.job(ALICE, Depth::Authz).await.expect("authz");
    assert_eq!((job.0.as_str(), job.1.as_deref(), job.3), ("queued", Some(FOLLOWS_RETRY), 0));
    assert!((30..=90).contains(&job.2), "a minute on: {job:?}");
    assert_eq!(h.stored(&bob_follow).await, Some((BOB.to_string(), R1.to_string())), "same subject: not rewritten");
    assert_eq!(counts().await, bob_count);
    assert!(!h.reads(CAROL, &p).await);

    // The re-run writes nothing and sets the flag; it holds once the
    // firehose applies R2. `mark_done` clears the marker.
    assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    let flag = h.flag(ALICE).await;
    assert!(flag.set && !flag.holds, "{flag:?}");
    assert_eq!(h.job(ALICE, Depth::Authz).await.expect("authz").1, None);
    h.fire(ALICE, R2, vec![follow_op("3kt41folcar1", CAROL)]).await;
    assert!(h.flag(ALICE).await.holds);
    assert!(h.reads(CAROL, &p).await);
    h.finish().await;
}

/// T-41: a clear that commits between a `member` run's start and its settle
/// leaves the flag NULL, and readiness is decided as before.
#[tokio::test]
async fn t41_a_clear_during_a_run_leaves_the_flag_null() {
    let Some(h) = H::new("s10-t41-clear", PublicMode::On).await else {
        return;
    };
    h.alice_following("t41r00").await;
    h.clear_jobs().await;
    let b = h.backfill();
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Member).await;
    let run = h.spawn_run(&b, ALICE, Depth::Member);
    hook.reached().await;
    {
        let mut conn = h.pool().acquire().await.expect("conn");
        clear_readiness(&mut conn, h.f.writer.private().as_ref(), ALICE).await.expect("clear");
    }
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    assert!(!h.flag(ALICE).await.set);
    assert!(h.member_ready(ALICE).await, "readiness as before");
    h.finish().await;
}

/// T-41, review 1 finding 1, both routes, and T-36's counter read (review
/// 7, finding 3):
/// - O has no `sync_state` row when its `private` run starts; the run makes
///   it. During the run O edits its follow of V to a subject that is not a
///   DID: the firehose op fails, `clear_readiness` replaces the counter,
///   the advance updates the row; the settle leaves the flag NULL.
/// - The row deleted during a run (as `owners.rs` does outside S) and made
///   again by the firehose's advance: a fresh counter, and NULL. A settle
///   that finds no row at all takes the `INSERT` branch: NULL.
/// - The counter read: with no row, it makes one and returns its counter;
///   with a row, the stored counter, and nothing else changes; with the row
///   deleted before the read, the run reads a fresh counter, no error
///   reaches `fail()`, and its pass sets the flag.
#[tokio::test]
async fn t41_the_counter_is_read_from_a_row_the_run_made() {
    let Some(h) = H::new("s10-t41-counter", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t41k00").await;
    h.commit_follow(ALICE, "3kt41kfolbob", BOB, R1).await;
    h.clear_jobs().await;
    let b = h.backfill();
    let del = format!("DELETE FROM sync_state WHERE did = '{ALICE}'");

    // The first route.
    exec(h.pool(), &del).await;
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    assert_eq!(h.applied(ALICE).await, None);
    assert!(h.flag(ALICE).await.clears > 0, "the run made the row");
    let report = h.fire(ALICE, R2, vec![edit_op("3kt41kfolbob", "not a did")]).await;
    assert!(report.readiness_cleared);
    hook.release();
    run.await.expect("task").expect("db").expect("job");
    drop(hook);
    assert!(!h.flag(ALICE).await.set);
    exec(h.pool(), &format!("UPDATE sync_state SET head_rev = applied_rev WHERE did = '{ALICE}'")).await;
    h.ready_again().await;
    assert!(!h.reads(BOB, &p).await);

    // The second route.
    h.clear_jobs().await;
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Authz).await;
    let run = h.spawn_run(&b, ALICE, Depth::Authz);
    hook.reached().await;
    exec(h.pool(), &del).await;
    h.commit_nothing(ALICE, R3).await;
    hook.release();
    run.await.expect("task").expect("db").expect("job");
    assert!(!h.flag(ALICE).await.set, "a fresh counter");
    drop(hook);
    // No row at all at the settle: the `INSERT` branch.
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Authz).await;
    let run = h.spawn_run(&b, ALICE, Depth::Authz);
    hook.reached().await;
    exec(h.pool(), &del).await;
    hook.release();
    run.await.expect("task").expect("db").expect("job");
    assert!(!h.flag(ALICE).await.set, "the INSERT branch writes NULL");
    drop(hook);

    // The counter read itself.
    exec(h.pool(), &del).await;
    let mut conn = h.pool().acquire().await.expect("conn");
    let fresh = read_follows_clears(&mut conn, ALICE).await.expect("read");
    assert!(fresh > 0);
    exec(h.pool(), &format!("UPDATE sync_state SET applied_rev = '{R3}' WHERE did = '{ALICE}'")).await;
    assert_eq!(read_follows_clears(&mut conn, ALICE).await.expect("read"), fresh);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R3), "nothing else changes");
    drop(conn);
    // Deleted before the read: a fresh counter, and the pass sets the flag.
    exec(h.pool(), &del).await;
    h.clear_jobs().await;
    h.pds.set_rev(ALICE, R3);
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert!(h.flag(ALICE).await.set);
    h.finish().await;
}

/// T-41, review 1 finding 2 (#722): a `member` run, a `private` run and a
/// catch-up, each held just before its settle while a gap commits, settle
/// nothing: neither the flag, `ready_at`, owner readiness nor `applied_rev`
/// moves. The other order: the settle holds the gap lock, the gap waits for
/// its commit, then clears what it wrote. On a fresh database whose
/// `gap_epoch` row exists only from the migration's seed.
#[tokio::test]
async fn t41_722_a_gap_orders_every_settle() {
    let Some(h) = H::new("s10-t41-722", PublicMode::On).await else {
        return;
    };
    h.alice_following("t41g00").await;
    h.commit_follow(ALICE, "3kt41gfolbob", BOB, R1).await;
    let seeded: String = sqlx::query_scalar("SELECT value #>> '{}' FROM worker_state WHERE key = 'gap_epoch'").fetch_one(h.pool()).await.expect("seed");
    assert_eq!(seeded, "0");
    let b = h.backfill();
    h.pds.set_rev(ALICE, R2);
    for depth in [Depth::Member, Depth::Private] {
        h.clear_jobs().await;
        h.ready_again().await;
        let applied = h.applied(ALICE).await;
        let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
        h.enqueue(ALICE, depth).await;
        let run = h.spawn_run(&b, ALICE, depth);
        hook.reached().await;
        h.gap().await.expect("gap");
        hook.release();
        let (_, outcome) = run.await.expect("task").expect("db").expect("job");
        assert_eq!(outcome, JobOutcome::Requeued("gap_epoch_changed"), "{depth:?}");
        assert!(!h.flag(ALICE).await.set, "{depth:?}");
        assert!(!h.member_ready(ALICE).await, "{depth:?}");
        assert!(!h.owner_ready(ALICE).await, "{depth:?}");
        assert_eq!(h.applied(ALICE).await, applied, "{depth:?}");
    }
    // A catch-up.
    h.clear_jobs().await;
    h.ready_again().await;
    let applied = h.applied(ALICE).await;
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    let catch = {
        let (pds, writer) = (Arc::clone(&h.pds), h.f.writer.clone());
        tokio::spawn(async move { catch_up(pds.as_ref(), &writer, &CatchUp::new(H::catch_up_cfg()), ALICE, PDS, R2).await })
    };
    hook.reached().await;
    h.gap().await.expect("gap");
    hook.release();
    assert!(!catch.await.expect("task"));
    drop(hook);
    assert!(!h.flag(ALICE).await.set);
    assert!(!h.member_ready(ALICE).await);
    assert_eq!(h.applied(ALICE).await, applied);

    // The other order.
    h.clear_jobs().await;
    h.ready_again().await;
    let epoch = h.epoch().await;
    let hook = hooks::arm(hooks::SETTLE_HOLDS_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Member).await;
    let run = h.spawn_run(&b, ALICE, Depth::Member);
    hook.reached().await;
    let gap = {
        let (pool, scope, private) = (h.pool().clone(), h.f.writer.scope().clone(), Arc::clone(h.f.writer.private()));
        tokio::spawn(async move { gap::run_gap_transaction(&pool, &scope, private.as_ref(), GapCause::Operator, false).await })
    };
    until_waiting(h.pool(), GAP_LOCK_PREFIX).await;
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    assert_eq!(gap.await.expect("task").expect("gap"), epoch + 1);
    assert!(!h.flag(ALICE).await.set, "the gap cleared the flag the settle set");
    assert!(!h.member_ready(ALICE).await);
    assert!(!h.owner_ready(ALICE).await);
    h.finish().await;
}

/// T-41: a follow deleted in O's repository, the delete never delivered:
/// the next `authz` deletes the row and sets the flag; no `following` read
/// succeeds after its pass, the flag still set from before included.
#[tokio::test]
async fn t41_an_undelivered_unfollow_ends_at_the_next_authz() {
    let Some(h) = H::new("s10-t41-hole", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t41h00").await;
    h.commit_follow(ALICE, "3kt41hfolbob", BOB, R1).await;
    set_follows_verified(h.pool(), ALICE, true).await;
    assert!(h.reads(BOB, &p).await, "the hole of §3.7, until the next authz");
    h.pds.remove(ALICE, FOLLOW, "3kt41hfolbob");
    h.pds.set_rev(ALICE, R2);
    h.clear_jobs().await;
    let b = h.backfill();
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Authz).await;
    let run = h.spawn_run(&b, ALICE, Depth::Authz);
    hook.reached().await;
    assert_eq!(h.stored(&follow_uri(ALICE, "3kt41hfolbob")).await, None);
    assert!(h.flag(ALICE).await.set);
    assert!(!h.reads(BOB, &p).await);
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    let flag = h.flag(ALICE).await;
    assert!(flag.set && flag.rev.as_deref() == Some(R2), "{flag:?}");
    h.commit_nothing(ALICE, R3).await;
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

/// T-41, review 1 finding 4: a stored follow of O at rev C, absent from O's
/// repository, whose head R is below C (a rolled-back PDS). The pass
/// confirms it gone, re-reads it, clears the flag in its own transaction,
/// and ends incomplete; the `authz` run re-queues a minute on with the
/// marker, then an hour on. V never reads after the clear. Once O commits
/// above C, the next pass deletes the row and sets the flag, and
/// `mark_done` clears the marker.
#[tokio::test]
async fn t41_a_rollback_is_a_contradiction() {
    let Some(h) = H::new("s10-t41-rollback", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t41b00").await;
    h.commit_follow(ALICE, "3kt41bfolbob", BOB, R3).await;
    set_follows_verified(h.pool(), ALICE, true).await;
    // The PDS is restored to R2, before the follow.
    h.pds.remove(ALICE, FOLLOW, "3kt41bfolbob");
    h.pds.set_rev(ALICE, R2);
    h.clear_jobs().await;
    let b = h.backfill();
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert!(!h.flag(ALICE).await.set);
    assert!(h.stored(&follow_uri(ALICE, "3kt41bfolbob")).await.is_some(), "nothing deleted");
    let job = h.job(ALICE, Depth::Authz).await.expect("authz");
    assert_eq!((job.0.as_str(), job.1.as_deref()), ("queued", Some(FOLLOWS_RETRY)));
    assert!((30..=90).contains(&job.2), "a minute on: {job:?}");
    assert!(!h.reads(BOB, &p).await);
    // Meeting it again: an hour on.
    assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    let job = h.job(ALICE, Depth::Authz).await.expect("authz");
    assert_eq!((job.1.as_deref(), job.3), (Some(FOLLOWS_RETRY), 0));
    assert!((3500..=3700).contains(&job.2), "an hour on: {job:?}");
    assert!(!h.reads(BOB, &p).await);
    // O commits above C: the row goes, the flag is set, the marker cleared.
    h.commit_nothing(ALICE, R4).await;
    assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(h.stored(&follow_uri(ALICE, "3kt41bfolbob")).await, None);
    assert!(h.flag(ALICE).await.holds);
    assert_eq!(h.job(ALICE, Depth::Authz).await.expect("authz").1, None);
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

/// T-41, review 2 finding 2: the same rolled-back O with its flag set, whose
/// `authz` settles through `ceiling_hit` and, separately,
/// `auth_write_failed`: the flag is NULL after the run, though the settle
/// wrote neither `applied_rev` nor the flag. The same in a catch-up that
/// ends unsettled.
#[tokio::test]
async fn t41_a_contradiction_clears_whatever_the_settle_does() {
    let Some(h) = H::with_pds("s10-t41-ceiling", PublicMode::On, Pds::new(1, false)).await else {
        return;
    };
    h.alice_following("t41e00").await;
    h.audience(ALICE, "3kaudt41e01", "list").await;
    h.commit_follow(ALICE, "3kt41efolbob", BOB, R3).await;
    h.pds.remove(ALICE, FOLLOW, "3kt41efolbob");
    h.pds.set_rev(ALICE, R2);
    let ceiling = h.backfill_with(BackfillConfig { full_listing_ceiling: 1, ..BackfillConfig::default() });
    for (case, b) in [("ceiling_hit", &ceiling), ("auth_write_failed", &h.backfill())] {
        if case == "auth_write_failed" {
            h.pds.put(ALICE, "tools.kryphocron.graph.block", "3kt41ebadblk", json!({"$type": "tools.kryphocron.graph.block", "subject": "not a did", "createdAt": "2026-09-26T00:00:00Z"}));
        }
        h.clear_jobs().await;
        set_follows_verified(h.pool(), ALICE, true).await;
        let applied = h.applied(ALICE).await;
        assert_eq!(h.run(b, ALICE, Depth::Authz).await, JobOutcome::Done, "{case}");
        assert!(!h.flag(ALICE).await.set, "{case}");
        assert_eq!(h.applied(ALICE).await, applied, "{case}");
    }
    h.pds.remove(ALICE, "tools.kryphocron.graph.block", "3kt41ebadblk");
    // A catch-up.
    h.ready_again().await;
    set_follows_verified(h.pool(), ALICE, true).await;
    exec(h.pool(), &format!("UPDATE sync_state SET applied_rev = '{REV}' WHERE did = '{ALICE}'")).await;
    assert!(!h.catch_up(ALICE, R2).await);
    assert!(!h.flag(ALICE).await.set);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(REV));
    h.finish().await;
}

/// T-41, review 2 finding 6: a stored follow created and deleted on the
/// firehose after R. With the delete applied between `getRecord`'s answer
/// and the re-read, the re-read finds the row gone and the pass completes.
/// With the delete applied only after the re-read: a contradiction, the
/// flag cleared, and the re-queue a minute on completes and sets the flag.
#[tokio::test]
async fn t41_the_reread_resolves_a_firehose_delete() {
    let Some(h) = H::new("s10-t41-reread", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t41w00").await;
    h.pds.set_rev(ALICE, R1);
    // Created after R, on the firehose; never listed.
    h.fire(ALICE, R2, vec![follow_op("3kt41wfolbob", BOB)]).await;
    h.clear_jobs().await;
    let b = h.backfill();
    let hook = hooks::arm(hooks::PASS_BEFORE_REREAD, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Authz).await;
    let run = h.spawn_run(&b, ALICE, Depth::Authz);
    hook.reached().await;
    h.fire(ALICE, R3, vec![unfollow_op("3kt41wfolbob")]).await;
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    drop(hook);
    assert!(h.flag(ALICE).await.holds, "complete");

    // The delete after the re-read.
    h.fire(ALICE, R4, vec![follow_op("3kt41wfolbo2", BOB)]).await;
    h.clear_jobs().await;
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert!(!h.flag(ALICE).await.set, "a contradiction");
    assert!(!h.reads(BOB, &p).await);
    h.fire(ALICE, R5, vec![unfollow_op("3kt41wfolbo2")]).await;
    assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert!(h.flag(ALICE).await.holds);
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

/// T-41: a listed follow whose subject changed is rewritten, which clears
/// the flag (§4.4); a listed follow whose subject does not parse deletes
/// its stored row; one with no stored row is skipped and counted, and the
/// pass still completes.
#[tokio::test]
async fn t41_changed_and_unparsable_subjects() {
    let Some(h) = H::new("s10-t41-subjects", PublicMode::On).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let (_, p) = h.alice_following("t41s00").await;
    h.commit_follow(ALICE, "3kt41sfolbob", BOB, R1).await;
    h.commit_follow(ALICE, "3kt41sfolcar", CAROL, R1).await;
    let b = h.backfill();
    // Bob's follow now names Dave in the repository.
    h.pds.follow(ALICE, "3kt41sfolbob", DAVE);
    h.pds.set_rev(ALICE, R2);
    h.clear_jobs().await;
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(h.stored(&follow_uri(ALICE, "3kt41sfolbob")).await.map(|s| s.0), Some(DAVE.to_string()));
    assert!(!h.flag(ALICE).await.set, "a changed subject clears");
    // Carol's names nobody now; another names nobody and was never stored.
    h.pds.put(ALICE, FOLLOW, "3kt41sfolcar", follow_record("not a did"));
    h.pds.put(ALICE, FOLLOW, "3kt41sfolnon", follow_record("nobody at all"));
    h.pds.set_rev(ALICE, R3);
    assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(h.stored(&follow_uri(ALICE, "3kt41sfolcar")).await, None);
    assert_eq!(h.stored(&follow_uri(ALICE, "3kt41sfolnon")).await, None);
    assert!(h.flag(ALICE).await.set, "the pass completed");
    let rendered = handle.render();
    assert!(rendered.lines().any(|l| l.starts_with("follows_pass_skipped_total") && l.ends_with(" 1")), "{rendered}");
    h.commit_nothing(ALICE, R3).await;
    assert!(!h.reads(CAROL, &p).await);
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

/// T-41: over `follows_pass_ceiling` the flag stays NULL with no re-queue;
/// O stays ready and reads its own `following` post, and V does not.
#[tokio::test]
async fn t41_over_the_ceiling_the_owner_reads_alone() {
    let Some(h) = H::with_pds("s10-t41-over", PublicMode::On, Pds::new(1, false)).await else {
        return;
    };
    let (_, p) = h.alice_following("t41o00").await;
    for (rkey, did) in [("3kt41ofol001", BOB), ("3kt41ofol002", CAROL), ("3kt41ofol003", DAVE)] {
        h.commit_follow(ALICE, rkey, did, R1).await;
    }
    h.clear_jobs().await;
    let b = h.backfill_with(BackfillConfig { follows_pass_ceiling: 2, ..BackfillConfig::default() });
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert!(!h.flag(ALICE).await.set);
    assert_eq!(h.job(ALICE, Depth::Authz).await.expect("authz").0, "done");
    assert!(h.owner_ready(ALICE).await && h.member_ready(ALICE).await);
    assert!(h.reads(ALICE, &p).await);
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

/// T-41, review 1 finding 6: more than `reconcile_confirm_limit` stale rows
/// leave the pass incomplete and `authz` re-queued a minute on. A `member`
/// job whose public collections use up the job's shared budget still
/// gives the pass its own full budget.
#[tokio::test]
async fn t41_the_pass_has_its_own_budget() {
    let Some(h) = H::new("s10-t41-budget", PublicMode::On).await else {
        return;
    };
    h.alice_following("t41u00").await;
    for (rkey, did) in [("3kt41ufol001", BOB), ("3kt41ufol002", CAROL), ("3kt41ufol003", DAVE)] {
        h.commit_follow(ALICE, rkey, did, R1).await;
        h.pds.remove(ALICE, FOLLOW, rkey);
    }
    h.pds.set_rev(ALICE, R2);
    h.clear_jobs().await;
    let b = h.backfill_with(BackfillConfig { reconcile_confirm_limit: 2, ..BackfillConfig::default() });
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert!(!h.flag(ALICE).await.set);
    let job = h.job(ALICE, Depth::Authz).await.expect("authz");
    assert_eq!(job.0, "queued");
    assert!((30..=90).contains(&job.2), "a minute on: {job:?}");
    let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM follows WHERE author_did = '{ALICE}'")).fetch_one(h.pool()).await.expect("count");
    assert_eq!(left, 1, "two confirmed and deleted");

    // A member job: two stale posts use up the shared budget of 2 before
    // the plan's follows; the pass still reconciles the last follow.
    for rkey in ["3kt41upst001", "3kt41upst002"] {
        exec(
            h.pool(),
            &format!("INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at) VALUES ('at://{ALICE}/app.bsky.feed.post/{rkey}', '{ALICE}', '{rkey}', 'bafy', '{REV}', '{{}}'::jsonb, now())"),
        )
        .await;
    }
    h.clear_jobs().await;
    assert_eq!(h.run(&b, ALICE, Depth::Member).await, JobOutcome::Ready);
    let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM follows WHERE author_did = '{ALICE}'")).fetch_one(h.pool()).await.expect("count");
    assert_eq!(left, 0);
    assert!(h.flag(ALICE).await.set, "the pass completed on its own budget");
    h.finish().await;
}

/// The stored follows of `did`, and `scope_dids`.
async fn follows_and_scope(pool: &PgPool, did: &str) -> (Vec<(String, String)>, Vec<String>) {
    let follows = sqlx::query_as("SELECT uri, subject_did FROM follows WHERE author_did = $1 ORDER BY uri").bind(did).fetch_all(pool).await.expect("follows");
    let scope = sqlx::query_scalar("SELECT did FROM scope_dids ORDER BY did").fetch_all(pool).await.expect("scope");
    (follows, scope)
}

/// T-41, C-F1: a member owner with 10 001 follows on a newest-first host
/// and on an ascending one: the stored follows and `scope_dids` are the
/// same before and after the pass.
async fn newest_ten_thousand(test: &str, descending: bool) {
    let Some(h) = H::with_pds(test, PublicMode::On, Pds::new(100, descending)).await else {
        return;
    };
    h.alice_following("t41n00").await;
    for i in 0..=10_000 {
        h.pds.follow(ALICE, &format!("3kn{i:010}"), &format!("did:plc:n{i:023}"));
    }
    h.pds.set_rev(ALICE, R1);
    h.clear_jobs().await;
    let b = h.backfill();
    let hook = hooks::arm(hooks::PASS_AFTER_LISTING, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Member).await;
    let run = h.spawn_run(&b, ALICE, Depth::Member);
    // The plan writes 10 000 follows first, longer than a hook's usual wait.
    for _ in 0..1200 {
        if hook.hits() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    hook.reached().await;
    let before = follows_and_scope(h.pool(), ALICE).await;
    assert_eq!(before.0.len(), 10_000, "the plan's newest 10 000");
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    assert_eq!(follows_and_scope(h.pool(), ALICE).await, before);
    // The plan's inserts cleared the flag (§4.4); the `authz` that follows
    // writes nothing and sets it.
    assert!(!h.flag(ALICE).await.set);
    drop(hook);
    assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(follows_and_scope(h.pool(), ALICE).await, before);
    assert!(h.flag(ALICE).await.set);
    h.finish().await;
}

#[tokio::test]
async fn t41_a_member_owner_keeps_its_newest_ten_thousand_descending() {
    newest_ten_thousand("s10-t41-newest-desc", true).await;
}

#[tokio::test]
async fn t41_a_member_owner_keeps_its_newest_ten_thousand_ascending() {
    newest_ten_thousand("s10-t41-newest-asc", false).await;
}

/// T-41: a non-member owner whose follow of member V predates Prism: the
/// `private` job inserts it, which writes NULL; the `authz` re-run a minute
/// later sets the flag, and V reads.
#[tokio::test]
async fn t41_a_follow_that_predates_prism() {
    let Some(h) = H::new("s10-t41-predates", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.eve("t41p00").await;
    h.pds.follow(EVE, "3kt41pfolbob", BOB);
    h.pds.set_rev(EVE, R1);
    h.clear_jobs().await;
    let b = h.backfill();
    assert_eq!(h.run(&b, EVE, Depth::Private).await, JobOutcome::Ready);
    assert!(!h.flag(EVE).await.set);
    let authz = h.job(EVE, Depth::Authz).await.expect("authz");
    assert_eq!(authz.0, "queued");
    assert!((30..=90).contains(&authz.2), "{authz:?}");
    assert_eq!(h.rerun(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    assert!(h.flag(EVE).await.set);
    h.commit_nothing(EVE, R1).await;
    assert!(h.reads(BOB, &p).await);
    assert!(!h.reads(CAROL, &p).await);
    h.finish().await;
}

/// T-41: a job that decided "not a following owner" while the audience was
/// switched to `following` mid-run: `authz` follows (enqueued after a
/// `private` run, the own row re-queued after an `authz` one), and its pass
/// sets the flag.
#[tokio::test]
async fn t41_a_switch_mid_run_is_rechecked_at_settle() {
    let Some(h) = H::new("s10-t41-recheck", PublicMode::On).await else {
        return;
    };
    let aud = h.audience(ALICE, "3kaudt41m00", "list").await;
    h.post(ALICE, "3kpstt41m00", &aud, None).await;
    h.f.ready_all().await;
    h.clear_jobs().await;
    let b = h.backfill();
    for depth in [Depth::Private, Depth::Authz] {
        exec(h.pool(), "UPDATE private_audiences SET mode = 'list'").await;
        h.clear_jobs().await;
        let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
        h.enqueue(ALICE, depth).await;
        let run = h.spawn_run(&b, ALICE, depth);
        hook.reached().await;
        exec(h.pool(), "UPDATE private_audiences SET mode = 'following'").await;
        hook.release();
        run.await.expect("task").expect("db").expect("job");
        drop(hook);
        let authz = h.job(ALICE, Depth::Authz).await.expect("authz");
        assert_eq!(authz.0, "queued", "{depth:?}");
        assert!(authz.2 <= 1, "due now: {authz:?}");
        assert!(!h.flag(ALICE).await.set);
        assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready, "{depth:?}");
        assert!(h.flag(ALICE).await.set, "{depth:?}");
    }
    h.finish().await;
}

/// T-41, review 2 finding 1: a non-member owner of a `following` audience
/// confirmed ineligible by its `authz` run ends `done`, with no re-queue and
/// no `authz` queued after it; its audience row remains, and it owns no
/// `following` audience. The same for a DID no longer tracked at settle.
/// Startup's rollout queues nothing for it.
#[tokio::test]
async fn t41_an_ineligible_owner_is_not_requeued() {
    let Some(h) = H::new("s10-t41-ineligible", PublicMode::On).await else {
        return;
    };
    h.eve("t41i00").await;
    h.pds.inactive.lock().expect("inactive").insert(EVE.into());
    h.clear_jobs().await;
    let b = h.backfill();
    assert_eq!(h.run(&b, EVE, Depth::Authz).await, JobOutcome::Done);
    assert_eq!(h.job(EVE, Depth::Authz).await.expect("authz").0, "done");
    let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM private_audiences WHERE owner_did = '{EVE}' AND mode = 'following'")).fetch_one(h.pool()).await.expect("rows");
    assert_eq!(rows, 1);
    let mut conn = h.pool().acquire().await.expect("conn");
    assert!(!KryphocronPrivateTier.owns_following_audience(&mut conn, EVE).await.expect("owns"));
    drop(conn);
    assert_eq!(KryphocronPrivateTier.enqueue_following_verification(h.pool()).await.expect("rollout"), 0);
    assert_eq!(h.job(EVE, Depth::Authz).await.expect("authz").0, "done");

    // Tracked at no point of the run's decision, eligible again, and gone
    // from the tracked set before the settle: no re-check.
    h.pds.inactive.lock().expect("inactive").clear();
    h.f.writer.scope().write_guard().await.private_owners.remove(EVE);
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(EVE));
    h.enqueue(EVE, Depth::Private).await;
    let run = h.spawn_run(&b, EVE, Depth::Private);
    hook.reached().await;
    h.f.writer.scope().write_guard().await.private_owners.remove(EVE);
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Done);
    assert_eq!(h.job(EVE, Depth::Authz).await.expect("authz").0, "done", "no authz after it");
    assert_eq!(h.job(EVE, Depth::Private).await.expect("private").0, "done");
    h.finish().await;
}

// ---- T-42: catch-up ----

/// T-42: a following owner with 30 follows, all stored: `caughtUp: true`,
/// `applied_rev = R` and the flag set in one transaction. With 900 follows:
/// `caughtUp: false`, and neither moves. A non-following owner's catch-up
/// writes the flag NULL. A caller with no `sync_state` row at step 2 has one
/// made there. An audience switched to `following` in the catch-up's own
/// listing runs the pass in that call.
#[tokio::test]
async fn t42_catch_up_verifies_follows_within_its_budget() {
    let Some(h) = H::new("s10-t42-budget", PublicMode::On).await else {
        return;
    };
    h.alice_following("t42b00").await;
    let ops: Vec<RawOp> = (0..30).map(|i| follow_op(&format!("3kt42f{i:06}"), &format!("did:plc:f{i:023}"))).collect();
    for op in &ops {
        h.pds.follow(ALICE, &op.rkey, op.record.as_ref().and_then(|r| r["subject"].as_str()).expect("subject"));
    }
    h.f.writer.apply(ALICE, REV, &ops, ApplyOptions { advance_applied_rev: false, ..firehose() }).await.expect("apply");
    h.pds.set_rev(ALICE, R1);
    assert!(h.catch_up(ALICE, R1).await);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R1));
    let flag = h.flag(ALICE).await;
    assert!(flag.set && flag.rev.as_deref() == Some(R1), "{flag:?}");

    // 900 follows: over the shared page budget.
    for i in 30..900 {
        h.pds.follow(ALICE, &format!("3kt42f{i:06}"), &format!("did:plc:f{i:023}"));
    }
    set_follows_verified(h.pool(), ALICE, false).await;
    h.pds.set_rev(ALICE, R2);
    assert!(!h.catch_up(ALICE, R2).await);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R1));
    assert!(!h.flag(ALICE).await.set);

    // A non-following owner: Bob, `list` only, flag set: NULL.
    let list = h.audience(BOB, "3kaudt42b01", "list").await;
    h.post(BOB, "3kpstt42b01", &list, None).await;
    h.f.ready_all().await;
    set_follows_verified(h.pool(), BOB, true).await;
    h.pds.set_rev(BOB, R1);
    assert!(h.catch_up(BOB, R1).await);
    assert!(!h.flag(BOB).await.set);

    // No `sync_state` row: made at step 2. Bob's audience switches to
    // `following` in the listing: the pass runs in that call.
    exec(h.pool(), &format!("DELETE FROM sync_state WHERE did = '{BOB}'")).await;
    h.pds.put_cid(BOB, NSID_AUDIENCE, "3kaudt42b01", "bafyswitched", json!({"$type": NSID_AUDIENCE, "mode": "following", "members": [], "createdAt": "2026-09-26T00:00:00Z"}));
    h.pds.set_rev(BOB, R2);
    assert!(h.catch_up(BOB, R2).await);
    let flag = h.flag(BOB).await;
    assert!(flag.set, "the pass ran in the call: {flag:?}");
    h.finish().await;
}

/// `requestRecord` over the test host.
async fn index_server(h: &H) -> String {
    let state = crate::xrpc::handlers::index::IndexState {
        app: h.f.app.clone(),
        source: Arc::clone(&h.pds) as Arc<dyn BackfillSource>,
        writer: h.f.writer.clone(),
        attempts: 1,
        catch_up: CatchUp::new(H::catch_up_cfg()),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, crate::xrpc::router::index_router(state).into_make_service_with_connect_info::<SocketAddr>())
            .await
            .expect("serve");
    });
    format!("http://{addr}")
}

/// T-42, review 7 finding 1: O's flag holds and V reads O's older
/// `following` posts. With the consumer held back, O writes a new
/// `following` post P at R and calls `requestRecord`: `caughtUp: true`,
/// `applied_rev = R`, `follows_verified_rev = R`. Until the firehose
/// applies R, V's reads of P and of the older posts miss, and O's other
/// private posts read; then V reads them all. The lexicon says so.
#[tokio::test]
async fn t42_caught_up_following_audiences_wait_for_the_firehose() {
    let Some(h) = H::new("s10-t42-firehose", PublicMode::On).await else {
        return;
    };
    let (aud, older) = h.alice_following("t42f00").await;
    let list = h.audience_of(ALICE, "3kaudt42f01", "list", &[BOB]).await;
    let other = h.post(ALICE, "3kpstt42f01", &list, None).await;
    h.commit_follow(ALICE, "3kt42ffolbob", BOB, R1).await;
    set_follows_verified(h.pool(), ALICE, true).await;
    h.f.ready_all().await;
    assert!(h.reads(BOB, &older).await);
    // P at R2, held back from the firehose.
    let p_rkey = "3kpstt42f02";
    let record = h.f.post_record(ALICE, p_rkey, &aud, None).await;
    h.pds.put(ALICE, NSID_POST_PRIVATE, p_rkey, record.clone());
    h.pds.set_rev(ALICE, R2);
    let p = format!("at://{ALICE}/{NSID_POST_PRIVATE}/{p_rkey}");
    let index = index_server(&h).await;
    let (status, body) = post(&index, REQUEST_RECORD, &json!({ "uri": p }), ALICE).await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!((v["status"].as_str(), v["caughtUp"].as_bool()), (Some("indexed"), Some(true)), "{body}");
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R2));
    assert_eq!(h.flag(ALICE).await.rev.as_deref(), Some(R2));
    exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R2}' WHERE did = '{ALICE}'")).await;
    assert!(!h.reads(BOB, &p).await);
    assert!(!h.reads(BOB, &older).await);
    assert!(h.reads(BOB, &other).await, "O's other private posts read at once");
    h.fire(ALICE, R2, vec![RawOp { action: Action::Create, collection: NSID_POST_PRIVATE.into(), rkey: p_rkey.into(), cid: Some(format!("bafy{p_rkey}")), record: Some(record) }]).await;
    assert!(h.reads(BOB, &p).await);
    assert!(h.reads(BOB, &older).await);
    h.finish().await;
}

/// T-42, review 7 finding 1: `requestRecord.json`'s `caughtUp` description
/// carries the `following` sentence. The lexicons sit beside the crate; a
/// build tree with the crate alone skips this.
#[test]
fn t42_the_caught_up_lexicon_names_following_audiences() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../lexicons/app/nearhorizon/index/requestRecord.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        eprintln!("{} not found; skipping the lexicon text check", path.display());
        return;
    };
    let v: Value = serde_json::from_str(&text).expect("json");
    let description = v["defs"]["main"]["output"]["schema"]["properties"]["caughtUp"]["description"].as_str().expect("description");
    assert!(description.contains(
        "until the caller's repository moves again, except under the caller's `following` audiences: those are read again once the firehose has applied the caller's commit, usually within seconds."
    ), "{description}");
    assert!(description.contains("For an owner of a `following` audience, its follows count toward the same limit."), "{description}");
}

/// T-42, review 3 finding 3: a contradiction in catch-up clears the flag,
/// settles nothing, and queues `authz` a minute on; an `authz` already
/// queued is left as it was. Review 4 finding 3: a follow write in the pass
/// that fails deterministically leaves `ready_at` and owner readiness set,
/// O's other private posts read, and the catch-up ends unsettled; the same
/// for a rejected follow whose delete fails. A counter replaced during the
/// catch-up sets no flag.
#[tokio::test]
async fn t42_catch_up_contradictions_and_failures() {
    let Some(h) = H::new("s10-t42-fail", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t42x00").await;
    // A stored follow at R3 the repository at R2 lacks (a rollback).
    h.f.writer.apply(ALICE, R3, &[follow_op("3kt42xfolbob", BOB)], ApplyOptions { advance_applied_rev: false, ..firehose() }).await.expect("apply");
    h.pds.set_rev(ALICE, R2);
    set_follows_verified(h.pool(), ALICE, true).await;
    h.clear_jobs().await;
    assert!(!h.catch_up(ALICE, R2).await);
    assert!(!h.flag(ALICE).await.set);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(REV));
    let authz = h.job(ALICE, Depth::Authz).await.expect("authz");
    assert_eq!(authz.0, "queued");
    assert!((30..=90).contains(&authz.2), "{authz:?}");
    // With an `authz` already queued, due now: left as it was.
    exec(h.pool(), "UPDATE backfill_jobs SET not_before = now() - interval '1 hour'").await;
    let queued = h.job(ALICE, Depth::Authz).await;
    assert!(!h.catch_up(ALICE, R2).await);
    assert_eq!(h.job(ALICE, Depth::Authz).await.map(|j| (j.0, j.4)), queued.map(|j| (j.0, j.4)));
    exec(h.pool(), &format!("DELETE FROM follows WHERE author_did = '{ALICE}'")).await;

    // A follow write that fails: readiness stays, unsettled.
    let failing = follow_uri(ALICE, "3kt42xfolcar");
    let hook = hooks::arm(hooks::FOLLOW_WRITE_FAILS, &failing);
    h.pds.follow(ALICE, "3kt42xfolcar", CAROL);
    assert!(!h.catch_up(ALICE, R2).await);
    assert!(hook.hits() > 0);
    drop(hook);
    assert!(h.member_ready(ALICE).await && h.owner_ready(ALICE).await);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(REV));
    // Written at R2: the catch-ups listed O's repository at R2, so an op
    // below it is history (replay-readiness design §4.3).
    let list = format!("at://{ALICE}/{NSID_AUDIENCE}/3kaudt42x01");
    let audience = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [BOB], "createdAt": "2026-09-26T00:00:00Z"});
    h.f.apply_op(ALICE, Action::Create, NSID_AUDIENCE, "3kaudt42x01", R2, Some(audience.clone())).await;
    h.pds.put_cid(ALICE, NSID_AUDIENCE, "3kaudt42x01", &format!("bafy3kaudt42x01{R2}"), audience);
    let record = h.f.post_record(ALICE, "3kpstt42x01", &list, None).await;
    h.f.apply_op(ALICE, Action::Create, NSID_POST_PRIVATE, "3kpstt42x01", R2, Some(record.clone())).await;
    h.pds.put_cid(ALICE, NSID_POST_PRIVATE, "3kpstt42x01", &format!("bafy3kpstt42x01{R2}"), record);
    let other = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kpstt42x01");
    assert!(h.served(BOB, &other).await);
    assert!(!h.served(BOB, &p).await);

    // Eve's follow of Bob, edited to Walt in her repository: rejected, and
    // its delete fails.
    let (_, _) = h.eve("t42x10").await;
    h.fire(EVE, R1, vec![follow_op("3kt42xevefol", BOB)]).await;
    h.pds.follow(EVE, "3kt42xevefol", WALT);
    h.pds.set_rev(EVE, R2);
    let failing = follow_uri(EVE, "3kt42xevefol");
    let hook = hooks::arm(hooks::FOLLOW_WRITE_FAILS, &failing);
    assert!(!h.catch_up(EVE, R2).await);
    assert!(hook.hits() > 0);
    drop(hook);
    assert!(h.owner_ready(EVE).await, "readiness stays");
    assert_eq!(h.applied(EVE).await.as_deref(), Some(R1));
    assert!(!h.flag(EVE).await.set, "a contradiction");

    // A counter replaced during the catch-up: no flag.
    h.pds.remove(ALICE, FOLLOW, "3kt42xfolcar");
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    let catch = {
        let (pds, writer) = (Arc::clone(&h.pds), h.f.writer.clone());
        tokio::spawn(async move { catch_up(pds.as_ref(), &writer, &CatchUp::new(H::catch_up_cfg()), ALICE, PDS, R2).await })
    };
    hook.reached().await;
    {
        let mut conn = h.pool().acquire().await.expect("conn");
        clear_follows_verified(&mut conn, ALICE).await.expect("clear");
    }
    hook.release();
    assert!(catch.await.expect("task"), "it settles");
    assert!(!h.flag(ALICE).await.set);
    h.finish().await;
}

// ---- T-43: the writer ----

/// T-43: the follow tombstone (§4.1): non-member O outside S, the
/// firehose's delete of follow X finds no row and still tombstones it; a
/// job's later write of listed X at an older rev is refused, and V does not
/// read. A rejected follow update (§4.2), in Off and in On: the row naming
/// V goes, a tombstone at U, V misses, the outcome is `Rejected`. The same
/// edit by a member O is admitted: the row names W.
async fn t43_tombstones(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let (_, p) = h.eve("t43t00").await;
    let b = h.backfill();
    // The tombstone with no row.
    let x = follow_uri(EVE, "3kt43tfolbob");
    h.fire(EVE, R2, vec![unfollow_op("3kt43tfolbob")]).await;
    assert_eq!(h.tombstone(&x).await.as_deref(), Some(R2));
    h.pds.follow(EVE, "3kt43tfolbob", BOB);
    h.pds.set_rev(EVE, R1);
    h.clear_jobs().await;
    assert_eq!(h.run(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(h.stored(&x).await, None, "the older write is refused");
    set_follows_verified(h.pool(), EVE, true).await;
    assert!(!h.reads(BOB, &p).await);

    // A rejected update.
    let y = follow_uri(EVE, "3kt43tfolby2");
    h.fire(EVE, R3, vec![follow_op("3kt43tfolby2", BOB)]).await;
    set_follows_verified(h.pool(), EVE, true).await;
    assert!(h.reads(BOB, &p).await);
    let report = h.fire(EVE, R4, vec![edit_op("3kt43tfolby2", WALT)]).await;
    assert!(matches!(report.outcomes.first(), Some(OpOutcome::Rejected(_))), "{report:?}");
    assert_eq!(h.stored(&y).await, None);
    assert_eq!(h.tombstone(&y).await.as_deref(), Some(R4));
    assert!(!h.reads(BOB, &p).await);

    // A member O's edit is admitted.
    h.fire(ALICE, R1, vec![follow_op("3kt43tfolali", BOB)]).await;
    h.fire(ALICE, R2, vec![edit_op("3kt43tfolali", WALT)]).await;
    assert_eq!(h.stored(&follow_uri(ALICE, "3kt43tfolali")).await.map(|s| s.0), Some(WALT.to_string()));
    h.finish().await;
}

#[tokio::test]
async fn t43_follow_tombstones_on() {
    t43_tombstones("s10-t43-tomb-on", PublicMode::On).await;
}

#[tokio::test]
async fn t43_follow_tombstones_off() {
    t43_tombstones("s10-t43-tomb-off", PublicMode::Off).await;
}

/// T-43, review 1 finding 3, for a job and a catch-up: the run lists X
/// naming V at R (X not stored, so the run writes it); the firehose then
/// applies O's rejected edit of X at U > R; the run then writes X at R,
/// which the tombstone at U refuses, so no row names V and V does not read.
/// O then edits X back to V at U2 > U: stored, and V reads. (A `resync`
/// of a non-member lists no follows, its plan being the scope plan, so it
/// has no such write.)
#[tokio::test]
async fn t43_a_listing_older_than_a_rejected_edit_is_refused() {
    let Some(h) = H::new("s10-t43-older", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.eve("t43o00").await;
    let b = h.backfill();
    let mut revs = [(R1, R2, R3, R4), (R4, R5, R6, "3khhhhhhhhhh2")].into_iter();
    for via_job in [true, false] {
        let (r, u, u2, gone) = revs.next().expect("revs");
        let rkey = format!("3kt43ofol{}", u8::from(via_job));
        let x = follow_uri(EVE, &rkey);
        h.pds.follow(EVE, &rkey, BOB);
        h.pds.set_rev(EVE, r);
        // Behind R, as a catch-up needs.
        exec(h.pool(), &format!("UPDATE sync_state SET applied_rev = '{REV}' WHERE did = '{EVE}'")).await;
        h.clear_jobs().await;
        let hook = hooks::arm(hooks::PASS_AFTER_LISTING, &h.key(EVE));
        let run = if via_job {
            h.enqueue(EVE, Depth::Authz).await;
            let run = h.spawn_run(&b, EVE, Depth::Authz);
            tokio::spawn(async move {
                run.await.expect("task").expect("db").expect("job");
            })
        } else {
            let (pds, writer) = (Arc::clone(&h.pds), h.f.writer.clone());
            tokio::spawn(async move {
                catch_up(pds.as_ref(), &writer, &CatchUp::new(H::catch_up_cfg()), EVE, PDS, r).await;
            })
        };
        hook.reached().await;
        // The firehose applies the rejected edit at U, before the run's
        // write at R.
        let report = h.fire(EVE, u, vec![edit_op(&rkey, WALT)]).await;
        assert!(matches!(report.outcomes.first(), Some(OpOutcome::Rejected(_))), "{report:?}");
        assert_eq!(h.tombstone(&x).await.as_deref(), Some(u));
        hook.release();
        run.await.expect("task");
        drop(hook);
        assert_eq!(h.stored(&x).await, None, "via_job {via_job}: the write at R is refused");
        set_follows_verified(h.pool(), EVE, true).await;
        assert!(!h.reads(BOB, &p).await, "via_job {via_job}");
        // Back to V at U2.
        h.fire(EVE, u2, vec![edit_op(&rkey, BOB)]).await;
        assert_eq!(h.stored(&x).await.map(|s| s.0), Some(BOB.to_string()));
        set_follows_verified(h.pool(), EVE, true).await;
        assert!(h.reads(BOB, &p).await, "via_job {via_job}");
        h.fire(EVE, gone, vec![unfollow_op(&rkey)]).await;
        h.pds.remove(EVE, FOLLOW, &rkey);
    }
    h.finish().await;
}

/// T-43, review 3 finding 1: non-member O, the firehose held back. Listing
/// A of O at R1 sees X naming member V; O edits X to name non-member W at
/// U; listing B at R2 ≥ U sees X naming W, whose write is rejected,
/// deleting the row and tombstoning X at R2; B settles the flag. A then
/// writes X→V at R1: refused, no row names V, and V does not read before or
/// after the firehose applies U. Once with A a `private` job and B an
/// `authz`, once with B a catch-up; with X stored before U, and never.
#[tokio::test]
async fn t43_two_listings_a_newer_rejection_wins() {
    let Some(h) = H::new("s10-t43-two", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.eve("t43w00").await;
    let b = h.backfill();
    let mut case = 0;
    for b_is_catch_up in [false, true] {
        for stored_before in [true, false] {
            case += 1;
            let rkey = format!("3kt43wfol{case:03}");
            let x = follow_uri(EVE, &rkey);
            if stored_before {
                h.fire(EVE, R1, vec![follow_op(&rkey, BOB)]).await;
            }
            h.pds.follow(EVE, &rkey, BOB);
            h.pds.set_rev(EVE, R1);
            exec(h.pool(), &format!("UPDATE sync_state SET applied_rev = '{REV}' WHERE did = '{EVE}'")).await;
            h.clear_jobs().await;
            // A lists at R1 and holds.
            let (listed, gate) = h.pds.hold_follows(EVE);
            h.enqueue(EVE, Depth::Private).await;
            let a = h.spawn_run(&b, EVE, Depth::Private);
            listed.notified().await;
            // O edits X to W at U = R2; B lists at R2.
            h.pds.follow(EVE, &rkey, WALT);
            h.pds.set_rev(EVE, R2);
            if b_is_catch_up {
                assert!(h.catch_up(EVE, R2).await, "case {case}");
            } else {
                h.enqueue(EVE, Depth::Authz).await;
                assert_eq!(h.rerun(&b, EVE, Depth::Authz).await, JobOutcome::Ready, "case {case}");
            }
            assert_eq!(h.tombstone(&x).await.as_deref(), Some(R2), "case {case}");
            assert_eq!(h.stored(&x).await, None, "case {case}");
            assert!(h.flag(EVE).await.set, "case {case}");
            gate.add_permits(1);
            a.await.expect("task").expect("db").expect("job");
            assert_eq!(h.stored(&x).await, None, "case {case}: A's older write refused");
            h.commit_nothing(EVE, R2).await;
            set_follows_verified(h.pool(), EVE, true).await;
            assert!(!h.reads(BOB, &p).await, "case {case}");
            h.fire(EVE, R2, vec![edit_op(&rkey, WALT)]).await;
            assert_eq!(h.stored(&x).await, None, "case {case}");
            assert!(!h.reads(BOB, &p).await, "case {case}");
            h.pds.remove(EVE, FOLLOW, &rkey);
        }
    }
    h.finish().await;
}

/// T-43, review 2 finding 7 with review 3's cost rule, in Off: non-member O
/// with 100 follows of non-members. A pass writes none of them, and W's
/// reply under O's `following` audience is read by W alone. The first pass
/// writes 100 tombstones; a second at the same R writes none (rows and
/// `deleted_at` unchanged); a pass after O's next commit moves each to the
/// new R.
#[tokio::test]
async fn t43_rejected_follows_tombstone_forward_only() {
    let Some(h) = H::new("s10-t43-cost", PublicMode::Off).await else {
        return;
    };
    let (aud, p) = h.eve("t43c00").await;
    for i in 0..100 {
        h.pds.follow(EVE, &format!("3kt43c{i:06}"), &format!("did:plc:c{i:023}"));
    }
    h.pds.follow(EVE, "3kt43cfolwlt", WALT);
    h.pds.set_rev(EVE, R1);
    let reply_rkey = "3kt43cwltrep";
    let record = h.f.post_record(WALT, reply_rkey, &aud, Some((&p, &p))).await;
    let reply = h.direct(WALT, NSID_POST_PRIVATE, reply_rkey, record).await;
    h.clear_jobs().await;
    let b = h.backfill();
    let tombstones = || async {
        let rows: Vec<(String, String, DateTime<Utc>)> =
            sqlx::query_as(&format!("SELECT uri, rev, deleted_at FROM record_tombstones WHERE uri LIKE 'at://{EVE}/{FOLLOW}/%' ORDER BY uri"))
                .fetch_all(h.pool())
                .await
                .expect("tombstones");
        rows
    };
    assert_eq!(h.run(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    let first = tombstones().await;
    assert_eq!(first.len(), 101);
    assert!(first.iter().all(|(_, rev, _)| rev == R1));
    let stored: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM follows WHERE author_did = '{EVE}'")).fetch_one(h.pool()).await.expect("count");
    assert_eq!(stored, 0);
    h.commit_nothing(EVE, R1).await;
    assert!(h.flag(EVE).await.holds);
    assert!(!h.reads(BOB, &reply).await, "W's reply is read by W alone");
    assert_eq!(h.run(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(tombstones().await, first, "the same R writes none");
    h.pds.set_rev(EVE, R2);
    assert_eq!(h.run(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    let moved = tombstones().await;
    assert_eq!(moved.len(), 101);
    assert!(moved.iter().all(|(_, rev, _)| rev == R2));
    h.finish().await;
}

/// T-43, review 3 finding 2: X at U rejected because W is not a member; W
/// becomes one; a pass at R = U leaves no row (the outcome is
/// `Unchanged`). After O's next commit, the next pass stores X, which
/// clears the flag (§4.4), and after that pass's re-run W is admitted under
/// condition 2.
#[tokio::test]
async fn t43_a_tombstone_holds_until_the_next_commit() {
    let Some(h) = H::new("s10-t43-member", PublicMode::On).await else {
        return;
    };
    let (aud, p) = h.eve("t43m00").await;
    // Eve follows Bob, the reader.
    h.commit_follow(EVE, "3kt43mfolbob", BOB, REV2).await;
    let x = follow_uri(EVE, "3kt43mfolwlt");
    h.fire(EVE, R1, vec![follow_op("3kt43mfolwlt", WALT)]).await;
    assert_eq!(h.tombstone(&x).await.as_deref(), Some(R1));
    h.pds.follow(EVE, "3kt43mfolwlt", WALT);
    h.pds.set_rev(EVE, R1);
    // Walt becomes a member.
    actor(h.pool(), WALT, "walt.test", "walt.test").await;
    exec(h.pool(), &format!("INSERT INTO members (did, source, ready_at) VALUES ('{WALT}', 'admin', now())")).await;
    exec(h.pool(), &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{WALT}', 1)")).await;
    exec(h.pool(), &format!("INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at, firehose_rev) VALUES ('{WALT}', '{REV}', '{REV}', now(), '{REV}')")).await;
    {
        let mut sets = h.f.writer.scope().write_guard().await;
        sets.members.insert(WALT.into());
        sets.scope.insert(WALT.into());
    }
    let reply = h.post(WALT, "3kt43mwltrep", &aud, Some((&p, &p))).await;
    h.f.ready(WALT).await;
    h.clear_jobs().await;
    let b = h.backfill();
    assert_eq!(h.run(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(h.stored(&x).await, None, "the tombstone at U refuses it");
    h.commit_nothing(EVE, R1).await;
    assert!(h.flag(EVE).await.holds);
    assert!(!h.reads(BOB, &reply).await);
    // Eve's next commit.
    h.pds.set_rev(EVE, R2);
    assert_eq!(h.run(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(h.stored(&x).await.map(|s| s.0), Some(WALT.to_string()));
    assert!(!h.flag(EVE).await.set);
    assert_eq!(h.rerun(&b, EVE, Depth::Authz).await, JobOutcome::Ready);
    h.commit_nothing(EVE, R2).await;
    assert!(h.flag(EVE).await.holds);
    assert!(h.reads(BOB, &reply).await);
    h.finish().await;
}

/// T-43, review 3 finding 7: a rejected tracked follow whose delete fails on
/// every attempt is `Failed`, its savepoint rolled back, and on the
/// firehose O's readiness and flag are cleared.
#[tokio::test]
async fn t43_a_failing_rejected_follow_fails_closed() {
    let Some(h) = H::new("s10-t43-failing", PublicMode::On).await else {
        return;
    };
    h.eve("t43f00").await;
    let x = follow_uri(EVE, "3kt43ffolbob");
    h.fire(EVE, R1, vec![follow_op("3kt43ffolbob", BOB)]).await;
    set_follows_verified(h.pool(), EVE, true).await;
    let hook = hooks::arm(hooks::FOLLOW_WRITE_FAILS, &x);
    let report = h.fire(EVE, R2, vec![edit_op("3kt43ffolbob", WALT)]).await;
    drop(hook);
    assert!(matches!(report.outcomes.first(), Some(OpOutcome::Failed(_))), "{report:?}");
    assert!(report.readiness_cleared);
    assert_eq!(h.stored(&x).await.map(|s| s.0), Some(BOB.to_string()), "rolled back");
    assert_eq!(h.tombstone(&x).await, None);
    assert!(!h.owner_ready(EVE).await);
    assert!(!h.flag(EVE).await.set);
    h.finish().await;
}

/// How listing A writes in [`older_listing`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListingA {
    /// A `resync` of member O.
    Resync,
    /// A `member` plan's `Top(10_000)` write.
    MemberPlan,
}

/// T-43, review 4 finding 1: X→V is not stored, and O deletes X at D, held
/// back from the firehose. A lists X at R1; B (an `authz`) lists at
/// R2 ≥ D. (i) A writes before B settles: B's settle writes NULL, an
/// `authz` follows a minute on, and its pass deletes X and sets the flag.
/// (ii) A writes after B settles: A's write leaves the flag NULL and an
/// `authz` queued, and that pass deletes X and sets the flag. V reads at no
/// point while X is stored. `twin`, the update twin: X is stored naming
/// Carol; at R1 it names V, and by D it names Carol again, so A writes a
/// subject B never saw.
async fn older_listing(test: &str, a: ListingA, a_first: bool, twin: bool) {
    let Some(h) = H::new(test, PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t43a00").await;
    let rkey = "3kt43afolbob";
    let x = follow_uri(ALICE, rkey);
    if twin {
        h.commit_follow(ALICE, rkey, CAROL, REV2).await;
    }
    h.commit_nothing(ALICE, R1).await;
    h.pds.follow(ALICE, rkey, BOB);
    h.clear_jobs().await;
    let b = h.backfill();
    let a_depth = match a {
        ListingA::Resync => Depth::Resync,
        ListingA::MemberPlan => Depth::Member,
    };
    let (listed, gate) = h.pds.hold_follows(ALICE);
    h.enqueue(ALICE, a_depth).await;
    let run_a = h.spawn_run(&b, ALICE, a_depth);
    listed.notified().await;
    // At D = R2, held back from the firehose: X deleted (or, for the twin,
    // naming Carol again).
    if twin {
        h.pds.follow(ALICE, rkey, CAROL);
    } else {
        h.pds.remove(ALICE, FOLLOW, rkey);
    }
    h.pds.set_rev(ALICE, R2);
    let never_reads = |h: &H| {
        let (h_base, pool, x, p) = (h.base.clone(), h.pool().clone(), x.clone(), p.clone());
        async move {
            let stored: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM follows WHERE uri = $1 AND subject_did = $2)").bind(&x).bind(BOB).fetch_one(&pool).await.expect("stored");
            if stored {
                exec(&pool, "UPDATE sync_state SET head_checked_at = now()").await;
                let (_, body, _) = get(&h_base, POSTS, &format!("uris={p}"), BOB, BOB_KEY).await;
                assert_eq!(body, r#"{"posts":[]}"#, "V read while X was stored");
            }
        }
    };
    if a_first {
        // (i) A writes, then B settles.
        let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
        h.enqueue(ALICE, Depth::Authz).await;
        let run_b = h.spawn_run(&b, ALICE, Depth::Authz);
        hook.reached().await;
        gate.add_permits(1);
        hook.reached_times(2).await;
        never_reads(&h).await;
        hook.release();
        run_a.await.expect("task").expect("db").expect("job");
        assert_eq!(run_b.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
        drop(hook);
        assert!(!h.flag(ALICE).await.set, "B's settle writes NULL");
        let authz = h.job(ALICE, Depth::Authz).await.expect("authz");
        assert_eq!(authz.0, "queued");
        assert!((30..=90).contains(&authz.2), "{authz:?}");
    } else {
        // (ii) B settles, then A writes.
        h.enqueue(ALICE, Depth::Authz).await;
        assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
        assert!(h.flag(ALICE).await.set);
        gate.add_permits(1);
        run_a.await.expect("task").expect("db").expect("job");
        never_reads(&h).await;
        assert!(!h.flag(ALICE).await.set, "A's write clears the flag");
        assert!(h.authz_queued(ALICE).await);
    }
    never_reads(&h).await;
    // The follow-up `authz` runs (the twin's first re-run writes Carol
    // again, which clears once more).
    for _ in 0..3 {
        if !h.authz_queued(ALICE).await {
            break;
        }
        assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
        never_reads(&h).await;
    }
    assert!(h.stored(&x).await.is_none_or(|s| s.0 != BOB), "X naming V is gone");
    assert!(h.flag(ALICE).await.set);
    h.commit_nothing(ALICE, R3).await;
    never_reads(&h).await;
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

#[tokio::test]
async fn t43_an_older_resync_before_the_settle() {
    older_listing("s10-t43-older-resync-i", ListingA::Resync, true, false).await;
}

#[tokio::test]
async fn t43_an_older_resync_after_the_settle() {
    older_listing("s10-t43-older-resync-ii", ListingA::Resync, false, false).await;
}

#[tokio::test]
async fn t43_an_older_member_plan_before_the_settle() {
    older_listing("s10-t43-older-member-i", ListingA::MemberPlan, true, false).await;
}

#[tokio::test]
async fn t43_an_older_member_plan_after_the_settle() {
    older_listing("s10-t43-older-member-ii", ListingA::MemberPlan, false, false).await;
}

#[tokio::test]
async fn t43_an_older_subject_change_before_the_settle() {
    older_listing("s10-t43-older-twin-i", ListingA::Resync, true, true).await;
}

#[tokio::test]
async fn t43_an_older_subject_change_after_the_settle() {
    older_listing("s10-t43-older-twin-ii", ListingA::Resync, false, true).await;
}

/// T-43: a `member` job of a member that owns no `following` audience
/// inserts 100 follows: the flag stays NULL and no `authz` is queued.
/// Review 5 finding 2: a `member` job of a member following owner rewrites
/// its stored follows at a newer R with the same subjects: each row's rev
/// moves, the flag is not cleared and no `authz` is queued; one whose
/// subject changed clears it once.
#[tokio::test]
async fn t43_only_a_row_that_appears_or_changes_clears() {
    let Some(h) = H::new("s10-t43-rewrite", PublicMode::On).await else {
        return;
    };
    let b = h.backfill();
    for i in 0..100 {
        h.pds.follow(BOB, &format!("3kt43rb{i:05}"), &format!("did:plc:r{i:023}"));
    }
    h.pds.set_rev(BOB, R1);
    h.clear_jobs().await;
    assert_eq!(h.run(&b, BOB, Depth::Member).await, JobOutcome::Ready);
    let stored: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM follows WHERE author_did = '{BOB}'")).fetch_one(h.pool()).await.expect("count");
    assert_eq!(stored, 100);
    assert!(!h.flag(BOB).await.set);
    assert!(h.job(BOB, Depth::Authz).await.is_none());

    // Alice, a following owner, verified, with 50 follows.
    h.alice_following("t43r00").await;
    let ops: Vec<RawOp> = (0..50).map(|i| follow_op(&format!("3kt43ra{i:05}"), &format!("did:plc:r{i:023}"))).collect();
    for op in &ops {
        h.pds.follow(ALICE, &op.rkey, op.record.as_ref().and_then(|r| r["subject"].as_str()).expect("subject"));
    }
    h.pds.set_rev(ALICE, R1);
    h.fire(ALICE, R1, ops).await;
    set_follows_verified(h.pool(), ALICE, true).await;
    let before = h.flag(ALICE).await;
    h.pds.set_rev(ALICE, R2);
    h.clear_jobs().await;
    assert_eq!(h.run(&b, ALICE, Depth::Member).await, JobOutcome::Ready);
    let revs: Vec<String> = sqlx::query_scalar(&format!("SELECT DISTINCT rev FROM follows WHERE author_did = '{ALICE}'")).fetch_all(h.pool()).await.expect("revs");
    assert_eq!(revs, [R2]);
    let after = h.flag(ALICE).await;
    assert!(after.set);
    assert_eq!(after.clears, before.clears, "not cleared");
    assert!(!h.authz_queued(ALICE).await);
    // One changed subject.
    h.pds.follow(ALICE, "3kt43ra00000", CAROL);
    h.pds.set_rev(ALICE, R3);
    assert_eq!(h.run(&b, ALICE, Depth::Resync).await, JobOutcome::Done);
    let changed = h.flag(ALICE).await;
    assert!(!changed.set);
    assert_ne!(changed.clears, after.clears);
    assert!(h.authz_queued(ALICE).await);
    h.finish().await;
}

/// T-43, review 4 finding 4: a pass whose write of a changed subject for a
/// stored row fails (forced), the flag set by an earlier pass and the
/// settle going through `ceiling_hit`: the flag is NULL after the run. The
/// same for a failed reconcile delete of a gone `rev < R` row.
#[tokio::test]
async fn t43_a_failed_fix_of_a_stale_row_clears() {
    let Some(h) = H::with_pds("s10-t43-failfix", PublicMode::On, Pds::new(1, false)).await else {
        return;
    };
    h.alice_following("t43x00").await;
    h.audience(ALICE, "3kaudt43x01", "list").await;
    h.commit_follow(ALICE, "3kt43xfolbob", BOB, R1).await;
    h.commit_follow(ALICE, "3kt43xfolcar", CAROL, R1).await;
    let b = h.backfill_with(BackfillConfig { full_listing_ceiling: 1, ..BackfillConfig::default() });
    for (case, uri) in [("changed subject", follow_uri(ALICE, "3kt43xfolbob")), ("reconcile delete", follow_uri(ALICE, "3kt43xfolcar"))] {
        if case == "changed subject" {
            h.pds.follow(ALICE, "3kt43xfolbob", DAVE);
        } else {
            h.pds.remove(ALICE, FOLLOW, "3kt43xfolcar");
        }
        h.pds.set_rev(ALICE, R2);
        set_follows_verified(h.pool(), ALICE, true).await;
        h.clear_jobs().await;
        let hook = hooks::arm(hooks::FOLLOW_WRITE_FAILS, &uri);
        assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Done, "{case}");
        assert!(hook.hits() > 0, "{case}");
        drop(hook);
        assert!(!h.flag(ALICE).await.set, "{case}");
        assert!(h.stored(&uri).await.is_some(), "{case}: the stale row stays");
    }
    h.finish().await;
}

/// T-43, review 5 finding 1: the replay behind a settle. The consumer is
/// held back; O follows V at C1 and unfollows at D. B lists at R ≥ D, finds
/// no X, completes and settles: the flag is set with
/// `follows_verified_rev = R` and does not hold; no `authz` is queued. The
/// consumer replays C1, X is stored, V does not read; it applies D, X goes,
/// the flag holds. V reads at no point. Variants: an edit replayed the same
/// way, and a catch-up in B's place.
#[tokio::test]
async fn t43_a_replay_behind_a_settle_reads_nothing() {
    let Some(h) = H::new("s10-t43-replay", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t43p00").await;
    let list = h.audience_of(ALICE, "3kaudt43p01", "list", &[BOB]).await;
    let other = h.post(ALICE, "3kpstt43p01", &list, None).await;
    let b = h.backfill();
    let mut revs = [R1, R2, R3, R4, R5, R6].into_iter();
    for (case, catch) in [("create", false), ("edit", false), ("catch-up", true)] {
        let rkey = format!("3kt43p{}", &case[..4]);
        // X made at C1 and gone at D, both held back from the firehose; B
        // lists at R = D.
        let (c1, d) = (revs.next().expect("rev"), revs.next().expect("rev"));
        h.pds.set_rev(ALICE, d);
        h.clear_jobs().await;
        if catch {
            assert!(h.catch_up(ALICE, d).await, "{case}: caughtUp");
            assert!(h.reads(BOB, &other).await, "{case}: O's other private posts read at once");
        } else {
            assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready, "{case}");
        }
        let flag = h.flag(ALICE).await;
        assert!(flag.set && !flag.holds && flag.rev.as_deref() == Some(d), "{case}: {flag:?}");
        assert!(!h.authz_queued(ALICE).await, "{case}");
        // The held-back consumer replays C1, then D.
        let made = if case == "edit" { edit_op(&rkey, BOB) } else { follow_op(&rkey, BOB) };
        h.fire(ALICE, c1, vec![made]).await;
        assert!(h.stored(&follow_uri(ALICE, &rkey)).await.is_some(), "{case}");
        assert!(!h.reads(BOB, &p).await, "{case}");
        h.fire(ALICE, d, vec![unfollow_op(&rkey)]).await;
        assert!(h.flag(ALICE).await.holds, "{case}");
        assert!(!h.reads(BOB, &p).await, "{case}");
    }
    h.finish().await;
}

/// T-43, review 6 finding 1: a higher `applied_rev`. O's flag is NULL and
/// the consumer is held back at fr. B lists at R_B ≤ fr; O follows V with
/// Y at C_y > fr and unfollows at D_y; P lists at R_P ≥ D_y and its settle
/// writes NULL with `applied_rev = R_P`; B's pass completes and sets the
/// flag with `follows_verified_rev = R_P`; the consumer replays C_y. V does
/// not read until the consumer has applied D_y. The same with a
/// non-following catch-up in P's place.
#[tokio::test]
async fn t43_the_verified_rev_is_the_applied_rev_left_behind() {
    let Some(h) = H::with_pds("s10-t43-higher", PublicMode::On, Pds::new(1, false)).await else {
        return;
    };
    let (_, p) = h.alice_following("t43h00").await;
    h.commit_follow(ALICE, "3kt43hfolcar", CAROL, R1).await;
    h.commit_follow(ALICE, "3kt43hfoldav", DAVE, R1).await;
    let b = h.backfill();
    // P's run ends its pass over a ceiling, so its settle writes NULL.
    let p_runs = h.backfill_with(BackfillConfig { follows_pass_ceiling: 1, ..BackfillConfig::default() });
    for (i, p_is_catch_up) in [false, true].into_iter().enumerate() {
        let rkey = format!("3kt43hfoly{i:02}");
        let (fr, c_y, d_y) = if i == 0 { (R1, R2, R3) } else { (R3, R4, R5) };
        let r_p = d_y;
        h.commit_nothing(ALICE, fr).await;
        set_follows_verified(h.pool(), ALICE, false).await;
        h.clear_jobs().await;
        // B lists at R_B = fr, and its follows listing holds.
        h.pds.set_rev(ALICE, fr);
        let (listed, gate) = h.pds.hold_follows(ALICE);
        h.enqueue(ALICE, Depth::Authz).await;
        let run_b = h.spawn_run(&b, ALICE, Depth::Authz);
        listed.notified().await;
        // Y made at C_y and deleted at D_y, both held back; P lists at R_P
        // and settles NULL with `applied_rev = R_P`.
        h.pds.set_rev(ALICE, r_p);
        if p_is_catch_up {
            // Not a following owner at its decision: it settles `None`.
            exec(h.pool(), "UPDATE private_audiences SET mode = 'list'").await;
            assert!(h.catch_up(ALICE, r_p).await);
            exec(h.pool(), "UPDATE private_audiences SET mode = 'following'").await;
        } else {
            assert_eq!(h.run(&p_runs, ALICE, Depth::Private).await, JobOutcome::Ready);
        }
        assert_eq!(h.applied(ALICE).await.as_deref(), Some(r_p));
        assert!(!h.flag(ALICE).await.set);
        // B's pass completes, and its flag waits for R_P.
        gate.add_permits(1);
        assert_eq!(run_b.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
        let flag = h.flag(ALICE).await;
        assert!(flag.set && flag.rev.as_deref() == Some(r_p), "{flag:?}");
        // The consumer replays C_y: Y is stored, and V does not read.
        h.fire(ALICE, c_y, vec![follow_op(&rkey, BOB)]).await;
        assert!(h.stored(&follow_uri(ALICE, &rkey)).await.is_some());
        assert!(!h.flag(ALICE).await.holds);
        assert!(!h.reads(BOB, &p).await);
        // Once D_y is applied, the flag holds, and Y is gone.
        h.fire(ALICE, d_y, vec![unfollow_op(&rkey)]).await;
        assert!(h.flag(ALICE).await.holds);
        assert!(!h.reads(BOB, &p).await);
        assert!(h.reads(CAROL, &p).await);
    }
    h.finish().await;
}

/// T-43, review 6 finding 2: a dormant owner, `firehose_rev` NULL after
/// migration 0028 and no further commit: one `authz` sets the flag and
/// nothing is re-queued for it; its `following` audiences are read by O
/// alone. Once O's next commit is applied, they are read under rule 7 with
/// no job in between.
#[tokio::test]
async fn t43_a_dormant_owner_opens_at_its_next_commit() {
    let Some(h) = H::new("s10-t43-dormant", PublicMode::On).await else {
        return;
    };
    let (_, p) = h.alice_following("t43d00").await;
    h.commit_follow(ALICE, "3kt43dfolbob", BOB, R1).await;
    exec(h.pool(), &format!("UPDATE sync_state SET firehose_rev = NULL WHERE did = '{ALICE}'")).await;
    h.clear_jobs().await;
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Authz).await, JobOutcome::Ready);
    let flag = h.flag(ALICE).await;
    assert!(flag.set && !flag.holds && flag.firehose.is_none(), "{flag:?}");
    assert_eq!(h.job(ALICE, Depth::Authz).await.expect("authz").0, "done");
    assert!(!h.reads(BOB, &p).await);
    assert!(h.reads(ALICE, &p).await);
    h.commit_nothing(ALICE, R2).await;
    assert!(h.reads(BOB, &p).await);
    h.finish().await;
}

/// T-43, review 5 finding 3: a `resync`'s follow insert of O (enqueue, then
/// clear) and O's `authz` settle, interleaved after a gap: both commit, in
/// either order, with no `40P01`. Review 4 finding 5: a `member` settle's
/// re-check enqueue and a writer transaction that enqueues `authz` and then
/// fails an op: both commit, in either order, with no `40P01`.
#[tokio::test]
async fn t43_s10_lock_edges_close_no_cycle() {
    let Some(h) = H::new("s10-t43-locks", PublicMode::On).await else {
        return;
    };
    h.alice_following("t43l00").await;
    let b = h.backfill();
    // A `resync`'s write: a follow its listing saw, which the `authz`
    // run's own listing does not hold.
    let resync = ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: false, subscription_rules: true, source: WriteSource::Backfill };
    h.gap().await.expect("gap");
    for settle_first in [true, false] {
        let rkey = if settle_first { "3kt43lfolres" } else { "3kt43lfolre2" };
        h.pds.set_rev(ALICE, if settle_first { R1 } else { R2 });
        h.clear_jobs().await;
        h.enqueue(ALICE, Depth::Authz).await;
        if settle_first {
            // The settle holds its own row, `jobs(O, authz)`; the write's
            // enqueue waits on it, before its clear of `sync_state(O)`.
            let hook = hooks::arm(hooks::SETTLE_AFTER_FOLLOW_UP, &h.key(ALICE));
            let settle = h.spawn_run(&b, ALICE, Depth::Authz);
            hook.reached().await;
            let write = {
                let (writer, op) = (h.f.writer.clone(), follow_op(rkey, CAROL));
                tokio::spawn(async move { writer.apply(ALICE, R1, &[op], resync).await })
            };
            until_waiting(h.pool(), "INSERT INTO backfill_jobs").await;
            hook.release();
            assert_eq!(settle.await.expect("task").expect("db: no 40P01").expect("job").1, JobOutcome::Ready);
            write.await.expect("task").expect("write: no 40P01");
        } else {
            h.f.writer.apply(ALICE, R2, &[follow_op(rkey, CAROL)], resync).await.expect("write");
            assert_eq!(h.rerun(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
        }
        assert!(h.stored(&follow_uri(ALICE, rkey)).await.is_some());
    }

    // Review 4 finding 5: the `member` settle's re-check enqueue, after its
    // own row, and a writer that enqueues `authz` (§4.4) and fails an op.
    for settle_first in [true, false] {
        exec(h.pool(), "UPDATE private_audiences SET mode = 'list'").await;
        h.clear_jobs().await;
        h.ready_again().await;
        let decided = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
        h.enqueue(ALICE, Depth::Member).await;
        let settle = h.spawn_run(&b, ALICE, Depth::Member);
        decided.reached().await;
        exec(h.pool(), "UPDATE private_audiences SET mode = 'following'").await;
        let held = hooks::arm(hooks::SETTLE_AFTER_FOLLOW_UP, &h.key(ALICE));
        let rkey = if settle_first { "3kt43lfolm01" } else { "3kt43lfolm02" };
        let ops = vec![follow_op(rkey, DAVE), follow_op("3kt43lbad0001", "not a did")];
        let options = ApplyOptions { advance_applied_rev: false, dead_letter_readiness: true, attempts: 1, live: false, subscription_rules: false, source: WriteSource::CatchUp };
        if settle_first {
            decided.release();
            held.reached().await;
            let write = {
                let writer = h.f.writer.clone();
                tokio::spawn(async move { writer.apply(ALICE, R3, &ops, options).await })
            };
            until_waiting(h.pool(), "INSERT INTO backfill_jobs").await;
            held.release();
            settle.await.expect("task").expect("db (no 40P01)").expect("job");
            write.await.expect("task").expect("write (no 40P01)");
        } else {
            h.f.writer.apply(ALICE, R3, &ops, options).await.expect("write");
            decided.release();
            held.release();
            settle.await.expect("task").expect("db (no 40P01)").expect("job");
        }
        drop(decided);
        drop(held);
        assert!(h.authz_queued(ALICE).await);
    }
    h.finish().await;
}

// ---- T-44: one snapshot ----

/// The SQL statements sqlx runs on this thread, from its `sqlx::query`
/// events.
#[derive(Clone, Default)]
struct Statements(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Statements {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        #[derive(Default)]
        struct Fields {
            summary: String,
            statement: String,
        }
        impl tracing::field::Visit for Fields {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                match field.name() {
                    "summary" => self.summary = value.to_string(),
                    "db.statement" => self.statement = value.to_string(),
                    _ => {}
                }
            }
            fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let text = if fields.statement.trim().is_empty() { fields.summary } else { fields.statement.trim().to_string() };
        self.0.lock().expect("statements").push(text);
    }
}

/// T-44 (§5.4, §5.5): `following_facts_statement()` is one statement and
/// the text the snapshot runs. A post under no audience, under its owner's
/// `list` audience and under another's `following` audience each run the
/// same number of snapshot statements.
#[tokio::test]
async fn t44_the_following_facts_are_one_statement() {
    use tracing_subscriber::layer::SubscriberExt;
    let Some(h) = H::new("s10-t44", PublicMode::On).await else {
        return;
    };
    let facts = schema::following_facts_statement();
    assert_eq!(facts.matches("SELECT").count(), 5, "one SELECT of two EXISTS pairs: {facts}");
    assert!(!facts.contains(';'));
    let (aud, root) = h.alice_following("t44000").await;
    let list = h.audience(BOB, "3kaudt44001", "list").await;
    let own = h.post(BOB, "3kpstt44001", &list, None).await;
    let record = h.f.encoded_post(BOB, "3kpstt44002", "no audience", None).await;
    h.f.ingest(BOB, NSID_POST_PRIVATE, "3kpstt44002", record).await;
    let none = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kpstt44002");
    let other = h.post(BOB, "3kpstt44003", &aud, Some((&root, &root))).await;
    let statements = Statements::default();
    // With a single registered dispatcher, tracing-core asks only the
    // registering thread's default whether a new callsite is wanted, so a
    // callsite another test's thread registers first would be off here. A
    // second one keeps every dispatcher asked.
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(statements.clone()));
    let _guard = tracing::dispatcher::set_default(&dispatch);
    tracing::callsite::rebuild_interest_cache();
    let mut counts = Vec::new();
    for uri in [&none, &own, &other] {
        statements.0.lock().expect("statements").clear();
        oracle_admits(h.pool(), CAROL, uri).await;
        let ran = statements.0.lock().expect("statements").clone();
        assert!(ran.contains(&facts),"{uri}: the snapshot runs following_facts_statement(): {ran:#?}");
        counts.push(ran.len());
    }
    assert!(counts.windows(2).all(|w| w[0] == w[1]), "{counts:?}");
    h.finish().await;
}

// ---- T-45: the pre-release count ----

/// Prints the pre-S10 count query (following-audience design §10) for
/// skydeval to run read-only before migration 0028:
/// `cargo test pre_s10_count_query -- --ignored --nocapture`.
#[test]
#[ignore = "prints the pre-S10 count query; run with --ignored --nocapture"]
fn pre_s10_count_query() {
    println!("-- S10 pre-release count: what the `following` mode starts serving\n{}", schema::pre_s10_count_sql());
}

/// T-45: the pre-S10 count query runs, and counts each kind once.
#[tokio::test]
async fn pre_s10_count_query_counts_each_kind() {
    let Some(w) = Walk::new("pre-s10-count", Layout::SamePds, Some("following"), &[]).await else {
        return;
    };
    let root = w.root.clone();
    for did in [BOB, CAROL] {
        exec(&w.f.pool, &follow_sql(ALICE, did)).await;
    }
    // Starting: Bob's reply (Alice follows him). Not: Dave's.
    w.reply(BOB, "3kt45bobrep2", &root).await;
    w.reply(DAVE, "3kt45daverep", &root).await;
    // An eligible non-member owner with 10 000 stored follows.
    for sql in [
        format!("INSERT INTO private_owner_eligible (did, confirmed_at) VALUES ('{EVE}', now())"),
        format!(
            "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members) \
             VALUES ('at://{EVE}/{NSID_AUDIENCE}/3kt45eveaud1', '{EVE}', '3kt45eveaud1', 'bafy', '{REV}', 'following', '{{}}')"
        ),
        format!(
            "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) \
             SELECT 'at://{EVE}/app.bsky.graph.follow/' || i, '{EVE}', 'did:plc:x' || i, '{REV}', now() FROM generate_series(1, 10000) AS i"
        ),
    ] {
        exec(&w.f.pool, &sql).await;
    }
    let counts: (i64, i64, i64, i64, i64, i64, i64, i64, i64) =
        sqlx::query_as(&schema::pre_s10_count_sql()).fetch_one(&w.f.pool).await.expect("count query");
    // Two audiences and owners (a member, a non-member); one owner over 10 000;
    // Alice's root; two cross-owner replies, one starting; Alice's root
    // for Bob and Carol.
    assert_eq!(counts, (2, 2, 1, 1, 1, 1, 2, 1, 2));
    w.finish().await;
}

// ---- T-40: isolation ----

/// T-40 (§7): no S10 name outside `src/private_tier/` contains a private
/// table's name, by the Dockerfile's own pattern.
#[test]
fn t40_s10_names_name_no_private_table() {
    let tables = [
        "posts", "audiences", "blocks", "tombstones", "owner_ready", "owner_eligible", "bookmarks", "threadgates", "postgates",
        "quote_refs", "notifications", "notification_seen",
    ];
    let names = [
        "follows_verified_at",
        "follows_verified_rev",
        "firehose_rev",
        "follows_clears",
        "follows_clears_seq",
        "current_epoch_locked",
        "0028_follows_verified",
        "backfill.follows_pass_ceiling",
        "follows_pass_total",
        "follows_pass_skipped_total",
        "following_owners_waiting_firehose",
        "owns_following_audience",
        "enqueue_following_verification",
        "follows_pass.rs",
        "advance_applied_rev_with_follows",
        "read_follows_clears",
        "clear_follows_verified",
        "FOLLOWS_RETRY",
        "follows_retry",
    ];
    for name in names {
        for table in tables {
            assert!(!name.contains(&format!("private_{table}")), "{name} names private_{table}");
        }
    }
}

/// T-73 (replay-readiness design §10): no #723 name outside
/// `src/private_tier/` contains a private table's name, by the Dockerfile's
/// own pattern.
#[test]
fn t73_listed_rev_names_name_no_private_table() {
    let tables = [
        "posts", "audiences", "blocks", "tombstones", "owner_ready", "owner_eligible", "bookmarks", "threadgates", "postgates",
        "quote_refs", "notifications", "notification_seen",
    ];
    let names = [
        "post_listed_rev",
        "audience_listed_rev",
        "raise_listed_rev",
        "LISTED_REV_LOCK_DEADLINE",
        "listed_rev_lock_retries_total",
        "listed_rev_busy",
        "replayed_below",
        "settle_uncovered",
        "settle_uncovered_total",
        "0030_listed_revs",
        "ListedRevs",
        "Skipped",
        "Raise",
        "replayed_since_listing",
    ];
    for name in names {
        for table in tables {
            assert!(!name.contains(&format!("private_{table}")), "{name} names private_{table}");
        }
    }
}
