//! The history crawl against Postgres: crawls of signed test repositories
//! through a stub source, continuity on live copies, the sweeper and the
//! re-list on a set low-water entry, Complete, kinds changes and a full
//! trial's history rules.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proto_blue::crypto::{K256Keypair, Keypair};
use proto_blue::repo::{MemoryBlockstore, Repo, RepoWrite};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::client::{CrawlSource, Identity, IdentityMiss, ListedRepo, RepoFetch, RepoStatus, ReposPage};
use super::state::{self, Claim};
use super::task::{Crawler, Relay};
use crate::aggregations::ScopeState;
use crate::config::{FullConfig, FullCrawlConfig, FullGroup, IndexingConfig, PublicScope, ScopePolicy};
use crate::firehose::backfill::{ListPage, ListedRecord};
use crate::firehose::tier::CopyTier;
use crate::firehose::writer::{Action, ApplyOptions, RawOp, WriteSource, Writer};
use crate::indexing::public_mode::{Phase, Transition, now};
use crate::indexing::{PublicIndexing, PublicModeSettings, ScopeValue};
use crate::private_tier::DisabledPrivateTier;
use crate::test_support::fresh_db;

const MEMBER: &str = "did:plc:example2memberaaaaaaaaaa";
const STRANGER: &str = "did:plc:example3strangeraaaaaaaa";
const OTHER: &str = "did:plc:example4otheraaaaaaaaaaaa";
const S_AUTHOR: &str = "did:plc:example5sauthoraaaaaaaaa";
const PDS: &str = "https://pds.example";
const RELAY_A: &str = "https://relay-a.example";
const RELAY_B: &str = "https://relay-b.example";

async fn exec(p: &PgPool, sql: &str) {
    sqlx::raw_sql(sql).execute(p).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn count(p: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(p).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// A signed repository kept in memory.
struct Account {
    did: String,
    key: K256Keypair,
    storage: Arc<MemoryBlockstore>,
    repo: Repo,
}

impl Account {
    fn new(did: &str) -> Self {
        let key = K256Keypair::generate();
        let storage = Arc::new(MemoryBlockstore::new());
        let repo = Repo::create(storage.clone(), did.to_string(), &key).expect("repo");
        Self { did: did.to_string(), key, storage, repo }
    }

    fn put(&mut self, records: &[(&str, &str, Value)]) {
        let writes: Vec<RepoWrite> = records
            .iter()
            .map(|(coll, rkey, v)| {
                let value = proto_blue::lex_json::json_to_lex(v);
                let (collection, rkey) = ((*coll).to_string(), (*rkey).to_string());
                if self.repo.get_record(coll, &rkey).ok().flatten().is_some() {
                    RepoWrite::Update { collection, rkey, value }
                } else {
                    RepoWrite::Create { collection, rkey, value }
                }
            })
            .collect();
        self.repo.apply_writes(&writes, &self.key).expect("write");
    }

    fn delete(&mut self, coll: &str, rkey: &str) {
        self.repo.apply_writes(&[RepoWrite::Delete { collection: coll.into(), rkey: rkey.into() }], &self.key).expect("delete");
    }

    fn car(&self) -> Vec<u8> {
        proto_blue::repo::blocks_to_car(self.repo.commit_cid(), &self.storage.snapshot()).expect("car")
    }

    fn rev(&self) -> String {
        self.repo.commit().expect("commit").rev.clone()
    }

    fn key_did(&self) -> String {
        Keypair::did(&self.key)
    }
}

/// What the stub answers for one account.
#[derive(Clone)]
enum Answer {
    Car(Vec<u8>),
    TooLarge,
    Inactive(&'static str),
}

/// The crawl's outside world, in memory.
#[derive(Default)]
struct Fake {
    repos: Mutex<HashMap<String, Answer>>,
    /// Keys each identity read returns, in turn; the last one repeats.
    keys: Mutex<HashMap<String, VecDeque<Option<String>>>>,
    listings: Mutex<HashMap<String, Vec<ListedRepo>>>,
    records: Mutex<HashMap<(String, String), Vec<ListedRecord>>>,
    latest: Mutex<HashMap<String, String>>,
    statuses: Mutex<HashMap<String, RepoStatus>>,
    identity_reads: AtomicUsize,
    list_calls: AtomicUsize,
}

impl Fake {
    fn serve(&self, a: &Account) {
        self.repos.lock().expect("repos").insert(a.did.clone(), Answer::Car(a.car()));
        self.keys.lock().expect("keys").insert(a.did.clone(), VecDeque::from([Some(a.key_did())]));
    }

    fn answer(&self, did: &str, answer: Answer) {
        self.repos.lock().expect("repos").insert(did.to_string(), answer);
    }

    fn keys(&self, did: &str, keys: Vec<Option<String>>) {
        self.keys.lock().expect("keys").insert(did.to_string(), keys.into());
    }

    fn list(&self, base: &str, repos: Vec<(&str, Option<&str>)>) {
        let entries = repos.into_iter().map(|(did, rev)| ListedRepo { did: did.into(), head: None, rev: rev.map(str::to_string), active: Some(true), status: None }).collect();
        self.listings.lock().expect("listings").insert(base.to_string(), entries);
    }
}

#[async_trait]
impl CrawlSource for Fake {
    async fn list_repos(&self, base: &str, cursor: Option<&str>) -> Result<ReposPage, String> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        let all = self.listings.lock().expect("listings").get(base).cloned().ok_or("no listing")?;
        let from: usize = cursor.map_or(0, |c| c.parse().unwrap_or(0));
        let repos: Vec<ListedRepo> = all.iter().skip(from).take(2).cloned().collect();
        let cursor = (from + 2 < all.len()).then(|| (from + 2).to_string());
        Ok(ReposPage { repos, cursor })
    }

    async fn relay_repo_status(&self, _base: &str, did: &str) -> Result<Option<RepoStatus>, String> {
        Ok(self.statuses.lock().expect("statuses").get(did).cloned())
    }

    async fn identity(&self, did: &str, _fresh: bool) -> Result<Identity, IdentityMiss> {
        self.identity_reads.fetch_add(1, Ordering::SeqCst);
        let mut keys = self.keys.lock().expect("keys");
        let Some(q) = keys.get_mut(did) else {
            return Err(IdentityMiss::Gone("unknown".into()));
        };
        let key = if q.len() > 1 { q.pop_front().flatten() } else { q.front().cloned().flatten() };
        Ok(Identity { pds: PDS.into(), key })
    }

    async fn get_repo(&self, _pds: &str, did: &str, _max: u64) -> Result<RepoFetch, String> {
        Ok(match self.repos.lock().expect("repos").get(did).cloned() {
            Some(Answer::Car(b)) => RepoFetch::Car(b),
            Some(Answer::TooLarge) => RepoFetch::TooLarge,
            Some(Answer::Inactive(s)) => RepoFetch::Inactive(s.into()),
            None => RepoFetch::NotFound,
        })
    }

    async fn latest_rev(&self, _pds: &str, did: &str) -> Result<String, String> {
        self.latest.lock().expect("latest").get(did).cloned().ok_or_else(|| "no rev".into())
    }

    async fn list_records(&self, _pds: &str, did: &str, collection: &str, _cursor: Option<&str>) -> Result<ListPage, String> {
        let records = self.records.lock().expect("records").get(&(did.to_string(), collection.to_string())).cloned().unwrap_or_default();
        Ok(ListPage { records, cursor: None })
    }
}

/// A stack in On, Full: a member, S holding the member and one author.
struct Stack {
    pool: PgPool,
    scope: ScopeState,
    _pi: PublicIndexing,
    writer: Writer,
    fake: Arc<Fake>,
    crawler: Crawler,
    low_water: Arc<crate::firehose::consumer::LowWater>,
}

fn crawl_config() -> FullCrawlConfig {
    FullCrawlConfig { repos_per_sec: 1000, concurrency: 1, max_attempts: 2, ..FullCrawlConfig::default() }
}

async fn stack(p: &PgPool, full: FullConfig, crawl: FullCrawlConfig, relays: &[(&str, &str)]) -> Stack {
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{MEMBER}', 'admin', now())")).await;
    exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{MEMBER}', 2), ('{S_AUTHOR}', 2)")).await;
    sqlx::query("INSERT INTO operator_settings (key, value, file_value, updated_by) VALUES ('indexing.public_scope', '\"community\"', $1, 'cli')")
        .bind(json!({"value": "community", "explicit": true}))
        .execute(p)
        .await
        .expect("setting");
    sqlx::query("INSERT INTO worker_state (key, value) VALUES ('public_transition', $1)")
        .bind(Transition::settled(Phase::On, 3, now(), None).to_json())
        .execute(p)
        .await
        .expect("transition");
    let scope = ScopeState::load(p, &DisabledPrivateTier, ScopePolicy::MembersAndOneHop, 10_000).await.expect("scope");
    scope.load_full(p, &full, &["bsky".to_string()]).await.expect("full sets");
    let pi = PublicIndexing::new(p.clone(), scope.clone(), PublicModeSettings::from_config(&IndexingConfig::default())).with_full_kinds(&full.kinds);
    pi.startup(&IndexingConfig { public_scope: Some(PublicScope::Community), ..IndexingConfig::default() }).await.expect("startup");
    pi.apply_scope(ScopeValue::Full, "cli").await.expect("full");
    let writer = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    let fake = Arc::new(Fake::default());
    let low_water: Arc<crate::firehose::consumer::LowWater> = Arc::default();
    // Every relay has sent unstamped copies: Whole.
    let mut tiers = crate::firehose::tier::Tiers::new(crate::indexing::full::full_set(&FullGroup::ALL), Vec::<String>::new());
    for (label, _) in relays {
        tiers.observe(label, None);
    }
    let relays = relays.iter().map(|(label, base)| Relay { label: (*label).into(), base: (*base).into() }).collect();
    let crawler = Crawler::new(writer.clone(), fake.clone(), crawl, full, relays, tiers.board(), Vec::new(), 600).with_clocks(Arc::clone(&low_water), Arc::default()).without_claimers();
    Stack { pool: p.clone(), scope, _pi: pi, writer, fake, crawler, low_water }
}

async fn default_stack(p: &PgPool) -> Stack {
    stack(p, FullConfig::default(), crawl_config(), &[("bsky", RELAY_A)]).await
}

impl Stack {
    async fn generation(&self) -> i64 {
        i64::try_from(self.scope.read_guard().await.public.crawl_generation).expect("gen")
    }

    async fn enabled(&self) -> Vec<String> {
        state::read_groups(&self.pool).await.expect("groups").expect("groups").enabled()
    }

    async fn seed(&self, dids: &[&str]) {
        let dids: Vec<String> = dids.iter().map(|d| (*d).to_string()).collect();
        state::seed(&self.pool, &dids, self.generation().await, &self.enabled().await, 0, None).await.expect("seed");
    }

    async fn claim(&self) -> Claim {
        state::claim_next(&self.pool, self.generation().await, 1800).await.expect("claim").expect("a row is due")
    }

    async fn row(&self, did: &str) -> (String, Option<String>, Option<String>, i64, Option<String>, Vec<String>, Option<String>) {
        sqlx::query_as("SELECT f.state, f.last_rev, f.chain_rev, f.epoch, f.reason, f.groups, f.chain_from FROM full_crawl f JOIN dids d ON d.id = f.account WHERE d.did = $1")
            .bind(did)
            .fetch_one(&self.pool)
            .await
            .expect("row")
    }

    async fn marks(&self, did: &str) -> i64 {
        count(&self.pool, &format!("SELECT count(*) FROM full_crawl_groups m JOIN dids d ON d.id = m.account WHERE d.did = '{did}'")).await
    }

    async fn local(&self, did: &str, g: FullGroup) -> bool {
        let generation = self.scope.read_guard().await.public.crawl_generation;
        super::marks::marked(&self.pool, &[did.to_string()], g, generation).await.expect("marks").contains(did)
    }

    /// A live Whole copy of `did`'s commit `rev` with `since`, read at
    /// stream time `ms`.
    async fn live(&self, did: &str, rev: &str, since: Option<&str>, ops: &[RawOp], ms: i64, tier: CopyTier) {
        let options = ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: true, subscription_rules: false, source: WriteSource::Firehose };
        self.writer.apply_live(Some("bsky"), did, rev, since, ops, options, tier, Some(ms)).await.expect("live");
    }

    fn low_water(&self, ms: i64) {
        self.low_water.set(Some(&format!("{ms}-0")));
    }
}

fn post(text: &str) -> Value {
    json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-01-01T00:00:00Z"})
}

fn like_of_member(at: &str) -> Value {
    json!({"$type": "app.bsky.feed.like", "subject": {"uri": format!("at://{MEMBER}/app.bsky.feed.post/3kmemberpost2"), "cid": "bafyreid"}, "createdAt": at})
}

fn op(collection: &str, rkey: &str, record: Value) -> RawOp {
    RawOp { action: Action::Create, collection: collection.into(), rkey: rkey.into(), cid: Some("bafyop".into()), record: Some(record) }
}

/// A crawled repository is verified, applied as the crawl (no notification
/// for an old like of a member's post, where a live copy notifies), in
/// batches of at most 50 ops; private records in it are ignored; a stored
/// row it no longer holds is deleted with a tombstone at its rev; the
/// account gets a mark for every group and its `last_rev` and chain rise to
/// the repository's rev, so its own records read locally.
#[tokio::test]
async fn a_crawled_repository_is_verified_applied_reconciled_and_marked() {
    let Some(db) = fresh_db("crawl_applied").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let p = &s.pool;
    // A post the repository no longer holds, stored live earlier.
    s.live(STRANGER, "3kaaaaaaaaa22", None, &[op("app.bsky.feed.post", "3kgonepost222", post("gone"))], 1, CopyTier::Whole).await;
    // A live like of the member's post notifies (the control).
    s.live(OTHER, "3kaaaaaaaab22", None, &[op("app.bsky.feed.like", "3kotherlike22", like_of_member(&chrono::Utc::now().to_rfc3339()))], 2, CopyTier::Whole).await;
    assert_eq!(count(p, "SELECT count(*) FROM notifications WHERE reason = 'like'").await, 1, "a live like notifies");
    let mut a = Account::new(STRANGER);
    let mut records = vec![
        ("app.bsky.feed.post", "3kpostkept222".to_string(), post("kept")),
        ("app.bsky.feed.like", "3klikemember2".to_string(), like_of_member("2025-06-01T00:00:00Z")),
        ("app.bsky.actor.profile", "self".to_string(), json!({"$type": "app.bsky.actor.profile", "displayName": "Stranger"})),
        ("tools.kryphocron.feed.postPrivate", "3kprivate2222".to_string(), json!({"$type": "tools.kryphocron.feed.postPrivate", "text": "secret"})),
    ];
    for i in 0..60 {
        records.push((
            "app.bsky.feed.like",
            crate::test_support::posts::tid(100 + i),
            json!({"$type": "app.bsky.feed.like", "subject": {"uri": format!("at://{OTHER}/app.bsky.feed.post/3kotherpost22"), "cid": "bafyreid"}, "createdAt": "2025-01-01T00:00:00Z"}),
        ));
    }
    let refs: Vec<(&str, &str, Value)> = records.iter().map(|(c, r, v)| (*c, r.as_str(), v.clone())).collect();
    a.put(&refs);
    s.fake.serve(&a);
    let claim = s.claim().await;
    assert_eq!(claim.did, STRANGER);
    assert_eq!(claim.groups.len(), 9, "a first crawl owes every group");
    let outcome = s.crawler.process(&claim, false).await.expect("process");
    let super::apply::Outcome::Done { records, deleted, marks, whole, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!((records, deleted, marks, whole), (63, 1, 9, true), "the public records, the reconciled post, a mark per group");
    let rev = a.rev();
    let (st, last, chain, _, _, owed, _) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), last.as_deref(), chain.as_deref(), owed.len()), ("done", Some(rev.as_str()), Some(rev.as_str()), 0));
    let author = format!("(SELECT id FROM dids WHERE did = '{STRANGER}')");
    assert_eq!(count(p, &format!("SELECT count(*) FROM likes WHERE author = {author}")).await, 61);
    assert_eq!(count(p, &format!("SELECT count(*) FROM posts WHERE author = {author}")).await, 1, "the post the repository dropped is gone");
    assert_eq!(count(p, &format!("SELECT count(*) FROM record_tombstones WHERE author = {author}")).await, 1, "with a tombstone");
    assert_eq!(count(p, &format!("SELECT count(*) FROM actors WHERE id = {author} AND profile_record IS NOT NULL")).await, 1);
    assert_eq!(count(p, "SELECT count(*) FROM notifications WHERE reason = 'like'").await, 1, "the crawled like of the member's post notified nobody");
    assert!(s.local(STRANGER, FullGroup::Posts).await && s.local(STRANGER, FullGroup::Likes).await);
    assert!(!s.local(OTHER, FullGroup::Posts).await, "an account never crawled reads today's source");
    db.drop_db().await;
}

/// A signature that fails reads the directory again once (a rotated key
/// passes); one that still fails is retried with backoff and, after
/// `max_attempts`, stays `failed` (incomplete) with no mark.
#[tokio::test]
async fn a_bad_signature_reads_the_key_again_then_stays_incomplete() {
    let Some(db) = fresh_db("crawl_bad_signature").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let mut a = Account::new(STRANGER);
    a.put(&[("app.bsky.feed.post", "3kpost2222222", post("hi"))]);
    s.fake.serve(&a);
    let wrong = Account::new(OTHER).key_did();
    // Rotated: the first read is stale, the second current.
    s.fake.keys(STRANGER, vec![Some(wrong.clone()), Some(a.key_did())]);
    s.seed(&[STRANGER]).await;
    let out = s.crawler.process(&s.claim().await, false).await.expect("process");
    assert!(matches!(out, super::apply::Outcome::Done { whole: true, .. }), "{out:?}");
    // Now always wrong: two failed attempts leave it failed.
    s.fake.keys(STRANGER, vec![Some(wrong)]);
    exec(&s.pool, "UPDATE full_crawl SET state = 'pending', groups = ARRAY['posts'], next_at = now()").await;
    let out = s.crawler.process(&s.claim().await, false).await.expect("process");
    assert_eq!(out, super::apply::Outcome::Failed("bad_signature".into()));
    let (st, _, _, _, reason, _, _) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), reason.as_deref()), ("pending", Some("bad_signature")), "retried with backoff");
    assert!(state::claim_next(&s.pool, s.generation().await, 1800).await.expect("claim").is_none(), "not due before its backoff");
    exec(&s.pool, "UPDATE full_crawl SET next_at = now()").await;
    s.crawler.process(&s.claim().await, false).await.expect("process");
    let (st, ..) = s.row(STRANGER).await;
    assert_eq!(st, "failed", "after max_attempts it stays incomplete");
    db.drop_db().await;
}

/// A repository too large to fetch whole is read per collection: its
/// records are applied but, unverified, it gets no mark and stays
/// incomplete. A suspended or missing repository ends `skipped` or `gone`.
#[tokio::test]
async fn a_repository_too_large_is_listed_and_stays_incomplete() {
    let Some(db) = fresh_db("crawl_too_large").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    s.fake.answer(STRANGER, Answer::TooLarge);
    s.fake.keys(STRANGER, vec![Some("did:key:zunused".into())]);
    s.fake.latest.lock().expect("latest").insert(STRANGER.into(), "3kzzzzzzzzz22".into());
    s.fake.records.lock().expect("records").insert(
        (STRANGER.into(), "app.bsky.feed.post".into()),
        vec![ListedRecord { uri: format!("at://{STRANGER}/app.bsky.feed.post/3klisted22222"), cid: "bafylisted".into(), value: post("listed") }],
    );
    s.seed(&[STRANGER]).await;
    let out = s.crawler.process(&s.claim().await, false).await.expect("process");
    assert_eq!(out, super::apply::Outcome::Unverified { records: 1 });
    assert_eq!(count(&s.pool, &format!("SELECT count(*) FROM posts WHERE author = (SELECT id FROM dids WHERE did = '{STRANGER}')")).await, 1);
    let (st, _, _, _, reason, _, _) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), reason.as_deref()), ("failed", Some("unverified")));
    assert_eq!(s.marks(STRANGER).await, 0);
    // Suspended at its PDS, and missing.
    s.fake.answer(OTHER, Answer::Inactive("takendown"));
    s.fake.keys(OTHER, vec![Some("did:key:zunused".into())]);
    s.fake.keys(S_AUTHOR, vec![Some("did:key:zunused".into())]);
    s.seed(&[OTHER, S_AUTHOR]).await;
    s.crawler.process(&s.claim().await, false).await.expect("process");
    s.crawler.process(&s.claim().await, false).await.expect("process");
    assert_eq!(s.row(OTHER).await.0, "skipped");
    assert_eq!(s.row(S_AUTHOR).await.0, "gone");
    db.drop_db().await;
}

/// LWW and tombstones hold against a crawl: a stored row at a newer rev is
/// kept and a newer tombstone refuses the record; neither is reconciled
/// away. A CAR older than the account's first live sighting is retried, at
/// once the first time and then with backoff, never in a loop.
#[tokio::test]
async fn a_crawl_never_overwrites_newer_state() {
    let Some(db) = fresh_db("crawl_lww").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let mut a = Account::new(STRANGER);
    a.put(&[("app.bsky.feed.post", "3kpostold2222", post("from the repository")), ("app.bsky.feed.post", "3kpostdead222", post("deleted since"))]);
    s.fake.serve(&a);
    // Live, after the repository's rev: an edit and a delete.
    s.live(STRANGER, "3zzzzzzzzzz22", None, &[op("app.bsky.feed.post", "3kpostold2222", post("edited live"))], 1, CopyTier::Whole).await;
    let del = RawOp { action: Action::Delete, collection: "app.bsky.feed.post".into(), rkey: "3kpostdead222".into(), cid: None, record: None };
    s.live(STRANGER, "3zzzzzzzzzz23", Some("3zzzzzzzzzz22"), &[del], 2, CopyTier::Whole).await;
    let out = s.crawler.process(&s.claim().await, false).await.expect("process");
    assert!(matches!(out, super::apply::Outcome::Done { deleted: 0, .. }), "{out:?}");
    let p = &s.pool;
    let author = format!("(SELECT id FROM dids WHERE did = '{STRANGER}')");
    assert_eq!(count(p, &format!("SELECT count(*) FROM posts WHERE author = {author}")).await, 1, "the tombstoned post is not written back");
    let live = crate::db::ids::rev_value("3zzzzzzzzzz22");
    assert_eq!(count(p, &format!("SELECT count(*) FROM posts WHERE author = {author} AND rev = {live}")).await, 1, "the live edit stays");
    // An account first seen live after its repository's rev.
    let mut b = Account::new(OTHER);
    b.put(&[("app.bsky.feed.post", "3kpost2222222", post("old"))]);
    s.fake.serve(&b);
    s.live(OTHER, "3zzzzzzzzzz30", Some("3zzzzzzzzzz29"), &[], 3, CopyTier::Whole).await;
    let out = s.crawler.process(&s.claim().await, false).await.expect("process");
    assert_eq!(out, super::apply::Outcome::Refused);
    let (st, _, _, _, reason, _, from) = s.row(OTHER).await;
    assert_eq!((st.as_str(), reason.as_deref(), from.as_deref()), ("pending", Some("older_than_chain"), Some("3zzzzzzzzzz29")), "retried at once");
    assert_eq!(s.marks(OTHER).await, 0, "a CAR older than chain_from sets no mark");
    s.crawler.process(&s.claim().await, false).await.expect("process");
    let attempts: i32 = sqlx::query_scalar(&format!("SELECT attempts FROM full_crawl WHERE account = (SELECT id FROM dids WHERE did = '{OTHER}')")).fetch_one(p).await.expect("row");
    assert_eq!(attempts, 1, "then a counted attempt");
    assert!(state::claim_next(p, s.generation().await, 1800).await.expect("claim").is_none(), "with backoff");
    db.drop_db().await;
}

/// A live commit whose `since` is above `chain_rev` opens a link, which is
/// a hole (epoch once, marks cleared, queued) only once the low-water entry
/// is a window past it; a late copy of the missing commit closes it first;
/// a `#sync` is a hole at once and rebases so the next commit chains.
#[tokio::test]
async fn links_become_holes_after_the_window_and_a_sync_at_once() {
    let Some(db) = fresh_db("crawl_links").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let g = s.generation().await;
    let groups = s.enabled().await;
    let none: &[RawOp] = &[];
    s.live(STRANGER, "3kaaaaaaaaa22", None, none, 1_000, CopyTier::Whole).await;
    s.live(STRANGER, "3kaaaaaaaab22", Some("3kaaaaaaaaa22"), none, 2_000, CopyTier::Whole).await;
    exec(&s.pool, &format!("INSERT INTO full_crawl_groups (account, grp, group_generation, crawl_generation) SELECT id, 'posts', 1, {g} FROM dids WHERE did = '{STRANGER}'")).await;
    // A commit whose parent was missed: a link.
    s.live(STRANGER, "3kaaaaaaaad22", Some("3kaaaaaaaac22"), none, 3_000, CopyTier::Whole).await;
    assert_eq!(count(&s.pool, "SELECT count(*) FROM full_chain_links").await, 1);
    let epoch = s.row(STRANGER).await.3;
    s.low_water(3_000 + 599_000);
    s.crawler.sweep(g, &groups).await.expect("sweep");
    assert_eq!((s.row(STRANGER).await.3, s.marks(STRANGER).await), (epoch, 1), "inside the window: no hole");
    // The missed commit arrives late from another relay: it chains, and
    // the link closes.
    s.live(STRANGER, "3kaaaaaaaac22", Some("3kaaaaaaaab22"), none, 3_500, CopyTier::Whole).await;
    assert_eq!(count(&s.pool, "SELECT count(*) FROM full_chain_links").await, 0, "a late copy closes the link");
    assert_eq!(s.row(STRANGER).await.2.as_deref(), Some("3kaaaaaaaad22"));
    // Another missed commit, this time never delivered.
    s.live(STRANGER, "3kaaaaaaaag22", Some("3kaaaaaaaaf22"), none, 4_000, CopyTier::Whole).await;
    s.low_water(4_000 + 600_000);
    s.crawler.sweep(g, &groups).await.expect("sweep");
    let (st, last, chain, e, _, owed, from) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), e, s.marks(STRANGER).await), ("pending", epoch + 1, 0), "a hole: epoch once, marks cleared, queued");
    assert_eq!((last.as_deref(), chain.as_deref(), from.as_deref(), owed.len()), (Some("3kaaaaaaaag22"), Some("3kaaaaaaaag22"), Some("3kaaaaaaaag22"), 9));
    s.crawler.sweep(g, &groups).await.expect("sweep");
    assert_eq!(s.row(STRANGER).await.3, epoch + 1, "holed once");
    // The next commit chains from the rebase.
    s.live(STRANGER, "3kaaaaaaaah22", Some("3kaaaaaaaag22"), none, 5_000, CopyTier::Whole).await;
    assert_eq!(count(&s.pool, "SELECT count(*) FROM full_chain_links").await, 0);
    // A `#sync`: a hole at once, rebased on the reset's rev.
    assert!(s.writer.apply_sync(STRANGER, "3kaaaaaaaaz22", CopyTier::Whole, 6_000).await.expect("sync"));
    let (_, last, chain, e, ..) = s.row(STRANGER).await;
    assert_eq!((last.as_deref(), chain.as_deref(), e), (Some("3kaaaaaaaaz22"), Some("3kaaaaaaaaz22"), epoch + 2));
    s.live(STRANGER, "3kaaaaaabaa22", Some("3kaaaaaaaaz22"), none, 7_000, CopyTier::Whole).await;
    assert_eq!(count(&s.pool, "SELECT count(*) FROM full_chain_links").await, 0, "the next commit chains");
    // An Aside relay's `#sync` writes nothing.
    assert!(!s.writer.apply_sync(STRANGER, "3kaaaaaabzz22", CopyTier::Aside, 8_000).await.expect("sync"));
    db.drop_db().await;
}

/// A hole during a crawl refuses that crawl's `done` and returns the row to
/// `pending` at once, with no mark; the retry fetches every group and its
/// `done` raises `last_rev`. A crash mid-fetch leaves `fetching` rows that
/// the next start returns to `pending`.
#[tokio::test]
async fn a_hole_during_a_crawl_refuses_its_done() {
    let Some(db) = fresh_db("crawl_hole_refuses").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let mut a = Account::new(STRANGER);
    a.put(&[("app.bsky.feed.post", "3kpost2222222", post("hi"))]);
    s.fake.serve(&a);
    s.seed(&[STRANGER]).await;
    let claim = s.claim().await;
    // A `#sync` while the crawl is in flight.
    s.writer.apply_sync(STRANGER, &a.rev(), CopyTier::Whole, 1_000).await.expect("sync");
    let out = s.crawler.process(&claim, false).await.expect("process");
    assert_eq!(out, super::apply::Outcome::Refused);
    let (st, ..) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), s.marks(STRANGER).await), ("pending", 0), "refused, pending at once, no mark");
    let retry = s.claim().await;
    assert_eq!(retry.groups.len(), 9, "the retry owes every group");
    assert!(matches!(s.crawler.process(&retry, false).await.expect("process"), super::apply::Outcome::Done { whole: true, .. }));
    assert_eq!(s.marks(STRANGER).await, 9);
    // A crash mid-fetch, then a start.
    s.seed(&[OTHER]).await;
    exec(&s.pool, &format!("UPDATE full_crawl SET state = 'fetching', next_at = now() + interval '1 hour' WHERE account = (SELECT id FROM dids WHERE did = '{OTHER}')")).await;
    assert_eq!(state::revert_fetching(&s.pool).await.expect("revert"), 1);
    assert_eq!(s.row(OTHER).await.0, "pending");
    db.drop_db().await;
}

/// The list is the union of the listing relays, merged by DID; Complete
/// waits for the list to be paged through, the entry step and every row,
/// and with no listing relay no group is ever Complete.
#[tokio::test]
async fn complete_needs_the_whole_list_and_every_row() {
    let Some(db) = fresh_db("crawl_complete").await else {
        return;
    };
    let s = stack(&db.pool, FullConfig { takedown_relays: Vec::new(), ..FullConfig::default() }, crawl_config(), &[("bsky", RELAY_A), ("other", RELAY_B)]).await;
    let mut accounts = Vec::new();
    for did in [STRANGER, OTHER, S_AUTHOR] {
        let mut a = Account::new(did);
        a.put(&[("app.bsky.feed.post", "3kpost2222222", post(did))]);
        s.fake.serve(&a);
        accounts.push(a);
    }
    s.fake.list(RELAY_A, vec![(STRANGER, None), (OTHER, None)]);
    s.fake.list(RELAY_B, vec![(OTHER, None), (S_AUTHOR, None)]);
    let (mut entered, mut seeded, mut last) = (None, false, std::time::Instant::now() - std::time::Duration::from_secs(60));
    // Ticks page the relays and turn the entry step; claimers are driven
    // here by hand.
    for _ in 0..4 {
        s.crawler.tick(&mut entered, &mut seeded, &mut last).await.expect("tick");
    }
    let rows = count(&s.pool, "SELECT count(*) FROM full_crawl").await;
    assert_eq!(rows, 4, "the member, S's author (once) and the listed accounts, merged");
    let g = s.generation().await;
    assert!(state::complete_groups(&s.pool, g).await.expect("complete").is_empty(), "nothing crawled yet");
    while let Some(c) = state::claim_next(&s.pool, g, 1800).await.expect("claim") {
        s.crawler.process(&c, false).await.expect("process");
    }
    // The member's repository is not served: it is gone.
    let turned = state::complete_groups(&s.pool, g).await.expect("complete");
    assert_eq!(turned.len(), 9, "every group turns Complete: {turned:?}");
    // With no listing relay, the crawl never tests Complete.
    let none = Crawler::new(s.writer.clone(), s.fake.clone(), crawl_config(), FullConfig::default(), Vec::new(), Arc::default(), Vec::new(), 600).without_claimers();
    let (mut e2, mut s2, mut l2) = (None, false, std::time::Instant::now() - std::time::Duration::from_secs(60));
    exec(&s.pool, "UPDATE worker_state SET value = jsonb_set(value, '{groups,posts,state}', '\"crawling\"') WHERE key = 'full_groups'").await;
    none.tick(&mut e2, &mut s2, &mut l2).await.expect("tick");
    let groups = state::read_groups(&s.pool).await.expect("groups").expect("groups");
    assert_eq!(groups.groups["posts"].state, "crawling", "no listing relay: never Complete");
    drop(accounts);
    db.drop_db().await;
}

/// After a gap, the re-list pass re-fetches only accounts whose listed head
/// is above `last_rev` (a crawled account never seen live whose head equals
/// its CAR rev is not), once the low-water entry has passed each page's
/// tail; the gap resolves when the pass ends, and marks count again.
#[tokio::test]
async fn a_gap_refetches_only_accounts_whose_head_moved() {
    let Some(db) = fresh_db("crawl_relist").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let mut same = Account::new(STRANGER);
    same.put(&[("app.bsky.feed.post", "3kpost2222222", post("same"))]);
    let mut moved = Account::new(OTHER);
    moved.put(&[("app.bsky.feed.post", "3kpost2222222", post("moved"))]);
    s.fake.serve(&same);
    s.fake.serve(&moved);
    s.seed(&[STRANGER, OTHER]).await;
    for _ in 0..2 {
        let c = s.claim().await;
        s.crawler.process(&c, false).await.expect("process");
    }
    assert!(s.local(STRANGER, FullGroup::Posts).await);
    moved.delete("app.bsky.feed.post", "3kpost2222222");
    super::relist::open_gap(&s.pool, "trim", None, true, json!({})).await.expect("gap");
    assert!(!s.local(STRANGER, FullGroup::Posts).await, "a blocking gap: no mark counts");
    s.fake.list(RELAY_A, vec![(STRANGER, Some(&same.rev())), (OTHER, Some(&moved.rev()))]);
    let g = s.generation().await;
    let groups = s.enabled().await;
    let crawler = s.crawler.clone().with_fixed_tail(1_000);
    s.low_water(1);
    crawler.relist(g, &groups).await.expect("relist");
    assert_eq!(count(&s.pool, "SELECT count(*) FROM full_relist_candidates").await, 1, "only the moved head waits");
    assert_eq!(s.row(OTHER).await.0, "done", "not compared before the stream passes the page");
    s.low_water(i64::MAX / 2);
    crawler.relist(g, &groups).await.expect("relist");
    let (st, last, ..) = s.row(OTHER).await;
    assert_eq!((st.as_str(), last.as_deref(), s.marks(OTHER).await), ("pending", Some(moved.rev().as_str()), 0), "requeued");
    assert_eq!((s.row(STRANGER).await.0, s.marks(STRANGER).await), ("done".to_string(), 9), "an equal head missed nothing");
    assert_eq!(count(&s.pool, "SELECT count(*) FROM full_gaps WHERE resolved_at IS NULL").await, 0, "the pass ended");
    assert!(s.local(STRANGER, FullGroup::Posts).await, "marks count again");
    db.drop_db().await;
}

/// An account whose commits only an Aside relay delivers turns `uncovered`
/// one window after the first: its marks go and it is not queued; a Whole
/// copy reaching its Aside rev clears it and queues one crawl.
#[tokio::test]
async fn an_account_only_aside_relays_carry_turns_uncovered() {
    let Some(db) = fresh_db("crawl_uncovered").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let g = s.generation().await;
    let groups = s.enabled().await;
    let none: &[RawOp] = &[];
    s.live(STRANGER, "3kaaaaaaaaa22", None, none, 1_000, CopyTier::Whole).await;
    exec(&s.pool, &format!("UPDATE full_crawl SET state = 'done', groups = '{{}}' WHERE account = (SELECT id FROM dids WHERE did = '{STRANGER}')")).await;
    exec(&s.pool, &format!("INSERT INTO full_crawl_groups (account, grp, group_generation, crawl_generation) SELECT id, 'posts', 1, {g} FROM dids WHERE did = '{STRANGER}'")).await;
    // An Aside copy of a DID with no row writes none.
    s.live(OTHER, "3kaaaaaaaab22", None, none, 1_500, CopyTier::Aside).await;
    assert_eq!(count(&s.pool, &format!("SELECT count(*) FROM full_crawl f JOIN dids d ON d.id = f.account WHERE d.did = '{OTHER}'")).await, 0);
    for (i, rev) in ["3kaaaaaaaac22", "3kaaaaaaaad22"].iter().enumerate() {
        s.live(STRANGER, rev, None, none, 2_000 + i64::try_from(i).expect("i") * 1_000, CopyTier::Aside).await;
    }
    s.low_water(2_000 + 600_000);
    s.crawler.sweep(g, &groups).await.expect("sweep");
    let uncovered: bool = sqlx::query_scalar("SELECT uncovered FROM full_crawl").fetch_one(&s.pool).await.expect("row");
    assert!(uncovered);
    assert_eq!((s.row(STRANGER).await.0, s.marks(STRANGER).await), ("done".to_string(), 0), "marks gone, not queued");
    assert!(state::claim_next(&s.pool, g, 1800).await.expect("claim").is_none(), "an uncovered row is not claimed");
    // A Whole relay carries it again.
    s.live(STRANGER, "3kaaaaaaaae22", Some("3kaaaaaaaaa22"), none, 700_000, CopyTier::Whole).await;
    let (st, _, _, epoch, ..) = s.row(STRANGER).await;
    let uncovered: bool = sqlx::query_scalar("SELECT uncovered FROM full_crawl").fetch_one(&s.pool).await.expect("row");
    assert_eq!((st.as_str(), uncovered, epoch), ("pending", false, 1), "cleared and queued once");
    db.drop_db().await;
}

/// Removing `likes` takes it out of every owed set and sweeps a stranger's
/// likes (a like of a member's post stays), and likes marks stop counting
/// at once; adding it back unions it into every row (done rows return to
/// pending), and `likes` is not Complete before the union is done.
#[tokio::test]
async fn kinds_changes_crawl_added_groups_and_sweep_removed_ones() {
    let Some(db) = fresh_db("crawl_kinds").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let mut a = Account::new(STRANGER);
    a.put(&[
        ("app.bsky.feed.like", "3klikestrang2", json!({"$type": "app.bsky.feed.like", "subject": {"uri": format!("at://{OTHER}/app.bsky.feed.post/3kotherpost22"), "cid": "bafyreid"}, "createdAt": "2025-01-01T00:00:00Z"})),
        ("app.bsky.feed.like", "3klikemember2", like_of_member("2025-01-01T00:00:00Z")),
    ]);
    s.fake.serve(&a);
    s.seed(&[STRANGER]).await;
    s.crawler.process(&s.claim().await, false).await.expect("process");
    assert!(s.local(STRANGER, FullGroup::Likes).await);
    let g = s.generation().await;
    let without: Vec<FullGroup> = FullGroup::ALL.into_iter().filter(|k| *k != FullGroup::Likes).collect();
    let (added, removed) = super::groups_change::reconcile_kinds(&s.pool, g, &without).await.expect("kinds");
    assert_eq!((added.len(), removed), (0, vec!["likes".to_string()]));
    assert!(!s.local(STRANGER, FullGroup::Likes).await, "likes marks no longer count");
    while super::groups_change::step(&s.pool, g).await.expect("step") {}
    let author = format!("(SELECT id FROM dids WHERE did = '{STRANGER}')");
    assert_eq!(count(&s.pool, &format!("SELECT count(*) FROM likes WHERE author = {author}")).await, 1, "the member-involved like stays");
    // Back again.
    let (added, _) = super::groups_change::reconcile_kinds(&s.pool, g, &FullGroup::ALL).await.expect("kinds");
    assert_eq!(added, vec!["likes".to_string()]);
    assert!(state::complete_groups(&s.pool, g).await.expect("complete").iter().all(|x| x != "likes"), "not before the union");
    while super::groups_change::step(&s.pool, g).await.expect("step") {}
    let (st, _, _, _, _, owed, _) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), owed), ("pending", vec!["likes".to_string()]), "the done row owes likes again");
    let c = s.claim().await;
    assert_eq!(c.groups, vec!["likes".to_string()], "fetched for likes alone");
    let before = s.row(STRANGER).await.1;
    s.crawler.process(&c, false).await.expect("process");
    assert_eq!(s.row(STRANGER).await.1, before, "a group crawl writes no last_rev");
    assert!(s.local(STRANGER, FullGroup::Likes).await);
    db.drop_db().await;
}

/// With a trial's history crawl, S and the members are skipped with reason
/// `trial`; a reconcile deletes a recorded row the repository no longer
/// holds and leaves an unrecorded one, and clears a recorded profile but
/// not an unrecorded one.
#[tokio::test]
async fn a_trials_crawl_skips_s_and_members_and_reconciles_only_recorded_rows() {
    let Some(db) = fresh_db("crawl_trial_history").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let p = &s.pool;
    s.seed(&[S_AUTHOR, MEMBER]).await;
    for _ in 0..2 {
        let c = s.claim().await;
        assert_eq!(s.crawler.process(&c, true).await.expect("process"), super::apply::Outcome::Skipped("trial".into()));
    }
    assert_eq!(s.row(S_AUTHOR).await.4.as_deref(), Some("trial"));
    // Two stored posts and two profiles the repositories no longer hold:
    // one of each recorded by the trial.
    s.live(STRANGER, "3kaaaaaaaaa22", None, &[op("app.bsky.feed.post", "3krecorded222", post("r")), op("app.bsky.feed.post", "3kunrecorded2", post("u"))], 1, CopyTier::Whole).await;
    s.live(STRANGER, "3kaaaaaaaab22", Some("3kaaaaaaaaa22"), &[op("app.bsky.actor.profile", "self", json!({"$type": "app.bsky.actor.profile", "displayName": "R"}))], 2, CopyTier::Whole).await;
    s.live(OTHER, "3kaaaaaaaac22", None, &[op("app.bsky.actor.profile", "self", json!({"$type": "app.bsky.actor.profile", "displayName": "U"}))], 3, CopyTier::Whole).await;
    exec(p, &format!(
        "INSERT INTO full_trial_rows (tbl, a, b, c) SELECT 1, k.author, k.rkey, 0 FROM ref_of('at://{STRANGER}/app.bsky.feed.post/3krecorded222') k; \
         INSERT INTO full_trial_rows (tbl, a, b, c, rev) SELECT 16, id, 0, 0, 0 FROM dids WHERE did = '{STRANGER}'"
    ))
    .await;
    for did in [STRANGER, OTHER] {
        let mut a = Account::new(did);
        a.put(&[("app.bsky.feed.like", "3klike2222222", like_of_member("2025-01-01T00:00:00Z"))]);
        s.fake.serve(&a);
    }
    exec(p, "UPDATE full_crawl SET chain_from = NULL").await;
    for _ in 0..2 {
        let c = s.claim().await;
        s.crawler.process(&c, true).await.expect("process");
    }
    let id = |d: &str| format!("(SELECT id FROM dids WHERE did = '{d}')");
    assert_eq!(count(p, &format!("SELECT count(*) FROM posts WHERE author = {}", id(STRANGER))).await, 1, "only the recorded post is reconciled away");
    assert_eq!(count(p, &format!("SELECT count(*) FROM actors WHERE id = {} AND profile_record IS NOT NULL", id(STRANGER))).await, 0, "the recorded profile is cleared");
    assert_eq!(count(p, &format!("SELECT count(*) FROM actors WHERE id = {} AND profile_record IS NOT NULL", id(OTHER))).await, 1, "an unrecorded one stays");
    db.drop_db().await;
}

/// A takedown relay's suspension of a crawled account moves its epoch,
/// skips it and clears its marks; its return to active queues a crawl.
#[tokio::test]
async fn a_suspension_skips_and_a_return_queues() {
    let Some(db) = fresh_db("crawl_suspension").await else {
        return;
    };
    let s = default_stack(&db.pool).await;
    let mut a = Account::new(STRANGER);
    a.put(&[("app.bsky.feed.post", "3kpost2222222", post("hi"))]);
    s.fake.serve(&a);
    s.seed(&[STRANGER]).await;
    s.crawler.process(&s.claim().await, false).await.expect("process");
    let groups = s.enabled().await;
    crate::indexing::full::record_relay_status(&s.pool, STRANGER, "bsky", Some(false), Some("takendown")).await.expect("status");
    state::account_status(&s.pool, STRANGER, Some(false), Some("takendown"), true, &groups).await.expect("suspended");
    let (st, _, _, epoch, reason, ..) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), reason.as_deref(), epoch, s.marks(STRANGER).await), ("skipped", Some("takendown"), 1, 0));
    crate::indexing::full::record_relay_status(&s.pool, STRANGER, "bsky", Some(true), None).await.expect("status");
    state::account_status(&s.pool, STRANGER, Some(true), None, true, &groups).await.expect("active");
    let (st, _, _, epoch, ..) = s.row(STRANGER).await;
    assert_eq!((st.as_str(), epoch), ("pending", 2), "queued for a crawl");
    // A first sighting reads its takedown relay's status.
    s.fake.statuses.lock().expect("statuses").insert(OTHER.into(), RepoStatus { active: false, status: Some("suspended".into()) });
    s.crawler.first_sight.push(OTHER);
    let (mut e, mut sd, mut l) = (None, false, std::time::Instant::now());
    s.crawler.tick(&mut e, &mut sd, &mut l).await.expect("tick");
    assert_eq!(count(&s.pool, &format!("SELECT count(*) FROM relay_account_status r JOIN dids d ON d.id = r.account WHERE d.did = '{OTHER}'")).await, 1);
    db.drop_db().await;
}
