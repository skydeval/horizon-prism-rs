//! The backfill worker (design-v2 §5.10, §6.11.2, §6.11.5).
//!
//! Jobs are keyed by `(did, depth)` in `backfill_jobs`. The worker runs with
//! `backfill.concurrency` (4) and a per-PDS-host budget of
//! `backfill.per_pds_rate_per_sec` (5/s). For each job:
//! 1. upsert the DID's identity fields (PDS endpoint, verified handle,
//!    account status) — the claim already recorded `gap_epoch`; for a
//!    non-member's `private`/`authz` job, confirm owner eligibility
//!    (§6.11.1) and end `done` without listings if the DID is not an owner;
//! 2. `getLatestCommit` gives rev R;
//! 3. list each collection of the depth table with its limit, writing every
//!    selected record through the normal write path with rev R;
//! 4. reconcile every collection that was paged to the end: local rows with
//!    `rev < R` absent from the listing are confirmed with `getRecord` (at
//!    most `reconcile_confirm_limit` per job, in `hash(uri ‖ run id)` order)
//!    and deleted through the delete path with rev R;
//! 5. for `member` jobs and members' `authz` jobs, the Constellation
//!    inbound-block pass;
//! 6. the readiness (or done) transaction.
//!
//! Failures back off 1 m, 10 m, 1 h, and the job is `failed` after 5
//! attempts. A retry scheduler re-queues failed jobs of tracked DIDs every
//! `retry_every_hours`; every tracked DID gets a daily `authz` job.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::TimeDelta;
use crate::sync::{Class, HostBudgets};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{PgConnection, PgPool};
use tokio::task::JoinHandle;
use url::Url;

use super::classifier::{
    NSID_ACTOR_STATUS, NSID_AUDIENCE, NSID_BLOCK, NSID_CONTENT_VISIBILITY, NSID_FOLLOW, NSID_LABELER_SERVICE, NSID_LIKE,
    NSID_LIST, NSID_LIST_OPTOUT, NSID_LISTBLOCK, NSID_LISTITEM, NSID_NOTIFICATION_DECLARATION, NSID_POLL_POST,
    NSID_POST, NSID_POST_PRIVATE, NSID_POSTGATE, NSID_PRIVATE_BLOCK, NSID_PROFILE, NSID_REPOST, NSID_STARTER_PACK,
    NSID_THREADGATE,
};
use super::gap;
use super::jobs::Depth;
use super::writer::{Action, ApplyOptions, OpOutcome, RawOp, Writer};
use crate::config::BackfillConfig;
use crate::identity::refresher::{RefreshReason, RepoStatus, account_status, is_repo_not_found};
use crate::identity::resolver::extract_info;
use crate::identity::eligibility::{self, Confirmation};
use crate::identity::{IdentityResolver, ResolveIntent};
use crate::microcosm::{ConstellationClient, LinksPage};
use crate::net::{BodyCap, NetClient, RequestOptions};

/// `listRecords` page size.
pub const PAGE_LIMIT: u32 = 100;
/// Inbound-block rows per write batch (§6.11.7: at most 500 blocker locks).
pub const INBOUND_BATCH: usize = 500;
/// Attempts before a job is `failed`.
pub const MAX_ATTEMPTS: i32 = 5;
/// Poll interval when no job is due.
pub const IDLE_POLL: Duration = Duration::from_secs(5);
/// Listing-order detection is cached per PDS host for this long.
pub const ORDER_CACHE_TTL: Duration = Duration::from_secs(24 * 3600);
/// Re-queue delay for a job whose DID is being purged (§6.11.7).
pub const PURGING_REQUEUE_SECS: i32 = 60;
/// Metrics sampling interval.
pub const METRICS_INTERVAL: Duration = Duration::from_secs(60);
/// Daily `authz` schedule period (§6.11.5).
pub const DAILY: Duration = Duration::from_secs(24 * 3600);
/// Per-call timeout for PDS calls.
pub const PDS_TIMEOUT: Duration = Duration::from_secs(10);

pub const METRIC_JOBS: &str = "backfill_jobs";
pub const METRIC_MEMBERS_NOT_READY: &str = "members_not_ready_total";
/// An authorization record failed every deterministic attempt (§5.10).
pub const METRIC_AUTHZ_STUCK: &str = "authz_backfill_stuck_total";

/// Retry delay after the `attempt`-th consecutive failure (1-based).
#[must_use]
pub fn backoff(attempt: i32) -> TimeDelta {
    match attempt {
        ..=1 => TimeDelta::minutes(1),
        2 => TimeDelta::minutes(10),
        _ => TimeDelta::hours(1),
    }
}

/// How many records of a collection a depth takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// The newest N by rkey.
    Top(usize),
    /// To the end, up to `full_listing_ceiling`.
    All,
}

/// One row of the depth table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionPlan {
    pub nsid: &'static str,
    pub limit: Limit,
    /// One of the four authorization collections (§6.11.3).
    pub authorization: bool,
}

/// A depth's work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthPlan {
    pub profile: bool,
    /// Fetch [`SELF_RECORDS`] too.
    pub self_records: bool,
    pub collections: Vec<CollectionPlan>,
    pub inbound_blocks: bool,
}

const fn public(nsid: &'static str, limit: Limit) -> CollectionPlan {
    CollectionPlan {
        nsid,
        limit,
        authorization: false,
    }
}

const fn authorization(nsid: &'static str) -> CollectionPlan {
    CollectionPlan {
        nsid,
        limit: Limit::All,
        authorization: true,
    }
}

const AUTHORIZATION: [CollectionPlan; 4] = [
    authorization(NSID_BLOCK),
    authorization(NSID_PRIVATE_BLOCK),
    authorization(NSID_AUDIENCE),
    authorization(NSID_POST_PRIVATE),
];

// Gates are listed after posts, with the posts' limit, and only gates of the
// job's selected posts are kept (design-v3 §3.2, §II-5.10).
const SCOPE_PUBLIC: [CollectionPlan; 6] = [
    public(NSID_POST, Limit::Top(100)),
    public(NSID_REPOST, Limit::Top(100)),
    public(NSID_THREADGATE, Limit::Top(100)),
    public(NSID_POSTGATE, Limit::Top(100)),
    public(NSID_STARTER_PACK, Limit::Top(25)),
    public(NSID_POLL_POST, Limit::Top(100)),
];

// The list collections are listed whole, up to the ceiling. A truncated
// listing only logs: they are not authorization facts.
const MEMBER_PUBLIC: [CollectionPlan; 12] = [
    public(NSID_POST, Limit::Top(1000)),
    public(NSID_REPOST, Limit::Top(1000)),
    public(NSID_FOLLOW, Limit::Top(10_000)),
    public(NSID_LIKE, Limit::Top(1000)),
    public(NSID_THREADGATE, Limit::Top(1000)),
    public(NSID_POSTGATE, Limit::Top(1000)),
    public(NSID_LIST, Limit::All),
    public(NSID_LISTITEM, Limit::All),
    public(NSID_LISTBLOCK, Limit::All),
    public(NSID_LIST_OPTOUT, Limit::All),
    public(NSID_STARTER_PACK, Limit::All),
    public(NSID_POLL_POST, Limit::Top(1000)),
];

/// The rkey-self records fetched with `getRecord`, like the profile (§3.2).
pub const SELF_RECORDS: [&str; 4] = [
    NSID_LABELER_SERVICE,
    NSID_NOTIFICATION_DECLARATION,
    NSID_ACTOR_STATUS,
    NSID_CONTENT_VISIBILITY,
];

/// The §5.10 depth table. `resync` uses the member limits for a member and
/// the scope limits otherwise ("as the DID's last depth").
#[must_use]
pub fn plan(depth: Depth, is_member: bool) -> DepthPlan {
    match depth {
        Depth::Scope => DepthPlan {
            profile: true,
            self_records: true,
            collections: SCOPE_PUBLIC.to_vec(),
            inbound_blocks: false,
        },
        Depth::Member => DepthPlan {
            profile: true,
            self_records: true,
            collections: MEMBER_PUBLIC.iter().chain(AUTHORIZATION.iter()).copied().collect(),
            inbound_blocks: true,
        },
        Depth::Private => DepthPlan {
            profile: true,
            self_records: false,
            collections: AUTHORIZATION.to_vec(),
            inbound_blocks: false,
        },
        Depth::Authz => DepthPlan {
            profile: false,
            self_records: false,
            collections: AUTHORIZATION.to_vec(),
            inbound_blocks: is_member,
        },
        Depth::Resync => DepthPlan {
            profile: true,
            self_records: true,
            collections: if is_member { MEMBER_PUBLIC.to_vec() } else { SCOPE_PUBLIC.to_vec() },
            inbound_blocks: false,
        },
    }
}

/// A record as listed or fetched from a PDS.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ListedRecord {
    pub uri: String,
    pub cid: String,
    pub value: Value,
}

impl ListedRecord {
    #[must_use]
    pub fn rkey(&self) -> &str {
        self.uri.rsplit('/').next().unwrap_or_default()
    }
}

/// One `listRecords` page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ListPage {
    #[serde(default)]
    pub records: Vec<ListedRecord>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// The listing order a PDS uses (§5.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListOrder {
    Ascending,
    Descending,
}

/// Detect the order from a first page's rkeys; `None` when undetermined
/// (at most one record, or mixed).
#[must_use]
pub fn detect_order(records: &[ListedRecord]) -> Option<ListOrder> {
    if records.len() < 2 {
        return None;
    }
    let pairs = records.windows(2).map(|w| w[0].rkey().cmp(w[1].rkey()));
    let (mut asc, mut desc) = (true, true);
    for o in pairs {
        asc &= o == Ordering::Less;
        desc &= o == Ordering::Greater;
    }
    match (asc, desc) {
        (true, false) => Some(ListOrder::Ascending),
        (false, true) => Some(ListOrder::Descending),
        _ => None,
    }
}

/// Why `prepare` produced no PDS (§6.11.1 eligibility classes).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PrepareError {
    /// A definitive answer: no PDS in the DID document, repo not found, or
    /// repo inactive.
    #[error("{0}")]
    Negative(String),
    /// No answer: resolution or transport failure, 5xx, or another status.
    #[error("{0}")]
    Failed(String),
}

/// Everything a job fetches from outside the database.
#[async_trait]
pub trait BackfillSource: Send + Sync {
    /// Upsert the DID's identity fields, confirm hosting with
    /// `getRepoStatus`, and return its PDS endpoint.
    async fn prepare(&self, did: &str) -> Result<String, PrepareError>;
    /// `getLatestCommit` rev.
    async fn latest_rev(&self, pds: &str, did: &str) -> Result<String, String>;
    async fn list_records(&self, pds: &str, did: &str, collection: &str, cursor: Option<&str>)
    -> Result<ListPage, String>;
    /// `getRecord`; `Ok(None)` only for an explicit `RecordNotFound`.
    async fn get_record(&self, pds: &str, did: &str, collection: &str, rkey: &str)
    -> Result<Option<ListedRecord>, String>;
    /// One Constellation page of `app.bsky.graph.block` records at `.subject`
    /// linking to `member`.
    async fn inbound_blocks(&self, member: &str, cursor: Option<&str>) -> Result<LinksPage, String>;
}

/// The production source: `net` for PDS calls under the host's backfill
/// bucket (§6.11.3: independent of the probe and read-fetch buckets), the
/// identity resolver, and Constellation (which does not use the PDS budget).
pub struct NetSource {
    pool: PgPool,
    resolver: IdentityResolver,
    net: NetClient,
    constellation: ConstellationClient,
    budgets: HostBudgets,
}

impl NetSource {
    #[must_use]
    pub fn new(pool: PgPool, resolver: IdentityResolver, constellation: ConstellationClient, budgets: HostBudgets) -> Self {
        let net = resolver.net().clone();
        Self {
            pool,
            resolver,
            net,
            constellation,
            budgets,
        }
    }

    async fn pds_get(&self, pds: &str, method: &str, params: &[(&str, &str)], cap: BodyCap) -> Result<crate::net::NetResponse, String> {
        let url = Url::parse_with_params(&format!("{}/xrpc/{method}", pds.trim_end_matches('/')), params)
            .map_err(|e| format!("{method} url: {e}"))?;
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        self.budgets.acquire(&host, Class::Backfill).await;
        self.net
            .get(url.as_str(), &RequestOptions::new(cap).timeout(PDS_TIMEOUT))
            .await
            .map_err(|e| format!("{method}: {e}"))
    }
}

#[derive(Deserialize)]
struct LatestCommit {
    rev: String,
}

#[derive(Deserialize)]
struct XrpcError {
    #[serde(default)]
    error: Option<String>,
}

#[async_trait]
impl BackfillSource for NetSource {
    async fn prepare(&self, did: &str) -> Result<String, PrepareError> {
        let fetched = self
            .resolver
            .refresh_now(did, ResolveIntent::AdmittedAuthor)
            .await
            .map_err(|e| PrepareError::Failed(format!("resolve: {e}")))?;
        let info = extract_info(&fetched.doc, did);
        let pds = info
            .pds_endpoint
            .clone()
            .ok_or_else(|| PrepareError::Negative("DID document names no #atproto_pds".into()))?;
        let handle = self.resolver.verify_handle(did, info.claimed_handle.as_deref()).await;
        let response = self
            .pds_get(&pds, "com.atproto.sync.getRepoStatus", &[("did", did)], BodyCap::REPO_STATUS)
            .await
            .map_err(PrepareError::Failed)?;
        let status = response.status.as_u16();
        if !response.is_success() {
            if is_repo_not_found(status, &response.body) {
                // The refresher owns deletion suspicion (§5.6).
                sqlx::query(
                    "INSERT INTO identity_refresh (did, reasons) VALUES ($1, $2) \
                     ON CONFLICT (did) DO UPDATE SET reasons = identity_refresh.reasons | EXCLUDED.reasons",
                )
                .bind(did)
                .bind(RefreshReason::ACCOUNT)
                .execute(&self.pool)
                .await
                .map_err(|e| PrepareError::Failed(e.to_string()))?;
                return Err(PrepareError::Negative("repo not found at its PDS".to_string()));
            }
            return Err(PrepareError::Failed(format!("getRepoStatus: HTTP {status}")));
        }
        let repo: RepoStatus = response
            .json()
            .map_err(|e| PrepareError::Failed(format!("getRepoStatus: {e}")))?;
        let account = account_status(&repo);
        sqlx::query(
            "INSERT INTO actors (did, pds_endpoint, handle, handle_checked_at, account_status, status_checked_at) \
             VALUES ($1, $2, $3, now(), $4, now()) \
             ON CONFLICT (did) DO UPDATE SET pds_endpoint = EXCLUDED.pds_endpoint, handle = EXCLUDED.handle, \
               handle_checked_at = now(), account_status = EXCLUDED.account_status, status_checked_at = now(), \
               deletion_suspect_at = NULL, deletion_suspect_pds = NULL",
        )
        .bind(did)
        .bind(&pds)
        .bind(handle)
        .bind(&account)
        .execute(&self.pool)
        .await
        .map_err(|e| PrepareError::Failed(e.to_string()))?;
        if !repo.active {
            return Err(PrepareError::Negative(format!("repo inactive ({account})")));
        }
        Ok(pds)
    }

    async fn latest_rev(&self, pds: &str, did: &str) -> Result<String, String> {
        let r = self
            .pds_get(pds, "com.atproto.sync.getLatestCommit", &[("did", did)], BodyCap::REPO_STATUS)
            .await?;
        if !r.is_success() {
            return Err(format!("getLatestCommit: HTTP {}", r.status.as_u16()));
        }
        let c: LatestCommit = r.json().map_err(|e| format!("getLatestCommit: {e}"))?;
        if !proto_blue::syntax::Tid::is_valid(&c.rev) {
            return Err(format!("getLatestCommit: invalid rev {:?}", c.rev));
        }
        Ok(c.rev)
    }

    async fn list_records(
        &self,
        pds: &str,
        did: &str,
        collection: &str,
        cursor: Option<&str>,
    ) -> Result<ListPage, String> {
        let limit = PAGE_LIMIT.to_string();
        let mut params = vec![("repo", did), ("collection", collection), ("limit", limit.as_str())];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let r = self
            .pds_get(pds, "com.atproto.repo.listRecords", &params, BodyCap::LIST_RECORDS)
            .await?;
        if !r.is_success() {
            return Err(format!("listRecords {collection}: HTTP {}", r.status.as_u16()));
        }
        r.json().map_err(|e| format!("listRecords {collection}: {e}"))
    }

    async fn get_record(
        &self,
        pds: &str,
        did: &str,
        collection: &str,
        rkey: &str,
    ) -> Result<Option<ListedRecord>, String> {
        let r = self
            .pds_get(
                pds,
                "com.atproto.repo.getRecord",
                &[("repo", did), ("collection", collection), ("rkey", rkey)],
                BodyCap::GET_RECORD,
            )
            .await?;
        if r.is_success() {
            return r.json().map(Some).map_err(|e| format!("getRecord: {e}"));
        }
        let error = serde_json::from_slice::<XrpcError>(&r.body).ok().and_then(|e| e.error);
        if error.as_deref() == Some("RecordNotFound") {
            return Ok(None);
        }
        Err(format!("getRecord: HTTP {} {error:?}", r.status.as_u16()))
    }

    async fn inbound_blocks(&self, member: &str, cursor: Option<&str>) -> Result<LinksPage, String> {
        self.constellation
            .links(member, NSID_BLOCK, ".subject", PAGE_LIMIT, cursor)
            .await
            .map_err(|e| format!("constellation: {e}"))
    }
}

/// A claimed job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub did: String,
    pub depth: Depth,
    pub attempts: i32,
    pub run_id: u64,
}

/// What a run found, for the final transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RunResult {
    rev: String,
    /// An authorization listing (or the inbound pass) hit the ceiling.
    ceiling_hit: bool,
    /// An authorization record could not be written.
    auth_write_failed: bool,
    /// Authorization reconcile candidates remain unconfirmed.
    auth_reconcile_incomplete: bool,
}

/// How a job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    /// Readiness set (member and/or owner).
    Ready,
    Done,
    /// Re-queued: epoch changed, purging, or reconcile incomplete.
    Requeued(&'static str),
    /// Reset or cancelled by someone else while running.
    Superseded,
    Failed(String),
}

enum Step {
    Continue,
    Superseded,
}

struct Listing {
    selected: Vec<ListedRecord>,
    /// Every listed URI when the collection was paged to the end.
    complete: Option<HashSet<String>>,
    ceiling_hit: bool,
}

/// A record ordered by rkey for the bounded top-N heap.
struct ByRkey(ListedRecord);

impl PartialEq for ByRkey {
    fn eq(&self, other: &Self) -> bool {
        self.0.rkey() == other.0.rkey()
    }
}
impl Eq for ByRkey {}
impl PartialOrd for ByRkey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ByRkey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.rkey().cmp(other.0.rkey())
    }
}

/// The public table, and its author column, of an ecosystem collection that
/// is listed and reconciled.
fn public_table(nsid: &str) -> Option<(&'static str, &'static str)> {
    Some(match nsid {
        NSID_POST => ("posts", "author_did"),
        NSID_LIKE => ("likes", "author_did"),
        NSID_REPOST => ("reposts", "author_did"),
        NSID_FOLLOW => ("follows", "author_did"),
        NSID_BLOCK => ("blocks", "author_did"),
        NSID_THREADGATE => ("threadgates", "author_did"),
        NSID_POSTGATE => ("postgates", "author_did"),
        NSID_LIST => ("lists", "creator_did"),
        NSID_LISTITEM => ("list_items", "author_did"),
        NSID_LISTBLOCK => ("list_blocks", "author_did"),
        NSID_LIST_OPTOUT => ("list_optouts", "author_did"),
        NSID_STARTER_PACK => ("starter_packs", "creator_did"),
        NSID_POLL_POST => ("nearhorizon_poll_posts", "author_did"),
        _ => return None,
    })
}

/// The worker.
#[derive(Clone)]
pub struct Backfill {
    pool: PgPool,
    writer: Writer,
    source: Arc<dyn BackfillSource>,
    settings: BackfillConfig,
    attempts: u32,
    order_cache: moka::future::Cache<String, ListOrder>,
    /// The `community` network's `pds_patterns` (owner eligibility).
    community_patterns: Arc<Vec<String>>,
}

impl Backfill {
    /// `attempts` is `firehose.deterministic_attempts`, used for each write.
    #[must_use]
    pub fn new(writer: Writer, source: Arc<dyn BackfillSource>, settings: BackfillConfig, attempts: u32) -> Self {
        Self {
            pool: writer.pool().clone(),
            writer,
            source,
            settings,
            attempts,
            order_cache: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_live(ORDER_CACHE_TTL)
                .build(),
            community_patterns: Arc::new(Vec::new()),
        }
    }

    /// Set the community PDS patterns for owner eligibility (§6.11.1).
    /// Without them no non-member is eligible (fail closed).
    #[must_use]
    pub fn with_community_patterns(mut self, patterns: Vec<String>) -> Self {
        self.community_patterns = Arc::new(patterns);
        self
    }

    /// Start the workers, the retry scheduler, the daily `authz` schedule and
    /// the metrics sampler. Jobs left `running` by a previous process are
    /// re-queued first.
    ///
    /// # Errors
    ///
    /// The startup re-queue failed.
    pub async fn spawn(self) -> Result<Vec<JoinHandle<()>>, sqlx::Error> {
        let n = sqlx::query("UPDATE backfill_jobs SET state = 'queued', updated_at = now() WHERE state = 'running'")
            .execute(&self.pool)
            .await?
            .rows_affected();
        if n > 0 {
            tracing::info!(event = "backfill_requeued_interrupted", jobs = n, "re-queued jobs left running");
        }
        let mut handles = Vec::new();
        for worker in 0..self.settings.concurrency.max(1) {
            let this = self.clone();
            handles.push(tokio::spawn(async move { this.worker_loop(worker).await }));
        }
        let this = self.clone();
        handles.push(tokio::spawn(async move { this.retry_loop().await }));
        let this = self.clone();
        handles.push(tokio::spawn(async move { this.daily_loop().await }));
        let this = self;
        handles.push(tokio::spawn(async move { this.metrics_loop().await }));
        Ok(handles)
    }

    async fn worker_loop(self, worker: u32) {
        loop {
            match self.run_once().await {
                Ok(Some(_)) => {}
                Ok(None) => tokio::time::sleep(IDLE_POLL).await,
                Err(error) => {
                    tracing::warn!(event = "backfill_claim_failed", worker, %error, "claiming a backfill job failed");
                    tokio::time::sleep(IDLE_POLL).await;
                }
            }
        }
    }

    /// Claim and run one due job, if any.
    ///
    /// # Errors
    ///
    /// Claiming or settling failed (database).
    pub async fn run_once(&self) -> Result<Option<(Job, JobOutcome)>, sqlx::Error> {
        let Some(job) = self.claim().await? else {
            return Ok(None);
        };
        let outcome = match self.run(&job).await {
            Ok(Some(result)) => self.settle(&job, &result).await?,
            Ok(None) => JobOutcome::Superseded,
            Err(error) => {
                self.fail(&job, &error).await?;
                JobOutcome::Failed(error)
            }
        };
        tracing::info!(event = "backfill_job", did = %job.did, depth = job.depth.as_str(), outcome = ?outcome, "backfill job finished");
        Ok(Some((job, outcome)))
    }

    /// Claim the next due job, recording the current gap epoch (§6.11.2).
    async fn claim(&self) -> Result<Option<Job>, sqlx::Error> {
        let row: Option<(String, String, i32)> = sqlx::query_as(
            "WITH next AS ( \
               SELECT did, depth FROM backfill_jobs WHERE state = 'queued' AND not_before <= now() \
               ORDER BY not_before LIMIT 1 FOR UPDATE SKIP LOCKED) \
             UPDATE backfill_jobs j SET state = 'running', updated_at = now(), \
               gap_epoch = COALESCE((SELECT (value #>> '{}')::bigint FROM worker_state WHERE key = $1), 0) \
             FROM next WHERE j.did = next.did AND j.depth = next.depth \
             RETURNING j.did, j.depth, j.attempts",
        )
        .bind(gap::KEY_GAP_EPOCH)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(did, depth, attempts)| {
            Depth::parse(&depth).map(|depth| Job {
                did,
                depth,
                attempts,
                run_id: rand::random(),
            })
        }))
    }

    /// Still ours: not reset by a later enqueue and not cancelled.
    async fn still_running(&self, job: &Job) -> Result<Step, String> {
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM backfill_jobs WHERE did = $1 AND depth = $2")
            .bind(&job.did)
            .bind(job.depth.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(if state.as_deref() == Some("running") { Step::Continue } else { Step::Superseded })
    }

    /// Steps 1–5. `Ok(None)` when superseded part-way.
    async fn run(&self, job: &Job) -> Result<Option<RunResult>, String> {
        let is_member = self.writer.scope().read_guard().await.is_member(&job.did);
        let prepared = self.source.prepare(&job.did).await;
        // A non-member's private/authz job is an eligibility confirmation
        // (§6.11.1). A DID that is not an owner afterwards ends `done`
        // without listings; a failed confirmation of a still-eligible owner
        // fails the job, which retries.
        if matches!(job.depth, Depth::Private | Depth::Authz) && !is_member {
            let confirmation = Confirmation::from_prepare(&prepared, &self.community_patterns);
            let owner = eligibility::apply(&self.pool, self.writer.scope(), self.writer.private(), &job.did, &confirmation)
                .await
                .map_err(|e| e.to_string())?;
            if !owner {
                tracing::info!(event = "backfill_owner_ineligible", did = %job.did, ?confirmation, "not a private owner; job ends without listings");
                return Ok(Some(RunResult::default()));
            }
        }
        let pds = prepared.map_err(|e| e.to_string())?;
        let rev = self.source.latest_rev(&pds, &job.did).await?;
        let plan = plan(job.depth, is_member);
        let mut result = RunResult {
            rev,
            ..RunResult::default()
        };
        let mut budget = self.settings.reconcile_confirm_limit;
        // rkeys of the posts this job selected: a gate is kept only when
        // its post is among them (a gate's rkey is its post's rkey).
        let mut post_rkeys: HashSet<String> = HashSet::new();
        for c in &plan.collections {
            if let Step::Superseded = self.still_running(job).await? {
                return Ok(None);
            }
            let mut listing = self.list_collection(&pds, &job.did, c).await?;
            if c.nsid == NSID_POST {
                post_rkeys = listing.selected.iter().map(|r| r.rkey().to_string()).collect();
            } else if matches!(c.nsid, NSID_THREADGATE | NSID_POSTGATE) {
                listing.selected.retain(|r| post_rkeys.contains(r.rkey()));
            }
            if listing.ceiling_hit && !c.authorization {
                tracing::warn!(event = "backfill_listing_truncated", did = %job.did, collection = c.nsid, "listing hit the ceiling; continuing");
            }
            for record in &listing.selected {
                if let Some(error) = self.write_record(&job.did, &result.rev, c.nsid, record).await?
                    && c.authorization
                {
                    result.auth_write_failed = true;
                    if job.depth.is_full_authorization() {
                        self.authz_stuck(&job.did, &record.uri, &error);
                    }
                }
            }
            if listing.ceiling_hit && c.authorization {
                result.ceiling_hit = true;
                tracing::warn!(event = "backfill_block_ceiling", did = %job.did, collection = c.nsid, "authorization listing hit the ceiling; readiness not set");
            }
            if let Some(listed) = listing.complete {
                let unconfirmed = self
                    .reconcile(&pds, job, c.nsid, &result.rev, &listed, &mut budget)
                    .await?;
                if unconfirmed > 0 && c.authorization {
                    result.auth_reconcile_incomplete = true;
                }
            }
        }
        if plan.profile {
            match self.source.get_record(&pds, &job.did, NSID_PROFILE, "self").await? {
                Some(record) => {
                    self.write_record(&job.did, &result.rev, NSID_PROFILE, &record).await?;
                }
                None => self.delete_record(&job.did, &result.rev, NSID_PROFILE, "self").await?,
            }
        }
        if plan.self_records {
            for nsid in SELF_RECORDS {
                match self.source.get_record(&pds, &job.did, nsid, "self").await? {
                    Some(record) => {
                        self.write_record(&job.did, &result.rev, nsid, &record).await?;
                    }
                    None => self.delete_record(&job.did, &result.rev, nsid, "self").await?,
                }
            }
        }
        if plan.inbound_blocks {
            match self.inbound_pass(job).await? {
                None => return Ok(None),
                Some(ceiling) => result.ceiling_hit |= ceiling,
            }
        }
        Ok(Some(result))
    }

    /// List one collection under its limit (§5.10 order detection).
    async fn list_collection(&self, pds: &str, did: &str, c: &CollectionPlan) -> Result<Listing, String> {
        let host = Url::parse(pds)
            .ok()
            .and_then(|u| u.host_str().map(str::to_ascii_lowercase))
            .unwrap_or_default();
        let ceiling = usize::try_from(self.settings.full_listing_ceiling).unwrap_or(usize::MAX);
        let mut order = self.order_cache.get(&host).await;
        let mut all: HashSet<String> = HashSet::new();
        let mut selected: Vec<ListedRecord> = Vec::new();
        let mut heap: BinaryHeap<Reverse<ByRkey>> = BinaryHeap::new();
        let mut cursor: Option<String> = None;
        let mut first = true;
        loop {
            let page = self.source.list_records(pds, did, c.nsid, cursor.as_deref()).await?;
            if first {
                first = false;
                if order.is_none()
                    && let Some(o) = detect_order(&page.records)
                {
                    self.order_cache.insert(host.clone(), o).await;
                    order = Some(o);
                }
            }
            let empty = page.records.is_empty();
            for r in page.records {
                all.insert(r.uri.clone());
                match (c.limit, order) {
                    (Limit::All, _) => selected.push(r),
                    (Limit::Top(n), Some(ListOrder::Descending)) => {
                        if selected.len() < n {
                            selected.push(r);
                        }
                    }
                    (Limit::Top(n), _) => {
                        heap.push(Reverse(ByRkey(r)));
                        if heap.len() > n {
                            heap.pop();
                        }
                    }
                }
            }
            let more = page.cursor.is_some() && !empty;
            if let (Limit::Top(n), Some(ListOrder::Descending)) = (c.limit, order)
                && selected.len() >= n
                && more
            {
                // Descending: stop at the limit. Truncated: no reconcile.
                return Ok(Listing {
                    selected,
                    complete: None,
                    ceiling_hit: false,
                });
            }
            if all.len() >= ceiling && more {
                selected.extend(heap.into_iter().map(|Reverse(ByRkey(r))| r));
                return Ok(Listing {
                    selected,
                    complete: None,
                    ceiling_hit: true,
                });
            }
            if !more {
                break;
            }
            cursor = page.cursor;
        }
        let mut top: Vec<ListedRecord> = heap.into_iter().map(|Reverse(ByRkey(r))| r).collect();
        top.sort_by(|a, b| b.rkey().cmp(a.rkey()));
        selected.extend(top);
        Ok(Listing {
            selected,
            complete: Some(all),
            ceiling_hit: false,
        })
    }

    fn options(&self) -> ApplyOptions {
        ApplyOptions {
            advance_applied_rev: false,
            dead_letter_readiness: false,
            attempts: self.attempts,
        }
    }

    /// Write one record with rev R. Returns the error when it failed
    /// deterministically after the writer's in-place attempts (it is skipped).
    async fn write_record(
        &self,
        did: &str,
        rev: &str,
        collection: &str,
        record: &ListedRecord,
    ) -> Result<Option<String>, String> {
        let expected = format!("at://{did}/{collection}/");
        if !record.uri.starts_with(&expected) {
            tracing::warn!(event = "backfill_record_skipped", did, uri = %record.uri, "listed record outside the repo/collection");
            return Ok(Some("listed record outside the repo/collection".to_string()));
        }
        let op = RawOp {
            action: Action::Create,
            collection: collection.to_string(),
            rkey: record.rkey().to_string(),
            cid: Some(record.cid.clone()),
            record: Some(record.value.clone()),
        };
        let report = self
            .writer
            .apply(did, rev, &[op], self.options())
            .await
            .map_err(|e| e.to_string())?;
        if let Some(OpOutcome::Failed(error)) = report.outcomes.first() {
            tracing::warn!(event = "backfill_record_skipped", did, uri = %record.uri, %error, "record failed deterministically");
            return Ok(Some(error.clone()));
        }
        Ok(None)
    }

    /// A readiness-capable job could not write an authorization record after
    /// every deterministic attempt: readiness can never be set until the
    /// record is removed from the PDS (§5.10). Loud by design.
    fn authz_stuck(&self, did: &str, record_uri: &str, error: &str) {
        metrics::counter!(METRIC_AUTHZ_STUCK, "did" => did.to_string()).increment(1);
        tracing::error!(
            event = "authz_stuck",
            did,
            record_uri,
            error,
            attempts = self.attempts,
            "authorization record fails deterministically; readiness stays unset until it is removed from the PDS"
        );
    }

    async fn delete_record(&self, did: &str, rev: &str, collection: &str, rkey: &str) -> Result<(), String> {
        let op = RawOp {
            action: Action::Delete,
            collection: collection.to_string(),
            rkey: rkey.to_string(),
            cid: None,
            record: None,
        };
        self.writer
            .apply(did, rev, &[op], self.options())
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Local URIs of `did` in `nsid` with `rev < R` (reconcile candidates
    /// before the listing filter).
    async fn local_uris(&self, did: &str, nsid: &str, rev: &str) -> Result<Vec<String>, sqlx::Error> {
        if let Some((table, author)) = public_table(nsid) {
            return sqlx::query_scalar(&format!("SELECT uri FROM {table} WHERE {author} = $1 AND rev < $2"))
                .bind(did)
                .bind(rev)
                .fetch_all(&self.pool)
                .await;
        }
        let mut conn = self.pool.acquire().await?;
        self.writer.private().local_uris(&mut conn, did, nsid, rev).await
    }

    /// Reconcile one fully listed collection (§5.10). Returns the number of
    /// candidates left unconfirmed when the per-job bound ran out.
    async fn reconcile(
        &self,
        pds: &str,
        job: &Job,
        nsid: &str,
        rev: &str,
        listed: &HashSet<String>,
        budget: &mut u32,
    ) -> Result<usize, String> {
        let mut candidates: Vec<String> = self
            .local_uris(&job.did, nsid, rev)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|u| !listed.contains(u))
            .collect();
        if candidates.is_empty() {
            return Ok(0);
        }
        // Rotating order: hash(uri ‖ run id).
        let run = job.run_id.to_le_bytes();
        candidates.sort_by_cached_key(|u| {
            let mut h = blake3::Hasher::new();
            h.update(u.as_bytes());
            h.update(&run);
            *h.finalize().as_bytes()
        });
        let total = candidates.len();
        for (i, uri) in candidates.iter().enumerate() {
            if *budget == 0 {
                tracing::info!(event = "backfill_reconcile_deferred", did = %job.did, collection = nsid, remaining = total - i, "reconcile bound reached");
                return Ok(total - i);
            }
            *budget -= 1;
            let rkey = uri.rsplit('/').next().unwrap_or_default();
            if self.source.get_record(pds, &job.did, nsid, rkey).await?.is_none() {
                self.delete_record(&job.did, rev, nsid, rkey).await?;
                tracing::info!(event = "backfill_reconcile_deleted", did = %job.did, %uri, "confirmed deletion reconciled");
            }
        }
        Ok(0)
    }

    /// The Constellation inbound-block pass. `Ok(None)` when superseded;
    /// otherwise whether the ceiling was hit.
    async fn inbound_pass(&self, job: &Job) -> Result<Option<bool>, String> {
        let ceiling = self.settings.full_listing_ceiling;
        let mut seen: u64 = 0;
        let mut cursor: Option<String> = None;
        loop {
            if let Step::Superseded = self.still_running(job).await? {
                return Ok(None);
            }
            let page = self.source.inbound_blocks(&job.did, cursor.as_deref()).await?;
            let blockers: Vec<(String, String)> = page
                .linking_records
                .iter()
                .filter(|r| r.collection == NSID_BLOCK)
                .map(|r| (r.did.clone(), r.rkey.clone()))
                .collect();
            seen += page.linking_records.len() as u64;
            for chunk in blockers.chunks(INBOUND_BATCH) {
                self.writer
                    .apply_inbound_blocks(&job.did, chunk)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            let more = page.cursor.is_some() && !page.linking_records.is_empty();
            if !more {
                return Ok(Some(false));
            }
            if seen >= ceiling {
                tracing::warn!(event = "backfill_block_ceiling", did = %job.did, source = "constellation", "inbound-block pass hit the ceiling; readiness not set");
                return Ok(Some(true));
            }
            cursor = page.cursor;
        }
    }

    /// Step 6: the readiness or done transaction.
    async fn settle(&self, job: &Job, result: &RunResult) -> Result<JobOutcome, sqlx::Error> {
        let readiness_capable = job.depth.is_full_authorization();
        if readiness_capable && result.auth_reconcile_incomplete {
            // §5.10 #182: re-queue immediately, readiness and applied_rev untouched.
            let n = sqlx::query(
                "UPDATE backfill_jobs SET state = 'queued', not_before = now(), updated_at = now() \
                 WHERE did = $1 AND depth = $2 AND state = 'running'",
            )
            .bind(&job.did)
            .bind(job.depth.as_str())
            .execute(&self.pool)
            .await?
            .rows_affected();
            return Ok(if n > 0 { JobOutcome::Requeued("reconcile_incomplete") } else { JobOutcome::Superseded });
        }
        let may_set_readiness = readiness_capable && !result.ceiling_hit && !result.auth_write_failed;

        // Read guard before the connection (§6.11.7).
        let guard = self.writer.scope().read_guard().await;
        let is_member = guard.is_member(&job.did);
        let is_owner = guard.is_owner(&job.did);
        let tracked = guard.is_tracked(&job.did);
        let mut tx = self.pool.begin().await?;
        let row: Option<(String, Option<i64>)> =
            sqlx::query_as("SELECT state, gap_epoch FROM backfill_jobs WHERE did = $1 AND depth = $2 FOR UPDATE")
                .bind(&job.did)
                .bind(job.depth.as_str())
                .fetch_optional(&mut *tx)
                .await?;
        let Some((state, recorded_epoch)) = row else {
            return Ok(JobOutcome::Superseded);
        };
        if state != "running" {
            return Ok(JobOutcome::Superseded);
        }
        let outcome = if may_set_readiness {
            let epoch = gap::current_epoch(&mut tx).await?;
            let purging: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)")
                .bind(&job.did)
                .fetch_one(&mut *tx)
                .await?;
            if purging {
                requeue(&mut tx, job, PURGING_REQUEUE_SECS).await?;
                JobOutcome::Requeued("purging")
            } else if recorded_epoch != Some(epoch) {
                requeue(&mut tx, job, 0).await?;
                JobOutcome::Requeued("gap_epoch_changed")
            } else {
                let mut ready = false;
                if is_member && matches!(job.depth, Depth::Member | Depth::Authz) {
                    sqlx::query(
                        "UPDATE members SET ready_at = now(), first_ready_at = COALESCE(first_ready_at, now()) \
                         WHERE did = $1",
                    )
                    .bind(&job.did)
                    .execute(&mut *tx)
                    .await?;
                    ready = true;
                }
                if is_owner && matches!(job.depth, Depth::Private | Depth::Authz) {
                    self.writer.private().mark_owner_ready(&mut tx, &job.did).await?;
                    ready = true;
                }
                if tracked {
                    sqlx::query(
                        "INSERT INTO sync_state (did, applied_rev, last_resync_at) VALUES ($1, $2, now()) \
                         ON CONFLICT (did) DO UPDATE SET \
                           applied_rev = GREATEST(sync_state.applied_rev, EXCLUDED.applied_rev), \
                           last_resync_at = now()",
                    )
                    .bind(&job.did)
                    .bind(&result.rev)
                    .execute(&mut *tx)
                    .await?;
                }
                mark_done(&mut tx, job).await?;
                if ready { JobOutcome::Ready } else { JobOutcome::Done }
            }
        } else {
            if tracked {
                touch_last_resync(&mut tx, &job.did).await?;
            }
            mark_done(&mut tx, job).await?;
            JobOutcome::Done
        };
        tx.commit().await?;
        drop(guard);
        Ok(outcome)
    }

    async fn fail(&self, job: &Job, error: &str) -> Result<(), sqlx::Error> {
        let attempt = job.attempts + 1;
        let state = if attempt >= MAX_ATTEMPTS { "failed" } else { "queued" };
        let delay = backoff(attempt).num_seconds();
        let error: String = error.chars().take(512).collect();
        sqlx::query(
            "UPDATE backfill_jobs SET attempts = $3, state = $4, last_error = $5, updated_at = now(), \
               not_before = now() + make_interval(secs => $6) \
             WHERE did = $1 AND depth = $2 AND state = 'running'",
        )
        .bind(&job.did)
        .bind(job.depth.as_str())
        .bind(attempt)
        .bind(state)
        .bind(&error)
        .bind(delay as f64)
        .execute(&self.pool)
        .await?;
        tracing::warn!(event = "backfill_job_failed", did = %job.did, depth = job.depth.as_str(), attempt, state, %error, "backfill job failed");
        Ok(())
    }

    /// Every `retry_every_hours`: re-queue failed `member`, `private`, `authz`
    /// and `resync` jobs of DIDs still tracked, resetting attempts.
    async fn retry_loop(self) {
        let every = Duration::from_secs(u64::from(self.settings.retry_every_hours.max(1)) * 3600);
        let mut tick = tokio::time::interval(every);
        tick.tick().await;
        loop {
            tick.tick().await;
            let tracked = self.writer.scope().read_guard().await.tracked();
            let result = sqlx::query(
                "UPDATE backfill_jobs SET state = 'queued', attempts = 0, not_before = now(), updated_at = now() \
                 WHERE state = 'failed' AND depth IN ('member', 'private', 'authz', 'resync') AND did = ANY($1)",
            )
            .bind(&tracked)
            .execute(&self.pool)
            .await;
            match result {
                Ok(r) => tracing::info!(event = "backfill_retry_scheduled", jobs = r.rows_affected(), "failed jobs re-queued"),
                Err(error) => tracing::warn!(event = "backfill_retry_failed", %error, "re-queueing failed jobs failed"),
            }
        }
    }

    /// Daily `authz` for every tracked DID, spread over the day by DID hash
    /// (§6.11.5). A job already queued or running is left alone, so a gap or
    /// dead-letter job is never delayed.
    async fn daily_loop(self) {
        let mut tick = tokio::time::interval(DAILY);
        loop {
            tick.tick().await;
            let tracked = self.writer.scope().read_guard().await.tracked();
            let offsets: Vec<f64> = tracked.iter().map(|d| daily_offset_secs(d)).collect();
            let result = sqlx::query(
                "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, updated_at) \
                 SELECT d, 'authz', 'queued', 0, now() + make_interval(secs => o), now() \
                 FROM unnest($1::text[], $2::float8[]) AS t(d, o) \
                 ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, \
                   not_before = EXCLUDED.not_before, last_error = NULL, updated_at = now() \
                 WHERE backfill_jobs.state NOT IN ('queued', 'running')",
            )
            .bind(&tracked)
            .bind(&offsets)
            .execute(&self.pool)
            .await;
            if let Err(error) = result {
                tracing::warn!(event = "backfill_daily_authz_failed", %error, "scheduling daily authz jobs failed");
            }
        }
    }

    async fn metrics_loop(self) {
        let mut tick = tokio::time::interval(METRICS_INTERVAL);
        loop {
            tick.tick().await;
            if let Err(error) = self.sample_metrics().await {
                tracing::debug!(event = "backfill_metrics_failed", %error, "backfill metrics sample failed");
            }
        }
    }

    async fn sample_metrics(&self) -> Result<(), sqlx::Error> {
        let rows: Vec<(String, String, i64)> =
            sqlx::query_as("SELECT depth, state, count(*) FROM backfill_jobs GROUP BY depth, state")
                .fetch_all(&self.pool)
                .await?;
        for depth in ["member", "scope", "private", "authz", "resync"] {
            for state in ["queued", "running", "done", "failed", "cancelled"] {
                let n = rows
                    .iter()
                    .find(|(d, s, _)| d == depth && s == state)
                    .map_or(0, |(_, _, n)| *n);
                #[allow(clippy::cast_precision_loss)]
                metrics::gauge!(METRIC_JOBS, "depth" => depth, "state" => state).set(n as f64);
            }
        }
        let not_ready: i64 = sqlx::query_scalar("SELECT count(*) FROM members WHERE ready_at IS NULL")
            .fetch_one(&self.pool)
            .await?;
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_MEMBERS_NOT_READY).set(not_ready as f64);
        Ok(())
    }
}

/// Seconds into the day for a DID's daily job.
fn daily_offset_secs(did: &str) -> f64 {
    let h = blake3::hash(did.as_bytes());
    let mut b = [0u8; 8];
    b.copy_from_slice(&h.as_bytes()[..8]);
    #[allow(clippy::cast_precision_loss)]
    let secs = (u64::from_le_bytes(b) % 86_400) as f64;
    secs
}

async fn requeue(conn: &mut PgConnection, job: &Job, delay_secs: i32) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE backfill_jobs SET state = 'queued', not_before = now() + make_interval(secs => $3), updated_at = now() \
         WHERE did = $1 AND depth = $2",
    )
    .bind(&job.did)
    .bind(job.depth.as_str())
    .bind(f64::from(delay_secs))
    .execute(conn)
    .await?;
    Ok(())
}

async fn mark_done(conn: &mut PgConnection, job: &Job) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE backfill_jobs SET state = 'done', attempts = 0, last_error = NULL, updated_at = now() \
         WHERE did = $1 AND depth = $2",
    )
    .bind(&job.did)
    .bind(job.depth.as_str())
    .execute(conn)
    .await?;
    Ok(())
}

async fn touch_last_resync(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sync_state (did, last_resync_at) VALUES ($1, now()) \
         ON CONFLICT (did) DO UPDATE SET last_resync_at = now()",
    )
    .bind(did)
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::aggregations::{ScopeSets, ScopeState};
    use crate::config::ScopePolicy;
    use crate::firehose::jobs;
    use crate::microcosm::LinkingRecord;
    use crate::private_tier::DisabledPrivateTier;
    use crate::test_support::fresh_db;

    fn rec(did: &str, collection: &str, rkey: &str, value: Value) -> ListedRecord {
        ListedRecord {
            uri: format!("at://{did}/{collection}/{rkey}"),
            cid: format!("bafy{rkey}"),
            value,
        }
    }

    #[test]
    fn depth_table() {
        let scope = plan(Depth::Scope, false);
        assert!(scope.profile && scope.self_records && !scope.inbound_blocks);
        assert_eq!(scope.collections.iter().map(|c| (c.nsid, c.limit)).collect::<Vec<_>>(), [
            (NSID_POST, Limit::Top(100)),
            (NSID_REPOST, Limit::Top(100)),
            (NSID_THREADGATE, Limit::Top(100)),
            (NSID_POSTGATE, Limit::Top(100)),
            (NSID_STARTER_PACK, Limit::Top(25)),
            (NSID_POLL_POST, Limit::Top(100)),
        ]);
        let member = plan(Depth::Member, true);
        assert!(member.inbound_blocks && member.self_records);
        assert_eq!(member.collections.len(), 16);
        assert_eq!(member.collections.iter().filter(|c| c.authorization).count(), 4);
        assert!(member.collections.iter().filter(|c| c.authorization).all(|c| c.limit == Limit::All));
        // A member's list collections are listed whole.
        for nsid in [NSID_LIST, NSID_LISTITEM, NSID_LISTBLOCK, NSID_LIST_OPTOUT, NSID_STARTER_PACK] {
            assert_eq!(member.collections.iter().find(|c| c.nsid == nsid).map(|c| c.limit), Some(Limit::All), "{nsid}");
        }
        // Gates come after posts, with the posts' limit.
        let position = |nsid: &str| member.collections.iter().position(|c| c.nsid == nsid).expect("listed");
        assert!(position(NSID_POST) < position(NSID_THREADGATE) && position(NSID_POST) < position(NSID_POSTGATE));
        let authz = plan(Depth::Authz, false);
        assert!(!authz.profile && !authz.self_records && !authz.inbound_blocks);
        assert!(plan(Depth::Authz, true).inbound_blocks);
        assert_eq!(plan(Depth::Resync, true).collections.len(), 12);
        assert_eq!(plan(Depth::Resync, false).collections.len(), 6);
        // The new collections are no authorization facts: `private` and
        // `authz` never list them.
        let private = plan(Depth::Private, false);
        assert!(private.collections.iter().all(|c| c.authorization) && !private.self_records);
        // Every listed public collection has a table to reconcile against.
        for c in member.collections.iter().filter(|c| !c.authorization) {
            assert!(public_table(c.nsid).is_some(), "{}", c.nsid);
        }
    }

    #[test]
    fn order_detection_and_backoff() {
        let r = |k: &str| rec("did:plc:a", NSID_POST, k, json!({}));
        assert_eq!(detect_order(&[r("3kb"), r("3ka")]), Some(ListOrder::Descending));
        assert_eq!(detect_order(&[r("3ka"), r("3kb"), r("3kc")]), Some(ListOrder::Ascending));
        assert_eq!(detect_order(&[r("3ka")]), None);
        assert_eq!(detect_order(&[r("3ka"), r("3kc"), r("3kb")]), None);
        assert_eq!(backoff(1), TimeDelta::minutes(1));
        assert_eq!(backoff(2), TimeDelta::minutes(10));
        assert_eq!(backoff(4), TimeDelta::hours(1));
        assert!((0.0..86_400.0).contains(&daily_offset_secs("did:plc:a")));
    }

    /// An in-memory PDS and Constellation.
    #[derive(Default)]
    struct FakeSource {
        rev: String,
        /// collection → listed records, in listing order.
        listings: HashMap<String, Vec<ListedRecord>>,
        /// URIs that exist but are omitted from listings.
        hidden: HashSet<String>,
        inbound: Vec<LinkingRecord>,
        get_record_calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl BackfillSource for FakeSource {
        async fn prepare(&self, _did: &str) -> Result<String, PrepareError> {
            Ok("https://pds.example".to_string())
        }
        async fn latest_rev(&self, _pds: &str, _did: &str) -> Result<String, String> {
            Ok(self.rev.clone())
        }
        async fn list_records(
            &self,
            _pds: &str,
            _did: &str,
            collection: &str,
            cursor: Option<&str>,
        ) -> Result<ListPage, String> {
            let all = self.listings.get(collection).cloned().unwrap_or_default();
            let start: usize = cursor.map_or(0, |c| c.parse().expect("cursor"));
            let end = (start + 2).min(all.len());
            Ok(ListPage {
                records: all[start..end].to_vec(),
                cursor: (end < all.len()).then(|| end.to_string()),
            })
        }
        async fn get_record(
            &self,
            _pds: &str,
            did: &str,
            collection: &str,
            rkey: &str,
        ) -> Result<Option<ListedRecord>, String> {
            let uri = format!("at://{did}/{collection}/{rkey}");
            self.get_record_calls.lock().expect("lock").push(uri.clone());
            let listed = self
                .listings
                .get(collection)
                .and_then(|l| l.iter().find(|r| r.uri == uri).cloned());
            if let Some(r) = listed {
                return Ok(Some(r));
            }
            if self.hidden.contains(&uri) {
                return Ok(Some(rec(did, collection, rkey, json!({"text": "hidden"}))));
            }
            Ok(None)
        }
        async fn inbound_blocks(&self, _member: &str, _cursor: Option<&str>) -> Result<LinksPage, String> {
            Ok(LinksPage {
                total: self.inbound.len() as u64,
                linking_records: self.inbound.clone(),
                cursor: None,
            })
        }
    }

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    fn state(member: bool) -> ScopeState {
        let mut sets = ScopeSets::default();
        sets.scope.insert(DID.into());
        if member {
            sets.members.insert(DID.into());
        }
        ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000)
    }

    async fn insert_post(pool: &PgPool, rkey: &str, rev: &str) {
        sqlx::query(
            "INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at) \
             VALUES ($1, $2, $3, 'bafyold', $4, '{}'::jsonb, now())",
        )
        .bind(format!("at://{DID}/{NSID_POST}/{rkey}"))
        .bind(DID)
        .bind(rkey)
        .bind(rev)
        .execute(pool)
        .await
        .expect("insert post");
    }

    async fn post_uris(pool: &PgPool) -> Vec<String> {
        sqlx::query_scalar("SELECT uri FROM posts ORDER BY uri")
            .fetch_all(pool)
            .await
            .expect("posts")
    }

    /// design-v3 §3.2, §II-5.10: the v3 collections are backfilled.
    #[tokio::test]
    async fn the_v3_collections_are_listed_fetched_and_reconciled() {
        let Some(db) = fresh_db("backfill_v3_collections").await else {
            return;
        };
        let p = &db.pool;
        let post = |rkey: &str| format!("at://{DID}/{NSID_POST}/{rkey}");
        let list = format!("at://{DID}/{NSID_LIST}/3klist");
        let mut source = FakeSource {
            rev: "3kzzzzzzzzzzz".to_string(),
            ..FakeSource::default()
        };
        let mut listing = |nsid: &str, records: Vec<ListedRecord>| {
            source.listings.insert(nsid.to_string(), records);
        };
        listing(NSID_POST, vec![rec(DID, NSID_POST, "3kpost", json!({"text": "gated"}))]);
        // One gate for the selected post, one for a post the job did not
        // select: only the first is kept.
        listing(
            NSID_THREADGATE,
            vec![
                rec(DID, NSID_THREADGATE, "3kpost", json!({"post": post("3kpost"), "allow": []})),
                rec(DID, NSID_THREADGATE, "3kother", json!({"post": post("3kother")})),
            ],
        );
        listing(NSID_LIST, vec![rec(DID, NSID_LIST, "3klist", json!({"name": "Mods", "purpose": "app.bsky.graph.defs#modlist"}))]);
        listing(
            NSID_LISTITEM,
            vec![
                rec(DID, NSID_LISTITEM, "3ka", json!({"list": list, "subject": "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"})),
                rec(DID, NSID_LISTITEM, "3kb", json!({"list": list, "subject": "did:plc:cccccccccccccccccccccccc"})),
                rec(DID, NSID_LISTITEM, "3kc", json!({"list": list, "subject": "did:plc:dddddddddddddddddddddddd"})),
            ],
        );
        listing(NSID_LISTBLOCK, vec![rec(DID, NSID_LISTBLOCK, "3kblock", json!({"subject": "at://did:plc:x/app.bsky.graph.list/1"}))]);
        listing(NSID_STARTER_PACK, vec![rec(DID, NSID_STARTER_PACK, "3kpack", json!({"name": "Start", "list": list}))]);
        // A self record is fetched, not listed.
        listing(NSID_ACTOR_STATUS, vec![rec(DID, NSID_ACTOR_STATUS, "self", json!({"status": "app.bsky.actor.status#live"}))]);
        // A list item stored earlier that the repo no longer has, and a
        // declaration the repo no longer has.
        for sql in [
            "INSERT INTO list_items (uri, author_did, rkey, cid, rev, list_uri, subject_did, created_at) \
             VALUES ('at://' || $1 || '/app.bsky.graph.listitem/3kgone', $1, '3kgone', 'c', '3kaaaaaaaaaaa', 'l', 'did:plc:x', now())",
            "INSERT INTO actor_self_records (uri, did, collection, cid, rev, record) \
             VALUES ('at://' || $1 || '/app.bsky.notification.declaration/self', $1, 'app.bsky.notification.declaration', 'c', '3kaaaaaaaaaaa', '{}')",
        ] {
            sqlx::query(sql).bind(DID).execute(p).await.expect(sql);
        }
        let writer = Writer::new(p.clone(), state(true), Arc::new(DisabledPrivateTier));
        let backfill = Backfill::new(writer, Arc::new(source), BackfillConfig::default(), 5);
        let mut conn = p.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Resync).await.expect("enqueue");
        drop(conn);
        let (_, outcome) = backfill.run_once().await.expect("run").expect("a job");
        assert_eq!(outcome, JobOutcome::Done);

        let uris = |table: &'static str| async move {
            let v: Vec<String> = sqlx::query_scalar(&format!("SELECT uri FROM {table} ORDER BY uri")).fetch_all(p).await.expect(table);
            v
        };
        assert_eq!(uris("threadgates").await, [format!("at://{DID}/{NSID_THREADGATE}/3kpost")]);
        assert_eq!(uris("lists").await, std::slice::from_ref(&list));
        // The three listed items, and the one the repo no longer has is gone.
        assert_eq!(uris("list_items").await.len(), 3);
        assert_eq!(uris("list_blocks").await.len(), 1);
        assert_eq!(uris("starter_packs").await.len(), 1);
        assert_eq!(uris("actor_self_records").await, [format!("at://{DID}/{NSID_ACTOR_STATUS}/self")]);
        db.drop_db().await;
    }

    #[tokio::test]
    async fn reconcile_deletes_a_listed_absent_record_only_after_get_record_confirms() {
        let Some(db) = fresh_db("backfill_reconcile").await else {
            return;
        };
        let rev = "3kzzzzzzzzzzz";
        // Local rows: one still listed, one gone at the PDS, one the PDS omits
        // from listings but still serves, and one newer than R.
        insert_post(&db.pool, "3kaaaaaaaaaaa", "3kaaaaaaaaaaa").await;
        insert_post(&db.pool, "3kbbbbbbbbbbb", "3kbbbbbbbbbbb").await;
        insert_post(&db.pool, "3kccccccccccc", "3kccccccccccc").await;
        insert_post(&db.pool, "3kddddddddddd", "3lzzzzzzzzzzz").await;
        let mut source = FakeSource {
            rev: rev.to_string(),
            ..FakeSource::default()
        };
        source.listings.insert(
            NSID_POST.to_string(),
            vec![
                rec(DID, NSID_POST, "3kaaaaaaaaaaa", json!({"text": "kept"})),
                rec(DID, NSID_POST, "3keeeeeeeeeee", json!({"text": "new"})),
            ],
        );
        source.hidden.insert(format!("at://{DID}/{NSID_POST}/3kccccccccccc"));
        let source = Arc::new(source);
        let writer = Writer::new(db.pool.clone(), state(false), Arc::new(DisabledPrivateTier));
        let backfill = Backfill::new(writer, source.clone(), BackfillConfig::default(), 5);

        let mut conn = db.pool.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Resync).await.expect("enqueue");
        drop(conn);
        let (job, outcome) = backfill.run_once().await.expect("run").expect("a job");
        assert_eq!(job.depth, Depth::Resync);
        assert_eq!(outcome, JobOutcome::Done);

        assert_eq!(
            post_uris(&db.pool).await,
            [
                format!("at://{DID}/{NSID_POST}/3kaaaaaaaaaaa"),
                format!("at://{DID}/{NSID_POST}/3kccccccccccc"),
                format!("at://{DID}/{NSID_POST}/3kddddddddddd"),
                format!("at://{DID}/{NSID_POST}/3keeeeeeeeeee"),
            ]
        );
        // Only the listed-absent rows with rev < R were confirmed.
        let mut calls = source.get_record_calls.lock().expect("lock").clone();
        calls.retain(|u| u.contains(NSID_POST));
        calls.sort();
        assert_eq!(
            calls,
            [
                format!("at://{DID}/{NSID_POST}/3kbbbbbbbbbbb"),
                format!("at://{DID}/{NSID_POST}/3kccccccccccc"),
            ]
        );
        // The confirmed deletion went through the delete path with rev R.
        let tomb: String = sqlx::query_scalar("SELECT rev FROM record_tombstones WHERE uri = $1")
            .bind(format!("at://{DID}/{NSID_POST}/3kbbbbbbbbbbb"))
            .fetch_one(&db.pool)
            .await
            .expect("tombstone");
        assert_eq!(tomb, rev);
        let state: String = sqlx::query_scalar("SELECT state FROM backfill_jobs WHERE did = $1")
            .bind(DID)
            .fetch_one(&db.pool)
            .await
            .expect("job");
        assert_eq!(state, "done");
        db.drop_db().await;
    }

    #[tokio::test]
    async fn member_job_sets_readiness_only_with_an_unchanged_epoch() {
        let Some(db) = fresh_db("backfill_member_readiness").await else {
            return;
        };
        sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')")
            .bind(DID)
            .execute(&db.pool)
            .await
            .expect("member");
        let blocker = "did:plc:cccccccccccccccccccccccc";
        let source = Arc::new(FakeSource {
            rev: "3kzzzzzzzzzzz".to_string(),
            inbound: vec![LinkingRecord {
                did: blocker.to_string(),
                collection: NSID_BLOCK.to_string(),
                rkey: "3kaaaaaaaaaaa".to_string(),
            }],
            ..FakeSource::default()
        });
        let scope = state(true);
        let writer = Writer::new(db.pool.clone(), scope, Arc::new(DisabledPrivateTier));
        let backfill = Backfill::new(writer, source, BackfillConfig::default(), 5);

        // A gap between claim and settle: the epoch check fails, re-queue.
        let mut conn = db.pool.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Member).await.expect("enqueue");
        drop(conn);
        let job = backfill.claim().await.expect("claim").expect("job");
        let result = backfill.run(&job).await.expect("run").expect("not superseded");
        sqlx::query("INSERT INTO worker_state (key, value) VALUES ('gap_epoch', '7'::jsonb)")
            .execute(&db.pool)
            .await
            .expect("epoch");
        assert_eq!(
            backfill.settle(&job, &result).await.expect("settle"),
            JobOutcome::Requeued("gap_epoch_changed")
        );
        let ready: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT ready_at FROM members WHERE did = $1")
                .bind(DID)
                .fetch_one(&db.pool)
                .await
                .expect("member");
        assert_eq!(ready, None);

        // The re-run sees the new epoch throughout and sets readiness.
        let (_, outcome) = backfill.run_once().await.expect("run").expect("job");
        assert_eq!(outcome, JobOutcome::Ready);
        let (ready, first): (Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as("SELECT ready_at, first_ready_at FROM members WHERE did = $1")
                .bind(DID)
                .fetch_one(&db.pool)
                .await
                .expect("member");
        assert!(ready.is_some() && first.is_some());
        let applied: String = sqlx::query_scalar("SELECT applied_rev FROM sync_state WHERE did = $1")
            .bind(DID)
            .fetch_one(&db.pool)
            .await
            .expect("sync_state");
        assert_eq!(applied, "3kzzzzzzzzzzz");
        // The inbound block was written with the sentinel rev.
        let (author, subject, rev): (String, String, String) =
            sqlx::query_as("SELECT author_did, subject_did, rev FROM blocks")
                .fetch_one(&db.pool)
                .await
                .expect("inbound block");
        assert_eq!((author.as_str(), subject.as_str(), rev.as_str()), (blocker, DID, crate::firehose::writer::SENTINEL_REV));
        db.drop_db().await;
    }

    #[tokio::test]
    async fn a_bad_authorization_record_keeps_readiness_unset() {
        let Some(db) = fresh_db("backfill_authz_stuck").await else {
            return;
        };
        sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')")
            .bind(DID)
            .execute(&db.pool)
            .await
            .expect("member");
        let mut source = FakeSource {
            rev: "3kzzzzzzzzzzz".to_string(),
            ..FakeSource::default()
        };
        source.listings.insert(
            NSID_BLOCK.to_string(),
            vec![
                rec(DID, NSID_BLOCK, "3kaaaaaaaaaaa", json!({"subject": "not a did"})),
                rec(DID, NSID_BLOCK, "3kbbbbbbbbbbb", json!({"subject": "did:plc:cccccccccccccccccccccccc"})),
            ],
        );
        let writer = Writer::new(db.pool.clone(), state(true), Arc::new(DisabledPrivateTier));
        let backfill = Backfill::new(writer, Arc::new(source), BackfillConfig::default(), 5);
        let mut conn = db.pool.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Authz).await.expect("enqueue");
        drop(conn);
        let (_, outcome) = backfill.run_once().await.expect("run").expect("job");
        // Fail-closed: the job completes, the good record is written, and
        // readiness and applied_rev stay unset.
        assert_eq!(outcome, JobOutcome::Done);
        let blocks: i64 = sqlx::query_scalar("SELECT count(*) FROM blocks")
            .fetch_one(&db.pool)
            .await
            .expect("blocks");
        assert_eq!(blocks, 1);
        let ready: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT ready_at FROM members WHERE did = $1")
                .bind(DID)
                .fetch_one(&db.pool)
                .await
                .expect("member");
        assert_eq!(ready, None);
        let applied: Option<String> = sqlx::query_scalar("SELECT applied_rev FROM sync_state WHERE did = $1")
            .bind(DID)
            .fetch_optional(&db.pool)
            .await
            .expect("sync_state")
            .flatten();
        assert_eq!(applied, None);
        db.drop_db().await;
    }

    #[tokio::test]
    async fn failures_back_off_then_fail() {
        struct Down;
        #[async_trait]
        impl BackfillSource for Down {
            async fn prepare(&self, _did: &str) -> Result<String, PrepareError> {
                Err(PrepareError::Failed("pds down".to_string()))
            }
            async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
                unreachable!()
            }
            async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
                unreachable!()
            }
            async fn get_record(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<ListedRecord>, String> {
                unreachable!()
            }
            async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
                unreachable!()
            }
        }
        let Some(db) = fresh_db("backfill_failures").await else {
            return;
        };
        let writer = Writer::new(db.pool.clone(), state(false), Arc::new(DisabledPrivateTier));
        let backfill = Backfill::new(writer, Arc::new(Down), BackfillConfig::default(), 5);
        let mut conn = db.pool.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Scope).await.expect("enqueue");
        for attempt in 1..=MAX_ATTEMPTS {
            // Make the job due again.
            sqlx::query("UPDATE backfill_jobs SET not_before = now()")
                .execute(&mut *conn)
                .await
                .expect("due");
            let (_, outcome) = backfill.run_once().await.expect("run").expect("job");
            assert!(matches!(outcome, JobOutcome::Failed(_)));
            let (state, attempts): (String, i32) = sqlx::query_as("SELECT state, attempts FROM backfill_jobs")
                .fetch_one(&mut *conn)
                .await
                .expect("job");
            assert_eq!(attempts, attempt);
            assert_eq!(state, if attempt == MAX_ATTEMPTS { "failed" } else { "queued" });
        }
        drop(conn);
        db.drop_db().await;
    }
}
