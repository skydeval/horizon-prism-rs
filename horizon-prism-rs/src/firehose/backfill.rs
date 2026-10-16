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
//! 5. for an owner of a `following` audience, after the authorization
//!    listings, the follows pass (following-audience design §3.5, S10), its
//!    counter read before the first listing; for `member` jobs and members'
//!    `authz` jobs, the Constellation inbound-block pass;
//! 6. the readiness (or done) transaction, which takes the gap lock first
//!    and writes the follow-verification flag beside `applied_rev`.
//!
//! A claim carries the row's `generation` (design-v4 §II-5.10
//! "Generations"); every statement the run issues on its row is conditioned
//! on it, and every reset of the row by anyone else moves it on, so a run
//! whose row was reset meanwhile settles as superseded and writes nothing.
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
use super::follows_pass::{self, Fetched, FollowsLister, PassOutcome, PassSettings};
use super::gap;
use super::jobs::{self, Depth};
use super::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer, read_follows_clears};
use crate::config::BackfillConfig;
use crate::farsight::Farsight;
use crate::indexing::PublicMode;
use crate::identity::refresher::{RefreshReason, RepoStatus, account_status, is_repo_not_found};
use crate::identity::resolver::extract_info;
use crate::identity::eligibility::{self, Confirmation};
use crate::identity::{IdentityResolver, ResolveIntent};
use crate::microcosm::{ConstellationClient, LinksPage};
use crate::net::{BodyCap, Intent, NetClient, RequestOptions};
use crate::subscriptions::runner::ListSync;

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
/// `backfill_claims_total{order, depth}`: jobs claimed, by claim order.
pub const METRIC_CLAIMS: &str = "backfill_claims_total";
/// DIDs whose follow-verification flag is set and waits for the firehose
/// (following-audience design §3.2).
pub const METRIC_FOLLOWING_WAITING: &str = "following_owners_waiting_firehose";

/// The order a worker claims due jobs in (public-indexing §8.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOrder {
    /// The earliest `not_before` of any depth (worker 0).
    Plain,
    /// Due `member`, `private` and `authz` jobs first, each part by
    /// `not_before` (workers 1 and up).
    Priority,
}

impl ClaimOrder {
    #[must_use]
    pub fn for_worker(worker: u32) -> Self {
        if worker == 0 { Self::Plain } else { Self::Priority }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Priority => "priority",
        }
    }

    /// The row select, served by `backfill_jobs_claim_plain` or
    /// `backfill_jobs_claim_priority` (migration 0022).
    fn select(self) -> &'static str {
        match self {
            Self::Plain => {
                "SELECT did, depth FROM backfill_jobs \
                 WHERE state = 'queued' AND depth <> 'list_sync' AND not_before <= now() \
                 ORDER BY not_before LIMIT 1 FOR UPDATE SKIP LOCKED"
            }
            Self::Priority => {
                "SELECT did, depth FROM backfill_jobs \
                 WHERE state = 'queued' AND depth <> 'list_sync' AND not_before <= now() \
                 ORDER BY (depth IN ('member', 'private', 'authz')) DESC, not_before \
                 LIMIT 1 FOR UPDATE SKIP LOCKED"
            }
        }
    }
}

/// What one pass of a worker's loop did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Turn {
    ListSync,
    Job,
    /// Nothing due, or the claim failed.
    Idle,
}

/// Retry delay after the `attempt`-th consecutive failure (1-based).
#[must_use]
pub fn backoff(attempt: i32) -> TimeDelta {
    match attempt {
        ..=1 => TimeDelta::minutes(1),
        2 => TimeDelta::minutes(10),
        _ => TimeDelta::hours(1),
    }
}

/// `backfill_optional_record_unclear_total{collection}`: profile or
/// `self` record fetches a PDS answered with 404 `NotFound` instead of
/// the lexicon's `RecordNotFound`.
pub const METRIC_OPTIONAL_UNCLEAR: &str = "backfill_optional_record_unclear_total";

/// What a fetch of an optional record found.
#[derive(Debug)]
pub enum Optional {
    Found(Box<ListedRecord>),
    /// The PDS said `RecordNotFound`: the record does not exist.
    Absent,
    /// The PDS said 404 `NotFound`: probably absent, not provably.
    Unclear,
}

/// How an unsuccessful `getRecord` answer reads for an optional record, or
/// `None` for a failure.
///
/// A 404 `NotFound` is how some PDSes answer a missing record. It is not
/// proof of absence (a wrong route answers the same), so it is
/// [`Optional::Unclear`] and nothing stored is deleted on it. A missing
/// repository (`RepoNotFound`) stays a failure.
fn optional_miss(status: u16, error: Option<&str>) -> Option<Optional> {
    match (status, error) {
        (_, Some("RecordNotFound")) => Some(Optional::Absent),
        (404, Some("NotFound")) => Some(Optional::Unclear),
        _ => None,
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
    /// The rkey-self records fetched too: [`SELF_RECORDS`], those of
    /// [`OFF_SELF_RECORDS`], or none.
    pub self_records: &'static [&'static str],
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

/// The four authorization collections (§6.11.3), in listing order. Also
/// what `requestRecord`'s catch-up lists (private posting §3.8).
pub const AUTHORIZATION_NSIDS: [&str; 4] = [NSID_BLOCK, NSID_PRIVATE_BLOCK, NSID_AUDIENCE, NSID_POST_PRIVATE];

const AUTHORIZATION: [CollectionPlan; 4] = [
    authorization(AUTHORIZATION_NSIDS[0]),
    authorization(AUTHORIZATION_NSIDS[1]),
    authorization(AUTHORIZATION_NSIDS[2]),
    authorization(AUTHORIZATION_NSIDS[3]),
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

// Gates of a member's or a private owner's posts, public and private, listed
// after `AUTHORIZATION` so that the rkeys of both kinds of post are known
// when they are kept (private posting §4.4). Not authorization collections.
const GATES: [CollectionPlan; 2] = [
    public(NSID_THREADGATE, Limit::Top(1000)),
    public(NSID_POSTGATE, Limit::Top(1000)),
];

/// How many of a member's follows its plans take, newest first by rkey; a
/// following owner's pass writes no missing follow beyond them (C-F1).
pub const MEMBER_FOLLOWS: usize = 10_000;

// The list collections are listed whole, up to the ceiling. A truncated
// listing only logs: they are not authorization facts. The gates are in
// [`GATES`].
const MEMBER_PUBLIC: [CollectionPlan; 10] = [
    public(NSID_POST, Limit::Top(1000)),
    public(NSID_REPOST, Limit::Top(1000)),
    public(NSID_FOLLOW, Limit::Top(MEMBER_FOLLOWS)),
    public(NSID_LIKE, Limit::Top(1000)),
    public(NSID_LIST, Limit::All),
    public(NSID_LISTITEM, Limit::All),
    public(NSID_LISTBLOCK, Limit::All),
    public(NSID_LIST_OPTOUT, Limit::All),
    public(NSID_STARTER_PACK, Limit::All),
    public(NSID_POLL_POST, Limit::Top(1000)),
];

// Under the off rule (public-indexing design §5): what a member's job lists.
// Posts are listed so that members' references and scope growth follow
// them; no post is stored.
const MEMBER_OFF: [CollectionPlan; 8] = [
    public(NSID_POST, Limit::Top(1000)),
    public(NSID_FOLLOW, Limit::Top(MEMBER_FOLLOWS)),
    public(NSID_LIKE, Limit::Top(1000)),
    public(NSID_REPOST, Limit::Top(1000)),
    public(NSID_LIST, Limit::All),
    public(NSID_LISTITEM, Limit::All),
    public(NSID_LISTBLOCK, Limit::All),
    public(NSID_LIST_OPTOUT, Limit::All),
];

/// The rkey-self records fetched with `getRecord`, like the profile (§3.2).
pub const SELF_RECORDS: [&str; 4] = [
    NSID_LABELER_SERVICE,
    NSID_NOTIFICATION_DECLARATION,
    NSID_ACTOR_STATUS,
    NSID_CONTENT_VISIBILITY,
];

/// The self records the off plans fetch: all but the labeler service,
/// which the off rule does not store (public-indexing design §5).
pub const OFF_SELF_RECORDS: [&str; 3] = [NSID_NOTIFICATION_DECLARATION, NSID_ACTOR_STATUS, NSID_CONTENT_VISIBILITY];

/// The §5.10 depth table. `resync` uses the member limits for a member and
/// the scope limits otherwise ("as the DID's last depth"). In the off modes
/// the public parts are the off plans (public-indexing design §5); in On
/// and Filling they are today's.
#[must_use]
pub fn plan(depth: Depth, is_member: bool, mode: PublicMode) -> DepthPlan {
    let off = mode.off_rule();
    let self_records: &'static [&'static str] = if off { &OFF_SELF_RECORDS } else { &SELF_RECORDS };
    match depth {
        Depth::Scope => DepthPlan {
            profile: true,
            self_records,
            collections: if off { Vec::new() } else { SCOPE_PUBLIC.to_vec() },
            inbound_blocks: false,
        },
        Depth::Member => DepthPlan {
            profile: true,
            self_records,
            collections: if off { MEMBER_OFF.as_slice() } else { MEMBER_PUBLIC.as_slice() }
                .iter()
                .chain(AUTHORIZATION.iter())
                .chain(GATES.iter())
                .copied()
                .collect(),
            inbound_blocks: true,
        },
        Depth::Private => DepthPlan {
            profile: true,
            self_records: &[],
            collections: AUTHORIZATION.iter().chain(GATES.iter()).copied().collect(),
            inbound_blocks: false,
        },
        Depth::Authz => DepthPlan {
            profile: false,
            self_records: &[],
            collections: AUTHORIZATION.iter().chain(GATES.iter()).copied().collect(),
            inbound_blocks: is_member,
        },
        Depth::Resync => DepthPlan {
            profile: true,
            self_records,
            collections: match (off, is_member) {
                (false, true) => MEMBER_PUBLIC.iter().chain(GATES.iter()).copied().collect(),
                (false, false) => SCOPE_PUBLIC.to_vec(),
                (true, true) => MEMBER_OFF.to_vec(),
                (true, false) => Vec::new(),
            },
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
    /// [`Self::list_records`] under the host's catch-up budget, which never
    /// waits: `Ok(None)` when no token is left (private posting §3.8).
    async fn list_records_now(&self, pds: &str, did: &str, collection: &str, cursor: Option<&str>)
    -> Result<Option<ListPage>, String> {
        self.list_records(pds, did, collection, cursor).await.map(Some)
    }
    /// `getRecord`; `Ok(None)` only for an explicit `RecordNotFound`.
    async fn get_record(&self, pds: &str, did: &str, collection: &str, rkey: &str)
    -> Result<Option<ListedRecord>, String>;
    /// `getRecord` for a record an account may simply not have (its
    /// profile, a `self` record). Beside found and absent it can answer
    /// [`Optional::Unclear`]: the PDS said 404 `NotFound`, which is not the
    /// lexicon's `RecordNotFound` and does not say whether the record or
    /// something else is missing.
    async fn get_optional_record(&self, pds: &str, did: &str, collection: &str, rkey: &str) -> Result<Optional, String> {
        Ok(match self.get_record(pds, did, collection, rkey).await? {
            Some(record) => Optional::Found(Box::new(record)),
            None => Optional::Absent,
        })
    }
    /// One Constellation page of `app.bsky.graph.block` records at `.subject`
    /// linking to `member`.
    async fn inbound_blocks(&self, member: &str, cursor: Option<&str>) -> Result<LinksPage, String>;
}

/// The production source: `net` for PDS calls under the host's backfill
/// bucket (§6.11.3: independent of the probe and read-fetch buckets), the
/// identity resolver, and Constellation (which does not use the PDS budget).
/// Backfill is internal work: the resolver and the PDS client are the
/// internal intent's whatever `resolver` was built with (design-v4 §11 G-9).
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
        let resolver = resolver.with_intent(Intent::Internal);
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
        // The backfill class waits for its token, so `None` never comes.
        self.pds_get_in(pds, method, params, cap, Class::Backfill)
            .await?
            .ok_or_else(|| format!("{method}: no budget token"))
    }

    /// A PDS call under `class`'s budget. A class that waits always gets a
    /// token; `Class::CatchUp` never waits, and `Ok(None)` means no token.
    async fn pds_get_in(
        &self,
        pds: &str,
        method: &str,
        params: &[(&str, &str)],
        cap: BodyCap,
        class: Class,
    ) -> Result<Option<crate::net::NetResponse>, String> {
        let url = Url::parse_with_params(&format!("{}/xrpc/{method}", pds.trim_end_matches('/')), params)
            .map_err(|e| format!("{method} url: {e}"))?;
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        if class == Class::CatchUp {
            if !self.budgets.try_acquire(&host, class) {
                return Ok(None);
            }
        } else {
            self.budgets.acquire(&host, class).await;
        }
        self.net
            .get(url.as_str(), &RequestOptions::new(cap).timeout(PDS_TIMEOUT))
            .await
            .map(Some)
            .map_err(|e| format!("{method}: {e}"))
    }

    /// One `listRecords` page under `class`'s budget.
    async fn list_page(
        &self,
        pds: &str,
        did: &str,
        collection: &str,
        cursor: Option<&str>,
        class: Class,
    ) -> Result<Option<ListPage>, String> {
        let limit = PAGE_LIMIT.to_string();
        let mut params = vec![("repo", did), ("collection", collection), ("limit", limit.as_str())];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let Some(r) = self
            .pds_get_in(pds, "com.atproto.repo.listRecords", &params, BodyCap::LIST_RECORDS, class)
            .await?
        else {
            return Ok(None);
        };
        if !r.is_success() {
            return Err(format!("listRecords {collection}: HTTP {}", r.status.as_u16()));
        }
        r.json().map(Some).map_err(|e| format!("listRecords {collection}: {e}"))
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
        self.list_page(pds, did, collection, cursor, Class::Backfill)
            .await?
            .ok_or_else(|| format!("listRecords {collection}: no budget token"))
    }

    async fn list_records_now(
        &self,
        pds: &str,
        did: &str,
        collection: &str,
        cursor: Option<&str>,
    ) -> Result<Option<ListPage>, String> {
        self.list_page(pds, did, collection, cursor, Class::CatchUp).await
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

    async fn get_optional_record(&self, pds: &str, did: &str, collection: &str, rkey: &str) -> Result<Optional, String> {
        let r = self
            .pds_get(
                pds,
                "com.atproto.repo.getRecord",
                &[("repo", did), ("collection", collection), ("rkey", rkey)],
                BodyCap::GET_RECORD,
            )
            .await?;
        if r.is_success() {
            return r.json().map(|record| Optional::Found(Box::new(record))).map_err(|e| format!("getRecord: {e}"));
        }
        let error = serde_json::from_slice::<XrpcError>(&r.body).ok().and_then(|e| e.error);
        let status = r.status.as_u16();
        optional_miss(status, error.as_deref()).ok_or_else(|| format!("getRecord: HTTP {status} {error:?}"))
    }

    async fn inbound_blocks(&self, member: &str, cursor: Option<&str>) -> Result<LinksPage, String> {
        self.constellation
            .links(member, NSID_BLOCK, ".subject", PAGE_LIMIT, cursor)
            .await
            .map_err(|e| format!("constellation: {e}"))
    }
}

/// A job's follows-pass lister (following-audience design §3.5): the job's
/// own source, under the host's backfill budget, which waits.
struct JobLister<'a> {
    source: &'a dyn BackfillSource,
    pds: &'a str,
    did: &'a str,
}

#[async_trait]
impl FollowsLister for JobLister<'_> {
    async fn page(&mut self, cursor: Option<&str>) -> Result<Fetched<ListPage>, String> {
        self.source.list_records(self.pds, self.did, NSID_FOLLOW, cursor).await.map(Fetched::Got)
    }

    async fn get_record(&mut self, rkey: &str) -> Result<Fetched<Option<ListedRecord>>, String> {
        self.source.get_record(self.pds, self.did, NSID_FOLLOW, rkey).await.map(Fetched::Got)
    }
}

/// A claimed job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub did: String,
    pub depth: Depth,
    pub attempts: i32,
    pub run_id: u64,
    /// The row's generation when this run claimed it (design-v4 §II-5.10
    /// "Generations"). Every statement the run issues on its row is
    /// conditioned on it, so a run whose row was reset meanwhile writes
    /// nothing more to it.
    pub generation: i64,
    /// The gap epoch the claim recorded. `settle` compares it with the
    /// current one and never reads it back from the row.
    pub gap_epoch: i64,
    /// The row's `last_error` at the claim: [`FOLLOWS_RETRY`] when the
    /// previous `authz` run met a follows contradiction too.
    pub last_error: Option<String>,
}

/// The `last_error` marker of an `authz` row re-queued after a follows
/// pass that met a contradiction or whose settle wrote the flag NULL
/// (following-audience design §3.5): the next such re-queue waits an hour.
pub const FOLLOWS_RETRY: &str = "follows_retry";

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
    /// The run ended on the ineligible path, with no listing.
    ineligible: bool,
    /// The follow counter read at run start (following-audience design
    /// §3.2), for a tracked DID's full-authorization run.
    clears: Option<i64>,
    /// The follows pass, when the run decided the DID owns a `following`
    /// audience (§3.5).
    pass: Option<PassOutcome>,
}

/// What a settle queues after a follows pass, or after its re-check
/// (following-audience design §3.5): an `authz` run must follow. An
/// `authz` run re-queues its own row instead of `mark_done`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowUp {
    /// The DID owns a `following` audience the run did not verify: now.
    Now,
    /// Reconcile candidates left on budget: a minute on.
    Minute,
    /// A contradiction, or a complete pass whose settle wrote NULL: a
    /// minute on, then an hour once the marker is already there.
    Retry,
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
    /// The block-list index to prime on a member job, when configured.
    farsight: Option<Farsight>,
    /// The `list_sync` lanes the workers serve before their own claim.
    list_sync: Option<ListSync>,
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
            farsight: None,
            list_sync: None,
        }
    }

    /// Serve the `list_sync` lanes from these workers.
    #[must_use]
    pub fn with_list_sync(mut self, list_sync: Option<ListSync>) -> Self {
        self.list_sync = list_sync;
        self
    }

    /// Prime `farsight` on every member job.
    #[must_use]
    pub fn with_farsight(mut self, farsight: Option<Farsight>) -> Self {
        self.farsight = farsight;
        self
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
    /// re-queued first; a `list_sync` row row by row, as a run end with a
    /// new generation (`subscriptions::jobs::interrupt`).
    ///
    /// # Errors
    ///
    /// The startup re-queue failed.
    pub async fn spawn(self) -> Result<Vec<JoinHandle<()>>, sqlx::Error> {
        let mut n = sqlx::query(
            "UPDATE backfill_jobs SET state = 'queued', generation = generation + 1, updated_at = now() \
             WHERE state = 'running' AND depth <> 'list_sync'",
        )
            .execute(&self.pool)
            .await?
            .rows_affected();
        if let Some(list_sync) = &self.list_sync {
            n += list_sync.requeue_interrupted().await?;
        }
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
        if let Some(list_sync) = self.list_sync.clone() {
            handles.push(tokio::spawn(list_sync.clone().stuck_loop()));
            let every = Duration::from_secs(u64::from(self.settings.retry_every_hours.max(1)) * 3600);
            handles.push(tokio::spawn(list_sync.sweep_loop(every)));
        }
        let this = self;
        handles.push(tokio::spawn(async move { this.metrics_loop().await }));
        Ok(handles)
    }

    async fn worker_loop(self, worker: u32) {
        loop {
            if self.worker_turn(worker).await == Turn::Idle {
                tokio::time::sleep(IDLE_POLL).await;
            }
        }
    }

    /// One pass of worker `worker`'s loop.
    async fn worker_turn(&self, worker: u32) -> Turn {
        // A list sync first, when a lane has a free permit and a due row;
        // the lanes are smaller than the worker pool, so ordinary work
        // always keeps workers. With two or more workers, worker 0 never
        // serves the lanes, so a long list sync cannot hold it
        // (public-indexing §8.4).
        if let Some(list_sync) = self.list_sync.as_ref().filter(|_| worker != 0 || self.settings.concurrency < 2) {
            match list_sync.run_once().await {
                Ok(Some(_)) => return Turn::ListSync,
                Ok(None) => {}
                Err(error) => tracing::warn!(event = "list_sync_claim_failed", worker, %error, "claiming a list sync failed"),
            }
        }
        match self.run_once_as(worker).await {
            Ok(Some(_)) => Turn::Job,
            Ok(None) => Turn::Idle,
            Err(error) => {
                tracing::warn!(event = "backfill_claim_failed", worker, %error, "claiming a backfill job failed");
                Turn::Idle
            }
        }
    }

    /// Claim and run one due job, if any, as worker 0.
    ///
    /// # Errors
    ///
    /// Claiming or settling failed (database).
    pub async fn run_once(&self) -> Result<Option<(Job, JobOutcome)>, sqlx::Error> {
        self.run_once_as(0).await
    }

    /// Claim and run one due job, if any, as worker `worker`: worker 0
    /// claims in plain order, every other worker in priority order
    /// (public-indexing §8.4).
    ///
    /// # Errors
    ///
    /// Claiming or settling failed (database).
    pub async fn run_once_as(&self, worker: u32) -> Result<Option<(Job, JobOutcome)>, sqlx::Error> {
        let Some(job) = self.claim(ClaimOrder::for_worker(worker)).await? else {
            return Ok(None);
        };
        self.run_claimed(job).await.map(Some)
    }

    /// Claim and run the queued job `(did, depth)`, due or not (tests that
    /// drive one job at a time, a re-queued run included).
    ///
    /// # Errors
    ///
    /// Claiming or settling failed (database).
    #[cfg(test)]
    pub async fn run_job(&self, did: &str, depth: Depth) -> Result<Option<(Job, JobOutcome)>, sqlx::Error> {
        let row: Option<(i32, i64, i64, Option<String>)> = sqlx::query_as(
            "UPDATE backfill_jobs SET state = 'running', updated_at = now(), \
               gap_epoch = COALESCE((SELECT (value #>> '{}')::bigint FROM worker_state WHERE key = $3), 0) \
             WHERE did = $1 AND depth = $2 AND state = 'queued' \
             RETURNING attempts, generation, gap_epoch, last_error",
        )
        .bind(did)
        .bind(depth.as_str())
        .bind(gap::KEY_GAP_EPOCH)
        .fetch_optional(&self.pool)
        .await?;
        let Some((attempts, generation, gap_epoch, last_error)) = row else {
            return Ok(None);
        };
        let job = Job { did: did.to_string(), depth, attempts, run_id: rand::random(), generation, gap_epoch, last_error };
        self.run_claimed(job).await.map(Some)
    }

    /// Run a claimed job and settle it.
    async fn run_claimed(&self, job: Job) -> Result<(Job, JobOutcome), sqlx::Error> {
        let outcome = match self.run(&job).await {
            Ok(Some(result)) => self.settle(&job, &result).await?,
            Ok(None) => JobOutcome::Superseded,
            Err(error) => {
                if self.fail(&job, &error).await? {
                    JobOutcome::Failed(error)
                } else {
                    JobOutcome::Superseded
                }
            }
        };
        tracing::info!(event = "backfill_job", did = %job.did, depth = job.depth.as_str(), outcome = ?outcome, "backfill job finished");
        Ok((job, outcome))
    }

    /// Claim the next due job in `order`, recording the current gap epoch
    /// (§6.11.2). The run carries the row's generation and that epoch
    /// (§II-5.10 "Generations").
    async fn claim(&self, order: ClaimOrder) -> Result<Option<Job>, sqlx::Error> {
        let row: Option<(String, String, i32, i64, i64, Option<String>)> = sqlx::query_as(&format!(
            "WITH next AS ({}) \
             UPDATE backfill_jobs j SET state = 'running', updated_at = now(), \
               gap_epoch = COALESCE((SELECT (value #>> '{{}}')::bigint FROM worker_state WHERE key = $1), 0) \
             FROM next WHERE j.did = next.did AND j.depth = next.depth \
             RETURNING j.did, j.depth, j.attempts, j.generation, j.gap_epoch, j.last_error",
            order.select()
        ))
        .bind(gap::KEY_GAP_EPOCH)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((_, depth, ..)) = &row {
            metrics::counter!(METRIC_CLAIMS, "order" => order.as_str(), "depth" => depth.clone()).increment(1);
        }
        Ok(row.and_then(|(did, depth, attempts, generation, gap_epoch, last_error)| {
            Depth::parse(&depth).map(|depth| Job {
                did,
                depth,
                attempts,
                run_id: rand::random(),
                generation,
                gap_epoch,
                last_error,
            })
        }))
    }

    /// Still ours: not reset by a later enqueue, not cancelled, and not
    /// claimed since by another run.
    async fn still_running(&self, job: &Job) -> Result<Step, String> {
        let ours: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM backfill_jobs WHERE did = $1 AND depth = $2 AND state = 'running' AND generation = $3)",
        )
        .bind(&job.did)
        .bind(job.depth.as_str())
        .bind(job.generation)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(if ours { Step::Continue } else { Step::Superseded })
    }

    /// Steps 1–5. `Ok(None)` when superseded part-way.
    async fn run(&self, job: &Job) -> Result<Option<RunResult>, String> {
        let (is_member, mode, tracked) = {
            let sets = self.writer.scope().read_guard().await;
            (sets.is_member(&job.did), sets.public.mode, sets.is_tracked(&job.did))
        };
        // A member job asks Farsight to read the member's repository
        // (design-v3 §5.14.11). Best effort, on a task of its own: its
        // outcome never affects the job or readiness.
        if job.depth == Depth::Member
            && let Some(farsight) = self.farsight.clone().filter(Farsight::prime_on_enroll)
        {
            let did = job.did.clone();
            tokio::spawn(async move {
                if let Err(reason) = farsight.prime(&did, false).await {
                    tracing::debug!(event = "farsight_prime_failed", reason, "Farsight was not primed for a member");
                }
            });
        }
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
                return Ok(Some(RunResult { ineligible: true, ..RunResult::default() }));
            }
        }
        let pds = prepared.map_err(|e| e.to_string())?;
        // A run that may verify follows reads the counter before its first
        // listing, creating the row if need be, in one statement
        // (following-audience design §3.2): a clear from now on replaces it.
        let clears = if job.depth.is_full_authorization() && tracked {
            let mut conn = self.pool.acquire().await.map_err(|e| e.to_string())?;
            Some(read_follows_clears(&mut conn, &job.did).await.map_err(|e| e.to_string())?)
        } else {
            None
        };
        let rev = self.source.latest_rev(&pds, &job.did).await?;
        let plan = plan(job.depth, is_member, mode);
        let mut result = RunResult {
            rev,
            clears,
            ..RunResult::default()
        };
        let mut budget = self.settings.reconcile_confirm_limit;
        // rkeys of the posts this job selected: a gate is kept only when
        // its post is among them (a gate's rkey is its post's rkey).
        // The private posts' rkeys count too: those listed in this run and
        // those already stored (private posting §4.4), read once when the
        // first gate collection is reached.
        let mut post_rkeys: HashSet<String> = HashSet::new();
        let mut private_rkeys: HashSet<String> = HashSet::new();
        let mut stored_private_read = false;
        for c in &plan.collections {
            if let Step::Superseded = self.still_running(job).await? {
                return Ok(None);
            }
            let mut listing = self.list_collection(&pds, &job.did, c).await?;
            if c.nsid == NSID_POST {
                post_rkeys = listing.selected.iter().map(|r| r.rkey().to_string()).collect();
            } else if c.nsid == NSID_POST_PRIVATE {
                private_rkeys.extend(listing.selected.iter().map(|r| r.rkey().to_string()));
            } else if matches!(c.nsid, NSID_THREADGATE | NSID_POSTGATE) {
                if !stored_private_read {
                    let stored = self.writer.private().post_rkeys(&self.pool, &job.did).await.map_err(|e| e.to_string())?;
                    private_rkeys.extend(stored);
                    stored_private_read = true;
                }
                listing.selected.retain(|r| post_rkeys.contains(r.rkey()) || private_rkeys.contains(r.rkey()));
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
                    .reconcile(&pds, job, c.nsid, &result.rev, &listed, &mut budget, mode.off_rule())
                    .await?;
                if unconfirmed > 0 && c.authorization {
                    result.auth_reconcile_incomplete = true;
                }
            }
        }
        // The follows pass, for an owner of a `following` audience only
        // (C-F4), decided after the authorization listings, which wrote any
        // `following` audience listed in this run (following-audience
        // design §3.5). It leaves readiness as it was decided.
        if result.clears.is_some() {
            if let Step::Superseded = self.still_running(job).await? {
                return Ok(None);
            }
            let owns = {
                let mut conn = self.pool.acquire().await.map_err(|e| e.to_string())?;
                self.writer.private().owns_following_audience(&mut conn, &job.did).await.map_err(|e| e.to_string())?
            };
            if owns {
                let settings = PassSettings {
                    ceiling: usize::try_from(self.settings.follows_pass_ceiling).unwrap_or(usize::MAX),
                    newest_only: is_member.then_some(MEMBER_FOLLOWS),
                    budget: self.settings.reconcile_confirm_limit,
                    run_id: job.run_id,
                };
                let mut lister = JobLister { source: self.source.as_ref(), pds: &pds, did: &job.did };
                let outcome = follows_pass::run(&self.writer, self.options(), &mut lister, &job.did, &result.rev, settings).await?;
                tracing::info!(event = "follows_pass", did = %job.did, outcome = outcome.label(), "follows pass");
                result.pass = Some(outcome);
            }
        }
        // The profile and the `self` records are optional: a new account
        // has none. An unclear answer neither writes nor deletes, and never
        // fails the job, so that a PDS which answers a missing record with
        // a bare 404 does not keep its members from being set up.
        let optional: Vec<&str> = plan
            .profile
            .then_some(NSID_PROFILE)
            .into_iter()
            .chain(plan.self_records.iter().copied())
            .collect();
        for nsid in optional {
            match self.source.get_optional_record(&pds, &job.did, nsid, "self").await? {
                Optional::Found(record) => {
                    self.write_record(&job.did, &result.rev, nsid, &record).await?;
                }
                Optional::Absent => self.delete_record(&job.did, &result.rev, nsid, "self").await?,
                Optional::Unclear => {
                    metrics::counter!(METRIC_OPTIONAL_UNCLEAR, "collection" => nsid).increment(1);
                    tracing::debug!(event = "backfill_optional_record_unclear", did = %job.did, collection = nsid, "the PDS answered 404 NotFound; nothing written or deleted");
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
            live: false,
            subscription_rules: true,
            source: WriteSource::Backfill,
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

    /// [`local_uris`] through this job's writer.
    async fn local_uris(&self, did: &str, nsid: &str, rev: &str, off_rule: bool) -> Result<Vec<String>, sqlx::Error> {
        local_uris(&self.writer, did, nsid, rev, off_rule).await
    }

    /// Reconcile one fully listed collection (§5.10). Returns the number of
    /// candidates left unconfirmed when the per-job bound ran out.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile(
        &self,
        pds: &str,
        job: &Job,
        nsid: &str,
        rev: &str,
        listed: &HashSet<String>,
        budget: &mut u32,
        off_rule: bool,
    ) -> Result<usize, String> {
        let mut candidates: Vec<String> = self
            .local_uris(&job.did, nsid, rev, off_rule)
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
                 WHERE did = $1 AND depth = $2 AND state = 'running' AND generation = $3",
            )
            .bind(&job.did)
            .bind(job.depth.as_str())
            .bind(job.generation)
            .execute(&self.pool)
            .await?
            .rows_affected();
            return Ok(if n > 0 { JobOutcome::Requeued("reconcile_incomplete") } else { JobOutcome::Superseded });
        }
        let may_set_readiness = readiness_capable && !result.ceiling_hit && !result.auth_write_failed;

        #[cfg(test)]
        crate::test_support::hooks::pause_in(crate::test_support::hooks::SETTLE_BEFORE_GAP_LOCK, &self.pool, &job.did).await;
        // Read guard before the connection (§6.11.7).
        let guard = self.writer.scope().read_guard().await;
        let is_member = guard.is_member(&job.did);
        let is_owner = guard.is_owner(&job.did);
        let tracked = guard.is_tracked(&job.did);
        // Every job sets `last_resync_at` (§5.10) for a DID in S as well:
        // it is design-v3 §5.6's "backfilled once" fact, which
        // `subscribed-post` needs for an author that is not tracked.
        let in_scope = guard.in_scope(&job.did);
        let mut tx = self.pool.begin().await?;
        // The gap lock, shared, and the epoch read under it, before any row
        // (following-audience design §3.2, #722): a gap that commits first
        // is seen here; one that comes later waits for this commit and then
        // clears what it wrote.
        let epoch = gap::current_epoch_locked(&mut tx).await?;
        #[cfg(test)]
        crate::test_support::hooks::pause_in(crate::test_support::hooks::SETTLE_HOLDS_GAP_LOCK, &self.pool, &job.did).await;
        // Ours only while `running` at the claim's generation (§II-5.10):
        // otherwise the run was superseded and writes nothing.
        let ours: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM backfill_jobs WHERE did = $1 AND depth = $2 AND state = 'running' AND generation = $3 FOR UPDATE",
        )
        .bind(&job.did)
        .bind(job.depth.as_str())
        .bind(job.generation)
        .fetch_optional(&mut *tx)
        .await?;
        if ours.is_none() {
            return Ok(JobOutcome::Superseded);
        }
        // What must follow this run for its follows (§3.5), queued right
        // after the own row's lock and before `members`, so the settle
        // takes rows in the order the writer does (§3.3).
        let mut follow_up = self.follow_up(&mut tx, job, result, tracked).await?;
        if let Some(f) = follow_up {
            self.queue_follow_up(&mut tx, job, f).await?;
        }
        #[cfg(test)]
        crate::test_support::hooks::pause_in(crate::test_support::hooks::SETTLE_AFTER_FOLLOW_UP, &self.pool, &job.did).await;
        let outcome = if may_set_readiness {
            let purging: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)")
                .bind(&job.did)
                .fetch_one(&mut *tx)
                .await?;
            if purging {
                requeue(&mut tx, job, PURGING_REQUEUE_SECS).await?;
                JobOutcome::Requeued("purging")
            } else if job.gap_epoch != epoch {
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
                    // The flag moves only here, with `applied_rev`
                    // (following-audience design §3.2): set when this run
                    // made a complete pass and no clear replaced the
                    // counter it read at its start, compared under the row
                    // lock; NULL otherwise, the `INSERT` branch included.
                    // `follows_verified_rev` is the `applied_rev` this
                    // leaves behind, which the firehose must reach before
                    // the flag holds.
                    let pass = result.pass.filter(|p| p.complete).and(result.clears);
                    let verified: bool = sqlx::query_scalar(
                        "INSERT INTO sync_state (did, applied_rev, last_resync_at) VALUES ($1, $2, now()) \
                         ON CONFLICT (did) DO UPDATE SET \
                           applied_rev = GREATEST(sync_state.applied_rev, EXCLUDED.applied_rev), \
                           last_resync_at = now(), \
                           follows_verified_at = CASE WHEN $3 AND sync_state.follows_clears = $4 THEN now() END, \
                           follows_verified_rev = GREATEST(sync_state.applied_rev, EXCLUDED.applied_rev) \
                         RETURNING follows_verified_at IS NOT NULL",
                    )
                    .bind(&job.did)
                    .bind(&result.rev)
                    .bind(pass.is_some())
                    .bind(pass.unwrap_or(0))
                    .fetch_one(&mut *tx)
                    .await?;
                    // A complete pass whose counter moved during the run:
                    // an `authz` run must follow (§3.5). Usually the
                    // follow-up was queued above, the move already seen.
                    if pass.is_some() && !verified && follow_up != Some(FollowUp::Retry) {
                        follow_up = Some(FollowUp::Retry);
                        self.queue_follow_up(&mut tx, job, FollowUp::Retry).await?;
                    }
                } else if in_scope {
                    touch_last_resync(&mut tx, &job.did).await?;
                }
                finish(&mut tx, job, follow_up).await?;
                if ready { JobOutcome::Ready } else { JobOutcome::Done }
            }
        } else {
            if tracked || in_scope {
                touch_last_resync(&mut tx, &job.did).await?;
            }
            finish(&mut tx, job, follow_up).await?;
            JobOutcome::Done
        };
        tx.commit().await?;
        drop(guard);
        Ok(outcome)
    }

    /// What must follow this run for a `following` audience (following-
    /// audience design §3.5), decided on the settle's transaction after the
    /// own row's lock:
    /// - a contradiction: a retry, a minute on, then an hour;
    /// - a complete pass whose counter has already moved (read without a
    ///   lock; the settle statement catches a later move): the same;
    /// - candidates left on budget: a minute on;
    /// - no pass, and the DID owns a `following` audience now (the
    ///   re-check), unless the run ended ineligible or the DID is not
    ///   tracked: now.
    async fn follow_up(&self, conn: &mut PgConnection, job: &Job, result: &RunResult, tracked: bool) -> Result<Option<FollowUp>, sqlx::Error> {
        if !job.depth.is_full_authorization() {
            return Ok(None);
        }
        if let Some(pass) = result.pass {
            if pass.contradiction {
                return Ok(Some(FollowUp::Retry));
            }
            if pass.complete
                && let Some(clears) = result.clears
            {
                let now: Option<i64> = sqlx::query_scalar("SELECT follows_clears FROM sync_state WHERE did = $1")
                    .bind(&job.did)
                    .fetch_optional(&mut *conn)
                    .await?;
                return Ok((now != Some(clears)).then_some(FollowUp::Retry));
            }
            return Ok(pass.unconfirmed.then_some(FollowUp::Minute));
        }
        if result.ineligible || !tracked {
            return Ok(None);
        }
        let owns = self.writer.private().owns_following_audience(conn, &job.did).await?;
        Ok(owns.then_some(FollowUp::Now))
    }

    /// A `member` or `private` run queues the `authz` follow-up, leaving a
    /// queued or running one alone; an `authz` run re-queues its own row
    /// instead, in [`finish`].
    async fn queue_follow_up(&self, conn: &mut PgConnection, job: &Job, f: FollowUp) -> Result<(), sqlx::Error> {
        if job.depth == Depth::Authz {
            return Ok(());
        }
        let at = match f {
            FollowUp::Now => chrono::Utc::now(),
            FollowUp::Minute | FollowUp::Retry => chrono::Utc::now() + backoff(1),
        };
        jobs::enqueue_if_idle(conn, &job.did, Depth::Authz, at).await
    }

    /// Record a failed run on its row, with backoff. Returns whether the row
    /// was still the run's: a superseded run's failure touches nothing.
    async fn fail(&self, job: &Job, error: &str) -> Result<bool, sqlx::Error> {
        let attempt = job.attempts + 1;
        let state = if attempt >= MAX_ATTEMPTS { "failed" } else { "queued" };
        let delay = backoff(attempt).num_seconds();
        let error: String = error.chars().take(512).collect();
        let n = sqlx::query(
            "UPDATE backfill_jobs SET attempts = $3, state = $4, last_error = $5, updated_at = now(), \
               not_before = now() + make_interval(secs => $6) \
             WHERE did = $1 AND depth = $2 AND state = 'running' AND generation = $7",
        )
        .bind(&job.did)
        .bind(job.depth.as_str())
        .bind(attempt)
        .bind(state)
        .bind(&error)
        .bind(delay as f64)
        .bind(job.generation)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            tracing::info!(event = "backfill_job_failed_superseded", did = %job.did, depth = job.depth.as_str(), %error, "a superseded backfill run failed; its row is left alone");
            return Ok(false);
        }
        tracing::warn!(event = "backfill_job_failed", did = %job.did, depth = job.depth.as_str(), attempt, state, %error, "backfill job failed");
        Ok(true)
    }

    /// Every `retry_every_hours`: re-queue failed `member`, `private`, `authz`
    /// and `resync` jobs of DIDs still tracked, resetting attempts, at a new
    /// generation. Failed `list_sync` jobs are re-queued on the same period
    /// by the list sync's own loop (`ListSync::sweep_loop`), paced by owner.
    async fn retry_loop(self) {
        let every = Duration::from_secs(u64::from(self.settings.retry_every_hours.max(1)) * 3600);
        let mut tick = tokio::time::interval(every);
        tick.tick().await;
        loop {
            tick.tick().await;
            let tracked = self.writer.scope().read_guard().await.tracked();
            let result = sqlx::query(
                "UPDATE backfill_jobs SET state = 'queued', attempts = 0, generation = generation + 1, not_before = now(), updated_at = now() \
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
                 ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
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

    /// One sample of the backfill gauges, as `metrics_loop` takes it every
    /// [`METRICS_INTERVAL`].
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn sample_metrics(&self) -> Result<(), sqlx::Error> {
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
        let waiting = following_owners_waiting_firehose(&self.pool).await?;
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_FOLLOWING_WAITING).set(waiting as f64);
        Ok(())
    }
}

/// DIDs whose follow-verification flag is set but does not hold yet: the
/// firehose has not applied their commits through `follows_verified_rev`
/// (following-audience design §3.2). A short spike after commits is
/// normal; a floor names owners on PDSes no relay crawls, or waiting on a
/// `#sync`, a hole or a trim gap.
///
/// # Errors
///
/// Database errors.
pub async fn following_owners_waiting_firehose(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(*) FROM sync_state \
         WHERE follows_verified_at IS NOT NULL \
           AND (firehose_rev IS NULL OR firehose_rev < follows_verified_rev)",
    )
    .fetch_one(pool)
    .await
}

/// Local URIs of `did` in `nsid` with `rev < R`: the reconcile candidates
/// before the listing filter (§5.10), for a job and for `requestRecord`'s
/// catch-up (private posting §3.8). Under the off rule no post is stored, so
/// a member's posts are its references (public-indexing design §4.1).
///
/// # Errors
///
/// Database errors.
pub async fn local_uris(writer: &Writer, did: &str, nsid: &str, rev: &str, off_rule: bool) -> Result<Vec<String>, sqlx::Error> {
    let table = if off_rule && nsid == NSID_POST { Some(("member_post_refs", "author_did")) } else { public_table(nsid) };
    let mut uris = Vec::new();
    if let Some((table, author)) = table {
        uris = sqlx::query_scalar(&format!("SELECT uri FROM {table} WHERE {author} = $1 AND rev < $2"))
            .bind(did)
            .bind(rev)
            .fetch_all(writer.pool())
            .await?;
        // A gate may be stored by the private tier instead (private
        // posting §4.4).
        if !matches!(nsid, NSID_THREADGATE | NSID_POSTGATE) {
            return Ok(uris);
        }
    }
    let mut conn = writer.pool().acquire().await?;
    uris.extend(writer.private().local_uris(&mut conn, did, nsid, rev).await?);
    Ok(uris)
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

/// The run's own re-queue, only while the row is still the run's
/// (§II-5.10).
async fn requeue(conn: &mut PgConnection, job: &Job, delay_secs: i32) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE backfill_jobs SET state = 'queued', not_before = now() + make_interval(secs => $3), updated_at = now() \
         WHERE did = $1 AND depth = $2 AND generation = $4",
    )
    .bind(&job.did)
    .bind(job.depth.as_str())
    .bind(f64::from(delay_secs))
    .bind(job.generation)
    .execute(conn)
    .await?;
    Ok(())
}

/// The run's end: `mark_done`, or for an `authz` run with a follows
/// follow-up (following-audience design §3.5), its own row re-queued,
/// since a guarded enqueue would find it `running` and do nothing.
async fn finish(conn: &mut PgConnection, job: &Job, follow_up: Option<FollowUp>) -> Result<(), sqlx::Error> {
    match (job.depth, follow_up) {
        (Depth::Authz, Some(FollowUp::Now)) => requeue(conn, job, 0).await,
        (Depth::Authz, Some(FollowUp::Minute)) => requeue(conn, job, 60).await,
        (Depth::Authz, Some(FollowUp::Retry)) => retry_requeue(conn, job).await,
        _ => mark_done(conn, job).await,
    }
}

/// An `authz` run's re-queue after a follows contradiction, or a complete
/// pass whose settle wrote the flag NULL (following-audience design §3.5):
/// a minute on, or an hour on when the previous run ended so too (the
/// [`FOLLOWS_RETRY`] marker it read at its claim). It did not fail, so
/// `attempts` is reset; `mark_done` clears the marker, and a `fail()`
/// overwrites it. Only while the row is still the run's.
async fn retry_requeue(conn: &mut PgConnection, job: &Job) -> Result<(), sqlx::Error> {
    let delay = if job.last_error.as_deref() == Some(FOLLOWS_RETRY) { backoff(3) } else { backoff(1) };
    #[allow(clippy::cast_precision_loss)]
    let secs = delay.num_seconds() as f64;
    sqlx::query(
        "UPDATE backfill_jobs SET state = 'queued', not_before = now() + make_interval(secs => $3), attempts = 0, \
           last_error = $4, updated_at = now() \
         WHERE did = $1 AND depth = 'authz' AND generation = $2",
    )
    .bind(&job.did)
    .bind(job.generation)
    .bind(secs)
    .bind(FOLLOWS_RETRY)
    .execute(conn)
    .await?;
    Ok(())
}

/// The run's completion, only while the row is still the run's (§II-5.10).
async fn mark_done(conn: &mut PgConnection, job: &Job) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE backfill_jobs SET state = 'done', attempts = 0, last_error = NULL, updated_at = now() \
         WHERE did = $1 AND depth = $2 AND generation = $3",
    )
    .bind(&job.did)
    .bind(job.depth.as_str())
    .bind(job.generation)
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

    /// A missing optional record: `RecordNotFound` is absence; a PDS's
    /// bare 404 `NotFound` is unclear, so nothing is deleted on it; a
    /// missing repository and every other answer are failures.
    #[test]
    fn a_bare_404_for_an_optional_record_is_unclear_not_absent() {
        assert!(matches!(optional_miss(400, Some("RecordNotFound")), Some(Optional::Absent)));
        assert!(matches!(optional_miss(404, Some("NotFound")), Some(Optional::Unclear)));
        for (status, error) in [(404, Some("RepoNotFound")), (400, Some("NotFound")), (404, None), (500, Some("InternalServerError"))] {
            assert!(optional_miss(status, error).is_none(), "{status} {error:?}");
        }
    }
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
        let plan = |depth, is_member| plan(depth, is_member, PublicMode::On);
        let scope = plan(Depth::Scope, false);
        assert!(scope.profile && scope.self_records == SELF_RECORDS && !scope.inbound_blocks);
        assert_eq!(scope.collections.iter().map(|c| (c.nsid, c.limit)).collect::<Vec<_>>(), [
            (NSID_POST, Limit::Top(100)),
            (NSID_REPOST, Limit::Top(100)),
            (NSID_THREADGATE, Limit::Top(100)),
            (NSID_POSTGATE, Limit::Top(100)),
            (NSID_STARTER_PACK, Limit::Top(25)),
            (NSID_POLL_POST, Limit::Top(100)),
        ]);
        let member = plan(Depth::Member, true);
        assert!(member.inbound_blocks && member.self_records == SELF_RECORDS);
        assert_eq!(member.collections.len(), 16);
        assert_eq!(member.collections.iter().filter(|c| c.authorization).count(), 4);
        assert!(member.collections.iter().filter(|c| c.authorization).all(|c| c.limit == Limit::All));
        // A member's list collections are listed whole.
        for nsid in [NSID_LIST, NSID_LISTITEM, NSID_LISTBLOCK, NSID_LIST_OPTOUT, NSID_STARTER_PACK] {
            assert_eq!(member.collections.iter().find(|c| c.nsid == nsid).map(|c| c.limit), Some(Limit::All), "{nsid}");
        }
        // Gates come after posts, with the posts' limit, and after every
        // authorization collection, `postPrivate` included (private posting
        // §4.4).
        let position = |nsid: &str| member.collections.iter().position(|c| c.nsid == nsid).expect("listed");
        assert!(position(NSID_POST) < position(NSID_THREADGATE) && position(NSID_POST) < position(NSID_POSTGATE));
        assert!(position(NSID_POST_PRIVATE) < position(NSID_THREADGATE) && position(NSID_POST_PRIVATE) < position(NSID_POSTGATE));
        let authz = plan(Depth::Authz, false);
        assert!(!authz.profile && authz.self_records.is_empty() && !authz.inbound_blocks);
        assert!(plan(Depth::Authz, true).inbound_blocks);
        assert_eq!(plan(Depth::Resync, true).collections.len(), 12);
        assert_eq!(plan(Depth::Resync, false).collections.len(), 6);
        // The other new collections are no authorization facts: `private`
        // and `authz` list the authorization collections, then the gates,
        // which are not authorization collections.
        let gates = [(NSID_THREADGATE, Limit::Top(1000), false), (NSID_POSTGATE, Limit::Top(1000), false)];
        for p in [plan(Depth::Private, false), authz] {
            let (head, tail) = p.collections.split_at(AUTHORIZATION.len());
            assert!(head.iter().all(|c| c.authorization));
            assert_eq!(tail.iter().map(|c| (c.nsid, c.limit, c.authorization)).collect::<Vec<_>>(), gates);
        }
        assert!(plan(Depth::Private, false).self_records.is_empty());
        // Every listed public collection has a table to reconcile against.
        for c in member.collections.iter().filter(|c| !c.authorization) {
            assert!(public_table(c.nsid).is_some(), "{}", c.nsid);
        }
    }

    /// T-5 (public-indexing design §5): the off plans in the off modes, and
    /// today's plans in Filling and On.
    #[test]
    fn off_plans() {
        let nsids = |p: &DepthPlan| p.collections.iter().map(|c| c.nsid).collect::<Vec<_>>();
        let authz: Vec<&str> = AUTHORIZATION.iter().map(|c| c.nsid).collect();
        for mode in [PublicMode::Draining, PublicMode::Off, PublicMode::Held] {
            let member = plan(Depth::Member, true, mode);
            let public = [NSID_POST, NSID_FOLLOW, NSID_LIKE, NSID_REPOST, NSID_LIST, NSID_LISTITEM, NSID_LISTBLOCK, NSID_LIST_OPTOUT];
            let expected: Vec<&str> =
                public.iter().copied().chain(authz.iter().copied()).chain([NSID_THREADGATE, NSID_POSTGATE]).collect();
            assert_eq!(nsids(&member), expected, "{mode:?}");
            assert_eq!(member.collections.iter().find(|c| c.nsid == NSID_POST).map(|c| c.limit), Some(Limit::Top(1000)));
            assert_eq!(member.collections.iter().find(|c| c.nsid == NSID_FOLLOW).map(|c| c.limit), Some(Limit::Top(10_000)));
            assert!(member.profile && member.inbound_blocks);
            assert_eq!(member.self_records, OFF_SELF_RECORDS);
            assert!(!member.self_records.contains(&NSID_LABELER_SERVICE));
            let scope = plan(Depth::Scope, false, mode);
            assert!(scope.collections.is_empty() && scope.profile && !scope.inbound_blocks);
            assert_eq!(scope.self_records, OFF_SELF_RECORDS);
            let resync = plan(Depth::Resync, true, mode);
            assert_eq!(nsids(&resync), public);
            assert_eq!(resync.self_records, OFF_SELF_RECORDS);
            let other = plan(Depth::Resync, false, mode);
            assert!(other.collections.is_empty() && other.profile);
            assert_eq!(other.self_records, OFF_SELF_RECORDS);
            for (depth, is_member) in [(Depth::Private, false), (Depth::Authz, true), (Depth::Authz, false)] {
                assert_eq!(plan(depth, is_member, mode), plan(depth, is_member, PublicMode::On), "{depth:?}");
            }
        }
        for mode in [PublicMode::Filling, PublicMode::On] {
            for depth in [Depth::Scope, Depth::Member, Depth::Private, Depth::Authz, Depth::Resync] {
                for is_member in [true, false] {
                    let p = plan(depth, is_member, mode);
                    assert_eq!(p, plan(depth, is_member, PublicMode::On));
                    assert!(p.self_records.is_empty() || p.self_records == SELF_RECORDS);
                }
            }
            assert_eq!(nsids(&plan(Depth::Member, true, mode)).len(), 16);
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

    /// Private posting T-9 (§4.4): a `private` job lists the gates after
    /// `postPrivate`, and keeps those of the private posts listed in the run
    /// and of those already stored; a gate of a post it knows nothing of is
    /// not kept. Read through `PrivateTier`, so this module names no private
    /// table.
    #[tokio::test]
    async fn the_private_plan_keeps_gates_of_postprivate_records() {
        use crate::private_tier::{KryphocronPrivateTier, PrivateTier};
        let Some(db) = fresh_db("backfill_private_gates").await else {
            return;
        };
        let p = &db.pool;
        let private = |rkey: &str| format!("at://{DID}/{NSID_POST_PRIVATE}/{rkey}");
        let gate = |rkey: &str| rec(DID, NSID_THREADGATE, rkey, json!({"post": private(rkey), "allow": []}));
        let mut source = FakeSource {
            rev: "3kzzzzzzzzzzz".to_string(),
            ..FakeSource::default()
        };
        source.listings.insert(NSID_POST_PRIVATE.to_string(), vec![rec(DID, NSID_POST_PRIVATE, "3klisted", json!({"text": "listed"}))]);
        source.listings.insert(NSID_THREADGATE.to_string(), vec![gate("3klisted"), gate("3kstored"), gate("3kunknown")]);
        // Stored before the run, and still in the repo (not listed here).
        source.hidden.insert(private("3kstored"));
        let writer = Writer::new(p.clone(), state(true), Arc::new(KryphocronPrivateTier));
        let backfill = Backfill::new(writer, Arc::new(source), BackfillConfig::default(), 5);
        let stored = rec(DID, NSID_POST_PRIVATE, "3kstored", json!({"text": "stored"}));
        assert_eq!(backfill.write_record(DID, "3kaaaaaaaaaaa", NSID_POST_PRIVATE, &stored).await.expect("write"), None);
        let mut conn = p.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Private).await.expect("enqueue");
        drop(conn);
        loop {
            let (job, outcome) = backfill.run_once().await.expect("run").expect("a job");
            if job.depth == Depth::Private {
                assert_eq!(outcome, JobOutcome::Done);
                break;
            }
        }
        let mut conn = p.acquire().await.expect("conn");
        let mut kept = KryphocronPrivateTier.local_uris(&mut conn, DID, NSID_THREADGATE, "zzzzzzzzzzzzz").await.expect("gates");
        kept.sort();
        assert_eq!(kept, [format!("at://{DID}/{NSID_THREADGATE}/3klisted"), format!("at://{DID}/{NSID_THREADGATE}/3kstored")]);
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

    /// public-indexing design §4.1, §5 (T-5, T-22): under the off rule a
    /// member's resync writes references and growth for its posts and no
    /// post, counts the grow-only writes as success, reconciles the
    /// references, and never fetches the labeler service.
    #[tokio::test]
    async fn an_off_member_resync_writes_references_and_reconciles_them() {
        let Some(db) = fresh_db("backfill_off_member_resync").await else {
            return;
        };
        let p = &db.pool;
        let rev = "3kzzzzzzzzzzz";
        let subject = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let gone = format!("at://{DID}/{NSID_POST}/3kgone");
        sqlx::query("INSERT INTO member_post_refs (uri, author_did, rev, created_at) VALUES ($1, $2, '3kaaaaaaaaaaa', now())")
            .bind(&gone)
            .bind(DID)
            .execute(p)
            .await
            .expect("ref");
        let mut source = FakeSource { rev: rev.to_string(), ..FakeSource::default() };
        let reply = json!({"text": "hi", "reply": {
            "root": {"uri": format!("at://{subject}/{NSID_POST}/1"), "cid": "c"},
            "parent": {"uri": format!("at://{subject}/{NSID_POST}/1"), "cid": "c"}}});
        source.listings.insert(NSID_POST.to_string(), vec![rec(DID, NSID_POST, "3kreply", reply)]);
        source.listings.insert(NSID_LABELER_SERVICE.to_string(), vec![rec(DID, NSID_LABELER_SERVICE, "self", json!({"policies": {}}))]);
        let source = Arc::new(source);
        let scope = state(true);
        {
            let mut sets = scope.write_guard().await;
            let off = crate::indexing::PublicState { mode: PublicMode::Off, ..Default::default() };
            scope.set_public(&mut sets, off);
        }
        let writer = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
        let backfill = Backfill::new(writer, source.clone(), BackfillConfig::default(), 5);
        let mut conn = p.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Resync).await.expect("enqueue");
        drop(conn);
        let (_, outcome) = backfill.run_once().await.expect("run").expect("a job");
        assert_eq!(outcome, JobOutcome::Done);

        assert!(post_uris(p).await.is_empty());
        let refs: Vec<(String, Option<String>)> = sqlx::query_as("SELECT uri, reply_parent_did FROM member_post_refs ORDER BY uri")
            .fetch_all(p)
            .await
            .expect("refs");
        assert_eq!(refs, [(format!("at://{DID}/{NSID_POST}/3kreply"), Some(subject.to_string()))]);
        assert!(scope.snapshot().await.in_scope(subject));
        let calls = source.get_record_calls.lock().expect("lock").clone();
        assert!(calls.contains(&gone), "the stale reference was confirmed");
        assert!(!calls.iter().any(|u| u.contains(NSID_LABELER_SERVICE)), "{calls:?}");
        let labelers: i64 = sqlx::query_scalar("SELECT count(*) FROM labeler_services").fetch_one(p).await.expect("count");
        assert_eq!(labelers, 0);
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
        let job = backfill.claim(ClaimOrder::Plain).await.expect("claim").expect("job");
        let result = backfill.run(&job).await.expect("run").expect("not superseded");
        sqlx::query("UPDATE worker_state SET value = '7'::jsonb WHERE key = 'gap_epoch'")
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

    /// B-75 (design-v3 §5.6, §5.10): a `scope` job for a DID in S that is
    /// not tracked records `last_resync_at`, the "backfilled once" fact
    /// `subscribed-post` needs, and leaves `applied_rev` alone.
    #[tokio::test]
    async fn a_scope_job_for_an_untracked_did_in_s_sets_last_resync_at() {
        let Some(db) = fresh_db("backfill_scope_last_resync").await else {
            return;
        };
        let source = FakeSource { rev: "3kzzzzzzzzzzz".to_string(), ..FakeSource::default() };
        let writer = Writer::new(db.pool.clone(), state(false), Arc::new(DisabledPrivateTier));
        let backfill = Backfill::new(writer, Arc::new(source), BackfillConfig::default(), 5);
        let mut conn = db.pool.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Scope).await.expect("enqueue");
        drop(conn);
        let (_, outcome) = backfill.run_once().await.expect("run").expect("job");
        assert_eq!(outcome, JobOutcome::Done);
        let row: Option<(Option<String>, Option<chrono::DateTime<chrono::Utc>>)> =
            sqlx::query_as("SELECT applied_rev, last_resync_at FROM sync_state WHERE did = $1")
                .bind(DID)
                .fetch_optional(&db.pool)
                .await
                .expect("sync_state");
        let (applied, last) = row.expect("a sync_state row");
        assert_eq!(applied, None);
        assert!(last.is_some());
        db.drop_db().await;
    }

    /// A PDS that never answers: every job that reaches it fails, so a
    /// claimed job leaves the due set.
    struct Unreachable;

    #[async_trait]
    impl BackfillSource for Unreachable {
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

    const MEMBER_JOB: &str = "did:plc:readymember";
    const AUTHZ_JOB: &str = "did:plc:readyauthz";
    const PRIVATE_JOB: &str = "did:plc:readyprivate";

    /// The T-23 seed: 600 due resyncs (`did:plc:r0000` oldest), 300 `authz`
    /// jobs over the next 24 h, one due `member`, `authz` and `private`
    /// job each younger than every resync, and 100 old `done` rows; then
    /// `ANALYZE`.
    async fn seed_claims(pool: &PgPool) {
        for sql in [
            "INSERT INTO backfill_jobs (did, depth, state, not_before) \
             SELECT 'did:plc:r' || lpad(i::text, 4, '0'), 'resync', 'queued', now() - interval '2 hours' + i * interval '1 second' \
             FROM generate_series(0, 599) AS i",
            "INSERT INTO backfill_jobs (did, depth, state, not_before) \
             SELECT 'did:plc:f' || lpad(i::text, 4, '0'), 'authz', 'queued', now() + i * interval '288 seconds' \
             FROM generate_series(1, 300) AS i",
            "INSERT INTO backfill_jobs (did, depth, state, not_before) VALUES \
             ('did:plc:readymember', 'member', 'queued', now() - interval '30 minutes'), \
             ('did:plc:readyauthz', 'authz', 'queued', now() - interval '20 minutes'), \
             ('did:plc:readyprivate', 'private', 'queued', now() - interval '10 minutes')",
            "INSERT INTO backfill_jobs (did, depth, state, not_before) \
             SELECT 'did:plc:d' || lpad(i::text, 4, '0'), 'scope', 'done', now() - interval '3 hours' \
             FROM generate_series(0, 99) AS i",
            "ANALYZE backfill_jobs",
        ] {
            sqlx::query(sql).execute(pool).await.expect(sql);
        }
    }

    fn claim_backfill(pool: &PgPool, concurrency: u32) -> Backfill {
        let writer = Writer::new(pool.clone(), state(false), Arc::new(DisabledPrivateTier));
        let settings = BackfillConfig { concurrency, ..BackfillConfig::default() };
        Backfill::new(writer, Arc::new(Unreachable), settings, 5)
    }

    async fn claimed(backfill: &Backfill, order: ClaimOrder) -> (String, Depth) {
        let job = backfill.claim(order).await.expect("claim").expect("a due job");
        (job.did, job.depth)
    }

    /// public-indexing T-23: the two claim orders, starvation, and the
    /// plans.
    #[tokio::test]
    async fn claims_take_readiness_first_on_workers_one_and_up_and_the_oldest_on_worker_zero() {
        let Some(db) = fresh_db("backfill_claim_order").await else {
            return;
        };
        let p = &db.pool;
        seed_claims(p).await;
        // A due list sync, older than everything: no claim takes it.
        sqlx::query("INSERT INTO backfill_jobs (did, depth, state, not_before) VALUES ('did:plc:owner', 'list_sync', 'queued', now() - interval '4 hours')")
            .execute(p)
            .await
            .expect("list sync");

        // Each order's select is served by its index.
        for (order, index) in [(ClaimOrder::Plain, "backfill_jobs_claim_plain"), (ClaimOrder::Priority, "backfill_jobs_claim_priority")] {
            let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {}", order.select())).fetch_all(p).await.expect("explain");
            let plan = plan.join("\n");
            assert!(plan.contains(index), "{order:?} plan:\n{plan}");
        }

        let backfill = claim_backfill(p, 4);
        // Worker 0: the oldest due resync.
        assert_eq!(claimed(&backfill, ClaimOrder::for_worker(0)).await, ("did:plc:r0000".to_string(), Depth::Resync));
        // Workers 1 and up: the three due readiness jobs in `not_before`
        // order, then resyncs, never a future `authz`.
        let worker1 = ClaimOrder::for_worker(1);
        assert_eq!(worker1, ClaimOrder::Priority);
        assert_eq!(claimed(&backfill, worker1).await, (MEMBER_JOB.to_string(), Depth::Member));
        assert_eq!(claimed(&backfill, worker1).await, (AUTHZ_JOB.to_string(), Depth::Authz));
        assert_eq!(claimed(&backfill, worker1).await, (PRIVATE_JOB.to_string(), Depth::Private));
        for i in 1..=5 {
            assert_eq!(claimed(&backfill, worker1).await, (format!("did:plc:r{i:04}"), Depth::Resync));
        }

        // Starvation: four readiness jobs are queued again at `now()`
        // before every claim, as `reconcile_incomplete` does. Workers 1–3
        // take them; worker 0 still takes the oldest due resync.
        let requeue = "UPDATE backfill_jobs SET state = 'queued', not_before = now() \
                       WHERE did IN (SELECT did FROM backfill_jobs WHERE depth = 'authz' AND did LIKE 'did:plc:f%' ORDER BY did LIMIT 4) \
                         AND depth = 'authz'";
        for worker in 1..=3 {
            sqlx::query(requeue).execute(p).await.expect("requeue");
            let (did, depth) = claimed(&backfill, ClaimOrder::for_worker(worker)).await;
            assert_eq!(depth, Depth::Authz, "worker {worker} took {did}");
        }
        sqlx::query(requeue).execute(p).await.expect("requeue");
        assert_eq!(claimed(&backfill, ClaimOrder::for_worker(0)).await, ("did:plc:r0006".to_string(), Depth::Resync));

        // The list sync was never claimed, nor any `done` row.
        let left: (String, i64) = sqlx::query_as(
            "SELECT (SELECT state FROM backfill_jobs WHERE depth = 'list_sync'), \
                    (SELECT count(*) FROM backfill_jobs WHERE state <> 'done' AND did LIKE 'did:plc:d%')",
        )
        .fetch_one(p)
        .await
        .expect("left");
        assert_eq!(left, ("queued".to_string(), 0));
        db.drop_db().await;
    }

    /// public-indexing T-23: with 4 workers worker 0 never serves the
    /// lanes and worker 1 does; one worker serves them and claims in plain
    /// order; `run_once()` is `run_once_as(0)`.
    #[tokio::test]
    async fn worker_zero_skips_the_lanes_unless_it_is_the_only_worker() {
        use crate::subscriptions::applier::Applier;
        use crate::subscriptions::fetch::{Bounds, OwnerRecords};
        use crate::subscriptions::list_phase;
        use crate::subscriptions::references::{Budget, mute_list};
        use crate::subscriptions::run::{OwnerSource, OwnerStatus, Settings};
        use crate::subscriptions::runner::Lanes;

        /// An owner whose identity was deleted: the run needs no listing.
        struct Tombstoned;

        #[async_trait]
        impl OwnerSource for Tombstoned {
            async fn status(&self, _owner: &str) -> Result<OwnerStatus, String> {
                Ok(OwnerStatus::Tombstoned)
            }
            async fn records(&self, _owner: &str, _generation: i64) -> Result<Box<dyn OwnerRecords>, String> {
                Err("not listed".to_string())
            }
        }

        let Some(db) = fresh_db("backfill_claim_lanes").await else {
            return;
        };
        let p = &db.pool;
        seed_claims(p).await;
        // Two owners' list syncs, both due.
        let member = "did:plc:mmmmmmmmmmmmmmmmmmmmmmm1";
        for owner in ["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"] {
            let list = format!("at://{owner}/app.bsky.graph.list/3klist1");
            mute_list(p, member, &list, Budget { max_references: 5, max_owners: 5 }).await.expect("mute");
            list_phase::apply(p, &list, 600).await.expect("apply");
        }
        let list_syncs_queued = || async move {
            let n: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs WHERE depth = 'list_sync' AND state = 'queued' AND not_before <= now()")
                .fetch_one(p)
                .await
                .expect("list syncs");
            n
        };
        assert_eq!(list_syncs_queued().await, 2);
        let list_sync = || {
            let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
            let bounds = Bounds { max_records: 1000, max_pages: 10, probe_call: Duration::from_secs(5), min_records_per_call_sec: 1, max_call: Duration::from_secs(60), max_wall: Duration::from_secs(60) };
            let settings = Settings { max_items: 100, tombstones_max_per_owner: 10_000, min_interval_secs: 600, bounds };
            let lanes = Lanes { fast: 1, slow: 1, stuck_secs: 5400, max_wall_secs: 3600 };
            let applier = Applier::new(p.clone(), scope.clone(), 600);
            Some(ListSync::new(p.clone(), scope, Arc::new(Tombstoned), &applier, settings, lanes))
        };
        // The jobs a worker ran (each failed once on the unreachable PDS).
        let ran = || async move {
            let v: Vec<String> = sqlx::query_scalar("SELECT did FROM backfill_jobs WHERE attempts > 0 ORDER BY updated_at, did")
                .fetch_all(p)
                .await
                .expect("ran");
            v
        };

        // Four workers: worker 0 runs jobs, oldest first, and never the
        // due list syncs; worker 1 runs a list sync first.
        let four = claim_backfill(p, 4).with_list_sync(list_sync());
        for _ in 0..3 {
            assert_eq!(four.worker_turn(0).await, Turn::Job);
        }
        assert_eq!(ran().await, ["did:plc:r0000", "did:plc:r0001", "did:plc:r0002"]);
        assert_eq!(list_syncs_queued().await, 2);
        assert_eq!(four.worker_turn(1).await, Turn::ListSync);
        assert_eq!(list_syncs_queued().await, 1);

        // `run_once()` claims as worker 0: the next resync, although
        // readiness jobs are due; `run_once_as(1)` takes the member job.
        let (job, _) = four.run_once().await.expect("run").expect("a job");
        assert_eq!((job.did.as_str(), job.depth), ("did:plc:r0003", Depth::Resync));
        let (job, _) = four.run_once_as(0).await.expect("run").expect("a job");
        assert_eq!((job.did.as_str(), job.depth), ("did:plc:r0004", Depth::Resync));
        let (job, _) = four.run_once_as(1).await.expect("run").expect("a job");
        assert_eq!((job.did.as_str(), job.depth), (MEMBER_JOB, Depth::Member));

        // One worker (test-only): it serves the lanes, then claims in
        // plain order.
        let one = claim_backfill(p, 1).with_list_sync(list_sync());
        assert_eq!(one.worker_turn(0).await, Turn::ListSync);
        assert_eq!(list_syncs_queued().await, 0);
        assert_eq!(one.worker_turn(0).await, Turn::Job);
        assert_eq!(ran().await.last().map(String::as_str), Some("did:plc:r0005"));
        db.drop_db().await;
    }

    /// A PDS with nothing listed whose `getLatestCommit` answers only when
    /// the test lets it: the n-th call waits for `gates[n]`. With
    /// `first_fails` the first call then fails.
    struct Gated {
        calls: std::sync::atomic::AtomicUsize,
        entered: tokio::sync::watch::Sender<usize>,
        gates: [tokio::sync::Semaphore; 2],
        first_fails: bool,
    }

    impl Gated {
        fn new(first_fails: bool) -> Arc<Self> {
            Arc::new(Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                entered: tokio::sync::watch::Sender::new(0),
                gates: [tokio::sync::Semaphore::new(0), tokio::sync::Semaphore::new(0)],
                first_fails,
            })
        }

        /// Wait until `n` runs are inside `getLatestCommit`.
        async fn entered(&self, n: usize) {
            let mut entered = self.entered.subscribe();
            tokio::time::timeout(Duration::from_secs(20), entered.wait_for(|e| *e >= n))
                .await
                .expect("the run reached the PDS")
                .expect("gate");
        }

        fn release(&self, n: usize) {
            self.gates[n].add_permits(1);
        }
    }

    #[async_trait]
    impl BackfillSource for Gated {
        async fn prepare(&self, _did: &str) -> Result<String, PrepareError> {
            Ok("https://pds.example".to_string())
        }
        async fn latest_rev(&self, _pds: &str, _did: &str) -> Result<String, String> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered.send_modify(|e| *e += 1);
            if let Some(gate) = self.gates.get(n) {
                gate.acquire().await.expect("gate").forget();
            }
            if n == 0 && self.first_fails {
                return Err("the PDS went away".to_string());
            }
            Ok("3kzzzzzzzzzzz".to_string())
        }
        async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
            Ok(ListPage { records: Vec::new(), cursor: None })
        }
        async fn get_record(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<ListedRecord>, String> {
            Ok(None)
        }
        async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
            Ok(LinksPage { total: 0, linking_records: Vec::new(), cursor: None })
        }
    }

    /// A member, its `member` (or `private`) job queued, and a backfill
    /// over `source`.
    async fn gated_backfill(pool: &PgPool, source: Arc<Gated>, depth: Depth) -> Backfill {
        sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')").bind(DID).execute(pool).await.expect("member");
        let mut conn = pool.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, depth).await.expect("enqueue");
        let writer = Writer::new(pool.clone(), state(true), Arc::new(DisabledPrivateTier));
        Backfill::new(writer, source, BackfillConfig::default(), 5)
    }

    /// `(state, attempts, last_error)` of the DID's job of `depth`, and
    /// whether the member is ready.
    async fn job_and_readiness(pool: &PgPool, depth: Depth) -> (String, i32, Option<String>, bool) {
        sqlx::query_as(
            "SELECT j.state, j.attempts, j.last_error, m.ready_at IS NOT NULL FROM backfill_jobs j JOIN members m ON m.did = j.did \
             WHERE j.did = $1 AND j.depth = $2",
        )
        .bind(DID)
        .bind(depth.as_str())
        .fetch_one(pool)
        .await
        .expect("job")
    }

    /// B-65 member-job generations (design-v4 §II-5.10 "Generations";
    /// R3-67, R3-91): a `member` job is re-enqueued while it runs and a
    /// second worker claims it. The first run's `settle` writes nothing (no
    /// `ready_at`, no `applied_rev`, no row change) and returns Superseded;
    /// only the second run sets readiness.
    #[tokio::test]
    async fn b65_a_superseded_member_run_settles_nothing() {
        let Some(db) = fresh_db("b65_member_generation").await else {
            return;
        };
        let p = &db.pool;
        let source = Gated::new(false);
        let backfill = gated_backfill(p, Arc::clone(&source), Depth::Member).await;
        let run = |backfill: &Backfill| {
            let backfill = backfill.clone();
            tokio::spawn(async move { backfill.run_once_as(1).await })
        };
        let first = run(&backfill);
        source.entered(1).await;
        let mut conn = p.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Member).await.expect("re-enqueue");
        drop(conn);
        let second = run(&backfill);
        source.entered(2).await;

        source.release(0);
        let (_, outcome) = first.await.expect("task").expect("db").expect("a job");
        assert_eq!(outcome, JobOutcome::Superseded);
        assert_eq!(job_and_readiness(p, Depth::Member).await, ("running".to_string(), 0, None, false));
        let applied: Option<String> = sqlx::query_scalar("SELECT applied_rev FROM sync_state WHERE did = $1").bind(DID).fetch_optional(p).await.expect("sync_state").flatten();
        assert_eq!(applied, None);

        source.release(1);
        let (_, outcome) = second.await.expect("task").expect("db").expect("a job");
        assert_eq!(outcome, JobOutcome::Ready);
        assert_eq!(job_and_readiness(p, Depth::Member).await, ("done".to_string(), 0, None, true));
        db.drop_db().await;
    }

    /// B-65 member-job generations, the failing case: the first run fails
    /// after a second worker has claimed the re-enqueued job. Its `fail()`
    /// updates no row, and the second run's row is untouched.
    #[tokio::test]
    async fn b65_a_superseded_member_run_that_fails_leaves_the_live_row_alone() {
        let Some(db) = fresh_db("b65_member_generation_fail").await else {
            return;
        };
        let p = &db.pool;
        let source = Gated::new(true);
        let backfill = gated_backfill(p, Arc::clone(&source), Depth::Member).await;
        let first = {
            let backfill = backfill.clone();
            tokio::spawn(async move { backfill.run_once_as(1).await })
        };
        source.entered(1).await;
        let mut conn = p.acquire().await.expect("conn");
        jobs::enqueue(&mut conn, DID, Depth::Member).await.expect("re-enqueue");
        drop(conn);
        let second = {
            let backfill = backfill.clone();
            tokio::spawn(async move { backfill.run_once_as(1).await })
        };
        source.entered(2).await;

        source.release(0);
        let (_, outcome) = first.await.expect("task").expect("db").expect("a job");
        assert!(matches!(outcome, JobOutcome::Failed(_) | JobOutcome::Superseded), "{outcome:?}");
        assert_eq!(job_and_readiness(p, Depth::Member).await, ("running".to_string(), 0, None, false));
        source.release(1);
        assert_eq!(second.await.expect("task").expect("db").expect("a job").1, JobOutcome::Ready);
        db.drop_db().await;
    }

    /// B-65 member-job generations, the purge case: a purge's marker
    /// cancels a running `private` job, the private-ingest insert queues it
    /// again and a second worker claims it, all before the first run
    /// settles. The stale run settles nothing and the live row is left as
    /// the second run has it.
    #[tokio::test]
    async fn b65_a_private_run_cancelled_by_a_purge_and_requeued_settles_nothing() {
        use crate::aggregations::{PurgeKind, Purger};

        let Some(db) = fresh_db("b65_private_generation").await else {
            return;
        };
        let p = &db.pool;
        let source = Gated::new(false);
        let backfill = gated_backfill(p, Arc::clone(&source), Depth::Private).await;
        let first = {
            let backfill = backfill.clone();
            tokio::spawn(async move { backfill.run_once_as(1).await })
        };
        source.entered(1).await;
        let purger = Purger::new(p.clone(), backfill.writer.scope().clone(), Arc::new(DisabledPrivateTier));
        purger.marker(DID, PurgeKind::OutOfScope).await.expect("purge marker");
        assert_eq!(job_and_readiness(p, Depth::Private).await.0, "cancelled");
        let mut conn = p.acquire().await.expect("conn");
        crate::private_tier::ingest::enqueue_private_for_tests(&mut conn, DID).await.expect("ingest insert");
        drop(conn);
        assert_eq!(job_and_readiness(p, Depth::Private).await.0, "queued");
        let second = {
            let backfill = backfill.clone();
            tokio::spawn(async move { backfill.run_once_as(1).await })
        };
        source.entered(2).await;

        source.release(0);
        let (_, outcome) = first.await.expect("task").expect("db").expect("a job");
        assert_eq!(outcome, JobOutcome::Superseded);
        assert_eq!(job_and_readiness(p, Depth::Private).await.0, "running", "the live run's row is untouched");
        source.release(1);
        second.await.expect("task").expect("db").expect("a job");
        db.drop_db().await;
    }
}
