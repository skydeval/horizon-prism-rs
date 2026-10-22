//! The private read pipeline, per record (design-v2 §6.3), and the sealed
//! per-record witness `RecordAuthz` (§3.2).
//!
//! Steps 3–12 run here; the handler owns steps 1–2 (clock, auth gate) and
//! 13 (the whole-handler floor, when configured; 0 by default). Every
//! failure in steps 3–11 is the same uniform miss, and increments
//! `private_read_outcome_total{outcome}` with a server-side reason that
//! never reaches the wire.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use kryphocron::authority::{ViewPrivate, issue_user};
use kryphocron::ingress::from_xrpc_request;
use kryphocron::{
    AtUri, BoundUserProof, CodecError, CodecId, Did, EncodedRecord, Nsid, OracleSet, ReadAuthorization,
    RecordContentContext, RecordKey, RecordValidation, ResourceId, RotationGenerationMark, StructuralRepresentation,
    TargetRepresentation, TraceId,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::Mutex;

use super::clear_fields;
use super::content::{self, BadContent, Restored};
use super::emit::PrivateEmit;
use super::oracle::{OracleSnapshot, SnapshotInputs};
use super::{readiness, schema};
use crate::db::locks::uri_author;
use crate::firehose::classifier::NSID_POST_PRIVATE;
use crate::firehose::watermark::Watermarks;
use crate::hydration::actor::{ProfileContext, ProfileKind};
use crate::hydration::embeds::{normalize_embed, private_media_view};
use crate::hydration::ts;
use crate::kryphocron_glue::{PipelineGlue, VerifiedCaller};
use crate::moderation::{self, Gate};
use crate::sync::HeadProber;

/// Metric: per-record outcomes (server-side only).
pub(super) const METRIC_OUTCOMES: &str = "private_read_outcome_total";
/// Metric: decoded payloads by `form`, `text` or `content` (private-embeds
/// design §9): the PDS's conversion, seen from the read side.
pub(super) const METRIC_CONTENT: &str = "private_read_content_total";
/// Metric: the read-time clear-field check per hit and `field` (`quote`,
/// `mentions`, `media`), by `result` (`match`, `under`, `over`)
/// (private-encode design §6.2).
pub(super) const METRIC_INDEX_CHECK: &str = "private_read_index_check_total";
/// `notServed`'s content reason (not-served-reason design §2.1): the
/// record's clear fields over-claim.
pub(super) const OVER_CLAIM: &str = "overClaim";
/// Metric: the degraded fallback's per-record results (degraded-mode design
/// §8): `passed`, `behind`, `untracked`, `climb_cap`, `walk_outside`.
pub(super) const METRIC_FALLBACK: &str = "private_read_fallback_total";
/// Metric: unavailable answers (degraded-mode design §8): `thread` (B's
/// 503), `quote` (C's `#viewUnavailable`), `listed` (A's `unavailable`
/// entries).
pub(super) const METRIC_UNAVAILABLE: &str = "private_read_unavailable_total";
/// The most distinct climb authors one read checks (degraded-mode design
/// §2.5, RS-7): more is a miss while degraded.
pub(super) const CLIMB_AUTHORS_MAX: usize = 64;
/// The decode deadline after handler start (§6.3 step 11).
pub(super) const DECODE_BUDGET: Duration = Duration::from_secs(2);

/// The per-record witness (§3.2): the kryphocron `ReadAuthorization` together
/// with the resource it was bound for. Not `Clone`; fields private. Its only
/// constructor takes the bound proof.
pub(super) struct RecordAuthz {
    authz: ReadAuthorization,
    resource: ResourceId,
}

impl RecordAuthz {
    pub(super) fn new(bound: &BoundUserProof<'_, ViewPrivate>) -> Self {
        Self {
            authz: ReadAuthorization::from_view_private(bound),
            resource: bound.subject().clone(),
        }
    }

    pub(super) fn authz(&self) -> &ReadAuthorization {
        &self.authz
    }

    /// The bound resource's at-uri.
    pub(super) fn uri(&self) -> String {
        format!(
            "at://{}/{}/{}",
            self.resource.did().as_str(),
            self.resource.nsid().as_str(),
            self.resource.rkey().as_str()
        )
    }
}

impl std::fmt::Debug for RecordAuthz {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordAuthz").field("resource", &self.resource).finish_non_exhaustive()
    }
}

/// Why a record was a uniform miss (metrics only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Miss {
    NotPrivate,
    Moderation,
    NoRow,
    OwnerNotReady,
    AudienceOwnerNotReady,
    HeadCheck,
    Stale,
    IssueDenied,
    BindDenied,
    AuditUnavailable,
    Malformed,
    CodecMismatch,
    DecodeFailed,
    NotUtf8,
    /// A payload or `encodedContent` refused by `content::restore` or the
    /// input cap (private-embeds design §2, §3).
    BadContent,
    /// A clear index field claims a quote, a mention or media the payload
    /// does not carry, for a reader other than the author (private-encode
    /// design §6.3).
    IndexMismatch,
    /// A private post with no encoded content while plaintext is refused
    /// (S4, private-encode design §7.1), for every reader, its author
    /// included.
    Plaintext,
    /// Degraded with the head fallback on (degraded-mode design §2.5): a
    /// repository the read depends on is not shown to hold every commit up
    /// to its head (step 3′), or the snapshot's walk read a repository
    /// outside the checked set (step 5′).
    FallbackBehind,
    Db,
}

impl Miss {
    fn label(self) -> &'static str {
        match self {
            Self::NotPrivate => "not_private",
            Self::Moderation => "moderation",
            Self::NoRow => "no_row",
            Self::OwnerNotReady => "owner_not_ready",
            Self::AudienceOwnerNotReady => "audience_owner_not_ready",
            Self::HeadCheck => "head_check",
            Self::Stale => "stale",
            Self::IssueDenied => "issue_denied",
            Self::BindDenied => "bind_denied",
            Self::AuditUnavailable => "audit_unavailable",
            Self::Malformed => "malformed",
            Self::CodecMismatch => "codec_mismatch",
            Self::DecodeFailed => "decode_failed",
            Self::NotUtf8 => "not_utf8",
            Self::BadContent => "bad_content",
            Self::IndexMismatch => "index_mismatch",
            Self::Plaintext => "plaintext",
            Self::FallbackBehind => "fallback_behind",
            Self::Db => "db_error",
        }
    }
}

impl Miss {
    /// A transient miss's reason for its author (degraded-mode design
    /// §3.2): `syncing` when Prism has not seen what the read depends on,
    /// `indexing` when an owner is not ready; `None` for every miss whose
    /// cause needs a record to change.
    pub(super) fn unavailable_reason(self) -> Option<&'static str> {
        match self {
            Self::Stale | Self::HeadCheck | Self::FallbackBehind => Some("syncing"),
            Self::OwnerNotReady | Self::AudienceOwnerNotReady => Some("indexing"),
            _ => None,
        }
    }
}

impl From<sqlx::Error> for Miss {
    fn from(e: sqlx::Error) -> Self {
        tracing::warn!(event = "private_read_db_error", error = %e, "private read failed on the database");
        Self::Db
    }
}

/// What the pipeline needs, shared by the private handlers.
#[derive(Clone)]
pub(super) struct PipelineEnv {
    pub pool: PgPool,
    pub glue: PipelineGlue,
    pub prober: HeadProber,
    pub watermarks: Watermarks,
    /// `kryphocron.oracle_freshness_bound_secs`.
    pub freshness_bound: Duration,
    /// `sync.max_head_fetches_per_request`.
    pub max_head_fetches: usize,
    /// `private_read.max_chain`: the walk's cap (private-threads §2.3).
    pub max_chain: u32,
    /// `private_any_pds.enabled` (private-encode design §7, S4): a private
    /// post with no encoded content is a uniform miss for every reader.
    pub plaintext_refused: bool,
    /// `private_read.head_fallback` (degraded-mode design §2, DM-C9): while
    /// degraded, serve reads whose every repository is shown current.
    pub head_fallback: bool,
    /// `private_read.degraded_retry_min_secs` and `_max_secs` (§3.7).
    pub retry_min_secs: u64,
    pub retry_max_secs: u64,
}

/// `retryAfter` (degraded-mode design §3.7): a quarter of the private
/// watermark's delay (unbounded when there is none), rounded, within
/// `min..=max`. Early in a stall a retry comes soon; an old one backs off.
pub(super) fn retry_after_secs(watermark: Option<SystemTime>, now: SystemTime, min: u64, max: u64) -> u64 {
    let quarter = watermark.map_or(u64::MAX, |w| {
        let delay = now.duration_since(w).unwrap_or(Duration::ZERO).as_secs_f64();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let q = (delay / 4.0).round() as u64;
        q
    });
    quarter.clamp(min, max.max(min))
}

/// What a listed response carries at its top level (degraded-mode design
/// §3.2): `degraded` and `retryAfter` while degraded, and the caller's own
/// transient misses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Signals {
    pub degraded: bool,
    pub retry_after: Option<u64>,
    pub unavailable: Vec<(String, &'static str)>,
}

impl PipelineEnv {
    /// Step 5's test (§6.3): the private watermark is within
    /// `freshness_bound` of now. Global, and reads nothing.
    pub(super) fn watermark_fresh(&self) -> bool {
        let watermark = self.watermarks.private_watermark().map(SystemTime::from);
        watermark.is_some_and(|w| SystemTime::now().duration_since(w).unwrap_or(Duration::ZERO) <= self.freshness_bound)
    }
}

/// One DID's head result in a request: `applied_rev ≥ head` (step 3), and
/// under the degraded fallback `chain_rev ≥ head` and when the head was
/// checked (degraded-mode design §2.5). Outside the fallback `chain_ok` is
/// false and `checked_at` `None`: neither is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HeadResult {
    pub applied_ok: bool,
    pub chain_ok: bool,
    pub checked_at: Option<SystemTime>,
}

/// A head step 3′ looked up with no fetch for a DID step 3 did not check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lookup {
    Untracked,
    Stale,
    Fresh(HeadResult),
}

/// The request's climbs (degraded-mode design §2.6): by post URI.
#[derive(Default)]
struct Climbs {
    by_uri: HashMap<String, schema::Climbed>,
}

/// Per-request state: repo-head results by DID, the synchronous head fetch
/// budget, and whether the request is degraded (decided once, here; degraded-
/// mode design §3.1), with the fallback's climbs and no-fetch heads.
pub(super) struct RequestState {
    heads: Mutex<HashMap<String, HeadResult>>,
    fetch_budget: AtomicUsize,
    degraded: bool,
    fallback: bool,
    climbs: Mutex<Climbs>,
    lookups: Mutex<HashMap<String, Lookup>>,
    /// §3.7's value, taken when the request began.
    retry_after: u64,
    /// The caller's own posts this request read through the pipeline that
    /// missed transiently, each once, in first-read order.
    unavailable: std::sync::Mutex<Vec<(String, &'static str)>>,
}

impl RequestState {
    /// The request's state: degraded when the private watermark is not
    /// fresh now; the fallback on when also `private_read.head_fallback`.
    pub(super) fn new(env: &PipelineEnv) -> Self {
        let degraded = !env.watermark_fresh();
        let watermark = env.watermarks.private_watermark().map(SystemTime::from);
        Self {
            heads: Mutex::new(HashMap::new()),
            fetch_budget: AtomicUsize::new(env.max_head_fetches),
            degraded,
            fallback: degraded && env.head_fallback,
            climbs: Mutex::new(Climbs::default()),
            lookups: Mutex::new(HashMap::new()),
            retry_after: retry_after_secs(watermark, SystemTime::now(), env.retry_min_secs, env.retry_max_secs),
            unavailable: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// `retryAfter` for this request's unavailable answers (§3.7).
    pub(super) fn retry_after(&self) -> u64 {
        self.retry_after
    }

    /// Whether `uri` missed transiently for its own author in this request.
    pub(super) fn is_unavailable(&self, uri: &str) -> bool {
        self.unavailable.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().any(|(u, _)| u == uri)
    }

    /// A listed response's top-level fields (degraded-mode design §3.2),
    /// counting each `unavailable` entry.
    pub(super) fn signals(&self) -> Signals {
        let unavailable = self.unavailable.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        if !unavailable.is_empty() {
            metrics::counter!(METRIC_UNAVAILABLE, "kind" => "listed").increment(u64::try_from(unavailable.len()).unwrap_or(u64::MAX));
        }
        Signals { degraded: self.degraded, retry_after: self.degraded.then_some(self.retry_after), unavailable }
    }

    /// Note a transient miss of the caller's own post (§3.2).
    fn note_unavailable(&self, uri: &str, reason: &'static str) {
        let mut list = self.unavailable.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !list.iter().any(|(u, _)| u == uri) {
            list.push((uri.to_string(), reason));
        }
    }

    /// Whether this request is degraded (degraded-mode design §3.1): read
    /// everywhere from here, so one response never mixes two answers.
    pub(super) fn degraded(&self) -> bool {
        self.degraded
    }

    /// The head fetches this request may still make (T-56).
    #[cfg(test)]
    pub(super) fn budget_left(&self) -> usize {
        self.fetch_budget.load(Ordering::SeqCst)
    }

    /// The DIDs whose head results this request holds, with step 3's result
    /// (T-56).
    #[cfg(test)]
    pub(super) async fn cached(&self) -> HashMap<String, bool> {
        self.heads.lock().await.iter().map(|(d, r)| (d.clone(), r.applied_ok)).collect()
    }

    /// One charge of the per-request budget: whether a fetch is funded.
    fn charge(&self) -> bool {
        self.fetch_budget.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok()
    }
}

/// The repo-head check (§6.11.3): `applied_rev ≥ head_rev`, with the head no
/// older than `sync.head_max_age_secs` or fetched now within the budget.
async fn head_ok(env: &PipelineEnv, rs: &RequestState, did: &str) -> Result<bool, sqlx::Error> {
    if let Some(r) = rs.heads.lock().await.get(did) {
        return Ok(r.applied_ok);
    }
    let allow = rs.charge();
    let r = head_check(env, did, allow, rs.fallback).await?;
    rs.heads.lock().await.insert(did.to_string(), r);
    Ok(r.applied_ok)
}

/// `head_ok`'s check from the head read on (private-threads design §3.4):
/// the stored head when fresh, else one fetched when `allow`, against
/// `applied_rev`. Shared with [`prefetch_heads`], so both compare the same
/// way. One statement per DID when the stored head is fresh: the head and
/// `applied_rev` come from the same row (fast-reads design F-3). Under the
/// degraded fallback the same row also gives `chain_rev` and the check time
/// (degraded-mode design §2.5).
async fn head_check(env: &PipelineEnv, did: &str, allow: bool, fallback: bool) -> Result<HeadResult, sqlx::Error> {
    #[cfg(test)]
    if crate::test_support::hooks::injected(crate::test_support::hooks::HEAD_CHECK_FAILS, did) {
        return Err(sqlx::Error::PoolTimedOut);
    }
    if fallback {
        return Ok(match env.prober.read_head_state(did, allow).await? {
            None => HeadResult { applied_ok: false, chain_ok: false, checked_at: None },
            Some(h) => head_result(&h),
        });
    }
    Ok(HeadResult {
        applied_ok: match env.prober.read_head_applied(did, allow).await? {
            None => false,
            Some((head, applied)) => applied.is_some_and(|a| a >= head),
        },
        chain_ok: false,
        checked_at: None,
    })
}

fn head_result(h: &crate::sync::HeadState) -> HeadResult {
    HeadResult {
        applied_ok: h.applied.as_ref().is_some_and(|a| *a >= h.head),
        chain_ok: h.chain.as_ref().is_some_and(|c| *c >= h.head),
        checked_at: Some(SystemTime::from(h.checked_at)),
    }
}

/// What a head round left behind (§3.4).
#[derive(Debug, Default)]
pub(super) struct Prefetch {
    /// DIDs whose `head_check` returned a database error. Each keeps its one
    /// charge and stays uncached, as `head_ok` leaves it.
    pub db_failed: HashSet<String>,
}

/// One head round (private-threads design §3.4): check the heads of `dids`
/// with at most `concurrency` in flight, charging the per-request budget in
/// list order before any check starts, so which DIDs the budget funds never
/// depends on poll order. Results land in `rs`'s cache, so later `head_ok`
/// calls for these DIDs spend nothing. DIDs already cached, and repeats, are
/// dropped. Infallible: a database error on one DID is returned in
/// `db_failed` and leaves that DID uncached. Nothing is refunded.
pub(super) async fn prefetch_heads(env: &PipelineEnv, rs: &RequestState, dids: &[String], concurrency: usize) -> Prefetch {
    let mut funded: Vec<(String, bool)> = Vec::new();
    {
        let cached = rs.heads.lock().await;
        for did in dids {
            if cached.contains_key(did) || funded.iter().any(|(d, _)| d == did) {
                continue;
            }
            funded.push((did.clone(), false));
        }
    }
    for entry in &mut funded {
        entry.1 = rs.charge();
    }
    let checks: Vec<_> = funded
        .into_iter()
        .map(|(did, allow)| async move {
            let result = head_check(env, &did, allow, rs.fallback).await;
            (did, result)
        })
        .collect();
    let results: Vec<(String, Result<HeadResult, sqlx::Error>)> =
        futures::stream::iter(checks).buffered(concurrency.max(1)).collect().await;
    let mut out = Prefetch::default();
    let mut cached = rs.heads.lock().await;
    for (did, result) in results {
        match result {
            Ok(r) => {
                cached.insert(did, r);
            }
            Err(e) => {
                tracing::warn!(event = "private_read_db_error", error = %e, "a head check in the round failed on the database");
                out.db_failed.insert(did);
            }
        }
    }
    out
}

/// Count a read the handler decides without the pipeline (§3.4: a row
/// whose head check failed on the database in the round), as
/// [`read`] counts its own.
pub(super) fn count_db_miss() {
    metrics::counter!(METRIC_OUTCOMES, "outcome" => Miss::Db.label()).increment(1);
}

/// Step 3's readiness for a post by `author` under an audience of
/// `audience_owner` (§6.3): the author's owner readiness, and the
/// audience owner's when it differs, as [`read`] tests them.
///
/// # Errors
///
/// Database errors.
pub(super) async fn ready(env: &PipelineEnv, author: &str, audience_owner: Option<&str>) -> Result<bool, sqlx::Error> {
    if !readiness::owner_ready(&env.pool, author).await? {
        return Ok(false);
    }
    match audience_owner.filter(|a| *a != author) {
        Some(a) => readiness::owner_ready(&env.pool, a).await,
        None => Ok(true),
    }
}

/// The climbs of `uris` in one statement (degraded-mode design §2.6), for
/// a request under the fallback: every handler that reads a set of rows
/// calls this before reading them, so siblings and a page share one
/// statement. URIs already climbed are skipped. A database error leaves
/// them unclimbed: each read then climbs its own.
pub(super) async fn prefill_climbs(env: &PipelineEnv, rs: &RequestState, uris: &[String]) {
    if !rs.fallback {
        return;
    }
    let wanted: Vec<String> = {
        let climbs = rs.climbs.lock().await;
        let mut wanted: Vec<String> = Vec::new();
        for u in uris {
            if !climbs.by_uri.contains_key(u) && !wanted.contains(u) {
                wanted.push(u.clone());
            }
        }
        wanted
    };
    if wanted.is_empty() {
        return;
    }
    match schema::climb_authors(&env.pool, &wanted, env.max_chain).await {
        Ok(found) => rs.climbs.lock().await.by_uri.extend(found),
        Err(e) => tracing::warn!(event = "private_read_db_error", error = %e, "a climb prefill failed on the database"),
    }
}

/// The climb of `uri` (degraded-mode design §2.6): from the request's
/// cache, else one statement for it alone. `None` when no row is stored.
async fn climb_of(env: &PipelineEnv, rs: &RequestState, uri: &str) -> Result<Option<schema::Climbed>, sqlx::Error> {
    if let Some(c) = rs.climbs.lock().await.by_uri.get(uri) {
        return Ok(Some(c.clone()));
    }
    let mut found = schema::climb_authors(&env.pool, &[uri.to_string()], env.max_chain).await?;
    let climbed = found.remove(uri);
    if let Some(c) = &climbed {
        rs.climbs.lock().await.by_uri.insert(uri.to_string(), c.clone());
    }
    Ok(climbed)
}

/// A DID's head for step 3′: step 3's result when it checked the DID in
/// this request, else the stored head with no fetch (degraded-mode design
/// §2.5), cached apart so it never stands in for step 3's own check.
async fn fallback_head(env: &PipelineEnv, rs: &RequestState, did: &str) -> Result<Lookup, sqlx::Error> {
    if let Some(r) = rs.heads.lock().await.get(did) {
        return Ok(Lookup::Fresh(*r));
    }
    if let Some(l) = rs.lookups.lock().await.get(did) {
        return Ok(*l);
    }
    let l = match env.prober.stored_head(did).await? {
        crate::sync::StoredHead::Untracked => Lookup::Untracked,
        crate::sync::StoredHead::Stale => Lookup::Stale,
        crate::sync::StoredHead::Fresh(h) => Lookup::Fresh(head_result(&h)),
    };
    rs.lookups.lock().await.insert(did.to_string(), l);
    Ok(l)
}

/// What step 3′ found: the DIDs shown Current, and the oldest of their head
/// checks, which the oracles answer `last_synced_at` with.
struct Certified {
    current: HashSet<String>,
    last_synced: Option<SystemTime>,
}

fn fallback_miss(result: &'static str) -> Miss {
    metrics::counter!(METRIC_FALLBACK, "result" => result).increment(1);
    Miss::FallbackBehind
}

/// Step 3′ (degraded-mode design §2.5): Current(X) for every X in the
/// decide set D(V, P) = {W, V, A} ∪ Climb(P) (Climb only on another reader's
/// walked post), before the snapshot. W, V and A reuse step 3's results;
/// every other X is its stored head with no fetch. Runs for every record
/// that reaches it, whatever the read rule will say.
async fn certify(env: &PipelineEnv, rs: &RequestState, caller: &str, uri: &str, author: &str, audience_owner: Option<&str>) -> Result<Certified, Miss> {
    let mut d: Vec<String> = vec![author.to_string()];
    for x in [Some(caller), audience_owner].into_iter().flatten() {
        if !d.iter().any(|y| y == x) {
            d.push(x.to_string());
        }
    }
    if caller != author
        && let Some(climbed) = climb_of(env, rs, uri).await?
        && climbed.walked
    {
        let mut distinct: Vec<&String> = climbed.authors.iter().collect();
        distinct.sort_unstable();
        distinct.dedup();
        if distinct.len() > CLIMB_AUTHORS_MAX {
            return Err(fallback_miss("climb_cap"));
        }
        for x in distinct {
            if !d.contains(x) {
                d.push(x.clone());
            }
        }
    }
    let mut certified = Certified { current: HashSet::new(), last_synced: None };
    for x in d {
        match fallback_head(env, rs, &x).await? {
            Lookup::Untracked => return Err(fallback_miss("untracked")),
            Lookup::Stale => return Err(fallback_miss("behind")),
            Lookup::Fresh(r) if !r.chain_ok => return Err(fallback_miss("behind")),
            Lookup::Fresh(r) => {
                certified.last_synced = match (certified.last_synced, r.checked_at) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
                certified.current.insert(x);
            }
        }
    }
    Ok(certified)
}

fn trace_id() -> TraceId {
    TraceId::from_bytes(rand::random())
}

/// Read one private record for `caller` (§6.3 steps 3–12), counting the
/// outcome.
pub(super) async fn read(
    env: &PipelineEnv,
    rs: &RequestState,
    caller: &VerifiedCaller,
    uri: &str,
    authors: &ProfileContext,
    start: Instant,
) -> Result<PrivateEmit<Value>, Miss> {
    #[cfg(test)]
    crate::test_support::hooks::delay(crate::test_support::hooks::PRIVATE_READ, uri_author(uri).unwrap_or_default()).await;
    // Boxed: the bind and decode futures are large, and a page holds up to
    // `bind_concurrency` of them.
    let result = Box::pin(read_inner(env, rs, caller, uri, authors, start)).await;
    // The caller's own post, missed transiently (degraded-mode design §3.2).
    if let Err(m) = &result
        && let Some(reason) = m.unavailable_reason()
        && uri_author(uri) == Some(caller.did.as_str())
    {
        rs.note_unavailable(uri, reason);
    }
    let label = match &result {
        Ok(_) => "hit",
        Err(m) => m.label(),
    };
    metrics::counter!(METRIC_OUTCOMES, "outcome" => label).increment(1);
    tracing::debug!(event = "private_read", uri, outcome = label, "private read");
    result
}

async fn read_inner(
    env: &PipelineEnv,
    rs: &RequestState,
    caller: &VerifiedCaller,
    uri: &str,
    authors: &ProfileContext,
    start: Instant,
) -> Result<PrivateEmit<Value>, Miss> {
    // 3. Parse and pre-validate.
    let mut parts = uri.strip_prefix("at://").ok_or(Miss::NotPrivate)?.splitn(3, '/');
    let (Some(owner_s), Some(nsid_s), Some(rkey_s)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(Miss::NotPrivate);
    };
    if nsid_s != NSID_POST_PRIVATE {
        return Err(Miss::NotPrivate);
    }
    let owner = Did::new(owner_s).map_err(|_| Miss::NotPrivate)?;
    let nsid = Nsid::new(nsid_s).map_err(|_| Miss::NotPrivate)?;
    let rkey = RecordKey::new(rkey_s).map_err(|_| Miss::NotPrivate)?;
    let requester = Did::new(&caller.did).map_err(|_| Miss::NotPrivate)?;
    if moderation::gate(&env.pool, uri).await? != Gate::Visible {
        return Err(Miss::Moderation);
    }
    let facts = schema::post_facts(&env.pool, uri).await?.ok_or(Miss::NoRow)?;
    if facts.owner != owner_s {
        return Err(Miss::NoRow);
    }
    if !readiness::owner_ready(&env.pool, owner_s).await? {
        return Err(Miss::OwnerNotReady);
    }
    let audience_owner = facts.audience_uri.as_deref().and_then(uri_author);
    if let Some(a) = audience_owner.filter(|a| *a != owner_s)
        && !readiness::owner_ready(&env.pool, a).await?
    {
        return Err(Miss::AudienceOwnerNotReady);
    }
    let mut head_dids = vec![owner_s, caller.did.as_str()];
    if let Some(a) = audience_owner.filter(|a| *a != owner_s) {
        head_dids.push(a);
    }
    for did in head_dids {
        if !head_ok(env, rs, did).await? {
            return Err(Miss::HeadCheck);
        }
    }

    // 3′. Degraded with the head fallback on (degraded-mode design §2.5):
    // every repository the read depends on holds every commit up to its
    // head, shown before the snapshot.
    let certified = if rs.fallback {
        let c = certify(env, rs, &caller.did, uri, owner_s, audience_owner).await?;
        #[cfg(test)]
        crate::test_support::hooks::pause(crate::test_support::hooks::FALLBACK_AFTER_CHECK, uri).await;
        Some(c)
    } else {
        None
    };

    // 4. The oracle snapshot.
    let resource = ResourceId::new(owner.clone(), nsid.clone(), rkey.clone());
    let watermark = env.watermarks.private_watermark().map(SystemTime::from);
    let snap = OracleSnapshot::load(
        &env.pool,
        SnapshotInputs {
            requester: &caller.did,
            resource: &resource,
            uri,
            last_synced: certified.as_ref().map_or(watermark, |c| c.last_synced),
            bound: env.freshness_bound,
            max_chain: env.max_chain,
            with_authors: certified.is_some(),
        },
    )
    .await?;

    // 5. Host freshness: the only block-freshness enforcement point, or,
    // under the fallback, step 5′: fact 8's O and every author its walk
    // read are among the repositories step 3′ showed Current. On the
    // author's own read nothing beyond step 3′'s W and A.
    let now = SystemTime::now();
    if let Some(c) = &certified {
        if caller.did != owner_s {
            let outside = snap.fact8_owner().into_iter().chain(snap.walk_authors().iter().map(String::as_str)).any(|d| !c.current.contains(d));
            if outside {
                return Err(fallback_miss("walk_outside"));
            }
        }
        metrics::counter!(METRIC_FALLBACK, "result" => "passed").increment(1);
    } else {
        let fresh = watermark.is_some_and(|w| now.duration_since(w).unwrap_or(Duration::ZERO) <= env.freshness_bound);
        if !fresh {
            return Err(Miss::Stale);
        }
    }
    let view_audience = snap.view_audience().map(str::to_string);

    // 6–8. Context, issue, bind. `ctx` borrows `snap` and the glue's sinks.
    let trace = trace_id();
    let bound = {
        let ctx = from_xrpc_request(caller.jwt.clone(), trace, env.glue.audit_sinks(), OracleSet::new(&snap, &snap, &snap));
        let proof = issue_user::<ViewPrivate>(&ctx, resource.clone()).map_err(|_| Miss::IssueDenied)?;
        Box::pin(proof.bind(&ctx, &resource)).await.map_err(|e| match e {
            kryphocron::BindError::AuditUnavailable | kryphocron::BindError::AuditPanicked => Miss::AuditUnavailable,
            _ => Miss::BindDenied,
        })?
    };

    // 9. The witness. From here on content is loaded only through it.
    let rauth = RecordAuthz::new(&bound);

    // 10. Structural validation behind the witness, on one version of the
    // row read in one statement (private-encode design §6.1): the content
    // and the view's `cid` below come from the same version.
    let mut row = schema::snapshot(&env.pool, &rauth).await?.ok_or(Miss::NoRow)?;
    #[cfg(test)]
    crate::test_support::hooks::pause(crate::test_support::hooks::PRIVATE_SNAPSHOT_TAKEN, uri).await;
    kryphocron::validate_record_for_read(
        rauth.authz(),
        &RecordValidation::post_private(row.has_text, row.encoded_content.is_some(), row.has_codec, row.has_generation),
        nsid.clone(),
        trace,
        env.glue.substrate_sink(),
        now,
    )
    .map_err(|_| Miss::Malformed)?;

    // 11. Decode, then restore the payload (private-embeds design §3): the
    // text alone, or text, facets and embed. An `encodedContent` larger
    // than any record could hold is not decoded; the codec the hooks carry
    // refuses output past the same bound (S3), which fails `decode_failed`.
    let restored = match row.encoded_content.take() {
        Some(encoded) => {
            if encoded.len() > content::PAYLOAD_MAX_BYTES {
                return Err(Miss::BadContent);
            }
            let codec = CodecId::new(row.encoded_codec.take().unwrap_or_default()).map_err(|_| Miss::Malformed)?;
            // `encodedContentGeneration` is client-asserted (a client
            // encoder stamps it from its own clock). It is handed to the
            // codec only as the record's mark (laquna reads its slug from
            // the artifact, not from the mark), and nothing in the read
            // rule, the oracle or any other account's view compares it
            // with the clock or with anything else: a mark dated in the
            // future reads exactly as a current one (T-G1). Only a mark that
            // is not a mark at all makes the record malformed, as any other
            // malformed field of the author's own record does.
            let generation = row
                .encoded_generation
                .take()
                .map(RotationGenerationMark::new)
                .transpose()
                .map_err(|_| Miss::Malformed)?;
            let audience_list = facts.audience_uri.as_deref().and_then(|u| AtUri::new(u).ok());
            let context = RecordContentContext::new(
                nsid.clone(),
                rkey.clone(),
                owner.clone(),
                audience_list,
                requester,
                TargetRepresentation::structural_only(StructuralRepresentation::Resource {
                    did: owner.clone(),
                    nsid: nsid.clone(),
                }),
                trace,
                Default::default(),
            );
            let bytes = kryphocron::decode_record_content(
                rauth.authz(),
                env.glue.at_rest_hooks(),
                env.glue.user_sink(),
                &EncodedRecord::new(codec, encoded, generation),
                &context,
                start + DECODE_BUDGET,
                SystemTime::now(),
            )
            .await
            .map_err(|e| match e {
                CodecError::UnknownOrWrongCodec { .. } => Miss::CodecMismatch,
                _ => Miss::DecodeFailed,
            })?;
            let restored = content::restore(bytes).map_err(|e| match e {
                BadContent::NotUtf8 => Miss::NotUtf8,
                _ => Miss::BadContent,
            })?;
            metrics::counter!(METRIC_CONTENT, "form" => restored.form()).increment(1);
            restored
        }
        // S4 (private-encode design §7.1): plaintext is refused while
        // `private_any_pds.enabled`, from any host, the author included. The
        // row is kept, and served again if the switch is turned off.
        None if env.plaintext_refused => return Err(Miss::Plaintext),
        None => Restored::Text(row.plaintext_text.take().ok_or(Miss::Malformed)?),
    };

    // 12. The clear-field check (private-encode design §6.2, §6.3): the
    // facts a correct encoder writes for what the writer encoded, from the
    // payload's raw `facets` and `embed` (dropped at read or not) where the
    // payload has the key, else the stored clear column, against the row's
    // index columns. An over-claim is a miss for every reader but the
    // author; an under-claim is served.
    let is_author = caller.did == owner_s;
    let (raw_facets, raw_embed) = match &restored {
        Restored::Text(_) => (row.facets.as_ref(), row.embed.as_ref()),
        Restored::Content { facets, embed, .. } => (facets.raw(row.facets.as_ref()), embed.raw(row.embed.as_ref())),
    };
    let expected = clear_fields::index(&clear_fields::derive(raw_facets, raw_embed));
    let over_claim = index_check(&expected, &row);
    if over_claim && !is_author {
        return Err(Miss::IndexMismatch);
    }
    // The author's `notServed` (not-served-reason design §4.4): an
    // over-claim misses for every other reader, so the post is not served.
    // `decorate` adds the thread reasons in front of it on a top-level view.
    #[cfg(test)]
    let over_claim = over_claim && !crate::test_support::hooks::injected_in(crate::test_support::hooks::NOT_SERVED_OFF, &env.pool, &caller.did);
    let over_claim = over_claim && is_author;
    // The author's marker (§8.4): the payload fields this read leaves out.
    let mut dropped_fields: Vec<&str> = Vec::new();
    if let Restored::Content { facets, embed, .. } = &restored
        && is_author
    {
        #[cfg(test)]
        let marker_on = !crate::test_support::hooks::injected(crate::test_support::hooks::DROPPED_MARKER_OFF, uri);
        #[cfg(not(test))]
        let marker_on = true;
        if marker_on {
            dropped_fields.extend([("facets", facets), ("embed", embed)].into_iter().filter(|(_, f)| f.dropped()).map(|(k, _)| k));
        }
    }

    // The view, emitted through the witness. `facets` and `embed`: the
    // payload's when it has the key, else the clear fields as stored
    // (private-embeds design §2). `langs` and `labels` are always the
    // stored ones; nothing else in a payload is read.
    let (text, facets, embed) = match restored {
        Restored::Text(text) => (text, row.facets.take(), row.embed.take()),
        Restored::Content { text, facets, embed } => (text, facets.merge(row.facets.take()), embed.merge(row.embed.take())),
    };
    let meta = &row;
    let display = &row;
    let mut record = json!({
        "$type": NSID_POST_PRIVATE,
        "text": text,
        "createdAt": ts(meta.created_at),
    });
    for (key, value) in [
        ("facets", facets),
        ("embed", embed.clone()),
        ("langs", display.langs.as_ref().map(|l| json!(l))),
        ("labels", display.self_labels.clone()),
    ] {
        if let Some(v) = value {
            record[key] = v;
        }
    }
    if let Some(a) = &meta.audience_uri {
        record["audienceList"] = json!(a);
    }
    if let (Some(root), Some(parent)) = (&meta.reply_root_uri, &meta.reply_parent_uri) {
        // Strong refs; a cid is absent only for rows ingested before
        // migration 0002 that no resync has refreshed yet.
        let strong_ref = |uri: &str, cid: &Option<String>| match cid {
            Some(c) => json!({"uri": uri, "cid": c}),
            None => json!({"uri": uri}),
        };
        record["reply"] = json!({
            "root": strong_ref(root, &meta.reply_root_cid),
            "parent": strong_ref(parent, &meta.reply_parent_cid),
        });
    }
    // The self-labels as label objects (§3.3); the handler adds the
    // operator labels stored for the URI.
    let cts = ts(meta.created_at);
    let labels: Vec<Value> = self_label_values(display.self_labels.as_ref())
        .map(|val| json!({"src": owner_s, "uri": uri, "cid": meta.cid, "val": val, "cts": cts}))
        .collect();
    let mut view = json!({
        "uri": rauth.uri(),
        "cid": meta.cid,
        "author": authors.view(owner_s, ProfileKind::Basic),
        "record": record,
        "indexedAt": ts(meta.indexed_at),
        "labels": labels,
    });
    if !dropped_fields.is_empty() {
        view["droppedFields"] = json!(dropped_fields);
    }
    if over_claim {
        view["notServed"] = json!([OVER_CLAIM]);
    }
    // The media part of `embed` (§4.2); the handler adds the record part. A
    // video plays from the author's PDS (private-video-pds design).
    let pds = authors.rows.get(owner_s).and_then(|r| r.pds_endpoint.as_deref());
    if let Some(media) = embed
        .as_ref()
        .map(normalize_embed)
        .and_then(|e| private_media_view(owner_s, pds, &e))
    {
        view["embed"] = media;
    }
    // The audience (P-3, §4.10): the governing audience G (private-threads
    // design §2.6), present while its row exists; on the author's own read
    // only when the author is G's owner or passes rules 6 to 8. `mode` is
    // left out when NULL, which reads as `list`; `members` never appears.
    if let Some(a) = &view_audience
        && let Some((mode, name)) = schema::audience_view(&env.pool, a).await?
    {
        let mut audience = json!({"uri": a});
        if let Some(mode) = mode {
            audience["mode"] = json!(mode);
        }
        if let Some(name) = name {
            audience["name"] = json!(name);
        }
        view["audience"] = audience;
    }
    Ok(PrivateEmit::new(&rauth, view))
}

/// The read-time clear-field check (private-encode design §6.2): compare
/// the facts `expected` from what the writer encoded with the row's index
/// columns, field by field, counting each result. Whether any field
/// over-claims: a stored quote other than the expected one, a stored
/// mention or media bit the payload lacks.
fn index_check(expected: &clear_fields::IndexFacts, row: &schema::Snapshot) -> bool {
    let stored_media = u8::try_from(row.media_kinds & 0xFF).unwrap_or(0);
    let quote_over = row.quote_uri.is_some() && row.quote_uri != expected.quote_uri;
    let quote_under = expected.quote_uri.is_some() && row.quote_uri.is_none();
    let mentions_over = row.mention_dids.iter().any(|d| !expected.mention_dids.contains(d));
    let mentions_under = expected.mention_dids.iter().any(|d| !row.mention_dids.contains(d));
    let media_over = stored_media & !expected.media_kinds != 0;
    let media_under = expected.media_kinds & !stored_media != 0;
    let mut over = false;
    for (field, is_over, is_under) in [
        ("quote", quote_over, quote_under),
        ("mentions", mentions_over, mentions_under),
        ("media", media_over, media_under),
    ] {
        let result = if is_over {
            "over"
        } else if is_under {
            "under"
        } else {
            "match"
        };
        metrics::counter!(METRIC_INDEX_CHECK, "field" => field, "result" => result).increment(1);
        over |= is_over;
    }
    over
}

/// The `val`s of a stored `#selfLabels` object.
fn self_label_values(labels: Option<&Value>) -> impl Iterator<Item = &str> {
    labels
        .and_then(|l| l.get("values"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|v| v.get("val").and_then(Value::as_str))
}
