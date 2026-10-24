//! The fill (plural-pronouns design §2.4): one listing of a DID's four
//! collections, reconciled with what the firehose stored, after which an
//! absent record is provably absent.
//!
//! - Each fill runs in a task of its own (not the request's), under a
//!   process-wide concurrency limit and start rate, with a per-host bucket
//!   for each `listRecords` call. An in-process single-flight map dedupes
//!   every start.
//! - No transaction is held across a network call; only the reconcile runs
//!   in one.
//! - Any failed call ends the fill before the reconcile, so a fill writes
//!   all four collections or nothing (R4).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;

use super::{METRIC_FILL_CALLS, METRIC_FILL_SECONDS, METRIC_FILLS, METRIC_REPOS, NSID_PLURAL_MEMBER, PLURAL_COLLECTIONS, RepoRow, RepoState, record_uri};
use crate::aggregations::{PurgeKind, ScopeState};
use crate::config::PluralConfig;
use crate::db::locks::{lock_dids_shared, lock_uris};
use crate::firehose::backfill::ListPage;
use crate::firehose::normalize::normalize_record;
use crate::firehose::rows::plural_value;
use crate::firehose::writer::SENTINEL_REV;
use crate::identity::{IdentityError, IdentityResolver, ResolveIntent};
use crate::net::client::{BodyCap, NetClient, RequestOptions};
use crate::xrpc::pools::BackgroundBucket;

/// One `listRecords` call's timeout.
const CALL_TIMEOUT: Duration = Duration::from_secs(3);
/// A page.
const PAGE: u32 = 100;
/// Pages a self-keyed collection may take to reach (or rule out) `self`.
const SELF_PAGES: u32 = 10;
/// The background loop's period, and rows it claims per turn.
const LOOP_EVERY: Duration = Duration::from_secs(5);
const CLAIM: i64 = 20;
/// Error names that mean the repo is not served now (§2.4 step 4).
const UNAVAILABLE: [&str; 4] = ["RepoNotFound", "RepoDeactivated", "RepoTakendown", "RepoSuspended"];

/// Why a DID's PDS could not be named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupError {
    /// The DID is tombstoned: the account is gone.
    Tombstoned,
    Failed(String),
}

/// Where a DID's PDS is.
#[async_trait]
pub trait PdsLookup: Send + Sync {
    async fn pds(&self, did: &str) -> Result<String, LookupError>;
}

#[async_trait]
impl PdsLookup for IdentityResolver {
    async fn pds(&self, did: &str) -> Result<String, LookupError> {
        match self.resolve_info(did, ResolveIntent::Lookup).await {
            Ok(info) => info.pds_endpoint.ok_or_else(|| LookupError::Failed("no PDS endpoint".into())),
            Err(IdentityError::Tombstoned(_) | IdentityError::Gone(_)) => Err(LookupError::Tombstoned),
            Err(e) => Err(LookupError::Failed(e.to_string())),
        }
    }
}

/// How one fill ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Complete { truncated: bool },
    Unavailable(String),
    Failed(String),
    Gone,
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Self::Complete { .. } => "complete",
            Self::Unavailable(_) => "unavailable",
            Self::Failed(_) => "failed",
            Self::Gone => "gone",
        }
    }
}

/// One listed record.
#[derive(Debug, Clone)]
struct Listed {
    uri: String,
    collection: &'static str,
    rkey: String,
    cid: String,
    value: Value,
}

/// The retry delay after `attempts` failures (1 min, 5 min, 30 min, 2 h,
/// then 6 h).
#[must_use]
pub fn backoff(attempts: i32) -> Duration {
    const STEPS: [u64; 5] = [60, 300, 1800, 7200, 21_600];
    let i = usize::try_from(attempts.max(1) - 1).unwrap_or(0).min(STEPS.len() - 1);
    Duration::from_secs(STEPS[i])
}

/// The fills. Cheap to clone.
#[derive(Clone)]
pub struct Filler {
    inner: Arc<Inner>,
}

struct Inner {
    pool: PgPool,
    net: NetClient,
    lookup: Arc<dyn PdsLookup>,
    scope: ScopeState,
    cfg: PluralConfig,
    permits: Arc<Semaphore>,
    rate: BackgroundBucket,
    hosts: Mutex<HashMap<String, BackgroundBucket>>,
    inflight: Mutex<HashMap<String, watch::Receiver<bool>>>,
    /// Rows `pending` or `failed`, approximately (n2).
    pending: AtomicI64,
}

impl Filler {
    #[must_use]
    pub fn new(pool: PgPool, net: NetClient, lookup: Arc<dyn PdsLookup>, scope: ScopeState, cfg: &PluralConfig) -> Self {
        let rate = cfg.fill_rate_per_sec.0;
        Self {
            inner: Arc::new(Inner {
                pool,
                net,
                lookup,
                scope,
                cfg: cfg.clone(),
                permits: Arc::new(Semaphore::new(usize::try_from(cfg.fill_concurrency.max(1)).unwrap_or(1))),
                rate: BackgroundBucket::new(rate, burst(rate)),
                hosts: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
                pending: AtomicI64::new(0),
            }),
        }
    }

    #[must_use]
    pub fn config(&self) -> &PluralConfig {
        &self.inner.cfg
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    fn host_bucket(&self, endpoint: &str) -> BackgroundBucket {
        let host = crate::constellation::matcher::pds_host(endpoint).unwrap_or_default();
        let mut map = self.inner.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        if map.len() > 10_000 {
            map.clear();
        }
        let rate = self.inner.cfg.per_host_rate_per_sec.0;
        map.entry(host).or_insert_with(|| BackgroundBucket::new(rate, burst(rate))).clone()
    }

    /// Start `did`'s fill unless one is running; the receiver turns `true`
    /// when it has ended.
    pub fn start(&self, did: &str) -> watch::Receiver<bool> {
        let tx = {
            let mut map = self.inner.inflight.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(rx) = map.get(did) {
                return rx.clone();
            }
            let (tx, rx) = watch::channel(false);
            map.insert(did.to_string(), rx);
            tx
        };
        let rx = tx.subscribe();
        let this = self.clone();
        let did = did.to_string();
        tokio::spawn(async move {
            let permit = Arc::clone(&this.inner.permits).acquire_owned().await;
            this.inner.rate.acquire().await;
            let begun = Instant::now();
            match this.fill_now(&did).await {
                Ok(outcome) => {
                    metrics::counter!(METRIC_FILLS, "outcome" => outcome.label()).increment(1);
                    tracing::debug!(event = "plural_fill", did, outcome = outcome.label(), "plural listing ended");
                }
                Err(error) => {
                    metrics::counter!(METRIC_FILLS, "outcome" => "error").increment(1);
                    tracing::warn!(event = "plural_fill_failed", did, %error, "plural listing could not be written");
                }
            }
            metrics::histogram!(METRIC_FILL_SECONDS).record(begun.elapsed().as_secs_f64());
            drop(permit);
            this.inner.inflight.lock().unwrap_or_else(PoisonError::into_inner).remove(&did);
            let _ = tx.send(true);
        });
        rx
    }

    /// For a read naming `dids` (§2.4 "On ask"): create the rows of DIDs
    /// never asked about (while under `max_pending`), start the fills that
    /// are due, and re-verify old complete rows in the background. Returns
    /// the fills a read should wait for.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn on_ask(&self, dids: &[String], rows: &HashMap<String, RepoRow>) -> Result<Vec<watch::Receiver<bool>>, sqlx::Error> {
        let now = Utc::now();
        let mut waits = Vec::new();
        let missing: Vec<String> = dids.iter().filter(|d| !rows.contains_key(*d)).cloned().collect();
        if !missing.is_empty() {
            let room = i64::from(self.inner.cfg.max_pending) - self.inner.pending.load(Ordering::Relaxed);
            let take = usize::try_from(room.max(0)).unwrap_or(0).min(missing.len());
            if take < missing.len() {
                metrics::counter!(METRIC_FILLS, "outcome" => "capped").increment((missing.len() - take) as u64);
            }
            let inserted: Vec<String> = sqlx::query_scalar(
                "INSERT INTO plural_repos (did, state) SELECT d, 'pending' FROM unnest($1::text[]) AS d \
                 ON CONFLICT (did) DO NOTHING RETURNING did",
            )
            .bind(&missing[..take])
            .fetch_all(&self.inner.pool)
            .await?;
            self.inner.pending.fetch_add(i64::try_from(inserted.len()).unwrap_or(0), Ordering::Relaxed);
            for did in &missing[..take] {
                waits.push(self.start(did));
            }
        }
        let reverify = chrono::Duration::hours(i64::from(self.inner.cfg.reverify_hours));
        for (did, row) in rows {
            let due = row.next_fill_at.is_some_and(|t| t <= now);
            match row.state {
                RepoState::Pending => waits.push(self.start(did)),
                RepoState::Failed | RepoState::Unavailable if due => waits.push(self.start(did)),
                RepoState::Complete if row.verified_at.is_none_or(|v| v < now - reverify) => {
                    let _ = self.start(did);
                }
                _ => {}
            }
        }
        Ok(waits)
    }

    /// Wait up to `inline_wait_ms` for `waits`.
    pub async fn wait(&self, waits: Vec<watch::Receiver<bool>>) {
        if waits.is_empty() {
            return;
        }
        let all = futures::future::join_all(waits.into_iter().map(|mut rx| async move {
            let _ = rx.wait_for(|done| *done).await;
        }));
        let _ = tokio::time::timeout(Duration::from_millis(u64::from(self.inner.cfg.inline_wait_ms)), all).await;
    }

    /// Run `did`'s fill here and record its outcome (tests call it
    /// directly).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn fill_now(&self, did: &str) -> Result<Outcome, sqlx::Error> {
        let pool = &self.inner.pool;
        let started: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(pool).await?;
        let deleted = { self.inner.scope.read_guard().await.purge_kind(did) == Some(PurgeKind::AccountDeleted) };
        let outcome = if deleted {
            Outcome::Gone
        } else {
            match self.inner.lookup.pds(did).await {
                Err(LookupError::Tombstoned) => Outcome::Gone,
                Err(LookupError::Failed(e)) => Outcome::Failed(e),
                Ok(pds) => match self.list(&pds, did).await {
                    Ok((listed, truncated)) => {
                        self.reconcile(did, started, &listed, truncated).await?;
                        return Ok(Outcome::Complete { truncated });
                    }
                    Err(outcome) => outcome,
                },
            }
        };
        self.record_failure(did, &outcome).await?;
        Ok(outcome)
    }

    /// The four collections' records, and whether the member listing was
    /// cut at `member_pages`.
    async fn list(&self, pds: &str, did: &str) -> Result<(Vec<Listed>, bool), Outcome> {
        let mut out = Vec::new();
        let mut truncated = false;
        for collection in PLURAL_COLLECTIONS {
            let member = collection == NSID_PLURAL_MEMBER;
            let pages = if member { self.inner.cfg.member_pages } else { SELF_PAGES };
            let mut cursor: Option<String> = None;
            let mut found_self = false;
            let mut ended = false;
            for _ in 0..pages {
                let page = self.page(pds, did, collection, cursor.as_deref()).await?;
                let empty = page.records.is_empty();
                for r in page.records {
                    let rkey = r.rkey().to_string();
                    if r.uri != record_uri(did, collection, &rkey) || !proto_blue::syntax::RecordKey::is_valid(&rkey) || !r.value.is_object() {
                        continue;
                    }
                    if !member && rkey != "self" {
                        continue;
                    }
                    found_self |= rkey == "self";
                    out.push(Listed { uri: r.uri, collection, rkey, cid: r.cid, value: normalize_record(&r.value) });
                }
                cursor = page.cursor.filter(|c| !c.is_empty());
                if cursor.is_none() || (!member && found_self) {
                    ended = true;
                    break;
                }
                if empty {
                    break;
                }
            }
            if !ended {
                if member {
                    truncated = true;
                } else {
                    // `self` neither found nor ruled out: no answer is
                    // better than a wrong `none`.
                    return Err(Outcome::Failed(format!("{collection}: self not reached")));
                }
            }
        }
        Ok((out, truncated))
    }

    async fn page(&self, pds: &str, did: &str, collection: &str, cursor: Option<&str>) -> Result<ListPage, Outcome> {
        let mut url = url::Url::parse(&format!("{}/xrpc/com.atproto.repo.listRecords", pds.trim_end_matches('/')))
            .map_err(|e| Outcome::Failed(e.to_string()))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("repo", did);
            q.append_pair("collection", collection);
            q.append_pair("limit", &PAGE.to_string());
            if let Some(c) = cursor {
                q.append_pair("cursor", c);
            }
        }
        self.host_bucket(pds).acquire().await;
        let options = RequestOptions::new(BodyCap::LIST_RECORDS).timeout(CALL_TIMEOUT);
        let response = match self.inner.net.get(url.as_str(), &options).await {
            Ok(r) => r,
            Err(e) => {
                metrics::counter!(METRIC_FILL_CALLS, "outcome" => "transport").increment(1);
                return Err(Outcome::Failed(e.to_string()));
            }
        };
        if response.is_success() {
            metrics::counter!(METRIC_FILL_CALLS, "outcome" => "ok").increment(1);
            return response.json::<ListPage>().map_err(|e| Outcome::Failed(format!("listRecords {collection}: {e}")));
        }
        let name = serde_json::from_slice::<Value>(&response.body)
            .ok()
            .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_default();
        let status = response.status.as_u16();
        if status == 404 || UNAVAILABLE.contains(&name.as_str()) {
            metrics::counter!(METRIC_FILL_CALLS, "outcome" => "unavailable").increment(1);
            return Err(Outcome::Unavailable(if name.is_empty() { format!("HTTP {status}") } else { name }));
        }
        metrics::counter!(METRIC_FILL_CALLS, "outcome" => "error").increment(1);
        Err(Outcome::Failed(format!("listRecords {collection}: HTTP {status} {name}")))
    }

    /// §2.4 step 5: the listing against the stored rows, in one
    /// transaction, then the row `complete`.
    async fn reconcile(&self, did: &str, started: DateTime<Utc>, listed: &[Listed], truncated: bool) -> Result<(), sqlx::Error> {
        let pool = &self.inner.pool;
        let before: Vec<String> = sqlx::query_scalar("SELECT uri FROM plural_records WHERE did = $1").bind(did).fetch_all(pool).await?;
        let mut uris: Vec<String> = listed.iter().map(|l| l.uri.clone()).chain(before).collect();
        uris.sort();
        uris.dedup();
        let mut tx = pool.begin().await?;
        lock_dids_shared(&mut tx, &[did.to_string()]).await?;
        lock_uris(&mut tx, &uris).await?;
        let stored: Vec<(String, String, String, DateTime<Utc>)> =
            sqlx::query_as("SELECT uri, collection, cid, indexed_at FROM plural_records WHERE did = $1")
                .bind(did)
                .fetch_all(&mut *tx)
                .await?;
        let stored: HashMap<String, (String, String, DateTime<Utc>)> = stored.into_iter().map(|(u, c, cid, at)| (u, (c, cid, at))).collect();
        let listed_uris: HashSet<&str> = listed.iter().map(|l| l.uri.as_str()).collect();
        for l in listed {
            match stored.get(&l.uri) {
                None => {
                    sqlx::query(
                        "INSERT INTO plural_records (uri, did, rkey, cid, rev, collection, record, indexed_at) \
                         SELECT $1, $2, $3, $4, $5, $6, $7, now() \
                         WHERE NOT EXISTS (SELECT 1 FROM record_tombstones WHERE uri = $1 AND deleted_at >= $8) \
                         ON CONFLICT (uri) DO NOTHING",
                    )
                    .bind(&l.uri)
                    .bind(did)
                    .bind(&l.rkey)
                    .bind(&l.cid)
                    .bind(SENTINEL_REV)
                    .bind(l.collection)
                    .bind(plural_value(&l.value))
                    .bind(started)
                    .execute(&mut *tx)
                    .await?;
                }
                Some((_, cid, at)) if *cid != l.cid && *at < started => {
                    sqlx::query("UPDATE plural_records SET cid = $2, record = $3, indexed_at = now() WHERE uri = $1 AND indexed_at < $4")
                        .bind(&l.uri)
                        .bind(&l.cid)
                        .bind(plural_value(&l.value))
                        .bind(started)
                        .execute(&mut *tx)
                        .await?;
                }
                Some(_) => {}
            }
        }
        for (uri, (collection, _, at)) in &stored {
            if listed_uris.contains(uri.as_str()) || *at >= started || (truncated && collection == NSID_PLURAL_MEMBER) {
                continue;
            }
            sqlx::query("DELETE FROM plural_records WHERE uri = $1 AND indexed_at < $2")
                .bind(uri)
                .bind(started)
                .execute(&mut *tx)
                .await?;
        }
        let was_pending = sqlx::query_scalar::<_, Option<String>>(
            "INSERT INTO plural_repos (did, state, verified_at, next_fill_at, attempts, members_truncated, last_error, updated_at) \
             VALUES ($1, 'complete', now(), NULL, 0, $2, NULL, now()) \
             ON CONFLICT (did) DO UPDATE SET state = 'complete', verified_at = now(), next_fill_at = NULL, attempts = 0, \
               members_truncated = EXCLUDED.members_truncated, last_error = NULL, updated_at = now() \
             RETURNING (SELECT state FROM plural_repos WHERE did = $1)",
        )
        .bind(did)
        .bind(truncated)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        if was_pending.flatten().as_deref().is_some_and(|s| s == "pending" || s == "failed") {
            self.inner.pending.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// A fill that did not complete (§2.4 step 6).
    async fn record_failure(&self, did: &str, outcome: &Outcome) -> Result<(), sqlx::Error> {
        let mut tx = self.inner.pool.begin().await?;
        let (state, error) = match outcome {
            Outcome::Complete { .. } => return Ok(()),
            Outcome::Unavailable(e) => (RepoState::Unavailable, Some(e.as_str())),
            Outcome::Failed(e) => (RepoState::Failed, Some(e.as_str())),
            Outcome::Gone => (RepoState::Gone, None),
        };
        // `unavailable` and `gone` keep nothing of the repo (R3).
        if state != RepoState::Failed {
            lock_dids_shared(&mut tx, &[did.to_string()]).await?;
            sqlx::query("DELETE FROM plural_records WHERE did = $1").bind(did).execute(&mut *tx).await?;
        }
        let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM plural_repos WHERE did = $1")
            .bind(did)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0)
            + 1;
        let next = (state != RepoState::Gone).then(|| Utc::now() + chrono::Duration::from_std(backoff(attempts)).unwrap_or_default());
        sqlx::query(
            "INSERT INTO plural_repos (did, state, next_fill_at, attempts, last_error, updated_at) VALUES ($1, $2, $3, $4, $5, now()) \
             ON CONFLICT (did) DO UPDATE SET state = EXCLUDED.state, next_fill_at = EXCLUDED.next_fill_at, \
               attempts = EXCLUDED.attempts, last_error = EXCLUDED.last_error, updated_at = now()",
        )
        .bind(did)
        .bind(state.as_str())
        .bind(next)
        .bind(attempts)
        .bind(error)
        .execute(&mut *tx)
        .await?;
        tx.commit().await
    }

    /// One turn of the background loop: claim due rows (with a 10 minute
    /// lease) and start their fills. Returns the rows claimed.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn claim_due(&self) -> Result<usize, sqlx::Error> {
        let dids: Vec<String> = sqlx::query_scalar(
            "UPDATE plural_repos SET next_fill_at = now() + interval '10 minutes', updated_at = now() \
             WHERE did IN (SELECT did FROM plural_repos WHERE next_fill_at <= now() ORDER BY next_fill_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
             RETURNING did",
        )
        .bind(CLAIM)
        .fetch_all(&self.inner.pool)
        .await?;
        for did in &dids {
            let _ = self.start(did);
        }
        Ok(dids.len())
    }

    /// The gauge sampler (each minute): rows by state, and the in-memory
    /// pending count (n2).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn sample(&self) -> Result<(), sqlx::Error> {
        let rows: Vec<(String, i64)> = sqlx::query_as("SELECT state, count(*) FROM plural_repos GROUP BY state").fetch_all(&self.inner.pool).await?;
        let mut pending = 0;
        for state in ["pending", "complete", "unavailable", "failed", "gone"] {
            let n = rows.iter().find(|(s, _)| s == state).map_or(0, |(_, n)| *n);
            if state == "pending" || state == "failed" {
                pending += n;
            }
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!(METRIC_REPOS, "state" => state).set(n as f64);
        }
        self.inner.pending.store(pending, Ordering::Relaxed);
        Ok(())
    }

    /// Housekeeping (m1): `failed` rows that kept failing and were not
    /// touched for a week go; a later ask creates them again.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn housekeep(&self) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query("DELETE FROM plural_repos WHERE state = 'failed' AND attempts >= 5 AND updated_at < now() - interval '7 days'")
            .execute(&self.inner.pool)
            .await?
            .rows_affected())
    }

    /// The background loop.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut turns: u64 = 0;
            loop {
                if turns.is_multiple_of(12)
                    && let Err(error) = self.sample().await
                {
                    tracing::warn!(event = "plural_sample_failed", %error, "plural gauges not sampled");
                }
                if turns.is_multiple_of(720)
                    && let Err(error) = self.housekeep().await
                {
                    tracing::warn!(event = "plural_housekeep_failed", %error, "old failed plural rows not removed");
                }
                if let Err(error) = self.claim_due().await {
                    tracing::warn!(event = "plural_claim_failed", %error, "due plural listings not claimed");
                }
                turns = turns.wrapping_add(1);
                tokio::time::sleep(LOOP_EVERY).await;
            }
        })
    }
}

/// A bucket's burst: twice its rate, at least 1.
fn burst(rate: f64) -> u32 {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let b = (rate * 2.0).ceil().clamp(1.0, 10_000.0) as u32;
    b
}

/// An account event for `did` (§2.4 "Account events", R3): a DID with a
/// `plural_repos` row, or with any stored plural record, is listed again
/// 30 s on. Any other DID writes nothing.
///
/// # Errors
///
/// Database errors.
pub async fn account_event(pool: &PgPool, did: &str) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(
        "INSERT INTO plural_repos (did, state, next_fill_at) \
         SELECT $1, 'pending', now() + interval '30 seconds' \
         WHERE EXISTS (SELECT 1 FROM plural_repos WHERE did = $1) OR EXISTS (SELECT 1 FROM plural_records WHERE did = $1) \
         ON CONFLICT (did) DO UPDATE SET next_fill_at = now() + interval '30 seconds', updated_at = now()",
    )
    .bind(did)
    .execute(pool)
    .await?
    .rows_affected()
        > 0)
}
