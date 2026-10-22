//! The repo-head prober and the read-path head fetch (design-v2 §6.11.3).
//!
//! - **Prober.** Every tracked DID (members ∪ private owners) has its
//!   `sync_state.head_rev` refreshed from `com.atproto.sync.getLatestCommit`
//!   on its current PDS within 0.75 × `sync.head_max_age_secs`, under the
//!   host's probe bucket.
//! - **Divergence** is detected here, never by reads (#163). When a probe
//!   finds `head_rev > applied_rev` for longer than
//!   `sync.divergence_grace_secs`, measured from `diverged_since`, the prober
//!   clears the DID's readiness and enqueues an `authz` job.
//! - **Read path.** [`HeadProber::read_head`] uses a head no older than
//!   `head_max_age_secs`, or fetches one synchronously under the host's
//!   read-fetch bucket, which never waits: no token is a uniform miss.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use url::Url;

use super::budgets::{Class, HostBudgets};
use crate::aggregations::ScopeState;
use crate::config::SyncConfig;
use crate::firehose::jobs::{self, Depth};
use crate::firehose::watermark::Watermarks;
use crate::firehose::writer::clear_readiness;
use crate::net::{BodyCap, NetClient, RequestOptions, Transports};
use crate::private_tier::PrivateTier;

pub const METRIC_CHECKS: &str = "sync_head_check_total";
pub const METRIC_CHECK_SECONDS: &str = "sync_head_check_seconds";
pub const METRIC_DIVERGENCE: &str = "sync_divergence_detected_total";
/// Metric: `authz` jobs the degraded probe enqueued for a repo whose chain
/// stayed behind its head (degraded-mode design §2.4).
pub const METRIC_DEGRADED_AUTHZ: &str = "sync_degraded_authz_enqueued_total";

/// Pause between prober passes when nothing was due.
pub const IDLE: Duration = Duration::from_secs(1);
/// Timeout for a background probe.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Where heads come from.
#[async_trait]
pub trait HeadSource: Send + Sync {
    /// `getLatestCommit` rev for `did` at `pds`.
    async fn latest_rev(&self, pds: &str, did: &str, timeout: Duration) -> Result<String, String>;
}

#[derive(Deserialize)]
struct LatestCommit {
    rev: String,
}

/// `net`-backed head source. The prober and the private handlers' head
/// fetches are internal work: it uses the internal transport (design-v4
/// §8.1 "Private-context hydration", §11 G-9).
pub struct NetHeadSource {
    net: NetClient,
}

impl NetHeadSource {
    #[must_use]
    pub fn new(nets: &Transports) -> Self {
        Self {
            net: nets.internal().client().clone(),
        }
    }
}

#[async_trait]
impl HeadSource for NetHeadSource {
    async fn latest_rev(&self, pds: &str, did: &str, timeout: Duration) -> Result<String, String> {
        let url = Url::parse_with_params(
            &format!("{}/xrpc/com.atproto.sync.getLatestCommit", pds.trim_end_matches('/')),
            &[("did", did)],
        )
        .map_err(|e| e.to_string())?;
        let response = self
            .net
            .get(url.as_str(), &RequestOptions::new(BodyCap::REPO_STATUS).timeout(timeout))
            .await
            .map_err(|e| e.to_string())?;
        if !response.is_success() {
            return Err(format!("getLatestCommit: HTTP {}", response.status.as_u16()));
        }
        let c: LatestCommit = response.json().map_err(|e| e.to_string())?;
        if !proto_blue::syntax::Tid::is_valid(&c.rev) {
            return Err(format!("getLatestCommit: invalid rev {:?}", c.rev));
        }
        Ok(c.rev)
    }
}

/// A DID's stored head, whether it is fresh, its PDS and its `applied_rev`.
type HeadRow = (Option<String>, bool, Option<String>, Option<String>);

/// A head with what the degraded fallback compares it against
/// (degraded-mode design §2.5): `applied_rev`, `chain_rev`, and when the
/// head was checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadState {
    pub head: String,
    pub applied: Option<String>,
    pub chain: Option<String>,
    pub checked_at: chrono::DateTime<chrono::Utc>,
}

/// A stored head read with no fetch (degraded-mode design §2.5): the DID
/// has no `sync_state` row, its head is older than `head_max_age_secs` (or
/// unknown), or it is fresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredHead {
    Untracked,
    Stale,
    Fresh(HeadState),
}

/// What one probe found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeResult {
    /// `head_rev ≤ applied_rev`.
    Current,
    /// `head_rev > applied_rev`, within the grace period.
    Behind,
    /// Past the grace period: readiness cleared and `authz` enqueued.
    DivergenceDetected,
    /// Past the grace period, but an `authz` job is already in flight.
    DivergenceAwaitingAuthz,
    Error(String),
}

impl ProbeResult {
    fn label(&self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Behind => "behind",
            Self::DivergenceDetected | Self::DivergenceAwaitingAuthz => "diverged",
            Self::Error(_) => "error",
        }
    }
}

/// The prober. Cheap to clone.
#[derive(Clone)]
pub struct HeadProber {
    pool: PgPool,
    scope: ScopeState,
    private: Arc<dyn PrivateTier>,
    source: Arc<dyn HeadSource>,
    budgets: HostBudgets,
    cfg: SyncConfig,
    /// The watermarks and freshness bound the degraded probe tests; `None`
    /// never enqueues (degraded-mode design §2.4).
    degraded: Option<(Watermarks, Duration)>,
}

impl HeadProber {
    #[must_use]
    pub fn new(
        pool: PgPool,
        scope: ScopeState,
        private: Arc<dyn PrivateTier>,
        source: Arc<dyn HeadSource>,
        budgets: HostBudgets,
        cfg: SyncConfig,
    ) -> Self {
        Self {
            pool,
            scope,
            private,
            source,
            budgets,
            cfg,
            degraded: None,
        }
    }

    /// With the degraded probe (degraded-mode design §2.4): while private
    /// reads are degraded, a repo whose chain has been behind its head for
    /// `sync.divergence_grace_secs` gets an `authz` job, its readiness kept.
    #[must_use]
    pub fn with_degraded(mut self, watermarks: Watermarks, bound: Duration) -> Self {
        self.degraded = Some((watermarks, bound));
        self
    }

    fn is_degraded(&self) -> bool {
        self.degraded.as_ref().is_some_and(|(w, bound)| w.degraded_at(chrono::Utc::now(), *bound))
    }

    #[allow(clippy::cast_precision_loss)]
    fn refresh_window_secs(&self) -> f64 {
        0.75 * self.cfg.head_max_age_secs as f64
    }

    /// Tracked DIDs with a known PDS, grouped by host, and those due for a
    /// probe (never checked, or older than 0.75 × the maximum head age).
    async fn plan(&self) -> Result<HashMap<String, Vec<(String, String)>>, sqlx::Error> {
        let tracked = self.scope.read_guard().await.tracked();
        let rows: Vec<(String, Option<String>, bool)> = sqlx::query_as(
            "SELECT a.did, a.pds_endpoint, \
               (s.head_checked_at IS NULL OR s.head_checked_at < now() - make_interval(secs => $2)) AS due \
             FROM actors a LEFT JOIN sync_state s ON s.did = a.did WHERE a.did = ANY($1)",
        )
        .bind(&tracked)
        .bind(self.refresh_window_secs())
        .fetch_all(&self.pool)
        .await?;
        let mut per_host: HashMap<String, (usize, Vec<(String, String)>)> = HashMap::new();
        for (did, pds, due) in rows {
            let Some(pds) = pds else { continue };
            let entry = per_host.entry(HostBudgets::host_of(&pds)).or_default();
            entry.0 += 1;
            if due {
                entry.1.push((did, pds));
            }
        }
        Ok(per_host
            .into_iter()
            .map(|(host, (tracked, due))| {
                self.budgets.set_probe_tracked(&host, tracked);
                (host, due)
            })
            .collect())
    }

    /// One pass: probe every due DID, hosts concurrently, each host under its
    /// own probe bucket. Returns the number probed.
    ///
    /// # Errors
    ///
    /// Database errors building the plan.
    pub async fn pass(&self) -> Result<usize, sqlx::Error> {
        let plan = self.plan().await?;
        let mut set = tokio::task::JoinSet::new();
        for (host, due) in plan {
            if due.is_empty() {
                continue;
            }
            let this = self.clone();
            set.spawn(async move {
                let mut n = 0;
                for (did, pds) in due {
                    this.budgets.acquire(&host, Class::Probe).await;
                    this.probe(&did, &pds).await;
                    n += 1;
                }
                n
            });
        }
        let mut total = 0;
        while let Some(n) = set.join_next().await {
            total += n.unwrap_or(0);
        }
        Ok(total)
    }

    /// Probe one DID and record the result.
    pub async fn probe(&self, did: &str, pds: &str) -> ProbeResult {
        let start = Instant::now();
        let result = match self.source.latest_rev(pds, did, PROBE_TIMEOUT).await {
            Ok(rev) => self.record_head(did, &rev).await.unwrap_or_else(|e| ProbeResult::Error(e.to_string())),
            Err(e) => ProbeResult::Error(e),
        };
        metrics::histogram!(METRIC_CHECK_SECONDS, "path" => "probe").record(start.elapsed().as_secs_f64());
        metrics::counter!(METRIC_CHECKS, "result" => result.label()).increment(1);
        if let ProbeResult::Error(error) = &result {
            tracing::debug!(event = "sync_head_check_failed", did, %error, "head probe failed");
        }
        result
    }

    /// Store a probed head and evaluate divergence.
    async fn record_head(&self, did: &str, rev: &str) -> Result<ProbeResult, sqlx::Error> {
        // Read guard before the connection (§6.11.7): the divergence branch
        // clears readiness like any other ordinary writer.
        let guard = self.scope.read_guard().await;
        if !guard.is_tracked(did) {
            // Rows exist only for tracked DIDs.
            return Ok(ProbeResult::Current);
        }
        let mut tx = self.pool.begin().await?;
        // `chain_behind_since` (degraded-mode design §2.4) is kept the way
        // `diverged_since` is, against `chain_rev`, on every probe.
        let (behind, overdue, chain_overdue): (bool, bool, bool) = sqlx::query_as(
            "INSERT INTO sync_state (did, head_rev, head_checked_at, diverged_since, chain_behind_since) \
             VALUES ($1, $2, now(), CASE WHEN $2 > '' THEN now() END, CASE WHEN $2 > '' THEN now() END) \
             ON CONFLICT (did) DO UPDATE SET head_rev = EXCLUDED.head_rev, head_checked_at = now(), \
               diverged_since = CASE \
                 WHEN EXCLUDED.head_rev > COALESCE(sync_state.applied_rev, '') \
                   THEN COALESCE(sync_state.diverged_since, now()) \
                 ELSE NULL END, \
               chain_behind_since = CASE \
                 WHEN EXCLUDED.head_rev > COALESCE(sync_state.chain_rev, '') \
                   THEN COALESCE(sync_state.chain_behind_since, now()) \
                 ELSE NULL END \
             RETURNING diverged_since IS NOT NULL, \
               COALESCE(diverged_since < now() - make_interval(secs => $3), false), \
               COALESCE(chain_behind_since < now() - make_interval(secs => $3), false)",
        )
        .bind(did)
        .bind(rev)
        .bind(self.cfg.divergence_grace_secs as f64)
        .fetch_one(&mut *tx)
        .await?;
        if !(behind && overdue) {
            if chain_overdue && self.is_degraded() {
                self.degraded_probe(&mut tx, did).await?;
            }
            tx.commit().await?;
            return Ok(if behind { ProbeResult::Behind } else { ProbeResult::Current });
        }
        // Past the grace. An authz job already queued or running will raise
        // applied_rev; re-queueing it would only restart it.
        let in_flight: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM backfill_jobs WHERE did = $1 AND depth = 'authz' \
             AND state IN ('queued', 'running'))",
        )
        .bind(did)
        .fetch_one(&mut *tx)
        .await?;
        if in_flight {
            tx.commit().await?;
            return Ok(ProbeResult::DivergenceAwaitingAuthz);
        }
        #[cfg(test)]
        crate::test_support::hooks::pause_in(crate::test_support::hooks::PROBER_HOLDS_SYNC_STATE, &self.pool, did).await;
        clear_readiness(&mut tx, self.private.as_ref(), did).await?;
        // Re-arm: a divergence that persists after this authz is detected
        // again one grace period later.
        sqlx::query("UPDATE sync_state SET diverged_since = NULL WHERE did = $1")
            .bind(did)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        drop(guard);
        metrics::counter!(METRIC_DIVERGENCE).increment(1);
        tracing::warn!(event = "sync_divergence_detected", did, head_rev = rev, "repo head ahead of applied_rev past the grace; readiness cleared, authz enqueued");
        Ok(ProbeResult::DivergenceDetected)
    }

    /// The degraded probe (degraded-mode design §2.4): an `authz` job for a
    /// repo whose chain has stayed behind its head past the grace, without
    /// clearing its readiness (the daily job's kind of run), skipping one
    /// with an `authz` job queued or running. Re-armed either way, so the
    /// next job comes one grace later at the earliest.
    async fn degraded_probe(&self, conn: &mut sqlx::PgConnection, did: &str) -> Result<(), sqlx::Error> {
        let in_flight: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM backfill_jobs WHERE did = $1 AND depth = 'authz' \
             AND state IN ('queued', 'running'))",
        )
        .bind(did)
        .fetch_one(&mut *conn)
        .await?;
        if !in_flight {
            jobs::enqueue_if_idle(&mut *conn, did, Depth::Authz, chrono::Utc::now()).await?;
            metrics::counter!(METRIC_DEGRADED_AUTHZ).increment(1);
            tracing::info!(event = "sync_degraded_authz_enqueued", did, "chain behind its head while degraded; authz enqueued");
        }
        sqlx::query("UPDATE sync_state SET chain_behind_since = now() WHERE did = $1")
            .bind(did)
            .execute(conn)
            .await?;
        Ok(())
    }

    /// The read path's head for `did` (§6.11.3): the stored head when checked
    /// within `head_max_age_secs`, else a synchronous fetch under the host's
    /// read-fetch bucket with `head_fetch_timeout_ms`. `None` means a uniform
    /// miss (no PDS known, no token, or the fetch failed). Never enqueues work.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn read_head(&self, did: &str) -> Result<Option<String>, sqlx::Error> {
        self.read_head_with(did, true).await
    }

    /// [`HeadProber::read_head`], with the synchronous fetch allowed only
    /// when `allow_fetch` (the caller's per-request cap,
    /// `sync.max_head_fetches_per_request`).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn read_head_with(&self, did: &str, allow_fetch: bool) -> Result<Option<String>, sqlx::Error> {
        Ok(self.read_head_applied(did, allow_fetch).await?.map(|(head, _)| head))
    }

    /// [`HeadProber::read_head_with`] with the DID's `applied_rev`, for the
    /// read path's comparison (fast-reads design F-3): `(head, applied_rev)`.
    /// With a fresh stored head this is **one statement**, the head and
    /// `applied_rev` read from the same row. After a synchronous fetch,
    /// `applied_rev` is the one the fetch's own write returns, as a separate
    /// read after it would see. `None` as `read_head_with`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn read_head_applied(&self, did: &str, allow_fetch: bool) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
        let start = Instant::now();
        let head = self.read_head_inner(did, allow_fetch).await;
        metrics::histogram!(METRIC_CHECK_SECONDS, "path" => "read").record(start.elapsed().as_secs_f64());
        head
    }

    async fn read_head_inner(&self, did: &str, allow_fetch: bool) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
        let row: Option<HeadRow> = sqlx::query_as(
            "SELECT s.head_rev, \
               COALESCE(s.head_checked_at > now() - make_interval(secs => $2), false), a.pds_endpoint, s.applied_rev \
             FROM actors a LEFT JOIN sync_state s ON s.did = a.did WHERE a.did = $1",
        )
        .bind(did)
        .bind(self.cfg.head_max_age_secs as f64)
        .fetch_optional(&self.pool)
        .await?;
        let Some((head, fresh, pds, applied)) = row else {
            return Ok(None);
        };
        if fresh && let Some(head) = head {
            return Ok(Some((head, applied)));
        }
        let Some(pds) = pds.filter(|_| allow_fetch) else {
            return Ok(None);
        };
        if !self.budgets.try_acquire(&HostBudgets::host_of(&pds), Class::ReadFetch) {
            metrics::counter!(METRIC_CHECKS, "result" => "read_budget_exhausted").increment(1);
            return Ok(None);
        }
        let timeout = Duration::from_millis(self.cfg.head_fetch_timeout_ms);
        let fetched = self.source.latest_rev(&pds, did, timeout).await;
        let Ok(rev) = fetched else {
            metrics::counter!(METRIC_CHECKS, "result" => "error").increment(1);
            return Ok(None);
        };
        metrics::counter!(METRIC_CHECKS, "result" => "read_fetch").increment(1);
        // No row (no `sync_state` yet): nothing applied, so `None`.
        let applied: Option<String> = sqlx::query_scalar(
            "UPDATE sync_state SET head_rev = $2, head_checked_at = now() WHERE did = $1 RETURNING applied_rev",
        )
        .bind(did)
        .bind(&rev)
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        Ok(Some((rev, applied)))
    }

    /// [`HeadProber::read_head_applied`] for the degraded fallback
    /// (degraded-mode design §2.5): the same head, with `chain_rev` and the
    /// head's check time from the same row, in one statement when the
    /// stored head is fresh; after a fetch, from the fetch's own write. Only
    /// a degraded request with the fallback on uses it, so a fresh request's
    /// statements are unchanged.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn read_head_state(&self, did: &str, allow_fetch: bool) -> Result<Option<HeadState>, sqlx::Error> {
        let start = Instant::now();
        let head = self.read_head_state_inner(did, allow_fetch).await;
        metrics::histogram!(METRIC_CHECK_SECONDS, "path" => "read").record(start.elapsed().as_secs_f64());
        head
    }

    async fn read_head_state_inner(&self, did: &str, allow_fetch: bool) -> Result<Option<HeadState>, sqlx::Error> {
        type Row = (Option<String>, bool, Option<String>, Option<String>, Option<String>, Option<chrono::DateTime<chrono::Utc>>);
        let row: Option<Row> = sqlx::query_as(
            "SELECT s.head_rev, \
               COALESCE(s.head_checked_at > now() - make_interval(secs => $2), false), a.pds_endpoint, s.applied_rev, \
               s.chain_rev, s.head_checked_at \
             FROM actors a LEFT JOIN sync_state s ON s.did = a.did WHERE a.did = $1",
        )
        .bind(did)
        .bind(self.cfg.head_max_age_secs as f64)
        .fetch_optional(&self.pool)
        .await?;
        let Some((head, fresh, pds, applied, chain, checked_at)) = row else {
            return Ok(None);
        };
        if fresh && let (Some(head), Some(checked_at)) = (head, checked_at) {
            return Ok(Some(HeadState { head, applied, chain, checked_at }));
        }
        let Some(pds) = pds.filter(|_| allow_fetch) else {
            return Ok(None);
        };
        if !self.budgets.try_acquire(&HostBudgets::host_of(&pds), Class::ReadFetch) {
            metrics::counter!(METRIC_CHECKS, "result" => "read_budget_exhausted").increment(1);
            return Ok(None);
        }
        let timeout = Duration::from_millis(self.cfg.head_fetch_timeout_ms);
        let Ok(rev) = self.source.latest_rev(&pds, did, timeout).await else {
            metrics::counter!(METRIC_CHECKS, "result" => "error").increment(1);
            return Ok(None);
        };
        metrics::counter!(METRIC_CHECKS, "result" => "read_fetch").increment(1);
        let written: Option<(Option<String>, Option<String>, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "UPDATE sync_state SET head_rev = $2, head_checked_at = now() WHERE did = $1 \
             RETURNING applied_rev, chain_rev, head_checked_at",
        )
        .bind(did)
        .bind(&rev)
        .fetch_optional(&self.pool)
        .await?;
        // No row: nothing applied, and no chain.
        let (applied, chain, checked_at) = written.unwrap_or((None, None, chrono::Utc::now()));
        Ok(Some(HeadState { head: rev, applied, chain, checked_at }))
    }

    /// A DID's stored head with no fetch (degraded-mode design §2.5, step
    /// 3′): the prober keeps every tracked DID's head fresh, so a stale one
    /// is a miss, and no fetch budget or read-fetch token is spent. One
    /// statement.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn stored_head(&self, did: &str) -> Result<StoredHead, sqlx::Error> {
        type Row = (Option<String>, bool, Option<String>, Option<String>, Option<chrono::DateTime<chrono::Utc>>);
        let row: Option<Row> = sqlx::query_as(
            "SELECT head_rev, COALESCE(head_checked_at > now() - make_interval(secs => $2), false), \
               applied_rev, chain_rev, head_checked_at \
             FROM sync_state WHERE did = $1",
        )
        .bind(did)
        .bind(self.cfg.head_max_age_secs as f64)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            None => StoredHead::Untracked,
            Some((Some(head), true, applied, chain, Some(checked_at))) => StoredHead::Fresh(HeadState { head, applied, chain, checked_at }),
            Some(_) => StoredHead::Stale,
        })
    }

    /// Run passes forever.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match self.pass().await {
                    Ok(0) => tokio::time::sleep(IDLE).await,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(event = "sync_prober_pass_failed", %error, "head prober pass failed");
                        tokio::time::sleep(IDLE).await;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;
    use crate::private_tier::DisabledPrivateTier;
    use crate::sync::budgets::BudgetRates;
    use crate::test_support::fresh_db;

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const PDS: &str = "https://pds.example.com";

    struct FakeHeads {
        rev: Mutex<String>,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl HeadSource for FakeHeads {
        async fn latest_rev(&self, _pds: &str, _did: &str, _timeout: Duration) -> Result<String, String> {
            *self.calls.lock().expect("lock") += 1;
            Ok(self.rev.lock().expect("lock").clone())
        }
    }

    fn rates() -> BudgetRates {
        BudgetRates {
            max_probe_per_host: 25.0,
            head_max_age_secs: 60.0,
            read_fetch_per_host: 20.0,
            backfill_per_host: 5.0,
            catch_up_per_host: 10.0,
        }
    }

    async fn exec(pool: &PgPool, sql: &str) {
        sqlx::query(sql).execute(pool).await.expect(sql);
    }

    #[tokio::test]
    async fn prober_advances_head_and_detects_divergence() {
        let Some(db) = fresh_db("head_prober").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO actors (did, pds_endpoint) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'https://pds.example.com')").await;
        exec(p, "INSERT INTO members (did, source, ready_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin', now())").await;
        exec(p, "INSERT INTO sync_state (did, applied_rev) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '3kaaaaaaaaaaa')").await;
        let mut sets = ScopeSets::default();
        sets.members.insert(DID.into());
        sets.scope.insert(DID.into());
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10);
        let heads = Arc::new(FakeHeads {
            rev: Mutex::new("3kaaaaaaaaaaa".into()),
            calls: Mutex::new(0),
        });
        let prober = HeadProber::new(
            p.clone(),
            scope,
            Arc::new(DisabledPrivateTier),
            heads.clone(),
            HostBudgets::new(rates()),
            SyncConfig::default(),
        );

        // In sync: head recorded, nothing diverged.
        assert_eq!(prober.pass().await.expect("pass"), 1);
        let (head, diverged): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as("SELECT head_rev, diverged_since FROM sync_state WHERE did = $1")
                .bind(DID)
                .fetch_one(p)
                .await
                .expect("sync_state");
        assert_eq!((head.as_deref(), diverged), (Some("3kaaaaaaaaaaa"), None));
        // Checked just now: not due again.
        assert_eq!(prober.pass().await.expect("pass"), 0);

        // The PDS moves ahead: behind, within the grace.
        *heads.rev.lock().expect("lock") = "3kbbbbbbbbbbb".into();
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Behind);
        let head: String = sqlx::query_scalar("SELECT head_rev FROM sync_state WHERE did = $1")
            .bind(DID)
            .fetch_one(p)
            .await
            .expect("head");
        assert_eq!(head, "3kbbbbbbbbbbb");

        // Still behind past the grace: readiness cleared, authz enqueued.
        exec(p, "UPDATE sync_state SET diverged_since = now() - interval '10 minutes'").await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::DivergenceDetected);
        let ready: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar("SELECT ready_at FROM members")
            .fetch_one(p)
            .await
            .expect("member");
        assert_eq!(ready, None);
        let job: String = sqlx::query_scalar("SELECT state FROM backfill_jobs WHERE did = $1 AND depth = 'authz'")
            .bind(DID)
            .fetch_one(p)
            .await
            .expect("authz job");
        assert_eq!(job, "queued");

        // While that authz is in flight, a later overdue probe does not
        // restart it.
        exec(p, "UPDATE sync_state SET diverged_since = now() - interval '10 minutes'").await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::DivergenceAwaitingAuthz);

        // The authz job applies up to the head: current again, re-armed.
        exec(p, "UPDATE sync_state SET applied_rev = '3kbbbbbbbbbbb'").await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Current);
        let diverged: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT diverged_since FROM sync_state WHERE did = $1")
                .bind(DID)
                .fetch_one(p)
                .await
                .expect("sync_state");
        assert_eq!(diverged, None);
        db.drop_db().await;
    }

    /// #736: a new tracked DID's first probe stamps `diverged_since`; the
    /// advance that brings `applied_rev` up to the head clears it, so a
    /// later divergence counts its grace from its own start.
    #[tokio::test]
    async fn catching_up_to_the_head_ends_the_divergence() {
        let Some(db) = fresh_db("head_prober_catch_up").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO actors (did, pds_endpoint) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'https://pds.example.com')").await;
        let mut sets = ScopeSets::default();
        sets.members.insert(DID.into());
        sets.scope.insert(DID.into());
        let heads = Arc::new(FakeHeads { rev: Mutex::new("3kbbbbbbbbbbb".into()), calls: Mutex::new(0) });
        let prober = HeadProber::new(
            p.clone(),
            ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10),
            Arc::new(DisabledPrivateTier),
            heads.clone(),
            HostBudgets::new(rates()),
            SyncConfig::default(),
        );
        let diverged = || async move {
            sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>("SELECT diverged_since FROM sync_state WHERE did = $1")
                .bind(DID)
                .fetch_one(p)
                .await
                .expect("sync_state")
        };
        // The first probe of a DID nothing has been applied for.
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Behind);
        assert!(diverged().await.is_some());
        // An advance short of the head leaves the divergence alone.
        let mut conn = p.acquire().await.expect("conn");
        crate::firehose::writer::advance_applied_rev(&mut conn, DID, "3kaaaaaaaaaaa", false).await.expect("advance");
        assert!(diverged().await.is_some(), "still behind the head");
        // The advance that reaches the head ends it, before any probe.
        crate::firehose::writer::advance_applied_rev(&mut conn, DID, "3kbbbbbbbbbbb", true).await.expect("advance");
        assert_eq!(diverged().await, None);
        // The settle path does the same (listed revs cover the rev).
        exec(p, "UPDATE sync_state SET diverged_since = now() - interval '1 hour', head_rev = '3kccccccccccc', \
                 post_listed_rev = '3kccccccccccc', audience_listed_rev = '3kccccccccccc'").await;
        crate::firehose::writer::advance_applied_rev_with_follows(&mut conn, DID, "3kccccccccccc", None).await.expect("settle");
        assert_eq!(diverged().await, None);
        drop(conn);
        // A new commit past the applied rev starts a fresh divergence, not
        // one dated from the first probe.
        *heads.rev.lock().expect("lock") = "3kddddddddddd".into();
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Behind);
        let since = diverged().await.expect("diverged");
        assert!(chrono::Utc::now() - since < chrono::TimeDelta::minutes(1), "{since}");
        db.drop_db().await;
    }

    #[tokio::test]
    async fn read_head_uses_fresh_heads_and_never_waits() {
        let Some(db) = fresh_db("head_read_path").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO actors (did, pds_endpoint) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'https://pds.example.com')").await;
        exec(p, "INSERT INTO sync_state (did, head_rev, head_checked_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '3kaaaaaaaaaaa', now())").await;
        let heads = Arc::new(FakeHeads {
            rev: Mutex::new("3kccccccccccc".into()),
            calls: Mutex::new(0),
        });
        let budgets = HostBudgets::new(BudgetRates {
            read_fetch_per_host: 1.0,
            ..rates()
        });
        let prober = HeadProber::new(
            p.clone(),
            ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10),
            Arc::new(DisabledPrivateTier),
            heads.clone(),
            budgets,
            SyncConfig::default(),
        );
        // Fresh: served from sync_state, no fetch.
        assert_eq!(prober.read_head(DID).await.expect("read"), Some("3kaaaaaaaaaaa".into()));
        assert_eq!(*heads.calls.lock().expect("lock"), 0);
        // Stale: one synchronous fetch, then the bucket is empty and the
        // next stale read misses instead of waiting.
        exec(p, "UPDATE sync_state SET head_checked_at = now() - interval '5 minutes'").await;
        assert_eq!(prober.read_head(DID).await.expect("read"), Some("3kccccccccccc".into()));
        exec(p, "UPDATE sync_state SET head_checked_at = now() - interval '5 minutes'").await;
        assert_eq!(prober.read_head(DID).await.expect("read"), None);
        assert_eq!(*heads.calls.lock().expect("lock"), 1);
        assert_eq!(prober.read_head("did:plc:unknown").await.expect("read"), None);
        db.drop_db().await;
    }

    /// T-F4 (fast-reads design F-3): the head with `applied_rev` from one
    /// read gives what the head and a separate `applied_rev` read gave:
    /// fresh and current, fresh and behind, stale with a fetch, no
    /// `sync_state` row, no PDS, an unknown DID.
    #[tokio::test]
    async fn the_head_and_applied_rev_come_together() {
        let Some(db) = fresh_db("head_read_applied").await else {
            return;
        };
        let p = &db.pool;
        const NO_ROW: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        const NO_PDS: &str = "did:plc:cccccccccccccccccccccccc";
        exec(p, "INSERT INTO actors (did, pds_endpoint) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'https://pds.example.com'), \
                 ('did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', 'https://pds.example.com'), ('did:plc:cccccccccccccccccccccccc', NULL)").await;
        exec(p, "INSERT INTO sync_state (did, head_rev, applied_rev, head_checked_at) VALUES \
                 ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '3kaaaaaaaaaaa', '3kaaaaaaaaaaa', now()), \
                 ('did:plc:cccccccccccccccccccccccc', '3kaaaaaaaaaaa', '3kaaaaaaaaaaa', now() - interval '5 minutes')").await;
        let heads = Arc::new(FakeHeads { rev: Mutex::new("3kccccccccccc".into()), calls: Mutex::new(0) });
        let prober = HeadProber::new(
            p.clone(),
            ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10),
            Arc::new(DisabledPrivateTier),
            heads.clone(),
            HostBudgets::new(rates()),
            SyncConfig::default(),
        );
        let read = |did: &'static str| {
            let prober = prober.clone();
            async move { prober.read_head_applied(did, true).await.expect("read") }
        };
        let some = |head: &str, applied: Option<&str>| Some((head.to_string(), applied.map(str::to_string)));
        // Fresh and current, then fresh and behind: no fetch.
        assert_eq!(read(DID).await, some("3kaaaaaaaaaaa", Some("3kaaaaaaaaaaa")));
        exec(p, "UPDATE sync_state SET head_rev = '3kbbbbbbbbbbb' WHERE did = 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa'").await;
        assert_eq!(read(DID).await, some("3kbbbbbbbbbbb", Some("3kaaaaaaaaaaa")));
        assert_eq!(*heads.calls.lock().expect("lock"), 0);
        // Stale: one fetch, its head and the row's applied_rev.
        exec(p, "UPDATE sync_state SET head_checked_at = now() - interval '5 minutes' WHERE did = 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa'").await;
        assert_eq!(read(DID).await, some("3kccccccccccc", Some("3kaaaaaaaaaaa")));
        assert_eq!(*heads.calls.lock().expect("lock"), 1);
        // No sync_state row: the fetched head with nothing applied.
        assert_eq!(read(NO_ROW).await, some("3kccccccccccc", None));
        // A stale head and no PDS, and an unknown DID: a miss.
        assert_eq!(read(NO_PDS).await, None);
        assert_eq!(read("did:plc:unknown").await, None);
        // And `read_head_with` is the same head.
        assert_eq!(prober.read_head_with(DID, false).await.expect("read"), Some("3kccccccccccc".into()));
        db.drop_db().await;
    }

    /// T-DM15 (degraded-mode design §2.4): `chain_behind_since` is set when
    /// a probe finds the head above `chain_rev` and cleared when it does
    /// not; while degraded, a DID behind past the grace gets one `authz` job,
    /// keeps its readiness, and gets no second job within the next grace;
    /// while fresh, none.
    #[tokio::test]
    async fn degraded_probe_enqueues_authz_for_a_chain_behind() {
        let Some(db) = fresh_db("dm15_degraded_probe").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO actors (did, pds_endpoint) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'https://pds.example.com')").await;
        exec(p, "INSERT INTO members (did, source, ready_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin', now())").await;
        // Applied up to the head, so no divergence; the chain is behind.
        exec(p, "INSERT INTO sync_state (did, applied_rev, chain_rev) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '3kbbbbbbbbbbb', '3kaaaaaaaaaaa')").await;
        let mut sets = ScopeSets::default();
        sets.members.insert(DID.into());
        sets.scope.insert(DID.into());
        let heads = Arc::new(FakeHeads { rev: Mutex::new("3kbbbbbbbbbbb".into()), calls: Mutex::new(0) });
        let watermarks = Watermarks::new(std::collections::BTreeMap::new(), vec!["r".into()]);
        let prober = HeadProber::new(
            p.clone(),
            ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10),
            Arc::new(DisabledPrivateTier),
            heads.clone(),
            HostBudgets::new(rates()),
            SyncConfig::default(),
        )
        .with_degraded(watermarks.clone(), Duration::from_secs(120));
        let behind_since = || async move {
            sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>("SELECT chain_behind_since FROM sync_state WHERE did = $1")
                .bind(DID)
                .fetch_one(p)
                .await
                .expect("sync_state")
        };
        let jobs = || async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM backfill_jobs WHERE did = $1 AND depth = 'authz'")
                .bind(DID)
                .fetch_one(p)
                .await
                .expect("jobs")
        };
        let ready = || async move {
            sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>("SELECT ready_at FROM members WHERE did = $1")
                .bind(DID)
                .fetch_one(p)
                .await
                .expect("member")
        };
        // Degraded (the relay never reported). The first probe stamps the
        // chain's clock; within the grace nothing is queued.
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Current);
        let first = behind_since().await.expect("stamped");
        assert_eq!(jobs().await, 0);
        // Past the grace: one authz, readiness kept, the clock re-armed.
        exec(p, "UPDATE sync_state SET chain_behind_since = now() - interval '10 minutes'").await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Current);
        assert_eq!(jobs().await, 1);
        assert!(ready().await.is_some(), "the degraded probe keeps readiness");
        let rearmed = behind_since().await.expect("re-armed");
        assert!(rearmed >= first);
        // Within the next grace (the job done, say): no second job.
        exec(p, "UPDATE backfill_jobs SET state = 'done'").await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Current);
        let state: String = sqlx::query_scalar("SELECT state FROM backfill_jobs WHERE did = $1").bind(DID).fetch_one(p).await.expect("job");
        assert_eq!(state, "done");
        // Past it with the job queued: skipped, as divergence skips.
        exec(p, "UPDATE backfill_jobs SET state = 'queued'").await;
        exec(p, "UPDATE sync_state SET chain_behind_since = now() - interval '10 minutes'").await;
        let generation = || async move {
            sqlx::query_scalar::<_, i64>("SELECT generation::bigint FROM backfill_jobs WHERE did = $1").bind(DID).fetch_one(p).await.expect("gen")
        };
        let before = generation().await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Current);
        assert_eq!(generation().await, before, "a queued job is not reset");
        exec(p, "UPDATE backfill_jobs SET state = 'done'").await;

        // Not degraded: past the grace, nothing is queued.
        watermarks.observe("r", chrono::Utc::now());
        exec(p, "UPDATE sync_state SET chain_behind_since = now() - interval '10 minutes'").await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Current);
        let state: String = sqlx::query_scalar("SELECT state FROM backfill_jobs WHERE did = $1").bind(DID).fetch_one(p).await.expect("job");
        assert_eq!(state, "done", "fresh: no job");
        assert!(behind_since().await.is_some(), "still behind: the clock runs");

        // The chain reaches the head: the clock clears.
        exec(p, "UPDATE sync_state SET chain_rev = '3kbbbbbbbbbbb'").await;
        assert_eq!(prober.probe(DID, PDS).await, ProbeResult::Current);
        assert_eq!(behind_since().await, None);
        // A NULL chain is behind any head.
        exec(p, "UPDATE sync_state SET chain_rev = NULL").await;
        prober.probe(DID, PDS).await;
        assert!(behind_since().await.is_some());
        db.drop_db().await;
    }
}
