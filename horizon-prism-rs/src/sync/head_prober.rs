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
use crate::firehose::writer::clear_readiness;
use crate::net::{BodyCap, NetClient, RequestOptions, Transports};
use crate::private_tier::PrivateTier;

pub const METRIC_CHECKS: &str = "sync_head_check_total";
pub const METRIC_CHECK_SECONDS: &str = "sync_head_check_seconds";
pub const METRIC_DIVERGENCE: &str = "sync_divergence_detected_total";

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
        }
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
        metrics::histogram!(METRIC_CHECK_SECONDS).record(start.elapsed().as_secs_f64());
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
        let (behind, overdue): (bool, bool) = sqlx::query_as(
            "INSERT INTO sync_state (did, head_rev, head_checked_at, diverged_since) \
             VALUES ($1, $2, now(), CASE WHEN $2 > '' THEN now() END) \
             ON CONFLICT (did) DO UPDATE SET head_rev = EXCLUDED.head_rev, head_checked_at = now(), \
               diverged_since = CASE \
                 WHEN EXCLUDED.head_rev > COALESCE(sync_state.applied_rev, '') \
                   THEN COALESCE(sync_state.diverged_since, now()) \
                 ELSE NULL END \
             RETURNING diverged_since IS NOT NULL, \
               COALESCE(diverged_since < now() - make_interval(secs => $3), false)",
        )
        .bind(did)
        .bind(rev)
        .bind(self.cfg.divergence_grace_secs as f64)
        .fetch_one(&mut *tx)
        .await?;
        if !behind {
            tx.commit().await?;
            return Ok(ProbeResult::Current);
        }
        if !overdue {
            tx.commit().await?;
            return Ok(ProbeResult::Behind);
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
        let row: Option<(Option<String>, bool, Option<String>)> = sqlx::query_as(
            "SELECT s.head_rev, \
               COALESCE(s.head_checked_at > now() - make_interval(secs => $2), false), a.pds_endpoint \
             FROM actors a LEFT JOIN sync_state s ON s.did = a.did WHERE a.did = $1",
        )
        .bind(did)
        .bind(self.cfg.head_max_age_secs as f64)
        .fetch_optional(&self.pool)
        .await?;
        let Some((head, fresh, pds)) = row else {
            return Ok(None);
        };
        if fresh && head.is_some() {
            return Ok(head);
        }
        let Some(pds) = pds.filter(|_| allow_fetch) else {
            return Ok(None);
        };
        if !self.budgets.try_acquire(&HostBudgets::host_of(&pds), Class::ReadFetch) {
            metrics::counter!(METRIC_CHECKS, "result" => "read_budget_exhausted").increment(1);
            return Ok(None);
        }
        let timeout = Duration::from_millis(self.cfg.head_fetch_timeout_ms);
        let start = Instant::now();
        let fetched = self.source.latest_rev(&pds, did, timeout).await;
        metrics::histogram!(METRIC_CHECK_SECONDS).record(start.elapsed().as_secs_f64());
        let Ok(rev) = fetched else {
            metrics::counter!(METRIC_CHECKS, "result" => "error").increment(1);
            return Ok(None);
        };
        metrics::counter!(METRIC_CHECKS, "result" => "read_fetch").increment(1);
        sqlx::query(
            "UPDATE sync_state SET head_rev = $2, head_checked_at = now() WHERE did = $1",
        )
        .bind(did)
        .bind(&rev)
        .execute(&self.pool)
        .await?;
        Ok(Some(rev))
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
}
