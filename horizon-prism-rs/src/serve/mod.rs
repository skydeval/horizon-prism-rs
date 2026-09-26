//! `serve` (design-v2 §3.3): configuration, the migration gate, the shared
//! `PgPool` / Redis / `NetClient` / `IdentityResolver` / `ScopeState`, the
//! background tasks, and the HTTP listener.
//!
//! The HTTP surface is a placeholder until the XRPC items land: only
//! `GET /xrpc/_health` (§7.1).

pub mod health;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use metrics_exporter_prometheus::PrometheusBuilder;
use redis::aio::ConnectionManager;
use tokio::net::TcpListener;

use crate::aggregations::counts::CountRefresher;
use crate::aggregations::{PurgeLifecycle, Purger, Rebuild, ScopeState};
use crate::config::AppConfig;
use crate::db::retention::{PublicRetention, RetentionInputs, RetentionSettings, RetentionTask, spawn_scheduler};
use crate::db::{GateError, ScopeGuard, StorageSampler, enforce_gate};
use crate::admin_executor::{AdminExecutor, Handlers};
use crate::firehose::backfill::{Backfill, BackfillSource, NetSource};
use crate::sync::{BudgetRates, HeadProber, HostBudgets, NetHeadSource};
use crate::firehose::consumer::{Consumer, ConsumerStatus, spawn_lag_sampler};
use crate::firehose::coverage::CoverageTracker;
use crate::firehose::watermark::Watermarks;
use crate::firehose::writer::Writer;
use crate::identity::{IdentityRefresher, IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::microcosm::ConstellationClient;
use crate::net::{NetClient, NetConfig};
use crate::private_tier::{DisabledPrivateTier, PrivateTier};
use health::{AuditLatch, HealthState};

/// Exit code for configuration errors and the migration gate (EX_CONFIG).
pub const EXIT_CONFIG: u8 = 78;
/// Timeout for the startup Redis connection.
pub const REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A startup failure, with the process exit code to use.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct StartupError {
    pub message: String,
    pub exit_code: u8,
}

impl StartupError {
    fn new(exit_code: u8, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exit_code,
        }
    }
}

/// `retention` inputs from `ScopeState`. Private row authors come from the
/// private tier (item 6); until then there are none.
struct ScopeRetentionInputs {
    scope: ScopeState,
}

#[async_trait]
impl RetentionInputs for ScopeRetentionInputs {
    async fn private_owners(&self) -> Result<Vec<String>, sqlx::Error> {
        let mut owners: Vec<String> = self.scope.snapshot().await.private_owners.into_iter().collect();
        owners.sort();
        Ok(owners)
    }

    async fn private_row_authors(&self) -> Result<Vec<String>, sqlx::Error> {
        Ok(Vec::new())
    }
}

fn init_tracing(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .try_init();
}

async fn redis_manager(url: &str) -> Result<ConnectionManager, StartupError> {
    let client = redis::Client::open(url).map_err(|e| StartupError::new(EXIT_CONFIG, format!("redis url: {e}")))?;
    tokio::time::timeout(REDIS_CONNECT_TIMEOUT, ConnectionManager::new(client))
        .await
        .map_err(|_| StartupError::new(1, "redis connect timed out"))?
        .map_err(|e| StartupError::new(1, format!("redis connect: {e}")))
}

/// Run `serve` until SIGTERM or Ctrl-C.
///
/// # Errors
///
/// A [`StartupError`] carrying the exit code.
pub async fn run() -> Result<(), StartupError> {
    let cfg = AppConfig::load_from_env().map_err(|e| StartupError::new(EXIT_CONFIG, format!("config_error: {e}")))?;
    cfg.validate()
        .map_err(|e| StartupError::new(EXIT_CONFIG, format!("config_error: {e}")))?;
    init_tracing(&cfg.env.log_level);
    PrometheusBuilder::new()
        .with_http_listener(cfg.file.metrics.bind)
        .install()
        .map_err(|e| StartupError::new(1, format!("metrics exporter: {e}")))?;

    let pool = crate::db::connect(&cfg)
        .await
        .map_err(|e| StartupError::new(1, format!("database connect: {e}")))?;
    match enforce_gate(&pool, cfg.env.allow_startup_migrate).await {
        Ok(outcome) => tracing::info!(event = "migration_gate_passed", ?outcome, "migration gate passed"),
        Err(e) => {
            let code = u8::try_from(GateError::exit_code(&e)).unwrap_or(1);
            return Err(StartupError::new(code, format!("migration gate: {e}")));
        }
    }
    let redis = redis_manager(&cfg.env.redis_url).await?;
    let consumer_redis = redis_manager(&cfg.env.redis_url).await?;
    let net = NetClient::new(&NetConfig::from_app(&cfg)).map_err(|e| StartupError::new(1, format!("net client: {e}")))?;

    // Private tier: item 6 replaces this.
    let private: Arc<dyn PrivateTier> = Arc::new(DisabledPrivateTier);
    let scope = ScopeState::load(
        &pool,
        private.as_ref(),
        cfg.file.scope.policy,
        cfg.file.scope.max_follow_scope_per_member,
    )
    .await
    .map_err(|e| StartupError::new(1, format!("loading scope state: {e}")))?;
    let policy: Arc<dyn PersistencePolicy> = Arc::new(scope.clone());
    let resolver = IdentityResolver::new(net.clone(), pool.clone(), IdentitySettings::from_app(&cfg), policy);
    let writer = Writer::new(pool.clone(), scope.clone(), Arc::clone(&private));
    let purger = Purger::new(pool.clone(), scope.clone(), Arc::clone(&private));
    let constellation = ConstellationClient::new(net.clone(), &cfg.file.microcosm.constellation_url);

    let mut tasks = Vec::new();

    // Purges interrupted by a crash resume first (§6.11.7).
    {
        let purger = purger.clone();
        tasks.push(tokio::spawn(async move {
            match purger.resume_all().await {
                Ok(n) => tracing::info!(event = "purges_resumed", purges = n, "interrupted purges resumed"),
                Err(error) => tracing::error!(event = "purge_resume_failed", %error, "listing interrupted purges failed"),
            }
        }));
    }

    // Identity refresher, with the account_deleted purge hook.
    tasks.push(IdentityRefresher::new(pool.clone(), resolver.clone(), Arc::new(PurgeLifecycle::new(purger.clone()))).spawn());

    // Firehose: watermarks, coverage, consumer, lag sampler.
    let fh = cfg.file.firehose.clone();
    let watermarks = Watermarks::new(fh.relay_labels.clone(), fh.private_relays.clone());
    watermarks
        .restore(&pool)
        .await
        .map_err(|e| StartupError::new(1, format!("restoring watermarks: {e}")))?;
    tasks.push(watermarks.spawn_persister(pool.clone()));
    let coverage_labels: Vec<String> = fh.relay_labels.values().cloned().collect();
    let (coverage, coverage_task) = CoverageTracker::new(
        coverage_labels,
        Duration::from_secs(fh.coverage_window_secs),
        usize::try_from(fh.coverage_capacity).unwrap_or(usize::MAX),
    )
    .spawn();
    tasks.push(coverage_task);
    let status = Arc::new(ConsumerStatus::default());
    tasks.push(
        Consumer::new(
            fh.clone(),
            consumer_redis,
            writer.clone(),
            watermarks.clone(),
            coverage,
            Arc::clone(&status),
        )
        .spawn(),
    );
    tasks.push(spawn_lag_sampler(redis.clone(), fh.clone()));

    // Retention and the storage sampler.
    let retention: Vec<Arc<dyn RetentionTask>> = vec![Arc::new(PublicRetention::new(Arc::new(ScopeRetentionInputs {
        scope: scope.clone(),
    })))];
    let guard: Arc<dyn ScopeGuard> = Arc::new(scope.clone());
    tasks.push(spawn_scheduler(
        pool.clone(),
        guard,
        retention,
        RetentionSettings::from(&cfg.file.retention),
        cfg.file.retention.run_at_utc,
    ));
    tasks.push(
        StorageSampler {
            pool: pool.clone(),
            redis: redis.clone(),
            stream_key: fh.stream_key.clone(),
            budget_bytes: cfg.file.storage.budget_bytes,
        }
        .spawn(),
    );

    // Per-host PDS budgets (§6.11.3): probe, read-fetch and backfill buckets.
    let budgets = HostBudgets::new(BudgetRates::from_app(&cfg));

    // Backfill, count refresher, weekly rebuild.
    let source: Arc<dyn BackfillSource> = Arc::new(NetSource::new(
        pool.clone(),
        resolver.clone(),
        constellation.clone(),
        budgets.clone(),
    ));
    let backfill = Backfill::new(writer.clone(), Arc::clone(&source), cfg.file.backfill.clone(), fh.deterministic_attempts);
    tasks.extend(
        backfill
            .spawn()
            .await
            .map_err(|e| StartupError::new(1, format!("starting backfill: {e}")))?,
    );
    tasks.push(CountRefresher::new(pool.clone(), redis.clone(), constellation, &cfg.file.counts).spawn());
    let rebuild = Rebuild::new(pool.clone(), scope.clone(), Arc::clone(&private), cfg.file.scope.rebuild_grace_days);
    tasks.push(rebuild.clone().spawn_weekly());

    // Repo-head prober (§6.11.3).
    tasks.push(
        HeadProber::new(
            pool.clone(),
            scope.clone(),
            Arc::clone(&private),
            Arc::new(NetHeadSource::new(net.clone())),
            budgets,
            cfg.file.sync.clone(),
        )
        .spawn(),
    );

    // Admin executor (§3.3).
    tasks.push(
        AdminExecutor::new(Handlers {
            pool: pool.clone(),
            scope: scope.clone(),
            private: Arc::clone(&private),
            writer: writer.clone(),
            purger: purger.clone(),
            rebuild,
            redis: redis.clone(),
            source,
            dead_letter_stream: fh.dead_letter_stream.clone(),
            deterministic_attempts: fh.deterministic_attempts,
        })
        .spawn(),
    );

    // HTTP.
    let state = HealthState {
        pool: pool.clone(),
        redis,
        watermarks,
        status,
        audit: AuditLatch::default(),
    };
    let listener = TcpListener::bind(cfg.file.http.bind)
        .await
        .map_err(|e| StartupError::new(1, format!("binding {}: {e}", cfg.file.http.bind)))?;
    tracing::info!(event = "serve_started", bind = %cfg.file.http.bind, tasks = tasks.len(), "serving");
    axum::serve(listener, health::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| StartupError::new(1, format!("http server: {e}")))?;
    for t in tasks {
        t.abort();
    }
    tracing::info!(event = "serve_stopped", "shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
}

/// `healthcheck`: `GET http://127.0.0.1:3030/xrpc/_health`; success iff 200
/// with `"ok": true` (§3.3). A plain client: `net` refuses loopback.
pub async fn healthcheck() -> bool {
    let Ok(client) = reqwest::Client::builder().timeout(Duration::from_secs(5)).build() else {
        return false;
    };
    let Ok(response) = client.get("http://127.0.0.1:3030/xrpc/_health").send().await else {
        return false;
    };
    if response.status() != reqwest::StatusCode::OK {
        return false;
    }
    response
        .json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|v| v.get("ok").and_then(serde_json::Value::as_bool))
        .unwrap_or(false)
}
