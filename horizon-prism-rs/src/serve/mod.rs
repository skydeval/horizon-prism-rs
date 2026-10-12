//! `serve` (design-v2 §3.3): configuration, the migration gate, the shared
//! `PgPool` / Redis / `NetClient` / `IdentityResolver` / `ScopeState`, the
//! background tasks, and the HTTP listener.
//!
//! HTTP: the `app.bsky.*` surface (§7.1), the `app.nearhorizon.private.*`
//! endpoints (§7.9), Thread Lens (§10), `resolveDid` (§8.7), `requestRecord` (§5.9),
//! `/.well-known/did.json` and `GET /xrpc/_health`.

pub mod health;

use std::net::SocketAddr;
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
use crate::db::schema_backfill::{self, SchemaBackfill};
use crate::db::{GateError, ScopeGuard, StorageSampler, enforce_gate};
use crate::admin_executor::{AdminExecutor, Handlers};
use crate::firehose::backfill::{Backfill, BackfillSource, NetSource};
use crate::sync::{BudgetRates, HeadProber, HostBudgets, NetHeadSource};
use crate::firehose::consumer::{Consumer, ConsumerStatus, spawn_lag_sampler};
use crate::firehose::coverage::CoverageTracker;
use crate::firehose::watermark::Watermarks;
use crate::firehose::writer::Writer;
use crate::identity::fetch_cell::AuthBudget;
use crate::identity::{IdentityRefresher, IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::microcosm::ConstellationClient;
use crate::net::{NetClient, NetConfig};
use crate::aggregations::enroll::AutoEnroller;
use crate::constellation::resolve_did::ResolveDidState;
use crate::constellation::{Cascade, DidLookup, RequestLookup, Router as DidRouter, RoutingTable};
use crate::hydration::upstream::{PdsLister, RealUpstreamSource};
use crate::hydration::{HydrationConfig, Hydrator, ReadCache};
use crate::microcosm::SlingshotClient;
use crate::threadlens::{Ladder, ThreadLensState};
use crate::kryphocron_glue::GlueContext;
use crate::private_tier::handlers::PrivateState;
use crate::private_tier::retention::PrivateRetention;
use crate::private_tier::{KryphocronPrivateTier, PrivateTier};
use crate::xrpc::backstop::{Backstop, VerifyLimits};
use crate::xrpc::member_keys::MemberKeys;
use crate::xrpc::pools::{HostBuckets, RequesterLimit, Shares};
use crate::xrpc::{AppState, RateLimits};
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

/// `retention` inputs: the owner set from `ScopeState`, and private row
/// authors from the private tier (after `PrivateRetention` ran).
struct ScopeRetentionInputs {
    scope: ScopeState,
    pool: sqlx::PgPool,
}

#[async_trait]
impl RetentionInputs for ScopeRetentionInputs {
    async fn private_owners(&self) -> Result<Vec<String>, sqlx::Error> {
        let mut owners: Vec<String> = self.scope.snapshot().await.private_owners.into_iter().collect();
        owners.sort();
        Ok(owners)
    }

    async fn private_row_authors(&self) -> Result<Vec<String>, sqlx::Error> {
        crate::private_tier::retention::row_authors(&self.pool).await
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

    let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
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
    let writer = Writer::new(pool.clone(), scope.clone(), Arc::clone(&private))
        .with_subscribed_post_max_age_secs(cfg.file.notifications.subscribed_post_max_age_secs);
    let purger = Purger::new(pool.clone(), scope.clone(), Arc::clone(&private));
    let constellation = ConstellationClient::new(net.clone(), &cfg.file.microcosm.constellation_url);

    let mut tasks = Vec::new();

    // Every upstream a public request reaches has one request bucket,
    // split into pools and per-requester shares (design-v3 §II-8.3).
    let shares = Shares::from_config(&cfg.file);
    // The block-list index (design-v3 §5.14), only when configured.
    let farsight = match (&cfg.file.farsight.url, &cfg.env.farsight_token) {
        (Some(url), Some(token)) => {
            let client = crate::farsight::FarsightClient::new(net.clone(), url, token.expose())
                .ok_or_else(|| StartupError::new(78, "FARSIGHT_TOKEN is not a usable header value".to_string()))?;
            tracing::info!(event = "farsight_enabled", "inbound list blocks are read from Farsight");
            Some(crate::farsight::Farsight::new(client, &cfg.file.farsight, &shares))
        }
        _ => None,
    };
    let microcosm = &cfg.file.microcosm;
    let identity = &cfg.file.identity;
    // Identity work for requests: the requester's identity share, and PLC's
    // request bucket with its issuer-refresh slice.
    let identity_share = RequesterLimit::new(
        "identity",
        identity.requester_resolutions_per_sec.0,
        identity.requester_resolutions_burst,
        cfg.file.constellation.requester_share_max_entries,
    );
    let (plc_bucket, issuer_slice) = shares.bucket_with_slice(
        "plc",
        identity.plc_request_rate_per_sec.0,
        identity.plc_request_burst,
        Duration::from_secs(identity.member_pool_refresh_secs),
    );
    let member_keys = MemberKeys::default();

    // Client authentication (§6.7, §6.8): service identity and the JWT path.
    let auth_budget = AuthBudget {
        identity: identity_share.clone(),
        plc: plc_bucket.clone(),
        slice: issuer_slice,
        members: member_keys.clone(),
    };
    let (glue, issuer_drainer) = GlueContext::new(&cfg, resolver.clone(), auth_budget)
        .map_err(|e| StartupError::new(EXIT_CONFIG, format!("config_error: {e}")))?;
    tasks.push(issuer_drainer);

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
    // Private retention runs first (§3.2), so the public `actors` step sees
    // the private row authors that remain.
    let retention: Vec<Arc<dyn RetentionTask>> = vec![
        Arc::new(PrivateRetention::new(scope.clone())),
        Arc::new(PublicRetention::new(Arc::new(ScopeRetentionInputs {
            scope: scope.clone(),
            pool: pool.clone(),
        }))),
    ];
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
    let community_patterns: Vec<String> =
        cfg.file.constellation.community().map(|n| n.pds_patterns.clone()).unwrap_or_default();
    let backfill = Backfill::new(writer.clone(), Arc::clone(&source), cfg.file.backfill.clone(), fh.deterministic_attempts)
        .with_community_patterns(community_patterns.clone())
        .with_farsight(farsight.clone());
    tasks.extend(
        backfill
            .spawn()
            .await
            .map_err(|e| StartupError::new(1, format!("starting backfill: {e}")))?,
    );
    tasks.push(CountRefresher::new(pool.clone(), redis.clone(), constellation.clone(), &cfg.file.counts).spawn());
    let rebuild = Rebuild::new(pool.clone(), scope.clone(), Arc::clone(&private), cfg.file.scope.rebuild_grace_days);
    tasks.push(rebuild.clone().spawn_weekly());

    // Repo-head prober (§6.11.3); the private pipeline shares it.
    let prober = HeadProber::new(
        pool.clone(),
        scope.clone(),
        Arc::clone(&private),
        Arc::new(NetHeadSource::new(net.clone())),
        budgets.clone(),
        cfg.file.sync.clone(),
    );
    tasks.push(prober.clone().spawn());

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
            source: Arc::clone(&source),
            dead_letter_stream: fh.dead_letter_stream.clone(),
            deterministic_attempts: fh.deterministic_attempts,
            community_patterns: Arc::new(community_patterns.clone()),
            farsight: farsight.clone(),
        })
        .spawn(),
    );

    // Constellation routing (§8): the table, the `route` cache over the
    // identity resolver, one client per outbound network, the cascade, and
    // the upstream source behind hydration's seam.
    let table = Arc::new(RoutingTable::from_config(&cfg.file.constellation));
    // Request-path identity work: one token of the requester's identity
    // share per DID or handle, and PLC's request bucket per PLC fetch.
    let lookup = RequestLookup::new(resolver.clone(), identity_share, plc_bucket);
    let did_router = DidRouter::new(Arc::clone(&table), Arc::new(lookup) as Arc<dyn DidLookup>);
    let cascade = Cascade::new(
        Arc::clone(&table),
        crate::appview_clients::from_config(&cfg.file.constellation, &net, &glue.service_key),
        &shares,
    );
    let read_cache = ReadCache::new(cfg.file.cache.read_time_bytes);
    let slingshot = SlingshotClient::new(net.clone(), &microcosm.slingshot_url).metered(shares.bucket(
        "slingshot",
        microcosm.slingshot_rate_per_sec.0,
        microcosm.slingshot_burst,
    ));
    let upstream = RealUpstreamSource::new(
        did_router.clone(),
        cascade.clone(),
        slingshot.clone(),
        PdsLister::new(
            net.clone(),
            HostBuckets::new(
                &shares,
                cfg.file.sync.public_fallback_rate_per_host.0,
                cfg.file.sync.max_public_fallback_hosts,
            ),
        ),
        read_cache.clone(),
    )
    .with_list_read_pages(cfg.file.lists.read_listing_pages);

    // HTTP.
    let hydrator = Hydrator::new(
        pool.clone(),
        scope.clone(),
        Some(ConstellationClient::new(net.clone(), &cfg.file.microcosm.constellation_url)),
        Some(redis.clone()),
        Arc::new(upstream),
        read_cache,
        HydrationConfig {
            labeler_did: glue.service_key.did().to_string(),
            counts: cfg.file.counts.clone(),
            thread: cfg.file.thread.clone(),
            thread_mutes_max: cfg.file.thread_mutes.max_per_actor,
            activity_subscriptions_max: cfg.file.activity_subscriptions.max_per_actor,
            known_followers_scan_max_pages: cfg.file.known_followers.scan_max_pages,
            known_followers_profile_scan_pages: cfg.file.known_followers.profile_scan_pages,
            known_likers_scan_max_pages: cfg.file.known_likers.scan_max_pages,
            thread_v2_max_parents: cfg.file.thread_v2.max_parents,
            op_thread_max_walk: cfg.file.op_thread.max_walk,
            farsight: farsight.clone(),
            constellation_bucket: Some(shares.bucket(
                "constellation",
                microcosm.constellation_rate_per_sec.0,
                microcosm.constellation_burst,
            )),
        },
    );
    // The pre-auth backstop (design-v3 §II-8.3): ready members' keys in
    // memory, and the limits on verifying against them.
    let backstop = Backstop {
        limits: VerifyLimits::from_config(&cfg.file.auth),
        keys: member_keys,
    };
    if let Err(error) = backstop.keys.reload(&pool).await {
        tracing::warn!(event = "member_keys_reload_failed", %error, "member keys not loaded at startup");
    }
    let verify_limits = backstop.limits.clone();
    tasks.push(
        backstop
            .keys
            .clone()
            // A member that left the ready set loses its in-process state:
            // its verification limits and its cached Farsight answers.
            .spawn(pool.clone(), resolver.clone(), move |did| {
                verify_limits.forget(did);
                if let Some(farsight) = &farsight {
                    farsight.forget(did);
                }
            }),
    );
    let limits = RateLimits::from_config(&cfg.file.rate_limits).with_backstop(backstop);
    tasks.push(limits.spawn_housekeeping());
    let app = AppState {
        hydrator,
        auth: Arc::new(glue.verifier.clone()),
        limits,
        enroller: Some(AutoEnroller::new(
            &cfg,
            pool.clone(),
            scope.clone(),
            Arc::clone(&private),
            resolver.clone(),
            net.clone(),
        )),
        trusted_proxy: cfg.file.http.trusted_proxy,
        did_document: Arc::new(glue.service_key.did_document()),
    };
    let private_state = PrivateState::new(
        app.clone(),
        glue.pipeline.clone(),
        prober,
        watermarks.clone(),
        Duration::from_secs(cfg.file.kryphocron.oracle_freshness_bound_secs),
        community_patterns,
        usize::try_from(cfg.file.sync.max_head_fetches_per_request).unwrap_or(16),
        &cfg.file.private_read,
    );
    // The post-migration task (design-v3 §12.2): every migration is applied
    // and ingest is running, so old rows can be filled and the large
    // indexes built while the app serves.
    let schema_backfill = SchemaBackfill::new(pool.clone(), schema_backfill::Settings::from_config(&cfg.file));
    let schema_status = schema_backfill.status();
    tasks.push(schema_backfill.spawn());
    // Starter-pack joined counts (design-v3 §5.5).
    tasks.push(
        crate::aggregations::pack_counts::PackCounts::new(
            pool.clone(),
            Some(constellation.clone()),
            Duration::from_secs(cfg.file.starter_packs.counts_refresh_secs),
        )
        .spawn(),
    );
    // The one-time re-sync that fetches the new collections of DIDs known
    // before the upgrade (design-v3 §12.3).
    tasks.push(crate::firehose::v3_resync::spawn(pool.clone()));
    let state = HealthState {
        pool: pool.clone(),
        redis,
        watermarks,
        status,
        audit: AuditLatch::from_flag(glue.audit_latched()),
        auth: app.auth.clone(),
        limits: app.limits.clone(),
        trusted_proxy: app.trusted_proxy,
        schema: schema_status,
    };
    let listener = TcpListener::bind(cfg.file.http.bind)
        .await
        .map_err(|e| StartupError::new(1, format!("binding {}: {e}", cfg.file.http.bind)))?;
    tracing::info!(event = "serve_started", bind = %cfg.file.http.bind, tasks = tasks.len(), "serving");
    let thread_lens = ThreadLensState {
        app: app.clone(),
        ladder: Ladder::new(app.hydrator.clone(), did_router.clone(), cascade, slingshot),
    };
    let index = crate::xrpc::handlers::index::IndexState {
        app: app.clone(),
        source,
        writer: writer.clone(),
        attempts: fh.deterministic_attempts,
    };
    let resolve_did = ResolveDidState {
        app: app.clone(),
        router: did_router,
    };
    // The routes of every `[services.*]` block in `proxy` state (§6.7.3).
    let proxied = crate::services::proxy::router(&app, &cfg.file, &shares, &cfg.env.public_hostname)
        .map_err(|e| StartupError::new(1, format!("service proxy client: {e}")))?;
    // §6.3's order: merge every route, then the fallbacks, the rate limit
    // and CORS outermost. Nothing is merged after `finalize`.
    let merged = crate::xrpc::router(app.clone())
        .merge(crate::honest_501::router(&app, &cfg.file))
        .merge(proxied)
        .merge(crate::private_tier::handlers::router(private_state))
        .merge(crate::constellation::resolve_did::router(resolve_did))
        .merge(crate::threadlens::router(thread_lens))
        .merge(crate::xrpc::router::index_router(index))
        .merge(health::router(state));
    let routes = crate::xrpc::router::finalize(
        merged,
        app,
        crate::xrpc::router::FinalConfig {
            not_ready_retry_secs: cfg.file.community.not_ready_retry_secs,
        },
    );
    axum::serve(listener, routes.into_make_service_with_connect_info::<SocketAddr>())
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
