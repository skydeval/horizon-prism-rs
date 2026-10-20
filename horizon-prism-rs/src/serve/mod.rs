//! `serve` (design-v2 §3.3): configuration, the migration gate, the shared
//! `PgPool` / Redis / `net` transports / `IdentityResolver` / `ScopeState`,
//! the background tasks, and the HTTP listener.
//!
//! Every `net` user is given its intent here (design-v4 §11 G-9): request
//! paths the public transport (or, for subject-named hosts, the transport
//! of the request's mode, through `Transports::for_request`), background
//! work and the private tier the internal one.
//!
//! HTTP: the `app.bsky.*` surface (§7.1), the `app.nearhorizon.private.*`
//! endpoints (§7.9), Thread Lens (§10), `resolveDid` (§8.7), `requestRecord` (§5.9),
//! `/.well-known/did.json` and `GET /xrpc/_health`.

pub mod health;
#[cfg(all(test, feature = "standin-hosts"))]
mod standin_tests;

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
use crate::subscriptions::{self, applier::Applier, fetch::Bounds, pds::PdsOwners, runner::{Lanes, ListSync}};
use crate::sync::{BudgetRates, HeadProber, HostBudgets};
use crate::firehose::consumer::{Consumer, ConsumerStatus, LAG_SAMPLE_INTERVAL, spawn_lag_sampler};
use crate::firehose::coverage::CoverageTracker;
use crate::firehose::watermark::Watermarks;
use crate::firehose::writer::Writer;
use crate::identity::fetch_cell::AuthBudget;
use crate::indexing::{PublicIndexing, PublicModeSettings};
use crate::identity::{IdentityRefresher, IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::microcosm::ConstellationClient;
use crate::net::{Intent, NetConfig, Transports};
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
use health::{AuditLatch, HealthState, PROBE_TIMEOUT};

/// Exit code for configuration errors and the migration gate (EX_CONFIG).
pub const EXIT_CONFIG: u8 = 78;
/// Timeout for the startup Redis connection.
pub const REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the in-process gauges `viewer_private_cache_entries` and
/// `host_budget_buckets` are sampled.
const PROCESS_GAUGES_EVERY: Duration = Duration::from_secs(15);

/// The in-process gauges' sampler, for as long as the process runs.
async fn sample_process_gauges(read_cache: ReadCache, budgets: HostBudgets) {
    loop {
        read_cache.sample_private();
        budgets.sample();
        tokio::time::sleep(PROCESS_GAUGES_EVERY).await;
    }
}
/// The `worker_state` key marking the private display refill as queued
/// (private posting §3.1).
pub const PRIVATE_DISPLAY_REFILL: &str = "private_display_refill";

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

    async fn subscription_tombstones_aged(&self) -> Result<(), sqlx::Error> {
        self.scope.reload_tombstone_counts(&self.pool).await
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
    // A Phase B test build says what it is before its config loads, so on
    // every start, a failed one included: on stderr, which no log filter
    // suppresses, and at error level in the log.
    #[cfg(feature = "standin-hosts")]
    {
        eprintln!("{}: this binary can reach private test origins; never deploy this build", crate::net::standin::MARKER);
        let level = std::env::var(crate::config::ENV_LOG_LEVEL).ok().filter(|v| !v.trim().is_empty());
        init_tracing(level.as_deref().unwrap_or(crate::config::DEFAULT_LOG_LEVEL));
        tracing::error!(event = "standin_hosts_build", marker = crate::net::standin::MARKER, "built with the standin-hosts feature: this binary can reach private test origins; never deploy this build");
    }
    let cfg = AppConfig::load_from_env().map_err(|e| StartupError::new(EXIT_CONFIG, format!("config_error: {e}")))?;
    cfg.validate()
        .map_err(|e| StartupError::new(EXIT_CONFIG, format!("config_error: {e}")))?;
    // A test build never serves as an ordinary deployment: it needs its
    // listed origins, checked before any port is bound.
    #[cfg(feature = "standin-hosts")]
    {
        crate::net::standin::required(&cfg.file.standin).map_err(|e| StartupError::new(EXIT_CONFIG, format!("config_error: {e}")))?;
        let origins: Vec<String> = cfg.file.standin.hosts.iter().map(|h| format!("{} {:?}", h.origin, h.intents)).collect();
        tracing::error!(event = "standin_hosts_origins", ?origins, "these private test origins are reachable on a request's first hop");
    }
    init_tracing(&cfg.env.log_level);
    PrometheusBuilder::new()
        .with_http_listener(cfg.file.metrics.bind)
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(crate::private_tier::handlers::METRIC_UNFLOORED.to_string()),
            &crate::private_tier::handlers::unfloored_buckets(),
        )
        .map_err(|e| StartupError::new(1, format!("metrics exporter: {e}")))?
        .install()
        .map_err(|e| StartupError::new(1, format!("metrics exporter: {e}")))?;
    #[cfg(feature = "standin-hosts")]
    metrics::gauge!("standin_hosts_build").set(1.0);
    // A bench build refuses to start without its heads file (private-threads
    // design §4), and says what it is.
    #[cfg(feature = "bench-heads")]
    let bench_heads = {
        let source = crate::sync::bench_heads::BenchHeadSource::from_config(&cfg.file)
            .map_err(|e| StartupError::new(EXIT_CONFIG, format!("config_error: {e}")))?;
        tracing::warn!(
            event = "bench_heads",
            "built with the bench-heads feature: repo heads come from bench.heads_file and the background head prober is off; never deploy this build"
        );
        source
    };
    // The effective head age, for the release check (private-threads design
    // §5), and the bucket note of §3.4.
    tracing::info!(
        event = "config_head_max_age",
        "config: sync.head_max_age_secs={}",
        cfg.file.sync.head_max_age_secs
    );
    if cfg.file.sync.max_head_fetches_per_request > cfg.file.sync.read_fetch_rate_per_host {
        tracing::warn!(
            event = "config_head_fetches_over_host_rate",
            max_head_fetches_per_request = cfg.file.sync.max_head_fetches_per_request,
            read_fetch_rate_per_host = cfg.file.sync.read_fetch_rate_per_host,
            "sync.max_head_fetches_per_request is above sync.read_fetch_rate_per_host: a private thread's head round can find a host's bucket empty and miss"
        );
    }

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
    // First-run setup (admin-ui design §5.2): while no operator exists, the
    // setup code is kept or made, and printed now and every 10 minutes.
    let admin_settings = crate::admin::AdminSettings::from_config(&cfg);
    let mut setup_reprint = None;
    if cfg.file.admin.web_ui {
        match crate::admin::setup_code::at_startup(&pool, &admin_settings.setup_code_file).await {
            Ok(Some(open)) => {
                crate::admin::setup_code::print(&open, &cfg.env.public_hostname);
                setup_reprint = Some(crate::admin::setup_code::spawn_reprint(
                    pool.clone(),
                    admin_settings.setup_code_file.clone(),
                    cfg.env.public_hostname.clone(),
                ));
            }
            Ok(None) => {}
            Err(e) => return Err(StartupError::new(1, format!("admin setup code: {e}"))),
        }
    }
    // The memory of learned blocks (§5.14.18.1) is off unless switched on.
    // Off, its table holds nothing: emptied here, best effort.
    if !cfg.file.farsight.memory.enabled {
        crate::farsight::truncate_memory(&pool).await;
        for problem in crate::farsight::memory_problems(&cfg.file.farsight)
            .into_iter()
            .filter(|p| !p.contains("needs farsight.url"))
        {
            tracing::warn!(event = "farsight_memory_config", %problem, "this value would be rejected if farsight.memory.enabled were true");
        }
    }
    let redis = redis_manager(&cfg.env.redis_url).await?;
    let consumer_redis = redis_manager(&cfg.env.redis_url).await?;
    // The three `net` transports (design-v4 §II-8.5), and what the startup
    // warning says of the internal resolver (§11 G-9 "Upstream DNS").
    let nets = Transports::new(&NetConfig::from_app(&cfg), &cfg.file.net).map_err(|e| StartupError::new(1, format!("net client: {e}")))?;
    for warning in crate::net::transport::warnings(&cfg.file.net) {
        tracing::warn!(event = "net_nameservers_config", %warning, "internal lookups may be visible to others who query the same resolver");
    }

    let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
    let scope = ScopeState::load(
        &pool,
        private.as_ref(),
        cfg.file.scope.policy,
        cfg.file.scope.max_follow_scope_per_member,
    )
    .await
    .map_err(|e| StartupError::new(1, format!("loading scope state: {e}")))?
    .with_windows(cfg.file.scope.windows());
    if !crate::aggregations::policy::grows_on_interaction(cfg.file.scope.policy) {
        tracing::info!(
            event = "scope_windows_unused",
            "scope.reply_quote_days and scope.like_repost_days are unused under this scope.policy"
        );
    }
    // The public-indexing switch (public-indexing design §3.5): its startup
    // sequence runs before the first writer (`purger.resume_all` below) and
    // hands back the one task the state needs, spawned here.
    let public = PublicIndexing::new(pool.clone(), scope.clone(), PublicModeSettings::from_config(&cfg.file.indexing));
    let public_task = public
        .startup(&cfg.file.indexing)
        .await
        .map_err(|e| StartupError::new(1, format!("public indexing startup: {e}")))?;
    let policy: Arc<dyn PersistencePolicy> = Arc::new(scope.clone());
    // Internal by default; each request-path user takes its own view.
    let resolver = IdentityResolver::new(nets.clone(), pool.clone(), IdentitySettings::from_app(&cfg), policy, Intent::Internal);
    // Rule 4 (§4.3): the applier runs list phases and releases.
    let applier = Applier::new(pool.clone(), scope.clone(), cfg.file.lists.sync_min_interval_secs);
    // The storage bound for the member count now (§4.7.6); the storage
    // sampler keeps the gauge current.
    let members: i64 = sqlx::query_scalar("SELECT count(*) FROM members")
        .fetch_one(&pool)
        .await
        .map_err(|e| StartupError::new(1, format!("counting members: {e}")))?;
    subscriptions::gauges::log_rows_bound(
        u64::try_from(members).unwrap_or(0),
        cfg.file.lists.subscribed_max_per_member,
        cfg.file.lists.subscribed_max_items,
    );
    let list_budget = subscriptions::references::Budget {
        max_references: cfg.file.lists.subscribed_max_per_member,
        max_owners: cfg.file.lists.max_owners_per_member,
    };
    // The live feed's hub (admin-ui-live-feed design §4.1), before the
    // writer, so every clone of the writer carries its tap; only with the
    // web UI on.
    let live_hub = cfg.file.admin.web_ui.then(|| crate::admin::live::Hub::new(cfg.file.admin.live_feed_snippets));
    let writer = Writer::new(pool.clone(), scope.clone(), Arc::clone(&private));
    let writer = match &live_hub {
        Some(hub) => writer.with_feed(hub.tap()),
        None => writer,
    }
    .with_list_references(list_budget, applier.clone())
        .with_subscribed_post_max_age_secs(cfg.file.notifications.subscribed_post_max_age_secs)
        .with_private_notify_max_age_hours(cfg.file.private_notify.backfill_max_age_hours)
        .with_subscription_limits(subscriptions::copy::Limits {
            max_items: cfg.file.lists.subscribed_max_items,
            tombstones_max_per_owner: cfg.file.lists.subscription_tombstones_max_per_owner,
        });
    let purger = Purger::new(pool.clone(), scope.clone(), Arc::clone(&private))
        .with_sync_min_interval_secs(cfg.file.lists.sync_min_interval_secs)
        .with_applier(applier.clone());
    // Constellation for background work: backfill, the count refresher and
    // the starter-pack counts.
    let constellation = ConstellationClient::new(nets.internal().client().clone(), &cfg.file.microcosm.constellation_url);

    let mut tasks = Vec::new();
    tasks.extend(setup_reprint);
    if let Some(task) = public_task {
        tasks.push(public.spawn(task));
    }

    // Every upstream a public request reaches has one request bucket,
    // split into pools and per-requester shares (design-v3 §II-8.3).
    let shares = Shares::from_config(&cfg.file);
    // The fallback's keys stop a start only when it is switched on
    // (§5.14.17.1); off, what would be rejected is only said.
    if !cfg.file.farsight.fallback.enabled {
        for problem in crate::farsight::fallback_problems(&cfg.file.farsight, &cfg.file.constellation.networks)
            .into_iter()
            .filter(|p| !p.contains("needs farsight.url"))
        {
            tracing::warn!(event = "farsight_fallback_config", %problem, "this value would be rejected if farsight.fallback.enabled were true");
        }
    }
    // The block-list index (design-v3 §5.14), only when configured.
    let farsight = match (&cfg.file.farsight.url, &cfg.env.farsight_token) {
        (Some(url), Some(token)) => {
            let client = crate::farsight::FarsightClient::new(&nets, url, token.expose())
                .ok_or_else(|| StartupError::new(78, "FARSIGHT_TOKEN is not a usable header value".to_string()))?;
            tracing::info!(event = "farsight_enabled", "inbound list blocks are read from Farsight");
            let farsight = crate::farsight::Farsight::new(client, &cfg.file.farsight, &shares);
            // The fallback (§5.14.17), only when the operator switched it
            // on. Validation has checked the entry it names.
            let fallback = &cfg.file.farsight.fallback;
            if fallback.enabled
                && let Some(network) = cfg.file.constellation.networks.iter().find(|n| n.name == fallback.network)
            {
                let upstream = crate::appview_clients::AppViewClient::new(
                    &network.name,
                    &network.appview_url,
                    crate::appview_clients::AppViewAuth::Unauth,
                    nets.public().client().clone(),
                );
                farsight.enable_fallback(upstream, fallback, &shares);
                tracing::warn!(
                    event = "farsight_fallback_enabled",
                    network = %network.name,
                    appview = %network.appview_url,
                    "the fallback is on: while Farsight is down, each fallback call tells this AppView that this deployment is rendering a page for a member that contains the accounts named, and it can join that with other calls from this address",
                );
            }
            Some(farsight)
        }
        _ => None,
    };
    // The memory's load (§5.14.18.1), before the first task: the purge
    // hook is made from what it returns, so no purge can clear a viewer
    // while rows are read and not yet installed.
    if let Some(farsight) = &farsight {
        let loaded = farsight.load_memory(&pool, &cfg.file.farsight.memory).await;
        purger.set_hook(loaded.hook());
        if loaded.entries.is_some() {
            tasks.push(tokio::spawn(farsight.clone().run_memory_writer()));
        }
        // The cache's sweep runs whenever Farsight is configured, with
        // the memory on or off: an entry that is never read again (a
        // Thread Lens visitor that does not come back) is removed by
        // nothing else.
        tasks.push(tokio::spawn(farsight.clone().run_cache_sweeper()));
        // `farsight_down` and `farsight_fallback_withheld{reason}`: set
        // here first, so every series exists before the first call.
        farsight.sample_gate();
        tasks.push(tokio::spawn(farsight.clone().run_gate_sampler()));
    }
    let microcosm = &cfg.file.microcosm;
    let identity = &cfg.file.identity;
    // Lodestar (lodestar-reads design), only when configured: the client,
    // its gate and the probe that keeps the gate.
    let lodestar = identity.lodestar().map(|url| {
        let lodestar = Arc::new(crate::identity::lodestar::Lodestar::new(
            nets.public().client().clone(),
            url,
            Duration::from_millis(identity.lodestar_timeout_ms),
            Duration::from_secs(identity.did_fresh_secs),
        ));
        tracing::info!(
            event = "lodestar_enabled",
            host = %lodestar.host(),
            "public request paths read identities from lodestar first; plc.directory stays the fallback and the key path's source"
        );
        tasks.push(Arc::clone(&lodestar).spawn_probe());
        lodestar
    });
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
    let (glue, issuer_drainer) = GlueContext::new(&cfg, resolver.with_intent(Intent::Public), auth_budget)
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
    let consumer = Consumer::new(
        fh.clone(),
        consumer_redis,
        writer.clone(),
        watermarks.clone(),
        coverage,
        Arc::clone(&status),
    )
    .with_sync_min_interval_secs(cfg.file.lists.sync_min_interval_secs);
    let throughput = consumer.throughput_shared();
    tasks.push(consumer.spawn());
    tasks.push(spawn_lag_sampler(redis.clone(), fh.clone(), throughput, LAG_SAMPLE_INTERVAL));

    // Retention and the storage sampler.
    // Private retention runs first (§3.2), so the public `actors` step sees
    // the private row authors that remain.
    let retention: Vec<Arc<dyn RetentionTask>> = vec![
        Arc::new(PrivateRetention::new(scope.clone())),
        Arc::new(PublicRetention::new(Arc::new(ScopeRetentionInputs {
            scope: scope.clone(),
            pool: pool.clone(),
        }))),
        // The memory's table (§5.14.18.1), with or without an engine.
        Arc::new(crate::farsight::retention::MemoryRetention::new(&cfg.file.farsight.memory, farsight.clone())),
        // Rule 4's daily safety net (design-v4 §14.1 R4).
        Arc::new(crate::subscriptions::sweep::OrphanSweep::new(applier.clone(), cfg.file.lists.sync_min_interval_secs)),
    ];
    let guard: Arc<dyn ScopeGuard> = Arc::new(scope.clone());
    tasks.push(spawn_scheduler(
        pool.clone(),
        guard,
        retention,
        RetentionSettings {
            trend_aggregate_hours: cfg.file.trends.aggregate_retention_hours,
            ..RetentionSettings::from(&cfg.file.retention)
        },
        cfg.file.retention.run_at_utc,
    ));
    tasks.push(
        StorageSampler {
            pool: pool.clone(),
            redis: redis.clone(),
            stream_key: fh.stream_key.clone(),
            budget_bytes: cfg.file.storage.budget_bytes,
            subscribed_max_per_member: cfg.file.lists.subscribed_max_per_member,
            subscribed_max_items: cfg.file.lists.subscribed_max_items,
        }
        .spawn(),
    );

    // Per-host PDS budgets (§6.11.3): probe, read-fetch and backfill buckets.
    let budgets = HostBudgets::new(BudgetRates::from_app(&cfg));

    // The private display refill (private posting §3.1, S2): once, at the
    // first start after migration 0023, a `private` job for each DID whose
    // rows predate it. A failure records nothing, so the next start retries.
    match private.enqueue_display_refill(&pool, PRIVATE_DISPLAY_REFILL).await {
        Ok(Some(queued)) => tracing::info!(event = "private_display_refill_queued", queued, "private jobs queued to fill display fields"),
        Ok(None) => {}
        Err(error) => tracing::warn!(event = "private_display_refill_failed", %error, "queuing the private display refill failed; retried at the next start"),
    }
    // The `following` rollout (following-audience design §3.6): at each
    // start, an `authz` job for every owner of a `following` audience whose
    // follows are not verified. Until it settles, those audiences are read
    // by their owners alone.
    match private.enqueue_following_verification(&pool).await {
        Ok(queued) => tracing::info!(event = "following_verification_queued", queued, "authz jobs queued to verify following owners' follows"),
        Err(error) => tracing::warn!(event = "following_verification_failed", %error, "queuing following owners' verification failed; retried at the next start"),
    }

    // Backfill, count refresher, weekly rebuild.
    let source: Arc<dyn BackfillSource> = Arc::new(NetSource::new(
        pool.clone(),
        resolver.clone(),
        constellation.clone(),
        budgets.clone(),
    ));
    // Rule 4 (§4.3): the applier's loop, and the backfill workers serving
    // the `list_sync` lanes.
    let lists = &cfg.file.lists;
    tasks.push(tokio::spawn(applier.clone().run()));
    // The budget may have changed since the references were admitted.
    tasks.push(tokio::spawn({
        let (pool, applier) = (pool.clone(), applier.clone());
        async move {
            match subscriptions::references::recompute_all(&pool, list_budget).await {
                Ok(true) => applier.signal(),
                Ok(false) => {}
                Err(error) => tracing::warn!(event = "list_references_recompute_failed", %error, "recomputing list references at startup failed"),
            }
        }
    }));
    let list_sync = ListSync::new(
        pool.clone(),
        scope.clone(),
        Arc::new(PdsOwners::new(pool.clone(), resolver.clone(), budgets.clone())),
        &applier,
        subscriptions::run::Settings {
            max_items: lists.subscribed_max_items,
            tombstones_max_per_owner: lists.subscription_tombstones_max_per_owner,
            min_interval_secs: lists.sync_min_interval_secs,
            bounds: Bounds {
                max_records: lists.subscribed_max_owner_listitems,
                max_pages: lists.sync_max_pages,
                probe_call: Duration::from_secs(u64::from(lists.sync_probe_call_secs)),
                min_records_per_call_sec: lists.sync_min_records_per_call_sec,
                max_call: Duration::from_secs(u64::from(lists.sync_max_call_secs)),
                max_wall: Duration::from_secs(u64::from(lists.sync_max_wall_secs)),
            },
        },
        Lanes {
            fast: lists.sync_max_concurrent,
            slow: lists.sync_slow_max_concurrent,
            stuck_secs: lists.sync_stuck_secs,
            max_wall_secs: lists.sync_max_wall_secs,
        },
    );
    let backfill = Backfill::new(writer.clone(), Arc::clone(&source), cfg.file.backfill.clone(), fh.deterministic_attempts)
        .with_farsight(farsight.clone())
        .with_list_sync(Some(list_sync));
    tasks.extend(
        backfill
            .spawn()
            .await
            .map_err(|e| StartupError::new(1, format!("starting backfill: {e}")))?,
    );
    tasks.push(CountRefresher::new(pool.clone(), redis.clone(), constellation.clone(), &cfg.file.counts).spawn());
    let rebuild = Rebuild::new(pool.clone(), scope.clone(), Arc::clone(&private), cfg.file.scope.rebuild_grace_days)
        .with_sync_min_interval_secs(lists.sync_min_interval_secs)
        .with_max_purges(cfg.file.scope.sweep_max_purges)
        .with_retention_at(cfg.file.retention.run_at_utc);
    if cfg.file.scope.rebuild_grace_days > 1 {
        tracing::warn!(
            event = "scope_grace_slows_unfollow",
            grace_days = cfg.file.scope.rebuild_grace_days,
            "scope.rebuild_grace_days above 1: an unfollow takes up to grace + 1 days to apply (scope-recency design §5)"
        );
    }
    tasks.push(rebuild.clone().spawn_daily());
    tasks.push(rebuild.clone().spawn_weekly());

    // Repo-head prober (§6.11.3); the private pipeline shares it. Under the
    // B-5 bench's feature its source is the bench file, read at startup,
    // and no background pass runs (private-threads design §4).
    #[cfg(feature = "bench-heads")]
    let head_source: Arc<dyn crate::sync::HeadSource> = Arc::new(bench_heads);
    #[cfg(not(feature = "bench-heads"))]
    let head_source: Arc<dyn crate::sync::HeadSource> = Arc::new(crate::sync::NetHeadSource::new(&nets));
    let prober = HeadProber::new(
        pool.clone(),
        scope.clone(),
        Arc::clone(&private),
        head_source,
        budgets.clone(),
        cfg.file.sync.clone(),
    );
    if crate::sync::SPAWNS_PROBER {
        tasks.push(prober.clone().spawn());
    }

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
            farsight: farsight.clone(),
            sync_min_interval_secs: lists.sync_min_interval_secs,
            list_budget,
            public: public.clone(),
            operators: crate::admin_executor::OperatorDeps {
                identity: Some(Arc::new(resolver.with_intent(Intent::Internal))),
                setup_code_file: Some(admin_settings.setup_code_file.clone()),
            },
        })
        .spawn(),
    );

    // Constellation routing (§8): the table, the `route` cache over the
    // identity resolver, one client per outbound network, the cascade, and
    // the upstream source behind hydration's seam.
    let table = Arc::new(RoutingTable::from_config(&cfg.file.constellation));
    // Request-path identity work: one token of the requester's identity
    // share per DID or handle, and PLC's request bucket per PLC fetch.
    // `resolveHandle` asks DNS for the handle's TXT record itself, to tell
    // "no such handle" from a lookup that failed (design-v3 §5.11). Every
    // lookup goes through the transport of the request's mode.
    let mut lookup = RequestLookup::new(resolver.with_intent(Intent::Public), identity_share, plc_bucket).with_handle_lookup(
        crate::constellation::routing::HandleCacheSettings {
            capacity: cfg.file.identity.resolve_handle_cache_max,
            ttl: Duration::from_secs(cfg.file.identity.resolve_handle_ttl_secs),
            negative_ttl: Duration::from_secs(cfg.file.identity.resolve_handle_negative_ttl_secs),
        },
    );
    // Lodestar first on public request paths only, with its own bucket
    // (lodestar-reads design §2.2, §2.11). No other user of `resolver`
    // gets it.
    if let Some(lodestar) = &lodestar {
        let bucket = shares.bucket(
            "lodestar",
            cfg.file.identity.lodestar_request_rate_per_sec.0,
            cfg.file.identity.lodestar_request_burst,
        );
        lookup = lookup.with_lodestar(Arc::clone(lodestar), bucket);
    }
    let did_router = DidRouter::new(Arc::clone(&table), Arc::new(lookup) as Arc<dyn DidLookup>);
    let cascade = Cascade::new(
        Arc::clone(&table),
        crate::appview_clients::from_config(&cfg.file.constellation, nets.public().client(), &glue.service_key),
        &shares,
    );
    let read_cache = ReadCache::new(cfg.file.cache.read_time_bytes)
        .with_ttls(crate::hydration::cache::RtTtls {
            profile: Duration::from_secs(cfg.file.cache.profile_ttl_secs),
            negative: Duration::from_secs(cfg.file.cache.profile_negative_ttl_secs),
            farsight_pair: Duration::from_secs(cfg.file.farsight.cache_ttl_secs),
            other: crate::hydration::cache::TTL,
        })
        .with_private(crate::hydration::cache::PrivateCacheSettings {
            ttl: Duration::from_secs(cfg.file.cache.viewer_private_ttl_secs),
            max_entries: cfg.file.cache.viewer_private_max,
            per_viewer_max: usize::try_from(cfg.file.cache.viewer_private_per_viewer_max).unwrap_or(usize::MAX),
        });
    let profile_queue = crate::hydration::ProfileQueue::new(cfg.file.cache.profile_refresh_max);
    // `viewer_private_cache_entries` and `host_budget_buckets`: sampled
    // once now, so both exist from the start, then every
    // `PROCESS_GAUGES_EVERY`.
    tasks.push(tokio::spawn(sample_process_gauges(read_cache.clone(), budgets.clone())));
    let slingshot = SlingshotClient::new(nets.public().client().clone(), &microcosm.slingshot_url).metered(shares.bucket(
        "slingshot",
        microcosm.slingshot_rate_per_sec.0,
        microcosm.slingshot_burst,
    ));
    let upstream = RealUpstreamSource::new(
        did_router.clone(),
        cascade.clone(),
        slingshot.clone(),
        PdsLister::new(
            nets.clone(),
            HostBuckets::new(
                &shares,
                cfg.file.sync.public_fallback_rate_per_host.0,
                cfg.file.sync.max_public_fallback_hosts,
            ),
        ),
        read_cache.clone(),
    )
    .with_list_read_pages(cfg.file.lists.read_listing_pages)
    .with_scope(scope.clone());

    // HTTP.
    let hydrator = Hydrator::new(
        pool.clone(),
        scope.clone(),
        Some(ConstellationClient::new(nets.public().client().clone(), &cfg.file.microcosm.constellation_url)),
        Some(redis.clone()),
        Arc::new(upstream),
        read_cache.clone(),
        HydrationConfig {
            labeler_did: glue.service_key.did().to_string(),
            counts: cfg.file.counts.clone(),
            thread: cfg.file.thread.clone(),
            thread_mutes_max: cfg.file.thread_mutes.max_per_actor,
            activity_subscriptions_max: cfg.file.activity_subscriptions.max_per_actor,
            bookmarks_max: cfg.file.bookmarks.max_per_actor,
            bookmarks_emit_count: cfg.file.bookmarks.emit_count,
            bookmarks_count_floor: cfg.file.bookmarks.count_floor,
            video: cfg.file.video.clone(),
            drafts_max: cfg.file.drafts.max_per_actor,
            drafts_max_bytes: cfg.file.drafts.max_bytes,
            labels_query_max_patterns: cfg.file.labels.query_max_patterns,
            labelers_max_dids: cfg.file.labelers.get_services_max_dids,
            known_followers_scan_max_pages: cfg.file.known_followers.scan_max_pages,
            known_followers_profile_scan_pages: cfg.file.known_followers.profile_scan_pages,
            known_likers_scan_max_pages: cfg.file.known_likers.scan_max_pages,
            thread_v2_max_parents: cfg.file.thread_v2.max_parents,
            op_thread_max_walk: cfg.file.op_thread.max_walk,
            search: cfg.file.search.clone(),
            suggestions: cfg.file.suggestions.clone(),
            unspecced: cfg.file.unspecced.clone(),
            trends: cfg.file.trends.clone(),
            farsight: farsight.clone(),
            list_budget,
            list_applier: Some(applier.clone()),
            constellation_bucket: Some(shares.bucket(
                "constellation",
                microcosm.constellation_rate_per_sec.0,
                microcosm.constellation_burst,
            )),
            profile_queue: profile_queue.clone(),
        },
    );
    // The background profile refresh (design-v4 §8.1): one task, fed only
    // by the tiers of public requests.
    tasks.push(crate::hydration::ProfileDrain::new(profile_queue, read_cache.clone(), did_router.clone(), cascade.clone()).spawn());
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
            // its verification limits, its cached Farsight answers and its
            // viewer-private cache entries (design-v4 §8.1).
            .spawn(pool.clone(), resolver.clone(), move |did| {
                verify_limits.forget(did);
                read_cache.forget_viewer(did);
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
            resolver.with_intent(Intent::Public),
        )),
        trusted_proxy: cfg.file.http.trusted_proxy,
        did_document: Arc::new(glue.service_key.did_document()),
        membership: crate::xrpc::handlers::membership::MembershipSettings::new(private.as_ref(), &cfg.file.private_read),
    };
    let private_state = PrivateState::new(
        app.clone(),
        glue.pipeline.clone(),
        prober,
        watermarks.clone(),
        Duration::from_secs(cfg.file.kryphocron.oracle_freshness_bound_secs),
        usize::try_from(cfg.file.sync.max_head_fetches_per_request).unwrap_or(16),
        &cfg.file.private_read,
        // Required and never generated (private posting §3.6, C-14).
        crate::cursor_key::CursorKey::new(cfg.env.private_cursor_key.expose()),
    )
    .with_search(&cfg.file.private_search);
    log_floors(&cfg.file.private_read.floor_ms);
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
        public,
        probe_timeout: PROBE_TIMEOUT,
    };
    let listener = TcpListener::bind(cfg.file.http.bind)
        .await
        .map_err(|e| StartupError::new(1, format!("binding {}: {e}", cfg.file.http.bind)))?;
    tracing::info!(event = "serve_started", bind = %cfg.file.http.bind, tasks = tasks.len(), "serving");
    let thread_lens = ThreadLensState {
        app: app.clone(),
        ladder: Ladder::new(app.hydrator.clone(), did_router.clone(), cascade, slingshot),
        cfg: cfg.file.threadlens.clone(),
        // Without a configured key, cursors do not outlive the process.
        cursor_key: cfg
            .env
            .threadlens_cursor_key
            .as_ref()
            .map_or_else(crate::cursor_key::CursorKey::generated, |key| crate::cursor_key::CursorKey::new(key.expose())),
    };
    let index = crate::xrpc::handlers::index::IndexState {
        app: app.clone(),
        source,
        writer: writer.clone(),
        attempts: fh.deterministic_attempts,
        catch_up: crate::firehose::index_request::CatchUp::new(crate::firehose::index_request::CatchUpConfig::from_app(&cfg)),
    };
    let resolve_did = ResolveDidState {
        app: app.clone(),
        router: did_router,
    };
    // The routes of every `[services.*]` block in `proxy` state (§6.7.3).
    let proxied = crate::services::proxy::router(&app, &cfg.file, &shares, &cfg.env.public_hostname)
        .map_err(|e| StartupError::new(1, format!("service proxy client: {e}")))?;
    // The web admin UI (admin-ui design §3), unless `admin.web_ui` is off:
    // its OAuth fetches through the public transport, its identity work
    // through the internal one (§5.10).
    let admin_routes = if let Some(hub) = &live_hub {
        let admin = crate::admin::AdminState::new(
            pool.clone(),
            scope.clone(),
            Arc::new(resolver.with_intent(Intent::Internal)),
            Arc::new(crate::admin::oauth::NetFetch(nets.public().client().clone())),
            admin_settings,
            hub.clone(),
        );
        tasks.push(admin.spawn_housekeeping());
        tasks.push(hub.spawn_sampler());
        crate::admin::router(admin)
    } else {
        axum::Router::new()
    };
    // §6.3's order: merge every route, then the fallbacks, the rate limit
    // and CORS outermost. Nothing is merged after `finalize`.
    let merged = crate::xrpc::router(app.clone())
        .merge(admin_routes)
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
            public: crate::honest_501::PublicMessages::from_config(&cfg.file),
        },
    );
    // Graceful shutdown waits for open connections, so the live streams
    // are ended first (admin-ui-live-feed design §3.5). A restart from the
    // UI must resolve this same future.
    axum::serve(listener, routes.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            if let Some(hub) = live_hub {
                hub.close();
            }
        })
        .await
        .map_err(|e| StartupError::new(1, format!("http server: {e}")))?;
    for t in tasks {
        t.abort();
    }
    tracing::info!(event = "serve_stopped", "shutdown complete");
    Ok(())
}

/// The private-read floors, once at startup (timing-floors design §2): `info`
/// with each value, and `warn` naming the zero keys when only some are set,
/// since a post can be timed through any endpoint that reads it.
fn log_floors(floors: &crate::config::PrivateReadFloors) {
    use crate::config::FloorsSetting;
    let values = floors
        .entries()
        .iter()
        .map(|(key, ms)| format!("{key}={ms}"))
        .collect::<Vec<_>>()
        .join(" ");
    match floors.setting() {
        FloorsSetting::Off => tracing::info!(event = "private_read_floors", "private read floors: off"),
        FloorsSetting::On => tracing::info!(event = "private_read_floors", floors = %values, "private read floors: on"),
        FloorsSetting::Partial { zero } => {
            tracing::info!(event = "private_read_floors", floors = %values, "private read floors: partial");
            tracing::warn!(
                event = "private_read_floors_partial",
                zero = %zero.join(" "),
                "private read floors are set on only some endpoints; response time still shows what they hide through the others"
            );
        }
    }
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
