//! The crawl task: while breadth is Full it follows the crawl generation,
//! seeds the members and S, pages the relays' listings into the queue,
//! runs `concurrency` claimers at `repos_per_sec`, reads the takedown
//! relays' statuses, turns groups Complete (then repairs the counts once,
//! before they read locally), pauses at the free-space floor, and runs the
//! sweeper and the re-list. Live ingest is never stopped.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use redis::aio::ConnectionManager;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::client::{CrawlSource, ListedRepo, Pace};
use super::{DidQueue, FirstSight, LIST_KEY, OnDemand, state};
use crate::aggregations::ScopeState;
use crate::config::{AtBudget, FullConfig, FullCrawlConfig};
use crate::firehose::consumer::LowWater;
use crate::firehose::tier::{CopyTier, TierBoard};
use crate::firehose::writer::Writer;
use crate::indexing::Breadth;

/// A relay the crawl can list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relay {
    pub label: String,
    /// The HTTPS base of its XRPC methods.
    pub base: String,
}

/// The crawl's progress, for status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    pub running: bool,
    pub paused: bool,
    pub history: bool,
    pub crawled: u64,
    pub failed: u64,
    pub last_error: Option<String>,
}

/// Everything the crawl needs. Cheap to clone.
#[derive(Clone)]
pub struct Crawler {
    pub(super) pool: PgPool,
    pub(super) writer: Writer,
    pub(super) scope: ScopeState,
    pub(super) source: Arc<dyn CrawlSource>,
    pub(super) crawl: FullCrawlConfig,
    pub(super) full: FullConfig,
    pub(super) relays: Vec<Relay>,
    pub(super) tiers: Arc<TierBoard>,
    pub(super) old_filtered: Vec<String>,
    pub(super) low_water: Option<Arc<LowWater>>,
    pub(super) covered: Arc<AtomicI64>,
    pub(super) redis: Option<ConnectionManager>,
    pub(super) stream_key: String,
    pub(super) window_ms: i64,
    pub(super) data_dir: PathBuf,
    pub(super) first_sight: Arc<FirstSight>,
    pub(super) on_demand: Arc<OnDemand>,
    pub(super) repo_pace: Arc<Pace>,
    pub(super) relist_pace: Arc<Pace>,
    pub(super) status_pace: Arc<Pace>,
    pub(super) list_pace: Arc<Pace>,
    /// The crawl generation the claimers run under, plus one; 0 stopped.
    running: Arc<AtomicU64>,
    /// Whether ticks start claimers (off where a caller drives claims).
    claimers: bool,
    /// The stream tail a re-list page waits for when there is no stream to
    /// read it from.
    pub(super) fixed_tail: Option<i64>,
    progress: Arc<std::sync::Mutex<Progress>>,
    /// A counts repair after a group turned complete is running.
    repairing: Arc<AtomicBool>,
    /// The directory export's follower (`identity_source = "plc_export"`).
    pub(super) export: Arc<super::plc_export::ExportState>,
    pub(super) export_pace: Arc<Pace>,
    /// Takedown relays whose entries came back to their primary URL: their
    /// account statuses are read again.
    back_on_primary: Arc<DidQueue>,
    /// The figures status shows, and when they were last read.
    figures: Arc<std::sync::Mutex<Value>>,
    figures_at: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl Crawler {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        writer: Writer,
        source: Arc<dyn CrawlSource>,
        crawl: FullCrawlConfig,
        full: FullConfig,
        relays: Vec<Relay>,
        tiers: Arc<TierBoard>,
        old_filtered: Vec<String>,
        window_secs: u64,
    ) -> Self {
        let export = Arc::new(super::plc_export::ExportState::new(crawl.identity_source == crate::config::IdentitySource::PlcExport));
        let export_pace = Arc::new(Pace::per_sec(crawl.plc_rate_per_sec));
        Self {
            pool: writer.pool().clone(),
            scope: writer.scope().clone(),
            writer,
            source,
            repo_pace: Arc::new(Pace::per_sec(crawl.repos_per_sec)),
            relist_pace: Arc::new(Pace::per_sec(full.relist_pages_per_sec)),
            status_pace: Arc::new(Pace::per_sec(full.status_pass_pages_per_sec)),
            list_pace: Arc::new(Pace::per_sec(full.relist_pages_per_sec)),
            crawl,
            full,
            relays,
            tiers,
            old_filtered,
            low_water: None,
            covered: Arc::default(),
            redis: None,
            stream_key: String::new(),
            window_ms: i64::try_from(window_secs.saturating_mul(1000)).unwrap_or(i64::MAX),
            data_dir: PathBuf::from("."),
            first_sight: Arc::new(DidQueue::new(super::QUEUE_CAP)),
            on_demand: Arc::new(DidQueue::new(super::QUEUE_CAP)),
            running: Arc::default(),
            claimers: true,
            fixed_tail: None,
            progress: Arc::default(),
            repairing: Arc::default(),
            export,
            export_pace,
            figures: Arc::new(std::sync::Mutex::new(Value::Null)),
            figures_at: Arc::default(),
            back_on_primary: Arc::new(DidQueue::new(64)),
        }
    }

    /// With the queue the consumer names a relay on when its entries come
    /// from its primary URL again.
    #[must_use]
    pub fn with_back_on_primary(mut self, queue: Arc<DidQueue>) -> Self {
        self.back_on_primary = queue;
        self
    }

    /// A takedown relay back on its primary URL: account events inside the
    /// fallback may be lost, so its listing's statuses are read again, a
    /// status-only pass at the status pace, resumed after a restart.
    pub(super) async fn status_again_step(&self, generation: i64) -> Result<(), sqlx::Error> {
        let mut st = self.list_state(generation).await?;
        if !st["again"].is_object() {
            st["again"] = json!({});
        }
        for label in self.back_on_primary.take(64) {
            if self.status_relay(&label) {
                tracing::info!(event = "full_status_pass_again", relay = %label, "a takedown relay is back on its primary URL; its account statuses are read again");
                st["again"][&label] = Value::Null;
            }
        }
        let pending: Vec<String> = st["again"].as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
        if pending.is_empty() {
            return Ok(());
        }
        for label in pending {
            let Some(relay) = self.relays.iter().find(|r| r.label == label).cloned() else {
                if let Some(m) = st["again"].as_object_mut() {
                    m.remove(&label);
                }
                continue;
            };
            let cursor = st["again"][&label].as_str().map(str::to_string);
            self.status_pace.wait().await;
            match self.source.list_repos(&relay.base, cursor.as_deref()).await {
                Ok(page) => {
                    record_statuses(&self.pool, &relay.label, &page.repos).await?;
                    match page.cursor {
                        Some(c) if !page.repos.is_empty() => st["again"][&label] = Value::String(c),
                        _ => {
                            if let Some(m) = st["again"].as_object_mut() {
                                m.remove(&label);
                            }
                            tracing::info!(event = "full_status_pass_again_done", relay = %label, "a takedown relay's account statuses were read again");
                        }
                    }
                }
                Err(error) => tracing::warn!(event = "full_status_pass_waiting", relay = %label, %error, "the status pass is waiting on a relay"),
            }
        }
        self.save_list_state(&st).await
    }

    /// What status and Health show of the crawl.
    #[must_use]
    pub fn board(&self) -> super::status::CrawlBoard {
        super::status::CrawlBoard { progress: Arc::clone(&self.progress), export: Arc::clone(&self.export), figures: Arc::clone(&self.figures) }
    }

    /// Read the figures again when they are older than
    /// [`super::status::FIGURES_EVERY`], and show each group's history.
    async fn refresh_figures(&self, generation: i64, groups: &super::groups::Groups) -> Result<(), sqlx::Error> {
        for (name, g) in &groups.groups {
            for state in [super::groups::CRAWLING, super::groups::PAUSED, super::groups::COMPLETE, super::groups::REMOVED] {
                metrics::gauge!(super::METRIC_GROUP_HISTORY, "group" => name.clone(), "state" => state).set(if g.state == state { 1.0 } else { 0.0 });
            }
        }
        let due = self.figures_at.lock().map(|at| at.is_none_or(|t| t.elapsed() >= super::status::FIGURES_EVERY)).unwrap_or(true);
        if !due {
            return Ok(());
        }
        let f = super::status::figures(&self.pool, generation).await?;
        if let Ok(mut slot) = self.figures.lock() {
            *slot = f;
        }
        if let Ok(mut at) = self.figures_at.lock() {
            *at = Some(Instant::now());
        }
        Ok(())
    }

    /// With a fixed stream tail for re-list pages, where no stream is read.
    #[must_use]
    pub fn with_fixed_tail(mut self, tail_ms: i64) -> Self {
        self.fixed_tail = Some(tail_ms);
        self
    }

    /// Without claimers: the caller claims and processes accounts itself.
    #[must_use]
    pub fn without_claimers(mut self) -> Self {
        self.claimers = false;
        self
    }

    /// With the consumer's clocks: its low-water entry and its last Whole or
    /// Covered copy.
    #[must_use]
    pub fn with_clocks(mut self, low_water: Arc<LowWater>, covered: Arc<AtomicI64>) -> Self {
        self.low_water = Some(low_water);
        self.covered = covered;
        self
    }

    /// With the stream, whose tail a re-list page waits for.
    #[must_use]
    pub fn with_stream(mut self, redis: ConnectionManager, stream_key: &str) -> Self {
        self.redis = Some(redis);
        self.stream_key = stream_key.to_string();
        self
    }

    /// With the data directory, whose disk stands in for the database
    /// volume when that is not visible.
    #[must_use]
    pub fn with_data_dir(mut self, dir: &std::path::Path) -> Self {
        self.data_dir = dir.to_path_buf();
        self
    }

    /// With the queues the writer and the read path feed.
    #[must_use]
    pub fn with_queues(mut self, first_sight: Arc<FirstSight>, on_demand: Arc<OnDemand>) -> Self {
        self.first_sight = first_sight;
        self.on_demand = on_demand;
        self
    }

    /// The crawl's progress.
    #[must_use]
    pub fn progress(&self) -> Progress {
        self.progress.lock().map(|p| p.clone()).unwrap_or_default()
    }

    fn note(&self, f: impl FnOnce(&mut Progress)) {
        if let Ok(mut p) = self.progress.lock() {
            f(&mut p);
        }
    }

    /// The relays whose listing the crawl and the re-list read: those whose
    /// copies this process has seen, Whole or Covered. A configured relay
    /// that sends nothing (no reader runs for it) is not listed.
    pub(super) fn listing_relays(&self) -> Vec<Relay> {
        let board = self.tiers.relays();
        self.relays
            .iter()
            .filter(|r| board.get(&r.label).is_some_and(|t| t.tier != CopyTier::Aside) && !self.old_filtered.contains(&r.label))
            .cloned()
            .collect()
    }

    /// Whether `label` is a takedown relay, whose listing's statuses are
    /// recorded whatever its tier.
    pub(super) fn status_relay(&self, label: &str) -> bool {
        self.full.takedown_relays.iter().any(|t| t == label)
    }

    /// Spawn the task.
    #[must_use]
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self) {
        // The export follower runs beside the task, for the process's life.
        let _export = self.spawn_export();
        let mut entered: Option<i64> = None;
        let mut s_seeded = false;
        let mut last_complete = Instant::now() - Duration::from_secs(3600);
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match self.tick(&mut entered, &mut s_seeded, &mut last_complete).await {
                Ok(()) => {}
                Err(error) => {
                    tracing::warn!(event = "full_crawl_error", %error, "the crawl task's step failed; it goes on");
                    self.note(|p| p.last_error = Some(error.to_string()));
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// One tick of the task.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn tick(&self, entered: &mut Option<i64>, s_seeded: &mut bool, last_complete: &mut Instant) -> Result<(), sqlx::Error> {
        let (breadth, generation, on, history, members, scope_dids) = {
            let sets = self.scope.read_guard().await;
            let trial = sets.trial.as_ref().filter(|t| t.state == crate::indexing::trial::TrialState::Running);
            let history = self.crawl.enabled && trial.is_none_or(|t| t.history);
            let members: Vec<String> = sets.members.iter().cloned().collect();
            let scope_dids: Vec<String> = if *s_seeded { Vec::new() } else { sets.scope.iter().cloned().collect() };
            (sets.public.breadth, i64::try_from(sets.public.crawl_generation).unwrap_or(i64::MAX), sets.public.mode.is_on(), history.then_some(trial.is_some()), members, scope_dids)
        };
        if breadth != Breadth::Full {
            if entered.take().is_some() {
                self.running.store(0, Ordering::SeqCst);
                self.note(|p| p.running = false);
                // No group has a history outside full.
                for g in crate::config::FullGroup::ALL {
                    for state in [super::groups::CRAWLING, super::groups::PAUSED, super::groups::COMPLETE, super::groups::REMOVED] {
                        metrics::gauge!(super::METRIC_GROUP_HISTORY, "group" => g.as_str(), "state" => state).set(0.0);
                    }
                }
                tracing::info!(event = "full_crawl_stopped", "the crawl stopped: full indexing is off");
            }
            *s_seeded = false;
            return Ok(());
        }
        let Some(groups) = state::read_groups(&self.pool).await?.filter(|g| g.crawl_generation == generation) else {
            return Ok(());
        };
        let enabled = groups.enabled();
        self.refresh_figures(generation, &groups).await?;
        if *entered != Some(generation) {
            self.running.store(0, Ordering::SeqCst);
            let reverted = state::revert_fetching(&self.pool).await?;
            super::groups_change::reconcile_kinds(&self.pool, generation, &self.full.kinds).await?;
            *entered = Some(generation);
            *s_seeded = false;
            for chunk in members.chunks(5000) {
                state::seed(&self.pool, chunk, generation, &enabled, -3 * 86_400, (history == Some(true)).then_some("trial")).await?;
            }
            tracing::info!(event = "full_crawl_entered", crawl_generation = generation, reverted, "the crawl follows full indexing at this generation");
        }
        let trial_history = history == Some(true);
        let skipped = trial_history.then_some("trial");
        // Members, then S once the community fill has ended, ahead of the
        // list.
        let enabled_now = state::read_groups(&self.pool).await?.map(|g| g.enabled()).unwrap_or(enabled);
        if on && !*s_seeded {
            for chunk in members.chunks(5000) {
                state::seed(&self.pool, chunk, generation, &enabled_now, -3 * 86_400, skipped).await?;
            }
            for chunk in scope_dids.chunks(5000) {
                state::seed(&self.pool, chunk, generation, &enabled_now, -2 * 86_400, skipped).await?;
            }
            *s_seeded = true;
        }
        super::groups_change::step(&self.pool, generation).await?;
        // The takedown relays' status pass at entry, whether the crawl runs
        // or not, then the entry step.
        self.status_step(generation, &enabled_now).await?;
        self.status_again_step(generation).await?;
        self.first_sight_step().await?;
        // The sweeper and the re-list run with the crawl on or off.
        self.sweep(generation, &enabled_now).await?;
        self.relist(generation, &enabled_now).await?;
        let paused = self.budget_paused(generation).await?;
        let Some(trial_history) = history.map(|_| trial_history) else {
            self.running.store(0, Ordering::SeqCst);
            self.note(|p| {
                p.running = false;
                p.history = false;
            });
            return Ok(());
        };
        if paused {
            self.running.store(0, Ordering::SeqCst);
            return Ok(());
        }
        // On demand: accounts members read move ahead of the list.
        if self.full.on_demand == crate::config::OnDemand::Members {
            state::hurry(&self.pool, &self.on_demand.take(1000)).await?;
        }
        self.list_step(generation, &enabled_now).await?;
        self.ensure_claimers(generation, trial_history);
        if last_complete.elapsed() >= Duration::from_secs(30) {
            *last_complete = Instant::now();
            if self.may_complete(generation).await? {
                let turned = state::complete_groups(&self.pool, generation).await?;
                for g in &turned {
                    tracing::info!(event = "full_group_complete", group = %g, crawl_generation = generation, "a group's history is complete");
                }
            }
            self.repair_counts_after_complete(generation).await?;
        }
        Ok(())
    }

    /// With `[full] counts = "auto"`, one keyset counts repair for the groups
    /// that turned complete, in the background; their counts read locally
    /// once it has finished.
    async fn repair_counts_after_complete(&self, generation: i64) -> Result<(), sqlx::Error> {
        if self.full.counts != crate::config::FullCounts::Auto || self.repairing.load(Ordering::SeqCst) {
            return Ok(());
        }
        if state::counts_pending(&self.pool, generation).await?.is_empty() {
            return Ok(());
        }
        self.repairing.store(true, Ordering::SeqCst);
        let pool = self.pool.clone();
        let repairing = Arc::clone(&self.repairing);
        tokio::spawn(async move {
            match state::repair_counts_for_complete(&pool, generation).await {
                Ok(marked) => {
                    for g in &marked {
                        tracing::info!(event = "full_group_counts_ready", group = %g, crawl_generation = generation, "the counts repair ran: the group's counts read locally");
                    }
                }
                Err(error) => tracing::warn!(event = "full_counts_repair_failed", %error, "the counts repair after a group turned complete failed; it runs again"),
            }
            repairing.store(false, Ordering::SeqCst);
        });
        Ok(())
    }

    /// Start the claimers for `generation` unless they run.
    fn ensure_claimers(&self, generation: i64, trial_history: bool) {
        if !self.claimers {
            return;
        }
        // A trial kept as full for real changes what the claimers skip.
        let tag = (u64::try_from(generation).unwrap_or(0) + 1) * 2 + u64::from(trial_history);
        if self.running.swap(tag, Ordering::SeqCst) == tag {
            return;
        }
        self.note(|p| {
            p.running = true;
            p.history = true;
        });
        for _ in 0..self.crawl.concurrency.max(1) {
            let me = self.clone();
            tokio::spawn(async move { me.claimer(tag, generation, trial_history).await });
        }
    }

    async fn claimer(self, tag: u64, generation: i64, trial_history: bool) {
        while self.running.load(Ordering::SeqCst) == tag {
            self.repo_pace.wait().await;
            if self.running.load(Ordering::SeqCst) != tag {
                return;
            }
            let claimed = match state::claim_next(&self.pool, generation, self.crawl.claim_lease_secs).await {
                Ok(c) => c,
                Err(error) => {
                    tracing::warn!(event = "full_crawl_claim_failed", %error, "a crawl claim failed");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let Some(claim) = claimed else {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            };
            match self.process(&claim, trial_history).await {
                Ok(outcome) => {
                    tracing::debug!(event = "full_crawl_repo", did = %claim.did, ?outcome, "a repository was crawled");
                    self.note(|p| match outcome {
                        super::apply::Outcome::Failed(_) => p.failed += 1,
                        _ => p.crawled += 1,
                    });
                }
                Err(error) => {
                    tracing::warn!(event = "full_crawl_repo_error", did = %claim.did, %error, "a repository's crawl hit a database error; it is retried");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// The free-space floor: below `pause_free_percent` the crawl pauses
    /// (its groups show Paused); it resumes two points above.
    async fn budget_paused(&self, generation: i64) -> Result<bool, sqlx::Error> {
        if self.full.at_budget == AtBudget::None {
            return Ok(false);
        }
        let space = crate::admin::stats::space(std::path::Path::new(crate::admin::stats::POSTGRES_MOUNT)).or_else(|| crate::admin::stats::space(&self.data_dir));
        let Some(space) = space.filter(|s| s.total > 0) else {
            return Ok(false);
        };
        #[allow(clippy::cast_precision_loss)]
        let free = space.free as f64 * 100.0 / space.total as f64;
        let was = self.progress().paused;
        let floor = f64::from(self.full.pause_free_percent);
        let paused = if was { free < floor + 2.0 } else { free < floor };
        if paused != was {
            state::set_paused(&self.pool, generation, paused).await?;
            metrics::gauge!(super::METRIC_PAUSED).set(if paused { 1.0 } else { 0.0 });
            tracing::warn!(event = "full_crawl_paused", paused, free_percent = free, "the crawl's free-space floor");
            self.note(|p| p.paused = paused);
        }
        Ok(paused)
    }

    /// The listing's stored state, reset for a new crawl generation.
    async fn list_state(&self, generation: i64) -> Result<Value, sqlx::Error> {
        let v: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(LIST_KEY).fetch_optional(&self.pool).await?;
        Ok(match v {
            Some(v) if v.get("crawl_generation").and_then(Value::as_i64) == Some(generation) => v,
            _ => json!({ "crawl_generation": generation, "list": {}, "status": {}, "listed": 0, "entry_done": false }),
        })
    }

    async fn save_list_state(&self, v: &Value) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
        )
        .bind(LIST_KEY)
        .bind(v)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Page the listing relays into the queue while it runs low: the list
    /// comes after members, S and on-demand, so it is read only as fast as
    /// the crawl takes it.
    async fn list_step(&self, generation: i64, groups: &[String]) -> Result<(), sqlx::Error> {
        let queued: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT 1 FROM full_crawl WHERE state = 'pending' AND generation = $1 AND next_at <= now() LIMIT 2000) x",
        )
        .bind(generation)
        .fetch_one(&self.pool)
        .await?;
        if queued >= 1000 {
            return Ok(());
        }
        let mut st = self.list_state(generation).await?;
        let mut listed = st.get("listed").and_then(Value::as_i64).unwrap_or(0);
        for relay in self.listing_relays() {
            let cursor = st["list"].get(&relay.label).cloned().unwrap_or(Value::Null);
            if cursor.as_str() == Some("done") {
                continue;
            }
            self.list_pace.wait().await;
            match self.source.list_repos(&relay.base, cursor.as_str()).await {
                Ok(page) => {
                    let dids: Vec<String> = page.repos.iter().map(|r| r.did.clone()).filter(|d| proto_blue::syntax::Did::is_valid(d)).collect();
                    listed += i64::try_from(dids.len()).unwrap_or(0);
                    state::seed(&self.pool, &dids, generation, groups, 0, None).await?;
                    if self.status_relay(&relay.label) {
                        record_statuses(&self.pool, &relay.label, &page.repos).await?;
                    }
                    let next = match page.cursor {
                        Some(c) if !page.repos.is_empty() => Value::String(c),
                        _ => Value::String("done".into()),
                    };
                    st["list"][&relay.label] = next;
                }
                Err(error) => {
                    tracing::warn!(event = "full_crawl_list_waiting", relay = %relay.label, %error, "the crawl's list is waiting on a relay");
                }
            }
        }
        st["listed"] = json!(listed);
        self.save_list_state(&st).await
    }

    /// The takedown relays' status pass at entry to full, at its own pace,
    /// then the entry step (older-generation rows moved to this one).
    async fn status_step(&self, generation: i64, groups: &[String]) -> Result<(), sqlx::Error> {
        let mut st = self.list_state(generation).await?;
        if st.get("entry_done").and_then(Value::as_bool) == Some(true) {
            return Ok(());
        }
        let relays: Vec<Relay> = self.relays.iter().filter(|r| self.status_relay(&r.label)).cloned().collect();
        let mut done = true;
        for relay in relays {
            let cursor = st["status"].get(&relay.label).cloned().unwrap_or(Value::Null);
            if cursor.as_str() == Some("done") {
                continue;
            }
            done = false;
            self.status_pace.wait().await;
            match self.source.list_repos(&relay.base, cursor.as_str()).await {
                Ok(page) => {
                    record_statuses(&self.pool, &relay.label, &page.repos).await?;
                    st["status"][&relay.label] = match page.cursor {
                        Some(c) if !page.repos.is_empty() => Value::String(c),
                        _ => Value::String("done".into()),
                    };
                }
                Err(error) => tracing::warn!(event = "full_status_pass_waiting", relay = %relay.label, %error, "the status pass is waiting on a relay"),
            }
        }
        if done {
            let moved = state::entry_step(&self.pool, generation, groups).await?;
            st["entry_done"] = json!(true);
            tracing::info!(event = "full_entry_step", moved, "the takedown relays' status pass ended; older rows moved to this generation");
        }
        self.save_list_state(&st).await
    }

    /// Whether Complete may be tested: the entry step done, the list paged
    /// through on every listing relay, and the merged list not empty.
    async fn may_complete(&self, generation: i64) -> Result<bool, sqlx::Error> {
        let st = self.list_state(generation).await?;
        let relays = self.listing_relays();
        Ok(st.get("entry_done").and_then(Value::as_bool) == Some(true)
            && !relays.is_empty()
            && relays.iter().all(|r| st["list"].get(&r.label).and_then(Value::as_str) == Some("done"))
            && st.get("listed").and_then(Value::as_i64).unwrap_or(0) > 0)
    }

    /// Accounts first seen live: each takedown relay's `getRepoStatus`, on
    /// the status pass's pace.
    async fn first_sight_step(&self) -> Result<(), sqlx::Error> {
        let relays: Vec<Relay> = self.relays.iter().filter(|r| self.status_relay(&r.label)).cloned().collect();
        if relays.is_empty() {
            self.first_sight.take(usize::MAX);
            return Ok(());
        }
        for did in self.first_sight.take(10) {
            for relay in &relays {
                self.status_pace.wait().await;
                match self.source.relay_repo_status(&relay.base, &did).await {
                    Ok(Some(s)) => crate::indexing::full::record_relay_status(&self.pool, &did, &relay.label, Some(s.active), s.status.as_deref()).await?,
                    Ok(None) => {}
                    Err(error) => tracing::debug!(event = "full_first_sight_status_failed", %did, relay = %relay.label, %error, "a first sighting's status could not be read"),
                }
            }
        }
        Ok(())
    }
}

/// Record a takedown relay's listing page: its non-active entries are kept
/// per relay, and an active entry removes the account's row of that relay.
///
/// # Errors
///
/// Any database error.
pub async fn record_statuses(pool: &PgPool, relay: &str, repos: &[ListedRepo]) -> Result<(), sqlx::Error> {
    let active: Vec<String> = repos.iter().filter(|r| r.active != Some(false)).map(|r| r.did.clone()).collect();
    if !active.is_empty() {
        sqlx::query("DELETE FROM relay_account_status r USING dids d WHERE d.id = r.account AND d.did = ANY($1) AND r.relay = $2")
            .bind(&active)
            .bind(relay)
            .execute(pool)
            .await?;
    }
    for r in repos.iter().filter(|r| r.active == Some(false)) {
        crate::indexing::full::record_relay_status(pool, &r.did, relay, Some(false), r.status.as_deref()).await?;
    }
    Ok(())
}
