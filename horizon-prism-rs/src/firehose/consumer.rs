//! The firehose consumer worker (design-v2 §5.3, §5.7, §6.11.4; firehose
//! throughput design).
//!
//! **Startup.** Load the scripts; make sure the group exists (below); then
//! `XAUTOCLAIM` under `claim_in_progress` (deleted ids, or a flag left set,
//! are a gap); then drain the pending list.
//!
//! **Loop.** Read through `read_checked`; only when it returns no entries,
//! issue a blocking `XREADGROUP … BLOCK 1000 … >`. Entries are processed
//! sequentially and acked (pipelined per batch) after: each in its own
//! transaction, or none for a commit with nothing to write. A transient
//! failure sleeps with backoff (100 ms doubling to 10 s) and re-reads the
//! pending list.
//!
//! **Gaps** run the gap transaction before anything further is read or acked;
//! then `reanchor` sets the anchor and reads the first batch, or the pending
//! ids are acked. The anchor is the oldest entry, unless the worker has
//! measured itself slower than its input over a failure-free window: then
//! it is the tail as it was when the gap was detected.
//!
//! **A missing group**, wherever a read or `reanchor` reports it, and at
//! startup: while watermarks exist, the recreation gap is first marked owed
//! in `worker_state`, then the group is created at the tail, then the gap
//! runs and clears the mark in its own transaction. Nothing is read while
//! the mark is set.
//!
//! **Throughput.** Each successful step is timed, less its blocking wait, and
//! booked as busy; a step that processed a full batch and ran no gap
//! transaction is booked as backlog too (firehose throughput design §4.2).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use redis::Value;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::coverage::CoverageHandle;
use super::deadletter::{self, DeadLetter};
use super::gap::{self, GapCause};
use super::router::route_identity;
use super::scripts::{self, CheckedRead, GapEvidence, ReanchorMode, ScriptError};
use super::stream::{Commit, Event, IdentityKind, StreamEntry, decode, parse_read_reply, repo_hint};
use super::watermark::{Watermarks, contribution};
use super::writer::{ApplyOptions, Transient, WriteSource, Writer};
use crate::config::FirehoseConfig;
use crate::subscriptions::triggers;

/// `XAUTOCLAIM` min-idle (§5.3).
pub const AUTOCLAIM_MIN_IDLE_MS: u64 = 60_000;
/// Blocking read timeout (§5.3).
pub const BLOCK_MS: u64 = 1000;
/// Transient backoff bounds (§5.3).
pub const BACKOFF_MIN: Duration = Duration::from_millis(100);
pub const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// Lag and pending sampling interval (§5.7).
pub const LAG_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
/// Sustained lag before `firehose_lag_warn` (§5.7).
pub const LAG_WARN_AFTER: Duration = Duration::from_secs(60);
/// Each sampler call's timeout: a hung Redis cannot stall the sampler.
pub const SAMPLE_CALL_TIMEOUT: Duration = Duration::from_secs(3);
/// The backlog fraction's span, and how long it must stay above
/// [`HEADROOM_WARN_FRACTION`] before `firehose_headroom_warn` (§5.7).
pub const HEADROOM_SPAN: Duration = Duration::from_secs(300);
pub const HEADROOM_WARN_AFTER: Duration = Duration::from_secs(900);
pub const HEADROOM_WARN_FRACTION: f64 = 0.5;
/// F2's throughput samples: at most one per `SAMPLE_EVERY`, kept for
/// `WINDOW` (firehose throughput design §2.2, C-H1).
pub const SAMPLE_EVERY: Duration = Duration::from_millis(100);
pub const WINDOW: Duration = Duration::from_secs(30);

pub const METRIC_LAG: &str = "firehose_consumer_lag_entries";
pub const METRIC_PENDING: &str = "firehose_consumer_pending";
/// 1 while the consumer group exists, else 0; unchanged when the sampler
/// cannot reach Redis.
pub const METRIC_GROUP_PRESENT: &str = "firehose_consumer_group_present";
/// One per processed entry, by `path`.
pub const METRIC_PROCESSED: &str = "firehose_entries_processed_total";
/// Wall time in successful steps, less blocking waits. The `metrics` crate's
/// counters are integers, so the seconds counters are gauges that only
/// grow; `rate()` reads them as it reads a counter.
pub const METRIC_BUSY: &str = "firehose_consumer_busy_seconds_total";
pub const METRIC_BACKLOG_PROCESSED: &str = "firehose_entries_processed_backlog_total";
pub const METRIC_BACKLOG_BUSY: &str = "firehose_consumer_backlog_busy_seconds_total";
/// The stream's input, from `XINFO STREAM` `entries-added`.
pub const METRIC_STREAM_ADDED: &str = "firehose_stream_entries_added_total";
pub const METRIC_REANCHOR: &str = "firehose_reanchor_total";
pub const METRIC_REANCHOR_DECISIONS: &str = "firehose_reanchor_decisions_total";
pub const METRIC_SAMPLE_CALLS: &str = "firehose_lag_sample_calls_total";
pub const METRIC_SAMPLE_FAILURES: &str = "firehose_lag_sample_failures_total";

/// A failure that aborts the current step; always transient.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("script: {0}")]
    Script(#[from] ScriptError),
    #[error(transparent)]
    Write(#[from] Transient),
    #[error("unexpected reply: {0}")]
    Shape(String),
    /// The stream's key or the group is missing (firehose throughput design
    /// §6.2), with what reported it.
    #[error("consumer group missing: {0}")]
    NoGroup(String),
}

/// §6.2: whether a Redis answer says the stream's key or the group is
/// missing. `NOGROUP`, or an `ERR` whose detail is `no such key`, alone or
/// followed by a space (an error escaping a script carries a
/// ` script: <sha>, on @user_script:<n>.` suffix). Nothing transient
/// answers either: a loading Redis answers `LOADING`, a dropped connection
/// is an I/O error, and `OOM` and `BUSY` have codes of their own.
#[must_use]
pub fn is_no_group(e: &redis::RedisError) -> bool {
    match e.code() {
        Some("NOGROUP") => true,
        Some("ERR") => e
            .detail()
            .and_then(|d| d.strip_prefix("no such key"))
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(' ')),
        _ => false,
    }
}

fn redis_error(e: redis::RedisError) -> ConsumerError {
    if is_no_group(&e) {
        ConsumerError::NoGroup(e.to_string())
    } else {
        ConsumerError::Redis(e)
    }
}

fn script_error(e: ScriptError) -> ConsumerError {
    match e {
        ScriptError::Redis(e) => redis_error(e),
        other => ConsumerError::Script(other),
    }
}

/// State shared with `/xrpc/_health`.
#[derive(Debug, Default)]
pub struct ConsumerStatus {
    gap_blocked: AtomicBool,
}

impl ConsumerStatus {
    /// A gap was detected, or a recreation gap is owed, and its transaction
    /// has not yet committed.
    #[must_use]
    pub fn gap_blocked(&self) -> bool {
        self.gap_blocked.load(Ordering::Relaxed)
    }

    fn set_gap_blocked(&self, v: bool) {
        self.gap_blocked.store(v, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Re-read this consumer's pending list (`0`).
    Pending,
    /// Read new entries (`>`).
    New,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::New => "new",
        }
    }
}

/// How a processed entry went (`firehose_entries_processed_total{path}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryPath {
    /// A commit with nothing to write: no transaction.
    NoTx,
    /// A commit applied in a transaction.
    Tx,
    /// An identity, account, handle or tombstone event.
    Identity,
    /// An undecodable entry, dead-lettered whole.
    DeadLetter,
}

impl EntryPath {
    pub const ALL: [Self; 4] = [Self::NoTx, Self::Tx, Self::Identity, Self::DeadLetter];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoTx => "no_tx",
            Self::Tx => "tx",
            Self::Identity => "identity",
            Self::DeadLetter => "dead_letter",
        }
    }
}

/// Why a re-anchor took its mode: the first condition of §2.2 that failed,
/// or `tail` (`firehose_reanchor_decisions_total{reason}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    NoPrev,
    StreamReset,
    Span,
    NoWindow,
    NotSaturated,
    NotSlower,
    Tail,
    /// A tail was chosen, but an entry after `L` had been trimmed.
    TailTrimmed,
}

impl Reason {
    pub const ALL: [Self; 8] = [
        Self::NoPrev,
        Self::StreamReset,
        Self::Span,
        Self::NoWindow,
        Self::NotSaturated,
        Self::NotSlower,
        Self::Tail,
        Self::TailTrimmed,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoPrev => "no_prev",
            Self::StreamReset => "stream_reset",
            Self::Span => "span",
            Self::NoWindow => "no_window",
            Self::NotSaturated => "not_saturated",
            Self::NotSlower => "not_slower",
            Self::Tail => "tail",
            Self::TailTrimmed => "tail_trimmed",
        }
    }
}

/// What F2 compares across its window (§2.2): elapsed time, the stream's
/// growth (`None` when it shrank: a recreated stream), entries processed and
/// busy time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowDelta {
    pub dt_us: u64,
    pub da: Option<u64>,
    pub dp: u64,
    pub db_us: u64,
}

/// The re-anchor's mode and reason at a trim gap (§2.2). The anchor is the
/// oldest entry unless all four hold: a repeat gap within one span; a
/// window reaching back `WINDOW` with no failed step; busy for at least
/// 90 % of it; and fewer entries processed per busy second than the stream
/// gained per second. Integer arithmetic only.
#[must_use]
pub fn reanchor_mode(prev: Option<u64>, ev: &GapEvidence, window: Option<WindowDelta>) -> (ReanchorMode, Reason) {
    let oldest = |reason| (ReanchorMode::Oldest, reason);
    let Some(prev) = prev else {
        return oldest(Reason::NoPrev);
    };
    let Some(since) = ev.added().checked_sub(prev) else {
        return oldest(Reason::StreamReset);
    };
    if since >= ev.stream_length() {
        return oldest(Reason::Span);
    }
    let Some(w) = window else {
        return oldest(Reason::NoWindow);
    };
    let Some(da) = w.da else {
        return oldest(Reason::StreamReset);
    };
    if 10 * u128::from(w.db_us) < 9 * u128::from(w.dt_us) {
        return oldest(Reason::NotSaturated);
    }
    if u128::from(w.dp) * u128::from(w.dt_us) >= u128::from(da) * u128::from(w.db_us) {
        return oldest(Reason::NotSlower);
    }
    (ReanchorMode::Tail, Reason::Tail)
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    t: Instant,
    added: u64,
    processed: u64,
    busy: Duration,
}

/// The worker's own throughput counters and F2's window of samples
/// (firehose throughput design §2.2, §4.2), updated by the worker alone.
#[derive(Debug)]
pub struct Throughput {
    /// Entries processed, every path.
    pub processed: u64,
    /// Time in successful steps, less blocking waits.
    pub busy: Duration,
    /// Entries and time of successful full steps without a gap.
    pub backlog_processed: u64,
    pub backlog_busy: Duration,
    /// `processed` by [`EntryPath`], in [`EntryPath::ALL`]'s order.
    pub by_path: [u64; 4],
    window: VecDeque<Sample>,
    span: Duration,
}

impl Throughput {
    fn new(span: Duration) -> Self {
        Self {
            processed: 0,
            busy: Duration::ZERO,
            backlog_processed: 0,
            backlog_busy: Duration::ZERO,
            by_path: [0; 4],
            window: VecDeque::new(),
            span,
        }
    }

    /// Push a sample of `entries-added`, unless the newest is younger than
    /// `SAMPLE_EVERY`; keep the newest sample at least `span` old and
    /// everything after it. `running` is the step in progress so far, less
    /// its blocking wait: samples are taken inside steps, so busy time is
    /// read as of the sample, not as of the last booked step.
    pub fn push(&mut self, now: Instant, added: u64, running: Duration) {
        if self.window.back().is_some_and(|s| now.duration_since(s.t) < SAMPLE_EVERY) {
            return;
        }
        let busy = self.busy + running;
        self.window.push_back(Sample { t: now, added, processed: self.processed, busy });
        while self.window.get(1).is_some_and(|s| now.duration_since(s.t) >= self.span) {
            self.window.pop_front();
        }
    }

    /// Forget every sample: after a failed step, and when the group is found
    /// absent.
    pub fn clear(&mut self) {
        self.window.clear();
    }

    /// How many samples the window holds.
    #[must_use]
    pub fn samples(&self) -> usize {
        self.window.len()
    }

    /// The oldest sample's time.
    #[must_use]
    pub fn oldest_sample(&self) -> Option<Instant> {
        self.window.front().map(|s| s.t)
    }

    /// The deltas from `s0`, the newest sample at least `span` old, to now
    /// (`running` as in [`Self::push`]); `None` without one.
    #[must_use]
    pub fn delta(&self, now: Instant, added: u64, running: Duration) -> Option<WindowDelta> {
        let s0 = self.window.iter().rev().find(|s| now.duration_since(s.t) >= self.span)?;
        Some(WindowDelta {
            dt_us: micros(now.duration_since(s0.t)),
            da: added.checked_sub(s0.added),
            dp: self.processed.saturating_sub(s0.processed),
            db_us: micros((self.busy + running).saturating_sub(s0.busy)),
        })
    }
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// What the lag sampler reads from the worker.
#[derive(Debug, Default)]
pub struct ThroughputShared {
    backlog_busy_us: AtomicU64,
}

impl ThroughputShared {
    #[must_use]
    pub fn backlog_busy_us(&self) -> u64 {
        self.backlog_busy_us.load(Ordering::Relaxed)
    }
}

/// One step's own figures, booked when it ends.
#[derive(Debug, Default)]
struct StepStats {
    started: Option<Instant>,
    /// Time inside `XREADGROUP … BLOCK`.
    blocked: Duration,
    /// It ran a gap transaction, which took `gap_time`.
    gap: bool,
    gap_time: Duration,
    /// Live entries it processed.
    processed: u64,
}

/// Test-only settings and the harness's record (firehose throughput design
/// §2.4, §5.2).
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestTuning {
    pub entry_delay: Duration,
    paced_until: Option<tokio::time::Instant>,
    pub ring_reset: bool,
    /// The database's name: the key of the consumer's hooks.
    pub db: String,
    pub record: Arc<std::sync::Mutex<Record>>,
}

/// One re-anchor, as the harness records it.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct ReanchorRecord {
    pub at: Instant,
    pub mode: ReanchorMode,
    pub reason: Reason,
    pub anchor: String,
    pub busy_share: Option<f64>,
    pub processed_per_added: Option<f64>,
}

#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub(crate) struct Record {
    pub reanchors: Vec<ReanchorRecord>,
    /// Each missing group handled: the mode of its step and the error.
    pub no_groups: Vec<(&'static str, String)>,
    /// Each processed entry's id, in order.
    pub processed: Vec<(Instant, String)>,
    /// Each committed gap transaction: its cause, epoch and time.
    pub gaps: Vec<(Instant, GapCause, i64, Duration)>,
    /// Each batch processed, by size.
    pub batches: Vec<(Instant, usize)>,
    /// F2's state when the recreation flag was set: `prev_reanchor_added`
    /// and the window's samples.
    pub at_recreation_flag: Vec<(Option<u64>, usize)>,
}

/// The worker.
pub struct Consumer {
    cfg: FirehoseConfig,
    /// A dedicated connection: blocking reads must not stall other users of
    /// a shared multiplexed connection.
    redis: ConnectionManager,
    pool: PgPool,
    writer: Writer,
    watermarks: Watermarks,
    coverage: CoverageHandle,
    status: Arc<ConsumerStatus>,
    /// `lists.sync_min_interval_secs`, for a sync an owner's event causes.
    sync_min_interval_secs: u32,
    throughput: Throughput,
    shared: Arc<ThroughputShared>,
    /// `A` at this process's previous trim re-anchor.
    prev_reanchor_added: Option<u64>,
    /// A recreation gap is owed: the cache of `recreation_gap_owed`.
    owed: bool,
    step: StepStats,
    #[cfg(test)]
    pub(crate) test: TestTuning,
}

impl Consumer {
    #[must_use]
    pub fn new(
        cfg: FirehoseConfig,
        redis: ConnectionManager,
        writer: Writer,
        watermarks: Watermarks,
        coverage: CoverageHandle,
        status: Arc<ConsumerStatus>,
    ) -> Self {
        let pool = writer.pool().clone();
        register_series();
        Self {
            #[cfg(test)]
            test: TestTuning {
                entry_delay: Duration::ZERO,
                paced_until: None,
                ring_reset: true,
                db: pool.connect_options().get_database().unwrap_or_default().to_string(),
                record: Arc::default(),
            },
            cfg,
            redis,
            pool,
            writer,
            watermarks,
            coverage,
            status,
            sync_min_interval_secs: 600,
            throughput: Throughput::new(WINDOW),
            shared: Arc::default(),
            prev_reanchor_added: None,
            owed: false,
            step: StepStats::default(),
        }
    }

    /// Set `lists.sync_min_interval_secs`.
    #[must_use]
    pub fn with_sync_min_interval_secs(mut self, secs: u32) -> Self {
        self.sync_min_interval_secs = secs;
        self
    }

    /// What the lag sampler reads from this worker.
    #[must_use]
    pub fn throughput_shared(&self) -> Arc<ThroughputShared> {
        Arc::clone(&self.shared)
    }

    /// The worker's counters.
    #[must_use]
    pub fn throughput(&self) -> &Throughput {
        &self.throughput
    }

    fn apply_options(&self) -> ApplyOptions {
        ApplyOptions {
            advance_applied_rev: true,
            dead_letter_readiness: true,
            attempts: self.cfg.deterministic_attempts,
            live: true,
            subscription_rules: true,
            source: WriteSource::Firehose,
        }
    }

    /// Run startup (retrying with backoff) and then the loop, forever.
    #[must_use]
    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut backoff = BACKOFF_MIN;
            loop {
                match self.startup().await {
                    Ok(()) => break,
                    Err(error) => {
                        tracing::warn!(event = "firehose_startup_failed", %error, "consumer startup failed; retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(BACKOFF_MAX);
                    }
                }
            }
            self.run().await;
        })
    }

    /// Startup steps 1–2 (§5.3); the pending drain is the loop's first mode.
    ///
    /// # Errors
    ///
    /// Any transient failure; startup is re-run from the top.
    pub async fn startup(&mut self) -> Result<(), ConsumerError> {
        scripts::load_all(&mut self.redis).await?;
        // A recreation gap a previous run owed is run before anything else.
        self.owed = gap::recreation_gap_owed(&self.pool).await?;
        if self.owed {
            self.status.set_gap_blocked(true);
        }
        self.ensure_group().await?;
        self.autoclaim().await
    }

    /// §6.11.4 item 3 (firehose throughput design §6.2): if the group is
    /// absent, forget F2's state; while watermarks exist, durably mark the
    /// recreation gap owed; create the group at the tail. Then, if a gap is
    /// owed, from now or from before, run it; its transaction clears the
    /// mark. Every entry the creation skips was added before the create, so
    /// before the gap began, and the gap's jobs cover it.
    async fn ensure_group(&mut self) -> Result<(), ConsumerError> {
        if !self.group_exists().await? {
            self.prev_reanchor_added = None;
            self.throughput.clear();
            if Watermarks::any_persisted(&self.pool).await? {
                // The create runs only after this write returned `Ok`.
                gap::set_recreation_gap_owed(&self.pool).await?;
                self.owed = true;
                self.status.set_gap_blocked(true);
            }
            #[cfg(test)]
            {
                use crate::test_support::hooks;
                let state = (self.prev_reanchor_added, self.throughput.samples());
                self.test.record.lock().expect("record").at_recreation_flag.push(state);
                hooks::pause(hooks::AFTER_RECREATION_FLAG, &self.test.db).await;
                if hooks::injected(hooks::GROUP_CREATE_FAILS, &self.test.db) {
                    return Err(ConsumerError::Shape("injected group-create failure".into()));
                }
            }
            let created = scripts::group_create(&mut self.redis, &self.cfg.stream_key, &self.cfg.group)
                .await
                .map_err(script_error)?;
            if created {
                tracing::info!(event = "firehose_group_created", group = %self.cfg.group, owed = self.owed, "consumer group created");
            }
            #[cfg(test)]
            crate::test_support::hooks::pause(crate::test_support::hooks::AFTER_RECREATION_CREATE, &self.test.db).await;
        }
        if self.owed {
            self.gap(GapCause::GroupRecreated, false, true).await?;
        }
        Ok(())
    }

    /// Whether the group exists; a missing stream has none.
    async fn group_exists(&mut self) -> Result<bool, ConsumerError> {
        let reply: Result<Value, redis::RedisError> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&self.cfg.stream_key)
            .query_async(&mut self.redis)
            .await;
        match reply {
            Ok(groups) => Ok(group_field(&groups, &self.cfg.group, "name").is_some()),
            Err(e) if is_no_group(&e) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// `XAUTOCLAIM` until the cursor is `0-0`, under `claim_in_progress`.
    async fn autoclaim(&mut self) -> Result<(), ConsumerError> {
        let interrupted = gap::claim_in_progress(&self.pool).await?;
        gap::set_claim_in_progress(&self.pool, true).await?;
        let mut deleted: Vec<String> = Vec::new();
        let mut cursor = "0-0".to_string();
        loop {
            let reply: Value = redis::cmd("XAUTOCLAIM")
                .arg(&self.cfg.stream_key)
                .arg(&self.cfg.group)
                .arg(&self.cfg.consumer_name)
                .arg(AUTOCLAIM_MIN_IDLE_MS)
                .arg(&cursor)
                .arg("COUNT")
                .arg(self.cfg.batch_size)
                .arg("JUSTID")
                .query_async(&mut self.redis)
                .await
                .map_err(redis_error)?;
            let (next, dels) = parse_autoclaim(reply).map_err(ConsumerError::Shape)?;
            deleted.extend(dels);
            if next == "0-0" {
                break;
            }
            cursor = next;
        }
        if interrupted || !deleted.is_empty() {
            let cause = if deleted.is_empty() { GapCause::InterruptedClaim } else { GapCause::Pending };
            self.gap(cause, true, false).await?;
            self.ack(&deleted).await?;
        } else {
            gap::set_claim_in_progress(&self.pool, false).await?;
        }
        Ok(())
    }

    /// Run the gap transaction, with `gap_blocked` set until it commits and,
    /// while a recreation gap is owed, until that one does. `clear_owed`
    /// clears `recreation_gap_owed` in the same transaction.
    async fn gap(&mut self, cause: GapCause, clear_claim: bool, clear_owed: bool) -> Result<(), ConsumerError> {
        self.status.set_gap_blocked(true);
        let started = Instant::now();
        let private = Arc::clone(self.writer.private());
        let _epoch = gap::run_gap_transaction_clearing(
            &self.pool,
            self.writer.scope(),
            private.as_ref(),
            cause,
            clear_claim,
            clear_owed,
        )
        .await?;
        let took = started.elapsed();
        self.step.gap = true;
        self.step.gap_time += took;
        #[cfg(test)]
        self.test.record.lock().expect("record").gaps.push((Instant::now(), cause, _epoch, took));
        if clear_owed {
            self.owed = false;
        }
        if !self.owed {
            self.status.set_gap_blocked(false);
        }
        Ok(())
    }

    async fn run(mut self) {
        let mut mode = Mode::Pending;
        let mut backoff = BACKOFF_MIN;
        loop {
            self.turn(&mut mode, &mut backoff).await;
        }
    }

    /// One step, timed and booked (§4.2): a successful step's time, less
    /// its blocking wait, is busy; a full one without a gap is backlog too.
    /// A failed step books nothing, clears F2's window, and sleeps.
    async fn turn(&mut self, mode: &mut Mode, backoff: &mut Duration) {
        let started = Instant::now();
        self.step = StepStats { started: Some(started), ..StepStats::default() };
        let result = self.step(mode).await;
        let elapsed = started.elapsed().saturating_sub(self.step.blocked);
        match result {
            Ok(()) => {
                *backoff = BACKOFF_MIN;
                self.book(elapsed);
            }
            Err(error) => {
                tracing::warn!(event = "firehose_transient", %error, backoff_ms = backoff.as_millis(), "transient failure; re-reading pending entries");
                if self.ring_reset() {
                    self.throughput.clear();
                }
                tokio::time::sleep(*backoff).await;
                *backoff = (*backoff * 2).min(BACKOFF_MAX);
                *mode = Mode::Pending;
            }
        }
    }

    /// The harness's driver (firehose throughput design §5.2): `run`'s loop
    /// of timed and booked steps, from the pending drain, until `stop`
    /// holds or `within` has passed. Whether `stop` held.
    #[cfg(test)]
    pub(crate) async fn run_until(&mut self, stop: impl Fn(&Self) -> bool, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        let mut mode = Mode::Pending;
        let mut backoff = BACKOFF_MIN;
        while Instant::now() < deadline {
            if stop(self) {
                return true;
            }
            self.turn(&mut mode, &mut backoff).await;
        }
        stop(self)
    }

    #[cfg(test)]
    fn ring_reset(&self) -> bool {
        self.test.ring_reset
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    fn ring_reset(&self) -> bool {
        true
    }

    /// The step in progress so far, less its blocking wait.
    fn running(&self) -> Duration {
        self.step.started.map_or(Duration::ZERO, |s| s.elapsed().saturating_sub(self.step.blocked))
    }

    fn book(&mut self, elapsed: Duration) {
        self.throughput.busy += elapsed;
        metrics::gauge!(METRIC_BUSY).increment(elapsed.as_secs_f64());
        let full = !self.step.gap && self.step.processed == u64::from(self.cfg.batch_size);
        if full {
            self.throughput.backlog_processed += self.step.processed;
            self.throughput.backlog_busy += elapsed;
            self.shared.backlog_busy_us.fetch_add(micros(elapsed), Ordering::Relaxed);
            metrics::counter!(METRIC_BACKLOG_PROCESSED).increment(self.step.processed);
            metrics::gauge!(METRIC_BACKLOG_BUSY).increment(elapsed.as_secs_f64());
        }
    }

    async fn step(&mut self, mode: &mut Mode) -> Result<(), ConsumerError> {
        // §6.2: nothing is read while a recreation gap is owed.
        if self.owed {
            self.ensure_group().await?;
        }
        match self.read_and_process(mode).await {
            // §6.2: a missing group, in either mode, from any read or from
            // `reanchor`. After a recreation the pending list is empty, so
            // the next step moves on to New.
            Err(ConsumerError::NoGroup(error)) => {
                tracing::warn!(event = "firehose_group_missing", mode = mode.as_str(), %error, "consumer group missing; recreating");
                #[cfg(test)]
                self.test.record.lock().expect("record").no_groups.push((mode.as_str(), error.clone()));
                self.ensure_group().await?;
                *mode = Mode::Pending;
                Ok(())
            }
            other => other,
        }
    }

    async fn read_and_process(&mut self, mode: &mut Mode) -> Result<(), ConsumerError> {
        match mode {
            Mode::Pending => {
                let entries = self.read_pending().await?;
                if entries.is_empty() {
                    *mode = Mode::New;
                    return Ok(());
                }
                // [id, nil]: a pending entry trimmed from the stream (§5.3).
                let (trimmed, live): (Vec<StreamEntry>, Vec<StreamEntry>) =
                    entries.into_iter().partition(|e| e.fields.is_none());
                if !trimmed.is_empty() {
                    self.gap(GapCause::Pending, false, false).await?;
                    let ids: Vec<String> = trimmed.into_iter().map(|e| e.id).collect();
                    self.ack(&ids).await?;
                }
                self.process_batch(live).await
            }
            Mode::New => {
                let read = scripts::read_checked(
                    &mut self.redis,
                    &self.cfg.stream_key,
                    &self.cfg.group,
                    &self.cfg.consumer_name,
                    self.cfg.batch_size,
                )
                .await
                .map_err(script_error)?;
                match read {
                    CheckedRead::NoGroup => Err(ConsumerError::NoGroup("read_checked: nogroup".into())),
                    CheckedRead::Gap(evidence) => {
                        let running = self.running();
                        self.throughput.push(Instant::now(), evidence.added(), running);
                        self.trim_gap(evidence).await
                    }
                    CheckedRead::Entries { added, entries } => {
                        let running = self.running();
                        self.throughput.push(Instant::now(), added, running);
                        if entries.is_empty() {
                            let entries = self.read_blocking().await?;
                            self.process_batch(entries).await
                        } else {
                            self.process_batch(entries).await
                        }
                    }
                }
            }
        }
    }

    /// A trim gap (§6.11.4; firehose throughput design §2.4): the gap
    /// transaction, then the anchor's mode, then `reanchor`, which returns
    /// the first batch, processed before the next `read_checked`.
    async fn trim_gap(&mut self, evidence: GapEvidence) -> Result<(), ConsumerError> {
        tracing::warn!(
            event = "firehose_trim_gap",
            first_entry_id = %evidence.first_entry_id,
            entries_added = %evidence.entries_added,
            length = %evidence.length,
            last_generated_id = %evidence.last_generated_id,
            "trim evidence"
        );
        self.gap(GapCause::Trim, false, false).await?;
        let added = evidence.added();
        let window = self.throughput.delta(Instant::now(), added, self.running());
        let (wanted, mut reason) = reanchor_mode(self.prev_reanchor_added, &evidence, window);
        #[cfg(test)]
        crate::test_support::hooks::pause(crate::test_support::hooks::BEFORE_REANCHOR, &self.test.db).await;
        let re = scripts::reanchor(
            &mut self.redis,
            &self.cfg.stream_key,
            &self.cfg.group,
            &self.cfg.consumer_name,
            self.cfg.batch_size,
            wanted,
            &evidence,
        )
        .await
        .map_err(script_error)?;
        if wanted == ReanchorMode::Tail && re.mode == ReanchorMode::Oldest {
            reason = Reason::TailTrimmed;
        }
        self.prev_reanchor_added = Some(added);
        let busy_share = window.map(|w| ratio(w.db_us, w.dt_us));
        let processed_per_added = window.and_then(|w| w.da.map(|da| ratio(w.dp, da)));
        tracing::info!(
            event = "firehose_reanchored",
            mode = re.mode.as_str(),
            reason = reason.as_str(),
            last_delivered_id = %re.id,
            entries_read = %re.entries_read,
            busy_share = ?busy_share,
            processed_per_added = ?processed_per_added,
            "group re-anchored after gap"
        );
        metrics::counter!(METRIC_REANCHOR, "mode" => re.mode.as_str()).increment(1);
        metrics::counter!(METRIC_REANCHOR_DECISIONS, "reason" => reason.as_str()).increment(1);
        #[cfg(test)]
        self.test.record.lock().expect("record").reanchors.push(ReanchorRecord {
            at: Instant::now(),
            mode: re.mode,
            reason,
            anchor: re.id.clone(),
            busy_share,
            processed_per_added,
        });
        self.process_batch(re.entries).await
    }

    async fn read_pending(&mut self) -> Result<Vec<StreamEntry>, ConsumerError> {
        let reply: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.cfg.group)
            .arg(&self.cfg.consumer_name)
            .arg("COUNT")
            .arg(self.cfg.batch_size)
            .arg("STREAMS")
            .arg(&self.cfg.stream_key)
            .arg("0")
            .query_async(&mut self.redis)
            .await
            .map_err(redis_error)?;
        parse_read_reply(reply).map_err(ConsumerError::Shape)
    }

    /// Only after an empty `read_checked`: everything up to the tail is read,
    /// so nothing unread can be trimmed before this returns (§6.11.4). Its
    /// wait is not busy time.
    async fn read_blocking(&mut self) -> Result<Vec<StreamEntry>, ConsumerError> {
        let started = Instant::now();
        let reply: Result<Value, redis::RedisError> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.cfg.group)
            .arg(&self.cfg.consumer_name)
            .arg("COUNT")
            .arg(self.cfg.batch_size)
            .arg("BLOCK")
            .arg(BLOCK_MS)
            .arg("STREAMS")
            .arg(&self.cfg.stream_key)
            .arg(">")
            .query_async(&mut self.redis)
            .await;
        self.step.blocked += started.elapsed();
        parse_read_reply(reply.map_err(redis_error)?).map_err(ConsumerError::Shape)
    }

    /// Process entries in stream order; ack the processed ones in one
    /// pipelined call, including on a transient failure part-way through.
    async fn process_batch(&mut self, entries: Vec<StreamEntry>) -> Result<(), ConsumerError> {
        #[cfg(test)]
        self.test.record.lock().expect("record").batches.push((Instant::now(), entries.len()));
        let mut acks: Vec<String> = Vec::with_capacity(entries.len());
        for entry in &entries {
            match self.process_entry(entry).await {
                Ok(path) => self.count(entry, path),
                Err(error) => {
                    if let Err(ack_error) = self.ack(&acks).await {
                        tracing::warn!(event = "firehose_ack_failed", error = %ack_error, "acking committed entries failed; they will be re-applied");
                    }
                    return Err(error);
                }
            }
            acks.push(entry.id.clone());
        }
        self.ack(&acks).await
    }

    /// Book one processed entry (§4.2).
    fn count(&mut self, entry: &StreamEntry, path: EntryPath) {
        self.throughput.processed += 1;
        if let Some(i) = EntryPath::ALL.iter().position(|p| *p == path) {
            self.throughput.by_path[i] += 1;
        }
        self.step.processed += 1;
        metrics::counter!(METRIC_PROCESSED, "path" => path.as_str()).increment(1);
        #[cfg(test)]
        self.test.record.lock().expect("record").processed.push((Instant::now(), entry.id.clone()));
        #[cfg(not(test))]
        let _ = entry;
    }

    async fn process_entry(&mut self, entry: &StreamEntry) -> Result<EntryPath, ConsumerError> {
        #[cfg(test)]
        {
            crate::test_support::hooks::delay(crate::test_support::hooks::CONSUMER_STALL, &self.test.db).await;
            // A set apply rate: each entry is due `entry_delay` after the
            // previous one, and the worker sleeps only once it is 2 ms ahead
            // of now, so the timer's coarseness evens out.
            if !self.test.entry_delay.is_zero() {
                let now = tokio::time::Instant::now();
                let due = self.test.paced_until.map_or(now, |d| d.max(now)) + self.test.entry_delay;
                self.test.paced_until = Some(due);
                if due > now + Duration::from_millis(2) {
                    tokio::time::sleep_until(due).await;
                }
            }
        }
        let event = match decode(entry) {
            Ok(e) => e,
            Err(error) => {
                // Deterministic: dead-letter the whole entry (§6.11.6).
                let (repo, rev) = repo_hint(entry);
                let letter = DeadLetter {
                    orig_id: entry.id.clone(),
                    entry_type: entry.field("type").unwrap_or_default().to_string(),
                    seq: entry.field("seq").unwrap_or_default().to_string(),
                    repo,
                    rev,
                    error,
                    data: entry.raw("data").map(<[u8]>::to_vec).unwrap_or_default(),
                    ..DeadLetter::default()
                };
                self.dead_letter(&[letter]).await?;
                return Ok(EntryPath::DeadLetter);
            }
        };
        match event {
            Event::Identity { kind, did, .. } => {
                route_identity(&self.pool, kind, &did).await?;
                // Rule 4 (§4.3): an owner of a subscribed list that looks
                // gone is looked at again; an ordinary owner's events
                // cause no sync.
                let owner_event = matches!(kind, IdentityKind::Identity | IdentityKind::Account);
                if owner_event && self.writer.scope().read_guard().await.subscribed.tracks(&did) {
                    triggers::owner_event(&self.pool, &did, self.sync_min_interval_secs).await?;
                }
                Ok(EntryPath::Identity)
            }
            Event::Commit(commit) => self.process_commit(entry, commit).await,
        }
    }

    async fn process_commit(&mut self, entry: &StreamEntry, commit: Commit) -> Result<EntryPath, ConsumerError> {
        let label = self.watermarks.label_for(commit.relay.as_deref()).to_string();
        self.coverage.observe(&commit.repo, &commit.rev, &label);
        tracing::debug!(
            event = "firehose_ops",
            id = %entry.id,
            relay = %label,
            repo = %commit.repo,
            rev = %commit.rev,
            ops = commit.ops.len(),
            "commit received"
        );
        let report = self
            .writer
            .apply(&commit.repo, &commit.rev, &commit.ops, self.apply_options())
            .await?;
        if !report.failed.is_empty() {
            let data = entry.raw("data").map(<[u8]>::to_vec).unwrap_or_default();
            let letters: Vec<DeadLetter> = report
                .failed
                .iter()
                .map(|f| DeadLetter {
                    orig_id: entry.id.clone(),
                    entry_type: "commit".to_string(),
                    relay: label.clone(),
                    seq: entry.field("seq").unwrap_or_default().to_string(),
                    repo: commit.repo.clone(),
                    rev: commit.rev.clone(),
                    action: f.action.as_str().to_string(),
                    path: f.path.clone(),
                    error: f.error.clone(),
                    data: data.clone(),
                })
                .collect();
            self.dead_letter(&letters).await?;
        }
        if let Some(c) = contribution(entry.id_ms(), commit.time) {
            self.watermarks.observe(&label, c);
        }
        Ok(if report.transaction { EntryPath::Tx } else { EntryPath::NoTx })
    }

    async fn dead_letter(&mut self, letters: &[DeadLetter]) -> Result<(), ConsumerError> {
        deadletter::write(
            &mut self.redis,
            &self.cfg.dead_letter_stream,
            self.cfg.dead_letter_maxlen,
            letters,
        )
        .await?;
        Ok(())
    }

    async fn ack(&mut self, ids: &[String]) -> Result<(), ConsumerError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        for chunk in ids.chunks(500) {
            pipe.cmd("XACK")
                .arg(&self.cfg.stream_key)
                .arg(&self.cfg.group)
                .arg(chunk)
                .ignore();
        }
        pipe.query_async::<()>(&mut self.redis).await?;
        Ok(())
    }
}

#[allow(clippy::cast_precision_loss)]
fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}

/// Every series of the firehose throughput design §4.2, at zero (the group
/// gauge at 1), so a rule over them has data from the start.
fn register_series() {
    for path in EntryPath::ALL {
        metrics::counter!(METRIC_PROCESSED, "path" => path.as_str()).increment(0);
    }
    for mode in [ReanchorMode::Oldest, ReanchorMode::Tail] {
        metrics::counter!(METRIC_REANCHOR, "mode" => mode.as_str()).increment(0);
    }
    for reason in Reason::ALL {
        metrics::counter!(METRIC_REANCHOR_DECISIONS, "reason" => reason.as_str()).increment(0);
    }
    for call in SampleCall::ALL {
        metrics::counter!(METRIC_SAMPLE_CALLS, "call" => call.as_str()).increment(0);
        metrics::counter!(METRIC_SAMPLE_FAILURES, "call" => call.as_str()).increment(0);
    }
    metrics::gauge!(METRIC_BUSY).increment(0.0);
    metrics::gauge!(METRIC_BACKLOG_BUSY).increment(0.0);
    metrics::counter!(METRIC_BACKLOG_PROCESSED).increment(0);
    metrics::gauge!(METRIC_GROUP_PRESENT).set(1.0);
}

/// `[next-cursor, [ids…], [deleted ids…]]`.
fn parse_autoclaim(reply: Value) -> Result<(String, Vec<String>), String> {
    let Value::Array(parts) = reply else {
        return Err(format!("XAUTOCLAIM reply: {reply:?}"));
    };
    let mut it = parts.into_iter();
    let next = it
        .next()
        .as_ref()
        .and_then(scripts::string)
        .ok_or("XAUTOCLAIM without cursor")?;
    let _claimed = it.next();
    let deleted = match it.next() {
        Some(Value::Array(ids)) => ids.iter().filter_map(scripts::string).collect(),
        _ => Vec::new(),
    };
    Ok((next, deleted))
}

/// The lag sampler's three calls (§4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleCall {
    Stream,
    Groups,
    Pending,
}

impl SampleCall {
    pub const ALL: [Self; 3] = [Self::Stream, Self::Groups, Self::Pending];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Groups => "groups",
            Self::Pending => "pending",
        }
    }
}

/// Where the sampler's replies come from; a test injects its own.
pub(crate) trait SampleCalls {
    async fn stream_info(&mut self) -> redis::RedisResult<Value>;
    async fn groups(&mut self) -> redis::RedisResult<Value>;
    async fn pending(&mut self) -> redis::RedisResult<Value>;
}

struct RedisCalls {
    conn: ConnectionManager,
    stream: String,
    group: String,
}

impl SampleCalls for RedisCalls {
    async fn stream_info(&mut self) -> redis::RedisResult<Value> {
        redis::cmd("XINFO").arg("STREAM").arg(&self.stream).query_async(&mut self.conn).await
    }

    async fn groups(&mut self) -> redis::RedisResult<Value> {
        redis::cmd("XINFO").arg("GROUPS").arg(&self.stream).query_async(&mut self.conn).await
    }

    async fn pending(&mut self) -> redis::RedisResult<Value> {
        redis::cmd("XPENDING").arg(&self.stream).arg(&self.group).query_async(&mut self.conn).await
    }
}

/// One sampler call's class (§4.2 table).
enum Reply {
    Ok(Value),
    /// The key or the group is missing.
    Missing,
    /// Anything else, a timeout included.
    Failed,
}

async fn classify_call(call: SampleCall, reply: impl Future<Output = redis::RedisResult<Value>>) -> Reply {
    metrics::counter!(METRIC_SAMPLE_CALLS, "call" => call.as_str()).increment(1);
    match tokio::time::timeout(SAMPLE_CALL_TIMEOUT, reply).await {
        Ok(Ok(v)) => Reply::Ok(v),
        Ok(Err(e)) if is_no_group(&e) => Reply::Missing,
        Ok(Err(error)) => {
            tracing::debug!(event = "firehose_lag_sample_failed", call = call.as_str(), %error, "lag sample call failed");
            metrics::counter!(METRIC_SAMPLE_FAILURES, "call" => call.as_str()).increment(1);
            Reply::Failed
        }
        Err(_) => {
            tracing::debug!(event = "firehose_lag_sample_failed", call = call.as_str(), "lag sample call timed out");
            metrics::counter!(METRIC_SAMPLE_FAILURES, "call" => call.as_str()).increment(1);
            Reply::Failed
        }
    }
}

/// What the sampler keeps between ticks.
#[derive(Debug, Default)]
pub(crate) struct SamplerState {
    prev_added: Option<u64>,
    /// The lag gauge's last value.
    pub lag: Option<u64>,
    lag_above_since: Option<Instant>,
    lag_warned: bool,
    /// `backlog_busy_us` at each tick within the span.
    backlog: VecDeque<(Instant, u64)>,
    headroom_above_since: Option<Instant>,
    headroom_warned: bool,
}

/// What `entries-added` adds to the input counter: the growth since the
/// previous sample. The first sample is a baseline, and a decrease (a
/// recreated stream) adds nothing and becomes the new baseline.
pub(crate) fn stream_added_delta(prev: &mut Option<u64>, now: u64) -> u64 {
    let delta = prev.map_or(0, |p| now.saturating_sub(p));
    *prev = Some(now);
    delta
}

/// One sampler tick (§4.2): three independent calls, each with its own
/// timeout, classed as success, missing key or group, or failure.
pub(crate) async fn sample_once<C: SampleCalls>(calls: &mut C, cfg: &FirehoseConfig, state: &mut SamplerState) {
    // 1. The stream: its input, and its length for a missing group's lag.
    let length = match classify_call(SampleCall::Stream, calls.stream_info()).await {
        Reply::Ok(info) => {
            if let Some(added) = flat_u64(&info, "entries-added") {
                metrics::counter!(METRIC_STREAM_ADDED).increment(stream_added_delta(&mut state.prev_added, added));
            }
            flat_u64(&info, "length")
        }
        Reply::Missing => Some(0),
        Reply::Failed => None,
    };
    // 2. The group.
    let listed = match classify_call(SampleCall::Groups, calls.groups()).await {
        Reply::Ok(groups) => group_field(&groups, &cfg.group, "name").map(|_| groups),
        Reply::Missing => None,
        Reply::Failed => return sample_pending(calls).await,
    };
    match listed {
        Some(groups) => {
            metrics::gauge!(METRIC_GROUP_PRESENT).set(1.0);
            if let Some(lag) = group_field(&groups, &cfg.group, "lag").and_then(|v| as_u64(&v)) {
                set_lag(state, cfg, lag);
            }
        }
        None => {
            metrics::gauge!(METRIC_GROUP_PRESENT).set(0.0);
            // This tick's length; unchanged when the stream call failed.
            if let Some(length) = length {
                set_lag(state, cfg, length);
            }
        }
    }
    sample_pending(calls).await;
}

async fn sample_pending<C: SampleCalls>(calls: &mut C) {
    match classify_call(SampleCall::Pending, calls.pending()).await {
        Reply::Ok(Value::Array(parts)) => {
            let count = parts.first().and_then(as_u64).unwrap_or(0);
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!(METRIC_PENDING).set(count as f64);
        }
        Reply::Ok(_) | Reply::Missing => metrics::gauge!(METRIC_PENDING).set(0.0),
        Reply::Failed => {}
    }
}

fn set_lag(state: &mut SamplerState, cfg: &FirehoseConfig, lag: u64) {
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(METRIC_LAG).set(lag as f64);
    state.lag = Some(lag);
    if lag > cfg.lag_warn_entries {
        let since = *state.lag_above_since.get_or_insert_with(Instant::now);
        if !state.lag_warned && since.elapsed() >= LAG_WARN_AFTER {
            tracing::warn!(event = "firehose_lag_warn", lag, threshold = cfg.lag_warn_entries, "consumer lag above threshold for 60 s");
            state.lag_warned = true;
        }
    } else {
        state.lag_above_since = None;
        state.lag_warned = false;
    }
}

/// The backlog fraction over the preceding `HEADROOM_SPAN`, from the values
/// kept at each tick; `firehose_headroom_warn` once it has been above 0.5 at
/// every tick for `HEADROOM_WARN_AFTER` (§5.7, the same condition as the
/// `FirehoseBacklogPersistent` alert).
fn headroom_tick(state: &mut SamplerState, now: Instant, backlog_busy_us: u64) {
    state.backlog.push_back((now, backlog_busy_us));
    while state.backlog.get(1).is_some_and(|(t, _)| now.duration_since(*t) >= HEADROOM_SPAN) {
        state.backlog.pop_front();
    }
    let Some(&(t0, v0)) = state.backlog.front() else {
        return;
    };
    let span = micros(now.duration_since(t0));
    if span == 0 {
        return;
    }
    let fraction = ratio(backlog_busy_us.saturating_sub(v0), span);
    if fraction > HEADROOM_WARN_FRACTION {
        let since = *state.headroom_above_since.get_or_insert(now);
        if !state.headroom_warned && now.duration_since(since) >= HEADROOM_WARN_AFTER {
            tracing::warn!(event = "firehose_headroom_warn", fraction, "backlog fraction above 0.5 for 15 min: no margin for a burst");
            state.headroom_warned = true;
        }
    } else {
        state.headroom_above_since = None;
        state.headroom_warned = false;
    }
}

/// Sample the stream's input, the group's presence, lag and pending count
/// every `interval` (§5.7), and the worker's backlog fraction. Uses a shared
/// connection; the worker's own is kept for its reads.
#[must_use]
pub fn spawn_lag_sampler(
    redis: ConnectionManager,
    cfg: FirehoseConfig,
    shared: Arc<ThroughputShared>,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut calls = RedisCalls { conn: redis, stream: cfg.stream_key.clone(), group: cfg.group.clone() };
        let mut state = SamplerState::default();
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            sample_once(&mut calls, &cfg, &mut state).await;
            headroom_tick(&mut state, Instant::now(), shared.backlog_busy_us());
        }
    })
}

fn as_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Int(i) => u64::try_from(*i).ok(),
        other => scripts::string(other).and_then(|s| s.parse().ok()),
    }
}

/// A field of a flat `[k, v, …]` reply, or of a RESP3 map.
fn flat_u64(reply: &Value, name: &str) -> Option<u64> {
    let pairs: Vec<(&Value, &Value)> = match reply {
        Value::Array(flat) => flat.chunks(2).filter_map(|c| Some((c.first()?, c.get(1)?))).collect(),
        Value::Map(m) => m.iter().map(|(k, v)| (k, v)).collect(),
        _ => return None,
    };
    pairs
        .into_iter()
        .find(|(k, _)| scripts::string(k).as_deref() == Some(name))
        .and_then(|(_, v)| as_u64(v))
}

/// One field of a group in an `XINFO GROUPS` reply (RESP2 flat arrays or
/// RESP3 maps).
fn group_field(groups: &Value, group: &str, name: &str) -> Option<Value> {
    let Value::Array(list) = groups else {
        return None;
    };
    for g in list {
        let pairs: Vec<(Value, Value)> = match g {
            Value::Array(flat) => flat.chunks(2).filter_map(|c| Some((c.first()?.clone(), c.get(1)?.clone()))).collect(),
            Value::Map(m) => m.clone(),
            _ => continue,
        };
        let find = |k: &str| {
            pairs
                .iter()
                .find(|(key, _)| scripts::string(key).as_deref() == Some(k))
                .map(|(_, v)| v.clone())
        };
        if find("name").as_ref().and_then(scripts::string).as_deref() == Some(group) {
            return find(name);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    mod bench;
    mod live;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn parses_autoclaim_replies() {
        let reply = Value::Array(vec![
            bulk("0-0"),
            Value::Array(vec![bulk("1-1")]),
            Value::Array(vec![bulk("1-2"), bulk("1-3")]),
        ]);
        assert_eq!(parse_autoclaim(reply).expect("ok"), ("0-0".to_string(), vec!["1-2".to_string(), "1-3".to_string()]));
    }

    #[test]
    fn finds_group_fields() {
        let groups = Value::Array(vec![Value::Array(vec![
            bulk("name"),
            bulk("horizon-prism-rs"),
            bulk("lag"),
            Value::Int(12),
        ])]);
        assert_eq!(group_field(&groups, "horizon-prism-rs", "lag"), Some(Value::Int(12)));
        assert_eq!(group_field(&groups, "other", "lag"), None);
    }

    fn evidence(added: u64, length: u64) -> GapEvidence {
        GapEvidence {
            first_entry_id: "1-0".into(),
            entries_added: added.to_string(),
            length: length.to_string(),
            last_generated_id: "9-0".into(),
        }
    }

    /// Firehose throughput §2.6: each condition of the rule, in order.
    #[test]
    fn reanchor_mode_cases() {
        const S: u64 = 30_000_000;
        let w = |db_us: u64, dp: u64| Some(WindowDelta { dt_us: S, da: Some(30_000), dp, db_us });
        let ev = evidence(100_000, 50_000);
        let oldest = |r| (ReanchorMode::Oldest, r);
        assert_eq!(reanchor_mode(None, &ev, w(S, 10)), oldest(Reason::NoPrev));
        assert_eq!(reanchor_mode(Some(10), &evidence(5, 50_000), w(S, 10)), oldest(Reason::StreamReset));
        assert_eq!(reanchor_mode(Some(50_000), &ev, w(S, 10)), oldest(Reason::Span));
        assert_eq!(reanchor_mode(Some(60_000), &ev, None), oldest(Reason::NoWindow));
        assert_eq!(reanchor_mode(Some(60_000), &ev, w(S / 2, 10)), oldest(Reason::NotSaturated));
        assert_eq!(reanchor_mode(Some(60_000), &ev, w(S * 89 / 100, 15_000)), oldest(Reason::NotSaturated));
        assert_eq!(reanchor_mode(Some(60_000), &ev, w(S * 95 / 100, 29_100)), oldest(Reason::NotSlower));
        assert_eq!(reanchor_mode(Some(60_000), &ev, w(S, 30_000)), oldest(Reason::NotSlower));
        assert_eq!(reanchor_mode(Some(60_000), &ev, w(S, 18_000)), (ReanchorMode::Tail, Reason::Tail));
        let shrunk = Some(WindowDelta { dt_us: S, da: None, dp: 10, db_us: S });
        assert_eq!(reanchor_mode(Some(60_000), &ev, shrunk), oldest(Reason::StreamReset));
    }

    #[test]
    fn throughput_window_prunes_to_one_sample_before_the_window() {
        let mut t = Throughput::new(WINDOW);
        let base = Instant::now();
        let mut now = base;
        for i in 0..=800u64 {
            now = base + Duration::from_millis(50 * i);
            t.push(now, i, Duration::ZERO);
        }
        // Pushes 50 ms apart keep every other one; the window keeps the
        // sample at 10 s (now − 30 s) and the 300 after it.
        assert_eq!(t.samples(), 301);
        let oldest = t.oldest_sample().expect("sample");
        assert_eq!(now.duration_since(oldest), WINDOW);
        let d = t.delta(now, 800, Duration::ZERO).expect("delta");
        assert_eq!((d.dt_us, d.da), (30_000_000, Some(600)));
        assert_eq!(t.delta(now, 100, Duration::ZERO).expect("delta").da, None);
        t.clear();
        assert_eq!(t.samples(), 0);
        assert_eq!(t.delta(now, 800, Duration::ZERO), None);
    }

    fn server_error(text: &str) -> redis::RedisError {
        redis::parse_redis_value(format!("-{text}\r\n").as_bytes())
            .expect("parse")
            .extract_error()
            .expect_err("an error")
    }

    #[test]
    fn no_group_error_mapping() {
        for text in [
            "NOGROUP No such key 'firehose:events' or consumer group 'g' in XREADGROUP with GROUP option",
            "ERR no such key",
            "ERR no such key script: abc, on @user_script:47.",
        ] {
            assert!(is_no_group(&server_error(text)), "{text}");
            assert!(matches!(redis_error(server_error(text)), ConsumerError::NoGroup(_)), "{text}");
        }
        let io = redis::RedisError::from(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"));
        assert!(!is_no_group(&io));
        for text in [
            "LOADING Redis is loading the dataset in memory",
            "BUSY Redis is busy running a script",
            "OOM command not allowed when used memory > 'maxmemory'",
            "ERR no such keyword",
        ] {
            assert!(!is_no_group(&server_error(text)), "{text}");
        }
    }

    #[test]
    fn stream_added_delta_ignores_a_reset() {
        let mut prev = None;
        let got: Vec<u64> = [100, 150, 20, 30].into_iter().map(|n| stream_added_delta(&mut prev, n)).collect();
        assert_eq!(got, [0, 50, 0, 10]);
    }

    /// One injected sampler reply.
    enum Spec {
        Ok(Value),
        /// A server error, as its RESP text.
        Server(&'static str),
        Io,
        /// Never resolves.
        Hang,
    }

    struct Fake {
        stream: Spec,
        groups: Spec,
        pending: Spec,
    }

    async fn reply(r: &Spec) -> redis::RedisResult<Value> {
        match r {
            Spec::Ok(v) => Ok(v.clone()),
            Spec::Server(text) => Err(server_error(text)),
            Spec::Io => Err(redis::RedisError::from(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"))),
            Spec::Hang => std::future::pending().await,
        }
    }

    impl SampleCalls for Fake {
        async fn stream_info(&mut self) -> redis::RedisResult<Value> {
            reply(&self.stream).await
        }

        async fn groups(&mut self) -> redis::RedisResult<Value> {
            reply(&self.groups).await
        }

        async fn pending(&mut self) -> redis::RedisResult<Value> {
            reply(&self.pending).await
        }
    }

    fn rendered(handle: &metrics_exporter_prometheus::PrometheusHandle, name: &str, labels: &[&str]) -> Option<f64> {
        crate::test_support::series(&handle.render(), name, labels)
    }

    /// Firehose throughput §4.5: the sampler's classes of reply.
    #[tokio::test(start_paused = true)]
    async fn sample_once_classifies_errors() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let cfg = FirehoseConfig::default();
        let calls = |call: &str| format!("call=\"{call}\"");
        let total = |h: &metrics_exporter_prometheus::PrometheusHandle, name: &str, call: &str| {
            rendered(h, name, &[&calls(call)]).unwrap_or(0.0)
        };
        register_series();
        metrics::gauge!(METRIC_LAG).set(7.0);

        // An I/O error on all three: nothing changes but the counters.
        let mut state = SamplerState::default();
        let mut fake = Fake { stream: Spec::Io, groups: Spec::Io, pending: Spec::Io };
        sample_once(&mut fake, &cfg, &mut state).await;
        assert_eq!(rendered(&handle, METRIC_GROUP_PRESENT, &[]), Some(1.0));
        assert_eq!(rendered(&handle, METRIC_LAG, &[]), Some(7.0));
        for call in ["stream", "groups", "pending"] {
            assert_eq!(total(&handle, METRIC_SAMPLE_CALLS, call), 1.0, "{call}");
            assert_eq!(total(&handle, METRIC_SAMPLE_FAILURES, call), 1.0, "{call}");
        }

        // A hang: each call fails after its timeout, and the tick completes.
        let mut hung = Fake { stream: Spec::Hang, groups: Spec::Hang, pending: Spec::Hang };
        let started = tokio::time::Instant::now();
        sample_once(&mut hung, &cfg, &mut state).await;
        assert_eq!(started.elapsed(), SAMPLE_CALL_TIMEOUT * 3);
        for call in ["stream", "groups", "pending"] {
            assert_eq!(total(&handle, METRIC_SAMPLE_FAILURES, call), total(&handle, METRIC_SAMPLE_CALLS, call), "{call}");
        }

        // `ERR no such key script: …` is a missing key; `NOGROUP` from
        // XPENDING is not a failure and sets pending to 0.
        metrics::gauge!(METRIC_PENDING).set(5.0);
        let before = total(&handle, METRIC_SAMPLE_FAILURES, "groups");
        let mut missing = Fake {
            stream: Spec::Ok(Value::Array(vec![bulk("length"), Value::Int(42), bulk("entries-added"), Value::Int(50)])),
            groups: Spec::Server("ERR no such key script: abc, on @user_script:3."),
            pending: Spec::Server("NOGROUP No such key 'x' or consumer group 'g'"),
        };
        sample_once(&mut missing, &cfg, &mut state).await;
        assert_eq!(total(&handle, METRIC_SAMPLE_FAILURES, "groups"), before);
        assert_eq!(rendered(&handle, METRIC_GROUP_PRESENT, &[]), Some(0.0));
        assert_eq!(rendered(&handle, METRIC_LAG, &[]), Some(42.0));
        assert_eq!(rendered(&handle, METRIC_PENDING, &[]), Some(0.0));

        // Call 1 fails and call 2 reports the group missing: the group goes
        // to 0, the lag gauge is left as it was.
        metrics::gauge!(METRIC_GROUP_PRESENT).set(1.0);
        let mut half = Fake { stream: Spec::Io, groups: Spec::Ok(Value::Array(vec![])), pending: Spec::Io };
        sample_once(&mut half, &cfg, &mut state).await;
        assert_eq!(rendered(&handle, METRIC_GROUP_PRESENT, &[]), Some(0.0));
        assert_eq!(rendered(&handle, METRIC_LAG, &[]), Some(42.0));
    }
}
