//! The thread outline index: a
//! table of every public post the firehose delivers, with its author, record
//! key, parent, root, time and rev, and a mark when deleted. No text.
//!
//! - **Written** by the firehose consumer only, for every DID. The consumer
//!   stamps each commit's outline ops with its stream id and hands them to
//!   one writer task over a bounded channel; it never waits (§3.1).
//! - **The writer** owns every outline write. It flushes in one transaction
//!   on a dedicated connection that holds the outline's advisory lock, and
//!   replays from the stream what it missed (§3.2, §3.3, §3.6).
//! - **Coverage**: `complete_since` is the stream time from which the
//!   outline holds every post the consumer applied; `through` is the stream
//!   position it has written through (§3.3).
//! - **Read** only by Thread Lens, through [`Outline`]: whether it is fresh,
//!   whether a thread is complete, and which authors are gone (S2).

pub mod flush;
pub mod id;
pub mod ops;
pub mod retention;
pub mod writer;

#[cfg(test)]
pub(crate) mod tests;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::{PgConnection, PgPool};
use tokio::sync::mpsc;

use self::id::StreamId;
use self::ops::Op;
use crate::config::ThreadOutlineConfig;
use crate::firehose::gap::GapCause;

/// `worker_state` key: the stream id the outline is written through, as
/// `"ms-seq"`.
pub const KEY_THROUGH: &str = "outline:through";
/// `worker_state` key: the stream ms from which the outline is complete.
pub const KEY_COMPLETE_SINCE: &str = "outline:complete_since";

pub const METRIC_OPS: &str = "outline_ops_total";
pub const METRIC_HOLES: &str = "outline_holes_total";
pub const METRIC_MISSED: &str = "outline_missed_total";
pub const METRIC_REPLAYS: &str = "outline_replays_total";
pub const METRIC_FLUSH_SECONDS: &str = "outline_flush_seconds";
pub const METRIC_QUEUE_OPS: &str = "outline_queue_ops";
pub const METRIC_ACTOR_IDS: &str = "outline_actor_ids";
pub const METRIC_WRITER_ACTIVE: &str = "outline_writer_active";
/// Seconds since the epoch.
pub const METRIC_COMPLETE_SINCE: &str = "outline_complete_since";
pub const METRIC_THROUGH_AGE: &str = "outline_through_age_seconds";
/// 1 for the outline's current [`Freshness`] state, 0 for the others;
/// set every second by the writer.
pub const METRIC_FRESHNESS: &str = "outline_freshness";

/// Commits a relay's liveness median is taken over (R5-4).
pub const LIVENESS_RING: usize = 100;

/// Why the writer awaits a `Resume` (`outline_replays_total{why}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Why {
    /// The process started.
    Startup = 0,
    /// The consumer found the channel full.
    Lag = 1,
    /// The writer lost its lock, or found it held elsewhere.
    Inactive = 2,
    /// A flush or replay gave up.
    Flush = 3,
    /// The consumer's group was recreated: a new stream.
    Reset = 4,
}

impl Why {
    pub const ALL: [Self; 5] = [Self::Startup, Self::Lag, Self::Inactive, Self::Flush, Self::Reset];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Lag => "lag",
            Self::Inactive => "inactive",
            Self::Flush => "flush",
            Self::Reset => "reset",
        }
    }

    fn from_u8(v: u8) -> Self {
        Self::ALL.into_iter().find(|w| *w as u8 == v).unwrap_or(Self::Startup)
    }
}

/// Whether Thread Lens may use the outline now (§3.3 "Freshness"), or
/// the first reason it may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    /// The writer holds no lock (lost, or held elsewhere).
    Inactive,
    /// A `Resume` is awaited: the epochs differ.
    Resuming,
    /// A replay is running.
    Replaying,
    /// Nothing is written through yet, or `through` is more than
    /// `fresh_secs` behind Redis's clock.
    Behind,
    /// No relay's commits are live: a sidecar may be catching up.
    NoLiveRelay,
}

impl Freshness {
    pub const ALL: [Self; 6] = [Self::Fresh, Self::Inactive, Self::Resuming, Self::Replaying, Self::Behind, Self::NoLiveRelay];

    #[must_use]
    pub fn is_fresh(self) -> bool {
        self == Self::Fresh
    }

    /// The state's label in `outline_freshness{state}`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Inactive => "inactive",
            Self::Resuming => "resuming",
            Self::Replaying => "replaying",
            Self::Behind => "behind",
            Self::NoLiveRelay => "no_live_relay",
        }
    }

    /// Set `outline_freshness{state}`: 1 for this state, 0 for the others.
    pub fn record(self) {
        for state in Self::ALL {
            metrics::gauge!(METRIC_FRESHNESS, "state" => state.as_str()).set(if state == self { 1.0 } else { 0.0 });
        }
    }
}

/// State the consumer, the writer and readers share.
#[derive(Debug)]
pub(crate) struct Shared {
    /// The epoch the writer wants resumed (R5-3). Starts at 1, so the
    /// consumer's first message is a `Resume`.
    want_epoch: AtomicU64,
    /// The epoch the writer last resumed at.
    resumed_epoch: AtomicU64,
    /// What the last bump was for.
    why: AtomicU8,
    /// The next `Resume` follows a stream reset.
    reset: AtomicBool,
    replaying: AtomicBool,
    active: AtomicBool,
    /// 0 while nothing is written through.
    through_ms: AtomicU64,
    /// 0 while there is no coverage.
    complete_since_ms: AtomicU64,
    /// The writer's last Redis `TIME`.
    redis_now_ms: AtomicU64,
    /// Per relay label, the last [`LIVENESS_RING`] commits'
    /// `min(stream ms, commit.time)`.
    rings: Mutex<HashMap<String, VecDeque<i64>>>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            want_epoch: AtomicU64::new(1),
            resumed_epoch: AtomicU64::new(0),
            why: AtomicU8::new(Why::Startup as u8),
            reset: AtomicBool::new(false),
            replaying: AtomicBool::new(false),
            active: AtomicBool::new(false),
            through_ms: AtomicU64::new(0),
            complete_since_ms: AtomicU64::new(0),
            redis_now_ms: AtomicU64::new(0),
            rings: Mutex::default(),
        }
    }
}

impl Shared {
    /// Want a `Resume` of a new epoch (R5-3).
    pub(crate) fn bump_for(&self, why: Why) {
        self.why.store(why as u8, Ordering::Relaxed);
        self.want_epoch.fetch_add(1, Ordering::AcqRel);
    }

    fn want(&self) -> u64 {
        self.want_epoch.load(Ordering::Acquire)
    }

    fn resumed(&self) -> u64 {
        self.resumed_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn want_epoch(&self) -> u64 {
        self.want()
    }

    pub(crate) fn last_why(&self) -> Why {
        Why::from_u8(self.why.load(Ordering::Relaxed))
    }

    pub(crate) fn set_resumed(&self, epoch: u64) {
        self.resumed_epoch.store(epoch, Ordering::Release);
    }

    pub(crate) fn resumed_epoch(&self) -> u64 {
        self.resumed()
    }

    pub(crate) fn set_replaying(&self, v: bool) {
        self.replaying.store(v, Ordering::Release);
    }

    pub(crate) fn set_active(&self, v: bool) {
        self.active.store(v, Ordering::Release);
        metrics::gauge!(METRIC_WRITER_ACTIVE).set(if v { 1.0 } else { 0.0 });
    }

    pub(crate) fn set_through_ms(&self, ms: i64) {
        self.through_ms.store(u64::try_from(ms).unwrap_or(0), Ordering::Release);
    }

    pub(crate) fn set_redis_now_ms(&self, ms: i64) {
        self.redis_now_ms.store(u64::try_from(ms).unwrap_or(0), Ordering::Relaxed);
    }
}

/// The outline's handle: what Thread Lens reads (S2), and what the consumer
/// and the writer share.
#[derive(Debug, Clone)]
pub struct Outline {
    shared: Arc<Shared>,
    cfg: Arc<ThreadOutlineConfig>,
}

/// The facts about a thread's root that decide whether the thread is
/// complete (§3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootFacts {
    /// The root row's `seen_at`.
    pub seen_at: DateTime<Utc>,
    /// The root's decoded TID rkey.
    pub rkey: i64,
}

impl Outline {
    #[must_use]
    pub fn new(cfg: ThreadOutlineConfig) -> Self {
        Self { shared: Arc::default(), cfg: Arc::new(cfg) }
    }

    #[must_use]
    pub fn config(&self) -> &ThreadOutlineConfig {
        &self.cfg
    }

    pub(crate) fn shared(&self) -> &Shared {
        &self.shared
    }

    /// Whether the outline may be read now (§3.3 "Freshness"), with Redis's
    /// clock as the writer last read it.
    #[must_use]
    pub fn freshness(&self) -> Freshness {
        let now = match self.shared.redis_now_ms.load(Ordering::Relaxed) {
            0 => Utc::now().timestamp_millis(),
            ms => i64::try_from(ms).unwrap_or(i64::MAX),
        };
        self.freshness_at(now)
    }

    /// [`Outline::freshness`] at `now_ms` on Redis's clock.
    #[must_use]
    pub fn freshness_at(&self, now_ms: i64) -> Freshness {
        let s = &self.shared;
        if !s.active.load(Ordering::Acquire) {
            return Freshness::Inactive;
        }
        if s.want() != s.resumed() {
            return Freshness::Resuming;
        }
        if s.replaying.load(Ordering::Acquire) {
            return Freshness::Replaying;
        }
        let fresh_ms = i64::from(self.cfg.fresh_secs) * 1000;
        match self.through_ms() {
            Some(t) if now_ms - t <= fresh_ms => {}
            _ => return Freshness::Behind,
        }
        if !self.any_relay_live(now_ms, fresh_ms) {
            return Freshness::NoLiveRelay;
        }
        Freshness::Fresh
    }

    /// Whether some relay's median commit time, over its last
    /// [`LIVENESS_RING`] commits, is within `fresh_ms` of `now_ms` (R5-4). A
    /// median cannot be lifted by one PDS whose clock runs ahead.
    fn any_relay_live(&self, now_ms: i64, fresh_ms: i64) -> bool {
        let rings = self.shared.rings.lock().unwrap_or_else(PoisonError::into_inner);
        rings.values().any(|ring| {
            let mut v: Vec<i64> = ring.iter().copied().collect();
            if v.is_empty() {
                return false;
            }
            v.sort_unstable();
            (now_ms - v[v.len() / 2]).abs() <= fresh_ms
        })
    }

    /// Record one commit of relay `label`: `min(stream ms, commit.time)`.
    pub fn observe_relay(&self, label: &str, ms: i64) {
        let mut rings = self.shared.rings.lock().unwrap_or_else(PoisonError::into_inner);
        if !rings.contains_key(label) {
            rings.insert(label.to_string(), VecDeque::new());
        }
        let Some(ring) = rings.get_mut(label) else {
            return;
        };
        if ring.len() == LIVENESS_RING {
            ring.pop_front();
        }
        ring.push_back(ms);
    }

    /// The stream ms from which the outline holds every post the consumer
    /// applied; `None` before coverage began.
    #[must_use]
    pub fn complete_since_ms(&self) -> Option<i64> {
        nonzero(self.shared.complete_since_ms.load(Ordering::Acquire))
    }

    /// The stream ms the outline is written through, as the writer last
    /// committed it; `None` before anything was.
    #[must_use]
    pub fn through_ms(&self) -> Option<i64> {
        nonzero(self.shared.through_ms.load(Ordering::Acquire))
    }

    /// Whether the writer holds the outline's lock.
    #[must_use]
    pub fn active(&self) -> bool {
        self.shared.active.load(Ordering::Acquire)
    }

    /// Move `complete_since` forward to `ms` in memory (`fetch_max`, N3): it
    /// never moves back.
    pub fn raise_complete_since(&self, ms: i64) {
        let Ok(ms) = u64::try_from(ms) else {
            return;
        };
        let prev = self.shared.complete_since_ms.fetch_max(ms, Ordering::AcqRel);
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_COMPLETE_SINCE).set(prev.max(ms) as f64 / 1000.0);
    }

    /// Whether a thread with this root is complete at `now` (§3.3): it was
    /// seen at least `settle` after coverage began, its TID time is no more
    /// than `settle` before coverage began, and it was seen within the
    /// window less a day. The root having a row is the caller's to check.
    #[must_use]
    pub fn thread_complete(&self, root: RootFacts, now: DateTime<Utc>) -> bool {
        let Some(since) = self.complete_since_ms().and_then(DateTime::from_timestamp_millis) else {
            return false;
        };
        let settle = TimeDelta::seconds(i64::from(self.cfg.settle_secs));
        let Some(tid_at) = ops::tid_time(root.rkey) else {
            return false;
        };
        let window_edge = now - TimeDelta::days(i64::from(self.cfg.window_days)) + TimeDelta::days(1);
        root.seen_at >= since + settle && tid_at >= since - settle && root.seen_at >= window_edge
    }

    /// The consumer's half of the channel, and the writer's.
    #[must_use]
    pub fn channel(&self) -> (Sender, mpsc::Receiver<Msg>) {
        let cap = usize::try_from(self.cfg.queue_messages).unwrap_or(1).max(1);
        let (tx, rx) = mpsc::channel(cap);
        (Sender { tx, outline: self.clone(), resumed: 0 }, rx)
    }
}

fn nonzero(v: u64) -> Option<i64> {
    (v != 0).then(|| i64::try_from(v).unwrap_or(i64::MAX))
}

/// One message from the consumer to the writer (§3.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    /// One commit's outline ops, all of them (N2).
    Commit { id: StreamId, ops: Vec<Op> },
    /// The consumer has processed every entry through `id`.
    Through(StreamId),
    /// The consumer is about to process `first_id`, under `epoch`.
    Resume { first_id: StreamId, epoch: u64, reset: bool },
}

/// The consumer's half: it never waits (§3.1, R4-9, R5-3).
#[derive(Debug)]
pub struct Sender {
    tx: mpsc::Sender<Msg>,
    outline: Outline,
    /// The epoch this consumer last sent a `Resume` for.
    resumed: u64,
}

impl Sender {
    /// The shared handle.
    #[must_use]
    pub fn outline(&self) -> &Outline {
        &self.outline
    }

    fn sending(&self) -> bool {
        self.outline.shared.want() == self.resumed
    }

    /// Between entries: when the writer wants a `Resume` and the channel
    /// has room, send `Resume { first_id }` for the entry about to be
    /// processed.
    pub fn before_entry(&mut self, id: &str) {
        let shared = &self.outline.shared;
        let want = shared.want();
        if want == self.resumed || self.tx.capacity() == 0 {
            return;
        }
        let Some(first_id) = StreamId::parse(id) else {
            return;
        };
        let reset = shared.reset.swap(false, Ordering::AcqRel);
        match self.tx.try_send(Msg::Resume { first_id, epoch: want, reset }) {
            Ok(()) => self.resumed = want,
            Err(_) => {
                if reset {
                    shared.reset.store(true, Ordering::Release);
                }
            }
        }
    }

    /// Send one commit's ops in one message, unless a `Resume` is owed; a
    /// full channel stops sending and bumps the epoch.
    pub fn commit(&mut self, id: &str, ops: Vec<Op>) {
        if ops.is_empty() || !self.sending() {
            return;
        }
        let Some(id) = StreamId::parse(id) else {
            return;
        };
        self.send(Msg::Commit { id, ops });
    }

    /// After a batch: the consumer has processed every entry through `id`.
    pub fn through(&mut self, id: &str) {
        if !self.sending() {
            return;
        }
        if let Some(id) = StreamId::parse(id) {
            self.send(Msg::Through(id));
        }
    }

    fn send(&mut self, msg: Msg) {
        if self.tx.try_send(msg).is_err() {
            self.outline.shared.bump_for(Why::Lag);
        }
    }

    /// The group was recreated (a new stream may begin below `through`):
    /// the next `Resume` rebases `through` (§3.3 step 4).
    pub fn reset_stream(&self) {
        self.outline.shared.reset.store(true, Ordering::Release);
        self.outline.shared.bump_for(Why::Reset);
    }
}

/// The value a gap of `cause` writes into `complete_since` (§3.3 "Firehose
/// gap", R3-4): a trim, the ms of the stream's last generated id; pending,
/// interrupted-claim and recreation gaps, Redis's `TIME`; an operator gap
/// loses nothing and writes none.
#[must_use]
pub fn gap_value(cause: GapCause, last_generated_id: Option<&str>, redis_now_ms: Option<i64>) -> Option<i64> {
    match cause {
        GapCause::Trim => last_generated_id.and_then(StreamId::parse).map(StreamId::ms_i64),
        GapCause::Pending | GapCause::InterruptedClaim | GapCause::GroupRecreated => redis_now_ms,
        GapCause::Operator => None,
    }
}

/// Redis's clock, in ms.
///
/// # Errors
///
/// Any Redis error, or a reply that is not `TIME`'s.
pub async fn redis_time_ms(conn: &mut redis::aio::ConnectionManager) -> Result<i64, redis::RedisError> {
    let (secs, micros): (i64, i64) = redis::cmd("TIME").query_async(conn).await?;
    Ok(secs * 1000 + micros / 1000)
}

/// `complete_since = GREATEST(stored, ms)` on `conn`, the gap transaction's
/// own or the writer's; the stored value after.
///
/// # Errors
///
/// Any database error.
pub async fn raise_stored_complete_since(conn: &mut PgConnection, ms: i64) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb($2::bigint), now()) \
         ON CONFLICT (key) DO UPDATE SET value = to_jsonb(GREATEST((worker_state.value #>> '{}')::bigint, $2::bigint)), updated_at = now() \
         RETURNING (value #>> '{}')::bigint",
    )
    .bind(KEY_COMPLETE_SINCE)
    .bind(ms)
    .fetch_one(conn)
    .await
}

/// The stored `through` and `complete_since`, for the startup log line.
///
/// # Errors
///
/// Any database error.
pub async fn stored_progress(pool: &PgPool) -> Result<(Option<StreamId>, Option<i64>), sqlx::Error> {
    let through: Option<String> = sqlx::query_scalar("SELECT value #>> '{}' FROM worker_state WHERE key = $1")
        .bind(KEY_THROUGH)
        .fetch_optional(pool)
        .await?;
    let since: Option<i64> = sqlx::query_scalar("SELECT (value #>> '{}')::bigint FROM worker_state WHERE key = $1")
        .bind(KEY_COMPLETE_SINCE)
        .fetch_optional(pool)
        .await?;
    Ok((through.as_deref().and_then(StreamId::parse), since))
}

/// Switching off (§3.3 step 1): `through` is deleted and `complete_since`
/// cleared, so switching on again starts coverage afresh.
///
/// # Errors
///
/// Any database error.
pub async fn switch_off(pool: &PgPool) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query("DELETE FROM worker_state WHERE key IN ($1, $2)")
        .bind(KEY_THROUGH)
        .bind(KEY_COMPLETE_SINCE)
        .execute(pool)
        .await?
        .rows_affected())
}

/// The gone set among `dids` (R4-2, R5-1): an author is gone when at least
/// one relay has reported and every relay that has reported says inactive.
/// Thread Lens uses it only to keep a gone author's posts out of the gap
/// nodes it emits without a fetch (§5.3).
///
/// # Errors
///
/// Any database error.
pub async fn gone_dids(pool: &PgPool, dids: &[String]) -> Result<HashSet<String>, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT a.did FROM outline_gone g JOIN outline_actors a ON a.id = g.author \
         WHERE a.did = ANY($1) GROUP BY a.did HAVING bool_and(NOT g.active)",
    )
    .bind(dids)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Every series of the outline's metrics at zero, so a rule over them has
/// data from the start.
pub fn register_series() {
    for op in ops::OP_LABELS {
        metrics::counter!(METRIC_OPS, "op" => op).increment(0);
    }
    for reason in ops::HoleReason::ALL {
        metrics::counter!(METRIC_HOLES, "reason" => reason.as_str()).increment(0);
    }
    for reason in ops::MissedReason::ALL {
        metrics::counter!(METRIC_MISSED, "reason" => reason.as_str()).increment(0);
    }
    for why in Why::ALL {
        metrics::counter!(METRIC_REPLAYS, "why" => why.as_str()).increment(0);
    }
    metrics::gauge!(METRIC_QUEUE_OPS).set(0.0);
    metrics::gauge!(METRIC_WRITER_ACTIVE).set(0.0);
    Freshness::Inactive.record();
}
