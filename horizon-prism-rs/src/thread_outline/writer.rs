//! The outline writer (thread outline design §3.2–§3.6): one task owns every
//! outline write.
//!
//! - It holds `pg_try_advisory_lock` on a dedicated `PgConnection`, outside
//!   the pool, and runs every flush on it, so a lost lock fails the flush
//!   rather than racing it (R3-12, R4-3).
//! - It flushes every `flush_ms`, or at [`FLUSH_OPS`] ops, in one
//!   transaction with its progress.
//! - After a bump of the epoch it discards every message until a `Resume`
//!   carrying the current epoch (R5-3), and on that `Resume` replays from
//!   the stream what it missed (§3.3).
//! - While inactive it drops what is queued, bumps the epoch and retries
//!   the lock every minute (§3.6).

use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::{Script, Value};
use sqlx::{Connection, PgConnection, PgPool};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::flush::{self, Committed, FlushError, Progress};
use super::id::StreamId;
use super::ops::{self, HoleReason, MissedReason, Op};
use super::{METRIC_HOLES, METRIC_MISSED, METRIC_QUEUE_OPS, METRIC_REPLAYS, METRIC_THROUGH_AGE, Msg, Outline, Why};
use crate::firehose::stream::{StreamEntry, parse_entry};
use crate::firehose::watermark::Watermarks;

/// The outline's advisory lock, in the style of the crate's other keys.
pub const LOCK_SQL: &str = "SELECT pg_try_advisory_lock(hashtextextended('outline:writer', 0))";
/// A flush runs at this many buffered ops, whatever `flush_ms` says.
pub const FLUSH_OPS: usize = 2000;
/// Entries per replay chunk (§3.3 step 3).
pub const REPLAY_CHUNK: u32 = 250;
/// How often an inactive writer retries the lock (§3.6).
pub const LOCK_RETRY: Duration = Duration::from_secs(60);
/// Attempts of one flush or chunk read before it gives up and bumps the
/// epoch, so the next `Resume` replays what it held.
pub const ATTEMPTS: u32 = 6;
const BACKOFF_MIN: Duration = Duration::from_millis(100);

static OUTLINE_RANGE: std::sync::LazyLock<Script> = std::sync::LazyLock::new(|| Script::new(include_str!("outline_range.lua")));

/// Why a write stopped.
#[derive(Debug)]
enum Stop {
    /// The lock connection is gone, so the lock is too.
    LockLost,
    /// Transient failures outlasted [`ATTEMPTS`].
    GaveUp,
}

/// The writer task.
pub struct Writer {
    outline: Outline,
    pool: PgPool,
    redis: ConnectionManager,
    stream_key: String,
    watermarks: Watermarks,
    rx: mpsc::Receiver<Msg>,
    /// The lock connection, while active.
    conn: Option<PgConnection>,
    /// Ops not yet flushed, in stream order.
    batch: Vec<(StreamId, Op)>,
    /// The highest `Through` not yet flushed.
    pending_through: Option<StreamId>,
    /// The stored `through`, as this writer last committed or read it.
    through: Option<StreamId>,
    /// The `through` this writer last committed (R5-2).
    last_committed: Option<StreamId>,
    /// The first acquisition has happened.
    loaded: bool,
    /// The lock was found held elsewhere since the last acquisition.
    contended: bool,
    /// The next `Resume` is treated as trimmed (§3.6).
    trim_next: bool,
    /// A `Resume` with `reset` was received, stale or dropped ones
    /// included; cleared once a reset is committed (§3.3 step 4).
    reset: bool,
    lock_retry: Duration,
    chunk: u32,
    #[cfg(test)]
    db: String,
}

impl Writer {
    /// A writer for `outline`, reading messages from `rx`.
    #[must_use]
    pub fn new(outline: Outline, pool: PgPool, redis: ConnectionManager, stream_key: String, watermarks: Watermarks, rx: mpsc::Receiver<Msg>) -> Self {
        Self {
            #[cfg(test)]
            db: pool.connect_options().get_database().unwrap_or_default().to_string(),
            outline,
            pool,
            redis,
            stream_key,
            watermarks,
            rx,
            conn: None,
            batch: Vec::new(),
            pending_through: None,
            through: None,
            last_committed: None,
            loaded: false,
            contended: false,
            trim_next: false,
            reset: false,
            lock_retry: LOCK_RETRY,
            chunk: REPLAY_CHUNK,
        }
    }

    /// Set the lock's retry interval and the replay's chunk size (tests).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn tuned(mut self, lock_retry: Duration, chunk: u32) -> Self {
        self.lock_retry = lock_retry;
        self.chunk = chunk;
        self
    }

    /// Run until the channel closes.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(mut self) {
        super::register_series();
        self.acquire().await;
        let flush_every = Duration::from_millis(u64::from(self.outline.config().flush_ms.max(1)));
        let mut flush_tick = tokio::time::interval(flush_every);
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut clock = tokio::time::interval(Duration::from_secs(1));
        clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut retry = tokio::time::interval(self.lock_retry);
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        retry.reset();
        loop {
            if self.conn.is_none() {
                // Inactive: what is queued is dropped, but a reset it
                // carried is kept.
                while let Ok(msg) = self.rx.try_recv() {
                    self.latch(&msg);
                }
                tokio::select! {
                    _ = retry.tick() => self.acquire().await,
                    _ = clock.tick() => self.clock().await,
                    msg = self.rx.recv() => match msg {
                        Some(msg) => self.latch(&msg),
                        None => return,
                    },
                }
                continue;
            }
            tokio::select! {
                msg = self.rx.recv() => match msg {
                    Some(msg) => self.handle(msg).await,
                    None => {
                        let _ = self.flush().await;
                        return;
                    }
                },
                _ = flush_tick.tick() => {
                    let _ = self.flush().await;
                }
                _ = clock.tick() => self.clock().await,
            }
        }
    }

    /// Refresh Redis's clock, and the through-age gauge.
    async fn clock(&mut self) {
        if let Ok(ms) = super::redis_time_ms(&mut self.redis).await {
            self.outline.shared().set_redis_now_ms(ms);
            if let Some(t) = self.outline.through_ms() {
                #[allow(clippy::cast_precision_loss)]
                metrics::gauge!(METRIC_THROUGH_AGE).set((ms - t) as f64 / 1000.0);
            }
        }
        self.outline.freshness().record();
    }

    /// Try to take the lock on a fresh dedicated connection (§3.6).
    async fn acquire(&mut self) {
        let mut conn = match PgConnection::connect_with(&self.pool.connect_options()).await {
            Ok(c) => c,
            Err(error) => {
                tracing::warn!(event = "outline_lock_failed", %error, "thread outline: cannot open the lock connection; retrying");
                return;
            }
        };
        match sqlx::query_scalar::<_, bool>(LOCK_SQL).fetch_one(&mut conn).await {
            Ok(true) => {}
            Ok(false) => {
                self.contended = true;
                tracing::warn!(event = "outline_lock_held", "thread outline: the writer's lock is held elsewhere; retrying every minute");
                let _ = conn.close().await;
                return;
            }
            Err(error) => {
                tracing::warn!(event = "outline_lock_failed", %error, "thread outline: taking the lock failed; retrying");
                return;
            }
        }
        let stored = match flush::stored_through(&mut conn).await {
            Ok(t) => t,
            Err(error) => {
                tracing::warn!(event = "outline_lock_failed", %error, "thread outline: reading progress failed; retrying");
                return;
            }
        };
        let since: Option<i64> = sqlx::query_scalar("SELECT (value #>> '{}')::bigint FROM worker_state WHERE key = $1")
            .bind(super::KEY_COMPLETE_SINCE)
            .fetch_optional(&mut conn)
            .await
            .ok()
            .flatten();
        if let Some(ms) = since {
            self.outline.raise_complete_since(ms);
        }
        if let Ok(max) = sqlx::query_scalar::<_, i32>("SELECT coalesce(max(id), 0) FROM outline_actors").fetch_one(&mut conn).await {
            metrics::gauge!(super::METRIC_ACTOR_IDS).set(f64::from(max));
        }
        // R5-2: another writer may have taken entries meanwhile.
        if self.contended || (self.loaded && stored != self.last_committed) {
            self.trim_next = true;
        }
        // A `Resume` sent while inactive was dropped: the consumer's next
        // message is a `Resume` of a new epoch.
        self.outline.shared().bump_for(if self.loaded { Why::Inactive } else { Why::Startup });
        self.loaded = true;
        self.contended = false;
        self.through = stored;
        self.last_committed = stored;
        if let Some(t) = stored {
            self.outline.shared().set_through_ms(t.ms_i64());
        }
        self.conn = Some(conn);
        self.outline.shared().set_active(true);
        tracing::info!(event = "outline_writer_active", through = ?stored.map(|t| t.to_string()), trim_next = self.trim_next, "thread outline writer holds its lock");
    }

    /// Lose the lock: drop what is queued and want a `Resume` (§3.6).
    fn inactive(&mut self) {
        self.conn = None;
        self.batch.clear();
        self.pending_through = None;
        metrics::gauge!(METRIC_QUEUE_OPS).set(0.0);
        let shared = self.outline.shared();
        shared.set_active(false);
        shared.set_replaying(false);
        shared.bump_for(Why::Inactive);
        tracing::warn!(event = "outline_writer_inactive", "thread outline writer lost its lock; retrying every minute");
    }

    fn resuming(&self) -> bool {
        let shared = self.outline.shared();
        shared.want_epoch() != shared.resumed_epoch()
    }

    /// Keep a reset any `Resume` carries, whatever becomes of it.
    fn latch(&mut self, msg: &Msg) {
        if let Msg::Resume { reset: true, .. } = msg {
            self.reset = true;
        }
    }

    async fn handle(&mut self, msg: Msg) {
        #[cfg(test)]
        crate::test_support::hooks::pause(crate::test_support::hooks::OUTLINE_WRITER_PAUSE, &self.db).await;
        match msg {
            Msg::Commit { id, ops } => {
                if self.resuming() || self.skipped(id) {
                    return;
                }
                self.batch.extend(ops.into_iter().map(|op| (id, op)));
                #[allow(clippy::cast_precision_loss)]
                metrics::gauge!(METRIC_QUEUE_OPS).set(self.batch.len() as f64);
                if self.batch.len() >= FLUSH_OPS {
                    let _ = self.flush().await;
                }
            }
            Msg::Through(id) => {
                if self.resuming() {
                    return;
                }
                self.pending_through = self.pending_through.max(Some(id));
            }
            Msg::Resume { first_id, epoch, reset } => {
                self.reset |= reset;
                if epoch != self.outline.shared().want_epoch() {
                    return;
                }
                self.resume(first_id, epoch).await;
            }
        }
    }

    /// The skip rule: ops at or below `through` are repeats.
    fn skipped(&self, id: StreamId) -> bool {
        self.through.max(self.pending_through).is_some_and(|t| id <= t)
    }

    /// Flush what is buffered with the pending `Through`.
    async fn flush(&mut self) -> Result<(), Stop> {
        if self.pending_through <= self.through {
            self.pending_through = None;
        }
        if self.batch.is_empty() && self.pending_through.is_none() {
            return Ok(());
        }
        let ops = std::mem::take(&mut self.batch);
        let progress = Progress { through: self.pending_through.take(), ..Progress::default() };
        metrics::gauge!(METRIC_QUEUE_OPS).set(0.0);
        self.commit(ops, progress).await
    }

    /// Write `ops` and `progress`, retrying transient failures and
    /// re-flushing op by op on a deterministic one (§3.4). On a lost lock
    /// the writer goes inactive; when it gives up, it bumps the epoch.
    async fn commit(&mut self, ops: Vec<(StreamId, Op)>, mut progress: Progress) -> Result<(), Stop> {
        progress.complete_since_ms = self.outline.complete_since_ms();
        match self.with_retries(&ops, &progress).await {
            Ok(c) => {
                self.committed(c);
                Ok(())
            }
            Err(Ok(())) => self.one_by_one(ops, progress).await,
            Err(Err(stop)) => Err(self.stopped(stop)),
        }
    }

    fn stopped(&mut self, stop: Stop) -> Stop {
        match stop {
            Stop::LockLost => self.inactive(),
            Stop::GaveUp => {
                tracing::warn!(event = "outline_flush_gave_up", "thread outline: a write kept failing; its entries will be replayed");
                self.batch.clear();
                self.pending_through = None;
                self.outline.shared().bump_for(Why::Flush);
            }
        }
        stop
    }

    /// One write with retries: `Err(Ok(()))` on a deterministic failure.
    async fn with_retries(&mut self, ops: &[(StreamId, Op)], progress: &Progress) -> Result<Committed, Result<(), Stop>> {
        let mut backoff = BACKOFF_MIN;
        for attempt in 1..=ATTEMPTS {
            let Some(conn) = self.conn.as_mut() else {
                return Err(Err(Stop::LockLost));
            };
            match flush::apply(conn, ops, progress).await {
                Ok(c) => return Ok(c),
                Err(FlushError::Deterministic(error)) => {
                    tracing::warn!(event = "outline_flush_deterministic", %error, ops = ops.len(), "thread outline: a flush failed; writing its ops one by one");
                    return Err(Ok(()));
                }
                Err(FlushError::Transient(error)) => {
                    // A live session still holds the lock; a dead one lost it.
                    if sqlx::query("SELECT 1").execute(&mut *conn).await.is_err() {
                        return Err(Err(Stop::LockLost));
                    }
                    tracing::debug!(event = "outline_flush_transient", %error, attempt, "thread outline: a flush failed; retrying");
                    if attempt < ATTEMPTS {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                }
            }
        }
        Err(Err(Stop::GaveUp))
    }

    /// §3.4's deterministic path: each op on its own; one that fails is a
    /// hole at its parent, else its root, and counted.
    async fn one_by_one(&mut self, ops: Vec<(StreamId, Op)>, progress: Progress) -> Result<(), Stop> {
        for (id, op) in ops {
            let one = [(id, op)];
            match self.with_retries(&one, &Progress::default()).await {
                Ok(_) => {}
                Err(Ok(())) => {
                    let [(id, op)] = one;
                    metrics::counter!(METRIC_MISSED, "reason" => MissedReason::Flush.as_str()).increment(1);
                    let node = match &op {
                        Op::Create(p) | Op::Update(p) => p.hole_node(),
                        _ => None,
                    };
                    if let Some(node) = node {
                        let hole = [(id, Op::Hole { node, reason: HoleReason::Flush })];
                        match self.with_retries(&hole, &Progress::default()).await {
                            Ok(_) | Err(Ok(())) => {}
                            Err(Err(stop)) => return Err(self.stopped(stop)),
                        }
                    }
                }
                Err(Err(stop)) => return Err(self.stopped(stop)),
            }
        }
        match self.with_retries(&[], &progress).await {
            Ok(c) => {
                self.committed(c);
                Ok(())
            }
            Err(Ok(())) => Ok(()),
            Err(Err(stop)) => Err(self.stopped(stop)),
        }
    }

    fn committed(&mut self, c: Committed) {
        if let Some(t) = c.through {
            self.through = Some(t);
            self.last_committed = Some(t);
            self.outline.shared().set_through_ms(t.ms_i64());
        }
        if let Some(ms) = c.complete_since_ms {
            self.outline.raise_complete_since(ms);
        }
    }

    /// A `Resume` of the current epoch: the only hand-back point (§3.3).
    async fn resume(&mut self, first: StreamId, epoch: u64) {
        // What came before it is written first.
        if self.flush().await.is_err() {
            return;
        }
        let why = self.outline.shared().last_why();
        metrics::counter!(METRIC_REPLAYS, "why" => why.as_str()).increment(1);
        let shared = self.outline.shared();
        shared.set_replaying(true);
        shared.set_resumed(epoch);
        let before = first.prev();
        let rebase = Progress { rebase: Some(before), ..Progress::default() };
        let result = match self.through {
            // Step 1: first switch-on, or after switching off. Nothing is
            // replayed, so a pending reset or trim is moot.
            None => {
                self.outline.raise_complete_since(first.ms_i64());
                let r = self.commit(Vec::new(), Progress { through: Some(before), ..Progress::default() }).await;
                if r.is_ok() {
                    self.reset = false;
                    self.trim_next = false;
                }
                r
            }
            // Step 4, and a contended lock (§3.6): as trimmed, with
            // `through` rebased to `first_id − 1`. The flags clear only once
            // that is committed.
            Some(_) if self.reset || self.trim_next => {
                let r = self.trimmed(first, rebase).await;
                if r.is_ok() {
                    self.reset = false;
                    self.trim_next = false;
                }
                r
            }
            // Step 2: nothing to replay; the skip rule drops the repeats.
            Some(t) if first <= t.next() => Ok(()),
            // Step 3.
            Some(t) => self.replay(t, first).await,
        };
        if result.is_ok() {
            tracing::info!(event = "outline_resumed", first_id = %first, why = why.as_str(), through = ?self.through.map(|t| t.to_string()), "thread outline resumed");
        }
        self.outline.shared().set_replaying(false);
    }

    /// The trimmed case: coverage begins again at `first` (no hole row:
    /// the hole is time-wide, N4).
    async fn trimmed(&mut self, first: StreamId, progress: Progress) -> Result<(), Stop> {
        metrics::counter!(METRIC_HOLES, "reason" => HoleReason::Trimmed.as_str()).increment(1);
        tracing::warn!(event = "outline_trimmed", first_id = %first, "thread outline: entries it missed are gone; coverage starts again");
        self.outline.raise_complete_since(first.ms_i64());
        self.commit(Vec::new(), progress).await
    }

    /// Replay `(from, first)` in chunks read with the stream's first
    /// retained id (§3.3 step 3, R4-1).
    async fn replay(&mut self, from: StreamId, first: StreamId) -> Result<(), Stop> {
        let mut start = from;
        loop {
            let (head, entries) = self.read_chunk(start, first).await?;
            if head.is_none_or(|h| h > start) {
                // The rest of the range is trimmed.
                return self.trimmed(first, Progress { through: Some(first.prev()), ..Progress::default() }).await;
            }
            let Some(last) = entries.last().and_then(|e| StreamId::parse(&e.id)) else {
                break;
            };
            let full = entries.len() >= usize::try_from(self.chunk).unwrap_or(usize::MAX);
            let relay_key = |relay: Option<&str>| ops::status_relay(&self.watermarks, relay);
            let mut ops: Vec<(StreamId, Op)> = Vec::new();
            for entry in &entries {
                let Some(id) = StreamId::parse(&entry.id) else {
                    continue;
                };
                let now = DateTime::from_timestamp_millis(id.ms_i64()).unwrap_or_else(Utc::now);
                ops.extend(ops::entry_ops(entry, now, &relay_key).into_iter().map(|op| (id, op)));
            }
            self.commit(ops, Progress { through: Some(last), ..Progress::default() }).await?;
            start = last;
            #[cfg(test)]
            crate::test_support::hooks::pause(crate::test_support::hooks::OUTLINE_REPLAY_BETWEEN_CHUNKS, &self.db).await;
            if !full {
                break;
            }
        }
        self.commit(Vec::new(), Progress { through: Some(first.prev()), ..Progress::default() }).await
    }

    /// One chunk and the stream's first retained id, retried on failure.
    async fn read_chunk(&mut self, start: StreamId, end: StreamId) -> Result<(Option<StreamId>, Vec<StreamEntry>), Stop> {
        let mut backoff = BACKOFF_MIN;
        for attempt in 1..=ATTEMPTS {
            let reply: Result<Value, redis::RedisError> = OUTLINE_RANGE
                .key(&self.stream_key)
                .arg(start.to_string())
                .arg(end.to_string())
                .arg(self.chunk)
                .invoke_async(&mut self.redis)
                .await;
            match reply.map_err(|e| e.to_string()).and_then(parse_range) {
                Ok(r) => return Ok(r),
                Err(error) => {
                    tracing::debug!(event = "outline_replay_read_failed", %error, attempt, "thread outline: a replay read failed; retrying");
                    if attempt < ATTEMPTS {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                }
            }
        }
        Err(self.stopped(Stop::GaveUp))
    }
}

fn parse_range(reply: Value) -> Result<(Option<StreamId>, Vec<StreamEntry>), String> {
    let Value::Array(items) = reply else {
        return Err(format!("outline_range reply: {reply:?}"));
    };
    let mut it = items.into_iter();
    let head = it.next().as_ref().and_then(crate::firehose::scripts::string).and_then(|s| StreamId::parse(&s));
    let entries = match it.next() {
        None | Some(Value::Nil) => Vec::new(),
        Some(Value::Array(list)) => list.into_iter().map(parse_entry).collect::<Result<Vec<_>, _>>()?,
        Some(other) => return Err(format!("outline_range entries: {other:?}")),
    };
    Ok((head, entries))
}

/// Start the outline: its handle, the consumer's sender, and the writer to
/// spawn. `redis` is a shared connection: the writer's reads never block.
#[must_use]
pub fn start(
    cfg: crate::config::ThreadOutlineConfig,
    pool: PgPool,
    redis: ConnectionManager,
    stream_key: String,
    watermarks: Watermarks,
) -> (Outline, super::Sender, Writer) {
    let outline = Outline::new(cfg);
    let (sender, rx) = outline.channel();
    let writer = Writer::new(outline.clone(), pool, redis, stream_key, watermarks, rx);
    (outline, sender, writer)
}

impl Writer {
    /// The shared handle.
    #[must_use]
    pub fn outline(&self) -> &Outline {
        &self.outline
    }
}
