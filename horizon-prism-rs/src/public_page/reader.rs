//! The live feed's reader (public-page design §6.2): its own plain reads of
//! the firehose stream, apart from any of Prism's work.
//!
//! - **Anchor.** `XINFO STREAM` and its `last-generated-id`, never `$`
//!   (re-read at each call, so an entry added between two calls would be
//!   lost). A missing key is an empty stream, anchored at `0-0`.
//! - **Poll.** Every 50 ms on its own grid, one non-blocking
//!   `XREAD COUNT 1000` from the last id read, at once again while a reply
//!   is full; never `BLOCK`. Each id is read once, in stream order.
//! - **Every call** has a 2 s timeout. A timeout or an error backs off
//!   (1 s, doubling to 30 s) and keeps the id: the entries are still in the
//!   stream.
//! - **Once a second** `XINFO STREAM` again: the input rate from
//!   `entries-added`, the lag behind the stream's own tail (so relay silence
//!   is never lag), and the three re-anchors, each sending one `skipped`:
//!   behind by over 30 s, unread entries trimmed, or the stream recreated.
//! - **Once a minute** Redis `TIME`, three times, keeping the offset of the
//!   shortest round trip: entry ids are Redis time, so release ticks are too.
//!
//! It runs on a connection of its own, uses no consumer group and writes
//! nothing to Redis. It reads nothing of Prism's state.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::Value;
use redis::aio::ConnectionManager;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::decode::{Decoded, Labels, decode};
use crate::firehose::stream::parse_read_reply;

/// `public_feed_undecodable_total`: entries that give no line and no count.
pub const METRIC_UNDECODABLE: &str = "public_feed_undecodable_total";
/// `public_feed_reader_lag_seconds`: the reader's lag behind the stream's
/// tail, or the time since it last completed a loop, whichever is larger.
pub const METRIC_LAG: &str = "public_feed_reader_lag_seconds";
/// Entries one `XREAD` asks for.
pub const COUNT: usize = 1000;

/// The reader's timings, shortened in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderSettings {
    pub poll: Duration,
    pub timeout: Duration,
    /// Lag past this re-anchors.
    pub behind: Duration,
    pub info_every: Duration,
    pub clock_every: Duration,
    pub backoff_first: Duration,
    pub backoff_max: Duration,
}

impl Default for ReaderSettings {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(50),
            timeout: Duration::from_secs(2),
            behind: Duration::from_secs(30),
            info_every: Duration::from_secs(1),
            clock_every: Duration::from_secs(60),
            backoff_first: Duration::from_secs(1),
            backoff_max: Duration::from_secs(30),
        }
    }
}

/// Why entries were skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Why {
    /// The reader fell behind (a trim included).
    Behind,
    /// The stream was recreated.
    Reset,
}

impl Why {
    #[must_use]
    pub fn slug(self) -> &'static str {
        match self {
            Self::Behind => "behind",
            Self::Reset => "reset",
        }
    }
}

/// A re-anchor: everything at or below `to_ms` that was not read is skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Skip {
    /// The new anchor's `last-generated-id` milliseconds.
    pub to_ms: i64,
    pub why: Why,
    /// The previous anchor's milliseconds.
    pub prev_anchor_ms: i64,
}

/// What the reader hands the publisher, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Msg {
    /// One poll's decoded entries.
    Entries(Vec<Decoded>),
    Skipped(Skip),
    /// An `entries-added` reading, at a local Unix-ms time.
    Input { at_ms: i64, entries_added: u64 },
    /// Redis time minus local time, in ms.
    Offset(i64),
}

/// What tests watch and hold.
#[derive(Debug, Default)]
pub struct Probe {
    /// While set, the reader waits before its next call.
    pub paused: AtomicBool,
    /// Notified after each poll that returned nothing.
    pub polled_empty: Notify,
}

/// Local Unix time, ms.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn local_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

/// A stream id as `(ms, seq)`.
#[must_use]
pub fn parse_id(id: &str) -> Option<(i64, i64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::BulkString(b) => String::from_utf8(b.clone()).ok(),
        Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

fn number(v: &Value) -> Option<u64> {
    match v {
        Value::Int(n) => u64::try_from(*n).ok(),
        other => text(other).and_then(|s| s.parse().ok()),
    }
}

/// The fields of an `XINFO STREAM` reply the reader uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamInfo {
    pub last_generated: (i64, i64),
    pub entries_added: u64,
    pub length: u64,
    pub first_entry: Option<(i64, i64)>,
}

/// Parse an `XINFO STREAM` reply (RESP2 flat array or RESP3 map).
#[must_use]
pub fn stream_info(reply: &Value) -> Option<StreamInfo> {
    let pairs: Vec<(&Value, &Value)> = match reply {
        Value::Array(flat) => flat.chunks(2).filter_map(|c| Some((c.first()?, c.get(1)?))).collect(),
        Value::Map(m) => m.iter().map(|(k, v)| (k, v)).collect(),
        _ => return None,
    };
    let get = |name: &str| pairs.iter().find(|(k, _)| text(k).as_deref() == Some(name)).map(|(_, v)| *v);
    let first_entry = match get("first-entry") {
        Some(Value::Array(parts)) => parts.first().and_then(text).and_then(|id| parse_id(&id)),
        _ => None,
    };
    Some(StreamInfo {
        last_generated: get("last-generated-id").and_then(text).and_then(|id| parse_id(&id))?,
        entries_added: get("entries-added").and_then(number)?,
        length: get("length").and_then(number).unwrap_or(0),
        first_entry,
    })
}

/// The reader.
pub struct Reader {
    conn: ConnectionManager,
    key: String,
    labels: Labels,
    tx: mpsc::Sender<Msg>,
    s: ReaderSettings,
    probe: Arc<Probe>,
    /// The last id read (or the anchor).
    id: (i64, i64),
    /// The anchor's milliseconds, and the last Redis time the reader was at
    /// the stream's tail (the anchor, or a poll that was not full).
    anchor_ms: i64,
    at_tail_redis: i64,
    /// `entries-added` at the anchor, and entries read since.
    added_at_anchor: u64,
    read_since_anchor: u64,
    offset: i64,
    lag_ms: Arc<AtomicI64>,
    last_loop_ms: Arc<AtomicI64>,
}

enum Failure {
    /// The key does not exist: an empty stream.
    NoKey,
    /// Unreachable, an error reply, or a timeout.
    Down,
}

impl Reader {
    #[must_use]
    pub fn new(conn: ConnectionManager, key: &str, labels: Labels, tx: mpsc::Sender<Msg>, s: ReaderSettings, probe: Arc<Probe>) -> Self {
        Self {
            conn,
            key: key.to_string(),
            labels,
            tx,
            s,
            probe,
            id: (0, 0),
            anchor_ms: 0,
            at_tail_redis: 0,
            added_at_anchor: 0,
            read_since_anchor: 0,
            offset: 0,
            lag_ms: Arc::default(),
            last_loop_ms: Arc::new(AtomicI64::new(local_ms())),
        }
    }

    /// Run the reader and the lag gauge's task.
    #[must_use]
    pub fn spawn(self) -> Vec<JoinHandle<()>> {
        let (lag, last) = (Arc::clone(&self.lag_ms), Arc::clone(&self.last_loop_ms));
        let gauge = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let since = (local_ms() - last.load(Ordering::Relaxed)).max(0);
                #[allow(clippy::cast_precision_loss)]
                metrics::gauge!(METRIC_LAG).set(lag.load(Ordering::Relaxed).max(since) as f64 / 1000.0);
            }
        });
        vec![tokio::spawn(self.run()), gauge]
    }

    async fn call(&mut self, cmd: &redis::Cmd) -> Result<Value, Failure> {
        match tokio::time::timeout(self.s.timeout, cmd.query_async::<Value>(&mut self.conn)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) if e.to_string().to_ascii_lowercase().contains("no such key") => Err(Failure::NoKey),
            Ok(Err(_)) | Err(_) => Err(Failure::Down),
        }
    }

    async fn info(&mut self) -> Result<Option<StreamInfo>, Failure> {
        let mut cmd = redis::cmd("XINFO");
        cmd.arg("STREAM").arg(&self.key);
        match self.call(&cmd).await {
            Ok(v) => Ok(stream_info(&v)),
            Err(Failure::NoKey) => Ok(None),
            Err(f) => Err(f),
        }
    }

    fn redis_now(&self) -> i64 {
        local_ms() + self.offset
    }

    /// Anchor at the stream's `last-generated-id`.
    async fn anchor(&mut self) -> Result<(), Failure> {
        let info = self.info().await?;
        let (id, added) = info.map_or(((0, 0), 0), |i| (i.last_generated, i.entries_added));
        self.id = id;
        self.anchor_ms = id.0;
        self.at_tail_redis = self.redis_now();
        self.added_at_anchor = added;
        self.read_since_anchor = 0;
        self.lag_ms.store(0, Ordering::Relaxed);
        Ok(())
    }

    /// Redis time three times; the offset of the shortest round trip.
    async fn sync_clock(&mut self) {
        let mut best: Option<(i64, i64)> = None;
        for _ in 0..3 {
            let before = local_ms();
            let Ok(v) = self.call(&redis::cmd("TIME")).await else {
                continue;
            };
            let after = local_ms();
            let Value::Array(parts) = v else {
                continue;
            };
            let (Some(secs), Some(micros)) = (parts.first().and_then(number), parts.get(1).and_then(number)) else {
                continue;
            };
            let redis_ms = i64::try_from(secs * 1000 + micros / 1000).unwrap_or(0);
            let rtt = after - before;
            let offset = redis_ms - (before + after) / 2;
            if best.is_none_or(|(r, _)| rtt < r) {
                best = Some((rtt, offset));
            }
        }
        if let Some((_, offset)) = best {
            if offset.abs() > 1000 {
                tracing::warn!(event = "public_feed_clock_offset", offset_ms = offset, "the Redis clock is over a second from this host's; the live feed keeps Redis time");
            }
            self.offset = offset;
            let _ = self.tx.send(Msg::Offset(offset)).await;
        }
    }

    /// One `XREAD`; whether the reply was full.
    async fn poll(&mut self) -> Result<bool, Failure> {
        let mut cmd = redis::cmd("XREAD");
        cmd.arg("COUNT").arg(COUNT).arg("STREAMS").arg(&self.key).arg(format!("{}-{}", self.id.0, self.id.1));
        let sent_at = self.redis_now();
        let reply = self.call(&cmd).await?;
        let entries = parse_read_reply(reply).map_err(|_| Failure::Down)?;
        let full = entries.len() >= COUNT;
        if !full {
            // It read everything up to the moment it asked: it was at the
            // tail then, whatever arrives after.
            self.at_tail_redis = self.at_tail_redis.max(sent_at);
        }
        if entries.is_empty() {
            self.probe.polled_empty.notify_waiters();
            return Ok(false);
        }
        let mut decoded = Vec::with_capacity(entries.len());
        let mut undecodable = 0u64;
        for e in &entries {
            if let Some(id) = parse_id(&e.id) {
                self.id = id;
            }
            match decode(e, &self.labels) {
                Some(d) => decoded.push(d),
                None => undecodable += 1,
            }
        }
        self.read_since_anchor += entries.len() as u64;
        if undecodable > 0 {
            metrics::counter!(METRIC_UNDECODABLE).increment(undecodable);
        }
        // A full channel makes the reader wait; its lag grows, and the
        // re-anchor applies. Nothing is dropped silently.
        let _ = self.tx.send(Msg::Entries(decoded)).await;
        Ok(full)
    }

    /// The once-a-second check: the input reading, the lag, and the three
    /// re-anchors.
    async fn check(&mut self) -> Result<(), Failure> {
        let Some(info) = self.info().await? else {
            // A missing key: an empty stream; keep the id. A stream made
            // again counts its entries from zero.
            self.lag_ms.store(0, Ordering::Relaxed);
            self.added_at_anchor = 0;
            self.read_since_anchor = 0;
            return Ok(());
        };
        let _ = self.tx.send(Msg::Input { at_ms: local_ms(), entries_added: info.entries_added }).await;
        // Entries added since the anchor that the reader has not read. With
        // the whole stream above its id they cannot exceed the length, and
        // at the tail they are none; past either, unread entries were
        // trimmed (nothing issues `XDEL` on the stream).
        let missing = info.entries_added.saturating_sub(self.added_at_anchor + self.read_since_anchor);
        let trimmed = (info.first_entry.is_some_and(|f| f > self.id) && missing > info.length) || (info.last_generated == self.id && missing > 0);
        let why = if info.last_generated < self.id {
            Some(Why::Reset)
        } else if trimmed {
            Some(Why::Behind)
        } else {
            // Its position in time: its id, or the last moment it was at the
            // tail (so an entry that ends a long silence just before this
            // check is no lag).
            let own = self.id.0.max(self.at_tail_redis);
            let lag = (info.last_generated.0 - own).max(0);
            self.lag_ms.store(lag, Ordering::Relaxed);
            (u128::try_from(lag).unwrap_or(0) > self.s.behind.as_millis()).then_some(Why::Behind)
        };
        if let Some(why) = why {
            let prev_anchor_ms = self.anchor_ms;
            self.anchor().await?;
            tracing::info!(event = "public_feed_skipped", why = why.slug(), "the live reader re-anchored at the stream's tail");
            let _ = self.tx.send(Msg::Skipped(Skip { to_ms: self.anchor_ms, why, prev_anchor_ms })).await;
        }
        Ok(())
    }

    async fn wait_unpaused(&self) {
        while self.probe.paused.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn run(mut self) {
        self.sync_clock().await;
        let mut backoff = self.s.backoff_first;
        loop {
            self.wait_unpaused().await;
            if self.anchor().await.is_ok() {
                break;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(self.s.backoff_max);
        }
        backoff = self.s.backoff_first;
        let mut next_poll = Instant::now();
        let mut next_check = Instant::now() + self.s.info_every;
        let mut next_clock = Instant::now() + self.s.clock_every;
        loop {
            tokio::time::sleep_until(next_poll).await;
            self.wait_unpaused().await;
            // A check that is due runs before the next read, so a reader that
            // was held (a hung call, a full channel, a starved host) measures
            // its lag first and re-anchors rather than reading what it missed.
            if Instant::now() >= next_check {
                next_check = Instant::now() + self.s.info_every;
                if self.check().await.is_err() {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.s.backoff_max);
                    next_poll = Instant::now();
                    continue;
                }
            }
            let polled = self.poll().await;
            let full = match polled {
                Ok(full) => full,
                Err(_) => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.s.backoff_max);
                    next_poll = Instant::now();
                    continue;
                }
            };
            let now = Instant::now();
            if full {
                // At once after a full reply.
                next_check = now;
            }
            backoff = self.s.backoff_first;
            if now >= next_clock {
                next_clock = now + self.s.clock_every;
                self.sync_clock().await;
            }
            self.last_loop_ms.store(local_ms(), Ordering::Relaxed);
            next_poll = if full {
                Instant::now()
            } else {
                // The 50 ms grid; a late loop takes the next point of it.
                let mut n = next_poll + self.s.poll;
                while n <= Instant::now() {
                    n += self.s.poll;
                }
                n
            };
        }
    }
}
