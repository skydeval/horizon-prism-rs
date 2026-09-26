//! The firehose consumer worker (design-v2 §5.3, §5.7, §6.11.4).
//!
//! **Startup.** Load the scripts; run `group_create` (a creation while
//! watermarks exist is a gap); `XAUTOCLAIM` under `claim_in_progress` (deleted
//! ids, or a flag left set, are a gap); then drain the pending list.
//!
//! **Loop.** Read through `read_checked`; only when it returns no entries,
//! issue a blocking `XREADGROUP … BLOCK 1000 … >`. Entries are processed
//! sequentially, each in its own transaction, and acked (pipelined per batch)
//! only after commit. A transient failure sleeps with backoff (100 ms doubling
//! to 10 s) and re-reads the pending list.
//!
//! **Gaps** run the gap transaction before anything further is read or acked;
//! then `reanchor` runs, or the pending ids are acked.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use redis::Value;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::coverage::CoverageHandle;
use super::deadletter::{self, DeadLetter};
use super::gap::{self, GapCause};
use super::router::route_identity;
use super::scripts::{self, CheckedRead, ScriptError};
use super::stream::{Commit, Event, StreamEntry, decode, parse_read_reply, repo_hint};
use super::watermark::{Watermarks, contribution};
use super::writer::{ApplyOptions, Transient, Writer};
use crate::config::FirehoseConfig;

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

pub const METRIC_LAG: &str = "firehose_consumer_lag_entries";
pub const METRIC_PENDING: &str = "firehose_consumer_pending";

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
}

/// State shared with `/xrpc/_health`.
#[derive(Debug, Default)]
pub struct ConsumerStatus {
    gap_blocked: AtomicBool,
}

impl ConsumerStatus {
    /// A gap was detected and its transaction has not yet committed.
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
        Self {
            cfg,
            redis,
            pool,
            writer,
            watermarks,
            coverage,
            status,
        }
    }

    fn apply_options(&self) -> ApplyOptions {
        ApplyOptions {
            advance_applied_rev: true,
            dead_letter_readiness: true,
            attempts: self.cfg.deterministic_attempts,
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
        self.ensure_group().await?;
        self.autoclaim().await
    }

    async fn ensure_group(&mut self) -> Result<(), ConsumerError> {
        let created = scripts::group_create(&mut self.redis, &self.cfg.stream_key, &self.cfg.group).await?;
        if created {
            tracing::info!(event = "firehose_group_created", group = %self.cfg.group, "consumer group created");
            // §6.11.4 item 3: a (re)creation while watermarks exist is a gap.
            if Watermarks::any_persisted(&self.pool).await? {
                self.gap(GapCause::GroupRecreated, false).await?;
            }
        }
        Ok(())
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
                .await?;
            let (next, dels) = parse_autoclaim(reply).map_err(ConsumerError::Shape)?;
            deleted.extend(dels);
            if next == "0-0" {
                break;
            }
            cursor = next;
        }
        if interrupted || !deleted.is_empty() {
            let cause = if deleted.is_empty() { GapCause::InterruptedClaim } else { GapCause::Pending };
            self.gap(cause, true).await?;
            self.ack(&deleted).await?;
        } else {
            gap::set_claim_in_progress(&self.pool, false).await?;
        }
        Ok(())
    }

    /// Run the gap transaction, with `gap_blocked` set until it commits.
    async fn gap(&mut self, cause: GapCause, clear_claim: bool) -> Result<(), ConsumerError> {
        self.status.set_gap_blocked(true);
        let private = Arc::clone(self.writer.private());
        gap::run_gap_transaction(&self.pool, self.writer.scope(), private.as_ref(), cause, clear_claim).await?;
        self.status.set_gap_blocked(false);
        Ok(())
    }

    async fn run(mut self) {
        let mut mode = Mode::Pending;
        let mut backoff = BACKOFF_MIN;
        loop {
            match self.step(&mut mode).await {
                Ok(()) => backoff = BACKOFF_MIN,
                Err(error) => {
                    tracing::warn!(event = "firehose_transient", %error, backoff_ms = backoff.as_millis(), "transient failure; re-reading pending entries");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    mode = Mode::Pending;
                }
            }
        }
    }

    async fn step(&mut self, mode: &mut Mode) -> Result<(), ConsumerError> {
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
                    self.gap(GapCause::Pending, false).await?;
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
                .await?;
                match read {
                    CheckedRead::NoGroup => {
                        tracing::warn!(event = "firehose_group_missing", "consumer group missing; recreating");
                        self.ensure_group().await
                    }
                    CheckedRead::Gap(evidence) => {
                        tracing::warn!(
                            event = "firehose_trim_gap",
                            first_entry_id = %evidence.first_entry_id,
                            entries_added = %evidence.entries_added,
                            length = %evidence.length,
                            "trim evidence"
                        );
                        self.gap(GapCause::Trim, false).await?;
                        let (id, read) = scripts::reanchor(&mut self.redis, &self.cfg.stream_key, &self.cfg.group).await?;
                        tracing::info!(event = "firehose_reanchored", last_delivered_id = %id, entries_read = %read, "group re-anchored after gap");
                        Ok(())
                    }
                    CheckedRead::Entries(entries) if entries.is_empty() => {
                        let entries = self.read_blocking().await?;
                        self.process_batch(entries).await
                    }
                    CheckedRead::Entries(entries) => self.process_batch(entries).await,
                }
            }
        }
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
            .await?;
        parse_read_reply(reply).map_err(ConsumerError::Shape)
    }

    /// Only after an empty `read_checked`: everything up to the tail is read,
    /// so nothing unread can be trimmed before this returns (§6.11.4).
    async fn read_blocking(&mut self) -> Result<Vec<StreamEntry>, ConsumerError> {
        let reply: Value = redis::cmd("XREADGROUP")
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
            .await?;
        parse_read_reply(reply).map_err(ConsumerError::Shape)
    }

    /// Process entries in stream order; ack the committed ones in one
    /// pipelined call, including on a transient failure part-way through.
    async fn process_batch(&mut self, entries: Vec<StreamEntry>) -> Result<(), ConsumerError> {
        let mut acks: Vec<String> = Vec::with_capacity(entries.len());
        for entry in &entries {
            if let Err(error) = self.process_entry(entry).await {
                if let Err(ack_error) = self.ack(&acks).await {
                    tracing::warn!(event = "firehose_ack_failed", error = %ack_error, "acking committed entries failed; they will be re-applied");
                }
                return Err(error);
            }
            acks.push(entry.id.clone());
        }
        self.ack(&acks).await
    }

    async fn process_entry(&mut self, entry: &StreamEntry) -> Result<(), ConsumerError> {
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
                return Ok(());
            }
        };
        match event {
            Event::Identity { kind, did, .. } => {
                route_identity(&self.pool, kind, &did).await?;
                Ok(())
            }
            Event::Commit(commit) => self.process_commit(entry, commit).await,
        }
    }

    async fn process_commit(&mut self, entry: &StreamEntry, commit: Commit) -> Result<(), ConsumerError> {
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
        Ok(())
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

/// Sample `firehose_consumer_lag_entries` and `firehose_consumer_pending`
/// every 10 s, logging `firehose_lag_warn` after 60 s above the threshold
/// (§5.7). Uses a shared connection; the worker's own is kept for its reads.
#[must_use]
pub fn spawn_lag_sampler(mut redis: ConnectionManager, cfg: FirehoseConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(LAG_SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut above_since: Option<Instant> = None;
        let mut warned = false;
        loop {
            tick.tick().await;
            match sample_lag(&mut redis, &cfg).await {
                Ok((lag, pending)) => {
                    if let Some(lag) = lag {
                        #[allow(clippy::cast_precision_loss)]
                        metrics::gauge!(METRIC_LAG).set(lag as f64);
                    }
                    #[allow(clippy::cast_precision_loss)]
                    metrics::gauge!(METRIC_PENDING).set(pending as f64);
                    if lag.is_some_and(|l| l > cfg.lag_warn_entries) {
                        let since = *above_since.get_or_insert_with(Instant::now);
                        if !warned && since.elapsed() >= LAG_WARN_AFTER {
                            tracing::warn!(event = "firehose_lag_warn", lag, threshold = cfg.lag_warn_entries, "consumer lag above threshold for 60 s");
                            warned = true;
                        }
                    } else {
                        above_since = None;
                        warned = false;
                    }
                }
                Err(error) => tracing::debug!(event = "firehose_lag_sample_failed", %error, "lag sample failed"),
            }
        }
    })
}

async fn sample_lag(redis: &mut ConnectionManager, cfg: &FirehoseConfig) -> Result<(Option<u64>, u64), redis::RedisError> {
    let groups: Value = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(&cfg.stream_key)
        .query_async(redis)
        .await?;
    let lag = group_field(&groups, &cfg.group, "lag").and_then(|v| match v {
        Value::Int(i) => u64::try_from(i).ok(),
        other => scripts::string(&other).and_then(|s| s.parse().ok()),
    });
    let pending: Value = redis::cmd("XPENDING")
        .arg(&cfg.stream_key)
        .arg(&cfg.group)
        .query_async(redis)
        .await?;
    let count = match pending {
        Value::Array(parts) => parts
            .first()
            .and_then(|v| match v {
                Value::Int(i) => u64::try_from(*i).ok(),
                other => scripts::string(other).and_then(|s| s.parse().ok()),
            })
            .unwrap_or(0),
        _ => 0,
    };
    Ok((lag, count))
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
}
