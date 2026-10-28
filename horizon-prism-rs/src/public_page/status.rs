//! The status snapshot (public-page design §4.1): one task builds a
//! [`StatusSnapshot`] every [`REFRESH_EVERY`] and stores it in an
//! `ArcSwap`, so `GET /` renders from memory and never queries anything.
//!
//! What it reads: the database and Redis probes, the audit latch, and the
//! live reader's summary (each relay's own delay and silence, and the input
//! rate). Nothing it reads says how far Prism's own processing has got: no
//! watermark, no consumer lag, no backlog (§2.2).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::serve::health::{AuditLatch, PROBE_TIMEOUT, probe_db, probe_redis};

/// How often the snapshot is rebuilt.
pub const REFRESH_EVERY: Duration = Duration::from_secs(15);

/// A relay is "not heard from" once the newest published second is this far
/// past the last second it was heard in.
pub const SILENT_AFTER_SECS: i64 = 60;

/// One relay's figure in the live reader's summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayFigure {
    /// The entry-time second (Unix) of the newest entry read from it.
    pub last_heard_sec: i64,
    /// How far behind the network it was, at the last published second
    /// that held a commit from it with a time. A second with none keeps the
    /// last figure; `None` only while no commit with a time has been seen
    /// from it since the start ("receiving (no commits to time)").
    pub delay: Option<Delay>,
}

/// A relay's delay: the lower edge of the histogram bucket holding a
/// second's median (§6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delay {
    Ms(u64),
    /// The top bucket, over 10 minutes.
    OverTenMinutes,
}

/// What the live reader publishes for the status block (§4.1), stored in a
/// [`FeedHandle`] once a second.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeedSummary {
    /// By relay host.
    pub relays: BTreeMap<String, RelayFigure>,
    /// The newest entry-time second published (Unix).
    pub newest_published_sec: Option<i64>,
    /// The relays' input, events a second over the last 10 s; `None` until
    /// two readings exist.
    pub input_per_sec: Option<f64>,
    /// Arrivals since the start, by kind slug, the nonzero ones.
    pub totals: Vec<(String, u64)>,
    /// Identity events since the start.
    pub events_total: u64,
    /// The start, rounded to the hour ("YYYY-MM-DD HH:00", UTC).
    pub since: String,
    /// The reader skipped events at some point since the start.
    pub skipped_since_start: bool,
}

/// The reader's summary, shared with the status task.
pub type FeedHandle = Arc<ArcSwap<FeedSummary>>;

/// Everything `GET /` shows that can change while Prism runs. Every field
/// is listed in T-P6's destructuring, so a field fed from the consumer's
/// lag or a watermark cannot be added unnoticed.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusSnapshot {
    pub db_ok: bool,
    /// Redis unreachable or the audit guard latched: "Horizon-Prism is
    /// having a problem", with no cause.
    pub problem: bool,
    /// The relays in one line: how many it receives from, and how far
    /// behind the network they are.
    pub relays: String,
    /// The relays' input, events a second.
    pub input_per_sec: Option<f64>,
}

/// What a snapshot is built from, read at one moment.
pub struct Observed<'a> {
    pub db_ok: bool,
    pub redis_ok: bool,
    pub audit_latched: bool,
    pub feed: &'a FeedSummary,
}

/// The configured relays, as the reader keys them, sorted, once each.
#[must_use]
pub fn relay_list(relay_labels: &BTreeMap<String, String>) -> Vec<String> {
    let mut hosts: Vec<String> = super::relay_hosts(relay_labels).into_values().collect();
    hosts.sort();
    hosts.dedup();
    hosts
}

/// Whether a relay is receiving: heard from within the last
/// [`SILENT_AFTER_SECS`] of published seconds.
fn receiving(figure: &RelayFigure, newest_published_sec: Option<i64>) -> bool {
    newest_published_sec.map_or(0, |newest| newest - figure.last_heard_sec) < SILENT_AFTER_SECS
}

#[allow(clippy::cast_precision_loss)]
fn delay_words(d: Delay) -> String {
    match d {
        Delay::OverTenMinutes => "more than 10 minutes behind the network".to_string(),
        Delay::Ms(ms) => format!("{:.1} s behind the network", ms as f64 / 1000.0),
    }
}

/// The relays' line (§2.2): how many of the configured relays are
/// receiving, and the median of their delays, with no relay named.
#[must_use]
pub fn relays_words(feed: &FeedSummary, configured: &[String]) -> String {
    let receiving: Vec<&RelayFigure> = configured
        .iter()
        .filter_map(|r| feed.relays.get(r))
        .filter(|f| receiving(f, feed.newest_published_sec))
        .collect();
    if receiving.is_empty() {
        return "Not receiving from any relay right now".to_string();
    }
    let n = configured.len();
    let count = if receiving.len() == n {
        format!("Receiving from {n} relay{}", if n == 1 { "" } else { "s" })
    } else {
        format!("Receiving from {} of {n} relays", receiving.len())
    };
    let mut delays: Vec<Delay> = receiving.iter().filter_map(|f| f.delay).collect();
    delays.sort_by_key(|d| match d {
        Delay::Ms(ms) => *ms,
        Delay::OverTenMinutes => u64::MAX,
    });
    match delays.get(delays.len().saturating_sub(1) / 2) {
        Some(d) => format!("{count}, {}", delay_words(*d)),
        None => format!("{count} (no commits to time yet)"),
    }
}

/// Build the snapshot from what was observed; `relays` are the configured
/// relays, as the reader keys them.
#[must_use]
pub fn build(o: &Observed<'_>, relays: &[String]) -> StatusSnapshot {
    StatusSnapshot {
        db_ok: o.db_ok,
        problem: !o.redis_ok || o.audit_latched,
        relays: relays_words(o.feed, relays),
        input_per_sec: o.feed.input_per_sec,
    }
}

/// The task's inputs (§4.1), cloned in `serve`.
#[derive(Clone)]
pub struct StatusInputs {
    pub pool: PgPool,
    /// `None` reads as unreachable.
    pub redis: Option<ConnectionManager>,
    pub audit: AuditLatch,
    /// The configured relays, as the reader keys them (counted, never shown).
    pub relays: Vec<String>,
    pub feed: FeedHandle,
}

/// The snapshot task.
pub struct Status {
    inputs: StatusInputs,
    cell: Arc<ArcSwap<StatusSnapshot>>,
}

impl Status {
    /// Build the first snapshot, so the page never serves an empty one.
    pub async fn start(inputs: StatusInputs) -> Arc<Self> {
        let first = observe_and_build(&inputs).await;
        Arc::new(Self { inputs, cell: Arc::new(ArcSwap::from_pointee(first)) })
    }

    /// The snapshot the page reads.
    #[must_use]
    pub fn cell(&self) -> Arc<ArcSwap<StatusSnapshot>> {
        Arc::clone(&self.cell)
    }

    /// Rebuild the snapshot now.
    pub async fn refresh(&self) {
        let next = observe_and_build(&self.inputs).await;
        self.cell.store(Arc::new(next));
    }

    /// Rebuild every [`REFRESH_EVERY`], for as long as `serve` runs.
    #[must_use]
    pub fn spawn(self: &Arc<Self>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REFRESH_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                this.refresh().await;
            }
        })
    }
}

async fn observe_and_build(i: &StatusInputs) -> StatusSnapshot {
    let redis = async {
        match &i.redis {
            Some(r) => probe_redis(r, PROBE_TIMEOUT).await,
            None => false,
        }
    };
    let (db_ok, redis_ok) = tokio::join!(probe_db(&i.pool, PROBE_TIMEOUT), redis);
    let feed = i.feed.load();
    build(&Observed { db_ok, redis_ok, audit_latched: i.audit.latched(), feed: &feed }, &i.relays)
}
