//! Health (admin-ui design §9): Prism's own metrics read back in process,
//! the snapshot ring, and the checks of §9.2 in plain words.
//!
//! `serve` keeps the Prometheus recorder's handle (§9.1); [`MetricsText`]
//! renders it, and [`parse`] reads the text exposition back. Every 60 s a
//! task keeps the watched series ([`WATCHED`]) in a [`Ring`] of the last
//! 60 snapshots, which answers "has it been true for 5 minutes" and "how
//! much did this counter rise in the last hour" without a time-series
//! database. The ring starts empty at each start.
//!
//! Honest state (§2, principle 5): a check whose input is missing is
//! [`Level::NoData`], shown grey, never green. A "for N minutes" check is
//! "no data" until N minutes of snapshots exist (review 1, finding 10).
//! The Disk check reads the disks (§10.1) when the checks are gathered.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use metrics_exporter_prometheus::PrometheusHandle;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::AdminState;
use super::auth::Banners;
use crate::db::schema_backfill::SchemaStatus;
use crate::farsight::Farsight;
use crate::firehose::consumer::ConsumerStatus;
use crate::serve::health::{AuditLatch, PROBE_TIMEOUT, probe_db, probe_redis};

/// How often the ring takes a snapshot.
pub const SNAPSHOT_EVERY: Duration = Duration::from_secs(60);
/// Snapshots kept: one hour.
pub const RING_LEN: usize = 60;

/// The series the ring keeps and the pages read. Each is emitted somewhere
/// in `src/` (test T-A13 scans for it).
pub const WATCHED: &[&str] = &[
    crate::firehose::consumer::METRIC_GROUP_PRESENT,
    crate::firehose::consumer::METRIC_LAG,
    crate::firehose::consumer::METRIC_PROCESSED,
    crate::firehose::watermark::METRIC_DELAY,
    crate::firehose::deadletter::METRIC_DEAD_LETTERS,
    crate::firehose::deadletter::METRIC_MALFORMED_DROPPED,
    crate::firehose::gap::METRIC_GAPS,
    crate::db::sampler::METRIC_DB_BUDGET_RATIO,
    crate::db::sampler::METRIC_DB_SIZE,
    crate::db::sampler::METRIC_REDIS_RATIO,
    crate::db::sampler::METRIC_REDIS_USED,
    crate::db::sampler::METRIC_REDIS_MAX,
    crate::firehose::backfill::METRIC_AUTHZ_STUCK,
    crate::farsight::gate::METRIC_DOWN,
    crate::farsight::gate::METRIC_WITHHELD,
    crate::farsight::lookup::METRIC_AUTH_FAILED,
    crate::indexing::public_sweep::METRIC_STALLED,
    crate::indexing::public_fill::METRIC_REMAINING,
    crate::db::schema_backfill::METRIC_REMAINING,
    crate::db::schema_backfill::METRIC_INDEX_PENDING,
    crate::aggregations::rebuild::METRIC_SWEEP_RUNS,
    // Private posts' new form (private-embeds design §9): the read side's
    // form and its unreadable payloads, and where ingest took quotes and
    // mentions from.
    PRIVATE_READ_CONTENT,
    PRIVATE_READ_OUTCOMES,
    PRIVATE_INGEST_SOURCE,
    crate::system_accounts::METRIC_STATE,
    crate::system_accounts::METRIC_PENDING,
];

/// `private_read_content_total{form}`, emitted by the private tier.
pub const PRIVATE_READ_CONTENT: &str = "private_read_content_total";
/// `private_read_outcome_total{outcome}`, emitted by the private tier.
pub const PRIVATE_READ_OUTCOMES: &str = "private_read_outcome_total";
/// `private_ingest_index_source_total{field, source}`.
pub const PRIVATE_INGEST_SOURCE: &str = "private_ingest_index_source_total";

/// Something that renders the metrics as Prometheus text.
pub trait MetricsText: Send + Sync {
    fn render(&self) -> String;
}

impl MetricsText for PrometheusHandle {
    fn render(&self) -> String {
        PrometheusHandle::render(self)
    }
}

/// One series: its name, its labels sorted by name, and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

impl Sample {
    /// The value of one of its labels.
    #[must_use]
    pub fn label(&self, key: &str) -> Option<&str> {
        self.labels.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }
}

fn parse_value(s: &str) -> Option<f64> {
    match s {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        other => other.parse().ok(),
    }
}

/// `key="value",…` up to the closing brace; the rest of the line after it.
fn parse_labels(s: &str) -> Option<(Vec<(String, String)>, &str)> {
    let mut labels = Vec::new();
    let mut rest = s;
    loop {
        rest = rest.trim_start_matches([',', ' ']);
        if let Some(after) = rest.strip_prefix('}') {
            labels.sort();
            return Some((labels, after));
        }
        let (key, after) = rest.split_once("=\"")?;
        let mut value = String::new();
        let mut chars = after.char_indices();
        let end = loop {
            let (i, c) = chars.next()?;
            match c {
                '\\' => match chars.next()?.1 {
                    'n' => value.push('\n'),
                    other => value.push(other),
                },
                '"' => break i,
                other => value.push(other),
            }
        };
        labels.push((key.trim().to_string(), value));
        rest = &after[end + 1..];
    }
}

/// The samples of a text exposition whose name `keep` accepts. Comments,
/// blank lines and lines that do not parse are skipped.
#[must_use]
pub fn parse(text: &str, keep: impl Fn(&str) -> bool) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let end = line.find(['{', ' ']).unwrap_or(line.len());
        let name = &line[..end];
        if !keep(name) {
            continue;
        }
        let (labels, rest) = if line[end..].starts_with('{') {
            match parse_labels(&line[end + 1..]) {
                Some(parsed) => parsed,
                None => continue,
            }
        } else {
            (Vec::new(), &line[end..])
        };
        let Some(value) = rest.split_whitespace().next().and_then(parse_value) else {
            continue;
        };
        out.push(Sample { name: name.to_string(), labels, value });
    }
    out
}

/// The watched series at one moment.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub at: DateTime<Utc>,
    pub series: Vec<Sample>,
}

impl Snapshot {
    /// The watched series of `text`, as of `at`.
    #[must_use]
    pub fn from_text(text: &str, at: DateTime<Utc>) -> Self {
        Self { at, series: parse(text, |n| WATCHED.contains(&n)) }
    }

    fn of<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Sample> + 'a {
        self.series.iter().filter(move |s| s.name == name)
    }

    /// The series' value, summed over its labels; `None` when it is absent.
    #[must_use]
    pub fn sum(&self, name: &str) -> Option<f64> {
        self.of(name).map(|s| s.value).reduce(|a, b| a + b)
    }

    /// The largest value over its labels.
    #[must_use]
    pub fn max(&self, name: &str) -> Option<f64> {
        self.of(name).map(|s| s.value).reduce(f64::max)
    }

    /// The sum over the series whose `label` is `value`.
    #[must_use]
    pub fn sum_where(&self, name: &str, label: &str, value: &str) -> Option<f64> {
        self.of(name).filter(|s| s.label(label) == Some(value)).map(|s| s.value).reduce(|a, b| a + b)
    }

    /// The sum over the series whose labels hold every `(label, value)`.
    #[must_use]
    pub fn sum_where_all(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.of(name).filter(|s| labels.iter().all(|(l, v)| s.label(l) == Some(*v))).map(|s| s.value).reduce(|a, b| a + b)
    }

    /// Each value of `label`, with its value, in label order.
    #[must_use]
    pub fn by(&self, name: &str, label: &str) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = self.of(name).map(|s| (s.label(label).unwrap_or_default().to_string(), s.value)).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

/// What a "for N minutes" question answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    /// Not enough history yet, or the series is missing.
    NoData,
    /// True at every snapshot of the window.
    Yes,
    No,
}

#[derive(Default)]
struct RingInner {
    snapshots: VecDeque<Arc<Snapshot>>,
    /// Whether a snapshot has been dropped, so the oldest one no longer
    /// shows the process's start.
    evicted: bool,
}

/// The last [`RING_LEN`] snapshots, in memory.
#[derive(Default)]
pub struct Ring(Mutex<RingInner>);

impl Ring {
    fn inner(&self) -> std::sync::MutexGuard<'_, RingInner> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keep a snapshot, dropping the oldest past [`RING_LEN`].
    pub fn push(&self, snapshot: Snapshot) {
        let mut inner = self.inner();
        inner.snapshots.push_back(Arc::new(snapshot));
        while inner.snapshots.len() > RING_LEN {
            inner.snapshots.pop_front();
            inner.evicted = true;
        }
    }

    /// The newest snapshot.
    #[must_use]
    pub fn latest(&self) -> Option<Arc<Snapshot>> {
        self.inner().snapshots.back().cloned()
    }

    /// Whether `pred` has held at every snapshot of the last `window`
    /// (§9.1). "No data" until the ring spans `window`, or when `pred`
    /// finds its series missing at any of them.
    #[must_use]
    pub fn held_for(&self, window: Duration, pred: impl Fn(&Snapshot) -> Option<bool>) -> Held {
        let inner = self.inner();
        let (Some(first), Some(last)) = (inner.snapshots.front(), inner.snapshots.back()) else {
            return Held::NoData;
        };
        let window = TimeDelta::from_std(window).unwrap_or(TimeDelta::MAX);
        if last.at - first.at < window {
            return Held::NoData;
        }
        let mut all = true;
        for s in inner.snapshots.iter().filter(|s| last.at - s.at <= window) {
            match pred(s.as_ref()) {
                None => return Held::NoData,
                Some(v) => all &= v,
            }
        }
        if all { Held::Yes } else { Held::No }
    }

    /// How much the counter `value` reads rose over the last `window`:
    /// the newest value less the value at the window's start. A counter
    /// starts at zero with the process, so before the ring spans `window`
    /// (and has dropped nothing) the rise is the newest value. `None` when
    /// the newest snapshot lacks the series.
    #[must_use]
    pub fn rise(&self, window: Duration, value: impl Fn(&Snapshot) -> Option<f64>) -> Option<f64> {
        let inner = self.inner();
        let last = inner.snapshots.back()?;
        let now = value(last.as_ref())?;
        let window = TimeDelta::from_std(window).unwrap_or(TimeDelta::MAX);
        let start = inner.snapshots.iter().rev().find(|s| last.at - s.at >= window);
        let base = match start {
            Some(s) => value(s.as_ref()).unwrap_or(0.0),
            None if inner.evicted => inner.snapshots.front().and_then(|s| value(s.as_ref())).unwrap_or(0.0),
            None => 0.0,
        };
        Some((now - base).max(0.0))
    }

    /// The counter's rise per second over about the last `window` (the
    /// whole ring when it is shorter); `None` before two snapshots.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn rate(&self, window: Duration, value: impl Fn(&Snapshot) -> Option<f64>) -> Option<f64> {
        let inner = self.inner();
        let last = inner.snapshots.back()?;
        let w = TimeDelta::from_std(window).unwrap_or(TimeDelta::MAX);
        let start = inner.snapshots.iter().rev().find(|s| last.at - s.at >= w).or_else(|| inner.snapshots.front())?;
        let secs = (last.at - start.at).num_milliseconds() as f64 / 1000.0;
        if secs <= 0.0 {
            return None;
        }
        Some(((value(last.as_ref())? - value(start.as_ref()).unwrap_or(0.0)) / secs).max(0.0))
    }
}

/// What the health pages read besides the database and the settings.
/// Every part is optional: a missing one makes its checks "no data".
#[derive(Clone, Default)]
pub struct HealthInputs {
    pub metrics: Option<Arc<dyn MetricsText>>,
    pub ring: Arc<Ring>,
    pub redis: Option<ConnectionManager>,
    pub consumer: Option<Arc<ConsumerStatus>>,
    pub audit: Option<AuditLatch>,
    pub schema: Option<SchemaStatus>,
    pub farsight: Option<Farsight>,
}

impl HealthInputs {
    /// The watched series now, rendered afresh; `None` without metrics.
    #[must_use]
    pub fn now(&self) -> Option<Snapshot> {
        self.metrics.as_ref().map(|m| Snapshot::from_text(&m.render(), Utc::now()))
    }
}

/// How a check stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Red,
    Amber,
    Info,
    NoData,
    Ok,
}

impl Level {
    /// The class and word a page shows.
    #[must_use]
    pub fn class(self) -> &'static str {
        match self {
            Self::Red => "red",
            Self::Amber => "amber",
            Self::Info => "info",
            Self::NoData => "nodata",
            Self::Ok => "fine",
        }
    }

    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Self::Red => "Problem",
            Self::Amber => "Warning",
            Self::Info => "Note",
            Self::NoData => "No data",
            Self::Ok => "Fine",
        }
    }

    /// Whether Home lists it under "needs attention".
    #[must_use]
    pub fn needs_attention(self) -> bool {
        matches!(self, Self::Red | Self::Amber)
    }
}

/// An action offered beside a check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// `resync --private-and-members` (typed), beside a held gap.
    Resync,
}

/// One check of §9.2.
#[derive(Debug, Clone, PartialEq)]
pub struct Check {
    /// Its name on the page.
    pub name: &'static str,
    pub level: Level,
    /// What it says now.
    pub text: String,
    /// Where to act, when somewhere.
    pub link: Option<(&'static str, &'static str)>,
    pub action: Option<Action>,
}

impl Check {
    fn new(name: &'static str, level: Level, text: impl Into<String>) -> Self {
        Self { name, level, text: text.into(), link: None, action: None }
    }

    fn link(mut self, href: &'static str, label: &'static str) -> Self {
        self.link = Some((href, label));
        self
    }

    fn no_data(name: &'static str, why: &str) -> Self {
        Self::new(name, Level::NoData, format!("No data: {why}"))
    }
}

/// What the checks read besides the ring.
#[derive(Debug, Clone, Default)]
pub struct Inputs {
    pub db: Option<bool>,
    pub redis: Option<bool>,
    pub audit_ok: Option<bool>,
    pub gap_blocked: Option<bool>,
    /// Failed jobs Prism would still run (actionable-warnings design §5.1).
    pub failed_jobs: Option<i64>,
    /// Accounts Prism follows whose jobs ended `unavailable`.
    pub unavailable_accounts: Option<i64>,
    pub members_not_ready: Option<i64>,
    pub set_aside: usize,
    pub waiting: usize,
    /// The schema task: back-fill done, indexes ready.
    pub schema: Option<(bool, bool)>,
    /// `firehose.lag_warn_entries`.
    pub lag_warn_entries: u64,
    /// The relays the configuration names, so a relay never heard from
    /// still has a line.
    pub relays: Vec<String>,
    /// `firehose.private_relays`: private reads wait for every one of them,
    /// so one that has never reported is a problem, not a warning (#737).
    pub private_relays: Vec<String>,
    /// The disks (§10.1), read now.
    pub disks: Option<super::stats::Disks>,
}

/// Under this share free a disk is red (§9.2).
pub const DISK_RED_FREE: f64 = 0.02;
/// Under this share free a disk is amber.
pub const DISK_AMBER_FREE: f64 = 0.10;
/// Under this many bytes free a disk is amber: 5 GB.
pub const DISK_AMBER_BYTES: u64 = 5_000_000_000;

/// The Disk check (§9.2): the fullest disk against the thresholds.
#[allow(clippy::cast_precision_loss)]
fn disk_check(disks: Option<&super::stats::Disks>) -> Check {
    const NAME: &str = "Disk";
    let Some(d) = disks else {
        return Check::no_data(NAME, "the disks are not connected to these pages.");
    };
    let Some(fullest) = d.fullest() else {
        return Check::no_data(NAME, "Prism cannot see any of its disks.");
    };
    let free = fullest.space.free;
    let free_share = 1.0 - fullest.space.used_ratio();
    let text = format!("The disk holding {} has {} free ({} free).", fullest.holds_words(), super::pages::bytes(free as f64), percent(free_share));
    let unseen = if d.not_visible.is_empty() {
        String::new()
    } else {
        format!(" Prism cannot see the disk of {}.", super::stats::join_words(&d.not_visible))
    };
    let check = if free_share < DISK_RED_FREE {
        Check::new(NAME, Level::Red, text)
    } else if free_share < DISK_AMBER_FREE || free < DISK_AMBER_BYTES {
        Check::new(NAME, Level::Amber, text)
    } else {
        Check::new(NAME, Level::Ok, format!("{text}{unseen}"))
    };
    check.link("/admin/stats", "Stats")
}

const FIVE_MIN: Duration = Duration::from_secs(300);
const TWO_MIN: Duration = Duration::from_secs(120);
const FIFTEEN_MIN: Duration = Duration::from_secs(900);
const HOUR: Duration = Duration::from_secs(3600);

/// Prometheus' `+Inf` and a relay never heard from.
fn minutes(secs: f64) -> String {
    let m = (secs / 60.0).round();
    if m <= 1.0 { "about a minute".to_string() } else { format!("{m:.0} minutes") }
}

/// A count in words.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn count(n: f64) -> String {
    let n = n.max(0.0).round() as u64;
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// A percentage of a ratio.
fn percent(ratio: f64) -> String {
    format!("{:.0}%", ratio * 100.0)
}

fn yes_no_check(name: &'static str, ok: Option<bool>, bad: &str, fine: &str, why: &str) -> Check {
    match ok {
        None => Check::no_data(name, why),
        Some(true) => Check::new(name, Level::Ok, fine),
        Some(false) => Check::new(name, Level::Red, bad),
    }
}

/// The checks of §9.2, in that order.
#[must_use]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub fn checks(i: &Inputs, ring: &Ring) -> Vec<Check> {
    let latest = ring.latest();
    let mut out = vec![
        yes_no_check("Database", i.db, "Prism cannot reach its database.", "Prism can reach its database.", "not checked."),
        yes_no_check("Redis", i.redis, "Prism cannot reach Redis.", "Prism can reach Redis.", "Redis is not connected to these pages."),
        yes_no_check(
            "Audit guard",
            i.audit_ok,
            "Prism stopped because its private-post audit log failed. Restart Prism; if it happens again, the logs say why.",
            "The private-post audit log is working.",
            "the audit guard is not connected to these pages.",
        ),
    ];

    out.push(match ring.held_for(TWO_MIN, |s| s.sum(crate::firehose::consumer::METRIC_GROUP_PRESENT).map(|v| v < 0.5)) {
        Held::NoData => Check::no_data("Stream reader", "Prism needs two minutes of readings first."),
        Held::Yes => Check::new("Stream reader", Level::Red, "Prism is not reading the network stream."),
        Held::No => Check::new("Stream reader", Level::Ok, "Prism is reading the network stream."),
    });

    let mut gap = match i.gap_blocked {
        None => Check::no_data("Held gap", "the stream reader is not connected to these pages."),
        Some(false) => Check::new("Held gap", Level::Ok, "No part of the network stream was missed."),
        Some(true) => Check::new("Held gap", Level::Red, "Prism missed part of the network stream. Private posts are held until a re-sync."),
    };
    if i.gap_blocked == Some(true) {
        gap = gap.link("/admin/health", "Health");
        gap.action = Some(Action::Resync);
    }
    out.push(gap);

    let lag = latest.as_ref().and_then(|s| s.sum(crate::firehose::consumer::METRIC_LAG));
    #[allow(clippy::cast_precision_loss)]
    let warn = i.lag_warn_entries as f64;
    out.push(match ring.held_for(FIVE_MIN, |s| s.sum(crate::firehose::consumer::METRIC_LAG).map(|v| v > warn)) {
        Held::NoData => Check::no_data("Backlog", "Prism needs five minutes of readings first."),
        Held::Yes => Check::new("Backlog", Level::Amber, format!("Prism is {} records behind the network.", count(lag.unwrap_or(0.0)))),
        Held::No => Check::new("Backlog", Level::Ok, format!("Prism is keeping up ({} records waiting).", count(lag.unwrap_or(0.0)))),
    });

    // One line per relay: the configured ones, and any other the metric names.
    let mut relays: Vec<String> = i.relays.clone();
    if let Some(s) = &latest {
        for (relay, _) in s.by(crate::firehose::watermark::METRIC_DELAY, "relay") {
            if !relays.contains(&relay) {
                relays.push(relay);
            }
        }
    }
    relays.sort();
    for relay in relays {
        let delay_of = |s: &Snapshot| s.sum_where(crate::firehose::watermark::METRIC_DELAY, "relay", &relay);
        let now = latest.as_ref().and_then(|s| delay_of(s.as_ref()));
        let held = ring.held_for(FIVE_MIN, |s| delay_of(s).map(|d| d > 120.0));
        let private = i.private_relays.contains(&relay);
        out.push(match (held, now) {
            (Held::NoData, _) => Check::no_data("Relay delay", &format!("{relay}: Prism needs five minutes of readings first.")),
            // No watermark, persisted or new: the relay has never reported.
            (Held::Yes, Some(d)) if d.is_infinite() && private => Check::new(
                "Relay delay",
                Level::Red,
                format!(
                    "{relay} has never reported. Private posts wait for it, so every private read misses as stale. \
                     Start its relay reader, or remove it from the relays private posts wait for."
                ),
            )
            .link("/admin/settings", "Settings"),
            (Held::Yes, Some(d)) if d.is_infinite() => Check::new("Relay delay", Level::Amber, format!("{relay} has never reported.")),
            (Held::Yes, d) if private => Check::new(
                "Relay delay",
                Level::Amber,
                format!("{relay} is {} behind; private posts newer than that miss as stale.", minutes(d.unwrap_or(0.0))),
            ),
            (Held::Yes, d) => Check::new("Relay delay", Level::Amber, format!("{relay} is {} behind.", minutes(d.unwrap_or(0.0)))),
            (Held::No, Some(d)) if d.is_finite() => Check::new("Relay delay", Level::Ok, format!("{relay} is up to date ({} s behind).", d.round())),
            (Held::No, _) => Check::new("Relay delay", Level::Ok, format!("{relay} is up to date.")),
        });
    }

    out.push(match ring.rise(HOUR, |s| s.sum(crate::firehose::deadletter::METRIC_DEAD_LETTERS)) {
        None => Check::no_data("Unprocessed records", "none have been counted since Prism started."),
        Some(n) if n > 0.0 => Check::new("Unprocessed records", Level::Amber, format!("{} records could not be processed in the last hour.", count(n))).link("/admin/problems", "See them"),
        Some(_) => {
            // Actionable-warnings design §7.1: malformed records that could
            // not matter are set aside, not listed.
            let set_aside = match ring.rise(HOUR, |s| s.sum(crate::firehose::deadletter::METRIC_MALFORMED_DROPPED)) {
                Some(n) if n > 0.0 => {
                    let one = (n - 1.0).abs() < 0.5;
                    format!(
                        " {} malformed record{} from accounts Prism does not keep {} set aside.",
                        count(n),
                        if one { "" } else { "s" },
                        if one { "was" } else { "were" }
                    )
                }
                _ => String::new(),
            };
            Check::new("Unprocessed records", Level::Ok, format!("Every record in the last hour was processed.{set_aside}")).link("/admin/problems", "Problems")
        }
    });

    out.push(match ring.rise(FIFTEEN_MIN, |s| s.sum(crate::firehose::gap::METRIC_GAPS)) {
        None => Check::no_data("Trim gap", "none have been counted since Prism started."),
        Some(n) if n > 0.0 => Check::new("Trim gap", Level::Red, "Network records were dropped before Prism read them."),
        Some(_) => Check::new("Trim gap", Level::Ok, "No network records were dropped in the last 15 minutes."),
    });

    out.push(match latest.as_ref().and_then(|s| s.sum(crate::db::sampler::METRIC_DB_BUDGET_RATIO)) {
        None => Check::no_data("Database budget", "not measured yet (every 5 minutes)."),
        Some(r) if r >= 0.95 => Check::new("Database budget", Level::Red, format!("The database is at {} of its budget.", percent(r))).link("/admin/settings/storage", "Storage settings"),
        Some(r) if r >= 0.8 => Check::new("Database budget", Level::Amber, format!("The database is at {} of its budget.", percent(r))).link("/admin/settings/storage", "Storage settings"),
        Some(r) => Check::new("Database budget", Level::Ok, format!("The database is at {} of its budget.", percent(r))),
    });

    out.push(match latest.as_ref().and_then(|s| s.sum(crate::db::sampler::METRIC_REDIS_RATIO)) {
        None => Check::no_data("Redis memory", "not measured yet, or Redis has no memory limit."),
        Some(r) if r >= 0.8 => Check::new("Redis memory", Level::Amber, format!("Redis is at {} of its memory; at 100% Prism stops reading the network.", percent(r))),
        Some(r) => Check::new("Redis memory", Level::Ok, format!("Redis is at {} of its memory.", percent(r))),
    });

    out.push(disk_check(i.disks.as_ref()));

    // Actionable-warnings design §5.1: only failed jobs Prism would still
    // run count; an account that is not there is not a failure.
    out.push(match i.failed_jobs {
        None => Check::no_data("Failed jobs", "the count did not finish in time."),
        Some(n) if n > 0 => Check::new(
            "Failed jobs",
            Level::Amber,
            format!("{} sync job{} failed after trying again several times. See what went wrong.", count(n as f64), if n == 1 { "" } else { "s" }),
        )
        .link("/admin/jobs", "See them"),
        Some(_) => {
            let unavailable = match i.unavailable_accounts {
                Some(u) if u > 0 => format!(
                    " {} account{} could not be read because {} deleted, taken down or deactivated; Prism reads {} again if {} back.",
                    count(u as f64),
                    if u == 1 { "" } else { "s" },
                    if u == 1 { "it is" } else { "they are" },
                    if u == 1 { "it" } else { "them" },
                    if u == 1 { "it comes" } else { "they come" },
                ),
                _ => String::new(),
            };
            Check::new("Failed jobs", Level::Ok, format!("No sync job has failed.{unavailable}")).link("/admin/jobs", "Jobs")
        }
    });

    out.push(match i.members_not_ready {
        None => Check::no_data("Members not ready", "the count did not finish in time."),
        Some(n) if n > 0 => Check::new(
            "Members not ready",
            Level::Amber,
            format!("{n} member{} not finished their first sync after a day.", if n == 1 { " has" } else { "s have" }),
        )
        .link("/admin/members", "Members"),
        Some(_) => Check::new("Members not ready", Level::Ok, "Every member added over a day ago has finished their first sync."),
    });

    out.push(match ring.held_for(FIVE_MIN, |s| Some(s.sum(crate::firehose::backfill::METRIC_AUTHZ_STUCK)? > 0.0)) {
        Held::NoData => Check::no_data("Stuck private sync", "nothing has been counted since Prism started, or Prism needs five minutes of readings first."),
        Held::Yes => Check::new("Stuck private sync", Level::Amber, "Private access for some accounts is stuck."),
        Held::No => Check::new("Stuck private sync", Level::Ok, "No account's private access is stuck."),
    });

    out.push(match ring.held_for(FIVE_MIN, |s| s.sum(crate::farsight::gate::METRIC_DOWN).map(|v| v >= 1.0)) {
        Held::NoData => Check::no_data("Farsight", "Farsight is not set up, or Prism needs five minutes of readings first."),
        Held::Yes => Check::new("Farsight", Level::Amber, "Farsight is not answering; block-list blocks may be missing.").link("/admin/farsight", "Blocks"),
        Held::No => Check::new("Farsight", Level::Ok, "Farsight is answering.").link("/admin/farsight", "Blocks"),
    });

    out.push(match ring.rise(FIFTEEN_MIN, |s| s.sum(crate::farsight::lookup::METRIC_AUTH_FAILED)) {
        None => Check::no_data("Farsight sign-in", "no refusal has been counted since Prism started."),
        Some(n) if n > 0.0 => Check::new("Farsight sign-in", Level::Red, "Farsight refuses Prism's token.").link("/admin/farsight", "Blocks"),
        Some(_) => Check::new("Farsight sign-in", Level::Ok, "Farsight accepts Prism's token."),
    });

    out.push(match ring.held_for(FIVE_MIN, |s| s.max(crate::farsight::gate::METRIC_WITHHELD).map(|v| v >= 1.0)) {
        Held::NoData => Check::no_data("Farsight fallback", "Farsight is not set up, or Prism needs five minutes of readings first."),
        Held::Yes => Check::new("Farsight fallback", Level::Amber, "The Farsight fallback is on but not being used.").link("/admin/farsight", "Blocks"),
        Held::No => Check::new("Farsight fallback", Level::Ok, "The Farsight fallback is off, or ready when needed."),
    });

    out.push(match latest.as_ref().and_then(|s| s.sum(crate::indexing::public_sweep::METRIC_STALLED)) {
        None => Check::no_data("Public data clean-up", "nothing is being deleted, or it has not been measured yet."),
        Some(v) if v >= 1.0 => Check::new("Public data clean-up", Level::Amber, "Deleting public data has stalled.").link("/admin/settings/storage", "Public data"),
        Some(_) => Check::new("Public data clean-up", Level::Ok, "Deleting public data is not stalled."),
    });

    out.push(if i.set_aside > 0 {
        Check::new("Settings", Level::Red, format!("{} saved setting{} could not be used.", i.set_aside, if i.set_aside == 1 { "" } else { "s" })).link("/admin/settings", "Settings")
    } else if i.waiting > 0 {
        Check::new("Settings", Level::Info, format!("{} change{} wait for a restart.", i.waiting, if i.waiting == 1 { "" } else { "s" })).link("/admin/settings", "Settings")
    } else {
        Check::new("Settings", Level::Ok, "Every saved setting is in use.")
    });

    out.push(system_accounts_check(latest.as_deref()));

    out.push(match i.schema {
        None => Check::no_data("Search", "the preparation task is not connected to these pages."),
        Some((true, true)) => Check::new("Search", Level::Ok, "Search is ready."),
        Some(_) => Check::new("Search", Level::Info, "Search is still being prepared."),
    });
    out
}

/// The System accounts check (system-accounts amendment 1 §7), from the
/// state gauge.
fn system_accounts_check(now: Option<&Snapshot>) -> Check {
    const NAME: &str = "System accounts";
    let var = crate::config::ENV_SYSTEM_ACCOUNTS_KEY_HEX;
    let state = now.and_then(|s| {
        crate::system_accounts::STATE_LABELS
            .into_iter()
            .find(|label| s.sum_where(crate::system_accounts::METRIC_STATE, "state", label).is_some_and(|v| v >= 0.5))
    });
    let locked = |what: String| {
        Check::new(
            NAME,
            Level::Red,
            format!("System accounts are off: {what}. Set the key they were sealed with and restart. Account deletions are recorded meanwhile and applied then."),
        )
    };
    match state {
        None => Check::no_data(NAME, "not reported yet."),
        Some("on") => Check::new(NAME, Level::Ok, "System accounts are on."),
        Some("catching_up") => Check::new(
            NAME,
            Level::Amber,
            "System accounts are catching up: applying the account changes seen while they were locked, and waiting for the identity checks of listed accounts. They serve again when done.",
        ),
        Some("locked_unset") => locked(format!("{var} is not set, and lists are stored")),
        Some("locked_mismatch") => locked(format!("{var} is not the key the stored lists were sealed with")),
        Some(_) => Check::new(NAME, Level::Ok, "System accounts are not in use."),
    }
}

/// The failed jobs Health counts (actionable-warnings design §5.1): those of
/// accounts Prism still follows, and `list_sync` jobs of owners with a list
/// still subscribed. A failed job Prism would not run again needs nobody.
pub const FAILED_JOBS_SQL: &str = "SELECT count(*) FROM backfill_jobs j WHERE j.state = 'failed' \
     AND (CASE WHEN j.depth = 'list_sync' \
               THEN EXISTS (SELECT 1 FROM subscribed_lists l WHERE l.owner_did = j.did AND l.state = 'active') \
               ELSE EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = j.did) END)";

/// Accounts Prism follows whose sync jobs ended `unavailable` (§5.1): a DID
/// can have more than one such row.
pub const UNAVAILABLE_ACCOUNTS_SQL: &str = "SELECT count(DISTINCT j.did) FROM backfill_jobs j WHERE j.state = 'unavailable' \
     AND EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = j.did)";

/// One count under a 2-second statement timeout (§6.6); `None` when it
/// fails or runs out of time, so its check says "no data".
pub async fn timed_count(pool: &PgPool, sql: &str) -> Option<i64> {
    let run = async {
        let mut tx = pool.begin().await?;
        sqlx::query("SET LOCAL statement_timeout = '2s'").execute(&mut *tx).await?;
        let n: i64 = sqlx::query_scalar(sql).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(n)
    };
    match run.await {
        Ok(n) => Some(n),
        Err(error) => {
            tracing::warn!(event = "admin_health_count_failed", %error, "a health count did not finish");
            None
        }
    }
}

/// The checks now: the probes and the two counts run at once, the rest is
/// read from memory and the ring. `banners` are the signed-in page's.
pub async fn gather(state: &AdminState, banners: Banners) -> Vec<Check> {
    let h = &state.health;
    let redis = async {
        match &h.redis {
            Some(r) => Some(probe_redis(r, PROBE_TIMEOUT).await),
            None => None,
        }
    };
    let (db, redis, failed_jobs, unavailable_accounts, members_not_ready) = tokio::join!(
        probe_db(&state.pool, PROBE_TIMEOUT),
        redis,
        timed_count(&state.pool, FAILED_JOBS_SQL),
        timed_count(&state.pool, UNAVAILABLE_ACCOUNTS_SQL),
        timed_count(&state.pool, "SELECT count(*) FROM members WHERE ready_at IS NULL AND added_at < now() - interval '24 hours'"),
    );
    let fh = &state.settings.running.firehose;
    let mut relays: Vec<String> = fh.relay_labels.values().cloned().collect();
    relays.sort();
    relays.dedup();
    let inputs = Inputs {
        db: Some(db),
        redis,
        audit_ok: h.audit.as_ref().map(|a| !a.latched()),
        gap_blocked: h.consumer.as_ref().map(|c| c.gap_blocked()),
        failed_jobs,
        unavailable_accounts,
        members_not_ready,
        set_aside: banners.set_aside,
        waiting: banners.waiting,
        schema: h.schema.as_ref().map(|s| (s.backfill_done(), s.indexes_ready())),
        lag_warn_entries: fh.lag_warn_entries,
        relays,
        private_relays: fh.private_relays.clone(),
        disks: Some(super::stats::read_disks(&state.disks)),
    };
    checks(&inputs, &h.ring)
}

impl super::AdminState {
    /// Take a snapshot of the watched series every [`SNAPSHOT_EVERY`], for
    /// as long as `serve` runs; nothing without a metrics handle.
    #[must_use]
    pub fn spawn_snapshots(&self) -> JoinHandle<()> {
        let health = self.health.clone();
        tokio::spawn(async move {
            let Some(metrics) = health.metrics.clone() else {
                return;
            };
            let mut tick = tokio::time::interval(SNAPSHOT_EVERY);
            loop {
                tick.tick().await;
                health.ring.push(Snapshot::from_text(&metrics.render(), Utc::now()));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// System-accounts amendment 1 T-A8: the check for each state.
    #[test]
    fn system_accounts_check_says_each_state() {
        let at = |state: &str| {
            let text: String = crate::system_accounts::STATE_LABELS
                .iter()
                .map(|l| format!("system_accounts_state{{state=\"{l}\"}} {}\n", u8::from(*l == state)))
                .collect();
            system_accounts_check(Some(&Snapshot::from_text(&text, Utc::now())))
        };
        assert_eq!(at("on").level, Level::Ok);
        assert_eq!(at("off").text, "System accounts are not in use.");
        let unset = at("locked_unset");
        assert_eq!(unset.level, Level::Red);
        assert!(unset.text.contains("APP_SYSTEM_ACCOUNTS_KEY_HEX is not set"), "{}", unset.text);
        let mismatch = at("locked_mismatch");
        assert_eq!(mismatch.level, Level::Red);
        assert!(mismatch.text.contains("is not the key the stored lists were sealed with"), "{}", mismatch.text);
        assert_eq!(at("catching_up").level, Level::Amber);
        assert_eq!(system_accounts_check(None).level, Level::NoData);
    }

    const TEXT: &str = "# TYPE firehose_worker_delay_seconds gauge\n\
        firehose_worker_delay_seconds{relay=\"bsky\"} 3\n\
        firehose_worker_delay_seconds{relay=\"micro\\\"cosm\"} +Inf\n\
        firehose_consumer_lag_entries 12\n\
        firehose_dead_letter_total{type=\"commit\"} 2\n\
        firehose_dead_letter_total{type=\"identity\"} 3\n\
        unwatched_total 9\n\
        \n\
        bad line\n";

    fn snap(text: &str, at_min: i64) -> Snapshot {
        Snapshot::from_text(text, DateTime::<Utc>::UNIX_EPOCH + TimeDelta::minutes(at_min))
    }

    #[test]
    fn parses_the_text_exposition() {
        let s = snap(TEXT, 0);
        assert_eq!(s.sum("firehose_consumer_lag_entries"), Some(12.0));
        assert_eq!(s.sum("firehose_dead_letter_total"), Some(5.0));
        assert_eq!(s.sum_where("firehose_dead_letter_total", "type", "identity"), Some(3.0));
        assert_eq!(s.sum_where("firehose_worker_delay_seconds", "relay", "micro\"cosm"), Some(f64::INFINITY));
        assert_eq!(s.sum("unwatched_total"), None, "only watched series are kept");
        assert_eq!(s.sum("db_budget_ratio"), None);
        let multi = parse("x{a=\"1\",b=\"two, three\"} 4.5", |_| true);
        assert_eq!(multi, vec![Sample { name: "x".into(), labels: vec![("a".into(), "1".into()), ("b".into(), "two, three".into())], value: 4.5 }]);
        let sources = Snapshot::from_text(
            "private_ingest_index_source_total{field=\"quote\",source=\"dedicated\"} 2\n\
             private_ingest_index_source_total{field=\"mentions\",source=\"dedicated\"} 3\n\
             private_ingest_index_source_total{field=\"media\",source=\"dedicated\"} 5\n",
            DateTime::<Utc>::UNIX_EPOCH,
        );
        let of = |field: &str| sources.sum_where_all(PRIVATE_INGEST_SOURCE, &[("field", field), ("source", "dedicated")]);
        assert_eq!((of("quote"), of("mentions"), of("media"), of("other")), (Some(2.0), Some(3.0), Some(5.0), None));
    }

    #[test]
    fn held_for_needs_the_whole_window() {
        let ring = Ring::default();
        let lagging = |s: &Snapshot| s.sum("firehose_consumer_lag_entries").map(|v| v > 10.0);
        for m in 0..5 {
            ring.push(snap(TEXT, m));
            assert_eq!(ring.held_for(FIVE_MIN, lagging), Held::NoData, "minute {m}");
        }
        ring.push(snap(TEXT, 5));
        assert_eq!(ring.held_for(FIVE_MIN, lagging), Held::Yes);
        ring.push(snap("firehose_consumer_lag_entries 1", 6));
        assert_eq!(ring.held_for(FIVE_MIN, lagging), Held::No);
        ring.push(snap("", 7));
        assert_eq!(ring.held_for(FIVE_MIN, lagging), Held::NoData, "a missing series is no data");
    }

    #[test]
    fn rise_counts_from_zero_then_from_the_window_start() {
        let ring = Ring::default();
        let dl = |s: &Snapshot| s.sum("firehose_dead_letter_total");
        assert_eq!(ring.rise(HOUR, dl), None);
        ring.push(snap("firehose_dead_letter_total 4", 0));
        assert_eq!(ring.rise(HOUR, dl), Some(4.0), "the counter started at zero with the process");
        for m in 1..=70 {
            ring.push(snap(&format!("firehose_dead_letter_total {}", 4 + m), m));
        }
        // The ring now holds minutes 11..=70; the hour before minute 70
        // starts at minute 10, which was dropped: the oldest kept is used.
        assert_eq!(ring.rise(HOUR, dl), Some(59.0));
        assert_eq!(ring.rise(FIFTEEN_MIN, dl), Some(15.0));
    }

    #[test]
    fn checks_say_no_data_never_fine_when_inputs_are_missing() {
        let ring = Ring::default();
        let all = checks(&Inputs::default(), &ring);
        for c in &all {
            if c.name != "Settings" {
                assert_eq!(c.level, Level::NoData, "{c:?}");
            }
        }
        assert!(all.iter().all(|c| !c.text.is_empty()));
    }

    #[test]
    fn checks_turn_amber_and_red() {
        let ring = Ring::default();
        let text = "firehose_consumer_group_present 0\nfirehose_consumer_lag_entries 500\n\
            firehose_worker_delay_seconds{relay=\"bsky\"} +Inf\ndb_budget_ratio 0.97\nredis_memory_ratio 0.5\n\
            firehose_dead_letter_total{type=\"commit\"} 2\nfarsight_down 0\nfarsight_fallback_withheld{reason=\"off\"} 0";
        for m in 0..=5 {
            ring.push(snap(text, m));
        }
        let inputs = Inputs {
            db: Some(true),
            redis: Some(false),
            audit_ok: Some(true),
            gap_blocked: Some(true),
            failed_jobs: Some(3),
            unavailable_accounts: Some(232),
            members_not_ready: Some(0),
            set_aside: 0,
            waiting: 2,
            schema: Some((true, false)),
            lag_warn_entries: 100,
            relays: vec!["bsky".into(), "microcosm".into()],
            private_relays: Vec::new(),
            disks: Some(disks(1_000_000_000_000, 15_000_000_000)),
        };
        let all = checks(&inputs, &ring);
        let get = |name: &str, text: &str| all.iter().find(|c| c.name == name && c.text.contains(text)).unwrap_or_else(|| panic!("{name}: {all:#?}")).level;
        assert_eq!(get("Redis", "cannot"), Level::Red);
        assert_eq!(get("Stream reader", "not reading"), Level::Red);
        assert_eq!(get("Held gap", "missed"), Level::Red);
        assert_eq!(all.iter().find(|c| c.name == "Held gap").and_then(|c| c.action), Some(Action::Resync));
        assert_eq!(get("Backlog", "500 records behind"), Level::Amber);
        assert_eq!(get("Relay delay", "bsky has never reported."), Level::Amber, "not a private relay");
        assert_eq!(get("Relay delay", "microcosm"), Level::NoData, "a configured relay with no series");
        assert_eq!(get("Unprocessed records", "2 records"), Level::Amber);
        assert_eq!(get("Database budget", "97%"), Level::Red);
        assert_eq!(get("Redis memory", "50%"), Level::Ok);
        assert_eq!(get("Failed jobs", "3 sync jobs failed after trying again several times"), Level::Amber);
        assert_eq!(get("Members not ready", "Every member"), Level::Ok);
        assert_eq!(get("Farsight", "answering"), Level::Ok);
        assert_eq!(get("Farsight fallback", "off"), Level::Ok);
        assert_eq!(get("Settings", "2 changes wait"), Level::Info);
        assert_eq!(get("Search", "still being prepared"), Level::Info);
        assert_eq!(get("Trim gap", "No data"), Level::NoData, "a counter never incremented is absent");
        assert_eq!(get("Disk", "The disk holding the database (Postgres) has 15.0 GB free (2% free)"), Level::Red);
    }

    /// #737: a private relay that has never reported is a Problem naming
    /// it; one that is behind is a Warning; one that keeps up is fine.
    #[test]
    fn a_private_relay_that_never_reported_is_a_problem() {
        let ring = Ring::default();
        let text = "firehose_worker_delay_seconds{relay=\"bsky\"} 2\n\
            firehose_worker_delay_seconds{relay=\"microcosm\"} +Inf\n\
            firehose_worker_delay_seconds{relay=\"community-a\"} 900";
        for m in 0..=5 {
            ring.push(snap(text, m));
        }
        let inputs = Inputs {
            relays: vec!["community-a".into(), "bsky".into(), "microcosm".into()],
            private_relays: vec!["bsky".into(), "microcosm".into(), "community-a".into()],
            ..Inputs::default()
        };
        let relay = |name: &str| {
            checks(&inputs, &ring)
                .into_iter()
                .find(|c| c.name == "Relay delay" && c.text.starts_with(name))
                .map(|c| (c.level, c.text))
                .expect("check")
        };
        let (level, text) = relay("microcosm");
        assert_eq!(level, Level::Red);
        assert!(text.starts_with("microcosm has never reported. Private posts wait for it, so every private read misses as stale."), "{text}");
        assert_eq!(relay("bsky").0, Level::Ok);
        assert_eq!(relay("community-a"), (Level::Amber, "community-a is 15 minutes behind; private posts newer than that miss as stale.".to_string()));
    }

    /// T-AW13 (actionable-warnings design §7.1): the Unprocessed records
    /// words, with and without records set aside.
    #[test]
    fn unprocessed_records_words() {
        let text_of = |metrics: &str| {
            let ring = Ring::default();
            ring.push(snap(metrics, 0));
            checks(&Inputs::default(), &ring).into_iter().find(|c| c.name == "Unprocessed records").map(|c| (c.level, c.text)).expect("check")
        };
        assert_eq!(text_of("firehose_dead_letter_total{type=\"commit\"} 2"), (Level::Amber, "2 records could not be processed in the last hour.".to_string()));
        assert_eq!(
            text_of("firehose_dead_letter_total{type=\"commit\"} 0\nfirehose_malformed_dropped_total{check=\"rev\"} 40\nfirehose_malformed_dropped_total{check=\"did\"} 2"),
            (Level::Ok, "Every record in the last hour was processed. 42 malformed records from accounts Prism does not keep were set aside.".to_string())
        );
        assert_eq!(
            text_of("firehose_dead_letter_total{type=\"commit\"} 0\nfirehose_malformed_dropped_total{check=\"rev\"} 1"),
            (Level::Ok, "Every record in the last hour was processed. 1 malformed record from accounts Prism does not keep was set aside.".to_string())
        );
        assert_eq!(text_of("firehose_dead_letter_total{type=\"commit\"} 0"), (Level::Ok, "Every record in the last hour was processed.".to_string()));
    }

    /// T-AW7 (actionable-warnings design §5.1): the Failed jobs words.
    #[test]
    fn failed_jobs_words() {
        let ring = Ring::default();
        let text = |failed: i64, unavailable: Option<i64>| {
            let inputs = Inputs { failed_jobs: Some(failed), unavailable_accounts: unavailable, ..Inputs::default() };
            let check = checks(&inputs, &ring).into_iter().find(|c| c.name == "Failed jobs").expect("check");
            (check.level, check.text)
        };
        assert_eq!(text(1, Some(5)), (Level::Amber, "1 sync job failed after trying again several times. See what went wrong.".to_string()));
        assert_eq!(
            text(0, Some(232)),
            (Level::Ok, "No sync job has failed. 232 accounts could not be read because they are deleted, taken down or deactivated; Prism reads them again if they come back.".to_string())
        );
        assert_eq!(
            text(0, Some(1)),
            (Level::Ok, "No sync job has failed. 1 account could not be read because it is deleted, taken down or deactivated; Prism reads it again if it comes back.".to_string())
        );
        assert_eq!(text(0, Some(0)), (Level::Ok, "No sync job has failed.".to_string()));
        assert_eq!(text(0, None), (Level::Ok, "No sync job has failed.".to_string()));
    }

    /// One disk holding the database, `total` bytes with `free` free.
    fn disks(total: u64, free: u64) -> crate::admin::stats::Disks {
        use crate::admin::stats::{Disk, Disks, HOLDS_POSTGRES, Space};
        Disks { disks: vec![Disk { holds: vec![HOLDS_POSTGRES], space: Space { total, used: total - free, free, id: (1, 1, 1) } }], not_visible: Vec::new() }
    }

    /// §9.2's Disk check: red under 2% free, amber under 10% or 5 GB,
    /// fine otherwise; no data without disks.
    #[test]
    fn the_disk_check() {
        let level = |d: Option<crate::admin::stats::Disks>| disk_check(d.as_ref()).level;
        const GB: u64 = 1_000_000_000;
        assert_eq!(level(Some(disks(1000 * GB, 15 * GB))), Level::Red, "1.5% free");
        assert_eq!(level(Some(disks(1000 * GB, 50 * GB))), Level::Amber, "5% free");
        assert_eq!(level(Some(disks(40 * GB, 4 * GB))), Level::Amber, "10% but under 5 GB");
        assert_eq!(level(Some(disks(100 * GB, 40 * GB))), Level::Ok);
        assert_eq!(level(None), Level::NoData);
        assert_eq!(level(Some(crate::admin::stats::Disks::default())), Level::NoData);
        let mut unseen = disks(100 * GB, 40 * GB);
        unseen.not_visible.push(crate::admin::stats::HOLDS_REDIS);
        assert!(disk_check(Some(&unseen)).text.ends_with("Prism cannot see the disk of Redis."));
    }

    #[test]
    fn counts_read_in_words() {
        assert_eq!(count(0.0), "0");
        assert_eq!(count(1234.4), "1,234");
        assert_eq!(count(1_234_567.0), "1,234,567");
        assert_eq!(minutes(30.0), "about a minute");
        assert_eq!(minutes(600.0), "10 minutes");
    }

    /// T-A13: every series a check or page reads is emitted somewhere in
    /// `src/` outside the admin module.
    #[test]
    fn t_a13_watched_series_are_emitted() {
        fn walk(dir: &std::path::Path, out: &mut String) {
            for entry in std::fs::read_dir(dir).expect("dir").flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if !path.ends_with("admin") {
                        walk(&path, out);
                    }
                } else if path.extension().is_some_and(|e| e == "rs") {
                    out.push_str(&std::fs::read_to_string(&path).expect("read"));
                }
            }
        }
        let mut source = String::new();
        walk(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut source);
        for name in WATCHED.iter().chain(crate::admin::stats::STATS_SERIES) {
            assert!(source.contains(&format!("\"{name}\"")), "{name} is not named outside src/admin");
        }
    }
}
