//! Per-relay watermarks (design-v2 §5.3).
//!
//! - Per processed and acked commit entry from relay `r`:
//!   `contrib = min(stream-id ms, commit.time)`, and
//!   `watermark[r] = max(watermark[r], contrib)`.
//! - `private_watermark = min over private_relays of watermark[r]`; a relay
//!   with no watermark makes it `None` (fail-closed).
//! - Persisted to `worker_state['watermark:<label>']` every 10 s, restored at
//!   startup, and only ever advanced.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// Persistence interval (§5.3).
pub const PERSIST_INTERVAL: Duration = Duration::from_secs(10);
/// Delay-gauge refresh interval.
pub const GAUGE_INTERVAL: Duration = Duration::from_secs(1);
/// Label for a `_relay` URL absent from `firehose.relay_labels`.
pub const UNKNOWN_RELAY: &str = "other";
/// How often the private relays are checked for silence, the first time
/// this long after the start.
pub const PRIVATE_RELAY_CHECK_INTERVAL: Duration = Duration::from_secs(300);
/// A private relay whose watermark is further behind than this is silent.
pub const PRIVATE_RELAY_SILENT_AFTER: Duration = Duration::from_secs(300);

/// Metric: `now − watermark[r]`.
pub const METRIC_DELAY: &str = "firehose_worker_delay_seconds";
/// Metric: commits whose `time` did not parse.
pub const METRIC_TIME_UNPARSED: &str = "firehose_commit_time_unparsed_total";
/// Metric: 1 while private reads are degraded (the private watermark is
/// `None` or older than the freshness bound), else 0 (degraded-mode design
/// §8).
pub const METRIC_DEGRADED: &str = "private_read_degraded";

const KEY_PREFIX: &str = "watermark:";

/// The watermark contribution of one commit entry, if any.
#[must_use]
pub fn contribution(stream_id_ms: Option<i64>, time: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    let id_time = stream_id_ms.and_then(|ms| Utc.timestamp_millis_opt(ms).single())?;
    let Some(time) = time else {
        metrics::counter!(METRIC_TIME_UNPARSED).increment(1);
        return None;
    };
    Some(id_time.min(time))
}

/// The shared watermark state.
#[derive(Clone)]
pub struct Watermarks {
    inner: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
    /// `_relay` URL → label.
    labels: Arc<BTreeMap<String, String>>,
    private_relays: Arc<Vec<String>>,
    /// `kryphocron.oracle_freshness_bound_secs`, for the degraded gauge and
    /// its transition logs; without it neither is recorded.
    freshness_bound: Option<Duration>,
    /// The degraded state the gauge loop last saw.
    was_degraded: Arc<AtomicBool>,
}

impl Watermarks {
    #[must_use]
    pub fn new(labels: BTreeMap<String, String>, private_relays: Vec<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            labels: Arc::new(labels),
            private_relays: Arc::new(private_relays),
            freshness_bound: None,
            was_degraded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// With the freshness bound the degraded gauge and its transition logs
    /// test against (degraded-mode design §8).
    #[must_use]
    pub fn with_freshness_bound(mut self, bound: Duration) -> Self {
        self.freshness_bound = Some(bound);
        self
    }

    /// Whether private reads are degraded at `now` (degraded-mode design,
    /// Terms): the private watermark is `None` or more than `bound` behind.
    /// Global; reads nothing but the watermarks.
    #[must_use]
    pub fn degraded_at(&self, now: DateTime<Utc>, bound: Duration) -> bool {
        let bound = chrono::TimeDelta::from_std(bound).unwrap_or(chrono::TimeDelta::MAX);
        self.private_watermark().is_none_or(|w| now - w > bound)
    }

    /// The degraded gauge and its transition logs, against the configured
    /// bound: `private_read_degraded_started` (with the private relays
    /// holding reads back) and `_ended`, once per change.
    fn record_degraded(&self, now: DateTime<Utc>) {
        let Some(bound) = self.freshness_bound else {
            return;
        };
        let degraded = self.degraded_at(now, bound);
        metrics::gauge!(METRIC_DEGRADED).set(if degraded { 1.0 } else { 0.0 });
        if self.was_degraded.swap(degraded, Ordering::SeqCst) == degraded {
            return;
        }
        if degraded {
            let relays: Vec<String> = self.silent_private_relays(now, bound).into_iter().map(|(r, _)| r).collect();
            tracing::warn!(
                event = "private_read_degraded_started",
                relays = %relays.join(","),
                "private reads are degraded: the private watermark is behind; reads fall back to per-repo head checks"
            );
        } else {
            tracing::info!(event = "private_read_degraded_ended", "private reads are no longer degraded");
        }
    }

    /// The label of a `_relay` URL.
    #[must_use]
    pub fn label_for(&self, relay: Option<&str>) -> &str {
        relay
            .and_then(|r| self.labels.get(r))
            .map_or(UNKNOWN_RELAY, String::as_str)
    }

    /// Every configured label, sorted.
    #[must_use]
    pub fn labels(&self) -> Vec<String> {
        let mut v: Vec<String> = self.labels.values().cloned().collect();
        v.sort();
        v.dedup();
        v
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, DateTime<Utc>>> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Advance one relay's watermark.
    pub fn observe(&self, label: &str, contrib: DateTime<Utc>) {
        let mut m = self.lock();
        let w = m.entry(label.to_string()).or_insert(contrib);
        if contrib > *w {
            *w = contrib;
        }
    }

    #[must_use]
    pub fn get(&self, label: &str) -> Option<DateTime<Utc>> {
        self.lock().get(label).copied()
    }

    /// `min over private_relays`, or `None` if any has no watermark.
    #[must_use]
    pub fn private_watermark(&self) -> Option<DateTime<Utc>> {
        let m = self.lock();
        self.private_relays
            .iter()
            .map(|r| m.get(r).copied())
            .try_fold(None::<DateTime<Utc>>, |acc, w| {
                let w = w?;
                Some(Some(acc.map_or(w, |a| a.min(w))))
            })
            .flatten()
    }

    /// The private relays holding private reads back as of `now`: each one
    /// with no watermark (`None`: it has never reported) or a watermark
    /// more than `silent_after` behind. Private reads wait for the slowest
    /// private relay, so any one of these makes them miss as stale.
    #[must_use]
    pub fn silent_private_relays(&self, now: DateTime<Utc>, silent_after: Duration) -> Vec<(String, Option<DateTime<Utc>>)> {
        let limit = chrono::TimeDelta::from_std(silent_after).unwrap_or(chrono::TimeDelta::MAX);
        let m = self.lock();
        self.private_relays
            .iter()
            .map(|r| (r.clone(), m.get(r).copied()))
            .filter(|(_, w)| w.is_none_or(|w| now - w > limit))
            .collect()
    }

    /// Log a warning for each private relay [`Self::silent_private_relays`]
    /// names; returns how many were named.
    pub fn warn_silent_private_relays(&self, now: DateTime<Utc>) -> usize {
        let silent = self.silent_private_relays(now, PRIVATE_RELAY_SILENT_AFTER);
        for (relay, w) in &silent {
            match w {
                None => tracing::warn!(
                    event = "private_relay_never_reported",
                    relay = %relay,
                    "relay {relay} is in firehose.private_relays but has never reported; every private read misses as stale until it does. \
                     Start its relay reader, or remove it from firehose.private_relays"
                ),
                Some(w) => tracing::warn!(
                    event = "private_relay_silent",
                    relay = %relay,
                    behind_secs = (now - *w).num_seconds(),
                    "relay {relay} is in firehose.private_relays but its last event is {} s old; newer private posts miss as stale until it catches up",
                    (now - *w).num_seconds()
                ),
            }
        }
        silent.len()
    }

    /// The startup check: after [`Self::restore`], a private relay with no
    /// persisted watermark while another relay has one has never reported
    /// on this database. A fresh database (no watermark at all) waits for
    /// the periodic check instead.
    pub fn warn_unreported_at_start(&self) -> usize {
        let missing: Vec<String> = {
            let m = self.lock();
            if m.is_empty() {
                return 0;
            }
            self.private_relays.iter().filter(|r| !m.contains_key(*r)).cloned().collect()
        };
        for relay in &missing {
            tracing::warn!(
                event = "private_relay_never_reported",
                relay = %relay,
                "relay {relay} is in firehose.private_relays but has never reported on this database; every private read misses as stale until it does. \
                 Start its relay reader, or remove it from firehose.private_relays"
            );
        }
        missing.len()
    }

    /// Every configured label with its watermark (for `/xrpc/_health`).
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<String, Option<DateTime<Utc>>> {
        let m = self.lock();
        let mut out: BTreeMap<String, Option<DateTime<Utc>>> =
            self.labels().into_iter().map(|l| (l, None)).collect();
        for (k, v) in m.iter() {
            out.insert(k.clone(), Some(*v));
        }
        out
    }

    /// Whether any watermark has been persisted (stream-evidence gap check,
    /// §6.11.4 item 3).
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn any_persisted(pool: &PgPool) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM worker_state WHERE key LIKE 'watermark:%')")
            .fetch_one(pool)
            .await
    }

    /// Restore persisted watermarks.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn restore(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let rows: Vec<(String, Option<String>)> =
            sqlx::query_as("SELECT key, value #>> '{}' FROM worker_state WHERE key LIKE 'watermark:%'")
                .fetch_all(pool)
                .await?;
        for (key, value) in rows {
            if let (Some(label), Some(t)) = (
                key.strip_prefix(KEY_PREFIX),
                value.as_deref().and_then(|v| DateTime::parse_from_rfc3339(v).ok()),
            ) {
                self.observe(label, t.with_timezone(&Utc));
            }
        }
        Ok(())
    }

    /// Persist every watermark, never moving a persisted value backwards.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn persist(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let entries: Vec<(String, DateTime<Utc>)> = self.lock().iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (label, t) in entries {
            sqlx::query(
                "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb($2::text), now()) \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now() \
                 WHERE (worker_state.value #>> '{}')::timestamptz < (EXCLUDED.value #>> '{}')::timestamptz",
            )
            .bind(format!("{KEY_PREFIX}{label}"))
            .bind(t.to_rfc3339_opts(SecondsFormat::Millis, true))
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    fn record_gauges(&self) {
        let now = Utc::now();
        self.record_degraded(now);
        for (label, w) in self.snapshot() {
            // A relay with no watermark is infinitely stale.
            let delay = w.map_or(f64::INFINITY, |w| secs(now - w));
            metrics::gauge!(METRIC_DELAY, "relay" => label).set(delay);
        }
    }

    /// Refresh the delay gauges every second, persist every 10 s, and warn
    /// about silent private relays every [`PRIVATE_RELAY_CHECK_INTERVAL`]
    /// (the first time one interval after the start).
    #[must_use]
    pub fn spawn_persister(&self, pool: PgPool) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut gauges = tokio::time::interval(GAUGE_INTERVAL);
            gauges.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut persist = tokio::time::interval(PERSIST_INTERVAL);
            persist.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut relays = tokio::time::interval_at(
                tokio::time::Instant::now() + PRIVATE_RELAY_CHECK_INTERVAL,
                PRIVATE_RELAY_CHECK_INTERVAL,
            );
            relays.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = gauges.tick() => this.record_gauges(),
                    _ = relays.tick() => {
                        this.warn_silent_private_relays(Utc::now());
                    }
                    _ = persist.tick() => {
                        if let Err(error) = this.persist(&pool).await {
                            tracing::warn!(event = "watermark_persist_failed", %error, "persisting watermarks failed");
                        }
                    }
                }
            }
        })
    }
}

#[allow(clippy::cast_precision_loss)]
fn secs(d: chrono::TimeDelta) -> f64 {
    d.num_milliseconds() as f64 / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).expect("time").with_timezone(&Utc)
    }

    fn marks() -> Watermarks {
        let labels = [("wss://a", "bsky"), ("wss://b", "microcosm"), ("wss://c", "community-a")]
            .into_iter()
            .map(|(u, l)| (u.to_string(), l.to_string()))
            .collect();
        Watermarks::new(labels, vec!["bsky".into(), "microcosm".into()])
    }

    /// Degraded-mode design §8: degraded while any private relay has no
    /// watermark or the minimum is past the bound; the transition is
    /// recorded once per change.
    #[test]
    fn degraded_follows_the_private_watermark() {
        let w = marks().with_freshness_bound(Duration::from_secs(120));
        let now = t("2026-09-26T12:00:00Z");
        let bound = Duration::from_secs(120);
        assert!(w.degraded_at(now, bound), "no watermark: degraded");
        w.observe("bsky", t("2026-09-26T11:59:30Z"));
        assert!(w.degraded_at(now, bound), "microcosm never reported");
        w.observe("microcosm", t("2026-09-26T11:57:00Z"));
        assert!(w.degraded_at(now, bound), "three minutes behind");
        w.observe("microcosm", t("2026-09-26T11:58:00Z"));
        assert!(!w.degraded_at(now, bound), "exactly at the bound is fresh");
        // community-a is not private and never counts.
        w.observe("community-a", t("2020-01-01T00:00:00Z"));
        assert!(!w.degraded_at(now, bound));
        w.record_degraded(now);
        assert!(!w.was_degraded.load(Ordering::SeqCst));
        w.record_degraded(t("2026-09-26T12:05:00Z"));
        assert!(w.was_degraded.load(Ordering::SeqCst));
        // Without a bound nothing is recorded.
        let unbound = marks();
        unbound.record_degraded(now);
        assert!(!unbound.was_degraded.load(Ordering::SeqCst));
    }

    #[test]
    fn contribution_is_the_minimum() {
        let id_ms = t("2026-09-26T12:00:00Z").timestamp_millis();
        // A future-dated commit is bounded by the XADD time.
        assert_eq!(contribution(Some(id_ms), Some(t("2030-01-01T00:00:00Z"))), Some(t("2026-09-26T12:00:00Z")));
        // A replayed old commit keeps the watermark behind.
        assert_eq!(contribution(Some(id_ms), Some(t("2026-09-26T11:00:00Z"))), Some(t("2026-09-26T11:00:00Z")));
        assert_eq!(contribution(Some(id_ms), None), None);
    }

    #[test]
    fn watermarks_only_advance_and_private_is_the_min() {
        let w = marks();
        assert_eq!(w.label_for(Some("wss://a")), "bsky");
        assert_eq!(w.label_for(Some("wss://x")), UNKNOWN_RELAY);
        assert_eq!(w.private_watermark(), None);
        w.observe("bsky", t("2026-09-26T12:00:00Z"));
        w.observe("bsky", t("2026-09-26T11:00:00Z"));
        assert_eq!(w.get("bsky"), Some(t("2026-09-26T12:00:00Z")));
        // microcosm has no watermark: private is fail-closed.
        assert_eq!(w.private_watermark(), None);
        w.observe("microcosm", t("2026-09-26T11:30:00Z"));
        assert_eq!(w.private_watermark(), Some(t("2026-09-26T11:30:00Z")));
        // community-a is not a private relay.
        w.observe("community-a", t("2020-01-01T00:00:00Z"));
        assert_eq!(w.private_watermark(), Some(t("2026-09-26T11:30:00Z")));
        let snap = w.snapshot();
        assert_eq!(snap.len(), 3);
    }

    /// #737: a private relay that never reports, or stops reporting, is
    /// named; a non-private one never is.
    #[test]
    fn silent_private_relays_are_named() {
        let w = marks();
        let now = t("2026-09-26T12:00:00Z");
        let five = PRIVATE_RELAY_SILENT_AFTER;
        assert_eq!(w.warn_unreported_at_start(), 0, "a fresh database waits for the periodic check");
        assert_eq!(w.silent_private_relays(now, five), vec![("bsky".to_string(), None), ("microcosm".to_string(), None)]);
        w.observe("bsky", t("2026-09-26T11:59:00Z"));
        w.observe("community-a", t("2020-01-01T00:00:00Z"));
        // The silonew case: bsky reports, microcosm never does.
        assert_eq!(w.silent_private_relays(now, five), vec![("microcosm".to_string(), None)]);
        assert_eq!(w.warn_unreported_at_start(), 1);
        assert_eq!(w.warn_silent_private_relays(now), 1);
        w.observe("microcosm", t("2026-09-26T11:50:00Z"));
        assert_eq!(w.warn_unreported_at_start(), 0);
        assert_eq!(w.silent_private_relays(now, five), vec![("microcosm".to_string(), Some(t("2026-09-26T11:50:00Z")))], "ten minutes behind");
        w.observe("microcosm", t("2026-09-26T11:58:00Z"));
        assert!(w.silent_private_relays(now, five).is_empty());
        assert_eq!(w.warn_silent_private_relays(now), 0);
    }

    mod pg {
        use super::*;
        use crate::test_support::fresh_db;

        #[tokio::test]
        async fn persist_and_restore_never_regress() {
            let Some(db) = fresh_db("watermark_persist_and_restore").await else {
                return;
            };
            let w = marks();
            assert!(!Watermarks::any_persisted(&db.pool).await.expect("query"));
            w.observe("bsky", t("2026-09-26T12:00:00Z"));
            w.persist(&db.pool).await.expect("persist");
            // A fresh process with an older in-memory value cannot regress it.
            let older = marks();
            older.observe("bsky", t("2026-09-26T10:00:00Z"));
            older.persist(&db.pool).await.expect("persist");
            let restored = marks();
            restored.restore(&db.pool).await.expect("restore");
            assert_eq!(restored.get("bsky"), Some(t("2026-09-26T12:00:00Z")));
            assert!(Watermarks::any_persisted(&db.pool).await.expect("query"));
            db.drop_db().await;
        }
    }
}
