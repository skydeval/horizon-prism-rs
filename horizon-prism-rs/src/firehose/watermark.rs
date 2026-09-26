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

/// Metric: `now − watermark[r]`.
pub const METRIC_DELAY: &str = "firehose_worker_delay_seconds";
/// Metric: commits whose `time` did not parse.
pub const METRIC_TIME_UNPARSED: &str = "firehose_commit_time_unparsed_total";

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
}

impl Watermarks {
    #[must_use]
    pub fn new(labels: BTreeMap<String, String>, private_relays: Vec<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            labels: Arc::new(labels),
            private_relays: Arc::new(private_relays),
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
        for (label, w) in self.snapshot() {
            // A relay with no watermark is infinitely stale.
            let delay = w.map_or(f64::INFINITY, |w| secs(now - w));
            metrics::gauge!(METRIC_DELAY, "relay" => label).set(delay);
        }
    }

    /// Refresh the delay gauges every second and persist every 10 s.
    #[must_use]
    pub fn spawn_persister(&self, pool: PgPool) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut gauges = tokio::time::interval(GAUGE_INTERVAL);
            gauges.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut persist = tokio::time::interval(PERSIST_INTERVAL);
            persist.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = gauges.tick() => this.record_gauges(),
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
