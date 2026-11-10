//! What Prism's own database backends wait on, sampled once a second while
//! the full network is indexed or tried: the largest class says where the
//! writers' time goes (a disk read, a WAL flush, a lock, the next statement
//! from Prism, or running). Also the pool's connections in use.

use std::collections::BTreeMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::aggregations::ScopeState;

/// Metric: sampled backends by what they waited on.
pub const METRIC_WAITS: &str = "db_wait_samples_total";
/// How often the sampler looks.
pub const INTERVAL: Duration = Duration::from_secs(1);

/// What a sampled backend was doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WaitClass {
    /// Reading a table or index page from disk.
    DiskRead,
    /// Writing or flushing WAL, a commit's flush included.
    Wal,
    /// Other file I/O.
    OtherIo,
    /// A heavyweight lock: a row, an advisory lock, a table.
    Lock,
    /// A lightweight lock, a buffer's for example.
    LwLock,
    /// Inside a transaction, waiting for Prism's next statement.
    RoundTrip,
    /// Running, no wait.
    Running,
    Other,
}

impl WaitClass {
    pub const ALL: [Self; 8] = [Self::DiskRead, Self::Wal, Self::OtherIo, Self::Lock, Self::LwLock, Self::RoundTrip, Self::Running, Self::Other];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DiskRead => "disk_read",
            Self::Wal => "wal",
            Self::OtherIo => "other_io",
            Self::Lock => "lock",
            Self::LwLock => "lwlock",
            Self::RoundTrip => "round_trip",
            Self::Running => "running",
            Self::Other => "other",
        }
    }

    /// The class in words, after "the database spent most of its time".
    #[must_use]
    pub fn words(self) -> &'static str {
        match self {
            Self::DiskRead => "waiting for disk reads",
            Self::Wal => "waiting for its log to be written to disk",
            Self::OtherIo => "waiting for other disk work",
            Self::Lock => "waiting for locks other writers held",
            Self::LwLock => "waiting on its own internal locks",
            Self::RoundTrip => "waiting for Prism's next statement",
            Self::Running => "running statements",
            Self::Other => "waiting on other things",
        }
    }

    /// The class of one `pg_stat_activity` row, or `None` for an idle
    /// connection, which waits on nothing of the writers'.
    #[must_use]
    pub fn of(state: Option<&str>, wait_type: Option<&str>, wait_event: Option<&str>) -> Option<Self> {
        let state = state.unwrap_or("");
        if state == "idle" || state.is_empty() {
            return None;
        }
        Some(match (wait_type, wait_event) {
            (None, _) => Self::Running,
            (Some("IO"), Some(e)) if e.starts_with("DataFileRead") || e.starts_with("DataFilePrefetch") => Self::DiskRead,
            (Some("IO"), Some(e)) if e.starts_with("WAL") => Self::Wal,
            (Some("IO"), _) => Self::OtherIo,
            (Some("LWLock"), Some(e)) if e.starts_with("WAL") => Self::Wal,
            (Some("LWLock"), _) => Self::LwLock,
            (Some("Lock"), _) => Self::Lock,
            (Some("Client"), Some("ClientRead")) => Self::RoundTrip,
            _ => Self::Other,
        })
    }
}

/// The samples by class since the start, shared with whoever reports where
/// the time went.
#[derive(Debug, Default)]
pub struct WaitTally(Mutex<BTreeMap<WaitClass, u64>>);

impl WaitTally {
    pub fn add(&self, class: WaitClass, n: u64) {
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).entry(class).or_default() += n;
    }

    /// Every class's samples so far.
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<WaitClass, u64> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// The class with more than half of the samples between `before` and
    /// now, if one has.
    #[must_use]
    pub fn largest_since(&self, before: &BTreeMap<WaitClass, u64>) -> Option<WaitClass> {
        let now = self.snapshot();
        let delta: Vec<(WaitClass, u64)> = now.iter().map(|(c, n)| (*c, n.saturating_sub(before.get(c).copied().unwrap_or(0)))).collect();
        let total: u64 = delta.iter().map(|(_, n)| n).sum();
        delta.into_iter().max_by_key(|(_, n)| *n).filter(|(_, n)| total > 0 && n * 2 > total).map(|(c, _)| c)
    }
}

/// One sample of Prism's own backends, the sampler's own connection left
/// out, counted into `tally` and the metric.
///
/// # Errors
///
/// Database errors.
pub async fn sample(pool: &PgPool, tally: &WaitTally) -> Result<(), sqlx::Error> {
    let rows: Vec<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT state, wait_event_type, wait_event FROM pg_stat_activity \
         WHERE application_name = $1 AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
    )
    .bind(super::pool::APPLICATION_NAME)
    .fetch_all(pool)
    .await?;
    let mut counts: BTreeMap<WaitClass, u64> = BTreeMap::new();
    for (state, ty, ev) in &rows {
        if let Some(class) = WaitClass::of(state.as_deref(), ty.as_deref(), ev.as_deref()) {
            *counts.entry(class).or_default() += 1;
        }
    }
    for (class, n) in counts {
        tally.add(class, n);
        metrics::counter!(METRIC_WAITS, "class" => class.as_str()).increment(n);
    }
    Ok(())
}

/// The process's tally, which the sampler fills.
#[must_use]
pub fn tally() -> &'static WaitTally {
    static TALLY: LazyLock<WaitTally> = LazyLock::new(WaitTally::default);
    &TALLY
}

/// Register every class at zero, so a rule over the series has data.
pub fn register_series() {
    for class in WaitClass::ALL {
        metrics::counter!(METRIC_WAITS, "class" => class.as_str()).increment(0);
    }
}

/// Sample the pool every second, and Prism's backends while breadth is
/// Full or a trial records.
#[must_use]
pub fn spawn(pool: PgPool, scope: ScopeState) -> JoinHandle<()> {
    register_series();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            super::pool::sample_in_use(&pool);
            let on = scope.public_breadth() == crate::indexing::Breadth::Full || scope.read_guard().await.recording().is_some();
            if on && let Err(error) = sample(&pool, tally()).await {
                tracing::debug!(event = "db_wait_sample_failed", %error, "sampling the database's waits failed");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backends_are_classed_by_their_wait() {
        let c = |s, t, e| WaitClass::of(Some(s), t, e);
        assert_eq!(c("idle", Some("Client"), Some("ClientRead")), None, "an idle pooled connection waits on nothing of ours");
        assert_eq!(c("active", Some("IO"), Some("DataFileRead")), Some(WaitClass::DiskRead));
        assert_eq!(c("active", Some("IO"), Some("WALSync")), Some(WaitClass::Wal));
        assert_eq!(c("active", Some("LWLock"), Some("WALWrite")), Some(WaitClass::Wal));
        assert_eq!(c("active", Some("LWLock"), Some("BufferContent")), Some(WaitClass::LwLock));
        assert_eq!(c("active", Some("Lock"), Some("advisory")), Some(WaitClass::Lock));
        assert_eq!(c("idle in transaction", Some("Client"), Some("ClientRead")), Some(WaitClass::RoundTrip));
        assert_eq!(c("active", None, None), Some(WaitClass::Running));
    }

    #[test]
    fn the_largest_class_needs_more_than_half() {
        let t = WaitTally::default();
        t.add(WaitClass::Running, 10);
        let before = t.snapshot();
        t.add(WaitClass::DiskRead, 6);
        t.add(WaitClass::Wal, 4);
        assert_eq!(t.largest_since(&before), Some(WaitClass::DiskRead));
        let before = t.snapshot();
        t.add(WaitClass::DiskRead, 5);
        t.add(WaitClass::Wal, 5);
        assert_eq!(t.largest_since(&before), None, "an even split names none");
        assert_eq!(t.largest_since(&t.snapshot()), None);
    }

    /// The sampler sees a backend of Prism's waiting on a lock another
    /// holds.
    #[tokio::test]
    async fn a_backend_waiting_on_a_lock_is_sampled_as_one() {
        let Some(db) = crate::test_support::fresh_db("db_waits_lock").await else {
            return;
        };
        let options = (*db.pool.connect_options()).clone().application_name(super::super::pool::APPLICATION_NAME);
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect_with(options).await.expect("pool");
        let mut holder = pool.begin().await.expect("begin");
        sqlx::query("SELECT pg_advisory_xact_lock(4242)").execute(&mut *holder).await.expect("lock");
        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut tx = pool.begin().await.expect("begin");
                sqlx::query("SELECT pg_advisory_xact_lock(4242)").execute(&mut *tx).await.expect("lock");
                tx.commit().await.expect("commit");
            })
        };
        let tally = WaitTally::default();
        let mut seen = false;
        for _ in 0..50 {
            sample(&pool, &tally).await.expect("sample");
            if tally.snapshot().get(&WaitClass::Lock).copied().unwrap_or(0) > 0 {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(seen, "the waiting backend was sampled as a lock wait: {:?}", tally.snapshot());
        // The holder sits in its transaction between statements.
        assert!(tally.snapshot().get(&WaitClass::RoundTrip).copied().unwrap_or(0) > 0, "{:?}", tally.snapshot());
        holder.commit().await.expect("commit");
        waiter.await.expect("waiter");
        pool.close().await;
        db.drop_db().await;
    }
}
