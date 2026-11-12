//! Local counts from the count log.
//!
//! The count triggers append every change to `count_log` and
//! `count_log_tags` in the row's own transaction; nothing writes a count row
//! at once. The [`Folder`] applies the log to `post_counts`, `actor_counts`
//! and `tag_activity_hourly` every `[counts] fold_interval_ms`, so counts run
//! up to about that far behind ingest.
//!
//! - A **fold** is one transaction calling `prism_fold_counts`: under the
//!   counts lock it deletes one slice of the log and applies it, likes and
//!   reposts by account, in the same snapshot.
//! - A **round** folds every slice once. A log larger than
//!   `fold_slice_rows` is cut into a power of two of slices by account, so
//!   one fold, and so a wait for the counts lock, stays short.
//! - A **settle** ([`settle`]) is a round that waits for the lock: when it
//!   returns, everything committed before it began is counted.
//! - Every other writer of a count row settles first, then takes the counts
//!   lock ([`lock`]) for its one count statement, which also drops or keeps
//!   the key's pending log rows.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sqlx::{PgConnection, PgPool};
use tokio::task::JoinHandle;

use crate::config::CountsConfig;

/// Metric: seconds since the start of the last completed round.
pub const METRIC_BEHIND: &str = "counts_behind_seconds";
/// Metric: the time of one slice's fold.
pub const METRIC_FOLD_SECONDS: &str = "counts_fold_seconds";
/// Metric: the slices of the last round.
pub const METRIC_SLICES: &str = "counts_fold_slices";
/// Metric: log rows folded, by `table` (`count_log`, `count_log_tags`).
pub const METRIC_FOLDED: &str = "counts_folded_total";
/// Metric: folds that found the counts lock taken.
pub const METRIC_SKIPPED: &str = "counts_fold_skipped_total";
/// Metric: rounds that failed.
pub const METRIC_FAILURES: &str = "counts_fold_failures_total";
/// Metric: the log's rows at the start of the last round.
pub const METRIC_LOG_ROWS: &str = "counts_log_rows";

/// The counts lock's two keys (`prism_fold_lock()`).
pub const LOCK_KEYS: (i64, i64) = (1_129_270_868, 1);
/// A round has at most this many slices.
pub const MAX_SLICES: i32 = 1024;
/// `[counts] fold_interval_ms` above this warns.
pub const INTERVAL_WARN: Duration = Duration::from_secs(60);
/// A fold that found the lock taken tries again after this.
const RETRY: Duration = Duration::from_millis(50);
/// After this long of tries the folder waits for the lock instead.
const WAIT_AFTER: Duration = Duration::from_secs(1);
/// The folder vacuums the log this often.
const VACUUM_EVERY: Duration = Duration::from_secs(10);
/// A failed round is retried after a backoff doubling up to this.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
/// The last round at shutdown runs at most this long.
pub const FINAL_ROUND_CAP: Duration = Duration::from_secs(5);
/// Behind more than this many intervals, and more than [`WARN_FLOOR`], for
/// [`WARN_HELD`]: Health warns.
pub const WARN_INTERVALS: u32 = 10;
pub const WARN_FLOOR: Duration = Duration::from_secs(30);
pub const WARN_HELD: Duration = Duration::from_secs(60);
/// A transaction open this long is named as keeping the log from being
/// cleaned.
pub const OLD_SNAPSHOT: Duration = Duration::from_secs(300);
/// The rates Health shows are taken over this long.
const RATE_WINDOW: Duration = Duration::from_secs(60);

/// `fold_slice_rows` for settles, set by the folder from the config.
static SLICE_ROWS: AtomicU64 = AtomicU64::new(100_000);

/// `fold_interval_ms`, set by the folder from the config.
static INTERVAL_MS: AtomicU64 = AtomicU64::new(1000);

/// The slice rows settles use.
#[must_use]
pub fn slice_rows() -> u64 {
    SLICE_ROWS.load(Ordering::Relaxed)
}

/// The folder's interval, as configured.
#[must_use]
pub fn fold_interval() -> Duration {
    Duration::from_millis(INTERVAL_MS.load(Ordering::Relaxed))
}

/// Each database's folder status and the most it was behind since the last
/// [`take_behind`], seconds.
type Registry = Vec<(String, Arc<FoldStatus>, f64)>;
static REGISTRY: Mutex<Registry> = Mutex::new(Vec::new());

fn note_behind(db: &str, secs: f64) {
    let mut r = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(e) = r.iter_mut().find(|e| e.0 == db) {
        e.2 = e.2.max(secs);
    }
}

/// The most the pool's database's counts were behind since the last call,
/// in seconds, now included; `None` when no folder runs for it in this
/// process or none has folded yet.
#[must_use]
pub fn take_behind(pool: &PgPool) -> Option<f64> {
    let db = database(pool);
    let mut r = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
    let e = r.iter_mut().find(|e| e.0 == db)?;
    let now = e.1.view().behind.map(|b| b.as_secs_f64());
    let most = now.map(|n| n.max(e.2));
    e.2 = 0.0;
    most
}

/// The smallest power of two of slices that puts at most `slice_rows` of
/// `rows` in a slice, at most [`MAX_SLICES`].
#[must_use]
pub fn slices_for(rows: i64, slice_rows: u64) -> i32 {
    let per = slice_rows.max(1);
    let rows = u64::try_from(rows).unwrap_or(0);
    let mut s: u64 = 1;
    while s < u64::from(MAX_SLICES.unsigned_abs()) && rows.div_ceil(s) > per {
        s *= 2;
    }
    i32::try_from(s).unwrap_or(MAX_SLICES)
}

/// What one round did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Round {
    /// Rows of `count_log` and `count_log_tags` folded.
    pub log_rows: i64,
    pub tag_rows: i64,
    /// The round's slices of each table.
    pub slices: i32,
    pub tag_slices: i32,
    /// Folds that found the lock taken and tried again.
    pub skipped: u64,
    /// The tables' rows when the round began.
    pub log_before: i64,
    pub tags_before: i64,
}

impl Round {
    #[must_use]
    pub fn folded(&self) -> i64 {
        self.log_rows + self.tag_rows
    }
}

fn is_lock_timeout(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(d) if d.code().as_deref() == Some("55P03"))
}

/// One fold transaction; `None` when the lock was taken (not waiting).
async fn fold_once(pool: &PgPool, wait: bool, slices: i32, tag_slices: i32, slice: i32) -> Result<Option<(i64, i64)>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let (log_rows, tag_rows): (i64, i64) = sqlx::query_as("SELECT log_rows, tag_rows FROM prism_fold_counts($1, $2, $3, $4)")
        .bind(wait)
        .bind(slices)
        .bind(slice)
        .bind(tag_slices)
        .fetch_one(&mut *tx)
        .await?;
    if log_rows < 0 {
        tx.rollback().await?;
        return Ok(None);
    }
    #[cfg(test)]
    if crate::test_support::hooks::injected(crate::test_support::hooks::FOLD_FAILS, pool.connect_options().get_database().unwrap_or_default()) {
        tx.rollback().await?;
        return Err(sqlx::Error::Protocol("injected fold failure".to_string()));
    }
    tx.commit().await?;
    Ok(Some((log_rows, tag_rows)))
}

/// One slice, folded: tried, and retried after [`RETRY`] while the lock is
/// taken; after [`WAIT_AFTER`] of tries, waited for. With `wait`, waited for
/// at once. A wait that ends at the lock timeout counts as a try, unless
/// `wait`.
async fn fold_slice(pool: &PgPool, wait: bool, round: &mut Round, slice: i32) -> Result<(), sqlx::Error> {
    let mut first_try: Option<Instant> = None;
    loop {
        let waiting = wait || first_try.is_some_and(|t| t.elapsed() >= WAIT_AFTER);
        let started = Instant::now();
        match fold_once(pool, waiting, round.slices, round.tag_slices, slice).await {
            Ok(Some((log_rows, tag_rows))) => {
                metrics::histogram!(METRIC_FOLD_SECONDS).record(started.elapsed().as_secs_f64());
                metrics::counter!(METRIC_FOLDED, "table" => "count_log").increment(u64::try_from(log_rows).unwrap_or(0));
                metrics::counter!(METRIC_FOLDED, "table" => "count_log_tags").increment(u64::try_from(tag_rows).unwrap_or(0));
                round.log_rows += log_rows;
                round.tag_rows += tag_rows;
                return Ok(());
            }
            Ok(None) => {}
            Err(e) if !wait && is_lock_timeout(&e) => {}
            Err(e) => return Err(e),
        }
        round.skipped += 1;
        metrics::counter!(METRIC_SKIPPED).increment(1);
        first_try.get_or_insert(started);
        if !waiting {
            tokio::time::sleep(RETRY).await;
        }
    }
}

/// One round: the log's rows counted, the slices set from them, and every
/// slice folded in turn, each its own transaction.
///
/// # Errors
///
/// Database errors of a fold, or of the count.
pub async fn round(pool: &PgPool, wait: bool, slice_rows: u64) -> Result<Round, sqlx::Error> {
    let (log_before, tags_before): (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM count_log), (SELECT count(*) FROM count_log_tags)").fetch_one(pool).await?;
    let mut r = Round { slices: slices_for(log_before, slice_rows), tag_slices: slices_for(tags_before, slice_rows), log_before, tags_before, ..Round::default() };
    for slice in 0..r.slices.max(r.tag_slices) {
        fold_slice(pool, wait, &mut r, slice).await?;
    }
    Ok(r)
}

/// A settle: one round that waits for the counts lock. When it returns,
/// everything committed before it began is in the count tables. Run it
/// outside any transaction of the caller's.
///
/// # Errors
///
/// Database errors.
pub async fn settle(pool: &PgPool) -> Result<Round, sqlx::Error> {
    round(pool, true, slice_rows()).await
}

/// [`settle`] for a direct writer of count rows whose statement is right
/// whatever the log holds (a recomputer, a key deleter): a failure is
/// logged and the caller goes on, which is how counts recover from a
/// folder that keeps failing.
pub async fn settle_or_go_on(pool: &PgPool, what: &'static str) {
    if let Err(error) = settle(pool).await {
        tracing::warn!(event = "counts_settle_failed", what, %error, "folding the count log first failed; going on");
    }
    #[cfg(test)]
    crate::test_support::hooks::pause(crate::test_support::hooks::COUNTS_AFTER_SETTLE, pool.connect_options().get_database().unwrap_or_default()).await;
}

/// Take the counts lock in the caller's transaction: its first statement
/// after any DID or URI lock, before it writes any row. It waits at most
/// one slice's fold.
///
/// # Errors
///
/// Database errors, the lock timeout among them.
pub async fn lock(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT prism_fold_lock()").execute(conn).await?;
    Ok(())
}

/// Vacuum the log, skipping a table another session holds locked.
pub async fn vacuum(pool: &PgPool) {
    if let Err(error) = sqlx::raw_sql("VACUUM (SKIP_LOCKED) count_log, count_log_tags").execute(pool).await {
        tracing::debug!(event = "counts_vacuum_failed", %error, "vacuuming the count log failed");
    }
}

/// The last round at shutdown, after the lanes stopped: at most
/// [`FINAL_ROUND_CAP`].
pub async fn final_round(pool: &PgPool) {
    match tokio::time::timeout(FINAL_ROUND_CAP, settle(pool)).await {
        Ok(Ok(r)) => tracing::info!(event = "counts_final_round", folded = r.folded(), "counts folded at shutdown"),
        Ok(Err(error)) => tracing::warn!(event = "counts_final_round_failed", %error, "the last counts fold failed; the log keeps the changes"),
        Err(_) => tracing::warn!(event = "counts_final_round_timeout", "the last counts fold did not finish in time; the log keeps the changes"),
    }
}

/// Before a count read in a test build: settle, unless the test set
/// [`COUNTS_LAG`](crate::test_support::hooks::COUNTS_LAG) for the database.
/// So a test reads what production leaves after a fold. Nothing in a
/// production build.
#[cfg(test)]
pub async fn before_read(pool: &PgPool) {
    if crate::test_support::hooks::armed(crate::test_support::hooks::COUNTS_LAG, &database(pool)) {
        return;
    }
    let _ = settle(pool).await;
}

/// Before a count read in a test build: nothing in a production build.
#[cfg(not(test))]
#[allow(clippy::unused_async)]
pub async fn before_read(_pool: &PgPool) {}

/// What a direct writer of count rows in this process is, while it holds
/// or waits for the counts lock, by database: Health names it when counts
/// fall behind.
static HOLDERS: Mutex<Vec<(String, &'static str)>> = Mutex::new(Vec::new());

/// Registered while held: see [`holding`].
pub struct Holding(String, &'static str);

fn database(pool: &PgPool) -> String {
    pool.connect_options().get_database().unwrap_or_default().to_string()
}

/// Register `what` (words such as "a counts repair is running") for the
/// pool's database until the guard drops.
#[must_use]
pub fn holding(pool: &PgPool, what: &'static str) -> Holding {
    let db = database(pool);
    HOLDERS.lock().unwrap_or_else(PoisonError::into_inner).push((db.clone(), what));
    Holding(db, what)
}

impl Drop for Holding {
    fn drop(&mut self) {
        let mut h = HOLDERS.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(i) = h.iter().position(|(db, w)| *db == self.0 && *w == self.1) {
            h.remove(i);
        }
    }
}

fn holder(pool: &PgPool) -> Option<&'static str> {
    let db = database(pool);
    HOLDERS.lock().unwrap_or_else(PoisonError::into_inner).iter().find(|(d, _)| *d == db).map(|(_, w)| *w)
}

/// The folder's state, for Health and the admin pages.
#[derive(Default)]
pub struct FoldStatus(Mutex<Inner>);

#[derive(Default)]
struct Inner {
    interval: Duration,
    /// The start of the last completed round: everything committed before
    /// it is counted.
    counted_to: Option<Instant>,
    /// A fold is running now.
    folding: bool,
    failing_since: Option<Instant>,
    last_error: Option<String>,
    /// Since when counts have been behind more than the warning's bound.
    over_since: Option<Instant>,
    /// Recent rounds: their start, the log's rows then, the rows folded.
    recent: VecDeque<(Instant, i64, i64)>,
    /// Rows folded since the start.
    folded: i64,
}

/// The folder's state now.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusView {
    pub interval: Duration,
    /// `None` until the first round since the start.
    pub behind: Option<Duration>,
    /// How long counts have been behind the warning's bound.
    pub over_for: Option<Duration>,
    /// How long rounds have failed, and the last error.
    pub failing: Option<(Duration, String)>,
    /// Changes a second the folder folded and ingest logged, over the last
    /// minute of rounds.
    pub fold_rate: Option<f64>,
    pub ingest_rate: Option<f64>,
    /// The log's rows when the last round began.
    pub log_rows: Option<i64>,
    pub folding: bool,
}

impl FoldStatus {
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self(Mutex::new(Inner { interval, ..Inner::default() }))
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn started(&self) {
        self.inner().folding = true;
    }

    /// A round that began at `start` completed.
    pub fn done(&self, start: Instant, r: &Round) {
        let mut i = self.inner();
        i.folding = false;
        i.counted_to = Some(start);
        i.failing_since = None;
        i.last_error = None;
        i.folded += r.folded();
        i.recent.push_back((start, r.log_before + r.tags_before, r.folded()));
        while i.recent.front().is_some_and(|(t, _, _)| start.duration_since(*t) > RATE_WINDOW) && i.recent.len() > 2 {
            i.recent.pop_front();
        }
    }

    /// A round failed.
    pub fn failed(&self, error: &str) {
        let mut i = self.inner();
        i.folding = false;
        i.failing_since.get_or_insert_with(Instant::now);
        i.last_error = Some(error.to_string());
    }

    /// The bound past which Health warns: ten intervals, and at least 30 s.
    #[must_use]
    pub fn warn_bound(interval: Duration) -> Duration {
        (interval * WARN_INTERVALS).max(WARN_FLOOR)
    }

    /// The state now, at `now`.
    #[must_use]
    pub fn view_at(&self, now: Instant) -> StatusView {
        let mut i = self.inner();
        let behind = i.counted_to.map(|t| now.saturating_duration_since(t));
        if behind.is_some_and(|b| b > Self::warn_bound(i.interval)) {
            i.over_since.get_or_insert(now);
        } else {
            i.over_since = None;
        }
        let (fold_rate, ingest_rate) = match (i.recent.front(), i.recent.back()) {
            (Some(first), Some(last)) if last.0 > first.0 => {
                let secs = last.0.duration_since(first.0).as_secs_f64();
                let folded: i64 = i.recent.iter().take(i.recent.len() - 1).map(|r| r.2).sum();
                #[allow(clippy::cast_precision_loss)]
                let rates = (folded as f64 / secs, ((last.1 - first.1 + folded).max(0)) as f64 / secs);
                (Some(rates.0), Some(rates.1))
            }
            _ => (None, None),
        };
        StatusView {
            interval: i.interval,
            behind,
            over_for: i.over_since.map(|t| now.saturating_duration_since(t)),
            failing: i.failing_since.map(|t| (now.saturating_duration_since(t), i.last_error.clone().unwrap_or_default())),
            fold_rate,
            ingest_rate,
            log_rows: i.recent.back().map(|r| r.1),
            folding: i.folding,
        }
    }

    #[must_use]
    pub fn view(&self) -> StatusView {
        self.view_at(Instant::now())
    }

    /// Log rows the folder folded since the start.
    #[must_use]
    pub fn folded(&self) -> i64 {
        self.inner().folded
    }
}

/// How the Counts line stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Health {
    /// Counts have not been folded since the start.
    NoData(String),
    Ok(String),
    Warning(String),
}

/// A duration in words: tenths of a second under 10 s, seconds under a
/// minute, then minutes, then hours.
#[must_use]
pub fn span(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 10.0 {
        format!("{s:.1} s")
    } else if s < 60.0 {
        format!("{} s", s.round())
    } else if s < 7200.0 {
        let m = (s / 60.0).round();
        if (m - 1.0).abs() < f64::EPSILON { "1 minute".to_string() } else { format!("{m} minutes") }
    } else {
        format!("{} hours", (s / 3600.0).round())
    }
}

/// The fold interval in words.
#[must_use]
pub fn interval_words(d: Duration) -> String {
    let ms = d.as_millis();
    if ms < 1000 {
        format!("{ms} ms")
    } else if ms.is_multiple_of(1000) {
        format!("{} s", ms / 1000)
    } else {
        span(d)
    }
}

/// Why counts are behind, when a cause is known: a direct writer of this
/// process, another session holding the counts lock, or an old open
/// transaction that keeps the log from being cleaned.
async fn cause(pool: &PgPool, v: &StatusView, old_after: Duration) -> Option<String> {
    if let Some(what) = holder(pool) {
        return Some(format!("Now {what}."));
    }
    let held: Option<bool> = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted AND objsubid = 2 \
           AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND classid::bigint = $1 AND objid::bigint = $2 AND pid <> pg_backend_pid())",
    )
    .bind(LOCK_KEYS.0)
    .bind(LOCK_KEYS.1)
    .fetch_one(pool)
    .await
    .ok();
    if held == Some(true) && !v.folding {
        return Some("Another database session holds the counts lock.".to_string());
    }
    let old: Option<bool> = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND backend_xmin IS NOT NULL \
           AND xact_start < now() - make_interval(secs => $1) AND pid <> pg_backend_pid())",
    )
    .bind(old_after.as_secs_f64())
    .fetch_one(pool)
    .await
    .ok();
    (old == Some(true)).then(|| "An old open transaction keeps the log from being cleaned.".to_string())
}

/// The Counts line of Health.
pub async fn health(status: &FoldStatus, pool: &PgPool) -> Health {
    health_at(status, pool, Instant::now(), OLD_SNAPSHOT).await
}

/// [`health`] at `now`, naming a transaction open longer than `old_after`.
pub(crate) async fn health_at(status: &FoldStatus, pool: &PgPool, now: Instant, old_after: Duration) -> Health {
    let v = status.view_at(now);
    if let Some((for_, error)) = &v.failing {
        return Health::Warning(format!(
            "The counts folder has failed for {}: {error}. Changes wait in the log; nothing is lost. \
             If it keeps failing, run the counts repair, which recomputes counts and removes the log rows it covers.",
            span(*for_)
        ));
    }
    let Some(behind) = v.behind else {
        return Health::NoData("Counts have not been folded yet since start.".to_string());
    };
    if v.over_for.is_some_and(|d| d >= WARN_HELD) {
        let rates = match (v.fold_rate, v.ingest_rate) {
            (Some(f), Some(i)) => format!(
                ": the counts folder folds {} changes a second, ingest makes {}",
                crate::admin::health::count(f.round()),
                crate::admin::health::count(i.round())
            ),
            _ => String::new(),
        };
        let cause = cause(pool, &v, old_after).await.map(|c| format!(" {c}")).unwrap_or_default();
        let size: Option<i64> = sqlx::query_scalar("SELECT pg_total_relation_size('count_log') + pg_total_relation_size('count_log_tags')")
            .fetch_one(pool)
            .await
            .ok();
        #[allow(clippy::cast_precision_loss)]
        let log = match (v.log_rows, size) {
            (Some(n), Some(b)) => format!(" The log holds {} changes ({:.0} MB).", crate::admin::health::count(n as f64), b as f64 / 1_048_576.0),
            _ => String::new(),
        };
        return Health::Warning(format!("Counts are {} behind ingest{rates}. Nothing is lost; counts catch up when ingest slows.{cause}{log}", span(behind)));
    }
    if v.interval > INTERVAL_WARN {
        return Health::Warning(format!(
            "counts.fold_interval_ms is {}: counts will be up to {} behind ingest (now {}).",
            v.interval.as_millis(),
            span(v.interval),
            span(behind)
        ));
    }
    Health::Ok(format!("Counts are up to {} behind ingest (folded every {}).", span(behind), interval_words(v.interval)))
}

/// The background folder: a round every `fold_interval_ms` from the start
/// of the previous one, at once when a round took longer.
pub struct Folder {
    pool: PgPool,
    interval: Duration,
    slice_rows: u64,
    status: Arc<FoldStatus>,
}

impl Folder {
    #[must_use]
    pub fn new(pool: PgPool, cfg: &CountsConfig, status: Arc<FoldStatus>) -> Self {
        let interval = Duration::from_millis(cfg.fold_interval_ms.max(1));
        SLICE_ROWS.store(cfg.fold_slice_rows.max(1), Ordering::Relaxed);
        if interval > INTERVAL_WARN {
            tracing::warn!(
                event = "counts_fold_interval_long",
                fold_interval_ms = cfg.fold_interval_ms,
                "counts.fold_interval_ms is over a minute: counts will be up to {} behind ingest",
                span(interval)
            );
        }
        status.inner().interval = interval;
        INTERVAL_MS.store(cfg.fold_interval_ms.max(1), Ordering::Relaxed);
        {
            let db = database(&pool);
            let mut r = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
            r.retain(|e| e.0 != db);
            r.push((db, Arc::clone(&status), 0.0));
        }
        Self { pool, interval, slice_rows: cfg.fold_slice_rows.max(1), status }
    }

    /// The status this folder reports to.
    #[must_use]
    pub fn status(&self) -> Arc<FoldStatus> {
        Arc::clone(&self.status)
    }

    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    /// Run rounds until aborted.
    pub async fn run(self) {
        let mut backoff = FIRST_BACKOFF;
        let mut last_vacuum = Instant::now();
        let db = database(&self.pool);
        loop {
            let start = Instant::now();
            if let Some(b) = self.status.view_at(start).behind {
                metrics::gauge!(METRIC_BEHIND).set(b.as_secs_f64());
                note_behind(&db, b.as_secs_f64());
            }
            self.status.started();
            match round(&self.pool, false, self.slice_rows).await {
                Ok(r) => {
                    self.status.done(start, &r);
                    backoff = FIRST_BACKOFF;
                    metrics::gauge!(METRIC_SLICES).set(f64::from(r.slices.max(r.tag_slices)));
                    #[allow(clippy::cast_precision_loss)]
                    metrics::gauge!(METRIC_LOG_ROWS).set((r.log_before + r.tags_before) as f64);
                    metrics::gauge!(METRIC_BEHIND).set(start.elapsed().as_secs_f64());
                }
                Err(error) => {
                    metrics::counter!(METRIC_FAILURES).increment(1);
                    tracing::warn!(event = "counts_fold_failed", %error, retry_secs = backoff.as_secs(), "folding the count log failed; ingest goes on and the log keeps the changes");
                    self.status.failed(&error.to_string());
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
            }
            if last_vacuum.elapsed() >= VACUUM_EVERY {
                vacuum(&self.pool).await;
                last_vacuum = Instant::now();
            }
            let next = start + self.interval;
            if next > Instant::now() {
                tokio::time::sleep_until(next.into()).await;
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
