//! Retention (design-v2 §4.4): a daily in-process job at `retention.run_at_utc`
//! (default 04:00 UTC).
//!
//! The scheduler runs an ordered list of [`RetentionTask`]s. This module
//! provides [`PublicRetention`], which covers every public table. The
//! private-tier deletes are a separate task that `private_tier::retention`
//! registers (§3.2, §4.4); it runs first, so the public `actors` step sees
//! which private rows remain. Public predicates that depend on private-tier
//! facts (the private-owner exception for `blocks`, and authors that still
//! hold private rows for `actors`) receive them through [`RetentionInputs`],
//! so no private table is named outside `private_tier` (§4.2).
//!
//! Deletes run in batches of 10 000 rows with a 100 ms pause between batches.
//! Each batch is its own statement; predicates are re-evaluated per batch
//! against `scope_dids` and `members`, which are the durable source of truth.
//!
//! Retention is an ordinary writer under the §6.11.7 lock order: every batch
//! takes the `ScopeState` read guard (through [`ScopeGuard`]) before its
//! statement acquires a pool connection, and releases it after the statement
//! commits. A batch therefore never runs concurrently with a write-guard
//! holder (member add/remove, purge marker steps), so a DID re-entering scope
//! through member add cannot have its rows deleted mid-batch. The pause
//! between batches runs without the guard.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::config::{RetentionConfig, UtcTimeOfDay};

/// Rows deleted per batch (§4.4).
pub const BATCH_SIZE: u64 = 10_000;
/// Pause between batches (§4.4).
pub const BATCH_PAUSE: Duration = Duration::from_millis(100);
/// `constellation_counts` rows unrefreshed this long are deleted (§4.4).
pub const CONSTELLATION_COUNTS_DAYS: i32 = 7;
/// `did_cache` rows are deleted this long after `expires_at` (§4.4).
pub const DID_CACHE_GRACE_DAYS: i32 = 7;
/// Finished `backfill_jobs` and `admin_commands` are kept this long (§4.4).
pub const FINISHED_WORK_DAYS: i32 = 30;

/// Metric: rows deleted by retention, labelled by table.
pub const METRIC_DELETED_ROWS: &str = "retention_deleted_rows_total";
/// Metric: retention task failures, labelled by task.
pub const METRIC_TASK_FAILURES: &str = "retention_task_failures_total";

/// Settings a retention run needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSettings {
    pub tombstone_days: u32,
    pub notification_days: u32,
    /// 0 disables the optional age cap.
    pub max_age_days: u32,
}

impl From<&RetentionConfig> for RetentionSettings {
    fn from(c: &RetentionConfig) -> Self {
        Self {
            tombstone_days: c.tombstone_days,
            notification_days: c.notification_days,
            max_age_days: c.max_age_days,
        }
    }
}

/// Rows deleted per table by one task.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionReport {
    pub deleted: Vec<(&'static str, u64)>,
}

impl RetentionReport {
    #[must_use]
    pub fn total(&self) -> u64 {
        self.deleted.iter().map(|(_, n)| n).sum()
    }
}

/// A held `ScopeState` read guard. Dropping it releases the guard.
pub struct ScopeReadGuard {
    /// Held only for its `Drop`.
    _held: Box<dyn Any + Send + Sync>,
}

impl ScopeReadGuard {
    /// Wrap the owned guard (for example a `tokio::sync::OwnedRwLockReadGuard`).
    #[must_use]
    pub fn new<G: Any + Send + Sync>(guard: G) -> Self {
        Self {
            _held: Box::new(guard),
        }
    }
}

/// Access to the `ScopeState` guard (§6.11.7 lock order, step 1), implemented
/// by `ScopeState`. Writers take the read guard before acquiring a pool
/// connection and hold it until after commit.
#[async_trait]
pub trait ScopeGuard: Send + Sync {
    /// Wait for and take the read guard.
    async fn read(&self) -> ScopeReadGuard;
}

/// One unit of retention work. Tasks run sequentially in registration order.
#[async_trait]
pub trait RetentionTask: Send + Sync {
    /// Stable name for logs and metrics.
    fn name(&self) -> &'static str;

    /// Run the task once. Every batch takes `scope`'s read guard first.
    async fn run(
        &self,
        pool: &PgPool,
        scope: &dyn ScopeGuard,
        settings: &RetentionSettings,
    ) -> Result<RetentionReport, sqlx::Error>;
}

/// Private-tier facts the public predicates need (§4.4), supplied by the
/// module that owns them.
#[async_trait]
pub trait RetentionInputs: Send + Sync {
    /// The current private-owner set (§6.11.1). Their `app.bsky.graph.block`
    /// rows are kept (§9.3 rule 3).
    async fn private_owners(&self) -> Result<Vec<String>, sqlx::Error>;

    /// DIDs that still author private-tier rows after the private retention
    /// task ran. Their `actors` rows are kept (class M identity rows).
    async fn private_row_authors(&self) -> Result<Vec<String>, sqlx::Error>;
}

/// A bind parameter for one retention statement.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Bind {
    None,
    Days(i32),
    Dids(Vec<String>),
}

/// One batched delete.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Step {
    table: &'static str,
    sql: String,
    bind: Bind,
}

const NOT_IN_SCOPE: &str = "NOT EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = {col})";

fn not_in_scope(col: &str) -> String {
    NOT_IN_SCOPE.replace("{col}", col)
}

/// The DID in an at-uri's authority segment (`at://<did>/<collection>/<rkey>`).
fn author_of(col: &str) -> String {
    format!("split_part({col}, '/', 3)")
}

fn member_among(exprs: &[String]) -> String {
    format!(
        "EXISTS (SELECT 1 FROM members m WHERE m.did IN ({}))",
        exprs.join(", ")
    )
}

/// `DELETE FROM <table> WHERE <key> IN (SELECT <key> FROM <table> <alias> WHERE <pred> LIMIT n)`.
fn batched(table: &'static str, key: &str, alias: &str, pred: &str, bind: Bind) -> Step {
    Step {
        table,
        sql: format!(
            "DELETE FROM {table} WHERE {key} IN (SELECT {alias}.{key} FROM {table} {alias} WHERE {pred} LIMIT {BATCH_SIZE})"
        ),
        bind,
    }
}

/// Class S / M deletes (§4.4 retention predicates), in dependency order:
/// content first, then derived counts, then identity rows.
fn scope_steps(private_owners: Vec<String>, private_row_authors: Vec<String>) -> Vec<Step> {
    let post_member_exception = member_among(&[
        author_of("p.reply_parent_uri"),
        author_of("p.reply_root_uri"),
        author_of("p.quote_uri"),
    ]);
    let subject_member = |alias: &str| member_among(&[author_of(&format!("{alias}.subject_uri"))]);
    vec![
        batched(
            "posts",
            "uri",
            "p",
            &format!("{} AND NOT {post_member_exception}", not_in_scope("p.author_did")),
            Bind::None,
        ),
        batched(
            "likes",
            "uri",
            "l",
            &format!("{} AND NOT {}", not_in_scope("l.author_did"), subject_member("l")),
            Bind::None,
        ),
        batched(
            "reposts",
            "uri",
            "r",
            &format!("{} AND NOT {}", not_in_scope("r.author_did"), subject_member("r")),
            Bind::None,
        ),
        batched(
            "follows",
            "uri",
            "f",
            &format!(
                "{} AND NOT {}",
                not_in_scope("f.author_did"),
                member_among(&["f.subject_did".to_string()])
            ),
            Bind::None,
        ),
        batched(
            "blocks",
            "uri",
            "b",
            &format!(
                "{} AND NOT {} AND NOT (b.author_did = ANY($1::text[]))",
                not_in_scope("b.author_did"),
                member_among(&["b.subject_did".to_string()])
            ),
            Bind::Dids(private_owners),
        ),
        batched(
            "feed_items",
            "uri",
            "fi",
            &not_in_scope("fi.originator_did"),
            Bind::None,
        ),
        // Derived: counts for posts whose author left scope and that no
        // remaining interaction row references.
        batched(
            "post_counts",
            "post_uri",
            "pc",
            &format!(
                "{} AND NOT EXISTS (SELECT 1 FROM likes x WHERE x.subject_uri = pc.post_uri) \
                 AND NOT EXISTS (SELECT 1 FROM reposts x WHERE x.subject_uri = pc.post_uri) \
                 AND NOT EXISTS (SELECT 1 FROM posts x WHERE x.reply_parent_uri = pc.post_uri) \
                 AND NOT EXISTS (SELECT 1 FROM posts x WHERE x.quote_uri = pc.post_uri)",
                not_in_scope(&author_of("pc.post_uri"))
            ),
            Bind::None,
        ),
        // Identity rows: out of scope, not a member, and no class-M row
        // authored by the DID remains in any table.
        batched(
            "actors",
            "did",
            "a",
            &format!(
                "{} AND NOT {} \
                 AND NOT EXISTS (SELECT 1 FROM posts x WHERE x.author_did = a.did) \
                 AND NOT EXISTS (SELECT 1 FROM likes x WHERE x.author_did = a.did) \
                 AND NOT EXISTS (SELECT 1 FROM reposts x WHERE x.author_did = a.did) \
                 AND NOT EXISTS (SELECT 1 FROM follows x WHERE x.author_did = a.did) \
                 AND NOT EXISTS (SELECT 1 FROM blocks x WHERE x.author_did = a.did) \
                 AND NOT (a.did = ANY($1::text[]))",
                not_in_scope("a.did"),
                member_among(&["a.did".to_string()])
            ),
            Bind::Dids(private_row_authors),
        ),
        // actor_counts (class S) follow their identity row.
        batched(
            "actor_counts",
            "did",
            "ac",
            &format!(
                "{} AND NOT EXISTS (SELECT 1 FROM actors x WHERE x.did = ac.did)",
                not_in_scope("ac.did")
            ),
            Bind::None,
        ),
    ]
}

/// Class T deletes (§4.4).
fn time_steps(settings: &RetentionSettings) -> Vec<Step> {
    let days = |d: u32| Bind::Days(i32::try_from(d).unwrap_or(i32::MAX));
    vec![
        batched(
            "record_tombstones",
            "uri",
            "t",
            "t.deleted_at < now() - make_interval(days => $1)",
            days(settings.tombstone_days),
        ),
        batched(
            "notifications",
            "id",
            "n",
            "n.indexed_at < now() - make_interval(days => $1)",
            days(settings.notification_days),
        ),
        batched(
            "constellation_counts",
            "target",
            "cc",
            "cc.fetched_at < now() - make_interval(days => $1)",
            Bind::Days(CONSTELLATION_COUNTS_DAYS),
        ),
        batched(
            "did_cache",
            "did",
            "dc",
            "dc.expires_at < now() - make_interval(days => $1)",
            Bind::Days(DID_CACHE_GRACE_DAYS),
        ),
        batched(
            "backfill_jobs",
            "ctid",
            "bj",
            "bj.state IN ('done', 'failed', 'cancelled') AND bj.updated_at < now() - make_interval(days => $1)",
            Bind::Days(FINISHED_WORK_DAYS),
        ),
        batched(
            "admin_commands",
            "id",
            "ac",
            "ac.finished_at IS NOT NULL AND ac.finished_at < now() - make_interval(days => $1)",
            Bind::Days(FINISHED_WORK_DAYS),
        ),
    ]
}

/// Optional age cap (§4.4): S-class rows of unprotected authors older than
/// `max_age_days`. Protected = members ∪ DIDs followed by members. The class-M
/// exceptions of the scope predicates still apply. Graph records (follows,
/// blocks) are not age-capped: they are state, not content.
fn age_cap_steps(max_age_days: u32) -> Vec<Step> {
    if max_age_days == 0 {
        return Vec::new();
    }
    let days = i32::try_from(max_age_days).unwrap_or(i32::MAX);
    let unprotected = |col: &str| {
        format!(
            "NOT EXISTS (SELECT 1 FROM members m WHERE m.did = {col}) \
             AND NOT EXISTS (SELECT 1 FROM follows f JOIN members m ON m.did = f.author_did WHERE f.subject_did = {col})"
        )
    };
    let post_member_exception = member_among(&[
        author_of("p.reply_parent_uri"),
        author_of("p.reply_root_uri"),
        author_of("p.quote_uri"),
    ]);
    vec![
        batched(
            "posts",
            "uri",
            "p",
            &format!(
                "p.created_at < now() - make_interval(days => $1) AND {} AND NOT {post_member_exception}",
                unprotected("p.author_did")
            ),
            Bind::Days(days),
        ),
        batched(
            "likes",
            "uri",
            "l",
            &format!(
                "l.created_at < now() - make_interval(days => $1) AND {} AND NOT {}",
                unprotected("l.author_did"),
                member_among(&[author_of("l.subject_uri")])
            ),
            Bind::Days(days),
        ),
        batched(
            "reposts",
            "uri",
            "r",
            &format!(
                "r.created_at < now() - make_interval(days => $1) AND {} AND NOT {}",
                unprotected("r.author_did"),
                member_among(&[author_of("r.subject_uri")])
            ),
            Bind::Days(days),
        ),
        batched(
            "feed_items",
            "uri",
            "fi",
            &format!(
                "fi.sort_at < now() - make_interval(days => $1) AND {}",
                unprotected("fi.originator_did")
            ),
            Bind::Days(days),
        ),
    ]
}

/// Run one batch under the scope read guard. The guard is taken before the
/// statement acquires a pool connection and released after it commits (a
/// single statement is its own transaction).
async fn run_batch(pool: &PgPool, scope: &dyn ScopeGuard, step: &Step) -> Result<u64, sqlx::Error> {
    let guard = scope.read().await;
    let query = sqlx::query::<sqlx::Postgres>(&step.sql);
    let query = match &step.bind {
        Bind::None => query,
        Bind::Days(d) => query.bind(*d),
        Bind::Dids(dids) => query.bind(dids.clone()),
    };
    let result = query.execute(pool).await;
    drop(guard);
    Ok(result?.rows_affected())
}

/// Run one step to completion in batches, pausing between batches without
/// the guard.
async fn run_step(pool: &PgPool, scope: &dyn ScopeGuard, step: &Step) -> Result<u64, sqlx::Error> {
    let mut total = 0u64;
    loop {
        let n = run_batch(pool, scope, step).await?;
        total += n;
        if n < BATCH_SIZE {
            return Ok(total);
        }
        tokio::time::sleep(BATCH_PAUSE).await;
    }
}

async fn run_steps(
    pool: &PgPool,
    scope: &dyn ScopeGuard,
    steps: &[Step],
) -> Result<RetentionReport, sqlx::Error> {
    let mut report = RetentionReport::default();
    for step in steps {
        let n = run_step(pool, scope, step).await?;
        if n > 0 {
            metrics::counter!(METRIC_DELETED_ROWS, "table" => step.table).increment(n);
        }
        report.deleted.push((step.table, n));
    }
    Ok(report)
}

/// Retention for every public table (§4.4).
pub struct PublicRetention {
    inputs: Arc<dyn RetentionInputs>,
}

impl PublicRetention {
    #[must_use]
    pub fn new(inputs: Arc<dyn RetentionInputs>) -> Self {
        Self { inputs }
    }
}

#[async_trait]
impl RetentionTask for PublicRetention {
    fn name(&self) -> &'static str {
        "public"
    }

    async fn run(
        &self,
        pool: &PgPool,
        scope: &dyn ScopeGuard,
        settings: &RetentionSettings,
    ) -> Result<RetentionReport, sqlx::Error> {
        let owners = self.inputs.private_owners().await?;
        let private_authors = self.inputs.private_row_authors().await?;
        let mut steps = scope_steps(owners, private_authors);
        steps.extend(age_cap_steps(settings.max_age_days));
        steps.extend(time_steps(settings));
        run_steps(pool, scope, &steps).await
    }
}

/// The next run strictly after `now` at `at` UTC.
#[must_use]
pub fn next_run_after(now: DateTime<Utc>, at: UtcTimeOfDay) -> DateTime<Utc> {
    let midnight = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .map_or(now, |t| t.and_utc());
    let today = midnight + TimeDelta::seconds(i64::from(at.seconds_after_midnight()));
    if today > now {
        today
    } else {
        today + TimeDelta::days(1)
    }
}

/// Run every task once, in order. A failing task is logged and counted; the
/// remaining tasks still run.
pub async fn run_once(
    pool: &PgPool,
    scope: &dyn ScopeGuard,
    tasks: &[Arc<dyn RetentionTask>],
    settings: &RetentionSettings,
) {
    for task in tasks {
        match task.run(pool, scope, settings).await {
            Ok(report) => tracing::info!(
                event = "retention_task_done",
                task = task.name(),
                deleted = report.total(),
                per_table = ?report.deleted,
                "retention task finished",
            ),
            Err(error) => {
                metrics::counter!(METRIC_TASK_FAILURES, "task" => task.name()).increment(1);
                tracing::error!(
                    event = "retention_task_failed",
                    task = task.name(),
                    %error,
                    "retention task failed; it runs again at the next scheduled time",
                );
            }
        }
    }
}

/// Spawn the daily scheduler. It sleeps until the next `at` UTC, runs every
/// task, and repeats. Abort the handle to stop it.
#[must_use]
pub fn spawn_scheduler(
    pool: PgPool,
    scope: Arc<dyn ScopeGuard>,
    tasks: Vec<Arc<dyn RetentionTask>>,
    settings: RetentionSettings,
    at: UtcTimeOfDay,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let now = Utc::now();
            let next = next_run_after(now, at);
            let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
            tracing::debug!(event = "retention_scheduled", next = %next, "next retention run scheduled");
            tokio::time::sleep(wait).await;
            run_once(&pool, scope.as_ref(), &tasks, &settings).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn all_steps() -> Vec<Step> {
        let settings = RetentionSettings {
            tombstone_days: 7,
            notification_days: 90,
            max_age_days: 30,
        };
        let mut steps = scope_steps(vec!["did:plc:owner".into()], vec!["did:plc:author".into()]);
        steps.extend(age_cap_steps(settings.max_age_days));
        steps.extend(time_steps(&settings));
        steps
    }

    #[test]
    fn every_step_is_batched_at_ten_thousand() {
        for step in all_steps() {
            assert!(step.sql.contains("LIMIT 10000"), "{}: {}", step.table, step.sql);
            assert!(step.sql.starts_with(&format!("DELETE FROM {} ", step.table)));
        }
    }

    #[test]
    fn no_step_names_a_private_table() {
        // Built at runtime so this file never contains the identifiers the
        // image's private-table source check looks for (§4.2, §11.4).
        let forbidden: Vec<String> = [
            "posts",
            "audiences",
            "blocks",
            "tombstones",
            "owner_ready",
            "owner_eligible",
        ]
        .iter()
        .map(|t| format!("private_{t}"))
        .collect();
        for step in all_steps() {
            for f in &forbidden {
                assert!(!step.sql.contains(f.as_str()), "{} names {f}", step.table);
            }
        }
    }

    #[test]
    fn scope_steps_order_content_before_counts_and_identity() {
        let tables: Vec<&str> = scope_steps(vec![], vec![]).iter().map(|s| s.table).collect();
        assert_eq!(
            tables,
            [
                "posts",
                "likes",
                "reposts",
                "follows",
                "blocks",
                "feed_items",
                "post_counts",
                "actors",
                "actor_counts"
            ]
        );
    }

    #[test]
    fn class_m_exceptions_are_present() {
        let steps = scope_steps(vec![], vec![]);
        let sql = |t: &str| steps.iter().find(|s| s.table == t).map(|s| s.sql.clone()).unwrap_or_default();
        assert!(sql("posts").contains("split_part(p.reply_parent_uri, '/', 3)"));
        assert!(sql("posts").contains("split_part(p.reply_root_uri, '/', 3)"));
        assert!(sql("posts").contains("split_part(p.quote_uri, '/', 3)"));
        assert!(sql("likes").contains("split_part(l.subject_uri, '/', 3)"));
        assert!(sql("follows").contains("m.did IN (f.subject_did)"));
        assert!(sql("blocks").contains("b.author_did = ANY($1::text[])"));
    }

    #[test]
    fn binds_carry_private_inputs_and_days() {
        let steps = scope_steps(vec!["did:plc:o".into()], vec!["did:plc:a".into()]);
        let blocks = steps.iter().find(|s| s.table == "blocks").expect("blocks step");
        assert_eq!(blocks.bind, Bind::Dids(vec!["did:plc:o".into()]));
        let actors = steps.iter().find(|s| s.table == "actors").expect("actors step");
        assert_eq!(actors.bind, Bind::Dids(vec!["did:plc:a".into()]));

        let settings = RetentionSettings {
            tombstone_days: 7,
            notification_days: 90,
            max_age_days: 0,
        };
        let t = time_steps(&settings);
        let bind = |table: &str| t.iter().find(|s| s.table == table).map(|s| s.bind.clone());
        assert_eq!(bind("record_tombstones"), Some(Bind::Days(7)));
        assert_eq!(bind("notifications"), Some(Bind::Days(90)));
        assert_eq!(bind("constellation_counts"), Some(Bind::Days(7)));
        assert_eq!(bind("did_cache"), Some(Bind::Days(7)));
        assert_eq!(bind("backfill_jobs"), Some(Bind::Days(30)));
        assert_eq!(bind("admin_commands"), Some(Bind::Days(30)));
    }

    #[test]
    fn age_cap_is_off_at_zero() {
        assert!(age_cap_steps(0).is_empty());
        let tables: Vec<&str> = age_cap_steps(30).iter().map(|s| s.table).collect();
        assert_eq!(tables, ["posts", "likes", "reposts", "feed_items"]);
    }

    #[test]
    fn finished_jobs_only() {
        let settings = RetentionSettings {
            tombstone_days: 7,
            notification_days: 90,
            max_age_days: 0,
        };
        let t = time_steps(&settings);
        let jobs = t.iter().find(|s| s.table == "backfill_jobs").expect("jobs step");
        assert!(jobs.sql.contains("('done', 'failed', 'cancelled')"));
        let admin = t.iter().find(|s| s.table == "admin_commands").expect("admin step");
        assert!(admin.sql.contains("finished_at IS NOT NULL"));
    }

    #[test]
    fn next_run_is_later_today_or_tomorrow() {
        let at = UtcTimeOfDay::new(4, 0).expect("valid");
        let before = Utc.with_ymd_and_hms(2026, 9, 26, 3, 59, 0).single().expect("valid");
        assert_eq!(
            next_run_after(before, at),
            Utc.with_ymd_and_hms(2026, 9, 26, 4, 0, 0).single().expect("valid")
        );
        let exactly = Utc.with_ymd_and_hms(2026, 9, 26, 4, 0, 0).single().expect("valid");
        assert_eq!(
            next_run_after(exactly, at),
            Utc.with_ymd_and_hms(2026, 9, 27, 4, 0, 0).single().expect("valid")
        );
        let after = Utc.with_ymd_and_hms(2026, 12, 31, 23, 0, 0).single().expect("valid");
        assert_eq!(
            next_run_after(after, at),
            Utc.with_ymd_and_hms(2027, 1, 1, 4, 0, 0).single().expect("valid")
        );
    }

    /// The `ScopeState` shape item 4 provides: an owned tokio `RwLock` guard.
    struct LockScope {
        lock: Arc<tokio::sync::RwLock<()>>,
        reads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl ScopeGuard for LockScope {
        async fn read(&self) -> ScopeReadGuard {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ScopeReadGuard::new(Arc::clone(&self.lock).read_owned().await)
        }
    }

    fn lock_scope() -> LockScope {
        LockScope {
            lock: Arc::new(tokio::sync::RwLock::new(())),
            reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[tokio::test]
    async fn read_guard_excludes_writers_until_dropped() {
        let scope = lock_scope();
        let guard = scope.read().await;
        assert!(scope.lock.try_write().is_err(), "writer excluded while held");
        drop(guard);
        assert!(scope.lock.try_write().is_ok(), "writer admitted after release");
    }

    #[tokio::test]
    async fn batch_takes_the_guard_and_releases_it_on_failure() {
        let scope = lock_scope();
        // Nothing listens on port 1, so the statement fails at connect.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(500))
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .expect("lazy pool");
        let step = time_steps(&RetentionSettings {
            tombstone_days: 7,
            notification_days: 90,
            max_age_days: 0,
        })
        .remove(0);
        assert!(run_batch(&pool, &scope, &step).await.is_err());
        assert_eq!(scope.reads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(scope.lock.try_write().is_ok(), "guard released after the batch");
    }

    #[test]
    fn settings_from_config() {
        let s = RetentionSettings::from(&RetentionConfig::default());
        assert_eq!(
            s,
            RetentionSettings {
                tombstone_days: 7,
                notification_days: 90,
                max_age_days: 0
            }
        );
    }
}
