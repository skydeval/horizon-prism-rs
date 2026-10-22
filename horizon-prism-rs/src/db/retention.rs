//! Retention (design-v2 §4.4): a daily in-process job at `retention.run_at_utc`
//! (default 04:00 UTC).
//!
//! The scheduler runs an ordered list of [`RetentionTask`]s. This module
//! provides [`PublicRetention`], which covers every public table. The
//! private-tier deletes are a separate task that `private_tier::retention`
//! registers (§3.2, §4.4); it runs first, so the public `actors` step sees
//! which private rows remain. Public predicates that depend on private-tier
//! facts (authors that still hold private rows, for `actors`) receive them
//! through [`RetentionInputs`], so no private table is named outside
//! `private_tier` (§4.2). Blocks and list blocks are kept only with a member
//! side (member-blocks-only design §8).
//!
//! Deletes run in batches of 10 000 rows with a 100 ms pause between batches.
//! Each batch is its own statement; predicates are re-evaluated per batch
//! against `scope_dids` and `members`, which are the durable source of truth.
//!
//! Retention is an ordinary writer under the §6.11.7 lock order: every batch
//! takes the `ScopeState` read guard (through [`ScopeGuard`]) before it
//! acquires a pool connection, and releases it after commit. A batch never
//! runs concurrently with a write-guard holder (member add/remove, purge
//! marker steps). The pause between batches runs without the guard.
//!
//! Steps whose deletions depend on scope or membership run **per DID**: under
//! the read guard, one transaction takes the DID's exclusive advisory lock
//! (`hashtextextended('did:' || did, 0)`) and then deletes that DID's rows, so
//! the delete's scope and membership predicates are evaluated with the lock
//! held. Scope growth takes the shared lock on each growth subject DID before
//! inserting `scope_dids` (§6.11.7, §9.4), so a DID re-entering scope by any
//! path is either visible to that delete or blocked until it finishes; it can
//! never be half-deleted. Time-bounded steps keep plain row batches.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::config::{RetentionConfig, UtcTimeOfDay};

/// Rows deleted per batch (§4.4).
pub const BATCH_SIZE: u64 = 10_000;
/// DIDs fetched per candidate page for per-DID steps.
pub const CANDIDATE_PAGE: u64 = 500;
/// Takes a DID's exclusive advisory lock for the transaction (§6.11.7 key).
pub const LOCK_DID_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended('did:' || $1, 0))";
/// Pause between batches (§4.4).
pub const BATCH_PAUSE: Duration = Duration::from_millis(100);
/// `constellation_counts` rows unrefreshed this long are deleted (§4.4).
pub const CONSTELLATION_COUNTS_DAYS: i32 = 7;
/// `did_cache` rows are deleted this long after `expires_at` (§4.4).
pub const DID_CACHE_GRACE_DAYS: i32 = 7;
/// Finished `backfill_jobs` and `admin_commands` are kept this long (§4.4).
pub const FINISHED_WORK_DAYS: i32 = 30;
/// `admin_logins` rows are kept this long (admin-ui design §5.8).
pub const ADMIN_LOGINS_DAYS: i32 = 90;
/// How long `tag_activity_hourly` rows are kept unless
/// `trends.aggregate_retention_hours` says otherwise (design-v3 §7.5). It
/// matches the triggers' own default cutoff.
pub const TREND_AGGREGATE_HOURS: u32 = 72;

/// Metric: rows deleted by retention, labelled by table.
pub const METRIC_DELETED_ROWS: &str = "retention_deleted_rows_total";
/// Metric: retention task failures, labelled by task.
pub const METRIC_TASK_FAILURES: &str = "retention_task_failures_total";
/// Activity subscriptions to a subject outside the indexed set, by what
/// happened to them (`rejected` at the endpoint, `swept` here).
pub const METRIC_SUBSCRIPTIONS_OUT_OF_SCOPE: &str = "activity_subscriptions_out_of_scope_total";
/// Metric: candidate DIDs with nothing left to delete once locked, because
/// they re-entered scope (or became protected) meanwhile. Counts how often
/// the retention/growth race actually fires.
pub const METRIC_SKIPPED_BY_RECHECK: &str = "retention_dids_skipped_by_recheck_total";

/// Settings a retention run needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSettings {
    pub tombstone_days: u32,
    pub notification_days: u32,
    /// 0 disables the optional age cap.
    pub max_age_days: u32,
    /// `trends.aggregate_retention_hours`: how long `tag_activity_hourly`
    /// rows are kept.
    pub trend_aggregate_hours: u32,
}

impl From<&RetentionConfig> for RetentionSettings {
    fn from(c: &RetentionConfig) -> Self {
        Self {
            tombstone_days: c.tombstone_days,
            notification_days: c.notification_days,
            max_age_days: c.max_age_days,
            trend_aggregate_hours: TREND_AGGREGATE_HOURS,
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
    /// DIDs that still author private-tier rows after the private retention
    /// task ran. Their `actors` rows are kept (class M identity rows).
    async fn private_row_authors(&self) -> Result<Vec<String>, sqlx::Error>;

    /// Called after rule 4's tombstones were deleted by age, so in-memory
    /// counts of them can be read again (design-v3 §4.3).
    async fn subscription_tombstones_aged(&self) -> Result<(), sqlx::Error> {
        Ok(())
    }
}

/// A bind parameter for one retention statement.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Bind {
    None,
    Days(i32),
    Dids(Vec<String>),
}

/// One retention delete.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Step {
    table: &'static str,
    /// The batched delete. For a per-DID step it also filters on the DID
    /// parameter that follows the step's own bind.
    sql: String,
    /// Per-DID steps: the query listing DIDs that have deletable rows.
    candidates: Option<String>,
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
        candidates: None,
        bind,
    }
}

/// A per-DID step (§6.11.7): rows are deleted one DID at a time, under that
/// DID's exclusive advisory lock. `did_col` is the expression naming the DID a
/// row belongs to. The DID is the parameter after the step's own bind.
fn scoped(
    table: &'static str,
    key: &str,
    alias: &str,
    did_col: &str,
    pred: &str,
    bind: Bind,
) -> Step {
    let did_param = if matches!(bind, Bind::None) { 1 } else { 2 };
    Step {
        table,
        sql: format!(
            "DELETE FROM {table} WHERE {key} IN (SELECT {alias}.{key} FROM {table} {alias} \
             WHERE {did_col} = ${did_param} AND {pred} LIMIT {BATCH_SIZE})"
        ),
        candidates: Some(format!(
            "SELECT DISTINCT {did_col} AS did FROM {table} {alias} WHERE {pred} LIMIT {CANDIDATE_PAGE}"
        )),
        bind,
    }
}

/// Class S / M deletes (§4.4 retention predicates), in dependency order:
/// content first, then derived counts, then identity rows.
fn scope_steps(private_row_authors: Vec<String>) -> Vec<Step> {
    let post_member_exception = member_among(&[
        author_of("p.reply_parent_uri"),
        author_of("p.reply_root_uri"),
        author_of("p.quote_uri"),
    ]);
    let subject_member = |alias: &str| member_among(&[author_of(&format!("{alias}.subject_uri"))]);
    let mut steps = vec![
        scoped(
            "posts",
            "uri",
            "p",
            "p.author_did",
            &format!("{} AND NOT {post_member_exception}", not_in_scope("p.author_did")),
            Bind::None,
        ),
        scoped(
            "likes",
            "uri",
            "l",
            "l.author_did",
            &format!("{} AND NOT {}", not_in_scope("l.author_did"), subject_member("l")),
            Bind::None,
        ),
        scoped(
            "reposts",
            "uri",
            "r",
            "r.author_did",
            &format!("{} AND NOT {}", not_in_scope("r.author_did"), subject_member("r")),
            Bind::None,
        ),
        scoped(
            "follows",
            "uri",
            "f",
            "f.author_did",
            &format!(
                "{} AND NOT {}",
                not_in_scope("f.author_did"),
                member_among(&["f.subject_did".to_string()])
            ),
            Bind::None,
        ),
        // Rule M (member-blocks-only design §8): a block is kept only with
        // a member side, whatever the author's scope.
        scoped(
            "blocks",
            "uri",
            "b",
            "b.author_did",
            &format!(
                "NOT {} AND NOT {}",
                member_among(&["b.author_did".to_string()]),
                member_among(&["b.subject_did".to_string()])
            ),
            Bind::None,
        ),
        scoped(
            "feed_items",
            "uri",
            "fi",
            "fi.originator_did",
            &not_in_scope("fi.originator_did"),
            Bind::None,
        ),
        // Derived: counts for posts whose author left scope and that no
        // remaining interaction row references.
        scoped(
            "post_counts",
            "post_uri",
            "pc",
            "split_part(pc.post_uri, '/', 3)",
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
        scoped(
            "actors",
            "did",
            "a",
            "a.did",
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
        scoped(
            "actor_counts",
            "did",
            "ac",
            "ac.did",
            &format!(
                "{} AND NOT EXISTS (SELECT 1 FROM actors x WHERE x.did = ac.did)",
                not_in_scope("ac.did")
            ),
            Bind::None,
        ),
    ];
    // The v3 record tables are content: after `feed_items`, before the
    // derived counts and identity rows.
    steps.splice(6..6, v3_scope_steps());
    steps
}

/// Class S deletes of the record tables design-v3 added (§14.1): kept while
/// the author is in scope. Subscriptions never keep, create or delete rows
/// of these tables.
fn v3_scope_steps() -> Vec<Step> {
    let class_s = |table: &'static str, alias: &'static str, author: &'static str| {
        let col = format!("{alias}.{author}");
        scoped(table, "uri", alias, &col, &not_in_scope(&col), Bind::None)
    };
    // A gate is also kept while its post row is kept (class M).
    let gate = |table: &'static str, alias: &'static str| {
        let col = format!("{alias}.author_did");
        let pred = format!(
            "{} AND NOT EXISTS (SELECT 1 FROM posts x WHERE x.uri = {alias}.post_uri)",
            not_in_scope(&col)
        );
        scoped(table, "uri", alias, &col, &pred, Bind::None)
    };
    vec![
        class_s("lists", "ls", "creator_did"),
        class_s("list_items", "li", "author_did"),
        // Rule M (member-blocks-only design §8): a member's list blocks only.
        scoped(
            "list_blocks",
            "uri",
            "lb",
            "lb.author_did",
            &format!("NOT {}", member_among(&["lb.author_did".to_string()])),
            Bind::None,
        ),
        class_s("list_optouts", "lo", "author_did"),
        class_s("starter_packs", "sp", "creator_did"),
        class_s("labeler_services", "lsv", "creator_did"),
        class_s("actor_self_records", "asr", "did"),
        class_s("nearhorizon_poll_posts", "pp", "author_did"),
        gate("threadgates", "tg"),
        gate("postgates", "pg"),
        // Derived counts follow their pack: an orphan sweep.
        scoped(
            "starter_pack_counts",
            "pack_uri",
            "spc",
            "split_part(spc.pack_uri, '/', 3)",
            "NOT EXISTS (SELECT 1 FROM starter_packs x WHERE x.uri = spc.pack_uri)",
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
        // Rule 4's own tombstones (design-v3 §4.3), on the same clock.
        batched(
            "subscription_tombstones",
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
        // Trend aggregates: hours past the aggregate's retention (§7.5).
        batched(
            "tag_activity_hourly",
            "ctid",
            "ta",
            &format!("ta.hour < now() - make_interval(hours => {})", settings.trend_aggregate_hours),
            Bind::None,
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
            // `unavailable` is finished too (actionable-warnings design §3.5).
            "bj.state IN ('done', 'failed', 'unavailable', 'cancelled') AND bj.updated_at < now() - make_interval(days => $1)",
            Bind::Days(FINISHED_WORK_DAYS),
        ),
        batched(
            "admin_commands",
            "id",
            "ac",
            "ac.finished_at IS NOT NULL AND ac.finished_at < now() - make_interval(days => $1)",
            Bind::Days(FINISHED_WORK_DAYS),
        ),
        // The admin UI's sign-in rows (admin-ui design §12): expired
        // sessions, unfinished sign-ins past ten minutes, the sign-in log
        // past 90 days.
        batched("admin_sessions", "id_hash", "ses", "ses.expires_at < now()", Bind::None),
        batched("admin_oauth_states", "state", "st", "st.created_at < now() - interval '10 minutes'", Bind::None),
        batched("admin_logins", "id", "al", "al.at < now() - make_interval(days => $1)", Bind::Days(ADMIN_LOGINS_DAYS)),
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

/// Run one step to completion, pausing between batches without the guard.
async fn run_step(pool: &PgPool, scope: &dyn ScopeGuard, step: &Step) -> Result<u64, sqlx::Error> {
    if let Some(candidates) = &step.candidates {
        return run_per_did(pool, scope, step, candidates).await;
    }
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

/// A per-DID step: list DIDs with deletable rows, then delete each DID's
/// rows under its exclusive lock. Each DID is handled at most once per run.
async fn run_per_did(
    pool: &PgPool,
    scope: &dyn ScopeGuard,
    step: &Step,
    candidates_sql: &str,
) -> Result<u64, sqlx::Error> {
    let mut total = 0u64;
    let mut seen = HashSet::new();
    loop {
        let query = sqlx::query_scalar::<sqlx::Postgres, String>(candidates_sql);
        let query = match &step.bind {
            Bind::None => query,
            Bind::Days(d) => query.bind(*d),
            Bind::Dids(dids) => query.bind(dids.clone()),
        };
        let fresh: Vec<String> = query
            .fetch_all(pool)
            .await?
            .into_iter()
            .filter(|did| seen.insert(did.clone()))
            .collect();
        if fresh.is_empty() {
            return Ok(total);
        }
        for did in &fresh {
            total += delete_for_did(pool, scope, step, did).await?;
        }
        tokio::time::sleep(BATCH_PAUSE).await;
    }
}

/// Delete one DID's rows for `step` (§6.11.7). Each batch: read guard, then a
/// transaction that takes the DID's exclusive advisory lock and deletes. The
/// delete's own predicate re-checks `scope_dids` and `members` after the lock
/// is held, so a scope insert committed by a writer holding the DID's shared
/// lock is always seen: either it committed before the lock was granted, or
/// the writer is still waiting for the lock and inserts afterwards.
async fn delete_for_did(
    pool: &PgPool,
    scope: &dyn ScopeGuard,
    step: &Step,
    did: &str,
) -> Result<u64, sqlx::Error> {
    let mut total = 0u64;
    let mut first = true;
    loop {
        let guard = scope.read().await;
        let mut tx = pool.begin().await?;
        sqlx::query(LOCK_DID_SQL).bind(did).execute(&mut *tx).await?;
        let query = sqlx::query::<sqlx::Postgres>(&step.sql);
        let query = match &step.bind {
            Bind::None => query.bind(did),
            Bind::Days(d) => query.bind(*d).bind(did),
            Bind::Dids(dids) => query.bind(dids.clone()).bind(did),
        };
        let n = query.execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        drop(guard);
        if first && n == 0 {
            metrics::counter!(METRIC_SKIPPED_BY_RECHECK, "table" => step.table).increment(1);
            tracing::debug!(event = "retention_did_skipped_by_recheck", table = step.table, did, "DID re-entered scope before its rows were deleted");
        }
        first = false;
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
        let private_authors = self.inputs.private_row_authors().await?;
        let mut steps = scope_steps(private_authors);
        steps.extend(age_cap_steps(settings.max_age_days));
        steps.extend(time_steps(settings));
        let mut report = run_steps(pool, scope, &steps).await?;
        self.inputs.subscription_tombstones_aged().await?;
        report.deleted.push(("activity_subscriptions", sweep_subscriptions(pool).await?));
        Ok(report)
    }
}

/// Delete activity subscriptions whose subject is no longer indexed
/// (design-v3 §5.6 "Subjects that leave S"): such a subscription can never
/// notify, so it is removed, not shown as active. In batches, until none is
/// left.
pub(crate) async fn sweep_subscriptions(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let mut total = 0;
    loop {
        let n = sqlx::query(
            "DELETE FROM activity_subscriptions a WHERE a.ctid IN ( \
               SELECT a2.ctid FROM activity_subscriptions a2 \
               WHERE NOT EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = a2.subject_did) \
               LIMIT 10000)",
        )
        .execute(pool)
        .await?
        .rows_affected();
        total += n;
        if n == 0 {
            if total > 0 {
                metrics::counter!(METRIC_SUBSCRIPTIONS_OUT_OF_SCOPE, "action" => "swept").increment(total);
            }
            return Ok(total);
        }
        tokio::time::sleep(BATCH_PAUSE).await;
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
            trend_aggregate_hours: 72,
        };
        let mut steps = scope_steps(vec!["did:plc:author".into()]);
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
        let tables: Vec<&str> = scope_steps(vec![]).iter().map(|s| s.table).collect();
        assert_eq!(
            tables,
            [
                "posts",
                "likes",
                "reposts",
                "follows",
                "blocks",
                "feed_items",
                "lists",
                "list_items",
                "list_blocks",
                "list_optouts",
                "starter_packs",
                "labeler_services",
                "actor_self_records",
                "nearhorizon_poll_posts",
                "threadgates",
                "postgates",
                "starter_pack_counts",
                "post_counts",
                "actors",
                "actor_counts"
            ]
        );
    }

    /// design-v3 §14.1: the rule-4 subscription tables and the
    /// AppView-private tables are never retention's to delete.
    #[test]
    fn subscription_and_viewer_private_tables_are_not_swept() {
        for step in all_steps() {
            for kept in [
                "subscribed_lists",
                "subscribed_list_items",
                "subscribed_owners",
                "list_references",
                "list_mutes",
                "thread_mutes",
                "bookmarks",
                "drafts",
                "activity_subscriptions",
                "notification_preferences_v2",
            ] {
                assert_ne!(step.table, kept);
            }
        }
        // A gate outlives its author's scope while its post row is kept.
        let steps = scope_steps(vec![]);
        let gate = steps.iter().find(|s| s.table == "threadgates").expect("gate step");
        assert!(gate.sql.contains("NOT EXISTS (SELECT 1 FROM posts x WHERE x.uri = tg.post_uri)"));
    }

    #[test]
    fn class_m_exceptions_are_present() {
        let steps = scope_steps(vec![]);
        let sql = |t: &str| steps.iter().find(|s| s.table == t).map(|s| s.sql.clone()).unwrap_or_default();
        assert!(sql("posts").contains("split_part(p.reply_parent_uri, '/', 3)"));
        assert!(sql("posts").contains("split_part(p.reply_root_uri, '/', 3)"));
        assert!(sql("posts").contains("split_part(p.quote_uri, '/', 3)"));
        assert!(sql("likes").contains("split_part(l.subject_uri, '/', 3)"));
        assert!(sql("follows").contains("m.did IN (f.subject_did)"));
        // Rule M (member-blocks-only design §8): blocks and list blocks by
        // member side, not by scope.
        assert!(sql("blocks").contains("m.did IN (b.author_did)") && sql("blocks").contains("m.did IN (b.subject_did)"));
        assert!(!sql("blocks").contains("scope_dids"));
        assert!(sql("list_blocks").contains("m.did IN (lb.author_did)") && !sql("list_blocks").contains("scope_dids"));
    }

    #[test]
    fn binds_carry_private_inputs_and_days() {
        let steps = scope_steps(vec!["did:plc:a".into()]);
        let blocks = steps.iter().find(|s| s.table == "blocks").expect("blocks step");
        assert_eq!(blocks.bind, Bind::None);
        let actors = steps.iter().find(|s| s.table == "actors").expect("actors step");
        assert_eq!(actors.bind, Bind::Dids(vec!["did:plc:a".into()]));

        let settings = RetentionSettings {
            tombstone_days: 7,
            notification_days: 90,
            max_age_days: 0,
            trend_aggregate_hours: 72,
        };
        let t = time_steps(&settings);
        let bind = |table: &str| t.iter().find(|s| s.table == table).map(|s| s.bind.clone());
        assert_eq!(bind("record_tombstones"), Some(Bind::Days(7)));
        assert_eq!(bind("subscription_tombstones"), Some(Bind::Days(7)));
        assert_eq!(bind("notifications"), Some(Bind::Days(90)));
        assert_eq!(bind("constellation_counts"), Some(Bind::Days(7)));
        assert_eq!(bind("did_cache"), Some(Bind::Days(7)));
        assert_eq!(bind("backfill_jobs"), Some(Bind::Days(30)));
        assert_eq!(bind("admin_commands"), Some(Bind::Days(30)));
        assert_eq!(bind("admin_logins"), Some(Bind::Days(90)));
        assert_eq!(bind("admin_sessions"), Some(Bind::None));
        assert_eq!(bind("admin_oauth_states"), Some(Bind::None));
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
            trend_aggregate_hours: 72,
        };
        let t = time_steps(&settings);
        let jobs = t.iter().find(|s| s.table == "backfill_jobs").expect("jobs step");
        assert!(jobs.sql.contains("('done', 'failed', 'unavailable', 'cancelled')"));
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
            trend_aggregate_hours: 72,
        })
        .remove(0);
        assert!(run_batch(&pool, &scope, &step).await.is_err());
        assert_eq!(scope.reads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(scope.lock.try_write().is_ok(), "guard released after the batch");
    }

    #[test]
    fn scope_steps_run_per_did_and_time_steps_do_not() {
        for step in scope_steps(vec![]) {
            let candidates = step.candidates.as_deref().unwrap_or_else(|| panic!("{} per DID", step.table));
            assert!(candidates.starts_with("SELECT DISTINCT "), "{}", step.table);
            assert!(candidates.ends_with("LIMIT 500"), "{}", step.table);
        }
        let settings = RetentionSettings {
            tombstone_days: 7,
            notification_days: 90,
            max_age_days: 30,
            trend_aggregate_hours: 72,
        };
        for step in time_steps(&settings).into_iter().chain(age_cap_steps(30)) {
            assert!(step.candidates.is_none(), "{} uses row batches", step.table);
        }
    }

    #[test]
    fn per_did_delete_binds_the_did_after_the_step_bind() {
        let steps = scope_steps(vec![]);
        let sql = |t: &str| steps.iter().find(|s| s.table == t).map(|s| s.sql.clone()).unwrap_or_default();
        assert!(sql("posts").contains("WHERE p.author_did = $1 AND "));
        assert!(sql("feed_items").contains("WHERE fi.originator_did = $1 AND "));
        assert!(sql("post_counts").contains("WHERE split_part(pc.post_uri, '/', 3) = $1 AND "));
        assert!(sql("blocks").contains("WHERE b.author_did = $1 AND "));
        // actors binds a DID array first.
        assert!(sql("actors").contains("WHERE a.did = $2 AND "));
    }

    #[test]
    fn did_lock_uses_the_design_key() {
        assert_eq!(
            LOCK_DID_SQL,
            "SELECT pg_advisory_xact_lock(hashtextextended('did:' || $1, 0))"
        );
    }

    /// Tests against a real Postgres 16. They run when `HPRS_TEST_DATABASE_URL`
    /// names a server the tests may create databases on; otherwise they pass
    /// without doing anything.
    mod pg {
        use super::*;

        const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

        struct NoPrivate;

        #[async_trait]
        impl RetentionInputs for NoPrivate {
            async fn private_row_authors(&self) -> Result<Vec<String>, sqlx::Error> {
                Ok(Vec::new())
            }
        }

        async fn fresh_db() -> Option<crate::test_support::TestDb> {
            crate::test_support::fresh_db("retention").await
        }

        async fn insert_post(pool: &PgPool) -> String {
            let uri = format!("at://{DID}/app.bsky.feed.post/3kaaaaaaaaaaa");
            sqlx::query(
                "INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at) \
                 VALUES ($1, $2, '3kaaaaaaaaaaa', 'bafyreitest', '3kaaaaaaaaaaa', '{}'::jsonb, now())",
            )
            .bind(&uri)
            .bind(DID)
            .execute(pool)
            .await
            .expect("insert post");
            uri
        }

        async fn post_exists(pool: &PgPool, uri: &str) -> bool {
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM posts WHERE uri = $1)")
                .bind(uri)
                .fetch_one(pool)
                .await
                .expect("query post")
        }

        async fn run_public(pool: &PgPool) -> RetentionReport {
            let settings = RetentionSettings {
                tombstone_days: 7,
                notification_days: 90,
                max_age_days: 0,
                trend_aggregate_hours: 72,
            };
            PublicRetention::new(Arc::new(NoPrivate))
                .run(pool, &lock_scope(), &settings)
                .await
                .expect("retention run")
        }

        #[tokio::test]
        async fn out_of_scope_rows_are_deleted() {
            let Some(db) = fresh_db().await else { return };
            let uri = insert_post(&db.pool).await;
            run_public(&db.pool).await;
            assert!(!post_exists(&db.pool, &uri).await);
            db.drop_db().await;
        }

        /// B-75: a subscription whose subject left the indexed set is
        /// swept; one whose subject is still in it stays.
        #[tokio::test]
        async fn subscriptions_to_subjects_out_of_scope_are_swept() {
            let Some(db) = fresh_db().await else { return };
            let gone = "did:plc:gggggggggggggggggggggggg";
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)")
                .bind(DID)
                .execute(&db.pool)
                .await
                .expect("insert scope");
            for subject in [DID, gone] {
                sqlx::query("INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply) VALUES ('did:plc:m', $1, true, false)")
                    .bind(subject)
                    .execute(&db.pool)
                    .await
                    .expect("subscription");
            }
            let report = run_public(&db.pool).await;
            assert!(report.deleted.contains(&("activity_subscriptions", 1)));
            let left: Vec<String> = sqlx::query_scalar("SELECT subject_did FROM activity_subscriptions")
                .fetch_all(&db.pool)
                .await
                .expect("left");
            assert_eq!(left, [DID]);
            db.drop_db().await;
        }

        #[tokio::test]
        async fn did_in_scope_at_recheck_is_skipped() {
            let Some(db) = fresh_db().await else { return };
            let uri = insert_post(&db.pool).await;
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)")
                .bind(DID)
                .execute(&db.pool)
                .await
                .expect("insert scope");
            run_public(&db.pool).await;
            assert!(post_exists(&db.pool, &uri).await);
            db.drop_db().await;
        }

        /// The race: growth holds the subject DID's shared lock with its
        /// `scope_dids` insert not yet committed when retention reaches the
        /// DID. Retention must wait for it and then keep the rows.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn in_flight_growth_wins_over_retention() {
            let Some(db) = fresh_db().await else { return };
            let uri = insert_post(&db.pool).await;

            let mut growth = db.pool.begin().await.expect("begin growth");
            sqlx::query("SELECT pg_advisory_xact_lock_shared(hashtextextended('did:' || $1, 0))")
                .bind(DID)
                .execute(&mut *growth)
                .await
                .expect("shared DID lock");
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)")
                .bind(DID)
                .execute(&mut *growth)
                .await
                .expect("uncommitted scope insert");

            let pool = db.pool.clone();
            let retention = tokio::spawn(async move { run_public(&pool).await });
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(!retention.is_finished(), "retention waits for the growth transaction");

            growth.commit().await.expect("commit growth");
            retention.await.expect("retention task");
            assert!(post_exists(&db.pool, &uri).await, "the re-entered DID keeps its rows");
            db.drop_db().await;
        }

        const MEMBER: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
        const SCOPED: &str = "did:plc:ssssssssssssssssssssssss";
        const STRANGER: &str = "did:plc:tttttttttttttttttttttttt";
        const LONE: &str = "did:plc:llllllllllllllllllllllll";
        const OTHER: &str = "did:plc:oooooooooooooooooooooooo";

        async fn exec(pool: &PgPool, sql: &str) {
            sqlx::query(sql).execute(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        }

        async fn block(pool: &PgPool, author: &str, rkey: &str, subject: &str) {
            exec(pool, &format!(
                "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) \
                 VALUES ('at://{author}/app.bsky.graph.block/{rkey}', '{author}', '{subject}', '3k', now())"
            ))
            .await;
        }

        async fn list_block(pool: &PgPool, author: &str) {
            exec(pool, &format!(
                "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) \
                 VALUES ('at://{author}/app.bsky.graph.listblock/3klb', '{author}', '3klb', 'bafy', '3k', 'at://{OTHER}/app.bsky.graph.list/3kl', now())"
            ))
            .await;
        }

        async fn rows(pool: &PgPool, sql: &str) -> Vec<String> {
            sqlx::query_scalar(sql).fetch_all(pool).await.expect(sql)
        }

        /// T-MB4 (member-blocks-only design §8, §12): one run deletes
        /// exactly the blocks and list blocks with no member side, scope or
        /// not; an identity row out of S whose only rows were such blocks
        /// goes, and one in S stays.
        #[tokio::test]
        async fn blocks_with_no_member_side_are_deleted() {
            let Some(db) = fresh_db().await else { return };
            let p = &db.pool;
            exec(p, &format!("INSERT INTO members (did, source) VALUES ('{MEMBER}', 'admin')")).await;
            exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{MEMBER}', 1), ('{SCOPED}', 4)")).await;
            for d in [SCOPED, LONE] {
                exec(p, &format!(
                    "INSERT INTO actors (did, handle, pds_endpoint, profile_record) VALUES ('{d}', '{d}', 'https://pds.example.com', '{{}}'::jsonb)"
                ))
                .await;
            }
            block(p, STRANGER, "nn", OTHER).await; // no member side: goes
            block(p, STRANGER, "nm", MEMBER).await; // names a member: stays
            block(p, MEMBER, "mn", OTHER).await; // a member's: stays
            block(p, SCOPED, "sn", OTHER).await; // in S, no member side: goes
            block(p, LONE, "ln", OTHER).await; // out of S, its only row: goes
            list_block(p, SCOPED).await; // a non-member's: goes
            list_block(p, MEMBER).await; // a member's: stays

            let report = run_public(p).await;
            assert!(report.deleted.contains(&("blocks", 3)), "{report:?}");
            assert!(report.deleted.contains(&("list_blocks", 1)), "{report:?}");
            let left = rows(p, "SELECT uri FROM blocks ORDER BY uri").await;
            assert_eq!(
                left,
                [format!("at://{MEMBER}/app.bsky.graph.block/mn"), format!("at://{STRANGER}/app.bsky.graph.block/nm")]
            );
            assert_eq!(rows(p, "SELECT author_did FROM list_blocks").await, [MEMBER]);
            // The identity row out of S went with its blocks; the one in S stays.
            assert_eq!(rows(p, "SELECT did FROM actors ORDER BY did").await, [SCOPED]);
            db.drop_db().await;
        }

        /// T-MB4: a member added while retention reaches the DID (the
        /// membership commits under the DID's lock) keeps its blocks.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_member_added_meanwhile_keeps_its_blocks() {
            let Some(db) = fresh_db().await else { return };
            block(&db.pool, SCOPED, "sn", OTHER).await;
            list_block(&db.pool, SCOPED).await;

            let mut join = db.pool.begin().await.expect("begin");
            sqlx::query("SELECT pg_advisory_xact_lock_shared(hashtextextended('did:' || $1, 0))")
                .bind(SCOPED)
                .execute(&mut *join)
                .await
                .expect("shared DID lock");
            sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')")
                .bind(SCOPED)
                .execute(&mut *join)
                .await
                .expect("uncommitted member");

            let pool = db.pool.clone();
            let retention = tokio::spawn(async move { run_public(&pool).await });
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(!retention.is_finished(), "retention waits for the membership");
            join.commit().await.expect("commit member");
            retention.await.expect("retention task");
            assert_eq!(rows(&db.pool, "SELECT author_did FROM blocks").await, [SCOPED]);
            assert_eq!(rows(&db.pool, "SELECT author_did FROM list_blocks").await, [SCOPED]);
            db.drop_db().await;
        }
    }

    #[test]
    fn settings_from_config() {
        let s = RetentionSettings::from(&RetentionConfig::default());
        assert_eq!(
            s,
            RetentionSettings {
                tombstone_days: 7,
                notification_days: 90,
                max_age_days: 0,
                trend_aggregate_hours: 72,
            }
        );
    }
}
