//! Full trials: full indexing switched on for a bounded time, keeping a
//! record of every row it stores that the mode it started from would not
//! have stored. When it ends, Prism leaves full through full's own exit,
//! checks that every recorded row is gone or kept for a reason that arose
//! during the trial, says so in plain words, and drops the record.
//!
//! The trial rides `public_transition` as its `trial` object, so every phase
//! change carries it. The writer records in the transaction of the row it
//! records; the bounds are checked by a supervised task and by the storage
//! sampler, either of which ends the trial.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};

use super::public_mode::{Breadth, PublicIndexing, PublicMode, ScopeValue, now, parse_ts, rfc3339};

/// `full_trial_state{state}`: 1 for the trial's state, 0 otherwise.
pub const METRIC_STATE: &str = "full_trial_state";
/// `full_trial_recorded_rows_total{table}`.
pub const METRIC_RECORDED: &str = "full_trial_recorded_rows_total";
/// `full_trial_database_growth_bytes`.
pub const METRIC_GROWTH: &str = "full_trial_database_growth_bytes";
/// `full_trial_check_rows{result}`.
pub const METRIC_CHECK_ROWS: &str = "full_trial_check_rows";

/// The bounds checker's interval.
pub const CHECK_EVERY: Duration = Duration::from_secs(60);
/// Health warns when the checker has not stamped for this long.
pub const STALE_AFTER: TimeDelta = TimeDelta::minutes(5);

/// A trial's state while it rides the transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialState {
    /// Full runs and the record is written.
    Running,
    /// Full's exit (a narrowing or a drain) runs.
    Ending,
    /// The exit ended; the check runs.
    Checking,
    /// The check found faults; the record is kept until it passes or is
    /// dismissed.
    Failed,
}

impl TrialState {
    pub const ALL: [Self; 4] = [Self::Running, Self::Ending, Self::Checking, Self::Failed];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Ending => "ending",
            Self::Checking => "checking",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.as_str() == s)
    }
}

/// The `trial` object of `public_transition`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trial {
    /// `full_trials.id`.
    pub id: i64,
    /// The start mode.
    pub from: ScopeValue,
    /// The mode the exit goes to: the start mode unless a switch changed it.
    pub to: ScopeValue,
    pub state: TrialState,
    /// When the baseline was taken.
    pub measured_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub max_added_bytes: u64,
    pub end_below_free_percent: u32,
    pub history: bool,
    pub baseline_db_bytes: u64,
    /// The transition's `last_fill_since` before the switch.
    pub start_last_fill_since: Option<DateTime<Utc>>,
    /// What ended it: `time`, `growth`, `free_space`, `manual`, `startup` or
    /// `switch`.
    pub ended_by: Option<String>,
    /// Progress: the per-table record counts, the start mode's own writes,
    /// the full-arm decisions and the checker's stamp.
    pub counters: Value,
    /// The check's cursor and, once done, its result.
    pub check: Value,
}

impl Trial {
    /// The stored JSON.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "from": self.from.as_str(),
            "to": self.to.as_str(),
            "state": self.state.as_str(),
            "measured_at": rfc3339(&self.measured_at),
            "started_at": rfc3339(&self.started_at),
            "ends_at": rfc3339(&self.ends_at),
            "max_added_bytes": self.max_added_bytes,
            "end_below_free_percent": self.end_below_free_percent,
            "history": self.history,
            "baseline_db_bytes": self.baseline_db_bytes,
            "start_last_fill_since": self.start_last_fill_since.as_ref().map(rfc3339),
            "ended_by": self.ended_by,
            "counters": self.counters,
            "check": self.check,
        })
    }

    /// A stored trial object, or `None` when any field does not validate.
    #[must_use]
    pub fn parse(v: &Value) -> Option<Self> {
        let settled = |k: &str| match v.get(k)?.as_str()? {
            "community" => Some(ScopeValue::Community),
            "kryphocron_only" => Some(ScopeValue::KryphocronOnly),
            _ => None,
        };
        let start_last_fill_since = match v.get("start_last_fill_since") {
            None | Some(Value::Null) => None,
            Some(t) => Some(parse_ts(t)?),
        };
        let ended_by = match v.get("ended_by") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => return None,
        };
        let measured_at = parse_ts(v.get("measured_at")?)?;
        let started_at = parse_ts(v.get("started_at")?)?;
        let ends_at = parse_ts(v.get("ends_at")?)?;
        if started_at < measured_at {
            return None;
        }
        Some(Self {
            id: v.get("id")?.as_i64().filter(|id| *id > 0)?,
            from: settled("from")?,
            to: settled("to")?,
            state: TrialState::parse(v.get("state")?.as_str()?)?,
            measured_at,
            started_at,
            ends_at,
            max_added_bytes: v.get("max_added_bytes")?.as_u64()?,
            end_below_free_percent: u32::try_from(v.get("end_below_free_percent")?.as_u64()?).ok().filter(|p| *p <= 100)?,
            history: v.get("history")?.as_bool()?,
            baseline_db_bytes: v.get("baseline_db_bytes")?.as_u64()?,
            start_last_fill_since,
            ended_by,
            counters: v.get("counters").cloned().filter(Value::is_object).unwrap_or_else(|| json!({})),
            check: v.get("check").cloned().filter(Value::is_object).unwrap_or_else(|| json!({})),
        })
    }

    /// Whether the trial's state fits the transition it rides: running in
    /// breadth Full, ending outside it, checking in a settled phase.
    #[must_use]
    pub fn fits(&self, breadth: Breadth, mode: PublicMode) -> bool {
        match self.state {
            TrialState::Running => breadth == Breadth::Full,
            TrialState::Ending => breadth != Breadth::Full,
            TrialState::Checking => breadth == Breadth::Community && matches!(mode, PublicMode::On | PublicMode::Off),
            TrialState::Failed => true,
        }
    }

    /// The start mode's admission view: its mode and breadth.
    #[must_use]
    pub fn start_view(&self) -> (PublicMode, Breadth) {
        match self.from {
            ScopeValue::KryphocronOnly => (PublicMode::Off, Breadth::Community),
            _ => (PublicMode::On, Breadth::Community),
        }
    }
}

/// What the writer needs of a running trial, held in the scope sets: the
/// start mode's view, and the counters it adds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recording {
    pub mode: PublicMode,
    pub breadth: Breadth,
    pub from: ScopeValue,
}

/// The trial's in-memory counters, flushed to `trial.counters` every
/// minute and at every phase change.
#[derive(Default)]
pub struct TrialCounters {
    /// Full-arm admission decisions.
    full: AtomicU64,
    recorded: Mutex<BTreeMap<String, u64>>,
    /// The start mode's own writes: shadow-admitted written ops, by table.
    start_writes: Mutex<BTreeMap<String, u64>>,
}

impl std::fmt::Debug for TrialCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrialCounters").field("full", &self.full.load(Ordering::Relaxed)).finish_non_exhaustive()
    }
}

impl PartialEq for TrialCounters {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for TrialCounters {}

impl TrialCounters {
    pub fn full_decision(&self) {
        self.full.fetch_add(1, Ordering::Relaxed);
    }

    pub fn recorded(&self, table: &'static str, n: u64) {
        metrics::counter!(METRIC_RECORDED, "table" => table).increment(n);
        let mut m = self.recorded.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *m.entry(table.to_string()).or_default() += n;
    }

    pub fn start_write(&self, table: &'static str) {
        let mut m = self.start_writes.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *m.entry(table.to_string()).or_default() += 1;
    }

    /// Start from stored counters (a restart).
    pub fn load(&self, v: &Value) {
        self.full.store(v.get("full_decisions").and_then(Value::as_u64).unwrap_or(0), Ordering::Relaxed);
        let map = |k: &str| -> BTreeMap<String, u64> {
            v.get(k)
                .and_then(Value::as_object)
                .map(|o| o.iter().filter_map(|(t, n)| n.as_u64().map(|n| (t.clone(), n))).collect())
                .unwrap_or_default()
        };
        *self.recorded.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = map("recorded");
        *self.start_writes.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = map("start_writes");
    }

    /// Back to zero, for a new trial.
    pub fn reset(&self) {
        self.load(&json!({}));
    }

    /// The counters as stored, with the checker's stamp.
    #[must_use]
    pub fn to_json(&self, checked_at: Option<DateTime<Utc>>) -> Value {
        let recorded = self.recorded.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        let start = self.start_writes.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        json!({
            "full_decisions": self.full.load(Ordering::Relaxed),
            "recorded": recorded,
            "start_writes": start,
            "checked_at": checked_at.as_ref().map(rfc3339),
        })
    }

    #[must_use]
    pub fn full_decisions(&self) -> u64 {
        self.full.load(Ordering::Relaxed)
    }
}

/// A table the record names, with its code in `full_trial_rows.tbl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i16)]
pub enum TrialTable {
    Posts = 1,
    Likes = 2,
    Reposts = 3,
    Follows = 4,
    Blocks = 5,
    Lists = 6,
    ListItems = 7,
    ListBlocks = 8,
    ListOptouts = 9,
    StarterPacks = 10,
    Threadgates = 11,
    Postgates = 12,
    PollPosts = 13,
    LabelerServices = 14,
    ActorSelfRecords = 15,
    Actors = 16,
    RecordTombstones = 17,
    FeedItems = 18,
}

impl TrialTable {
    pub const ALL: [Self; 18] = [
        Self::Posts,
        Self::Likes,
        Self::Reposts,
        Self::Follows,
        Self::Blocks,
        Self::Lists,
        Self::ListItems,
        Self::ListBlocks,
        Self::ListOptouts,
        Self::StarterPacks,
        Self::Threadgates,
        Self::Postgates,
        Self::PollPosts,
        Self::LabelerServices,
        Self::ActorSelfRecords,
        Self::Actors,
        Self::RecordTombstones,
        Self::FeedItems,
    ];

    #[must_use]
    pub fn code(self) -> i16 {
        self as i16
    }

    #[must_use]
    pub fn from_code(code: i16) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.code() == code)
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Posts => "posts",
            Self::Likes => "likes",
            Self::Reposts => "reposts",
            Self::Follows => "follows",
            Self::Blocks => "blocks",
            Self::Lists => "lists",
            Self::ListItems => "list_items",
            Self::ListBlocks => "list_blocks",
            Self::ListOptouts => "list_optouts",
            Self::StarterPacks => "starter_packs",
            Self::Threadgates => "threadgates",
            Self::Postgates => "postgates",
            Self::PollPosts => "nearhorizon_poll_posts",
            Self::LabelerServices => "labeler_services",
            Self::ActorSelfRecords => "actor_self_records",
            Self::Actors => "actors",
            Self::RecordTombstones => "record_tombstones",
            Self::FeedItems => "feed_items",
        }
    }

    /// The record table of a keyed row's table name.
    #[must_use]
    pub fn of_table(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.name() == name)
    }

    /// The SQL that a stored row `x` of the table has the record key `r`.
    #[must_use]
    pub fn key_match(self) -> &'static str {
        match self {
            Self::LabelerServices => "x.author = r.a",
            Self::ActorSelfRecords => "x.author = r.a AND x.coll = r.c",
            // A profile counts as there while the trial's profile is.
            Self::Actors => "x.id = r.a AND x.profile_rev = r.rev",
            Self::RecordTombstones => "x.author = r.a AND x.rkey = r.b AND x.coll = r.c",
            Self::FeedItems => "x.originator = r.a AND x.src_rkey = r.b AND x.is_repost = (r.c = 1)",
            _ => "x.author = r.a AND x.rkey = r.b",
        }
    }

    /// The column naming the row's account.
    #[must_use]
    pub fn account_column(self) -> &'static str {
        match self {
            Self::Actors => "id",
            Self::FeedItems => "originator",
            _ => "author",
        }
    }
}

/// One record entry, as the writer collects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub tbl: TrialTable,
    pub a: i32,
    pub b: i64,
    pub c: i16,
    pub rev: Option<i64>,
    pub s: Option<(i32, Option<i64>)>,
    pub q: Option<(i32, i64)>,
}

impl Entry {
    #[must_use]
    pub fn new(tbl: TrialTable, a: i32, b: i64, c: i16) -> Self {
        Self { tbl, a, b, c, rev: None, s: None, q: None }
    }
}

/// Write a transaction's entries: one statement. A profile entry's rev only
/// rises; every other entry is written once.
///
/// # Errors
///
/// Database errors.
pub async fn insert_entries(conn: &mut PgConnection, entries: &[Entry]) -> Result<(), sqlx::Error> {
    if entries.is_empty() {
        return Ok(());
    }
    // One entry per key: a statement may not touch a row twice.
    let mut by_key: BTreeMap<(i16, i32, i64, i16), Entry> = BTreeMap::new();
    for e in entries {
        by_key
            .entry((e.tbl.code(), e.a, e.b, e.c))
            .and_modify(|old| {
                old.rev = old.rev.max(e.rev);
                old.s = old.s.or(e.s);
                old.q = old.q.or(e.q);
            })
            .or_insert(*e);
    }
    let es: Vec<&Entry> = by_key.values().collect();
    let tbl: Vec<i16> = es.iter().map(|e| e.tbl.code()).collect();
    let a: Vec<i32> = es.iter().map(|e| e.a).collect();
    let b: Vec<i64> = es.iter().map(|e| e.b).collect();
    let c: Vec<i16> = es.iter().map(|e| e.c).collect();
    let rev: Vec<Option<i64>> = es.iter().map(|e| e.rev).collect();
    let s_a: Vec<Option<i32>> = es.iter().map(|e| e.s.map(|s| s.0)).collect();
    let s_b: Vec<Option<i64>> = es.iter().map(|e| e.s.and_then(|s| s.1)).collect();
    let q_a: Vec<Option<i32>> = es.iter().map(|e| e.q.map(|q| q.0)).collect();
    let q_b: Vec<Option<i64>> = es.iter().map(|e| e.q.map(|q| q.1)).collect();
    sqlx::query(
        "INSERT INTO full_trial_rows (tbl, a, b, c, rev, s_a, s_b, q_a, q_b) \
         SELECT * FROM unnest($1::smallint[], $2::integer[], $3::bigint[], $4::smallint[], $5::bigint[], \
                              $6::integer[], $7::bigint[], $8::integer[], $9::bigint[]) \
         ON CONFLICT (tbl, a, b, c) DO UPDATE SET rev = GREATEST(full_trial_rows.rev, EXCLUDED.rev) \
         WHERE EXCLUDED.rev IS NOT NULL AND EXCLUDED.rev > COALESCE(full_trial_rows.rev, EXCLUDED.rev - 1)",
    )
    .bind(&tbl)
    .bind(&a)
    .bind(&b)
    .bind(&c)
    .bind(&rev)
    .bind(&s_a)
    .bind(&s_b)
    .bind(&q_a)
    .bind(&q_b)
    .execute(conn)
    .await?;
    Ok(())
}

/// Drop the record: its rows, emptied with `TRUNCATE`, which returns their
/// disk at once, and the fill's jobs, deleted (a few rows per member and S
/// account), so a fill batch still open does not hold the drop up.
///
/// # Errors
///
/// Database errors.
pub async fn drop_record(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("TRUNCATE full_trial_rows").execute(&mut *conn).await?;
    sqlx::query("DELETE FROM full_trial_jobs").execute(&mut *conn).await?;
    Ok(())
}

/// Record the `resync` jobs of `dids` that the fill's enqueue is about to
/// touch, as they are now, in the enqueue's transaction: a missing job, or
/// one not queued (a queued job the enqueue leaves alone). A job already
/// recorded keeps its first entry.
///
/// # Errors
///
/// Database errors.
pub async fn record_fill_jobs(conn: &mut PgConnection, dids: &[String]) -> Result<(), sqlx::Error> {
    if dids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO full_trial_jobs (did, depth, existed, state, attempts, not_before, last_error, updated_at, generation, gap_epoch, \
           rerun_requested, priority, refract_cursor, fill_generation) \
         SELECT d, 'resync', j.did IS NOT NULL, j.state, j.attempts, j.not_before, j.last_error, j.updated_at, j.generation, j.gap_epoch, \
           j.rerun_requested, j.priority, j.refract_cursor, COALESCE(j.generation + 1, 0) \
         FROM unnest($1::text[]) AS t(d) LEFT JOIN backfill_jobs j ON j.did = t.d AND j.depth = 'resync' \
         WHERE j.did IS NULL OR j.state <> 'queued' \
         ON CONFLICT (did, depth) DO NOTHING",
    )
    .bind(dids)
    .execute(conn)
    .await?;
    Ok(())
}

/// A recorded job the fill still holds: a job it created that is still
/// there with the fill's generation, or one it queued again that is still
/// queued or running with it.
const FILL_JOB_HELD: &str = "j.generation = r.fill_generation AND (NOT r.existed OR j.state IN ('queued', 'running'))";

/// Put the fill's jobs back as they were before the trial. A job the fill
/// created goes; one it queued again gets its earlier state, unless the
/// start mode queued it again since (a newer generation, still queued or
/// running), which is left for the start mode. A job that was running
/// before is queued once more, with a new generation, since its run was
/// superseded. A worker still running the fill's generation finds its row
/// changed and leaves it. Returns the jobs put back and removed.
///
/// # Errors
///
/// Database errors.
pub async fn restore_fill_jobs(conn: &mut PgConnection) -> Result<(u64, u64), sqlx::Error> {
    let restored = sqlx::query(
        "UPDATE backfill_jobs j SET \
           state = CASE WHEN r.state = 'running' THEN 'queued' ELSE r.state END, \
           attempts = r.attempts, \
           not_before = CASE WHEN r.state = 'running' THEN now() ELSE r.not_before END, \
           last_error = r.last_error, \
           updated_at = CASE WHEN r.state = 'running' THEN now() ELSE r.updated_at END, \
           generation = CASE WHEN r.state = 'running' THEN j.generation + 1 ELSE r.generation END, \
           gap_epoch = r.gap_epoch, rerun_requested = r.rerun_requested, priority = r.priority, refract_cursor = r.refract_cursor \
         FROM full_trial_jobs r \
         WHERE r.existed AND j.did = r.did AND j.depth = r.depth \
           AND (j.generation = r.fill_generation OR (j.generation > r.fill_generation AND j.state NOT IN ('queued', 'running')))",
    )
    .execute(&mut *conn)
    .await?
    .rows_affected();
    let removed = sqlx::query(
        "DELETE FROM backfill_jobs j USING full_trial_jobs r \
         WHERE NOT r.existed AND j.did = r.did AND j.depth = r.depth \
           AND (j.generation = r.fill_generation OR j.state NOT IN ('queued', 'running'))",
    )
    .execute(&mut *conn)
    .await?
    .rows_affected();
    Ok((restored, removed))
}

/// The recorded jobs the fill still holds (none once they are put back).
///
/// # Errors
///
/// Database errors.
pub async fn fill_jobs_held(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(&format!("SELECT count(*) FROM full_trial_jobs r JOIN backfill_jobs j ON j.did = r.did AND j.depth = r.depth WHERE {FILL_JOB_HELD}"))
        .fetch_one(pool)
        .await
}

/// Account ids per statement when the count rows are compared.
const COUNT_RANGE: i32 = 1 << 20;

/// Posts that stored likes, reposts, replies or quotes name and that have
/// no count row above zero: rows the counts should hold and do not. Read
/// in ranges of the subject's account id, one short statement each.
///
/// # Errors
///
/// Database errors.
pub async fn missing_post_counts(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let max: Option<i32> = sqlx::query_scalar("SELECT max(id) FROM dids").fetch_one(pool).await?;
    let max = max.unwrap_or(0);
    let mut total = 0;
    let mut lo = 0;
    while lo <= max {
        let hi = lo.saturating_add(COUNT_RANGE);
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ( \
               SELECT subj_author AS a, subj_rkey AS k FROM likes WHERE subj_coll = 1 AND subj_author >= $1 AND subj_author < $2 \
               UNION SELECT subj_author, subj_rkey FROM reposts WHERE subj_coll = 1 AND subj_author >= $1 AND subj_author < $2 \
               UNION SELECT parent_author, parent_rkey FROM posts WHERE parent_coll = 1 AND parent_author >= $1 AND parent_author < $2 \
               UNION SELECT quote_author, quote_rkey FROM posts WHERE quote_coll = 1 AND quote_author >= $1 AND quote_author < $2 \
             ) s WHERE a IS NOT NULL AND k IS NOT NULL AND NOT EXISTS (SELECT 1 FROM post_counts c WHERE c.author = s.a AND c.rkey = s.k \
               AND (c.like_count > 0 OR c.repost_count > 0 OR c.reply_count > 0 OR c.quote_count > 0))",
        )
        .bind(lo)
        .bind(hi)
        .fetch_one(pool)
        .await?;
        total += n;
        if hi == i32::MAX {
            break;
        }
        lo = hi;
    }
    Ok(total)
}

/// The trend hours still within the trends cutoff, each with its rows that
/// count anything: `{hour: rows}`.
///
/// # Errors
///
/// Database errors.
pub async fn trend_hours(pool: &PgPool) -> Result<BTreeMap<String, i64>, sqlx::Error> {
    let rows: Vec<(DateTime<Utc>, i64)> = sqlx::query_as(
        "SELECT hour, count(*) FILTER (WHERE posts > 0 OR likes > 0 OR reposts > 0 OR replies > 0) FROM tag_activity_hourly \
         WHERE hour >= trends_cutoff() GROUP BY hour",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(h, n)| (rfc3339(&h), n)).collect())
}

/// The trend rows of a trial from kryphocron_only, as its baseline keeps
/// them: `[[tag, hour as epoch seconds], …]`. That mode writes few, so the
/// whole set is kept, and any row outside it afterwards is the trial's: its
/// trigger could not take back a reply's count when the drain removed the
/// reply's parent first.
///
/// # Errors
///
/// Database errors.
pub async fn trend_keys(pool: &PgPool) -> Result<Value, sqlx::Error> {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(jsonb_build_array(tag, extract(epoch FROM hour)::bigint)), '[]'::jsonb) FROM tag_activity_hourly")
        .fetch_one(pool)
        .await
}

/// A trend row `ta` that is not in the running trial's baseline keys
/// ([`trend_keys`]): false when the baseline has none (a trial from
/// community), so nothing is taken then.
pub const TREND_ROW_ADDED: &str = "EXISTS (SELECT 1 FROM full_trials tft \
       WHERE tft.id = (SELECT (tw.value #>> '{trial,id}')::bigint FROM worker_state tw WHERE tw.key = 'public_transition') \
         AND jsonb_typeof(tft.baseline->'trend_keys') = 'array' \
         AND NOT tft.baseline->'trend_keys' @> jsonb_build_array(jsonb_build_array(ta.tag, extract(epoch FROM ta.hour)::bigint)))";

/// How the trial treats a table of the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// In the record; removed by the exit; checked.
    Recorded,
    /// Follows its owner rows; checked through the record's keys.
    Derived,
    /// Written only in breadth Full; the exit empties it.
    FullOnly,
    /// A cache: not recorded; thinned by the exit or expiring.
    Cache,
    /// Kept by compact storage: reported, not judged.
    Kept,
    /// Written as the start mode would: not recorded.
    ModeIndependent,
}

/// The tables full writes only in breadth Full: the exit empties them.
pub const FULL_ONLY: [&str; 10] = [
    "full_crawl",
    "full_crawl_groups",
    "relay_account_status",
    "refract_excluded",
    "full_chain_links",
    "full_relist_candidates",
    "full_gaps",
    "full_incomplete",
    "crawl_identity",
    "full_labels",
];

/// The coverage table: every table of the schema in one class. A table in
/// no class fails the coverage test, so a new table must be placed here.
/// The private tier's tables (`private_` and the like) are class M by their
/// prefix.
#[must_use]
pub fn class_of(table: &str) -> Option<Class> {
    if TrialTable::of_table(table).is_some() {
        return Some(Class::Recorded);
    }
    if FULL_ONLY.contains(&table) {
        return Some(Class::FullOnly);
    }
    let prefixed = ["private_", "outline_", crate::system_accounts::TABLE_PREFIX, "admin_", "refract_", "subscription", "subscribed_", "list_reference", "member_", "notification", "plural_"];
    Some(match table {
        "post_counts" | "actor_counts" | "starter_pack_counts" | "odd_refs" | "post_search" | "tag_activity_hourly" => Class::Derived,
        "did_cache" | "constellation_counts" | "identity_refresh" | "learned_inbound_blocks" | "refract_record_reads" => Class::Cache,
        "dids" | "rkey_aliases" | "collections" | "record_dicts" => Class::Kept,
        "_sqlx_migrations" | "full_trials" | "full_trial_rows" | "full_trial_jobs" | "members" | "scope_dids" | "operator_settings" | "operator_settings_history"
        | "worker_state" | "backfill_jobs" | "sync_state" | "sync_chain_links" | "purging_dids" | "labels" | "mutes" | "list_mutes"
        | "thread_mutes" | "bookmarks" | "drafts" | "activity_subscriptions" | "notifications" | "purging" => Class::ModeIndependent,
        _ if prefixed.iter().any(|p| table.starts_with(p)) => Class::ModeIndependent,
        _ => return None,
    })
}

/// The bounds of a new trial.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bounds {
    pub hours: f64,
    pub max_added_gb: f64,
    pub end_below_free_percent: u32,
    pub history: bool,
}

impl Bounds {
    #[must_use]
    pub fn from_config(c: &crate::config::FullTrialConfig) -> Self {
        Self { hours: c.hours.0, max_added_gb: c.max_added_gb.0, end_below_free_percent: c.end_below_free_percent, history: c.history }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn max_added_bytes(self) -> u64 {
        gb_bytes(self.max_added_gb)
    }

    /// How long the trial runs.
    #[must_use]
    pub fn duration(self) -> TimeDelta {
        hours_delta(self.hours)
    }

    #[must_use]
    pub fn to_json(self) -> Value {
        json!({ "hours": self.hours, "max_added_gb": self.max_added_gb, "end_below_free_percent": self.end_below_free_percent, "history": self.history })
    }
}

/// GB as bytes; never negative.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn gb_bytes(gb: f64) -> u64 {
    (gb.max(0.0) * 1e9) as u64
}

/// Hours as a duration, to the second; never negative.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn hours_delta(hours: f64) -> TimeDelta {
    TimeDelta::seconds((hours.max(0.0) * 3600.0).round() as i64)
}

/// A trial's start, refused: the reason, in plain words.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Refused(pub String);

/// What [`PublicIndexing::start_trial`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Started {
    pub id: i64,
    pub from: ScopeValue,
    pub ends_at: DateTime<Utc>,
    pub action: &'static str,
}

/// Every table of the schema with its exact rows and bytes, each read in
/// its own statement with the time it was read.
///
/// # Errors
///
/// Database errors.
pub async fn table_figures(pool: &PgPool) -> Result<BTreeMap<String, Value>, sqlx::Error> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind = 'r' AND n.nspname = current_schema() ORDER BY 1",
    )
    .fetch_all(pool)
    .await?;
    let mut out = BTreeMap::new();
    for t in tables {
        let (rows, bytes): (i64, i64) =
            sqlx::query_as(&format!("SELECT (SELECT count(*) FROM \"{t}\"), pg_total_relation_size('\"{t}\"'::regclass)")).fetch_one(pool).await?;
        out.insert(t, json!({ "rows": rows, "bytes": bytes, "at": rfc3339(&now()) }));
    }
    Ok(out)
}

/// The `full_labels` rows trial `id`'s baseline counted, if it has one.
///
/// # Errors
///
/// Database errors.
pub async fn labels_at_start(pool: &PgPool, id: i64) -> Result<Option<i64>, sqlx::Error> {
    let rows: Option<Option<i64>> = sqlx::query_scalar("SELECT (baseline #>> '{tables,full_labels,rows}')::bigint FROM full_trials WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(rows.flatten())
}

/// `pg_database_size` of the current database.
///
/// # Errors
///
/// Database errors.
pub async fn database_bytes(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let n: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())").fetch_one(pool).await?;
    Ok(u64::try_from(n).unwrap_or(0))
}

/// The baseline (taken before the switch): every table's rows and bytes,
/// the database size and the volume's free space, the high-water marks of
/// the tables compact storage keeps, and the `worker_state` keys.
///
/// # Errors
///
/// Database errors.
pub async fn baseline(pool: &PgPool, free: Option<(u64, u64)>, from: ScopeValue) -> Result<Value, sqlx::Error> {
    let tables = table_figures(pool).await?;
    let db = database_bytes(pool).await?;
    let dids_max: Option<i32> = sqlx::query_scalar("SELECT max(id) FROM dids").fetch_one(pool).await?;
    let aliases_max: Option<i64> = sqlx::query_scalar("SELECT max(id) FROM rkey_aliases").fetch_one(pool).await?;
    let collections: i64 = sqlx::query_scalar("SELECT count(*) FROM collections").fetch_one(pool).await?;
    let dicts: i64 = sqlx::query_scalar("SELECT count(*) FROM record_dicts").fetch_one(pool).await?;
    let keys: Vec<String> = sqlx::query_scalar("SELECT key FROM worker_state ORDER BY key").fetch_all(pool).await?;
    let missing_counts = missing_post_counts(pool).await?;
    let trends = trend_hours(pool).await?;
    let keys_of_trends = if from == ScopeValue::KryphocronOnly { trend_keys(pool).await? } else { Value::Null };
    Ok(json!({
        "trend_keys": keys_of_trends,
        "post_counts_missing": missing_counts,
        "trend_hours": trends,
        "tables": tables,
        "database_bytes": db,
        "free_bytes": free.map(|f| f.0),
        "volume_bytes": free.map(|f| f.1),
        "dids_max": dids_max,
        "rkey_aliases_max": aliases_max,
        "collections": collections,
        "record_dicts": dicts,
        "worker_state_keys": keys,
        "at": rfc3339(&now()),
    }))
}

/// The database volume's free and total bytes.
#[must_use]
pub fn volume(data_dir: &std::path::Path) -> Option<(u64, u64)> {
    crate::admin::stats::space(std::path::Path::new(crate::admin::stats::POSTGRES_MOUNT))
        .or_else(|| crate::admin::stats::space(data_dir))
        .map(|s| (s.free, s.total))
}

/// Which bound has passed, if any.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn bound_passed(t: &Trial, at: DateTime<Utc>, db_bytes: Option<u64>, free: Option<(u64, u64)>) -> Option<&'static str> {
    if at >= t.ends_at {
        return Some("time");
    }
    if let Some(db) = db_bytes
        && db.saturating_sub(t.baseline_db_bytes) >= t.max_added_bytes
    {
        return Some("growth");
    }
    if let Some((free, total)) = free
        && total > 0
        && (free as f64) * 100.0 < f64::from(t.end_below_free_percent) * (total as f64)
    {
        return Some("free_space");
    }
    None
}

/// Plain words for bytes.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn bytes_words(b: u64) -> String {
    if b >= 1_000_000_000 {
        format!("{:.1} GB", b as f64 / 1e9)
    } else if b >= 1_000_000 {
        format!("{:.1} MB", b as f64 / 1e6)
    } else {
        format!("{:.0} kB", b as f64 / 1e3)
    }
}

/// Which bound ends a trial first at `per_day` bytes of growth a day: the
/// time limit, the growth limit or the free-space floor.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn first_bound(bounds: Bounds, free: Option<(u64, u64)>, per_day: u64) -> Option<&'static str> {
    if per_day == 0 {
        return None;
    }
    let days = |bytes: f64| bytes / per_day as f64;
    let mut ends = vec![("time", bounds.hours / 24.0), ("database growth", days(bounds.max_added_bytes() as f64))];
    if let Some((free, total)) = free {
        let floor = total as f64 * f64::from(bounds.end_below_free_percent) / 100.0;
        ends.push(("free-space", days((free as f64 - floor).max(0.0))));
    }
    ends.into_iter().min_by(|a, b| a.1.total_cmp(&b.1)).map(|(name, _)| name)
}

/// What the trial does not exercise and what its end does not restore,
/// for the confirm text.
#[must_use]
pub fn confirm_text(from: ScopeValue, bounds: Bounds, warnings: &[String], last_fill: Option<TimeDelta>, first_bound: Option<&str>) -> Vec<String> {
    let mut out = vec![format!(
        "A full trial from {}: full indexing for at most {} h, or until the database grows by {} GB, or the database volume's free space falls under {}%. Then Prism leaves full, removes what the trial added, checks it and says so.",
        from.as_str(),
        bounds.hours,
        bounds.max_added_gb,
        bounds.end_below_free_percent
    )];
    if from == ScopeValue::KryphocronOnly {
        out.push(match last_fill {
            Some(d) => format!(
                "From kryphocron_only, S's community fill runs first; the time limit includes it. The last fill took {} min.",
                d.num_minutes()
            ),
            None => "From kryphocron_only, S's community fill runs first; the time limit includes it.".to_string(),
        });
    }
    out.extend(warnings.iter().cloned());
    if let Some(b) = first_bound {
        out.push(format!("At the measured growth, the {b} bound ends the trial first."));
    }
    out.push(if bounds.history {
        "With history: the crawl fetches accounts outside S and the members as [full_crawl] says; S and the members are not crawled, and a crawl removes only rows the trial recorded. Not exercised: labelers' history (subscribed from now), and notifications or private-tier effects of the trial's rows (members get only the start mode's).".to_string()
    } else {
        "Not exercised: the history crawl (live only), labelers' history (subscribed from now), and notifications or private-tier effects of the trial's rows (members get only the start mode's).".to_string()
    });
    out.push("Removed at the end: every row the trial stored that the start mode would not have. Not restored: accounts first seen during the trial stay interned, caches refill on demand, and the database keeps the freed space for reuse until the reclaim step. The free-space floor leaves room for the exit itself, whose deletes free no disk until then.".to_string());
    out.push("Run again with --confirm FULL to start.".to_string());
    out
}

impl PublicIndexing {
    /// The trial in memory, if any.
    pub async fn trial(&self) -> Option<Trial> {
        self.scope.read_guard().await.trial.clone()
    }

    /// Whether a trial is running.
    pub async fn trial_running(&self) -> bool {
        self.trial().await.is_some_and(|t| t.state == TrialState::Running)
    }

    /// `full-trial start`: refuse a state that is not a settled start
    /// mode, take the baseline, then switch to full with the trial in the
    /// same transaction as the transition.
    ///
    /// # Errors
    ///
    /// [`Refused`] in plain words; a database error as its text.
    pub async fn start_trial(&self, bounds: Bounds, actor: &str) -> Result<Started, Refused> {
        let db = |e: sqlx::Error| Refused(e.to_string());
        let from = self.refusal().await.map_err(db)??;
        let measured_at = now();
        let id: i64 = sqlx::query_scalar("INSERT INTO full_trials (\"from\", \"to\", state, bounds) VALUES ($1, $1, 'measuring', $2) RETURNING id")
            .bind(from.as_str())
            .bind(bounds.to_json())
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        self.pause("trial_measuring").await;
        let free = self.volume_now();
        let base = baseline(&self.pool, free, from).await.map_err(db)?;
        let baseline_db = base["database_bytes"].as_u64().unwrap_or(0);
        let started_at = now();
        let trial = Trial {
            id,
            from,
            to: from,
            state: TrialState::Running,
            measured_at,
            started_at,
            ends_at: started_at + bounds.duration(),
            max_added_bytes: bounds.max_added_bytes(),
            end_below_free_percent: bounds.end_below_free_percent,
            history: bounds.history,
            baseline_db_bytes: baseline_db,
            start_last_fill_since: None,
            ended_by: None,
            counters: json!({}),
            check: json!({}),
        };
        let applied = self.apply_trial_start(trial.clone(), actor).await.map_err(db)?;
        let Some(applied) = applied else {
            sqlx::query("UPDATE full_trials SET state = 'abandoned', ended_at = now(), ended_by = 'switch', baseline = $2 WHERE id = $1")
                .bind(id)
                .bind(&base)
                .execute(&self.pool)
                .await
                .map_err(db)?;
            return Err(Refused("The mode changed while the baseline was taken; the trial was abandoned. Run the command again.".into()));
        };
        sqlx::query("UPDATE full_trials SET state = 'running', started_at = $2, baseline = $3 WHERE id = $1")
            .bind(id)
            .bind(started_at)
            .bind(&base)
            .execute(&self.pool)
            .await
            .map_err(db)?;
        if let Some(task) = applied.spawn {
            drop(self.spawn(task));
        }
        drop(self.spawn_trial_bounds());
        tracing::info!(event = "full_trial_started", id, from = from.as_str(), ends_at = %rfc3339(&trial.ends_at), "a full trial started");
        Ok(Started { id, from, ends_at: trial.ends_at, action: applied.action })
    }

    /// The start mode, or why a trial cannot start now.
    async fn refusal(&self) -> Result<Result<ScopeValue, Refused>, sqlx::Error> {
        let (public, trial) = {
            let sets = self.scope.read_guard().await;
            (sets.public, sets.trial.clone())
        };
        if let Some(t) = trial {
            let what = match t.state {
                TrialState::Running => "a trial is running; end it with `full-trial end` first",
                TrialState::Ending => "the last trial is still ending; wait for its check",
                TrialState::Checking => "the last trial's check is running; wait for its result",
                TrialState::Failed => "the last trial's check failed; run `full-trial check` again or `full-trial dismiss` first",
            };
            return Ok(Err(Refused(format!("No trial started: {what}."))));
        }
        let record: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM full_trial_rows) OR EXISTS (SELECT 1 FROM full_trial_jobs)").fetch_one(&self.pool).await?;
        if record {
            return Ok(Err(Refused("No trial started: an earlier trial's record has not been dropped; run `full-trial dismiss` first.".into())));
        }
        Ok(match (public.mode, public.breadth) {
            (_, Breadth::Full) => Err(Refused("No trial started: full indexing is already on.".into())),
            (_, Breadth::Narrowing) => Err(Refused("No trial started: Prism is leaving full (narrowing); wait until it is On, Community.".into())),
            (PublicMode::On, Breadth::Community) => Ok(ScopeValue::Community),
            (PublicMode::Off, Breadth::Community) => Ok(ScopeValue::KryphocronOnly),
            (PublicMode::Filling, _) => Err(Refused("No trial started: the community fill is running; wait until it is On.".into())),
            (PublicMode::Draining, _) => Err(Refused("No trial started: the drain is running; wait until it is Off.".into())),
            (PublicMode::Held, _) => Err(Refused("No trial started: the stored state is Held; switch with `public-scope` first.".into())),
        })
    }

    /// The database volume's free and total bytes; a test may set them.
    pub(super) fn volume_now(&self) -> Option<(u64, u64)> {
        #[cfg(test)]
        if let Some(v) = *self.hooks.volume.lock().expect("hooks") {
            return Some(v);
        }
        volume(&self.data_dir)
    }

    /// Test the three bounds, and end the trial with the first that passed
    /// (`path` names who looked: the checker, the sampler or startup).
    /// Returns the cause it ended with.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn check_trial_bounds(&self, path: &'static str) -> Result<Option<&'static str>, sqlx::Error> {
        let Some(t) = self.trial().await.filter(|t| t.state == TrialState::Running) else {
            return Ok(None);
        };
        let db = database_bytes(&self.pool).await?;
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_GROWTH).set(db.saturating_sub(t.baseline_db_bytes) as f64);
        let Some(cause) = bound_passed(&t, now(), Some(db), self.volume_now()) else {
            return Ok(None);
        };
        let cause = if path == "startup" { "startup" } else { cause };
        let ended = self.end_trial(cause).await?;
        if ended.is_some() {
            tracing::info!(event = "full_trial_bound", cause, path, "a full trial's bound passed; it ends");
        }
        Ok(ended.map(|_| cause))
    }

    /// End a running trial: its exit, under full's own transitions.
    /// A trial that is no longer running makes this a no-op.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn end_trial(&self, cause: &'static str) -> Result<Option<super::Applied>, sqlx::Error> {
        let Some(to) = self.trial().await.filter(|t| t.state == TrialState::Running).map(|t| t.to) else {
            return Ok(None);
        };
        let applied = self.apply_scope_as(to, "trial", super::public_mode::Intent::End { cause }).await?;
        if applied.action == "no_op" {
            return Ok(None);
        }
        if let Some(task) = applied.spawn.clone() {
            drop(self.spawn(task));
        }
        Ok(Some(applied))
    }

    /// How this trial's exit empties `full_labels`, from the rows its
    /// baseline counted. Not known (no trial, no baseline, a read error):
    /// every row is deleted, which empties it all the same.
    pub(super) async fn trial_labels_exit(&self) -> super::narrowing::LabelsExit {
        let Some(id) = self.trial().await.map(|t| t.id) else {
            return super::narrowing::LabelsExit::Delete;
        };
        match labels_at_start(&self.pool, id).await {
            Ok(rows) => super::narrowing::LabelsExit::of_start(rows),
            Err(error) => {
                tracing::warn!(event = "full_trial_labels_read_failed", %error, "reading the trial's baseline failed; its labels are deleted row by row");
                super::narrowing::LabelsExit::Delete
            }
        }
    }

    /// Write the counters into the stored trial, conditional on the
    /// trial's id; with `stamp`, the checker's stamp too.
    pub(super) async fn flush_trial_counters(&self, stamp: bool) -> Result<(), sqlx::Error> {
        let Some(t) = self.trial().await else {
            return Ok(());
        };
        let stamp_at = if stamp {
            Some(now())
        } else {
            t.counters.get("checked_at").and_then(parse_ts)
        };
        let counters = self.trial_counters().to_json(stamp_at);
        let guard = self.scope.read_guard().await;
        sqlx::query(
            "UPDATE worker_state SET value = jsonb_set(value, '{trial,counters}', $2), updated_at = now() \
             WHERE key = 'public_transition' AND (value->'trial'->>'id')::bigint = $1",
        )
        .bind(t.id)
        .bind(&counters)
        .execute(&self.pool)
        .await?;
        drop(guard);
        let mut sets = self.scope.write_guard().await;
        if let Some(mem) = sets.trial.as_mut().filter(|m| m.id == t.id) {
            mem.counters = counters;
        }
        Ok(())
    }

    /// The trial's counters (shared with the writer through the sets).
    #[must_use]
    pub fn trial_counters(&self) -> std::sync::Arc<TrialCounters> {
        self.scope.trial_counters()
    }

    /// The bounds checker, supervised: spawned with the trial and at every
    /// start while one runs; an inner task that ends while the trial still
    /// runs is spawned again.
    #[must_use]
    pub fn spawn_trial_bounds(&self) -> tokio::task::JoinHandle<()> {
        self.trace_spawn("trial_bounds");
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let inner = tokio::spawn(me.clone().trial_bounds_loop());
                let ended = inner.await;
                if !me.trial_running().await {
                    return;
                }
                tracing::warn!(event = "full_trial_checker_restarted", panicked = ended.is_err(), "the full trial's bounds checker stopped; it is started again");
                me.trace_spawn("trial_bounds_again");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    }

    async fn trial_bounds_loop(self) {
        loop {
            if !self.trial_running().await {
                return;
            }
            #[cfg(test)]
            if self.hooks.checker_panics.swap(false, Ordering::SeqCst) {
                panic!("injected checker panic");
            }
            #[cfg(test)]
            let stopped = self.hooks.checker_stopped.load(Ordering::SeqCst);
            #[cfg(not(test))]
            let stopped = false;
            if !stopped {
                match self.check_trial_bounds("checker").await {
                    Ok(Some(_)) => return,
                    Ok(None) => {}
                    Err(error) => tracing::warn!(event = "full_trial_check_failed", %error, "testing the trial's bounds failed"),
                }
                if let Err(error) = self.flush_trial_counters(true).await {
                    tracing::warn!(event = "full_trial_check_failed", %error, "writing the trial's counters failed");
                }
                self.trial_gauges().await;
            }
            tokio::time::sleep(self.settings.trial_check_every).await;
        }
    }

    /// `full_trial_state{state}`.
    pub(super) async fn trial_gauges(&self) {
        let state = self.trial().await.map(|t| t.state);
        for s in TrialState::ALL {
            metrics::gauge!(METRIC_STATE, "state" => s.as_str()).set(if Some(s) == state { 1.0 } else { 0.0 });
        }
    }

    /// The trial's startup work, before any writer: a measuring row
    /// left by a restart is abandoned; a running trial past a bound ends at
    /// once with cause `startup`; then the bounds checker or the check is
    /// spawned. Returns the exit's task when one ended it.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(super) async fn trial_startup(&self) -> Result<Option<super::Spawn>, sqlx::Error> {
        sqlx::query("UPDATE full_trials SET state = 'abandoned', ended_at = now(), ended_by = 'restart' WHERE state = 'measuring'")
            .execute(&self.pool)
            .await?;
        let Some(t) = self.trial().await else {
            return Ok(None);
        };
        self.trial_counters().load(&t.counters);
        self.trial_gauges().await;
        match t.state {
            TrialState::Running => {
                let db = database_bytes(&self.pool).await?;
                if bound_passed(&t, now(), Some(db), self.volume_now()).is_some() {
                    let applied = self.apply_scope_as(t.to, "trial", super::public_mode::Intent::End { cause: "startup" }).await?;
                    tracing::info!(event = "full_trial_bound", cause = "startup", "a full trial's bound passed while Prism was down; it ends");
                    return Ok(applied.spawn);
                }
                drop(self.spawn_trial_bounds());
            }
            TrialState::Checking => drop(self.spawn_trial_check()),
            TrialState::Ending | TrialState::Failed => {}
        }
        Ok(None)
    }

    /// `full-trial keep`: full for real. The trial leaves the
    /// transition, the record is dropped and the result is `kept`.
    ///
    /// # Errors
    ///
    /// [`Refused`] when no trial runs; database errors as text.
    pub async fn keep_trial(&self, actor: &str) -> Result<Value, Refused> {
        let applied = self.apply_scope_as(ScopeValue::Full, actor, super::public_mode::Intent::Command).await.map_err(|e| Refused(e.to_string()))?;
        if applied.action != "kept" {
            return Err(Refused("No trial is running.".into()));
        }
        Ok(json!({ "action": "kept", "mode": applied.mode.as_str() }))
    }

    /// `full-trial dismiss`: a failed trial's record is dropped and its
    /// result kept as `dismissed`; also from Held, or with a record left by
    /// no trial.
    ///
    /// # Errors
    ///
    /// [`Refused`] while a trial runs, ends or is checked.
    pub async fn dismiss_trial(&self) -> Result<Value, Refused> {
        let db = |e: sqlx::Error| Refused(e.to_string());
        let mut sets = self.scope.write_guard().await;
        if let Some(t) = &sets.trial
            && t.state != TrialState::Failed
        {
            return Err(Refused(format!("The trial is {}; only a failed trial's record is dismissed.", t.state.as_str())));
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        drop_record(&mut tx).await.map_err(db)?;
        sqlx::query("UPDATE worker_state SET value = value - 'trial', updated_at = now() WHERE key = 'public_transition'")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let n = sqlx::query("UPDATE full_trials SET state = 'dismissed', ended_at = COALESCE(ended_at, now()) WHERE state IN ('failed', 'running', 'ending', 'checking')")
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        sqlx::query("UPDATE full_trials SET result = result || '{\"warning_dismissed\": true}'::jsonb WHERE state = 'passed' AND result->>'warning' = 'true'")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        self.scope.set_trial(&mut sets, None);
        drop(sets);
        self.trial_gauges().await;
        Ok(json!({ "action": "dismissed", "trials": n }))
    }

    /// `full-trial bounds`: change a running trial's bounds.
    ///
    /// # Errors
    ///
    /// [`Refused`] when no trial runs.
    pub async fn trial_bounds(&self, hours: Option<f64>, max_added_gb: Option<f64>, free_percent: Option<u32>) -> Result<Value, Refused> {
        let db = |e: sqlx::Error| Refused(e.to_string());
        if free_percent.is_some_and(|p| p > 100) {
            return Err(Refused("--end-below-free-percent is a share of 0 to 100.".into()));
        }
        let mut sets = self.scope.write_guard().await;
        let Some(mut t) = sets.trial.clone().filter(|t| t.state == TrialState::Running) else {
            return Err(Refused("No trial is running.".into()));
        };
        if let Some(h) = hours {
            t.ends_at = t.started_at + hours_delta(h);
        }
        if let Some(gb) = max_added_gb {
            t.max_added_bytes = gb_bytes(gb);
        }
        if let Some(p) = free_percent {
            t.end_below_free_percent = p;
        }
        let n = sqlx::query(
            "UPDATE worker_state SET value = jsonb_set(jsonb_set(jsonb_set(value, '{trial,ends_at}', $2), '{trial,max_added_bytes}', $3), \
               '{trial,end_below_free_percent}', $4), updated_at = now() \
             WHERE key = 'public_transition' AND (value->'trial'->>'id')::bigint = $1 AND value->'trial'->>'state' = 'running'",
        )
        .bind(t.id)
        .bind(json!(rfc3339(&t.ends_at)))
        .bind(json!(t.max_added_bytes))
        .bind(json!(t.end_below_free_percent))
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        if n == 1 {
            sqlx::query("UPDATE full_trials SET bounds = bounds || $2 WHERE id = $1")
                .bind(t.id)
                .bind(json!({ "ends_at": rfc3339(&t.ends_at), "max_added_bytes": t.max_added_bytes, "end_below_free_percent": t.end_below_free_percent }))
                .execute(&self.pool)
                .await
                .map_err(db)?;
            self.scope.set_trial(&mut sets, Some(t.clone()));
        }
        Ok(json!({ "ends_at": rfc3339(&t.ends_at), "max_added_bytes": t.max_added_bytes, "end_below_free_percent": t.end_below_free_percent }))
    }

    /// The trial block of `admin indexing status` and Health: the state,
    /// its figures, any warning, and the last result in plain words.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn trial_status(&self) -> Result<Value, sqlx::Error> {
        let t = self.trial().await;
        let last: Option<(i64, String, Option<Value>, Option<String>)> =
            sqlx::query_as("SELECT id, state, result, ended_by FROM full_trials ORDER BY id DESC LIMIT 1").fetch_optional(&self.pool).await?;
        let mut warnings = Vec::new();
        let mut block = serde_json::Map::new();
        if let Some(t) = &t {
            block.insert("id".into(), json!(t.id));
            block.insert("state".into(), json!(t.state.as_str()));
            block.insert("from".into(), json!(t.from.as_str()));
            block.insert("to".into(), json!(t.to.as_str()));
            block.insert("since".into(), json!(rfc3339(&t.started_at)));
            block.insert("ends_at".into(), json!(rfc3339(&t.ends_at)));
            block.insert("live_only".into(), json!(!t.history));
            block.insert("ended_by".into(), json!(t.ended_by));
            if t.state == TrialState::Running {
                let db = database_bytes(&self.pool).await?;
                let record: i64 = sqlx::query_scalar("SELECT pg_total_relation_size('full_trial_rows')").fetch_one(&self.pool).await?;
                block.insert(
                    "database_growth".into(),
                    json!(format!("{} of {}", bytes_words(db.saturating_sub(t.baseline_db_bytes)), bytes_words(t.max_added_bytes))),
                );
                block.insert("record_bytes".into(), json!(record));
                block.insert("recorded".into(), self.trial_counters().to_json(None)["recorded"].clone());
                block.insert("free".into(), json!(self.volume_now().map(|v| bytes_words(v.0))));
                let checked = t.counters.get("checked_at").and_then(parse_ts);
                if checked.is_none_or(|c| now() - c > STALE_AFTER) && now() - t.started_at > STALE_AFTER {
                    warnings.push("The full trial's bounds checker has not run for over 5 minutes; the storage sampler still ends the trial.".to_string());
                }
            }
            if t.state == TrialState::Checking {
                block.insert("check".into(), t.check.clone());
            }
            if t.state == TrialState::Failed {
                for c in t.check.get("causes").and_then(Value::as_array).into_iter().flatten() {
                    warnings.push(format!("Full trial check failed: {}", c.as_str().unwrap_or_default()));
                }
            }
        }
        // Faults the check removed stay on Health until dismissed.
        if let Some((_, state, Some(result), _)) = &last
            && state == "passed"
            && result.get("warning").and_then(Value::as_bool) == Some(true)
            && result.get("warning_dismissed").and_then(Value::as_bool) != Some(true)
        {
            warnings.push(format!(
                "The last full trial's exit left {} rows the check had to remove (a fault in the exit or its keep rules); `full-trial dismiss` clears this warning.",
                result.get("faults").and_then(Value::as_u64).unwrap_or(0)
            ));
        }
        let last = last.map(|(id, state, result, ended_by)| {
            json!({ "id": id, "state": state, "ended_by": ended_by, "words": result.as_ref().and_then(|r| r.get("words")).cloned() })
        });
        Ok(json!({ "trial": if t.is_some() { Value::Object(block) } else { Value::Null }, "last": last, "warnings": warnings }))
    }
}
