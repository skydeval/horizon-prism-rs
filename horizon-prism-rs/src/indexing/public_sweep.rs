//! The sweep (public-indexing design §7.2–§7.5): table by table, in §7.4's
//! order, it deletes the public data the off rule no longer stores.
//!
//! Each batch is one transaction under the scope read guard: a window of up
//! to `n` keys after the stored cursor, chosen with no predicate on the rows;
//! a delete of the window's rows that the table's predicate drops; and the
//! cursor write, conditional on the phase and the task's generation. The
//! cursor so advances by up to `n` keys whatever the batch deletes, and a
//! table ends only on an empty window.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgConnection;

use super::public_mode::{
    Breadth, Phase, PublicIndexing, PublicMode, ScopeValue, SweepList, SweepState, Transition, g64, now, parse_ts, replace_if, rfc3339,
    spawn_of, state_of,
};
use super::trial::TrialState;
use crate::db::retention::BATCH_PAUSE;

/// `public_sweep_deleted_rows_total{table}`.
pub const METRIC_DELETED: &str = "public_sweep_deleted_rows_total";
/// `public_sweep_retries_total{table, reason}`.
pub const METRIC_RETRIES: &str = "public_sweep_retries_total";
/// `public_sweep_batch_seconds{table}`.
pub const METRIC_BATCH_SECONDS: &str = "public_sweep_batch_seconds";
/// `public_sweep_batch_rows{table}`.
pub const METRIC_BATCH_ROWS: &str = "public_sweep_batch_rows";
/// `public_sweep_stalled`: 1 while the sweep has not advanced for
/// `stall_after`.
pub const METRIC_STALLED: &str = "public_sweep_stalled";

/// The smallest window a statement timeout halves to (§7.2).
pub const WINDOW_FLOOR: u64 = 10;
/// The cap on the retry backoff (§7.2).
pub const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// How a table's rows are keyed, and so how its cursor is encoded (§3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    /// A text key (`uri`, `post_uri`, `pack_uri`): a JSON string.
    Text(&'static str),
    /// `notifications.id` (bigserial): a JSON integer.
    Id,
    /// `tag_activity_hourly (tag, hour)`: `[tag, RFC 3339 hour in UTC]`.
    TagHour,
    /// A record table keyed by ids, `(author, rkey)`: `[author, rkey]`, two
    /// integers. A text cursor stored before the table moved to ids does not
    /// decode, so the sweep starts that table again.
    AuthorRkey,
    /// `feed_items (originator, is_repost, src_rkey)`: `[originator,
    /// is_repost, src_rkey]`. A text cursor starts the table again, as for
    /// [`Key::AuthorRkey`].
    FeedItem,
    /// A record table keyed by its author alone (`labeler_services`):
    /// `[author]`. A text cursor starts the table again, as for
    /// [`Key::AuthorRkey`].
    Author,
}

/// One swept table (§7.2's table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepTable {
    pub name: &'static str,
    pub key: Key,
    /// The rows of the window that are deleted, over alias `t`.
    pub delete_when: &'static str,
    /// The starting window `n`.
    pub start: u64,
}

const ALL: &str = "TRUE";
// Every swept table names its author by id.
const AUTHOR_NOT_MEMBER: &str = "NOT EXISTS (SELECT 1 FROM members m JOIN dids d ON d.did = m.did WHERE d.id = t.author)";
// Refract's spares (refract design §5.4): an author in R (database state)
// keeps the rows the restored rule admitted.
const NOT_R_AUTHOR: &str =
    "NOT EXISTS (SELECT 1 FROM refract_dids r JOIN dids d ON d.did = r.did WHERE d.id = t.author AND r.state IN ('restored', 'grace'))";
const AUTHOR_NOT_MEMBER_NOR_R: &str = "NOT EXISTS (SELECT 1 FROM members m JOIN dids d ON d.did = m.did WHERE d.id = t.author) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r JOIN dids d ON d.did = r.did WHERE d.id = t.author AND r.state IN ('restored', 'grace'))";
// Follows name their accounts by id.
const NO_MEMBER_END_NOR_R: &str = "NOT EXISTS (SELECT 1 FROM members m JOIN dids d ON d.did = m.did WHERE d.id IN (t.author, t.subject)) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r JOIN dids d ON d.did = r.did WHERE d.id = t.author AND r.state IN ('restored', 'grace'))";
// Likes and reposts name their author by id.
const LIKE_NOT_MEMBER_NOR_R: &str = "NOT EXISTS (SELECT 1 FROM members m JOIN dids d ON d.did = m.did WHERE d.id = t.author) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r JOIN dids d ON d.did = r.did WHERE d.id = t.author AND r.state IN ('restored', 'grace'))";
// refract-reposts design §4: a repost whose subject's author is in R is
// kept by the restored-subject rule, whoever wrote it.
const REPOST_NOT_MEMBER_NOR_R: &str = "NOT EXISTS (SELECT 1 FROM members m JOIN dids d ON d.did = m.did WHERE d.id = t.author) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r JOIN dids d ON d.did = r.did WHERE d.id = t.author AND r.state IN ('restored', 'grace')) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r JOIN dids d ON d.did = r.did WHERE d.id = t.subj_author AND r.state IN ('restored', 'grace'))";
// Posts and their counts are keyed by the post's author id.
const POST_NOT_R: &str =
    "NOT EXISTS (SELECT 1 FROM refract_dids r JOIN dids d ON d.did = r.did WHERE d.id = t.author AND r.state IN ('restored', 'grace'))";

const fn table(name: &'static str, key: Key, delete_when: &'static str, start: u64) -> SweepTable {
    SweepTable { name, key, delete_when, start }
}

#[cfg(test)]
const URI: Key = Key::Text("uri");

/// §7.4's order, with §7.2's predicates and starting sizes. Every name and
/// column is checked against the migrations (0001, 0004, 0009, 0010, 0014,
/// 0016). `member_post_refs` is not swept.
pub const TABLES: [SweepTable; 18] = [
    // 1.
    table("feed_items", Key::FeedItem, ALL, 10_000),
    table("notifications", Key::Id, ALL, 10_000),
    // 2. Non-members' likes and reposts, while `posts` still exists for
    // their trend triggers.
    table("likes", Key::AuthorRkey, LIKE_NOT_MEMBER_NOR_R, 1_000),
    table("reposts", Key::AuthorRkey, REPOST_NOT_MEMBER_NOR_R, 1_000),
    // 3.
    // A post's search row and odd references go with it.
    table("posts", Key::AuthorRkey, POST_NOT_R, 1_000),
    // 4.
    table("threadgates", Key::AuthorRkey, NOT_R_AUTHOR, 10_000),
    table("postgates", Key::AuthorRkey, NOT_R_AUTHOR, 10_000),
    table("starter_packs", Key::AuthorRkey, ALL, 10_000),
    table("labeler_services", Key::Author, ALL, 10_000),
    table("nearhorizon_poll_posts", Key::AuthorRkey, ALL, 10_000),
    table("lists", Key::AuthorRkey, AUTHOR_NOT_MEMBER_NOR_R, 10_000),
    table("list_items", Key::AuthorRkey, AUTHOR_NOT_MEMBER_NOR_R, 10_000),
    table("list_optouts", Key::AuthorRkey, AUTHOR_NOT_MEMBER_NOR_R, 10_000),
    table("list_blocks", Key::AuthorRkey, AUTHOR_NOT_MEMBER, 10_000),
    // 5.
    table("follows", Key::AuthorRkey, NO_MEMBER_END_NOR_R, 10_000),
    // 6. Also removes the negative trend rows steps 2–3 created. The
    // counts of R's posts stay, as `load_posts` falls back to them.
    table("post_counts", Key::AuthorRkey, POST_NOT_R, 10_000),
    table("tag_activity_hourly", Key::TagHour, ALL, 10_000),
    table("starter_pack_counts", Key::AuthorRkey, ALL, 10_000),
];

/// A window's largest key: the stored cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cursor {
    Text(String),
    Id(i64),
    TagHour(String, DateTime<Utc>),
    AuthorRkey(i32, i64),
    FeedItem(i32, bool, i64),
    Author(i32),
}

impl Cursor {
    /// The §3.2 encoding.
    #[must_use]
    pub fn to_json(&self) -> Value {
        match self {
            Self::Text(s) => json!(s),
            Self::Id(id) => json!(id),
            Self::TagHour(tag, hour) => json!([tag, rfc3339(hour)]),
            Self::AuthorRkey(author, rkey) => json!([author, rkey]),
            Self::FeedItem(originator, is_repost, rkey) => json!([originator, is_repost, rkey]),
            Self::Author(author) => json!([author]),
        }
    }

    /// Decode a stored `sweep.after` for a table keyed by `key`: `Some(None)`
    /// for null, `None` when it does not have the key's encoding.
    #[must_use]
    pub fn from_json(key: Key, v: &Value) -> Option<Option<Self>> {
        if v.is_null() {
            return Some(None);
        }
        let cursor = match (key, v) {
            (Key::Text(_), Value::String(s)) => Self::Text(s.clone()),
            (Key::Id, Value::Number(n)) => Self::Id(n.as_i64()?),
            (Key::TagHour, Value::Array(a)) if a.len() == 2 => Self::TagHour(a[0].as_str()?.to_string(), parse_ts(&a[1])?),
            (Key::AuthorRkey, Value::Array(a)) if a.len() == 2 => {
                Self::AuthorRkey(i32::try_from(a[0].as_i64()?).ok()?, a[1].as_i64()?)
            }
            (Key::FeedItem, Value::Array(a)) if a.len() == 3 => {
                Self::FeedItem(i32::try_from(a[0].as_i64()?).ok()?, a[1].as_bool()?, a[2].as_i64()?)
            }
            (Key::Author, Value::Array(a)) if a.len() == 1 => Self::Author(i32::try_from(a[0].as_i64()?).ok()?),
            _ => return None,
        };
        Some(Some(cursor))
    }
}

/// §3.5 step 2.2: a stored sweep state that validates, checked against
/// its own list (a state stored without one is `today`).
#[must_use]
pub fn valid_state(v: &Value) -> Option<SweepState> {
    let list = match v.get("list") {
        None | Some(Value::Null) => SweepList::Today,
        Some(l) => SweepList::parse(l.as_str()?)?,
    };
    let name = v.get("table")?.as_str()?;
    // A narrowing step, or one of the steps a full drain runs after the
    // tables: no cursor.
    let step = match list {
        SweepList::Today => false,
        SweepList::Full => super::narrowing::FULL_DRAIN_STEP_NAMES.contains(&name),
        SweepList::TrialFull => super::narrowing::TRIAL_FULL_DRAIN_STEP_NAMES.contains(&name),
        SweepList::Narrowing | SweepList::TrialNarrowing => {
            super::narrowing::step_index(list, name)?;
            true
        }
    };
    if step {
        let last_advanced_at = parse_ts(v.get("last_advanced_at")?)?;
        return Some(SweepState { table: name.to_string(), after: Value::Null, last_advanced_at, list });
    }
    let table = TABLES.iter().find(|t| t.name == name)?;
    let mut after = v.get("after").cloned().unwrap_or(Value::Null);
    if Cursor::from_json(table.key, &after).is_none() {
        // A text cursor of a table that moved to ids: that table starts
        // again from its first key, which only deletes again what the
        // predicate drops. Any other mismatch is not a valid state.
        if !matches!(table.key, Key::AuthorRkey | Key::FeedItem | Key::Author) {
            return None;
        }
        after = Value::Null;
    }
    let last_advanced_at = parse_ts(v.get("last_advanced_at")?)?;
    Some(SweepState { table: name.to_string(), after, last_advanced_at, list })
}

/// A window of keys, in key order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Window {
    Text(Vec<String>),
    Id(Vec<i64>),
    TagHour(Vec<String>, Vec<DateTime<Utc>>),
    AuthorRkey(Vec<i32>, Vec<i64>),
    FeedItem(Vec<i32>, Vec<bool>, Vec<i64>),
    Author(Vec<i32>),
}

/// Whether a sweep of `list` passes over `table` without deleting: a
/// trial's drain leaves the count rows and the trend hours to the triggers
/// of the rows it deletes, so what the start mode held before the trial
/// stays, and then removes the rows those deletes left at zero. The
/// ordinary drain's predicates there would take every count row and trend
/// hour the off rule does not write itself, though it keeps the members'
/// likes and reposts they come from.
#[must_use]
pub fn left_to_triggers(list: SweepList, table: &str) -> bool {
    list == SweepList::TrialFull && matches!(table, "post_counts" | "tag_activity_hourly")
}

impl Window {
    fn empty(key: Key) -> Self {
        match key {
            Key::Text(_) => Self::Text(Vec::new()),
            Key::Id => Self::Id(Vec::new()),
            Key::TagHour => Self::TagHour(Vec::new(), Vec::new()),
            Key::AuthorRkey => Self::AuthorRkey(Vec::new(), Vec::new()),
            Key::FeedItem => Self::FeedItem(Vec::new(), Vec::new(), Vec::new()),
            Key::Author => Self::Author(Vec::new()),
        }
    }

    fn last(&self) -> Option<Cursor> {
        match self {
            Self::Text(keys) => keys.last().cloned().map(Cursor::Text),
            Self::Id(keys) => keys.last().copied().map(Cursor::Id),
            Self::TagHour(tags, hours) => tags.last().cloned().zip(hours.last().copied()).map(|(t, h)| Cursor::TagHour(t, h)),
            Self::AuthorRkey(authors, rkeys) => authors.last().copied().zip(rkeys.last().copied()).map(|(a, k)| Cursor::AuthorRkey(a, k)),
            Self::FeedItem(originators, reposts, rkeys) => match (originators.last(), reposts.last(), rkeys.last()) {
                (Some(o), Some(r), Some(k)) => Some(Cursor::FeedItem(*o, *r, *k)),
                _ => None,
            },
            Self::Author(authors) => authors.last().copied().map(Cursor::Author),
        }
    }
}

/// What one batch did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Batch {
    /// The in-memory generation is not the task's.
    Exit,
    /// The conditional write missed; everything rolled back.
    Superseded,
    /// The window was deleted and the cursor moved to `last`.
    Advanced { deleted: u64, last: Cursor },
    /// The window was empty; the cursor moved to the next table.
    TableDone,
    /// The last table's window was empty; nothing was written.
    SweepDone,
}

/// The four SQLSTATEs retried with backoff (§7.2), with the metric's reason.
pub(super) fn retry_reason(e: &sqlx::Error) -> Option<&'static str> {
    let sqlx::Error::Database(db) = e else {
        return None;
    };
    match db.code().as_deref() {
        Some("40P01") => Some("deadlock"),
        Some("55P03") => Some("lock_timeout"),
        Some("40001") => Some("serialization"),
        Some("57014") => Some("statement_timeout"),
        _ => None,
    }
}

impl PublicIndexing {
    fn start_window(&self, table: usize) -> u64 {
        self.settings.sweep_window.unwrap_or(TABLES[table].start)
    }

    /// One batch of §7.2, steps 1–6 (the pause excluded).
    #[cfg(test)]
    pub(super) async fn sweep_batch(&self, generation: u64, table: usize, after: Option<&Cursor>, n: u64) -> Result<Batch, sqlx::Error> {
        self.sweep_batch_in(SweepList::Today, generation, table, after, n).await
    }

    /// [`Self::sweep_batch`] for a sweep of `list`: a full drain moves on
    /// from the last table to its steps.
    pub(super) async fn sweep_batch_in(
        &self,
        list: SweepList,
        generation: u64,
        table: usize,
        after: Option<&Cursor>,
        n: u64,
    ) -> Result<Batch, sqlx::Error> {
        let t = &TABLES[table];
        // 1. The guard before the connection.
        let guard = self.scope.read_guard().await;
        if guard.public.generation != generation {
            return Ok(Batch::Exit);
        }
        #[cfg(test)]
        self.hooks.windows.lock().expect("hooks").push((t.name, n));
        let mut tx = self.pool.begin().await?;
        // 2.
        sqlx::query("SET LOCAL lock_timeout = '1s'").execute(&mut *tx).await?;
        sqlx::query("SET LOCAL statement_timeout = '3s'").execute(&mut *tx).await?;
        #[cfg(test)]
        {
            let code = self.hooks.inject.lock().expect("hooks").pop_front();
            if let Some(code) = code {
                sqlx::query(&format!("DO $$ BEGIN RAISE EXCEPTION 'injected' USING ERRCODE = '{code}'; END $$"))
                    .execute(&mut *tx)
                    .await?;
            }
        }
        // 3. The window, with no predicate on the rows. A trial's drain
        // leaves the counts and the trend hours to their triggers.
        let window = if left_to_triggers(list, t.name) { Window::empty(t.key) } else { select_window(&mut tx, t, after, n).await? };
        let Some(last) = window.last() else {
            let next = match (TABLES.get(table + 1), list) {
                (Some(next), _) => next.name,
                (None, SweepList::Full) => super::narrowing::FULL_DRAIN_STEP_NAMES[0],
                (None, SweepList::TrialFull) => super::narrowing::TRIAL_FULL_DRAIN_STEP_NAMES[0],
                (None, _) => return Ok(Batch::SweepDone),
            };
            let sweep = SweepState { table: next.to_string(), after: Value::Null, last_advanced_at: now(), list };
            let n = sqlx::query(
                "UPDATE worker_state SET value = jsonb_set(value, '{sweep}', $3), updated_at = now() \
                 WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2",
            )
            .bind(Phase::Draining.as_str())
            .bind(g64(generation))
            .bind(serde_json::to_value(&sweep).unwrap_or(Value::Null))
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if n == 0 {
                return Ok(Batch::Superseded);
            }
            tx.commit().await?;
            drop(guard);
            return Ok(Batch::TableDone);
        };
        self.pause("sweep_window").await;
        // 4. The predicate delete.
        let deleted = delete_window(&mut tx, t, &window).await?;
        self.pause("sweep_cursor").await;
        // 5. The cursor.
        let n = sqlx::query(
            "UPDATE worker_state SET value = jsonb_set(jsonb_set(value, '{sweep,after}', $3), '{sweep,last_advanced_at}', $4), \
               updated_at = now() \
             WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2",
        )
        .bind(Phase::Draining.as_str())
        .bind(g64(generation))
        .bind(last.to_json())
        .bind(json!(rfc3339(&now())))
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Ok(Batch::Superseded);
        }
        // 6.
        tx.commit().await?;
        drop(guard);
        Ok(Batch::Advanced { deleted, last })
    }

    /// The sweep task: batches until the last table ends, then §7.5.
    pub(super) async fn run_sweep(self, generation: u64, state: SweepState) {
        let list = state.list;
        if let Some(step) = super::narrowing::step_index(list, &state.table) {
            return self.run_steps(generation, list, step).await;
        }
        let mut table = TABLES.iter().position(|t| t.name == state.table).unwrap_or(0);
        let mut after = Cursor::from_json(TABLES[table].key, &state.after).flatten();
        let mut last_advanced = state.last_advanced_at;
        let mut n = self.start_window(table);
        let mut backoff = self.settings.retry_base;
        self.progress(|p| p.sweep_batch_rows = n);
        loop {
            let name = TABLES[table].name;
            let started = Instant::now();
            match self.sweep_batch_in(list, generation, table, after.as_ref(), n).await {
                Ok(Batch::Exit) => return,
                Ok(Batch::Superseded) => return Self::superseded("sweep", generation),
                Ok(Batch::SweepDone) => {
                    // A failed end is retried; the end's own checks make a
                    // retry safe.
                    loop {
                        match self.end_sweep_of(list, generation).await {
                            Ok(()) => return,
                            Err(error) => {
                                tracing::error!(event = "public_sweep_failed", table = name, %error, "ending the sweep failed; retrying");
                                self.progress(|p| p.sweep_last_error = Some(error.to_string()));
                                tokio::time::sleep(self.settings.failure_pause).await;
                            }
                        }
                    }
                }
                Ok(batch) => {
                    match batch {
                        Batch::Advanced { deleted, last } => {
                            metrics::histogram!(METRIC_BATCH_SECONDS, "table" => name).record(started.elapsed().as_secs_f64());
                            metrics::histogram!(METRIC_BATCH_ROWS, "table" => name).record(deleted as f64);
                            metrics::counter!(METRIC_DELETED, "table" => name).increment(deleted);
                            after = Some(last);
                            // A success doubles `n` back toward its start.
                            n = n.saturating_mul(2).min(self.start_window(table));
                            self.progress(|p| p.sweep_deleted += deleted);
                        }
                        _ => {
                            tracing::info!(event = "public_sweep_table_done", table = name, "swept table");
                            table += 1;
                            if table == TABLES.len() {
                                // A full drain goes on with its steps.
                                return self.run_steps(generation, list, 0).await;
                            }
                            after = None;
                            n = self.start_window(table);
                        }
                    }
                    last_advanced = now();
                    backoff = self.settings.retry_base;
                    metrics::gauge!(METRIC_STALLED).set(0.0);
                    self.progress(|p| {
                        p.sweep_batch_rows = n;
                        p.sweep_last_error = None;
                        p.sweep_stalled = false;
                    });
                    tokio::time::sleep(BATCH_PAUSE).await;
                }
                Err(error) => {
                    if let Some(reason) = retry_reason(&error) {
                        metrics::counter!(METRIC_RETRIES, "table" => name, "reason" => reason).increment(1);
                        if reason == "statement_timeout" {
                            n = (n / 2).max(WINDOW_FLOOR.min(self.start_window(table)));
                        }
                        tracing::warn!(event = "public_sweep_retry", table = name, reason, n, %error, "sweep batch rolled back; retrying");
                        self.progress(|p| {
                            p.sweep_batch_rows = n;
                            p.sweep_last_error = Some(error.to_string());
                        });
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                    } else {
                        tracing::error!(event = "public_sweep_failed", table = name, %error, "sweep batch failed; retrying");
                        self.progress(|p| p.sweep_last_error = Some(error.to_string()));
                        tokio::time::sleep(self.settings.failure_pause).await;
                    }
                    // The stall alarm (§7.2); the next advance clears it.
                    let stalled = (now() - last_advanced).to_std().is_ok_and(|d| d > self.settings.stall_after);
                    if stalled {
                        metrics::gauge!(METRIC_STALLED).set(1.0);
                        self.progress(|p| p.sweep_stalled = true);
                    }
                }
            }
        }
    }

    /// §7.5: under the write guard, conditional on the phase and the
    /// generation, Off, or Filling when `pending` is set. The fill pass is
    /// spawned after the commit.
    #[cfg(test)]
    pub(super) async fn end_sweep(&self, generation: u64) -> Result<(), sqlx::Error> {
        self.end_sweep_of(SweepList::Today, generation).await
    }

    /// The end of a sweep of `list`. A drain ends Off, or Filling when
    /// `pending` is set (Filling, Full for `full`); a narrowing ends in
    /// Filling, Community, whose fill re-syncs S.
    pub(super) async fn end_sweep_of(&self, list: SweepList, generation: u64) -> Result<(), sqlx::Error> {
        let mut sets = self.scope.write_guard().await;
        if sets.public.generation != generation {
            return Ok(());
        }
        let phase = list.phase();
        let mut tx = self.pool.begin().await?;
        let pending: Option<Value> = sqlx::query_scalar(
            "SELECT coalesce(value->'pending', 'null'::jsonb) FROM worker_state \
             WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2 FOR UPDATE",
        )
        .bind(phase.as_str())
        .bind(g64(generation))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(pending) = pending else {
            Self::superseded("sweep", generation);
            return Ok(());
        };
        let next = generation + 1;
        let at = now();
        let cg = sets.public.crawl_generation;
        // A trial's exit: its narrowing from On, Full ends without a refill,
        // and every exit that does not refill puts `last_fill_since` back.
        let mut trial = sets.trial.clone();
        let ending = trial.as_ref().filter(|t| t.state == TrialState::Ending).map(|t| t.start_last_fill_since);
        let mut kept = false;
        let (mut t, value) = if phase == Phase::Narrowing {
            match ending {
                Some(since) if sets.public.mode == PublicMode::On => {
                    (Transition::settled(Phase::On, next, at, since).in_breadth(Breadth::Community, cg), None)
                }
                _ => (Transition::filling(next, at).in_breadth(Breadth::Community, cg), None),
            }
        } else if pending == json!(ScopeValue::Community.as_str()) {
            if let Some(t) = trial.as_mut().filter(|t| t.state == TrialState::Ending) {
                t.to = ScopeValue::Community;
            }
            (Transition::filling(next, at).in_breadth(Breadth::Community, cg), Some(ScopeValue::Community))
        } else if pending == json!(ScopeValue::Full.as_str()) {
            // Full for real after a trial's drain: its record goes now.
            if ending.is_some() {
                kept = true;
                trial = None;
            }
            (Transition::filling(next, at).in_breadth(Breadth::Full, cg), Some(ScopeValue::Full))
        } else {
            let since = ending.unwrap_or(sets.public.last_fill_since);
            (Transition::settled(Phase::Off, next, at, since).in_breadth(Breadth::Community, cg), None)
        };
        // The exit's settled phase: the check runs.
        let mut check = false;
        if let Some(tr) = trial.as_mut()
            && tr.state == TrialState::Ending
            && matches!(t.phase, Phase::On | Phase::Off)
        {
            tr.state = TrialState::Checking;
            tr.check = json!({});
            check = true;
        }
        t.trial = self.with_counters(trial);
        if !replace_if(&mut tx, phase, generation, &t).await? {
            Self::superseded("sweep", generation);
            return Ok(());
        }
        if let Some(value) = value {
            sqlx::query("UPDATE operator_settings SET value = $2, updated_at = now() WHERE key = $1")
                .bind(super::public_mode::SETTING_KEY)
                .bind(value.to_json())
                .execute(&mut *tx)
                .await?;
        }
        let excluded = if t.breadth == Breadth::Full {
            super::full::reset_groups(&mut tx, cg, &self.full_kinds).await?;
            Some(super::full::excluded_dids(&mut tx).await?)
        } else {
            None
        };
        if let Some(old) = sets.trial.as_ref().filter(|_| kept) {
            super::trial::drop_record(&mut tx).await?;
            sqlx::query("UPDATE full_trials SET state = 'kept', ended_by = 'kept' WHERE id = $1").bind(old.id).execute(&mut *tx).await?;
        }
        if let Some(tr) = t.trial.as_ref().filter(|_| check) {
            sqlx::query("UPDATE full_trials SET state = 'checking' WHERE id = $1").bind(tr.id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        if let Some(dids) = excluded {
            sets.full.excluded.replace(dids.into_iter().collect());
        }
        let state = state_of(&t);
        self.set_state(&mut sets, state);
        self.scope.set_trial(&mut sets, t.trial.clone());
        drop(sets);
        if check {
            drop(self.spawn_trial_check());
        }
        tracing::info!(event = "public_sweep_done", list = list.as_str(), mode = state.mode.as_str(), breadth = state.breadth.as_str(), generation = next, "the sweep ended");
        if let Some(task) = spawn_of(&t) {
            drop(self.spawn(task));
        }
        Ok(())
    }
}

async fn select_window(conn: &mut PgConnection, t: &SweepTable, after: Option<&Cursor>, n: u64) -> Result<Window, sqlx::Error> {
    let limit = i64::try_from(n).unwrap_or(i64::MAX);
    Ok(match t.key {
        Key::Text(col) => {
            let after = match after {
                Some(Cursor::Text(s)) => Some(s.as_str()),
                _ => None,
            };
            let keys: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT {col} FROM {name} WHERE ($1::text IS NULL OR {col} > $1) ORDER BY {col} LIMIT $2",
                name = t.name
            ))
            .bind(after)
            .bind(limit)
            .fetch_all(conn)
            .await?;
            Window::Text(keys)
        }
        Key::Id => {
            let after = match after {
                Some(Cursor::Id(id)) => Some(*id),
                _ => None,
            };
            let keys: Vec<i64> = sqlx::query_scalar(&format!(
                "SELECT id FROM {name} WHERE ($1::bigint IS NULL OR id > $1) ORDER BY id LIMIT $2",
                name = t.name
            ))
            .bind(after)
            .bind(limit)
            .fetch_all(conn)
            .await?;
            Window::Id(keys)
        }
        Key::TagHour => {
            let (tag, hour) = match after {
                Some(Cursor::TagHour(tag, hour)) => (Some(tag.as_str()), Some(*hour)),
                _ => (None, None),
            };
            let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(&format!(
                "SELECT tag, hour FROM {name} \
                 WHERE ($1::text IS NULL OR (tag, hour) > ($1, $2::timestamptz)) \
                 ORDER BY tag, hour LIMIT $3",
                name = t.name
            ))
            .bind(tag)
            .bind(hour)
            .bind(limit)
            .fetch_all(conn)
            .await?;
            let (tags, hours) = rows.into_iter().unzip();
            Window::TagHour(tags, hours)
        }
        Key::AuthorRkey => {
            let (author, rkey) = match after {
                Some(Cursor::AuthorRkey(author, rkey)) => (Some(*author), Some(*rkey)),
                _ => (None, None),
            };
            let rows: Vec<(i32, i64)> = sqlx::query_as(&format!(
                "SELECT author, rkey FROM {name} \
                 WHERE ($1::integer IS NULL OR (author, rkey) > ($1, $2::bigint)) \
                 ORDER BY author, rkey LIMIT $3",
                name = t.name
            ))
            .bind(author)
            .bind(rkey)
            .bind(limit)
            .fetch_all(conn)
            .await?;
            let (authors, rkeys) = rows.into_iter().unzip();
            Window::AuthorRkey(authors, rkeys)
        }
        Key::FeedItem => {
            let (originator, is_repost, rkey) = match after {
                Some(Cursor::FeedItem(o, r, k)) => (Some(*o), Some(*r), Some(*k)),
                _ => (None, None, None),
            };
            let rows: Vec<(i32, bool, i64)> = sqlx::query_as(&format!(
                "SELECT originator, is_repost, src_rkey FROM {name} \
                 WHERE ($1::integer IS NULL OR (originator, is_repost, src_rkey) > ($1, $2::boolean, $3::bigint)) \
                 ORDER BY originator, is_repost, src_rkey LIMIT $4",
                name = t.name
            ))
            .bind(originator)
            .bind(is_repost)
            .bind(rkey)
            .bind(limit)
            .fetch_all(conn)
            .await?;
            let mut window = (Vec::with_capacity(rows.len()), Vec::with_capacity(rows.len()), Vec::with_capacity(rows.len()));
            for (o, r, k) in rows {
                window.0.push(o);
                window.1.push(r);
                window.2.push(k);
            }
            Window::FeedItem(window.0, window.1, window.2)
        }
        Key::Author => {
            let after = match after {
                Some(Cursor::Author(a)) => Some(*a),
                _ => None,
            };
            let keys: Vec<i32> = sqlx::query_scalar(&format!(
                "SELECT author FROM {name} WHERE ($1::integer IS NULL OR author > $1) ORDER BY author LIMIT $2",
                name = t.name
            ))
            .bind(after)
            .bind(limit)
            .fetch_all(conn)
            .await?;
            Window::Author(keys)
        }
    })
}

async fn delete_window(conn: &mut PgConnection, t: &SweepTable, window: &Window) -> Result<u64, sqlx::Error> {
    let (name, pred) = (t.name, t.delete_when);
    let result = match (t.key, window) {
        (Key::Text(col), Window::Text(keys)) => {
            sqlx::query(&format!("DELETE FROM {name} t WHERE t.{col} = ANY($1) AND {pred}"))
                .bind(keys)
                .execute(conn)
                .await?
        }
        (Key::Id, Window::Id(keys)) => {
            sqlx::query(&format!("DELETE FROM {name} t WHERE t.id = ANY($1) AND {pred}"))
                .bind(keys)
                .execute(conn)
                .await?
        }
        (Key::TagHour, Window::TagHour(tags, hours)) => {
            sqlx::query(&format!(
                "DELETE FROM {name} t WHERE (t.tag, t.hour) IN (SELECT * FROM unnest($1::text[], $2::timestamptz[])) AND {pred}"
            ))
            .bind(tags)
            .bind(hours)
            .execute(conn)
            .await?
        }
        (Key::AuthorRkey, Window::AuthorRkey(authors, rkeys)) => {
            sqlx::query(&format!(
                "DELETE FROM {name} t WHERE (t.author, t.rkey) IN (SELECT * FROM unnest($1::integer[], $2::bigint[])) AND {pred}"
            ))
            .bind(authors)
            .bind(rkeys)
            .execute(conn)
            .await?
        }
        (Key::FeedItem, Window::FeedItem(originators, reposts, rkeys)) => {
            sqlx::query(&format!(
                "DELETE FROM {name} t WHERE (t.originator, t.is_repost, t.src_rkey) IN \
                   (SELECT * FROM unnest($1::integer[], $2::boolean[], $3::bigint[])) AND {pred}"
            ))
            .bind(originators)
            .bind(reposts)
            .bind(rkeys)
            .execute(conn)
            .await?
        }
        (Key::Author, Window::Author(authors)) => {
            sqlx::query(&format!("DELETE FROM {name} t WHERE t.author = ANY($1) AND {pred}"))
                .bind(authors)
                .execute(conn)
                .await?
        }
        _ => unreachable!("a window has its table's key"),
    };
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §3.2: each key's encoding round-trips, and the wrong one is refused.
    #[test]
    fn cursors_round_trip_by_key() {
        let hour = parse_ts(&json!("2026-10-14T05:00:00Z")).expect("hour");
        for (key, cursor) in [
            (URI, Cursor::Text("at://did:plc:a/app.bsky.feed.like/1".into())),
            (Key::Id, Cursor::Id(42)),
            (Key::TagHour, Cursor::TagHour("rust".into(), hour)),
            (Key::AuthorRkey, Cursor::AuthorRkey(7, -3)),
            (Key::FeedItem, Cursor::FeedItem(7, true, -3)),
            (Key::Author, Cursor::Author(7)),
        ] {
            assert_eq!(Cursor::from_json(key, &cursor.to_json()), Some(Some(cursor)));
            assert_eq!(Cursor::from_json(key, &Value::Null), Some(None));
        }
        assert_eq!(Cursor::from_json(URI, &json!(7)), None);
        assert_eq!(Cursor::from_json(Key::Id, &json!("7")), None);
        assert_eq!(Cursor::from_json(Key::TagHour, &json!(["rust"])), None);
        assert_eq!(Cursor::from_json(Key::TagHour, &json!(["rust", "yesterday"])), None);
        // A follows cursor stored as a URI before the table moved to ids
        // does not decode, so the sweep starts that table again.
        assert_eq!(Cursor::from_json(Key::AuthorRkey, &json!("at://did:plc:a/app.bsky.graph.follow/1")), None);
        let follows = TABLES.iter().find(|t| t.name == "follows").expect("follows");
        assert_eq!(follows.key, Key::AuthorRkey);
        let state = json!({"table": "follows", "after": "at://did:plc:a/app.bsky.graph.follow/1", "last_advanced_at": "2026-10-14T05:00:00Z"});
        let restarted = valid_state(&state).expect("still a valid state");
        assert_eq!((restarted.table.as_str(), &restarted.after), ("follows", &Value::Null));
        // So does every other table keyed by ids.
        for name in [
            "likes", "reposts", "post_counts", "posts", "feed_items", "threadgates", "postgates", "starter_packs", "labeler_services",
            "nearhorizon_poll_posts", "lists", "list_items", "list_optouts", "list_blocks", "starter_pack_counts",
        ] {
            let state = json!({"table": name, "after": "at://did:plc:a/app.bsky.feed.like/1", "last_advanced_at": "2026-10-14T05:00:00Z"});
            assert_eq!(valid_state(&state).expect(name).after, Value::Null, "{name} starts again");
        }
        // Any other table's mismatched cursor is still refused.
        let tags = json!({"table": "tag_activity_hourly", "after": 7, "last_advanced_at": "2026-10-14T05:00:00Z"});
        assert!(valid_state(&tags).is_none());
    }

    /// Only a trial's drain passes over the counts and the trend hours, and
    /// its steps then remove the rows left at zero.
    #[test]
    fn a_trial_drain_leaves_counts_and_trends_to_their_triggers() {
        for list in [SweepList::Today, SweepList::Full, SweepList::TrialFull] {
            for t in TABLES {
                let left = left_to_triggers(list, t.name);
                assert_eq!(left, list == SweepList::TrialFull && matches!(t.name, "post_counts" | "tag_activity_hourly"), "{list:?} {}", t.name);
            }
        }
        for step in [super::super::narrowing::TRIAL_ZERO_COUNTS, super::super::narrowing::TRIAL_ZERO_TRENDS, super::super::narrowing::TRIAL_ADDED_TRENDS] {
            assert!(super::super::narrowing::TRIAL_FULL_DRAIN_STEP_NAMES.contains(&step), "{step}");
            assert!(!super::super::narrowing::FULL_DRAIN_STEP_NAMES.contains(&step), "{step}");
        }
    }

    #[test]
    fn the_order_starts_with_feed_items_and_ends_with_the_counts() {
        let names: Vec<&str> = TABLES.iter().map(|t| t.name).collect();
        assert_eq!(names.first(), Some(&"feed_items"));
        assert_eq!(&names[15..], ["post_counts", "tag_activity_hourly", "starter_pack_counts"]);
        assert!(!names.contains(&"member_post_refs"));
        let small: Vec<&str> = TABLES.iter().filter(|t| t.start == 1_000).map(|t| t.name).collect();
        assert_eq!(small, ["likes", "reposts", "posts"]);
    }
}
