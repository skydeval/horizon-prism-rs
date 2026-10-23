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
    Phase, PublicIndexing, PublicMode, PublicState, ScopeValue, Spawn, SweepState, Transition, g64, now, parse_ts, replace_if, rfc3339,
};
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
const AUTHOR_NOT_MEMBER: &str = "NOT EXISTS (SELECT 1 FROM members m WHERE m.did = t.author_did)";
// Refract's spares (refract design §5.4): an author in R (database state)
// keeps the rows the restored rule admitted.
const NOT_R_AUTHOR: &str = "NOT EXISTS (SELECT 1 FROM refract_dids r WHERE r.did = t.author_did AND r.state IN ('restored', 'grace'))";
const AUTHOR_NOT_MEMBER_NOR_R: &str = "NOT EXISTS (SELECT 1 FROM members m WHERE m.did = t.author_did) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r WHERE r.did = t.author_did AND r.state IN ('restored', 'grace'))";
const CREATOR_NOT_MEMBER_NOR_R: &str = "NOT EXISTS (SELECT 1 FROM members m WHERE m.did = t.creator_did) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r WHERE r.did = t.creator_did AND r.state IN ('restored', 'grace'))";
const NO_MEMBER_END_NOR_R: &str = "NOT EXISTS (SELECT 1 FROM members m WHERE m.did = t.author_did OR m.did = t.subject_did) \
     AND NOT EXISTS (SELECT 1 FROM refract_dids r WHERE r.did = t.author_did AND r.state IN ('restored', 'grace'))";
const POST_NOT_R: &str =
    "NOT EXISTS (SELECT 1 FROM refract_dids r WHERE r.did = split_part(t.post_uri, '/', 3) AND r.state IN ('restored', 'grace'))";

const fn table(name: &'static str, key: Key, delete_when: &'static str, start: u64) -> SweepTable {
    SweepTable { name, key, delete_when, start }
}

const URI: Key = Key::Text("uri");

/// §7.4's order, with §7.2's predicates and starting sizes. Every name and
/// column is checked against the migrations (0001, 0004, 0009, 0010, 0014,
/// 0016). `member_post_refs` is not swept.
pub const TABLES: [SweepTable; 18] = [
    // 1.
    table("feed_items", URI, ALL, 10_000),
    table("notifications", Key::Id, ALL, 10_000),
    // 2. Non-members' likes and reposts, while `posts` still exists for
    // their trend triggers.
    table("likes", URI, AUTHOR_NOT_MEMBER_NOR_R, 1_000),
    table("reposts", URI, AUTHOR_NOT_MEMBER_NOR_R, 1_000),
    // 3.
    table("posts", URI, NOT_R_AUTHOR, 1_000),
    // 4.
    table("threadgates", URI, NOT_R_AUTHOR, 10_000),
    table("postgates", URI, NOT_R_AUTHOR, 10_000),
    table("starter_packs", URI, ALL, 10_000),
    table("labeler_services", URI, ALL, 10_000),
    table("nearhorizon_poll_posts", URI, ALL, 10_000),
    table("lists", URI, CREATOR_NOT_MEMBER_NOR_R, 10_000),
    table("list_items", URI, AUTHOR_NOT_MEMBER_NOR_R, 10_000),
    table("list_optouts", URI, AUTHOR_NOT_MEMBER_NOR_R, 10_000),
    table("list_blocks", URI, AUTHOR_NOT_MEMBER, 10_000),
    // 5.
    table("follows", URI, NO_MEMBER_END_NOR_R, 10_000),
    // 6. Also removes the negative trend rows steps 2–3 created. The
    // counts of R's posts stay, as `load_posts` falls back to them.
    table("post_counts", Key::Text("post_uri"), POST_NOT_R, 10_000),
    table("tag_activity_hourly", Key::TagHour, ALL, 10_000),
    table("starter_pack_counts", Key::Text("pack_uri"), ALL, 10_000),
];

/// A window's largest key: the stored cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cursor {
    Text(String),
    Id(i64),
    TagHour(String, DateTime<Utc>),
}

impl Cursor {
    /// The §3.2 encoding.
    #[must_use]
    pub fn to_json(&self) -> Value {
        match self {
            Self::Text(s) => json!(s),
            Self::Id(id) => json!(id),
            Self::TagHour(tag, hour) => json!([tag, rfc3339(hour)]),
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
            _ => return None,
        };
        Some(Some(cursor))
    }
}

/// §3.5 step 2.2: a stored sweep state that validates.
#[must_use]
pub fn valid_state(v: &Value) -> Option<SweepState> {
    let name = v.get("table")?.as_str()?;
    let table = TABLES.iter().find(|t| t.name == name)?;
    let after = v.get("after").cloned().unwrap_or(Value::Null);
    Cursor::from_json(table.key, &after)?;
    let last_advanced_at = parse_ts(v.get("last_advanced_at")?)?;
    Some(SweepState { table: name.to_string(), after, last_advanced_at })
}

/// A window of keys, in key order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Window {
    Text(Vec<String>),
    Id(Vec<i64>),
    TagHour(Vec<String>, Vec<DateTime<Utc>>),
}

impl Window {
    fn last(&self) -> Option<Cursor> {
        match self {
            Self::Text(keys) => keys.last().cloned().map(Cursor::Text),
            Self::Id(keys) => keys.last().copied().map(Cursor::Id),
            Self::TagHour(tags, hours) => tags.last().cloned().zip(hours.last().copied()).map(|(t, h)| Cursor::TagHour(t, h)),
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
    pub(super) async fn sweep_batch(&self, generation: u64, table: usize, after: Option<&Cursor>, n: u64) -> Result<Batch, sqlx::Error> {
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
        // 3. The window, with no predicate on the rows.
        let window = select_window(&mut tx, t, after, n).await?;
        let Some(last) = window.last() else {
            let Some(next) = TABLES.get(table + 1) else {
                return Ok(Batch::SweepDone);
            };
            let sweep = SweepState { table: next.name.to_string(), after: Value::Null, last_advanced_at: now() };
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
        let mut table = TABLES.iter().position(|t| t.name == state.table).unwrap_or(0);
        let mut after = Cursor::from_json(TABLES[table].key, &state.after).flatten();
        let mut last_advanced = state.last_advanced_at;
        let mut n = self.start_window(table);
        let mut backoff = self.settings.retry_base;
        self.progress(|p| p.sweep_batch_rows = n);
        loop {
            let name = TABLES[table].name;
            let started = Instant::now();
            match self.sweep_batch(generation, table, after.as_ref(), n).await {
                Ok(Batch::Exit) => return,
                Ok(Batch::Superseded) => return Self::superseded("sweep", generation),
                Ok(Batch::SweepDone) => {
                    // A failed end is retried; the end's own checks make a
                    // retry safe.
                    loop {
                        match self.end_sweep(generation).await {
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
    pub(super) async fn end_sweep(&self, generation: u64) -> Result<(), sqlx::Error> {
        let mut sets = self.scope.write_guard().await;
        if sets.public.generation != generation {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let pending: Option<Value> = sqlx::query_scalar(
            "SELECT coalesce(value->'pending', 'null'::jsonb) FROM worker_state \
             WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2 FOR UPDATE",
        )
        .bind(Phase::Draining.as_str())
        .bind(g64(generation))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(pending) = pending else {
            Self::superseded("sweep", generation);
            return Ok(());
        };
        let next = generation + 1;
        let at = now();
        let filling = pending == json!(ScopeValue::CommunityScoped.as_str());
        let t = if filling {
            Transition::filling(next, at)
        } else {
            Transition::settled(Phase::Off, next, at, sets.public.last_fill_since)
        };
        if !replace_if(&mut tx, Phase::Draining, generation, &t).await? {
            Self::superseded("sweep", generation);
            return Ok(());
        }
        if filling {
            sqlx::query("UPDATE operator_settings SET value = $2, updated_at = now() WHERE key = $1")
                .bind(super::public_mode::SETTING_KEY)
                .bind(ScopeValue::CommunityScoped.to_json())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        let mode = if filling { PublicMode::Filling } else { PublicMode::Off };
        self.set_state(&mut sets, PublicState { mode, generation: next, last_fill_since: t.last_fill_since });
        drop(sets);
        tracing::info!(event = "public_sweep_done", mode = mode.as_str(), generation = next, "the sweep ended");
        if let Some(fill) = t.fill {
            drop(self.spawn(Spawn::Fill { generation: next, fill }));
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
        ] {
            assert_eq!(Cursor::from_json(key, &cursor.to_json()), Some(Some(cursor)));
            assert_eq!(Cursor::from_json(key, &Value::Null), Some(None));
        }
        assert_eq!(Cursor::from_json(URI, &json!(7)), None);
        assert_eq!(Cursor::from_json(Key::Id, &json!("7")), None);
        assert_eq!(Cursor::from_json(Key::TagHour, &json!(["rust"])), None);
        assert_eq!(Cursor::from_json(Key::TagHour, &json!(["rust", "yesterday"])), None);
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
