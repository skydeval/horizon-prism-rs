//! The post-migration backfill task (design-v3 §12.2): a background task of
//! `serve`, started once every migration is applied and ingest is running.
//! It is resumable, idempotent and paced, and it changes no semantics.
//!
//! **Phase 1: column backfill.**
//! - `actors`: sets the profile columns from `profile_record`, in keyset
//!   batches by account id. A batch updates only rows whose `profile_rev` is the
//!   one it read, so a row the ingest writer rewrote meanwhile is never
//!   overwritten with values derived from a stale read. `profile_cid` is not
//!   derived here: it comes with the record on the next profile write or
//!   re-sync.
//! - Progress is kept in `worker_state` (`backfill:actors_v3`), so a
//!   restart resumes; a cursor stored before rows were keyed by id starts
//!   the table again. (The search columns of posts were filled here once;
//!   they now live in `post_search`, which migration 0057 filled.)
//!
//! **Phase 2: large-table indexes**, after phase 1, one at a time, each with
//! `CREATE INDEX CONCURRENTLY` on a dedicated connection that has no lock or
//! statement timeout. An invalid index left by an interrupted build is
//! waited for while a backend is still building it, and dropped otherwise.
//! A failed build is retried after a backoff that doubles up to an hour.
//!
//! **Phase 3: records compressed**, one record family at a time, the
//! families side by side: posts, profiles, and the other kept records
//! (lists, starter packs, gates, polls, labeler services, self records).
//! Once the instance
//! holds [`TRAIN_RECORDS`] records of a family, a dictionary is trained from
//! the earliest of them and stored in `record_dicts`; writes use it from
//! then on. Then the rows still stored as dict 0 (the JSON text) are
//! rewritten with it, table by table in keyset batches by key, each update
//! guarded by the rev and the dict byte it read, so a newer write, a delete
//! or a purge in between makes it a no-op and nothing deleted comes back.
//! Progress is kept in `worker_state` (`records:recompress_<table>`).
//!
//! Until the task finishes, search runs without its indexes, and trends
//! wait for the tag index.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use serde_json::{Value, json};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgConnection, PgPool};
use tokio::task::JoinHandle;

use crate::config::FileConfig;
use crate::firehose::records::ProfileRecord;

pub const KEY_ACTORS: &str = "backfill:actors_v3";
/// `schema_backfill_remaining{table}`: an estimate of rows not yet visited.
pub const METRIC_REMAINING: &str = "schema_backfill_remaining";
/// `schema_index_pending`: indexes not yet built.
pub const METRIC_INDEX_PENDING: &str = "schema_index_pending";
pub const METRIC_INDEX_FAILURES: &str = "schema_index_build_failures_total";
/// How long to wait for another backend's build before looking again.
const LIVE_BUILD_POLL: Duration = Duration::from_secs(30);
/// The longest wait between attempts at one index.
const MAX_BACKOFF: Duration = Duration::from_secs(3600);
/// Waits between retries of a batch that lost a lock race.
const LOCK_RETRY: Duration = Duration::from_millis(500);

/// Post records a dictionary is trained from: the earliest stored. Training
/// waits until the instance holds this many.
pub const TRAIN_RECORDS: i64 = 10_000;
/// A trained dictionary's size.
pub const DICT_BYTES: usize = 64 * 1024;
/// How long to wait before looking again for enough records to train from.
const TRAIN_RETRY: Duration = Duration::from_secs(3600);
pub const KEY_RECOMPRESS: &str = "records:recompress_posts";
/// `record_recompress_remaining`: an estimate of post rows not yet visited.
pub const METRIC_RECOMPRESS_REMAINING: &str = "record_recompress_remaining";
/// `other_record_recompress_remaining`: an estimate of the other kept
/// records' rows not yet visited.
pub const METRIC_OTHER_RECOMPRESS_REMAINING: &str = "other_record_recompress_remaining";
/// `profile_record_recompress_remaining`: an estimate of the profile rows
/// not yet visited.
pub const METRIC_PROFILE_RECOMPRESS_REMAINING: &str = "profile_record_recompress_remaining";

/// A table whose rows keep their record, as the recompression visits it:
/// keyed by its `author` column and `key` (a column, or `None` for a table
/// keyed by its author alone), its record in `record` at rev `rev`, its
/// rows ordered in time by `at` for training.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordTable {
    pub table: &'static str,
    pub author: &'static str,
    pub key: Option<&'static str>,
    pub rev: &'static str,
    pub record: &'static str,
    pub at: &'static str,
}

impl RecordTable {
    const fn rkey(table: &'static str) -> Self {
        Self::keyed(table, Some("rkey"))
    }

    const fn keyed(table: &'static str, key: Option<&'static str>) -> Self {
        Self { table, author: "author", key, rev: "rev", record: "record", at: "created_at" }
    }

    /// SQL: the row's second key over alias `t`, as a bigint.
    fn key_sql(self) -> String {
        self.key.map_or_else(|| "0::bigint".to_string(), |k| format!("t.{k}::bigint"))
    }

    /// Its progress key in `worker_state`.
    fn progress_key(self) -> String {
        format!("records:recompress_{}", self.table)
    }
}

/// A record family's tables, with what the family trains from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordFamily {
    pub family: &'static str,
    pub tables: &'static [RecordTable],
    /// `record_recompress_remaining`-style gauge of the family.
    metric: &'static str,
}

/// The posts.
pub const POSTS: RecordFamily = RecordFamily {
    family: super::records::FAMILY_POSTS,
    tables: &[RecordTable::rkey("posts")],
    metric: METRIC_RECOMPRESS_REMAINING,
};

/// The other kept records.
pub const OTHER: RecordFamily = RecordFamily {
    family: super::records::FAMILY_OTHER,
    tables: &[
        RecordTable::rkey("lists"),
        RecordTable::rkey("starter_packs"),
        RecordTable::rkey("threadgates"),
        RecordTable::rkey("postgates"),
        RecordTable::rkey("nearhorizon_poll_posts"),
        RecordTable::keyed("labeler_services", None),
        // Self records keep no creation time; their first-seen time stands
        // in.
        RecordTable { at: "indexed_at", ..RecordTable::keyed("actor_self_records", Some("coll")) },
    ],
    metric: METRIC_OTHER_RECOMPRESS_REMAINING,
};

/// The profiles, kept on their account's `actors` row, which has none until
/// a profile is stored. The row's first-seen time orders them.
pub const PROFILES: RecordFamily = RecordFamily {
    family: super::records::FAMILY_PROFILES,
    tables: &[RecordTable { table: "actors", author: "id", key: None, rev: "profile_rev", record: "profile_record", at: "indexed_at" }],
    metric: METRIC_PROFILE_RECOMPRESS_REMAINING,
};
/// `posts_dead_rows`: dead tuples in `posts`, which the rewrites leave until
/// a vacuum (or a repack) takes them.
pub const METRIC_POSTS_DEAD_ROWS: &str = "posts_dead_rows";

/// The large-table indexes, by name, with the one statement that builds
/// each: the search indexes of `post_search`.
pub const INDEXES: [(&str, &str); 7] = [
    ("post_search_tsv", "CREATE INDEX CONCURRENTLY IF NOT EXISTS post_search_tsv ON post_search USING gin (search_tsv)"),
    ("post_search_tags", "CREATE INDEX CONCURRENTLY IF NOT EXISTS post_search_tags ON post_search USING gin (tags)"),
    ("post_search_domains", "CREATE INDEX CONCURRENTLY IF NOT EXISTS post_search_domains ON post_search USING gin (link_domains)"),
    ("post_search_urls", "CREATE INDEX CONCURRENTLY IF NOT EXISTS post_search_urls ON post_search USING gin (link_urls)"),
    ("post_search_mentions", "CREATE INDEX CONCURRENTLY IF NOT EXISTS post_search_mentions ON post_search USING gin (mention_ids)"),
    ("post_search_langs", "CREATE INDEX CONCURRENTLY IF NOT EXISTS post_search_langs ON post_search USING gin (lang_prefixes)"),
    (
        "post_search_created",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS post_search_created ON post_search (created_at DESC, author DESC, rkey DESC)",
    ),
];

/// The task's settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// `schema_backfill.batch_size`.
    pub batch_size: u32,
    /// `schema_backfill.pause_ms`: the pause between batches.
    pub pause_ms: u64,
    /// `schema_index.retry_backoff_secs`: the first wait after a failed
    /// index build.
    pub index_retry_backoff_secs: u64,
}

impl Settings {
    #[must_use]
    pub fn from_config(cfg: &FileConfig) -> Self {
        Self {
            batch_size: cfg.schema_backfill.batch_size,
            pause_ms: cfg.schema_backfill.pause_ms,
            index_retry_backoff_secs: cfg.schema_index.retry_backoff_secs,
        }
    }
}

/// What `_health` reports about the task. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct SchemaStatus {
    backfill_done: Arc<AtomicBool>,
    indexes_ready: Arc<AtomicBool>,
}

impl SchemaStatus {
    /// Phase 1 has visited every row.
    #[must_use]
    pub fn backfill_done(&self) -> bool {
        self.backfill_done.load(Ordering::Relaxed)
    }

    /// Every index of phase 2 is built and valid.
    #[must_use]
    pub fn indexes_ready(&self) -> bool {
        self.indexes_ready.load(Ordering::Relaxed)
    }
}

/// A table's progress, as stored in `worker_state`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Progress {
    /// The last key visited.
    cursor: String,
    done: bool,
    /// Rows visited so far, for the remaining estimate.
    visited: i64,
}

async fn load_progress(pool: &PgPool, key: &str) -> Result<Progress, sqlx::Error> {
    let value: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    Ok(value.map_or_else(Progress::default, |v| Progress {
        cursor: v.get("cursor").and_then(Value::as_str).unwrap_or_default().to_string(),
        done: v.get("done").and_then(Value::as_bool).unwrap_or(false),
        visited: v.get("visited").and_then(Value::as_i64).unwrap_or(0),
    }))
}

async fn save_progress(pool: &PgPool, key: &str, p: &Progress) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(key)
    .bind(json!({"cursor": p.cursor, "done": p.done, "visited": p.visited}))
    .execute(pool)
    .await?;
    Ok(())
}

/// SQLSTATE 40P01 (deadlock) or 55P03 (lock not available): the batch took
/// row locks outside the writer's lock order, so it is simply run again.
fn lost_a_lock_race(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().is_some_and(|c| c == "40P01" || c == "55P03"))
}

/// One `actors` batch after `after` (an account id; anything else starts
/// from the first), over rows that have a profile rev.
async fn actors_batch(pool: &PgPool, after: &str, limit: i64) -> Result<Option<(String, i64)>, sqlx::Error> {
    let after: i32 = after.parse().unwrap_or(i32::MIN);
    let rows: Vec<(i32, i64, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT id, profile_rev, profile_record FROM actors WHERE id > $1 AND profile_rev IS NOT NULL ORDER BY id LIMIT $2",
    )
    .bind(after)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let Some(last) = rows.last().map(|r| r.0.to_string()) else {
        return Ok(None);
    };
    let now = Utc::now();
    let mut values: Vec<Value> = Vec::with_capacity(rows.len());
    for (id, rev, stored) in &rows {
        let Some(stored) = stored else { continue };
        let record = super::records::decode_value(pool, stored).await?;
        let p = ProfileRecord::parse(&record, now);
        values.push(json!({
            "id": id, "rev": rev, "pronouns": p.pronouns, "website": p.website,
            "via_uri": p.joined_via_starter_pack.as_ref().map(|s| s.uri.clone()),
            "via_cid": p.joined_via_starter_pack.as_ref().and_then(|s| s.cid.clone()),
            "created": p.created_at.map(|t| t.to_rfc3339()),
        }));
    }
    // `profile_created_at` is clamped to the row's first-seen time plus five
    // minutes, as a post's is. The CID is kept in its text form.
    sqlx::query(
        "UPDATE actors a SET pronouns = v.pronouns, website = v.website, \
           joined_via_starter_pack_uri = v.via_uri, joined_via_starter_pack_cid = '\\x00'::bytea || convert_to(v.via_cid, 'UTF8'), \
           profile_created_at = LEAST(v.created, a.indexed_at + interval '5 minutes') \
         FROM jsonb_to_recordset($1::jsonb) AS v(id integer, rev bigint, pronouns text, website text, \
           via_uri text, via_cid text, created timestamptz) \
         WHERE a.id = v.id AND a.profile_rev = v.rev",
    )
    .bind(Value::Array(values))
    .execute(pool)
    .await?;
    Ok(Some((last, i64::try_from(rows.len()).unwrap_or(i64::MAX))))
}

/// Train the posts dictionary, when none is stored yet and the instance
/// holds [`TRAIN_RECORDS`] posts: its id, or `None`.
///
/// # Errors
///
/// Database errors, and a stored record that does not decode.
pub async fn train_posts_dict(pool: &PgPool) -> Result<Option<i16>, sqlx::Error> {
    train_dict(pool, POSTS).await
}

/// SQL: the records a family trains from, its earliest stored first.
fn samples_sql(family: RecordFamily) -> String {
    let arms: Vec<String> = family
        .tables
        .iter()
        .map(|t| format!("SELECT {} AS record, {} AS at FROM {} WHERE {0} IS NOT NULL", t.record, t.at, t.table))
        .collect();
    format!("SELECT record FROM ({}) r ORDER BY at LIMIT $1", arms.join(" UNION ALL "))
}

/// Train a family's dictionary, when none is stored yet and the instance
/// holds [`TRAIN_RECORDS`] of its records: its id, or `None`.
///
/// # Errors
///
/// Database errors, and a stored record that does not decode.
pub async fn train_dict(pool: &PgPool, family: RecordFamily) -> Result<Option<i16>, sqlx::Error> {
    let have: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM record_dicts WHERE family = $1)")
        .bind(family.family)
        .fetch_one(pool)
        .await?;
    if have {
        return Ok(None);
    }
    let stored: Vec<Vec<u8>> = sqlx::query_scalar(&samples_sql(family)).bind(TRAIN_RECORDS).fetch_all(pool).await?;
    if i64::try_from(stored.len()).unwrap_or(0) < TRAIN_RECORDS {
        return Ok(None);
    }
    let mut samples = Vec::with_capacity(stored.len());
    for record in &stored {
        samples.push(super::records::decode(pool, record).await?.into_bytes());
    }
    let dict = match super::ids::train_dict(&samples, DICT_BYTES) {
        Ok(dict) => dict,
        Err(error) => {
            tracing::warn!(event = "record_dict_training_failed", family = family.family, %error, "a record dictionary could not be trained");
            return Ok(None);
        }
    };
    let id: i16 = sqlx::query_scalar(
        "INSERT INTO record_dicts (id, family, bytes) SELECT COALESCE(max(id), 0) + 1, $1, $2 FROM record_dicts RETURNING id",
    )
    .bind(family.family)
    .bind(&dict)
    .fetch_one(pool)
    .await?;
    super::records::reload(pool).await?;
    tracing::info!(event = "record_dict_trained", id, family = family.family, bytes = dict.len(), "a record dictionary was trained");
    Ok(Some(id))
}

/// The rows of one recompression batch to rewrite: each key, the rev it was
/// read at, and its record with the current dictionary.
#[derive(Debug, Default)]
struct Rewrites {
    authors: Vec<i32>,
    rkeys: Vec<i64>,
    revs: Vec<i64>,
    records: Vec<Vec<u8>>,
}

/// A row as the recompression reads it: its key, its rev and its record
/// (both `None` on an account with no profile).
type StoredRecordRow = (i32, i64, Option<i64>, Option<Vec<u8>>);

/// Read one recompression batch of `table` after `after` (`author:key`):
/// the last key read, the rows read, and the rows stored as dict 0 with
/// their records encoded again in `family`; `None` at the end of the table.
async fn recompress_read(pool: &PgPool, family: &str, table: RecordTable, after: &str, limit: i64) -> Result<Option<(String, i64, Rewrites)>, sqlx::Error> {
    let (author, rkey) = after
        .split_once(':')
        .and_then(|(a, k)| Some((a.parse::<i32>().ok()?, k.parse::<i64>().ok()?)))
        .unwrap_or((i32::MIN, i64::MIN));
    let key = table.key_sql();
    let (author_col, rev_col, record_col) = (table.author, table.rev, table.record);
    let rows: Vec<StoredRecordRow> = sqlx::query_as(&format!(
        "SELECT t.{author_col}, {key}, t.{rev_col}, t.{record_col} FROM {} t WHERE (t.{author_col}, {key}) > ($1, $2) \
         ORDER BY t.{author_col}, {key} LIMIT $3",
        table.table
    ))
    .bind(author)
    .bind(rkey)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let Some(&(last_author, last_rkey, _, _)) = rows.last() else {
        return Ok(None);
    };
    let mut out = Rewrites::default();
    for (a, k, rev, stored) in &rows {
        // A row without a record (an account with no profile) has nothing
        // to compress.
        let (Some(rev), Some(stored)) = (rev, stored) else { continue };
        if stored.first() != Some(&0) {
            continue;
        }
        let text = super::records::decode(pool, stored).await?;
        let packed = super::records::encode_text(pool, family, &text).await?;
        if packed.first() == Some(&0) {
            continue;
        }
        out.authors.push(*a);
        out.rkeys.push(*k);
        out.revs.push(*rev);
        out.records.push(packed);
    }
    Ok(Some((format!("{last_author}:{last_rkey}"), i64::try_from(rows.len()).unwrap_or(i64::MAX), out)))
}

/// Write a batch's rewrites: a row whose rev moved on, or that is no longer
/// dict 0, or no longer there, is left as it is.
async fn recompress_write(pool: &PgPool, table: RecordTable, rewrites: &Rewrites) -> Result<u64, sqlx::Error> {
    if rewrites.authors.is_empty() {
        return Ok(0);
    }
    let (author_col, rev_col, record_col) = (table.author, table.rev, table.record);
    Ok(sqlx::query(&format!(
        "UPDATE {} t SET {record_col} = v.record \
         FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::bytea[]) AS v(author, rkey, rev, record) \
         WHERE t.{author_col} = v.author AND {} = v.rkey AND t.{rev_col} = v.rev AND get_byte(t.{record_col}, 0) = 0",
        table.table,
        table.key_sql()
    ))
    .bind(&rewrites.authors)
    .bind(&rewrites.rkeys)
    .bind(&rewrites.revs)
    .bind(&rewrites.records)
    .execute(pool)
    .await?
    .rows_affected())
}

/// One recompression batch after `after`: the last key read and the rows
/// read, or `None` at the end of the table.
async fn recompress_batch(pool: &PgPool, family: &str, table: RecordTable, after: &str, limit: i64) -> Result<Option<(String, i64)>, sqlx::Error> {
    let Some((last, rows, rewrites)) = recompress_read(pool, family, table, after, limit).await? else {
        return Ok(None);
    };
    recompress_write(pool, table, &rewrites).await?;
    Ok(Some((last, rows)))
}

/// The planner's row estimate for `table`.
async fn estimated_rows(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar::<_, f32>("SELECT GREATEST(reltuples, 0)::real FROM pg_class WHERE relname = $1 AND relkind = 'r'")
        .bind(table)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map_or(0, |n| n as i64)
}

/// Whether the index named `name` exists, and if so whether it is valid.
async fn index_valid(conn: &mut PgConnection, name: &str) -> Result<Option<bool>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid \
         WHERE c.relname = $1 AND c.relkind = 'i' AND pg_table_is_visible(c.oid)",
    )
    .bind(name)
    .fetch_optional(conn)
    .await
}

/// Whether some backend is building the index named `name` right now.
async fn build_in_flight(conn: &mut PgConnection, name: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_progress_create_index p JOIN pg_class c ON c.oid = p.index_relid \
         WHERE c.relname = $1)",
    )
    .bind(name)
    .fetch_one(conn)
    .await
}

/// The task. Cheap to clone.
#[derive(Clone)]
pub struct SchemaBackfill {
    pool: PgPool,
    cfg: Settings,
    status: SchemaStatus,
}

impl SchemaBackfill {
    #[must_use]
    pub fn new(pool: PgPool, cfg: Settings) -> Self {
        Self {
            pool,
            cfg,
            status: SchemaStatus::default(),
        }
    }

    #[must_use]
    pub fn status(&self) -> SchemaStatus {
        self.status.clone()
    }

    /// Phase 1 for one table: batches until the end of the table, saving
    /// progress after each.
    async fn backfill_table(&self, table: &'static str, key: &'static str) -> Result<(), sqlx::Error> {
        let mut progress = load_progress(&self.pool, key).await?;
        let limit = i64::from(self.cfg.batch_size.max(1));
        let mut estimate = estimated_rows(&self.pool, table).await;
        let mut estimated_at = std::time::Instant::now();
        while !progress.done {
            let batch = actors_batch(&self.pool, &progress.cursor, limit).await;
            match batch {
                Ok(Some((last, rows))) => {
                    progress.cursor = last;
                    progress.visited += rows;
                }
                Ok(None) => progress.done = true,
                Err(e) if lost_a_lock_race(&e) => {
                    tokio::time::sleep(LOCK_RETRY).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
            save_progress(&self.pool, key, &progress).await?;
            // The remaining count is an estimate, refreshed once a minute,
            // never a count(*) per batch.
            if estimated_at.elapsed() >= Duration::from_secs(60) {
                estimate = estimated_rows(&self.pool, table).await;
                estimated_at = std::time::Instant::now();
            }
            let remaining = if progress.done { 0 } else { (estimate - progress.visited).max(0) };
            metrics::gauge!(METRIC_REMAINING, "table" => table).set(remaining as f64);
            if !progress.done {
                tokio::time::sleep(Duration::from_millis(self.cfg.pause_ms)).await;
            }
        }
        metrics::gauge!(METRIC_REMAINING, "table" => table).set(0.0);
        Ok(())
    }

    /// Phase 1: the column backfill of `actors`.
    ///
    /// # Errors
    ///
    /// Any database error other than a lost lock race.
    pub async fn run_column_backfill(&self) -> Result<(), sqlx::Error> {
        self.backfill_table("actors", KEY_ACTORS).await?;
        self.status.backfill_done.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// A dedicated connection for index builds: the pool's session
    /// `lock_timeout` would abort a build that is waiting for old snapshots.
    async fn build_connection(&self) -> Result<PgConnection, sqlx::Error> {
        let options: PgConnectOptions = self.pool.connect_options().as_ref().clone().options([
            ("lock_timeout", "0"),
            ("statement_timeout", "0"),
            ("idle_in_transaction_session_timeout", "0"),
        ]);
        options.connect().await
    }

    /// One attempt at one index: `true` once it exists and is valid.
    async fn build_index(&self, conn: &mut PgConnection, name: &str, create: &str) -> Result<bool, sqlx::Error> {
        match index_valid(conn, name).await? {
            Some(true) => return Ok(true),
            Some(false) => {
                // Left by an interrupted build. Postgres does not notice a
                // vanished client by default, so a backend may still be
                // building it: wait for that one.
                if build_in_flight(conn, name).await? {
                    tokio::time::sleep(LIVE_BUILD_POLL).await;
                    return Ok(false);
                }
                sqlx::query(&format!("DROP INDEX CONCURRENTLY IF EXISTS {name}")).execute(&mut *conn).await?;
            }
            None => {}
        }
        sqlx::query(create).execute(&mut *conn).await?;
        // `IF NOT EXISTS` silently skips an invalid index of the same name.
        Ok(index_valid(conn, name).await? == Some(true))
    }

    /// Phase 2: build every index of [`INDEXES`], one at a time.
    ///
    /// # Errors
    ///
    /// A failure to open the dedicated connection. A failed build is
    /// retried, not returned.
    pub async fn run_index_builds(&self) -> Result<(), sqlx::Error> {
        let mut conn = self.build_connection().await?;
        let mut pending = INDEXES.len();
        metrics::gauge!(METRIC_INDEX_PENDING).set(pending as f64);
        for (name, create) in INDEXES {
            let mut backoff = Duration::from_secs(self.cfg.index_retry_backoff_secs.max(1));
            loop {
                match self.build_index(&mut conn, name, create).await {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => {
                        metrics::counter!(METRIC_INDEX_FAILURES, "index" => name).increment(1);
                        tracing::warn!(event = "schema_index_build_failed", index = name, %error, "index build failed; retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        // The connection may be the casualty: open another.
                        if let Ok(fresh) = self.build_connection().await {
                            conn = fresh;
                        }
                    }
                }
            }
            sqlx::query(
                "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb(now()::text), now()) \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
            )
            .bind(format!("index_build:{name}"))
            .execute(&self.pool)
            .await?;
            pending -= 1;
            metrics::gauge!(METRIC_INDEX_PENDING).set(pending as f64);
        }
        self.status.indexes_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Phase 3 for one family: train its dictionary once there are records
    /// enough, waiting for them, then rewrite the rows stored as dict 0 with
    /// it, table by table.
    ///
    /// # Errors
    ///
    /// Any database error other than a lost lock race.
    pub async fn run_recompression(&self, family: RecordFamily) -> Result<(), sqlx::Error> {
        let mut pending = Vec::new();
        for table in family.tables {
            let key = if family == POSTS { KEY_RECOMPRESS.to_string() } else { table.progress_key() };
            let progress = load_progress(&self.pool, &key).await?;
            if !progress.done {
                pending.push((*table, key, progress));
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        while super::records::current_id(&self.pool, family.family).await? == 0 {
            if train_dict(&self.pool, family).await?.is_none() {
                tokio::time::sleep(TRAIN_RETRY).await;
            }
        }
        let limit = i64::from(self.cfg.batch_size.max(1));
        let mut estimate = 0;
        for (table, _, _) in &pending {
            estimate += estimated_rows(&self.pool, table.table).await;
        }
        let mut visited = 0;
        for (table, key, mut progress) in pending {
            while !progress.done {
                match recompress_batch(&self.pool, family.family, table, &progress.cursor, limit).await {
                    Ok(Some((last, rows))) => {
                        progress.cursor = last;
                        progress.visited += rows;
                        visited += rows;
                    }
                    Ok(None) => progress.done = true,
                    Err(e) if lost_a_lock_race(&e) => {
                        tokio::time::sleep(LOCK_RETRY).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
                save_progress(&self.pool, &key, &progress).await?;
                metrics::gauge!(family.metric).set((estimate - visited).max(0) as f64);
                if !progress.done {
                    tokio::time::sleep(Duration::from_millis(self.cfg.pause_ms)).await;
                }
            }
        }
        metrics::gauge!(family.metric).set(0.0);
        if family == POSTS {
            let dead: Option<i64> = sqlx::query_scalar("SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'posts'")
                .fetch_optional(&self.pool)
                .await?;
            metrics::gauge!(METRIC_POSTS_DEAD_ROWS).set(dead.unwrap_or(0) as f64);
            tracing::info!(event = "record_recompress_done", family = family.family, rows = visited, dead_rows = dead.unwrap_or(0), "post records are compressed");
        } else {
            tracing::info!(event = "record_recompress_done", family = family.family, rows = visited, "records are compressed");
        }
        Ok(())
    }

    /// Run every phase in the background. A database error ends the pass,
    /// and the task tries again after a minute: every step resumes from
    /// what is recorded.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let result = async {
                    self.run_column_backfill().await?;
                    self.run_index_builds().await?;
                    // Each family waits for its own records; neither holds
                    // the other back.
                    tokio::try_join!(self.run_recompression(POSTS), self.run_recompression(PROFILES), self.run_recompression(OTHER))?;
                    Ok::<(), sqlx::Error>(())
                }
                .await;
                match result {
                    Ok(()) => {
                        tracing::info!(event = "schema_backfill_done", "column backfill, index builds and recompression finished");
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(event = "schema_backfill_failed", %error, "schema backfill pass failed; retrying");
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    fn task(pool: &PgPool, batch_size: u32) -> SchemaBackfill {
        SchemaBackfill::new(
            pool.clone(),
            Settings {
                batch_size,
                pause_ms: 0,
                index_retry_backoff_secs: 1,
            },
        )
    }

    const REV_A: &str = "3kaaaaaaaaaaa";
    const REV_B: &str = "3kbbbbbbbbbbb";

    /// An actor row as written before the profile columns existed.
    async fn old_actor(pool: &PgPool, did: &str, rev: Option<&str>, record: Option<Value>) {
        let rev = rev.map(|r| format!("'{r}'"));
        let record = record.map(|r| format!("'{r}'"));
        let mut columns: Vec<(&str, &str)> = vec![("indexed_at", "'2026-01-01T00:00:00Z'")];
        if let Some(r) = &rev {
            columns.push(("profile_rev", r));
        }
        if let Some(r) = &record {
            columns.push(("profile_record", r));
        }
        crate::test_support::actors::put(pool, did, &columns).await;
    }

    async fn id_of(pool: &PgPool, did: &str) -> i32 {
        crate::db::ids::account_id(pool, did).await.expect("id").expect("interned")
    }

    #[tokio::test]
    async fn the_column_backfill_fills_old_rows_and_resumes() {
        let Some(db) = fresh_db("schema_backfill_columns").await else {
            return;
        };
        let p = &db.pool;
        for i in 0..5 {
            let record = json!({"pronouns": format!("p{i}"), "website": "https://a.example", "createdAt": "2030-01-01T00:00:00Z"});
            old_actor(p, &format!("did:plc:a{i}"), Some(REV_A), Some(record)).await;
        }
        // Rows with no rev, or no record, are passed over.
        old_actor(p, "did:plc:b", None, None).await;
        old_actor(p, "did:plc:c", Some(REV_A), None).await;

        // A batch of two, interrupted after one batch: the cursor is kept.
        let t = task(p, 2);
        let first = actors_batch(p, "", 2).await.expect("batch").expect("rows");
        assert_eq!(first, (id_of(p, "did:plc:a1").await.to_string(), 2));
        save_progress(p, KEY_ACTORS, &Progress { cursor: first.0, done: false, visited: 2 }).await.expect("save");

        assert!(!t.status().backfill_done());
        t.run_column_backfill().await.expect("backfill");
        assert!(t.status().backfill_done());
        assert!(load_progress(p, KEY_ACTORS).await.expect("progress").done);

        let filled: Vec<(String, Option<String>, bool)> = sqlx::query_as(
            "SELECT d.did, a.pronouns, a.profile_created_at = '2026-01-01T00:05:00Z' FROM actors a JOIN dids d ON d.id = a.id \
             WHERE d.did LIKE 'did:plc:a%' ORDER BY d.did",
        )
        .fetch_all(p)
        .await
        .expect("actors");
        // The future createdAt is clamped to first-seen plus five minutes.
        let expected: Vec<(String, Option<String>, bool)> = (0..5).map(|i| (format!("did:plc:a{i}"), Some(format!("p{i}")), true)).collect();
        assert_eq!(filled, expected);
        let cid: Option<Vec<u8>> = sqlx::query_scalar(&format!("SELECT profile_cid FROM actors WHERE {}", crate::test_support::actors::is("", "did:plc:a0")))
            .fetch_one(p)
            .await
            .expect("cid");
        assert_eq!(cid, None, "the CID is left for the next write");

        // A cursor stored before rows were keyed by id starts again.
        save_progress(p, KEY_ACTORS, &Progress { cursor: "did:plc:a1".into(), done: false, visited: 0 }).await.expect("save");
        t.run_column_backfill().await.expect("again");
        assert!(load_progress(p, KEY_ACTORS).await.expect("progress").done);
        db.drop_db().await;
    }

    #[tokio::test]
    async fn a_stale_read_never_overwrites_a_rewritten_row() {
        let Some(db) = fresh_db("schema_backfill_stale").await else {
            return;
        };
        let p = &db.pool;
        old_actor(p, "did:plc:a", Some(REV_A), Some(json!({"pronouns": "old"}))).await;
        // The batch's update names the rev it read; the row has since been
        // rewritten by the writer, so the guard on the rev leaves it alone.
        let rewrite = crate::test_support::actors::update_sql("did:plc:a", &format!("profile_rev = rev_to_bigint('{REV_B}'), pronouns = 'new'"));
        sqlx::query(&rewrite).execute(p).await.expect("rewrite");
        let n = sqlx::query(
            "UPDATE actors a SET pronouns = v.pronouns \
             FROM jsonb_to_recordset($1::jsonb) AS v(id integer, rev bigint, pronouns text) \
             WHERE a.id = v.id AND a.profile_rev = v.rev",
        )
        .bind(json!([{"id": id_of(p, "did:plc:a").await, "rev": crate::db::ids::rev_value(REV_A), "pronouns": "old"}]))
        .execute(p)
        .await
        .expect("update")
        .rows_affected();
        assert_eq!(n, 0);
        db.drop_db().await;
    }

    /// Profiles: stored as dict 0, rewritten with their dictionary once
    /// there is one; an account with no profile is passed over, and a
    /// profile purged between a batch's read and its write stays purged.
    #[tokio::test]
    async fn profile_records_are_compressed_and_a_purge_is_kept() {
        let Some(db) = fresh_db("schema_backfill_recompress_profiles").await else {
            return;
        };
        let p = &db.pool;
        let profile = |name: &str| json!({"$type": "app.bsky.actor.profile", "displayName": name, "description": format!("about {name}")});
        for (did, name) in [("did:plc:a", "Ann"), ("did:plc:b", "Bea")] {
            old_actor(p, did, Some(REV_A), Some(profile(name))).await;
        }
        old_actor(p, "did:plc:c", None, None).await;
        assert_eq!(train_dict(p, PROFILES).await.expect("train"), None, "too few records to train from");

        let samples: Vec<Vec<u8>> = (0..500).map(|i| profile(&format!("name {i}")).to_string().into_bytes()).collect();
        let dict = crate::db::ids::train_dict(&samples, 16 * 1024).expect("dictionary");
        sqlx::query("INSERT INTO record_dicts (id, family, bytes) VALUES (1, 'profiles', $1)").bind(&dict).execute(p).await.expect("dict");
        crate::db::records::reload(p).await.expect("reload");

        let actors = PROFILES.tables[0];
        let (_, read, rewrites) = recompress_read(p, PROFILES.family, actors, "", 100).await.expect("read").expect("rows");
        assert_eq!((read, rewrites.authors.len()), (3, 2), "the account with no profile is read and passed over");
        // A purge between the read and the write.
        let purge = crate::test_support::actors::update_sql("did:plc:b", "profile_record = NULL, profile_rev = NULL, display_name = NULL");
        sqlx::query(&purge).execute(p).await.expect("purge");
        assert_eq!(recompress_write(p, actors, &rewrites).await.expect("write"), 1);
        assert_eq!(crate::test_support::actors::record(p, "did:plc:b").await, None, "the purged profile stays purged");
        let stored: Vec<u8> = sqlx::query_scalar(&format!("SELECT profile_record FROM actors WHERE {}", crate::test_support::actors::is("", "did:plc:a")))
            .fetch_one(p)
            .await
            .expect("record");
        assert_eq!(stored[0], 1, "rewritten with the dictionary");
        assert_eq!(crate::test_support::actors::record(p, "did:plc:a").await, Some(profile("Ann")));

        let t = task(p, 1);
        t.run_recompression(PROFILES).await.expect("recompress");
        assert!(load_progress(p, "records:recompress_actors").await.expect("progress").done);
        t.run_recompression(PROFILES).await.expect("again");
        db.drop_db().await;
    }

    /// Post records: dict 0 reads; no dictionary is trained from too few
    /// records; with one, the rows still stored as dict 0 are rewritten, a
    /// row written again between the batch's read and its write keeps the
    /// newer record, and a row deleted meanwhile stays deleted; a record
    /// naming a dictionary that does not exist is an error, never empty.
    #[tokio::test]
    async fn post_records_are_compressed_and_newer_writes_kept() {
        use crate::test_support::posts;

        let Some(db) = fresh_db("schema_backfill_recompress").await else {
            return;
        };
        let p = &db.pool;
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let mut uris = Vec::new();
        for n in 0..3 {
            uris.push(posts::plain(p, did, &posts::tid(n), &format!("post number {n} about rust")).await);
        }
        let first = posts::record(p, &uris[0]).await.expect("dict 0 reads");
        assert_eq!(first["text"], "post number 0 about rust");
        assert_eq!(train_posts_dict(p).await.expect("train"), None, "too few records to train from");

        let samples: Vec<Vec<u8>> = (0..500)
            .map(|i| json!({"$type": "app.bsky.feed.post", "text": format!("sample {i} about rust and tokio"), "createdAt": "2026-10-01T00:00:00Z"}).to_string().into_bytes())
            .collect();
        let dict = crate::db::ids::train_dict(&samples, 16 * 1024).expect("dictionary");
        sqlx::query("INSERT INTO record_dicts (id, family, bytes) VALUES (1, 'posts', $1)").bind(&dict).execute(p).await.expect("dict");
        crate::db::records::reload(p).await.expect("reload");

        let posts_table = POSTS.tables[0];
        let (_, read, rewrites) = recompress_read(p, POSTS.family, posts_table, "", 100).await.expect("read").expect("rows");
        assert_eq!((read, rewrites.authors.len()), (3, 3));
        // Between the read and the write: one post is written again, one
        // deleted.
        posts::edit(p, &uris[1], |r| r["text"] = json!("rewritten")).await;
        sqlx::query(&format!("DELETE FROM posts WHERE {}", posts::key_is("", &uris[2]))).execute(p).await.expect("delete");
        assert_eq!(recompress_write(p, posts_table, &rewrites).await.expect("write"), 1);
        let dict_of = |uri: String| async move {
            sqlx::query_scalar::<_, Vec<u8>>(&format!("SELECT record FROM posts WHERE {}", posts::key_is("", &uri)))
                .fetch_one(p)
                .await
                .expect("record")[0]
        };
        assert_eq!(dict_of(uris[0].clone()).await, 1, "rewritten with the dictionary");
        assert_eq!(posts::record(p, &uris[0]).await, Some(first));
        assert_eq!(dict_of(uris[1].clone()).await, 1, "the newer write used the dictionary itself");
        assert_eq!(posts::record(p, &uris[1]).await.expect("newer")["text"], "rewritten");
        assert!(!posts::stored(p, &uris[2]).await, "a deleted row stays deleted");

        // The task runs the rest to its end, and once only.
        let t = task(p, 2);
        t.run_recompression(POSTS).await.expect("recompress");
        assert!(load_progress(p, KEY_RECOMPRESS).await.expect("progress").done);
        t.run_recompression(POSTS).await.expect("again");

        // A dictionary that is not stored is an error, never an empty record.
        assert!(matches!(crate::db::records::decode(p, &[9, 1, 2, 3]).await, Err(sqlx::Error::Decode(_))));
        db.drop_db().await;
    }

    /// The other kept records: stored as dict 0, trained from once there
    /// are enough of them, then rewritten table by table, a self record
    /// and a labeler service (keyed without an rkey) among them.
    #[tokio::test]
    async fn other_records_are_compressed_table_by_table() {
        use crate::test_support::records;

        let Some(db) = fresh_db("schema_backfill_recompress_other").await else {
            return;
        };
        let p = &db.pool;
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let list = records::list(p, did, "3klist2222222", "Rust people", "app.bsky.graph.defs#curatelist").await;
        let declaration = json!({"$type": "app.bsky.notification.declaration", "allowSubscriptions": "followers"});
        let own = records::self_record(p, did, "app.bsky.notification.declaration", &declaration).await;
        let labeler = records::labeler(p, did, &json!({"$type": "app.bsky.labeler.service", "policies": {"labelValues": ["spam"]}})).await;
        assert_eq!(train_dict(p, OTHER).await.expect("train"), None, "too few records to train from");
        let before = records::record(p, "lists", &list).await.expect("dict 0 reads");

        let samples: Vec<Vec<u8>> = (0..500)
            .map(|i| json!({"$type": "app.bsky.graph.list", "name": format!("list {i}"), "purpose": "app.bsky.graph.defs#curatelist"}).to_string().into_bytes())
            .collect();
        let dict = crate::db::ids::train_dict(&samples, 16 * 1024).expect("dictionary");
        sqlx::query("INSERT INTO record_dicts (id, family, bytes) VALUES (1, 'other', $1)").bind(&dict).execute(p).await.expect("dict");
        crate::db::records::reload(p).await.expect("reload");

        let t = task(p, 1);
        t.run_recompression(OTHER).await.expect("recompress");
        for (table, uri) in [("lists", &list), ("actor_self_records", &own), ("labeler_services", &labeler)] {
            let stored: Vec<u8> = sqlx::query_scalar(&format!("SELECT record FROM {table} WHERE {}", records::key_is(table, "", uri)))
                .fetch_one(p)
                .await
                .expect("record");
            assert_eq!(stored[0], 1, "{table}: rewritten with the dictionary");
            assert!(load_progress(p, &format!("records:recompress_{table}")).await.expect("progress").done, "{table}");
        }
        assert_eq!(records::record(p, "lists", &list).await, Some(before));
        assert_eq!(records::record(p, "actor_self_records", &own).await, Some(declaration));
        // Done: a second run reads nothing.
        t.run_recompression(OTHER).await.expect("again");
        db.drop_db().await;
    }

    #[tokio::test]
    async fn index_builds_create_every_index_and_replace_an_invalid_one() {
        let Some(db) = fresh_db("schema_backfill_indexes").await else {
            return;
        };
        let p = &db.pool;
        let t = task(p, 100);
        let valid = |name: &'static str| async move {
            let mut conn = p.acquire().await.expect("conn");
            index_valid(&mut conn, name).await.expect("lookup")
        };
        assert_eq!(valid("post_search_tags").await, None);
        // An invalid index, as an interrupted concurrent build leaves one.
        sqlx::query("CREATE INDEX post_search_tags ON post_search USING gin (tags)").execute(p).await.expect("index");
        sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'post_search_tags'::regclass")
            .execute(p)
            .await
            .expect("invalidate");
        assert_eq!(valid("post_search_tags").await, Some(false));

        assert!(!t.status().indexes_ready());
        t.run_index_builds().await.expect("builds");
        assert!(t.status().indexes_ready());
        for (name, _) in INDEXES {
            assert_eq!(valid(name).await, Some(true), "{name}");
        }
        let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_state WHERE key LIKE 'index_build:%'")
            .fetch_one(p)
            .await
            .expect("recorded");
        assert_eq!(recorded, 7);
        // A second pass finds every index valid and builds nothing.
        t.run_index_builds().await.expect("again");
        db.drop_db().await;
    }
}
