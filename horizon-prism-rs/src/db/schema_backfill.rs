//! The post-migration backfill task (design-v3 §12.2): a background task of
//! `serve`, started once every migration is applied and ingest is running.
//! It is resumable, idempotent and paced, and it changes no semantics.
//!
//! **Phase 1: column backfill.**
//! - `actors`: sets the profile columns from `profile_record`, in keyset
//!   batches by `did`. A batch updates only rows whose `profile_rev` is the
//!   one it read, so a row the ingest writer rewrote meanwhile is never
//!   overwritten with values derived from a stale read. `profile_cid` is not
//!   derived here: it comes with the record on the next profile write or
//!   re-sync.
//! - Progress is kept in `worker_state` (`backfill:actors_v3`), so a
//!   restart resumes. (The search columns of posts were filled here once;
//!   they now live in `post_search`, which migration 0057 filled.)
//!
//! **Phase 2: large-table indexes**, after phase 1, one at a time, each with
//! `CREATE INDEX CONCURRENTLY` on a dedicated connection that has no lock or
//! statement timeout. An invalid index left by an interrupted build is
//! waited for while a backend is still building it, and dropped otherwise.
//! A failed build is retried after a backoff that doubles up to an hour.
//!
//! **Phase 3: post records compressed.** Once the instance holds
//! [`TRAIN_RECORDS`] posts, a dictionary is trained from the earliest of
//! them and stored in `record_dicts`; writes use it from then on. Then the
//! rows still stored as dict 0 (the JSON text) are rewritten with it, in
//! keyset batches by `(author, rkey)`, each update guarded by the rev and
//! the dict byte it read, so a newer write, a delete or a purge in between
//! makes it a no-op and nothing deleted comes back. Progress is kept in
//! `worker_state` (`records:recompress_posts`).
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

/// One `actors` batch after `after`, over rows that have a profile rev.
async fn actors_batch(pool: &PgPool, after: &str, limit: i64) -> Result<Option<(String, i64)>, sqlx::Error> {
    let rows: Vec<(String, String, Option<Value>)> = sqlx::query_as(
        "SELECT did, profile_rev, profile_record FROM actors WHERE did > $1 AND profile_rev IS NOT NULL ORDER BY did LIMIT $2",
    )
    .bind(after)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let Some(last) = rows.last().map(|r| r.0.clone()) else {
        return Ok(None);
    };
    let now = Utc::now();
    let values: Vec<Value> = rows
        .iter()
        .filter_map(|(did, rev, record)| {
            let p = ProfileRecord::parse(record.as_ref()?, now);
            Some(json!({
                "did": did, "rev": rev, "pronouns": p.pronouns, "website": p.website,
                "via_uri": p.joined_via_starter_pack.as_ref().map(|s| s.uri.clone()),
                "via_cid": p.joined_via_starter_pack.as_ref().and_then(|s| s.cid.clone()),
                "created": p.created_at.map(|t| t.to_rfc3339()),
            }))
        })
        .collect();
    // `profile_created_at` is clamped to the row's first-seen time plus five
    // minutes, as a post's is (§3.1).
    sqlx::query(
        "UPDATE actors a SET pronouns = v.pronouns, website = v.website, \
           joined_via_starter_pack_uri = v.via_uri, joined_via_starter_pack_cid = v.via_cid, \
           profile_created_at = LEAST(v.created, a.indexed_at + interval '5 minutes') \
         FROM jsonb_to_recordset($1::jsonb) AS v(did text, rev text, pronouns text, website text, \
           via_uri text, via_cid text, created timestamptz) \
         WHERE a.did = v.did AND a.profile_rev = v.rev",
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
    let have: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM record_dicts WHERE family = $1)")
        .bind(super::records::FAMILY_POSTS)
        .fetch_one(pool)
        .await?;
    if have {
        return Ok(None);
    }
    let stored: Vec<Vec<u8>> = sqlx::query_scalar("SELECT record FROM posts ORDER BY created_at LIMIT $1").bind(TRAIN_RECORDS).fetch_all(pool).await?;
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
            tracing::warn!(event = "record_dict_training_failed", %error, "the posts dictionary could not be trained");
            return Ok(None);
        }
    };
    let id: i16 = sqlx::query_scalar(
        "INSERT INTO record_dicts (id, family, bytes) SELECT COALESCE(max(id), 0) + 1, $1, $2 FROM record_dicts RETURNING id",
    )
    .bind(super::records::FAMILY_POSTS)
    .bind(&dict)
    .fetch_one(pool)
    .await?;
    super::records::reload(pool).await?;
    tracing::info!(event = "record_dict_trained", id, family = super::records::FAMILY_POSTS, bytes = dict.len(), "a record dictionary was trained");
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

/// Read one recompression batch after `after` (`author:rkey`): the last key
/// read, the rows read, and the rows stored as dict 0 with their records
/// encoded again; `None` at the end of the table.
async fn recompress_read(pool: &PgPool, after: &str, limit: i64) -> Result<Option<(String, i64, Rewrites)>, sqlx::Error> {
    let (author, rkey) = after
        .split_once(':')
        .and_then(|(a, k)| Some((a.parse::<i32>().ok()?, k.parse::<i64>().ok()?)))
        .unwrap_or((i32::MIN, i64::MIN));
    let rows: Vec<(i32, i64, i64, Vec<u8>)> = sqlx::query_as(
        "SELECT author, rkey, rev, record FROM posts WHERE (author, rkey) > ($1, $2) ORDER BY author, rkey LIMIT $3",
    )
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
        if stored.first() != Some(&0) {
            continue;
        }
        let text = super::records::decode(pool, stored).await?;
        let packed = super::records::encode_text(pool, super::records::FAMILY_POSTS, &text).await?;
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
async fn recompress_write(pool: &PgPool, rewrites: &Rewrites) -> Result<u64, sqlx::Error> {
    if rewrites.authors.is_empty() {
        return Ok(0);
    }
    Ok(sqlx::query(
        "UPDATE posts p SET record = v.record \
         FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::bytea[]) AS v(author, rkey, rev, record) \
         WHERE p.author = v.author AND p.rkey = v.rkey AND p.rev = v.rev AND get_byte(p.record, 0) = 0",
    )
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
async fn recompress_batch(pool: &PgPool, after: &str, limit: i64) -> Result<Option<(String, i64)>, sqlx::Error> {
    let Some((last, rows, rewrites)) = recompress_read(pool, after, limit).await? else {
        return Ok(None);
    };
    recompress_write(pool, &rewrites).await?;
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

    /// Phase 3: train the posts dictionary once there are records enough,
    /// waiting for them, then rewrite the rows stored as dict 0 with it.
    ///
    /// # Errors
    ///
    /// Any database error other than a lost lock race.
    pub async fn run_recompression(&self) -> Result<(), sqlx::Error> {
        let mut progress = load_progress(&self.pool, KEY_RECOMPRESS).await?;
        if progress.done {
            return Ok(());
        }
        while super::records::current_id(&self.pool, super::records::FAMILY_POSTS).await? == 0 {
            if train_posts_dict(&self.pool).await?.is_none() {
                tokio::time::sleep(TRAIN_RETRY).await;
            }
        }
        let limit = i64::from(self.cfg.batch_size.max(1));
        let estimate = estimated_rows(&self.pool, "posts").await;
        while !progress.done {
            match recompress_batch(&self.pool, &progress.cursor, limit).await {
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
            save_progress(&self.pool, KEY_RECOMPRESS, &progress).await?;
            let remaining = if progress.done { 0 } else { (estimate - progress.visited).max(0) };
            metrics::gauge!(METRIC_RECOMPRESS_REMAINING).set(remaining as f64);
            if !progress.done {
                tokio::time::sleep(Duration::from_millis(self.cfg.pause_ms)).await;
            }
        }
        let dead: Option<i64> = sqlx::query_scalar("SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'posts'")
            .fetch_optional(&self.pool)
            .await?;
        metrics::gauge!(METRIC_POSTS_DEAD_ROWS).set(dead.unwrap_or(0) as f64);
        tracing::info!(event = "record_recompress_done", rows = progress.visited, dead_rows = dead.unwrap_or(0), "post records are compressed");
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
                    self.run_recompression().await
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

    /// An actor row as written before the profile columns existed.
    async fn old_actor(pool: &PgPool, did: &str, rev: Option<&str>, record: Option<Value>) {
        sqlx::query("INSERT INTO actors (did, profile_rev, profile_record, indexed_at) VALUES ($1, $2, $3, '2026-01-01T00:00:00Z')")
            .bind(did)
            .bind(rev)
            .bind(record)
            .execute(pool)
            .await
            .expect("actor");
    }

    #[tokio::test]
    async fn the_column_backfill_fills_old_rows_and_resumes() {
        let Some(db) = fresh_db("schema_backfill_columns").await else {
            return;
        };
        let p = &db.pool;
        for i in 0..5 {
            let record = json!({"pronouns": format!("p{i}"), "website": "https://a.example", "createdAt": "2030-01-01T00:00:00Z"});
            old_actor(p, &format!("did:plc:a{i}"), Some("3k"), Some(record)).await;
        }
        // Rows with no rev, or no record, are passed over.
        old_actor(p, "did:plc:b", None, None).await;
        old_actor(p, "did:plc:c", Some("3k"), None).await;

        // A batch of two, interrupted after one batch: the cursor is kept.
        let t = task(p, 2);
        let first = actors_batch(p, "", 2).await.expect("batch").expect("rows");
        assert_eq!(first, ("did:plc:a1".to_string(), 2));
        save_progress(p, KEY_ACTORS, &Progress { cursor: first.0, done: false, visited: 2 }).await.expect("save");

        assert!(!t.status().backfill_done());
        t.run_column_backfill().await.expect("backfill");
        assert!(t.status().backfill_done());
        assert!(load_progress(p, KEY_ACTORS).await.expect("progress").done);

        let filled: Vec<(String, Option<String>, bool)> = sqlx::query_as(
            "SELECT did, pronouns, profile_created_at = '2026-01-01T00:05:00Z' FROM actors WHERE did LIKE 'did:plc:a%' ORDER BY did",
        )
        .fetch_all(p)
        .await
        .expect("actors");
        // The future createdAt is clamped to first-seen plus five minutes.
        let expected: Vec<(String, Option<String>, bool)> = (0..5).map(|i| (format!("did:plc:a{i}"), Some(format!("p{i}")), true)).collect();
        assert_eq!(filled, expected);
        let cid: Option<String> = sqlx::query_scalar("SELECT profile_cid FROM actors WHERE did = 'did:plc:a0'").fetch_one(p).await.expect("cid");
        assert_eq!(cid, None, "the CID is left for the next write");

        // Running again does nothing and stays done.
        t.run_column_backfill().await.expect("again");
        db.drop_db().await;
    }

    #[tokio::test]
    async fn a_stale_read_never_overwrites_a_rewritten_row() {
        let Some(db) = fresh_db("schema_backfill_stale").await else {
            return;
        };
        let p = &db.pool;
        old_actor(p, "did:plc:a", Some("3k"), Some(json!({"pronouns": "old"}))).await;
        // The batch's update names the rev it read; the row has since been
        // rewritten by the writer, so the guard on the rev leaves it alone.
        sqlx::query("UPDATE actors SET profile_rev = '3l', pronouns = 'new' WHERE did = 'did:plc:a'").execute(p).await.expect("rewrite");
        let n = sqlx::query(
            "UPDATE actors a SET pronouns = v.pronouns \
             FROM jsonb_to_recordset($1::jsonb) AS v(did text, rev text, pronouns text) \
             WHERE a.did = v.did AND a.profile_rev = v.rev",
        )
        .bind(json!([{"did": "did:plc:a", "rev": "3k", "pronouns": "old"}]))
        .execute(p)
        .await
        .expect("update")
        .rows_affected();
        assert_eq!(n, 0);
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

        let (_, read, rewrites) = recompress_read(p, "", 100).await.expect("read").expect("rows");
        assert_eq!((read, rewrites.authors.len()), (3, 3));
        // Between the read and the write: one post is written again, one
        // deleted.
        posts::edit(p, &uris[1], |r| r["text"] = json!("rewritten")).await;
        sqlx::query(&format!("DELETE FROM posts WHERE {}", posts::key_is("", &uris[2]))).execute(p).await.expect("delete");
        assert_eq!(recompress_write(p, &rewrites).await.expect("write"), 1);
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
        t.run_recompression().await.expect("recompress");
        assert!(load_progress(p, KEY_RECOMPRESS).await.expect("progress").done);
        t.run_recompression().await.expect("again");

        // A dictionary that is not stored is an error, never an empty record.
        assert!(matches!(crate::db::records::decode(p, &[9, 1, 2, 3]).await, Err(sqlx::Error::Decode(_))));
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
