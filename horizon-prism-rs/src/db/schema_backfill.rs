//! The post-migration backfill task (design-v3 §12.2): a background task of
//! `serve`, started once every migration is applied and ingest is running.
//! It is resumable, idempotent and paced, and it changes no semantics.
//!
//! **Phase 1: column backfill.**
//! - `posts`: fills `tags`, `link_domains`, `link_urls`, `lang_prefixes`,
//!   `embed_type`, `media_type` and `search_tsv` from `posts.record`, in
//!   keyset batches by `uri`. A batch updates only rows whose `search_tsv`
//!   is still NULL and whose `cid` is the one it read, so a row the ingest
//!   writer rewrote meanwhile (and filled itself) is never overwritten with
//!   values derived from a stale read.
//! - `actors`: sets the profile columns from `profile_record`, with the
//!   same guard on `profile_rev`. `profile_cid` is not derived here: it
//!   comes with the record on the next profile write or re-sync.
//! - Progress is kept in `worker_state` (`backfill:posts_v3`,
//!   `backfill:actors_v3`), so a restart resumes.
//!
//! **Phase 2: large-table indexes**, after phase 1, one at a time, each with
//! `CREATE INDEX CONCURRENTLY` on a dedicated connection that has no lock or
//! statement timeout. An invalid index left by an interrupted build is
//! waited for while a backend is still building it, and dropped otherwise.
//! A failed build is retried after a backoff that doubles up to an hour.
//!
//! Until the task finishes, search and trends are incomplete for old rows,
//! and search runs without its indexes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use serde_json::{Value, json};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgConnection, PgPool};
use tokio::task::JoinHandle;

use crate::config::FileConfig;
use crate::firehose::extract::PostExtract;
use crate::firehose::records::ProfileRecord;
use crate::hydration::feed::media_type;

pub const KEY_POSTS: &str = "backfill:posts_v3";
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

/// The large-table indexes, by name, with the one statement that builds
/// each (§3.1: the names and definitions are normative).
pub const INDEXES: [(&str, &str); 9] = [
    ("posts_search", "CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_search ON posts USING gin (search_tsv)"),
    ("posts_tags", "CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_tags ON posts USING gin (tags)"),
    ("posts_domains", "CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_domains ON posts USING gin (link_domains)"),
    ("posts_urls", "CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_urls ON posts USING gin (link_urls)"),
    ("posts_mentions", "CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_mentions ON posts USING gin (mention_dids)"),
    ("posts_langs", "CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_langs ON posts USING gin (lang_prefixes)"),
    ("posts_created", "CREATE INDEX CONCURRENTLY IF NOT EXISTS posts_created ON posts (created_at DESC, uri DESC)"),
    ("likes_via", "CREATE INDEX CONCURRENTLY IF NOT EXISTS likes_via ON likes (via_uri) WHERE via_uri IS NOT NULL"),
    ("reposts_via", "CREATE INDEX CONCURRENTLY IF NOT EXISTS reposts_via ON reposts (via_uri) WHERE via_uri IS NOT NULL"),
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

/// One `posts` batch after `after`: the last `uri` read and the rows read,
/// or `None` at the end of the table.
async fn posts_batch(pool: &PgPool, after: &str, limit: i64) -> Result<Option<(String, i64)>, sqlx::Error> {
    let rows: Vec<(String, String, Value)> =
        sqlx::query_as("SELECT uri, cid, record FROM posts WHERE uri > $1 AND search_tsv IS NULL ORDER BY uri LIMIT $2")
            .bind(after)
            .bind(limit)
            .fetch_all(pool)
            .await?;
    let Some(last) = rows.last().map(|r| r.0.clone()) else {
        return Ok(None);
    };
    let values: Vec<Value> = rows
        .iter()
        .map(|(uri, cid, record)| {
            let x = PostExtract::of(record);
            json!({
                "uri": uri, "cid": cid, "tags": x.tags, "link_domains": x.link_domains, "link_urls": x.link_urls,
                "lang_prefixes": x.lang_prefixes, "embed_type": x.embed_type, "media_type": media_type(record),
            })
        })
        .collect();
    sqlx::query(
        "UPDATE posts p SET tags = v.tags, link_domains = v.link_domains, link_urls = v.link_urls, \
           lang_prefixes = v.lang_prefixes, embed_type = v.embed_type, media_type = v.media_type, \
           search_tsv = to_tsvector('simple', p.text) \
         FROM jsonb_to_recordset($1::jsonb) AS v(uri text, cid text, tags text[], link_domains text[], \
           link_urls text[], lang_prefixes text[], embed_type text, media_type text) \
         WHERE p.uri = v.uri AND p.search_tsv IS NULL AND p.cid = v.cid",
    )
    .bind(Value::Array(values))
    .execute(pool)
    .await?;
    Ok(Some((last, i64::try_from(rows.len()).unwrap_or(i64::MAX))))
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
            let batch = if table == "posts" {
                posts_batch(&self.pool, &progress.cursor, limit).await
            } else {
                actors_batch(&self.pool, &progress.cursor, limit).await
            };
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

    /// Phase 1: the column backfill of `posts`, then `actors`.
    ///
    /// # Errors
    ///
    /// Any database error other than a lost lock race.
    pub async fn run_column_backfill(&self) -> Result<(), sqlx::Error> {
        self.backfill_table("posts", KEY_POSTS).await?;
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

    /// Run both phases in the background. A database error ends the pass,
    /// and the task tries again after a minute: every step resumes from
    /// what is recorded.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let result = async {
                    self.run_column_backfill().await?;
                    self.run_index_builds().await
                }
                .await;
                match result {
                    Ok(()) => {
                        tracing::info!(event = "schema_backfill_done", "column backfill and index builds finished");
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

    async fn old_post(pool: &PgPool, rkey: &str, cid: &str, record: Value) {
        // As written before the columns existed: every new column NULL.
        sqlx::query(
            "INSERT INTO posts (uri, author_did, rkey, cid, rev, text, record, created_at) \
             VALUES ($1, 'did:plc:a', $2, $3, '3k', $4, $5, now() - interval '30 days')",
        )
        .bind(format!("at://did:plc:a/app.bsky.feed.post/{rkey}"))
        .bind(rkey)
        .bind(cid)
        .bind(record.get("text").and_then(Value::as_str).unwrap_or_default().to_string())
        .bind(&record)
        .execute(pool)
        .await
        .expect("post");
    }

    #[tokio::test]
    async fn the_column_backfill_fills_old_rows_and_resumes() {
        let Some(db) = fresh_db("schema_backfill_columns").await else {
            return;
        };
        let p = &db.pool;
        for i in 0..7 {
            let record = json!({"text": format!("post number {i}"), "tags": ["Cats"], "langs": ["en-GB"],
                "facets": [{"features": [{"$type": "app.bsky.richtext.facet#link", "uri": "https://a.example.com/x"}]}]});
            old_post(p, &format!("3k{i}"), "bafyold", record).await;
        }
        // A row the writer already filled keeps what it has.
        sqlx::query("UPDATE posts SET tags = '{writer}', search_tsv = to_tsvector('simple', 'writer') WHERE rkey = '3k6'")
            .execute(p)
            .await
            .expect("filled");
        sqlx::query(
            "INSERT INTO actors (did, profile_rev, profile_record, indexed_at) VALUES \
             ('did:plc:a', '3k', '{\"pronouns\": \"it/its\", \"website\": \"https://a.example\", \"createdAt\": \"2030-01-01T00:00:00Z\"}', '2026-01-01T00:00:00Z'), \
             ('did:plc:b', NULL, NULL, now()), \
             ('did:plc:c', '3k', NULL, now())",
        )
        .execute(p)
        .await
        .expect("actors");

        // A batch of three, interrupted after one batch: the cursor is kept.
        let t = task(p, 3);
        let first = posts_batch(p, "", 3).await.expect("batch").expect("rows");
        assert_eq!(first, ("at://did:plc:a/app.bsky.feed.post/3k2".to_string(), 3));
        save_progress(p, KEY_POSTS, &Progress { cursor: first.0, done: false, visited: 3 }).await.expect("save");

        assert!(!t.status().backfill_done());
        t.run_column_backfill().await.expect("backfill");
        assert!(t.status().backfill_done());
        assert!(load_progress(p, KEY_POSTS).await.expect("progress").done);

        type Filled = (String, Vec<String>, Vec<String>, Vec<String>, bool);
        let filled: Vec<Filled> = sqlx::query_as(
            "SELECT rkey, tags, COALESCE(link_domains, '{}'), COALESCE(lang_prefixes, '{}'), \
                    search_tsv @@ to_tsquery('simple', 'number') FROM posts ORDER BY rkey",
        )
        .fetch_all(p)
        .await
        .expect("posts");
        assert_eq!(filled.len(), 7);
        for row in &filled[..6] {
            assert_eq!((&row.1, &row.2, &row.3, row.4), (&vec!["cats".to_string()], &vec!["a.example.com".to_string(), "example.com".to_string()], &vec!["en".to_string(), "en-gb".to_string()], true), "{}", row.0);
        }
        assert_eq!(filled[6].1, ["writer"], "the writer's values are kept");

        let actor: (Option<String>, Option<String>, bool, Option<String>) = sqlx::query_as(
            "SELECT pronouns, website, profile_created_at = '2026-01-01T00:05:00Z', profile_cid FROM actors WHERE did = 'did:plc:a'",
        )
        .fetch_one(p)
        .await
        .expect("actor");
        // The future createdAt is clamped to first-seen plus five minutes,
        // and the CID is left for the next write.
        assert_eq!(actor, (Some("it/its".into()), Some("https://a.example".into()), true, None));

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
        old_post(p, "3k1", "bafyold", json!({"text": "old text", "tags": ["old"]})).await;
        // The batch's update names the cid it read; the row has since been
        // rewritten by the writer (new cid, columns still NULL in this
        // contrived case), so the guard on cid leaves it alone.
        let values = json!([{"uri": "at://did:plc:a/app.bsky.feed.post/3k1", "cid": "bafyold", "tags": ["old"],
            "link_domains": [], "link_urls": [], "lang_prefixes": [], "embed_type": null, "media_type": null}]);
        sqlx::query("UPDATE posts SET cid = 'bafynew' WHERE rkey = '3k1'").execute(p).await.expect("rewrite");
        let n = sqlx::query(
            "UPDATE posts p SET tags = v.tags, search_tsv = to_tsvector('simple', p.text) \
             FROM jsonb_to_recordset($1::jsonb) AS v(uri text, cid text, tags text[]) \
             WHERE p.uri = v.uri AND p.search_tsv IS NULL AND p.cid = v.cid",
        )
        .bind(values)
        .execute(p)
        .await
        .expect("update")
        .rows_affected();
        assert_eq!(n, 0);
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
        assert_eq!(valid("posts_tags").await, None);
        // An invalid index, as an interrupted concurrent build leaves one.
        sqlx::query("CREATE INDEX posts_tags ON posts USING gin (tags)").execute(p).await.expect("index");
        sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'posts_tags'::regclass")
            .execute(p)
            .await
            .expect("invalidate");
        assert_eq!(valid("posts_tags").await, Some(false));

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
        assert_eq!(recorded, 9);
        // A second pass finds every index valid and builds nothing.
        t.run_index_builds().await.expect("again");
        db.drop_db().await;
    }
}
