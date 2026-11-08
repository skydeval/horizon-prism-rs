//! Gaps and the re-list pass, and the sweeper on the merged stream's own
//! clock.
//!
//! A gap (a trim gap, a stream recreation, a relay reader's `#resume`, a
//! spell with no Whole or Covered relay, the periodic backstop) writes a
//! `full_gaps` row. While a blocking row is open no non-S, non-member mark
//! counts. A re-list pass pages every Whole and Covered relay's `listRepos`
//! and, once the low-water entry has passed the stream tail read with each
//! page, compares each listed head with the account's `last_rev`: a head
//! above it is a requeue (`last_rev` raised to the head, then rebased as a
//! hole). The pass's end resolves every row opened before it began (a
//! `continuity` row only when the relay had caught up before the pass
//! began). The takedown relays' status is read on the same pages.
//!
//! The sweeper turns a link into a hole once the low-water entry is a
//! window past it, and an account whose Aside copies no Whole or Covered
//! relay has matched for a window into `uncovered`.

use serde_json::{Value, json};
use sqlx::PgPool;

use super::client::ListedRepo;
use super::continuity;
use super::task::Crawler;
use crate::db::ids::AccountId;

/// Metric: re-list requeues.
pub const METRIC_REQUEUES: &str = "full_relist_requeues_total";
/// Metric: accounts turned `uncovered`.
pub const METRIC_UNCOVERED: &str = "full_uncovered_total";

/// Open a gap row.
///
/// # Errors
///
/// Any database error.
pub async fn open_gap(pool: &PgPool, kind: &str, relay: Option<&str>, blocking: bool, detail: Value) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO full_gaps (kind, relay, blocking, detail) VALUES ($1, $2, $3, $4)")
        .bind(kind)
        .bind(relay)
        .bind(blocking)
        .bind(detail)
        .execute(pool)
        .await?;
    tracing::info!(event = "full_gap_opened", kind, relay = ?relay, blocking, "full indexing recorded a gap; a re-list pass resolves it");
    Ok(())
}

/// Whether a blocking gap row is open: no non-S, non-member mark counts.
///
/// # Errors
///
/// Any database error.
pub async fn blocked(pool: &PgPool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM full_gaps WHERE resolved_at IS NULL AND blocking)").fetch_one(pool).await
}

/// The stream-id milliseconds of an entry id.
#[must_use]
pub fn id_ms(id: &str) -> Option<i64> {
    id.split_once('-').and_then(|(ms, _)| ms.parse().ok()).or_else(|| id.parse().ok())
}

/// The re-list pass's stored state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Pass {
    pub started_at: Option<String>,
    /// The newest gap row the pass resolves.
    pub upto: i64,
    pub blocking: bool,
    /// Relay label → its cursor; `"done"` when paged through.
    pub cursors: serde_json::Map<String, Value>,
    pub last_end: Option<String>,
}

impl Pass {
    fn parse(v: &Value) -> Self {
        Self {
            started_at: v.get("started_at").and_then(Value::as_str).map(str::to_string),
            upto: v.get("upto").and_then(Value::as_i64).unwrap_or(0),
            blocking: v.get("blocking").and_then(Value::as_bool).unwrap_or(false),
            cursors: v.get("cursors").and_then(Value::as_object).cloned().unwrap_or_default(),
            last_end: v.get("last_end").and_then(Value::as_str).map(str::to_string),
        }
    }

    fn to_json(&self) -> Value {
        json!({ "started_at": self.started_at, "upto": self.upto, "blocking": self.blocking, "cursors": self.cursors, "last_end": self.last_end })
    }
}

async fn read_pass(pool: &PgPool) -> Result<Pass, sqlx::Error> {
    let v: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(super::RELIST_KEY).fetch_optional(pool).await?;
    Ok(v.as_ref().map(Pass::parse).unwrap_or_default())
}

async fn write_pass(pool: &PgPool, p: &Pass) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(super::RELIST_KEY)
    .bind(p.to_json())
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert one page's candidates: listed accounts with a row whose head is
/// above `last_rev`, or whose `last_rev` is NULL and whose row is not
/// `pending` (a crawled account never seen live), or that are `uncovered`.
async fn page_candidates(pool: &PgPool, repos: &[ListedRepo], relay: &str, tail_ms: i64, page_ms: i64) -> Result<u64, sqlx::Error> {
    let (dids, heads): (Vec<String>, Vec<String>) = repos.iter().filter_map(|r| Some((r.did.clone(), r.rev.clone()?))).unzip();
    if dids.is_empty() {
        return Ok(0);
    }
    Ok(sqlx::query(
        "INSERT INTO full_relist_candidates (account, head, tail_ms, relay, page_ms) \
         SELECT f.account, l.head, $3, $4, $5 FROM unnest($1::text[], $2::text[]) l (did, head) \
         JOIN dids d ON d.did = l.did JOIN full_crawl f ON f.account = d.id \
         WHERE f.uncovered OR (f.last_rev IS NULL AND f.state <> 'pending') OR l.head COLLATE \"C\" > f.last_rev \
         ON CONFLICT (account) DO UPDATE SET head = GREATEST(full_relist_candidates.head, EXCLUDED.head), \
           tail_ms = GREATEST(full_relist_candidates.tail_ms, EXCLUDED.tail_ms), page_ms = EXCLUDED.page_ms",
    )
    .bind(&dids)
    .bind(&heads)
    .bind(tail_ms)
    .bind(relay)
    .bind(page_ms)
    .execute(pool)
    .await?
    .rows_affected())
}

/// One candidate, compared under its row's lock: a head above `last_rev`
/// (or a crawled account never seen live) raises `last_rev` to it and is
/// rebased as a hole, which queues it; a quiet `uncovered` account gets its
/// one-shot requeue at the page's stream time instead, still `uncovered`.
async fn compare(pool: &PgPool, account: AccountId, head: &str, page_ms: i64, crawl_generation: i64, groups: &[String]) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    type Row = (Option<String>, String, bool);
    let row: Option<Row> = sqlx::query_as("SELECT last_rev, state, uncovered FROM full_crawl WHERE account = $1 FOR UPDATE")
        .bind(account)
        .fetch_optional(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM full_relist_candidates WHERE account = $1").bind(account).execute(&mut *tx).await?;
    let requeued = match row {
        Some((_, state, true)) => {
            if !matches!(state.as_str(), "gone" | "skipped") {
                sqlx::query(
                    "UPDATE full_crawl SET uncovered_requeue = $2, groups = $3, next_at = now(), \
                       state = CASE WHEN state = 'fetching' THEN state ELSE 'pending' END WHERE account = $1",
                )
                .bind(account)
                .bind(page_ms)
                .bind(groups)
                .execute(&mut *tx)
                .await?;
            }
            true
        }
        Some((last, state, false)) if last.as_deref().is_none_or(|l| head > l) && !(last.is_none() && state == "pending") => {
            sqlx::query("UPDATE full_crawl SET last_rev = CASE WHEN last_rev IS NULL OR last_rev < $2 THEN $2 ELSE last_rev END WHERE account = $1")
                .bind(account)
                .bind(head)
                .execute(&mut *tx)
                .await?;
            continuity::hole(&mut tx, account, crawl_generation, groups, "relist").await?;
            // The rise may have reached the Aside rev.
            let reached: bool = sqlx::query_scalar("SELECT aside_rev IS NOT NULL AND last_rev >= aside_rev FROM full_crawl WHERE account = $1")
                .bind(account)
                .fetch_one(&mut *tx)
                .await?;
            if reached {
                continuity::clear_aside(&mut tx, account).await?;
            }
            true
        }
        _ => false,
    };
    tx.commit().await?;
    if requeued {
        metrics::counter!(METRIC_REQUEUES).increment(1);
    }
    Ok(requeued)
}

impl Crawler {
    /// The stream's newest entry id's milliseconds: what a page waits for.
    async fn tail_ms(&self) -> Option<i64> {
        let mut redis = self.redis.clone()?;
        let reply: redis::streams::StreamRangeReply = redis::cmd("XREVRANGE")
            .arg(&self.stream_key)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut redis)
            .await
            .ok()?;
        reply.ids.first().and_then(|e| id_ms(&e.id))
    }

    /// The low-water entry's milliseconds, when the consumer has read one.
    pub(super) fn low_water_ms(&self) -> Option<i64> {
        self.low_water.as_ref()?.get().as_deref().and_then(id_ms)
    }

    /// One tick of the sweeper: links a window behind the low-water entry
    /// become holes, after a re-read under the row's lock; while no
    /// blocking gap is open, accounts whose Aside copies have gone a window
    /// unmatched turn `uncovered`; the continuity set is watched.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn sweep(&self, crawl_generation: i64, groups: &[String]) -> Result<(), sqlx::Error> {
        let Some(lw) = self.low_water_ms() else {
            return Ok(());
        };
        let due = lw - self.window_ms;
        let accounts: Vec<AccountId> = sqlx::query_scalar("SELECT DISTINCT account FROM full_chain_links WHERE linked_ms <= $1 LIMIT 500")
            .bind(due)
            .fetch_all(&self.pool)
            .await?;
        for account in accounts {
            let mut tx = self.pool.begin().await?;
            let held: Option<i64> = sqlx::query_scalar("SELECT epoch FROM full_crawl WHERE account = $1 FOR UPDATE").bind(account).fetch_optional(&mut *tx).await?;
            if held.is_none() {
                sqlx::query("DELETE FROM full_chain_links WHERE account = $1").bind(account).execute(&mut *tx).await?;
                tx.commit().await?;
                continue;
            }
            // A link a lane closed meanwhile, or at or below the chain, goes;
            // one still open past the window is a hole.
            sqlx::query("DELETE FROM full_chain_links l USING full_crawl f WHERE l.account = $1 AND f.account = $1 AND l.rev <= f.chain_rev")
                .bind(account)
                .execute(&mut *tx)
                .await?;
            let open: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM full_chain_links WHERE account = $1 AND linked_ms <= $2)")
                .bind(account)
                .bind(due)
                .fetch_one(&mut *tx)
                .await?;
            if open {
                continuity::hole(&mut tx, account, crawl_generation, groups, "link_timeout").await?;
            }
            tx.commit().await?;
        }
        // The continuity set: empty while no Whole or Covered copy came
        // within a window of the low-water entry.
        let covered = self.covered.load(std::sync::atomic::Ordering::Relaxed);
        if covered > 0 {
            let open: Option<(i64, bool)> =
                sqlx::query_as("SELECT id, caught_up_at IS NOT NULL FROM full_gaps WHERE kind = 'continuity' AND resolved_at IS NULL ORDER BY id DESC LIMIT 1")
                    .fetch_optional(&self.pool)
                    .await?;
            match (lw - covered > self.window_ms, open) {
                (true, None) => open_gap(&self.pool, "continuity", None, true, json!({ "covered_ms": covered, "low_water_ms": lw })).await?,
                (false, Some((id, false))) => {
                    sqlx::query("UPDATE full_gaps SET caught_up_at = now() WHERE id = $1").bind(id).execute(&self.pool).await?;
                }
                _ => {}
            }
        }
        if blocked(&self.pool).await? {
            return Ok(());
        }
        let turned: Vec<AccountId> = sqlx::query_scalar(
            "UPDATE full_crawl SET uncovered = true WHERE account IN ( \
               SELECT account FROM full_crawl WHERE aside_ms IS NOT NULL AND NOT uncovered AND aside_ms <= $1 \
                 AND (last_rev IS NULL OR aside_rev > last_rev) LIMIT 500 FOR UPDATE) \
             RETURNING account",
        )
        .bind(due)
        .fetch_all(&self.pool)
        .await?;
        if !turned.is_empty() {
            sqlx::query("DELETE FROM full_crawl_groups WHERE account = ANY($1)").bind(&turned).execute(&self.pool).await?;
            metrics::counter!(METRIC_UNCOVERED).increment(u64::try_from(turned.len()).unwrap_or(0));
        }
        Ok(())
    }

    /// One tick of the re-list: open the periodic row when due, start a
    /// pass for open rows, page each listing relay once, compare the
    /// candidates the low-water entry has passed, and end the pass when
    /// every relay is paged through and its candidates are compared.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn relist(&self, crawl_generation: i64, groups: &[String]) -> Result<(), sqlx::Error> {
        let mut pass = read_pass(&self.pool).await?;
        // The periodic backstop.
        if self.full.relist_every_days > 0 {
            let since: Option<chrono::DateTime<chrono::Utc>> = pass
                .last_end
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&chrono::Utc));
            let open: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM full_gaps WHERE kind = 'periodic' AND resolved_at IS NULL)").fetch_one(&self.pool).await?;
            match since {
                None if pass.started_at.is_none() => {
                    pass.last_end = Some(chrono::Utc::now().to_rfc3339());
                    write_pass(&self.pool, &pass).await?;
                }
                Some(t) if !open && chrono::Utc::now() - t > chrono::TimeDelta::days(i64::from(self.full.relist_every_days)) => {
                    open_gap(&self.pool, "periodic", None, false, json!({})).await?;
                }
                _ => {}
            }
        }
        let (newest, any_blocking): (Option<i64>, bool) =
            sqlx::query_as("SELECT max(id), COALESCE(bool_or(blocking), false) FROM full_gaps WHERE resolved_at IS NULL").fetch_one(&self.pool).await?;
        let Some(newest) = newest else {
            return Ok(());
        };
        let relays = self.listing_relays();
        if relays.is_empty() {
            // Fail closed: the rows stay open.
            return Ok(());
        }
        // A blocking row stops a periodic pass, and the blocking pass starts
        // at once.
        if pass.started_at.is_some() && !pass.blocking && any_blocking {
            pass.started_at = None;
        }
        if pass.started_at.is_none() {
            pass = Pass {
                started_at: Some(chrono::Utc::now().to_rfc3339()),
                upto: newest,
                blocking: any_blocking,
                cursors: relays.iter().map(|r| (r.label.clone(), Value::Null)).collect(),
                last_end: pass.last_end,
            };
            sqlx::query("DELETE FROM full_relist_candidates").execute(&self.pool).await?;
            write_pass(&self.pool, &pass).await?;
            tracing::info!(event = "full_relist_started", upto = newest, blocking = any_blocking, "a re-list pass started");
        }
        // Page each relay once, at the pass's pace.
        for relay in &relays {
            let cursor = pass.cursors.get(&relay.label).cloned().unwrap_or(Value::Null);
            if cursor.as_str() == Some("done") {
                continue;
            }
            self.relist_pace.wait().await;
            let tail = match self.fixed_tail {
                Some(t) => t,
                None => self.tail_ms().await.or_else(|| self.low_water_ms()).unwrap_or(0),
            };
            match self.source.list_repos(&relay.base, cursor.as_str()).await {
                Ok(page) => {
                    let page_ms = chrono::Utc::now().timestamp_millis();
                    page_candidates(&self.pool, &page.repos, &relay.label, tail, page_ms).await?;
                    if self.status_relay(&relay.label) {
                        super::task::record_statuses(&self.pool, &relay.label, &page.repos).await?;
                    }
                    let next = match page.cursor {
                        Some(c) if !page.repos.is_empty() => Value::String(c),
                        _ => Value::String("done".into()),
                    };
                    pass.cursors.insert(relay.label.clone(), next);
                    write_pass(&self.pool, &pass).await?;
                }
                Err(error) => {
                    tracing::warn!(event = "full_relist_waiting", relay = %relay.label, %error, "the re-list is waiting on a relay; its gap stays open");
                }
            }
        }
        // Compare what the stream has passed.
        if let Some(lw) = self.low_water_ms() {
            type Cand = (AccountId, String, i64);
            let due: Vec<Cand> = sqlx::query_as("SELECT account, head, page_ms FROM full_relist_candidates WHERE tail_ms <= $1 ORDER BY tail_ms LIMIT 1000")
                .bind(lw)
                .fetch_all(&self.pool)
                .await?;
            for (account, head, page_ms) in due {
                compare(&self.pool, account, &head, page_ms, crawl_generation, groups).await?;
            }
        }
        let paged = relays.iter().all(|r| pass.cursors.get(&r.label).and_then(Value::as_str) == Some("done"));
        let left: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM full_relist_candidates)").fetch_one(&self.pool).await?;
        if paged && !left {
            let started = pass.started_at.clone().unwrap_or_default();
            let resolved = sqlx::query(
                "UPDATE full_gaps SET resolved_at = now() WHERE resolved_at IS NULL AND id <= $1 \
                   AND (kind <> 'continuity' OR (caught_up_at IS NOT NULL AND caught_up_at < $2::timestamptz))",
            )
            .bind(pass.upto)
            .bind(&started)
            .execute(&self.pool)
            .await?
            .rows_affected();
            tracing::info!(event = "full_relist_ended", resolved, "a re-list pass ended");
            pass = Pass { last_end: Some(chrono::Utc::now().to_rfc3339()), ..Pass::default() };
            write_pass(&self.pool, &pass).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn stream_ids_name_their_milliseconds() {
        assert_eq!(id_ms("1700000000000-3"), Some(1_700_000_000_000));
        assert_eq!(id_ms("12"), Some(12));
        assert_eq!(id_ms("x-1"), None);
    }
}
