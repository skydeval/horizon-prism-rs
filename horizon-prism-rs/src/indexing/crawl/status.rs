//! What status and Health show of the crawl: its progress in memory, the
//! directory export's state, and figures read from the database every few
//! minutes by the crawl task (a count over every crawl row is too heavy for
//! each Health reload at the network's size).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;

use super::plc_export::ExportState;
use super::task::Progress;

/// How often the crawl task reads its figures again.
pub const FIGURES_EVERY: Duration = Duration::from_secs(300);

/// What the crawl shares with status. Cheap to clone.
#[derive(Clone, Default)]
pub struct CrawlBoard {
    pub(super) progress: Arc<Mutex<Progress>>,
    pub(super) export: Arc<ExportState>,
    pub(super) figures: Arc<Mutex<Value>>,
}

impl CrawlBoard {
    /// The crawl's progress and figures, as status shows them.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let p = self.progress.lock().map(|p| p.clone()).unwrap_or_default();
        let figures = self.figures.lock().map(|f| f.clone()).unwrap_or(Value::Null);
        json!({
            "running": p.running,
            "paused": p.paused,
            "waiting_for_live": p.waiting_for_live,
            "history": p.history,
            "crawled": p.crawled,
            "failed": p.failed,
            "last_error": p.last_error,
            "export": { "active": self.export.active(), "set_aside": self.export.set_aside() },
            "figures": figures,
        })
    }
}

/// The share of the last seven days during which a blocking gap was open,
/// from the gap rows that overlap it (rows that overlap each other count
/// twice, so it is capped at the whole).
const BLOCKED_SQL: &str = "SELECT COALESCE(sum(extract(epoch FROM LEAST(COALESCE(resolved_at, now()), now()) \
       - GREATEST(opened_at, now() - interval '7 days'))), 0)::float8 / (7 * 86400) \
     FROM full_gaps WHERE blocking AND COALESCE(resolved_at, now()) > now() - interval '7 days'";

/// The crawl's figures at `crawl_generation`: rows by state, the most
/// common reasons of failed rows, accounts kept incomplete by hand per
/// group, `uncovered` accounts per Aside relay, the open gaps, the share of
/// the last week local serving was blocked, and the export's cursor.
///
/// # Errors
///
/// Database errors.
pub async fn figures(pool: &PgPool, crawl_generation: i64) -> Result<Value, sqlx::Error> {
    let states: Vec<(String, i64)> = sqlx::query_as("SELECT state, count(*) FROM full_crawl WHERE generation = $1 GROUP BY state ORDER BY state")
        .bind(crawl_generation)
        .fetch_all(pool)
        .await?;
    let older: i64 = sqlx::query_scalar("SELECT count(*) FROM full_crawl WHERE generation <> $1").bind(crawl_generation).fetch_one(pool).await?;
    let failed: Vec<(Option<String>, i64)> =
        sqlx::query_as("SELECT reason, count(*) FROM full_crawl WHERE generation = $1 AND state = 'failed' GROUP BY reason ORDER BY 2 DESC, 1 LIMIT 5")
            .bind(crawl_generation)
            .fetch_all(pool)
            .await?;
    let incomplete: Vec<(String, i64)> = sqlx::query_as("SELECT grp, count(*) FROM full_incomplete GROUP BY grp ORDER BY grp").fetch_all(pool).await?;
    let uncovered: Vec<(Option<String>, i64)> =
        sqlx::query_as("SELECT aside_relay, count(*) FROM full_crawl WHERE uncovered GROUP BY aside_relay ORDER BY 1").fetch_all(pool).await?;
    let gaps: Vec<(String, Option<String>, bool, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as("SELECT kind, relay, blocking, opened_at FROM full_gaps WHERE resolved_at IS NULL ORDER BY opened_at LIMIT 20").fetch_all(pool).await?;
    let blocked: f64 = sqlx::query_scalar(BLOCKED_SQL).fetch_one(pool).await?;
    let export: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(super::plc_export::EXPORT_KEY).fetch_optional(pool).await?;
    let relist: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(super::RELIST_KEY).fetch_optional(pool).await?;
    Ok(json!({
        "at": crate::indexing::public_mode::rfc3339(&chrono::Utc::now()),
        "states": states.into_iter().map(|(s, n)| (s, json!(n))).collect::<serde_json::Map<String, Value>>(),
        "older_generation": older,
        "failed_reasons": failed.into_iter().map(|(r, n)| json!({ "reason": r, "accounts": n })).collect::<Vec<_>>(),
        "incomplete": incomplete.into_iter().map(|(g, n)| (g, json!(n))).collect::<serde_json::Map<String, Value>>(),
        "uncovered": uncovered.into_iter().map(|(r, n)| json!({ "relay": r, "accounts": n })).collect::<Vec<_>>(),
        "open_gaps": gaps
            .into_iter()
            .map(|(kind, relay, blocking, at)| json!({ "kind": kind, "relay": relay, "blocking": blocking, "opened_at": crate::indexing::public_mode::rfc3339(&at) }))
            .collect::<Vec<_>>(),
        "blocked_share_7d": blocked.clamp(0.0, 1.0),
        "relist_running": relist.is_some(),
        "export": export,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The figures name rows by state, failure reasons, incomplete and
    /// uncovered accounts, open gaps and the blocked share of the week.
    #[tokio::test]
    async fn the_crawl_figures_count_what_status_shows() {
        let Some(db) = crate::test_support::fresh_db("crawl_status_figures").await else {
            return;
        };
        let p = &db.pool;
        sqlx::raw_sql(
            "INSERT INTO dids (did) VALUES ('did:plc:example1'), ('did:plc:example2'), ('did:plc:example3'), ('did:plc:example4'); \
             INSERT INTO full_crawl (account, generation, state, reason, uncovered, aside_relay) \
               SELECT id, 2, s, r, u, a FROM (VALUES ('did:plc:example1', 'done', NULL, false, NULL), ('did:plc:example2', 'failed', 'bad_signature', false, NULL), \
                 ('did:plc:example3', 'pending', NULL, true, 'other'), ('did:plc:example4', 'done', NULL, false, NULL)) v(d, s, r, u, a) \
               JOIN dids ON did = d; \
             UPDATE full_crawl SET generation = 1 WHERE account = (SELECT id FROM dids WHERE did = 'did:plc:example4'); \
             INSERT INTO full_incomplete (grp, account) SELECT 'likes', id FROM dids WHERE did = 'did:plc:example2'; \
             INSERT INTO full_gaps (kind, relay, blocking, opened_at) VALUES ('trim', 'bsky', true, now() - interval '1 day'), ('periodic', NULL, false, now());",
        )
        .execute(p)
        .await
        .expect("seed");
        let f = figures(p, 2).await.expect("figures");
        assert_eq!(f["states"], json!({"done": 1, "failed": 1, "pending": 1}));
        assert_eq!(f["older_generation"], json!(1));
        assert_eq!(f["failed_reasons"], json!([{"reason": "bad_signature", "accounts": 1}]));
        assert_eq!(f["incomplete"], json!({"likes": 1}));
        assert_eq!(f["uncovered"], json!([{"relay": "other", "accounts": 1}]));
        assert_eq!(f["open_gaps"].as_array().map(Vec::len), Some(2));
        let blocked = f["blocked_share_7d"].as_f64().expect("share");
        assert!((blocked - 1.0 / 7.0).abs() < 0.01, "{blocked}");
        db.drop_db().await;
    }
}
