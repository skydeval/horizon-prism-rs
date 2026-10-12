//! Starter-pack joined counts (design-v3 §5.5 "Joined counts"): a job that
//! refreshes `starter_pack_counts` for every stored pack, every
//! `starter_packs.counts_refresh_secs`.
//!
//! - **All time.** The number of distinct accounts whose profile names the
//!   pack in `joinedViaStarterPack`, from Constellation. If Constellation
//!   does not answer, the local count of such `actors` rows.
//! - **Week.** Once a day the all-time value is appended to a history of 8
//!   days. The week count is today's value minus the value of 7 days ago,
//!   when both came from the same source. Otherwise, and until 7 days of
//!   history exist, it is the local count of joiners whose profile was
//!   created within 7 days.
//!
//! A row is written only while its pack exists, so a pack deleted during a
//! run leaves no orphan.

use std::time::Duration;

use chrono::{Days, NaiveDate, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::microcosm::ConstellationClient;

/// `starter_pack_counts_refreshed_total{source}`.
pub const METRIC_REFRESHED: &str = "starter_pack_counts_refreshed_total";
/// Days of all-time history kept.
const HISTORY_DAYS: usize = 8;
const PROFILE: &str = "app.bsky.actor.profile";
const JOINED_PATH: &str = ".joinedViaStarterPack.uri";

/// One day's all-time value and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Day {
    date: NaiveDate,
    source: String,
    all_time: i64,
}

fn parse_history(v: &Value) -> Vec<Day> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| {
            Some(Day {
                date: e.get(0)?.as_str()?.parse().ok()?,
                source: e.get(1)?.as_str()?.to_string(),
                all_time: e.get(2)?.as_i64()?,
            })
        })
        .collect()
}

fn history_json(days: &[Day]) -> Value {
    Value::Array(days.iter().map(|d| json!([d.date.to_string(), d.source, d.all_time])).collect())
}

/// Append today's value if the history has none for today, keeping the
/// newest [`HISTORY_DAYS`] entries. Returns the week count the history
/// supports: today's entry minus the entry of 7 days ago, floored at 0,
/// when both exist and share a source.
fn advance(mut days: Vec<Day>, today: NaiveDate, source: &str, all_time: i64) -> (Vec<Day>, Option<i64>) {
    if !days.iter().any(|d| d.date == today) {
        days.push(Day {
            date: today,
            source: source.to_string(),
            all_time,
        });
    }
    days.sort_by_key(|d| d.date);
    let excess = days.len().saturating_sub(HISTORY_DAYS);
    days.drain(..excess);
    let now = days.iter().find(|d| d.date == today);
    let then = today.checked_sub_days(Days::new(7)).and_then(|week_ago| days.iter().find(|d| d.date == week_ago));
    let week = match (now, then) {
        (Some(now), Some(then)) if now.source == then.source => Some((now.all_time - then.all_time).max(0)),
        _ => None,
    };
    (days, week)
}

/// The job. Cheap to clone.
#[derive(Clone)]
pub struct PackCounts {
    pool: PgPool,
    constellation: Option<ConstellationClient>,
    every: Duration,
}

impl PackCounts {
    #[must_use]
    pub fn new(pool: PgPool, constellation: Option<ConstellationClient>, every: Duration) -> Self {
        Self { pool, constellation, every }
    }

    /// Refresh one pack's row. Does nothing for a pack that is not stored.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refresh(&self, pack: &str) -> Result<(), sqlx::Error> {
        let remote = match &self.constellation {
            Some(c) => c.distinct_dids(pack, PROFILE, JOINED_PATH).await.ok(),
            None => None,
        };
        let (all_time, source) = match remote {
            Some(n) => (i64::try_from(n).unwrap_or(i64::MAX), "constellation"),
            None => {
                let n: i64 = sqlx::query_scalar("SELECT count(*) FROM actors WHERE joined_via_starter_pack_uri = $1")
                    .bind(pack)
                    .fetch_one(&self.pool)
                    .await?;
                (n, "local")
            }
        };
        let stored: Option<Value> = sqlx::query_scalar("SELECT all_time_daily FROM starter_pack_counts WHERE pack_uri = $1")
            .bind(pack)
            .fetch_optional(&self.pool)
            .await?;
        let (days, week) = advance(stored.as_ref().map(parse_history).unwrap_or_default(), Utc::now().date_naive(), source, all_time);
        let week = match week {
            Some(n) => n,
            None => {
                sqlx::query_scalar(
                    "SELECT count(*) FROM actors WHERE joined_via_starter_pack_uri = $1 \
                     AND profile_created_at > now() - interval '7 days'",
                )
                .bind(pack)
                .fetch_one(&self.pool)
                .await?
            }
        };
        let clamp = |n: i64| i32::try_from(n).unwrap_or(i32::MAX);
        sqlx::query(
            "INSERT INTO starter_pack_counts (pack_uri, joined_week_count, joined_all_time_count, all_time_source, all_time_daily, refreshed_at) \
             SELECT $1, $2, $3, $4, $5, now() WHERE EXISTS (SELECT 1 FROM starter_packs WHERE uri = $1) \
             ON CONFLICT (pack_uri) DO UPDATE SET joined_week_count = EXCLUDED.joined_week_count, \
               joined_all_time_count = EXCLUDED.joined_all_time_count, all_time_source = EXCLUDED.all_time_source, \
               all_time_daily = EXCLUDED.all_time_daily, refreshed_at = EXCLUDED.refreshed_at",
        )
        .bind(pack)
        .bind(clamp(week))
        .bind(clamp(all_time))
        .bind(source)
        .bind(history_json(&days))
        .execute(&self.pool)
        .await?;
        metrics::counter!(METRIC_REFRESHED, "source" => source).increment(1);
        Ok(())
    }

    /// Refresh every stored pack, one at a time. Returns how many.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refresh_all(&self) -> Result<usize, sqlx::Error> {
        let packs: Vec<String> = sqlx::query_scalar("SELECT uri FROM starter_packs ORDER BY uri")
            .fetch_all(&self.pool)
            .await?;
        for pack in &packs {
            self.refresh(pack).await?;
        }
        Ok(packs.len())
    }

    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(self.every);
            loop {
                tick.tick().await;
                if let Err(error) = self.refresh_all().await {
                    tracing::warn!(event = "starter_pack_counts_failed", %error, "starter-pack counts not refreshed");
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hydration::testing::exec;
    use crate::test_support::fresh_db;

    fn date(s: &str) -> NaiveDate {
        s.parse().expect("date")
    }

    fn day(d: &str, source: &str, all_time: i64) -> Day {
        Day {
            date: date(d),
            source: source.to_string(),
            all_time,
        }
    }

    #[test]
    fn the_history_gives_a_week_count_only_across_one_source() {
        // No history: nothing to subtract.
        let (days, week) = advance(Vec::new(), date("2026-10-12"), "constellation", 50);
        assert_eq!((days.len(), week), (1, None));
        // A second run the same day appends nothing and keeps the first value.
        let (days, _) = advance(days, date("2026-10-12"), "constellation", 55);
        assert_eq!(days, [day("2026-10-12", "constellation", 50)]);

        // Seven days back, same source.
        let week_of = |source: &str, then: i64, now: i64| {
            let history = vec![day("2026-10-05", source, then)];
            advance(history, date("2026-10-12"), "constellation", now).1
        };
        assert_eq!(week_of("constellation", 40, 50), Some(10));
        assert_eq!(week_of("constellation", 60, 50), Some(0), "never negative");
        assert_eq!(week_of("local", 40, 50), None, "sources differ");
        // A gap at exactly seven days back: no week count.
        let history = vec![day("2026-10-04", "constellation", 40), day("2026-10-06", "constellation", 45)];
        assert_eq!(advance(history, date("2026-10-12"), "constellation", 50).1, None);

        // Eight entries are kept.
        let mut days = Vec::new();
        for d in 1..=12 {
            days = advance(days, date(&format!("2026-10-{d:02}")), "local", i64::from(d)).0;
        }
        assert_eq!(days.len(), 8);
        assert_eq!((days[0].date, days[7].date), (date("2026-10-05"), date("2026-10-12")));
        assert_eq!(parse_history(&history_json(&days)), days);
        assert!(parse_history(&json!("nonsense")).is_empty());
    }

    #[tokio::test]
    async fn local_counts_and_no_orphans() {
        let Some(db) = fresh_db("pack_counts").await else {
            return;
        };
        let p = &db.pool;
        let owner = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let pack = format!("at://{owner}/app.bsky.graph.starterpack/p1");
        exec(p, &format!(
            "INSERT INTO starter_packs (uri, creator_did, rkey, cid, rev, name, list_uri, record, created_at) VALUES \
             ('{pack}', '{owner}', 'p1', 'bafy', '3k', 'Pack', 'at://{owner}/app.bsky.graph.list/l', '{{}}', now())"
        )).await;
        for (i, age_days) in [1, 3, 30].into_iter().enumerate() {
            exec(p, &format!(
                "INSERT INTO actors (did, handle, joined_via_starter_pack_uri, profile_created_at) VALUES \
                 ('did:plc:joiner{i}', 'j{i}.test', '{pack}', now() - interval '{age_days} days')"
            )).await;
        }
        let job = PackCounts::new(p.clone(), None, Duration::from_secs(3600));
        assert_eq!(job.refresh_all().await.expect("refresh"), 1);
        let row: (i32, i32, String) =
            sqlx::query_as("SELECT joined_week_count, joined_all_time_count, all_time_source FROM starter_pack_counts WHERE pack_uri = $1")
                .bind(&pack)
                .fetch_one(p)
                .await
                .expect("row");
        assert_eq!(row, (2, 3, "local".to_string()));
        // A second run updates in place.
        job.refresh(&pack).await.expect("again");
        // A pack that is not stored gets no row.
        job.refresh("at://did:plc:zzzzzzzzzzzzzzzzzzzzzzzz/app.bsky.graph.starterpack/gone").await.expect("no-op");
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM starter_pack_counts").fetch_one(p).await.expect("count");
        assert_eq!(rows, 1);
        db.drop_db().await;
    }
}
