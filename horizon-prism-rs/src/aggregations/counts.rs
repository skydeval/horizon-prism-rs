//! Constellation count enrichment (design-v2 §9.6, §7.6).
//!
//! - Post targets use one `/links/all` call; follower counts use
//!   `/links/count/distinct-dids` on `app.bsky.graph.follow` `.subject`.
//! - List views never call synchronously: they read `constellation_counts`
//!   ([`cached`]) and enqueue the target on the Redis set `cc:refresh`
//!   ([`enqueue_refresh`]), capped at `counts.refresh_queue_max`.
//! - The count refresher drains `cc:refresh` at `counts.refresh_rate_per_sec`
//!   (20/s) and writes `constellation_counts`.
//! - Fresh for `fresh_secs` (300), usable stale up to `stale_max_secs`
//!   (86 400); staleness is exported as `constellation_count_age_seconds`.
//! - Any Redis error on `cc:refresh` is a no-op, counted in
//!   `cache_errors_total{op}`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::config::CountsConfig;
use crate::microcosm::ConstellationClient;
use crate::microcosm::constellation::AllLinks;

/// The request-fed refresh set (§7.6).
pub const REFRESH_KEY: &str = "cc:refresh";
pub const METRIC_AGE: &str = "constellation_count_age_seconds";
pub const METRIC_DROPPED: &str = "cc_refresh_dropped_total";
pub const METRIC_CACHE_ERRORS: &str = "cache_errors_total";
/// Poll interval when the refresh set is empty.
pub const IDLE_POLL: Duration = Duration::from_secs(1);

/// Counts for one target. Post targets fill the first four; DID targets fill
/// `followers`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub likes: Option<i32>,
    pub reposts: Option<i32>,
    pub replies: Option<i32>,
    pub quotes: Option<i32>,
    pub followers: Option<i32>,
}

/// Freshness of a cached row (§9.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    /// Usable, and worth a refresh.
    Stale,
    /// Too old: use local counts.
    Expired,
}

/// Classify an age against the configured bounds.
#[must_use]
pub fn freshness(age_secs: i64, cfg: &CountsConfig) -> Freshness {
    let age = u64::try_from(age_secs).unwrap_or(0);
    if age <= cfg.fresh_secs {
        Freshness::Fresh
    } else if age <= cfg.stale_max_secs {
        Freshness::Stale
    } else {
        Freshness::Expired
    }
}

fn clamp(n: u64) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// The §9.6 mapping from `/links/all` to post counts.
#[must_use]
pub fn post_counts(all: &AllLinks) -> Counts {
    let distinct = |c: &str, p: &str| all.get(c, p).map_or(0, |v| v.distinct_dids);
    let records = |c: &str, p: &str| all.get(c, p).map_or(0, |v| v.records);
    Counts {
        likes: Some(clamp(distinct("app.bsky.feed.like", ".subject.uri"))),
        reposts: Some(clamp(distinct("app.bsky.feed.repost", ".subject.uri"))),
        replies: Some(clamp(records("app.bsky.feed.post", ".reply.parent.uri"))),
        quotes: Some(clamp(
            records("app.bsky.feed.post", ".embed.record.uri") + records("app.bsky.feed.post", ".embed.record.record.uri"),
        )),
        followers: None,
    }
}

/// Fetch counts for a post URI or a DID.
///
/// # Errors
///
/// A Constellation failure, as text.
pub async fn fetch(client: &ConstellationClient, target: &str) -> Result<Counts, String> {
    if target.starts_with("did:") {
        let n = client
            .distinct_dids(target, "app.bsky.graph.follow", ".subject")
            .await
            .map_err(|e| e.to_string())?;
        return Ok(Counts {
            followers: Some(clamp(n)),
            ..Counts::default()
        });
    }
    if target.starts_with("at://") {
        let all = client.all(target).await.map_err(|e| e.to_string())?;
        return Ok(post_counts(&all));
    }
    Err(format!("not a count target: {target:?}"))
}

/// Upsert a fetched row.
///
/// # Errors
///
/// Database errors.
pub async fn store(pool: &PgPool, target: &str, c: &Counts) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO constellation_counts (target, likes, reposts, replies, quotes, followers, fetched_at) \
         VALUES ($1, $2, $3, $4, $5, $6, now()) \
         ON CONFLICT (target) DO UPDATE SET likes = EXCLUDED.likes, reposts = EXCLUDED.reposts, \
           replies = EXCLUDED.replies, quotes = EXCLUDED.quotes, followers = EXCLUDED.followers, fetched_at = now()",
    )
    .bind(target)
    .bind(c.likes)
    .bind(c.reposts)
    .bind(c.replies)
    .bind(c.quotes)
    .bind(c.followers)
    .execute(pool)
    .await?;
    Ok(())
}

/// A cached row with its freshness; `None` when absent or expired. Records
/// the row's age in `constellation_count_age_seconds`.
///
/// # Errors
///
/// Database errors.
pub async fn cached(pool: &PgPool, target: &str, cfg: &CountsConfig) -> Result<Option<(Counts, Freshness)>, sqlx::Error> {
    type Row = (Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>, DateTime<Utc>, DateTime<Utc>);
    let row: Option<Row> = sqlx::query_as(
        "SELECT likes, reposts, replies, quotes, followers, fetched_at, now() FROM constellation_counts WHERE target = $1",
    )
    .bind(target)
    .fetch_optional(pool)
    .await?;
    let Some((likes, reposts, replies, quotes, followers, fetched_at, now)) = row else {
        return Ok(None);
    };
    let age = (now - fetched_at).num_seconds();
    #[allow(clippy::cast_precision_loss)]
    metrics::histogram!(METRIC_AGE).record(age.max(0) as f64);
    let f = freshness(age, cfg);
    if f == Freshness::Expired {
        return Ok(None);
    }
    Ok(Some((
        Counts {
            likes,
            reposts,
            replies,
            quotes,
            followers,
        },
        f,
    )))
}

/// Enqueue a target for background refresh, if the set is below its cap.
/// Returns whether it was enqueued; Redis errors are a counted no-op.
pub async fn enqueue_refresh(redis: &mut ConnectionManager, target: &str, max: u64) -> bool {
    let size: redis::RedisResult<u64> = redis.scard(REFRESH_KEY).await;
    match size {
        Ok(n) if n >= max => {
            metrics::counter!(METRIC_DROPPED).increment(1);
            false
        }
        Ok(_) => match redis.sadd::<_, _, i64>(REFRESH_KEY, target).await {
            Ok(_) => true,
            Err(_) => {
                metrics::counter!(METRIC_CACHE_ERRORS, "op" => "cc_sadd").increment(1);
                false
            }
        },
        Err(_) => {
            metrics::counter!(METRIC_CACHE_ERRORS, "op" => "cc_scard").increment(1);
            false
        }
    }
}

/// The count refresher task.
pub struct CountRefresher {
    pool: PgPool,
    redis: ConnectionManager,
    client: ConstellationClient,
    rate_per_sec: u32,
}

impl CountRefresher {
    #[must_use]
    pub fn new(pool: PgPool, redis: ConnectionManager, client: ConstellationClient, cfg: &CountsConfig) -> Self {
        Self {
            pool,
            redis,
            client,
            rate_per_sec: cfg.refresh_rate_per_sec.max(1),
        }
    }

    /// Pop and refresh one target. Returns `false` when the set was empty
    /// (or Redis failed).
    pub async fn refresh_one(&mut self) -> bool {
        let popped: redis::RedisResult<Option<String>> = self.redis.spop(REFRESH_KEY).await;
        let target = match popped {
            Ok(Some(t)) => t,
            Ok(None) => return false,
            Err(_) => {
                metrics::counter!(METRIC_CACHE_ERRORS, "op" => "cc_spop").increment(1);
                return false;
            }
        };
        match fetch(&self.client, &target).await {
            Ok(counts) => {
                if let Err(error) = store(&self.pool, &target, &counts).await {
                    tracing::warn!(event = "count_refresh_store_failed", %target, %error, "storing counts failed");
                }
            }
            Err(error) => tracing::debug!(event = "count_refresh_failed", %target, %error, "constellation count fetch failed"),
        }
        true
    }

    /// Drain `cc:refresh` at the configured rate.
    #[must_use]
    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1) / self.rate_per_sec);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if !self.refresh_one().await {
                    tokio::time::sleep(IDLE_POLL).await;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_and_freshness() {
        let all: AllLinks = serde_json::from_str(
            r#"{"links":{
                "app.bsky.feed.like":{".subject.uri":{"records":7,"distinct_dids":5}},
                "app.bsky.feed.repost":{".subject.uri":{"records":2,"distinct_dids":2}},
                "app.bsky.feed.post":{
                    ".reply.parent.uri":{"records":3,"distinct_dids":1},
                    ".embed.record.uri":{"records":1,"distinct_dids":1},
                    ".embed.record.record.uri":{"records":2,"distinct_dids":2}}}}"#,
        )
        .expect("json");
        assert_eq!(
            post_counts(&all),
            Counts {
                likes: Some(5),
                reposts: Some(2),
                replies: Some(3),
                quotes: Some(3),
                followers: None
            }
        );
        let cfg = CountsConfig::default();
        assert_eq!(freshness(10, &cfg), Freshness::Fresh);
        assert_eq!(freshness(301, &cfg), Freshness::Stale);
        assert_eq!(freshness(86_401, &cfg), Freshness::Expired);
    }

    mod live {
        use super::*;
        use crate::test_support::fresh_db;

        #[tokio::test]
        async fn store_and_read_back() {
            let Some(db) = fresh_db("constellation_counts_roundtrip").await else {
                return;
            };
            let c = Counts {
                followers: Some(12),
                ..Counts::default()
            };
            store(&db.pool, "did:plc:a", &c).await.expect("store");
            let cfg = CountsConfig::default();
            assert_eq!(cached(&db.pool, "did:plc:a", &cfg).await.expect("read"), Some((c, Freshness::Fresh)));
            sqlx::query("UPDATE constellation_counts SET fetched_at = now() - interval '2 days'")
                .execute(&db.pool)
                .await
                .expect("age");
            assert_eq!(cached(&db.pool, "did:plc:a", &cfg).await.expect("read"), None);
            db.drop_db().await;
        }

        #[tokio::test]
        async fn refresh_queue_is_capped() {
            let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
                return;
            };
            let client = redis::Client::open(url).expect("url");
            let mut conn = ConnectionManager::new(client).await.expect("redis");
            let _: () = redis::cmd("DEL").arg(REFRESH_KEY).query_async(&mut conn).await.expect("del");
            assert!(enqueue_refresh(&mut conn, "at://a", 2).await);
            assert!(enqueue_refresh(&mut conn, "at://b", 2).await);
            assert!(!enqueue_refresh(&mut conn, "at://c", 2).await);
            let n: u64 = conn.scard(REFRESH_KEY).await.expect("scard");
            assert_eq!(n, 2);
            let _: () = redis::cmd("DEL").arg(REFRESH_KEY).query_async(&mut conn).await.expect("del");
        }
    }
}
