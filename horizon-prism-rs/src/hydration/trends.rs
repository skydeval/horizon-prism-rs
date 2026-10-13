//! Trending topics and trends (design-v3 §7.4, §7.5): hashtags, ranked
//! from the posts and the engagement this AppView stores. Nothing is made
//! up: no fixed topics, no generated descriptions.
//!
//! Both lists are deployment-wide snapshots, computed again when older
//! than `trends.cache_ttl_secs`.
//!
//! - **Topics** are tags by post count over `trends.topics_window_hours`.
//!   The hourly aggregate only picks candidates; the count that ranks is
//!   taken again from `posts`.
//! - **Trends** are tags by engagement velocity over `trends.window_hours`:
//!   likes, reposts and replies, each weighted by how recently it was
//!   made (half-life `trends.decay_half_life_hours`). Engagement counts
//!   only on a post made within `trends.subject_lookback_hours` before it,
//!   so an old post that suddenly gets likes does not make its tag trend.
//!
//! In both, only posts by authors the suggestion pool's conditions allow
//! (activity aside) count, and a tag needs `trends.min_posts` posts by
//! `trends.min_authors` distinct authors, so one account cannot carry a
//! tag.
//!
//! The trend refresh runs under `trends.refresh_timeout_ms`. If that
//! passes, the previous snapshot keeps being served.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::search::{set_timeout, timed_out};
use super::suggestions::{computed_at, eligible, rec_id};
use super::{Hydrator, ViewResult, Viewer, ts};
use crate::xrpc::pools::Requester;

/// `trends_refresh_timeout_total`: trend refreshes stopped by their
/// statement timeout.
pub const METRIC_REFRESH_TIMEOUT: &str = "trends_refresh_timeout_total";

/// Accounts one trend names.
const TREND_ACTORS: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Topic {
    tag: String,
}

#[derive(Debug, Clone, PartialEq)]
struct Trend {
    tag: String,
    posts: i64,
    started_at: DateTime<Utc>,
    hot: bool,
    /// The authors whose posts with the tag drew the most engagement.
    actors: Vec<String>,
}

struct Snapshot<T> {
    items: Arc<Vec<T>>,
    rec_id: String,
    at: Option<Instant>,
}

impl<T> Default for Snapshot<T> {
    fn default() -> Self {
        Self { items: Arc::default(), rec_id: String::new(), at: None }
    }
}

/// The trend state one hydrator keeps.
#[derive(Default)]
pub struct State {
    topics: tokio::sync::Mutex<Snapshot<Topic>>,
    trends: tokio::sync::Mutex<Snapshot<Trend>>,
}

/// Engagement in one hour on one author's posts with one tag: the tag, the
/// author, the hour and the count.
type Engaged = (String, String, DateTime<Utc>, i64);

/// One tag's recomputed hourly engagement, and what follows from it.
#[derive(Debug, Default)]
struct Series {
    by_hour: BTreeMap<DateTime<Utc>, i64>,
    by_author: HashMap<String, i64>,
}

/// What ranks and describes a trend, from its hourly series.
#[derive(Debug, PartialEq)]
struct Measured {
    score: f64,
    started_at: DateTime<Utc>,
    hot: bool,
}

/// Measure one series as of `now`. `None` when it holds no engagement.
fn measure(series: &BTreeMap<DateTime<Utc>, i64>, now: DateTime<Utc>, window_hours: u32, half_life_hours: f64, started_ratio: f64, hot_ratio: f64) -> Option<Measured> {
    let total: i64 = series.values().sum();
    let first = series.iter().find(|(_, n)| **n > 0).map(|(hour, _)| *hour)?;
    #[allow(clippy::cast_precision_loss)]
    let count = |n: i64| n as f64;
    let mean = count(total) / f64::from(window_hours.max(1));
    let score = series
        .iter()
        .map(|(hour, n)| {
            let age_hours = count((now - *hour).num_minutes().max(0)) / 60.0;
            count(*n) * (-age_hours / half_life_hours.max(0.1)).exp2()
        })
        .sum();
    // Started: the first hour at `started_ratio` times the tag's own
    // hourly mean, else the first hour with any activity.
    let started_at = series.iter().find(|(_, n)| count(**n) >= started_ratio * mean && **n > 0).map_or(first, |(hour, _)| *hour);
    let this_hour = now.duration_trunc(TimeDelta::hours(1)).unwrap_or(now);
    let last_full = series.get(&(this_hour - TimeDelta::hours(1))).copied().unwrap_or(0);
    Some(Measured { score, started_at, hot: last_full > 0 && count(last_full) >= hot_ratio * mean })
}

impl Hydrator {
    /// `{topic, displayName, link, description?}` for a tag.
    fn topic_view(&self, tag: &str) -> Value {
        let cfg = &self.inner.cfg.trends;
        let encoded: String = url::form_urlencoded::byte_serialize(tag.as_bytes()).collect::<String>().replace('+', "%20");
        let mut view = json!({
            "topic": format!("#{tag}"),
            "displayName": format!("#{tag}"),
            "link": cfg.topic_link_template.replace("{tag}", &encoded),
        });
        if let Some(description) = cfg.descriptions.get(tag) {
            view["description"] = json!(description);
        }
        view
    }

    /// Whether the tag index exists yet (design-v3 §12.2). Until it does
    /// the refreshes are skipped: without it each would scan every post.
    async fn tags_indexed(&self) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT to_regclass('posts_tags') IS NOT NULL").fetch_one(self.pool()).await
    }

    async fn topics_snapshot(&self) -> Result<Arc<Vec<Topic>>, sqlx::Error> {
        let cfg = &self.inner.cfg.trends;
        let mut slot = self.inner.trends.topics.lock().await;
        if slot.at.is_none_or(|at| at.elapsed() >= Duration::from_secs(cfg.cache_ttl_secs)) && self.tags_indexed().await? {
            let hours = i32::try_from(cfg.topics_window_hours).unwrap_or(i32::MAX);
            let candidates: Vec<String> = sqlx::query_scalar(
                "SELECT tag FROM tag_activity_hourly WHERE hour >= now() - make_interval(hours => $1) \
                 GROUP BY tag ORDER BY sum(GREATEST(posts, 0)) DESC, tag LIMIT $2",
            )
            .bind(hours)
            .bind(i64::from(cfg.candidate_count))
            .fetch_all(self.pool())
            .await?;
            // The count that ranks, from the posts themselves.
            let sql = format!(
                "SELECT t.tag FROM unnest($1::text[]) AS t(tag) \
                 JOIN posts p ON p.tags @> ARRAY[t.tag] AND p.created_at >= now() - make_interval(hours => $2) \
                 JOIN actors a ON a.did = p.author_did WHERE {} \
                 GROUP BY t.tag HAVING count(*) >= $3 AND count(DISTINCT p.author_did) >= $4 \
                 ORDER BY count(*) DESC, t.tag",
                eligible(None)
            );
            let tags: Vec<String> = sqlx::query_scalar(&sql)
                .bind(&candidates)
                .bind(hours)
                .bind(i64::from(cfg.min_posts))
                .bind(i64::from(cfg.min_authors))
                .fetch_all(self.pool())
                .await?;
            slot.items = Arc::new(tags.into_iter().map(|tag| Topic { tag }).collect());
            slot.at = Some(Instant::now());
        }
        Ok(Arc::clone(&slot.items))
    }

    /// `app.bsky.unspecced.getTrendingTopics` (design-v3 §7.4). `suggested`
    /// is the operator's own list. The `viewer` parameter changes nothing:
    /// topics are deployment-wide.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn trending_topics(&self, limit: u32) -> ViewResult<Value> {
        let topics = self.topics_snapshot().await?;
        let limit = usize::try_from(limit.clamp(1, 25)).unwrap_or(10);
        let views: Vec<Value> = topics.iter().take(limit).map(|t| self.topic_view(&t.tag)).collect();
        let suggested: Vec<Value> = self
            .inner
            .cfg
            .trends
            .suggested_topics
            .iter()
            .map(|s| {
                let mut view = json!({ "topic": s.topic, "link": s.link });
                if let Some(name) = &s.display_name {
                    view["displayName"] = json!(name);
                }
                if let Some(description) = &s.description {
                    view["description"] = json!(description);
                }
                view
            })
            .collect();
        Ok(Ok(Self::emit(json!({ "topics": views, "suggested": suggested }))))
    }

    /// Compute the trend snapshot. `Ok(None)` when the refresh timed out.
    async fn compute_trends(&self) -> Result<Option<Vec<Trend>>, sqlx::Error> {
        let cfg = &self.inner.cfg.trends;
        let window = i32::try_from(cfg.window_hours).unwrap_or(i32::MAX);
        let lookback = i32::try_from(cfg.subject_lookback_hours).unwrap_or(i32::MAX);
        // The aggregate only picks candidates.
        let candidates: Vec<String> = sqlx::query_scalar(
            "SELECT tag FROM tag_activity_hourly WHERE hour >= now() - make_interval(hours => $1) \
             GROUP BY tag \
             ORDER BY sum((GREATEST(likes, 0) + GREATEST(reposts, 0) + GREATEST(replies, 0)) \
                          * power(2, -extract(epoch FROM now() - hour) / 3600.0 / $2)) DESC, tag \
             LIMIT $3",
        )
        .bind(window)
        .bind(cfg.decay_half_life_hours.0.max(0.1))
        .bind(i64::from(cfg.candidate_count))
        .fetch_all(self.pool())
        .await?;
        if candidates.is_empty() {
            return Ok(Some(Vec::new()));
        }
        // The posts that carry a candidate tag, by authors that count.
        let subjects = format!(
            "SELECT t.tag, p.uri, p.author_did, p.created_at FROM unnest($1::text[]) AS t(tag) \
             JOIN posts p ON p.tags @> ARRAY[t.tag] AND p.created_at >= now() - make_interval(hours => $2 + $3) \
             JOIN actors a ON a.did = p.author_did WHERE {}",
            eligible(None)
        );
        let counted = format!(
            "WITH subj AS ({subjects}) SELECT tag, count(*), count(DISTINCT author_did) FROM subj \
             WHERE created_at >= now() - make_interval(hours => $2) GROUP BY tag"
        );
        // Engagement made in the window, on a post made at most the
        // lookback before it.
        let engagement = |table: &str, subject: &str| {
            format!(
                "SELECT s.tag, s.author_did, date_trunc('hour', e.created_at, 'UTC') AS hour FROM subj s \
                 JOIN {table} e ON e.{subject} = s.uri \
                 WHERE e.created_at >= now() - make_interval(hours => $2) \
                   AND e.created_at <= s.created_at + make_interval(hours => $3)"
            )
        };
        let series = format!(
            "WITH subj AS ({subjects}), ev AS ({} UNION ALL {} UNION ALL {}) \
             SELECT tag, author_did, hour, count(*) FROM ev GROUP BY tag, author_did, hour",
            engagement("likes", "subject_uri"),
            engagement("reposts", "subject_uri"),
            engagement("posts", "reply_parent_uri"),
        );
        let mut tx = self.pool().begin().await?;
        set_timeout(&mut tx, cfg.refresh_timeout_ms).await?;
        let counts: Result<Vec<(String, i64, i64)>, sqlx::Error> =
            sqlx::query_as(&counted).bind(&candidates).bind(window).bind(lookback).fetch_all(&mut *tx).await;
        let counts = match counts {
            Ok(rows) => rows,
            Err(error) if timed_out(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let rows: Result<Vec<Engaged>, sqlx::Error> =
            sqlx::query_as(&series).bind(&candidates).bind(window).bind(lookback).fetch_all(&mut *tx).await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) if timed_out(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        tx.commit().await?;

        let mut by_tag: HashMap<String, Series> = HashMap::new();
        for (tag, author, hour, n) in rows {
            let series = by_tag.entry(tag).or_default();
            *series.by_hour.entry(hour).or_default() += n;
            *series.by_author.entry(author).or_default() += n;
        }
        let now = Utc::now();
        let mut ranked: Vec<(f64, Trend)> = Vec::new();
        for (tag, posts, authors) in counts {
            if posts < i64::from(cfg.min_posts) || authors < i64::from(cfg.min_authors) {
                continue;
            }
            let Some(series) = by_tag.get(&tag) else { continue };
            let Some(m) = measure(&series.by_hour, now, cfg.window_hours, cfg.decay_half_life_hours.0, cfg.started_ratio.0, cfg.hot_ratio.0) else {
                continue;
            };
            let mut authors: Vec<(&String, &i64)> = series.by_author.iter().collect();
            authors.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
            let actors = authors.into_iter().take(TREND_ACTORS).map(|(did, _)| did.clone()).collect();
            ranked.push((m.score, Trend { tag, posts, started_at: m.started_at, hot: m.hot, actors }));
        }
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.tag.cmp(&b.1.tag)));
        Ok(Some(ranked.into_iter().map(|(_, trend)| trend).collect()))
    }

    async fn trends_snapshot(&self) -> Result<(Arc<Vec<Trend>>, String), sqlx::Error> {
        let cfg = &self.inner.cfg.trends;
        let mut slot = self.inner.trends.trends.lock().await;
        if slot.at.is_none_or(|at| at.elapsed() >= Duration::from_secs(cfg.cache_ttl_secs)) && self.tags_indexed().await? {
            match self.compute_trends().await? {
                Some(trends) => {
                    let topics: Vec<&[u8]> = trends.iter().map(|t| t.tag.as_bytes()).collect();
                    let mut parts: Vec<&[u8]> = Vec::with_capacity(topics.len() + 1);
                    let at = computed_at();
                    parts.push(&at);
                    parts.extend(topics);
                    slot.rec_id = rec_id("trends/v1", &parts);
                    slot.items = Arc::new(trends);
                }
                // The previous snapshot stands.
                None => metrics::counter!(METRIC_REFRESH_TIMEOUT).increment(1),
            }
            slot.at = Some(Instant::now());
        }
        Ok((Arc::clone(&slot.items), slot.rec_id.clone()))
    }

    /// `app.bsky.unspecced.getTrends` (design-v3 §7.5). A trend's `actors`
    /// are shown after the viewer's own blocks and mutes; `category` is
    /// never emitted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn trends(&self, viewer: &Viewer, req: &Requester, limit: u32) -> ViewResult<Value> {
        let (trends, rec_id) = self.trends_snapshot().await?;
        let limit = usize::try_from(limit.clamp(1, 25)).unwrap_or(10);
        let page: Vec<&Trend> = trends.iter().take(limit).collect();
        let mut dids: Vec<String> = page.iter().flat_map(|t| t.actors.iter().cloned()).collect();
        dids.sort();
        dids.dedup();
        let (visible, ctx) = self.searchable(viewer, req, &dids).await?;
        let visible: HashSet<&String> = visible.iter().collect();
        let views: Vec<Value> = page
            .iter()
            .map(|t| {
                let mut view = self.topic_view(&t.tag);
                view["startedAt"] = json!(ts(t.started_at));
                view["postCount"] = json!(t.posts);
                view["actors"] = t.actors.iter().filter(|d| visible.contains(d)).map(|d| ctx.view(d, ProfileKind::Basic)).collect();
                if t.hot {
                    view["status"] = json!("hot");
                }
                view
            })
            .collect();
        let mut out = json!({ "trends": views });
        if !rec_id.is_empty() {
            out["recIdStr"] = json!(rec_id);
        }
        Ok(Ok(Self::emit(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::test_support::fresh_db;

    #[test]
    fn a_series_is_scored_by_recency_and_marked_by_its_own_mean() {
        let now = DateTime::parse_from_rfc3339("2026-10-12T12:30:00Z").expect("now").with_timezone(&Utc);
        let hour = |h: i64| now.duration_trunc(TimeDelta::hours(1)).expect("hour") - TimeDelta::hours(h);
        assert_eq!(measure(&BTreeMap::new(), now, 24, 6.0, 2.0, 2.0), None);
        // 24 engagements in the window: a mean of one an hour. The burst
        // of the last full hour is where it started, and it is hot.
        let burst = BTreeMap::from([(hour(20), 1), (hour(1), 23)]);
        let m = measure(&burst, now, 24, 6.0, 2.0, 2.0).expect("measured");
        assert_eq!((m.started_at, m.hot), (hour(1), true));
        // The same count long ago scores far less, and is not hot.
        let old = BTreeMap::from([(hour(20), 23), (hour(1), 1)]);
        let o = measure(&old, now, 24, 6.0, 2.0, 2.0).expect("measured");
        assert!(m.score > 5.0 * o.score && !o.hot && o.started_at == hour(20));
        // Never reaching the ratio: it started with its first activity.
        let flat = BTreeMap::from([(hour(3), 1), (hour(2), 1)]);
        assert_eq!(measure(&flat, now, 1, 6.0, 2.0, 2.0).expect("measured").started_at, hour(3));
    }

    /// B-45 / B-46 (design-v3 §7.4, §7.5): a tag needs enough posts by
    /// enough authors that count; topics rank by posts and trends by
    /// engagement; one account cannot carry a tag.
    #[tokio::test]
    async fn tags_trend_by_posts_and_by_engagement() {
        let Some(db) = fresh_db("hydration_trends").await else {
            return;
        };
        let p = &db.pool;
        let did = |c: char| format!("did:plc:{}", c.to_string().repeat(24));
        let (a, b, c, hidden) = (did('a'), did('b'), did('c'), did('h'));
        for (i, d) in [&a, &b, &c, &hidden].into_iter().enumerate() {
            actor(p, d, &format!("user{i}.test"), "User").await;
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)").bind(d).execute(p).await.expect("scope");
        }
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{hidden}', '!hide')")).await;
        exec(p, "CREATE INDEX posts_tags ON posts USING gin (tags)").await;
        // #rust: three posts by two authors. #solo: three posts by one.
        // #quiet: three posts by two authors, liked less. #ghost: carried
        // by a hidden author.
        let mut uris: HashMap<&str, Vec<String>> = HashMap::new();
        for (author, rkey, tag) in [
            (&a, "3ka", "rust"), (&a, "3kb", "rust"), (&b, "3kc", "rust"),
            (&a, "3kd", "solo"), (&a, "3ke", "solo"), (&a, "3kf", "solo"),
            (&b, "3kg", "quiet"), (&c, "3kh", "quiet"), (&c, "3ki", "quiet"), (&c, "3kj", "quiet"),
            (&hidden, "3kk", "ghost"), (&hidden, "3kl", "ghost"), (&a, "3km", "ghost"),
        ] {
            let uri = post(p, author, rkey, "tagged", None, None).await;
            sqlx::query("UPDATE posts SET tags = ARRAY[$2] WHERE uri = $1").bind(&uri).bind(tag).execute(p).await.expect("tag");
            uris.entry(tag).or_default().push(uri);
        }
        let like = |n: usize, subject: &str| {
            format!("INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES ('at://{c}/app.bsky.feed.like/{n}', '{c}', '{subject}', 'bafy', '3k', now())")
        };
        for n in 0..6 {
            exec(p, &like(n, &uris["rust"][n % 3])).await;
        }
        exec(p, &like(10, &uris["quiet"][0])).await;
        for n in 20..40 {
            exec(p, &like(n, &uris["solo"][0])).await;
            exec(p, &like(n + 100, &uris["ghost"][0])).await;
        }

        let h = hydrator(p, ScopeSets::default());
        let req = Requester::test();
        let topics = h.trending_topics(10).await.expect("db").expect("view").inner().clone();
        let names = |view: &Value, key: &str| view[key].as_array().expect(key).iter().map(|t| t["topic"].as_str().expect("topic").to_string()).collect::<Vec<_>>();
        // By posts: #quiet has four, #rust three. #solo has one author;
        // #ghost has one author that counts.
        assert_eq!(names(&topics, "topics"), ["#quiet", "#rust"]);
        assert_eq!((topics["topics"][1]["link"].clone(), topics["suggested"].clone()), (json!("/hashtag/rust"), json!([])));

        let trends = h.trends(&Viewer::anonymous(), &req, 10).await.expect("db").expect("view").inner().clone();
        // By engagement: #rust's six likes beat #quiet's one.
        assert_eq!(names(&trends, "trends"), ["#rust", "#quiet"]);
        let rust = &trends["trends"][0];
        assert_eq!((rust["postCount"].clone(), rust["displayName"].clone()), (json!(3), json!("#rust")));
        let actors: Vec<&str> = rust["actors"].as_array().expect("actors").iter().map(|x| x["did"].as_str().expect("did")).collect();
        assert_eq!(actors, [a.as_str(), b.as_str()]);
        assert!(rust["startedAt"].is_string() && rust.get("category").is_none());
        assert_eq!(trends["recIdStr"].as_str().map(str::len), Some(32));
        db.drop_db().await;
    }
}
