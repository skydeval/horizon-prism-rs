//! Public hydration (design-v2 §7.1, §7.2, §7.5, §8.6, §9.6): view assembly
//! for `app.bsky.*`.
//!
//! - Everything is built from public tables, Constellation data and (from
//!   item 7) normalized upstream views, and emitted as [`PublicEmit`].
//! - Every read path runs the §7.10 moderation gate before consulting any
//!   source.
//! - Viewer state is computed only for ready community members (§7.5); for
//!   anyone else `viewer` is `{}`. Blocks come from `blocks` only.
//! - Counts follow §9.6: synchronous Constellation enrichment for
//!   single-object views, cached values plus `cc:refresh` for lists.

pub mod actor;
pub mod cache;
pub mod emit;
pub mod feed;
pub mod graph;
pub mod hint;
pub mod normalize;
pub mod notifications;
pub mod thread;
pub mod upstream;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use redis::aio::ConnectionManager;
use serde_json::{Value, json};
use sqlx::PgPool;

pub use cache::ReadCache;
pub use emit::PublicEmit;
pub use upstream::{NoUpstream, UpstreamSource};

use crate::aggregations::ScopeState;
use crate::aggregations::counts::{self, Counts, Freshness};
use crate::config::{CountsConfig, CountsSource};
use crate::db::locks::uri_author;
use crate::microcosm::ConstellationClient;

/// Placeholder handle for DIDs without a verified handle (ATProto convention).
pub const INVALID_HANDLE: &str = "handle.invalid";
/// Synchronous enrichment: per-call timeout, concurrency, request budget (§9.6).
pub const SYNC_TIMEOUT: Duration = Duration::from_millis(800);
pub const SYNC_CONCURRENCY: usize = 8;
pub const SYNC_BUDGET: Duration = Duration::from_secs(1);

/// A hydration failure: always a server error.
#[derive(Debug, thiserror::Error)]
pub enum HydrationError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
}

/// A lexicon-level failure a handler returns as a 400.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewError {
    pub error: &'static str,
    pub message: String,
}

impl ViewError {
    pub fn new(error: &'static str, message: impl Into<String>) -> Self {
        Self {
            error,
            message: message.into(),
        }
    }
}

/// A handler outcome from hydration.
pub type ViewResult<T> = Result<Result<PublicEmit<T>, ViewError>, HydrationError>;

/// Who is asking (§7.5).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Viewer {
    /// The authenticated caller, if any.
    pub did: Option<String>,
    /// Viewer state is hydrated only for ready members.
    pub hydrate: bool,
}

impl Viewer {
    #[must_use]
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// The DID to hydrate viewer state for, if any.
    #[must_use]
    pub fn state_did(&self) -> Option<&str> {
        if self.hydrate { self.did.as_deref() } else { None }
    }
}

/// Count mode for a view (§9.6).
#[derive(Debug, Clone, Copy)]
pub enum CountMode {
    /// Single-object views: fetch synchronously within the request budget.
    Sync { started: Instant },
    /// List views: cached rows only; misses are enqueued.
    List,
}

/// Static hydration settings.
#[derive(Debug, Clone)]
pub struct HydrationConfig {
    /// `did:web:<host>`: the `src` of local labels (§7.8).
    pub labeler_did: String,
    pub counts: CountsConfig,
}

struct Inner {
    pool: PgPool,
    scope: ScopeState,
    constellation: Option<ConstellationClient>,
    redis: Option<ConnectionManager>,
    upstream: Arc<dyn UpstreamSource>,
    cache: ReadCache,
    cfg: HydrationConfig,
}

/// The hydrator. Cheap to clone.
#[derive(Clone)]
pub struct Hydrator {
    inner: Arc<Inner>,
}

/// One `actors` row joined with `actor_counts`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ActorRow {
    pub did: String,
    pub handle: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub avatar_cid: Option<String>,
    pub banner_cid: Option<String>,
    pub pds_endpoint: Option<String>,
    pub indexed_at: DateTime<Utc>,
    pub profile_record: Option<Value>,
    pub followers_count: Option<i32>,
    pub follows_count: Option<i32>,
    pub posts_count: Option<i32>,
}

/// One `posts` row joined with `post_counts`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PostRow {
    pub uri: String,
    pub author_did: String,
    pub cid: String,
    pub record: Value,
    pub embed: Option<Value>,
    pub reply_parent_uri: Option<String>,
    pub reply_root_uri: Option<String>,
    pub quote_uri: Option<String>,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
    pub like_count: Option<i32>,
    pub repost_count: Option<i32>,
    pub reply_count: Option<i32>,
    pub quote_count: Option<i32>,
}

/// Viewer relationships to actors (§7.5).
#[derive(Debug, Clone, Default)]
pub struct ActorViewer {
    pub following: HashMap<String, String>,
    pub followed_by: HashMap<String, String>,
    pub blocking: HashMap<String, String>,
    pub blocked_by: HashSet<String>,
    pub muted: HashSet<String>,
}

impl ActorViewer {
    /// The lexicon `viewerState` for one actor.
    #[must_use]
    pub fn view(&self, did: &str) -> Value {
        let mut v = serde_json::Map::new();
        v.insert("muted".into(), json!(self.muted.contains(did)));
        v.insert("blockedBy".into(), json!(self.blocked_by.contains(did)));
        if let Some(u) = self.blocking.get(did) {
            v.insert("blocking".into(), json!(u));
        }
        if let Some(u) = self.following.get(did) {
            v.insert("following".into(), json!(u));
        }
        if let Some(u) = self.followed_by.get(did) {
            v.insert("followedBy".into(), json!(u));
        }
        Value::Object(v)
    }

    /// A block in either direction.
    #[must_use]
    pub fn blocked_either_way(&self, did: &str) -> bool {
        self.blocking.contains_key(did) || self.blocked_by.contains(did)
    }
}

/// Timestamps as ATProto datetimes.
#[must_use]
pub fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// A blob URL served by the author's PDS (`com.atproto.sync.getBlob`).
#[must_use]
pub fn blob_url(pds: Option<&str>, did: &str, cid: &str) -> Option<String> {
    let pds = pds?.trim_end_matches('/');
    Some(format!("{pds}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}"))
}

impl Hydrator {
    #[must_use]
    pub fn new(
        pool: PgPool,
        scope: ScopeState,
        constellation: Option<ConstellationClient>,
        redis: Option<ConnectionManager>,
        upstream: Arc<dyn UpstreamSource>,
        cache: ReadCache,
        cfg: HydrationConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                scope,
                constellation,
                redis,
                upstream,
                cache,
                cfg,
            }),
        }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    #[must_use]
    pub fn scope(&self) -> &ScopeState {
        &self.inner.scope
    }

    fn constellation(&self) -> Option<&ConstellationClient> {
        if self.inner.cfg.counts.public_source == CountsSource::Local {
            None
        } else {
            self.inner.constellation.as_ref()
        }
    }

    fn backlinks(&self) -> Option<&ConstellationClient> {
        self.inner.constellation.as_ref()
    }

    fn upstream(&self) -> &dyn UpstreamSource {
        self.inner.upstream.as_ref()
    }

    fn cache(&self) -> &ReadCache {
        &self.inner.cache
    }

    /// The viewer for an authenticated (or anonymous) caller: state is
    /// hydrated only for a member with `ready_at` set (§7.5).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn viewer(&self, caller: Option<&str>) -> Result<Viewer, sqlx::Error> {
        let Some(did) = caller else {
            return Ok(Viewer::anonymous());
        };
        let ready: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1 AND ready_at IS NOT NULL)")
            .bind(did)
            .fetch_one(self.pool())
            .await?;
        Ok(Viewer {
            did: Some(did.to_string()),
            hydrate: ready,
        })
    }

    /// Resolve an `actor` parameter (DID or handle) to a DID, locally.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn resolve_actor(&self, actor: &str) -> Result<Option<String>, sqlx::Error> {
        let actor = actor.trim().trim_start_matches('@');
        if actor.starts_with("did:") {
            return Ok(proto_blue::syntax::Did::is_valid(actor).then(|| actor.to_string()));
        }
        let local: Option<String> = sqlx::query_scalar("SELECT did FROM actors WHERE handle = $1")
            .bind(actor.to_ascii_lowercase())
            .fetch_optional(self.pool())
            .await?;
        if local.is_some() || !proto_blue::syntax::Handle::is_valid(actor) {
            return Ok(local);
        }
        // A handle this AppView has not indexed (§6.7).
        Ok(self.upstream().resolve_handle(actor).await)
    }

    /// Whether a DID is an in-scope subject (§7.1).
    pub async fn in_scope(&self, did: &str) -> bool {
        self.scope().read_guard().await.in_scope(did)
    }

    /// Local actor rows.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn load_actors(&self, dids: &[String]) -> Result<HashMap<String, ActorRow>, sqlx::Error> {
        if dids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<ActorRow> = sqlx::query_as(
            "SELECT a.did, a.handle, a.display_name, a.description, a.avatar_cid, a.banner_cid, a.pds_endpoint, \
               a.indexed_at, a.profile_record, c.followers_count, c.follows_count, c.posts_count \
             FROM actors a LEFT JOIN actor_counts c ON c.did = a.did WHERE a.did = ANY($1)",
        )
        .bind(dids)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(|r| (r.did.clone(), r)).collect())
    }

    /// Local post rows (never private-tier: those live only in `private_tier`).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn load_posts(&self, uris: &[String]) -> Result<HashMap<String, PostRow>, sqlx::Error> {
        if uris.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<PostRow> = sqlx::query_as(
            "SELECT p.uri, p.author_did, p.cid, p.record, p.embed, p.reply_parent_uri, p.reply_root_uri, \
               p.quote_uri, p.created_at, p.indexed_at, \
               c.like_count, c.repost_count, c.reply_count, c.quote_count \
             FROM posts p LEFT JOIN post_counts c ON c.post_uri = p.uri WHERE p.uri = ANY($1)",
        )
        .bind(uris)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(|r| (r.uri.clone(), r)).collect())
    }

    /// Local labels (§7.8) by subject, as lexicon label objects.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn load_labels(&self, subjects: &[String]) -> Result<HashMap<String, Vec<Value>>, sqlx::Error> {
        if subjects.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<(String, String, DateTime<Utc>)> =
            sqlx::query_as("SELECT subject, val, created_at FROM labels WHERE subject = ANY($1) ORDER BY id")
                .bind(subjects)
                .fetch_all(self.pool())
                .await?;
        let mut out: HashMap<String, Vec<Value>> = HashMap::new();
        for (subject, val, cts) in rows {
            out.entry(subject.clone()).or_default().push(json!({
                "src": self.inner.cfg.labeler_did,
                "uri": subject,
                "val": val,
                "cts": ts(cts),
            }));
        }
        Ok(out)
    }

    /// Viewer relationships to `dids` (§7.5). Empty for a non-hydrated viewer.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn actor_viewer(&self, viewer: &Viewer, dids: &[String]) -> Result<ActorViewer, sqlx::Error> {
        let Some(me) = viewer.state_did() else {
            return Ok(ActorViewer::default());
        };
        if dids.is_empty() {
            return Ok(ActorViewer::default());
        }
        let p = self.pool();
        let following: Vec<(String, String)> =
            sqlx::query_as("SELECT subject_did, uri FROM follows WHERE author_did = $1 AND subject_did = ANY($2)")
                .bind(me)
                .bind(dids)
                .fetch_all(p)
                .await?;
        let followed_by: Vec<(String, String)> =
            sqlx::query_as("SELECT author_did, uri FROM follows WHERE subject_did = $1 AND author_did = ANY($2)")
                .bind(me)
                .bind(dids)
                .fetch_all(p)
                .await?;
        // `blocks` only: `app.bsky.graph.block`, never the private-tier block
        // table (§7.5).
        let blocking: Vec<(String, String)> =
            sqlx::query_as("SELECT subject_did, uri FROM blocks WHERE author_did = $1 AND subject_did = ANY($2)")
                .bind(me)
                .bind(dids)
                .fetch_all(p)
                .await?;
        let blocked_by: Vec<String> =
            sqlx::query_scalar("SELECT author_did FROM blocks WHERE subject_did = $1 AND author_did = ANY($2)")
                .bind(me)
                .bind(dids)
                .fetch_all(p)
                .await?;
        let muted: Vec<String> =
            sqlx::query_scalar("SELECT subject_did FROM mutes WHERE viewer_did = $1 AND subject_did = ANY($2)")
                .bind(me)
                .bind(dids)
                .fetch_all(p)
                .await?;
        Ok(ActorViewer {
            following: following.into_iter().collect(),
            followed_by: followed_by.into_iter().collect(),
            blocking: blocking.into_iter().collect(),
            blocked_by: blocked_by.into_iter().collect(),
            muted: muted.into_iter().collect(),
        })
    }

    /// Normalize an upstream view for `viewer` (§8.6): local viewer state
    /// (§7.5) and local labels replace upstream's, and private-tier
    /// references become not-found. Returns what normalization observed.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn normalized(&self, viewer: &Viewer, mut view: Value) -> Result<(Value, normalize::NormalizeReport), sqlx::Error> {
        let (dids, uris) = normalize::subjects(&view);
        let dids: Vec<String> = dids.into_iter().collect();
        let uris: Vec<String> = uris.into_iter().collect();
        let actors = self.actor_viewer(viewer, &dids).await?;
        let post_viewers = self.post_viewer(viewer, &uris).await?;
        let labelled: Vec<String> = dids.iter().chain(uris.iter()).cloned().collect();
        let overlay = normalize::Overlay {
            profile_viewers: dids.iter().map(|d| (d.clone(), actors.view(d))).collect(),
            post_viewers,
            labels: self.load_labels(&labelled).await?,
        };
        let report = normalize::normalize_upstream(&mut view, &overlay);
        Ok((view, report))
    }

    /// The viewer's likes and reposts of `uris` (§7.5).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn post_viewer(&self, viewer: &Viewer, uris: &[String]) -> Result<HashMap<String, Value>, sqlx::Error> {
        let Some(me) = viewer.state_did() else {
            return Ok(HashMap::new());
        };
        let likes: Vec<(String, String)> =
            sqlx::query_as("SELECT subject_uri, uri FROM likes WHERE author_did = $1 AND subject_uri = ANY($2)")
                .bind(me)
                .bind(uris)
                .fetch_all(self.pool())
                .await?;
        let reposts: Vec<(String, String)> =
            sqlx::query_as("SELECT subject_uri, uri FROM reposts WHERE author_did = $1 AND subject_uri = ANY($2)")
                .bind(me)
                .bind(uris)
                .fetch_all(self.pool())
                .await?;
        let mut out: HashMap<String, Value> = uris.iter().map(|u| (u.clone(), json!({}))).collect();
        for (subject, like) in likes {
            out.entry(subject).or_insert_with(|| json!({}))["like"] = json!(like);
        }
        for (subject, repost) in reposts {
            out.entry(subject).or_insert_with(|| json!({}))["repost"] = json!(repost);
        }
        Ok(out)
    }

    /// Constellation counts for targets under the §9.6 rule. Targets with no
    /// usable value are absent (the caller uses local counts).
    pub async fn constellation_counts(&self, targets: &[String], mode: CountMode) -> HashMap<String, Counts> {
        let mut out = HashMap::new();
        let Some(client) = self.constellation() else {
            return out;
        };
        let cfg = &self.inner.cfg.counts;
        let mut misses: Vec<String> = Vec::new();
        for t in targets {
            match counts::cached(self.pool(), t, cfg).await {
                Ok(Some((c, Freshness::Fresh))) => {
                    out.insert(t.clone(), c);
                }
                Ok(Some((c, _))) => {
                    out.insert(t.clone(), c);
                    self.enqueue_refresh(t).await;
                }
                Ok(None) => misses.push(t.clone()),
                Err(error) => tracing::debug!(event = "constellation_counts_read_failed", %error, "counts read failed"),
            }
        }
        match mode {
            CountMode::List => {
                for t in &misses {
                    self.enqueue_refresh(t).await;
                }
            }
            CountMode::Sync { started } => {
                use futures::StreamExt;
                let fetched: Vec<(String, Option<Counts>)> = futures::stream::iter(misses)
                    .map(|t| async move {
                        if started.elapsed() >= SYNC_BUDGET {
                            return (t, None);
                        }
                        let result = tokio::time::timeout(SYNC_TIMEOUT, counts::fetch(client, &t)).await;
                        (t, result.ok().and_then(Result::ok))
                    })
                    .buffer_unordered(SYNC_CONCURRENCY)
                    .collect()
                    .await;
                for (t, c) in fetched {
                    match c {
                        Some(c) => {
                            if let Err(error) = counts::store(self.pool(), &t, &c).await {
                                tracing::debug!(event = "constellation_counts_store_failed", %error, "storing counts failed");
                            }
                            out.insert(t, c);
                        }
                        None => self.enqueue_refresh(&t).await,
                    }
                }
            }
        }
        out
    }

    async fn enqueue_refresh(&self, target: &str) {
        if let Some(redis) = &self.inner.redis {
            let mut conn = redis.clone();
            counts::enqueue_refresh(&mut conn, target, self.inner.cfg.counts.refresh_queue_max).await;
        }
    }

    /// Authors of `uris`, deduplicated.
    #[must_use]
    pub fn authors_of(uris: &[String]) -> Vec<String> {
        let mut seen = HashSet::new();
        uris.iter()
            .filter_map(|u| uri_author(u))
            .filter(|d| seen.insert(d.to_string()))
            .map(str::to_string)
            .collect()
    }

    pub(crate) fn emit<T>(value: T) -> PublicEmit<T> {
        PublicEmit::new(value)
    }
}

/// Test helpers shared by the hydration and xrpc tests.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;

    pub(crate) const LABELER: &str = "did:web:appview.example.com";

    pub(crate) fn hydrator(pool: &PgPool, sets: ScopeSets) -> Hydrator {
        Hydrator::new(
            pool.clone(),
            ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000),
            None,
            None,
            Arc::new(NoUpstream),
            ReadCache::new(1024 * 1024),
            HydrationConfig {
                labeler_did: LABELER.into(),
                counts: CountsConfig::default(),
            },
        )
    }

    pub(crate) async fn exec(pool: &PgPool, sql: &str) {
        sqlx::query(sql).execute(pool).await.expect(sql);
    }

    pub(crate) async fn actor(pool: &PgPool, did: &str, handle: &str, name: &str) {
        sqlx::query(
            "INSERT INTO actors (did, handle, display_name, pds_endpoint, profile_record) \
             VALUES ($1, $2, $3, 'https://pds.example.com', '{\"createdAt\":\"2026-01-01T00:00:00Z\"}'::jsonb)",
        )
        .bind(did)
        .bind(handle)
        .bind(name)
        .execute(pool)
        .await
        .expect("actor");
    }

    pub(crate) async fn post(pool: &PgPool, author: &str, rkey: &str, text: &str, parent: Option<&str>, root: Option<&str>) -> String {
        let uri = format!("at://{author}/app.bsky.feed.post/{rkey}");
        let mut record = json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-09-26T12:00:00.000Z"});
        if let (Some(p), Some(r)) = (parent, root) {
            record["reply"] = json!({"parent": {"uri": p, "cid": "bafyparent"}, "root": {"uri": r, "cid": "bafyroot"}});
        }
        sqlx::query(
            "INSERT INTO posts (uri, author_did, rkey, cid, rev, text, reply_parent_uri, reply_root_uri, record, created_at) \
             VALUES ($1, $2, $3, $4, '3kaaaaaaaaaaa', $5, $6, $7, $8, now())",
        )
        .bind(&uri)
        .bind(author)
        .bind(rkey)
        .bind(format!("bafy{rkey}"))
        .bind(text)
        .bind(parent)
        .bind(root)
        .bind(&record)
        .execute(pool)
        .await
        .expect("post");
        let kind = if parent.is_some() { "reply" } else { "post" };
        sqlx::query("INSERT INTO feed_items (uri, originator_did, post_uri, kind, sort_at) VALUES ($1, $2, $1, $3, now())")
            .bind(&uri)
            .bind(author)
            .bind(kind)
            .execute(pool)
            .await
            .expect("feed item");
        uri
    }
}
