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

pub mod activity_subscriptions;
pub mod bookmarks;
pub mod drafts;
pub mod actor;
pub mod cache;
pub mod emit;
pub mod feed;
pub mod graph;
pub mod hint;
pub mod identity;
pub mod labelers;
pub mod labels;
pub mod lens;
pub mod list_mutes;
pub mod lists;
pub mod normalize;
pub mod notification_prefs;
pub mod notifications;
pub mod polls;
pub mod relationships;
pub mod repo;
pub mod search;
pub mod starter_packs;
pub mod suggestions;
pub mod thread;
pub mod thread_v2;
pub mod trends;
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
use crate::config::{CountsConfig, CountsSource, ThreadConfig};
use crate::db::locks::uri_author;
use crate::farsight::Farsight;
use crate::microcosm::ConstellationClient;
use crate::subscriptions::applier::Applier;
use crate::subscriptions::references::Budget;
use crate::xrpc::pools::{Exhausted, Requester, SharedBucket};

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
    /// A listing had no upstream budget before it produced anything
    /// (design-v3 §II-8.3): `429 RateLimitExceeded`, never an empty page.
    #[error("no upstream budget for this request")]
    NoBudget(Exhausted),
    /// What the request needs from another server could not be had now,
    /// and nothing shows it is absent: `502 UpstreamFailure`.
    #[error("an upstream lookup could not be completed")]
    Upstream,
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
    /// Viewer-private listings (bookmarks): cached rows only, and nothing
    /// is enqueued, so the read leaves no trace in shared state.
    Quiet,
}

/// Static hydration settings.
#[derive(Debug, Clone)]
pub struct HydrationConfig {
    /// `did:web:<host>`: the `src` of local labels (§7.8).
    pub labeler_did: String,
    pub counts: CountsConfig,
    pub thread: ThreadConfig,
    /// `thread_mutes.max_per_actor`.
    pub thread_mutes_max: u32,
    /// `activity_subscriptions.max_per_actor`.
    pub activity_subscriptions_max: u32,
    /// `bookmarks.max_per_actor`.
    pub bookmarks_max: u32,
    /// `bookmarks.emit_count`.
    pub bookmarks_emit_count: crate::config::BookmarkCount,
    /// `bookmarks.count_floor`.
    pub bookmarks_count_floor: u32,
    /// `drafts.max_per_actor`.
    pub drafts_max: u32,
    /// `drafts.max_bytes`.
    pub drafts_max_bytes: u32,
    /// `labels.query_max_patterns`.
    pub labels_query_max_patterns: u32,
    /// `labelers.get_services_max_dids`.
    pub labelers_max_dids: u32,
    /// `known_followers.scan_max_pages`.
    pub known_followers_scan_max_pages: u32,
    /// `known_followers.profile_scan_pages`.
    pub known_followers_profile_scan_pages: u32,
    /// `known_likers.scan_max_pages`.
    pub known_likers_scan_max_pages: u32,
    /// `thread_v2.max_parents`.
    pub thread_v2_max_parents: u32,
    /// `op_thread.max_walk`.
    pub op_thread_max_walk: u32,
    /// `[search]`.
    pub search: crate::config::SearchConfig,
    /// `[suggestions]`.
    pub suggestions: crate::config::SuggestionsConfig,
    /// `[unspecced]`.
    pub unspecced: crate::config::UnspeccedConfig,
    /// `[trends]`.
    pub trends: crate::config::TrendsConfig,
    /// The block-list index, when `farsight.url` is set.
    pub farsight: Option<Farsight>,
    /// A member's budget of list mutes (`lists.subscribed_max_per_member`,
    /// `lists.max_owners_per_member`).
    pub list_budget: Budget,
    /// Told when a member mutes or unmutes a list, so the list phase
    /// follows at once.
    pub list_applier: Option<Applier>,
    /// The shared bucket for request-origin Constellation calls
    /// (`microcosm.constellation_rate_per_sec`); `None` leaves them unmetered.
    pub constellation_bucket: Option<SharedBucket>,
}

struct Inner {
    pool: PgPool,
    scope: ScopeState,
    constellation: Option<ConstellationClient>,
    redis: Option<ConnectionManager>,
    upstream: Arc<dyn UpstreamSource>,
    cache: ReadCache,
    /// The suggestion pools and their per-viewer caches (§7.1).
    suggestions: suggestions::State,
    /// The trend snapshots (§7.4, §7.5).
    trends: trends::State,
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

/// Blocks a request knows beyond what local rows show (design-v3
/// §5.15.12). Thread Lens judges every author with Farsight's help and
/// supplies the result here, so that every view built for the request
/// (profiles, posts, embeds, upstream views) applies the same blocks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KnownBlocks {
    /// Authors the viewer blocks.
    pub by_viewer: HashSet<String>,
    /// Authors that block the viewer.
    pub of_viewer: HashSet<String>,
}

tokio::task_local! {
    static KNOWN_BLOCKS: Arc<KnownBlocks>;
}

/// Run `work` with `blocks` applied by every `actor_viewer` call it makes.
pub async fn with_known_blocks<F: std::future::Future>(blocks: Arc<KnownBlocks>, work: F) -> F::Output {
    KNOWN_BLOCKS.scope(blocks, work).await
}

fn known_blocks() -> Option<Arc<KnownBlocks>> {
    KNOWN_BLOCKS.try_with(Arc::clone).ok()
}

/// A locally servable list, for a `listViewBasic`: uri, cid, name,
/// purpose, avatar CID, indexed time, and its creator's PDS.
type LocalListRow = (String, String, String, String, Option<String>, DateTime<Utc>, Option<String>);

/// When a list reference `r` to the list row `l` **applies** (design-v3
/// §4.3 "Effective references", "List purpose"): it is admitted, a sync
/// has served it, its list is active, and the list is a moderation list.
/// A reference past the member's budget, not yet synced, or to any other
/// kind of list has no effect on any view.
pub(crate) const APPLIES: &str =
    "r.admitted AND r.effective AND l.state = 'active' AND l.purpose = 'app.bsky.graph.defs#modlist'";

/// Viewer relationships to actors (§7.5).
#[derive(Debug, Clone, Default)]
pub struct ActorViewer {
    pub following: HashMap<String, String>,
    pub followed_by: HashMap<String, String>,
    pub blocking: HashMap<String, String>,
    pub blocked_by: HashSet<String>,
    /// Subjects the viewer blocks by a block this AppView holds no record
    /// of (one Farsight reported). It applies as a block; `viewer.blocking`
    /// needs a record URI and is not emitted for it.
    pub blocking_unrecorded: HashSet<String>,
    /// Subjects that are fully muted: by a `full` direct mute, or by a list
    /// mute of the viewer's that applies.
    pub muted: HashSet<String>,
    /// Subjects on a list the viewer mutes, with that list (design-v3
    /// §5.3). Only a mute that applies: admitted, synced since, and of a
    /// moderation list.
    pub muted_by_list: HashMap<String, String>,
    /// Subjects on a list the viewer blocks, with that list, under the
    /// same rule. Such a block is the viewer's own and applies outbound.
    pub blocking_by_list: HashMap<String, String>,
    /// `listViewBasic`s for the lists above that are served from local
    /// rows.
    pub list_views: HashMap<String, Value>,
    /// Subjects whose direct mute covers reposts (`reposts`,
    /// `reposts_and_quotes`).
    pub muted_reposts: HashSet<String>,
    /// Subjects whose direct mute covers quote posts (`quotes`,
    /// `reposts_and_quotes`).
    pub muted_quotes: HashSet<String>,
    /// Subjects the viewer has an activity subscription to: `(post, reply)`.
    pub subscribed: HashMap<String, (bool, bool)>,
}

impl ActorViewer {
    /// The lexicon `viewerState` for one actor. `muted` is the full mute;
    /// the scoped flags are emitted only when it is false (design-v3 §5.3,
    /// "Viewer mute flags"). A list mute that applies sets `muted` too,
    /// and names its list whatever the direct mute's scope.
    #[must_use]
    pub fn view(&self, did: &str) -> Value {
        let mut v = serde_json::Map::new();
        let muted = self.muted.contains(did);
        v.insert("muted".into(), json!(muted));
        for (field, by_list) in [("mutedByList", &self.muted_by_list), ("blockingByList", &self.blocking_by_list)] {
            if let Some(view) = by_list.get(did).and_then(|list| self.list_views.get(list)) {
                v.insert(field.into(), view.clone());
            }
        }
        if !muted && self.muted_reposts.contains(did) {
            v.insert("mutedOnlyReposts".into(), json!(true));
        }
        if !muted && self.muted_quotes.contains(did) {
            v.insert("mutedOnlyQuoteposts".into(), json!(true));
        }
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
        if let Some((post, reply)) = self.subscribed.get(did) {
            v.insert("activitySubscription".into(), json!({"post": post, "reply": reply}));
        }
        Value::Object(v)
    }

    /// A block in either direction: a direct one, an inbound list block,
    /// or the viewer's own list block that applies.
    #[must_use]
    pub fn blocked_either_way(&self, did: &str) -> bool {
        self.blocking.contains_key(did)
            || self.blocked_by.contains(did)
            || self.blocking_by_list.contains_key(did)
            || self.blocking_unrecorded.contains(did)
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
                suggestions: suggestions::State::new(&cfg.suggestions),
                trends: trends::State::default(),
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

    fn constellation_bucket(&self) -> Option<&SharedBucket> {
        self.inner.cfg.constellation_bucket.as_ref()
    }

    /// Charge one request-origin Constellation listing call to `req`.
    pub(super) fn draw_constellation(&self, req: &Requester) -> Result<(), Exhausted> {
        self.constellation_bucket().map_or(Ok(()), |b| req.try_draw(b))
    }

    /// The block-list index, when one is configured.
    #[must_use]
    pub fn farsight(&self) -> Option<&Farsight> {
        self.inner.cfg.farsight.as_ref()
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
    pub async fn resolve_actor(&self, req: &Requester, actor: &str) -> Result<Option<String>, sqlx::Error> {
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
        Ok(self.upstream().resolve_handle(req, actor).await)
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
        let mutes: Vec<(String, String)> =
            sqlx::query_as("SELECT subject_did, scope FROM mutes WHERE viewer_did = $1 AND subject_did = ANY($2)")
                .bind(me)
                .bind(dids)
                .fetch_all(p)
                .await?;
        let subscribed: Vec<(String, bool, bool)> = sqlx::query_as(
            "SELECT subject_did, post, reply FROM activity_subscriptions WHERE subscriber_did = $1 AND subject_did = ANY($2)",
        )
        .bind(me)
        .bind(dids)
        .fetch_all(p)
        .await?;
        let mut out = ActorViewer {
            subscribed: subscribed.into_iter().map(|(did, post, reply)| (did, (post, reply))).collect(),
            following: following.into_iter().collect(),
            followed_by: followed_by.into_iter().collect(),
            blocking: blocking.into_iter().collect(),
            blocked_by: blocked_by.into_iter().collect(),
            ..ActorViewer::default()
        };
        // Inbound list blocks, from Farsight (design-v3 §5.14.5): asked only
        // when this request carries a handle, about the accounts with no
        // direct block. An account that blocks the viewer through a list is
        // treated as one that blocks it directly.
        if let Some(lookup) = crate::xrpc::backstop::farsight() {
            let direct: HashSet<String> = out.blocking.keys().chain(&out.blocked_by).cloned().collect();
            let by_list = lookup.inbound_list_blocks(me, dids, &direct).await;
            out.blocked_by.extend(by_list.into_keys());
        }
        for (did, scope) in mutes {
            match scope.as_str() {
                "reposts" => {
                    out.muted_reposts.insert(did);
                }
                "quotes" => {
                    out.muted_quotes.insert(did);
                }
                "reposts_and_quotes" => {
                    out.muted_reposts.insert(did.clone());
                    out.muted_quotes.insert(did);
                }
                _ => {
                    out.muted.insert(did);
                }
            }
        }
        // The viewer's own list mutes and list blocks that apply (design-v3
        // §4.3 "List purpose", §5.3): the reference is admitted and has been
        // served by a sync, its list is active and is a moderation list.
        // The items come from the subscription copy, never from
        // `list_items`, so every reference is applied from one source.
        let by_list: Vec<(String, String, String)> = sqlx::query_as(&format!(
            "SELECT DISTINCT ON (i.subject_did, r.source) i.subject_did, r.source, r.list_uri \
             FROM list_references r \
             JOIN subscribed_lists l ON l.list_uri = r.list_uri \
             JOIN subscribed_list_items i ON i.list_uri = r.list_uri \
             WHERE r.member_did = $1 AND {APPLIES} AND i.subject_did = ANY($2) \
             ORDER BY i.subject_did, r.source, r.arrived_at, r.list_uri"
        ))
        .bind(me)
        .bind(dids)
        .fetch_all(p)
        .await?;
        for (did, source, list) in by_list {
            // The viewer is never hidden from itself by its own list.
            if did == me {
                continue;
            }
            if source == "mute" {
                out.muted.insert(did.clone());
                out.muted_by_list.insert(did, list);
            } else {
                out.blocking_by_list.insert(did, list);
            }
        }
        out.list_views = self.local_list_basics(me, out.muted_by_list.values().chain(out.blocking_by_list.values())).await?;
        // Blocks the request itself knows (Thread Lens, §5.15.12). The
        // direction is the one the request found, not a guess from which
        // source spoke.
        if let Some(known) = known_blocks() {
            for did in dids.iter().filter(|did| did.as_str() != me) {
                if known.of_viewer.contains(did) {
                    out.blocked_by.insert(did.clone());
                }
                if known.by_viewer.contains(did) && !out.blocking.contains_key(did) {
                    out.blocking_unrecorded.insert(did.clone());
                }
            }
        }
        Ok(out)
    }

    /// `listViewBasic`s for those of `lists` that are locally servable
    /// (their owner is a ready member), with the viewer's own state on
    /// them. Any other list has no view here: its record is not stored.
    async fn local_list_basics(&self, me: &str, lists: impl Iterator<Item = &String>) -> Result<HashMap<String, Value>, sqlx::Error> {
        let mut uris: Vec<String> = lists.cloned().collect();
        uris.sort_unstable();
        uris.dedup();
        if uris.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<LocalListRow> = sqlx::query_as(
            "SELECT l.uri, l.cid, l.name, l.purpose, l.avatar_cid, l.indexed_at, a.pds_endpoint FROM lists l \
             JOIN members m ON m.did = l.creator_did AND m.ready_at IS NOT NULL \
             LEFT JOIN actors a ON a.did = l.creator_did WHERE l.uri = ANY($1)",
        )
        .bind(&uris)
        .fetch_all(self.pool())
        .await?;
        let muted: HashSet<String> = sqlx::query_scalar("SELECT list_uri FROM list_mutes WHERE viewer_did = $1 AND list_uri = ANY($2)")
            .bind(me)
            .bind(&uris)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .collect();
        let blocked: HashMap<String, String> = sqlx::query_as(
            "SELECT DISTINCT ON (list_uri) list_uri, uri FROM list_blocks WHERE author_did = $1 AND list_uri = ANY($2) ORDER BY list_uri, created_at, uri",
        )
        .bind(me)
        .bind(&uris)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .collect();
        let labels = self.load_labels(&uris).await?;
        Ok(rows
            .into_iter()
            .map(|(uri, cid, name, purpose, avatar_cid, indexed_at, pds)| {
                let mut viewer = json!({ "muted": muted.contains(&uri) });
                if let Some(record) = blocked.get(&uri) {
                    viewer["blocked"] = json!(record);
                }
                let mut view = json!({
                    "uri": uri,
                    "cid": cid,
                    "name": name,
                    "purpose": purpose,
                    "labels": labels.get(&uri).cloned().unwrap_or_default(),
                    "viewer": viewer,
                    "indexedAt": ts(indexed_at),
                });
                let creator = uri_author(&uri).unwrap_or_default();
                if let Some(url) = avatar_cid.as_deref().and_then(|cid| blob_url(pds.as_deref(), creator, cid)) {
                    view["avatar"] = json!(url);
                }
                (uri, view)
            })
            .collect())
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
        // An upstream reply is not stored, so its root comes from its record.
        let roots: Vec<String> = normalize::thread_roots(&view).into_iter().collect();
        let muted_threads = self.in_muted_threads(viewer, &roots).await?;
        let labelled: Vec<String> = dids.iter().chain(uris.iter()).cloned().collect();
        let overlay = normalize::Overlay {
            profile_viewers: dids.iter().map(|d| (d.clone(), actors.view(d))).collect(),
            post_viewers,
            muted_threads,
            labels: self.load_labels(&labelled).await?,
        };
        let report = normalize::normalize_upstream(&mut view, &overlay);
        self.enrich_counts(&mut view).await?;
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
        // A post whose author turned quoting off (design-v3 §8.3): a valid
        // postgate with a `disableRule`. Not for the author's own view.
        let gated: Vec<String> = sqlx::query_scalar(
            "SELECT post_uri FROM postgates WHERE post_uri = ANY($1) \
               AND embedding_rules @> '[{\"$type\": \"app.bsky.feed.postgate#disableRule\"}]'::jsonb \
               AND author_did = split_part(post_uri, '/', 3) AND rkey = split_part(post_uri, '/', 5)",
        )
        .bind(uris)
        .fetch_all(self.pool())
        .await?;
        for uri in gated {
            if uri_author(&uri) != Some(me) {
                out.entry(uri).or_insert_with(|| json!({}))["embeddingDisabled"] = json!(true);
            }
        }
        // A post the viewer bookmarked (design-v3 §8.2).
        let bookmarked: Vec<String> = sqlx::query_scalar("SELECT post_uri FROM bookmarks WHERE viewer_did = $1 AND post_uri = ANY($2)")
            .bind(me)
            .bind(uris)
            .fetch_all(self.pool())
            .await?;
        for uri in bookmarked {
            out.entry(uri).or_insert_with(|| json!({}))["bookmarked"] = json!(true);
        }
        // A post in a thread the viewer muted (design-v3 §5.3, §8.2).
        for uri in self.in_muted_threads(viewer, uris).await? {
            out.entry(uri).or_insert_with(|| json!({}))["threadMuted"] = json!(true);
        }
        Ok(out)
    }

    /// Constellation counts for targets under the §9.6 rule. Targets with no
    /// usable value are absent (the caller uses local counts).
    pub async fn constellation_counts(&self, req: &Requester, targets: &[String], mode: CountMode) -> HashMap<String, Counts> {
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
                    if !matches!(mode, CountMode::Quiet) {
                        self.enqueue_refresh(t).await;
                    }
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
            CountMode::Quiet => {}
            CountMode::Sync { started } => {
                use futures::StreamExt;
                let fetched: Vec<(String, Option<Counts>)> = futures::stream::iter(misses)
                    .map(|t| async move {
                        if started.elapsed() >= SYNC_BUDGET {
                            return (t, None);
                        }
                        // No budget: the stored or local count is used, and
                        // the request is not marked (design-v3 §II-8.3:
                        // counts never turn a response into a 429).
                        if self.constellation_bucket().is_some_and(|b| b.draw(req.key()).is_err()) {
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

    /// An upstream answering from fixed maps, counting its calls.
    #[derive(Default)]
    pub(crate) struct FixedUpstream {
        /// DID → `profileViewDetailed`.
        pub profiles: HashMap<String, Value>,
        /// URI → `postView`.
        pub posts: HashMap<String, Value>,
        /// DID → `getAuthorFeed` output, keyed with the cursor it answers.
        pub feeds: HashMap<(String, Option<String>), Value>,
        /// What `lists_descending` answers.
        pub descending: bool,
        /// URI → `{uri, cid, value}`.
        pub records: HashMap<String, Value>,
        /// List URI → `list_items` output.
        pub list_items: HashMap<String, Value>,
        /// DID → `getLists` output.
        pub lists: HashMap<String, Value>,
        /// List URI → `getListFeed` output.
        pub list_feeds: HashMap<String, Value>,
        /// Actor DID → `getRelationships` output.
        pub relationships: HashMap<String, Value>,
        /// Pack URI → `getStarterPack` output.
        pub starter_packs: HashMap<String, Value>,
        /// DID → `getActorStarterPacks` output.
        pub actor_packs: HashMap<String, Value>,
        pub calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl UpstreamSource for FixedUpstream {
        async fn profile(&self, _req: &Requester, actor: &str) -> Option<Value> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.profiles.get(actor).cloned()
        }
        async fn posts(&self, _req: &Requester, uris: &[String]) -> Vec<Value> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            uris.iter().filter_map(|u| self.posts.get(u).cloned()).collect()
        }
        async fn post_records(&self, req: &Requester, uris: &[String]) -> Vec<Value> {
            self.posts(req, uris).await
        }
        async fn record(&self, _req: &Requester, uri: &str) -> Option<Value> {
            self.records.get(uri).cloned()
        }
        async fn list_items(&self, _req: &Requester, list: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
            self.list_items.get(list).cloned()
        }
        async fn lists(&self, _req: &Requester, actor: &str, _limit: u32, _cursor: Option<&str>, _purposes: &[String]) -> Option<Value> {
            self.lists.get(actor).cloned()
        }
        async fn list_feed(&self, _req: &Requester, list: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
            self.list_feeds.get(list).cloned()
        }
        async fn relationships(&self, _req: &Requester, actor: &str, _others: &[String]) -> Option<Value> {
            self.relationships.get(actor).cloned()
        }
        async fn starter_pack(&self, _req: &Requester, uri: &str) -> Option<Value> {
            self.starter_packs.get(uri).cloned()
        }
        async fn actor_starter_packs(&self, _req: &Requester, actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
            self.actor_packs.get(actor).cloned()
        }
        async fn post_thread(&self, _req: &Requester, _uri: &str, _depth: u32, _parent_height: u32) -> Option<Value> {
            None
        }
        async fn author_feed(&self, _req: &Requester, actor: &str, _limit: u32, cursor: Option<&str>, _filter: &str) -> Option<Value> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.feeds.get(&(actor.to_string(), cursor.map(str::to_string))).cloned()
        }
        async fn follows(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
            None
        }
        async fn lists_descending(&self, _req: &Requester, _actor: &str) -> bool {
            self.descending
        }
    }

    pub(crate) fn hydrator(pool: &PgPool, sets: ScopeSets) -> Hydrator {
        hydrator_with(pool, sets, Arc::new(NoUpstream))
    }

    /// A hydrator that asks `farsight` about inbound list blocks.
    pub(crate) fn hydrator_with_farsight(pool: &PgPool, sets: ScopeSets, farsight: Farsight) -> Hydrator {
        Hydrator::new(
            pool.clone(),
            ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000),
            None,
            None,
            Arc::new(NoUpstream),
            ReadCache::new(1024 * 1024),
            HydrationConfig {
                farsight: Some(farsight),
                ..test_config()
            },
        )
    }

    /// The settings test hydrators run with: small caps, so a test can
    /// reach them.
    pub(crate) fn test_config() -> HydrationConfig {
        HydrationConfig {
            labeler_did: LABELER.into(),
            counts: CountsConfig::default(),
            thread: ThreadConfig::default(),
            thread_mutes_max: 3,
            activity_subscriptions_max: 2,
            bookmarks_max: 3,
            bookmarks_emit_count: crate::config::BookmarkCount::Never,
            bookmarks_count_floor: 2,
            drafts_max: 2,
            drafts_max_bytes: 2000,
            labels_query_max_patterns: 25,
            labelers_max_dids: 3,
            known_followers_scan_max_pages: 2,
            known_followers_profile_scan_pages: 2,
            known_likers_scan_max_pages: 2,
            thread_v2_max_parents: 80,
            op_thread_max_walk: 4,
            search: crate::config::SearchConfig::default(),
            // Every request computes the pools again, so a test sees
            // what it just wrote.
            suggestions: crate::config::SuggestionsConfig { refresh_secs: 0, ..Default::default() },
            unspecced: crate::config::UnspeccedConfig::default(),
            trends: crate::config::TrendsConfig { cache_ttl_secs: 0, ..Default::default() },
            farsight: None,
            list_budget: Budget { max_references: 2, max_owners: 5 },
            list_applier: None,
            constellation_bucket: None,
        }
    }

    /// A hydrator with no upstream and settings of the test's choosing.
    pub(crate) fn hydrator_configured(pool: &PgPool, sets: ScopeSets, cfg: HydrationConfig) -> Hydrator {
        Hydrator::new(
            pool.clone(),
            ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000),
            None,
            None,
            Arc::new(NoUpstream),
            ReadCache::new(1024 * 1024),
            cfg,
        )
    }

    pub(crate) fn hydrator_with(pool: &PgPool, sets: ScopeSets, upstream: Arc<dyn UpstreamSource>) -> Hydrator {
        hydrator_full(pool, sets, upstream, None)
    }

    /// A hydrator with an upstream and a Constellation at `constellation`
    /// (a fake's base URL).
    pub(crate) fn hydrator_full(pool: &PgPool, sets: ScopeSets, upstream: Arc<dyn UpstreamSource>, constellation: Option<&str>) -> Hydrator {
        Hydrator::new(
            pool.clone(),
            ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000),
            constellation.map(|base| ConstellationClient::new(crate::net::NetClient::loopback_for_tests(), base)),
            None,
            upstream,
            ReadCache::new(1024 * 1024),
            test_config(),
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
