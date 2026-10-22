//! `app.nearhorizon.private.*` handlers (design-v2 §7.9, §6.3, §6.9).
//!
//! - Steps 1–2 (clock, auth gate) run first. Gate failures (401,
//!   `NotEnrolled`, `NotReady`, 429) are ordinary errors outside the floor.
//! - Candidates come from the §6.9 prefilter. Each is read through the
//!   per-record pipeline with in-task concurrency `bind_concurrency`.
//! - Step 13: every response, hit or miss, waits until the endpoint's floor
//!   measured from handler entry, when one is configured: floors are 0 by
//!   default, and at 0 nothing waits (timing-floors design). The response
//!   is built before the floor and returned unchanged. The unfloored time
//!   is exported as `private_read_unfloored_seconds{endpoint}`.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use futures::StreamExt;
use serde_json::{Value, json};

use super::emit::{
    BookmarkView, ContextPost, FeedItem, NotificationRecord, NotificationView, PrivateEmit, PrivateResponse, QuoteItem, ReplyContext,
    ThreadNode, ThreadParent,
};
use super::pipeline::{self, OVER_CLAIM, PipelineEnv, RequestState};
use super::{media, stages};
use super::schema::{self, AuthorFilter, Interaction};
use super::cursor as sealed;
use crate::config::{PrivateReadConfig, PrivateSearchConfig};
use crate::cursor_key::CursorKey;
use crate::db::locks::uri_author;
use crate::firehose::classifier::NSID_POST_PRIVATE;
use crate::hydration::actor::{ProfileContext, ProfileKind};
use crate::hydration::embeds::{EMBED_RECORD_WITH_MEDIA, normalize_embed, private_media_view, quoted_uri};
use crate::hydration::normalize::is_private_uri;
use crate::hydration::{HydrationContext, ts};
use crate::kryphocron_glue::VerifiedCaller;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Params, prepare_private, request_handle, request_ip};
use crate::xrpc::errors::XrpcError;

pub const GET_POSTS: &str = "app.nearhorizon.private.getPosts";
pub const GET_POST_THREAD: &str = "app.nearhorizon.private.getPostThread";
pub const GET_AUTHOR_FEED: &str = "app.nearhorizon.private.getAuthorFeed";
pub const GET_TIMELINE: &str = "app.nearhorizon.private.getTimeline";
pub const CREATE_BOOKMARK: &str = "app.nearhorizon.private.createBookmark";
pub const DELETE_BOOKMARK: &str = "app.nearhorizon.private.deleteBookmark";
pub const GET_BOOKMARKS: &str = "app.nearhorizon.private.getBookmarks";
pub const GET_LIKES: &str = "app.nearhorizon.private.getLikes";
pub const GET_REPOSTED_BY: &str = "app.nearhorizon.private.getRepostedBy";
pub const GET_QUOTES: &str = "app.nearhorizon.private.getQuotes";
pub const LIST_NOTIFICATIONS: &str = "app.nearhorizon.private.listNotifications";
pub const GET_UNREAD_COUNT: &str = "app.nearhorizon.private.getUnreadCount";
pub const UPDATE_SEEN: &str = "app.nearhorizon.private.updateSeen";
pub const GET_NOTIFICATION_PREFERENCES: &str = "app.nearhorizon.private.getNotificationPreferences";
pub const PUT_NOTIFICATION_PREFERENCES: &str = "app.nearhorizon.private.putNotificationPreferences";
pub const GET_REPLIES: &str = "app.nearhorizon.private.getReplies";
pub const SEARCH_POSTS: &str = "app.nearhorizon.private.searchPosts";

/// `getAuthorFeed`'s `filter` values (P-20, §4.14; P-30); the first is the
/// default.
const POSTS_WITH_REPLIES: &str = "posts_with_replies";
const POSTS_AND_AUTHOR_THREADS: &str = "posts_and_author_threads";
const POSTS_WITH_MEDIA: &str = "posts_with_media";
const POSTS_WITH_VIDEO: &str = "posts_with_video";

/// Metric: a media-filter candidate whose embed, as read, lacks the
/// filter's media (private-media-filters design §5), by `filter`.
pub const METRIC_MEDIA_MISMATCH: &str = "private_feed_media_mismatch_total";

/// Metric: handler time before the floor, a histogram (private-threads
/// design §4: B-5 reads its p99).
pub const METRIC_UNFLOORED: &str = "private_read_unfloored_seconds";

/// Metric: the unfloored time by stage (`gate`, `reads`, `context`,
/// `rest`), a histogram (fast-reads design §7.2).
pub const METRIC_STAGES: &str = "private_read_stage_seconds";

/// Metric: a paged read that returned no items and a cursor (fast-reads
/// design §5.2, C-11), by `endpoint`.
pub const METRIC_EMPTY_PAGE: &str = "private_read_empty_page_total";

/// [`METRIC_UNFLOORED`]'s buckets: 20, logarithmic, from 5 ms to 5 s.
#[must_use]
pub fn unfloored_buckets() -> Vec<f64> {
    (0..20).map(|i| 0.005 * 1000f64.powf(f64::from(i) / 19.0)).collect()
}
/// A thread holds at most this many nodes (anchor, parents, replies).
pub(super) const THREAD_NODE_CAP: usize = 200;
/// Replies per thread node.
pub(super) const REPLIES_PER_NODE: usize = 50;

/// The endpoint floors (`private_read.floor_ms.*`).
#[derive(Debug, Clone, Copy)]
pub struct Floors {
    pub get_posts: Duration,
    pub get_post_thread: Duration,
    pub get_author_feed: Duration,
    pub get_timeline: Duration,
    pub get_bookmarks: Duration,
    pub get_likes: Duration,
    pub get_reposted_by: Duration,
    pub get_quotes: Duration,
    pub list_notifications: Duration,
    pub get_unread_count: Duration,
    pub get_replies: Duration,
    pub search_posts: Duration,
}

/// `searchPosts`'s bounded scan (`[private_search]`, private posting §4.17).
#[derive(Debug, Clone, Copy)]
pub(super) struct SearchBounds {
    /// Prefilter-passing rows a page reads at most.
    pub(super) scan_candidates: i64,
    /// The span below the cursor a page scans.
    pub(super) span: TimeDelta,
    /// How far back from the first page's request time a search reaches.
    pub(super) max_age: TimeDelta,
}

impl From<&PrivateSearchConfig> for SearchBounds {
    fn from(cfg: &PrivateSearchConfig) -> Self {
        Self {
            scan_candidates: i64::from(cfg.scan_candidates),
            span: TimeDelta::try_hours(i64::from(cfg.span_hours)).unwrap_or(TimeDelta::MAX),
            max_age: TimeDelta::try_days(i64::from(cfg.max_age_days)).unwrap_or(TimeDelta::MAX),
        }
    }
}

/// State for the private router.
#[derive(Clone)]
pub struct PrivateState {
    pub(super) app: AppState,
    pub(super) env: PipelineEnv,
    pub(super) floors: Floors,
    pub(super) concurrency: usize,
    /// `bookmarks.max_per_actor`, counted over private bookmarks alone
    /// (private posting C-12).
    pub(super) bookmarks_max: u32,
    /// The private cursor key (`APP_PRIVATE_CURSOR_KEY_HEX`, §3.6, C-14).
    pub(super) cursor_key: CursorKey,
    /// `[private_search]` (§4.17).
    pub(super) search: SearchBounds,
    /// `private_read.page_reads_factor` (private-threads design §3.3).
    pub(super) reads_factor: usize,
    /// The walk's cap and the batch bound (`[private_read]`, §3.2.1).
    pub(super) bounds: schema::ReadBounds,
}

impl PrivateState {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        app: AppState,
        glue: crate::kryphocron_glue::PipelineGlue,
        prober: crate::sync::HeadProber,
        watermarks: crate::firehose::watermark::Watermarks,
        freshness_bound: Duration,
        max_head_fetches: usize,
        cfg: &PrivateReadConfig,
        cursor_key: CursorKey,
    ) -> Self {
        let pool = app.hydrator.pool().clone();
        let bookmarks_max = app.hydrator.bookmarks_max();
        Self {
            app,
            env: PipelineEnv {
                pool,
                glue,
                prober,
                watermarks,
                freshness_bound,
                max_head_fetches,
                max_chain: cfg.max_chain,
                plaintext_refused: crate::config::PrivateAnyPdsConfig::default().enabled,
            },
            floors: Floors {
                get_posts: Duration::from_millis(cfg.floor_ms.get_posts),
                get_post_thread: Duration::from_millis(cfg.floor_ms.get_post_thread),
                get_author_feed: Duration::from_millis(cfg.floor_ms.get_author_feed),
                get_timeline: Duration::from_millis(cfg.floor_ms.get_timeline),
                get_bookmarks: Duration::from_millis(cfg.floor_ms.get_bookmarks),
                get_likes: Duration::from_millis(cfg.floor_ms.get_likes),
                get_reposted_by: Duration::from_millis(cfg.floor_ms.get_reposted_by),
                get_quotes: Duration::from_millis(cfg.floor_ms.get_quotes),
                list_notifications: Duration::from_millis(cfg.floor_ms.list_notifications),
                get_unread_count: Duration::from_millis(cfg.floor_ms.get_unread_count),
                get_replies: Duration::from_millis(cfg.floor_ms.get_replies),
                search_posts: Duration::from_millis(cfg.floor_ms.search_posts),
            },
            concurrency: usize::try_from(cfg.bind_concurrency).unwrap_or(8).max(1),
            bookmarks_max,
            cursor_key,
            search: SearchBounds::from(&PrivateSearchConfig::default()),
            reads_factor: usize::try_from(cfg.page_reads_factor).unwrap_or(2).max(1),
            bounds: schema::ReadBounds::new(cfg.max_chain, cfg.walk_batch_max),
        }
    }

    /// `searchPosts` scans by `cfg` (`[private_search]`) rather than the
    /// defaults.
    #[must_use]
    pub(crate) fn with_search(mut self, cfg: &PrivateSearchConfig) -> Self {
        self.search = SearchBounds::from(cfg);
        self
    }

    /// S4 (private-encode design §7.1): while `refused`
    /// (`private_any_pds.enabled`), a plaintext private post is a uniform
    /// miss for every reader. On by default, as the config.
    #[must_use]
    pub(crate) fn with_plaintext_refused(mut self, refused: bool) -> Self {
        self.env.plaintext_refused = refused;
        self
    }
}

fn xrpc(nsid: &str) -> String {
    crate::admin::usage::path("private", nsid)
}

/// The private router, merged into the app by `serve`.
pub fn router(state: PrivateState) -> Router {
    Router::new()
        .route(&xrpc(GET_POSTS), get(get_posts))
        .route(&xrpc(GET_POST_THREAD), get(get_post_thread))
        .route(&xrpc(GET_AUTHOR_FEED), get(get_author_feed))
        .route(&xrpc(GET_TIMELINE), get(get_timeline))
        .route(&xrpc(CREATE_BOOKMARK), post(create_bookmark))
        .route(&xrpc(DELETE_BOOKMARK), post(delete_bookmark))
        .route(&xrpc(GET_BOOKMARKS), get(get_bookmarks))
        .route(&xrpc(GET_LIKES), get(get_likes))
        .route(&xrpc(GET_REPOSTED_BY), get(get_reposted_by))
        .route(&xrpc(GET_QUOTES), get(get_quotes))
        .route(&xrpc(LIST_NOTIFICATIONS), get(list_notifications))
        .route(&xrpc(GET_UNREAD_COUNT), get(get_unread_count))
        .route(&xrpc(UPDATE_SEEN), post(update_seen))
        .route(&xrpc(GET_NOTIFICATION_PREFERENCES), get(get_notification_preferences))
        .route(&xrpc(PUT_NOTIFICATION_PREFERENCES), post(put_notification_preferences))
        .route(&xrpc(GET_REPLIES), get(get_replies))
        .route(&xrpc(SEARCH_POSTS), get(search_posts))
        .with_state(state)
}

/// Step 13: record the unfloored time and its stages (fast-reads design
/// §7.2), then wait out the floor. `gate` is how long the gate took from
/// `start`.
async fn floor(endpoint: &'static str, start: Instant, gate: Duration, timed: &stages::Stages, target: Duration) {
    let unfloored = start.elapsed();
    metrics::histogram!(METRIC_UNFLOORED, "endpoint" => endpoint).record(unfloored.as_secs_f64());
    stages::record(endpoint, unfloored, gate, timed);
    kryphocron::equalize_timing(start, target).await;
}

/// Count `result` when it is a page with no items and a cursor.
fn note_page(endpoint: &'static str, result: &Result<PrivateResponse, XrpcError>) {
    stages::count_empty_page(endpoint, result.as_ref().is_ok_and(PrivateResponse::is_empty_page));
}

/// One private record read through the pipeline for `caller`, timed in the
/// `reads` stage.
async fn read_post(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    uri: &str,
    authors: &ProfileContext,
    start: Instant,
) -> Result<PrivateEmit<Value>, pipeline::Miss> {
    stages::reading(pipeline::read(&s.env, rs, caller, uri, authors, start)).await
}

/// The private-context hydration of a request by `caller` (design-v4 §8.1
/// "Private-context hydration"): the only way these handlers reach the
/// `Hydrator`, so no shared cache, upstream or Farsight is touched.
async fn private_hydration(s: &PrivateState, caller: &VerifiedCaller, ip: IpAddr) -> Result<HydrationContext, XrpcError> {
    let viewer = s.app.hydrator.viewer(Some(&caller.did)).await?;
    let req = request_handle(viewer.state_did(), ip);
    Ok(HydrationContext::private(&s.app.hydrator, viewer, &req))
}

/// Profiles for the owners of `uris` (public data, viewer-relative).
async fn profiles(hydration: &HydrationContext, uris: &[String]) -> Result<ProfileContext, XrpcError> {
    let mut dids: Vec<String> = Vec::new();
    for u in uris {
        if let Some(a) = uri_author(u)
            && !dids.iter().any(|d| d == a)
        {
            dids.push(a.to_string());
        }
    }
    Ok(hydration.profiles(&dids).await?)
}

/// Read `uris` in order with the configured concurrency; misses are `None`.
async fn read_many(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    uris: &[String],
    authors: &ProfileContext,
    start: Instant,
) -> Vec<Option<PrivateEmit<Value>>> {
    // Futures are lazy: building them all first only fixes their borrows.
    let reads: Vec<_> = uris.iter().map(|u| read_post(s, rs, caller, u, authors, start)).collect();
    futures::stream::iter(reads)
        .buffered(s.concurrency)
        .map(Result::ok)
        .collect()
        .await
}

/// Add the operator labels stored for each post (the `labels` table) after
/// its self-labels (private-posting design §3.3).
async fn operator_labels(hydration: &HydrationContext, posts: &mut [&mut PrivateEmit<Value>]) -> Result<(), XrpcError> {
    let uris: Vec<String> = posts.iter().filter_map(|p| view_uri(p)).collect();
    if uris.is_empty() {
        return Ok(());
    }
    let stored = hydration.labels(&uris).await?;
    for post in posts.iter_mut() {
        let Some(extra) = view_uri(post).and_then(|u| stored.get(&u)) else {
            continue;
        };
        let mut labels = post.view().get("labels").and_then(Value::as_array).cloned().unwrap_or_default();
        labels.extend(extra.iter().cloned());
        post.decorate("labels", Value::Array(labels));
    }
    Ok(())
}

fn view_uri(post: &PrivateEmit<Value>) -> Option<String> {
    post.view().get("uri").and_then(Value::as_str).map(str::to_string)
}

/// The stored embed of a post's record, `$type` normalized (§4.2).
fn stored_embed(post: &PrivateEmit<Value>) -> Option<Value> {
    post.view().pointer("/record/embed").map(normalize_embed)
}

/// The media view of a `recordWithMedia` embed's `media`, on the author's
/// PDS from `authors`; a video plays from that PDS (private-video-pds
/// design).
fn embedded_media(post: &PrivateEmit<Value>, embed: &Value, authors: &ProfileContext) -> Option<Value> {
    let did = post.view().pointer("/author/did").and_then(Value::as_str)?;
    let pds = authors.rows.get(did).and_then(|r| r.pds_endpoint.as_deref());
    private_media_view(did, pds, embed.get("media")?)
}

fn not_found_record(uri: &str) -> Value {
    json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true})
}

/// A readable quoted private post as `app.bsky.embed.record#viewRecord`
/// (§4.2): the decoded record as `value`, and its media one level deep, as
/// `record_view` does for public posts.
fn view_record(quoted: &PrivateEmit<Value>, authors: &ProfileContext) -> Value {
    let v = quoted.view();
    let mut record = json!({
        "$type": "app.bsky.embed.record#viewRecord",
        "uri": v["uri"],
        "cid": v["cid"],
        "author": v["author"],
        "value": v["record"],
        "labels": v["labels"],
        "indexedAt": v["indexedAt"],
    });
    // The four counts, as on every private post view (§4.6).
    for key in COUNT_KEYS {
        if let Some(n) = v.get(key) {
            record[key] = n.clone();
        }
    }
    let media = match v.get("embed") {
        Some(media) => Some(media.clone()),
        None => stored_embed(quoted)
            .filter(|e| e.get("$type").and_then(Value::as_str) == Some(EMBED_RECORD_WITH_MEDIA))
            .and_then(|e| embedded_media(quoted, &e, authors)),
    };
    if let Some(media) = media {
        record["embeds"] = json!([media]);
    }
    record
}

/// The count fields of a private post view, in the order they are added.
const COUNT_KEYS: [&str; 4] = ["replyCount", "repostCount", "likeCount", "quoteCount"];

/// The counts on each hit (private-posting design §3.4, §4.6): integers,
/// always present, 0 when none, each counting only what the caller is
/// entitled to know exists, matched by URI.
async fn counts(s: &PrivateState, caller: &VerifiedCaller, posts: &mut [&mut PrivateEmit<Value>]) -> Result<(), XrpcError> {
    let uris: Vec<String> = posts.iter().filter_map(|p| view_uri(p)).collect();
    if uris.is_empty() {
        return Ok(());
    }
    let counts = schema::counts(&s.env.pool, &caller.did, &uris, s.env.max_chain).await?;
    for post in posts.iter_mut() {
        let c = view_uri(post).and_then(|u| counts.get(&u).copied()).unwrap_or_default();
        for (key, n) in COUNT_KEYS.into_iter().zip([c.reply, c.repost, c.like, c.quote]) {
            post.decorate(key, json!(n));
        }
    }
    Ok(())
}

/// `viewer` on each hit (private-posting design §4.6):
/// - `bookmarked`, from the caller's own private bookmarks (§4.3);
/// - `like` and `repost`: the caller's own newest like or repost of the
///   post, by URI, so a refetch and a CID change keep them;
/// - `threadMuted`, when the caller muted the thread root the post claims
///   (C-18): its `reply.root`, or the post itself when top-level. Left out
///   otherwise, as on public views.
async fn viewer_state(s: &PrivateState, caller: &VerifiedCaller, posts: &mut [&mut PrivateEmit<Value>]) -> Result<(), XrpcError> {
    let uris: Vec<String> = posts.iter().filter_map(|p| view_uri(p)).collect();
    if uris.is_empty() {
        return Ok(());
    }
    let pool = &s.env.pool;
    let marked: HashSet<String> = schema::bookmarked(pool, &caller.did, &uris).await?.into_iter().collect();
    let likes = schema::own_interactions(pool, &caller.did, Interaction::Like, &uris).await?;
    let reposts = schema::own_interactions(pool, &caller.did, Interaction::Repost, &uris).await?;
    let roots: Vec<String> = posts.iter().filter_map(|p| claimed_root(p)).collect();
    let muted: HashSet<String> = schema::muted_roots(pool, &caller.did, &roots).await?.into_iter().collect();
    for post in posts.iter_mut() {
        let uri = view_uri(post).unwrap_or_default();
        let mut viewer = json!({"bookmarked": marked.contains(&uri)});
        if let Some(like) = likes.get(&uri) {
            viewer["like"] = json!(like);
        }
        if let Some(repost) = reposts.get(&uri) {
            viewer["repost"] = json!(repost);
        }
        if claimed_root(post).is_some_and(|r| muted.contains(&r)) {
            viewer["threadMuted"] = json!(true);
        }
        post.decorate("viewer", viewer);
    }
    Ok(())
}

/// Metric: the author's own views marked `notServed`, one per view and
/// `reason` (not-served-reason design §4.5).
pub(super) const METRIC_NOT_SERVED: &str = "private_read_not_served_total";

/// `notServed` on the caller's own top-level views (not-served-reason
/// design §4.4): the thread reasons from one `schema::not_served` statement
/// over the page's own posts, then `overClaim` where the pipeline set it.
/// A page with no post by the caller runs nothing and changes nothing, so
/// every other reader's view is as it was. Quoted posts never come here.
async fn not_served(s: &PrivateState, caller: &VerifiedCaller, posts: &mut [&mut PrivateEmit<Value>]) -> Result<(), XrpcError> {
    let me = caller.did.as_str();
    let mut own: Vec<String> = posts.iter().filter_map(|p| view_uri(p)).filter(|u| uri_author(u) == Some(me)).collect();
    if own.is_empty() {
        return Ok(());
    }
    #[cfg(test)]
    {
        use crate::test_support::hooks;
        if hooks::injected_in(hooks::NOT_SERVED_OFF, &s.env.pool, me) {
            return Ok(());
        }
        let _ = hooks::injected_in(hooks::NOT_SERVED_RUN, &s.env.pool, me);
    }
    own.sort_unstable();
    own.dedup();
    let faults = schema::not_served(&s.env.pool, me, &own, s.env.max_chain).await?;
    for post in posts.iter_mut() {
        let Some(uri) = view_uri(post).filter(|u| uri_author(u) == Some(me)) else {
            continue;
        };
        let mut reasons = faults.get(&uri).copied().map(schema::ThreadFault::names).unwrap_or_default();
        let over = post
            .view()
            .get("notServed")
            .and_then(Value::as_array)
            .is_some_and(|a| a.iter().any(|r| r.as_str() == Some(OVER_CLAIM)));
        if over {
            reasons.push(OVER_CLAIM);
        }
        if reasons.is_empty() {
            continue;
        }
        for reason in &reasons {
            let label = match *reason {
                "chainCap" => "chain_cap",
                OVER_CLAIM => "over_claim",
                other => other,
            };
            metrics::counter!(METRIC_NOT_SERVED, "reason" => label).increment(1);
        }
        post.decorate("notServed", json!(reasons));
    }
    Ok(())
}

/// The thread root a post claims (private-posting design §3.1, C-18): its
/// record's `reply.root.uri`, or the post itself when it is top-level. The
/// claim is in the record the reader already reads.
fn claimed_root(post: &PrivateEmit<Value>) -> Option<String> {
    post.view()
        .pointer("/record/reply/root/uri")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| view_uri(post))
}

/// `viewer.replyDisabled` for the posts of the thread rooted at the private
/// post `root`, from its valid private threadgate (§4.4), for the caller.
/// The public rules of design-v3 §8.3 with the private tier's inputs; `None`
/// leaves the field out: no gate, no `allow`, or an answer not known here.
/// The caller has read `root` in this request (C-17).
/// - `followingRule` reads the stored follow only: no Constellation scan in
///   private context, so a missing row makes the input incomplete.
/// - `listRule` reads a locally servable list only; any other is incomplete.
async fn reply_disabled(
    s: &PrivateState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    root: &str,
    gate: Option<&schema::ThreadgateRow>,
) -> Result<Option<bool>, XrpcError> {
    let Some(rules) = gate.and_then(|g| g.allow.as_ref()).and_then(Value::as_array) else {
        return Ok(None);
    };
    let me = caller.did.as_str();
    let author = uri_author(root).unwrap_or_default();
    if me == author {
        return Ok(Some(false));
    }
    if rules.is_empty() {
        return Ok(Some(true));
    }
    let pool = &s.env.pool;
    // Whether every rule's input was complete.
    let mut complete = true;
    for rule in rules {
        let allowed = match rule.get("$type").and_then(Value::as_str).unwrap_or_default() {
            "app.bsky.feed.threadgate#mentionRule" => schema::mentions(pool, root, me).await?,
            "app.bsky.feed.threadgate#followerRule" => schema::follows(pool, me, author).await?,
            "app.bsky.feed.threadgate#followingRule" => {
                let follows = schema::follows(pool, author, me).await?;
                complete &= follows;
                follows
            }
            "app.bsky.feed.threadgate#listRule" => {
                let list = rule.get("list").and_then(Value::as_str).unwrap_or_default();
                match hydration.list_rule_allows(list, me).await? {
                    Some(listed) => listed,
                    None => {
                        complete = false;
                        false
                    }
                }
            }
            // A rule this version does not know.
            _ => {
                complete = false;
                false
            }
        };
        if allowed {
            return Ok(Some(false));
        }
    }
    Ok(complete.then_some(true))
}

/// The gate pass of a page (private-posting design §4.4), over hits only.
/// `read` holds every URI read through the pipeline for this caller in this
/// request: the page's hits and any root a thread handler read.
/// - `viewer.replyDisabled`, from the post's claimed root's private
///   threadgate, **only when that root is in `read`** (C-17). An unread root
///   gives the answer a missing one does: no field.
/// - `viewer.embeddingDisabled`: the post's own valid postgate has a
///   `disableRule` and the caller is not its author.
/// - `threadgate` on a top-level post's view, with its locally servable lists.
///
/// A gate only sets flags: the oracle already decided every hit.
async fn gate_state(
    s: &PrivateState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    posts: &mut [&mut PrivateEmit<Value>],
    read: &HashSet<String>,
) -> Result<(), XrpcError> {
    let uris: Vec<String> = posts.iter().filter_map(|p| view_uri(p)).collect();
    if uris.is_empty() {
        return Ok(());
    }
    let mut roots: Vec<String> = Vec::new();
    for root in posts.iter().filter_map(|p| claimed_root(p)) {
        if read.contains(&root) && is_private_uri(&root) && !roots.contains(&root) {
            roots.push(root);
        }
    }
    let mut gated = roots.clone();
    gated.extend(uris.iter().filter(|u| !roots.contains(u)).cloned());
    let threadgates: HashMap<String, schema::ThreadgateRow> =
        schema::threadgates(&s.env.pool, &gated).await?.into_iter().map(|g| (g.post_uri.clone(), g)).collect();
    let postgates: HashMap<String, schema::PostgateRow> =
        schema::postgates(&s.env.pool, &uris).await?.into_iter().map(|g| (g.post_uri.clone(), g)).collect();
    let mut disabled: HashMap<String, bool> = HashMap::new();
    for root in &roots {
        if let Some(d) = reply_disabled(s, caller, hydration, root, threadgates.get(root)).await? {
            disabled.insert(root.clone(), d);
        }
    }
    for post in posts.iter_mut() {
        let Some(uri) = view_uri(post) else {
            continue;
        };
        let mut viewer = post.view().get("viewer").cloned().unwrap_or_else(|| json!({}));
        if let Some(d) = claimed_root(post).and_then(|r| disabled.get(&r).copied()) {
            viewer["replyDisabled"] = json!(d);
        }
        if postgates.get(&uri).is_some_and(|g| g.embedding_disabled) && uri_author(&uri) != Some(caller.did.as_str()) {
            viewer["embeddingDisabled"] = json!(true);
        }
        post.decorate("viewer", viewer);
        if post.view().pointer("/record/reply").is_none()
            && let Some(g) = threadgates.get(&uri)
        {
            let lists = hydration.threadgate_lists(g.allow.as_ref()).await?;
            post.decorate("threadgate", json!({"uri": g.uri, "cid": g.cid, "record": g.record, "lists": lists}));
        }
    }
    Ok(())
}

/// The decoration pass of a page (private-posting design §3.3), over hits
/// only: the operator labels, the counts (§3.4), `viewer`, the gates
/// (§4.4), then the record part of each `embed`. `roots_read` are the roots
/// a thread handler read for the caller beyond the hits.
///
/// - A quoted private post passes the prefilter first, once for the page
///   (private-threads design §3.4), so an unreadable one costs no read and
///   no head budget. The rest are read through the pipeline for the same
///   reader and the same `RequestState`, all of the page's at once:
///   readable gives `#viewRecord`, with its counts; anything else, a block
///   with its author included, gives `#viewNotFound`, the answer for a URI
///   that names nothing.
/// - A quoted public post comes from local rows, in On mode only. With
///   nothing there the view carries no `embed`, for `record` and
///   `recordWithMedia` alike, and the client builds the quote from
///   `record.embed`.
/// - A readable quoted private post whose own postgate detaches the quoting
///   post gives `#viewDetached`. Detachment is checked only after the read
///   succeeds, so it says nothing about an unreadable post (§4.2).
/// - Quotes are not followed past one level.
#[allow(clippy::too_many_arguments)]
async fn decorate(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    posts: &mut [&mut PrivateEmit<Value>],
    roots_read: &[String],
    start: Instant,
) -> Result<(), XrpcError> {
    operator_labels(hydration, posts).await?;
    counts(s, caller, posts).await?;
    viewer_state(s, caller, posts).await?;
    not_served(s, caller, posts).await?;
    let read: HashSet<String> = posts.iter().filter_map(|p| view_uri(p)).chain(roots_read.iter().cloned()).collect();
    gate_state(s, caller, hydration, posts, &read).await?;
    let quotes: Vec<Option<(Value, String)>> = posts
        .iter()
        .map(|p| {
            let embed = stored_embed(p)?;
            let quoted = quoted_uri(&embed)?.to_string();
            Some((embed, quoted))
        })
        .collect();
    let mut private: Vec<String> = Vec::new();
    for (_, uri) in quotes.iter().flatten() {
        if is_private_uri(uri) && !private.contains(uri) {
            private.push(uri.clone());
        }
    }
    let mut records: HashMap<String, Value> = HashMap::new();
    // By readable quoted post: the quoting URIs its postgate detaches.
    let mut detached: HashMap<String, Vec<String>> = HashMap::new();
    let passing: HashSet<String> = schema::prefilter_uris(&s.env.pool, &caller.did, &private, s.env.max_chain).await?.into_iter().collect();
    private.retain(|u| passing.contains(u));
    if !private.is_empty() {
        let quoted_authors = profiles(hydration, &private).await?;
        let mut hits: Vec<PrivateEmit<Value>> =
            read_many(s, rs, caller, &private, &quoted_authors, start).await.into_iter().flatten().collect();
        let mut refs: Vec<&mut PrivateEmit<Value>> = hits.iter_mut().collect();
        operator_labels(hydration, &mut refs).await?;
        counts(s, caller, &mut refs).await?;
        for q in &hits {
            if let Some(uri) = view_uri(q) {
                records.insert(uri, view_record(q, &quoted_authors));
            }
        }
        let readable: Vec<String> = records.keys().cloned().collect();
        for gate in schema::postgates(&s.env.pool, &readable).await? {
            detached.insert(gate.post_uri, gate.detached);
        }
    }
    // Authors' PDS endpoints, for the media of `recordWithMedia` embeds.
    let with_media: Vec<String> = posts
        .iter()
        .zip(&quotes)
        .filter(|(_, q)| q.as_ref().is_some_and(|(e, _)| e.get("$type").and_then(Value::as_str) == Some(EMBED_RECORD_WITH_MEDIA)))
        .filter_map(|(p, _)| view_uri(p))
        .collect();
    let media_authors = if with_media.is_empty() {
        None
    } else {
        Some(profiles(hydration, &with_media).await?)
    };
    for (post, quote) in posts.iter_mut().zip(quotes) {
        let (Some((embed, quoted)), Some(uri)) = (quote, view_uri(post)) else {
            continue;
        };
        let record = if is_private_uri(&quoted) {
            match records.get(&quoted) {
                Some(_) if detached.get(&quoted).is_some_and(|d| d.contains(&uri)) => {
                    json!({"$type": "app.bsky.embed.record#viewDetached", "uri": &quoted, "detached": true})
                }
                Some(record) => record.clone(),
                None => not_found_record(&quoted),
            }
        } else {
            match hydration.public_record_view(&quoted, &uri).await? {
                Some(view) => view.inner().clone(),
                None => continue,
            }
        };
        let record = json!({"$type": "app.bsky.embed.record#view", "record": record});
        if embed.get("$type").and_then(Value::as_str) == Some(EMBED_RECORD_WITH_MEDIA) {
            let Some(media) = media_authors.as_ref().and_then(|a| embedded_media(post, &embed, a)) else {
                continue;
            };
            post.decorate(
                "embed",
                json!({"$type": "app.bsky.embed.recordWithMedia#view", "record": record, "media": media}),
            );
        } else {
            post.decorate("embed", record);
        }
    }
    Ok(())
}

fn ok(body: PrivateResponse) -> Response {
    Json(body).into_response()
}

pub async fn get_posts(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_POSTS).await?;
    let gate = start.elapsed();
    let requested = Params::parse(q.as_deref()).all("uris");
    if requested.is_empty() || requested.len() > 25 {
        return Err(XrpcError::invalid("Error: uris must have between 1 and 25 items"));
    }
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(posts_body(&s, &caller, ip, &requested, start)).await;
    floor("getPosts", start, gate, &timed, s.floors.get_posts).await;
    result.map(ok)
}

async fn posts_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    requested: &[String],
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let mut wanted: Vec<String> = Vec::new();
    for u in requested {
        if !wanted.contains(u) {
            wanted.push(u.clone());
        }
    }
    let passing: HashSet<String> = schema::prefilter_uris(&s.env.pool, &caller.did, &wanted, s.env.max_chain).await?.into_iter().collect();
    let candidates: Vec<String> = wanted.into_iter().filter(|u| passing.contains(u)).collect();
    let hydration = private_hydration(s, caller, ip).await?;
    let authors = profiles(&hydration, &candidates).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    let mut posts: Vec<PrivateEmit<Value>> =
        read_many(s, &rs, caller, &candidates, &authors, start).await.into_iter().flatten().collect();
    let mut refs: Vec<&mut PrivateEmit<Value>> = posts.iter_mut().collect();
    decorate(s, &rs, caller, &hydration, &mut refs, &[], start).await?;
    Ok(PrivateResponse::posts(posts))
}

fn not_found() -> XrpcError {
    XrpcError::new(StatusCode::BAD_REQUEST, "NotFound", "Post not found")
}

/// `depth` 0–10 (default 6), `parentHeight` 0–30 (default 20).
fn bounded(p: &Params, key: &str, default: u32, max: u32) -> Result<u32, XrpcError> {
    match p.u32(key)?.unwrap_or(default) {
        v if v <= max => Ok(v),
        _ => Err(XrpcError::invalid(format!("Error: {key} must be between 0 and {max}"))),
    }
}

pub async fn get_post_thread(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_POST_THREAD).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let uri = p.required("uri")?.to_string();
    let depth = bounded(&p, "depth", 6, 10)?;
    let parent_height = bounded(&p, "parentHeight", 20, 30)?;
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(thread_body(&s, &caller, ip, &uri, depth, parent_height, start)).await;
    floor("getPostThread", start, gate, &timed, s.floors.get_post_thread).await;
    result.and_then(|t| t.ok_or_else(not_found)).map(ok)
}

/// The parent URI named by an authorized view's `record.reply.parent`.
fn view_parent(post: &PrivateEmit<Value>) -> Option<String> {
    post.view()
        .pointer("/record/reply/parent/uri")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// A row a head round covers (private-threads design §3.4): its URI, its
/// author and its audience's owner, as the pipeline's head stage names
/// them.
struct Covered {
    uri: String,
    author: String,
    audience_owner: Option<String>,
}

impl Covered {
    /// The stored row's head DIDs, from its non-content facts; `None` when
    /// no row is stored.
    async fn of(s: &PrivateState, uri: &str) -> Result<Option<Self>, XrpcError> {
        Ok(schema::post_facts(&s.env.pool, uri).await?.map(|f| Self {
            uri: uri.to_string(),
            audience_owner: f.audience_uri.as_deref().and_then(uri_author).map(str::to_string),
            author: f.owner,
        }))
    }

    /// The DIDs the pipeline's head stage checks for this row and `caller`,
    /// in its order: the author, the reader, the audience's owner when it
    /// differs from the author.
    fn head_dids(&self, caller: &VerifiedCaller) -> Vec<String> {
        let mut dids = vec![self.author.clone(), caller.did.clone()];
        if let Some(a) = self.audience_owner.as_ref().filter(|a| **a != self.author) {
            dids.push(a.clone());
        }
        dids
    }

    /// Whether the pipeline is ready for this row (step 3's readiness).
    async fn ready(&self, s: &PrivateState) -> Result<bool, XrpcError> {
        Ok(pipeline::ready(&s.env, &self.author, self.audience_owner.as_deref()).await?)
    }
}

/// One head round over `rows`, the anchor first (§3.4 step 4): each row's
/// head DIDs in the pipeline's order, repeats dropped at their first
/// position. Returns the DIDs whose check failed on the database.
async fn head_round(s: &PrivateState, rs: &RequestState, caller: &VerifiedCaller, rows: &[Covered]) -> HashSet<String> {
    let dids: Vec<String> = rows.iter().flat_map(|r| r.head_dids(caller)).collect();
    stages::reading(pipeline::prefetch_heads(&s.env, rs, &dids, s.concurrency)).await.db_failed
}

async fn thread_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    uri: &str,
    depth: u32,
    parent_height: u32,
    start: Instant,
) -> Result<Option<PrivateResponse>, XrpcError> {
    let rs = RequestState::new(s.env.max_head_fetches);
    let hydration = private_hydration(s, caller, ip).await?;
    let pool = &s.env.pool;
    // 1. The structural chain: the anchor's row and its ancestors, with no
    // reader. Its rows are not exposed.
    let rows = schema::ancestors(pool, uri, parent_height).await?;
    let claimed = match rows.first() {
        Some((_, _, Some(root), _)) => root.clone(),
        _ => uri.to_string(),
    };
    // 2. One prefilter over the anchor, the ancestors and a private
    // claimed root.
    let mut wanted: Vec<String> = rows.iter().map(|(u, ..)| u.clone()).collect();
    if is_private_uri(&claimed) && !wanted.contains(&claimed) {
        wanted.push(claimed.clone());
    }
    let passing: HashSet<String> = schema::prefilter_uris(pool, &caller.did, &wanted, s.env.max_chain).await?.into_iter().collect();
    #[cfg(test)]
    crate::test_support::hooks::pause(crate::test_support::hooks::THREAD_AFTER_STRUCTURE, uri).await;
    // 3. The watermark, then readiness. An anchor that fails the
    // prefilter, a stale watermark or an anchor that is not ready takes
    // HEAD's sequential path, with no round.
    let anchor_row = match rows.first() {
        Some((u, ..)) if passing.contains(u) && s.env.watermark_fresh() => Covered::of(s, u).await?,
        _ => None,
    };
    let Some(anchor_row) = anchor_row else {
        return thread_sequential(s, &rs, caller, &hydration, uri, depth, parent_height, start).await;
    };
    if !anchor_row.ready(s).await? {
        return thread_sequential(s, &rs, caller, &hydration, uri, depth, parent_height, start).await;
    }
    // The prefix: the longest run of ancestors, nearest first, that pass
    // the prefilter and are ready.
    let mut covered = vec![anchor_row];
    for (u, ..) in rows.iter().skip(1) {
        if !passing.contains(u) {
            break;
        }
        let Some(row) = Covered::of(s, u).await? else { break };
        if !row.ready(s).await? {
            break;
        }
        covered.push(row);
    }
    // 4. One head round, in HEAD's charge order. The claimed root is not in
    // it.
    let db_failed = head_round(s, &rs, caller, &covered).await;
    // 5. The reads, stopping at the first miss of any kind. Every head of
    // the prefix is in the request's cache now, so the reads run together
    // (§3.4's named fallback, `read_many`'s `buffered(concurrency)`), up to
    // the first row that lists a `db_failed` DID, which is not read; then
    // they are taken in order, and every result above the first miss is
    // discarded. The response is the sequential one.
    let touches_failed = |row: &Covered| row.head_dids(caller).iter().any(|d| db_failed.contains(d));
    let uris: Vec<String> = covered.iter().map(|r| r.uri.clone()).collect();
    let authors = profiles(&hydration, &uris).await?;
    if touches_failed(&covered[0]) {
        // HEAD's `Miss::Db` at the anchor: its not-found.
        pipeline::count_db_miss();
        return Ok(None);
    }
    let readable = covered.iter().take_while(|r| !touches_failed(r)).count().min(parent_height as usize + 1).min(THREAD_NODE_CAP);
    let reads: Vec<_> = covered[..readable].iter().map(|r| read_post(s, &rs, caller, &r.uri, &authors, start)).collect();
    let mut reads = futures::stream::iter(reads).buffered(s.concurrency.max(1));
    let Some(Ok(anchor)) = reads.next().await else {
        return Ok(None);
    };
    let mut budget = THREAD_NODE_CAP - 1;
    let mut chain: Vec<PrivateEmit<Value>> = Vec::new();
    let mut next = view_parent(&anchor);
    for row in covered.iter().skip(1) {
        if chain.len() >= parent_height as usize || budget == 0 || next.as_deref() != Some(row.uri.as_str()) {
            break;
        }
        if touches_failed(row) {
            pipeline::count_db_miss();
            break;
        }
        let Some(Ok(p)) = reads.next().await else {
            break;
        };
        budget -= 1;
        next = view_parent(&p);
        chain.push(p);
    }
    drop(reads);
    #[cfg(test)]
    tests::record_chain_state(uri, &rs).await;
    // The public top (review 15, m2): the last structural row's public
    // parent, when every private ancestor was read and fewer than
    // `parentHeight` of them were, as HEAD's loop tests before it looks at
    // the next parent.
    let mut public_top = None;
    let all_read = chain.len() + 1 == rows.len();
    if let Some(parent) = next.filter(|p| !is_private_uri(p))
        && all_read
        && rows.last().and_then(|(_, p, ..)| p.as_deref()) == Some(parent.as_str())
        && chain.len() < parent_height as usize
        && budget > 0
        && let Some(post) = hydration.public_post(&parent).await?
    {
        budget -= 1;
        public_top = Some(post);
    }
    // 6. The claimed root, read after the chain only if the prefilter
    // passed it. An unreadable root is not read.
    let chain_has_root = chain.iter().any(|p| view_uri(p).as_deref() == Some(claimed.as_str()));
    let root_read = if claimed == uri || chain_has_root {
        true
    } else if is_private_uri(&claimed) && passing.contains(&claimed) {
        let authors = profiles(&hydration, std::slice::from_ref(&claimed)).await?;
        read_post(s, &rs, caller, &claimed, &authors, start).await.is_ok()
    } else {
        false
    };
    thread_tail(s, &rs, caller, &hydration, uri, anchor, chain, public_top, (claimed, root_read), depth, budget, start).await
}

/// `getPostThread` as HEAD reads it (private-threads design §3.4 step 2):
/// the anchor through the pipeline with its sequential heads, then each
/// parent one at a time, then the claimed root, with no round. Taken when
/// the anchor fails the prefilter, the watermark is stale or the anchor is
/// not ready, so the bytes and the budget of a miss are HEAD's.
#[allow(clippy::too_many_arguments)]
async fn thread_sequential(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    uri: &str,
    depth: u32,
    parent_height: u32,
    start: Instant,
) -> Result<Option<PrivateResponse>, XrpcError> {
    let anchor_uri = vec![uri.to_string()];
    let authors = profiles(hydration, &anchor_uri).await?;
    let Ok(anchor) = read_post(s, rs, caller, uri, &authors, start).await else {
        return Ok(None);
    };
    let mut budget = THREAD_NODE_CAP - 1;

    // Parents: the chain stops at the first miss or at a public parent.
    let mut chain: Vec<PrivateEmit<Value>> = Vec::new();
    let mut public_top = None;
    let mut next = view_parent(&anchor);
    while let Some(parent) = next.take() {
        if chain.len() >= parent_height as usize || budget == 0 {
            break;
        }
        if is_private_uri(&parent) {
            let authors = profiles(hydration, std::slice::from_ref(&parent)).await?;
            let Ok(p) = read_post(s, rs, caller, &parent, &authors, start).await else {
                break;
            };
            budget -= 1;
            next = view_parent(&p);
            chain.push(p);
        } else {
            if let Some(post) = hydration.public_post(&parent).await? {
                budget -= 1;
                public_top = Some(post);
            }
            break;
        }
    }

    // The thread's root, the anchor's claimed one (§3.1, C-18), read once
    // for the caller unless the anchor or the chain already read it, so
    // that its gate applies (§4.4, C-17). A root the caller cannot read
    // hides nothing and disables nothing.
    let root = claimed_root(&anchor).unwrap_or_else(|| uri.to_string());
    let chain_has_root = chain.iter().any(|p| view_uri(p).as_deref() == Some(root.as_str()));
    let root_read = if root == uri || chain_has_root {
        true
    } else if is_private_uri(&root) {
        let authors = profiles(hydration, std::slice::from_ref(&root)).await?;
        read_post(s, rs, caller, &root, &authors, start).await.is_ok()
    } else {
        false
    };
    thread_tail(s, rs, caller, hydration, uri, anchor, chain, public_top, (root, root_read), depth, budget, start).await
}

/// The rest of `getPostThread` once the anchor, the parents and the root
/// are read: the root's hidden replies, the expansion, the decoration and
/// the nesting. `root` is the claimed root and whether it was read.
#[allow(clippy::too_many_arguments)]
async fn thread_tail(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    uri: &str,
    mut anchor: PrivateEmit<Value>,
    mut chain: Vec<PrivateEmit<Value>>,
    public_top: Option<crate::hydration::PublicEmit<Value>>,
    (root, root_read): (String, bool),
    depth: u32,
    mut budget: usize,
    start: Instant,
) -> Result<Option<PrivateResponse>, XrpcError> {
    let chain_has_root = chain.iter().any(|p| view_uri(p).as_deref() == Some(root.as_str()));
    let roots_read: Vec<String> = if root_read && root != uri && !chain_has_root { vec![root.clone()] } else { Vec::new() };
    // The replies the read root's private threadgate hides, except from
    // their own author, left out before they are read (`thread.rs:207-208`).
    let hidden = if root_read { hidden_replies(s, caller, &root).await? } else { HashSet::new() };

    let (mut replies, expanded) = expand(s, rs, caller, hydration, vec![uri.to_string()], depth, &mut budget, &hidden, start).await?;

    // Every node is read; decorate them all at once.
    let mut refs: Vec<&mut PrivateEmit<Value>> = std::iter::once(&mut anchor)
        .chain(chain.iter_mut())
        .chain(replies.values_mut().flatten())
        .collect();
    decorate(s, rs, caller, hydration, &mut refs, &roots_read, start).await?;

    let mut root = build(anchor, uri, &mut replies, &expanded);
    // Nest the parent chain above the anchor, nearest first.
    let mut parent: Option<ThreadParent> = public_top.map(ThreadParent::Public);
    for p in chain.into_iter().rev() {
        parent = Some(ThreadParent::Private(ThreadNode {
            post: p,
            parent: parent.map(Box::new),
            replies: None,
        }));
    }
    root.parent = parent.map(Box::new);
    Ok(Some(PrivateResponse::thread(root)))
}

/// The replies the private threadgate of `root`, a root the caller read in
/// this request (C-17), hides from the caller: all it lists except the
/// caller's own (`thread.rs:207-208`). None for a public root.
async fn hidden_replies(s: &PrivateState, caller: &VerifiedCaller, root: &str) -> Result<HashSet<String>, XrpcError> {
    if !is_private_uri(root) {
        return Ok(HashSet::new());
    }
    Ok(schema::threadgates(&s.env.pool, &[root.to_string()])
        .await?
        .into_iter()
        .flat_map(|g| g.hidden_replies)
        .filter(|reply| uri_author(reply) != Some(caller.did.as_str()))
        .collect())
}

/// The replies below `level`, breadth-first, up to `depth` levels, at most
/// [`REPLIES_PER_NODE`] candidates a node, each read for the caller, while
/// `budget` nodes remain. `hidden` are left out before they are read.
/// Returns the hits by parent URI and the URIs that were expanded.
#[allow(clippy::too_many_arguments)]
async fn expand(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    mut level: Vec<String>,
    depth: u32,
    budget: &mut usize,
    hidden: &HashSet<String>,
    start: Instant,
) -> Result<(HashMap<String, Vec<PrivateEmit<Value>>>, HashSet<String>), XrpcError> {
    let mut replies: HashMap<String, Vec<PrivateEmit<Value>>> = HashMap::new();
    let mut expanded: HashSet<String> = HashSet::new();
    for _ in 0..depth {
        if level.is_empty() || *budget == 0 {
            break;
        }
        let mut batch: Vec<(String, String)> = Vec::new();
        for parent in &level {
            if batch.len() >= *budget {
                break;
            }
            expanded.insert(parent.clone());
            let candidates = schema::reply_candidates(&s.env.pool, &caller.did, parent, REPLIES_PER_NODE, s.bounds).await?;
            for c in candidates {
                if batch.len() >= *budget {
                    break;
                }
                if hidden.contains(&c) {
                    continue;
                }
                batch.push((parent.clone(), c));
            }
        }
        let uris: Vec<String> = batch.iter().map(|(_, c)| c.clone()).collect();
        let authors = profiles(hydration, &uris).await?;
        let results = read_many(s, rs, caller, &uris, &authors, start).await;
        level = Vec::new();
        for ((parent, child), hit) in batch.into_iter().zip(results) {
            if let Some(post) = hit {
                *budget -= 1;
                level.push(child);
                replies.entry(parent).or_default().push(post);
            }
        }
    }
    Ok((replies, expanded))
}

fn build(
    post: PrivateEmit<Value>,
    uri: &str,
    replies: &mut HashMap<String, Vec<PrivateEmit<Value>>>,
    expanded: &HashSet<String>,
) -> ThreadNode {
    let children = replies.remove(uri).unwrap_or_default();
    let nodes = children
        .into_iter()
        .map(|c| {
            let child_uri = c.view().get("uri").and_then(Value::as_str).unwrap_or_default().to_string();
            build(c, &child_uri, replies, expanded)
        })
        .collect();
    ThreadNode {
        post,
        parent: None,
        replies: expanded.contains(uri).then_some(nodes),
    }
}

/// A plain `created_at::key` cursor, for the lists that walk the caller's
/// own rows or public rows (private posting §3.6): bookmarks, likes,
/// reposts and notifications. The feeds' cursors are sealed.
fn cursor(at: DateTime<Utc>, rkey: &str) -> String {
    format!("{}::{rkey}", at.to_rfc3339_opts(SecondsFormat::Micros, true))
}

fn parse_cursor(c: Option<&str>) -> Result<Option<(DateTime<Utc>, String)>, XrpcError> {
    let Some(c) = c.filter(|c| !c.is_empty()) else {
        return Ok(None);
    };
    let (at, rkey) = c.split_once("::").ok_or_else(|| XrpcError::invalid("Error: malformed cursor"))?;
    let at = DateTime::parse_from_rfc3339(at).map_err(|_| XrpcError::invalid("Error: malformed cursor"))?;
    Ok(Some((at.with_timezone(&Utc), rkey.to_string())))
}

pub async fn get_author_feed(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_AUTHOR_FEED).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let actor = p.required("actor")?.to_string();
    if !actor.starts_with("did:") {
        return Err(XrpcError::invalid("Error: actor must be a valid did"));
    }
    let limit = feed_limit(&p)?;
    // P-20 (§4.14): normalized, so an absent and an explicit default give
    // the same cursor binding (§3.6).
    let (filter, by) = match p.one("filter").unwrap_or(POSTS_WITH_REPLIES) {
        f @ POSTS_WITH_REPLIES => (f, AuthorFilter::Replies),
        f @ POSTS_AND_AUTHOR_THREADS => (f, AuthorFilter::OwnThreads),
        f @ POSTS_WITH_MEDIA => (f, AuthorFilter::Media),
        f @ POSTS_WITH_VIDEO => (f, AuthorFilter::Video),
        _ => {
            return Err(XrpcError::invalid(
                "Error: filter must be posts_with_replies, posts_and_author_threads, posts_with_media or posts_with_video",
            ));
        }
    };
    let aad = sealed::aad(GET_AUTHOR_FEED, &caller.did, &actor, filter);
    let after = sealed::open(&s.cursor_key, &aad, p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let of = FeedOf::Author { actor: &actor, filter: by };
    let (result, timed) = stages::run(feed_body(&s, &caller, ip, of, limit, after, &aad, start)).await;
    floor("getAuthorFeed", start, gate, &timed, s.floors.get_author_feed).await;
    note_page("getAuthorFeed", &result);
    result.map(ok)
}

/// `limit` 1–50, default 30: the feeds and `getReplies`.
fn feed_limit(p: &Params) -> Result<usize, XrpcError> {
    match p.u32("limit")?.unwrap_or(30) {
        l @ 1..=50 => Ok(l as usize),
        _ => Err(XrpcError::invalid("Error: limit must be between 1 and 50")),
    }
}

/// `app.nearhorizon.private.getTimeline`: the private posts the caller may
/// read, by the caller and by the accounts the caller follows, newest
/// first. A post the caller may not read leaves no trace, and a page with
/// none takes as long as a page with some.
pub async fn get_timeline(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_TIMELINE).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let limit = feed_limit(&p)?;
    let aad = sealed::aad(GET_TIMELINE, &caller.did, "", "");
    let after = sealed::open(&s.cursor_key, &aad, p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(feed_body(&s, &caller, ip, FeedOf::Timeline, limit, after, &aad, start)).await;
    floor("getTimeline", start, gate, &timed, s.floors.get_timeline).await;
    note_page("getTimeline", &result);
    result.map(ok)
}

/// Whose feed [`feed_body`] pages.
#[derive(Debug, Clone, Copy)]
enum FeedOf<'a> {
    /// The caller's home timeline.
    Timeline,
    /// `actor`'s feed under `filter`.
    Author { actor: &'a str, filter: AuthorFilter },
}

/// One page of a feed: `actor`'s posts, or with no actor the caller's
/// home timeline, each with its repost items (§4.7). The candidates only
/// narrow the work; `pipeline::read` decides each post.
///
/// The read cap (private-threads design §3.3): a page reads at most
/// `page_reads_factor × limit` candidates through the pipeline, the first
/// rows that pass the prefilter, in batches (§3.2.1), plus one row of
/// lookahead that tells whether more remain. When the reads run out before
/// the page is full, the page ends there with a cursor. No outcome depends
/// on elapsed time.
///
/// The cursor is sealed (private posting §3.6, P-17), bound to `aad`, and
/// holds the last **examined** candidate's position. It is present
/// whenever candidates remain past that row, so a short or empty page can
/// carry one (C-9). Candidates are rows that passed the prefilter, so only
/// posts the caller is entitled to can produce one; a taken-down or hidden
/// post cannot. A position too long to seal (§11) gives no cursor.
///
/// A repost item is its subject read for the caller, with `reason`
/// (`app.bsky.feed.defs#reasonRepost`: `by` the reposter, `uri` the
/// repost, `indexedAt` its time). A subject that misses leaves no trace of
/// the repost.
#[allow(clippy::too_many_arguments)]
async fn feed_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    of: FeedOf<'_>,
    limit: usize,
    after: Option<(DateTime<Utc>, String)>,
    aad: &str,
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let max_reads = limit * s.reads_factor;
    // One lookahead row tells whether prefiltered candidates remain.
    let need = max_reads + 1;
    let hydration = private_hydration(s, caller, ip).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    let (pool, me, bounds) = (&s.env.pool, caller.did.as_str(), s.bounds);
    let first = i64::try_from(need).unwrap_or(i64::MAX);
    let key = |c: &schema::FeedCandidate| (c.created_at, c.key.clone());
    let mut candidates = match of {
        FeedOf::Author { actor, filter } => {
            schema::passing(need, first, bounds.batch_max, after, key, move |after, n| {
                schema::author_batch(pool, me, actor, filter, after, n, bounds.max_chain)
            })
            .await?
        }
        FeedOf::Timeline => {
            schema::passing(need, first, bounds.batch_max, after, key, move |after, n| {
                schema::timeline_batch(pool, me, after, n, bounds.max_chain)
            })
            .await?
        }
    };
    let beyond = candidates.len() > max_reads;
    candidates.truncate(max_reads);

    let mut feed: Vec<PrivateEmit<Value>> = Vec::new();
    // By feed item: the repost it stands for, `(uri, reposter, time)`.
    let mut reposts: Vec<Option<(String, String, DateTime<Utc>)>> = Vec::new();
    // The last examined candidate's position.
    let mut last: Option<(DateTime<Utc>, String)> = None;
    let uris: Vec<String> = candidates.iter().map(|c| c.uri.clone()).collect();
    let authors = profiles(&hydration, &uris).await?;
    let mut full_at: Option<usize> = None;
    // A media filter's bit: the marker only narrows the candidates; the
    // post as read decides (private-media-filters design §5).
    let media_filter = match of {
        FeedOf::Author { filter, .. } => filter.media_bit().map(|bit| (bit, if bit == media::IMAGE { POSTS_WITH_MEDIA } else { POSTS_WITH_VIDEO })),
        FeedOf::Timeline => None,
    };
    let reads: Vec<_> = uris.iter().map(|u| read_post(s, &rs, caller, u, &authors, start)).collect();
    let mut reads = futures::stream::iter(reads).buffered(s.concurrency).enumerate();
    while let Some((i, hit)) = reads.next().await {
        let c = &candidates[i];
        last = Some((c.created_at, c.key.clone()));
        if let Ok(post) = hit {
            if let Some((bit, name)) = media_filter
                && (post.view().pointer("/record/embed").map_or(0, media::kinds_of_embed) & bit) == 0
            {
                metrics::counter!(METRIC_MEDIA_MISMATCH, "filter" => name).increment(1);
                continue;
            }
            feed.push(post);
            reposts.push(c.repost_uri.clone().zip(c.reposter.clone()).map(|(u, by)| (u, by, c.created_at)));
            if feed.len() == limit {
                full_at = Some(i);
                break;
            }
        }
    }
    drop(reads);
    // A short page examined every candidate it read; candidates remain
    // only behind the lookahead row.
    let more = full_at.map_or(beyond, |i| i + 1 < candidates.len() || beyond);
    let cursor = last.filter(|_| more).and_then(|(at, key)| sealed::seal(&s.cursor_key, aad, at, &key));
    let mut refs: Vec<&mut PrivateEmit<Value>> = feed.iter_mut().collect();
    decorate(s, &rs, caller, &hydration, &mut refs, &[], start).await?;
    // The reply context, once every item is read and decorated (fast-reads
    // design §5.2 B-2), before the repost reasons: a context view is the
    // post's own view.
    let replies = stages::context(reply_context(s, &rs, caller, &hydration, &feed, limit, start)).await?;
    let mut reposters: Vec<String> = Vec::new();
    for (_, by, _) in reposts.iter().flatten() {
        if !reposters.contains(by) {
            reposters.push(by.clone());
        }
    }
    if !reposters.is_empty() {
        let by = hydration.profiles(&reposters).await?;
        for (post, repost) in feed.iter_mut().zip(&reposts) {
            if let Some((uri, reposter, at)) = repost {
                post.decorate(
                    "reason",
                    json!({"$type": "app.bsky.feed.defs#reasonRepost", "by": by.view(reposter, ProfileKind::Basic),
                           "uri": uri, "indexedAt": ts(*at)}),
                );
            }
        }
    }
    let items = feed.into_iter().zip(replies).map(|(post, reply)| FeedItem { post, reply }).collect();
    Ok(PrivateResponse::feed(items, cursor))
}

/// A strong ref's `(uri, cid)`; the cid absent on rows stored before it was
/// kept.
fn strong_ref(r: &Value) -> Option<(&str, Option<&str>)> {
    Some((r.get("uri")?.as_str()?, r.get("cid").and_then(Value::as_str)))
}

/// The reply context of a feed page's items (fast-reads design §5.2 B-2,
/// C-9): for each item whose record names `reply`, its root and parent.
/// - A private root or parent already an item of the page is that item's
///   view again, not read again. The others, each once, at most `2 ×
///   limit`, pass the prefilter and are read through the pipeline for the
///   caller in the request's `RequestState`, after every item, so an item's
///   head-fetch funding is what it was; they are not charged to the page's
///   read cap. Every check of a read applies: the clear-field check,
///   `droppedFields` on the author's own view, S4. A hit is decorated as
///   `getPosts` decorates (labels, counts, `viewer`, gates, the record part
///   of `embed`).
/// - A private one that does not read, whatever the reason, is
///   `#notFoundPost`, the answer for a URI that names nothing.
/// - A public one is `#publicPostRef {uri, cid}` from the record's strong
///   ref (C-3): a private handler does not render public posts. Without a
///   stored cid the item carries no `reply`.
#[allow(clippy::too_many_arguments)]
async fn reply_context(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    items: &[PrivateEmit<Value>],
    limit: usize,
    start: Instant,
) -> Result<Vec<Option<ReplyContext>>, XrpcError> {
    let named: Vec<Option<(&Value, &Value)>> = items
        .iter()
        .map(|p| {
            let reply = p.view().pointer("/record/reply")?;
            Some((reply.get("root")?, reply.get("parent")?))
        })
        .collect();
    let on_page: HashMap<String, usize> = items.iter().enumerate().filter_map(|(i, p)| view_uri(p).map(|u| (u, i))).collect();
    let mut wanted: Vec<String> = Vec::new();
    for (root, parent) in named.iter().flatten() {
        for (uri, _) in [root, parent].into_iter().filter_map(|r| strong_ref(r)) {
            if is_private_uri(uri) && !on_page.contains_key(uri) && !wanted.iter().any(|w| w == uri) {
                wanted.push(uri.to_string());
            }
        }
    }
    wanted.truncate(2 * limit);
    let passing: HashSet<String> = schema::prefilter_uris(&s.env.pool, &caller.did, &wanted, s.env.max_chain).await?.into_iter().collect();
    wanted.retain(|u| passing.contains(u));
    let mut read: HashMap<String, PrivateEmit<Value>> = HashMap::new();
    if !wanted.is_empty() {
        let authors = profiles(hydration, &wanted).await?;
        let mut hits: Vec<PrivateEmit<Value>> = read_many(s, rs, caller, &wanted, &authors, start).await.into_iter().flatten().collect();
        // The page's items were read for the caller in this request too.
        let page: Vec<String> = on_page.keys().cloned().collect();
        let mut refs: Vec<&mut PrivateEmit<Value>> = hits.iter_mut().collect();
        decorate(s, rs, caller, hydration, &mut refs, &page, start).await?;
        for hit in hits {
            if let Some(uri) = view_uri(&hit) {
                read.insert(uri, hit);
            }
        }
    }
    let side = |r: &Value| -> Option<ContextPost> {
        let (uri, cid) = strong_ref(r)?;
        if !is_private_uri(uri) {
            return Some(ContextPost::Public { uri: uri.to_string(), cid: cid?.to_string() });
        }
        Some(match on_page.get(uri).map(|&i| &items[i]).or_else(|| read.get(uri)) {
            Some(post) => ContextPost::Private(post.duplicate()),
            None => ContextPost::NotFound(uri.to_string()),
        })
    };
    Ok(named
        .into_iter()
        .map(|n| {
            let (root, parent) = n?;
            Some(ReplyContext { root: side(root)?, parent: side(parent)? })
        })
        .collect())
}

/// Whether `uri` is `at://<did>/tools.kryphocron.feed.postPrivate/<rkey>`,
/// decided from the URI alone (private posting §4.3).
fn is_post_private_uri(uri: &str) -> bool {
    let mut parts = uri.strip_prefix("at://").unwrap_or_default().split('/');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(did), Some(NSID_POST_PRIVATE), Some(rkey), None)
            if kryphocron::Did::new(did).is_ok() && kryphocron::RecordKey::new(rkey).is_ok()
    )
}

/// A required string field of a write's JSON input.
fn field<'a>(input: &'a Value, name: &str) -> Result<&'a str, XrpcError> {
    input
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| XrpcError::invalid(format!("Error: Input must have the property \"{name}\"")))
}

/// The bookmark URI of a write, checked from the URI alone.
fn bookmark_uri(input: &Value) -> Result<&str, XrpcError> {
    let uri = field(input, "uri")?;
    if !is_post_private_uri(uri) {
        return Err(XrpcError::invalid(format!("Error: uri must be a {NSID_POST_PRIVATE} record")));
    }
    Ok(uri)
}

/// `app.nearhorizon.private.createBookmark {uri, cid}` (private posting
/// §4.3): the post is **not** read, so the answer and its time are the same
/// whether it exists or not. Idempotent: a repeat replaces the stored CID
/// and keeps the bookmark's place. The cap is `bookmarks.max_per_actor`,
/// counted over private bookmarks alone (C-12). No floor: nothing here
/// depends on a private post.
pub async fn create_bookmark(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, XrpcError> {
    let caller = prepare_private(&s.app, &headers, peer, CREATE_BOOKMARK).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let uri = bookmark_uri(&input)?;
    let cid = field(&input, "cid")?;
    let (held, existing) = schema::bookmarks_held(&s.env.pool, &caller.did, uri).await?;
    if !existing && held >= i64::from(s.bookmarks_max) {
        return Err(XrpcError::invalid("bookmark limit reached"));
    }
    schema::put_bookmark(&s.env.pool, &caller.did, uri, cid).await?;
    Ok(ok(PrivateResponse::empty()))
}

/// `app.nearhorizon.private.deleteBookmark {uri}`: idempotent, with
/// `createBookmark`'s URI check.
pub async fn delete_bookmark(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, XrpcError> {
    let caller = prepare_private(&s.app, &headers, peer, DELETE_BOOKMARK).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let uri = bookmark_uri(&input)?;
    schema::delete_bookmark(&s.env.pool, &caller.did, uri).await?;
    Ok(ok(PrivateResponse::empty()))
}

/// `app.nearhorizon.private.getBookmarks {limit 1-100 default 50, cursor}`:
/// the caller's bookmarks, newest first, each post read through the
/// pipeline. An unreadable one is omitted. The cursor is
/// `created_at::post_uri` of the last **row** scanned, present when the
/// row page was full, so omissions do not move it: it walks the caller's own
/// rows, which say nothing about any post. Floored (`get_bookmarks`).
pub async fn get_bookmarks(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_BOOKMARKS).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let limit = match p.u32("limit")?.unwrap_or(50) {
        l @ 1..=100 => l,
        _ => return Err(XrpcError::invalid("Error: limit must be between 1 and 100")),
    };
    let after = parse_cursor(p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(bookmarks_body(&s, &caller, ip, limit as usize, after, start)).await;
    floor("getBookmarks", start, gate, &timed, s.floors.get_bookmarks).await;
    note_page("getBookmarks", &result);
    result.map(ok)
}

async fn bookmarks_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    limit: usize,
    after: Option<(DateTime<Utc>, String)>,
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let rows = schema::bookmark_rows(&s.env.pool, &caller.did, after, i64::try_from(limit).unwrap_or(i64::MAX)).await?;
    let next = (rows.len() == limit)
        .then(|| rows.last().map(|(uri, _, at)| cursor(*at, uri)))
        .flatten();
    // SQL narrows; the pipeline decides each post (§2 rule 1).
    let uris: Vec<String> = rows.iter().map(|(uri, ..)| uri.clone()).collect();
    let passing: HashSet<String> = schema::prefilter_uris(&s.env.pool, &caller.did, &uris, s.env.max_chain).await?.into_iter().collect();
    let candidates: Vec<String> = uris.into_iter().filter(|u| passing.contains(u)).collect();
    let hydration = private_hydration(s, caller, ip).await?;
    let authors = profiles(&hydration, &candidates).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    let mut hits: HashMap<String, PrivateEmit<Value>> = HashMap::new();
    for (uri, hit) in candidates.iter().zip(read_many(s, &rs, caller, &candidates, &authors, start).await) {
        if let Some(post) = hit {
            hits.insert(uri.clone(), post);
        }
    }
    let mut bookmarks: Vec<BookmarkView> = rows
        .into_iter()
        .filter_map(|(uri, cid, at)| {
            let item = hits.remove(&uri)?;
            Some(BookmarkView {
                created_at: ts(at),
                uri,
                cid,
                item,
            })
        })
        .collect();
    let mut refs: Vec<&mut PrivateEmit<Value>> = bookmarks.iter_mut().map(|b| &mut b.item).collect();
    decorate(s, &rs, caller, &hydration, &mut refs, &[], start).await?;
    Ok(PrivateResponse::bookmarks(bookmarks, next))
}

/// `limit` 1–100, default 50 (private posting §4.7).
fn page_limit(p: &Params) -> Result<usize, XrpcError> {
    match p.u32("limit")?.unwrap_or(50) {
        l @ 1..=100 => Ok(l as usize),
        _ => Err(XrpcError::invalid("Error: limit must be between 1 and 100")),
    }
}

/// Whether `uri` reads for the caller: the anchor of `getLikes`,
/// `getRepostedBy` and `getQuotes` (private posting §4.7), read through the
/// pipeline as any private post is. A miss, whatever its reason (not
/// readable, not private, unknown), gives the same empty answer.
async fn anchor_readable(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    uri: &str,
    start: Instant,
) -> Result<bool, XrpcError> {
    let authors = profiles(hydration, &[uri.to_string()]).await?;
    Ok(read_post(s, rs, caller, uri, &authors, start).await.is_ok())
}

/// `app.nearhorizon.private.getLikes {uri, limit 1-100 default 50, cursor}`
/// (private posting §4.7): `app.bsky.feed.getLikes`'s output for a private
/// post the caller reads, by member likers with no block either way with
/// the caller (C-2), newest first. On a miss, `{uri, likes: []}`, the same
/// bytes whatever the reason. The cursor walks the like rows, which are
/// public records. Floored (`get_likes`).
pub async fn get_likes(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    interactions(&s, peer, &headers, q.as_deref(), Interaction::Like).await
}

/// `app.nearhorizon.private.getRepostedBy`: [`get_likes`] over reposts,
/// `{uri, cursor?, repostedBy: profileView[]}`. Floored (`get_reposted_by`).
pub async fn get_reposted_by(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    interactions(&s, peer, &headers, q.as_deref(), Interaction::Repost).await
}

async fn interactions(
    s: &PrivateState,
    peer: SocketAddr,
    headers: &HeaderMap,
    q: Option<&str>,
    kind: Interaction,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let (method, endpoint, target) = match kind {
        Interaction::Like => (GET_LIKES, "getLikes", s.floors.get_likes),
        Interaction::Repost => (GET_REPOSTED_BY, "getRepostedBy", s.floors.get_reposted_by),
    };
    let caller = prepare_private(&s.app, headers, peer, method).await?;
    let gate = start.elapsed();
    let p = Params::parse(q);
    let uri = p.required("uri")?.to_string();
    let limit = page_limit(&p)?;
    let after = parse_cursor(p.one("cursor"))?;
    let ip = request_ip(&s.app, headers, peer);
    let (result, timed) = stages::run(interactions_body(s, &caller, ip, kind, &uri, limit, after, start)).await;
    floor(endpoint, start, gate, &timed, target).await;
    note_page(endpoint, &result);
    result.map(ok)
}

#[allow(clippy::too_many_arguments)]
async fn interactions_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    kind: Interaction,
    uri: &str,
    limit: usize,
    after: Option<(DateTime<Utc>, String)>,
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let respond = |items: Vec<Value>, cursor: Option<String>| match kind {
        Interaction::Like => PrivateResponse::likes(uri.to_string(), items, cursor),
        Interaction::Repost => PrivateResponse::reposted_by(uri.to_string(), items, cursor),
    };
    let hydration = private_hydration(s, caller, ip).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    if !anchor_readable(s, &rs, caller, &hydration, uri, start).await? {
        return Ok(respond(Vec::new(), None));
    }
    let rows = schema::interactions(&s.env.pool, &caller.did, kind, uri, after, i64::try_from(limit).unwrap_or(i64::MAX)).await?;
    let next = (rows.len() == limit)
        .then(|| rows.last().map(|(_, at, _, row)| cursor(*at, row)))
        .flatten();
    let mut dids: Vec<String> = Vec::new();
    for (did, ..) in &rows {
        if !dids.contains(did) {
            dids.push(did.clone());
        }
    }
    let actors = hydration.profiles(&dids).await?;
    let items = rows
        .iter()
        .map(|(did, created, indexed, _)| match kind {
            Interaction::Like => {
                json!({"indexedAt": ts(*indexed), "createdAt": ts(*created), "actor": actors.view(did, ProfileKind::View)})
            }
            Interaction::Repost => actors.view(did, ProfileKind::View),
        })
        .collect();
    Ok(respond(items, next))
}

/// `app.nearhorizon.private.getQuotes {uri, limit 1-100 default 50, cursor}`
/// (private posting §4.7) → `{uri, cursor?, posts}`, for a private post the
/// caller reads; on a miss `{uri, posts: []}`. Newest first, merged by
/// `(createdAt, uri)`:
/// - private quoting posts passing the prefilter, each read through the
///   pipeline, as `#privatePostView`;
/// - members' public quoting posts whose author has no block either way
///   with the caller, as `#publicPostRef {uri, cid}` (C-3): Prism may not
///   fetch their bodies from a private handler, and the client hydrates
///   them through its public AppView.
///
/// The read cap and the lookahead as [`feed_body`]. The cursor is sealed (§3.6),
/// bound to the method, the caller and `uri`, and holds the last examined
/// candidate's position: it is present whenever candidates remain, so a
/// short or empty page can carry one. Floored (`get_quotes`).
pub async fn get_quotes(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_QUOTES).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let uri = p.required("uri")?.to_string();
    let limit = page_limit(&p)?;
    let aad = sealed::aad(GET_QUOTES, &caller.did, &uri, "");
    let after = sealed::open(&s.cursor_key, &aad, p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(quotes_body(&s, &caller, ip, &uri, limit, after, &aad, start)).await;
    floor("getQuotes", start, gate, &timed, s.floors.get_quotes).await;
    note_page("getQuotes", &result);
    result.map(ok)
}

#[allow(clippy::too_many_arguments)]
async fn quotes_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    uri: &str,
    limit: usize,
    after: Option<(DateTime<Utc>, String)>,
    aad: &str,
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let hydration = private_hydration(s, caller, ip).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    if !anchor_readable(s, &rs, caller, &hydration, uri, start).await? {
        return Ok(PrivateResponse::quotes(uri.to_string(), Vec::new(), None));
    }
    // The read cap (private-threads design §3.3), with one row of
    // lookahead.
    let max_reads = limit * s.reads_factor;
    let need = max_reads + 1;
    let (pool, me, bounds) = (&s.env.pool, caller.did.as_str(), s.bounds);
    let first = i64::try_from(need).unwrap_or(i64::MAX);
    let key = |c: &schema::QuoteCandidate| (c.created_at, c.uri.clone());
    let mut candidates = schema::passing(need, first, bounds.batch_max, after, key, move |after, n| {
        schema::quote_batch(pool, me, uri, after, n, bounds.max_chain)
    })
    .await?;
    let beyond = candidates.len() > max_reads;
    candidates.truncate(max_reads);
    let mut items: Vec<QuoteItem> = Vec::new();
    // The last examined candidate's position.
    let mut last: Option<(DateTime<Utc>, String)> = None;
    let private: Vec<String> = candidates.iter().filter(|c| c.cid.is_none()).map(|c| c.uri.clone()).collect();
    let authors = profiles(&hydration, &private).await?;
    let reads: Vec<_> = private.iter().map(|u| read_post(s, &rs, caller, u, &authors, start)).collect();
    let mut reads = futures::stream::iter(reads).buffered(s.concurrency);
    let mut full_at: Option<usize> = None;
    for (i, c) in candidates.iter().enumerate() {
        last = Some((c.created_at, c.uri.clone()));
        match &c.cid {
            Some(cid) => items.push(QuoteItem::Public {
                uri: c.uri.clone(),
                cid: cid.clone(),
            }),
            None => {
                if let Some(Ok(post)) = reads.next().await {
                    items.push(QuoteItem::Private(post));
                }
            }
        }
        if items.len() == limit {
            full_at = Some(i);
            break;
        }
    }
    drop(reads);
    let more = full_at.map_or(beyond, |i| i + 1 < candidates.len() || beyond);
    let cursor = last.filter(|_| more).and_then(|(at, key)| sealed::seal(&s.cursor_key, aad, at, &key));
    let mut refs: Vec<&mut PrivateEmit<Value>> = items
        .iter_mut()
        .filter_map(|item| match item {
            QuoteItem::Private(post) => Some(post),
            QuoteItem::Public { .. } => None,
        })
        .collect();
    decorate(s, &rs, caller, &hydration, &mut refs, &[], start).await?;
    Ok(PrivateResponse::quotes(uri.to_string(), items, cursor))
}

/// `app.nearhorizon.private.getReplies {uri, limit 1-50 default 30, cursor,
/// depth 0-6 default 2}` (private posting §4.12, P-6, P-14) →
/// `{replies: #privateThreadView[], cursor?}`: the private replies to
/// `uri`, oldest first.
/// - A private `uri` is read for the caller first. A miss, whatever its
///   reason, gives `{replies: []}`, the answer for a post that does not
///   exist (C-15), as `getPostThread`'s anchor miss ends the thread. Its
///   claimed root (C-18), found by a one-row lookup, is read too when it
///   passes the prefilter, which is the anchor itself when it is top-level,
///   so that the root's private threadgate hides its `hiddenReplies` and
///   sets `replyDisabled` (§4.4, C-17) ([`replies_anchor`]).
/// - A public `uri` is not read or fetched: only its private replies are
///   listed, and nothing about the public post is emitted.
/// - Candidates pass the prefilter. The read cap, the lookahead and the
///   sealed cursor (§3.6), bound to the method, the caller and `uri`, as
///   [`feed_body`].
/// - Each reply is expanded breadth-first `depth` levels below it, at most
///   [`REPLIES_PER_NODE`] children a node and [`THREAD_NODE_CAP`] nodes in
///   the response, as `getPostThread`; deeper levels are reached by calling
///   `getReplies` on a node.
///
/// Floored (`get_replies`).
pub async fn get_replies(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_REPLIES).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let uri = p.required("uri")?.to_string();
    let limit = feed_limit(&p)?;
    let depth = bounded(&p, "depth", 2, 6)?;
    let aad = sealed::aad(GET_REPLIES, &caller.did, &uri, "");
    let after = sealed::open(&s.cursor_key, &aad, p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(replies_body(&s, &caller, ip, &uri, limit, depth, after, &aad, start)).await;
    floor("getReplies", start, gate, &timed, s.floors.get_replies).await;
    note_page("getReplies", &result);
    result.map(ok)
}

/// `getReplies`' private anchor and its claimed root (private-threads
/// design §3.4): `None` when the anchor misses; otherwise the root, when it
/// was read for the caller.
///
/// A one-row lookup with no reader finds the claimed root, the stored
/// `reply_root_uri` the anchor's view emits. One prefilter covers the
/// anchor and a private root. When the anchor passes, the watermark is
/// fresh and the anchor is ready, one head round covers the anchor's DIDs
/// before it is read, and the root is read only if it passed the prefilter.
/// Otherwise the anchor is read as HEAD reads it, and the root is read
/// after it.
async fn replies_anchor(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    uri: &str,
    start: Instant,
) -> Result<Option<Option<String>>, XrpcError> {
    let pool = &s.env.pool;
    let stored = schema::stored_root(pool, uri).await?;
    let claimed = stored.clone().flatten().unwrap_or_else(|| uri.to_string());
    let mut wanted = vec![uri.to_string()];
    if claimed != uri && is_private_uri(&claimed) {
        wanted.push(claimed.clone());
    }
    let passing: HashSet<String> = schema::prefilter_uris(pool, &caller.did, &wanted, s.env.max_chain).await?.into_iter().collect();
    let anchor_row = if stored.is_some() && passing.contains(uri) && s.env.watermark_fresh() {
        Covered::of(s, uri).await?
    } else {
        None
    };
    let anchor_row = match anchor_row {
        Some(row) if row.ready(s).await? => Some(row),
        _ => None,
    };
    let authors = profiles(hydration, &[uri.to_string()]).await?;
    let Some(anchor_row) = anchor_row else {
        // HEAD's path.
        let Ok(anchor) = read_post(s, rs, caller, uri, &authors, start).await else {
            return Ok(None);
        };
        let root = claimed_root(&anchor).unwrap_or_else(|| uri.to_string());
        let read = root == uri || (is_private_uri(&root) && anchor_readable(s, rs, caller, hydration, &root, start).await?);
        return Ok(Some(read.then_some(root)));
    };
    let db_failed = head_round(s, rs, caller, std::slice::from_ref(&anchor_row)).await;
    if anchor_row.head_dids(caller).iter().any(|d| db_failed.contains(d)) {
        pipeline::count_db_miss();
        return Ok(None);
    }
    if read_post(s, rs, caller, uri, &authors, start).await.is_err() {
        return Ok(None);
    }
    let read = claimed == uri
        || (is_private_uri(&claimed) && passing.contains(&claimed) && anchor_readable(s, rs, caller, hydration, &claimed, start).await?);
    Ok(Some(read.then_some(claimed)))
}

#[allow(clippy::too_many_arguments)]
async fn replies_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    uri: &str,
    limit: usize,
    depth: u32,
    after: Option<(DateTime<Utc>, String)>,
    aad: &str,
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let hydration = private_hydration(s, caller, ip).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    // The thread's root, when it was read for the caller in this request.
    let mut roots_read: Vec<String> = Vec::new();
    if is_private_uri(uri) {
        match replies_anchor(s, &rs, caller, &hydration, uri, start).await? {
            Some(root) => roots_read.extend(root),
            None => return Ok(PrivateResponse::replies(Vec::new(), None)),
        }
    }
    let hidden = match roots_read.first() {
        Some(root) => hidden_replies(s, caller, root).await?,
        None => HashSet::new(),
    };
    let hidden_list: Vec<String> = hidden.iter().cloned().collect();

    // The read cap (private-threads design §3.3), with one row of
    // lookahead.
    let max_reads = limit * s.reads_factor;
    let need = max_reads + 1;
    let (pool, me, bounds) = (&s.env.pool, caller.did.as_str(), s.bounds);
    let first = i64::try_from(need).unwrap_or(i64::MAX);
    let key = |(u, at): &(String, DateTime<Utc>)| (*at, u.clone());
    let hidden_ref = &hidden_list;
    let mut candidates = schema::passing(need, first, bounds.batch_max, after, key, move |after, n| {
        schema::reply_page_batch(pool, me, uri, hidden_ref, after, n, bounds.max_chain)
    })
    .await?;
    let beyond = candidates.len() > max_reads;
    candidates.truncate(max_reads);
    let mut hits: Vec<PrivateEmit<Value>> = Vec::new();
    // The last examined candidate's position.
    let mut last: Option<(DateTime<Utc>, String)> = None;
    let uris: Vec<String> = candidates.iter().map(|(u, _)| u.clone()).collect();
    let authors = profiles(&hydration, &uris).await?;
    let mut full_at: Option<usize> = None;
    let reads: Vec<_> = uris.iter().map(|u| read_post(s, &rs, caller, u, &authors, start)).collect();
    let mut reads = futures::stream::iter(reads).buffered(s.concurrency).enumerate();
    while let Some((i, hit)) = reads.next().await {
        let (u, at) = &candidates[i];
        last = Some((*at, u.clone()));
        if let Ok(post) = hit {
            hits.push(post);
            if hits.len() == limit {
                full_at = Some(i);
                break;
            }
        }
    }
    drop(reads);
    let more = full_at.map_or(beyond, |i| i + 1 < candidates.len() || beyond);
    let cursor = last.filter(|_| more).and_then(|(at, key)| sealed::seal(&s.cursor_key, aad, at, &key));

    let mut budget = THREAD_NODE_CAP.saturating_sub(hits.len());
    let level: Vec<String> = hits.iter().filter_map(view_uri).collect();
    let (mut replies, expanded) = expand(s, &rs, caller, &hydration, level, depth, &mut budget, &hidden, start).await?;
    let mut refs: Vec<&mut PrivateEmit<Value>> = hits.iter_mut().chain(replies.values_mut().flatten()).collect();
    decorate(s, &rs, caller, &hydration, &mut refs, &roots_read, start).await?;
    let nodes = hits
        .into_iter()
        .map(|hit| {
            let hit_uri = view_uri(&hit).unwrap_or_default();
            build(hit, &hit_uri, &mut replies, &expanded)
        })
        .collect();
    Ok(PrivateResponse::replies(nodes, cursor))
}

/// A search's `q` (private posting §4.17): the case-folded words a post's
/// decoded text must all contain, and the `from:<did>` authors it must have.
#[derive(Debug, Default, PartialEq, Eq)]
struct SearchQuery {
    words: Vec<String>,
    from: Vec<String>,
}

impl SearchQuery {
    fn parse(q: &str) -> Result<Self, XrpcError> {
        let mut query = Self::default();
        for token in q.split_whitespace() {
            match token.strip_prefix("from:") {
                Some(did) if did.starts_with("did:") => query.from.push(did.to_string()),
                Some(_) => return Err(XrpcError::invalid("Error: from: must name a did")),
                None => query.words.push(token.to_lowercase()),
            }
        }
        if query.words.is_empty() && query.from.is_empty() {
            return Err(XrpcError::invalid("Error: q must hold at least one word"));
        }
        Ok(query)
    }

    /// Whether a post at `uri` can match at all: its author is every
    /// `from:` DID. Decided from the URI, before any read.
    fn may_match(&self, uri: &str) -> bool {
        self.from.iter().all(|did| uri_author(uri) == Some(did.as_str()))
    }

    /// Whether a post read for the caller matches. The decoded text lives
    /// only here, in the handler, for this request.
    fn matches(&self, post: &PrivateEmit<Value>) -> bool {
        let text = post.view().pointer("/record/text").and_then(Value::as_str).unwrap_or_default().to_lowercase();
        self.words.iter().all(|w| text.contains(w.as_str()))
    }
}

/// `t` at the microsecond, the precision a cursor and the database keep.
fn micros(t: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(t.timestamp_micros()).unwrap_or(t)
}

/// `at − by`, clamped to the earliest time the database stores well.
fn before(at: DateTime<Utc>, by: TimeDelta) -> DateTime<Utc> {
    let year_one = DateTime::from_timestamp(-62_135_596_800, 0).unwrap_or(DateTime::<Utc>::MIN_UTC);
    at.checked_sub_signed(by).unwrap_or(year_one).max(year_one)
}

/// `app.nearhorizon.private.searchPosts {q, sort: latest|top, limit 1-100
/// default 25, cursor}` (private posting §4.17, P-7, C-8) → `{posts:
/// #privatePostView[], cursor?}`: a bounded scan at query time. No index of
/// decoded text exists; each candidate is read through the pipeline and
/// matched in the handler.
/// - A page scans the next `scan_candidates` rows that pass the prefilter
///   for the caller, newest first, strictly below the cursor's
///   `(created_at, uri)` pair and no older than `span` below its
///   `created_at`, nor than the search's bound. The first page starts at
///   `(request time, '')`, and fixes the bound at `request time −
///   max_age`; the sealed cursor carries it on (§3.6).
/// - Rows are read newest first and kept when their text contains every
///   word of `q`, case-folded, and their author is every `from:` DID, up to
///   `limit`.
/// - The cursor is the last returned row when the page reached `limit`;
///   else the last scanned row when `scan_candidates` were found; else the
///   span's lower edge `(at − span, '')`. It is absent exactly when fewer
///   than `scan_candidates` were found and that edge reached the bound. So
///   where a page ends, and whether a cursor follows, depend only on rows
///   the caller is entitled to and on the clock.
/// - `sort: "top"` is answered in `latest` order: there is nothing to rank.
///
/// The AAD binds the cursor to the caller and `q` as given. Floored
/// (`search_posts`); charged 5 tokens of the `private` bucket
/// (`rate_limit::COSTS`).
pub async fn search_posts(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, SEARCH_POSTS).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let q = p.required("q")?.to_string();
    let query = SearchQuery::parse(&q)?;
    if !matches!(p.one("sort").unwrap_or("latest"), "latest" | "top") {
        return Err(XrpcError::invalid("Error: sort must be latest or top"));
    }
    let limit = match p.u32("limit")?.unwrap_or(25) {
        l @ 1..=100 => l as usize,
        _ => return Err(XrpcError::invalid("Error: limit must be between 1 and 100")),
    };
    let aad = sealed::aad(SEARCH_POSTS, &caller.did, &q, "");
    let from = sealed::open_search(&s.cursor_key, &aad, p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(search_body(&s, &caller, ip, &query, limit, from, &aad, start)).await;
    floor("searchPosts", start, gate, &timed, s.floors.search_posts).await;
    note_page("searchPosts", &result);
    result.map(ok)
}

#[allow(clippy::too_many_arguments)]
async fn search_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    query: &SearchQuery,
    limit: usize,
    from: Option<sealed::SearchPosition>,
    aad: &str,
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let bounds = s.search;
    let pos = from.unwrap_or_else(|| {
        let now = micros(Utc::now());
        sealed::SearchPosition {
            at: now,
            uri: String::new(),
            bound: before(now, bounds.max_age),
        }
    });
    let lower = before(pos.at, bounds.span).max(pos.bound);
    // The next `scan_candidates` rows that pass the prefilter, in batches
    // (private-threads design §3.2.1).
    let (pool, me, read) = (&s.env.pool, caller.did.as_str(), s.bounds);
    let need = usize::try_from(bounds.scan_candidates).unwrap_or(usize::MAX);
    let start_at = (pos.at, pos.uri.clone());
    let key = |(u, at): &(String, DateTime<Utc>)| (*at, u.clone());
    let candidates = schema::passing(need, bounds.scan_candidates, read.batch_max, Some(start_at.clone()), key, move |after, n| {
        let from = after.unwrap_or_else(|| start_at.clone());
        schema::search_batch(pool, me, from, lower, n, read.max_chain)
    })
    .await?;
    let scanned_all = i64::try_from(candidates.len()).unwrap_or(i64::MAX) >= bounds.scan_candidates;

    let hydration = private_hydration(s, caller, ip).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    let wanted: Vec<&(String, DateTime<Utc>)> = candidates.iter().filter(|(u, _)| query.may_match(u)).collect();
    let uris: Vec<String> = wanted.iter().map(|(u, _)| u.clone()).collect();
    let authors = profiles(&hydration, &uris).await?;
    let mut posts: Vec<PrivateEmit<Value>> = Vec::new();
    // The last returned row, once the page holds `limit`.
    let mut full_at: Option<(DateTime<Utc>, String)> = None;
    let reads: Vec<_> = uris.iter().map(|u| read_post(s, &rs, caller, u, &authors, start)).collect();
    let mut reads = futures::stream::iter(reads).buffered(s.concurrency).enumerate();
    while let Some((i, hit)) = reads.next().await {
        if let Ok(post) = hit
            && query.matches(&post)
        {
            posts.push(post);
            if posts.len() == limit {
                let (uri, at) = wanted[i];
                full_at = Some((*at, uri.clone()));
                break;
            }
        }
    }
    drop(reads);

    let next = match (full_at, candidates.last()) {
        (Some((at, uri)), _) => Some((at, uri)),
        (None, Some((uri, at))) if scanned_all => Some((*at, uri.clone())),
        _ if lower > pos.bound => Some((lower, String::new())),
        _ => None,
    };
    let cursor = next.and_then(|(at, uri)| {
        sealed::seal_search(&s.cursor_key, aad, &sealed::SearchPosition { at, uri, bound: pos.bound })
    });
    let mut refs: Vec<&mut PrivateEmit<Value>> = posts.iter_mut().collect();
    decorate(s, &rs, caller, &hydration, &mut refs, &[], start).await?;
    Ok(PrivateResponse::search(posts, cursor))
}

/// The `indexed_at::id` cursor of a notification page (plain: it walks the
/// caller's own rows, private posting §3.6).
fn notification_cursor(c: Option<&str>) -> Result<Option<(DateTime<Utc>, i64)>, XrpcError> {
    let Some((at, id)) = parse_cursor(c)? else {
        return Ok(None);
    };
    let id = id.parse().map_err(|_| XrpcError::invalid("Error: malformed cursor"))?;
    Ok(Some((at, id)))
}

/// `app.nearhorizon.private.listNotifications {limit 1-100 default 50,
/// cursor, reasons?}` (private posting §4.8) → `{notifications, cursor?,
/// seenAt?}`, each item in the shape of
/// `app.bsky.notification.listNotifications#notification`:
/// 1. the caller's rows are scanned, newest first, with the `reasons`
///    filter, the caller's notification preferences and the plain
///    `indexed_at::id` cursor;
/// 2. after the scan, a row is dropped for a block either way with its
///    author (`blocks` and `private_blocks`), a mute of its author (full,
///    or scoped by reason), a moderation list that applies, a muted
///    thread, or the caller's notification preferences (checked again);
/// 3. a row from a private post reads the post through the pipeline for
///    the caller: a miss drops it, and `record` is the decoded record;
/// 4. a row from a member's public record carries that record (a post's
///    as stored, a like or repost rebuilt from its row), and is dropped
///    once its subject private post is no longer stored.
///
/// The caller's private notification preferences apply (private
/// notification preferences design §6); `reasons` narrows further. The
/// cursor follows the scan, so a page may be short. Floored
/// (`list_notifications`).
pub async fn list_notifications(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, LIST_NOTIFICATIONS).await?;
    let gate = start.elapsed();
    let p = Params::parse(q.as_deref());
    let limit = page_limit(&p)?;
    let after = notification_cursor(p.one("cursor"))?;
    let reasons = p.all("reasons");
    let ip = request_ip(&s.app, &headers, peer);
    let (result, timed) = stages::run(notifications_body(&s, &caller, ip, limit, after, &reasons, start)).await;
    floor("listNotifications", start, gate, &timed, s.floors.list_notifications).await;
    note_page("listNotifications", &result);
    result.map(ok)
}

#[allow(clippy::too_many_arguments)]
async fn notifications_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    limit: usize,
    after: Option<(DateTime<Utc>, i64)>,
    reasons: &[String],
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let pool = &s.env.pool;
    let me = caller.did.as_str();
    let rows = schema::notification_rows(pool, me, after, reasons, i64::try_from(limit).unwrap_or(i64::MAX)).await?;
    let next = (rows.len() == limit)
        .then(|| rows.last().map(|r| cursor(r.indexed_at, &r.id.to_string())))
        .flatten();
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let kept: HashSet<i64> = schema::notifications_kept(pool, me, &ids).await?.into_iter().collect();
    let rows: Vec<schema::NotificationRow> = rows.into_iter().filter(|r| kept.contains(&r.id)).collect();
    let hydration = private_hydration(s, caller, ip).await?;

    // Rows from a private post: each post read for the caller, after the
    // prefilter narrows them (§2 rule 1).
    let post_uris: Vec<String> = rows.iter().filter(|r| r.is_from_private_post()).map(|r| r.record_uri.clone()).collect();
    let passing: HashSet<String> = schema::prefilter_uris(pool, me, &post_uris, s.env.max_chain).await?.into_iter().collect();
    let candidates: Vec<String> = post_uris.into_iter().filter(|u| passing.contains(u)).collect();
    let authors = profiles(&hydration, &candidates).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    let mut posts: Vec<PrivateEmit<Value>> = read_many(s, &rs, caller, &candidates, &authors, start).await.into_iter().flatten().collect();
    let mut refs: Vec<&mut PrivateEmit<Value>> = posts.iter_mut().collect();
    operator_labels(&hydration, &mut refs).await?;
    let mut hits: HashMap<String, Vec<PrivateEmit<Value>>> = HashMap::new();
    for post in posts {
        if let Some(uri) = view_uri(&post) {
            hits.entry(uri).or_default().push(post);
        }
    }
    // The subjects, after the notifications' own posts (fast-reads design
    // §5.2 B-3).
    let subject_posts = notification_subjects(s, &rs, caller, &hydration, &rows, start).await?;

    // Rows from a public record: the subject must still be stored.
    let public: Vec<&schema::NotificationRow> = rows.iter().filter(|r| !r.is_from_private_post()).collect();
    let subjects: Vec<String> = public.iter().filter_map(|r| r.reason_subject.clone()).collect();
    let stored: HashSet<String> = schema::stored_posts(pool, &subjects).await?.into_iter().collect();
    let rebuild: Vec<String> = public.iter().filter(|r| r.public_record.is_none()).map(|r| r.record_uri.clone()).collect();
    let mut records: HashMap<String, Value> = HashMap::new();
    for (kind, ty) in [(Interaction::Like, "app.bsky.feed.like"), (Interaction::Repost, "app.bsky.feed.repost")] {
        for (uri, subject, cid, created) in schema::interaction_records(pool, kind, &rebuild).await? {
            let record = json!({"$type": ty, "subject": {"uri": subject, "cid": cid.unwrap_or_default()}, "createdAt": ts(created)});
            records.insert(uri, record);
        }
    }
    let public_uris: Vec<String> = public.iter().map(|r| r.record_uri.clone()).collect();
    let labels = hydration.labels(&public_uris).await?;

    let mut dids: Vec<String> = Vec::new();
    for r in &rows {
        if !dids.contains(&r.author_did) {
            dids.push(r.author_did.clone());
        }
    }
    let actors = hydration.profiles(&dids).await?;
    let seen = schema::notification_seen(pool, me).await?;
    let mut notifications: Vec<NotificationView> = Vec::new();
    for r in rows {
        let record = if r.is_from_private_post() {
            match hits.get_mut(&r.record_uri).and_then(Vec::pop) {
                Some(post) => NotificationRecord::Private(post),
                None => continue,
            }
        } else {
            if !r.reason_subject.as_ref().is_some_and(|s| stored.contains(s)) {
                continue;
            }
            let Some(record) = r.public_record.clone().or_else(|| records.get(&r.record_uri).cloned()) else {
                continue;
            };
            NotificationRecord::Public {
                labels: json!(labels.get(&r.record_uri).cloned().unwrap_or_default()),
                uri: r.record_uri,
                cid: r.record_cid,
                record,
            }
        };
        let reason_subject_post = r.reason_subject.as_ref().and_then(|u| subject_posts.get(u)).map(PrivateEmit::duplicate);
        notifications.push(NotificationView {
            record,
            author: actors.view(&r.author_did, ProfileKind::View),
            reason: r.reason,
            reason_subject: r.reason_subject,
            reason_subject_post,
            is_read: seen.is_some_and(|at| r.indexed_at <= at),
            indexed_at: ts(r.indexed_at),
        });
    }
    Ok(PrivateResponse::notifications(notifications, next, seen.map(ts)))
}

/// The private posts the notification `rows` name as `reasonSubject`, read
/// for the caller (fast-reads design §5.2 B-3, C-10), by URI: each once,
/// after the prefilter, through the pipeline in the request's
/// `RequestState` (the clear-field check, `droppedFields` and S4 apply),
/// decorated as `getPosts` decorates. A subject that does not read, for
/// any reason, is absent, so its notification carries no
/// `reasonSubjectPost`, as one with a deleted subject.
async fn notification_subjects(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    rows: &[schema::NotificationRow],
    start: Instant,
) -> Result<HashMap<String, PrivateEmit<Value>>, XrpcError> {
    let mut wanted: Vec<String> = Vec::new();
    for subject in rows.iter().filter_map(|r| r.reason_subject.as_ref()) {
        if is_private_uri(subject) && !wanted.contains(subject) {
            wanted.push(subject.clone());
        }
    }
    let passing: HashSet<String> = schema::prefilter_uris(&s.env.pool, &caller.did, &wanted, s.env.max_chain).await?.into_iter().collect();
    wanted.retain(|u| passing.contains(u));
    if wanted.is_empty() {
        return Ok(HashMap::new());
    }
    let authors = profiles(hydration, &wanted).await?;
    let mut hits: Vec<PrivateEmit<Value>> = read_many(s, rs, caller, &wanted, &authors, start).await.into_iter().flatten().collect();
    let mut refs: Vec<&mut PrivateEmit<Value>> = hits.iter_mut().collect();
    decorate(s, rs, caller, hydration, &mut refs, &[], start).await?;
    Ok(hits.into_iter().filter_map(|hit| view_uri(&hit).map(|u| (u, hit))).collect())
}

/// `app.nearhorizon.private.getUnreadCount {reasons?}` (private posting
/// §4.8; fast-reads design §5.2 B-1) → `{count}`: the caller's rows after
/// its last-seen time, under the list's exclusions and the caller's
/// notification preferences, counting a private post's row only when the
/// post passes the prefilter for the caller, and a public record's only
/// while its subject is stored. `reasons` narrows the rows as on
/// `listNotifications`. Floored (`get_unread_count`): the scan's time
/// depends on rows the caller cannot read.
pub async fn get_unread_count(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let start = Instant::now();
    let caller = prepare_private(&s.app, &headers, peer, GET_UNREAD_COUNT).await?;
    let gate = start.elapsed();
    let reasons = Params::parse(q.as_deref()).all("reasons");
    let (result, timed) = stages::run(schema::unread_count_for(&s.env.pool, &caller.did, s.env.max_chain, &reasons)).await;
    floor("getUnreadCount", start, gate, &timed, s.floors.get_unread_count).await;
    Ok(ok(PrivateResponse::unread_count(result?)))
}

/// `app.nearhorizon.private.updateSeen {seenAt}` → `{}`: the caller's
/// last-seen time moves to `GREATEST(stored, seenAt)`, clamped to now, as
/// the public method does. No floor: nothing here depends on a post.
pub async fn update_seen(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, XrpcError> {
    let caller = prepare_private(&s.app, &headers, peer, UPDATE_SEEN).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let seen_at = DateTime::parse_from_rfc3339(field(&input, "seenAt")?)
        .map_err(|_| XrpcError::invalid("Error: seenAt must be a datetime"))?;
    schema::update_seen(&s.env.pool, &caller.did, seen_at.with_timezone(&Utc)).await?;
    Ok(ok(PrivateResponse::empty()))
}

/// The reasons a private notification has, each a key of the preferences
/// (private notification preferences design §4): the private side has no
/// follow, chat or activity reasons.
pub(super) const PREFERENCE_REASONS: [&str; 5] = ["like", "mention", "quote", "reply", "repost"];

/// `stored` over the defaults (`{include: all, list: true, push: true}`),
/// key by key: exactly the five reasons, each a complete preference. A
/// stored key that is not an object reads as the default.
fn complete_preferences(stored: Option<&Value>) -> Value {
    let mut out = serde_json::Map::new();
    for key in PREFERENCE_REASONS {
        let value = stored
            .and_then(|s| s.get(key))
            .filter(|v| v.is_object())
            .cloned()
            .unwrap_or_else(|| json!({"include": "all", "list": true, "push": true}));
        out.insert(key.to_string(), value);
    }
    Value::Object(out)
}

/// `app.nearhorizon.private.getNotificationPreferences` (private
/// notification preferences design §4.1) → `{preferences}`: the caller's own
/// five preferences over the defaults. No floor: nothing here depends on a
/// post or on another account.
pub async fn get_notification_preferences(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, XrpcError> {
    let caller = prepare_private(&s.app, &headers, peer, GET_NOTIFICATION_PREFERENCES).await?;
    let stored = schema::notification_prefs(&s.env.pool, &caller.did).await?;
    Ok(ok(PrivateResponse::notification_preferences(complete_preferences(stored.as_ref()))))
}

/// `app.nearhorizon.private.putNotificationPreferences` (§4.2) →
/// `{preferences}`: each supplied reason replaces the caller's preference
/// for it whole, the others are kept (stock `putPreferencesV2`). A supplied
/// preference is checked by the public validator; other keys are ignored;
/// with no known key nothing is written. The gate runs before the body is
/// read. No floor.
pub async fn put_notification_preferences(
    State(s): State<PrivateState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, XrpcError> {
    let caller = prepare_private(&s.app, &headers, peer, PUT_NOTIFICATION_PREFERENCES).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    if !input.is_object() {
        return Err(XrpcError::invalid("Error: input must be an object"));
    }
    let mut supplied = serde_json::Map::new();
    for key in PREFERENCE_REASONS {
        if let Some(value) = input.get(key) {
            let checked = crate::hydration::notification_prefs::checked(key, value, true)
                .map_err(|message| XrpcError::invalid(format!("Error: {message}")))?;
            supplied.insert(key.to_string(), checked);
        }
    }
    let stored = if supplied.is_empty() {
        schema::notification_prefs(&s.env.pool, &caller.did).await?
    } else {
        Some(schema::merge_notification_prefs(&s.env.pool, &caller.did, &Value::Object(supplied)).await?)
    };
    Ok(ok(PrivateResponse::notification_preferences(complete_preferences(stored.as_ref()))))
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    /// What a request's head state was once `getPostThread`'s parent chain
    /// was read: the budget left and the cached head results.
    pub(in crate::private_tier) type ChainState = (usize, HashMap<String, bool>);

    fn chain_states() -> &'static Mutex<HashMap<String, ChainState>> {
        static STATES: OnceLock<Mutex<HashMap<String, ChainState>>> = OnceLock::new();
        STATES.get_or_init(Mutex::default)
    }

    /// Record `rs`'s state after the chain of the thread anchored at `uri`
    /// (private-threads T-56).
    pub(super) async fn record_chain_state(uri: &str, rs: &RequestState) {
        let state = (rs.budget_left(), rs.cached().await);
        chain_states().lock().expect("states").insert(uri.to_string(), state);
    }

    /// The state the last thread anchored at `uri` recorded.
    pub(in crate::private_tier) fn chain_state(uri: &str) -> Option<ChainState> {
        chain_states().lock().expect("states").get(uri).cloned()
    }

    #[test]
    fn cursor_round_trips() {
        let at = DateTime::parse_from_rfc3339("2026-09-26T12:00:00.123456Z").expect("ts").with_timezone(&Utc);
        let c = cursor(at, "3kaaaaaaaaaa2");
        assert_eq!(parse_cursor(Some(&c)).expect("parse"), Some((at, "3kaaaaaaaaaa2".to_string())));
        assert!(parse_cursor(Some("garbage")).is_err());
        assert_eq!(parse_cursor(None).expect("none"), None);
    }
}
