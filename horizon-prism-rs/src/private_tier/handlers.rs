//! `app.nearhorizon.private.*` handlers (design-v2 §7.9, §6.3, §6.9).
//!
//! - Steps 1–2 (clock, auth gate) run first. Gate failures (401,
//!   `NotEnrolled`, `NotReady`, 429) are ordinary errors outside the floor.
//! - Candidates come from the §6.9 prefilter. Each is read through the
//!   per-record pipeline with in-task concurrency `bind_concurrency`.
//! - Step 13: every response, hit or miss, waits until the endpoint's floor
//!   measured from handler entry. The unfloored time is exported as
//!   `private_read_unfloored_seconds{endpoint}`.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use futures::StreamExt;
use serde_json::{Value, json};

use super::emit::{BookmarkView, PrivateEmit, PrivateResponse, ThreadNode, ThreadParent};
use super::pipeline::{self, PipelineEnv, RequestState};
use super::schema;
use crate::config::PrivateReadConfig;
use crate::db::locks::uri_author;
use crate::firehose::classifier::NSID_POST_PRIVATE;
use crate::hydration::actor::ProfileContext;
use crate::hydration::embeds::{EMBED_RECORD_WITH_MEDIA, media_view, normalize_embed, quoted_uri};
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

/// Metric: handler time before the floor.
const METRIC_UNFLOORED: &str = "private_read_unfloored_seconds";
/// A thread holds at most this many nodes (anchor, parents, replies).
pub(super) const THREAD_NODE_CAP: usize = 200;
/// Replies per thread node.
pub(super) const REPLIES_PER_NODE: i64 = 50;

/// The endpoint floors (`private_read.floor_ms.*`).
#[derive(Debug, Clone, Copy)]
pub struct Floors {
    pub get_posts: Duration,
    pub get_post_thread: Duration,
    pub get_author_feed: Duration,
    pub get_timeline: Duration,
    pub get_bookmarks: Duration,
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
    ) -> Self {
        let pool = app.hydrator.pool().clone();
        let video = app.hydrator.video().clone();
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
                video,
            },
            floors: Floors {
                get_posts: Duration::from_millis(cfg.floor_ms.get_posts),
                get_post_thread: Duration::from_millis(cfg.floor_ms.get_post_thread),
                get_author_feed: Duration::from_millis(cfg.floor_ms.get_author_feed),
                get_timeline: Duration::from_millis(cfg.floor_ms.get_timeline),
                get_bookmarks: Duration::from_millis(cfg.floor_ms.get_bookmarks),
            },
            concurrency: usize::try_from(cfg.bind_concurrency).unwrap_or(8).max(1),
            bookmarks_max,
        }
    }
}

fn xrpc(nsid: &str) -> String {
    format!("/xrpc/{nsid}")
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
        .with_state(state)
}

/// Step 13: record the unfloored time, then wait out the floor.
async fn floor(endpoint: &'static str, start: Instant, target: Duration) {
    metrics::histogram!(METRIC_UNFLOORED, "endpoint" => endpoint).record(start.elapsed().as_secs_f64());
    kryphocron::equalize_timing(start, target).await;
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
    let reads: Vec<_> = uris.iter().map(|u| pipeline::read(&s.env, rs, caller, u, authors, start)).collect();
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
/// PDS from `authors`.
fn embedded_media(s: &PrivateState, post: &PrivateEmit<Value>, embed: &Value, authors: &ProfileContext) -> Option<Value> {
    let did = post.view().pointer("/author/did").and_then(Value::as_str)?;
    let pds = authors.rows.get(did).and_then(|r| r.pds_endpoint.as_deref());
    media_view(did, pds, embed.get("media")?, &s.env.video)
}

fn not_found_record(uri: &str) -> Value {
    json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true})
}

/// A readable quoted private post as `app.bsky.embed.record#viewRecord`
/// (§4.2): the decoded record as `value`, and its media one level deep, as
/// `record_view` does for public posts.
fn view_record(s: &PrivateState, quoted: &PrivateEmit<Value>, authors: &ProfileContext) -> Value {
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
    let media = match v.get("embed") {
        Some(media) => Some(media.clone()),
        None => stored_embed(quoted)
            .filter(|e| e.get("$type").and_then(Value::as_str) == Some(EMBED_RECORD_WITH_MEDIA))
            .and_then(|e| embedded_media(s, quoted, &e, authors)),
    };
    if let Some(media) = media {
        record["embeds"] = json!([media]);
    }
    record
}

/// `viewer` on each hit (private-posting design §4.6): `bookmarked`, from
/// the caller's own private bookmarks (§4.3).
async fn viewer_state(s: &PrivateState, caller: &VerifiedCaller, posts: &mut [&mut PrivateEmit<Value>]) -> Result<(), XrpcError> {
    let uris: Vec<String> = posts.iter().filter_map(|p| view_uri(p)).collect();
    if uris.is_empty() {
        return Ok(());
    }
    let marked: HashSet<String> = schema::bookmarked(&s.env.pool, &caller.did, &uris).await?.into_iter().collect();
    for post in posts.iter_mut() {
        let bookmarked = view_uri(post).is_some_and(|u| marked.contains(&u));
        post.decorate("viewer", json!({"bookmarked": bookmarked}));
    }
    Ok(())
}

/// The decoration pass of a page (private-posting design §3.3), over hits
/// only: the operator labels, `viewer`, then the record part of each
/// `embed`.
///
/// - A quoted private post is read through the pipeline for the same reader
///   and the same `RequestState`, all of the page's at once: readable gives
///   `#viewRecord`; anything else, a block with its author included, gives
///   `#viewNotFound`, the answer for a URI that names nothing.
/// - A quoted public post comes from local rows, in On mode only. With
///   nothing there the view carries no `embed`, for `record` and
///   `recordWithMedia` alike, and the client builds the quote from
///   `record.embed`.
/// - Quotes are not followed past one level.
async fn decorate(
    s: &PrivateState,
    rs: &RequestState,
    caller: &VerifiedCaller,
    hydration: &HydrationContext,
    posts: &mut [&mut PrivateEmit<Value>],
    start: Instant,
) -> Result<(), XrpcError> {
    operator_labels(hydration, posts).await?;
    viewer_state(s, caller, posts).await?;
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
    if !private.is_empty() {
        let quoted_authors = profiles(hydration, &private).await?;
        let mut hits: Vec<PrivateEmit<Value>> =
            read_many(s, rs, caller, &private, &quoted_authors, start).await.into_iter().flatten().collect();
        let mut refs: Vec<&mut PrivateEmit<Value>> = hits.iter_mut().collect();
        operator_labels(hydration, &mut refs).await?;
        for q in &hits {
            if let Some(uri) = view_uri(q) {
                records.insert(uri, view_record(s, q, &quoted_authors));
            }
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
            records.get(&quoted).cloned().unwrap_or_else(|| not_found_record(&quoted))
        } else {
            match hydration.public_record_view(&quoted, &uri).await? {
                Some(view) => view.inner().clone(),
                None => continue,
            }
        };
        let record = json!({"$type": "app.bsky.embed.record#view", "record": record});
        if embed.get("$type").and_then(Value::as_str) == Some(EMBED_RECORD_WITH_MEDIA) {
            let Some(media) = media_authors.as_ref().and_then(|a| embedded_media(s, post, &embed, a)) else {
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
    let caller = prepare_private(&s.app, &headers, peer,GET_POSTS).await?;
    let requested = Params::parse(q.as_deref()).all("uris");
    if requested.is_empty() || requested.len() > 25 {
        return Err(XrpcError::invalid("Error: uris must have between 1 and 25 items"));
    }
    let ip = request_ip(&s.app, &headers, peer);
    let result = posts_body(&s, &caller, ip, &requested, start).await;
    floor("getPosts", start, s.floors.get_posts).await;
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
    let passing: HashSet<String> = schema::prefilter_uris(&s.env.pool, &caller.did, &wanted).await?.into_iter().collect();
    let candidates: Vec<String> = wanted.into_iter().filter(|u| passing.contains(u)).collect();
    let hydration = private_hydration(s, caller, ip).await?;
    let authors = profiles(&hydration, &candidates).await?;
    let rs = RequestState::new(s.env.max_head_fetches);
    let mut posts: Vec<PrivateEmit<Value>> =
        read_many(s, &rs, caller, &candidates, &authors, start).await.into_iter().flatten().collect();
    let mut refs: Vec<&mut PrivateEmit<Value>> = posts.iter_mut().collect();
    decorate(s, &rs, caller, &hydration, &mut refs, start).await?;
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
    let caller = prepare_private(&s.app, &headers, peer,GET_POST_THREAD).await?;
    let p = Params::parse(q.as_deref());
    let uri = p.required("uri")?.to_string();
    let depth = bounded(&p, "depth", 6, 10)?;
    let parent_height = bounded(&p, "parentHeight", 20, 30)?;
    let ip = request_ip(&s.app, &headers, peer);
    let result = thread_body(&s, &caller, ip, &uri, depth, parent_height, start).await;
    floor("getPostThread", start, s.floors.get_post_thread).await;
    result.and_then(|t| t.ok_or_else(not_found)).map(ok)
}

/// The parent URI named by an authorized view's `record.reply.parent`.
fn view_parent(post: &PrivateEmit<Value>) -> Option<String> {
    post.view()
        .pointer("/record/reply/parent/uri")
        .and_then(Value::as_str)
        .map(str::to_string)
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
    let anchor_uri = vec![uri.to_string()];
    let authors = profiles(&hydration, &anchor_uri).await?;
    let Ok(mut anchor) = pipeline::read(&s.env, &rs, caller, uri, &authors, start).await else {
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
            let authors = profiles(&hydration, std::slice::from_ref(&parent)).await?;
            let Ok(p) = pipeline::read(&s.env, &rs, caller, &parent, &authors, start).await else {
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

    // Replies, breadth-first, up to `depth` levels and the node cap.
    let mut replies: HashMap<String, Vec<PrivateEmit<Value>>> = HashMap::new();
    let mut expanded: HashSet<String> = HashSet::new();
    let mut level: Vec<String> = anchor_uri;
    for _ in 0..depth {
        if level.is_empty() || budget == 0 {
            break;
        }
        let mut batch: Vec<(String, String)> = Vec::new();
        for parent in &level {
            if batch.len() >= budget {
                break;
            }
            expanded.insert(parent.clone());
            for c in schema::reply_candidates(&s.env.pool, &caller.did, parent, REPLIES_PER_NODE).await? {
                if batch.len() >= budget {
                    break;
                }
                batch.push((parent.clone(), c));
            }
        }
        let uris: Vec<String> = batch.iter().map(|(_, c)| c.clone()).collect();
        let authors = profiles(&hydration, &uris).await?;
        let results = read_many(s, &rs, caller, &uris, &authors, start).await;
        level = Vec::new();
        for ((parent, child), hit) in batch.into_iter().zip(results) {
            if let Some(post) = hit {
                budget -= 1;
                level.push(child);
                replies.entry(parent).or_default().push(post);
            }
        }
    }

    // Every node is read; decorate them all at once.
    let mut refs: Vec<&mut PrivateEmit<Value>> = std::iter::once(&mut anchor)
        .chain(chain.iter_mut())
        .chain(replies.values_mut().flatten())
        .collect();
    decorate(s, &rs, caller, &hydration, &mut refs, start).await?;

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

/// `created_at::rkey` for one author's feed; `created_at::uri` for the
/// timeline, where a record key is not unique.
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
    let caller = prepare_private(&s.app, &headers, peer,GET_AUTHOR_FEED).await?;
    let p = Params::parse(q.as_deref());
    let actor = p.required("actor")?.to_string();
    if !actor.starts_with("did:") {
        return Err(XrpcError::invalid("Error: actor must be a valid did"));
    }
    let limit = match p.u32("limit")?.unwrap_or(30) {
        l @ 1..=50 => l,
        _ => return Err(XrpcError::invalid("Error: limit must be between 1 and 50")),
    };
    let after = parse_cursor(p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let result = feed_body(&s, &caller, ip, Some(&actor), limit as usize, after, start).await;
    floor("getAuthorFeed", start, s.floors.get_author_feed).await;
    result.map(ok)
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
    let p = Params::parse(q.as_deref());
    let limit = match p.u32("limit")?.unwrap_or(30) {
        l @ 1..=50 => l,
        _ => return Err(XrpcError::invalid("Error: limit must be between 1 and 50")),
    };
    let after = parse_cursor(p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let result = feed_body(&s, &caller, ip, None, limit as usize, after, start).await;
    floor("getTimeline", start, s.floors.get_timeline).await;
    result.map(ok)
}

/// Windows of candidates one feed request may read.
const FEED_WINDOWS: usize = 2;
/// A second window is begun only this soon after the handler started.
const SECOND_WINDOW_BY: Duration = Duration::from_millis(600);

/// One page of a feed: `actor`'s posts, or with no actor the caller's
/// home timeline. The candidates only narrow the work; `pipeline::read`
/// decides each post.
///
/// A window is `limit × 4` candidates. If it ends short of `limit` and
/// candidates remain, one more is read, from after the last row the first
/// examined, so that a run of posts nobody can read (content that does
/// not decode, say) does not end the feed. The cursor is always that of
/// the last readable post: it never carries the position of a post the
/// caller may not read.
async fn feed_body(
    s: &PrivateState,
    caller: &VerifiedCaller,
    ip: IpAddr,
    actor: Option<&str>,
    limit: usize,
    after: Option<(DateTime<Utc>, String)>,
    start: Instant,
) -> Result<PrivateResponse, XrpcError> {
    let max_binds = limit * 4;
    // One lookahead row tells whether prefiltered candidates remain.
    let wanted = i64::try_from(max_binds + 1).unwrap_or(i64::MAX);
    let hydration = private_hydration(s, caller, ip).await?;
    // One state for the whole request: the head-fetch budget is per
    // request, not per window.
    let rs = RequestState::new(s.env.max_head_fetches);

    let mut feed = Vec::new();
    let mut last_hit: Option<(DateTime<Utc>, String)> = None;
    let mut more = false;
    let mut after = after;
    for window in 0..FEED_WINDOWS {
        let mut candidates = match actor {
            Some(actor) => schema::author_candidates(&s.env.pool, &caller.did, actor, after.clone(), wanted).await?,
            None => schema::timeline_candidates(&s.env.pool, &caller.did, after.clone(), wanted).await?,
        };
        let beyond = candidates.len() > max_binds;
        candidates.truncate(max_binds);
        let uris: Vec<String> = candidates.iter().map(|(u, _, _)| u.clone()).collect();
        let authors = profiles(&hydration, &uris).await?;
        let mut full_at: Option<usize> = None;
        let reads: Vec<_> = uris.iter().map(|u| pipeline::read(&s.env, &rs, caller, u, &authors, start)).collect();
        let mut reads = futures::stream::iter(reads).buffered(s.concurrency).enumerate();
        while let Some((i, hit)) = reads.next().await {
            if let Ok(post) = hit {
                feed.push(post);
                last_hit = Some((candidates[i].1, candidates[i].2.clone()));
                if feed.len() == limit {
                    full_at = Some(i);
                    break;
                }
            }
        }
        drop(reads);
        if let Some(i) = full_at {
            more = i + 1 < candidates.len() || beyond;
            break;
        }
        // The window ended short. What it held is examined; candidates
        // remain only behind the lookahead row.
        more = beyond;
        let Some(tail) = candidates.last() else { break };
        if !beyond || window + 1 == FEED_WINDOWS || start.elapsed() >= SECOND_WINDOW_BY {
            break;
        }
        after = Some((tail.1, tail.2.clone()));
    }
    let cursor = last_hit.filter(|_| more).map(|(at, key)| cursor(at, &key));
    let mut refs: Vec<&mut PrivateEmit<Value>> = feed.iter_mut().collect();
    decorate(s, &rs, caller, &hydration, &mut refs, start).await?;
    Ok(PrivateResponse::feed(feed, cursor))
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
    let p = Params::parse(q.as_deref());
    let limit = match p.u32("limit")?.unwrap_or(50) {
        l @ 1..=100 => l,
        _ => return Err(XrpcError::invalid("Error: limit must be between 1 and 100")),
    };
    let after = parse_cursor(p.one("cursor"))?;
    let ip = request_ip(&s.app, &headers, peer);
    let result = bookmarks_body(&s, &caller, ip, limit as usize, after, start).await;
    floor("getBookmarks", start, s.floors.get_bookmarks).await;
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
    let passing: HashSet<String> = schema::prefilter_uris(&s.env.pool, &caller.did, &uris).await?.into_iter().collect();
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
    decorate(s, &rs, caller, &hydration, &mut refs, start).await?;
    Ok(PrivateResponse::bookmarks(bookmarks, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips() {
        let at = DateTime::parse_from_rfc3339("2026-09-26T12:00:00.123456Z").expect("ts").with_timezone(&Utc);
        let c = cursor(at, "3kaaaaaaaaaa2");
        assert_eq!(parse_cursor(Some(&c)).expect("parse"), Some((at, "3kaaaaaaaaaa2".to_string())));
        assert!(parse_cursor(Some("garbage")).is_err());
        assert_eq!(parse_cursor(None).expect("none"), None);
    }
}
