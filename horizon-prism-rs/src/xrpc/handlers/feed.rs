//! `app.bsky.feed.getPosts`, `getPostThread`, `getAuthorFeed`, `getLikes`,
//! `getRepostedBy`, `getQuotes`.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::hydration::thread::{DEFAULT_DEPTH, DEFAULT_PARENT_HEIGHT};
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const GET_POSTS: &str = "app.bsky.feed.getPosts";
pub const GET_POST_THREAD: &str = "app.bsky.feed.getPostThread";
pub const GET_AUTHOR_FEED: &str = "app.bsky.feed.getAuthorFeed";
pub const GET_LIKES: &str = "app.bsky.feed.getLikes";
pub const GET_REPOSTED_BY: &str = "app.bsky.feed.getRepostedBy";
pub const GET_QUOTES: &str = "app.bsky.feed.getQuotes";
pub const GET_ACTOR_LIKES: &str = "app.bsky.feed.getActorLikes";

// The lexicon's known values at `b3d7c33` (design-v3 §11 G-2).
const FILTERS: [&str; 5] = [
    "posts_with_replies",
    "posts_no_replies",
    "posts_with_media",
    "posts_and_author_threads",
    "posts_with_video",
];

pub async fn get_posts(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_POSTS, Access::Opt, false).await?;
    let uris = Params::parse(q.as_deref()).all("uris");
    if uris.is_empty() || uris.len() > 25 {
        return Err(XrpcError::invalid("Error: uris must have between 1 and 25 items"));
    }
    respond(&req.req, s.hydrator.get_posts(&req.viewer, &req.req, &uris).await)
}

pub async fn get_post_thread(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_POST_THREAD, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    let depth = p.u32("depth")?.unwrap_or(DEFAULT_DEPTH);
    let parent_height = p.u32("parentHeight")?.unwrap_or(DEFAULT_PARENT_HEIGHT);
    respond(&req.req, s.hydrator.get_post_thread(&req.viewer, &req.req, p.required("uri")?, depth, parent_height).await)
}

pub const GET_POST_THREAD_V2: &str = "app.bsky.unspecced.getPostThreadV2";
pub const GET_POST_THREAD_OTHER_V2: &str = "app.bsky.unspecced.getPostThreadOtherV2";

/// A bounded integer parameter: its default when absent, `400` outside the
/// lexicon's range.
fn bounded(p: &Params, key: &str, default: u32, max: u32) -> Result<u32, XrpcError> {
    match p.u32(key)? {
        None => Ok(default),
        Some(v) if v <= max => Ok(v),
        Some(_) => Err(XrpcError::invalid(format!("Error: {key} can not be greater than {max}"))),
    }
}

pub async fn get_post_thread_v2(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    use crate::hydration::thread_v2::{DEFAULT_BELOW, DEFAULT_BRANCHING, MAX_BELOW, MAX_BRANCHING, Options, Sort};

    let req = prepare(&s, &headers, peer, GET_POST_THREAD_V2, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    let options = Options {
        above: p.one("above") != Some("false"),
        below: bounded(&p, "below", DEFAULT_BELOW, MAX_BELOW)?,
        branching: bounded(&p, "branchingFactor", DEFAULT_BRANCHING, MAX_BRANCHING)?,
        sort: Sort::parse(p.one("sort")),
    };
    respond(&req.req, s.hydrator.get_post_thread_v2(&req.viewer, &req.req, p.required("anchor")?, &options).await)
}

pub async fn get_post_thread_other_v2(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_POST_THREAD_OTHER_V2, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_post_thread_other_v2(&req.viewer, &req.req, p.required("anchor")?).await)
}

pub async fn get_author_feed(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_AUTHOR_FEED, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    let filter = p.one("filter").unwrap_or("posts_with_replies");
    if !FILTERS.contains(&filter) {
        return Err(XrpcError::invalid(format!("Error: filter must be one of {}", FILTERS.join(", "))));
    }
    respond(
        &req.req,
        s.hydrator
            .get_author_feed(
                &req.viewer,
                &req.req,
                p.required("actor")?,
                clamp_limit(p.u32("limit")?),
                p.one("cursor"),
                filter,
                p.bool("includePins"),
            )
            .await,
    )
}

pub async fn get_likes(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_LIKES, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_likes(&req.viewer, &req.req, p.required("uri")?, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

pub async fn get_reposted_by(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_REPOSTED_BY, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .get_reposted_by(&req.viewer, &req.req, p.required("uri")?, clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
}

pub async fn get_quotes(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_QUOTES, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_quotes(&req.viewer, &req.req, p.required("uri")?, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

/// A member's own likes only (design-v3 §5.4).
pub async fn get_actor_likes(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let access = Access::Member { ready: true, enroll: false };
    let req = prepare(&s, &headers, peer, GET_ACTOR_LIKES, access, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .get_actor_likes(&req.viewer, &req.req, p.required("actor")?, clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
}
