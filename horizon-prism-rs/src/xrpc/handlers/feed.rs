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
