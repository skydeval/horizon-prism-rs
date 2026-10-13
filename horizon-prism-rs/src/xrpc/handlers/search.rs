//! `app.bsky.feed.searchPosts` and `app.bsky.feed.searchPostsV2` (design-v3
//! §9.1, §9.2): two parameter parsers over one search.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::search::PostSearchParams;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const SEARCH_POSTS: &str = "app.bsky.feed.searchPosts";
pub const SEARCH_POSTS_V2: &str = "app.bsky.feed.searchPostsV2";

/// The lexicon's `limit`: 1 – 100, default 25.
fn limit(p: &Params) -> Result<u32, XrpcError> {
    match p.u32("limit")? {
        None => Ok(25),
        Some(n) if (1..=100).contains(&n) => Ok(n),
        Some(_) => Err(XrpcError::invalid("Error: limit must be between 1 and 100")),
    }
}

fn owned(value: Option<&str>) -> Option<String> {
    value.filter(|v| !v.is_empty()).map(str::to_string)
}

pub async fn search_posts(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, SEARCH_POSTS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    // `q` is required by the lexicon, though it may hold operators alone.
    if p.one("q").is_none() {
        return Err(XrpcError::invalid("Error: Params must have the property \"q\""));
    }
    let one = |key: &str| owned(p.one(key)).into_iter().collect::<Vec<String>>();
    let params = PostSearchParams {
        v2: false,
        query: owned(p.one("q")),
        sort: owned(p.one("sort")),
        since: owned(p.one("since")),
        until: owned(p.one("until")),
        authors: one("author"),
        mentions: one("mentions"),
        languages: one("lang"),
        domains: one("domain"),
        urls: one("url"),
        hashtags: p.all("tag"),
        limit: limit(&p)?,
        cursor: owned(p.one("cursor")),
        ..PostSearchParams::default()
    };
    respond(&req.req, s.hydrator.search_posts(&req.viewer, &req.req, &params).await)
}

pub async fn search_posts_v2(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, SEARCH_POSTS_V2, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    // `queryLanguage` is accepted and unused: matching is language-neutral.
    let params = PostSearchParams {
        v2: true,
        query: owned(p.one("query")),
        sort: owned(p.one("sort")),
        since: owned(p.one("since")),
        until: owned(p.one("until")),
        all_time: p.bool("allTime"),
        authors: p.all("authors"),
        exclude_authors: p.all("excludeAuthors"),
        mentions: p.all("mentions"),
        exclude_mentions: p.all("excludeMentions"),
        domains: p.all("domains"),
        exclude_domains: p.all("excludeDomains"),
        urls: p.all("urls"),
        exclude_urls: p.all("excludeUrls"),
        embedded: p.all("embeddedAtUris"),
        exclude_embedded: p.all("excludeEmbeddedAtUris"),
        hashtags: p.all("hashtags"),
        exclude_hashtags: p.all("excludeHashtags"),
        languages: p.all("languages"),
        exclude_languages: p.all("excludeLanguages"),
        has_media: p.bool("hasMedia"),
        has_video: p.bool("hasVideo"),
        reply_parent: owned(p.one("replyParentUri")),
        thread_root: owned(p.one("threadRootUri")),
        replies_only: p.bool("repliesOnly"),
        exclude_replies: p.bool("excludeReplies"),
        following: p.bool("following"),
        limit: limit(&p)?,
        cursor: owned(p.one("cursor")),
    };
    respond(&req.req, s.hydrator.search_posts(&req.viewer, &req.req, &params).await)
}
