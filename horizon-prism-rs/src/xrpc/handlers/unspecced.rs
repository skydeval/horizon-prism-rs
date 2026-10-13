//! Suggestions, trends and `getConfig` (design-v3 §7).

use std::net::SocketAddr;

use axum::Json;
use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::suggestions::{Kind, Listing};
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const GET_SUGGESTED_USERS: &str = "app.bsky.unspecced.getSuggestedUsers";
pub const GET_SUGGESTIONS: &str = "app.bsky.actor.getSuggestions";
pub const GET_SUGGESTED_USERS_FOR_DISCOVER: &str = "app.bsky.unspecced.getSuggestedUsersForDiscover";
pub const GET_SUGGESTED_USERS_FOR_EXPLORE: &str = "app.bsky.unspecced.getSuggestedUsersForExplore";
pub const GET_SUGGESTED_USERS_FOR_SEE_MORE: &str = "app.bsky.unspecced.getSuggestedUsersForSeeMore";
pub const GET_SUGGESTED_ONBOARDING_USERS: &str = "app.bsky.unspecced.getSuggestedOnboardingUsers";
pub const GET_SUGGESTED_FOLLOWS_BY_ACTOR: &str = "app.bsky.graph.getSuggestedFollowsByActor";
pub const GET_TAGGED_SUGGESTIONS: &str = "app.bsky.unspecced.getTaggedSuggestions";
pub const GET_CONFIG: &str = "app.bsky.unspecced.getConfig";
pub const GET_TRENDING_TOPICS: &str = "app.bsky.unspecced.getTrendingTopics";
pub const GET_TRENDS: &str = "app.bsky.unspecced.getTrends";
pub const GET_SUGGESTED_STARTER_PACKS: &str = "app.bsky.unspecced.getSuggestedStarterPacks";
pub const GET_ONBOARDING_SUGGESTED_STARTER_PACKS: &str = "app.bsky.unspecced.getOnboardingSuggestedStarterPacks";

/// A lexicon `limit` between 1 and `max`.
fn limit(p: &Params, default: u32, max: u32) -> Result<u32, XrpcError> {
    match p.u32("limit")? {
        None => Ok(default),
        Some(n) if (1..=max).contains(&n) => Ok(n),
        Some(_) => Err(XrpcError::invalid(format!("Error: limit must be between 1 and {max}"))),
    }
}

/// One of the `{actors, recIdStr}` endpoints. `category` is accepted and
/// unused: there is no interest categorization.
async fn users(s: AppState, peer: SocketAddr, headers: HeaderMap, q: Option<String>, nsid: &'static str, kind: Kind) -> XrpcResult {
    let req = prepare(&s, &headers, peer, nsid, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    let paged = nsid == GET_SUGGESTIONS;
    let limit = if paged { limit(&p, 50, 100)? } else { limit(&p, 25, 50)? };
    let listing = Listing { endpoint: nsid, limit, paged, cursor: p.one("cursor") };
    respond(&req.req, s.hydrator.suggested_users(&req.viewer, &req.req, kind, listing).await)
}

macro_rules! users_endpoint {
    ($name:ident, $nsid:expr, $kind:expr) => {
        pub async fn $name(
            State(s): State<AppState>,
            ConnectInfo(peer): ConnectInfo<SocketAddr>,
            headers: HeaderMap,
            RawQuery(q): RawQuery,
        ) -> XrpcResult {
            users(s, peer, headers, q, $nsid, $kind).await
        }
    };
}

users_endpoint!(get_suggested_users, GET_SUGGESTED_USERS, Kind::Newest);
users_endpoint!(get_suggestions, GET_SUGGESTIONS, Kind::Newest);
users_endpoint!(get_suggested_users_for_discover, GET_SUGGESTED_USERS_FOR_DISCOVER, Kind::Friends { more: false });
users_endpoint!(get_suggested_users_for_explore, GET_SUGGESTED_USERS_FOR_EXPLORE, Kind::Popular);
users_endpoint!(get_suggested_users_for_see_more, GET_SUGGESTED_USERS_FOR_SEE_MORE, Kind::Friends { more: true });
users_endpoint!(get_suggested_onboarding_users, GET_SUGGESTED_ONBOARDING_USERS, Kind::Popular);

pub async fn get_suggested_follows_by_actor(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_SUGGESTED_FOLLOWS_BY_ACTOR, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.suggested_follows_by_actor(&req.viewer, &req.req, p.required("actor")?).await)
}

pub async fn get_tagged_suggestions(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_TAGGED_SUGGESTIONS, Access::Opt, false).await?;
    respond(&req.req, s.hydrator.tagged_suggestions().await)
}

pub async fn get_config(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> XrpcResult {
    prepare(&s, &headers, peer, GET_CONFIG, Access::Opt, false).await?;
    Ok(Json(crate::hydration::Hydrator::emit(s.hydrator.unspecced_config())))
}

/// Both starter-pack suggestion endpoints share one pool (design-v3 §7.2).
async fn packs(s: AppState, peer: SocketAddr, headers: HeaderMap, q: Option<String>, nsid: &'static str) -> XrpcResult {
    let req = prepare(&s, &headers, peer, nsid, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.suggested_starter_packs(&req.viewer, &req.req, limit(&p, 10, 25)?).await)
}

pub async fn get_suggested_starter_packs(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    packs(s, peer, headers, q, GET_SUGGESTED_STARTER_PACKS).await
}

pub async fn get_onboarding_suggested_starter_packs(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    packs(s, peer, headers, q, GET_ONBOARDING_SUGGESTED_STARTER_PACKS).await
}

pub async fn get_trending_topics(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_TRENDING_TOPICS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.trending_topics(limit(&p, 10, 25)?).await)
}

pub async fn get_trends(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_TRENDS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.trends(&req.viewer, &req.req, limit(&p, 10, 25)?).await)
}
