//! `app.bsky.actor.getProfile`, `getProfiles`, `searchActors`.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const GET_PROFILE: &str = "app.bsky.actor.getProfile";
pub const GET_PROFILES: &str = "app.bsky.actor.getProfiles";
pub const SEARCH_ACTORS: &str = "app.bsky.actor.searchActors";

pub async fn get_profile(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_PROFILE, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(s.hydrator.get_profile(&req.viewer, p.required("actor")?).await)
}

pub async fn get_profiles(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_PROFILES, Access::Opt, false).await?;
    let actors = Params::parse(q.as_deref()).all("actors");
    if actors.is_empty() || actors.len() > 25 {
        return Err(XrpcError::invalid("Error: actors must have between 1 and 25 items"));
    }
    respond(s.hydrator.get_profiles(&req.viewer, &actors).await)
}

pub async fn search_actors(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, SEARCH_ACTORS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    let term = p.one("q").or_else(|| p.one("term")).unwrap_or_default();
    respond(
        s.hydrator
            .search_actors(&req.viewer, term, clamp_limit(p.u32("limit")?).min(100), p.one("cursor"))
            .await,
    )
}
