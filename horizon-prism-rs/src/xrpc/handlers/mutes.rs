//! `app.bsky.graph.getMutes`, `muteActor`, `unmuteActor` (member; mute
//! management does not require readiness, §7.1).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;
use serde_json::Value;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const GET_MUTES: &str = "app.bsky.graph.getMutes";
pub const MUTE_ACTOR: &str = "app.bsky.graph.muteActor";
pub const UNMUTE_ACTOR: &str = "app.bsky.graph.unmuteActor";

const MEMBER: Access = Access::Member { ready: false, enroll: false };

pub async fn get_mutes(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_MUTES, MEMBER, false).await?;
    let p = Params::parse(q.as_deref());
    respond(s.hydrator.get_mutes(&req.viewer, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

fn actor_of(body: &[u8]) -> Result<String, XrpcError> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("actor").and_then(Value::as_str).map(str::to_string))
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"actor\""))
}

pub async fn mute_actor(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, MUTE_ACTOR, MEMBER, true).await?;
    respond(s.hydrator.set_mute(&req.viewer, &actor_of(&body)?, true).await)
}

pub async fn unmute_actor(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, UNMUTE_ACTOR, MEMBER, true).await?;
    respond(s.hydrator.set_mute(&req.viewer, &actor_of(&body)?, false).await)
}
