//! `app.bsky.graph.getMutes`, `muteActor`, `unmuteActor`, `muteThread`,
//! `unmuteThread` (member; mute management does not require readiness,
//! §7.1) and `getBlocks` (ready member).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;
use serde_json::Value;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::hydration::graph::MuteScope;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::backstop;
use crate::xrpc::errors::XrpcError;

pub const GET_MUTES: &str = "app.bsky.graph.getMutes";
pub const MUTE_ACTOR: &str = "app.bsky.graph.muteActor";
pub const UNMUTE_ACTOR: &str = "app.bsky.graph.unmuteActor";
pub const MUTE_THREAD: &str = "app.bsky.graph.muteThread";
pub const UNMUTE_THREAD: &str = "app.bsky.graph.unmuteThread";
pub const GET_BLOCKS: &str = "app.bsky.graph.getBlocks";
pub const MUTE_ACTOR_LIST: &str = "app.bsky.graph.muteActorList";
pub const UNMUTE_ACTOR_LIST: &str = "app.bsky.graph.unmuteActorList";
pub const GET_LIST_MUTES: &str = "app.bsky.graph.getListMutes";
pub const GET_LIST_BLOCKS: &str = "app.bsky.graph.getListBlocks";

const MEMBER: Access = Access::Member { ready: false, enroll: false };

pub async fn get_mutes(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_MUTES, MEMBER, false).await?;
    // The listed accounts are the viewer's private mutes: Farsight is not
    // asked about them, so it never sees a viewer beside its mutes
    // (design-v3 §5.14.5, viewer-private mode).
    if let Some(lookup) = backstop::farsight() {
        lookup.set_cache_only();
    }
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_mutes(&req.viewer, &req.req, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

fn input(body: &[u8]) -> Result<(String, Value), XrpcError> {
    let v = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
    let actor = v
        .get("actor")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"actor\""))?;
    Ok((actor, v))
}

fn actor_of(body: &[u8]) -> Result<String, XrpcError> {
    input(body).map(|(actor, _)| actor)
}

/// `muteActor`'s optional `onlyReposts` / `onlyQuoteposts` (design-v3 §5.3).
/// A flag that is present must be a boolean.
fn scope_of(v: &Value) -> Result<MuteScope, XrpcError> {
    let flag = |name: &str| match v.get(name) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(XrpcError::invalid(format!("Error: {name} must be a boolean"))),
    };
    Ok(MuteScope::from_flags(flag("onlyReposts")?, flag("onlyQuoteposts")?))
}

pub async fn mute_actor(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, MUTE_ACTOR, MEMBER, true).await?;
    let (actor, v) = input(&body)?;
    respond(&req.req, s.hydrator.set_mute(&req.viewer, &req.req, &actor,Some(scope_of(&v)?)).await)
}

fn root_of(body: &[u8]) -> Result<String, XrpcError> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("root").and_then(Value::as_str).map(str::to_string))
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"root\""))
}

pub async fn mute_thread(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, MUTE_THREAD, MEMBER, true).await?;
    respond(&req.req, s.hydrator.set_thread_mute(&req.viewer, &root_of(&body)?, true).await)
}

pub async fn unmute_thread(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, UNMUTE_THREAD, MEMBER, true).await?;
    respond(&req.req, s.hydrator.set_thread_mute(&req.viewer, &root_of(&body)?, false).await)
}

/// `app.bsky.graph.getBlocks`: a ready member's own blocks.
pub async fn get_blocks(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let access = Access::Member { ready: true, enroll: false };
    let req = prepare(&s, &headers, peer, GET_BLOCKS, access, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_blocks(&req.viewer, &req.req, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

fn list_of(body: &[u8]) -> Result<String, XrpcError> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("list").and_then(Value::as_str).map(str::to_string))
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"list\""))
}

/// `app.bsky.graph.muteActorList` (design-v3 §5.3): a member's private
/// mute of a moderation list.
pub async fn mute_actor_list(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, MUTE_ACTOR_LIST, MEMBER, true).await?;
    respond(&req.req, s.hydrator.set_list_mute(&req.viewer, &req.req, &list_of(&body)?, true).await)
}

pub async fn unmute_actor_list(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, UNMUTE_ACTOR_LIST, MEMBER, true).await?;
    respond(&req.req, s.hydrator.set_list_mute(&req.viewer, &req.req, &list_of(&body)?, false).await)
}

/// `app.bsky.graph.getListMutes`: the lists the viewer mutes.
pub async fn get_list_mutes(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_LIST_MUTES, MEMBER, false).await?;
    // The viewer's private mutes: Farsight is not asked about the lists'
    // owners beside them (design-v3 §5.14.5, viewer-private mode).
    if let Some(lookup) = backstop::farsight() {
        lookup.set_cache_only();
    }
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_list_mutes(&req.viewer, &req.req, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

/// `app.bsky.graph.getListBlocks`: the lists a ready member blocks.
pub async fn get_list_blocks(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let access = Access::Member { ready: true, enroll: false };
    let req = prepare(&s, &headers, peer, GET_LIST_BLOCKS, access, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_list_blocks(&req.viewer, &req.req, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

pub async fn unmute_actor(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, UNMUTE_ACTOR, MEMBER, true).await?;
    respond(&req.req, s.hydrator.set_mute(&req.viewer, &req.req, &actor_of(&body)?, None).await)
}
