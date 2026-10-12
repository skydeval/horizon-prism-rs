//! `app.nearhorizon.threadLens.getThread` and `.resolve` (design-v3 §5.15).
//!
//! - Both are procedures (POST, JSON input).
//! - **A member endpoint** (§5.15.2): the caller must be a ready member,
//!   because only a ready member's own block state is complete here. A
//!   member that is not ready and a non-member get the refusals every
//!   member-only endpoint gives (`NotReady`, `NotEnrolled`). Responses are
//!   for the caller: [`gate`] returns who is asking.
//! - **Non-enumeration:** every per-URI failure is a typed gap. Request-level
//!   failures (malformed body, bad parameters, too many URIs) are
//!   `400 InvalidRequest` and depend only on the input and on the caller's
//!   membership and readiness.

use std::net::SocketAddr;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;

use super::ladder::Ladder;
use super::thread::{Request, get_thread as assemble, resolve as resolve_each};
use crate::config::ThreadLensConfig;
use crate::db::locks::uri_author;
use crate::hydration::lens::{lens_items, lens_thread};
use crate::hydration::{PublicEmit, Viewer};
use crate::xrpc::AppState;
use crate::xrpc::auth::{authenticate, request_handle, request_ip};
use crate::xrpc::errors::XrpcError;
use crate::xrpc::pools::Requester;
use crate::xrpc::rate_limit::Family;

pub const GET_THREAD: &str = "app.nearhorizon.threadLens.getThread";
pub const RESOLVE: &str = "app.nearhorizon.threadLens.resolve";
/// `resolve` accepts 1–25 URIs.
pub const MAX_URIS: usize = 25;
/// `depth` and `parentHeight` take the range `getPostThread` takes.
pub const MAX_DEPTH: u32 = 1000;
pub const DEFAULT_DEPTH: u32 = 6;
pub const DEFAULT_PARENT_HEIGHT: u32 = 80;

/// Handler state.
#[derive(Clone)]
pub struct ThreadLensState {
    pub app: AppState,
    pub ladder: Ladder,
    pub cfg: ThreadLensConfig,
}

/// The Thread Lens routes, merged into the app by `serve`.
pub fn router(state: ThreadLensState) -> Router {
    Router::new()
        .route(&format!("/xrpc/{GET_THREAD}"), post(get_thread))
        .route(&format!("/xrpc/{RESOLVE}"), post(resolve))
        .with_state(state)
}

/// Authenticate, rate-limit, and require a ready member. Returns the
/// request's charge handle (its upstream calls are charged to the member)
/// and the viewer the response is built for.
async fn gate(s: &ThreadLensState, headers: &HeaderMap, peer: SocketAddr, method: &str) -> Result<(Requester, Viewer), XrpcError> {
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = authenticate(&s.app, authorization, method).await?;
    let ip = request_ip(&s.app, headers, peer);
    s.app.limits.charge(method, Family::ThreadLens, Some(&caller.did), ip)?;
    let ready: Option<bool> = sqlx::query_scalar("SELECT ready_at IS NOT NULL FROM members WHERE did = $1")
        .bind(&caller.did)
        .fetch_optional(s.app.hydrator.pool())
        .await?;
    match ready {
        Some(true) => {}
        Some(false) => return Err(XrpcError::not_ready()),
        None => return Err(XrpcError::not_enrolled()),
    }
    let viewer = Viewer {
        did: Some(caller.did.clone()),
        hydrate: true,
    };
    Ok((request_handle(Some(&caller.did), ip), viewer))
}

fn at_uri(u: &str) -> bool {
    u.starts_with("at://") && uri_author(u).is_some_and(proto_blue::syntax::Did::is_valid) && u.matches('/').count() == 4
}

fn body<T: for<'de> Deserialize<'de>>(bytes: &Bytes) -> Result<T, XrpcError> {
    serde_json::from_slice(bytes).map_err(|e| XrpcError::invalid(format!("Error: invalid input: {e}")))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetThreadInput {
    anchor: String,
    depth: Option<u32>,
    parent_height: Option<u32>,
    replies_cursor: Option<String>,
}

#[derive(Deserialize)]
struct ResolveInput {
    uris: Vec<String>,
}

pub async fn get_thread(
    State(s): State<ThreadLensState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<PublicEmit<Value>>, XrpcError> {
    let (req, viewer) = gate(&s, &headers, peer, GET_THREAD).await?;
    let input: GetThreadInput = body(&bytes)?;
    if !at_uri(&input.anchor) {
        return Err(XrpcError::invalid("Error: anchor must be an at-uri"));
    }
    let depth = input.depth.unwrap_or(DEFAULT_DEPTH);
    let parent_height = input.parent_height.unwrap_or(DEFAULT_PARENT_HEIGHT);
    if depth > MAX_DEPTH || parent_height > MAX_DEPTH {
        return Err(XrpcError::invalid("Error: depth and parentHeight must be 0-1000"));
    }
    // No cursor is issued yet, so none can be valid: the client restarts
    // that node without one.
    if input.replies_cursor.is_some() {
        return Err(XrpcError::invalid("Error: repliesCursor is not valid for this anchor"));
    }
    let request = Request {
        viewer: &viewer,
        req: &req,
        anchor: &input.anchor,
        depth,
        parent_height,
    };
    let thread = assemble(&s.ladder, &s.cfg, request).await?;
    Ok(Json(lens_thread(&input.anchor, &thread.nodes, thread.bridging)))
}

pub async fn resolve(
    State(s): State<ThreadLensState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<PublicEmit<Value>>, XrpcError> {
    let (req, viewer) = gate(&s, &headers, peer, RESOLVE).await?;
    let input: ResolveInput = body(&bytes)?;
    if input.uris.is_empty() || input.uris.len() > MAX_URIS {
        return Err(XrpcError::invalid("Error: uris must have between 1 and 25 items"));
    }
    if !input.uris.iter().all(|u| at_uri(u)) {
        return Err(XrpcError::invalid("Error: uris must be at-uris"));
    }
    let resolved = resolve_each(&s.ladder, &s.cfg, &viewer, &req, &input.uris).await?;
    Ok(Json(lens_items(&resolved.nodes, resolved.bridging)))
}
