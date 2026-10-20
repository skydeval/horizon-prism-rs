//! `app.nearhorizon.threadLens.getThread` and `.resolve` (design-v3 §5.15).
//!
//! - Both are procedures (POST, JSON input).
//! - **For signed-in callers** (§5.15.2, `threadlens.access`). A token
//!   that verifies is always required. Who is served then depends on the
//!   setting:
//!   - `members`: ready members only. A member that is not ready and a
//!     non-member get the refusals every member-only endpoint gives
//!     (`NotReady`, `NotEnrolled`).
//!   - `signed_in`: everyone. A ready member is served as a **member**.
//!     Anyone else, a member that is not ready included, is served as a
//!     **visitor**: its verdicts come from Farsight and from the block rows
//!     held here for it, it has no viewer state but `blockedBy`, nothing
//!     is remembered for it, and its Farsight calls draw on the non-member
//!     pool.
//!
//!   Responses are for the caller: [`gate`] returns who is asking.
//! - **Non-enumeration:** every per-URI failure is a typed gap. Request-level
//!   failures (malformed body, bad parameters, too many URIs) are
//!   `400 InvalidRequest` and depend only on the input and, under
//!   `members`, on the caller's membership and readiness.

use std::net::SocketAddr;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;

use super::cursor::CursorKey;
use super::ladder::Ladder;
use super::thread::{Request, get_thread as assemble, resolve as resolve_each};
use crate::config::{ThreadLensAccess, ThreadLensConfig};
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
    /// The bounds of one response. `access` is read from `live`, not from
    /// here.
    pub cfg: ThreadLensConfig,
    /// The settings that apply at once (admin-ui design §8.5):
    /// `threadlens.access`.
    pub live: crate::admin::settings::LiveSettings,
    /// Seals the paging cursors.
    pub cursor_key: CursorKey,
}

/// The Thread Lens routes, merged into the app by `serve`.
pub fn router(state: ThreadLensState) -> Router {
    Router::new()
        .route(&format!("/xrpc/{GET_THREAD}"), post(get_thread))
        .route(&format!("/xrpc/{RESOLVE}"), post(resolve))
        .with_state(state)
}

/// Authenticate, rate-limit, and sort the caller by `threadlens.access`.
/// Returns the request's charge handle and the viewer the response is
/// built for.
///
/// - A ready member is a member: `hydrate` is true.
/// - Under `members` anyone else is refused. Under `signed_in` anyone else
///   is a visitor: `hydrate` is false, which is what every later step reads
///   to tell the two apart.
///
/// The rate class is charged to the caller's DID for both kinds. The
/// charge handle is the one the backstop selected before the handler ran.
/// Where none ran (tests), a visitor is charged by address and never given
/// a member key.
async fn gate(s: &ThreadLensState, headers: &HeaderMap, peer: SocketAddr, method: &str) -> Result<(Requester, Viewer), XrpcError> {
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = authenticate(&s.app, authorization, method).await?;
    let ip = request_ip(&s.app, headers, peer);
    s.app.limits.charge(method, Family::ThreadLens, Some(&caller.did), ip)?;
    let ready: Option<bool> = sqlx::query_scalar("SELECT ready_at IS NOT NULL FROM members WHERE did = $1")
        .bind(&caller.did)
        .fetch_optional(s.app.hydrator.pool())
        .await?;
    // Read at each request: the admin UI changes it at once (admin-ui
    // design §8.5).
    let access = s.live.load().threadlens_access;
    let hydrate = match (ready, access) {
        (Some(true), _) => true,
        (Some(false), ThreadLensAccess::Members) => return Err(XrpcError::not_ready()),
        (None, ThreadLensAccess::Members) => return Err(XrpcError::not_enrolled()),
        (Some(false) | None, ThreadLensAccess::SignedIn) => false,
    };
    let viewer = Viewer {
        did: Some(caller.did.clone()),
        hydrate,
    };
    Ok((request_handle(viewer.state_did(), ip), viewer))
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
    // A cursor that fails authentication, or was issued for another node,
    // is refused: the client restarts that node without one.
    let cursor = match input.replies_cursor {
        None => None,
        Some(raw) => match s.cursor_key.open(&input.anchor, &raw) {
            Some(position) => Some((position, raw)),
            None => return Err(XrpcError::invalid("Error: repliesCursor is not valid for this anchor")),
        },
    };
    let request = Request {
        viewer: &viewer,
        req: &req,
        anchor: &input.anchor,
        depth,
        parent_height,
    };
    let thread = assemble(&s.ladder, &s.cfg, &s.cursor_key, request, cursor).await?;
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
    let resolved = resolve_each(&s.ladder, &s.cfg, &s.cursor_key, &viewer, &req, &input.uris).await?;
    Ok(Json(lens_items(&resolved.nodes, resolved.bridging)))
}
