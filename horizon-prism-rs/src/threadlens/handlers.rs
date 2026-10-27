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
//! - **The reader's labelers** (threadlens-hydration design §3.6 (b),
//!   skydeval 2026-10-24): a request's `atproto-accept-labelers`, its valid
//!   entries only, goes with the upstream `getPostThread` and `getPosts`
//!   calls the response makes; the labels those labelers applied are kept
//!   on its views, and `atproto-content-labelers` names the ones applied.
//!   No other request header, and never the caller's DID or token, goes
//!   upstream.
//! - **Non-enumeration:** every per-URI failure is a typed gap. Request-level
//!   failures (malformed body, bad parameters, too many URIs) are
//!   `400 InvalidRequest` and depend only on the input and, under
//!   `members`, on the caller's membership and readiness; and, for a
//!   handle-form anchor, on whether the handle resolves, which depends on
//!   the network too (`HandleNotFound`, `UpstreamFailure`, `429`).
//! - **Handle-form anchor** (threadlens-handle-anchor design, feature
//!   `threadLens.handleAnchor`): `getThread` takes
//!   `at://<handle>/app.bsky.feed.post/<rkey>` (any case), resolves the
//!   handle as `resolveHandle` does after the cheap checks, and from then on
//!   uses the DID form only; the answer's `anchor` is DID-form.

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
use super::thread::{Request, get_thread_outlined as assemble, resolve as resolve_each};
use crate::appview_clients::accept_labelers::{AcceptLabelers, CONTENT_LABELERS, LabelerScope, with_labelers};
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
    /// The thread outline's reading side, when `[thread_outline]` is on
    /// (thread outline design §5).
    pub outline: Option<super::outline::Reader>,
}

/// The Thread Lens routes, merged into the app by `serve`.
pub fn router(state: ThreadLensState) -> Router {
    Router::new()
        .route(&crate::admin::usage::path("threadlens", GET_THREAD), post(get_thread))
        .route(&crate::admin::usage::path("threadlens", RESOLVE), post(resolve))
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
    let row: Option<(bool, bool)> = sqlx::query_as(crate::aggregations::enroll::MEMBER_READINESS_SQL)
        .bind(&caller.did)
        .fetch_optional(s.app.hydrator.pool())
        .await?;
    let ready = row.map(|(ready, _)| ready);
    let reindexing = row.is_some_and(|(ready, first_ready)| !ready && first_ready);
    // Read at each request: the admin UI changes it at once (admin-ui
    // design §8.5).
    let access = s.live.load().threadlens_access;
    let hydrate = match (ready, access) {
        (Some(true), _) => true,
        // Degraded-mode design §3.5: re-indexing after the first time.
        (Some(false), ThreadLensAccess::Members) if reindexing => {
            return Err(XrpcError::reindexing(s.app.membership.reindex_retry_secs, "threadlens"));
        }
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

/// The response's `atproto-content-labelers`, for a request that named its
/// labelers (threadlens-hydration design §3.6 (b)): those every scoped
/// upstream answer said it applied (threadlens-quoted-posts design §3), and
/// Prism's own when named. Absent otherwise.
fn content_labelers(scope: Option<&LabelerScope>) -> HeaderMap {
    let mut out = HeaderMap::new();
    if let Some(value) = scope.and_then(LabelerScope::content_labelers) {
        out.insert(axum::http::HeaderName::from_static(CONTENT_LABELERS), value);
    }
    out
}

/// The post collection, the only one a handle-form anchor may name.
const POST_COLLECTION: &str = "app.bsky.feed.post";

/// A handle-form anchor (threadlens-handle-anchor design §1.1): the handle,
/// lower-cased, and the record key, when `u` is exactly
/// `at://<handle>/app.bsky.feed.post/<rkey>` with a valid handle (any case)
/// and a valid record key. Checked before any resolution.
pub(super) fn handle_anchor(u: &str) -> Option<(String, String)> {
    let mut parts = u.strip_prefix("at://")?.split('/');
    let (authority, collection, rkey) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || collection != POST_COLLECTION || !proto_blue::syntax::RecordKey::is_valid(rkey) {
        return None;
    }
    let handle = authority.to_ascii_lowercase();
    proto_blue::syntax::Handle::is_valid(&handle).then(|| (handle, rkey.to_string()))
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
) -> Result<(HeaderMap, Json<PublicEmit<Value>>), XrpcError> {
    let (req, viewer) = gate(&s, &headers, peer, GET_THREAD).await?;
    let input: GetThreadInput = body(&bytes)?;
    let handle_form = if at_uri(&input.anchor) {
        None
    } else {
        Some(handle_anchor(&input.anchor).ok_or_else(|| XrpcError::invalid("Error: anchor must be an at-uri"))?)
    };
    let depth = input.depth.unwrap_or(DEFAULT_DEPTH);
    let parent_height = input.parent_height.unwrap_or(DEFAULT_PARENT_HEIGHT);
    if depth > MAX_DEPTH || parent_height > MAX_DEPTH {
        return Err(XrpcError::invalid("Error: depth and parentHeight must be 0-1000"));
    }
    // A handle-form anchor is resolved now, after the cheap checks, and only
    // the DID form is used from here on (threadlens-handle-anchor design
    // §1.2–§1.5). Errors map as `resolveHandle`'s do.
    let anchor = match handle_form {
        None => input.anchor.clone(),
        Some((handle, rkey)) => {
            let resolved = crate::xrpc::handlers::respond(&req, s.app.hydrator.resolve_handle(&req, &handle).await)?;
            let did = resolved
                .0
                .inner()
                .get("did")
                .and_then(Value::as_str)
                .filter(|d| proto_blue::syntax::Did::is_valid(d))
                .map(str::to_string)
                .ok_or_else(XrpcError::internal)?;
            format!("at://{did}/{POST_COLLECTION}/{rkey}")
        }
    };
    // A cursor that fails authentication, or was issued for another node,
    // is refused: the client restarts that node without one.
    let cursor = match input.replies_cursor {
        None => None,
        Some(raw) => match s.cursor_key.open(&anchor, &raw) {
            Some(position) => Some((position, raw)),
            None => return Err(XrpcError::invalid("Error: repliesCursor is not valid for this anchor")),
        },
    };
    let request = Request {
        viewer: &viewer,
        req: &req,
        anchor: &anchor,
        depth,
        parent_height,
    };
    let scope = AcceptLabelers::from_headers(&headers).map(|accept| LabelerScope::new(accept, s.ladder.hydrator().labeler_did()));
    let thread = with_labelers(scope.clone(), assemble(&s.ladder, &s.cfg, &s.cursor_key, s.outline.as_ref(), request, cursor)).await?;
    Ok((content_labelers(scope.as_deref()), Json(lens_thread(&anchor, &thread.nodes, thread.bridging, viewer.hydrate))))
}

pub async fn resolve(
    State(s): State<ThreadLensState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<(HeaderMap, Json<PublicEmit<Value>>), XrpcError> {
    let (req, viewer) = gate(&s, &headers, peer, RESOLVE).await?;
    let input: ResolveInput = body(&bytes)?;
    if input.uris.is_empty() || input.uris.len() > MAX_URIS {
        return Err(XrpcError::invalid("Error: uris must have between 1 and 25 items"));
    }
    if !input.uris.iter().all(|u| at_uri(u)) {
        return Err(XrpcError::invalid("Error: uris must be at-uris"));
    }
    let scope = AcceptLabelers::from_headers(&headers).map(|accept| LabelerScope::new(accept, s.ladder.hydrator().labeler_did()));
    let resolved = with_labelers(scope.clone(), resolve_each(&s.ladder, &s.cfg, &s.cursor_key, &viewer, &req, &input.uris)).await?;
    Ok((content_labelers(scope.as_deref()), Json(lens_items(&resolved.nodes, resolved.bridging, viewer.hydrate))))
}
