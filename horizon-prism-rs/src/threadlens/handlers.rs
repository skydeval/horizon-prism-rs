//! `app.nearhorizon.threadLens.getThread` and `.resolve` (design-v2 §10.1).
//!
//! - Both are procedures (POST, JSON input).
//! - **Auth** is required (a service JWT, §6.8). The authenticated DID is
//!   used for the auth gate and the rate-limit key (2/s, burst 10) and is
//!   then dropped: [`gate`] returns nothing, so no later step can hydrate,
//!   filter or classify by it.
//! - **Non-enumeration:** every per-URI failure is a typed gap. Request-level
//!   failures (malformed body, bad parameters, too many URIs) are
//!   `400 InvalidRequest` and depend only on the input.

use std::net::SocketAddr;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;

use super::ladder::{Ladder, Mode, Resolved};
use super::thread::{LADDER_CONCURRENCY, get_thread as assemble};
use crate::db::locks::uri_author;
use crate::hydration::PublicEmit;
use crate::hydration::lens::{LensNode, lens_items, lens_thread};
use crate::xrpc::AppState;
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::Class;

pub const GET_THREAD: &str = "app.nearhorizon.threadLens.getThread";
pub const RESOLVE: &str = "app.nearhorizon.threadLens.resolve";
/// `resolve` accepts 1–25 URIs.
pub const MAX_URIS: usize = 25;

/// Handler state.
#[derive(Clone)]
pub struct ThreadLensState {
    pub app: AppState,
    pub ladder: Ladder,
}

/// The Thread Lens routes, merged into the app by `serve`.
pub fn router(state: ThreadLensState) -> Router {
    Router::new()
        .route(&format!("/xrpc/{GET_THREAD}"), post(get_thread))
        .route(&format!("/xrpc/{RESOLVE}"), post(resolve))
        .with_state(state)
}

/// Authenticate and rate-limit. Returns nothing: the caller's identity does
/// not leave this function (§10.1, §10.3).
async fn gate(s: &ThreadLensState, headers: &HeaderMap, method: &str) -> Result<(), XrpcError> {
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = s.app.auth.verify(authorization, method).await?;
    s.app.limits.check(Class::ThreadLens, &caller.did)?;
    Ok(())
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
}

#[derive(Deserialize)]
struct ResolveInput {
    uris: Vec<String>,
}

pub async fn get_thread(
    State(s): State<ThreadLensState>,
    ConnectInfo(_peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<PublicEmit<Value>>, XrpcError> {
    gate(&s, &headers, GET_THREAD).await?;
    let input: GetThreadInput = body(&bytes)?;
    if !at_uri(&input.anchor) {
        return Err(XrpcError::invalid("Error: anchor must be an at-uri"));
    }
    let depth = input.depth.unwrap_or(6);
    let parent_height = input.parent_height.unwrap_or(30);
    if depth > 10 || parent_height > 30 {
        return Err(XrpcError::invalid("Error: depth must be 0-10 and parentHeight 0-30"));
    }
    let nodes = assemble(&s.ladder, &input.anchor, depth, parent_height).await?;
    Ok(Json(lens_thread(&input.anchor, &nodes)))
}

pub async fn resolve(
    State(s): State<ThreadLensState>,
    ConnectInfo(_peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<PublicEmit<Value>>, XrpcError> {
    gate(&s, &headers, RESOLVE).await?;
    let input: ResolveInput = body(&bytes)?;
    if input.uris.is_empty() || input.uris.len() > MAX_URIS {
        return Err(XrpcError::invalid("Error: uris must have between 1 and 25 items"));
    }
    if !input.uris.iter().all(|u| at_uri(u)) {
        return Err(XrpcError::invalid("Error: uris must be at-uris"));
    }
    let ladder = &s.ladder;
    let resolved: Vec<Result<LensNode, sqlx::Error>> = futures::stream::iter(input.uris)
        .map(|uri| async move {
            let r = ladder.resolve(&uri, Mode::Full).await?;
            Ok(match r {
                Resolved::Post(post) => LensNode::post(post),
                Resolved::Gap(gap) => LensNode::Gap { uri, parent_uri: None, gap },
            })
        })
        .buffered(LADDER_CONCURRENCY)
        .collect()
        .await;
    let items = resolved.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok(Json(lens_items(&items)))
}
