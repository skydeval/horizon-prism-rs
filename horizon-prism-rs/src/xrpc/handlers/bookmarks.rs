//! `app.bsky.bookmark.createBookmark`, `deleteBookmark` (member; writes)
//! and `getBookmarks` (member; a viewer-private listing).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;
use serde_json::Value;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const CREATE_BOOKMARK: &str = "app.bsky.bookmark.createBookmark";
pub const DELETE_BOOKMARK: &str = "app.bsky.bookmark.deleteBookmark";
pub const GET_BOOKMARKS: &str = "app.bsky.bookmark.getBookmarks";

/// Bookmarks are the member's own state here: no readiness needed.
const MEMBER: Access = Access::Member { ready: false, enroll: false };

fn field<'a>(input: &'a Value, name: &str) -> Result<&'a str, XrpcError> {
    input
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| XrpcError::invalid(format!("Error: Input must have the property \"{name}\"")))
}

pub async fn create_bookmark(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, CREATE_BOOKMARK, MEMBER, true).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let (uri, cid) = (field(&input, "uri")?, field(&input, "cid")?);
    respond(&req.req, s.hydrator.create_bookmark(&req.viewer, uri, cid).await)
}

pub async fn delete_bookmark(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, DELETE_BOOKMARK, MEMBER, true).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    respond(&req.req, s.hydrator.delete_bookmark(&req.viewer, field(&input, "uri")?).await)
}

/// A viewer-private listing: an inbound list block applies from the
/// viewer's cache only, with no Farsight call (design-v3 §5.14.5).
pub async fn get_bookmarks(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_BOOKMARKS, MEMBER, false).await?;
    if let Some(lookup) = crate::xrpc::backstop::farsight() {
        lookup.set_cache_only();
    }
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator.get_bookmarks(&req.viewer, &req.req, clamp_limit(p.u32("limit")?), p.one("cursor")).await,
    )
}
