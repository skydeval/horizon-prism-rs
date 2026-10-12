//! `app.bsky.draft.createDraft`, `updateDraft`, `deleteDraft` (member;
//! writes) and `getDrafts` (member).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use serde_json::Value;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const CREATE_DRAFT: &str = "app.bsky.draft.createDraft";
pub const UPDATE_DRAFT: &str = "app.bsky.draft.updateDraft";
pub const DELETE_DRAFT: &str = "app.bsky.draft.deleteDraft";
pub const GET_DRAFTS: &str = "app.bsky.draft.getDrafts";

/// Drafts are the member's own state here: no readiness needed.
const MEMBER: Access = Access::Member { ready: false, enroll: false };

fn missing(name: &str) -> XrpcError {
    XrpcError::invalid(format!("Error: Input must have the property \"{name}\""))
}

pub async fn create_draft(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, CREATE_DRAFT, MEMBER, true).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let draft = input.get("draft").ok_or_else(|| missing("draft"))?;
    respond(&req.req, s.hydrator.create_draft(&req.viewer, draft).await)
}

/// The lexicon defines no output for an update or a delete.
pub async fn update_draft(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode, XrpcError> {
    let req = prepare(&s, &headers, peer, UPDATE_DRAFT, MEMBER, true).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let id = input.pointer("/draft/id").and_then(Value::as_str).ok_or_else(|| missing("draft.id"))?;
    let draft = input.pointer("/draft/draft").ok_or_else(|| missing("draft.draft"))?;
    respond(&req.req, s.hydrator.update_draft(&req.viewer, id, draft).await).map(|_| StatusCode::OK)
}

pub async fn delete_draft(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode, XrpcError> {
    let req = prepare(&s, &headers, peer, DELETE_DRAFT, MEMBER, true).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let id = input.get("id").and_then(Value::as_str).ok_or_else(|| missing("id"))?;
    respond(&req.req, s.hydrator.delete_draft(&req.viewer, id).await).map(|_| StatusCode::OK)
}

pub async fn get_drafts(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_DRAFTS, MEMBER, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_drafts(&req.viewer, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}
