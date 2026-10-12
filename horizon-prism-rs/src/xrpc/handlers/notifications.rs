//! `app.bsky.notification.listNotifications`, `getUnreadCount` (member,
//! ready; trigger auto-enroll) and `updateSeen` (member, ready; a write).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};
use crate::xrpc::errors::XrpcError;

pub const LIST_NOTIFICATIONS: &str = "app.bsky.notification.listNotifications";
pub const GET_UNREAD_COUNT: &str = "app.bsky.notification.getUnreadCount";
pub const UPDATE_SEEN: &str = "app.bsky.notification.updateSeen";

const TRIGGER: Access = Access::Member { ready: true, enroll: true };

pub async fn list_notifications(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, LIST_NOTIFICATIONS, TRIGGER, false).await?;
    let p = Params::parse(q.as_deref());
    // design-v3 §11 G-5: at `b3d7c33` the lexicon says `seenAt` here is
    // "unsupported and will cause an error". `priority` is deprecated and
    // ignored.
    if p.one("seenAt").is_some() {
        return Err(XrpcError::invalid("Error: seenAt is not supported on listNotifications"));
    }
    respond(
        s.hydrator
            .list_notifications(&req.viewer, clamp_limit(p.u32("limit")?), p.one("cursor"), &p.all("reasons"))
            .await,
    )
}

pub async fn get_unread_count(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_UNREAD_COUNT, TRIGGER, false).await?;
    // design-v3 §11 G-4: `seenAt`, when present, is the comparison point.
    // `priority` is deprecated and ignored.
    let seen_at = match Params::parse(q.as_deref()).one("seenAt") {
        None => None,
        Some(raw) => Some(
            DateTime::parse_from_rfc3339(raw)
                .map_err(|_| XrpcError::invalid("Error: seenAt must be a datetime"))?
                .with_timezone(&Utc),
        ),
    };
    respond(s.hydrator.unread_count_since(&req.viewer, seen_at).await)
}

pub async fn update_seen(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let access = Access::Member { ready: true, enroll: false };
    let req = prepare(&s, &headers, peer, UPDATE_SEEN, access, true).await?;
    let seen_at = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("seenAt").and_then(Value::as_str).map(str::to_string))
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"seenAt\""))?;
    respond(s.hydrator.update_seen(&req.viewer, &seen_at).await)
}
