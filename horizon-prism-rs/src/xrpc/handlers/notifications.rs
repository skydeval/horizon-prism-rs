//! `app.bsky.notification.listNotifications`, `getUnreadCount` (member,
//! ready; trigger auto-enroll) and `updateSeen` (member, ready; a write).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
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
pub const GET_PREFERENCES: &str = "app.bsky.notification.getPreferences";
pub const PUT_PREFERENCES: &str = "app.bsky.notification.putPreferences";
pub const PUT_PREFERENCES_V2: &str = "app.bsky.notification.putPreferencesV2";
pub const LIST_ACTIVITY_SUBSCRIPTIONS: &str = "app.bsky.notification.listActivitySubscriptions";
pub const PUT_ACTIVITY_SUBSCRIPTION: &str = "app.bsky.notification.putActivitySubscription";

const TRIGGER: Access = Access::Member { ready: true, enroll: true };
/// Preferences are the member's own state here: no readiness needed.
const MEMBER: Access = Access::Member { ready: false, enroll: false };

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
        &req.req,
        s.hydrator
            .list_notifications(&req.viewer, &req.req, clamp_limit(p.u32("limit")?), p.one("cursor"), &p.all("reasons"))
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
    respond(&req.req, s.hydrator.unread_count_since(&req.viewer, seen_at).await)
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
    respond(&req.req, s.hydrator.update_seen(&req.viewer, &seen_at).await)
}

pub async fn get_preferences(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_PREFERENCES, MEMBER, false).await?;
    respond(&req.req, s.hydrator.get_notification_preferences(&req.viewer).await)
}

pub async fn put_preferences_v2(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, PUT_PREFERENCES_V2, MEMBER, true).await?;
    let input = serde_json::from_slice::<Value>(&body)
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| XrpcError::invalid("Error: Input must be an object"))?;
    respond(&req.req, s.hydrator.put_notification_preferences(&req.viewer, &input).await)
}

/// A viewer-private listing: an inbound list block applies from the
/// viewer's cache only, with no Farsight call (design-v3 §5.14.5).
pub async fn list_activity_subscriptions(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, LIST_ACTIVITY_SUBSCRIPTIONS, MEMBER, false).await?;
    if let Some(lookup) = crate::xrpc::backstop::farsight() {
        lookup.set_cache_only();
    }
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .list_activity_subscriptions(&req.viewer, &req.req, clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
}

/// Turning a subscription off needs no readiness, so the access level asks
/// for none; turning one on checks it in the hydrator.
pub async fn put_activity_subscription(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, PUT_ACTIVITY_SUBSCRIPTION, MEMBER, true).await?;
    let input = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let subject = input
        .get("subject")
        .and_then(Value::as_str)
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"subject\""))?;
    let flag = |name: &str| input.pointer(&format!("/activitySubscription/{name}")).and_then(Value::as_bool);
    let (Some(post), Some(reply)) = (flag("post"), flag("reply")) else {
        return Err(XrpcError::invalid("Error: Input must have the property \"activitySubscription\""));
    };
    respond(&req.req, s.hydrator.put_activity_subscription(&req.viewer, subject, post, reply).await)
}

/// The legacy `putPreferences {priority}` (design-v3 §5.6): accepted so old
/// clients do not fail, and nothing is stored, because `priority` no longer
/// means anything. The lexicon defines no output.
pub async fn put_preferences(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<StatusCode, XrpcError> {
    prepare(&s, &headers, peer, PUT_PREFERENCES, MEMBER, true).await?;
    serde_json::from_slice::<Value>(&body)
        .ok()
        .filter(|v| v.get("priority").is_some_and(Value::is_boolean))
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"priority\""))?;
    Ok(StatusCode::OK)
}
