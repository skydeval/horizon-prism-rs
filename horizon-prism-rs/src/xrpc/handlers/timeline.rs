//! `app.bsky.feed.getTimeline` (member, ready; triggers auto-enroll).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};

pub const GET_TIMELINE: &str = "app.bsky.feed.getTimeline";

pub async fn get_timeline(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let access = Access::Member { ready: true, enroll: true };
    let req = prepare(&s, &headers, peer, GET_TIMELINE, access, false).await?;
    let p = Params::parse(q.as_deref());
    respond(s.hydrator.get_timeline(&req.viewer, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}
