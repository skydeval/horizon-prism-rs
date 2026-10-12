//! `app.bsky.graph.getFollows`, `getFollowers`.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};

pub const GET_FOLLOWS: &str = "app.bsky.graph.getFollows";
pub const GET_FOLLOWERS: &str = "app.bsky.graph.getFollowers";

pub async fn get_follows(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_FOLLOWS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_follows(&req.viewer, &req.req, p.required("actor")?, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

pub async fn get_followers(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_FOLLOWERS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .get_followers(&req.viewer, &req.req, p.required("actor")?, clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
}
