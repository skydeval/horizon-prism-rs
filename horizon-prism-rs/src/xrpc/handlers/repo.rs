//! `com.atproto.repo.getRecord` (public): stored public records only; and
//! `com.atproto.identity.resolveHandle` (public).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};

pub const GET_RECORD: &str = "com.atproto.repo.getRecord";
pub const RESOLVE_HANDLE: &str = "com.atproto.identity.resolveHandle";

pub async fn resolve_handle(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, RESOLVE_HANDLE, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.resolve_handle(&req.req, p.required("handle")?).await)
}

pub async fn get_record(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_RECORD, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .repo_get_record(p.required("repo")?, p.required("collection")?, p.required("rkey")?, p.one("cid"))
            .await,
    )
}
