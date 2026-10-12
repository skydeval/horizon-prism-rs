//! `com.atproto.label.queryLabels` (public).

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::labels::DEFAULT_LIMIT;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};

pub const QUERY_LABELS: &str = "com.atproto.label.queryLabels";

pub async fn query_labels(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, QUERY_LABELS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    let limit = p.u32("limit")?.unwrap_or(DEFAULT_LIMIT);
    respond(
        &req.req,
        s.hydrator.query_labels(&p.all("uriPatterns"), &p.all("sources"), limit, p.one("cursor")).await,
    )
}
