//! `app.bsky.graph.getFollows`, `getFollowers`, `getList`, `getLists`,
//! `getStarterPack`, `getStarterPacks`, `getActorStarterPacks`.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::HeaderMap;

use super::{XrpcResult, respond};
use crate::hydration::feed::clamp_limit;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Access, Params, prepare};

pub const GET_FOLLOWS: &str = "app.bsky.graph.getFollows";
pub const GET_FOLLOWERS: &str = "app.bsky.graph.getFollowers";
pub const GET_LIST: &str = "app.bsky.graph.getList";
pub const GET_LISTS: &str = "app.bsky.graph.getLists";
pub const GET_RELATIONSHIPS: &str = "app.bsky.graph.getRelationships";
pub const GET_LISTS_WITH_MEMBERSHIP: &str = "app.bsky.graph.getListsWithMembership";
pub const GET_STARTER_PACKS_WITH_MEMBERSHIP: &str = "app.bsky.graph.getStarterPacksWithMembership";
pub const GET_STARTER_PACK: &str = "app.bsky.graph.getStarterPack";
pub const GET_STARTER_PACKS: &str = "app.bsky.graph.getStarterPacks";
pub const GET_ACTOR_STARTER_PACKS: &str = "app.bsky.graph.getActorStarterPacks";

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

pub async fn get_list(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_LIST, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_list(&req.viewer, &req.req, p.required("list")?, clamp_limit(p.u32("limit")?), p.one("cursor")).await)
}

pub async fn get_lists(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_LISTS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .get_lists(&req.viewer, &req.req, p.required("actor")?, &p.all("purposes"), clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
}

pub async fn get_starter_pack(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_STARTER_PACK, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_starter_pack(&req.viewer, &req.req, p.required("starterPack")?).await)
}

pub async fn get_starter_packs(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_STARTER_PACKS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_starter_packs(&req.viewer, &req.req, &p.all("uris")).await)
}

pub async fn get_actor_starter_packs(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_ACTOR_STARTER_PACKS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .get_actor_starter_packs(&req.viewer, &req.req, p.required("actor")?, clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
}

pub async fn get_relationships(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_RELATIONSHIPS, Access::Opt, false).await?;
    let p = Params::parse(q.as_deref());
    respond(&req.req, s.hydrator.get_relationships(&req.req, p.required("actor")?, &p.all("others")).await)
}

/// The viewer's own lists and packs: a ready member.
const OWN: Access = Access::Member { ready: true, enroll: false };

pub async fn get_lists_with_membership(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_LISTS_WITH_MEMBERSHIP, OWN, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .get_lists_with_membership(&req.viewer, &req.req, p.required("actor")?, &p.all("purposes"), clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
}

pub async fn get_starter_packs_with_membership(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> XrpcResult {
    let req = prepare(&s, &headers, peer, GET_STARTER_PACKS_WITH_MEMBERSHIP, OWN, false).await?;
    let p = Params::parse(q.as_deref());
    respond(
        &req.req,
        s.hydrator
            .get_starter_packs_with_membership(&req.viewer, &req.req, p.required("actor")?, clamp_limit(p.u32("limit")?), p.one("cursor"))
            .await,
    )
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
