//! `app.nearhorizon.plural.getRecords` and `listMembers` (plural-pronouns
//! design §2.8): queries, no sign-in needed. Signed out they are charged by
//! IP to the tight `plural_anon` class; with a service token that verifies,
//! by DID to `plural_auth` (signed-out re-asks a lot less frequent). Every 429 carries `Retry-After`. Kept in every
//! public-indexing mode, CORS as every route. With `plural.enabled` off
//! both answer 501 `MethodNotImplemented` from their own routes.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use serde_json::{Value, json};

use super::fill::Filler;
use super::{
    Answer, Facts, GET_RECORDS, LIST_MEMBERS, METRIC_ANSWERS, METRIC_READ_SECONDS, METRIC_READS, NSID_PLURAL_FRONT, NSID_PLURAL_MEMBER,
    NSID_PLURAL_SYSTEM, NSID_PRONOUNS, RepoState, answer, parse_member_uri, reads, record_uri,
};
use crate::firehose::watermark::Watermarks;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Params, request_handle, request_ip};
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::Family;

/// `getRecords`' caps (§2.8).
pub const MAX_ACTORS: usize = 50;
pub const MAX_MEMBERS: usize = 100;
/// `listMembers`' page size.
pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 100;
/// `cache-control` with every answer known; with any unknown for a
/// signed-in caller; with any unknown for a signed-out one (its client
/// re-asks after 30 s anyway). `record` and `none` stay within the minute.
pub const CACHE_KNOWN: &str = "public, max-age=30";
pub const CACHE_UNKNOWN: &str = "public, max-age=5";
pub const CACHE_UNKNOWN_ANON: &str = "public, max-age=30";

/// Whether the firehose is current (§2.5).
#[derive(Clone)]
pub enum Freshness {
    /// The newest relay watermark within the bound.
    Watermarks(Watermarks, Duration),
    /// Fixed (tests).
    Fixed(bool),
}

impl Freshness {
    #[must_use]
    pub fn current(&self) -> bool {
        match self {
            Self::Fixed(f) => *f,
            Self::Watermarks(w, bound) => w
                .snapshot()
                .into_values()
                .flatten()
                .max()
                .is_some_and(|newest| (Utc::now() - newest).to_std().map_or(true, |lag| lag <= *bound)),
        }
    }
}

/// Handler state.
#[derive(Clone)]
pub struct PluralState {
    pub app: AppState,
    pub filler: Filler,
    pub fresh: Freshness,
}

/// The two routes, merged into the app by `serve`.
pub fn router(state: PluralState) -> Router {
    Router::new()
        .route(&crate::admin::usage::path("plural", GET_RECORDS), get(get_records))
        .route(&crate::admin::usage::path("plural", LIST_MEMBERS), get(list_members))
        .with_state(state)
}

fn not_served() -> XrpcError {
    XrpcError::new(StatusCode::NOT_IMPLEMENTED, "MethodNotImplemented", "Pronouns and plural records are not served here")
}

/// Charge the call (§3): a token that verifies charges `plural_auth` by
/// DID; no token charges `plural_anon` by IP; a token that does not verify
/// is refused. Returns whether the caller is signed in.
async fn charge(s: &PluralState, headers: &HeaderMap, ip: std::net::IpAddr, method: &str) -> Result<bool, XrpcError> {
    let authorization = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    let caller = match authorization {
        Some(_) => Some(crate::xrpc::auth::authenticate(&s.app, authorization, method).await?),
        None => None,
    };
    s.app.limits.charge(method, Family::Plural, caller.as_ref().map(|c| c.did.as_str()), ip)?;
    Ok(caller.is_some())
}

/// What one read answered: its body, whether any answer is `unknown`, and
/// whether the caller is signed in.
type Done = (Value, bool, bool);

fn finish(method: &'static str, begun: Instant, result: Result<Done, XrpcError>) -> Result<Response, XrpcError> {
    metrics::histogram!(METRIC_READ_SECONDS, "method" => method).record(begun.elapsed().as_secs_f64());
    match result {
        Ok((body, unknown, signed_in)) => {
            metrics::counter!(METRIC_READS, "method" => method, "outcome" => "ok").increment(1);
            let mut response = Json(body).into_response();
            let cache = match (unknown, signed_in) {
                (false, _) => CACHE_KNOWN,
                (true, true) => CACHE_UNKNOWN,
                (true, false) => CACHE_UNKNOWN_ANON,
            };
            response.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
            Ok(response)
        }
        Err(e) => {
            let outcome = match e.status {
                StatusCode::BAD_REQUEST => "invalid",
                StatusCode::TOO_MANY_REQUESTS => "limited",
                StatusCode::NOT_IMPLEMENTED => "disabled",
                _ => "error",
            };
            metrics::counter!(METRIC_READS, "method" => method, "outcome" => outcome).increment(1);
            Err(e)
        }
    }
}

/// Distinct values in first-seen order.
fn distinct(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values.into_iter().filter(|v| seen.insert(v.clone())).collect()
}

/// The states of `dids`, after starting the fills a read needs and waiting
/// for them within the inline budget (§2.4 "On ask").
async fn states(s: &PluralState, dids: &[String]) -> Result<HashMap<String, super::RepoRow>, XrpcError> {
    let pool = s.filler.pool();
    let rows = reads::repos(pool, dids).await?;
    let waits = s.filler.on_ask(dids, &rows).await?;
    if waits.is_empty() {
        return Ok(rows);
    }
    s.filler.wait(waits).await;
    Ok(reads::repos(pool, dids).await?)
}

pub async fn get_records(
    State(s): State<PluralState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let begun = Instant::now();
    let result = get_records_inner(&s, peer, &headers, q.as_deref()).await;
    finish("getRecords", begun, result)
}

async fn get_records_inner(s: &PluralState, peer: SocketAddr, headers: &HeaderMap, q: Option<&str>) -> Result<Done, XrpcError> {
    let ip = request_ip(&s.app, headers, peer);
    let signed_in = charge(s, headers, ip, GET_RECORDS).await?;
    if !s.filler.config().enabled {
        return Err(not_served());
    }
    let params = Params::parse(q);
    let actors = distinct(params.all("actors"));
    let members = distinct(params.all("members"));
    if actors.is_empty() && members.is_empty() {
        return Err(XrpcError::invalid("Error: Params must have the property \"actors\" or \"members\""));
    }
    if actors.len() > MAX_ACTORS {
        return Err(XrpcError::invalid(format!("Error: actors must not have more than {MAX_ACTORS} elements")));
    }
    if members.len() > MAX_MEMBERS {
        return Err(XrpcError::invalid(format!("Error: members must not have more than {MAX_MEMBERS} elements")));
    }
    if let Some(bad) = actors.iter().find(|d| !proto_blue::syntax::Did::is_valid(d)) {
        return Err(XrpcError::invalid(format!("Error: actors must be DIDs: {bad:?}")));
    }
    let mut member_dids = Vec::with_capacity(members.len());
    for m in &members {
        let Some((did, _)) = parse_member_uri(m) else {
            return Err(XrpcError::invalid(format!("Error: members must be at://<did>/{NSID_PLURAL_MEMBER}/<rkey> URIs: {m:?}")));
        };
        member_dids.push(did.to_string());
    }
    let dids = distinct(actors.iter().cloned().chain(member_dids).collect());
    if dids.len() > MAX_ACTORS {
        return Err(XrpcError::invalid(format!("Error: actors and members together must name at most {MAX_ACTORS} accounts")));
    }
    let repos = states(s, &dids).await?;
    // Every URI this answer reads.
    let selfs = [("pronouns", NSID_PRONOUNS), ("system", NSID_PLURAL_SYSTEM), ("front", NSID_PLURAL_FRONT)];
    let mut uris: Vec<String> = actors.iter().flat_map(|d| selfs.iter().map(move |(_, c)| record_uri(d, c, "self"))).collect();
    uris.extend(members.iter().cloned());
    let stored = reads::records(s.filler.pool(), &uris).await?;
    let req = request_handle(None, ip);
    let subjects: Vec<String> = dids.iter().cloned().chain(members.iter().cloned()).collect();
    let gates = s.app.hydrator.gate_many(&req, &subjects).await?;
    let hidden = |subject: &str| gates.get(subject).is_some_and(|g| g.is_hidden());
    let fresh = s.fresh.current();
    let mut unknown = false;
    let mut decide = |kind: &'static str, did: &str, uri: &str, member: bool| -> Value {
        let a = answer(uri, Facts { hidden: hidden(did) || (member && hidden(uri)), repo: repos.get(did), stored: stored.get(uri), fresh, member });
        unknown |= a == Answer::Unknown;
        metrics::counter!(METRIC_ANSWERS, "kind" => kind, "status" => a.status()).increment(1);
        a.to_json()
    };
    let mut actor_out = Vec::with_capacity(actors.len());
    for did in &actors {
        let mut row = json!({"did": did});
        for (field, collection) in selfs {
            row[field] = decide(field, did, &record_uri(did, collection, "self"), false);
        }
        actor_out.push(row);
    }
    let mut member_out = Vec::with_capacity(members.len());
    for uri in &members {
        let did = parse_member_uri(uri).map(|(d, _)| d).unwrap_or_default();
        let mut a = decide("member", did, uri, true);
        if let Value::Object(m) = &mut a {
            m.insert("uri".into(), json!(uri));
        }
        member_out.push(a);
    }
    Ok((json!({"actors": actor_out, "members": member_out}), unknown, signed_in))
}

pub async fn list_members(
    State(s): State<PluralState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let begun = Instant::now();
    let result = list_members_inner(&s, peer, &headers, q.as_deref()).await;
    finish("listMembers", begun, result)
}

async fn list_members_inner(s: &PluralState, peer: SocketAddr, headers: &HeaderMap, q: Option<&str>) -> Result<Done, XrpcError> {
    let ip = request_ip(&s.app, headers, peer);
    let signed_in = charge(s, headers, ip, LIST_MEMBERS).await?;
    if !s.filler.config().enabled {
        return Err(not_served());
    }
    let params = Params::parse(q);
    let actor = params.required("actor")?.to_string();
    if !proto_blue::syntax::Did::is_valid(&actor) {
        return Err(XrpcError::invalid("Error: actor must be a DID"));
    }
    let limit = params.u32("limit")?.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(XrpcError::invalid(format!("Error: limit must be 1 to {MAX_LIMIT}")));
    }
    let cursor = params.one("cursor").map(str::to_string);
    if cursor.as_deref().is_some_and(|c| !proto_blue::syntax::RecordKey::is_valid(c)) {
        return Err(XrpcError::invalid("Error: cursor is not valid"));
    }
    let repos = states(s, std::slice::from_ref(&actor)).await?;
    let req = request_handle(None, ip);
    let gate = s.app.hydrator.gate_many(&req, std::slice::from_ref(&actor)).await?;
    let unknown = json!({"actor": actor, "status": "unknown", "members": []});
    let row = repos.get(&actor);
    let listed = row.is_some_and(|r| r.state == RepoState::Complete);
    if gate.get(&actor).is_some_and(|g| g.is_hidden()) || !listed {
        // `gone`: the account is gone, so it has no members.
        if row.is_some_and(|r| r.state == RepoState::Gone) {
            return Ok((json!({"actor": actor, "status": "complete", "members": []}), false, signed_in));
        }
        return Ok((unknown, true, signed_in));
    }
    let page = reads::member_page(s.filler.pool(), &actor, cursor.as_deref(), i64::from(limit) + 1).await?;
    let more = page.len() > usize::try_from(limit).unwrap_or(usize::MAX);
    let page: Vec<_> = page.into_iter().take(usize::try_from(limit).unwrap_or(usize::MAX)).collect();
    let uris: Vec<String> = page.iter().map(|(u, ..)| u.clone()).collect();
    let gates = s.app.hydrator.gate_many(&req, &uris).await?;
    let mut partial = row.is_some_and(|r| r.members_truncated) || !s.fresh.current();
    let mut members = Vec::with_capacity(page.len());
    for (uri, rkey, cid, value) in &page {
        if gates.get(uri).is_some_and(|g| g.is_hidden()) {
            continue;
        }
        let Some(value) = value else {
            partial = true;
            continue;
        };
        members.push(json!({"uri": uri, "rkey": rkey, "cid": cid, "value": value}));
    }
    let mut out = json!({"actor": actor, "status": if partial { "partial" } else { "complete" }, "members": members});
    if more && let Some((_, rkey, ..)) = page.last() {
        out["cursor"] = json!(rkey);
    }
    Ok((out, partial, signed_in))
}
