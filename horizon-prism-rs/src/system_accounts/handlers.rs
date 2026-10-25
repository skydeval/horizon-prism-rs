//! The five `app.nearhorizon.account.*` methods (design §2). Routed in every
//! public-indexing mode, CORS as every route, `Cache-Control: private,
//! no-store` on every answer. With the feature off each answers `501
//! MethodNotImplemented` before authentication.
//!
//! Every call authenticates as Prism's other authed methods do
//! ([`authenticate`]); the three writes add the token rules ([`tokens`]):
//! a bearer living over [`MAX_LIFETIME`] is `401 InvalidToken`, and each
//! sensitive token is used once, counted at commit. The proof is verified
//! with the verifier itself, so the backstop records nothing about the
//! listed account (R1-2). Errors are logged by SQLSTATE only.

use std::net::SocketAddr;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::SecondsFormat;
use serde_json::{Map, Value, json};

use super::store::{self, Refusal, StoreError};
use super::tokens::{UsedToken, lifetime_ok, used_token};
use super::{
    ADD_SYSTEM_ACCOUNT, CONFIRM_SYSTEM_ACCOUNT, GET_SYSTEM_ACCOUNT_STATUS, GET_SYSTEM_ACCOUNTS, Keys, LEAVE_SYSTEM_ACCOUNTS, MAX_BODY,
    MAX_ENTRIES, MAX_LIFETIME, METRIC_REQUESTS, REMOVE_SYSTEM_ACCOUNT,
};
use crate::kryphocron_glue::VerifiedCaller;
use crate::xrpc::AppState;
use crate::xrpc::auth::{authenticate, request_ip};
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::{Class, Family};

/// `Cache-Control` on every answer: it is the caller's own.
pub const CACHE_CONTROL: &str = "private, no-store";

/// Handler state.
#[derive(Clone)]
pub struct SystemAccountsState {
    pub app: AppState,
    /// `None`: the feature is off.
    pub keys: Option<Keys>,
}

/// The five routes, merged into the app by `serve`.
pub fn router(state: SystemAccountsState) -> Router {
    let path = |nsid: &str| crate::admin::usage::path("account", nsid);
    Router::new()
        .route(&path(GET_SYSTEM_ACCOUNTS), get(get_system_accounts))
        .route(&path(ADD_SYSTEM_ACCOUNT), post(add_system_account))
        .route(&path(REMOVE_SYSTEM_ACCOUNT), post(remove_system_account))
        .route(&path(GET_SYSTEM_ACCOUNT_STATUS), get(get_system_account_status))
        .route(&path(LEAVE_SYSTEM_ACCOUNTS), post(leave_system_accounts))
        .with_state(state)
}

/// A refusal and its metric outcome.
struct Fail {
    error: XrpcError,
    outcome: &'static str,
}

impl From<XrpcError> for Fail {
    fn from(error: XrpcError) -> Self {
        let outcome = match error.status {
            StatusCode::UNAUTHORIZED => "unauthorized",
            StatusCode::TOO_MANY_REQUESTS => "limited",
            StatusCode::BAD_REQUEST => "invalid",
            StatusCode::NOT_IMPLEMENTED => "disabled",
            _ => "error",
        };
        Self { error, outcome }
    }
}

impl From<crate::xrpc::rate_limit::Limited> for Fail {
    fn from(l: crate::xrpc::rate_limit::Limited) -> Self {
        XrpcError::from(l).into()
    }
}

fn fail(error: XrpcError, outcome: &'static str) -> Fail {
    Fail { error, outcome }
}

fn invalid(message: &str) -> Fail {
    XrpcError::invalid(message).into()
}

fn invalid_proof(message: &str) -> Fail {
    fail(XrpcError::new(StatusCode::BAD_REQUEST, "InvalidProof", message), "invalid_proof")
}

/// A database failure, logged by its code only.
fn internal(method: &'static str, code: &str) -> Fail {
    tracing::error!(event = "system_accounts_db_error", method, code, "a system-accounts request failed in the database");
    fail(XrpcError::internal(), "error")
}

fn from_store(method: &'static str, e: StoreError) -> Fail {
    match e {
        StoreError::Refused(Refusal::BearerUsed) => fail(XrpcError::new(StatusCode::UNAUTHORIZED, "InvalidToken", "Token already used"), "replay"),
        StoreError::Refused(Refusal::ProofUsed) => {
            fail(XrpcError::new(StatusCode::BAD_REQUEST, "InvalidProof", "The proof was already used"), "replay")
        }
        StoreError::Refused(Refusal::ListFull) => fail(
            XrpcError::new(StatusCode::BAD_REQUEST, "ListFull", format!("Up to {MAX_ENTRIES} system accounts")),
            "list_full",
        ),
        StoreError::Db(code) => internal(method, &code),
    }
}

/// The answer, counted, with `Cache-Control`.
fn finish(method: &'static str, result: Result<Value, Fail>) -> Response {
    let (mut response, outcome) = match result {
        Ok(body) => (Json(body).into_response(), "ok"),
        Err(f) => (f.error.into_response(), f.outcome),
    };
    metrics::counter!(METRIC_REQUESTS, "method" => method, "outcome" => outcome).increment(1);
    response.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_CONTROL));
    response
}

fn not_served() -> XrpcError {
    XrpcError::new(StatusCode::NOT_IMPLEMENTED, "MethodNotImplemented", "System accounts are not served here")
}

/// What every method starts with: the feature, the caller, and, for a
/// write, its bearer's lifetime and stored identity; then the rate charge.
struct Gate {
    keys: Keys,
    caller: VerifiedCaller,
    /// The bearer's stored identity, for the three writes.
    bearer: Option<UsedToken>,
}

async fn gate(s: &SystemAccountsState, headers: &HeaderMap, peer: SocketAddr, method: &str, write: bool) -> Result<Gate, Fail> {
    let ip = request_ip(&s.app, headers, peer);
    let Some(keys) = s.keys.clone() else {
        s.app.limits.check_ip(Class::AnonRead, ip)?;
        return Err(not_served().into());
    };
    let authorization = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    let caller = authenticate(&s.app, authorization, method).await?;
    let bearer = if write {
        if !lifetime_ok(&caller.jwt) {
            return Err(fail(
                XrpcError::new(
                    StatusCode::UNAUTHORIZED,
                    "InvalidToken",
                    format!("Token lifetime is over {} seconds; this method takes a fresh one-shot token", MAX_LIFETIME.as_secs()),
                ),
                "unauthorized",
            ));
        }
        Some(used_token(&keys, authorization.unwrap_or_default(), &caller.did, caller.jwt.expires_at()))
    } else {
        None
    };
    s.app.limits.charge(method, Family::SystemAccounts, Some(&caller.did), ip)?;
    Ok(Gate { keys, caller, bearer })
}

/// A write's JSON object body; an empty body is `{}` where `empty_ok`.
fn object(body: &Bytes, empty_ok: bool) -> Result<Map<String, Value>, Fail> {
    if body.len() > MAX_BODY {
        return Err(invalid("Error: request body is too large"));
    }
    if body.iter().all(u8::is_ascii_whitespace) && empty_ok {
        return Ok(Map::new());
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(invalid("Error: input must be a JSON object")),
    }
}

/// The `account` property: a DID.
fn account(input: &Map<String, Value>) -> Result<String, Fail> {
    match input.get("account").and_then(Value::as_str) {
        Some(a) if proto_blue::syntax::Did::is_valid(a) => Ok(a.to_string()),
        Some(_) => Err(invalid("Error: account must be a DID")),
        None => Err(invalid("Error: Input must have the property \"account\"")),
    }
}

/// A DID without any `#fragment`.
fn without_fragment(did: &str) -> &str {
    did.split_once('#').map_or(did, |(d, _)| d)
}

/// Verify the proof with the verifier itself (R1-2): a failure on a request
/// whose identity budget ran out is a 429, as on the auth path; any other
/// failure is `InvalidProof`.
async fn verify_proof(app: &AppState, proof: &str) -> Result<VerifiedCaller, Fail> {
    match app.auth.verify(Some(proof), CONFIRM_SYSTEM_ACCOUNT).await {
        Ok(verified) => Ok(verified),
        Err(_) => match crate::xrpc::backstop::selected().and_then(|s| s.req.exhausted()) {
            Some(exhausted) => {
                metrics::counter!(crate::xrpc::auth::METRIC_AUTH_BUDGET_EXHAUSTED).increment(1);
                Err(XrpcError::from(exhausted).into())
            }
            None => Err(invalid_proof("The account's server did not confirm it")),
        },
    }
}

pub async fn get_system_accounts(State(s): State<SystemAccountsState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    finish("getSystemAccounts", get_inner(&s, peer, &headers).await)
}

async fn get_inner(s: &SystemAccountsState, peer: SocketAddr, headers: &HeaderMap) -> Result<Value, Fail> {
    let g = gate(s, headers, peer, GET_SYSTEM_ACCOUNTS, false).await?;
    let owner = g.caller.did.as_str();
    let (rev, entries) = store::list(s.app.hydrator.pool(), owner)
        .await
        .map_err(|e| from_store("getSystemAccounts", e))?;
    let mut accounts = Vec::with_capacity(entries.len());
    for e in entries {
        let Some(did) = g.keys.open(owner, &e.index, &e.sealed) else {
            return Err(internal("getSystemAccounts", "unseal"));
        };
        accounts.push(json!({"did": did, "addedAt": e.added_at.to_rfc3339_opts(SecondsFormat::Millis, true)}));
    }
    Ok(json!({"accounts": accounts, "rev": rev}))
}

pub async fn add_system_account(
    State(s): State<SystemAccountsState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    finish("addSystemAccount", add_inner(&s, peer, &headers, &body).await)
}

async fn add_inner(s: &SystemAccountsState, peer: SocketAddr, headers: &HeaderMap, body: &Bytes) -> Result<Value, Fail> {
    let g = gate(s, headers, peer, ADD_SYSTEM_ACCOUNT, true).await?;
    let input = object(body, false)?;
    let account = account(&input)?;
    let Some(proof) = input.get("proof").and_then(Value::as_str).filter(|p| !p.trim().is_empty()) else {
        return Err(invalid("Error: Input must have the property \"proof\""));
    };
    if account == g.caller.did {
        return Err(invalid("Error: account must not be the requester"));
    }
    let proven = verify_proof(&s.app, proof).await?;
    if !lifetime_ok(&proven.jwt) {
        return Err(invalid_proof("The proof's lifetime is too long"));
    }
    if without_fragment(&proven.did) != account {
        return Err(invalid_proof("The proof was issued by another account"));
    }
    let proof_token = used_token(&g.keys, proof, &proven.did, proven.jwt.expires_at());
    let index = g.keys.index(&account);
    let Some(sealed) = g.keys.seal(&g.caller.did, &index, &account) else {
        return Err(internal("addSystemAccount", "seal"));
    };
    let Some(bearer) = g.bearer.as_ref() else {
        return Err(internal("addSystemAccount", "bearer"));
    };
    let rev = store::add(s.app.hydrator.pool(), &g.caller.did, &index, &sealed, bearer, &proof_token)
        .await
        .map_err(|e| from_store("addSystemAccount", e))?;
    Ok(json!({"rev": rev}))
}

pub async fn remove_system_account(
    State(s): State<SystemAccountsState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    finish("removeSystemAccount", remove_inner(&s, peer, &headers, &body).await)
}

async fn remove_inner(s: &SystemAccountsState, peer: SocketAddr, headers: &HeaderMap, body: &Bytes) -> Result<Value, Fail> {
    let g = gate(s, headers, peer, REMOVE_SYSTEM_ACCOUNT, true).await?;
    let input = object(body, false)?;
    let account = account(&input)?;
    let Some(bearer) = g.bearer.as_ref() else {
        return Err(internal("removeSystemAccount", "bearer"));
    };
    let rev = store::remove(s.app.hydrator.pool(), &g.caller.did, &g.keys.index(&account), bearer)
        .await
        .map_err(|e| from_store("removeSystemAccount", e))?;
    Ok(json!({"rev": rev}))
}

pub async fn get_system_account_status(
    State(s): State<SystemAccountsState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    finish("getSystemAccountStatus", status_inner(&s, peer, &headers).await)
}

async fn status_inner(s: &SystemAccountsState, peer: SocketAddr, headers: &HeaderMap) -> Result<Value, Fail> {
    let g = gate(s, headers, peer, GET_SYSTEM_ACCOUNT_STATUS, false).await?;
    let registered = store::listed(s.app.hydrator.pool(), &g.keys.index(&g.caller.did))
        .await
        .map_err(|e| from_store("getSystemAccountStatus", e))?;
    Ok(json!({"registered": registered}))
}

pub async fn leave_system_accounts(
    State(s): State<SystemAccountsState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    finish("leaveSystemAccounts", leave_inner(&s, peer, &headers, &body).await)
}

async fn leave_inner(s: &SystemAccountsState, peer: SocketAddr, headers: &HeaderMap, body: &Bytes) -> Result<Value, Fail> {
    let g = gate(s, headers, peer, LEAVE_SYSTEM_ACCOUNTS, true).await?;
    object(body, true)?;
    let Some(bearer) = g.bearer.as_ref() else {
        return Err(internal("leaveSystemAccounts", "bearer"));
    };
    store::leave(s.app.hydrator.pool(), &g.keys.index(&g.caller.did), bearer)
        .await
        .map_err(|e| from_store("leaveSystemAccounts", e))?;
    Ok(json!({}))
}
