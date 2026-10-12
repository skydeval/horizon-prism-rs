//! Request authentication and the per-endpoint gate (design-v2 §6.8, §7.1,
//! §7.4, §9.2).
//!
//! Callers are authenticated by `kryphocron_glue::verify_request_jwt`; public
//! reads construct no kryphocron `AuthContext` (§13 row 182). Viewer column:
//! - `opt`: an absent `Authorization` is anonymous; a present one must verify;
//! - `member`: auth required and the caller must be a member (`NotEnrolled`),
//!   and ready unless the endpoint is mute management (`NotReady`).
//!   `getTimeline`, `listNotifications` and `getUnreadCount` trigger
//!   auto-enroll.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use async_trait::async_trait;
use axum::http::HeaderMap;

use super::AppState;
use super::errors::XrpcError;
use super::rate_limit::{Family, client_ip};
use crate::aggregations::enroll::Enrollment;
use crate::hydration::Viewer;
use crate::kryphocron_glue::jwt::IssuerRefresh;
use crate::kryphocron_glue::{AuthError, JwtVerifier, VerifiedCaller};

/// Verifies `Authorization` headers for a method.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn verify(&self, authorization: Option<&str>, method: &str) -> Result<VerifiedCaller, AuthError>;
}

#[async_trait]
impl<F: IssuerRefresh> Authenticator for JwtVerifier<F> {
    async fn verify(&self, authorization: Option<&str>, method: &str) -> Result<VerifiedCaller, AuthError> {
        self.verify_request_jwt(authorization, method).await
    }
}

/// The viewer column of §7.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Opt,
    Member { ready: bool, enroll: bool },
}

/// Query parameters, including repeated keys (`actors=a&actors=b`).
#[derive(Debug, Clone, Default)]
pub struct Params(HashMap<String, Vec<String>>);

impl Params {
    #[must_use]
    pub fn parse(raw: Option<&str>) -> Self {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (k, v) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
            map.entry(k.into_owned()).or_default().push(v.into_owned());
        }
        Self(map)
    }

    #[must_use]
    pub fn one(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.first()).map(String::as_str)
    }

    #[must_use]
    pub fn all(&self, key: &str) -> Vec<String> {
        self.0.get(key).cloned().unwrap_or_default()
    }

    /// A required parameter.
    ///
    /// # Errors
    ///
    /// `InvalidRequest` when missing.
    pub fn required(&self, key: &str) -> Result<&str, XrpcError> {
        self.one(key)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| XrpcError::invalid(format!("Error: Params must have the property \"{key}\"")))
    }

    /// An optional unsigned integer.
    ///
    /// # Errors
    ///
    /// `InvalidRequest` when present but not a number.
    pub fn u32(&self, key: &str) -> Result<Option<u32>, XrpcError> {
        self.one(key)
            .map(|v| v.parse().map_err(|_| XrpcError::invalid(format!("Error: {key} must be an integer"))))
            .transpose()
    }

    #[must_use]
    pub fn bool(&self, key: &str) -> bool {
        self.one(key).is_some_and(|v| v == "true")
    }
}

/// A prepared request.
#[derive(Debug, Clone)]
pub struct Prepared {
    pub viewer: Viewer,
    pub ip: IpAddr,
}

/// The client IP for a request.
#[must_use]
pub fn request_ip(state: &AppState, headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    let xff = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
    client_ip(peer.ip(), xff, state.trusted_proxy)
}

/// Authenticate, rate-limit and gate one request.
///
/// # Errors
///
/// The request's XRPC error.
pub async fn prepare(
    state: &AppState,
    headers: &HeaderMap,
    peer: SocketAddr,
    method: &str,
    access: Access,
    write: bool,
) -> Result<Prepared, XrpcError> {
    let ip = request_ip(state, headers, peer);
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = match (access, authorization) {
        (Access::Opt, None) => None,
        _ => Some(state.auth.verify(authorization, method).await?),
    };
    let family = if write { Family::Write } else { Family::Read };
    state.limits.charge(method, family, caller.as_ref().map(|c| c.did.as_str()), ip)?;
    let caller_did = caller.map(|c| c.did);
    if let Access::Member { ready, enroll } = access {
        let did = caller_did.as_deref().ok_or_else(|| XrpcError::from(AuthError::Required))?;
        let state_now = if enroll {
            match &state.enroller {
                Some(e) => e.check(did).await?,
                None => member_state(state, did).await?,
            }
        } else {
            member_state(state, did).await?
        };
        match state_now {
            Enrollment::Member { ready: true } => {}
            Enrollment::Member { ready: false } if !ready => {}
            Enrollment::Member { ready: false } | Enrollment::Enrolled | Enrollment::Purging => {
                return Err(XrpcError::not_ready());
            }
            Enrollment::Refused(_) => return Err(XrpcError::not_enrolled()),
        }
    }
    let viewer = state.hydrator.viewer(caller_did.as_deref()).await?;
    // Mute management works before readiness: act as the caller.
    let viewer = if matches!(access, Access::Member { ready: false, .. }) && !viewer.hydrate {
        Viewer {
            did: caller_did,
            hydrate: false,
        }
    } else {
        viewer
    };
    Ok(Prepared { viewer, ip })
}

/// The private-endpoint gate (§6.3 steps 1–2, §7.4): authentication
/// required, the `private` per-DID bucket (or the endpoint's configured
/// family), and a ready member. Returns the
/// verified caller, whose JWT feeds the kryphocron `AuthContext`. Failures
/// here are ordinary errors, outside the timing floor.
///
/// # Errors
///
/// `AuthRequired`/`InvalidToken`, `RateLimitExceeded`, `NotEnrolled`,
/// `NotReady`.
pub async fn prepare_private(
    state: &AppState,
    headers: &HeaderMap,
    peer: SocketAddr,
    method: &str,
) -> Result<VerifiedCaller, XrpcError> {
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = state.auth.verify(authorization, method).await?;
    let ip = request_ip(state, headers, peer);
    state.limits.charge(method, Family::Private, Some(&caller.did), ip)?;
    match member_state(state, &caller.did).await? {
        Enrollment::Member { ready: true } => Ok(caller),
        Enrollment::Member { ready: false } | Enrollment::Enrolled | Enrollment::Purging => Err(XrpcError::not_ready()),
        Enrollment::Refused(_) => Err(XrpcError::not_enrolled()),
    }
}

async fn member_state(state: &AppState, did: &str) -> Result<Enrollment, sqlx::Error> {
    let ready: Option<bool> = sqlx::query_scalar("SELECT ready_at IS NOT NULL FROM members WHERE did = $1")
        .bind(did)
        .fetch_optional(state.hydrator.pool())
        .await?;
    Ok(match ready {
        Some(ready) => Enrollment::Member { ready },
        None => Enrollment::Refused("not_member"),
    })
}
