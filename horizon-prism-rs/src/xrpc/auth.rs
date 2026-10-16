//! Request authentication and the per-endpoint gate (design-v2 §6.8, §7.1,
//! §7.4, §9.2).
//!
//! Callers are authenticated by `kryphocron_glue::verify_request_jwt`; public
//! reads construct no kryphocron `AuthContext` (§13 row 182). Viewer column:
//! - `opt`: an absent `Authorization` is anonymous; a present one must verify;
//! - `member`: auth required and the caller must be a member (`NotEnrolled`),
//!   and ready unless the endpoint is mute management (`NotReady`).
//!   `getTimeline`, `listNotifications` and `getUnreadCount` trigger
//!   auto-enroll, as does `actor.getMembership` (`handlers::membership`),
//!   which has no member gate.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::SystemTime;

use async_trait::async_trait;
use axum::http::HeaderMap;

use super::AppState;
use super::backstop::{self, Claims};
use super::errors::XrpcError;
use super::member_keys::KeyAge;
use super::pools::{ChargeKey, Requester};
use super::rate_limit::{Family, client_ip};
use crate::aggregations::enroll::Enrollment;
use crate::hydration::Viewer;
use crate::kryphocron_glue::jwt::{IssuerRefresh, MemoryVerdict, fragment_allowed};
use crate::kryphocron_glue::{AuthError, JwtVerifier, VerifiedCaller};

/// Verifies `Authorization` headers for a method.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn verify(&self, authorization: Option<&str>, method: &str) -> Result<VerifiedCaller, AuthError>;

    /// [`Authenticator::verify`] for a proxied service's route: the `aud`
    /// fragment is absent or one of `fragments` (design-v3 §6.7.5.4).
    async fn verify_for(&self, authorization: Option<&str>, method: &str, _fragments: &[String]) -> Result<VerifiedCaller, AuthError> {
        self.verify(authorization, method).await
    }

    /// The backstop's claims pre-check on the token's unverified payload
    /// ([`backstop::precheck`]). `None` by default: nothing is pre-verified.
    fn precheck(&self, _authorization: &str, _method: &str) -> Option<Claims> {
        None
    }

    /// Verify against one in-memory DID document, with no network call.
    async fn verify_in_memory(&self, _authorization: &str, _method: &str, _document: &[u8]) -> MemoryVerdict {
        MemoryVerdict::NotVerified
    }
}

#[async_trait]
impl<F: IssuerRefresh> Authenticator for JwtVerifier<F> {
    async fn verify(&self, authorization: Option<&str>, method: &str) -> Result<VerifiedCaller, AuthError> {
        self.verify_request_jwt(authorization, method).await
    }

    async fn verify_for(&self, authorization: Option<&str>, method: &str, fragments: &[String]) -> Result<VerifiedCaller, AuthError> {
        self.verify_request_jwt_for(authorization, method, fragments).await
    }

    fn precheck(&self, authorization: &str, method: &str) -> Option<Claims> {
        backstop::precheck(authorization, self.service_did(), method, self.config(), SystemTime::now())
    }

    async fn verify_in_memory(&self, authorization: &str, method: &str, document: &[u8]) -> MemoryVerdict {
        JwtVerifier::verify_in_memory(self, authorization, method, document).await
    }
}

/// Authenticate a request's token for `method` (§6.8).
///
/// - A caller the backstop already verified with a fresh in-memory key is
///   returned as is: the token is not verified twice.
/// - Otherwise the verifier runs, under §II-6.7's freshness rule. If the
///   issuer lookup found no budget, the answer is `429`, never `401`
///   (design-v3 §II-8.3, R3-212).
///
/// # Errors
///
/// `AuthRequired`, `InvalidToken` or `RateLimitExceeded`.
pub async fn authenticate(state: &AppState, authorization: Option<&str>, method: &str) -> Result<VerifiedCaller, XrpcError> {
    authenticate_for(state, authorization, method, None).await
}

/// [`authenticate`], with a proxied service's `aud` fragment allowlist in
/// place of the core one when `fragments` is given (design-v3 §6.7.5.4). The
/// allowlist applies to a caller the backstop already verified too; its
/// signature is still not verified twice.
///
/// # Errors
///
/// As [`authenticate`].
pub async fn authenticate_for(
    state: &AppState,
    authorization: Option<&str>,
    method: &str,
    fragments: Option<&[String]>,
) -> Result<VerifiedCaller, XrpcError> {
    let selected = backstop::selected();
    if let (Some(s), Some(a)) = (&selected, authorization)
        && let Some(caller) = s.verified(a)
    {
        return match fragments {
            Some(allowed) if !fragment_allowed(&caller.jwt, allowed) => {
                Err(AuthError::Invalid("aud fragment is not served on this route".to_string()).into())
            }
            _ => Ok(caller),
        };
    }
    let verified = match fragments {
        Some(allowed) => state.auth.verify_for(authorization, method, allowed).await,
        None => state.auth.verify(authorization, method).await,
    };
    match verified {
        Ok(caller) => {
            // A verification here with a fresh key is a recent success for
            // the caller at this IP key (R3-233).
            if let (Some(b), Some(ChargeKey::Ip(ip))) = (state.limits.backstop(), selected.as_ref().map(|s| s.req.key()))
                && let Some(key) = b.keys.get(&caller.did)
                && key.age(chrono::Utc::now()) == Some(KeyAge::Fresh)
            {
                b.limits.record_prepare_success(*ip, &caller.did, key.print);
            }
            Ok(caller)
        }
        Err(e) => Err(selected.and_then(|s| s.req.exhausted()).map_or_else(|| e.into(), Into::into)),
    }
}

/// The handle a request's upstream calls are charged to: the one the
/// backstop selected, or, where no backstop ran (tests), a ready member's
/// DID or the IP key.
#[must_use]
pub fn request_handle(ready_member: Option<&str>, ip: IpAddr) -> Requester {
    backstop::selected().map_or_else(|| Requester::of(ready_member, ip), |s| s.req)
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
    /// Who the request's upstream calls are charged to: a ready member by
    /// DID, everyone else by IP key.
    pub req: Requester,
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
        _ => Some(authenticate(state, authorization, method).await?),
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
    let req = request_handle(viewer.state_did(), ip);
    // A ready member's public request gets its Farsight handle here
    // (design-v3 §5.14.5). The private-tier gate below attaches none, so
    // private reads never reach Farsight.
    if viewer.state_did().is_some()
        && let Some(farsight) = state.hydrator.farsight()
        && let Some(selected) = backstop::selected()
    {
        selected.attach_farsight(farsight.request(&req));
    }
    // Mute management works before readiness: act as the caller.
    let viewer = if matches!(access, Access::Member { ready: false, .. }) && !viewer.hydrate {
        Viewer {
            did: caller_did,
            hydrate: false,
        }
    } else {
        viewer
    };
    Ok(Prepared { viewer, ip, req })
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
    let caller = authenticate(state, authorization, method).await?;
    let ip = request_ip(state, headers, peer);
    state.limits.charge(method, Family::Private, Some(&caller.did), ip)?;
    match member_state(state, &caller.did).await? {
        Enrollment::Member { ready: true } => Ok(caller),
        Enrollment::Member { ready: false } | Enrollment::Enrolled | Enrollment::Purging => Err(XrpcError::not_ready()),
        Enrollment::Refused(_) => Err(XrpcError::not_enrolled()),
    }
}

/// The caller's membership without enrolling it.
pub(crate) async fn member_state(state: &AppState, did: &str) -> Result<Enrollment, sqlx::Error> {
    let ready: Option<bool> = sqlx::query_scalar("SELECT ready_at IS NOT NULL FROM members WHERE did = $1")
        .bind(did)
        .fetch_optional(state.hydrator.pool())
        .await?;
    Ok(match ready {
        Some(ready) => Enrollment::Member { ready },
        None => Enrollment::Refused("not_member"),
    })
}
