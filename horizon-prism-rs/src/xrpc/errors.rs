//! XRPC error responses (design-v2 §7.3, §7.1, §6.8): `{"error", "message"}`
//! with the lexicon error names. `app.bsky.*` endpoints follow their
//! lexicons' public semantics; none reveals private-tier facts.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::http::header::RETRY_AFTER;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use super::pools::Exhausted;
use super::rate_limit::Limited;
use crate::hydration::{HydrationError, ViewError};
use crate::kryphocron_glue::AuthError;

/// The lexicon error name of the readiness gate.
pub const NOT_READY: &str = "NotReady";

/// Response extension set on every `400 NotReady`. The final router's layer
/// turns it into `Retry-After` (design-v3 §II-7.1), so no handler needs the
/// configured value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotReadyHint;

/// An XRPC error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XrpcError {
    pub status: StatusCode,
    pub error: &'static str,
    pub message: String,
    pub reset: Option<u64>,
    /// `Retry-After` in seconds, when the error names one itself.
    pub retry_after: Option<u64>,
    /// Whether this is the `NotReady` gate error (see [`NotReadyHint`]).
    pub not_ready: bool,
    /// `retryAfter` in the body as well as the header: for clients behind a
    /// proxy that drops `Retry-After` (degraded-mode design §3.3, §3.5).
    pub retry_after_in_body: bool,
}

impl XrpcError {
    pub fn new(status: StatusCode, error: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            error,
            message: message.into(),
            reset: None,
            retry_after: None,
            not_ready: false,
            retry_after_in_body: false,
        }
    }

    /// The same error with `Retry-After: secs`.
    #[must_use]
    pub fn with_retry_after(self, secs: u64) -> Self {
        Self {
            retry_after: Some(secs),
            ..self
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    #[must_use]
    pub fn not_enrolled() -> Self {
        Self::new(StatusCode::BAD_REQUEST, "NotEnrolled", "Not enrolled in this AppView")
    }

    #[must_use]
    pub fn not_ready() -> Self {
        Self {
            not_ready: true,
            ..Self::new(StatusCode::BAD_REQUEST, NOT_READY, "Account indexing in progress")
        }
    }

    /// `503 Unavailable` (degraded-mode design §3.3): a private anchor that
    /// cannot be served yet; `retryAfter` in the body and the header.
    #[must_use]
    pub fn unavailable(secs: u64) -> Self {
        Self {
            retry_after_in_body: true,
            ..Self::new(StatusCode::SERVICE_UNAVAILABLE, "Unavailable", "Temporarily unavailable; try again").with_retry_after(secs)
        }
    }

    #[must_use]
    pub fn internal() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError", "Internal Server Error")
    }
}

impl From<AuthError> for XrpcError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::Required => Self::new(StatusCode::UNAUTHORIZED, "AuthRequired", "Authentication Required"),
            AuthError::Invalid(_) => Self::new(StatusCode::UNAUTHORIZED, "InvalidToken", "Invalid token"),
        }
    }
}

/// Every limiter's refusal: `ratelimit-reset` (epoch seconds) and
/// `Retry-After` (the seconds left to it, at least 1).
impl From<Limited> for XrpcError {
    fn from(l: Limited) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            error: "RateLimitExceeded",
            message: "Rate Limit Exceeded".into(),
            reset: Some(l.reset_epoch_secs),
            retry_after: Some(l.retry_after_secs(now)),
            not_ready: false,
            retry_after_in_body: false,
        }
    }
}

impl From<ViewError> for XrpcError {
    fn from(e: ViewError) -> Self {
        // The timeline's own readiness check raises `NotReady` as a view
        // error; it carries the retry hint like the gate's.
        Self {
            not_ready: e.error == NOT_READY,
            ..Self::new(StatusCode::BAD_REQUEST, e.error, e.message)
        }
    }
}

impl From<Exhausted> for XrpcError {
    fn from(e: Exhausted) -> Self {
        Limited {
            reset_epoch_secs: e.reset_epoch_secs(),
        }
        .into()
    }
}

impl From<HydrationError> for XrpcError {
    fn from(e: HydrationError) -> Self {
        if let HydrationError::NoBudget(exhausted) = e {
            return exhausted.into();
        }
        if matches!(e, HydrationError::Upstream) {
            return Self::new(StatusCode::BAD_GATEWAY, "UpstreamFailure", "An upstream lookup could not be completed");
        }
        tracing::error!(event = "xrpc_internal_error", error = %e, "request failed");
        Self::internal()
    }
}

impl From<sqlx::Error> for XrpcError {
    fn from(e: sqlx::Error) -> Self {
        HydrationError::from(e).into()
    }
}

/// Test check for a limiter's `429`: `Retry-After` is at least 1 and agrees
/// with `ratelimit-reset` (a retry after it is no earlier than the reset,
/// and it is no longer than the reset plus the test's own elapsed time).
#[cfg(test)]
pub(crate) fn assert_retry_after(retry_after: Option<&str>, reset: Option<&str>) {
    let retry: u64 = retry_after.expect("Retry-After").parse().expect("delta-seconds");
    let reset: u64 = reset.expect("ratelimit-reset").parse().expect("epoch seconds");
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("time").as_secs();
    let left = reset.saturating_sub(now);
    assert!(retry >= 1, "Retry-After {retry}");
    assert!(retry >= left, "Retry-After {retry} ends before ratelimit-reset {reset} (now {now})");
    assert!(retry <= left.max(1) + 2, "Retry-After {retry} is past ratelimit-reset {reset} (now {now})");
}

impl IntoResponse for XrpcError {
    fn into_response(self) -> Response {
        let body = match self.retry_after.filter(|_| self.retry_after_in_body) {
            Some(secs) => json!({ "error": self.error, "message": self.message, "retryAfter": secs }),
            None => json!({ "error": self.error, "message": self.message }),
        };
        let mut response = (self.status, Json(body)).into_response();
        if let Some(reset) = self.reset
            && let Ok(v) = HeaderValue::from_str(&reset.to_string())
        {
            response.headers_mut().insert("ratelimit-reset", v);
        }
        if let Some(secs) = self.retry_after {
            response.headers_mut().insert(RETRY_AFTER, HeaderValue::from(secs));
        }
        if self.not_ready {
            response.extensions_mut().insert(NotReadyHint);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::xrpc::pools::Pool;

    fn headers(e: XrpcError) -> (Option<String>, Option<String>) {
        let r = e.into_response();
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        let get = |name: &str| r.headers().get(name).map(|v| v.to_str().expect("ascii").to_string());
        (get("retry-after"), get("ratelimit-reset"))
    }

    /// Every limiter's 429 names `Retry-After`, the seconds left to its
    /// `ratelimit-reset`, at least 1.
    #[test]
    fn a_limiters_429_carries_retry_after_from_its_reset() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("time").as_secs();
        let limited = Limited { reset_epoch_secs: now + 7 };
        assert_eq!(limited.retry_after_secs(now), 7);
        let (retry, reset) = headers(limited.into());
        assert_retry_after(retry.as_deref(), reset.as_deref());
        // A reset already reached, or this very second: still at least 1.
        assert_eq!(Limited { reset_epoch_secs: now - 3 }.retry_after_secs(now), 1);
        assert_eq!(Limited { reset_epoch_secs: now }.retry_after_secs(now), 1);
        let (retry, _) = headers(Limited { reset_epoch_secs: now.saturating_sub(3) }.into());
        assert_eq!(retry.as_deref(), Some("1"));
        // A spent share or pool (the auth path, an upstream budget, the
        // backstop's per-member ceiling) goes through the same conversion.
        let exhausted = Exhausted { pool: Pool::Member, retry_after: Duration::from_millis(2500) };
        let (retry, reset) = headers(exhausted.into());
        assert_retry_after(retry.as_deref(), reset.as_deref());
        assert!(matches!(retry.as_deref(), Some("2" | "3" | "4")), "{retry:?}");
    }
}
