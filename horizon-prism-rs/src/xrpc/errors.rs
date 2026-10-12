//! XRPC error responses (design-v2 §7.3, §7.1, §6.8): `{"error", "message"}`
//! with the lexicon error names. `app.bsky.*` endpoints follow their
//! lexicons' public semantics; none reveals private-tier facts.

use axum::Json;
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
    /// Whether this is the `NotReady` gate error (see [`NotReadyHint`]).
    pub not_ready: bool,
}

impl XrpcError {
    pub fn new(status: StatusCode, error: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            error,
            message: message.into(),
            reset: None,
            not_ready: false,
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

impl From<Limited> for XrpcError {
    fn from(l: Limited) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            error: "RateLimitExceeded",
            message: "Rate Limit Exceeded".into(),
            reset: Some(l.reset_epoch_secs),
            not_ready: false,
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
        tracing::error!(event = "xrpc_internal_error", error = %e, "request failed");
        Self::internal()
    }
}

impl From<sqlx::Error> for XrpcError {
    fn from(e: sqlx::Error) -> Self {
        HydrationError::from(e).into()
    }
}

impl IntoResponse for XrpcError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(json!({ "error": self.error, "message": self.message }))).into_response();
        if let Some(reset) = self.reset
            && let Ok(v) = HeaderValue::from_str(&reset.to_string())
        {
            response.headers_mut().insert("ratelimit-reset", v);
        }
        if self.not_ready {
            response.extensions_mut().insert(NotReadyHint);
        }
        response
    }
}
