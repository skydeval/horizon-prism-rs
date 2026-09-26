//! Upstream response classification (design-v2 §8.3).
//!
//! | Upstream result | Class |
//! |---|---|
//! | 2xx with a parseable body | hit |
//! | 400 `NotFound`/`RecordNotFound`/`ProfileNotFound`, or `InvalidRequest` whose message contains "not found"; 404 | miss |
//! | 400 `AccountTakedown`/`AccountDeactivated`; message "Account has been suspended"; 403 | withheld |
//! | 401 | auth-refused |
//! | 429, 5xx, connect or read timeout | transient |
//!
//! Two cases the table leaves open are decided here: a 2xx body that is not
//! JSON, and a transport failure that is not a timeout or connection error
//! (for example an oversized body), are `transient` (the upstream did not
//! answer usefully, and it counts against the breaker); any other 4xx is a
//! `miss` (the upstream answered, and it is not unhealthy).

use serde::Deserialize;
use serde_json::Value;

use crate::net::{NetError, NetResponse};

/// The class of one upstream call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Hit,
    Miss,
    Withheld,
    AuthRefused,
    Transient,
}

impl Class {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Withheld => "withheld",
            Self::AuthRefused => "auth_refused",
            Self::Transient => "transient",
        }
    }

    /// Counts against the network's breaker.
    #[must_use]
    pub fn is_failure(self) -> bool {
        matches!(self, Self::Transient | Self::AuthRefused)
    }
}

#[derive(Deserialize, Default)]
struct ErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Classify a call, returning the parsed body for a hit.
#[must_use]
pub fn classify(result: &Result<NetResponse, NetError>) -> (Class, Option<Value>) {
    let response = match result {
        Ok(r) => r,
        Err(_) => return (Class::Transient, None),
    };
    let status = response.status.as_u16();
    if response.is_success() {
        return match serde_json::from_slice::<Value>(&response.body) {
            Ok(v) => (Class::Hit, Some(v)),
            Err(_) => (Class::Transient, None),
        };
    }
    let body: ErrorBody = serde_json::from_slice(&response.body).unwrap_or_default();
    let message = body.message.unwrap_or_default().to_ascii_lowercase();
    let class = match (status, body.error.as_deref()) {
        (401, _) => Class::AuthRefused,
        (403, _) | (400, Some("AccountTakedown" | "AccountDeactivated")) => Class::Withheld,
        (400, _) if message.contains("account has been suspended") => Class::Withheld,
        (404, _) | (400, Some("NotFound" | "RecordNotFound" | "ProfileNotFound")) => Class::Miss,
        (400, Some("InvalidRequest")) if message.contains("not found") => Class::Miss,
        (429 | 500..=599, _) => Class::Transient,
        _ => Class::Miss,
    };
    (class, None)
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::*;

    fn response(status: u16, body: &Value) -> Result<NetResponse, NetError> {
        Ok(NetResponse {
            status: StatusCode::from_u16(status).expect("status"),
            content_type: Some("application/json".into()),
            final_url: "https://x".into(),
            body: serde_json::to_vec(body).expect("json"),
        })
    }

    #[test]
    fn table() {
        let e = |error: &str, message: &str| json!({"error": error, "message": message});
        let cases = [
            (response(200, &json!({"did": "x"})), Class::Hit),
            (response(404, &json!({})), Class::Miss),
            (response(400, &e("NotFound", "x")), Class::Miss),
            (response(400, &e("RecordNotFound", "x")), Class::Miss),
            (response(400, &e("ProfileNotFound", "x")), Class::Miss),
            (response(400, &e("InvalidRequest", "Profile not found")), Class::Miss),
            (response(400, &e("AccountTakedown", "x")), Class::Withheld),
            (response(400, &e("AccountDeactivated", "x")), Class::Withheld),
            (response(400, &e("InvalidRequest", "Account has been suspended")), Class::Withheld),
            (response(403, &json!({})), Class::Withheld),
            (response(401, &e("AuthMissing", "x")), Class::AuthRefused),
            (response(429, &json!({})), Class::Transient),
            (response(502, &json!({})), Class::Transient),
            (Err(NetError::Timeout), Class::Transient),
            (Err(NetError::BodyTooLarge { cap: 1 }), Class::Transient),
            (response(400, &e("InvalidRequest", "bad params")), Class::Miss),
        ];
        for (result, expected) in cases {
            assert_eq!(classify(&result).0, expected, "{result:?}");
        }
        let (class, body) = classify(&response(200, &json!({"did": "x"})));
        assert_eq!((class, body), (Class::Hit, Some(json!({"did": "x"}))));
        let not_json = Ok(NetResponse {
            status: StatusCode::OK,
            content_type: None,
            final_url: "https://x".into(),
            body: b"<html>".to_vec(),
        });
        assert_eq!(classify(&not_json).0, Class::Transient);
        assert!(Class::AuthRefused.is_failure() && Class::Transient.is_failure());
        assert!(!Class::Miss.is_failure() && !Class::Withheld.is_failure());
    }
}
