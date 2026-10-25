//! The token rules of the sensitive tokens (design §3): the bearer of the
//! three writes and the `confirmSystemAccount` proof live at most
//! [`MAX_LIFETIME`] and are used once, counted at commit.
//!
//! A token is identified by its issuer and `jti`, or, without a `jti`, by
//! its signing input (`header.payload`): never by its whole text, since the
//! signature segment has more than one valid form (R1-1). The identity is
//! stored only as a keyed hash.

use std::time::SystemTime;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use kryphocron::verification::VerifiedJwt;

use super::{Keys, MAX_LIFETIME, TOKEN_MEMORY_SKEW};

/// A used token as stored: its keyed hash and when it may be forgotten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UsedToken {
    pub hash: [u8; 32],
    pub expires_at: DateTime<Utc>,
}

/// Whether a verified token's `exp − iat` is within [`MAX_LIFETIME`].
#[must_use]
pub(crate) fn lifetime_ok(jwt: &VerifiedJwt) -> bool {
    jwt.expires_at()
        .duration_since(jwt.issued_at())
        .is_ok_and(|lifetime| lifetime <= MAX_LIFETIME)
}

/// The stored identity of a verified token. `raw` is the token as sent (a
/// `Bearer ` prefix is ignored), `issuer` its verified `iss`.
#[must_use]
pub(crate) fn used_token(keys: &Keys, raw: &str, issuer: &str, expires_at: SystemTime) -> UsedToken {
    let raw = raw.trim();
    let token = raw.strip_prefix("Bearer ").unwrap_or(raw).trim();
    let mut parts = token.split('.');
    let header = parts.next().unwrap_or_default();
    let payload = parts.next().unwrap_or_default();
    let jti = URL_SAFE_NO_PAD
        .decode(payload)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|claims| claims.get("jti").and_then(serde_json::Value::as_str).map(str::to_string))
        .filter(|jti| !jti.is_empty());
    let material = match jti {
        Some(jti) => [b"jti\0".as_slice(), issuer.as_bytes(), b"\0", jti.as_bytes()].concat(),
        None => [b"sig\0".as_slice(), header.as_bytes(), b".", payload.as_bytes()].concat(),
    };
    UsedToken {
        hash: keys.token_hash(&material),
        expires_at: DateTime::<Utc>::from(expires_at.checked_add(TOKEN_MEMORY_SKEW).unwrap_or(expires_at)),
    }
}
