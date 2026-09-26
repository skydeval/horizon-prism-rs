//! The XRPC surface (design-v2 §7): `app.bsky.*` handlers, the auth
//! extractor, rate limiting and error mapping. Private-tier, Thread Lens,
//! `resolveDid` and `requestRecord` endpoints arrive with items 6 and 7.

pub mod auth;
pub mod errors;
pub mod handlers;
pub mod rate_limit;
pub mod router;

use std::net::IpAddr;
use std::sync::Arc;

use serde_json::Value;

pub use auth::Authenticator;
pub use rate_limit::RateLimits;
pub use router::router;

use crate::aggregations::enroll::AutoEnroller;
use crate::hydration::Hydrator;

/// Shared handler state. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    pub hydrator: Hydrator,
    pub auth: Arc<dyn Authenticator>,
    pub limits: RateLimits,
    /// `None` disables auto-enroll entirely (tests).
    pub enroller: Option<AutoEnroller>,
    /// `http.trusted_proxy` (Caddy's fixed address, §7.4).
    pub trusted_proxy: IpAddr,
    /// The `/.well-known/did.json` document (§6.8).
    pub did_document: Arc<Value>,
}
