//! The XRPC surface (design-v2 §7): `app.bsky.*` handlers,
//! `index.requestRecord`, the auth extractor, rate limiting and error
//! mapping. The private-tier, Thread Lens and `resolveDid` routers live with
//! their modules.

pub mod auth;
pub mod errors;
pub mod handlers;
pub mod pools;
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
