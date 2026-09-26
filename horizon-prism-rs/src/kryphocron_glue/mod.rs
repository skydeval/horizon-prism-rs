//! Process-wide kryphocron state (design-v2 §3.2). Item 5 builds the client
//! authentication path (§6.7, §6.8; §13 row 182): the AppView's service
//! identity and `verify_request_jwt`. Item 6 adds the audit sinks, at-rest
//! hooks and the private pipeline's use of them.
//!
//! This module holds no private-tier row access.

pub mod jwt;
pub mod service_key;

use tokio::task::JoinHandle;

pub use jwt::{AuthError, JwtVerifier, VerifiedCaller};
pub use service_key::{ServiceKey, ServiceKeyError, keygen};

use crate::config::AppConfig;
use crate::identity::{GuardedDidFetcher, IdentityResolver, JwtFetchCell};

/// The item-5 glue context.
#[derive(Clone)]
pub struct GlueContext {
    pub service_key: ServiceKey,
    pub verifier: JwtVerifier<GuardedDidFetcher>,
}

impl GlueContext {
    /// Build the context and its issuer-eviction drainer (spawn it).
    ///
    /// # Errors
    ///
    /// An invalid service key or hostname.
    pub fn new(cfg: &AppConfig, resolver: IdentityResolver) -> Result<(Self, JoinHandle<()>), ServiceKeyError> {
        let service_key = ServiceKey::new(&cfg.env.public_hostname, cfg.env.service_signing_key.expose())?;
        let identity = service_key.service_identity()?;
        let fetcher = GuardedDidFetcher::new(resolver, JwtFetchCell::new());
        let (verifier, drainer) = JwtVerifier::new(
            identity,
            fetcher,
            cfg.file.identity.jwt_issuer_tracker_max,
            jwt::EVICTION_QUEUE,
        );
        Ok((Self { service_key, verifier }, drainer.spawn()))
    }
}
