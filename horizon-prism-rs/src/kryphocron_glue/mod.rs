//! Process-wide kryphocron state (design-v2 §3.2), `GlueContext`:
//! - audit sinks (§6.5);
//! - at-rest hooks (§6.6);
//! - `ServiceIdentity` and `JwtVerificationConfig`, and client JWT
//!   verification (§6.7, §6.8), whose `verify_request_jwt` is the only caller
//!   of `kryphocron::verification::verify_jwt`.
//!
//! This module holds no private-tier row access. Startup builds one
//! `GlueContext`, shared by every handler.

pub mod audit;
pub mod jwt;
pub mod service_key;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use kryphocron::{AtRestHooks, AuditSinks, CorrelationKey, DefaultAtRestHooks, SubstrateAuditSink, UserAuditSink};
use tokio::task::JoinHandle;

pub use audit::GlueSinks;
pub use jwt::{AuthError, JwtVerifier, VerifiedCaller};
pub use service_key::{ServiceKey, ServiceKeyError, keygen};

use crate::config::AppConfig;
use crate::identity::{GuardedDidFetcher, IdentityResolver, JwtFetchCell};

/// A startup failure building the glue.
#[derive(Debug, thiserror::Error)]
pub enum GlueError {
    #[error(transparent)]
    ServiceKey(#[from] ServiceKeyError),
    /// `DefaultAtRestHooks::for_data_dir` failed (for example an unwritable
    /// data directory).
    #[error("at-rest hooks: {0}")]
    AtRestHooks(String),
    /// `validate_at_rest_install` rejected the hook set.
    #[error("at-rest install: {0}")]
    AtRestInstall(String),
}

/// Build and validate the at-rest hooks for `data_dir` (§6.6). Laquna is
/// installed inside kryphocron; the host never names the codec.
///
/// # Errors
///
/// Either fallible step.
pub fn at_rest_hooks(data_dir: PathBuf) -> Result<Arc<dyn AtRestHooks>, GlueError> {
    let hooks = DefaultAtRestHooks::for_data_dir(data_dir).map_err(|e| {
        tracing::error!(event = "at_rest_hooks_failed", error = ?e, "at-rest hooks could not be built");
        GlueError::AtRestHooks(format!("{e:?}"))
    })?;
    kryphocron::validate_at_rest_install(&hooks).map_err(|e| {
        tracing::error!(event = "at_rest_install_invalid", error = ?e, "at-rest install validation failed");
        GlueError::AtRestInstall(format!("{e:?}"))
    })?;
    Ok(Arc::new(hooks))
}

/// What the private read pipeline needs from the glue (§6.3 steps 6, 10, 11):
/// the audit sinks and the at-rest hooks. Cheap to clone.
#[derive(Clone)]
pub struct PipelineGlue {
    sinks: Arc<GlueSinks>,
    hooks: Arc<dyn AtRestHooks>,
}

impl PipelineGlue {
    #[must_use]
    pub fn new(sinks: GlueSinks, hooks: Arc<dyn AtRestHooks>) -> Self {
        Self {
            sinks: Arc::new(sinks),
            hooks,
        }
    }

    /// A fresh `AuditSinks<'_>` per record (§6.3 step 6).
    #[must_use]
    pub fn audit_sinks(&self) -> AuditSinks<'_> {
        self.sinks.audit_sinks()
    }

    #[must_use]
    pub fn user_sink(&self) -> &dyn UserAuditSink {
        self.sinks.user_sink()
    }

    #[must_use]
    pub fn substrate_sink(&self) -> &dyn SubstrateAuditSink {
        self.sinks.substrate_sink()
    }

    #[must_use]
    pub fn at_rest_hooks(&self) -> &dyn AtRestHooks {
        self.hooks.as_ref()
    }

    /// `GlueContext::audit_latched` (§6.5): set when the user sink panicked.
    #[must_use]
    pub fn audit_latched(&self) -> Arc<AtomicBool> {
        self.sinks.latch()
    }
}

/// The process-wide glue.
#[derive(Clone)]
pub struct GlueContext {
    pub service_key: ServiceKey,
    pub verifier: JwtVerifier<GuardedDidFetcher>,
    pub pipeline: PipelineGlue,
}

impl GlueContext {
    /// Build the context and its issuer-eviction drainer (spawn it).
    ///
    /// # Errors
    ///
    /// An invalid service key or hostname, or at-rest hooks that cannot be
    /// built or validated. `serve` exits before any listener binds.
    pub fn new(cfg: &AppConfig, resolver: IdentityResolver) -> Result<(Self, JoinHandle<()>), GlueError> {
        let service_key = ServiceKey::new(&cfg.env.public_hostname, cfg.env.service_signing_key.expose())?;
        let identity = service_key.service_identity()?;
        let fetcher = GuardedDidFetcher::new(resolver, JwtFetchCell::new());
        let (verifier, drainer) = JwtVerifier::new(
            identity,
            fetcher,
            cfg.file.identity.jwt_issuer_tracker_max,
            jwt::EVICTION_QUEUE,
        );
        let sinks = GlueSinks::log(CorrelationKey::from_bytes(*cfg.env.correlation_key.expose()));
        let hooks = at_rest_hooks(cfg.env.data_dir.clone())?;
        Ok((
            Self {
                service_key,
                verifier,
                pipeline: PipelineGlue::new(sinks, hooks),
            },
            drainer.spawn(),
        ))
    }

    /// `GlueContext::audit_latched` (§6.5): set when the user sink panicked.
    #[must_use]
    pub fn audit_latched(&self) -> Arc<AtomicBool> {
        self.pipeline.audit_latched()
    }
}
