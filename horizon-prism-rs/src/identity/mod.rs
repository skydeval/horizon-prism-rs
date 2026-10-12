//! DID and handle resolution (design-v2 §5.6, §6.7).
//!
//! - [`resolver`]: DID parsing, document fetch and extraction, the cached
//!   resolver, and bidirectional handle verification.
//! - [`cache`]: `did_cache` (fresh 1 h, stale-usable 24 h) plus the bounded
//!   in-process cache for DIDs outside the persistence set.
//! - [`fetch_cell`]: the JWT-path single-flight fetch cell and the
//!   `GuardedDidFetcher` that kryphocron's `DefaultDidResolver` calls.
//! - [`refresher`]: the `identity_refresh` worker.

pub mod cache;
pub mod eligibility;
pub mod fetch_cell;
pub mod handle_lookup;
pub mod refresher;
pub mod resolver;

use std::time::Duration;

use crate::config::AppConfig;
use crate::net::NetError;

pub use cache::{CachedDoc, DidCache, Freshness, NON_PERSISTED_CAPACITY, NON_PERSISTED_TTL};
pub use fetch_cell::{FetchOutcome, GuardedDidFetcher, JwtFetchCell};
pub use refresher::{AccountLifecycle, IdentityRefresher, RefreshReason};
pub use resolver::{
    DidDocInfo, DidTarget, FetchedDoc, IdentityResolver, parse_did, verify_bidirectional,
};

/// Identity failures.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("invalid DID {did:?}: {reason}")]
    InvalidDid { did: String, reason: String },
    #[error("unsupported DID method in {0:?}")]
    UnsupportedMethod(String),
    /// The DID's registry does not know it (PLC 404, did:web 404).
    #[error("DID not found: {0}")]
    NotFound(String),
    /// PLC answered 410: the DID is tombstoned. did:web 410 is `Gone`.
    #[error("DID tombstoned: {0}")]
    Tombstoned(String),
    #[error("DID document gone: {0}")]
    Gone(String),
    #[error("DID resolution upstream returned HTTP {status} for {did}")]
    Upstream { did: String, status: u16 },
    #[error("malformed DID document for {did}: {reason}")]
    MalformedDocument { did: String, reason: String },
    #[error(transparent)]
    Net(#[from] NetError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// No fetch was made: the request had no budget for it (design-v3
    /// §II-8.3). No answer, and nothing to cache.
    #[error("no request budget to resolve {0}")]
    NoBudget(String),
}

impl IdentityError {
    /// True for failures worth retrying with backoff (§5.6): network and
    /// database trouble, and upstream 5xx/429.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Net(e) => e.is_transient(),
            Self::Database(_) => true,
            Self::Upstream { status, .. } => *status == 429 || *status >= 500,
            _ => false,
        }
    }
}

/// Settings from `[identity]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentitySettings {
    /// PLC directory base URL, without a trailing slash.
    pub plc_url: String,
    /// `did_cache` rows are fresh for this long (1 h).
    pub fresh: Duration,
    /// Stale rows are usable off the JWT path until this age (24 h).
    pub stale_max: Duration,
}

impl IdentitySettings {
    #[must_use]
    pub fn from_app(cfg: &AppConfig) -> Self {
        Self {
            plc_url: cfg.file.identity.plc_url.trim_end_matches('/').to_string(),
            fresh: Duration::from_secs(cfg.file.identity.did_fresh_secs),
            stale_max: Duration::from_secs(cfg.file.identity.did_stale_max_secs),
        }
    }
}

/// Why a DID is being resolved; decides whether the result may be persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveIntent {
    /// Any lookup: request paths, cascade routing, JWT issuers. Persisted only
    /// when the DID is in the persistence set.
    Lookup,
    /// Resolving the author of a row being admitted (§6.7): always persisted.
    AdmittedAuthor,
}

/// Membership in the `did_cache` persistence set (§6.7): members ∪ `scope_dids`.
/// Implemented over the in-memory `ScopeState` (§9.3).
pub trait PersistencePolicy: Send + Sync {
    fn in_persistence_set(&self, did: &str) -> bool;
}

/// Whether a resolved document goes to `did_cache` (§6.7). Everything else
/// goes to the bounded in-process cache, so request traffic cannot grow
/// `did_cache`.
#[must_use]
pub fn should_persist(intent: ResolveIntent, policy: &dyn PersistencePolicy, did: &str) -> bool {
    match intent {
        ResolveIntent::AdmittedAuthor => true,
        ResolveIntent::Lookup => policy.in_persistence_set(did),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    struct Set(HashSet<&'static str>);

    impl PersistencePolicy for Set {
        fn in_persistence_set(&self, did: &str) -> bool {
            self.0.contains(did)
        }
    }

    #[test]
    fn persistence_set_decisions() {
        let policy = Set(HashSet::from(["did:plc:member", "did:plc:scoped"]));
        // Members and in-scope DIDs persist on any lookup.
        assert!(should_persist(ResolveIntent::Lookup, &policy, "did:plc:member"));
        assert!(should_persist(ResolveIntent::Lookup, &policy, "did:plc:scoped"));
        // Authors of admitted rows always persist.
        assert!(should_persist(ResolveIntent::AdmittedAuthor, &policy, "did:plc:author"));
        // Everything else (anonymous resolveDid, cascade lookups, JWT issuers
        // outside the set, including self-minted did:web) stays in-process.
        assert!(!should_persist(ResolveIntent::Lookup, &policy, "did:plc:stranger"));
        assert!(!should_persist(ResolveIntent::Lookup, &policy, "did:web:attacker.example"));
    }

    #[test]
    fn transient_classification() {
        assert!(IdentityError::Upstream { did: "d".into(), status: 503 }.is_transient());
        assert!(IdentityError::Upstream { did: "d".into(), status: 429 }.is_transient());
        assert!(!IdentityError::Upstream { did: "d".into(), status: 400 }.is_transient());
        assert!(!IdentityError::NotFound("d".into()).is_transient());
        assert!(IdentityError::Net(NetError::Timeout).is_transient());
    }
}
