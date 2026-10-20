//! Scope, admission and aggregation (design-v2 §9).
//!
//! - [`scope_state`]: `ScopeState`, the in-memory sets behind the §6.11.7 guard.
//! - [`admit`]: the one admission predicate (§9.3).
//! - [`policy`]: the scope policy and the scope predicate (§9.2).
//! - [`growth`]: scope growth under the subject's shared DID lock (§9.4).
//! - [`purge`]: the two-phase purges (§6.11.7).
//! - [`rebuild`]: the daily scope sweep and the weekly counts repair (§9.4,
//!   §9.5; scope-recency design §3.3).
//! - [`counts`]: Constellation count enrichment (§9.6).
//! - [`expiry`]: the hourly never-ready expiry (membership design §3.2).

pub mod admit;
pub mod counts;
pub mod growth;
pub mod policy;
pub mod enroll;
pub mod expiry;
pub mod membership;
pub mod pack_counts;
pub mod purge;
pub mod rebuild;
pub mod scope_state;

pub use admit::{Admission, AdmissionInput, AdmitRule, RecordKind, RejectReason, admit};
pub use growth::GrowthPlan;
pub use policy::{REASON_FOLLOWED, REASON_INTERACTED, REASON_MEMBER, REASON_SUBSCRIBED, in_index_scope};
pub use purge::{MarkerGuard, MarkerOutcome, PurgeLifecycle, Purger};
pub use rebuild::Rebuild;
pub use scope_state::{PurgeKind, ScopeSets, ScopeState};
