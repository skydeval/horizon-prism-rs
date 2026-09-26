//! Scope, admission and aggregation (design-v2 §9).
//!
//! - [`scope_state`]: `ScopeState`, the in-memory sets behind the §6.11.7 guard.
//! - [`admit`]: the one admission predicate (§9.3).
//! - [`policy`]: the scope policy and the scope predicate (§9.2).
//! - [`growth`]: scope growth under the subject's shared DID lock (§9.4).
//! - [`purge`]: the two-phase purges (§6.11.7).
//! - [`rebuild`]: scope rebuild and its weekly schedule (§9.4, §9.5).
//! - [`counts`]: Constellation count enrichment (§9.6).

pub mod admit;
pub mod counts;
pub mod growth;
pub mod policy;
pub mod enroll;
pub mod membership;
pub mod purge;
pub mod rebuild;
pub mod scope_state;

pub use admit::{Admission, AdmissionInput, AdmitRule, RecordKind, RejectReason, admit};
pub use growth::GrowthPlan;
pub use policy::{REASON_FOLLOWED, REASON_INTERACTED, REASON_MEMBER, in_index_scope};
pub use purge::{PurgeLifecycle, Purger};
pub use rebuild::Rebuild;
pub use scope_state::{PurgeKind, ScopeSets, ScopeState};
