//! Small public views built here so their handlers return a sealed
//! [`PublicEmit`] (§7.2): the `app.nearhorizon.constellation.resolveDid`
//! view (§8.7), the `index.requestRecord` status (§5.9) and the
//! `actor.getMembership` answer (private posting §4.1). Every input is
//! public: the DID document, the routing table, scope membership, the
//! caller's own write outcome and the caller's own membership.

use serde_json::{Value, json};

use super::PublicEmit;

/// The fields of a routing hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingHint {
    pub did: String,
    pub pds: Option<String>,
    pub handle: Option<String>,
    /// Network name, `community`, or `None` when unknown.
    pub home_network: Option<String>,
    /// `None` for `community` (use this AppView) and for unknown. Unless
    /// the public-indexing mode is On, a `community` DID names the first
    /// cascade AppView instead (C4).
    pub home_appview: Option<String>,
    /// Enabled network names in fallback order.
    pub cascade: Vec<String>,
    /// `Hydrator::stores_public_of`: the mode is On and the DID is in
    /// `scope_dids`.
    pub local_indexed: bool,
}

/// The `app.nearhorizon.index.requestRecord` output (§5.9): the outcome for
/// the caller's own record only.
#[must_use]
pub fn index_status(status: crate::firehose::index_request::IndexStatus) -> PublicEmit<Value> {
    PublicEmit::new(json!({ "status": status.as_str() }))
}

/// The caller's membership as `app.nearhorizon.actor.getMembership`
/// answers it (private posting §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipStatus {
    /// A ready member.
    Member,
    /// Enrolled, not ready yet (or being purged).
    Indexing,
    /// Not enrolled now: "not now", not "never".
    NotEligible,
}

impl MembershipStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Indexing => "indexing",
            Self::NotEligible => "notEligible",
        }
    }
}

/// The `getMembership` output: the caller's own state and the private
/// families this build serves. No private fact is in it.
#[must_use]
pub fn membership(status: MembershipStatus, features: &[&str]) -> PublicEmit<Value> {
    PublicEmit::new(json!({ "status": status.as_str(), "features": features }))
}

/// The `resolveDid` output.
#[must_use]
pub fn routing_hint(h: &RoutingHint) -> PublicEmit<Value> {
    PublicEmit::new(json!({
        "did": h.did,
        "pds": h.pds,
        "handle": h.handle,
        "homeNetwork": h.home_network,
        "homeAppview": h.home_appview,
        "cascade": h.cascade,
        "capabilities": { "localIndexed": h.local_indexed },
    }))
}
