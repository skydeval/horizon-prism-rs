//! The `app.nearhorizon.constellation.resolveDid` view (design-v2 §8.7),
//! built here so the handler returns a sealed [`PublicEmit`] (§7.2). Every
//! input is public: the DID document, the routing table, and scope
//! membership.

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
    /// `None` for `community` (use this AppView) and for unknown.
    pub home_appview: Option<String>,
    /// Enabled network names in fallback order.
    pub cascade: Vec<String>,
    /// The DID is in `scope_dids`.
    pub local_indexed: bool,
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
