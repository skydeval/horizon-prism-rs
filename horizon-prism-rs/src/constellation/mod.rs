//! Constellation multi-AppView routing (design-v2 §8): the routing table
//! and PDS matching, response classification, the cascade with per-network
//! token buckets and breakers, and the `resolveDid` client hint.

pub mod cascade;
pub mod classifier;
pub mod matcher;
pub mod resolve_did;
pub mod routing;

pub use cascade::{Call, Cascade, Fetched};
pub use classifier::Class;
pub use routing::{DidLookup, DidSummary, Router, RoutingDecision, RoutingTable};
