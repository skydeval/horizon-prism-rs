//! horizon-prism-rs: a Rust-native ATProto AppView with kryphocron private-tier
//! reads and Constellation-aware multi-AppView routing (design-v2 §1).
//!
//! Modules are added in the §15 implementation order.

pub mod aggregations;
pub mod config;
pub mod db;
pub mod firehose;
pub mod identity;
pub mod microcosm;
pub mod net;
pub mod private_tier;
pub mod serve;

#[cfg(test)]
pub(crate) mod test_support;
