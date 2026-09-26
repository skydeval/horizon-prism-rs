//! horizon-prism-rs: a Rust-native ATProto AppView with kryphocron private-tier
//! reads and Constellation-aware multi-AppView routing (design-v2 §1).
//!
//! Modules are added in the §15 implementation order.

pub mod admin_executor;
pub mod aggregations;
pub mod cli;
pub mod config;
pub mod db;
pub mod firehose;
pub mod hydration;
pub mod identity;
pub(crate) mod kryphocron_glue;
pub mod microcosm;
pub mod moderation;
pub mod net;
pub mod private_tier;
pub mod serve;
pub mod sync;
pub mod xrpc;

#[cfg(test)]
pub(crate) mod test_support;
