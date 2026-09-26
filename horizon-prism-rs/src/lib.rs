//! horizon-prism-rs: a Rust-native ATProto AppView with kryphocron private-tier
//! reads and Constellation-aware multi-AppView routing (design-v2 §1).
//!
//! Modules are added in the §15 implementation order.

pub mod aggregations;
pub mod config;
pub mod db;
pub mod firehose;
pub mod identity;
pub mod net;
pub mod private_tier;
