//! horizon-prism-rs: a Rust-native ATProto AppView with kryphocron private-tier
//! reads and Constellation-aware multi-AppView routing (design-v2 §1).
//!
//! Modules are added in the §15 implementation order.

// §3.1, §6.3: the public codec surface (`ContentCodec::decode`,
// `DecodeContext`, `laquna::Codec`) is forbidden by clippy.toml. One
// `#[expect]` lets `kryphocron_glue::bounded_codec` build laquna for the
// hooks, to set its decode bound (private-embeds design S3).
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

// The stand-in exception is test-only (docs/internal/design/
// standin-guard-exception.md §3.1): it never builds without debug assertions.
#[cfg(all(feature = "standin-hosts", not(debug_assertions)))]
compile_error!("standin-hosts is test-only: build it with --profile standin, never --release");

pub mod admin;
pub mod admin_executor;
pub mod aggregations;
pub mod appview_clients;
pub mod cli;
pub mod config;
pub mod constellation;
pub mod cursor_key;
pub mod db;
pub mod farsight;
pub mod firehose;
pub mod honest_501;
pub mod hydration;
pub mod identity;
pub mod indexing;
pub(crate) mod kryphocron_glue;
pub mod microcosm;
pub mod moderation;
pub mod net;
pub mod plural;
pub mod private_tier;
pub mod public_page;
pub mod refract;
pub mod serve;
pub mod services;
pub mod subscriptions;
pub mod sync;
pub mod system_accounts;
pub mod thread_outline;
pub mod threadlens;
pub mod xrpc;

#[cfg(test)]
mod name_guard;
#[cfg(test)]
pub(crate) mod test_support;
