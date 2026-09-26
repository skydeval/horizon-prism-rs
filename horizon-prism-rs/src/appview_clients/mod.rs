//! Outbound AppView clients and service-JWT minting (design-v2 §3.2, §6.8,
//! §8).

pub mod client;
pub mod jwt;

pub use client::{AppViewAuth, AppViewClient, CALL_TIMEOUT, from_config};
