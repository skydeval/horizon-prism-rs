//! Outbound HTTP (design-v2 §8.5; design-v4 §II-8.5): SSRF-guarded
//! `reqwest::Client`s, one per `net` intent ([`Transports`]), plus one
//! `service_proxy` client per proxied service ([`service_proxy`]).

pub mod client;
pub mod resolver;
pub mod service_proxy;
pub mod transport;

#[cfg(test)]
mod intent_tests;

pub use client::{
    BodyCap, CONNECT_TIMEOUT, DEFAULT_TIMEOUT, Intent, MAX_REDIRECTS, NetClient, NetConfig, NetError,
    NetResponse, RequestOptions, user_agent,
};
pub use resolver::{AllAddressesBlocked, GuardedResolver, HickoryLookup, HostLookup, TxtSource, is_blocked};
pub use transport::{FetchMode, Transport, Transports};
