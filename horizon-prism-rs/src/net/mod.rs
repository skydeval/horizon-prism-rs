//! Outbound HTTP (design-v2 §8.5): one SSRF-guarded `reqwest::Client` for
//! every outbound call.

pub mod client;
pub mod resolver;
pub mod service_proxy;

pub use client::{
    BodyCap, CONNECT_TIMEOUT, DEFAULT_TIMEOUT, MAX_REDIRECTS, NetClient, NetConfig, NetError,
    NetResponse, RequestOptions, user_agent,
};
pub use resolver::{AllAddressesBlocked, GuardedResolver, HickoryLookup, HostLookup, is_blocked};
