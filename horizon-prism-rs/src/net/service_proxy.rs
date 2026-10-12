//! The `service_proxy` transport (design-v3 §6.7.8): one client per proxied
//! `[services.*]` block, used only by `services::proxy`.
//!
//! It follows [`super::NetClient`]'s rules (`https`, the resolver guard, no
//! environment proxy, capped bodies) with three differences:
//! - **No redirects.** A redirect would re-send the forwarded
//!   `Authorization` to a host the operator never named.
//! - **System lookup.** The one name it resolves is the configured
//!   `proxy_url` host, through `/etc/resolv.conf`: inside compose that is the
//!   resolver that knows a service name such as `contact`.
//! - **`allow_private_network`.** For that host only, `http` is permitted and
//!   loopback, RFC 1918, CGNAT and unique-local addresses are admitted.
//!   Link-local (which holds cloud metadata) and every other blocked range
//!   stay blocked. Over `http`, every resolved address must be one of the
//!   admitted private ones, so a verified token never crosses a public
//!   network in cleartext.
//!
//! The target URL is `<proxy_url>/xrpc/<nsid>?<query>`. Its host and path
//! never come from a request.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use ipnet::{Ipv4Net, Ipv6Net};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode, redirect};
use url::{Host, Url};

use super::client::{CONNECT_TIMEOUT, NetError};
use super::resolver::{BoxError, HickoryLookup, HostLookup, is_blocked};

/// The IPv4 ranges `allow_private_network` admits.
static PRIVATE_V4: LazyLock<Vec<Ipv4Net>> = LazyLock::new(|| {
    [
        "10.0.0.0/8",     // private (RFC 1918)
        "100.64.0.0/10",  // CGNAT
        "127.0.0.0/8",    // loopback
        "172.16.0.0/12",  // private (RFC 1918)
        "192.168.0.0/16", // private (RFC 1918)
    ]
    .iter()
    .map(|s| s.parse().expect("static IPv4 range"))
    .collect()
});

/// The IPv6 ranges `allow_private_network` admits. IPv4-mapped and
/// IPv4-compatible forms of a private IPv4 address are not among them.
static PRIVATE_V6: LazyLock<Vec<Ipv6Net>> = LazyLock::new(|| {
    [
        "::1/128",  // loopback
        "fc00::/7", // unique-local
    ]
    .iter()
    .map(|s| s.parse().expect("static IPv6 range"))
    .collect()
});

/// True when `ip` is in a range `allow_private_network` admits.
#[must_use]
pub fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => PRIVATE_V4.iter().any(|net| net.contains(&v4)),
        IpAddr::V6(v6) => PRIVATE_V6.iter().any(|net| net.contains(&v6)),
    }
}

/// Which addresses one service's client may connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Guard {
    /// `allow_private_network`.
    allow_private: bool,
    /// The target is `http`: only private addresses, and all of them.
    cleartext: bool,
}

/// No address of the target may be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no usable address for the proxy target {host}")]
struct Refused {
    host: String,
}

impl Guard {
    /// The addresses of `host` this client may use.
    fn admit(self, host: &str, addrs: Vec<IpAddr>) -> Result<Vec<IpAddr>, Refused> {
        let refused = || Refused { host: host.to_string() };
        if self.cleartext {
            // One public address is enough to refuse: the connection could
            // pick it.
            return if !addrs.is_empty() && addrs.iter().all(|ip| is_private(*ip)) {
                Ok(addrs)
            } else {
                Err(refused())
            };
        }
        let kept: Vec<IpAddr> = addrs
            .into_iter()
            .filter(|ip| !is_blocked(*ip) || (self.allow_private && is_private(*ip)))
            .collect();
        if kept.is_empty() { Err(refused()) } else { Ok(kept) }
    }
}

struct ServiceResolver<L: HostLookup> {
    lookup: Arc<L>,
    guard: Guard,
}

impl<L: HostLookup> Resolve for ServiceResolver<L> {
    fn resolve(&self, name: Name) -> Resolving {
        let lookup = Arc::clone(&self.lookup);
        let guard = self.guard;
        let host = name.as_str().to_string();
        Box::pin(async move {
            let resolved = lookup.lookup(host.clone()).await?;
            let kept = guard.admit(&host, resolved).map_err(|e| {
                tracing::warn!(event = "service_proxy_target_refused", host, "no usable address for the proxy target");
                Box::new(e) as BoxError
            })?;
            let addrs: Addrs = Box::new(kept.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok::<Addrs, BoxError>(addrs)
        })
    }
}

/// Settings of one service's client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceProxyConfig {
    /// The validated `proxy_url`: a scheme, a host and an optional port.
    pub proxy_url: String,
    pub allow_private_network: bool,
    /// The AppView's user agent ([`super::user_agent`]).
    pub user_agent: String,
}

/// One forwarded call.
#[derive(Debug)]
pub struct Forward<'a> {
    /// `POST` when true, else `GET`.
    pub post: bool,
    /// The route's NSID, from the service table.
    pub nsid: &'a str,
    /// The request's query string, copied as it came.
    pub raw_query: Option<&'a str>,
    /// The headers to send: the caller's allowlist, nothing else.
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
    /// The whole call, including reading the response body.
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

/// The target's answer, whatever its status.
#[derive(Debug)]
pub struct Upstream {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

/// Why a forwarded call has no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardError {
    /// `timeout` elapsed.
    Timeout,
    /// Connect, TLS, reset, or an address the guard refuses.
    Transport,
    /// The response body is over `max_response_bytes`.
    TooLarge,
}

/// The client of one proxied service. Cheap to clone.
#[derive(Debug, Clone)]
pub struct ServiceProxyClient {
    http: reqwest::Client,
    /// `scheme://host[:port]`.
    origin: String,
    /// The target is an IP literal the guard refuses. A literal never
    /// reaches the resolver, so it is checked here.
    refused_literal: bool,
}

impl ServiceProxyClient {
    /// Build the client with the system DNS configuration.
    ///
    /// # Errors
    ///
    /// [`NetError::Build`] when DNS configuration or TLS setup fails, and
    /// [`NetError::InvalidUrl`] for a `proxy_url` validation would refuse.
    pub fn new(config: &ServiceProxyConfig) -> Result<Self, NetError> {
        let lookup = HickoryLookup::from_system().map_err(|e| NetError::Build(e.to_string()))?;
        Self::with_lookup(config, lookup)
    }

    /// Build the client with a specific DNS backend.
    ///
    /// # Errors
    ///
    /// As [`ServiceProxyClient::new`].
    pub fn with_lookup<L: HostLookup>(config: &ServiceProxyConfig, lookup: L) -> Result<Self, NetError> {
        let invalid = |reason: &str| NetError::InvalidUrl {
            url: config.proxy_url.clone(),
            reason: reason.to_string(),
        };
        let url = Url::parse(&config.proxy_url).map_err(|e| invalid(&e.to_string()))?;
        let cleartext = match url.scheme() {
            "https" => false,
            "http" if config.allow_private_network => true,
            _ => return Err(invalid("not https, or http with allow_private_network")),
        };
        let guard = Guard {
            allow_private: config.allow_private_network,
            cleartext,
        };
        let literal = match url.host() {
            None => return Err(invalid("no host")),
            Some(Host::Domain(_)) => None,
            Some(Host::Ipv4(v4)) => Some(IpAddr::V4(v4)),
            Some(Host::Ipv6(v6)) => Some(IpAddr::V6(v6)),
        };
        let refused_literal = literal.is_some_and(|ip| guard.admit(&ip.to_string(), vec![ip]).is_err());
        if refused_literal {
            tracing::warn!(event = "service_proxy_target_refused", "the proxy target's address is in a blocked range");
        }
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .dns_resolver(Arc::new(ServiceResolver {
                lookup: Arc::new(lookup),
                guard,
            }))
            .redirect(redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(config.user_agent.clone())
            .build()
            .map_err(|e| NetError::Build(e.to_string()))?;
        Ok(Self {
            http,
            origin: url.origin().ascii_serialization(),
            refused_literal,
        })
    }

    /// Send one call to the target and buffer its answer.
    ///
    /// # Errors
    ///
    /// [`ForwardError`] when the target gave no usable answer in time.
    pub async fn forward(&self, call: Forward<'_>) -> Result<Upstream, ForwardError> {
        if self.refused_literal {
            return Err(ForwardError::Transport);
        }
        let mut target = format!("{}/xrpc/{}", self.origin, call.nsid);
        if let Some(query) = call.raw_query {
            target.push('?');
            target.push_str(query);
        }
        let url = Url::parse(&target).map_err(|_| ForwardError::Transport)?;
        let method = if call.post { Method::POST } else { Method::GET };
        let mut request = self.http.request(method, url).headers(call.headers);
        if let Some(body) = call.body {
            request = request.body(body);
        }
        let cap = call.max_response_bytes;
        let answer = async move {
            let mut response = request.send().await.map_err(|_| ForwardError::Transport)?;
            if response.content_length().is_some_and(|len| len > cap as u64) {
                return Err(ForwardError::TooLarge);
            }
            let status = response.status();
            let headers = response.headers().clone();
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| ForwardError::Transport)? {
                if body.len() + chunk.len() > cap {
                    return Err(ForwardError::TooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(Upstream { status, headers, body })
        };
        tokio::time::timeout(call.timeout, answer)
            .await
            .unwrap_or(Err(ForwardError::Timeout))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use futures::future::BoxFuture;

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("test address")
    }

    const PUBLIC: Guard = Guard {
        allow_private: false,
        cleartext: false,
    };
    const PRIVATE_TLS: Guard = Guard {
        allow_private: true,
        cleartext: false,
    };
    const CLEARTEXT: Guard = Guard {
        allow_private: true,
        cleartext: true,
    };

    #[test]
    fn private_ranges() {
        for addr in ["10.1.2.3", "172.16.0.1", "172.31.255.255", "192.168.1.1", "100.64.0.1", "127.0.0.1", "::1", "fd00::1", "fc00::1"] {
            assert!(is_private(ip(addr)), "{addr}");
        }
        for addr in [
            "8.8.8.8",
            "169.254.169.254",
            "0.0.0.0",
            "224.0.0.1",
            "172.32.0.1",
            "fe80::1",
            "::",
            "64:ff9b::a00:1",
            "2002:a00:1::1",
            "2001::1",
            "::ffff:10.0.0.1",
            "::10.0.0.1",
            "2606:4700::1",
        ] {
            assert!(!is_private(ip(addr)), "{addr}");
        }
    }

    #[test]
    fn the_guard_admits_private_addresses_only_when_allowed() {
        let all = vec![ip("93.184.216.34"), ip("10.0.0.5"), ip("169.254.169.254")];
        // Without the exception: the public guard.
        assert_eq!(PUBLIC.admit("t", all.clone()), Ok(vec![ip("93.184.216.34")]));
        assert!(PUBLIC.admit("t", vec![ip("10.0.0.5")]).is_err());
        // With it, over TLS: private addresses join, link-local never does.
        assert_eq!(PRIVATE_TLS.admit("t", all), Ok(vec![ip("93.184.216.34"), ip("10.0.0.5")]));
        for blocked in ["169.254.169.254", "fe80::1", "0.0.0.0", "::", "224.0.0.1", "::ffff:10.0.0.1", "64:ff9b::a00:1"] {
            assert!(PRIVATE_TLS.admit("t", vec![ip(blocked)]).is_err(), "{blocked}");
        }
        assert!(PRIVATE_TLS.admit("t", vec![]).is_err());
    }

    #[test]
    fn cleartext_needs_every_address_private() {
        assert_eq!(CLEARTEXT.admit("t", vec![ip("172.18.0.5"), ip("fd00::5")]), Ok(vec![ip("172.18.0.5"), ip("fd00::5")]));
        assert!(CLEARTEXT.admit("t", vec![ip("172.18.0.5"), ip("93.184.216.34")]).is_err());
        assert!(CLEARTEXT.admit("t", vec![ip("93.184.216.34")]).is_err());
        assert!(CLEARTEXT.admit("t", vec![ip("169.254.169.254")]).is_err());
        assert!(CLEARTEXT.admit("t", vec![]).is_err());
    }

    struct MockLookup(HashMap<String, Vec<IpAddr>>);

    impl HostLookup for MockLookup {
        fn lookup(&self, host: String) -> BoxFuture<'static, Result<Vec<IpAddr>, BoxError>> {
            let answer = self.0.get(&host).cloned();
            Box::pin(async move { answer.ok_or_else(|| BoxError::from(format!("NXDOMAIN {host}"))) })
        }
    }

    #[tokio::test]
    async fn the_resolver_applies_the_guard() {
        let resolver = |guard| ServiceResolver {
            lookup: Arc::new(MockLookup(HashMap::from([
                ("contact".to_string(), vec![ip("172.18.0.5")]),
                ("split.example".to_string(), vec![ip("172.18.0.5"), ip("93.184.216.34")]),
            ]))),
            guard,
        };
        let resolve = |guard, host: &'static str| async move {
            let addrs = resolver(guard).resolve(Name::from_str(host).expect("name")).await?;
            Ok::<Vec<SocketAddr>, BoxError>(addrs.collect())
        };
        assert_eq!(resolve(CLEARTEXT, "contact").await.expect("private"), [SocketAddr::new(ip("172.18.0.5"), 0)]);
        assert!(resolve(PUBLIC, "contact").await.is_err());
        assert!(resolve(CLEARTEXT, "split.example").await.is_err());
        assert_eq!(resolve(PUBLIC, "split.example").await.expect("public"), [SocketAddr::new(ip("93.184.216.34"), 0)]);
        assert!(resolve(PRIVATE_TLS, "missing.example").await.is_err());
    }

    fn config(proxy_url: &str, allow_private_network: bool) -> ServiceProxyConfig {
        ServiceProxyConfig {
            proxy_url: proxy_url.to_string(),
            allow_private_network,
            user_agent: "horizon-prism-rs/test".to_string(),
        }
    }

    fn client(proxy_url: &str, allow_private_network: bool) -> Result<ServiceProxyClient, NetError> {
        ServiceProxyClient::with_lookup(&config(proxy_url, allow_private_network), MockLookup(HashMap::new()))
    }

    fn call(nsid: &str) -> Forward<'_> {
        Forward {
            post: false,
            nsid,
            raw_query: None,
            headers: HeaderMap::new(),
            body: None,
            timeout: Duration::from_secs(2),
            max_response_bytes: 1024,
        }
    }

    #[tokio::test]
    async fn schemes_and_literal_targets() {
        assert!(client("http://contact:8080", false).is_err());
        assert!(client("ftp://contact", true).is_err());
        assert_eq!(client("https://contact.example.net:8443/", false).expect("client").origin, "https://contact.example.net:8443");
        // A blocked literal is refused at call time, without a connection.
        for (url, private) in [
            ("https://10.0.0.5", false),
            ("https://169.254.169.254", true),
            ("http://93.184.216.34", true),
            ("http://169.254.169.254", true),
        ] {
            let c = client(url, private).expect("client");
            assert_eq!(c.forward(call("eu.example.a.b")).await.err(), Some(ForwardError::Transport), "{url}");
        }
        // A name the lookup cannot answer is a transport failure too.
        let c = client("https://missing.example", false).expect("client");
        assert_eq!(c.forward(call("eu.example.a.b")).await.err(), Some(ForwardError::Transport));
    }
}
