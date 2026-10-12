//! SSRF-guarded DNS resolution (design-v2 §8.5).
//!
//! Every resolved address in a blocked range is dropped. If no address
//! survives, resolution fails and the call is classified `transient`. IP-literal
//! hosts never reach a resolver (hyper connects to them directly), so the
//! client checks them with [`is_blocked`] before sending and on every redirect.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, LazyLock};

use futures::future::BoxFuture;
use ipnet::{Ipv4Net, Ipv6Net};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// Error type reqwest expects from a resolver.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// IPv4 ranges that outbound calls may never reach (§8.5).
static BLOCKED_V4: LazyLock<Vec<Ipv4Net>> = LazyLock::new(|| {
    [
        "0.0.0.0/8",      // "this network", including unspecified
        "10.0.0.0/8",     // private (RFC 1918)
        "100.64.0.0/10",  // CGNAT
        "127.0.0.0/8",    // loopback
        "169.254.0.0/16", // link-local
        "172.16.0.0/12",  // private (RFC 1918)
        "192.168.0.0/16", // private (RFC 1918)
        "224.0.0.0/4",    // multicast
        "240.0.0.0/4",    // reserved, including limited broadcast
    ]
    .iter()
    .map(|s| s.parse().expect("static IPv4 range"))
    .collect()
});

/// IPv6 ranges that outbound calls may never reach (§8.5). IPv4-mapped and
/// IPv4-compatible addresses are checked against [`BLOCKED_V4`] by their
/// embedded IPv4 address.
static BLOCKED_V6: LazyLock<Vec<Ipv6Net>> = LazyLock::new(|| {
    [
        "::/128",        // unspecified
        "::1/128",       // loopback
        "64:ff9b::/96",  // NAT64
        "2001::/32",     // Teredo
        "2002::/16",     // 6to4
        "fc00::/7",      // unique-local
        "fe80::/10",     // link-local
        "ff00::/8",      // multicast
    ]
    .iter()
    .map(|s| s.parse().expect("static IPv6 range"))
    .collect()
});

/// True when `ip` is in a blocked range.
#[must_use]
pub fn is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4_blocked(v4),
        IpAddr::V6(v6) => v6_blocked(v6),
    }
}

fn v4_blocked(ip: Ipv4Addr) -> bool {
    BLOCKED_V4.iter().any(|net| net.contains(&ip))
}

fn v6_blocked(ip: Ipv6Addr) -> bool {
    if BLOCKED_V6.iter().any(|net| net.contains(&ip)) {
        return true;
    }
    // IPv4-mapped (::ffff:0:0/96) and IPv4-compatible (::/96) forms of a
    // blocked IPv4 address. `to_ipv4` converts both forms.
    embedded_v4(ip).is_some_and(v4_blocked)
}

/// The IPv4 address inside an IPv4-mapped or IPv4-compatible IPv6 address.
fn embedded_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = ip.segments();
    let mapped = s[..5] == [0; 5] && s[5] == 0xffff;
    let compatible = s[..6] == [0; 6];
    if mapped || compatible {
        ip.to_ipv4()
    } else {
        None
    }
}

/// The resolved addresses of `host`, with blocked ones removed.
///
/// # Errors
///
/// [`AllAddressesBlocked`] when nothing survives.
pub fn filter_resolved(host: &str, addrs: Vec<IpAddr>) -> Result<Vec<IpAddr>, AllAddressesBlocked> {
    let total = addrs.len();
    let kept: Vec<IpAddr> = addrs.into_iter().filter(|ip| !is_blocked(*ip)).collect();
    if kept.is_empty() {
        tracing::warn!(
            event = "ssrf_guard_blocked",
            host,
            resolved = total,
            "every resolved address is in a blocked range",
        );
        return Err(AllAddressesBlocked {
            host: host.to_string(),
        });
    }
    Ok(kept)
}

/// No resolved address of `host` survived the guard.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no address of {host} is outside the blocked ranges")]
pub struct AllAddressesBlocked {
    pub host: String,
}

/// A DNS lookup backend. The production one is hickory; tests use fixed answers.
pub trait HostLookup: Send + Sync + 'static {
    /// Resolve `host` to its IP addresses.
    fn lookup(&self, host: String) -> BoxFuture<'static, Result<Vec<IpAddr>, BoxError>>;
}

/// hickory-resolver backed lookup using the system resolver configuration.
#[derive(Clone)]
pub struct HickoryLookup {
    resolver: hickory_resolver::TokioResolver,
}

impl HickoryLookup {
    /// Build from `/etc/resolv.conf`.
    ///
    /// # Errors
    ///
    /// The resolver configuration could not be read, or the resolver could
    /// not be built (hickory 0.26 made `build` fallible).
    pub fn from_system() -> Result<Self, hickory_resolver::net::NetError> {
        let resolver = hickory_resolver::TokioResolver::builder_tokio()?.build()?;
        Ok(Self { resolver })
    }
}

/// What a TXT lookup found, as far as its caller needs to tell apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxtAnswer {
    /// The TXT records of the name, each with its strings joined.
    Records(Vec<String>),
    /// The name does not exist, or has no TXT record (NXDOMAIN, NODATA).
    NoSuchName,
    /// Anything else: a timeout, SERVFAIL, REFUSED. Says nothing about
    /// the name.
    Failed,
}

/// Whether a lookup failed because the name does not exist or has no
/// record of the type asked for.
#[must_use]
pub fn no_such_name(error: &hickory_resolver::net::NetError) -> bool {
    use hickory_resolver::net::{DnsError, NetError};
    use hickory_resolver::proto::op::ResponseCode;

    matches!(
        error,
        NetError::Dns(DnsError::NoRecordsFound(none)) if matches!(none.response_code, ResponseCode::NXDomain | ResponseCode::NoError)
    )
}

/// `host` as a fully qualified name, so the resolver never retries it with
/// a search domain (which can turn a server failure into "no such name").
/// An address is left as it is.
fn fully_qualified(host: &str) -> String {
    if host.ends_with('.') || host.parse::<IpAddr>().is_ok() {
        host.to_string()
    } else {
        format!("{host}.")
    }
}

impl HickoryLookup {
    /// The TXT records of `name`, asked as a fully qualified name.
    pub async fn txt(&self, name: &str) -> TxtAnswer {
        use hickory_resolver::proto::rr::RData;

        match self.resolver.txt_lookup(fully_qualified(name)).await {
            Ok(lookup) => TxtAnswer::Records(
                lookup
                    .answers()
                    .iter()
                    .filter_map(|record| match &record.data {
                        RData::TXT(txt) => Some(txt.txt_data.iter().map(|part| String::from_utf8_lossy(part)).collect::<String>()),
                        _ => None,
                    })
                    .collect(),
            ),
            Err(error) if no_such_name(&error) => TxtAnswer::NoSuchName,
            Err(_) => TxtAnswer::Failed,
        }
    }
}

impl HostLookup for HickoryLookup {
    fn lookup(&self, host: String) -> BoxFuture<'static, Result<Vec<IpAddr>, BoxError>> {
        let resolver = self.resolver.clone();
        Box::pin(async move {
            let lookup = resolver
                .lookup_ip(fully_qualified(&host))
                .await
                .map_err(|e| Box::new(e) as BoxError)?;
            let ips: Vec<IpAddr> = lookup.iter().collect();
            Ok::<Vec<IpAddr>, BoxError>(ips)
        })
    }
}

/// reqwest DNS resolver that applies the §8.5 guard to every answer.
pub struct GuardedResolver<L: HostLookup> {
    lookup: Arc<L>,
}

impl<L: HostLookup> GuardedResolver<L> {
    pub fn new(lookup: L) -> Self {
        Self {
            lookup: Arc::new(lookup),
        }
    }
}

impl<L: HostLookup> Resolve for GuardedResolver<L> {
    fn resolve(&self, name: Name) -> Resolving {
        let lookup = Arc::clone(&self.lookup);
        let host = name.as_str().to_string();
        Box::pin(async move {
            let resolved = lookup.lookup(host.clone()).await?;
            let kept = filter_resolved(&host, resolved).map_err(|e| Box::new(e) as BoxError)?;
            // Port 0 is replaced by the scheme's port (or the URL's explicit port).
            let addrs: Addrs = Box::new(kept.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok::<Addrs, BoxError>(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("test address")
    }

    #[test]
    fn every_listed_ipv4_range_is_blocked() {
        for addr in [
            "0.0.0.0",
            "0.1.2.3",
            "10.0.0.1",
            "10.255.255.255",
            "100.64.0.1",
            "100.127.255.254",
            "127.0.0.1",
            "127.255.255.255",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "224.0.0.1",
            "239.255.255.250",
            "255.255.255.255",
        ] {
            assert!(is_blocked(ip(addr)), "{addr} should be blocked");
        }
    }

    #[test]
    fn every_listed_ipv6_range_is_blocked() {
        for addr in [
            "::",
            "::1",
            "64:ff9b::1.2.3.4",
            "64:ff9b::8.8.8.8",
            "2001::1",
            "2001:0:ffff::1",
            "2002::1",
            "2002:c000:0204::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "febf::1",
            "ff02::1",
            "ff0e::1",
        ] {
            assert!(is_blocked(ip(addr)), "{addr} should be blocked");
        }
    }

    #[test]
    fn ipv4_mapped_and_compatible_forms_are_blocked() {
        for addr in [
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:100.64.0.1",
            "::ffff:192.168.0.1",
            "::ffff:0.0.0.0",
            "::ffff:224.0.0.1",
            "::127.0.0.1",
            "::10.0.0.1",
            "::172.16.0.1",
            "::169.254.1.1",
        ] {
            assert!(is_blocked(ip(addr)), "{addr} should be blocked");
        }
    }

    #[test]
    fn public_addresses_pass() {
        for addr in [
            "8.8.8.8",
            "1.1.1.1",
            "100.63.255.255",
            "100.128.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.169.0.1",
            "2606:4700:4700::1111",
            "2a00:1450:4001::200e",
            "2001:4860:4860::8888",
            "::ffff:8.8.8.8",
        ] {
            assert!(!is_blocked(ip(addr)), "{addr} should pass");
        }
    }

    #[test]
    fn filter_keeps_only_public_answers() {
        let kept = filter_resolved(
            "mixed.example",
            vec![ip("127.0.0.1"), ip("93.184.216.34"), ip("fd00::1")],
        )
        .expect("one survivor");
        assert_eq!(kept, [ip("93.184.216.34")]);
        assert_eq!(
            filter_resolved("evil.example", vec![ip("10.0.0.5"), ip("::1")]),
            Err(AllAddressesBlocked {
                host: "evil.example".to_string()
            })
        );
        assert!(filter_resolved("empty.example", vec![]).is_err());
    }

    /// Fixed DNS answers.
    struct MockLookup(HashMap<String, Vec<IpAddr>>);

    impl HostLookup for MockLookup {
        fn lookup(&self, host: String) -> BoxFuture<'static, Result<Vec<IpAddr>, BoxError>> {
            let answer = self.0.get(&host).cloned();
            Box::pin(async move { answer.ok_or_else(|| BoxError::from(format!("NXDOMAIN {host}"))) })
        }
    }

    fn mock() -> GuardedResolver<MockLookup> {
        GuardedResolver::new(MockLookup(HashMap::from([
            ("public.example".to_string(), vec![ip("93.184.216.34")]),
            (
                "rebind.example".to_string(),
                vec![ip("127.0.0.1"), ip("::ffff:10.0.0.1")],
            ),
            (
                "split.example".to_string(),
                vec![ip("169.254.169.254"), ip("2606:4700::6810:84e5")],
            ),
            (
                "metadata.example".to_string(),
                vec![ip("169.254.169.254")],
            ),
        ])))
    }

    async fn resolve(r: &GuardedResolver<MockLookup>, host: &str) -> Result<Vec<SocketAddr>, BoxError> {
        let name = Name::from_str(host).expect("valid name");
        let addrs = r.resolve(name).await?;
        Ok(addrs.collect())
    }

    #[tokio::test]
    async fn resolver_passes_public_answers() {
        let addrs = resolve(&mock(), "public.example").await.expect("resolves");
        assert_eq!(addrs, [SocketAddr::new(ip("93.184.216.34"), 0)]);
    }

    #[tokio::test]
    async fn resolver_drops_blocked_answers() {
        let addrs = resolve(&mock(), "split.example").await.expect("one survivor");
        assert_eq!(addrs, [SocketAddr::new(ip("2606:4700::6810:84e5"), 0)]);
    }

    #[tokio::test]
    async fn resolver_fails_when_nothing_survives() {
        for host in ["rebind.example", "metadata.example"] {
            let err = resolve(&mock(), host).await.expect_err("all blocked");
            assert!(err.to_string().contains(host), "{err}");
        }
    }

    #[tokio::test]
    async fn resolver_propagates_lookup_failure() {
        assert!(resolve(&mock(), "missing.example").await.is_err());
    }
}
