//! The transports of the `net` intents (design-v4 §11 G-9 "Transport" and
//! "Upstream DNS", §II-8.5; R3-168, R3-177, R3-213).
//!
//! [`Transports`] holds one [`Transport`] per [`Intent`]: a [`NetClient`]
//! (so its own connection pool) and a hickory resolver (so its own DNS
//! cache), the client resolving through that same resolver. Nothing is
//! shared between two intents, so internal work never warms a public
//! request's in-process DNS cache, TLS sessions or connections, and a
//! viewer-private fetch warms nothing at all.
//!
//! - `public` forwards to `net.public_nameservers`;
//! - `internal` forwards to `net.internal_nameservers`;
//! - `viewer_private` forwards to `net.public_nameservers` with
//!   `cache_size = 0`, and its client keeps no idle connection and resumes
//!   no TLS session.
//!
//! Each list, when empty, means `/etc/resolv.conf`. A request-path caller
//! does not pick an intent: it asks [`Transports::for_request`], which reads
//! the request's [`FetchMode`].

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use super::client::{Intent, NetClient, NetConfig, NetError};
use super::resolver::{HickoryLookup, TxtSource};
use crate::config::NetSectionConfig;

/// Docker's embedded DNS: a forwarder, never private to this deployment
/// (§11 G-9).
const DOCKER_DNS: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 11));

/// One intent's client and DNS resolver. Cheap to clone; clones share both.
#[derive(Clone)]
pub struct Transport {
    client: NetClient,
    dns: Arc<dyn TxtSource>,
}

impl Transport {
    /// The intent's HTTP client.
    #[must_use]
    pub fn client(&self) -> &NetClient {
        &self.client
    }

    /// The intent's resolver, for TXT records (a handle's `_atproto` name).
    #[must_use]
    pub fn dns(&self) -> &dyn TxtSource {
        self.dns.as_ref()
    }

    #[must_use]
    pub fn intent(&self) -> Intent {
        self.client.intent()
    }
}

/// Which intent a fetch made for a request uses (design-v4 §8.1, §11 G-9).
/// [`crate::xrpc::pools::Requester`] is the one implementation: its
/// viewer-private mode selects `viewer_private`, private-context hydration
/// selects `internal`, and every other request `public`.
pub trait FetchMode {
    fn intent(&self) -> Intent;
}

/// The three transports. Cheap to clone.
#[derive(Clone)]
pub struct Transports {
    public: Transport,
    internal: Transport,
    viewer_private: Transport,
}

/// The resolver of `intent` under `nameservers`.
fn lookup_for(intent: Intent, nameservers: &NetSectionConfig) -> Result<HickoryLookup, NetError> {
    let built = match intent {
        Intent::Public => HickoryLookup::forwarding(&nameservers.public_nameservers, None),
        Intent::Internal => HickoryLookup::forwarding(&nameservers.internal_nameservers, None),
        Intent::ViewerPrivate => HickoryLookup::forwarding(&nameservers.public_nameservers, Some(0)),
    };
    built.map_err(|e| NetError::Build(format!("{} resolver: {e}", intent.as_str())))
}

impl Transports {
    /// Build the three from the same client settings, each with its own
    /// resolver (design-v4 §II-8.5).
    ///
    /// # Errors
    ///
    /// [`NetError::Build`] when a resolver or a client cannot be built.
    pub fn new(config: &NetConfig, nameservers: &NetSectionConfig) -> Result<Self, NetError> {
        let build = |intent: Intent| -> Result<Transport, NetError> {
            let lookup = lookup_for(intent, nameservers)?;
            Ok(Transport {
                client: NetClient::with_lookup(config, intent, lookup.clone())?,
                dns: Arc::new(lookup),
            })
        };
        Ok(Self {
            public: build(Intent::Public)?,
            internal: build(Intent::Internal)?,
            viewer_private: build(Intent::ViewerPrivate)?,
        })
    }

    /// Public request handlers, and the fixed upstreams (cascade AppViews,
    /// Slingshot, Constellation, PLC) whatever the request's mode.
    #[must_use]
    pub fn public(&self) -> &Transport {
        &self.public
    }

    /// Background work and the private tier.
    #[must_use]
    pub fn internal(&self) -> &Transport {
        &self.internal
    }

    /// The transport of `intent`.
    #[must_use]
    pub fn get(&self, intent: Intent) -> &Transport {
        match intent {
            Intent::Public => &self.public,
            Intent::Internal => &self.internal,
            Intent::ViewerPrivate => &self.viewer_private,
        }
    }

    /// The transport for a fetch made for `req` from a host the subject
    /// names (a handle's DNS and well-known file, a `did:web` document, a
    /// subject's PDS): `viewer_private` for a viewer-private fetch, and
    /// never the public one (design-v4 §8.1, R3-213).
    #[must_use]
    pub fn for_request(&self, req: &impl FetchMode) -> &Transport {
        self.get(req.intent())
    }

    /// Test transports for fake upstreams on 127.0.0.1: each intent's
    /// client sends its own `User-Agent` ([`super::client::test_user_agent`])
    /// and every resolver answers "failed".
    #[cfg(test)]
    #[must_use]
    pub(crate) fn loopback_for_tests() -> Self {
        let build = |intent| Transport {
            client: NetClient::loopback_for_tests_as(intent),
            dns: Arc::new(tests::NoTxt),
        };
        Self {
            public: build(Intent::Public),
            internal: build(Intent::Internal),
            viewer_private: build(Intent::ViewerPrivate),
        }
    }

    /// These transports with `dns` as `intent`'s resolver (tests).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_dns_for_tests(mut self, intent: Intent, dns: Arc<dyn TxtSource>) -> Self {
        let transport = match intent {
            Intent::Public => &mut self.public,
            Intent::Internal => &mut self.internal,
            Intent::ViewerPrivate => &mut self.viewer_private,
        };
        transport.dns = dns;
        self
    }

    /// Guarded transports over a fixed address lookup (tests).
    #[cfg(test)]
    pub(crate) fn with_lookup_for_tests<L: super::HostLookup + Clone>(config: &NetConfig, lookup: L) -> Self {
        let build = |intent| Transport {
            client: NetClient::with_lookup(config, intent, lookup.clone()).expect("client builds"),
            dns: Arc::new(tests::NoTxt),
        };
        Self {
            public: build(Intent::Public),
            internal: build(Intent::Internal),
            viewer_private: build(Intent::ViewerPrivate),
        }
    }
}

/// `[net]` values `serve` refuses (design-v4 §13): an address no query can
/// be sent to, or one listed twice.
pub fn problems(cfg: &NetSectionConfig, out: &mut Vec<String>) {
    for (key, list) in [("net.public_nameservers", &cfg.public_nameservers), ("net.internal_nameservers", &cfg.internal_nameservers)] {
        let mut seen = BTreeSet::new();
        for ip in list {
            if ip.is_unspecified() || ip.is_multicast() || *ip == IpAddr::V4(Ipv4Addr::BROADCAST) {
                out.push(format!("{key}: {ip} cannot be a nameserver"));
            }
            if !seen.insert(*ip) {
                out.push(format!("{key}: {ip} is listed twice"));
            }
        }
    }
}

/// Private to the deployment's own networks: RFC 1918, loopback, or
/// unique-local (the compose network is one of these).
fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback(),
        IpAddr::V6(v6) => v6 == Ipv6Addr::LOCALHOST || (v6.segments()[0] & 0xfe00) == 0xfc00,
    }
}

/// What `serve` warns about at startup (design-v4 §11 G-9 "Upstream DNS",
/// §13; R3-181): an internal resolver that is plainly not private to this
/// deployment. A private forwarder in front of a shared resolver cannot be
/// detected (§4.4 item 9).
#[must_use]
pub fn warnings(cfg: &NetSectionConfig) -> Vec<String> {
    let internal = &cfg.internal_nameservers;
    if internal.is_empty() {
        return vec![
            "net.internal_nameservers is empty: internal lookups go to /etc/resolv.conf's resolver, which others may query too (design-v4 §4.4 item 9)"
                .to_string(),
        ];
    }
    let mut out = Vec::new();
    let as_set = |list: &[IpAddr]| list.iter().copied().collect::<BTreeSet<_>>();
    if as_set(internal) == as_set(&cfg.public_nameservers) {
        out.push("net.internal_nameservers equals net.public_nameservers: internal and public lookups share one resolver cache".to_string());
    }
    for ip in internal {
        if *ip == DOCKER_DNS {
            out.push(format!("net.internal_nameservers: {ip} is Docker's DNS, which forwards to a shared resolver"));
        } else if !is_private(*ip) {
            out.push(format!("net.internal_nameservers: {ip} is not a private address, so others can query the same resolver"));
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::net::resolver::TxtAnswer;

    /// A resolver that answers nothing.
    pub(crate) struct NoTxt;

    #[async_trait::async_trait]
    impl TxtSource for NoTxt {
        async fn txt(&self, _name: &str) -> TxtAnswer {
            TxtAnswer::Failed
        }
    }

    fn ips(list: &[&str]) -> Vec<IpAddr> {
        list.iter().map(|s| s.parse().expect("ip")).collect()
    }

    fn section(public: &[&str], internal: &[&str]) -> NetSectionConfig {
        NetSectionConfig {
            public_nameservers: ips(public),
            internal_nameservers: ips(internal),
        }
    }

    /// Each intent forwards where §11 G-9 says, and only `viewer_private`
    /// keeps no answers (R3-213). Every intent's resolver is its own.
    #[tokio::test]
    async fn each_intent_has_its_own_resolver() {
        let ns = section(&["192.0.2.53"], &["10.0.0.53"]);
        let public = lookup_for(Intent::Public, &ns).expect("public");
        let internal = lookup_for(Intent::Internal, &ns).expect("internal");
        let private = lookup_for(Intent::ViewerPrivate, &ns).expect("viewer_private");
        assert_eq!(private.cache_size(), 0);
        assert!(public.cache_size() > 0 && internal.cache_size() > 0);
        let config = NetConfig::new("appview.example.com", "https://plc.directory");
        let transports = Transports::new(&config, &ns).expect("transports");
        for intent in [Intent::Public, Intent::Internal, Intent::ViewerPrivate] {
            assert_eq!(transports.get(intent).intent(), intent);
            assert_eq!(transports.get(intent).client().intent(), intent);
        }
        assert_eq!(transports.public().intent(), Intent::Public);
        assert_eq!(transports.internal().intent(), Intent::Internal);
    }

    /// `for_request` follows the request's mode (design-v4 §8.1): an
    /// ordinary request is public, a viewer-private one is never public,
    /// and private-context hydration is internal.
    #[test]
    fn the_request_mode_picks_the_transport() {
        use crate::xrpc::pools::Requester;

        let t = Transports::loopback_for_tests();
        let public = Requester::test();
        assert_eq!(t.for_request(&public).intent(), Intent::Public);
        let private = public.viewer_private("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");
        assert_eq!(t.for_request(&private).intent(), Intent::ViewerPrivate);
        let context = public.private_context("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");
        assert_eq!(t.for_request(&context).intent(), Intent::Internal);
    }

    #[test]
    fn nameserver_lists_are_checked() {
        let found = |public: &[&str], internal: &[&str]| {
            let mut out = Vec::new();
            problems(&section(public, internal), &mut out);
            out
        };
        assert!(found(&[], &[]).is_empty());
        assert!(found(&["1.1.1.1", "2606:4700:4700::1111"], &["10.0.0.53"]).is_empty());
        for bad in ["0.0.0.0", "::", "224.0.0.1", "ff02::1", "255.255.255.255"] {
            let problems = found(&[bad], &[]);
            assert!(problems.len() == 1 && problems[0].contains("cannot be a nameserver"), "{bad}: {problems:?}");
        }
        let twice = found(&[], &["10.0.0.53", "10.0.0.53"]);
        assert!(twice.len() == 1 && twice[0].starts_with("net.internal_nameservers") && twice[0].contains("twice"), "{twice:?}");
        // A file naming bad addresses fails to parse, and the defaults are
        // empty lists.
        assert!(crate::config::FileConfig::parse("[net]\npublic_nameservers = [\"not an ip\"]\n").is_err());
        assert!(crate::config::FileConfig::parse("[net]\npublic_nameservers = [\"1.1.1.1:53\"]\n").is_err());
        assert_eq!(crate::config::FileConfig::default().net, NetSectionConfig::default());
    }

    /// design-v4 §11 G-9: the startup warnings.
    #[test]
    fn startup_warnings_name_an_internal_resolver_others_can_query() {
        let warned = |public: &[&str], internal: &[&str]| warnings(&section(public, internal));
        let unset = warned(&[], &[]);
        assert!(unset.len() == 1 && unset[0].contains("is empty"), "{unset:?}");
        let shared = warned(&["10.0.0.53"], &["10.0.0.53"]);
        assert!(shared.len() == 1 && shared[0].contains("equals"), "{shared:?}");
        let docker = warned(&[], &["127.0.0.11"]);
        assert!(docker.len() == 1 && docker[0].contains("Docker"), "{docker:?}");
        let public = warned(&[], &["8.8.8.8"]);
        assert!(public.len() == 1 && public[0].contains("not a private address"), "{public:?}");
        for private in ["10.0.0.53", "172.30.0.53", "192.168.1.1", "127.0.0.1", "::1", "fd00::53"] {
            assert!(warned(&["1.1.1.1"], &[private]).is_empty(), "{private}");
        }
    }
}
