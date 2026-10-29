//! The Phase B stand-in exception to the outbound guard. Test-only: this
//! module exists only under the `standin-hosts` feature, which does not
//! compile without debug assertions (`lib.rs`).
//!
//! A `[standin]` section lists origins `http://<private IP literal>:<port>`,
//! each with the intents whose clients may reach it. On a request's first
//! hop, and only there, [`super::client`]'s URL policy lets a listed origin
//! through. The resolver is untouched, redirects never use the exception,
//! and `viewer_private` can never be listed.

use std::collections::BTreeSet;
use std::net::IpAddr;

use url::{Host, Url};

use super::client::Intent;
use crate::config::{StandinConfig, StandinIntent};

/// The marker a feature binary carries: printed on stderr and logged at
/// every `serve` start, and what `docker/Dockerfile` greps a deploy binary
/// for. The literal appears nowhere else in `src/`.
pub const MARKER: &str = "HPRS_STANDIN_HOSTS_BUILD";

/// At most this many listed origins (§3.3 rule 7).
pub const MAX_HOSTS: usize = 8;

/// One listed origin, as parsed values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandinOrigin {
    pub ip: IpAddr,
    pub port: u16,
}

impl StandinOrigin {
    /// The origin `url` names, when it is `http` with an IP-literal host.
    #[must_use]
    pub fn of_url(url: &Url) -> Option<Self> {
        if url.scheme() != "http" {
            return None;
        }
        let ip = match url.host()? {
            Host::Ipv4(v4) => IpAddr::V4(v4),
            Host::Ipv6(v6) => IpAddr::V6(v6),
            Host::Domain(_) => return None,
        };
        Some(Self {
            ip,
            port: url.port_or_known_default()?,
        })
    }
}

/// The origins one client may reach on a first hop.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Standins(pub Vec<StandinOrigin>);

impl Standins {
    /// The origins of `listed` whose intents include `intent`. The
    /// `viewer_private` client always gets none.
    #[must_use]
    pub fn for_intent(listed: &[(StandinOrigin, Vec<StandinIntent>)], intent: Intent) -> Self {
        let wanted = match intent {
            Intent::Public => Some(StandinIntent::Public),
            Intent::Internal => Some(StandinIntent::Internal),
            Intent::ViewerPrivate => None,
        };
        let Some(wanted) = wanted else {
            return Self::default();
        };
        Self(
            listed
                .iter()
                .filter(|(_, intents)| intents.contains(&wanted))
                .map(|(origin, _)| *origin)
                .collect(),
        )
    }

    /// Whether `url` is `http`, on an IP literal equal to a listed one, at
    /// its port. Compared as parsed values, so another spelling of the same
    /// address is the same address.
    #[must_use]
    pub fn admits(&self, url: &Url) -> bool {
        StandinOrigin::of_url(url).is_some_and(|origin| self.0.contains(&origin))
    }
}

/// The listed origins with their intents, for `NetConfig`. Validation has
/// refused every entry this would drop.
#[must_use]
pub fn listed(cfg: &StandinConfig) -> Vec<(StandinOrigin, Vec<StandinIntent>)> {
    cfg.hosts
        .iter()
        .filter_map(|h| {
            let url = Url::parse(&h.origin).ok()?;
            Some((StandinOrigin::of_url(&url)?, h.intents.clone()))
        })
        .collect()
}

/// Whether `value`'s origin is listed for every one of `intents` (§3.3
/// rule 8). An empty `intents` is never listed.
#[must_use]
pub fn listed_for(cfg: &StandinConfig, value: &str, intents: &[Intent]) -> bool {
    let Some(origin) = Url::parse(value).ok().as_ref().and_then(StandinOrigin::of_url) else {
        return false;
    };
    let entries = listed(cfg);
    !intents.is_empty()
        && intents.iter().all(|intent| Standins::for_intent(&entries, *intent).0.contains(&origin))
}

/// Whether `ip` is an address a stand-in may be listed at (§3.3 rule 5):
/// RFC 1918 or ULA, never `.0`, `.1` or `.255`, never an IPv6 interface
/// id of 0 or 1.
#[must_use]
pub fn listable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let net = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
                .iter()
                .filter_map(|n| n.parse::<ipnet::Ipv4Net>().ok())
                .any(|n| n.contains(&v4));
            net && !matches!(v4.octets()[3], 0 | 1 | 255)
        }
        IpAddr::V6(v6) => {
            let ula = "fc00::/7".parse::<ipnet::Ipv6Net>().is_ok_and(|n| n.contains(&v6));
            let interface = u128::from(v6) & u128::from(u64::MAX);
            ula && interface > 1
        }
    }
}

/// Every problem in `[standin]` (§3.3 rules 2 – 7). An empty section has
/// none: `serve` refuses it ([`required`]), not validation.
pub fn problems(cfg: &StandinConfig, out: &mut Vec<String>) {
    if cfg.hosts.len() > MAX_HOSTS {
        out.push(format!("standin.hosts: {} entries, at most {MAX_HOSTS}", cfg.hosts.len()));
    }
    let mut seen = BTreeSet::new();
    for host in &cfg.hosts {
        let origin = &host.origin;
        if !seen.insert(origin.as_str()) {
            out.push(format!("standin.hosts: {origin:?} is listed twice"));
        }
        if let Err(e) = origin_problem(origin) {
            out.push(format!("standin.hosts: {origin:?} {e}"));
        }
        if host.intents.is_empty() {
            out.push(format!("standin.hosts: {origin:?} lists no intents"));
        }
        let distinct: std::collections::HashSet<_> = host.intents.iter().collect();
        if distinct.len() != host.intents.len() {
            out.push(format!("standin.hosts: {origin:?} repeats an intent"));
        }
    }
}

/// Rules 2 – 5 for one origin.
fn origin_problem(origin: &str) -> Result<(), String> {
    let url = Url::parse(origin).map_err(|e| format!("is not a URL: {e}"))?;
    if url.origin().ascii_serialization() != origin || url.port().is_none() {
        return Err("must be exactly http://<IP>:<port>, with the port written and nothing after it".to_string());
    }
    if url.scheme() != "http" {
        return Err("must be http: no stand-in certificate is trusted".to_string());
    }
    let Some(found) = StandinOrigin::of_url(&url) else {
        return Err("must name an IP literal, not a hostname".to_string());
    };
    if !listable(found.ip) {
        return Err("is not a listable private address (10/8, 172.16/12, 192.168/16 or fc00::/7, never .0, .1, .255 or interface id 0 or 1)".to_string());
    }
    Ok(())
}

/// `serve`'s rule 1 (§3.3): a feature build refuses to start without at
/// least one listed origin.
///
/// # Errors
///
/// The refusal's text.
pub fn required(cfg: &StandinConfig) -> Result<(), String> {
    if cfg.hosts.is_empty() {
        Err("a standin-hosts build needs at least one [[standin.hosts]] entry".to_string())
    } else {
        Ok(())
    }
}
