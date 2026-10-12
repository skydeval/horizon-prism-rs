//! Rate limiting (design-v2 §7.4, design-v3 §10): `governor` keyed buckets.
//! Exceeding one returns `429 {"error":"RateLimitExceeded"}` with a
//! `ratelimit-reset` header (epoch seconds).
//!
//! Every number is operator config, `[rate_limits.<class>]` with `per_sec`
//! and `burst`. The defaults:
//!
//! | Class | Key | Rate / burst |
//! |---|---|---|
//! | `anon_read` | IP | 30/s, 60 |
//! | `auth_read` | DID | 60/s, 120 |
//! | `search_anon` | IP | 10/s, 20 |
//! | `search_auth` | DID | 30/s, 60 |
//! | `write` | DID | 5/s, 20 |
//! | `private` | DID | 10/s, 20 |
//! | `thread_lens` | DID | 2/s, 10 |
//! | `index_request` | DID | 0.5/s, 30 |
//! | `resolve` | endpoint and IP | 10/s, 20 |
//! | `global` | IP | 100/s, 200 (backstop) |
//!
//! Classes form **families**: a request is charged to its family's IP-keyed
//! member when anonymous and to its DID-keyed member when authenticated.
//! `rate_limits.endpoint_class.<nsid>` moves one served endpoint to another
//! family.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use governor::clock::{Clock, DefaultClock};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use tokio::task::JoinHandle;

use super::backstop::Backstop;
use super::pools::ChargeKey;
use crate::config::{RateClassConfig, RateLimitsConfig};

/// A bucket class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    AnonRead,
    AuthRead,
    SearchAnon,
    SearchAuth,
    Write,
    Private,
    ThreadLens,
    IndexRequest,
    Resolve,
    Global,
    /// A proxied service's calls from a ready member (design-v3 §6.7.6.2).
    ServiceProxyMember,
    /// A proxied service's calls from everyone else, by IP key.
    ServiceProxyIp,
}

impl Class {
    const ALL: [Self; 12] = [
        Self::AnonRead,
        Self::AuthRead,
        Self::SearchAnon,
        Self::SearchAuth,
        Self::Write,
        Self::Private,
        Self::ThreadLens,
        Self::IndexRequest,
        Self::Resolve,
        Self::Global,
        Self::ServiceProxyMember,
        Self::ServiceProxyIp,
    ];

    fn config(self, cfg: &RateLimitsConfig) -> RateClassConfig {
        match self {
            Self::AnonRead => cfg.anon_read,
            Self::AuthRead => cfg.auth_read,
            Self::SearchAnon => cfg.search_anon,
            Self::SearchAuth => cfg.search_auth,
            Self::Write => cfg.write,
            Self::Private => cfg.private,
            Self::ThreadLens => cfg.thread_lens,
            Self::IndexRequest => cfg.index_request,
            Self::Resolve => cfg.resolve,
            Self::Global => cfg.global,
            Self::ServiceProxyMember => cfg.service_proxy_member,
            Self::ServiceProxyIp => cfg.service_proxy_ip,
        }
    }
}

/// A class family (§10): what `rate_limits.endpoint_class` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// `anon_read` / `auth_read`.
    Read,
    /// `search_anon` / `search_auth`.
    Search,
    Write,
    Private,
    ThreadLens,
    IndexRequest,
    /// IP-keyed, with one bucket per endpoint.
    Resolve,
}

impl Family {
    /// Parse a family name from config.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "read" => Self::Read,
            "search" => Self::Search,
            "write" => Self::Write,
            "private" => Self::Private,
            "thread_lens" => Self::ThreadLens,
            "index_request" => Self::IndexRequest,
            "resolve" => Self::Resolve,
            _ => return None,
        })
    }

    /// Whether the family has an IP-keyed member, so that an anonymous call
    /// can be charged to it.
    #[must_use]
    pub fn serves_anonymous(self) -> bool {
        matches!(self, Self::Read | Self::Search | Self::Resolve)
    }
}

/// A served XRPC method: its family and whether it accepts anonymous calls.
/// Override keys may name only these; deferrals, the router fallback,
/// `_health` and `did.json` are fixed at `anon_read`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Served {
    pub nsid: &'static str,
    pub family: Family,
    pub anonymous: bool,
}

const fn served(nsid: &'static str, family: Family, anonymous: bool) -> Served {
    Served { nsid, family, anonymous }
}

/// Every XRPC method this build serves.
pub const SERVED: &[Served] = &[
    served("app.bsky.actor.getProfile", Family::Read, true),
    served("app.bsky.actor.getProfiles", Family::Read, true),
    served("app.bsky.actor.searchActors", Family::Search, true),
    served("app.bsky.feed.getPosts", Family::Read, true),
    served("app.bsky.feed.getPostThread", Family::Read, true),
    served("app.bsky.unspecced.getPostThreadV2", Family::Read, true),
    served("app.bsky.unspecced.getPostThreadOtherV2", Family::Read, true),
    served("app.bsky.feed.getAuthorFeed", Family::Read, true),
    served("app.bsky.feed.getLikes", Family::Read, true),
    served("app.bsky.feed.getRepostedBy", Family::Read, true),
    served("app.bsky.feed.getQuotes", Family::Read, true),
    served("app.bsky.feed.getTimeline", Family::Read, false),
    served("app.bsky.feed.getListFeed", Family::Read, true),
    served("app.bsky.graph.getFollows", Family::Read, true),
    served("app.bsky.graph.getFollowers", Family::Read, true),
    served("app.bsky.graph.getList", Family::Read, true),
    served("app.bsky.graph.getLists", Family::Read, true),
    served("app.bsky.graph.getKnownFollowers", Family::Read, false),
    served("app.bsky.graph.getRelationships", Family::Read, true),
    served("app.bsky.graph.getListsWithMembership", Family::Read, false),
    served("app.bsky.graph.getStarterPacksWithMembership", Family::Read, false),
    served("app.bsky.graph.getStarterPack", Family::Read, true),
    served("app.bsky.graph.getStarterPacks", Family::Read, true),
    served("app.bsky.graph.getActorStarterPacks", Family::Read, true),
    served("app.bsky.graph.getMutes", Family::Read, false),
    served("app.bsky.graph.muteActor", Family::Write, false),
    served("app.bsky.graph.unmuteActor", Family::Write, false),
    served("app.bsky.graph.muteThread", Family::Write, false),
    served("app.bsky.graph.unmuteThread", Family::Write, false),
    served("app.bsky.graph.getBlocks", Family::Read, false),
    served("app.bsky.notification.listNotifications", Family::Read, false),
    served("app.bsky.notification.getUnreadCount", Family::Read, false),
    served("app.bsky.notification.updateSeen", Family::Write, false),
    served("app.nearhorizon.private.getPosts", Family::Private, false),
    served("app.nearhorizon.private.getPostThread", Family::Private, false),
    served("app.nearhorizon.private.getAuthorFeed", Family::Private, false),
    served("app.nearhorizon.threadLens.getThread", Family::ThreadLens, false),
    served("app.nearhorizon.threadLens.resolve", Family::ThreadLens, false),
    served("app.nearhorizon.index.requestRecord", Family::IndexRequest, false),
    served("app.nearhorizon.constellation.resolveDid", Family::Resolve, true),
];

/// The smallest and largest accepted `per_sec` (§10).
pub const MIN_PER_SEC: f64 = 0.000_001;
pub const MAX_PER_SEC: f64 = 1_000_000.0;

/// The replenishment period of a validated rate: one token every period.
fn period(per_sec: f64) -> Duration {
    Duration::from_secs_f64(1.0 / per_sec)
}

/// Why a class's numbers are unusable (§10), if they are.
pub(crate) fn class_problem(c: RateClassConfig) -> Option<String> {
    if !c.per_sec.is_finite() {
        return Some("per_sec must be a finite number".to_string());
    }
    if !(MIN_PER_SEC..=MAX_PER_SEC).contains(&c.per_sec) {
        return Some(format!("per_sec = {} is outside {MIN_PER_SEC}..={MAX_PER_SEC}", c.per_sec));
    }
    if c.burst < 1 {
        return Some("burst must be at least 1".to_string());
    }
    // governor multiplies the period by burst - 1 without overflow checks.
    let period_ns = period(c.per_sec).as_nanos();
    if period_ns == 0 || period_ns.saturating_mul(u128::from(c.burst - 1)) > u128::from(u64::MAX) {
        return Some(format!("per_sec = {} with burst = {} overflows the bucket arithmetic", c.per_sec, c.burst));
    }
    None
}

/// Every problem in `[rate_limits]` (§10).
pub fn problems(cfg: &RateLimitsConfig, out: &mut Vec<String>) {
    for (name, class) in cfg.classes() {
        if let Some(p) = class_problem(class) {
            out.push(format!("rate_limits.{name}: {p}"));
        }
    }
    for (nsid, family) in &cfg.endpoint_class {
        let Some(served) = SERVED.iter().find(|s| s.nsid == nsid) else {
            out.push(format!(
                "rate_limits.endpoint_class: {nsid:?} is not an XRPC method this AppView serves (deferrals, the fallback, _health and did.json are fixed at anon_read)"
            ));
            continue;
        };
        match Family::parse(family) {
            None => out.push(format!("rate_limits.endpoint_class[{nsid}]: {family:?} is not a class family")),
            Some(f) if served.anonymous && !f.serves_anonymous() => out.push(format!(
                "rate_limits.endpoint_class[{nsid}]: {family:?} is keyed by DID, and this endpoint accepts anonymous calls"
            )),
            Some(_) => {}
        }
    }
}

fn limiter(class: RateClassConfig) -> DefaultKeyedRateLimiter<String> {
    let q = Quota::with_period(period(class.per_sec))
        .unwrap_or_else(|| Quota::per_second(NonZeroU32::MIN))
        .allow_burst(NonZeroU32::new(class.burst).unwrap_or(NonZeroU32::MIN));
    RateLimiter::keyed(q)
}

/// A rejection: the epoch second at which a retry may succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limited {
    pub reset_epoch_secs: u64,
}

/// All buckets. Cheap to clone.
#[derive(Clone)]
pub struct RateLimits {
    buckets: Arc<HashMap<Class, DefaultKeyedRateLimiter<String>>>,
    /// The family of each served method: the compiled default, or the
    /// operator's `endpoint_class` override.
    families: Arc<HashMap<String, Family>>,
    /// The pre-auth backstop (design-v3 §II-8.3); `None` charges every
    /// request to its IP key (tests).
    backstop: Option<Backstop>,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimits {
    /// The default rates.
    #[must_use]
    pub fn new() -> Self {
        Self::from_config(&RateLimitsConfig::default())
    }

    /// Buckets and families from a validated `[rate_limits]`.
    #[must_use]
    pub fn from_config(cfg: &RateLimitsConfig) -> Self {
        let buckets = Class::ALL.into_iter().map(|c| (c, limiter(c.config(cfg)))).collect();
        let mut families: HashMap<String, Family> = SERVED.iter().map(|s| (s.nsid.to_string(), s.family)).collect();
        for (nsid, family) in &cfg.endpoint_class {
            if let Some(f) = Family::parse(family) {
                families.insert(nsid.clone(), f);
            }
        }
        Self {
            buckets: Arc::new(buckets),
            families: Arc::new(families),
            backstop: None,
        }
    }

    /// Select each request's charge key with `backstop`.
    #[must_use]
    pub fn with_backstop(mut self, backstop: Backstop) -> Self {
        self.backstop = Some(backstop);
        self
    }

    #[must_use]
    pub fn backstop(&self) -> Option<&Backstop> {
        self.backstop.as_ref()
    }

    /// Take one token for `key` in `class`.
    ///
    /// # Errors
    ///
    /// [`Limited`] when the bucket is empty.
    pub fn check(&self, class: Class, key: &str) -> Result<(), Limited> {
        let Some(bucket) = self.buckets.get(&class) else {
            return Ok(());
        };
        bucket.check_key(&key.to_string()).map_err(|not_until| {
            let wait = not_until.wait_time_from(DefaultClock::default().now());
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
            Limited {
                reset_epoch_secs: (now + wait + Duration::from_millis(999)).as_secs(),
            }
        })
    }

    /// Per-IP check (anonymous reads, global backstop).
    ///
    /// # Errors
    ///
    /// [`Limited`] when the bucket is empty.
    pub fn check_ip(&self, class: Class, ip: IpAddr) -> Result<(), Limited> {
        self.check(class, &ip_key(ip).to_string())
    }

    /// The family a method is charged to: its entry in the served table,
    /// which carries any `endpoint_class` override, else `default`.
    #[must_use]
    pub fn family(&self, method: &str, default: Family) -> Family {
        self.families.get(method).copied().unwrap_or(default)
    }

    /// Charge one request of `method` to its family (§10 "Class families"):
    /// the DID-keyed member for an authenticated caller, the IP-keyed one
    /// for an anonymous caller. `resolve` is keyed by IP and has one bucket
    /// per endpoint.
    ///
    /// # Errors
    ///
    /// [`Limited`] when the bucket is empty.
    pub fn charge(&self, method: &str, default: Family, did: Option<&str>, ip: IpAddr) -> Result<(), Limited> {
        match (self.family(method, default), did) {
            (Family::Resolve, _) => self.check(Class::Resolve, &format!("{method}|{}", ip_key(ip))),
            (Family::Read, Some(did)) => self.check(Class::AuthRead, did),
            (Family::Search, Some(did)) => self.check(Class::SearchAuth, did),
            (Family::Search, None) => self.check_ip(Class::SearchAnon, ip),
            (Family::Write, Some(did)) => self.check(Class::Write, did),
            (Family::Private, Some(did)) => self.check(Class::Private, did),
            (Family::ThreadLens, Some(did)) => self.check(Class::ThreadLens, did),
            (Family::IndexRequest, Some(did)) => self.check(Class::IndexRequest, did),
            // An anonymous read. A DID-keyed family never reaches here with
            // no caller: validation keeps it off endpoints that accept
            // anonymous calls, and its handlers authenticate first.
            (_, None) => self.check_ip(Class::AnonRead, ip),
        }
    }

    /// Charge one call to proxied service `service` (design-v3 §6.7.6.2):
    /// `service_proxy_member` for a ready member, `service_proxy_ip` for
    /// everyone else, one bucket per service and key. `key` is the key the
    /// backstop selected, never a DID verified later.
    ///
    /// # Errors
    ///
    /// [`Limited`] when the bucket is empty.
    pub fn charge_service(&self, service: &str, key: &ChargeKey) -> Result<(), Limited> {
        match key {
            ChargeKey::Member(did) => self.check(Class::ServiceProxyMember, &format!("{service}|{did}")),
            ChargeKey::Ip(ip) => self.check(Class::ServiceProxyIp, &format!("{service}|{ip}")),
        }
    }

    /// Drop idle keys every minute so memory stays bounded.
    #[must_use]
    pub fn spawn_housekeeping(&self) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                for bucket in this.buckets.values() {
                    bucket.retain_recent();
                    bucket.shrink_to_fit();
                }
            }
        })
    }
}

/// The client IP (§7.4): the TCP peer, or the last `X-Forwarded-For`
/// element when the peer is the trusted proxy (Caddy's fixed address).
#[must_use]
pub fn client_ip(peer: IpAddr, xff: Option<&str>, trusted_proxy: IpAddr) -> IpAddr {
    if peer != trusted_proxy {
        return peer;
    }
    xff.and_then(|v| v.rsplit(',').next())
        .and_then(|last| last.trim().parse().ok())
        .unwrap_or(peer)
}

/// The key an address is charged under (design-v3 §II-8.3 item 3): an IPv4
/// address, or an IPv6 `/64`, since one subscriber holds a whole `/64`.
#[must_use]
pub fn ip_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or_else(
            || IpAddr::V6(Ipv6Addr::from(u128::from(v6) & !u128::from(u64::MAX))),
            IpAddr::V4,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn per_ip_and_per_did_buckets_are_independent() {
        let limits = RateLimits::new();
        let ip: IpAddr = "203.0.113.7".parse().expect("ip");
        let other: IpAddr = "203.0.113.8".parse().expect("ip");
        // Anonymous reads: burst 60 per IP, then limited.
        for _ in 0..60 {
            limits.check_ip(Class::AnonRead, ip).expect("within burst");
        }
        let limited = limits.check_ip(Class::AnonRead, ip).expect_err("limited");
        assert!(limited.reset_epoch_secs >= SystemTime::now().duration_since(UNIX_EPOCH).expect("time").as_secs());
        limits.check_ip(Class::AnonRead, other).expect("another IP has its own bucket");

        // Per-DID classes do not share the IP buckets.
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        for _ in 0..20 {
            limits.check(Class::Write, did).expect("write burst 20");
        }
        assert!(limits.check(Class::Write, did).is_err());
        for _ in 0..120 {
            limits.check(Class::AuthRead, did).expect("read burst 120");
        }
        assert!(limits.check(Class::AuthRead, did).is_err());
        limits.check(Class::AuthRead, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").expect("another DID");
    }

    #[test]
    fn xff_only_from_the_trusted_proxy() {
        let caddy: IpAddr = "172.30.0.10".parse().expect("ip");
        let peer: IpAddr = "172.30.0.1".parse().expect("ip");
        assert_eq!(client_ip(peer, Some("1.2.3.4"), caddy), peer);
        assert_eq!(client_ip(caddy, Some("9.9.9.9, 1.2.3.4"), caddy), "1.2.3.4".parse::<IpAddr>().expect("ip"));
        assert_eq!(client_ip(caddy, Some("garbage"), caddy), caddy);
        assert_eq!(client_ip(caddy, None, caddy), caddy);
    }

    #[test]
    fn ipv6_is_keyed_by_its_64() {
        let key = |s: &str| ip_key(s.parse().expect("ip"));
        assert_eq!(key("203.0.113.7"), key("203.0.113.7"));
        assert_ne!(key("203.0.113.7"), key("203.0.113.8"));
        assert_eq!(key("2001:db8:1:2:aaaa::1"), "2001:db8:1:2::".parse::<IpAddr>().expect("ip"));
        assert_eq!(key("2001:db8:1:2:aaaa::1"), key("2001:db8:1:2:ffff:ffff:ffff:ffff"));
        assert_ne!(key("2001:db8:1:2::1"), key("2001:db8:1:3::1"));
        assert_eq!(key("::ffff:203.0.113.7"), key("203.0.113.7"));

        // One /64 shares an anonymous bucket.
        let limits = RateLimits::new();
        for i in 0..60 {
            let ip: IpAddr = format!("2001:db8:1:2::{i:x}").parse().expect("ip");
            limits.check_ip(Class::AnonRead, ip).expect("within burst");
        }
        assert!(limits.check_ip(Class::AnonRead, "2001:db8:1:2::ffff".parse().expect("ip")).is_err());
        limits.check_ip(Class::AnonRead, "2001:db8:1:3::1".parse().expect("ip")).expect("another /64");
    }

    fn check(toml: &str) -> Vec<String> {
        let cfg = crate::config::FileConfig::parse(toml).expect("parse");
        let mut out = Vec::new();
        problems(&cfg.rate_limits, &mut out);
        out
    }

    #[test]
    fn served_table_is_well_formed() {
        let unique: BTreeSet<&str> = SERVED.iter().map(|s| s.nsid).collect();
        assert_eq!(unique.len(), SERVED.len());
        for s in SERVED {
            assert!(kryphocron_lexicons::Nsid::new(s.nsid).is_ok(), "{}", s.nsid);
            // No served method is also a deferral (§6.6).
            assert!(crate::honest_501::find(s.nsid).is_none(), "{}", s.nsid);
            // An endpoint that accepts anonymous calls is in a family that
            // can charge one.
            assert!(!s.anonymous || s.family.serves_anonymous(), "{}", s.nsid);
        }
    }

    #[test]
    fn class_numbers_are_validated() {
        assert!(check("").is_empty());
        assert!(check("[rate_limits.write]\nper_sec = 0.5\nburst = 3\n").is_empty());
        let one = |toml: &str, needle: &str| {
            let out = check(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        };
        one("[rate_limits.write]\nper_sec = nan\nburst = 3\n", "rate_limits.write: per_sec must be a finite number");
        one("[rate_limits.global]\nper_sec = inf\nburst = 3\n", "finite");
        one("[rate_limits.write]\nper_sec = 0.0\nburst = 3\n", "outside");
        one("[rate_limits.write]\nper_sec = 0.0000001\nburst = 3\n", "outside");
        one("[rate_limits.write]\nper_sec = 2000000.0\nburst = 3\n", "outside");
        one("[rate_limits.write]\nper_sec = -1.0\nburst = 3\n", "outside");
        one("[rate_limits.write]\nper_sec = 5.0\nburst = 0\n", "burst must be at least 1");
        one("[rate_limits.write]\nper_sec = 0.000001\nburst = 4000000000\n", "overflows");
        // A class table gives both keys.
        assert!(crate::config::FileConfig::parse("[rate_limits.write]\nper_sec = 5.0\n").is_err());
    }

    #[test]
    fn endpoint_overrides_are_validated() {
        let one = |toml: &str, needle: &str| {
            let out = check(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        };
        assert!(check("[rate_limits.endpoint_class]\n\"app.bsky.feed.getTimeline\" = \"search\"\n").is_empty());
        assert!(check("[rate_limits.endpoint_class]\n\"app.bsky.feed.getTimeline\" = \"write\"\n").is_empty());
        one("[rate_limits.endpoint_class]\n\"app.bsky.feed.getTimeline\" = \"fast\"\n", "is not a class family");
        one("[rate_limits.endpoint_class]\n\"app.example.nope\" = \"read\"\n", "not an XRPC method");
        // Deferrals are fixed at anon_read.
        one("[rate_limits.endpoint_class]\n\"app.bsky.feed.getFeed\" = \"read\"\n", "not an XRPC method");
        // A DID-keyed family cannot take an endpoint that allows anonymous calls.
        one("[rate_limits.endpoint_class]\n\"app.bsky.feed.getPosts\" = \"write\"\n", "keyed by DID");
        one("[rate_limits.endpoint_class]\n\"app.bsky.actor.getProfile\" = \"private\"\n", "keyed by DID");
        assert!(check("[rate_limits.endpoint_class]\n\"app.bsky.feed.getPosts\" = \"resolve\"\n").is_empty());
    }

    #[test]
    fn charging_follows_the_family_and_overrides() {
        let ip: IpAddr = "203.0.113.7".parse().expect("ip");
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let cfg = crate::config::FileConfig::parse(concat!(
            "[rate_limits.search_auth]\nper_sec = 1.0\nburst = 2\n",
            "[rate_limits.auth_read]\nper_sec = 1.0\nburst = 5\n",
            "[rate_limits.resolve]\nper_sec = 1.0\nburst = 1\n",
            "[rate_limits.endpoint_class]\n\"app.bsky.feed.getTimeline\" = \"search\"\n",
        ))
        .expect("parse");
        let limits = RateLimits::from_config(&cfg.rate_limits);

        // B-64: the overridden endpoint exhausts search_auth, not auth_read.
        let timeline = "app.bsky.feed.getTimeline";
        for _ in 0..2 {
            limits.charge(timeline, Family::Read, Some(did), ip).expect("search_auth burst 2");
        }
        assert!(limits.charge(timeline, Family::Read, Some(did), ip).is_err());
        // auth_read is untouched: another read endpoint still has its burst.
        for _ in 0..5 {
            limits.charge("app.bsky.feed.getPosts", Family::Read, Some(did), ip).expect("auth_read burst 5");
        }
        assert!(limits.charge("app.bsky.feed.getPosts", Family::Read, Some(did), ip).is_err());
        // The same endpoint, anonymous, is charged by IP to anon_read.
        limits.charge("app.bsky.feed.getPosts", Family::Read, None, ip).expect("anon_read");

        // Search by default; an unknown method takes the caller's default.
        assert_eq!(limits.family("app.bsky.actor.searchActors", Family::Read), Family::Search);
        assert_eq!(limits.family("app.example.unknown", Family::Write), Family::Write);

        // resolve: one bucket per endpoint and IP.
        let resolve = "app.nearhorizon.constellation.resolveDid";
        limits.charge(resolve, Family::Resolve, None, ip).expect("burst 1");
        assert!(limits.charge(resolve, Family::Resolve, None, ip).is_err());
        limits.charge("app.example.other", Family::Resolve, None, ip).expect("another endpoint's bucket");
    }
}
