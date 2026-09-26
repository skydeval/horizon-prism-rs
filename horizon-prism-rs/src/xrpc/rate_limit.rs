//! Rate limiting (design-v2 §7.4): `governor` keyed buckets. Exceeding one
//! returns `429 {"error":"RateLimitExceeded"}` with a `ratelimit-reset`
//! header (epoch seconds).
//!
//! | Class | Key | Rate / burst |
//! |---|---|---|
//! | anonymous reads (`app.bsky.*`) | IP | 30/s, 60 |
//! | authenticated reads (`app.bsky.*`) | DID | 60/s, 120 |
//! | writes to AppView state (`muteActor`, `unmuteActor`, `updateSeen`) | DID | 5/s, 20 |
//! | `app.nearhorizon.private.*` | DID | 10/s, 20 |
//! | any request | IP | 100/s, 200 (global backstop) |
//!
//! The item-7 classes (`threadLens.*`, `requestRecord`, `resolveDid`) are
//! added with their endpoints.

use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use governor::clock::{Clock, DefaultClock};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use tokio::task::JoinHandle;

/// A bucket class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    AnonRead,
    AuthRead,
    Write,
    Private,
    Global,
}

/// Rate and burst per class (§7.4).
#[must_use]
pub fn quota(class: Class) -> (u32, u32) {
    match class {
        Class::AnonRead => (30, 60),
        Class::AuthRead => (60, 120),
        Class::Write => (5, 20),
        Class::Private => (10, 20),
        Class::Global => (100, 200),
    }
}

fn limiter(class: Class) -> DefaultKeyedRateLimiter<String> {
    let (rate, burst) = quota(class);
    let q = Quota::per_second(NonZeroU32::new(rate).unwrap_or(NonZeroU32::MIN))
        .allow_burst(NonZeroU32::new(burst).unwrap_or(NonZeroU32::MIN));
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
    anon: Arc<DefaultKeyedRateLimiter<String>>,
    auth: Arc<DefaultKeyedRateLimiter<String>>,
    write: Arc<DefaultKeyedRateLimiter<String>>,
    private: Arc<DefaultKeyedRateLimiter<String>>,
    global: Arc<DefaultKeyedRateLimiter<String>>,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimits {
    #[must_use]
    pub fn new() -> Self {
        Self {
            anon: Arc::new(limiter(Class::AnonRead)),
            auth: Arc::new(limiter(Class::AuthRead)),
            write: Arc::new(limiter(Class::Write)),
            private: Arc::new(limiter(Class::Private)),
            global: Arc::new(limiter(Class::Global)),
        }
    }

    fn bucket(&self, class: Class) -> &DefaultKeyedRateLimiter<String> {
        match class {
            Class::AnonRead => &self.anon,
            Class::AuthRead => &self.auth,
            Class::Write => &self.write,
            Class::Private => &self.private,
            Class::Global => &self.global,
        }
    }

    /// Take one token for `key` in `class`.
    ///
    /// # Errors
    ///
    /// [`Limited`] when the bucket is empty.
    pub fn check(&self, class: Class, key: &str) -> Result<(), Limited> {
        self.bucket(class).check_key(&key.to_string()).map_err(|not_until| {
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
        self.check(class, &ip.to_string())
    }

    /// Drop idle keys every minute so memory stays bounded.
    #[must_use]
    pub fn spawn_housekeeping(&self) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                for class in [Class::AnonRead, Class::AuthRead, Class::Write, Class::Private, Class::Global] {
                    let b = this.bucket(class);
                    b.retain_recent();
                    b.shrink_to_fit();
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

#[cfg(test)]
mod tests {
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
}
