//! Shared upstream buckets, pools and per-requester fair shares (design-v3
//! §II-8.3).
//!
//! Every upstream a public request can reach has one [`SharedBucket`], so
//! public traffic never exceeds what that upstream tolerates.
//!
//! - **Pools.** A bucket is split into a member pool and a non-member pool
//!   (`constellation.nonmember_fraction` of its rate and burst). Ready
//!   members draw on the member pool first, then the non-member pool;
//!   everyone else draws on the non-member pool only.
//! - **Fair shares.** Inside a pool, a requester has a share of
//!   `constellation.requester_share` of the pool's rate and burst (burst at
//!   least 1). A call takes a share token, then a pool token; if the pool is
//!   empty the share token is not spent. So no single requester empties a
//!   pool.
//! - **State.** Pools are fixed and never evicted. Shares live in one LRU
//!   cache whose time-to-idle is `max(60 s, 2 × burst / rate)`, so an evicted
//!   share has always refilled.
//!
//! An empty share or pool is "no answer": the caller decides what that means
//! for its item. It is never a breaker record.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use moka::Expiry;
use moka::ops::compute::Op;
use moka::policy::EvictionPolicy;
use moka::sync::Cache;

use super::rate_limit::{class_problem, ip_key};
use crate::config::{FileConfig, RateClassConfig};

/// `requester_share_exhausted_total{target_kind, pool}`.
pub const METRIC_EXHAUSTED: &str = "requester_share_exhausted_total";
/// The shortest time-to-idle of a share.
const MIN_IDLE: Duration = Duration::from_secs(60);

/// The key a request is charged to (§II-8.3 "Key and evaluation order").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChargeKey {
    /// A ready member, by DID.
    Member(Arc<str>),
    /// Everyone else, by IP key: an IPv4 address or an IPv6 `/64`.
    Ip(IpAddr),
}

impl ChargeKey {
    #[must_use]
    pub fn member(did: &str) -> Self {
        Self::Member(Arc::from(did))
    }

    /// The IP key of `ip`.
    #[must_use]
    pub fn ip(ip: IpAddr) -> Self {
        Self::Ip(ip_key(ip))
    }
}

/// Which pool a token came from, or which one ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pool {
    Member,
    NonMember,
}

impl Pool {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::NonMember => "nonmember",
        }
    }
}

/// No token: the requester's share or its pool is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exhausted {
    /// The last pool tried.
    pub pool: Pool,
    /// How long until that pool or share has a token again.
    pub retry_after: Duration,
}

impl Exhausted {
    /// The epoch second for a `ratelimit-reset` header.
    #[must_use]
    pub fn reset_epoch_secs(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        (now + self.retry_after + Duration::from_millis(999)).as_secs()
    }
}

#[derive(Debug, Clone, Copy)]
struct Rate {
    per_sec: f64,
    burst: f64,
}

impl Rate {
    fn scaled(self, by: f64) -> Self {
        Self {
            per_sec: self.per_sec * by,
            burst: (self.burst * by).max(1.0),
        }
    }

    /// `max(60 s, 2 × burst / rate)`: an idle bucket has refilled by then.
    fn idle(self) -> Duration {
        Duration::try_from_secs_f64(2.0 * self.burst / self.per_sec).map_or(Duration::MAX, |d| d.max(MIN_IDLE))
    }
}

/// A token bucket that starts full.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn full(rate: Rate, now: Instant) -> Self {
        Self { tokens: rate.burst, at: now }
    }

    fn refill(&mut self, rate: Rate, now: Instant) {
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate.per_sec).min(rate.burst);
        self.at = now;
    }

    /// How long until one token is available.
    fn wait(&self, rate: Rate) -> Duration {
        Duration::try_from_secs_f64((1.0 - self.tokens).max(0.0) / rate.per_sec).unwrap_or(Duration::MAX)
    }

    fn take(&mut self, rate: Rate, now: Instant) -> Result<(), Duration> {
        self.refill(rate, now);
        if self.tokens < 1.0 {
            return Err(self.wait(rate));
        }
        self.tokens -= 1.0;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TargetId(u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShareKey {
    requester: ChargeKey,
    target: TargetId,
    pool: Pool,
}

#[derive(Debug, Clone, Copy)]
struct Share {
    bucket: Bucket,
    idle: Duration,
}

struct IdleExpiry;

impl Expiry<ShareKey, Share> for IdleExpiry {
    fn expire_after_create(&self, _: &ShareKey, share: &Share, _: Instant) -> Option<Duration> {
        Some(share.idle)
    }

    fn expire_after_update(&self, _: &ShareKey, share: &Share, _: Instant, _: Option<Duration>) -> Option<Duration> {
        Some(share.idle)
    }
}

/// The per-requester shares of every shared bucket: one LRU cache. Cheap to
/// clone.
#[derive(Clone)]
pub struct Shares {
    cache: Cache<ShareKey, Share>,
    next_target: Arc<AtomicU32>,
    /// `constellation.requester_share`.
    share: f64,
    /// `constellation.nonmember_fraction`.
    nonmember_fraction: f64,
}

impl Shares {
    /// An empty share cache from a validated config.
    #[must_use]
    pub fn from_config(cfg: &FileConfig) -> Self {
        let c = &cfg.constellation;
        Self::new(c.requester_share.0, c.nonmember_fraction.0, c.requester_share_max_entries)
    }

    #[must_use]
    pub fn new(share: f64, nonmember_fraction: f64, max_entries: u64) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(max_entries)
                .eviction_policy(EvictionPolicy::lru())
                .expire_after(IdleExpiry)
                .build(),
            next_target: Arc::new(AtomicU32::new(0)),
            share,
            nonmember_fraction,
        }
    }

    /// A shared bucket of `per_sec` and `burst` for one upstream.
    /// `target_kind` labels its exhaustion metric.
    #[must_use]
    pub fn bucket(&self, target_kind: &'static str, per_sec: f64, burst: u32) -> SharedBucket {
        self.bucket_with_share(target_kind, per_sec, burst, self.share)
    }

    /// Like [`Shares::bucket`], with this bucket's own requester share in
    /// place of `constellation.requester_share`.
    #[must_use]
    pub fn bucket_with_share(&self, target_kind: &'static str, per_sec: f64, burst: u32, share: f64) -> SharedBucket {
        let whole = Rate {
            per_sec,
            burst: f64::from(burst),
        };
        let now = Instant::now();
        let side = |pool: Pool, fraction: f64| {
            let rate = whole.scaled(fraction);
            let share_rate = rate.scaled(share);
            Side {
                pool,
                rate,
                share_rate,
                idle: share_rate.idle(),
                state: Mutex::new(Bucket::full(rate, now)),
            }
        };
        SharedBucket {
            inner: Arc::new(Inner {
                target: TargetId(self.next_target.fetch_add(1, Ordering::Relaxed)),
                target_kind,
                member: side(Pool::Member, 1.0 - self.nonmember_fraction),
                nonmember: side(Pool::NonMember, self.nonmember_fraction),
                shares: self.cache.clone(),
            }),
        }
    }

    /// Live share entries (tests, metrics).
    #[must_use]
    pub fn entries(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.entry_count()
    }
}

struct Side {
    pool: Pool,
    rate: Rate,
    share_rate: Rate,
    idle: Duration,
    state: Mutex<Bucket>,
}

impl Side {
    fn take(&self, now: Instant) -> Result<(), Duration> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take(self.rate, now)
    }
}

struct Inner {
    target: TargetId,
    target_kind: &'static str,
    member: Side,
    nonmember: Side,
    shares: Cache<ShareKey, Share>,
}

/// One upstream's request bucket, split into pools and shares. Cheap to
/// clone.
#[derive(Clone)]
pub struct SharedBucket {
    inner: Arc<Inner>,
}

impl SharedBucket {
    /// Take one token for `requester`. Never waits.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] when the requester's share or its pool is empty in
    /// every pool it may draw on.
    pub fn draw(&self, requester: &ChargeKey) -> Result<Pool, Exhausted> {
        self.draw_at(requester, Instant::now())
    }

    fn draw_at(&self, requester: &ChargeKey, now: Instant) -> Result<Pool, Exhausted> {
        let inner = &self.inner;
        let outcome = match requester {
            ChargeKey::Member(_) => self
                .draw_in(&inner.member, requester, now)
                .or_else(|_| self.draw_in(&inner.nonmember, requester, now)),
            ChargeKey::Ip(_) => self.draw_in(&inner.nonmember, requester, now),
        };
        if let Err(e) = &outcome {
            metrics::counter!(METRIC_EXHAUSTED, "target_kind" => inner.target_kind, "pool" => e.pool.label())
                .increment(1);
        }
        outcome
    }

    /// The share token, then the pool token, as one step on the share's
    /// entry: an empty pool leaves the share untouched.
    fn draw_in(&self, side: &Side, requester: &ChargeKey, now: Instant) -> Result<Pool, Exhausted> {
        let key = ShareKey {
            requester: requester.clone(),
            target: self.inner.target,
            pool: side.pool,
        };
        let mut outcome = Ok(side.pool);
        let _ = self.inner.shares.entry(key).and_compute_with(|entry| {
            let mut bucket = entry.map_or_else(|| Bucket::full(side.share_rate, now), |e| e.into_value().bucket);
            bucket.refill(side.share_rate, now);
            let spent = if bucket.tokens < 1.0 {
                Err(bucket.wait(side.share_rate))
            } else {
                side.take(now)
            };
            match spent {
                Ok(()) => {
                    bucket.tokens -= 1.0;
                    Op::Put(Share { bucket, idle: side.idle })
                }
                Err(retry_after) => {
                    outcome = Err(Exhausted {
                        pool: side.pool,
                        retry_after,
                    });
                    Op::Nop
                }
            }
        });
        outcome
    }
}

/// The time one XRPC request may spend on synchronous upstream work
/// (§II-8.3 "Limits"): every cascade of the request shares it.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(6);

/// The request-scoped handle: who is charged for the request's upstream
/// calls, the deadline they share, and whether any of them found its share
/// or pool empty. A handler creates one and passes it down; a path that is
/// given none (a background job, the CLI) cannot draw on a request bucket.
/// Cheap to clone; clones share the exhaustion record.
#[derive(Debug, Clone)]
pub struct Requester {
    key: ChargeKey,
    deadline: Instant,
    exhausted: Arc<Mutex<Option<Exhausted>>>,
}

impl Requester {
    /// A request starting now.
    #[must_use]
    pub fn new(key: ChargeKey) -> Self {
        Self {
            key,
            deadline: Instant::now() + REQUEST_DEADLINE,
            exhausted: Arc::new(Mutex::new(None)),
        }
    }

    /// A ready member's request when `member` is given, else `ip`'s.
    #[must_use]
    pub fn of(member: Option<&str>, ip: IpAddr) -> Self {
        Self::new(member.map_or_else(|| ChargeKey::ip(ip), ChargeKey::member))
    }

    /// A request from a fixed test address.
    #[cfg(test)]
    #[must_use]
    pub fn test() -> Self {
        Self::of(None, IpAddr::from([192, 0, 2, 1]))
    }

    #[must_use]
    pub fn key(&self) -> &ChargeKey {
        &self.key
    }

    /// When the request's synchronous upstream work must end.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Take one token of `bucket` for this request. `false` is "no answer"
    /// (§II-8.3 "Exhaustion"), and the request remembers it.
    #[must_use]
    pub fn draw(&self, bucket: &SharedBucket) -> bool {
        match bucket.draw(&self.key) {
            Ok(_) => true,
            Err(e) => {
                let mut seen = self.exhausted.lock().unwrap_or_else(PoisonError::into_inner);
                if seen.is_none_or(|s| s.retry_after < e.retry_after) {
                    *seen = Some(e);
                }
                false
            }
        }
    }

    /// The longest wait among the draws of this request that found nothing.
    #[must_use]
    pub fn exhausted(&self) -> Option<Exhausted> {
        *self.exhausted.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Every problem in the pool and shared-bucket settings (design-v3 §13).
pub fn problems(cfg: &FileConfig, out: &mut Vec<String>) {
    let c = &cfg.constellation;
    let share = c.requester_share.0;
    if !(share > 0.0 && share <= 1.0) {
        out.push(format!("constellation.requester_share = {share} must be above 0 and at most 1"));
    }
    let fraction = c.nonmember_fraction.0;
    if !(fraction > 0.0 && fraction < 1.0) {
        out.push(format!("constellation.nonmember_fraction = {fraction} must be between 0 and 1, exclusive"));
    }
    if c.requester_share_max_entries < 1000 {
        out.push("constellation.requester_share_max_entries must be at least 1000".to_string());
    }
    let rates = [
        ("microcosm.slingshot", cfg.microcosm.slingshot_rate_per_sec, cfg.microcosm.slingshot_burst),
        ("microcosm.constellation", cfg.microcosm.constellation_rate_per_sec, cfg.microcosm.constellation_burst),
        ("identity.plc_request", cfg.identity.plc_request_rate_per_sec, cfg.identity.plc_request_burst),
        (
            "identity.requester_resolutions",
            cfg.identity.requester_resolutions_per_sec,
            cfg.identity.requester_resolutions_burst,
        ),
    ];
    for (name, per_sec, burst) in rates {
        if let Some(p) = class_problem(RateClassConfig::new(per_sec.0, burst)) {
            out.push(format!("{name}: {p}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shares() -> Shares {
        Shares::new(0.25, 0.5, 1000)
    }

    fn ip(s: &str) -> ChargeKey {
        ChargeKey::ip(s.parse().expect("ip"))
    }

    #[test]
    fn one_requester_cannot_empty_a_pool() {
        // 20/s, burst 40: the non-member pool is 10/s, burst 20, and a share
        // of it is 2.5/s, burst 5 (the design's Bluesky example).
        let bucket = shares().bucket("network", 20.0, 40);
        let now = Instant::now();
        let a = ip("203.0.113.1");
        for _ in 0..5 {
            assert_eq!(bucket.draw_at(&a, now), Ok(Pool::NonMember));
        }
        let e = bucket.draw_at(&a, now).expect_err("the share is empty");
        assert_eq!(e.pool, Pool::NonMember);
        assert_eq!(e.retry_after, Duration::from_millis(400));
        // The pool still serves others: three more keys take the other 15.
        for other in ["203.0.113.2", "203.0.113.3", "203.0.113.4"] {
            for _ in 0..5 {
                bucket.draw_at(&ip(other), now).expect("its own share");
            }
        }
        // Four keys emptied the pool: a fifth has a full share and no token.
        let e = bucket.draw_at(&ip("203.0.113.5"), now).expect_err("the pool is empty");
        assert_eq!(e.retry_after, Duration::from_millis(100));
        // A share refills at its own rate.
        bucket.draw_at(&a, now + Duration::from_millis(400)).expect("one share token back");
    }

    #[test]
    fn an_empty_pool_does_not_spend_the_share() {
        let bucket = shares().bucket("network", 20.0, 40);
        let now = Instant::now();
        for other in ["203.0.113.1", "203.0.113.2", "203.0.113.3", "203.0.113.4"] {
            for _ in 0..5 {
                bucket.draw_at(&ip(other), now).expect("its own share");
            }
        }
        let late = ip("203.0.113.9");
        for _ in 0..10 {
            assert!(bucket.draw_at(&late, now).is_err());
        }
        // Half a second later the pool holds 5 tokens, and the late key's
        // share is still whole: it takes all 5.
        let later = now + Duration::from_millis(500);
        for _ in 0..5 {
            bucket.draw_at(&late, later).expect("an unspent share");
        }
        assert!(bucket.draw_at(&late, later).is_err());
    }

    #[test]
    fn members_draw_on_their_own_pool_first_and_non_members_never_on_it() {
        let bucket = shares().bucket("network", 20.0, 40);
        let now = Instant::now();
        // Non-members empty their pool.
        for other in ["203.0.113.1", "203.0.113.2", "203.0.113.3", "203.0.113.4"] {
            for _ in 0..5 {
                bucket.draw_at(&ip(other), now).expect("its own share");
            }
        }
        assert!(bucket.draw_at(&ip("203.0.113.5"), now).is_err());
        // The member pool is untouched.
        let m = ChargeKey::member("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
        for _ in 0..5 {
            assert_eq!(bucket.draw_at(&m, now), Ok(Pool::Member));
        }
        // Its member share is empty and so is the non-member pool.
        assert_eq!(bucket.draw_at(&m, now).expect_err("both").pool, Pool::NonMember);

        // With the non-member pool whole, a member overflows into it.
        let bucket = shares().bucket("network", 20.0, 40);
        for _ in 0..5 {
            assert_eq!(bucket.draw_at(&m, now), Ok(Pool::Member));
        }
        for _ in 0..5 {
            assert_eq!(bucket.draw_at(&m, now), Ok(Pool::NonMember));
        }
        assert!(bucket.draw_at(&m, now).is_err());
    }

    #[test]
    fn buckets_are_independent_and_ipv6_shares_are_per_64() {
        let shares = shares();
        let a = shares.bucket("network", 20.0, 40);
        let b = shares.bucket("slingshot", 20.0, 40);
        let now = Instant::now();
        let key = ip("2001:db8:1:2::1");
        for _ in 0..5 {
            a.draw_at(&key, now).expect("share of a");
        }
        assert!(a.draw_at(&ip("2001:db8:1:2:ffff::9"), now).is_err(), "the same /64 is one requester");
        b.draw_at(&key, now).expect("another bucket has its own share");
        assert_eq!(shares.entries(), 2);
    }

    #[test]
    fn a_small_share_still_has_a_token() {
        // 1/s, burst 2 at a 1% share: the share's burst is raised to 1.
        let bucket = Shares::new(0.01, 0.5, 1000).bucket("network", 1.0, 2);
        let now = Instant::now();
        let key = ip("203.0.113.1");
        bucket.draw_at(&key, now).expect("one token");
        assert!(bucket.draw_at(&key, now).is_err());
    }

    #[test]
    fn settings_are_validated() {
        let check = |toml: &str| {
            let cfg = FileConfig::parse(toml).expect("parse");
            let mut out = Vec::new();
            problems(&cfg, &mut out);
            out
        };
        assert!(check("").is_empty());
        assert!(check("[constellation]\nrequester_share = 1.0\n").is_empty());
        for (toml, needle) in [
            ("[constellation]\nrequester_share = 0.0\n", "requester_share"),
            ("[constellation]\nrequester_share = 1.5\n", "requester_share"),
            ("[constellation]\nrequester_share = nan\n", "requester_share"),
            ("[constellation]\nnonmember_fraction = 1.0\n", "nonmember_fraction"),
            ("[constellation]\nnonmember_fraction = 0.0\n", "nonmember_fraction"),
            ("[constellation]\nrequester_share_max_entries = 10\n", "requester_share_max_entries"),
            ("[microcosm]\nslingshot_rate_per_sec = 0.0\n", "microcosm.slingshot"),
            ("[microcosm]\nconstellation_burst = 0\n", "microcosm.constellation"),
            ("[identity]\nplc_request_rate_per_sec = inf\n", "identity.plc_request"),
            ("[identity]\nrequester_resolutions_burst = 0\n", "identity.requester_resolutions"),
        ] {
            let out = check(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        }
    }
}
