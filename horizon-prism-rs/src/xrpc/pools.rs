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
use std::sync::atomic::{AtomicU64, Ordering};
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
pub(super) struct Rate {
    pub(super) per_sec: f64,
    pub(super) burst: f64,
}

impl Rate {
    pub(super) fn new(per_sec: f64, burst: u32) -> Self {
        Self {
            per_sec,
            burst: f64::from(burst),
        }
    }

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

/// A token bucket that starts full. Tokens can be taken and given back.
#[derive(Debug, Clone, Copy)]
pub(super) struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Bucket {
    pub(super) fn full(rate: Rate, now: Instant) -> Self {
        Self { tokens: rate.burst, at: now }
    }

    /// Whether a token could be taken now, without taking it.
    pub(super) fn has_token(&mut self, rate: Rate, now: Instant) -> bool {
        self.refill(rate, now);
        self.tokens >= 1.0
    }

    /// Return a taken token. A bucket never rises above its burst.
    pub(super) fn give_back(&mut self, rate: Rate) {
        self.tokens = (self.tokens + 1.0).min(rate.burst);
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

    /// Take one token, or say how long until there is one.
    pub(super) fn take(&mut self, rate: Rate, now: Instant) -> Result<(), Duration> {
        self.refill(rate, now);
        if self.tokens < 1.0 {
            return Err(self.wait(rate));
        }
        self.tokens -= 1.0;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TargetId(u64);

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

impl<K> Expiry<K, Share> for IdleExpiry {
    fn expire_after_create(&self, _: &K, share: &Share, _: Instant) -> Option<Duration> {
        Some(share.idle)
    }

    fn expire_after_update(&self, _: &K, share: &Share, _: Instant, _: Option<Duration>) -> Option<Duration> {
        Some(share.idle)
    }
}

/// A per-requester rate with no pool behind it: the identity share
/// (`identity.requester_resolutions_per_sec`), one token per DID or handle
/// resolved for a request. Cheap to clone.
#[derive(Clone)]
pub struct RequesterLimit {
    cache: Cache<ChargeKey, Share>,
    rate: Rate,
    idle: Duration,
    target_kind: &'static str,
}

impl RequesterLimit {
    #[must_use]
    pub fn new(target_kind: &'static str, per_sec: f64, burst: u32, max_entries: u64) -> Self {
        let rate = Rate {
            per_sec,
            burst: f64::from(burst),
        };
        Self {
            cache: Cache::builder()
                .max_capacity(max_entries)
                .eviction_policy(EvictionPolicy::lru())
                .expire_after(IdleExpiry)
                .build(),
            rate,
            idle: rate.idle(),
            target_kind,
        }
    }

    /// Take one token for `requester`. Never waits.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] when the requester's bucket is empty.
    pub fn draw(&self, requester: &ChargeKey) -> Result<(), Exhausted> {
        self.draw_at(requester, Instant::now())
    }

    fn draw_at(&self, requester: &ChargeKey, now: Instant) -> Result<(), Exhausted> {
        let pool = match requester {
            ChargeKey::Member(_) => Pool::Member,
            ChargeKey::Ip(_) => Pool::NonMember,
        };
        let mut outcome = Ok(());
        let _ = self.cache.entry(requester.clone()).and_compute_with(|entry| {
            let mut bucket = entry.map_or_else(|| Bucket::full(self.rate, now), |e| e.into_value().bucket);
            match bucket.take(self.rate, now) {
                Ok(()) => Op::Put(Share { bucket, idle: self.idle }),
                Err(retry_after) => {
                    outcome = Err(Exhausted { pool, retry_after });
                    Op::Nop
                }
            }
        });
        if outcome.is_err() {
            metrics::counter!(METRIC_EXHAUSTED, "target_kind" => self.target_kind, "pool" => pool.label()).increment(1);
        }
        outcome
    }
}

/// The per-requester shares of every shared bucket: one LRU cache. Cheap to
/// clone.
#[derive(Clone)]
pub struct Shares {
    cache: Cache<ShareKey, Share>,
    next_target: Arc<AtomicU64>,
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
            next_target: Arc::new(AtomicU64::new(0)),
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
        self.build(target_kind, per_sec, burst, share, 1.0 - self.nonmember_fraction)
    }

    /// PLC's bucket, whose member pool gives half of its rate and burst to
    /// an **issuer-refresh slice** (§II-8.3 "Pools"). Resolving a member's
    /// DID to check a token on a request charged to an IP key may draw on
    /// the slice, once per member per `refresh`, and on nothing else of the
    /// member pool. So however many keys a flood has, it takes at most the
    /// slice, and only to resolve members.
    #[must_use]
    pub fn bucket_with_slice(&self, target_kind: &'static str, per_sec: f64, burst: u32, refresh: Duration) -> (SharedBucket, IssuerSlice) {
        let half = (1.0 - self.nonmember_fraction) / 2.0;
        let rate = Rate::new(per_sec, burst).scaled(half);
        let slice = IssuerSlice {
            inner: Arc::new(SliceInner {
                rate,
                state: Mutex::new(Bucket::full(rate, Instant::now())),
                used: Cache::builder().max_capacity(1_000_000).time_to_live(refresh).build(),
            }),
        };
        (self.build(target_kind, per_sec, burst, self.share, half), slice)
    }

    fn build(&self, target_kind: &'static str, per_sec: f64, burst: u32, share: f64, member_fraction: f64) -> SharedBucket {
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
                member: side(Pool::Member, member_fraction),
                nonmember: side(Pool::NonMember, self.nonmember_fraction),
                shares: self.cache.clone(),
            }),
        }
    }

    /// Shares for tests: one requester may use a whole pool.
    #[cfg(test)]
    #[must_use]
    pub fn for_tests() -> Self {
        Self::new(1.0, 0.5, 10_000)
    }

    /// Live share entries (tests, metrics).
    #[must_use]
    pub fn entries(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.entry_count()
    }
}

struct SliceInner {
    rate: Rate,
    state: Mutex<Bucket>,
    /// Members whose allowance for this window is used.
    used: Cache<String, ()>,
}

/// The issuer-refresh slice of PLC's member pool
/// ([`Shares::bucket_with_slice`]). Cheap to clone.
#[derive(Clone)]
pub struct IssuerSlice {
    inner: Arc<SliceInner>,
}

impl IssuerSlice {
    /// Take the slice token for resolving member `did`, if its allowance
    /// for this window is unused and the slice has a token. The allowance
    /// is spent only when a token is taken.
    #[must_use]
    pub fn take(&self, did: &str) -> bool {
        if self.inner.used.contains_key(did) {
            return false;
        }
        let taken = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take(self.inner.rate, Instant::now())
            .is_ok();
        if taken {
            self.inner.used.insert(did.to_string(), ());
        }
        taken
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

impl std::fmt::Debug for SharedBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBucket").field("target_kind", &self.inner.target_kind).finish_non_exhaustive()
    }
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

    /// [`Self::draw`] without counting a miss in
    /// `requester_share_exhausted_total`. For a caller that treats an
    /// empty share as "no answer" and counts it itself (Farsight's lookup
    /// and its fallback, design-v3 §5.14.17.4): such a miss is never a
    /// `429` and stops nothing else in the request.
    ///
    /// # Errors
    ///
    /// [`Exhausted`], as [`Self::draw`].
    pub fn quiet_draw(&self, requester: &ChargeKey) -> Result<Pool, Exhausted> {
        self.take(requester, Instant::now())
    }

    /// [`Self::quiet_draw`] on the non-member pool only, whatever the
    /// key's variant. For a caller that is known by its account and is not
    /// a ready member (a Thread Lens visitor, charged to its DID): it has a
    /// share of its own there, the one a member's overflow uses, and it
    /// never touches the member pool. On success the pool is always
    /// [`Pool::NonMember`].
    ///
    /// # Errors
    ///
    /// [`Exhausted`] when the requester's share of the non-member pool, or
    /// that pool, is empty.
    pub fn quiet_draw_nonmember(&self, requester: &ChargeKey) -> Result<Pool, Exhausted> {
        self.take_nonmember(requester, Instant::now())
    }

    fn take_nonmember(&self, requester: &ChargeKey, now: Instant) -> Result<Pool, Exhausted> {
        self.draw_in(&self.inner.nonmember, requester, now)
    }

    fn take(&self, requester: &ChargeKey, now: Instant) -> Result<Pool, Exhausted> {
        let inner = &self.inner;
        match requester {
            ChargeKey::Member(_) => self
                .draw_in(&inner.member, requester, now)
                .or_else(|_| self.draw_in(&inner.nonmember, requester, now)),
            ChargeKey::Ip(_) => self.draw_in(&inner.nonmember, requester, now),
        }
    }

    fn draw_at(&self, requester: &ChargeKey, now: Instant) -> Result<Pool, Exhausted> {
        let inner = &self.inner;
        let outcome = self.take(requester, now);
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

/// `public_fallback_hosts`: host buckets alive now.
pub const METRIC_HOSTS: &str = "public_fallback_hosts";

/// The public-fallback bucket of every PDS host a public request reads
/// (design-v3 §II-6.11.3): one [`SharedBucket`] per host, created on first
/// use. Any did:web or PDS host can be named anonymously, so the buckets
/// live in an LRU cache of `sync.max_public_fallback_hosts` entries with a
/// time-to-idle of `max(2 s, 2 / rate)`: an evicted bucket has refilled, and
/// a new host always gets one. Cheap to clone.
#[derive(Clone)]
pub struct HostBuckets {
    hosts: Cache<String, SharedBucket>,
    shares: Shares,
    per_sec: f64,
    burst: u32,
}

impl HostBuckets {
    /// Buckets of `per_sec` per host, for at most `max_hosts` hosts.
    #[must_use]
    pub fn new(shares: &Shares, per_sec: f64, max_hosts: u64) -> Self {
        let idle = Duration::try_from_secs_f64(2.0 / per_sec).map_or(Duration::MAX, |d| d.max(Duration::from_secs(2)));
        Self {
            hosts: Cache::builder()
                .max_capacity(max_hosts)
                .eviction_policy(EvictionPolicy::lru())
                .time_to_idle(idle)
                .build(),
            shares: shares.clone(),
            per_sec,
            // A second's worth, and at least one token in each pool.
            burst: (per_sec.ceil() as u32).max(2),
        }
    }

    /// Take one token of `host`'s bucket for `req`. `false` is "no answer",
    /// and the request remembers it.
    #[must_use]
    pub fn draw(&self, req: &Requester, host: &str) -> bool {
        let bucket = self
            .hosts
            .get_with_by_ref(host, || self.shares.bucket("host", self.per_sec, self.burst));
        metrics::gauge!(METRIC_HOSTS).set(self.hosts.entry_count() as f64);
        req.draw(&bucket)
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
///
/// It also carries **viewer-private mode** (design-v4 §8.1 "Viewer-private
/// fetches"; R3-188, R3-200, R3-201, R3-208): see
/// [`Requester::viewer_private`].
#[derive(Debug, Clone)]
pub struct Requester {
    key: ChargeKey,
    deadline: Instant,
    exhausted: Arc<Mutex<Option<Exhausted>>>,
    /// The viewer a viewer-private fetch is made for, if this is one.
    private_viewer: Option<Arc<str>>,
    /// Private-context hydration (design-v4 §8.1): local rows only.
    local_only: bool,
    /// The XRPC method being served, for the `endpoint` label of outbound
    /// call metrics (design-v3 §14.2).
    endpoint: Option<Arc<str>>,
}

/// `origin` of an outbound call made while serving an XRPC request
/// (design-v3 §14.2).
pub const ORIGIN_REQUEST: &str = "request";
/// `origin` of an outbound call made by background work: the count
/// refresher, backfill, `list_sync` and other jobs.
pub const ORIGIN_BACKGROUND: &str = "background";

/// The `(origin, endpoint)` labels of an outbound call made with no request
/// handle in hand (Constellation): the running request's, if the call is
/// made while serving one, else `background` with an empty endpoint. The
/// request context carries the label, so background work never moves a
/// request-origin series.
#[must_use]
pub fn ambient_labels() -> (&'static str, String) {
    super::backstop::selected().map_or_else(|| (ORIGIN_BACKGROUND, String::new()), |s| s.req.labels())
}

impl Requester {
    /// A request starting now.
    #[must_use]
    pub fn new(key: ChargeKey) -> Self {
        Self {
            key,
            deadline: Instant::now() + REQUEST_DEADLINE,
            exhausted: Arc::new(Mutex::new(None)),
            private_viewer: None,
            local_only: false,
            endpoint: None,
        }
    }

    /// This request, serving the XRPC method `endpoint`.
    #[must_use]
    pub fn for_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(Arc::from(endpoint));
        self
    }

    /// The `(origin, endpoint)` labels of an outbound call charged to this
    /// request: a request handle exists only while serving a request.
    #[must_use]
    pub fn labels(&self) -> (&'static str, String) {
        (ORIGIN_REQUEST, self.endpoint.as_deref().unwrap_or_default().to_string())
    }

    /// This request, for **private-context hydration** (design-v4 §8.1
    /// "Private-context hydration", R3-179): the private handlers' view of
    /// the public parts of a private view. It is viewer-private for
    /// `viewer` (no shared cache is read or written, counts are read-only
    /// and nothing is queued), and it is local only: no cascade, Slingshot
    /// or Constellation call is made on its behalf. Only
    /// `hydration::HydrationContext::Private` builds one.
    #[must_use]
    pub(crate) fn private_context(&self, viewer: &str) -> Self {
        Self {
            local_only: true,
            ..self.viewer_private(viewer)
        }
    }

    /// Whether this is private-context hydration
    /// ([`Requester::private_context`]): local rows only.
    #[must_use]
    pub fn is_local_only(&self) -> bool {
        self.local_only
    }

    /// This request, for an upstream fetch or hydration whose subject set
    /// comes from, or is the subject of a write to, one of `viewer`'s
    /// AppView-private rows (§4.5: `mutes`, `list_mutes`, `thread_mutes`,
    /// `bookmarks`, `drafts`, `activity_subscriptions`,
    /// `notification_preferences_v2`). Every layer below reads the mode
    /// from here:
    /// - the shared in-process caches (`rt`, `route`, the request-side
    ///   identity summaries, the resolveHandle cache and its negative
    ///   entries, the PDS listing-order cache) are neither read nor written;
    ///   `rt` reads and writes go to `viewer`'s own private cache instead;
    /// - identity documents are fetched without a `did_cache` read or write;
    /// - stored counts are read but never written, and nothing is put on
    ///   `cc:refresh`.
    ///
    /// The clone shares this request's deadline, charge key and exhaustion
    /// record, so a viewer-private call draws on the viewer's fair share
    /// like any other (design-v4 §8.1).
    #[must_use]
    pub fn viewer_private(&self, viewer: &str) -> Self {
        Self {
            private_viewer: Some(Arc::from(viewer)),
            ..self.clone()
        }
    }

    /// The viewer of a viewer-private fetch; `None` for an ordinary one.
    #[must_use]
    pub fn private_viewer(&self) -> Option<&str> {
        self.private_viewer.as_deref()
    }

    /// Whether this is a viewer-private fetch ([`Requester::viewer_private`]).
    #[must_use]
    pub fn is_viewer_private(&self) -> bool {
        self.private_viewer.is_some()
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

    #[cfg(test)]
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = deadline;
        self
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
        self.try_draw(bucket).is_ok()
    }

    /// [`Requester::draw`], with what ran out.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] when the share or pool is empty.
    pub fn try_draw(&self, bucket: &SharedBucket) -> Result<(), Exhausted> {
        bucket.draw(&self.key).map(|_| ()).inspect_err(|e| self.note(*e))
    }

    /// Take one token of this requester's own `limit`. `false` is "no
    /// answer", and the request remembers it.
    #[must_use]
    pub fn draw_own(&self, limit: &RequesterLimit) -> bool {
        limit.draw(&self.key).inspect_err(|e| self.note(*e)).is_ok()
    }

    fn note(&self, e: Exhausted) {
        let mut seen = self.exhausted.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.is_none_or(|s| s.retry_after < e.retry_after) {
            *seen = Some(e);
        }
    }

    /// Whether any draw of this request found nothing. Once it has, the
    /// request tries no further network or fallback tier for the item it
    /// was fetching (§II-8.3 "Exhaustion").
    #[must_use]
    pub fn spent(&self) -> bool {
        self.exhausted().is_some()
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
    let hosts = cfg.sync.public_fallback_rate_per_host.0;
    if let Some(p) = class_problem(RateClassConfig::new(hosts, 2)) {
        out.push(format!("sync.public_fallback_rate_per_host: {p}"));
    }
    if cfg.identity.member_pool_refresh_secs < 60 {
        out.push("identity.member_pool_refresh_secs must be at least 60".to_string());
    } else {
        // Every member must be able to refresh once per window from the
        // issuer-refresh slice: half of PLC's member pool (§13, R3-209).
        let slice_rate = 0.5 * (1.0 - fraction) * cfg.identity.plc_request_rate_per_sec.0;
        let needed = f64::from(cfg.community.max_members) / cfg.identity.member_pool_refresh_secs as f64;
        if needed > slice_rate {
            out.push(format!(
                "community.max_members ({}) / identity.member_pool_refresh_secs ({}) must be at most \
                 0.5 x (1 - constellation.nonmember_fraction ({fraction})) x identity.plc_request_rate_per_sec ({}), \
                 the rate of PLC's issuer-refresh slice",
                cfg.community.max_members, cfg.identity.member_pool_refresh_secs, cfg.identity.plc_request_rate_per_sec.0
            ));
        }
    }
    if cfg.sync.max_public_fallback_hosts < 1000 {
        out.push("sync.max_public_fallback_hosts must be at least 1000".to_string());
    }
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

    /// A draw on the non-member pool by account: each account has a share
    /// of its own there, and the member pool is never touched, also when
    /// the key is a member key.
    #[test]
    fn a_nonmember_draw_by_account_has_its_own_share_and_leaves_the_member_pool() {
        // As above: the non-member pool is 10/s, burst 20; a share, burst 5.
        let bucket = shares().bucket("farsight", 20.0, 40);
        let now = Instant::now();
        let a = ChargeKey::member("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
        let b = ChargeKey::member("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        // Two accounts behind one address each have a whole share.
        for key in [&a, &b] {
            for _ in 0..5 {
                assert_eq!(bucket.take_nonmember(key, now), Ok(Pool::NonMember));
            }
            assert_eq!(bucket.take_nonmember(key, now).expect_err("its share is empty").pool, Pool::NonMember);
        }
        // The address they came from has its own share too.
        assert_eq!(bucket.take_nonmember(&ip("203.0.113.1"), now), Ok(Pool::NonMember));
        // The member pool is whole: the same key, drawn the ordinary way,
        // takes its five member tokens. Its non-member share is the one
        // already emptied above, so nothing is left for it after that.
        for _ in 0..5 {
            assert_eq!(bucket.draw_at(&a, now), Ok(Pool::Member));
        }
        assert!(bucket.draw_at(&a, now).is_err());
        // With the non-member pool empty, a non-member draw fails though
        // the member pool has tokens.
        for other in ["203.0.113.2", "203.0.113.3"] {
            while bucket.draw_at(&ip(other), now).is_ok() {}
        }
        let c = ChargeKey::member("did:plc:cccccccccccccccccccccccc");
        assert!(bucket.take_nonmember(&c, now).is_err());
        assert_eq!(bucket.draw_at(&c, now), Ok(Pool::Member));
        // The public entry point draws on the same pool.
        let fresh = shares().bucket("farsight", 20.0, 40);
        assert_eq!(fresh.quiet_draw_nonmember(&a), Ok(Pool::NonMember));
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
    fn each_host_has_its_own_bucket_and_new_hosts_always_get_one() {
        // 4/s per host: two tokens in the non-member pool.
        let hosts = HostBuckets::new(&Shares::for_tests(), 4.0, 1000);
        let req = Requester::test();
        assert!(hosts.draw(&req, "pds.a.example"));
        assert!(hosts.draw(&req, "pds.a.example"));
        assert!(!hosts.draw(&req, "pds.a.example"));
        assert!(req.spent());
        // Another host is unaffected, and so are hosts past the capacity
        // (LRU eviction, no refusal).
        let other = Requester::of(None, "198.51.100.7".parse().expect("ip"));
        for i in 0..3000 {
            assert!(hosts.draw(&other, &format!("pds{i}.example")), "host {i}");
        }
        assert!(!other.spent());
    }

    #[test]
    fn the_identity_share_is_per_requester() {
        let limit = RequesterLimit::new("identity", 1.0, 2, 1000);
        let now = Instant::now();
        let a = ip("203.0.113.1");
        limit.draw_at(&a, now).expect("first");
        limit.draw_at(&a, now).expect("second");
        let e = limit.draw_at(&a, now).expect_err("empty");
        assert_eq!((e.pool, e.retry_after), (Pool::NonMember, Duration::from_secs(1)));
        limit.draw_at(&ip("203.0.113.2"), now).expect("another requester");
        limit.draw_at(&ChargeKey::member("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"), now).expect("a member");
        limit.draw_at(&a, now + Duration::from_secs(1)).expect("refilled");
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
            ("[sync]\npublic_fallback_rate_per_host = 0.0\n", "sync.public_fallback_rate_per_host"),
            ("[sync]\nmax_public_fallback_hosts = 10\n", "sync.max_public_fallback_hosts"),
            ("[identity]\nmember_pool_refresh_secs = 5\n", "identity.member_pool_refresh_secs"),
            ("[community]\nmax_members = 5000\n", "issuer-refresh slice"),
        ] {
            let out = check(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        }
    }
}
