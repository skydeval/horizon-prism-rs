//! The parts of the pre-auth backstop that need no I/O (design-v3 §II-8.3
//! "Key and evaluation order", item 1): the claims pre-check, and the limits
//! on verification done before the backstop.
//!
//! **Claims pre-check.** [`precheck`] reads a token's *unverified* payload
//! and applies every claim check that kryphocron 0.4.0 makes after the
//! signature (`verify_claims_and_construct`), plus this host's `aud`
//! fragment and `lxm` checks. It is those predicates, neither stricter nor
//! looser. A token that fails it is not verified before the backstop: it is
//! charged to its IP key and takes no token below, so nobody can spend a
//! member's buckets with a token the verifier would reject on its claims.
//!
//! **Limits** ([`VerifyLimits`]). A request holds a ceiling token only
//! while it is being verified:
//! 1. failure-bucket admission: failed verifications are limited per
//!    (IP key, `iss`) and per IP key. A member that verified from this IP
//!    key against the same key within the success window is exempt from
//!    both;
//! 2. (the ready-member gate, done by the caller);
//! 3. a per-member token: none left is a `429`, with no verification and
//!    no charge to either key;
//! 4. a per-IP-key token: none left returns the per-member token, and the
//!    request goes to its IP key unverified;
//! 5. verification;
//! 6. settling: "signature verified" returns the per-IP-key token and keeps
//!    the per-member one; "not verified" keeps the per-IP-key token,
//!    returns the per-member one, and spends the failure buckets.
//!
//! So forgers never spend a member's ceiling, valid traffic never drains an
//! IP key's ceiling, and one member's key holder gets at most the
//! per-member rate of pre-backstop verifications.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kryphocron::Did;
use kryphocron::verification::JwtVerificationConfig;
use moka::ops::compute::Op;
use moka::policy::EvictionPolicy;
use moka::sync::Cache;
use serde_json::Value;

use super::auth::Authenticator;
use super::member_keys::{KeyAge, MemberKeys};
use super::pools::{Bucket, ChargeKey, Rate, Requester};
use super::rate_limit::{class_problem, ip_key};
use crate::config::{AuthConfig, RateClassConfig};
use crate::farsight::Lookup;
use crate::kryphocron_glue::VerifiedCaller;
use crate::kryphocron_glue::jwt::{APPVIEW_FRAGMENT, MemoryVerdict};

/// `auth_verify_ceiling_exhausted_total{ceiling = "ip" | "member"}`.
pub const METRIC_CEILING: &str = "auth_verify_ceiling_exhausted_total";
/// `auth_failed_verify_limited_total`: requests sent to their IP key
/// because a failure bucket was empty.
pub const METRIC_FAILURE_LIMITED: &str = "auth_failed_verify_limited_total";
/// `auth_member_verify_exhausted_total`: the per-member 429, a request
/// whose member ceiling was empty (R3-243).
pub const METRIC_MEMBER_EXHAUSTED: &str = "auth_member_verify_exhausted_total";
/// `auth_claims_precheck_failed_total`: tokens whose unverified claims
/// failed the pre-check, charged to their IP key unverified (R3-246).
pub const METRIC_PRECHECK_FAILED: &str = "auth_claims_precheck_failed_total";
/// How long an untouched failure or ceiling bucket is kept.
const STATE_IDLE: Duration = Duration::from_secs(3600);

/// A fingerprint of the issuer key a verification is checked against.
pub type KeyPrint = u64;

/// What the pre-check read from a token that passed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// The unverified issuer.
    pub iss: String,
}

/// The bearer token of an `Authorization` value, as kryphocron strips it.
fn bearer(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    let token = ["Bearer ", "bearer ", "BEARER "]
        .iter()
        .find_map(|p| trimmed.strip_prefix(p))
        .map_or(trimmed, str::trim_start);
    (!token.is_empty()).then_some(token)
}

/// The claims pre-check: `Some` when the unverified payload passes every
/// claim check the verifier makes after the signature.
///
/// `now` and `config` are the verifier's own, so the two agree.
#[must_use]
pub fn precheck(
    authorization: &str,
    service_did: &Did,
    method: &str,
    config: &JwtVerificationConfig,
    now: SystemTime,
) -> Option<Claims> {
    let mut segments = bearer(authorization)?.split('.');
    let (_, payload, _) = (segments.next()?, segments.next()?, segments.next()?);
    if segments.next().is_some() {
        return None;
    }
    let p: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;

    let iss = p.get("iss")?.as_str()?;
    Did::new(iss).ok()?;

    // `aud`: this service's DID, with no fragment or `#bsky_appview`.
    let aud = p.get("aud")?.as_str()?;
    let aud_did = match aud.split_once('#') {
        Some((did, APPVIEW_FRAGMENT)) => did,
        Some(_) => return None,
        None => aud,
    };
    if Did::new(aud_did).ok()? != *service_did {
        return None;
    }

    if p.get("lxm")?.as_str()? != method {
        return None;
    }

    // Times, with the verifier's arithmetic. A value too large to be a
    // time fails here; the verifier would not accept it either.
    let at = |secs: u64| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(secs));
    let skew = config.max_clock_skew;
    let iat = at(p.get("iat")?.as_u64()?)?;
    let exp = at(p.get("exp")?.as_u64()?)?;
    if exp.duration_since(iat).unwrap_or(Duration::ZERO) > config.max_validity_window {
        return None;
    }
    let late = now.checked_add(skew)?;
    if now > exp.checked_add(skew)? || late < iat {
        return None;
    }
    // A non-integer `nbf` is ignored, as the verifier ignores it.
    if let Some(nbf) = p.get("nbf").and_then(Value::as_u64)
        && late < at(nbf)?
    {
        return None;
    }
    // A non-string `nonce` is ignored too.
    if let Some(nonce) = p.get("nonce").and_then(Value::as_str)
        && URL_SAFE_NO_PAD.decode(nonce).ok()?.len() != 16
    {
        return None;
    }
    Some(Claims { iss: iss.to_string() })
}

/// How a pre-backstop verification ended (R3-250).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The verifier accepted the token, or rejected it only after its
    /// signature verified against an in-memory key of `iss`. `fresh` is
    /// whether that key authenticates (a stale key only selects the member).
    SignatureVerified { fresh: bool },
    /// Everything else.
    NotVerified,
}

/// What [`VerifyLimits::admit`] decided.
#[derive(Debug)]
pub enum Admission {
    /// Verify now, then [`VerifyLimits::settle`] the ticket.
    Verify(Ticket),
    /// Do not verify: charge the request to its IP key.
    Unverified,
    /// The member's own ceiling is empty: `429`, charged to neither key.
    Limited { retry_after: Duration },
}

/// The tokens one verification holds.
#[derive(Debug)]
#[must_use = "a ticket holds ceiling tokens until it is settled"]
pub struct Ticket {
    ip: IpAddr,
    iss: String,
    key: KeyPrint,
    exempt: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StateKey {
    /// Failed verifications of one `iss` from one IP key.
    Pair(IpAddr, String),
    /// Failed verifications from one IP key.
    Aggregate(IpAddr),
    /// The per-IP-key CPU ceiling.
    Ceiling(IpAddr),
}

#[derive(Clone, Copy)]
enum BucketOp {
    /// Whether a token is there, without taking it.
    Peek,
    Take,
    GiveBack,
}

struct Success {
    ip: IpAddr,
    at: Instant,
    key: KeyPrint,
}

/// A member's ceiling and its recent successes. Lives outside the failure
/// state, so failures cannot evict it.
struct MemberState {
    ceiling: Bucket,
    successes: Vec<Success>,
}

struct Inner {
    pair: Rate,
    aggregate: Rate,
    ceiling: Rate,
    member: Rate,
    success_window: Duration,
    success_ips_max: usize,
    state: Cache<StateKey, Bucket>,
    members: Mutex<HashMap<String, MemberState>>,
}

/// The limits on pre-backstop verification. Cheap to clone.
#[derive(Clone)]
pub struct VerifyLimits {
    inner: Arc<Inner>,
}

impl VerifyLimits {
    /// Limits from a validated `[auth]`.
    #[must_use]
    pub fn from_config(cfg: &AuthConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                pair: Rate::new(cfg.failed_verify_per_iss_rate_per_sec.0, cfg.failed_verify_per_iss_burst),
                aggregate: Rate::new(cfg.failed_verify_rate_per_sec.0, cfg.failed_verify_burst),
                ceiling: Rate::new(cfg.verify_rate_per_sec.0, cfg.verify_burst),
                member: Rate::new(cfg.member_verify_rate_per_sec.0, cfg.member_verify_burst),
                success_window: Duration::from_secs(cfg.failed_verify_success_window_secs),
                success_ips_max: usize::try_from(cfg.failed_verify_success_ips_max).unwrap_or(usize::MAX),
                state: Cache::builder()
                    .max_capacity(cfg.failed_verify_state_max)
                    .eviction_policy(EvictionPolicy::lru())
                    .time_to_idle(STATE_IDLE)
                    .build(),
                members: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn members(&self) -> std::sync::MutexGuard<'_, HashMap<String, MemberState>> {
        self.inner.members.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Apply `op` to a state bucket, creating it full, and say whether it
    /// succeeded. An evicted bucket comes back full, which only refills it.
    fn bucket(&self, key: StateKey, rate: Rate, now: Instant, op: BucketOp) -> bool {
        let mut ok = false;
        let _ = self.inner.state.entry(key).and_compute_with(|entry| {
            let mut bucket = entry.map_or_else(|| Bucket::full(rate, now), |e| e.into_value());
            ok = match op {
                BucketOp::Peek => bucket.has_token(rate, now),
                BucketOp::Take => bucket.take(rate, now).is_ok(),
                BucketOp::GiveBack => {
                    bucket.give_back(rate);
                    true
                }
            };
            Op::Put(bucket)
        });
        ok
    }

    fn recent_success(&self, iss: &str, ip: IpAddr, key: KeyPrint, now: Instant) -> bool {
        self.members().get(iss).is_some_and(|m| {
            m.successes
                .iter()
                .any(|s| s.ip == ip && s.key == key && now.saturating_duration_since(s.at) < self.inner.success_window)
        })
    }

    /// Steps 1, 3 and 4 of the token lifecycle for a ready member `iss`
    /// whose in-memory key has fingerprint `key`, asked from IP key `ip`.
    #[must_use]
    pub fn admit(&self, ip: IpAddr, iss: &str, key: KeyPrint) -> Admission {
        self.admit_at(ip, iss, key, Instant::now())
    }

    fn admit_at(&self, ip: IpAddr, iss: &str, key: KeyPrint, now: Instant) -> Admission {
        let inner = &self.inner;
        // 1. Failure buckets, unless this member verified from here lately.
        let exempt = self.recent_success(iss, ip, key, now);
        if !exempt {
            let pair = self.bucket(StateKey::Pair(ip, iss.to_string()), inner.pair, now, BucketOp::Peek);
            let aggregate = self.bucket(StateKey::Aggregate(ip), inner.aggregate, now, BucketOp::Peek);
            if !pair || !aggregate {
                metrics::counter!(METRIC_FAILURE_LIMITED).increment(1);
                return Admission::Unverified;
            }
        }
        // 3. The per-member ceiling.
        let taken = self
            .members()
            .entry(iss.to_string())
            .or_insert_with(|| MemberState {
                ceiling: Bucket::full(inner.member, now),
                successes: Vec::new(),
            })
            .ceiling
            .take(inner.member, now);
        if let Err(retry_after) = taken {
            metrics::counter!(METRIC_CEILING, "ceiling" => "member").increment(1);
            metrics::counter!(METRIC_MEMBER_EXHAUSTED).increment(1);
            return Admission::Limited { retry_after };
        }
        // 4. The per-IP-key ceiling.
        if !self.bucket(StateKey::Ceiling(ip), inner.ceiling, now, BucketOp::Take) {
            self.return_member_token(iss);
            metrics::counter!(METRIC_CEILING, "ceiling" => "ip").increment(1);
            return Admission::Unverified;
        }
        Admission::Verify(Ticket {
            ip,
            iss: iss.to_string(),
            key,
            exempt,
        })
    }

    fn return_member_token(&self, iss: &str) {
        if let Some(m) = self.members().get_mut(iss) {
            m.ceiling.give_back(self.inner.member);
        }
    }

    /// Step 6: settle a verification's tokens by its outcome.
    pub fn settle(&self, ticket: Ticket, outcome: Outcome) {
        self.settle_at(ticket, outcome, Instant::now());
    }

    fn settle_at(&self, ticket: Ticket, outcome: Outcome, now: Instant) {
        let inner = &self.inner;
        match outcome {
            Outcome::SignatureVerified { fresh } => {
                self.bucket(StateKey::Ceiling(ticket.ip), inner.ceiling, now, BucketOp::GiveBack);
                // A stale-key match counts for the ceilings but is no success.
                if fresh {
                    self.record_success(&ticket, now);
                }
            }
            Outcome::NotVerified => {
                self.return_member_token(&ticket.iss);
                if !ticket.exempt {
                    self.bucket(StateKey::Pair(ticket.ip, ticket.iss), inner.pair, now, BucketOp::Take);
                    self.bucket(StateKey::Aggregate(ticket.ip), inner.aggregate, now, BucketOp::Take);
                }
            }
        }
    }

    fn record_success(&self, ticket: &Ticket, now: Instant) {
        let mut members = self.members();
        let Some(m) = members.get_mut(&ticket.iss) else {
            return;
        };
        if let Some(s) = m.successes.iter_mut().find(|s| s.ip == ticket.ip) {
            (s.at, s.key) = (now, ticket.key);
            return;
        }
        if m.successes.len() >= self.inner.success_ips_max
            && let Some(oldest) = m.successes.iter().enumerate().min_by_key(|(_, s)| s.at).map(|(i, _)| i)
        {
            m.successes.swap_remove(oldest);
        }
        m.successes.push(Success {
            ip: ticket.ip,
            at: now,
            key: ticket.key,
        });
    }

    /// Record a verification that `prepare` made with a fresh key, outside
    /// the ticket lifecycle: it counts as a recent success too (R3-233).
    pub fn record_prepare_success(&self, ip: IpAddr, iss: &str, key: KeyPrint) {
        let now = Instant::now();
        self.members().entry(iss.to_string()).or_insert_with(|| MemberState {
            ceiling: Bucket::full(self.inner.member, now),
            successes: Vec::new(),
        });
        self.record_success(
            &Ticket {
                ip,
                iss: iss.to_string(),
                key,
                exempt: true,
            },
            now,
        );
    }

    /// Drop a removed member's state.
    pub fn forget(&self, iss: &str) {
        self.members().remove(iss);
    }
}

/// What the backstop decided for one request: who is charged, and the
/// caller if a fresh key already authenticated it.
#[derive(Debug, Clone)]
pub struct Selected {
    /// The request's handle: its charge key, deadline and exhaustion record.
    pub req: Requester,
    /// The `Authorization` value and the caller it verified to.
    pre: Option<Arc<(String, VerifiedCaller)>>,
    /// The request's handle on Farsight, once a public handler has
    /// attached one for a ready-member viewer (design-v3 §5.14.5). Empty
    /// for every other request, which then makes no lookup.
    farsight: Arc<OnceLock<Lookup>>,
}

impl Selected {
    /// A request charged to `ip`'s key, not verified.
    #[must_use]
    pub fn by_ip(ip: IpAddr) -> Self {
        Self {
            req: Requester::new(ChargeKey::ip(ip)),
            pre: None,
            farsight: Arc::default(),
        }
    }

    /// Give the request its Farsight handle. The first one stays.
    pub fn attach_farsight(&self, lookup: Lookup) {
        let _ = self.farsight.set(lookup);
    }

    /// The request's Farsight handle, if one was attached.
    #[must_use]
    pub fn farsight(&self) -> Option<Lookup> {
        self.farsight.get().cloned()
    }

    /// The caller the backstop verified `authorization` to, so a handler
    /// does not verify the token twice.
    #[must_use]
    pub fn verified(&self, authorization: &str) -> Option<VerifiedCaller> {
        self.pre.as_ref().filter(|p| p.0 == authorization).map(|p| p.1.clone())
    }
}

tokio::task_local! {
    /// The running request's selection. The backstop middleware sets it
    /// around the rest of the request, which runs in the same task; it is
    /// read where a handler authenticates and where the token path fetches
    /// an issuer's document. Outside a request there is none.
    static SELECTED: Selected;
}

/// The running request's selection, if a backstop set one.
#[must_use]
pub fn selected() -> Option<Selected> {
    SELECTED.try_with(Clone::clone).ok()
}

/// The running request's Farsight handle: present only when a public
/// handler attached one. A path with no request (a background job, the
/// CLI) and the private tier have none, and so never reach Farsight.
#[must_use]
pub fn farsight() -> Option<Lookup> {
    SELECTED.try_with(Selected::farsight).ok().flatten()
}

/// Run `f` as the rest of a request with selection `s`.
pub async fn scope<F: Future>(s: Selected, f: F) -> F::Output {
    SELECTED.scope(s, f).await
}

/// The outcome of [`Backstop::select`].
#[derive(Debug)]
pub enum Selection {
    Charge(Selected),
    /// The member's own verification ceiling is empty: `429`.
    Limited { retry_after: Duration },
}

/// The pre-auth backstop's state. Cheap to clone.
#[derive(Clone)]
pub struct Backstop {
    pub limits: VerifyLimits,
    pub keys: MemberKeys,
}

impl Backstop {
    /// Choose the key a request is charged to (§II-8.3 items 1–3). Reads no
    /// database and makes no network call.
    ///
    /// A request is charged to a member's DID only when its token passes
    /// the claims pre-check, names a ready member whose key is in memory,
    /// is admitted by the verification limits, and its signature verifies
    /// against that key. Everything else is charged to its IP key.
    pub async fn select(&self, auth: &dyn Authenticator, authorization: Option<&str>, method: Option<&str>, ip: IpAddr) -> Selection {
        let by_ip = || Selection::Charge(Selected::by_ip(ip));
        let (Some(authorization), Some(method)) = (authorization, method) else {
            return by_ip();
        };
        let Some(claims) = auth.precheck(authorization, method) else {
            metrics::counter!(METRIC_PRECHECK_FAILED).increment(1);
            return by_ip();
        };
        let Some(key) = self.keys.get(&claims.iss) else {
            return by_ip();
        };
        let Some(age) = key.age(chrono::Utc::now()) else {
            return by_ip();
        };
        let ticket = match self.limits.admit(ip_key(ip), &claims.iss, key.print) {
            Admission::Verify(ticket) => ticket,
            Admission::Unverified => return by_ip(),
            Admission::Limited { retry_after } => return Selection::Limited { retry_after },
        };
        let fresh = age == KeyAge::Fresh;
        let member = |pre| {
            Selection::Charge(Selected {
                req: Requester::new(ChargeKey::member(&claims.iss)),
                pre,
                farsight: Arc::default(),
            })
        };
        match auth.verify_in_memory(authorization, method, &key.doc).await {
            MemoryVerdict::Verified(caller) if caller.did == claims.iss => {
                self.limits.settle(ticket, Outcome::SignatureVerified { fresh });
                // A stale key only selects the member: the handler verifies.
                member(fresh.then(|| Arc::new((authorization.to_string(), *caller))))
            }
            MemoryVerdict::Verified(_) | MemoryVerdict::NotVerified => {
                self.limits.settle(ticket, Outcome::NotVerified);
                by_ip()
            }
            MemoryVerdict::SignatureOnly => {
                self.limits.settle(ticket, Outcome::SignatureVerified { fresh: false });
                member(None)
            }
        }
    }
}

/// Every problem in `[auth]` (design-v3 §13).
pub fn problems(cfg: &AuthConfig, out: &mut Vec<String>) {
    let rates = [
        ("failed_verify", cfg.failed_verify_rate_per_sec, cfg.failed_verify_burst),
        ("failed_verify_per_iss", cfg.failed_verify_per_iss_rate_per_sec, cfg.failed_verify_per_iss_burst),
        ("verify", cfg.verify_rate_per_sec, cfg.verify_burst),
        ("member_verify", cfg.member_verify_rate_per_sec, cfg.member_verify_burst),
    ];
    for (name, per_sec, burst) in rates {
        if let Some(p) = class_problem(RateClassConfig::new(per_sec.0, burst)) {
            out.push(format!("auth.{name}: {p}"));
        }
    }
    // One always-failing member may use at most a quarter of the aggregate.
    if cfg.failed_verify_rate_per_sec.0 < 4.0 * cfg.failed_verify_per_iss_rate_per_sec.0 {
        out.push("auth.failed_verify_rate_per_sec must be at least 4 times auth.failed_verify_per_iss_rate_per_sec".to_string());
    }
    if cfg.failed_verify_success_window_secs < 600 {
        out.push("auth.failed_verify_success_window_secs must be at least 600".to_string());
    }
    if cfg.failed_verify_state_max < 1000 {
        out.push("auth.failed_verify_state_max must be at least 1000".to_string());
    }
    if cfg.failed_verify_success_ips_max < 1 {
        out.push("auth.failed_verify_success_ips_max must be at least 1".to_string());
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::config::FileConfig;

    const SERVICE: &str = "did:web:appview.example.com";
    const MEMBER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const METHOD: &str = "app.bsky.feed.getTimeline";
    const NOW: u64 = 1_800_000_000;

    fn token(payload: &Value) -> String {
        let b64 = |v: &Value| URL_SAFE_NO_PAD.encode(serde_json::to_vec(v).expect("json"));
        format!("{}.{}.c2ln", b64(&json!({"alg": "ES256K", "typ": "JWT"})), b64(payload))
    }

    fn good() -> Value {
        json!({"iss": MEMBER, "aud": SERVICE, "lxm": METHOD, "iat": NOW - 5, "exp": NOW + 55})
    }

    fn check(payload: &Value) -> Option<Claims> {
        check_raw(&format!("Bearer {}", token(payload)))
    }

    fn check_raw(authorization: &str) -> Option<Claims> {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(NOW);
        precheck(
            authorization,
            &Did::new(SERVICE).expect("did"),
            METHOD,
            &JwtVerificationConfig::atproto_service_auth(),
            now,
        )
    }

    fn with(key: &str, value: Value) -> Value {
        let mut p = good();
        p[key] = value;
        p
    }

    fn without(key: &str) -> Value {
        let mut p = good();
        p.as_object_mut().expect("object").remove(key);
        p
    }

    #[test]
    fn the_precheck_accepts_what_the_verifier_accepts() {
        assert_eq!(check(&good()), Some(Claims { iss: MEMBER.into() }));
        // A PDS proxy sends a bare-DID `aud`; a client may name the service.
        assert!(check(&with("aud", json!(format!("{SERVICE}#bsky_appview")))).is_some());
        // Bearer prefixes the verifier strips, and a bare token.
        let t = token(&good());
        for raw in [format!("bearer {t}"), format!("BEARER {t}"), format!("  Bearer   {t} "), t.clone()] {
            assert!(check_raw(&raw).is_some(), "{raw}");
        }
        // Within the 30 s skew on both sides.
        assert!(check(&with("exp", json!(NOW - 30))).is_some());
        assert!(check(&with("iat", json!(NOW + 30))).is_some());
        assert!(check(&with("nbf", json!(NOW + 30))).is_some());
        // Exactly the maximum window.
        let mut hour = with("iat", json!(NOW - 1800));
        hour["exp"] = json!(NOW + 1800);
        assert!(check(&hour).is_some());
        // Ignored, as the verifier ignores them.
        assert!(check(&with("nbf", json!("soon"))).is_some());
        assert!(check(&with("nonce", json!(7))).is_some());
        assert!(check(&with("nonce", json!(URL_SAFE_NO_PAD.encode([7u8; 16])))).is_some());
    }

    #[test]
    fn the_precheck_rejects_what_the_verifier_rejects_after_the_signature() {
        let cases = [
            ("no iss", without("iss")),
            ("iss not a DID", with("iss", json!("alice.example.com"))),
            ("no aud", without("aud")),
            ("aud array", with("aud", json!([SERVICE]))),
            ("another service", with("aud", json!("did:web:feeds.example.com"))),
            ("empty fragment", with("aud", json!(format!("{SERVICE}#")))),
            ("another fragment", with("aud", json!(format!("{SERVICE}#bsky_notif")))),
            ("no lxm", without("lxm")),
            ("another lxm", with("lxm", json!("app.bsky.feed.getPosts"))),
            ("lxm not a string", with("lxm", json!(1))),
            ("no iat", without("iat")),
            ("iat a string", with("iat", json!("1"))),
            ("iat negative", with("iat", json!(-1))),
            ("no exp", without("exp")),
            ("expired past the skew", with("exp", json!(NOW - 31))),
            ("iat in the future past the skew", with("iat", json!(NOW + 31))),
            ("nbf in the future past the skew", with("nbf", json!(NOW + 31))),
            ("iat = now - 4000 (window too long)", with("iat", json!(NOW - 4000))),
            ("exp too large to be a time", with("exp", json!(u64::MAX))),
            ("nonce not 16 bytes", with("nonce", json!(URL_SAFE_NO_PAD.encode([7u8; 8])))),
            ("nonce not base64url", with("nonce", json!("!!"))),
        ];
        for (name, payload) in cases {
            assert_eq!(check(&payload), None, "{name}");
        }
        // Not a JWT at all.
        for raw in ["", "Bearer ", "Bearer a.b", "Bearer a.b.c.d", "Bearer a.!!.c", "Bearer a.e30.c"] {
            assert_eq!(check_raw(raw), None, "{raw:?}");
        }
    }

    fn limits(toml: &str) -> VerifyLimits {
        VerifyLimits::from_config(&FileConfig::parse(toml).expect("parse").auth)
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("ip")
    }

    fn ticket(a: Admission) -> Ticket {
        match a {
            Admission::Verify(t) => t,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn failures_are_limited_per_pair_and_never_spend_the_member_ceiling() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        // Pair: burst 5. Member ceiling: burst 2.
        let l = limits("[auth]\nmember_verify_rate_per_sec = 1.0\nmember_verify_burst = 2\n");
        let now = Instant::now();
        let pds = ip("203.0.113.1");
        // Five failing tokens naming the member are verified; the sixth is
        // sent to the IP key without verification.
        for _ in 0..5 {
            let t = ticket(l.admit_at(pds, MEMBER, 1, now));
            l.settle_at(t, Outcome::NotVerified, now);
        }
        assert!(matches!(l.admit_at(pds, MEMBER, 1, now), Admission::Unverified));
        // None of that spent the member's ceiling: from another IP key the
        // member still has both of its tokens.
        let home = ip("198.51.100.7");
        let a = ticket(l.admit_at(home, MEMBER, 1, now));
        let b = ticket(l.admit_at(home, MEMBER, 1, now));
        // Its own ceiling empty: 429, and no charge to either key.
        assert!(matches!(l.admit_at(home, MEMBER, 1, now), Admission::Limited { retry_after } if retry_after == Duration::from_secs(1)));
        l.settle_at(a, Outcome::SignatureVerified { fresh: true }, now);
        l.settle_at(b, Outcome::SignatureVerified { fresh: true }, now);
        // Successes keep the member token: still empty until it refills.
        assert!(matches!(l.admit_at(home, MEMBER, 1, now), Admission::Limited { .. }));
        let _ = ticket(l.admit_at(home, MEMBER, 1, now + Duration::from_secs(1)));
        // Another member on the first PDS has its own pair bucket.
        let _ = ticket(l.admit_at(pds, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", 1, now));
        // The two per-member 429s are counted (R3-243).
        let text = handle.render();
        assert_eq!(crate::test_support::series(&text, METRIC_MEMBER_EXHAUSTED, &[]), Some(2.0), "{text}");
    }

    #[test]
    fn a_recent_success_exempts_the_member_for_the_same_key_only() {
        let l = limits("");
        let now = Instant::now();
        let pds = ip("203.0.113.1");
        let t = ticket(l.admit_at(pds, MEMBER, 1, now));
        l.settle_at(t, Outcome::SignatureVerified { fresh: true }, now);
        // A forger naming the member from the same IP key cannot empty its
        // pair bucket: 50 failures, and it is still verified.
        for _ in 0..50 {
            let t = ticket(l.admit_at(pds, MEMBER, 1, now));
            l.settle_at(t, Outcome::NotVerified, now);
        }
        assert!(matches!(l.admit_at(pds, MEMBER, 1, now), Admission::Verify(_)));
        // After the member's key changes, the record names the old key and
        // the pair bucket applies again: burst 5.
        for _ in 0..5 {
            let t = ticket(l.admit_at(pds, MEMBER, 2, now));
            l.settle_at(t, Outcome::NotVerified, now);
        }
        assert!(matches!(l.admit_at(pds, MEMBER, 2, now), Admission::Unverified));
        // A stale-key match is no success.
        let other = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let t = ticket(l.admit_at(pds, other, 1, now));
        l.settle_at(t, Outcome::SignatureVerified { fresh: false }, now);
        assert!(!l.recent_success(other, pds, 1, now));
        // The exemption ends with the window.
        assert!(l.recent_success(MEMBER, pds, 1, now + Duration::from_secs(86_399)));
        assert!(!l.recent_success(MEMBER, pds, 1, now + Duration::from_secs(86_400)));
    }

    #[test]
    fn only_failures_drain_the_ip_ceiling() {
        // IP ceiling: burst 3. Failure buckets wide open.
        let l = limits(concat!(
            "[auth]\nverify_rate_per_sec = 1.0\nverify_burst = 3\n",
            "failed_verify_rate_per_sec = 1000.0\nfailed_verify_burst = 1000\n",
            "failed_verify_per_iss_rate_per_sec = 100.0\nfailed_verify_per_iss_burst = 1000\n",
        ));
        let now = Instant::now();
        let pds = ip("203.0.113.1");
        // Valid traffic, however heavy, returns its token every time.
        for _ in 0..100 {
            let t = ticket(l.admit_at(pds, MEMBER, 1, now));
            l.settle_at(t, Outcome::SignatureVerified { fresh: true }, now);
        }
        // Three failures (of another member) keep theirs.
        let other = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        for _ in 0..3 {
            let t = ticket(l.admit_at(pds, other, 1, now));
            l.settle_at(t, Outcome::NotVerified, now);
        }
        // The ceiling is empty: the member goes to the IP key unverified,
        // and its own token came back.
        assert!(matches!(l.admit_at(pds, MEMBER, 1, now), Admission::Unverified));
        let elsewhere = ip("198.51.100.7");
        for _ in 0..3 {
            let t = ticket(l.admit_at(elsewhere, MEMBER, 1, now));
            l.settle_at(t, Outcome::SignatureVerified { fresh: true }, now);
        }
    }

    #[test]
    fn success_records_are_bounded_per_member() {
        let l = limits("[auth]\nfailed_verify_success_ips_max = 2\n");
        let now = Instant::now();
        for (i, addr) in ["203.0.113.1", "203.0.113.2", "203.0.113.3"].into_iter().enumerate() {
            let at = now + Duration::from_secs(i as u64);
            let t = ticket(l.admit_at(ip(addr), MEMBER, 1, at));
            l.settle_at(t, Outcome::SignatureVerified { fresh: true }, at);
        }
        let later = now + Duration::from_secs(10);
        assert!(!l.recent_success(MEMBER, ip("203.0.113.1"), 1, later), "the least recent was replaced");
        assert!(l.recent_success(MEMBER, ip("203.0.113.2"), 1, later));
        assert!(l.recent_success(MEMBER, ip("203.0.113.3"), 1, later));
        l.forget(MEMBER);
        assert!(!l.recent_success(MEMBER, ip("203.0.113.3"), 1, later));
    }

    #[tokio::test]
    async fn a_request_is_charged_to_a_member_only_when_its_key_verifies() {
        use crate::kryphocron_glue::jwt::tests::{StubFetcher, mint, signing_key, verifier};
        use crate::kryphocron_glue::service_key::multikey;

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        // The verifier's own fetcher must never be asked: the gate uses the
        // in-memory document only.
        let fetcher = StubFetcher::default();
        let (v, _drainer) = verifier(fetcher.clone(), 100, 16);
        let key = signing_key(3);
        let compressed = key.verifying_key().to_encoded_point(true).as_bytes().to_vec();
        let doc = json!({"id": MEMBER, "verificationMethod": [{
            "id": format!("{MEMBER}#atproto"), "type": "Multikey", "controller": MEMBER,
            "publicKeyMultibase": multikey(&compressed),
        }]});
        let backstop = Backstop {
            limits: limits(""),
            keys: MemberKeys::default(),
        };
        let now = chrono::Utc::now();
        backstop.keys.insert(MEMBER, &doc, now);
        let pds = ip("203.0.113.1");
        let select = |authorization: Option<String>, method: &'static str| {
            let (backstop, v) = (backstop.clone(), v.clone());
            async move {
                match backstop.select(&v, authorization.as_deref(), Some(method), pds).await {
                    Selection::Charge(s) => s,
                    Selection::Limited { .. } => panic!("limited"),
                }
            }
        };
        let member = ChargeKey::member(MEMBER);
        let by_ip = ChargeKey::ip(pds);

        // A fresh key authenticates: charged by DID, and the caller is
        // handed on so the handler does not verify again.
        let good = format!("Bearer {}", mint(&key, MEMBER, SERVICE, Some(METHOD)));
        let s = select(Some(good.clone()), METHOD).await;
        assert_eq!(s.req.key(), &member);
        assert_eq!(s.verified(&good).map(|c| c.did), Some(MEMBER.to_string()));
        assert!(s.verified("Bearer other").is_none());

        // A forged token naming the member: the IP key.
        let forged = format!("Bearer {}", mint(&signing_key(9), MEMBER, SERVICE, Some(METHOD)));
        let s = select(Some(forged.clone()), METHOD).await;
        assert_eq!(s.req.key(), &by_ip);
        assert!(s.verified(&forged).is_none());

        // A token the member signed for another method or another service
        // fails the pre-check: the IP key, and no verification at all.
        let s = select(Some(good.clone()), "app.bsky.feed.getPosts").await;
        assert_eq!(s.req.key(), &by_ip);
        let elsewhere = format!("Bearer {}", mint(&key, MEMBER, "did:web:feeds.example.com", Some(METHOD)));
        assert_eq!(select(Some(elsewhere), METHOD).await.req.key(), &by_ip);
        // Both are counted; the forged token passed the pre-check.
        let text = handle.render();
        assert_eq!(crate::test_support::series(&text, METRIC_PRECHECK_FAILED, &[]), Some(2.0), "{text}");

        // Anonymous, and a non-member with a valid token of its own.
        assert_eq!(select(None, METHOD).await.req.key(), &by_ip);
        let outsider = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let theirs = format!("Bearer {}", mint(&signing_key(4), outsider, SERVICE, Some(METHOD)));
        assert_eq!(select(Some(theirs), METHOD).await.req.key(), &by_ip);

        // A stale key (2 h old) only selects the member: not verified.
        backstop.keys.insert(MEMBER, &doc, now - chrono::Duration::hours(2));
        let s = select(Some(good.clone()), METHOD).await;
        assert_eq!(s.req.key(), &member);
        assert!(s.verified(&good).is_none());

        // A key older than a day is no key.
        backstop.keys.insert(MEMBER, &doc, now - chrono::Duration::hours(25));
        assert_eq!(select(Some(good), METHOD).await.req.key(), &by_ip);

        assert_eq!(fetcher.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn settings_are_validated() {
        let check = |toml: &str| {
            let cfg = FileConfig::parse(toml).expect("parse");
            let mut out = Vec::new();
            problems(&cfg.auth, &mut out);
            out
        };
        assert!(check("").is_empty());
        for (toml, needle) in [
            ("[auth]\nverify_rate_per_sec = 0.0\n", "auth.verify"),
            ("[auth]\nmember_verify_burst = 0\n", "auth.member_verify"),
            ("[auth]\nfailed_verify_per_iss_rate_per_sec = 3.0\n", "at least 4 times"),
            ("[auth]\nfailed_verify_success_window_secs = 60\n", "success_window_secs"),
            ("[auth]\nfailed_verify_state_max = 10\n", "state_max"),
            ("[auth]\nfailed_verify_success_ips_max = 0\n", "success_ips_max"),
        ] {
            let out = check(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        }
    }
}
