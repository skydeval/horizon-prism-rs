//! `app.nearhorizon.actor.getMembership` (private posting §4.1, P-10): the
//! caller's membership, and the feature families this build serves: the
//! private ones (with `private.anyPds` while `private_any_pds.enabled`),
//! then Thread Lens's, read per call from the live
//! `threadlens.access` (Thread Lens amendment 1 §0A).
//!
//! - Query (GET), authentication required, no parameters.
//! - Rate class: the `private` per-DID bucket. There is no member gate:
//!   the answer is the gate's own state.
//! - It enrolls an eligible caller exactly as `getTimeline` does: the same
//!   checks and network calls, through the enroller's **public** intent
//!   (`aggregations::enroll`). It is in `KEPT_METHODS`, so it answers, and
//!   enrolls, in every public-indexing mode.
//! - Output `{status, features, permissions, validForSecs}`, built in
//!   `hydration`, with `Cache-Control: private, no-store` (fast-reads design
//!   §4.1): `validForSecs` is how long the client may act on the answer,
//!   from when it received it; reads are still authorized per request.
//!   `indexing` carries
//!   `Retry-After` (`private_read.membership_retry_after_secs`, a polling
//!   hint). The hourly enrolment cap is `429 RateLimitExceeded` with
//!   `Retry-After: 60`. Every other refusal is `notEligible`, which can be
//!   temporary: a cached refusal expires after
//!   `community.enroll_refusal_cache_secs`.
//! - It reads no private table and renders no private view, so it has no
//!   floor: the answer concerns only the caller.

use std::net::SocketAddr;

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::header::{CACHE_CONTROL as CACHE_CONTROL_HEADER, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::AppendHeaders;
use serde_json::{Value, json};

use crate::admin::settings::LiveSettings;
use crate::aggregations::enroll::{Enrollment, REFUSED_RATE};
use crate::config::{CommunityConfig, PrivateAnyPdsConfig, PrivateReadConfig, ThreadLensAccess};
use crate::hydration::PublicEmit;
use crate::hydration::hint::{MembershipStatus, membership};
use crate::private_tier::{DisabledPrivateTier, PrivateTier};
use crate::xrpc::AppState;
use crate::xrpc::auth::{authenticate, member_state, request_ip};
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::Family;

pub const GET_MEMBERSHIP: &str = "app.nearhorizon.actor.getMembership";
/// `Retry-After` on the hourly enrolment cap's 429 (§4.1).
pub const ENROLL_CAP_RETRY_AFTER_SECS: u64 = 60;

/// Thread Lens is served (Thread Lens amendment 1 §0A): its routes are
/// registered in every build and every public-indexing mode. It does not
/// depend on Farsight: without it every lens answer says `bridging: "off"`.
pub const FEATURE_THREAD_LENS: &str = "threadLens";
/// Thread Lens serves every signed-in account (`threadlens.access =
/// "signed_in"`), a non-member as a visitor.
pub const FEATURE_THREAD_LENS_SIGNED_IN: &str = "threadLens.signedIn";

/// Private posts from any PDS are fully served, and a plaintext one is
/// refused (private-encode design §7, S4): listed after the private tier's
/// own values while `private_any_pds.enabled`.
pub const FEATURE_ANY_PDS: &str = "private.anyPds";

/// What `getMembership` answers with besides the caller's state.
#[derive(Debug, Clone)]
pub struct MembershipSettings {
    /// [`PrivateTier::features`]: none when the private tier is disabled.
    pub private_features: &'static [&'static str],
    /// `private_any_pds.enabled`: [`FEATURE_ANY_PDS`] is listed, with the
    /// private values.
    pub any_pds: bool,
    /// `private_read.membership_retry_after_secs`.
    pub retry_after_secs: u64,
    /// `private_read.membership_valid_for_secs`: a `member` answer's
    /// `validForSecs`.
    pub valid_for_secs: u64,
    /// `community.enroll_refusal_cache_secs`: how long a refusal is
    /// remembered, so a `notEligible` answer holds at most that long.
    pub refusal_cache_secs: u64,
    /// The live settings, read on every call for `threadlens.access`.
    pub live: LiveSettings,
    /// `community.reindex_retry_secs` (degraded-mode design §3.5): the
    /// `Reindexing` answers' `retryAfter` and `reindexing`'s validity.
    pub reindex_retry_secs: u64,
}

impl MembershipSettings {
    /// With the live values of the default configuration; `serve` passes
    /// its own handle through [`Self::with_live`].
    #[must_use]
    pub fn new(private: &dyn PrivateTier, cfg: &PrivateReadConfig) -> Self {
        Self {
            private_features: private.features(),
            any_pds: PrivateAnyPdsConfig::default().enabled,
            retry_after_secs: cfg.membership_retry_after_secs,
            valid_for_secs: cfg.membership_valid_for_secs,
            refusal_cache_secs: CommunityConfig::default().enroll_refusal_cache_secs,
            live: LiveSettings::default(),
            reindex_retry_secs: CommunityConfig::default().reindex_retry_secs,
        }
    }

    /// `community.reindex_retry_secs` from the config.
    #[must_use]
    pub fn with_reindex_retry(mut self, secs: u64) -> Self {
        self.reindex_retry_secs = secs;
        self
    }

    /// `community.enroll_refusal_cache_secs` from the config.
    #[must_use]
    pub fn with_refusal_cache(mut self, secs: u64) -> Self {
        self.refusal_cache_secs = secs;
        self
    }

    /// How long the client may act on an answer of `status`, from when it
    /// received it (fast-reads design §4.1): `member` the configured
    /// validity; `indexing` the `Retry-After` hint, so the client polls as
    /// before; `notEligible` no longer than the refusal is remembered.
    #[must_use]
    pub fn valid_for(&self, status: MembershipStatus) -> u64 {
        match status {
            MembershipStatus::Member => self.valid_for_secs,
            MembershipStatus::Indexing => self.retry_after_secs,
            MembershipStatus::Reindexing => self.reindex_retry_secs,
            MembershipStatus::NotEligible => self.valid_for_secs.min(self.refusal_cache_secs),
        }
    }

    /// `permissions` for a caller of `status` (fast-reads design §4.1), from
    /// the same state `features` is read from:
    /// - `privateRead`: the private read methods serve the caller now (a
    ///   ready member, and the private tier serves);
    /// - `privateIndexed`: private posts the caller writes are indexed and,
    ///   once it is ready, served (a member, ready or not, and the private
    ///   tier serves). It does not say a post without encoded content is
    ///   served: while `private.anyPds` is listed (`private_any_pds.enabled`)
    ///   such a post is refused for every reader (S4), so the client
    ///   encodes;
    /// - `threadLens`: Thread Lens serves the caller (a ready member, or
    ///   anyone signed in while `threadLens.signedIn` is listed).
    #[must_use]
    pub fn permissions(&self, status: MembershipStatus, features: &[&str]) -> Permissions {
        let private = !self.private_features.is_empty();
        // A member being re-indexed keeps a member's permissions (§3.5).
        let member = matches!(status, MembershipStatus::Member | MembershipStatus::Reindexing);
        Permissions {
            private_read: private && member,
            private_indexed: private && matches!(status, MembershipStatus::Member | MembershipStatus::Reindexing | MembershipStatus::Indexing),
            thread_lens: features.contains(&FEATURE_THREAD_LENS) && (member || features.contains(&FEATURE_THREAD_LENS_SIGNED_IN)),
        }
    }

    #[must_use]
    pub fn with_live(mut self, live: LiveSettings) -> Self {
        self.live = live;
        self
    }

    /// `private_any_pds.enabled` from the config.
    #[must_use]
    pub fn with_any_pds(mut self, enabled: bool) -> Self {
        self.any_pds = enabled;
        self
    }

    /// `features` as of this call: the private values (with
    /// [`FEATURE_ANY_PDS`] last while it is on), then Thread Lens's, from
    /// the live `threadlens.access`.
    #[must_use]
    pub fn features(&self) -> Vec<&'static str> {
        let mut out = self.private_features.to_vec();
        if self.any_pds && !self.private_features.is_empty() {
            out.push(FEATURE_ANY_PDS);
        }
        out.push(FEATURE_THREAD_LENS);
        if self.live.load().threadlens_access == ThreadLensAccess::SignedIn {
            out.push(FEATURE_THREAD_LENS_SIGNED_IN);
        }
        out
    }
}

/// A disabled private tier and the default config (tests).
impl Default for MembershipSettings {
    fn default() -> Self {
        Self::new(&DisabledPrivateTier, &PrivateReadConfig::default())
    }
}

/// `getMembership`'s `permissions` (fast-reads design §4.1, C-7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Permissions {
    pub private_read: bool,
    pub private_indexed: bool,
    pub thread_lens: bool,
}

/// `Cache-Control` on every answer (fast-reads design §4.1, C-5): it varies
/// by caller and the validity is in the body, so no cache keeps it.
pub const CACHE_CONTROL: &str = "private, no-store";

/// The handler's response: the view, with `Cache-Control`, and
/// `Retry-After` on `indexing`.
pub type MembershipResult = Result<(AppendHeaders<Vec<(HeaderName, HeaderValue)>>, Json<PublicEmit<Value>>), XrpcError>;

pub async fn get_membership(State(s): State<AppState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> MembershipResult {
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = authenticate(&s, authorization, GET_MEMBERSHIP).await?;
    let ip = request_ip(&s, &headers, peer);
    s.limits.charge(GET_MEMBERSHIP, Family::Private, Some(&caller.did), ip)?;
    let enrollment = match &s.enroller {
        Some(e) => e.check(&caller.did).await?,
        None => member_state(&s, &caller.did).await?,
    };
    let status = match enrollment {
        Enrollment::Member { ready: true, .. } => MembershipStatus::Member,
        Enrollment::Member { ready: false, reindexing: true } => MembershipStatus::Reindexing,
        Enrollment::Member { ready: false, .. } | Enrollment::Enrolled | Enrollment::Purging => MembershipStatus::Indexing,
        Enrollment::Refused(REFUSED_RATE) => {
            return Err(XrpcError::new(StatusCode::TOO_MANY_REQUESTS, "RateLimitExceeded", "Rate Limit Exceeded")
                .with_retry_after(ENROLL_CAP_RETRY_AFTER_SECS));
        }
        Enrollment::Refused(_) => MembershipStatus::NotEligible,
    };
    let mut headers = vec![(CACHE_CONTROL_HEADER, HeaderValue::from_static(CACHE_CONTROL))];
    if status == MembershipStatus::Indexing {
        headers.push((RETRY_AFTER, HeaderValue::from(s.membership.retry_after_secs)));
    }
    if status == MembershipStatus::Reindexing {
        metrics::counter!(crate::xrpc::errors::METRIC_REINDEXING, "site" => "membership").increment(1);
        headers.push((RETRY_AFTER, HeaderValue::from(s.membership.reindex_retry_secs)));
    }
    let features = s.membership.features();
    let permissions = s.membership.permissions(status, &features);
    let view = membership(
        status,
        &features,
        json!({
            "privateRead": permissions.private_read,
            "privateIndexed": permissions.private_indexed,
            "threadLens": permissions.thread_lens,
        }),
        s.membership.valid_for(status),
    );
    Ok((AppendHeaders(headers), Json(view)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::aggregations::enroll::AutoEnroller;
    use crate::config::AppConfig;
    use crate::honest_501::PublicMessages;
    use crate::hydration::testing::{exec, hydrator, set_mode};
    use crate::identity::{IdentityResolver, IdentitySettings};
    use crate::indexing::PublicMode;
    use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
    use crate::net::{Intent, Transports};
    use crate::private_tier::{FEATURES, KryphocronPrivateTier};
    use crate::test_support::fresh_db;
    use crate::xrpc::RateLimits;
    use crate::xrpc::router::{FinalConfig, finalize, router};

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
    const DAVE: &str = "did:plc:dddddddddddddddddddddddd";
    const ERIN: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";

    fn app_config(toml: &str) -> AppConfig {
        let lookup = |var: &str| -> Option<String> {
            match var {
                "APP_PUBLIC_HOSTNAME" => Some(AUDIENCE_HOST.to_string()),
                "APP_DATABASE_URL" => Some("postgres://prism@db:5432/prism".to_string()),
                "APP_REDIS_URL" => Some("redis://redis:6379".to_string()),
                "APP_SERVICE_SIGNING_KEY_HEX" | "APP_CORRELATION_KEY_HEX" | "APP_PRIVATE_CURSOR_KEY_HEX" => Some("07".repeat(32)),
                _ => None,
            }
        };
        AppConfig::from_parts(toml, &lookup).expect("config")
    }

    async fn serve(state: AppState) -> String {
        let app: Router = finalize(router(state.clone()), state, FinalConfig { not_ready_retry_secs: 7, public: PublicMessages::default() });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("serve");
        });
        format!("http://{addr}")
    }

    /// Status, `Retry-After` and body of one call.
    async fn call(client: &reqwest::Client, base: &str, token: Option<&str>) -> (u16, Option<String>, Value) {
        let mut req = client.get(format!("{base}/xrpc/{GET_MEMBERSHIP}"));
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let r = req.send().await.expect("request");
        let status = r.status().as_u16();
        let retry = r.headers().get("retry-after").map(|v| v.to_str().expect("ascii").to_string());
        (status, retry, r.json().await.unwrap_or(Value::Null))
    }

    /// T-2 (private posting §4.1, P-10): each enrolment state's answer,
    /// through the real enroller and its public intent; 200 in every
    /// public-indexing mode; `features` lists the shipped strings, and none
    /// with the private tier disabled.
    #[tokio::test]
    async fn get_membership_answers_the_callers_state() {
        let Some(db) = fresh_db("xrpc_get_membership").await else {
            return;
        };
        let p = &db.pool;
        let cfg = app_config("[community]\nauto_enroll = true\nmax_auto_enroll_per_hour = 1\n");
        let hydrator = hydrator(p, ScopeSets::default());
        let scope = hydrator.scope().clone();
        let resolver = IdentityResolver::new(Transports::loopback_for_tests(), p.clone(), IdentitySettings::from_app(&cfg), Arc::new(scope.clone()), Intent::Internal);
        let enroller = AutoEnroller::new(&cfg, p.clone(), scope.clone(), Arc::new(KryphocronPrivateTier), resolver);
        let fetcher = StubFetcher::default();
        for (i, did) in [ALICE, BOB, CAROL, DAVE, ERIN].into_iter().enumerate() {
            fetcher.set_key(did, signing_key(u8::try_from(i).expect("small") + 3));
        }
        let (auth, _drainer) = verifier(fetcher, 100, 16);
        let limits = crate::config::FileConfig::parse(concat!(
            "[rate_limits.private]\nper_sec = 1000.0\nburst = 10000\n",
            "[rate_limits.global]\nper_sec = 1000.0\nburst = 10000\n",
        ))
        .expect("parse");
        let state = AppState {
            hydrator,
            auth: Arc::new(auth),
            limits: RateLimits::from_config(&limits.rate_limits),
            enroller: Some(enroller.clone()),
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: MembershipSettings::new(&KryphocronPrivateTier, &cfg.file.private_read),
        };
        let mut disabled = state.clone();
        disabled.membership = MembershipSettings::default();
        let base = serve(state).await;
        let disabled_base = serve(disabled).await;
        let client = reqwest::Client::new();
        let aud = format!("did:web:{AUDIENCE_HOST}");
        let token = |i: u8, did: &str| mint(&signing_key(i + 3), did, &aud, Some(GET_MEMBERSHIP));
        // The private values, then Thread Lens's (amendment 1 §0A).
        // `private.anyPds` follows them while `private_any_pds.enabled`
        // (private-encode design §7; on by default).
        let features = json!(FEATURES.iter().copied().chain([FEATURE_ANY_PDS, FEATURE_THREAD_LENS, FEATURE_THREAD_LENS_SIGNED_IN]).collect::<Vec<_>>());
        assert_eq!(json!(FEATURES), json!(["private", "private.embeds", "private.bookmarks", "private.gates", "private.interactions", "private.notifications", "private.freshIndex", "private.paging", "private.crossPdsReplies", "private.following", "private.search", "private.threadAudience", "private.mediaFilters", "private.notificationPreferences", "private.unreadReasons", "private.replyContext", "private.notificationSubjects", "private.notServed", "private.degraded", "private.reindexing"]));
        // T-N9 (private notification preferences design §8): the
        // preferences' feature followed every value before it; the fast
        // reads' values follow it (fast-reads design §5.3).
        assert_eq!(FEATURES.iter().position(|f| *f == "private.notificationPreferences"), Some(13));

        // Authentication is required.
        let (status, _, v) = call(&client, &base, None).await;
        assert_eq!((status, v["error"].as_str()), (401, Some("AuthRequired")));

        set_mode(&scope, PublicMode::Off).await;
        // A DID the enroller cannot confirm: notEligible, nothing inserted,
        // and the refusal cached for the next call.
        let (status, retry, v) = call(&client, &base, Some(&token(0, ALICE))).await;
        assert_eq!((status, retry, &v), (200, None, &answer("notEligible", &features, [false, false, true], 3600)));
        assert_eq!(enroller.cached_refusal_for_tests(ALICE).await, Some("unresolvable"));
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM members").fetch_one(p).await.expect("count");
        assert_eq!(rows, 0);

        // An eligible DID is enrolled (step 1's PDS check stands in): the
        // answer is `indexing` with the polling hint, then `member`.
        assert_eq!(enroller.admit_for_tests(BOB).await.expect("admit"), Enrollment::Enrolled);
        let (status, retry, v) = call(&client, &base, Some(&token(1, BOB))).await;
        assert_eq!((status, retry.as_deref(), &v), (200, Some("5"), &answer("indexing", &features, [false, true, true], 5)));
        exec(p, "UPDATE members SET ready_at = now() WHERE did = 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb'").await;
        // A ready member: `member` in every public-indexing mode.
        for mode in [PublicMode::Off, PublicMode::Draining, PublicMode::Held, PublicMode::Filling, PublicMode::On] {
            set_mode(&scope, mode).await;
            let (status, retry, v) = call(&client, &base, Some(&token(1, BOB))).await;
            assert_eq!((status, retry, &v), (200, None, &answer("member", &features, [true, true, true], 3600)), "{mode:?}");
        }
        set_mode(&scope, PublicMode::Off).await;

        // The hourly cap: refused as `rate`, never cached, and answered 429
        // with `Retry-After: 60`.
        assert_eq!(enroller.admit_for_tests(CAROL).await.expect("admit"), Enrollment::Refused(REFUSED_RATE));
        assert_eq!(enroller.cached_refusal_for_tests(CAROL).await, None);
        enroller.cache_refusal_for_tests(CAROL, REFUSED_RATE).await;
        let (status, retry, v) = call(&client, &base, Some(&token(2, CAROL))).await;
        assert_eq!((status, retry.as_deref(), v["error"].as_str()), (429, Some("60"), Some("RateLimitExceeded")));
        // Any other refusal is notEligible.
        enroller.cache_refusal_for_tests(DAVE, "max_members").await;
        let (status, _, v) = call(&client, &base, Some(&token(3, DAVE))).await;
        assert_eq!((status, &v["status"]), (200, &json!("notEligible")));

        // A member added earlier and not ready yet is `indexing`.
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:eeeeeeeeeeeeeeeeeeeeeeee', 'admin')").await;
        let (status, retry, v) = call(&client, &base, Some(&token(4, ERIN))).await;
        assert_eq!((status, retry.as_deref(), &v["status"]), (200, Some("5"), &json!("indexing")));

        // The private tier disabled: no private features; Thread Lens's.
        let (status, _, v) = call(&client, &disabled_base, Some(&token(1, BOB))).await;
        assert_eq!((status, &v), (200, &answer("member", &json!(["threadLens", "threadLens.signedIn"]), [false, false, true], 3600)));
        db.drop_db().await;
    }

    /// The whole answer: `status`, `features`, `permissions` as
    /// `[privateRead, privateIndexed, threadLens]`, `validForSecs`.
    fn answer(status: &str, features: &Value, [read, indexed, lens]: [bool; 3], valid_for: u64) -> Value {
        json!({
            "status": status,
            "features": features,
            "permissions": {"privateRead": read, "privateIndexed": indexed, "threadLens": lens},
            "validForSecs": valid_for,
        })
    }

    /// T-F5 (fast-reads design §4.1): `validForSecs` per status and setting
    /// (a refusal cached for less than the validity bounds `notEligible`'s),
    /// `permissions` per status, with `threadlens.access` `members` and
    /// `signed_in` and with the private tier disabled, and `Cache-Control:
    /// private, no-store` on every answer.
    #[tokio::test]
    async fn t_f5_validity_permissions_and_cache_control() {
        let Some(db) = fresh_db("xrpc_get_membership_validity").await else {
            return;
        };
        let p = &db.pool;
        let fetcher = StubFetcher::default();
        for (i, did) in [ALICE, BOB, CAROL].into_iter().enumerate() {
            fetcher.set_key(did, signing_key(u8::try_from(i).expect("small") + 3));
        }
        let (auth, _drainer) = verifier(fetcher, 100, 16);
        let live = crate::admin::settings::LiveSettings::default();
        let read = PrivateReadConfig { membership_valid_for_secs: 600, ..PrivateReadConfig::default() };
        let state = AppState {
            hydrator: hydrator(p, ScopeSets::default()),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: MembershipSettings::new(&KryphocronPrivateTier, &read).with_live(live.clone()).with_refusal_cache(300),
        };
        let mut disabled = state.clone();
        disabled.membership = MembershipSettings::default().with_live(live.clone());
        let base = serve(state).await;
        let disabled_base = serve(disabled).await;
        exec(p, "INSERT INTO members (did, source, ready_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin', now())").await;
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', 'admin')").await;
        let aud = format!("did:web:{AUDIENCE_HOST}");
        let client = reqwest::Client::new();
        let ask = |base: String, i: u8, did: &'static str| {
            let token = mint(&signing_key(i + 3), did, &aud, Some(GET_MEMBERSHIP));
            let client = client.clone();
            async move {
                let r = client.get(format!("{base}/xrpc/{GET_MEMBERSHIP}")).header("authorization", format!("Bearer {token}")).send().await.expect("request");
                let cache = r.headers().get("cache-control").map(|v| v.to_str().expect("ascii").to_string());
                let v: Value = r.json().await.expect("json");
                (cache, v["permissions"].clone(), v["validForSecs"].clone())
            }
        };
        let perms = |read: bool, indexed: bool, lens: bool| json!({"privateRead": read, "privateIndexed": indexed, "threadLens": lens});
        let cache = Some(CACHE_CONTROL.to_string());
        for (access, lens_for_others) in [(ThreadLensAccess::SignedIn, true), (ThreadLensAccess::Members, false)] {
            live.update(|v| v.threadlens_access = access);
            // A ready member: the configured validity.
            assert_eq!(ask(base.clone(), 0, ALICE).await, (cache.clone(), perms(true, true, true), json!(600)), "{access:?}");
            // Indexing: the polling hint; indexed, not readable yet.
            assert_eq!(ask(base.clone(), 1, BOB).await, (cache.clone(), perms(false, true, lens_for_others), json!(5)), "{access:?}");
            // Not eligible: no longer than the refusal is remembered.
            assert_eq!(ask(base.clone(), 2, CAROL).await, (cache.clone(), perms(false, false, lens_for_others), json!(300)), "{access:?}");
            // The private tier disabled: nothing private, whatever the status.
            assert_eq!(ask(disabled_base.clone(), 0, ALICE).await, (cache.clone(), perms(false, false, true), json!(3600)), "{access:?}");
            assert_eq!(ask(disabled_base.clone(), 1, BOB).await.1, perms(false, false, lens_for_others), "{access:?}");
        }
        db.drop_db().await;
    }

    /// T-DM9 (degraded-mode design §3.5), `getMembership` and the
    /// member-only public gate, with and without an enroller: a member whose
    /// readiness was cleared after the first time is `reindexing` (member
    /// permissions, `validForSecs` and `Retry-After` the re-index interval)
    /// and gets `503 Reindexing` with `retryAfter` in the body and the
    /// header; a member never ready is `indexing` and `400 NotReady`; once
    /// ready again, `member` and 200.
    #[tokio::test]
    async fn tdm9_reindexing_membership_and_public_gate() {
        let Some(db) = fresh_db("xrpc_get_membership_reindexing").await else {
            return;
        };
        let p = &db.pool;
        let cfg = app_config("[community]\nauto_enroll = true\n");
        let fetcher = StubFetcher::default();
        for (i, did) in [ALICE, BOB].into_iter().enumerate() {
            fetcher.set_key(did, signing_key(u8::try_from(i).expect("small") + 3));
        }
        let (auth, _drainer) = verifier(fetcher, 100, 16);
        let hydrator = hydrator(p, ScopeSets::default());
        let scope = hydrator.scope().clone();
        let resolver = IdentityResolver::new(Transports::loopback_for_tests(), p.clone(), IdentitySettings::from_app(&cfg), Arc::new(scope.clone()), Intent::Internal);
        let enroller = AutoEnroller::new(&cfg, p.clone(), scope.clone(), Arc::new(KryphocronPrivateTier), resolver);
        let without = AppState {
            hydrator,
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: MembershipSettings::new(&KryphocronPrivateTier, &cfg.file.private_read).with_reindex_retry(30),
        };
        let mut with = without.clone();
        with.enroller = Some(enroller);
        let bases = [serve(without).await, serve(with).await];
        exec(p, "INSERT INTO members (did, source, ready_at, first_ready_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin', NULL, now() - interval '1 day')").await;
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', 'admin')").await;
        let aud = format!("did:web:{AUDIENCE_HOST}");
        let client = reqwest::Client::new();
        let features = json!(FEATURES.iter().copied().chain([FEATURE_ANY_PDS, FEATURE_THREAD_LENS, FEATURE_THREAD_LENS_SIGNED_IN]).collect::<Vec<_>>());
        let timeline = |base: &str, i: u8, did: &str| {
            let token = mint(&signing_key(i + 3), did, &aud, Some("app.bsky.feed.getTimeline"));
            let req = client.get(format!("{base}/xrpc/app.bsky.feed.getTimeline")).header("authorization", format!("Bearer {token}"));
            async move {
                let r = req.send().await.expect("request");
                let status = r.status().as_u16();
                let retry = r.headers().get("retry-after").map(|v| v.to_str().expect("ascii").to_string());
                (status, retry, r.text().await.expect("body"))
            }
        };
        for base in &bases {
            let alice = mint(&signing_key(3), ALICE, &aud, Some(GET_MEMBERSHIP));
            let (status, retry, v) = call(&client, base, Some(&alice)).await;
            assert_eq!((status, retry.as_deref(), &v), (200, Some("30"), &answer("reindexing", &features, [true, true, true], 30)));
            let (status, retry, body) = timeline(base, 0, ALICE).await;
            assert_eq!(status, 503, "{body}");
            assert_eq!(retry.as_deref(), Some("30"));
            let v: Value = serde_json::from_str(&body).expect("json");
            assert_eq!((&v["error"], &v["retryAfter"]), (&json!("Reindexing"), &json!(30)), "{body}");
            let bob = mint(&signing_key(4), BOB, &aud, Some(GET_MEMBERSHIP));
            let (status, retry, v) = call(&client, base, Some(&bob)).await;
            assert_eq!((status, retry.as_deref(), &v["status"]), (200, Some("5"), &json!("indexing")));
            let (status, retry, body) = timeline(base, 1, BOB).await;
            assert!(status == 400 && body.contains("\"NotReady\"") && !body.contains("retryAfter"), "{body}");
            assert_eq!(retry.as_deref(), Some("7"));
        }
        exec(p, "UPDATE members SET ready_at = now() WHERE did = 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa'").await;
        for base in &bases {
            let alice = mint(&signing_key(3), ALICE, &aud, Some(GET_MEMBERSHIP));
            let (_, _, v) = call(&client, base, Some(&alice)).await;
            assert_eq!(v["status"], "member");
            let (status, _, body) = timeline(base, 0, ALICE).await;
            assert_eq!(status, 200, "{body}");
        }
        db.drop_db().await;
    }

    /// `private.anyPds` is listed, last of the private values, exactly
    /// while `private_any_pds.enabled` and the private tier serves.
    #[test]
    fn any_pds_follows_its_switch() {
        let on = MembershipSettings::new(&KryphocronPrivateTier, &PrivateReadConfig::default());
        let mut want: Vec<&str> = FEATURES.to_vec();
        want.extend([FEATURE_ANY_PDS, FEATURE_THREAD_LENS, FEATURE_THREAD_LENS_SIGNED_IN]);
        assert_eq!(on.features(), want);
        let off = on.with_any_pds(false);
        assert!(!off.features().contains(&FEATURE_ANY_PDS));
        assert_eq!(off.features().len(), FEATURES.len() + 2);
        let disabled = MembershipSettings::default().with_any_pds(true);
        assert_eq!(disabled.features(), [FEATURE_THREAD_LENS, FEATURE_THREAD_LENS_SIGNED_IN]);
    }

    /// T-0 (Thread Lens amendment 1 §0A): `features` follows a live change
    /// of `threadlens.access` on a running server, with no restart, with
    /// the private tier enabled and disabled.
    #[tokio::test]
    async fn thread_lens_features_follow_the_live_access() {
        let Some(db) = fresh_db("xrpc_get_membership_threadlens").await else {
            return;
        };
        let fetcher = StubFetcher::default();
        fetcher.set_key(ALICE, signing_key(3));
        let (auth, _drainer) = verifier(fetcher, 10, 4);
        let live = crate::admin::settings::LiveSettings::default();
        let state = AppState {
            hydrator: hydrator(&db.pool, ScopeSets::default()),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: MembershipSettings::new(&KryphocronPrivateTier, &PrivateReadConfig::default()).with_live(live.clone()),
        };
        let mut disabled = state.clone();
        disabled.membership = MembershipSettings::default().with_live(live.clone());
        let base = serve(state).await;
        let disabled_base = serve(disabled).await;
        let client = reqwest::Client::new();
        let token = mint(&signing_key(3), ALICE, &format!("did:web:{AUDIENCE_HOST}"), Some(GET_MEMBERSHIP));
        let features = |extra: &[&str]| json!(FEATURES.iter().copied().chain([FEATURE_ANY_PDS]).chain(extra.iter().copied()).collect::<Vec<_>>());
        let both = [FEATURE_THREAD_LENS, FEATURE_THREAD_LENS_SIGNED_IN];

        live.update(|v| v.threadlens_access = ThreadLensAccess::SignedIn);
        let (_, _, v) = call(&client, &base, Some(&token)).await;
        assert_eq!(v["features"], features(&both), "{v}");
        let (_, _, v) = call(&client, &disabled_base, Some(&token)).await;
        assert_eq!(v["features"], json!(both), "{v}");

        // Members only: Thread Lens is still served, to ready members.
        live.update(|v| v.threadlens_access = ThreadLensAccess::Members);
        let (_, _, v) = call(&client, &base, Some(&token)).await;
        assert_eq!(v["features"], features(&[FEATURE_THREAD_LENS]), "{v}");
        let (_, _, v) = call(&client, &disabled_base, Some(&token)).await;
        assert_eq!(v["features"], json!(["threadLens"]), "{v}");

        // And back, still without a restart.
        live.update(|v| v.threadlens_access = ThreadLensAccess::SignedIn);
        let (_, _, v) = call(&client, &base, Some(&token)).await;
        assert_eq!(v["features"], features(&both), "{v}");
        db.drop_db().await;
    }

    /// T-A9 (admin-ui design §15): `community.auto_enroll` off → on applies
    /// at the next call although a refusal was cached; `max_members` too.
    #[tokio::test]
    async fn auto_enroll_follows_the_live_settings() {
        let Some(db) = fresh_db("xrpc_enroll_live").await else {
            return;
        };
        let p = &db.pool;
        let cfg = app_config("[community]\nauto_enroll = false\n");
        let scope = hydrator(p, ScopeSets::default()).scope().clone();
        let resolver = IdentityResolver::new(Transports::loopback_for_tests(), p.clone(), IdentitySettings::from_app(&cfg), Arc::new(scope.clone()), Intent::Internal);
        let live = crate::admin::settings::LiveSettings::new(&cfg.file);
        let enroller = AutoEnroller::new(&cfg, p.clone(), scope, Arc::new(KryphocronPrivateTier), resolver).with_live(live.clone());
        assert_eq!(enroller.check(ALICE).await.expect("check"), Enrollment::Refused("disabled"));
        assert_eq!(enroller.cached_refusal_for_tests(ALICE).await, Some("disabled"));
        live.update(|v| v.auto_enroll = true);
        // The cached refusal is gone; the account goes on to the PDS check,
        // which no server answers here.
        assert_eq!(enroller.check(ALICE).await.expect("check"), Enrollment::Refused("unresolvable"));
        live.update(|v| v.max_members = 0);
        assert_eq!(enroller.admit_for_tests(BOB).await.expect("admit"), Enrollment::Refused("max_members"));
        live.update(|v| v.max_members = 10);
        assert_eq!(enroller.admit_for_tests(CAROL).await.expect("admit"), Enrollment::Enrolled);
        db.drop_db().await;
    }

    /// T-F1 (fast-reads design §2.2): `getMembership` called directly, with
    /// a token from `getServiceAuth`: a bare `aud` and `#bsky_appview` with
    /// Horizon's 59-minute window answer; another fragment, a window past an
    /// hour and another method's token are 401.
    #[tokio::test]
    async fn direct_tokens_reach_get_membership() {
        use crate::kryphocron_glue::jwt::tests::mint_claims;

        let Some(db) = fresh_db("xrpc_get_membership_direct").await else {
            return;
        };
        let fetcher = StubFetcher::default();
        fetcher.set_key(ALICE, signing_key(3));
        let (auth, _drainer) = verifier(fetcher, 10, 4);
        let state = AppState {
            hydrator: hydrator(&db.pool, ScopeSets::default()),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: MembershipSettings::default(),
        };
        let base = serve(state).await;
        let client = reqwest::Client::new();
        let host = format!("did:web:{AUDIENCE_HOST}");
        let now = chrono::Utc::now().timestamp();
        let token = |aud: &str, lxm: &str, window: i64| {
            mint_claims(&signing_key(3), &json!({"iss": ALICE, "aud": aud, "lxm": lxm, "iat": now, "exp": now + window}))
        };
        let fragment = format!("{host}#bsky_appview");
        let other = format!("{host}#atproto_labeler");
        for (aud, lxm, window, status) in [
            (host.as_str(), GET_MEMBERSHIP, 60, 200),
            (fragment.as_str(), GET_MEMBERSHIP, 3_540, 200),
            (fragment.as_str(), GET_MEMBERSHIP, 3_600, 200),
            (fragment.as_str(), GET_MEMBERSHIP, 3_601, 401),
            (other.as_str(), GET_MEMBERSHIP, 3_540, 401),
            (fragment.as_str(), "app.nearhorizon.private.getPosts", 3_540, 401),
        ] {
            let (got, _, v) = call(&client, &base, Some(&token(aud, lxm, window))).await;
            assert_eq!(got, status, "{aud} {lxm} {window}: {v}");
        }
        db.drop_db().await;
    }

    /// With no enroller, the gate's own state: a non-member is notEligible.
    #[tokio::test]
    async fn without_an_enroller_a_non_member_is_not_eligible() {
        let Some(db) = fresh_db("xrpc_get_membership_no_enroller").await else {
            return;
        };
        let fetcher = StubFetcher::default();
        fetcher.set_key(ALICE, signing_key(3));
        let (auth, _drainer) = verifier(fetcher, 10, 4);
        let state = AppState {
            hydrator: hydrator(&db.pool, ScopeSets::default()),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: MembershipSettings::default(),
        };
        let base = serve(state).await;
        let token = mint(&signing_key(3), ALICE, &format!("did:web:{AUDIENCE_HOST}"), Some(GET_MEMBERSHIP));
        let (status, _, v) = call(&reqwest::Client::new(), &base, Some(&token)).await;
        assert_eq!((status, &v), (200, &answer("notEligible", &json!(["threadLens", "threadLens.signedIn"]), [false, false, true], 3600)));
        db.drop_db().await;
    }
}
