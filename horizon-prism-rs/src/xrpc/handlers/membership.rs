//! `app.nearhorizon.actor.getMembership` (private posting §4.1, P-10): the
//! caller's membership, and the private families this build serves.
//!
//! - Query (GET), authentication required, no parameters.
//! - Rate class: the `private` per-DID bucket. There is no member gate:
//!   the answer is the gate's own state.
//! - It enrolls an eligible caller exactly as `getTimeline` does: the same
//!   checks and network calls, through the enroller's **public** intent
//!   (`aggregations::enroll`). It is in `KEPT_METHODS`, so it answers, and
//!   enrolls, in every public-indexing mode.
//! - Output `{status, features}`, built in `hydration`. `indexing` carries
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
use axum::http::header::RETRY_AFTER;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::AppendHeaders;
use serde_json::Value;

use crate::aggregations::enroll::{Enrollment, REFUSED_RATE};
use crate::config::PrivateReadConfig;
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

/// What `getMembership` answers with besides the caller's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipSettings {
    /// [`PrivateTier::features`]: none when the private tier is disabled.
    pub features: &'static [&'static str],
    /// `private_read.membership_retry_after_secs`.
    pub retry_after_secs: u64,
}

impl MembershipSettings {
    #[must_use]
    pub fn new(private: &dyn PrivateTier, cfg: &PrivateReadConfig) -> Self {
        Self {
            features: private.features(),
            retry_after_secs: cfg.membership_retry_after_secs,
        }
    }
}

/// A disabled private tier and the default config (tests).
impl Default for MembershipSettings {
    fn default() -> Self {
        Self::new(&DisabledPrivateTier, &PrivateReadConfig::default())
    }
}

/// The handler's response: the view, with `Retry-After` on `indexing`.
pub type MembershipResult = Result<(AppendHeaders<Option<(HeaderName, HeaderValue)>>, Json<PublicEmit<Value>>), XrpcError>;

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
        Enrollment::Member { ready: true } => MembershipStatus::Member,
        Enrollment::Member { ready: false } | Enrollment::Enrolled | Enrollment::Purging => MembershipStatus::Indexing,
        Enrollment::Refused(REFUSED_RATE) => {
            return Err(XrpcError::new(StatusCode::TOO_MANY_REQUESTS, "RateLimitExceeded", "Rate Limit Exceeded")
                .with_retry_after(ENROLL_CAP_RETRY_AFTER_SECS));
        }
        Enrollment::Refused(_) => MembershipStatus::NotEligible,
    };
    let retry = (status == MembershipStatus::Indexing).then(|| (RETRY_AFTER, HeaderValue::from(s.membership.retry_after_secs)));
    Ok((AppendHeaders(retry), Json(membership(status, s.membership.features))))
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
        let cfg = app_config("[community]\nmax_auto_enroll_per_hour = 1\n");
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
        let features = json!(FEATURES);
        assert_eq!(features, json!(["private", "private.embeds", "private.bookmarks", "private.gates", "private.interactions", "private.notifications", "private.freshIndex", "private.paging", "private.crossPdsReplies", "private.following", "private.search", "private.threadAudience", "private.mediaFilters"]));
        // T-M10 (private-media-filters design §6): the media filters' feature
        // is listed last.
        assert_eq!(FEATURES.last(), Some(&"private.mediaFilters"));

        // Authentication is required.
        let (status, _, v) = call(&client, &base, None).await;
        assert_eq!((status, v["error"].as_str()), (401, Some("AuthRequired")));

        set_mode(&scope, PublicMode::Off).await;
        // A DID the enroller cannot confirm: notEligible, nothing inserted,
        // and the refusal cached for the next call.
        let (status, retry, v) = call(&client, &base, Some(&token(0, ALICE))).await;
        assert_eq!((status, retry, &v), (200, None, &json!({"status": "notEligible", "features": features})));
        assert_eq!(enroller.cached_refusal_for_tests(ALICE).await, Some("unresolvable"));
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM members").fetch_one(p).await.expect("count");
        assert_eq!(rows, 0);

        // An eligible DID is enrolled (step 1's PDS check stands in): the
        // answer is `indexing` with the polling hint, then `member`.
        assert_eq!(enroller.admit_for_tests(BOB).await.expect("admit"), Enrollment::Enrolled);
        let (status, retry, v) = call(&client, &base, Some(&token(1, BOB))).await;
        assert_eq!((status, retry.as_deref(), &v), (200, Some("5"), &json!({"status": "indexing", "features": features})));
        exec(p, "UPDATE members SET ready_at = now() WHERE did = 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb'").await;
        // A ready member: `member` in every public-indexing mode.
        for mode in [PublicMode::Off, PublicMode::Draining, PublicMode::Held, PublicMode::Filling, PublicMode::On] {
            set_mode(&scope, mode).await;
            let (status, retry, v) = call(&client, &base, Some(&token(1, BOB))).await;
            assert_eq!((status, retry, &v), (200, None, &json!({"status": "member", "features": features})), "{mode:?}");
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

        // The private tier disabled: no features.
        let (status, _, v) = call(&client, &disabled_base, Some(&token(1, BOB))).await;
        assert_eq!((status, &v), (200, &json!({"status": "member", "features": []})));
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
        assert_eq!((status, &v), (200, &json!({"status": "notEligible", "features": []})));
        db.drop_db().await;
    }
}
