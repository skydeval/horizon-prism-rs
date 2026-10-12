//! `GET /xrpc/_health` (design-v2 §7.1). Anonymous callers get
//! `{"ok","version"}`; a caller whose service JWT verifies (`lxm` `_health`,
//! no membership check) gets the detail:
//! `{"ok","version","db","redis","audit_ok","watermarks":{label: rfc3339|null},
//! "private_watermark","gap_blocked"}`. 200 when db and redis are reachable
//! and the audit guard has not latched (§6.5), else 503. An invalid token is
//! answered as anonymous.
//!
//! Exempt from the global per-IP bucket, except that a request carrying an
//! `Authorization` header is charged to it before verification (issuer
//! resolution is outbound work).

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::{DateTime, SecondsFormat, Utc};
use redis::aio::ConnectionManager;
use serde::Serialize;
use sqlx::PgPool;

use crate::db::schema_backfill::SchemaStatus;
use crate::firehose::consumer::ConsumerStatus;
use crate::firehose::watermark::Watermarks;
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::{Class, client_ip};
use crate::xrpc::{Authenticator, RateLimits};

/// Per-probe timeout.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// The `lxm` a detail token must carry.
pub const METHOD: &str = "_health";

/// The audit latch (§6.5): shared with `GlueContext::audit_latched`, set when
/// the user audit sink panicked. `audit_ok = !latched`.
#[derive(Debug, Clone, Default)]
pub struct AuditLatch(Arc<AtomicBool>);

impl AuditLatch {
    /// Report the glue's latch flag.
    #[must_use]
    pub fn from_flag(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }

    #[must_use]
    pub fn latched(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn latch(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Handler state.
#[derive(Clone)]
pub struct HealthState {
    pub pool: PgPool,
    pub redis: ConnectionManager,
    pub watermarks: Watermarks,
    pub status: Arc<ConsumerStatus>,
    pub audit: AuditLatch,
    pub auth: Arc<dyn Authenticator>,
    pub limits: RateLimits,
    /// `http.trusted_proxy` (§7.4).
    pub trusted_proxy: IpAddr,
    /// The post-migration task's progress (design-v3 §12.2).
    pub schema: SchemaStatus,
}

/// The anonymous response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub ok: bool,
    pub version: &'static str,
}

impl From<&Health> for Summary {
    fn from(h: &Health) -> Self {
        Self {
            ok: h.ok,
            version: h.version,
        }
    }
}

/// The authenticated response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Health {
    pub ok: bool,
    pub version: &'static str,
    pub db: bool,
    pub redis: bool,
    pub audit_ok: bool,
    pub watermarks: BTreeMap<String, Option<String>>,
    pub private_watermark: Option<String>,
    pub gap_blocked: bool,
    /// The column backfill has visited every old row. Until then search
    /// and trends are incomplete for old rows.
    pub schema_backfill_done: bool,
    /// Every search index is built. Until then search runs degraded.
    pub search_indexes_ready: bool,
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Assemble the body from probe results.
#[must_use]
pub fn body(db: bool, redis: bool, audit_ok: bool, watermarks: &Watermarks, gap_blocked: bool, schema: &SchemaStatus) -> Health {
    Health {
        ok: db && redis && audit_ok,
        version: env!("CARGO_PKG_VERSION"),
        db,
        redis,
        audit_ok,
        watermarks: watermarks
            .snapshot()
            .into_iter()
            .map(|(k, v)| (k, v.map(rfc3339)))
            .collect(),
        private_watermark: watermarks.private_watermark().map(rfc3339),
        gap_blocked,
        schema_backfill_done: schema.backfill_done(),
        search_indexes_ready: schema.indexes_ready(),
    }
}

async fn probe_db(pool: &PgPool) -> bool {
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, sqlx::query("SELECT 1").execute(pool)).await,
        Ok(Ok(_))
    )
}

async fn probe_redis(redis: &ConnectionManager) -> bool {
    let mut conn = redis.clone();
    let ping = redis::cmd("PING");
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, ping.query_async::<String>(&mut conn)).await,
        Ok(Ok(_))
    )
}

/// Whether the caller presented a verifying token.
async fn authenticated(state: &HealthState, authorization: Option<&str>) -> bool {
    authorization.is_some() && state.auth.verify(authorization, METHOD).await.is_ok()
}

async fn health(State(state): State<HealthState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    if authorization.is_some() {
        let xff = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
        let ip = client_ip(peer.ip(), xff, state.trusted_proxy);
        if let Err(limited) = state.limits.check_ip(Class::Global, ip) {
            return XrpcError::from(limited).into_response();
        }
    }
    let (detail, db, redis) = tokio::join!(
        authenticated(&state, authorization),
        probe_db(&state.pool),
        probe_redis(&state.redis)
    );
    let h = body(db, redis, !state.audit.latched(), &state.watermarks, state.status.gap_blocked(), &state.schema);
    let code = if h.ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    if detail {
        (code, Json(h)).into_response()
    } else {
        (code, Json(Summary::from(&h))).into_response()
    }
}

/// The placeholder router.
pub fn router(state: HealthState) -> Router {
    Router::new().route("/xrpc/_health", get(health)).with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marks() -> Watermarks {
        let labels = [("wss://a", "bsky"), ("wss://b", "microcosm")]
            .into_iter()
            .map(|(u, l)| (u.to_string(), l.to_string()))
            .collect();
        Watermarks::new(labels, vec!["bsky".into(), "microcosm".into()])
    }

    #[test]
    fn body_shape() {
        let w = marks();
        let t = DateTime::parse_from_rfc3339("2026-09-26T12:00:00Z").expect("t").with_timezone(&Utc);
        w.observe("bsky", t);
        let schema = SchemaStatus::default();
        let h = body(true, true, true, &w, false, &schema);
        assert!(h.ok);
        let json = serde_json::to_value(&h).expect("json");
        assert_eq!(json["watermarks"]["bsky"], "2026-09-26T12:00:00.000Z");
        assert!(json["watermarks"]["microcosm"].is_null());
        assert!(json["private_watermark"].is_null());
        assert_eq!(json["gap_blocked"], false);
        // An unfinished backfill is reported and does not fail health.
        assert_eq!((&json["schema_backfill_done"], &json["search_indexes_ready"]), (&false.into(), &false.into()));
        for key in ["ok", "version", "db", "redis", "audit_ok", "watermarks", "private_watermark", "gap_blocked"] {
            assert!(json.get(key).is_some(), "{key}");
        }
    }

    #[test]
    fn unhealthy_when_any_dependency_fails_or_audit_latches() {
        let w = marks();
        let s = SchemaStatus::default();
        assert!(!body(false, true, true, &w, false, &s).ok);
        assert!(!body(true, false, true, &w, false, &s).ok);
        let latch = AuditLatch::default();
        latch.latch();
        assert!(!body(true, true, !latch.latched(), &w, false, &s).ok);
        // A held gap is reported but does not fail health.
        assert!(body(true, true, true, &w, true, &s).ok);
    }

    /// Anonymous and invalid-token callers get `{ok, version}`; a verifying
    /// `_health` token gets the detail.
    #[tokio::test]
    async fn detail_requires_a_verified_token() {
        use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
        use crate::test_support::fresh_db;

        const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let Some(db) = fresh_db("health_detail").await else {
            return;
        };
        let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
            eprintln!("HPRS_TEST_REDIS_URL unset; skipping health detail test");
            db.drop_db().await;
            return;
        };
        let redis = ConnectionManager::new(redis::Client::open(url).expect("client")).await.expect("redis");
        let fetcher = StubFetcher::default();
        fetcher.set_key(ALICE, signing_key(3));
        let (auth, _drainer) = verifier(fetcher, 100, 16);
        let state = HealthState {
            pool: db.pool.clone(),
            redis,
            watermarks: marks(),
            status: Arc::new(ConsumerStatus::default()),
            audit: AuditLatch::default(),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            schema: SchemaStatus::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("serve");
        });
        let client = reqwest::Client::new();
        let url = format!("http://{addr}/xrpc/_health");
        let get = |token: Option<String>| {
            let mut req = client.get(&url);
            if let Some(t) = token {
                req = req.header("authorization", format!("Bearer {t}"));
            }
            async move {
                let r = req.send().await.expect("request");
                let status = r.status().as_u16();
                (status, r.json::<serde_json::Value>().await.expect("json"))
            }
        };
        let minimal = serde_json::json!({"ok": true, "version": env!("CARGO_PKG_VERSION")});
        let aud = format!("did:web:{AUDIENCE_HOST}");

        let (status, v) = get(None).await;
        assert_eq!((status, &v), (200, &minimal));
        let (status, v) = get(Some("garbage".into())).await;
        assert_eq!((status, &v), (200, &minimal));
        let other_lxm = mint(&signing_key(3), ALICE, &aud, Some("app.bsky.feed.getTimeline"));
        let (status, v) = get(Some(other_lxm)).await;
        assert_eq!((status, &v), (200, &minimal));

        let token = mint(&signing_key(3), ALICE, &aud, Some(METHOD));
        let (status, v) = get(Some(token)).await;
        assert_eq!(status, 200, "{v}");
        for key in ["ok", "version", "db", "redis", "audit_ok", "watermarks", "private_watermark", "gap_blocked"] {
            assert!(v.get(key).is_some(), "{key}: {v}");
        }
        assert_eq!(v["db"], true);
        assert_eq!(v["redis"], true);
        db.drop_db().await;
    }
}
