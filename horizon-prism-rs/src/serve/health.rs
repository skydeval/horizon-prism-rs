//! `GET /xrpc/_health` (design-v2 §7.1):
//! `{"ok","version","db","redis","audit_ok","watermarks":{label: rfc3339|null},
//! "private_watermark","gap_blocked"}`. 200 when db and redis are reachable
//! and the audit guard has not latched (§6.5), else 503.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use chrono::{DateTime, SecondsFormat, Utc};
use redis::aio::ConnectionManager;
use serde::Serialize;
use sqlx::PgPool;

use crate::firehose::consumer::ConsumerStatus;
use crate::firehose::watermark::Watermarks;

/// Per-probe timeout.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// The audit latch (§6.5). The kryphocron glue item sets it when the user
/// audit sink terminates; until then it never latches.
#[derive(Debug, Clone, Default)]
pub struct AuditLatch(Arc<AtomicBool>);

impl AuditLatch {
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
}

/// The response body.
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
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Assemble the body from probe results.
#[must_use]
pub fn body(db: bool, redis: bool, audit_ok: bool, watermarks: &Watermarks, gap_blocked: bool) -> Health {
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

async fn health(State(state): State<HealthState>) -> (StatusCode, Json<Health>) {
    let (db, redis) = tokio::join!(probe_db(&state.pool), probe_redis(&state.redis));
    let h = body(db, redis, !state.audit.latched(), &state.watermarks, state.status.gap_blocked());
    let code = if h.ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, Json(h))
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
        let h = body(true, true, true, &w, false);
        assert!(h.ok);
        let json = serde_json::to_value(&h).expect("json");
        assert_eq!(json["watermarks"]["bsky"], "2026-09-26T12:00:00.000Z");
        assert!(json["watermarks"]["microcosm"].is_null());
        assert!(json["private_watermark"].is_null());
        assert_eq!(json["gap_blocked"], false);
        for key in ["ok", "version", "db", "redis", "audit_ok", "watermarks", "private_watermark", "gap_blocked"] {
            assert!(json.get(key).is_some(), "{key}");
        }
    }

    #[test]
    fn unhealthy_when_any_dependency_fails_or_audit_latches() {
        let w = marks();
        assert!(!body(false, true, true, &w, false).ok);
        assert!(!body(true, false, true, &w, false).ok);
        let latch = AuditLatch::default();
        latch.latch();
        assert!(!body(true, true, !latch.latched(), &w, false).ok);
        // A held gap is reported but does not fail health.
        assert!(body(true, true, true, &w, true).ok);
    }
}
