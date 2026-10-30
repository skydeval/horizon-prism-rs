//! Push wakes for private notifications: once a new private notification
//! commits, its recipient's DID goes to the push service `[private_push]`
//! names, in batches. A call carries DIDs only: no record, URI or reason.
//!
//! - **Marking** ([`mark`]): a private notification row inserted for a
//!   live write (the firehose or `requestRecord`; not backfill or catch-up),
//!   or by the subscribed-post fan-out, `pg_notify`s [`CHANNEL`] with its
//!   id on the inserting transaction. Postgres delivers it at commit, so a
//!   rolled-back row is never sent. Nothing is marked unless [`enabled`].
//! - **The sender** ([`PushSender::spawn`]): `LISTEN` [`CHANNEL`], gather
//!   ids for `batch_secs` after the first, then send each recipient whose
//!   rows still exist and pass what `listNotifications` would apply now
//!   (blocks, mutes, muted threads, its preferences) and its `push`
//!   preference, up to [`MAX_DIDS`] a call, with a service JWT. Best
//!   effort: a failed call is counted and not retried; the app reads what
//!   it missed the next time it opens.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde_json::json;
use sqlx::postgres::PgListener;
use sqlx::{PgConnection, PgPool};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::schema;
use crate::config::PrivatePushConfig;
use crate::firehose::writer::WriteSource;
use crate::kryphocron_glue::ServiceKey;
use crate::net::{BodyCap, NetClient, RequestOptions};

/// The NOTIFY channel a new private notification's id is sent on.
pub const CHANNEL: &str = "private_push";
/// The push service's method.
pub const LXM: &str = "app.nearhorizon.push.notify";
/// DIDs in one call.
pub const MAX_DIDS: usize = 100;
/// The listener's reconnect wait.
const RECONNECT: Duration = Duration::from_secs(5);

/// Metric: calls, by `outcome`: `ok`, `unauthorized` (401), `forbidden`
/// (403), `rejected` (any other status) or `network`. 401 and 403 mean this
/// AppView's `[private_push]` or the push service's issuer list is wrong.
pub const METRIC_CALLS: &str = "private_push_calls_total";
/// Metric: DIDs sent in calls that succeeded.
pub const METRIC_DIDS: &str = "private_push_dids_total";
/// Metric: new notifications dropped, by `reason`: `full` (more than
/// `max_pending` waiting) or `db_error` (their batch's check failed).
pub const METRIC_DROPPED: &str = "private_push_dropped_total";

/// Whether new notifications are marked: set once at startup when
/// `[private_push]` is configured and the private tier is on.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether new private notifications are marked for push.
#[must_use]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Turn marking on (startup; tests).
pub(crate) fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// Whether a write from `source` is live enough to push: the firehose, and
/// `requestRecord` (a client asking for its own record at once, or a
/// dead-letter replay). Backfill and catch-up find old records.
#[must_use]
pub fn live(source: WriteSource) -> bool {
    matches!(source, WriteSource::Firehose | WriteSource::IndexRequest)
}

/// Mark the new private notification rows `ids` for push, on the inserting
/// transaction: delivered at its commit. Nothing while not [`enabled`].
///
/// # Errors
///
/// Database errors.
pub(super) async fn mark(conn: &mut PgConnection, ids: &[i64]) -> Result<(), sqlx::Error> {
    if ids.is_empty() || !enabled() {
        return Ok(());
    }
    sqlx::query("SELECT pg_notify($1, x::text) FROM unnest($2::bigint[]) AS x")
        .bind(CHANNEL)
        .bind(ids)
        .execute(conn)
        .await?;
    Ok(())
}

/// The sender task.
#[derive(Clone)]
pub(crate) struct PushSender {
    pool: PgPool,
    net: NetClient,
    key: ServiceKey,
    endpoint: String,
    aud: String,
    batch: Duration,
    timeout: Duration,
    max_pending: usize,
}

impl PushSender {
    /// The sender for `cfg`; `None` when `[private_push]` names no service.
    #[must_use]
    pub(crate) fn new(pool: PgPool, net: NetClient, key: ServiceKey, cfg: &PrivatePushConfig) -> Option<Self> {
        let (url, did) = (cfg.url.as_deref()?, cfg.service_did.as_deref()?);
        Some(Self {
            pool,
            net,
            key,
            endpoint: format!("{}/xrpc/{LXM}", url.trim_end_matches('/')),
            aud: did.to_string(),
            batch: Duration::from_secs(cfg.batch_secs),
            timeout: Duration::from_millis(cfg.timeout_ms),
            max_pending: cfg.max_pending,
        })
    }

    #[must_use]
    pub(crate) fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self) {
        let mut listener = loop {
            match self.listen().await {
                Ok(l) => break l,
                Err(error) => {
                    tracing::warn!(event = "private_push_listen_failed", %error, "LISTEN failed; retrying");
                    tokio::time::sleep(RECONNECT).await;
                }
            }
        };
        let mut pending: HashSet<i64> = HashSet::new();
        loop {
            // The first id of a batch; `recv` reconnects by itself, and what
            // was sent while it was away is lost.
            match listener.recv().await {
                Ok(n) => self.add(&mut pending, n.payload()),
                Err(error) => {
                    tracing::debug!(event = "private_push_listen_error", %error, "listener error; it reconnects");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
            let deadline = Instant::now() + self.batch;
            loop {
                tokio::select! {
                    n = listener.recv() => match n {
                        Ok(n) => self.add(&mut pending, n.payload()),
                        Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                    },
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
            let ids: Vec<i64> = pending.drain().collect();
            self.flush(&ids).await;
        }
    }

    async fn listen(&self) -> Result<PgListener, sqlx::Error> {
        let mut l = PgListener::connect_with(&self.pool).await?;
        l.listen(CHANNEL).await?;
        Ok(l)
    }

    fn add(&self, pending: &mut HashSet<i64>, payload: &str) {
        let Ok(id) = payload.parse::<i64>() else {
            return;
        };
        if pending.len() >= self.max_pending {
            metrics::counter!(METRIC_DROPPED, "reason" => "full").increment(1);
        } else {
            pending.insert(id);
        }
    }

    /// Send the recipients of `ids` that pass ([`schema::push_recipients`]).
    pub(super) async fn flush(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        let dids = match schema::push_recipients(&self.pool, ids).await {
            Ok(d) => d,
            Err(error) => {
                tracing::warn!(event = "private_push_check_failed", %error, "checking push recipients failed; this batch is dropped");
                metrics::counter!(METRIC_DROPPED, "reason" => "db_error").increment(ids.len() as u64);
                return;
            }
        };
        for chunk in dids.chunks(MAX_DIDS) {
            self.send(chunk).await;
        }
    }

    /// One call with `dids` (1 to [`MAX_DIDS`]); the outcome label.
    pub(super) async fn send(&self, dids: &[String]) -> &'static str {
        let token = crate::appview_clients::jwt::mint(&self.key, &self.aud, LXM, chrono::Utc::now().timestamp());
        let Ok(bearer) = HeaderValue::from_str(&format!("Bearer {token}")) else {
            return "network";
        };
        let options = RequestOptions::new(BodyCap(4096)).timeout(self.timeout).header(AUTHORIZATION, bearer);
        let outcome = match self.net.post_json(&self.endpoint, &json!({ "dids": dids }), &options).await {
            Ok(r) if r.is_success() => "ok",
            Ok(r) if r.status.as_u16() == 401 => "unauthorized",
            Ok(r) if r.status.as_u16() == 403 => "forbidden",
            Ok(r) => {
                tracing::warn!(event = "private_push_rejected", status = r.status.as_u16(), "the push service refused a call");
                "rejected"
            }
            Err(error) => {
                tracing::warn!(event = "private_push_failed", %error, "a push call failed");
                "network"
            }
        };
        if matches!(outcome, "unauthorized" | "forbidden") {
            tracing::warn!(
                event = "private_push_misconfigured",
                outcome,
                aud = %self.aud,
                "the push service refused this AppView's token: check private_push.service_did and the service's allowed issuers"
            );
        }
        if outcome == "ok" {
            metrics::counter!(METRIC_DIDS).increment(dids.len() as u64);
        }
        metrics::counter!(METRIC_CALLS, "outcome" => outcome).increment(1);
        outcome
    }
}
