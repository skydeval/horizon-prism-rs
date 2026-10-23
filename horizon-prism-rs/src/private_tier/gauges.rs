//! The private tier's gauges (design-v4 §II-5.10, §II-6.1), sampled with
//! the backfill gauges every `METRICS_INTERVAL`:
//! - `private_owners_not_ready_total`: private owners (§6.11.1) with no
//!   readiness row;
//! - `private_tier_last_event_age_seconds`: seconds since the live
//!   firehose last delivered a private-tier op. Community PDSes must be
//!   crawled by every private relay; a value past 24 h while private rows
//!   exist says one is not, and is logged (`private_tier_ingest_stale`);
//! - `private_subscribed_post_queue` and
//!   `private_subscribed_post_queue_lag_seconds`: the subscribed-post
//!   fan-out's rows not done, and the oldest one's age (private-activity-subs
//!   design §12);
//! - `appearance_retained_rows`: appearance rows kept after a removal
//!   (private-appearance amendment 1 §10).
//!
//! The last event is kept in process. At a start, before the first event,
//! it is the newest private row's `indexed_at`, so a restart does not hide
//! a stale tier. With no private row and no event the age is 0: there is
//! nothing to be stale.

use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, Utc};
use sqlx::PgPool;

/// `private_owners_not_ready_total`.
pub const METRIC_OWNERS_NOT_READY: &str = "private_owners_not_ready_total";
/// `private_tier_last_event_age_seconds`.
pub const METRIC_LAST_EVENT_AGE: &str = "private_tier_last_event_age_seconds";
/// The age past which the tier is stale (§II-6.1).
pub const STALE_AFTER_SECS: i64 = 24 * 3600;

/// When the live firehose last delivered a private-tier op, in unix
/// milliseconds; 0 for never in this process.
static LAST_EVENT_MS: AtomicI64 = AtomicI64::new(0);

/// Note a private-tier op the live firehose delivered.
pub fn note_event() {
    note_event_at(Utc::now());
}

fn note_event_at(at: DateTime<Utc>) {
    LAST_EVENT_MS.fetch_max(at.timestamp_millis(), Ordering::Relaxed);
}

/// Private owners with no `private_owner_ready` row, by `owners::load`'s
/// definition.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn owners_not_ready(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(*) FROM ( \
           SELECT author_did AS d FROM private_posts UNION SELECT owner_did FROM private_audiences \
         ) r WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = r.d) \
           AND NOT EXISTS (SELECT 1 FROM private_owner_ready o WHERE o.did = r.d)",
    )
    .fetch_one(pool)
    .await
}

/// Whether any private row is stored.
async fn rows_exist(pool: &PgPool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM private_posts) OR EXISTS (SELECT 1 FROM private_audiences)",
    )
    .fetch_one(pool)
    .await
}

/// The newest private row's `indexed_at`: the seed before the first event.
async fn newest_row(pool: &PgPool) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT max(t) FROM (SELECT max(indexed_at) AS t FROM private_posts \
           UNION ALL SELECT max(indexed_at) FROM private_audiences) m",
    )
    .fetch_one(pool)
    .await
}

/// The last event's age in seconds, seeding it from the rows first if no
/// event came yet; 0 with no row and no event.
async fn last_event_age(pool: &PgPool, now: DateTime<Utc>) -> Result<i64, sqlx::Error> {
    if LAST_EVENT_MS.load(Ordering::Relaxed) == 0
        && let Some(newest) = newest_row(pool).await?
    {
        note_event_at(newest);
    }
    let last = LAST_EVENT_MS.load(Ordering::Relaxed);
    if last == 0 {
        return Ok(0);
    }
    Ok(((now.timestamp_millis() - last) / 1000).max(0))
}

/// One sample of both gauges.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn sample(pool: &PgPool) -> Result<(), sqlx::Error> {
    let not_ready = owners_not_ready(pool).await?;
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(METRIC_OWNERS_NOT_READY).set(not_ready as f64);
    let age = last_event_age(pool, Utc::now()).await?;
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(METRIC_LAST_EVENT_AGE).set(age as f64);
    let (queued, lag) = super::fanout::queue_state(pool).await?;
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(super::fanout::METRIC_QUEUE).set(queued as f64);
    metrics::gauge!(super::fanout::METRIC_QUEUE_LAG).set(lag);
    let retained = super::schema::appearance_retained_count(pool).await?;
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(super::appearance::METRIC_RETAINED_ROWS).set(retained as f64);
    if age > STALE_AFTER_SECS && rows_exist(pool).await? {
        tracing::warn!(
            event = "private_tier_ingest_stale",
            age_secs = age,
            "no private-tier event from the relays for over 24 h while private rows exist: check that every private relay crawls the community PDSes"
        );
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn last_event_ms() -> i64 {
    LAST_EVENT_MS.load(Ordering::Relaxed)
}
