//! DID document caches (design-v2 §6.7, §7.6).
//!
//! - `did_cache` (Postgres): only for the persistence set. Rows are fresh for
//!   1 h (`stale_at`) and stale-usable up to 24 h (`expires_at`).
//! - In-process: a bounded LRU `moka` cache (100 000 entries, TTL 1 h) for
//!   every other resolution, so request traffic can only evict entries.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use moka::future::Cache;
use moka::policy::EvictionPolicy;
use serde_json::Value;
use sqlx::PgPool;

use super::IdentitySettings;

/// In-process cache capacity for non-persisted documents (§7.6).
pub const NON_PERSISTED_CAPACITY: u64 = 100_000;
/// In-process cache TTL (§7.6).
pub const NON_PERSISTED_TTL: Duration = Duration::from_secs(3600);

/// A cached DID document.
#[derive(Debug, Clone, PartialEq)]
pub struct CachedDoc {
    pub doc: Arc<Value>,
    pub fetched_at: DateTime<Utc>,
}

/// Where a cached document sits in its lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// Younger than the fresh window: usable everywhere, including the JWT path.
    Fresh,
    /// Past fresh, within the stale window: usable off the JWT path, and a
    /// background refresh is due.
    Stale,
    /// Past the stale window: treated as a miss.
    Expired,
}

/// Classify a `did_cache` row by its stored bounds.
#[must_use]
pub fn classify_row(
    stale_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Freshness {
    if now < stale_at {
        Freshness::Fresh
    } else if now < expires_at {
        Freshness::Stale
    } else {
        Freshness::Expired
    }
}

/// `at + d`, saturating at the maximum representable time.
#[must_use]
pub fn saturating_add(at: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    TimeDelta::from_std(d)
        .ok()
        .and_then(|d| at.checked_add_signed(d))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

/// Classify by age against the configured windows (in-process entries).
#[must_use]
pub fn classify_age(
    fetched_at: DateTime<Utc>,
    now: DateTime<Utc>,
    settings: &IdentitySettings,
) -> Freshness {
    classify_row(
        saturating_add(fetched_at, settings.fresh),
        saturating_add(fetched_at, settings.stale_max),
        now,
    )
}

#[derive(sqlx::FromRow)]
struct DidCacheRow {
    doc: Value,
    fetched_at: DateTime<Utc>,
    stale_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

/// Both cache layers.
#[derive(Clone)]
pub struct DidCache {
    pool: PgPool,
    memory: Cache<String, CachedDoc>,
    settings: IdentitySettings,
}

impl DidCache {
    #[must_use]
    pub fn new(pool: PgPool, settings: IdentitySettings) -> Self {
        let memory = Cache::builder()
            .max_capacity(NON_PERSISTED_CAPACITY)
            .time_to_live(NON_PERSISTED_TTL)
            .eviction_policy(EvictionPolicy::lru())
            .build();
        Self {
            pool,
            memory,
            settings,
        }
    }

    #[must_use]
    pub fn settings(&self) -> &IdentitySettings {
        &self.settings
    }

    /// The cached document for `did` and its freshness, from either layer.
    /// Expired rows are returned as [`Freshness::Expired`]; callers treat them
    /// as misses.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn lookup(&self, did: &str) -> Result<Option<(CachedDoc, Freshness)>, sqlx::Error> {
        let now = Utc::now();
        if let Some(entry) = self.memory.get(did).await {
            let freshness = classify_age(entry.fetched_at, now, &self.settings);
            return Ok(Some((entry, freshness)));
        }
        let row = sqlx::query_as::<_, DidCacheRow>(
            "SELECT doc, fetched_at, stale_at, expires_at FROM did_cache WHERE did = $1",
        )
        .bind(did)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            let freshness = classify_row(r.stale_at, r.expires_at, now);
            (
                CachedDoc {
                    doc: Arc::new(r.doc),
                    fetched_at: r.fetched_at,
                },
                freshness,
            )
        }))
    }

    /// Store a freshly fetched document: in `did_cache` when `persist`, else
    /// in the in-process cache. A persisted write never replaces a row fetched
    /// later than this one.
    ///
    /// # Errors
    ///
    /// Any database error (persisted writes only).
    pub async fn store(&self, did: &str, entry: CachedDoc, persist: bool) -> Result<(), sqlx::Error> {
        if !persist {
            self.memory.insert(did.to_string(), entry).await;
            return Ok(());
        }
        let stale_at = saturating_add(entry.fetched_at, self.settings.fresh);
        let expires_at = saturating_add(entry.fetched_at, self.settings.stale_max);
        sqlx::query(
            "INSERT INTO did_cache (did, doc, fetched_at, stale_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (did) DO UPDATE SET doc = EXCLUDED.doc, fetched_at = EXCLUDED.fetched_at, \
               stale_at = EXCLUDED.stale_at, expires_at = EXCLUDED.expires_at \
             WHERE did_cache.fetched_at <= EXCLUDED.fetched_at",
        )
        .bind(did)
        .bind(entry.doc.as_ref())
        .bind(entry.fetched_at)
        .bind(stale_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        // A DID that joined the persistence set leaves the in-process layer.
        self.memory.invalidate(did).await;
        Ok(())
    }

    /// Drop `did` from both layers.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn remove(&self, did: &str) -> Result<(), sqlx::Error> {
        self.memory.invalidate(did).await;
        sqlx::query("DELETE FROM did_cache WHERE did = $1")
            .bind(did)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn t(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 26, h, m, 0).single().expect("valid")
    }

    fn settings() -> IdentitySettings {
        IdentitySettings {
            plc_url: "https://plc.directory".to_string(),
            fresh: Duration::from_secs(3600),
            stale_max: Duration::from_secs(86_400),
        }
    }

    #[test]
    fn row_freshness_uses_stored_bounds() {
        let stale_at = t(11, 0);
        let expires_at = t(23, 0);
        assert_eq!(classify_row(stale_at, expires_at, t(10, 59)), Freshness::Fresh);
        assert_eq!(classify_row(stale_at, expires_at, t(11, 0)), Freshness::Stale);
        assert_eq!(classify_row(stale_at, expires_at, t(22, 59)), Freshness::Stale);
        assert_eq!(classify_row(stale_at, expires_at, t(23, 0)), Freshness::Expired);
    }

    #[test]
    fn age_freshness_is_fresh_one_hour_stale_one_day() {
        let s = settings();
        let fetched = t(0, 0);
        assert_eq!(classify_age(fetched, t(0, 59), &s), Freshness::Fresh);
        assert_eq!(classify_age(fetched, t(1, 0), &s), Freshness::Stale);
        assert_eq!(classify_age(fetched, t(23, 59), &s), Freshness::Stale);
        let next_day = fetched + TimeDelta::days(1);
        assert_eq!(classify_age(fetched, next_day, &s), Freshness::Expired);
    }

    #[test]
    fn clock_skew_counts_as_fresh() {
        let s = settings();
        assert_eq!(classify_age(t(12, 0), t(11, 0), &s), Freshness::Fresh);
    }

    #[test]
    fn in_process_limits_match_design() {
        assert_eq!(NON_PERSISTED_CAPACITY, 100_000);
        assert_eq!(NON_PERSISTED_TTL, Duration::from_secs(3600));
    }
}
