//! Storage sampler (design-v2 §4.6).
//!
//! Every 300 s: per-table rows and bytes, database size and budget ratio, the
//! firehose stream length, the scope and member counts, and rule 4's row
//! counts and storage bound ([`gauges`]). Every 30 s: Redis
//! `INFO memory`. Budget and Redis-memory warnings are logged once per sample.

use std::time::Duration;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::subscriptions::gauges;

pub const DB_SAMPLE_INTERVAL: Duration = Duration::from_secs(300);
pub const REDIS_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

pub const METRIC_TABLE_ROWS: &str = "db_table_rows";
pub const METRIC_TABLE_BYTES: &str = "db_table_bytes";
pub const METRIC_DB_SIZE: &str = "db_size_bytes";
pub const METRIC_DB_BUDGET_RATIO: &str = "db_budget_ratio";
pub const METRIC_REDIS_USED: &str = "redis_used_memory_bytes";
pub const METRIC_REDIS_MAX: &str = "redis_maxmemory_bytes";
pub const METRIC_REDIS_RATIO: &str = "redis_memory_ratio";
pub const METRIC_STREAM_LENGTH: &str = "firehose_stream_length";
pub const METRIC_SCOPE_DIDS: &str = "scope_dids_total";
pub const METRIC_MEMBERS: &str = "members_total";
pub const METRIC_SAMPLE_ERRORS: &str = "storage_sample_errors_total";

/// `storage_budget_warn` threshold (§4.6).
pub const BUDGET_WARN_RATIO: f64 = 0.80;
/// `storage_budget_critical` threshold (§4.6).
pub const BUDGET_CRITICAL_RATIO: f64 = 0.95;
/// `redis_memory_warn` threshold (§4.6).
pub const REDIS_WARN_RATIO: f64 = 0.80;

/// Where a ratio sits against the storage budget thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetLevel {
    Ok,
    Warn,
    Critical,
}

/// Classify a `db_budget_ratio`.
#[must_use]
pub fn budget_level(ratio: f64) -> BudgetLevel {
    if ratio >= BUDGET_CRITICAL_RATIO {
        BudgetLevel::Critical
    } else if ratio >= BUDGET_WARN_RATIO {
        BudgetLevel::Warn
    } else {
        BudgetLevel::Ok
    }
}

/// `used / total`, or 0 when `total` is 0 (Redis `maxmemory 0` means unlimited).
#[must_use]
pub fn ratio(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        used as f64 / total as f64
    }
}

/// The fields of Redis `INFO memory` the sampler exports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisMemory {
    pub used_memory: u64,
    /// 0 when no limit is configured.
    pub maxmemory: u64,
}

/// Parse `INFO memory` output (`key:value` lines, `#` comments).
#[must_use]
pub fn parse_info_memory(info: &str) -> Option<RedisMemory> {
    let mut used = None;
    let mut max = None;
    for line in info.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        match line.split_once(':') {
            Some(("used_memory", v)) => used = v.trim().parse().ok(),
            Some(("maxmemory", v)) => max = v.trim().parse().ok(),
            _ => {}
        }
    }
    Some(RedisMemory {
        used_memory: used?,
        maxmemory: max?,
    })
}

/// One row of `pg_stat_user_tables`.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TableSample {
    pub table: String,
    pub rows: i64,
    pub bytes: i64,
}

/// One database sample.
#[derive(Debug, Clone, PartialEq)]
pub struct DbSample {
    pub tables: Vec<TableSample>,
    pub db_size_bytes: i64,
    pub budget_ratio: f64,
    pub scope_dids_total: i64,
    pub members_total: i64,
}

/// Read the Postgres half of a sample.
///
/// # Errors
///
/// Any database error.
pub async fn sample_db(pool: &PgPool, budget_bytes: u64) -> Result<DbSample, sqlx::Error> {
    let tables = sqlx::query_as::<_, TableSample>(
        "SELECT relname::text AS \"table\", n_live_tup AS \"rows\", \
         pg_total_relation_size(relid) AS \"bytes\" \
         FROM pg_stat_user_tables ORDER BY relname",
    )
    .fetch_all(pool)
    .await?;
    let db_size_bytes: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await?;
    let scope_dids_total: i64 = sqlx::query_scalar("SELECT count(*) FROM scope_dids")
        .fetch_one(pool)
        .await?;
    let members_total: i64 = sqlx::query_scalar("SELECT count(*) FROM members")
        .fetch_one(pool)
        .await?;
    Ok(DbSample {
        tables,
        db_size_bytes,
        budget_ratio: ratio(u64::try_from(db_size_bytes).unwrap_or(0), budget_bytes),
        scope_dids_total,
        members_total,
    })
}

/// Export a database sample and log budget warnings.
pub fn record_db_sample(sample: &DbSample, budget_bytes: u64) {
    for t in &sample.tables {
        metrics::gauge!(METRIC_TABLE_ROWS, "table" => t.table.clone()).set(t.rows as f64);
        metrics::gauge!(METRIC_TABLE_BYTES, "table" => t.table.clone()).set(t.bytes as f64);
    }
    metrics::gauge!(METRIC_DB_SIZE).set(sample.db_size_bytes as f64);
    metrics::gauge!(METRIC_DB_BUDGET_RATIO).set(sample.budget_ratio);
    metrics::gauge!(METRIC_SCOPE_DIDS).set(sample.scope_dids_total as f64);
    metrics::gauge!(METRIC_MEMBERS).set(sample.members_total as f64);
    match budget_level(sample.budget_ratio) {
        BudgetLevel::Ok => {}
        BudgetLevel::Warn => tracing::warn!(
            event = "storage_budget_warn",
            db_size_bytes = sample.db_size_bytes,
            budget_bytes,
            ratio = sample.budget_ratio,
            "database size is at or above 80% of storage.budget_bytes",
        ),
        BudgetLevel::Critical => tracing::error!(
            event = "storage_budget_critical",
            db_size_bytes = sample.db_size_bytes,
            budget_bytes,
            ratio = sample.budget_ratio,
            "database size is at or above 95% of storage.budget_bytes",
        ),
    }
}

/// Export a Redis memory sample and log the warning.
pub fn record_redis_memory(mem: RedisMemory) {
    let r = ratio(mem.used_memory, mem.maxmemory);
    metrics::gauge!(METRIC_REDIS_USED).set(mem.used_memory as f64);
    metrics::gauge!(METRIC_REDIS_MAX).set(mem.maxmemory as f64);
    metrics::gauge!(METRIC_REDIS_RATIO).set(r);
    if r >= REDIS_WARN_RATIO {
        tracing::warn!(
            event = "redis_memory_warn",
            used_memory = mem.used_memory,
            maxmemory = mem.maxmemory,
            ratio = r,
            "Redis memory is at or above 80% of maxmemory; at the ceiling ingest stops",
        );
    }
}

/// The storage sampler task.
pub struct StorageSampler {
    pub pool: PgPool,
    pub redis: ConnectionManager,
    /// `firehose.stream_key`.
    pub stream_key: String,
    /// `storage.budget_bytes`.
    pub budget_bytes: u64,
    /// `lists.subscribed_max_per_member`, for `list_subscription_rows_bound`.
    pub subscribed_max_per_member: u32,
    /// `lists.subscribed_max_items`, for `list_subscription_rows_bound`.
    pub subscribed_max_items: u32,
}

impl StorageSampler {
    async fn db_tick(&self) {
        match sample_db(&self.pool, self.budget_bytes).await {
            Ok(sample) => {
                record_db_sample(&sample, self.budget_bytes);
                let members = u64::try_from(sample.members_total).unwrap_or(0);
                let bound = gauges::rows_bound(members, self.subscribed_max_per_member, self.subscribed_max_items);
                metrics::gauge!(gauges::METRIC_ROWS_BOUND).set(bound as f64);
            }
            Err(error) => {
                metrics::counter!(METRIC_SAMPLE_ERRORS, "source" => "postgres").increment(1);
                tracing::warn!(event = "storage_sample_failed", source = "postgres", %error, "storage sample failed");
            }
        }
        // Rule 4's row counts (design-v3 §14.2).
        match gauges::sample(&self.pool).await {
            Ok(sample) => gauges::record(&sample),
            Err(error) => {
                metrics::counter!(METRIC_SAMPLE_ERRORS, "source" => "list_subscriptions").increment(1);
                tracing::warn!(event = "storage_sample_failed", source = "list_subscriptions", %error, "list subscription sample failed");
            }
        }
        let mut conn = self.redis.clone();
        let len: redis::RedisResult<u64> = conn.xlen(&self.stream_key).await;
        match len {
            Ok(n) => metrics::gauge!(METRIC_STREAM_LENGTH).set(n as f64),
            Err(error) => {
                metrics::counter!(METRIC_SAMPLE_ERRORS, "source" => "redis_xlen").increment(1);
                tracing::warn!(event = "storage_sample_failed", source = "redis_xlen", %error, "stream length sample failed");
            }
        }
    }

    async fn redis_tick(&self) {
        let mut conn = self.redis.clone();
        let info: redis::RedisResult<String> =
            redis::cmd("INFO").arg("memory").query_async(&mut conn).await;
        match info.map(|text| parse_info_memory(&text)) {
            Ok(Some(mem)) => record_redis_memory(mem),
            Ok(None) => {
                metrics::counter!(METRIC_SAMPLE_ERRORS, "source" => "redis_info").increment(1);
                tracing::warn!(event = "storage_sample_failed", source = "redis_info", "INFO memory lacked used_memory or maxmemory");
            }
            Err(error) => {
                metrics::counter!(METRIC_SAMPLE_ERRORS, "source" => "redis_info").increment(1);
                tracing::warn!(event = "storage_sample_failed", source = "redis_info", %error, "Redis memory sample failed");
            }
        }
    }

    /// Spawn the sampler. Both intervals tick immediately, then every 300 s
    /// and 30 s. Abort the handle to stop it.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut db = tokio::time::interval(DB_SAMPLE_INTERVAL);
            db.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut redis = tokio::time::interval(REDIS_SAMPLE_INTERVAL);
            redis.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = db.tick() => self.db_tick().await,
                    _ = redis.tick() => self.redis_tick().await,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_thresholds() {
        assert_eq!(budget_level(0.0), BudgetLevel::Ok);
        assert_eq!(budget_level(0.79), BudgetLevel::Ok);
        assert_eq!(budget_level(0.80), BudgetLevel::Warn);
        assert_eq!(budget_level(0.949), BudgetLevel::Warn);
        assert_eq!(budget_level(0.95), BudgetLevel::Critical);
        assert_eq!(budget_level(1.2), BudgetLevel::Critical);
    }

    #[test]
    fn ratio_handles_unlimited() {
        assert!((ratio(50, 100) - 0.5).abs() < f64::EPSILON);
        assert!(ratio(50, 0).abs() < f64::EPSILON);
    }

    #[test]
    fn parses_info_memory() {
        let info = "# Memory\r\nused_memory:1048576\r\nused_memory_human:1.00M\r\nmaxmemory:6442450944\r\nmaxmemory_human:6.00G\r\nmaxmemory_policy:noeviction\r\n";
        assert_eq!(
            parse_info_memory(info),
            Some(RedisMemory {
                used_memory: 1_048_576,
                maxmemory: 6_442_450_944,
            })
        );
    }

    #[test]
    fn info_memory_missing_fields_is_none() {
        assert_eq!(parse_info_memory("# Memory\r\nused_memory:10\r\n"), None);
        assert_eq!(parse_info_memory(""), None);
    }

    #[test]
    fn metric_names_match_design() {
        assert_eq!(
            [
                METRIC_TABLE_ROWS,
                METRIC_TABLE_BYTES,
                METRIC_DB_SIZE,
                METRIC_DB_BUDGET_RATIO,
                METRIC_REDIS_USED,
                METRIC_REDIS_MAX,
                METRIC_REDIS_RATIO,
                METRIC_STREAM_LENGTH,
                METRIC_SCOPE_DIDS,
                METRIC_MEMBERS,
            ],
            [
                "db_table_rows",
                "db_table_bytes",
                "db_size_bytes",
                "db_budget_ratio",
                "redis_used_memory_bytes",
                "redis_maxmemory_bytes",
                "redis_memory_ratio",
                "firehose_stream_length",
                "scope_dids_total",
                "members_total",
            ]
        );
    }

    #[test]
    fn intervals_match_design() {
        assert_eq!(DB_SAMPLE_INTERVAL, Duration::from_secs(300));
        assert_eq!(REDIS_SAMPLE_INTERVAL, Duration::from_secs(30));
    }
}
