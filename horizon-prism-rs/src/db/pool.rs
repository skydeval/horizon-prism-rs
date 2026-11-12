//! Postgres pool construction (design-v2 §4.1).

use std::str::FromStr;
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::config::AppConfig;

/// The pool's size when nothing else says (tests and one-off tools).
pub const MAX_CONNECTIONS: u32 = 20;
/// Pool acquire timeout (§4.1). A timeout is a transient failure (§5.3).
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
/// `application_name` reported to Postgres.
pub const APPLICATION_NAME: &str = "horizon-prism-rs";
/// A pool this close to the server's `max_connections` is warned about.
pub const MAX_CONNECTIONS_MARGIN: u32 = 5;
/// Metric: the time a writer waited for a pooled connection.
pub const METRIC_ACQUIRE: &str = "db_pool_acquire_seconds";
/// Metric: pooled connections in use, and the pool's size.
pub const METRIC_IN_USE: &str = "db_pool_connections_in_use";
pub const METRIC_SIZE: &str = "db_pool_connections_max";

/// Connection options for `database_url`.
///
/// `lock_timeout` is set as a session parameter on every pooled connection.
/// It is the §6.11.7 backstop against lock waits ("every transaction sets
/// `lock_timeout = '30s'`"); as the session default it applies to every
/// transaction without a per-transaction statement.
///
/// JIT compilation is off on every pooled connection. The request paths are
/// short indexed statements, but the planner's cost for a large private
/// read batch passes `jit_above_cost`, and compiling it took about 3 s a
/// statement (B-5 part (3), 2026-10-17), far more than running it.
///
/// # Errors
///
/// [`sqlx::Error::Configuration`] when the URL does not parse.
pub fn connect_options(
    database_url: &str,
    lock_timeout_secs: u64,
) -> Result<PgConnectOptions, sqlx::Error> {
    Ok(PgConnectOptions::from_str(database_url)?
        .application_name(APPLICATION_NAME)
        .options([("lock_timeout", format!("{lock_timeout_secs}s")), ("jit", "off".to_string())]))
}

/// Pool options with the §4.1 settings, `size` connections at most.
#[must_use]
pub fn pool_options_sized(size: u32) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(size.max(1))
        .acquire_timeout(ACQUIRE_TIMEOUT)
}

/// Pool options with the §4.1 settings and the default size.
#[must_use]
pub fn pool_options() -> PgPoolOptions {
    pool_options_sized(MAX_CONNECTIONS)
}

/// Connect the shared pool from `APP_DATABASE_URL` and `[db]` settings.
///
/// # Errors
///
/// Any [`sqlx::Error`] from parsing the URL or opening the first connection.
///
/// Every connection also carries the two settings the trend triggers read
/// (design-v3 §7.5): how old a row may be and still be aggregated, and how
/// long after a post its engagement still counts. A session without them,
/// an operator's `psql` say, gets the triggers' own defaults.
pub async fn connect(cfg: &AppConfig) -> Result<PgPool, sqlx::Error> {
    let trends = &cfg.file.trends;
    let options = connect_options(cfg.env.database_url.expose(), cfg.file.db.lock_timeout_secs)?.options([
        ("prism.trends_retention_hours", trends.aggregate_retention_hours.to_string()),
        ("prism.trends_subject_lookback_hours", trends.subject_lookback_hours.to_string()),
    ]);
    let size = cfg.file.db.pool_size.connections(&cfg.file);
    let pool = pool_options_sized(size).connect_with(options).await?;
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(METRIC_SIZE).set(f64::from(size));
    match server_max_connections(&pool).await {
        Ok(max) => {
            if let Some(warning) = pool_warning(size, max) {
                tracing::warn!(event = "db_pool_near_max_connections", pool_size = size, max_connections = max, "{warning}");
            }
        }
        Err(error) => tracing::debug!(event = "db_max_connections_unread", %error, "the server's max_connections could not be read"),
    }
    Ok(pool)
}

/// The server's `max_connections`.
///
/// # Errors
///
/// Database errors.
pub async fn server_max_connections(pool: &PgPool) -> Result<u32, sqlx::Error> {
    let v: String = sqlx::query_scalar("SELECT current_setting('max_connections')").fetch_one(pool).await?;
    Ok(v.trim().parse().unwrap_or(u32::MAX))
}

/// The warning for a pool of `size` on a server allowing `max_connections`:
/// within [`MAX_CONNECTIONS_MARGIN`] of it, or over it. Never a refusal.
#[must_use]
pub fn pool_warning(size: u32, max_connections: u32) -> Option<String> {
    (size.saturating_add(MAX_CONNECTIONS_MARGIN) > max_connections).then(|| {
        format!(
            "db.pool_size is {size} connections and the database server allows {max_connections} (max_connections): \
             other clients, a second Prism process or the admin CLI may find no connection free. Raise max_connections or lower db.pool_size."
        )
    })
}

/// Record how long a writer waited for a pooled connection.
pub fn record_acquire(d: Duration) {
    metrics::histogram!(METRIC_ACQUIRE).record(d.as_secs_f64());
}

/// Sample the pool's connections in use.
pub fn sample_in_use(pool: &PgPool) {
    let in_use = pool.size().saturating_sub(u32::try_from(pool.num_idle()).unwrap_or(u32::MAX));
    metrics::gauge!(METRIC_IN_USE).set(f64::from(in_use));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_options_parse_and_carry_lock_timeout() {
        let opts = connect_options("postgres://prism:pw@db:5432/prism", 30).expect("valid url");
        assert_eq!(opts.get_host(), "db");
        assert_eq!(opts.get_port(), 5432);
        assert_eq!(opts.get_database(), Some("prism"));
        assert_eq!(opts.get_options(), Some("-c lock_timeout=30s -c jit=off"));
    }

    #[test]
    fn connect_options_reject_garbage() {
        assert!(connect_options("not a url", 30).is_err());
    }

    #[test]
    fn pool_settings_match_design() {
        let opts = pool_options();
        assert_eq!(opts.get_max_connections(), 20);
        assert_eq!(opts.get_acquire_timeout(), Duration::from_secs(5));
        assert_eq!(pool_options_sized(37).get_max_connections(), 37);
    }

    #[test]
    fn the_auto_pool_follows_the_writers_and_warns_near_the_servers_limit() {
        use crate::config::{FileConfig, PoolSize};
        let mut file = FileConfig::default();
        // 16 + the counts folder + 1 lane + 4 claimers + 4 backfill workers.
        assert_eq!(file.db.pool_size, PoolSize::Auto);
        assert_eq!(file.db.pool_size.connections(&file), 26);
        file.firehose.writer_lanes = 8;
        file.full_crawl.concurrency = 6;
        file.backfill.concurrency = 2;
        assert_eq!(file.db.pool_size.connections(&file), 33);
        assert_eq!(PoolSize::Fixed(12).connections(&file), 12);
        // Postgres's default max_connections is 100.
        assert_eq!(pool_warning(32, 100), None);
        assert_eq!(pool_warning(95, 100), None);
        assert!(pool_warning(96, 100).expect("warns").contains("db.pool_size is 96"));
        assert!(pool_warning(150, 100).is_some());
    }

    #[test]
    fn pool_size_reads_auto_or_a_number() {
        use crate::config::PoolSize;
        #[derive(serde::Deserialize)]
        struct T {
            pool_size: PoolSize,
        }
        let t: T = toml::from_str("pool_size = \"auto\"").expect("auto");
        assert_eq!(t.pool_size, PoolSize::Auto);
        let t: T = toml::from_str("pool_size = 40").expect("number");
        assert_eq!(t.pool_size, PoolSize::Fixed(40));
        assert!(toml::from_str::<T>("pool_size = \"big\"").is_err());
    }
}
