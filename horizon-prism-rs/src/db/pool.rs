//! Postgres pool construction (design-v2 §4.1).

use std::str::FromStr;
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::config::AppConfig;

/// Pool size (§4.1).
pub const MAX_CONNECTIONS: u32 = 20;
/// Pool acquire timeout (§4.1). A timeout is a transient failure (§5.3).
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
/// `application_name` reported to Postgres.
pub const APPLICATION_NAME: &str = "horizon-prism-rs";

/// Connection options for `database_url`.
///
/// `lock_timeout` is set as a session parameter on every pooled connection.
/// It is the §6.11.7 backstop against lock waits ("every transaction sets
/// `lock_timeout = '30s'`"); as the session default it applies to every
/// transaction without a per-transaction statement.
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
        .options([("lock_timeout", format!("{lock_timeout_secs}s"))]))
}

/// Pool options with the §4.1 settings.
#[must_use]
pub fn pool_options() -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .acquire_timeout(ACQUIRE_TIMEOUT)
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
    pool_options().connect_with(options).await
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
        assert_eq!(opts.get_options(), Some("-c lock_timeout=30s"));
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
    }
}
