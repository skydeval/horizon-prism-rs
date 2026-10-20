//! Read-only CLI commands (design-v2 §3.3). They run in the CLI process,
//! never through the admin executor, and change nothing.

pub mod admin_commands_list;
pub mod deadletter_list;
pub mod identity_compare;
pub mod keygen;
pub mod label_list;
pub mod member_list;
pub mod migrate_status;
pub mod operator_list;
pub mod setup_code;
pub mod stream_info;

use redis::aio::ConnectionManager;
use sqlx::PgPool;

use crate::config::AppConfig;

/// Render rows as a left-aligned, space-padded table with a header.
#[must_use]
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if let Some(w) = widths.get_mut(i) {
                *w = (*w).max(cell.chars().count());
            }
        }
    }
    let line = |cells: Vec<&str>| {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let pad = widths[i].saturating_sub(c.chars().count());
                if i + 1 == cells.len() { (*c).to_string() } else { format!("{c}{}", " ".repeat(pad)) }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };
    let mut out = line(headers.to_vec());
    for row in rows {
        out.push('\n');
        out.push_str(&line(row.iter().map(String::as_str).collect()));
    }
    out
}

/// Load configuration for a read-only command.
///
/// # Errors
///
/// A printable message.
pub fn config() -> Result<AppConfig, String> {
    AppConfig::load_from_env().map_err(|e| format!("config_error: {e}"))
}

/// Connect to Postgres for a read-only command.
///
/// # Errors
///
/// A printable message.
pub async fn pool(cfg: &AppConfig) -> Result<PgPool, String> {
    crate::db::connect(cfg).await.map_err(|e| format!("database connect: {e}"))
}

/// Connect to Redis for a read-only command.
///
/// # Errors
///
/// A printable message.
pub async fn redis(cfg: &AppConfig) -> Result<ConnectionManager, String> {
    let client = redis::Client::open(cfg.env.redis_url.as_str()).map_err(|e| format!("redis url: {e}"))?;
    ConnectionManager::new(client).await.map_err(|e| format!("redis connect: {e}"))
}

/// A datetime cell.
#[must_use]
pub fn when(t: Option<chrono::DateTime<chrono::Utc>>) -> String {
    t.map_or_else(|| "-".to_string(), |t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_align() {
        let t = table(&["did", "source"], &[vec!["did:plc:a".into(), "admin".into()], vec!["x".into(), "auto_enroll".into()]]);
        assert_eq!(t, "did        source\ndid:plc:a  admin\nx          auto_enroll");
        assert_eq!(table(&["a"], &[]), "a");
    }
}
