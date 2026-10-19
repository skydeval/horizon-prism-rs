//! `admin setup-code` (admin-ui design §5.2, §7): prints the first-run
//! setup code from its file while no operator exists. Read-only: it never
//! makes a code; `serve` does, at startup.

use std::path::Path;

use sqlx::PgPool;

use crate::admin::setup_code;

/// The code and its expiry, or why there is none.
///
/// # Errors
///
/// A printable failure.
pub async fn render(pool: &PgPool, data_dir: &Path) -> Result<String, String> {
    if setup_code::operators_exist(pool).await.map_err(|e| e.to_string())? {
        return Err("An operator exists, so first-run setup is closed. Use `admin operator add` to add another.".to_string());
    }
    match setup_code::current(pool, &setup_code::file_path(data_dir)).await.map_err(|e| e.to_string())? {
        Some(open) => Ok(format!(
            "{}\nEnter it at /admin/setup before {}.",
            open.code,
            open.expires_at.format("%Y-%m-%d %H:%M UTC")
        )),
        None => Err("No valid setup code: it expired or Prism has not started yet. Restart Prism for a new one, or run `admin operator add`.".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    /// T-A1, with the CLI: it prints the code `serve` made, and nothing once
    /// an operator exists.
    #[tokio::test]
    async fn prints_the_code_only_while_setup_is_open() {
        let Some(db) = fresh_db("cli_setup_code").await else {
            return;
        };
        let dir = std::env::temp_dir().join(format!("hprs-cli-setup-{}", rand::random::<u32>()));
        std::fs::create_dir_all(&dir).expect("dir");
        assert!(render(&db.pool, &dir).await.is_err(), "no code yet");
        let open = setup_code::at_startup(&db.pool, &setup_code::file_path(&dir)).await.expect("startup").expect("open");
        let out = render(&db.pool, &dir).await.expect("code");
        assert!(out.starts_with(&open.code), "{out}");
        sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'cli')")
            .execute(&db.pool)
            .await
            .expect("operator");
        assert!(render(&db.pool, &dir).await.expect_err("closed").contains("closed"));
        let _ = std::fs::remove_dir_all(&dir);
        db.drop_db().await;
    }
}
