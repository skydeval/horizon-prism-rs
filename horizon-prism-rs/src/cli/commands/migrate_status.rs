//! `migrate status`: applied and pending migrations (read-only; never
//! creates `_sqlx_migrations`).

use sqlx::PgPool;

use crate::db::migrations::{MIGRATOR, applied_migrations, known_migrations, plan};

/// Render the migration status.
///
/// # Errors
///
/// Database errors.
pub async fn render(pool: &PgPool) -> Result<String, sqlx::Error> {
    let known = known_migrations(&MIGRATOR);
    let applied = applied_migrations(pool).await?;
    let gate = plan(&known, &applied);
    let mut out = Vec::new();
    for k in &known {
        let state = if gate.pending.iter().any(|p| p.version == k.version) {
            "pending"
        } else if gate.mismatched.contains(&k.version) {
            "applied (checksum mismatch)"
        } else if gate.dirty.contains(&k.version) {
            "failed (dirty)"
        } else {
            "applied"
        };
        out.push(format!("{}  {state}", k.name()));
    }
    for v in &gate.unknown {
        out.push(format!("{v:04}  applied (unknown to this binary)"));
    }
    out.push(if gate.is_inconsistent() {
        "status: inconsistent (serve refuses to start, exit 78)".to_string()
    } else if gate.pending.is_empty() {
        "status: up to date".to_string()
    } else {
        format!("status: {} pending", gate.pending.len())
    });
    Ok(out.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn reports_applied_migrations() {
        let Some(db) = fresh_db("cli_migrate_status").await else {
            return;
        };
        let out = render(&db.pool).await.expect("render");
        assert_eq!(out, "0001_initial  applied\n0002_private_reply_cids  applied\n0003_eligibility_failures  applied\nstatus: up to date");
        db.drop_db().await;
    }
}
