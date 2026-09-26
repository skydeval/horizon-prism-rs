//! Owner readiness (design-v2 §6.11.2): `private_owner_ready`.

use sqlx::{PgConnection, PgPool};

use super::owners;

/// `readiness::clear_all`: the gap transaction's wipe (§6.11.4).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn clear_all(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM private_owner_ready").execute(conn).await?;
    Ok(())
}

/// Clear one DID's owner readiness.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn clear_owner(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM private_owner_ready WHERE did = $1")
        .bind(did)
        .execute(conn)
        .await?;
    Ok(())
}

/// `readiness::mark_owner_ready`: set readiness for a current owner (called
/// inside a job's epoch-checked readiness transaction, §5.10). A non-owner is
/// left unready, so readiness and owner status never disagree (#137).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn mark_owner_ready(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    if !owners::is_owner(&mut *conn, did).await? {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO private_owner_ready (did, ready_at) VALUES ($1, now()) \
         ON CONFLICT (did) DO UPDATE SET ready_at = now()",
    )
    .bind(did)
    .execute(conn)
    .await?;
    Ok(())
}

/// `readiness::owner_ready`.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn owner_ready(pool: &PgPool, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM private_owner_ready WHERE did = $1)")
        .bind(did)
        .fetch_one(pool)
        .await
}
