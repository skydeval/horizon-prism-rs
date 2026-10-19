//! `admin member list`: did, source, added_at, ready_at. With `--denied`,
//! the denials instead (membership design §9): did, denied_by, denied_at,
//! reason.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::{table, when};

/// Render the members table, or with `denied` the denials table.
///
/// # Errors
///
/// Database errors.
pub async fn render(pool: &PgPool, denied: bool) -> Result<String, sqlx::Error> {
    if denied {
        return render_denials(pool).await;
    }
    type Row = (String, String, DateTime<Utc>, Option<DateTime<Utc>>);
    let rows: Vec<Row> =
        sqlx::query_as("SELECT did, source, added_at, ready_at FROM members ORDER BY added_at, did")
            .fetch_all(pool)
            .await?;
    let rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|(did, source, added, ready)| vec![did, source, when(Some(added)), when(ready)])
        .collect();
    Ok(table(&["did", "source", "added_at", "ready_at"], &rows))
}

async fn render_denials(pool: &PgPool) -> Result<String, sqlx::Error> {
    type Row = (String, String, DateTime<Utc>, Option<String>);
    let rows: Vec<Row> =
        sqlx::query_as("SELECT did, denied_by, denied_at, reason FROM member_denials ORDER BY denied_at, did")
            .fetch_all(pool)
            .await?;
    let rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|(did, by, at, reason)| vec![did, by, when(Some(at)), reason.unwrap_or_else(|| "-".to_string())])
        .collect();
    Ok(table(&["did", "denied_by", "denied_at", "reason"], &rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn lists_members() {
        let Some(db) = fresh_db("cli_member_list").await else {
            return;
        };
        sqlx::query("INSERT INTO members (did, source, added_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin', '2026-09-26T12:00:00Z')")
            .execute(&db.pool)
            .await
            .expect("member");
        let out = render(&db.pool, false).await.expect("render");
        assert_eq!(
            out,
            "did                               source  added_at              ready_at\n\
             did:plc:aaaaaaaaaaaaaaaaaaaaaaaa  admin   2026-09-26T12:00:00Z  -"
        );
        sqlx::query(
            "INSERT INTO member_denials (did, reason, denied_by, denied_at) \
             VALUES ('did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', 'spam', 'cli', '2026-09-27T12:00:00Z')",
        )
        .execute(&db.pool)
        .await
        .expect("denial");
        let out = render(&db.pool, true).await.expect("render");
        assert_eq!(
            out,
            "did                               denied_by  denied_at             reason\n\
             did:plc:bbbbbbbbbbbbbbbbbbbbbbbb  cli        2026-09-27T12:00:00Z  spam"
        );
        db.drop_db().await;
    }
}
