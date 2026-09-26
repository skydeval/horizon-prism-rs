//! `admin member list`: did, source, added_at, ready_at.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::{table, when};

/// Render the members table.
///
/// # Errors
///
/// Database errors.
pub async fn render(pool: &PgPool) -> Result<String, sqlx::Error> {
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
        let out = render(&db.pool).await.expect("render");
        assert_eq!(
            out,
            "did                               source  added_at              ready_at\n\
             did:plc:aaaaaaaaaaaaaaaaaaaaaaaa  admin   2026-09-26T12:00:00Z  -"
        );
        db.drop_db().await;
    }
}
