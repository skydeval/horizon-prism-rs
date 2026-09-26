//! `admin label list [--subject <uri-or-did>] [--val <val>]`.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::{table, when};

/// Render labels, optionally filtered.
///
/// # Errors
///
/// Database errors.
pub async fn render(pool: &PgPool, subject: Option<&str>, val: Option<&str>) -> Result<String, sqlx::Error> {
    let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT subject, val, created_at FROM labels \
         WHERE ($1::text IS NULL OR subject = $1) AND ($2::text IS NULL OR val = $2) \
         ORDER BY created_at, id",
    )
    .bind(subject)
    .bind(val)
    .fetch_all(pool)
    .await?;
    let rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|(s, v, t)| vec![s, v, when(Some(t))])
        .collect();
    Ok(table(&["subject", "val", "created_at"], &rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn filters_by_subject_and_value() {
        let Some(db) = fresh_db("cli_label_list").await else {
            return;
        };
        for (s, v) in [("did:plc:a", "porn"), ("did:plc:a", "!hide"), ("did:plc:b", "porn")] {
            sqlx::query("INSERT INTO labels (subject, val, created_at) VALUES ($1, $2, '2026-09-26T12:00:00Z')")
                .bind(s)
                .bind(v)
                .execute(&db.pool)
                .await
                .expect("label");
        }
        let all = render(&db.pool, None, None).await.expect("render");
        assert_eq!(all.lines().count(), 4);
        let by_subject = render(&db.pool, Some("did:plc:a"), None).await.expect("render");
        assert_eq!(by_subject.lines().count(), 3);
        let by_val = render(&db.pool, None, Some("porn")).await.expect("render");
        assert_eq!(
            by_val,
            "subject    val   created_at\ndid:plc:a  porn  2026-09-26T12:00:00Z\ndid:plc:b  porn  2026-09-26T12:00:00Z"
        );
        db.drop_db().await;
    }
}
