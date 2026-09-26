//! `admin commands list [--state <state>]`.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;

use super::{table, when};

/// Render queued/finished admin commands, newest last.
///
/// # Errors
///
/// Database errors.
pub async fn render(pool: &PgPool, state: Option<&str>) -> Result<String, sqlx::Error> {
    type Row = (i64, String, Value, String, Option<Value>, DateTime<Utc>, Option<DateTime<Utc>>);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, command, args, state, result, created_at, finished_at FROM admin_commands \
         WHERE ($1::text IS NULL OR state = $1) ORDER BY created_at, id",
    )
    .bind(state)
    .fetch_all(pool)
    .await?;
    let rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|(id, command, args, state, result, created, finished)| {
            vec![
                id.to_string(),
                command,
                args.to_string(),
                state,
                when(Some(created)),
                when(finished),
                result.map_or_else(|| "-".to_string(), |r| r.to_string()),
            ]
        })
        .collect();
    Ok(table(&["id", "command", "args", "state", "created_at", "finished_at", "result"], &rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn lists_and_filters_by_state() {
        let Some(db) = fresh_db("cli_commands_list").await else {
            return;
        };
        sqlx::query(
            "INSERT INTO admin_commands (command, args, state, created_at) VALUES \
             ('label add', '{\"val\":\"porn\"}', 'queued', '2026-09-26T12:00:00Z'), \
             ('resync', '{}', 'done', '2026-09-26T12:01:00Z')",
        )
        .execute(&db.pool)
        .await
        .expect("rows");
        let all = render(&db.pool, None).await.expect("render");
        assert_eq!(all.lines().count(), 3);
        let queued = render(&db.pool, Some("queued")).await.expect("render");
        assert!(queued.lines().nth(1).expect("row").contains("label add"));
        assert_eq!(queued.lines().count(), 2);
        db.drop_db().await;
    }
}
