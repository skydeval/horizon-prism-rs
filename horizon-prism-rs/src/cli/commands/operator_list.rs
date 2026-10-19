//! `admin operator list` (admin-ui design §5.7): the operators (DID, note,
//! last known server, who added them) and the allowed servers.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::{table, when};
use crate::admin::operators::endpoint_host;

/// Render both tables. An empty server list allows any server.
///
/// # Errors
///
/// Database errors.
pub async fn render(pool: &PgPool) -> Result<String, sqlx::Error> {
    type Row = (String, Option<String>, Option<String>, String, DateTime<Utc>);
    let operators: Vec<Row> =
        sqlx::query_as("SELECT did, note, pds_endpoint, added_by, added_at FROM admin_operators ORDER BY added_at, did")
            .fetch_all(pool)
            .await?;
    let hosts: Vec<(String, String, DateTime<Utc>)> =
        sqlx::query_as("SELECT pattern, added_by, added_at FROM admin_operator_hosts ORDER BY pattern")
            .fetch_all(pool)
            .await?;
    let operators: Vec<Vec<String>> = operators
        .into_iter()
        .map(|(did, note, endpoint, by, at)| {
            vec![
                did,
                note.unwrap_or_else(|| "-".to_string()),
                endpoint.as_deref().and_then(endpoint_host).unwrap_or_else(|| "-".to_string()),
                by,
                when(Some(at)),
            ]
        })
        .collect();
    let any = hosts.is_empty();
    let hosts: Vec<Vec<String>> = hosts.into_iter().map(|(p, by, at)| vec![p, by, when(Some(at))]).collect();
    let mut out = table(&["did", "note", "host", "added_by", "added_at"], &operators);
    out.push_str("\n\n");
    out.push_str(&table(&["allowed_host", "added_by", "added_at"], &hosts));
    if any {
        out.push_str("\n(no host listed: an operator on any host can sign in)");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn lists_operators_and_hosts() {
        let Some(db) = fresh_db("cli_operator_list").await else {
            return;
        };
        let out = render(&db.pool).await.expect("render");
        assert!(out.contains("any host"), "{out}");
        sqlx::query(
            "INSERT INTO admin_operators (did, added_by, added_at, note, pds_endpoint) \
             VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'setup', '2026-10-19T12:00:00Z', 'me', 'https://pds.example.com')",
        )
        .execute(&db.pool)
        .await
        .expect("operator");
        sqlx::query("INSERT INTO admin_operator_hosts (pattern, added_by, added_at) VALUES ('pds.example.com', 'setup', '2026-10-19T12:00:00Z')")
            .execute(&db.pool)
            .await
            .expect("host");
        let out = render(&db.pool).await.expect("render");
        assert_eq!(
            out,
            "did                               note  host             added_by  added_at\n\
             did:plc:aaaaaaaaaaaaaaaaaaaaaaaa  me    pds.example.com  setup     2026-10-19T12:00:00Z\n\n\
             allowed_host     added_by  added_at\n\
             pds.example.com  setup     2026-10-19T12:00:00Z"
        );
        db.drop_db().await;
    }
}
