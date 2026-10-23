//! `admin refract status`, `list [--state]` and `show <did>` (refract design
//! §9). Read-only; `check`, `end`, `keep` and `unkeep` go through `serve`.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;

use super::{table, when};
use crate::admin::settings::{overlay, registry, store};
use crate::config::{FileConfig, RefractCommunity};
use crate::refract::communities::entry_hash;

/// A `refract_dids` row as `list` shows it.
type DidRow = (String, String, String, Option<String>, Option<DateTime<Utc>>, DateTime<Utc>);
/// A `refract_labels` row as `show` shows it.
type LabelRow = (String, String, String, Option<DateTime<Utc>>, Option<DateTime<Utc>>, bool, bool);
/// A `refract_events` row.
type EventRow = (DateTime<Utc>, Option<String>, String, Option<String>);

/// The value `serve` runs with for `key`: the saved one unless it was set
/// aside at the last start, else `app.toml`'s.
async fn effective(pool: &PgPool, file: &FileConfig, key: &str) -> Result<Value, sqlx::Error> {
    let saved = store::rows(pool).await?;
    Ok(match saved.into_iter().find(|r| r.key == key && r.set_aside.is_none()) {
        Some(r) => r.value,
        None => registry::value_at(&overlay::json(file), key),
    })
}

/// The switch, each community with its entry hash and its DIDs by state,
/// the pending ends, and each labeler's cursor.
///
/// # Errors
///
/// A printable message.
pub async fn render_status(pool: &PgPool, file: &FileConfig) -> Result<String, String> {
    let err = |e: sqlx::Error| e.to_string();
    let enabled = effective(pool, file, "refract.enabled").await.map_err(err)?;
    let communities: Vec<RefractCommunity> =
        serde_json::from_value(effective(pool, file, "refract.communities").await.map_err(err)?).map_err(|e| format!("refract.communities: {e}"))?;
    let counts: Vec<(String, String, i64)> = sqlx::query_as("SELECT community, state, count(*) FROM refract_dids GROUP BY community, state ORDER BY community, state")
        .fetch_all(pool)
        .await
        .map_err(err)?;
    let mut out = vec![format!("refract.enabled: {}", enabled.as_bool().unwrap_or(false)), String::new()];
    let states = ["candidate", "rejected", "restored", "grace", "ending"];
    let mut rows: Vec<Vec<String>> = communities
        .iter()
        .map(|c| {
            let mut row = vec![c.name.clone(), entry_hash(c), c.tier.to_string(), if c.enabled { "yes" } else { "no" }.to_string()];
            for s in states {
                let n = counts.iter().find(|(name, st, _)| *name == c.name && st == s).map_or(0, |(_, _, n)| *n);
                row.push(n.to_string());
            }
            row
        })
        .collect();
    // Rows of communities no longer listed.
    let mut unlisted: Vec<&String> = counts.iter().map(|(c, _, _)| c).filter(|c| !communities.iter().any(|x| &x.name == *c)).collect();
    unlisted.dedup();
    for name in unlisted {
        let mut row = vec![name.clone(), "-".to_string(), "-".to_string(), "unlisted".to_string()];
        for s in states {
            let n = counts.iter().find(|(c, st, _)| c == name && st == s).map_or(0, |(_, _, n)| *n);
            row.push(n.to_string());
        }
        rows.push(row);
    }
    out.push(table(&["community", "entry", "tier", "enabled", "candidate", "rejected", "restored", "grace", "ending"], &rows));
    let pending: Vec<(String, DateTime<Utc>, bool, i64)> = sqlx::query_as(
        "SELECT p.scope, p.first_seen, p.keep, (SELECT count(*) FROM refract_pending_dids d WHERE d.scope = p.scope) \
         FROM refract_pending p ORDER BY p.scope",
    )
    .fetch_all(pool)
    .await
    .map_err(err)?;
    if !pending.is_empty() {
        out.push(String::new());
        let rows: Vec<Vec<String>> = pending
            .into_iter()
            .map(|(scope, first, keep, n)| vec![scope, when(Some(first)), if keep { "kept" } else { "-" }.to_string(), n.to_string()])
            .collect();
        out.push(table(&["pending", "since", "keep", "dids"], &rows));
    }
    let cursors: Vec<(String, Value, DateTime<Utc>)> = sqlx::query_as("SELECT key, value, updated_at FROM worker_state WHERE key LIKE 'refract_label_cursor:%' ORDER BY key")
        .fetch_all(pool)
        .await
        .map_err(err)?;
    let newest: Vec<(String, Option<DateTime<Utc>>)> = sqlx::query_as("SELECT labeler_did, max(cts) FROM refract_labels GROUP BY labeler_did")
        .fetch_all(pool)
        .await
        .map_err(err)?;
    if !cursors.is_empty() {
        out.push(String::new());
        let rows: Vec<Vec<String>> = cursors
            .into_iter()
            .map(|(key, value, at)| {
                let labeler = key.trim_start_matches("refract_label_cursor:").to_string();
                let latest = newest.iter().find(|(l, _)| *l == labeler).and_then(|(_, t)| *t);
                let lag = latest.map_or_else(|| "-".to_string(), |t| format!("{}s", (Utc::now() - t).num_seconds().max(0)));
                vec![labeler, value.as_str().unwrap_or("-").to_string(), when(Some(at)), lag]
            })
            .collect();
        out.push(table(&["labeler", "cursor", "advanced", "newest label age"], &rows));
    }
    Ok(out.join("\n"))
}

/// The accounts Refract knows, optionally in one state.
///
/// # Errors
///
/// A printable message.
pub async fn render_list(pool: &PgPool, state: Option<&str>) -> Result<String, String> {
    if let Some(s) = state
        && crate::refract::State::parse(s).is_none()
    {
        return Err(format!("unknown state {s:?}: candidate, rejected, restored, grace or ending"));
    }
    let rows: Vec<DidRow> = sqlx::query_as(
        "SELECT did, community, state, pds_host, checked_at, next_check_at FROM refract_dids \
         WHERE ($1::text IS NULL OR state = $1) ORDER BY community, state, did",
    )
    .bind(state)
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;
    let rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|(did, community, state, host, checked, next)| vec![did, community, state, host.unwrap_or_else(|| "-".to_string()), when(checked), when(Some(next))])
        .collect();
    Ok(table(&["did", "community", "state", "pds", "checked", "next check"], &rows))
}

/// One account: its row, its last verdict, its labels and its events.
///
/// # Errors
///
/// A printable message.
pub async fn render_show(pool: &PgPool, did: &str) -> Result<String, String> {
    let err = |e: sqlx::Error| e.to_string();
    let row: Option<Value> = sqlx::query_scalar("SELECT to_jsonb(d) - 'last_verdict' FROM refract_dids d WHERE did = $1")
        .bind(did)
        .fetch_optional(pool)
        .await
        .map_err(err)?;
    let Some(row) = row else {
        return Err(format!("Refract has no row for {did}"));
    };
    let verdict: Option<Value> = sqlx::query_scalar("SELECT last_verdict FROM refract_dids WHERE did = $1").bind(did).fetch_one(pool).await.map_err(err)?;
    let mut out = vec![serde_json::to_string_pretty(&row).unwrap_or_default()];
    out.push(format!("last verdict: {}", verdict.map_or_else(|| "-".to_string(), |v| v.to_string())));
    let labels: Vec<LabelRow> = sqlx::query_as(
        "SELECT labeler_did, subject, val, cts, exp, gone, verified FROM refract_labels WHERE did = $1 ORDER BY cts DESC NULLS LAST LIMIT 50",
    )
    .bind(did)
    .fetch_all(pool)
    .await
    .map_err(err)?;
    out.push(String::new());
    let rows: Vec<Vec<String>> = labels
        .into_iter()
        .map(|(l, s, v, cts, exp, gone, verified)| vec![l, s, v, when(cts), when(exp), gone.to_string(), verified.to_string()])
        .collect();
    out.push(table(&["labeler", "subject", "val", "cts", "exp", "gone", "verified"], &rows));
    let events: Vec<EventRow> =
        sqlx::query_as("SELECT at, from_state, to_state, reason FROM refract_events WHERE did = $1 ORDER BY at DESC, id DESC LIMIT 50")
            .bind(did)
            .fetch_all(pool)
            .await
            .map_err(err)?;
    out.push(String::new());
    let rows: Vec<Vec<String>> = events
        .into_iter()
        .map(|(at, from, to, reason)| vec![when(Some(at)), from.unwrap_or_else(|| "-".to_string()), to, reason.unwrap_or_default()])
        .collect();
    out.push(table(&["at", "from", "to", "reason"], &rows));
    Ok(out.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn status_list_and_show() {
        let Some(db) = fresh_db("cli_refract").await else {
            return;
        };
        let p = &db.pool;
        let did = "did:plc:restoredaaaaaaaaaaaaaaaa";
        sqlx::query("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ($1, 'community-a', 'community-a.example', 'restored', 'fill')")
            .bind(did)
            .execute(p)
            .await
            .expect("row");
        sqlx::query("INSERT INTO refract_events (did, from_state, to_state, reason) VALUES ($1, 'candidate', 'restored', 'rules')")
            .bind(did)
            .execute(p)
            .await
            .expect("event");
        let status = render_status(p, &FileConfig::default()).await.expect("status");
        assert!(status.starts_with("refract.enabled: false"), "{status}");
        assert!(status.lines().any(|l| l.starts_with("community-a") && l.contains(&entry_hash(&RefractCommunity::example()))), "{status}");
        let listed = render_list(p, Some("restored")).await.expect("list");
        assert!(listed.contains(did));
        assert!(render_list(p, Some("grace")).await.expect("list").lines().count() == 1);
        assert!(render_list(p, Some("nope")).await.is_err());
        let shown = render_show(p, did).await.expect("show");
        assert!(shown.contains("\"state\": \"restored\"") && shown.contains("rules"), "{shown}");
        assert!(render_show(p, "did:plc:nobody").await.is_err());
        db.drop_db().await;
    }
}
