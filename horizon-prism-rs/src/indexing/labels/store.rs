//! `external_labels` rows: a subscription's writes and the views' reads.

use std::collections::HashMap;

use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};

use super::{Progress, STATE_KEY};
use crate::refract::labels::Label;

/// One stored label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub src: String,
    pub uri: String,
    /// The record version the label is about; empty for none.
    pub cid: String,
    pub val: String,
    pub neg: bool,
    pub cts: DateTime<Utc>,
    pub exp: Option<DateTime<Utc>>,
    pub sig: Option<Vec<u8>>,
    pub verified: bool,
}

impl Row {
    /// The row of `label`, or `None` when it has no `cts`.
    #[must_use]
    pub fn of(label: &Label, verified: bool) -> Option<Self> {
        let sig = label.raw.get("sig").and_then(|s| match proto_blue::lex_json::json_to_lex(s) {
            proto_blue::lex_data::LexValue::Bytes(b) => Some(b),
            _ => None,
        });
        Some(Self {
            src: label.src.clone(),
            uri: label.uri.clone(),
            cid: label.raw.get("cid").and_then(Value::as_str).unwrap_or_default().to_string(),
            val: label.val.clone(),
            neg: label.neg,
            cts: label.cts?,
            exp: label.exp,
            sig,
            verified,
        })
    }

    /// The label as a view carries it.
    #[must_use]
    pub fn view(&self) -> Value {
        let mut v = json!({
            "ver": 1,
            "src": self.src,
            "uri": self.uri,
            "val": self.val,
            "cts": self.cts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        });
        if !self.cid.is_empty() {
            v["cid"] = json!(self.cid);
        }
        if let Some(exp) = self.exp {
            v["exp"] = json!(exp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        }
        if let Some(sig) = &self.sig {
            v["sig"] = json!({ "$bytes": base64::engine::general_purpose::STANDARD_NO_PAD.encode(sig) });
        }
        v
    }

    fn key(&self) -> (&str, &str, &str, &str) {
        (&self.src, &self.uri, &self.cid, &self.val)
    }
}

/// Of `rows`, the newest per key (the later one on a tie), in arrival order.
#[must_use]
pub fn newest_per_key(rows: Vec<Row>) -> Vec<Row> {
    let mut at: HashMap<(String, String, String, String), usize> = HashMap::with_capacity(rows.len());
    let mut out: Vec<Row> = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row.key();
        let key = (key.0.to_string(), key.1.to_string(), key.2.to_string(), key.3.to_string());
        match at.get(&key) {
            Some(&i) if row.cts >= out[i].cts => out[i] = row,
            Some(_) => {}
            None => {
                at.insert(key, out.len());
                out.push(row);
            }
        }
    }
    out
}

const UPSERT_SQL: &str = "INSERT INTO external_labels (src, uri, cid, val, neg, cts, exp, sig, verified) \
     SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bool[], $6::timestamptz[], $7::timestamptz[], $8::bytea[], $9::bool[]) \
     ON CONFLICT (src, uri, cid, val) DO UPDATE SET neg = EXCLUDED.neg, cts = EXCLUDED.cts, exp = EXCLUDED.exp, sig = EXCLUDED.sig, verified = EXCLUDED.verified \
     WHERE EXCLUDED.cts >= external_labels.cts";

/// Store `rows` (newest `cts` per key wins, against stored rows too).
///
/// # Errors
///
/// Database errors.
pub async fn upsert(conn: &mut PgConnection, rows: Vec<Row>) -> Result<(), sqlx::Error> {
    let rows = newest_per_key(rows);
    if rows.is_empty() {
        return Ok(());
    }
    let n = rows.len();
    let (mut src, mut uri, mut cid, mut val) = (Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n));
    let (mut neg, mut cts, mut exp, mut sig, mut verified) = (Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n));
    for r in rows {
        src.push(r.src);
        uri.push(r.uri);
        cid.push(r.cid);
        val.push(r.val);
        neg.push(r.neg);
        cts.push(r.cts);
        exp.push(r.exp);
        sig.push(r.sig);
        verified.push(r.verified);
    }
    sqlx::query(UPSERT_SQL)
        .bind(src)
        .bind(uri)
        .bind(cid)
        .bind(val)
        .bind(neg)
        .bind(cts)
        .bind(exp)
        .bind(sig)
        .bind(verified)
        .execute(conn)
        .await?;
    Ok(())
}

/// Store a subscription's progress under its labeler.
///
/// # Errors
///
/// Database errors.
pub async fn save_progress(conn: &mut PgConnection, did: &str, p: &Progress) -> Result<(), sqlx::Error> {
    let value = serde_json::to_value(p).unwrap_or(Value::Null);
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, jsonb_build_object($2::text, $3::jsonb), now()) \
         ON CONFLICT (key) DO UPDATE SET value = worker_state.value || EXCLUDED.value, updated_at = now()",
    )
    .bind(STATE_KEY)
    .bind(did)
    .bind(value)
    .execute(conn)
    .await?;
    Ok(())
}

/// Every subscription's stored progress.
///
/// # Errors
///
/// Database errors.
pub async fn load_progress(pool: &PgPool) -> Result<HashMap<String, Progress>, sqlx::Error> {
    let v: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(STATE_KEY).fetch_optional(pool).await?;
    Ok(v.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default())
}

type StoredRow = (String, String, String, String, DateTime<Utc>, Option<DateTime<Utc>>, Option<Vec<u8>>);

/// The labels that apply now: verified, not negated, not expired, of
/// `subjects` from `srcs`.
///
/// # Errors
///
/// Database errors.
pub async fn applied(pool: &PgPool, subjects: &[String], srcs: &[String]) -> Result<Vec<Row>, sqlx::Error> {
    let rows: Vec<StoredRow> = sqlx::query_as(
        "SELECT src, uri, cid, val, cts, exp, sig FROM external_labels \
         WHERE uri = ANY($1) AND src = ANY($2) AND verified AND NOT neg AND (exp IS NULL OR exp > now()) \
         ORDER BY uri, src, cts, val",
    )
    .bind(subjects)
    .bind(srcs)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(src, uri, cid, val, cts, exp, sig)| Row { src, uri, cid, val, neg: false, cts, exp, sig, verified: true })
        .collect())
}

/// The `(src, subject)` pairs of `subjects` and `srcs` that carry a label
/// whose signature did not verify.
///
/// # Errors
///
/// Database errors.
pub async fn unverified_subjects(pool: &PgPool, subjects: &[String], srcs: &[String]) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as("SELECT DISTINCT src, uri FROM external_labels WHERE NOT verified AND src = ANY($2) AND uri = ANY($1)")
        .bind(subjects)
        .bind(srcs)
        .fetch_all(pool)
        .await
}

/// The stored CIDs of the posts that `rows` with a record version name.
///
/// # Errors
///
/// Database errors.
pub async fn stored_post_cids(pool: &PgPool, rows: &[Row]) -> Result<HashMap<String, String>, sqlx::Error> {
    let mut uris: Vec<String> = rows.iter().filter(|r| !r.cid.is_empty()).map(|r| r.uri.clone()).collect();
    uris.dedup();
    if uris.is_empty() {
        return Ok(HashMap::new());
    }
    let found: Vec<(String, Vec<u8>)> = sqlx::query_as(&format!("SELECT u.uri, p.cid FROM {}", crate::db::posts::rows_named(1))).bind(&uris).fetch_all(pool).await?;
    Ok(found.into_iter().map(|(u, c)| (u, crate::db::ids::cid_from_db(&c))).collect())
}
