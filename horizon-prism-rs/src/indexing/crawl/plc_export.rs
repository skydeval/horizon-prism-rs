//! `[full_crawl] identity_source = "plc_export"`: the crawl's identity rows
//! from the PLC directory's export, paged in bulk from `identity.plc_url`
//! only (a mirror serves no export, and keys never come from one).
//!
//! The follower pages `/export?after=<createdAt>` in the export's order
//! and keeps the latest operation per account in `crawl_identity`: its PDS
//! and its `#atproto` key, or neither for a tombstone, at the operation's
//! time (milliseconds, the `seq` column). The export's cursor is a time, so
//! each page starts a millisecond before the last one ended: operations of
//! that millisecond are read again rather than skipped, which the upsert
//! makes harmless. It keeps following once it reaches the end, so a rotated
//! key is current. These rows are the crawl's alone: nothing else reads
//! them, and they never enter `did_cache`.
//!
//! A crawl's identity read takes the account's row when there is one. An
//! account the export has not reached, a `did:web` account, and a re-read
//! after a bad signature go to the directory one account at a time, as
//! `per_did` does. A directory that serves no export (or one whose entries
//! carry no time) sets the option aside for `per_did`, with a warning.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;

use super::client::{Identity, IdentityMiss};
use super::task::Crawler;

/// The `worker_state` key of the follower: the time (milliseconds) of the
/// last operation stored, and when it last reached the end of the export.
pub const EXPORT_KEY: &str = "full_plc_export";
/// Operations a page asks for.
pub const PAGE: usize = 1000;
/// Metric: export operations stored, by kind (`operation`, `tombstone`).
pub const METRIC_EXPORT_OPS: &str = "full_crawl_plc_export_ops_total";

/// One operation of the export, as the crawl keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportOp {
    pub did: String,
    /// The operation's time in the export, in milliseconds.
    pub seq: i64,
    /// `None` for a tombstone, or an operation naming no PDS.
    pub pds: Option<String>,
    /// The `#atproto` key as a `did:key`.
    pub key: Option<String>,
}

/// Why a page could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportError {
    /// The directory serves no export, or no times in it.
    NotServed(String),
    /// No answer now; try again later.
    Failed(String),
}

/// One line of the export. A nullified operation, or a line that is not
/// an operation, is `Ok(None)`; a line without a `createdAt` is an error:
/// the export cannot be followed by it.
///
/// # Errors
///
/// A line without a readable `createdAt`.
pub fn parse_line(line: &str) -> Result<Option<ExportOp>, ExportError> {
    let Ok(v) = serde_json::from_str::<Value>(line) else {
        return Ok(None);
    };
    let Some(seq) = v
        .get("createdAt")
        .and_then(Value::as_str)
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.timestamp_millis())
    else {
        return Err(ExportError::NotServed("the export's entries carry no time".into()));
    };
    let Some(did) = v.get("did").and_then(Value::as_str).filter(|d| proto_blue::syntax::Did::is_valid(d)) else {
        return Ok(None);
    };
    if v.get("nullified").and_then(Value::as_bool) == Some(true) {
        return Ok(None);
    }
    let Some(op) = v.get("operation") else {
        return Ok(None);
    };
    let (pds, key) = match op.get("type").and_then(Value::as_str) {
        Some("plc_operation") => (
            op.pointer("/services/atproto_pds/endpoint").and_then(Value::as_str).map(str::to_string),
            op.pointer("/verificationMethods/atproto").and_then(Value::as_str).map(str::to_string),
        ),
        Some("plc_tombstone") => (None, None),
        // The legacy genesis form.
        Some("create") => (
            op.get("service").and_then(Value::as_str).map(str::to_string),
            op.get("signingKey").and_then(Value::as_str).map(str::to_string),
        ),
        _ => return Ok(None),
    };
    Ok(Some(ExportOp { did: did.to_string(), seq, pds, key }))
}

/// Store a page: per account, its latest operation, over an older stored
/// one. Returns how many accounts were written.
///
/// # Errors
///
/// Database errors.
pub async fn store(pool: &PgPool, ops: &[ExportOp]) -> Result<u64, sqlx::Error> {
    let mut latest: BTreeMap<&str, &ExportOp> = BTreeMap::new();
    for op in ops {
        // The export's order breaks ties: a later line is newer.
        let e = latest.entry(op.did.as_str()).or_insert(op);
        if op.seq >= e.seq {
            *e = op;
        }
    }
    if latest.is_empty() {
        return Ok(0);
    }
    let dids: Vec<String> = latest.keys().map(|d| (*d).to_string()).collect();
    let pds: Vec<Option<String>> = latest.values().map(|o| o.pds.clone()).collect();
    let keys: Vec<Option<String>> = latest.values().map(|o| o.key.clone()).collect();
    let seqs: Vec<i64> = latest.values().map(|o| o.seq).collect();
    sqlx::query("INSERT INTO dids (did) SELECT d FROM unnest($1::text[]) AS d ON CONFLICT (did) DO NOTHING")
        .bind(&dids)
        .execute(pool)
        .await?;
    let n = sqlx::query(
        "INSERT INTO crawl_identity (account, pds, signing_key, seq) \
         SELECT d.id, x.pds, x.key, x.seq FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[]) AS x(did, pds, key, seq) \
         JOIN dids d ON d.did = x.did \
         ON CONFLICT (account) DO UPDATE SET pds = EXCLUDED.pds, signing_key = EXCLUDED.signing_key, seq = EXCLUDED.seq \
         WHERE crawl_identity.seq <= EXCLUDED.seq",
    )
    .bind(&dids)
    .bind(&pds)
    .bind(&keys)
    .bind(&seqs)
    .execute(pool)
    .await?
    .rows_affected();
    let tombstones = latest.values().filter(|o| o.pds.is_none()).count();
    metrics::counter!(METRIC_EXPORT_OPS, "kind" => "tombstone").increment(u64::try_from(tombstones).unwrap_or(0));
    metrics::counter!(METRIC_EXPORT_OPS, "kind" => "operation").increment(u64::try_from(latest.len() - tombstones).unwrap_or(0));
    Ok(n)
}

/// An account's stored export row: its PDS and key.
type ExportRow = (Option<String>, Option<String>);

/// The follower's state, shared with status.
#[derive(Debug, Default)]
pub struct ExportState {
    /// `identity_source = "plc_export"` is configured and not set aside.
    active: AtomicBool,
    /// Why it was set aside, when it was.
    set_aside: Mutex<Option<String>>,
}

impl ExportState {
    #[must_use]
    pub fn new(active: bool) -> Self {
        Self { active: AtomicBool::new(active), set_aside: Mutex::default() }
    }

    #[must_use]
    pub fn active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Why the export was set aside for `per_did`, if it was.
    #[must_use]
    pub fn set_aside(&self) -> Option<String> {
        self.set_aside.lock().ok().and_then(|g| g.clone())
    }

    fn put_aside(&self, why: &str) {
        self.active.store(false, Ordering::SeqCst);
        if let Ok(mut g) = self.set_aside.lock() {
            *g = Some(why.to_string());
        }
    }
}

async fn cursor(pool: &PgPool) -> Result<Option<i64>, sqlx::Error> {
    let v: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(EXPORT_KEY).fetch_optional(pool).await?;
    Ok(v.and_then(|v| v.get("after").and_then(Value::as_i64)))
}

async fn save(pool: &PgPool, after: Option<i64>, at_end: bool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (key) DO UPDATE SET value = worker_state.value || EXCLUDED.value, updated_at = now()",
    )
    .bind(EXPORT_KEY)
    .bind(if at_end { json!({ "after": after, "reached_end_at": chrono::Utc::now().to_rfc3339() }) } else { json!({ "after": after }) })
    .execute(pool)
    .await?;
    Ok(())
}

impl Crawler {
    /// One account's identity for its crawl: its export row while the
    /// export is followed, else (and for a re-read, a `did:web` account or
    /// one the export has not reached) the directory, one account at a time.
    pub(super) async fn identity(&self, did: &str, fresh: bool) -> Result<Identity, IdentityMiss> {
        if !fresh && self.export.active() && did.starts_with("did:plc:") {
            let row: Result<Option<ExportRow>, sqlx::Error> =
                sqlx::query_as("SELECT ci.pds, ci.signing_key FROM crawl_identity ci JOIN dids d ON d.id = ci.account WHERE d.did = $1")
                    .bind(did)
                    .fetch_optional(&self.pool)
                    .await;
            match row {
                Ok(Some((Some(pds), key))) => {
                    metrics::counter!(super::METRIC_IDENTITY_READS, "source" => "plc_export").increment(1);
                    return Ok(Identity { pds, key });
                }
                Ok(Some((None, _))) => return Err(IdentityMiss::Gone("tombstoned in the directory's export".into())),
                Ok(None) => {}
                Err(e) => return Err(IdentityMiss::Failed(e.to_string())),
            }
        }
        self.source.identity(did, fresh).await
    }

    /// The export's state, for status.
    #[must_use]
    pub fn export_state(&self) -> std::sync::Arc<ExportState> {
        std::sync::Arc::clone(&self.export)
    }

    /// One step of the follower: a page while breadth is Full and the crawl
    /// runs. Returns how long to wait before the next step.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn export_step(&self) -> Result<Duration, sqlx::Error> {
        if !self.export.active() {
            return Ok(Duration::from_secs(60));
        }
        let running = {
            let sets = self.scope.read_guard().await;
            let trial = sets.trial.as_ref().filter(|t| t.state == crate::indexing::trial::TrialState::Running);
            sets.public.breadth == crate::indexing::Breadth::Full && self.crawl.enabled && trial.is_none_or(|t| t.history)
        };
        if !running {
            return Ok(Duration::from_secs(5));
        }
        let after = cursor(&self.pool).await?;
        self.export_pace.wait().await;
        // A millisecond back, so operations of the last one are read again
        // rather than skipped.
        match self.source.plc_export(after.map(|a| a - 1)).await {
            Ok(ops) => {
                let last = ops.iter().map(|o| o.seq).max().or(after).max(after);
                store(&self.pool, &ops).await?;
                let at_end = ops.len() < PAGE;
                save(&self.pool, last, at_end).await?;
                Ok(if at_end { Duration::from_secs(30) } else { Duration::ZERO })
            }
            Err(ExportError::NotServed(why)) => {
                tracing::warn!(event = "full_plc_export_set_aside", reason = %why, "full_crawl.identity_source = \"plc_export\": the directory serves no export; set aside, the crawl reads each account's directory entry");
                self.export.put_aside(&why);
                Ok(Duration::from_secs(60))
            }
            Err(ExportError::Failed(why)) => {
                tracing::debug!(event = "full_plc_export_failed", reason = %why, "reading the directory's export failed; it is tried again");
                Ok(Duration::from_secs(10))
            }
        }
    }

    /// Follow the export for as long as the process runs.
    pub(super) fn spawn_export(&self) -> Option<tokio::task::JoinHandle<()>> {
        if !self.export.active() {
            return None;
        }
        let me = self.clone();
        Some(tokio::spawn(async move {
            loop {
                let wait = match me.export_step().await {
                    Ok(d) => d,
                    Err(error) => {
                        tracing::warn!(event = "full_plc_export_error", %error, "storing the directory's export failed; it goes on");
                        Duration::from_secs(5)
                    }
                };
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_lines_by_kind() {
        let op = r#"{"did":"did:plc:example1","createdAt":"2024-01-02T03:04:05.006Z","nullified":false,"operation":{"type":"plc_operation","services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://pds.example"}},"verificationMethods":{"atproto":"did:key:zRepo"}}}"#;
        assert_eq!(
            parse_line(op),
            Ok(Some(ExportOp { did: "did:plc:example1".into(), seq: 1_704_164_645_006, pds: Some("https://pds.example".into()), key: Some("did:key:zRepo".into()) }))
        );
        let tomb = r#"{"did":"did:plc:example1","createdAt":"2024-01-02T03:04:06Z","operation":{"type":"plc_tombstone","prev":"x"}}"#;
        assert_eq!(parse_line(tomb), Ok(Some(ExportOp { did: "did:plc:example1".into(), seq: 1_704_164_646_000, pds: None, key: None })));
        // The directory's genesis form, as its export serves it.
        let legacy = r#"{"did":"did:plc:example2","cid":"bafyx","createdAt":"2022-11-17T00:35:16.391Z","operation":{"sig":"x","prev":null,"type":"create","handle":"a.example","service":"https://old.example","signingKey":"did:key:zOld","recoveryKey":"did:key:zRec"},"nullified":false}"#;
        assert_eq!(parse_line(legacy).expect("legacy").map(|o| (o.pds, o.key)), Some((Some("https://old.example".into()), Some("did:key:zOld".into()))));
        let nullified = r#"{"did":"did:plc:example1","createdAt":"2024-01-02T03:04:05Z","nullified":true,"operation":{"type":"plc_operation"}}"#;
        assert_eq!(parse_line(nullified), Ok(None));
        assert!(matches!(parse_line(r#"{"did":"did:plc:example1","operation":{}}"#), Err(ExportError::NotServed(_))), "no time");
        assert_eq!(parse_line("not json"), Ok(None));
    }
}
