//! Community labels (refract design §4.5): parsing, signature checks, the
//! labeler's key and endpoint, and the `refract_labels` writes.
//!
//! Every write is tied to the DID's row: its transaction first takes
//! `SELECT … FROM refract_dids WHERE did = $1 FOR UPDATE` and writes only
//! for `candidate`, `restored` or `grace`, so a fetch racing a purge's end
//! leaves no orphan row.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, PgPool};

use super::state::{ABSENT, State};
use crate::db::locks::uri_author;
use crate::identity::IdentityResolver;
use crate::identity::resolver::normalize_pds;

/// `refract_labels_rejected_total{community, reason}`.
pub const METRIC_REJECTED: &str = "refract_labels_rejected_total";

/// One label as a labeler or an AppView view carries it.
#[derive(Debug, Clone, PartialEq)]
pub struct Label {
    pub src: String,
    pub uri: String,
    pub val: String,
    pub neg: bool,
    pub cts: Option<DateTime<Utc>>,
    pub exp: Option<DateTime<Utc>>,
    /// The label object as received, for its signature check.
    pub raw: Value,
}

impl Label {
    /// Parse a label object; `None` for one without `src`, `uri` or `val`.
    #[must_use]
    pub fn parse(v: &Value) -> Option<Self> {
        let s = |k: &str| v.get(k).and_then(Value::as_str);
        let time = |k: &str| s(k).and_then(|t| DateTime::parse_from_rfc3339(t).ok()).map(|t| t.with_timezone(&Utc));
        Some(Self {
            src: s("src")?.to_string(),
            uri: s("uri")?.to_string(),
            val: s("val")?.to_string(),
            neg: v.get("neg").and_then(Value::as_bool).unwrap_or(false),
            cts: time("cts"),
            exp: time("exp"),
            raw: v.clone(),
        })
    }

    /// The DID the label is about: the subject DID, or an at-URI's
    /// authority.
    #[must_use]
    pub fn subject_did(&self) -> Option<&str> {
        if self.uri.starts_with("did:") { Some(self.uri.as_str()) } else { uri_author(&self.uri) }
    }

    /// Whether the label carries a signature.
    #[must_use]
    pub fn signed(&self) -> bool {
        self.raw.get("sig").is_some()
    }

    /// Whether the label is about the account (not one of its records).
    #[must_use]
    pub fn on_account(&self) -> bool {
        self.uri.starts_with("did:")
    }
}

/// Whether `label`'s signature verifies with the `did:key` `key`: DAG-CBOR
/// of the label without `sig`, ECDSA over its SHA-256, low-S only.
#[must_use]
pub fn verify(label: &Label, key: &str) -> bool {
    let Some(sig) = label.raw.get("sig") else {
        return false;
    };
    let sig_bytes = match proto_blue::lex_json::json_to_lex(sig) {
        proto_blue::lex_data::LexValue::Bytes(b) => b,
        _ => return false,
    };
    let mut unsigned = label.raw.clone();
    if let Some(o) = unsigned.as_object_mut() {
        o.remove("sig");
    }
    let Ok(bytes) = proto_blue::lex_cbor::encode(&proto_blue::lex_json::json_to_lex(&unsigned)) else {
        return false;
    };
    proto_blue::crypto::verify_signature(key, &bytes, &sig_bytes, false).unwrap_or(false)
}

/// What a labeler's DID document says: its signing key (`#atproto_label`,
/// as a `did:key`) and its endpoint (`#atproto_labeler`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelerDoc {
    pub key: Option<String>,
    pub endpoint: Option<String>,
}

/// Read a labeler's key and endpoint from its document.
#[must_use]
pub fn labeler_doc(doc: &Value, did: &str) -> LabelerDoc {
    let id_is = |v: &Value, frag: &str| v.get("id").and_then(Value::as_str).is_some_and(|id| id == frag || id == format!("{did}{frag}"));
    let key = doc
        .get("verificationMethod")
        .and_then(Value::as_array)
        .and_then(|m| m.iter().find(|v| id_is(v, "#atproto_label")))
        .and_then(|v| v.get("publicKeyMultibase").and_then(Value::as_str))
        .map(|k| format!("did:key:{k}"));
    let endpoint = doc
        .get("service")
        .and_then(Value::as_array)
        .and_then(|s| s.iter().find(|v| id_is(v, "#atproto_labeler")))
        .and_then(|v| v.get("serviceEndpoint").and_then(Value::as_str))
        .and_then(normalize_pds);
    LabelerDoc { key, endpoint }
}

/// The labelers' documents, read through the resolver's caches and re-read
/// from the directory on a failed check (§4.5 "Verification").
#[derive(Clone)]
pub struct Labelers {
    resolver: IdentityResolver,
}

impl Labelers {
    #[must_use]
    pub fn new(resolver: IdentityResolver) -> Self {
        Self { resolver }
    }

    /// The labeler's document, cached or fresh.
    pub async fn doc(&self, did: &str, fresh: bool) -> Option<LabelerDoc> {
        let doc: Arc<Value> = if fresh {
            self.resolver.fetch_document(did).await.ok()?.doc
        } else {
            self.resolver.resolve(did, crate::identity::ResolveIntent::Lookup).await.ok()?
        };
        Some(labeler_doc(&doc, did))
    }

    /// Check `labels` from `labeler`: those without a `sig` are accepted
    /// when `unsigned_ok` (an AppView-sourced view), a signed one must
    /// verify, re-reading the key once on a failure. Returns each label
    /// with whether it verified; a label that fails and is not gone is
    /// dropped, a gone one is kept unverified (fail closed).
    pub async fn check(&self, labeler: &str, community: &str, labels: Vec<Label>, gone: &[String], unsigned_ok: bool) -> Vec<(Label, bool)> {
        let mut key = None;
        let mut refreshed = false;
        let mut out = Vec::new();
        for l in labels {
            if l.src != labeler {
                continue;
            }
            if !l.signed() && unsigned_ok {
                out.push((l, true));
                continue;
            }
            if key.is_none() {
                key = self.doc(labeler, false).await.and_then(|d| d.key);
            }
            let mut ok = key.as_deref().is_some_and(|k| verify(&l, k));
            if !ok && !refreshed {
                refreshed = true;
                key = self.doc(labeler, true).await.and_then(|d| d.key);
                ok = key.as_deref().is_some_and(|k| verify(&l, k));
            }
            if ok {
                out.push((l, true));
            } else if gone.contains(&l.val) {
                metrics::counter!(METRIC_REJECTED, "community" => community.to_string(), "reason" => "unverified_gone").increment(1);
                out.push((l, false));
            } else {
                metrics::counter!(METRIC_REJECTED, "community" => community.to_string(), "reason" => "unverified").increment(1);
            }
        }
        out
    }
}

/// The labels left once negations are applied, in the order received: a
/// negation removes the same `(uri, val)` received before it.
#[must_use]
pub fn net_of(labels: Vec<(Label, bool)>) -> Vec<(Label, bool)> {
    let mut by_key: Vec<((String, String), (Label, bool))> = Vec::new();
    for (l, ok) in labels {
        let key = (l.uri.clone(), l.val.clone());
        by_key.retain(|(k, _)| *k != key);
        if !l.neg {
            by_key.push((key, (l, ok)));
        }
    }
    by_key.into_iter().map(|(_, v)| v).collect()
}

/// Lock `did`'s row and read its state, or `None` when it has no row.
///
/// # Errors
///
/// Database errors.
pub async fn lock_row(conn: &mut PgConnection, did: &str) -> Result<Option<(State, Option<DateTime<Utc>>, String)>, sqlx::Error> {
    let row: Option<(String, Option<DateTime<Utc>>, String)> =
        sqlx::query_as("SELECT state, awaiting_labels_since, community FROM refract_dids WHERE did = $1 FOR UPDATE")
            .bind(did)
            .fetch_optional(&mut *conn)
            .await?;
    Ok(row.and_then(|(s, a, c)| State::parse(&s).map(|s| (s, a, c))))
}

/// Which stored rows a write replaces.
#[derive(Debug, Clone)]
pub enum Replace<'a> {
    /// The account's own labels.
    Account,
    /// Every label of the DID, account and records (a targeted fetch).
    All,
    /// The labels of these records.
    Records(&'a [String]),
}

/// Replace `did`'s stored labels from `labeler` (the scope `which`) with
/// `labels`, inside the caller's transaction, which has locked the row.
/// The synthetic omission rows are never replaced here.
///
/// # Errors
///
/// Database errors.
pub async fn replace(
    conn: &mut PgConnection,
    labeler: &str,
    did: &str,
    which: Replace<'_>,
    labels: &[(Label, bool)],
    gone: &[String],
) -> Result<(), sqlx::Error> {
    match which {
        Replace::Account => {
            sqlx::query("DELETE FROM refract_labels WHERE labeler_did = $1 AND did = $2 AND subject = $2 AND val <> $3")
                .bind(labeler)
                .bind(did)
                .bind(ABSENT)
                .execute(&mut *conn)
                .await?;
        }
        Replace::All => {
            sqlx::query("DELETE FROM refract_labels WHERE labeler_did = $1 AND did = $2 AND val <> $3")
                .bind(labeler)
                .bind(did)
                .bind(ABSENT)
                .execute(&mut *conn)
                .await?;
        }
        Replace::Records(uris) => {
            sqlx::query("DELETE FROM refract_labels WHERE labeler_did = $1 AND subject = ANY($2) AND val <> $3")
                .bind(labeler)
                .bind(uris)
                .bind(ABSENT)
                .execute(&mut *conn)
                .await?;
        }
    }
    for (l, verified) in labels {
        if l.subject_did() != Some(did) {
            continue;
        }
        upsert(conn, labeler, did, l, *verified, gone).await?;
    }
    Ok(())
}

/// Store one label (or, for a negation, delete it).
///
/// # Errors
///
/// Database errors.
pub async fn upsert(conn: &mut PgConnection, labeler: &str, did: &str, l: &Label, verified: bool, gone: &[String]) -> Result<(), sqlx::Error> {
    if l.neg {
        sqlx::query("DELETE FROM refract_labels WHERE labeler_did = $1 AND subject = $2 AND val = $3")
            .bind(labeler)
            .bind(&l.uri)
            .bind(&l.val)
            .execute(&mut *conn)
            .await?;
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO refract_labels (labeler_did, subject, val, did, cts, exp, gone, verified) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (labeler_did, subject, val) DO UPDATE SET cts = EXCLUDED.cts, exp = EXCLUDED.exp, gone = EXCLUDED.gone, verified = EXCLUDED.verified",
    )
    .bind(labeler)
    .bind(&l.uri)
    .bind(&l.val)
    .bind(did)
    .bind(l.cts)
    .bind(l.exp)
    .bind(gone.contains(&l.val))
    .bind(verified)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The account verdict of rule 4 from the stored rows (§4.1): a gone label
/// that counts now (`Some(true)` if any is verified, `Some(false)` if only
/// unverified ones), or `None`.
///
/// # Errors
///
/// Database errors.
pub async fn account_gone(conn: &mut PgConnection, labeler: &str, did: &str) -> Result<Option<bool>, sqlx::Error> {
    let rows: Vec<bool> = sqlx::query_scalar(
        "SELECT verified FROM refract_labels WHERE labeler_did = $1 AND subject = $2 AND gone AND (exp IS NULL OR exp > now())",
    )
    .bind(labeler)
    .bind(did)
    .fetch_all(&mut *conn)
    .await?;
    Ok(if rows.is_empty() { None } else { Some(rows.iter().any(|v| *v)) })
}

/// Delete every stored label and record read of `dids` (a candidate
/// rejected, a community changed, a purge ended).
///
/// # Errors
///
/// Database errors.
pub async fn forget(conn: &mut PgConnection, dids: &[String]) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM refract_labels WHERE did = ANY($1)").bind(dids).execute(&mut *conn).await?;
    sqlx::query("DELETE FROM refract_record_reads WHERE did = ANY($1)").bind(dids).execute(&mut *conn).await?;
    Ok(())
}

/// Labels kept for subjects of the DIDs in `allowed`: candidates ∪ R,
/// grouped by DID (the poll's filter, §4.5).
#[must_use]
pub fn for_allowed(labels: Vec<(Label, bool)>, allowed: &HashSet<String>) -> HashMap<String, Vec<(Label, bool)>> {
    let mut out: HashMap<String, Vec<(Label, bool)>> = HashMap::new();
    for (l, ok) in labels {
        if let Some(did) = l.subject_did().filter(|d| allowed.contains(*d)).map(str::to_string) {
            out.entry(did).or_default().push((l, ok));
        }
    }
    out
}

/// Whether `pool` holds any label row for `did` (tests and status).
///
/// # Errors
///
/// Database errors.
pub async fn count_for(pool: &PgPool, did: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM refract_labels WHERE did = $1").bind(did).fetch_one(pool).await
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use proto_blue::crypto::{K256Keypair, Keypair, Signer};
    use serde_json::json;

    use super::*;

    /// A label signed by `kp` as a labeler signs it.
    pub(crate) fn signed(kp: &K256Keypair, label: Value) -> Value {
        let bytes = proto_blue::lex_cbor::encode(&proto_blue::lex_json::json_to_lex(&label)).expect("cbor");
        let sig = kp.sign(&bytes).expect("sign");
        let mut out = label;
        out["sig"] = json!({"$bytes": base64::engine::general_purpose::STANDARD_NO_PAD.encode(sig)});
        out
    }

    #[test]
    fn signatures_verify_and_tampering_fails() {
        let kp = K256Keypair::generate();
        let key = proto_blue::crypto::format_did_key("ES256K", &kp.public_key_compressed());
        let raw = signed(&kp, json!({"ver": 1, "src": "did:plc:lab", "uri": "did:plc:x", "val": "!takedown", "cts": "2026-10-22T00:00:00.000Z"}));
        let l = Label::parse(&raw).expect("label");
        assert!(l.signed() && verify(&l, &key));
        let mut tampered = raw.clone();
        tampered["val"] = json!("!hide");
        assert!(!verify(&Label::parse(&tampered).expect("label"), &key));
        let other = K256Keypair::generate();
        assert!(!verify(&l, &proto_blue::crypto::format_did_key("ES256K", &other.public_key_compressed())));
        assert_eq!(l.subject_did(), Some("did:plc:x"));
        let rec = Label::parse(&json!({"src": "s", "uri": "at://did:plc:y/app.bsky.feed.post/1", "val": "x"})).expect("label");
        assert_eq!(rec.subject_did(), Some("did:plc:y"));
        assert!(!rec.on_account() && !rec.signed());
    }

    #[test]
    fn labeler_documents_name_key_and_endpoint() {
        let doc = json!({
            "id": "did:plc:lab",
            "verificationMethod": [
                {"id": "did:plc:lab#atproto", "publicKeyMultibase": "zA"},
                {"id": "#atproto_label", "publicKeyMultibase": "zB"}
            ],
            "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler", "serviceEndpoint": "https://mod.example/"}]
        });
        assert_eq!(labeler_doc(&doc, "did:plc:lab"), LabelerDoc { key: Some("did:key:zB".into()), endpoint: Some("https://mod.example".into()) });
    }

    #[test]
    fn negations_cancel_earlier_labels() {
        let l = |val: &str, neg: bool| (Label::parse(&json!({"src": "s", "uri": "did:plc:x", "val": val, "neg": neg})).expect("label"), true);
        let net = net_of(vec![l("!takedown", false), l("spam", false), l("!takedown", true), l("!hide", false)]);
        let vals: Vec<&str> = net.iter().map(|(l, _)| l.val.as_str()).collect();
        assert_eq!(vals, ["spam", "!hide"]);
        let allowed: HashSet<String> = ["did:plc:x".to_string()].into();
        assert_eq!(for_allowed(net, &allowed)["did:plc:x"].len(), 2);
    }
}
