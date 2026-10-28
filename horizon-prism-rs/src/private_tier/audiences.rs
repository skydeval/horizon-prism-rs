//! Audiences held by this AppView (private-audiences design). Slice S0: the
//! stored-row reads and the comparison of the read-only `admin audiences
//! check` (§3). The check's network and printing live in
//! `cli::commands::audiences_check`; its SQL lives here, under the
//! private-table source check.

use std::collections::{HashMap, HashSet};

use sqlx::PgPool;

use super::ingest::AudienceFields;
use crate::firehose::backfill::ListedRecord;
use crate::firehose::classifier::NSID_AUDIENCE;
use crate::firehose::normalize::normalize_record;

/// A member's stored audiences: each row's URI and token (`cid`).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn check_rows(pool: &PgPool, did: &str) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as("SELECT uri, cid FROM private_audiences WHERE owner_did = $1 ORDER BY uri")
        .bind(did)
        .fetch_all(pool)
        .await
}

/// One member's listed records against its stored rows (§3, "What it
/// compares"). The expected state is what today's ingest would store: a
/// record with a valid record key that [`AudienceFields::parse`] accepts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Comparison {
    /// Records listed.
    pub records: usize,
    /// Rows stored.
    pub rows: usize,
    /// Accepted records with no row.
    pub missing: Vec<String>,
    /// Accepted records whose row has another `cid`.
    pub differing: Vec<String>,
    /// Rows no record is listed for.
    pub extra: Vec<String>,
    /// Records the parser refuses, which no ingest would store.
    pub refused: Vec<String>,
    /// Refused records whose URI has a stored row: kept, but unproven.
    pub refused_with_row: Vec<String>,
}

impl Comparison {
    /// No missing, differing or extra row.
    #[must_use]
    pub(crate) fn matches(&self) -> bool {
        self.missing.is_empty() && self.differing.is_empty() && self.extra.is_empty()
    }
}

/// Whether today's ingest would store `record` under `rkey`: the writer's
/// record-key check, then the audience parser on the normalized record.
fn accepted(rkey: &str, record: &serde_json::Value) -> bool {
    proto_blue::syntax::RecordKey::is_valid(rkey) && AudienceFields::parse(&normalize_record(record)).is_ok()
}

/// Compare `did`'s `listed` records with its stored `rows` (URI, `cid`).
#[must_use]
pub(crate) fn compare(did: &str, listed: &[ListedRecord], rows: &[(String, String)]) -> Comparison {
    let stored: HashMap<&str, &str> = rows.iter().map(|(u, c)| (u.as_str(), c.as_str())).collect();
    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Comparison { records: listed.len(), rows: rows.len(), ..Comparison::default() };
    for record in listed {
        let rkey = record.rkey();
        let uri = format!("at://{did}/{NSID_AUDIENCE}/{rkey}");
        if !seen.insert(uri.clone()) {
            continue;
        }
        let row = stored.get(uri.as_str()).copied();
        if !accepted(rkey, &record.value) {
            // Whatever row there is, or none, is accepted (§3, "refused,
            // exactly"); a row is printed as unproven.
            if row.is_some() {
                out.refused_with_row.push(uri.clone());
            }
            out.refused.push(uri);
            continue;
        }
        match row {
            None => out.missing.push(uri),
            Some(cid) if cid != record.cid => out.differing.push(uri),
            Some(_) => {}
        }
    }
    out.extra = rows.iter().filter(|(u, _)| !seen.contains(u)).map(|(u, _)| u.clone()).collect();
    out
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    fn listed(rkey: &str, cid: &str, value: serde_json::Value) -> ListedRecord {
        ListedRecord { uri: format!("at://{DID}/{NSID_AUDIENCE}/{rkey}"), cid: cid.to_string(), value }
    }

    fn uri(rkey: &str) -> String {
        format!("at://{DID}/{NSID_AUDIENCE}/{rkey}")
    }

    #[test]
    fn compares_like_ingest() {
        let ok = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": []});
        let bad = json!({"$type": NSID_AUDIENCE, "members": "nope"});
        let records = [
            listed("3kaaaaaaaaaa2", "bafysame", ok.clone()),
            listed("3kbbbbbbbbbb2", "bafynew", ok.clone()),
            listed("3kcccccccccc2", "bafyx", ok),
            listed("3kdddddddddd2", "bafybad", bad.clone()),
            listed("3keeeeeeeeee2", "bafybad", bad),
        ];
        let rows = vec![
            (uri("3kaaaaaaaaaa2"), "bafysame".to_string()),
            (uri("3kbbbbbbbbbb2"), "bafyold".to_string()),
            (uri("3kdddddddddd2"), "bafyvalid".to_string()),
            (uri("3kffffffffff2"), "bafygone".to_string()),
        ];
        let c = compare(DID, &records, &rows);
        assert_eq!(c.differing, vec![uri("3kbbbbbbbbbb2")]);
        assert_eq!(c.missing, vec![uri("3kcccccccccc2")]);
        assert_eq!(c.extra, vec![uri("3kffffffffff2")]);
        assert_eq!(c.refused, vec![uri("3kdddddddddd2"), uri("3keeeeeeeeee2")]);
        assert_eq!(c.refused_with_row, vec![uri("3kdddddddddd2")]);
        assert!(!c.matches());
        assert_eq!((c.records, c.rows), (5, 4));
        // An invalid record key is refused as the writer refuses it.
        let c = compare(DID, &[listed("..", "bafyz", json!({}))], &[]);
        assert_eq!(c.refused.len(), 1);
        assert!(c.matches());
    }
}
