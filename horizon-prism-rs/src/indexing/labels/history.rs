//! A labeler's label keys over time, from its directory audit log: the key
//! a replayed label is checked against when the current one fails.

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::net::{BodyCap, NetClient, RequestOptions};

/// A labeler's `#atproto_label` keys, each with when it took effect, oldest
/// first. An operation with no label key is a `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyHistory {
    entries: Vec<(DateTime<Utc>, Option<String>)>,
}

impl KeyHistory {
    /// Parse a `/{did}/log/audit` answer: nullified operations are skipped,
    /// a tombstone ends every key.
    #[must_use]
    pub fn parse(audit: &Value) -> Self {
        let mut entries: Vec<(DateTime<Utc>, Option<String>)> = Vec::new();
        for e in audit.as_array().map(Vec::as_slice).unwrap_or_default() {
            if e.get("nullified").and_then(Value::as_bool).unwrap_or(false) {
                continue;
            }
            let Some(at) = e.get("createdAt").and_then(Value::as_str).and_then(|t| DateTime::parse_from_rfc3339(t).ok()) else {
                continue;
            };
            let op = e.get("operation").unwrap_or(&Value::Null);
            let key = match op.get("type").and_then(Value::as_str) {
                Some("plc_operation") => op.pointer("/verificationMethods/atproto_label").and_then(Value::as_str).map(str::to_string),
                _ => None,
            };
            entries.push((at.with_timezone(&Utc), key));
        }
        entries.sort_by_key(|(at, _)| *at);
        Self { entries }
    }

    /// The label key in effect at `t`.
    #[must_use]
    pub fn key_at(&self, t: DateTime<Utc>) -> Option<&str> {
        self.entries.iter().rev().find(|(at, _)| *at <= t).and_then(|(_, k)| k.as_deref())
    }
}

/// Read `did`'s audit log from the directory at `plc_url`. A `did:web`
/// labeler has none.
pub async fn fetch(net: &NetClient, plc_url: &str, did: &str) -> Option<KeyHistory> {
    if !did.starts_with("did:plc:") {
        return None;
    }
    let url = format!("{}/{did}/log/audit", plc_url.trim_end_matches('/'));
    let r = net.get(&url, &RequestOptions::new(BodyCap::APPVIEW).timeout(std::time::Duration::from_secs(20))).await.ok()?;
    if !r.status.is_success() {
        return None;
    }
    serde_json::from_slice::<Value>(&r.body).ok().map(|v| KeyHistory::parse(&v))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn the_key_in_effect_at_a_time() {
        let audit = json!([
            {"createdAt": "2024-01-01T00:00:00Z", "operation": {"type": "plc_operation", "verificationMethods": {"atproto": "did:key:zA", "atproto_label": "did:key:zOld"}}},
            {"createdAt": "2024-06-01T00:00:00Z", "nullified": true, "operation": {"type": "plc_operation", "verificationMethods": {"atproto_label": "did:key:zForged"}}},
            {"createdAt": "2025-01-01T00:00:00Z", "operation": {"type": "plc_operation", "verificationMethods": {"atproto_label": "did:key:zNew"}}}
        ]);
        let h = KeyHistory::parse(&audit);
        let t = |s: &str| DateTime::parse_from_rfc3339(s).expect("time").with_timezone(&Utc);
        assert_eq!(h.key_at(t("2023-12-31T00:00:00Z")), None);
        assert_eq!(h.key_at(t("2024-07-01T00:00:00Z")), Some("did:key:zOld"), "a nullified operation never counts");
        assert_eq!(h.key_at(t("2025-02-01T00:00:00Z")), Some("did:key:zNew"));
    }
}
