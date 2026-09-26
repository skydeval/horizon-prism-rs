//! Record encoding normalization (design-v2 §5.8).
//!
//! horizon-firehose renders CID links as bare strings and bytes as lowercase
//! hex (`lex_to_json`). Slingshot, AppViews and PDS `getRecord`/`listRecords`
//! return ATProto JSON: `{"$link": …}` and `{"$bytes": base64}`.
//! - Field extraction accepts both forms at known positions
//!   ([`cid_link`], [`bytes_field`]).
//! - Stored record JSON is rewritten to ATProto JSON ([`normalize_record`]):
//!   a blob's `ref` becomes `{"$link": …}`. Blobs are recognised by their
//!   `"$type": "blob"` marker, never by the shape of a string. Unknown fields
//!   are kept verbatim, and a hex string in an unknown position stays a string.

use base64::Engine;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use serde_json::{Map, Value};

/// Standard base64, padded or not (ATProto `$bytes`).
const BASE64_ANY_PADDING: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Rewrite a record to ATProto JSON form (blob refs as `{"$link": …}`).
#[must_use]
pub fn normalize_record(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let is_blob = map.get("$type").and_then(Value::as_str) == Some("blob");
            let mut out = Map::with_capacity(map.len());
            for (k, v) in map {
                let v = if is_blob && k == "ref" {
                    match v {
                        Value::String(cid) => link(cid),
                        other => normalize_record(other),
                    }
                } else {
                    normalize_record(v)
                };
                out.insert(k.clone(), v);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(normalize_record).collect()),
        other => other.clone(),
    }
}

fn link(cid: &str) -> Value {
    let mut m = Map::new();
    m.insert("$link".to_string(), Value::String(cid.to_string()));
    Value::Object(m)
}

/// A CID link at a known position: a bare string or `{"$link": …}`.
#[must_use]
pub fn cid_link(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Object(m) => m.get("$link").and_then(Value::as_str).map(str::to_string),
        _ => None,
    }
}

/// A blob's CID (`ref` as a string or `{"$link"}`), e.g. a profile avatar.
#[must_use]
pub fn blob_cid(value: &Value) -> Option<String> {
    value.get("ref").and_then(cid_link)
}

/// A bytes field at a known position: lowercase hex (firehose) or
/// `{"$bytes": base64}` (PDS/Slingshot). `None` when neither decodes.
#[must_use]
pub fn bytes_field(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::String(hex_str) => hex::decode(hex_str).ok(),
        Value::Object(m) => m
            .get("$bytes")
            .and_then(Value::as_str)
            .and_then(|b| BASE64_ANY_PADDING.decode(b).ok()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn blob_refs_become_links_in_any_position() {
        let firehose = json!({
            "$type": "app.bsky.feed.post",
            "text": "hi",
            "embed": {"$type": "app.bsky.embed.images", "images": [
                {"alt": "", "image": {"$type": "blob", "ref": "bafkreiabc", "mimeType": "image/png", "size": 3}}
            ]}
        });
        let out = normalize_record(&firehose);
        assert_eq!(
            out["embed"]["images"][0]["image"]["ref"],
            json!({"$link": "bafkreiabc"})
        );
        assert_eq!(out["text"], "hi");
    }

    #[test]
    fn already_normal_records_are_unchanged() {
        let pds = json!({
            "avatar": {"$type": "blob", "ref": {"$link": "bafkreiabc"}, "mimeType": "image/png", "size": 3},
            "displayName": "x"
        });
        assert_eq!(normalize_record(&pds), pds);
    }

    #[test]
    fn hex_in_unknown_positions_is_left_alone() {
        let r = json!({"unknown": "deadbeef", "ref": "bafy", "subject": {"cid": "bafycid", "uri": "at://x"}});
        assert_eq!(normalize_record(&r), r);
    }

    #[test]
    fn cid_links_in_both_forms() {
        assert_eq!(cid_link(&json!("bafy")), Some("bafy".into()));
        assert_eq!(cid_link(&json!({"$link": "bafy"})), Some("bafy".into()));
        assert_eq!(cid_link(&json!(3)), None);
        assert_eq!(
            blob_cid(&json!({"$type": "blob", "ref": {"$link": "bafy"}})),
            Some("bafy".into())
        );
    }

    #[test]
    fn bytes_in_both_forms() {
        assert_eq!(bytes_field(&json!("00ff10")), Some(vec![0, 255, 16]));
        assert_eq!(bytes_field(&json!({"$bytes": "AP8Q"})), Some(vec![0, 255, 16]));
        assert_eq!(bytes_field(&json!({"$bytes": "AP8QAA=="})), Some(vec![0, 255, 16, 0]));
        assert_eq!(bytes_field(&json!({"$bytes": "AP8QAA"})), Some(vec![0, 255, 16, 0]));
        assert_eq!(bytes_field(&json!("xyz")), None);
    }
}
