//! A member's appearance setting (private-appearance design §5): the input
//! of `putAppearance`, the check of the value, and the answer of both
//! methods. The handlers are in `handlers`, the queries in `schema`.

use chrono::SecondsFormat;
use serde_json::{Map, Value};

use super::schema::AppearanceRow;
use crate::xrpc::errors::XrpcError;

/// The record type the value must name.
pub(super) const NSID_APPEARANCE: &str = "app.nearhorizon.actor.appearance";
/// A put's body is refused unparsed past this many bytes.
pub(super) const BODY_MAX: usize = 64 * 1024;
/// The value's compact JSON is at most this many bytes.
pub(super) const VALUE_MAX: usize = 16_384;

/// The three choices, as stored.
const STORES: [&str; 3] = ["private", "public", "device"];

/// A checked `putAppearance` input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PutInput {
    pub store: &'static str,
    /// The value's compact JSON text, with `private` and `public`.
    pub value: Option<String>,
    /// Absent: unconditional; `""`: create only; else the rev to replace.
    pub swap: Option<String>,
}

/// `putAppearance`'s body, checked in the design's order (§5.3 steps 2–6).
/// Unknown top-level keys are ignored.
///
/// # Errors
///
/// `InvalidRequest`, its message `"Error: "`-prefixed.
pub(super) fn put_input(body: &[u8]) -> Result<PutInput, XrpcError> {
    if body.len() > BODY_MAX {
        return Err(XrpcError::invalid("Error: input is too large"));
    }
    let input = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
    let Some(input) = input.as_object() else {
        return Err(XrpcError::invalid("Error: input must be an object"));
    };
    let store = input
        .get("store")
        .and_then(Value::as_str)
        .and_then(|s| STORES.into_iter().find(|k| *k == s))
        .ok_or_else(|| XrpcError::invalid("Error: store must be one of private, public, device"))?;
    let value = match (store, input.get("appearance")) {
        ("device", None) => None,
        ("device", Some(_)) => return Err(XrpcError::invalid("Error: appearance must be absent with store device")),
        (_, None | Some(Value::Null)) => return Err(XrpcError::invalid("Error: appearance is required with store private or public")),
        (_, Some(v)) => Some(checked_value(v)?),
    };
    let swap = match input.get("swapRev") {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(XrpcError::invalid("Error: swapRev must be a string")),
    };
    Ok(PutInput { store, value, swap })
}

/// The value's checks (§5.4): `$type`, size, the strict atproto data
/// model, `version` and `updatedAt`; nothing of the record lexicon's
/// per-field constraints. Returns the compact JSON text to store.
///
/// # Errors
///
/// `InvalidRequest`.
pub(super) fn checked_value(value: &Value) -> Result<String, XrpcError> {
    let fields = value
        .as_object()
        .filter(|m| m.get("$type").and_then(Value::as_str) == Some(NSID_APPEARANCE))
        .ok_or_else(|| XrpcError::invalid(format!("Error: appearance.$type must be {NSID_APPEARANCE}")))?;
    let text = serde_json::to_string(value).map_err(|_| XrpcError::invalid("Error: appearance is not valid atproto data"))?;
    if text.len() > VALUE_MAX {
        return Err(XrpcError::invalid(format!("Error: appearance is too large ({VALUE_MAX} bytes at most)")));
    }
    proto_blue::lex_json::json_to_lex_with(value, proto_blue::lex_json::LexParseOptions::strict())
        .map_err(|e| XrpcError::invalid(format!("Error: appearance is not valid atproto data: {e}")))?;
    if fields.get("version").and_then(Value::as_i64).is_none_or(|v| v < 1) {
        return Err(XrpcError::invalid("Error: appearance.version must be an integer of at least 1"));
    }
    if !fields.get("updatedAt").and_then(Value::as_str).is_some_and(proto_blue::syntax::Datetime::is_valid) {
        return Err(XrpcError::invalid("Error: appearance.updatedAt must be a datetime"));
    }
    Ok(text)
}

/// `#appearanceState` (§5.1): `{}` with no row; else `store`, `rev`,
/// `migratedAt`, and `appearance` with `private` and `public`.
pub(super) fn state(row: Option<&AppearanceRow>) -> Value {
    let mut out = Map::new();
    if let Some(row) = row {
        out.insert("store".into(), Value::String(row.store.clone()));
        if let Some(v) = row.appearance.as_deref().and_then(|t| serde_json::from_str::<Value>(t).ok()) {
            out.insert("appearance".into(), v);
        }
        out.insert("rev".into(), Value::String(row.rev.to_string()));
        out.insert("migratedAt".into(), Value::String(row.created_at.to_rfc3339_opts(SecondsFormat::Millis, true)));
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn value() -> Value {
        json!({"$type": NSID_APPEARANCE, "version": 1, "updatedAt": "2026-10-23T12:00:00.000Z", "backdrop": "dusk"})
    }

    fn message(r: Result<PutInput, XrpcError>) -> String {
        r.expect_err("refused").message
    }

    #[test]
    fn inputs() {
        let ok = put_input(json!({"store": "private", "appearance": value(), "swapRev": "", "extra": 1}).to_string().as_bytes()).expect("ok");
        assert_eq!((ok.store, ok.swap.as_deref()), ("private", Some("")));
        assert_eq!(ok.value.as_deref(), Some(value().to_string().as_str()));
        let device = put_input(br#"{"store": "device"}"#).expect("ok");
        assert_eq!((device.value, device.swap), (None, None));
        assert_eq!(message(put_input(b"[]")), "Error: input must be an object");
        assert_eq!(message(put_input(b"")), "Error: input must be an object");
        assert_eq!(message(put_input(br#"{"store": "cloud"}"#)), "Error: store must be one of private, public, device");
        assert_eq!(message(put_input(br#"{"store": "public"}"#)), "Error: appearance is required with store private or public");
        assert_eq!(message(put_input(br#"{"store": "public", "appearance": null}"#)), "Error: appearance is required with store private or public");
        assert_eq!(message(put_input(br#"{"store": "device", "appearance": null}"#)), "Error: appearance must be absent with store device");
        assert_eq!(message(put_input(br#"{"store": "device", "swapRev": null}"#)), "Error: swapRev must be a string");
        assert_eq!(message(put_input(br#"{"store": "device", "swapRev": 3}"#)), "Error: swapRev must be a string");
        let big = format!(r#"{{"store": "device", "pad": "{}"}}"#, "x".repeat(BODY_MAX));
        assert_eq!(message(put_input(big.as_bytes())), "Error: input is too large");
    }

    #[test]
    fn values() {
        let refused = |v: Value| checked_value(&v).expect_err("refused").message;
        let mut v = value();
        v.as_object_mut().expect("object").remove("$type");
        assert_eq!(refused(v), format!("Error: appearance.$type must be {NSID_APPEARANCE}"));
        assert!(refused(json!({"$type": "app.bsky.actor.profile", "version": 1})).starts_with("Error: appearance.$type"));
        assert!(refused(json!([value()])).starts_with("Error: appearance.$type"));
        let with = |k: &str, x: Value| {
            let mut v = value();
            v[k] = x;
            v
        };
        let mut v = value();
        v.as_object_mut().expect("object").remove("version");
        assert_eq!(refused(v), "Error: appearance.version must be an integer of at least 1");
        assert!(refused(with("version", json!(1.5))).starts_with("Error: appearance is not valid atproto data"));
        assert_eq!(refused(with("version", json!(0))), "Error: appearance.version must be an integer of at least 1");
        assert_eq!(refused(with("updatedAt", json!("yesterday"))), "Error: appearance.updatedAt must be a datetime");
        // Exactly at the cap passes; one byte over is refused.
        let base = with("futureField", json!("")).to_string().len();
        let at_cap = with("futureField", json!("x".repeat(VALUE_MAX - base)));
        assert_eq!(at_cap.to_string().len(), VALUE_MAX);
        assert!(checked_value(&at_cap).is_ok());
        let over = with("futureField", json!("x".repeat(VALUE_MAX - base + 1)));
        assert_eq!(refused(over), format!("Error: appearance is too large ({VALUE_MAX} bytes at most)"));
        // A NUL in a string and in a key is kept.
        let nul = with("presets", json!([{"name": "a\u{0}b", "k\u{0}": 1}]));
        assert_eq!(checked_value(&nul).expect("kept"), nul.to_string());
    }
}
