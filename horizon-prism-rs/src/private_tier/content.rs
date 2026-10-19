//! A private post's decoded payload (private-embeds design §2, #504): the
//! text alone (the old form), or `0xFF` followed by a JSON object carrying
//! `text`, `facets` and `embed` (the new form). Used only by the read
//! pipeline, on bytes decoded behind the witness; nothing here is stored.

use serde_json::Value;

use super::ingest::DISPLAY_FIELD_MAX_BYTES;
use crate::firehose::normalize::normalize_record;

/// The largest payload restored, and the largest `encodedContent` decoded:
/// the size of the largest record a PDS accepts (the lexicon bounds
/// `encodedContent` at the same). A payload restores at most what a record
/// could hold in the clear.
pub(super) const PAYLOAD_MAX_BYTES: usize = 1_000_000;

/// The byte that opens a new-form payload. It never begins valid UTF-8, so
/// it tells the two forms apart.
const NEW_FORM: u8 = 0xFF;

/// One of `facets` and `embed` in a new-form payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Field {
    /// The key is missing, or `null`: the clear field stands.
    Absent,
    /// A well-typed value within [`DISPLAY_FIELD_MAX_BYTES`], blob refs
    /// normalized.
    Kept(Value),
    /// The wrong type, or too large: left out, and the clear field does not
    /// stand in for it (C-16).
    Dropped,
}

impl Field {
    /// The display merge (§2): the payload's value when the payload has the
    /// key, else the clear `column`.
    pub(super) fn merge(self, column: Option<Value>) -> Option<Value> {
        match self {
            Self::Absent => column,
            Self::Kept(v) => Some(v),
            Self::Dropped => None,
        }
    }
}

/// A restored payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Restored {
    /// The old form: the payload is the text.
    Text(String),
    /// The new form.
    Content { text: String, facets: Field, embed: Field },
}

impl Restored {
    /// The `form` label of `private_read_content_total`.
    pub(super) fn form(&self) -> &'static str {
        match self {
            Self::Text(_) => "text",
            Self::Content { .. } => "content",
        }
    }
}

/// Why a payload is refused (each a uniform miss).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BadContent {
    TooLarge,
    /// The old form, not UTF-8: today's `not_utf8` miss.
    NotUtf8,
    NotJson,
    NotObject,
    NoText,
}

/// Restore a decoded payload (§2's table).
///
/// # Errors
///
/// [`BadContent`].
pub(super) fn restore(bytes: Vec<u8>) -> Result<Restored, BadContent> {
    if bytes.len() > PAYLOAD_MAX_BYTES {
        return Err(BadContent::TooLarge);
    }
    if bytes.first() != Some(&NEW_FORM) {
        return String::from_utf8(bytes).map(Restored::Text).map_err(|_| BadContent::NotUtf8);
    }
    // serde_json's own recursion limit (128) bounds the nesting.
    let value: Value = serde_json::from_slice(&bytes[1..]).map_err(|_| BadContent::NotJson)?;
    let Value::Object(mut map) = value else {
        return Err(BadContent::NotObject);
    };
    let Some(Value::String(text)) = map.remove("text") else {
        return Err(BadContent::NoText);
    };
    let facets = field(map.remove("facets"), Value::is_array);
    let embed = field(map.remove("embed"), Value::is_object);
    Ok(Restored::Content { text, facets, embed })
}

/// One payload field, with the checks a clear one gets at ingest
/// (`DisplayFields::parse`): `null` is absent; the wrong type, or over
/// [`DISPLAY_FIELD_MAX_BYTES`] serialized once its blob refs are
/// normalized, is dropped.
fn field(value: Option<Value>, ok: fn(&Value) -> bool) -> Field {
    match value {
        None | Some(Value::Null) => Field::Absent,
        Some(v) if ok(&v) => {
            let v = normalize_record(&v);
            if serde_json::to_vec(&v).is_ok_and(|b| b.len() <= DISPLAY_FIELD_MAX_BYTES) {
                Field::Kept(v)
            } else {
                Field::Dropped
            }
        }
        Some(_) => Field::Dropped,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn new_form(v: &Value) -> Vec<u8> {
        let mut out = vec![NEW_FORM];
        out.extend(serde_json::to_vec(v).expect("json"));
        out
    }

    fn new_form_raw(s: &str) -> Vec<u8> {
        let mut out = vec![NEW_FORM];
        out.extend(s.as_bytes());
        out
    }

    /// T-E11 (restore): §2's table, and §5's payload rows.
    #[test]
    fn restore_follows_the_payload_table() {
        // The old form: text, verbatim, whatever it looks like.
        assert_eq!(restore(b"hello".to_vec()), Ok(Restored::Text("hello".into())));
        assert_eq!(restore(Vec::new()), Ok(Restored::Text(String::new())));
        let json_like = r#"{"text": "not a payload"}"#;
        assert_eq!(restore(json_like.as_bytes().to_vec()), Ok(Restored::Text(json_like.into())));
        // U+00FF is C3 BF in UTF-8, not a leading 0xFF.
        assert_eq!(restore("ÿes".as_bytes().to_vec()), Ok(Restored::Text("ÿes".into())));
        assert_eq!(restore(vec![0xC3, 0x28]), Err(BadContent::NotUtf8));
        // Size: the bound itself passes, one byte more does not.
        assert!(matches!(restore(vec![b'a'; PAYLOAD_MAX_BYTES]), Ok(Restored::Text(_))));
        assert_eq!(restore(vec![b'a'; PAYLOAD_MAX_BYTES + 1]), Err(BadContent::TooLarge));
        let mut big = new_form(&json!({"text": "x"}));
        big.resize(PAYLOAD_MAX_BYTES + 1, b' ');
        assert_eq!(restore(big), Err(BadContent::TooLarge));

        // The new form.
        let facets = json!([{"index": {"byteStart": 0, "byteEnd": 1}, "features": []}]);
        let embed = json!({"$type": "app.bsky.embed.images", "images": [
            {"image": {"$type": "blob", "ref": "bafyimg", "mimeType": "image/jpeg", "size": 1}, "alt": ""}]});
        let restored = restore(new_form(&json!({"text": "hi", "facets": facets, "embed": embed}))).expect("restores");
        let normalized = json!({"$type": "app.bsky.embed.images", "images": [
            {"image": {"$type": "blob", "ref": {"$link": "bafyimg"}, "mimeType": "image/jpeg", "size": 1}, "alt": ""}]});
        assert_eq!(
            restored,
            Restored::Content {
                text: "hi".into(),
                facets: Field::Kept(facets),
                embed: Field::Kept(normalized),
            },
            "blob refs are normalized as a clear record's are"
        );
        assert_eq!(restored.form(), "content");
        assert_eq!(
            restore(new_form(&json!({"text": ""}))),
            Ok(Restored::Content { text: String::new(), facets: Field::Absent, embed: Field::Absent })
        );
        // Null is absent; the wrong type is dropped; other keys are ignored.
        assert_eq!(
            restore(new_form(&json!({"text": "t", "facets": null, "embed": [1], "reply": {}, "quote": {}, "mentionDids": [], "$type": "x"}))),
            Ok(Restored::Content { text: "t".into(), facets: Field::Absent, embed: Field::Dropped })
        );
        assert_eq!(
            restore(new_form(&json!({"text": "t", "facets": {"a": 1}, "embed": null}))),
            Ok(Restored::Content { text: "t".into(), facets: Field::Dropped, embed: Field::Absent })
        );
        // Over 64 KiB serialized: dropped.
        let long = json!({"text": "t", "embed": {"$type": "app.bsky.embed.external",
            "external": {"uri": "x", "title": "t", "description": "d".repeat(70_000)}}});
        assert!(matches!(restore(new_form(&long)), Ok(Restored::Content { embed: Field::Dropped, .. })));
        // Duplicate keys: the last wins, as on the PDS.
        assert_eq!(restore(new_form_raw(r#"{"text": "a", "text": "b"}"#)), Ok(Restored::Content {
            text: "b".into(),
            facets: Field::Absent,
            embed: Field::Absent
        }));
        // Trailing whitespace is allowed; anything else after the value is not.
        assert!(restore(new_form_raw("{\"text\": \"a\"}  \n")).is_ok());
        assert_eq!(restore(new_form_raw(r#"{"text": "a"} x"#)), Err(BadContent::NotJson));

        // Refused.
        for (case, bytes, reason) in [
            ("0xFF alone", vec![NEW_FORM], BadContent::NotJson),
            ("0xFF and whitespace", new_form_raw("  "), BadContent::NotJson),
            ("not JSON", new_form_raw("{text"), BadContent::NotJson),
            ("an array", new_form(&json!([1])), BadContent::NotObject),
            ("a string", new_form(&json!("text")), BadContent::NotObject),
            ("a number", new_form(&json!(5)), BadContent::NotObject),
            ("no text", new_form(&json!({"facets": []})), BadContent::NoText),
            ("text not a string", new_form(&json!({"text": 5})), BadContent::NoText),
            ("text null", new_form(&json!({"text": null})), BadContent::NoText),
            ("nested deeper than 128", new_form_raw(&format!("{}{}", "[".repeat(200), "]".repeat(200))), BadContent::NotJson),
        ] {
            assert_eq!(restore(bytes), Err(reason), "{case}");
        }
        // 128 levels inside the object is still within the limit.
        let deep = format!(r#"{{"text": "t", "x": {}{}}}"#, "[".repeat(120), "]".repeat(120));
        assert!(restore(new_form_raw(&deep)).is_ok());
    }

    #[test]
    fn the_merge_takes_the_payload_key_when_present() {
        let clear = Some(json!(["clear"]));
        assert_eq!(Field::Absent.merge(clear.clone()), clear);
        assert_eq!(Field::Kept(json!(["payload"])).merge(clear.clone()), Some(json!(["payload"])));
        assert_eq!(Field::Dropped.merge(clear), None, "a bad payload field does not fall back");
        assert_eq!(Field::Absent.merge(None), None);
    }
}
