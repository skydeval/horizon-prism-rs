//! The #504 clear fields, one derivation (private-encode design §5): the
//! payload a private post's content is encoded as, the clear fields a
//! correct encoder writes beside it, and the facts Prism indexes from them.
//! Used by `encodePost`, by ingest's fallback and by the read-time check.
//!
//! - [`payload`] is the PDS's content form byte for byte: `0xFF`, then a
//!   JSON object with `text`, then `facets`, then `embed` when present, each
//!   the value as parsed. Nothing inside `facets` or `embed` is read,
//!   renamed, reordered or dropped, so extension keys survive (serde_json's
//!   `preserve_order`, declared in Cargo.toml, keeps every object's key
//!   order).
//! - [`derive`] ports the PDS's `mention_dids`, `quoted_record`,
//!   `media_kinds` and `blob_objects`, with the same type lists, order and
//!   omissions. It accepts any JSON: a field of the wrong type derives no
//!   mention, quote or media kind.
//! - [`index`] is what Prism indexes from wire clear fields, with ingest's
//!   filters.
//!
//! Secrecy (design §9, S-3, S-5): [`ClearFields`] holds content-derived
//! values and is not `Debug`; nothing here logs, and nothing panics on an
//! input value.

use serde_json::{Map, Value};

use super::media;
use crate::db::locks::uri_author;

/// The first byte of the content form (the PDS's `CONTENT_FORM_MARKER`).
pub(super) const CONTENT_FORM: u8 = 0xFF;

/// Image embeds, in the stock and the kryphocron vocabularies.
const IMAGE_EMBED_TYPES: [&str; 4] = [
    "app.bsky.embed.images",
    "app.bsky.embed.gallery",
    "tools.kryphocron.feed.postPublic#embedImage",
    "tools.kryphocron.feed.postPublic#embedGallery",
];

/// Video embeds, in the stock and the kryphocron vocabularies.
const VIDEO_EMBED_TYPES: [&str; 2] = ["app.bsky.embed.video", "tools.kryphocron.feed.postPublic#embedVideo"];

/// Record-with-media embeds, in the stock and the kryphocron vocabularies.
const RECORD_WITH_MEDIA_TYPES: [&str; 2] = ["app.bsky.embed.recordWithMedia", "tools.kryphocron.feed.postPublic#embedRecordWithMedia"];

/// Record embeds, in the stock and the kryphocron vocabularies.
const RECORD_TYPES: [&str; 2] = ["app.bsky.embed.record", "tools.kryphocron.feed.postPublic#embedRecord"];

/// Mention facet features: the stock and the kryphocron vocabularies, and
/// the bare local form.
pub(super) const MENTION_TYPES: [&str; 3] = [
    "app.bsky.richtext.facet#mention",
    "tools.kryphocron.feed.postPublic#mention",
    "#mention",
];

/// The clear fields a correct encoder writes for one post's content, in wire
/// form (what goes on the record). Each is empty (or `None`) when the
/// content has none, and is then omitted from the record.
#[derive(Clone, Default, PartialEq)]
pub(super) struct ClearFields {
    /// `embedBlobs`: every blob object of the embed, whole, in traversal
    /// order.
    pub embed_blobs: Vec<Value>,
    /// `mentionDids`: the mentioned DIDs, deduplicated in first-appearance
    /// order, DID syntax not checked (as the PDS writes them).
    pub mention_dids: Vec<Value>,
    /// `quote`: `{uri, cid}` of the quoted record, when both are strings.
    pub quote: Option<Value>,
    /// `mediaKinds`: `["image"]` or `["video"]`.
    pub media_kinds: Vec<Value>,
}

impl ClearFields {
    /// The fields as record keys, each omitted when empty, in the PDS's
    /// order of insertion (`mentionDids`, `quote`, `mediaKinds`,
    /// `embedBlobs`).
    #[cfg_attr(not(test), expect(dead_code, reason = "the encoder (private-encode E3) is its first caller outside the parity tests"))]
    pub(super) fn into_record_fields(self) -> Map<String, Value> {
        let mut out = Map::new();
        if !self.mention_dids.is_empty() {
            out.insert("mentionDids".into(), Value::Array(self.mention_dids));
        }
        if let Some(q) = self.quote {
            out.insert("quote".into(), q);
        }
        if !self.media_kinds.is_empty() {
            out.insert(media::MEDIA_KINDS_FIELD.into(), Value::Array(self.media_kinds));
        }
        if !self.embed_blobs.is_empty() {
            out.insert("embedBlobs".into(), Value::Array(self.embed_blobs));
        }
        out
    }
}

/// What Prism indexes from a post's clear fields (structural facts): the
/// quoted URI, the mentioned DIDs and the media bits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct IndexFacts {
    pub quote_uri: Option<String>,
    pub mention_dids: Vec<String>,
    pub media_kinds: u8,
}

/// The content-form payload of `text`, `facets` and `embed`, byte for byte
/// as the PDS's `content_payload` builds it.
#[cfg_attr(not(test), expect(dead_code, reason = "the encoder (private-encode E3) is its first caller outside the parity tests"))]
pub(super) fn payload(text: &str, facets: Option<&Value>, embed: Option<&Value>) -> Vec<u8> {
    let mut content = Map::new();
    content.insert("text".into(), Value::String(text.to_string()));
    if let Some(f) = facets {
        content.insert("facets".into(), f.clone());
    }
    if let Some(e) = embed {
        content.insert("embed".into(), e.clone());
    }
    let mut bytes = vec![CONTENT_FORM];
    // A `Value` with string keys always serializes; an error here would be
    // an allocation failure, and the vector simply stays short.
    let _ = serde_json::to_writer(&mut bytes, &Value::Object(content));
    bytes
}

/// The clear fields of `facets` and `embed` (the PDS's derivation).
pub(super) fn derive(facets: Option<&Value>, embed: Option<&Value>) -> ClearFields {
    let mut out = ClearFields::default();
    if let Some(f) = facets {
        out.mention_dids = mention_dids(f);
    }
    if let Some(e) = embed {
        out.quote = quoted_record(e);
        out.media_kinds = media_kinds(e);
        blob_objects(e, &mut out.embed_blobs);
    }
    out
}

/// An `at://` URI whose authority is a valid DID: what ingest indexes as a
/// quote.
pub(super) fn indexable_quote(uri: &str) -> bool {
    uri.starts_with("at://") && uri_author(uri).is_some_and(proto_blue::syntax::Did::is_valid)
}

/// The facts Prism indexes from wire clear fields, with ingest's filters: a
/// quote URI with a valid DID authority, mention entries that are valid
/// DIDs (deduplicated in order), and `media::kinds_of_field`.
pub(super) fn index(fields: &ClearFields) -> IndexFacts {
    let quote_uri = fields
        .quote
        .as_ref()
        .and_then(|q| q.get("uri"))
        .and_then(Value::as_str)
        .filter(|u| indexable_quote(u))
        .map(str::to_string);
    let mut mention_dids: Vec<String> = Vec::new();
    for did in fields.mention_dids.iter().filter_map(Value::as_str) {
        if proto_blue::syntax::Did::is_valid(did) && !mention_dids.iter().any(|d| d == did) {
            mention_dids.push(did.to_string());
        }
    }
    IndexFacts {
        quote_uri,
        mention_dids,
        media_kinds: media::kinds_of_field(&fields.media_kinds),
    }
}

/// The media kinds `embed` itself carries (the PDS's `media_kinds`).
fn media_kinds(embed: &Value) -> Vec<Value> {
    let embed_type = |value: &Value| value.get("$type").and_then(Value::as_str).map(str::to_string);
    let media_type = match embed_type(embed) {
        Some(t) if RECORD_WITH_MEDIA_TYPES.contains(&t.as_str()) => embed.get("media").and_then(embed_type),
        other => other,
    };
    let kind = match media_type.as_deref() {
        Some(t) if IMAGE_EMBED_TYPES.contains(&t) => media::KIND_IMAGE,
        Some(t) if VIDEO_EMBED_TYPES.contains(&t) => media::KIND_VIDEO,
        _ => return Vec::new(),
    };
    vec![Value::String(kind.to_string())]
}

/// The DIDs `facets` mention, deduplicated, in first-appearance order (the
/// PDS's `mention_dids`).
fn mention_dids(facets: &Value) -> Vec<Value> {
    let mut dids: Vec<Value> = Vec::new();
    let features = facets
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|facet| facet.get("features").and_then(Value::as_array))
        .flatten();
    for feature in features {
        let is_mention = feature.get("$type").and_then(Value::as_str).is_some_and(|t| MENTION_TYPES.contains(&t));
        if let (true, Some(did)) = (is_mention, feature.get("did").and_then(Value::as_str)) {
            let did = Value::String(did.to_string());
            if !dids.contains(&did) {
                dids.push(did);
            }
        }
    }
    dids
}

/// The strong ref (`{uri, cid}`) of the record `embed` quotes (the PDS's
/// `quoted_record`): both must be strings.
fn quoted_record(embed: &Value) -> Option<Value> {
    let t = embed.get("$type").and_then(Value::as_str)?;
    let strong_ref = if RECORD_TYPES.contains(&t) {
        embed.get("record")?
    } else if RECORD_WITH_MEDIA_TYPES.contains(&t) {
        embed.get("record")?.get("record")?
    } else {
        return None;
    };
    let uri = strong_ref.get("uri").and_then(Value::as_str)?;
    let cid = strong_ref.get("cid").and_then(Value::as_str)?;
    let mut out = Map::new();
    out.insert("uri".into(), Value::String(uri.to_string()));
    out.insert("cid".into(), Value::String(cid.to_string()));
    Some(Value::Object(out))
}

/// Every blob object (`$type: blob`) inside `value`, copied whole (the
/// PDS's `blob_objects`). Recursion depth is bounded by the JSON parser's
/// own limit (128) on every value that reaches it.
fn blob_objects(value: &Value, found: &mut Vec<Value>) {
    match value {
        Value::Object(map) => {
            if map.get("$type").and_then(Value::as_str) == Some("blob") {
                found.push(value.clone());
            } else {
                map.values().for_each(|v| blob_objects(v, found));
            }
        }
        Value::Array(items) => items.iter().for_each(|v| blob_objects(v, found)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// T-P2 (design §5.3): `serde_json::Map` keeps insertion order in this
    /// build, and a parsed object keeps its key order. If a dependency
    /// change turned `preserve_order` off, payload bytes and Horizon's key
    /// order would change silently; this makes it loud.
    #[test]
    fn t_p2_maps_keep_insertion_and_parse_order() {
        let mut m = Map::new();
        m.insert("b".into(), json!(1));
        m.insert("a".into(), json!(2));
        assert_eq!(serde_json::to_string(&Value::Object(m)).expect("json"), r#"{"b":1,"a":2}"#);
        let parsed: Value = serde_json::from_str(r#"{"z":1,"$type":"x","a":{"y":2,"b":3}}"#).expect("json");
        assert_eq!(serde_json::to_string(&parsed).expect("json"), r#"{"z":1,"$type":"x","a":{"y":2,"b":3}}"#);
        assert_eq!(payload("t", None, Some(&parsed))[1..], *br#"{"text":"t","embed":{"z":1,"$type":"x","a":{"y":2,"b":3}}}"#);
    }

    fn fixture() -> Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/private_tier/tests/fixtures/content_parity.json");
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("fixture json")
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// T-P1 (design §5.3): `payload` and `derive` over every input of the
    /// fixture equal what the PDS's own code produced, byte for byte and
    /// key for key, extension keys included.
    #[test]
    fn t_p1_payload_and_clear_fields_match_the_pds() {
        let doc = fixture();
        let cases = doc["cases"].as_array().expect("cases");
        assert!(cases.len() >= 30, "{}", cases.len());
        for case in cases {
            let name = case["name"].as_str().expect("name");
            let text = case["text"].as_str().expect("text");
            let (facets, embed) = (case.get("facets"), case.get("embed"));
            assert_eq!(hex(&payload(text, facets, embed)), case["payloadHex"].as_str().expect("hex"), "{name}: payload");
            let derived = Value::Object(derive(facets, embed).into_record_fields());
            // Key order too: compare the serialized forms.
            assert_eq!(
                serde_json::to_string(&derived).expect("json"),
                serde_json::to_string(&case["clear"]).expect("json"),
                "{name}: clear fields"
            );
        }
        // The fixture carries what §5.3 names.
        let names: Vec<&str> = cases.iter().filter_map(|c| c["name"].as_str()).collect();
        for needed in ["quote without a cid", "nested record with media", "unknown embed type with a blob", "Horizon extension keys", "keys in non-alphabetical order", "non-ASCII and control characters"] {
            assert!(names.contains(&needed), "{needed}");
        }
    }

    /// `index` applies ingest's filters to the wire fields.
    #[test]
    fn index_filters_as_ingest_does() {
        let fields = ClearFields {
            embed_blobs: vec![],
            mention_dids: vec![json!("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"), json!("not a did"), json!(5), json!("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"), json!("did:web:b.example")],
            quote: Some(json!({"uri": "at://did:plc:cccccccccccccccccccccccc/app.bsky.feed.post/3k", "cid": "c"})),
            media_kinds: vec![json!("video"), json!("audio")],
        };
        assert_eq!(
            index(&fields),
            IndexFacts {
                quote_uri: Some("at://did:plc:cccccccccccccccccccccccc/app.bsky.feed.post/3k".into()),
                mention_dids: vec!["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(), "did:web:b.example".into()],
                media_kinds: media::VIDEO,
            }
        );
        let bad_quote = ClearFields { quote: Some(json!({"uri": "at://nobody/x/y", "cid": "c"})), ..ClearFields::default() };
        assert_eq!(index(&bad_quote).quote_uri, None);
        // A wrong-typed field derives nothing.
        assert!(index(&derive(Some(&json!("a string")), Some(&json!(["an", "array"])))) == IndexFacts::default());
        assert!(index(&derive(Some(&json!({"features": []})), Some(&json!(5)))) == IndexFacts::default());
    }

    /// The media derivation equals the read-side classifier over every
    /// embed fixture (private-media-filters T-M6's set).
    #[test]
    fn media_kinds_equal_the_classifier() {
        for (case, embed, bits) in media::embed_fixtures() {
            assert_eq!(index(&derive(None, Some(&embed))).media_kinds, bits, "{case}");
        }
    }
}
