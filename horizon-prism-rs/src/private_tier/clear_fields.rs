//! The #504 clear fields, one derivation (private-encode design §5): the
//! payload a private post's content is encoded as, the clear fields a
//! correct encoder writes beside it, and the facts Prism indexes from them.
//! Used by ingest's fallback and by the read-time check.
//!
//! The derivation is kryphocron's own (`kryphocron::post_content`, 0.4.4):
//! the encoder Horizon runs in the client is kryphocron, so Prism verifies
//! with the same function the encoder used. The parity tests hold it equal
//! to the PDS's encoder over a fixture the PDS's code generated.
//!
//! - [`payload`] is the content form: `0xFF`, then a JSON object with
//!   `text`, then `facets`, then `embed` when present, each the value as
//!   parsed (`PostContent::to_payload`). Nothing inside `facets` or `embed`
//!   is read, renamed, reordered or dropped, so extension keys survive
//!   (serde_json's `preserve_order`, declared in Cargo.toml, keeps every
//!   object's key order).
//! - [`derive`] is `kryphocron::post_content::derive_clear_fields`, in wire
//!   form. It accepts any JSON: a field of the wrong type derives no
//!   mention, quote or media kind.
//! - [`index`] is what Prism indexes from wire clear fields, with ingest's
//!   filters.
//!
//! [`ClearFields`] holds content-derived values and is not `Debug`;
//! nothing here logs, and nothing panics on an input value.

use kryphocron::post_content::{PostContent, derive_clear_fields};
use serde_json::{Map, Value};

use super::media;
use crate::db::locks::uri_author;

/// The clear fields a correct encoder writes for one post's content, in wire
/// form (what goes on the record). Each is empty (or `None`) when the
/// content has none, and is then omitted from the record.
#[derive(Clone, Default, PartialEq)]
pub(super) struct ClearFields {
    /// `embedBlobs`: every blob object of the embed, whole, in traversal
    /// order.
    pub embed_blobs: Vec<Value>,
    /// `mentionDids`: the mentioned DIDs, deduplicated in first-appearance
    /// order, DID syntax not checked (as an encoder writes them).
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
    #[cfg_attr(not(test), expect(dead_code, reason = "the wire form a client encoder must produce; only the parity tests build it (Prism's encoder was withdrawn)"))]
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

/// The content-form payload of `text`, `facets` and `embed`
/// (`PostContent::to_payload`).
#[cfg_attr(not(test), expect(dead_code, reason = "the wire form a client encoder must produce; only the parity tests build it (Prism's encoder was withdrawn)"))]
pub(super) fn payload(text: &str, facets: Option<&Value>, embed: Option<&Value>) -> Vec<u8> {
    PostContent {
        text: text.to_string(),
        facets: facets.cloned(),
        embed: embed.cloned(),
    }
    .to_payload()
}

/// The clear fields of `facets` and `embed`
/// (`kryphocron::post_content::derive_clear_fields`), in wire form.
pub(super) fn derive(facets: Option<&Value>, embed: Option<&Value>) -> ClearFields {
    let fields = derive_clear_fields(facets, embed);
    ClearFields {
        embed_blobs: fields.embed_blobs,
        mention_dids: fields.mention_dids.into_iter().map(Value::String).collect(),
        quote: fields.quote,
        media_kinds: fields.media_kinds.into_iter().map(|k| Value::String(k.to_string())).collect(),
    }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The derivation Prism used before it called kryphocron's (private-encode
    /// E1, a port of the PDS's), kept for the parity test below: kryphocron's
    /// must give the same wire fields over every fixture input.
    fn old_port(facets: Option<&Value>, embed: Option<&Value>) -> ClearFields {
        const IMAGE: [&str; 4] = ["app.bsky.embed.images", "app.bsky.embed.gallery", "tools.kryphocron.feed.postPublic#embedImage", "tools.kryphocron.feed.postPublic#embedGallery"];
        const VIDEO: [&str; 2] = ["app.bsky.embed.video", "tools.kryphocron.feed.postPublic#embedVideo"];
        const RWM: [&str; 2] = ["app.bsky.embed.recordWithMedia", "tools.kryphocron.feed.postPublic#embedRecordWithMedia"];
        const REC: [&str; 2] = ["app.bsky.embed.record", "tools.kryphocron.feed.postPublic#embedRecord"];
        const MENTION: [&str; 3] = ["app.bsky.richtext.facet#mention", "tools.kryphocron.feed.postPublic#mention", "#mention"];
        fn blobs(v: &Value, out: &mut Vec<Value>) {
            match v {
                Value::Object(m) if m.get("$type").and_then(Value::as_str) == Some("blob") => out.push(v.clone()),
                Value::Object(m) => m.values().for_each(|c| blobs(c, out)),
                Value::Array(a) => a.iter().for_each(|c| blobs(c, out)),
                _ => {}
            }
        }
        let mut out = ClearFields::default();
        for f in facets.and_then(Value::as_array).into_iter().flatten().filter_map(|f| f.get("features").and_then(Value::as_array)).flatten() {
            if f.get("$type").and_then(Value::as_str).is_some_and(|t| MENTION.contains(&t))
                && let Some(d) = f.get("did").and_then(Value::as_str)
                && !out.mention_dids.contains(&json!(d))
            {
                out.mention_dids.push(json!(d));
            }
        }
        if let Some(e) = embed {
            let t = e.get("$type").and_then(Value::as_str);
            let r = match t {
                Some(t) if REC.contains(&t) => e.get("record"),
                Some(t) if RWM.contains(&t) => e.get("record").and_then(|r| r.get("record")),
                _ => None,
            };
            if let Some((u, c)) = r.and_then(|r| Some((r.get("uri")?.as_str()?, r.get("cid")?.as_str()?))) {
                out.quote = Some(json!({"uri": u, "cid": c}));
            }
            let media = match t {
                Some(t) if RWM.contains(&t) => e.get("media").and_then(|m| m.get("$type")).and_then(Value::as_str),
                other => other,
            };
            match media {
                Some(m) if IMAGE.contains(&m) => out.media_kinds.push(json!("image")),
                Some(m) if VIDEO.contains(&m) => out.media_kinds.push(json!("video")),
                _ => {}
            }
            blobs(e, &mut out.embed_blobs);
        }
        out
    }

    /// Parity between kryphocron's derivation and the one Prism ingested
    /// with before (the PDS's): equal over every fixture input, every media
    /// fixture, and wrong-typed fields.
    #[test]
    fn kryphocrons_derivation_equals_the_old_port() {
        let doc = fixture();
        let mut inputs: Vec<(Option<Value>, Option<Value>)> = doc["cases"]
            .as_array()
            .expect("cases")
            .iter()
            .map(|c| (c.get("facets").cloned(), c.get("embed").cloned()))
            .collect();
        inputs.extend(media::embed_fixtures().into_iter().map(|(_, e, _)| (None, Some(e))));
        inputs.extend([(Some(json!("a string")), Some(json!([1, {"$type": "blob"}]))), (Some(json!({"features": []})), Some(json!(5)))]);
        for (facets, embed) in &inputs {
            assert!(derive(facets.as_ref(), embed.as_ref()) == old_port(facets.as_ref(), embed.as_ref()), "{facets:?} {embed:?}");
        }
    }

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

    /// T-P1 (design §5.3): kryphocron's `to_payload` and
    /// `derive_clear_fields`, as Prism calls them, over every input of the
    /// fixture equal what the PDS's own code produced, byte for byte and
    /// key for key, extension keys included; and `from_payload` reads each
    /// payload back to its input. So the client encoder (kryphocron), the
    /// PDS and Prism's check agree.
    #[test]
    fn t_p1_payload_and_clear_fields_match_the_pds() {
        let doc = fixture();
        let cases = doc["cases"].as_array().expect("cases");
        assert!(cases.len() >= 30, "{}", cases.len());
        for case in cases {
            let name = case["name"].as_str().expect("name");
            let text = case["text"].as_str().expect("text");
            let (facets, embed) = (case.get("facets"), case.get("embed"));
            let bytes = payload(text, facets, embed);
            assert_eq!(hex(&bytes), case["payloadHex"].as_str().expect("hex"), "{name}: payload");
            let back = PostContent::from_payload(&bytes).expect("reads back");
            assert_eq!((back.text.as_str(), back.facets.as_ref(), back.embed.as_ref()), (text, facets, embed), "{name}: from_payload");
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
