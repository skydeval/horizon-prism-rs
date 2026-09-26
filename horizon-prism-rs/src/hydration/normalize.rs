//! Upstream view normalization (design-v2 §8.6), applied to every view that
//! came from an upstream AppView before it is emitted:
//! 1. **Content stripping** is detected first (for Thread Lens, §10.4): a post
//!    view whose `record` is absent or has neither `text` nor `embed`, or
//!    whose upstream labels include `!takedown`, `!hide` or `!warn`.
//! 2. **Labels.** Upstream `labels` arrays are removed at every depth; local
//!    labels (§7.8) are applied instead.
//! 3. **Viewer state.** `viewer` objects are replaced with locally computed
//!    state (§7.5); with an anonymous viewer, `viewer` is `{}`.
//! 4. **Private-tier references.** An embedded record whose URI is
//!    private-tier becomes `app.bsky.embed.record#viewNotFound`; a private
//!    post in a thread becomes `app.bsky.feed.defs#notFoundPost`.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use crate::db::locks::uri_author;
use crate::firehose::classifier::{RecordClass, classify};

/// Labels whose presence upstream marks a post as content-stripped (§10.4).
pub const STRIPPING_LABELS: [&str; 3] = ["!takedown", "!hide", "!warn"];

/// Whether an at-URI names a private-tier record (§7.1, §7.3).
#[must_use]
pub fn is_private_uri(uri: &str) -> bool {
    let Some(rest) = uri.strip_prefix("at://") else {
        return false;
    };
    let mut parts = rest.splitn(3, '/');
    let (_, Some(nsid), rkey) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    matches!(
        classify(nsid, rkey.unwrap_or_default()),
        RecordClass::Kryphocron {
            tier: kryphocron_lexicons::Tier::Private,
            ..
        }
    )
}

/// Local state applied in place of what upstream said.
#[derive(Debug, Clone, Default)]
pub struct Overlay {
    /// Actor viewer state by DID (absent means `{}`).
    pub profile_viewers: HashMap<String, Value>,
    /// Post viewer state by URI (absent means `{}`).
    pub post_viewers: HashMap<String, Value>,
    /// Local label objects by subject (URI or DID).
    pub labels: HashMap<String, Vec<Value>>,
}

/// What normalization observed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NormalizeReport {
    /// Post URIs detected as content-stripped, before labels were removed.
    pub content_stripped: HashSet<String>,
}

fn is_post_view(m: &Map<String, Value>) -> bool {
    m.get("uri").and_then(Value::as_str).is_some() && m.contains_key("author") && m.contains_key("cid")
}

fn is_profile_view(m: &Map<String, Value>) -> bool {
    m.get("did").and_then(Value::as_str).is_some() && m.contains_key("handle")
}

fn stripped(m: &Map<String, Value>) -> bool {
    let record_empty = match m.get("record").or_else(|| m.get("value")) {
        None | Some(Value::Null) => true,
        Some(r) => r.get("text").is_none() && r.get("embed").is_none(),
    };
    let label_hit = m.get("labels").and_then(Value::as_array).is_some_and(|ls| {
        ls.iter()
            .filter_map(|l| l.get("val").and_then(Value::as_str))
            .any(|v| STRIPPING_LABELS.contains(&v))
    });
    record_empty || label_hit
}

fn walk(v: &mut Value, overlay: &Overlay, report: &mut NormalizeReport) {
    match v {
        Value::Array(items) => {
            for item in items {
                walk(item, overlay, report);
            }
        }
        Value::Object(m) => {
            let ty = m.get("$type").and_then(Value::as_str).unwrap_or_default().to_string();
            let uri = m.get("uri").and_then(Value::as_str).map(str::to_string);
            // 4. Private-tier references never survive.
            if let Some(uri) = &uri
                && is_private_uri(uri)
            {
                let replacement = if ty.starts_with("app.bsky.embed.record#view") {
                    json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true})
                } else if is_post_view(m) || ty.starts_with("app.bsky.feed.defs#") {
                    json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri, "notFound": true})
                } else {
                    json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true})
                };
                *v = replacement;
                return;
            }
            let post_like = is_post_view(m) || ty == "app.bsky.embed.record#viewRecord";
            // 1. Content stripping, before labels go.
            if post_like
                && let Some(uri) = &uri
                && stripped(m)
            {
                report.content_stripped.insert(uri.clone());
            }
            // 2. Labels: upstream removed; local applied on labelled subjects.
            m.remove("labels");
            // 3. Viewer state.
            if post_like && let Some(uri) = &uri {
                if m.contains_key("viewer") || is_post_view(m) {
                    let viewer = overlay.post_viewers.get(uri).cloned().unwrap_or_else(|| json!({}));
                    m.insert("viewer".into(), viewer);
                }
                if let Some(local) = overlay.labels.get(uri) {
                    m.insert("labels".into(), Value::Array(local.clone()));
                }
            } else if is_profile_view(m) {
                let did = m.get("did").and_then(Value::as_str).unwrap_or_default().to_string();
                let viewer = overlay.profile_viewers.get(&did).cloned().unwrap_or_else(|| json!({}));
                m.insert("viewer".into(), viewer);
                if let Some(local) = overlay.labels.get(&did) {
                    m.insert("labels".into(), Value::Array(local.clone()));
                }
            } else if m.contains_key("viewer") {
                m.insert("viewer".into(), json!({}));
            }
            for (k, child) in m.iter_mut() {
                // Record bodies are the author's data: labels inside them
                // (self-labels) are part of the record, not upstream labels.
                if k == "record" && !child.get("$type").and_then(Value::as_str).is_some_and(|t| t.starts_with("app.bsky.embed")) && post_like {
                    continue;
                }
                if k == "value" && ty == "app.bsky.embed.record#viewRecord" {
                    continue;
                }
                walk(child, overlay, report);
            }
        }
        _ => {}
    }
}

/// Normalize an upstream view in place, returning what was observed.
pub fn normalize_upstream(view: &mut Value, overlay: &Overlay) -> NormalizeReport {
    let mut report = NormalizeReport::default();
    walk(view, overlay, &mut report);
    report
}

/// The author DIDs and post URIs an upstream view mentions (to build the
/// overlay before normalizing).
#[must_use]
pub fn subjects(view: &Value) -> (HashSet<String>, HashSet<String>) {
    fn go(v: &Value, dids: &mut HashSet<String>, uris: &mut HashSet<String>) {
        match v {
            Value::Array(a) => a.iter().for_each(|x| go(x, dids, uris)),
            Value::Object(m) => {
                if is_profile_view(m)
                    && let Some(d) = m.get("did").and_then(Value::as_str)
                {
                    dids.insert(d.to_string());
                }
                if is_post_view(m)
                    && let Some(u) = m.get("uri").and_then(Value::as_str)
                {
                    uris.insert(u.to_string());
                    if let Some(a) = uri_author(u) {
                        dids.insert(a.to_string());
                    }
                }
                m.values().for_each(|x| go(x, dids, uris));
            }
            _ => {}
        }
    }
    let (mut dids, mut uris) = (HashSet::new(), HashSet::new());
    go(view, &mut dids, &mut uris);
    (dids, uris)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(val: &str) -> Value {
        json!({"src": "did:web:upstream.example", "uri": "x", "val": val, "cts": "2026-01-01T00:00:00Z"})
    }

    fn post(uri: &str, text: Option<&str>, labels: Vec<Value>) -> Value {
        let record = text.map_or_else(|| json!({"$type": "app.bsky.feed.post"}), |t| json!({"$type": "app.bsky.feed.post", "text": t, "labels": {"$type": "com.atproto.label.defs#selfLabels", "values": [{"val": "nudity"}]}}));
        json!({
            "$type": "app.bsky.feed.defs#postView",
            "uri": uri, "cid": "bafy",
            "author": {"did": "did:plc:author", "handle": "a.example", "labels": [label("spam")], "viewer": {"muted": true, "following": "at://x"}},
            "record": record,
            "labels": labels,
            "viewer": {"like": "at://upstream-like"},
            "indexedAt": "2026-01-01T00:00:00Z"
        })
    }

    fn count_key(v: &Value, key: &str) -> usize {
        match v {
            Value::Array(a) => a.iter().map(|x| count_key(x, key)).sum(),
            Value::Object(m) => usize::from(m.contains_key(key)) + m.values().map(|x| count_key(x, key)).sum::<usize>(),
            _ => 0,
        }
    }

    #[test]
    fn strips_upstream_labels_at_every_depth() {
        let mut thread = json!({
            "thread": {
                "$type": "app.bsky.feed.defs#threadViewPost",
                "post": post("at://did:plc:author/app.bsky.feed.post/1", Some("hi"), vec![label("porn")]),
                "parent": {"$type": "app.bsky.feed.defs#threadViewPost",
                           "post": post("at://did:plc:author/app.bsky.feed.post/0", Some("p"), vec![label("!warn")])},
                "replies": [{"$type": "app.bsky.feed.defs#threadViewPost",
                             "post": post("at://did:plc:author/app.bsky.feed.post/2", Some("r"), vec![label("sexual")])}]
            }
        });
        let before = count_key(&thread, "labels");
        assert!(before >= 6);
        normalize_upstream(&mut thread, &Overlay::default());
        // Only the three record self-label blocks remain (they are record data).
        let after = count_key(&thread, "labels");
        assert_eq!(after, 3, "{thread:#}");
        assert_eq!(thread["thread"]["post"]["record"]["labels"]["values"][0]["val"], "nudity");
    }

    #[test]
    fn replaces_viewer_state_and_applies_local_labels() {
        let uri = "at://did:plc:author/app.bsky.feed.post/1";
        let mut v = json!({"posts": [post(uri, Some("hi"), vec![label("porn")])]});
        let mut overlay = Overlay::default();
        overlay.post_viewers.insert(uri.into(), json!({"like": "at://local-like"}));
        overlay.labels.insert("did:plc:author".into(), vec![json!({"src": "did:web:here", "uri": "did:plc:author", "val": "!hide", "cts": "c"})]);
        normalize_upstream(&mut v, &overlay);
        assert_eq!(v["posts"][0]["viewer"], json!({"like": "at://local-like"}));
        assert_eq!(v["posts"][0]["author"]["viewer"], json!({}), "anonymous-equivalent viewer");
        assert_eq!(v["posts"][0]["author"]["labels"][0]["src"], "did:web:here");
        assert!(v["posts"][0].get("labels").is_none());
    }

    #[test]
    fn private_references_become_not_found_and_stripping_is_recorded() {
        let private = "at://did:plc:author/tools.kryphocron.feed.postPrivate/1";
        assert!(is_private_uri(private));
        assert!(!is_private_uri("at://did:plc:author/app.bsky.feed.post/1"));
        assert!(!is_private_uri("at://did:plc:author/tools.kryphocron.feed.postPublic/1"));
        let mut v = json!({
            "post": {
                "uri": "at://did:plc:author/app.bsky.feed.post/1", "cid": "c",
                "author": {"did": "did:plc:author", "handle": "a"},
                "record": {"text": "quoting"},
                "embed": {"$type": "app.bsky.embed.record#view",
                          "record": {"$type": "app.bsky.embed.record#viewRecord", "uri": private, "cid": "c",
                                     "author": {"did": "did:plc:author", "handle": "a"}, "value": {"text": "secret"}}}
            },
            "stripped": post("at://did:plc:author/app.bsky.feed.post/9", None, vec![]),
            "warned": post("at://did:plc:author/app.bsky.feed.post/8", Some("t"), vec![label("!warn")])
        });
        let report = normalize_upstream(&mut v, &Overlay::default());
        assert_eq!(v["post"]["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound");
        assert!(!v.to_string().contains("secret"));
        assert!(report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/9"));
        assert!(report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/8"));
        assert!(!report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/1"));
    }
}
