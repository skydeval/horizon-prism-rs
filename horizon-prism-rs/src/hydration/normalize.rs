//! Upstream view normalization (design-v2 §8.6), applied to every view that
//! came from an upstream AppView before it is emitted:
//! 1. **Content stripping** is detected first (for Thread Lens, §10.4): a post
//!    view whose `record` is absent or has neither `text` nor `embed`, or
//!    whose upstream labels include `!takedown` or `!hide`. `!warn` is not
//!    among them (Thread Lens amendment 1 §1): such a post is shown, with
//!    its upstream labels removed, as on every other view.
//! 2. **Labels.** Upstream `labels` arrays are removed at every depth; local
//!    labels (§7.8) are applied instead.
//! 3. **Viewer state.** `viewer` objects are replaced with locally computed
//!    state (§7.5); with an anonymous viewer, `viewer` is `{}`.
//! 4. **Private-tier references.** An embedded record whose URI is
//!    private-tier becomes `app.bsky.embed.record#viewNotFound`; a private
//!    post in a thread becomes `app.bsky.feed.defs#notFoundPost`.
//! 5. **The gate for everything nested** (refract design §7.2 step 4): a
//!    thread node whose post the gate does not list becomes a whole
//!    `notFoundPost` (or `blockedPost`) with no replies; so does a reply
//!    ref; a quoted post the gate hides becomes `viewNotFound` (or
//!    `viewBlocked`), and a list, feed, starter pack or labeler embed by a
//!    hidden creator `viewNotFound`. Arrays are filtered by the callers,
//!    with the gate map `normalized` returns.
//! 6. **The restored marker** (refract design §7.3), on the views of
//!    accounts in R-live, never on a view the gate hides.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use crate::db::locks::uri_author;
use crate::firehose::classifier::{RecordClass, classify};
use crate::moderation::Gate;

/// Labels whose presence upstream marks a post as content-stripped (§10.4).
pub const STRIPPING_LABELS: [&str; 2] = ["!takedown", "!hide"];

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
    /// Thread roots the viewer muted, among [`thread_roots`] of the view.
    pub muted_threads: HashSet<String>,
    /// Local label objects by subject (URI or DID).
    pub labels: HashMap<String, Vec<Value>>,
    /// The gate verdict of each nested post URI and DID (refract design
    /// §7.2 step 4). Absent means visible.
    pub hidden: HashMap<String, Gate>,
    /// DIDs whose own gate verdict is `Hidden`.
    pub hidden_creators: HashSet<String>,
    /// DIDs the viewer blocks or is blocked by.
    pub blocked: HashSet<String>,
    /// The restored marker by DID (refract design §7.3).
    pub restored: HashMap<String, Value>,
}

impl Overlay {
    fn gate(&self, uri: &str) -> Gate {
        self.hidden.get(uri).copied().unwrap_or(Gate::Visible)
    }

    fn author_view(&self, did: &str) -> Value {
        json!({"did": did, "viewer": self.profile_viewers.get(did).cloned().unwrap_or_else(|| json!({}))})
    }

    /// What replaces a thread node or a reply ref of `uri`: `blockedPost`
    /// for a blocked author, `notFoundPost` for a post the gate does not
    /// list.
    fn post_slot(&self, uri: &str) -> Option<Value> {
        let author = uri_author(uri).unwrap_or_default();
        if self.blocked.contains(author) {
            return Some(json!({"$type": "app.bsky.feed.defs#blockedPost", "uri": uri, "blocked": true, "author": self.author_view(author)}));
        }
        (!self.gate(uri).listable()).then(|| json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri, "notFound": true}))
    }

    /// What replaces an embed's `.record` slot.
    fn record_slot(&self, slot: &Map<String, Value>) -> Option<Value> {
        let ty = slot.get("$type").and_then(Value::as_str).unwrap_or_default();
        let uri = slot.get("uri").and_then(Value::as_str).unwrap_or_default();
        if ty == "app.bsky.embed.record#viewRecord" || (slot.contains_key("author") && slot.contains_key("value")) {
            let author = uri_author(uri).unwrap_or_default();
            if self.blocked.contains(author) {
                return Some(json!({"$type": "app.bsky.embed.record#viewBlocked", "uri": uri, "blocked": true, "author": self.author_view(author)}));
            }
            return self.gate(uri).is_hidden().then(|| json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true}));
        }
        let creator = slot.get("creator").and_then(|c| c.get("did")).and_then(Value::as_str);
        let embedded = matches!(
            ty,
            "app.bsky.graph.defs#listView" | "app.bsky.feed.defs#generatorView" | "app.bsky.graph.defs#starterPackViewBasic" | "app.bsky.labeler.defs#labelerView"
        ) || (ty.is_empty() && creator.is_some());
        (embedded && creator.is_some_and(|c| self.hidden_creators.contains(c)))
            .then(|| json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true}))
    }
}

/// What normalization observed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NormalizeReport {
    /// Post URIs detected as content-stripped, before labels were removed.
    pub content_stripped: HashSet<String>,
}

pub(super) fn is_post_view(m: &Map<String, Value>) -> bool {
    m.get("uri").and_then(Value::as_str).is_some() && m.contains_key("author") && m.contains_key("cid")
}

pub(super) fn is_profile_view(m: &Map<String, Value>) -> bool {
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

/// The thread root a post view's record names, if the post is a reply.
fn reply_root(m: &Map<String, Value>) -> Option<&str> {
    m.get("record")
        .or_else(|| m.get("value"))
        .and_then(|r| r.pointer("/reply/root/uri"))
        .and_then(Value::as_str)
}

/// The thread roots named by the reply posts in an upstream view.
#[must_use]
pub fn thread_roots(v: &Value) -> HashSet<String> {
    fn collect(v: &Value, out: &mut HashSet<String>) {
        match v {
            Value::Array(items) => items.iter().for_each(|item| collect(item, out)),
            Value::Object(m) => {
                if let Some(root) = reply_root(m) {
                    out.insert(root.to_string());
                }
                m.values().for_each(|child| collect(child, out));
            }
            _ => {}
        }
    }
    let mut out = HashSet::new();
    collect(v, &mut out);
    out
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
            // 5. The gate for everything nested.
            if ty == "app.bsky.feed.defs#threadViewPost"
                && let Some(u) = m.get("post").and_then(|p| p.get("uri")).and_then(Value::as_str)
                && let Some(mut replacement) = overlay.post_slot(u)
            {
                // Its replies go; the chain above it stays, so Thread Lens
                // still walks past it to the root.
                if let Some(mut parent) = m.remove("parent")
                    && let Some(r) = replacement.as_object_mut()
                {
                    walk(&mut parent, overlay, report);
                    r.insert("parent".into(), parent);
                }
                *v = replacement;
                return;
            }
            let slot = match ty.as_str() {
                "app.bsky.embed.record#view" => m.get_mut("record"),
                "app.bsky.embed.recordWithMedia#view" => m.get_mut("record").and_then(|r| r.get_mut("record")),
                _ => None,
            };
            if let Some(slot) = slot
                && let Some(replacement) = slot.as_object().and_then(|o| overlay.record_slot(o))
            {
                *slot = replacement;
            }
            if let Some(Value::Object(reply)) = m.get_mut("reply") {
                for key in ["root", "parent"] {
                    let replacement = reply
                        .get(key)
                        .and_then(Value::as_object)
                        .filter(|r| r.contains_key("author"))
                        .and_then(|r| r.get("uri").and_then(Value::as_str))
                        .and_then(|u| overlay.post_slot(u));
                    if let Some(r) = replacement {
                        reply.insert(key.to_string(), r);
                    }
                }
            }
            let post_like = is_post_view(m) || ty == "app.bsky.embed.record#viewRecord";
            // 6. The marker, on what the gate let through.
            let marked = if post_like {
                uri.as_deref().and_then(uri_author).and_then(|a| overlay.restored.get(a))
            } else if is_profile_view(m) {
                m.get("did").and_then(Value::as_str).and_then(|d| overlay.restored.get(d))
            } else {
                None
            };
            if let Some(marker) = marked {
                m.insert("restored".into(), marker.clone());
            }
            // 1. Content stripping, before labels go. A restored author's
            // view is never content-stripped: Bluesky's own takedown is
            // what Refract serves past (refract design §7.5).
            if post_like
                && marked.is_none()
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
                    let mut viewer = overlay.post_viewers.get(uri).cloned().unwrap_or_else(|| json!({}));
                    if reply_root(m).is_some_and(|root| overlay.muted_threads.contains(root)) {
                        viewer["threadMuted"] = json!(true);
                    }
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
                // Every `viewer` here is local now (or `{}`): walking it would
                // strip the labels and viewer state of the list views behind
                // `mutedByList` and `blockingByList` (B-73).
                if k == "viewer" {
                    continue;
                }
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

    /// B-73: the list view behind a local `blockingByList` keeps its labels
    /// and the viewer's `blocked`, as on a local profile; the upstream's
    /// viewer and its list view are gone.
    #[test]
    fn a_local_list_view_in_the_viewer_survives_normalization() {
        let did = "did:plc:subject";
        let list = "at://did:plc:owner/app.bsky.graph.list/1";
        let local_list = json!({
            "uri": list, "cid": "c", "name": "Mods", "purpose": "app.bsky.graph.defs#modlist",
            "labels": [], "viewer": {"muted": false, "blocked": "at://did:plc:me/app.bsky.graph.listblock/1"},
            "indexedAt": "2026-01-01T00:00:00Z"
        });
        let local_viewer = json!({"muted": false, "blockingByList": local_list, "blockedBy": false});
        let mut v = json!({
            "did": did, "handle": "s.example", "labels": [label("spam")],
            "viewer": {"blockingByList": {"uri": "at://did:plc:up/app.bsky.graph.list/9", "viewer": {"muted": true}, "labels": [label("x")]}}
        });
        let mut overlay = Overlay::default();
        overlay.profile_viewers.insert(did.into(), local_viewer.clone());
        normalize_upstream(&mut v, &overlay);
        assert_eq!(v["viewer"], local_viewer);
        assert_eq!(v["viewer"]["blockingByList"]["labels"], json!([]));
        assert!(v.get("labels").is_none());
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
            "warned": post("at://did:plc:author/app.bsky.feed.post/8", Some("t"), vec![label("!warn")]),
            "hidden": post("at://did:plc:author/app.bsky.feed.post/7", Some("t"), vec![label("!hide")]),
            "takendown": post("at://did:plc:author/app.bsky.feed.post/6", Some("t"), vec![label("!takedown")]),
            "bare": post("at://did:plc:author/app.bsky.feed.post/5", Some("t"), vec![label("!warn"), label("porn")])
        });
        let report = normalize_upstream(&mut v, &Overlay::default());
        assert_eq!(v["post"]["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound");
        assert!(!v.to_string().contains("secret"));
        assert!(report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/9"));
        // `!hide` and `!takedown` strip; `!warn` does not (Thread Lens
        // amendment 1 §1): the post is shown with its labels removed.
        assert!(report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/7"));
        assert!(report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/6"));
        assert!(!report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/8"));
        assert!(!report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/5"));
        assert!(v["warned"].get("labels").is_none() && v["bare"].get("labels").is_none(), "{v}");
        assert!(!report.content_stripped.contains("at://did:plc:author/app.bsky.feed.post/1"));
        // Refract §7.5: a restored author's view is never content-stripped
        // by Bluesky's own labels; it carries the marker.
        let mut v = json!({"takendown": post("at://did:plc:restored/app.bsky.feed.post/6", Some("t"), vec![label("!takedown")])});
        let mut overlay = Overlay::default();
        overlay.restored.insert("did:plc:restored".into(), json!({"community": "sky", "tier": 1}));
        let report = normalize_upstream(&mut v, &overlay);
        assert!(report.content_stripped.is_empty());
        assert_eq!(v["takendown"]["restored"]["community"], "sky");
    }

    /// Refract T-8ah, T-8al, T-8am, T-13 (refract design §7.2 step 4,
    /// §7.3): a thread node the gate does not list becomes a whole
    /// `notFoundPost` with no replies; a reply ref one too; a hidden quote
    /// becomes `viewNotFound`, a collapsed one stays; a quote by a blocked
    /// author `viewBlocked` with its author; a list embed by a hidden
    /// creator `viewNotFound`, a top-level list array untouched; the marker
    /// sits on visible post and profile views only.
    #[test]
    fn nested_views_are_gated_and_marked() {
        use crate::moderation::HiddenReason;
        let p = |n: &str, author: &str| format!("at://{author}/app.bsky.feed.post/{n}");
        let pv = |uri: &str, author: &str| json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "c", "author": {"did": author, "handle": "h.test"}, "record": {"text": "t"}, "indexedAt": "2026-01-01T00:00:00Z"});
        let (ok, gone, hide, blocker, restored) = ("did:plc:ok", "did:plc:gone", "did:plc:hide", "did:plc:blocker", "did:plc:restored");
        let mut quoting = pv(&p("q", ok), ok);
        quoting["embed"] = json!({"$type": "app.bsky.embed.record#view", "record": {"$type": "app.bsky.embed.record#viewRecord", "uri": p("1", gone), "cid": "c", "author": {"did": gone, "handle": "g"}, "value": {"text": "x"}}});
        let mut collapsed = pv(&p("c", ok), ok);
        collapsed["embed"] = json!({"$type": "app.bsky.embed.recordWithMedia#view", "record": {"$type": "app.bsky.embed.record#view", "record": {"$type": "app.bsky.embed.record#viewRecord", "uri": p("1", hide), "cid": "c", "author": {"did": hide, "handle": "g"}, "value": {"text": "x"}}}, "media": {}});
        let mut blocked_quote = pv(&p("b", ok), ok);
        blocked_quote["embed"] = json!({"$type": "app.bsky.embed.record#view", "record": {"$type": "app.bsky.embed.record#viewRecord", "uri": p("1", blocker), "cid": "c", "author": {"did": blocker, "handle": "g"}, "value": {"text": "x"}}});
        let mut list_embed = pv(&p("l", ok), ok);
        list_embed["embed"] = json!({"$type": "app.bsky.embed.record#view", "record": {"$type": "app.bsky.graph.defs#listView", "uri": format!("at://{gone}/app.bsky.graph.list/1"), "creator": {"did": gone, "handle": "g"}, "name": "L"}});
        let mut v = json!({
            "thread": {"$type": "app.bsky.feed.defs#threadViewPost", "post": pv(&p("root", restored), restored), "replies": [
                {"$type": "app.bsky.feed.defs#threadViewPost", "post": pv(&p("2", gone), gone), "replies": [{"$type": "app.bsky.feed.defs#threadViewPost", "post": pv(&p("3", ok), ok)}]},
                {"$type": "app.bsky.feed.defs#threadViewPost", "post": pv(&p("2", hide), hide)}
            ]},
            "feed": [{"post": pv(&p("f", ok), ok), "reply": {"root": pv(&p("1", gone), gone), "parent": pv(&p("1", blocker), blocker)}}],
            "quotes": [quoting, collapsed, blocked_quote, list_embed],
            "lists": [{"$type": "app.bsky.graph.defs#listView", "uri": format!("at://{gone}/app.bsky.graph.list/1"), "creator": {"did": gone, "handle": "g"}, "name": "L"}]
        });
        let mut overlay = Overlay::default();
        overlay.hidden.insert(p("2", gone), Gate::Hidden(HiddenReason::Takedown));
        overlay.hidden.insert(p("1", gone), Gate::Hidden(HiddenReason::Takedown));
        overlay.hidden.insert(gone.to_string(), Gate::Hidden(HiddenReason::Takedown));
        overlay.hidden.insert(p("2", hide), Gate::Collapsed);
        overlay.hidden.insert(p("1", hide), Gate::Collapsed);
        overlay.hidden_creators.insert(gone.to_string());
        overlay.blocked.insert(blocker.to_string());
        overlay.restored.insert(restored.to_string(), json!({"community": "sky", "tier": 1}));
        normalize_upstream(&mut v, &overlay);
        let replies = &v["thread"]["replies"];
        assert_eq!(replies[0], json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": p("2", gone), "notFound": true}), "the whole node, no replies");
        assert_eq!(replies[1]["$type"], "app.bsky.feed.defs#notFoundPost", "a collapsed post is not listed in a thread");
        assert_eq!(v["feed"][0]["reply"]["root"]["$type"], "app.bsky.feed.defs#notFoundPost");
        assert_eq!(v["feed"][0]["reply"]["parent"]["$type"], "app.bsky.feed.defs#blockedPost");
        assert_eq!(v["feed"][0]["reply"]["parent"]["author"]["did"], blocker);
        assert_eq!(v["quotes"][0]["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound");
        assert_eq!(v["quotes"][1]["embed"]["record"]["record"]["$type"], "app.bsky.embed.record#viewRecord", "a collapsed quote stays");
        assert_eq!(v["quotes"][2]["embed"]["record"]["$type"], "app.bsky.embed.record#viewBlocked");
        assert_eq!(v["quotes"][3]["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound");
        assert_eq!(v["lists"][0]["$type"], "app.bsky.graph.defs#listView", "top-level arrays are the callers'");
        assert_eq!(v["thread"]["post"]["restored"], json!({"community": "sky", "tier": 1}));
        assert_eq!(v["thread"]["post"]["author"]["restored"], json!({"community": "sky", "tier": 1}));
        assert_eq!(v.to_string().matches("\"restored\":{").count(), 2, "on visible views of the restored account only");
    }
}
