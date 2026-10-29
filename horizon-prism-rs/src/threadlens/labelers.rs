//! The reader's labelers on the posts Prism built itself.
//!
//! A post whose view came from an upstream call made with the request's
//! labelers already carries their labels (normalization kept them). A post
//! built from Prism's own rows or from its record did not ask anyone: for
//! those, one `getPosts` per home network and 25 posts, with the header,
//! and only the labels are taken from the answer (the post's and its
//! author's, from the accepted labelers other than Prism's own). Nothing
//! else of the answer is read, so what the response shows is decided as
//! before; the labels only add to it. A post the upstream does not return
//! keeps the labels it has.

use std::collections::HashMap;

use futures::StreamExt;
use serde_json::Value;

use super::ladder::{CASCADE_CONCURRENCY, GET_POSTS_MAX, Ladder};
use crate::appview_clients::accept_labelers::{LabelerScope, current};
use crate::constellation::{Call, Fetched};
use crate::db::locks::uri_author;
use crate::hydration::lens::LensNode;
use crate::hydration::normalize::{is_private_uri, same_label};
use crate::xrpc::pools::Requester;

/// The labels of `labels` from the accepted labelers, Prism's own left out.
fn accepted(labels: Option<&Value>, scope: &LabelerScope, own: &str) -> Vec<Value> {
    labels
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|l| l.get("src").and_then(Value::as_str).is_some_and(|s| s != own && scope.accept().contains(s)))
        .cloned()
        .collect()
}

/// Add `extra` to the `labels` array of `view`, without repeats.
fn add_labels(view: &mut Value, extra: Vec<Value>) {
    if extra.is_empty() || !view.is_object() {
        return;
    }
    if !view.get("labels").is_some_and(Value::is_array) {
        view["labels"] = Value::Array(Vec::new());
    }
    if let Some(labels) = view.get_mut("labels").and_then(Value::as_array_mut) {
        for label in extra {
            if !labels.iter().any(|l| same_label(l, &label)) {
                labels.push(label);
            }
        }
    }
}

/// Give the post nodes that were not fetched with the request's labelers
/// the labels those labelers applied. Outside a request that named any,
/// nothing.
pub async fn add_reader_labels(ladder: &Ladder, req: &Requester, nodes: &mut [LensNode]) {
    let Some(scope) = current() else {
        return;
    };
    let h = ladder.hydrator();
    // Prism's own labels are on every view it serves: the scope counts its
    // labeler as applied from the start (threadlens-quoted-posts design §3).
    let own = h.labeler_did().to_string();
    let mut wanted: Vec<String> = Vec::new();
    for node in nodes.iter() {
        if let LensNode::Post { uri, .. } = node
            && !is_private_uri(uri)
            && !scope.is_labelled(uri)
            && !wanted.contains(uri)
        {
            wanted.push(uri.clone());
        }
    }
    if wanted.is_empty() {
        return;
    }
    // One `getPosts` per home network and 25 posts, as the ladder groups
    // them, and with the ladder's rule for the community network (C4).
    let public_on = h.scope().public_mode().is_on();
    let mut groups: Vec<(Option<String>, Vec<String>)> = Vec::new();
    for uri in wanted {
        let author = uri_author(&uri).unwrap_or_default();
        let home = ladder.router().resolve_did(req, author).await;
        let restored = h.refract_snapshot(req).await.is_restored(author);
        if !(restored || !home.is_community() || !public_on) {
            continue;
        }
        match groups.iter_mut().find(|(network, group)| *network == home.network_name && group.len() < GET_POSTS_MAX) {
            Some((_, group)) => group.push(uri),
            None => groups.push((home.network_name.clone(), vec![uri])),
        }
    }
    let answers: Vec<Fetched> = futures::stream::iter(groups)
        .map(|(network, group)| async move { ladder.cascade().fetch(req, network.as_deref(), Call::Posts(&group)).await })
        .buffer_unordered(CASCADE_CONCURRENCY)
        .collect()
        .await;
    let mut found: HashMap<String, (Vec<Value>, Vec<Value>)> = HashMap::new();
    for answer in answers {
        let Some(body) = answer.hit() else { continue };
        for view in body.get("posts").and_then(Value::as_array).into_iter().flatten() {
            let Some(uri) = view.get("uri").and_then(Value::as_str) else { continue };
            let post = accepted(view.get("labels"), &scope, &own);
            let author = accepted(view.pointer("/author/labels"), &scope, &own);
            found.insert(uri.to_string(), (post, author));
        }
    }
    for node in nodes.iter_mut() {
        if let LensNode::Post { uri, post, .. } = node
            && let Some((post_labels, author_labels)) = found.remove(uri.as_str())
        {
            add_labels(post, post_labels);
            if let Some(author) = post.get_mut("author") {
                add_labels(author, author_labels);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::appview_clients::accept_labelers::AcceptLabelers;

    #[test]
    fn only_accepted_labels_are_added_once() {
        let scope = LabelerScope::new(AcceptLabelers::parse("did:plc:meme, did:web:prism.example").expect("two"), "did:web:prism.example");
        let labels = json!([
            {"src": "did:plc:meme", "uri": "at://x", "val": "cat"},
            {"src": "did:plc:other", "uri": "at://x", "val": "dog"},
            {"src": "did:web:prism.example", "uri": "at://x", "val": "own"}
        ]);
        let got = accepted(Some(&labels), &scope, "did:web:prism.example");
        assert_eq!(got, [json!({"src": "did:plc:meme", "uri": "at://x", "val": "cat"})]);
        let mut view = json!({"labels": [{"src": "did:plc:meme", "uri": "at://x", "val": "cat"}]});
        add_labels(&mut view, got.clone());
        assert_eq!(view["labels"].as_array().map(Vec::len), Some(1));
        let mut bare = json!({"uri": "at://x"});
        add_labels(&mut bare, got);
        assert_eq!(bare["labels"][0]["val"], "cat");
        let mut untouched = json!({"uri": "at://x"});
        add_labels(&mut untouched, Vec::new());
        assert!(untouched.get("labels").is_none());
    }
}
