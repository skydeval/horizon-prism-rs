//! `getThread` assembly (design-v2 §10.4).
//!
//! - **Structure** comes from `app.bsky.feed.getPostThread` on the anchor
//!   author's home AppView, then the cascade: `unauth` or AppView
//!   `service_jwt`, never a user identity, with `depth` and `parentHeight`
//!   passed through. `#threadViewPost` nodes become post nodes after
//!   normalization; `#blockedPost` stubs run ladder steps 1, 2 and 4 and
//!   otherwise become `blocked` gaps (blocks between other parties: the
//!   fetch carries no viewer); `#notFoundPost` stubs run the full ladder.
//! - **No structure** (every upstream missed or was transient): the anchor
//!   via the ladder, then its parent chain by `reply.parent.uri` up to
//!   `parentHeight`, stopping at (and emitting) the first gap.
//! - **Replies:** local `posts_parent` replies and Constellation `/links`
//!   replies absent from the structure are appended breadth-first to
//!   `depth`, each through the ladder.
//! - **Cap:** 200 nodes; the rest are dropped without distinction.
//!
//! Output order: the parent chain (root-most first), the anchor, then
//! replies breadth-first.

use std::collections::HashSet;

use futures::StreamExt;
use serde_json::Value;

use super::gaps::Gap;
use super::ladder::{Ladder, Mode, Resolved};
use crate::constellation::Call;
use crate::db::locks::uri_author;
use crate::hydration::lens::LensNode;
use crate::hydration::normalize::{NormalizeReport, is_private_uri};
use crate::xrpc::pools::Requester;

/// Nodes per response (anchor, parents and replies).
pub const NODE_CAP: usize = 200;
/// Replies listed per node.
pub const REPLIES_PER_NODE: u32 = 50;
/// Ladder runs in flight.
pub const LADDER_CONCURRENCY: usize = 8;

fn str_at(v: &Value, pointer: &str) -> Option<String> {
    v.pointer(pointer).and_then(Value::as_str).map(str::to_string)
}

fn node_from(resolved: Resolved, uri: &str, parent_uri: Option<String>) -> LensNode {
    match resolved {
        Resolved::Post(post) => LensNode::post(post),
        Resolved::Gap(gap) => LensNode::Gap {
            uri: uri.to_string(),
            parent_uri,
            gap,
        },
    }
}

/// One structure node (already normalized) to a lens node.
async fn structure_node(
    ladder: &Ladder,
    req: &Requester,
    v: &Value,
    parent_uri: Option<String>,
    report: &NormalizeReport,
) -> Result<Option<LensNode>, sqlx::Error> {
    let ty = v.get("$type").and_then(Value::as_str).unwrap_or_default();
    if ty == "app.bsky.feed.defs#threadViewPost" || v.get("post").is_some() {
        let Some(uri) = str_at(v, "/post/uri") else {
            return Ok(None);
        };
        if report.content_stripped.contains(&uri) {
            let parent = str_at(v, "/post/record/reply/parent/uri").or(parent_uri);
            return Ok(Some(LensNode::Gap {
                uri,
                parent_uri: parent,
                gap: Gap::ContentStripped,
            }));
        }
        return Ok(Some(LensNode::post(v["post"].clone())));
    }
    let Some(uri) = str_at(v, "/uri") else {
        return Ok(None);
    };
    if ty == "app.bsky.feed.defs#blockedPost" || v.get("blocked").is_some() {
        let node = match ladder.resolve(req, &uri, Mode::BlockedStub).await? {
            Resolved::Post(post) => LensNode::post(post),
            Resolved::Gap(gap @ (Gap::TierHidden | Gap::Deleted | Gap::ContentStripped)) => LensNode::Gap { uri, parent_uri, gap },
            Resolved::Gap(_) => LensNode::Gap {
                uri,
                parent_uri,
                gap: Gap::Blocked,
            },
        };
        return Ok(Some(node));
    }
    // `#notFoundPost` and anything else with a URI: the full ladder.
    let resolved = ladder.resolve(req, &uri, Mode::Full).await?;
    Ok(Some(node_from(resolved, &uri, parent_uri)))
}

/// The nodes for a `getThread` response.
///
/// # Errors
///
/// Database errors.
pub async fn get_thread(ladder: &Ladder, req: &Requester, anchor: &str, depth: u32, parent_height: u32) -> Result<Vec<LensNode>, sqlx::Error> {
    if is_private_uri(anchor) {
        return Ok(vec![LensNode::Gap {
            uri: anchor.to_string(),
            parent_uri: None,
            gap: Gap::TierHidden,
        }]);
    }
    let mut nodes: Vec<LensNode> = Vec::new();
    let mut structured = false;
    if ladder.hydrator().lens_visible(anchor).await?
        && let Some(author) = uri_author(anchor)
    {
        let home = ladder.router().resolve_did(req, author).await;
        if !home.is_community()
            && let Some(body) = ladder
                .cascade()
                .fetch(req, home.network_name.as_deref(), Call::PostThread { uri: anchor, depth, parent_height })
                .await
                .hit()
            && let Some(thread) = body.get("thread").filter(|t| t.is_object()).cloned()
        {
            structured = true;
            let (thread, report) = ladder.hydrator().lens_normalize(thread).await?;
            // Parents, root-most first.
            let mut chain: Vec<Value> = Vec::new();
            let mut cur = thread.get("parent").cloned();
            while let Some(p) = cur.filter(Value::is_object) {
                cur = p.get("parent").cloned();
                chain.push(p);
            }
            chain.reverse();
            let mut above: Option<String> = None;
            for p in &chain {
                if let Some(n) = structure_node(ladder, req, p, above.clone(), &report).await? {
                    above = Some(n.uri().to_string());
                    nodes.push(n);
                }
            }
            if let Some(n) = structure_node(ladder, req, &thread, above, &report).await? {
                nodes.push(n);
            }
            // Upstream replies, breadth-first.
            let mut level: Vec<(Value, String)> = thread
                .get("replies")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|r| (r.clone(), anchor.to_string()))
                .collect();
            while !level.is_empty() && nodes.len() < NODE_CAP {
                let mut next = Vec::new();
                for (r, parent) in level {
                    if nodes.len() >= NODE_CAP {
                        break;
                    }
                    if let Some(n) = structure_node(ladder, req, &r, Some(parent), &report).await? {
                        let uri = n.uri().to_string();
                        for child in r.get("replies").and_then(Value::as_array).into_iter().flatten() {
                            next.push((child.clone(), uri.clone()));
                        }
                        nodes.push(n);
                    }
                }
                level = next;
            }
        }
    }
    if !structured {
        // The anchor and its parent chain through the ladder.
        let anchor_node = node_from(ladder.resolve(req, anchor, Mode::Full).await?, anchor, None);
        let mut chain: Vec<LensNode> = Vec::new();
        let mut next = match &anchor_node {
            LensNode::Post { parent_uri, .. } => parent_uri.clone(),
            LensNode::Gap { .. } => None,
        };
        while let Some(p) = next.take() {
            if chain.len() >= usize::try_from(parent_height).unwrap_or(0) {
                break;
            }
            let node = node_from(ladder.resolve(req, &p, Mode::Full).await?, &p, None);
            next = match &node {
                LensNode::Post { parent_uri, .. } => parent_uri.clone(),
                LensNode::Gap { .. } => None,
            };
            chain.push(node);
        }
        chain.reverse();
        nodes.extend(chain);
        nodes.push(anchor_node);
    }
    append_replies(ladder, req, anchor, depth, &mut nodes).await?;
    nodes.truncate(NODE_CAP);
    Ok(nodes)
}

/// Local and Constellation replies absent from `nodes`, breadth-first from
/// the anchor to `depth`, each through the ladder, within the cap.
async fn append_replies(ladder: &Ladder, req: &Requester, anchor: &str, depth: u32, nodes: &mut Vec<LensNode>) -> Result<(), sqlx::Error> {
    let mut seen: HashSet<String> = nodes.iter().map(|n| n.uri().to_string()).collect();
    let anchor_is_post = nodes.iter().any(|n| matches!(n, LensNode::Post { uri, .. } if uri == anchor));
    let mut level: Vec<String> = if anchor_is_post { vec![anchor.to_string()] } else { Vec::new() };
    for _ in 0..depth {
        if level.is_empty() || nodes.len() >= NODE_CAP {
            break;
        }
        let mut next: Vec<String> = Vec::new();
        for parent in &level {
            // Children the structure already holds continue the walk.
            next.extend(nodes.iter().filter_map(|n| match n {
                LensNode::Post { uri, parent_uri: Some(p), .. } if p == parent => Some(uri.clone()),
                _ => None,
            }));
            let fresh: Vec<String> = ladder
                .hydrator()
                .lens_replies(req, parent, REPLIES_PER_NODE)
                .await?
                .into_iter()
                .filter(|u| seen.insert(u.clone()))
                .take(NODE_CAP.saturating_sub(nodes.len()))
                .collect();
            let resolved: Vec<(String, Result<Resolved, sqlx::Error>)> = futures::stream::iter(fresh)
                .map(|u| async move {
                    let r = ladder.resolve(req, &u, Mode::Full).await;
                    (u, r)
                })
                .buffered(LADDER_CONCURRENCY)
                .collect()
                .await;
            for (uri, r) in resolved {
                let node = node_from(r?, &uri, Some(parent.clone()));
                if matches!(node, LensNode::Post { .. }) {
                    next.push(uri);
                }
                nodes.push(node);
            }
            if nodes.len() >= NODE_CAP {
                break;
            }
        }
        level = next;
    }
    Ok(())
}
