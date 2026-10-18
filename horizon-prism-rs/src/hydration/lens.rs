//! Thread Lens hydration (design-v3 §5.15; proposed locked decision 38).
//!
//! Thread Lens is for signed-in callers (ready members only, or every
//! signed-in account, by `threadlens.access`) and its responses are for the
//! caller:
//! - post views carry the caller's viewer state, as ordinary member views
//!   do. For a caller that is not a ready member (a visitor) that is
//!   `blockedBy` from the request's verdicts and nothing else;
//! - the blocks the request knows (its verdicts) are supplied to every
//!   hydration step through [`super::with_known_blocks`], so local and
//!   upstream views agree with the gaps the response shows;
//! - a node can carry continuation markers: a statement about work the
//!   response did not do, never about a block.
//!
//! Responses are sealed [`PublicEmit`]s built here (§7.2).

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::normalize::NormalizeReport;
use super::{CountMode, HydrationError, Hydrator, PublicEmit, Viewer};
use crate::db::locks::uri_author;
use crate::moderation::Gate;
use crate::threadlens::gaps::Gap;
use crate::xrpc::pools::Requester;

/// Where a response stopped short of a node's surroundings (§5.15.14).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Markers {
    /// The node has a parent the response does not include, because the
    /// walk stopped for `parentHeight`, the page budget or an allowance.
    pub more_parents: bool,
    /// The node's replies were not read to their end. The inner value is
    /// the position to resume from, absent when none was read.
    pub more_replies: Option<Option<String>>,
}

impl Markers {
    #[must_use]
    pub fn any(&self) -> bool {
        self.more_parents || self.more_replies.is_some()
    }
}

/// One `lensNode` (§5.15.15).
#[derive(Debug, Clone, PartialEq)]
pub enum LensNode {
    Post {
        uri: String,
        parent_uri: Option<String>,
        root_uri: Option<String>,
        post: Value,
        markers: Markers,
    },
    Gap {
        uri: String,
        parent_uri: Option<String>,
        gap: Gap,
        markers: Markers,
    },
}

impl LensNode {
    #[must_use]
    pub fn uri(&self) -> &str {
        match self {
            Self::Post { uri, .. } | Self::Gap { uri, .. } => uri,
        }
    }

    #[must_use]
    pub fn parent_uri(&self) -> Option<&str> {
        match self {
            Self::Post { parent_uri, .. } | Self::Gap { parent_uri, .. } => parent_uri.as_deref(),
        }
    }

    #[must_use]
    pub fn markers(&self) -> &Markers {
        match self {
            Self::Post { markers, .. } | Self::Gap { markers, .. } => markers,
        }
    }

    pub fn markers_mut(&mut self) -> &mut Markers {
        match self {
            Self::Post { markers, .. } | Self::Gap { markers, .. } => markers,
        }
    }

    /// A post node, taking the parent and root from the record's reply ref.
    #[must_use]
    pub fn post(post: Value) -> Self {
        let get = |p: &str| post.pointer(p).and_then(Value::as_str).map(str::to_string);
        Self::Post {
            uri: get("/uri").unwrap_or_default(),
            parent_uri: get("/record/reply/parent/uri"),
            root_uri: get("/record/reply/root/uri"),
            post,
            markers: Markers::default(),
        }
    }

    /// A gap with no markers.
    #[must_use]
    pub fn gap(uri: &str, parent_uri: Option<String>, gap: Gap) -> Self {
        Self::Gap {
            uri: uri.to_string(),
            parent_uri,
            gap,
            markers: Markers::default(),
        }
    }

    /// A post node, or a gap for a block between the viewer and its author:
    /// the walks go through either.
    #[must_use]
    pub fn walkable(&self) -> bool {
        match self {
            Self::Post { .. } => true,
            Self::Gap { gap, .. } => gap.known_block(),
        }
    }

    /// Whether the node's replies are known to be none: a post view whose
    /// `replyCount` is present and zero. A view with no count (one built
    /// from a bare record) says nothing, and counts as having some.
    #[must_use]
    pub fn no_replies(&self) -> bool {
        match self {
            Self::Post { post, .. } => post.get("replyCount").and_then(Value::as_u64) == Some(0),
            Self::Gap { .. } => false,
        }
    }

    fn to_json(&self) -> Value {
        let mut v = match self {
            Self::Post { uri, root_uri, post, .. } => {
                let mut v = json!({"$type": "app.nearhorizon.threadLens.defs#postNode", "uri": uri, "post": post});
                if let Some(r) = root_uri {
                    v["rootUri"] = json!(r);
                }
                v
            }
            Self::Gap { uri, gap, .. } => json!({"$type": "app.nearhorizon.threadLens.defs#gapNode", "uri": uri, "gap": gap.label()}),
        };
        if let Some(p) = self.parent_uri() {
            v["parentUri"] = json!(p);
        }
        // Only a post node or a known-block gap carries a marker.
        if self.walkable() {
            let markers = self.markers();
            if markers.more_parents {
                v["moreParents"] = json!(true);
            }
            if let Some(cursor) = &markers.more_replies {
                v["moreReplies"] = match cursor {
                    Some(c) => json!({ "cursor": c }),
                    None => json!({}),
                };
            }
        }
        v
    }
}

/// The state of bridging for one request (§5.15.16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bridging {
    /// Off: the response is the ordinary view.
    Off,
    /// On, and at least one candidate was held back because the server
    /// could not check the viewer's blocks for its author.
    Limited,
    /// On, and nothing was held back for that reason.
    Full,
}

impl Bridging {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Limited => "limited",
            Self::Full => "full",
        }
    }
}

/// `threadLens.getThread` output. `truncated` says that some node carries
/// a continuation marker.
#[must_use]
pub fn lens_thread(anchor: &str, nodes: &[LensNode], bridging: Bridging) -> PublicEmit<Value> {
    let truncated = nodes.iter().any(|n| n.walkable() && n.markers().any());
    PublicEmit::new(json!({
        "anchor": anchor,
        "nodes": nodes.iter().map(LensNode::to_json).collect::<Vec<_>>(),
        "truncated": truncated,
        "bridging": bridging.label(),
    }))
}

/// `threadLens.resolve` output. Items carry no markers.
#[must_use]
pub fn lens_items(nodes: &[LensNode], bridging: Bridging) -> PublicEmit<Value> {
    let items: Vec<Value> = nodes
        .iter()
        .map(|n| {
            let mut v = n.to_json();
            if let Some(m) = v.as_object_mut() {
                m.remove("moreParents");
                m.remove("moreReplies");
            }
            v
        })
        .collect();
    PublicEmit::new(json!({ "items": items, "bridging": bridging.label() }))
}

/// Every account a view names: authors, creators and the authorities of
/// the at-uris in it (reply parents and roots, embedded records). These
/// are the authors a baseline round judges (§5.15.6).
#[must_use]
pub fn named_accounts(view: &Value) -> HashSet<String> {
    fn go(v: &Value, key: Option<&str>, out: &mut HashSet<String>) {
        match v {
            Value::Array(items) => items.iter().for_each(|item| go(item, None, out)),
            Value::Object(m) => m.iter().for_each(|(k, child)| go(child, Some(k), out)),
            Value::String(s) => {
                if key == Some("did") && s.starts_with("did:") {
                    out.insert(s.clone());
                } else if key == Some("uri")
                    && s.starts_with("at://")
                    && let Some(author) = uri_author(s)
                    && author.starts_with("did:")
                {
                    out.insert(author.to_string());
                }
            }
            _ => {}
        }
    }
    let mut out = HashSet::new();
    go(view, None, &mut out);
    out
}

impl Hydrator {
    /// The §7.10 gate for a lens URI: `Hidden` and `Collapsed` (`!hide`) are
    /// both not shown, and no source is consulted. The author's status is
    /// read only if it is in S or a member (§11 G-9).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_visible(&self, req: &Requester, uri: &str) -> Result<bool, sqlx::Error> {
        Ok(self.gate(req, uri).await? == Gate::Visible)
    }

    /// The baseline (§5.15.7): the tree the ordinary thread view returns
    /// for this viewer and anchor, or `None` where that view finds nothing.
    /// Run inside [`super::with_known_blocks`], it treats the request's
    /// blocks as it treats any other of the viewer's.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_baseline(&self, viewer: &Viewer, req: &Requester, anchor: &str, depth: u32, parent_height: u32) -> Result<Option<Value>, HydrationError> {
        Ok(self.thread_tree(viewer, req, anchor, depth, parent_height, false).await?.ok().map(|(tree, _)| tree))
    }

    /// Local post views for `uris`, hydrated for the viewer.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_local_posts(&self, viewer: &Viewer, req: &Requester, uris: &[String]) -> Result<HashMap<String, Value>, sqlx::Error> {
        let ctx = self.post_context_for(Some(viewer), req, uris, CountMode::List, &[], &[]).await?;
        Ok(uris
            .iter()
            .filter(|u| ctx.gates.get(*u).copied().unwrap_or(Gate::Visible) == Gate::Visible)
            .filter_map(|u| ctx.post_view(u).map(|v| (u.clone(), v)))
            .collect())
    }

    /// Normalize an upstream view for the viewer (§II-8.6): local viewer
    /// state and labels replace the upstream's.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_normalize(&self, viewer: &Viewer, req: &Requester, view: Value) -> Result<(Value, NormalizeReport), sqlx::Error> {
        let (mut view, report) = self.normalized(viewer, req, view).await?;
        self.enrich_counts(req, &mut view).await?;
        Ok((view, report))
    }

    /// Whether `record_tombstones` holds the URI (§10.4 `deleted`).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn tombstoned(&self, uri: &str) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM record_tombstones WHERE uri = $1)")
            .bind(uri)
            .fetch_one(self.pool())
            .await
    }

    /// Whether the author's account is locally known to be `active`.
    /// "Locally known" needs the author in scope or a member: the row of any
    /// other DID (a private owner, the author of a private reply) is
    /// treated as absent (design-v3 §11 G-9, §II-10.4; R3-142).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn account_known_active(&self, did: &str) -> Result<bool, sqlx::Error> {
        if !self.in_scope_or_member(did).await {
            return Ok(false);
        }
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM actors WHERE did = $1 AND account_status = 'active')")
            .bind(did)
            .fetch_one(self.pool())
            .await
    }

    /// Of `uris`, the posts stored here, each with the parent its row
    /// names. A node "built from a local row" is one of these.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_local_rows(&self, uris: &[String]) -> Result<HashMap<String, Option<String>>, sqlx::Error> {
        let rows: Vec<(String, Option<String>)> = sqlx::query_as("SELECT uri, reply_parent_uri FROM posts WHERE uri = ANY($1)")
            .bind(uris)
            .fetch_all(self.pool())
            .await?;
        Ok(rows.into_iter().collect())
    }

    /// The local phase of a post's reply sequence (§5.15.14): the stored
    /// replies to `uri` after the position `after`, in `(created_at, uri)`
    /// order, at most `limit`, each with its `created_at`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_local_replies(&self, uri: &str, after: Option<(DateTime<Utc>, &str)>, limit: u32) -> Result<Vec<(String, DateTime<Utc>)>, sqlx::Error> {
        sqlx::query_as(
            "SELECT uri, created_at FROM posts WHERE reply_parent_uri = $1 \
               AND ($2::timestamptz IS NULL OR (created_at, uri) > ($2, $3)) \
             ORDER BY created_at, uri LIMIT $4",
        )
        .bind(uri)
        .bind(after.map(|(at, _)| at))
        .bind(after.map_or("", |(_, uri)| uri))
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await
    }

    /// The Constellation phase of a post's reply sequence: one page of
    /// the replies its listing names from `cursor`, at most `limit`, and
    /// the listing's next cursor. `None` when the listing gave no answer
    /// (no budget, or Constellation did not respond). With no
    /// Constellation configured there is no such phase: an empty page.
    pub async fn lens_listed_replies(&self, req: &Requester, uri: &str, limit: u32, cursor: Option<&str>) -> Option<(Vec<String>, Option<String>)> {
        if self.backlinks().is_none() {
            return Some((Vec::new(), None));
        }
        let page = self.links_page(req, uri, super::feed::NSID_POST, ".reply.parent.uri", limit, cursor).await.ok()??;
        let replies = page.linking_records.into_iter().map(|r| format!("at://{}/{}/{}", r.did, r.collection, r.rkey)).collect();
        Some((replies, page.cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_shapes() {
        let post = json!({"uri": "at://did:plc:a/app.bsky.feed.post/2", "cid": "c", "author": {"did": "did:plc:a", "handle": "a", "viewer": {"blockedBy": false}},
                          "record": {"text": "r", "reply": {"root": {"uri": "at://did:plc:a/app.bsky.feed.post/0"}, "parent": {"uri": "at://did:plc:a/app.bsky.feed.post/1"}}}});
        let mut first = LensNode::post(post);
        first.markers_mut().more_parents = true;
        first.markers_mut().more_replies = Some(Some("sealed".into()));
        let mut hidden = LensNode::gap("at://did:plc:a/tools.kryphocron.feed.postPrivate/3", Some("at://did:plc:a/app.bsky.feed.post/2".into()), Gap::TierHidden);
        // A marker on a gap that is not a known block is never emitted.
        hidden.markers_mut().more_replies = Some(None);
        let mut blocked = LensNode::gap("at://did:plc:b/app.bsky.feed.post/4", None, Gap::BlockedByYou);
        blocked.markers_mut().more_replies = Some(None);
        let nodes = [first, hidden, blocked];
        let out = lens_thread("at://did:plc:a/app.bsky.feed.post/2", &nodes, Bridging::Limited);
        let v = out.inner();
        assert_eq!(v["nodes"][0]["$type"], "app.nearhorizon.threadLens.defs#postNode");
        assert_eq!(v["nodes"][0]["parentUri"], "at://did:plc:a/app.bsky.feed.post/1");
        assert_eq!(v["nodes"][0]["rootUri"], "at://did:plc:a/app.bsky.feed.post/0");
        // Viewer state is the caller's, kept as hydrated.
        assert_eq!(v["nodes"][0]["post"]["author"]["viewer"], json!({"blockedBy": false}));
        assert_eq!((&v["nodes"][0]["moreParents"], &v["nodes"][0]["moreReplies"]), (&json!(true), &json!({"cursor": "sealed"})));
        assert_eq!(v["nodes"][1], json!({"$type": "app.nearhorizon.threadLens.defs#gapNode", "uri": "at://did:plc:a/tools.kryphocron.feed.postPrivate/3",
                                         "parentUri": "at://did:plc:a/app.bsky.feed.post/2", "gap": "tier-hidden"}));
        assert_eq!((&v["nodes"][2]["gap"], &v["nodes"][2]["moreReplies"]), (&json!("blocked-by-you"), &json!({})));
        assert_eq!((&v["truncated"], &v["bridging"]), (&json!(true), &json!("limited")));
        let items = lens_items(&nodes, Bridging::Off);
        assert!(items.inner()["items"][0].get("moreReplies").is_none());
        assert_eq!(items.inner()["bridging"], "off");
    }

    #[test]
    fn the_accounts_a_view_names() {
        let view = json!({"post": {"uri": "at://did:plc:a/app.bsky.feed.post/2", "author": {"did": "did:plc:a"},
            "record": {"reply": {"parent": {"uri": "at://did:plc:b/app.bsky.feed.post/1"}}},
            "embed": {"record": {"uri": "at://did:plc:c/app.bsky.feed.post/9", "author": {"did": "did:plc:c"}, "creator": {"did": "did:plc:d"}}}},
            "replies": [{"uri": "at://did:plc:e/app.bsky.feed.post/5", "blocked": true}], "text": "did:plc:zzz"});
        let mut got: Vec<String> = named_accounts(&view).into_iter().collect();
        got.sort();
        assert_eq!(got, ["did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d", "did:plc:e"]);
    }
}
