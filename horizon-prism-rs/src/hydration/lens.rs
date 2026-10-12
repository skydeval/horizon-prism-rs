//! Thread Lens hydration (design-v2 §10), block-party-clean (§10.3):
//! - Local post views are built with **no** viewer computation at all
//!   ([`Hydrator::post_context_for`] with `None`: never `actor_viewer`, never
//!   `post_viewer`, never a block table).
//! - Upstream views are normalized with a labels-only overlay.
//! - Every `viewer` object in a lens response is then forced to `{}`
//!   ([`anonymize`]), so no `viewer.blocking`/`blockedBy` can appear.
//! - Responses are sealed [`PublicEmit`]s built here (§7.2).

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use super::normalize::{NormalizeReport, Overlay, normalize_upstream, subjects};
use super::{CountMode, Hydrator, PublicEmit};
use crate::moderation::{self, Gate};
use crate::threadlens::gaps::Gap;
use crate::xrpc::pools::Requester;

/// One `lensNode` (§10.2).
#[derive(Debug, Clone, PartialEq)]
pub enum LensNode {
    Post {
        uri: String,
        parent_uri: Option<String>,
        root_uri: Option<String>,
        post: Value,
    },
    Gap {
        uri: String,
        parent_uri: Option<String>,
        gap: Gap,
    },
}

impl LensNode {
    #[must_use]
    pub fn uri(&self) -> &str {
        match self {
            Self::Post { uri, .. } | Self::Gap { uri, .. } => uri,
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
        }
    }

    fn to_json(&self) -> Value {
        match self {
            Self::Post { uri, parent_uri, root_uri, post } => {
                let mut post = post.clone();
                anonymize(&mut post);
                let mut v = json!({"$type": "app.nearhorizon.threadLens.defs#postNode", "uri": uri, "post": post});
                if let Some(p) = parent_uri {
                    v["parentUri"] = json!(p);
                }
                if let Some(r) = root_uri {
                    v["rootUri"] = json!(r);
                }
                v
            }
            Self::Gap { uri, parent_uri, gap } => {
                let mut v = json!({"$type": "app.nearhorizon.threadLens.defs#gapNode", "uri": uri, "gap": gap.label()});
                if let Some(p) = parent_uri {
                    v["parentUri"] = json!(p);
                }
                v
            }
        }
    }
}

/// Force every `viewer` object to `{}` (§10.3).
pub fn anonymize(v: &mut Value) {
    match v {
        Value::Array(items) => items.iter_mut().for_each(anonymize),
        Value::Object(m) => {
            for (k, child) in m.iter_mut() {
                if k == "viewer" {
                    *child = Value::Object(Map::new());
                } else {
                    anonymize(child);
                }
            }
        }
        _ => {}
    }
}

/// `threadLens.getThread` output.
#[must_use]
pub fn lens_thread(anchor: &str, nodes: &[LensNode]) -> PublicEmit<Value> {
    PublicEmit::new(json!({"anchor": anchor, "nodes": nodes.iter().map(LensNode::to_json).collect::<Vec<_>>()}))
}

/// `threadLens.resolve` output.
#[must_use]
pub fn lens_items(nodes: &[LensNode]) -> PublicEmit<Value> {
    PublicEmit::new(json!({"items": nodes.iter().map(LensNode::to_json).collect::<Vec<_>>()}))
}

impl Hydrator {
    /// The §7.10 gate for a lens URI: `Hidden` and `Collapsed` (`!hide`) are
    /// both not shown, and no source is consulted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_visible(&self, uri: &str) -> Result<bool, sqlx::Error> {
        Ok(moderation::gate(self.pool(), uri).await? == Gate::Visible)
    }

    /// Local post views for `uris`, hydrated with no viewer.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_local_posts(&self, req: &Requester, uris: &[String]) -> Result<HashMap<String, Value>, sqlx::Error> {
        let ctx = self.post_context_for(None, req, uris, CountMode::List).await?;
        Ok(uris
            .iter()
            .filter(|u| ctx.gates.get(*u).copied().unwrap_or(Gate::Visible) == Gate::Visible)
            .filter_map(|u| ctx.post_view(u).map(|mut v| {
                anonymize(&mut v);
                (u.clone(), v)
            }))
            .collect())
    }

    /// Normalize an upstream view for Thread Lens: local labels only, no
    /// viewer state (§8.6, §10.3).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_normalize(&self, mut view: Value) -> Result<(Value, NormalizeReport), sqlx::Error> {
        let (dids, uris) = subjects(&view);
        let labelled: Vec<String> = dids.into_iter().chain(uris).collect();
        let overlay = Overlay {
            labels: self.load_labels(&labelled).await?,
            ..Overlay::default()
        };
        let report = normalize_upstream(&mut view, &overlay);
        self.enrich_counts(&mut view).await?;
        anonymize(&mut view);
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
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn account_known_active(&self, did: &str) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM actors WHERE did = $1 AND account_status = 'active')")
            .bind(did)
            .fetch_one(self.pool())
            .await
    }

    /// Reply URIs to `uri`: local `posts_parent` replies, then Constellation
    /// `/links` replies not already listed (§10.4), at most `limit`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn lens_replies(&self, req: &Requester, uri: &str, limit: u32) -> Result<Vec<String>, sqlx::Error> {
        let mut out: Vec<String> =
            sqlx::query_scalar("SELECT uri FROM posts WHERE reply_parent_uri = $1 ORDER BY created_at, uri LIMIT $2")
                .bind(uri)
                .bind(i64::from(limit))
                .fetch_all(self.pool())
                .await?;
        if let Some(page) = self.links_page(req, uri, super::feed::NSID_POST, ".reply.parent.uri", limit, None).await {
            for r in page.linking_records {
                let reply = format!("at://{}/{}/{}", r.did, r.collection, r.rkey);
                if !out.contains(&reply) {
                    out.push(reply);
                }
            }
        }
        out.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymize_empties_every_viewer() {
        let mut v = json!({"post": {"viewer": {"like": "x"}, "author": {"viewer": {"blocking": "at://b", "blockedBy": true}},
                           "embed": {"record": {"author": {"viewer": {"muted": true}}}}}});
        anonymize(&mut v);
        assert_eq!(v.to_string().matches("\"viewer\":{}").count(), 3);
        assert!(!v.to_string().contains("blocking") && !v.to_string().contains("blockedBy"));
    }

    #[test]
    fn node_shapes() {
        let post = json!({"uri": "at://did:plc:a/app.bsky.feed.post/2", "cid": "c", "author": {"did": "did:plc:a", "handle": "a", "viewer": {"blockedBy": false}},
                          "record": {"text": "r", "reply": {"root": {"uri": "at://did:plc:a/app.bsky.feed.post/0"}, "parent": {"uri": "at://did:plc:a/app.bsky.feed.post/1"}}}});
        let nodes = [
            LensNode::post(post),
            LensNode::Gap {
                uri: "at://did:plc:a/tools.kryphocron.feed.postPrivate/3".into(),
                parent_uri: Some("at://did:plc:a/app.bsky.feed.post/2".into()),
                gap: Gap::TierHidden,
            },
        ];
        let out = lens_thread("at://did:plc:a/app.bsky.feed.post/2", &nodes);
        let v = out.inner();
        assert_eq!(v["nodes"][0]["$type"], "app.nearhorizon.threadLens.defs#postNode");
        assert_eq!(v["nodes"][0]["parentUri"], "at://did:plc:a/app.bsky.feed.post/1");
        assert_eq!(v["nodes"][0]["rootUri"], "at://did:plc:a/app.bsky.feed.post/0");
        assert_eq!(v["nodes"][0]["post"]["author"]["viewer"], json!({}));
        assert_eq!(v["nodes"][1], json!({"$type": "app.nearhorizon.threadLens.defs#gapNode", "uri": "at://did:plc:a/tools.kryphocron.feed.postPrivate/3",
                                         "parentUri": "at://did:plc:a/app.bsky.feed.post/2", "gap": "tier-hidden"}));
        assert_eq!(lens_items(&nodes[1..]).inner()["items"][0]["gap"], "tier-hidden");
    }
}
