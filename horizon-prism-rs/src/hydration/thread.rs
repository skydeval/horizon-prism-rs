//! `app.bsky.feed.getPostThread` (design-v2 §7.1, §7.3), public tier only.
//!
//! The local tree follows `posts_parent` / `posts_root`: the parent chain up
//! to `parentHeight`, replies to `depth`, 200 nodes in total. A private-tier,
//! hidden or collapsed anchor is `400 NotFound "Post not found"`, the same as
//! a nonexistent post. The Constellation reply union and non-local nodes need
//! Slingshot (item 7); until then only local replies are shown.

use std::collections::HashMap;
use std::time::Instant;

use serde_json::{Value, json};

use super::feed::PostContext;
use super::normalize::is_private_uri;
use super::{CountMode, Hydrator, ViewError, ViewResult, Viewer};
use crate::db::locks::uri_author;
use crate::moderation::Gate;

/// Total nodes in one thread response (§7.1).
pub const MAX_NODES: usize = 200;
pub const DEFAULT_DEPTH: u32 = 6;
pub const DEFAULT_PARENT_HEIGHT: u32 = 80;

fn not_found() -> ViewError {
    ViewError::new("NotFound", "Post not found")
}

fn not_found_post(uri: &str) -> Value {
    json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri, "notFound": true})
}

impl PostContext {
    fn listable(&self, uri: &str) -> bool {
        self.gates.get(uri).copied().unwrap_or(Gate::Visible).listable()
    }

    fn blocked_post(&self, uri: &str) -> Option<Value> {
        let author = uri_author(uri).unwrap_or_default();
        self.profiles.viewer.blocked_either_way(author).then(|| {
            json!({
                "$type": "app.bsky.feed.defs#blockedPost", "uri": uri, "blocked": true,
                "author": {"did": author, "viewer": self.profiles.viewer.view(author)}
            })
        })
    }

    fn parent_node(&self, uri: &str, remaining: u32) -> Value {
        if let Some(b) = self.blocked_post(uri) {
            return b;
        }
        let Some(post) = self.post_view(uri).filter(|_| self.listable(uri)) else {
            return not_found_post(uri);
        };
        let mut node = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post});
        if remaining > 0
            && let Some(parent) = self.posts.get(uri).and_then(|r| r.reply_parent_uri.clone())
        {
            node["parent"] = self.parent_node(&parent, remaining - 1);
        }
        node
    }

    fn reply_nodes(&self, uri: &str, children: &HashMap<String, Vec<String>>) -> Vec<Value> {
        children
            .get(uri)
            .map(|kids| {
                kids.iter()
                    .filter(|k| self.listable(k) && self.blocked_post(k).is_none())
                    .filter_map(|k| {
                        let post = self.post_view(k)?;
                        let mut node = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post});
                        let replies = self.reply_nodes(k, children);
                        if children.contains_key(k.as_str()) {
                            node["replies"] = Value::Array(replies);
                        }
                        Some(node)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Hydrator {
    /// `app.bsky.feed.getPostThread`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_post_thread(&self, viewer: &Viewer, uri: &str, depth: u32, parent_height: u32) -> ViewResult<Value> {
        let started = Instant::now();
        if is_private_uri(uri) {
            return Ok(Err(not_found()));
        }
        let depth = depth.min(1000);
        let parent_height = parent_height.min(1000);
        let anchor = self.load_posts(&[uri.to_string()]).await?;
        let Some(anchor_row) = anchor.get(uri) else {
            let author = uri_author(uri).unwrap_or_default().to_string();
            if !self.in_scope(&author).await
                && let Some(v) = self.upstream().post_thread(uri, depth, parent_height).await
            {
                return Ok(Ok(Self::emit(v)));
            }
            return Ok(Err(not_found()));
        };

        // Parent chain.
        let mut uris: Vec<String> = vec![uri.to_string()];
        let mut next = anchor_row.reply_parent_uri.clone();
        let mut height = 0;
        while let Some(p) = next.take() {
            if height >= parent_height || uris.contains(&p) || is_private_uri(&p) {
                break;
            }
            height += 1;
            uris.push(p.clone());
            next = self.load_posts(std::slice::from_ref(&p)).await?.get(&p).and_then(|r| r.reply_parent_uri.clone());
        }

        // Replies, breadth-first, within the node budget.
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        let mut frontier = vec![uri.to_string()];
        for _ in 0..depth {
            let budget = MAX_NODES.saturating_sub(uris.len());
            if frontier.is_empty() || budget == 0 {
                break;
            }
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT uri, reply_parent_uri FROM posts WHERE reply_parent_uri = ANY($1) \
                 ORDER BY created_at, uri LIMIT $2",
            )
            .bind(&frontier)
            .bind(i64::try_from(budget).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await?;
            frontier = Vec::new();
            for (child, parent) in rows {
                if uris.contains(&child) {
                    continue;
                }
                children.entry(parent).or_default().push(child.clone());
                uris.push(child.clone());
                frontier.push(child);
            }
        }

        let mut ctx = self.post_context(viewer, &uris, CountMode::List).await?;
        let anchor_counts = self
            .constellation_counts(std::slice::from_ref(&uri.to_string()), CountMode::Sync { started })
            .await;
        ctx.counts.extend(anchor_counts);
        if !ctx.listable(uri) {
            return Ok(Err(not_found()));
        }
        if let Some(blocked) = ctx.blocked_post(uri) {
            return Ok(Ok(Self::emit(json!({ "thread": blocked }))));
        }
        let Some(post) = ctx.post_view(uri) else {
            return Ok(Err(not_found()));
        };
        let mut thread = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post});
        if parent_height > 0
            && let Some(parent) = &anchor_row.reply_parent_uri
        {
            thread["parent"] = ctx.parent_node(parent, parent_height - 1);
        }
        thread["replies"] = Value::Array(ctx.reply_nodes(uri, &children));
        Ok(Ok(Self::emit(json!({ "thread": thread }))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    #[tokio::test]
    async fn thread_tree_parents_replies_and_misses() {
        let Some(db) = fresh_db("hydration_thread").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        sets.scope.insert(BOB.into());
        let h = hydrator(p, sets);
        let root = post(p, ALICE, "3kaaaaaaaaaa2", "root", None, None).await;
        let mid = post(p, BOB, "3kaaaaaaaaaa3", "mid", Some(&root), Some(&root)).await;
        let leaf = post(p, ALICE, "3kaaaaaaaaaa4", "leaf", Some(&mid), Some(&root)).await;
        let sibling = post(p, ALICE, "3kaaaaaaaaaa5", "sibling", Some(&mid), Some(&root)).await;
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{sibling}', '!hide')")).await;
        let anon = Viewer::anonymous();

        let t = h.get_post_thread(&anon, &mid, 6, 80).await.expect("db").expect("view");
        let t = &t.inner()["thread"];
        assert_eq!(t["post"]["uri"], json!(mid));
        assert_eq!(t["parent"]["post"]["uri"], json!(root));
        let replies = t["replies"].as_array().expect("replies");
        assert_eq!(replies.len(), 1, "the !hide sibling is omitted from threads");
        assert_eq!(replies[0]["post"]["uri"], json!(leaf));

        // parentHeight 0 drops the parent; depth 0 drops replies.
        let t = h.get_post_thread(&anon, &mid, 0, 0).await.expect("db").expect("view");
        assert!(t.inner()["thread"].get("parent").is_none());
        assert_eq!(t.inner()["thread"]["replies"], json!([]));

        // Misses: private, unknown and collapsed anchors are all NotFound.
        for uri in [
            format!("at://{ALICE}/tools.kryphocron.feed.postPrivate/3kaaaaaaaaaa6"),
            format!("at://{ALICE}/app.bsky.feed.post/3kaaaaaaaaaz9"),
            sibling.clone(),
        ] {
            let e = h.get_post_thread(&anon, &uri, 6, 80).await.expect("db").expect_err("miss");
            assert_eq!((e.error, e.message.as_str()), ("NotFound", "Post not found"), "{uri}");
        }
        db.drop_db().await;
    }
}
