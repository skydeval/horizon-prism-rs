//! `app.bsky.feed.getPostThread` (design-v2 §7.1, §7.3), public tier only.
//!
//! The local tree follows `posts_parent` / `posts_root`: the parent chain up
//! to `parentHeight`, replies to `depth`, 200 nodes in total. A private-tier,
//! hidden or collapsed anchor is `400 NotFound "Post not found"`, the same as
//! a nonexistent post. A node's replies are the union of the local rows and
//! the replies Constellation lists for it; replies and parents not stored
//! here come from Slingshot (design-v3 §5.1, §11 G-6), within
//! `thread.max_remote_fetches`.

use std::collections::HashMap;
use std::time::Instant;

use futures::StreamExt;
use serde_json::{Value, json};

use super::feed::{NSID_POST, PostContext};
use super::normalize::is_private_uri;
use super::{CountMode, Hydrator, ViewError, ViewResult, Viewer};
use crate::db::locks::uri_author;
use crate::moderation::{self, Gate};
use crate::xrpc::pools::Requester;

/// Total nodes in one thread response (§7.1): the default of
/// `thread.max_nodes`.
pub const MAX_NODES: usize = 200;
/// Constellation reply listings in flight for one response.
pub const LISTING_CONCURRENCY: usize = 8;
pub const DEFAULT_DEPTH: u32 = 6;
pub const DEFAULT_PARENT_HEIGHT: u32 = 80;

fn not_found() -> ViewError {
    ViewError::new("NotFound", "Post not found")
}

fn not_found_post(uri: &str) -> Value {
    json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri, "notFound": true})
}

/// The posts of one thread that are not stored here: views built from
/// Slingshot records (design-v3 §5.1).
#[derive(Default)]
struct Remote {
    views: HashMap<String, Value>,
    /// A parent there was no budget to fetch. The chain ends below it, with
    /// no item for it: it is not known to be missing (§II-8.3).
    cut: Option<String>,
}

impl Remote {
    fn parent_of(&self, uri: &str) -> Option<String> {
        reply_parent(self.views.get(uri)?)
    }
}

/// The parent a post view's record replies to.
fn reply_parent(view: &Value) -> Option<String> {
    view.pointer("/record/reply/parent/uri").and_then(Value::as_str).map(str::to_string)
}

impl PostContext {
    fn listable(&self, uri: &str) -> bool {
        self.gates.get(uri).copied().unwrap_or(Gate::Visible).listable()
    }

    /// A node's post view: the local row, else the fetched record.
    fn node_post(&self, uri: &str, remote: &Remote) -> Option<Value> {
        self.post_view(uri).or_else(|| remote.views.get(uri).cloned())
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

    /// The node for parent `uri`, or `None` where the chain was cut.
    fn parent_node(&self, uri: &str, remaining: u32, remote: &Remote) -> Option<Value> {
        if remote.cut.as_deref() == Some(uri) {
            return None;
        }
        if let Some(b) = self.blocked_post(uri) {
            return Some(b);
        }
        let Some(post) = self.node_post(uri, remote).filter(|_| self.listable(uri)) else {
            return Some(not_found_post(uri));
        };
        let mut node = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post});
        let parent = self.posts.get(uri).and_then(|r| r.reply_parent_uri.clone()).or_else(|| remote.parent_of(uri));
        if remaining > 0
            && let Some(parent) = parent
            && let Some(above) = self.parent_node(&parent, remaining - 1, remote)
        {
            node["parent"] = above;
        }
        Some(node)
    }

    fn reply_nodes(&self, uri: &str, children: &HashMap<String, Vec<String>>, remote: &Remote) -> Vec<Value> {
        children
            .get(uri)
            .map(|kids| {
                kids.iter()
                    .filter(|k| self.listable(k) && self.blocked_post(k).is_none())
                    .filter_map(|k| {
                        let post = self.node_post(k, remote)?;
                        let mut node = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post});
                        let replies = self.reply_nodes(k, children, remote);
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
    pub async fn get_post_thread(&self, viewer: &Viewer, req: &Requester, uri: &str, depth: u32, parent_height: u32) -> ViewResult<Value> {
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
                && !moderation::gate(self.pool(), uri).await?.is_hidden()
                && let Some(v) = self.upstream().post_thread(req, uri, depth, parent_height).await
            {
                return Ok(Ok(Self::emit(self.normalized(viewer, v).await?.0)));
            }
            return Ok(Err(not_found()));
        };

        let cfg = &self.inner.cfg.thread;
        let max_nodes = usize::try_from(cfg.max_nodes).unwrap_or(MAX_NODES);
        let per_node = cfg.constellation_replies_per_node;
        let mut fetches_left = usize::try_from(cfg.max_remote_fetches).unwrap_or(usize::MAX);
        let mut remote = Remote::default();

        // Parent chain: local rows, then Slingshot for a parent not stored
        // here (design-v3 §5.1). It stops at the first miss, which becomes a
        // not-found item.
        let mut uris: Vec<String> = vec![uri.to_string()];
        let mut next = anchor_row.reply_parent_uri.clone();
        let mut height = 0;
        while let Some(p) = next.take() {
            if height >= parent_height || uris.contains(&p) || is_private_uri(&p) {
                break;
            }
            height += 1;
            if let Some(row) = self.load_posts(std::slice::from_ref(&p)).await?.get(&p) {
                next = row.reply_parent_uri.clone();
                uris.push(p);
                continue;
            }
            // An in-scope author's posts are all stored: this one is gone.
            if self.in_scope(uri_author(&p).unwrap_or_default()).await {
                uris.push(p);
                break;
            }
            if fetches_left == 0 {
                remote.cut = Some(p);
                break;
            }
            fetches_left -= 1;
            match self.remote_posts(viewer, req, std::slice::from_ref(&p)).await?.remove(&p) {
                Some(view) => {
                    next = reply_parent(&view);
                    remote.views.insert(p.clone(), view);
                    uris.push(p);
                }
                // No budget to fetch it: the chain ends here, with no
                // not-found item (§II-8.3).
                None if req.spent() => remote.cut = Some(p),
                None => uris.push(p),
            }
        }

        // Replies, breadth-first, within the node budget. Each node's
        // replies are the union of the local rows and the replies
        // Constellation lists for it (design-v3 §11 G-6).
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        let mut frontier = vec![uri.to_string()];
        for _ in 0..depth {
            let budget = max_nodes.saturating_sub(uris.len());
            if frontier.is_empty() || budget == 0 {
                break;
            }
            let parents = frontier.clone();
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

            // Replies Constellation lists and this AppView does not store,
            // fetched from Slingshot. One that cannot be fetched is left
            // out: a listing is not a claim that the record exists.
            let listed: Vec<(String, Vec<String>)> = futures::stream::iter(parents)
                .map(|parent| async move {
                    let page = self.links_page(req, &parent, NSID_POST, ".reply.parent.uri", per_node, None).await;
                    let kids = page
                        .ok()
                        .flatten()
                        .map(|p| p.linking_records.into_iter().map(|r| format!("at://{}/{}/{}", r.did, r.collection, r.rkey)).collect())
                        .unwrap_or_default();
                    (parent, kids)
                })
                .buffered(LISTING_CONCURRENCY)
                .collect()
                .await;
            let room = fetches_left.min(max_nodes.saturating_sub(uris.len()));
            let mut wanted: Vec<(String, String)> = Vec::new();
            for (parent, kids) in listed {
                for kid in kids {
                    if wanted.len() >= room {
                        break;
                    }
                    // A reply by an in-scope author would be a local row.
                    if uris.contains(&kid) || wanted.iter().any(|(_, k)| *k == kid) || self.in_scope(uri_author(&kid).unwrap_or_default()).await {
                        continue;
                    }
                    wanted.push((parent.clone(), kid));
                }
            }
            if wanted.is_empty() {
                continue;
            }
            fetches_left -= wanted.len();
            let asked: Vec<String> = wanted.iter().map(|(_, k)| k.clone()).collect();
            let mut views = self.remote_posts(viewer, req, &asked).await?;
            for (parent, kid) in wanted {
                // Keep it only if its record does reply to this parent.
                if let Some(view) = views.remove(&kid).filter(|v| reply_parent(v).as_deref() == Some(parent.as_str())) {
                    remote.views.insert(kid.clone(), view);
                    children.entry(parent).or_default().push(kid.clone());
                    uris.push(kid.clone());
                    frontier.push(kid);
                }
            }
        }

        let mut ctx = self.post_context(viewer, req, &uris, CountMode::List).await?;
        let anchor_counts = self
            .constellation_counts(req, std::slice::from_ref(&uri.to_string()), CountMode::Sync { started })
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
            && let Some(above) = ctx.parent_node(parent, parent_height - 1, &remote)
        {
            thread["parent"] = above;
        }
        thread["replies"] = Value::Array(ctx.reply_nodes(uri, &children, &remote));
        Ok(Ok(Self::emit(json!({ "thread": thread }))))
    }

    /// Post views for URIs not stored here, built from their Slingshot
    /// records and normalized: hidden posts are not fetched, and a record
    /// that cannot be fetched is absent.
    ///
    /// # Errors
    ///
    /// Database errors.
    async fn remote_posts(&self, viewer: &Viewer, req: &Requester, uris: &[String]) -> Result<HashMap<String, Value>, sqlx::Error> {
        let gates = moderation::gate_many(self.pool(), uris).await?;
        let allowed: Vec<String> = uris
            .iter()
            .filter(|u| !gates.get(*u).copied().unwrap_or(Gate::Visible).is_hidden())
            .cloned()
            .collect();
        let mut out = HashMap::new();
        if allowed.is_empty() {
            return Ok(out);
        }
        let fetched = Value::Array(self.upstream().post_records(req, &allowed).await);
        if let Value::Array(views) = self.normalized(viewer, fetched).await?.0 {
            for v in views {
                if let Some(u) = v.get("uri").and_then(Value::as_str).map(str::to_string) {
                    out.insert(u, v);
                }
            }
        }
        Ok(out)
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

    /// design-v3 §5.1, §11 G-6: replies and parents not stored here.
    #[tokio::test]
    async fn replies_and_parents_not_stored_here_come_from_slingshot() {
        use std::sync::Arc;

        use axum::extract::Query;
        use axum::routing::get;

        use crate::hydration::testing::{FixedUpstream, hydrator_full};

        let Some(db) = fresh_db("hydration_thread_remote").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        let outsider = "did:plc:oooooooooooooooooooooooo";
        let far = |rkey: &str| format!("at://{outsider}/app.bsky.feed.post/{rkey}");
        // outsider's `top` ← alice's `anchor` ← alice's `local` and
        // outsider's `reply` ← outsider's `deep`.
        let anchor = post(p, ALICE, "3kaaaaaaaaaa2", "anchor", Some(&far("top")), Some(&far("top"))).await;
        let local = post(p, ALICE, "3kaaaaaaaaaa3", "local", Some(&anchor), Some(&far("top"))).await;

        let view = |rkey: &str, parent: Option<&str>| {
            let mut record = json!({"$type": "app.bsky.feed.post", "text": rkey, "createdAt": "2026-09-26T00:00:00Z"});
            if let Some(parent) = parent {
                record["reply"] = json!({"parent": {"uri": parent, "cid": "c"}, "root": {"uri": far("top"), "cid": "c"}});
            }
            json!({"$type": "app.bsky.feed.defs#postView", "uri": far(rkey), "cid": "bafy",
                   "author": {"did": outsider, "handle": "far.example", "viewer": {}, "labels": []},
                   "record": record, "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "labels": []})
        };
        let mut up = FixedUpstream::default();
        up.posts.insert(far("top"), view("top", None));
        up.posts.insert(far("reply"), view("reply", Some(&anchor)));
        up.posts.insert(far("deep"), view("deep", Some(&far("reply"))));
        // Listed under the anchor, but its record replies to something else.
        up.posts.insert(far("stray"), view("stray", Some(&far("elsewhere"))));

        // Constellation: replies listed per target.
        let listing: HashMap<String, Vec<&'static str>> = HashMap::from([
            (anchor.clone(), vec!["reply", "stray", "gone"]),
            (far("reply"), vec!["deep"]),
        ]);
        let router = axum::Router::new().route(
            "/links",
            get(move |Query(q): Query<HashMap<String, String>>| {
                let kids: Vec<Value> = listing
                    .get(&q["target"])
                    .into_iter()
                    .flatten()
                    .map(|rkey| json!({"did": outsider, "collection": "app.bsky.feed.post", "rkey": rkey}))
                    .collect();
                async move { axum::Json(json!({"total": 0, "linking_records": kids})) }
            }),
        );
        let base = crate::test_support::serve_fake(router).await;
        let h = hydrator_full(p, sets, Arc::new(up), Some(&base));

        let req = Requester::test();
        let t = h.get_post_thread(&Viewer::anonymous(), &req, &anchor, 6, 80).await.expect("db").expect("view");
        let t = &t.inner()["thread"];
        // The parent not stored here is fetched.
        assert_eq!(t["parent"]["post"]["uri"], json!(far("top")));
        assert_eq!(t["parent"]["post"]["record"]["text"], "top");
        // Local replies first, then the listed one; `stray` and `gone` are
        // left out, not shown as not-found.
        let replies = t["replies"].as_array().expect("replies");
        let uris: Vec<&str> = replies.iter().filter_map(|r| r["post"]["uri"].as_str()).collect();
        assert_eq!(uris, [local.as_str(), far("reply").as_str()]);
        // A fetched reply's own replies are listed and fetched too.
        assert_eq!(replies[1]["replies"][0]["post"]["uri"], json!(far("deep")));
        db.drop_db().await;
    }

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
        let req = Requester::test();

        let t = h.get_post_thread(&anon, &req,&mid, 6, 80).await.expect("db").expect("view");
        let t = &t.inner()["thread"];
        assert_eq!(t["post"]["uri"], json!(mid));
        assert_eq!(t["parent"]["post"]["uri"], json!(root));
        let replies = t["replies"].as_array().expect("replies");
        assert_eq!(replies.len(), 1, "the !hide sibling is omitted from threads");
        assert_eq!(replies[0]["post"]["uri"], json!(leaf));

        // parentHeight 0 drops the parent; depth 0 drops replies.
        let t = h.get_post_thread(&anon, &req,&mid, 0, 0).await.expect("db").expect("view");
        assert!(t.inner()["thread"].get("parent").is_none());
        assert_eq!(t.inner()["thread"]["replies"], json!([]));

        // Misses: private, unknown and collapsed anchors are all NotFound.
        for uri in [
            format!("at://{ALICE}/tools.kryphocron.feed.postPrivate/3kaaaaaaaaaa6"),
            format!("at://{ALICE}/app.bsky.feed.post/3kaaaaaaaaaz9"),
            sibling.clone(),
        ] {
            let e = h.get_post_thread(&anon, &req,&uri, 6, 80).await.expect("db").expect_err("miss");
            assert_eq!((e.error, e.message.as_str()), ("NotFound", "Post not found"), "{uri}");
        }
        db.drop_db().await;
    }
}
