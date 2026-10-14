//! `app.bsky.feed.getPostThread` (design-v2 §7.1, §7.3), public tier only.
//!
//! The local tree follows `posts_parent` / `posts_root`: the parent chain up
//! to `parentHeight`, replies to `depth`, 200 nodes in total. A private-tier,
//! hidden or collapsed anchor is `400 NotFound "Post not found"`, the same as
//! a nonexistent post. A node's replies are the union of the local rows and
//! the replies Constellation lists for it; replies and parents not stored
//! here come from Slingshot (design-v3 §5.1, §11 G-6), within
//! `thread.max_remote_fetches`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use futures::StreamExt;
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::feed::{NSID_POST, PostContext};
use super::graph::NSID_FOLLOW;
use super::lists::is_list_uri;
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
pub const NSID_LIKE: &str = "app.bsky.feed.like";
/// Likers asked of Constellation per page of a known-likers scan.
const KNOWN_LIKERS_PAGE: u32 = 100;
const FOLLOWER_SCAN_PAGE: u32 = 100;
/// Profiles shown in `viewer.knownLikers`.
const KNOWN_LIKERS_SHOWN: usize = 5;
pub const DEFAULT_DEPTH: u32 = 6;
pub const DEFAULT_PARENT_HEIGHT: u32 = 80;

/// Set on a reply node that the root's threadgate hides, on the tree the V2
/// endpoints are built from. Never part of a response.
pub(super) const HIDDEN_MARK: &str = "hiddenByThreadgate";

/// The replies the root's threadgate hides from this viewer.
#[derive(Default)]
struct Hidden {
    uris: HashSet<String>,
    /// Keep them in the tree, marked, instead of leaving them out.
    keep: bool,
}

/// The root's valid threadgate, as one thread needs it.
struct RootGate {
    /// The `threadgateView`.
    view: Value,
    /// The replies it hides from this viewer.
    hidden: HashSet<String>,
    /// Its `allow` rules: `None` when the field is absent (anyone may
    /// reply), an empty array when nobody may.
    allow: Option<Value>,
}

/// A `threadgates` row: URI, CID, record, hidden replies, `allow`.
type GateRow = (String, String, Value, Vec<String>, Option<Value>);

/// A thread tree and the root's `threadgateView`, if the root has a valid
/// threadgate.
pub(super) type Tree = (Value, Option<Value>);

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

    /// The reply nodes under `uri`. A reply the root's threadgate hides is
    /// left out, or, with `hidden.keep`, kept and marked for the V2
    /// endpoints to sort out.
    fn reply_nodes(&self, uri: &str, children: &HashMap<String, Vec<String>>, remote: &Remote, hidden: &Hidden) -> Vec<Value> {
        children
            .get(uri)
            .map(|kids| {
                kids.iter()
                    .filter(|k| self.listable(k) && self.blocked_post(k).is_none())
                    .filter(|k| hidden.keep || !hidden.uris.contains(*k))
                    .filter_map(|k| {
                        let post = self.node_post(k, remote)?;
                        let mut node = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post});
                        if hidden.uris.contains(k) {
                            node[HIDDEN_MARK] = json!(true);
                        }
                        let replies = self.reply_nodes(k, children, remote, hidden);
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
        Ok(match self.thread_tree(viewer, req, uri, depth, parent_height, false).await? {
            Ok((thread, gate)) => {
                let mut out = json!({ "thread": thread });
                if let Some(gate) = gate {
                    out["threadgate"] = gate;
                }
                Ok(Self::emit(out))
            }
            Err(e) => Err(e),
        })
    }

    /// The root's threadgate (design-v3 §8.3): its view, and the replies it
    /// hides. A gate counts only when its record key is its post's and its
    /// author is the post's author. A hidden reply is not hidden from its
    /// own author.
    async fn threadgate(&self, viewer: &Viewer, req: &Requester, root: &str) -> Result<Option<RootGate>, sqlx::Error> {
        let rows: Vec<GateRow> =
            sqlx::query_as("SELECT uri, cid, record, hidden_replies, allow FROM threadgates WHERE post_uri = $1")
                .bind(root)
                .fetch_all(self.pool())
                .await?;
        let rkey = |uri: &str| uri.rsplit('/').next().unwrap_or_default().to_string();
        let Some((uri, cid, record, hidden, allow)) = rows
            .into_iter()
            .find(|(gate, ..)| uri_author(gate) == uri_author(root) && rkey(gate) == rkey(root))
        else {
            return Ok(None);
        };
        let view = self.threadgate_view(viewer, req, &uri, &cid, record, allow.as_ref()).await?;
        let me = viewer.did.as_deref();
        let hidden = hidden.into_iter().filter(|reply| me.is_none() || uri_author(reply) != me).collect();
        Ok(Some(RootGate { view, hidden, allow }))
    }

    /// `viewer.replyDisabled` for a post of the thread rooted at `root`, from
    /// the root's valid threadgate stored here (single-object views other
    /// than a thread, design-v3 §8.2).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(super) async fn reply_disabled_in(&self, viewer: &Viewer, req: &Requester, root: &str) -> Result<Option<bool>, sqlx::Error> {
        if viewer.state_did().is_none() {
            return Ok(None);
        }
        let allow: Option<Option<Value>> = sqlx::query_scalar(
            "SELECT allow FROM threadgates WHERE post_uri = $1 \
               AND author_did = split_part(post_uri, '/', 3) AND rkey = split_part(post_uri, '/', 5)",
        )
        .bind(root)
        .fetch_optional(self.pool())
        .await?;
        self.reply_disabled(viewer, req, root, allow.flatten().as_ref()).await
    }

    /// A `threadgateView`: the gate's record and the lists its rules name,
    /// as `listViewBasic`, under the locally-servable rule.
    pub(super) async fn threadgate_view(
        &self,
        viewer: &Viewer,
        req: &Requester,
        uri: &str,
        cid: &str,
        record: Value,
        allow: Option<&Value>,
    ) -> Result<Value, sqlx::Error> {
        let mut lists = Vec::new();
        let named = allow
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|rule| rule.get("$type").and_then(Value::as_str) == Some("app.bsky.feed.threadgate#listRule"))
            .filter_map(|rule| rule.get("list").and_then(Value::as_str));
        for list in named {
            if let Some((row, local)) = self.load_list(req, list).await? {
                let count = if local { Some(self.list_item_count(list).await?) } else { None };
                lists.push(self.list_basic(viewer, &row, None, count).await?);
            }
        }
        Ok(json!({"uri": uri, "cid": cid, "record": record, "lists": lists}))
    }

    /// `viewer.knownLikers` for a thread's anchor (design-v3 §8.2): the
    /// accounts the ready-member viewer follows that liked the post. Stored
    /// likes are joined with a bounded scan of the post's likers on
    /// Constellation, each page charged to the caller's share. `None` when
    /// there are none.
    async fn known_likers(&self, viewer: &Viewer, req: &Requester, post: &str) -> Result<Option<Value>, sqlx::Error> {
        let Some(me) = viewer.state_did() else {
            return Ok(None);
        };
        let mut likers: Vec<String> = sqlx::query_scalar(
            "SELECT author_did FROM likes WHERE subject_uri = $2 \
               AND author_did IN (SELECT subject_did FROM follows WHERE author_did = $1) \
             ORDER BY created_at DESC, author_did",
        )
        .bind(me)
        .bind(post)
        .fetch_all(self.pool())
        .await?;
        if let Some(client) = self.backlinks() {
            let follows: HashSet<String> = sqlx::query_scalar("SELECT subject_did FROM follows WHERE author_did = $1")
                .bind(me)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .collect();
            let mut cursor: Option<String> = None;
            for _ in 0..self.inner.cfg.known_likers_scan_max_pages {
                if follows.is_empty() || self.draw_constellation(req).is_err() {
                    break;
                }
                let Ok(page) = client.distinct_dids_page(post, NSID_LIKE, ".subject.uri", KNOWN_LIKERS_PAGE, cursor.as_deref()).await else {
                    break;
                };
                for did in page.linking_dids {
                    if follows.contains(&did) && !likers.contains(&did) {
                        likers.push(did);
                    }
                }
                cursor = page.cursor;
                if cursor.is_none() {
                    break;
                }
            }
        }
        let state = self.actor_viewer(viewer, &likers).await?;
        likers.retain(|d| !state.blocked_either_way(d));
        if likers.is_empty() {
            return Ok(None);
        }
        let shown: Vec<String> = likers.iter().take(KNOWN_LIKERS_SHOWN).cloned().collect();
        let ctx = self.profile_context(viewer, req, &shown, None).await?;
        let actors: Vec<Value> = shown.iter().map(|d| ctx.view(d, ProfileKind::Basic)).collect();
        Ok(Some(json!({"count": likers.len(), "actors": actors})))
    }

    /// Whether `follower` is among `subject`'s followers on Constellation: a
    /// scan of at most `known_followers.profile_scan_pages` pages, each
    /// charged to the caller's share. `None` when the scan could not finish
    /// (no Constellation, no budget, an error, or more pages than allowed).
    async fn follower_scan(&self, req: &Requester, subject: &str, follower: &str) -> Option<bool> {
        let client = self.backlinks()?;
        let mut cursor: Option<String> = None;
        for _ in 0..self.inner.cfg.known_followers_profile_scan_pages {
            self.draw_constellation(req).ok()?;
            let page = client.distinct_dids_page(subject, NSID_FOLLOW, ".subject", FOLLOWER_SCAN_PAGE, cursor.as_deref()).await.ok()?;
            if page.linking_dids.iter().any(|d| d == follower) {
                return Some(true);
            }
            cursor = page.cursor;
            if cursor.is_none() {
                return Some(false);
            }
        }
        None
    }

    /// `viewer.replyDisabled` for a post in the thread rooted at `root`
    /// (design-v3 §8.3), for a ready member. `None` leaves the field out:
    /// no gate, no `allow`, or an answer this AppView cannot know.
    async fn reply_disabled(&self, viewer: &Viewer, req: &Requester, root: &str, allow: Option<&Value>) -> Result<Option<bool>, sqlx::Error> {
        let (Some(me), Some(rules)) = (viewer.state_did(), allow.and_then(Value::as_array)) else {
            return Ok(None);
        };
        let author = uri_author(root).unwrap_or_default();
        if me == author {
            return Ok(Some(false));
        }
        if rules.is_empty() {
            return Ok(Some(true));
        }
        // Whether every rule's input was complete.
        let mut complete = true;
        for rule in rules {
            let allowed = match rule.get("$type").and_then(Value::as_str).unwrap_or_default() {
                "app.bsky.feed.threadgate#mentionRule" => {
                    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM posts WHERE uri = $1 AND $2 = ANY(mention_dids))")
                        .bind(root)
                        .bind(me)
                        .fetch_one(self.pool())
                        .await?
                }
                "app.bsky.feed.threadgate#followerRule" => {
                    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM follows WHERE author_did = $1 AND subject_did = $2)")
                        .bind(me)
                        .bind(author)
                        .fetch_one(self.pool())
                        .await?
                }
                "app.bsky.feed.threadgate#followingRule" => {
                    let follows: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM follows WHERE author_did = $1 AND subject_did = $2)")
                        .bind(author)
                        .bind(me)
                        .fetch_one(self.pool())
                        .await?;
                    // A follow not stored here may still exist: look for the
                    // author among the viewer's followers on Constellation.
                    match follows {
                        true => true,
                        false => match self.follower_scan(req, me, author).await {
                            Some(found) => found,
                            None => {
                                complete = false;
                                false
                            }
                        },
                    }
                }
                "app.bsky.feed.threadgate#listRule" => {
                    let list = rule.get("list").and_then(Value::as_str).unwrap_or_default();
                    // Only a list served from this AppView's own rows is a
                    // complete input.
                    if is_list_uri(list) && self.is_ready_member(uri_author(list).unwrap_or_default()).await? {
                        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM list_items WHERE list_uri = $1 AND subject_did = $2)")
                            .bind(list)
                            .bind(me)
                            .fetch_one(self.pool())
                            .await?
                    } else {
                        complete = false;
                        false
                    }
                }
                // A rule this version does not know.
                _ => {
                    complete = false;
                    false
                }
            };
            if allowed {
                return Ok(Some(false));
            }
        }
        Ok(complete.then_some(true))
    }

    /// The tree of `getPostThread`, with the root's threadgate view. With
    /// `keep_hidden`, replies the gate hides stay in the tree, marked.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(super) async fn thread_tree(
        &self,
        viewer: &Viewer,
        req: &Requester,
        uri: &str,
        depth: u32,
        parent_height: u32,
        keep_hidden: bool,
    ) -> Result<Result<Tree, ViewError>, super::HydrationError> {
        let started = Instant::now();
        if is_private_uri(uri) {
            return Ok(Err(not_found()));
        }
        let depth = depth.min(1000);
        let parent_height = parent_height.min(1000);
        let anchor = self.load_posts(&[uri.to_string()]).await?;
        let Some(anchor_row) = anchor.get(uri) else {
            let author = uri_author(uri).unwrap_or_default().to_string();
            if !self.stores_public_of(&author).await
                && !moderation::gate(self.pool(), uri).await?.is_hidden()
                && let Some(v) = self.upstream().post_thread(req, uri, depth, parent_height).await
            {
                // An upstream AppView's thread, with whatever gate view it
                // gave.
                let mut v = self.normalized(viewer, v).await?.0;
                let gate = v.get("threadgate").cloned().filter(|g| !g.is_null());
                return Ok(Ok((v["thread"].take(), gate)));
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
            // An in-scope author's posts are all stored while the mode is
            // On: this one is gone.
            if self.stores_public_of(uri_author(&p).unwrap_or_default()).await {
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
                    // A reply by an in-scope author would be a local row
                    // while the mode is On.
                    if uris.contains(&kid) || wanted.iter().any(|(_, k)| *k == kid) || self.stores_public_of(uri_author(&kid).unwrap_or_default()).await {
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
            return Ok(Ok((blocked, None)));
        }
        let Some(post) = ctx.post_view(uri) else {
            return Ok(Err(not_found()));
        };
        let root = anchor_row.reply_root_uri.clone().unwrap_or_else(|| uri.to_string());
        let root_gate = self.threadgate(viewer, req, &root).await?;
        let reply_disabled = self.reply_disabled(viewer, req, &root, root_gate.as_ref().and_then(|g| g.allow.as_ref())).await?;
        let (gate, hidden) = match root_gate {
            Some(g) => (Some(g.view), Hidden { uris: g.hidden, keep: keep_hidden }),
            None => (None, Hidden::default()),
        };
        let mut post = post;
        if let Some(disabled) = reply_disabled {
            post["viewer"]["replyDisabled"] = json!(disabled);
        }
        if let Some(likers) = self.known_likers(viewer, req, uri).await? {
            post["viewer"]["knownLikers"] = likers;
        }
        let mut thread = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post});
        if parent_height > 0
            && let Some(parent) = &anchor_row.reply_parent_uri
            && let Some(above) = ctx.parent_node(parent, parent_height - 1, &remote)
        {
            thread["parent"] = above;
        }
        thread["replies"] = Value::Array(ctx.reply_nodes(uri, &children, &remote, &hidden));
        Ok(Ok((thread, gate)))
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

    /// design-v3 §8.3 `followingRule`: a follow by the root's author that is
    /// not stored here is looked for among the viewer's followers on
    /// Constellation, and a scan that cannot finish leaves the field out.
    #[tokio::test]
    async fn the_following_rule_scans_the_viewers_followers() {
        use std::sync::Arc;

        use axum::extract::Query;
        use axum::routing::get;

        use crate::hydration::testing::{FixedUpstream, hydrator_full};

        let Some(db) = fresh_db("hydration_following_rule").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let root = post(p, BOB, "3kaaaaaaaaaa2", "root", None, None).await;
        let carol = "did:plc:cccccccccccccccccccccccc";
        // Alice's followers: Bob, on the second of two pages. Carol's: one
        // page without Bob. Bob's own: more pages than a scan may read.
        let router = axum::Router::new().route(
            "/links/distinct-dids",
            get(move |Query(q): Query<HashMap<String, String>>| async move {
                let target = q.get("target").map(String::as_str).unwrap_or_default();
                let page = q.get("cursor").map(String::as_str);
                axum::Json(match (target, page) {
                    (ALICE, None) => json!({"total": 2, "linking_dids": ["did:plc:xxxxxxxxxxxxxxxxxxxxxxxx"], "cursor": "2"}),
                    (ALICE, Some(_)) => json!({"total": 2, "linking_dids": [BOB]}),
                    (BOB, _) => json!({"total": 9, "linking_dids": [], "cursor": "more"}),
                    _ => json!({"total": 1, "linking_dids": ["did:plc:xxxxxxxxxxxxxxxxxxxxxxxx"]}),
                })
            }),
        );
        let base = crate::test_support::serve_fake(router).await;
        let h = hydrator_full(p, ScopeSets::default(), Arc::new(FixedUpstream::default()), Some(&base));
        let req = Requester::test();
        let allow = json!([{"$type": "app.bsky.feed.threadgate#followingRule"}]);
        let who = |did: &str| Viewer {
            did: Some(did.into()),
            hydrate: true,
        };
        assert_eq!(h.reply_disabled(&who(ALICE), &req, &root, Some(&allow)).await.expect("db"), Some(false));
        assert_eq!(h.reply_disabled(&who(carol), &req, &root, Some(&allow)).await.expect("db"), Some(true));
        // Bob looks at a post of Alice's: his followers run to more pages
        // than the scan may read, so the answer is unknown.
        let hers = post(p, ALICE, "3kaaaaaaaaaa3", "hers", None, None).await;
        assert_eq!(h.reply_disabled(&who(BOB), &req, &hers, Some(&allow)).await.expect("db"), None);
        // Without Constellation, a follow not stored here is unknown too.
        let bare = hydrator(p, ScopeSets::default());
        assert_eq!(bare.reply_disabled(&who(ALICE), &req, &root, Some(&allow)).await.expect("db"), None);
        db.drop_db().await;
    }

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

    /// T-16 (public-indexing design §9.1): while the mode is On, an S
    /// author's missing parent is gone; in Filling it is fetched, since
    /// the fill may not have stored it yet.
    #[tokio::test]
    async fn a_missing_s_author_parent_is_fetched_unless_the_mode_is_on() {
        use std::sync::Arc;

        use crate::hydration::testing::{FixedUpstream, hydrator_with, set_mode};
        use crate::indexing::PublicMode;

        let Some(db) = fresh_db("hydration_thread_filling").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        sets.scope.insert(BOB.into());
        let parent = format!("at://{BOB}/app.bsky.feed.post/3kparent2222");
        let anchor = post(p, ALICE, "3kanchor2222", "anchor", Some(&parent), Some(&parent)).await;
        let mut up = FixedUpstream::default();
        up.posts.insert(
            parent.clone(),
            json!({"$type": "app.bsky.feed.defs#postView", "uri": parent, "cid": "bafy",
                   "author": {"did": BOB, "handle": "bob.test", "viewer": {}, "labels": []},
                   "record": {"$type": "app.bsky.feed.post", "text": "parent", "createdAt": "2026-09-26T00:00:00Z"},
                   "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "labels": []}),
        );
        let h = hydrator_with(p, sets, Arc::new(up));
        let (anon, req) = (Viewer::anonymous(), Requester::test());

        let t = h.get_post_thread(&anon, &req, &anchor, 0, 80).await.expect("db").expect("view");
        assert_eq!(t.inner()["thread"]["parent"]["$type"], "app.bsky.feed.defs#notFoundPost", "On: stored if it exists");
        assert_eq!(t.inner()["thread"]["parent"]["uri"], json!(parent));
        set_mode(h.scope(), PublicMode::Filling).await;
        let t = h.get_post_thread(&anon, &req, &anchor, 0, 80).await.expect("db").expect("view");
        assert_eq!(t.inner()["thread"]["parent"]["post"]["record"]["text"], "parent", "Filling: fetched");
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
