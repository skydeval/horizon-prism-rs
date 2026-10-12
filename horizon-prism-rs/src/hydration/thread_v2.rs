//! `app.bsky.unspecced.getPostThreadV2` and `getPostThreadOtherV2`
//! (design-v3 §5.1).
//!
//! Both are built from the tree `getPostThread` assembles (the local rows,
//! Constellation's reply listings, Slingshot for posts not stored here, or
//! an upstream AppView's thread for an anchor this AppView does not hold),
//! so the three endpoints agree on what a thread contains. This module
//! turns that tree into V2's flat list:
//! - parents at negative depths, the anchor at 0, replies below;
//! - siblings ordered root author first, then the viewer's own replies, then
//!   accounts the viewer follows, then everyone else, each group by `sort`;
//! - every direct reply of the anchor, and at most `branchingFactor` replies
//!   of each deeper post, down to `below` levels;
//! - a direct reply of the anchor by an account the viewer fully mutes
//!   moves to "other" (`getPostThreadOtherV2`); a muted reply deeper down is
//!   left out of both;
//! - the contiguous thread by the root's author is marked (`opThread`).
//!
//! - a signed-out viewer is not shown posts whose author's profile carries
//!   the `!no-unauthenticated` label: the anchor and parents become
//!   `threadItemNoUnauthenticated`, and replies are left out.

use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use super::thread::{HIDDEN_MARK, Tree};
use super::{Hydrator, ViewResult, Viewer};
use crate::xrpc::pools::Requester;

pub const DEFAULT_BELOW: u32 = 6;
pub const MAX_BELOW: u32 = 20;
pub const DEFAULT_BRANCHING: u32 = 10;
pub const MAX_BRANCHING: u32 = 100;

const POST_NODE: &str = "app.bsky.feed.defs#threadViewPost";
const BLOCKED_NODE: &str = "app.bsky.feed.defs#blockedPost";
/// A post kept from a signed-out viewer at its author's request: the node
/// `withhold` leaves in the tree, with only `uri` and `parent`.
const WITHHELD_NODE: &str = "withheld";
/// The self-label by which an account asks not to be shown to signed-out
/// viewers.
const NO_UNAUTHENTICATED: &str = "!no-unauthenticated";

/// Every post author in the tree: the anchor, its parents and its replies.
fn authors(node: &Value, out: &mut Vec<String>) {
    let who = author(node);
    if !who.is_empty() && !out.iter().any(|d| d == who) {
        out.push(who.to_string());
    }
    if let Some(parent) = node.get("parent") {
        authors(parent, out);
    }
    for reply in node.get("replies").and_then(Value::as_array).into_iter().flatten() {
        authors(reply, out);
    }
}

/// Remove the posts of `withheld` authors: the anchor and each parent
/// become a bare node (and the anchor loses its replies); a reply goes, with
/// everything under it.
fn withhold(node: &mut Value, withheld: &HashSet<String>) {
    withhold_above(node, withheld);
    if kind(node) == POST_NODE {
        withhold_below(node, withheld);
    }
}

fn withhold_above(node: &mut Value, withheld: &HashSet<String>) {
    if let Some(parent) = node.get_mut("parent") {
        withhold_above(parent, withheld);
    }
    if kind(node) == POST_NODE && withheld.contains(author(node)) {
        let parent = node.get_mut("parent").map(Value::take);
        let at = uri(node).to_string();
        *node = json!({"$type": WITHHELD_NODE, "uri": at});
        if let Some(parent) = parent {
            node["parent"] = parent;
        }
    }
}

fn withhold_below(node: &mut Value, withheld: &HashSet<String>) {
    if let Some(replies) = node.get_mut("replies").and_then(Value::as_array_mut) {
        replies.retain(|r| !(kind(r) == POST_NODE && withheld.contains(author(r))));
        for reply in replies {
            withhold_below(reply, withheld);
        }
    }
}

/// Sibling order within a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    Oldest,
    Newest,
    Top,
}

impl Sort {
    /// `sort` is an open set: any value but `newest` and `top` is `oldest`.
    #[must_use]
    pub fn parse(value: Option<&str>) -> Self {
        match value {
            Some("newest") => Self::Newest,
            Some("top") => Self::Top,
            _ => Self::Oldest,
        }
    }
}

/// The request's shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub above: bool,
    pub below: u32,
    pub branching: u32,
    pub sort: Sort,
}

fn kind(node: &Value) -> &str {
    node.get("$type").and_then(Value::as_str).unwrap_or_default()
}

fn author(node: &Value) -> &str {
    node.pointer("/post/author/did").and_then(Value::as_str).unwrap_or_default()
}

fn uri(node: &Value) -> &str {
    node.pointer("/post/uri").or_else(|| node.get("uri")).and_then(Value::as_str).unwrap_or_default()
}

fn created(node: &Value) -> &str {
    node.pointer("/post/record/createdAt")
        .or_else(|| node.pointer("/post/indexedAt"))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn count(node: &Value, field: &str) -> usize {
    node.pointer(&format!("/post/{field}")).and_then(Value::as_u64).map_or(0, |n| usize::try_from(n).unwrap_or(usize::MAX))
}

fn is_reply(node: &Value) -> bool {
    node.pointer("/post/record/reply/parent/uri").is_some()
}

fn muted(node: &Value) -> bool {
    node.pointer("/post/author/viewer/muted").and_then(Value::as_bool).unwrap_or(false)
}

/// Hidden by the root's threadgate (marked by the tree builder).
fn hidden(node: &Value) -> bool {
    node.get(HIDDEN_MARK).and_then(Value::as_bool).unwrap_or(false)
}

/// Left out of the main list: hidden by the gate, or by a muted author.
fn set_aside(node: &Value) -> bool {
    muted(node) || hidden(node)
}

/// The post replies of a node: blocked and missing replies are not items.
fn replies(node: &Value) -> Vec<&Value> {
    node.get("replies")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter(|n| kind(n) == POST_NODE).collect())
        .unwrap_or_default()
}

fn item(uri: &str, depth: i64, value: Value) -> Value {
    json!({"uri": uri, "depth": depth, "value": value})
}

struct Flattener<'a> {
    options: &'a Options,
    viewer: Option<&'a str>,
    root_author: &'a str,
    /// Position in the contiguous thread by the root's author, by URI.
    op: HashMap<String, usize>,
    items: Vec<Value>,
    other: Vec<Value>,
}

impl Flattener<'_> {
    fn post_item(&self, node: &Value, depth: i64, more_parents: bool, more_replies: usize, aside: bool) -> Value {
        let mut value = json!({
            "$type": "app.bsky.unspecced.defs#threadItemPost",
            "post": node["post"],
            "moreParents": more_parents,
            "moreReplies": more_replies,
            "opThread": false,
            "hiddenByThreadgate": aside && hidden(node),
            "mutedByViewer": aside && muted(node),
        });
        // Marked only when the OP's thread has at least two posts.
        if self.op.len() >= 2
            && let Some(index) = self.op.get(uri(node))
        {
            value["opThread"] = json!(true);
            value["opThreadPostIndex"] = json!(index);
            value["opThreadPostCount"] = json!(self.op.len());
        }
        item(uri(node), depth, value)
    }

    /// Siblings in V2's order: the root's author, the viewer, accounts the
    /// viewer follows, everyone else; each group by `sort`.
    fn ordered<'n>(&self, mut nodes: Vec<&'n Value>) -> Vec<&'n Value> {
        let group = |n: &Value| {
            let who = author(n);
            if who == self.root_author {
                0
            } else if self.viewer == Some(who) {
                1
            } else if n.pointer("/post/author/viewer/following").is_some() {
                2
            } else {
                3
            }
        };
        nodes.sort_by(|a, b| {
            group(a).cmp(&group(b)).then_with(|| match self.options.sort {
                Sort::Oldest => created(a).cmp(created(b)),
                Sort::Newest => created(b).cmp(created(a)),
                Sort::Top => count(b, "likeCount").cmp(&count(a, "likeCount")).then_with(|| created(a).cmp(created(b))),
            })
        });
        nodes
    }

    /// The replies of `node` that are returned when `node` is at `depth`.
    fn returned<'n>(&self, node: &'n Value, depth: i64) -> Vec<&'n Value> {
        let below = i64::from(self.options.below);
        if depth >= below {
            return Vec::new();
        }
        let kept: Vec<&Value> = replies(node).into_iter().filter(|n| !set_aside(n)).collect();
        let mut kept = self.ordered(kept);
        // Every direct reply of the anchor; a bounded number deeper down.
        if depth >= 1 {
            kept.truncate(usize::try_from(self.options.branching).unwrap_or(usize::MAX));
        }
        kept
    }

    /// Replies of `node` known and not returned: best effort.
    fn more_replies(&self, node: &Value, depth: i64) -> usize {
        count(node, "replyCount")
            .max(replies(node).len())
            .saturating_sub(self.returned(node, depth).len())
    }

    fn walk(&mut self, node: &Value, depth: i64) {
        if depth == 0 {
            // A direct reply the gate hides, or by a fully muted account,
            // goes to "other".
            let moved: Vec<&Value> = replies(node).into_iter().filter(|n| set_aside(n)).collect();
            for reply in self.ordered(moved) {
                let more = count(reply, "replyCount").max(replies(reply).len());
                let value = self.post_item(reply, 1, false, more, true);
                self.other.push(value);
            }
        }
        for reply in self.returned(node, depth) {
            let more = self.more_replies(reply, depth + 1);
            let value = self.post_item(reply, depth + 1, false, more, false);
            self.items.push(value);
            self.walk(reply, depth + 1);
        }
    }
}

/// The contiguous thread by the root's author: from the root, each post by
/// that author that replies to the previous one. Along the parent chain the
/// one known child is taken; below the anchor, the earliest reply by the
/// author. Empty when the root is not in the response.
fn op_thread(chain: &[&Value], anchor: &Value) -> HashMap<String, usize> {
    // Root first, anchor last.
    let mut path: Vec<&Value> = chain.iter().rev().copied().collect();
    path.push(anchor);
    let Some(root) = path.first().copied().filter(|n| kind(n) == POST_NODE && !is_reply(n)) else {
        return HashMap::new();
    };
    let op = author(root);
    let mut out = HashMap::new();
    for node in &path {
        if kind(node) != POST_NODE || author(node) != op {
            return out;
        }
        out.insert(uri(node).to_string(), out.len() + 1);
    }
    let mut last = anchor;
    while let Some(next) = replies(last).into_iter().filter(|n| author(n) == op).min_by(|a, b| created(a).cmp(created(b))) {
        out.insert(uri(next).to_string(), out.len() + 1);
        last = next;
    }
    out
}

/// A `getPostThread` tree as V2's flat list, with the replies moved to
/// "other".
#[must_use]
pub fn flatten(thread: &Value, options: &Options, viewer: Option<&str>) -> (Vec<Value>, Vec<Value>) {
    if kind(thread) == BLOCKED_NODE {
        let blocked = json!({"$type": "app.bsky.unspecced.defs#threadItemBlocked", "author": thread["author"]});
        return (vec![item(uri(thread), 0, blocked)], Vec::new());
    }
    // The parent chain, nearest first.
    let mut chain: Vec<&Value> = Vec::new();
    let mut above = thread.get("parent");
    while let Some(node) = above {
        chain.push(node);
        above = node.get("parent");
    }
    let root_author = chain.last().map_or_else(|| author(thread), |top| author(top)).to_string();
    let mut f = Flattener {
        options,
        viewer,
        root_author: &root_author,
        op: op_thread(&chain, thread),
        items: Vec::new(),
        other: Vec::new(),
    };
    if options.above {
        for (i, node) in chain.iter().enumerate().rev() {
            let depth = -i64::try_from(i + 1).unwrap_or(i64::MAX);
            let topmost = i + 1 == chain.len();
            let value = match kind(node) {
                POST_NODE => f.post_item(node, depth, topmost && is_reply(node), 0, false),
                BLOCKED_NODE => item(uri(node), depth, json!({"$type": "app.bsky.unspecced.defs#threadItemBlocked", "author": node["author"]})),
                WITHHELD_NODE => item(uri(node), depth, json!({"$type": "app.bsky.unspecced.defs#threadItemNoUnauthenticated"})),
                _ => item(uri(node), depth, json!({"$type": "app.bsky.unspecced.defs#threadItemNotFound"})),
            };
            f.items.push(value);
        }
    }
    if kind(thread) == WITHHELD_NODE {
        f.items.push(item(uri(thread), 0, json!({"$type": "app.bsky.unspecced.defs#threadItemNoUnauthenticated"})));
        return (f.items, f.other);
    }
    let more_parents = is_reply(thread) && (!options.above || chain.is_empty());
    let more = f.more_replies(thread, 0);
    let anchor = f.post_item(thread, 0, more_parents, more, false);
    f.items.push(anchor);
    f.walk(thread, 0);
    (f.items, f.other)
}

impl Hydrator {
    /// The tree both V2 endpoints are built from, with replies the root's
    /// threadgate hides kept and marked, and the gate's view. `None` for an
    /// anchor that misses (nonexistent, private-tier, hidden or collapsed).
    async fn v2_tree(&self, viewer: &Viewer, req: &Requester, anchor: &str, below: u32) -> Result<Option<Tree>, super::HydrationError> {
        let Ok((mut tree, gate)) = self.thread_tree(viewer, req, anchor, below, self.inner.cfg.thread_v2_max_parents, true).await? else {
            return Ok(None);
        };
        // A signed-out viewer is not shown posts by accounts that ask for
        // that with a label on their profile. Only profiles stored here are
        // read: no fetch is made to find the label of any other author.
        if viewer.did.is_none() {
            let mut dids = Vec::new();
            authors(&tree, &mut dids);
            let withheld: HashSet<String> = sqlx::query_scalar(
                "SELECT did FROM actors WHERE did = ANY($1) AND profile_record->'labels'->'values' @> $2",
            )
            .bind(&dids)
            .bind(json!([{ "val": NO_UNAUTHENTICATED }]))
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .collect();
            if !withheld.is_empty() {
                withhold(&mut tree, &withheld);
            }
        }
        Ok(Some((tree, gate)))
    }

    /// `app.bsky.unspecced.getPostThreadV2`. The lexicon declares no errors:
    /// a missing anchor is an item.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_post_thread_v2(&self, viewer: &Viewer, req: &Requester, anchor: &str, options: &Options) -> ViewResult<Value> {
        let Some((tree, gate)) = self.v2_tree(viewer, req, anchor, options.below).await? else {
            let missing = item(anchor, 0, json!({"$type": "app.bsky.unspecced.defs#threadItemNotFound"}));
            return Ok(Ok(Self::emit(json!({"thread": [missing], "hasOtherReplies": false}))));
        };
        let (thread, other) = flatten(&tree, options, viewer.state_did());
        let mut out = json!({"thread": thread, "hasOtherReplies": !other.is_empty()});
        if let Some(gate) = gate {
            out["threadgate"] = gate;
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.unspecced.getPostThreadOtherV2`: the anchor's direct
    /// replies that V2 left out. Never the anchor or its ancestors. An
    /// anchor that misses or is blocked gives an empty list.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_post_thread_other_v2(&self, viewer: &Viewer, req: &Requester, anchor: &str) -> ViewResult<Value> {
        let options = Options {
            above: false,
            below: 1,
            branching: 0,
            sort: Sort::Oldest,
        };
        let other = match self.v2_tree(viewer, req, anchor, 1).await? {
            Some((tree, _)) => flatten(&tree, &options, viewer.state_did()).1,
            None => Vec::new(),
        };
        Ok(Ok(Self::emit(json!({ "thread": other }))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::test_support::fresh_db;

    const OP: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
    const ME: &str = "did:plc:dddddddddddddddddddddddd";

    fn node(uri: &str, who: &str, at: &str, parent: Option<&str>, likes: u64, replies: Vec<Value>) -> Value {
        let mut record = json!({"text": uri, "createdAt": at});
        if let Some(p) = parent {
            record["reply"] = json!({"parent": {"uri": p}, "root": {"uri": "root"}});
        }
        let reply_count = replies.len();
        json!({"$type": POST_NODE, "post": {"uri": uri, "author": {"did": who, "viewer": {"muted": false}}, "record": record, "likeCount": likes, "replyCount": reply_count}, "replies": replies})
    }

    fn opts(below: u32, branching: u32, sort: Sort) -> Options {
        Options {
            above: true,
            below,
            branching,
            sort,
        }
    }

    fn shape(items: &[Value]) -> Vec<(String, i64)> {
        items.iter().map(|i| (i["uri"].as_str().expect("uri").to_string(), i["depth"].as_i64().expect("depth"))).collect()
    }

    fn pairs(list: &[(&str, i64)]) -> Vec<(String, i64)> {
        list.iter().map(|(u, d)| ((*u).to_string(), *d)).collect()
    }

    #[test]
    fn a_tree_becomes_a_flat_list_in_sibling_order() {
        // root (OP) <- anchor (OP) with four replies; one has replies of its own.
        let mut followed = node("r-followed", CAROL, "2026-01-04", Some("anchor"), 9, vec![]);
        followed["post"]["author"]["viewer"]["following"] = json!("at://x/app.bsky.graph.follow/1");
        let deep = node(
            "r-other",
            BOB,
            "2026-01-01",
            Some("anchor"),
            1,
            vec![
                node("d1", BOB, "2026-01-05", Some("r-other"), 0, vec![node("e1", BOB, "2026-01-06", Some("d1"), 0, vec![])]),
                node("d2", CAROL, "2026-01-06", Some("r-other"), 5, vec![]),
                node("d3", CAROL, "2026-01-07", Some("r-other"), 0, vec![]),
            ],
        );
        let mut anchor = node(
            "anchor",
            OP,
            "2026-01-01",
            Some("root"),
            0,
            vec![deep, followed, node("r-mine", ME, "2026-01-03", Some("anchor"), 0, vec![]), node("r-op", OP, "2026-01-05", Some("anchor"), 0, vec![])],
        );
        anchor["parent"] = node("root", OP, "2025-12-31", None, 0, vec![]);

        let (items, other) = flatten(&anchor, &opts(2, 2, Sort::Oldest), Some(ME));
        assert!(other.is_empty());
        // Parent, anchor, then replies: OP, the viewer, a followed account,
        // everyone else; two levels deep, two replies per deeper post.
        assert_eq!(
            shape(&items),
            pairs(&[("root", -1), ("anchor", 0), ("r-op", 1), ("r-mine", 1), ("r-followed", 1), ("r-other", 1), ("d1", 2), ("d2", 2)])
        );
        let by_uri = |u: &str| items.iter().find(|i| i["uri"] == u).expect("item")["value"].clone();
        // What was known and not returned.
        assert_eq!(by_uri("r-other")["moreReplies"], 1);
        assert_eq!(by_uri("d1")["moreReplies"], 1);
        assert_eq!(by_uri("anchor")["moreReplies"], 0);
        assert_eq!(by_uri("root")["moreParents"], false);
        // The OP's contiguous thread: root, anchor, and the OP's reply.
        for (u, index) in [("root", 1), ("anchor", 2), ("r-op", 3)] {
            let v = by_uri(u);
            assert_eq!((&v["opThread"], &v["opThreadPostIndex"], &v["opThreadPostCount"]), (&json!(true), &json!(index), &json!(3)), "{u}");
        }
        assert_eq!(by_uri("r-mine")["opThread"], false);
        assert!(by_uri("r-mine").get("opThreadPostIndex").is_none());

        // `top` orders a group by likes; `newest` by time.
        let (items, _) = flatten(&anchor, &opts(2, 3, Sort::Top), None);
        let deeper: Vec<String> = shape(&items).into_iter().filter(|(_, d)| *d == 2).map(|(u, _)| u).collect();
        assert_eq!(deeper, ["d2", "d1", "d3"]);
        let (items, _) = flatten(&anchor, &opts(2, 3, Sort::Newest), None);
        let deeper: Vec<String> = shape(&items).into_iter().filter(|(_, d)| *d == 2).map(|(u, _)| u).collect();
        assert_eq!(deeper, ["d3", "d2", "d1"]);

        // `above = false`: no parents, and the anchor says there are some.
        let mut no_parents = opts(0, 10, Sort::Oldest);
        no_parents.above = false;
        let (items, _) = flatten(&anchor, &no_parents, None);
        assert_eq!(shape(&items), pairs(&[("anchor", 0)]));
        assert_eq!((&items[0]["value"]["moreParents"], &items[0]["value"]["moreReplies"]), (&json!(true), &json!(4)));
        assert_eq!(Sort::parse(Some("hottest")), Sort::Oldest);
    }

    #[test]
    fn muted_blocked_and_missing_nodes() {
        let mut muted_reply = node("r-muted", BOB, "2026-01-02", Some("anchor"), 0, vec![]);
        muted_reply["post"]["author"]["viewer"]["muted"] = json!(true);
        let mut deep_muted = node("d-muted", BOB, "2026-01-03", Some("r-ok"), 0, vec![]);
        deep_muted["post"]["author"]["viewer"]["muted"] = json!(true);
        let ok = node("r-ok", CAROL, "2026-01-02", Some("anchor"), 0, vec![deep_muted]);
        let blocked_reply = json!({"$type": BLOCKED_NODE, "uri": "r-blocked", "blocked": true, "author": {"did": BOB}});
        let mut anchor = node("anchor", OP, "2026-01-01", Some("p1"), 0, vec![muted_reply, ok, blocked_reply]);
        // The parent's own parent is blocked: the chain ends there.
        let mut parent = node("p1", CAROL, "2025-12-31", Some("p2"), 0, vec![]);
        parent["parent"] = json!({"$type": BLOCKED_NODE, "uri": "p2", "blocked": true, "author": {"did": BOB, "viewer": {"blockedBy": true}}});
        anchor["parent"] = parent;

        let (items, other) = flatten(&anchor, &opts(6, 10, Sort::Oldest), Some(ME));
        assert_eq!(shape(&items), pairs(&[("p2", -2), ("p1", -1), ("anchor", 0), ("r-ok", 1)]));
        assert_eq!(items[0]["value"], json!({"$type": "app.bsky.unspecced.defs#threadItemBlocked", "author": {"did": BOB, "viewer": {"blockedBy": true}}}));
        // The root is not in the response: no OP thread is claimed.
        assert!(items.iter().all(|i| i["value"].get("opThread").is_none_or(|v| v == false)));
        // The muted direct reply moved to "other"; the deeper one is gone.
        assert_eq!(shape(&other), pairs(&[("r-muted", 1)]));
        assert_eq!(other[0]["value"]["mutedByViewer"], true);
        assert_eq!(items[3]["value"]["moreReplies"], 1);

        // A blocked anchor is one item; a missing parent is an item.
        let blocked = json!({"$type": BLOCKED_NODE, "uri": "anchor", "blocked": true, "author": {"did": BOB}});
        let (items, other) = flatten(&blocked, &opts(6, 10, Sort::Oldest), None);
        assert_eq!((shape(&items), other.len()), (pairs(&[("anchor", 0)]), 0));
        let mut orphan = node("anchor", OP, "2026-01-01", Some("gone"), 0, vec![]);
        orphan["parent"] = json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": "gone", "notFound": true});
        let (items, _) = flatten(&orphan, &opts(6, 10, Sort::Oldest), None);
        assert_eq!(items[0], json!({"uri": "gone", "depth": -1, "value": {"$type": "app.bsky.unspecced.defs#threadItemNotFound"}}));
    }

    /// design-v3 §5.1: posts by an account whose profile carries
    /// `!no-unauthenticated` are kept from a signed-out viewer only.
    #[tokio::test]
    async fn a_signed_out_viewer_is_not_shown_labelled_authors() {
        let Some(db) = fresh_db("hydration_thread_v2_no_unauth").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(OP, "op.test"), (BOB, "bob.test"), (ME, "me.test")] {
            actor(p, d, h, h).await;
        }
        let root = post(p, OP, "3kaaaaaaaaaa2", "root", None, None).await;
        let second = post(p, OP, "3kaaaaaaaaaa3", "second", Some(&root), Some(&root)).await;
        let reply = post(p, BOB, "3kaaaaaaaaaa4", "reply", Some(&second), Some(&root)).await;
        post(p, ME, "3kaaaaaaaaaa5", "under the reply", Some(&reply), Some(&root)).await;
        let mut sets = ScopeSets::default();
        for d in [OP, BOB, ME] {
            sets.scope.insert(d.into());
        }
        let h = hydrator(p, sets);
        let (anon, req) = (Viewer::anonymous(), Requester::test());
        let options = opts(DEFAULT_BELOW, DEFAULT_BRANCHING, Sort::Oldest);
        let label = |did: &'static str, on: bool| async move {
            let labels = if on { r#"{"labels": {"$type": "com.atproto.label.defs#selfLabels", "values": [{"val": "!no-unauthenticated"}]}}"# } else { "{}" };
            exec(p, &format!("UPDATE actors SET profile_record = '{labels}' WHERE did = '{did}'")).await;
        };
        const WITHHELD: &str = "app.bsky.unspecced.defs#threadItemNoUnauthenticated";
        const POST: &str = "app.bsky.unspecced.defs#threadItemPost";
        let types = |v: &Value| -> Vec<(String, String)> {
            v["thread"]
                .as_array()
                .expect("thread")
                .iter()
                .map(|i| (i["uri"].as_str().expect("uri").to_string(), i["value"]["$type"].as_str().expect("type").to_string()))
                .collect()
        };
        let of = |list: &[(&String, &str)]| -> Vec<(String, String)> { list.iter().map(|(u, t)| ((*u).clone(), (*t).to_string())).collect() };

        // A labelled reply author: the reply and what is under it are gone.
        label(BOB, true).await;
        let v = h.get_post_thread_v2(&anon, &req, &second, &options).await.expect("db").expect("view");
        assert_eq!(types(v.inner()), of(&[(&root, POST), (&second, POST)]));
        // A signed-in viewer is shown everything.
        let me = Viewer {
            did: Some(ME.into()),
            hydrate: true,
        };
        let v = h.get_post_thread_v2(&me, &req, &second, &options).await.expect("db").expect("view");
        assert_eq!(v.inner()["thread"].as_array().map(Vec::len), Some(4));
        // A labelled anchor: its parents, the anchor as an item, no replies.
        let v = h.get_post_thread_v2(&anon, &req, &reply, &options).await.expect("db").expect("view");
        assert_eq!(types(v.inner()), of(&[(&root, POST), (&second, POST), (&reply, WITHHELD)]));
        assert!(v.inner()["thread"][2]["value"].get("post").is_none());
        let o = h.get_post_thread_other_v2(&anon, &req, &reply).await.expect("db").expect("view");
        assert_eq!(o.inner()["thread"], json!([]));
        // Labelled parents are items; the anchor is shown.
        label(BOB, false).await;
        label(OP, true).await;
        let v = h.get_post_thread_v2(&anon, &req, &reply, &options).await.expect("db").expect("view");
        assert_eq!(types(v.inner())[..3], of(&[(&root, WITHHELD), (&second, WITHHELD), (&reply, POST)]));
        db.drop_db().await;
    }

    /// B-41: the endpoints over stored rows.
    #[tokio::test]
    async fn v2_over_local_rows() {
        let Some(db) = fresh_db("hydration_thread_v2").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(OP, "op.test"), (BOB, "bob.test"), (ME, "me.test")] {
            actor(p, d, h, h).await;
        }
        let root = post(p, OP, "3kaaaaaaaaaa2", "root", None, None).await;
        let second = post(p, OP, "3kaaaaaaaaaa3", "second", Some(&root), Some(&root)).await;
        let reply = post(p, BOB, "3kaaaaaaaaaa4", "reply", Some(&second), Some(&root)).await;
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{ME}', '{BOB}')")).await;
        let mut sets = ScopeSets::default();
        for d in [OP, BOB, ME] {
            sets.scope.insert(d.into());
        }
        let h = hydrator(p, sets);
        let (anon, req) = (Viewer::anonymous(), Requester::test());
        let options = opts(DEFAULT_BELOW, DEFAULT_BRANCHING, Sort::Oldest);

        let v = h.get_post_thread_v2(&anon, &req, &second, &options).await.expect("db").expect("view");
        let v = v.inner();
        assert_eq!(shape(v["thread"].as_array().expect("thread")), pairs(&[(root.as_str(), -1), (second.as_str(), 0), (reply.as_str(), 1)]));
        assert_eq!(v["hasOtherReplies"], false);
        assert_eq!(v["thread"][1]["value"]["opThreadPostCount"], 2);
        assert_eq!(v["thread"][1]["value"]["post"]["record"]["text"], "second");

        // The muting viewer: the reply moves to "other".
        let me = Viewer {
            did: Some(ME.into()),
            hydrate: true,
        };
        // The author turned quoting off on `second`: every viewer but the
        // author is told. A postgate under another record key is ignored.
        for (rkey, target) in [("3kaaaaaaaaaa3", &second), ("elsewhere", &root)] {
            sqlx::query(
                "INSERT INTO postgates (uri, author_did, rkey, cid, rev, post_uri, embedding_rules, record, created_at) \
                 VALUES ($1, $2, $3, 'bafypg', '3k', $4, '[{\"$type\": \"app.bsky.feed.postgate#disableRule\"}]', '{}', now())",
            )
            .bind(format!("at://{OP}/app.bsky.feed.postgate/{rkey}"))
            .bind(OP)
            .bind(rkey)
            .bind(target)
            .execute(p)
            .await
            .expect("postgate");
        }
        let v = h.get_post_thread_v2(&me, &req, &second, &options).await.expect("db").expect("view");
        assert_eq!(v.inner()["thread"][1]["value"]["post"]["viewer"]["embeddingDisabled"], true);
        assert!(v.inner()["thread"][0]["value"]["post"]["viewer"].get("embeddingDisabled").is_none());
        let author = Viewer {
            did: Some(OP.into()),
            hydrate: true,
        };
        let own = h.get_post_thread_v2(&author, &req, &second, &options).await.expect("db").expect("view");
        assert!(own.inner()["thread"][1]["value"]["post"]["viewer"].get("embeddingDisabled").is_none());
        assert_eq!(v.inner()["thread"].as_array().map(Vec::len), Some(2));
        assert_eq!(v.inner()["hasOtherReplies"], true);
        let o = h.get_post_thread_other_v2(&me, &req, &second).await.expect("db").expect("view");
        assert_eq!(shape(o.inner()["thread"].as_array().expect("thread")), pairs(&[(reply.as_str(), 1)]));
        let o = h.get_post_thread_other_v2(&anon, &req, &second).await.expect("db").expect("view");
        assert_eq!(o.inner()["thread"], json!([]));

        // A miss is an item, the same for a post that never existed and a
        // private-tier URI; "other" is empty.
        for missing in [format!("at://{OP}/app.bsky.feed.post/3knope"), format!("at://{OP}/tools.kryphocron.feed.postPrivate/3kaaaaaaaaaa2")] {
            let v = h.get_post_thread_v2(&anon, &req, &missing, &options).await.expect("db").expect("view");
            assert_eq!(
                v.inner(),
                &json!({"hasOtherReplies": false, "thread": [{"uri": missing, "depth": 0, "value": {"$type": "app.bsky.unspecced.defs#threadItemNotFound"}}]})
            );
            let o = h.get_post_thread_other_v2(&anon, &req, &missing).await.expect("db").expect("view");
            assert_eq!(o.inner()["thread"], json!([]));
        }

        // The root's threadgate hides the reply: gone from the thread, in
        // "other" with its flag, and still shown to its own author. A gate
        // whose record key is not the post's is ignored.
        let gate = format!("at://{OP}/app.bsky.feed.threadgate/3kaaaaaaaaaa2");
        for (uri, rkey, hides) in [(gate.as_str(), "3kaaaaaaaaaa2", reply.as_str()), ("at://x/app.bsky.feed.threadgate/other", "other", second.as_str())] {
            sqlx::query(
                "INSERT INTO threadgates (uri, author_did, rkey, cid, rev, post_uri, allow, hidden_replies, record, created_at) \
                 VALUES ($1, $2, $3, 'bafygate', '3k', $4, '[]', $5, '{\"$type\": \"app.bsky.feed.threadgate\"}', now())",
            )
            .bind(uri)
            .bind(OP)
            .bind(rkey)
            .bind(&root)
            .bind(vec![hides.to_string()])
            .execute(p)
            .await
            .expect("gate");
        }
        let v = h.get_post_thread_v2(&anon, &req, &second, &options).await.expect("db").expect("view");
        assert_eq!(shape(v.inner()["thread"].as_array().expect("thread")), pairs(&[(root.as_str(), -1), (second.as_str(), 0)]));
        assert_eq!(v.inner()["hasOtherReplies"], true);
        assert_eq!((&v.inner()["threadgate"]["uri"], &v.inner()["threadgate"]["lists"]), (&json!(gate), &json!([])));
        // The root's own post view carries its gate; a reply's does not.
        assert_eq!(v.inner()["thread"][0]["value"]["post"]["threadgate"]["uri"], json!(gate));
        assert!(v.inner()["thread"][1]["value"]["post"].get("threadgate").is_none());
        let o = h.get_post_thread_other_v2(&anon, &req, &second).await.expect("db").expect("view");
        let hidden_item = &o.inner()["thread"][0]["value"];
        assert_eq!((&hidden_item["hiddenByThreadgate"], &hidden_item["mutedByViewer"]), (&json!(true), &json!(false)));
        let v1 = h.get_post_thread(&anon, &req, &second, 6, 80).await.expect("db").expect("view");
        assert_eq!(v1.inner()["thread"]["replies"], json!([]));
        assert_eq!(v1.inner()["threadgate"]["cid"], "bafygate");
        assert!(!v1.inner().to_string().contains("hiddenByThreadgate"));
        let author = Viewer {
            did: Some(BOB.into()),
            hydrate: true,
        };
        let v = h.get_post_thread_v2(&author, &req, &second, &options).await.expect("db").expect("view");
        assert_eq!(v.inner()["thread"].as_array().map(Vec::len), Some(3));

        // `viewer.replyDisabled` on the anchor, from the root's gate.
        let disabled = |who: Viewer| {
            let (h, req, second, options) = (&h, &req, &second, &options);
            async move {
                let v = h.get_post_thread_v2(&who, req, second, options).await.expect("db").expect("view");
                v.inner()["thread"][1]["value"]["post"]["viewer"].get("replyDisabled").cloned()
            }
        };
        let op = Viewer {
            did: Some(OP.into()),
            hydrate: true,
        };
        // `allow = []`: nobody but the root's author.
        assert_eq!(disabled(me.clone()).await, Some(json!(true)));
        assert_eq!(disabled(op).await, Some(json!(false)));
        assert_eq!(disabled(anon.clone()).await, None);
        // `getPosts` carries it too, for the root and a reply alike.
        let both = [root.clone(), second.clone()];
        let v = h.get_posts(&me, &req, &both).await.expect("db").expect("view");
        for i in 0..2 {
            assert_eq!(v.inner()["posts"][i]["viewer"]["replyDisabled"], true);
        }
        let v = h.get_posts(&anon, &req, &both).await.expect("db").expect("view");
        assert!(v.inner()["posts"][0]["viewer"].get("replyDisabled").is_none());
        // Followers may reply: not a follower, then a follower.
        exec(p, &format!("UPDATE threadgates SET allow = '[{{\"$type\": \"app.bsky.feed.threadgate#followerRule\"}}]' WHERE uri = '{gate}'")).await;
        assert_eq!(disabled(me.clone()).await, Some(json!(true)));
        exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{ME}/app.bsky.graph.follow/1', '{ME}', '{OP}', '3k', now())")).await;
        assert_eq!(disabled(me.clone()).await, Some(json!(false)));
        // A rule whose input this AppView does not hold in full: unknown.
        exec(p, &format!("UPDATE threadgates SET allow = '[{{\"$type\": \"app.bsky.feed.threadgate#listRule\", \"list\": \"at://{BOB}/app.bsky.graph.list/l\"}}]' WHERE uri = '{gate}'")).await;
        assert_eq!(disabled(me.clone()).await, None);
        // No `allow` at all: anyone may reply, and the field is left out.
        exec(p, &format!("UPDATE threadgates SET allow = NULL WHERE uri = '{gate}'")).await;
        assert_eq!(disabled(me.clone()).await, None);

        // `viewer.knownLikers` on the anchor: likers the viewer follows.
        // The viewer follows the OP (above), who liked the post; Bob liked
        // it too, and the viewer does not follow Bob.
        for (who, n) in [(OP, 1), (BOB, 2)] {
            exec(p, &format!(
                "INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES \
                 ('at://{who}/app.bsky.feed.like/{n}', '{who}', '{second}', 'bafy', '3k', now())"
            )).await;
        }
        let v = h.get_post_thread_v2(&me, &req, &second, &options).await.expect("db").expect("view");
        let known = &v.inner()["thread"][1]["value"]["post"]["viewer"]["knownLikers"];
        assert_eq!((&known["count"], &known["actors"][0]["did"]), (&json!(1), &json!(OP)));
        // Only on the anchor, and only for a ready member.
        assert!(v.inner()["thread"][0]["value"]["post"]["viewer"].get("knownLikers").is_none());
        let v = h.get_post_thread_v2(&anon, &req, &second, &options).await.expect("db").expect("view");
        assert!(v.inner()["thread"][1]["value"]["post"]["viewer"].get("knownLikers").is_none());
        db.drop_db().await;
    }
}
