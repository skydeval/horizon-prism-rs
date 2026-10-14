//! `getThread` assembly (design-v3 §5.15.7 – §5.15.14).
//!
//! Bluesky treats a block as nuclear: the posts above and below a blocked
//! post disappear for the viewer too. Thread Lens makes a block hide one
//! author's posts and nothing else. It never shows a post by an author the
//! viewer has a block with, in either direction, and it adds to the
//! ordinary thread view only what it can check.
//!
//! 1. **Baseline.** The ordinary thread view for this viewer and anchor,
//!    never cut. Its authors are judged in one round, which also decides
//!    whether bridging is on (`verdict`). The view is then built with the
//!    request's blocks applied.
//! 2. **Bridging off.** The response is the baseline.
//! 3. **Bridging on.** In this order, within `threadlens.page_nodes` added
//!    nodes: the anchor (when the baseline is empty) and the baseline's
//!    stubs; the walk up the parents; the replies, level by level. An
//!    author's verdict comes from the URI, before anything is fetched:
//!    `Clear` enters as a post, a block as a gap the walks go through, and
//!    an author the server cannot check does not enter, with everything
//!    reachable only through it.
//! 4. **Enforcement.** One pass over every view sent: a post by a blocked
//!    author is a gap, and an embedded record by one is `#viewBlocked`.
//!
//! Where the work stopped for a bound, the node says so with a marker, and
//! the client asks again from there.
//!
//! Output order: the parent chain (root-most first), the anchor, then
//! replies breadth-first.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::StreamExt;
use serde_json::{Value, json};

use super::cursor::{CursorKey, Position};
use super::gaps::Gap;
use super::ladder::{Ladder, Mode, Resolved};
use super::verdict::{Judge, Verdict};
use crate::config::ThreadLensConfig;
use crate::db::locks::uri_author;
use crate::farsight::{Farsight, Lookup};
use crate::hydration::lens::{Bridging, LensNode, named_accounts};
use crate::hydration::normalize::is_private_uri;
use crate::hydration::{HydrationError, KnownBlocks, Viewer, with_known_blocks};
use crate::xrpc::pools::Requester;

/// Ladder runs in flight.
pub const LADDER_CONCURRENCY: usize = 8;
/// Replies read of one node's sequence in each of its two phases.
const LISTING_PAGE: u32 = 100;
const NSID_POST: &str = "app.bsky.feed.post";

/// One `getThread` request, after its input was checked.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    /// The caller: a ready member (`hydrate` true), or a visitor.
    pub viewer: &'a Viewer,
    pub req: &'a Requester,
    pub anchor: &'a str,
    pub depth: u32,
    pub parent_height: u32,
}

/// What a `getThread` response holds.
#[derive(Debug, Clone, PartialEq)]
pub struct Thread {
    pub nodes: Vec<LensNode>,
    pub bridging: Bridging,
}

/// `at://<did>/app.bsky.feed.post/<rkey>`.
#[must_use]
pub fn is_post_uri(uri: &str) -> bool {
    let Some(rest) = uri.strip_prefix("at://") else {
        return false;
    };
    let mut parts = rest.split('/');
    let (Some(did), Some(collection), Some(rkey), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    proto_blue::syntax::Did::is_valid(did) && collection == NSID_POST && !rkey.is_empty()
}

/// The gap a URI is by its form alone (§5.15.15 precedence 1 and 2): a
/// private-tier URI, or one that is not a post's. No verdict is computed
/// and nothing is looked up for either.
#[must_use]
pub fn gap_by_form(uri: &str) -> Option<Gap> {
    if is_private_uri(uri) {
        Some(Gap::TierHidden)
    } else if !is_post_uri(uri) {
        Some(Gap::NotFound)
    } else {
        None
    }
}

/// The request's handle on Farsight. For a caller that is not a ready
/// member it is a visitor's handle: nothing is remembered or recalled for
/// it, and its calls draw on the non-member pool and never probe. The mark
/// comes from the viewer, never from the charge key: a ready member can be
/// charged by address, and that must not switch its memory off.
fn lens_lookup(farsight: &Farsight, viewer: &Viewer, req: &Requester) -> Lookup {
    let lookup = farsight.request(req);
    if !viewer.hydrate {
        lookup.set_visitor();
    }
    lookup
}

fn block_gap(verdict: Option<Verdict>) -> Option<Gap> {
    match verdict {
        Some(Verdict::BlockedByYou) => Some(Gap::BlockedByYou),
        Some(Verdict::BlockingYou) => Some(Gap::BlockingYou),
        _ => None,
    }
}

fn str_at(v: &Value, pointer: &str) -> Option<String> {
    v.pointer(pointer).and_then(Value::as_str).map(str::to_string)
}

/// One node of the ordinary view's tree as a lens node.
fn baseline_node(v: &Value, parent_uri: Option<String>, judge: &Judge) -> Option<LensNode> {
    let ty = v.get("$type").and_then(Value::as_str).unwrap_or_default();
    if ty == "app.bsky.feed.defs#threadViewPost" || v.get("post").is_some() {
        let mut node = LensNode::post(v.get("post")?.clone());
        if let LensNode::Post { parent_uri: own, .. } = &mut node
            && own.is_none()
        {
            *own = parent_uri;
        }
        return (!node.uri().is_empty()).then_some(node);
    }
    let uri = str_at(v, "/uri")?;
    let author = uri_author(&uri).unwrap_or_default();
    let gap = if ty == "app.bsky.feed.defs#blockedPost" || v.get("blocked").is_some() {
        // A stub the viewer is a party to says which way; any other is a
        // block between other people.
        block_gap(judge.verdict(author)).unwrap_or(Gap::Blocked)
    } else {
        Gap::NotFound
    };
    Some(LensNode::gap(&uri, parent_uri, gap))
}

/// The ordinary view's tree, flattened: parents root-most first, the
/// anchor, replies breadth-first. The second value is each reply's level
/// below the anchor.
fn flatten(tree: &Value, judge: &Judge) -> (Vec<LensNode>, HashMap<String, u32>) {
    let mut chain: Vec<&Value> = Vec::new();
    let mut cur = tree.get("parent");
    while let Some(p) = cur.filter(|p| p.is_object()) {
        chain.push(p);
        cur = p.get("parent");
    }
    chain.reverse();
    let mut nodes = Vec::new();
    let mut levels = HashMap::new();
    let mut above: Option<String> = None;
    for p in chain {
        if let Some(n) = baseline_node(p, above.clone(), judge) {
            above = Some(n.uri().to_string());
            nodes.push(n);
        }
    }
    let Some(anchor) = baseline_node(tree, above, judge) else {
        return (Vec::new(), levels);
    };
    let anchor_uri = anchor.uri().to_string();
    levels.insert(anchor_uri.clone(), 0);
    nodes.push(anchor);
    let children = |v: &'_ Value| v.get("replies").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut level: Vec<(Value, String)> = children(tree).into_iter().map(|r| (r, anchor_uri.clone())).collect();
    let mut depth = 1;
    while !level.is_empty() {
        let mut next = Vec::new();
        for (r, parent) in level {
            if let Some(n) = baseline_node(&r, Some(parent), judge) {
                let uri = n.uri().to_string();
                next.extend(children(&r).into_iter().map(|c| (c, uri.clone())));
                levels.insert(uri, depth);
                nodes.push(n);
            }
        }
        level = next;
        depth += 1;
    }
    (nodes, levels)
}

/// `#viewBlocked` for an embedded record by a blocked author: the URI,
/// the author's DID and which way the block runs. Nothing else.
fn view_blocked(uri: &str, did: &str, judge: &Judge) -> Value {
    let (inbound, _) = judge.directions(did);
    json!({
        "$type": "app.bsky.embed.record#viewBlocked",
        "uri": uri,
        "blocked": true,
        "author": {"did": did, "viewer": {"blockedBy": inbound}},
    })
}

/// Replace, anywhere inside `v`, an embedded record whose author or
/// creator the viewer has a block with (§5.15.12): a `#viewRecord` at any
/// depth, and the generator, list, labeler and starter-pack cards, which
/// carry a `creator`.
fn enforce_embeds(v: &mut Value, judge: &Judge) {
    match v {
        Value::Array(items) => items.iter_mut().for_each(|item| enforce_embeds(item, judge)),
        Value::Object(m) => {
            let ty = m.get("$type").and_then(Value::as_str).unwrap_or_default();
            let embedded = matches!(
                ty,
                "app.bsky.embed.record#viewRecord"
                    | "app.bsky.feed.defs#generatorView"
                    | "app.bsky.graph.defs#listView"
                    | "app.bsky.labeler.defs#labelerView"
                    | "app.bsky.graph.defs#starterPackViewBasic"
            );
            let owner = ["author", "creator"]
                .into_iter()
                .find_map(|k| m.get(k).and_then(|a| a.get("did")).and_then(Value::as_str))
                .map(str::to_string);
            if embedded
                && let Some(did) = owner
                && block_gap(judge.verdict(&did)).is_some()
            {
                let uri = m.get("uri").and_then(Value::as_str).unwrap_or_default().to_string();
                *v = view_blocked(&uri, &did, judge);
                return;
            }
            m.values_mut().for_each(|child| enforce_embeds(child, judge));
        }
        _ => {}
    }
}

/// One reply read of a node's sequence.
struct Reply {
    uri: String,
    /// It has a row here.
    local_row: bool,
    /// The sequence position before it: where a cursor points when this
    /// is the first reply a response did not place. `None` is the start.
    before: Option<Position>,
}

/// One node's sequence as read.
struct Seq {
    parent: String,
    replies: Vec<Reply>,
    /// Where reading stopped short of the sequence's end, if it did: the
    /// position to resume from (`None` inside: from the start).
    rest: Option<Option<Position>>,
}

/// What to do with a reply that was read.
enum Act {
    Gap(Gap),
    Fetch,
    Skip,
}

struct Work<'a> {
    ladder: &'a Ladder,
    r: Request<'a>,
    judge: Judge,
    /// The request's blocks as of the last round, for the views it builds.
    known: Arc<KnownBlocks>,
    nodes: Vec<LensNode>,
    seen: HashSet<String>,
    /// Nodes this response may still add beyond the baseline.
    budget: usize,
    /// Reply sequences it may still start, and Constellation pages it may
    /// still fetch, counted together.
    listings: u32,
    parent_rounds: u32,
    /// A candidate was held back because its author could not be checked.
    limited: bool,
    /// URIs of post views fetched by bridging, whose embedded authors are
    /// judged before the response goes out.
    bridged: Vec<String>,
    key: &'a CursorKey,
    /// Whether bridging is on for the request.
    bridging: bool,
    /// The position a `repliesCursor` request resumes the anchor's
    /// sequence from, and the cursor as it was sent.
    start: Option<(Position, String)>,
}

impl Work<'_> {
    fn index(&self, uri: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.uri() == uri)
    }

    fn refresh_known(&mut self) {
        self.known = Arc::new(self.judge.known_blocks());
    }

    /// The ladder for one URI, hydrated with the request's blocks.
    async fn fetch(&self, uri: &str) -> Result<Resolved, sqlx::Error> {
        with_known_blocks(Arc::clone(&self.known), self.ladder.resolve(self.r.viewer, self.r.req, uri, Mode::Full)).await
    }

    fn ladder_node(&mut self, resolved: Resolved, uri: &str, parent_uri: Option<String>) -> LensNode {
        match resolved {
            Resolved::Post(post) => {
                self.bridged.push(uri.to_string());
                let mut node = LensNode::post(post);
                if let LensNode::Post { parent_uri: own, .. } = &mut node
                    && own.is_none()
                {
                    *own = parent_uri;
                }
                node
            }
            Resolved::Gap(gap) => LensNode::gap(uri, parent_uri, gap),
        }
    }

    /// The anchor of an empty baseline (§5.15.10 "The anchor comes
    /// first"). Returns whether anything may attach to it.
    async fn anchor_alone(&mut self) -> Result<bool, sqlx::Error> {
        let anchor = self.r.anchor;
        let verdict = self.judge.verdict(uri_author(anchor).unwrap_or_default());
        let node = match (block_gap(verdict), verdict) {
            (Some(gap), _) => LensNode::gap(anchor, None, gap),
            (None, Some(Verdict::Clear)) if self.judge.bridging() => {
                let resolved = self.fetch(anchor).await?;
                self.ladder_node(resolved, anchor, None)
            }
            _ => {
                self.limited = true;
                LensNode::gap(anchor, None, Gap::BlockStateUnknown)
            }
        };
        let attach = node.walkable();
        self.seen.insert(anchor.to_string());
        self.nodes.push(node);
        Ok(attach)
    }

    /// The baseline's stubs (§5.15.9): a `blocked` or `not-found` gap
    /// whose author is `Clear` goes through the ladder and, if it is a
    /// post, replaces the stub in place. That does not count toward the
    /// page budget.
    async fn stubs(&mut self) -> Result<(), sqlx::Error> {
        let stubs: Vec<(usize, String)> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(i, n)| match n {
                LensNode::Gap { uri, gap: Gap::Blocked | Gap::NotFound, .. } if gap_by_form(uri).is_none() => Some((i, uri.clone())),
                _ => None,
            })
            .collect();
        for (i, uri) in stubs {
            match self.judge.verdict(uri_author(&uri).unwrap_or_default()) {
                Some(Verdict::Clear) => {
                    let parent = self.nodes[i].parent_uri().map(str::to_string);
                    if let Resolved::Post(post) = self.fetch(&uri).await? {
                        self.nodes[i] = self.ladder_node(Resolved::Post(post), &uri, parent);
                    }
                }
                Some(Verdict::Undetermined) | None => self.limited = true,
                Some(Verdict::BlockedByYou | Verdict::BlockingYou) => {}
            }
        }
        Ok(())
    }

    /// The parent of the chain's top node, when the walk may name it: a
    /// post's own reply ref, or, above a known-block gap, the parent a
    /// local row names. It is never fetched from a source for a gap, since
    /// that would fetch the blocked author's record.
    async fn parent_of_top(&self) -> Result<Option<String>, sqlx::Error> {
        Ok(match self.nodes.first() {
            Some(LensNode::Post { parent_uri, .. }) => parent_uri.clone(),
            Some(LensNode::Gap { uri, gap, parent_uri, .. }) if gap.known_block() => match parent_uri {
                Some(p) => Some(p.clone()),
                None => self.ladder.hydrator().lens_local_rows(std::slice::from_ref(uri)).await?.remove(uri).flatten(),
            },
            _ => None,
        })
    }

    /// The walk up the parents (§5.15.11): one step at a time from the
    /// top of the chain, through known-block gaps, until the root, a URI
    /// already seen, a gap the ladder classifies, an author that cannot be
    /// checked, or a bound. A bound leaves `moreParents` on the top node.
    async fn parents(&mut self) -> Result<(), sqlx::Error> {
        loop {
            let Some(parent) = self.parent_of_top().await? else {
                return Ok(());
            };
            if self.seen.contains(&parent) {
                return Ok(());
            }
            let chain = self.index(self.r.anchor).unwrap_or(0);
            let bound = chain >= usize::try_from(self.r.parent_height).unwrap_or(usize::MAX) || self.budget == 0 || self.parent_rounds == 0;
            let author = uri_author(&parent).unwrap_or_default().to_string();
            let judged = if bound || gap_by_form(&parent).is_some() { !bound } else {
                self.parent_rounds -= 1;
                let judged = self.judge.round(std::slice::from_ref(&author)).await?;
                self.refresh_known();
                judged
            };
            if !judged {
                if let Some(top) = self.nodes.first_mut() {
                    top.markers_mut().more_parents = true;
                }
                return Ok(());
            }
            let verdict = self.judge.verdict(&author);
            let node = match (gap_by_form(&parent), block_gap(verdict), verdict) {
                (Some(gap), ..) => LensNode::gap(&parent, None, gap),
                (None, Some(gap), _) => LensNode::gap(&parent, None, gap),
                (None, None, Some(Verdict::Clear)) => {
                    let resolved = self.fetch(&parent).await?;
                    self.ladder_node(resolved, &parent, None)
                }
                _ => {
                    self.limited = true;
                    LensNode::gap(&parent, None, Gap::BlockStateUnknown)
                }
            };
            // The node below now has its parent in the response.
            if let Some(LensNode::Gap { parent_uri, .. }) = self.nodes.first_mut()
                && parent_uri.is_none()
            {
                *parent_uri = Some(parent.clone());
            }
            let goes_on = node.walkable();
            self.seen.insert(parent);
            self.nodes.insert(0, node);
            self.budget -= 1;
            if !goes_on {
                return Ok(());
            }
        }
    }

    /// One node's reply sequence from `from` (§5.15.14): the stored
    /// replies, then the ones Constellation lists that are not stored.
    /// The Constellation phase is read only with bridging on.
    async fn sequence(&mut self, parent: &str, from: Option<Position>) -> Result<Seq, sqlx::Error> {
        let h = self.ladder.hydrator();
        let page = usize::try_from(LISTING_PAGE).unwrap_or(usize::MAX);
        let mut replies: Vec<Reply> = Vec::new();
        let seq = |replies, rest| Seq { parent: parent.to_string(), replies, rest };
        let listed_from = match &from {
            Some(Position::Listed { cursor }) => cursor.clone(),
            local => {
                let after = match local {
                    Some(Position::Local { at, uri }) => Some((*at, uri.as_str())),
                    _ => None,
                };
                let rows = h.lens_local_replies(parent, after, LISTING_PAGE + 1).await?;
                let more = rows.len() > page;
                let mut before = from.clone();
                for (uri, at) in rows.into_iter().take(page) {
                    replies.push(Reply { uri: uri.clone(), local_row: true, before: before.clone() });
                    before = Some(Position::Local { at, uri });
                }
                if more {
                    return Ok(seq(replies, Some(before)));
                }
                None
            }
        };
        if !self.bridging {
            // In this state the stored replies are all there is to read. A
            // cursor already in the Constellation phase cannot be read
            // from at all: the marker keeps it.
            let kept = matches!(from, Some(Position::Listed { .. })).then(|| from.clone());
            return Ok(seq(replies, kept));
        }
        let here = Position::Listed { cursor: listed_from.clone() };
        if self.listings == 0 {
            return Ok(seq(replies, Some(Some(here))));
        }
        self.listings -= 1;
        let rest = match h.lens_listed_replies(self.r.req, parent, LISTING_PAGE, listed_from.as_deref()).await {
            // No answer: the marker keeps the cursor it had.
            None => Some(Some(here)),
            Some((listed, next)) => {
                let stored = h.lens_local_rows(&listed).await?;
                for uri in listed {
                    if !stored.contains_key(&uri) && !replies.iter().any(|r| r.uri == uri) {
                        replies.push(Reply { uri, local_row: false, before: Some(here.clone()) });
                    }
                }
                next.map(|cursor| Some(Position::Listed { cursor: Some(cursor) }))
            }
        };
        Ok(seq(replies, rest))
    }

    /// The replies, level by level below the anchor and node by node in
    /// response order (§5.15.14). The authors of every reply read for a
    /// level are judged in one round before any is fetched or placed.
    ///
    /// With bridging off this runs only for a continuation, and reads the
    /// anchor's stored replies alone: they are not expanded.
    async fn replies(&mut self) -> Result<(), sqlx::Error> {
        let mut level: Vec<String> = vec![self.r.anchor.to_string()];
        let mut start = self.start.as_ref().map(|(position, _)| position.clone());
        let levels = if self.bridging { self.r.depth } else { self.r.depth.min(1) };
        for _ in 0..levels {
            let mut read: Vec<Seq> = Vec::new();
            for parent in &level {
                let Some(node) = self.index(parent).map(|i| &self.nodes[i]) else {
                    continue;
                };
                if !node.walkable() || node.no_replies() {
                    continue;
                }
                let from = start.take();
                if self.listings == 0 || self.budget == 0 {
                    self.mark_at(parent, from);
                    continue;
                }
                self.listings -= 1;
                read.push(self.sequence(parent, from).await?);
            }
            let authors: Vec<String> = read
                .iter()
                .flat_map(|seq| seq.replies.iter())
                .filter(|reply| !self.seen.contains(&reply.uri))
                .filter_map(|reply| uri_author(&reply.uri).map(str::to_string))
                .collect();
            let judged = self.judge.round(&authors).await?;
            self.refresh_known();
            let parents: Vec<String> = read.iter().map(|seq| seq.parent.clone()).collect();
            let local_parents = self.ladder.hydrator().lens_local_rows(&parents).await?;
            for seq in read {
                // A reply whose author could not be sent in the round is
                // not placed: the cursor is the position before it.
                let stopped = if judged {
                    self.place(&seq, local_parents.contains_key(&seq.parent)).await?
                } else {
                    seq.replies.iter().position(|reply| !self.seen.contains(&reply.uri))
                };
                let resume = match stopped {
                    Some(i) => Some(seq.replies[i].before.clone()),
                    None => seq.rest.clone(),
                };
                if let Some(position) = resume {
                    self.mark_at(&seq.parent, position);
                }
            }
            let mut next: Vec<String> = Vec::new();
            for parent in &level {
                next.extend(self.nodes.iter().filter(|n| n.parent_uri() == Some(parent.as_str()) && n.walkable()).map(|n| n.uri().to_string()));
            }
            level = next;
            if level.is_empty() {
                return Ok(());
            }
        }
        // The nodes at the boundary: their sequences were not started. A
        // request with `depth: 0` marks nothing on its anchor.
        if self.r.depth > 0 {
            for uri in level {
                if self.index(&uri).is_some_and(|i| !self.nodes[i].no_replies() && self.nodes[i].markers().more_replies.is_none()) {
                    self.mark_at(&uri, None);
                }
            }
        }
        Ok(())
    }

    /// Mark `uri`'s replies as not read to their end, to be resumed at
    /// `position` (`None`: from the start, with no cursor). A position
    /// that is the one this request came in with keeps the cursor as it
    /// was sent, so a client can see that nothing moved.
    fn mark_at(&mut self, uri: &str, position: Option<Position>) {
        let cursor = match (&position, &self.start) {
            (Some(p), Some((came_with, raw))) if p == came_with && uri == self.r.anchor => Some(raw.clone()),
            (Some(p), _) => self.key.seal(uri, p),
            (None, _) => None,
        };
        if let Some(i) = self.index(uri) {
            self.nodes[i].markers_mut().more_replies = Some(cursor);
        }
    }

    /// Place the replies read for a node that the response does not hold
    /// yet. Returns the index of the first reply that was not placed for
    /// the page budget, if any.
    ///
    /// A stored reply to a node that was itself built from a stored row is
    /// one of the ordinary view's own rows: it is shown without a `Clear`
    /// verdict. A known block makes it a gap in its place with bridging
    /// on, and leaves it out with bridging off. Every other reply is a
    /// bridge candidate, and enters only with bridging on.
    async fn place(&mut self, seq: &Seq, local_parent: bool) -> Result<Option<usize>, sqlx::Error> {
        let mut acts: Vec<(&str, Act)> = Vec::new();
        let mut stopped = None;
        for (i, reply) in seq.replies.iter().enumerate() {
            if self.seen.contains(&reply.uri) {
                continue;
            }
            let verdict = self.judge.verdict(uri_author(&reply.uri).unwrap_or_default());
            let own_row = reply.local_row && local_parent;
            let act = match (gap_by_form(&reply.uri), block_gap(verdict), verdict) {
                (Some(_), ..) => Act::Skip,
                (None, Some(gap), _) if self.bridging => Act::Gap(gap),
                (None, Some(_), _) => Act::Skip,
                (None, None, _) if own_row => Act::Fetch,
                (None, None, Some(Verdict::Clear)) if self.bridging => Act::Fetch,
                _ => {
                    self.limited |= self.bridging;
                    Act::Skip
                }
            };
            if matches!(act, Act::Skip) {
                continue;
            }
            if acts.len() >= self.budget {
                stopped = Some(i);
                break;
            }
            acts.push((&reply.uri, act));
        }
        let fetches: Vec<String> = acts.iter().filter(|(_, act)| matches!(act, Act::Fetch)).map(|(uri, _)| (*uri).to_string()).collect();
        let this = &*self;
        let mut fetched: HashMap<String, Resolved> = futures::stream::iter(fetches)
            .map(|uri| async move {
                let resolved = this.fetch(&uri).await;
                resolved.map(|r| (uri, r))
            })
            .buffered(LADDER_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        for (uri, act) in acts {
            let node = match act {
                Act::Gap(gap) => LensNode::gap(uri, Some(seq.parent.clone()), gap),
                Act::Fetch => match fetched.remove(uri) {
                    Some(resolved) => self.ladder_node(resolved, uri, Some(seq.parent.clone())),
                    None => continue,
                },
                Act::Skip => continue,
            };
            self.seen.insert(uri.to_string());
            self.nodes.push(node);
            self.budget = self.budget.saturating_sub(1);
        }
        Ok(stopped)
    }

    /// Enforcement (§5.15.12): one pass over every view the response
    /// holds, after the authors that bridged views embed were judged.
    async fn enforce(&mut self) -> Result<(), sqlx::Error> {
        let embedded: Vec<String> = self
            .nodes
            .iter()
            .filter_map(|n| match n {
                LensNode::Post { uri, post, .. } if self.bridged.contains(uri) => Some(named_accounts(post)),
                _ => None,
            })
            .flatten()
            .collect();
        if !embedded.is_empty() {
            // Over the allowance, the authors are not judged: an embedded
            // record by one is left as its view has it, as for an author
            // that cannot be checked.
            self.judge.round(&embedded).await?;
        }
        let judge = &self.judge;
        for node in &mut self.nodes {
            let LensNode::Post { uri, parent_uri, post, markers, .. } = node else {
                continue;
            };
            let author = uri_author(uri).unwrap_or_default();
            if let Some(gap) = block_gap(judge.verdict(author)) {
                *node = LensNode::Gap {
                    uri: uri.clone(),
                    parent_uri: parent_uri.clone(),
                    gap,
                    markers: markers.clone(),
                };
            } else if let Some(embed) = post.get_mut("embed") {
                enforce_embeds(embed, judge);
            }
        }
        Ok(())
    }
}

/// The nodes for a `getThread` response.
///
/// # Errors
///
/// Database errors.
pub async fn get_thread(
    ladder: &Ladder,
    cfg: &ThreadLensConfig,
    key: &CursorKey,
    r: Request<'_>,
    cursor: Option<(Position, String)>,
) -> Result<Thread, HydrationError> {
    // Decided by the URI's form: one node, no round, no call.
    if let Some(gap) = gap_by_form(r.anchor) {
        return Ok(Thread {
            nodes: vec![LensNode::gap(r.anchor, None, gap)],
            bridging: Bridging::Off,
        });
    }
    let h = ladder.hydrator();
    let me = r.viewer.did.clone().unwrap_or_default();
    let lookup = h.farsight().map(|farsight| lens_lookup(farsight, r.viewer, r.req));
    let mut judge = Judge::new(h.pool().clone(), &me, lookup, cfg);

    // A request with a cursor evaluates its anchor as any request does,
    // but builds no parent chain and collects no replies again: the
    // ordinary assembly runs for the anchor alone.
    let (depth, parent_height) = if cursor.is_some() { (0, 0) } else { (r.depth, r.parent_height) };
    // Collect: the ordinary view, with no Thread Lens rule applied, names
    // the authors. Verdicts: one round for all of them. Emit: the ordinary
    // view again, with the request's blocks.
    let collected = h.lens_baseline(r.viewer, r.req, r.anchor, depth, parent_height).await?;
    let mut authors: Vec<String> = collected.as_ref().map(named_accounts).unwrap_or_default().into_iter().collect();
    authors.extend(uri_author(r.anchor).map(str::to_string));
    judge.baseline(&authors).await?;
    let known = Arc::new(judge.known_blocks());
    let tree = match collected {
        Some(_) => with_known_blocks(Arc::clone(&known), h.lens_baseline(r.viewer, r.req, r.anchor, depth, parent_height)).await?,
        None => None,
    };
    let (nodes, levels) = tree.as_ref().map(|tree| flatten(tree, &judge)).unwrap_or_default();
    let bridging = judge.bridging();
    let mut work = Work {
        ladder,
        r,
        judge,
        known,
        seen: nodes.iter().map(|n| n.uri().to_string()).collect(),
        nodes,
        budget: usize::try_from(cfg.page_nodes).unwrap_or(usize::MAX),
        listings: cfg.listing_calls,
        parent_rounds: cfg.parent_rounds,
        limited: false,
        bridged: Vec::new(),
        key,
        bridging,
        start: cursor,
    };
    let attach = if work.nodes.is_empty() { work.anchor_alone().await? } else { true };
    if work.start.is_some() {
        // The anchor's sequence from the cursor. A post's stored replies
        // can be read in either state; anything else needs bridging. Where
        // nothing can be read, the anchor keeps the cursor it came with.
        let is_post = matches!(work.nodes.last(), Some(LensNode::Post { .. }));
        if attach && (bridging || is_post) {
            work.replies().await?;
        } else if attach {
            let from = work.start.as_ref().map(|(position, _)| position.clone());
            work.mark_at(r.anchor, from);
        }
    } else if bridging && attach {
        work.stubs().await?;
        work.parents().await?;
        work.replies().await?;
    } else if !bridging {
        // The response is the baseline. It says where the ordinary view
        // stopped: at `parentHeight`, and at the `depth` boundary.
        let chain = work.index(r.anchor).unwrap_or(0);
        let parent_height = usize::try_from(r.parent_height).unwrap_or(usize::MAX);
        if let Some(LensNode::Post { parent_uri: Some(parent), markers, .. }) = work.nodes.first_mut()
            && chain >= parent_height
            && !work.seen.contains(parent.as_str())
        {
            markers.more_parents = true;
        }
        if r.depth > 0 {
            for node in &mut work.nodes {
                if matches!(node, LensNode::Post { .. }) && levels.get(node.uri()) == Some(&r.depth) && !node.no_replies() {
                    node.markers_mut().more_replies = Some(None);
                }
            }
        }
    }
    work.enforce().await?;
    let bridging = match (bridging, work.limited) {
        (false, _) => Bridging::Off,
        (true, true) => Bridging::Limited,
        (true, false) => Bridging::Full,
    };
    Ok(Thread { nodes: work.nodes, bridging })
}

/// `resolve`: each input URI on its own (§5.15.9, §5.15.10). Its authors
/// are judged in one round, which decides the switch. `Clear` goes through
/// the ladder; a block is a gap; anything else is `block-state-unknown`.
///
/// # Errors
///
/// Database errors.
pub async fn resolve(
    ladder: &Ladder,
    cfg: &ThreadLensConfig,
    key: &CursorKey,
    viewer: &Viewer,
    req: &Requester,
    uris: &[String],
) -> Result<Thread, HydrationError> {
    let h = ladder.hydrator();
    let me = viewer.did.clone().unwrap_or_default();
    let judged: Vec<String> = uris.iter().filter(|uri| gap_by_form(uri).is_none()).filter_map(|uri| uri_author(uri).map(str::to_string)).collect();
    // Inputs all decided by their form: no round, no call.
    if judged.is_empty() {
        let nodes = uris.iter().map(|uri| LensNode::gap(uri, None, gap_by_form(uri).unwrap_or(Gap::NotFound))).collect();
        return Ok(Thread { nodes, bridging: Bridging::Off });
    }
    let lookup = h.farsight().map(|farsight| lens_lookup(farsight, viewer, req));
    let mut judge = Judge::new(h.pool().clone(), &me, lookup, cfg);
    judge.baseline(&judged).await?;
    let bridging = judge.bridging();
    let mut work = Work {
        ladder,
        r: Request { viewer, req, anchor: "", depth: 0, parent_height: 0 },
        known: Arc::new(judge.known_blocks()),
        judge,
        nodes: Vec::new(),
        seen: HashSet::new(),
        budget: 0,
        listings: 0,
        parent_rounds: 0,
        limited: false,
        bridged: Vec::new(),
        key,
        bridging,
        start: None,
    };
    let fetches: Vec<String> = uris
        .iter()
        .filter(|uri| gap_by_form(uri).is_none() && bridging && work.judge.verdict(uri_author(uri).unwrap_or_default()) == Some(Verdict::Clear))
        .cloned()
        .collect();
    let this = &work;
    let mut fetched: HashMap<String, Resolved> = futures::stream::iter(fetches)
        .map(|uri| async move {
            let resolved = this.fetch(&uri).await;
            resolved.map(|r| (uri, r))
        })
        .buffered(LADDER_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, sqlx::Error>>()?;
    for uri in uris {
        let verdict = work.judge.verdict(uri_author(uri).unwrap_or_default());
        let node = match (gap_by_form(uri), block_gap(verdict), fetched.remove(uri)) {
            (Some(gap), ..) => LensNode::gap(uri, None, gap),
            (None, Some(gap), _) => LensNode::gap(uri, None, gap),
            (None, None, Some(resolved)) => work.ladder_node(resolved, uri, None),
            (None, None, None) => {
                work.limited = true;
                LensNode::gap(uri, None, Gap::BlockStateUnknown)
            }
        };
        work.nodes.push(node);
    }
    work.enforce().await?;
    let bridging = match (bridging, work.limited) {
        (false, _) => Bridging::Off,
        (true, true) => Bridging::Limited,
        (true, false) => Bridging::Full,
    };
    Ok(Thread { nodes: work.nodes, bridging })
}

#[cfg(test)]
mod tests {
    use super::*;

    const V: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv";
    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";

    fn uri(did: &str, rkey: &str) -> String {
        format!("at://{did}/app.bsky.feed.post/{rkey}")
    }

    fn view(did: &str, rkey: &str, parent: Option<&str>) -> Value {
        let mut record = json!({"text": rkey});
        if let Some(p) = parent {
            record["reply"] = json!({"parent": {"uri": p}, "root": {"uri": p}});
        }
        json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": {"uri": uri(did, rkey), "cid": "c", "author": {"did": did}, "record": record}})
    }

    /// The ordinary view's tree becomes parents, the anchor, then replies
    /// level by level; a stub the viewer is a party to says which way.
    #[tokio::test]
    async fn the_baseline_tree_is_flattened_in_order() {
        let judge = Judge::judged(V, &[B], &[C], &[A]);
        let mut tree = view(A, "anchor", Some(&uri(A, "root")));
        tree["parent"] = view(A, "root", None);
        let mut first = view(A, "r1", Some(&uri(A, "anchor")));
        first["replies"] = json!([view(A, "r1a", Some(&uri(A, "r1")))]);
        tree["replies"] = json!([
            first,
            {"$type": "app.bsky.feed.defs#blockedPost", "uri": uri(B, "mine"), "blocked": true},
            {"$type": "app.bsky.feed.defs#blockedPost", "uri": uri(C, "theirs"), "blocked": true},
            {"$type": "app.bsky.feed.defs#blockedPost", "uri": uri(A, "others"), "blocked": true},
            {"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri(A, "gone"), "notFound": true},
        ]);
        let (nodes, levels) = flatten(&tree, &judge);
        let got: Vec<(String, Option<&str>)> = nodes
            .iter()
            .map(|n| match n {
                LensNode::Post { uri, .. } => (uri.clone(), None),
                LensNode::Gap { uri, gap, .. } => (uri.clone(), Some(gap.label())),
            })
            .collect();
        assert_eq!(
            got,
            [
                (uri(A, "root"), None),
                (uri(A, "anchor"), None),
                (uri(A, "r1"), None),
                (uri(B, "mine"), Some("blocked-by-you")),
                (uri(C, "theirs"), Some("blocking-you")),
                (uri(A, "others"), Some("blocked")),
                (uri(A, "gone"), Some("not-found")),
                (uri(A, "r1a"), None),
            ]
        );
        assert_eq!((levels[&uri(A, "anchor")], levels[&uri(A, "r1")], levels[&uri(A, "r1a")]), (0, 1, 2));
        // A stub keeps the parent the tree placed it under.
        assert_eq!(nodes[3].parent_uri(), Some(uri(A, "anchor").as_str()));
    }

    /// An embedded record by an author the viewer has a block with
    /// becomes `#viewBlocked`, at any depth; any other is left alone.
    #[tokio::test]
    async fn embedded_records_by_blocked_authors_are_replaced() {
        let judge = Judge::judged(V, &[B], &[C], &[A]);
        let record = |did: &str, rkey: &str| json!({"$type": "app.bsky.embed.record#viewRecord", "uri": uri(did, rkey), "author": {"did": did, "handle": "h"}, "value": {"text": "secret"}});
        let mut nested = record(A, "outer");
        nested["value"] = json!({"text": "kept too"});
        nested["embeds"] = json!([{"$type": "app.bsky.embed.record#view", "record": record(C, "inner")}]);
        let mut embed = json!({
            "$type": "app.bsky.embed.recordWithMedia#view",
            "media": {"$type": "app.bsky.embed.images#view", "images": []},
            "record": {"record": nested},
            "also": [
                record(B, "quoted"),
                {"$type": "app.bsky.graph.defs#listView", "uri": format!("at://{B}/app.bsky.graph.list/l"), "creator": {"did": B}, "name": "secret list"},
                {"$type": "app.bsky.feed.defs#generatorView", "uri": format!("at://{A}/app.bsky.feed.generator/g"), "creator": {"did": A}, "displayName": "kept"},
            ],
        });
        enforce_embeds(&mut embed, &judge);
        let text = embed.to_string();
        assert!(!text.contains("secret"), "{text}");
        assert!(text.contains("kept") && text.contains(&uri(A, "outer")));
        // The viewer blocks B; C blocks the viewer.
        assert_eq!(embed["also"][0], json!({"$type": "app.bsky.embed.record#viewBlocked", "uri": uri(B, "quoted"), "blocked": true, "author": {"did": B, "viewer": {"blockedBy": false}}}));
        assert_eq!(embed["record"]["record"]["embeds"][0]["record"]["author"]["viewer"]["blockedBy"], json!(true));
        assert_eq!(embed["also"][1]["$type"], "app.bsky.embed.record#viewBlocked");
    }

    #[test]
    fn a_uris_form_decides_before_anything_else() {
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(gap_by_form(&format!("at://{did}/tools.kryphocron.feed.postPrivate/3k")), Some(Gap::TierHidden));
        assert_eq!(gap_by_form(&format!("at://{did}/app.bsky.graph.list/3k")), Some(Gap::NotFound));
        assert_eq!(gap_by_form("at://not-a-did/app.bsky.feed.post/3k"), Some(Gap::NotFound));
        assert_eq!(gap_by_form(&format!("at://{did}/app.bsky.feed.post/3k")), None);
    }
}
