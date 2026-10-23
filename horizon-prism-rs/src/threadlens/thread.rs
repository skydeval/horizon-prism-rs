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
//! 5. **Hidden replies.** One pass over every node: a node the thread
//!    root's threadgate hides from the caller carries `hiddenByThreadgate`,
//!    whatever path it came by (Thread Lens amendment 1 §2). The baseline
//!    keeps such replies instead of leaving them out. The response has no
//!    gate field, and nothing of a gap root's gate is sent.
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

/// Reply sequences read at once for one level (threadlens-hydration
/// design §2.3, F2).
pub const LISTING_CONCURRENCY: usize = 8;
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
    /// Where the request's time went, phase by phase, in order
    /// (threadlens-hydration design §2.5). Also recorded as
    /// [`METRIC_PHASE_SECONDS`].
    pub phases: Vec<(&'static str, std::time::Duration)>,
}

/// Metric: seconds a `getThread` or `resolve` request spent in each phase,
/// labelled `phase`.
pub const METRIC_PHASE_SECONDS: &str = "threadlens_phase_seconds";
/// Metric: Constellation reply pages a response stopped waiting for
/// (`threadlens.listing_timeout_ms`).
pub const METRIC_LISTING_TIMEOUTS: &str = "threadlens_listing_timeouts_total";

/// The phase clock of one request.
struct Phases {
    last: std::time::Instant,
    list: Vec<(&'static str, std::time::Duration)>,
}

impl Phases {
    fn start() -> Self {
        Self { last: std::time::Instant::now(), list: Vec::new() }
    }

    /// Close the phase that ran since the last mark.
    fn mark(&mut self, phase: &'static str) {
        let now = std::time::Instant::now();
        self.list.push((phase, now - self.last));
        self.last = now;
    }

    /// Record every phase on the histogram and hand them over.
    fn finish(self) -> Vec<(&'static str, std::time::Duration)> {
        for (phase, took) in &self.list {
            metrics::histogram!(METRIC_PHASE_SECONDS, "phase" => *phase).record(took.as_secs_f64());
        }
        self.list
    }
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

/// One Constellation page of replies, asked for as its own task: the
/// listed reply URIs and the next cursor, or no answer.
type PageCall = tokio::task::JoinHandle<Option<(Vec<String>, Option<String>)>>;

/// Ask for one page of `parent`'s replies from `cursor`, as its own task, so
/// a response that stops waiting for it leaves it to finish and fill the
/// read-time cache (F5).
fn listing_call(h: &crate::hydration::Hydrator, req: &Requester, parent: &str, cursor: Option<String>) -> PageCall {
    let (h, req, parent) = (h.clone(), req.clone(), parent.to_string());
    tokio::spawn(async move { h.lens_listed_replies(&req, &parent, LISTING_PAGE, cursor.as_deref()).await })
}

/// One node's stored replies as read, before its listing page.
struct LocalRead {
    replies: Vec<Reply>,
    /// Set when more stored replies remain than a page holds: the position
    /// to resume from. Nothing is listed then.
    full: Option<Option<Position>>,
    /// The listing cursor to read from.
    listed_from: Option<String>,
    /// Where the sequence started.
    from: Option<Position>,
}

/// One sequence of a level, while its page is read: done, or its page due
/// (the node, its stored replies, and the page's call if already asked).
type Slot = Result<Seq, (String, LocalRead, Option<PageCall>)>;

/// One sequence's placement: what to do with each reply, and the first
/// reply left for the page budget.
type Plan = (Vec<(String, Act)>, Option<usize>);

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
    /// URIs of post views built from the record alone (Slingshot), whose
    /// authors the author pass fills in.
    from_record: HashSet<String>,
    key: &'a CursorKey,
    /// Whether bridging is on for the request.
    bridging: bool,
    /// `threadlens.listing_timeout_ms`.
    listing_timeout: std::time::Duration,
    /// The anchor's first Constellation page, asked for while the baseline
    /// was assembled (threadlens-hydration design §2.7, F7).
    anchor_page: Option<PageCall>,
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

    /// The ladder for each of `uris` at once (threadlens-hydration design
    /// §2.3, F3), hydrated with the request's blocks.
    async fn fetch_many(&self, uris: &[String]) -> Result<HashMap<String, Resolved>, sqlx::Error> {
        if uris.is_empty() {
            return Ok(HashMap::new());
        }
        with_known_blocks(Arc::clone(&self.known), self.ladder.resolve_many(self.r.viewer, self.r.req, uris, Mode::Full)).await
    }

    fn ladder_node(&mut self, resolved: Resolved, uri: &str, parent_uri: Option<String>) -> LensNode {
        let resolved_kind = match &resolved {
            Resolved::RecordPost(_) => Some(true),
            Resolved::Post(_) => Some(false),
            Resolved::Gap(_) => None,
        };
        match resolved {
            Resolved::Post(post) | Resolved::RecordPost(post) => {
                if matches!(&resolved_kind, Some(true)) {
                    self.from_record.insert(uri.to_string());
                }
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
        // The routes of the stubs' authors at once, when lodestar is set
        // (lodestar-reads design §2.10).
        let authors: Vec<String> = stubs
            .iter()
            .filter_map(|(_, uri)| uri_author(uri))
            .filter(|author| self.judge.verdict(author) == Some(Verdict::Clear))
            .map(str::to_string)
            .collect();
        self.ladder.router().prefetch(self.r.req, &authors).await;
        // Every `Clear` stub through the ladder at once.
        let clear: Vec<String> = stubs
            .iter()
            .filter(|(_, uri)| self.judge.verdict(uri_author(uri).unwrap_or_default()) == Some(Verdict::Clear))
            .map(|(_, uri)| uri.clone())
            .collect();
        let mut fetched = self.fetch_many(&clear).await?;
        for (i, uri) in stubs {
            match self.judge.verdict(uri_author(&uri).unwrap_or_default()) {
                Some(Verdict::Clear) => {
                    let parent = self.nodes[i].parent_uri().map(str::to_string);
                    let Some(resolved) = fetched.remove(&uri) else {
                        continue;
                    };
                    if !matches!(resolved, Resolved::Gap(_)) {
                        self.nodes[i] = self.ladder_node(resolved, &uri, parent);
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
    /// checked, or a bound. A bound leaves `moreParents` on the top node;
    /// a known-block gap there keeps the `parentUri` [`Self::parent_of_top`]
    /// named.
    async fn parents(&mut self) -> Result<(), sqlx::Error> {
        loop {
            let Some(parent) = self.parent_of_top().await? else {
                return Ok(());
            };
            // A known-block gap's parent, named by a local row or the
            // baseline (never the hidden record), is written whether or not
            // the walk goes on: a bound leaves `moreParents` and the link.
            if let Some(LensNode::Gap { parent_uri, .. }) = self.nodes.first_mut()
                && parent_uri.is_none()
            {
                *parent_uri = Some(parent.clone());
            }
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
            let goes_on = node.walkable();
            self.seen.insert(parent);
            self.nodes.insert(0, node);
            self.budget -= 1;
            if !goes_on {
                return Ok(());
            }
        }
    }

    /// The local phase of one node's reply sequence from `from` (§5.15.14):
    /// the stored replies. A cursor already in the Constellation phase
    /// reads none.
    async fn local_phase(h: &crate::hydration::Hydrator, parent: &str, from: Option<Position>) -> Result<LocalRead, sqlx::Error> {
        let page = usize::try_from(LISTING_PAGE).unwrap_or(usize::MAX);
        let mut replies: Vec<Reply> = Vec::new();
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
                    return Ok(LocalRead { replies, full: Some(before), listed_from: None, from });
                }
                None
            }
        };
        Ok(LocalRead { replies, full: None, listed_from, from })
    }

    /// The Constellation phase of one node's reply sequence: one page of
    /// the replies its listing names that are not stored, after the stored
    /// ones read.
    ///
    /// A page that takes longer than `timeout` is no answer for this
    /// response (threadlens-hydration design §2.3, F5): the marker keeps
    /// the cursor, and the call goes on in the background, so the page is
    /// in the read-time cache when the client follows the marker.
    ///
    /// `early` is the same page already asked for (F7): its call is awaited
    /// instead of a new one.
    async fn listing_phase(
        h: &crate::hydration::Hydrator,
        req: &Requester,
        parent: &str,
        mut read: LocalRead,
        timeout: std::time::Duration,
        early: Option<PageCall>,
    ) -> Result<Seq, sqlx::Error> {
        let here = Position::Listed { cursor: read.listed_from.clone() };
        let call = match early {
            Some(call) => call,
            None => listing_call(h, req, parent, read.listed_from.clone()),
        };
        let page = match tokio::time::timeout(timeout, call).await {
            Ok(Ok(page)) => page,
            Ok(Err(_)) => None,
            Err(_) => {
                metrics::counter!(METRIC_LISTING_TIMEOUTS).increment(1);
                None
            }
        };
        let rest = match page {
            // No answer: the marker keeps the cursor it had.
            None => Some(Some(here)),
            Some((listed, next)) => {
                let stored = h.lens_local_rows(&listed).await?;
                for uri in listed {
                    if !stored.contains_key(&uri) && !read.replies.iter().any(|r| r.uri == uri) {
                        read.replies.push(Reply { uri, local_row: false, before: Some(here.clone()) });
                    }
                }
                next.map(|cursor| Some(Position::Listed { cursor: Some(cursor) }))
            }
        };
        Ok(Seq { parent: parent.to_string(), replies: read.replies, rest })
    }

    /// One level's reply sequences (§5.15.14), in level order. The stored
    /// replies of every node and then the Constellation pages are read
    /// concurrently (threadlens-hydration design §2.3, F2); the
    /// allowances are spent in the order and on the reads they were spent
    /// on when the sequences were read one by one, so the same sequences
    /// are started and the same markers set.
    async fn read_level(&mut self, level: &[String], start: &mut Option<Position>) -> Result<Vec<Seq>, sqlx::Error> {
        // The nodes whose sequences may start, each with where it starts:
        // the first one takes the request's cursor position.
        let mut eligible: Vec<(String, Option<Position>)> = Vec::new();
        for parent in level {
            let Some(node) = self.index(parent).map(|i| &self.nodes[i]) else {
                continue;
            };
            if !node.walkable() || node.no_replies() {
                continue;
            }
            eligible.push((parent.clone(), start.take()));
        }
        // Each started sequence spends a listing, so no more than
        // `listings` of them can start: only theirs are read.
        let readable = if self.budget == 0 { 0 } else { usize::try_from(self.listings).unwrap_or(usize::MAX) };
        let h = self.ladder.hydrator().clone();
        let locals: Vec<LocalRead> = futures::stream::iter(eligible.iter().take(readable).cloned())
            .map(|(parent, from)| {
                let h = &h;
                async move { Self::local_phase(h, &parent, from).await }
            })
            .buffered(LISTING_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let mut locals = locals.into_iter();
        // In level order: the outer allowance for the sequence, then, when
        // its stored replies end, the inner one for its listing page.
        let mut slots: Vec<Slot> = Vec::new();
        for (parent, from) in eligible {
            if self.listings == 0 || self.budget == 0 {
                self.mark_at(&parent, from);
                continue;
            }
            self.listings -= 1;
            let read = locals.next().expect("a local read for every sequence started");
            if let Some(rest) = read.full {
                slots.push(Ok(Seq { parent, replies: read.replies, rest: Some(rest) }));
                continue;
            }
            if !self.bridging {
                // In this state the stored replies are all there is to read.
                // A cursor already in the Constellation phase cannot be read
                // from at all: the marker keeps it.
                let kept = matches!(read.from, Some(Position::Listed { .. })).then(|| read.from.clone());
                slots.push(Ok(Seq { parent, replies: read.replies, rest: kept }));
                continue;
            }
            if self.listings == 0 {
                let here = Position::Listed { cursor: read.listed_from.clone() };
                slots.push(Ok(Seq { parent, replies: read.replies, rest: Some(Some(here)) }));
                continue;
            }
            self.listings -= 1;
            // The anchor's first page may already be on its way (F7).
            let early = if parent == self.r.anchor && read.listed_from.is_none() { self.anchor_page.take() } else { None };
            slots.push(Err((parent, read, early)));
        }
        let (req, timeout) = (self.r.req, self.listing_timeout);
        futures::stream::iter(slots)
            .map(|slot| {
                let h = &h;
                async move {
                    match slot {
                        Ok(seq) => Ok(seq),
                        Err((parent, read, early)) => Self::listing_phase(h, req, &parent, read, timeout, early).await,
                    }
                }
            })
            .buffered(LISTING_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect()
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
            let read: Vec<Seq> = self.read_level(&level, &mut start).await?;
            let authors: Vec<String> = read
                .iter()
                .flat_map(|seq| seq.replies.iter())
                .filter(|reply| !self.seen.contains(&reply.uri))
                .filter_map(|reply| uri_author(&reply.uri).map(str::to_string))
                .collect();
            let judged = self.judge.round(&authors).await?;
            self.refresh_known();
            // The level's authors' routes at once, when lodestar is set
            // (lodestar-reads design §2.10).
            if judged {
                self.ladder.router().prefetch(self.r.req, &authors).await;
            }
            let parents: Vec<String> = read.iter().map(|seq| seq.parent.clone()).collect();
            let local_parents = self.ladder.hydrator().lens_local_rows(&parents).await?;
            // Every sequence's replies are planned in level order, as if
            // each were placed before the next was planned; the level's
            // fetches then go through the ladder at once (F3).
            let mut plans: Vec<Plan> = Vec::new();
            if judged {
                let (mut seen, mut budget) = (self.seen.clone(), self.budget);
                for seq in &read {
                    plans.push(self.plan(seq, local_parents.contains_key(&seq.parent), &mut seen, &mut budget));
                }
            }
            let fetches: Vec<String> = plans
                .iter()
                .flat_map(|(acts, _)| acts.iter())
                .filter(|(_, act)| matches!(act, Act::Fetch))
                .map(|(uri, _)| uri.clone())
                .collect();
            let mut fetched = self.fetch_many(&fetches).await?;
            let mut plans = plans.into_iter();
            for seq in read {
                // A reply whose author could not be sent in the round is
                // not placed: the cursor is the position before it.
                let stopped = match plans.next() {
                    Some((acts, stopped)) => {
                        self.place(&seq.parent, acts, &mut fetched);
                        stopped
                    }
                    None => seq.replies.iter().position(|reply| !self.seen.contains(&reply.uri)),
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

    /// What to do with the replies read for a node that the response does
    /// not hold yet, and the index of the first reply that was not placed
    /// for the page budget, if any. `seen` and `budget` are the level's
    /// running copies: they move as if the sequences before this one had
    /// been placed already (threadlens-hydration design §2.3, F3).
    ///
    /// A stored reply to a node that was itself built from a stored row is
    /// one of the ordinary view's own rows: it is shown without a `Clear`
    /// verdict. A known block makes it a gap in its place with bridging
    /// on, and leaves it out with bridging off. Every other reply is a
    /// bridge candidate, and enters only with bridging on.
    fn plan(&mut self, seq: &Seq, local_parent: bool, seen: &mut HashSet<String>, budget: &mut usize) -> Plan {
        let mut acts: Vec<(String, Act)> = Vec::new();
        let mut stopped = None;
        for (i, reply) in seq.replies.iter().enumerate() {
            if seen.contains(&reply.uri) {
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
            if acts.len() >= *budget {
                stopped = Some(i);
                break;
            }
            acts.push((reply.uri.clone(), act));
        }
        *budget = budget.saturating_sub(acts.len());
        seen.extend(acts.iter().map(|(uri, _)| uri.clone()));
        (acts, stopped)
    }

    /// Place one sequence's planned replies, from the level's fetches.
    fn place(&mut self, parent: &str, acts: Vec<(String, Act)>, fetched: &mut HashMap<String, Resolved>) {
        for (uri, act) in acts {
            let node = match act {
                Act::Gap(gap) => LensNode::gap(&uri, Some(parent.to_string()), gap),
                Act::Fetch => match fetched.remove(&uri) {
                    Some(resolved) => self.ladder_node(resolved, &uri, Some(parent.to_string())),
                    None => continue,
                },
                Act::Skip => continue,
            };
            self.seen.insert(uri);
            self.nodes.push(node);
            self.budget = self.budget.saturating_sub(1);
        }
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
            let LensNode::Post { uri, parent_uri, post, markers, hidden, .. } = node else {
                continue;
            };
            let author = uri_author(uri).unwrap_or_default();
            if let Some(gap) = block_gap(judge.verdict(author)) {
                *node = LensNode::Gap {
                    uri: uri.clone(),
                    parent_uri: parent_uri.clone(),
                    gap,
                    markers: markers.clone(),
                    hidden: *hidden,
                    blocking_list: None,
                };
            } else if let Some(embed) = post.get_mut("embed") {
                enforce_embeds(embed, judge);
            }
        }
        Ok(())
    }

    /// On every `blocked-by-you` gap, the caller's own list that blocks the
    /// author, when the block comes through a list and not directly
    /// (amendment 1 §2A). From the request's verdict data only: no call.
    fn name_lists(&mut self) {
        for node in &mut self.nodes {
            let list = match &*node {
                LensNode::Gap { uri, gap: Gap::BlockedByYou, .. } => self.judge.blocking_list(uri_author(uri).unwrap_or_default()),
                _ => continue,
            };
            node.set_blocking_list(list);
        }
    }

    /// The author pass (design-v4 §8.1): a post built from its record
    /// carries `{did, handle}`. Its author, when not readable, takes the
    /// profile tiers, and a resolved one becomes the batch's
    /// `profileViewBasic` with the node's own `viewer` and `labels`, which
    /// carry the request's block state. No viewer state is computed, so no
    /// Farsight lookup is made outside the judged rounds.
    async fn author_pass(&mut self) {
        let mut dids: Vec<String> = self
            .nodes
            .iter()
            .filter_map(|n| match n {
                LensNode::Post { uri, .. } if self.from_record.contains(uri) => uri_author(uri).map(str::to_string),
                _ => None,
            })
            .collect();
        dids.sort_unstable();
        dids.dedup();
        if dids.is_empty() {
            return;
        }
        let h = self.ladder.hydrator();
        let readable = h.readable_dids(&dids).await;
        dids.retain(|d| !readable.contains(d));
        let remote = h.remote_profiles(self.r.req, &dids).await;
        for node in &mut self.nodes {
            let LensNode::Post { uri, post, .. } = node else {
                continue;
            };
            if !self.from_record.contains(uri) {
                continue;
            }
            let Some(view) = uri_author(uri).and_then(|d| remote.get(d)) else {
                continue;
            };
            set_author(post, view);
        }
    }
}

/// Replace `post.author` with the remote `view` projected to
/// `profileViewBasic`, keeping the author's own `viewer` and `labels`.
pub(crate) fn set_author(post: &mut Value, view: &Value) {
    let mut author = crate::hydration::profile_refresh::project(view, crate::hydration::actor::ProfileKind::Basic);
    let old = post.get("author");
    for key in ["did", "viewer", "labels"] {
        if let Some(v) = old.and_then(|a| a.get(key)) {
            author.insert(key.to_string(), v.clone());
        }
    }
    post["author"] = Value::Object(author);
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
            phases: Vec::new(),
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
    let mut phases = Phases::start();
    // F7: the anchor's first page of listed replies is asked for now, beside
    // the baseline, when bridging can be on and the replies will be read.
    let anchor_page = (cursor.is_none() && r.depth > 0 && h.farsight().is_some() && h.lists_replies())
        .then(|| listing_call(h, r.req, r.anchor, None));
    let memo = crate::hydration::thread::ThreadMemo::new();
    let collected = h.lens_baseline(r.viewer, r.req, r.anchor, depth, parent_height, &memo).await?;
    let mut authors: Vec<String> = collected.as_ref().map(|(tree, _)| named_accounts(tree)).unwrap_or_default().into_iter().collect();
    authors.extend(uri_author(r.anchor).map(str::to_string));
    phases.mark("collect");
    judge.baseline(&authors).await?;
    phases.mark("baseline_round");
    let known = Arc::new(judge.known_blocks());
    let (tree, gate_view) = match collected {
        Some(_) => match with_known_blocks(Arc::clone(&known), h.lens_baseline(r.viewer, r.req, r.anchor, depth, parent_height, &memo)).await? {
            Some((tree, gate)) => (Some(tree), gate),
            None => (None, None),
        },
        None => (None, None),
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
        from_record: HashSet::new(),
        key,
        bridging,
        listing_timeout: std::time::Duration::from_millis(u64::from(cfg.listing_timeout_ms)),
        anchor_page,
        start: cursor,
    };
    phases.mark("emit");
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
        phases.mark("stubs");
        work.parents().await?;
        phases.mark("parents");
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
    phases.mark("replies");
    work.enforce().await?;
    phases.mark("enforce");
    work.author_pass().await;
    work.name_lists();
    phases.mark("authors");
    if let Some((root, hidden)) = thread_gate(h, r.anchor, &work.nodes, gate_view.as_ref()).await? {
        flag_hidden(&mut work.nodes, &root, &hidden, r.viewer);
    }
    phases.mark("gate");
    let bridging = match (bridging, work.limited) {
        (false, _) => Bridging::Off,
        (true, true) => Bridging::Limited,
        (true, false) => Bridging::Full,
    };
    Ok(Thread { nodes: work.nodes, bridging, phases: phases.finish() })
}

/// The last path segment of an at-uri (its record key).
fn rkey(uri: &str) -> &str {
    uri.rsplit('/').next().unwrap_or_default()
}

/// The thread's root and the replies its threadgate hides, for one
/// `getThread` response (amendment 1 §2.3). Read once, after enforcement.
///
/// The root is the anchor's stored row's root, else the root its post
/// node's record names. The gate is the root's valid stored threadgate,
/// else the gate view the ordinary assembly returned with the baseline,
/// used only when it is self-consistent and names this root. A private
/// root has no gate here: Thread Lens never reads the private tier.
/// Only the flags come of it.
async fn thread_gate(h: &crate::hydration::Hydrator, anchor: &str, nodes: &[LensNode], view: Option<&Value>) -> Result<Option<(String, HashSet<String>)>, sqlx::Error> {
    let root = match h.lens_stored_root(anchor).await? {
        Some(root) => Some(root),
        None => nodes.iter().find_map(|n| match n {
            LensNode::Post { uri, root_uri, .. } if uri == anchor => Some(root_uri.clone().unwrap_or_else(|| anchor.to_string())),
            _ => None,
        }),
    };
    if root.as_deref().is_some_and(is_private_uri) {
        return Ok(None);
    }
    if let Some(root) = &root
        && let Some(hidden) = h.lens_hidden_replies(std::slice::from_ref(root)).await?.remove(root)
    {
        return Ok(Some((root.clone(), hidden)));
    }
    let Some(view) = view else {
        return Ok(None);
    };
    let (Some(gate), Some(post)) = (view.get("uri").and_then(Value::as_str), view.pointer("/record/post").and_then(Value::as_str)) else {
        return Ok(None);
    };
    let consistent = gate_by_form(gate) && is_post_uri(post) && uri_author(gate) == uri_author(post) && rkey(gate) == rkey(post);
    if !consistent || is_private_uri(post) || root.as_deref().is_some_and(|r| r != post) {
        return Ok(None);
    }
    let hidden = view
        .pointer("/record/hiddenReplies")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    Ok(Some((post.to_string(), hidden)))
}

/// `at://<did>/app.bsky.feed.threadgate/<rkey>`.
fn gate_by_form(uri: &str) -> bool {
    let mut parts = uri.strip_prefix("at://").unwrap_or_default().split('/');
    matches!((parts.next(), parts.next(), parts.next(), parts.next()), (Some(_), Some("app.bsky.feed.threadgate"), Some(k), None) if !k.is_empty())
}

/// Set `hiddenByThreadgate` on every node the gate lists, except the root
/// and the caller's own posts (amendment 1 §2.2).
fn flag_hidden(nodes: &mut [LensNode], root: &str, hidden: &HashSet<String>, viewer: &Viewer) {
    let me = viewer.did.as_deref();
    for node in nodes {
        let uri = node.uri();
        let on = uri != root && hidden.contains(uri) && (me.is_none() || uri_author(uri) != me);
        node.set_hidden_by_threadgate(on);
    }
}

/// `hiddenByThreadgate` on `resolve` items (amendment 1 §2.6): post items
/// only, each from the stored gate of the root its own record names. A gap
/// item is never flagged: its root could come only from a stored row, and
/// the flag would then say whether this AppView holds the post.
async fn resolve_hidden(h: &crate::hydration::Hydrator, nodes: &mut [LensNode], viewer: &Viewer) -> Result<(), sqlx::Error> {
    let roots: Vec<String> = nodes
        .iter()
        .filter_map(|n| match n {
            LensNode::Post { uri, root_uri, .. } => Some(root_uri.clone().unwrap_or_else(|| uri.clone())),
            LensNode::Gap { .. } => None,
        })
        .filter(|root| !is_private_uri(root))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let gates = h.lens_hidden_replies(&roots).await?;
    if gates.is_empty() {
        return Ok(());
    }
    for node in nodes {
        let LensNode::Post { uri, root_uri, .. } = &*node else {
            continue;
        };
        let root = root_uri.clone().unwrap_or_else(|| uri.clone());
        if let Some(hidden) = gates.get(&root) {
            let one = std::slice::from_mut(node);
            flag_hidden(one, &root, hidden, viewer);
        }
    }
    Ok(())
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
        return Ok(Thread { nodes, bridging: Bridging::Off, phases: Vec::new() });
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
        from_record: HashSet::new(),
        key,
        bridging,
        listing_timeout: std::time::Duration::from_millis(u64::from(cfg.listing_timeout_ms)),
        anchor_page: None,
        start: None,
    };
    let fetches: Vec<String> = uris
        .iter()
        .filter(|uri| gap_by_form(uri).is_none() && bridging && work.judge.verdict(uri_author(uri).unwrap_or_default()) == Some(Verdict::Clear))
        .cloned()
        .collect();
    let mut fetched = work.fetch_many(&fetches).await?;
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
    work.author_pass().await;
    work.name_lists();
    resolve_hidden(h, &mut work.nodes, viewer).await?;
    let bridging = match (bridging, work.limited) {
        (false, _) => Bridging::Off,
        (true, true) => Bridging::Limited,
        (true, false) => Bridging::Full,
    };
    Ok(Thread { nodes: work.nodes, bridging, phases: Vec::new() })
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
