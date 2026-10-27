//! Thread Lens reading the thread outline (thread outline design §5).
//!
//! When the outline is enabled, active and fresh, Farsight is configured,
//! the anchor is a post, `depth > 0` and there is no cursor, `getThread`
//! reads the anchor's surroundings from the outline in one task beside the
//! baseline (§5.1):
//!
//! 1. one recursive walk up the parents to the root, with a step bound and
//!    `CYCLE` detection;
//! 2. whether the root's thread is complete (§3.3);
//! 3. one bounded recursive read of the region below the anchor;
//! 4. the orphan check, once per distinct root;
//! 5. the holes on the walk and in the region;
//! 6. the moderation gate over the walk;
//! 7. the stored rows of the region and the walk, and the miss rule.
//!
//! Any step that fails gives the request today's path, and says why in
//! `threadlens_outline_total{use}`. With a [`Tree`], the parents and the
//! replies come from it: one candidate round, one fetch, no listing pages
//! beyond the anchor's first, which serves as a cross-check (§5.6).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::hydration::Hydrator;
use crate::thread_outline::ops::{tid_string, tid_time, tid_value};
use crate::thread_outline::{Outline, RootFacts};
use crate::xrpc::pools::Requester;

/// `threadlens_outline_total{use}`: how each request that could use the
/// outline was served (§5.1's table).
pub const METRIC_USE: &str = "threadlens_outline_total";
/// `outline_crosscheck_total{result}`: the anchor cross-check (§5.6).
pub const METRIC_CROSSCHECK: &str = "outline_crosscheck_total";

/// How long a request waits for the outline's reads once its baseline is
/// assembled. They run beside the baseline, so they are usually done; past
/// this the request takes today's path (`use="error"`).
pub const READ_WAIT: Duration = Duration::from_millis(300);
/// Steps the walk to the root takes at most.
pub const WALK_STEPS: i32 = 1000;
/// The margin between Postgres's clock and the stream's (§5.1 step 7,
/// §5.6): a row newer than `through_ms` less this may still be on its way.
pub const CLOCK_MARGIN_MS: i64 = 60_000;
/// How long an anchor or root found too big is remembered.
const TOO_BIG_TTL: Duration = Duration::from_secs(600);
/// Keys remembered as too big, at most.
const TOO_BIG_MAX: usize = 10_000;
/// How long a root's orphan verdict is kept.
const ROOT_TTL: Duration = Duration::from_secs(10);
/// Root verdicts kept, at most.
const ROOTS_MAX: usize = 10_000;

const NSID_POST: &str = "app.bsky.feed.post";

/// How a request that could use the outline was served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Use {
    /// Every step passed and the cross-check found no miss.
    Tree,
    /// The anchor has no row.
    Absent,
    /// The thread is not complete.
    Incomplete,
    /// A hole, an orphan, a cycle, the walk's bound, or a stored row the
    /// outline lacks.
    Hole,
    /// The region, the thread or its stored rows are over their bound.
    TooBig,
    /// The outline is not fresh.
    Stale,
    /// Bridging is off for the request.
    BridgingOff,
    /// The anchor's first listing page names a reply the outline misses.
    Crosscheck,
    /// The outline is disabled, or its writer inactive.
    Off,
    /// The reads failed, or took longer than [`READ_WAIT`] past the
    /// baseline.
    Error,
}

impl Use {
    pub const ALL: [Self; 10] = [
        Self::Tree,
        Self::Absent,
        Self::Incomplete,
        Self::Hole,
        Self::TooBig,
        Self::Stale,
        Self::BridgingOff,
        Self::Crosscheck,
        Self::Off,
        Self::Error,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tree => "tree",
            Self::Absent => "absent",
            Self::Incomplete => "incomplete",
            Self::Hole => "hole",
            Self::TooBig => "too_big",
            Self::Stale => "stale",
            Self::BridgingOff => "bridging_off",
            Self::Crosscheck => "crosscheck",
            Self::Off => "off",
            Self::Error => "error",
        }
    }

    pub fn record(self) {
        metrics::counter!(METRIC_USE, "use" => self.as_str()).increment(1);
    }
}

/// The anchor cross-check's result (§5.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    /// Every listed reply is stored or has an outline row.
    Match,
    /// A listed reply is neither, nor on its way.
    Miss,
    /// No miss, and a listed reply is still on its way.
    Pending,
    /// The page had not answered when the candidates were formed.
    Late,
}

impl Check {
    pub const ALL: [Self; 4] = [Self::Match, Self::Miss, Self::Pending, Self::Late];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::Miss => "miss",
            Self::Pending => "pending",
            Self::Late => "late",
        }
    }

    pub fn record(self) {
        metrics::counter!(METRIC_CROSSCHECK, "result" => self.as_str()).increment(1);
    }
}

/// Every series of these metrics at zero, so a rule over them has data
/// from the start.
pub fn register_series() {
    for u in Use::ALL {
        metrics::counter!(METRIC_USE, "use" => u.as_str()).increment(0);
    }
    for c in Check::ALL {
        metrics::counter!(METRIC_CROSSCHECK, "result" => c.as_str()).increment(0);
    }
}

/// One node of the walk to the root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainNode {
    pub uri: String,
    /// The row is marked deleted: a known node, never fetched or placed.
    pub deleted: bool,
    /// The moderation gate does not show it (§5.1 step 6): the chain stops
    /// there.
    pub hidden: bool,
}

/// One outline child of a region node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Child {
    pub uri: String,
    pub created_at: DateTime<Utc>,
    pub deleted: bool,
}

/// One stored reply of a region node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub uri: String,
    pub created_at: DateTime<Utc>,
    /// After the request's `Local` cursor position, by the database's own
    /// `(created_at, uri)` order (as `lens_local_replies` compares it).
    pub after_cursor: bool,
}

/// The anchor's surroundings, as the outline holds them (§5.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tree {
    /// The anchor, then each parent up to the root.
    pub chain: Vec<ChainNode>,
    /// Each region node's outline children (live or deleted), in
    /// `(created_at, uri)` order on the URI's bytes.
    pub children: HashMap<String, Vec<Child>>,
    /// Each region node's stored replies, in `(created_at, uri)` order.
    pub stored: HashMap<String, Vec<Stored>>,
    /// The posts stored here among the walk and the region.
    pub local: HashSet<String>,
    /// The posts with an outline row among the walk and the region.
    pub known: HashSet<String>,
    /// The posts whose replies the reads covered: the anchor, and the live
    /// region rows above the last level.
    pub walked: HashSet<String>,
}

impl Tree {
    /// The outline-only children of `parent` the sequence reads: live,
    /// not stored here.
    pub fn outline_children(&self, parent: &str) -> impl Iterator<Item = &Child> {
        self.children.get(parent).into_iter().flatten().filter(|c| !c.deleted && !self.local.contains(&c.uri))
    }
}

/// What the reads gave: a tree, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    Tree(Box<Tree>),
    Fallback(Use),
}

/// A post's place in the outline: its author's id and decoded TID.
type NodeKey = (i32, i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootVerdict {
    Whole,
    Orphan,
    TooBig,
}

/// The outline's reading side for Thread Lens: the handle, and the
/// in-process memory of what was too big and of each root's verdict. Cheap
/// to clone.
#[derive(Debug, Clone)]
pub struct Reader {
    outline: Outline,
    /// Reads started, for tests that prove none ran.
    reads: Arc<std::sync::atomic::AtomicUsize>,
    too_big: Arc<Mutex<HashMap<NodeKey, Instant>>>,
    roots: Arc<Mutex<HashMap<NodeKey, (Instant, RootVerdict)>>>,
}

#[derive(sqlx::FromRow)]
struct WalkRow {
    author: i32,
    rkey: i64,
    did: String,
    parent_author: Option<i32>,
    root_author: Option<i32>,
    root_rkey: Option<i64>,
    seen_at: DateTime<Utc>,
    deleted: bool,
    step: i32,
    looped: bool,
}

#[derive(sqlx::FromRow)]
struct RegionRow {
    author: i32,
    rkey: i64,
    did: String,
    parent_did: String,
    parent_rkey: i64,
    root_author: Option<i32>,
    root_rkey: Option<i64>,
    created_at: DateTime<Utc>,
    deleted: bool,
    lvl: i32,
}

#[derive(sqlx::FromRow)]
struct StoredRow {
    uri: String,
    reply_parent_uri: Option<String>,
    created_at: DateTime<Utc>,
    indexed_at: DateTime<Utc>,
    after_cursor: Option<bool>,
}

/// Step 1: up `parent` from the anchor, one row per step, with `CYCLE`.
const WALK_SQL: &str = "WITH RECURSIVE up AS ( \
       SELECT o.author, o.rkey, o.parent_author, o.parent_rkey, o.root_author, o.root_rkey, o.seen_at, \
              o.deleted_at IS NOT NULL AS deleted, 0 AS step \
         FROM outline_posts o JOIN outline_actors a ON a.id = o.author WHERE a.did = $1 AND o.rkey = $2 \
       UNION ALL \
       SELECT o.author, o.rkey, o.parent_author, o.parent_rkey, o.root_author, o.root_rkey, o.seen_at, \
              o.deleted_at IS NOT NULL, u.step + 1 \
         FROM up u JOIN outline_posts o ON o.author = u.parent_author AND o.rkey = u.parent_rkey \
        WHERE u.step < $3 \
     ) CYCLE author, rkey SET looped USING path \
     SELECT u.author, u.rkey, a.did, u.parent_author, u.root_author, u.root_rkey, u.seen_at, u.deleted, u.step, u.looped \
       FROM up u JOIN outline_actors a ON a.id = u.author ORDER BY u.step, u.looped";

/// Step 3: down `outline_posts_parent` from the anchor, levels `1..=depth`,
/// not below a deleted row, bounded inside.
pub(crate) const REGION_SQL: &str = "WITH RECURSIVE down AS ( \
       SELECT o.author, o.rkey, o.parent_author, o.parent_rkey, o.root_author, o.root_rkey, o.created_at, \
              o.deleted_at IS NOT NULL AS deleted, 1 AS lvl \
         FROM outline_posts o WHERE o.parent_author = $1 AND o.parent_rkey = $2 \
       UNION ALL \
       SELECT o.author, o.rkey, o.parent_author, o.parent_rkey, o.root_author, o.root_rkey, o.created_at, \
              o.deleted_at IS NOT NULL, d.lvl + 1 \
         FROM down d JOIN outline_posts o ON o.parent_author = d.author AND o.parent_rkey = d.rkey \
        WHERE d.lvl < $3 AND NOT d.deleted \
     ) \
     SELECT r.author, r.rkey, a.did, pa.did AS parent_did, r.parent_rkey, r.root_author, r.root_rkey, r.created_at, r.deleted, r.lvl \
       FROM (SELECT * FROM down LIMIT $4) r \
       JOIN outline_actors a ON a.id = r.author JOIN outline_actors pa ON pa.id = r.parent_author";

/// Step 4: one root's rows, bounded, with whether any names a parent that
/// has no row (R4-6, R4-10).
const ORPHAN_SQL: &str = "SELECT count(*), coalesce(bool_or(c.parent_author IS NOT NULL AND NOT EXISTS ( \
         SELECT 1 FROM outline_posts p WHERE p.author = c.parent_author AND p.rkey = c.parent_rkey)), false) \
       FROM (SELECT parent_author, parent_rkey FROM outline_posts WHERE root_author = $1 AND root_rkey = $2 LIMIT $3) c";

/// Step 5: whether any of the keys has a hole.
const HOLES_SQL: &str = "SELECT EXISTS (SELECT 1 FROM outline_holes h \
       JOIN unnest($1::int4[], $2::int8[]) AS k(author, rkey) ON h.author = k.author AND h.rkey = k.rkey)";

/// Step 7: the stored replies of the region's posts, and the walk's own
/// rows, in `(created_at, uri)` order, bounded.
const STORED_SQL: &str = "SELECT uri, reply_parent_uri, created_at, indexed_at, \
            ($3::timestamptz IS NOT NULL AND (created_at, uri) > ($3::timestamptz, $4::text)) AS after_cursor \
       FROM posts WHERE reply_parent_uri = ANY($1) OR uri = ANY($2) \
      ORDER BY created_at, uri LIMIT $5";

/// The post a URI names, as an outline key: its DID and decoded TID.
fn post_key(uri: &str) -> Option<(&str, i64)> {
    let rest = uri.strip_prefix("at://")?;
    let mut parts = rest.split('/');
    let (did, collection, rkey) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || collection != NSID_POST || !proto_blue::syntax::Did::is_valid(did) {
        return None;
    }
    Some((did, tid_value(rkey)?))
}

fn post_uri(did: &str, rkey: i64) -> String {
    format!("at://{did}/{NSID_POST}/{}", tid_string(rkey))
}

/// Whether a reply that is neither stored nor in the outline may still be
/// on its way (R5-6): its rkey is a TID whose time is at most
/// [`CLOCK_MARGIN_MS`] before `through_ms`.
#[must_use]
pub fn on_its_way(uri: &str, through_ms: i64) -> bool {
    let Some((_, rkey)) = post_key(uri) else {
        return false;
    };
    tid_time(rkey).is_some_and(|t| t.timestamp_millis() >= through_ms - CLOCK_MARGIN_MS)
}

/// Of `uris`, those with an outline row, live or deleted, probed by key.
///
/// # Errors
///
/// Database errors.
pub async fn with_rows(pool: &PgPool, uris: &[String]) -> Result<HashSet<String>, sqlx::Error> {
    let (dids, rkeys): (Vec<String>, Vec<i64>) = uris.iter().filter_map(|u| post_key(u)).map(|(d, r)| (d.to_string(), r)).unzip();
    if dids.is_empty() {
        return Ok(HashSet::new());
    }
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT a.did, o.rkey FROM unnest($1::text[], $2::int8[]) AS k(did, rkey) \
         JOIN outline_actors a ON a.did = k.did JOIN outline_posts o ON o.author = a.id AND o.rkey = k.rkey",
    )
    .bind(&dids)
    .bind(&rkeys)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(did, rkey)| post_uri(&did, rkey)).collect())
}

impl Reader {
    #[must_use]
    pub fn new(outline: Outline) -> Self {
        Self { outline, reads: Arc::default(), too_big: Arc::default(), roots: Arc::default() }
    }

    /// How many times [`Self::read`] has run.
    #[must_use]
    pub fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A reader over a fresh, active outline whose coverage began two hours
    /// ago, so a thread whose root was seen an hour ago is complete.
    #[cfg(test)]
    pub(crate) fn fresh_for_tests(cfg: crate::config::ThreadOutlineConfig) -> Self {
        let outline = Outline::new(crate::config::ThreadOutlineConfig { enabled: true, fresh_secs: 3600, ..cfg });
        let shared = outline.shared();
        shared.set_active(true);
        shared.set_resumed(shared.want_epoch());
        let now = Utc::now().timestamp_millis();
        shared.set_through_ms(now);
        outline.observe_relay("test", now);
        outline.raise_complete_since(now - 2 * 3_600_000);
        Self::new(outline)
    }

    #[must_use]
    pub fn outline(&self) -> &Outline {
        &self.outline
    }

    /// The judge allowance kept for enforcement (`enforce_reserve`).
    #[must_use]
    pub fn enforce_reserve(&self) -> usize {
        usize::try_from(self.outline.config().enforce_reserve).unwrap_or(usize::MAX)
    }

    /// Why the outline may not be read now, if it may not: `off` while the
    /// writer is inactive, `stale` while it is not fresh.
    #[must_use]
    pub fn unusable(&self) -> Option<Use> {
        if !self.outline.active() {
            Some(Use::Off)
        } else if !self.outline.freshness().is_fresh() {
            Some(Use::Stale)
        } else {
            None
        }
    }

    fn recalls_too_big(&self, key: NodeKey) -> bool {
        let mut set = self.too_big.lock().unwrap_or_else(PoisonError::into_inner);
        match set.get(&key) {
            Some(at) if at.elapsed() < TOO_BIG_TTL => true,
            Some(_) => {
                set.remove(&key);
                false
            }
            None => false,
        }
    }

    fn remember_too_big(&self, key: NodeKey) {
        let mut set = self.too_big.lock().unwrap_or_else(PoisonError::into_inner);
        if set.len() >= TOO_BIG_MAX {
            set.retain(|_, at| at.elapsed() < TOO_BIG_TTL);
        }
        if set.len() >= TOO_BIG_MAX
            && let Some(oldest) = set.iter().min_by_key(|(_, at)| **at).map(|(k, _)| *k)
        {
            set.remove(&oldest);
        }
        set.insert(key, Instant::now());
    }

    /// Step 4 for one root, through the 10 s cache.
    async fn root_verdict(&self, pool: &PgPool, root: NodeKey) -> Result<RootVerdict, sqlx::Error> {
        {
            let roots = self.roots.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((at, verdict)) = roots.get(&root)
                && at.elapsed() < ROOT_TTL
            {
                return Ok(*verdict);
            }
        }
        let max_thread = i64::from(self.outline.config().max_thread);
        let (n, orphan): (i64, bool) = sqlx::query_as(ORPHAN_SQL).bind(root.0).bind(root.1).bind(max_thread + 1).fetch_one(pool).await?;
        let verdict = if n > max_thread {
            RootVerdict::TooBig
        } else if orphan {
            RootVerdict::Orphan
        } else {
            RootVerdict::Whole
        };
        let mut roots = self.roots.lock().unwrap_or_else(PoisonError::into_inner);
        if roots.len() >= ROOTS_MAX {
            roots.retain(|_, (at, _)| at.elapsed() < ROOT_TTL);
        }
        roots.insert(root, (Instant::now(), verdict));
        Ok(verdict)
    }

    /// §5.1's reads for `anchor`, its subtree within `depth` levels. `after`
    /// is a `Local` cursor's position, for a continuation (§5.3.4).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn read(
        &self,
        h: &Hydrator,
        req: &Requester,
        anchor: &str,
        depth: u32,
        after: Option<(DateTime<Utc>, String)>,
    ) -> Result<Read, sqlx::Error> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pool = h.pool();
        let cfg = self.outline.config();
        let max_tree = usize::try_from(cfg.max_tree).unwrap_or(usize::MAX);
        let Some((did, rkey)) = post_key(anchor) else {
            return Ok(Read::Fallback(Use::Absent));
        };
        // 1. The walk to the root.
        let walk: Vec<WalkRow> = sqlx::query_as(WALK_SQL).bind(did).bind(rkey).bind(WALK_STEPS).fetch_all(pool).await?;
        let Some(top) = walk.last() else {
            return Ok(Read::Fallback(Use::Absent));
        };
        if walk.iter().any(|w| w.looped) {
            return Ok(Read::Fallback(Use::Hole));
        }
        if top.parent_author.is_some() {
            // The bound, or an ancestor with no row: an old thread whose
            // top is older than the outline (N5).
            return Ok(Read::Fallback(if top.step >= WALK_STEPS { Use::Hole } else { Use::Incomplete }));
        }
        let anchor_key = (walk[0].author, walk[0].rkey);
        let root_key = (top.author, top.rkey);
        if self.recalls_too_big(anchor_key) || self.recalls_too_big(root_key) {
            return Ok(Read::Fallback(Use::TooBig));
        }
        // 2. Completeness, of the root the walk found.
        if !self.outline.thread_complete(RootFacts { seen_at: top.seen_at, rkey: top.rkey }, Utc::now()) {
            return Ok(Read::Fallback(Use::Incomplete));
        }
        // 3. The region below the anchor.
        let levels = i32::try_from(depth).unwrap_or(i32::MAX);
        let region: Vec<RegionRow> = sqlx::query_as(REGION_SQL)
            .bind(anchor_key.0)
            .bind(anchor_key.1)
            .bind(levels)
            .bind(i64::from(cfg.max_tree) + 1)
            .fetch_all(pool)
            .await?;
        if region.len() > max_tree {
            self.remember_too_big(anchor_key);
            return Ok(Read::Fallback(Use::TooBig));
        }
        // 4. Orphans, per distinct root of the walk and the region.
        let mut roots: Vec<NodeKey> = vec![root_key];
        let named = walk.iter().map(|w| (w.root_author, w.root_rkey)).chain(region.iter().map(|r| (r.root_author, r.root_rkey)));
        for (a, r) in named {
            if let (Some(a), Some(r)) = (a, r)
                && !roots.contains(&(a, r))
            {
                roots.push((a, r));
            }
        }
        for root in roots {
            match self.root_verdict(pool, root).await? {
                RootVerdict::Whole => {}
                RootVerdict::Orphan => return Ok(Read::Fallback(Use::Hole)),
                RootVerdict::TooBig => {
                    self.remember_too_big(root);
                    return Ok(Read::Fallback(Use::TooBig));
                }
            }
        }
        // 5. Holes on the walk and in the region.
        let (authors, rkeys): (Vec<i32>, Vec<i64>) = walk.iter().map(|w| (w.author, w.rkey)).chain(region.iter().map(|r| (r.author, r.rkey))).unzip();
        let holed: bool = sqlx::query_scalar(HOLES_SQL).bind(&authors).bind(&rkeys).fetch_one(pool).await?;
        if holed {
            return Ok(Read::Fallback(Use::Hole));
        }
        // 6. The gate over the walk.
        let walk_uris: Vec<String> = walk.iter().map(|w| post_uri(&w.did, w.rkey)).collect();
        let gates = h.gate_many(req, &walk_uris).await?;
        // 7. The stored rows.
        let mut parents: Vec<String> = vec![walk_uris[0].clone()];
        parents.extend(region.iter().filter(|r| !r.deleted && r.lvl < levels).map(|r| post_uri(&r.did, r.rkey)));
        let (after_at, after_uri) = match &after {
            Some((at, uri)) => (Some(*at), uri.clone()),
            None => (None, String::new()),
        };
        let stored: Vec<StoredRow> = sqlx::query_as(STORED_SQL)
            .bind(&parents)
            .bind(&walk_uris)
            .bind(after_at)
            .bind(&after_uri)
            .bind(i64::from(cfg.max_tree) + 1)
            .fetch_all(pool)
            .await?;
        if stored.len() > max_tree {
            self.remember_too_big(anchor_key);
            return Ok(Read::Fallback(Use::TooBig));
        }
        let mut tree = Tree { walked: parents.iter().cloned().collect(), ..Tree::default() };
        for (w, uri) in walk.iter().zip(&walk_uris) {
            let hidden = gates.get(uri).is_some_and(|g| !g.listable());
            tree.chain.push(ChainNode { uri: uri.clone(), deleted: w.deleted, hidden });
            tree.known.insert(uri.clone());
        }
        for r in &region {
            let uri = post_uri(&r.did, r.rkey);
            tree.known.insert(uri.clone());
            tree.children
                .entry(post_uri(&r.parent_did, r.parent_rkey))
                .or_default()
                .push(Child { uri, created_at: r.created_at, deleted: r.deleted });
        }
        for kids in tree.children.values_mut() {
            kids.sort_by(|a, b| (a.created_at, a.uri.as_bytes()).cmp(&(b.created_at, b.uri.as_bytes())));
        }
        // The miss rule (R4-7): a stored reply in the region with no row is
        // a miss only once it was indexed well before `through`.
        let parent_set: HashSet<&str> = parents.iter().map(String::as_str).collect();
        let cutoff = self.outline.through_ms().and_then(|ms| DateTime::from_timestamp_millis(ms - CLOCK_MARGIN_MS)).unwrap_or(DateTime::<Utc>::MIN_UTC);
        let mut suspects: Vec<String> = Vec::new();
        for row in &stored {
            tree.local.insert(row.uri.clone());
            let Some(parent) = row.reply_parent_uri.as_deref().filter(|p| parent_set.contains(p)) else {
                continue;
            };
            if !tree.known.contains(&row.uri) && row.indexed_at < cutoff {
                suspects.push(row.uri.clone());
            }
            tree.stored.entry(parent.to_string()).or_default().push(Stored {
                uri: row.uri.clone(),
                created_at: row.created_at,
                after_cursor: row.after_cursor.unwrap_or(false),
            });
        }
        if !suspects.is_empty() {
            let present = with_rows(pool, &suspects).await?;
            if suspects.iter().any(|s| !present.contains(s)) {
                return Ok(Read::Fallback(Use::Hole));
            }
        }
        Ok(Read::Tree(Box::new(tree)))
    }
}
