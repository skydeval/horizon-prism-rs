//! The interaction rule: where data about interactions with a post or an
//! account comes from. Counts, likers, reposters, quoters, replies,
//! followers, known followers and likers, and inbound list blocks are read
//! from today's sources (Constellation, the cascade, Farsight) unless full
//! indexing holds all of them: breadth Full, the group on and its history
//! complete, no blocking gap open, and no history cutoff. Counts also wait
//! for the counts repair that follows a group turning complete, and stay on
//! Constellation under `[full] counts = "constellation"`.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::aggregations::counts::Counts;
use crate::config::FullGroup;
use crate::indexing::crawl::groups::Groups;

/// Where one request's interaction data comes from. The default is today's
/// sources for everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Interactions {
    /// Groups whose interaction data is read locally.
    local: HashSet<FullGroup>,
    /// Groups whose counts are read locally.
    counts: HashSet<FullGroup>,
    /// Inbound list blocks are read from stored rows.
    list_blocks: bool,
}

impl Interactions {
    /// Listings and lookups of `group` (likers for `likes`, replies and
    /// quoters for `posts`, followers for `follows`) read locally.
    #[must_use]
    pub fn local(&self, group: FullGroup) -> bool {
        self.local.contains(&group)
    }

    /// `group`'s counts read locally.
    #[must_use]
    pub fn counts(&self, group: FullGroup) -> bool {
        self.counts.contains(&group)
    }

    /// Inbound list blocks read from stored rows instead of Farsight.
    #[must_use]
    pub fn list_blocks(&self) -> bool {
        self.list_blocks
    }

    /// Whether every count of `target` (a post URI, or an account's DID) is
    /// read locally, so Constellation is not asked for it.
    #[must_use]
    pub fn counts_all_local(&self, target: &str) -> bool {
        if target.starts_with("did:") {
            self.counts(FullGroup::Follows)
        } else {
            self.counts(FullGroup::Likes) && self.counts(FullGroup::Reposts) && self.counts(FullGroup::Posts)
        }
    }

    /// Clear the fields of `c` that are read locally, so a caller's
    /// `remote.or(local)` takes the local value.
    pub fn mask(&self, c: &mut Counts) {
        if self.counts(FullGroup::Likes) {
            c.likes = None;
        }
        if self.counts(FullGroup::Reposts) {
            c.reposts = None;
        }
        if self.counts(FullGroup::Posts) {
            c.replies = None;
            c.quotes = None;
        }
        if self.counts(FullGroup::Follows) {
            c.followers = None;
        }
    }

    /// Whether any count is read locally.
    #[must_use]
    pub fn any_counts(&self) -> bool {
        !self.counts.is_empty()
    }

    /// The rule over its inputs: the enabled groups, the stored histories
    /// at `crawl_generation`, whether a blocking gap is open, whether counts
    /// may read locally at all, and whether every account's blocks are
    /// stored.
    #[must_use]
    pub fn decide(kinds: &[FullGroup], groups: &Groups, crawl_generation: u64, gap_open: bool, counts_auto: bool, blocks_local: bool) -> Self {
        let mut out = Self::default();
        if gap_open || u64::try_from(groups.crawl_generation).ok() != Some(crawl_generation) {
            return out;
        }
        for g in kinds {
            let Some(h) = groups.groups.get(g.as_str()) else {
                continue;
            };
            if h.enabled() && h.complete() {
                out.local.insert(*g);
                if counts_auto && h.counts_ready {
                    out.counts.insert(*g);
                }
            }
        }
        out.list_blocks = blocks_local && out.local(FullGroup::Blocks) && out.local(FullGroup::Lists);
        out
    }
}

const STATE_SQL: &str = "SELECT (SELECT value FROM worker_state WHERE key = 'full_groups'), \
     EXISTS (SELECT 1 FROM full_gaps g WHERE g.resolved_at IS NULL AND g.blocking)";

/// The local counts of `targets`: post URIs from `post_counts`, account DIDs
/// from `actor_counts`; a target with no row counts zero. Only the fields
/// `ix` reads locally are set.
const POST_COUNTS_SQL: &str = "SELECT u.uri, c.like_count, c.repost_count, c.reply_count, c.quote_count \
     FROM unnest($1::text[]) AS u(uri) CROSS JOIN LATERAL ref_of(u.uri) s \
     LEFT JOIN post_counts c ON c.author = s.author AND c.rkey = s.rkey AND s.coll = 1";
const ACTOR_COUNTS_SQL: &str = "SELECT u.did, c.followers_count FROM unnest($1::text[]) AS u(did) \
     LEFT JOIN dids d ON d.did = u.did LEFT JOIN actor_counts c ON c.id = d.id";

/// Of `dids`, those that block `me` through a moderation list `me` is on,
/// with the first such list: `list_blocks` naming a `lists` row of purpose
/// `modlist` (the reference's collection compared) that has a `list_items`
/// row for `me`.
const INBOUND_LIST_BLOCKS_SQL: &str = "SELECT DISTINCT ON (ba.did) ba.did, at_uri(l.author, $3::smallint, l.rkey) \
     FROM list_items i \
     JOIN lists l ON l.author = i.author AND l.rkey = i.list_rkey AND l.purpose = $4 \
     JOIN list_blocks b ON b.subj_coll = $3 AND b.subj_author = l.author AND b.subj_rkey = l.rkey \
     JOIN dids ba ON ba.id = b.author \
     WHERE i.subject = (SELECT id FROM dids WHERE did = $1) AND ba.did = ANY($2) AND ba.did <> $1 \
     ORDER BY ba.did, b.created_at, b.rkey";

/// The interaction rule for this moment, for a caller with no hydrator
/// (a background job). Outside breadth Full (or not On) everything reads
/// today's sources and nothing is queried.
pub(crate) async fn current(scope: &crate::aggregations::ScopeState, pool: &sqlx::PgPool) -> Interactions {
    let (kinds, generation, counts_auto, blocks_local) = {
        let sets = scope.read_guard().await;
        let full = sets.public.breadth == crate::indexing::Breadth::Full && sets.public.mode.is_on();
        if !full || sets.full.history_cutoff {
            return Interactions::default();
        }
        (sets.full.kinds.clone(), sets.public.crawl_generation, sets.full.counts_auto, sets.full.blocks_local)
    };
    match sqlx::query_as::<_, (Option<Value>, bool)>(STATE_SQL).fetch_one(pool).await {
        Ok((Some(v), gap_open)) => Interactions::decide(&kinds, &Groups::parse(&v), generation, gap_open, counts_auto, blocks_local),
        Ok((None, _)) => Interactions::default(),
        Err(error) => {
            tracing::warn!(event = "full_interactions_read_failed", %error, "reading full's group histories failed; interaction data reads today's sources");
            Interactions::default()
        }
    }
}

impl super::Hydrator {
    /// The interaction rule for this moment. Outside breadth Full (or not
    /// On) everything reads today's sources and nothing is queried.
    pub(crate) async fn interactions(&self) -> Interactions {
        let (kinds, generation, counts_auto, blocks_local) = {
            let sets = self.scope().read_guard().await;
            let full = sets.public.breadth == crate::indexing::Breadth::Full && sets.public.mode.is_on();
            if !full || sets.full.history_cutoff {
                return Interactions::default();
            }
            (sets.full.kinds.clone(), sets.public.crawl_generation, sets.full.counts_auto, sets.full.blocks_local)
        };
        match sqlx::query_as::<_, (Option<Value>, bool)>(STATE_SQL).fetch_one(self.pool()).await {
            Ok((Some(v), gap_open)) => Interactions::decide(&kinds, &Groups::parse(&v), generation, gap_open, counts_auto, blocks_local),
            Ok((None, _)) => Interactions::default(),
            Err(error) => {
                tracing::warn!(event = "full_interactions_read_failed", %error, "reading full's group histories failed; interaction data reads today's sources");
                Interactions::default()
            }
        }
    }

    /// The local counts of those `targets` whose counts `ix` reads locally,
    /// each with only its local fields set (zero when no row holds it).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(crate) async fn local_counts(&self, ix: &Interactions, targets: &[String]) -> Result<HashMap<String, Counts>, sqlx::Error> {
        let mut out = HashMap::new();
        if !ix.any_counts() {
            return Ok(out);
        }
        crate::counts::before_read(self.pool()).await;
        let (dids, posts): (Vec<String>, Vec<String>) = targets.iter().cloned().partition(|t| t.starts_with("did:"));
        if ix.counts(FullGroup::Follows) && !dids.is_empty() {
            let rows: Vec<(String, Option<i32>)> = sqlx::query_as(ACTOR_COUNTS_SQL).bind(&dids).fetch_all(self.pool()).await?;
            for (did, followers) in rows {
                out.insert(did, Counts { followers: Some(followers.unwrap_or(0)), ..Counts::default() });
            }
        }
        if !posts.is_empty() && [FullGroup::Likes, FullGroup::Reposts, FullGroup::Posts].iter().any(|g| ix.counts(*g)) {
            let rows: Vec<PostCountsRow> = sqlx::query_as(POST_COUNTS_SQL).bind(&posts).fetch_all(self.pool()).await?;
            for (uri, likes, reposts, replies, quotes) in rows {
                let n = |v: Option<i32>, g: FullGroup| ix.counts(g).then(|| v.unwrap_or(0));
                out.insert(
                    uri,
                    Counts {
                        likes: n(likes, FullGroup::Likes),
                        reposts: n(reposts, FullGroup::Reposts),
                        replies: n(replies, FullGroup::Posts),
                        quotes: n(quotes, FullGroup::Posts),
                        followers: None,
                    },
                );
            }
        }
        Ok(out)
    }

    /// Of `dids`, those that block `me` through a moderation list, from
    /// stored rows, with the list.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(crate) async fn local_inbound_list_blocks(&self, me: &str, dids: &[String]) -> Result<HashMap<String, String>, sqlx::Error> {
        if dids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<(String, String)> = sqlx::query_as(INBOUND_LIST_BLOCKS_SQL)
            .bind(me)
            .bind(dids)
            .bind(crate::db::ids::COLL_LIST)
            .bind(MODLIST)
            .fetch_all(self.pool())
            .await?;
        Ok(rows.into_iter().collect())
    }
}

/// Between `$1` and each of `$2`, from stored rows: direct blocks and blocks
/// through a moderation list, each way (`in`: the other blocks `$1`; `out`:
/// `$1` blocks the other), with the list for a list block.
const PAIR_BLOCKS_SQL: &str = "WITH me AS (SELECT id FROM dids WHERE did = $1), \
       them AS (SELECT d.id, d.did FROM dids d WHERE d.did = ANY($2) AND d.did <> $1) \
     SELECT t.did, true, NULL::text FROM them t JOIN blocks b ON b.author = t.id AND b.subject = (SELECT id FROM me) \
     UNION ALL SELECT t.did, false, NULL::text FROM them t JOIN blocks b ON b.author = (SELECT id FROM me) AND b.subject = t.id \
     UNION ALL SELECT t.did, true, at_uri(l.author, $3::smallint, l.rkey) FROM them t \
       JOIN list_blocks lb ON lb.author = t.id AND lb.subj_coll = $3 \
       JOIN lists l ON l.author = lb.subj_author AND l.rkey = lb.subj_rkey AND l.purpose = $4 \
       JOIN list_items i ON i.author = l.author AND i.list_rkey = l.rkey AND i.subject = (SELECT id FROM me) \
     UNION ALL SELECT t.did, false, at_uri(l.author, $3::smallint, l.rkey) FROM them t \
       JOIN list_items i ON i.subject = t.id \
       JOIN lists l ON l.author = i.author AND l.rkey = i.list_rkey AND l.purpose = $4 \
       JOIN list_blocks lb ON lb.author = (SELECT id FROM me) AND lb.subj_coll = $3 AND lb.subj_author = l.author AND lb.subj_rkey = l.rkey";

/// The blocks between `of` and each of `others`, from stored rows, as the
/// pair answers Farsight gives: every one of `others` is answered (a clean
/// answer means no block either way), and a list block names its list, the
/// smallest URI when there are several. Only sound once the interaction
/// rule reads blocks and lists locally ([`Interactions::list_blocks`]).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn local_pair_blocks(pool: &sqlx::PgPool, of: &str, others: &[String]) -> Result<HashMap<String, crate::farsight::PairAnswer>, sqlx::Error> {
    let mut out: HashMap<String, crate::farsight::PairAnswer> = others.iter().filter(|d| d.as_str() != of).map(|d| (d.clone(), crate::farsight::PairAnswer::clean(true))).collect();
    if out.is_empty() {
        return Ok(out);
    }
    let rows: Vec<(String, bool, Option<String>)> = sqlx::query_as(PAIR_BLOCKS_SQL)
        .bind(of)
        .bind(others)
        .bind(crate::db::ids::COLL_LIST)
        .bind(MODLIST)
        .fetch_all(pool)
        .await?;
    for (did, inbound, list) in rows {
        let Some(a) = out.get_mut(&did) else { continue };
        let r = if inbound { &mut a.inbound } else { &mut a.outbound };
        match list {
            None => r.direct = true,
            Some(l) => {
                if r.list.as_ref().is_none_or(|have| l < *have) {
                    r.list = Some(l);
                }
            }
        }
    }
    Ok(out)
}

/// A post's stored counts: likes, reposts, replies, quotes.
type PostCountsRow = (String, Option<i32>, Option<i32>, Option<i32>, Option<i32>);

/// The moderation-list purpose: only such a list's blocks apply.
pub(crate) const MODLIST: &str = "app.bsky.graph.defs#modlist";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexing::crawl::groups::{COMPLETE, CRAWLING, Group, REMOVED};

    fn groups(entries: &[(&str, &str, bool)]) -> Groups {
        Groups {
            crawl_generation: 3,
            groups: entries
                .iter()
                .map(|(name, state, ready)| {
                    ((*name).to_string(), Group { generation: 1, state: (*state).to_string(), incomplete: false, union_done: true, counts_ready: *ready })
                })
                .collect(),
        }
    }

    #[test]
    fn only_complete_enabled_groups_read_locally() {
        let kinds = [FullGroup::Likes, FullGroup::Posts, FullGroup::Follows, FullGroup::Blocks, FullGroup::Lists];
        let g = groups(&[("likes", COMPLETE, true), ("posts", CRAWLING, false), ("follows", COMPLETE, false), ("blocks", COMPLETE, true), ("lists", COMPLETE, true), ("reposts", COMPLETE, true)]);
        let ix = Interactions::decide(&kinds, &g, 3, false, true, true);
        assert!(ix.local(FullGroup::Likes) && ix.counts(FullGroup::Likes));
        assert!(!ix.local(FullGroup::Posts) && !ix.counts(FullGroup::Posts));
        // Complete, but the counts repair has not run yet.
        assert!(ix.local(FullGroup::Follows) && !ix.counts(FullGroup::Follows));
        // Complete in the histories but not enabled.
        assert!(!ix.local(FullGroup::Reposts));
        assert!(ix.list_blocks());
        // A post's counts are all local only when every counted group is.
        assert!(!ix.counts_all_local("at://did:plc:example/app.bsky.feed.post/3k"));
        assert!(!ix.counts_all_local("did:plc:example"));
        let mut c = Counts { likes: Some(9), reposts: Some(8), replies: Some(7), quotes: Some(6), followers: Some(5) };
        ix.mask(&mut c);
        assert_eq!(c, Counts { likes: None, reposts: Some(8), replies: Some(7), quotes: Some(6), followers: Some(5) });
    }

    #[test]
    fn a_gap_a_generation_or_the_constellation_option_keep_todays_sources() {
        let kinds = FullGroup::ALL;
        let all: Vec<(&str, &str, bool)> = FullGroup::ALL.iter().map(|g| (g.as_str(), COMPLETE, true)).collect();
        let g = groups(&all);
        assert!(Interactions::decide(&kinds, &g, 3, false, true, true).counts_all_local("did:plc:example"));
        assert_eq!(Interactions::decide(&kinds, &g, 3, true, true, true), Interactions::default(), "a blocking gap");
        assert_eq!(Interactions::decide(&kinds, &g, 4, false, true, true), Interactions::default(), "another generation's histories");
        let constellation = Interactions::decide(&kinds, &g, 3, false, false, true);
        assert!(constellation.local(FullGroup::Likes) && !constellation.any_counts());
        assert!(!Interactions::decide(&kinds, &g, 3, false, true, false).list_blocks(), "blocks keep a member side");
        let mut removed = g.clone();
        removed.groups.get_mut("lists").unwrap().state = REMOVED.to_string();
        assert!(!Interactions::decide(&kinds, &removed, 3, false, true, true).list_blocks());
    }
}
