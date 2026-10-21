//! Thread Lens verdicts (design-v3 §5.15.4 – §5.15.6).
//!
//! For the viewer and an author, a **verdict** says what the server knows
//! about blocks between them. Thread Lens adds content to the ordinary
//! thread view ("bridging") only for authors that are [`Verdict::Clear`]:
//! known to have no block with the viewer in either direction. Only
//! Farsight can vouch for that; this AppView's own rows are never used to
//! conclude that an author has no list block against the viewer.
//!
//! **The switch.** Bridging is on for a request when Farsight is
//! configured and the request's first round of lookups was answered whole
//! at coverage `complete` or `assisted`. Otherwise nothing is added to
//! the ordinary view on that request.
//!
//! **Rounds.** Verdicts are fetched in rounds through the `farsight`
//! module, which holds the cache and the request's memo: an author is
//! sent at most once per request, and a cached block still applies when
//! Farsight gives no answer.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use sqlx::PgPool;

use crate::config::ThreadLensConfig;
use crate::farsight::lookup::{PairAnswers, PairBudget};
use crate::farsight::{Lookup, PairAnswer};
use crate::hydration::{APPLIES, KnownBlocks};

/// What the server knows about blocks between the viewer and one author.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Verdict {
    /// The viewer blocks the author, directly or through a list.
    BlockedByYou,
    /// The author blocks the viewer, directly or through a list.
    BlockingYou,
    /// Farsight vouches that there is no block either way.
    Clear,
    /// The server cannot say. Nothing by such an author is bridged.
    Undetermined,
}

/// The verdict rules, in order (§5.15.5). `out_local` and `in_local` are
/// rule 2's local blocks; `pair` is the pair answer the request holds for
/// the author, if any.
///
/// Blocks are read from every source before a block verdict is given, and
/// a block in both directions is `BlockedByYou`: the half the viewer can
/// change. A block applies whether or not its pair answer vouches. With no
/// block, the author is `Clear` only if bridging is on and the pair answer
/// vouches.
#[must_use]
pub fn decide(is_viewer: bool, out_local: bool, in_local: bool, pair: Option<&PairAnswer>, bridging: bool) -> Verdict {
    if is_viewer {
        return Verdict::Clear;
    }
    if out_local || pair.is_some_and(|p| p.outbound.is_block()) {
        Verdict::BlockedByYou
    } else if in_local || pair.is_some_and(|p| p.inbound.is_block()) {
        Verdict::BlockingYou
    } else if bridging && pair.is_some_and(|p| p.vouched) {
        Verdict::Clear
    } else {
        Verdict::Undetermined
    }
}

/// One request's verdicts.
pub struct Judge {
    pool: PgPool,
    viewer: String,
    /// `None` when Farsight is not configured: bridging is then off.
    lookup: Option<Lookup>,
    round_timeout: Duration,
    /// Distinct authors that may still be looked up for bridging, beyond
    /// the baseline round's (`threadlens.verdict_max_dids`).
    dids_left: usize,
    bridging: bool,
    baseline_done: bool,
    out_local: HashSet<String>,
    /// Of `out_local`, the authors the viewer blocks directly.
    out_direct: HashSet<String>,
    /// Of `out_local`, the smallest URI of the viewer's applying list-block
    /// references that cover each author.
    out_lists: HashMap<String, String>,
    in_local: HashSet<String>,
    pairs: HashMap<String, PairAnswer>,
    judged: HashSet<String>,
}

impl Judge {
    #[must_use]
    pub fn new(pool: PgPool, viewer: &str, lookup: Option<Lookup>, cfg: &ThreadLensConfig) -> Self {
        Self {
            pool,
            viewer: viewer.to_string(),
            lookup,
            round_timeout: Duration::from_millis(u64::from(cfg.verdict_round_timeout_ms)),
            dids_left: usize::try_from(cfg.verdict_max_dids).unwrap_or(usize::MAX),
            bridging: false,
            baseline_done: false,
            out_local: HashSet::new(),
            out_direct: HashSet::new(),
            out_lists: HashMap::new(),
            in_local: HashSet::new(),
            pairs: HashMap::new(),
            judged: HashSet::new(),
        }
    }

    /// A judge that has already judged: the viewer blocks `by_viewer`,
    /// `of_viewer` block the viewer, and `others` have no known block and
    /// are not vouched for. It never reaches a database or Farsight.
    #[cfg(test)]
    pub(crate) fn judged(viewer: &str, by_viewer: &[&str], of_viewer: &[&str], others: &[&str]) -> Self {
        let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
        let mut judge = Self::new(pool, viewer, None, &ThreadLensConfig::default());
        let own = |dids: &[&str]| dids.iter().map(ToString::to_string).collect::<HashSet<String>>();
        judge.out_local = own(by_viewer);
        judge.out_direct = own(by_viewer);
        judge.in_local = own(of_viewer);
        judge.judged = own(by_viewer).into_iter().chain(own(of_viewer)).chain(own(others)).collect();
        judge.baseline_done = true;
        judge
    }

    /// The viewer the verdicts are for.
    #[must_use]
    pub fn viewer(&self) -> &str {
        &self.viewer
    }

    /// Whether bridging is on. False until the baseline round has run.
    #[must_use]
    pub fn bridging(&self) -> bool {
        self.bridging
    }

    /// The verdict for `did`, or `None` for an author no round has judged.
    #[must_use]
    pub fn verdict(&self, did: &str) -> Option<Verdict> {
        if did == self.viewer {
            return Some(Verdict::Clear);
        }
        self.judged
            .contains(did)
            .then(|| decide(false, self.out_local.contains(did), self.in_local.contains(did), self.pairs.get(did), self.bridging))
    }

    /// Which way a block with `did` runs, as `(the author blocks the
    /// viewer, the viewer blocks the author)`: from the local rows and the
    /// pair answer's fields, not from which rule spoke first.
    #[must_use]
    pub fn directions(&self, did: &str) -> (bool, bool) {
        let pair = self.pairs.get(did);
        (
            self.in_local.contains(did) || pair.is_some_and(|p| p.inbound.is_block()),
            self.out_local.contains(did) || pair.is_some_and(|p| p.outbound.is_block()),
        )
    }

    /// For an author the viewer blocks through a list and not directly,
    /// one such list of the viewer's own: the smallest URI among its
    /// applying list-block references that cover the author and the list
    /// Farsight's `blockedByActor` named (Thread Lens amendment 1 §2A).
    /// `None` for a direct block, and when no list is known. Nothing of the
    /// author's own records is read.
    #[must_use]
    pub fn blocking_list(&self, did: &str) -> Option<String> {
        let pair = self.pairs.get(did);
        if self.out_direct.contains(did) || pair.is_some_and(|p| p.outbound.direct) {
            return None;
        }
        let farsight = pair.and_then(|p| p.outbound.list.as_ref());
        [self.out_lists.get(did), farsight].into_iter().flatten().min().cloned()
    }

    /// Every block the request knows so far, for the views it builds
    /// (§5.15.12).
    #[must_use]
    pub fn known_blocks(&self) -> KnownBlocks {
        let mut known = KnownBlocks::default();
        for did in &self.judged {
            let (inbound, outbound) = self.directions(did);
            if inbound {
                known.of_viewer.insert(did.clone());
            }
            if outbound {
                known.by_viewer.insert(did.clone());
            }
        }
        known
    }

    /// Rule 2 for `authors`: the viewer's direct blocks, its list-block
    /// references that apply, and direct blocks of the viewer.
    async fn local(&mut self, authors: &[String]) -> Result<(), sqlx::Error> {
        // The third column names the list for a list block, and is NULL for
        // a direct one.
        let rows: Vec<(String, bool, Option<String>)> = sqlx::query_as(&format!(
            "SELECT subject_did, true, NULL::text FROM blocks WHERE author_did = $1 AND subject_did = ANY($2) \
             UNION ALL SELECT author_did, false, NULL::text FROM blocks WHERE subject_did = $1 AND author_did = ANY($2) \
             UNION ALL SELECT i.subject_did, true, r.list_uri FROM list_references r \
               JOIN subscribed_lists l ON l.list_uri = r.list_uri \
               JOIN subscribed_list_items i ON i.list_uri = r.list_uri \
               WHERE r.member_did = $1 AND r.source = 'block' AND {APPLIES} AND i.subject_did = ANY($2)"
        ))
        .bind(&self.viewer)
        .bind(authors)
        .fetch_all(&self.pool)
        .await?;
        for (did, outbound, list) in rows {
            if !outbound {
                self.in_local.insert(did);
                continue;
            }
            match list {
                Some(list) => {
                    let smallest = self.out_lists.entry(did.clone()).or_insert_with(|| list.clone());
                    if list < *smallest {
                        *smallest = list;
                    }
                }
                None => {
                    self.out_direct.insert(did.clone());
                }
            }
            self.out_local.insert(did);
        }
        Ok(())
    }

    /// The authors of `authors` this request has not judged yet, once
    /// each, without the viewer.
    fn fresh(&self, authors: &[String]) -> Vec<String> {
        let mut seen = HashSet::new();
        authors
            .iter()
            .filter(|did| **did != self.viewer && !self.judged.contains(*did) && seen.insert(*did))
            .cloned()
            .collect()
    }

    async fn ask(&self, others: &[String], max_calls: u32, force: Option<&str>) -> PairAnswers {
        let budget = PairBudget { max_calls, timeout: self.round_timeout };
        match &self.lookup {
            Some(lookup) => lookup.pair_answers(&self.viewer, others, budget, force).await,
            None => PairAnswers::default(),
        }
    }

    fn take(&mut self, authors: Vec<String>, answers: HashMap<String, PairAnswer>) {
        self.pairs.extend(answers);
        self.judged.extend(authors);
    }

    /// The baseline round: every author of the ordinary view, however
    /// many. It always runs first, and it decides the switch. When every
    /// author is settled from the cache it still makes one call, to read
    /// the level of the viewer's side: for one author Farsight accepts,
    /// or else for the viewer itself.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn baseline(&mut self, authors: &[String]) -> Result<(), sqlx::Error> {
        let authors = self.fresh(authors);
        self.local(&authors).await?;
        let mut got = self.ask(&authors, u32::MAX, None).await;
        if got.calls == 0 && self.lookup.is_some() {
            let one = authors.iter().find(|did| crate::farsight::did::canonical(did).is_some()).cloned();
            let forced = match &one {
                Some(did) => self.ask(std::slice::from_ref(did), 1, Some(did)).await,
                None => self.ask(std::slice::from_ref(&self.viewer), 1, None).await,
            };
            got.vouches = forced.vouches;
            got.answers.extend(forced.answers);
        }
        self.bridging = self.lookup.is_some() && got.vouches;
        self.baseline_done = true;
        self.take(authors, got.answers);
        Ok(())
    }

    /// A later round, for authors found beyond the ordinary view. Returns
    /// false, judging nobody, when the request's allowance of authors is
    /// spent: an allowance running out is not a verdict. With bridging
    /// off no round runs, and local blocks alone are read.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn round(&mut self, authors: &[String]) -> Result<bool, sqlx::Error> {
        debug_assert!(self.baseline_done, "the baseline round runs first");
        let authors = self.fresh(authors);
        if authors.is_empty() {
            return Ok(true);
        }
        if authors.len() > self.dids_left {
            return Ok(false);
        }
        self.dids_left -= authors.len();
        self.local(&authors).await?;
        let answers = if self.bridging { self.ask(&authors, u32::MAX, None).await.answers } else { HashMap::new() };
        self.take(authors, answers);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::farsight::Relation;
    use crate::farsight::lookup::tests::{LIST, V, engine};
    use crate::test_support::fresh_db;
    use crate::xrpc::pools::{ChargeKey, Requester};

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";
    const D: &str = "did:plc:dddddddddddddddddddddddd";

    fn pair(inbound: bool, outbound: bool, vouched: bool) -> PairAnswer {
        let relation = |on: bool| Relation { direct: false, list: on.then(|| LIST.to_string()) };
        PairAnswer { inbound: relation(inbound), outbound: relation(outbound), vouched }
    }

    #[test]
    fn the_rules_in_order() {
        use Verdict::{BlockedByYou, BlockingYou, Clear, Undetermined};
        // 1. The viewer is never its own blocker.
        assert_eq!(decide(true, true, true, None, false), Clear);
        // 2. Local blocks, whatever Farsight says.
        assert_eq!(decide(false, true, false, Some(&pair(false, false, true)), true), BlockedByYou);
        assert_eq!(decide(false, false, true, None, false), BlockingYou);
        // 3 and 4. A block in the pair answer, vouched or not.
        assert_eq!(decide(false, false, false, Some(&pair(true, false, false)), true), BlockingYou);
        assert_eq!(decide(false, false, false, Some(&pair(false, true, false)), false), BlockedByYou);
        // Both directions, by any mix of rules: the half the viewer can change.
        assert_eq!(decide(false, false, true, Some(&pair(false, true, true)), true), BlockedByYou);
        assert_eq!(decide(false, false, false, Some(&pair(true, true, true)), true), BlockedByYou);
        // 5. Clear needs bridging on and a vouching, clean answer.
        assert_eq!(decide(false, false, false, Some(&pair(false, false, true)), true), Clear);
        assert_eq!(decide(false, false, false, Some(&pair(false, false, true)), false), Undetermined);
        assert_eq!(decide(false, false, false, Some(&pair(false, false, false)), true), Undetermined);
        assert_eq!(decide(false, false, false, None, true), Undetermined);
    }

    fn dids(all: &[&str]) -> Vec<String> {
        all.iter().map(ToString::to_string).collect()
    }

    /// The baseline round decides the switch; later rounds judge more
    /// authors within the allowance; with Farsight not vouching, or not
    /// configured, nobody is `Clear` and local blocks still apply.
    #[tokio::test]
    async fn rounds_and_the_switch() {
        let Some(db) = fresh_db("threadlens_verdict").await else {
            return;
        };
        let p = &db.pool;
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert(A.to_string());
        // The viewer blocks C directly; D blocks the viewer.
        for (author, subject, rkey) in [(V, C, "1"), (D, V, "2")] {
            sqlx::query("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, '3k', now())")
                .bind(format!("at://{author}/app.bsky.graph.block/{rkey}"))
                .bind(author)
                .bind(subject)
                .execute(p)
                .await
                .expect("block");
        }
        let cfg = ThreadLensConfig { verdict_max_dids: 2, ..ThreadLensConfig::default() };
        let lookup = || Some(farsight.request(&Requester::new(ChargeKey::member(V))));
        let calls = || fake.calls.load(std::sync::atomic::Ordering::SeqCst);

        let mut judge = Judge::new(p.clone(), V, lookup(), &cfg);
        assert_eq!(judge.verdict(A), None, "nobody is judged before a round");
        judge.baseline(&dids(&[A, B, V, A])).await.expect("baseline");
        assert!(judge.bridging());
        assert_eq!((judge.verdict(A), judge.verdict(B), judge.verdict(V)), (Some(Verdict::BlockingYou), Some(Verdict::Clear), Some(Verdict::Clear)));
        assert_eq!(judge.verdict(C), None);
        // A later round: local blocks win; an author already judged is not
        // sent again; the allowance of two authors is then spent.
        assert!(judge.round(&dids(&[C, D, A])).await.expect("round"));
        assert_eq!((judge.verdict(C), judge.verdict(D)), (Some(Verdict::BlockedByYou), Some(Verdict::BlockingYou)));
        assert_eq!(calls(), 2);
        let more = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
        assert!(!judge.round(&dids(&[more])).await.expect("round"));
        assert_eq!(judge.verdict(more), None, "an allowance running out is not a verdict");

        // Everything cached: the baseline still makes one call for the
        // level, and bridging is on.
        let mut again = Judge::new(p.clone(), V, lookup(), &cfg);
        again.baseline(&dids(&[A, B])).await.expect("baseline");
        assert!(again.bridging());
        assert_eq!(calls(), 3);
        // Nobody but the viewer: the viewer itself is sent.
        let mut alone = Judge::new(p.clone(), V, lookup(), &cfg);
        alone.baseline(&dids(&[V])).await.expect("baseline");
        assert!(alone.bridging());
        assert_eq!(fake.asked.lock().expect("asked").last().expect("asked"), &[V.to_string()]);

        // Farsight answers but does not vouch: bridging is off, nobody is
        // Clear, a block it names still applies, and no later round calls.
        *fake.level.lock().expect("level") = "partial";
        let mut partial = Judge::new(p.clone(), V, lookup(), &cfg);
        partial.baseline(&dids(&[A, B])).await.expect("baseline");
        assert!(!partial.bridging());
        assert_eq!((partial.verdict(A), partial.verdict(B)), (Some(Verdict::BlockingYou), Some(Verdict::Undetermined)));
        let before = calls();
        assert!(partial.round(&dids(&[C])).await.expect("round"));
        assert_eq!((partial.verdict(C), calls()), (Some(Verdict::BlockedByYou), before));

        // Farsight not configured: off, with local blocks only.
        let mut none = Judge::new(p.clone(), V, None, &cfg);
        none.baseline(&dids(&[B, C])).await.expect("baseline");
        assert!(!none.bridging());
        assert_eq!((none.verdict(B), none.verdict(C)), (Some(Verdict::Undetermined), Some(Verdict::BlockedByYou)));
        db.drop_db().await;
    }
}
