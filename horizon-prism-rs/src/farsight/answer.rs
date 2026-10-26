//! What Farsight said about one pair of accounts, as this AppView reads it
//! (design-v3 §5.14.9), and how a new answer meets a cached one (§5.14.6).
//!
//! This is the only place a `checkBlocks` body is interpreted.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The blocks one account holds against the other.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    /// A direct `app.bsky.graph.block`.
    pub direct: bool,
    /// The first list (the smallest URI) carrying a list block, if any.
    /// Every list Farsight names here is a block list: its purpose is not
    /// looked up.
    pub list: Option<String>,
}

impl Relation {
    /// A direct block, or a list block. The two are independent.
    #[must_use]
    pub fn is_block(&self) -> bool {
        self.direct || self.list.is_some()
    }

    fn parse(v: &Value) -> Option<Self> {
        let direct = v.get("direct")?.as_bool()?;
        let lists = v.get("lists")?.as_array()?;
        let list = match lists.first() {
            None => None,
            Some(first) => Some(first.as_str()?.to_string()),
        };
        Some(Self { direct, list })
    }

    /// This relation with any block `other` names added.
    fn with(&self, other: &Self) -> Self {
        Self {
            direct: self.direct || other.direct,
            list: self.list.clone().or_else(|| other.list.clone()),
        }
    }
}

/// Who wrote a cache entry. A field of the entry, not of the pair answer:
/// the request memo and a remembered block carry none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Farsight,
    /// The fallback AppView (design-v3 §5.14.17). Such an entry always
    /// names an inbound list block, and never stands in for Farsight.
    Fallback,
}

/// Farsight's answer for (viewer, other).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairAnswer {
    /// Blocks the other account holds against the viewer.
    pub inbound: Relation,
    /// Blocks the viewer holds against the other account.
    pub outbound: Relation,
    /// Farsight answered, at coverage `complete` or `assisted`, and did not
    /// flag this pair. Only then does a clean answer mean "no block".
    pub vouched: bool,
    /// Farsight answered and did not flag this pair, at any coverage level.
    /// Thread Lens bridges on it (skydeval 2026-10-25: a response-level
    /// `partial`, such as a short firehose gap being repaired, must not
    /// switch bridging off for every thread). It never removes a block:
    /// only `vouched` does.
    #[serde(default)]
    pub unflagged: bool,
}

impl PairAnswer {
    /// No block, vouched or not.
    #[must_use]
    pub fn clean(vouched: bool) -> Self {
        Self {
            vouched,
            unflagged: vouched,
            ..Self::default()
        }
    }

    /// Whether either direction is a block.
    #[must_use]
    pub fn any_block(&self) -> bool {
        self.inbound.is_block() || self.outbound.is_block()
    }

    /// What a cache entry holds after `new` is written over `live`
    /// (§5.14.6):
    /// - an answer that vouches replaces the entry, so a vouched clean
    ///   answer removes a cached block;
    /// - an answer that does not vouch never removes a block: the entry
    ///   keeps every block it names, gains any the new answer names, and is
    ///   not vouched from then on. If the live entry named no block, the
    ///   new answer replaces it.
    ///
    /// An entry the fallback wrote (design-v3 §5.14.17.6) holds an inbound
    /// list block a third party reported, and is treated apart:
    /// - if the new answer itself names an inbound list block, the entry
    ///   becomes Farsight's: Farsight's list, any other block the entry
    ///   held, Farsight's lifetime;
    /// - if it does not (and does not vouch), the fallback's block stays
    ///   with the fallback's lifetime, the entry stays the fallback's, and
    ///   any other block the answer names is added.
    ///
    /// So an entry is Farsight's only when every block in it came from
    /// Farsight, and a non-vouching answer never removes a block.
    ///
    /// The second value is whether the entry's expiry restarts: it does
    /// when the entry is replaced, and not when it is merged. The third is
    /// the source of the result.
    #[must_use]
    pub fn written_over(new: &Self, live: Option<(&Self, Source)>) -> (Self, bool, Source) {
        match live {
            Some((live, Source::Fallback)) if !new.vouched && new.inbound.list.is_some() => (
                Self {
                    inbound: Relation { direct: new.inbound.direct || live.inbound.direct, list: new.inbound.list.clone() },
                    outbound: new.outbound.with(&live.outbound),
                    vouched: false,
                    unflagged: new.unflagged,
                },
                true,
                Source::Farsight,
            ),
            Some((live, Source::Fallback)) if !new.vouched => (
                Self {
                    inbound: Relation { direct: live.inbound.direct || new.inbound.direct, list: live.inbound.list.clone() },
                    outbound: live.outbound.with(&new.outbound),
                    vouched: false,
                    unflagged: false,
                },
                false,
                Source::Fallback,
            ),
            Some((live, Source::Farsight)) if !new.vouched && live.any_block() => (
                Self {
                    inbound: live.inbound.with(&new.inbound),
                    outbound: live.outbound.with(&new.outbound),
                    vouched: false,
                    unflagged: new.unflagged,
                },
                false,
                Source::Farsight,
            ),
            _ => (new.clone(), true, Source::Farsight),
        }
    }

    /// The pair answer a fallback result "blocked" becomes: an inbound
    /// list block and nothing else, never vouched.
    #[must_use]
    pub fn from_fallback(list: String) -> Self {
        Self {
            inbound: Relation { direct: false, list: Some(list) },
            outbound: Relation::default(),
            vouched: false,
            unflagged: false,
        }
    }
}

/// The response-level coverage of one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    Complete,
    Assisted,
    Partial,
}

impl Coverage {
    /// An unknown level counts as `partial`.
    #[must_use]
    pub fn parse(level: Option<&str>) -> Self {
        match level {
            Some("complete") => Self::Complete,
            Some("assisted") => Self::Assisted,
            _ => Self::Partial,
        }
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Assisted => "assisted",
            Self::Partial => "partial",
        }
    }

    /// Whether a pair the response does not flag is vouched for.
    #[must_use]
    pub fn vouches(self) -> bool {
        matches!(self, Self::Complete | Self::Assisted)
    }
}

/// One `checkBlocks` response, read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAnswer {
    pub coverage: Coverage,
    /// A pair answer for every asked DID that got one. A DID Farsight did
    /// not list has no block.
    pub pairs: HashMap<String, PairAnswer>,
    /// Asked DIDs whose result did not have the expected shape: "no
    /// answer" for that account only.
    pub malformed: HashSet<String>,
    /// `partialFor` reasons, for counting.
    pub partial_reasons: Vec<String>,
}

impl ChunkAnswer {
    /// Read a 200 body for a call that asked about `asked` (canonical
    /// DIDs). `None` when the body as a whole is not a `checkBlocks`
    /// answer.
    #[must_use]
    pub fn read(body: &Value, asked: &[String]) -> Option<Self> {
        let results = body.get("results")?.as_array()?;
        let partial_for = body.get("partialFor")?.as_array()?;
        let coverage = Coverage::parse(body.pointer("/freshness/coverage/level").and_then(Value::as_str));
        let asked_set: HashSet<&str> = asked.iter().map(String::as_str).collect();

        let mut flagged: HashSet<String> = HashSet::new();
        let mut partial_reasons = Vec::new();
        for entry in partial_for {
            if let Some(did) = entry.get("did").and_then(Value::as_str) {
                flagged.insert(did.to_string());
            }
            partial_reasons.extend(
                entry
                    .get("reasons")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|r| r.as_str().map(str::to_string)),
            );
        }

        let mut blocks: HashMap<String, (Relation, Relation)> = HashMap::new();
        let mut malformed: HashSet<String> = HashSet::new();
        for result in results {
            let Some(did) = result.get("did").and_then(Value::as_str) else {
                continue;
            };
            // An account nobody asked about is ignored.
            if !asked_set.contains(did) {
                continue;
            }
            let relations = result
                .get("blocksActor")
                .and_then(Relation::parse)
                .zip(result.get("blockedByActor").and_then(Relation::parse));
            match relations {
                Some(pair) => {
                    blocks.insert(did.to_string(), pair);
                }
                None => {
                    malformed.insert(did.to_string());
                }
            }
        }
        let pairs = asked
            .iter()
            .filter(|did| !malformed.contains(*did))
            .map(|did| {
                let (inbound, outbound) = blocks.remove(did).unwrap_or_default();
                let unflagged = !flagged.contains(did);
                let vouched = coverage.vouches() && unflagged;
                (did.clone(), PairAnswer { inbound, outbound, vouched, unflagged })
            })
            .collect();
        Some(Self {
            coverage,
            pairs,
            malformed,
            partial_reasons,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";
    const D: &str = "did:plc:dddddddddddddddddddddddd";
    const L1: &str = "at://did:plc:llllllllllllllllllllllll/app.bsky.graph.list/1";
    const L2: &str = "at://did:plc:llllllllllllllllllllllll/app.bsky.graph.list/2";

    fn rel(direct: bool, lists: &[&str]) -> Value {
        json!({"direct": direct, "lists": lists})
    }

    fn asked() -> Vec<String> {
        [A, B, C, D].iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_response_becomes_one_pair_answer_per_asked_account() {
        let body = json!({
            "actor": "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv",
            "results": [
                {"did": A, "blocksActor": rel(false, &[L1, L2]), "blockedByActor": rel(false, &[])},
                {"did": B, "blocksActor": rel(true, &[]), "blockedByActor": rel(false, &[L2])},
                {"did": "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz", "blocksActor": rel(true, &[]), "blockedByActor": rel(true, &[])},
            ],
            "partialFor": [{"did": B, "reasons": ["list_pending"]}, {"did": C, "reasons": ["party_debt", "list_capped"]}],
            "freshness": {"coverage": {"level": "assisted"}},
        });
        let read = ChunkAnswer::read(&body, &asked()).expect("answer");
        assert_eq!(read.coverage, Coverage::Assisted);
        assert_eq!(read.pairs.len(), 4);
        // The first list is the block list; the pair is vouched.
        assert_eq!(
            read.pairs[A],
            PairAnswer { inbound: Relation { direct: false, list: Some(L1.into()) }, outbound: Relation::default(), vouched: true, unflagged: true }
        );
        // A flagged pair is not vouched, and its blocks are still blocks.
        assert_eq!(read.pairs[B].inbound, Relation { direct: true, list: None });
        assert_eq!(read.pairs[B].outbound.list.as_deref(), Some(L2));
        assert!(!read.pairs[B].vouched);
        // Not listed: no block. Flagged: not vouched. Unflagged: vouched.
        assert_eq!(read.pairs[C], PairAnswer::clean(false));
        assert_eq!(read.pairs[D], PairAnswer::clean(true));
        assert!(read.malformed.is_empty());
        assert_eq!(read.partial_reasons, ["list_pending", "party_debt", "list_capped"]);
    }

    #[test]
    fn coverage_decides_whether_a_clean_answer_vouches() {
        let body = |level: Value| json!({"results": [], "partialFor": [], "freshness": {"coverage": {"level": level}}});
        for (level, vouched) in [(json!("complete"), true), (json!("assisted"), true), (json!("partial"), false), (json!("brand-new"), false), (Value::Null, false)] {
            let read = ChunkAnswer::read(&body(level.clone()), &asked()).expect("answer");
            assert!(read.pairs.values().all(|p| p.vouched == vouched && p.unflagged && !p.any_block()), "{level}");
        }
    }

    #[test]
    fn a_misshapen_result_is_no_answer_for_that_account_only() {
        let body = json!({
            "results": [
                {"did": A, "blocksActor": {"direct": "yes", "lists": []}, "blockedByActor": rel(false, &[])},
                {"did": B, "blocksActor": rel(false, &[L1])},
                {"did": C, "blocksActor": {"direct": false, "lists": [7]}, "blockedByActor": rel(false, &[])},
                {"did": D, "blocksActor": rel(false, &[L1]), "blockedByActor": rel(false, &[])},
            ],
            "partialFor": [],
            "freshness": {"coverage": {"level": "complete"}},
        });
        let read = ChunkAnswer::read(&body, &asked()).expect("answer");
        assert_eq!(read.malformed, [A, B, C].iter().map(ToString::to_string).collect());
        assert_eq!(read.pairs.keys().collect::<Vec<_>>(), [D]);
        // A body that is not an answer at all.
        for bad in [json!({}), json!({"results": []}), json!({"results": {}, "partialFor": []}), json!("nope")] {
            assert!(ChunkAnswer::read(&bad, &asked()).is_none(), "{bad}");
        }
    }

    fn pair(inbound: Option<&str>, vouched: bool) -> PairAnswer {
        PairAnswer {
            inbound: Relation { direct: false, list: inbound.map(str::to_string) },
            outbound: Relation::default(),
            vouched,
            unflagged: vouched,
        }
    }

    #[test]
    fn a_vouched_answer_replaces_and_an_unvouched_one_never_removes_a_block() {
        use Source::{Fallback, Farsight};
        // Nothing cached: the answer is stored as it is.
        assert_eq!(PairAnswer::written_over(&pair(Some(L1), false), None), (pair(Some(L1), false), true, Farsight));
        // A vouched clean answer removes a cached block, whoever wrote it.
        for source in [Farsight, Fallback] {
            assert_eq!(PairAnswer::written_over(&pair(None, true), Some((&pair(Some(L1), false), source))), (pair(None, true), true, Farsight));
        }
        // An unvouched clean answer keeps the block and its expiry, and the
        // entry no longer vouches.
        assert_eq!(PairAnswer::written_over(&pair(None, false), Some((&pair(Some(L1), true), Farsight))), (pair(Some(L1), false), false, Farsight));
        // An unvouched answer adds its own blocks to the entry's.
        let mut outbound = pair(None, false);
        outbound.outbound.direct = true;
        let (merged, restart, _) = PairAnswer::written_over(&outbound, Some((&pair(Some(L1), false), Farsight)));
        assert_eq!((merged.inbound.list.as_deref(), merged.outbound.direct, merged.vouched, restart), (Some(L1), true, false, false));
        // The entry named no block: the new answer replaces it.
        assert_eq!(PairAnswer::written_over(&pair(Some(L2), false), Some((&pair(None, true), Farsight))), (pair(Some(L2), false), true, Farsight));
    }

    /// design-v3 §5.14.17.6, the last two rows of the merge table.
    #[test]
    fn an_entry_the_fallback_wrote_is_merged_by_its_own_rules() {
        use Source::{Fallback, Farsight};
        let fallback = PairAnswer::from_fallback(L1.to_string());
        assert!(!fallback.vouched && fallback.inbound.list.as_deref() == Some(L1) && !fallback.outbound.is_block());
        // Farsight, not vouching, reports an inbound list block itself: the
        // entry becomes Farsight's, with Farsight's list and lifetime.
        let (merged, restart, source) = PairAnswer::written_over(&pair(Some(L2), false), Some((&fallback, Fallback)));
        assert_eq!((merged.inbound.list.as_deref(), restart, source), (Some(L2), true, Farsight));
        // Farsight, not vouching, reports no inbound list block: the
        // fallback's block and lifetime stay, the entry stays the
        // fallback's, and another block the answer names is added.
        let mut other = pair(None, false);
        other.outbound.direct = true;
        let (merged, restart, source) = PairAnswer::written_over(&other, Some((&fallback, Fallback)));
        assert_eq!((merged.inbound.list.as_deref(), merged.outbound.direct, merged.vouched), (Some(L1), true, false));
        assert_eq!((restart, source), (false, Fallback));
        // That added block is kept when Farsight later names the list.
        let (again, _, source) = PairAnswer::written_over(&pair(Some(L2), false), Some((&merged, Fallback)));
        assert_eq!((again.inbound.list.as_deref(), again.outbound.direct, source), (Some(L2), true, Farsight));
    }
}
