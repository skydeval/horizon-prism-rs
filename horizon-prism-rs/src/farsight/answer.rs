//! What Farsight said about one pair of accounts, as this AppView reads it
//! (design-v3 §5.14.9), and how a new answer meets a cached one (§5.14.6).
//!
//! This is the only place a `checkBlocks` body is interpreted.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

/// The blocks one account holds against the other.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

/// Farsight's answer for (viewer, other).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PairAnswer {
    /// Blocks the other account holds against the viewer.
    pub inbound: Relation,
    /// Blocks the viewer holds against the other account.
    pub outbound: Relation,
    /// Farsight answered, at coverage `complete` or `assisted`, and did not
    /// flag this pair. Only then does a clean answer mean "no block".
    pub vouched: bool,
}

impl PairAnswer {
    /// No block, vouched or not.
    #[must_use]
    pub fn clean(vouched: bool) -> Self {
        Self {
            vouched,
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
    /// The second value is whether the entry's expiry restarts: it does
    /// when the entry is replaced, and not when it is merged.
    #[must_use]
    pub fn written_over(new: &Self, live: Option<&Self>) -> (Self, bool) {
        match live {
            Some(live) if !new.vouched && live.any_block() => (
                Self {
                    inbound: live.inbound.with(&new.inbound),
                    outbound: live.outbound.with(&new.outbound),
                    vouched: false,
                },
                false,
            ),
            _ => (new.clone(), true),
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
                let vouched = coverage.vouches() && !flagged.contains(did);
                (did.clone(), PairAnswer { inbound, outbound, vouched })
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
        assert_eq!(read.pairs[A], PairAnswer { inbound: Relation { direct: false, list: Some(L1.into()) }, outbound: Relation::default(), vouched: true });
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
            assert!(read.pairs.values().all(|p| p.vouched == vouched && !p.any_block()), "{level}");
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
        }
    }

    #[test]
    fn a_vouched_answer_replaces_and_an_unvouched_one_never_removes_a_block() {
        // Nothing cached: the answer is stored as it is.
        assert_eq!(PairAnswer::written_over(&pair(Some(L1), false), None), (pair(Some(L1), false), true));
        // A vouched clean answer removes a cached block.
        assert_eq!(PairAnswer::written_over(&pair(None, true), Some(&pair(Some(L1), true))), (pair(None, true), true));
        // An unvouched clean answer keeps the block and its expiry, and the
        // entry no longer vouches.
        assert_eq!(PairAnswer::written_over(&pair(None, false), Some(&pair(Some(L1), true))), (pair(Some(L1), false), false));
        // An unvouched answer adds its own blocks to the entry's.
        let mut outbound = pair(None, false);
        outbound.outbound.direct = true;
        let (merged, restart) = PairAnswer::written_over(&outbound, Some(&pair(Some(L1), false)));
        assert_eq!((merged.inbound.list.as_deref(), merged.outbound.direct, merged.vouched, restart), (Some(L1), true, false, false));
        // The entry named no block: the new answer replaces it.
        assert_eq!(PairAnswer::written_over(&pair(Some(L2), false), Some(&pair(None, true))), (pair(Some(L2), false), true));
    }
}
