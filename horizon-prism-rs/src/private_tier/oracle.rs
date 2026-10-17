//! Per-record oracle snapshot (design-v2 §6.4). Built asynchronously before
//! the `AuthContext` from a fixed set of queries, then answers kryphocron's
//! synchronous `BlockOracle`, `AudienceOracle` and `MuteOracle` calls from
//! memory.
//!
//! `AudienceOracle::audience_state` is the read rule of
//! docs/internal/design/private-threads.md §2.2 (first matching rule wins).
//! A thread reply is read by the readers of its root's **current** audience
//! G, and a reply in a private branch under a public post by those of its
//! branch root's; O is G's owner, the root's (or branch root's) author:
//! 1. viewer == the post's author W → `InAudience` (self-read), whoever has
//!    blocked or removed the author: only the moderation gate, which the
//!    pipeline applies first, hides a post from its author;
//! 2. no `private_posts` row → `NoAudienceConfigured`;
//! 3. no `audience_uri` → `NoAudienceConfigured`;
//! 4. a block either way between the viewer and W (the bind's
//!    requester–owner stage, mirrored in fact 8);
//! 5. anchoring, for every post: a valid anchor (a stored row of the post's
//!    role's kind, naming an audience of its own author, that the gate
//!    shows); the post names an audience of O; when W ≠ O, or the post is a
//!    branch reply, its chain of parents passes the walk, at most
//!    `private_read.max_chain` stored rows, each step agreeing on the root,
//!    under an audience of O, shown by the gate and, when not by O, by a
//!    writer with no block either way with the viewer or O who stands under
//!    G now; when W ≠ O, O is active and W stands under G;
//! 6. no row for G → `NotInAudience`;
//! 7. a block either way between the viewer and O (unless the viewer or W
//!    is O), or between W and O (unless W is O) → `NotInAudience`;
//! 8. by G's `mode`, matched exactly: `list` or none, viewer ∈ `members`;
//!    `followers`, the viewer is O or a follow record by the viewer naming O
//!    is stored; `everyone`, always; `following`, the viewer is O, or a
//!    follow record by O naming the viewer is stored and O's
//!    follow-verification flag holds; anything else → `NotInAudience`.
//!
//! Rules 4 to 8 are fact 8, the one-row case of the candidate statement the
//! prefilter runs (`schema::fact8`); `inside` and the guarded `blocked`
//! restate rules 7 and 8 over G. "Stands" is the PDS's write rule for the
//! mode: `list` or none, listed in `members`; `followers`, a stored follow
//! of O; `everyone`, always; `following`, O's stored follow with O's flag.
//! These are Aurora-Locus's write rules applied on reads, checked live: Prism is
//! stricter than Aurora-Locus, which checks a writer once at write and
//! trusts the reply's own audience on a direct read.

use std::time::{Duration, SystemTime};

use kryphocron::oracle::{AudienceOracle, BlockOracle, MuteOracle};
use kryphocron::{
    AudienceOracleQuery, AudienceState, BlockOracleQuery, BlockState, Did, MuteOracleQuery, MuteState, ResourceId,
};
use sqlx::PgPool;

use super::schema;

/// Every snapshot answer is in memory (§6.4).
pub(super) const WORST_CASE_LATENCY: Duration = Duration::from_millis(1);

/// The facts one private read depends on.
#[derive(Debug, Clone)]
pub(super) struct OracleSnapshot {
    requester: String,
    resource: ResourceId,
    audience: AudienceState,
    /// The governing audience as the view shows it (§2.6), if at all.
    view_audience: Option<String>,
    requester_blocks_owner: bool,
    owner_blocks_requester: bool,
    muted: bool,
    last_synced: SystemTime,
    bound: Duration,
}

/// Inputs for [`OracleSnapshot::load`].
pub(super) struct SnapshotInputs<'a> {
    pub requester: &'a str,
    pub resource: &'a ResourceId,
    pub uri: &'a str,
    /// `private_watermark`; `None` (no watermark yet) is infinitely stale.
    pub watermark: Option<SystemTime>,
    pub bound: Duration,
    /// `private_read.max_chain`, the walk's cap (private-threads §2.3).
    pub max_chain: u32,
}

impl OracleSnapshot {
    /// Load the snapshot: the same fixed set of statements for every post
    /// (no audience, its owner's audience or another's, any mode), whether
    /// or not rows exist (§6.4; private-posting §3.7 "The statement set is
    /// fixed"), so latency varies little with existence.
    ///
    /// # Errors
    ///
    /// Database errors (the caller maps them to the uniform miss).
    pub(super) async fn load(pool: &PgPool, input: SnapshotInputs<'_>) -> Result<Self, sqlx::Error> {
        let owner = input.resource.did().as_str();
        // Fact 1.
        let facts = schema::post_facts(pool, input.uri).await?;
        // Fact 8: the read rule's verdict, as the one-row case of the
        // candidate statement (private-threads design §3.2): O, G, stage 1
        // and the walk.
        let f8 = schema::fact8(pool, input.requester, input.uri, input.max_chain).await?;
        // Fact 2 and the relation facts, bound to fact 8's G and O, loaded
        // on every load whatever the mode, so that the mode adds no
        // difference of its own. With no anchor (`''`, the post's owner
        // standing in) the answers go unread.
        let audience_row = schema::audience_row(pool, f8.gov.as_deref().unwrap_or("")).await?;
        let relation_owner = f8.owner.clone().filter(|d| !d.is_empty()).unwrap_or_else(|| owner.to_string());
        let viewer_follows = schema::follows(pool, input.requester, &relation_owner).await?;
        let viewer_block = schema::blocks_between(pool, input.requester, &relation_owner).await?;
        let author_block = schema::blocks_between(pool, owner, &relation_owner).await?;
        // Facts 10–12 (S10), one statement: O follows the requester, and
        // the post's owner, and its follow-verification flag holds.
        let (owner_follows_viewer, _) = schema::following_facts(pool, &relation_owner, input.requester, owner).await?;
        let relation_owner_active = schema::account_active(pool, &relation_owner).await?;
        let o = relation_owner.as_str();
        // Rule 8 for the requester, over G, as HEAD's arms.
        let inside = audience_row.as_ref().is_some_and(|a| match a.mode.as_deref().unwrap_or("list") {
            "list" => a.members.iter().any(|m| m == input.requester),
            "followers" => input.requester == o || viewer_follows,
            "everyone" => true,
            "following" => input.requester == o || owner_follows_viewer,
            _ => false,
        });
        // Rule 7 against O: a block either way between the requester and O
        // (unless the requester or the post's owner is O), or between the
        // post's owner and O (unless that owner is O).
        let author_blocked = o != owner && (author_block.0 || author_block.1);
        let blocked = (input.requester != o && o != owner && (viewer_block.0 || viewer_block.1)) || author_blocked;
        let audience = if input.requester == owner {
            // Rule 1: the author reads its own post (the pipeline's gate has
            // already applied), whoever blocked or removed it.
            AudienceState::InAudience
        } else if facts.as_ref().is_none_or(|f| f.owner != owner || f.audience_uri.is_none()) {
            // Rules 2 and 3: no row, or no audience.
            AudienceState::NoAudienceConfigured
        } else if f8.gov.is_none() || audience_row.is_none() {
            // Rule 5 found no anchor, or rule 6: G's row is missing.
            AudienceState::NotInAudience
        } else {
            // Rule 5 is fact 8's verdict; condition 4 applies only when O is
            // not the post's owner; `inside` and the guarded `blocked`
            // mirror rules 7 and 8 over G.
            let active = o == owner || relation_owner_active;
            if f8.verdict && active && inside && !blocked { AudienceState::InAudience } else { AudienceState::NotInAudience }
        };
        // The view's `audience` (§2.6) is G. On the author's own read it
        // shows only when the author is O, or passes rules 6 to 8.
        let view_audience = f8.gov.clone().filter(|_| {
            f8.anchored && (o == owner || (audience_row.is_some() && !author_blocked && inside))
        });
        let (requester_blocks_owner, owner_blocks_requester) = schema::blocks_between(pool, input.requester, owner).await?;
        // A full mute only: a mute scoped to reposts or quote posts
        // (design-v3 §5.3) is not a mute of the account.
        let muted: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM mutes WHERE viewer_did = $1 AND subject_did = $2 AND scope = 'full')",
        )
            .bind(input.requester)
            .bind(owner)
            .fetch_one(pool)
            .await?;
        Ok(Self {
            requester: input.requester.to_string(),
            resource: input.resource.clone(),
            audience,
            view_audience,
            requester_blocks_owner,
            owner_blocks_requester,
            muted,
            last_synced: input.watermark.unwrap_or(SystemTime::UNIX_EPOCH),
            bound: input.bound,
        })
    }

    /// The audience the post's view names (private-threads design §2.6):
    /// the governing audience G, or none.
    pub(super) fn view_audience(&self) -> Option<&str> {
        self.view_audience.as_deref()
    }

    fn is_pair(&self, a: &Did, b: &Did) -> Option<bool> {
        let owner = self.resource.did().as_str();
        match (a.as_str(), b.as_str()) {
            (x, y) if x == self.requester && y == owner => Some(true),
            (x, y) if x == owner && y == self.requester => Some(false),
            _ => None,
        }
    }
}

impl BlockOracle for OracleSnapshot {
    fn block_state(&self, a: &Did, b: &Did) -> BlockState {
        // Only requester-vs-owner facts are loaded; any other pair fails
        // closed.
        let Some(requester_first) = self.is_pair(a, b) else {
            return BlockState::Mutual;
        };
        let (a_blocks_b, b_blocks_a) = if requester_first {
            (self.requester_blocks_owner, self.owner_blocks_requester)
        } else {
            (self.owner_blocks_requester, self.requester_blocks_owner)
        };
        match (a_blocks_b, b_blocks_a) {
            (true, true) => BlockState::Mutual,
            (true, false) => BlockState::OneWay {
                blocker: a.clone(),
                blocked: b.clone(),
            },
            (false, true) => BlockState::OneWay {
                blocker: b.clone(),
                blocked: a.clone(),
            },
            (false, false) => BlockState::None,
        }
    }

    fn last_synced_at(&self) -> SystemTime {
        self.last_synced
    }

    fn data_freshness_bound(&self) -> Duration {
        self.bound
    }

    fn worst_case_latency_for(&self, _query: BlockOracleQuery) -> Duration {
        WORST_CASE_LATENCY
    }
}

impl AudienceOracle for OracleSnapshot {
    fn audience_state(&self, viewer: &Did, resource: &ResourceId) -> AudienceState {
        if viewer.as_str() == self.requester && resource == &self.resource {
            self.audience
        } else {
            AudienceState::NotInAudience
        }
    }

    fn last_synced_at(&self) -> SystemTime {
        self.last_synced
    }

    fn data_freshness_bound(&self) -> Duration {
        self.bound
    }

    fn worst_case_latency_for(&self, _query: AudienceOracleQuery) -> Duration {
        WORST_CASE_LATENCY
    }
}

impl MuteOracle for OracleSnapshot {
    fn mute_state(&self, viewer: &Did, target: &Did) -> MuteState {
        if self.muted && viewer.as_str() == self.requester && target == self.resource.did() {
            MuteState::Muted
        } else {
            MuteState::None
        }
    }

    fn last_synced_at(&self) -> SystemTime {
        self.last_synced
    }

    fn data_freshness_bound(&self) -> Duration {
        self.bound
    }

    fn worst_case_latency_for(&self, _query: MuteOracleQuery) -> Duration {
        WORST_CASE_LATENCY
    }
}
