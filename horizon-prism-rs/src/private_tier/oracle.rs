//! Per-record oracle snapshot (design-v2 §6.4). Built asynchronously before
//! the `AuthContext` from a fixed set of queries, then answers kryphocron's
//! synchronous `BlockOracle`, `AudienceOracle` and `MuteOracle` calls from
//! memory.
//!
//! `AudienceOracle::audience_state` follows Aurora-Locus
//! `authorize_private_read` (first matching rule wins), with rule 4
//! stricter than it (docs/internal/design/private-posting.md §3.7):
//! 1. viewer == owner → `InAudience` (self-read);
//! 2. no `private_posts` row → `NoAudienceConfigured`;
//! 3. no `audience_uri` → `NoAudienceConfigured`;
//! 4. audience owner ≠ post owner, on any PDS → `NotInAudience` unless all
//!    of: (1) the post hangs from a post by the audience's owner under the
//!    same audience, found by walking stored parents at most
//!    `RULE4_MAX_HOPS` rows, each step one the viewer could read
//!    (`schema::anchored`); (2) the post's owner may write there by
//!    aurora's write rule: `list` or none, listed in `members`;
//!    `followers`, a stored follow of the audience's owner; `everyone`,
//!    always; anything else, no; (3) under `followers` and `everyone`, no
//!    block either way between the post's owner and the audience's owner;
//!    (4) the audience's owner has an `actors` row with status `active`;
//! 5. no `private_audiences` row → `NotInAudience`;
//! 6. for `followers` and `everyone` under an audience whose owner is not
//!    the post's owner: a block either way between that owner and the
//!    viewer (unless the viewer is that owner), or between that owner and
//!    the post's owner → `NotInAudience`;
//! 7. by `mode`, matched exactly: `list` or none, viewer ∈ `members`;
//!    `followers`, the viewer is the audience's owner or a follow record by
//!    the viewer naming that owner is stored; `everyone`, always; anything
//!    else (`nobody`, `following`, an unknown value) → `NotInAudience`.
//!
//! Rules 1–3, 5 and `list` mirror Aurora-Locus `authorize_private_read`.
//! Rule 4 is stricter: aurora trusts a same-PDS reply whatever it replies
//! to, and Prism ingests from any PDS. `followers` and `everyone` go beyond
//! it: the PDS denies every mode but `list`, having no follow graph to ask
//! (docs/internal/design/private-audience-modes.md).

use std::time::{Duration, SystemTime};

use kryphocron::oracle::{AudienceOracle, BlockOracle, MuteOracle};
use kryphocron::{
    AudienceOracleQuery, AudienceState, BlockOracleQuery, BlockState, Did, MuteOracleQuery, MuteState, ResourceId,
};
use sqlx::PgPool;

use super::schema;
use crate::db::locks::uri_author;

/// Every snapshot answer is in memory (§6.4).
pub(super) const WORST_CASE_LATENCY: Duration = Duration::from_millis(1);

/// The facts one private read depends on.
#[derive(Debug, Clone)]
pub(super) struct OracleSnapshot {
    requester: String,
    resource: ResourceId,
    audience: AudienceState,
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
        let facts = schema::post_facts(pool, input.uri).await?;
        let audience_uri = facts.as_ref().and_then(|f| f.audience_uri.clone());
        let parent = facts.as_ref().and_then(|f| f.reply_parent_uri.clone());
        // Loaded on every load, with `''` when the post names no audience.
        let audience_row = schema::audience_row(pool, audience_uri.as_deref().unwrap_or("")).await?;
        // The facts rules 4, 6 and 7 read, loaded whatever the mode so that
        // the mode adds no difference of its own. With no usable audience
        // URI the post's owner stands in, and the answers go unread.
        let relation_owner = audience_uri.as_deref().and_then(uri_author).filter(|d| !d.is_empty()).unwrap_or(owner).to_string();
        let viewer_follows = schema::follows(pool, input.requester, &relation_owner).await?;
        let viewer_block = schema::blocks_between(pool, input.requester, &relation_owner).await?;
        let author_block = schema::blocks_between(pool, owner, &relation_owner).await?;
        let author_follows = schema::follows(pool, owner, &relation_owner).await?;
        let relation_owner_active = schema::account_active(pool, &relation_owner).await?;
        let anchored = schema::anchored_for(
            pool,
            parent.as_deref(),
            audience_uri.as_deref().unwrap_or(""),
            &relation_owner,
            audience_row.as_ref().and_then(|a| a.mode.as_deref()),
            audience_row.as_ref().map_or(&[][..], |a| a.members.as_slice()),
            input.requester,
        )
        .await?;
        let audience = if input.requester == owner {
            AudienceState::InAudience
        } else if facts.as_ref().is_none_or(|f| f.owner != owner) {
            AudienceState::NoAudienceConfigured
        } else if let Some(uri) = &audience_uri {
            let audience_owner = uri_author(uri).unwrap_or_default();
            match &audience_row {
                None => AudienceState::NotInAudience,
                Some(a) => {
                    let mode = a.mode.as_deref().unwrap_or("list");
                    let author_blocked = author_block.0 || author_block.1;
                    // Rule 4, on any PDS: anchored (1), the post's owner
                    // may write here (2) and is not blocked either way
                    // with the audience's owner outside `list` (3), and
                    // that owner is active (4).
                    let may_write = match mode {
                        "list" => a.members.iter().any(|m| m == owner),
                        "followers" => author_follows && !author_blocked,
                        "everyone" => !author_blocked,
                        _ => false,
                    };
                    let rule4 = audience_owner == owner || (anchored && may_write && relation_owner_active);
                    // Rule 6: under these modes a block is the audience
                    // owner's only way to keep one reader out.
                    let blocked = matches!(mode, "followers" | "everyone")
                        && audience_owner != owner
                        && ((input.requester != audience_owner && (viewer_block.0 || viewer_block.1)) || author_blocked);
                    let inside = match mode {
                        "list" => a.members.iter().any(|m| m == input.requester),
                        "followers" => input.requester == audience_owner || viewer_follows,
                        "everyone" => true,
                        _ => false,
                    };
                    if rule4 && inside && !blocked { AudienceState::InAudience } else { AudienceState::NotInAudience }
                }
            }
        } else {
            AudienceState::NoAudienceConfigured
        };
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
            requester_blocks_owner,
            owner_blocks_requester,
            muted,
            last_synced: input.watermark.unwrap_or(SystemTime::UNIX_EPOCH),
            bound: input.bound,
        })
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
