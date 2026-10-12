//! Per-record oracle snapshot (design-v2 §6.4). Built asynchronously before
//! the `AuthContext` from a fixed set of queries, then answers kryphocron's
//! synchronous `BlockOracle`, `AudienceOracle` and `MuteOracle` calls from
//! memory.
//!
//! `AudienceOracle::audience_state` mirrors Aurora-Locus
//! `authorize_private_read` exactly (first matching rule wins):
//! 1. viewer == owner → `InAudience` (self-read);
//! 2. no `private_posts` row → `NoAudienceConfigured`;
//! 3. no `audience_uri` → `NoAudienceConfigured`;
//! 4. audience owner ≠ post owner and not on the same PDS (both endpoints
//!    known and equal, host matching the `community` network's patterns,
//!    audience owner `active`) → `NotInAudience`; an unknown endpoint denies
//!    and re-queues the identity refresher for both DIDs;
//! 5. no `private_audiences` row → `NotInAudience`;
//! 6. `mode` neither NULL nor `list` → `NotInAudience`;
//! 7. viewer ∈ `members` → `InAudience`, else `NotInAudience`.

use std::time::{Duration, SystemTime};

use kryphocron::oracle::{AudienceOracle, BlockOracle, MuteOracle};
use kryphocron::{
    AudienceOracleQuery, AudienceState, BlockOracleQuery, BlockState, Did, MuteOracleQuery, MuteState, ResourceId,
};
use sqlx::PgPool;

use super::schema;
use crate::aggregations::enroll::{host_matches, pds_host};
use crate::db::locks::uri_author;
use crate::identity::RefreshReason;

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
    pub community_patterns: &'a [String],
}

#[derive(sqlx::FromRow)]
struct ActorEndpoint {
    pds_endpoint: Option<String>,
    account_status: String,
}

async fn endpoint(pool: &PgPool, did: &str) -> Result<Option<ActorEndpoint>, sqlx::Error> {
    sqlx::query_as("SELECT pds_endpoint, account_status FROM actors WHERE did = $1")
        .bind(did)
        .fetch_optional(pool)
        .await
}

async fn requeue_identity(pool: &PgPool, dids: [&str; 2]) -> Result<(), sqlx::Error> {
    // Both DIDs already have rows here, so this is bounded by existing rows.
    sqlx::query(
        "INSERT INTO identity_refresh (did, reasons) SELECT d, $2 FROM unnest($1::text[]) AS d \
         ON CONFLICT (did) DO UPDATE SET reasons = identity_refresh.reasons | EXCLUDED.reasons",
    )
    .bind(dids.map(str::to_string).to_vec())
    .bind(RefreshReason::STALE)
    .execute(pool)
    .await?;
    Ok(())
}

/// Rule 4's same-PDS condition.
async fn same_pds(pool: &PgPool, owner: &str, audience_owner: &str, patterns: &[String]) -> Result<bool, sqlx::Error> {
    let (Some(o), Some(a)) = (endpoint(pool, owner).await?, endpoint(pool, audience_owner).await?) else {
        requeue_identity(pool, [owner, audience_owner]).await?;
        return Ok(false);
    };
    let (Some(oh), Some(ah)) = (
        o.pds_endpoint.as_deref().and_then(pds_host),
        a.pds_endpoint.as_deref().and_then(pds_host),
    ) else {
        requeue_identity(pool, [owner, audience_owner]).await?;
        return Ok(false);
    };
    Ok(oh == ah && patterns.iter().any(|p| host_matches(&oh, p)) && a.account_status == "active")
}

impl OracleSnapshot {
    /// Load the snapshot: the same fixed set of queries whether or not rows
    /// exist (§6.4), so latency varies little with existence.
    ///
    /// # Errors
    ///
    /// Database errors (the caller maps them to the uniform miss).
    pub(super) async fn load(pool: &PgPool, input: SnapshotInputs<'_>) -> Result<Self, sqlx::Error> {
        let owner = input.resource.did().as_str();
        let facts = schema::post_facts(pool, input.uri).await?;
        let audience_uri = facts.as_ref().and_then(|f| f.audience_uri.clone());
        let audience_row = match &audience_uri {
            Some(u) => schema::audience_row(pool, u).await?,
            None => None,
        };
        let audience = if input.requester == owner {
            AudienceState::InAudience
        } else if facts.as_ref().is_none_or(|f| f.owner != owner) {
            AudienceState::NoAudienceConfigured
        } else if let Some(uri) = &audience_uri {
            let audience_owner = uri_author(uri).unwrap_or_default();
            if audience_owner != owner && !same_pds(pool, owner, audience_owner, input.community_patterns).await? {
                AudienceState::NotInAudience
            } else {
                match &audience_row {
                    None => AudienceState::NotInAudience,
                    Some(a) if a.mode.as_deref().is_some_and(|m| m != "list") => AudienceState::NotInAudience,
                    Some(a) if a.members.iter().any(|m| m == input.requester) => AudienceState::InAudience,
                    Some(_) => AudienceState::NotInAudience,
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
