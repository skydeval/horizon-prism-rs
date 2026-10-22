//! Auto-enroll (design-v2 §9.2). Triggered only by authenticated calls to
//! `getTimeline`, `listNotifications`, `getUnreadCount` and
//! `app.nearhorizon.actor.getMembership` (private posting §4.1), the last
//! in every public-indexing mode.
//!
//! Checked before inserting, in order:
//! 0. the DID is not being purged (else `NotReady`, nothing inserted), and
//!    not denied (membership design §2.2: refused `denied`, never cached);
//! 1. its PDS host matches `community.enroll_pds_patterns` (default: the
//!    `community` network's `pds_patterns`) **and** that PDS answers
//!    `getRepoStatus` with `active: true`;
//! 2. `members` count < `community.max_members`;
//! 3. enrollments in the past hour < `community.max_auto_enroll_per_hour`.
//!
//! Refusals are cached in-process for `community.enroll_refusal_cache_secs`
//! and counted in `auto_enroll_refused_total{reason}`. No network call runs
//! under the `ScopeState` guard.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use moka::future::Cache;
use sqlx::PgPool;

use super::ScopeState;
use crate::admin::settings::LiveSettings;
use super::membership::{AddOutcome, add_member, is_denied};
use crate::config::AppConfig;
use crate::identity::refresher::RepoStatus;
use crate::identity::{IdentityResolver, ResolveIntent};
use crate::net::{BodyCap, Intent, NetClient, RequestOptions};
use crate::private_tier::PrivateTier;
use crate::subscriptions::references::Budget;

pub const METRIC_REFUSED: &str = "auto_enroll_refused_total";
/// The refusal of the hourly enrolment cap: transient, never cached.
pub const REFUSED_RATE: &str = "rate";
/// The refusal of a denied DID (membership design §2.2): never cached.
pub const REFUSED_DENIED: &str = "denied";

/// The caller's membership state for a `member` endpoint (§7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Enrollment {
    /// A member; `ready` when `members.ready_at` is set; `reindexing` when
    /// it is not but `first_ready_at` is (readiness cleared after the first
    /// time: a gap, a divergence, a dead letter; degraded-mode design §3.5).
    Member { ready: bool, reindexing: bool },
    /// Just enrolled (not ready yet).
    Enrolled,
    /// Being purged: `NotReady`.
    Purging,
    /// Not enrolled: `NotEnrolled`, with the refusal reason.
    Refused(&'static str),
}

impl Enrollment {
    /// A member from its two readiness columns.
    #[must_use]
    pub fn member(ready: bool, first_ready: bool) -> Self {
        Self::Member { ready, reindexing: !ready && first_ready }
    }
}

/// The member row's readiness as [`Enrollment::member`] reads it:
/// `ready_at` and `first_ready_at` set, or `None` for no row.
pub(crate) const MEMBER_READINESS_SQL: &str = "SELECT ready_at IS NOT NULL, first_ready_at IS NOT NULL FROM members WHERE did = $1";

/// §8.2 PDS matching, shared with the routing table.
pub use crate::constellation::matcher::{host_matches, pds_host};

/// The enroller. Cheap to clone.
#[derive(Clone)]
pub struct AutoEnroller {
    pool: PgPool,
    scope: ScopeState,
    private: Arc<dyn PrivateTier>,
    resolver: IdentityResolver,
    net: NetClient,
    /// `community.auto_enroll`, `max_members`, `max_auto_enroll_per_hour`
    /// and `enroll_pds_patterns`, read at each call (admin-ui design §8.5).
    live: LiveSettings,
    /// The `community` network's patterns, for an empty
    /// `enroll_pds_patterns`. The routing table changes only at a restart.
    community_patterns: Arc<Vec<String>>,
    /// The live values' version the refusal cache was last cleared at: a
    /// change clears it, so it takes effect at the next call.
    seen: Arc<AtomicU64>,
    /// A member's budget of list references (design-v3 §4.3).
    list_budget: Budget,
    refused: Cache<String, &'static str>,
}

impl AutoEnroller {
    /// The enroller. Enrolment runs inside a member's public request, so
    /// its DID resolution and `getRepoStatus` go through the public intent,
    /// whatever `resolver` was built with (design-v4 §11 G-9).
    #[must_use]
    pub fn new(cfg: &AppConfig, pool: PgPool, scope: ScopeState, private: Arc<dyn PrivateTier>, resolver: IdentityResolver) -> Self {
        let resolver = resolver.with_intent(Intent::Public);
        let net = resolver.net().clone();
        let c = &cfg.file.community;
        let community_patterns = cfg
            .file
            .constellation
            .networks
            .iter()
            .find(|n| n.name == crate::config::COMMUNITY_NETWORK)
            .map(|n| n.pds_patterns.clone())
            .unwrap_or_default();
        let live = LiveSettings::new(&cfg.file);
        Self {
            pool,
            scope,
            private,
            resolver,
            net,
            seen: Arc::new(AtomicU64::new(live.version())),
            live,
            community_patterns: Arc::new(community_patterns),
            list_budget: Budget {
                max_references: cfg.file.lists.subscribed_max_per_member,
                max_owners: cfg.file.lists.max_owners_per_member,
            },
            refused: Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(c.enroll_refusal_cache_secs))
                .build(),
        }
    }

    /// Read `[community]` from `live`, the values the admin UI swaps.
    #[must_use]
    pub fn with_live(mut self, live: LiveSettings) -> Self {
        self.seen = Arc::new(AtomicU64::new(live.version()));
        self.live = live;
        self
    }

    /// Drop every cached refusal when the live values changed since the
    /// last call (admin-ui design §8.5's apply hook).
    fn follow_live(&self) {
        let now = self.live.version();
        if self.seen.swap(now, Ordering::AcqRel) != now {
            self.refused.invalidate_all();
        }
    }

    async fn refuse(&self, did: &str, reason: &'static str) -> Enrollment {
        metrics::counter!(METRIC_REFUSED, "reason" => reason).increment(1);
        self.refused.insert(did.to_string(), reason).await;
        Enrollment::Refused(reason)
    }

    /// The `denied` refusal: counted, never cached.
    fn refuse_denied(&self) -> Enrollment {
        metrics::counter!(METRIC_REFUSED, "reason" => REFUSED_DENIED).increment(1);
        Enrollment::Refused(REFUSED_DENIED)
    }

    /// The caller's state, enrolling it if it is eligible.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn check(&self, did: &str) -> Result<Enrollment, sqlx::Error> {
        let row: Option<(bool, bool)> = sqlx::query_as(MEMBER_READINESS_SQL).bind(did).fetch_optional(&self.pool).await?;
        if let Some((ready, first_ready)) = row {
            return Ok(Enrollment::member(ready, first_ready));
        }
        if self.scope.read_guard().await.purge_kind(did).is_some() {
            return Ok(Enrollment::Purging);
        }
        // A denial, first among the refusals and never cached, so `member
        // allow` takes effect at the next call (membership design §3.1).
        if is_denied(&mut *self.pool.acquire().await?, did).await? {
            return Ok(self.refuse_denied());
        }
        self.follow_live();
        if let Some(reason) = self.refused.get(did).await {
            return Ok(Enrollment::Refused(reason));
        }
        let live = self.live.load();
        if !live.auto_enroll {
            return Ok(self.refuse(did, "disabled").await);
        }
        // 1. PDS host and hosting confirmation (network, before any guard).
        let Ok(info) = self.resolver.resolve_info(did, ResolveIntent::Lookup).await else {
            return Ok(self.refuse(did, "unresolvable").await);
        };
        let Some(endpoint) = info.pds_endpoint else {
            return Ok(self.refuse(did, "no_pds").await);
        };
        let Some(host) = pds_host(&endpoint) else {
            return Ok(self.refuse(did, "no_pds").await);
        };
        let patterns = if live.enroll_pds_patterns.is_empty() { self.community_patterns.as_slice() } else { live.enroll_pds_patterns.as_slice() };
        if !patterns.iter().any(|p| host_matches(&host, p)) {
            return Ok(self.refuse(did, "pds_not_community").await);
        }
        let url = format!("https://{host}/xrpc/com.atproto.sync.getRepoStatus?did={did}");
        let active = match self.net.get_json::<RepoStatus>(&url, &RequestOptions::new(BodyCap::REPO_STATUS)).await {
            Ok(Ok(status)) => status.active,
            _ => false,
        };
        if !active {
            return Ok(self.refuse(did, "not_hosted").await);
        }
        self.admit(did).await
    }

    /// Steps 2 and 3, then the insert, for a DID whose PDS passed step 1.
    async fn admit(&self, did: &str) -> Result<Enrollment, sqlx::Error> {
        // 2 and 3. Bounds.
        let (members, recent): (i64, i64) = sqlx::query_as(
            "SELECT count(*), count(*) FILTER (WHERE source = 'auto_enroll' AND added_at > now() - interval '1 hour') FROM members",
        )
        .fetch_one(&self.pool)
        .await?;
        let live = self.live.load();
        if members >= i64::from(live.max_members) {
            return Ok(self.refuse(did, "max_members").await);
        }
        if recent >= i64::from(live.max_auto_enroll_per_hour) {
            // Transient: do not cache the refusal for an hour.
            metrics::counter!(METRIC_REFUSED, "reason" => REFUSED_RATE).increment(1);
            return Ok(Enrollment::Refused(REFUSED_RATE));
        }
        Ok(match add_member(&self.pool, &self.scope, &self.private, did, "auto_enroll", self.list_budget).await? {
            AddOutcome::Added { .. } => {
                tracing::info!(event = "auto_enrolled", did, "member auto-enrolled");
                Enrollment::Enrolled
            }
            AddOutcome::AlreadyMember => Enrollment::Member { ready: false, reindexing: false },
            AddOutcome::Purging => Enrollment::Purging,
            // Denied between the check and the insert.
            AddOutcome::Denied => self.refuse_denied(),
        })
    }

    /// Steps 2 and 3 and the insert, as if step 1's network checks had
    /// passed (tests: no PDS answers here).
    #[cfg(test)]
    pub(crate) async fn admit_for_tests(&self, did: &str) -> Result<Enrollment, sqlx::Error> {
        self.admit(did).await
    }

    /// Cache a refusal for `did`, as [`AutoEnroller::check`] does (tests).
    #[cfg(test)]
    pub(crate) async fn cache_refusal_for_tests(&self, did: &str, reason: &'static str) {
        self.refused.insert(did.to_string(), reason).await;
    }

    /// The cached refusal for `did`, if any (tests).
    #[cfg(test)]
    pub(crate) async fn cached_refusal_for_tests(&self, did: &str) -> Option<&'static str> {
        self.refused.get(did).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_matching() {
        assert!(host_matches("morel.us-east.host.bsky.network", "*.host.bsky.network"));
        assert!(!host_matches("host.bsky.network", "*.host.bsky.network"));
        assert!(!host_matches("evilhost.bsky.network", "*.host.bsky.network"));
        assert!(host_matches("bsky.social", "bsky.social"));
        assert!(!host_matches("x.bsky.social", "bsky.social"));
        assert!(host_matches("PDS.Example.org", "pds.example.org"));
        assert_eq!(pds_host("https://PDS.example.org:8443/").as_deref(), Some("pds.example.org"));
    }
}
