//! Live settings (admin-ui design §8.5): the keys whose change applies at
//! once, without a restart. Built at startup from the effective
//! configuration and swapped by the executor after a `settings set` that
//! touched one of them. The deployment is a single `serve` (lock #9), so
//! that swap is the whole propagation. Each consumer reads the current
//! value when it uses it.
//!
//! Also the restart handle: the `restart` command resolves the same
//! shutdown `serve` waits on at SIGTERM.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use tokio::sync::{Notify, watch};

use crate::config::{FileConfig, RefractCommunity, SuggestedTopic, ThreadLensAccess, UnspeccedConfig};

/// The keys that apply at once (§8.5), besides `indexing.public_scope`,
/// which has its own command.
pub const LIVE_KEYS: [&str; 15] = [
    "community.auto_enroll",
    "community.enroll_pds_patterns",
    "community.max_members",
    "community.max_auto_enroll_per_hour",
    "community.never_ready_expiry_days",
    "threadlens.access",
    "unspecced.check_email_confirmed",
    "unspecced.live_now",
    "trends.descriptions",
    "trends.suggested_topics",
    // Refract (refract design §3.1, C-2).
    "refract.enabled",
    "refract.communities",
    "refract.visitor_viewer_state",
    // The public page at `/`.
    "public_page.enabled",
    "public_page.name",
];

/// Whether `key` applies at once.
#[must_use]
pub fn is_live(key: &str) -> bool {
    LIVE_KEYS.contains(&key)
}

/// The live keys' values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveValues {
    pub auto_enroll: bool,
    /// Empty: the `community` network's patterns.
    pub enroll_pds_patterns: Vec<String>,
    pub max_members: u32,
    pub max_auto_enroll_per_hour: u32,
    pub never_ready_expiry_days: u32,
    pub threadlens_access: ThreadLensAccess,
    /// `getConfig`'s answers and the actor view's live badge.
    pub unspecced: UnspeccedConfig,
    pub trend_descriptions: BTreeMap<String, String>,
    pub suggested_topics: Vec<SuggestedTopic>,
    /// `refract.enabled`.
    pub refract_enabled: bool,
    /// `refract.communities`, the whole list as one value.
    pub refract_communities: Vec<RefractCommunity>,
    /// `refract.visitor_viewer_state`: whether a visitor's own records are
    /// looked up on Constellation with its DID (RF-S1, RF-S12).
    pub refract_visitor_viewer_state: bool,
    /// `public_page.enabled`: whether `/` and `/live` answer.
    pub public_page_enabled: bool,
    /// `public_page.name`: the page's title; empty is the hostname.
    pub public_page_name: String,
}

impl LiveValues {
    /// The live keys of a configuration.
    #[must_use]
    pub fn from_file(f: &FileConfig) -> Self {
        Self {
            auto_enroll: f.community.auto_enroll,
            enroll_pds_patterns: f.community.enroll_pds_patterns.clone(),
            max_members: f.community.max_members,
            max_auto_enroll_per_hour: f.community.max_auto_enroll_per_hour,
            never_ready_expiry_days: f.community.never_ready_expiry_days,
            threadlens_access: f.threadlens.access,
            unspecced: f.unspecced.clone(),
            trend_descriptions: f.trends.descriptions.clone(),
            suggested_topics: f.trends.suggested_topics.clone(),
            refract_enabled: f.refract.enabled,
            refract_communities: f.refract.communities.clone(),
            refract_visitor_viewer_state: f.refract.visitor_viewer_state,
            public_page_enabled: f.public_page.enabled,
            public_page_name: f.public_page.name.clone(),
        }
    }
}

/// The live values, shared by every consumer. Cheap to clone.
#[derive(Clone)]
pub struct LiveSettings {
    values: Arc<ArcSwap<LiveValues>>,
    /// Bumped at every swap, so a consumer that caches a decision (the
    /// enroller's refusals) knows to drop it.
    version: Arc<AtomicU64>,
    /// The version, sent after every swap (refract design §3.2), so a task
    /// that waits for changes never misses a save made while it worked.
    changed: Arc<watch::Sender<u64>>,
}

impl LiveSettings {
    /// The live values of `f`.
    #[must_use]
    pub fn new(f: &FileConfig) -> Self {
        Self {
            values: Arc::new(ArcSwap::from_pointee(LiveValues::from_file(f))),
            version: Arc::new(AtomicU64::new(0)),
            changed: Arc::new(watch::channel(0).0),
        }
    }

    /// The current values.
    #[must_use]
    pub fn load(&self) -> Arc<LiveValues> {
        self.values.load_full()
    }

    /// How many swaps there have been.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Swap in new values (§8.3 step 5).
    pub fn store(&self, values: LiveValues) {
        self.values.store(Arc::new(values));
        let version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
        self.changed.send_replace(version);
    }

    /// A receiver told of every swap after it is made (refract design
    /// §3.2). Several swaps before it looks are seen as one.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// Change some values in place (tests, and a consumer's own setter).
    pub fn update(&self, f: impl FnOnce(&mut LiveValues)) {
        let mut values = (*self.load()).clone();
        f(&mut values);
        self.store(values);
    }
}

impl Default for LiveSettings {
    fn default() -> Self {
        Self::new(&FileConfig::default())
    }
}

impl fmt::Debug for LiveSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveSettings").field("values", &self.load()).field("version", &self.version()).finish()
    }
}

/// The restart from the admin UI (§8.5, C-A4): the `restart` command
/// triggers it after its row is settled `done`; `serve` waits on it beside
/// SIGTERM and then exits with [`crate::serve::EXIT_RESTART`].
#[derive(Clone, Default)]
pub struct RestartHandle {
    inner: Arc<(Notify, AtomicBool)>,
}

impl RestartHandle {
    /// Ask `serve` to shut down and exit for a restart.
    pub fn trigger(&self) {
        self.inner.1.store(true, Ordering::Release);
        self.inner.0.notify_one();
    }

    /// Whether a restart was asked for.
    #[must_use]
    pub fn requested(&self) -> bool {
        self.inner.1.load(Ordering::Acquire)
    }

    /// Resolves once a restart is asked for.
    pub async fn wait(&self) {
        if self.requested() {
            return;
        }
        self.inner.0.notified().await;
    }
}

impl fmt::Debug for RestartHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestartHandle").field("requested", &self.requested()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn swaps_are_seen_and_counted() {
        let live = LiveSettings::default();
        assert_eq!(live.load().threadlens_access, ThreadLensAccess::SignedIn);
        let v0 = live.version();
        live.update(|v| v.threadlens_access = ThreadLensAccess::Members);
        assert_eq!(live.load().threadlens_access, ThreadLensAccess::Members);
        assert!(live.version() > v0);
        assert!(is_live("threadlens.access") && !is_live("retention.tombstone_days"));
        assert!(is_live("refract.enabled") && is_live("refract.communities") && !is_live("refract.grace_days"));
        // The watch channel sees every swap, coalesced.
        let mut rx = live.subscribe();
        live.update(|v| v.refract_enabled = true);
        live.update(|v| v.refract_communities.clear());
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.changed()).await.expect("told").expect("open");
        assert_eq!(*rx.borrow_and_update(), live.version());
        assert!(!rx.has_changed().expect("open"), "two swaps, one wake");
        assert!(live.load().refract_enabled && live.load().refract_communities.is_empty());
        // A trigger before anyone waits is not lost.
        let restart = RestartHandle::default();
        restart.trigger();
        tokio::time::timeout(std::time::Duration::from_secs(1), restart.wait()).await.expect("resolves");
        assert!(restart.requested());
    }
}
