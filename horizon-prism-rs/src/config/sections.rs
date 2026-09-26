//! File-backed configuration sections (`app.toml`).
//!
//! Every struct denies unknown fields, so a typo in a production config fails
//! startup instead of silently falling back to a default (design-v2 §3.2).
//! Defaults equal the values named in design-v2; `docker/config/app.toml`
//! lists every one of them explicitly.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::Deserialize;

/// `[http]`: the XRPC listener (§11.2) and client-IP derivation (§7.4).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    pub bind: SocketAddr,
    /// `X-Forwarded-For` is honored only when the TCP peer equals this address.
    pub trusted_proxy: IpAddr,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([0, 0, 0, 0], 3030)),
            trusted_proxy: IpAddr::V4(Ipv4Addr::new(172, 30, 0, 10)),
        }
    }
}

/// `[metrics]`: the Prometheus listener (§11.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    pub bind: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([0, 0, 0, 0], 9091)),
        }
    }
}

/// `indexing.public_scope` (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicScope {
    #[default]
    CommunityScoped,
    /// Recognized so the config parses, then rejected by validation (lock #4).
    Full,
}

/// `[indexing]`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexingConfig {
    pub public_scope: PublicScope,
}

/// `scope.policy` (§9.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopePolicy {
    MembersOnly,
    MembersAndFollows,
    MembersAndInteractions,
    #[default]
    MembersAndOneHop,
}

/// `[scope]` (§9.2, §9.4).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScopeConfig {
    pub policy: ScopePolicy,
    pub rebuild_grace_days: u32,
    pub max_follow_scope_per_member: u32,
}

impl Default for ScopeConfig {
    fn default() -> Self {
        Self {
            policy: ScopePolicy::MembersAndOneHop,
            rebuild_grace_days: 7,
            max_follow_scope_per_member: 10_000,
        }
    }
}

/// `[community]` (§9.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommunityConfig {
    pub auto_enroll: bool,
    pub max_auto_enroll_per_hour: u32,
    pub max_members: u32,
    pub never_ready_expiry_days: u32,
    pub enroll_refusal_cache_secs: u64,
    /// Empty means the `community` network's `pds_patterns`.
    pub enroll_pds_patterns: Vec<String>,
}

impl Default for CommunityConfig {
    fn default() -> Self {
        Self {
            auto_enroll: true,
            max_auto_enroll_per_hour: 50,
            max_members: 1000,
            never_ready_expiry_days: 7,
            enroll_refusal_cache_secs: 3600,
            enroll_pds_patterns: Vec::new(),
        }
    }
}

/// `[firehose]` (§5.3, §5.11).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FirehoseConfig {
    pub stream_key: String,
    pub group: String,
    pub consumer_name: String,
    pub batch_size: u32,
    pub dead_letter_stream: String,
    pub deterministic_attempts: u32,
    pub dead_letter_maxlen: u64,
    /// Labels (values of `relay_labels`) whose watermarks gate private reads.
    pub private_relays: Vec<String>,
    pub lag_warn_entries: u64,
    pub coverage_window_secs: u64,
    pub coverage_capacity: u64,
    /// Relay URL (the `_relay` field) to short metrics label.
    pub relay_labels: BTreeMap<String, String>,
}

impl Default for FirehoseConfig {
    fn default() -> Self {
        let relay_labels = [
            ("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos", "bsky"),
            ("wss://relay.fire.hose.cam/xrpc/com.atproto.sync.subscribeRepos", "microcosm"),
            ("wss://relay.community-a.example/xrpc/com.atproto.sync.subscribeRepos", "community-a"),
        ]
        .into_iter()
        .map(|(url, label)| (url.to_string(), label.to_string()))
        .collect();
        Self {
            stream_key: "firehose:events".to_string(),
            group: "horizon-prism-rs".to_string(),
            consumer_name: "app".to_string(),
            batch_size: 500,
            dead_letter_stream: "firehose:dead-letters".to_string(),
            deterministic_attempts: 5,
            dead_letter_maxlen: 10_000,
            private_relays: vec!["bsky".to_string(), "microcosm".to_string()],
            lag_warn_entries: 100_000,
            coverage_window_secs: 600,
            coverage_capacity: 3_000_000,
            relay_labels,
        }
    }
}

/// `[identity]` (§6.7).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IdentityConfig {
    /// PLC directory base URL. The only URL allowed to use plain `http` (§8.5).
    pub plc_url: String,
    /// `did_cache` rows are fresh for this long.
    pub did_fresh_secs: u64,
    /// Stale rows are usable (outside the JWT path) until this age.
    pub did_stale_max_secs: u64,
    /// Capacity of the kryphocron issuer tracker (§6.7).
    pub jwt_issuer_tracker_max: u64,
}

impl Default for IdentityConfig {
    fn default() -> Self {
        Self {
            plc_url: "https://plc.directory".to_string(),
            did_fresh_secs: 3600,
            did_stale_max_secs: 86_400,
            jwt_issuer_tracker_max: 20_000,
        }
    }
}

/// `[kryphocron]` (§6.3, §6.4).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KryphocronConfig {
    pub oracle_freshness_bound_secs: u64,
}

impl Default for KryphocronConfig {
    fn default() -> Self {
        Self {
            oracle_freshness_bound_secs: 120,
        }
    }
}

/// `[private_read.floor_ms]`: whole-handler latency floors (§6.9).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrivateReadFloors {
    pub get_posts: u64,
    pub get_post_thread: u64,
    pub get_author_feed: u64,
}

impl Default for PrivateReadFloors {
    fn default() -> Self {
        Self {
            get_posts: 800,
            get_post_thread: 1000,
            get_author_feed: 1000,
        }
    }
}

/// `[private_read]` (§6.9).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrivateReadConfig {
    pub bind_concurrency: u32,
    pub floor_ms: PrivateReadFloors,
}

impl Default for PrivateReadConfig {
    fn default() -> Self {
        Self {
            bind_concurrency: 8,
            floor_ms: PrivateReadFloors::default(),
        }
    }
}

/// `[sync]`: repo-head check budgets (§6.11.3).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncConfig {
    pub head_max_age_secs: u64,
    pub max_head_fetches_per_request: u32,
    pub head_fetch_timeout_ms: u64,
    pub divergence_grace_secs: u64,
    pub max_probe_rate_per_host: u32,
    pub read_fetch_rate_per_host: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            head_max_age_secs: 60,
            max_head_fetches_per_request: 16,
            head_fetch_timeout_ms: 500,
            divergence_grace_secs: 300,
            max_probe_rate_per_host: 25,
            read_fetch_rate_per_host: 20,
        }
    }
}

/// `[db]` (§6.11.7).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DbConfig {
    pub lock_timeout_secs: u64,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            lock_timeout_secs: 30,
        }
    }
}

/// `[admin]` (§3.3).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConfig {
    pub command_timeout_secs: u64,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            command_timeout_secs: 600,
        }
    }
}

/// `[cache]` (§7.6).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub read_time_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            read_time_bytes: 256 * 1024 * 1024,
        }
    }
}

/// `counts.public_source` (§9.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CountsSource {
    Local,
    #[default]
    LocalThenConstellation,
}

/// `[counts]` (§9.6).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CountsConfig {
    pub public_source: CountsSource,
    pub fresh_secs: u64,
    pub stale_max_secs: u64,
    pub sync_timeout_ms: u64,
    pub refresh_rate_per_sec: u32,
    pub refresh_queue_max: u64,
}

impl Default for CountsConfig {
    fn default() -> Self {
        Self {
            public_source: CountsSource::LocalThenConstellation,
            fresh_secs: 300,
            stale_max_secs: 86_400,
            sync_timeout_ms: 800,
            refresh_rate_per_sec: 20,
            refresh_queue_max: 100_000,
        }
    }
}

/// `[microcosm]` (§2.4).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MicrocosmConfig {
    pub slingshot_url: String,
    pub constellation_url: String,
}

impl Default for MicrocosmConfig {
    fn default() -> Self {
        Self {
            slingshot_url: "https://slingshot.microcosm.blue".to_string(),
            constellation_url: "https://constellation.microcosm.blue".to_string(),
        }
    }
}

/// `[backfill]` (§5.10).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackfillConfig {
    pub concurrency: u32,
    pub per_pds_rate_per_sec: u32,
    pub full_listing_ceiling: u64,
    pub retry_every_hours: u32,
    pub reconcile_confirm_limit: u32,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            per_pds_rate_per_sec: 5,
            full_listing_ceiling: 1_000_000,
            retry_every_hours: 6,
            reconcile_confirm_limit: 1000,
        }
    }
}

/// A UTC time of day written `HH:MM` (`retention.run_at_utc`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct UtcTimeOfDay {
    hour: u8,
    minute: u8,
}

impl UtcTimeOfDay {
    /// Construct a time of day; `None` when out of range.
    #[must_use]
    pub fn new(hour: u8, minute: u8) -> Option<Self> {
        (hour < 24 && minute < 60).then_some(Self { hour, minute })
    }

    #[must_use]
    pub fn hour(&self) -> u8 {
        self.hour
    }

    #[must_use]
    pub fn minute(&self) -> u8 {
        self.minute
    }

    /// Seconds after 00:00 UTC.
    #[must_use]
    pub fn seconds_after_midnight(&self) -> u32 {
        u32::from(self.hour) * 3600 + u32::from(self.minute) * 60
    }
}

impl TryFrom<String> for UtcTimeOfDay {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let invalid = || format!("expected HH:MM (UTC), got {value:?}");
        let (h, m) = value.split_once(':').ok_or_else(invalid)?;
        if h.len() != 2 || m.len() != 2 {
            return Err(invalid());
        }
        let hour: u8 = h.parse().map_err(|_| invalid())?;
        let minute: u8 = m.parse().map_err(|_| invalid())?;
        Self::new(hour, minute).ok_or_else(invalid)
    }
}

impl fmt::Display for UtcTimeOfDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02}:{:02}", self.hour, self.minute)
    }
}

/// `[retention]` (§4.4).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionConfig {
    /// Age cap for unprotected S-class rows; 0 disables it.
    pub max_age_days: u32,
    pub tombstone_days: u32,
    pub notification_days: u32,
    pub run_at_utc: UtcTimeOfDay,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age_days: 0,
            tombstone_days: 7,
            notification_days: 90,
            run_at_utc: UtcTimeOfDay { hour: 4, minute: 0 },
        }
    }
}

/// `[storage]` (§4.6).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub budget_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            budget_bytes: 200 * 1024 * 1024 * 1024,
        }
    }
}

/// Upstream auth mode for a routing-table network (§8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkAuth {
    Unauth,
    ServiceJwt,
}

/// One `[[constellation.networks]]` entry (§8.1).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    pub name: String,
    /// Exact hosts or `*.suffix` patterns (§8.2).
    #[serde(default)]
    pub pds_patterns: Vec<String>,
    /// Upstream AppView base URL, or `local` for the community network.
    pub appview_url: String,
    pub auth: NetworkAuth,
    /// Audience DID for `service_jwt` mode.
    #[serde(default)]
    pub appview_did: String,
    /// Per-network token bucket (§8.3); absent for the local network.
    #[serde(default)]
    pub rate_per_sec: Option<u32>,
    #[serde(default)]
    pub burst: Option<u32>,
    pub enabled: bool,
}

/// `[constellation.cascade]` (§8.1).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CascadeConfig {
    pub order: Vec<String>,
}

impl Default for CascadeConfig {
    fn default() -> Self {
        Self {
            order: vec![
                "bluesky".to_string(),
                "community-a".to_string(),
                "community-b".to_string(),
            ],
        }
    }
}

/// `[constellation]`: the multi-AppView routing table (§8.1).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConstellationConfig {
    pub networks: Vec<NetworkConfig>,
    pub cascade: CascadeConfig,
}

/// The `appview_url` value marking the network served by this AppView.
pub const LOCAL_APPVIEW: &str = "local";

/// The network name for the community served by this AppView.
pub const COMMUNITY_NETWORK: &str = "community";

impl Default for ConstellationConfig {
    fn default() -> Self {
        let network = |name: &str,
                       patterns: &[&str],
                       url: &str,
                       auth: NetworkAuth,
                       rate: Option<(u32, u32)>,
                       enabled: bool| NetworkConfig {
            name: name.to_string(),
            pds_patterns: patterns.iter().map(|p| (*p).to_string()).collect(),
            appview_url: url.to_string(),
            auth,
            appview_did: String::new(),
            rate_per_sec: rate.map(|(r, _)| r),
            burst: rate.map(|(_, b)| b),
            enabled,
        };
        Self {
            networks: vec![
                network(
                    "bluesky",
                    &["*.host.bsky.network", "bsky.social"],
                    "https://public.api.bsky.app",
                    NetworkAuth::Unauth,
                    Some((20, 40)),
                    true,
                ),
                network(
                    "community-a",
                    &["community-a.example", "*.community-a.example"],
                    "https://api.community-a.example",
                    NetworkAuth::Unauth,
                    Some((10, 20)),
                    true,
                ),
                network(
                    "community-b",
                    &["community-b.example", "*.community-b.example"],
                    "https://api.community-b.example",
                    NetworkAuth::ServiceJwt,
                    Some((10, 20)),
                    false,
                ),
                network(
                    COMMUNITY_NETWORK,
                    &[],
                    LOCAL_APPVIEW,
                    NetworkAuth::Unauth,
                    None,
                    true,
                ),
            ],
            cascade: CascadeConfig::default(),
        }
    }
}

impl ConstellationConfig {
    /// The network named `community`, which validation guarantees exists.
    #[must_use]
    pub fn community(&self) -> Option<&NetworkConfig> {
        self.networks.iter().find(|n| n.name == COMMUNITY_NETWORK)
    }
}

/// Everything read from `app.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub http: HttpConfig,
    pub metrics: MetricsConfig,
    pub indexing: IndexingConfig,
    pub scope: ScopeConfig,
    pub community: CommunityConfig,
    pub firehose: FirehoseConfig,
    pub identity: IdentityConfig,
    pub kryphocron: KryphocronConfig,
    pub private_read: PrivateReadConfig,
    pub sync: SyncConfig,
    pub db: DbConfig,
    pub admin: AdminConfig,
    pub cache: CacheConfig,
    pub counts: CountsConfig,
    pub microcosm: MicrocosmConfig,
    pub backfill: BackfillConfig,
    pub retention: RetentionConfig,
    pub storage: StorageConfig,
    pub constellation: ConstellationConfig,
}
