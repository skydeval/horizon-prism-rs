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
    /// `Retry-After` on every `400 NotReady` (design-v3 §II-7.1): a constant,
    /// not an estimate of indexing progress.
    pub not_ready_retry_secs: u64,
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
            not_ready_retry_secs: 5,
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
    /// The shared bucket for request-origin PLC fetches (design-v3 §II-8.3).
    pub plc_request_rate_per_sec: Float,
    pub plc_request_burst: u32,
    /// One requester's identity share: a token per DID or handle resolved.
    pub requester_resolutions_per_sec: Float,
    pub requester_resolutions_burst: u32,
    /// How often one member's DID may be resolved from the issuer-refresh
    /// slice of PLC's member pool.
    pub member_pool_refresh_secs: u64,
    /// `resolveHandle`'s cache (design-v3 §5.11): how many answers it
    /// keeps, and for how long a DID and a "no such handle" each.
    pub resolve_handle_cache_max: u64,
    pub resolve_handle_ttl_secs: u64,
    pub resolve_handle_negative_ttl_secs: u64,
}

impl Default for IdentityConfig {
    fn default() -> Self {
        Self {
            plc_url: "https://plc.directory".to_string(),
            did_fresh_secs: 3600,
            did_stale_max_secs: 86_400,
            jwt_issuer_tracker_max: 20_000,
            plc_request_rate_per_sec: Float(20.0),
            plc_request_burst: 40,
            requester_resolutions_per_sec: Float(5.0),
            requester_resolutions_burst: 25,
            member_pool_refresh_secs: 600,
            resolve_handle_cache_max: 100_000,
            resolve_handle_ttl_secs: 300,
            resolve_handle_negative_ttl_secs: 60,
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
    pub get_timeline: u64,
}

impl Default for PrivateReadFloors {
    fn default() -> Self {
        Self {
            get_posts: 800,
            get_post_thread: 1000,
            get_author_feed: 1000,
            get_timeline: 1000,
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
    /// Each PDS host's bucket for listings made while serving public
    /// requests (design-v3 §II-6.11.3).
    pub public_fallback_rate_per_host: Float,
    /// How many hosts have such a bucket at once.
    pub max_public_fallback_hosts: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            head_max_age_secs: 60,
            max_head_fetches_per_request: 16,
            head_fetch_timeout_ms: 500,
            divergence_grace_secs: 300,
            max_probe_rate_per_host: 25,
            read_fetch_rate_per_host: 25,
            public_fallback_rate_per_host: Float(25.0),
            max_public_fallback_hosts: 100_000,
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
    /// The shared buckets for request-origin calls (design-v3 §II-8.3).
    pub slingshot_rate_per_sec: Float,
    pub slingshot_burst: u32,
    pub constellation_rate_per_sec: Float,
    pub constellation_burst: u32,
}

impl Default for MicrocosmConfig {
    fn default() -> Self {
        Self {
            slingshot_url: "https://slingshot.microcosm.blue".to_string(),
            constellation_url: "https://constellation.microcosm.blue".to_string(),
            slingshot_rate_per_sec: Float(50.0),
            slingshot_burst: 100,
            constellation_rate_per_sec: Float(50.0),
            constellation_burst: 100,
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
    /// One requester's fair share of a pool, in (0, 1] (design-v3 §II-8.3).
    pub requester_share: Float,
    /// The part of every shared bucket that non-members draw on, in (0, 1).
    pub nonmember_fraction: Float,
    /// Capacity of the per-requester share cache.
    pub requester_share_max_entries: u64,
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
            requester_share: Float(0.25),
            nonmember_fraction: Float(0.5),
            requester_share_max_entries: 200_000,
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

/// `[features]` (design-v3 §6.6). Every flag is `false` in v0.1, and a flag
/// may be `true` only for a surface this build implements.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeaturesConfig {
    pub feed_generators: bool,
    pub embed_external_view: bool,
    pub verification: bool,
    /// Removed: `[services.push].state` replaces it (§6.7.2.5). Still parsed
    /// so that validation can name the replacement key.
    pub push: Option<bool>,
    /// Removed: `[services.age_assurance].state` replaces it.
    pub age_assurance: Option<bool>,
    /// Removed: `[services.contact].state` replaces it.
    pub contact: Option<bool>,
}

impl FeaturesConfig {
    /// Whether the named feature flag is on. An unknown name is off.
    #[must_use]
    pub fn enabled(&self, feature: &str) -> bool {
        match feature {
            "feed_generators" => self.feed_generators,
            "embed_external_view" => self.embed_external_view,
            "verification" => self.verification,
            _ => false,
        }
    }
}

/// `[honest_501]` (design-v3 §6.1, §6.6).
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Honest501Config {
    /// The `<link>` of every 501 message. Empty drops the sentence that
    /// contains it.
    pub status_url: String,
    /// Per-NSID message overrides, merged over the compiled defaults.
    pub messages: BTreeMap<String, String>,
}

/// The state of a `[services.<name>]` block (design-v3 §6.7.2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceState {
    #[default]
    Disabled,
    Proxy,
    Serve,
}

impl ServiceState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Proxy => "proxy",
            Self::Serve => "serve",
        }
    }
}

/// One `[services.<name>]` block (design-v3 §6.7.2). Every key but `state`
/// and `methods` is accepted only with `state = "proxy"`; `crate::services`
/// validates the rest.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceConfig {
    pub state: ServiceState,
    pub proxy_url: Option<String>,
    pub aud_fragments: Option<Vec<String>>,
    pub timeout_ms: Option<u64>,
    pub body_read_timeout_ms: Option<u64>,
    pub max_request_bytes: Option<u64>,
    pub max_response_bytes: Option<u64>,
    pub max_in_flight: Option<u64>,
    pub rate_per_sec: Option<u32>,
    pub burst: Option<u32>,
    pub allow_private_network: Option<bool>,
    /// NSID to `"GET"` or `"POST"`: required on a custom service, rejected
    /// on a built-in one.
    pub methods: Option<BTreeMap<String, String>>,
}

/// A decimal config value. Compared bitwise, so the config stays `Eq` (a NaN
/// equals itself here; validation rejects one anyway).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(transparent)]
pub struct Float(pub f64);

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for Float {}

/// `[schema_backfill]`: the post-migration column backfill (design-v3 §12.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchemaBackfillConfig {
    /// Rows per batch.
    pub batch_size: u32,
    /// Pause between batches.
    pub pause_ms: u64,
}

impl Default for SchemaBackfillConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            pause_ms: 100,
        }
    }
}

/// `[schema_index]`: the post-migration index builds (design-v3 §12.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchemaIndexConfig {
    /// The first wait after a failed build; it doubles up to an hour.
    pub retry_backoff_secs: u64,
}

impl Default for SchemaIndexConfig {
    fn default() -> Self {
        Self { retry_backoff_secs: 300 }
    }
}

/// `[thread]`: bounds on one thread response (design-v3 §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThreadConfig {
    /// Nodes in one response: anchor, parents and replies.
    pub max_nodes: u32,
    /// Replies Constellation is asked for per node.
    pub constellation_replies_per_node: u32,
    /// Posts fetched from Slingshot for one response.
    pub max_remote_fetches: u32,
}

impl Default for ThreadConfig {
    fn default() -> Self {
        Self {
            max_nodes: 200,
            constellation_replies_per_node: 50,
            max_remote_fetches: 100,
        }
    }
}

/// `[thread_mutes]`: the cap on one member's muted threads (design-v3 §4.5).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThreadMutesConfig {
    /// Threads one member may have muted; `muteThread` past it is a 400.
    pub max_per_actor: u32,
}

impl Default for ThreadMutesConfig {
    fn default() -> Self {
        Self { max_per_actor: 10_000 }
    }
}

/// `[activity_subscriptions]`: the cap on one member's subscriptions
/// (design-v3 §4.5).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActivitySubscriptionsConfig {
    /// Accounts one member may subscribe to; a new one past it is a 400.
    pub max_per_actor: u32,
}

impl Default for ActivitySubscriptionsConfig {
    fn default() -> Self {
        Self { max_per_actor: 1000 }
    }
}

/// `bookmarks.emit_count` (design-v3 §8.2, locked decision 27): when a
/// post view carries `bookmarkCount`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookmarkCount {
    /// Never: safe at any size.
    #[default]
    Never,
    /// Only when the count is at least `bookmarks.count_floor`.
    AboveFloor,
    /// Always, zero included.
    Always,
}

/// `[bookmarks]` (design-v3 §5.8, §8.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BookmarksConfig {
    /// Posts one member may bookmark; another past it is a 400.
    pub max_per_actor: u32,
    pub emit_count: BookmarkCount,
    /// The smallest count `above_floor` shows.
    pub count_floor: u32,
}

impl Default for BookmarksConfig {
    fn default() -> Self {
        Self {
            max_per_actor: 50_000,
            emit_count: BookmarkCount::Never,
            count_floor: 10,
        }
    }
}

/// `[labelers]` (design-v3 §5.7).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LabelersConfig {
    /// Labelers one `getServices` request may name.
    pub get_services_max_dids: u32,
}

impl Default for LabelersConfig {
    fn default() -> Self {
        Self { get_services_max_dids: 50 }
    }
}

/// `[labels]` (design-v3 §5.12).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LabelsConfig {
    /// Patterns one `queryLabels` request may carry.
    pub query_max_patterns: u32,
}

impl Default for LabelsConfig {
    fn default() -> Self {
        Self { query_max_patterns: 25 }
    }
}

/// `[drafts]` (design-v3 §5.9).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DraftsConfig {
    /// Drafts one member may keep; another past it is `DraftLimitReached`.
    pub max_per_actor: u32,
    /// The largest stored draft, as serialized JSON.
    pub max_bytes: u32,
}

impl Default for DraftsConfig {
    fn default() -> Self {
        Self {
            max_per_actor: 1000,
            max_bytes: 1024 * 1024,
        }
    }
}

/// `[notifications]` (design-v3 §5.6).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationsConfig {
    /// How old a post may be and still be announced to its author's
    /// subscribers.
    pub subscribed_post_max_age_secs: u32,
}

impl Default for NotificationsConfig {
    fn default() -> Self {
        Self {
            subscribed_post_max_age_secs: 86_400,
        }
    }
}

/// `[farsight]`: the block and block-list index this AppView asks about
/// inbound list blocks (design-v3 §5.14.4). With `url` unset, no call is
/// ever made.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FarsightConfig {
    /// Base URL of the Farsight instance; `https` only.
    pub url: Option<String>,
    /// One `checkBlocks` call, and all one request may wait on Farsight.
    pub timeout_ms: u64,
    /// Lifetime of a cached pair answer.
    pub cache_ttl_secs: u64,
    pub cache_entries_per_viewer: u64,
    pub cache_entries_total: u64,
    /// Chunks of up to 100 accounts one request may ask about.
    pub max_calls_per_request: u32,
    pub rate_per_sec: Float,
    pub burst: u32,
    /// One requester's share of that bucket.
    pub requester_share: Float,
    /// Whether enrolling a member asks Farsight to read its repository.
    pub prime_on_enroll: bool,
    /// `[farsight.fallback]`: off by default.
    pub fallback: FarsightFallbackConfig,
    /// `[farsight.memory]`: off by default.
    pub memory: FarsightMemoryConfig,
}

/// `[farsight.memory]` (design-v3 §5.14.18; proposed locked decision 39):
/// remember, per ready member and for a bounded time, the inbound list
/// blocks a live answer reported, so they keep applying while Farsight
/// gives no answer and across a restart. Off by default. The numbers are
/// signed and, with the switch off, only parsed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FarsightMemoryConfig {
    pub enabled: bool,
    /// A remembered block applies until this long after it was last
    /// confirmed.
    pub retention_hours: i64,
    /// Blocks remembered for one member.
    pub max_per_viewer: i64,
    /// Blocks remembered in all.
    pub max_total: i64,
}

impl Default for FarsightMemoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            retention_hours: 24,
            max_per_viewer: 1000,
            max_total: 100_000,
        }
    }
}

/// `[farsight.fallback]` (design-v3 §5.14.17; proposed locked decision
/// 39): while Farsight is down, a ready member's ordinary render may ask
/// one AppView of the routing table who blocks the member through a list.
/// Off by default. The numbers are signed so that a negative value is out
/// of range rather than of the wrong type: with the switch off they are
/// only parsed, and a config that never enables the fallback always
/// starts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FarsightFallbackConfig {
    pub enabled: bool,
    /// The `name` of a `[[constellation.networks]]` entry; its
    /// `appview_url` is the AppView asked.
    pub network: String,
    /// One fallback call, and all one request may wait on the fallback.
    pub timeout_ms: i64,
    /// Calls of up to 30 accounts one request may make.
    pub max_calls_per_request: i64,
    /// Lifetime of a fallback result.
    pub cache_ttl_secs: i64,
    /// The fallback's own request bucket.
    pub rate_per_sec: Float,
    pub burst: i64,
    /// One requester's share of that bucket.
    pub requester_share: Float,
}

impl Default for FarsightFallbackConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            network: "bluesky".to_string(),
            timeout_ms: 1000,
            max_calls_per_request: 2,
            cache_ttl_secs: 300,
            rate_per_sec: Float(10.0),
            burst: 20,
            requester_share: Float(0.25),
        }
    }
}

impl Default for FarsightConfig {
    fn default() -> Self {
        Self {
            url: None,
            timeout_ms: 500,
            cache_ttl_secs: 30,
            cache_entries_per_viewer: 2000,
            cache_entries_total: 200_000,
            max_calls_per_request: 3,
            rate_per_sec: Float(50.0),
            burst: 100,
            requester_share: Float(0.25),
            prime_on_enroll: true,
            fallback: FarsightFallbackConfig::default(),
            memory: FarsightMemoryConfig::default(),
        }
    }
}

/// `[known_likers]` (design-v3 §8.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KnownLikersConfig {
    /// Constellation pages one thread view may read for `knownLikers`.
    pub scan_max_pages: u32,
}

impl Default for KnownLikersConfig {
    fn default() -> Self {
        Self { scan_max_pages: 5 }
    }
}

/// `[thread_v2]` (design-v3 §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThreadV2Config {
    /// Parents `getPostThreadV2` returns above the anchor.
    pub max_parents: u32,
}

impl Default for ThreadV2Config {
    fn default() -> Self {
        Self { max_parents: 80 }
    }
}

/// `[op_thread]` (design-v3 §8.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpThreadConfig {
    /// Stored posts one feed item's walk along its author's thread may
    /// cover. Below 2, feed items carry no thread position.
    pub max_walk: u32,
}

impl Default for OpThreadConfig {
    fn default() -> Self {
        Self { max_walk: 50 }
    }
}

/// `[suggestions]` (design-v3 §7.1 – §7.3, §13).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SuggestionsConfig {
    /// An account is suggested only if it posted within this many days.
    pub active_days: u32,
    /// How long a computed pool is served before it is computed again.
    pub refresh_secs: u64,
    /// Accounts (or packs) one pool holds.
    pub pool_size: u32,
    /// The member's most recent follows that friends-of-friends reads.
    pub fof_max_follows: u32,
    pub fof_cache_ttl_secs: u64,
    pub fof_cache_max: u64,
    /// Results the "see more" page skips.
    pub see_more_offset: u32,
    /// The actor's most recent in-scope followers that co-follow reads.
    pub cofollow_sample_followers: u32,
    pub cofollow_cache_ttl_secs: u64,
    pub cofollow_cache_max: u64,
    /// Below this many co-follow results, the popular order pads the list.
    pub cofollow_min: u32,
    /// Accounts `getTaggedSuggestions` names.
    pub tagged_count: u32,
}

impl Default for SuggestionsConfig {
    fn default() -> Self {
        Self {
            active_days: 14,
            refresh_secs: 600,
            pool_size: 1000,
            fof_max_follows: 500,
            fof_cache_ttl_secs: 600,
            fof_cache_max: 10_000,
            see_more_offset: 25,
            cofollow_sample_followers: 1000,
            cofollow_cache_ttl_secs: 600,
            cofollow_cache_max: 10_000,
            cofollow_min: 5,
            tagged_count: 10,
        }
    }
}

/// One entry of `unspecced.live_now`: an account whose live status may
/// link to these domains.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveNow {
    pub did: String,
    pub domains: Vec<String>,
}

/// `[unspecced]` (design-v3 §7.6): what `getConfig` answers.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UnspeccedConfig {
    /// An operator policy switch: the AppView holds no account's email.
    pub check_email_confirmed: bool,
    pub live_now: Vec<LiveNow>,
}

/// One entry of `trends.suggested_topics`, in the lexicon's shape. An
/// entry without `topic` and `link` does not parse.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuggestedTopic {
    pub topic: String,
    pub link: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
}

/// `[trends]` (design-v3 §7.4, §7.5, §13).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TrendsConfig {
    /// The window trending topics count posts over.
    pub topics_window_hours: u32,
    /// The window trends measure engagement over.
    pub window_hours: u32,
    /// Tags the hourly aggregate hands to the exact recount.
    pub candidate_count: u32,
    /// A tag needs this many posts in the window,
    pub min_posts: u32,
    /// by this many distinct authors.
    pub min_authors: u32,
    /// Engagement this old counts half.
    pub decay_half_life_hours: Float,
    /// Engagement counts only on a post made at most this long before it.
    pub subject_lookback_hours: u32,
    /// How long the hourly aggregate's rows are kept, and how old a post,
    /// like or repost may be and still be added to it.
    pub aggregate_retention_hours: u32,
    /// The longest a trend refresh may run; past it the previous list
    /// keeps being served.
    pub refresh_timeout_ms: u64,
    /// A trend started in the first hour at this many times its hourly
    /// mean.
    pub started_ratio: Float,
    /// A trend is "hot" when its last full hour is at this many times its
    /// hourly mean.
    pub hot_ratio: Float,
    /// How long a computed list is served.
    pub cache_ttl_secs: u64,
    /// Where a topic links to; `{tag}` is the tag, percent-encoded.
    pub topic_link_template: String,
    /// Descriptions the operator gives to tags.
    pub descriptions: BTreeMap<String, String>,
    /// Topics the operator suggests, served as `suggested`.
    pub suggested_topics: Vec<SuggestedTopic>,
}

impl Default for TrendsConfig {
    fn default() -> Self {
        Self {
            topics_window_hours: 6,
            window_hours: 24,
            candidate_count: 50,
            min_posts: 3,
            min_authors: 2,
            decay_half_life_hours: Float(6.0),
            subject_lookback_hours: 72,
            aggregate_retention_hours: 72,
            refresh_timeout_ms: 10_000,
            started_ratio: Float(2.0),
            hot_ratio: Float(2.0),
            cache_ttl_secs: 300,
            topic_link_template: "/hashtag/{tag}".to_string(),
            descriptions: BTreeMap::new(),
            suggested_topics: Vec::new(),
        }
    }
}

/// `[search]` (design-v3 §9.0, §13).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchConfig {
    /// The longest one search statement may run; past it the search is
    /// refused as too expensive.
    pub statement_timeout_ms: u64,
    /// `hitsTotal` is given only up to this many matches.
    pub hits_total_max: u32,
    /// The longest the count behind `hitsTotal` may run; past it the page
    /// is returned without the count.
    pub hits_total_timeout_ms: u64,
    /// How deep an offset cursor may go (`top`, starter packs).
    pub max_offset: u32,
    /// The default window of V1 `top`, in days.
    pub top_window_days: u32,
    /// The default window of V1 `latest` and of V2, in days.
    pub recent_window_days: u32,
    /// Values one array filter may carry.
    pub max_filter_values: u32,
    /// The newest matches a `top` search scores.
    pub top_candidates: u32,
    /// Rows each branch of a typeahead takes before ranking.
    pub typeahead_candidates: u32,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            statement_timeout_ms: 2000,
            hits_total_max: 10_000,
            hits_total_timeout_ms: 500,
            max_offset: 1000,
            top_window_days: 30,
            recent_window_days: 30,
            max_filter_values: 25,
            top_candidates: 2000,
            typeahead_candidates: 200,
        }
    }
}

/// `threadlens.access`: who may call Thread Lens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadLensAccess {
    /// Ready members only. Anyone else is refused `NotEnrolled` or
    /// `NotReady`.
    Members,
    /// Every caller whose token verifies. A caller that is not a ready
    /// member is served as a visitor: its verdicts come from Farsight and
    /// from the block rows this AppView happens to hold for it.
    #[default]
    SignedIn,
}

/// `[threadlens]` (design-v3 §5.15, §13): who may call Thread Lens, and the
/// bounds of one response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThreadLensConfig {
    /// Who may call. An anonymous caller is refused under either value.
    pub access: ThreadLensAccess,
    /// The most nodes one response adds beyond the ordinary thread view.
    /// The ordinary view itself is never cut and does not count.
    pub page_nodes: u32,
    /// Reply sequences a response starts to read, and Constellation pages
    /// it fetches, counted together.
    pub listing_calls: u32,
    /// The time limit of one verdict round's Farsight calls.
    pub verdict_round_timeout_ms: u32,
    /// Verdict rounds the walk up the parents may make in one request.
    pub parent_rounds: u32,
    /// Distinct authors looked up for bridging in one request, beyond the
    /// ordinary view's own.
    pub verdict_max_dids: u32,
}

impl Default for ThreadLensConfig {
    fn default() -> Self {
        Self {
            access: ThreadLensAccess::default(),
            page_nodes: 200,
            listing_calls: 60,
            verdict_round_timeout_ms: 500,
            parent_rounds: 8,
            verdict_max_dids: 600,
        }
    }
}

/// `[lists]` (design-v3 §5.5).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListsConfig {
    /// Owner-PDS pages one `getList` request reads when no AppView answers.
    pub read_listing_pages: u32,
    // Rule 4 (design-v3 §4.3): a member's budget, per source.
    /// Admitted references one member may hold of one source.
    pub subscribed_max_per_member: u32,
    /// Distinct list owners those references may name.
    pub max_owners_per_member: u32,
    /// Items kept of one subscribed list (its smallest record keys).
    pub subscribed_max_items: u32,
    /// Tombstones kept per list owner.
    pub subscription_tombstones_max_per_owner: u32,
    // `list_sync` pacing and lanes.
    /// The least time between two runs for one owner.
    pub sync_min_interval_secs: u32,
    pub sync_max_concurrent: u32,
    pub sync_slow_max_concurrent: u32,
    /// A run left `running` this long with nothing running it is re-queued.
    pub sync_stuck_secs: u32,
    // The bounds of one run's listings.
    pub subscribed_max_owner_listitems: u64,
    pub sync_max_pages: u32,
    pub sync_probe_call_secs: u32,
    pub sync_min_records_per_call_sec: u32,
    pub sync_max_call_secs: u32,
    pub sync_max_wall_secs: u32,
}

impl Default for ListsConfig {
    fn default() -> Self {
        Self {
            read_listing_pages: 5,
            subscribed_max_per_member: 25,
            max_owners_per_member: 10,
            subscribed_max_items: 50_000,
            subscription_tombstones_max_per_owner: 10_000,
            sync_min_interval_secs: 600,
            sync_max_concurrent: 1,
            sync_slow_max_concurrent: 1,
            sync_stuck_secs: 5400,
            subscribed_max_owner_listitems: 1_000_000,
            sync_max_pages: 20_000,
            sync_probe_call_secs: 60,
            sync_min_records_per_call_sec: 200,
            sync_max_call_secs: 900,
            sync_max_wall_secs: 3600,
        }
    }
}

/// `[known_followers]` (design-v3 §5.3).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KnownFollowersConfig {
    /// Constellation pages one `getKnownFollowers` scan may read.
    pub scan_max_pages: u32,
    /// Constellation pages a single-object view may read of an account's
    /// followers (a threadgate's `followingRule`, design-v3 §8.3).
    pub profile_scan_pages: u32,
}

impl Default for KnownFollowersConfig {
    fn default() -> Self {
        Self { scan_max_pages: 20, profile_scan_pages: 2 }
    }
}

/// `[starter_packs]` (design-v3 §5.5).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StarterPacksConfig {
    /// How often every stored pack's joined counts are refreshed.
    pub counts_refresh_secs: u64,
}

impl Default for StarterPacksConfig {
    fn default() -> Self {
        Self { counts_refresh_secs: 3600 }
    }
}

/// `[auth]`: limits on token verification done before the per-requester
/// backstop (design-v3 §II-8.3).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Failed verifications per IP key, all issuers together.
    pub failed_verify_rate_per_sec: Float,
    pub failed_verify_burst: u32,
    /// Failed verifications per (IP key, issuer).
    pub failed_verify_per_iss_rate_per_sec: Float,
    pub failed_verify_per_iss_burst: u32,
    /// A member that verified from an IP key within this window is exempt
    /// from both failure limits there.
    pub failed_verify_success_window_secs: u64,
    /// Capacity of the failure and ceiling state.
    pub failed_verify_state_max: u64,
    /// IP keys remembered per member as recent successes.
    pub failed_verify_success_ips_max: u32,
    /// The per-IP-key ceiling: failed verifications it may cost.
    pub verify_rate_per_sec: Float,
    pub verify_burst: u32,
    /// The per-member ceiling on successful verifications.
    pub member_verify_rate_per_sec: Float,
    pub member_verify_burst: u32,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            failed_verify_rate_per_sec: Float(10.0),
            failed_verify_burst: 20,
            failed_verify_per_iss_rate_per_sec: Float(1.0),
            failed_verify_per_iss_burst: 5,
            failed_verify_success_window_secs: 86_400,
            failed_verify_state_max: 100_000,
            failed_verify_success_ips_max: 16,
            verify_rate_per_sec: Float(500.0),
            verify_burst: 1000,
            member_verify_rate_per_sec: Float(200.0),
            member_verify_burst: 400,
        }
    }
}

/// One rate-limit class (design-v3 §10): `per_sec` tokens a second, `burst`
/// at most. A class table gives both keys.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateClassConfig {
    pub per_sec: f64,
    pub burst: u32,
}

impl RateClassConfig {
    #[must_use]
    pub const fn new(per_sec: f64, burst: u32) -> Self {
        Self { per_sec, burst }
    }
}

// Bitwise on the float, so the config stays `Eq` (a NaN equals itself here;
// validation rejects one anyway).
impl PartialEq for RateClassConfig {
    fn eq(&self, other: &Self) -> bool {
        self.per_sec.to_bits() == other.per_sec.to_bits() && self.burst == other.burst
    }
}

impl Eq for RateClassConfig {}

/// `[rate_limits]` (design-v3 §10): every number is operator config. The
/// defaults are the rates that used to be constants.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitsConfig {
    pub anon_read: RateClassConfig,
    pub auth_read: RateClassConfig,
    pub search_anon: RateClassConfig,
    pub search_auth: RateClassConfig,
    pub write: RateClassConfig,
    pub private: RateClassConfig,
    pub thread_lens: RateClassConfig,
    pub index_request: RateClassConfig,
    pub resolve: RateClassConfig,
    pub global: RateClassConfig,
    /// The two members of the `service_proxy` family (§6.7.6.2), each with
    /// one bucket per proxied service and key.
    pub service_proxy_member: RateClassConfig,
    pub service_proxy_ip: RateClassConfig,
    /// NSID to family name: moves one served endpoint to another family.
    pub endpoint_class: BTreeMap<String, String>,
}

impl Default for RateLimitsConfig {
    fn default() -> Self {
        Self {
            anon_read: RateClassConfig::new(30.0, 60),
            auth_read: RateClassConfig::new(60.0, 120),
            search_anon: RateClassConfig::new(10.0, 20),
            search_auth: RateClassConfig::new(30.0, 60),
            write: RateClassConfig::new(5.0, 20),
            private: RateClassConfig::new(10.0, 20),
            thread_lens: RateClassConfig::new(2.0, 10),
            index_request: RateClassConfig::new(0.5, 30),
            resolve: RateClassConfig::new(10.0, 20),
            global: RateClassConfig::new(100.0, 200),
            service_proxy_member: RateClassConfig::new(5.0, 20),
            service_proxy_ip: RateClassConfig::new(10.0, 20),
            endpoint_class: BTreeMap::new(),
        }
    }
}

impl RateLimitsConfig {
    /// Every class with its config key, in §10's order.
    #[must_use]
    pub fn classes(&self) -> [(&'static str, RateClassConfig); 12] {
        [
            ("anon_read", self.anon_read),
            ("auth_read", self.auth_read),
            ("search_anon", self.search_anon),
            ("search_auth", self.search_auth),
            ("write", self.write),
            ("private", self.private),
            ("thread_lens", self.thread_lens),
            ("index_request", self.index_request),
            ("resolve", self.resolve),
            ("global", self.global),
            ("service_proxy_member", self.service_proxy_member),
            ("service_proxy_ip", self.service_proxy_ip),
        ]
    }
}

/// Everything read from `app.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub rate_limits: RateLimitsConfig,
    pub auth: AuthConfig,
    pub thread: ThreadConfig,
    pub thread_v2: ThreadV2Config,
    pub op_thread: OpThreadConfig,
    pub threadlens: ThreadLensConfig,
    pub thread_mutes: ThreadMutesConfig,
    pub activity_subscriptions: ActivitySubscriptionsConfig,
    pub notifications: NotificationsConfig,
    pub bookmarks: BookmarksConfig,
    pub search: SearchConfig,
    pub suggestions: SuggestionsConfig,
    pub trends: TrendsConfig,
    pub unspecced: UnspeccedConfig,
    pub drafts: DraftsConfig,
    pub labels: LabelsConfig,
    pub labelers: LabelersConfig,
    pub starter_packs: StarterPacksConfig,
    pub known_followers: KnownFollowersConfig,
    pub known_likers: KnownLikersConfig,
    pub lists: ListsConfig,
    pub farsight: FarsightConfig,
    pub schema_backfill: SchemaBackfillConfig,
    pub schema_index: SchemaIndexConfig,
    pub features: FeaturesConfig,
    pub honest_501: Honest501Config,
    pub services: BTreeMap<String, ServiceConfig>,
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
