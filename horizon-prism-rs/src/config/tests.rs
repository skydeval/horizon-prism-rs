use std::collections::HashMap;

use super::*;

const SIGNING_HEX: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const CORRELATION_HEX: &str = "0202020202020202020202020202020202020202020202020202020202020202";
const PRIVATE_CURSOR_HEX: &str = "0303030303030303030303030303030303030303030303030303030303030303";

fn base_env() -> HashMap<&'static str, String> {
    HashMap::from([
        (ENV_PUBLIC_HOSTNAME, "appview.example.com".to_string()),
        (
            ENV_DATABASE_URL,
            "postgres://prism:secret@db:5432/prism".to_string(),
        ),
        (ENV_REDIS_URL, "redis://redis:6379".to_string()),
        (ENV_SERVICE_SIGNING_KEY_HEX, SIGNING_HEX.to_string()),
        (ENV_CORRELATION_KEY_HEX, CORRELATION_HEX.to_string()),
        (ENV_PRIVATE_CURSOR_KEY_HEX, PRIVATE_CURSOR_HEX.to_string()),
    ])
}

fn lookup_in<'a>(env: &'a HashMap<&'static str, String>) -> impl Fn(&str) -> Option<String> + 'a {
    move |k: &str| env.get(k).cloned()
}

fn invalid_problems(result: Result<AppConfig, ConfigError>) -> Vec<String> {
    match result {
        Err(ConfigError::Invalid(p)) => p,
        other => panic!("expected ConfigError::Invalid, got {other:?}"),
    }
}

fn load(toml_text: &str) -> Result<AppConfig, ConfigError> {
    let env = base_env();
    AppConfig::from_parts(toml_text, &lookup_in(&env))
}

fn has(problems: &[String], needle: &str) -> bool {
    problems.iter().any(|p| p.contains(needle))
}

// ---- deserialization ----

#[test]
fn empty_file_yields_design_defaults() {
    let cfg = load("").expect("defaults are valid");
    assert_eq!(cfg.file, FileConfig::default());
    assert_eq!(cfg.file.firehose.private_relays, ["bsky", "microcosm"]);
    assert_eq!(cfg.file.identity.jwt_issuer_tracker_max, 20_000);
    assert_eq!(cfg.file.retention.run_at_utc.to_string(), "04:00");
    assert_eq!(cfg.file.kryphocron.oracle_freshness_bound_secs, 120);
    assert_eq!(cfg.file.constellation.networks.len(), 4);
}

/// The committed `docker/config/app.toml`, reproduced here because the image
/// build copies only the crate directory. Keep the two in step.
const DOCKER_APP_TOML: &str = r#"
[http]
bind = "0.0.0.0:3030"
trusted_proxy = "172.30.0.10"

[metrics]
bind = "0.0.0.0:9091"

[net]
public_nameservers = []
internal_nameservers = []

[indexing]
# off | community_scoped | full (full is refused). Default: off.
# Set this, or run `admin indexing public-scope`, to change it (design §3.3).
# public_scope = "off"
fill_max_wait_hours = 24

[scope]
policy = "members_and_one_hop"
rebuild_grace_days = 7
max_follow_scope_per_member = 10000

[community]
auto_enroll = true
max_auto_enroll_per_hour = 50
max_members = 1000
never_ready_expiry_days = 7
enroll_refusal_cache_secs = 3600
enroll_pds_patterns = []
not_ready_retry_secs = 5

[firehose]
stream_key = "firehose:events"
group = "horizon-prism-rs"
consumer_name = "app"
batch_size = 500
dead_letter_stream = "firehose:dead-letters"
deterministic_attempts = 5
dead_letter_maxlen = 10000
private_relays = ["bsky", "microcosm"]
lag_warn_entries = 100000
coverage_window_secs = 600
coverage_capacity = 3000000

[firehose.relay_labels]
"wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos" = "bsky"
"wss://relay.fire.hose.cam/xrpc/com.atproto.sync.subscribeRepos" = "microcosm"
"wss://relay.community-a.example/xrpc/com.atproto.sync.subscribeRepos" = "community-a"

[identity]
plc_url = "https://plc.directory"
did_fresh_secs = 3600
did_stale_max_secs = 86400
jwt_issuer_tracker_max = 20000
plc_request_rate_per_sec = 20.0
plc_request_burst = 40
requester_resolutions_per_sec = 5.0
requester_resolutions_burst = 25
member_pool_refresh_secs = 600
resolve_handle_cache_max = 100000
resolve_handle_ttl_secs = 300
resolve_handle_negative_ttl_secs = 60

[kryphocron]
oracle_freshness_bound_secs = 120

[private_read]
bind_concurrency = 8
membership_retry_after_secs = 5
max_chain = 16
page_reads_factor = 2
walk_batch_max = 1000

[private_read.floor_ms]
get_posts = 0
get_post_thread = 0
get_author_feed = 0
get_timeline = 0
get_bookmarks = 0
get_likes = 0
get_reposted_by = 0
get_quotes = 0
list_notifications = 0
get_unread_count = 0
get_replies = 0
search_posts = 0

[private_notify]
backfill_max_age_hours = 48

[private_search]
scan_candidates = 300
span_hours = 168
max_age_days = 365

[sync]
head_max_age_secs = 60
max_head_fetches_per_request = 16
head_fetch_timeout_ms = 500
divergence_grace_secs = 300
max_probe_rate_per_host = 25
read_fetch_rate_per_host = 25
public_fallback_rate_per_host = 25.0
max_public_fallback_hosts = 100000
catch_up_rate_per_host = 10

[index_request]
catch_up_min_interval_secs = 10
catch_up_max_pages = 8

[db]
lock_timeout_secs = 30

[admin]
command_timeout_secs = 600
web_ui = true
session_idle_secs = 28800
session_max_secs = 604800

[cache]
read_time_bytes = 268435456
viewer_private_ttl_secs = 3600
viewer_private_max = 50000
viewer_private_per_viewer_max = 500

[counts]
public_source = "local_then_constellation"
fresh_secs = 300
stale_max_secs = 86400
sync_timeout_ms = 800
refresh_rate_per_sec = 20
refresh_queue_max = 100000

[microcosm]
slingshot_url = "https://slingshot.microcosm.blue"
constellation_url = "https://constellation.microcosm.blue"
slingshot_rate_per_sec = 50.0
slingshot_burst = 100
constellation_rate_per_sec = 50.0
constellation_burst = 100

[backfill]
concurrency = 4
per_pds_rate_per_sec = 5
full_listing_ceiling = 1000000
retry_every_hours = 6
reconcile_confirm_limit = 1000
follows_pass_ceiling = 50000

[retention]
max_age_days = 0
tombstone_days = 7
notification_days = 90
run_at_utc = "04:00"

[storage]
budget_bytes = 214748364800

[constellation]
requester_share = 0.25
nonmember_fraction = 0.5
requester_share_max_entries = 200000

[[constellation.networks]]
name = "bluesky"
pds_patterns = ["*.host.bsky.network", "bsky.social"]
appview_url = "https://public.api.bsky.app"
auth = "unauth"
rate_per_sec = 20
burst = 40
enabled = true

[[constellation.networks]]
name = "community-a"
pds_patterns = ["community-a.example", "*.community-a.example"]
appview_url = "https://api.community-a.example"
auth = "unauth"
rate_per_sec = 10
burst = 20
enabled = true

[[constellation.networks]]
name = "community-b"
pds_patterns = ["community-b.example", "*.community-b.example"]
appview_url = "https://api.community-b.example"
auth = "service_jwt"
appview_did = ""
rate_per_sec = 10
burst = 20
enabled = false

[[constellation.networks]]
name = "community"
pds_patterns = []
appview_url = "local"
auth = "unauth"
enabled = true

[constellation.cascade]
order = ["bluesky", "community-a", "community-b"]

[honest_501]
status_url = ""

[features]
feed_generators = false
embed_external_view = false
verification = false

[rate_limits]
anon_read = { per_sec = 30.0, burst = 60 }
auth_read = { per_sec = 60.0, burst = 120 }
search_anon = { per_sec = 10.0, burst = 20 }
search_auth = { per_sec = 30.0, burst = 60 }
write = { per_sec = 5.0, burst = 20 }
private = { per_sec = 10.0, burst = 20 }
thread_lens = { per_sec = 2.0, burst = 10 }
index_request = { per_sec = 0.5, burst = 30 }
resolve = { per_sec = 10.0, burst = 20 }
global = { per_sec = 100.0, burst = 200 }
service_proxy_member = { per_sec = 5.0, burst = 20 }
service_proxy_ip = { per_sec = 10.0, burst = 20 }

[auth]
failed_verify_rate_per_sec = 10.0
failed_verify_burst = 20
failed_verify_per_iss_rate_per_sec = 1.0
failed_verify_per_iss_burst = 5
failed_verify_success_window_secs = 86400
failed_verify_state_max = 100000
failed_verify_success_ips_max = 16
verify_rate_per_sec = 500.0
verify_burst = 1000
member_verify_rate_per_sec = 200.0
member_verify_burst = 400

[thread]
max_nodes = 200
constellation_replies_per_node = 50
max_remote_fetches = 100

[thread_mutes]
max_per_actor = 10000

[activity_subscriptions]
max_per_actor = 1000

[notifications]
subscribed_post_max_age_secs = 86400

[bookmarks]
max_per_actor = 50000
emit_count = "never"
count_floor = 10

[video]
playback = "bsky_cdn"
cdn_url = "https://video.bsky.app"

[drafts]
max_per_actor = 1000
max_bytes = 1048576

[labels]
query_max_patterns = 25

[labelers]
get_services_max_dids = 50

[starter_packs]
counts_refresh_secs = 3600

[known_followers]
scan_max_pages = 20
profile_scan_pages = 2

[known_likers]
scan_max_pages = 5

[thread_v2]
max_parents = 80

[op_thread]
max_walk = 50

[threadlens]
access = "signed_in"
page_nodes = 200
listing_calls = 60
verdict_round_timeout_ms = 500
parent_rounds = 8
verdict_max_dids = 600

[lists]
read_listing_pages = 5
subscribed_max_per_member = 25
max_owners_per_member = 10
subscribed_max_items = 50000
subscription_tombstones_max_per_owner = 10000
sync_min_interval_secs = 600
sync_max_concurrent = 1
sync_slow_max_concurrent = 1
sync_stuck_secs = 5400
subscribed_max_owner_listitems = 1000000
sync_max_pages = 20000
sync_probe_call_secs = 60
sync_min_records_per_call_sec = 200
sync_max_call_secs = 900
sync_max_wall_secs = 3600

[farsight]
timeout_ms = 500
cache_ttl_secs = 30
cache_entries_per_viewer = 2000
cache_entries_total = 200000
max_calls_per_request = 3
rate_per_sec = 50.0
burst = 100
requester_share = 0.25
prime_on_enroll = true

[schema_backfill]
batch_size = 10000
pause_ms = 100

[schema_index]
retry_backoff_secs = 300

[farsight.fallback]
enabled = false
network = "bluesky"
timeout_ms = 1000
max_calls_per_request = 2
cache_ttl_secs = 300
rate_per_sec = 10.0
burst = 20
requester_share = 0.25

[farsight.memory]
enabled = false
retention_hours = 24
max_per_viewer = 1000
max_total = 100000

[search]
statement_timeout_ms = 2000
hits_total_max = 10000
hits_total_timeout_ms = 500
max_offset = 1000
recent_window_days = 30
top_window_days = 30
max_filter_values = 25
top_candidates = 2000
typeahead_candidates = 200

[suggestions]
active_days = 14
refresh_secs = 600
pool_size = 1000
fof_max_follows = 500
fof_cache_ttl_secs = 600
fof_cache_max = 10000
see_more_offset = 25
cofollow_sample_followers = 1000
cofollow_cache_ttl_secs = 600
cofollow_cache_max = 10000
cofollow_min = 5
tagged_count = 10

[trends]
topics_window_hours = 6
window_hours = 24
candidate_count = 50
min_posts = 3
min_authors = 2
decay_half_life_hours = 6.0
subject_lookback_hours = 72
aggregate_retention_hours = 72
refresh_timeout_ms = 10000
started_ratio = 2.0
hot_ratio = 2.0
cache_ttl_secs = 300
topic_link_template = "/hashtag/{tag}"

[unspecced]
check_email_confirmed = false
live_now = []
"#;

#[test]
fn docker_app_toml_equals_defaults() {
    let cfg = load(DOCKER_APP_TOML).expect("shipped config is valid");
    assert_eq!(cfg.file, FileConfig::default());
}

#[test]
fn partial_section_keeps_other_defaults() {
    let cfg = load("[firehose]\nbatch_size = 50\n").expect("valid");
    assert_eq!(cfg.file.firehose.batch_size, 50);
    assert_eq!(cfg.file.firehose.group, "horizon-prism-rs");
}

#[test]
fn enums_parse_snake_case() {
    let cfg = load(
        "[scope]\npolicy = \"members_only\"\n[counts]\npublic_source = \"local\"\n",
    )
    .expect("valid");
    assert_eq!(cfg.file.scope.policy, ScopePolicy::MembersOnly);
    assert_eq!(cfg.file.counts.public_source, CountsSource::Local);
}

// ---- unknown keys ----

#[test]
fn unknown_top_level_section_is_rejected() {
    assert!(matches!(
        load("[firehouse]\nbatch_size = 1\n"),
        Err(ConfigError::Parse(_))
    ));
}

#[test]
fn unknown_nested_key_is_rejected() {
    let err = load("[firehose]\nbatch_sise = 1\n").unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
    assert!(err.to_string().contains("batch_sise"));
}

#[test]
fn unknown_key_in_network_entry_is_rejected() {
    let toml_text = "[[constellation.networks]]\nname = \"community\"\nappview_url = \"local\"\nauth = \"unauth\"\nenabled = true\nratelimit = 3\n";
    assert!(matches!(load(toml_text), Err(ConfigError::Parse(_))));
}

#[test]
fn unknown_key_in_floor_table_is_rejected() {
    assert!(matches!(
        load("[private_read.floor_ms]\nget_post = 1\n"),
        Err(ConfigError::Parse(_))
    ));
}

#[test]
fn bad_enum_value_is_rejected() {
    assert!(matches!(
        load("[indexing]\npublic_scope = \"everything\"\n"),
        Err(ConfigError::Parse(_))
    ));
}

#[test]
fn bad_run_at_utc_is_rejected() {
    for bad in ["4:00", "24:00", "04:60", "0400", "04:00:00"] {
        let text = format!("[retention]\nrun_at_utc = \"{bad}\"\n");
        assert!(matches!(load(&text), Err(ConfigError::Parse(_))), "{bad}");
    }
}

// ---- environment overlay ----

#[test]
fn env_overlay_reads_every_variable() {
    let mut env = base_env();
    env.insert(ENV_DATA_DIR, "/srv/data".to_string());
    env.insert(ENV_LOG_LEVEL, "debug".to_string());
    env.insert(ENV_ALLOW_STARTUP_MIGRATE, "1".to_string());
    let cfg = AppConfig::from_parts("", &lookup_in(&env)).expect("valid");
    assert_eq!(cfg.env.public_hostname, "appview.example.com");
    assert_eq!(cfg.service_did(), "did:web:appview.example.com");
    assert_eq!(cfg.env.redis_url, "redis://redis:6379");
    assert_eq!(cfg.env.data_dir, std::path::PathBuf::from("/srv/data"));
    assert_eq!(cfg.env.log_level, "debug");
    assert!(cfg.env.allow_startup_migrate);
    assert_eq!(cfg.env.service_signing_key.expose(), &[1u8; 32]);
    assert_eq!(cfg.env.correlation_key.expose(), &[2u8; 32]);
    assert_eq!(
        cfg.env.database_url.expose(),
        "postgres://prism:secret@db:5432/prism"
    );
}

#[test]
fn env_overlay_defaults() {
    let cfg = load("").expect("valid");
    assert_eq!(cfg.env.data_dir, std::path::PathBuf::from(DEFAULT_DATA_DIR));
    assert_eq!(cfg.env.log_level, DEFAULT_LOG_LEVEL);
    assert!(!cfg.env.allow_startup_migrate);
    // The private cursor key is read, and it is not Thread Lens's.
    assert_eq!(cfg.env.private_cursor_key.expose(), &[3u8; 32]);
    assert!(cfg.env.threadlens_cursor_key.is_none());
}

#[test]
fn env_overlay_does_not_read_toml_keys() {
    // Secrets are env-only: a TOML key with the same meaning is unknown.
    assert!(matches!(
        load("database_url = \"postgres://x\"\n"),
        Err(ConfigError::Parse(_))
    ));
}

#[test]
fn missing_required_env_is_reported() {
    for var in [
        ENV_PUBLIC_HOSTNAME,
        ENV_DATABASE_URL,
        ENV_REDIS_URL,
        ENV_SERVICE_SIGNING_KEY_HEX,
        ENV_CORRELATION_KEY_HEX,
        ENV_PRIVATE_CURSOR_KEY_HEX,
    ] {
        let mut env = base_env();
        env.remove(var);
        match AppConfig::from_parts("", &lookup_in(&env)) {
            Err(ConfigError::MissingEnv(v)) => assert_eq!(v, var),
            other => panic!("{var}: expected MissingEnv, got {other:?}"),
        }
    }
}

#[test]
fn malformed_env_is_reported() {
    let cases = [
        (ENV_SERVICE_SIGNING_KEY_HEX, "abcd"),
        (
            ENV_CORRELATION_KEY_HEX,
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ),
        (ENV_PRIVATE_CURSOR_KEY_HEX, "abcd"),
        (
            ENV_PRIVATE_CURSOR_KEY_HEX,
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ),
        (ENV_PUBLIC_HOSTNAME, "https://appview.example.com"),
        (ENV_PUBLIC_HOSTNAME, "AppView.example.com"),
        (ENV_DATABASE_URL, "mysql://db"),
        (ENV_REDIS_URL, "http://redis"),
        (ENV_ALLOW_STARTUP_MIGRATE, "true"),
    ];
    for (var, value) in cases {
        let mut env = base_env();
        env.insert(var, value.to_string());
        match AppConfig::from_parts("", &lookup_in(&env)) {
            Err(ConfigError::InvalidEnv { var: v, .. }) => assert_eq!(v, var),
            other => panic!("{var}={value}: expected InvalidEnv, got {other:?}"),
        }
    }
}

#[test]
fn allow_startup_migrate_zero_is_closed() {
    let mut env = base_env();
    env.insert(ENV_ALLOW_STARTUP_MIGRATE, "0".to_string());
    let cfg = AppConfig::from_parts("", &lookup_in(&env)).expect("valid");
    assert!(!cfg.env.allow_startup_migrate);
}

#[test]
fn secrets_are_redacted_in_debug() {
    let cfg = load("").expect("valid");
    let debug = format!("{cfg:?}");
    assert!(!debug.contains(SIGNING_HEX));
    assert!(!debug.contains(PRIVATE_CURSOR_HEX));
    assert!(!debug.contains("secret@db"));
    assert!(debug.contains("<redacted>"));
}

#[test]
fn config_path_defaults_and_overrides() {
    let empty: HashMap<&'static str, String> = HashMap::new();
    assert_eq!(
        config_path(&lookup_in(&empty)),
        std::path::PathBuf::from(DEFAULT_CONFIG_PATH)
    );
    let env = HashMap::from([(ENV_CONFIG_PATH, "/tmp/app.toml".to_string())]);
    assert_eq!(
        config_path(&lookup_in(&env)),
        std::path::PathBuf::from("/tmp/app.toml")
    );
}

#[test]
fn load_with_reports_missing_file() {
    let env = base_env();
    let result = AppConfig::load_with(
        std::path::Path::new("/nonexistent/horizon-prism-rs/app.toml"),
        &lookup_in(&env),
    );
    assert!(matches!(result, Err(ConfigError::Io { .. })));
}

// ---- validation rules ----

/// design-v3 §6.6, §6.7.2.3: `disabled` and `proxy` load; `serve` does not,
/// and push stays `disabled`.
#[test]
fn services_and_features_are_validated_at_load() {
    let cfg = load("[services.contact]\nstate = \"proxy\"\nproxy_url = \"https://c.example\"\n").expect("loads");
    assert_eq!(cfg.file.services["contact"].state, crate::config::ServiceState::Proxy);
    // B-28's last sentence.
    for text in [
        "[services.push]\nstate = \"serve\"\n",
        "[services.push]\nstate = \"proxy\"\nproxy_url = \"https://p.example\"\n",
    ] {
        assert!(has(&invalid_problems(load(text)), "services.push.state"), "{text}");
    }
    let p = invalid_problems(load("[features]\npush = true\n"));
    assert!(has(&p, "features.push is removed: set services.push.state"), "{p:?}");
    // Rule 4 reads APP_PUBLIC_HOSTNAME.
    let p = invalid_problems(load(
        "[services.contact]\nstate = \"proxy\"\nproxy_url = \"https://appview.example.com\"\n",
    ));
    assert!(has(&p, "own hostname"), "{p:?}");
    // An unknown state is a parse error, like any bad enum value.
    assert!(matches!(
        load("[services.contact]\nstate = \"on\"\n"),
        Err(ConfigError::Parse(_))
    ));
    // Disabled blocks, built-in or custom, load.
    let cfg = load(concat!(
        "[services.contact]\nstate = \"disabled\"\n",
        "[services.eu]\nstate = \"disabled\"\nmethods = { \"eu.example.region.getNotices\" = \"GET\" }\n",
    ))
    .expect("loads");
    assert_eq!(cfg.file.services.len(), 2);
}

#[test]
fn full_public_scope_is_refused() {
    let p = invalid_problems(load("[indexing]\npublic_scope = \"full\"\n"));
    assert!(has(&p, "public_scope=full is not implemented in v1"));
}

/// T-1 (public-indexing design §3.1, §3.3): `off` parses; an absent key is
/// `None`, which is effectively `off`; the fill bound defaults to 24 hours.
#[test]
fn public_scope_off_and_an_absent_key() {
    let cfg = load("[indexing]\npublic_scope = \"off\"\n").expect("valid");
    assert_eq!(cfg.file.indexing.public_scope, Some(PublicScope::Off));
    let cfg = load("[indexing]\npublic_scope = \"community_scoped\"\n").expect("valid");
    assert_eq!(cfg.file.indexing.public_scope(), PublicScope::CommunityScoped);
    let cfg = load("").expect("valid");
    assert_eq!(cfg.file.indexing.public_scope, None);
    assert_eq!(cfg.file.indexing.public_scope(), PublicScope::Off);
    assert_eq!(cfg.file.indexing.fill_max_wait_hours, 24);
    let cfg = load("[indexing]\nfill_max_wait_hours = 2\n").expect("valid");
    assert_eq!(cfg.file.indexing.fill_max_wait_hours, 2);
    assert_eq!(cfg.file.honest_501.public_off_message, None);
    let cfg = load("[honest_501]\npublic_off_message = \"off\"\npublic_filling_message = \"soon\"\n").expect("valid");
    assert_eq!(cfg.file.honest_501.public_off_message.as_deref(), Some("off"));
    assert_eq!(cfg.file.honest_501.public_filling_message.as_deref(), Some("soon"));
}

/// T-14 (template): the shipped copy writes no `public_scope` line, so an
/// upgrade never applies one from it.
#[test]
fn the_template_carries_no_explicit_public_scope() {
    assert!(!DOCKER_APP_TOML.lines().any(|l| l.trim_start().starts_with("public_scope") && l.contains('=')));
    assert!(DOCKER_APP_TOML.contains("# public_scope = \"off\""));
}

#[test]
fn private_relay_must_be_a_relay_label() {
    let p = invalid_problems(load("[firehose]\nprivate_relays = [\"bsky\", \"hideout\"]\n"));
    assert!(has(&p, "\"hideout\" is not a value in firehose.relay_labels"));
}

#[test]
fn private_relays_must_not_be_empty() {
    let p = invalid_problems(load("[firehose]\nprivate_relays = []\n"));
    assert!(has(&p, "private_relays must name at least one"));
}

#[test]
fn relay_labels_must_be_distinct_and_not_other() {
    let text = "[firehose.relay_labels]\n\"wss://a.example/x\" = \"bsky\"\n\"wss://b.example/x\" = \"bsky\"\n\"wss://c.example/x\" = \"other\"\n";
    // relay_labels replaces the default map entirely; bsky stays a valid
    // private relay, microcosm does not.
    let p = invalid_problems(load(text));
    assert!(has(&p, "distinct label"));
    assert!(has(&p, "reserved for unlisted relays"));
    assert!(has(&p, "\"microcosm\" is not a value"));
}

fn with_networks(networks: &str, cascade: &str) -> String {
    format!("{networks}\n[constellation.cascade]\norder = {cascade}\n")
}

const COMMUNITY: &str = "[[constellation.networks]]\nname = \"community\"\npds_patterns = [\"pds.example.org\"]\nappview_url = \"local\"\nauth = \"unauth\"\nenabled = true\n";

#[test]
fn enabled_service_jwt_requires_appview_did() {
    let text = with_networks(
        &format!(
            "{COMMUNITY}[[constellation.networks]]\nname = \"community-b\"\nappview_url = \"https://api.community-b.example\"\nauth = \"service_jwt\"\nappview_did = \"\"\nrate_per_sec = 10\nburst = 20\nenabled = true\n"
        ),
        "[\"community-b\"]",
    );
    let p = invalid_problems(load(&text));
    assert!(has(&p, "requires appview_did"));
}

#[test]
fn enabled_service_jwt_with_did_is_accepted() {
    let text = with_networks(
        &format!(
            "{COMMUNITY}[[constellation.networks]]\nname = \"community-b\"\nappview_url = \"https://api.community-b.example\"\nauth = \"service_jwt\"\nappview_did = \"did:web:api.community-b.example\"\nrate_per_sec = 10\nburst = 20\nenabled = true\n"
        ),
        "[\"community-b\"]",
    );
    load(&text).expect("valid");
}

#[test]
fn appview_did_must_be_a_did() {
    let text = with_networks(
        &format!(
            "{COMMUNITY}[[constellation.networks]]\nname = \"x\"\nappview_url = \"https://x.example\"\nauth = \"service_jwt\"\nappview_did = \"api.x.example\"\nrate_per_sec = 1\nburst = 1\nenabled = true\n"
        ),
        "[\"x\"]",
    );
    assert!(has(&invalid_problems(load(&text)), "is not a DID"));
}

#[test]
fn community_network_is_required_and_local() {
    let text = with_networks(
        "[[constellation.networks]]\nname = \"bluesky\"\nappview_url = \"https://public.api.bsky.app\"\nauth = \"unauth\"\nrate_per_sec = 1\nburst = 1\nenabled = true\n",
        "[\"bluesky\"]",
    );
    assert!(has(&invalid_problems(load(&text)), "named \"community\" is required"));

    let text = with_networks(
        "[[constellation.networks]]\nname = \"community\"\nappview_url = \"https://elsewhere.example\"\nauth = \"unauth\"\nenabled = true\n",
        "[]",
    );
    assert!(has(&invalid_problems(load(&text)), "must use appview_url = \"local\""));
}

#[test]
fn only_community_may_be_local() {
    let text = with_networks(
        &format!(
            "{COMMUNITY}[[constellation.networks]]\nname = \"other\"\nappview_url = \"local\"\nauth = \"unauth\"\nenabled = false\n"
        ),
        "[]",
    );
    assert!(has(&invalid_problems(load(&text)), "only \"community\" may use"));
}

#[test]
fn duplicate_network_names_are_rejected() {
    let text = with_networks(&format!("{COMMUNITY}{COMMUNITY}"), "[]");
    assert!(has(&invalid_problems(load(&text)), "duplicate network"));
}

#[test]
fn cascade_names_must_exist_once_and_not_be_community() {
    let text = with_networks(COMMUNITY, "[\"bluesky\", \"community\"]");
    let p = invalid_problems(load(&text));
    assert!(has(&p, "\"bluesky\" is not a configured network"));
    assert!(has(&p, "cannot be a cascade step"));

    let text = with_networks(
        &format!(
            "{COMMUNITY}[[constellation.networks]]\nname = \"b\"\nappview_url = \"https://b.example\"\nauth = \"unauth\"\nrate_per_sec = 1\nburst = 1\nenabled = true\n"
        ),
        "[\"b\", \"b\"]",
    );
    assert!(has(&invalid_problems(load(&text)), "listed twice"));
}

#[test]
fn enabled_upstream_needs_a_rate() {
    let text = with_networks(
        &format!(
            "{COMMUNITY}[[constellation.networks]]\nname = \"b\"\nappview_url = \"https://b.example\"\nauth = \"unauth\"\nenabled = true\n"
        ),
        "[\"b\"]",
    );
    assert!(has(&invalid_problems(load(&text)), "needs rate_per_sec and burst"));
}

#[test]
fn upstream_appview_must_be_https() {
    let text = with_networks(
        &format!(
            "{COMMUNITY}[[constellation.networks]]\nname = \"b\"\nappview_url = \"http://b.example\"\nauth = \"unauth\"\nrate_per_sec = 1\nburst = 1\nenabled = true\n"
        ),
        "[\"b\"]",
    );
    assert!(has(&invalid_problems(load(&text)), "must be an https:// URL"));
}

#[test]
fn pds_patterns_are_checked() {
    for bad in ["https://pds.example", "*.", "pds.example:443", "PDS.example", "*.*.example", "localhost"] {
        assert!(pds_pattern(bad).is_err(), "{bad}");
        let text = format!("[community]\nenroll_pds_patterns = [\"{bad}\"]\n");
        assert!(has(&invalid_problems(load(&text)), "enroll_pds_patterns"), "{bad}");
    }
    for good in ["pds.example.org", "*.host.bsky.network", "bsky.social"] {
        assert!(pds_pattern(good).is_ok(), "{good}");
    }
}

/// Admin UI sessions (admin-ui design §5.4, §13): on by default, eight
/// hours idle and seven days at most; idle may not exceed the maximum.
#[test]
fn admin_ui_sessions_are_checked() {
    let cfg = load("").expect("defaults");
    assert!(cfg.file.admin.web_ui);
    assert_eq!((cfg.file.admin.session_idle_secs, cfg.file.admin.session_max_secs), (28_800, 604_800));
    let p = invalid_problems(load("[admin]\nsession_idle_secs = 700000\n"));
    assert!(has(&p, "admin.session_idle_secs must not exceed"));
    let p = invalid_problems(load("[admin]\nsession_idle_secs = 0\n"));
    assert!(has(&p, "admin.session_idle_secs must be greater than 0"));
    assert!(!load("[admin]\nweb_ui = false\n").expect("valid").file.admin.web_ui);
}

#[test]
fn microcosm_urls_must_be_https() {
    let p = invalid_problems(load("[microcosm]\nslingshot_url = \"http://slingshot.example\"\n"));
    assert!(has(&p, "microcosm.slingshot_url"));
}

/// Private posting, Addendum A: `[video]` takes the two playback values and
/// an `https` CDN base.
#[test]
fn video_playback_and_cdn_url_are_checked() {
    assert_eq!(load("").expect("defaults").file.video.playback, VideoPlayback::BskyCdn);
    let cfg = load("[video]\nplayback = \"pds_blob\"\n").expect("valid");
    assert_eq!(cfg.file.video.playback, VideoPlayback::PdsBlob);
    assert!(matches!(load("[video]\nplayback = \"hls\"\n"), Err(ConfigError::Parse(_))));
    let p = invalid_problems(load("[video]\ncdn_url = \"http://video.example\"\n"));
    assert!(has(&p, "video.cdn_url"));
}

#[test]
fn plc_url_may_be_plain_http_for_a_test_plc() {
    let cfg = load("[identity]\nplc_url = \"http://plc.test.internal:2582\"\n").expect("valid");
    assert_eq!(cfg.file.identity.plc_url, "http://plc.test.internal:2582");
    let p = invalid_problems(load("[identity]\nplc_url = \"ftp://plc.example\"\n"));
    assert!(has(&p, "identity.plc_url"));
}

#[test]
fn zero_values_that_break_runtime_are_rejected() {
    for (section, key) in [
        ("firehose", "batch_size"),
        ("firehose", "deterministic_attempts"),
        ("private_read", "bind_concurrency"),
        ("private_read", "membership_retry_after_secs"),
        ("backfill", "concurrency"),
        ("backfill", "follows_pass_ceiling"),
        ("sync", "read_fetch_rate_per_host"),
        ("sync", "catch_up_rate_per_host"),
        ("index_request", "catch_up_min_interval_secs"),
        ("index_request", "catch_up_max_pages"),
        ("private_search", "scan_candidates"),
        ("private_search", "span_hours"),
        ("private_search", "max_age_days"),
        ("storage", "budget_bytes"),
        ("identity", "jwt_issuer_tracker_max"),
    ] {
        let text = format!("[{section}]\n{key} = 0\n");
        let p = invalid_problems(load(&text));
        assert!(has(&p, &format!("{section}.{key} must be greater than 0")), "{section}.{key}");
    }
}

/// design-v3 §13: the Thread Lens bounds have floors, and the cursor key
/// is optional.
#[test]
fn threadlens_bounds_have_floors() {
    let p = invalid_problems(load("[threadlens]\npage_nodes = 1\nlisting_calls = 1\nparent_rounds = 0\n"));
    assert!(has(&p, "threadlens.page_nodes must be at least 2"), "{p:?}");
    assert!(has(&p, "threadlens.listing_calls must be at least 2") && has(&p, "threadlens.parent_rounds must be at least 1"), "{p:?}");
    let cfg = load("[threadlens]\npage_nodes = 2\nlisting_calls = 2\nparent_rounds = 1\n").expect("the floors themselves are valid");
    assert_eq!((cfg.file.threadlens.verdict_max_dids, cfg.file.threadlens.verdict_round_timeout_ms), (600, 500));
    assert!(cfg.env.threadlens_cursor_key.is_none());
}

/// `threadlens.access`: every signed-in account by default, members only
/// on request, and any other value does not parse.
#[test]
fn threadlens_access_is_members_or_signed_in() {
    assert_eq!(load("").expect("defaults").file.threadlens.access, ThreadLensAccess::SignedIn);
    let cfg = load("[threadlens]\naccess = \"members\"\n").expect("members");
    assert_eq!(cfg.file.threadlens.access, ThreadLensAccess::Members);
    let cfg = load("[threadlens]\naccess = \"signed_in\"\n").expect("signed_in");
    assert_eq!(cfg.file.threadlens.access, ThreadLensAccess::SignedIn);
    assert!(!matches!(load("[threadlens]\naccess = \"everyone\"\n"), Ok(_) | Err(ConfigError::Invalid(_))), "an unknown value is a parse error");
}

/// design-v3 §4.3 "Budgets": a config that still carries a key the
/// admission model removed is told what replaced it.
#[test]
fn removed_list_keys_point_at_what_replaced_them() {
    let p = invalid_problems(load("[lists]\nsubscribed_max_total = 5000\n"));
    assert!(has(&p, "subscribed_max_total was removed") && has(&p, "locked decision 36"), "{p:?}");
    let p = invalid_problems(load("[retention]\nsubscribed_list_grace_days = 7\n"));
    assert!(has(&p, "subscribed_list_grace_days was removed"), "{p:?}");
    // Any other unknown key is still the parser's own error.
    assert!(matches!(load("[lists]\nno_such_key = 1\n"), Err(ConfigError::Parse(_))));
}

/// design-v3 §4.3, §13: the list-sync lanes leave two backfill workers
/// free, and the wall bound cannot cut what the other bounds allow.
#[test]
fn list_sync_bounds_are_checked_against_each_other() {
    let p = invalid_problems(load("[lists]\nsync_max_concurrent = 2\n"));
    assert!(has(&p, "must be at most backfill.concurrency - 2"), "{p:?}");
    let p = invalid_problems(load("[backfill]\nconcurrency = 3\n"));
    assert!(has(&p, "must be at most backfill.concurrency - 2"), "{p:?}");
    // 900 + 1 000 000 / 100 / 5 = 2900 at the defaults.
    let p = invalid_problems(load("[lists]\nsync_max_wall_secs = 2899\n"));
    assert!(has(&p, "lists.sync_max_wall_secs must be at least 2900"), "{p:?}");
    assert!(load("[lists]\nsync_max_wall_secs = 2900\n").is_ok());
    let p = invalid_problems(load("[lists]\nsync_probe_call_secs = 901\n"));
    assert!(has(&p, "sync_probe_call_secs must be at most"), "{p:?}");
    let p = invalid_problems(load("[lists]\nsubscribed_max_per_member = 0\nmax_owners_per_member = 0\n"));
    assert!(has(&p, "lists.subscribed_max_per_member must be at least 1") && has(&p, "lists.max_owners_per_member must be at least 1"));
    // The removed aggregate cap is an unknown key.
    assert!(load("[lists]\nsubscribed_max_total = 5\n").is_err());
}

/// Private posting §8: a search span no longer than the search's reach.
#[test]
fn private_search_span_fits_its_reach() {
    let p = invalid_problems(load("[private_search]\nspan_hours = 25\nmax_age_days = 1\n"));
    assert!(has(&p, "private_search.span_hours must be at most 24 × private_search.max_age_days"), "{p:?}");
    let cfg = load("[private_search]\nspan_hours = 24\nmax_age_days = 1\n").expect("one day of one day");
    assert_eq!(cfg.file.private_search.scan_candidates, 300);
    // Floors are 0 by default (timing-floors design §3.3).
    assert_eq!(cfg.file.private_read.floor_ms.search_posts, 0);
}

/// T-TF1 (timing-floors design §6): every floor defaults to 0, 0 is
/// accepted, and a floor is at most 10 000 ms.
#[test]
fn floors_default_to_zero_and_are_at_most_ten_seconds() {
    let cfg = load("").expect("defaults");
    assert!(cfg.file.private_read.floor_ms.entries().iter().all(|(_, ms)| *ms == 0));
    let zeros = "[private_read.floor_ms]\nget_posts = 0\nget_post_thread = 0\nget_author_feed = 0\nget_timeline = 0\nget_bookmarks = 0\nget_likes = 0\nget_reposted_by = 0\nget_quotes = 0\nlist_notifications = 0\nget_unread_count = 0\nget_replies = 0\nsearch_posts = 0\n";
    let cfg = load(zeros).expect("every floor 0 loads");
    assert_eq!(cfg.file.private_read.floor_ms, crate::config::PrivateReadFloors::default());
    let p = invalid_problems(load("[private_read.floor_ms]\nget_posts = 10001\n"));
    assert!(has(&p, "private_read.floor_ms.get_posts must be at most 10000"), "{p:?}");
    let cfg = load("[private_read.floor_ms]\nget_posts = 10000\n").expect("10 000 ms loads");
    assert_eq!(cfg.file.private_read.floor_ms.get_posts, 10_000);
}

/// T-TF4 (timing-floors design §6): the startup log's level is decided by
/// a pure function of the floors: all zero is off, all non-zero is on, a
/// mix is partial with the zero keys listed.
#[test]
fn floors_setting_is_off_on_or_partial() {
    use crate::config::{FloorsSetting, PrivateReadFloors};
    assert_eq!(PrivateReadFloors::default().setting(), FloorsSetting::Off);
    let all = PrivateReadFloors {
        get_posts: 1000,
        get_post_thread: 1200,
        get_author_feed: 1200,
        get_timeline: 1200,
        get_bookmarks: 1200,
        get_likes: 800,
        get_reposted_by: 800,
        get_quotes: 1200,
        list_notifications: 1200,
        get_unread_count: 300,
        get_replies: 1200,
        search_posts: 1500,
    };
    assert_eq!(all.setting(), FloorsSetting::On);
    // The four keys a host file may still set, the rest at the default.
    let some = PrivateReadFloors {
        get_posts: 1000,
        get_post_thread: 1200,
        get_author_feed: 1200,
        get_timeline: 1200,
        ..PrivateReadFloors::default()
    };
    assert_eq!(
        some.setting(),
        FloorsSetting::Partial {
            zero: vec![
                "get_bookmarks",
                "get_likes",
                "get_reposted_by",
                "get_quotes",
                "list_notifications",
                "get_unread_count",
                "get_replies",
                "search_posts",
            ]
        }
    );
}

/// Private-threads design §5: `max_chain` is 1 to 100 (16 by default, 100
/// accepted), the paging keys are positive, and T-57's head-age cap: a
/// `sync.head_max_age_secs` above 600 is refused on every build.
#[test]
fn private_threads_keys_and_the_head_age_cap_are_checked() {
    let cfg = load("").expect("defaults");
    let read = &cfg.file.private_read;
    assert_eq!((read.max_chain, read.page_reads_factor, read.walk_batch_max), (16, 2, 1000));
    for ok in [1, 16, 100] {
        let cfg = load(&format!("[private_read]\nmax_chain = {ok}\n")).expect("in range");
        assert_eq!(cfg.file.private_read.max_chain, ok);
    }
    for bad in [0, 101] {
        let p = invalid_problems(load(&format!("[private_read]\nmax_chain = {bad}\n")));
        assert!(has(&p, "private_read.max_chain must be 1 to 100"), "{bad}: {p:?}");
    }
    for key in ["page_reads_factor", "walk_batch_max"] {
        let p = invalid_problems(load(&format!("[private_read]\n{key} = 0\n")));
        assert!(has(&p, &format!("private_read.{key} must be greater than 0")), "{p:?}");
    }
    assert_eq!(load("[sync]\nhead_max_age_secs = 600\n").expect("at the cap").file.sync.head_max_age_secs, 600);
    let p = invalid_problems(load("[sync]\nhead_max_age_secs = 601\n"));
    assert!(has(&p, "sync.head_max_age_secs must be at most 600"), "{p:?}");
}

/// T-57: without the `bench-heads` feature a `[bench]` section is an
/// unknown key, so a bench config cannot load on a deploy build.
#[cfg(not(feature = "bench-heads"))]
#[test]
fn a_bench_section_is_refused_without_the_feature() {
    assert!(matches!(load("[bench]\nheads_file = \"/tmp/heads\"\n"), Err(ConfigError::Parse(_))));
}

/// Under `bench-heads` the section parses (serve then requires the file).
#[cfg(feature = "bench-heads")]
#[test]
fn a_bench_section_parses_under_the_feature() {
    let cfg = load("[bench]\nheads_file = \"/tmp/heads\"\n").expect("parses");
    assert_eq!(cfg.file.bench.heads_file.as_deref(), Some(std::path::Path::new("/tmp/heads")));
}

#[test]
fn did_freshness_must_precede_staleness() {
    let p = invalid_problems(load("[identity]\ndid_fresh_secs = 90000\n"));
    assert!(has(&p, "did_fresh_secs must be less than"));
}

#[test]
fn every_problem_is_reported_at_once() {
    let text = "[indexing]\npublic_scope = \"full\"\n[firehose]\nprivate_relays = [\"nope\"]\nbatch_size = 0\n";
    let p = invalid_problems(load(text));
    assert!(p.len() >= 3, "{p:?}");
    let err = ConfigError::Invalid(p);
    assert!(err.to_string().starts_with("config_error: "));
}

#[test]
fn utc_time_of_day_parses() {
    let t = UtcTimeOfDay::try_from("23:59".to_string()).expect("valid");
    assert_eq!((t.hour(), t.minute()), (23, 59));
    assert_eq!(t.seconds_after_midnight(), 23 * 3600 + 59 * 60);
    assert!(UtcTimeOfDay::new(24, 0).is_none());
}

/// Stand-in design T1: without the `standin-hosts` feature a `[standin]`
/// section is an unknown key, so a test config cannot load on a deploy
/// build.
#[cfg(not(feature = "standin-hosts"))]
#[test]
fn a_standin_section_is_refused_without_the_feature() {
    let text = "[standin]\nhosts = [{ origin = \"http://172.29.77.21:8081\", intents = [\"public\"] }]\n";
    assert!(matches!(load(text), Err(ConfigError::Parse(_))));
    assert!(matches!(load("[standin]\n"), Err(ConfigError::Parse(_))));
}

/// Stand-in design T7 and T8, under the feature.
#[cfg(feature = "standin-hosts")]
mod standin {
    use super::*;
    use crate::net::standin::required;

    fn hosts(entries: &[(&str, &str)]) -> String {
        let rows: Vec<String> = entries.iter().map(|(origin, intents)| format!("  {{ origin = \"{origin}\", intents = [{intents}] }},")).collect();
        format!("[standin]\nhosts = [\n{}\n]\n", rows.join("\n"))
    }

    /// The problems of a config holding only `[standin]` with `entries`.
    fn standin_problems(entries: &[(&str, &str)]) -> Vec<String> {
        match load(&hosts(entries)) {
            Ok(_) => Vec::new(),
            Err(e) => invalid_problems(Err(e)),
        }
    }

    /// T7: the defaults pass validation (rule 1 is `serve`'s), a good
    /// listing passes, and each bad entry is exactly one problem.
    #[test]
    fn standin_hosts_are_validated() {
        let cfg = load("").expect("no [standin] passes validation");
        assert_eq!(cfg.file.standin, StandinConfig::default());
        let missing = required(&cfg.file.standin).expect_err("serve's rule 1");
        assert!(missing.contains("needs at least one [[standin.hosts]] entry"), "{missing}");
        let empty = load("[standin]\nhosts = []\n").expect("an empty list passes validation");
        assert!(required(&empty.file.standin).is_err());

        let good = [
            ("http://172.29.77.21:8081", "\"public\", \"internal\""),
            ("http://172.29.77.21:8082", "\"public\""),
            ("http://10.9.8.7:9000", "\"internal\""),
            ("http://192.168.4.20:8000", "\"public\""),
            ("http://[fd00::21]:8083", "\"public\""),
        ];
        assert_eq!(standin_problems(&good), Vec::<String>::new());
        let listed = load(&hosts(&good)).expect("valid");
        required(&listed.file.standin).expect("rule 1 passes");
        assert_eq!(listed.file.standin.hosts[0].intents, [StandinIntent::Public, StandinIntent::Internal]);

        for origin in [
            "http://standin.example:8081",
            "http://93.184.216.34:8081",
            "http://127.0.0.1:8081",
            "http://169.254.169.254:8081",
            "http://100.64.0.1:8081",
            "http://172.29.77.1:8081",
            "http://10.0.0.0:8081",
            "http://192.168.1.255:8081",
            "http://[::1]:8081",
            "http://[fe80::1]:8081",
            "http://[fd00::1]:8081",
            "http://[::ffff:172.29.77.21]:8081",
            "https://172.29.77.21:8081",
            "http://172.29.77.21",
            "http://172.29.77.21:80",
            "http://172.29.77.21:8081/xrpc",
            "http://172.29.77.21:8081/",
            "http://user@172.29.77.21:8081",
            "HTTP://172.29.77.21:8081",
        ] {
            let p = standin_problems(&[(origin, "\"public\"")]);
            assert_eq!(p.len(), 1, "{origin}: {p:?}");
            assert!(p[0].starts_with("standin.hosts: "), "{p:?}");
        }
        for (entries, needle) in [
            (vec![("http://172.29.77.21:8081", "")], "lists no intents"),
            (vec![("http://172.29.77.21:8081", "\"public\", \"public\"")], "repeats an intent"),
            (vec![("http://172.29.77.21:8081", "\"public\""), ("http://172.29.77.21:8081", "\"internal\"")], "is listed twice"),
        ] {
            let p = standin_problems(&entries);
            assert_eq!(p.len(), 1, "{p:?}");
            assert!(has(&p, needle), "{needle}: {p:?}");
        }
        let nine: Vec<String> = (8081..=8089).map(|port| format!("http://172.29.77.21:{port}")).collect();
        let nine: Vec<(&str, &str)> = nine.iter().map(|o| (o.as_str(), "\"public\"")).collect();
        let p = standin_problems(&nine);
        assert_eq!(p.len(), 1, "{p:?}");
        assert!(has(&p, "9 entries, at most 8"), "{p:?}");
        assert_eq!(standin_problems(&nine[..8]), Vec::<String>::new());
        // `viewer_private` is not an intent that can be named.
        assert!(matches!(load(&hosts(&[("http://172.29.77.21:8081", "\"viewer_private\"")])), Err(ConfigError::Parse(_))));
        assert!(matches!(load("[standin]\nhosts = [{ origin = \"http://172.29.77.21:8081\", intents = [\"public\"], extra = 1 }]\n"), Err(ConfigError::Parse(_))));
    }

    /// The problems naming `key` in `file` with `[standin]` from `entries`.
    fn key_problems(entries: &[(&str, &str)], settings: &str, edit: impl Fn(&mut FileConfig), key: &str) -> Vec<String> {
        let mut file = FileConfig::parse(&format!("{}{settings}", hosts(entries))).expect("parse");
        edit(&mut file);
        crate::config::validate::problems(&file, "appview.example.com").into_iter().filter(|p| p.contains(key)).collect()
    }

    /// T8: each URL key takes an `http` value only on an origin listed for
    /// every intent its clients use; `video.cdn_url` never does.
    #[test]
    fn url_keys_take_http_only_on_an_origin_listed_for_their_intents() {
        let both = "\"public\", \"internal\"";
        let public = "\"public\"";
        let internal = "\"internal\"";
        let none = |_: &mut FileConfig| {};
        let appview = |url: &'static str| {
            move |f: &mut FileConfig| {
                let n = f.constellation.networks.iter_mut().find(|n| n.name == "bluesky").expect("bluesky");
                n.appview_url = url.to_string();
            }
        };
        // (key, settings, the intents it needs)
        let cases: [(&str, String, &str); 4] = [
            ("farsight.url", "[farsight]\nurl = \"http://172.29.77.21:8081\"\n".to_string(), both),
            ("microcosm.slingshot_url", "[microcosm]\nslingshot_url = \"http://172.29.77.21:8081\"\n".to_string(), public),
            ("microcosm.constellation_url", "[microcosm]\nconstellation_url = \"http://172.29.77.21:8081\"\n".to_string(), both),
            ("identity.plc_url", "[identity]\nplc_url = \"http://172.29.77.21:8081\"\n".to_string(), both),
        ];
        for (key, settings, needs) in &cases {
            let origin = "http://172.29.77.21:8081";
            assert_eq!(key_problems(&[(origin, *needs)], settings, none, key), Vec::<String>::new(), "{key} on a listed origin");
            assert_eq!(key_problems(&[(origin, both)], settings, none, key), Vec::<String>::new(), "{key} listed for both");
            assert_eq!(key_problems(&[("http://172.29.77.21:8082", both)], settings, none, key).len(), 1, "{key} on an unlisted origin");
            if *needs == both {
                assert_eq!(key_problems(&[(origin, public)], settings, none, key).len(), 1, "{key} listed for public only");
                assert_eq!(key_problems(&[(origin, internal)], settings, none, key).len(), 1, "{key} listed for internal only");
            } else {
                assert_eq!(key_problems(&[(origin, internal)], settings, none, key).len(), 1, "{key} listed for internal only");
            }
        }
        let key = "constellation.networks[bluesky].appview_url";
        let listed = [("http://172.29.77.21:8082", public)];
        assert_eq!(key_problems(&listed, "", appview("http://172.29.77.21:8082"), key), Vec::<String>::new());
        assert_eq!(key_problems(&listed, "", appview("http://172.29.77.21:8083"), key).len(), 1);
        assert_eq!(key_problems(&[("http://172.29.77.21:8082", internal)], "", appview("http://172.29.77.21:8082"), key).len(), 1);
        // No leave for the video CDN, listed or not.
        let video = "[video]\ncdn_url = \"http://172.29.77.21:8081\"\n";
        assert_eq!(key_problems(&[("http://172.29.77.21:8081", both)], video, none, "video.cdn_url").len(), 1);
        // A hostname PLC over http keeps its old rule: allowed, unlisted.
        let hostname_plc = "[identity]\nplc_url = \"http://plc.test.internal:2582\"\n";
        assert_eq!(key_problems(&[("http://172.29.77.21:8086", both)], hostname_plc, none, "identity.plc_url"), Vec::<String>::new());
    }
}
