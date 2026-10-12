//! Post-deserialization validation of `app.toml`.
//!
//! Every problem is collected, so an operator fixes a bad config in one pass.
//! Rules stated by design-v2 are marked with their section; the others guard
//! values that would otherwise fail later at runtime (zero-rate token buckets,
//! a division by a zero budget, an empty private-relay set).

use std::collections::BTreeSet;

use super::sections::{
    COMMUNITY_NETWORK, FileConfig, LOCAL_APPVIEW, NetworkAuth, PublicScope,
};

/// Metrics label for relays absent from `firehose.relay_labels` (§5.11).
pub const OTHER_RELAY_LABEL: &str = "other";

/// Validate a parsed file config, returning every problem found.
/// `public_hostname` is `APP_PUBLIC_HOSTNAME`, which a proxied service's
/// URL must not name (design-v3 §6.7.2.3 rule 4).
#[must_use]
pub fn problems(cfg: &FileConfig, public_hostname: &str) -> Vec<String> {
    let mut out = Vec::new();
    indexing(cfg, &mut out);
    firehose(cfg, &mut out);
    networks(cfg, &mut out);
    urls(cfg, &mut out);
    numbers(cfg, &mut out);
    crate::services::problems(cfg, public_hostname, &mut out);
    crate::xrpc::rate_limit::problems(&cfg.rate_limits, &mut out);
    crate::xrpc::pools::problems(cfg, &mut out);
    crate::xrpc::backstop::problems(&cfg.auth, &mut out);
    if cfg.schema_backfill.batch_size == 0 {
        out.push("schema_backfill.batch_size must be at least 1".to_string());
    }
    if cfg.known_likers.scan_max_pages > 1000 {
        out.push("known_likers.scan_max_pages must be at most 1000".to_string());
    }
    if cfg.thread_v2.max_parents > 1000 {
        out.push("thread_v2.max_parents must be at most 1000".to_string());
    }
    if cfg.op_thread.max_walk > 1000 {
        out.push("op_thread.max_walk must be at most 1000".to_string());
    }
    if !(1..=100).contains(&cfg.lists.read_listing_pages) {
        out.push("lists.read_listing_pages must be 1 to 100".to_string());
    }
    if cfg.known_followers.profile_scan_pages > 100 {
        out.push("known_followers.profile_scan_pages must be at most 100".to_string());
    }
    if !(1..=1000).contains(&cfg.known_followers.scan_max_pages) {
        out.push("known_followers.scan_max_pages must be 1 to 1000".to_string());
    }
    if cfg.starter_packs.counts_refresh_secs < 60 {
        out.push("starter_packs.counts_refresh_secs must be at least 60".to_string());
    }
    if !(60..=2_592_000).contains(&cfg.notifications.subscribed_post_max_age_secs) {
        out.push("notifications.subscribed_post_max_age_secs must be 60 to 2592000".to_string());
    }
    if cfg.bookmarks.max_per_actor == 0 {
        out.push("bookmarks.max_per_actor must be at least 1".to_string());
    }
    if cfg.activity_subscriptions.max_per_actor == 0 {
        out.push("activity_subscriptions.max_per_actor must be at least 1".to_string());
    }
    if cfg.thread_mutes.max_per_actor == 0 {
        out.push("thread_mutes.max_per_actor must be at least 1".to_string());
    }
    if cfg.thread.max_nodes == 0 {
        out.push("thread.max_nodes must be at least 1".to_string());
    }
    out
}

fn indexing(cfg: &FileConfig, out: &mut Vec<String>) {
    // §9.1 / lock #4: `full` is recognized, then refused.
    if cfg.indexing.public_scope == PublicScope::Full {
        out.push("public_scope=full is not implemented in v1".to_string());
    }
}

fn firehose(cfg: &FileConfig, out: &mut Vec<String>) {
    let fh = &cfg.firehose;
    let labels: BTreeSet<&str> = fh.relay_labels.values().map(String::as_str).collect();
    if labels.len() != fh.relay_labels.len() {
        out.push("firehose.relay_labels: each relay URL needs a distinct label".to_string());
    }
    if labels.contains(OTHER_RELAY_LABEL) {
        out.push(format!(
            "firehose.relay_labels: {OTHER_RELAY_LABEL:?} is reserved for unlisted relays"
        ));
    }
    for url in fh.relay_labels.keys() {
        if !(url.starts_with("wss://") || url.starts_with("ws://")) {
            out.push(format!("firehose.relay_labels: {url:?} is not a ws(s):// relay URL"));
        }
    }
    // §5.3: a private relay missing from relay_labels would fail private reads
    // closed forever.
    if fh.private_relays.is_empty() {
        out.push("firehose.private_relays must name at least one relay label".to_string());
    }
    for relay in &fh.private_relays {
        if !labels.contains(relay.as_str()) {
            out.push(format!(
                "firehose.private_relays: {relay:?} is not a value in firehose.relay_labels"
            ));
        }
    }
    for (key, value) in [
        ("firehose.stream_key", &fh.stream_key),
        ("firehose.group", &fh.group),
        ("firehose.consumer_name", &fh.consumer_name),
        ("firehose.dead_letter_stream", &fh.dead_letter_stream),
    ] {
        if value.trim().is_empty() {
            out.push(format!("{key} must not be empty"));
        }
    }
}

fn networks(cfg: &FileConfig, out: &mut Vec<String>) {
    let c = &cfg.constellation;
    let mut names = BTreeSet::new();
    for n in &c.networks {
        if n.name.trim().is_empty() {
            out.push("constellation.networks: a network has an empty name".to_string());
            continue;
        }
        if !names.insert(n.name.as_str()) {
            out.push(format!("constellation.networks: duplicate network {:?}", n.name));
        }
        let is_community = n.name == COMMUNITY_NETWORK;
        if is_community && n.appview_url != LOCAL_APPVIEW {
            out.push(format!(
                "constellation.networks: {COMMUNITY_NETWORK:?} must use appview_url = {LOCAL_APPVIEW:?}"
            ));
        }
        if !is_community {
            if n.appview_url == LOCAL_APPVIEW {
                out.push(format!(
                    "constellation.networks: only {COMMUNITY_NETWORK:?} may use appview_url = {LOCAL_APPVIEW:?} ({:?})",
                    n.name
                ));
            } else if let Err(e) = https_url(&n.appview_url) {
                out.push(format!("constellation.networks[{}].appview_url: {e}", n.name));
            }
            if n.enabled && (n.rate_per_sec.unwrap_or(0) == 0 || n.burst.unwrap_or(0) == 0) {
                out.push(format!(
                    "constellation.networks[{}]: an enabled upstream needs rate_per_sec and burst > 0",
                    n.name
                ));
            }
        }
        // §8.1: startup rejects enabled service_jwt with an empty appview_did.
        if n.enabled && n.auth == NetworkAuth::ServiceJwt && n.appview_did.trim().is_empty() {
            out.push(format!(
                "constellation.networks[{}]: auth = \"service_jwt\" requires appview_did when enabled",
                n.name
            ));
        }
        if !n.appview_did.is_empty() && !proto_blue::syntax::Did::is_valid(&n.appview_did) {
            out.push(format!(
                "constellation.networks[{}].appview_did: {:?} is not a DID",
                n.name, n.appview_did
            ));
        }
        for p in &n.pds_patterns {
            if let Err(e) = pds_pattern(p) {
                out.push(format!("constellation.networks[{}].pds_patterns: {e}", n.name));
            }
        }
    }
    if c.community().is_none() {
        out.push(format!(
            "constellation.networks: a network named {COMMUNITY_NETWORK:?} is required"
        ));
    }
    let mut seen = BTreeSet::new();
    for name in &c.cascade.order {
        if name == COMMUNITY_NETWORK {
            out.push(format!(
                "constellation.cascade.order: {COMMUNITY_NETWORK:?} is served locally and cannot be a cascade step"
            ));
        } else if !names.contains(name.as_str()) {
            out.push(format!(
                "constellation.cascade.order: {name:?} is not a configured network"
            ));
        }
        if !seen.insert(name.as_str()) {
            out.push(format!("constellation.cascade.order: {name:?} is listed twice"));
        }
    }
    for p in &cfg.community.enroll_pds_patterns {
        if let Err(e) = pds_pattern(p) {
            out.push(format!("community.enroll_pds_patterns: {e}"));
        }
    }
}

fn urls(cfg: &FileConfig, out: &mut Vec<String>) {
    // §8.5: https only, except a test PLC.
    match url::Url::parse(&cfg.identity.plc_url) {
        Ok(u) if (u.scheme() == "https" || u.scheme() == "http") && u.host_str().is_some() => {}
        _ => out.push(format!(
            "identity.plc_url: {:?} must be an http(s) URL",
            cfg.identity.plc_url
        )),
    }
    for (key, value) in [
        ("microcosm.slingshot_url", &cfg.microcosm.slingshot_url),
        ("microcosm.constellation_url", &cfg.microcosm.constellation_url),
    ] {
        if let Err(e) = https_url(value) {
            out.push(format!("{key}: {e}"));
        }
    }
}

fn numbers(cfg: &FileConfig, out: &mut Vec<String>) {
    let nonzero: [(&str, u64); 17] = [
        ("firehose.batch_size", u64::from(cfg.firehose.batch_size)),
        ("firehose.deterministic_attempts", u64::from(cfg.firehose.deterministic_attempts)),
        ("identity.did_fresh_secs", cfg.identity.did_fresh_secs),
        ("identity.jwt_issuer_tracker_max", cfg.identity.jwt_issuer_tracker_max),
        ("kryphocron.oracle_freshness_bound_secs", cfg.kryphocron.oracle_freshness_bound_secs),
        ("private_read.bind_concurrency", u64::from(cfg.private_read.bind_concurrency)),
        ("private_read.floor_ms.get_posts", cfg.private_read.floor_ms.get_posts),
        ("private_read.floor_ms.get_post_thread", cfg.private_read.floor_ms.get_post_thread),
        ("private_read.floor_ms.get_author_feed", cfg.private_read.floor_ms.get_author_feed),
        ("sync.max_probe_rate_per_host", u64::from(cfg.sync.max_probe_rate_per_host)),
        ("sync.read_fetch_rate_per_host", u64::from(cfg.sync.read_fetch_rate_per_host)),
        ("backfill.concurrency", u64::from(cfg.backfill.concurrency)),
        ("backfill.per_pds_rate_per_sec", u64::from(cfg.backfill.per_pds_rate_per_sec)),
        ("counts.refresh_rate_per_sec", u64::from(cfg.counts.refresh_rate_per_sec)),
        ("retention.tombstone_days", u64::from(cfg.retention.tombstone_days)),
        ("cache.read_time_bytes", cfg.cache.read_time_bytes),
        ("storage.budget_bytes", cfg.storage.budget_bytes),
    ];
    for (key, value) in nonzero {
        if value == 0 {
            out.push(format!("{key} must be greater than 0"));
        }
    }
    if cfg.identity.did_fresh_secs >= cfg.identity.did_stale_max_secs {
        out.push("identity.did_fresh_secs must be less than identity.did_stale_max_secs".to_string());
    }
    if cfg.counts.fresh_secs > cfg.counts.stale_max_secs {
        out.push("counts.fresh_secs must not exceed counts.stale_max_secs".to_string());
    }
}

/// An absolute `https://` URL with a host.
fn https_url(value: &str) -> Result<(), String> {
    match url::Url::parse(value) {
        Ok(u) if u.scheme() == "https" && u.host_str().is_some() => Ok(()),
        Ok(_) => Err(format!("{value:?} must be an https:// URL (§8.5)")),
        Err(e) => Err(format!("{value:?} is not a URL: {e}")),
    }
}

/// A PDS host pattern: an exact lowercase host, or `*.` followed by one (§8.2).
///
/// # Errors
///
/// A reason string when the pattern is malformed.
pub fn pds_pattern(pattern: &str) -> Result<(), String> {
    let host = pattern.strip_prefix("*.").unwrap_or(pattern);
    super::env::validate_hostname(host).map_err(|e| format!("{pattern:?}: {e}"))
}
