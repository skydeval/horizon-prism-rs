//! Post-deserialization validation of `app.toml`.
//!
//! Every problem is collected, so an operator fixes a bad config in one pass.
//! Rules stated by design-v2 are marked with their section; the others guard
//! values that would otherwise fail later at runtime (zero-rate token buckets,
//! a division by a zero budget, an empty private-relay set).

use std::collections::BTreeSet;

use super::sections::{
    COMMUNITY_NETWORK, FileConfig, LOCAL_APPVIEW, NetworkAuth, PrivateReadFloors, PublicScope,
};
use crate::net::Intent;

/// Metrics label for relays absent from `firehose.relay_labels` (§5.11).
pub const OTHER_RELAY_LABEL: &str = "other";

/// The largest `sync.head_max_age_secs` (private-threads design §5): a
/// runner or test config copied to a deploy cannot stretch the read path's
/// head check (§6.11.3) past this.
pub const MAX_HEAD_AGE_SECS: u64 = 600;

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
    crate::net::transport::problems(&cfg.net, &mut out);
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
    // design-v3 §13: below these a Thread Lens response could add nothing,
    // or could never reach a parent beyond the ordinary view.
    for (key, value, least) in [
        ("threadlens.page_nodes", cfg.threadlens.page_nodes, 2),
        ("threadlens.listing_calls", cfg.threadlens.listing_calls, 2),
        ("threadlens.parent_rounds", cfg.threadlens.parent_rounds, 1),
        ("threadlens.verdict_round_timeout_ms", cfg.threadlens.verdict_round_timeout_ms, 1),
    ] {
        if value < least {
            out.push(format!("{key} must be at least {least}"));
        }
    }
    lists_problems(cfg, &mut out);
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
    if cfg.identity.resolve_handle_cache_max == 0 {
        out.push("identity.resolve_handle_cache_max must be at least 1".to_string());
    }
    for (name, secs) in [
        ("resolve_handle_ttl_secs", cfg.identity.resolve_handle_ttl_secs),
        ("resolve_handle_negative_ttl_secs", cfg.identity.resolve_handle_negative_ttl_secs),
    ] {
        if !(1..=86_400).contains(&secs) {
            out.push(format!("identity.{name} must be 1 to 86400"));
        }
    }
    if !(1..=1000).contains(&cfg.labelers.get_services_max_dids) {
        out.push("labelers.get_services_max_dids must be 1 to 1000".to_string());
    }
    if !(1..=1000).contains(&cfg.labels.query_max_patterns) {
        out.push("labels.query_max_patterns must be 1 to 1000".to_string());
    }
    if cfg.drafts.max_per_actor == 0 {
        out.push("drafts.max_per_actor must be at least 1".to_string());
    }
    if !(1024..=16 * 1024 * 1024).contains(&cfg.drafts.max_bytes) {
        out.push("drafts.max_bytes must be 1024 to 16777216".to_string());
    }
    if cfg.bookmarks.count_floor == 0 {
        out.push("bookmarks.count_floor must be at least 1".to_string());
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
    discovery(cfg, &mut out);
    #[cfg(feature = "standin-hosts")]
    crate::net::standin::problems(&cfg.standin, &mut out);
    out
}

/// `[search]`, `[suggestions]` and `[trends]` (design-v3 §7, §9, §13):
/// values with which a search or a refresh could not run at all.
fn discovery(cfg: &FileConfig, out: &mut Vec<String>) {
    let (search, suggestions, trends) = (&cfg.search, &cfg.suggestions, &cfg.trends);
    for (key, value) in [
        ("search.statement_timeout_ms", search.statement_timeout_ms),
        ("search.hits_total_timeout_ms", search.hits_total_timeout_ms),
        ("search.max_filter_values", u64::from(search.max_filter_values)),
        ("search.top_candidates", u64::from(search.top_candidates)),
        ("search.typeahead_candidates", u64::from(search.typeahead_candidates)),
        ("search.recent_window_days", u64::from(search.recent_window_days)),
        ("search.top_window_days", u64::from(search.top_window_days)),
        ("suggestions.active_days", u64::from(suggestions.active_days)),
        ("suggestions.pool_size", u64::from(suggestions.pool_size)),
        ("trends.topics_window_hours", u64::from(trends.topics_window_hours)),
        ("trends.window_hours", u64::from(trends.window_hours)),
        ("trends.candidate_count", u64::from(trends.candidate_count)),
        ("trends.subject_lookback_hours", u64::from(trends.subject_lookback_hours)),
        ("trends.aggregate_retention_hours", u64::from(trends.aggregate_retention_hours)),
        ("trends.refresh_timeout_ms", trends.refresh_timeout_ms),
    ] {
        if value == 0 {
            out.push(format!("{key} must be at least 1"));
        }
    }
    // The offset cursor of `top` pages over the candidates and no further.
    if search.max_offset > search.top_candidates {
        out.push(format!("search.max_offset = {} is above search.top_candidates = {}", search.max_offset, search.top_candidates));
    }
    for (key, value) in [
        ("trends.decay_half_life_hours", trends.decay_half_life_hours.0),
        ("trends.started_ratio", trends.started_ratio.0),
        ("trends.hot_ratio", trends.hot_ratio.0),
    ] {
        if !(value.is_finite() && value > 0.0) {
            out.push(format!("{key} must be a number above 0"));
        }
    }
    // The aggregate must hold every hour a trend reads.
    if trends.aggregate_retention_hours < trends.window_hours.max(trends.topics_window_hours) {
        out.push("trends.aggregate_retention_hours must cover trends.window_hours and trends.topics_window_hours".to_string());
    }
    if !trends.topic_link_template.contains("{tag}") {
        out.push("trends.topic_link_template must contain {tag}".to_string());
    }
}

/// `[lists]` rule-4 keys (design-v3 §4.3, §13).
fn lists_problems(cfg: &FileConfig, out: &mut Vec<String>) {
    let l = &cfg.lists;
    for (name, value) in [
        ("subscribed_max_per_member", u64::from(l.subscribed_max_per_member)),
        ("max_owners_per_member", u64::from(l.max_owners_per_member)),
        ("subscribed_max_items", u64::from(l.subscribed_max_items)),
        ("subscription_tombstones_max_per_owner", u64::from(l.subscription_tombstones_max_per_owner)),
        ("sync_min_interval_secs", u64::from(l.sync_min_interval_secs)),
        ("sync_max_concurrent", u64::from(l.sync_max_concurrent)),
        ("sync_slow_max_concurrent", u64::from(l.sync_slow_max_concurrent)),
        ("sync_stuck_secs", u64::from(l.sync_stuck_secs)),
        ("subscribed_max_owner_listitems", l.subscribed_max_owner_listitems),
        ("sync_max_pages", u64::from(l.sync_max_pages)),
        ("sync_probe_call_secs", u64::from(l.sync_probe_call_secs)),
        ("sync_min_records_per_call_sec", u64::from(l.sync_min_records_per_call_sec)),
        ("sync_max_call_secs", u64::from(l.sync_max_call_secs)),
        ("sync_max_wall_secs", u64::from(l.sync_max_wall_secs)),
    ] {
        if value == 0 {
            out.push(format!("lists.{name} must be at least 1"));
        }
    }
    if l.sync_probe_call_secs > l.sync_max_call_secs {
        out.push("lists.sync_probe_call_secs must be at most lists.sync_max_call_secs".to_string());
    }
    // Two backfill workers always stay free for readiness jobs. Worker 0
    // never serves the lanes (public-indexing §8.4), so it is one of the
    // two: the lanes take at most `concurrency - 2` of the other workers.
    let lanes = u64::from(l.sync_max_concurrent) + u64::from(l.sync_slow_max_concurrent);
    if lanes + 2 > u64::from(cfg.backfill.concurrency) {
        out.push(format!(
            "lists.sync_max_concurrent + lists.sync_slow_max_concurrent ({lanes}) must be at most backfill.concurrency - 2 ({})",
            i64::from(cfg.backfill.concurrency) - 2
        ));
    }
    // On an idle host the wall bound must never cut an owner the records,
    // pages, throughput and in-call bounds would let finish.
    let rate = u64::from(cfg.backfill.per_pds_rate_per_sec.max(1));
    let least_wall = u64::from(l.sync_max_call_secs) + l.subscribed_max_owner_listitems.div_ceil(100).div_ceil(rate);
    if u64::from(l.sync_max_wall_secs) < least_wall {
        out.push(format!(
            "lists.sync_max_wall_secs must be at least {least_wall} (sync_max_call_secs + subscribed_max_owner_listitems / 100 / backfill.per_pds_rate_per_sec)"
        ));
    }
}

fn indexing(cfg: &FileConfig, out: &mut Vec<String>) {
    // §9.1 / lock #4: `full` is recognized, then refused.
    if cfg.indexing.public_scope == Some(PublicScope::Full) {
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
            } else if let Err(e) = with_leave(cfg, &n.appview_url, &[Intent::Public], https_url(&n.appview_url)) {
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
    // A PLC on a private literal is reached only as a listed test origin.
    #[cfg(feature = "standin-hosts")]
    {
        let private_literal = url::Url::parse(&cfg.identity.plc_url).ok().is_some_and(|u| {
            u.scheme() == "http"
                && match u.host() {
                    Some(url::Host::Ipv4(v4)) => crate::net::is_blocked(v4.into()),
                    Some(url::Host::Ipv6(v6)) => crate::net::is_blocked(v6.into()),
                    _ => false,
                }
        });
        if private_literal && !crate::net::standin::listed_for(&cfg.standin, &cfg.identity.plc_url, &[Intent::Public, Intent::Internal]) {
            out.push(format!(
                "identity.plc_url: {:?} names a private address that [standin] does not list for public and internal",
                cfg.identity.plc_url
            ));
        }
    }
    // §5.14.4: Farsight over https only (or a listed test origin).
    if let Some(url) = &cfg.farsight.url {
        let checked = match url::Url::parse(url) {
            Ok(u) if u.scheme() == "https" && u.host_str().is_some() => Ok(()),
            Ok(_) => Err(format!("farsight.url = {url:?} must be an https URL")),
            Err(e) => Err(format!("farsight.url = {url:?} is not a URL: {e}")),
        };
        if let Err(e) = with_leave(cfg, url, &[Intent::Public, Intent::Internal], checked) {
            out.push(e);
        }
    }
    for (key, value, intents) in [
        ("microcosm.slingshot_url", &cfg.microcosm.slingshot_url, &[Intent::Public][..]),
        ("microcosm.constellation_url", &cfg.microcosm.constellation_url, &[Intent::Public, Intent::Internal][..]),
        // No leave: an empty intent list is never listed.
        ("video.cdn_url", &cfg.video.cdn_url, &[][..]),
    ] {
        if let Err(e) = with_leave(cfg, value, intents, https_url(value)) {
            out.push(format!("{key}: {e}"));
        }
    }
}

/// `checked`, an https rule's result, with the Phase B leave (stand-in
/// design §3.3 rule 8): in a test build, an `http` value whose origin is
/// listed for every one of `intents` passes. In any other build, `checked`.
fn with_leave(_cfg: &FileConfig, _value: &str, _intents: &[Intent], checked: Result<(), String>) -> Result<(), String> {
    #[cfg(feature = "standin-hosts")]
    {
        if checked.is_err() && crate::net::standin::listed_for(&_cfg.standin, _value, _intents) {
            return Ok(());
        }
    }
    checked
}

fn numbers(cfg: &FileConfig, out: &mut Vec<String>) {
    let nonzero: [(&str, u64); 25] = [
        ("firehose.batch_size", u64::from(cfg.firehose.batch_size)),
        ("firehose.deterministic_attempts", u64::from(cfg.firehose.deterministic_attempts)),
        ("identity.did_fresh_secs", cfg.identity.did_fresh_secs),
        ("identity.jwt_issuer_tracker_max", cfg.identity.jwt_issuer_tracker_max),
        ("kryphocron.oracle_freshness_bound_secs", cfg.kryphocron.oracle_freshness_bound_secs),
        ("private_read.bind_concurrency", u64::from(cfg.private_read.bind_concurrency)),
        ("private_read.membership_retry_after_secs", cfg.private_read.membership_retry_after_secs),
        ("private_search.scan_candidates", u64::from(cfg.private_search.scan_candidates)),
        ("private_search.span_hours", u64::from(cfg.private_search.span_hours)),
        ("private_search.max_age_days", u64::from(cfg.private_search.max_age_days)),
        ("sync.max_probe_rate_per_host", u64::from(cfg.sync.max_probe_rate_per_host)),
        ("sync.read_fetch_rate_per_host", u64::from(cfg.sync.read_fetch_rate_per_host)),
        ("sync.catch_up_rate_per_host", u64::from(cfg.sync.catch_up_rate_per_host)),
        ("index_request.catch_up_min_interval_secs", cfg.index_request.catch_up_min_interval_secs),
        ("index_request.catch_up_max_pages", u64::from(cfg.index_request.catch_up_max_pages)),
        ("backfill.concurrency", u64::from(cfg.backfill.concurrency)),
        ("backfill.per_pds_rate_per_sec", u64::from(cfg.backfill.per_pds_rate_per_sec)),
        ("backfill.follows_pass_ceiling", cfg.backfill.follows_pass_ceiling),
        ("counts.refresh_rate_per_sec", u64::from(cfg.counts.refresh_rate_per_sec)),
        ("retention.tombstone_days", u64::from(cfg.retention.tombstone_days)),
        ("cache.read_time_bytes", cfg.cache.read_time_bytes),
        ("cache.viewer_private_ttl_secs", cfg.cache.viewer_private_ttl_secs),
        ("cache.viewer_private_max", cfg.cache.viewer_private_max),
        ("cache.viewer_private_per_viewer_max", cfg.cache.viewer_private_per_viewer_max),
        ("storage.budget_bytes", cfg.storage.budget_bytes),
    ];
    for (key, value) in nonzero {
        if value == 0 {
            out.push(format!("{key} must be greater than 0"));
        }
    }
    // Floors are 0 (off) by default and at most 10 s (timing-floors design §2).
    for (key, ms) in cfg.private_read.floor_ms.entries() {
        if ms > PrivateReadFloors::MAX_MS {
            out.push(format!("private_read.floor_ms.{key} must be at most {}", PrivateReadFloors::MAX_MS));
        }
    }
    if cfg.identity.did_fresh_secs >= cfg.identity.did_stale_max_secs {
        out.push("identity.did_fresh_secs must be less than identity.did_stale_max_secs".to_string());
    }
    if cfg.counts.fresh_secs > cfg.counts.stale_max_secs {
        out.push("counts.fresh_secs must not exceed counts.stale_max_secs".to_string());
    }
    // A span no longer than the search's reach (private posting §8).
    let search = &cfg.private_search;
    if u64::from(search.span_hours) > 24 * u64::from(search.max_age_days) {
        out.push("private_search.span_hours must be at most 24 × private_search.max_age_days".to_string());
    }
    // The private-threads walk and paging (private-threads design §5).
    let read = &cfg.private_read;
    if !(1..=100).contains(&read.max_chain) {
        out.push("private_read.max_chain must be 1 to 100".to_string());
    }
    if read.page_reads_factor == 0 {
        out.push("private_read.page_reads_factor must be greater than 0".to_string());
    }
    if read.walk_batch_max == 0 {
        out.push("private_read.walk_batch_max must be greater than 0".to_string());
    }
    if cfg.sync.head_max_age_secs > MAX_HEAD_AGE_SECS {
        out.push(format!("sync.head_max_age_secs must be at most {MAX_HEAD_AGE_SECS}"));
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

#[cfg(test)]
mod tests {
    use super::*;

    /// design-v3 §13: the search, suggestion and trend defaults are valid,
    /// and a value a search or a refresh could not run with is named.
    #[test]
    fn discovery_settings_are_checked() {
        let found = |toml: &str| {
            let mut out = Vec::new();
            discovery(&FileConfig::parse(toml).expect("parse"), &mut out);
            out
        };
        assert_eq!(found(""), Vec::<String>::new());
        for (toml, needle) in [
            ("[search]\nstatement_timeout_ms = 0\n", "search.statement_timeout_ms must be at least 1"),
            ("[search]\nmax_offset = 5000\n", "search.max_offset = 5000 is above search.top_candidates = 2000"),
            ("[suggestions]\npool_size = 0\n", "suggestions.pool_size must be at least 1"),
            ("[trends]\ndecay_half_life_hours = 0.0\n", "trends.decay_half_life_hours must be a number above 0"),
            ("[trends]\nwindow_hours = 100\n", "trends.aggregate_retention_hours must cover"),
            ("[trends]\ntopic_link_template = \"/hashtag\"\n", "trends.topic_link_template must contain {tag}"),
        ] {
            let out = found(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        }
        // A suggested topic needs its topic and its link.
        assert!(FileConfig::parse("[[trends.suggested_topics]]\ntopic = \"#x\"\n").is_err());
    }

    /// design-v3 §5.14.4: `farsight.url` must be an https URL. The rule
    /// lives here, beside the other upstream URLs, with its texts as they
    /// were in `farsight::config_problems`.
    #[test]
    fn farsight_url_must_be_https() {
        let found = |toml: &str| problems(&FileConfig::parse(toml).expect("parse"), "appview.example.com");
        let has = |toml: &str, needle: &str| {
            let out = found(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        };
        assert!(!found("[farsight]\nurl = \"https://farsight.example.net\"\n").iter().any(|p| p.contains("farsight.url")));
        has("[farsight]\nurl = \"http://farsight.example.net\"\n", "farsight.url = \"http://farsight.example.net\" must be an https URL");
        has("[farsight]\nurl = \"farsight\"\n", "farsight.url = \"farsight\" is not a URL");
    }
}
