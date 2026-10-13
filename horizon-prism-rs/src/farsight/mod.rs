//! Farsight: the stack's block and block-list index (design-v3 §5.14,
//! proposed locked decision 37).
//!
//! This AppView is community-scoped and does not index the network's block
//! lists, so it cannot know from its own rows when another account blocks
//! the viewer through a list. It asks Farsight at render time:
//! `checkBlocks(actor = viewer, others = the accounts on the page)`.
//!
//! This module is the only code that calls Farsight and the only code that
//! interprets its answers. It holds no storage.
//!
//! - [`did`]: the DIDs Farsight accepts, and their canonical form.
//! - [`chunks`]: splitting a lookup into calls.
//! - [`answer`]: a pair answer, reading a response, and the cache's merge
//!   rule.

//! - [`client`]: the HTTP call and how its failures are classified.

pub mod answer;
pub mod chunks;
pub mod client;
pub mod did;
pub mod fallback;
pub mod gate;
pub mod lookup;
pub mod memory;
pub mod retention;

pub use answer::{ChunkAnswer, Coverage, PairAnswer, Relation};
pub use client::{CallError, FarsightClient};
pub use lookup::{Farsight, Loaded, Lookup, PairAnswers, PairBudget, PurgeHook};

use crate::config::{FarsightConfig, LOCAL_APPVIEW, NetworkAuth, NetworkConfig, RateClassConfig};
use crate::xrpc::rate_limit::class_problem;

/// Every problem in `[farsight]` (design-v3 §5.14.4, §13). `token` is
/// whether `FARSIGHT_TOKEN` is set. With `url` unset the feature is off and
/// only the numbers are checked.
pub fn config_problems(cfg: &FarsightConfig, token: bool, out: &mut Vec<String>) {
    if let Some(url) = &cfg.url {
        match url::Url::parse(url) {
            Ok(u) if u.scheme() == "https" && u.host_str().is_some() => {}
            Ok(_) => out.push(format!("farsight.url = {url:?} must be an https URL")),
            Err(e) => out.push(format!("farsight.url = {url:?} is not a URL: {e}")),
        }
        if !token {
            out.push("farsight.url is set without FARSIGHT_TOKEN: Farsight needs an API key with the read and backfill scopes".to_string());
        }
    }
    for (key, value, min, max) in [
        ("timeout_ms", cfg.timeout_ms, 50, 10_000),
        ("cache_ttl_secs", cfg.cache_ttl_secs, 1, 3600),
        ("cache_entries_per_viewer", cfg.cache_entries_per_viewer, 1, 1_000_000),
        ("cache_entries_total", cfg.cache_entries_total, 1, 100_000_000),
        ("max_calls_per_request", u64::from(cfg.max_calls_per_request), 1, 100),
    ] {
        if !(min..=max).contains(&value) {
            out.push(format!("farsight.{key} = {value} is outside {min}..={max}"));
        }
    }
    if let Some(p) = class_problem(RateClassConfig::new(cfg.rate_per_sec.0, cfg.burst)) {
        out.push(format!("farsight.rate_per_sec: {p}"));
    }
    let share = cfg.requester_share.0;
    if !(share > 0.0 && share <= 1.0) {
        out.push(format!("farsight.requester_share = {share} must be above 0 and at most 1"));
    }
}

/// What startup rejects in `[farsight.fallback]` when the fallback is
/// enabled (design-v3 §5.14.17.1). With the switch off these are not
/// errors: `serve` logs them and starts.
#[must_use]
pub fn fallback_problems(cfg: &FarsightConfig, networks: &[NetworkConfig]) -> Vec<String> {
    let f = &cfg.fallback;
    let mut out = Vec::new();
    // The fallback is for a Farsight that is down, never in its place.
    if cfg.url.is_none() {
        out.push("farsight.fallback.enabled needs farsight.url: the fallback only stands in while a configured Farsight is down".to_string());
    }
    match networks.iter().find(|n| n.name == f.network) {
        None => out.push(format!("farsight.fallback.network = {:?} names no [[constellation.networks]] entry", f.network)),
        Some(n) if !n.enabled => out.push(format!("farsight.fallback.network = {:?} names an entry that is not enabled", f.network)),
        Some(n) if n.appview_url == LOCAL_APPVIEW => out.push(format!("farsight.fallback.network = {:?} names the local entry, which has no AppView to ask", f.network)),
        // A signed call would put this AppView's identity beside the
        // viewer's DID; the method needs no authentication.
        Some(n) if n.auth != NetworkAuth::Unauth => {
            out.push(format!("farsight.fallback.network = {:?} names an entry whose auth is not \"unauth\"", f.network));
        }
        Some(_) => {}
    }
    let ttl_floor = i64::try_from(cfg.cache_ttl_secs).unwrap_or(i64::MAX);
    for (key, value, min, max) in [
        ("timeout_ms", f.timeout_ms, 100, 2000),
        ("max_calls_per_request", f.max_calls_per_request, 1, 10),
        ("cache_ttl_secs", f.cache_ttl_secs, ttl_floor, 3600),
    ] {
        if !(min..=max).contains(&value) {
            out.push(format!("farsight.fallback.{key} = {value} is outside {min}..={max}"));
        }
    }
    match u32::try_from(f.burst) {
        Ok(burst) => {
            if let Some(p) = class_problem(RateClassConfig::new(f.rate_per_sec.0, burst)) {
                out.push(format!("farsight.fallback.rate_per_sec: {p}"));
            }
        }
        Err(_) => out.push(format!("farsight.fallback.burst = {} is out of range", f.burst)),
    }
    let share = f.requester_share.0;
    if !(share > 0.0 && share <= 1.0) {
        out.push(format!("farsight.fallback.requester_share = {share} must be above 0 and at most 1"));
    }
    out
}

/// What startup rejects in `[farsight.memory]` when the memory is enabled
/// (design-v3 §5.14.18.1). With the switch off these are not errors.
#[must_use]
pub fn memory_problems(cfg: &FarsightConfig) -> Vec<String> {
    let m = &cfg.memory;
    let mut out = Vec::new();
    if cfg.url.is_none() {
        out.push("farsight.memory.enabled needs farsight.url: without Farsight nothing could ever be learned".to_string());
    }
    for (key, value, min, max) in [
        ("retention_hours", m.retention_hours, 1, 720),
        ("max_per_viewer", m.max_per_viewer, 1, 10_000),
        ("max_total", m.max_total, m.max_per_viewer.max(1), 1_000_000),
    ] {
        if !(min..=max).contains(&value) {
            out.push(format!("farsight.memory.{key} = {value} is outside {min}..={max}"));
        }
    }
    out
}

/// With the memory off, empty its table (design-v3 §5.14.18.1 "When
/// disabled"): nothing reads it then, and rows left from a time it was on
/// are a member's reading activity at rest. Best effort, under a 5 s lock
/// timeout: a failure is counted and never stops a start. A function over
/// the pool, since it must run with no Farsight configured too.
pub async fn truncate_memory(pool: &sqlx::PgPool) -> bool {
    let emptied = async {
        let mut tx = pool.begin().await?;
        sqlx::query("SET LOCAL lock_timeout = '5s'").execute(&mut *tx).await?;
        sqlx::query("TRUNCATE learned_inbound_blocks").execute(&mut *tx).await?;
        tx.commit().await
    }
    .await;
    if let Err(error) = &emptied {
        metrics::counter!(METRIC_MEMORY_DROPPED, "reason" => "startup_truncate").increment(1);
        tracing::warn!(event = "farsight_memory_truncate_failed", %error, "the memory's table could not be emptied; the daily job tries again");
    }
    emptied.is_ok()
}

/// `farsight_memory_dropped_total{reason}`.
pub const METRIC_MEMORY_DROPPED: &str = "farsight_memory_dropped_total";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FileConfig;

    /// design-v3 §5.14.18.1: the memory's keys are checked only when it is
    /// on; off, its table is emptied.
    #[tokio::test]
    async fn the_memory_is_off_by_default_and_then_holds_nothing() {
        let problems = |toml: &str| memory_problems(&FileConfig::parse(toml).expect("parse").farsight);
        let on = "[farsight]\nurl = \"https://farsight.example.net\"\n[farsight.memory]\nenabled = true\n";
        assert_eq!(problems(on), Vec::<String>::new(), "the defaults are valid");
        let has = |toml: &str, needle: &str| {
            let out = problems(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        };
        has("[farsight.memory]\nenabled = true\n", "needs farsight.url");
        has(&format!("{on}retention_hours = 0\n"), "farsight.memory.retention_hours");
        has(&format!("{on}max_per_viewer = 10001\n"), "farsight.memory.max_per_viewer");
        has(&format!("{on}max_per_viewer = 500\nmax_total = 100\n"), "farsight.memory.max_total = 100 is outside 500..=1000000");
        assert!(FileConfig::parse("[farsight.memory]\nno_such_key = 1\n").is_err());

        let Some(db) = crate::test_support::fresh_db("farsight_memory_truncate").await else {
            return;
        };
        sqlx::query("INSERT INTO learned_inbound_blocks (viewer_did, other_did, source, confirmed_at) VALUES ('did:plc:v', 'did:plc:o', 'farsight', now())")
            .execute(&db.pool)
            .await
            .expect("row");
        assert!(truncate_memory(&db.pool).await);
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM learned_inbound_blocks").fetch_one(&db.pool).await.expect("count");
        assert_eq!(rows, 0);
        db.drop_db().await;
    }

    /// design-v3 §5.14.17.1: off, the fallback's keys never stop a start;
    /// on, each bad value is named.
    #[test]
    fn the_fallbacks_keys_are_checked_only_when_it_is_on() {
        let problems = |toml: &str| {
            let cfg = FileConfig::parse(toml).expect("parse");
            fallback_problems(&cfg.farsight, &cfg.constellation.networks)
        };
        let on = "[farsight]\nurl = \"https://farsight.example.net\"\n[farsight.fallback]\nenabled = true\n";
        assert_eq!(problems(on), Vec::<String>::new(), "the defaults are valid");
        let has = |toml: &str, needle: &str| {
            let out = problems(toml);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        };
        has("[farsight.fallback]\nenabled = true\n", "needs farsight.url");
        has(&format!("{on}network = \"nowhere\"\n"), "names no [[constellation.networks]] entry");
        has(&format!("{on}network = \"community\"\n"), "local entry");
        has(&format!("{on}timeout_ms = -5\n"), "farsight.fallback.timeout_ms = -5");
        has(&format!("{on}max_calls_per_request = 11\n"), "farsight.fallback.max_calls_per_request");
        // Not shorter-lived than a Farsight answer.
        has(&format!("{on}cache_ttl_secs = 10\n"), "farsight.fallback.cache_ttl_secs = 10 is outside 30..=3600");
        has(&format!("{on}burst = -1\n"), "farsight.fallback.burst");
        has(&format!("{on}requester_share = 0.0\n"), "farsight.fallback.requester_share");
        // A wrong type and an unknown key are parse errors whatever the switch.
        assert!(FileConfig::parse("[farsight.fallback]\ntimeout_ms = \"soon\"\n").is_err());
        assert!(FileConfig::parse("[farsight.fallback]\nno_such_key = 1\n").is_err());
    }

    fn check(toml: &str, token: bool) -> Vec<String> {
        let cfg = FileConfig::parse(toml).expect("parse");
        let mut out = Vec::new();
        config_problems(&cfg.farsight, token, &mut out);
        out
    }

    #[test]
    fn the_feature_is_off_by_default_and_needs_a_key_when_on() {
        assert!(check("", false).is_empty());
        assert!(check("[farsight]\nurl = \"https://farsight.example.net\"\n", true).is_empty());
        let one = |toml: &str, token: bool, needle: &str| {
            let out = check(toml, token);
            assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
        };
        one("[farsight]\nurl = \"https://farsight.example.net\"\n", false, "without FARSIGHT_TOKEN");
        one("[farsight]\nurl = \"http://farsight.example.net\"\n", true, "must be an https URL");
        one("[farsight]\nurl = \"farsight\"\n", true, "is not a URL");
        one("[farsight]\ntimeout_ms = 10\n", false, "farsight.timeout_ms");
        one("[farsight]\ncache_ttl_secs = 0\n", false, "farsight.cache_ttl_secs");
        one("[farsight]\nmax_calls_per_request = 0\n", false, "farsight.max_calls_per_request");
        one("[farsight]\nrate_per_sec = 0.0\n", false, "farsight.rate_per_sec");
        one("[farsight]\nrequester_share = 1.5\n", false, "farsight.requester_share");
    }
}
