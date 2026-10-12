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
pub mod lookup;

pub use answer::{ChunkAnswer, Coverage, PairAnswer, Relation};
pub use client::{CallError, FarsightClient};
pub use lookup::{Farsight, Lookup};

use crate::config::{FarsightConfig, RateClassConfig};
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FileConfig;

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
