//! PDS matching (design-v2 §8.2).
//!
//! - **Host:** the lowercased host of the DID document's `#atproto_pds`
//!   endpoint, port stripped.
//! - **Patterns:** `*.suffix` matches any host with at least one label
//!   before `.suffix`; a bare host matches exactly.
//! - **Evaluation:** networks in file order, first match wins.

use crate::config::NetworkConfig;

/// §8.2 pattern: `*.suffix` needs at least one label before `.suffix`; a bare
/// host matches exactly.
#[must_use]
pub fn host_matches(host: &str, pattern: &str) -> bool {
    let host = host.to_ascii_lowercase();
    let pattern = pattern.to_ascii_lowercase();
    match pattern.strip_prefix("*.") {
        Some(suffix) => host
            .strip_suffix(suffix)
            .and_then(|rest| rest.strip_suffix('.'))
            .is_some_and(|label| !label.is_empty()),
        None => host == pattern,
    }
}

/// The lowercased host of a PDS endpoint, port stripped (§8.2).
#[must_use]
pub fn pds_host(endpoint: &str) -> Option<String> {
    url::Url::parse(endpoint).ok()?.host_str().map(str::to_ascii_lowercase)
}

/// The first network (file order) with a pattern matching `host`.
#[must_use]
pub fn first_match<'a>(networks: &'a [NetworkConfig], host: &str) -> Option<&'a NetworkConfig> {
    networks.iter().find(|n| n.pds_patterns.iter().any(|p| host_matches(host, p)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConstellationConfig, NetworkAuth};

    #[test]
    fn suffix_and_exact_patterns() {
        assert!(host_matches("morel.us-east.host.bsky.network", "*.host.bsky.network"));
        assert!(!host_matches("host.bsky.network", "*.host.bsky.network"), "needs a label before the suffix");
        assert!(!host_matches("evilhost.bsky.network", "*.host.bsky.network"));
        assert!(host_matches("BSKY.social", "bsky.social"));
        assert!(!host_matches("x.bsky.social", "bsky.social"), "a bare host is exact");
        assert_eq!(pds_host("https://Morel.us-east.host.bsky.network:443/"), Some("morel.us-east.host.bsky.network".into()));
        assert_eq!(pds_host("not a url"), None);
    }

    #[test]
    fn first_match_wins_in_file_order() {
        let mut cfg = ConstellationConfig::with_example();
        let names = |host: &str, cfg: &ConstellationConfig| first_match(&cfg.networks, host).map(|n| n.name.clone());
        assert_eq!(names("pds.community-a.example", &cfg), Some("community-a".into()));
        assert_eq!(names("pds.example.org", &cfg), None);
        // An earlier entry shadows a later, more specific one.
        cfg.networks.insert(
            0,
            NetworkConfig {
                name: "catchall".into(),
                pds_patterns: vec!["*.community-a.example".into()],
                appview_url: "https://appview.catchall.example".into(),
                auth: NetworkAuth::Unauth,
                appview_did: String::new(),
                rate_per_sec: Some(1),
                burst: Some(1),
                enabled: true,
                background_rate_per_sec: crate::config::background_rate_per_sec(),
                background_burst: crate::config::background_burst(),
            },
        );
        assert_eq!(names("pds.community-a.example", &cfg), Some("catchall".into()));
        assert_eq!(names("community-a.example", &cfg), Some("community-a".into()));
    }
}
