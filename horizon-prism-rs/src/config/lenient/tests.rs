use std::collections::HashMap;

use super::*;
use crate::config::{
    ENV_CORRELATION_KEY_HEX, ENV_DATABASE_URL, ENV_PRIVATE_CURSOR_KEY_HEX, ENV_PUBLIC_HOSTNAME, ENV_REDIS_URL, ENV_SERVICE_SIGNING_KEY_HEX,
    ENV_SUPERVISED, ENV_SYSTEM_ACCOUNTS_KEY_HEX,
};

const KEY_A: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const KEY_B: &str = "0202020202020202020202020202020202020202020202020202020202020202";
const KEY_C: &str = "0303030303030303030303030303030303030303030303030303030303030303";

fn env() -> HashMap<&'static str, String> {
    HashMap::from([
        (ENV_PUBLIC_HOSTNAME, "appview.example.com".to_string()),
        (ENV_DATABASE_URL, "postgres://prism:secret@db:5432/prism".to_string()),
        (ENV_REDIS_URL, "redis://redis:6379".to_string()),
        (ENV_SERVICE_SIGNING_KEY_HEX, KEY_A.to_string()),
        (ENV_CORRELATION_KEY_HEX, KEY_B.to_string()),
        (ENV_PRIVATE_CURSOR_KEY_HEX, KEY_C.to_string()),
    ])
}

fn load_with(toml: &str, env: &HashMap<&'static str, String>) -> Result<Loaded, ConfigError> {
    load(toml, &|k: &str| env.get(k).cloned())
}

fn loads(toml: &str) -> Loaded {
    load_with(toml, &env()).expect("starts")
}

fn keys(loaded: &Loaded) -> Vec<&str> {
    loaded.set_aside.iter().map(|s| s.key.as_str()).collect()
}

fn stops(toml: &str) -> Vec<String> {
    match load_with(toml, &env()) {
        Err(ConfigError::Invalid(p)) => p,
        other => panic!("expected the start to stop, got {other:?}"),
    }
}

#[test]
fn a_clean_file_sets_nothing_aside() {
    let loaded = loads("");
    assert!(loaded.set_aside.is_empty());
    assert_eq!(loaded.cfg.file, FileConfig::default());
}

#[test]
fn an_unknown_key_or_section_is_set_aside() {
    let loaded = loads("[private_read]\nmax_chain = 100\nbogus = 1\n\n[nowhere]\nx = 1\n");
    let mut aside = keys(&loaded);
    aside.sort_unstable();
    assert_eq!(aside, ["nowhere", "private_read.bogus"]);
    // The good value beside it is kept.
    assert_eq!(loaded.cfg.file.private_read.max_chain, 100);
    assert!(!loaded.table.contains_key("nowhere"));
}

#[test]
fn a_wrong_type_falls_back_to_the_default() {
    let loaded = loads("[private_read]\nmax_chain = \"lots\"\n");
    assert_eq!(keys(&loaded), ["private_read.max_chain"]);
    assert_eq!(loaded.cfg.file.private_read.max_chain, FileConfig::default().private_read.max_chain);
}

#[test]
fn a_value_out_of_range_falls_back_to_the_default() {
    let loaded = loads("[private_read]\nmax_chain = 500\n");
    assert_eq!(keys(&loaded), ["private_read.max_chain"]);
    assert!(loaded.set_aside[0].problem.contains("private_read.max_chain must be 1 to 100"));
}

#[test]
fn a_bad_feature_url_turns_the_feature_off() {
    let loaded = loads("[private_push]\nurl = \"http://push.example.net\"\nservice_did = \"did:web:push.example.net\"\n");
    assert_eq!(keys(&loaded), ["private_push.url", "private_push.service_did"]);
    assert_eq!(loaded.cfg.file.private_push.url, None);
    assert_eq!(loaded.cfg.file.private_push.service_did, None);
}

#[test]
fn farsight_without_its_token_is_off() {
    let loaded = loads("[farsight]\nurl = \"https://farsight.example.net\"\n");
    assert_eq!(keys(&loaded), ["farsight.url"]);
    assert_eq!(loaded.cfg.file.farsight.url, None);
}

#[test]
fn one_bad_list_entry_goes_alone() {
    let toml = r#"
[[constellation.networks]]
name = "bluesky"
pds_patterns = ["*.host.bsky.network"]
appview_url = "https://api.bsky.app"
auth = "unauth"
rate_per_sec = 20
burst = 40
enabled = true

[[constellation.networks]]
name = "other"
pds_patterns = ["other.example.net"]
appview_url = "http://appview.example.net"
auth = "unauth"
rate_per_sec = 10
burst = 20
enabled = true

[[constellation.networks]]
name = "community"
pds_patterns = ["*.example.com"]
appview_url = "local"
auth = "unauth"
enabled = true
"#;
    let loaded = loads(toml);
    assert_eq!(keys(&loaded), ["constellation.networks[other]"]);
    let names: Vec<&str> = loaded.cfg.file.constellation.networks.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, ["bluesky", "community"]);
}

#[test]
fn an_entry_that_does_not_parse_goes_alone() {
    let loaded = loads("[[trends.suggested_topics]]\ntopic = \"#a\"\nlink = \"/a\"\n\n[[trends.suggested_topics]]\ntopic = \"#b\"\n");
    assert_eq!(keys(&loaded), ["trends.suggested_topics[1]"]);
    assert_eq!(loaded.cfg.file.trends.suggested_topics.len(), 1);
}

#[test]
fn a_core_setting_still_stops_the_start() {
    assert!(stops("[firehose]\nstream_key = \"\"\n").iter().any(|p| p.contains("firehose.stream_key")));
    assert!(stops("[scope]\npolicy = \"everyone\"\n").iter().any(|p| p.contains("scope.policy")));
    assert!(stops("[indexing]\npublic_scope = \"full\"\n").iter().any(|p| p.contains("indexing.public_scope")));
    assert!(stops("[http]\nbind = 3030\n").iter().any(|p| p.contains("http.bind")));
}

#[test]
fn a_section_holding_a_core_key_keeps_it() {
    // The unknown key beside the core key goes; the core key stays.
    let loaded = loads("[firehose]\nstream_key = \"events\"\nnope = true\n");
    assert_eq!(keys(&loaded), ["firehose.nope"]);
    assert_eq!(loaded.cfg.file.firehose.stream_key, "events");
}

#[test]
fn toml_that_is_not_toml_stops_the_start() {
    assert!(matches!(load_with("[http\n", &env()), Err(ConfigError::Parse(_))));
}

#[test]
fn optional_environment_problems_do_not_stop_the_start() {
    let mut e = env();
    e.remove(ENV_PRIVATE_CURSOR_KEY_HEX);
    e.remove(ENV_CORRELATION_KEY_HEX);
    e.insert(ENV_SYSTEM_ACCOUNTS_KEY_HEX, "short".to_string());
    e.insert(ENV_SUPERVISED, "yes".to_string());
    let loaded = load_with("", &e).expect("starts");
    let mut aside = keys(&loaded);
    aside.sort_unstable();
    assert_eq!(aside, [ENV_CORRELATION_KEY_HEX, ENV_PRIVATE_CURSOR_KEY_HEX, ENV_SUPERVISED, ENV_SYSTEM_ACCOUNTS_KEY_HEX]);
    assert!(loaded.cfg.env.system_accounts_key.is_none());
    assert!(!loaded.cfg.env.supervised);
    // Made for the run: two keys, not the same one.
    assert_ne!(loaded.cfg.env.private_cursor_key, loaded.cfg.env.correlation_key);
}

#[test]
fn core_environment_problems_stop_the_start() {
    for var in [ENV_PUBLIC_HOSTNAME, ENV_DATABASE_URL, ENV_REDIS_URL, ENV_SERVICE_SIGNING_KEY_HEX] {
        let mut e = env();
        e.remove(var);
        assert!(matches!(load_with("", &e), Err(ConfigError::MissingEnv(v)) if v == var), "{var}");
    }
}

#[test]
fn problems_name_their_keys() {
    let table: Table = "[lists]\nsync_max_concurrent = 9\n[backfill]\nconcurrency = 4\n".parse().expect("toml");
    let found: Vec<String> = paths_in("lists.sync_max_concurrent + lists.sync_slow_max_concurrent (10) must be at most backfill.concurrency - 2 (2)", &table)
        .into_iter()
        .map(|p| p.text)
        .collect();
    assert_eq!(found, ["lists.sync_max_concurrent", "backfill.concurrency"]);
    // A `[[table]]` named in passing is not a key the problem is about.
    let table: Table = "[farsight.fallback]\nnetwork = \"x\"\n[[constellation.networks]]\nname = \"x\"\n".parse().expect("toml");
    let found: Vec<String> = paths_in("farsight.fallback.network = \"x\" names no [[constellation.networks]] entry", &table).into_iter().map(|p| p.text).collect();
    assert_eq!(found, ["farsight.fallback.network"]);
}
