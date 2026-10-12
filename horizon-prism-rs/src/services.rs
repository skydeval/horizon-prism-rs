//! `[services.*]`: deferred external services (design-v3 §6.7, proposed
//! locked decision 35). Each built-in service (contact, age assurance, push)
//! and each operator-defined one is `disabled`, `proxy` or `serve`.
//!
//! This build implements `disabled` only: the compiled service table, the
//! whole of §6.7.2.3's validation, and the disjointness check. `proxy` and
//! `serve` are rejected at startup (rules 2, 2a and 9) until the proxy
//! handler lands (§II-15 item 11a).

use std::collections::BTreeMap;

use crate::config::{FeaturesConfig, FileConfig, ServiceConfig, ServiceState};
use crate::honest_501::{self, Owner, Verb};

/// A built-in service: its NSIDs, verbs and messages are compiled into
/// `honest_501::DEFERRALS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Builtin {
    pub name: &'static str,
    /// The `aud` fragment a client names for this surface on Bluesky
    /// (§6.7.2.2); the default of `aud_fragments`.
    pub canonical_fragment: &'static str,
    /// Whether `state = "proxy"` is accepted (rule 2a).
    pub proxyable: bool,
}

pub const BUILTIN: &[Builtin] = &[
    Builtin {
        name: "contact",
        canonical_fragment: "bsky_appview",
        proxyable: true,
    },
    Builtin {
        name: "age_assurance",
        canonical_fragment: "bsky_appview",
        proxyable: true,
    },
    Builtin {
        name: "push",
        canonical_fragment: "bsky_notif",
        proxyable: false,
    },
];

/// Services this build can `serve`, and feature flags it can turn on
/// (§6.6, R3-84). Both are empty in v0.1.
pub const IMPLEMENTED_SERVICES: &[&str] = &[];
pub const IMPLEMENTED_FEATURES: &[&str] = &[];
/// Rule 9: whether this build has the proxy handler. Item 11a turns it on.
pub const PROXY_BUILT: bool = false;
/// `aud_fragments` default for a custom service (§6.7.2.4).
pub const CUSTOM_FRAGMENT: &str = "bsky_appview";
/// A custom service may not claim an NSID in these namespaces (§6.7.2.4).
pub const RESERVED_NAMESPACES: &[&str] = &[
    "com.atproto.",
    "app.bsky.",
    "chat.bsky.",
    "tools.ozone.",
    "tools.kryphocron.",
    "app.nearhorizon.",
];
const MAX_CUSTOM_METHODS: usize = 64;

#[must_use]
pub fn builtin(name: &str) -> Option<&'static Builtin> {
    BUILTIN.iter().find(|b| b.name == name)
}

/// One row of the compiled service table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Service {
    pub name: String,
    pub builtin: bool,
    pub state: ServiceState,
    /// Every NSID the service claims, with its verb. For `contact` this
    /// includes the pinned `sendNotification`.
    pub methods: Vec<(String, Verb)>,
}

/// The compiled service table: the three built-in services, whether or not
/// the file names them, then the custom ones in name order. Call it on a
/// validated config.
#[must_use]
pub fn table(cfg: &FileConfig) -> Vec<Service> {
    let mut out: Vec<Service> = BUILTIN
        .iter()
        .map(|b| Service {
            name: b.name.to_string(),
            builtin: true,
            state: cfg.services.get(b.name).map(|s| s.state).unwrap_or_default(),
            methods: honest_501::DEFERRALS
                .iter()
                .filter(|d| d.owner == Owner::Service(b.name))
                .map(|d| (d.nsid.to_string(), d.verb))
                .collect(),
        })
        .collect();
    for (name, s) in &cfg.services {
        if builtin(name).is_some() {
            continue;
        }
        let methods = s
            .methods
            .iter()
            .flatten()
            .filter_map(|(nsid, verb)| Some((nsid.clone(), Verb::parse(verb)?)))
            .collect();
        out.push(Service {
            name: name.clone(),
            builtin: false,
            state: s.state,
            methods,
        });
    }
    out
}

/// Every problem in `[features]`, `[honest_501]` and `[services.*]`
/// (§6.6, §6.7.2.3, §6.7.2.4). `public_hostname` is `APP_PUBLIC_HOSTNAME`.
pub fn problems(cfg: &FileConfig, public_hostname: &str, out: &mut Vec<String>) {
    features(&cfg.features, out);
    for nsid in cfg.honest_501.messages.keys() {
        if honest_501::find(nsid).is_none() {
            out.push(format!("honest_501.messages: {nsid:?} is not a deferred endpoint of this AppView"));
        }
    }
    // NSID to the block that claims it. The built-in NSIDs cannot collide
    // with a custom one, since a custom NSID may not be in their namespaces.
    let mut claimed: BTreeMap<&str, &str> = BTreeMap::new();
    for (name, s) in &cfg.services {
        let known = builtin(name);
        state(name, s, known, out);
        proxy_keys(name, s, public_hostname, out);
        match known {
            Some(_) => {
                if s.methods.is_some() {
                    out.push(format!("services.{name}.methods: a built-in service's methods are compiled in"));
                }
            }
            None => custom(name, s, &mut claimed, out),
        }
    }
}

fn features(f: &FeaturesConfig, out: &mut Vec<String>) {
    for (key, set) in [
        ("push", f.push.is_some()),
        ("age_assurance", f.age_assurance.is_some()),
        ("contact", f.contact.is_some()),
    ] {
        if set {
            out.push(format!("features.{key} is removed: set services.{key}.state instead"));
        }
    }
    for feature in ["feed_generators", "embed_external_view", "verification"] {
        if f.enabled(feature) && !IMPLEMENTED_FEATURES.contains(&feature) {
            out.push(format!("features.{feature} = true: this build does not implement it"));
        }
    }
}

/// Rules 2, 2a and 9, and the custom-service state rule.
fn state(name: &str, s: &ServiceConfig, known: Option<&Builtin>, out: &mut Vec<String>) {
    match s.state {
        ServiceState::Disabled => {}
        ServiceState::Serve => {
            if known.is_none() {
                out.push(format!("services.{name}.state = \"serve\": a custom service can only be disabled or proxied"));
            } else if !IMPLEMENTED_SERVICES.contains(&name) {
                out.push(format!("services.{name}.state = \"serve\": this build does not implement it"));
            }
        }
        ServiceState::Proxy => {
            if known.is_some_and(|b| !b.proxyable) {
                out.push(format!(
                    "services.{name}.state = \"proxy\": {name} cannot be proxied in this version (deferral D-2)"
                ));
            } else if !PROXY_BUILT {
                out.push(format!("services.{name}.state = \"proxy\": this build does not support proxying yet"));
            }
        }
    }
}

/// Rules 3 to 6.
fn proxy_keys(name: &str, s: &ServiceConfig, public_hostname: &str, out: &mut Vec<String>) {
    let set: [(&str, bool); 10] = [
        ("proxy_url", s.proxy_url.is_some()),
        ("aud_fragments", s.aud_fragments.is_some()),
        ("timeout_ms", s.timeout_ms.is_some()),
        ("body_read_timeout_ms", s.body_read_timeout_ms.is_some()),
        ("max_request_bytes", s.max_request_bytes.is_some()),
        ("max_response_bytes", s.max_response_bytes.is_some()),
        ("max_in_flight", s.max_in_flight.is_some()),
        ("rate_per_sec", s.rate_per_sec.is_some()),
        ("burst", s.burst.is_some()),
        ("allow_private_network", s.allow_private_network.is_some()),
    ];
    if s.state != ServiceState::Proxy {
        for (key, present) in set {
            if present {
                out.push(format!(
                    "services.{name}.{key} is accepted only with state = \"proxy\" (state is {:?})",
                    s.state.as_str()
                ));
            }
        }
        return;
    }
    match &s.proxy_url {
        None => out.push(format!("services.{name}.proxy_url is required with state = \"proxy\"")),
        Some(url) => {
            if let Err(e) = proxy_url(url, s.allow_private_network == Some(true), public_hostname) {
                out.push(format!("services.{name}.proxy_url: {e}"));
            }
        }
    }
    for fragment in s.aud_fragments.iter().flatten() {
        if !fragment_ok(fragment) {
            out.push(format!(
                "services.{name}.aud_fragments: {fragment:?} is not a DID URL fragment (give it without the #)"
            ));
        }
    }
    const KIB: u64 = 1024;
    const MAX_BYTES: u64 = 16 * 1024 * KIB;
    for (key, value, min, max) in [
        ("timeout_ms", s.timeout_ms, 100, 60_000),
        ("body_read_timeout_ms", s.body_read_timeout_ms, 100, 60_000),
        ("max_request_bytes", s.max_request_bytes, KIB, MAX_BYTES),
        ("max_response_bytes", s.max_response_bytes, KIB, MAX_BYTES),
        ("max_in_flight", s.max_in_flight, 1, 10_000),
    ] {
        if let Some(v) = value
            && !(min..=max).contains(&v)
        {
            out.push(format!("services.{name}.{key} = {v} is outside {min}..={max}"));
        }
    }
    for (key, value) in [("rate_per_sec", s.rate_per_sec), ("burst", s.burst)] {
        if value == Some(0) {
            out.push(format!("services.{name}.{key} must be greater than 0"));
        }
    }
}

/// Rule 4: `https` (or `http` with `allow_private_network`), a host and an
/// optional port and nothing else, and not this AppView itself.
fn proxy_url(value: &str, allow_private_network: bool, public_hostname: &str) -> Result<(), String> {
    let url = url::Url::parse(value).map_err(|e| format!("{value:?} is not a URL: {e}"))?;
    match url.scheme() {
        "https" => {}
        "http" if allow_private_network => {}
        "http" => return Err(format!("{value:?}: http is accepted only with allow_private_network = true")),
        other => return Err(format!("{value:?}: scheme {other:?} is not https")),
    }
    let Some(host) = url.host_str() else {
        return Err(format!("{value:?} has no host"));
    };
    if !url.username().is_empty() || url.password().is_some() {
        return Err(format!("{value:?} must not carry credentials"));
    }
    if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
        return Err(format!("{value:?} must be a scheme, host and optional port only"));
    }
    if host.eq_ignore_ascii_case(public_hostname) {
        return Err(format!("{value:?} is this AppView's own hostname"));
    }
    Ok(())
}

/// Rule 5: a non-empty DID URL fragment, without the `#`.
fn fragment_ok(fragment: &str) -> bool {
    !fragment.is_empty()
        && fragment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-._~:@!$&'()*+,;=/?%".contains(c))
}

fn custom_name_ok(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= 32
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// §6.7.2.4.
fn custom<'a>(name: &'a str, s: &'a ServiceConfig, claimed: &mut BTreeMap<&'a str, &'a str>, out: &mut Vec<String>) {
    if !custom_name_ok(name) {
        out.push(format!("services.{name}: a custom service name matches [a-z][a-z0-9_]{{0,31}}"));
    }
    let Some(methods) = &s.methods else {
        out.push(format!("services.{name}.methods is required on a custom service"));
        return;
    };
    if methods.is_empty() || methods.len() > MAX_CUSTOM_METHODS {
        out.push(format!("services.{name}.methods must hold 1 to {MAX_CUSTOM_METHODS} entries"));
    }
    for (nsid, verb) in methods {
        if kryphocron_lexicons::Nsid::new(nsid).is_err() {
            out.push(format!("services.{name}.methods: {nsid:?} is not an NSID"));
            continue;
        }
        if Verb::parse(verb).is_none() {
            out.push(format!("services.{name}.methods[{nsid}]: {verb:?} is not \"GET\" or \"POST\""));
        }
        if let Some(reserved) = RESERVED_NAMESPACES.iter().find(|ns| nsid.starts_with(**ns)) {
            out.push(format!(
                "services.{name}.methods: {nsid:?} is in the reserved namespace {reserved}*"
            ));
        }
        if let Some(other) = claimed.insert(nsid.as_str(), name) {
            out.push(format!("services.{name}.methods: {nsid:?} is also claimed by services.{other}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST: &str = "appview.example.com";

    fn check(toml: &str) -> Vec<String> {
        let cfg = FileConfig::parse(toml).expect("parse");
        let mut out = Vec::new();
        problems(&cfg, HOST, &mut out);
        out
    }

    fn one(toml: &str, needle: &str) {
        let out = check(toml);
        assert!(out.iter().any(|p| p.contains(needle)), "{needle:?} not in {out:?}");
    }

    #[test]
    fn defaults_and_disabled_blocks_are_clean() {
        assert!(check("").is_empty());
        assert!(check("[services.contact]\nstate = \"disabled\"\n[services.push]\n[services.age_assurance]\n").is_empty());
        assert!(
            check("[services.eu_notices]\nstate = \"disabled\"\nmethods = { \"eu.example.region.getNotices\" = \"GET\" }\n")
                .is_empty()
        );
    }

    #[test]
    fn serve_and_proxy_are_rejected_in_this_build() {
        one("[services.contact]\nstate = \"serve\"\n", "does not implement");
        one("[services.push]\nstate = \"serve\"\n", "does not implement");
        // Rule 2a: push names D-2. Rule 9: the others are not built yet.
        one("[services.push]\nstate = \"proxy\"\nproxy_url = \"https://p.example\"\n", "D-2");
        one(
            "[services.contact]\nstate = \"proxy\"\nproxy_url = \"https://c.example\"\n",
            "does not support proxying yet",
        );
        one(
            "[services.eu]\nstate = \"proxy\"\nproxy_url = \"https://e.example\"\nmethods = { \"eu.example.a.b\" = \"GET\" }\n",
            "does not support proxying yet",
        );
        one(
            "[services.eu]\nstate = \"serve\"\nmethods = { \"eu.example.a.b\" = \"GET\" }\n",
            "can only be disabled or proxied",
        );
    }

    #[test]
    fn proxy_only_keys_need_the_proxy_state() {
        one("[services.contact]\nproxy_url = \"https://c.example\"\n", "accepted only with state = \"proxy\"");
        one("[services.contact]\ntimeout_ms = 500\n", "services.contact.timeout_ms");
        one("[services.contact]\nallow_private_network = true\n", "allow_private_network");
        one("[services.contact]\nstate = \"proxy\"\n", "proxy_url is required");
    }

    #[test]
    fn proxy_url_shape() {
        let p = |url: &str, private: bool| proxy_url(url, private, HOST);
        assert!(p("https://contact.example.net", false).is_ok());
        assert!(p("https://contact.example.net:8443/", false).is_ok());
        assert!(p("http://contact:8080", true).is_ok());
        assert!(p("http://contact:8080", false).is_err());
        assert!(p("ftp://contact.example.net", true).is_err());
        assert!(p("https://user:pw@contact.example.net", false).is_err());
        assert!(p("https://contact.example.net/xrpc", false).is_err());
        assert!(p("https://contact.example.net?x=1", false).is_err());
        assert!(p("https://contact.example.net#f", false).is_err());
        assert!(p("https://APPVIEW.example.com", false).is_err());
        assert!(p("not a url", false).is_err());
    }

    #[test]
    fn numeric_ranges_and_fragments() {
        let base = "[services.contact]\nstate = \"proxy\"\nproxy_url = \"https://c.example\"\n";
        one(&format!("{base}timeout_ms = 50\n"), "timeout_ms = 50");
        one(&format!("{base}body_read_timeout_ms = 60001\n"), "body_read_timeout_ms");
        one(&format!("{base}max_request_bytes = 1023\n"), "max_request_bytes");
        one(&format!("{base}max_response_bytes = 16777217\n"), "max_response_bytes");
        one(&format!("{base}max_in_flight = 0\n"), "max_in_flight");
        one(&format!("{base}rate_per_sec = 0\n"), "rate_per_sec must be greater than 0");
        one(&format!("{base}aud_fragments = [\"#bsky_appview\"]\n"), "not a DID URL fragment");
        one(&format!("{base}aud_fragments = [\"\"]\n"), "not a DID URL fragment");
        assert!(fragment_ok("bsky_appview") && fragment_ok("x_test"));
    }

    #[test]
    fn builtin_methods_and_custom_rules() {
        one(
            "[services.contact]\nmethods = { \"app.bsky.contact.getMatches\" = \"GET\" }\n",
            "compiled in",
        );
        one("[services.eu]\nstate = \"disabled\"\n", "methods is required");
        one("[services.eu]\nmethods = {}\n", "1 to 64");
        one("[services.Eu]\nmethods = { \"eu.example.a.b\" = \"GET\" }\n", "custom service name");
        one("[services.eu]\nmethods = { \"not an nsid\" = \"GET\" }\n", "is not an NSID");
        one("[services.eu]\nmethods = { \"eu.example.a.b\" = \"PUT\" }\n", "\"GET\" or \"POST\"");
        for reserved in [
            "com.atproto.server.createSession",
            "app.bsky.feed.getPosts",
            "chat.bsky.convo.getLog",
            "tools.ozone.team.listMembers",
            "tools.kryphocron.feed.getPost",
            "app.nearhorizon.private.getPosts",
        ] {
            one(
                &format!("[services.eu]\nmethods = {{ \"{reserved}\" = \"GET\" }}\n"),
                "reserved namespace",
            );
        }
        one(
            "[services.eu]\nmethods = { \"eu.example.a.b\" = \"GET\" }\n[services.uk]\nmethods = { \"eu.example.a.b\" = \"GET\" }\n",
            "also claimed by services.eu",
        );
    }

    #[test]
    fn features_and_message_keys() {
        one("[features]\npush = false\n", "features.push is removed: set services.push.state");
        one("[features]\ncontact = true\n", "services.contact.state");
        one("[features]\nage_assurance = true\n", "services.age_assurance.state");
        one("[features]\nfeed_generators = true\n", "does not implement");
        one("[features]\nverification = true\n", "does not implement");
        assert!(check("[features]\nfeed_generators = false\nembed_external_view = false\nverification = false\n").is_empty());
        one(
            "[honest_501.messages]\n\"eu.example.region.getNotices\" = \"x\"\n",
            "not a deferred endpoint",
        );
        assert!(check("[honest_501.messages]\n\"app.bsky.contact.sendNotification\" = \"x\"\n").is_empty());
    }

    #[test]
    fn compiled_table() {
        let cfg = FileConfig::parse(
            "[services.eu_notices]\nmethods = { \"eu.example.region.getNotices\" = \"GET\", \"eu.example.region.ackNotice\" = \"POST\" }\n",
        )
        .expect("parse");
        let t = table(&cfg);
        let names: Vec<&str> = t.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["contact", "age_assurance", "push", "eu_notices"]);
        assert_eq!(t.iter().map(|s| s.methods.len()).collect::<Vec<_>>(), [8, 5, 2, 2]);
        assert!(t.iter().all(|s| s.state == ServiceState::Disabled));
        assert!(t[..3].iter().all(|s| s.builtin) && !t[3].builtin);
        // Disjointness over the whole table: no NSID is claimed twice.
        let mut all: Vec<&str> = t.iter().flat_map(|s| s.methods.iter().map(|(n, _)| n.as_str())).collect();
        let total = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total);
    }
}
