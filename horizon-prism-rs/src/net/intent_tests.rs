//! B-54's transport clauses as source checks (design-v4 §11 G-9, §6.7.8,
//! §II-8.5; R3-168, R3-179, R3-213, DS-09): every `net` user names its
//! intent, request paths take a subject-named host's transport from the
//! request's mode, and only `src/services/proxy*` uses a `service_proxy`
//! client.

use std::path::{Path, PathBuf};

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Files that are test modules as a whole: a `tests.rs`, and every file
/// under a `tests/` directory (the submodules of a `#[cfg(test)] mod
/// tests`, such as B-5's release runner, which drives `serve` over HTTP).
fn test_only(relative: &str) -> bool {
    let file = relative.rsplit('/').next().unwrap_or_default();
    file == "tests.rs"
        || file.ends_with("_tests.rs")
        || relative.contains("/tests/")
        || file == "test_support.rs"
        || relative == "services/e2e.rs"
}

/// The non-test code of every source file, as `(path under src/, code)`:
/// the text before the first inline test module, without comment lines.
fn sources() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut out = Vec::new();
    for path in files {
        let relative = path.strip_prefix(&root).expect("under src").to_string_lossy().replace('\\', "/");
        if test_only(&relative) {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("source");
        let mut code = String::new();
        let mut lines = text.lines().peekable();
        while let Some(line) = lines.next() {
            let next = lines.peek().map_or("", |l| l.trim());
            if line.trim() == "#[cfg(test)]" && next.contains("mod ") && next.ends_with('{') {
                break;
            }
            if !line.trim_start().starts_with("//") {
                code.push_str(line);
                code.push('\n');
            }
        }
        out.push((relative, code));
    }
    assert!(out.len() > 50, "the source tree was not found");
    out
}

/// The files of `sources` whose code contains any of `needles`.
fn naming<'a>(sources: &'a [(String, String)], needles: &[&str]) -> Vec<&'a str> {
    sources
        .iter()
        .filter(|(_, code)| needles.iter().any(|n| code.contains(n)))
        .map(|(file, _)| file.as_str())
        .collect()
}

fn is_proxy(file: &str) -> bool {
    file.starts_with("services/proxy")
}

/// DS-09 (§6.7.8): `src/services/proxy*` uses only its service's
/// `service_proxy` client, and no other code path uses one.
#[test]
fn only_the_proxy_uses_a_service_proxy_client() {
    let sources = sources();
    let users: Vec<&str> = naming(&sources, &["ServiceProxyClient", "service_proxy::"])
        .into_iter()
        .filter(|f| *f != "net/service_proxy.rs")
        .collect();
    assert!(users.iter().all(|f| is_proxy(f)), "a service_proxy client outside src/services/proxy*: {users:?}");
    assert!(users.contains(&"services/proxy.rs"), "the proxy was not found: {users:?}");
    let other = ["NetClient", "Transports", "Intent::", "HickoryLookup", "IdentityResolver", "for_request("];
    let proxy: Vec<(String, String)> = sources.into_iter().filter(|(f, _)| is_proxy(f)).collect();
    assert!(naming(&proxy, &other).is_empty(), "the proxy names another transport: {:?}", naming(&proxy, &other));
}

/// §II-8.5: `net` builds every client and resolver. Outside it, nothing
/// makes a client or a resolver of its own (the healthcheck's loopback
/// probe of this process excepted).
#[test]
fn every_client_and_resolver_is_built_by_net() {
    let sources = sources();
    let builders = ["NetClient::with_lookup(", "HickoryLookup::", "TokioResolver", "reqwest::Client::builder(", "reqwest::Client::new("];
    let outside: Vec<&str> = naming(&sources, &builders).into_iter().filter(|f| !f.starts_with("net/") && *f != "serve/mod.rs").collect();
    assert!(outside.is_empty(), "clients or resolvers built outside net: {outside:?}");
    let serve = &sources.iter().find(|(f, _)| f == "serve/mod.rs").expect("serve").1;
    assert_eq!(serve.matches("reqwest::Client::builder(").count(), 1, "only the healthcheck builds a client in serve");
    for needle in ["NetClient::with_lookup(", "HickoryLookup::", "TokioResolver"] {
        assert!(!serve.contains(needle), "serve builds {needle} itself");
    }
    assert!(serve.contains("Transports::new("), "serve builds the transports");
}

/// §11 G-9, R3-213: the public request paths never pin the internal or the
/// `viewer_private` intent; they read the transport for a subject-named
/// host from the request (`for_request`), so a viewer-private fetch never
/// goes through the public transport, and private-context hydration goes
/// through the internal one.
#[test]
fn request_paths_take_the_transport_from_the_request() {
    let sources = sources();
    let request_path = |f: &str| ["hydration/", "xrpc/", "constellation/", "threadlens/"].iter().any(|d| f.starts_with(d));
    let mut pinned = Vec::new();
    let mut unselected = Vec::new();
    // `pools.rs` maps a request to its intent; `member_keys.rs` is the
    // background refresh of members' keys (checked below).
    let exempt = ["xrpc/pools.rs", "xrpc/member_keys.rs"];
    for (file, code) in sources.iter().filter(|(f, _)| request_path(f)) {
        if !exempt.contains(&file.as_str()) && ["Intent::Internal", "Intent::ViewerPrivate", ".internal()", ".get(Intent"].iter().any(|n| code.contains(n)) {
            pinned.push(file.as_str());
        }
        // A `Transports` held on a request path is only read per request.
        if code.matches("nets.").count() != code.matches("nets.for_request(").count() {
            unselected.push(file.as_str());
        }
    }
    assert!(pinned.is_empty(), "request paths pinning an intent: {pinned:?}");
    assert!(unselected.is_empty(), "request paths reading a transport not chosen by the request: {unselected:?}");
    // The identity lookups of a request go through the request's view of
    // the resolver, never the resolver as it was built.
    let routing = &sources.iter().find(|(f, _)| f == "constellation/routing.rs").expect("routing").1;
    assert_eq!(
        routing.matches("self.resolver.").count(),
        routing.matches("self.resolver.for_request(req)").count(),
        "a RequestLookup call bypasses for_request"
    );
    for direct in ["self.resolve_", "self.verify_handle(", "self.fetch_document("] {
        assert!(!routing.contains(direct), "DidLookup for IdentityResolver calls {direct} without for_request");
    }
    let upstream = &sources.iter().find(|(f, _)| f == "hydration/upstream.rs").expect("upstream").1;
    assert!(upstream.contains("nets.for_request(req)"), "PdsLister lists through the request's transport");
    // The one place that maps a request to its intent.
    let pools = &sources.iter().find(|(f, _)| f == "xrpc/pools.rs").expect("pools").1;
    assert!(pools.contains("impl crate::net::FetchMode for Requester"));
}

/// §11 G-9: background work and the private tier pin the internal intent
/// in their own constructors, and the auth middleware and auto-enrol pin
/// the public one, so a caller cannot hand them another.
#[test]
fn background_and_auth_users_pin_their_intent() {
    let sources = sources();
    let code = |file: &str| &sources.iter().find(|(f, _)| f == file).unwrap_or_else(|| panic!("{file}")).1;
    for file in ["firehose/backfill.rs", "subscriptions/pds.rs", "identity/refresher.rs", "xrpc/member_keys.rs"] {
        assert!(code(file).contains("with_intent(") && code(file).contains("Intent::Internal"), "{file} does not pin the internal intent");
    }
    assert!(code("sync/head_prober.rs").contains("nets.internal().client()"), "the prober's head fetches are internal");
    for file in ["identity/fetch_cell.rs", "aggregations/enroll.rs"] {
        assert!(code(file).contains("with_intent(") && code(file).contains("Intent::Public"), "{file} does not pin the public intent");
    }
    let farsight = code("farsight/client.rs");
    assert!(farsight.contains("self.public.get(") && farsight.contains("self.internal.post_json(") && farsight.contains("self.internal.get("));
}
