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

/// The lines of `text` before its first inline test module, without
/// comment lines.
fn code_lines(text: &str) -> Vec<String> {
    let mut code = Vec::new();
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        let next = lines.peek().map_or("", |l| l.trim());
        if line.trim() == "#[cfg(test)]" && next.contains("mod ") && next.ends_with('{') {
            break;
        }
        if !line.trim_start().starts_with("//") {
            code.push(line.to_string());
        }
    }
    code
}

/// The non-test code of every source file, as `(path under src/, lines)`.
fn source_lines() -> Vec<(String, Vec<String>)> {
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
        out.push((relative, code_lines(&text)));
    }
    assert!(out.len() > 50, "the source tree was not found");
    out
}

/// The non-test code of every source file, as `(path under src/, code)`:
/// the text before the first inline test module, without comment lines.
fn sources() -> Vec<(String, String)> {
    source_lines()
        .into_iter()
        .map(|(file, lines)| {
            let mut code = String::new();
            for line in lines {
                code.push_str(&line);
                code.push('\n');
            }
            (file, code)
        })
        .collect()
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

// ---- T10: the stand-in feature's source checks (stand-in design §3.5, §3.6) ----

/// The module the feature's `mod` line gates as a whole.
const FEATURE_MODULE: &str = "net/standin.rs";
/// The other files that may name the feature, on gated lines only.
const FEATURE_FILES: [&str; 7] = [
    "net/mod.rs",
    "net/client.rs",
    "config/sections.rs",
    "config/mod.rs",
    "config/validate.rs",
    "serve/mod.rs",
    "lib.rs",
];
/// The marker, spelled whole only in `net/standin.rs`.
const MARKER_LITERAL: &str = concat!("HPRS_STANDIN", "_HOSTS_BUILD");
/// The feature as a `cfg` predicate.
const FEATURE_PREDICATE: &str = concat!("feature = \"stand", "in-hosts\"");

/// A token of the feature: lowercased, it starts with `standin` and not
/// with `standing`.
fn is_feature_token(token: &str) -> bool {
    let lower = token.to_ascii_lowercase();
    lower.starts_with(concat!("stand", "in")) && !lower.starts_with("standing")
}

/// The identifier tokens of `line`.
fn tokens(line: &str) -> impl Iterator<Item = &str> + '_ {
    line.split(|c: char| !(c.is_alphanumeric() || c == '_')).filter(|t| !t.is_empty())
}

/// `line` with a trailing `//` comment cut (outside string literals), and
/// the same with every string literal's contents blanked.
fn split_code(line: &str) -> (String, String) {
    let mut code = String::new();
    let mut blank = String::new();
    let mut in_string = false;
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            code.push(c);
            if c == '\\' && i + 1 < chars.len() {
                code.push(chars[i + 1]);
                blank.push_str("  ");
                i += 2;
                continue;
            }
            if c == '"' {
                in_string = false;
                blank.push('"');
            } else {
                blank.push(' ');
            }
            i += 1;
            continue;
        }
        match c {
            '/' if chars.get(i + 1) == Some(&'/') => break,
            '"' => {
                in_string = true;
                code.push(c);
                blank.push(c);
            }
            // A char literal, `'x'` or `'\x'`, is copied whole; a lifetime
            // is an ordinary character.
            '\'' => {
                let len = match (chars.get(i + 1), chars.get(i + 2), chars.get(i + 3)) {
                    (Some('\\'), Some(_), Some('\'')) => 4,
                    (Some(x), Some('\''), _) if *x != '\\' => 3,
                    _ => 1,
                };
                for k in 0..len {
                    code.push(chars[i + k]);
                    blank.push(if k == 0 || k == len - 1 { '\'' } else { ' ' });
                }
                i += len;
                continue;
            }
            _ => {
                code.push(c);
                blank.push(c);
            }
        }
        i += 1;
    }
    (code, blank)
}

/// The leading attributes of a line of code, and the text after them.
fn leading_attributes(code: &str) -> (Vec<String>, String) {
    let mut rest = code.trim_start();
    let mut attributes = Vec::new();
    while rest.starts_with("#[") {
        let mut depth = 0;
        let mut in_string = false;
        let mut escaped = false;
        let mut end = None;
        for (i, c) in rest.char_indices() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    in_string = false;
                }
                continue;
            }
            match c {
                '"' => in_string = true,
                '[' => depth += 1,
                ']' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end) = end else {
            break;
        };
        attributes.push(rest[..=end].to_string());
        rest = rest[end + 1..].trim_start();
    }
    (attributes, rest.to_string())
}

/// A gating attribute: exactly `#[cfg(feature = "standin-hosts")]`, or a
/// `#[cfg(all(…))]` with that predicate among its arguments. `not(…)` and
/// `any(…)` forms are not gating.
fn is_gating(attribute: &str) -> bool {
    let Some(inner) = attribute.strip_prefix("#[cfg(").and_then(|a| a.strip_suffix(")]")) else {
        return false;
    };
    let inner = inner.trim();
    if inner == FEATURE_PREDICATE {
        return true;
    }
    let Some(args) = inner.strip_prefix("all(").and_then(|a| a.strip_suffix(')')) else {
        return false;
    };
    let mut depth = 0;
    let mut start = 0;
    let mut parts = Vec::new();
    for (i, c) in args.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(args[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(args[start..].trim());
    parts.contains(&FEATURE_PREDICATE)
}

fn indentation(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

/// The lines of a feature file (its code lines) that name the feature
/// without being gated (stand-in design §3.5): a line is gated when it
/// carries a gating attribute, when the attributes just above it include
/// one, or when it lies in a gated block, which opens at a gated item whose
/// first line ends in `{` and closes at the first later line at the same
/// indentation that is exactly `}`, `};` or `},`.
fn ungated_feature_lines(lines: &[String]) -> Vec<String> {
    let prepared: Vec<(String, String)> = lines.iter().map(|l| split_code(l)).collect();
    let n = prepared.len();
    let blank = |i: usize| prepared[i].0.trim().is_empty();
    let pure_attribute = |i: usize| {
        let (attributes, rest) = leading_attributes(&prepared[i].0);
        !attributes.is_empty() && rest.is_empty()
    };
    let carries_gating = |i: usize| leading_attributes(&prepared[i].0).0.iter().any(|a| is_gating(a));
    let mut gated = vec![false; n];
    let mut problems = Vec::new();
    for (i, g) in gated.iter_mut().enumerate() {
        if carries_gating(i) {
            *g = true;
            continue;
        }
        let mut j = i;
        while let Some(p) = (0..j).rev().find(|&k| !blank(k)) {
            if !pure_attribute(p) {
                break;
            }
            if carries_gating(p) {
                *g = true;
                break;
            }
            j = p;
        }
    }
    for k in 0..n {
        if !carries_gating(k) {
            continue;
        }
        let item = if pure_attribute(k) {
            match (k + 1..n).find(|&m| !blank(m) && !pure_attribute(m)) {
                Some(m) => m,
                None => continue,
            }
        } else {
            k
        };
        if !prepared[item].1.trim_end().ends_with('{') {
            continue;
        }
        let indent = indentation(&prepared[item].0);
        let end = (item + 1..n).find(|&m| indentation(&prepared[m].0) == indent && matches!(prepared[m].0.trim(), "}" | "};" | "},"));
        match end {
            Some(end) => gated[item..=end].iter_mut().for_each(|g| *g = true),
            None => problems.push(format!("a gated block that never closes: {}", lines[item])),
        }
    }
    problems.extend((0..n).filter(|&i| !gated[i] && tokens(&prepared[i].0).any(is_feature_token)).map(|i| lines[i].clone()));
    problems
}

/// Stand-in design §3.5: the feature is named only in its own module and,
/// on gated lines, in the files the design lists; its marker is spelled
/// whole only in its module.
#[test]
fn the_standin_feature_is_named_only_on_gated_lines() {
    let files = source_lines();
    let mut outside = Vec::new();
    let mut ungated = Vec::new();
    let mut marker = Vec::new();
    for (file, lines) in &files {
        let code: Vec<String> = lines.iter().map(|l| split_code(l).0).collect();
        if file != FEATURE_MODULE && code.iter().any(|l| l.contains(MARKER_LITERAL)) {
            marker.push(file.as_str());
        }
        if file == FEATURE_MODULE {
            continue;
        }
        if FEATURE_FILES.contains(&file.as_str()) {
            ungated.extend(ungated_feature_lines(lines).into_iter().map(|l| format!("{file}: {l}")));
        } else if code.iter().any(|l| tokens(l).any(is_feature_token)) {
            outside.push(file.as_str());
        }
    }
    assert!(outside.is_empty(), "the feature named outside its files: {outside:?}");
    assert!(ungated.is_empty(), "the feature named on ungated lines: {ungated:#?}");
    assert!(marker.is_empty(), "the marker spelled outside {FEATURE_MODULE}: {marker:?}");
    let module = &files.iter().find(|(f, _)| f == FEATURE_MODULE).expect("the feature module").1;
    assert!(module.iter().any(|l| l.contains(MARKER_LITERAL)), "the module holds the marker");
    for file in FEATURE_FILES {
        let lines = &files.iter().find(|(f, _)| f == file).unwrap_or_else(|| panic!("{file}")).1;
        assert!(lines.iter().any(|l| tokens(l).any(is_feature_token)), "{file} no longer names the feature: update the list");
    }
}

/// T10's fixtures for the gating rule.
#[test]
fn the_gating_rule_reads_its_fixtures() {
    let check = |text: &str| ungated_feature_lines(&code_lines(&text.replace("FEATURE", FEATURE_PREDICATE)));
    for pass in [
        "#[cfg(FEATURE)]\npub mod standin;\n",
        "#[cfg(FEATURE)]\n\npub mod standin;\n",
        "fn f() {\n    let standing = 1;\n    let outstanding = standing;\n}\n",
        "#[cfg(FEATURE)]\n#[derive(Debug)]\n#[serde(default)]\npub struct StandinX {\n    a: u8,\n\n    b: StandinY,\n}\n",
        "fn f() {\n    #[cfg(FEATURE)]\n    if x {\n        standin_a();\n    } else {\n        standin_b();\n    }\n}\n",
        "    #[cfg(FEATURE)]\n    {\n        let s = \"}\";\n        standin_a();\n    }\n",
        "    #[cfg(FEATURE)] standin: Vec::new(),\n",
        "#[cfg(all(FEATURE, not(debug_assertions)))]\ncompile_error!(\"standin-hosts is test-only\");\n",
        "#[cfg(all(test, FEATURE))]\nmod standin_tests;\n",
        "let x = 1; // standin, in a trailing comment\n// standin, on a comment line\n",
    ] {
        assert_eq!(check(pass), Vec::<String>::new(), "{pass}");
    }
    for fail in [
        "pub mod standin;\n",
        "let STANDIN_X = 1;\n",
        "let f = \"standin-hosts\";\n",
        "#[cfg(FEATURE)]\nfn f(\n    x: StandinThing,\n) {\n}\n",
        "#[cfg(not(FEATURE))]\nfn standin_off() {}\n",
        "#[cfg(any(FEATURE, feature = \"bench-heads\"))]\nfn standin_any() {}\n",
        "let u = \"http://172.29.77.21:8081\"; let standin = 1;\n",
        "#[cfg(FEATURE)]\nlet a = 1;\nlet standin = 2;\n",
        "#[cfg(FEATURE)]\nfn f() {\n}\nfn standin_after() {}\n",
        "#[cfg(FEATURE)]\nfn f() {\n    standin_a();\n",
    ] {
        assert!(!check(fail).is_empty(), "{fail}");
    }
}

/// Stand-in design §3.6: `docker/Dockerfile` greps the binary that ships
/// for the marker and fails closed. A crate-only tree skips this, unless
/// `HPRS_REQUIRE_REPO_TEXT=1`.
#[test]
fn the_dockerfile_refuses_a_standin_binary() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../docker/Dockerfile");
    let Ok(text) = std::fs::read_to_string(&path) else {
        assert!(
            std::env::var("HPRS_REQUIRE_REPO_TEXT").as_deref() != Ok("1"),
            "{} not found and HPRS_REQUIRE_REPO_TEXT=1",
            path.display()
        );
        eprintln!("{} not found; skipping the Dockerfile check", path.display());
        return;
    };
    let lines: Vec<&str> = text.lines().map(str::trim).collect();
    for needed in [
        r#"ENV RUSTFLAGS="" CARGO_ENCODED_RUSTFLAGS="" CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=false"#,
        "RUN test ! -e .cargo/config.toml && test ! -e .cargo/config",
        "RUN cargo build --release --locked --bin horizon-prism-rs",
    ] {
        assert!(lines.contains(&needed), "the Dockerfile lacks {needed:?}");
    }
    let quoted = format!("grep -a -q '{MARKER_LITERAL}' ");
    let grep = lines.iter().find(|l| l.contains(&quoted)).expect("the Dockerfile greps for the marker");
    let grep_path = grep.split(&quoted).nth(1).and_then(|r| r.split_whitespace().next()).expect("the grep's path");
    let copy = lines
        .iter()
        .find(|l| l.starts_with("COPY --from=builder ") && l.ends_with(" /app/horizon-prism-rs"))
        .expect("the runtime stage copies the binary");
    let source = copy.split_whitespace().nth(2).expect("the copy's source");
    assert_eq!(grep_path, source, "the grep reads the file that ships");
    assert!(text.contains(r#"if [ "$rc" -eq 0 ]; then echo 'refusing"#), "a match fails the build");
    assert!(text.contains(r#"if [ "$rc" -ne 1 ]; then"#), "a grep error fails the build");
    #[cfg(feature = "standin-hosts")]
    {
        assert_eq!(MARKER_LITERAL, crate::net::standin::MARKER);
    }
}
