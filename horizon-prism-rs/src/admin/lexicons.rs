//! The custom lexicons, compiled in from `lexicons/` at the repository root
//! by `build.rs`, so they always match the running version. The Instance
//! page lists them and `/admin/lexicons/<path>` serves each one's JSON.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};

use super::AdminState;
use super::auth::{Need, require};

include!(concat!(env!("OUT_DIR"), "/lexicons.rs"));

/// One compiled-in lexicon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lexicon {
    /// Its path under `lexicons/`, as served.
    pub path: &'static str,
    /// Its NSID, from the path.
    pub nsid: String,
}

/// Every lexicon, in path order.
#[must_use]
pub fn all() -> Vec<Lexicon> {
    FILES
        .iter()
        .map(|(path, _)| Lexicon { path, nsid: path.trim_end_matches(".json").replace('/', ".") })
        .collect()
}

/// The text of the lexicon at `path`.
#[must_use]
pub fn get(path: &str) -> Option<&'static str> {
    FILES.iter().find(|(p, _)| *p == path).map(|(_, text)| *text)
}

/// `GET /admin/lexicons/{*path}`: one lexicon's JSON, for a signed-in
/// operator; any other path is a 404.
pub async fn serve(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Path(path): Path<String>) -> Response {
    if let Err(response) = require(&state, &headers, peer, Need::Page, None).await {
        return response;
    }
    match get(&path) {
        Some(text) => ([(CONTENT_TYPE, HeaderValue::from_static("application/json"))], text).into_response(),
        None => super::pages::not_found().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The compiled-in set is every `.json` file under `lexicons/`, by path,
    /// and each one parses and names the NSID its path gives.
    #[test]
    fn the_compiled_set_is_the_folder() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("lexicons");
        assert!(root.is_dir(), "the lexicons/ folder must sit beside the crate for this test");
        fn walk(dir: &std::path::Path, base: &std::path::Path, out: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).expect("dir").flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, base, out);
                } else if p.extension().is_some_and(|e| e == "json") {
                    out.push(p.strip_prefix(base).expect("under").to_string_lossy().replace('\\', "/"));
                }
            }
        }
        let mut on_disk = Vec::new();
        walk(&root, &root, &mut on_disk);
        on_disk.sort();
        let compiled: Vec<&str> = FILES.iter().map(|(p, _)| *p).collect();
        assert!(!compiled.is_empty());
        assert_eq!(compiled, on_disk);
        for lex in all() {
            let v: serde_json::Value = serde_json::from_str(get(lex.path).expect("text")).expect(lex.path);
            assert_eq!(v["id"].as_str(), Some(lex.nsid.as_str()), "{}", lex.path);
        }
    }
}
