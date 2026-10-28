//! A check that no blocked word is in the repository's text. The words are
//! held only as SHA-256 hashes in `blocked.sha256`; `tools/name-guard.py`
//! (the git hooks in `.githooks/`) applies the same rule to commits.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

const WINDOW: usize = 8;

fn blocked() -> HashSet<String> {
    include_str!("blocked.sha256")
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

fn digest(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

/// The candidates of `text`, as the Python guard forms them.
fn candidates(text: &str) -> Vec<String> {
    let lower = text.to_lowercase();
    let mut out = Vec::new();
    let is_word = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    for w in lower.split(|c: char| !is_word(c)).filter(|w| !w.is_empty()) {
        out.push(w.to_string());
        if w.len() >= WINDOW {
            for i in 0..=w.len() - WINDOW {
                out.push(w[i..i + WINDOW].to_string());
            }
        }
    }
    // Dotted runs, as the regex `[a-z0-9]+(\.[a-z0-9]+)+` finds them.
    for run in lower.split(|c: char| !is_word(c) && c != '.') {
        let mut seq = Vec::new();
        for part in run.split('.') {
            if part.is_empty() {
                push_dotted(&seq, &mut out);
                seq.clear();
            } else {
                seq.push(part);
            }
        }
        push_dotted(&seq, &mut out);
    }
    out
}

fn push_dotted(parts: &[&str], out: &mut Vec<String>) {
    for i in 0..parts.len() {
        for j in i + 2..=parts.len() {
            out.push(parts[i..j].join("."));
        }
    }
}

fn hit(text: &str, blocked: &HashSet<String>) -> bool {
    candidates(text).iter().any(|c| blocked.contains(&digest(c)))
}

/// The folders scanned: the repository root when this crate sits in it,
/// else the crate alone.
fn root() -> PathBuf {
    let krate = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match krate.parent() {
        Some(up) if up.join("CHANGELOG.md").is_file() => up.to_path_buf(),
        _ => krate,
    }
}

const SKIP: &[&str] = &[".git", "target", ".claude", ".chainlink", "node_modules"];

fn walk(dir: &Path, root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let path = e.path();
        let name = e.file_name().to_string_lossy().to_string();
        let rel = path.strip_prefix(root).unwrap_or(&path).to_string_lossy().replace('\\', "/");
        if path.is_dir() {
            if SKIP.contains(&name.as_str()) || rel == "docs/internal" {
                continue;
            }
            walk(&path, root, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn the_rule_matches_words_windows_and_hosts() {
    // A stand-in list, so the test names no blocked word.
    let list: HashSet<String> = ["skyhorse", "relay.example"].iter().map(|w| digest(w)).collect();
    assert!(hit("name = \"SkyHorse\"", &list));
    assert!(hit("let skyhorse_url = 1;", &list));
    assert!(hit("fn theSkyhorseConfig()", &list), "an 8-character window inside a longer word");
    assert!(hit("wss://relay.example/xrpc", &list));
    assert!(hit("wss://a.relay.example.", &list));
    assert!(!hit("sky horse", &list));
    assert!(!hit("relay-example", &list));
}

#[test]
fn no_blocked_word_in_the_repository() {
    let blocked = blocked();
    let root = root();
    let mut files = Vec::new();
    walk(&root, &root, &mut files);
    let mut found = Vec::new();
    for path in files {
        let rel = path.strip_prefix(&root).unwrap_or(&path).to_string_lossy().to_string();
        if hit(&rel, &blocked) {
            found.push(format!("{rel} (name)"));
        }
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        for (n, line) in text.lines().enumerate() {
            if hit(line, &blocked) {
                found.push(format!("{rel}:{}", n + 1));
            }
        }
    }
    assert!(found.is_empty(), "blocked word at: {found:?}");
}
