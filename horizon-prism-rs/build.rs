//! Compiles the custom lexicons in (the admin Instance page serves them at
//! `/admin/lexicons/<path>`). They live at the repository root, beside this
//! crate, in `lexicons/`; the image copies them to `/lexicons` before the
//! build so the same relative path resolves there. A build without the
//! folder still succeeds, with no lexicons and a warning.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Not following links, so a link loop cannot recurse forever.
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "json") {
            out.push(path);
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let root = manifest.join("..").join("lexicons");
    let mut files = Vec::new();
    if root.is_dir() {
        println!("cargo:rerun-if-changed={}", root.to_str().expect("the lexicons path must be UTF-8"));
        walk(&root, &mut files);
    } else {
        println!("cargo:warning=no lexicons/ folder beside the crate: the admin Instance page lists no lexicons");
    }
    files.sort();
    let mut code = String::from("/// Every custom lexicon: its path under `lexicons/` and its text.\npub static FILES: &[(&str, &str)] = &[\n");
    for file in &files {
        let rel = file.strip_prefix(&root).expect("under the root");
        let rel: Vec<&str> = rel.components().map(|c| c.as_os_str().to_str().expect("lexicon paths must be UTF-8")).collect();
        let abs = file.canonicalize().unwrap_or_else(|_| file.clone());
        let abs = abs.to_str().expect("lexicon paths must be UTF-8");
        writeln!(code, "    ({:?}, include_str!({abs:?})),", rel.join("/")).expect("write");
    }
    code.push_str("];\n");
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("lexicons.rs");
    std::fs::write(out, code).expect("write lexicons.rs");
}
