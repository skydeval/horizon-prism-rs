//! Stand-in design T9: a `standin-hosts` build's `serve` says what it is on
//! stderr before its config loads, and refuses to start without
//! `[standin]`. Ignored: run by hand against the `standin` profile's binary
//! (`cargo build --profile standin --features standin-hosts`), found
//! through `HPRS_STANDIN_BIN`, else `target/standin/horizon-prism-rs`:
//! `cargo test --features standin-hosts standin_serve -- --ignored`.
//!
//! Every run is refused before Postgres, Redis or the metrics port is
//! touched, so the database and Redis URLs point at nothing.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::net::standin::MARKER;

fn standin_bin() -> PathBuf {
    std::env::var("HPRS_STANDIN_BIN").map_or_else(
        |_| Path::new(env!("CARGO_MANIFEST_DIR")).join("target/standin/horizon-prism-rs"),
        PathBuf::from,
    )
}

/// Run `serve` on `config`, returning its exit code, stderr and stdout.
fn serve_once(name: &str, config: &str, log_level: Option<&str>) -> (Option<i32>, String, String) {
    let dir = std::env::temp_dir().join(format!("hprs-standin-t9-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("app.toml");
    std::fs::write(&path, config).expect("config");
    let mut cmd = Command::new(standin_bin());
    cmd.arg("serve")
        .env_clear()
        .env("APP_CONFIG_PATH", &path)
        .env("APP_PUBLIC_HOSTNAME", "appview.example.com")
        .env("APP_DATABASE_URL", "postgres://nobody:nothing@127.0.0.1:1/none")
        .env("APP_REDIS_URL", "redis://127.0.0.1:1")
        .env("APP_SERVICE_SIGNING_KEY_HEX", "01".repeat(32))
        .env("APP_CORRELATION_KEY_HEX", "02".repeat(32))
        .env("APP_PRIVATE_CURSOR_KEY_HEX", "03".repeat(32))
        .env("APP_DATA_DIR", &dir);
    if let Some(level) = log_level {
        cmd.env("APP_LOG_LEVEL", level);
    }
    let out = cmd.output().expect("the standin binary runs (build it first)");
    let _ = std::fs::remove_dir_all(&dir);
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or_default()
}

#[test]
#[ignore = "needs the standin profile's binary"]
fn standin_serve_says_what_it_is_and_refuses_without_hosts() {
    // Run 1: no [standin]: rule 1.
    let (code, stderr, stdout) = serve_once("missing", "", None);
    assert!(first_line(&stderr).starts_with(MARKER), "stderr: {stderr}");
    assert_eq!(code, Some(78), "stderr: {stderr}");
    assert!(stderr.contains("a standin-hosts build needs at least one [[standin.hosts]] entry"), "stderr: {stderr}");
    assert!(stdout.contains("standin_hosts_build") && stdout.contains(MARKER), "the error-level log line: {stdout}");

    // Run 2: a file that does not parse.
    let (code, stderr, _) = serve_once("parse", "[standin\n", None);
    assert!(first_line(&stderr).starts_with(MARKER), "stderr: {stderr}");
    assert_eq!(code, Some(78), "stderr: {stderr}");
    assert!(stderr.contains("invalid config file"), "stderr: {stderr}");

    // Run 3: logging off; stderr still says it.
    let (code, stderr, stdout) = serve_once("off", "", Some("off"));
    assert!(first_line(&stderr).starts_with(MARKER), "stderr: {stderr}");
    assert_eq!(code, Some(78), "stderr: {stderr}");
    assert!(!stdout.contains("standin_hosts_build"), "logging is off: {stdout}");
}
