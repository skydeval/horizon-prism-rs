//! The B-5 bench's in-process head source (private-threads design §4).
//! Compiled only under the `bench-heads` feature, which no deploy build
//! names: the release `NetClient` refuses loopback, so a network stub cannot
//! be reached, and B-5's stale-head fixtures need a source that answers.
//!
//! - `latest_rev` sleeps [`BENCH_LATENCY`], under the 500 ms read timeout,
//!   then answers the DID's entry from `bench.heads_file`: its rev (which
//!   the fixture builder writes as the DID's `applied_rev`, so an answered
//!   head is never ahead and never diverges), or an error for `fail`.
//! - A DID not in the file is an error, which only denies.
//! - Serve refuses to start under the feature without a readable file, and
//!   spawns no background prober ([`super::SPAWNS_PROBER`]).

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;

use super::HeadSource;
use crate::config::FileConfig;

/// How long each answer takes: under `sync.head_fetch_timeout_ms`'s
/// default of 500 ms, so a round of 16 at `buffered(8)` is two waves.
pub const BENCH_LATENCY: Duration = Duration::from_millis(450);

/// Heads by DID from `bench.heads_file`: one `<did> <rev>` or `<did> fail`
/// per line; blank lines and lines starting with `#` are skipped.
#[derive(Debug, Clone, Default)]
pub struct BenchHeadSource {
    /// `None`: the DID's head fails.
    heads: HashMap<String, Option<String>>,
}

impl BenchHeadSource {
    /// Parse a heads file's text.
    ///
    /// # Errors
    ///
    /// A line that is not `<did> <rev>` or `<did> fail`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut heads = HashMap::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let (Some(did), Some(answer), None) = (parts.next(), parts.next(), parts.next()) else {
                return Err(format!("line {}: expected `<did> <rev>` or `<did> fail`", n + 1));
            };
            if !did.starts_with("did:") {
                return Err(format!("line {}: {did:?} is not a DID", n + 1));
            }
            heads.insert(did.to_string(), (answer != "fail").then(|| answer.to_string()));
        }
        Ok(Self { heads })
    }

    /// The source `cfg` names: `bench.heads_file`, required and readable.
    ///
    /// # Errors
    ///
    /// The key is unset, or the file cannot be read or parsed.
    pub fn from_config(cfg: &FileConfig) -> Result<Self, String> {
        let path = cfg
            .bench
            .heads_file
            .as_deref()
            .ok_or_else(|| "bench.heads_file is required in a build with the bench-heads feature".to_string())?;
        Self::load(path)
    }

    /// Read and parse `path`.
    ///
    /// # Errors
    ///
    /// The file cannot be read or parsed.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("bench.heads_file {}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| format!("bench.heads_file {}: {e}", path.display()))
    }
}

#[async_trait]
impl HeadSource for BenchHeadSource {
    async fn latest_rev(&self, _pds: &str, did: &str, _timeout: Duration) -> Result<String, String> {
        tokio::time::sleep(BENCH_LATENCY).await;
        match self.heads.get(did) {
            Some(Some(rev)) => Ok(rev.clone()),
            Some(None) => Err(format!("bench head for {did} fails")),
            None => Err(format!("no bench head for {did}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    /// T-57: a listed DID's rev after 450 ms, a `fail` DID's error after
    /// 450 ms, an unlisted DID's error; and serve's requirement of a
    /// readable `bench.heads_file`, with no background prober.
    #[tokio::test]
    async fn bench_heads_answer_after_the_bench_latency() {
        let source = BenchHeadSource::parse("# heads\ndid:plc:a 3kaaaaaaaaaa2\n\ndid:plc:b fail\n").expect("parses");
        let start = Instant::now();
        assert_eq!(source.latest_rev("https://bench-1.invalid", "did:plc:a", Duration::from_millis(500)).await, Ok("3kaaaaaaaaaa2".to_string()));
        assert!(start.elapsed() >= BENCH_LATENCY);
        let start = Instant::now();
        assert!(source.latest_rev("https://bench-2.invalid", "did:plc:b", Duration::from_millis(500)).await.is_err());
        assert!(start.elapsed() >= BENCH_LATENCY);
        assert!(source.latest_rev("https://bench-3.invalid", "did:plc:c", Duration::from_millis(500)).await.is_err());
        assert!(BenchHeadSource::parse("did:plc:a").is_err());
        assert!(BenchHeadSource::parse("bob 3k").is_err());

        let unset = FileConfig::default();
        assert!(BenchHeadSource::from_config(&unset).unwrap_err().contains("bench.heads_file is required"));
        let missing = FileConfig::parse("[bench]\nheads_file = \"/nonexistent/hprs-bench-heads\"\n").expect("parses");
        assert!(BenchHeadSource::from_config(&missing).is_err());
        let path = std::env::temp_dir().join(format!("hprs-bench-heads-{}", rand::random::<u64>()));
        std::fs::write(&path, "did:plc:a 3kaaaaaaaaaa2\n").expect("write");
        let readable = FileConfig::parse(&format!("[bench]\nheads_file = {:?}\n", path.display().to_string())).expect("parses");
        assert!(BenchHeadSource::from_config(&readable).is_ok());
        std::fs::remove_file(&path).expect("remove");
        assert!(!super::super::SPAWNS_PROBER, "no background prober under bench-heads");
    }
}
