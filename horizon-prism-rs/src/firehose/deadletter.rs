//! Dead letters (design-v2 §5.3, §6.11.6).
//!
//! `XADD firehose:dead-letters MAXLEN ~ 10000 * orig_id <id> type <t>
//! relay <label> seq <s> repo <did> rev <rev> action <a>
//! path <collection/rkey> error <msg ≤ 512 B> data_prefix <first 4096 bytes>
//! data_len <n>`
//!
//! The entry is bounded at about 5 KiB. The full payload is not retained:
//! `deadletter replay` re-fetches `repo`/`path` from the PDS (§5.9).

use redis::aio::ConnectionManager;

/// Metric: dead letters written, by entry type.
pub const METRIC_DEAD_LETTERS: &str = "firehose_dead_letter_total";
/// Bytes of `error` kept.
pub const ERROR_MAX: usize = 512;
/// Bytes of `data` kept.
pub const DATA_PREFIX_MAX: usize = 4096;

/// One dead letter. Fields not known for an unparseable entry are empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeadLetter {
    pub orig_id: String,
    pub entry_type: String,
    pub relay: String,
    pub seq: String,
    pub repo: String,
    pub rev: String,
    pub action: String,
    pub path: String,
    pub error: String,
    pub data: Vec<u8>,
}

/// Truncate to at most `max` bytes on a UTF-8 boundary.
fn truncate_str(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

impl DeadLetter {
    /// The XADD field list.
    #[must_use]
    pub fn fields(&self) -> Vec<(&'static str, Vec<u8>)> {
        let prefix = &self.data[..self.data.len().min(DATA_PREFIX_MAX)];
        vec![
            ("orig_id", self.orig_id.clone().into_bytes()),
            ("type", self.entry_type.clone().into_bytes()),
            ("relay", self.relay.clone().into_bytes()),
            ("seq", self.seq.clone().into_bytes()),
            ("repo", self.repo.clone().into_bytes()),
            ("rev", self.rev.clone().into_bytes()),
            ("action", self.action.clone().into_bytes()),
            ("path", self.path.clone().into_bytes()),
            ("error", truncate_str(&self.error, ERROR_MAX).as_bytes().to_vec()),
            ("data_prefix", prefix.to_vec()),
            ("data_len", self.data.len().to_string().into_bytes()),
        ]
    }
}

/// Write dead letters with one pipelined round trip.
///
/// # Errors
///
/// Any Redis error.
pub async fn write(
    conn: &mut ConnectionManager,
    stream: &str,
    maxlen: u64,
    letters: &[DeadLetter],
) -> Result<(), redis::RedisError> {
    if letters.is_empty() {
        return Ok(());
    }
    let mut pipe = redis::pipe();
    for l in letters {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("MAXLEN").arg("~").arg(maxlen).arg("*");
        for (k, v) in l.fields() {
            cmd.arg(k).arg(v);
        }
        pipe.add_command(cmd).ignore();
    }
    pipe.query_async::<()>(conn).await?;
    for l in letters {
        metrics::counter!(METRIC_DEAD_LETTERS, "type" => l.entry_type.clone()).increment(1);
        tracing::warn!(
            event = "firehose_dead_letter",
            orig_id = %l.orig_id,
            repo = %l.repo,
            path = %l.path,
            error = %truncate_str(&l.error, ERROR_MAX),
            "op dead-lettered"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_are_bounded() {
        let l = DeadLetter {
            orig_id: "1-0".into(),
            entry_type: "commit".into(),
            error: "é".repeat(400),
            data: vec![b'x'; 10_000],
            ..DeadLetter::default()
        };
        let f = l.fields();
        let get = |k: &str| f.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone()).expect("field");
        assert!(get("error").len() <= ERROR_MAX);
        assert!(std::str::from_utf8(&get("error")).is_ok());
        assert_eq!(get("data_prefix").len(), DATA_PREFIX_MAX);
        assert_eq!(get("data_len"), b"10000");
        assert_eq!(get("repo"), b"");
        let total: usize = f.iter().map(|(k, v)| k.len() + v.len()).sum();
        assert!(total < 5 * 1024);
    }
}
