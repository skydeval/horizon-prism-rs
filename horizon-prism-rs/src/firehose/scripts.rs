//! The consumer's Lua scripts (design-v2 §6.11.4), loaded at startup.
//!
//! `redis::Script` invokes by `EVALSHA` and falls back to `EVAL` on
//! `NOSCRIPT`, so a Redis restart that flushes the script cache is harmless.

use std::sync::LazyLock;

use redis::aio::ConnectionManager;
use redis::{Script, Value};

use super::stream::{StreamEntry, parse_read_reply};

static READ_CHECKED: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("scripts/read_checked.lua")));
static REANCHOR: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("scripts/reanchor.lua")));
static GROUP_CREATE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("scripts/group_create.lua")));

/// Trim evidence returned by `read_checked`, read atomically at detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapEvidence {
    pub first_entry_id: String,
    pub entries_added: String,
    pub length: String,
    /// `L`, the tail a tail re-anchor uses (firehose throughput design §2.2).
    pub last_generated_id: String,
}

impl GapEvidence {
    /// `entries-added` as a number (`A`).
    #[must_use]
    pub fn added(&self) -> u64 {
        self.entries_added.parse().unwrap_or(0)
    }

    /// `length` as a number (`N`).
    #[must_use]
    pub fn stream_length(&self) -> u64 {
        self.length.parse().unwrap_or(0)
    }
}

/// The outcome of one `read_checked` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckedRead {
    /// The group does not exist (the stream was deleted or recreated).
    NoGroup,
    Gap(GapEvidence),
    /// The stream's `entries-added` at the read, and what it read.
    Entries { added: u64, entries: Vec<StreamEntry> },
}

/// The anchor a re-anchor sets (firehose throughput design §2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReanchorMode {
    /// Just before the oldest surviving entry.
    Oldest,
    /// At `L`, the tail as it was when the gap was detected.
    Tail,
}

impl ReanchorMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Oldest => "oldest",
            Self::Tail => "tail",
        }
    }
}

/// What `reanchor` did, with the first batch it read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reanchored {
    pub id: String,
    pub entries_read: String,
    /// The mode it used: a tail falls back to oldest when an entry after
    /// `L` was trimmed.
    pub mode: ReanchorMode,
    pub entries: Vec<StreamEntry>,
}

/// A script failure or an unexpected reply shape.
#[derive(Debug, thiserror::Error)]
pub enum ScriptError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("unexpected script reply: {0}")]
    Shape(String),
}

/// Load all three scripts into the server's script cache.
///
/// # Errors
///
/// Any Redis error.
pub async fn load_all(conn: &mut ConnectionManager) -> Result<(), redis::RedisError> {
    for script in [&*READ_CHECKED, &*REANCHOR, &*GROUP_CREATE] {
        script.prepare_invoke().load_async(conn).await?;
    }
    Ok(())
}

/// Create the group if absent. Returns `true` when it was created.
///
/// # Errors
///
/// Any Redis error.
pub async fn group_create(conn: &mut ConnectionManager, stream: &str, group: &str) -> Result<bool, ScriptError> {
    let created: i64 = GROUP_CREATE.key(stream).arg(group).invoke_async(conn).await?;
    Ok(created == 1)
}

/// The atomic trim check plus a non-blocking read of new entries.
///
/// # Errors
///
/// A Redis error or an unexpected reply.
pub async fn read_checked(
    conn: &mut ConnectionManager,
    stream: &str,
    group: &str,
    consumer: &str,
    count: u32,
) -> Result<CheckedRead, ScriptError> {
    let reply: Value = READ_CHECKED
        .key(stream)
        .arg(group)
        .arg(consumer)
        .arg(count)
        .invoke_async(conn)
        .await?;
    parse_checked(reply)
}

fn parse_checked(reply: Value) -> Result<CheckedRead, ScriptError> {
    let Value::Array(items) = reply else {
        return Err(ScriptError::Shape(format!("{reply:?}")));
    };
    let mut it = items.into_iter();
    let tag = it.next().and_then(|v| string(&v));
    match tag.as_deref() {
        Some("nogroup") => Ok(CheckedRead::NoGroup),
        Some("gap") => {
            let rest: Vec<String> = it.filter_map(|v| string(&v)).collect();
            let [first_entry_id, entries_added, length, last_generated_id] = <[String; 4]>::try_from(rest)
                .map_err(|r| ScriptError::Shape(format!("gap evidence {r:?}")))?;
            Ok(CheckedRead::Gap(GapEvidence {
                first_entry_id,
                entries_added,
                length,
                last_generated_id,
            }))
        }
        Some("ok") => {
            let added = it
                .next()
                .as_ref()
                .and_then(string)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| ScriptError::Shape("ok without entries-added".into()))?;
            let entries = match it.next() {
                None | Some(Value::Nil) => Vec::new(),
                Some(v) => parse_read_reply(v).map_err(ScriptError::Shape)?,
            };
            Ok(CheckedRead::Entries { added, entries })
        }
        other => Err(ScriptError::Shape(format!("tag {other:?}"))),
    }
}

/// Re-anchor the group after a gap transaction, in `mode`, and read the
/// first batch of up to `count` entries (firehose throughput design §2.4).
///
/// # Errors
///
/// A Redis error or an unexpected reply.
pub async fn reanchor(
    conn: &mut ConnectionManager,
    stream: &str,
    group: &str,
    consumer: &str,
    count: u32,
    mode: ReanchorMode,
    evidence: &GapEvidence,
) -> Result<Reanchored, ScriptError> {
    let reply: Value = REANCHOR
        .key(stream)
        .arg(group)
        .arg(consumer)
        .arg(count)
        .arg(mode.as_str())
        .arg(&evidence.last_generated_id)
        .arg(&evidence.entries_added)
        .invoke_async(conn)
        .await?;
    parse_reanchored(reply)
}

fn parse_reanchored(reply: Value) -> Result<Reanchored, ScriptError> {
    let Value::Array(items) = reply else {
        return Err(ScriptError::Shape(format!("reanchor {reply:?}")));
    };
    let mut it = items.into_iter();
    let mut text = || it.next().as_ref().and_then(string);
    let (Some(id), Some(entries_read), Some(mode)) = (text(), text(), text()) else {
        return Err(ScriptError::Shape("reanchor reply".into()));
    };
    let mode = match mode.as_str() {
        "tail" => ReanchorMode::Tail,
        "oldest" => ReanchorMode::Oldest,
        other => return Err(ScriptError::Shape(format!("reanchor mode {other:?}"))),
    };
    let entries = match it.next() {
        None | Some(Value::Nil) => Vec::new(),
        Some(v) => parse_read_reply(v).map_err(ScriptError::Shape)?,
    };
    Ok(Reanchored { id, entries_read, mode, entries })
}

pub(crate) fn string(v: &Value) -> Option<String> {
    match v {
        Value::BulkString(b) => String::from_utf8(b.clone()).ok(),
        Value::SimpleString(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn parses_script_replies() {
        assert_eq!(parse_checked(Value::Array(vec![bulk("nogroup")])).expect("ok"), CheckedRead::NoGroup);
        assert_eq!(
            parse_checked(Value::Array(vec![bulk("gap"), bulk("5-0"), bulk("100"), bulk("10"), bulk("9-3")])).expect("ok"),
            CheckedRead::Gap(GapEvidence {
                first_entry_id: "5-0".into(),
                entries_added: "100".into(),
                length: "10".into(),
                last_generated_id: "9-3".into(),
            })
        );
        assert_eq!(
            parse_checked(Value::Array(vec![bulk("ok"), bulk("12")])).expect("ok"),
            CheckedRead::Entries { added: 12, entries: Vec::new() }
        );
        let reply = Value::Array(vec![Value::Array(vec![
            bulk("firehose:events"),
            Value::Array(vec![Value::Array(vec![
                bulk("1-0"),
                Value::Array(vec![bulk("type"), bulk("commit"), bulk("data"), bulk("{}"), bulk("seq"), bulk("7")]),
            ])]),
        ])]);
        match parse_checked(Value::Array(vec![bulk("ok"), bulk("13"), reply.clone()])).expect("ok") {
            CheckedRead::Entries { added, entries: e } => {
                assert_eq!(added, 13);
                assert_eq!(e.len(), 1);
                assert_eq!(e[0].id, "1-0");
                assert_eq!(e[0].field("type"), Some("commit"));
            }
            other => panic!("{other:?}"),
        }
        assert!(parse_checked(Value::Array(vec![bulk("ok")])).is_err(), "an ok reply carries entries-added");
        assert!(parse_checked(Value::Int(1)).is_err());

        let re = parse_reanchored(Value::Array(vec![bulk("4-0"), bulk("7"), bulk("tail"), reply])).expect("ok");
        assert_eq!((re.id.as_str(), re.entries_read.as_str(), re.mode, re.entries.len()), ("4-0", "7", ReanchorMode::Tail, 1));
        let re = parse_reanchored(Value::Array(vec![bulk("4-0"), bulk("7"), bulk("oldest"), Value::Nil])).expect("ok");
        assert_eq!((re.mode, re.entries.len()), (ReanchorMode::Oldest, 0));
    }

    /// Against a real Redis 7 when `HPRS_TEST_REDIS_URL` is set.
    mod live {
        use super::*;

        const GROUP: &str = "g";
        const CONSUMER: &str = "app";

        async fn conn() -> Option<ConnectionManager> {
            let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
                eprintln!("HPRS_TEST_REDIS_URL unset; skipping Redis-backed script test");
                return None;
            };
            let client = redis::Client::open(url).expect("redis url");
            Some(ConnectionManager::new(client).await.expect("connect redis"))
        }

        /// Add `n` entries; the last one's id.
        async fn xadd(c: &mut ConnectionManager, stream: &str, maxlen: u64, n: usize) -> String {
            let mut last = String::new();
            for i in 0..n {
                last = redis::cmd("XADD")
                    .arg(stream)
                    .arg("MAXLEN")
                    .arg(maxlen)
                    .arg("*")
                    .arg("type")
                    .arg("commit")
                    .arg("data")
                    .arg(i.to_string())
                    .query_async(c)
                    .await
                    .expect("xadd");
            }
            last
        }

        async fn read(c: &mut ConnectionManager, stream: &str) -> CheckedRead {
            read_checked(c, stream, GROUP, CONSUMER, 500).await.expect("read_checked")
        }

        fn entries(r: CheckedRead) -> Vec<StreamEntry> {
            match r {
                CheckedRead::Entries { entries, .. } => entries,
                other => panic!("expected entries, got {other:?}"),
            }
        }

        async fn stream_field(c: &mut ConnectionManager, stream: &str, name: &str) -> Value {
            let info: Value = redis::cmd("XINFO").arg("STREAM").arg(stream).query_async(c).await.expect("xinfo");
            let Value::Array(flat) = info else {
                panic!("XINFO STREAM {info:?}");
            };
            flat.chunks(2)
                .find(|kv| string(&kv[0]).as_deref() == Some(name))
                .map(|kv| kv[1].clone())
                .unwrap_or_else(|| panic!("no {name}"))
        }

        async fn group_field(c: &mut ConnectionManager, stream: &str, name: &str) -> Option<String> {
            let groups: Value = redis::cmd("XINFO").arg("GROUPS").arg(stream).query_async(c).await.expect("xinfo");
            let Value::Array(list) = groups else {
                panic!("XINFO GROUPS {groups:?}");
            };
            let Some(Value::Array(flat)) = list.into_iter().next() else {
                panic!("no group");
            };
            flat.chunks(2).find(|kv| string(&kv[0]).as_deref() == Some(name)).and_then(|kv| string(&kv[1]))
        }

        /// A group that has read three entries, then five more added with
        /// `MAXLEN 3`: two unread entries trimmed. Returns the evidence.
        async fn gap(c: &mut ConnectionManager, stream: &str) -> GapEvidence {
            load_all(c).await.expect("load");
            assert!(group_create(c, stream, GROUP).await.expect("create"));
            xadd(c, stream, 3, 3).await;
            assert_eq!(entries(read(c, stream).await).len(), 3);
            xadd(c, stream, 3, 5).await;
            match read(c, stream).await {
                CheckedRead::Gap(e) => e,
                other => panic!("expected gap, got {other:?}"),
            }
        }

        async fn del(c: &mut ConnectionManager, stream: &str) {
            let _: () = redis::cmd("DEL").arg(stream).query_async(c).await.expect("del");
        }

        #[tokio::test]
        async fn gap_detection_benign_trim_and_reanchor() {
            let Some(mut c) = conn().await else {
                return;
            };
            let stream = format!("test:stream:{}", rand::random::<u32>());
            load_all(&mut c).await.expect("load");
            assert_eq!(read(&mut c, &stream).await, CheckedRead::NoGroup);

            // Create on a missing stream, then again: only the first creates.
            assert!(group_create(&mut c, &stream, GROUP).await.expect("create"));
            assert!(!group_create(&mut c, &stream, GROUP).await.expect("create"));
            assert!(entries(read(&mut c, &stream).await).is_empty());

            // Read three entries.
            xadd(&mut c, &stream, 3, 3).await;
            assert_eq!(entries(read(&mut c, &stream).await).len(), 3);

            // Benign: three more trim only what was read (lag 3 ≤ length 3).
            xadd(&mut c, &stream, 3, 3).await;
            let got = entries(read(&mut c, &stream).await);
            assert_eq!(got.len(), 3, "the next unread entry is the head");

            // Gap: five more with MAXLEN 3 trim two unread entries.
            xadd(&mut c, &stream, 3, 5).await;
            let evidence = match read(&mut c, &stream).await {
                CheckedRead::Gap(e) => e,
                other => panic!("expected gap, got {other:?}"),
            };
            assert_eq!(evidence.length, "3");
            assert_eq!(evidence.entries_added, "11");
            // The gap reply read nothing and left the evidence in place.
            assert!(matches!(read(&mut c, &stream).await, CheckedRead::Gap(_)));

            // Re-anchor: the survivors come back with it and lag is exact
            // again.
            let re = reanchor(&mut c, &stream, GROUP, CONSUMER, 500, ReanchorMode::Oldest, &evidence)
                .await
                .expect("reanchor");
            assert_eq!((re.entries_read.as_str(), re.mode), ("8", ReanchorMode::Oldest));
            assert_eq!(re.entries.len(), 3);
            assert_eq!(re.entries[0].id, evidence.first_entry_id);
            assert!(entries(read(&mut c, &stream).await).is_empty());
            del(&mut c, &stream).await;
        }

        /// Firehose throughput §2.6.
        #[tokio::test]
        async fn gap_evidence_carries_last_generated_id() {
            let Some(mut c) = conn().await else {
                return;
            };
            let stream = format!("test:stream:{}", rand::random::<u32>());
            load_all(&mut c).await.expect("load");
            assert!(group_create(&mut c, &stream, GROUP).await.expect("create"));
            xadd(&mut c, &stream, 3, 3).await;
            let CheckedRead::Entries { added, .. } = read(&mut c, &stream).await else {
                panic!("entries");
            };
            assert_eq!(Value::Int(i64::try_from(added).expect("added")), stream_field(&mut c, &stream, "entries-added").await);
            let last = xadd(&mut c, &stream, 3, 5).await;
            let CheckedRead::Gap(evidence) = read(&mut c, &stream).await else {
                panic!("gap");
            };
            assert_eq!(evidence.last_generated_id, last);
            assert_eq!(evidence.added(), 8);
            del(&mut c, &stream).await;
        }

        #[tokio::test]
        async fn tail_reanchor_delivers_only_entries_after_detection() {
            let Some(mut c) = conn().await else {
                return;
            };
            let stream = format!("test:stream:{}", rand::random::<u32>());
            let evidence = gap(&mut c, &stream).await;
            let a = evidence.added();
            xadd(&mut c, &stream, 3, 1).await;
            let second = xadd(&mut c, &stream, 3, 1).await;
            let re = reanchor(&mut c, &stream, GROUP, CONSUMER, 500, ReanchorMode::Tail, &evidence)
                .await
                .expect("reanchor");
            assert_eq!((re.mode, re.entries_read.clone(), re.id.clone()), (ReanchorMode::Tail, a.to_string(), evidence.last_generated_id.clone()));
            assert_eq!(re.entries.len(), 2);
            assert_eq!(re.entries[1].id, second);
            assert!(entries(read(&mut c, &stream).await).is_empty(), "no gap and nothing left");
            assert_eq!(group_field(&mut c, &stream, "entries-read").await, Some((a + 2).to_string()));
            del(&mut c, &stream).await;
        }

        #[tokio::test]
        async fn tail_reanchor_falls_back_when_the_tail_was_trimmed() {
            let Some(mut c) = conn().await else {
                return;
            };
            let stream = format!("test:stream:{}", rand::random::<u32>());
            let evidence = gap(&mut c, &stream).await;
            xadd(&mut c, &stream, 3, 5).await;
            let re = reanchor(&mut c, &stream, GROUP, CONSUMER, 500, ReanchorMode::Tail, &evidence)
                .await
                .expect("reanchor");
            assert_eq!(re.mode, ReanchorMode::Oldest);
            let first = stream_field(&mut c, &stream, "first-entry").await;
            let Value::Array(first) = first else {
                panic!("first-entry {first:?}");
            };
            assert_eq!(Some(re.entries[0].id.clone()), string(&first[0]));
            let added = evidence.added() + 5;
            assert_eq!(re.entries_read, (added - 3).to_string());
            del(&mut c, &stream).await;
        }

        #[tokio::test]
        async fn reanchor_returns_the_first_batch() {
            let Some(mut c) = conn().await else {
                return;
            };
            let stream = format!("test:stream:{}", rand::random::<u32>());
            let evidence = gap(&mut c, &stream).await;
            let re = reanchor(&mut c, &stream, GROUP, CONSUMER, 2, ReanchorMode::Oldest, &evidence)
                .await
                .expect("reanchor");
            assert_eq!(re.entries.len(), 2);
            assert_eq!(re.entries[0].id, evidence.first_entry_id);
            assert_eq!(group_field(&mut c, &stream, "last-delivered-id").await, Some(re.entries[1].id.clone()));
            del(&mut c, &stream).await;
        }

        #[tokio::test]
        async fn group_created_on_an_existing_stream_has_exact_lag() {
            let Some(mut c) = conn().await else {
                return;
            };
            let stream = format!("test:stream:{}", rand::random::<u32>());
            xadd(&mut c, &stream, 100, 4).await;
            assert!(group_create(&mut c, &stream, GROUP).await.expect("create"));
            // Created at `$` with ENTRIESREAD = entries-added: lag 0, no gap,
            // and history is not delivered.
            assert!(entries(read(&mut c, &stream).await).is_empty());
            xadd(&mut c, &stream, 100, 1).await;
            assert_eq!(entries(read(&mut c, &stream).await).len(), 1);
            del(&mut c, &stream).await;
        }
    }
}
