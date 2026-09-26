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

/// Trim evidence returned by `read_checked`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapEvidence {
    pub first_entry_id: String,
    pub entries_added: String,
    pub length: String,
}

/// The outcome of one `read_checked` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckedRead {
    /// The group does not exist (the stream was deleted or recreated).
    NoGroup,
    Gap(GapEvidence),
    Entries(Vec<StreamEntry>),
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
            let [first_entry_id, entries_added, length] = <[String; 3]>::try_from(rest)
                .map_err(|r| ScriptError::Shape(format!("gap evidence {r:?}")))?;
            Ok(CheckedRead::Gap(GapEvidence {
                first_entry_id,
                entries_added,
                length,
            }))
        }
        Some("ok") => match it.next() {
            None | Some(Value::Nil) => Ok(CheckedRead::Entries(Vec::new())),
            Some(v) => parse_read_reply(v)
                .map(CheckedRead::Entries)
                .map_err(ScriptError::Shape),
        },
        other => Err(ScriptError::Shape(format!("tag {other:?}"))),
    }
}

/// Re-anchor the group after a gap transaction. Returns the new
/// last-delivered id and entries-read.
///
/// # Errors
///
/// A Redis error or an unexpected reply.
pub async fn reanchor(conn: &mut ConnectionManager, stream: &str, group: &str) -> Result<(String, String), ScriptError> {
    let reply: Vec<String> = REANCHOR.key(stream).arg(group).invoke_async(conn).await?;
    <[String; 2]>::try_from(reply)
        .map(|[id, read]| (id, read))
        .map_err(|r| ScriptError::Shape(format!("reanchor {r:?}")))
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
            parse_checked(Value::Array(vec![bulk("gap"), bulk("5-0"), bulk("100"), bulk("10")])).expect("ok"),
            CheckedRead::Gap(GapEvidence {
                first_entry_id: "5-0".into(),
                entries_added: "100".into(),
                length: "10".into()
            })
        );
        assert_eq!(parse_checked(Value::Array(vec![bulk("ok")])).expect("ok"), CheckedRead::Entries(Vec::new()));
        let reply = Value::Array(vec![Value::Array(vec![
            bulk("firehose:events"),
            Value::Array(vec![Value::Array(vec![
                bulk("1-0"),
                Value::Array(vec![bulk("type"), bulk("commit"), bulk("data"), bulk("{}"), bulk("seq"), bulk("7")]),
            ])]),
        ])]);
        match parse_checked(Value::Array(vec![bulk("ok"), reply])).expect("ok") {
            CheckedRead::Entries(e) => {
                assert_eq!(e.len(), 1);
                assert_eq!(e[0].id, "1-0");
                assert_eq!(e[0].field("type"), Some("commit"));
            }
            other => panic!("{other:?}"),
        }
        assert!(parse_checked(Value::Int(1)).is_err());
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

        async fn xadd(c: &mut ConnectionManager, stream: &str, maxlen: u64, n: usize) {
            for i in 0..n {
                let _: String = redis::cmd("XADD")
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
        }

        async fn read(c: &mut ConnectionManager, stream: &str) -> CheckedRead {
            read_checked(c, stream, GROUP, CONSUMER, 500).await.expect("read_checked")
        }

        fn entries(r: CheckedRead) -> Vec<StreamEntry> {
            match r {
                CheckedRead::Entries(e) => e,
                other => panic!("expected entries, got {other:?}"),
            }
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

            // Re-anchor: the survivors are delivered and lag is exact again.
            let (_id, entries_read) = reanchor(&mut c, &stream, GROUP).await.expect("reanchor");
            assert_eq!(entries_read, "8");
            let got = entries(read(&mut c, &stream).await);
            assert_eq!(got.len(), 3);
            assert_eq!(got[0].id, evidence.first_entry_id);
            assert!(entries(read(&mut c, &stream).await).is_empty());

            let _: () = redis::cmd("DEL").arg(&stream).query_async(&mut c).await.expect("del");
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
            let _: () = redis::cmd("DEL").arg(&stream).query_async(&mut c).await.expect("del");
        }
    }
}
