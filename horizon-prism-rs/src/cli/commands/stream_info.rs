//! `admin stream info` (§3.3): consumer-group lag, pending count,
//! last-delivered id and first stream id. Lag is shown both as Redis reports
//! it and as computed from the group counter (`entries-added −
//! entries-read`, §6.11.4), which is the value the gap check uses.

use redis::Value;
use redis::aio::ConnectionManager;

use crate::firehose::scripts::string;

fn fields(v: &Value) -> Vec<(String, Value)> {
    match v {
        Value::Array(flat) => flat
            .chunks(2)
            .filter_map(|c| Some((string(c.first()?)?, c.get(1)?.clone())))
            .collect(),
        Value::Map(m) => m.iter().filter_map(|(k, v)| Some((string(k)?, v.clone()))).collect(),
        _ => Vec::new(),
    }
}

fn get<'a>(f: &'a [(String, Value)], k: &str) -> Option<&'a Value> {
    f.iter().find(|(n, _)| n == k).map(|(_, v)| v)
}

fn show(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Nil) => "nil".into(),
        Some(Value::Array(a)) => a.first().and_then(string).unwrap_or_else(|| "nil".into()),
        Some(other) => string(other).unwrap_or_else(|| format!("{other:?}")),
    }
}

fn int(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Int(i) => Some(*i),
        other => string(other)?.parse().ok(),
    }
}

/// The stream and its reader group, read for the admin UI's Problems page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamInfo {
    pub length: Option<i64>,
    /// The first entry's id, `None` when the stream is empty.
    pub first_entry_id: Option<String>,
    /// `None`: the group is absent.
    pub group: Option<GroupInfo>,
}

/// A reader group's state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupInfo {
    pub pending: Option<i64>,
    pub last_delivered_id: Option<String>,
    /// `entries-added − entries-read`, the value the gap check uses; `None`
    /// when Redis cannot say (a gap).
    pub lag: Option<i64>,
}

fn id(v: Option<&Value>) -> Option<String> {
    Some(show(v)).filter(|s| s != "nil")
}

/// Read stream and group state.
///
/// # Errors
///
/// Redis errors (for example, a missing stream).
pub async fn read(redis: &mut ConnectionManager, stream: &str, group: &str) -> Result<StreamInfo, String> {
    let info: Value = redis::cmd("XINFO").arg("STREAM").arg(stream).query_async(redis).await.map_err(|e| e.to_string())?;
    let s = fields(&info);
    let groups: Value = redis::cmd("XINFO").arg("GROUPS").arg(stream).query_async(redis).await.map_err(|e| e.to_string())?;
    let found = match &groups {
        Value::Array(list) => list.iter().map(fields).find(|g| get(g, "name").and_then(string).as_deref() == Some(group)),
        _ => None,
    };
    Ok(StreamInfo {
        length: int(get(&s, "length")),
        first_entry_id: id(get(&s, "first-entry")),
        group: found.map(|g| GroupInfo {
            pending: int(get(&g, "pending")),
            last_delivered_id: id(get(&g, "last-delivered-id")),
            lag: match (int(get(&s, "entries-added")), int(get(&g, "entries-read"))) {
                (Some(a), Some(r)) => Some(a - r),
                _ => None,
            },
        }),
    })
}

/// Render stream and group state.
///
/// # Errors
///
/// Redis errors (for example, a missing stream).
pub async fn render(redis: &mut ConnectionManager, stream: &str, group: &str) -> Result<String, String> {
    let info: Value = redis::cmd("XINFO")
        .arg("STREAM")
        .arg(stream)
        .query_async(redis)
        .await
        .map_err(|e| e.to_string())?;
    let s = fields(&info);
    let mut out = vec![
        format!("stream: {stream}"),
        format!("length: {}", show(get(&s, "length"))),
        format!("entries-added: {}", show(get(&s, "entries-added"))),
        format!("first-entry-id: {}", show(get(&s, "first-entry"))),
        format!("last-generated-id: {}", show(get(&s, "last-generated-id"))),
    ];
    let groups: Value = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream)
        .query_async(redis)
        .await
        .map_err(|e| e.to_string())?;
    let found = match &groups {
        Value::Array(list) => list.iter().map(fields).find(|g| get(g, "name").and_then(string).as_deref() == Some(group)),
        _ => None,
    };
    match found {
        None => out.push(format!("group {group}: absent")),
        Some(g) => {
            let computed = match (int(get(&s, "entries-added")), int(get(&g, "entries-read"))) {
                (Some(a), Some(r)) => (a - r).to_string(),
                _ => "nil (gap)".into(),
            };
            out.push(format!("group: {group}"));
            out.push(format!("consumers: {}", show(get(&g, "consumers"))));
            out.push(format!("pending: {}", show(get(&g, "pending"))));
            out.push(format!("last-delivered-id: {}", show(get(&g, "last-delivered-id"))));
            out.push(format!("entries-read: {}", show(get(&g, "entries-read"))));
            out.push(format!("lag (computed): {computed}"));
            out.push(format!("lag (reported): {}", show(get(&g, "lag"))));
        }
    }
    Ok(out.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shows_group_state() {
        let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
            return;
        };
        let mut c = ConnectionManager::new(redis::Client::open(url).expect("url")).await.expect("redis");
        let stream = format!("test:stream:{}", rand::random::<u32>());
        for _ in 0..3 {
            let _: String = redis::cmd("XADD").arg(&stream).arg("*").arg("k").arg("v").query_async(&mut c).await.expect("xadd");
        }
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg("g")
            .arg("0")
            .arg("ENTRIESREAD")
            .arg(0)
            .query_async(&mut c)
            .await
            .expect("group");
        let out = render(&mut c, &stream, "g").await.expect("render");
        assert!(out.contains("length: 3"), "{out}");
        assert!(out.contains("entries-added: 3"), "{out}");
        assert!(out.contains("lag (computed): 3"), "{out}");
        let absent = render(&mut c, &stream, "nope").await.expect("render");
        assert!(absent.contains("group nope: absent"));
        let info = read(&mut c, &stream, "g").await.expect("read");
        assert_eq!(info.length, Some(3));
        assert!(info.first_entry_id.is_some());
        assert_eq!(info.group.as_ref().and_then(|g| g.lag), Some(3));
        assert_eq!(info.group.as_ref().and_then(|g| g.last_delivered_id.clone()), Some("0-0".to_string()));
        assert_eq!(read(&mut c, &stream, "nope").await.expect("read").group, None);
        let _: () = redis::cmd("DEL").arg(&stream).query_async(&mut c).await.expect("del");
    }
}
