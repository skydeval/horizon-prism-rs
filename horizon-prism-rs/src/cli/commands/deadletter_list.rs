//! `admin deadletter list [--limit N]`: newest dead letters first (§5.3).

use redis::Value;
use redis::aio::ConnectionManager;

use super::table;
use crate::firehose::stream::parse_entry;

/// Default number of entries shown.
pub const DEFAULT_LIMIT: u32 = 50;

/// Render the newest `limit` dead letters.
///
/// # Errors
///
/// Redis errors or an unexpected reply.
pub async fn render(redis: &mut ConnectionManager, stream: &str, limit: u32) -> Result<String, String> {
    let reply: Value = redis::cmd("XREVRANGE")
        .arg(stream)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(limit.max(1))
        .query_async(redis)
        .await
        .map_err(|e| e.to_string())?;
    let Value::Array(items) = reply else {
        return Err(format!("unexpected XREVRANGE reply: {reply:?}"));
    };
    let mut rows = Vec::with_capacity(items.len());
    for item in items {
        let e = parse_entry(item)?;
        let f = |k: &str| e.field(k).unwrap_or_default().to_string();
        let error: String = f("error").chars().take(120).collect();
        rows.push(vec![e.id.clone(), f("type"), f("relay"), f("repo"), f("rev"), f("action"), f("path"), error]);
    }
    Ok(table(&["id", "type", "relay", "repo", "rev", "action", "path", "error"], &rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lists_newest_first() {
        let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
            return;
        };
        let mut c = ConnectionManager::new(redis::Client::open(url).expect("url")).await.expect("redis");
        let stream = format!("test:dl:{}", rand::random::<u32>());
        for (i, path) in ["app.bsky.feed.post/1", "app.bsky.feed.like/2"].iter().enumerate() {
            let _: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("type")
                .arg("commit")
                .arg("repo")
                .arg("did:plc:a")
                .arg("path")
                .arg(*path)
                .arg("error")
                .arg(format!("bad {i}"))
                .query_async(&mut c)
                .await
                .expect("xadd");
        }
        let out = render(&mut c, &stream, 10).await.expect("render");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("id "));
        assert!(lines[1].contains("app.bsky.feed.like/2") && lines[1].contains("bad 1"));
        assert!(lines[2].contains("app.bsky.feed.post/1"));
        let one = render(&mut c, &stream, 1).await.expect("render");
        assert_eq!(one.lines().count(), 2);
        let _: () = redis::cmd("DEL").arg(&stream).query_async(&mut c).await.expect("del");
    }
}
