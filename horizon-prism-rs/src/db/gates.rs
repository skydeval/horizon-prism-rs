//! Stored threadgates and postgates, as the read paths use them.
//!
//! A gate row is keyed `(author, rkey)` and names its post as a reference
//! `(post_author, post_coll, post_rkey)`. A gate counts only when it is its
//! post's own: its post is an `app.bsky.feed.post` with the gate's author and
//! record key, so a post's valid gate is found by the post's
//! own key. Its rules are read from its record.

use std::collections::HashMap;

use serde_json::Value;
use sqlx::PgPool;

use super::ids::{COLL_POST, cid_from_db};
use super::records::decode_value;

/// A post's valid stored gate.
#[derive(Debug, Clone, PartialEq)]
pub struct Gate {
    /// The gate's URI: its collection under the post's author and key.
    pub uri: String,
    pub cid: String,
    pub record: Value,
}

impl Gate {
    fn field(&self, key: &str) -> Option<&Value> {
        self.record.get(key).filter(|v| !v.is_null())
    }

    fn strings(&self, key: &str) -> Vec<String> {
        self.record
            .get(key)
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
            .unwrap_or_default()
    }

    /// A threadgate's `allow`: `None` when absent (anyone may reply); an
    /// empty array lets nobody.
    #[must_use]
    pub fn allow(&self) -> Option<Value> {
        self.field("allow").cloned()
    }

    /// A threadgate's hidden replies.
    #[must_use]
    pub fn hidden_replies(&self) -> Vec<String> {
        self.strings("hiddenReplies")
    }

    /// A postgate's detached quotes.
    #[must_use]
    pub fn detached(&self) -> Vec<String> {
        self.strings("detachedEmbeddingUris")
    }

    /// Whether a postgate's rules turn quoting off (a `disableRule`).
    #[must_use]
    pub fn embedding_disabled(&self) -> bool {
        self.field("embeddingRules")
            .and_then(Value::as_array)
            .is_some_and(|rules| rules.iter().any(|r| r.get("$type").and_then(Value::as_str) == Some("app.bsky.feed.postgate#disableRule")))
    }
}

/// The gate table of a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Threadgate,
    Postgate,
}

impl Kind {
    const fn table(self) -> &'static str {
        match self {
            Self::Threadgate => "threadgates",
            Self::Postgate => "postgates",
        }
    }

    const fn nsid(self) -> &'static str {
        match self {
            Self::Threadgate => "app.bsky.feed.threadgate",
            Self::Postgate => "app.bsky.feed.postgate",
        }
    }
}

/// The valid stored gates of `posts`, by post URI. A URI that is not a
/// whole stored post's has none.
///
/// # Errors
///
/// Database errors, and a stored record that does not decode.
pub async fn valid(pool: &PgPool, kind: Kind, posts: &[String]) -> Result<HashMap<String, Gate>, sqlx::Error> {
    if posts.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(String, Vec<u8>, Vec<u8>)> = sqlx::query_as(&format!(
        "SELECT u.uri, g.cid, g.record FROM unnest($1::text[]) AS u(uri) CROSS JOIN LATERAL ref_of(u.uri) k \
         JOIN {} g ON k.coll = {COLL_POST} AND g.author = k.author AND g.rkey = k.rkey \
         WHERE g.post_coll = {COLL_POST} AND g.post_author = g.author AND g.post_rkey = g.rkey",
        kind.table()
    ))
    .bind(posts)
    .fetch_all(pool)
    .await?;
    let mut out = HashMap::with_capacity(rows.len());
    for (post, cid, record) in rows {
        let Some((prefix, rkey)) = post.rsplit_once('/') else { continue };
        let Some(repo) = prefix.strip_suffix(crate::firehose::classifier::NSID_POST).and_then(|p| p.strip_suffix('/')) else {
            continue;
        };
        let gate = Gate { uri: format!("{repo}/{}/{rkey}", kind.nsid()), cid: cid_from_db(&cid), record: decode_value(pool, &record).await? };
        out.insert(post, gate);
    }
    Ok(out)
}

/// The valid stored gate of one post.
///
/// # Errors
///
/// As [`valid`].
pub async fn valid_one(pool: &PgPool, kind: Kind, post: &str) -> Result<Option<Gate>, sqlx::Error> {
    Ok(valid(pool, kind, std::slice::from_ref(&post.to_string())).await?.remove(post))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn gate(record: Value) -> Gate {
        Gate { uri: String::new(), cid: String::new(), record }
    }

    #[test]
    fn rules_are_read_from_the_record() {
        let g = gate(json!({"allow": [], "hiddenReplies": ["at://a/b/c", 7]}));
        assert_eq!((g.allow(), g.hidden_replies()), (Some(json!([])), vec!["at://a/b/c".to_string()]));
        assert_eq!(gate(json!({"allow": null})).allow(), None, "a null allow is none");
        let p = gate(json!({"embeddingRules": [{"$type": "app.bsky.feed.postgate#disableRule"}], "detachedEmbeddingUris": ["at://q"]}));
        assert!(p.embedding_disabled());
        assert_eq!(p.detached(), ["at://q"]);
        assert!(!gate(json!({"embeddingRules": {"$type": "app.bsky.feed.postgate#disableRule"}})).embedding_disabled(), "rules are an array");
        assert!(!gate(json!({})).embedding_disabled());
    }
}
