//! Drafts (design-v3 §5.9): `createDraft`, `getDrafts`, `updateDraft`,
//! `deleteDraft`. A draft is the member's private stored content here, not
//! a repository record.
//!
//! - A draft is checked against `app.bsky.draft.defs#draft` and a size cap
//!   before it is stored.
//! - Nothing in a draft is followed: not its on-device file paths, not its
//!   links or quoted records. Draft content is never logged.
//! - `updateDraft` for an id the member does not own, or that does not
//!   exist, writes nothing and still succeeds, as the lexicon says.

use chrono::{DateTime, Utc};
use proto_blue::common::strings::grapheme_len;
use proto_blue::syntax::Tid;
use serde_json::{Value, json};

use super::feed::{local_cursor, parse_local_cursor};
use super::{Hydrator, ViewError, ViewResult, Viewer, ts};

type Check = Result<(), String>;

/// `value[name]`, when present, as a string of at most `bytes` bytes and
/// (when given) `graphemes` graphemes.
fn text(value: &Value, name: &str, at: &str, bytes: usize, graphemes: Option<usize>) -> Check {
    let Some(field) = value.get(name) else { return Ok(()) };
    let s = field.as_str().ok_or_else(|| format!("{at}.{name} must be a string"))?;
    if s.len() > bytes {
        return Err(format!("{at}.{name} must not be longer than {bytes} bytes"));
    }
    if graphemes.is_some_and(|max| grapheme_len(s) > max) {
        return Err(format!("{at}.{name} must not be longer than {} graphemes", graphemes.unwrap_or_default()));
    }
    Ok(())
}

/// `value[name]`, when present, as an array of at most `max` items, each
/// passing `item`.
fn items(value: &Value, name: &str, at: &str, max: usize, item: impl Fn(&Value, &str) -> Check) -> Check {
    let Some(field) = value.get(name) else { return Ok(()) };
    let list = field.as_array().ok_or_else(|| format!("{at}.{name} must be an array"))?;
    if list.len() > max {
        return Err(format!("{at}.{name} must not have more than {max} elements"));
    }
    list.iter().enumerate().try_for_each(|(i, v)| item(v, &format!("{at}.{name}[{i}]")))
}

fn object(value: &Value, at: &str) -> Check {
    value.is_object().then_some(()).ok_or_else(|| format!("{at} must be an object"))
}

/// A member of an open union: an object that names its type.
fn union_member(value: &Value, at: &str) -> Check {
    object(value, at)?;
    value.get("$type").and_then(Value::as_str).map(|_| ()).ok_or_else(|| format!("{at} must have a $type"))
}

fn local_ref(value: &Value, at: &str) -> Check {
    object(value, at)?;
    let path = value
        .pointer("/localRef/path")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{at}.localRef.path is required"))?;
    if path.is_empty() || path.len() > 1024 {
        return Err(format!("{at}.localRef.path must be 1 to 1024 bytes"));
    }
    text(value, "alt", at, usize::MAX, Some(2000))
}

fn video(value: &Value, at: &str) -> Check {
    local_ref(value, at)?;
    items(value, "captions", at, 20, |caption, at| {
        object(caption, at)?;
        if caption.get("lang").and_then(Value::as_str).is_none() || caption.get("content").is_none() {
            return Err(format!("{at} must have lang and content"));
        }
        text(caption, "content", at, 10_000, None)
    })
}

fn external(value: &Value, at: &str) -> Check {
    object(value, at)?;
    let uri = value.get("uri").and_then(Value::as_str).ok_or_else(|| format!("{at}.uri is required"))?;
    // The lexicon's `uri` format: a scheme, a colon, and something after it.
    let well_formed = uri.split_once(':').is_some_and(|(scheme, rest)| {
        !rest.is_empty() && scheme.starts_with(|c: char| c.is_ascii_alphabetic()) && scheme.chars().all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
    });
    well_formed.then_some(()).ok_or_else(|| format!("{at}.uri must be a URI"))
}

fn record(value: &Value, at: &str) -> Check {
    object(value, at)?;
    let field = |name: &str| value.pointer(&format!("/record/{name}")).and_then(Value::as_str).is_some_and(|s| !s.is_empty());
    (field("uri") && field("cid")).then_some(()).ok_or_else(|| format!("{at}.record must have uri and cid"))
}

fn post(value: &Value, at: &str) -> Check {
    object(value, at)?;
    if value.get("text").is_none() {
        return Err(format!("{at}.text is required"));
    }
    text(value, "text", at, 10_000, Some(1000))?;
    if let Some(labels) = value.get("labels") {
        union_member(labels, &format!("{at}.labels"))?;
    }
    items(value, "embedImages", at, 4, local_ref)?;
    items(value, "embedVideos", at, 1, video)?;
    items(value, "embedExternals", at, 1, external)?;
    items(value, "embedRecords", at, 1, record)?;
    if let Some(gallery) = value.get("embedGallery") {
        let at = format!("{at}.embedGallery");
        object(gallery, &at)?;
        if gallery.get("items").is_none() {
            return Err(format!("{at}.items is required"));
        }
        items(gallery, "items", &at, 20, local_ref)?;
    }
    Ok(())
}

/// `draft` against `app.bsky.draft.defs#draft`.
fn validate(draft: &Value) -> Check {
    object(draft, "draft")?;
    let posts = draft.get("posts").and_then(Value::as_array).ok_or("draft.posts is required")?;
    if posts.is_empty() {
        return Err("draft.posts must not have fewer than 1 elements".to_string());
    }
    items(draft, "posts", "draft", 100, post)?;
    text(draft, "deviceId", "draft", 100, None)?;
    text(draft, "deviceName", "draft", 100, None)?;
    items(draft, "langs", "draft", 3, |lang, at| lang.is_string().then_some(()).ok_or_else(|| format!("{at} must be a string")))?;
    items(draft, "postgateEmbeddingRules", "draft", 5, union_member)?;
    items(draft, "threadgateAllow", "draft", 5, union_member)
}

fn invalid(message: impl Into<String>) -> ViewError {
    ViewError::new("InvalidRequest", message)
}

impl Hydrator {
    /// The checks every stored draft passes: the lexicon's shape and the
    /// size cap (`drafts.max_bytes`).
    fn checked_draft(&self, draft: &Value) -> Result<(), ViewError> {
        validate(draft).map_err(invalid)?;
        let size = serde_json::to_vec(draft).map_or(usize::MAX, |bytes| bytes.len());
        if size > usize::try_from(self.inner.cfg.drafts_max_bytes).unwrap_or(usize::MAX) {
            return Err(invalid("draft is too large"));
        }
        Ok(())
    }

    /// `app.bsky.draft.createDraft`: stores the draft under an id made here.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn create_draft(&self, viewer: &Viewer, draft: &Value) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        if let Err(e) = self.checked_draft(draft) {
            return Ok(Err(e));
        }
        let held: i64 = sqlx::query_scalar("SELECT count(*) FROM drafts WHERE owner_did = $1")
            .bind(me)
            .fetch_one(self.pool())
            .await?;
        if held >= i64::from(self.inner.cfg.drafts_max) {
            return Ok(Err(ViewError::new("DraftLimitReached", "Draft limit reached")));
        }
        let micros = u64::try_from(Utc::now().timestamp_micros()).unwrap_or_default();
        let id = Tid::from_timestamp(micros, rand::random::<u16>() & 0x3ff).into_inner();
        sqlx::query("INSERT INTO drafts (owner_did, id, draft) VALUES ($1, $2, $3)")
            .bind(me)
            .bind(&id)
            .bind(draft)
            .execute(self.pool())
            .await?;
        Ok(Ok(Self::emit(json!({ "id": id }))))
    }

    /// `app.bsky.draft.updateDraft`: replaces the member's draft `id`. An id
    /// that is not the member's changes nothing.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn update_draft(&self, viewer: &Viewer, id: &str, draft: &Value) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        if let Err(e) = self.checked_draft(draft) {
            return Ok(Err(e));
        }
        sqlx::query("UPDATE drafts SET draft = $3, updated_at = now() WHERE owner_did = $1 AND id = $2")
            .bind(me)
            .bind(id)
            .bind(draft)
            .execute(self.pool())
            .await?;
        Ok(Ok(Self::emit(json!({}))))
    }

    /// `app.bsky.draft.deleteDraft`: idempotent.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn delete_draft(&self, viewer: &Viewer, id: &str) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        sqlx::query("DELETE FROM drafts WHERE owner_did = $1 AND id = $2")
            .bind(me)
            .bind(id)
            .execute(self.pool())
            .await?;
        Ok(Ok(Self::emit(json!({}))))
    }

    /// `app.bsky.draft.getDrafts`: the member's drafts, most recently
    /// changed first.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_drafts(&self, viewer: &Viewer, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, Value, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT id, draft, created_at, updated_at FROM drafts WHERE owner_did = $1 \
               AND ($2::timestamptz IS NULL OR (updated_at, id) < ($2, $3)) \
             ORDER BY updated_at DESC, id DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(id, _, _, updated)| local_cursor(*updated, id)))
            .flatten();
        let drafts: Vec<Value> = rows
            .into_iter()
            .map(|(id, draft, created, updated)| json!({"id": id, "draft": draft, "createdAt": ts(created), "updatedAt": ts(updated)}))
            .collect();
        let mut out = json!({ "drafts": drafts });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::hydrator;
    use crate::test_support::fresh_db;

    const ME: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    fn draft(text: &str) -> Value {
        json!({"posts": [{"text": text}]})
    }

    #[test]
    fn drafts_are_checked_against_the_lexicon() {
        let full = json!({
            "deviceId": "0b9a", "deviceName": "phone", "langs": ["en"],
            "postgateEmbeddingRules": [{"$type": "app.bsky.feed.postgate#disableRule"}],
            "threadgateAllow": [{"$type": "app.bsky.feed.threadgate#mentionRule"}],
            "posts": [{
                "text": "hello",
                "labels": {"$type": "com.atproto.label.defs#selfLabels", "values": []},
                "embedImages": [{"localRef": {"path": "/a.jpg"}, "alt": "a"}],
                "embedVideos": [{"localRef": {"path": "/v.mp4"}, "captions": [{"lang": "en", "content": "c"}]}],
                "embedExternals": [{"uri": "https://example.com/"}],
                "embedRecords": [{"record": {"uri": "at://did:plc:x/app.bsky.feed.post/1", "cid": "bafy"}}],
                "embedGallery": {"items": [{"localRef": {"path": "/g.jpg"}}]},
            }],
        });
        assert_eq!(validate(&full), Ok(()));
        let with = |pointer: &str, value: Value| {
            let mut d = full.clone();
            *d.pointer_mut(pointer).expect(pointer) = value;
            d
        };
        let image = json!({"localRef": {"path": "/a.jpg"}});
        for (pointer, value) in [
            ("/posts", json!([])),
            ("/posts", Value::Array(vec![json!({"text": "t"}); 101])),
            ("/posts/0", json!({})),
            ("/posts/0/text", json!("x".repeat(10_001))),
            // 1001 graphemes in under 10 000 bytes.
            ("/posts/0/text", json!("é".repeat(1001))),
            ("/posts/0/embedImages", Value::Array(vec![image.clone(); 5])),
            ("/posts/0/embedImages/0", json!({"alt": "no file"})),
            ("/posts/0/embedImages/0/localRef/path", json!("")),
            ("/posts/0/embedVideos", Value::Array(vec![image.clone(); 2])),
            ("/posts/0/embedVideos/0/captions/0", json!({"lang": "en"})),
            ("/posts/0/embedExternals/0/uri", json!("no scheme")),
            ("/posts/0/embedRecords/0/record", json!({"uri": "at://x/y/z"})),
            ("/posts/0/embedGallery", json!({})),
            ("/posts/0/embedGallery/items", Value::Array(vec![image.clone(); 21])),
            ("/deviceName", json!("d".repeat(101))),
            ("/langs", json!(["a", "b", "c", "d"])),
            ("/threadgateAllow/0", json!({"no": "type"})),
            ("/postgateEmbeddingRules", json!("rule")),
        ] {
            assert!(validate(&with(pointer, value)).is_err(), "{pointer}");
        }
        assert!(validate(&json!("draft")).is_err());
        assert!(validate(&json!({})).is_err());
    }

    /// The four endpoints over the table.
    #[tokio::test]
    async fn drafts_are_stored_listed_changed_and_removed() {
        let Some(db) = fresh_db("hydration_drafts").await else {
            return;
        };
        let p = &db.pool;
        let h = hydrator(p, ScopeSets::default());
        let me = Viewer {
            did: Some(ME.into()),
            hydrate: false,
        };
        let other = Viewer {
            did: Some(BOB.into()),
            hydrate: false,
        };
        let id_of = |v: &Value| v["id"].as_str().expect("id").to_string();

        assert_eq!(h.create_draft(&me, &json!({"posts": []})).await.expect("db").expect_err("invalid").error, "InvalidRequest");
        // The size cap (2 000 bytes in tests): 900 graphemes, 3 600 bytes.
        assert!(h.create_draft(&me, &draft(&"😀".repeat(900))).await.expect("db").expect_err("large").message.contains("large"));
        let first = id_of(h.create_draft(&me, &draft("first")).await.expect("db").expect("stored").inner());
        let second = id_of(h.create_draft(&me, &draft("second")).await.expect("db").expect("stored").inner());
        assert!(Tid::is_valid(&first) && first != second);
        // The cap (2 in tests).
        assert_eq!(h.create_draft(&me, &draft("third")).await.expect("db").expect_err("cap").error, "DraftLimitReached");

        // An update moves the draft to the top; another member's update of
        // the same id, and an update of an id nobody has, change nothing.
        h.update_draft(&me, &first, &draft("first, edited")).await.expect("db").expect("updated");
        h.update_draft(&other, &first, &draft("stolen")).await.expect("db").expect("ignored");
        h.update_draft(&me, "3kzzzzzzzzzzz", &draft("nowhere")).await.expect("db").expect("ignored");
        assert_eq!(h.update_draft(&me, &first, &json!({})).await.expect("db").expect_err("invalid").error, "InvalidRequest");
        let l = h.get_drafts(&me, 50, None).await.expect("db").expect("view");
        let d = l.inner()["drafts"].as_array().expect("drafts");
        assert_eq!(d.len(), 2);
        assert_eq!((&d[0]["id"], &d[0]["draft"]["posts"][0]["text"]), (&json!(first), &json!("first, edited")));
        assert!(d[0]["createdAt"].is_string() && d[0]["updatedAt"].is_string());
        assert_eq!(h.get_drafts(&other, 50, None).await.expect("db").expect("view").inner()["drafts"], json!([]));
        // Paging.
        let page = h.get_drafts(&me, 1, None).await.expect("db").expect("view");
        let cursor = page.inner()["cursor"].as_str().expect("cursor").to_string();
        let rest = h.get_drafts(&me, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(rest.inner()["drafts"][0]["id"], json!(second));

        // Removal is the owner's only, and can be repeated.
        h.delete_draft(&other, &first).await.expect("db").expect("ignored");
        for _ in 0..2 {
            h.delete_draft(&me, &second).await.expect("db").expect("removed");
        }
        let l = h.get_drafts(&me, 50, None).await.expect("db").expect("view");
        assert_eq!(l.inner()["drafts"].as_array().map(Vec::len), Some(1));
        db.drop_db().await;
    }
}
