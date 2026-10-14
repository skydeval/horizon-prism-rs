//! Bookmarks (design-v3 §5.8): `createBookmark`, `deleteBookmark`,
//! `getBookmarks`. A bookmark is the member's private state here, not a
//! repository record.
//!
//! - Only `app.bsky.feed.post` URIs can be bookmarked. Anything else,
//!   private-tier URIs included, is `UnsupportedCollection`, decided from
//!   the URI alone: the answer never says whether a record exists.
//! - The post is not looked up on a write.
//! - The listing reads stored posts only and leaves no trace in shared
//!   state: no fetch, no count refresh. A post that is not stored here, is
//!   gone or is hidden shows as `notFoundPost`.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::feed::{NSID_POST, local_cursor, parse_local_cursor};
use super::{CountMode, Hydrator, ViewError, ViewResult, Viewer, ts};
use crate::xrpc::pools::Requester;

/// Whether `uri` is `at://<did>/app.bsky.feed.post/<rkey>`.
fn is_post_uri(uri: &str) -> bool {
    let mut parts = uri.strip_prefix("at://").unwrap_or_default().split('/');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(did), Some(NSID_POST), Some(rkey), None) if did.starts_with("did:") && !rkey.is_empty()
    )
}

fn unsupported() -> ViewError {
    ViewError::new("UnsupportedCollection", "Only app.bsky.feed.post records can be bookmarked")
}

impl Hydrator {
    /// `app.bsky.bookmark.createBookmark`: idempotent; a repeat replaces the
    /// stored CID and keeps the bookmark's place.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn create_bookmark(&self, viewer: &Viewer, uri: &str, cid: &str) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        if !is_post_uri(uri) {
            return Ok(Err(unsupported()));
        }
        let (held, existing): (i64, bool) =
            sqlx::query_as("SELECT count(*), COALESCE(bool_or(post_uri = $2), false) FROM bookmarks WHERE viewer_did = $1")
                .bind(me)
                .bind(uri)
                .fetch_one(self.pool())
                .await?;
        if !existing && held >= i64::from(self.inner.cfg.bookmarks_max) {
            return Ok(Err(ViewError::new("InvalidRequest", "bookmark limit reached")));
        }
        sqlx::query(
            "INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ($1, $2, $3) \
             ON CONFLICT (viewer_did, post_uri) DO UPDATE SET post_cid = EXCLUDED.post_cid",
        )
        .bind(me)
        .bind(uri)
        .bind(cid)
        .execute(self.pool())
        .await?;
        Ok(Ok(Self::emit(json!({}))))
    }

    /// `app.bsky.bookmark.deleteBookmark`: idempotent.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn delete_bookmark(&self, viewer: &Viewer, uri: &str) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        if !is_post_uri(uri) {
            return Ok(Err(unsupported()));
        }
        sqlx::query("DELETE FROM bookmarks WHERE viewer_did = $1 AND post_uri = $2")
            .bind(me)
            .bind(uri)
            .execute(self.pool())
            .await?;
        Ok(Ok(Self::emit(json!({}))))
    }

    /// `app.bsky.bookmark.getBookmarks`: the member's bookmarks, newest
    /// first.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_bookmarks(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        // The items come from the viewer's bookmarks (design-v4 §8.1).
        let req = &req.viewer_private(me);
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT post_uri, post_cid, created_at FROM bookmarks WHERE viewer_did = $1 \
               AND ($2::timestamptz IS NULL OR (created_at, post_uri) < ($2, $3)) \
             ORDER BY created_at DESC, post_uri DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(uri, _, t)| local_cursor(*t, uri)))
            .flatten();
        let uris: Vec<String> = rows.iter().map(|(uri, ..)| uri.clone()).collect();
        let ctx = self.post_context(viewer, req, &uris, CountMode::Quiet).await?;
        let bookmarks: Vec<Value> = rows
            .iter()
            .map(|(uri, cid, created)| {
                json!({
                    "subject": {"uri": uri, "cid": cid},
                    "createdAt": ts(*created),
                    "item": ctx.reply_ref_view(uri),
                })
            })
            .collect();
        let mut out = json!({ "bookmarks": bookmarks });
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
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::test_support::fresh_db;

    const ME: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    /// B-53 (bookmarks): the collection rule, the cap, and the listing.
    #[tokio::test]
    async fn bookmarks_are_kept_listed_and_removed() {
        let Some(db) = fresh_db("hydration_bookmarks").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ME, "me.test", "Me").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let first = post(p, BOB, "3kaaaaaaaaaa2", "first", None, None).await;
        let second = post(p, BOB, "3kaaaaaaaaaa3", "second", None, None).await;
        let h = hydrator(p, ScopeSets::default());
        let me = Viewer {
            did: Some(ME.into()),
            hydrate: true,
        };
        let req = Requester::test();

        // Only posts; a private-tier URI gets the same answer whether or
        // not it exists.
        for bad in [
            format!("at://{BOB}/app.bsky.feed.like/1"),
            format!("at://{BOB}/tools.kryphocron.feed.postPrivate/3kaaaaaaaaaa2"),
            format!("at://bob.test/{NSID_POST}/1"),
            "nonsense".to_string(),
        ] {
            assert_eq!(h.create_bookmark(&me, &bad, "bafy").await.expect("db").expect_err("bad").error, "UnsupportedCollection", "{bad}");
            assert_eq!(h.delete_bookmark(&me, &bad).await.expect("db").expect_err("bad").error, "UnsupportedCollection", "{bad}");
        }
        let gone = format!("at://{BOB}/{NSID_POST}/3knever");
        for (uri, cid) in [(&first, "bafyold"), (&second, "bafysecond"), (&gone, "bafygone")] {
            h.create_bookmark(&me, uri, cid).await.expect("db").expect("kept");
        }
        // A repeat replaces the CID and is allowed at the cap (3 in tests);
        // a fourth post is not.
        h.create_bookmark(&me, &first, "bafyfirst").await.expect("db").expect("repeat");
        let over = format!("at://{BOB}/{NSID_POST}/3kfourth");
        assert!(h.create_bookmark(&me, &over, "bafy").await.expect("db").expect_err("cap").message.contains("limit"));

        // Newest first: a post never stored is a notFoundPost.
        let l = h.get_bookmarks(&me, &req, 50, None).await.expect("db").expect("view");
        let b = l.inner()["bookmarks"].as_array().expect("bookmarks");
        assert_eq!(b.len(), 3);
        assert_eq!((&b[0]["subject"]["uri"], &b[0]["item"]["$type"]), (&json!(gone), &json!("app.bsky.feed.defs#notFoundPost")));
        assert_eq!((&b[1]["item"]["$type"], &b[1]["item"]["record"]["text"]), (&json!("app.bsky.feed.defs#postView"), &json!("second")));
        assert_eq!(b[2]["subject"], json!({"uri": first, "cid": "bafyfirst"}));
        // The post's own view says so, for this member only.
        assert_eq!(b[2]["item"]["viewer"]["bookmarked"], true);
        let anon = h.get_posts(&Viewer::anonymous(), &req, std::slice::from_ref(&first)).await.expect("db").expect("view");
        assert!(anon.inner()["posts"][0]["viewer"].get("bookmarked").is_none());
        // A block with the author shows as blockedPost; a hidden post as
        // notFoundPost.
        exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{ME}/app.bsky.graph.block/1', '{ME}', '{BOB}', '3k', now())")).await;
        let l = h.get_bookmarks(&me, &req, 50, None).await.expect("db").expect("view");
        assert_eq!(l.inner()["bookmarks"][1]["item"]["$type"], "app.bsky.feed.defs#blockedPost");
        exec(p, "DELETE FROM blocks").await;
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{second}', '!takedown')")).await;
        let l = h.get_bookmarks(&me, &req, 50, None).await.expect("db").expect("view");
        assert_eq!(l.inner()["bookmarks"][1]["item"]["$type"], "app.bsky.feed.defs#notFoundPost");

        // Paging, and removal (which can be repeated).
        let page = h.get_bookmarks(&me, &req, 2, None).await.expect("db").expect("view");
        let cursor = page.inner()["cursor"].as_str().expect("cursor").to_string();
        let rest = h.get_bookmarks(&me, &req, 2, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(rest.inner()["bookmarks"][0]["subject"]["uri"], json!(first));
        assert!(rest.inner().get("cursor").is_none());
        for _ in 0..2 {
            h.delete_bookmark(&me, &first).await.expect("db").expect("removed");
        }
        let l = h.get_bookmarks(&me, &req, 50, None).await.expect("db").expect("view");
        assert_eq!(l.inner()["bookmarks"].as_array().map(Vec::len), Some(2));
        // `bookmarkCount` is not shown by default. With a floor of 2 it
        // appears only once two members bookmarked the post; with "always"
        // a post nobody bookmarked says 0.
        use crate::config::BookmarkCount;
        use crate::hydration::testing::{hydrator_configured, test_config};
        exec(p, "DELETE FROM labels").await;
        let anon = Viewer::anonymous();
        let count = |h: &Hydrator, uri: &String| {
            let (h, uri, anon, req) = (h.clone(), uri.clone(), anon.clone(), req.clone());
            async move {
                let v = h.get_posts(&anon, &req, std::slice::from_ref(&uri)).await.expect("db").expect("view");
                v.inner()["posts"][0].get("bookmarkCount").cloned()
            }
        };
        let with = |emit: BookmarkCount| {
            hydrator_configured(p, ScopeSets::default(), crate::hydration::HydrationConfig { bookmarks_emit_count: emit, ..test_config() })
        };
        assert_eq!(count(&h, &second).await, None);
        let floor = with(BookmarkCount::AboveFloor);
        assert_eq!(count(&floor, &second).await, None);
        exec(p, &format!("INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ('{BOB}', '{second}', 'bafy')")).await;
        assert_eq!(count(&floor, &second).await, Some(json!(2)));
        let always = with(BookmarkCount::Always);
        assert_eq!(count(&always, &first).await, Some(json!(0)));
        exec(p, &format!("DELETE FROM bookmarks WHERE viewer_did = '{BOB}'")).await;

        // Another member's bookmarks are its own.
        let other = Viewer {
            did: Some(BOB.into()),
            hydrate: true,
        };
        let l = h.get_bookmarks(&other, &req, 50, None).await.expect("db").expect("view");
        assert_eq!(l.inner()["bookmarks"], json!([]));
        db.drop_db().await;
    }
}
