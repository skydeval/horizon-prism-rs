//! Notification reads (design-v2 §7.7): `listNotifications`,
//! `getUnreadCount`, `updateSeen`, for a member viewer.
//!
//! - Muted authors and authors with an `app.bsky` block in either direction
//!   are excluded, and so is every notification in a thread the viewer
//!   muted (design-v3 §5.3).
//! - `isRead = indexed_at <= notification_seen.seen_at`.
//! - `updateSeen` sets `seen_at = GREATEST(existing, seenAt)`, clamped to
//!   `now()`.

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::{Hydrator, ViewError, ViewResult, Viewer, ts};
use crate::xrpc::pools::Requester;

/// Cursor: `<indexed_at>::<id>`.
fn cursor_of(at: DateTime<Utc>, id: i64) -> String {
    format!("{}::{id}", at.to_rfc3339_opts(SecondsFormat::Micros, true))
}

fn parse_cursor(c: Option<&str>) -> (Option<DateTime<Utc>>, i64) {
    c.and_then(|c| c.split_once("::"))
        .and_then(|(t, id)| Some((DateTime::parse_from_rfc3339(t).ok()?.with_timezone(&Utc), id.parse().ok()?)))
        .map_or((None, 0), |(t, id)| (Some(t), id))
}

// What the unread count leaves out; `listNotifications` applies the same
// rules after hydration.
// - Mutes by scope (design-v3 §5.3, "Where mutes apply"): a full mute
//   excludes every reason, a repost mute the repost reasons, a quote mute
//   `quote`.
// - Blocks in either direction.
// - Muted threads: the notification's record or its subject post is the
//   muted root, or a post under it.
const EXCLUSIONS: &str = "NOT EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.subject_did = n.author_did \
       AND (m.scope = 'full' \
         OR (m.scope IN ('reposts', 'reposts_and_quotes') AND n.reason IN ('repost', 'repost-via-repost')) \
         OR (m.scope IN ('quotes', 'reposts_and_quotes') AND n.reason = 'quote'))) \
     AND NOT EXISTS (SELECT 1 FROM blocks b WHERE (b.author_did = $1 AND b.subject_did = n.author_did) \
                     OR (b.author_did = n.author_did AND b.subject_did = $1)) \
     AND NOT EXISTS (SELECT 1 FROM thread_mutes t WHERE t.viewer_did = $1 AND t.root_uri IN ( \
                       n.record_uri, n.reason_subject, \
                       (SELECT p.reply_root_uri FROM posts p WHERE p.uri = n.record_uri), \
                       (SELECT p.reply_root_uri FROM posts p WHERE p.uri = n.reason_subject)))";

/// The member's notification preferences as a condition on `n` (design-v3
/// §5.6): `hidden` and `follows` are the placeholders of the reasons with
/// `list: false` and of those kept only from accounts the member follows.
fn preference_clause(hidden: usize, follows: usize) -> String {
    format!(
        "n.reason <> ALL(${hidden}::text[]) AND (n.reason <> ALL(${follows}::text[]) \
           OR EXISTS (SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = n.author_did))"
    )
}

/// A like or repost row: URI, subject URI and CID, `created_at`, `via` URI
/// and CID.
type SubjectRow = (String, String, Option<String>, DateTime<Utc>, Option<String>, Option<String>);
/// A follow row: URI, subject, `created_at`, `via` URI and CID.
type FollowRow = (String, String, DateTime<Utc>, Option<String>, Option<String>);

#[derive(Debug, sqlx::FromRow)]
struct NotificationRow {
    id: i64,
    author_did: String,
    reason: String,
    reason_subject: Option<String>,
    record_uri: String,
    record_cid: String,
    indexed_at: DateTime<Utc>,
    /// On a `follow`: the starter pack the follow was made through.
    starter_pack_uri: Option<String>,
}

impl Hydrator {
    async fn seen_at(&self, me: &str) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        sqlx::query_scalar("SELECT seen_at FROM notification_seen WHERE did = $1")
            .bind(me)
            .fetch_optional(self.pool())
            .await
    }

    /// The notification's record: stored post JSON, or the like / repost /
    /// follow record rebuilt from its row.
    async fn notification_records(&self, rows: &[NotificationRow]) -> Result<std::collections::HashMap<String, Value>, sqlx::Error> {
        let uris: Vec<String> = rows.iter().map(|r| r.record_uri.clone()).collect();
        let mut out = std::collections::HashMap::new();
        let posts: Vec<(String, Value)> = sqlx::query_as("SELECT uri, record FROM posts WHERE uri = ANY($1)")
            .bind(&uris)
            .fetch_all(self.pool())
            .await?;
        out.extend(posts);
        for (table, ty) in [("likes", "app.bsky.feed.like"), ("reposts", "app.bsky.feed.repost")] {
            let rows: Vec<SubjectRow> = sqlx::query_as(&format!(
                "SELECT uri, subject_uri, subject_cid, created_at, via_uri, via_cid FROM {table} WHERE uri = ANY($1)"
            ))
            .bind(&uris)
            .fetch_all(self.pool())
            .await?;
            for (uri, subject, cid, created, via_uri, via_cid) in rows {
                let mut record = json!({"$type": ty, "subject": {"uri": subject, "cid": cid.unwrap_or_default()}, "createdAt": ts(created)});
                if let Some(via) = via_uri {
                    record["via"] = json!({"uri": via, "cid": via_cid.unwrap_or_default()});
                }
                out.insert(uri, record);
            }
        }
        let follows: Vec<FollowRow> =
            sqlx::query_as("SELECT uri, subject_did, created_at, via_uri, via_cid FROM follows WHERE uri = ANY($1)")
                .bind(&uris)
                .fetch_all(self.pool())
                .await?;
        for (uri, subject, created, via_uri, via_cid) in follows {
            let mut record = json!({"$type": "app.bsky.graph.follow", "subject": subject, "createdAt": ts(created)});
            if let Some(via) = via_uri {
                record["via"] = json!({"uri": via, "cid": via_cid.unwrap_or_default()});
            }
            out.insert(uri, record);
        }
        Ok(out)
    }

    /// `app.bsky.notification.listNotifications` (limit ≤ 100).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn list_notifications(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>, reasons: &[String]) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let (at, id) = parse_cursor(cursor);
        // The scan applies none of the viewer's private state: every scanned
        // row is hydrated, and the viewer's mutes, blocks and thread mutes
        // drop rows afterwards (design-v3 §8.1 "Hydrate before filtering").
        // So what a member's read fetches and caches does not depend on what
        // it muted. A page can be short; the cursor follows the scan.
        let rows: Vec<NotificationRow> = sqlx::query_as(
            "SELECT n.id, n.author_did, n.reason, n.reason_subject, n.record_uri, n.record_cid, n.indexed_at, n.starter_pack_uri \
             FROM notifications n WHERE n.recipient_did = $1 \
               AND ($2::timestamptz IS NULL OR (n.indexed_at, n.id) < ($2, $3)) \
               AND (cardinality($5::text[]) = 0 OR n.reason = ANY($5)) \
             ORDER BY n.indexed_at DESC, n.id DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(id)
        .bind(i64::from(limit))
        .bind(reasons)
        .fetch_all(self.pool())
        .await?;
        let seen = self.seen_at(me).await?;
        let authors: Vec<String> = rows.iter().map(|r| r.author_did.clone()).collect();
        let ctx = self.profile_context(viewer, req, &authors, None).await?;
        let records = self.notification_records(&rows).await?;
        let labels = self.load_labels(&rows.iter().map(|r| r.record_uri.clone()).collect::<Vec<_>>()).await?;
        let threads: Vec<String> = rows
            .iter()
            .flat_map(|r| std::iter::once(&r.record_uri).chain(&r.reason_subject))
            .cloned()
            .collect();
        let muted_threads = self.in_muted_threads(viewer, &threads).await?;
        // The starter packs follows were made through (§8.6); one that no
        // longer resolves leaves its field out.
        let mut pack_uris: Vec<String> = rows.iter().filter_map(|r| r.starter_pack_uri.clone()).collect();
        pack_uris.sort_unstable();
        pack_uris.dedup();
        let mut packs: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
        for chunk in pack_uris.chunks(25) {
            if let Ok(found) = self.get_starter_packs(viewer, req, chunk).await? {
                for pack in found.inner()["starterPacks"].as_array().into_iter().flatten() {
                    if let Some(uri) = pack.get("uri").and_then(Value::as_str) {
                        packs.insert(uri.to_string(), pack.clone());
                    }
                }
            }
        }
        // The member's preferences are private state too, applied here and
        // not in the scan.
        let prefs = self.reason_filter(me).await?;
        let followed: std::collections::HashSet<String> = if prefs.follows_only.is_empty() {
            std::collections::HashSet::new()
        } else {
            sqlx::query_scalar("SELECT subject_did FROM follows WHERE author_did = $1 AND subject_did = ANY($2)")
                .bind(me)
                .bind(&authors)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .collect()
        };
        let dropped = |r: &NotificationRow| {
            let who = &ctx.viewer;
            who.blocked_either_way(&r.author_did)
                || prefs.hidden.contains(&r.reason)
                || (prefs.follows_only.contains(&r.reason) && !followed.contains(&r.author_did))
                || who.muted.contains(&r.author_did)
                || (who.muted_reposts.contains(&r.author_did) && matches!(r.reason.as_str(), "repost" | "repost-via-repost"))
                || (who.muted_quotes.contains(&r.author_did) && r.reason == "quote")
                || muted_threads.contains(&r.record_uri)
                || r.reason_subject.as_ref().is_some_and(|s| muted_threads.contains(s))
        };
        let notifications: Vec<Value> = rows
            .iter()
            .filter(|r| !dropped(r))
            .filter_map(|r| {
                let record = records.get(&r.record_uri)?.clone();
                let mut n = json!({
                    "uri": r.record_uri,
                    "cid": r.record_cid,
                    "author": ctx.view(&r.author_did, ProfileKind::View),
                    "reason": r.reason,
                    "record": record,
                    "isRead": seen.is_some_and(|s| r.indexed_at <= s),
                    "indexedAt": ts(r.indexed_at),
                    "labels": labels.get(&r.record_uri).cloned().unwrap_or_default(),
                });
                if let Some(s) = &r.reason_subject {
                    n["reasonSubject"] = json!(s);
                }
                if let Some(pack) = r.starter_pack_uri.as_ref().and_then(|u| packs.get(u)) {
                    n["starterPack"] = pack.clone();
                }
                Some(n)
            })
            .collect();
        let mut out = json!({ "notifications": notifications });
        if let Some(s) = seen {
            out["seenAt"] = json!(ts(s));
        }
        if rows.len() == usize::try_from(limit).unwrap_or(usize::MAX)
            && let Some(last) = rows.last()
        {
            out["cursor"] = json!(cursor_of(last.indexed_at, last.id));
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.notification.getUnreadCount`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn unread_count(&self, viewer: &Viewer) -> ViewResult<Value> {
        self.unread_count_since(viewer, None).await
    }

    /// `getUnreadCount` with its `seenAt` parameter (design-v3 §11 G-4):
    /// when present it is the comparison point, in place of the stored
    /// last-seen time.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn unread_count_since(&self, viewer: &Viewer, seen_at: Option<DateTime<Utc>>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let prefs = self.reason_filter(me).await?;
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM notifications n WHERE n.recipient_did = $1 AND {EXCLUSIONS} AND {} \
               AND n.indexed_at > COALESCE($2::timestamptz, \
                   (SELECT seen_at FROM notification_seen WHERE did = $1), '-infinity')",
            preference_clause(3, 4)
        ))
        .bind(me)
        .bind(seen_at)
        .bind(&prefs.hidden)
        .bind(&prefs.follows_only)
        .fetch_one(self.pool())
        .await?;
        // An inbound list block cannot be joined in SQL, so the count is
        // corrected (design-v3 §5.14.7): the unread rows' authors, newest
        // first, as many as the request may ask Farsight about, taken
        // before the viewer's mutes so that what is asked does not depend
        // on them; then the rows of the authors that block the viewer
        // through a list are subtracted, under the count's own exclusions.
        let mut count = count;
        if count > 0
            && let Some(lookup) = crate::xrpc::backstop::farsight()
        {
            let authors: Vec<String> = sqlx::query_scalar(
                "SELECT n.author_did FROM notifications n WHERE n.recipient_did = $1 \
                   AND n.indexed_at > COALESCE($2::timestamptz, \
                       (SELECT seen_at FROM notification_seen WHERE did = $1), '-infinity') \
                   AND NOT EXISTS (SELECT 1 FROM blocks b WHERE (b.author_did = $1 AND b.subject_did = n.author_did) \
                                   OR (b.author_did = n.author_did AND b.subject_did = $1)) \
                 GROUP BY n.author_did ORDER BY max(n.indexed_at) DESC LIMIT $3",
            )
            .bind(me)
            .bind(seen_at)
            .bind(i64::try_from(lookup.capacity()).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await?;
            let blockers: Vec<String> = lookup
                .inbound_list_blocks(me, &authors, &std::collections::HashSet::new())
                .await
                .into_keys()
                .collect();
            if !blockers.is_empty() {
                let hidden: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*) FROM notifications n WHERE n.recipient_did = $1 AND {EXCLUSIONS} AND {} \
                       AND n.indexed_at > COALESCE($2::timestamptz, \
                           (SELECT seen_at FROM notification_seen WHERE did = $1), '-infinity') \
                       AND n.author_did = ANY($3)",
                    preference_clause(4, 5)
                ))
                .bind(me)
                .bind(seen_at)
                .bind(&blockers)
                .bind(&prefs.hidden)
                .bind(&prefs.follows_only)
                .fetch_one(self.pool())
                .await?;
                count = (count - hidden).max(0);
            }
        }
        Ok(Ok(Self::emit(json!({ "count": count }))))
    }

    /// `app.bsky.notification.updateSeen`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn update_seen(&self, viewer: &Viewer, seen_at: &str) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let Ok(t) = DateTime::parse_from_rfc3339(seen_at) else {
            return Ok(Err(ViewError::new("InvalidRequest", "seenAt must be a datetime")));
        };
        sqlx::query(
            "INSERT INTO notification_seen (did, seen_at) VALUES ($1, LEAST($2, now())) \
             ON CONFLICT (did) DO UPDATE SET seen_at = GREATEST(notification_seen.seen_at, LEAST(EXCLUDED.seen_at, now()))",
        )
        .bind(me)
        .bind(t.with_timezone(&Utc))
        .execute(self.pool())
        .await?;
        Ok(Ok(Self::emit(json!({}))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
    const DAVE: &str = "did:plc:dddddddddddddddddddddddd";

    #[tokio::test]
    async fn every_reason_hydrates_its_record_and_blocks_exclude_both_ways() {
        let Some(db) = fresh_db("hydration_notification_reasons").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test"), (DAVE, "dave.test")] {
            actor(p, d, h, h).await;
        }
        let root = post(p, ALICE, "3kroot222222", "root", None, None).await;
        let reply = post(p, BOB, "3kreply22222", "reply", Some(&root), Some(&root)).await;
        let mention = post(p, BOB, "3kmention222", "hi @alice.test", None, None).await;
        let quote = post(p, BOB, "3kquote22222", "quoting", None, None).await;
        let (like, repost, follow) = (
            format!("at://{BOB}/app.bsky.feed.like/3klike"),
            format!("at://{BOB}/app.bsky.feed.repost/3krepost"),
            format!("at://{BOB}/app.bsky.graph.follow/3kfollow"),
        );
        exec(p, &format!("INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES ('{like}', '{BOB}', '{root}', 'bafyroot', '3k', now())")).await;
        exec(p, &format!("INSERT INTO reposts (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES ('{repost}', '{BOB}', '{root}', 'bafyroot', '3k', now())")).await;
        exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('{follow}', '{BOB}', '{ALICE}', '3k', now())")).await;
        for (reason, uri, subject) in [
            ("reply", &reply, Some(&root)),
            ("mention", &mention, None),
            ("quote", &quote, Some(&root)),
            ("like", &like, Some(&root)),
            ("repost", &repost, Some(&root)),
            ("follow", &follow, None),
        ] {
            sqlx::query(
                "INSERT INTO notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
                 VALUES ($1, $2, $3, $4, $5, 'bafy')",
            )
            .bind(ALICE)
            .bind(BOB)
            .bind(reason)
            .bind(subject)
            .bind(uri)
            .execute(p)
            .await
            .expect("notification");
        }
        // Carol blocks Alice; Alice blocks Dave. Both directions exclude.
        for (i, (author, subject, n)) in [(CAROL, ALICE, "carol"), (ALICE, DAVE, "dave")].into_iter().enumerate() {
            exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{author}/app.bsky.graph.block/{i}', '{author}', '{subject}', '3k', now())")).await;
            let other = if author == ALICE { subject } else { author };
            exec(p, &format!("INSERT INTO notifications (recipient_did, author_did, reason, record_uri, record_cid) VALUES ('{ALICE}', '{other}', 'follow', 'at://{other}/app.bsky.graph.follow/{n}', 'bafy')")).await;
        }
        let h = hydrator(p, ScopeSets::default());
        let me = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let req = Requester::test();
        let l = h.list_notifications(&me, &req,100, None, &[]).await.expect("db").expect("view");
        let list = l.inner()["notifications"].as_array().expect("list");
        let by_reason: std::collections::HashMap<&str, &Value> = list.iter().map(|n| (n["reason"].as_str().expect("reason"), n)).collect();
        assert_eq!(list.len(), 6, "blocked authors (both directions) are excluded: {list:?}");
        assert!(list.iter().all(|n| n["author"]["did"] == BOB));
        assert_eq!(by_reason["reply"]["record"]["text"], "reply");
        assert_eq!(by_reason["reply"]["reasonSubject"], json!(root));
        assert_eq!(by_reason["mention"]["record"]["text"], "hi @alice.test");
        assert_eq!(by_reason["quote"]["record"]["text"], "quoting");
        assert_eq!(by_reason["like"]["record"], json!({"$type": "app.bsky.feed.like", "subject": {"uri": root, "cid": "bafyroot"}, "createdAt": by_reason["like"]["record"]["createdAt"]}));
        assert_eq!(by_reason["repost"]["record"]["$type"], "app.bsky.feed.repost");
        assert_eq!(by_reason["repost"]["record"]["subject"]["uri"], json!(root));
        assert_eq!(by_reason["follow"]["record"]["subject"], ALICE);
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 6, "the count uses the same exclusions");

        // Paging with the indexed_at::id cursor. The cursor follows the scan,
        // which includes the two rows the blocks drop: the first page is
        // short, and nothing is lost.
        let first = h.list_notifications(&me, &req,4, None, &[]).await.expect("db").expect("view");
        assert_eq!(first.inner()["notifications"].as_array().map(Vec::len), Some(2));
        let cursor = first.inner()["cursor"].as_str().expect("cursor").to_string();
        assert!(cursor.contains("::"));
        let rest = h.list_notifications(&me, &req,4, Some(&cursor), &[]).await.expect("db").expect("view");
        assert_eq!(rest.inner()["notifications"].as_array().map(Vec::len), Some(4));
        let cursor = rest.inner()["cursor"].as_str().expect("cursor").to_string();
        let end = h.list_notifications(&me, &req,4, Some(&cursor), &[]).await.expect("db").expect("view");
        assert_eq!(end.inner()["notifications"], json!([]));
        assert!(end.inner().get("cursor").is_none());
        db.drop_db().await;
    }

    /// B-77: an author who blocks the viewer through a list sends it no
    /// notifications, in the list and in the unread count.
    #[tokio::test]
    async fn an_inbound_list_blocker_is_left_out_of_the_list_and_the_count() {
        use crate::farsight::lookup::tests::engine;
        use crate::hydration::testing::hydrator_with_farsight;
        use crate::xrpc::backstop::{self, Selected};

        let Some(db) = fresh_db("hydration_notifications_list_block").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test")] {
            actor(p, d, h, h).await;
        }
        for (author, n) in [(BOB, 1), (BOB, 2), (CAROL, 3)] {
            exec(p, &format!(
                "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{author}/app.bsky.graph.follow/{n}', '{author}', '{ALICE}', '3k', now())"
            )).await;
            exec(p, &format!(
                "INSERT INTO notifications (recipient_did, author_did, reason, record_uri, record_cid) VALUES \
                 ('{ALICE}', '{author}', 'follow', 'at://{author}/app.bsky.graph.follow/{n}', 'bafy')"
            )).await;
        }
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert(BOB.to_string());
        let h = hydrator_with_farsight(p, ScopeSets::default(), farsight.clone());
        let me = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let req = Requester::test();
        // Without a handle nothing is known of the list block.
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 3);
        let selected = Selected::by_ip("203.0.113.9".parse().expect("ip"));
        selected.attach_farsight(farsight.request(&req));
        backstop::scope(selected, async {
            assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 1);
            let l = h.list_notifications(&me, &req, 50, None, &[]).await.expect("db").expect("view");
            let authors: Vec<&str> = l.inner()["notifications"].as_array().expect("list").iter().map(|n| n["author"]["did"].as_str().expect("did")).collect();
            assert_eq!(authors, [CAROL]);
        })
        .await;
        db.drop_db().await;
    }

    #[tokio::test]
    async fn list_count_seen_and_exclusions() {
        let Some(db) = fresh_db("hydration_notifications").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let root = post(p, ALICE, "3kaaaaaaaaaa2", "root", None, None).await;
        let reply = post(p, BOB, "3kaaaaaaaaaa3", "reply", Some(&root), Some(&root)).await;
        exec(p, &format!("INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES \
                 ('at://{BOB}/app.bsky.feed.like/1', '{BOB}', '{root}', 'bafyroot', '3k', now())")).await;
        exec(p, &format!("INSERT INTO notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) VALUES \
                 ('{ALICE}', '{BOB}', 'reply', '{root}', '{reply}', 'bafyr'), \
                 ('{ALICE}', '{BOB}', 'like', '{root}', 'at://{BOB}/app.bsky.feed.like/1', 'bafyl')")).await;
        let h = hydrator(p, ScopeSets::default());
        let me = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let req = Requester::test();
        let l = h.list_notifications(&me, &req,50, None, &[]).await.expect("db").expect("view");
        let n = l.inner()["notifications"].as_array().expect("list");
        assert_eq!(n.len(), 2);
        assert!(n.iter().any(|x| x["reason"] == "like" && x["record"]["$type"] == "app.bsky.feed.like"));
        assert!(n.iter().all(|x| x["isRead"] == false));
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 2);

        // Reason filter.
        let only = h.list_notifications(&me, &req,50, None, &["reply".into()]).await.expect("db").expect("view");
        assert_eq!(only.inner()["notifications"].as_array().map(Vec::len), Some(1));

        // B-42: preferences are consulted. With no row, the defaults.
        let prefs = h.get_notification_preferences(&me).await.expect("db").expect("view");
        assert_eq!(prefs.inner()["preferences"], crate::hydration::notification_prefs::defaults());
        let reasons = |v: &Value| -> Vec<String> {
            v["notifications"].as_array().expect("list").iter().map(|x| x["reason"].as_str().expect("reason").to_string()).collect()
        };
        // `list: false` takes likes out of the list and the count; `chat`
        // in the input is ignored.
        let put = json!({"like": {"include": "all", "list": false, "push": true}, "chat": {"include": "accepted", "push": false}});
        let stored = h.put_notification_preferences(&me, &put).await.expect("db").expect("stored");
        assert_eq!(stored.inner()["preferences"]["like"]["list"], false);
        assert_eq!(stored.inner()["preferences"]["chat"], json!({"include": "all", "push": true}));
        assert_eq!(stored.inner()["preferences"].as_object().map(serde_json::Map::len), Some(13));
        let l = h.list_notifications(&me, &req, 50, None, &[]).await.expect("db").expect("view");
        assert_eq!(reasons(l.inner()), ["reply"]);
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 1);
        // `include: follows` keeps a reason only from accounts the member
        // follows; another key's put leaves the first one stored.
        let put = json!({"reply": {"include": "follows", "list": true, "push": true}});
        let stored = h.put_notification_preferences(&me, &put).await.expect("db").expect("stored");
        assert_eq!(stored.inner()["preferences"]["like"]["list"], false);
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 0);
        let l = h.list_notifications(&me, &req, 50, None, &[]).await.expect("db").expect("view");
        assert_eq!(l.inner()["notifications"], json!([]));
        exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{ALICE}/app.bsky.graph.follow/9', '{ALICE}', '{BOB}', '3k', now())")).await;
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 1);
        let l = h.list_notifications(&me, &req, 50, None, &[]).await.expect("db").expect("view");
        assert_eq!(reasons(l.inner()), ["reply"]);
        // A bad value is refused and changes nothing.
        let bad = json!({"quote": {"include": "mutuals", "list": true, "push": true}});
        assert_eq!(h.put_notification_preferences(&me, &bad).await.expect("db").expect_err("bad").error, "InvalidRequest");
        let prefs = h.get_notification_preferences(&me).await.expect("db").expect("view");
        assert_eq!(prefs.inner()["preferences"]["reply"]["include"], "follows");
        exec(p, "DELETE FROM notification_preferences_v2").await;
        exec(p, "DELETE FROM follows").await;

        // B-59: a muted thread sends no notifications (the reply under the
        // root, and the like of the root), and its posts say so.
        h.set_thread_mute(&me, &root, true).await.expect("db").expect("muted");
        h.set_thread_mute(&me, &root, true).await.expect("db").expect("idempotent");
        let l = h.list_notifications(&me, &req,50, None, &[]).await.expect("db").expect("view");
        assert_eq!(l.inner()["notifications"], json!([]));
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 0);
        let state = h.post_viewer(&me, &[root.clone(), reply.clone()]).await.expect("db");
        assert_eq!((&state[&root]["threadMuted"], &state[&reply]["threadMuted"]), (&json!(true), &json!(true)));
        let other = Viewer {
            did: Some(BOB.into()),
            hydrate: true,
        };
        assert!(h.post_viewer(&other, std::slice::from_ref(&root)).await.expect("db")[&root].get("threadMuted").is_none());
        // An upstream view of a reply that is not stored takes its root
        // from its record.
        let far = json!({"uri": "at://did:plc:zzzzzzzzzzzzzzzzzzzzzzzz/app.bsky.feed.post/3kfar", "cid": "bafyfar",
            "author": {"did": "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz", "handle": "far.example"},
            "record": {"text": "far reply", "reply": {"root": {"uri": root, "cid": "bafyroot"}, "parent": {"uri": root, "cid": "bafyroot"}}}});
        let (mine, _) = h.normalized(&me, far.clone()).await.expect("db");
        assert_eq!(mine["viewer"]["threadMuted"], true);
        let (theirs, _) = h.normalized(&other, far).await.expect("db");
        assert!(theirs["viewer"].get("threadMuted").is_none());
        h.set_thread_mute(&me, &root, false).await.expect("db").expect("unmuted");
        h.set_thread_mute(&me, &root, false).await.expect("db").expect("unmute is idempotent");
        let l = h.list_notifications(&me, &req,50, None, &[]).await.expect("db").expect("view");
        assert_eq!(l.inner()["notifications"].as_array().map(Vec::len), Some(2));
        assert!(h.post_viewer(&me, std::slice::from_ref(&root)).await.expect("db")[&root].get("threadMuted").is_none());

        // The root is not looked up, must be a post's at-uri, and the cap
        // (3 in tests) refuses a new thread but not a repeat.
        for bad in [
            "not a uri",
            "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.like/1",
            "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/1",
            "at://alice.test/app.bsky.feed.post/1",
            "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/1/extra",
        ] {
            let e = h.set_thread_mute(&me, bad, true).await.expect("db").expect_err(bad);
            assert_eq!(e.error, "InvalidRequest", "{bad}");
        }
        let unknown = |n: u8| format!("at://did:plc:zzzzzzzzzzzzzzzzzzzzzzzz/app.bsky.feed.post/{n}");
        for n in 0..3 {
            h.set_thread_mute(&me, &unknown(n), true).await.expect("db").expect("a post nobody stored");
        }
        let e = h.set_thread_mute(&me, &unknown(3), true).await.expect("db").expect_err("past the cap");
        assert!(e.message.contains("limit"), "{}", e.message);
        h.set_thread_mute(&me, &unknown(0), true).await.expect("db").expect("a repeat at the cap");
        h.set_thread_mute(&other, &unknown(3), true).await.expect("db").expect("another member's own cap");
        exec(p, "DELETE FROM thread_mutes").await;

        // updateSeen clamps to now and never moves back.
        h.update_seen(&me, "2099-01-01T00:00:00Z").await.expect("db").expect("seen");
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 0);
        h.update_seen(&me, "2000-01-01T00:00:00Z").await.expect("db").expect("seen");
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 0);
        // G-4: an explicit seenAt is the comparison point, whatever is stored.
        let since = |t: &str| {
            let t = DateTime::parse_from_rfc3339(t).expect("t").with_timezone(&Utc);
            let h = &h;
            let me = &me;
            async move { h.unread_count_since(me, Some(t)).await.expect("db").expect("view").inner()["count"].clone() }
        };
        assert_eq!(since("2000-01-01T00:00:00Z").await, 2);
        assert_eq!(since("2099-01-01T00:00:00Z").await, 0);
        let l = h.list_notifications(&me, &req,50, None, &[]).await.expect("db").expect("view");
        assert!(l.inner()["notifications"].as_array().expect("list").iter().all(|x| x["isRead"] == true));

        // Muting Bob hides his notifications.
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{ALICE}', '{BOB}')")).await;
        let l = h.list_notifications(&me, &req,50, None, &[]).await.expect("db").expect("view");
        assert_eq!(l.inner()["notifications"], json!([]));

        // Scoped mutes (B-34): a repost mute excludes only the repost
        // reasons, a quote mute only `quote`.
        exec(p, "DELETE FROM mutes").await;
        exec(p, &format!("INSERT INTO notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) VALUES \
                 ('{ALICE}', '{BOB}', 'repost', '{root}', 'at://{BOB}/app.bsky.feed.repost/1', 'bafyp'), \
                 ('{ALICE}', '{BOB}', 'quote', '{root}', 'at://{BOB}/app.bsky.feed.post/3kaaaaaaaaaa9', 'bafyq')")).await;
        let unread = || async {
            h.unread_count(&me).await.expect("db").expect("view").inner()["count"]
                .as_i64()
                .expect("count")
        };
        let base = unread().await;
        assert!(base >= 2, "{base}");
        for (scope, hidden) in [("reposts", 1), ("quotes", 1), ("reposts_and_quotes", 2)] {
            exec(p, "DELETE FROM mutes").await;
            exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{ALICE}', '{BOB}', '{scope}')")).await;
            assert_eq!(unread().await, base - hidden, "{scope}");
        }
        exec(p, &format!("UPDATE mutes SET scope = 'full' WHERE viewer_did = '{ALICE}'")).await;
        assert_eq!(unread().await, 0);
        db.drop_db().await;
    }
}
