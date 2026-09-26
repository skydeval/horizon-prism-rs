//! Notification reads (design-v2 §7.7): `listNotifications`,
//! `getUnreadCount`, `updateSeen`, for a member viewer.
//!
//! - Muted authors and authors with an `app.bsky` block in either direction
//!   are excluded.
//! - `isRead = indexed_at <= notification_seen.seen_at`.
//! - `updateSeen` sets `seen_at = GREATEST(existing, seenAt)`, clamped to
//!   `now()`.

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::{Hydrator, ViewError, ViewResult, Viewer, ts};

/// Cursor: `<indexed_at>::<id>`.
fn cursor_of(at: DateTime<Utc>, id: i64) -> String {
    format!("{}::{id}", at.to_rfc3339_opts(SecondsFormat::Micros, true))
}

fn parse_cursor(c: Option<&str>) -> (Option<DateTime<Utc>>, i64) {
    c.and_then(|c| c.split_once("::"))
        .and_then(|(t, id)| Some((DateTime::parse_from_rfc3339(t).ok()?.with_timezone(&Utc), id.parse().ok()?)))
        .map_or((None, 0), |(t, id)| (Some(t), id))
}

const EXCLUSIONS: &str = "NOT EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.subject_did = n.author_did) \
     AND NOT EXISTS (SELECT 1 FROM blocks b WHERE (b.author_did = $1 AND b.subject_did = n.author_did) \
                     OR (b.author_did = n.author_did AND b.subject_did = $1))";

#[derive(Debug, sqlx::FromRow)]
struct NotificationRow {
    id: i64,
    author_did: String,
    reason: String,
    reason_subject: Option<String>,
    record_uri: String,
    record_cid: String,
    indexed_at: DateTime<Utc>,
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
            let rows: Vec<(String, String, Option<String>, DateTime<Utc>)> = sqlx::query_as(&format!(
                "SELECT uri, subject_uri, subject_cid, created_at FROM {table} WHERE uri = ANY($1)"
            ))
            .bind(&uris)
            .fetch_all(self.pool())
            .await?;
            for (uri, subject, cid, created) in rows {
                out.insert(
                    uri,
                    json!({"$type": ty, "subject": {"uri": subject, "cid": cid.unwrap_or_default()}, "createdAt": ts(created)}),
                );
            }
        }
        let follows: Vec<(String, String, DateTime<Utc>)> =
            sqlx::query_as("SELECT uri, subject_did, created_at FROM follows WHERE uri = ANY($1)")
                .bind(&uris)
                .fetch_all(self.pool())
                .await?;
        for (uri, subject, created) in follows {
            out.insert(uri, json!({"$type": "app.bsky.graph.follow", "subject": subject, "createdAt": ts(created)}));
        }
        Ok(out)
    }

    /// `app.bsky.notification.listNotifications` (limit ≤ 100).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn list_notifications(&self, viewer: &Viewer, limit: u32, cursor: Option<&str>, reasons: &[String]) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let (at, id) = parse_cursor(cursor);
        let rows: Vec<NotificationRow> = sqlx::query_as(&format!(
            "SELECT n.id, n.author_did, n.reason, n.reason_subject, n.record_uri, n.record_cid, n.indexed_at \
             FROM notifications n WHERE n.recipient_did = $1 AND {EXCLUSIONS} \
               AND ($2::timestamptz IS NULL OR (n.indexed_at, n.id) < ($2, $3)) \
               AND (cardinality($5::text[]) = 0 OR n.reason = ANY($5)) \
             ORDER BY n.indexed_at DESC, n.id DESC LIMIT $4"
        ))
        .bind(me)
        .bind(at)
        .bind(id)
        .bind(i64::from(limit))
        .bind(reasons)
        .fetch_all(self.pool())
        .await?;
        let seen = self.seen_at(me).await?;
        let authors: Vec<String> = rows.iter().map(|r| r.author_did.clone()).collect();
        let ctx = self.profile_context(viewer, &authors, None).await?;
        let records = self.notification_records(&rows).await?;
        let labels = self.load_labels(&rows.iter().map(|r| r.record_uri.clone()).collect::<Vec<_>>()).await?;
        let notifications: Vec<Value> = rows
            .iter()
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
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM notifications n WHERE n.recipient_did = $1 AND {EXCLUSIONS} \
               AND n.indexed_at > COALESCE((SELECT seen_at FROM notification_seen WHERE did = $1), '-infinity')"
        ))
        .bind(me)
        .fetch_one(self.pool())
        .await?;
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
        let l = h.list_notifications(&me, 50, None, &[]).await.expect("db").expect("view");
        let n = l.inner()["notifications"].as_array().expect("list");
        assert_eq!(n.len(), 2);
        assert!(n.iter().any(|x| x["reason"] == "like" && x["record"]["$type"] == "app.bsky.feed.like"));
        assert!(n.iter().all(|x| x["isRead"] == false));
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 2);

        // Reason filter.
        let only = h.list_notifications(&me, 50, None, &["reply".into()]).await.expect("db").expect("view");
        assert_eq!(only.inner()["notifications"].as_array().map(Vec::len), Some(1));

        // updateSeen clamps to now and never moves back.
        h.update_seen(&me, "2099-01-01T00:00:00Z").await.expect("db").expect("seen");
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 0);
        h.update_seen(&me, "2000-01-01T00:00:00Z").await.expect("db").expect("seen");
        assert_eq!(h.unread_count(&me).await.expect("db").expect("view").inner()["count"], 0);
        let l = h.list_notifications(&me, 50, None, &[]).await.expect("db").expect("view");
        assert!(l.inner()["notifications"].as_array().expect("list").iter().all(|x| x["isRead"] == true));

        // Muting Bob hides his notifications.
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{ALICE}', '{BOB}')")).await;
        let l = h.list_notifications(&me, 50, None, &[]).await.expect("db").expect("view");
        assert_eq!(l.inner()["notifications"], json!([]));
        db.drop_db().await;
    }
}
