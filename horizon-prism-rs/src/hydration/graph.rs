//! Graph views (design-v2 §7.1): `getFollows` (local), `getFollowers`
//! (Constellation first), `getMutes`, `muteActor`, `unmuteActor` (local
//! `mutes`).

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::feed::{local_cursor, parse_local_cursor};
use super::{Hydrator, ViewError, ViewResult, Viewer};
use crate::moderation::{self, Gate};

pub const NSID_FOLLOW: &str = "app.bsky.graph.follow";

fn not_found() -> ViewError {
    ViewError::new("InvalidRequest", "Profile not found")
}

impl Hydrator {
    async fn subject(&self, actor: &str) -> Result<Result<String, ViewError>, sqlx::Error> {
        let Some(did) = self.resolve_actor(actor).await? else {
            return Ok(Err(not_found()));
        };
        if let Some((error, message)) = moderation::gate(self.pool(), &did).await?.profile_error() {
            return Ok(Err(ViewError::new(error, message)));
        }
        Ok(Ok(did))
    }

    /// Render a subject plus a list of actors as `profileView`s, dropping
    /// hidden actors and blocks in either direction.
    async fn actor_list(&self, viewer: &Viewer, subject: &str, dids: Vec<String>, key: &str, cursor: Option<String>) -> ViewResult<Value> {
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        let mut all = dids.clone();
        all.push(subject.to_string());
        let ctx = self.profile_context(viewer, &all, None).await?;
        let list: Vec<Value> = dids
            .iter()
            .filter(|d| !gates.get(*d).copied().unwrap_or(Gate::Visible).is_hidden() && !ctx.viewer.blocked_either_way(d))
            .map(|d| ctx.view(d, ProfileKind::View))
            .collect();
        let mut out = json!({ "subject": ctx.view(subject, ProfileKind::View) });
        out[key] = Value::Array(list);
        if let Some(c) = cursor {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.graph.getFollows`: local `follows` by author.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_follows(&self, viewer: &Viewer, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let did = match self.subject(actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        if !self.in_scope(&did).await {
            return Ok(match self.upstream().follows(&did, limit, cursor).await {
                Some(v) => Ok(Self::emit(self.normalized(viewer, v).await?.0)),
                None => Err(not_found()),
            });
        }
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT subject_did, uri, created_at FROM follows WHERE author_did = $1 \
             AND ($2::timestamptz IS NULL OR (created_at, uri) < ($2, $3)) \
             ORDER BY created_at DESC, uri DESC LIMIT $4",
        )
        .bind(&did)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(_, u, t)| local_cursor(*t, u)))
            .flatten();
        let dids = rows.into_iter().map(|(d, _, _)| d).collect();
        self.actor_list(viewer, &did, dids, "follows", next).await
    }

    /// `app.bsky.graph.getFollowers`: Constellation first for every subject,
    /// local `follows` only when Constellation is unavailable (§7.1).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_followers(&self, viewer: &Viewer, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let did = match self.subject(actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        if !cursor.is_some_and(|c| c.starts_with("L:"))
            && let Some(client) = self.backlinks()
        {
            let remote = cursor.and_then(|c| c.strip_prefix("C:"));
            match client.distinct_dids_page(&did, NSID_FOLLOW, ".subject", limit, remote).await {
                Ok(page) => {
                    let next = page.cursor.map(|c| format!("C:{c}"));
                    return self.actor_list(viewer, &did, page.linking_dids, "followers", next).await;
                }
                Err(error) => tracing::debug!(event = "constellation_followers_failed", %error, "falling back to local follows"),
            }
        }
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT author_did, uri, created_at FROM follows WHERE subject_did = $1 \
             AND ($2::timestamptz IS NULL OR (created_at, uri) < ($2, $3)) \
             ORDER BY created_at DESC, uri DESC LIMIT $4",
        )
        .bind(&did)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(_, u, t)| local_cursor(*t, u)))
            .flatten();
        let dids = rows.into_iter().map(|(d, _, _)| d).collect();
        self.actor_list(viewer, &did, dids, "followers", next).await
    }

    /// `app.bsky.graph.getMutes` for the (member) viewer.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_mutes(&self, viewer: &Viewer, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT subject_did, created_at FROM mutes WHERE viewer_did = $1 \
             AND ($2::timestamptz IS NULL OR (created_at, subject_did) < ($2, $3)) \
             ORDER BY created_at DESC, subject_did DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(d, t)| local_cursor(*t, d)))
            .flatten();
        let dids: Vec<String> = rows.into_iter().map(|(d, _)| d).collect();
        let ctx = self.profile_context(viewer, &dids, None).await?;
        let mut out = json!({ "mutes": dids.iter().map(|d| ctx.view(d, ProfileKind::View)).collect::<Vec<_>>() });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.graph.muteActor` / `unmuteActor` for the (member) viewer.
    /// Mutes are local AppView state; any valid DID may be muted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn set_mute(&self, viewer: &Viewer, actor: &str, mute: bool) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let Some(did) = self.resolve_actor(actor).await? else {
            return Ok(Err(ViewError::new("InvalidRequest", "Actor not found")));
        };
        if did == me {
            return Ok(Err(ViewError::new("InvalidRequest", "Cannot mute oneself")));
        }
        let sql = if mute {
            "INSERT INTO mutes (viewer_did, subject_did) VALUES ($1, $2) ON CONFLICT DO NOTHING"
        } else {
            "DELETE FROM mutes WHERE viewer_did = $1 AND subject_did = $2"
        };
        sqlx::query(sql).bind(me).bind(&did).execute(self.pool()).await?;
        Ok(Ok(Self::emit(json!({}))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    #[tokio::test]
    async fn follows_followers_and_mutes() {
        let Some(db) = fresh_db("hydration_graph").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        exec(p, "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/1', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', '3k', now())").await;
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        sets.scope.insert(BOB.into());
        let h = hydrator(p, sets);
        let anon = Viewer::anonymous();
        let f = h.get_follows(&anon, "alice.test", 50, None).await.expect("db").expect("view");
        assert_eq!(f.inner()["follows"][0]["did"], BOB);
        assert_eq!(f.inner()["subject"]["did"], ALICE);
        let f = h.get_followers(&anon, BOB, 50, None).await.expect("db").expect("view");
        assert_eq!(f.inner()["followers"][0]["did"], ALICE);
        let e = h.get_follows(&anon, "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz", 50, None).await.expect("db").expect_err("miss");
        assert_eq!(e.error, "InvalidRequest");

        let me = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        h.set_mute(&me, "bob.test", true).await.expect("db").expect("muted");
        h.set_mute(&me, "bob.test", true).await.expect("db").expect("idempotent");
        let m = h.get_mutes(&me, 50, None).await.expect("db").expect("view");
        assert_eq!(m.inner()["mutes"][0]["did"], BOB);
        assert_eq!(m.inner()["mutes"][0]["viewer"]["muted"], true);
        assert!(h.set_mute(&me, ALICE, true).await.expect("db").is_err());
        h.set_mute(&me, BOB, false).await.expect("db").expect("unmuted");
        h.set_mute(&me, BOB, false).await.expect("db").expect("unmute is idempotent");
        let m = h.get_mutes(&me, 50, None).await.expect("db").expect("view");
        assert_eq!(m.inner()["mutes"], json!([]));
        db.drop_db().await;
    }
}
