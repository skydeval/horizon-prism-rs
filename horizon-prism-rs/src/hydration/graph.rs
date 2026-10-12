//! Graph views (design-v2 §7.1): `getFollows` (local), `getFollowers`
//! (Constellation first), `getMutes`, `muteActor`, `unmuteActor` (local
//! `mutes`).

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::feed::{local_cursor, parse_local_cursor};
use super::{Hydrator, ViewError, ViewResult, Viewer};
use crate::moderation::{self, Gate};
use crate::xrpc::pools::Requester;

pub const NSID_FOLLOW: &str = "app.bsky.graph.follow";

/// What a direct mute covers (design-v3 §5.3): `mutes.scope`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MuteScope {
    Full,
    Reposts,
    Quotes,
    RepostsAndQuotes,
}

impl MuteScope {
    /// From `muteActor`'s `onlyReposts` and `onlyQuoteposts` flags: full
    /// when neither is set.
    #[must_use]
    pub fn from_flags(only_reposts: bool, only_quoteposts: bool) -> Self {
        match (only_reposts, only_quoteposts) {
            (false, false) => Self::Full,
            (true, false) => Self::Reposts,
            (false, true) => Self::Quotes,
            (true, true) => Self::RepostsAndQuotes,
        }
    }

    /// The stored value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Reposts => "reposts",
            Self::Quotes => "quotes",
            Self::RepostsAndQuotes => "reposts_and_quotes",
        }
    }
}

fn not_found() -> ViewError {
    ViewError::new("InvalidRequest", "Profile not found")
}

impl Hydrator {
    async fn subject(&self, req: &Requester, actor: &str) -> Result<Result<String, ViewError>, sqlx::Error> {
        let Some(did) = self.resolve_actor(req, actor).await? else {
            return Ok(Err(not_found()));
        };
        if let Some((error, message)) = moderation::gate(self.pool(), &did).await?.profile_error() {
            return Ok(Err(ViewError::new(error, message)));
        }
        Ok(Ok(did))
    }

    /// Render a subject plus a list of actors as `profileView`s, dropping
    /// hidden actors and blocks in either direction.
    async fn actor_list(&self, viewer: &Viewer, req: &Requester, subject: &str, dids: Vec<String>, key: &str, cursor: Option<String>) -> ViewResult<Value> {
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        let mut all = dids.clone();
        all.push(subject.to_string());
        let ctx = self.profile_context(viewer, req, &all, None).await?;
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
    pub async fn get_follows(&self, viewer: &Viewer, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let did = match self.subject(req, actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        if !self.in_scope(&did).await {
            return Ok(match self.upstream().follows(req, &did, limit, cursor).await {
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
        self.actor_list(viewer, req, &did, dids, "follows", next).await
    }

    /// `app.bsky.graph.getFollowers`: Constellation first for every subject,
    /// local `follows` only when Constellation is unavailable (§7.1).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_followers(&self, viewer: &Viewer, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let did = match self.subject(req, actor).await? {
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
                    return self.actor_list(viewer, req, &did, page.linking_dids, "followers", next).await;
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
        self.actor_list(viewer, req, &did, dids, "followers", next).await
    }

    /// `app.bsky.graph.getMutes` for the (member) viewer.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_mutes(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
            // Full mutes only: the lexicon excludes scoped ones (§5.3).
            "SELECT subject_did, created_at FROM mutes WHERE viewer_did = $1 AND scope = 'full' \
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
        let ctx = self.profile_context(viewer, req, &dids, None).await?;
        let mut out = json!({ "mutes": dids.iter().map(|d| ctx.view(d, ProfileKind::View)).collect::<Vec<_>>() });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.graph.muteActor` (`Some(scope)`) / `unmuteActor` (`None`)
    /// for the (member) viewer. Mutes are local AppView state; any valid DID
    /// may be muted. A repeat mute replaces the scope and keeps the row's
    /// `created_at`; an unmute deletes the row whatever its scope (§5.3).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn set_mute(&self, viewer: &Viewer, req: &Requester, actor: &str, mute: Option<MuteScope>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let Some(did) = self.resolve_actor(req, actor).await? else {
            return Ok(Err(ViewError::new("InvalidRequest", "Actor not found")));
        };
        if did == me {
            return Ok(Err(ViewError::new("InvalidRequest", "Cannot mute oneself")));
        }
        match mute {
            Some(scope) => {
                sqlx::query(
                    "INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ($1, $2, $3) \
                     ON CONFLICT (viewer_did, subject_did) \
                     DO UPDATE SET scope = EXCLUDED.scope, created_at = mutes.created_at",
                )
                .bind(me)
                .bind(&did)
                .bind(scope.as_str())
                .execute(self.pool())
                .await?;
            }
            None => {
                sqlx::query("DELETE FROM mutes WHERE viewer_did = $1 AND subject_did = $2")
                    .bind(me)
                    .bind(&did)
                    .execute(self.pool())
                    .await?;
            }
        }
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
        let req = Requester::test();
        let f = h.get_follows(&anon, &req,"alice.test", 50, None).await.expect("db").expect("view");
        assert_eq!(f.inner()["follows"][0]["did"], BOB);
        assert_eq!(f.inner()["subject"]["did"], ALICE);
        let f = h.get_followers(&anon, &req,BOB, 50, None).await.expect("db").expect("view");
        assert_eq!(f.inner()["followers"][0]["did"], ALICE);
        let e = h.get_follows(&anon, &req,"did:plc:zzzzzzzzzzzzzzzzzzzzzzzz", 50, None).await.expect("db").expect_err("miss");
        assert_eq!(e.error, "InvalidRequest");

        let me = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let full = Some(MuteScope::Full);
        h.set_mute(&me, &req,"bob.test", full).await.expect("db").expect("muted");
        h.set_mute(&me, &req,"bob.test", full).await.expect("db").expect("idempotent");
        let m = h.get_mutes(&me, &req,50, None).await.expect("db").expect("view");
        assert_eq!(m.inner()["mutes"][0]["did"], BOB);
        assert_eq!(m.inner()["mutes"][0]["viewer"]["muted"], true);
        assert!(h.set_mute(&me, &req,ALICE, full).await.expect("db").is_err());
        h.set_mute(&me, &req,BOB, None).await.expect("db").expect("unmuted");
        h.set_mute(&me, &req,BOB, None).await.expect("db").expect("unmute is idempotent");
        let m = h.get_mutes(&me, &req,50, None).await.expect("db").expect("view");
        assert_eq!(m.inner()["mutes"], json!([]));

        // B-34: scoped mutes. A repeat call replaces the scope and keeps the
        // row's created_at; `getMutes` lists full mutes only.
        let viewer_of_bob = || async {
            let ctx = h.profile_context(&me, &req, &[BOB.to_string()], None).await.expect("ctx");
            ctx.view(BOB, ProfileKind::View)["viewer"].clone()
        };
        let created = || async {
            sqlx::query_scalar::<_, DateTime<Utc>>("SELECT created_at FROM mutes WHERE subject_did = $1")
                .bind(BOB)
                .fetch_one(p)
                .await
                .expect("row")
        };
        h.set_mute(&me, &req,BOB, Some(MuteScope::Reposts)).await.expect("db").expect("scoped");
        let first = created().await;
        let v = viewer_of_bob().await;
        assert_eq!((&v["muted"], &v["mutedOnlyReposts"]), (&json!(false), &json!(true)));
        assert!(v.get("mutedOnlyQuoteposts").is_none());
        let m = h.get_mutes(&me, &req,50, None).await.expect("db").expect("view");
        assert_eq!(m.inner()["mutes"], json!([]));

        h.set_mute(&me, &req,BOB, Some(MuteScope::RepostsAndQuotes)).await.expect("db").expect("both");
        let v = viewer_of_bob().await;
        assert_eq!((&v["mutedOnlyReposts"], &v["mutedOnlyQuoteposts"]), (&json!(true), &json!(true)));

        // No flags: a full mute, and the scoped flags are suppressed.
        h.set_mute(&me, &req,BOB, full).await.expect("db").expect("widened by request");
        let v = viewer_of_bob().await;
        assert_eq!(v["muted"], true);
        assert!(v.get("mutedOnlyReposts").is_none() && v.get("mutedOnlyQuoteposts").is_none());
        let m = h.get_mutes(&me, &req,50, None).await.expect("db").expect("view");
        assert_eq!(m.inner()["mutes"][0]["did"], BOB);

        // Narrowed again; still one row, with its first created_at.
        h.set_mute(&me, &req,BOB, Some(MuteScope::Quotes)).await.expect("db").expect("narrowed");
        let v = viewer_of_bob().await;
        assert_eq!((&v["muted"], &v["mutedOnlyQuoteposts"]), (&json!(false), &json!(true)));
        assert!(v.get("mutedOnlyReposts").is_none());
        assert_eq!(created().await, first);
        h.set_mute(&me, &req,BOB, None).await.expect("db").expect("unmute deletes a scoped row");
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM mutes").fetch_one(p).await.expect("count");
        assert_eq!(rows, 0);
        db.drop_db().await;
    }
}
