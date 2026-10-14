//! Graph views (design-v2 §7.1): `getFollows` (local), `getFollowers`
//! (Constellation first), `getMutes`, `muteActor`, `unmuteActor` (local
//! `mutes`).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::feed::{local_cursor, parse_local_cursor};
use super::{Hydrator, SYNC_BUDGET, ViewError, ViewResult, Viewer};
use crate::moderation::{self, Gate};
use crate::hydration::HydrationError;
use crate::xrpc::pools::Requester;

pub const NSID_FOLLOW: &str = "app.bsky.graph.follow";
/// `known_followers_scan_truncated_total`: scans that stopped before the
/// end of the subject's followers, so the answer may be short.
pub const METRIC_KNOWN_FOLLOWERS_TRUNCATED: &str = "known_followers_scan_truncated_total";
/// Followers asked of Constellation per page of a known-followers scan.
const KNOWN_FOLLOWERS_PAGE: u32 = 100;
/// Followers a profile's `viewer.knownFollowers` shows (lexicon maximum).
const KNOWN_FOLLOWERS_SHOWN: usize = 5;

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
    pub(super) async fn subject(&self, req: &Requester, actor: &str) -> Result<Result<String, ViewError>, sqlx::Error> {
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
                Some(v) => Ok(Self::emit(self.normalized(viewer, req, v).await?.0)),
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
            // No budget is a 429, never the local fallback (design-v3
            // §II-8.3).
            self.draw_constellation(req).map_err(HydrationError::NoBudget)?;
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

    /// `app.bsky.graph.getBlocks`: the (member) viewer's own
    /// `app.bsky.graph.block` records, newest first, each with
    /// `viewer.blocking`. Private blocks are in another table and never
    /// listed (§II-7.5).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_blocks(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT subject_did, uri, created_at FROM blocks WHERE author_did = $1 \
             AND ($2::timestamptz IS NULL OR (created_at, uri) < ($2, $3)) \
             ORDER BY created_at DESC, uri DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(_, u, t)| local_cursor(*t, u)))
            .flatten();
        let dids: Vec<String> = rows.into_iter().map(|(d, _, _)| d).collect();
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        let ctx = self.profile_context(viewer, req, &dids, None).await?;
        let blocks: Vec<Value> = dids
            .iter()
            .filter(|d| !gates.get(*d).copied().unwrap_or(Gate::Visible).is_hidden())
            .map(|d| ctx.view(d, ProfileKind::View))
            .collect();
        let mut out = json!({ "blocks": blocks });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// For each of `subjects`, its followers that the viewer follows, with
    /// blocked accounts removed: those with a stored follow first, newest
    /// first, then the rest by DID. The stored follows are read in one
    /// query; Constellation is scanned for at most `pages` pages a subject.
    /// A subject reached after `deadline` has no entry.
    async fn known_followers_of(
        &self,
        viewer: &Viewer,
        req: &Requester,
        me: &str,
        subjects: &[String],
        pages: u32,
        deadline: Option<Instant>,
    ) -> Result<HashMap<String, Vec<String>>, sqlx::Error> {
        let mine: HashSet<String> = sqlx::query_scalar("SELECT subject_did FROM follows WHERE author_did = $1")
            .bind(me)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .collect();
        let stored: Vec<(String, String)> = sqlx::query_as(
            "SELECT subject_did, author_did FROM follows WHERE subject_did = ANY($2) \
               AND author_did IN (SELECT subject_did FROM follows WHERE author_did = $1) \
             ORDER BY created_at DESC, author_did",
        )
        .bind(me)
        .bind(subjects)
        .fetch_all(self.pool())
        .await?;
        let mut by_subject: HashMap<String, Vec<String>> = HashMap::new();
        for (subject, author) in stored {
            by_subject.entry(subject).or_default().push(author);
        }
        let mut out: HashMap<String, Vec<String>> = HashMap::new();
        for subject in subjects {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                continue;
            }
            let mut known = by_subject.remove(subject).unwrap_or_default();
            // Followers this AppView does not store, from Constellation: a
            // bounded scan, each page charged to the caller's share.
            let mut remote: Vec<String> = Vec::new();
            if let Some(client) = self.backlinks() {
                let mut cursor: Option<String> = None;
                let mut truncated = false;
                for page in 0..pages {
                    if self.draw_constellation(req).is_err() {
                        truncated = true;
                        break;
                    }
                    let Ok(found) = client.distinct_dids_page(subject, NSID_FOLLOW, ".subject", KNOWN_FOLLOWERS_PAGE, cursor.as_deref()).await else {
                        break;
                    };
                    remote.extend(found.linking_dids.into_iter().filter(|d| mine.contains(d)));
                    cursor = found.cursor;
                    if cursor.is_none() {
                        break;
                    }
                    truncated = page + 1 == pages;
                }
                if truncated {
                    metrics::counter!(METRIC_KNOWN_FOLLOWERS_TRUNCATED).increment(1);
                }
            }
            let stored: HashSet<String> = known.iter().cloned().collect();
            remote.retain(|d| !stored.contains(d));
            remote.sort_unstable();
            remote.dedup();
            known.extend(remote);
            known.dedup();
            out.insert(subject.clone(), known);
        }
        let mut all: Vec<String> = out.values().flatten().cloned().collect();
        all.sort_unstable();
        all.dedup();
        let state = self.actor_viewer(viewer, &all).await?;
        for (subject, known) in &mut out {
            known.retain(|d| d != me && d != subject && !state.blocked_either_way(d));
        }
        Ok(out)
    }

    /// `viewer.knownFollowers` for a ready-member viewer on the detailed
    /// views of `subjects` (design-v3 §8.1): `getKnownFollowers`' set, with
    /// the Constellation scan cut to `known_followers.profile_scan_pages`
    /// and no subject begun once the request's single-object budget is
    /// spent. `count` is the whole set; `followers` shows at most five. A
    /// subject with none, and the viewer itself, have no entry.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(super) async fn profile_known_followers(&self, viewer: &Viewer, req: &Requester, subjects: &[String], started: Instant) -> Result<HashMap<String, Value>, sqlx::Error> {
        let Some(me) = viewer.state_did() else {
            return Ok(HashMap::new());
        };
        let subjects: Vec<String> = subjects.iter().filter(|s| s.as_str() != me).cloned().collect();
        if subjects.is_empty() {
            return Ok(HashMap::new());
        }
        let pages = self.inner.cfg.known_followers_profile_scan_pages;
        let found = self.known_followers_of(viewer, req, me, &subjects, pages, Some(started + SYNC_BUDGET)).await?;
        let mut all: Vec<String> = found.values().flatten().cloned().collect();
        all.sort_unstable();
        all.dedup();
        let gates = moderation::gate_many(self.pool(), &all).await?;
        let found: Vec<(String, Vec<String>)> = found
            .into_iter()
            .map(|(subject, mut known)| {
                known.retain(|d| !gates.get(d).copied().unwrap_or(Gate::Visible).is_hidden());
                (subject, known)
            })
            .filter(|(_, known)| !known.is_empty())
            .collect();
        let mut shown: Vec<String> = found.iter().flat_map(|(_, known)| known.iter().take(KNOWN_FOLLOWERS_SHOWN)).cloned().collect();
        shown.sort_unstable();
        shown.dedup();
        let ctx = self.profile_context(viewer, req, &shown, None).await?;
        Ok(found
            .into_iter()
            .map(|(subject, known)| {
                let followers: Vec<Value> = known.iter().take(KNOWN_FOLLOWERS_SHOWN).map(|d| ctx.view(d, ProfileKind::Basic)).collect();
                (subject, json!({"count": known.len(), "followers": followers}))
            })
            .collect())
    }

    /// `app.bsky.graph.getKnownFollowers` for a ready-member viewer
    /// (design-v3 §5.3): the accounts the viewer follows that follow
    /// `actor`. The set is built once per (viewer, subject) and paged from
    /// a snapshot held for the read-time cache's lifetime.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_known_followers(&self, viewer: &Viewer, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.state_did().map(str::to_string) else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let did = match self.subject(req, actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        let key = format!("rt:known-followers:{me}:{did}");
        let offset = cursor.and_then(|c| c.parse::<usize>().ok()).unwrap_or(0);
        // A first page always rebuilds; later pages read the snapshot.
        let snapshot = if offset > 0 { self.cache().get_json::<Vec<String>>(&key).await } else { None };
        let all = match snapshot {
            Some(all) => all,
            None => {
                let pages = self.inner.cfg.known_followers_scan_max_pages;
                let all = self
                    .known_followers_of(viewer, req, &me, std::slice::from_ref(&did), pages, None)
                    .await?
                    .remove(&did)
                    .unwrap_or_default();
                self.cache().put_json(&key, &all).await;
                all
            }
        };
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let page: Vec<String> = all.iter().skip(offset).take(limit).cloned().collect();
        let next = (offset + page.len() < all.len()).then(|| (offset + page.len()).to_string());
        self.actor_list(viewer, req, &did, page, "followers", next).await
    }

    /// `app.bsky.graph.muteThread` (`mute`) / `unmuteThread` for the
    /// (member) viewer (design-v3 §5.3). `root` is the thread's root post.
    /// It is not looked up: a thread mute is the viewer's private state, so
    /// the call makes no fetch and answers the same for a post that exists
    /// and one that does not. A member holds at most
    /// `thread_mutes.max_per_actor` thread mutes.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn set_thread_mute(&self, viewer: &Viewer, root: &str, mute: bool) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        if !is_post_uri(root) {
            return Ok(Err(ViewError::new("InvalidRequest", "root must be the at-uri of a post")));
        }
        if !mute {
            sqlx::query("DELETE FROM thread_mutes WHERE viewer_did = $1 AND root_uri = $2")
                .bind(me)
                .bind(root)
                .execute(self.pool())
                .await?;
            return Ok(Ok(Self::emit(json!({}))));
        }
        // A repeat mute is not a new row, so it passes at the cap.
        let stored: bool = sqlx::query_scalar(
            "WITH ins AS ( \
               INSERT INTO thread_mutes (viewer_did, root_uri) \
               SELECT $1, $2 WHERE (SELECT count(*) FROM thread_mutes WHERE viewer_did = $1) < $3 \
               ON CONFLICT (viewer_did, root_uri) DO NOTHING RETURNING 1) \
             SELECT EXISTS (SELECT 1 FROM ins) \
                 OR EXISTS (SELECT 1 FROM thread_mutes WHERE viewer_did = $1 AND root_uri = $2)",
        )
        .bind(me)
        .bind(root)
        .bind(i64::from(self.inner.cfg.thread_mutes_max))
        .fetch_one(self.pool())
        .await?;
        if !stored {
            return Ok(Err(ViewError::new("InvalidRequest", "thread mute limit reached")));
        }
        Ok(Ok(Self::emit(json!({}))))
    }

    /// Which of `uris` are in a thread the ready-member viewer muted: a
    /// stored post whose root is muted, or any URI that is itself a muted
    /// root. Empty for every other viewer.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn in_muted_threads(&self, viewer: &Viewer, uris: &[String]) -> Result<HashSet<String>, sqlx::Error> {
        let Some(me) = viewer.state_did() else {
            return Ok(HashSet::new());
        };
        if uris.is_empty() {
            return Ok(HashSet::new());
        }
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT u.uri FROM unnest($2::text[]) AS u(uri) \
             LEFT JOIN posts p ON p.uri = u.uri \
             JOIN thread_mutes t ON t.viewer_did = $1 AND t.root_uri = COALESCE(p.reply_root_uri, u.uri)",
        )
        .bind(me)
        .bind(uris)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().collect())
    }
}

/// Whether `uri` is `at://<did>/app.bsky.feed.post/<rkey>`.
fn is_post_uri(uri: &str) -> bool {
    let Some(rest) = uri.strip_prefix("at://") else {
        return false;
    };
    let mut parts = rest.split('/');
    let (Some(did), Some(collection), Some(rkey), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    did.starts_with("did:") && collection == "app.bsky.feed.post" && !rkey.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    /// B-55: the accounts the viewer follows that follow the subject.
    #[tokio::test]
    async fn known_followers_are_the_viewers_follows_that_follow_the_subject() {
        let Some(db) = fresh_db("hydration_known_followers").await else {
            return;
        };
        let p = &db.pool;
        let (carol, dave, erin) = ("did:plc:cccccccccccccccccccccccc", "did:plc:dddddddddddddddddddddddd", "did:plc:eeeeeeeeeeeeeeeeeeeeeeee");
        for (d, h) in [(ALICE, "alice.test"), (BOB, "bob.test"), (carol, "carol.test"), (dave, "dave.test"), (erin, "erin.test")] {
            actor(p, d, h, h).await;
        }
        // Alice follows Bob, Carol and Erin. Bob, Carol, Erin and a
        // stranger follow Dave. Erin blocks Alice.
        let mut n = 0;
        for (author, subject, age) in [(ALICE, BOB, 0), (ALICE, carol, 0), (ALICE, erin, 0), (BOB, dave, 2), (carol, dave, 1), (erin, dave, 0), ("did:plc:ssssssssssssssssssssssss", dave, 0)] {
            n += 1;
            exec(p, &format!(
                "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{author}/app.bsky.graph.follow/{n}', '{author}', '{subject}', '3k', now() - interval '{age} minutes')"
            )).await;
        }
        exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{erin}/app.bsky.graph.block/1', '{erin}', '{ALICE}', '3k', now())")).await;
        let h = hydrator(p, ScopeSets::default());
        let me = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let req = Requester::test();
        let v = h.get_known_followers(&me, &req, "dave.test", 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["subject"]["did"], dave);
        let dids: Vec<&str> = v.inner()["followers"].as_array().expect("followers").iter().map(|f| f["did"].as_str().expect("did")).collect();
        assert_eq!(dids, [carol, BOB], "newest follow first; the blocker and the stranger are left out");
        assert!(v.inner().get("cursor").is_none());
        // Paged from the snapshot with an offset cursor.
        let first = h.get_known_followers(&me, &req, dave, 1, None).await.expect("db").expect("view");
        assert_eq!(first.inner()["cursor"], "1");
        let second = h.get_known_followers(&me, &req, dave, 1, Some("1")).await.expect("db").expect("view");
        assert_eq!(second.inner()["followers"][0]["did"], BOB);
        assert!(second.inner().get("cursor").is_none());
        // Only a ready member has follows to compare.
        assert!(h.get_known_followers(&Viewer::anonymous(), &req, dave, 50, None).await.expect("db").is_err());
        db.drop_db().await;
    }

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

        // B-56: getBlocks lists the viewer's own blocks, newest first, with
        // the block record's URI, and never anyone else's.
        let carol = "did:plc:cccccccccccccccccccccccc";
        actor(p, carol, "carol.test", "Carol").await;
        for (author, subject, rkey, age) in [(ALICE, BOB, "b1", 2), (ALICE, carol, "b2", 1), (BOB, ALICE, "b3", 0)] {
            exec(p, &format!(
                "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{author}/app.bsky.graph.block/{rkey}', '{author}', '{subject}', '3k', now() - interval '{age} minutes')"
            )).await;
        }
        let b = h.get_blocks(&me, &req, 50, None).await.expect("db").expect("view");
        let blocks = b.inner()["blocks"].as_array().expect("blocks");
        assert_eq!(blocks.iter().map(|v| v["did"].as_str().expect("did")).collect::<Vec<_>>(), [carol, BOB]);
        assert_eq!(blocks[0]["viewer"]["blocking"], format!("at://{ALICE}/app.bsky.graph.block/b2"));
        assert_eq!(blocks[1]["viewer"]["blockedBy"], true);
        assert!(b.inner().get("cursor").is_none());
        let first = h.get_blocks(&me, &req, 1, None).await.expect("db").expect("view");
        let cursor = first.inner()["cursor"].as_str().expect("cursor").to_string();
        let second = h.get_blocks(&me, &req, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(second.inner()["blocks"][0]["did"], BOB);
        assert!(h.get_blocks(&anon, &req, 50, None).await.expect("db").is_err());
        db.drop_db().await;
    }
}
