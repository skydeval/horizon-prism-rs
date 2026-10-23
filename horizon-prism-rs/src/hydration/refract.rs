//! Views for Refract's endpoints (refract design §7.1, §7.2): built by the
//! ordinary hydration from local rows (or, for a post not stored yet, the
//! cascade and Slingshot), gated, normalized and marked as every public
//! view is. The endpoints' gate, discovery and visitor blocks live in
//! `refract::handlers`; only hydration builds a response body.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::Duration;

use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::feed::{FeedItem, local_cursor, parse_local_cursor};
use super::{ActorRow, CountMode, HydrationError, Hydrator, PublicEmit, Viewer, blob_url, ts};
use crate::db::locks::uri_author;
use crate::hydration::normalize::is_private_uri;
use crate::xrpc::pools::Requester;

/// Restored DIDs Slingshot found no profile record for (refract design
/// §7.2): not walked again for ten minutes. Never the shared negative
/// cache, which a restored DID neither reads nor writes.
static NO_PROFILE: LazyLock<moka::sync::Cache<String, ()>> =
    LazyLock::new(|| moka::sync::Cache::builder().max_capacity(100_000).time_to_live(Duration::from_secs(600)).build());

/// A `profileViewDetailed`-shaped view of a profile record, for the
/// projection the tiers' views take.
fn record_profile(did: &str, row: Option<&ActorRow>, record: &Value) -> Value {
    let pds = row.and_then(|r| r.pds_endpoint.as_deref());
    let mut v = json!({"did": did});
    if let Some(h) = row.and_then(|r| r.handle.as_deref()) {
        v["handle"] = json!(h);
    }
    for (from, to) in [("displayName", "displayName"), ("description", "description"), ("createdAt", "createdAt"), ("pronouns", "pronouns"), ("website", "website")] {
        if let Some(x) = record.get(from).filter(|x| x.is_string()) {
            v[to] = x.clone();
        }
    }
    for key in ["avatar", "banner"] {
        if let Some(url) = record.pointer(&format!("/{key}/ref/$link")).and_then(Value::as_str).and_then(|cid| blob_url(pds, did, cid)) {
            v[key] = json!(url);
        }
    }
    v
}

impl Hydrator {
    /// The remote views of restored DIDs whose profile record is not stored
    /// (yet) (refract design §7.2), whatever tier mode the caller asked
    /// for: cached views, then the cascade (the community's AppView first),
    /// then Slingshot's profile record. The shared negative cache is neither
    /// read nor written and nothing is queued.
    pub(crate) async fn refract_profiles(&self, req: &Requester, dids: &[String], rows: &HashMap<String, ActorRow>) -> HashMap<String, Value> {
        let mut out = HashMap::new();
        if req.is_local_only() || dids.is_empty() {
            return out;
        }
        let snapshot = self.refract_snapshot(req).await;
        let mut bare: Vec<String> = Vec::new();
        for d in dids {
            if snapshot.is_restored(d) && rows.get(d).is_none_or(|r| r.profile_record.is_none()) && !bare.contains(d) && !NO_PROFILE.contains_key(d) {
                bare.push(d.clone());
            }
        }
        if bare.is_empty() {
            return out;
        }
        let cache = self.cache();
        let mut misses = Vec::new();
        for d in bare {
            match cache.get_json::<Value>(req, &format!("{}{d}", super::cache::PROFILE_PREFIX)).await {
                Some(v) => {
                    out.insert(d, v);
                }
                None => misses.push(d),
            }
        }
        if misses.is_empty() {
            return out;
        }
        let walk = req.detached();
        let fetched = self.upstream().fetch_profiles(&walk, &misses, req.deadline()).await;
        for (d, v) in fetched.found {
            cache.put_json(req, &format!("{}{d}", super::cache::PROFILE_PREFIX), &v).await;
            out.insert(d, v);
        }
        let unfound: Vec<String> = misses.into_iter().filter(|d| !out.contains_key(d)).collect();
        for d in unfound {
            // Only Slingshot's `RecordNotFound` is remembered; no answer is
            // asked again next time.
            match self.upstream().record_or_absent(&walk, &format!("at://{d}/app.bsky.actor.profile/self")).await {
                Some(Some(found)) => {
                    let view = record_profile(&d, rows.get(&d), found.get("value").unwrap_or(&Value::Null));
                    out.insert(d, view);
                }
                Some(None) => NO_PROFILE.insert(d, ()),
                None => {}
            }
        }
        out
    }

    /// `refract.fill`'s body: the `profileViewDetailed` of each DID of
    /// `profiles` that renders, the `postView` of each post of `posts`, and
    /// `pending` as the handler decided it. A post whose author the viewer
    /// blocks or is blocked by is left out, and listed in `withheld` (RF-S11)
    /// with the handler's `withheld`: `blocked`, or `unanswered` when a
    /// visitor's pair with the author went unanswered.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_fill(
        &self,
        viewer: &Viewer,
        req: &Requester,
        profiles: &[String],
        posts: &[String],
        pending: &[String],
        withheld: &[(String, &'static str)],
    ) -> Result<PublicEmit<Value>, HydrationError> {
        let mut out_profiles = Vec::new();
        let mut missing_profiles: Vec<String> = Vec::new();
        for did in profiles {
            // No stored profile record and no remote view (none answered,
            // and no `RecordNotFound` remembered): the view would be a bare
            // stub, so the profile is unavailable, not served.
            let stored: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM actors WHERE did = $1 AND profile_record IS NOT NULL)")
                .bind(did)
                .fetch_one(self.pool())
                .await?;
            if !stored
                && !NO_PROFILE.contains_key(did)
                && self.refract_profiles(req, std::slice::from_ref(did), &HashMap::new()).await.is_empty()
                && !NO_PROFILE.contains_key(did)
            {
                missing_profiles.push(did.clone());
                continue;
            }
            match self.get_profile(viewer, req, did).await? {
                Ok(view) => out_profiles.push(view.inner().clone()),
                Err(_) => missing_profiles.push(did.clone()),
            }
        }
        let mut out_posts: Vec<Value> = Vec::new();
        if !posts.is_empty()
            && let Ok(found) = self.get_posts(viewer, req, posts).await?
        {
            out_posts = found.inner().get("posts").and_then(Value::as_array).cloned().unwrap_or_default();
        }
        let authors: Vec<String> = out_posts.iter().filter_map(|p| p.get("uri").and_then(Value::as_str).and_then(uri_author)).map(str::to_string).collect();
        let actors = self.actor_viewer(viewer, &authors).await?;
        let wanted: HashSet<&String> = posts.iter().collect();
        out_posts.retain(|p| p.get("uri").and_then(Value::as_str).is_some_and(|u| wanted.contains(&u.to_string())));
        let unanswered = super::known_blocks().map(|k| k.unanswered.clone()).unwrap_or_default();
        let mut held: Vec<Value> = withheld.iter().map(|(id, reason)| json!({"id": id, "reason": reason})).collect();
        // RF-S14: a profile or post left out because it couldn't be fetched
        // (not because the gate hides it or a delete was seen) is
        // `unavailable`, not final.
        let returned: HashSet<String> = out_posts.iter().filter_map(|p| p.get("uri").and_then(Value::as_str).map(str::to_string)).collect();
        let missing_posts: Vec<String> = posts.iter().filter(|u| !returned.contains(*u)).cloned().collect();
        if !missing_profiles.is_empty() || !missing_posts.is_empty() {
            let subjects: Vec<String> = missing_profiles.iter().chain(missing_posts.iter()).cloned().collect();
            let gates = self.gate_many(req, &subjects).await?;
            let deleted: HashSet<String> = sqlx::query_scalar("SELECT uri FROM record_tombstones WHERE uri = ANY($1)")
                .bind(&missing_posts)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .collect();
            let hidden = |s: &str| gates.get(s).is_some_and(|g| g.is_hidden());
            for id in subjects.iter().filter(|s| !hidden(s) && !deleted.contains(*s)) {
                held.push(json!({"id": id, "reason": "unavailable"}));
            }
        }
        out_posts.retain(|p| {
            let Some(u) = p.get("uri").and_then(Value::as_str) else { return true };
            let Some(a) = uri_author(u).filter(|a| actors.blocked_either_way(a)) else { return true };
            if !withheld.iter().any(|(id, _)| id == u) {
                held.push(json!({"id": u, "reason": if unanswered.contains(a) { "unanswered" } else { "blocked" }}));
            }
            false
        });
        Ok(Self::emit(json!({"profiles": out_profiles, "posts": out_posts, "pending": pending, "withheld": held})))
    }

    /// `feedViewPost`s for Refract's feeds (refract design §7.2): local
    /// posts and reposts, with the subjects Prism does not store (repost
    /// subjects, reply parents and roots, quotes) gated first, never
    /// fetched when hidden or blocked, fetched through the cascade (at most
    /// `cap` of them), and gated again after the fetch. A repost whose
    /// subject cannot be shown is left out; a parent or root that cannot be
    /// is `notFoundPost` (`blockedPost` for a block); a quote `viewNotFound`.
    /// A blocked author's posts are left out.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_feed_views(&self, viewer: &Viewer, req: &Requester, items: &[FeedItem], cap: usize) -> Result<Vec<Value>, sqlx::Error> {
        let uris: Vec<String> = items.iter().map(|i| i.post_uri.clone()).collect();
        let originators: Vec<String> = items.iter().map(|i| i.originator_did.clone()).collect();
        let ctx = self.post_context_for(Some(viewer), req, &uris, CountMode::List, &[], &originators).await?;
        // Every subject of the page not stored here.
        let mut subjects: Vec<String> = Vec::new();
        let mut push = |u: &str| {
            if !ctx.posts.contains_key(u) && !is_private_uri(u) && !subjects.iter().any(|s| s == u) {
                subjects.push(u.to_string());
            }
        };
        for i in items {
            push(&i.post_uri);
        }
        for row in ctx.posts.values() {
            for u in [&row.reply_parent_uri, &row.reply_root_uri, &row.quote_uri].into_iter().flatten() {
                push(u);
            }
        }
        // Step 1: the gate first, and the viewer's blocks of the authors.
        let authors: Vec<String> = subjects.iter().chain(uris.iter()).filter_map(|u| uri_author(u)).map(str::to_string).collect::<HashSet<_>>().into_iter().collect();
        let mut gated: Vec<String> = subjects.clone();
        gated.extend(authors.iter().cloned());
        let gates = self.gate_many(req, &gated).await?;
        let actors = self.actor_viewer(viewer, &authors).await?;
        let blocked = |u: &str| uri_author(u).is_some_and(|a| actors.blocked_either_way(a));
        let hidden = |u: &str| gates.get(u).is_some_and(|g| g.is_hidden());
        // Steps 2 and 3: never fetch a hidden or blocked subject; the rest
        // through the cascade, within the cap and the request's budget.
        let fetchable: Vec<String> = subjects.iter().filter(|u| !hidden(u) && !blocked(u)).take(cap).cloned().collect();
        let mut fetched: HashMap<String, Value> = HashMap::new();
        if !fetchable.is_empty() && !req.spent() {
            let found = Value::Array(self.upstream().posts(req, &fetchable).await);
            // Step 4: the gate after the fetch, for everything nested.
            let (found, _, after) = self.normalized(viewer, req, found).await?;
            for v in found.as_array().into_iter().flatten() {
                if let Some(u) = v.get("uri").and_then(Value::as_str)
                    && after.listable(u)
                    && !after.blocked(uri_author(u).unwrap_or_default())
                {
                    fetched.insert(u.to_string(), v.clone());
                }
            }
        }
        let ref_view = |u: &str| -> Value {
            if ctx.posts.contains_key(u) {
                return ctx.reply_ref_view(u);
            }
            let author = uri_author(u).unwrap_or_default();
            if blocked(u) {
                return json!({"$type": "app.bsky.feed.defs#blockedPost", "uri": u, "blocked": true, "author": {"did": author, "viewer": actors.view(author)}});
            }
            fetched.get(u).cloned().unwrap_or_else(|| json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": u, "notFound": true}))
        };
        let mut out = Vec::new();
        for item in items {
            if gates.get(&item.originator_did).is_some_and(|g| g.is_hidden()) || blocked(&item.post_uri) {
                continue;
            }
            let local = ctx.posts.contains_key(&item.post_uri);
            if local && !ctx.gates.get(&item.post_uri).is_none_or(|g| g.listable()) {
                continue;
            }
            let post = if local { ctx.post_view(&item.post_uri) } else { fetched.get(&item.post_uri).cloned() };
            let Some(mut post) = post else { continue };
            // A local post quoting a post not stored here: the fetched view.
            if let Some(q) = ctx.posts.get(&item.post_uri).and_then(|r| r.quote_uri.clone())
                && let Some(view) = fetched.get(&q)
            {
                let slot = if post.pointer("/embed/record/record").is_some() { "/embed/record/record" } else { "/embed/record" };
                if let Some(target) = post.pointer_mut(slot)
                    && target.get("$type").and_then(Value::as_str) == Some("app.bsky.embed.record#viewNotFound")
                {
                    *target = view_record(view);
                }
            }
            let mut v = json!({"post": post});
            match item.kind.as_str() {
                "repost" => {
                    v["reason"] = json!({"$type": "app.bsky.feed.defs#reasonRepost", "by": ctx.profiles.view(&item.originator_did, ProfileKind::Basic), "uri": item.uri, "indexedAt": ts(item.sort_at)});
                }
                "pin" => v["reason"] = json!({"$type": "app.bsky.feed.defs#reasonPin"}),
                _ => {}
            }
            if let Some(row) = ctx.posts.get(&item.post_uri)
                && let (Some(parent), Some(root)) = (&row.reply_parent_uri, &row.reply_root_uri)
            {
                v["reply"] = json!({"root": ref_view(root), "parent": ref_view(parent)});
            }
            out.push(v);
        }
        Ok(out)
    }

    /// The page of `refract.getAuthorFeed` for a DID in R-live (refract
    /// design §7.1): its stored posts and reposts, newest first, local
    /// cursors only, with `getAuthorFeed`'s filters and its pinned post;
    /// and the next cursor.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_author_items(&self, did: &str, filter: &str, cursor: Option<&str>, limit: u32, include_pins: bool) -> Result<(Vec<FeedItem>, Option<String>), sqlx::Error> {
        let (at, after) = parse_local_cursor(cursor);
        let (posts_when, reposts) = match filter {
            "posts_no_replies" => ("p.reply_parent_uri IS NULL", true),
            "posts_with_media" => ("p.embed->>'$type' IN ('app.bsky.embed.images', 'app.bsky.embed.video', 'app.bsky.embed.recordWithMedia')", false),
            "posts_with_video" => ("(p.embed->>'$type' = 'app.bsky.embed.video' OR p.embed#>>'{media,$type}' = 'app.bsky.embed.video')", false),
            "posts_and_author_threads" => ("(p.reply_parent_uri IS NULL OR split_part(p.reply_root_uri, '/', 3) = $1)", true),
            _ => ("TRUE", true),
        };
        let sql = format!(
            "SELECT uri, originator_did, post_uri, kind, sort_at FROM ( \
               SELECT p.uri, p.author_did AS originator_did, p.uri AS post_uri, \
                 CASE WHEN p.reply_parent_uri IS NULL THEN 'post' ELSE 'reply' END AS kind, p.created_at AS sort_at \
               FROM posts p WHERE p.author_did = $1 AND {posts_when} \
               UNION ALL \
               SELECT r.uri, r.author_did, r.subject_uri, 'repost', r.created_at FROM reposts r WHERE r.author_did = $1 AND $5 \
             ) f WHERE ($2::timestamptz IS NULL OR (f.sort_at, f.uri) < ($2, $3)) \
             ORDER BY f.sort_at DESC, f.uri DESC LIMIT $4"
        );
        let mut items: Vec<FeedItem> = sqlx::query_as(&sql)
            .bind(did)
            .bind(at)
            .bind(after.unwrap_or_default())
            .bind(i64::from(limit))
            .bind(reposts)
            .fetch_all(self.pool())
            .await?;
        let next = (items.len() == usize::try_from(limit).unwrap_or(usize::MAX)).then(|| items.last().map(|i| local_cursor(i.sort_at, &i.uri))).flatten();
        if include_pins && cursor.is_none() {
            let pinned: Option<String> = sqlx::query_scalar("SELECT profile_record #>> '{pinnedPost,uri}' FROM actors WHERE did = $1")
                .bind(did)
                .fetch_optional(self.pool())
                .await?
                .flatten();
            if let Some(pin) = pinned.filter(|u| uri_author(u) == Some(did)) {
                items.retain(|i| i.post_uri != pin || i.kind == "repost");
                items.insert(0, FeedItem { uri: pin.clone(), originator_did: did.to_string(), post_uri: pin, kind: "pin".to_string(), sort_at: chrono::Utc::now() });
            }
        }
        Ok((items, next))
    }

    /// A Refract feed page's body: `{feed, cursor?}`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_feed(&self, viewer: &Viewer, req: &Requester, items: &[FeedItem], next: Option<&str>, cap: usize) -> Result<PublicEmit<Value>, HydrationError> {
        let feed = self.refract_feed_views(viewer, req, items, cap).await?;
        let mut out = json!({"feed": feed});
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Self::emit(out))
    }
}

/// An `embed.record#viewRecord` from a fetched `postView`.
fn view_record(post: &Value) -> Value {
    let mut v = json!({"$type": "app.bsky.embed.record#viewRecord"});
    for (from, to) in [("uri", "uri"), ("cid", "cid"), ("author", "author"), ("record", "value"), ("labels", "labels"), ("likeCount", "likeCount"), ("repostCount", "repostCount"), ("replyCount", "replyCount"), ("quoteCount", "quoteCount"), ("indexedAt", "indexedAt"), ("restored", "restored")] {
        if let Some(x) = post.get(from) {
            v[to] = x.clone();
        }
    }
    if let Some(e) = post.get("embed") {
        v["embeds"] = json!([e]);
    }
    v
}

/// What a member's own tables drop from a Refract timeline, for the DID
/// expression `d` (refract design §7.1 "Member-only data"): a full mute,
/// a block either way, and the list mutes and list blocks that apply.
fn timeline_excluded(d: &str) -> String {
    format!(
        "(EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.subject_did = {d} AND m.scope = 'full') \
          OR EXISTS (SELECT 1 FROM blocks b WHERE (b.author_did = $1 AND b.subject_did = {d}) OR (b.author_did = {d} AND b.subject_did = $1)) \
          OR EXISTS (SELECT 1 FROM list_references r \
                     JOIN subscribed_lists l ON l.list_uri = r.list_uri \
                     JOIN subscribed_list_items i ON i.list_uri = r.list_uri \
                     WHERE r.member_did = $1 AND i.subject_did = {d} \
                       AND r.admitted AND r.effective AND l.state = 'active' AND l.purpose = 'app.bsky.graph.defs#modlist'))"
    )
}

/// The most replies `refract.getReplies` returns.
pub const REPLIES_MAX: i64 = 200;
/// Records one Constellation `links` call for a threadgate rule asks for.
const RULE_LINKS: u32 = 50;

/// Threadgate records of roots not stored here (refract design §7.1): the
/// record, or `None` for no gate; ten minutes.
static ROOT_GATES: LazyLock<moka::sync::Cache<String, Option<Value>>> =
    LazyLock::new(|| moka::sync::Cache::builder().max_capacity(10_000).time_to_live(Duration::from_secs(600)).build());

/// The root's threadgate as `getReplies` applies it.
struct RootRules {
    hidden: HashSet<String>,
    /// `None`: no `allow`, everyone may reply.
    allow: Option<Vec<Value>>,
}

impl Hydrator {
    /// The page of `refract.getTimeline` for a member (refract design
    /// §7.1): posts by DIDs in `restored` the member follows (top-level, and
    /// replies whose parent author it follows) and their reposts, newest
    /// first, with the member's mutes, blocks and list mutes and blocks
    /// applied in SQL to the row's author and a repost's subject author.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_timeline_items(&self, me: &str, restored: &[String], cursor: Option<&str>, limit: u32) -> Result<(Vec<FeedItem>, Option<String>), sqlx::Error> {
        let (at, after) = parse_local_cursor(cursor);
        let sql = format!(
            "SELECT uri, originator_did, post_uri, kind, sort_at FROM ( \
               SELECT p.uri, p.author_did AS originator_did, p.uri AS post_uri, \
                 CASE WHEN p.reply_parent_uri IS NULL THEN 'post' ELSE 'reply' END AS kind, p.created_at AS sort_at, \
                 p.quote_uri IS NOT NULL AS quote \
               FROM posts p WHERE p.author_did = ANY($2) \
                 AND EXISTS (SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = p.author_did) \
                 AND (p.reply_parent_uri IS NULL \
                      OR EXISTS (SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = split_part(p.reply_parent_uri, '/', 3))) \
               UNION ALL \
               SELECT r.uri, r.author_did, r.subject_uri, 'repost', r.created_at, false FROM reposts r \
               WHERE r.author_did = ANY($2) \
                 AND EXISTS (SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = r.author_did) \
             ) t WHERE ($3::timestamptz IS NULL OR (t.sort_at, t.uri) < ($3, $4)) \
               AND NOT {} AND NOT {} \
               AND NOT (t.kind = 'repost' AND EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 \
                        AND m.subject_did = t.originator_did AND m.scope IN ('reposts', 'reposts_and_quotes'))) \
               AND NOT (t.quote AND EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 \
                        AND m.subject_did = t.originator_did AND m.scope IN ('quotes', 'reposts_and_quotes'))) \
             ORDER BY t.sort_at DESC, t.uri DESC LIMIT $5",
            timeline_excluded("t.originator_did"),
            timeline_excluded("split_part(t.post_uri, '/', 3)"),
        );
        let items: Vec<FeedItem> = sqlx::query_as(&sql)
            .bind(me)
            .bind(restored)
            .bind(at)
            .bind(after.unwrap_or_default())
            .bind(i64::from(limit))
            .fetch_all(self.pool())
            .await?;
        let next = (items.len() == usize::try_from(limit).unwrap_or(usize::MAX)).then(|| items.last().map(|i| local_cursor(i.sort_at, &i.uri))).flatten();
        Ok((items, next))
    }

    /// A body with nothing in it, for a caller a Refract method serves
    /// nothing (a non-member's `getTimeline`).
    #[must_use]
    pub fn refract_empty(key: &str) -> PublicEmit<Value> {
        Self::emit(json!({ key: [] }))
    }

    /// The root's threadgate (refract design §7.1 `getReplies`): stored
    /// here, else through Slingshot `getRecord`, cached ten minutes. `None`
    /// when it can't be known.
    async fn refract_root_rules(&self, req: &Requester, root: &str) -> Result<Option<RootRules>, sqlx::Error> {
        let (Some(author), Some(rkey)) = (uri_author(root), root.rsplit('/').next()) else {
            return Ok(None);
        };
        let stored: Option<(Vec<String>, Option<Value>)> = sqlx::query_as(
            "SELECT hidden_replies, allow FROM threadgates WHERE post_uri = $1 \
               AND author_did = split_part(post_uri, '/', 3) AND rkey = split_part(post_uri, '/', 5)",
        )
        .bind(root)
        .fetch_optional(self.pool())
        .await?;
        let record = if let Some((hidden, allow)) = stored {
            return Ok(Some(RootRules { hidden: hidden.into_iter().collect(), allow: allow.and_then(|a| a.as_array().cloned()) }));
        } else if let Some(cached) = ROOT_GATES.get(root) {
            cached
        } else {
            let uri = format!("at://{author}/app.bsky.feed.threadgate/{rkey}");
            let Some(found) = self.upstream().record_or_absent(req, &uri).await else {
                return Ok(None);
            };
            let record = found.and_then(|v| v.get("value").cloned());
            ROOT_GATES.insert(root.to_string(), record.clone());
            record
        };
        let Some(record) = record else {
            return Ok(Some(RootRules { hidden: HashSet::new(), allow: None }));
        };
        let hidden = record
            .get("hiddenReplies")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        Ok(Some(RootRules { hidden, allow: record.get("allow").and_then(Value::as_array).cloned() }))
    }

    /// Whether `follower` follows `subject` (refract design §7.1): from the
    /// stored `follows` when `local`, else one Constellation `links` call
    /// filtered to `follower`. `None` when it can't be known, including a
    /// server that ignored the filter.
    async fn refract_follows(&self, req: &Requester, follower: &str, subject: &str, local: bool) -> Result<Option<bool>, sqlx::Error> {
        if local {
            let found: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM follows WHERE author_did = $1 AND subject_did = $2)")
                .bind(follower)
                .bind(subject)
                .fetch_one(self.pool())
                .await?;
            return Ok(Some(found));
        }
        let Some(client) = self.backlinks() else {
            return Ok(None);
        };
        if self.draw_constellation(req).is_err() {
            return Ok(None);
        }
        let Ok(page) = client.links_by(subject, "app.bsky.graph.follow", ".subject", follower, RULE_LINKS).await else {
            return Ok(None);
        };
        if page.linking_records.iter().any(|r| r.did != follower) {
            return Ok(None);
        }
        Ok(Some(!page.linking_records.is_empty()))
    }

    /// Whether `replier` is on `list` (refract design §7.1 `listRule`).
    async fn refract_on_list(&self, req: &Requester, list: &str, replier: &str) -> Result<Option<bool>, sqlx::Error> {
        let Some(owner) = uri_author(list) else {
            return Ok(None);
        };
        let member: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1)")
            .bind(owner)
            .fetch_one(self.pool())
            .await?;
        if member {
            let found: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM list_items WHERE list_uri = $1 AND subject_did = $2)")
                .bind(list)
                .bind(replier)
                .fetch_one(self.pool())
                .await?;
            return Ok(Some(found));
        }
        let Some(client) = self.backlinks() else {
            return Ok(None);
        };
        if self.draw_constellation(req).is_err() {
            return Ok(None);
        }
        let Ok(page) = client.links_by(replier, "app.bsky.graph.listitem", ".subject", owner, RULE_LINKS).await else {
            return Ok(None);
        };
        if page.linking_records.iter().any(|r| r.did != owner) {
            return Ok(None);
        }
        let mut complete = true;
        for r in &page.linking_records {
            let uri = format!("at://{}/{}/{}", r.did, r.collection, r.rkey);
            match self.upstream().record(req, &uri).await {
                Some(v) if v.pointer("/value/list").and_then(Value::as_str) == Some(list) => return Ok(Some(true)),
                Some(_) => {}
                None => complete = false,
            }
        }
        Ok(complete.then_some(false))
    }

    /// Whether the root's `allow` rules admit `replier`: `false` too when no
    /// rule admits it and one couldn't be evaluated (dropped: fail closed).
    async fn refract_admitted(&self, req: &Requester, root_author: &str, rules: &[Value], replier: &str, mentions: Option<&HashSet<String>>) -> Result<bool, sqlx::Error> {
        Ok(self.refract_admission(req, root_author, rules, replier, mentions).await? == Some(true))
    }

    /// The root's `allow` rules for `replier`, three-valued: `Some(true)` a
    /// rule admits it, `Some(false)` every rule was evaluated and none does,
    /// `None` none admits it and one couldn't be evaluated.
    async fn refract_admission(&self, req: &Requester, root_author: &str, rules: &[Value], replier: &str, mentions: Option<&HashSet<String>>) -> Result<Option<bool>, sqlx::Error> {
        let members: Vec<String> = sqlx::query_scalar("SELECT did FROM members WHERE did = ANY($1)")
            .bind(vec![root_author.to_string(), replier.to_string()])
            .fetch_all(self.pool())
            .await?;
        let local = !members.is_empty();
        let mut complete = true;
        for rule in rules {
            let allowed = match rule.get("$type").and_then(Value::as_str).unwrap_or_default() {
                "app.bsky.feed.threadgate#mentionRule" => mentions.map(|m| m.contains(replier)),
                "app.bsky.feed.threadgate#followingRule" => self.refract_follows(req, root_author, replier, local).await?,
                "app.bsky.feed.threadgate#followerRule" => self.refract_follows(req, replier, root_author, local).await?,
                "app.bsky.feed.threadgate#listRule" => match rule.get("list").and_then(Value::as_str) {
                    Some(list) => self.refract_on_list(req, list, replier).await?,
                    None => None,
                },
                _ => None,
            };
            match allowed {
                Some(true) => return Ok(Some(true)),
                Some(false) => {}
                None => complete = false,
            }
        }
        Ok(complete.then_some(false))
    }

    /// The DIDs the root post mentions, from its stored row or its record.
    async fn refract_root_mentions(&self, req: &Requester, root: &str) -> Result<Option<HashSet<String>>, sqlx::Error> {
        let stored: Option<Option<Vec<String>>> = sqlx::query_scalar("SELECT mention_dids FROM posts WHERE uri = $1")
            .bind(root)
            .fetch_optional(self.pool())
            .await?;
        if let Some(m) = stored {
            return Ok(Some(m.unwrap_or_default().into_iter().collect()));
        }
        let Some(record) = self.upstream().record(req, root).await else {
            return Ok(None);
        };
        let mentions = record
            .pointer("/value/facets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .flat_map(|f| f.get("features").and_then(Value::as_array).cloned().unwrap_or_default())
            .filter(|feature| feature.get("$type").and_then(Value::as_str) == Some("app.bsky.richtext.facet#mention"))
            .filter_map(|feature| feature.get("did").and_then(Value::as_str).map(str::to_string))
            .collect();
        Ok(Some(mentions))
    }

    /// The stored replies by DIDs in `restored` under `root`, newest first,
    /// at most 200: `(uri, author)`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_reply_rows(&self, root: &str, restored: &[String]) -> Result<Vec<(String, String)>, sqlx::Error> {
        sqlx::query_as(
            "SELECT uri, author_did FROM posts WHERE reply_root_uri = $1 AND author_did = ANY($2) \
             ORDER BY created_at DESC, uri DESC LIMIT $3",
        )
        .bind(root)
        .bind(restored)
        .bind(REPLIES_MAX)
        .fetch_all(self.pool())
        .await
    }

    /// `refract.getReplies` (refract design §7.1): `rows` (from
    /// [`Hydrator::refract_reply_rows`]) as `postView`s, less those the root's threadgate hides or doesn't
    /// admit, those of an author the caller blocks or is blocked by
    /// (`blocks` for a visitor or a member that is not ready), and those
    /// whose author and the root's author block each other or were left
    /// unanswered (a separate Farsight reader, `round` long); then other
    /// accounts' direct replies to them (RF-S7,
    /// [`Hydrator::refract_child_replies`]).
    ///
    /// # Errors
    ///
    /// Database errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn refract_replies(
        &self,
        viewer: &Viewer,
        req: &Requester,
        root: &str,
        rows: Vec<(String, String)>,
        blocks: Option<&mut crate::refract::handlers::Blocks>,
        round: Duration,
    ) -> Result<PublicEmit<Value>, HydrationError> {
        if rows.is_empty() {
            return Ok(Self::refract_empty("posts"));
        }
        let Some(root_author) = uri_author(root).map(str::to_string) else {
            return Ok(Self::refract_empty("posts"));
        };
        // The threadgate: unknowable drops every reply (fail closed).
        let Some(rules) = self.refract_root_rules(req, root).await? else {
            return Ok(Self::refract_empty("posts"));
        };
        let mentions = match &rules.allow {
            Some(allow) if allow.iter().any(|r| r.get("$type").and_then(Value::as_str) == Some("app.bsky.feed.threadgate#mentionRule")) => {
                self.refract_root_mentions(req, root).await?
            }
            _ => None,
        };
        let mut blocks = blocks;
        let mut admitted: HashMap<String, bool> = HashMap::new();
        let thread = ThreadRules { root_author: &root_author, rules: &rules, mentions: mentions.as_ref(), round };
        let kept = self.refract_admit_replies(viewer, req, &thread, rows, blocks.as_deref_mut(), &mut admitted).await?;
        let uris: Vec<String> = kept.into_iter().map(|(u, _)| u).collect();
        let ctx = self.post_context_for(Some(viewer), req, &uris, CountMode::List, &[], &[]).await?;
        let mut posts: Vec<Value> = uris
            .iter()
            .filter(|u| ctx.gates.get(*u).is_none_or(|g| g.listable()))
            .filter_map(|u| ctx.post_view(u))
            .collect();
        // RF-S7: other accounts' direct replies to the restored ones.
        let children = self.refract_child_replies(viewer, req, root, &thread, &posts, blocks, &mut admitted).await?;
        posts.extend(children);
        Ok(Self::emit(json!({ "posts": posts })))
    }

    /// The replies of `items` (`(uri, author)`) `getReplies` may return:
    /// less those the root's threadgate hides (except from their own author)
    /// or doesn't admit (fail closed), those of an author the caller blocks
    /// or is blocked by (`blocks` for a visitor or a member that is not
    /// ready), and those whose author and the root's author block each
    /// other or were left unanswered. `admitted` remembers each author's
    /// admission across calls.
    async fn refract_admit_replies(
        &self,
        viewer: &Viewer,
        req: &Requester,
        t: &ThreadRules<'_>,
        items: Vec<(String, String)>,
        blocks: Option<&mut crate::refract::handlers::Blocks>,
        admitted: &mut HashMap<String, bool>,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        let me = viewer.did.as_deref();
        let mut kept: Vec<(String, String)> = Vec::new();
        for (uri, author) in items {
            if t.rules.hidden.contains(&uri) && Some(author.as_str()) != me {
                continue;
            }
            if let Some(allow) = &t.rules.allow
                && author != t.root_author
            {
                let ok = match admitted.get(&author) {
                    Some(ok) => *ok,
                    None => {
                        let ok = self.refract_admitted(req, t.root_author, allow, &author, t.mentions).await?;
                        admitted.insert(author.clone(), ok);
                        ok
                    }
                };
                if !ok {
                    continue;
                }
            }
            kept.push((uri, author));
        }
        let mut repliers: Vec<String> = kept.iter().map(|(_, a)| a.clone()).collect::<HashSet<_>>().into_iter().collect();
        repliers.sort_unstable();
        // The caller's blocks.
        let mut dropped: HashSet<String> = HashSet::new();
        match blocks {
            Some(b) => {
                b.round(&repliers).await;
                dropped.extend(repliers.iter().filter(|a| b.blocked(a)).cloned());
            }
            None => {
                let actors = self.actor_viewer(viewer, &repliers).await?;
                dropped.extend(repliers.iter().filter(|a| actors.blocked_either_way(a)).cloned());
            }
        }
        // The root author and each replier: a separate reader.
        let others: Vec<String> = repliers.iter().filter(|a| **a != t.root_author && !dropped.contains(*a)).cloned().collect();
        dropped.extend(self.refract_pairs_blocked(req, t.root_author, &others, t.round).await);
        Ok(kept.into_iter().filter(|(_, a)| !dropped.contains(a)).collect())
    }

    /// Those of `others` that block `of` or are blocked by it, or were left
    /// unanswered, through a separate Farsight reader (so its cache and
    /// fallback don't touch the caller's request), `round` long. None with
    /// no Farsight configured.
    async fn refract_pairs_blocked(&self, req: &Requester, of: &str, others: &[String], round: Duration) -> HashSet<String> {
        let mut out = HashSet::new();
        if others.is_empty() {
            return out;
        }
        let Some(farsight) = self.farsight() else {
            return out;
        };
        let reader = farsight.request(req);
        reader.set_visitor();
        let got = reader.pair_answers(of, others, crate::farsight::PairBudget { max_calls: u32::MAX, timeout: round }, None).await;
        for did in others {
            match got.answers.get(did) {
                Some(answer) if !answer.inbound.is_block() && !answer.outbound.is_block() => {}
                _ => {
                    out.insert(did.clone());
                }
            }
        }
        out
    }

    /// RF-S7: other accounts' direct replies to the restored replies of
    /// `posts`, at most [`CHILDREN_MAX`], from at most [`CHILD_PARENTS`]
    /// parents whose `replyCount` isn't 0 (one Constellation `links` page
    /// each, `.reply.parent.uri`, drawn from the caller's share). Gated
    /// before the fetch (never fetched when hidden), admitted as restored
    /// replies are, dropped across a block with the parent's author, then
    /// fetched through the cascade (nothing stored), judged again for a
    /// visitor over every account the fetched views name, normalized, and
    /// kept only when their record's root is `root` and their parent the
    /// one asked.
    #[allow(clippy::too_many_arguments)]
    async fn refract_child_replies(
        &self,
        viewer: &Viewer,
        req: &Requester,
        root: &str,
        t: &ThreadRules<'_>,
        posts: &[Value],
        mut blocks: Option<&mut crate::refract::handlers::Blocks>,
        admitted: &mut HashMap<String, bool>,
    ) -> Result<Vec<Value>, sqlx::Error> {
        let Some(client) = self.backlinks() else {
            return Ok(Vec::new());
        };
        let returned: HashSet<&str> = posts.iter().filter_map(|p| p.get("uri").and_then(Value::as_str)).collect();
        let parents: Vec<&str> = posts
            .iter()
            .filter(|p| p.get("replyCount").and_then(Value::as_u64) != Some(0))
            .filter_map(|p| p.get("uri").and_then(Value::as_str))
            .take(CHILD_PARENTS)
            .collect();
        let mut children: Vec<(String, String)> = Vec::new();
        let mut parent_of: HashMap<String, String> = HashMap::new();
        for parent in parents {
            let room = CHILDREN_MAX.saturating_sub(children.len());
            if room == 0 {
                break;
            }
            if self.constellation_bucket().is_some_and(|b| b.draw(req.key()).is_err()) {
                break;
            }
            let Ok(page) = client.links(parent, NSID_POST, ".reply.parent.uri", u32::try_from(room).unwrap_or(u32::MAX), None).await else {
                continue;
            };
            for r in page.linking_records {
                let uri = format!("at://{}/{}/{}", r.did, r.collection, r.rkey);
                if r.collection != NSID_POST || returned.contains(uri.as_str()) || is_private_uri(&uri) || parent_of.contains_key(&uri) || children.len() >= CHILDREN_MAX {
                    continue;
                }
                parent_of.insert(uri.clone(), parent.to_string());
                children.push((uri, r.did));
            }
        }
        if children.is_empty() {
            return Ok(Vec::new());
        }
        // The gate first: a hidden reply or author is never fetched.
        let mut gated: Vec<String> = children.iter().map(|(u, _)| u.clone()).collect();
        gated.extend(children.iter().map(|(_, a)| a.clone()));
        let gates = self.gate_many(req, &gated).await?;
        let hidden = |s: &str| gates.get(s).is_some_and(|g| g.is_hidden());
        children.retain(|(u, a)| !hidden(u) && !hidden(a));
        let mut kept = self.refract_admit_replies(viewer, req, t, children, blocks.as_deref_mut(), admitted).await?;
        // The parent's author and each child's: a separate reader.
        let parent_author = |u: &str| parent_of.get(u).and_then(|p| uri_author(p)).unwrap_or_default().to_string();
        let mut by_parent: HashMap<String, Vec<String>> = HashMap::new();
        for (u, a) in &kept {
            let pa = parent_author(u);
            if pa != *a && pa != t.root_author {
                let kids = by_parent.entry(pa).or_default();
                if !kids.contains(a) {
                    kids.push(a.clone());
                }
            }
        }
        let mut across: HashSet<(String, String)> = HashSet::new();
        for (pa, kids) in by_parent {
            for kid in self.refract_pairs_blocked(req, &pa, &kids, t.round).await {
                across.insert((pa.clone(), kid));
            }
        }
        kept.retain(|(u, a)| !across.contains(&(parent_author(u), a.clone())));
        if kept.is_empty() || req.spent() {
            return Ok(Vec::new());
        }
        let uris: Vec<String> = kept.into_iter().map(|(u, _)| u).collect();
        let found = Value::Array(self.upstream().posts(req, &uris).await);
        // For a visitor, every account the fetched views name is judged
        // before the views are built (collect → judge → emit).
        let known = match blocks {
            Some(b) => {
                let (dids, _) = super::normalize::subjects(&found);
                let dids: Vec<String> = dids.into_iter().collect();
                b.round(&dids).await;
                Some(b.known())
            }
            None => None,
        };
        let (found, _, after) = match known {
            Some(k) => super::with_known_blocks(k, self.normalized(viewer, req, found)).await?,
            None => self.normalized(viewer, req, found).await?,
        };
        let mut by_uri: HashMap<String, Value> = HashMap::new();
        for v in found.as_array().into_iter().flatten() {
            let Some(u) = v.get("uri").and_then(Value::as_str) else { continue };
            let in_thread = v.pointer("/record/reply/root/uri").and_then(Value::as_str) == Some(root)
                && v.pointer("/record/reply/parent/uri").and_then(Value::as_str).is_some_and(|p| parent_of.get(u).is_some_and(|q| q == p));
            if in_thread && after.listable(u) && !after.blocked(uri_author(u).unwrap_or_default()) {
                by_uri.insert(u.to_string(), v.clone());
            }
        }
        Ok(uris.iter().filter_map(|u| by_uri.remove(u)).collect())
    }
}

/// What `getReplies` judges every reply of one thread by.
struct ThreadRules<'a> {
    root_author: &'a str,
    rules: &'a RootRules,
    mentions: Option<&'a HashSet<String>>,
    /// How long a separate Farsight reader's round may take.
    round: Duration,
}

/// The collection of posts.
const NSID_POST: &str = "app.bsky.feed.post";
/// RF-S7: parents asked for other accounts' replies per `getReplies`.
pub const CHILD_PARENTS: usize = 10;
/// RF-S7: other accounts' replies per `getReplies`.
pub const CHILDREN_MAX: usize = 50;

/// Restored posts given a visitor's `like` and `repost` per request (RF-S1).
pub const VISITOR_POSTS: usize = 20;
/// Restored accounts given a visitor's `following` per request.
pub const VISITOR_ACCOUNTS: usize = 10;
/// Thread roots judged for a visitor's `replyDisabled` per request.
pub const VISITOR_ROOTS: usize = 5;
/// One Constellation call for a visitor's own record.
const VISITOR_CALL_TIMEOUT: Duration = Duration::from_millis(800);
/// No call for a visitor's viewer state starts later than this into the
/// step.
const VISITOR_BUDGET: Duration = Duration::from_secs(1);
const VISITOR_CONCURRENCY: usize = 8;
/// `refract_viewer_state_total{field, outcome}`: a visitor's viewer-state
/// fields, `found`, `none` or `unknown`.
pub const METRIC_VIEWER_STATE: &str = "refract_viewer_state_total";

/// A visitor's own-record field, and where Constellation keeps its links.
#[derive(Debug, Clone, Copy)]
enum OwnField {
    Like,
    Repost,
    Following,
}

impl OwnField {
    fn name(self) -> &'static str {
        match self {
            Self::Like => "like",
            Self::Repost => "repost",
            Self::Following => "following",
        }
    }

    fn collection(self) -> &'static str {
        match self {
            Self::Like => super::thread::NSID_LIKE,
            Self::Repost => super::feed::NSID_REPOST,
            Self::Following => super::graph::NSID_FOLLOW,
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Like | Self::Repost => ".subject.uri",
            Self::Following => ".subject",
        }
    }
}

/// What a visitor's viewer state is looked up for, in document order.
#[derive(Default)]
struct VisitorTargets {
    posts: Vec<String>,
    accounts: Vec<String>,
    roots: Vec<String>,
    /// Each post of `posts` → its thread root.
    root_of: HashMap<String, String>,
}

/// A profile view whose viewer carries a block either way.
fn block_marked(m: &serde_json::Map<String, Value>) -> bool {
    m.get("viewer").is_some_and(|v| {
        v.get("blockedBy").and_then(Value::as_bool) == Some(true) || v.get("blocking").is_some() || v.get("blockingByList").is_some()
    })
}

fn visitor_targets(v: &Value, t: &mut VisitorTargets) {
    match v {
        Value::Array(a) => a.iter().for_each(|x| visitor_targets(x, t)),
        Value::Object(m) => {
            if m.contains_key("restored") {
                if super::normalize::is_post_view(m)
                    && let Some(u) = m.get("uri").and_then(Value::as_str)
                {
                    if !t.root_of.contains_key(u) && t.posts.len() < VISITOR_POSTS {
                        t.posts.push(u.to_string());
                        let root = m.get("record").and_then(|r| r.pointer("/reply/root/uri")).and_then(Value::as_str).unwrap_or(u).to_string();
                        if !t.roots.contains(&root) && t.roots.len() < VISITOR_ROOTS {
                            t.roots.push(root.clone());
                        }
                        t.root_of.insert(u.to_string(), root);
                    }
                } else if super::normalize::is_profile_view(m)
                    && let Some(d) = m.get("did").and_then(Value::as_str)
                    && !block_marked(m)
                    && !t.accounts.iter().any(|a| a == d)
                    && t.accounts.len() < VISITOR_ACCOUNTS
                {
                    t.accounts.push(d.to_string());
                }
            }
            m.values().for_each(|x| visitor_targets(x, t));
        }
        _ => {}
    }
}

/// Write a visitor's viewer state into every restored view it was found
/// for.
fn apply_visitor_state(v: &mut Value, found: &HashMap<(&'static str, String), String>, disabled: &HashMap<String, bool>, root_of: &HashMap<String, String>) {
    match v {
        Value::Array(a) => a.iter_mut().for_each(|x| apply_visitor_state(x, found, disabled, root_of)),
        Value::Object(m) => {
            if m.contains_key("restored") {
                let mut fields: Vec<(&str, Value)> = Vec::new();
                if super::normalize::is_post_view(m)
                    && let Some(u) = m.get("uri").and_then(Value::as_str).map(str::to_string)
                {
                    for field in ["like", "repost"] {
                        if let Some(r) = found.get(&(field, u.clone())) {
                            fields.push((field, json!(r)));
                        }
                    }
                    if let Some(d) = root_of.get(&u).and_then(|r| disabled.get(r)) {
                        fields.push(("replyDisabled", json!(d)));
                    }
                } else if super::normalize::is_profile_view(m)
                    && !block_marked(m)
                    && let Some(r) = m.get("did").and_then(Value::as_str).and_then(|d| found.get(&("following", d.to_string())))
                {
                    fields.push(("following", json!(r)));
                }
                if !fields.is_empty() {
                    let viewer = m.entry("viewer").or_insert_with(|| json!({}));
                    if !viewer.is_object() {
                        *viewer = json!({});
                    }
                    for (k, x) in fields {
                        viewer[k] = x;
                    }
                }
            }
            m.values_mut().for_each(|x| apply_visitor_state(x, found, disabled, root_of));
        }
        _ => {}
    }
}

impl Hydrator {
    /// RF-S1: a visitor's own record linking to `target` from `collection`
    /// at `path`, through one Constellation `links` call filtered to `me`,
    /// drawn from the caller's share without marking the request. `None`
    /// when it can't be known (no Constellation, no share, an error, a
    /// server that ignored the filter); `Some(None)` for no record.
    async fn refract_own_link(&self, req: &Requester, me: &str, target: &str, collection: &str, path: &str) -> Option<Option<String>> {
        let client = self.backlinks()?;
        if self.constellation_bucket().is_some_and(|b| b.draw(req.key()).is_err()) {
            return None;
        }
        let page = tokio::time::timeout(VISITOR_CALL_TIMEOUT, client.links_by(target, collection, path, me, 1)).await.ok()?.ok()?;
        if page.linking_records.iter().any(|r| r.did != me || r.collection != collection) {
            return None;
        }
        Some(page.linking_records.first().map(|r| format!("at://{}/{}/{}", r.did, r.collection, r.rkey)))
    }

    /// RF-S1: whether `me` may not reply in the thread of `root`, from the
    /// root's threadgate as `getReplies` reads it. `None` when it can't be
    /// known; never `true` on an unevaluated rule.
    async fn refract_reply_disabled(&self, req: &Requester, me: &str, root: &str) -> Result<Option<bool>, sqlx::Error> {
        let Some(author) = uri_author(root) else {
            return Ok(None);
        };
        if author == me {
            return Ok(Some(false));
        }
        let Some(rules) = self.refract_root_rules(req, root).await? else {
            return Ok(None);
        };
        let Some(allow) = rules.allow else {
            return Ok(Some(false));
        };
        if allow.is_empty() {
            return Ok(Some(true));
        }
        let mentions = if allow.iter().any(|r| r.get("$type").and_then(Value::as_str) == Some("app.bsky.feed.threadgate#mentionRule")) {
            self.refract_root_mentions(req, root).await?
        } else {
            None
        };
        Ok(self.refract_admission(req, author, &allow, me, mentions.as_ref()).await?.map(|admitted| !admitted))
    }

    /// RF-S1: a visitor's viewer state on the restored views of `body`:
    /// `viewer.like` and `viewer.repost` on restored posts, `viewer.following`
    /// on restored accounts (not across a block), from the caller's own
    /// records on Constellation; `viewer.replyDisabled` from each post's
    /// root threadgate. Capped per request; a field that can't be known
    /// stays absent. Nothing is stored or cached.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_visitor_state(&self, me: &str, req: &Requester, body: PublicEmit<Value>) -> Result<PublicEmit<Value>, HydrationError> {
        use futures::StreamExt;
        let unchecked = || Self::refract_with(&body, &[("viewerChecked", json!({"posts": [], "actors": [], "roots": []}))]);
        // RF-S12: off, no reader DID leaves Prism and nothing is checked.
        if !self.inner.cfg.live.load().refract_visitor_viewer_state {
            return Ok(unchecked());
        }
        let mut t = VisitorTargets::default();
        visitor_targets(body.inner(), &mut t);
        if t.posts.is_empty() && t.accounts.is_empty() {
            return Ok(unchecked());
        }
        let started = std::time::Instant::now();
        let mut asks: Vec<(OwnField, String)> = Vec::new();
        for p in &t.posts {
            asks.push((OwnField::Like, p.clone()));
            asks.push((OwnField::Repost, p.clone()));
        }
        for d in &t.accounts {
            asks.push((OwnField::Following, d.clone()));
        }
        let answers: Vec<(OwnField, String, Option<Option<String>>)> = futures::stream::iter(asks)
            .map(|(field, target)| async move {
                let got = if started.elapsed() >= VISITOR_BUDGET { None } else { self.refract_own_link(req, me, &target, field.collection(), field.path()).await };
                (field, target, got)
            })
            .buffer_unordered(VISITOR_CONCURRENCY)
            .collect()
            .await;
        let mut found: HashMap<(&'static str, String), String> = HashMap::new();
        // RF-S13: the fields answered, a record or none.
        let mut known: HashSet<(&'static str, String)> = HashSet::new();
        for (field, target, got) in answers {
            if got.is_some() {
                known.insert((field.name(), target.clone()));
            }
            let outcome = match got {
                Some(Some(uri)) => {
                    found.insert((field.name(), target), uri);
                    "found"
                }
                Some(None) => "none",
                None => "unknown",
            };
            metrics::counter!(METRIC_VIEWER_STATE, "field" => field.name(), "outcome" => outcome).increment(1);
        }
        let mut disabled: HashMap<String, bool> = HashMap::new();
        for root in &t.roots {
            let got = if started.elapsed() >= VISITOR_BUDGET { None } else { self.refract_reply_disabled(req, me, root).await? };
            let outcome = match got {
                Some(d) => {
                    disabled.insert(root.clone(), d);
                    "found"
                }
                None => "unknown",
            };
            metrics::counter!(METRIC_VIEWER_STATE, "field" => "replyDisabled", "outcome" => outcome).increment(1);
        }
        let mut view = body.inner().clone();
        apply_visitor_state(&mut view, &found, &disabled, &t.root_of);
        // RF-S13: a listed subject's absent field is none; an unlisted
        // subject's is unknown.
        let posts: Vec<&String> = t.posts.iter().filter(|p| known.contains(&("like", (*p).clone())) && known.contains(&("repost", (*p).clone()))).collect();
        let actors: Vec<&String> = t.accounts.iter().filter(|d| known.contains(&("following", (*d).clone()))).collect();
        let roots: Vec<&String> = t.roots.iter().filter(|r| disabled.contains_key(*r)).collect();
        if let Some(m) = view.as_object_mut() {
            m.insert("viewerChecked".into(), json!({"posts": posts, "actors": actors, "roots": roots}));
        }
        Ok(Self::emit(view))
    }

    /// `body` with top-level `fields` added (RF-S10, RF-S13): answer
    /// metadata, never a view.
    #[must_use]
    pub fn refract_with(body: &PublicEmit<Value>, fields: &[(&str, Value)]) -> PublicEmit<Value> {
        let mut v = body.inner().clone();
        if let Some(m) = v.as_object_mut() {
            for (k, x) in fields {
                m.insert((*k).to_string(), x.clone());
            }
        }
        Self::emit(v)
    }
}
