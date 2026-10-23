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
            match self.upstream().record(&walk, &format!("at://{d}/app.bsky.actor.profile/self")).await {
                Some(found) => {
                    let view = record_profile(&d, rows.get(&d), found.get("value").unwrap_or(&Value::Null));
                    out.insert(d, view);
                }
                None => NO_PROFILE.insert(d, ()),
            }
        }
        out
    }

    /// `refract.fill`'s body: the `profileViewDetailed` of each DID of
    /// `profiles` that renders, the `postView` of each post of `posts`, and
    /// `pending` as the handler decided it. A post whose author the viewer
    /// blocks or is blocked by is left out, as every other omission.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn refract_fill(&self, viewer: &Viewer, req: &Requester, profiles: &[String], posts: &[String], pending: &[String]) -> Result<PublicEmit<Value>, HydrationError> {
        let mut out_profiles = Vec::new();
        for did in profiles {
            if let Ok(view) = self.get_profile(viewer, req, did).await? {
                out_profiles.push(view.inner().clone());
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
        out_posts.retain(|p| p.get("uri").and_then(Value::as_str).and_then(uri_author).is_none_or(|a| !actors.blocked_either_way(a)));
        let wanted: HashSet<&String> = posts.iter().collect();
        out_posts.retain(|p| p.get("uri").and_then(Value::as_str).is_some_and(|u| wanted.contains(&u.to_string())));
        Ok(Self::emit(json!({"profiles": out_profiles, "posts": out_posts, "pending": pending})))
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

