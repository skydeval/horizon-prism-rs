//! Post and feed views (design-v2 §7.1, §5.12, §7.3, §9.6): `getPosts`,
//! `getAuthorFeed`, `getTimeline`, `getLikes`, `getRepostedBy`, `getQuotes`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde_json::{Map, Value, json};

use super::actor::{ProfileContext, ProfileKind};
use super::normalize::is_private_uri;
use super::{CountMode, Hydrator, PostRow, PublicEmit, ViewError, ViewResult, Viewer, blob_url, ts};
use crate::aggregations::counts::Counts;
use crate::db::locks::uri_author;
use crate::firehose::normalize::blob_cid;
use crate::moderation::{self, Gate};

pub const NSID_LIKE: &str = "app.bsky.feed.like";
pub const NSID_REPOST: &str = "app.bsky.feed.repost";
pub const NSID_POST: &str = "app.bsky.feed.post";

/// Clamp a lexicon `limit` (1..=100, default 50).
#[must_use]
pub fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(50).clamp(1, 100)
}

/// The timestamp encoded in a TID rkey (microseconds since the epoch).
#[must_use]
pub fn tid_timestamp(rkey: &str) -> Option<DateTime<Utc>> {
    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    if rkey.len() != 13 {
        return None;
    }
    let mut v: u64 = 0;
    for c in rkey.bytes() {
        let d = ALPHABET.iter().position(|a| *a == c)?;
        v = (v << 5) | u64::try_from(d).ok()?;
    }
    let micros = i64::try_from(v >> 10).ok()?;
    Utc.timestamp_micros(micros).single()
}

/// A local-source cursor: `L:<sort_at>::<uri>`.
#[must_use]
pub fn local_cursor(at: DateTime<Utc>, uri: &str) -> String {
    format!("L:{}::{uri}", at.to_rfc3339_opts(SecondsFormat::Micros, true))
}

/// Parse a local-source cursor.
#[must_use]
pub fn parse_local_cursor(c: Option<&str>) -> (Option<DateTime<Utc>>, Option<String>) {
    let Some((at, uri)) = c.and_then(|c| c.strip_prefix("L:")).and_then(|c| c.split_once("::")) else {
        return (None, None);
    };
    match DateTime::parse_from_rfc3339(at) {
        Ok(t) => (Some(t.with_timezone(&Utc)), Some(uri.to_string())),
        Err(_) => (None, None),
    }
}

/// Everything needed to render post views.
pub struct PostContext {
    pub posts: HashMap<String, PostRow>,
    pub profiles: ProfileContext,
    pub viewers: HashMap<String, Value>,
    pub labels: HashMap<String, Vec<Value>>,
    pub counts: HashMap<String, Counts>,
    pub gates: HashMap<String, Gate>,
}

impl PostContext {
    fn author_blocked(&self, did: &str) -> bool {
        self.profiles.viewer.blocked_either_way(did)
    }

    /// `app.bsky.feed.defs#postView`, if the post is local and not hidden.
    #[must_use]
    pub fn post_view(&self, uri: &str) -> Option<Value> {
        self.post_view_depth(uri, 0)
    }

    fn post_view_depth(&self, uri: &str, depth: u8) -> Option<Value> {
        let row = self.posts.get(uri)?;
        if self.gates.get(uri).copied().unwrap_or(Gate::Visible).is_hidden() {
            return None;
        }
        let c = self.counts.get(uri);
        let mut v = Map::new();
        v.insert("$type".into(), json!("app.bsky.feed.defs#postView"));
        v.insert("uri".into(), json!(uri));
        v.insert("cid".into(), json!(row.cid));
        v.insert("author".into(), self.profiles.view(&row.author_did, ProfileKind::Basic));
        v.insert("record".into(), row.record.clone());
        if let Some(embed) = row.embed.as_ref().and_then(|e| self.embed_view(row, e, depth)) {
            v.insert("embed".into(), embed);
        }
        let pick = |remote: Option<i32>, local: Option<i32>| json!(remote.or(local).unwrap_or(0));
        v.insert("replyCount".into(), pick(c.and_then(|c| c.replies), row.reply_count));
        v.insert("repostCount".into(), pick(c.and_then(|c| c.reposts), row.repost_count));
        v.insert("likeCount".into(), pick(c.and_then(|c| c.likes), row.like_count));
        v.insert("quoteCount".into(), pick(c.and_then(|c| c.quotes), row.quote_count));
        v.insert("indexedAt".into(), json!(ts(row.indexed_at)));
        v.insert("viewer".into(), self.viewers.get(uri).cloned().unwrap_or_else(|| json!({})));
        v.insert("labels".into(), Value::Array(self.labels.get(uri).cloned().unwrap_or_default()));
        Some(Value::Object(v))
    }

    /// The view of an embedded record: a local post, or not-found / blocked.
    fn record_view(&self, uri: &str, depth: u8) -> Value {
        let author = uri_author(uri).unwrap_or_default();
        if is_private_uri(uri) || !uri.contains(&format!("/{}/", super::feed::NSID_POST)) {
            return json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true});
        }
        if self.author_blocked(author) {
            return json!({
                "$type": "app.bsky.embed.record#viewBlocked", "uri": uri, "blocked": true,
                "author": {"did": author, "viewer": self.profiles.viewer.view(author)}
            });
        }
        let Some(row) = self.posts.get(uri).filter(|_| !self.gates.get(uri).copied().unwrap_or(Gate::Visible).is_hidden()) else {
            return json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true});
        };
        let c = self.counts.get(uri);
        let mut v = Map::new();
        v.insert("$type".into(), json!("app.bsky.embed.record#viewRecord"));
        v.insert("uri".into(), json!(uri));
        v.insert("cid".into(), json!(row.cid));
        v.insert("author".into(), self.profiles.view(&row.author_did, ProfileKind::Basic));
        v.insert("value".into(), row.record.clone());
        v.insert("labels".into(), Value::Array(self.labels.get(uri).cloned().unwrap_or_default()));
        v.insert("likeCount".into(), json!(c.and_then(|c| c.likes).or(row.like_count).unwrap_or(0)));
        v.insert("repostCount".into(), json!(c.and_then(|c| c.reposts).or(row.repost_count).unwrap_or(0)));
        v.insert("replyCount".into(), json!(c.and_then(|c| c.replies).or(row.reply_count).unwrap_or(0)));
        v.insert("quoteCount".into(), json!(c.and_then(|c| c.quotes).or(row.quote_count).unwrap_or(0)));
        v.insert("indexedAt".into(), json!(ts(row.indexed_at)));
        if depth == 0
            && let Some(embed) = row.embed.as_ref().and_then(|e| self.embed_view(row, e, depth + 1))
        {
            v.insert("embeds".into(), json!([embed]));
        }
        Value::Object(v)
    }

    fn media_view(&self, row: &PostRow, embed: &Value) -> Option<Value> {
        let did = &row.author_did;
        let pds = self.profiles.rows.get(did).and_then(|r| r.pds_endpoint.clone());
        match embed.get("$type").and_then(Value::as_str)? {
            "app.bsky.embed.images" => {
                let images: Vec<Value> = embed
                    .get("images")?
                    .as_array()?
                    .iter()
                    .filter_map(|img| {
                        let cid = img.get("image").and_then(blob_cid)?;
                        let mut i = json!({
                            "thumb": blob_url(pds.as_deref(), did, &cid)?,
                            "fullsize": blob_url(pds.as_deref(), did, &cid)?,
                            "alt": img.get("alt").and_then(Value::as_str).unwrap_or_default(),
                        });
                        if let Some(ar) = img.get("aspectRatio") {
                            i["aspectRatio"] = ar.clone();
                        }
                        Some(i)
                    })
                    .collect();
                Some(json!({"$type": "app.bsky.embed.images#view", "images": images}))
            }
            "app.bsky.embed.external" => {
                let ext = embed.get("external")?;
                let mut e = json!({
                    "uri": ext.get("uri").and_then(Value::as_str).unwrap_or_default(),
                    "title": ext.get("title").and_then(Value::as_str).unwrap_or_default(),
                    "description": ext.get("description").and_then(Value::as_str).unwrap_or_default(),
                });
                if let Some(url) = ext.get("thumb").and_then(blob_cid).and_then(|c| blob_url(pds.as_deref(), did, &c)) {
                    e["thumb"] = json!(url);
                }
                Some(json!({"$type": "app.bsky.embed.external#view", "external": e}))
            }
            "app.bsky.embed.video" => {
                let cid = embed.get("video").and_then(blob_cid)?;
                let mut v = json!({
                    "$type": "app.bsky.embed.video#view",
                    "cid": cid,
                    "playlist": blob_url(pds.as_deref(), did, &cid)?,
                });
                if let Some(alt) = embed.get("alt") {
                    v["alt"] = alt.clone();
                }
                if let Some(ar) = embed.get("aspectRatio") {
                    v["aspectRatio"] = ar.clone();
                }
                Some(v)
            }
            _ => None,
        }
    }

    fn embed_view(&self, row: &PostRow, embed: &Value, depth: u8) -> Option<Value> {
        match embed.get("$type").and_then(Value::as_str)? {
            "app.bsky.embed.record" => {
                if depth > 0 {
                    return None;
                }
                let uri = embed.get("record")?.get("uri")?.as_str()?;
                Some(json!({"$type": "app.bsky.embed.record#view", "record": self.record_view(uri, depth)}))
            }
            "app.bsky.embed.recordWithMedia" => {
                let media = self.media_view(row, embed.get("media")?)?;
                if depth > 0 {
                    return Some(media);
                }
                let uri = embed.get("record")?.get("record")?.get("uri")?.as_str()?;
                Some(json!({
                    "$type": "app.bsky.embed.recordWithMedia#view",
                    "record": {"$type": "app.bsky.embed.record#view", "record": self.record_view(uri, depth)},
                    "media": media,
                }))
            }
            _ => self.media_view(row, embed),
        }
    }

    /// A reply-ref entry: post view, blocked post or not-found post.
    #[must_use]
    pub fn reply_ref_view(&self, uri: &str) -> Value {
        let author = uri_author(uri).unwrap_or_default();
        if self.author_blocked(author) {
            return json!({
                "$type": "app.bsky.feed.defs#blockedPost", "uri": uri, "blocked": true,
                "author": {"did": author, "viewer": self.profiles.viewer.view(author)}
            });
        }
        let listable = self.gates.get(uri).copied().unwrap_or(Gate::Visible).listable();
        match self.post_view(uri).filter(|_| listable) {
            Some(v) => v,
            None => json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": uri, "notFound": true}),
        }
    }
}

/// One `feed_items` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FeedItem {
    pub uri: String,
    pub originator_did: String,
    pub post_uri: String,
    pub kind: String,
    pub sort_at: DateTime<Utc>,
}

impl Hydrator {
    /// Load posts (plus their quoted, parent and root posts), authors,
    /// viewer state, labels, counts and gates.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn post_context(&self, viewer: &Viewer, uris: &[String], mode: CountMode) -> Result<PostContext, sqlx::Error> {
        self.post_context_for(Some(viewer), uris, mode).await
    }

    /// [`Hydrator::post_context`]; with `viewer = None` no viewer state is
    /// computed at all (Thread Lens, §10.3: never `actor_viewer` or
    /// `post_viewer`).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(crate) async fn post_context_for(
        &self,
        viewer: Option<&Viewer>,
        uris: &[String],
        mode: CountMode,
    ) -> Result<PostContext, sqlx::Error> {
        let uris: Vec<String> = uris.iter().filter(|u| !is_private_uri(u)).cloned().collect();
        let mut posts = self.load_posts(&uris).await?;
        let mut extra: Vec<String> = Vec::new();
        for row in posts.values() {
            for u in [&row.quote_uri, &row.reply_parent_uri, &row.reply_root_uri].into_iter().flatten() {
                if !posts.contains_key(u) && !is_private_uri(u) && !extra.contains(u) {
                    extra.push(u.clone());
                }
            }
            if let Some(q) = row.embed.as_ref().and_then(|e| e.get("record")).and_then(|r| r.get("record")).and_then(|r| r.get("uri")).and_then(Value::as_str)
                && !posts.contains_key(q)
                && !extra.iter().any(|e| e == q)
            {
                extra.push(q.to_string());
            }
        }
        posts.extend(self.load_posts(&extra).await?);
        let all_uris: Vec<String> = posts.keys().cloned().collect();
        let mut authors: Vec<String> = Self::authors_of(&all_uris);
        for u in &extra {
            if let Some(a) = uri_author(u)
                && !authors.iter().any(|x| x == a)
            {
                authors.push(a.to_string());
            }
        }
        let (profiles, viewers) = match viewer {
            Some(v) => (self.profile_context(v, &authors, None).await?, self.post_viewer(v, &all_uris).await?),
            None => (self.anonymous_profile_context(&authors).await?, HashMap::new()),
        };
        let labels = self.load_labels(&all_uris).await?;
        let counts = self.constellation_counts(&uris, mode).await;
        let gates = moderation::gate_many(self.pool(), &all_uris).await?;
        Ok(PostContext {
            posts,
            profiles,
            viewers,
            labels,
            counts,
            gates,
        })
    }

    /// One public `postView`, for a private thread's `publicParent`
    /// (§7.2): local and not hidden, else `None`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn public_post_view(&self, viewer: &Viewer, uri: &str) -> Result<Option<PublicEmit<Value>>, sqlx::Error> {
        if is_private_uri(uri) {
            return Ok(None);
        }
        let wanted = [uri.to_string()];
        let ctx = self.post_context(viewer, &wanted, CountMode::List).await?;
        Ok(ctx.post_view(uri).map(PublicEmit::new))
    }

    /// `app.bsky.feed.getPosts` (≤ 25): private-tier, hidden and unknown
    /// URIs are omitted; `!hide` posts are returned with their label (§7.10).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_posts(&self, viewer: &Viewer, uris: &[String]) -> ViewResult<Value> {
        let started = Instant::now();
        let mut wanted: Vec<String> = Vec::new();
        for u in uris.iter().take(25) {
            if !is_private_uri(u) && !wanted.contains(u) {
                wanted.push(u.clone());
            }
        }
        let ctx = self.post_context(viewer, &wanted, CountMode::Sync { started }).await?;
        let mut posts: Vec<Value> = wanted.iter().filter_map(|u| ctx.post_view(u)).collect();
        let missing: Vec<String> = wanted.iter().filter(|u| !ctx.posts.contains_key(*u)).cloned().collect();
        if !missing.is_empty() {
            // §8.3 step 0: the moderation gate before any upstream source.
            let gates = moderation::gate_many(self.pool(), &missing).await?;
            let allowed: Vec<String> = missing
                .into_iter()
                .filter(|u| !gates.get(u).copied().unwrap_or(Gate::Visible).is_hidden())
                .collect();
            let upstream = Value::Array(self.upstream().posts(&allowed).await);
            if let Value::Array(views) = self.normalized(viewer, upstream).await?.0 {
                posts.extend(views);
            }
        }
        Ok(Ok(Self::emit(json!({ "posts": posts }))))
    }

    /// Render feed items as `feedViewPost`s, dropping hidden, collapsed,
    /// muted-or-blocked (timeline) and missing posts.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn feed_views(&self, viewer: &Viewer, items: &[FeedItem]) -> Result<Vec<Value>, sqlx::Error> {
        let uris: Vec<String> = items.iter().map(|i| i.post_uri.clone()).collect();
        let ctx = self.post_context(viewer, &uris, CountMode::List).await?;
        let originators: Vec<String> = items.iter().map(|i| i.originator_did.clone()).collect();
        let reposters = self.profile_context(viewer, &originators, None).await?;
        let originator_gates = moderation::gate_many(self.pool(), &originators).await?;
        let mut out = Vec::new();
        for item in items {
            if !ctx.gates.get(&item.post_uri).copied().unwrap_or(Gate::Visible).listable()
                || originator_gates.get(&item.originator_did).copied().unwrap_or(Gate::Visible).is_hidden()
            {
                continue;
            }
            let Some(post) = ctx.post_view(&item.post_uri) else { continue };
            let mut v = json!({ "post": post });
            if item.kind == "repost" {
                v["reason"] = json!({
                    "$type": "app.bsky.feed.defs#reasonRepost",
                    "by": reposters.view(&item.originator_did, ProfileKind::Basic),
                    "indexedAt": ts(item.sort_at),
                });
            }
            if let Some(row) = ctx.posts.get(&item.post_uri)
                && let (Some(parent), Some(root)) = (&row.reply_parent_uri, &row.reply_root_uri)
            {
                v["reply"] = json!({
                    "root": ctx.reply_ref_view(root),
                    "parent": ctx.reply_ref_view(parent),
                });
            }
            out.push(v);
        }
        Ok(out)
    }

    /// `app.bsky.feed.getAuthorFeed` from local `feed_items` (§5.12 filters).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_author_feed(
        &self,
        viewer: &Viewer,
        actor: &str,
        limit: u32,
        cursor: Option<&str>,
        filter: &str,
        include_pins: bool,
    ) -> ViewResult<Value> {
        let Some(did) = self.resolve_actor(actor).await? else {
            return Ok(Err(ViewError::new("InvalidRequest", "Profile not found")));
        };
        if let Some((error, message)) = moderation::gate(self.pool(), &did).await?.profile_error() {
            return Ok(Err(ViewError::new(error, message)));
        }
        let rel = self.actor_viewer(viewer, std::slice::from_ref(&did)).await?;
        if rel.blocking.contains_key(&did) {
            return Ok(Err(ViewError::new("BlockedActor", "Requester has blocked actor")));
        }
        if rel.blocked_by.contains(&did) {
            return Ok(Err(ViewError::new("BlockedByActor", "Requester is blocked by actor")));
        }
        if !self.in_scope(&did).await {
            // Out-of-scope subject: the cascade (item 7).
            return Ok(
                match self.upstream().author_feed(&did, limit, cursor, filter).await {
                    Some(v) => Ok(Self::emit(self.normalized(viewer, v).await?.0)),
                    None => Err(ViewError::new("InvalidRequest", "Profile not found")),
                },
            );
        }
        let (at, after_uri) = parse_local_cursor(cursor);
        let kinds: &[&str] = match filter {
            "posts_no_replies" => &["post", "repost"],
            _ => &["post", "reply", "repost"],
        };
        let (media, threads) = (filter == "posts_with_media", filter == "posts_and_author_threads");
        let items: Vec<FeedItem> = sqlx::query_as(
            "SELECT f.uri, f.originator_did, f.post_uri, f.kind, f.sort_at FROM feed_items f \
             LEFT JOIN posts p ON p.uri = f.post_uri \
             WHERE f.originator_did = $1 AND f.kind = ANY($2) \
               AND ($3::timestamptz IS NULL OR (f.sort_at, f.uri) < ($3, $4)) \
               AND (NOT $6 OR (f.kind <> 'repost' AND p.embed->>'$type' IN \
                    ('app.bsky.embed.images', 'app.bsky.embed.video', 'app.bsky.embed.recordWithMedia'))) \
               AND (NOT $7 OR f.kind <> 'reply' OR split_part(p.reply_root_uri, '/', 3) = $1) \
             ORDER BY f.sort_at DESC, f.uri DESC LIMIT $5",
        )
        .bind(&did)
        .bind(kinds)
        .bind(at)
        .bind(after_uri.unwrap_or_default())
        .bind(i64::from(limit))
        .bind(media)
        .bind(threads)
        .fetch_all(self.pool())
        .await?;
        let mut feed = self.feed_views(viewer, &items).await?;
        if include_pins && cursor.is_none() {
            let pinned: Option<String> = sqlx::query_scalar("SELECT profile_record #>> '{pinnedPost,uri}' FROM actors WHERE did = $1")
                .bind(&did)
                .fetch_optional(self.pool())
                .await?
                .flatten();
            if let Some(uri) = pinned.filter(|u| uri_author(u) == Some(did.as_str())) {
                let ctx = self.post_context(viewer, std::slice::from_ref(&uri), CountMode::List).await?;
                if let Some(post) = ctx.post_view(&uri) {
                    feed.retain(|f| f["post"]["uri"] != json!(uri));
                    feed.insert(0, json!({"post": post, "reason": {"$type": "app.bsky.feed.defs#reasonPin"}}));
                }
            }
        }
        let mut out = json!({ "feed": feed });
        if items.len() == usize::try_from(limit).unwrap_or(usize::MAX)
            && let Some(last) = items.last()
        {
            out["cursor"] = json!(local_cursor(last.sort_at, &last.uri));
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.feed.getTimeline` for a ready member: `feed_items` of the
    /// viewer's follows and the viewer, excluding muted authors and blocks in
    /// either direction (§7.5), reverse-chronological.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_timeline(&self, viewer: &Viewer, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.state_did() else {
            return Ok(Err(ViewError::new("NotReady", "Account indexing in progress")));
        };
        let (at, after_uri) = parse_local_cursor(cursor);
        let items: Vec<FeedItem> = sqlx::query_as(
            "SELECT f.uri, f.originator_did, f.post_uri, f.kind, f.sort_at FROM feed_items f \
             WHERE (f.originator_did = $1 OR f.originator_did IN (SELECT subject_did FROM follows WHERE author_did = $1)) \
               AND ($2::timestamptz IS NULL OR (f.sort_at, f.uri) < ($2, $3)) \
               AND NOT EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.scope = 'full' \
                   AND m.subject_did IN (f.originator_did, split_part(f.post_uri, '/', 3))) \
               AND NOT (f.kind = 'repost' AND EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 \
                   AND m.subject_did = f.originator_did AND m.scope IN ('reposts', 'reposts_and_quotes'))) \
               AND NOT EXISTS (SELECT 1 FROM mutes m JOIN posts p ON p.author_did = m.subject_did \
                   WHERE m.viewer_did = $1 AND m.scope IN ('quotes', 'reposts_and_quotes') \
                     AND p.uri = f.post_uri AND p.quote_uri IS NOT NULL) \
               AND NOT EXISTS (SELECT 1 FROM blocks b WHERE \
                   (b.author_did = $1 AND b.subject_did IN (f.originator_did, split_part(f.post_uri, '/', 3))) \
                OR (b.subject_did = $1 AND b.author_did IN (f.originator_did, split_part(f.post_uri, '/', 3)))) \
             ORDER BY f.sort_at DESC, f.uri DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(after_uri.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let feed = self.feed_views(viewer, &items).await?;
        let mut out = json!({ "feed": feed });
        if items.len() == usize::try_from(limit).unwrap_or(usize::MAX)
            && let Some(last) = items.last()
        {
            out["cursor"] = json!(local_cursor(last.sort_at, &last.uri));
        }
        Ok(Ok(Self::emit(out)))
    }

    /// A Constellation `/links` page through the read-time cache.
    pub(super) async fn links_page(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Option<crate::microcosm::LinksPage> {
        let client = self.backlinks()?;
        let key = format!("links|{target}|{collection}|{path}|{limit}|{}", cursor.unwrap_or_default());
        if let Some(page) = self.cache().get_json(&key).await {
            return Some(page);
        }
        match client.links(target, collection, path, limit, cursor).await {
            Ok(page) => {
                self.cache().put_json(&key, &page).await;
                Some(page)
            }
            Err(error) => {
                tracing::debug!(event = "constellation_links_failed", %error, target, "falling back to local rows");
                None
            }
        }
    }

    /// Actors who liked or reposted `uri`: Constellation first, local rows
    /// only when Constellation is unavailable (§7.1). Entries are
    /// `(did, record uri, createdAt)`.
    async fn interaction_actors(
        &self,
        uri: &str,
        collection: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<(Vec<(String, DateTime<Utc>)>, Option<String>), sqlx::Error> {
        let remote_cursor = cursor.and_then(|c| c.strip_prefix("C:"));
        if !cursor.is_some_and(|c| c.starts_with("L:"))
            && let Some(page) = self.links_page(uri, collection, ".subject.uri", limit, remote_cursor).await
        {
            let mut seen = HashSet::new();
            let actors = page
                .linking_records
                .iter()
                .filter(|r| seen.insert(r.did.clone()))
                .map(|r| (r.did.clone(), tid_timestamp(&r.rkey).unwrap_or_else(Utc::now)))
                .collect();
            return Ok((actors, page.cursor.map(|c| format!("C:{c}"))));
        }
        let table = if collection == NSID_LIKE { "likes" } else { "reposts" };
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(&format!(
            "SELECT author_did, uri, created_at FROM {table} WHERE subject_uri = $1 \
             AND ($2::timestamptz IS NULL OR (created_at, uri) < ($2, $3)) \
             ORDER BY created_at DESC, uri DESC LIMIT $4"
        ))
        .bind(uri)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(_, u, t)| local_cursor(*t, u)))
            .flatten();
        Ok((rows.into_iter().map(|(d, _, t)| (d, t)).collect(), next))
    }

    async fn visible_actors(&self, viewer: &Viewer, actors: Vec<(String, DateTime<Utc>)>) -> Result<(Vec<(String, DateTime<Utc>)>, ProfileContext), sqlx::Error> {
        let dids: Vec<String> = actors.iter().map(|(d, _)| d.clone()).collect();
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        let ctx = self.profile_context(viewer, &dids, None).await?;
        let kept = actors
            .into_iter()
            .filter(|(d, _)| {
                !gates.get(d).copied().unwrap_or(Gate::Visible).is_hidden() && !ctx.viewer.blocked_either_way(d)
            })
            .collect();
        Ok((kept, ctx))
    }

    /// `app.bsky.feed.getLikes`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_likes(&self, viewer: &Viewer, uri: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        if is_private_uri(uri) || moderation::gate(self.pool(), uri).await?.is_hidden() {
            return Ok(Ok(Self::emit(json!({ "uri": uri, "likes": [] }))));
        }
        let (actors, next) = self.interaction_actors(uri, NSID_LIKE, limit, cursor).await?;
        let (actors, ctx) = self.visible_actors(viewer, actors).await?;
        let likes: Vec<Value> = actors
            .iter()
            .map(|(d, t)| json!({"indexedAt": ts(*t), "createdAt": ts(*t), "actor": ctx.view(d, ProfileKind::View)}))
            .collect();
        let mut out = json!({ "uri": uri, "likes": likes });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.feed.getRepostedBy`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_reposted_by(&self, viewer: &Viewer, uri: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        if is_private_uri(uri) || moderation::gate(self.pool(), uri).await?.is_hidden() {
            return Ok(Ok(Self::emit(json!({ "uri": uri, "repostedBy": [] }))));
        }
        let (actors, next) = self.interaction_actors(uri, NSID_REPOST, limit, cursor).await?;
        let (actors, ctx) = self.visible_actors(viewer, actors).await?;
        let reposted: Vec<Value> = actors.iter().map(|(d, _)| ctx.view(d, ProfileKind::View)).collect();
        let mut out = json!({ "uri": uri, "repostedBy": reposted });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.feed.getQuotes`: Constellation first over `.embed.record.uri`
    /// and `.embed.record.record.uri`, merged; local `posts_quote` otherwise.
    /// Non-local quoting posts need Slingshot (item 7) and are omitted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_quotes(&self, viewer: &Viewer, uri: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        if is_private_uri(uri) || moderation::gate(self.pool(), uri).await?.is_hidden() {
            return Ok(Ok(Self::emit(json!({ "uri": uri, "posts": [] }))));
        }
        let (c1, c2) = cursor
            .and_then(|c| c.strip_prefix("C:"))
            .and_then(|c| c.split_once('|'))
            .map_or((None, None), |(a, b)| ((!a.is_empty()).then_some(a), (!b.is_empty()).then_some(b)));
        let mut uris: Vec<String> = Vec::new();
        let mut next: Option<String> = None;
        let remote = if cursor.is_some_and(|c| c.starts_with("L:")) {
            None
        } else {
            let a = if cursor.is_none() || c1.is_some() {
                self.links_page(uri, NSID_POST, ".embed.record.uri", limit, c1).await
            } else {
                None
            };
            let b = if cursor.is_none() || c2.is_some() {
                self.links_page(uri, NSID_POST, ".embed.record.record.uri", limit, c2).await
            } else {
                None
            };
            (a.is_some() || b.is_some()).then_some((a, b))
        };
        if let Some((a, b)) = remote {
            for page in [&a, &b].into_iter().flatten() {
                for r in &page.linking_records {
                    let u = format!("at://{}/{}/{}", r.did, r.collection, r.rkey);
                    if !uris.contains(&u) {
                        uris.push(u);
                    }
                }
            }
            let na = a.and_then(|p| p.cursor).unwrap_or_default();
            let nb = b.and_then(|p| p.cursor).unwrap_or_default();
            if !na.is_empty() || !nb.is_empty() {
                next = Some(format!("C:{na}|{nb}"));
            }
        } else {
            let (at, after) = parse_local_cursor(cursor);
            let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
                "SELECT uri, created_at FROM posts WHERE quote_uri = $1 \
                 AND ($2::timestamptz IS NULL OR (created_at, uri) < ($2, $3)) \
                 ORDER BY created_at DESC, uri DESC LIMIT $4",
            )
            .bind(uri)
            .bind(at)
            .bind(after.unwrap_or_default())
            .bind(i64::from(limit))
            .fetch_all(self.pool())
            .await?;
            if rows.len() == usize::try_from(limit).unwrap_or(usize::MAX)
                && let Some((u, t)) = rows.last()
            {
                next = Some(local_cursor(*t, u));
            }
            uris = rows.into_iter().map(|(u, _)| u).collect();
        }
        let ctx = self.post_context(viewer, &uris, CountMode::List).await?;
        let posts: Vec<Value> = uris
            .iter()
            .filter(|u| ctx.gates.get(*u).copied().unwrap_or(Gate::Visible).listable())
            .filter(|u| !ctx.author_blocked(uri_author(u).unwrap_or_default()))
            .filter_map(|u| ctx.post_view(u))
            .collect();
        let mut out = json!({ "uri": uri, "posts": posts });
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

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn tids_and_cursors() {
        let t = tid_timestamp("3jzfcijpj2z2a").expect("tid");
        assert!(t.timestamp() > 1_600_000_000 && t.timestamp() < 1_800_000_000, "{t}");
        assert_eq!(tid_timestamp("self"), None);
        let now = Utc::now();
        let c = local_cursor(now, "at://x/y/z");
        let (at, uri) = parse_local_cursor(Some(&c));
        assert_eq!(uri.as_deref(), Some("at://x/y/z"));
        assert_eq!(at.map(|a| a.timestamp_micros()), Some(now.timestamp_micros()));
        assert_eq!(parse_local_cursor(Some("C:abc")), (None, None));
        assert_eq!(clamp_limit(Some(500)), 100);
        assert_eq!(clamp_limit(None), 50);
    }

    fn sets() -> ScopeSets {
        let mut s = ScopeSets::default();
        for d in [ALICE, BOB] {
            s.scope.insert(d.into());
            s.members.insert(d.into());
        }
        s
    }

    #[tokio::test]
    async fn local_labels_are_emitted_and_enforced() {
        let Some(db) = fresh_db("hydration_labels").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let taken = post(p, BOB, "3ktaken22222", "taken down", None, None).await;
        let hidden = post(p, BOB, "3khidden2222", "hidden", None, None).await;
        let nsfw = post(p, BOB, "3knsfw222222", "informational", None, None).await;
        let reply = post(p, ALICE, "3kreply22222", "graphic", Some(&nsfw), Some(&nsfw)).await;
        for (subject, val) in [(&taken, "!takedown"), (&hidden, "!hide"), (&nsfw, "porn"), (&reply, "graphic-media")] {
            exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{subject}', '{val}')")).await;
        }
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{BOB}', 'nudity')")).await;
        let h = hydrator(p, sets());
        let anon = Viewer::anonymous();
        let label_vals = |v: &Value| -> Vec<String> {
            v["labels"].as_array().into_iter().flatten().filter_map(|l| l["val"].as_str().map(str::to_string)).collect()
        };

        // Direct fetch: !takedown is not found; !hide is returned with its
        // label; informational labels ride along on post and author.
        let v = h.get_posts(&anon, &[taken.clone(), hidden.clone(), nsfw.clone()]).await.expect("db").expect("view");
        let posts = v.inner()["posts"].as_array().expect("posts");
        let uris: Vec<&str> = posts.iter().filter_map(|x| x["uri"].as_str()).collect();
        assert_eq!(uris, [hidden.as_str(), nsfw.as_str()], "!takedown is omitted even from a direct fetch");
        assert_eq!(label_vals(&posts[0]), ["!hide"]);
        assert_eq!(label_vals(&posts[1]), ["porn"]);
        assert_eq!(label_vals(&posts[1]["author"]), ["nudity"]);
        let l = &posts[1]["labels"][0];
        assert_eq!((l["src"].as_str(), l["uri"].as_str()), (Some(crate::hydration::testing::LABELER), Some(nsfw.as_str())));
        assert!(l["cts"].is_string());

        // Lists: both !takedown and !hide are omitted; informational stays.
        let feed = h.get_author_feed(&anon, BOB, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        let listed: Vec<&str> = feed.inner()["feed"].as_array().expect("feed").iter().filter_map(|i| i["post"]["uri"].as_str()).collect();
        assert_eq!(listed, [nsfw.as_str()]);
        assert_eq!(label_vals(&feed.inner()["feed"][0]["post"]), ["porn"]);

        // Profiles and threads carry labels too.
        let profile = h.get_profile(&anon, BOB).await.expect("db").expect("view");
        assert_eq!(label_vals(profile.inner()), ["nudity"]);
        let thread = h.get_post_thread(&anon, &nsfw, 6, 20).await.expect("db").expect("view");
        assert_eq!(label_vals(&thread.inner()["thread"]["post"]), ["porn"]);
        assert_eq!(label_vals(&thread.inner()["thread"]["replies"][0]["post"]), ["graphic-media"]);
        let e = h.get_post_thread(&anon, &taken, 6, 20).await.expect("db").expect_err("taken down");
        assert_eq!(e.error, "NotFound");

        // A takedown on the author hides the profile and every post.
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{BOB}', '!takedown')")).await;
        let e = h.get_profile(&anon, BOB).await.expect("db").expect_err("taken down");
        assert_eq!(e.error, "AccountTakedown");
        let v = h.get_posts(&anon, std::slice::from_ref(&nsfw)).await.expect("db").expect("view");
        assert_eq!(v.inner()["posts"], json!([]));
        db.drop_db().await;
    }

    #[tokio::test]
    async fn posts_author_feed_and_timeline() {
        let Some(db) = fresh_db("hydration_feed").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        exec(p, "INSERT INTO members (did, source, ready_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin', now())").await;
        let root = post(p, BOB, "3kaaaaaaaaaa2", "root", None, None).await;
        let reply = post(p, ALICE, "3kaaaaaaaaaa3", "reply", Some(&root), Some(&root)).await;
        let hidden = post(p, BOB, "3kaaaaaaaaaa4", "hidden", None, None).await;
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{hidden}', '!hide')")).await;
        let private = format!("at://{BOB}/tools.kryphocron.feed.postPrivate/3kaaaaaaaaaa5");
        let h = hydrator(p, sets());
        let anon = Viewer::anonymous();

        // getPosts: private omitted; !hide returned with its label.
        let v = h.get_posts(&anon, &[root.clone(), private, hidden.clone()]).await.expect("db").expect("view");
        let posts = v.inner()["posts"].as_array().expect("posts");
        assert_eq!(posts.len(), 2);
        assert_eq!(posts[1]["labels"][0]["val"], "!hide");
        assert_eq!(posts[0]["author"]["handle"], "bob.test");
        assert_eq!(posts[0]["$type"], "app.bsky.feed.defs#postView");

        // Author feed: the !hide post is omitted from lists.
        let feed = h.get_author_feed(&anon, "bob.test", 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        assert_eq!(feed.inner()["feed"].as_array().map(Vec::len), Some(1));

        // Alice's reply carries its reply context.
        let feed = h.get_author_feed(&anon, ALICE, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        assert_eq!(feed.inner()["feed"][0]["reply"]["parent"]["uri"], json!(root));
        let no_replies = h.get_author_feed(&anon, ALICE, 50, None, "posts_no_replies", false).await.expect("db").expect("view");
        assert_eq!(no_replies.inner()["feed"].as_array().map(Vec::len), Some(0));

        // Timeline for Alice (ready): her reply; Bob's root once she follows him.
        let me = h.viewer(Some(ALICE)).await.expect("viewer");
        let tl = h.get_timeline(&me, 50, None).await.expect("db").expect("view");
        assert_eq!(tl.inner()["feed"].as_array().map(Vec::len), Some(1));
        exec(p, "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/1', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', '3k', now())").await;
        let tl = h.get_timeline(&me, 50, None).await.expect("db").expect("view");
        assert_eq!(tl.inner()["feed"].as_array().map(Vec::len), Some(2));
        // Muting Bob removes his items (and Alice's reply stays).
        exec(p, "INSERT INTO mutes (viewer_did, subject_did) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb')").await;
        let tl = h.get_timeline(&me, 50, None).await.expect("db").expect("view");
        assert_eq!(tl.inner()["feed"].as_array().map(Vec::len), Some(1));
        // Scoped mutes (B-34). Bob reposts Alice's reply and writes a quote
        // post: the timeline holds Alice's reply, Bob's root, his repost and
        // his quote post.
        exec(p, "DELETE FROM mutes").await;
        let quote = post(p, BOB, "3kaaaaaaaaaa6", "quoting", None, None).await;
        exec(p, &format!("UPDATE posts SET quote_uri = '{reply}' WHERE uri = '{quote}'")).await;
        exec(p, &format!(
            "INSERT INTO feed_items (uri, originator_did, post_uri, kind, sort_at) VALUES \
             ('at://{BOB}/app.bsky.feed.repost/1', '{BOB}', '{reply}', 'repost', now())"
        )).await;
        let kinds = |v: &Value| -> Vec<(String, bool)> {
            v["feed"]
                .as_array()
                .expect("feed")
                .iter()
                .map(|i| (i["post"]["uri"].as_str().expect("uri").to_string(), i.get("reason").is_some()))
                .collect()
        };
        let all = h.get_timeline(&me, 50, None).await.expect("db").expect("view");
        assert_eq!(kinds(all.inner()).len(), 4);
        let mute = |scope: &'static str| async move {
            exec(p, "DELETE FROM mutes").await;
            exec(p, &format!(
                "INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{ALICE}', '{BOB}', '{scope}')"
            )).await;
        };
        // reposts: only Bob's repost item goes.
        mute("reposts").await;
        let tl = h.get_timeline(&me, 50, None).await.expect("db").expect("view");
        let k = kinds(tl.inner());
        assert_eq!(k.len(), 3, "{k:?}");
        assert!(k.iter().all(|(_, repost)| !repost));
        assert!(k.iter().any(|(u, _)| *u == root) && k.iter().any(|(u, _)| *u == quote));
        // quotes: only Bob's quote post goes.
        mute("quotes").await;
        let tl = h.get_timeline(&me, 50, None).await.expect("db").expect("view");
        let k = kinds(tl.inner());
        assert_eq!(k.len(), 3, "{k:?}");
        assert!(k.iter().all(|(u, _)| *u != quote) && k.iter().any(|(_, repost)| *repost));
        // both: the repost and the quote post go; Bob's plain post stays.
        mute("reposts_and_quotes").await;
        let tl = h.get_timeline(&me, 50, None).await.expect("db").expect("view");
        let k = kinds(tl.inner());
        assert_eq!(k.len(), 2, "{k:?}");
        assert!(k.iter().any(|(u, _)| *u == root) && k.iter().any(|(u, _)| *u == reply));
        exec(p, &format!(
            "DELETE FROM feed_items WHERE uri = 'at://{BOB}/app.bsky.feed.repost/1' OR post_uri = '{quote}'"
        )).await;
        exec(p, &format!("DELETE FROM posts WHERE uri = '{quote}'")).await;

        // Paging.
        exec(p, "DELETE FROM mutes").await;
        let page = h.get_timeline(&me, 1, None).await.expect("db").expect("view");
        let cursor = page.inner()["cursor"].as_str().expect("cursor").to_string();
        let next = h.get_timeline(&me, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_ne!(page.inner()["feed"][0]["post"]["uri"], next.inner()["feed"][0]["post"]["uri"]);

        // Local likes fallback (no Constellation in tests).
        exec(p, &format!("INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES \
                 ('at://{ALICE}/app.bsky.feed.like/1', '{ALICE}', '{root}', '3k', now())")).await;
        let likes = h.get_likes(&anon, &root, 50, None).await.expect("db").expect("view");
        assert_eq!(likes.inner()["likes"][0]["actor"]["did"], ALICE);
        let _ = reply;
        db.drop_db().await;
    }
}
