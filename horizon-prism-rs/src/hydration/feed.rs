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
use crate::config::BookmarkCount;
use crate::db::locks::uri_author;
use crate::firehose::normalize::blob_cid;
use crate::moderation::{self, Gate};
use crate::hydration::HydrationError;
use crate::xrpc::pools::{Exhausted, Requester};

/// `list_feed_partial_total`: list feeds answered from the in-scope members
/// only, because no upstream answered.
pub const METRIC_LIST_FEED_PARTIAL: &str = "list_feed_partial_total";

pub const NSID_LIKE: &str = "app.bsky.feed.like";
pub const NSID_REPOST: &str = "app.bsky.feed.repost";
pub const NSID_POST: &str = "app.bsky.feed.post";

/// The one media classifier (design-v3 §8.4): `images`, `video`, `gallery`
/// or `None` for a post record, looking at the top-level embed and at
/// `recordWithMedia.media`. The author-feed media filters use it for
/// records that have no `posts` row; the local filter's SQL is the same
/// test.
#[must_use]
pub fn media_type(record: &Value) -> Option<&'static str> {
    let embed = record.get("embed")?;
    let kind = match embed.get("$type").and_then(Value::as_str)? {
        "app.bsky.embed.recordWithMedia" => embed.pointer("/media/$type").and_then(Value::as_str)?,
        other => other,
    };
    match kind {
        "app.bsky.embed.images" => Some("images"),
        "app.bsky.embed.video" => Some("video"),
        "app.bsky.embed.gallery" => Some("gallery"),
        _ => None,
    }
}

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
    /// By post: its own valid threadgate, as a `threadgateView`.
    pub threadgates: HashMap<String, Value>,
    /// By post: the `bookmarkCount` to emit. A post with no entry carries
    /// none (`bookmarks.emit_count`).
    pub bookmark_counts: HashMap<String, i64>,
    /// By quoted post: the quoting posts its author detached (its valid
    /// postgate's `detachedEmbeddingUris`).
    pub detached: HashMap<String, HashSet<String>>,
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
        if let Some(n) = self.bookmark_counts.get(uri) {
            v.insert("bookmarkCount".into(), json!(n));
        }
        v.insert("indexedAt".into(), json!(ts(row.indexed_at)));
        v.insert("viewer".into(), self.viewers.get(uri).cloned().unwrap_or_else(|| json!({})));
        v.insert("labels".into(), Value::Array(self.labels.get(uri).cloned().unwrap_or_default()));
        if let Some(gate) = self.threadgates.get(uri) {
            v.insert("threadgate".into(), gate.clone());
        }
        Some(Value::Object(v))
    }

    /// The view of the record that post `quoting` embeds: a local post, or
    /// not-found, blocked, or detached by the quoted post's author
    /// (design-v3 §8.3).
    fn record_view(&self, uri: &str, quoting: &str, depth: u8) -> Value {
        let author = uri_author(uri).unwrap_or_default();
        if is_private_uri(uri) || !uri.contains(&format!("/{}/", super::feed::NSID_POST)) {
            return json!({"$type": "app.bsky.embed.record#viewNotFound", "uri": uri, "notFound": true});
        }
        if self.detached.get(uri).is_some_and(|quotes| quotes.contains(quoting)) {
            return json!({"$type": "app.bsky.embed.record#viewDetached", "uri": uri, "detached": true});
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
            // design-v3 §8.4. `items` is a union in the lexicon, so every
            // view item carries its `$type` (images#view items do not). Only
            // `#image` record items are read, and an item missing its
            // required `alt` or `aspectRatio` is dropped, not invented.
            "app.bsky.embed.gallery" => {
                let items: Vec<Value> = embed
                    .get("items")?
                    .as_array()?
                    .iter()
                    .filter(|item| item.get("$type").and_then(Value::as_str) == Some("app.bsky.embed.gallery#image"))
                    .filter_map(|item| {
                        let cid = item.get("image").and_then(blob_cid)?;
                        Some(json!({
                            "$type": "app.bsky.embed.gallery#viewImage",
                            "thumbnail": blob_url(pds.as_deref(), did, &cid)?,
                            "fullsize": blob_url(pds.as_deref(), did, &cid)?,
                            "alt": item.get("alt").and_then(Value::as_str)?,
                            "aspectRatio": item.get("aspectRatio").filter(|ar| ar.is_object())?.clone(),
                        }))
                    })
                    .collect();
                Some(json!({"$type": "app.bsky.embed.gallery#view", "items": items}))
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
                Some(json!({"$type": "app.bsky.embed.record#view", "record": self.record_view(uri, &row.uri, depth)}))
            }
            "app.bsky.embed.recordWithMedia" => {
                let media = self.media_view(row, embed.get("media")?)?;
                if depth > 0 {
                    return Some(media);
                }
                let uri = embed.get("record")?.get("record")?.get("uri")?.as_str()?;
                Some(json!({
                    "$type": "app.bsky.embed.recordWithMedia#view",
                    "record": {"$type": "app.bsky.embed.record#view", "record": self.record_view(uri, &row.uri, depth)},
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
    pub async fn post_context(&self, viewer: &Viewer, req: &Requester, uris: &[String], mode: CountMode) -> Result<PostContext, sqlx::Error> {
        self.post_context_for(Some(viewer), req, uris, mode).await
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
        req: &Requester,
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
            Some(v) => (self.profile_context(v, req, &authors, None).await?, self.post_viewer(v, &all_uris).await?),
            None => (self.anonymous_profile_context(&authors).await?, HashMap::new()),
        };
        let labels = self.load_labels(&all_uris).await?;
        let counts = self.constellation_counts(req, &uris, mode).await;
        let gates = moderation::gate_many(self.pool(), &all_uris).await?;
        // A postgate counts only under its post's record key and author.
        let detached: Vec<(String, Vec<String>)> = sqlx::query_as(
            "SELECT post_uri, detached_embedding_uris FROM postgates WHERE post_uri = ANY($1) \
               AND cardinality(detached_embedding_uris) > 0 \
               AND author_did = split_part(post_uri, '/', 3) AND rkey = split_part(post_uri, '/', 5)",
        )
        .bind(&all_uris)
        .fetch_all(self.pool())
        .await?;
        // A post's own valid threadgate, as its view (design-v3 §8.2).
        let gate_rows: Vec<(String, String, String, Value, Option<Value>)> = sqlx::query_as(
            "SELECT post_uri, uri, cid, record, allow FROM threadgates WHERE post_uri = ANY($1) \
               AND author_did = split_part(post_uri, '/', 3) AND rkey = split_part(post_uri, '/', 5)",
        )
        .bind(&all_uris)
        .fetch_all(self.pool())
        .await?;
        let anonymous = Viewer::anonymous();
        let mut threadgates = HashMap::new();
        for (post, uri, cid, record, allow) in gate_rows {
            let view = self.threadgate_view(viewer.unwrap_or(&anonymous), req, &uri, &cid, record, allow.as_ref()).await?;
            threadgates.insert(post, view);
        }
        // `bookmarkCount`, when the operator lets it be shown (§8.2). In a
        // small community a low count points at one member's private
        // bookmark, so by default it is never emitted.
        let emit = self.inner.cfg.bookmarks_emit_count;
        let mut bookmark_counts: HashMap<String, i64> = HashMap::new();
        if emit != BookmarkCount::Never {
            let rows: Vec<(String, i64)> =
                sqlx::query_as("SELECT post_uri, count(*) FROM bookmarks WHERE post_uri = ANY($1) GROUP BY post_uri")
                    .bind(&all_uris)
                    .fetch_all(self.pool())
                    .await?;
            if emit == BookmarkCount::Always {
                bookmark_counts.extend(all_uris.iter().map(|u| (u.clone(), 0)));
                bookmark_counts.extend(rows);
            } else {
                let floor = i64::from(self.inner.cfg.bookmarks_count_floor);
                bookmark_counts.extend(rows.into_iter().filter(|(_, n)| *n >= floor));
            }
        }
        Ok(PostContext {
            posts,
            profiles,
            viewers,
            labels,
            counts,
            gates,
            threadgates,
            bookmark_counts,
            detached: detached.into_iter().map(|(post, quotes)| (post, quotes.into_iter().collect())).collect(),
        })
    }

    /// One public `postView`, for a private thread's `publicParent`
    /// (§7.2): local and not hidden, else `None`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn public_post_view(&self, viewer: &Viewer, req: &Requester, uri: &str) -> Result<Option<PublicEmit<Value>>, sqlx::Error> {
        if is_private_uri(uri) {
            return Ok(None);
        }
        let wanted = [uri.to_string()];
        let ctx = self.post_context(viewer, req, &wanted, CountMode::List).await?;
        Ok(ctx.post_view(uri).map(PublicEmit::new))
    }

    /// `app.bsky.feed.getPosts` (≤ 25): private-tier, hidden and unknown
    /// URIs are omitted; `!hide` posts are returned with their label (§7.10).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_posts(&self, viewer: &Viewer, req: &Requester, uris: &[String]) -> ViewResult<Value> {
        let started = Instant::now();
        let mut wanted: Vec<String> = Vec::new();
        for u in uris.iter().take(25) {
            if !is_private_uri(u) && !wanted.contains(u) {
                wanted.push(u.clone());
            }
        }
        let ctx = self.post_context(viewer, req, &wanted, CountMode::Sync { started }).await?;
        let mut posts: Vec<Value> = wanted.iter().filter_map(|u| ctx.post_view(u)).collect();
        // `viewer.replyDisabled`, from each stored post's root (§8.2: a
        // single-object view). Decided once per root.
        let mut by_root: HashMap<String, Option<bool>> = HashMap::new();
        for post in &mut posts {
            let Some(uri) = post.get("uri").and_then(Value::as_str) else { continue };
            let Some(row) = ctx.posts.get(uri) else { continue };
            let root = row.reply_root_uri.clone().unwrap_or_else(|| row.uri.clone());
            let disabled = match by_root.get(&root) {
                Some(known) => *known,
                None => {
                    let found = self.reply_disabled_in(viewer, req, &root).await?;
                    by_root.insert(root, found);
                    found
                }
            };
            if let Some(disabled) = disabled {
                post["viewer"]["replyDisabled"] = json!(disabled);
            }
        }
        let missing: Vec<String> = wanted.iter().filter(|u| !ctx.posts.contains_key(*u)).cloned().collect();
        if !missing.is_empty() {
            // §8.3 step 0: the moderation gate before any upstream source.
            let gates = moderation::gate_many(self.pool(), &missing).await?;
            // A post this AppView saw deleted is not fetched from upstream,
            // whose copies (and the read-time cache of them) can outlive the
            // delete.
            let deleted: HashSet<String> =
                sqlx::query_scalar("SELECT uri FROM record_tombstones WHERE uri = ANY($1)")
                    .bind(&missing)
                    .fetch_all(self.pool())
                    .await?
                    .into_iter()
                    .collect();
            let allowed: Vec<String> = missing
                .into_iter()
                .filter(|u| !gates.get(u).copied().unwrap_or(Gate::Visible).is_hidden() && !deleted.contains(u))
                .collect();
            let upstream = Value::Array(self.upstream().posts(req, &allowed).await);
            if let Value::Array(views) = self.normalized(viewer, req, upstream).await?.0 {
                posts.extend(views);
            }
        }
        Ok(Ok(Self::emit(json!({ "posts": posts }))))
    }

    /// By post: its place and the length of its thread's contiguous run by
    /// the root's author (design-v3 §5.1 "OP thread", §8.2), for the posts
    /// on such a run of two or more. Stored posts only, at most
    /// `op_thread.max_walk` of them: a post is left out when the run is
    /// longer, when a post above it is not stored, or when its author is out
    /// of scope (whose replies are not all here).
    async fn op_paths(&self, posts: &HashMap<String, PostRow>, uris: &[String]) -> Result<HashMap<String, (i32, i32)>, sqlx::Error> {
        let max = i32::try_from(self.inner.cfg.op_thread_max_walk).unwrap_or(i32::MAX);
        if max < 2 {
            return Ok(HashMap::new());
        }
        let starts: Vec<String> = {
            let scope = self.scope().read_guard().await;
            let mut starts: Vec<String> = Vec::new();
            for uri in uris {
                let Some(row) = posts.get(uri) else { continue };
                let by_root_author = row.reply_root_uri.as_deref().is_none_or(|root| root.split('/').nth(2) == Some(row.author_did.as_str()));
                if by_root_author && scope.in_scope(&row.author_did) && !starts.contains(uri) {
                    starts.push(uri.clone());
                }
            }
            starts
        };
        if starts.is_empty() {
            return Ok(HashMap::new());
        }
        // Up: each step must be the author's earliest reply to a post by the
        // same author. The walk is on the run only when it ends at a root.
        let up: Vec<(String, i32, bool)> = sqlx::query_as(
            "WITH RECURSIVE up(start, uri, parent, author, at, n) AS ( \
               SELECT uri, uri, reply_parent_uri, author_did, created_at, 0 FROM posts WHERE uri = ANY($1) \
               UNION ALL \
               SELECT up.start, q.uri, q.reply_parent_uri, up.author, q.created_at, up.n + 1 \
               FROM up JOIN posts q ON q.uri = up.parent \
               WHERE up.n < $2 AND q.author_did = up.author \
                 AND NOT EXISTS (SELECT 1 FROM posts s WHERE s.reply_parent_uri = up.parent AND s.author_did = up.author \
                                 AND (s.created_at, s.uri) < (up.at, up.uri)) \
             ) SELECT DISTINCT ON (start) start, n, parent IS NULL FROM up ORDER BY start, n DESC",
        )
        .bind(&starts)
        .bind(max)
        .fetch_all(self.pool())
        .await?;
        let above: HashMap<String, i32> = up.into_iter().filter(|(_, _, at_root)| *at_root).map(|(start, n, _)| (start, n)).collect();
        if above.is_empty() {
            return Ok(HashMap::new());
        }
        let on_run: Vec<String> = above.keys().cloned().collect();
        // Down: the author's earliest reply to each post in turn.
        let down: Vec<(String, Option<i32>)> = sqlx::query_as(
            "WITH RECURSIVE down(start, uri, author, n) AS ( \
               SELECT uri, uri, author_did, 0 FROM posts WHERE uri = ANY($1) \
               UNION ALL \
               SELECT d.start, nx.uri, d.author, d.n + 1 FROM down d \
               CROSS JOIN LATERAL (SELECT s.uri FROM posts s WHERE s.reply_parent_uri = d.uri AND s.author_did = d.author \
                                   ORDER BY s.created_at, s.uri LIMIT 1) nx \
               WHERE d.n < $2 \
             ) SELECT start, max(n) FROM down GROUP BY start",
        )
        .bind(&on_run)
        .bind(max)
        .fetch_all(self.pool())
        .await?;
        let mut out = HashMap::new();
        for (start, below) in down {
            let Some(n) = above.get(&start) else { continue };
            let count = n + 1 + below.unwrap_or(0);
            if (2..=max).contains(&count) {
                out.insert(start, (n + 1, count));
            }
        }
        Ok(out)
    }

    /// Render feed items as `feedViewPost`s, dropping hidden, collapsed,
    /// muted-or-blocked (timeline) and missing posts.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn feed_views(&self, viewer: &Viewer, req: &Requester, items: &[FeedItem]) -> Result<Vec<Value>, sqlx::Error> {
        let uris: Vec<String> = items.iter().map(|i| i.post_uri.clone()).collect();
        let ctx = self.post_context(viewer, req, &uris, CountMode::List).await?;
        let originators: Vec<String> = items.iter().map(|i| i.originator_did.clone()).collect();
        let reposters = self.profile_context(viewer, req, &originators, None).await?;
        let originator_gates = moderation::gate_many(self.pool(), &originators).await?;
        let op_paths = self.op_paths(&ctx.posts, &uris).await?;
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
            if let Some((index, count)) = op_paths.get(&item.post_uri) {
                v["opThreadPostIndex"] = json!(index);
                v["opThreadPostCount"] = json!(count);
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
    #[allow(clippy::too_many_arguments)]
    pub async fn get_author_feed(
        &self,
        viewer: &Viewer,
        req: &Requester,
        actor: &str,
        limit: u32,
        cursor: Option<&str>,
        filter: &str,
        include_pins: bool,
    ) -> ViewResult<Value> {
        let Some(did) = self.resolve_actor(req, actor).await? else {
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
                match self.upstream().author_feed(req, &did, limit, cursor, filter).await {
                    Some(v) => Ok(Self::emit(self.normalized(viewer, req, v).await?.0)),
                    None => Err(ViewError::new("InvalidRequest", "Profile not found")),
                },
            );
        }
        // A `P:` cursor continues an in-scope author's feed from the author's
        // PDS, below the records stored here (design-v3 §11 G-3).
        if cursor.is_some_and(|c| c.starts_with("P:")) {
            return match self.upstream().author_feed(req, &did, limit, cursor, filter).await {
                Some(v) => Ok(Ok(Self::emit(self.normalized(viewer, req, v).await?.0))),
                None => match req.exhausted() {
                    // No budget for the listing: a 429, never an empty page.
                    Some(exhausted) => Err(HydrationError::NoBudget(exhausted)),
                    None => Ok(Ok(Self::emit(json!({ "feed": [] })))),
                },
            };
        }
        let (at, after_uri) = parse_local_cursor(cursor);
        let kinds: &[&str] = match filter {
            "posts_no_replies" => &["post", "repost"],
            _ => &["post", "reply", "repost"],
        };
        let threads = filter == "posts_and_author_threads";
        // The media embed types a media filter keeps (§8.4): the same test
        // as `media_type`, on the top-level embed or on recordWithMedia's
        // media. Video is its own filter. `posts.media_type` replaces the
        // expression once its migration lands (§II-15 item 11).
        let media: Option<Vec<&str>> = match filter {
            "posts_with_media" => Some(vec!["app.bsky.embed.images", "app.bsky.embed.gallery"]),
            "posts_with_video" => Some(vec!["app.bsky.embed.video"]),
            _ => None,
        };
        let items: Vec<FeedItem> = sqlx::query_as(
            "SELECT f.uri, f.originator_did, f.post_uri, f.kind, f.sort_at FROM feed_items f \
             LEFT JOIN posts p ON p.uri = f.post_uri \
             WHERE f.originator_did = $1 AND f.kind = ANY($2) \
               AND ($3::timestamptz IS NULL OR (f.sort_at, f.uri) < ($3, $4)) \
               AND ($6::text[] IS NULL OR (f.kind <> 'repost' AND \
                    (CASE WHEN p.embed->>'$type' = 'app.bsky.embed.recordWithMedia' \
                          THEN p.embed->'media'->>'$type' ELSE p.embed->>'$type' END) = ANY($6))) \
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
        let mut feed = self.feed_views(viewer, req, &items).await?;
        if include_pins && cursor.is_none() {
            let pinned: Option<String> = sqlx::query_scalar("SELECT profile_record #>> '{pinnedPost,uri}' FROM actors WHERE did = $1")
                .bind(&did)
                .fetch_optional(self.pool())
                .await?
                .flatten();
            if let Some(uri) = pinned.filter(|u| uri_author(u) == Some(did.as_str())) {
                let ctx = self.post_context(viewer, req, std::slice::from_ref(&uri), CountMode::List).await?;
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
        } else if self.upstream().lists_descending(req, &did).await {
            // The local rows are exhausted. On a PDS that lists newest
            // first, the feed continues there, below the oldest record of
            // each collection stored here (§II-7.1). On any other PDS it
            // ends here.
            let (posts, reposts): (Option<String>, Option<String>) = sqlx::query_as(
                "SELECT (SELECT min(rkey) FROM posts WHERE author_did = $1), \
                        (SELECT min(split_part(uri, '/', 5)) FROM feed_items WHERE originator_did = $1 AND kind = 'repost')",
            )
            .bind(&did)
            .fetch_one(self.pool())
            .await?;
            out["cursor"] = json!(format!("P:{}:{}", posts.unwrap_or_default(), reposts.unwrap_or_default()));
        }
        Ok(Ok(Self::emit(out)))
    }

    /// Drop the `feedViewPost`s the viewer's own mutes and blocks exclude
    /// (design-v3 §5.3 "Where mutes apply", "Where blocks apply"), from a
    /// feed that is already hydrated:
    /// - a block in either direction with the post's author or the reposter;
    /// - a full mute of either;
    /// - a repost by an account whose reposts are muted;
    /// - a quote post by an account whose quote posts are muted.
    ///
    /// Anyone but a ready member has no such state, and gets the feed back.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(super) async fn without_excluded(&self, viewer: &Viewer, feed: Vec<Value>) -> Result<Vec<Value>, sqlx::Error> {
        if viewer.state_did().is_none() {
            return Ok(feed);
        }
        let did = |item: &Value, pointer: &str| item.pointer(pointer).and_then(Value::as_str).map(str::to_string);
        let dids: Vec<String> = feed
            .iter()
            .flat_map(|item| [did(item, "/post/author/did"), did(item, "/reason/by/did")])
            .flatten()
            .collect();
        let state = self.actor_viewer(viewer, &dids).await?;
        // A quote post: by its stored row, or, for a post that is not
        // stored, by its record's embed.
        let uris: Vec<String> = feed.iter().filter_map(|item| did(item, "/post/uri")).collect();
        let stored_quotes: HashSet<String> = sqlx::query_scalar("SELECT uri FROM posts WHERE uri = ANY($1) AND quote_uri IS NOT NULL")
            .bind(&uris)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .collect();
        Ok(feed
            .into_iter()
            .filter(|item| {
                let author = did(item, "/post/author/did");
                let reposter = did(item, "/reason/by/did");
                let quotes = did(item, "/post/uri").is_some_and(|u| stored_quotes.contains(&u))
                    || item
                        .pointer("/post/record/embed/$type")
                        .and_then(Value::as_str)
                        .is_some_and(|t| matches!(t, "app.bsky.embed.record" | "app.bsky.embed.recordWithMedia"));
                let hidden = |d: &String| state.blocked_either_way(d) || state.muted.contains(d);
                !(author.iter().chain(&reposter).any(hidden)
                    || reposter.as_ref().is_some_and(|d| state.muted_reposts.contains(d))
                    || (quotes && author.as_ref().is_some_and(|d| state.muted_quotes.contains(d))))
            })
            .collect())
    }

    /// `app.bsky.feed.getListFeed` (design-v3 §5.5): the posts and reposts
    /// of a list's members, newest first.
    ///
    /// Local `feed_items` answer only when the list is served locally (its
    /// owner is a ready member) and every member of it is in scope, so the
    /// feed is complete. Otherwise an upstream AppView is asked. When none
    /// answers, a locally served list is answered from the members that are
    /// in scope, and `list_feed_partial_total` counts it.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_list_feed(&self, viewer: &Viewer, req: &Requester, list: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let unknown = || ViewError::new("UnknownList", "List not found");
        let Some((_, local)) = self.load_list(req, list).await? else {
            return Ok(Err(unknown()));
        };
        let owner = uri_author(list).unwrap_or_default().to_string();
        let gates = moderation::gate_many(self.pool(), &[owner, list.to_string()]).await?;
        if gates.values().any(|g| g.is_hidden()) {
            return Ok(Err(unknown()));
        }
        let members: Vec<String> = if local {
            sqlx::query_scalar("SELECT DISTINCT subject_did FROM list_items WHERE list_uri = $1")
                .bind(list)
                .fetch_all(self.pool())
                .await?
        } else {
            Vec::new()
        };
        let in_scope: Vec<String> = {
            let scope = self.scope().read_guard().await;
            members.iter().filter(|d| scope.in_scope(d)).cloned().collect()
        };
        let complete = local && in_scope.len() == members.len();
        // A `PL:` cursor continues a partial local answer: a local cursor
        // behind a `P`, so it is never sent upstream.
        let partial_cursor = cursor.filter(|c| c.starts_with("PL:")).map(|c| &c[1..]);
        if !complete && partial_cursor.is_none() {
            if let Some(found) = self.upstream().list_feed(req, list, limit, cursor).await {
                let (mut found, _) = self.normalized(viewer, req, found).await?;
                if let Some(feed) = found.get_mut("feed").and_then(Value::as_array_mut) {
                    *feed = self.without_excluded(viewer, std::mem::take(feed)).await?;
                }
                return Ok(Ok(Self::emit(found)));
            }
            if !local {
                return Ok(Err(unknown()));
            }
            metrics::counter!(METRIC_LIST_FEED_PARTIAL).increment(1);
        }
        let (at, after_uri) = parse_local_cursor(partial_cursor.or(cursor));
        let items: Vec<FeedItem> = sqlx::query_as(
            "SELECT f.uri, f.originator_did, f.post_uri, f.kind, f.sort_at FROM feed_items f \
             WHERE f.originator_did = ANY($1) AND f.kind IN ('post', 'reply', 'repost') \
               AND ($2::timestamptz IS NULL OR (f.sort_at, f.uri) < ($2, $3)) \
             ORDER BY f.sort_at DESC, f.uri DESC LIMIT $4",
        )
        .bind(&in_scope)
        .bind(at)
        .bind(after_uri.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let feed = self.without_excluded(viewer, self.feed_views(viewer, req, &items).await?).await?;
        let mut out = json!({ "feed": feed });
        if items.len() == usize::try_from(limit).unwrap_or(usize::MAX)
            && let Some(last) = items.last()
        {
            let next = local_cursor(last.sort_at, &last.uri);
            out["cursor"] = json!(if complete { next } else { format!("P{next}") });
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
    pub async fn get_timeline(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.state_did() else {
            return Ok(Err(ViewError::new("NotReady", "Account indexing in progress")));
        };
        let (at, after_uri) = parse_local_cursor(cursor);
        let items: Vec<FeedItem> = sqlx::query_as(
            "SELECT f.uri, f.originator_did, f.post_uri, f.kind, f.sort_at FROM feed_items f \
             WHERE (f.originator_did = $1 OR f.originator_did IN (SELECT subject_did FROM follows WHERE author_did = $1)) \
               AND ($2::timestamptz IS NULL OR (f.sort_at, f.uri) < ($2, $3)) \
             ORDER BY f.sort_at DESC, f.uri DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(after_uri.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        // Every scanned item is hydrated; the viewer's mutes and blocks drop
        // items afterwards (design-v3 §8.1 "Hydrate before filtering"). A
        // page can be short; the cursor follows the scan.
        let feed = self.without_excluded(viewer, self.feed_views(viewer, req, &items).await?).await?;
        let mut out = json!({ "feed": feed });
        if items.len() == usize::try_from(limit).unwrap_or(usize::MAX)
            && let Some(last) = items.last()
        {
            out["cursor"] = json!(local_cursor(last.sort_at, &last.uri));
        }
        Ok(Ok(Self::emit(out)))
    }

    /// A Constellation `/links` page through the read-time cache. `Ok(None)`
    /// is Constellation unavailable, where the caller falls back to local
    /// rows.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] when the requester has no budget for the call. That is
    /// no answer, and never the local fallback (design-v3 §II-8.3).
    pub(super) async fn links_page(
        &self,
        req: &Requester,
        target: &str,
        collection: &str,
        path: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Option<crate::microcosm::LinksPage>, Exhausted> {
        let Some(client) = self.backlinks() else {
            return Ok(None);
        };
        let key = format!("links|{target}|{collection}|{path}|{limit}|{}", cursor.unwrap_or_default());
        if let Some(page) = self.cache().get_json(&key).await {
            return Ok(Some(page));
        }
        self.draw_constellation(req)?;
        match client.links(target, collection, path, limit, cursor).await {
            Ok(page) => {
                self.cache().put_json(&key, &page).await;
                Ok(Some(page))
            }
            Err(error) => {
                tracing::debug!(event = "constellation_links_failed", %error, target, "falling back to local rows");
                Ok(None)
            }
        }
    }

    /// Actors who liked or reposted `uri`: Constellation first, local rows
    /// only when Constellation is unavailable (§7.1). Entries are
    /// `(did, record uri, createdAt)`.
    async fn interaction_actors(
        &self,
        req: &Requester,
        uri: &str,
        collection: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<(Vec<(String, DateTime<Utc>)>, Option<String>), HydrationError> {
        let remote_cursor = cursor.and_then(|c| c.strip_prefix("C:"));
        let remote = if cursor.is_some_and(|c| c.starts_with("L:")) {
            None
        } else {
            self.links_page(req, uri, collection, ".subject.uri", limit, remote_cursor)
                .await
                .map_err(HydrationError::NoBudget)?
        };
        if let Some(page) = remote {
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

    async fn visible_actors(&self, viewer: &Viewer, req: &Requester, actors: Vec<(String, DateTime<Utc>)>) -> Result<(Vec<(String, DateTime<Utc>)>, ProfileContext), sqlx::Error> {
        let dids: Vec<String> = actors.iter().map(|(d, _)| d.clone()).collect();
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        let ctx = self.profile_context(viewer, req, &dids, None).await?;
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
    pub async fn get_likes(&self, viewer: &Viewer, req: &Requester, uri: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        if is_private_uri(uri) || moderation::gate(self.pool(), uri).await?.is_hidden() {
            return Ok(Ok(Self::emit(json!({ "uri": uri, "likes": [] }))));
        }
        let (actors, next) = self.interaction_actors(req, uri, NSID_LIKE, limit, cursor).await?;
        let (actors, ctx) = self.visible_actors(viewer, req, actors).await?;
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
    pub async fn get_reposted_by(&self, viewer: &Viewer, req: &Requester, uri: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        if is_private_uri(uri) || moderation::gate(self.pool(), uri).await?.is_hidden() {
            return Ok(Ok(Self::emit(json!({ "uri": uri, "repostedBy": [] }))));
        }
        let (actors, next) = self.interaction_actors(req, uri, NSID_REPOST, limit, cursor).await?;
        let (actors, ctx) = self.visible_actors(viewer, req, actors).await?;
        let reposted: Vec<Value> = actors.iter().map(|(d, _)| ctx.view(d, ProfileKind::View)).collect();
        let mut out = json!({ "uri": uri, "repostedBy": reposted });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.feed.getQuotes`: Constellation first over `.embed.record.uri`
    /// and `.embed.record.record.uri`, merged; local `posts_quote` otherwise.
    /// Quoting posts not stored here come from Slingshot.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_quotes(&self, viewer: &Viewer, req: &Requester, uri: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
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
            // No budget for either listing is a 429 for the page: a partial
            // page would lose the other listing's cursor.
            let a = if cursor.is_none() || c1.is_some() {
                self.links_page(req, uri, NSID_POST, ".embed.record.uri", limit, c1)
                    .await
                    .map_err(HydrationError::NoBudget)?
            } else {
                None
            };
            let b = if cursor.is_none() || c2.is_some() {
                self.links_page(req, uri, NSID_POST, ".embed.record.record.uri", limit, c2)
                    .await
                    .map_err(HydrationError::NoBudget)?
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
        let ctx = self.post_context(viewer, req, &uris, CountMode::List).await?;
        let listable = |u: &&String| {
            ctx.gates.get(*u).copied().unwrap_or(Gate::Visible).listable() && !ctx.author_blocked(uri_author(u).unwrap_or_default())
        };
        // Quoting posts not stored here are fetched from Slingshot through
        // the read-time cache, at most `limit` per page (design-v3 §11 G-7).
        let missing: Vec<String> = uris
            .iter()
            .filter(listable)
            .filter(|u| ctx.post_view(u).is_none())
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .cloned()
            .collect();
        let mut remote: HashMap<String, Value> = HashMap::new();
        if !missing.is_empty() {
            let fetched = Value::Array(self.upstream().post_records(req, &missing).await);
            if let Value::Array(views) = self.normalized(viewer, req, fetched).await?.0 {
                for v in views {
                    if let Some(u) = v.get("uri").and_then(Value::as_str).map(str::to_string) {
                        remote.insert(u, v);
                    }
                }
            }
        }
        let posts: Vec<Value> = uris
            .iter()
            .filter(listable)
            .filter_map(|u| ctx.post_view(u).or_else(|| remote.remove(u)))
            .collect();
        let mut out = json!({ "uri": uri, "posts": posts });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.feed.getActorLikes` (design-v3 §5.4): the posts the caller
    /// liked, newest like first. `actor` must be the caller. A liked post
    /// that is gone, hidden, or by an author with a block is left out.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_actor_likes(&self, viewer: &Viewer, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        if self.resolve_actor(req, actor).await?.as_deref() != Some(me) {
            return Ok(Err(ViewError::new("InvalidRequest", "actor must be the requesting account")));
        }
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT uri, subject_uri, created_at FROM likes WHERE author_did = $1 \
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
            .then(|| rows.last().map(|(like, _, t)| local_cursor(*t, like)))
            .flatten();
        let mut uris: Vec<String> = Vec::new();
        for (_, subject, _) in rows {
            if !is_private_uri(&subject) && !uris.contains(&subject) {
                uris.push(subject);
            }
        }
        let ctx = self.post_context(viewer, req, &uris, CountMode::List).await?;
        let listable = |u: &&String| {
            ctx.gates.get(*u).copied().unwrap_or(Gate::Visible).listable() && !ctx.author_blocked(uri_author(u).unwrap_or_default())
        };
        // Liked posts not stored here come from Slingshot through the
        // read-time cache, at most `limit` per page.
        let missing: Vec<String> = uris
            .iter()
            .filter(listable)
            .filter(|u| ctx.post_view(u).is_none())
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .cloned()
            .collect();
        let mut remote: HashMap<String, Value> = HashMap::new();
        if !missing.is_empty() {
            let fetched = Value::Array(self.upstream().post_records(req, &missing).await);
            if let Value::Array(views) = self.normalized(viewer, req, fetched).await?.0 {
                for v in views {
                    if let Some(u) = v.get("uri").and_then(Value::as_str).map(str::to_string) {
                        remote.insert(u, v);
                    }
                }
            }
        }
        let feed: Vec<Value> = uris
            .iter()
            .filter(listable)
            .filter_map(|u| ctx.post_view(u).or_else(|| remote.remove(u)))
            .map(|post| json!({ "post": post }))
            .collect();
        let mut out = json!({ "feed": feed });
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

    /// A Constellation whose `/links` answers `records` for `path`, and an
    /// empty page for any other path.
    async fn fake_links(path: &'static str, records: Value) -> String {
        use axum::extract::Query;
        use axum::routing::get;

        let router = axum::Router::new().route(
            "/links",
            get(move |Query(q): Query<HashMap<String, String>>| {
                let records = if q.get("path").map(String::as_str) == Some(path) { records.clone() } else { json!([]) };
                async move { axum::Json(json!({"total": 0, "linking_records": records})) }
            }),
        );
        crate::test_support::serve_fake(router).await
    }

    /// design-v3 §11 G-3: an in-scope author's feed continues from a PDS
    /// that lists newest first.
    #[tokio::test]
    async fn an_in_scope_author_feed_continues_from_a_descending_pds() {
        use std::sync::Arc;

        use crate::hydration::testing::{FixedUpstream, hydrator_with};

        let Some(db) = fresh_db("hydration_author_feed_handover").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        post(p, ALICE, "3kbbbbbbbbbb2", "newer", None, None).await;
        post(p, ALICE, "3kaaaaaaaaaa2", "oldest stored", None, None).await;
        let anon = Viewer::anonymous();
        let req = Requester::test();

        // A PDS that lists oldest first, or whose order is unknown: the
        // feed ends with the local rows.
        let h = hydrator_with(p, sets(), Arc::new(FixedUpstream::default()));
        let page = h.get_author_feed(&anon, &req, ALICE, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        assert_eq!(page.inner()["feed"].as_array().map(Vec::len), Some(2));
        assert!(page.inner().get("cursor").is_none());

        // A PDS that lists newest first: the feed hands over below the
        // oldest stored record of each collection.
        let handover = "P:3kaaaaaaaaaa2:".to_string();
        let older = format!("at://{ALICE}/{NSID_POST}/3jzzzzzzzzzz2");
        let mut up = FixedUpstream {
            descending: true,
            ..FixedUpstream::default()
        };
        up.feeds.insert(
            (ALICE.to_string(), Some(handover.clone())),
            json!({"feed": [{"post": {"$type": "app.bsky.feed.defs#postView", "uri": older, "cid": "bafyold",
                "author": {"did": ALICE, "handle": "alice.test", "viewer": {}, "labels": []},
                "record": {"$type": NSID_POST, "text": "older, from the PDS", "createdAt": "2025-01-01T00:00:00Z"},
                "indexedAt": "2025-01-01T00:00:00Z", "viewer": {}, "labels": []}}],
                "cursor": "P:3jzzzzzzzzzz2:"}),
        );
        let h = hydrator_with(p, sets(), Arc::new(up));
        let page = h.get_author_feed(&anon, &req, ALICE, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        assert_eq!(page.inner()["cursor"], json!(handover));
        let next = h
            .get_author_feed(&anon, &req, ALICE, 50, Some(&handover), "posts_with_replies", false)
            .await
            .expect("db")
            .expect("view");
        assert_eq!(next.inner()["feed"][0]["post"]["uri"], json!(older));
        assert_eq!(next.inner()["cursor"], "P:3jzzzzzzzzzz2:");
        // Past the end of the PDS listing: an empty page and no cursor.
        let end = h
            .get_author_feed(&anon, &req, ALICE, 50, Some("P:3jzzzzzzzzzz2:"), "posts_with_replies", false)
            .await
            .expect("db")
            .expect("view");
        assert_eq!(end.inner(), &json!({"feed": []}));
        // A full local page keeps its local cursor.
        let page = h.get_author_feed(&anon, &req, ALICE, 1, None, "posts_with_replies", false).await.expect("db").expect("view");
        assert!(page.inner()["cursor"].as_str().is_some_and(|c| c.starts_with("L:")));
        db.drop_db().await;
    }

    /// design-v3 §11 G-7: quoting posts not stored here come from Slingshot.
    #[tokio::test]
    async fn get_quotes_fetches_quoting_posts_it_does_not_store() {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        use crate::hydration::testing::{FixedUpstream, hydrator_full};

        let Some(db) = fresh_db("hydration_quotes_upstream").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let subject = post(p, ALICE, "subject", "the quoted post", None, None).await;
        let local = post(p, BOB, "localquote", "bob quotes", None, None).await;
        let outsider = "did:plc:oooooooooooooooooooooooo";
        let far = format!("at://{outsider}/{NSID_POST}/farquote");
        let gone = format!("at://{outsider}/{NSID_POST}/gone");
        let base = fake_links(
            ".embed.record.uri",
            json!([
                {"did": outsider, "collection": NSID_POST, "rkey": "farquote"},
                {"did": BOB, "collection": NSID_POST, "rkey": "localquote"},
                {"did": outsider, "collection": NSID_POST, "rkey": "gone"},
            ]),
        )
        .await;
        let mut up = FixedUpstream::default();
        up.posts.insert(
            far.clone(),
            json!({"$type": "app.bsky.feed.defs#postView", "uri": far, "cid": "bafyfar",
                   "author": {"did": outsider, "handle": "far.example", "viewer": {}, "labels": []},
                   "record": {"$type": NSID_POST, "text": "a far quote", "createdAt": "2026-09-26T00:00:00Z"},
                   "indexedAt": "2026-09-26T00:00:00Z", "viewer": {}, "labels": []}),
        );
        let up = Arc::new(up);
        let h = hydrator_full(p, sets(), up.clone(), Some(&base));
        let req = Requester::test();
        let out = h.get_quotes(&Viewer::anonymous(), &req, &subject, 50, None).await.expect("db").expect("view");
        let uris: Vec<&str> = out.inner()["posts"].as_array().expect("posts").iter().filter_map(|v| v["uri"].as_str()).collect();
        // Listing order; the record nobody has is left out.
        assert_eq!(uris, [far.as_str(), local.as_str()]);
        assert_eq!(out.inner()["posts"][0]["record"]["text"], "a far quote");
        assert_eq!(up.calls.load(Ordering::SeqCst), 1, "one batch for the posts not stored here");
        let _ = gone;
        db.drop_db().await;
    }

    #[test]
    fn media_classifier() {
        let of = |embed: Value| media_type(&json!({"$type": NSID_POST, "embed": embed}));
        assert_eq!(of(json!({"$type": "app.bsky.embed.images"})), Some("images"));
        assert_eq!(of(json!({"$type": "app.bsky.embed.video"})), Some("video"));
        assert_eq!(of(json!({"$type": "app.bsky.embed.gallery"})), Some("gallery"));
        assert_eq!(of(json!({"$type": "app.bsky.embed.external"})), None);
        assert_eq!(of(json!({"$type": "app.bsky.embed.record"})), None);
        let with = |media: &str| of(json!({"$type": "app.bsky.embed.recordWithMedia", "media": {"$type": media}}));
        assert_eq!(with("app.bsky.embed.gallery"), Some("gallery"));
        assert_eq!(with("app.bsky.embed.video"), Some("video"));
        assert_eq!(with("app.bsky.embed.external"), None);
        assert_eq!(media_type(&json!({"$type": NSID_POST, "text": "plain"})), None);
    }

    /// design-v3 §8.4 and §11 G-2: gallery views, and the media and video
    /// filters of `getAuthorFeed`.
    /// B-42: a quote its target's author detached renders as `viewDetached`.
    #[tokio::test]
    async fn a_detached_quote_shows_no_record() {
        let Some(db) = fresh_db("hydration_detached_quote").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let quoted = post(p, ALICE, "3kaaaaaaaaaa2", "original", None, None).await;
        let detached = post(p, BOB, "3kaaaaaaaaaa3", "detached quote", None, None).await;
        let kept = post(p, BOB, "3kaaaaaaaaaa4", "kept quote", None, None).await;
        for quoting in [&detached, &kept] {
            sqlx::query("UPDATE posts SET quote_uri = $1, embed = $2 WHERE uri = $3")
                .bind(&quoted)
                .bind(json!({"$type": "app.bsky.embed.record", "record": {"uri": quoted, "cid": "bafyq"}}))
                .bind(quoting)
                .execute(p)
                .await
                .expect("embed");
        }
        // The valid gate detaches one quote. A gate under another record
        // key names the other, and is ignored.
        for (rkey, names) in [("3kaaaaaaaaaa2", &detached), ("other", &kept)] {
            sqlx::query(
                "INSERT INTO postgates (uri, author_did, rkey, cid, rev, post_uri, detached_embedding_uris, record, created_at) \
                 VALUES ($1, $2, $3, 'bafypg', '3k', $4, $5, '{}', now())",
            )
            .bind(format!("at://{ALICE}/app.bsky.feed.postgate/{rkey}"))
            .bind(ALICE)
            .bind(rkey)
            .bind(&quoted)
            .bind(vec![names.clone()])
            .execute(p)
            .await
            .expect("postgate");
        }
        let h = hydrator(p, ScopeSets::default());
        let v = h.get_posts(&Viewer::anonymous(), &Requester::test(), &[detached.clone(), kept.clone()]).await.expect("db").expect("view");
        let posts = v.inner()["posts"].as_array().expect("posts");
        assert_eq!(
            posts[0]["embed"]["record"],
            json!({"$type": "app.bsky.embed.record#viewDetached", "uri": quoted, "detached": true})
        );
        assert_eq!(posts[1]["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord");
        db.drop_db().await;
    }

    #[tokio::test]
    async fn feed_items_carry_their_place_in_the_authors_thread() {
        let Some(db) = fresh_db("hydration_op_thread").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let one = post(p, BOB, "3kaaaaaaaaaa2", "one", None, None).await;
        let two = post(p, BOB, "3kaaaaaaaaaa3", "two", Some(&one), Some(&one)).await;
        let three = post(p, BOB, "3kaaaaaaaaaa4", "three", Some(&two), Some(&one)).await;
        // Off the run: a later second reply by Bob to the root, and a reply
        // by someone else.
        let aside = post(p, BOB, "3kaaaaaaaaaa5", "aside", Some(&one), Some(&one)).await;
        post(p, ALICE, "3kaaaaaaaaaa6", "hello", Some(&one), Some(&one)).await;
        let alone = post(p, BOB, "3kaaaaaaaaaa7", "alone", None, None).await;
        let h = hydrator(p, sets());
        let place = |v: &Value, uri: &str| -> (Option<Value>, Option<Value>) {
            let item = v["feed"].as_array().expect("feed").iter().find(|i| i["post"]["uri"] == uri).expect("item");
            (item.get("opThreadPostIndex").cloned(), item.get("opThreadPostCount").cloned())
        };
        let feed = h.get_author_feed(&Viewer::anonymous(), &Requester::test(), BOB, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        for (uri, index) in [(&one, 1), (&two, 2), (&three, 3)] {
            assert_eq!(place(feed.inner(), uri), (Some(json!(index)), Some(json!(3))), "{uri}");
        }
        assert_eq!(place(feed.inner(), &aside), (None, None));
        assert_eq!(place(feed.inner(), &alone), (None, None));
        // A run longer than `op_thread.max_walk` (4 here) is not worked out.
        let four = post(p, BOB, "3kaaaaaaaaaa8", "four", Some(&three), Some(&one)).await;
        post(p, BOB, "3kaaaaaaaaaa9", "five", Some(&four), Some(&one)).await;
        let feed = h.get_author_feed(&Viewer::anonymous(), &Requester::test(), BOB, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        for uri in [&one, &three, &four] {
            assert_eq!(place(feed.inner(), uri), (None, None), "{uri}");
        }
        db.drop_db().await;
    }

    /// design-v3 §5.4: a member's own likes, without blocked authors.
    #[tokio::test]
    async fn actor_likes_are_the_callers_own() {
        let Some(db) = fresh_db("hydration_actor_likes").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let first = post(p, BOB, "3kaaaaaaaaaa2", "first", None, None).await;
        let second = post(p, BOB, "3kaaaaaaaaaa3", "second", None, None).await;
        for (n, subject) in [(1, &first), (2, &second)] {
            exec(p, &format!(
                "INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES \
                 ('at://{ALICE}/app.bsky.feed.like/{n}', '{ALICE}', '{subject}', 'bafy', '3k', now() + interval '{n} seconds')"
            )).await;
        }
        let h = hydrator(p, ScopeSets::default());
        let me = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let req = Requester::test();
        let uris = |v: &Value| -> Vec<String> {
            v["feed"].as_array().expect("feed").iter().map(|i| i["post"]["uri"].as_str().expect("uri").to_string()).collect()
        };
        // Newest like first; by handle too; no per-item cursor.
        let l = h.get_actor_likes(&me, &req, "alice.test", 50, None).await.expect("db").expect("view");
        assert_eq!(uris(l.inner()), [second.clone(), first.clone()]);
        assert!(l.inner()["feed"][0].get("cursor").is_none());
        assert_eq!(l.inner()["feed"][0]["post"]["viewer"]["like"].as_str().map(|s| s.contains("/app.bsky.feed.like/")), Some(true));
        // Someone else's likes are not served.
        let e = h.get_actor_likes(&me, &req, BOB, 50, None).await.expect("db").expect_err("other");
        assert!(e.message.contains("requesting account"));
        // Paging.
        let page = h.get_actor_likes(&me, &req, ALICE, 1, None).await.expect("db").expect("view");
        let cursor = page.inner()["cursor"].as_str().expect("cursor").to_string();
        let rest = h.get_actor_likes(&me, &req, ALICE, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(uris(rest.inner()), std::slice::from_ref(&first));
        // A block with the author leaves the post out.
        exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{BOB}/app.bsky.graph.block/1', '{BOB}', '{ALICE}', '3k', now())")).await;
        let l = h.get_actor_likes(&me, &req, ALICE, 50, None).await.expect("db").expect("view");
        assert_eq!(l.inner()["feed"], json!([]));
        db.drop_db().await;
    }

    #[tokio::test]
    async fn gallery_views_and_media_filters() {
        let Some(db) = fresh_db("hydration_gallery").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let blob = |cid: &str| json!({"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/png", "size": 1});
        let gallery = json!({"$type": "app.bsky.embed.gallery", "items": [
            {"$type": "app.bsky.embed.gallery#image", "image": blob("bafyg1"), "alt": "one",
             "aspectRatio": {"width": 4, "height": 3}},
            // Missing its required aspectRatio: dropped, not invented.
            {"$type": "app.bsky.embed.gallery#image", "image": blob("bafyg2"), "alt": "no ratio"},
            // Another item type: skipped.
            {"$type": "app.example.other#thing", "image": blob("bafyg3"), "alt": "x",
             "aspectRatio": {"width": 1, "height": 1}},
        ]});
        let plain = post(p, BOB, "3kaaaaaaaaaa2", "plain", None, None).await;
        let images = post(p, BOB, "3kaaaaaaaaaa3", "images", None, None).await;
        let video = post(p, BOB, "3kaaaaaaaaaa4", "video", None, None).await;
        let with_gallery = post(p, BOB, "3kaaaaaaaaaa5", "gallery", None, None).await;
        let quote_gallery = post(p, BOB, "3kaaaaaaaaaa6", "quote with gallery", None, None).await;
        let set = |uri: String, embed: Value| async move {
            sqlx::query("UPDATE posts SET embed = $1 WHERE uri = $2")
                .bind(embed)
                .bind(uri)
                .execute(p)
                .await
                .expect("embed");
        };
        set(images.clone(), json!({"$type": "app.bsky.embed.images", "images": [{"image": blob("bafyi1"), "alt": "i"}]})).await;
        set(video.clone(), json!({"$type": "app.bsky.embed.video", "video": blob("bafyv1")})).await;
        set(with_gallery.clone(), gallery.clone()).await;
        set(
            quote_gallery.clone(),
            json!({"$type": "app.bsky.embed.recordWithMedia",
                   "record": {"$type": "app.bsky.embed.record", "record": {"uri": plain, "cid": "bafyplain"}},
                   "media": gallery}),
        )
        .await;
        let h = hydrator(p, sets());
        let anon = Viewer::anonymous();
        let req = Requester::test();

        let v = h.get_posts(&anon, &req,&[with_gallery.clone(), quote_gallery.clone()]).await.expect("db").expect("view");
        let posts = v.inner()["posts"].as_array().expect("posts");
        let view = &posts[0]["embed"];
        assert_eq!(view["$type"], "app.bsky.embed.gallery#view");
        assert_eq!(view["items"].as_array().map(Vec::len), Some(1));
        let item = &view["items"][0];
        assert_eq!(item["$type"], "app.bsky.embed.gallery#viewImage");
        assert_eq!(item["alt"], "one");
        assert_eq!(item["aspectRatio"], json!({"width": 4, "height": 3}));
        let url = format!("https://pds.example.com/xrpc/com.atproto.sync.getBlob?did={BOB}&cid=bafyg1");
        assert_eq!((&item["thumbnail"], &item["fullsize"]), (&json!(url), &json!(url)));
        assert!(item.get("thumb").is_none());
        // recordWithMedia with gallery media keeps the quoted record.
        let rwm = &posts[1]["embed"];
        assert_eq!(rwm["$type"], "app.bsky.embed.recordWithMedia#view");
        assert_eq!(rwm["media"]["$type"], "app.bsky.embed.gallery#view");
        assert_eq!(rwm["record"]["record"]["uri"], json!(plain));

        let uris = |filter: &'static str| {
            let h = &h;
            let anon = &anon;
            let req = &req;
            async move {
                let feed = h.get_author_feed(anon, req, BOB, 50, None, filter, false).await.expect("db").expect("view");
                let mut uris: Vec<String> = feed.inner()["feed"]
                    .as_array()
                    .expect("feed")
                    .iter()
                    .map(|i| i["post"]["uri"].as_str().expect("uri").to_string())
                    .collect();
                uris.sort();
                uris
            }
        };
        // Gallery counts as media, directly and inside recordWithMedia;
        // video is its own filter.
        assert_eq!(uris("posts_with_media").await, [images, with_gallery, quote_gallery]);
        assert_eq!(uris("posts_with_video").await, [video]);
        assert_eq!(uris("posts_with_replies").await.len(), 5);
        db.drop_db().await;
    }

    /// A post this AppView saw deleted is not fetched from upstream, whose
    /// copy can outlive the delete (Phase B, 2026-10-14); one it holds no
    /// tombstone for still is.
    #[tokio::test]
    async fn get_posts_does_not_fetch_a_deleted_post_upstream() {
        let Some(db) = fresh_db("hydration_get_posts_tombstoned").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        let deleted = format!("at://{ALICE}/{NSID_POST}/3kdeleted222");
        let elsewhere = format!("at://{ALICE}/{NSID_POST}/3kelsewhere2");
        let view = |uri: &str| {
            json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "bafyup",
                "author": {"did": ALICE, "handle": "alice.test", "viewer": {}, "labels": []},
                "record": {"$type": NSID_POST, "text": "upstream copy", "createdAt": "2026-10-14T00:00:00Z"},
                "indexedAt": "2026-10-14T00:00:00Z", "viewer": {}, "labels": []})
        };
        let mut up = crate::hydration::testing::FixedUpstream::default();
        up.posts.insert(deleted.clone(), view(&deleted));
        up.posts.insert(elsewhere.clone(), view(&elsewhere));
        exec(p, &format!("INSERT INTO record_tombstones (uri, rev) VALUES ('{deleted}', '3kaaaaaaaaaa9')")).await;
        let h = crate::hydration::testing::hydrator_with(p, sets(), std::sync::Arc::new(up));
        let v = h
            .get_posts(&Viewer::anonymous(), &Requester::test(), &[deleted.clone(), elsewhere.clone()])
            .await
            .expect("db")
            .expect("view");
        let uris: Vec<&str> = v.inner()["posts"].as_array().expect("posts").iter().filter_map(|x| x["uri"].as_str()).collect();
        assert_eq!(uris, [elsewhere.as_str()]);
        db.drop_db().await;
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
        let req = Requester::test();
        let label_vals = |v: &Value| -> Vec<String> {
            v["labels"].as_array().into_iter().flatten().filter_map(|l| l["val"].as_str().map(str::to_string)).collect()
        };

        // Direct fetch: !takedown is not found; !hide is returned with its
        // label; informational labels ride along on post and author.
        let v = h.get_posts(&anon, &req,&[taken.clone(), hidden.clone(), nsfw.clone()]).await.expect("db").expect("view");
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
        let feed = h.get_author_feed(&anon, &req,BOB, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        let listed: Vec<&str> = feed.inner()["feed"].as_array().expect("feed").iter().filter_map(|i| i["post"]["uri"].as_str()).collect();
        assert_eq!(listed, [nsfw.as_str()]);
        assert_eq!(label_vals(&feed.inner()["feed"][0]["post"]), ["porn"]);

        // Profiles and threads carry labels too.
        let profile = h.get_profile(&anon, &req, BOB).await.expect("db").expect("view");
        assert_eq!(label_vals(profile.inner()), ["nudity"]);
        let thread = h.get_post_thread(&anon, &req,&nsfw, 6, 20).await.expect("db").expect("view");
        assert_eq!(label_vals(&thread.inner()["thread"]["post"]), ["porn"]);
        assert_eq!(label_vals(&thread.inner()["thread"]["replies"][0]["post"]), ["graphic-media"]);
        let e = h.get_post_thread(&anon, &req,&taken, 6, 20).await.expect("db").expect_err("taken down");
        assert_eq!(e.error, "NotFound");

        // A takedown on the author hides the profile and every post.
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{BOB}', '!takedown')")).await;
        let e = h.get_profile(&anon, &req, BOB).await.expect("db").expect_err("taken down");
        assert_eq!(e.error, "AccountTakedown");
        let v = h.get_posts(&anon, &req,std::slice::from_ref(&nsfw)).await.expect("db").expect("view");
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
        let req = Requester::test();

        // getPosts: private omitted; !hide returned with its label.
        let v = h.get_posts(&anon, &req,&[root.clone(), private, hidden.clone()]).await.expect("db").expect("view");
        let posts = v.inner()["posts"].as_array().expect("posts");
        assert_eq!(posts.len(), 2);
        assert_eq!(posts[1]["labels"][0]["val"], "!hide");
        assert_eq!(posts[0]["author"]["handle"], "bob.test");
        assert_eq!(posts[0]["$type"], "app.bsky.feed.defs#postView");

        // Author feed: the !hide post is omitted from lists.
        let feed = h.get_author_feed(&anon, &req,"bob.test", 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        assert_eq!(feed.inner()["feed"].as_array().map(Vec::len), Some(1));

        // Alice's reply carries its reply context.
        let feed = h.get_author_feed(&anon, &req,ALICE, 50, None, "posts_with_replies", false).await.expect("db").expect("view");
        assert_eq!(feed.inner()["feed"][0]["reply"]["parent"]["uri"], json!(root));
        let no_replies = h.get_author_feed(&anon, &req,ALICE, 50, None, "posts_no_replies", false).await.expect("db").expect("view");
        assert_eq!(no_replies.inner()["feed"].as_array().map(Vec::len), Some(0));

        // Timeline for Alice (ready): her reply; Bob's root once she follows him.
        let me = h.viewer(Some(ALICE)).await.expect("viewer");
        let tl = h.get_timeline(&me, &req,50, None).await.expect("db").expect("view");
        assert_eq!(tl.inner()["feed"].as_array().map(Vec::len), Some(1));
        exec(p, "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/1', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', '3k', now())").await;
        let tl = h.get_timeline(&me, &req,50, None).await.expect("db").expect("view");
        assert_eq!(tl.inner()["feed"].as_array().map(Vec::len), Some(2));
        // Muting Bob removes his items (and Alice's reply stays).
        exec(p, "INSERT INTO mutes (viewer_did, subject_did) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb')").await;
        let tl = h.get_timeline(&me, &req,50, None).await.expect("db").expect("view");
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
        let all = h.get_timeline(&me, &req,50, None).await.expect("db").expect("view");
        assert_eq!(kinds(all.inner()).len(), 4);
        let mute = |scope: &'static str| async move {
            exec(p, "DELETE FROM mutes").await;
            exec(p, &format!(
                "INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{ALICE}', '{BOB}', '{scope}')"
            )).await;
        };
        // reposts: only Bob's repost item goes.
        mute("reposts").await;
        let tl = h.get_timeline(&me, &req,50, None).await.expect("db").expect("view");
        let k = kinds(tl.inner());
        assert_eq!(k.len(), 3, "{k:?}");
        assert!(k.iter().all(|(_, repost)| !repost));
        assert!(k.iter().any(|(u, _)| *u == root) && k.iter().any(|(u, _)| *u == quote));
        // quotes: only Bob's quote post goes.
        mute("quotes").await;
        let tl = h.get_timeline(&me, &req,50, None).await.expect("db").expect("view");
        let k = kinds(tl.inner());
        assert_eq!(k.len(), 3, "{k:?}");
        assert!(k.iter().all(|(u, _)| *u != quote) && k.iter().any(|(_, repost)| *repost));
        // both: the repost and the quote post go; Bob's plain post stays.
        mute("reposts_and_quotes").await;
        let tl = h.get_timeline(&me, &req,50, None).await.expect("db").expect("view");
        let k = kinds(tl.inner());
        assert_eq!(k.len(), 2, "{k:?}");
        assert!(k.iter().any(|(u, _)| *u == root) && k.iter().any(|(u, _)| *u == reply));
        exec(p, &format!(
            "DELETE FROM feed_items WHERE uri = 'at://{BOB}/app.bsky.feed.repost/1' OR post_uri = '{quote}'"
        )).await;
        exec(p, &format!("DELETE FROM posts WHERE uri = '{quote}'")).await;

        // Paging.
        exec(p, "DELETE FROM mutes").await;
        let page = h.get_timeline(&me, &req,1, None).await.expect("db").expect("view");
        let cursor = page.inner()["cursor"].as_str().expect("cursor").to_string();
        let next = h.get_timeline(&me, &req,1, Some(&cursor)).await.expect("db").expect("view");
        assert_ne!(page.inner()["feed"][0]["post"]["uri"], next.inner()["feed"][0]["post"]["uri"]);

        // Local likes fallback (no Constellation in tests).
        exec(p, &format!("INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES \
                 ('at://{ALICE}/app.bsky.feed.like/1', '{ALICE}', '{root}', '3k', now())")).await;
        let likes = h.get_likes(&anon, &req, &root, 50, None).await.expect("db").expect("view");
        assert_eq!(likes.inner()["likes"][0]["actor"]["did"], ALICE);
        let _ = reply;
        db.drop_db().await;
    }
}
