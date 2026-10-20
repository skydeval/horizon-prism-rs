//! The upstream seam (design-v2 §8.3, §7.1 out-of-scope column).
//!
//! [`RealUpstreamSource`] serves subjects this AppView does not index:
//! 1. the subject DID's home network (§8.2, via the `route` cache);
//! 2. the home AppView, then the cascade (§8.3 steps 4–5), skipped entirely
//!    for the `community` network (local data only, then Slingshot) while
//!    the public-indexing mode is On (C4);
//! 3. record-level fallback: Slingshot `getRecord` for single records and
//!    threads; the author PDS `listRecords` for author feeds (continuing
//!    only on PDSes that list descending, §7.1, §5.10) and follows.
//!
//! Whatever it returns is raw upstream JSON or a view synthesized from
//! records: callers normalize it (§8.6) and never write it to content
//! tables (§7.1, §7.6). Post views and profiles go through the `rt`
//! read-time cache (§7.6).
//!
//! [`NoUpstream`] answers every lookup with nothing (tests, and callers that
//! must not reach upstream).

use std::cmp::Reverse;
use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Value, json};
use url::Url;

use super::cache::ReadCache;
use super::feed::media_type;
use super::normalize::is_private_uri;
use super::{INVALID_HANDLE, blob_url};
use crate::aggregations::ScopeState;
use crate::aggregations::counts::{self, Counts, Freshness};
use crate::constellation::{Call, Cascade, ProfileWalk, ProfilesFetched, Router};
use crate::db::locks::uri_author;
use crate::firehose::backfill::{ListOrder, ListPage, ListedRecord, detect_order};
use crate::firehose::normalize::{blob_cid, normalize_record};
use crate::identity::handle_lookup::HandleOutcome;
use crate::microcosm::SlingshotClient;
use crate::net::{BodyCap, RequestOptions, Transports};
use crate::sync::HostBudgets;
use crate::xrpc::pools::{HostBuckets, Requester};

/// Upstream lookups for subjects this AppView does not index.
#[async_trait]
pub trait UpstreamSource: Send + Sync {
    /// `app.bsky.actor.getProfile` for a DID or handle.
    async fn profile(&self, req: &Requester, actor: &str) -> Option<Value>;
    /// One walk of the cascade's `plan(None)` with `getProfiles` for `dids`
    /// (at most 25), drawing on `req`'s share and ending at `until`
    /// (design-v4 §8.1 "Batch routing"). Nothing is read from or written to
    /// `rt`: the caller does that. By default nothing is found.
    async fn fetch_profiles(&self, _req: &Requester, _dids: &[String], _until: std::time::Instant) -> ProfilesFetched {
        ProfilesFetched::default()
    }
    /// Post views for URIs (`getPosts` / Slingshot), in input order.
    async fn posts(&self, req: &Requester, uris: &[String]) -> Vec<Value>;
    /// Post views built from the records alone (Slingshot, §7.1's
    /// record-level fallback), in input order. No AppView is asked, so
    /// the views carry no counts and no hydrated embeds.
    async fn post_records(&self, _req: &Requester, _uris: &[String]) -> Vec<Value> {
        Vec::new()
    }
    /// `app.bsky.feed.getPostThread`.
    async fn post_thread(&self, req: &Requester, uri: &str, depth: u32, parent_height: u32) -> Option<Value>;
    /// `app.bsky.feed.getAuthorFeed` output.
    async fn author_feed(&self, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>, filter: &str) -> Option<Value>;
    /// `app.bsky.graph.getFollows` output.
    async fn follows(&self, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> Option<Value>;
    /// Whether `actor`'s PDS lists records newest first, so a feed can
    /// continue from it (§II-7.1). `false` when unknown.
    async fn lists_descending(&self, _req: &Requester, _actor: &str) -> bool {
        false
    }
    /// One record by AT-URI (Slingshot): `{uri, cid, value}`.
    async fn record(&self, _req: &Requester, _uri: &str) -> Option<Value> {
        None
    }
    /// The items of a list this AppView does not serve from its own rows
    /// (design-v3 §5.5): `{items: [listItemView], cursor?, listItemCount?}`.
    /// `listItemCount` is present only when an AppView answered.
    async fn list_items(&self, _req: &Requester, _list: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
    /// `app.bsky.graph.getLists` output for an actor whose lists this
    /// AppView does not hold in full. `purposes` are full tokens.
    async fn lists(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>, _purposes: &[String]) -> Option<Value> {
        None
    }
    /// An upstream AppView's `app.bsky.feed.getListFeed` output.
    async fn list_feed(&self, _req: &Requester, _list: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
    /// An upstream AppView's `app.bsky.graph.getRelationships` output.
    async fn relationships(&self, _req: &Requester, _actor: &str, _others: &[String]) -> Option<Value> {
        None
    }
    /// An upstream AppView's `app.bsky.graph.getStarterPack` output, for the
    /// parts of a pack this AppView cannot build itself (the sample of a
    /// list it does not serve locally).
    async fn starter_pack(&self, _req: &Requester, _uri: &str) -> Option<Value> {
        None
    }
    /// `app.bsky.graph.getActorStarterPacks` output for an actor whose
    /// packs this AppView does not hold in full.
    async fn actor_starter_packs(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
    /// `app.nearhorizon.feed.getAuthorPolls` output for an actor whose polls
    /// this AppView does not store: one page of the actor's PDS listing,
    /// public polls only (design-v3 §5.13).
    async fn author_polls(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
    /// The DID of a handle this AppView has not indexed (§6.7), verified
    /// bidirectionally.
    async fn resolve_handle(&self, _req: &Requester, _handle: &str) -> Option<String> {
        None
    }
    /// [`UpstreamSource::resolve_handle`] with the reason kept when there is
    /// no DID (design-v3 §5.11). By default every miss is "no such handle".
    async fn handle_outcome(&self, req: &Requester, handle: &str) -> HandleOutcome {
        match self.resolve_handle(req, handle).await {
            Some(did) => HandleOutcome::Found(did),
            None => HandleOutcome::NotFound,
        }
    }
}

/// No upstream.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoUpstream;

#[async_trait]
impl UpstreamSource for NoUpstream {
    async fn profile(&self, _req: &Requester, _actor: &str) -> Option<Value> {
        None
    }
    async fn posts(&self, _req: &Requester, _uris: &[String]) -> Vec<Value> {
        Vec::new()
    }
    async fn post_thread(&self, _req: &Requester, _uri: &str, _depth: u32, _parent_height: u32) -> Option<Value> {
        None
    }
    async fn author_feed(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>, _filter: &str) -> Option<Value> {
        None
    }
    async fn follows(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
}

/// Metric: where an upstream view's counts came from.
pub const METRIC_UPSTREAM_COUNTS: &str = "upstream_view_counts_total";

/// Reads one count from a Constellation row.
type CountPick = fn(&Counts) -> Option<i32>;

/// Count fields on post views, with the `Counts` field that overrides each.
const POST_COUNT_FIELDS: [(&str, CountPick); 4] = [
    ("likeCount", |c| c.likes),
    ("repostCount", |c| c.reposts),
    ("replyCount", |c| c.replies),
    ("quoteCount", |c| c.quotes),
];

/// The count-carrying views in an upstream response: post views by URI and
/// profile views with `followersCount` by DID.
fn count_targets(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Array(a) => a.iter().for_each(|x| count_targets(x, out)),
        Value::Object(m) => {
            let is_post = m.contains_key("author") && m.contains_key("cid");
            if is_post && let Some(u) = m.get("uri").and_then(Value::as_str) {
                out.push(u.to_string());
            }
            if m.contains_key("followersCount") && let Some(d) = m.get("did").and_then(Value::as_str) {
                out.push(d.to_string());
            }
            m.values().for_each(|x| count_targets(x, out));
        }
        _ => {}
    }
}

fn apply_counts(v: &mut Value, counts: &HashMap<String, Counts>) {
    match v {
        Value::Array(a) => a.iter_mut().for_each(|x| apply_counts(x, counts)),
        Value::Object(m) => {
            let is_post = m.contains_key("author") && m.contains_key("cid");
            let uri = m.get("uri").and_then(Value::as_str).map(str::to_string);
            let did = m.get("did").and_then(Value::as_str).map(str::to_string);
            if is_post && let Some(c) = uri.as_deref().and_then(|u| counts.get(u)) {
                for (field, pick) in POST_COUNT_FIELDS {
                    if let Some(n) = pick(c) {
                        m.insert(field.into(), json!(n));
                    }
                }
            }
            if m.contains_key("followersCount")
                && let Some(n) = did.as_deref().and_then(|d| counts.get(d)).and_then(|c| c.followers)
            {
                m.insert("followersCount".into(), json!(n));
            }
            m.values_mut().for_each(|x| apply_counts(x, counts));
        }
        _ => {}
    }
}

impl super::Hydrator {
    /// §7.1/§9.6 counts on upstream views: a fresh or stale
    /// `constellation_counts` row overrides the upstream count (stale rows
    /// are also queued for refresh); with no usable row the upstream count
    /// stays (never a local zero) and the target is queued on `cc:refresh`.
    /// A count Constellation does not carry is never touched. A
    /// viewer-private request reads the rows and queues nothing (design-v4
    /// §8.1).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(crate) async fn enrich_counts(&self, req: &Requester, view: &mut Value) -> Result<(), sqlx::Error> {
        let mut targets = Vec::new();
        count_targets(view, &mut targets);
        targets.sort();
        targets.dedup();
        let mut found: HashMap<String, Counts> = HashMap::new();
        for t in targets {
            let source = match counts::cached(self.pool(), &t, &self.inner.cfg.counts).await? {
                Some((c, Freshness::Fresh)) => {
                    found.insert(t, c);
                    "fresh"
                }
                Some((c, _)) => {
                    self.enqueue_refresh(req, &t).await;
                    found.insert(t, c);
                    "stale"
                }
                None => {
                    self.enqueue_refresh(req, &t).await;
                    "upstream"
                }
            };
            metrics::counter!(METRIC_UPSTREAM_COUNTS, "source" => source).increment(1);
        }
        apply_counts(view, &found);
        Ok(())
    }
}

/// Slingshot calls in flight per request.
pub const SLINGSHOT_CONCURRENCY: usize = 8;
/// PDS listing: per-call timeout, page size, and the ascending-PDS page cap.
pub const PDS_TIMEOUT: Duration = Duration::from_secs(3);
pub const PDS_PAGE: u32 = 100;
pub const ASCENDING_MAX_PAGES: usize = 10;
/// Listing-order detection cache per PDS host (§5.10).
pub const ORDER_TTL: Duration = Duration::from_secs(24 * 3600);

const NSID_PROFILE: &str = "app.bsky.actor.profile";
const NSID_POST: &str = "app.bsky.feed.post";
const NSID_REPOST: &str = "app.bsky.feed.repost";
const NSID_FOLLOW: &str = "app.bsky.graph.follow";
const NSID_LIST: &str = "app.bsky.graph.list";
const NSID_LISTITEM: &str = "app.bsky.graph.listitem";
const NSID_STARTERPACK: &str = "app.bsky.graph.starterpack";
const NSID_POLL: &str = "app.nearhorizon.poll.post";
/// Owner-PDS pages one `getList` request reads when no AppView answers:
/// the default of `lists.read_listing_pages` (design-v3 §5.5).
pub const LIST_READ_PAGES: usize = 5;

/// Author-PDS listings on the read path: the host's public-fallback bucket
/// (design-v3 §II-6.11.3; never queued, and separate from the private
/// tier's read-fetch bucket), and the §5.10 order cache. A PDS is a host
/// the subject names, so each listing goes through the request's `net`
/// intent: `viewer_private` for a viewer-private fetch (design-v4 §8.1,
/// R3-213).
#[derive(Clone)]
pub struct PdsLister {
    nets: Transports,
    hosts: HostBuckets,
    orders: moka::future::Cache<String, ListOrder>,
}

impl PdsLister {
    #[must_use]
    pub fn new(nets: Transports, hosts: HostBuckets) -> Self {
        Self {
            nets,
            hosts,
            orders: moka::future::Cache::builder().max_capacity(100_000).time_to_live(ORDER_TTL).build(),
        }
    }

    /// One `listRecords` page charged to `req`, or `None` (no budget, a
    /// passed deadline, a transport or status failure).
    pub async fn list(
        &self,
        req: &Requester,
        pds: &str,
        did: &str,
        collection: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Option<ListPage> {
        if std::time::Instant::now() >= req.deadline() || !self.hosts.draw(req, &HostBudgets::host_of(pds)) {
            return None;
        }
        let limit = limit.to_string();
        let mut params = vec![("repo", did), ("collection", collection), ("limit", limit.as_str())];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let url = Url::parse_with_params(&format!("{}/xrpc/com.atproto.repo.listRecords", pds.trim_end_matches('/')), &params).ok()?;
        let options = RequestOptions::new(BodyCap::LIST_RECORDS).timeout(PDS_TIMEOUT);
        self.nets.for_request(req).client().get_json::<ListPage>(url.as_str(), &options).await.ok()?.ok()
    }

    /// The host's listing order, probed once with a `limit=2` listing and
    /// cached 24 h. `None` when undetermined. A viewer-private request
    /// neither reads nor writes the cache, whose keys are subject-named
    /// hosts (design-v4 §8.1).
    pub async fn order(&self, req: &Requester, pds: &str, did: &str) -> Option<ListOrder> {
        let host = HostBudgets::host_of(pds);
        let shared = !req.is_viewer_private();
        if shared && let Some(o) = self.orders.get(&host).await {
            return Some(o);
        }
        let page = self.list(req, pds, did, NSID_POST, 2, None).await?;
        let order = detect_order(&page.records)?;
        if shared {
            self.orders.insert(host, order).await;
        }
        Some(order)
    }
}

/// The production upstream. Cheap to clone.
#[derive(Clone)]
pub struct RealUpstreamSource {
    router: Router,
    cascade: Cascade,
    slingshot: SlingshotClient,
    pds: PdsLister,
    cache: ReadCache,
    /// `lists.read_listing_pages`.
    list_read_pages: usize,
    /// Whose public-indexing mode decides C4 (public-indexing design §11).
    /// `None` reads as On: the community network is skipped.
    scope: Option<ScopeState>,
}

/// A `postView` synthesized from a Slingshot or PDS record: no counts, no
/// hydrated embed (§7.1 record-level fallback).
pub(crate) fn post_from_record(uri: &str, cid: Option<&str>, handle: &str, value: &Value) -> Value {
    let author = uri_author(uri).unwrap_or_default();
    let indexed = value.get("createdAt").cloned().unwrap_or(Value::Null);
    json!({
        "$type": "app.bsky.feed.defs#postView",
        "uri": uri,
        "cid": cid.unwrap_or_default(),
        "author": {"did": author, "handle": handle, "viewer": {}, "labels": []},
        "record": normalize_record(value),
        "indexedAt": indexed,
        "viewer": {},
        "labels": [],
    })
}

/// A `profileViewDetailed` synthesized from the profile record.
fn profile_from_record(did: &str, handle: &str, pds: Option<&str>, value: &Value) -> Value {
    let mut v = json!({"did": did, "handle": handle, "viewer": {}, "labels": []});
    for (from, to) in [("displayName", "displayName"), ("description", "description"), ("createdAt", "createdAt")] {
        if let Some(x) = value.get(from).filter(|x| x.is_string()) {
            v[to] = x.clone();
        }
    }
    for key in ["avatar", "banner"] {
        if let Some(url) = value.get(key).and_then(blob_cid).and_then(|cid| blob_url(pds, did, &cid)) {
            v[key] = json!(url);
        }
    }
    v
}

/// A `listView` synthesized from a list record: no item count, and the
/// creator in its minimal form.
pub(crate) fn list_from_record(uri: &str, cid: &str, creator_handle: &str, pds: Option<&str>, value: &Value) -> Value {
    let creator = uri_author(uri).unwrap_or_default();
    let mut v = json!({
        "uri": uri,
        "cid": cid,
        "creator": {"did": creator, "handle": creator_handle, "viewer": {}, "labels": []},
        "name": value.get("name").cloned().unwrap_or_else(|| json!("")),
        "purpose": value.get("purpose").cloned().unwrap_or(Value::Null),
        "indexedAt": value.get("createdAt").cloned().unwrap_or(Value::Null),
        "viewer": {},
        "labels": [],
    });
    for key in ["description", "descriptionFacets"] {
        if let Some(x) = value.get(key).filter(|x| !x.is_null()) {
            v[key] = x.clone();
        }
    }
    if let Some(url) = value.get("avatar").and_then(blob_cid).and_then(|cid| blob_url(pds, creator, &cid)) {
        v["avatar"] = json!(url);
    }
    v
}

impl RealUpstreamSource {
    #[must_use]
    pub fn new(router: Router, cascade: Cascade, slingshot: SlingshotClient, pds: PdsLister, cache: ReadCache) -> Self {
        Self {
            router,
            cascade,
            slingshot,
            pds,
            cache,
            list_read_pages: LIST_READ_PAGES,
            scope: None,
        }
    }

    /// Read the public-indexing mode from `scope` (C4): unless it is On, a
    /// community DID's public reads go to the cascade too.
    #[must_use]
    pub fn with_scope(mut self, scope: ScopeState) -> Self {
        self.scope = Some(scope);
        self
    }

    /// Set `lists.read_listing_pages`: the owner-PDS pages one `getList`
    /// request reads when no AppView answers.
    #[must_use]
    pub fn with_list_read_pages(mut self, pages: u32) -> Self {
        self.list_read_pages = usize::try_from(pages).unwrap_or(LIST_READ_PAGES);
        self
    }

    /// The DID's home network, and whether it is this AppView.
    async fn home(&self, req: &Requester, did: &str) -> (Option<String>, bool) {
        let d = self.router.resolve_did(req, did).await;
        let community = d.is_community();
        (d.network_name, community)
    }

    /// Cascade call for a subject DID, skipped for the community network
    /// while the public-indexing mode is On. Unless it is On, a community
    /// author's public records are not stored here, so the networks in
    /// cascade order are asked (C4, public-indexing design §11).
    async fn cascade(&self, req: &Requester, did: &str, call: Call<'_>) -> Option<Value> {
        let (home, community) = self.home(req, did).await;
        let on = self.scope.as_ref().is_none_or(|s| s.public_mode().is_on());
        if community && on {
            return None;
        }
        self.cascade.fetch(req, home.as_deref(), call).await.hit()
    }

    async fn handle_of(&self, req: &Requester, did: &str) -> String {
        self.router
            .lookup()
            .summary(req, did)
            .await
            .and_then(|s| s.handle)
            .unwrap_or_else(|| INVALID_HANDLE.to_string())
    }

    /// One post through Slingshot, cached.
    async fn slingshot_post(&self, req: &Requester, uri: &str) -> Option<Value> {
        let key = format!("rt:post:{uri}");
        if let Some(v) = self.cache.get_json::<Value>(req, &key).await {
            return Some(v);
        }
        let record = self.slingshot.get_record(req, uri).await.ok()?;
        let handle = self.handle_of(req, uri_author(uri)?).await;
        let view = post_from_record(uri, record.cid.as_deref(), &handle, &record.value);
        self.cache.put_json(req, &key, &view).await;
        Some(view)
    }

    /// Author-PDS feed items after the `P:` cursor (§7.1): posts and reposts
    /// merged by rkey (TID order), filtered, at most `limit`.
    async fn pds_feed(&self, req: &Requester, did: &str, limit: u32, cursor: Option<&str>, filter: &str) -> Option<Value> {
        let pds = self.router.lookup().summary(req, did).await?.pds?;
        let order = self.pds.order(req, &pds, did).await?;
        let (post_cursor, repost_cursor) = match cursor.and_then(|c| c.strip_prefix("P:")) {
            Some(c) => {
                let (p, r) = c.split_once(':').unwrap_or((c, ""));
                (Some(p.to_string()).filter(|s| !s.is_empty()), Some(r.to_string()).filter(|s| !s.is_empty()))
            }
            None => (None, None),
        };
        let limit_usize = usize::try_from(limit).unwrap_or(50);
        let (posts, reposts, paged) = match order {
            ListOrder::Descending => {
                let posts = self.pds.list(req, &pds, did, NSID_POST, limit, post_cursor.as_deref()).await?.records;
                let reposts = self.pds.list(req, &pds, did, NSID_REPOST, limit, repost_cursor.as_deref()).await?.records;
                (posts, reposts, true)
            }
            // An ascending PDS: only the first page, the newest `limit` of a
            // bounded listing, and no cursor (§7.1).
            ListOrder::Ascending if cursor.is_none() => {
                let posts = self.newest_ascending(req, &pds, did, NSID_POST, limit_usize).await;
                let reposts = self.newest_ascending(req, &pds, did, NSID_REPOST, limit_usize).await;
                (posts, reposts, false)
            }
            ListOrder::Ascending => return Some(json!({"feed": []})),
        };
        let mut merged: Vec<(bool, ListedRecord)> =
            posts.into_iter().map(|r| (false, r)).chain(reposts.into_iter().map(|r| (true, r))).collect();
        merged.sort_by_key(|(_, r)| Reverse(r.rkey().to_string()));
        let handle = self.handle_of(req, did).await;
        let mut feed = Vec::new();
        let (mut last_post, mut last_repost) = (post_cursor, repost_cursor);
        // Records that entered the window, kept by the filter or not.
        let mut consumed = 0usize;
        for (is_repost, r) in merged.into_iter().take(limit_usize) {
            if is_repost {
                let rkey = r.rkey().to_string();
                let subject = r
                    .value
                    .pointer("/subject/uri")
                    .and_then(Value::as_str)
                    .filter(|s| !is_private_uri(s) && !matches!(filter, "posts_with_media" | "posts_with_video"));
                let Some(subject) = subject else {
                    last_repost = Some(rkey);
                    consumed += 1;
                    continue;
                };
                match self.posts(req, &[subject.to_string()]).await.into_iter().next() {
                    Some(post) => feed.push(json!({
                        "post": post,
                        "reason": {"$type": "app.bsky.feed.defs#reasonRepost", "by": {"did": did, "handle": handle, "viewer": {}, "labels": []},
                                   "indexedAt": r.value.get("createdAt").cloned().unwrap_or(Value::Null)},
                    })),
                    // No budget to fetch the reposted post: the page ends
                    // before this item and the cursor stays behind it, so
                    // nothing is skipped (design-v3 §II-8.3).
                    None if req.spent() => break,
                    None => {}
                }
                last_repost = Some(rkey);
                consumed += 1;
            } else {
                last_post = Some(r.rkey().to_string());
                consumed += 1;
                let reply_root = r.value.pointer("/reply/root/uri").and_then(Value::as_str);
                let keep = match filter {
                    "posts_no_replies" => reply_root.is_none(),
                    // §8.4: gallery counts as media; video is its own filter.
                    "posts_with_media" => matches!(media_type(&r.value), Some("images" | "gallery")),
                    "posts_with_video" => media_type(&r.value) == Some("video"),
                    "posts_and_author_threads" => reply_root.is_none_or(|u| uri_author(u) == Some(did)),
                    _ => true,
                };
                if keep {
                    feed.push(json!({"post": post_from_record(&r.uri, Some(&r.cid), &handle, &r.value)}));
                }
            }
        }
        // A listing that ran out of budget before it produced anything is no
        // answer (a 429), never an empty page.
        if feed.is_empty() && req.spent() {
            return None;
        }
        let mut out = json!({ "feed": feed });
        // A cursor whenever the window consumed a record: a page the filter
        // emptied does not end the feed (§II-7.1, R3-110).
        if paged && consumed > 0 {
            out["cursor"] = json!(format!("P:{}:{}", last_post.unwrap_or_default(), last_repost.unwrap_or_default()));
        }
        Some(out)
    }

    /// The newest `n` records of an ascending listing, paging at most
    /// [`ASCENDING_MAX_PAGES`].
    async fn newest_ascending(&self, req: &Requester, pds: &str, did: &str, collection: &str, n: usize) -> Vec<ListedRecord> {
        let mut all: Vec<ListedRecord> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..ASCENDING_MAX_PAGES {
            let Some(page) = self.pds.list(req, pds, did, collection, PDS_PAGE, cursor.as_deref()).await else {
                break;
            };
            let done = page.cursor.is_none() || page.records.is_empty();
            all.extend(page.records);
            if done {
                break;
            }
            cursor = page.cursor;
        }
        all.sort_by_key(|r| Reverse(r.rkey().to_string()));
        all.truncate(n);
        all
    }
}

#[async_trait]
impl UpstreamSource for RealUpstreamSource {
    async fn fetch_profiles(&self, req: &Requester, dids: &[String], until: std::time::Instant) -> ProfilesFetched {
        // `plan(None)` never holds the `community` network, which is not
        // callable; that skips a network, not a DID (C4 is untouched).
        let plan = self.cascade.table().plan(None);
        self.cascade.fetch_profiles(ProfileWalk::Request { req, until }, &plan, dids).await
    }

    async fn profile(&self, req: &Requester, actor: &str) -> Option<Value> {
        let key = format!("rt:profile:{actor}");
        if let Some(v) = self.cache.get_json::<Value>(req, &key).await {
            return Some(v);
        }
        let view = match self.cascade(req, actor, Call::Profile(actor)).await {
            Some(v) => v,
            // An empty share or pool stops the plan (design-v3 §II-8.3).
            None if req.spent() => return None,
            None => {
                // Slingshot profile record (§7.1).
                let record = self.slingshot.get_record(req, &format!("at://{actor}/{NSID_PROFILE}/self")).await.ok()?;
                let summary = self.router.lookup().summary(req, actor).await.unwrap_or_default();
                let handle = summary.handle.unwrap_or_else(|| INVALID_HANDLE.to_string());
                profile_from_record(actor, &handle, summary.pds.as_deref(), &record.value)
            }
        };
        self.cache.put_json(req, &key, &view).await;
        Some(view)
    }

    async fn posts(&self, req: &Requester, uris: &[String]) -> Vec<Value> {
        let mut found: HashMap<String, Value> = HashMap::new();
        let mut by_author: HashMap<String, Vec<String>> = HashMap::new();
        for uri in uris.iter().filter(|u| !is_private_uri(u)) {
            if let Some(v) = self.cache.get_json::<Value>(req, &format!("rt:post:{uri}")).await {
                found.insert(uri.clone(), v);
            } else if let Some(a) = uri_author(uri) {
                by_author.entry(a.to_string()).or_default().push(uri.clone());
            }
        }
        // Every author's route at once, when lodestar is set
        // (lodestar-reads design §2.10).
        let authors: Vec<String> = by_author.keys().cloned().collect();
        self.router.prefetch(req, &authors).await;
        for (author, group) in &by_author {
            // Once a share or pool is empty, the remaining posts are left
            // out (design-v3 §II-8.3: list items are omitted).
            if req.spent() {
                break;
            }
            if let Some(body) = self.cascade(req, author, Call::Posts(group)).await {
                for post in body.get("posts").and_then(Value::as_array).into_iter().flatten() {
                    if let Some(uri) = post.get("uri").and_then(Value::as_str).filter(|u| group.iter().any(|g| g == u)) {
                        self.cache.put_json(req, &format!("rt:post:{uri}"), post).await;
                        found.insert(uri.to_string(), post.clone());
                    }
                }
            }
        }
        let missing: Vec<String> = by_author.into_values().flatten().filter(|u| !found.contains_key(u)).collect();
        let fetched: Vec<(String, Option<Value>)> = futures::stream::iter(missing)
            .map(|uri| async move {
                let v = if req.spent() { None } else { self.slingshot_post(req, &uri).await };
                (uri, v)
            })
            .buffer_unordered(SLINGSHOT_CONCURRENCY)
            .collect()
            .await;
        for (uri, v) in fetched {
            if let Some(v) = v {
                found.insert(uri, v);
            }
        }
        uris.iter().filter_map(|u| found.remove(u)).collect()
    }

    async fn post_records(&self, req: &Requester, uris: &[String]) -> Vec<Value> {
        let public: Vec<String> = uris.iter().filter(|u| !is_private_uri(u)).cloned().collect();
        futures::stream::iter(public)
            .map(|uri| async move {
                // Once a share or pool is empty, the rest are left out.
                if req.spent() { None } else { self.slingshot_post(req, &uri).await }
            })
            .buffered(SLINGSHOT_CONCURRENCY)
            .filter_map(|v| async move { v })
            .collect()
            .await
    }

    async fn post_thread(&self, req: &Requester, uri: &str, depth: u32, parent_height: u32) -> Option<Value> {
        if is_private_uri(uri) {
            return None;
        }
        let author = uri_author(uri)?;
        if let Some(v) = self.cascade(req, author, Call::PostThread { uri, depth, parent_height }).await {
            return Some(v);
        }
        if req.spent() {
            return None;
        }
        // Slingshot: the anchor and its parent chain (§7.1).
        let anchor = self.slingshot_post(req, uri).await?;
        let mut chain: Vec<Value> = Vec::new();
        let mut next = anchor.pointer("/record/reply/parent/uri").and_then(Value::as_str).map(str::to_string);
        while let Some(p) = next.take() {
            if chain.len() >= usize::try_from(parent_height).unwrap_or(0) {
                break;
            }
            if is_private_uri(&p) {
                chain.push(json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": p, "notFound": true}));
                break;
            }
            match self.slingshot_post(req, &p).await {
                Some(post) => {
                    next = post.pointer("/record/reply/parent/uri").and_then(Value::as_str).map(str::to_string);
                    chain.push(json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": post}));
                }
                // The chain stops at a parent it had no budget to fetch,
                // without calling it not-found (design-v3 §II-8.3).
                None if req.spent() => break,
                None => {
                    chain.push(json!({"$type": "app.bsky.feed.defs#notFoundPost", "uri": p, "notFound": true}));
                    break;
                }
            }
        }
        let mut parent: Option<Value> = None;
        for mut node in chain.into_iter().rev() {
            if let Some(p) = parent.take() {
                node["parent"] = p;
            }
            parent = Some(node);
        }
        let mut thread = json!({"$type": "app.bsky.feed.defs#threadViewPost", "post": anchor, "replies": []});
        if let Some(p) = parent {
            thread["parent"] = p;
        }
        Some(json!({ "thread": thread }))
    }

    async fn author_feed(&self, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>, filter: &str) -> Option<Value> {
        // A `P:` cursor continues from the author PDS; upstream cursors are
        // opaque and go back to the cascade.
        if !cursor.is_some_and(|c| c.starts_with("P:"))
            && let Some(v) = self.cascade(req, actor, Call::AuthorFeed { actor, limit, cursor, filter }).await
        {
            return Some(v);
        }
        if req.spent() {
            return None;
        }
        self.pds_feed(req, actor, limit, cursor, filter).await
    }

    async fn follows(&self, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> Option<Value> {
        if !cursor.is_some_and(|c| c.starts_with("P:"))
            && let Some(v) = self.cascade(req, actor, Call::Follows { actor, limit, cursor }).await
        {
            return Some(v);
        }
        if req.spent() {
            return None;
        }
        // The author PDS `listRecords app.bsky.graph.follow` (§7.1).
        let pds = self.router.lookup().summary(req, actor).await?.pds?;
        let page = self
            .pds
            .list(req, &pds, actor, NSID_FOLLOW, limit, cursor.and_then(|c| c.strip_prefix("P:")))
            .await?;
        let subjects: Vec<String> = page
            .records
            .iter()
            .filter_map(|r| r.value.get("subject").and_then(Value::as_str).map(str::to_string))
            .collect();
        let follows: Vec<Value> = futures::stream::iter(subjects)
            .map(|did| async move {
                let handle = self.handle_of(req, &did).await;
                json!({"did": did, "handle": handle, "viewer": {}, "labels": []})
            })
            .buffered(SLINGSHOT_CONCURRENCY)
            .collect()
            .await;
        let handle = self.handle_of(req, actor).await;
        let mut out = json!({"subject": {"did": actor, "handle": handle, "viewer": {}, "labels": []}, "follows": follows});
        if let Some(c) = page.cursor {
            out["cursor"] = json!(format!("P:{c}"));
        }
        Some(out)
    }

    async fn lists_descending(&self, req: &Requester, actor: &str) -> bool {
        let Some(pds) = self.router.lookup().summary(req, actor).await.and_then(|s| s.pds) else {
            return false;
        };
        self.pds.order(req, &pds, actor).await == Some(ListOrder::Descending)
    }

    async fn record(&self, req: &Requester, uri: &str) -> Option<Value> {
        let key = format!("rt:record:{uri}");
        if let Some(v) = self.cache.get_json::<Value>(req, &key).await {
            return Some(v);
        }
        let record = self.slingshot.get_record(req, uri).await.ok()?;
        let view = json!({"uri": uri, "cid": record.cid, "value": record.value});
        self.cache.put_json(req, &key, &view).await;
        Some(view)
    }

    async fn list_items(&self, req: &Requester, list: &str, limit: u32, cursor: Option<&str>) -> Option<Value> {
        let owner = uri_author(list)?;
        if !cursor.is_some_and(|c| c.starts_with("P:"))
            && let Some(body) = self.cascade(req, owner, Call::List { list, limit, cursor }).await
        {
            let mut out = json!({"items": body.get("items").cloned().unwrap_or_else(|| json!([]))});
            for (to, from) in [("cursor", "/cursor"), ("listItemCount", "/list/listItemCount")] {
                if let Some(v) = body.pointer(from).filter(|v| !v.is_null()) {
                    out[to] = v.clone();
                }
            }
            return Some(out);
        }
        if req.spent() {
            return None;
        }
        // The owner's PDS, filtered to this list: at most `list_read_pages`
        // pages, each asking only for what the page still lacks. The order
        // is the PDS's own.
        let pds = self.router.lookup().summary(req, owner).await?.pds?;
        let mut next = cursor.and_then(|c| c.strip_prefix("P:")).map(str::to_string);
        let mut subjects: Vec<(String, String)> = Vec::new();
        let mut answered = false;
        for _ in 0..self.list_read_pages {
            let want = limit.saturating_sub(u32::try_from(subjects.len()).unwrap_or(u32::MAX));
            if want == 0 {
                break;
            }
            let Some(page) = self.pds.list(req, &pds, owner, NSID_LISTITEM, want, next.as_deref()).await else {
                break;
            };
            answered = true;
            subjects.extend(page.records.iter().filter_map(|r| {
                let in_list = r.value.get("list").and_then(Value::as_str) == Some(list);
                let subject = r.value.get("subject").and_then(Value::as_str)?;
                in_list.then(|| (r.uri.clone(), subject.to_string()))
            }));
            next = page.cursor;
            if next.is_none() {
                break;
            }
        }
        if !answered {
            return None;
        }
        let items: Vec<Value> = futures::stream::iter(subjects)
            .map(|(uri, did)| async move {
                let handle = self.handle_of(req, &did).await;
                json!({"uri": uri, "subject": {"did": did, "handle": handle, "viewer": {}, "labels": []}})
            })
            .buffered(SLINGSHOT_CONCURRENCY)
            .collect()
            .await;
        let mut out = json!({"items": items});
        // A cursor whenever the PDS has more, so a short page never claims
        // to be the end of the list.
        if let Some(c) = next {
            out["cursor"] = json!(format!("P:{c}"));
        }
        Some(out)
    }

    async fn lists(&self, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>, purposes: &[String]) -> Option<Value> {
        if !cursor.is_some_and(|c| c.starts_with("P:"))
            && let Some(v) = self.cascade(req, actor, Call::Lists { actor, limit, cursor, purposes }).await
        {
            return Some(v);
        }
        if req.spent() {
            return None;
        }
        // The owner's PDS `listRecords app.bsky.graph.list`: one page, whose
        // cursor is the PDS's (§5.0's single-stream rule).
        let pds = self.router.lookup().summary(req, actor).await?.pds?;
        let page = self
            .pds
            .list(req, &pds, actor, NSID_LIST, limit, cursor.and_then(|c| c.strip_prefix("P:")))
            .await?;
        let handle = self.handle_of(req, actor).await;
        let lists: Vec<Value> = page
            .records
            .iter()
            .filter(|r| {
                let purpose = r.value.get("purpose").and_then(Value::as_str).unwrap_or_default();
                purposes.is_empty() || purposes.iter().any(|p| p == purpose)
            })
            .map(|r| list_from_record(&r.uri, &r.cid, &handle, Some(&pds), &r.value))
            .collect();
        let mut out = json!({"lists": lists});
        if let Some(c) = page.cursor {
            out["cursor"] = json!(format!("P:{c}"));
        }
        Some(out)
    }

    async fn list_feed(&self, req: &Requester, list: &str, limit: u32, cursor: Option<&str>) -> Option<Value> {
        self.cascade(req, uri_author(list)?, Call::ListFeed { list, limit, cursor }).await
    }

    async fn relationships(&self, req: &Requester, actor: &str, others: &[String]) -> Option<Value> {
        self.cascade(req, actor, Call::Relationships { actor, others }).await
    }

    async fn starter_pack(&self, req: &Requester, uri: &str) -> Option<Value> {
        self.cascade(req, uri_author(uri)?, Call::StarterPack(uri)).await
    }

    async fn actor_starter_packs(&self, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> Option<Value> {
        if !cursor.is_some_and(|c| c.starts_with("P:"))
            && let Some(v) = self.cascade(req, actor, Call::ActorStarterPacks { actor, limit, cursor }).await
        {
            return Some(v);
        }
        if req.spent() {
            return None;
        }
        // The owner's PDS `listRecords app.bsky.graph.starterpack`.
        let pds = self.router.lookup().summary(req, actor).await?.pds?;
        let page = self
            .pds
            .list(req, &pds, actor, NSID_STARTERPACK, limit, cursor.and_then(|c| c.strip_prefix("P:")))
            .await?;
        let handle = self.handle_of(req, actor).await;
        let packs: Vec<Value> = page
            .records
            .iter()
            .map(|r| {
                json!({
                    "uri": r.uri,
                    "cid": r.cid,
                    "record": r.value,
                    "creator": {"did": actor, "handle": handle, "viewer": {}, "labels": []},
                    "labels": [],
                    "indexedAt": r.value.get("createdAt").cloned().unwrap_or(Value::Null),
                })
            })
            .collect();
        let mut out = json!({"starterPacks": packs});
        if let Some(c) = page.cursor {
            out["cursor"] = json!(format!("P:{c}"));
        }
        Some(out)
    }

    async fn author_polls(&self, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> Option<Value> {
        // No AppView serves this NSID: the author's PDS
        // `listRecords app.nearhorizon.poll.post`, one page, whose cursor is
        // the PDS's. A page of only non-public polls is empty and still
        // carries its cursor, so the listing goes on.
        let pds = self.router.lookup().summary(req, actor).await?.pds?;
        let page = self
            .pds
            .list(req, &pds, actor, NSID_POLL, limit, cursor.and_then(|c| c.strip_prefix("P:")))
            .await?;
        let handle = self.handle_of(req, actor).await;
        let polls: Vec<Value> = page
            .records
            .iter()
            .filter(|r| r.value.get("visibility").and_then(Value::as_str) == Some("public"))
            .map(|r| {
                json!({
                    "uri": r.uri,
                    "cid": r.cid,
                    "author": {"did": actor, "handle": handle, "viewer": {}, "labels": []},
                    "record": r.value,
                    "indexedAt": r.value.get("createdAt").cloned().unwrap_or(Value::Null),
                })
            })
            .collect();
        let mut out = json!({ "polls": polls });
        if let Some(c) = page.cursor {
            out["cursor"] = json!(format!("P:{c}"));
        }
        Some(out)
    }

    async fn resolve_handle(&self, req: &Requester, handle: &str) -> Option<String> {
        self.router.lookup().did_for_handle(req, handle).await
    }

    async fn handle_outcome(&self, req: &Requester, handle: &str) -> HandleOutcome {
        self.router.lookup().handle_outcome(req, handle).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router as AxumRouter;
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;

    use super::*;
    use crate::constellation::cascade::tests::{cascade_for, fake_appview};
    use crate::constellation::routing::tests::{StubLookup, table_with_community};
    use crate::constellation::{DidLookup, Router as DidRouter};
    use crate::net::NetClient;
    use crate::test_support::serve_fake;
    use crate::xrpc::pools::Shares;

    const OUTSIDER: &str = "did:plc:oooooooooooooooooooooooo";

    /// B-54 "Every `net` call site names its intent", in-crate (design-v4
    /// §8.1, §11 G-9; R3-213): a PDS listing goes through the transport of
    /// the request's mode, so a viewer-private listing reaches the subject's
    /// PDS only through the `viewer_private` client, and private-context
    /// hydration only through the internal one.
    #[tokio::test]
    async fn pds_listings_go_through_the_request_modes_transport() {
        use axum::http::HeaderMap;

        use crate::net::Intent;
        use crate::net::client::test_user_agent;

        let agents: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let router = AxumRouter::new()
            .route(
                "/xrpc/com.atproto.repo.listRecords",
                get(|State(agents): State<Arc<std::sync::Mutex<Vec<String>>>>, headers: HeaderMap| async move {
                    let agent = headers.get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
                    agents.lock().expect("agents").push(agent);
                    axum::Json(json!({"records": []}))
                }),
            )
            .with_state(Arc::clone(&agents));
        let pds = serve_fake(router).await;
        let lister = PdsLister::new(Transports::loopback_for_tests(), hosts(200.0));
        let public = Requester::test();
        let private = public.viewer_private("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");
        let context = public.private_context("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");
        for req in [&public, &private, &context] {
            assert!(lister.list(req, &pds, OUTSIDER, NSID_POST, 2, None).await.is_some());
        }
        let expected: Vec<String> = [Intent::Public, Intent::ViewerPrivate, Intent::Internal].into_iter().map(test_user_agent).collect();
        assert_eq!(*agents.lock().expect("agents"), expected);
        // The order probe of a viewer-private request uses its transport too.
        lister.order(&private, &pds, OUTSIDER).await;
        assert_eq!(agents.lock().expect("agents").last(), Some(&test_user_agent(Intent::ViewerPrivate)));
    }

    /// Host buckets of `per_sec`: a non-member address gets half of it.
    fn hosts(per_sec: f64) -> HostBuckets {
        HostBuckets::new(&Shares::for_tests(), per_sec, 1000)
    }

    async fn fake_slingshot() -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = AxumRouter::new()
            .route(
                "/xrpc/com.atproto.repo.getRecord",
                get(|State(calls): State<Arc<AtomicUsize>>, Query(q): Query<HashMap<String, String>>| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let uri = format!("at://{}/{}/{}", q["repo"], q["collection"], q["rkey"]);
                    if q["rkey"] == "gone" {
                        return (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "RecordNotFound"}))).into_response();
                    }
                    let value = if q["collection"] == NSID_PROFILE {
                        json!({"$type": NSID_PROFILE, "displayName": "Outsider"})
                    } else {
                        json!({"$type": NSID_POST, "text": format!("from slingshot {}", q["rkey"]), "createdAt": "2026-09-26T00:00:00Z"})
                    };
                    axum::Json(json!({"uri": uri, "cid": "bafyslingshot", "value": value})).into_response()
                }),
            )
            .with_state(Arc::clone(&calls));
        (serve_fake(router).await, calls)
    }

    fn source(cascade: Cascade, slingshot: &str) -> RealUpstreamSource {
        let lookup = Arc::new(StubLookup::default());
        lookup.pds.lock().expect("lock").insert(OUTSIDER.into(), "https://pds.unknown.example".into());
        let router = DidRouter::new(Arc::new(table_with_community(&[])), lookup as Arc<dyn DidLookup>);
        let net = NetClient::loopback_for_tests();
        RealUpstreamSource::new(
            router,
            cascade,
            SlingshotClient::new(net, slingshot),
            PdsLister::new(Transports::loopback_for_tests(), hosts(20.0)),
            ReadCache::new(1024 * 1024),
        )
    }

    /// A PDS listing `app.bsky.feed.post` in ascending rkey order, `n` posts
    /// (`3k000000`…), honouring `limit` and `cursor`; no reposts. Counts
    /// listing calls per collection.
    async fn ascending_pds(n: usize) -> (String, Arc<std::sync::Mutex<HashMap<String, usize>>>) {
        let calls: Arc<std::sync::Mutex<HashMap<String, usize>>> = Arc::default();
        let router = AxumRouter::new()
            .route(
                "/xrpc/com.atproto.repo.listRecords",
                get(
                    move |State(calls): State<Arc<std::sync::Mutex<HashMap<String, usize>>>>,
                          Query(q): Query<HashMap<String, String>>| async move {
                        *calls.lock().expect("lock").entry(q["collection"].clone()).or_default() += 1;
                        if q["collection"] != NSID_POST {
                            return axum::Json(json!({"records": []})).into_response();
                        }
                        let limit: usize = q["limit"].parse().expect("limit");
                        let after = q.get("cursor").cloned().unwrap_or_default();
                        let records: Vec<Value> = (0..n)
                            .map(|i| format!("3k{i:06}"))
                            .filter(|rkey| *rkey > after)
                            .take(limit)
                            .map(|rkey| {
                                json!({"uri": format!("at://{OUTSIDER}/{NSID_POST}/{rkey}"), "cid": "c",
                                       "value": {"$type": NSID_POST, "text": rkey, "createdAt": "2026-09-26T00:00:00Z"}})
                            })
                            .collect();
                        let cursor = records.last().and_then(|r| r["uri"].as_str()).map(|u| u.rsplit('/').next().unwrap_or_default().to_string());
                        let more = cursor.as_ref().is_some_and(|c| (0..n).map(|i| format!("3k{i:06}")).any(|r| r > *c));
                        axum::Json(json!({"records": records, "cursor": if more { cursor } else { None }})).into_response()
                    },
                ),
            )
            .with_state(Arc::clone(&calls));
        (serve_fake(router).await, calls)
    }

    fn pds_source(pds: &str, cascade_base: &str) -> RealUpstreamSource {
        pds_source_with(pds, cascade_base, 200.0)
    }

    fn pds_source_with(pds: &str, cascade_base: &str, per_host: f64) -> RealUpstreamSource {
        let lookup = Arc::new(StubLookup::default());
        lookup.pds.lock().expect("lock").insert(OUTSIDER.into(), pds.to_string());
        let router = DidRouter::new(Arc::new(table_with_community(&[])), lookup as Arc<dyn DidLookup>);
        let net = NetClient::loopback_for_tests();
        RealUpstreamSource::new(
            router,
            cascade_for(&[("bluesky", cascade_base), ("community-a", cascade_base)]),
            SlingshotClient::new(net, cascade_base),
            PdsLister::new(Transports::loopback_for_tests(), hosts(per_host)),
            ReadCache::new(1024 * 1024),
        )
    }

    #[tokio::test]
    async fn a_pds_feed_with_no_budget_is_no_answer_not_an_empty_page() {
        let (missing, _) = fake_appview(StatusCode::BAD_REQUEST, json!({"error": "NotFound"})).await;
        let (pds, calls) = ascending_pds(250).await;
        // 2/s per host: one token for a non-member, which the order probe
        // takes. The listing itself then has none.
        let up = pds_source_with(&pds, &missing, 2.0);
        let req = Requester::test();
        assert_eq!(up.author_feed(&req, OUTSIDER, 5, None, "posts_with_replies").await, None);
        assert!(req.spent());
        assert_eq!(calls.lock().expect("lock").get(NSID_POST).copied(), Some(1));
        // Once spent, the request tries no fallback at all.
        assert_eq!(up.follows(&req, OUTSIDER, 5, None).await, None);
        assert_eq!(calls.lock().expect("lock").len(), 1);
    }

    #[tokio::test]
    async fn ascending_pds_feed_is_the_newest_limit_of_at_most_ten_pages_without_a_cursor() {
        let (missing, _) = fake_appview(StatusCode::BAD_REQUEST, json!({"error": "NotFound"})).await;
        let req = Requester::test();

        // 250 posts over 3 pages: the newest `limit` of all of them.
        let (pds, calls) = ascending_pds(250).await;
        let up = pds_source(&pds, &missing);
        let feed = up.author_feed(&req, OUTSIDER, 5, None, "posts_with_replies").await.expect("feed");
        let texts: Vec<&str> = feed["feed"].as_array().expect("feed").iter().filter_map(|i| i["post"]["record"]["text"].as_str()).collect();
        assert_eq!(texts, ["3k000249", "3k000248", "3k000247", "3k000246", "3k000245"]);
        assert!(feed.get("cursor").is_none(), "an ascending PDS gives no cursor");
        // The order probe plus three pages of posts.
        assert_eq!(calls.lock().expect("lock").get(NSID_POST).copied(), Some(4));
        // A `P:` cursor on an ascending PDS ends the feed.
        let next = up.author_feed(&req, OUTSIDER, 5, Some("P:3k000245:"), "posts_with_replies").await.expect("feed");
        assert_eq!(next, json!({"feed": []}));

        // 1500 posts (15 pages): at most 10 pages are read, so the page is the
        // newest of the first 1000 records, still with no cursor.
        let (pds, calls) = ascending_pds(1500).await;
        let up = pds_source(&pds, &missing);
        let feed = up.author_feed(&req, OUTSIDER, 3, None, "posts_with_replies").await.expect("feed");
        let texts: Vec<&str> = feed["feed"].as_array().expect("feed").iter().filter_map(|i| i["post"]["record"]["text"].as_str()).collect();
        assert_eq!(texts, ["3k000999", "3k000998", "3k000997"]);
        assert!(feed.get("cursor").is_none());
        assert_eq!(calls.lock().expect("lock").get(NSID_POST).copied(), Some(1 + ASCENDING_MAX_PAGES));
    }

    #[tokio::test]
    async fn upstream_counts_prefer_constellation() {
        let Some(db) = crate::test_support::fresh_db("upstream_counts").await else {
            return;
        };
        let p = &db.pool;
        let h = crate::hydration::testing::hydrator(p, crate::aggregations::ScopeSets::default());
        let fresh = format!("at://{OUTSIDER}/{NSID_POST}/fresh");
        let stale = format!("at://{OUTSIDER}/{NSID_POST}/stale");
        let unknown = format!("at://{OUTSIDER}/{NSID_POST}/unknown");
        let only_likes = Counts {
            likes: Some(200),
            ..Counts::default()
        };
        counts::store(p, &fresh, &only_likes).await.expect("store");
        counts::store(p, &stale, &Counts { likes: Some(7), ..Counts::default() }).await.expect("store");
        sqlx::query("UPDATE constellation_counts SET fetched_at = now() - interval '1 hour' WHERE target = $1")
            .bind(&stale)
            .execute(p)
            .await
            .expect("age");
        counts::store(p, OUTSIDER, &Counts { followers: Some(1234), ..Counts::default() }).await.expect("store");
        let post = |uri: &str| {
            json!({"uri": uri, "cid": "c", "author": {"did": OUTSIDER, "handle": "o.test"}, "record": {"text": "t"},
                   "likeCount": 50, "repostCount": 5, "replyCount": 3, "quoteCount": 1})
        };
        let view = json!({
            "posts": [post(&fresh), post(&stale), post(&unknown)],
            "profile": {"did": OUTSIDER, "handle": "o.test", "followersCount": 10, "followsCount": 4}
        });
        let (v, _) = h.normalized(&crate::hydration::Viewer::anonymous(), &Requester::test(), view).await.expect("normalize");
        assert_eq!(v["posts"][0]["likeCount"], 200, "a fresh Constellation value overrides upstream");
        assert_eq!(v["posts"][0]["repostCount"], 5, "a count Constellation does not carry is untouched");
        assert_eq!(v["posts"][1]["likeCount"], 7, "a stale value is still used");
        assert_eq!(v["posts"][2]["likeCount"], 50, "no row: upstream's count is kept, never a local zero");
        assert_eq!(v["profile"]["followersCount"], 1234);
        assert_eq!(v["profile"]["followsCount"], 4);
        db.drop_db().await;
    }

    #[tokio::test]
    async fn out_of_scope_profile_comes_from_the_cascade() {
        let (appview, calls) = fake_appview(StatusCode::OK, json!({"did": OUTSIDER, "handle": "outsider.example", "displayName": "From AppView"})).await;
        let (missing, _) = fake_appview(StatusCode::BAD_REQUEST, json!({"error": "NotFound"})).await;
        let (slingshot, slingshot_calls) = fake_slingshot().await;
        let up = source(cascade_for(&[("bluesky", &missing), ("community-a", &appview)]), &slingshot);
        let req = Requester::test();
        let v = up.profile(&req, OUTSIDER).await.expect("profile");
        assert_eq!(v["displayName"], "From AppView");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(slingshot_calls.load(Ordering::SeqCst), 0);
        // Cached for the next request.
        assert_eq!(up.profile(&req, OUTSIDER).await.expect("cached")["displayName"], "From AppView");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// C4 (public-indexing design §11): a community DID's public reads skip
    /// the cascade while the mode is On, and go to it otherwise.
    #[tokio::test]
    async fn community_reads_go_to_the_cascade_unless_the_mode_is_on() {
        use crate::aggregations::ScopeSets;
        use crate::config::ScopePolicy;
        use crate::indexing::{PublicMode, PublicState};

        const LOCAL: &str = "did:plc:cccccccccccccccccccccccc";
        let (appview, calls) = fake_appview(StatusCode::OK, json!({"did": LOCAL, "handle": "local.example", "displayName": "From AppView"})).await;
        let (slingshot, _) = fake_slingshot().await;
        let community = |scope: Option<ScopeState>| {
            let lookup = Arc::new(StubLookup::default());
            lookup.pds.lock().expect("lock").insert(LOCAL.into(), "https://pds1.community.example".into());
            let router = DidRouter::new(Arc::new(table_with_community(&["*.community.example"])), lookup as Arc<dyn DidLookup>);
            let net = NetClient::loopback_for_tests();
            let up = RealUpstreamSource::new(
                router,
                cascade_for(&[("bluesky", &appview), ("community-a", &appview)]),
                SlingshotClient::new(net, &slingshot),
                PdsLister::new(Transports::loopback_for_tests(), hosts(20.0)),
                ReadCache::new(1024 * 1024),
            );
            match scope {
                Some(scope) => up.with_scope(scope),
                None => up,
            }
        };
        let scope_in = |mode: PublicMode| {
            let sets = ScopeSets { public: PublicState { mode, generation: 1, last_fill_since: None }, ..ScopeSets::default() };
            ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000)
        };
        let req = Requester::test();
        // On, or no scope given: the record from Slingshot, no AppView call.
        for up in [community(None), community(Some(scope_in(PublicMode::On)))] {
            assert_eq!(up.profile(&req, LOCAL).await.expect("profile")["displayName"], "Outsider");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // Any other mode: the cascade answers.
        for (i, mode) in [PublicMode::Filling, PublicMode::Draining, PublicMode::Off, PublicMode::Held].into_iter().enumerate() {
            let up = community(Some(scope_in(mode)));
            assert_eq!(up.profile(&req, LOCAL).await.expect("profile")["displayName"], "From AppView", "{mode:?}");
            assert_eq!(calls.load(Ordering::SeqCst), i + 1, "{mode:?}");
        }
    }

    /// B-53 "No shared-cache reads", B-54 (design-v4 §8.1; R3-188, R3-208),
    /// in-crate: a viewer-private fetch neither reads nor writes `rt` or
    /// `route`, and its repeats come from the viewer's own cache. An
    /// ordinary fetch of the same subjects still writes both.
    #[tokio::test]
    async fn a_viewer_private_fetch_leaves_rt_and_route_untouched() {
        let (notfound, _) = fake_appview(StatusCode::BAD_REQUEST, json!({"error": "NotFound"})).await;
        let (slingshot, slingshot_calls) = fake_slingshot().await;
        let up = source(cascade_for(&[("bluesky", &notfound), ("community-a", &notfound)]), &slingshot);
        let public = Requester::test();
        let private = public.viewer_private("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");
        let post = format!("at://{OUTSIDER}/{NSID_POST}/3kaaa");
        let list = format!("at://{OUTSIDER}/{NSID_LIST}/3klist");

        assert!(up.record(&private, &list).await.is_some());
        assert!(up.profile(&private, OUTSIDER).await.is_some());
        assert_eq!(up.posts(&private, std::slice::from_ref(&post)).await.len(), 1);
        assert!(up.cache.shared_keys().await.is_empty(), "rt was written");
        assert!(up.router.cached_routes().await.is_empty(), "route was written");
        // A repeat is served from the viewer's own cache.
        let calls = slingshot_calls.load(Ordering::SeqCst);
        assert!(up.record(&private, &list).await.is_some());
        assert_eq!(slingshot_calls.load(Ordering::SeqCst), calls);

        // An ordinary request does not see the viewer's entries, and fills
        // the shared caches.
        assert!(up.record(&public, &list).await.is_some());
        assert_eq!(slingshot_calls.load(Ordering::SeqCst), calls + 1);
        assert!(up.profile(&public, OUTSIDER).await.is_some());
        assert_eq!(up.cache.shared_keys().await, [format!("rt:profile:{OUTSIDER}"), format!("rt:record:{list}")]);
        assert_eq!(up.router.cached_routes().await, [OUTSIDER]);
        // The shared entries are invisible to the viewer-private request.
        let calls = slingshot_calls.load(Ordering::SeqCst);
        assert!(up.profile(&private.viewer_private("did:plc:wwwwwwwwwwwwwwwwwwwwwwww"), OUTSIDER).await.is_some());
        assert!(slingshot_calls.load(Ordering::SeqCst) > calls);
    }

    #[tokio::test]
    async fn out_of_scope_post_falls_back_to_slingshot_when_the_cascade_misses() {
        let (missing, cascade_calls) = fake_appview(StatusCode::OK, json!({"posts": []})).await;
        let (slingshot, slingshot_calls) = fake_slingshot().await;
        let up = source(cascade_for(&[("bluesky", &missing), ("community-a", &missing)]), &slingshot);
        let uris = vec![
            format!("at://{OUTSIDER}/{NSID_POST}/3kaaa"),
            format!("at://{OUTSIDER}/{NSID_POST}/gone"),
            format!("at://{OUTSIDER}/tools.kryphocron.feed.postPrivate/3kaaa"),
        ];
        let req = Requester::test();
        let posts = up.posts(&req, &uris).await;
        assert_eq!(posts.len(), 1, "gone is a miss; private URIs never go upstream");
        assert_eq!(posts[0]["uri"], uris[0]);
        assert_eq!(posts[0]["record"]["text"], "from slingshot 3kaaa");
        assert_eq!(posts[0]["cid"], "bafyslingshot");
        assert_eq!(posts[0]["author"]["did"], OUTSIDER);
        // Both cascade networks were asked (an empty `posts` is a hit with
        // nothing in it), then Slingshot per missing URI.
        assert!(cascade_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(slingshot_calls.load(Ordering::SeqCst), 2);

        // The profile falls back to the Slingshot profile record too.
        let (notfound, _) = fake_appview(StatusCode::BAD_REQUEST, json!({"error": "NotFound"})).await;
        let up = source(cascade_for(&[("bluesky", &notfound), ("community-a", &notfound)]), &slingshot);
        let profile = up.profile(&req, OUTSIDER).await.expect("profile");
        assert_eq!((profile["displayName"].as_str(), profile["handle"].as_str()), (Some("Outsider"), Some("oooo.test")));
        // A thread falls back to the anchor from Slingshot.
        let thread = up.post_thread(&req, &uris[0], 6, 20).await.expect("thread");
        assert_eq!(thread["thread"]["post"]["record"]["text"], "from slingshot 3kaaa");
    }
}
