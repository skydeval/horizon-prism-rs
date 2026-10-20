//! Actor suggestions (design-v3 §7.1, §7.3) and `getConfig` (§7.6).
//!
//! Everything here is computed from what this AppView stores: no random
//! counts, no placeholder accounts, and no network call while ranking.
//!
//! **The pool.** An account can be suggested when it is in scope, active,
//! has a real handle, is not hidden by a label, has not asked to be left
//! out of recommendations (`contentVisibilityDeclaration`), and posted in
//! the last `suggestions.active_days`. The pool is kept in the process in
//! two orders, newest and popular, each recomputed when it is older than
//! `suggestions.refresh_secs`.
//!
//! **Per viewer.** A ready member also gets friends-of-friends (the
//! accounts most followed by the accounts the member follows), cached per
//! member. Its query runs under the search statement timeout; if that
//! passes, the popular order stands in and is cached just the same, so a
//! slow query costs one timeout per cache period and not one per request.
//!
//! **Per request.** A ready member never sees itself, an account it
//! follows, one it blocks or is blocked by, or one it mutes.
//!
//! `recIdStr` names the computation that produced a response.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::search::{set_timeout, timed_out};
use super::{Hydrator, PublicEmit, ViewError, ViewResult, Viewer};
use crate::config::SuggestionsConfig;
use crate::xrpc::pools::Requester;

/// `suggestions_timeout_total{endpoint}`: per-viewer or per-actor queries
/// stopped by the statement timeout.
pub const METRIC_TIMEOUT: &str = "suggestions_timeout_total";
/// `suggestions_refresh_seconds`: how long each recompute of the pool (one
/// order) took (§14.2).
pub const METRIC_REFRESH_SECONDS: &str = "suggestions_refresh_seconds";

/// What `getSuggestedFollowsByActor` returns at most: the lexicon has no
/// `limit`.
const BY_ACTOR_LIMIT: usize = 25;

/// The pool's two orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    Newest,
    Popular,
}

/// Which list an endpoint serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Newest,
    Popular,
    /// Friends-of-friends for a ready member, popular for anyone else.
    /// `more` skips the first `suggestions.see_more_offset` results.
    Friends { more: bool },
}

/// What one request of an `{actors, recIdStr}` endpoint asks for.
#[derive(Debug, Clone, Copy)]
pub struct Listing<'a> {
    /// The endpoint, for the timeout metric.
    pub endpoint: &'static str,
    pub limit: u32,
    /// `getSuggestions` pages over the pool by offset; the others do not
    /// page.
    pub paged: bool,
    pub cursor: Option<&'a str>,
}

/// An ordered list of accounts with the id of the computation behind it.
#[derive(Debug, Clone, Default)]
pub(super) struct Ranked {
    pub dids: Arc<Vec<String>>,
    pub rec_id: String,
}

#[derive(Default)]
struct Slot {
    ranked: Ranked,
    at: Option<Instant>,
}

/// The suggestion state one hydrator keeps.
pub struct State {
    newest: tokio::sync::Mutex<Slot>,
    popular: tokio::sync::Mutex<Slot>,
    friends: moka::future::Cache<String, Ranked>,
    cofollow: moka::future::Cache<String, Ranked>,
    /// The starter-pack pool (§7.2).
    pub(super) packs: tokio::sync::Mutex<(Arc<Vec<String>>, Option<Instant>)>,
}

impl State {
    #[must_use]
    pub fn new(cfg: &SuggestionsConfig) -> Self {
        let cache = |max: u64, ttl: u64| moka::future::Cache::builder().max_capacity(max).time_to_live(Duration::from_secs(ttl)).build();
        Self {
            newest: tokio::sync::Mutex::default(),
            popular: tokio::sync::Mutex::default(),
            friends: cache(cfg.fof_cache_max, cfg.fof_cache_ttl_secs),
            cofollow: cache(cfg.cofollow_cache_max, cfg.cofollow_cache_ttl_secs),
            packs: tokio::sync::Mutex::default(),
        }
    }
}

/// The hex of `blake3(domain ‖ parts…)`, truncated to 16 bytes.
pub(super) fn rec_id(domain: &str, parts: &[&[u8]]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain.as_bytes());
    for part in parts {
        hasher.update(part);
    }
    hex::encode(&hasher.finalize().as_bytes()[..16])
}

/// Now, as the big-endian microseconds a `recIdStr` is made from.
pub(super) fn computed_at() -> [u8; 8] {
    Utc::now().timestamp_micros().to_be_bytes()
}

/// The pool conditions on `a` (an `actors` row), with the activity
/// requirement when `active_days` is given.
pub(super) fn eligible(active_days: Option<u32>) -> String {
    let mut sql = "EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = a.did) \
         AND a.account_status = 'active' AND a.handle IS NOT NULL AND a.handle <> 'handle.invalid' \
         AND NOT EXISTS (SELECT 1 FROM labels l WHERE l.subject = a.did AND l.val IN ('!takedown', '!hide')) \
         AND NOT EXISTS (SELECT 1 FROM actor_self_records r WHERE r.did = a.did \
                         AND r.collection = 'app.bsky.actor.contentVisibilityDeclaration' \
                         AND r.record -> 'hideFromAlgorithmicRecommendations' = 'true'::jsonb)"
        .to_string();
    if let Some(days) = active_days {
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM posts p WHERE p.author_did = a.did AND p.created_at > now() - interval '{days} days')"
        ));
    }
    sql
}

/// `list` followed by what `rest` adds to it, without repeats.
fn followed_by(list: Vec<String>, rest: &[String]) -> Vec<String> {
    let mut seen: HashSet<String> = list.iter().cloned().collect();
    let mut out = list;
    for did in rest {
        if seen.insert(did.clone()) {
            out.push(did.clone());
        }
    }
    out
}

impl Hydrator {
    fn suggestions(&self) -> &State {
        &self.inner.suggestions
    }

    /// The pool in one order, recomputed when it is older than
    /// `suggestions.refresh_secs`. Stored values only.
    pub(super) async fn suggestion_pool(&self, ordering: Ordering) -> Result<Ranked, sqlx::Error> {
        let cfg = &self.inner.cfg.suggestions;
        let (slot, label, order) = match ordering {
            // A profile dated in the future cannot hold the top.
            Ordering::Newest => (&self.suggestions().newest, "newest", "LEAST(COALESCE(a.profile_created_at, a.indexed_at), now()) DESC, a.did"),
            Ordering::Popular => (
                &self.suggestions().popular,
                "popular",
                "COALESCE((SELECT cc.followers FROM constellation_counts cc WHERE cc.target = a.did), \
                          (SELECT c.followers_count FROM actor_counts c WHERE c.did = a.did), 0) DESC, \
                 (SELECT max(p.created_at) FROM posts p WHERE p.author_did = a.did) DESC NULLS LAST, a.did",
            ),
        };
        let mut slot = slot.lock().await;
        if slot.at.is_none_or(|at| at.elapsed() >= Duration::from_secs(cfg.refresh_secs)) {
            let sql = format!("SELECT a.did FROM actors a WHERE {} ORDER BY {order} LIMIT $1", eligible(Some(cfg.active_days)));
            let started = Instant::now();
            let found = sqlx::query_scalar(&sql).bind(i64::from(cfg.pool_size)).fetch_all(self.pool()).await;
            metrics::histogram!(METRIC_REFRESH_SECONDS).record(started.elapsed().as_secs_f64());
            let dids: Vec<String> = found?;
            slot.ranked = Ranked { dids: Arc::new(dids), rec_id: rec_id("suggestions/v1", &[label.as_bytes(), &computed_at()]) };
            slot.at = Some(Instant::now());
        }
        Ok(slot.ranked.clone())
    }

    /// Accounts ranked by how many of `sql`'s sample follow them, then by
    /// the popular order, under the statement timeout. `sql` takes the DID
    /// as `$1`, the sample size as `$2` and the result cap as `$3`, and
    /// gives `(did, count)`. On a timeout the popular order stands in.
    async fn ranked_by_follows(&self, sql: &str, did: &str, sample: u32, endpoint: &'static str) -> Result<Vec<String>, sqlx::Error> {
        let cfg = &self.inner.cfg;
        let popular = self.suggestion_pool(Ordering::Popular).await?;
        let mut tx = self.pool().begin().await?;
        set_timeout(&mut tx, cfg.search.statement_timeout_ms).await?;
        let found: Result<Vec<(String, i64)>, sqlx::Error> = sqlx::query_as(sql)
            .bind(did)
            .bind(i64::from(sample))
            .bind(i64::from(cfg.suggestions.pool_size))
            .fetch_all(&mut *tx)
            .await;
        match found {
            Ok(mut rows) => {
                tx.commit().await?;
                let rank: HashMap<&str, usize> = popular.dids.iter().enumerate().map(|(i, d)| (d.as_str(), i)).collect();
                rows.sort_by_key(|(did, n)| (std::cmp::Reverse(*n), rank.get(did.as_str()).copied().unwrap_or(usize::MAX)));
                Ok(rows.into_iter().map(|(did, _)| did).collect())
            }
            Err(error) if timed_out(&error) => {
                metrics::counter!(METRIC_TIMEOUT, "endpoint" => endpoint).increment(1);
                Ok(popular.dids.to_vec())
            }
            Err(error) => Err(error),
        }
    }

    /// Friends-of-friends for a ready member: the accounts most followed
    /// by the member's most recent follows, then the popular order. Cached
    /// per member, a timeout's stand-in included.
    async fn friends_of_friends(&self, me: &str, endpoint: &'static str) -> Result<Ranked, sqlx::Error> {
        if let Some(hit) = self.suggestions().friends.get(me).await {
            return Ok(hit);
        }
        let cfg = &self.inner.cfg.suggestions;
        let sql = format!(
            "WITH mine AS (SELECT f.subject_did FROM follows f WHERE f.author_did = $1 ORDER BY f.created_at DESC LIMIT $2) \
             SELECT f.subject_did, count(*) FROM follows f \
             JOIN mine ON mine.subject_did = f.author_did \
             JOIN actors a ON a.did = f.subject_did \
             WHERE f.subject_did <> $1 AND {} \
             GROUP BY f.subject_did ORDER BY count(*) DESC LIMIT $3",
            eligible(Some(cfg.active_days))
        );
        let ranked = self.ranked_by_follows(&sql, me, cfg.fof_max_follows, endpoint).await?;
        let popular = self.suggestion_pool(Ordering::Popular).await?;
        let out = Ranked {
            dids: Arc::new(followed_by(ranked, &popular.dids)),
            rec_id: rec_id("suggestions/v1", &[b"friends", &computed_at(), me.as_bytes()]),
        };
        self.suggestions().friends.insert(me.to_string(), out.clone()).await;
        Ok(out)
    }

    /// Co-follow for `actor`: the accounts most followed by a sample of
    /// its in-scope followers, padded with the popular order when there
    /// are few. Cached per actor, before any viewer's exclusions.
    async fn co_followed(&self, actor: &str) -> Result<Ranked, sqlx::Error> {
        if let Some(hit) = self.suggestions().cofollow.get(actor).await {
            return Ok(hit);
        }
        let cfg = &self.inner.cfg.suggestions;
        let sql = format!(
            "WITH fans AS (SELECT f.author_did FROM follows f WHERE f.subject_did = $1 \
                             AND EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = f.author_did) \
                           ORDER BY f.created_at DESC LIMIT $2) \
             SELECT f.subject_did, count(*) FROM follows f \
             JOIN fans ON fans.author_did = f.author_did \
             JOIN actors a ON a.did = f.subject_did \
             WHERE f.subject_did <> $1 AND {} \
             GROUP BY f.subject_did ORDER BY count(*) DESC LIMIT $3",
            eligible(Some(cfg.active_days))
        );
        let mut dids = self.ranked_by_follows(&sql, actor, cfg.cofollow_sample_followers, "getSuggestedFollowsByActor").await?;
        if dids.len() < usize::try_from(cfg.cofollow_min).unwrap_or(usize::MAX) {
            let popular = self.suggestion_pool(Ordering::Popular).await?;
            dids = followed_by(dids, &popular.dids);
        }
        dids.retain(|did| did != actor);
        let out = Ranked {
            dids: Arc::new(dids),
            rec_id: rec_id("suggestions/v1", &[b"cofollow", &computed_at(), actor.as_bytes()]),
        };
        self.suggestions().cofollow.insert(actor.to_string(), out.clone()).await;
        Ok(out)
    }

    /// Up to `limit` profile views from `ranked`, starting at `skip`, with
    /// the viewer's exclusions applied; and how far into the list the scan
    /// went. Inbound list blocks are known only after hydration, so a page
    /// can be short.
    async fn suggested_views(&self, viewer: &Viewer, req: &Requester, ranked: &Ranked, skip: usize, limit: usize) -> Result<(Vec<Value>, usize), sqlx::Error> {
        let window: Vec<String> = ranked.dids.iter().skip(skip).take(limit * 3 + 20).cloned().collect();
        let state = match viewer.state_did() {
            Some(_) => Some(self.actor_viewer(viewer, &window).await?),
            None => None,
        };
        let mut kept: Vec<String> = Vec::new();
        let mut scanned = 0;
        for (i, did) in window.iter().enumerate() {
            if kept.len() == limit {
                break;
            }
            scanned = i + 1;
            let excluded = viewer.did.as_deref() == Some(did.as_str())
                || state.as_ref().is_some_and(|s| s.following.contains_key(did) || s.blocked_either_way(did) || s.muted.contains(did));
            if !excluded {
                kept.push(did.clone());
            }
        }
        let (visible, ctx) = self.searchable(viewer, req, &kept).await?;
        Ok((visible.iter().map(|d| ctx.view(d, ProfileKind::View)).collect(), skip + scanned))
    }

    /// The actor-suggestion endpoints that answer `{actors, recIdStr}`
    /// (design-v3 §7.1's table).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn suggested_users(&self, viewer: &Viewer, req: &Requester, kind: Kind, listing: Listing<'_>) -> ViewResult<Value> {
        let cfg = &self.inner.cfg.suggestions;
        let Listing { endpoint, limit, paged, cursor } = listing;
        let (ranked, mut skip) = match (kind, viewer.state_did()) {
            (Kind::Newest, _) => (self.suggestion_pool(Ordering::Newest).await?, 0),
            (Kind::Friends { more }, Some(me)) => {
                let skip = if more { usize::try_from(cfg.see_more_offset).unwrap_or(0) } else { 0 };
                (self.friends_of_friends(me, endpoint).await?, skip)
            }
            (Kind::Popular | Kind::Friends { .. }, _) => (self.suggestion_pool(Ordering::Popular).await?, 0),
        };
        if paged {
            skip = cursor.and_then(|c| c.parse::<usize>().ok()).unwrap_or(0);
        }
        let limit = usize::try_from(limit.max(1)).unwrap_or(1);
        let (actors, reached) = self.suggested_views(viewer, req, &ranked, skip, limit).await?;
        let mut out = json!({ "actors": actors, "recIdStr": ranked.rec_id });
        if paged && reached < ranked.dids.len() {
            out["cursor"] = json!(reached.to_string());
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.graph.getSuggestedFollowsByActor`. `isFallback` and the
    /// numeric `recId` are deprecated and not emitted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn suggested_follows_by_actor(&self, viewer: &Viewer, req: &Requester, actor: &str) -> ViewResult<Value> {
        let Some(did) = self.resolve_actor(req, actor).await? else {
            return Ok(Err(ViewError::new("InvalidRequest", "Actor not found")));
        };
        let ranked = self.co_followed(&did).await?;
        let (suggestions, _) = self.suggested_views(viewer, req, &ranked, 0, BY_ACTOR_LIMIT).await?;
        Ok(Ok(Self::emit(json!({ "suggestions": suggestions, "recIdStr": ranked.rec_id }))))
    }

    /// `app.bsky.unspecced.getTaggedSuggestions` (design-v3 §7.3): the
    /// first accounts of the popular order, under the users tag. The feeds
    /// tag joins when feed generators are served.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn tagged_suggestions(&self) -> ViewResult<Value> {
        let count = usize::try_from(self.inner.cfg.suggestions.tagged_count).unwrap_or(0);
        let popular = self.suggestion_pool(Ordering::Popular).await?;
        let suggestions: Vec<Value> = popular
            .dids
            .iter()
            .take(count)
            .map(|did| json!({"tag": "suggested-users", "subjectType": "actor", "subject": did}))
            .collect();
        Ok(Ok(Self::emit(json!({ "suggestions": suggestions }))))
    }

    /// `getSuggestedStarterPacks` and `getOnboardingSuggestedStarterPacks`
    /// (design-v3 §7.2): stored packs of creators the pool's conditions
    /// allow (activity aside), most joined this week first. Never the
    /// viewer's own packs, nor a pack whose creator the viewer blocks, is
    /// blocked by, or mutes.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn suggested_starter_packs(&self, viewer: &Viewer, req: &Requester, limit: u32) -> ViewResult<Value> {
        let cfg = &self.inner.cfg.suggestions;
        let pool = {
            let mut slot = self.suggestions().packs.lock().await;
            if slot.1.is_none_or(|at| at.elapsed() >= Duration::from_secs(cfg.refresh_secs)) {
                let sql = format!(
                    "SELECT sp.uri FROM starter_packs sp JOIN actors a ON a.did = sp.creator_did \
                     LEFT JOIN starter_pack_counts c ON c.pack_uri = sp.uri WHERE {} \
                     ORDER BY COALESCE(c.joined_week_count, 0) DESC, COALESCE(c.joined_all_time_count, 0) DESC, sp.uri LIMIT $1",
                    eligible(None)
                );
                slot.0 = Arc::new(sqlx::query_scalar(&sql).bind(i64::from(cfg.pool_size)).fetch_all(self.pool()).await?);
                slot.1 = Some(Instant::now());
            }
            Arc::clone(&slot.0)
        };
        let limit = usize::try_from(limit.max(1)).unwrap_or(1);
        let mine = viewer.did.as_deref();
        let window: Vec<String> = pool
            .iter()
            .filter(|uri| mine.is_none() || crate::db::locks::uri_author(uri) != mine)
            .take(limit * 3 + 20)
            .cloned()
            .collect();
        let views = self.pack_views(viewer, req, &window, limit).await?;
        Ok(Ok(Self::emit(json!({ "starterPacks": views }))))
    }

    /// `app.bsky.unspecced.getConfig` (design-v3 §7.6): two operator
    /// settings. Email belongs to the PDS, so `checkEmailConfirmed` is a
    /// policy switch and not a fact about any account.
    #[must_use]
    pub fn unspecced_config(&self) -> PublicEmit<Value> {
        let live = self.inner.cfg.live.load();
        let cfg = &live.unspecced;
        let live: Vec<Value> = cfg.live_now.iter().map(|l| json!({"did": l.did, "domains": l.domains})).collect();
        Self::emit(json!({ "checkEmailConfirmed": cfg.check_email_confirmed, "liveNow": live }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::test_support::fresh_db;

    fn did(c: char) -> String {
        format!("did:plc:{}", c.to_string().repeat(24))
    }

    fn dids(view: &Value, key: &str) -> Vec<String> {
        view[key].as_array().expect(key).iter().map(|a| a["did"].as_str().expect("did").to_string()).collect()
    }

    /// B-43 / B-44 (design-v3 §7.1, §7.3): the pool's conditions, its two
    /// orders, friends-of-friends, co-follow with padding, and what a
    /// member is never shown.
    #[tokio::test]
    async fn suggestions_come_from_the_pool_and_respect_the_viewer() {
        let Some(db) = fresh_db("hydration_suggestions").await else {
            return;
        };
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let p = &db.pool;
        let (me, a, b, c, hidden, quiet, opted_out, outside) = (did('m'), did('a'), did('b'), did('c'), did('h'), did('q'), did('o'), did('x'));
        for (i, d) in [&me, &a, &b, &c, &hidden, &quiet, &opted_out, &outside].into_iter().enumerate() {
            actor(p, d, &format!("user{i}.test"), "User").await;
            if *d != outside {
                sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)").bind(d).execute(p).await.expect("scope");
            }
            if *d != quiet {
                post(p, d, "3ka", "hello", None, None).await;
            }
            // Later rows are newer profiles.
            sqlx::query("UPDATE actors SET profile_created_at = now() - make_interval(days => $2) WHERE did = $1")
                .bind(d)
                .bind(20 - i32::try_from(i).expect("i"))
                .execute(p)
                .await
                .expect("created");
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{me}', 'admin', now())")).await;
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{hidden}', '!hide')")).await;
        exec(p, &format!(
            "INSERT INTO actor_self_records (uri, did, collection, cid, rev, record) VALUES \
             ('at://{opted_out}/app.bsky.actor.contentVisibilityDeclaration/self', '{opted_out}', 'app.bsky.actor.contentVisibilityDeclaration', 'bafyd', '3k', \
              '{{\"hideFromAlgorithmicRecommendations\": true}}')"
        )).await;
        let follow = |from: &str, to: &str| format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{from}/app.bsky.graph.follow/{}', '{from}', '{to}', '3k', now())", &to[8..12]);
        // The member follows A; A follows B and C; B follows C. So C has
        // the most followers, and B and C are friends of a friend.
        for (from, to) in [(&me, &a), (&a, &b), (&a, &c), (&b, &c)] {
            exec(p, &follow(from, to)).await;
        }

        let h = hydrator(p, ScopeSets::default());
        let req = Requester::test();
        let anon = Viewer::anonymous();
        let member = Viewer { did: Some(me.clone()), hydrate: true };
        let users = |viewer: Viewer, kind: Kind| {
            let (h, req) = (h.clone(), req.clone());
            let listing = Listing { endpoint: "test", limit: 25, paged: false, cursor: None };
            async move { h.suggested_users(&viewer, &req, kind, listing).await.expect("db").expect("view").inner().clone() }
        };

        // Newest first; never the hidden, the silent, the opted-out or the
        // out-of-scope account.
        let newest = users(anon.clone(), Kind::Newest).await;
        assert_eq!(dids(&newest, "actors"), [c.clone(), b.clone(), a.clone(), me.clone()]);
        assert_eq!(newest["recIdStr"].as_str().map(str::len), Some(32));
        // Popular: by stored followers.
        let popular = users(anon.clone(), Kind::Popular).await;
        assert_eq!(dids(&popular, "actors")[0], c);
        // Signed out, friends-of-friends is popular.
        assert_eq!(dids(&users(anon.clone(), Kind::Friends { more: false }).await, "actors"), dids(&popular, "actors"));

        // The member never sees itself or the account it follows; the
        // friends of its friend come first, most followed first.
        let friends = users(member.clone(), Kind::Friends { more: false }).await;
        assert_eq!(dids(&friends, "actors"), [c.clone(), b.clone()]);
        // A mute leaves an account out.
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{me}', '{b}')")).await;
        assert_eq!(dids(&users(member.clone(), Kind::Newest).await, "actors"), std::slice::from_ref(&c));

        // getSuggestions pages over the pool by offset.
        let page = |cursor| Listing { endpoint: "test", limit: 2, paged: true, cursor };
        let first = h.suggested_users(&anon, &req, Kind::Newest, page(None)).await.expect("db").expect("view").inner().clone();
        assert_eq!((dids(&first, "actors"), &first["cursor"]), (vec![c.clone(), b.clone()], &json!("2")));
        let rest = h.suggested_users(&anon, &req, Kind::Newest, page(Some("2"))).await.expect("db").expect("view").inner().clone();
        assert_eq!((dids(&rest, "actors"), rest.get("cursor")), (vec![a.clone(), me.clone()], None));

        // Co-follow for A: its one in-scope follower follows nobody else,
        // so the popular order pads the list, without A itself.
        let by_actor = h.suggested_follows_by_actor(&anon, &req, &a).await.expect("db").expect("view").inner().clone();
        let suggested = dids(&by_actor, "suggestions");
        assert!(!suggested.contains(&a) && suggested.contains(&c), "{suggested:?}");

        // The users tag, for the first accounts of the popular order.
        let tagged = h.tagged_suggestions().await.expect("db").expect("view").inner().clone();
        assert_eq!(tagged["suggestions"][0], json!({"tag": "suggested-users", "subjectType": "actor", "subject": c}));
        assert_eq!(h.unspecced_config().inner(), &json!({"checkEmailConfirmed": false, "liveNow": []}));

        // Packs: of eligible creators, most joined this week first; never
        // the viewer's own, nor one whose creator the viewer mutes.
        for (owner, rkey, week) in [(&a, "pa", 1), (&c, "pc", 7), (&me, "pm", 9), (&hidden, "ph", 9), (&b, "pb", 3)] {
            let uri = format!("at://{owner}/app.bsky.graph.starterpack/{rkey}");
            exec(p, &format!(
                "INSERT INTO starter_packs (uri, creator_did, rkey, cid, rev, name, list_uri, record, created_at) VALUES \
                 ('{uri}', '{owner}', '{rkey}', 'bafyp', '3k', 'Pack', 'at://{owner}/app.bsky.graph.list/l', '{{\"name\": \"Pack\"}}', now())"
            )).await;
            exec(p, &format!(
                "INSERT INTO starter_pack_counts (pack_uri, joined_week_count, joined_all_time_count, all_time_source, refreshed_at) \
                 VALUES ('{uri}', {week}, {week}, 'local', now())"
            )).await;
        }
        let packs = |view: &Value| view["starterPacks"].as_array().expect("packs").iter().map(|s| s["uri"].as_str().expect("uri").rsplit('/').next().expect("rkey").to_string()).collect::<Vec<_>>();
        let all = h.suggested_starter_packs(&anon, &req, 10).await.expect("db").expect("view").inner().clone();
        assert_eq!(packs(&all), ["pm", "pc", "pb", "pa"]);
        let mine = h.suggested_starter_packs(&member, &req, 10).await.expect("db").expect("view").inner().clone();
        assert_eq!(packs(&mine), ["pc", "pa"]);
        // `suggestions_refresh_seconds`: every recompute of the pool is
        // timed. The test config (`refresh_secs = 0`) recomputes on every
        // read, so there is one per pool read above.
        let text = handle.render();
        let timed = crate::test_support::series(&text, "suggestions_refresh_seconds_count", &[]);
        assert!(timed.is_some_and(|n| n >= 2.0), "{text}");
        db.drop_db().await;
    }
}
