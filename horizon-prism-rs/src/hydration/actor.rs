//! Actor views (design-v2 §7.1): `getProfile`, `getProfiles`,
//! `searchActors`, and the `profileView*` builders every other view uses.

use std::collections::HashMap;
use std::time::Instant;

use futures::StreamExt;
use serde_json::{Map, Value, json};

use sqlx::{Postgres, QueryBuilder};

use super::search::{METRIC_TIMEOUT, set_timeout, timed_out, viewer_exclusions};
use super::{ActorRow, ActorViewer, CountMode, Hydrator, INVALID_HANDLE, ViewError, ViewResult, Viewer, blob_url, ts};
use crate::moderation::{self, Gate};
use crate::xrpc::pools::Requester;

/// Upstream profile fetches in flight for one `getProfiles`.
pub const PROFILE_FETCH_CONCURRENCY: usize = 8;

/// Which profile view to build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileKind {
    Basic,
    View,
    Detailed,
}

/// Inputs for building profile views of many actors.
pub struct ProfileContext {
    pub rows: HashMap<String, ActorRow>,
    pub viewer: ActorViewer,
    pub labels: HashMap<String, Vec<Value>>,
    pub followers: HashMap<String, i32>,
    /// By actor: `allowSubscriptions` of its stored notification
    /// declaration. No entry means no record, which reads as `followers`.
    pub declarations: HashMap<String, String>,
    /// By actor: its `statusView` (design-v3 §8.1), for the actors whose
    /// status is shown.
    pub statuses: HashMap<String, Value>,
    /// By actor: what only `getProfile` and `getProfiles` add to a
    /// detailed view. Empty everywhere else.
    pub details: HashMap<String, Detail>,
}

/// The parts of a detailed view that `getProfile` and `getProfiles` load
/// for their subjects alone (design-v3 §8.1). `None` leaves a field out.
#[derive(Debug, Clone, Default)]
pub struct Detail {
    /// `associated.lists`.
    pub lists: Option<i64>,
    /// `associated.starterPacks`.
    pub starter_packs: Option<i64>,
    /// `associated.labeler`.
    pub labeler: Option<bool>,
    /// `joinedViaStarterPack`, a `starterPackViewBasic`.
    pub joined_via: Option<Value>,
    /// `viewer.knownFollowers`.
    pub known_followers: Option<Value>,
}

impl ProfileContext {
    /// A profile view; an actor without a local row gets the minimal
    /// `{did, handle: "handle.invalid"}` form.
    #[must_use]
    pub fn view(&self, did: &str, kind: ProfileKind) -> Value {
        let mut v = Map::new();
        v.insert("did".into(), json!(did));
        let row = self.rows.get(did);
        v.insert(
            "handle".into(),
            json!(row.and_then(|r| r.handle.as_deref()).unwrap_or(INVALID_HANDLE)),
        );
        if let Some(r) = row {
            if let Some(n) = &r.display_name {
                v.insert("displayName".into(), json!(n));
            }
            if let Some(url) = r.avatar_cid.as_deref().and_then(|c| blob_url(r.pds_endpoint.as_deref(), did, c)) {
                v.insert("avatar".into(), json!(url));
            }
            if let Some(created) = r.profile_record.as_ref().and_then(|p| p.get("createdAt")).cloned() {
                v.insert("createdAt".into(), created);
            }
            if let Some(p) = &r.pronouns {
                v.insert("pronouns".into(), json!(p));
            }
            if kind != ProfileKind::Basic {
                if let Some(d) = &r.description {
                    v.insert("description".into(), json!(d));
                }
                v.insert("indexedAt".into(), json!(ts(r.indexed_at)));
            }
            if kind == ProfileKind::Detailed {
                if let Some(url) = r.banner_cid.as_deref().and_then(|c| blob_url(r.pds_endpoint.as_deref(), did, c)) {
                    v.insert("banner".into(), json!(url));
                }
                let followers = self.followers.get(did).copied().or(r.followers_count).unwrap_or(0);
                v.insert("followersCount".into(), json!(followers));
                v.insert("followsCount".into(), json!(r.follows_count.unwrap_or(0)));
                v.insert("postsCount".into(), json!(r.posts_count.unwrap_or(0)));
                if let Some(pinned) = r.profile_record.as_ref().and_then(|p| p.get("pinnedPost")).cloned() {
                    v.insert("pinnedPost".into(), pinned);
                }
                if let Some(w) = &r.website {
                    v.insert("website".into(), json!(w));
                }
                // Who may subscribe to this account's posts (design-v3 §8.1).
                let allow = self.declarations.get(did).map_or("followers", String::as_str);
                let mut associated = json!({"activitySubscription": {"allowSubscriptions": allow}});
                if let Some(d) = self.details.get(did) {
                    if let Some(n) = d.lists {
                        associated["lists"] = json!(n);
                    }
                    if let Some(n) = d.starter_packs {
                        associated["starterPacks"] = json!(n);
                    }
                    if let Some(b) = d.labeler {
                        associated["labeler"] = json!(b);
                    }
                    if let Some(pack) = &d.joined_via {
                        v.insert("joinedViaStarterPack".into(), pack.clone());
                    }
                }
                v.insert("associated".into(), associated);
            }
        }
        if let Some(status) = self.statuses.get(did) {
            v.insert("status".into(), status.clone());
        }
        let mut viewer = self.viewer.view(did);
        if kind == ProfileKind::Detailed
            && let Some(known) = self.details.get(did).and_then(|d| d.known_followers.as_ref())
        {
            viewer["knownFollowers"] = known.clone();
        }
        v.insert("viewer".into(), viewer);
        v.insert("labels".into(), Value::Array(self.labels.get(did).cloned().unwrap_or_default()));
        Value::Object(v)
    }
}

impl Hydrator {
    /// Everything needed to render profiles for `dids`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn profile_context(
        &self,
        viewer: &Viewer,
        req: &Requester,
        dids: &[String],
        followers: Option<CountMode>,
    ) -> Result<ProfileContext, sqlx::Error> {
        let rows = self.load_actors(dids).await?;
        let mut actor_viewer = self.actor_viewer(viewer, dids).await?;
        self.fill_list_views(viewer, req, &mut actor_viewer).await?;
        let labels = self.load_labels(dids).await?;
        let followers = match followers {
            Some(mode) => self
                .constellation_counts(req, dids, mode)
                .await
                .into_iter()
                .filter_map(|(d, c)| c.followers.map(|f| (d, f)))
                .collect(),
            None => HashMap::new(),
        };
        let declarations: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT did, record->>'allowSubscriptions' FROM actor_self_records WHERE did = ANY($1) AND collection = $2",
        )
        .bind(dids)
        .bind(super::activity_subscriptions::NSID_DECLARATION)
        .fetch_all(self.pool())
        .await?;
        let stored: Vec<(String, String, String, Value, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT did, uri, cid, record, indexed_at FROM actor_self_records WHERE did = ANY($1) AND collection = $2",
        )
        .bind(dids)
        .bind(NSID_STATUS)
        .fetch_all(self.pool())
        .await?;
        let now = chrono::Utc::now();
        let live_now = &self.inner.cfg.unspecced.live_now;
        let statuses = stored
            .into_iter()
            .filter_map(|(did, uri, cid, record, indexed_at)| {
                let pds = rows.get(&did).and_then(|r| r.pds_endpoint.as_deref());
                let view = status_view(&did, &uri, &cid, &record, indexed_at, now, pds, live_now)?;
                Some((did, view))
            })
            .collect();
        Ok(ProfileContext {
            rows,
            viewer: actor_viewer,
            labels,
            followers,
            declarations: declarations.into_iter().filter_map(|(did, allow)| Some((did, allow?))).collect(),
            statuses,
            details: HashMap::new(),
        })
    }

    /// The list views behind `viewer`'s `mutedByList` and `blockingByList`
    /// (design-v3 §5.3), hydrated like any other: a list that is not
    /// locally servable is read from its record. One that cannot be read
    /// leaves the field out; the mute or block applies all the same. The
    /// views carry the viewer's own state, so they stay in `viewer`, which
    /// lives for one request.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(super) async fn fill_list_views(&self, viewer: &Viewer, req: &Requester, state: &mut ActorViewer) -> Result<(), sqlx::Error> {
        let mut unseen: Vec<String> = state
            .muted_by_list
            .values()
            .chain(state.blocking_by_list.values())
            .filter(|list| !state.list_views.contains_key(*list))
            .cloned()
            .collect();
        unseen.sort_unstable();
        unseen.dedup();
        for list in unseen {
            if let Some((row, _)) = self.load_list(req, &list).await? {
                let view = self.list_basic(viewer, &row, None, None).await?;
                state.list_views.insert(list, view);
            }
        }
        Ok(())
    }

    /// Fill `ctx.details` for the actors of `ctx` with a local row, each
    /// part in one batch (design-v3 §8.1):
    /// - `associated.lists` and `associated.starterPacks` for a ready
    ///   member, whose backfill holds all of its lists and packs. For
    ///   anyone else a local count would be an undercount stated as fact.
    /// - `associated.labeler` for an account in scope, whose labeler
    ///   declaration is fetched at every depth.
    /// - `joinedViaStarterPack` as a `starterPackViewBasic`.
    /// - `viewer.knownFollowers` for a ready-member viewer.
    async fn profile_details(&self, viewer: &Viewer, req: &Requester, ctx: &mut ProfileContext, started: Instant) -> Result<(), sqlx::Error> {
        let dids: Vec<String> = ctx.rows.keys().cloned().collect();
        if dids.is_empty() {
            return Ok(());
        }
        let p = self.pool();
        let ready: Vec<String> = sqlx::query_scalar("SELECT did FROM members WHERE did = ANY($1) AND ready_at IS NOT NULL")
            .bind(&dids)
            .fetch_all(p)
            .await?;
        let mut counts: Vec<HashMap<String, i64>> = Vec::new();
        for table in ["lists", "starter_packs"] {
            let rows: Vec<(String, i64)> =
                sqlx::query_as(&format!("SELECT creator_did, count(*) FROM {table} WHERE creator_did = ANY($1) GROUP BY creator_did"))
                    .bind(&ready)
                    .fetch_all(p)
                    .await?;
            counts.push(rows.into_iter().collect());
        }
        let pack_counts = counts.pop().unwrap_or_default();
        let list_counts = counts.pop().unwrap_or_default();
        let labelers: std::collections::HashSet<String> = sqlx::query_scalar("SELECT creator_did FROM labeler_services WHERE creator_did = ANY($1)")
            .bind(&dids)
            .fetch_all(p)
            .await?
            .into_iter()
            .collect();
        let in_scope: Vec<String> = {
            let sets = self.scope().read_guard().await;
            dids.iter().filter(|d| sets.in_scope(d)).cloned().collect()
        };
        let packs: Vec<String> = ctx.rows.values().filter_map(|r| r.joined_via_starter_pack_uri.clone()).collect();
        let packs = self.pack_basics(viewer, req, &packs).await?;
        let mut known = self.profile_known_followers(viewer, req, &dids, started).await?;
        for did in dids {
            let member = ready.contains(&did);
            let detail = Detail {
                lists: member.then(|| list_counts.get(&did).copied().unwrap_or(0)),
                starter_packs: member.then(|| pack_counts.get(&did).copied().unwrap_or(0)),
                labeler: in_scope.contains(&did).then(|| labelers.contains(&did)),
                joined_via: ctx.rows.get(&did).and_then(|r| r.joined_via_starter_pack_uri.as_ref()).and_then(|u| packs.get(u).cloned()),
                known_followers: known.remove(&did),
            };
            ctx.details.insert(did, detail);
        }
        Ok(())
    }

    /// Profiles with no viewer state computed (Thread Lens, §10.3).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(crate) async fn anonymous_profile_context(&self, dids: &[String]) -> Result<ProfileContext, sqlx::Error> {
        Ok(ProfileContext {
            rows: self.load_actors(dids).await?,
            viewer: ActorViewer::default(),
            labels: self.load_labels(dids).await?,
            followers: HashMap::new(),
            declarations: HashMap::new(),
            statuses: HashMap::new(),
            details: HashMap::new(),
        })
    }

    /// `app.bsky.actor.getProfile`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_profile(&self, viewer: &Viewer, req: &Requester, actor: &str) -> ViewResult<Value> {
        let started = Instant::now();
        let Some(did) = self.resolve_actor(req, actor).await? else {
            return Ok(Err(ViewError::new("InvalidRequest", "Profile not found")));
        };
        let gate = moderation::gate(self.pool(), &did).await?;
        if let Some((error, message)) = gate.profile_error() {
            return Ok(Err(ViewError::new(error, message)));
        }
        let mut ctx = self
            .profile_context(viewer, req, std::slice::from_ref(&did), Some(CountMode::Sync { started }))
            .await?;
        if !ctx.rows.contains_key(&did) && !self.in_scope(&did).await {
            // Out-of-scope subject: the §8.3 cascade, then Slingshot.
            return Ok(match self.upstream().profile(req, &did).await {
                Some(v) => Ok(Self::emit(self.normalized(viewer, req, v).await?.0)),
                None => Err(ViewError::new("InvalidRequest", "Profile not found")),
            });
        }
        self.profile_details(viewer, req, &mut ctx, started).await?;
        Ok(Ok(Self::emit(ctx.view(&did, ProfileKind::Detailed))))
    }

    /// `app.bsky.actor.getProfiles` (≤ 25): unknown or hidden actors are
    /// omitted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_profiles(&self, viewer: &Viewer, req: &Requester, actors: &[String]) -> ViewResult<Value> {
        let started = Instant::now();
        let mut dids: Vec<String> = Vec::new();
        for a in actors.iter().take(25) {
            if let Some(d) = self.resolve_actor(req, a).await?
                && !dids.contains(&d)
            {
                dids.push(d);
            }
        }
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        dids.retain(|d| !gates.get(d).copied().unwrap_or(Gate::Visible).is_hidden());
        let mut ctx = self.profile_context(viewer, req, &dids, Some(CountMode::Sync { started })).await?;
        self.profile_details(viewer, req, &mut ctx, started).await?;
        // Out-of-scope actors take `getProfile`'s path: the cascade, then
        // the Slingshot profile record (design-v3 §11 G-1). One that no
        // source resolves, or that there is no budget to fetch, is omitted.
        let mut outside: Vec<String> = Vec::new();
        for d in &dids {
            if !ctx.rows.contains_key(d) && !self.in_scope(d).await {
                outside.push(d.clone());
            }
        }
        let fetched: Vec<(String, Option<Value>)> = futures::stream::iter(outside)
            .map(|d| async move {
                let view = self.upstream().profile(req, &d).await;
                (d, view)
            })
            .buffer_unordered(PROFILE_FETCH_CONCURRENCY)
            .collect()
            .await;
        let mut remote: HashMap<String, Value> = HashMap::new();
        for (d, view) in fetched {
            if let Some(v) = view {
                remote.insert(d, self.normalized(viewer, req, v).await?.0);
            }
        }
        let profiles: Vec<Value> = dids
            .iter()
            .filter_map(|d| {
                if ctx.rows.contains_key(d) {
                    Some(ctx.view(d, ProfileKind::Detailed))
                } else {
                    remote.remove(d)
                }
            })
            .collect();
        Ok(Ok(Self::emit(json!({ "profiles": profiles }))))
    }

    /// `app.bsky.actor.searchActors` (design-v3 §9.3): trigram match over
    /// the handles and display names of active accounts that are in scope
    /// or members. The corpus stops there because an identity row can exist
    /// for an account whose only tie to this AppView is a private reply to
    /// a member, and a search over every row would reveal it.
    ///
    /// A ready member's mutes and blocks apply, in the query and, with
    /// inbound list blocks, after hydration. The cursor is an offset that
    /// follows the scan, so a page can be short.
    ///
    /// # Errors
    ///
    /// Database errors other than the search's own timeout.
    pub async fn search_actors(&self, viewer: &Viewer, req: &Requester, q: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let q = q.trim().trim_start_matches('@').to_lowercase();
        if q.is_empty() {
            return Ok(Ok(Self::emit(json!({ "actors": [] }))));
        }
        let offset: i64 = cursor.and_then(|c| c.parse().ok()).unwrap_or(0).max(0);
        let limit = i64::from(limit.clamp(1, 100));
        let pattern = format!("%{}%", like_escape(&q));
        let sql = format!(
            "SELECT a.did FROM actors a CROSS JOIN LATERAL (SELECT a.did AS author_did) n \
             WHERE ($1::text IS NULL OR ({exclusions})) AND {IN_CORPUS} AND ($5 OR {SHOWN_SIGNED_OUT}) \
               AND (a.handle ILIKE $2 OR a.display_name ILIKE $2) \
             ORDER BY GREATEST(similarity(coalesce(a.handle, ''), $3), similarity(coalesce(a.display_name, ''), $3)) DESC, a.did \
             LIMIT $4 OFFSET $6",
            exclusions = viewer_exclusions(),
        );
        let mut tx = self.pool().begin().await?;
        set_timeout(&mut tx, self.inner.cfg.search.statement_timeout_ms).await?;
        let found = sqlx::query_scalar(&sql)
            .bind(viewer.state_did())
            .bind(&pattern)
            .bind(&q)
            .bind(limit)
            .bind(viewer.did.is_some())
            .bind(offset)
            .fetch_all(&mut *tx)
            .await;
        let dids: Vec<String> = match found {
            Ok(dids) => dids,
            Err(error) if timed_out(&error) => {
                metrics::counter!(METRIC_TIMEOUT, "endpoint" => "searchActors").increment(1);
                return Ok(Err(too_expensive()));
            }
            Err(error) => return Err(error.into()),
        };
        tx.commit().await?;
        let (visible, ctx) = self.searchable(viewer, req, &dids).await?;
        let actors: Vec<Value> = visible.iter().map(|d| ctx.view(d, ProfileKind::View)).collect();
        let mut out = json!({ "actors": actors });
        if i64::try_from(dids.len()).unwrap_or(0) == limit {
            out["cursor"] = json!((offset + limit).to_string());
        }
        Ok(Ok(Self::emit(out)))
    }

    /// Of the accounts a search found: those the moderation gate shows and
    /// the viewer neither blocks, is blocked by, nor fully mutes, with
    /// their profile context. An inbound list block is known only here.
    pub(super) async fn searchable(&self, viewer: &Viewer, req: &Requester, dids: &[String]) -> Result<(Vec<String>, ProfileContext), sqlx::Error> {
        let gates = moderation::gate_many(self.pool(), dids).await?;
        let shown: Vec<String> = dids
            .iter()
            .filter(|d| !gates.get(*d).copied().unwrap_or(Gate::Visible).is_hidden())
            .cloned()
            .collect();
        let ctx = self.profile_context(viewer, req, &shown, None).await?;
        let visible = shown
            .into_iter()
            .filter(|d| !ctx.viewer.blocked_either_way(d) && !ctx.viewer.muted.contains(d))
            .collect();
        Ok((visible, ctx))
    }

    /// `app.bsky.actor.searchActorsTypeahead` (design-v3 §9.4): accounts
    /// whose handle or display name starts with `q`, or (from three
    /// characters) whose display name holds a word that does.
    ///
    /// Each branch takes a bounded number of rows in index order before
    /// anything is ranked, so one character costs a bounded range scan. The
    /// prefix branches use the pattern operators, the only ones a
    /// `text_pattern_ops` index serves, between `q` and `q` with its last
    /// code point raised by one. An exact handle always takes part.
    ///
    /// Order: members, then accounts the viewer follows, then the exact
    /// handle, then the shortest handle. The corpus and the exclusions are
    /// `searchActors`'s; the caller makes the block lookup cache-only.
    ///
    /// # Errors
    ///
    /// Database errors other than the search's own timeout.
    pub async fn search_actors_typeahead(&self, viewer: &Viewer, req: &Requester, q: &str, limit: u32) -> ViewResult<Value> {
        let q = q.trim().trim_start_matches('@').to_lowercase();
        if q.is_empty() {
            return Ok(Ok(Self::emit(json!({ "actors": [] }))));
        }
        let cfg = &self.inner.cfg.search;
        let candidates = i64::from(cfg.typeahead_candidates);
        let upper = successor(&q);
        let prefix = format!("{}%", like_escape(&q));

        let mut qb: QueryBuilder<'_, Postgres> = QueryBuilder::new("WITH v AS (SELECT ");
        qb.push_bind(viewer.state_did().map(str::to_string)).push("::text AS did)");
        for (name, column) in [("h", "a.handle"), ("d", "lower(a.display_name)")] {
            qb.push(format!(", {name} AS (SELECT a.did FROM actors a WHERE {column} ~>=~ ")).push_bind(q.clone());
            if let Some(upper) = &upper {
                qb.push(format!(" AND {column} ~<~ ")).push_bind(upper.clone());
            }
            qb.push(format!(" AND {column} LIKE ")).push_bind(prefix.clone());
            qb.push(format!(" AND {IN_CORPUS} ORDER BY {column} USING ~<~ LIMIT ")).push_bind(candidates).push(")");
        }
        qb.push(", e AS (SELECT a.did FROM actors a WHERE a.handle = ").push_bind(q.clone()).push(format!(" AND {IN_CORPUS})"));
        let mut branches = "SELECT did FROM h UNION SELECT did FROM d UNION SELECT did FROM e".to_string();
        if q.chars().count() >= 3 {
            qb.push(", w AS (SELECT a.did FROM actors a WHERE a.display_name ILIKE ").push_bind(format!("% {}%", like_escape(&q)));
            qb.push(format!(" AND {IN_CORPUS} LIMIT ")).push_bind(candidates).push(")");
            branches.push_str(" UNION SELECT did FROM w");
        }
        qb.push(format!(
            " SELECT a.did FROM actors a CROSS JOIN LATERAL (SELECT a.did AS author_did) n \
              WHERE a.did IN ({branches}) AND ($1::text IS NULL OR ({exclusions}))",
            exclusions = viewer_exclusions(),
        ));
        if viewer.did.is_none() {
            qb.push(format!(" AND {SHOWN_SIGNED_OUT}"));
        }
        qb.push(
            " ORDER BY EXISTS (SELECT 1 FROM members m WHERE m.did = a.did) DESC, \
                       EXISTS (SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = a.did) DESC, \
                       (a.handle = ",
        );
        qb.push_bind(q.clone()).push(") DESC, length(a.handle), a.handle LIMIT ").push_bind(i64::from(limit.clamp(1, 100)));

        let mut tx = self.pool().begin().await?;
        set_timeout(&mut tx, cfg.statement_timeout_ms).await?;
        let dids: Vec<String> = match qb.build_query_scalar().fetch_all(&mut *tx).await {
            Ok(dids) => dids,
            Err(error) if timed_out(&error) => {
                metrics::counter!(METRIC_TIMEOUT, "endpoint" => "searchActorsTypeahead").increment(1);
                return Ok(Err(too_expensive()));
            }
            Err(error) => return Err(error.into()),
        };
        tx.commit().await?;
        let (visible, ctx) = self.searchable(viewer, req, &dids).await?;
        let actors: Vec<Value> = visible.iter().map(|d| ctx.view(d, ProfileKind::Basic)).collect();
        Ok(Ok(Self::emit(json!({ "actors": actors }))))
    }
}

pub const NSID_STATUS: &str = "app.bsky.actor.status";
const STATUS_LIVE: &str = "app.bsky.actor.status#live";

/// The `statusView` of a stored `app.bsky.actor.status` record (design-v3
/// §8.1), or `None` when it is not shown.
///
/// - `embed` is the `external#view` of the record's external embed.
/// - `expiresAt` and `isActive` are present only when the record sets
///   `durationMinutes`. The duration is clamped to 5 minutes – 4 hours and
///   counted from the record's `createdAt`, which is itself clamped to
///   when the record was indexed, so a status dated in the future cannot
///   stay live indefinitely.
/// - With `unspecced.live_now` set, a **live** status is shown only for an
///   account on that list whose embed links to one of the domains listed
///   for it; otherwise it is left out whole. Other statuses are untouched.
#[allow(clippy::too_many_arguments)]
fn status_view(
    did: &str,
    uri: &str,
    cid: &str,
    record: &Value,
    indexed_at: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
    pds: Option<&str>,
    live_now: &[crate::config::LiveNow],
) -> Option<Value> {
    let status = record.get("status")?.as_str()?;
    let external = record.get("embed").filter(|e| e.get("$type").and_then(Value::as_str) == Some("app.bsky.embed.external")).and_then(|e| e.get("external"));
    let link = external.and_then(|x| x.get("uri")).and_then(Value::as_str);
    if status == STATUS_LIVE && !live_now.is_empty() {
        let host = link.and_then(crate::firehose::extract::normalize_domain)?;
        let allowed = live_now.iter().filter(|l| l.did == did).flat_map(|l| l.domains.iter());
        let mut allowed = allowed.filter_map(|d| crate::firehose::extract::normalize_domain(d));
        if !allowed.any(|d| host == d || host.ends_with(&format!(".{d}"))) {
            return None;
        }
    }
    let mut view = json!({ "uri": uri, "cid": cid, "status": status, "record": record });
    if let Some(x) = external {
        let mut shown = json!({
            "uri": link.unwrap_or_default(),
            "title": x.get("title").and_then(Value::as_str).unwrap_or_default(),
            "description": x.get("description").and_then(Value::as_str).unwrap_or_default(),
        });
        if let Some(url) = x.get("thumb").and_then(crate::firehose::normalize::blob_cid).and_then(|c| blob_url(pds, did, &c)) {
            shown["thumb"] = json!(url);
        }
        view["embed"] = json!({ "$type": "app.bsky.embed.external#view", "external": shown });
    }
    if let Some(minutes) = record.get("durationMinutes").and_then(Value::as_i64) {
        let created = record
            .get("createdAt")
            .and_then(Value::as_str)
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map_or(indexed_at, |t| t.with_timezone(&chrono::Utc).min(indexed_at));
        let expires = created + chrono::TimeDelta::minutes(minutes.clamp(5, 240));
        view["expiresAt"] = json!(ts(expires));
        view["isActive"] = json!(now < expires);
    }
    Some(view)
}

/// The accounts a search may find: active, and in scope or a member.
pub(super) const IN_CORPUS: &str = "a.account_status = 'active' \
     AND (EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = a.did) OR EXISTS (SELECT 1 FROM members m WHERE m.did = a.did))";

/// What a signed-out search leaves out: an account whose profile asks not
/// to be shown to signed-out viewers.
pub(super) const SHOWN_SIGNED_OUT: &str =
    "NOT COALESCE(a.profile_record -> 'labels' -> 'values' @> '[{\"val\": \"!no-unauthenticated\"}]'::jsonb, false)";

/// The error of a search that passed its statement timeout, for the
/// methods that declare no error of their own (design-v3 §9.0).
pub(super) fn too_expensive() -> ViewError {
    ViewError::new("InvalidRequest", "query too expensive")
}

/// `q` with `LIKE`'s own characters escaped.
pub(super) fn like_escape(q: &str) -> String {
    q.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

/// The least string greater than every string that starts with `q`: `q`
/// with its last code point raised by one, past the surrogate range.
/// `None` when there is none to raise. UTF-8 byte order is code-point
/// order, so this bounds a byte-wise index range; raising the last byte
/// instead could leave text that is not UTF-8.
fn successor(q: &str) -> Option<String> {
    let mut chars: Vec<char> = q.chars().collect();
    let next = match u32::from(chars.pop()?) {
        0x0010_FFFF => return None,
        0xD7FF => 0xE000,
        n => n + 1,
    };
    chars.push(char::from_u32(next)?);
    Some(chars.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{LABELER, actor, exec, hydrator};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    fn sets() -> ScopeSets {
        let mut s = ScopeSets::default();
        for d in [ALICE, BOB] {
            s.scope.insert(d.into());
        }
        s.members.insert(ALICE.into());
        s
    }

    #[tokio::test]
    async fn viewer_state_is_empty_unless_the_viewer_is_a_ready_member() {
        let Some(db) = fresh_db("hydration_viewer_state").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin')").await;
        exec(p, "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/1', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', '3k', now())").await;
        exec(p, "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.graph.block/1', 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', '3k', now())").await;
        exec(p, "INSERT INTO labels (subject, val) VALUES ('did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', 'porn')").await;
        let h = hydrator(p, sets());
        let req = Requester::test();

        // Anonymous.
        let anon = h.get_profile(&Viewer::anonymous(), &req,"bob.test").await.expect("db").expect("view");
        assert_eq!(anon.inner()["viewer"], json!({"muted": false, "blockedBy": false}));
        assert_eq!(anon.inner()["handle"], "bob.test");
        assert_eq!(anon.inner()["labels"][0]["src"], LABELER);

        // A member who is not ready: exactly as anonymous.
        let unready = h.viewer(Some(ALICE)).await.expect("viewer");
        assert!(!unready.hydrate);
        let v = h.get_profile(&unready, &req, BOB).await.expect("db").expect("view");
        assert_eq!(v.inner()["viewer"], anon.inner()["viewer"]);

        // Ready: following and blockedBy populated.
        exec(p, "UPDATE members SET ready_at = now()").await;
        let ready = h.viewer(Some(ALICE)).await.expect("viewer");
        assert!(ready.hydrate);
        let v = h.get_profile(&ready, &req, BOB).await.expect("db").expect("view");
        assert_eq!(v.inner()["viewer"]["following"], "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/1");
        assert_eq!(v.inner()["viewer"]["blockedBy"], true);

        // A non-member caller is anonymous too.
        let stranger = h.viewer(Some("did:plc:cccccccccccccccccccccccc")).await.expect("viewer");
        assert!(!stranger.hydrate);
        db.drop_db().await;
    }

    /// design-v3 §11 G-1: `getProfiles` fetches out-of-scope actors upstream.
    /// B-77: an account that blocks the viewer through a list shows as
    /// `blockedBy`, but only on a request that carries a Farsight handle.
    #[tokio::test]
    async fn an_inbound_list_block_reads_as_blocked_by() {
        use std::sync::atomic::Ordering;

        use crate::farsight::lookup::tests::{V, engine};
        use crate::hydration::testing::hydrator_with_farsight;
        use crate::xrpc::backstop::{self, Selected};

        let Some(db) = fresh_db("hydration_inbound_list_block").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert(ALICE.to_string());
        let h = hydrator_with_farsight(p, sets(), farsight.clone());
        let me = Viewer {
            did: Some(V.into()),
            hydrate: true,
        };
        let req = Requester::test();
        let dids = [ALICE.to_string(), BOB.to_string()];

        // No handle (a private read, a background path): no call, no block.
        let ctx = h.profile_context(&me, &req, &dids, None).await.expect("ctx");
        assert_eq!(ctx.view(ALICE, ProfileKind::View)["viewer"]["blockedBy"], false);
        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);

        // With the handle a public handler attaches.
        let selected = Selected::by_ip("203.0.113.9".parse().expect("ip"));
        selected.attach_farsight(farsight.request(&req));
        backstop::scope(selected, async {
            let ctx = h.profile_context(&me, &req, &dids, None).await.expect("ctx");
            assert_eq!(ctx.view(ALICE, ProfileKind::View)["viewer"]["blockedBy"], true);
            assert_eq!(ctx.view(BOB, ProfileKind::View)["viewer"]["blockedBy"], false);
            assert!(ctx.viewer.blocked_either_way(ALICE));
            // A second hydration step of the request asks nothing new.
            h.profile_context(&me, &req, &dids, None).await.expect("ctx");
            assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
            // Anyone but a ready member has no viewer state, and no call.
            let ctx = h.profile_context(&Viewer::anonymous(), &req, &dids, None).await.expect("ctx");
            assert_eq!(ctx.view(ALICE, ProfileKind::View)["viewer"]["blockedBy"], false);
            assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        })
        .await;
        db.drop_db().await;
    }

    #[tokio::test]
    async fn get_profiles_fetches_out_of_scope_actors_upstream() {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        use crate::hydration::testing::{FixedUpstream, hydrator_with};

        let Some(db) = fresh_db("hydration_get_profiles_upstream").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        let (outsider, unknown, hidden) = (
            "did:plc:oooooooooooooooooooooooo",
            "did:plc:uuuuuuuuuuuuuuuuuuuuuuuu",
            "did:plc:hhhhhhhhhhhhhhhhhhhhhhhh",
        );
        exec(p, "INSERT INTO labels (subject, val) VALUES ('did:plc:hhhhhhhhhhhhhhhhhhhhhhhh', '!takedown')").await;
        let mut up = FixedUpstream::default();
        for did in [outsider, hidden] {
            up.profiles.insert(did.into(), json!({"did": did, "handle": "far.example", "displayName": "Far", "viewer": {"muted": true}, "labels": []}));
        }
        let up = Arc::new(up);
        let h = hydrator_with(p, sets(), up.clone());
        let req = Requester::test();
        let asked: Vec<String> = [outsider, ALICE, unknown, hidden].iter().map(|d| (*d).to_string()).collect();
        let out = h.get_profiles(&Viewer::anonymous(), &req, &asked).await.expect("db").expect("view");
        let profiles = out.inner()["profiles"].as_array().expect("profiles").clone();
        // Input order; the unresolved and the taken-down actor are omitted.
        let dids: Vec<&str> = profiles.iter().filter_map(|v| v["did"].as_str()).collect();
        assert_eq!(dids, [outsider, ALICE]);
        assert_eq!(profiles[0]["displayName"], "Far");
        // The upstream view is normalized: no upstream viewer state leaks.
        assert_ne!(profiles[0]["viewer"]["muted"], json!(true));
        assert_eq!(profiles[1]["handle"], "alice.test");
        // Only the two visible out-of-scope actors were asked for.
        assert_eq!(up.calls.load(Ordering::SeqCst), 2);
        db.drop_db().await;
    }

    #[tokio::test]
    async fn profiles_counts_gate_and_search() {
        let Some(db) = fresh_db("hydration_profiles").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        exec(p, "INSERT INTO actor_counts (did, followers_count, follows_count, posts_count) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 3, 2, 1)").await;
        let h = hydrator(p, sets());
        let req = Requester::test();
        let v = h.get_profile(&Viewer::anonymous(), &req,ALICE).await.expect("db").expect("view");
        let v = v.inner();
        assert_eq!((v["followersCount"].clone(), v["followsCount"].clone(), v["postsCount"].clone()), (json!(3), json!(2), json!(1)));
        assert_eq!(v["createdAt"], "2026-01-01T00:00:00Z");

        // Takedown: AccountTakedown, and omitted from getProfiles.
        exec(p, "INSERT INTO labels (subject, val) VALUES ('did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', '!takedown')").await;
        let e = h.get_profile(&Viewer::anonymous(), &req,BOB).await.expect("db").expect_err("hidden");
        assert_eq!(e.error, "AccountTakedown");
        let many = h
            .get_profiles(&Viewer::anonymous(), &req, &[ALICE.into(), BOB.into(), "nobody.test".into()])
            .await
            .expect("db")
            .expect("view");
        assert_eq!(many.inner()["profiles"].as_array().map(Vec::len), Some(1));

        // Unknown out-of-scope actor: uniform miss.
        let e = h
            .get_profile(&Viewer::anonymous(), &req, "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz")
            .await
            .expect("db")
            .expect_err("miss");
        assert_eq!(e.error, "InvalidRequest");

        // Search finds accounts in scope, and no others.
        let search = |q: &'static str| {
            let (h, req) = (h.clone(), req.clone());
            async move {
                let found = h.search_actors(&Viewer::anonymous(), &req, q, 10, None).await.expect("db").expect("view");
                found.inner()["actors"].as_array().expect("actors").iter().map(|a| a["did"].as_str().expect("did").to_string()).collect::<Vec<_>>()
            }
        };
        assert_eq!(search("ali").await, Vec::<String>::new());
        exec(p, "INSERT INTO scope_dids (did, reasons) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 2)").await;
        assert_eq!(search("ali").await, [ALICE]);
        db.drop_db().await;
    }

    /// B-32 (design-v3 §8.1): the detailed view carries pronouns, website,
    /// `joinedViaStarterPack`, a ready member's list and pack counts, the
    /// labeler flag, and, for a ready-member viewer, the followers of the
    /// subject that the viewer follows.
    #[tokio::test]
    async fn the_detailed_view_is_complete() {
        const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
        const DAVE: &str = "did:plc:dddddddddddddddddddddddd";
        const ERIN: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
        let Some(db) = fresh_db("hydration_detailed_view").await else {
            return;
        };
        let p = &db.pool;
        for (did, handle) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test"), (DAVE, "dave.test"), (ERIN, "erin.test")] {
            actor(p, did, handle, handle).await;
        }
        let pack = format!("at://{CAROL}/app.bsky.graph.starterpack/3kpack");
        exec(p, &format!(
            "UPDATE actors SET pronouns = 'they/them', website = 'https://bob.example', joined_via_starter_pack_uri = '{pack}' WHERE did = '{BOB}'"
        )).await;
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now()), ('{BOB}', 'admin', now())")).await;
        // Bob: two lists, a starter pack and a labeler. Carol made the pack
        // Bob joined through.
        for rkey in ["3kl1", "3kl2"] {
            exec(p, &format!(
                "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES \
                 ('at://{BOB}/app.bsky.graph.list/{rkey}', '{BOB}', '{rkey}', 'bafyl', '3k', 'L', 'app.bsky.graph.defs#curatelist', '{{}}', now())"
            )).await;
        }
        for (author, rkey) in [(BOB, "3kmine"), (CAROL, "3kpack")] {
            exec(p, &format!(
                "INSERT INTO starter_packs (uri, creator_did, rkey, cid, rev, name, list_uri, record, created_at) VALUES \
                 ('at://{author}/app.bsky.graph.starterpack/{rkey}', '{author}', '{rkey}', 'bafyp', '3k', 'P', \
                  'at://{author}/app.bsky.graph.list/3kl1', '{{\"name\": \"P\"}}', now())"
            )).await;
        }
        exec(p, &format!(
            "INSERT INTO labeler_services (uri, creator_did, cid, rev, policies, record, created_at) VALUES \
             ('at://{BOB}/app.bsky.labeler.service/self', '{BOB}', 'bafys', '3k', '{{}}', '{{}}', now())"
        )).await;
        // Alice follows Carol and Dave; Carol, Dave and Erin follow Bob.
        let mut n = 0;
        for (author, subject) in [(ALICE, CAROL), (ALICE, DAVE), (CAROL, BOB), (DAVE, BOB), (ERIN, BOB)] {
            n += 1;
            exec(p, &format!(
                "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{author}/app.bsky.graph.follow/{n}', '{author}', '{subject}', '3k', now())"
            )).await;
        }
        let mut sets = ScopeSets::default();
        for d in [ALICE, BOB, CAROL] {
            sets.scope.insert(d.into());
        }
        sets.members.insert(ALICE.into());
        sets.members.insert(BOB.into());
        let h = hydrator(p, sets);
        let req = Requester::test();
        let me = h.viewer(Some(ALICE)).await.expect("viewer");

        let v = h.get_profile(&me, &req, BOB).await.expect("db").expect("view");
        let v = v.inner();
        assert_eq!((v["pronouns"].clone(), v["website"].clone()), (json!("they/them"), json!("https://bob.example")));
        assert_eq!(v["joinedViaStarterPack"]["uri"], pack);
        assert_eq!(v["joinedViaStarterPack"]["creator"]["did"], CAROL);
        assert_eq!(
            v["associated"],
            json!({"lists": 2, "starterPacks": 1, "labeler": true, "activitySubscription": {"allowSubscriptions": "followers"}})
        );
        assert!(v.get("verification").is_none());
        assert_eq!(v["viewer"]["knownFollowers"]["count"], 2);
        let mut known: Vec<&str> = v["viewer"]["knownFollowers"]["followers"].as_array().expect("followers").iter().map(|f| f["did"].as_str().expect("did")).collect();
        known.sort_unstable();
        assert_eq!(known, [CAROL, DAVE]);

        // getProfiles: the same, in one batch. Carol is in scope but not a
        // member: no counts, and no labeler; nobody Alice follows follows
        // her. Anonymous viewers get no known followers.
        let many = h.get_profiles(&me, &req, &[BOB.into(), CAROL.into()]).await.expect("db").expect("view");
        let profiles = many.inner()["profiles"].as_array().expect("profiles").clone();
        assert_eq!(profiles[0]["associated"]["lists"], 2);
        assert_eq!(profiles[0]["viewer"]["knownFollowers"]["count"], 2);
        assert_eq!(profiles[1]["associated"], json!({"labeler": false, "activitySubscription": {"allowSubscriptions": "followers"}}));
        assert!(profiles[1]["viewer"].get("knownFollowers").is_none());
        let anon = h.get_profile(&Viewer::anonymous(), &req, BOB).await.expect("db").expect("view");
        assert!(anon.inner()["viewer"].get("knownFollowers").is_none());
        db.drop_db().await;
    }

    /// B-32 (design-v3 §8.1): a status is shown with its embed and its
    /// expiry; a live status needs the operator's leave when a list is set.
    #[test]
    fn a_status_view_follows_its_record_and_the_live_list() {
        let indexed = chrono::DateTime::parse_from_rfc3339("2026-10-12T10:00:00Z").expect("t").with_timezone(&chrono::Utc);
        let now = indexed + chrono::TimeDelta::minutes(30);
        let record = json!({
            "$type": "app.bsky.actor.status", "status": "app.bsky.actor.status#live",
            // Dated in the future: counted from when it was indexed.
            "createdAt": "2027-01-01T00:00:00Z", "durationMinutes": 100_000,
            "embed": {"$type": "app.bsky.embed.external", "external": {"uri": "https://www.twitch.tv/alice", "title": "Live", "description": "now"}},
        });
        let view = |live_now: &[crate::config::LiveNow]| status_view(ALICE, "at://a/app.bsky.actor.status/self", "bafys", &record, indexed, now, None, live_now);
        let shown = view(&[]).expect("no list: shown");
        assert_eq!(shown["embed"]["external"]["uri"], "https://www.twitch.tv/alice");
        // Clamped to four hours from the indexing time.
        assert_eq!((shown["expiresAt"].clone(), shown["isActive"].clone()), (json!("2026-10-12T14:00:00.000Z"), json!(true)));
        let list = |did: &str, domain: &str| vec![crate::config::LiveNow { did: did.to_string(), domains: vec![domain.to_string()] }];
        assert!(view(&list(ALICE, "twitch.tv")).is_some());
        assert!(view(&list(ALICE, "example.com")).is_none(), "another domain");
        assert!(view(&list(BOB, "twitch.tv")).is_none(), "another account");
        // A status that is not live is never gated, and has no expiry
        // unless it sets one.
        let away = json!({"status": "app.example.away", "createdAt": "2026-10-12T09:00:00Z"});
        let shown = status_view(ALICE, "u", "c", &away, indexed, now, None, &list(BOB, "twitch.tv")).expect("shown");
        assert!(shown.get("expiresAt").is_none() && shown.get("embed").is_none());
    }

    /// B-41 / #679 (design-v3 §9.3, §9.4): the corpus is scope and
    /// members; a member's mutes and blocks apply; typeahead matches
    /// prefixes and orders members, follows, the exact handle, then the
    /// shortest handle.
    #[tokio::test]
    async fn actor_search_and_typeahead_keep_to_the_corpus_and_the_viewers_blocks() {
        const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
        const DAVE: &str = "did:plc:dddddddddddddddddddddddd";
        const OUTSIDE: &str = "did:plc:oooooooooooooooooooooooo";
        let Some(db) = fresh_db("hydration_actor_search").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "al.test", "Bob Alston").await;
        actor(p, CAROL, "alfred.test", "Carol").await;
        actor(p, DAVE, "dave.test", "Dave").await;
        actor(p, OUTSIDE, "alien.test", "Alien").await;
        for did in [BOB, CAROL, DAVE] {
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)").bind(did).execute(p).await.expect("scope");
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
        let h = hydrator(p, sets());
        let req = Requester::test();
        let dids = |view: Value| view["actors"].as_array().expect("actors").iter().map(|a| a["did"].as_str().expect("did").to_string()).collect::<Vec<_>>();
        let anon = Viewer::anonymous();
        let me = Viewer { did: Some(DAVE.to_string()), hydrate: true };
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{DAVE}', 'admin', now())")).await;

        // Prefixes of the handle or of the display name; a member first,
        // then the shortest handle. The account outside the corpus is
        // never found.
        let found = h.search_actors_typeahead(&anon, &req, "@AL", 10).await.expect("db").expect("view");
        assert_eq!(dids(found.inner().clone()), [ALICE, BOB, CAROL]);
        // A word inside the display name, from three characters.
        let found = h.search_actors_typeahead(&anon, &req, "als", 10).await.expect("db").expect("view");
        assert_eq!(dids(found.inner().clone()), [BOB]);
        // `%` and `_` are characters, not wildcards.
        let found = h.search_actors_typeahead(&anon, &req, "a%", 10).await.expect("db").expect("view");
        assert_eq!(dids(found.inner().clone()), Vec::<String>::new());
        assert_eq!(successor("al").as_deref(), Some("am"));
        assert_eq!(successor("a\u{d7ff}").as_deref(), Some("a\u{e000}"));
        assert_eq!(successor("\u{10ffff}"), None);

        // An account the viewer follows comes before the others; one it
        // mutes or blocks is left out of both searches.
        exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{DAVE}/app.bsky.graph.follow/1', '{DAVE}', '{CAROL}', '3k', now())")).await;
        let found = h.search_actors_typeahead(&me, &req, "al", 10).await.expect("db").expect("view");
        assert_eq!(dids(found.inner().clone()), [ALICE, CAROL, BOB]);
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{DAVE}', '{BOB}')")).await;
        exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{ALICE}/app.bsky.graph.block/1', '{ALICE}', '{DAVE}', '3k', now())")).await;
        let found = h.search_actors_typeahead(&me, &req, "al", 10).await.expect("db").expect("view");
        assert_eq!(dids(found.inner().clone()), [CAROL]);
        let found = h.search_actors(&me, &req, "al", 10, None).await.expect("db").expect("view");
        assert_eq!(dids(found.inner().clone()), [CAROL]);
        // Signed out, an account that asks not to be shown is left out.
        exec(p, &format!("UPDATE actors SET profile_record = '{{\"labels\": {{\"values\": [{{\"val\": \"!no-unauthenticated\"}}]}}}}' WHERE did = '{CAROL}'")).await;
        let found = h.search_actors(&anon, &req, "alf", 10, None).await.expect("db").expect("view");
        assert_eq!(dids(found.inner().clone()), Vec::<String>::new());
        db.drop_db().await;
    }
}
