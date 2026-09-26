//! Actor views (design-v2 §7.1): `getProfile`, `getProfiles`,
//! `searchActors`, and the `profileView*` builders every other view uses.

use std::collections::HashMap;
use std::time::Instant;

use serde_json::{Map, Value, json};

use super::{ActorRow, ActorViewer, CountMode, Hydrator, INVALID_HANDLE, ViewError, ViewResult, Viewer, blob_url, ts};
use crate::moderation::{self, Gate};

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
            }
        }
        v.insert("viewer".into(), self.viewer.view(did));
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
        dids: &[String],
        followers: Option<CountMode>,
    ) -> Result<ProfileContext, sqlx::Error> {
        let rows = self.load_actors(dids).await?;
        let actor_viewer = self.actor_viewer(viewer, dids).await?;
        let labels = self.load_labels(dids).await?;
        let followers = match followers {
            Some(mode) => self
                .constellation_counts(dids, mode)
                .await
                .into_iter()
                .filter_map(|(d, c)| c.followers.map(|f| (d, f)))
                .collect(),
            None => HashMap::new(),
        };
        Ok(ProfileContext {
            rows,
            viewer: actor_viewer,
            labels,
            followers,
        })
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
        })
    }

    /// `app.bsky.actor.getProfile`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_profile(&self, viewer: &Viewer, actor: &str) -> ViewResult<Value> {
        let started = Instant::now();
        let Some(did) = self.resolve_actor(actor).await? else {
            return Ok(Err(ViewError::new("InvalidRequest", "Profile not found")));
        };
        let gate = moderation::gate(self.pool(), &did).await?;
        if let Some((error, message)) = gate.profile_error() {
            return Ok(Err(ViewError::new(error, message)));
        }
        let ctx = self
            .profile_context(viewer, std::slice::from_ref(&did), Some(CountMode::Sync { started }))
            .await?;
        if !ctx.rows.contains_key(&did) && !self.in_scope(&did).await {
            // Out-of-scope subject: the §8.3 cascade, then Slingshot.
            return Ok(match self.upstream().profile(&did).await {
                Some(v) => Ok(Self::emit(self.normalized(viewer, v).await?.0)),
                None => Err(ViewError::new("InvalidRequest", "Profile not found")),
            });
        }
        Ok(Ok(Self::emit(ctx.view(&did, ProfileKind::Detailed))))
    }

    /// `app.bsky.actor.getProfiles` (≤ 25): unknown or hidden actors are
    /// omitted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_profiles(&self, viewer: &Viewer, actors: &[String]) -> ViewResult<Value> {
        let started = Instant::now();
        let mut dids: Vec<String> = Vec::new();
        for a in actors.iter().take(25) {
            if let Some(d) = self.resolve_actor(a).await?
                && !dids.contains(&d)
            {
                dids.push(d);
            }
        }
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        dids.retain(|d| !gates.get(d).copied().unwrap_or(Gate::Visible).is_hidden());
        let ctx = self.profile_context(viewer, &dids, Some(CountMode::Sync { started })).await?;
        let profiles: Vec<Value> = dids
            .iter()
            .filter(|d| ctx.rows.contains_key(*d))
            .map(|d| ctx.view(d, ProfileKind::Detailed))
            .collect();
        Ok(Ok(Self::emit(json!({ "profiles": profiles }))))
    }

    /// `app.bsky.actor.searchActors`: trigram match over indexed actors'
    /// handles and display names (§7.1). The cursor is an offset.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn search_actors(&self, viewer: &Viewer, q: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let q = q.trim().trim_start_matches('@').to_lowercase();
        if q.is_empty() {
            return Ok(Ok(Self::emit(json!({ "actors": [] }))));
        }
        let offset: i64 = cursor.and_then(|c| c.parse().ok()).unwrap_or(0).max(0);
        let limit = i64::from(limit.clamp(1, 100));
        let pattern = format!("%{}%", q.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"));
        let dids: Vec<String> = sqlx::query_scalar(
            "SELECT did FROM actors \
             WHERE account_status = 'active' AND (handle ILIKE $1 OR display_name ILIKE $1) \
             ORDER BY GREATEST(similarity(coalesce(handle, ''), $2), similarity(coalesce(display_name, ''), $2)) DESC, did \
             LIMIT $3 OFFSET $4",
        )
        .bind(&pattern)
        .bind(&q)
        .bind(limit)
        .bind(offset)
        .fetch_all(self.pool())
        .await?;
        let gates = moderation::gate_many(self.pool(), &dids).await?;
        let visible: Vec<String> = dids
            .iter()
            .filter(|d| !gates.get(*d).copied().unwrap_or(Gate::Visible).is_hidden())
            .cloned()
            .collect();
        let ctx = self.profile_context(viewer, &visible, None).await?;
        let actors: Vec<Value> = visible.iter().map(|d| ctx.view(d, ProfileKind::View)).collect();
        let mut out = json!({ "actors": actors });
        if i64::try_from(dids.len()).unwrap_or(0) == limit {
            out["cursor"] = json!((offset + limit).to_string());
        }
        Ok(Ok(Self::emit(out)))
    }
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

        // Anonymous.
        let anon = h.get_profile(&Viewer::anonymous(), "bob.test").await.expect("db").expect("view");
        assert_eq!(anon.inner()["viewer"], json!({"muted": false, "blockedBy": false}));
        assert_eq!(anon.inner()["handle"], "bob.test");
        assert_eq!(anon.inner()["labels"][0]["src"], LABELER);

        // A member who is not ready: exactly as anonymous.
        let unready = h.viewer(Some(ALICE)).await.expect("viewer");
        assert!(!unready.hydrate);
        let v = h.get_profile(&unready, BOB).await.expect("db").expect("view");
        assert_eq!(v.inner()["viewer"], anon.inner()["viewer"]);

        // Ready: following and blockedBy populated.
        exec(p, "UPDATE members SET ready_at = now()").await;
        let ready = h.viewer(Some(ALICE)).await.expect("viewer");
        assert!(ready.hydrate);
        let v = h.get_profile(&ready, BOB).await.expect("db").expect("view");
        assert_eq!(v.inner()["viewer"]["following"], "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/1");
        assert_eq!(v.inner()["viewer"]["blockedBy"], true);

        // A non-member caller is anonymous too.
        let stranger = h.viewer(Some("did:plc:cccccccccccccccccccccccc")).await.expect("viewer");
        assert!(!stranger.hydrate);
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
        let v = h.get_profile(&Viewer::anonymous(), ALICE).await.expect("db").expect("view");
        let v = v.inner();
        assert_eq!((v["followersCount"].clone(), v["followsCount"].clone(), v["postsCount"].clone()), (json!(3), json!(2), json!(1)));
        assert_eq!(v["createdAt"], "2026-01-01T00:00:00Z");

        // Takedown: AccountTakedown, and omitted from getProfiles.
        exec(p, "INSERT INTO labels (subject, val) VALUES ('did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', '!takedown')").await;
        let e = h.get_profile(&Viewer::anonymous(), BOB).await.expect("db").expect_err("hidden");
        assert_eq!(e.error, "AccountTakedown");
        let many = h
            .get_profiles(&Viewer::anonymous(), &[ALICE.into(), BOB.into(), "nobody.test".into()])
            .await
            .expect("db")
            .expect("view");
        assert_eq!(many.inner()["profiles"].as_array().map(Vec::len), Some(1));

        // Unknown out-of-scope actor: uniform miss.
        let e = h
            .get_profile(&Viewer::anonymous(), "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz")
            .await
            .expect("db")
            .expect_err("miss");
        assert_eq!(e.error, "InvalidRequest");

        let found = h.search_actors(&Viewer::anonymous(), "ali", 10, None).await.expect("db").expect("view");
        assert_eq!(found.inner()["actors"][0]["did"], ALICE);
        db.drop_db().await;
    }
}
