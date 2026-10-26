//! The per-URI fetch ladder (design-v2 §10.4).
//!
//! 1. **Private-tier NSID?** `tier-hidden`, with no lookup of any kind:
//!    before the moderation gate, the database, the router and every
//!    upstream (§10.5).
//! 2. **Local.** The §7.10 gate (a hidden or `!hide` post is not shown and
//!    no source is consulted), `record_tombstones` (`deleted`), then local
//!    `posts`, hydrated for the viewer.
//! 3. **Home AppView, then the cascade**, via unauthenticated
//!    `app.bsky.feed.getPosts` (skipped for `#blockedPost` stubs, and for
//!    the `community` network while the public-indexing mode is On).
//! 4. **Slingshot** `getRecord`.
//! 5. **Classify** (§10.4 table, [`super::gaps::classify`]).
//!
//! Upstream and Slingshot views are normalized for the viewer (design-v3
//! §5.15.17); a view that normalization flags as content-stripped becomes
//! that gap. The sources themselves are still asked with no user identity:
//! none of them learns who is asking. Thread Lens calls the ladder only
//! for an author whose verdict is `Clear` (§5.15.10).

use std::collections::HashMap;

use futures::StreamExt;
use serde_json::Value;

use super::gaps::{CascadeOutcome, Gap, SlingshotOutcome, classify};
use crate::constellation::{Call, Cascade, Fetched, Router};
use crate::db::locks::uri_author;
use crate::hydration::{Hydrator, Viewer};
use crate::hydration::normalize::is_private_uri;
use crate::hydration::upstream::{ViewSource, post_from_record};
use crate::microcosm::{SlingshotClient, SlingshotError};
use crate::moderation::Gate;
use crate::xrpc::pools::Requester;

/// URIs one `getPosts` call takes.
pub const GET_POSTS_MAX: usize = 25;
/// `getPosts` calls of one batch in flight at once.
pub const CASCADE_CONCURRENCY: usize = 8;
/// Slingshot steps of one batch in flight at once.
pub const SLINGSHOT_CONCURRENCY: usize = 8;

/// Whether an at-uri's last segment is a valid record key.
fn rkey_valid(uri: &str) -> bool {
    uri.rsplit('/').next().is_some_and(proto_blue::syntax::RecordKey::is_valid)
}

/// Which ladder steps run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Steps 1–5 (`resolve`, `#notFoundPost`, replies, parents).
    Full,
    /// Steps 1, 2 and 4 (`#blockedPost` stubs, §10.4).
    BlockedStub,
}

/// A resolved URI.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolved {
    /// A `postView`, with the viewer's state, and the step it came from:
    /// `Local` (step 2) or `Cascade` (step 3, an upstream AppView's
    /// `getPosts` answer; threadlens-quoted-posts design §4).
    Post(Value, ViewSource),
    /// A `postView` built from the record alone (Slingshot): its author is
    /// `{did, handle}` from the identity lookup, until the author pass
    /// gives it a profile (design-v4 §8.1).
    RecordPost(Value),
    Gap(Gap),
}

/// The ladder's sources. Cheap to clone.
#[derive(Clone)]
pub struct Ladder {
    hydrator: Hydrator,
    router: Router,
    cascade: Cascade,
    slingshot: SlingshotClient,
}

impl Ladder {
    #[must_use]
    pub fn new(hydrator: Hydrator, router: Router, cascade: Cascade, slingshot: SlingshotClient) -> Self {
        Self {
            hydrator,
            router,
            cascade,
            slingshot,
        }
    }

    #[must_use]
    pub fn hydrator(&self) -> &Hydrator {
        &self.hydrator
    }

    #[must_use]
    pub fn router(&self) -> &Router {
        &self.router
    }

    #[must_use]
    pub fn cascade(&self) -> &Cascade {
        &self.cascade
    }

    /// Normalize an upstream or Slingshot view; content-stripped views
    /// become that gap.
    async fn upstream_view(&self, viewer: &Viewer, req: &Requester, uri: &str, view: Value) -> Result<Resolved, sqlx::Error> {
        let (view, report) = self.hydrator.lens_normalize(viewer, req, view).await?;
        Ok(if report.content_stripped.contains(uri) {
            Resolved::Gap(Gap::ContentStripped)
        } else {
            // Step 4 turns this into a `RecordPost`.
            Resolved::Post(view, ViewSource::Fallback)
        })
    }

    /// Run the ladder for one URI.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn resolve(&self, viewer: &Viewer, req: &Requester, uri: &str, mode: Mode) -> Result<Resolved, sqlx::Error> {
        let uris = [uri.to_string()];
        Ok(self.resolve_many(viewer, req, &uris, mode).await?.remove(uri).unwrap_or(Resolved::Gap(Gap::NotFound)))
    }

    /// Run the ladder for each of `uris` (threadlens-hydration design §2.3,
    /// F3). Every URI takes the steps it takes alone, in the same order,
    /// and gets the answer it would get alone; the steps' reads are made
    /// for all of them at once: one gate read, one tombstone read, one
    /// local read, one `getPosts` per home network for up to 25 posts, and
    /// one normalization of every upstream view. The Slingshot step runs
    /// per URI, concurrently.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn resolve_many(&self, viewer: &Viewer, req: &Requester, uris: &[String], mode: Mode) -> Result<HashMap<String, Resolved>, sqlx::Error> {
        let mut out: HashMap<String, Resolved> = HashMap::new();
        // 1. Private tier: nothing else runs.
        let mut left: Vec<String> = Vec::new();
        for uri in uris {
            if out.contains_key(uri) || left.contains(uri) {
                continue;
            }
            if is_private_uri(uri) {
                out.insert(uri.clone(), Resolved::Gap(Gap::TierHidden));
            } else if uri_author(uri).is_none() {
                out.insert(uri.clone(), Resolved::Gap(Gap::NotFound));
            } else {
                left.push(uri.clone());
            }
        }
        if left.is_empty() {
            return Ok(out);
        }
        // 2. The gate, tombstones, local posts.
        let gates = self.hydrator.gate_many(req, &left).await?;
        left.retain(|uri| {
            let visible = gates.get(uri).copied().unwrap_or(Gate::Visible) == Gate::Visible;
            if !visible {
                out.insert(uri.clone(), Resolved::Gap(Gap::NotFound));
            }
            visible
        });
        let tombstoned = self.hydrator.tombstoned_many(&left).await?;
        left.retain(|uri| {
            let gone = tombstoned.contains(uri);
            if gone {
                out.insert(uri.clone(), Resolved::Gap(Gap::Deleted));
            }
            !gone
        });
        if !left.is_empty() {
            let mut local = self.hydrator.lens_local_posts(viewer, req, &left).await?;
            left.retain(|uri| match local.remove(uri) {
                Some(view) => {
                    out.insert(uri.clone(), Resolved::Post(view, ViewSource::Local));
                    false
                }
                None => true,
            });
        }
        // 3. Home AppView and the cascade, one `getPosts` per home network
        // and 25 posts.
        let mut cascade: HashMap<String, CascadeOutcome> = HashMap::new();
        if mode == Mode::Full && !left.is_empty() {
            let mut groups: Vec<(Option<String>, Vec<String>)> = Vec::new();
            for uri in &left {
                let author = uri_author(uri).unwrap_or_default();
                let home = self.router.resolve_did(req, author).await;
                // C4 (public-indexing design §11): unless the mode is On, a
                // community author's posts are not stored, so the cascade is
                // asked for them too. A restored author's post missing here
                // takes the community path in every mode (refract design
                // §7.5).
                let restored = self.hydrator.refract_snapshot(req).await.is_restored(author);
                if !(restored || !home.is_community() || !self.hydrator.scope().public_mode().is_on()) {
                    continue;
                }
                cascade.insert(uri.clone(), CascadeOutcome::Skipped);
                // A record key an AppView may refuse goes alone, so it can
                // fail only its own `getPosts`.
                if !rkey_valid(uri) {
                    groups.push((home.network_name.clone(), vec![uri.clone()]));
                    continue;
                }
                match groups.iter_mut().find(|(network, group)| *network == home.network_name && group.len() < GET_POSTS_MAX && group.first().is_some_and(|u| rkey_valid(u))) {
                    Some((_, group)) => group.push(uri.clone()),
                    None => groups.push((home.network_name.clone(), vec![uri.clone()])),
                }
            }
            let answers: Vec<(Vec<String>, Fetched)> = futures::stream::iter(groups)
                .map(|(network, group)| async move {
                    let fetched = self.cascade.fetch(req, network.as_deref(), Call::Posts(&group)).await;
                    (group, fetched)
                })
                .buffer_unordered(CASCADE_CONCURRENCY)
                .collect()
                .await;
            let mut views: Vec<Value> = Vec::new();
            for (group, fetched) in answers {
                match fetched {
                    Fetched::Hit { body, .. } => {
                        let posts = body.get("posts").and_then(Value::as_array).cloned().unwrap_or_default();
                        for uri in group {
                            match posts.iter().find(|p| p.get("uri").and_then(Value::as_str) == Some(uri.as_str())) {
                                Some(view) => views.push(view.clone()),
                                // `getPosts` omits what it cannot show: a miss.
                                None => {
                                    cascade.insert(uri, CascadeOutcome::Answered);
                                }
                            }
                        }
                    }
                    exhausted if exhausted.all_transient() => group.into_iter().for_each(|uri| {
                        cascade.insert(uri, CascadeOutcome::AllTransient);
                    }),
                    Fetched::Exhausted(_) | Fetched::NoAnswer(_) => group.into_iter().for_each(|uri| {
                        cascade.insert(uri, CascadeOutcome::Answered);
                    }),
                }
            }
            if !views.is_empty() {
                let found: Vec<String> = views.iter().filter_map(|v| v.get("uri").and_then(Value::as_str).map(str::to_string)).collect();
                let (normalized, report) = self.hydrator.lens_normalize(viewer, req, Value::Array(views)).await?;
                // Fetched with the request's labelers, if it named any
                // (threadlens-hydration design §3.6 (b)).
                if let Some(scope) = crate::appview_clients::accept_labelers::current() {
                    scope.mark_labelled(report.labelled.iter().cloned());
                }
                for (uri, view) in found.into_iter().zip(normalized.as_array().cloned().unwrap_or_default()) {
                    let resolved = if report.content_stripped.contains(&uri) { Resolved::Gap(Gap::ContentStripped) } else { Resolved::Post(view, ViewSource::Cascade) };
                    cascade.remove(&uri);
                    out.insert(uri, resolved);
                }
            }
            left.retain(|uri| !out.contains_key(uri));
        }
        // 4. Slingshot and 5. classify, per URI.
        let rest: Vec<(String, Resolved)> = futures::stream::iter(left)
            .map(|uri| {
                let outcome = cascade.get(&uri).copied().unwrap_or(CascadeOutcome::Skipped);
                async move {
                    let resolved = self.slingshot_step(viewer, req, &uri, outcome).await;
                    resolved.map(|r| (uri, r))
                }
            })
            .buffered(SLINGSHOT_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        out.extend(rest);
        Ok(out)
    }

    /// Steps 4 and 5 for one URI the earlier steps did not settle.
    async fn slingshot_step(&self, viewer: &Viewer, req: &Requester, uri: &str, cascade: CascadeOutcome) -> Result<Resolved, sqlx::Error> {
        let author = uri_author(uri).unwrap_or_default().to_string();
        let slingshot = match self.slingshot.get_record(req, uri).await {
            Ok(record) => {
                let handle = self
                    .router
                    .lookup()
                    .summary(req, &author)
                    .await
                    .and_then(|s| s.handle)
                    .unwrap_or_else(|| crate::hydration::INVALID_HANDLE.to_string());
                let view = post_from_record(uri, record.cid.as_deref(), &handle, &record.value);
                return Ok(match self.upstream_view(viewer, req, uri, view).await? {
                    Resolved::Post(view, _) => Resolved::RecordPost(view),
                    other => other,
                });
            }
            Err(SlingshotError::NotFound) => SlingshotOutcome::NotFound,
            Err(SlingshotError::Transient(_) | SlingshotError::Invalid(_) | SlingshotError::NoBudget) => {
                SlingshotOutcome::Unavailable
            }
        };
        let active = self.hydrator.account_known_active(&author).await?;
        Ok(Resolved::Gap(classify(false, cascade, slingshot, active)))
    }
}
