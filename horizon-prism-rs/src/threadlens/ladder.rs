//! The per-URI fetch ladder (design-v2 §10.4).
//!
//! 1. **Private-tier NSID?** `tier-hidden`, with no lookup of any kind:
//!    before the moderation gate, the database, the router and every
//!    upstream (§10.5).
//! 2. **Local.** The §7.10 gate (a hidden or `!hide` post is not shown and
//!    no source is consulted), `record_tombstones` (`deleted`), then local
//!    `posts`, hydrated with no viewer.
//! 3. **Home AppView, then the cascade**, via unauthenticated
//!    `app.bsky.feed.getPosts` (skipped for the `community` network and for
//!    `#blockedPost` stubs).
//! 4. **Slingshot** `getRecord`.
//! 5. **Classify** (§10.4 table, [`super::gaps::classify`]).
//!
//! Upstream and Slingshot views are normalized with no viewer; a view that
//! normalization flags as content-stripped becomes that gap. No step reads
//! the caller's identity: the ladder is not given one.

use serde_json::Value;

use super::gaps::{CascadeOutcome, Gap, SlingshotOutcome, classify};
use crate::constellation::{Call, Cascade, Fetched, Router};
use crate::db::locks::uri_author;
use crate::hydration::Hydrator;
use crate::hydration::normalize::is_private_uri;
use crate::hydration::upstream::post_from_record;
use crate::microcosm::{SlingshotClient, SlingshotError};
use crate::xrpc::pools::Requester;

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
    /// A `postView` with viewer state `{}`.
    Post(Value),
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
    async fn upstream_view(&self, uri: &str, view: Value) -> Result<Resolved, sqlx::Error> {
        let (view, report) = self.hydrator.lens_normalize(view).await?;
        Ok(if report.content_stripped.contains(uri) {
            Resolved::Gap(Gap::ContentStripped)
        } else {
            Resolved::Post(view)
        })
    }

    /// Run the ladder for one URI.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn resolve(&self, req: &Requester, uri: &str, mode: Mode) -> Result<Resolved, sqlx::Error> {
        // 1. Private tier: nothing else runs.
        if is_private_uri(uri) {
            return Ok(Resolved::Gap(Gap::TierHidden));
        }
        let Some(author) = uri_author(uri).map(str::to_string) else {
            return Ok(Resolved::Gap(Gap::NotFound));
        };
        // 2. The gate, tombstones, local posts.
        if !self.hydrator.lens_visible(uri).await? {
            return Ok(Resolved::Gap(Gap::NotFound));
        }
        if self.hydrator.tombstoned(uri).await? {
            return Ok(Resolved::Gap(Gap::Deleted));
        }
        if let Some(view) = self.hydrator.lens_local_posts(req, &[uri.to_string()]).await?.remove(uri) {
            return Ok(Resolved::Post(view));
        }
        // 3. Home AppView and the cascade.
        let mut cascade = CascadeOutcome::Skipped;
        if mode == Mode::Full {
            let home = self.router.resolve_did(req, &author).await;
            if !home.is_community() {
                let uris = [uri.to_string()];
                match self.cascade.fetch(req, home.network_name.as_deref(), Call::Posts(&uris)).await {
                    Fetched::Hit { body, .. } => {
                        let found = body
                            .get("posts")
                            .and_then(Value::as_array)
                            .and_then(|posts| posts.iter().find(|p| p.get("uri").and_then(Value::as_str) == Some(uri)))
                            .cloned();
                        if let Some(view) = found {
                            return self.upstream_view(uri, view).await;
                        }
                        // `getPosts` omits what it cannot show: a miss.
                        cascade = CascadeOutcome::Answered;
                    }
                    exhausted if exhausted.all_transient() => cascade = CascadeOutcome::AllTransient,
                    Fetched::Exhausted(_) => cascade = CascadeOutcome::Answered,
                }
            }
        }
        // 4. Slingshot.
        let slingshot = match self.slingshot.get_record(uri).await {
            Ok(record) => {
                let handle = self
                    .router
                    .lookup()
                    .summary(req, &author)
                    .await
                    .and_then(|s| s.handle)
                    .unwrap_or_else(|| crate::hydration::INVALID_HANDLE.to_string());
                let view = post_from_record(uri, record.cid.as_deref(), &handle, &record.value);
                return self.upstream_view(uri, view).await;
            }
            Err(SlingshotError::NotFound) => SlingshotOutcome::NotFound,
            Err(SlingshotError::Transient(_) | SlingshotError::Invalid(_)) => SlingshotOutcome::Unavailable,
        };
        // 5. Classify.
        let active = self.hydrator.account_known_active(&author).await?;
        Ok(Resolved::Gap(classify(false, cascade, slingshot, active)))
    }
}
