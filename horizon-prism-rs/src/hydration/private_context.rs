//! Private-context hydration (design-v4 §8.1 "Private-context hydration";
//! R3-179, B-54): how the private handlers (`src/private_tier/handlers.rs`)
//! render the public parts of a private view, meaning authors,
//! `publicParent`, a quoted public post and the labels stored for a post.
//!
//! [`HydrationContext::Private`] is the private handlers' only door to the
//! [`Hydrator`]. It is a distinct type, so they cannot reach a public entry
//! point by mistake, and every call it makes runs with a request in
//! private-context mode ([`Requester::private_context`]):
//! - authors render from local `actors` rows whatever the DID's scope
//!   class (the viewer is already authorized for the record), otherwise in
//!   the minimal form;
//! - public posts render from local rows, with counts read as stored and
//!   nothing put on `cc:refresh`;
//! - no cascade, Slingshot or Constellation call is made, and neither `rt`,
//!   the `route` cache nor any identity cache is read or written; a list
//!   behind `mutedByList` or `blockingByList` that is not locally servable
//!   is left out, as one that cannot be read always is;
//! - Farsight is never asked: the private gate attaches no handle to the
//!   request (`xrpc::auth::prepare_private`), so `actor_viewer` makes no
//!   lookup and reads no Farsight cache, fallback entry or memory.
//!
//! So the private pipeline leaves no trace in any state a public request
//! reads.

use std::collections::HashMap;

use serde_json::Value;

use super::actor::ProfileContext;
use super::{Hydrator, PublicEmit, Viewer};
use crate::db::locks::uri_author;
use crate::xrpc::pools::Requester;

/// A hydration context. Private handlers hold the one variant there is.
#[derive(Clone)]
pub enum HydrationContext {
    /// Private-context hydration for `viewer`, a private-tier caller.
    Private {
        hydrator: Hydrator,
        viewer: Viewer,
        /// Always in private-context mode.
        req: Requester,
    },
}

impl HydrationContext {
    /// Private-context hydration for `viewer`, charged to `req`.
    #[must_use]
    pub fn private(hydrator: &Hydrator, viewer: Viewer, req: &Requester) -> Self {
        let req = req.private_context(viewer.did.as_deref().unwrap_or_default());
        Self::Private {
            hydrator: hydrator.clone(),
            viewer,
            req,
        }
    }

    /// Profiles for `dids`: local rows, otherwise the minimal form.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn profiles(&self, dids: &[String]) -> Result<ProfileContext, sqlx::Error> {
        let Self::Private { hydrator, viewer, req } = self;
        hydrator.profile_context(viewer, req, dids, None).await
    }

    /// One public `postView` for a private thread's `publicParent`: stored
    /// and not hidden, else `None`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn public_post(&self, uri: &str) -> Result<Option<PublicEmit<Value>>, sqlx::Error> {
        let Self::Private { hydrator, viewer, req } = self;
        hydrator.public_post_view(viewer, req, uri).await
    }

    /// The record view of a public post that the private post `quoting`
    /// embeds (private-posting design §4.2): local rows only, in On mode
    /// only, else `None`. Nothing is fetched upstream, so the read leaves no
    /// trace in a shared cache.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn public_record_view(&self, uri: &str, quoting: &str) -> Result<Option<PublicEmit<Value>>, sqlx::Error> {
        let Self::Private { hydrator, viewer, req } = self;
        hydrator.public_record_view(viewer, req, uri, quoting).await
    }

    /// The `lists` of a private root's `threadgateView` (private-posting
    /// design §4.4): the lists its `listRule`s name, as `listViewBasic`,
    /// from local rows only. A list that is not locally servable (owned by a
    /// ready member) is left out, never fetched.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn threadgate_lists(&self, allow: Option<&Value>) -> Result<Vec<Value>, sqlx::Error> {
        let Self::Private { hydrator, viewer, req } = self;
        hydrator.threadgate_lists(viewer, req, allow).await
    }

    /// A threadgate `listRule`'s input for `did` (§4.4): whether `did` is an
    /// item of `list` when the list is locally servable, else `None` (the
    /// input is incomplete).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn list_rule_allows(&self, list: &str, did: &str) -> Result<Option<bool>, sqlx::Error> {
        let Self::Private { hydrator, .. } = self;
        if !super::lists::is_list_uri(list) || !hydrator.is_ready_member(uri_author(list).unwrap_or_default()).await? {
            return Ok(None);
        }
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM list_items WHERE list_uri = $1 AND subject_did = $2)")
            .bind(list)
            .bind(did)
            .fetch_one(hydrator.pool())
            .await
            .map(Some)
    }

    /// The labels stored for `subjects` (the `labels` table), by subject, in
    /// the shape public views carry them (design-v4 §II-7.8).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn labels(&self, subjects: &[String]) -> Result<HashMap<String, Vec<Value>>, sqlx::Error> {
        let Self::Private { hydrator, .. } = self;
        hydrator.load_labels(subjects).await
    }
}
