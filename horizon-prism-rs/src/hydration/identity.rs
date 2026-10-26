//! `com.atproto.identity.resolveHandle` (design-v3 §5.11).
//!
//! 1. The handle's syntax is checked before anything is looked up.
//! 2. A handle stored here for an account this AppView indexes (in scope,
//!    or a member) is answered from that row. No other stored row is
//!    used, so the quick answer cannot show that an account outside the
//!    indexed set has been seen here.
//! 3. Otherwise the handle is resolved on the network, and "no such
//!    handle" (`HandleNotFound`) is told apart from "could not find out"
//!    (`502 UpstreamFailure`).

use proto_blue::syntax::Handle;
use serde_json::{Value, json};

use super::{HydrationError, Hydrator, ViewError, ViewResult};
use crate::identity::handle_lookup::HandleOutcome;
use crate::xrpc::pools::Requester;

const METRIC_HANDLE_RESOLUTIONS: &str = crate::constellation::routing::METRIC_HANDLE_RESOLUTIONS;

impl Hydrator {
    /// `com.atproto.identity.resolveHandle`.
    ///
    /// # Errors
    ///
    /// Database errors; no budget for the lookup; the lookup could not be
    /// completed.
    pub async fn resolve_handle(&self, req: &Requester, handle: &str) -> ViewResult<Value> {
        let handle = handle.trim().trim_start_matches('@').to_ascii_lowercase();
        if !Handle::is_valid(&handle) {
            return Ok(Err(ViewError::new("InvalidRequest", "Error: handle must be a valid handle")));
        }
        let stored: Option<String> = sqlx::query_scalar("SELECT did FROM actors WHERE handle = $1")
            .bind(&handle)
            .fetch_optional(self.pool())
            .await?;
        if let Some(did) = stored {
            let indexed = {
                let scope = self.scope().read_guard().await;
                scope.in_scope(&did) || scope.is_member(&did)
            };
            if indexed {
                metrics::counter!(METRIC_HANDLE_RESOLUTIONS, "intent" => "public", "result" => "local").increment(1);
                return Ok(Ok(Self::emit(json!({ "did": did }))));
            }
        }
        match self.upstream().handle_outcome(req, &handle).await {
            HandleOutcome::Found(did) => Ok(Ok(Self::emit(json!({ "did": did })))),
            HandleOutcome::NotFound => Ok(Err(ViewError::new("HandleNotFound", "Unable to resolve handle"))),
            HandleOutcome::Unavailable => match req.exhausted() {
                Some(exhausted) => Err(HydrationError::NoBudget(exhausted)),
                None => Err(HydrationError::Upstream),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, hydrator_with};
    use crate::hydration::upstream::UpstreamSource;
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const FAR: &str = "did:plc:ffffffffffffffffffffffff";

    /// An upstream whose handle answers are fixed by name.
    struct Handles;

    #[async_trait]
    impl UpstreamSource for Handles {
        async fn profile(&self, _req: &Requester, _actor: &str) -> Option<Value> {
            None
        }
        async fn posts(&self, _req: &Requester, _uris: &[String]) -> Vec<Value> {
            Vec::new()
        }
        async fn post_thread(&self, _req: &Requester, _uri: &str, _depth: u32, _parent_height: u32) -> Option<(Value, crate::hydration::upstream::ViewSource)> {
            None
        }
        async fn author_feed(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>, _filter: &str) -> Option<Value> {
            None
        }
        async fn follows(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
            None
        }
        async fn handle_outcome(&self, _req: &Requester, handle: &str) -> HandleOutcome {
            match handle {
                "far.example" | "bob.test" => HandleOutcome::Found(FAR.into()),
                "down.example" => HandleOutcome::Unavailable,
                _ => HandleOutcome::NotFound,
            }
        }
    }

    /// B-62: the three answers, and the local row only for indexed accounts.
    #[tokio::test]
    async fn a_handle_resolves_locally_on_the_network_or_not_at_all() {
        let Some(db) = fresh_db("hydration_resolve_handle").await else {
            return;
        };
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        let h = hydrator_with(p, sets, Arc::new(Handles));
        let req = Requester::test();
        let did = |r: ViewResult<Value>| r.expect("db").expect("view").inner()["did"].clone();

        // An indexed account's stored handle, whatever its case.
        assert_eq!(did(h.resolve_handle(&req, "@Alice.Test").await), ALICE);
        // A stored row of an account that is not indexed is not used: the
        // network's answer is.
        assert_eq!(did(h.resolve_handle(&req, "bob.test").await), FAR);
        assert_eq!(did(h.resolve_handle(&req, "far.example").await), FAR);
        // Bad syntax is refused before any lookup; no such handle; and a
        // lookup that could not be completed.
        let invalid = h.resolve_handle(&req, "not a handle").await.expect("db").expect_err("invalid");
        assert_eq!(invalid.error, "InvalidRequest");
        let missing = h.resolve_handle(&req, "nobody.example").await.expect("db").expect_err("missing");
        assert_eq!(missing.error, "HandleNotFound");
        assert!(matches!(h.resolve_handle(&req, "down.example").await, Err(HydrationError::Upstream)));
        // The local answer is a public resolution (§14.2, R3-155).
        let text = handle.render();
        let local = crate::test_support::series(&text, METRIC_HANDLE_RESOLUTIONS, &["intent=\"public\"", "result=\"local\""]);
        assert_eq!(local, Some(1.0), "{text}");
        db.drop_db().await;
    }
}
