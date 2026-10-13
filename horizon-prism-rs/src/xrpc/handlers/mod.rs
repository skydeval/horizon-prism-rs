//! `app.bsky.*` handlers (design-v2 §7.1). Every handler returns
//! `Json<PublicEmit<_>>` (§7.2): it can hold the type but not construct it.

pub mod actor;
pub mod bookmarks;
pub mod did_web;
pub mod drafts;
pub mod feed;
pub mod graph;
pub mod index;
pub mod labels;
pub mod mutes;
pub mod notifications;
pub mod repo;
pub mod search;
pub mod timeline;
pub mod unspecced;

use axum::Json;
use serde_json::Value;

use super::errors::XrpcError;
use super::pools::Requester;
use crate::hydration::{PublicEmit, ViewResult};

/// A handler's response.
pub type XrpcResult = Result<Json<PublicEmit<Value>>, XrpcError>;

/// Map a hydration outcome to a response.
///
/// A lexicon error from a request whose upstream budget ran out is a `429`
/// instead (design-v3 §II-8.3): the subject was not looked up everywhere,
/// so "not found" would be a guess.
///
/// # Errors
///
/// A 400 for a lexicon error, a 429 for an exhausted upstream budget, a 500
/// for a hydration failure.
pub fn respond(req: &Requester, result: ViewResult<Value>) -> XrpcResult {
    match result {
        Ok(Ok(view)) => Ok(Json(view)),
        Ok(Err(e)) => Err(req.exhausted().map_or_else(|| e.into(), Into::into)),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::*;
    use crate::hydration::{HydrationError, ViewError};
    use crate::xrpc::pools::Shares;

    fn failed(r: XrpcResult) -> axum::response::Response {
        let Err(e) = r else {
            panic!("expected an error");
        };
        e.into_response()
    }

    fn status(r: XrpcResult) -> StatusCode {
        failed(r).status()
    }

    #[test]
    fn a_lexicon_error_after_exhaustion_is_a_429() {
        let not_found = || Ok(Err(ViewError::new("InvalidRequest", "Profile not found")));
        let req = Requester::test();
        assert_eq!(status(respond(&req, not_found())), StatusCode::BAD_REQUEST);

        // 1/s, burst 2: one token in the non-member pool.
        let bucket = Shares::for_tests().bucket("network", 1.0, 2);
        assert!(req.draw(&bucket));
        assert!(!req.draw(&bucket));
        let response = failed(respond(&req, not_found()));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(response.headers().contains_key("ratelimit-reset"));

        // A listing with no budget is a 429 whoever asks.
        let exhausted = req.exhausted().expect("recorded");
        assert_eq!(
            status(respond(&Requester::test(), Err(HydrationError::NoBudget(exhausted)))),
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}
