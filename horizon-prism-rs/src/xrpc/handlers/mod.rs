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
pub mod membership;
pub mod mutes;
pub mod notifications;
pub mod repo;
pub mod search;
pub mod timeline;
pub mod unspecced;

use axum::Json;
use axum::http::{HeaderName, HeaderValue};
use axum::response::AppendHeaders;
use serde_json::Value;

use super::errors::XrpcError;
use super::pools::Requester;
use crate::hydration::{PublicEmit, ViewResult};

/// A handler's response.
pub type XrpcResult = Result<Json<PublicEmit<Value>>, XrpcError>;

/// The response header naming the order a `sort` request was served in
/// (locked decision 30, design-v4 §5.2).
pub const SORT_FALLBACK: &str = "x-prism-sort-fallback";

/// A sortable listing's response: [`XrpcResult`] plus, when the caller
/// asked for an order other than `latest`, `X-Prism-Sort-Fallback: latest`.
pub type SortedResult = Result<(AppendHeaders<Option<(HeaderName, HeaderValue)>>, Json<PublicEmit<Value>>), XrpcError>;

/// [`respond`] for `getFollows`, `getFollowers` and `getQuotes`: every
/// source is read in `latest` order, so any other `sort` (`top`, or an
/// unknown value) is served as `latest` and says so (locked decision 30).
///
/// # Errors
///
/// As [`respond`].
pub fn respond_sorted(req: &Requester, sort: Option<&str>, result: ViewResult<Value>) -> SortedResult {
    let fallback = sort
        .filter(|s| *s != "latest")
        .map(|_| (HeaderName::from_static(SORT_FALLBACK), HeaderValue::from_static("latest")));
    respond(req, result).map(|json| (AppendHeaders(fallback), json))
}

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

    /// B-51 (locked decision 30): `sort=top`, or any value but `latest`,
    /// is served as `latest` and says so; `latest` and no `sort` say
    /// nothing.
    #[test]
    fn a_sort_other_than_latest_falls_back_and_says_so() {
        let req = Requester::test();
        let ok = || Ok(Ok(crate::hydration::testing::emitted(serde_json::json!({"followers": []}))));
        for (sort, header) in [(Some("top"), Some("latest")), (Some("bogus"), Some("latest")), (Some("latest"), None), (None, None)] {
            let response = respond_sorted(&req, sort, ok()).expect("served").into_response();
            assert_eq!(response.status(), StatusCode::OK, "{sort:?}");
            assert_eq!(response.headers().get(SORT_FALLBACK).map(|v| v.to_str().expect("ascii")), header, "{sort:?}");
        }
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
