//! `GET /.well-known/did.json` and `GET /did.json` (design-v2 §6.8,
//! design-v3 §6.4): the AppView's did:web document with its secp256k1
//! Multikey and the `#bsky_appview` and `#bsky_notif` services. Both paths
//! serve the same bytes. `Access-Control-Allow-Origin: *` comes from the
//! final router's CORS layer (§6.5).

use axum::Json;
use axum::extract::State;
use axum::http::HeaderValue;
use axum::http::header::CACHE_CONTROL;
use axum::response::{IntoResponse, Response};

use crate::xrpc::AppState;

/// `cache-control` on both paths (§6.4).
pub const CACHE: &str = "public, max-age=300";

pub async fn did_json(State(s): State<AppState>) -> Response {
    let mut response = Json(s.did_document.as_ref()).into_response();
    response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static(CACHE));
    response
}
