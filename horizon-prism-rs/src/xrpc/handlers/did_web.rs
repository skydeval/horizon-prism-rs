//! `GET /.well-known/did.json` (design-v2 §6.8): the AppView's did:web
//! document with its secp256k1 Multikey and the `#bsky_appview` service.

use axum::Json;
use axum::extract::State;
use serde_json::Value;

use crate::xrpc::AppState;

pub async fn did_json(State(s): State<AppState>) -> Json<Value> {
    Json(s.did_document.as_ref().clone())
}
