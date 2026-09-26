//! `app.bsky.*` handlers (design-v2 §7.1). Every handler returns
//! `Json<PublicEmit<_>>` (§7.2): it can hold the type but not construct it.

pub mod actor;
pub mod did_web;
pub mod feed;
pub mod graph;
pub mod index;
pub mod mutes;
pub mod notifications;
pub mod timeline;

use axum::Json;
use serde_json::Value;

use super::errors::XrpcError;
use crate::hydration::{PublicEmit, ViewResult};

/// A handler's response.
pub type XrpcResult = Result<Json<PublicEmit<Value>>, XrpcError>;

/// Map a hydration outcome to a response.
///
/// # Errors
///
/// A 400 for a lexicon error, a 500 for a hydration failure.
pub fn respond(result: ViewResult<Value>) -> XrpcResult {
    match result {
        Ok(Ok(view)) => Ok(Json(view)),
        Ok(Err(e)) => Err(e.into()),
        Err(e) => Err(e.into()),
    }
}
