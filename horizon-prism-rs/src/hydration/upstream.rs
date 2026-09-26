//! The upstream seam (design-v2 §13 row 182). Out-of-scope subjects are
//! served by the §8 routing cascade and Slingshot, which item 7 builds. Until
//! then [`NoUpstream`] answers every lookup with nothing, which every caller
//! turns into that endpoint's uniform miss.
//!
//! Whatever an implementation returns is raw upstream JSON: callers pass it
//! through [`super::normalize::normalize_upstream`] and never write it to
//! content tables (§7.1, §7.6).

use async_trait::async_trait;
use serde_json::Value;

/// Upstream lookups for subjects this AppView does not index.
#[async_trait]
pub trait UpstreamSource: Send + Sync {
    /// `app.bsky.actor.getProfile` for a DID or handle.
    async fn profile(&self, actor: &str) -> Option<Value>;
    /// Post views for URIs (`getPosts` / Slingshot).
    async fn posts(&self, uris: &[String]) -> Vec<Value>;
    /// `app.bsky.feed.getPostThread`.
    async fn post_thread(&self, uri: &str, depth: u32, parent_height: u32) -> Option<Value>;
    /// `app.bsky.feed.getAuthorFeed` output.
    async fn author_feed(&self, actor: &str, limit: u32, cursor: Option<&str>, filter: &str) -> Option<Value>;
    /// `app.bsky.graph.getFollows` output.
    async fn follows(&self, actor: &str, limit: u32, cursor: Option<&str>) -> Option<Value>;
}

/// Item-5 wiring: no upstream.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoUpstream;

#[async_trait]
impl UpstreamSource for NoUpstream {
    async fn profile(&self, _actor: &str) -> Option<Value> {
        None
    }
    async fn posts(&self, _uris: &[String]) -> Vec<Value> {
        Vec::new()
    }
    async fn post_thread(&self, _uri: &str, _depth: u32, _parent_height: u32) -> Option<Value> {
        None
    }
    async fn author_feed(&self, _actor: &str, _limit: u32, _cursor: Option<&str>, _filter: &str) -> Option<Value> {
        None
    }
    async fn follows(&self, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
}
