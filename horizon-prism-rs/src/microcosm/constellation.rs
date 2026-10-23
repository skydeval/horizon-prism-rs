//! Constellation backlink client (design-v2 §5.10, §9.6). Response shapes
//! probed 2026-09-26:
//! - `/links?target&collection&path&limit[&cursor]` →
//!   `{"total", "linking_records": [{"did", "collection", "rkey"}], "cursor"}`
//! - `/links/count/distinct-dids?target&collection&path` → `{"total": n}`
//! - `/links/all?target` →
//!   `{"links": {collection: {path: {"records": n, "distinct_dids": m}}}}`

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;
use url::Url;

use crate::net::{BodyCap, NetClient, NetError, RequestOptions};

/// Per-call timeout for background (non-read-path) calls.
pub const TIMEOUT: Duration = Duration::from_secs(10);

/// A Constellation call failure.
#[derive(Debug, thiserror::Error)]
pub enum ConstellationError {
    #[error(transparent)]
    Net(#[from] NetError),
    #[error("constellation returned HTTP {0}")]
    Status(u16),
    #[error("invalid constellation URL: {0}")]
    Url(#[from] url::ParseError),
}

/// One record linking to the target.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, serde::Serialize)]
pub struct LinkingRecord {
    pub did: String,
    pub collection: String,
    pub rkey: String,
}

/// One `/links` page.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, serde::Serialize)]
pub struct LinksPage {
    #[serde(default)]
    pub total: u64,
    #[serde(default)]
    pub linking_records: Vec<LinkingRecord>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// One `/links/distinct-dids` page: `{"total", "linking_dids": [did…],
/// "cursor"}` (parsed leniently; this shape was not probed).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct DidsPage {
    #[serde(default)]
    pub total: u64,
    #[serde(default)]
    pub linking_dids: Vec<String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Counts for one `(collection, path)` in `/links/all`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct LinkCounts {
    #[serde(default)]
    pub records: u64,
    #[serde(default)]
    pub distinct_dids: u64,
}

/// `/links/all`: collection → path → counts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct AllLinks {
    #[serde(default)]
    pub links: BTreeMap<String, BTreeMap<String, LinkCounts>>,
}

impl AllLinks {
    #[must_use]
    pub fn get(&self, collection: &str, path: &str) -> Option<LinkCounts> {
        self.links.get(collection).and_then(|p| p.get(path)).copied()
    }
}

#[derive(Deserialize)]
struct Total {
    total: u64,
}

/// The client. All calls go through `net`.
#[derive(Clone)]
pub struct ConstellationClient {
    net: NetClient,
    base: String,
}

impl ConstellationClient {
    #[must_use]
    pub fn new(net: NetClient, base_url: &str) -> Self {
        Self {
            net,
            base: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn url(&self, path: &str, params: &[(&str, &str)]) -> Result<String, ConstellationError> {
        Ok(Url::parse_with_params(&format!("{}{path}", self.base), params)?.to_string())
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, ConstellationError> {
        // No request handle reaches this client: the running request, if
        // any, labels the call (design-v3 §14.2).
        let (origin, endpoint) = crate::xrpc::pools::ambient_labels();
        metrics::counter!(super::METRIC_REQUESTS, "service" => "constellation", "origin" => origin, "endpoint" => endpoint).increment(1);
        let options = RequestOptions::new(BodyCap::CONSTELLATION).timeout(TIMEOUT);
        match self.net.get_json::<T>(url, &options).await? {
            Ok(v) => Ok(v),
            Err(response) => Err(ConstellationError::Status(response.status.as_u16())),
        }
    }

    /// One page of records linking to `target` from `collection` at `path`.
    ///
    /// # Errors
    ///
    /// Transport, status or decode failures.
    pub async fn links(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<LinksPage, ConstellationError> {
        let limit = limit.to_string();
        let mut params = vec![("target", target), ("collection", collection), ("path", path), ("limit", &limit)];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let url = self.url("/links", &params)?;
        self.get(&url).await
    }

    /// One page of records linking to `target` from `collection` at
    /// `path`, filtered to those in the repository of `did` (Constellation's
    /// `did` parameter, refract design §7.1). The caller checks every
    /// returned record's DID: a server that ignores the filter is caught.
    ///
    /// # Errors
    ///
    /// Transport, status or decode failures.
    pub async fn links_by(&self, target: &str, collection: &str, path: &str, did: &str, limit: u32) -> Result<LinksPage, ConstellationError> {
        let limit = limit.to_string();
        let url = self.url("/links", &[("target", target), ("collection", collection), ("path", path), ("did", did), ("limit", &limit)])?;
        self.get(&url).await
    }

    /// Distinct DIDs linking to `target` from `collection` at `path`.
    ///
    /// # Errors
    ///
    /// Transport, status or decode failures.
    pub async fn distinct_dids(&self, target: &str, collection: &str, path: &str) -> Result<u64, ConstellationError> {
        let url = self.url(
            "/links/count/distinct-dids",
            &[("target", target), ("collection", collection), ("path", path)],
        )?;
        Ok(self.get::<Total>(&url).await?.total)
    }

    /// One page of distinct DIDs linking to `target` (`/links/distinct-dids`,
    /// §7.1 `getFollowers`).
    ///
    /// # Errors
    ///
    /// Transport, status or decode failures.
    pub async fn distinct_dids_page(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<DidsPage, ConstellationError> {
        let limit = limit.to_string();
        let mut params = vec![("target", target), ("collection", collection), ("path", path), ("limit", &limit)];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let url = self.url("/links/distinct-dids", &params)?;
        self.get(&url).await
    }

    /// Every link count to `target`.
    ///
    /// # Errors
    ///
    /// Transport, status or decode failures.
    pub async fn all(&self, target: &str) -> Result<AllLinks, ConstellationError> {
        let url = self.url("/links/all", &[("target", target)])?;
        self.get(&url).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_probed_shapes() {
        let page: LinksPage = serde_json::from_str(
            r#"{"total":2,"linking_records":[{"did":"did:plc:a","collection":"app.bsky.graph.block","rkey":"3k"}],"cursor":"abc"}"#,
        )
        .expect("page");
        assert_eq!(page.linking_records[0].rkey, "3k");
        assert_eq!(page.cursor.as_deref(), Some("abc"));
        let last: LinksPage = serde_json::from_str(r#"{"total":0,"linking_records":[],"cursor":null}"#).expect("page");
        assert_eq!(last.cursor, None);

        let all: AllLinks = serde_json::from_str(
            r#"{"links":{"app.bsky.feed.like":{".subject.uri":{"records":5,"distinct_dids":4}}}}"#,
        )
        .expect("all");
        assert_eq!(
            all.get("app.bsky.feed.like", ".subject.uri"),
            Some(LinkCounts {
                records: 5,
                distinct_dids: 4
            })
        );
        assert_eq!(all.get("app.bsky.feed.repost", ".subject.uri"), None);
    }

    /// B-53 (design-v3 §14.2): a call made while serving a request is
    /// counted with that request's method; any other is `background`.
    #[tokio::test]
    async fn requests_are_counted_by_origin_and_endpoint() {
        use axum::routing::get;

        use crate::test_support::{serve_fake, series};
        use crate::xrpc::backstop::{self, Selected};

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let router = axum::Router::new().route("/links/count/distinct-dids", get(|| async { axum::Json(serde_json::json!({"total": 3})) }));
        let client = ConstellationClient::new(NetClient::loopback_for_tests(), &serve_fake(router).await);
        let count = || client.distinct_dids("at://did:plc:x/app.bsky.feed.post/1", "app.bsky.feed.like", ".subject.uri");
        // The count refresher: no request.
        assert_eq!(count().await.expect("count"), 3);
        // Serving `getPosts`.
        let mut selected = Selected::by_ip("203.0.113.9".parse().expect("ip"));
        selected.req = selected.req.for_endpoint("app.bsky.feed.getPosts");
        backstop::scope(selected, async {
            count().await.expect("count");
            count().await.expect("count");
        })
        .await;
        let text = handle.render();
        let of = |origin: &str, endpoint: &str| {
            let (origin, endpoint) = (format!(r#"origin="{origin}""#), format!(r#"endpoint="{endpoint}""#));
            series(&text, "microcosm_requests_total", &[r#"service="constellation""#, &origin, &endpoint])
        };
        assert_eq!(of("background", ""), Some(1.0));
        assert_eq!(of("request", "app.bsky.feed.getPosts"), Some(2.0));
    }
}
