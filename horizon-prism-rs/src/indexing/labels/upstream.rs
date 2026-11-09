//! A labeler's own `com.atproto.label.queryLabels`, per subject, cached.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::METRIC_UPSTREAM;
use crate::net::{BodyCap, NetClient, RequestOptions};
use crate::refract::labels::{Label, Labelers, net_of, verify};

/// Subjects named in one `queryLabels` call.
pub const PATTERNS_PER_CALL: usize = 25;
/// Pages read per call at most.
pub const PAGES_PER_CALL: usize = 4;
/// Labels per page.
pub const PAGE: u32 = 250;
/// One call's timeout: a view waits on it.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(3);
/// Subjects kept in the cache at most.
pub const CACHE_ENTRIES: u64 = 100_000;

/// Reads labelers per request, through one cache.
pub struct Upstream {
    net: NetClient,
    labelers: Labelers,
    cache: moka::future::Cache<(String, String), Arc<Vec<Value>>>,
}

impl Upstream {
    #[must_use]
    pub fn new(net: NetClient, labelers: Labelers, cache_secs: u32) -> Self {
        let cache = moka::future::Cache::builder().max_capacity(CACHE_ENTRIES).time_to_live(Duration::from_secs(u64::from(cache_secs.max(1)))).build();
        Self { net, labelers, cache }
    }

    /// `labeler`'s current labels of `subjects`, by subject: its negations
    /// applied, expired ones left out, every signed label verified against
    /// its key (read again once on a failure). A failed read counts as no
    /// labels until the cache entry expires.
    pub async fn labels(&self, labeler: &str, subjects: &[String]) -> HashMap<String, Vec<Value>> {
        let mut out: HashMap<String, Vec<Value>> = HashMap::new();
        let mut missing: Vec<String> = Vec::new();
        for s in subjects {
            match self.cache.get(&(labeler.to_string(), s.clone())).await {
                Some(hit) => {
                    metrics::counter!(METRIC_UPSTREAM, "src" => labeler.to_string(), "result" => "cached").increment(1);
                    if !hit.is_empty() {
                        out.insert(s.clone(), hit.as_ref().clone());
                    }
                }
                None => missing.push(s.clone()),
            }
        }
        if missing.is_empty() {
            return out;
        }
        let endpoint = self.labelers.doc(labeler, false).await.and_then(|d| d.endpoint);
        for chunk in missing.chunks(PATTERNS_PER_CALL) {
            let read = match &endpoint {
                Some(e) => self.read(e, labeler, chunk).await,
                None => None,
            };
            let result = if read.is_some() { "read" } else { "failed" };
            metrics::counter!(METRIC_UPSTREAM, "src" => labeler.to_string(), "result" => result).increment(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            let mut found = read.unwrap_or_default();
            for s in chunk {
                let labels = found.remove(s).unwrap_or_default();
                self.cache.insert((labeler.to_string(), s.clone()), Arc::new(labels.clone())).await;
                if !labels.is_empty() {
                    out.insert(s.clone(), labels);
                }
            }
        }
        out
    }

    /// One call for `subjects`, paged; `None` when the labeler did not
    /// answer.
    async fn read(&self, endpoint: &str, labeler: &str, subjects: &[String]) -> Option<HashMap<String, Vec<Value>>> {
        let mut received: Vec<Label> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..PAGES_PER_CALL {
            let mut url = url::Url::parse(&format!("{}/xrpc/com.atproto.label.queryLabels", endpoint.trim_end_matches('/'))).ok()?;
            {
                let mut q = url.query_pairs_mut();
                for s in subjects {
                    q.append_pair("uriPatterns", s);
                }
                q.append_pair("sources", labeler);
                q.append_pair("limit", &PAGE.to_string());
                if let Some(c) = &cursor {
                    q.append_pair("cursor", c);
                }
            }
            let r = self.net.get(url.as_str(), &RequestOptions::new(BodyCap::APPVIEW).timeout(CALL_TIMEOUT)).await.ok()?;
            if !r.status.is_success() {
                return None;
            }
            let body: Value = serde_json::from_slice(&r.body).ok()?;
            let page: Vec<Label> = body.get("labels").and_then(Value::as_array).map(|ls| ls.iter().filter_map(Label::parse).collect()).unwrap_or_default();
            let full = page.len() >= usize::try_from(PAGE).unwrap_or(usize::MAX);
            received.extend(page);
            match body.get("cursor").and_then(Value::as_str) {
                Some(c) if full && cursor.as_deref() != Some(c) => cursor = Some(c.to_string()),
                _ => break,
            }
        }
        let checked = self.checked(labeler, received).await;
        let now = chrono::Utc::now();
        let mut out: HashMap<String, Vec<Value>> = HashMap::new();
        for (l, _) in net_of(checked) {
            if l.exp.is_some_and(|e| e <= now) || !subjects.contains(&l.uri) {
                continue;
            }
            let mut v = l.raw.clone();
            if v.get("ver").is_none() {
                v["ver"] = Value::from(1);
            }
            out.entry(l.uri.clone()).or_default().push(v);
        }
        Some(out)
    }

    /// The labels from `labeler` itself whose signature (when they carry
    /// one) verifies, its key read again once on a failure.
    async fn checked(&self, labeler: &str, labels: Vec<Label>) -> Vec<(Label, bool)> {
        let mut key: Option<String> = None;
        let mut refreshed = false;
        let mut out = Vec::new();
        for l in labels {
            if l.src != labeler {
                continue;
            }
            if !l.signed() {
                out.push((l, true));
                continue;
            }
            if key.is_none() {
                key = self.labelers.doc(labeler, false).await.and_then(|d| d.key);
            }
            let mut ok = key.as_deref().is_some_and(|k| verify(&l, k));
            if !ok && !refreshed {
                refreshed = true;
                key = self.labelers.doc(labeler, true).await.and_then(|d| d.key);
                ok = key.as_deref().is_some_and(|k| verify(&l, k));
            }
            if ok {
                out.push((l, true));
            }
        }
        out
    }
}
