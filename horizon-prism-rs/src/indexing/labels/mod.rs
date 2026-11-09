//! Full indexing's third-party labels.
//!
//! Under `[full] labels = "subscribe"` every labeler of `label_sources` is
//! followed through `com.atproto.label.subscribeLabels` and its labels are
//! stored in `full_labels`; views merge them for the labelers a request
//! accepts (`atproto-accept-labelers`). A labeler the request accepts that
//! is not subscribed, a subscription still replaying its history, the
//! subjects older than a subscription that started at the tip, and the
//! subjects that carry a label whose signature did not verify are read from
//! the labeler's own `queryLabels`, cached. Under `labels = "upstream"`
//! every accepted labeler is read that way.
//!
//! - A label whose `src` is not the subscription's labeler is dropped.
//! - Every label is checked against the labeler's `#atproto_label` key. A
//!   live label that fails after the key is read again is dropped; a
//!   replayed one is checked against the keys the labeler's directory log
//!   held at its `cts`, and one that still fails is stored unverified and
//!   never applied.
//! - The newest `cts` per (labeler, subject, record version, value) wins,
//!   which orders negations and re-issues; `exp` is checked when read.
//! - Nothing is subscribed outside breadth Full, and a full trial that runs
//!   live only subscribes from the tip.

pub mod history;
pub mod store;
pub mod subscriber;
pub mod upstream;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::config::{FullConfig, FullLabels};

/// `worker_state` key: each subscription's progress, by labeler DID.
pub const STATE_KEY: &str = "full_labels";
/// `full_labels_lag_seconds{src}`: how far behind now the newest label
/// a subscription stored was issued.
pub const METRIC_LAG: &str = "full_labels_lag_seconds";
/// `full_labels_rejected_total{src,reason}`: labels a subscription
/// dropped (`src`: not its labeler; `signature`: a live label that did not
/// verify; `malformed`).
pub const METRIC_REJECTED: &str = "full_labels_rejected_total";
/// `full_labels_stored_total{src,key}`: labels a subscription stored, by
/// the key that verified them (`current`, `history`) or `unverified`.
pub const METRIC_STORED: &str = "full_labels_stored_total";
/// `full_labels_upstream_total{src,result}`: subjects read from a
/// labeler's `queryLabels` (`cached`, `read`, `failed`).
pub const METRIC_UPSTREAM: &str = "full_labels_upstream_total";

/// `full_labels_unverified_settled_total{src}`: unverified labels
/// settled by the labeler's own current answer for their subject.
pub const METRIC_UNVERIFIED_SETTLED: &str = "full_labels_unverified_settled_total";

/// `[full]`'s label options as they apply: `subscribe` with no source is
/// `upstream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub mode: FullLabels,
    /// The subscribed labelers (empty under `upstream`).
    pub sources: Vec<String>,
    pub backfill: bool,
    pub cache_secs: u32,
}

impl Settings {
    #[must_use]
    pub fn from_config(full: &FullConfig) -> Self {
        let mut sources: Vec<String> = Vec::new();
        for s in &full.label_sources {
            if !sources.contains(s) {
                sources.push(s.clone());
            }
        }
        let mut mode = full.labels;
        if mode == FullLabels::Subscribe && sources.is_empty() {
            tracing::warn!(event = "full_labels_set_aside", "full.labels = \"subscribe\" with no label_sources; set aside: labels are read from each labeler per request");
            mode = FullLabels::Upstream;
        }
        if mode == FullLabels::Upstream {
            sources.clear();
        }
        Self { mode, sources, backfill: full.label_backfill, cache_secs: full.label_cache_secs }
    }

    /// Whether `did` is a subscribed labeler.
    #[must_use]
    pub fn subscribed(&self, did: &str) -> bool {
        self.mode == FullLabels::Subscribe && self.sources.iter().any(|s| s == did)
    }
}

/// One subscription's progress, as persisted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// The `seq` of the last frame stored.
    pub cursor: Option<i64>,
    /// The subscription replays the labeler's history from its first label.
    pub replay: bool,
    /// When a subscription from the tip started: the labels of subjects
    /// older than this are also read from the labeler.
    pub tip_started_at: Option<DateTime<Utc>>,
    /// The stored labels are the labeler's current ones (a replay that
    /// reached the tip; a subscription from the tip always).
    pub caught_up: bool,
}

impl Progress {
    /// A new subscription: a replay from the first label, or from the tip
    /// at `now`.
    #[must_use]
    pub fn start(replay: bool, now: DateTime<Utc>) -> Self {
        if replay { Self { cursor: Some(0), replay: true, tip_started_at: None, caught_up: false } } else { Self { cursor: None, replay: false, tip_started_at: Some(now), caught_up: true } }
    }
}

/// The labels shared by the subscriber and the views.
pub struct ExternalLabels {
    settings: Settings,
    progress: RwLock<HashMap<String, Progress>>,
    upstream: upstream::Upstream,
}

impl ExternalLabels {
    #[must_use]
    pub fn new(settings: Settings, upstream: upstream::Upstream) -> Arc<Self> {
        Arc::new(Self { settings, progress: RwLock::new(HashMap::new()), upstream })
    }

    #[must_use]
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// A subscription's progress, as the subscriber last stored it.
    #[must_use]
    pub fn progress(&self, did: &str) -> Option<Progress> {
        self.progress.read().unwrap_or_else(PoisonError::into_inner).get(did).cloned()
    }

    pub(crate) fn set_progress(&self, did: &str, p: Progress) {
        self.progress.write().unwrap_or_else(PoisonError::into_inner).insert(did.to_string(), p);
    }

    /// Forget every subscription's progress (full was left).
    pub(crate) fn clear_progress(&self) {
        self.progress.write().unwrap_or_else(PoisonError::into_inner).clear();
    }

    /// The labels of `subjects` from the labelers `accepted`, by subject, as
    /// views carry them. A private-tier subject is never asked about.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn for_subjects(&self, pool: &PgPool, subjects: &[String], accepted: &[String]) -> Result<HashMap<String, Vec<Value>>, sqlx::Error> {
        let subjects: Vec<String> = subjects.iter().filter(|s| asked_about(s)).cloned().collect();
        let mut out: HashMap<String, Vec<Value>> = HashMap::new();
        if subjects.is_empty() || accepted.is_empty() {
            return Ok(out);
        }
        let mut stored_srcs: Vec<String> = Vec::new();
        let mut asks: HashMap<String, HashSet<String>> = HashMap::new();
        for labeler in accepted {
            let progress = self.settings.subscribed(labeler).then(|| self.progress(labeler)).flatten();
            match progress {
                Some(p) if p.caught_up => {
                    stored_srcs.push(labeler.clone());
                    if let (false, Some(started)) = (p.replay, p.tip_started_at) {
                        let older: HashSet<String> = subjects.iter().filter(|s| older_than(s, started)).cloned().collect();
                        if !older.is_empty() {
                            asks.entry(labeler.clone()).or_default().extend(older);
                        }
                    }
                }
                // Not subscribed, or still replaying: the labeler answers.
                _ => asks.entry(labeler.clone()).or_default().extend(subjects.iter().cloned()),
            }
        }
        let mut unverified: Vec<(String, String)> = Vec::new();
        if !stored_srcs.is_empty() {
            unverified = store::unverified_subjects(pool, &subjects, &stored_srcs).await?;
            for (src, uri) in &unverified {
                asks.entry(src.clone()).or_default().insert(uri.clone());
            }
            let rows = store::applied(pool, &subjects, &stored_srcs).await?;
            let versions = store::stored_post_cids(pool, &rows).await?;
            for row in rows {
                if crate::hydration::normalize::names_a_version(&row.cid)
                    && let Some(have) = versions.get(&row.uri)
                    && *have != row.cid
                {
                    continue;
                }
                out.entry(row.uri.clone()).or_default().push(row.view());
            }
        }
        let asked = futures::future::join_all(asks.into_iter().map(|(labeler, subjects)| async move {
            let subjects: Vec<String> = subjects.into_iter().collect();
            self.upstream.labels(&labeler, &subjects).await
        }))
        .await;
        for answer in asked {
            for (uri, labels) in answer {
                let have = out.entry(uri).or_default();
                crate::hydration::normalize::add_missing_labels(have, labels);
            }
        }
        // A subject's unverified labels, once the labeler's own answer for
        // it is in: settled by it, so they stop counting as unverified.
        for (src, uri) in &unverified {
            if let Some(current) = self.upstream.answered(src, uri).await {
                let n = store::settle_unverified(pool, src, uri, &current).await?;
                if n > 0 {
                    metrics::counter!(METRIC_UNVERIFIED_SETTLED, "src" => src.clone()).increment(n);
                }
            }
        }
        out.retain(|_, v| !v.is_empty());
        Ok(out)
    }
}

impl std::fmt::Debug for ExternalLabels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalLabels").field("settings", &self.settings).finish_non_exhaustive()
    }
}

impl PartialEq for ExternalLabels {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for ExternalLabels {}

/// Whether a subject may be named to a labeler: never a private-tier record.
fn asked_about(subject: &str) -> bool {
    if subject.starts_with("did:") {
        return true;
    }
    subject.strip_prefix("at://").and_then(|rest| rest.split('/').nth(1)).is_some_and(|coll| !coll.starts_with("tools.kryphocron."))
}

/// Whether `subject` may be older than `t`: every account (Prism has no
/// account creation date), and every record whose key is not a TID of a
/// later time.
fn older_than(subject: &str, t: DateTime<Utc>) -> bool {
    if subject.starts_with("did:") {
        return true;
    }
    let rkey = subject.rsplit('/').next().unwrap_or_default();
    crate::hydration::feed::tid_timestamp(rkey).is_none_or(|at| at < t)
}
