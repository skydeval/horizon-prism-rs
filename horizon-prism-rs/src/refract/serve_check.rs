//! The serve-time label check (refract design §4.5): in Refract's
//! endpoints only, the restored posts a page's local query names whose
//! community labels were last read too long ago are read again from the
//! community AppView (`appview`-source communities), in batches of 25, at
//! most 4 calls a request, on the request's budget. The results go into
//! the request's `RefractCheck`, which the gate reads; they are stored as
//! the sweep stores its reads. A post whose check cannot run is served on
//! its last known state.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, TimeDelta, Utc};

use super::Refract;
use crate::config::LabelSource;
use crate::db::locks::uri_author;
use crate::xrpc::pools::Requester;

/// URIs per call.
pub const BATCH: usize = 25;
/// Calls per request.
pub const MAX_CALLS: usize = 4;

impl Refract {
    /// Run the check for `uris` (restored posts are picked out here).
    pub async fn serve_check(&self, req: &Requester, uris: &[String]) {
        if let Err(error) = self.serve_check_inner(req, uris).await {
            tracing::debug!(event = "refract_serve_check_failed", %error, "the serve-time label check failed; last known state served");
        }
    }

    async fn serve_check_inner(&self, req: &Requester, uris: &[String]) -> Result<(), sqlx::Error> {
        let restored: HashMap<String, String> = {
            let sets = self.inner.scope.read_guard().await;
            uris.iter()
                .filter_map(|u| {
                    let a = uri_author(u)?;
                    sets.restored.get(a).map(|c| (u.clone(), c.clone()))
                })
                .collect()
        };
        if restored.is_empty() {
            return Ok(());
        }
        let wanted: Vec<String> = restored.keys().cloned().collect();
        let reads: Vec<(String, DateTime<Utc>, i32)> = sqlx::query_as("SELECT uri, read_at, omissions FROM refract_record_reads WHERE uri = ANY($1)")
            .bind(&wanted)
            .fetch_all(&self.inner.pool)
            .await?;
        let reads: HashMap<String, (DateTime<Utc>, i32)> = reads.into_iter().map(|(u, t, o)| (u, (t, o))).collect();
        let ttl = TimeDelta::minutes(i64::from(self.inner.cfg.record_label_ttl_mins));
        let cap = TimeDelta::hours(i64::from(self.inner.cfg.record_label_sweep_hours));
        let now = Utc::now();
        let communities = self.communities();
        let mut by_community: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (uri, community) in &restored {
            let due = match reads.get(uri) {
                None => true,
                Some((at, omissions)) => {
                    // An omission backs off: the TTL doubles per confirming
                    // omission, up to the sweep's period.
                    let wait = ttl.checked_mul(1 << (*omissions).clamp(0, 16)).unwrap_or(cap).min(cap);
                    now - *at > wait
                }
            };
            if due {
                by_community.entry(community.clone()).or_default().push(uri.clone());
            }
        }
        let mut calls = 0;
        for (name, uris) in by_community {
            let Some(c) = communities.by_name(&name).filter(|c| c.enabled && c.label_source == Some(LabelSource::Appview)) else {
                continue;
            };
            let (Some(labeler), Some(network)) = (c.labeler_did.clone(), c.appview_network.clone()) else {
                continue;
            };
            for batch in uris.chunks(BATCH) {
                if calls >= MAX_CALLS || req.spent() {
                    return Ok(());
                }
                calls += 1;
                let Some(answer) = self.inner.net.request_posts(req, &network, batch, &labeler).await else {
                    return Ok(());
                };
                if let Some(found) = self.store_post_reads(c, batch, &answer).await? {
                    req.note_refract_check(|check| {
                        for u in &found.hidden {
                            check.note(u, true);
                        }
                        for u in &found.unhidden {
                            check.note(u, false);
                        }
                    });
                }
            }
        }
        Ok(())
    }
}
