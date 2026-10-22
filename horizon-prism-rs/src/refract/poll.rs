//! Label reads beyond the check (refract design §4.5): the incremental
//! poll of a `labeler`-source community, the rolling sweep of an
//! `appview`-source one, and the post reads both the sweep and the
//! serve-time check store.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::Refract;
use super::labels::{self, Label};
use super::net::Answer;
use super::state::{ABSENT, State};
use crate::config::{LabelSource, RefractCommunity};
use crate::db::locks::uri_author;

/// `refract_label_poll_total{community, outcome}`.
pub const METRIC_POLL: &str = "refract_label_poll_total";
/// `refract_label_poll_lag_seconds{community}`.
pub const METRIC_POLL_LAG: &str = "refract_label_poll_lag_seconds";
/// `refract_label_sweep_total{community, outcome}`.
pub const METRIC_SWEEP: &str = "refract_label_sweep_total";
/// `refract_label_sweep_lag_seconds{community}`.
pub const METRIC_SWEEP_LAG: &str = "refract_label_sweep_lag_seconds";
/// Pages one poll reads before it yields.
const POLL_MAX_PAGES: usize = 100;

fn cursor_key(labeler: &str) -> String {
    format!("refract_label_cursor:{labeler}")
}

/// What one batch of post reads found, per URI: hidden or not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostReads {
    pub hidden: HashSet<String>,
    pub unhidden: HashSet<String>,
}

impl Refract {
    /// Run the incremental poll of every `labeler`-source community, every
    /// `refract.label_poll_secs`, until aborted. Communities come and go
    /// with the live settings.
    pub async fn run_poller(self) {
        let every = Duration::from_secs(u64::from(self.inner.cfg.label_poll_secs.max(1)));
        loop {
            if self.enabled() {
                for c in self.communities().labeler_sourced().cloned().collect::<Vec<_>>() {
                    let outcome = match self.poll(&c).await {
                        Ok(o) => o,
                        Err(error) => {
                            tracing::warn!(event = "refract_label_poll_failed", community = %c.name, %error, "the label poll failed");
                            "error"
                        }
                    };
                    metrics::counter!(METRIC_POLL, "community" => c.name.clone(), "outcome" => outcome).increment(1);
                }
            }
            tokio::time::sleep(every).await;
        }
    }

    async fn cursor(&self, labeler: &str) -> Result<Option<String>, sqlx::Error> {
        let v: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(cursor_key(labeler)).fetch_optional(&self.inner.pool).await?;
        Ok(v.and_then(|v| v.as_str().map(str::to_string)))
    }

    async fn save_cursor(&self, labeler: &str, cursor: &str) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb($2::text), now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()")
            .bind(cursor_key(labeler))
            .bind(cursor)
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    /// One poll of `c`'s labeler: on first start a fast-forward that stores
    /// nothing, then the labels since the cursor, kept only for candidates
    /// ∪ R of this community.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn poll(&self, c: &RefractCommunity) -> Result<&'static str, sqlx::Error> {
        let Some(labeler) = c.labeler_did.clone() else {
            return Ok("no_labeler");
        };
        let Some(endpoint) = self.inner.labelers.doc(&labeler, false).await.and_then(|d| d.endpoint) else {
            return Ok("no_endpoint");
        };
        let patterns = ["did:*".to_string(), "at://*".to_string()];
        let saved = self.cursor(&labeler).await?;
        let fast_forward = saved.is_none();
        let mut cursor = saved;
        let pace = Duration::from_secs_f64(1.0 / f64::from(self.inner.cfg.label_ff_pages_per_sec.max(1)));
        for _ in 0..POLL_MAX_PAGES {
            let answer = self.inner.net.query_labels(&endpoint, &patterns, &labeler, cursor.as_deref()).await;
            let Answer::Reply { status, body, .. } = answer else {
                return Ok("unreachable");
            };
            if !(200..300).contains(&status) {
                return Ok("error");
            }
            let page: Vec<Label> = body.get("labels").and_then(Value::as_array).map(|a| a.iter().filter_map(Label::parse).collect()).unwrap_or_default();
            if let Some(newest) = page.iter().filter_map(|l| l.cts).max() {
                #[allow(clippy::cast_precision_loss)]
                metrics::gauge!(METRIC_POLL_LAG, "community" => c.name.clone()).set((Utc::now() - newest).num_seconds().max(0) as f64);
            }
            if !fast_forward && !page.is_empty() {
                self.store_polled(c, &labeler, page.clone()).await?;
            }
            let next = body.get("cursor").and_then(Value::as_str).map(str::to_string);
            match next {
                Some(n) if cursor.as_deref() != Some(n.as_str()) => {
                    self.save_cursor(&labeler, &n).await?;
                    cursor = Some(n);
                }
                _ => return Ok(if fast_forward { "fast_forwarded" } else { "ok" }),
            }
            if page.is_empty() {
                return Ok(if fast_forward { "fast_forwarded" } else { "ok" });
            }
            if fast_forward {
                tokio::time::sleep(pace).await;
            }
        }
        Ok("more")
    }

    /// Store a polled page's labels of this community's candidates ∪ R.
    async fn store_polled(&self, c: &RefractCommunity, labeler: &str, page: Vec<Label>) -> Result<(), sqlx::Error> {
        let allowed: HashSet<String> = {
            let sets = self.inner.scope.read_guard().await;
            sets.candidates
                .iter()
                .chain(sets.restored.iter())
                .filter(|(_, community)| **community == c.name)
                .map(|(d, _)| d.clone())
                .chain(sets.refract_hidden.accounts.iter().filter(|(_, r)| r.pending).map(|(d, _)| d.clone()))
                .collect()
        };
        let relevant: Vec<Label> = page.into_iter().filter(|l| l.subject_did().is_some_and(|d| allowed.contains(d))).collect();
        if relevant.is_empty() {
            return Ok(());
        }
        let checked = self.inner.labelers.check(labeler, &c.name, relevant, &c.gone_labels, false).await;
        for (did, labels) in labels::for_allowed(checked, &allowed) {
            let gone_account = labels.iter().any(|(l, _)| !l.neg && l.on_account() && c.gone_labels.contains(&l.val));
            let communities = self.communities();
            let mut sets = self.inner.scope.write_guard().await;
            let mut tx = self.inner.pool.begin().await?;
            let Some((state, _, community)) = labels::lock_row(&mut tx, &did).await? else {
                continue;
            };
            if !matches!(state, State::Candidate | State::Restored | State::Grace) || community != c.name {
                continue;
            }
            for (l, ok) in &labels {
                labels::upsert(&mut tx, labeler, &did, l, *ok, &c.gone_labels).await?;
            }
            if gone_account && state.in_r() {
                sqlx::query("UPDATE refract_dids SET due_seq = due_seq + 1, next_check_at = now() WHERE did = $1").bind(&did).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            super::state::refresh(&self.inner.pool, &mut sets, Some(std::slice::from_ref(&did)), &communities).await?;
            drop(sets);
            if gone_account {
                self.inner.due.notify_one();
            }
        }
        Ok(())
    }

    /// Run the rolling sweep of every `appview`-source community until
    /// aborted (§4.5): every stored post of R-live, re-read once every
    /// `refract.record_label_sweep_hours`.
    pub async fn run_sweep(self) {
        loop {
            let mut worked = false;
            if self.enabled() {
                for c in self.communities().all().iter().filter(|c| c.enabled && c.label_source == Some(LabelSource::Appview)).cloned().collect::<Vec<_>>() {
                    match self.sweep_once(&c).await {
                        Ok(n) => worked |= n > 0,
                        Err(error) => tracing::warn!(event = "refract_label_sweep_failed", community = %c.name, %error, "the label sweep failed"),
                    }
                }
            }
            if !worked {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }

    /// One sweep batch of `c`: up to 25 of its restored posts due for a
    /// read. Returns how many were read.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn sweep_once(&self, c: &RefractCommunity) -> Result<usize, sqlx::Error> {
        let (Some(labeler), Some(network)) = (c.labeler_did.clone(), c.appview_network.clone()) else {
            return Ok(0);
        };
        let dids: Vec<String> = {
            let sets = self.inner.scope.read_guard().await;
            sets.restored.iter().filter(|(_, n)| **n == c.name).map(|(d, _)| d.clone()).collect()
        };
        if dids.is_empty() {
            return Ok(0);
        }
        let due: Vec<(String, Option<DateTime<Utc>>)> = sqlx::query_as(
            "SELECT p.uri, r.read_at FROM posts p LEFT JOIN refract_record_reads r ON r.uri = p.uri \
             WHERE p.author_did = ANY($1) AND (r.read_at IS NULL OR r.read_at < now() - make_interval(hours => $2)) \
             ORDER BY r.read_at NULLS FIRST LIMIT 25",
        )
        .bind(&dids)
        .bind(i32::try_from(self.inner.cfg.record_label_sweep_hours).unwrap_or(24))
        .fetch_all(&self.inner.pool)
        .await?;
        if let Some((_, Some(oldest))) = due.iter().find(|(_, t)| t.is_some()) {
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!(METRIC_SWEEP_LAG, "community" => c.name.clone())
                .set(((Utc::now() - *oldest).num_seconds() - i64::from(self.inner.cfg.record_label_sweep_hours) * 3600).max(0) as f64);
        }
        if due.is_empty() {
            return Ok(0);
        }
        let uris: Vec<String> = due.into_iter().map(|(u, _)| u).collect();
        let answer = self.inner.net.sweep_posts(&network, &uris, &labeler).await;
        let outcome = if self.store_post_reads(c, &uris, &answer).await?.is_some() { "ok" } else { "error" };
        metrics::counter!(METRIC_SWEEP, "community" => c.name.clone(), "outcome" => outcome).increment(1);
        Ok(uris.len())
    }

    /// Store one `getPosts` answer for `uris` of `c`'s restored DIDs
    /// (§4.5): a returned post's labels replace its rows and un-hide an
    /// omission; an omitted post that was seen before is hidden by a
    /// synthetic row, one never seen stays visible. No omissions are
    /// recorded in `grace`. `None` when the answer does not count (an
    /// error, or the labeler header missing).
    ///
    /// The write guard is taken only when a post's hidden state changes.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn store_post_reads(&self, c: &RefractCommunity, uris: &[String], answer: &Answer) -> Result<Option<PostReads>, sqlx::Error> {
        let Some(labeler) = c.labeler_did.clone() else {
            return Ok(None);
        };
        let Answer::Reply { status, body, .. } = answer else {
            metrics::counter!(super::METRIC_LABEL_READS, "community" => c.name.clone(), "source" => "appview", "outcome" => "error").increment(1);
            return Ok(None);
        };
        if !(200..300).contains(status) || !answer.applied_labeler(&labeler) {
            metrics::counter!(super::METRIC_LABEL_READS, "community" => c.name.clone(), "source" => "appview", "outcome" => "error").increment(1);
            return Ok(None);
        }
        let posts: HashMap<String, Value> = body
            .get("posts")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|p| Some((p.get("uri")?.as_str()?.to_string(), p.clone()))).collect())
            .unwrap_or_default();
        let mut by_did: HashMap<String, Vec<String>> = HashMap::new();
        for u in uris {
            if let Some(d) = uri_author(u) {
                by_did.entry(d.to_string()).or_default().push(u.clone());
            }
        }
        let mut out = PostReads::default();
        for (did, uris) in by_did {
            // The labels of the returned posts, and the author's.
            let mut record_labels: Vec<Label> = Vec::new();
            let mut account: Vec<Label> = Vec::new();
            for u in &uris {
                if let Some(p) = posts.get(u) {
                    record_labels.extend(p.get("labels").and_then(Value::as_array).into_iter().flatten().filter_map(Label::parse).filter(|l| l.uri == *u));
                    account.extend(p.pointer("/author/labels").and_then(Value::as_array).into_iter().flatten().filter_map(Label::parse).filter(|l| l.uri == did));
                }
            }
            let records = labels::net_of(self.inner.labelers.check(&labeler, &c.name, record_labels, &c.gone_labels, true).await);
            let gone_account = labels::net_of(self.inner.labelers.check(&labeler, &c.name, account, &c.gone_labels, true).await)
                .iter()
                .any(|(l, _)| c.gone_labels.contains(&l.val));
            let reads = self.write_reads(c, &labeler, &did, &uris, &posts, &records, gone_account, false).await?;
            let reads = match reads {
                Some(r) => r,
                // The hidden state changes: again under the write guard.
                None => self.write_reads(c, &labeler, &did, &uris, &posts, &records, gone_account, true).await?.unwrap_or_default(),
            };
            out.hidden.extend(reads.hidden);
            out.unhidden.extend(reads.unhidden);
        }
        Ok(Some(out))
    }

    /// One DID's reads in one transaction. Without `guarded`, the
    /// transaction is rolled back and `None` returned when it would change
    /// a post's hidden state.
    #[allow(clippy::too_many_arguments)]
    async fn write_reads(
        &self,
        c: &RefractCommunity,
        labeler: &str,
        did: &str,
        uris: &[String],
        posts: &HashMap<String, Value>,
        records: &[(Label, bool)],
        gone_account: bool,
        guarded: bool,
    ) -> Result<Option<PostReads>, sqlx::Error> {
        let communities = self.communities();
        let mut guard = if guarded { Some(self.inner.scope.write_guard().await) } else { None };
        let mut tx = self.inner.pool.begin().await?;
        let Some((state, _, community)) = labels::lock_row(&mut tx, did).await? else {
            return Ok(Some(PostReads::default()));
        };
        if !state.in_r() || community != c.name {
            return Ok(Some(PostReads::default()));
        }
        let before: HashSet<String> = sqlx::query_scalar("SELECT DISTINCT subject FROM refract_labels WHERE labeler_did = $1 AND subject = ANY($2) AND gone AND (exp IS NULL OR exp > now())")
            .bind(labeler)
            .bind(uris)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
        let mut out = PostReads::default();
        for u in uris {
            let seen: Option<Option<DateTime<Utc>>> = sqlx::query_scalar("SELECT seen_at FROM refract_record_reads WHERE uri = $1").bind(u).fetch_optional(&mut *tx).await?;
            let seen = seen.flatten();
            if posts.contains_key(u) {
                let mine: Vec<(Label, bool)> = records.iter().filter(|(l, _)| l.uri == *u).cloned().collect();
                labels::replace(&mut tx, labeler, did, labels::Replace::Records(std::slice::from_ref(u)), &mine, &c.gone_labels).await?;
                sqlx::query("DELETE FROM refract_labels WHERE labeler_did = $1 AND subject = $2 AND val = $3").bind(labeler).bind(u).bind(ABSENT).execute(&mut *tx).await?;
                sqlx::query(
                    "INSERT INTO refract_record_reads (uri, did, read_at, seen_at, omissions) VALUES ($1, $2, now(), now(), 0) \
                     ON CONFLICT (uri) DO UPDATE SET read_at = now(), seen_at = now(), omissions = 0",
                )
                .bind(u)
                .bind(did)
                .execute(&mut *tx)
                .await?;
                metrics::counter!(super::METRIC_LABEL_READS, "community" => c.name.clone(), "source" => "appview", "outcome" => "returned").increment(1);
            } else if seen.is_some() && state != State::Grace {
                sqlx::query(
                    "INSERT INTO refract_labels (labeler_did, subject, val, did, gone, verified) VALUES ($1, $2, $3, $4, true, true) \
                     ON CONFLICT (labeler_did, subject, val) DO NOTHING",
                )
                .bind(labeler)
                .bind(u)
                .bind(ABSENT)
                .bind(did)
                .execute(&mut *tx)
                .await?;
                sqlx::query("UPDATE refract_record_reads SET read_at = now(), omissions = omissions + 1 WHERE uri = $1").bind(u).execute(&mut *tx).await?;
                metrics::counter!(super::METRIC_LABEL_READS, "community" => c.name.clone(), "source" => "appview", "outcome" => "omitted_seen").increment(1);
            } else {
                sqlx::query(
                    "INSERT INTO refract_record_reads (uri, did, read_at, omissions) VALUES ($1, $2, now(), 0) \
                     ON CONFLICT (uri) DO UPDATE SET read_at = now()",
                )
                .bind(u)
                .bind(did)
                .execute(&mut *tx)
                .await?;
                metrics::counter!(super::METRIC_LABEL_READS, "community" => c.name.clone(), "source" => "appview", "outcome" => "never_seen").increment(1);
            }
        }
        let after: HashSet<String> = sqlx::query_scalar("SELECT DISTINCT subject FROM refract_labels WHERE labeler_did = $1 AND subject = ANY($2) AND gone AND (exp IS NULL OR exp > now())")
            .bind(labeler)
            .bind(uris)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
        for u in uris {
            if after.contains(u) {
                out.hidden.insert(u.clone());
            } else {
                out.unhidden.insert(u.clone());
            }
        }
        if gone_account {
            sqlx::query("UPDATE refract_dids SET due_seq = due_seq + 1, next_check_at = now() WHERE did = $1").bind(did).execute(&mut *tx).await?;
        }
        if before != after && guard.is_none() {
            tx.rollback().await?;
            return Ok(None);
        }
        tx.commit().await?;
        if let Some(sets) = guard.as_mut() {
            super::state::refresh(&self.inner.pool, sets, Some(&[did.to_string()]), &communities).await?;
        }
        drop(guard);
        if gone_account {
            self.inner.due.notify_one();
        }
        Ok(Some(out))
    }
}
