//! The checker (refract design §4.1, §4.3, §4.4): it claims due rows in
//! states `candidate`, `restored` and `grace` (never `rejected`, `ending`
//! or a DID listed in `refract_pending_dids`), runs the five rules in order
//! with its network calls made before any guard, then writes the verdict
//! by §4.3's protocol: the write guard, one transaction that re-checks the
//! row's `version`, the commit, the in-memory change, the guard dropped.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use serde_json::{Value, json};
use sqlx::PgConnection;

use super::Refract;
use super::labels::{self, Label, Replace};
use super::net::Answer;
use super::state::{ABSENT, State};
use crate::config::{LabelSource, RefractCommunity};
use crate::constellation::matcher::pds_host;
use crate::identity::refresher::{RepoStatus, Suspicion, account_status, advance_suspicion, is_repo_not_found};
use crate::identity::resolver::extract_info;

/// `refract_checks_total{rule, verdict}`.
pub const METRIC_CHECKS: &str = "refract_checks_total";
/// `refract_transitions_total{from, to, reason}`.
pub const METRIC_TRANSITIONS: &str = "refract_transitions_total";
/// How long a claimed row is hidden from other claims.
pub const CLAIM_LEASE: TimeDelta = TimeDelta::minutes(10);
/// The checker's poll when nothing is due.
pub const IDLE_POLL: Duration = Duration::from_secs(5);
/// A candidate whose checks stay unknown is rejected after this (§4.3).
pub const CANDIDATE_MAX: TimeDelta = TimeDelta::hours(24);
/// A rejected row is deleted after this without a `fill` (§4.3).
pub const REJECTED_KEEP: TimeDelta = TimeDelta::days(30);

/// One rule's result (§4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail,
    Unknown,
    /// Rule 3 only: the account is deactivated.
    Grace,
}

impl Outcome {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Unknown => "unknown",
            Self::Grace => "grace",
        }
    }
}

/// The five rules' results, in order; `None` for a rule not reached.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rules {
    pub results: [Option<(Outcome, String)>; 5],
}

impl Rules {
    fn set(&mut self, rule: usize, outcome: Outcome, detail: impl Into<String>) {
        self.results[rule - 1] = Some((outcome, detail.into()));
    }

    fn get(&self, rule: usize) -> Option<Outcome> {
        self.results[rule - 1].as_ref().map(|(o, _)| *o)
    }

    /// The first failing rule (1-based).
    #[must_use]
    pub fn failed(&self) -> Option<usize> {
        self.results.iter().position(|r| matches!(r, Some((Outcome::Fail, _)))).map(|i| i + 1)
    }

    /// The overall verdict (§4.1 "Precedence"): a fail wins; then rule 3's
    /// grace, which outranks unknowns; then any unknown; else pass.
    #[must_use]
    pub fn verdict(&self) -> Outcome {
        if self.failed().is_some() {
            Outcome::Fail
        } else if self.get(3) == Some(Outcome::Grace) {
            Outcome::Grace
        } else if self.results.iter().any(|r| !matches!(r, Some((Outcome::Pass, _)))) {
            Outcome::Unknown
        } else {
            Outcome::Pass
        }
    }

    fn json(&self, now: DateTime<Utc>) -> Value {
        let rules: Vec<Value> = self
            .results
            .iter()
            .enumerate()
            .map(|(i, r)| match r {
                Some((o, d)) => json!({"rule": i + 1, "result": o.as_str(), "detail": d}),
                None => json!({"rule": i + 1, "result": "not_reached"}),
            })
            .collect();
        json!({"at": now.to_rfc3339(), "rules": rules})
    }
}

/// Rule 1 from Bluesky's answer (§4.1): withheld passes, a served profile
/// fails, anything else is unknown.
#[must_use]
pub fn rule1(answer: &Answer) -> (Outcome, String) {
    match answer {
        Answer::Reply { status, body, .. } if (200..300).contains(status) && body.get("did").is_some() => (Outcome::Fail, "bluesky serves it".into()),
        a if a.error() == Some("AccountTakedown") && a.status() == Some(400) => (Outcome::Pass, "AccountTakedown".into()),
        a if a.message().is_some_and(|m| m.to_ascii_lowercase().contains("account has been suspended")) => {
            (Outcome::Pass, "account has been suspended".into())
        }
        a => (Outcome::Unknown, describe(a)),
    }
}

fn describe(a: &Answer) -> String {
    match a {
        Answer::Reply { status, .. } => format!("{status} {}", a.error().unwrap_or_default()),
        Answer::Failed(e) => e.clone(),
    }
}

/// A `RepoNotFound` suspicion: the endpoint and since when.
pub type Suspect = (String, DateTime<Utc>);

/// Rule 3's result: the outcome and its detail, the suspicion clock and
/// the account status read.
pub type Rule3 = (Outcome, String, Option<Suspect>, Option<String>);

/// Rule 3 from a `getRepoStatus` answer (§4.1). `RepoNotFound` advances the
/// suspicion clock kept in `last_verdict`.
#[must_use]
pub fn rule3(answer: &Answer, endpoint: &str, prev: Option<(&str, DateTime<Utc>)>, now: DateTime<Utc>) -> Rule3 {
    match answer {
        Answer::Reply { status, body, .. } if (200..300).contains(status) => {
            let Ok(repo) = serde_json::from_value::<RepoStatus>(body.clone()) else {
                return (Outcome::Unknown, "unreadable status".into(), None, None);
            };
            let s = account_status(&repo);
            let outcome = match s.as_str() {
                "active" => Outcome::Pass,
                "takendown" | "suspended" | "deleted" => Outcome::Fail,
                "deactivated" => Outcome::Grace,
                _ => Outcome::Unknown,
            };
            (outcome, s.clone(), None, Some(s))
        }
        Answer::Reply { status, body, .. } if is_repo_not_found(*status, body.to_string().as_bytes()) => {
            match advance_suspicion(prev.map(|p| p.0), prev.map(|p| p.1), endpoint, now) {
                Suspicion::Deleted => (Outcome::Fail, "RepoNotFound past 24 h".into(), None, Some("deleted".into())),
                Suspicion::Open { since } => (Outcome::Unknown, "RepoNotFound".into(), Some((endpoint.to_string(), since)), None),
            }
        }
        a => (Outcome::Unknown, describe(a), None, None),
    }
}

/// The tier 1 part of rule 4 from the community AppView's `getProfile`
/// (§4.1), with the labels of its view.
fn rule4_appview(answer: &Answer) -> (Outcome, String) {
    match answer {
        Answer::Reply { status, body, .. } if (200..300).contains(status) && body.get("did").is_some() => (Outcome::Pass, "served".into()),
        a if a.error() == Some("AccountTakedown") => (Outcome::Fail, "AccountTakedown".into()),
        a if a.status() == Some(404)
            || a.error() == Some("ProfileNotFound")
            || (a.status() == Some(400) && a.message().is_some_and(|m| m.to_ascii_lowercase().contains("profile not found"))) =>
        {
            (Outcome::Fail, "profile not found".into())
        }
        a => (Outcome::Unknown, describe(a)),
    }
}

/// What the label fetch of a check found.
#[derive(Debug)]
enum Fetched {
    /// The labeler's own `queryLabels`: every label of the DID.
    All(Vec<(Label, bool)>),
    /// Through the AppView's `getProfile`: the account's labels and the
    /// profile record's.
    Profile { account: Vec<(Label, bool)>, profile: Vec<(Label, bool)> },
    /// The fetch could not be made or did not count.
    Failed,
    /// The community has no labeler.
    NoLabeler,
}

/// One claimed row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Claimed {
    pub did: String,
    pub state: String,
    pub version: i64,
    pub due_seq: i64,
    pub community: String,
    pub pds_host: Option<String>,
    pub attempts: i32,
    pub last_verdict: Option<Value>,
    pub grace_until: Option<DateTime<Utc>>,
    pub unverified_since: Option<DateTime<Utc>>,
    pub awaiting_labels_since: Option<DateTime<Utc>>,
    pub candidate_since: Option<DateTime<Utc>>,
}

/// Backoff after `attempts` consecutive unknowns: 5 min doubling to 6 h.
#[must_use]
pub fn backoff(attempts: i32) -> TimeDelta {
    let mins = 5_i64.saturating_mul(1_i64 << attempts.clamp(0, 12));
    TimeDelta::minutes(mins.min(360))
}

/// A rejected row's re-eligibility after its `n`-th rejection: 24 h
/// doubling to 7 d.
#[must_use]
pub fn rejected_wait(n: i64) -> TimeDelta {
    let hours = 24_i64.saturating_mul(1_i64 << (n - 1).clamp(0, 8));
    TimeDelta::hours(hours.min(24 * 7))
}

/// The decision for one verdict (§4.3's table).
#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    pub to: State,
    pub reason: Option<String>,
    pub next_check_at: DateTime<Utc>,
    pub attempts: i32,
}

/// The inputs of [`decide`] beyond the verdict.
#[derive(Debug, Clone, Copy)]
pub struct Context {
    pub now: DateTime<Utc>,
    pub recheck: TimeDelta,
    pub unverified_max: TimeDelta,
}

/// §4.3's table.
#[must_use]
pub fn decide(row: &Claimed, rules: &Rules, unverified_since: Option<DateTime<Utc>>, ctx: Context) -> Decision {
    let state = State::parse(&row.state).unwrap_or(State::Candidate);
    let now = ctx.now;
    let verdict = rules.verdict();
    let retry = |attempts: i32| Decision {
        to: state,
        reason: None,
        next_check_at: now + backoff(attempts),
        attempts: attempts + 1,
    };
    let end = |reason: String| Decision { to: State::Ending, reason: Some(reason), next_check_at: now, attempts: 0 };
    let fail_reason = || match rules.failed() {
        Some(1) => "bluesky_lifted".to_string(),
        Some(2) => "moved".to_string(),
        Some(n) => format!("rule_{n}"),
        None => "unknown".to_string(),
    };
    let rule4_unknown = rules.get(4) == Some(Outcome::Unknown);
    let stale_unverified = rule4_unknown && unverified_since.is_some_and(|t| now - t > ctx.unverified_max);
    let stale_awaiting = rule4_unknown && row.awaiting_labels_since.is_some_and(|t| now - t > ctx.unverified_max);
    match state {
        State::Candidate => match verdict {
            Outcome::Pass => Decision { to: State::Restored, reason: Some("checked".into()), next_check_at: now + ctx.recheck, attempts: 0 },
            Outcome::Fail => Decision { to: State::Rejected, reason: Some(fail_reason()), next_check_at: now, attempts: 0 },
            Outcome::Unknown | Outcome::Grace => {
                if row.candidate_since.is_some_and(|t| now - t > CANDIDATE_MAX) {
                    Decision { to: State::Rejected, reason: Some("unknown_24h".into()), next_check_at: now, attempts: 0 }
                } else {
                    retry(row.attempts)
                }
            }
        },
        State::Restored => match verdict {
            Outcome::Pass => Decision { to: State::Restored, reason: None, next_check_at: now + ctx.recheck, attempts: 0 },
            Outcome::Grace => Decision { to: State::Grace, reason: Some("deactivated".into()), next_check_at: now + backoff(0), attempts: 0 },
            Outcome::Fail => end(fail_reason()),
            Outcome::Unknown if stale_unverified || stale_awaiting => end("unverified_gone_label".into()),
            Outcome::Unknown => retry(row.attempts),
        },
        State::Grace => {
            let rule3 = rules.get(3);
            match verdict {
                Outcome::Fail => end(fail_reason()),
                _ if stale_awaiting => end("unverified_gone_label".into()),
                Outcome::Pass => Decision { to: State::Restored, reason: Some("reactivated".into()), next_check_at: now + ctx.recheck, attempts: 0 },
                _ if row.grace_until.is_some_and(|t| t <= now) && rule3 != Some(Outcome::Pass) => end("deactivated".into()),
                _ => retry(row.attempts),
            }
        }
        // Never claimed.
        State::Rejected | State::Ending => Decision { to: state, reason: None, next_check_at: now + ctx.recheck, attempts: row.attempts },
    }
}

impl Refract {
    /// Run the checker until aborted.
    pub async fn run_checker(self) {
        loop {
            if !self.enabled() {
                tokio::time::sleep(IDLE_POLL).await;
                continue;
            }
            let n = match self.check_due().await {
                Ok(n) => n,
                Err(error) => {
                    tracing::warn!(event = "refract_check_claim_failed", %error, "claiming due Refract rows failed");
                    0
                }
            };
            if n == 0 {
                tokio::select! {
                    () = self.inner.due.notified() => {}
                    () = tokio::time::sleep(IDLE_POLL) => {}
                }
            }
        }
    }

    /// Claim the due rows and check them, `checker_concurrency` at a time.
    /// Returns how many were claimed.
    ///
    /// # Errors
    ///
    /// Claim failures; each check's own failure is logged.
    pub async fn check_due(&self) -> Result<usize, sqlx::Error> {
        let limit = i64::from(self.inner.cfg.checker_concurrency.max(1));
        let claimed: Vec<Claimed> = sqlx::query_as(
            "UPDATE refract_dids r SET next_check_at = now() + make_interval(secs => $2) \
             FROM (SELECT d.did FROM refract_dids d \
                   WHERE d.state IN ('candidate', 'restored', 'grace') AND d.next_check_at <= now() \
                     AND NOT EXISTS (SELECT 1 FROM refract_pending_dids p WHERE p.did = d.did) \
                   ORDER BY d.next_check_at LIMIT $1 FOR UPDATE SKIP LOCKED) due \
             WHERE r.did = due.did \
             RETURNING r.did, r.state, r.version, r.due_seq, r.community, r.pds_host, r.attempts, r.last_verdict, \
               r.grace_until, r.unverified_since, r.awaiting_labels_since, r.candidate_since",
        )
        .bind(limit)
        .bind(CLAIM_LEASE.num_seconds() as f64)
        .fetch_all(&self.inner.pool)
        .await?;
        let n = claimed.len();
        futures::stream::iter(claimed)
            .for_each_concurrent(usize::try_from(limit).unwrap_or(4), |row| async move {
                let did = row.did.clone();
                if let Err(error) = self.check(row).await {
                    tracing::warn!(event = "refract_check_failed", did, %error, "a Refract check could not be written; it is retried after the lease");
                }
            })
            .await;
        Ok(n)
    }

    /// Run §4.1 for one claimed row and write the result.
    ///
    /// # Errors
    ///
    /// Database errors of the write.
    pub async fn check(&self, row: Claimed) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        let mut rules = Rules::default();
        let did = row.did.clone();
        // Rule 1: Bluesky withholds it.
        let (o, d) = rule1(&self.inner.net.bluesky_profile(&did).await);
        rules.set(1, o, d);
        let mut home: Option<RefractCommunity> = None;
        let mut endpoint: Option<String> = None;
        let mut host: Option<String> = None;
        let mut suspect = None;
        let mut status = None;
        let mut fetched = Fetched::Failed;
        let mut tier1: Option<(Outcome, String)> = None;
        if rules.failed().is_none() {
            // Rule 2: hosted, from a fresh resolve.
            let communities = self.communities();
            match self.inner.resolver.fetch_document(&did).await {
                Ok(doc) => {
                    endpoint = extract_info(&doc.doc, &did).pds_endpoint;
                    host = endpoint.as_deref().and_then(pds_host);
                    match host.as_deref().and_then(|h| communities.home(h)) {
                        Some(c) => {
                            home = Some(c.clone());
                            rules.set(2, Outcome::Pass, c.name.clone());
                        }
                        None if host.is_some() && host != row.pds_host => rules.set(2, Outcome::Fail, format!("moved to {}", host.clone().unwrap_or_default())),
                        None => rules.set(2, Outcome::Unknown, "host matches no enabled community"),
                    }
                }
                Err(e) => rules.set(2, Outcome::Unknown, e.to_string()),
            }
        }
        if rules.failed().is_none() {
            // Rule 3: active on its PDS.
            match endpoint.as_deref() {
                Some(ep) => {
                    let prev = row.last_verdict.as_ref().and_then(|v| {
                        let pds = v.pointer("/suspect/pds")?.as_str()?;
                        let since = v.pointer("/suspect/since")?.as_str()?;
                        Some((pds.to_string(), DateTime::parse_from_rfc3339(since).ok()?.with_timezone(&Utc)))
                    });
                    let (o, d, s, st) = rule3(&self.inner.net.repo_status(ep, &did).await, ep, prev.as_ref().map(|(p, t)| (p.as_str(), *t)), now);
                    rules.set(3, o, d);
                    suspect = s;
                    status = st;
                }
                None => rules.set(3, Outcome::Unknown, "no PDS endpoint"),
            }
        }
        if rules.failed().is_none()
            && let Some(c) = home.as_ref()
        {
            // Rule 4's network part.
            let labeler = c.labeler_did.as_deref();
            if c.tier == 1
                && let Some(network) = c.appview_network.as_deref()
            {
                let answer = self.inner.net.community_profile(network, &did, labeler).await;
                tier1 = Some(rule4_appview(&answer));
                if let (Some(l), Some(LabelSource::Appview)) = (labeler, c.label_source) {
                    fetched = self.profile_labels(c, l, &did, &answer).await;
                }
            }
            match (labeler, c.label_source) {
                (Some(l), Some(LabelSource::Labeler)) => fetched = self.targeted_fetch(c, l, &did).await,
                (None, _) => fetched = Fetched::NoLabeler,
                _ => {}
            }
        }
        self.write(row, rules, Written { now, home, host, endpoint, suspect, status, fetched, tier1 }).await
    }

    /// The account and profile-record labels of an AppView `getProfile`
    /// answer (§4.5, `appview` source).
    async fn profile_labels(&self, c: &RefractCommunity, labeler: &str, did: &str, answer: &Answer) -> Fetched {
        let Answer::Reply { status, body, .. } = answer else {
            return Fetched::Failed;
        };
        if !(200..300).contains(status) {
            // A refusal names no labels; the AppView part of rule 4 says
            // what it means.
            return if answer.error() == Some("AccountTakedown") || *status == 404 { Fetched::Profile { account: Vec::new(), profile: Vec::new() } } else { Fetched::Failed };
        }
        if !answer.applied_labeler(labeler) {
            metrics::counter!(super::METRIC_LABEL_READS, "community" => c.name.clone(), "source" => "appview", "outcome" => "error").increment(1);
            return Fetched::Failed;
        }
        let all: Vec<Label> = body.get("labels").and_then(Value::as_array).map(|a| a.iter().filter_map(Label::parse).collect()).unwrap_or_default();
        let checked = self.inner.labelers.check(labeler, &c.name, all, &c.gone_labels, true).await;
        let profile_uri = format!("at://{did}/app.bsky.actor.profile/self");
        let (account, profile): (Vec<_>, Vec<_>) = labels::net_of(checked).into_iter().filter(|(l, _)| l.uri == did || l.uri == profile_uri).partition(|(l, _)| l.uri == did);
        Fetched::Profile { account, profile }
    }

    /// The targeted fetch (§4.5, `labeler` source): every label of the DID
    /// and its records from the home labeler, paged to the end.
    async fn targeted_fetch(&self, c: &RefractCommunity, labeler: &str, did: &str) -> Fetched {
        let Some(doc) = self.inner.labelers.doc(labeler, false).await else {
            return Fetched::Failed;
        };
        let Some(endpoint) = doc.endpoint else {
            return Fetched::Failed;
        };
        let patterns = [did.to_string(), format!("at://{did}/*")];
        let mut cursor: Option<String> = None;
        let mut all = Vec::new();
        for _ in 0..100 {
            let answer = self.inner.net.query_labels(&endpoint, &patterns, labeler, cursor.as_deref()).await;
            let Answer::Reply { status, body, .. } = answer else {
                return Fetched::Failed;
            };
            if !(200..300).contains(&status) {
                return Fetched::Failed;
            }
            let page: Vec<Label> = body.get("labels").and_then(Value::as_array).map(|a| a.iter().filter_map(Label::parse).collect()).unwrap_or_default();
            let empty = page.is_empty();
            all.extend(page);
            match body.get("cursor").and_then(Value::as_str) {
                Some(c) if !empty && cursor.as_deref() != Some(c) => cursor = Some(c.to_string()),
                _ => {
                    let checked = self.inner.labelers.check(labeler, &c.name, all, &c.gone_labels, false).await;
                    return Fetched::All(labels::net_of(checked));
                }
            }
        }
        Fetched::Failed
    }

    /// Write one check's result (§4.3's protocol).
    async fn write(&self, row: Claimed, mut rules: Rules, w: Written) -> Result<(), sqlx::Error> {
        let did = row.did.clone();
        let communities = self.communities();
        let mut sets = self.inner.scope.write_guard().await;
        let mut tx = self.inner.pool.begin().await?;
        let current: Option<(i64, i64)> = sqlx::query_as("SELECT version, due_seq FROM refract_dids WHERE did = $1 FOR UPDATE")
            .bind(&did)
            .fetch_optional(&mut *tx)
            .await?;
        let Some((version, due_seq)) = current.filter(|(v, _)| *v == row.version) else {
            // Someone else moved it (a transition, a config change): this
            // verdict was computed against a stale row and writes nothing.
            tx.rollback().await?;
            return Ok(());
        };
        let state = State::parse(&row.state).unwrap_or(State::Candidate);
        // A community change (rule 2): the old labeler's rows and the
        // record reads go, whatever the verdict.
        let community = w.home.as_ref().map_or_else(|| row.community.clone(), |c| c.name.clone());
        if community != row.community {
            labels::forget(&mut tx, std::slice::from_ref(&did)).await?;
        }
        // The fetched labels, stored before rule 4 reads them.
        let mut labels_ok = false;
        let labeler = w.home.as_ref().and_then(|c| c.labeler_did.clone());
        let gone = w.home.as_ref().map(|c| c.gone_labels.clone()).unwrap_or_default();
        if let Some(l) = labeler.as_deref() {
            match &w.fetched {
                Fetched::All(found) => {
                    labels::replace(&mut tx, l, &did, Replace::All, found, &gone).await?;
                    labels_ok = true;
                }
                Fetched::Profile { account, profile } => {
                    labels::replace(&mut tx, l, &did, Replace::Account, account, &gone).await?;
                    let profile_uri = [format!("at://{did}/app.bsky.actor.profile/self")];
                    labels::replace(&mut tx, l, &did, Replace::Records(&profile_uri), profile, &gone).await?;
                    labels_ok = true;
                }
                Fetched::Failed | Fetched::NoLabeler => {}
            }
        }
        // Rule 4, from the AppView's answer and the stored account labels.
        let mut unverified_since = row.unverified_since;
        if rules.failed().is_none() && w.home.is_some() {
            let mut outcome = w.tier1.clone().unwrap_or((Outcome::Pass, "no AppView part".into()));
            if outcome.0 != Outcome::Fail
                && let Some(l) = labeler.as_deref()
            {
                match labels::account_gone(&mut tx, l, &did).await? {
                    Some(true) => {
                        outcome = (Outcome::Fail, "gone label".into());
                        unverified_since = None;
                    }
                    Some(false) => {
                        outcome = (Outcome::Unknown, "unverified gone label".into());
                        unverified_since = Some(unverified_since.unwrap_or(w.now));
                    }
                    None => {
                        unverified_since = None;
                        if !labels_ok {
                            outcome = (Outcome::Unknown, "labels not read".into());
                        }
                    }
                }
            }
            rules.set(4, outcome.0, outcome.1);
        }
        // Rule 5: no local takedown.
        if rules.failed().is_none() && w.home.is_some() {
            let takedown: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM labels WHERE subject = $1 AND val = '!takedown')")
                .bind(&did)
                .fetch_one(&mut *tx)
                .await?;
            rules.set(5, if takedown { Outcome::Fail } else { Outcome::Pass }, if takedown { "local takedown" } else { "none" });
        }
        let ctx = Context {
            now: w.now,
            recheck: TimeDelta::hours(i64::from(self.inner.cfg.recheck_hours)),
            unverified_max: TimeDelta::hours(i64::from(self.inner.cfg.unverified_max_hours)),
        };
        let mut decision = decide(&row, &rules, unverified_since, ctx);
        // No restore while any purge of the DID runs (§4.3).
        if decision.to == State::Restored && state != State::Restored {
            let purging: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)").bind(&did).fetch_one(&mut *tx).await?;
            if purging {
                decision = Decision { to: state, reason: None, next_check_at: w.now + backoff(row.attempts), attempts: row.attempts + 1 };
            }
        }
        for (i, r) in rules.results.iter().enumerate() {
            if let Some((o, _)) = r {
                metrics::counter!(METRIC_CHECKS, "rule" => (i + 1).to_string(), "verdict" => o.as_str()).increment(1);
            }
        }
        let mut verdict = rules.json(w.now);
        if let Some((pds, since)) = &w.suspect {
            verdict["suspect"] = json!({"pds": pds, "since": since.to_rfc3339()});
        }
        let rejections = row.last_verdict.as_ref().and_then(|v| v.get("rejections")).and_then(Value::as_i64).unwrap_or(0);
        verdict["rejections"] = json!(if decision.to == State::Rejected && state != State::Rejected { rejections + 1 } else { rejections });
        let next_check_at = match decision.to {
            State::Rejected => w.now + rejected_wait(verdict["rejections"].as_i64().unwrap_or(1)),
            _ if due_seq != row.due_seq => w.now,
            _ => decision.next_check_at,
        };
        let transition = decision.to != state;
        // The targeted fetch committed: a DID waiting for its first labels
        // is no longer hidden for that, and its records are fetched.
        let clear_awaiting = row.awaiting_labels_since.is_some() && (labels_ok || labeler.is_none()) && decision.to.in_r();
        let pds_host = if rules.get(2) == Some(Outcome::Pass) { w.host.clone() } else { row.pds_host.clone() };
        sqlx::query(
            "UPDATE refract_dids SET state = $2, version = version + CASE WHEN $3 THEN 1 ELSE 0 END, community = $4, pds_host = $5, \
               checked_at = $6, next_check_at = $7, attempts = $8, last_verdict = $9, unverified_since = $10, \
               awaiting_labels_since = CASE WHEN $11 THEN NULL ELSE awaiting_labels_since END, \
               since = CASE WHEN $3 AND $2 = 'restored' THEN $6 ELSE since END, \
               grace_until = CASE WHEN $2 = 'grace' AND state <> 'grace' THEN $6 + make_interval(days => $12) WHEN $2 = 'grace' THEN grace_until ELSE NULL END, \
               ended_reason = CASE WHEN $2 = 'ending' THEN $13 ELSE NULL END, \
               resync_at = CASE WHEN $3 AND $2 = 'restored' AND state = 'candidate' THEN $6 + make_interval(days => $14) ELSE resync_at END, \
               updated_at = now() \
             WHERE did = $1",
        )
        .bind(&did)
        .bind(decision.to.as_str())
        .bind(transition)
        .bind(&community)
        .bind(&pds_host)
        .bind(w.now)
        .bind(next_check_at)
        .bind(decision.attempts)
        .bind(&verdict)
        .bind(unverified_since)
        .bind(clear_awaiting)
        .bind(i32::try_from(self.inner.cfg.grace_days).unwrap_or(30))
        .bind(&decision.reason)
        .bind(i32::try_from(self.inner.cfg.resync_days).unwrap_or(7))
        .execute(&mut *tx)
        .await?;
        // What a DID entering or in R tells the gate (§4.1 rule 3).
        if decision.to.in_r() || decision.to == State::Ending {
            write_actor(&mut tx, &did, w.endpoint.as_deref(), w.status.as_deref()).await?;
        }
        if clear_awaiting {
            super::enqueue_backfill(&mut tx, &did).await?;
        }
        if transition {
            transition_effects(&mut tx, &did, state, decision.to, decision.reason.as_deref()).await?;
        }
        tx.commit().await?;
        if transition {
            metrics::counter!(METRIC_TRANSITIONS, "from" => state.as_str(), "to" => decision.to.as_str(), "reason" => decision.reason.clone().unwrap_or_default()).increment(1);
            tracing::info!(event = "refract_transition", did, from = state.as_str(), to = decision.to.as_str(), reason = decision.reason.as_deref().unwrap_or(""), "Refract state changed");
        }
        let _ = version;
        super::state::refresh(&self.inner.pool, &mut sets, Some(std::slice::from_ref(&did)), &communities).await?;
        drop(sets);
        Ok(())
    }
}

/// What a check gathered before its write.
struct Written {
    now: DateTime<Utc>,
    home: Option<RefractCommunity>,
    host: Option<String>,
    endpoint: Option<String>,
    suspect: Option<(String, DateTime<Utc>)>,
    status: Option<String>,
    fetched: Fetched,
    tier1: Option<(Outcome, String)>,
}

/// The `actors` facts the gate reads for a DID in R (§4.1 rule 3, §5.5).
async fn write_actor(conn: &mut PgConnection, did: &str, endpoint: Option<&str>, status: Option<&str>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO actors (did, pds_endpoint, account_status, status_checked_at) VALUES ($1, $2, COALESCE($3, 'active'), CASE WHEN $3 IS NULL THEN NULL ELSE now() END) \
         ON CONFLICT (did) DO UPDATE SET pds_endpoint = COALESCE(EXCLUDED.pds_endpoint, actors.pds_endpoint), \
           account_status = COALESCE($3, actors.account_status), status_checked_at = CASE WHEN $3 IS NULL THEN actors.status_checked_at ELSE now() END",
    )
    .bind(did)
    .bind(endpoint)
    .bind(status)
    .execute(conn)
    .await?;
    Ok(())
}

/// A transition's other writes (§4.3 step 3): the event; `→ restored`
/// enqueues the `refract` backfill; `grace → restored` drops the
/// omission rows; `→ ending` cancels the `refract` job only; `→ rejected`
/// drops the labels.
///
/// # Errors
///
/// Database errors.
pub async fn transition_effects(conn: &mut PgConnection, did: &str, from: State, to: State, reason: Option<&str>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO refract_events (did, from_state, to_state, reason) VALUES ($1, $2, $3, $4)")
        .bind(did)
        .bind(from.as_str())
        .bind(to.as_str())
        .bind(reason)
        .execute(&mut *conn)
        .await?;
    match to {
        State::Restored => {
            if from == State::Grace {
                sqlx::query("DELETE FROM refract_labels WHERE did = $1 AND val = $2").bind(did).bind(ABSENT).execute(&mut *conn).await?;
                sqlx::query("UPDATE refract_record_reads SET omissions = 0 WHERE did = $1").bind(did).execute(&mut *conn).await?;
            } else {
                super::enqueue_backfill(conn, did).await?;
            }
        }
        State::Ending => {
            sqlx::query(
                "UPDATE backfill_jobs SET state = 'cancelled', generation = generation + 1, refract_cursor = NULL, updated_at = now() \
                 WHERE did = $1 AND depth = 'refract' AND state IN ('queued', 'running', 'failed', 'unavailable')",
            )
            .bind(did)
            .execute(&mut *conn)
            .await?;
        }
        State::Rejected => labels::forget(conn, &[did.to_string()]).await?,
        State::Candidate | State::Grace => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(results: [Option<Outcome>; 5]) -> Rules {
        let mut r = Rules::default();
        for (i, o) in results.into_iter().enumerate() {
            if let Some(o) = o {
                r.set(i + 1, o, "");
            }
        }
        r
    }

    fn row(state: &str) -> Claimed {
        Claimed {
            did: "did:plc:x".into(),
            state: state.into(),
            version: 1,
            due_seq: 0,
            community: "c".into(),
            pds_host: Some("pds.c.example".into()),
            attempts: 0,
            last_verdict: None,
            grace_until: None,
            unverified_since: None,
            awaiting_labels_since: None,
            candidate_since: Some(Utc::now()),
        }
    }

    use Outcome::{Fail, Grace, Pass, Unknown};

    const ALL: [Option<Outcome>; 5] = [Some(Pass); 5];

    fn ctx() -> Context {
        Context { now: Utc::now(), recheck: TimeDelta::hours(24), unverified_max: TimeDelta::hours(72) }
    }

    /// T-4: every row of §4.3's table.
    #[test]
    fn t4_transition_table() {
        let c = ctx();
        let d = |r: &Claimed, rs: [Option<Outcome>; 5]| decide(r, &rules(rs), r.unverified_since, c);
        // candidate
        assert_eq!(d(&row("candidate"), ALL).to, State::Restored);
        assert_eq!(d(&row("candidate"), [Some(Fail), None, None, None, None]).to, State::Rejected);
        let unknown = [Some(Unknown), Some(Pass), Some(Pass), Some(Pass), Some(Pass)];
        assert_eq!(d(&row("candidate"), unknown).to, State::Candidate);
        assert_eq!(d(&row("candidate"), [Some(Unknown), Some(Pass), Some(Grace), Some(Pass), Some(Pass)]).to, State::Candidate, "grace: retried");
        let mut old = row("candidate");
        old.candidate_since = Some(c.now - TimeDelta::hours(25));
        assert_eq!(d(&old, unknown).to, State::Rejected, "rejected after 24 h");
        // restored
        let r = d(&row("restored"), ALL);
        assert_eq!((r.to, r.next_check_at), (State::Restored, c.now + TimeDelta::hours(24)));
        assert_eq!(d(&row("restored"), [Some(Unknown), Some(Pass), Some(Grace), Some(Unknown), Some(Pass)]).to, State::Grace, "grace outranks unknown");
        let lifted = d(&row("restored"), [Some(Fail), None, None, None, None]);
        assert_eq!((lifted.to, lifted.reason.as_deref()), (State::Ending, Some("bluesky_lifted")));
        let moved = d(&row("restored"), [Some(Pass), Some(Fail), None, None, None]);
        assert_eq!(moved.reason.as_deref(), Some("moved"));
        for n in 3..=5 {
            let mut rs = ALL;
            rs[n - 1] = Some(Fail);
            assert_eq!(d(&row("restored"), rs).reason, Some(format!("rule_{n}")));
        }
        let rule4_unknown = [Some(Pass), Some(Pass), Some(Pass), Some(Unknown), Some(Pass)];
        let r = d(&row("restored"), rule4_unknown);
        assert_eq!((r.to, r.attempts), (State::Restored, 1), "last good state, retried");
        assert_eq!(r.next_check_at, c.now + TimeDelta::minutes(5));
        let mut pinned = row("restored");
        pinned.unverified_since = Some(c.now - TimeDelta::hours(73));
        assert_eq!(d(&pinned, rule4_unknown).reason.as_deref(), Some("unverified_gone_label"));
        let mut awaiting = row("grace");
        awaiting.awaiting_labels_since = Some(c.now - TimeDelta::hours(73));
        assert_eq!(d(&awaiting, rule4_unknown).reason.as_deref(), Some("unverified_gone_label"));
        // grace
        assert_eq!(d(&row("grace"), ALL).to, State::Restored);
        assert_eq!(d(&row("grace"), rule4_unknown).to, State::Grace, "rule 3 passes, an unknown elsewhere");
        assert_eq!(d(&row("grace"), [Some(Unknown), Some(Pass), Some(Grace), Some(Unknown), Some(Pass)]).to, State::Grace);
        let mut expired = row("grace");
        expired.grace_until = Some(c.now - TimeDelta::minutes(1));
        let e = d(&expired, [Some(Unknown), Some(Pass), Some(Grace), Some(Pass), Some(Pass)]);
        assert_eq!((e.to, e.reason.as_deref()), (State::Ending, Some("deactivated")));
        assert_eq!(d(&expired, ALL).to, State::Restored, "back before the end is seen");
        assert_eq!(d(&row("grace"), [Some(Pass), Some(Pass), Some(Fail), None, None]).to, State::Ending);
    }

    #[test]
    fn backoffs() {
        assert_eq!(backoff(0), TimeDelta::minutes(5));
        assert_eq!(backoff(1), TimeDelta::minutes(10));
        assert_eq!(backoff(10), TimeDelta::hours(6));
        assert_eq!(rejected_wait(1), TimeDelta::hours(24));
        assert_eq!(rejected_wait(2), TimeDelta::hours(48));
        assert_eq!(rejected_wait(9), TimeDelta::days(7));
    }

    /// T-4: rule 1 and rule 3's value mappings; T-8f: `RepoNotFound` past
    /// the 24 h suspicion fails rule 3.
    #[test]
    fn rule_value_mappings() {
        let reply = |status: u16, body: Value| Answer::Reply { status, body, headers: Default::default() };
        assert_eq!(rule1(&reply(400, json!({"error": "AccountTakedown"}))).0, Pass);
        assert_eq!(rule1(&reply(400, json!({"error": "InvalidRequest", "message": "Account has been suspended"}))).0, Pass);
        assert_eq!(rule1(&reply(200, json!({"did": "did:plc:x"}))).0, Fail);
        for a in [
            reply(400, json!({"error": "AccountDeactivated"})),
            reply(400, json!({"error": "ProfileNotFound"})),
            reply(404, json!({})),
            reply(403, json!({})),
            reply(502, json!({})),
            Answer::Failed("timeout".into()),
        ] {
            assert_eq!(rule1(&a).0, Unknown, "{a:?}");
        }
        let now = Utc::now();
        let ep = "https://pds.example";
        let status = |s: Value| rule3(&reply(200, s), ep, None, now).0;
        assert_eq!(status(json!({"did": "x", "active": true})), Pass);
        for s in ["takendown", "suspended", "deleted"] {
            assert_eq!(status(json!({"did": "x", "active": false, "status": s})), Fail);
        }
        assert_eq!(status(json!({"did": "x", "active": false, "status": "deactivated"})), Grace);
        assert_eq!(status(json!({"did": "x", "active": false})), Grace, "inactive with no status reads as deactivated");
        for s in ["desynchronized", "throttled", "other"] {
            assert_eq!(status(json!({"did": "x", "active": false, "status": s})), Unknown);
        }
        let nf = reply(400, json!({"error": "RepoNotFound"}));
        let (o, _, suspect, _) = rule3(&nf, ep, None, now);
        assert_eq!(o, Unknown);
        let since = suspect.expect("clock starts").1;
        assert_eq!(rule3(&nf, ep, Some((ep, since)), now + TimeDelta::hours(23)).0, Unknown);
        assert_eq!(rule3(&nf, ep, Some((ep, since)), now + TimeDelta::hours(25)).0, Fail);
        assert_eq!(rule3(&nf, ep, Some(("https://other.example", since)), now + TimeDelta::hours(25)).0, Unknown, "another endpoint restarts it");
        assert_eq!(rule4_appview(&reply(400, json!({"error": "AccountTakedown"}))).0, Fail);
        assert_eq!(rule4_appview(&reply(400, json!({"error": "InvalidRequest", "message": "Profile not found"}))).0, Fail);
        assert_eq!(rule4_appview(&reply(200, json!({"did": "x"}))).0, Pass);
        assert_eq!(rule4_appview(&reply(500, json!({}))).0, Unknown);
    }
}
