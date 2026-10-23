//! The subscribed-post fan-out (private-activity-subs design §5): after a
//! private post's first store commits, a `subscribed-post` private
//! notification to each subscriber of its author who can read it.
//!
//! - **Enqueue** ([`enqueue`]): the store's transaction writes one queue row
//!   for every first store of a recent private post, whoever subscribes, and
//!   `pg_notify`s at commit. `requestRecord` never waits for the fan-out.
//! - **Worker** ([`Fanout::spawn`]): `LISTEN private_fanout` plus a poll;
//!   at most [`SLOTS`] posts at a time, each claimed with a lease, so a crash
//!   leaves its row due again.
//! - **An attempt** reads the post, its candidates (the public fan-out's
//!   rules), the read path's post-level checks (a fresh watermark, ready
//!   owners, current heads, the post servable to a clean reader, a
//!   `following` audience verified), then per candidate its readiness, its
//!   head and fact 8. A transient failure defers; nothing passes on a
//!   doubt. The passing rows are written under the scope read guard.
//! - **The bound**: no attempt starts `private_notify.subscribed_post_retry_
//!   secs` or more after the first store, and an attempt ends by then. A
//!   subscriber decided once is never decided again.

use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::postgres::PgListener;
use sqlx::{PgConnection, PgPool};
use tokio::task::{JoinHandle, JoinSet};

use super::handlers::PrivateState;
use super::pipeline::{self, PipelineEnv, RequestState};
use super::schema;
use crate::aggregations::ScopeState;
use crate::db::locks::uri_author;

/// The NOTIFY channel a first store signals at commit.
pub(super) const CHANNEL: &str = "private_fanout";
/// The safety-net poll.
pub const POLL: Duration = Duration::from_secs(5);
/// Posts fanned out at once.
pub(super) const SLOTS: usize = 2;
/// The longest one attempt runs: below the claim's 60 s lease.
pub(super) const ATTEMPT_LIMIT: Duration = Duration::from_secs(30);
/// The gaps before the 2nd, 3rd, 4th and every later attempt.
const GAPS_SECS: [i32; 4] = [5, 15, 30, 60];
/// How long a done queue row is kept, so a post re-stored after a delete
/// does not fan out again.
pub(super) const DONE_KEEP_DAYS: i32 = 2;

/// Metric: posts done, by `outcome` (`notified`, `no_candidates`,
/// `post_gone`, `not_member`, `unservable`, `gave_up`, `expired`).
pub const METRIC_FANOUTS: &str = "private_subscribed_post_fanouts_total";
/// Metric: `subscribed-post` rows written.
pub const METRIC_ROWS: &str = "private_subscribed_post_rows_total";
/// Metric: deferrals, by `reason` (`degraded`, `owner_not_ready`, `head`,
/// `not_servable`, `following_unverified`, `subscriber_not_ready`,
/// `subscriber_head`, `time_limit`, `db_error`).
pub const METRIC_DEFERRALS: &str = "private_subscribed_post_deferrals_total";
/// Metric: one attempt's time, a histogram.
pub const METRIC_SECONDS: &str = "private_subscribed_post_fanout_seconds";
/// Gauge: queue rows not done.
pub const METRIC_QUEUE: &str = "private_subscribed_post_queue";
/// Gauge: the age of the oldest queue row not done, in seconds.
pub const METRIC_QUEUE_LAG: &str = "private_subscribed_post_queue_lag_seconds";

/// Queue the post `uri` by `author` for its fan-out (§5.1), on the store's
/// own transaction: due three seconds on, so the firehose can deliver the
/// author's other commits first; a URI already queued (done within two
/// days) is not queued again. The notify is delivered at commit.
///
/// # Errors
///
/// Database errors.
pub(super) async fn enqueue(conn: &mut PgConnection, uri: &str, author: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO private_post_fanout (uri, author_did, not_before) VALUES ($1, $2, now() + interval '3 seconds') \
         ON CONFLICT (uri) DO NOTHING",
    )
    .bind(uri)
    .bind(author)
    .execute(&mut *conn)
    .await?;
    sqlx::query("SELECT pg_notify($1, '')").bind(CHANNEL).execute(&mut *conn).await?;
    Ok(())
}

/// The queue rows not done, and the age of the oldest in seconds (§12's
/// gauges).
///
/// # Errors
///
/// Database errors.
pub(super) async fn queue_state(pool: &PgPool) -> Result<(i64, f64), sqlx::Error> {
    sqlx::query_as(
        "SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(first_at)), 0)::float8 \
         FROM private_post_fanout WHERE done_at IS NULL",
    )
    .fetch_one(pool)
    .await
}

/// A claimed queue row.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct Claimed {
    pub uri: String,
    pub author_did: String,
    pub first_at: DateTime<Utc>,
    pub attempts: i32,
    pub pending: Option<Vec<String>>,
    /// The database's clock at the claim.
    pub claimed_at: DateTime<Utc>,
}

const CLAIMED_COLS: &str = "uri, author_did, first_at, attempts, pending, now() AS claimed_at";

/// How a claimed row's run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Finish {
    /// Done, with the attempt's rows written.
    Done { outcome: &'static str, rows: u64 },
    /// Due again later, with the subscribers still to decide (`None`: none
    /// evaluated yet).
    Retry { rows: u64, pending: Option<Vec<String>> },
    /// The run stopped before it settled the row (tests: a crash).
    #[cfg(test)]
    Crashed,
}

/// What one attempt found (§5.6).
enum Attempt {
    /// Done with no evaluation.
    Done(&'static str),
    /// A post-level check failed.
    PostDeferred(&'static str),
    /// The candidates were evaluated: rows written for those that passed;
    /// `deferred` still to decide.
    Evaluated { rows: u64, deferred: Vec<String> },
}

/// What an attempt has decided so far, for an attempt cut short.
#[derive(Debug, Default)]
struct Progress {
    candidates: Option<Vec<String>>,
    refused: Vec<String>,
    written: Vec<String>,
}

/// The fan-out worker.
#[derive(Clone)]
pub(crate) struct Fanout {
    env: PipelineEnv,
    scope: ScopeState,
    /// `private_notify.subscribed_post_retry_secs`.
    bound: Duration,
    poll: Duration,
    attempt_limit: Duration,
}

impl Fanout {
    /// The worker for the private router's `state` (its pipeline's pool,
    /// prober, watermarks and bounds) and the scope.
    #[must_use]
    pub(crate) fn new(state: &PrivateState, scope: ScopeState, bound_secs: u32) -> Self {
        Self {
            env: state.env.clone(),
            scope,
            bound: Duration::from_secs(u64::from(bound_secs)),
            poll: POLL,
            attempt_limit: ATTEMPT_LIMIT,
        }
    }

    /// Another poll (tests: none to speak of, so only a notify wakes it).
    #[cfg(test)]
    #[must_use]
    pub(super) fn with_poll(mut self, poll: Duration) -> Self {
        self.poll = poll;
        self
    }

    /// Another attempt limit (tests).
    #[cfg(test)]
    #[must_use]
    pub(super) fn with_attempt_limit(mut self, limit: Duration) -> Self {
        self.attempt_limit = limit;
        self
    }

    /// Listen, poll and run due posts, [`SLOTS`] at a time, forever.
    #[must_use]
    pub(crate) fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self) {
        let mut listener = match PgListener::connect_with(&self.env.pool).await {
            Ok(mut l) => match l.listen(CHANNEL).await {
                Ok(()) => Some(l),
                Err(error) => {
                    tracing::warn!(event = "private_fanout_listen_failed", %error, "LISTEN failed; polling only");
                    None
                }
            },
            Err(error) => {
                tracing::warn!(event = "private_fanout_listen_failed", %error, "LISTEN connection failed; polling only");
                None
            }
        };
        let mut running: JoinSet<()> = JoinSet::new();
        loop {
            let free = SLOTS.saturating_sub(running.len());
            if free > 0 {
                match self.claim_due(free).await {
                    Ok(rows) => {
                        for c in rows {
                            let me = self.clone();
                            running.spawn(async move {
                                if let Err(error) = me.run_claimed(c).await {
                                    tracing::warn!(event = "private_fanout_failed", %error, "a subscribed-post fan-out failed on the database; its row is due again after the lease");
                                }
                            });
                        }
                    }
                    Err(error) => tracing::warn!(event = "private_fanout_claim_failed", %error, "claiming subscribed-post fan-outs failed"),
                }
            }
            // The next due row, the poll at the latest.
            let wait = match self.next_due().await {
                Ok(Some(secs)) => Duration::from_secs_f64(secs.clamp(0.1, self.poll.as_secs_f64())),
                Ok(None) => self.poll,
                Err(_) => self.poll,
            };
            tokio::select! {
                n = recv(listener.as_mut()) => {
                    if let Err(error) = n {
                        tracing::debug!(event = "private_fanout_listen_error", %error, "listener error; it reconnects");
                    }
                }
                () = tokio::time::sleep(wait) => {}
                Some(_) = running.join_next(), if !running.is_empty() => {}
            }
        }
    }

    /// Seconds until the soonest row not done is due; `None` with none.
    async fn next_due(&self) -> Result<Option<f64>, sqlx::Error> {
        sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM min(not_before) - now())::float8 FROM private_post_fanout WHERE done_at IS NULL")
            .fetch_one(&self.env.pool)
            .await
    }

    /// Claim up to `slots` due rows (§5.2), each leased for 60 s.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(super) async fn claim_due(&self, slots: usize) -> Result<Vec<Claimed>, sqlx::Error> {
        sqlx::query_as(&format!(
            "UPDATE private_post_fanout SET not_before = now() + interval '60 seconds', attempts = attempts + 1 \
             WHERE uri IN (SELECT uri FROM private_post_fanout WHERE done_at IS NULL AND not_before <= now() \
                           ORDER BY not_before LIMIT $1 FOR UPDATE SKIP LOCKED) \
             RETURNING {CLAIMED_COLS}"
        ))
        .bind(i64::try_from(slots).unwrap_or(i64::MAX))
        .fetch_all(&self.env.pool)
        .await
    }

    /// Tests: claim the row of `uri` whenever it is due, and run it.
    #[cfg(test)]
    pub(super) async fn run_uri(&self, uri: &str) -> Result<Option<Finish>, sqlx::Error> {
        let claimed: Option<Claimed> = sqlx::query_as(&format!(
            "UPDATE private_post_fanout SET not_before = now() + interval '60 seconds', attempts = attempts + 1 \
             WHERE uri = $1 AND done_at IS NULL RETURNING {CLAIMED_COLS}"
        ))
        .bind(uri)
        .fetch_optional(&self.env.pool)
        .await?;
        match claimed {
            Some(c) => Ok(Some(self.run_claimed(c).await?)),
            None => Ok(None),
        }
    }

    /// Run one claimed row and settle it (§5.2, §5.6).
    ///
    /// # Errors
    ///
    /// Database errors settling the row; the lease then makes it due again.
    pub(super) async fn run_claimed(&self, c: Claimed) -> Result<Finish, sqlx::Error> {
        #[cfg(test)]
        if crate::test_support::hooks::injected(crate::test_support::hooks::FANOUT_CRASH, &c.uri) {
            return Ok(Finish::Crashed);
        }
        let deadline = c.first_at + chrono::Duration::seconds(i64::try_from(self.bound.as_secs()).unwrap_or(i64::from(u32::MAX)));
        // No attempt starts at or past the bound, a first one included.
        if c.claimed_at >= deadline {
            self.settle_done(&c.uri, c.pending.as_deref()).await?;
            return Ok(done("expired", 0));
        }
        let left = (deadline - c.claimed_at).to_std().unwrap_or(Duration::ZERO);
        let limit = self.attempt_limit.min(left);
        let progress = Mutex::new(Progress::default());
        let start = Instant::now();
        let result = tokio::time::timeout(limit, self.attempt(&c, &progress)).await;
        metrics::histogram!(METRIC_SECONDS).record(start.elapsed().as_secs_f64());
        let (rows, pending) = match result {
            Ok(Ok(Attempt::Done(outcome))) => {
                self.settle_done(&c.uri, c.pending.as_deref()).await?;
                return Ok(done(outcome, 0));
            }
            Ok(Ok(Attempt::Evaluated { rows, deferred })) if deferred.is_empty() => {
                self.settle_done(&c.uri, Some(&[])).await?;
                return Ok(done("notified", rows));
            }
            Ok(Ok(Attempt::Evaluated { rows, deferred })) => (rows, Some(deferred)),
            Ok(Ok(Attempt::PostDeferred(reason))) => {
                defer(reason, 1);
                (0, c.pending.clone())
            }
            Ok(Err(error)) => {
                tracing::warn!(event = "private_fanout_db_error", uri = %c.uri, %error, "a subscribed-post attempt failed on the database; deferred");
                defer("db_error", 1);
                (0, after_cut(&c, &progress))
            }
            Err(_) => {
                defer("time_limit", 1);
                (0, after_cut(&c, &progress))
            }
        };
        self.settle_retry(&c, rows, pending).await
    }

    /// Done: the row is kept two days, so the post fans out once.
    async fn settle_done(&self, uri: &str, pending: Option<&[String]>) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE private_post_fanout SET done_at = now(), pending = $2 WHERE uri = $1")
            .bind(uri)
            .bind(pending)
            .execute(&self.env.pool)
            .await?;
        Ok(())
    }

    /// A retry after the attempt's gap, unless it would start at or past the
    /// bound: then the post is done, `gave_up` (§5.6).
    async fn settle_retry(&self, c: &Claimed, rows: u64, pending: Option<Vec<String>>) -> Result<Finish, sqlx::Error> {
        let gap = GAPS_SECS[usize::try_from(c.attempts.max(1) - 1).unwrap_or(0).min(GAPS_SECS.len() - 1)];
        let bound = i32::try_from(self.bound.as_secs()).unwrap_or(i32::MAX);
        let scheduled = sqlx::query(
            "UPDATE private_post_fanout SET not_before = now() + make_interval(secs => $2), pending = $3 \
             WHERE uri = $1 AND now() + make_interval(secs => $2) < first_at + make_interval(secs => $4)",
        )
        .bind(&c.uri)
        .bind(f64::from(gap))
        .bind(pending.as_deref())
        .bind(f64::from(bound))
        .execute(&self.env.pool)
        .await?
        .rows_affected();
        if scheduled > 0 {
            return Ok(Finish::Retry { rows, pending });
        }
        self.settle_done(&c.uri, pending.as_deref()).await?;
        let undecided = pending.as_ref().map(Vec::len);
        tracing::info!(event = "private_fanout_gave_up", uri = %c.uri, ?undecided, "a subscribed-post fan-out reached its bound with subscribers undecided");
        Ok(done("gave_up", rows))
    }

    /// One attempt (§5.3–§5.5).
    async fn attempt(&self, c: &Claimed, progress: &Mutex<Progress>) -> Result<Attempt, sqlx::Error> {
        let env = &self.env;
        let (uri, author) = (c.uri.as_str(), c.author_did.as_str());
        // Step 0, the post.
        let Some(post) = schema::fanout_post(&env.pool, uri).await? else {
            return Ok(Attempt::Done("post_gone"));
        };
        if post.author_did != author {
            return Ok(Attempt::Done("post_gone"));
        }
        if !post.member {
            return Ok(Attempt::Done("not_member"));
        }
        if env.plaintext_refused && !post.encoded {
            return Ok(Attempt::Done("unservable"));
        }
        // Step 1, the candidates.
        let candidates = schema::fanout_candidates(&env.pool, author, uri, &post, c.pending.as_deref()).await?;
        if candidates.is_empty() {
            return Ok(Attempt::Done("no_candidates"));
        }
        lock(progress).candidates = Some(candidates.iter().map(|(d, _)| d.clone()).collect());
        // Post level: any failure defers the whole post.
        if !env.watermark_fresh() {
            return Ok(Attempt::PostDeferred("degraded"));
        }
        let audience_owner = post.audience_uri.as_deref().and_then(uri_author);
        if !pipeline::ready(env, author, audience_owner).await? {
            return Ok(Attempt::PostDeferred("owner_not_ready"));
        }
        let rs = RequestState::new(env);
        let mut heads = vec![author];
        if let Some(a) = audience_owner.filter(|a| *a != author) {
            heads.push(a);
        }
        for did in heads {
            if !pipeline::head_ok(env, &rs, did).await? {
                return Ok(Attempt::PostDeferred("head"));
            }
        }
        let served = schema::not_served_rows(&env.pool, author, &[uri.to_string()], env.max_chain)
            .await?
            .get(uri)
            .is_some_and(|r| r.served);
        if !served {
            return Ok(Attempt::PostDeferred("not_servable"));
        }
        if !schema::following_ready(&env.pool, uri, env.max_chain).await? {
            return Ok(Attempt::PostDeferred("following_unverified"));
        }
        #[cfg(test)]
        crate::test_support::hooks::pause(crate::test_support::hooks::FANOUT_BEFORE_VERDICT, uri).await;
        // Subscriber level.
        let mut passed: Vec<String> = Vec::new();
        let mut deferred: Vec<String> = Vec::new();
        for (did, ready) in candidates {
            if !ready {
                defer("subscriber_not_ready", 1);
                deferred.push(did);
                continue;
            }
            if !pipeline::head_ok(env, &rs, &did).await? {
                defer("subscriber_head", 1);
                deferred.push(did);
                continue;
            }
            if schema::fact8(&env.pool, &did, uri, env.max_chain).await?.verdict {
                passed.push(did);
            } else {
                lock(progress).refused.push(did);
            }
        }
        #[cfg(test)]
        crate::test_support::hooks::pause(crate::test_support::hooks::FANOUT_BEFORE_WRITE, uri).await;
        let rows = if passed.is_empty() { 0 } else { self.write(author, uri, &passed).await? };
        lock(progress).written = passed;
        if rows > 0 {
            metrics::counter!(METRIC_ROWS).increment(rows);
        }
        Ok(Attempt::Evaluated { rows, deferred })
    }

    /// The passing subscribers' rows (§5.5), under the scope read guard, so
    /// a `member_removed` or `account_deleted` marker cannot commit inside
    /// the transaction and its purge sees every row written.
    async fn write(&self, author: &str, uri: &str, passed: &[String]) -> Result<u64, sqlx::Error> {
        let guard = self.scope.read_guard().await;
        let mut tx = self.env.pool.begin().await?;
        let rows = schema::fanout_insert(&mut tx, author, uri, passed).await?;
        tx.commit().await?;
        drop(guard);
        Ok(rows)
    }
}

/// The subscribers an attempt cut short leaves to decide: those it had not
/// decided (written or refused), or the row's own when it decided none.
fn after_cut(c: &Claimed, progress: &Mutex<Progress>) -> Option<Vec<String>> {
    let p = lock(progress);
    if p.refused.is_empty() && p.written.is_empty() {
        return c.pending.clone();
    }
    let decided: HashSet<&String> = p.refused.iter().chain(&p.written).collect();
    let all = p.candidates.clone().or_else(|| c.pending.clone()).unwrap_or_default();
    Some(all.into_iter().filter(|d| !decided.contains(d)).collect())
}

fn lock(progress: &Mutex<Progress>) -> std::sync::MutexGuard<'_, Progress> {
    progress.lock().unwrap_or_else(PoisonError::into_inner)
}

fn done(outcome: &'static str, rows: u64) -> Finish {
    metrics::counter!(METRIC_FANOUTS, "outcome" => outcome).increment(1);
    Finish::Done { outcome, rows }
}

fn defer(reason: &'static str, n: u64) {
    metrics::counter!(METRIC_DEFERRALS, "reason" => reason).increment(n);
}

/// The listener's next notification; never, with none.
async fn recv(listener: Option<&mut PgListener>) -> Result<(), sqlx::Error> {
    match listener {
        Some(l) => l.recv().await.map(|_| ()),
        None => std::future::pending().await,
    }
}

/// A queue row's state (tests).
#[cfg(test)]
pub(super) async fn queue_row(pool: &PgPool, uri: &str) -> Option<(i32, Option<Vec<String>>, bool)> {
    sqlx::query_as("SELECT attempts, pending, done_at IS NOT NULL FROM private_post_fanout WHERE uri = $1")
        .bind(uri)
        .fetch_optional(pool)
        .await
        .expect("queue row")
}
