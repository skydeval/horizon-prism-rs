//! Private-owner eligibility confirmation (design-v2 §6.11.1).
//!
//! A non-member is eligible when its PDS host matches the `community`
//! network's patterns and that PDS confirms hosting (`getRepoStatus`,
//! `active: true`). Confirmations happen in `private`/`authz` backfill jobs
//! and, when the last one is older than 24 h, on `admin member remove`.
//! - **Definitive answers** set eligibility (confirmed) or remove it (host
//!   outside the patterns, repo not found, inactive, no PDS), and reset the
//!   failure streak.
//! - **Failed confirmations** (network error, 5xx, any other non-answer)
//!   keep eligibility and extend the streak in `actors`. After
//!   [`FAILURES_TO_CLEAR`] consecutive failures spanning at least
//!   [`FAILURE_SPAN_HOURS`], eligibility is removed
//!   (`eligibility_cleared_after_failures`). A short outage therefore never
//!   drops an owner, and a PDS that is gone for good stops being trusted.
//!
//! Every outcome re-evaluates owner status in the same transaction; the
//! in-memory owner set follows after commit.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use sqlx::{PgConnection, PgPool};

use crate::aggregations::ScopeState;
use crate::aggregations::enroll::{host_matches, pds_host};
use crate::firehose::backfill::{BackfillSource, PrepareError};
use crate::private_tier::PrivateTier;

/// Consecutive failed confirmations before eligibility is removed.
pub const FAILURES_TO_CLEAR: i32 = 3;
/// The failures must span at least this long.
pub const FAILURE_SPAN_HOURS: i64 = 24;
/// `admin member remove` re-confirms when the last confirmation is older.
pub const STALE_AFTER_HOURS: i64 = 24;

/// One confirmation attempt's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmation {
    Eligible,
    Ineligible(String),
    Failed(String),
}

impl Confirmation {
    /// Classify a `prepare` result (which ran `getRepoStatus`) against the
    /// community patterns.
    #[must_use]
    pub fn from_prepare(prepared: &Result<String, PrepareError>, patterns: &[String]) -> Self {
        match prepared {
            Ok(pds) if pds_host(pds).is_some_and(|h| patterns.iter().any(|p| host_matches(&h, p))) => Self::Eligible,
            Ok(pds) => Self::Ineligible(format!("PDS {pds} is not a community host")),
            Err(PrepareError::Negative(e)) => Self::Ineligible(e.clone()),
            Err(PrepareError::Failed(e)) => Self::Failed(e.clone()),
        }
    }
}

/// Run a fresh confirmation (network; call before taking any guard).
pub async fn confirm(source: &dyn BackfillSource, patterns: &[String], did: &str) -> Confirmation {
    Confirmation::from_prepare(&source.prepare(did).await, patterns)
}

/// Whether the last confirmation is missing or older than
/// [`STALE_AFTER_HOURS`].
#[must_use]
pub fn is_stale(confirmed_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    confirmed_at.is_none_or(|t| now - t > Duration::hours(STALE_AFTER_HOURS))
}

/// Count one failed confirmation. Returns true when the streak has reached
/// [`FAILURES_TO_CLEAR`] over at least [`FAILURE_SPAN_HOURS`]; the streak is
/// then reset, since eligibility is being removed.
///
/// # Errors
///
/// Database errors.
pub async fn record_failure(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    let (count, spanned): (i32, bool) = sqlx::query_as(
        "INSERT INTO actors (did, eligibility_failure_count, eligibility_failing_since) VALUES ($1, 1, now()) \
         ON CONFLICT (did) DO UPDATE SET \
           eligibility_failure_count = actors.eligibility_failure_count + 1, \
           eligibility_failing_since = COALESCE(actors.eligibility_failing_since, now()) \
         RETURNING eligibility_failure_count, eligibility_failing_since <= now() - make_interval(hours => $2)",
    )
    .bind(did)
    .bind(i32::try_from(FAILURE_SPAN_HOURS).unwrap_or(i32::MAX))
    .fetch_one(&mut *conn)
    .await?;
    let clear = count >= FAILURES_TO_CLEAR && spanned;
    if clear {
        reset(conn, did).await?;
    }
    Ok(clear)
}

/// Reset the failure streak (any definitive answer).
///
/// # Errors
///
/// Database errors.
pub async fn reset(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE actors SET eligibility_failure_count = 0, eligibility_failing_since = NULL \
         WHERE did = $1 AND (eligibility_failure_count <> 0 OR eligibility_failing_since IS NOT NULL)",
    )
    .bind(did)
    .execute(conn)
    .await?;
    Ok(())
}

/// Record a confirmation and re-evaluate owner status: read guard, one
/// transaction, then the in-memory owner set. Returns whether the DID is a
/// private owner afterwards.
///
/// # Errors
///
/// Database errors.
pub async fn apply(
    pool: &PgPool,
    scope: &ScopeState,
    private: &Arc<dyn PrivateTier>,
    did: &str,
    confirmation: &Confirmation,
) -> Result<bool, sqlx::Error> {
    let guard = scope.read_guard().await;
    let mut tx = pool.begin().await?;
    match confirmation {
        Confirmation::Eligible => {
            private.record_eligibility(&mut tx, did, true).await?;
            reset(&mut tx, did).await?;
        }
        Confirmation::Ineligible(reason) => {
            private.record_eligibility(&mut tx, did, false).await?;
            reset(&mut tx, did).await?;
            tracing::info!(event = "eligibility_negative", did, %reason, "owner eligibility not confirmed");
        }
        Confirmation::Failed(error) => {
            if record_failure(&mut tx, did).await? {
                private.record_eligibility(&mut tx, did, false).await?;
                metrics::counter!("eligibility_cleared_total", "reason" => "failures").increment(1);
                tracing::warn!(
                    event = "eligibility_cleared_after_failures",
                    did,
                    %error,
                    failures = FAILURES_TO_CLEAR,
                    span_hours = FAILURE_SPAN_HOURS,
                    "eligibility removed after repeated failed confirmations",
                );
            } else {
                tracing::info!(event = "eligibility_confirmation_failed", did, %error, "eligibility kept; failure counted");
            }
        }
    }
    let applied = private.reevaluate_owner(&mut tx, did).await?;
    tx.commit().await?;
    drop(guard);
    let owner = applied.owners_joined.iter().any(|d| d == did);
    let mut sets = scope.write_guard().await;
    if owner {
        sets.private_owners.insert(did.to_string());
    } else {
        sets.private_owners.remove(did);
    }
    Ok(owner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;
    use crate::private_tier::KryphocronPrivateTier;
    use crate::test_support::fresh_db;

    const DID: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";

    #[test]
    fn classification_and_staleness() {
        let patterns = vec!["*.community.example".to_string()];
        let ok = |p: &str| Ok::<_, PrepareError>(p.to_string());
        assert_eq!(Confirmation::from_prepare(&ok("https://pds1.community.example"), &patterns), Confirmation::Eligible);
        assert!(matches!(Confirmation::from_prepare(&ok("https://bsky.social"), &patterns), Confirmation::Ineligible(_)));
        assert!(matches!(
            Confirmation::from_prepare(&Err(PrepareError::Negative("repo inactive".into())), &patterns),
            Confirmation::Ineligible(_)
        ));
        assert!(matches!(
            Confirmation::from_prepare(&Err(PrepareError::Failed("HTTP 503".into())), &patterns),
            Confirmation::Failed(_)
        ));
        let now = Utc::now();
        assert!(is_stale(None, now));
        assert!(is_stale(Some(now - Duration::hours(25)), now));
        assert!(!is_stale(Some(now - Duration::hours(1)), now));
    }

    #[tokio::test]
    async fn three_failures_over_a_day_clear_and_success_resets() {
        let Some(db) = fresh_db("eligibility_failures").await else {
            return;
        };
        let pool = &db.pool;
        let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
        let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
        let confirmed = |pool: PgPool| {
            let private = Arc::clone(&private);
            async move {
                let mut conn = pool.acquire().await.expect("conn");
                private.eligibility_confirmed_at(&mut conn, DID).await.expect("read").is_some()
            }
        };
        let failed = Confirmation::Failed("HTTP 503".into());
        // Owner status needs a private row (eligibility alone is dropped by
        // re-evaluation otherwise).
        let mut conn = pool.acquire().await.expect("conn");
        private
            .apply(
                &mut conn,
                &crate::private_tier::PrivateOp {
                    uri: format!("at://{DID}/{}/3kpost222222", crate::firehose::classifier::NSID_POST_PRIVATE),
                    author_did: DID.into(),
                    nsid: crate::firehose::classifier::NSID_POST_PRIVATE.into(),
                    rkey: "3kpost222222".into(),
                    cid: "bafy".into(),
                    rev: "3kaaaaaaaaaa2".into(),
                    record: serde_json::json!({"text": "t", "createdAt": "2026-09-26T00:00:00Z"}),
                    source: crate::firehose::writer::WriteSource::Firehose,
                    created_at: chrono::Utc::now(),
                    notify_max_age: chrono::Duration::hours(48),
                },
            )
            .await
            .expect("row");
        drop(conn);

        apply(pool, &scope, &private, DID, &Confirmation::Eligible).await.expect("confirm");
        assert!(confirmed(pool.clone()).await);
        // Three quick failures: counted, eligibility kept (span < 24 h).
        for _ in 0..3 {
            apply(pool, &scope, &private, DID, &failed).await.expect("fail");
        }
        assert!(confirmed(pool.clone()).await, "a short outage keeps eligibility");
        let (count,): (i32,) = sqlx::query_as("SELECT eligibility_failure_count FROM actors WHERE did = $1")
            .bind(DID)
            .fetch_one(pool)
            .await
            .expect("count");
        assert_eq!(count, 3);

        // Success resets the streak.
        apply(pool, &scope, &private, DID, &Confirmation::Eligible).await.expect("confirm");
        let row: (i32, Option<DateTime<Utc>>) =
            sqlx::query_as("SELECT eligibility_failure_count, eligibility_failing_since FROM actors WHERE did = $1")
                .bind(DID)
                .fetch_one(pool)
                .await
                .expect("row");
        assert_eq!(row, (0, None));

        // A success mid-streak restarts it: two failures, a success, then a
        // failure more than a day after the first one does not clear.
        apply(pool, &scope, &private, DID, &failed).await.expect("fail");
        apply(pool, &scope, &private, DID, &failed).await.expect("fail");
        sqlx::query("UPDATE actors SET eligibility_failing_since = now() - interval '25 hours' WHERE did = $1")
            .bind(DID)
            .execute(pool)
            .await
            .expect("age");
        apply(pool, &scope, &private, DID, &Confirmation::Eligible).await.expect("confirm");
        apply(pool, &scope, &private, DID, &failed).await.expect("fail");
        assert!(confirmed(pool.clone()).await, "the streak restarted at the success");
        let (count,): (i32,) = sqlx::query_as("SELECT eligibility_failure_count FROM actors WHERE did = $1")
            .bind(DID)
            .fetch_one(pool)
            .await
            .expect("count");
        assert_eq!(count, 1);
        apply(pool, &scope, &private, DID, &Confirmation::Eligible).await.expect("confirm");

        // Two failures, a day passes, a third: cleared.
        apply(pool, &scope, &private, DID, &failed).await.expect("fail");
        apply(pool, &scope, &private, DID, &failed).await.expect("fail");
        assert!(confirmed(pool.clone()).await);
        sqlx::query("UPDATE actors SET eligibility_failing_since = now() - interval '25 hours' WHERE did = $1")
            .bind(DID)
            .execute(pool)
            .await
            .expect("age");
        apply(pool, &scope, &private, DID, &failed).await.expect("fail");
        assert!(!confirmed(pool.clone()).await, "3 failures over 24 h clear eligibility");

        // A negative answer clears at once.
        apply(pool, &scope, &private, DID, &Confirmation::Eligible).await.expect("confirm");
        apply(pool, &scope, &private, DID, &Confirmation::Ineligible("repo inactive".into())).await.expect("negative");
        assert!(!confirmed(pool.clone()).await);
        db.drop_db().await;
    }
}
