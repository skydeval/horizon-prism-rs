//! Non-commit events (design-v2 §5.6).
//!
//! Identity, account, handle and tombstone events carry no rev, so they are
//! not applied directly. If the DID is in `scope_dids` or `members`, or has an
//! `actors` row, or a Refract row (refract design §4.4), the worker upserts `identity_refresh` with a 30 s debounce,
//! OR-ing the reason bit into any pending row. A DID that owns a system-account
//! list, or (with the key) is on one, is queued the same way (system-accounts
//! design §6), without counting as queued: the live feed and the log keep
//! saying only whether the main tables track it. Otherwise the event is ignored.

use sqlx::PgPool;

use super::stream::IdentityKind;

/// Debounce for duplicate deliveries from several relays (§5.6).
pub const DEBOUNCE_SECS: i32 = 30;

impl IdentityKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Account => "account",
            Self::Handle => "handle",
            Self::Tombstone => "tombstone",
        }
    }
}

/// Route one event. Returns whether a refresh was queued for a DID the main
/// tables track; `system_accounts` is the feature's keys (`None` while off).
///
/// # Errors
///
/// Any database error (transient for the worker).
pub async fn route_identity(
    pool: &PgPool,
    kind: IdentityKind,
    did: &str,
    system_accounts: Option<&crate::system_accounts::Keys>,
) -> Result<bool, sqlx::Error> {
    let queued = sqlx::query(
        "INSERT INTO identity_refresh (did, reasons, not_before) \
         SELECT $1, $2, now() + make_interval(secs => $3) \
         WHERE EXISTS (SELECT 1 FROM scope_dids WHERE did = $1) \
            OR EXISTS (SELECT 1 FROM members WHERE did = $1) \
            OR EXISTS (SELECT 1 FROM actors WHERE did = $1) \
            OR EXISTS (SELECT 1 FROM refract_dids WHERE did = $1) \
         ON CONFLICT (did) DO UPDATE SET reasons = identity_refresh.reasons | EXCLUDED.reasons",
    )
    .bind(did)
    .bind(kind.reason_bit())
    .bind(DEBOUNCE_SECS)
    .execute(pool)
    .await?
    .rows_affected()
        > 0;
    // A system-accounts owner or listed account: its deletion must reach
    // the lists. Queued, but not reported as queued.
    if !queued && crate::system_accounts::holds(pool, system_accounts, did).await? {
        sqlx::query(
            "INSERT INTO identity_refresh (did, reasons, not_before) VALUES ($1, $2, now() + make_interval(secs => $3)) \
             ON CONFLICT (did) DO UPDATE SET reasons = identity_refresh.reasons | EXCLUDED.reasons",
        )
        .bind(did)
        .bind(kind.reason_bit())
        .bind(DEBOUNCE_SECS)
        .execute(pool)
        .await?;
    }
    // A DID with plural records or a plural repo row is listed again soon
    // (plural-pronouns design §2.4, R3), so a deactivation or a deletion
    // reaches its answers and a deleted account's records go.
    let plural = crate::plural::fill::account_event(pool, did).await?;
    tracing::debug!(event = "firehose_identity_event", r#type = kind.as_str(), did, queued, plural, "non-commit event routed");
    Ok(queued)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::refresher::RefreshReason;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn known_dids_are_queued_and_reasons_accumulate() {
        let Some(db) = fresh_db("known_dids_are_queued_and_reasons_accumulate").await else {
            return;
        };
        let known = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        sqlx::query("INSERT INTO actors (did) VALUES ($1)")
            .bind(known)
            .execute(&db.pool)
            .await
            .expect("actor");
        assert!(!route_identity(&db.pool, IdentityKind::Handle, "did:plc:stranger", None).await.expect("route"));
        // A Refract candidate's events are routed too (refract design §4.4).
        sqlx::query("INSERT INTO refract_dids (did, community, state, source) VALUES ('did:plc:candidate', 'community-a', 'candidate', 'fill')")
            .execute(&db.pool)
            .await
            .expect("candidate");
        assert!(route_identity(&db.pool, IdentityKind::Account, "did:plc:candidate", None).await.expect("route"));
        assert!(route_identity(&db.pool, IdentityKind::Handle, known, None).await.expect("route"));
        assert!(route_identity(&db.pool, IdentityKind::Account, known, None).await.expect("route"));
        let (reasons, delay): (i16, f64) = sqlx::query_as(
            "SELECT reasons, extract(epoch FROM not_before - now())::float8 FROM identity_refresh WHERE did = $1",
        )
        .bind(known)
        .fetch_one(&db.pool)
        .await
        .expect("row");
        assert_eq!(reasons, RefreshReason::HANDLE | RefreshReason::ACCOUNT);
        assert!(delay > 20.0 && delay <= 30.0, "{delay}");
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM identity_refresh")
            .fetch_one(&db.pool)
            .await
            .expect("count");
        assert_eq!(rows, 2);
        db.drop_db().await;
    }

    /// T-P9 (plural-pronouns design §2.4, R3): an account event lists a DID
    /// with plural rows again in 30 s; one with only stored records gets a
    /// pending row; any other DID writes nothing.
    #[tokio::test]
    async fn account_events_reach_plural_dids() {
        let Some(db) = fresh_db("account_events_reach_plural_dids").await else {
            return;
        };
        let p = &db.pool;
        sqlx::query("INSERT INTO plural_repos (did, state, verified_at) VALUES ('did:plc:complete', 'complete', now())").execute(p).await.expect("repo");
        sqlx::query(
            "INSERT INTO plural_records (uri, did, rkey, cid, rev, collection, record) \
             VALUES ('at://did:plc:recordonly/app.nearhorizon.plural.member/3k', 'did:plc:recordonly', '3k', 'c', 'r', 'app.nearhorizon.plural.member', '{}')",
        )
        .execute(p)
        .await
        .expect("record");
        for did in ["did:plc:complete", "did:plc:recordonly", "did:plc:nobody"] {
            route_identity(p, IdentityKind::Account, did, None).await.expect("route");
        }
        let rows: Vec<(String, String, f64)> =
            sqlx::query_as("SELECT did, state, extract(epoch FROM next_fill_at - now())::float8 FROM plural_repos ORDER BY did").fetch_all(p).await.expect("rows");
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!((rows[0].0.as_str(), rows[0].1.as_str()), ("did:plc:complete", "complete"));
        assert_eq!((rows[1].0.as_str(), rows[1].1.as_str()), ("did:plc:recordonly", "pending"));
        assert!(rows.iter().all(|r| r.2 > 20.0 && r.2 <= 30.0), "{rows:?}");
        db.drop_db().await;
    }
}
