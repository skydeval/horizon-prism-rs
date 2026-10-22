//! Non-commit events (design-v2 §5.6).
//!
//! Identity, account, handle and tombstone events carry no rev, so they are
//! not applied directly. If the DID is in `scope_dids` or `members`, or has an
//! `actors` row, or a Refract row (refract design §4.4), the worker upserts `identity_refresh` with a 30 s debounce,
//! OR-ing the reason bit into any pending row. Otherwise the event is ignored.

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

/// Route one event. Returns whether a refresh was queued.
///
/// # Errors
///
/// Any database error (transient for the worker).
pub async fn route_identity(pool: &PgPool, kind: IdentityKind, did: &str) -> Result<bool, sqlx::Error> {
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
    tracing::debug!(event = "firehose_identity_event", r#type = kind.as_str(), did, queued, "non-commit event routed");
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
        assert!(!route_identity(&db.pool, IdentityKind::Handle, "did:plc:stranger").await.expect("route"));
        // A Refract candidate's events are routed too (refract design §4.4).
        sqlx::query("INSERT INTO refract_dids (did, community, state, source) VALUES ('did:plc:candidate', 'community-a', 'candidate', 'fill')")
            .execute(&db.pool)
            .await
            .expect("candidate");
        assert!(route_identity(&db.pool, IdentityKind::Account, "did:plc:candidate").await.expect("route"));
        assert!(route_identity(&db.pool, IdentityKind::Handle, known).await.expect("route"));
        assert!(route_identity(&db.pool, IdentityKind::Account, known).await.expect("route"));
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
}
