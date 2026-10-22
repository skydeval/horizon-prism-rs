//! Adding members (design-v2 §9.2, §6.11.1): shared by `admin member add`
//! (source `admin`) and auto-enroll (source `auto_enroll`), and the
//! operator's denials (membership design §2.2, §5).
//!
//! Under the `ScopeState` write guard and the DID's shared lock: refuse a
//! denied DID, insert the member, put it in S (reason bit 1), enqueue a
//! `member` backfill, and re-evaluate owner status (enqueueing `private` if
//! it becomes an owner). The in-memory sets follow after commit. No network
//! call is made here.
//!
//! **Invariant D** (membership design §2.2): no committed state has a DID
//! both in `members` and in `member_denials`. [`add_member`] reads the
//! denial in its own transaction under the write guard; `member deny`
//! writes the denial in the purge marker's transaction, which deletes the
//! `members` row under the same guard (`MarkerGuard::Deny`).

use std::sync::Arc;

use sqlx::{PgConnection, PgPool};

use super::ScopeState;
use super::policy::REASON_MEMBER;
use crate::db::locks::lock_dids_shared;
use crate::firehose::jobs::{self, Depth};
use crate::private_tier::PrivateTier;
use crate::subscriptions::references::{Budget, adopt_member};

/// What adding a member did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddOutcome {
    Added { became_owner: bool },
    AlreadyMember,
    /// A purge of the DID is running (§6.11.7): try again after it ends.
    Purging,
    /// The DID is denied: nothing is inserted until `admin member allow`.
    Denied,
}

/// A denial to write (membership design §2.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    pub did: String,
    /// The operator's note, never shown to the DID.
    pub reason: Option<String>,
    /// `cli`, or an operator DID.
    pub denied_by: String,
}

/// Whether `did` is denied.
///
/// # Errors
///
/// Database errors.
pub async fn is_denied(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM member_denials WHERE did = $1)")
        .bind(did)
        .fetch_one(conn)
        .await
}

/// Write the denial, keeping `denied_at` and `denied_by` of an existing one
/// and replacing its `reason` when one is given (membership design §5.2).
/// Runs inside the purge marker's transaction.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn upsert_denial(conn: &mut PgConnection, denial: &Denial) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO member_denials (did, reason, denied_by) VALUES ($1, $2, $3) \
         ON CONFLICT (did) DO UPDATE SET reason = COALESCE(EXCLUDED.reason, member_denials.reason)",
    )
    .bind(&denial.did)
    .bind(&denial.reason)
    .bind(&denial.denied_by)
    .execute(conn)
    .await?;
    Ok(())
}

/// Delete the denial of `did` (`admin member allow`). Adds nothing. Returns
/// whether a denial was there.
///
/// # Errors
///
/// Database errors.
pub async fn allow(pool: &PgPool, did: &str) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query("DELETE FROM member_denials WHERE did = $1")
        .bind(did)
        .execute(pool)
        .await?
        .rows_affected()
        > 0)
}

/// Add `did` as a member.
///
/// # Errors
///
/// Database errors (nothing is changed).
pub async fn add_member(
    pool: &PgPool,
    scope: &ScopeState,
    private: &Arc<dyn PrivateTier>,
    did: &str,
    source: &str,
    list_budget: Budget,
) -> Result<AddOutcome, sqlx::Error> {
    let mut sets = scope.write_guard().await;
    if sets.purge_kind(did).is_some() {
        return Ok(AddOutcome::Purging);
    }
    let mut tx = pool.begin().await?;
    let purging: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)")
        .bind(did)
        .fetch_one(&mut *tx)
        .await?;
    if purging {
        return Ok(AddOutcome::Purging);
    }
    // Invariant D: under the write guard, which `member deny`'s marker
    // also holds, so no denial commits between this read and the insert.
    if is_denied(&mut tx, did).await? {
        return Ok(AddOutcome::Denied);
    }
    lock_dids_shared(&mut tx, &[did.to_string()]).await?;
    let inserted = sqlx::query("INSERT INTO members (did, source) VALUES ($1, $2) ON CONFLICT (did) DO NOTHING")
        .bind(did)
        .bind(source)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        > 0;
    if !inserted {
        return Ok(AddOutcome::AlreadyMember);
    }
    // Rule 4 (design-v3 §4.3): the list mutes and list blocks the new
    // member already has stored become its references. The list phase
    // picks the markers up at its next pass.
    adopt_member(&mut tx, did, list_budget).await?;
    sqlx::query(
        "INSERT INTO scope_dids (did, reasons) VALUES ($1, $2) \
         ON CONFLICT (did) DO UPDATE SET reasons = scope_dids.reasons | EXCLUDED.reasons, justified_at = now()",
    )
    .bind(did)
    .bind(REASON_MEMBER)
    .execute(&mut *tx)
    .await?;
    jobs::enqueue(&mut tx, did, Depth::Member).await?;
    let applied = private.reevaluate_owner(&mut tx, did).await?;
    let became_owner = applied.owners_joined.iter().any(|d| d == did);
    if became_owner {
        jobs::enqueue(&mut tx, did, Depth::Private).await?;
    }
    let follows: i64 = sqlx::query_scalar("SELECT count(*) FROM follows WHERE author_did = $1")
        .bind(did)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;

    sets.members.insert(did.to_string());
    sets.scope.insert(did.to_string());
    sets.member_follows.insert(did.to_string(), u32::try_from(follows).unwrap_or(u32::MAX));
    for d in applied.owners_joined {
        sets.private_owners.insert(d);
    }
    for d in &applied.owners_left {
        sets.private_owners.remove(d);
    }
    Ok(AddOutcome::Added { became_owner })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::aggregations::enroll::{AutoEnroller, Enrollment, METRIC_REFUSED, REFUSED_DENIED};
    use crate::aggregations::purge::PurgeError;
    use crate::aggregations::{MarkerGuard, MarkerOutcome, PurgeKind, Purger, ScopeSets};
    use crate::config::{AppConfig, ScopePolicy};
    use crate::identity::{IdentityResolver, IdentitySettings};
    use crate::net::{Intent, Transports};
    use crate::private_tier::KryphocronPrivateTier;
    use crate::test_support::{fresh_db, series};

    const M: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const N: &str = "did:plc:nnnnnnnnnnnnnnnnnnnnnnnn";
    const BUDGET: Budget = Budget { max_references: 25, max_owners: 10 };

    struct T {
        pool: PgPool,
        scope: ScopeState,
        private: Arc<dyn PrivateTier>,
        purger: Purger,
    }

    impl T {
        fn new(pool: &PgPool) -> Self {
            let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
            let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
            let purger = Purger::new(pool.clone(), scope.clone(), Arc::clone(&private));
            Self { pool: pool.clone(), scope, private, purger }
        }

        async fn add(&self, did: &str) -> AddOutcome {
            add_member(&self.pool, &self.scope, &self.private, did, "admin", BUDGET).await.expect("add")
        }

        fn deny(did: &str, reason: Option<&str>) -> MarkerGuard {
            MarkerGuard::Deny(Denial { did: did.into(), reason: reason.map(str::to_string), denied_by: "cli".into() })
        }

        async fn count(&self, sql: &str) -> i64 {
            sqlx::query_scalar(sql).fetch_one(&self.pool).await.expect(sql)
        }

        async fn member(&self, did: &str) -> bool {
            self.count(&format!("SELECT count(*) FROM members WHERE did = '{did}'")).await == 1
        }

        async fn denied(&self, did: &str) -> bool {
            self.count(&format!("SELECT count(*) FROM member_denials WHERE did = '{did}'")).await == 1
        }

        async fn purging(&self, did: &str) -> bool {
            self.count(&format!("SELECT count(*) FROM purging_dids WHERE did = '{did}'")).await == 1
        }

        fn enroller(&self) -> AutoEnroller {
            self.enroller_with("[community]\nauto_enroll = true\n")
        }

        fn enroller_with(&self, toml: &str) -> AutoEnroller {
            let lookup = |var: &str| -> Option<String> {
                match var {
                    "APP_PUBLIC_HOSTNAME" => Some("prism.example.com".to_string()),
                    "APP_DATABASE_URL" => Some("postgres://prism@db:5432/prism".to_string()),
                    "APP_REDIS_URL" => Some("redis://redis:6379".to_string()),
                    "APP_SERVICE_SIGNING_KEY_HEX" | "APP_CORRELATION_KEY_HEX" | "APP_PRIVATE_CURSOR_KEY_HEX" => Some("07".repeat(32)),
                    _ => None,
                }
            };
            let cfg = AppConfig::from_parts(toml, &lookup).expect("config");
            let resolver = IdentityResolver::new(
                Transports::loopback_for_tests(),
                self.pool.clone(),
                IdentitySettings::from_app(&cfg),
                Arc::new(self.scope.clone()),
                Intent::Internal,
            );
            AutoEnroller::new(&cfg, self.pool.clone(), self.scope.clone(), Arc::clone(&self.private), resolver)
        }
    }

    /// T-M1 (§2.2, §5.2): `member deny` of a member is one marker
    /// transaction that deletes `members` and writes the denial; after the
    /// purge end, auto-enroll refuses `denied` (counted, never cached) with
    /// nothing inserted, before any network check.
    #[tokio::test]
    async fn t_m1_deny_of_a_member() {
        let Some(db) = fresh_db("membership_t_m1").await else {
            return;
        };
        let t = T::new(&db.pool);
        assert_eq!(t.add(M).await, AddOutcome::Added { became_owner: false });
        let outcome = t.purger.marker_guarded(M, PurgeKind::MemberRemoved, &T::deny(M, Some("spam"))).await.expect("marker");
        assert_eq!(outcome, MarkerOutcome::Marked);
        assert!(!t.member(M).await && t.denied(M).await && t.purging(M).await, "one transaction");
        let snap = t.scope.snapshot().await;
        assert!(!snap.is_member(M));
        assert_eq!(snap.purge_kind(M), Some(PurgeKind::MemberRemoved));
        let row: (Option<String>, String) =
            sqlx::query_as("SELECT reason, denied_by FROM member_denials WHERE did = $1").bind(M).fetch_one(&db.pool).await.expect("denial");
        assert_eq!(row, (Some("spam".to_string()), "cli".to_string()));
        // While the purge runs, the enroller answers `Purging`.
        let enroller = t.enroller();
        assert_eq!(enroller.check(M).await.expect("check"), Enrollment::Purging);
        t.purger.run_batches(M, PurgeKind::MemberRemoved).await.expect("batches");
        t.purger.end(M).await.expect("end");

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        for _ in 0..2 {
            assert_eq!(enroller.check(M).await.expect("check"), Enrollment::Refused(REFUSED_DENIED));
        }
        assert_eq!(series(&handle.render(), METRIC_REFUSED, &["reason=\"denied\""]), Some(2.0));
        assert_eq!(enroller.cached_refusal_for_tests(M).await, None, "never cached");
        assert!(!t.member(M).await, "nothing inserted");
        // The admit path too: a denial written after the network checks.
        assert_eq!(enroller.admit_for_tests(M).await.expect("admit"), Enrollment::Refused(REFUSED_DENIED));
        assert!(!t.member(M).await);
        // Allowed: the next call goes on to the other checks (here the
        // unresolvable DID), with no cache to clear.
        assert!(allow(&db.pool, M).await.expect("allow"));
        assert_eq!(enroller.check(M).await.expect("check"), Enrollment::Refused("unresolvable"));
        db.drop_db().await;
    }

    /// T-M2 (§5.2–5.4): `deny` of a non-member commits the denial alone;
    /// `add_member` is then `Denied`; a second `deny` keeps `denied_at` and
    /// `denied_by` and replaces the reason only when one is given; `allow`
    /// then `add` adds.
    #[tokio::test]
    async fn t_m2_deny_of_a_non_member_then_allow_and_add() {
        let Some(db) = fresh_db("membership_t_m2").await else {
            return;
        };
        let t = T::new(&db.pool);
        let outcome = t.purger.purge_guarded(N, PurgeKind::MemberRemoved, &T::deny(N, Some("first"))).await.expect("deny");
        assert_eq!(outcome, MarkerOutcome::Skipped);
        assert!(t.denied(N).await && !t.purging(N).await && !t.member(N).await);
        assert_eq!(t.scope.snapshot().await.purge_kind(N), None, "nothing in memory");
        assert_eq!(t.add(N).await, AddOutcome::Denied);
        assert!(!t.member(N).await);
        assert!(!t.scope.snapshot().await.is_member(N));

        let at: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT denied_at FROM member_denials WHERE did = $1").bind(N).fetch_one(&db.pool).await.expect("at");
        let again = MarkerGuard::Deny(Denial { did: N.into(), reason: None, denied_by: "did:plc:operator".into() });
        assert_eq!(t.purger.marker_guarded(N, PurgeKind::MemberRemoved, &again).await.expect("deny"), MarkerOutcome::Skipped);
        let row: (Option<String>, String, chrono::DateTime<chrono::Utc>) =
            sqlx::query_as("SELECT reason, denied_by, denied_at FROM member_denials WHERE did = $1").bind(N).fetch_one(&db.pool).await.expect("row");
        assert_eq!(row, (Some("first".to_string()), "cli".to_string(), at));
        t.purger.marker_guarded(N, PurgeKind::MemberRemoved, &T::deny(N, Some("second"))).await.expect("deny");
        let reason: Option<String> = sqlx::query_scalar("SELECT reason FROM member_denials WHERE did = $1").bind(N).fetch_one(&db.pool).await.expect("reason");
        assert_eq!(reason.as_deref(), Some("second"));

        assert!(allow(&db.pool, N).await.expect("allow"));
        assert!(!allow(&db.pool, N).await.expect("allow again"), "not denied");
        assert_eq!(t.add(N).await, AddOutcome::Added { became_owner: false });
        assert!(t.member(N).await && t.scope.snapshot().await.is_member(N));
        db.drop_db().await;
    }

    /// T-M8 (§3.1, C-M2 (b)): with a configuration that does not set
    /// `community.auto_enroll`, auto-enroll is off: a non-member is refused
    /// `disabled` and nothing is inserted, while a member enrolled earlier
    /// by auto-enroll stays a member. Set explicitly, it is on.
    #[tokio::test]
    async fn t_m8_auto_enroll_is_off_by_default() {
        let Some(db) = fresh_db("membership_t_m8").await else {
            return;
        };
        let t = T::new(&db.pool);
        assert!(!crate::config::FileConfig::default().community.auto_enroll);
        assert_eq!(add_member(&db.pool, &t.scope, &t.private, M, "auto_enroll", BUDGET).await.expect("add"), AddOutcome::Added { became_owner: false });
        let off = t.enroller_with("");
        assert_eq!(off.check(N).await.expect("check"), Enrollment::Refused("disabled"));
        assert!(!t.member(N).await, "nothing inserted");
        assert_eq!(off.check(M).await.expect("check"), Enrollment::Member { ready: false, reindexing: false }, "an earlier auto-enrolled member stays");
        assert!(t.member(M).await);
        let on = t.enroller_with("[community]\nauto_enroll = true\n");
        assert_eq!(on.check(N).await.expect("check"), Enrollment::Refused("unresolvable"), "on: the network checks run");
        db.drop_db().await;
    }

    /// T-M3 (§2.2, Invariant D): `add_member` before the deny marker (the
    /// marker removes it, `Marked`) and after it (`Denied`); a deny while a
    /// purge of the DID runs is `Busy` and writes nothing; and racing pairs
    /// never commit a DID both a member and denied.
    #[tokio::test]
    async fn t_m3_add_and_deny_never_break_invariant_d() {
        let Some(db) = fresh_db("membership_t_m3").await else {
            return;
        };
        let t = T::new(&db.pool);
        // Before: the marker removes the member.
        assert_eq!(t.add(M).await, AddOutcome::Added { became_owner: false });
        assert_eq!(t.purger.purge_guarded(M, PurgeKind::MemberRemoved, &T::deny(M, None)).await.expect("deny"), MarkerOutcome::Marked);
        assert!(!t.member(M).await && t.denied(M).await && !t.purging(M).await, "the whole purge ran");
        // After: refused.
        assert_eq!(t.add(M).await, AddOutcome::Denied);
        // A running purge: Busy, and no denial written.
        assert_eq!(t.add(N).await, AddOutcome::Added { became_owner: false });
        t.purger.marker(N, PurgeKind::MemberRemoved).await.expect("remove marker");
        assert!(matches!(
            t.purger.marker_guarded(N, PurgeKind::MemberRemoved, &T::deny(N, None)).await,
            Err(PurgeError::Busy("member_removed"))
        ));
        assert!(!t.denied(N).await);
        assert!(matches!(
            t.purger.purge_guarded(N, PurgeKind::MemberRemoved, &T::deny(N, None)).await,
            Err(PurgeError::Busy("member_removed"))
        ));
        assert!(!t.denied(N).await, "a guarded purge resumes nothing");

        // Races: each pair runs concurrently; whichever wins, no DID ends
        // both a member and denied.
        let dids: Vec<String> = (0..24).map(|i| format!("did:plc:race{i:020}")).collect();
        let mut tasks = Vec::new();
        for (i, did) in dids.iter().enumerate() {
            let (pool, scope, private, purger, did) =
                (db.pool.clone(), t.scope.clone(), Arc::clone(&t.private), t.purger.clone(), did.clone());
            tasks.push(tokio::spawn(async move {
                let add = async { add_member(&pool, &scope, &private, &did, "auto_enroll", BUDGET).await.expect("add") };
                let deny = async { purger.purge_guarded(&did, PurgeKind::MemberRemoved, &T::deny(&did, None)).await.expect("deny") };
                if i % 2 == 0 { tokio::join!(add, deny) } else { let (d, a) = tokio::join!(deny, add); (a, d) }
            }));
        }
        let mut seen = BTreeSet::new();
        for task in tasks {
            let (added, denied) = task.await.expect("task");
            seen.insert(format!("{added:?}/{denied:?}"));
        }
        let both = t
            .count("SELECT count(*) FROM members m WHERE EXISTS (SELECT 1 FROM member_denials d WHERE d.did = m.did)")
            .await;
        assert_eq!(both, 0, "outcomes: {seen:?}");
        assert_eq!(t.count("SELECT count(*) FROM member_denials WHERE did LIKE 'did:plc:race%'").await, 24);
        assert_eq!(t.count("SELECT count(*) FROM purging_dids WHERE did LIKE 'did:plc:race%'").await, 0, "every marked race ran to its end");
        db.drop_db().await;
    }

    /// The `Expiry` guard (§3.2; the task that uses it is `expiry`): an
    /// `auto_enroll` member never ready and added before the cutoff is
    /// marked; one ready once, an `admin` one, or one added after the
    /// cutoff is `Skipped`: no `purging_dids` row, its bookmarks and mutes
    /// kept, no job's generation moved, and nothing in memory changed.
    #[tokio::test]
    async fn expiry_guard_skips_unless_the_condition_holds() {
        let Some(db) = fresh_db("membership_expiry_guard").await else {
            return;
        };
        let t = T::new(&db.pool);
        let cutoff = chrono::Utc::now() - chrono::Duration::days(7);
        let cases = [
            ("did:plc:expirednever000000000000", "auto_enroll", "now() - interval '8 days'", false, true),
            ("did:plc:readyonce0000000000000000", "auto_enroll", "now() - interval '8 days'", true, false),
            ("did:plc:adminnever00000000000000", "admin", "now() - interval '8 days'", false, false),
            ("did:plc:recentnever0000000000000", "auto_enroll", "now() - interval '1 day'", false, false),
        ];
        for (did, source, added, ready, _) in cases {
            assert_eq!(add_member(&db.pool, &t.scope, &t.private, did, source, BUDGET).await.expect("add"), AddOutcome::Added { became_owner: false });
            sqlx::query(&format!(
                "UPDATE members SET added_at = {added}, first_ready_at = CASE WHEN $2 THEN now() END WHERE did = $1"
            ))
            .bind(did)
            .bind(ready)
            .execute(&db.pool)
            .await
            .expect("age");
            for sql in [
                "INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ($1, 'at://x/app.bsky.feed.post/1', 'c')",
                "INSERT INTO mutes (viewer_did, subject_did) VALUES ($1, 'did:plc:x')",
            ] {
                sqlx::query(sql).bind(did).execute(&db.pool).await.expect(sql);
            }
        }
        let generations = || async {
            let rows: Vec<(String, String, i64)> =
                sqlx::query_as("SELECT did, depth, generation FROM backfill_jobs ORDER BY did, depth").fetch_all(&db.pool).await.expect("jobs");
            rows
        };
        let before = generations().await;
        for (did, _, _, _, expires) in cases {
            let outcome = t.purger.purge_guarded(did, PurgeKind::MemberRemoved, &MarkerGuard::Expiry { cutoff }).await.expect("expiry");
            if expires {
                assert_eq!(outcome, MarkerOutcome::Marked, "{did}");
                assert!(!t.member(did).await, "{did}");
                assert!(!t.denied(did).await, "expiry is not exclusion");
            } else {
                assert_eq!(outcome, MarkerOutcome::Skipped, "{did}");
                assert!(t.member(did).await && !t.purging(did).await, "{did}");
                assert!(t.scope.snapshot().await.is_member(did), "{did}");
                for table in ["bookmarks", "mutes"] {
                    assert_eq!(t.count(&format!("SELECT count(*) FROM {table} WHERE viewer_did = '{did}'")).await, 1, "{did}: {table}");
                }
            }
        }
        let after = generations().await;
        let kept = |rows: &[(String, String, i64)]| rows.iter().filter(|r| !r.0.starts_with("did:plc:expired")).cloned().collect::<Vec<_>>();
        assert_eq!(kept(&after), kept(&before), "no skipped member's job moved");
        db.drop_db().await;
    }
}
