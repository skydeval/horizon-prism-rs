//! Membership not tied to hosting, S1 (docs/internal/design/membership.md
//! §13): the owner rule (T-M4), removal (T-M5), migration 0031 (T-M6) and
//! PB-M2's pre-release count. Built on the S10 harness, whose items this
//! module sees: [`H`], and jobs from [`Pds`].

use super::*;
use crate::aggregations::membership::{AddOutcome, add_member};
use crate::subscriptions::references::Budget;

const BUDGET: Budget = Budget { max_references: 25, max_owners: 10 };

fn audience_record(members: &[&str]) -> Value {
    json!({"$type": NSID_AUDIENCE, "mode": "list", "members": members, "createdAt": "2026-09-26T00:00:00Z"})
}

impl H {
    /// Add `did` as `admin member add` does.
    async fn add(&self, did: &str) -> AddOutcome {
        add_member(self.pool(), self.f.writer.scope(), self.f.writer.private(), did, "admin", BUDGET).await.expect("add member")
    }

    /// `did`'s head at its applied rev, checked now, so a read's head check
    /// passes after a job.
    async fn head_at_applied(&self, did: &str) {
        exec(
            self.pool(),
            &format!("UPDATE sync_state SET head_rev = applied_rev, firehose_rev = applied_rev, head_checked_at = now() WHERE did = '{did}'"),
        )
        .await;
    }

    async fn jobs_of(&self, did: &str) -> Vec<(String, String)> {
        sqlx::query_as("SELECT depth, state FROM backfill_jobs WHERE did = $1 ORDER BY depth")
            .bind(did)
            .fetch_all(self.pool())
            .await
            .expect("jobs")
    }

    fn asked(&self, did: &str) -> bool {
        self.pds.prepared.lock().expect("prepared").iter().any(|d| d == did)
    }
}

/// T-M4 (§2.3): Eve, a non-member outside S, has an audience naming Bob and
/// a post under it, both stored. She is no owner; her post is a miss to
/// Bob; her rows queued no job, and a `private` job queued by hand ends
/// `done` without asking her PDS. `member add` makes her an owner and
/// queues her `private` job, which runs; Bob then reads the post.
#[tokio::test]
async fn t_m4_membership_is_the_only_owner_gate() {
    let Some(h) = H::new("membership-t-m4", PublicMode::On).await else {
        return;
    };
    actor(h.pool(), EVE, "eve.test", "eve.test").await;
    h.clear_jobs().await;
    let aud = h.direct(EVE, NSID_AUDIENCE, "3ktm4aud", audience_record(&[BOB])).await;
    let record = h.f.post_record(EVE, "3ktm4pst", &aud, None).await;
    let post = h.direct(EVE, NSID_POST_PRIVATE, "3ktm4pst", record).await;
    assert!(!KryphocronPrivateTier.load_owners(h.pool()).await.expect("owners").contains(EVE));
    assert!(!h.f.writer.scope().read_guard().await.is_owner(EVE));
    assert!(h.jobs_of(EVE).await.is_empty(), "a non-member's rows queue nothing");
    assert!(!h.served(BOB, &post).await, "a uniform miss");
    // A job queued anyway (one queued before the upgrade, say).
    let b = h.backfill();
    assert_eq!(h.run(&b, EVE, Depth::Private).await, JobOutcome::Done);
    assert!(!h.asked(EVE), "no network call");
    assert!(!h.owner_ready(EVE).await);

    // Added: an owner, with its `private` job.
    h.clear_jobs().await;
    assert_eq!(h.add(EVE).await, AddOutcome::Added { became_owner: true });
    assert!(h.f.writer.scope().read_guard().await.is_owner(EVE));
    assert!(h.jobs_of(EVE).await.contains(&("private".to_string(), "queued".to_string())));
    assert_eq!(h.rerun(&b, EVE, Depth::Private).await, JobOutcome::Ready);
    assert!(h.asked(EVE));
    h.head_at_applied(EVE).await;
    assert!(h.reads(BOB, &post).await);
    assert!(!h.reads(CAROL, &post).await, "the audience still decides");
    h.finish().await;
}

/// T-M5 (§5.1): Eve a member and an owner, her post under an audience
/// naming Bob and Carol, Bob's reply under it. After `member remove` (the
/// whole purge) both are misses to Carol: Eve is no owner, and rule 5 needs
/// the audience's owner ready. Re-added, both are served again once her
/// `private` job has run.
#[tokio::test]
async fn t_m5_removal_stops_serving_and_a_re_add_restores() {
    let Some(h) = H::new("membership-t-m5", PublicMode::On).await else {
        return;
    };
    actor(h.pool(), EVE, "eve.test", "eve.test").await;
    assert_eq!(h.add(EVE).await, AddOutcome::Added { became_owner: false });
    let aud = h.audience_of(EVE, "3ktm5aud", "list", &[BOB, CAROL]).await;
    let post = h.post(EVE, "3ktm5pst", &aud, None).await;
    let reply = h.post(BOB, "3ktm5rep", &aud, Some((&post, &post))).await;
    h.f.ready_all().await;
    let b = h.backfill();
    h.clear_jobs().await;
    assert_eq!(h.run(&b, EVE, Depth::Private).await, JobOutcome::Ready);
    h.head_at_applied(EVE).await;
    assert!(h.reads(CAROL, &post).await);
    assert!(h.reads(CAROL, &reply).await);

    let purger = Purger::new(h.pool().clone(), h.f.writer.scope().clone(), Arc::clone(h.f.writer.private()));
    purger.purge(EVE, PurgeKind::MemberRemoved).await.expect("member removed");
    assert!(!h.f.writer.scope().read_guard().await.is_owner(EVE));
    assert!(!h.owner_ready(EVE).await);
    assert!(!h.served(CAROL, &post).await, "her post");
    assert!(!h.served(CAROL, &reply).await, "a reply under her audience");
    let stored: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM private_posts WHERE uri IN ('{post}', '{reply}')"))
        .fetch_one(h.pool())
        .await
        .expect("stored");
    assert_eq!(stored, 2, "kept, not served");

    h.clear_jobs().await;
    assert_eq!(h.add(EVE).await, AddOutcome::Added { became_owner: true });
    assert!(!h.served(CAROL, &post).await, "not before readiness");
    assert_eq!(h.rerun(&b, EVE, Depth::Private).await, JobOutcome::Ready);
    h.head_at_applied(EVE).await;
    assert!(h.reads(CAROL, &post).await);
    assert!(h.reads(CAROL, &reply).await);
    h.finish().await;
}

/// PB-M2 (§14): before S1 deploys, the owners S1 stops serving and the
/// stored plaintext rows (C-M5). Read-only; it names only tables and
/// columns that exist before 0031.
/// - `member_owners`: DIDs with private rows that are members (unchanged);
/// - `nonmember_owners`: DIDs with private rows that are owners today by
///   community-PDS eligibility alone, and stop at the upgrade;
/// - `nonmember_rows_unserved`: DIDs with private rows that are neither,
///   already not served;
/// - `plaintext_posts`, `plaintext_posts_nonmember_owners`: private posts
///   with no `encodedContent`, all and those of the non-member owners.
const PB_M2_COUNT_SQL: &str = "WITH r AS (
  SELECT author_did AS did FROM private_posts UNION SELECT owner_did FROM private_audiences
), o AS (
  SELECT r.did FROM r WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = r.did)
                        AND EXISTS (SELECT 1 FROM private_owner_eligible e WHERE e.did = r.did)
)
SELECT
  (SELECT count(*) FROM r WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = r.did)) AS member_owners,
  (SELECT count(*) FROM o) AS nonmember_owners,
  (SELECT count(*) FROM r WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = r.did)
                            AND NOT EXISTS (SELECT 1 FROM private_owner_eligible e WHERE e.did = r.did)) AS nonmember_rows_unserved,
  (SELECT count(*) FROM private_posts WHERE encoded_content IS NULL) AS plaintext_posts,
  (SELECT count(*) FROM private_posts p WHERE p.encoded_content IS NULL AND p.author_did IN (SELECT did FROM o))
    AS plaintext_posts_nonmember_owners";

/// PB-M2's list: each non-member owner, for the operator to decide whom to
/// `admin member add` before the upgrade (§10).
const PB_M2_LIST_SQL: &str = "SELECT e.did, a.handle, e.confirmed_at,
  (SELECT count(*) FROM private_posts p WHERE p.author_did = e.did) AS posts,
  (SELECT count(*) FROM private_audiences x WHERE x.owner_did = e.did) AS audiences
FROM private_owner_eligible e LEFT JOIN actors a ON a.did = e.did
WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = e.did)
  AND (EXISTS (SELECT 1 FROM private_posts p WHERE p.author_did = e.did)
       OR EXISTS (SELECT 1 FROM private_audiences x WHERE x.owner_did = e.did))
ORDER BY e.did";

/// Prints PB-M2's queries for skydeval to run read-only on the deployed
/// database before S1 (migration 0031) deploys:
/// `cargo test pre_s1_membership_count_query -- --ignored --nocapture`.
#[test]
#[ignore = "prints PB-M2's pre-S1 queries; run with --ignored --nocapture"]
fn pre_s1_membership_count_query() {
    println!("-- PB-M2 (membership S1): the owners S1 stops serving, and the plaintext rows\n{PB_M2_COUNT_SQL};\n");
    println!("-- PB-M2: each non-member owner, to `admin member add` if it should stay one\n{PB_M2_LIST_SQL};");
}

/// T-M6 (§10): migration 0031 on a fixture before it. M, a member owner,
/// keeps everything. E, a non-member eligible owner in S, loses its
/// readiness and its repo-head state as `forget_tracked_sync_state` would
/// (the row kept, its flag cleared, its counter replaced), and its queued
/// `private`/`authz` jobs are cancelled with a new generation (its `scope`
/// job stays). F, the same outside S, loses its whole `sync_state` row. X,
/// a non-member with rows and no eligibility (no owner already), keeps its
/// `sync_state`; a readiness row or a `private`/`authz` job of any
/// non-member goes. The table and
/// the actors' failure columns are gone. PB-M2's queries, run first, count
/// and list the same DIDs.
#[tokio::test]
async fn t_m6_migration_0031() {
    const M: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const E: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
    const F: &str = "did:plc:ffffffffffffffffffffffff";
    const X: &str = "did:plc:xxxxxxxxxxxxxxxxxxxxxxxx";
    let Some(db) = crate::test_support::empty_db("membership-t-m6").await else {
        return;
    };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 31).expect("0031");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    for (did, member, eligible, in_scope) in [(M, true, false, true), (E, false, true, true), (F, false, true, false), (X, false, false, true)] {
        exec(p, &format!("INSERT INTO actors (did, handle) VALUES ('{did}', '{}.test')", &did[8..9])).await;
        // A plaintext post for each, an encoded one for M.
        exec(
            p,
            &format!(
                "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, plaintext_text, created_at) \
                 VALUES ('at://{did}/{NSID_POST_PRIVATE}/3kp', '{did}', '3kp', 'bafy', '{REV}', 't', now())"
            ),
        )
        .await;
        exec(p, &format!("INSERT INTO private_owner_ready (did, ready_at) VALUES ('{did}', now())")).await;
        exec(
            p,
            &format!(
                "INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at, follows_verified_at, last_resync_at) \
                 VALUES ('{did}', '{REV}', '{REV}', now(), now(), now())"
            ),
        )
        .await;
        for depth in ["private", "authz", "scope"] {
            exec(p, &format!("INSERT INTO backfill_jobs (did, depth, state) VALUES ('{did}', '{depth}', 'queued')")).await;
        }
        if member {
            exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{did}', 'admin', now())")).await;
        }
        if eligible {
            exec(p, &format!("INSERT INTO private_owner_eligible (did, confirmed_at) VALUES ('{did}', now())")).await;
        }
        if in_scope {
            exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{did}', 2)")).await;
        }
    }
    exec(
        p,
        &format!(
            "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, encoded_content, created_at) \
             VALUES ('at://{M}/{NSID_POST_PRIVATE}/3ke', '{M}', '3ke', 'bafy', '{REV}', '\\x00', now())"
        ),
    )
    .await;

    let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(PB_M2_COUNT_SQL).fetch_one(p).await.expect("count query");
    assert_eq!(counts, (1, 2, 1, 4, 2));
    type Listed = (String, Option<String>, DateTime<Utc>, i64, i64);
    let listed: Vec<Listed> = sqlx::query_as(PB_M2_LIST_SQL).fetch_all(p).await.expect("list query");
    assert_eq!(listed.iter().map(|r| (r.0.as_str(), r.1.as_deref(), r.3, r.4)).collect::<Vec<_>>(), [(E, Some("e.test"), 1, 0), (F, Some("f.test"), 1, 0)]);
    let clears = |did: &'static str| async move {
        let n: i64 = sqlx::query_scalar("SELECT follows_clears FROM sync_state WHERE did = $1").bind(did).fetch_one(p).await.expect("clears");
        n
    };
    let (e_clears, x_clears) = (clears(E).await, clears(X).await);

    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0031");

    let ready: Vec<String> = sqlx::query_scalar("SELECT did FROM private_owner_ready ORDER BY did").fetch_all(p).await.expect("ready");
    assert_eq!(ready, [M.to_string()], "no non-member is ready");
    type Sync = (String, Option<String>, Option<String>, bool, bool, bool);
    let sync: Vec<Sync> = sqlx::query_as(
        "SELECT did, applied_rev, head_rev, head_checked_at IS NOT NULL, follows_verified_at IS NOT NULL, last_resync_at IS NOT NULL \
         FROM sync_state ORDER BY did",
    )
    .fetch_all(p)
    .await
    .expect("sync_state");
    let r = || Some(REV.to_string());
    assert_eq!(
        sync,
        [
            (E.to_string(), None, None, false, false, true),
            (M.to_string(), r(), r(), true, true, true),
            (X.to_string(), r(), r(), true, true, true),
        ]
    );
    assert_ne!(clears(E).await, e_clears, "E's counter replaced");
    assert_eq!(clears(X).await, x_clears);
    let jobs: Vec<(String, String, String, i64)> =
        sqlx::query_as("SELECT did, depth, state, generation FROM backfill_jobs ORDER BY did, depth").fetch_all(p).await.expect("jobs");
    let job = |did: &str, depth: &str, state: &str, generation: i64| (did.to_string(), depth.to_string(), state.to_string(), generation);
    assert_eq!(
        jobs,
        [
            job(E, "authz", "cancelled", 1),
            job(E, "private", "cancelled", 1),
            job(E, "scope", "queued", 0),
            job(F, "authz", "cancelled", 1),
            job(F, "private", "cancelled", 1),
            job(F, "scope", "queued", 0),
            job(M, "authz", "queued", 0),
            job(M, "private", "queued", 0),
            job(M, "scope", "queued", 0),
            job(X, "authz", "cancelled", 1),
            job(X, "private", "cancelled", 1),
            job(X, "scope", "queued", 0),
        ]
    );
    let gone: bool = sqlx::query_scalar("SELECT to_regclass('private_owner_eligible') IS NULL").fetch_one(p).await.expect("regclass");
    assert!(gone);
    let columns: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 'actors' AND column_name LIKE 'eligibility%'",
    )
    .fetch_one(p)
    .await
    .expect("columns");
    assert_eq!(columns, 0);
    let denials: i64 = sqlx::query_scalar("SELECT count(*) FROM member_denials").fetch_one(p).await.expect("member_denials");
    assert_eq!(denials, 0);
    // The rest of the migrations still apply on top.
    for m in &migrations[at + 1..] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    db.drop_db().await;
}

/// T-M6: what the owner set loads after 0031 is what the rule says: Alice,
/// a member with an audience, and not Eve, a non-member with one.
#[tokio::test]
async fn t_m6_owners_load_members_only() {
    let Some(db) = fresh_db("membership-t-m6-load").await else {
        return;
    };
    let p = &db.pool;
    for did in [ALICE, EVE] {
        exec(
            p,
            &format!(
                "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev) \
                 VALUES ('at://{did}/{NSID_AUDIENCE}/3ka', '{did}', '3ka', 'bafy', '{REV}')"
            ),
        )
        .await;
    }
    exec(p, &format!("INSERT INTO members (did, source) VALUES ('{ALICE}', 'admin')")).await;
    let owners = KryphocronPrivateTier.load_owners(p).await.expect("load");
    assert_eq!(owners.into_iter().collect::<Vec<_>>(), [ALICE.to_string()]);
    let mut conn = p.acquire().await.expect("conn");
    assert!(crate::private_tier::owners::is_owner(&mut conn, ALICE).await.expect("owner"));
    assert!(!crate::private_tier::owners::is_owner(&mut conn, EVE).await.expect("owner"));
    drop(conn);
    db.drop_db().await;
}
