//! #723, replay below a settle (docs/internal/design/replay-readiness.md
//! §11): the listed rev, the ingest refusal, the settle's coverage and
//! catch-up's check, T-63 to T-82 (T-73 sits beside T-40). Built on the S10
//! harness, whose items this module sees: the firehose's own commits through
//! [`H::fire`], jobs and catch-ups from [`Pds`].
//!
//! Private posts are the one grant collection: audiences are held by this
//! AppView and never listed (private-audiences design §4.2, §4.3), so T-62,
//! which tested replayed audiences by mode, is gone, and the others replay
//! posts (a stale post version moves it from a wide audience to a narrow
//! one).
//!
//! The control (review 1, LB3): a test that asserts a miss also asserts, at
//! the read, that O and V are ready and `applied_rev ≥ head_rev` for both
//! with O's head at the job's rev, and runs once more with the refusal off
//! ([`hooks::LISTED_REV_REFUSAL_OFF`]), where V must read. T-75 and T-76
//! have their own controls (the check off, the raise forced).

use metrics_exporter_prometheus::PrometheusHandle;

use super::*;
use crate::firehose::index_request::{IndexStatus, index_record};
use crate::firehose::writer::{
    LISTED_REV_LOCK_DEADLINE, ListedRevs, METRIC_LISTED_REV_LOCK_RETRIES, METRIC_SETTLE_UNCOVERED, Raise, raise_listed_rev,
};
use crate::test_support::series;

/// A data test in On and Off, as built (`true`) and as its control
/// (`false`).
macro_rules! with_control {
    ($f:ident: $on:ident, $off:ident, $on_control:ident, $off_control:ident) => {
        #[tokio::test]
        async fn $on() {
            $f(stringify!($on), PublicMode::On, true).await;
        }
        #[tokio::test]
        async fn $off() {
            $f(stringify!($off), PublicMode::Off, true).await;
        }
        #[tokio::test]
        async fn $on_control() {
            $f(stringify!($on_control), PublicMode::On, false).await;
        }
        #[tokio::test]
        async fn $off_control() {
            $f(stringify!($off_control), PublicMode::Off, false).await;
        }
    };
}

/// A data test in On and Off.
macro_rules! in_both_modes {
    ($f:ident: $on:ident, $off:ident) => {
        #[tokio::test]
        async fn $on() {
            $f(stringify!($on), PublicMode::On).await;
        }
        #[tokio::test]
        async fn $off() {
            $f(stringify!($off), PublicMode::Off).await;
        }
    };
}

/// The `n`th rev of this module's own sequence, above every fixture rev.
fn rev(n: u8) -> String {
    format!("3km{}aaaaaaaa2", char::from(b'a' + n))
}

fn audience_record(mode: &str, members: &[&str]) -> Value {
    json!({"$type": NSID_AUDIENCE, "mode": mode, "members": members, "createdAt": "2026-09-26T00:00:00Z"})
}

fn upsert(collection: &str, rkey: &str, cid: &str, record: Value) -> RawOp {
    RawOp { action: Action::Create, collection: collection.into(), rkey: rkey.into(), cid: Some(cid.into()), record: Some(record) }
}

fn delete(collection: &str, rkey: &str) -> RawOp {
    RawOp { action: Action::Delete, collection: collection.into(), rkey: rkey.into(), cid: None, record: None }
}

fn uri(did: &str, collection: &str, rkey: &str) -> String {
    format!("at://{did}/{collection}/{rkey}")
}

/// A job's write (`Backfill::options`): an older job's, written by hand.
fn job_write() -> ApplyOptions {
    ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: false, subscription_rules: true, source: WriteSource::Backfill }
}

/// Catch-up's settings with room for a test's pauses.
fn roomy() -> CatchUpConfig {
    CatchUpConfig { deadline: Duration::from_secs(10), ..H::catch_up_cfg() }
}

/// The refusal off for `did`'s records while the result lives, in a
/// control run.
fn refusal_off(h: &H, did: &str, refusal: bool) -> Option<hooks::Armed> {
    (!refusal).then(|| hooks::arm(hooks::LISTED_REV_REFUSAL_OFF, &h.key(did)))
}

/// O (Alice) an owner, ready: an audience listing Bob, held here. Its URI.
async fn owner(h: &H, tag: &str) -> String {
    let rkey = format!("3k{tag}base");
    let aud = h.audience_of(ALICE, &rkey, "list", &[BOB]).await;
    h.f.ready_all().await;
    aud
}

/// `did`'s post listed rev.
async fn listed(h: &H, did: &str) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>("SELECT post_listed_rev FROM sync_state WHERE did = $1")
        .bind(did)
        .fetch_optional(h.pool())
        .await
        .expect("sync_state")
        .flatten()
}

fn at(r: &str) -> Option<String> {
    Some(r.to_string())
}

/// The stored `(rev, cid)` of `uri` in `table`.
async fn row(h: &H, table: &str, uri: &str) -> Option<(String, Option<String>)> {
    sqlx::query_as(&format!("SELECT rev, cid FROM {table} WHERE uri = $1"))
        .bind(uri)
        .fetch_optional(h.pool())
        .await
        .expect(table)
}

/// Whether `getPosts` answers `reader` for `uri` byte-identically to a URI
/// that does not exist.
async fn misses(h: &H, reader: &str, uri: &str) -> bool {
    h.fresh_heads().await;
    let missing = format!("{}/3knothere0000", uri.rsplit_once('/').expect("at-uri").0);
    let (status, body, _) = get(&h.base, POSTS, &format!("uris={uri}"), reader, key_of(reader)).await;
    let (none_status, none, _) = get(&h.base, POSTS, &format!("uris={missing}"), reader, key_of(reader)).await;
    assert_eq!(status, 200, "{body}");
    (status, body) == (none_status, none)
}

/// The control at the read: `owner` and `reader` ready, and `applied_rev ≥
/// head_rev` for both, `owner`'s head set to `head` and fresh. A miss is
/// then the refusal's.
async fn control(h: &H, owner: &str, reader: &str, head: &str) {
    sqlx::query("UPDATE sync_state SET head_rev = $2, head_checked_at = now() WHERE did = $1")
        .bind(owner)
        .bind(head)
        .execute(h.pool())
        .await
        .expect("head");
    assert!(h.member_ready(owner).await && h.owner_ready(owner).await, "{owner} ready");
    assert!(h.member_ready(reader).await, "{reader} ready");
    for did in [owner, reader] {
        let fresh: bool = sqlx::query_scalar("SELECT COALESCE(applied_rev >= head_rev, false) FROM sync_state WHERE did = $1")
            .bind(did)
            .fetch_one(h.pool())
            .await
            .expect("head check");
        assert!(fresh, "{did}: applied_rev ≥ head_rev");
    }
}

/// A catch-up of `did` at `rev` on a task of its own, with [`roomy`].
fn spawn_catch_up(h: &H, did: &str, rev: &str) -> tokio::task::JoinHandle<bool> {
    let (pds, writer, did, rev) = (Arc::clone(&h.pds), h.f.writer.clone(), did.to_string(), rev.to_string());
    tokio::spawn(async move { catch_up(pds.as_ref(), &writer, &CatchUp::new(roomy()), &did, PDS, &rev).await })
}

/// A transaction holding `did`'s lock shared, as a writer's does.
async fn hold_shared(pool: &PgPool, did: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("SELECT pg_advisory_xact_lock_shared(hashtextextended('did:' || $1, 0))")
        .bind(did)
        .execute(&mut *tx)
        .await
        .expect("shared lock");
    tx
}

/// Another shared locker of `did` is never blocked by a raise's tries.
async fn shared_locker_passes(pool: &PgPool, did: &str) {
    let tx = tokio::time::timeout(Duration::from_secs(2), hold_shared(pool, did)).await.expect("a shared locker is never blocked");
    tx.rollback().await.expect("rollback");
}

/// Wait until a raise has found the lock held at least once.
async fn until_retrying(handle: &PrometheusHandle) {
    for _ in 0..250 {
        if series(&handle.render(), METRIC_LISTED_REV_LOCK_RETRIES, &[]).is_some_and(|n| n >= 1.0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no raise retried");
}

fn catch_up_outcome(handle: &PrometheusHandle, outcome: &str) -> f64 {
    series(&handle.render(), "index_request_catch_up_total", &[&format!("outcome=\"{outcome}\"")]).unwrap_or(0.0)
}

/// One ordering of the issue: in O's repository a private post Q under O's
/// audience (Bob listed) created at C1 and deleted at D, the head D.
/// Nothing of it applied yet: the consumer is behind.
struct Scene {
    q_rkey: String,
    q: String,
    q_rec: Value,
    c1: String,
    d: String,
}

impl Scene {
    async fn new(h: &H, tag: &str, audience: &str, n: u8) -> Self {
        let q_rkey = format!("3k{tag}pst");
        let q_rec = h.f.post_record(ALICE, &q_rkey, audience, None).await;
        let (c1, d) = (rev(n), rev(n + 2));
        h.pds.set_rev(ALICE, &d);
        Self { q: uri(ALICE, NSID_POST_PRIVATE, &q_rkey), q_rkey, q_rec, c1, d }
    }

    /// The consumer's replay of C1.
    fn replay(&self) -> RawOp {
        upsert(NSID_POST_PRIVATE, &self.q_rkey, &format!("bafy{}", self.q_rkey), self.q_rec.clone())
    }
}

// ---- T-63: a deleted post ----

/// T-63: P' (a mention of V, created now) created at C1 and deleted at D; a
/// job at R ≥ D. The replay of C1 is refused: no row and no notification.
/// With the refusal off, V reads P' and has the notification.
async fn t63_deleted_post(test: &str, mode: PublicMode, refusal: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let _off = refusal_off(&h, ALICE, refusal);
    let base = owner(&h, "t63").await;
    let rkey = "3kt63pst001";
    let mut record = h.f.post_record(ALICE, rkey, &base, None).await;
    record["createdAt"] = json!(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true));
    record["facets"] = json!([{"index": {"byteStart": 0, "byteEnd": 4}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": BOB}]}]);
    h.pds.set_rev(ALICE, R2);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    let report = h.fire(ALICE, R1, vec![upsert(NSID_POST_PRIVATE, rkey, "bafyt63", record)]).await;
    let p = uri(ALICE, NSID_POST_PRIVATE, rkey);
    let notified: i64 = sqlx::query_scalar("SELECT count(*) FROM private_notifications WHERE record_uri = $1 AND recipient_did = $2")
        .bind(&p)
        .bind(BOB)
        .fetch_one(h.pool())
        .await
        .expect("notifications");
    control(&h, ALICE, BOB, R2).await;
    if refusal {
        assert_eq!(report.outcomes, [OpOutcome::Unchanged]);
        assert_eq!(row(&h, "private_posts", &p).await, None);
        assert_eq!(notified, 0);
        assert!(misses(&h, BOB, &p).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Written]);
        assert_eq!(notified, 1);
        assert!(h.reads(BOB, &p).await);
    }
    h.finish().await;
}

with_control!(t63_deleted_post: t63_deleted_post_on, t63_deleted_post_off, t63_control_on, t63_control_off);

// ---- T-64: the interleavings of §4.5 ----

/// T-64: (a) a replay committed between the post listing and the raise is
/// deleted by the reconcile before the settle; (b) a writer holding the
/// DID's lock shared when the raise starts: the raise retries without
/// blocking another shared locker, the writer commits, the raise commits,
/// the reconcile deletes the row; (c) a replay between the raise and the
/// reconcile returns `Unchanged` (with the refusal off, `Written`, and the
/// reconcile still deletes it); (d) after the settle, the same, and with
/// the refusal off V reads. (a) and (b) pass at HEAD too: non-regression.
async fn t64_interleavings(test: &str, mode: PublicMode, refusal: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let _off = refusal_off(&h, ALICE, refusal);
    let base = owner(&h, "t64").await;
    let b = h.backfill();
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);

    // (a) Before the raise.
    let s = Scene::new(&h, "t64a", &base, 0).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    assert_eq!(h.fire(ALICE, &s.c1, vec![s.replay()]).await.outcomes, [OpOutcome::Written], "below no listed rev yet");
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    drop(hook);
    assert_eq!(row(&h, "private_posts", &s.q).await, None, "(a) reconciled");

    // (b) A writer in flight when the raise starts.
    let s = Scene::new(&h, "t64b", &base, 3).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    let mut writer_tx = hold_shared(h.pool(), ALICE).await;
    sqlx::query(
        "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, plaintext_text, created_at, display_filled) \
         VALUES ($1, $2, $3, 'bafyt64b', $4, $5, 'x', now(), true)",
    )
    .bind(&s.q)
    .bind(ALICE)
    .bind(&s.q_rkey)
    .bind(&s.c1)
    .bind(&base)
    .execute(&mut *writer_tx)
    .await
    .expect("the writer's row");
    hook.release();
    until_retrying(&handle).await;
    shared_locker_passes(h.pool(), ALICE).await;
    assert!(!run.is_finished(), "the raise waits for the writer");
    writer_tx.commit().await.expect("the writer commits");
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    drop(hook);
    assert_eq!(row(&h, "private_posts", &s.q).await, None, "(b) reconciled");

    // (c) Between the raise and the reconcile.
    let s = Scene::new(&h, "t64c", &base, 6).await;
    let hook = hooks::arm(hooks::LISTED_REV_AFTER_RAISE, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    assert_eq!(listed(&h, ALICE).await.as_deref(), Some(s.d.as_str()), "the raise committed");
    let expected = if refusal { OpOutcome::Unchanged } else { OpOutcome::Written };
    assert_eq!(h.fire(ALICE, &s.c1, vec![s.replay()]).await.outcomes, [expected], "(c)");
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    drop(hook);
    assert_eq!(row(&h, "private_posts", &s.q).await, None, "(c)");

    // (d) After the settle.
    let s = Scene::new(&h, "t64d", &base, 9).await;
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready);
    let report = h.fire(ALICE, &s.c1, vec![s.replay()]).await;
    control(&h, ALICE, BOB, &s.d).await;
    if refusal {
        assert_eq!(report.outcomes, [OpOutcome::Unchanged], "(d)");
        assert_eq!(row(&h, "private_posts", &s.q).await, None);
        assert!(misses(&h, BOB, &s.q).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Written], "(d)");
        assert!(h.reads(BOB, &s.q).await, "(d): the HEAD leak");
    }
    h.finish().await;
}

with_control!(t64_interleavings: t64_interleavings_on, t64_interleavings_off, t64_control_on, t64_control_off);

// ---- T-65: ops at or above R ----

/// T-65: after a settle at R, an op at R for a record the run did not write
/// (created in commit R, deleted before the live listing) applies, and so
/// does an op above R. An op at R for a record the run stored at R is
/// refused by last-writer-wins, as at HEAD.
async fn t65_at_or_above_r(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t65").await;
    let listed_post = h.post(ALICE, "3kt65pst", &base, None).await;
    h.pds.set_rev(ALICE, R2);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    assert_eq!(listed(&h, ALICE).await, at(R2));
    assert_eq!(row(&h, "private_posts", &listed_post).await.map(|r| r.0), Some(R2.to_string()), "the run stored it at R");
    for (r, rkey, cid) in [(R2, "3kt65atr", "bafyt65a"), (R3, "3kt65above", "bafyt65b")] {
        let record = h.f.post_record(ALICE, rkey, &base, None).await;
        assert_eq!(h.fire(ALICE, r, vec![upsert(NSID_POST_PRIVATE, rkey, cid, record)]).await.outcomes, [OpOutcome::Written], "{rkey}");
    }
    let record = h.f.post_record(ALICE, "3kt65pst", &base, None).await;
    assert_eq!(h.fire(ALICE, R2, vec![upsert(NSID_POST_PRIVATE, "3kt65pst", "bafyt65c", record)]).await.outcomes, [OpOutcome::Unchanged]);
    h.finish().await;
}

in_both_modes!(t65_at_or_above_r: t65_at_or_above_r_on, t65_at_or_above_r_off);

// ---- T-81: the raise's `INSERT` branch ----

/// T-81: a tracked DID with no `sync_state` row gets one holding only the
/// listed rev; an untracked DID gets none, and nothing is raised.
async fn t81_the_insert_branch(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    exec(h.pool(), &format!("DELETE FROM sync_state WHERE did = '{ALICE}'")).await;
    let until = std::time::Instant::now() + LISTED_REV_LOCK_DEADLINE;
    let post = ListedRevs::of(NSID_POST_PRIVATE).expect("a grant collection");
    assert_eq!(ListedRevs::of(NSID_AUDIENCE), None, "audiences are not listed");
    assert_eq!(raise_listed_rev(&h.f.writer, ALICE, R1, post, until, None).await.expect("raise"), Raise::Raised);
    let stored: (Option<String>, Option<String>, Option<String>) =
        sqlx::query_as("SELECT applied_rev, head_rev, post_listed_rev FROM sync_state WHERE did = $1")
            .bind(ALICE)
            .fetch_one(h.pool())
            .await
            .expect("a row");
    assert_eq!(stored, (None, None, Some(R1.to_string())));
    assert_eq!(raise_listed_rev(&h.f.writer, NOBODY, R1, ListedRevs::POST, until, None).await.expect("raise"), Raise::Untracked);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM sync_state WHERE did = $1").bind(NOBODY).fetch_one(h.pool()).await.expect("count");
    assert_eq!(rows, 0);
    h.finish().await;
}

in_both_modes!(t81_the_insert_branch: t81_the_insert_branch_on, t81_the_insert_branch_off);

// ---- T-82: the check's boundary ----

/// T-82: a skipped row at exactly R with another CID is not flagged; one
/// below R with another CID is; one below R with the listed CID is not; an
/// audience is never checked. T-79 (d): an older job at R_j < R writes a
/// newer version onto a skipped URI before the raise: the catch-up ends
/// `replayed_below`, and the next catch-up settles.
async fn t82_the_checks_boundary(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t82").await;
    let rkey = "3kt82pst";
    let a = h.post(ALICE, rkey, &base, None).await;
    let cid = format!("bafy{rkey}");
    let check = |nsid: &'static str, rev: &'static str, listed_cid: String, uri: String| {
        let pool = h.pool().clone();
        async move {
            let mut conn = pool.acquire().await.expect("conn");
            KryphocronPrivateTier.replayed_since_listing(&mut conn, nsid, &[uri], &[listed_cid], rev).await.expect("check")
        }
    };
    exec(h.pool(), &format!("UPDATE private_posts SET rev = '{R2}' WHERE uri = '{a}'")).await;
    assert!(!check(NSID_POST_PRIVATE, R2, "bafyother".into(), a.clone()).await, "at exactly R");
    exec(h.pool(), &format!("UPDATE private_posts SET rev = '{R1}' WHERE uri = '{a}'")).await;
    assert!(check(NSID_POST_PRIVATE, R2, "bafyother".into(), a.clone()).await, "below R, another CID");
    assert!(!check(NSID_POST_PRIVATE, R2, cid.clone(), a.clone()).await, "below R, the listed CID");
    exec(h.pool(), &format!("UPDATE private_audiences SET rev = '{R1}' WHERE uri = '{base}'")).await;
    assert!(!check(NSID_AUDIENCE, R2, "bafyother".into(), base.clone()).await, "audiences are never checked");
    exec(h.pool(), &format!("UPDATE private_posts SET rev = '{REV}' WHERE uri = '{a}'")).await;

    // T-79 (d).
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    h.pds.set_rev(ALICE, R3);
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R3);
    hook.reached().await;
    let newer = h.f.post_record(ALICE, rkey, &base, None).await;
    let report = h.f.writer.apply(ALICE, R1, &[upsert(NSID_POST_PRIVATE, rkey, "bafyt82new", newer.clone())], job_write()).await.expect("apply");
    assert_eq!(report.outcomes, [OpOutcome::Written]);
    h.pds.put_cid(ALICE, NSID_POST_PRIVATE, rkey, "bafyt82new", newer);
    hook.release();
    assert!(!catch.await.expect("task"));
    drop(hook);
    assert_eq!(catch_up_outcome(&handle, "replayed_below"), 1.0);
    assert_eq!(listed(&h, ALICE).await, None);
    assert!(h.catch_up(ALICE, R3).await, "the next catch-up settles");
    assert_eq!(listed(&h, ALICE).await, at(R3));
    h.finish().await;
}

in_both_modes!(t82_the_checks_boundary: t82_the_checks_boundary_on, t82_the_checks_boundary_off);

// ---- T-66: catch-up ----

/// T-66: the listed rev rises after catch-up's listings and before its
/// reconciles; a CID-skipped row nothing changed is untouched; a catch-up
/// that stops at the page cap raises nothing; one that ends
/// `reconcile_incomplete` leaves it raised.
async fn t66_catch_up(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t66").await;
    let kept = h.post(ALICE, "3kt66pst", &base, None).await;
    let gone = h.post(ALICE, "3kt66gon", &base, None).await;
    h.pds.remove(ALICE, NSID_POST_PRIVATE, "3kt66gon");
    h.pds.set_rev(ALICE, R1);
    let run = |cfg: CatchUpConfig| {
        let (pds, writer) = (Arc::clone(&h.pds), h.f.writer.clone());
        async move { catch_up(pds.as_ref(), &writer, &CatchUp::new(cfg), ALICE, PDS, R1).await }
    };
    assert!(!run(CatchUpConfig { max_pages: 1, ..roomy() }).await, "the page cap");
    assert_eq!(listed(&h, ALICE).await, None);
    assert!(!run(CatchUpConfig { reconcile_confirm_limit: 0, ..roomy() }).await, "reconcile_incomplete");
    assert_eq!(listed(&h, ALICE).await, at(R1));
    assert!(row(&h, "private_posts", &gone).await.is_some());

    h.pds.set_rev(ALICE, R2);
    let before = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let after = hooks::arm(hooks::LISTED_REV_AFTER_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R2);
    before.reached().await;
    assert_eq!(listed(&h, ALICE).await, at(R1), "listings done, nothing raised");
    before.release();
    after.reached().await;
    assert_eq!(listed(&h, ALICE).await, at(R2), "raised");
    assert!(row(&h, "private_posts", &gone).await.is_some(), "not yet reconciled");
    after.release();
    assert!(catch.await.expect("task"));
    assert_eq!(row(&h, "private_posts", &gone).await, None);
    assert_eq!(row(&h, "private_posts", &kept).await, Some((REV.to_string(), Some("bafy3kt66pst".to_string()))), "skipped, untouched");
    h.finish().await;
}

in_both_modes!(t66_catch_up: t66_catch_up_on, t66_catch_up_off);

// ---- T-67: sources ----

/// T-67: `requestRecord` at R ≥ the listed rev writes. A job whose R is
/// below a listed rev a newer job raised writes `Unchanged`, not `Failed`,
/// so it settles, and `applied_rev` stays the higher. A `requestRecord`
/// whose R is below a run's listed rev reports `unchanged`. Passes at HEAD
/// too: non-regression.
async fn t67_sources(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t67").await;
    let b = h.backfill();
    h.pds.set_rev(ALICE, R2);
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready);
    h.pds.put(ALICE, NSID_POST_PRIVATE, "3kt67new", h.f.post_record(ALICE, "3kt67new", &base, None).await);
    h.pds.set_rev(ALICE, R3);
    let indexed = index_record(h.pds.as_ref(), &h.f.writer, ALICE, NSID_POST_PRIVATE, "3kt67new", 1, false).await.expect("index");
    assert_eq!(indexed.status, IndexStatus::Indexed);

    // A newer job at R4, then an older one at R3 whose listing saw a record
    // deleted before the newer listing.
    h.pds.set_rev(ALICE, R4);
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready);
    h.pds.put(ALICE, NSID_POST_PRIVATE, "3kt67old", h.f.post_record(ALICE, "3kt67old", &base, None).await);
    h.pds.set_rev(ALICE, R3);
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready, "no write failed");
    assert_eq!(row(&h, "private_posts", &uri(ALICE, NSID_POST_PRIVATE, "3kt67old")).await, None);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R4));
    assert_eq!(listed(&h, ALICE).await, at(R4));

    h.pds.put(ALICE, NSID_POST_PRIVATE, "3kt67req", h.f.post_record(ALICE, "3kt67req", &base, None).await);
    let indexed = index_record(h.pds.as_ref(), &h.f.writer, ALICE, NSID_POST_PRIVATE, "3kt67req", 1, false).await.expect("index");
    assert_eq!(indexed.status, IndexStatus::Unchanged);
    assert_eq!(row(&h, "private_posts", &uri(ALICE, NSID_POST_PRIVATE, "3kt67req")).await, None);
    h.finish().await;
}

in_both_modes!(t67_sources: t67_sources_on, t67_sources_off);

// ---- T-68: deny-only classes ----

/// T-68: below the listed rev, a block, a threadgate of a private post and
/// a follow are applied, and so is a delete.
async fn t68_deny_only_classes(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t68").await;
    let p = h.post(ALICE, "3kt68pst", &base, None).await;
    h.pds.set_rev(ALICE, R3);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    assert_eq!(listed(&h, ALICE).await, at(R3));
    let at_time = "2026-10-01T00:00:00Z";
    for (collection, rkey, record) in [
        ("app.bsky.graph.block", "3kt68blk", json!({"$type": "app.bsky.graph.block", "subject": DAVE, "createdAt": at_time})),
        (THREADGATE, "3kt68pst", json!({"$type": THREADGATE, "post": p, "allow": [], "createdAt": at_time})),
        (FOLLOW, "3kt68fol", follow_record(CAROL)),
    ] {
        let report = h.fire(ALICE, R1, vec![upsert(collection, rkey, &format!("bafy{rkey}"), record)]).await;
        assert_eq!(report.outcomes, [OpOutcome::Written], "{collection}");
    }
    // A row kept at an old rev (as catch-up keeps one by CID), deleted
    // below the listed rev.
    let kept = uri(ALICE, NSID_POST_PRIVATE, "3kt68old");
    exec(
        h.pool(),
        &format!(
            "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, plaintext_text, created_at, display_filled) \
             VALUES ('{kept}', '{ALICE}', '3kt68old', 'bafyold', '{REV}', '{base}', 'x', now(), true)"
        ),
    )
    .await;
    assert_eq!(h.fire(ALICE, R1, vec![delete(NSID_POST_PRIVATE, "3kt68old")]).await.outcomes, [OpOutcome::Written]);
    assert_eq!(row(&h, "private_posts", &kept).await, None);
    h.finish().await;
}

in_both_modes!(t68_deny_only_classes: t68_deny_only_classes_on, t68_deny_only_classes_off);

// ---- T-69: settle coverage ----

/// T-69: a job settle whose listed rev is below R (the raise skipped by a
/// hook) leaves `applied_rev` unchanged, counts `settle_uncovered`, and
/// writes the flag NULL after a complete follows pass, which queues the
/// retry; the `INSERT` branch writes `applied_rev` NULL. The same for
/// catch-up.
async fn t69_settle_coverage(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    h.alice_following("t69").await;
    let b = h.backfill();
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let uncovered = |path: &str| series(&handle.render(), METRIC_SETTLE_UNCOVERED, &[&format!("path=\"{path}\"")]);
    h.pds.set_rev(ALICE, R1);
    let skip = hooks::arm(hooks::LISTED_REV_SKIP_RAISE, &h.key(ALICE));
    set_follows_verified(h.pool(), ALICE, true).await;
    h.clear_jobs().await;
    assert_eq!(h.run(&b, ALICE, Depth::Authz).await, JobOutcome::Ready);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(REV));
    assert_eq!(listed(&h, ALICE).await, None);
    assert!(!h.flag(ALICE).await.set);
    let job = h.job(ALICE, Depth::Authz).await.expect("authz");
    assert_eq!((job.0.as_str(), job.1.as_deref()), ("queued", Some(FOLLOWS_RETRY)), "the retry");
    assert_eq!(uncovered("job"), Some(1.0));

    set_follows_verified(h.pool(), ALICE, true).await;
    assert!(!h.catch_up(ALICE, R1).await);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(REV));
    assert!(!h.flag(ALICE).await.set);
    assert_eq!(uncovered("catch_up"), Some(1.0));
    drop(skip);

    // The `INSERT` branch: the row deleted mid-run.
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    h.clear_jobs().await;
    h.enqueue(ALICE, Depth::Authz).await;
    let run = h.spawn_run(&b, ALICE, Depth::Authz);
    hook.reached().await;
    exec(h.pool(), &format!("DELETE FROM sync_state WHERE did = '{ALICE}'")).await;
    hook.release();
    run.await.expect("task").expect("db").expect("job");
    drop(hook);
    assert_eq!(h.applied(ALICE).await, None);
    assert_eq!(uncovered("job"), Some(2.0));

    h.ready_again().await;
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R2);
    h.pds.set_rev(ALICE, R2);
    hook.reached().await;
    exec(h.pool(), &format!("DELETE FROM sync_state WHERE did = '{ALICE}'")).await;
    hook.release();
    assert!(!catch.await.expect("task"));
    drop(hook);
    assert_eq!(h.applied(ALICE).await, None);
    assert_eq!(uncovered("catch_up"), Some(2.0));
    h.finish().await;
}

in_both_modes!(t69_settle_coverage: t69_settle_coverage_on, t69_settle_coverage_off);

/// T-A11 (private-audiences design §13): a job settle and a catch-up
/// settle raise `applied_rev` with only the post listed rev raised; the
/// settle is covered and `settle_uncovered_total` does not move.
async fn t_a11_settles_cover_without_audiences(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    owner(&h, "ta11").await;
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    h.pds.set_rev(ALICE, R1);
    h.clear_jobs().await;
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    assert_eq!((listed(&h, ALICE).await, h.applied(ALICE).await), (at(R1), at(R1)));
    h.pds.set_rev(ALICE, R2);
    assert!(h.catch_up(ALICE, R2).await);
    assert_eq!((listed(&h, ALICE).await, h.applied(ALICE).await), (at(R2), at(R2)));
    assert_eq!(series(&handle.render(), METRIC_SETTLE_UNCOVERED, &[]).unwrap_or(0.0), 0.0);
    h.finish().await;
}

in_both_modes!(t_a11_settles_cover_without_audiences: t_a11_settles_cover_on, t_a11_settles_cover_off);

// ---- T-70: lock contention ----

/// T-70: O's lock held shared past the deadline. Another shared locker is
/// never blocked by the raise; the job re-queues a minute on with
/// `attempts` unchanged and settles nothing; the catch-up ends
/// `listed_rev_busy`.
async fn t70_lock_contention(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    owner(&h, "t70").await;
    let b = h.backfill();
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    h.pds.set_rev(ALICE, R1);
    let held = hold_shared(h.pool(), ALICE).await;
    h.clear_jobs().await;
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    until_retrying(&handle).await;
    shared_locker_passes(h.pool(), ALICE).await;
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Requeued("listed_rev_busy"));
    let job = h.job(ALICE, Depth::Private).await.expect("private");
    assert_eq!((job.0.as_str(), job.3), ("queued", 0), "{job:?}");
    assert!((30..=90).contains(&job.2), "a minute on: {job:?}");
    assert_eq!(listed(&h, ALICE).await, None);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(REV));

    assert!(!h.catch_up(ALICE, R1).await);
    assert_eq!(catch_up_outcome(&handle, "listed_rev_busy"), 1.0);
    assert_eq!(listed(&h, ALICE).await, None);
    held.rollback().await.expect("rollback");
    h.finish().await;
}

in_both_modes!(t70_lock_contention: t70_lock_contention_on, t70_lock_contention_off);

// ---- T-71: the migration, and the pre-release count ----

/// The pre-release count (§9): the `sync_state` rows migration 0030's seed
/// gives listed revs, those of them with no `firehose_rev` (where
/// `applied_rev` may be the firehose's own, §9's imperfect seed), and every
/// row. Read-only; it names only columns that exist before 0030.
const PRE_0030_COUNT_SQL: &str = "SELECT \
  count(*) FILTER (WHERE applied_rev IS NOT NULL AND (firehose_rev IS NULL OR firehose_rev < applied_rev)) AS seeded, \
  count(*) FILTER (WHERE applied_rev IS NOT NULL AND firehose_rev IS NULL) AS seeded_without_firehose_rev, \
  count(*) AS sync_state_rows \
FROM sync_state";

/// Prints the pre-release count for skydeval to run read-only on the
/// deployed database before migration 0030:
/// `cargo test pre_listed_revs_count_query -- --ignored --nocapture`.
#[test]
#[ignore = "prints the pre-0030 count query; run with --ignored --nocapture"]
fn pre_listed_revs_count_query() {
    println!("-- #723 pre-release count: the rows migration 0030 seeds\n{PRE_0030_COUNT_SQL};");
}

/// T-71: a row with `applied_rev` ahead of `firehose_rev` is seeded; one
/// with `firehose_rev ≥ applied_rev` is not; one with `firehose_rev` NULL
/// and `applied_rev` set is. `sync_state`'s `relfilenode` is unchanged
/// across the `ALTER`. The pre-release count counts the same rows. (0030
/// added both listed revs; 0052 later drops the audience one.)
#[tokio::test]
async fn t71_the_migration_seeds_settled_rows() {
    let Some(db) = crate::test_support::empty_db("i723-t71").await else {
        return;
    };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 30).expect("0030");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    for (did, applied, firehose) in [
        ("did:plc:ahead", Some(R2), Some(R1)),
        ("did:plc:behind", Some(R1), Some(R2)),
        ("did:plc:equal", Some(R1), Some(R1)),
        ("did:plc:nofirehose", Some(R1), None),
        ("did:plc:nothing", None, None),
    ] {
        sqlx::query("INSERT INTO sync_state (did, applied_rev, firehose_rev) VALUES ($1, $2, $3)")
            .bind(did)
            .bind(applied)
            .bind(firehose)
            .execute(p)
            .await
            .expect("row");
    }
    let counts: (i64, i64, i64) = sqlx::query_as(PRE_0030_COUNT_SQL).fetch_one(p).await.expect("count query");
    assert_eq!(counts, (2, 1, 5));
    let relfilenode = || async {
        let n: i64 = sqlx::query_scalar("SELECT relfilenode::bigint FROM pg_class WHERE relname = 'sync_state'").fetch_one(p).await.expect("pg_class");
        n
    };
    let before = relfilenode().await;
    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0030");
    assert_eq!(relfilenode().await, before, "no rewrite");
    let rows: Vec<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT did, post_listed_rev, audience_listed_rev FROM sync_state ORDER BY did").fetch_all(p).await.expect("rows");
    let r = |s: &str| Some(s.to_string());
    assert_eq!(
        rows,
        [
            ("did:plc:ahead".to_string(), r(R2), r(R2)),
            ("did:plc:behind".to_string(), None, None),
            ("did:plc:equal".to_string(), None, None),
            ("did:plc:nofirehose".to_string(), r(R1), r(R1)),
            ("did:plc:nothing".to_string(), None, None),
        ]
    );
    db.drop_db().await;
}

// ---- T-72: untracked to tracked ----

/// T-72: a replayed post of a DID with no `sync_state` row is inserted; the
/// DID becomes an owner; its `private` job raises the listed rev, and the
/// reconcile deletes the row before readiness is set. Passes at HEAD too:
/// non-regression.
async fn t72_untracked_to_tracked(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    actor(h.pool(), EVE, "eve.test", "eve.test").await;
    h.member_outside_s(EVE).await;
    let aud = h.direct(EVE, NSID_AUDIENCE, "3kt72aud", audience_record("list", &[BOB])).await;
    let rec = h.f.post_record(EVE, "3kt72rep", &aud, None).await;
    let replayed = h.direct(EVE, NSID_POST_PRIVATE, "3kt72rep", rec).await;
    h.pds.remove(EVE, NSID_POST_PRIVATE, "3kt72rep");
    assert!(row(&h, "private_posts", &replayed).await.is_some(), "inserted: no listed rev");
    let rec = h.f.post_record(EVE, "3kt72kep", &aud, None).await;
    let kept = h.direct(EVE, NSID_POST_PRIVATE, "3kt72kep", rec).await;
    h.f.writer.scope().write_guard().await.private_owners.insert(EVE.into());
    h.pds.set_rev(EVE, R1);
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(EVE));
    h.clear_jobs().await;
    h.enqueue(EVE, Depth::Private).await;
    let run = h.spawn_run(&h.backfill(), EVE, Depth::Private);
    hook.reached().await;
    assert_eq!(listed(&h, EVE).await, at(R1));
    assert_eq!(row(&h, "private_posts", &replayed).await, None, "reconciled");
    assert!(!h.owner_ready(EVE).await, "before readiness");
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    assert!(h.owner_ready(EVE).await);
    assert!(row(&h, "private_posts", &kept).await.is_some());
    assert!(row(&h, "private_audiences", &aud).await.is_some(), "the held audience is never reconciled");
    h.finish().await;
}

in_both_modes!(t72_untracked_to_tracked: t72_untracked_to_tracked_on, t72_untracked_to_tracked_off);

// ---- T-74, T-75, T-77: a stale version after catch-up ----

const STALE_RKEY: &str = "3kt74pst";
const U_PRIME_CID: &str = "bafyt74uprime";
const U_CID: &str = "bafyt74u";

/// O's post A with versions U' at R2 (under O's audience listing V) and U
/// at R3 (under an audience listing no one). A job's live listing stored
/// U's content at R_j = R1 < R2; O's head is now R4.
struct Stale {
    a: String,
    u_prime: RawOp,
    u: RawOp,
}

async fn stale_version(h: &H) -> Stale {
    let wide = owner(h, "t74").await;
    let narrow = h.audience_of(ALICE, "3kt74nar", "list", &[]).await;
    let u_prime = h.f.post_record(ALICE, STALE_RKEY, &wide, None).await;
    let u = h.f.post_record(ALICE, STALE_RKEY, &narrow, None).await;
    h.pds.put_cid(ALICE, NSID_POST_PRIVATE, STALE_RKEY, U_CID, u.clone());
    h.pds.set_rev(ALICE, R1);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    let a = uri(ALICE, NSID_POST_PRIVATE, STALE_RKEY);
    assert_eq!(row(h, "private_posts", &a).await, Some((R1.to_string(), Some(U_CID.to_string()))));
    h.pds.set_rev(ALICE, R4);
    Stale { a, u_prime: upsert(NSID_POST_PRIVATE, STALE_RKEY, U_PRIME_CID, u_prime), u: upsert(NSID_POST_PRIVATE, STALE_RKEY, U_CID, u) }
}

/// T-74: catch-up at R4 skips A by CID and settles; the replay of U' is
/// refused and V does not read A. With the refusal off, V reads: the HEAD
/// leak.
async fn t74_a_stale_version_after_catch_up(test: &str, mode: PublicMode, refusal: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let _off = refusal_off(&h, ALICE, refusal);
    let s = stale_version(&h).await;
    assert!(h.catch_up(ALICE, R4).await);
    assert_eq!(row(&h, "private_posts", &s.a).await, Some((R1.to_string(), Some(U_CID.to_string()))), "skipped by CID");
    assert_eq!(listed(&h, ALICE).await, at(R4));
    let report = h.fire(ALICE, R2, vec![s.u_prime.clone()]).await;
    control(&h, ALICE, BOB, R4).await;
    if refusal {
        assert_eq!(report.outcomes, [OpOutcome::Unchanged]);
        assert!(misses(&h, BOB, &s.a).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Written]);
        assert!(h.reads(BOB, &s.a).await, "the HEAD leak");
    }
    h.finish().await;
}

with_control!(t74_a_stale_version_after_catch_up: t74_stale_version_on, t74_stale_version_off, t74_control_on, t74_control_off);

/// T-75: as T-74, with the replay of U' committed after catch-up read its
/// stored CIDs and before its raise. The check finds A; the raise rolls
/// back; catch-up ends `replayed_below` with no listed rev raised and
/// `applied_rev` where the firehose left it; V misses on the head check
/// (the cause shown: with the head at `applied_rev`, V would read) until U
/// replays, after which V does not read. Without the check (`check` false):
/// the raise and settle commit, and V reads A at U' with `applied_rev ≥
/// head`.
async fn t75_catch_ups_check(test: &str, mode: PublicMode, check: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let s = stale_version(&h).await;
    let _off = (!check).then(|| hooks::arm(hooks::LISTED_REV_CHECK_OFF, &h.key(ALICE)));
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R4);
    hook.reached().await;
    assert_eq!(h.fire(ALICE, R2, vec![s.u_prime.clone()]).await.outcomes, [OpOutcome::Written], "above the listed rev R1");
    hook.release();
    let caught = catch.await.expect("task");
    drop(hook);
    if check {
        assert!(!caught);
        assert_eq!(catch_up_outcome(&handle, "replayed_below"), 1.0);
        assert_eq!(listed(&h, ALICE).await, at(R1));
        assert_eq!(h.applied(ALICE).await.as_deref(), Some(R2), "the firehose's, not moved by the catch-up");
        exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R4}', head_checked_at = now() WHERE did = '{ALICE}'")).await;
        assert!(h.owner_ready(ALICE).await && h.member_ready(BOB).await);
        assert!(oracle_admits(h.pool(), BOB, &s.a).await, "the row admits V");
        assert!(!h.served(BOB, &s.a).await, "the head check");
        exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R2}' WHERE did = '{ALICE}'")).await;
        assert!(h.served(BOB, &s.a).await, "the head check was the cause");
        exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R4}' WHERE did = '{ALICE}'")).await;
        assert_eq!(h.fire(ALICE, R3, vec![s.u.clone()]).await.outcomes, [OpOutcome::Written], "U by last-writer-wins");
        assert!(!h.served(BOB, &s.a).await);
        assert!(!oracle_admits(h.pool(), BOB, &s.a).await);
    } else {
        assert!(caught);
        assert_eq!(listed(&h, ALICE).await, at(R4));
        control(&h, ALICE, BOB, R4).await;
        assert!(h.reads(BOB, &s.a).await, "the leak the check closes");
    }
    h.finish().await;
}

with_control!(t75_catch_ups_check: t75_catch_ups_check_on, t75_catch_ups_check_off, t75_control_on, t75_control_off);

/// T-77: as T-75, with the catch-up ended by an injected error after its
/// listings and before the raise: no listed rev changes; U then applies by
/// last-writer-wins; V does not read.
async fn t77_error_before_the_raise(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let s = stale_version(&h).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let _fail = hooks::arm(hooks::CATCH_UP_FAILS_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R4);
    hook.reached().await;
    h.fire(ALICE, R2, vec![s.u_prime.clone()]).await;
    hook.release();
    assert!(!catch.await.expect("task"));
    assert_eq!(listed(&h, ALICE).await, at(R1));
    assert_eq!(h.fire(ALICE, R3, vec![s.u.clone()]).await.outcomes, [OpOutcome::Written]);
    assert!(!h.reads(BOB, &s.a).await);
    h.finish().await;
}

in_both_modes!(t77_error_before_the_raise: t77_error_before_the_raise_on, t77_error_before_the_raise_off);

/// T-77: the same with the handler's future dropped there.
async fn t77_dropped_before_the_raise(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let s = stale_version(&h).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R4);
    hook.reached().await;
    h.fire(ALICE, R2, vec![s.u_prime.clone()]).await;
    catch.abort();
    assert!(catch.await.expect_err("dropped").is_cancelled());
    drop(hook);
    assert_eq!(listed(&h, ALICE).await, at(R1));
    assert_eq!(h.fire(ALICE, R3, vec![s.u.clone()]).await.outcomes, [OpOutcome::Written]);
    assert!(!h.reads(BOB, &s.a).await);
    h.finish().await;
}

in_both_modes!(t77_dropped_before_the_raise: t77_dropped_before_the_raise_on, t77_dropped_before_the_raise_off);

/// T-77: no replay before the raise, and the error injected after the raise
/// commits: the stored A is the listed version, and a later replay of U' is
/// refused.
async fn t77_error_after_the_raise(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let s = stale_version(&h).await;
    let _fail = hooks::arm(hooks::CATCH_UP_FAILS_AFTER_RAISE, &h.key(ALICE));
    assert!(!h.catch_up(ALICE, R4).await);
    assert_eq!(listed(&h, ALICE).await, at(R4));
    assert_eq!(row(&h, "private_posts", &s.a).await, Some((R1.to_string(), Some(U_CID.to_string()))), "the listed version");
    assert_eq!(h.fire(ALICE, R2, vec![s.u_prime.clone()]).await.outcomes, [OpOutcome::Unchanged]);
    assert!(!h.reads(BOB, &s.a).await);
    h.finish().await;
}

in_both_modes!(t77_error_after_the_raise: t77_error_after_the_raise_on, t77_error_after_the_raise_off);

// ---- T-76: no raise over a failed write ----

/// T-76: A, O's post, stored at X0 under an audience listing V; O's next
/// version X1 of A has an `audienceList` that is no at-uri; then an
/// unrelated commit X2. A job at X2 fails on A: the listed rev is not
/// raised, and the settle takes the no-readiness branch, leaving O ready.
/// The consumer replays X1, then X2. As built (`default`): X1 fails on the
/// firehose, O's readiness is cleared, and V misses on readiness alone.
/// With the raise forced: X1 is refused as `Unchanged`, X2 brings
/// `applied_rev` to the head, O stays ready, and V reads A at X0: the leak
/// this guards against.
async fn t76_no_raise_over_a_failed_write(test: &str, mode: PublicMode, default: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t76").await;
    let a = h.post(ALICE, "3kt76pst", &base, None).await;
    let mut x1 = h.f.post_record(ALICE, "3kt76pst", &base, None).await;
    x1["audienceList"] = json!(3);
    h.pds.put_cid(ALICE, NSID_POST_PRIVATE, "3kt76pst", "bafyt76x1", x1.clone());
    h.pds.set_rev(ALICE, R2);
    let _force = (!default).then(|| hooks::arm(hooks::LISTED_REV_FORCE_RAISE, &h.key(ALICE)));
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Done);
    assert!(h.owner_ready(ALICE).await);
    let expected = if default { None } else { at(R2) };
    assert_eq!(listed(&h, ALICE).await, expected);
    exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R2}', head_checked_at = now() WHERE did = '{ALICE}'")).await;

    let report = h.fire(ALICE, R1, vec![upsert(NSID_POST_PRIVATE, "3kt76pst", "bafyt76x1", x1)]).await;
    if default {
        assert!(report.readiness_cleared, "{report:?}");
        assert!(!h.owner_ready(ALICE).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Unchanged]);
        assert!(h.owner_ready(ALICE).await);
    }
    h.commit_nothing(ALICE, R2).await;
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R2), "applied_rev at the head");
    assert!(h.member_ready(BOB).await);
    if default {
        assert!(!h.owner_ready(ALICE).await);
        assert!(!h.served(BOB, &a).await, "readiness alone");
    } else {
        assert!(h.owner_ready(ALICE).await);
        assert!(h.reads(BOB, &a).await, "the leak");
    }
    h.finish().await;
}

with_control!(t76_no_raise_over_a_failed_write: t76_no_raise_over_a_failed_write_on, t76_no_raise_over_a_failed_write_off, t76_control_on, t76_control_off);

// ---- T-78: between reconcile and settle ----

/// T-78: a replay that commits after the job's reconciles and before its
/// settle returns `Unchanged`; V does not read after the settle. With the
/// refusal off, the row survives the settle and V reads (HEAD).
async fn t78_between_reconcile_and_settle(test: &str, mode: PublicMode, refusal: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let _off = refusal_off(&h, ALICE, refusal);
    let base = owner(&h, "t78").await;
    let b = h.backfill();
    let s = Scene::new(&h, "t78", &base, 0).await;
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    let report = h.fire(ALICE, &s.c1, vec![s.replay()]).await;
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    drop(hook);
    control(&h, ALICE, BOB, &s.d).await;
    if refusal {
        assert_eq!(report.outcomes, [OpOutcome::Unchanged]);
        assert_eq!(row(&h, "private_posts", &s.q).await, None);
        assert!(misses(&h, BOB, &s.q).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Written]);
        assert!(row(&h, "private_posts", &s.q).await.is_some(), "survives the settle");
        assert!(h.reads(BOB, &s.q).await);
    }
    h.finish().await;
}

with_control!(t78_between_reconcile_and_settle: t78_between_reconcile_and_settle_on, t78_between_reconcile_and_settle_off, t78_control_on, t78_control_off);

// ---- T-79: newer states are not replays ----

/// T-79: catch-up skips A by CID; before its raise, (a) a live delete of A
/// above R, (b) a live edit above R, (c) a concurrent `requestRecord` above
/// R that writes A. In each the check finds nothing, the raise commits on
/// its first try, and catch-up reconciles and settles. (d) is in T-82.
async fn t79_newer_states_are_not_replays(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t79").await;
    for rkey in ["3kt79pst1", "3kt79pst2", "3kt79pst3"] {
        h.post(ALICE, rkey, &base, None).await;
    }
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    for (case, n) in [("delete", 0), ("edit", 2), ("requestRecord", 4)] {
        let (r, above) = (rev(n), rev(n + 1));
        h.pds.set_rev(ALICE, &r);
        let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
        let catch = spawn_catch_up(&h, ALICE, &r);
        hook.reached().await;
        match case {
            "delete" => {
                h.pds.remove(ALICE, NSID_POST_PRIVATE, "3kt79pst1");
                h.pds.set_rev(ALICE, &above);
                assert_eq!(h.fire(ALICE, &above, vec![delete(NSID_POST_PRIVATE, "3kt79pst1")]).await.outcomes, [OpOutcome::Written]);
            }
            "edit" => {
                let edited = h.f.post_record(ALICE, "3kt79pst2", &base, None).await;
                h.pds.put_cid(ALICE, NSID_POST_PRIVATE, "3kt79pst2", "bafyt79edit", edited.clone());
                h.pds.set_rev(ALICE, &above);
                let report = h.fire(ALICE, &above, vec![upsert(NSID_POST_PRIVATE, "3kt79pst2", "bafyt79edit", edited)]).await;
                assert_eq!(report.outcomes, [OpOutcome::Written]);
            }
            _ => {
                let edited = h.f.post_record(ALICE, "3kt79pst3", &base, None).await;
                h.pds.put_cid(ALICE, NSID_POST_PRIVATE, "3kt79pst3", "bafyt79req", edited);
                h.pds.set_rev(ALICE, &above);
                let indexed = index_record(h.pds.as_ref(), &h.f.writer, ALICE, NSID_POST_PRIVATE, "3kt79pst3", 1, false).await.expect("index");
                assert_eq!(indexed.status, IndexStatus::Indexed);
            }
        }
        hook.release();
        assert!(catch.await.expect("task"), "{case}");
        drop(hook);
        assert_eq!(listed(&h, ALICE).await, at(&r), "{case}");
        assert_eq!(series(&handle.render(), METRIC_LISTED_REV_LOCK_RETRIES, &[]).unwrap_or(0.0), 0.0, "{case}: the first try");
    }
    assert_eq!(catch_up_outcome(&handle, "settled"), 3.0);
    h.finish().await;
}

in_both_modes!(t79_newer_states_are_not_replays: t79_newer_states_are_not_replays_on, t79_newer_states_are_not_replays_off);

// ---- T-80: no resurrection ----

/// T-80 (review 4's ordering): O an owner outside S; catch-up C skips A,
/// O's post; O deletes A at D' > R; an older job reconciles A away with a
/// tombstone at R_j < R; the firehose applies D' with no row and no
/// tombstone. C's check finds nothing, C raises and settles, and A stays
/// absent: V does not read.
async fn t80_no_resurrection(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    actor(h.pool(), EVE, "eve.test", "eve.test").await;
    h.member_outside_s(EVE).await;
    exec(
        h.pool(),
        &format!("INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at, firehose_rev) VALUES ('{EVE}', '{REV}', '{REV}', now(), '{REV}')"),
    )
    .await;
    h.f.writer.scope().write_guard().await.private_owners.insert(EVE.into());
    let aud = h.direct(EVE, NSID_AUDIENCE, "3kt80aud", audience_record("list", &[BOB])).await;
    let rkey = "3kt80pst";
    let p_rec = h.f.post_record(EVE, rkey, &aud, None).await;
    let a = h.direct(EVE, NSID_POST_PRIVATE, rkey, p_rec).await;
    h.f.ready(EVE).await;
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    h.pds.set_rev(EVE, R2);
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(EVE));
    let catch = spawn_catch_up(&h, EVE, R2);
    hook.reached().await;
    h.pds.remove(EVE, NSID_POST_PRIVATE, rkey);
    h.pds.set_rev(EVE, R3);
    let report = h.f.writer.apply(EVE, R1, &[delete(NSID_POST_PRIVATE, rkey)], job_write()).await.expect("apply");
    assert_eq!(report.outcomes, [OpOutcome::Written], "the older job's reconcile");
    h.fire(EVE, R3, vec![delete(NSID_POST_PRIVATE, rkey)]).await;
    let tombstone: Option<String> = sqlx::query_scalar("SELECT rev FROM private_tombstones WHERE uri = $1").bind(&a).fetch_optional(h.pool()).await.expect("tombstone");
    assert_eq!(tombstone.as_deref(), Some(R1), "no tombstone at D'");
    hook.release();
    catch.await.expect("task");
    drop(hook);
    assert_eq!(catch_up_outcome(&handle, "settled"), 1.0);
    assert_eq!(listed(&h, EVE).await, at(R2));
    assert_eq!(row(&h, "private_posts", &a).await, None, "not resurrected");
    assert!(!h.served(BOB, &a).await);
    h.finish().await;
}

in_both_modes!(t80_no_resurrection: t80_no_resurrection_on, t80_no_resurrection_off);
