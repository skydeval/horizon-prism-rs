//! #723, replay below a settle (docs/internal/design/replay-readiness.md
//! §11): the listed revs, the ingest refusal, the settle's coverage and
//! catch-up's check, T-62 to T-82 (T-73 sits beside T-40). Built on the S10
//! harness, whose items this module sees: the firehose's own commits through
//! [`H::fire`], jobs and catch-ups from [`Pds`].
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

/// O (Alice) an owner, ready: an audience listing Bob, stored and in her
/// repository. Its rkey.
async fn owner(h: &H, tag: &str) -> String {
    let rkey = format!("3k{tag}base");
    h.audience_of(ALICE, &rkey, "list", &[BOB]).await;
    h.f.ready_all().await;
    rkey
}

/// `did`'s listed revs: `(post, audience)`.
async fn listed(h: &H, did: &str) -> (Option<String>, Option<String>) {
    sqlx::query_as::<_, (Option<String>, Option<String>)>("SELECT post_listed_rev, audience_listed_rev FROM sync_state WHERE did = $1")
        .bind(did)
        .fetch_optional(h.pool())
        .await
        .expect("sync_state")
        .unwrap_or((None, None))
}

fn both(r: &str) -> (Option<String>, Option<String>) {
    (Some(r.to_string()), Some(r.to_string()))
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

/// One ordering of the issue: in O's repository an audience A (mode
/// `mode`, Bob listed) created at C1, a post P under it, A deleted at D, the
/// head D. Nothing of it applied yet: the consumer is behind.
struct Scene {
    a_rkey: String,
    a: String,
    a_rec: Value,
    p: String,
    c1: String,
    c2: String,
    d: String,
}

impl Scene {
    async fn new(h: &H, tag: &str, mode: &str, n: u8) -> Self {
        let (a_rkey, p_rkey) = (format!("3k{tag}aud"), format!("3k{tag}pst"));
        let a = uri(ALICE, NSID_AUDIENCE, &a_rkey);
        let p_rec = h.f.post_record(ALICE, &p_rkey, &a, None).await;
        h.pds.put(ALICE, NSID_POST_PRIVATE, &p_rkey, p_rec);
        let (c1, c2, d) = (rev(n), rev(n + 1), rev(n + 2));
        h.pds.set_rev(ALICE, &d);
        Self { a_rec: audience_record(mode, &[BOB]), p: uri(ALICE, NSID_POST_PRIVATE, &p_rkey), a, a_rkey, c1, c2, d }
    }

    /// The consumer's replay of C1.
    fn replay(&self) -> RawOp {
        upsert(NSID_AUDIENCE, &self.a_rkey, &format!("bafy{}", self.a_rkey), self.a_rec.clone())
    }
}

// ---- T-62: the issue ----

/// T-62: a `private` job at R ≥ D settles; the replay of C1 returns
/// `Unchanged` and stores no A; V's `getPosts` of P misses byte-identically,
/// before and after D replays. For `list`, `followers` (V follows O) and
/// `everyone`. With the refusal off, V reads P: the HEAD behaviour.
async fn t62_the_issue(test: &str, mode: PublicMode, refusal: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let _off = refusal_off(&h, ALICE, refusal);
    owner(&h, "t62").await;
    exec(h.pool(), &follow_sql(BOB, ALICE)).await;
    let b = h.backfill();
    for (i, audience_mode) in ["list", "followers", "everyone"].into_iter().enumerate() {
        let s = Scene::new(&h, &format!("t62{i}"), audience_mode, u8::try_from(i * 3).expect("small")).await;
        assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready, "{audience_mode}");
        assert_eq!(listed(&h, ALICE).await, both(&s.d));
        assert_eq!(h.applied(ALICE).await.as_deref(), Some(s.d.as_str()));
        let report = h.fire(ALICE, &s.c1, vec![s.replay()]).await;
        control(&h, ALICE, BOB, &s.d).await;
        if refusal {
            assert_eq!(report.outcomes, [OpOutcome::Unchanged], "{audience_mode}");
            assert_eq!(row(&h, "private_audiences", &s.a).await, None);
            assert!(misses(&h, BOB, &s.p).await, "{audience_mode}");
        } else {
            assert_eq!(report.outcomes, [OpOutcome::Written], "{audience_mode}");
            assert!(h.reads(BOB, &s.p).await, "{audience_mode}: the HEAD leak");
        }
        // C2 (P itself) and D replay.
        let p_rkey = s.p.rsplit('/').next().expect("rkey").to_string();
        let p_rec = h.f.post_record(ALICE, &p_rkey, &s.a, None).await;
        h.fire(ALICE, &s.c2, vec![upsert(NSID_POST_PRIVATE, &p_rkey, &format!("bafy{p_rkey}"), p_rec)]).await;
        h.fire(ALICE, &s.d, vec![delete(NSID_AUDIENCE, &s.a_rkey)]).await;
        control(&h, ALICE, BOB, &s.d).await;
        assert!(misses(&h, BOB, &s.p).await, "{audience_mode}: after D");
    }
    h.finish().await;
}

with_control!(t62_the_issue: t62_the_issue_on, t62_the_issue_off, t62_control_on, t62_control_off);

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
    let mut record = h.f.post_record(ALICE, rkey, &uri(ALICE, NSID_AUDIENCE, &base), None).await;
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

/// T-64: (a) a replay committed between the audience listing and the raise
/// is deleted by the reconcile before the settle; (b) a writer holding the
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
    owner(&h, "t64").await;
    let b = h.backfill();
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);

    // (a) Before the raise.
    let s = Scene::new(&h, "t64a", "list", 0).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    assert_eq!(h.fire(ALICE, &s.c1, vec![s.replay()]).await.outcomes, [OpOutcome::Written], "below no listed rev yet");
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    drop(hook);
    assert_eq!(row(&h, "private_audiences", &s.a).await, None, "(a) reconciled");

    // (b) A writer in flight when the raise starts.
    let s = Scene::new(&h, "t64b", "list", 3).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    let mut writer_tx = hold_shared(h.pool(), ALICE).await;
    sqlx::query(
        "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, display_filled) \
         VALUES ($1, $2, $3, 'bafyt64b', $4, 'list', $5, true)",
    )
    .bind(&s.a)
    .bind(ALICE)
    .bind(&s.a_rkey)
    .bind(&s.c1)
    .bind(vec![BOB.to_string()])
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
    assert_eq!(row(&h, "private_audiences", &s.a).await, None, "(b) reconciled");

    // (c) Between the raise and the reconcile.
    let s = Scene::new(&h, "t64c", "list", 6).await;
    let hook = hooks::arm(hooks::LISTED_REV_AFTER_RAISE, &h.key(ALICE));
    h.enqueue(ALICE, Depth::Private).await;
    let run = h.spawn_run(&b, ALICE, Depth::Private);
    hook.reached().await;
    assert_eq!(listed(&h, ALICE).await.1.as_deref(), Some(s.d.as_str()), "the audience raise committed");
    let expected = if refusal { OpOutcome::Unchanged } else { OpOutcome::Written };
    assert_eq!(h.fire(ALICE, &s.c1, vec![s.replay()]).await.outcomes, [expected], "(c)");
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    drop(hook);
    assert_eq!(row(&h, "private_audiences", &s.a).await, None, "(c)");

    // (d) After the settle.
    let s = Scene::new(&h, "t64d", "list", 9).await;
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready);
    let report = h.fire(ALICE, &s.c1, vec![s.replay()]).await;
    control(&h, ALICE, BOB, &s.d).await;
    if refusal {
        assert_eq!(report.outcomes, [OpOutcome::Unchanged], "(d)");
        assert_eq!(row(&h, "private_audiences", &s.a).await, None);
        assert!(misses(&h, BOB, &s.p).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Written], "(d)");
        assert!(h.reads(BOB, &s.p).await, "(d): the HEAD leak");
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
    h.pds.set_rev(ALICE, R2);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    assert_eq!(listed(&h, ALICE).await, both(R2));
    let record = audience_record("list", &[BOB]);
    assert_eq!(h.fire(ALICE, R2, vec![upsert(NSID_AUDIENCE, "3kt65atr", "bafyt65a", record.clone())]).await.outcomes, [OpOutcome::Written]);
    assert_eq!(h.fire(ALICE, R3, vec![upsert(NSID_AUDIENCE, "3kt65above", "bafyt65b", record.clone())]).await.outcomes, [OpOutcome::Written]);
    assert_eq!(h.fire(ALICE, R2, vec![upsert(NSID_AUDIENCE, &base, "bafyt65c", record)]).await.outcomes, [OpOutcome::Unchanged]);
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
    assert_eq!(raise_listed_rev(&h.f.writer, ALICE, R1, post, until, None).await.expect("raise"), Raise::Raised);
    let stored: (Option<String>, Option<String>, Option<String>, Option<String>) =
        sqlx::query_as("SELECT applied_rev, head_rev, post_listed_rev, audience_listed_rev FROM sync_state WHERE did = $1")
            .bind(ALICE)
            .fetch_one(h.pool())
            .await
            .expect("a row");
    assert_eq!(stored, (None, None, Some(R1.to_string()), None));
    assert_eq!(raise_listed_rev(&h.f.writer, NOBODY, R1, ListedRevs::BOTH, until, None).await.expect("raise"), Raise::Untracked);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM sync_state WHERE did = $1").bind(NOBODY).fetch_one(h.pool()).await.expect("count");
    assert_eq!(rows, 0);
    h.finish().await;
}

in_both_modes!(t81_the_insert_branch: t81_the_insert_branch_on, t81_the_insert_branch_off);

// ---- T-82: the check's boundary ----

/// T-82: a skipped row at exactly R with another CID is not flagged; one
/// below R with another CID is; one below R with the listed CID is not.
/// T-79 (d): an older job at R_j < R writes a newer version onto a skipped
/// URI before the raise: the catch-up ends `replayed_below`, and the next
/// catch-up settles.
async fn t82_the_checks_boundary(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let rkey = "3kt82aud";
    let a = h.audience_of(ALICE, rkey, "list", &[BOB]).await;
    h.f.ready_all().await;
    let cid = format!("bafy{rkey}");
    let check = |rev: &'static str, listed_cid: String| {
        let (pool, a) = (h.pool().clone(), a.clone());
        async move {
            let mut conn = pool.acquire().await.expect("conn");
            KryphocronPrivateTier.replayed_since_listing(&mut conn, NSID_AUDIENCE, &[a], &[listed_cid], rev).await.expect("check")
        }
    };
    exec(h.pool(), &format!("UPDATE private_audiences SET rev = '{R2}' WHERE uri = '{a}'")).await;
    assert!(!check(R2, "bafyother".into()).await, "at exactly R");
    exec(h.pool(), &format!("UPDATE private_audiences SET rev = '{R1}' WHERE uri = '{a}'")).await;
    assert!(check(R2, "bafyother".into()).await, "below R, another CID");
    assert!(!check(R2, cid.clone()).await, "below R, the listed CID");
    exec(h.pool(), &format!("UPDATE private_audiences SET rev = '{REV}' WHERE uri = '{a}'")).await;

    // T-79 (d).
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    h.pds.set_rev(ALICE, R3);
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R3);
    hook.reached().await;
    let newer = audience_record("list", &[BOB, CAROL]);
    let report = h.f.writer.apply(ALICE, R1, &[upsert(NSID_AUDIENCE, rkey, "bafyt82new", newer.clone())], job_write()).await.expect("apply");
    assert_eq!(report.outcomes, [OpOutcome::Written]);
    h.pds.put_cid(ALICE, NSID_AUDIENCE, rkey, "bafyt82new", newer);
    hook.release();
    assert!(!catch.await.expect("task"));
    drop(hook);
    assert_eq!(catch_up_outcome(&handle, "replayed_below"), 1.0);
    assert_eq!(listed(&h, ALICE).await, (None, None));
    assert!(h.catch_up(ALICE, R3).await, "the next catch-up settles");
    assert_eq!(listed(&h, ALICE).await, both(R3));
    h.finish().await;
}

in_both_modes!(t82_the_checks_boundary: t82_the_checks_boundary_on, t82_the_checks_boundary_off);

// ---- T-66: catch-up ----

/// T-66: both listed revs rise after catch-up's listings and before its
/// reconciles; a CID-skipped row nothing changed is untouched; a catch-up
/// that stops at the page cap raises nothing; one that ends
/// `reconcile_incomplete` leaves them raised.
async fn t66_catch_up(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let kept = h.audience_of(ALICE, "3kt66aud", "list", &[BOB]).await;
    let gone = h.audience_of(ALICE, "3kt66gon", "list", &[BOB]).await;
    h.pds.remove(ALICE, NSID_AUDIENCE, "3kt66gon");
    h.f.ready_all().await;
    h.pds.set_rev(ALICE, R1);
    let run = |cfg: CatchUpConfig| {
        let (pds, writer) = (Arc::clone(&h.pds), h.f.writer.clone());
        async move { catch_up(pds.as_ref(), &writer, &CatchUp::new(cfg), ALICE, PDS, R1).await }
    };
    assert!(!run(CatchUpConfig { max_pages: 1, ..roomy() }).await, "the page cap");
    assert_eq!(listed(&h, ALICE).await, (None, None));
    assert!(!run(CatchUpConfig { reconcile_confirm_limit: 0, ..roomy() }).await, "reconcile_incomplete");
    assert_eq!(listed(&h, ALICE).await, both(R1));
    assert!(row(&h, "private_audiences", &gone).await.is_some());

    h.pds.set_rev(ALICE, R2);
    let before = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let after = hooks::arm(hooks::LISTED_REV_AFTER_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R2);
    before.reached().await;
    assert_eq!(listed(&h, ALICE).await, both(R1), "listings done, nothing raised");
    before.release();
    after.reached().await;
    assert_eq!(listed(&h, ALICE).await, both(R2), "raised");
    assert!(row(&h, "private_audiences", &gone).await.is_some(), "not yet reconciled");
    after.release();
    assert!(catch.await.expect("task"));
    assert_eq!(row(&h, "private_audiences", &gone).await, None);
    assert_eq!(row(&h, "private_audiences", &kept).await, Some((REV.to_string(), Some("bafy3kt66aud".to_string()))), "skipped, untouched");
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
    owner(&h, "t67").await;
    let b = h.backfill();
    let record = audience_record("list", &[BOB]);
    h.pds.set_rev(ALICE, R2);
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready);
    h.pds.put(ALICE, NSID_AUDIENCE, "3kt67new", record.clone());
    h.pds.set_rev(ALICE, R3);
    let indexed = index_record(h.pds.as_ref(), &h.f.writer, ALICE, NSID_AUDIENCE, "3kt67new", 1, false).await.expect("index");
    assert_eq!(indexed.status, IndexStatus::Indexed);

    // A newer job at R4, then an older one at R3 whose listing saw a record
    // deleted before the newer listing.
    h.pds.set_rev(ALICE, R4);
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready);
    h.pds.put(ALICE, NSID_AUDIENCE, "3kt67old", record.clone());
    h.pds.set_rev(ALICE, R3);
    assert_eq!(h.run(&b, ALICE, Depth::Private).await, JobOutcome::Ready, "no write failed");
    assert_eq!(row(&h, "private_audiences", &uri(ALICE, NSID_AUDIENCE, "3kt67old")).await, None);
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(R4));
    assert_eq!(listed(&h, ALICE).await, both(R4));

    h.pds.put(ALICE, NSID_AUDIENCE, "3kt67req", record);
    let indexed = index_record(h.pds.as_ref(), &h.f.writer, ALICE, NSID_AUDIENCE, "3kt67req", 1, false).await.expect("index");
    assert_eq!(indexed.status, IndexStatus::Unchanged);
    assert_eq!(row(&h, "private_audiences", &uri(ALICE, NSID_AUDIENCE, "3kt67req")).await, None);
    h.finish().await;
}

in_both_modes!(t67_sources: t67_sources_on, t67_sources_off);

// ---- T-68: deny-only classes ----

/// T-68: below the listed revs, a private block, a public block, a
/// threadgate of a private post and a follow are applied, and so is a
/// delete.
async fn t68_deny_only_classes(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let base = owner(&h, "t68").await;
    let p = h.post(ALICE, "3kt68pst", &uri(ALICE, NSID_AUDIENCE, &base), None).await;
    h.pds.set_rev(ALICE, R3);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    assert_eq!(listed(&h, ALICE).await, both(R3));
    let at = "2026-10-01T00:00:00Z";
    for (collection, rkey, record) in [
        ("tools.kryphocron.graph.block", "3kt68pblk", json!({"$type": "tools.kryphocron.graph.block", "subject": CAROL, "createdAt": at})),
        ("app.bsky.graph.block", "3kt68blk", json!({"$type": "app.bsky.graph.block", "subject": DAVE, "createdAt": at})),
        (THREADGATE, "3kt68pst", json!({"$type": THREADGATE, "post": p, "allow": [], "createdAt": at})),
        (FOLLOW, "3kt68fol", follow_record(CAROL)),
    ] {
        let report = h.fire(ALICE, R1, vec![upsert(collection, rkey, &format!("bafy{rkey}"), record)]).await;
        assert_eq!(report.outcomes, [OpOutcome::Written], "{collection}");
    }
    // A row kept at an old rev (as catch-up keeps one by CID), deleted
    // below the listed revs.
    let kept = uri(ALICE, NSID_AUDIENCE, "3kt68old");
    exec(
        h.pool(),
        &format!(
            "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, display_filled) \
             VALUES ('{kept}', '{ALICE}', '3kt68old', 'bafyold', '{REV}', 'list', '{{}}', true)"
        ),
    )
    .await;
    assert_eq!(h.fire(ALICE, R1, vec![delete(NSID_AUDIENCE, "3kt68old")]).await.outcomes, [OpOutcome::Written]);
    assert_eq!(row(&h, "private_audiences", &kept).await, None);
    h.finish().await;
}

in_both_modes!(t68_deny_only_classes: t68_deny_only_classes_on, t68_deny_only_classes_off);

// ---- T-69: settle coverage ----

/// T-69: a job settle whose listed revs are below R (the raise skipped by a
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
    assert_eq!(listed(&h, ALICE).await, (None, None));
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
    assert_eq!(listed(&h, ALICE).await, (None, None));
    assert_eq!(h.applied(ALICE).await.as_deref(), Some(REV));

    assert!(!h.catch_up(ALICE, R1).await);
    assert_eq!(catch_up_outcome(&handle, "listed_rev_busy"), 1.0);
    assert_eq!(listed(&h, ALICE).await, (None, None));
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
/// across the `ALTER`. The pre-release count counts the same rows.
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

/// T-72: a replayed audience row of a DID with no `sync_state` row is
/// inserted; the DID becomes an owner; its `private` job raises the listed
/// revs, and the reconcile deletes the row before readiness is set. Passes
/// at HEAD too: non-regression.
async fn t72_untracked_to_tracked(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    actor(h.pool(), EVE, "eve.test", "eve.test").await;
    exec(h.pool(), &format!("INSERT INTO private_owner_eligible (did, confirmed_at) VALUES ('{EVE}', now())")).await;
    let replayed = h.direct(EVE, NSID_AUDIENCE, "3kt72aud", audience_record("list", &[BOB])).await;
    h.pds.remove(EVE, NSID_AUDIENCE, "3kt72aud");
    assert!(row(&h, "private_audiences", &replayed).await.is_some(), "inserted: no listed rev");
    let kept = h.direct(EVE, NSID_AUDIENCE, "3kt72kep", audience_record("list", &[BOB])).await;
    h.f.writer.scope().write_guard().await.private_owners.insert(EVE.into());
    h.pds.set_rev(EVE, R1);
    let hook = hooks::arm(hooks::SETTLE_BEFORE_GAP_LOCK, &h.key(EVE));
    h.clear_jobs().await;
    h.enqueue(EVE, Depth::Private).await;
    let run = h.spawn_run(&h.backfill(), EVE, Depth::Private);
    hook.reached().await;
    assert_eq!(listed(&h, EVE).await, both(R1));
    assert_eq!(row(&h, "private_audiences", &replayed).await, None, "reconciled");
    assert!(!h.owner_ready(EVE).await, "before readiness");
    hook.release();
    assert_eq!(run.await.expect("task").expect("db").expect("job").1, JobOutcome::Ready);
    assert!(h.owner_ready(EVE).await);
    assert!(row(&h, "private_audiences", &kept).await.is_some());
    h.finish().await;
}

in_both_modes!(t72_untracked_to_tracked: t72_untracked_to_tracked_on, t72_untracked_to_tracked_off);

// ---- T-74, T-75, T-77: a stale version after catch-up ----

const STALE_RKEY: &str = "3kt74aud";
const U_PRIME_CID: &str = "bafyt74uprime";
const U_CID: &str = "bafyt74u";

/// U' (Bob listed), A's version at R2.
fn u_prime() -> RawOp {
    upsert(NSID_AUDIENCE, STALE_RKEY, U_PRIME_CID, audience_record("list", &[BOB]))
}

/// U (Bob removed), A's version at R3.
fn u() -> RawOp {
    upsert(NSID_AUDIENCE, STALE_RKEY, U_CID, audience_record("list", &[]))
}

/// O's audience A with versions U' at R2 and U at R3, and P under it. A
/// job's live listing stored U's content at R_j = R1 < R2; O's head is now
/// R4. Returns `(A, P)`.
async fn stale_version(h: &H) -> (String, String) {
    owner(h, "t74").await;
    let a = uri(ALICE, NSID_AUDIENCE, STALE_RKEY);
    h.pds.put_cid(ALICE, NSID_AUDIENCE, STALE_RKEY, U_CID, audience_record("list", &[]));
    let p_rec = h.f.post_record(ALICE, "3kt74pst", &a, None).await;
    h.pds.put(ALICE, NSID_POST_PRIVATE, "3kt74pst", p_rec);
    h.pds.set_rev(ALICE, R1);
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Ready);
    assert_eq!(row(h, "private_audiences", &a).await, Some((R1.to_string(), Some(U_CID.to_string()))));
    h.pds.set_rev(ALICE, R4);
    (a, uri(ALICE, NSID_POST_PRIVATE, "3kt74pst"))
}

/// T-74: catch-up at R4 skips A by CID and settles; the replay of U' is
/// refused and V does not read P under A. With the refusal off, V reads:
/// the HEAD leak.
async fn t74_a_stale_version_after_catch_up(test: &str, mode: PublicMode, refusal: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let _off = refusal_off(&h, ALICE, refusal);
    let (a, p) = stale_version(&h).await;
    assert!(h.catch_up(ALICE, R4).await);
    assert_eq!(row(&h, "private_audiences", &a).await, Some((R1.to_string(), Some(U_CID.to_string()))), "skipped by CID");
    assert_eq!(listed(&h, ALICE).await, both(R4));
    let report = h.fire(ALICE, R2, vec![u_prime()]).await;
    control(&h, ALICE, BOB, R4).await;
    if refusal {
        assert_eq!(report.outcomes, [OpOutcome::Unchanged]);
        assert!(misses(&h, BOB, &p).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Written]);
        assert!(h.reads(BOB, &p).await, "the HEAD leak");
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
/// the raise and settle commit, and V reads P under U' with `applied_rev ≥
/// head`.
async fn t75_catch_ups_check(test: &str, mode: PublicMode, check: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let (_, p) = stale_version(&h).await;
    let _off = (!check).then(|| hooks::arm(hooks::LISTED_REV_CHECK_OFF, &h.key(ALICE)));
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R4);
    hook.reached().await;
    assert_eq!(h.fire(ALICE, R2, vec![u_prime()]).await.outcomes, [OpOutcome::Written], "above the listed rev R1");
    hook.release();
    let caught = catch.await.expect("task");
    drop(hook);
    if check {
        assert!(!caught);
        assert_eq!(catch_up_outcome(&handle, "replayed_below"), 1.0);
        assert_eq!(listed(&h, ALICE).await, both(R1));
        assert_eq!(h.applied(ALICE).await.as_deref(), Some(R2), "the firehose's, not moved by the catch-up");
        exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R4}', head_checked_at = now() WHERE did = '{ALICE}'")).await;
        assert!(h.owner_ready(ALICE).await && h.member_ready(BOB).await);
        assert!(oracle_admits(h.pool(), BOB, &p).await, "the row admits V");
        assert!(!h.served(BOB, &p).await, "the head check");
        exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R2}' WHERE did = '{ALICE}'")).await;
        assert!(h.served(BOB, &p).await, "the head check was the cause");
        exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R4}' WHERE did = '{ALICE}'")).await;
        assert_eq!(h.fire(ALICE, R3, vec![u()]).await.outcomes, [OpOutcome::Written], "U by last-writer-wins");
        assert!(!h.served(BOB, &p).await);
        assert!(!oracle_admits(h.pool(), BOB, &p).await);
    } else {
        assert!(caught);
        assert_eq!(listed(&h, ALICE).await, both(R4));
        control(&h, ALICE, BOB, R4).await;
        assert!(h.reads(BOB, &p).await, "the leak the check closes");
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
    let (_, p) = stale_version(&h).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let _fail = hooks::arm(hooks::CATCH_UP_FAILS_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R4);
    hook.reached().await;
    h.fire(ALICE, R2, vec![u_prime()]).await;
    hook.release();
    assert!(!catch.await.expect("task"));
    assert_eq!(listed(&h, ALICE).await, both(R1));
    assert_eq!(h.fire(ALICE, R3, vec![u()]).await.outcomes, [OpOutcome::Written]);
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

in_both_modes!(t77_error_before_the_raise: t77_error_before_the_raise_on, t77_error_before_the_raise_off);

/// T-77: the same with the handler's future dropped there.
async fn t77_dropped_before_the_raise(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let (_, p) = stale_version(&h).await;
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(ALICE));
    let catch = spawn_catch_up(&h, ALICE, R4);
    hook.reached().await;
    h.fire(ALICE, R2, vec![u_prime()]).await;
    catch.abort();
    assert!(catch.await.expect_err("dropped").is_cancelled());
    drop(hook);
    assert_eq!(listed(&h, ALICE).await, both(R1));
    assert_eq!(h.fire(ALICE, R3, vec![u()]).await.outcomes, [OpOutcome::Written]);
    assert!(!h.reads(BOB, &p).await);
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
    let (a, p) = stale_version(&h).await;
    let _fail = hooks::arm(hooks::CATCH_UP_FAILS_AFTER_RAISE, &h.key(ALICE));
    assert!(!h.catch_up(ALICE, R4).await);
    assert_eq!(listed(&h, ALICE).await, both(R4));
    assert_eq!(row(&h, "private_audiences", &a).await, Some((R1.to_string(), Some(U_CID.to_string()))), "the listed version");
    assert_eq!(h.fire(ALICE, R2, vec![u_prime()]).await.outcomes, [OpOutcome::Unchanged]);
    assert!(!h.reads(BOB, &p).await);
    h.finish().await;
}

in_both_modes!(t77_error_after_the_raise: t77_error_after_the_raise_on, t77_error_after_the_raise_off);

// ---- T-76: no raise over a failed write ----

/// T-76: A stored at X0 with V; O's next version X1 of A has a non-array
/// `members`; then an unrelated commit X2. A job at X2 fails on A: the
/// audience listed rev is not raised (the post one is), and the settle
/// takes the no-readiness branch, leaving O ready. The consumer replays X1,
/// then X2. As built (`default`): X1 fails on the firehose, O's readiness is
/// cleared, and V misses on readiness alone. With the raise forced: X1 is
/// refused as `Unchanged`, X2 brings `applied_rev` to the head, O stays
/// ready, and V reads P under X0: the leak this guards against.
async fn t76_no_raise_over_a_failed_write(test: &str, mode: PublicMode, default: bool) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    let a = h.audience_of(ALICE, "3kt76aud", "list", &[BOB]).await;
    let p = h.post(ALICE, "3kt76pst", &a, None).await;
    h.f.ready_all().await;
    let x1 = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": "not an array", "createdAt": "2026-09-26T00:00:00Z"});
    h.pds.put_cid(ALICE, NSID_AUDIENCE, "3kt76aud", "bafyt76x1", x1.clone());
    h.pds.set_rev(ALICE, R2);
    let _force = (!default).then(|| hooks::arm(hooks::LISTED_REV_FORCE_RAISE, &h.key(ALICE)));
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Private).await, JobOutcome::Done);
    assert!(h.owner_ready(ALICE).await);
    let expected = if default { (Some(R2.to_string()), None) } else { both(R2) };
    assert_eq!(listed(&h, ALICE).await, expected);
    exec(h.pool(), &format!("UPDATE sync_state SET head_rev = '{R2}', head_checked_at = now() WHERE did = '{ALICE}'")).await;

    let report = h.fire(ALICE, R1, vec![upsert(NSID_AUDIENCE, "3kt76aud", "bafyt76x1", x1)]).await;
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
        assert!(!h.served(BOB, &p).await, "readiness alone");
    } else {
        assert!(h.owner_ready(ALICE).await);
        assert!(h.reads(BOB, &p).await, "the leak");
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
    owner(&h, "t78").await;
    let b = h.backfill();
    let s = Scene::new(&h, "t78", "list", 0).await;
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
        assert_eq!(row(&h, "private_audiences", &s.a).await, None);
        assert!(misses(&h, BOB, &s.p).await);
    } else {
        assert_eq!(report.outcomes, [OpOutcome::Written]);
        assert!(row(&h, "private_audiences", &s.a).await.is_some(), "survives the settle");
        assert!(h.reads(BOB, &s.p).await);
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
    for rkey in ["3kt79aud1", "3kt79aud2", "3kt79aud3"] {
        h.audience_of(ALICE, rkey, "list", &[BOB]).await;
    }
    h.f.ready_all().await;
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
                h.pds.remove(ALICE, NSID_AUDIENCE, "3kt79aud1");
                h.pds.set_rev(ALICE, &above);
                assert_eq!(h.fire(ALICE, &above, vec![delete(NSID_AUDIENCE, "3kt79aud1")]).await.outcomes, [OpOutcome::Written]);
            }
            "edit" => {
                let edited = audience_record("list", &[BOB, CAROL]);
                h.pds.put_cid(ALICE, NSID_AUDIENCE, "3kt79aud2", "bafyt79edit", edited.clone());
                h.pds.set_rev(ALICE, &above);
                let report = h.fire(ALICE, &above, vec![upsert(NSID_AUDIENCE, "3kt79aud2", "bafyt79edit", edited)]).await;
                assert_eq!(report.outcomes, [OpOutcome::Written]);
            }
            _ => {
                h.pds.put_cid(ALICE, NSID_AUDIENCE, "3kt79aud3", "bafyt79req", audience_record("list", &[BOB, DAVE]));
                h.pds.set_rev(ALICE, &above);
                let indexed = index_record(h.pds.as_ref(), &h.f.writer, ALICE, NSID_AUDIENCE, "3kt79aud3", 1, false).await.expect("index");
                assert_eq!(indexed.status, IndexStatus::Indexed);
            }
        }
        hook.release();
        assert!(catch.await.expect("task"), "{case}");
        drop(hook);
        assert_eq!(listed(&h, ALICE).await, both(&r), "{case}");
        assert_eq!(series(&handle.render(), METRIC_LISTED_REV_LOCK_RETRIES, &[]).unwrap_or(0.0), 0.0, "{case}: the first try");
    }
    assert_eq!(catch_up_outcome(&handle, "settled"), 3.0);
    h.finish().await;
}

in_both_modes!(t79_newer_states_are_not_replays: t79_newer_states_are_not_replays_on, t79_newer_states_are_not_replays_off);

// ---- T-80: no resurrection ----

/// T-80 (review 4's ordering): O a non-member owner outside S; catch-up C
/// skips A; O deletes A at D' > R; an older job reconciles A away with a
/// tombstone at R_j < R; the firehose applies D' with no row and no
/// tombstone. C's check finds nothing, C raises and settles, and A stays
/// absent: V does not read.
async fn t80_no_resurrection(test: &str, mode: PublicMode) {
    let Some(h) = H::new(test, mode).await else {
        return;
    };
    actor(h.pool(), EVE, "eve.test", "eve.test").await;
    exec(h.pool(), &format!("INSERT INTO private_owner_eligible (did, confirmed_at) VALUES ('{EVE}', now())")).await;
    exec(
        h.pool(),
        &format!("INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at, firehose_rev) VALUES ('{EVE}', '{REV}', '{REV}', now(), '{REV}')"),
    )
    .await;
    h.f.writer.scope().write_guard().await.private_owners.insert(EVE.into());
    let rkey = "3kt80aud";
    let a = h.direct(EVE, NSID_AUDIENCE, rkey, audience_record("list", &[BOB])).await;
    let p_rec = h.f.post_record(EVE, "3kt80pst", &a, None).await;
    let p = h.direct(EVE, NSID_POST_PRIVATE, "3kt80pst", p_rec).await;
    h.f.ready(EVE).await;
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    h.pds.set_rev(EVE, R2);
    let hook = hooks::arm(hooks::LISTED_REV_BEFORE_RAISE, &h.key(EVE));
    let catch = spawn_catch_up(&h, EVE, R2);
    hook.reached().await;
    h.pds.remove(EVE, NSID_AUDIENCE, rkey);
    h.pds.set_rev(EVE, R3);
    let report = h.f.writer.apply(EVE, R1, &[delete(NSID_AUDIENCE, rkey)], job_write()).await.expect("apply");
    assert_eq!(report.outcomes, [OpOutcome::Written], "the older job's reconcile");
    h.fire(EVE, R3, vec![delete(NSID_AUDIENCE, rkey)]).await;
    let tombstone: Option<String> = sqlx::query_scalar("SELECT rev FROM private_tombstones WHERE uri = $1").bind(&a).fetch_optional(h.pool()).await.expect("tombstone");
    assert_eq!(tombstone.as_deref(), Some(R1), "no tombstone at D'");
    hook.release();
    catch.await.expect("task");
    drop(hook);
    assert_eq!(catch_up_outcome(&handle, "settled"), 1.0);
    assert_eq!(listed(&h, EVE).await, both(R2));
    assert_eq!(row(&h, "private_audiences", &a).await, None, "not resurrected");
    assert!(!h.served(BOB, &p).await);
    h.finish().await;
}

in_both_modes!(t80_no_resurrection: t80_no_resurrection_on, t80_no_resurrection_off);
