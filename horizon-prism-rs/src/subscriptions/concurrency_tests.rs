//! Rule 4 under forced interleavings (Phase B B-65 and B-74, in-crate).
//!
//! The pause points and injected errors of `test_support::hooks` stop a
//! `list_sync` batch, a run, a member phase or a release at a named step,
//! so that another task can act in between. Each test keys its hooks to
//! DIDs no other test uses, since the suite runs in parallel.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::json;
use sqlx::PgPool;

use super::applier::Applier;
use super::fetch::{Bounds, NSID_LIST, NSID_LISTITEM, OwnerRecords, Page};
use super::jobs;
use super::list_phase::{self, Applied, ListState};
use super::references::{Budget, Muted, mute_list, unmute_list};
use super::run::{Outcome, OwnerSource, OwnerStatus, Settings};
use super::runner::{Lanes, ListSync};
use super::{retryable, retrying};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;
use crate::firehose::backfill::ListedRecord;
use crate::test_support::{fresh_db, holds_advisory, hooks, idle_in_transaction_pid, locked_relations, waits_on_lock};

const BUDGET: Budget = Budget { max_references: 5, max_owners: 5 };
/// A request on a held list must answer within this (B-74).
const REQUEST_BOUND: Duration = Duration::from_millis(200);
/// No request comes near this: a request that waited on the held row would
/// wait until the test let it go, seconds later. The machine's own load
/// (the whole suite runs in parallel) can push a single answer past
/// [`REQUEST_BOUND`], so that bound is held by the median.
const REQUEST_CEILING: Duration = Duration::from_secs(1);

/// The B-74 bound over a run of requests: each under [`REQUEST_CEILING`],
/// the median under [`REQUEST_BOUND`].
fn within_request_bound(took: &mut [Duration]) {
    took.sort_unstable();
    let (median, slowest) = (took[took.len() / 2], took[took.len() - 1]);
    eprintln!("b74: {} requests on a held list: median {median:?}, slowest {slowest:?}", took.len());
    assert!(slowest < REQUEST_CEILING, "a request waited: {slowest:?}");
    assert!(median < REQUEST_BOUND, "median {median:?}");
}

fn list_of(owner: &str) -> String {
    format!("at://{owner}/{NSID_LIST}/3klist1")
}

fn scope() -> ScopeState {
    ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000)
}

/// Every owner is active, with one moderation list (`3klist1`) of `items`
/// items. Each status answer is a newer revision, so each run's listing is
/// newer than the last.
struct Owners {
    items: usize,
    revs: AtomicU64,
}

struct Records {
    owner: String,
    items: usize,
}

#[async_trait]
impl OwnerRecords for Records {
    async fn page(&self, collection: &str, cursor: Option<&str>) -> Result<Page, String> {
        let list = list_of(&self.owner);
        let from: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
        let total = if collection == NSID_LIST { 1 } else { self.items };
        let to = (from + 100).min(total);
        let records = (from..to)
            .map(|n| {
                if collection == NSID_LIST {
                    ListedRecord { uri: list.clone(), cid: "bafylist".into(), value: json!({"purpose": "app.bsky.graph.defs#modlist"}) }
                } else {
                    ListedRecord {
                        uri: format!("at://{}/{NSID_LISTITEM}/3k{n:06}", self.owner),
                        cid: "bafyitem".into(),
                        value: json!({"list": list, "subject": format!("did:plc:subject{n}")}),
                    }
                }
            })
            .collect();
        Ok(Page { records, cursor: (to < total).then(|| to.to_string()), call_time: Duration::from_millis(1) })
    }
}

#[async_trait]
impl OwnerSource for Owners {
    async fn status(&self, _owner: &str) -> Result<OwnerStatus, String> {
        let n = self.revs.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(OwnerStatus::Active { rev: format!("3k{n:09}") })
    }
    async fn records(&self, owner: &str, _generation: i64) -> Result<Box<dyn OwnerRecords>, String> {
        Ok(Box::new(Records { owner: owner.to_string(), items: self.items }))
    }
}

/// The lanes: `fast` permits, one slow one, and the stuck-row bound.
fn lanes(fast: u32, stuck_secs: u32) -> Lanes {
    Lanes { fast, slow: 1, stuck_secs, max_wall_secs: 3600 }
}

/// A `ListSync` over [`Owners`], with no pause between runs of an owner.
fn list_sync(pool: &PgPool, applier: &Applier, items: usize, lanes: Lanes) -> ListSync {
    let bounds = Bounds {
        max_records: 1_000_000,
        max_pages: 10_000,
        probe_call: Duration::from_secs(60),
        min_records_per_call_sec: 1,
        max_call: Duration::from_secs(900),
        max_wall: Duration::from_secs(3600),
    };
    let settings = Settings { max_items: 100_000, tombstones_max_per_owner: 10_000, min_interval_secs: 0, bounds };
    let source = Arc::new(Owners { items, revs: AtomicU64::new(0) });
    ListSync::new(pool.clone(), scope(), source, applier, settings, lanes)
}

/// A member's mute of `list`, then the list phase.
async fn subscribe(pool: &PgPool, member: &str, list: &str) {
    assert_eq!(mute_list(pool, member, list, BUDGET).await.expect("mute"), Muted::Added);
    let applied = list_phase::apply(pool, list, 0).await.expect("list phase");
    assert!(matches!(applied, Applied::Done { state: Some(ListState::Active), .. }), "{applied:?}");
}

/// An owner's `list_sync` row: `(state, priority, rerun_requested,
/// attempts, last_error, generation)`.
type JobRow = (String, i16, bool, i32, Option<String>, i64);

async fn job(pool: &PgPool, owner: &str) -> JobRow {
    sqlx::query_as("SELECT state, priority, rerun_requested, attempts, last_error, generation FROM backfill_jobs WHERE did = $1 AND depth = 'list_sync'")
        .bind(owner)
        .fetch_one(pool)
        .await
        .expect("job")
}

async fn effective(pool: &PgPool, member: &str, list: &str) -> bool {
    sqlx::query_scalar("SELECT effective FROM list_references WHERE member_did = $1 AND list_uri = $2")
        .bind(member)
        .bind(list)
        .fetch_one(pool)
        .await
        .expect("reference")
}

async fn count(pool: &PgPool, sql: &str, list: &str) -> i64 {
    sqlx::query_scalar(sql).bind(list).fetch_one(pool).await.expect(sql)
}

async fn markers(pool: &PgPool, list: &str) -> i64 {
    count(pool, "SELECT count(*) FROM list_reference_changes WHERE list_uri = $1", list).await
}

/// The list's row state, `None` once it is gone.
async fn list_state(pool: &PgPool, list: &str) -> Option<String> {
    sqlx::query_scalar("SELECT state FROM subscribed_lists WHERE list_uri = $1")
        .bind(list)
        .fetch_optional(pool)
        .await
        .expect("row")
}

/// Poll `check` every 20 ms until it holds; how long that took. Fails
/// after `within`.
async fn until<F, Fut>(within: Duration, what: &str, mut check: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while !check().await {
        assert!(started.elapsed() < within, "{what}: not within {within:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    started.elapsed()
}

/// A synced copy of `list` of `n` items at revision `3k` (older than any
/// listing), its references effective. The rows are written with the
/// per-row count trigger off, as a seed (the count is set after); every
/// later write runs it.
async fn synced_copy(pool: &PgPool, owner: &str, list: &str, n: i64) {
    let mut tx = pool.begin().await.expect("tx");
    sqlx::query("SET LOCAL session_replication_role = replica").execute(&mut *tx).await.expect("seed mode");
    sqlx::query(
        "INSERT INTO subscribed_list_items (uri, list_uri, owner_did, rkey, cid, rev, subject_did) \
         SELECT 'at://' || $1 || '/app.bsky.graph.listitem/3kcopy' || i, $2, $1, '3kcopy' || i, 'bafy', '3k', 'did:plc:copied' || i \
         FROM generate_series(1, $3) AS i",
    )
    .bind(owner)
    .bind(list)
    .bind(n)
    .execute(&mut *tx)
    .await
    .expect("items");
    tx.commit().await.expect("seeded");
    sqlx::query("UPDATE subscribed_lists SET synced_at = now(), item_count = $2, purpose = 'app.bsky.graph.defs#modlist' WHERE list_uri = $1")
        .bind(list)
        .bind(i32::try_from(n).expect("count"))
        .execute(pool)
        .await
        .expect("synced");
    sqlx::query("UPDATE list_references SET effective = true WHERE list_uri = $1").bind(list).execute(pool).await.expect("effective");
}

/// B-65 forced interleavings (R3-138, R3-144; proposed locked decision
/// 36): with a `list_sync` batch paused while it holds its list's row,
/// another member's `muteActorList` of that list returns without waiting,
/// and the list phase, which never waits on the row, completes after the
/// batch. No job fails, no lost deadlock reaches the caller, and every
/// active list's owner has its row.
#[tokio::test]
async fn b65_a_mute_never_waits_on_a_batch_holding_its_list() {
    const OWNER: &str = "did:plc:b65heldbatchowner";
    const M1: &str = "did:plc:b65heldbatchmember1";
    const M2: &str = "did:plc:b65heldbatchmember2";
    let Some(db) = fresh_db("b65_held_batch").await else {
        return;
    };
    let p = &db.pool;
    let list = list_of(OWNER);
    subscribe(p, M1, &list).await;
    let applier = Applier::new(p.clone(), scope(), 0);
    let sync = list_sync(p, &applier, 3, lanes(1, 5400));

    let held = hooks::arm(hooks::BATCH_HOLDS_LIST_ROW, &list);
    let run = tokio::spawn({
        let sync = sync.clone();
        async move { sync.run_once().await }
    });
    held.reached().await;
    let row_held: bool = sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM subscribed_lists WHERE list_uri = $1 FOR UPDATE SKIP LOCKED)")
        .bind(&list)
        .fetch_one(p)
        .await
        .expect("probe");
    assert!(row_held, "the paused batch holds the list's row");

    // The member phase touches nothing the batch holds.
    let began = Instant::now();
    let muted = tokio::time::timeout(Duration::from_secs(5), retrying(retryable, || mute_list(p, M2, &list, BUDGET)))
        .await
        .expect("the mute does not wait on the batch")
        .expect("no error reaches the client");
    assert_eq!(muted, Muted::Added);
    // The batch holds the row until it is released below.
    assert!(began.elapsed() < REQUEST_CEILING, "{:?}", began.elapsed());
    // The list phase skips the held row and keeps its marker.
    let applied = tokio::time::timeout(Duration::from_secs(5), list_phase::apply(p, &list, 0)).await.expect("the list phase does not wait");
    assert_eq!(applied.expect("list phase"), Applied::Busy);
    assert_eq!(markers(p, &list).await, 1);

    held.release();
    let (owner, outcome) = run.await.expect("task").expect("claim").expect("a run");
    assert_eq!((owner.as_str(), outcome), (OWNER, Outcome::Completed));
    // After the batch the list phase completes, and M2 waits on a sync.
    let applied = list_phase::apply(p, &list, 0).await.expect("list phase");
    assert_eq!(applied, Applied::Done { state: Some(ListState::Active), waiting: vec![(M2.to_string(), "mute".to_string())] });
    assert_eq!(markers(p, &list).await, 0);
    assert!(effective(p, M1, &list).await && !effective(p, M2, &list).await);
    let (state, _, _, attempts, error, _) = job(p, OWNER).await;
    assert!(state == "queued" && attempts == 0 && error.is_none(), "{state} {attempts} {error:?}");
    let ownerless: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM subscribed_lists l WHERE l.state = 'active' AND NOT EXISTS (SELECT 1 FROM subscribed_owners o WHERE o.owner_did = l.owner_did)",
    )
    .fetch_one(p)
    .await
    .expect("owners");
    assert_eq!(ownerless, 0);
    drop(held);
    db.drop_db().await;
}

/// B-65 "no `40P01` surfaced" (design-v3 §4.3 "Lock order"): a rule-4
/// transaction that Postgres picks as a deadlock's victim is run again in
/// place, and its caller sees only the success.
#[tokio::test]
async fn b65_a_lost_deadlock_is_retried_in_place() {
    const X: &str = "did:plc:b65deadlockx";
    const Y: &str = "did:plc:b65deadlocky";
    let Some(db) = fresh_db("b65_deadlock").await else {
        return;
    };
    let p = &db.pool;
    sqlx::query("INSERT INTO subscribed_owners (owner_did) VALUES ($1), ($2)").bind(X).bind(Y).execute(p).await.expect("owners");
    let touch = "UPDATE subscribed_owners SET resync_requested_at = clock_timestamp() WHERE owner_did = $1";
    let (x_locked, y_locked) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
    // The other transaction takes Y, then X, and commits.
    let other = tokio::spawn({
        let (p, x_locked, y_locked) = (p.clone(), Arc::clone(&x_locked), Arc::clone(&y_locked));
        async move {
            let mut tx = p.begin().await.expect("tx");
            x_locked.notified().await;
            sqlx::query(touch).bind(Y).execute(&mut *tx).await.expect("Y");
            y_locked.notify_one();
            // Wait second, well inside the 1 s `deadlock_timeout`, so the
            // retried transaction is the one whose deadlock check fires.
            tokio::time::sleep(Duration::from_millis(600)).await;
            sqlx::query(touch).bind(X).execute(&mut *tx).await.expect("the other transaction is not the victim");
            tx.commit().await.expect("commit");
        }
    });
    let attempts = AtomicU64::new(0);
    let result: Result<(), sqlx::Error> = retrying(retryable, || async {
        let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
        let mut tx = p.begin().await?;
        sqlx::query(touch).bind(X).execute(&mut *tx).await?;
        if first {
            x_locked.notify_one();
            y_locked.notified().await;
        }
        sqlx::query(touch).bind(Y).execute(&mut *tx).await?;
        tx.commit().await
    })
    .await;
    result.expect("the lost deadlock never reaches the caller");
    assert_eq!(attempts.load(Ordering::SeqCst), 2, "lost once, then won");
    other.await.expect("the other transaction");
    db.drop_db().await;
}

/// B-65 `list_sync` rerun (R3-137): a reference admitted while a run is
/// under way, to a list already in its `S0`, is not made effective by that
/// run; the run asked for by its enqueue (`rerun_requested`) makes it so.
#[tokio::test]
async fn b65_a_reference_admitted_mid_run_waits_for_the_rerun() {
    const OWNER: &str = "did:plc:b65rerunowner";
    const M1: &str = "did:plc:b65rerunmember1";
    const M2: &str = "did:plc:b65rerunmember2";
    let Some(db) = fresh_db("b65_rerun").await else {
        return;
    };
    let p = &db.pool;
    let list = list_of(OWNER);
    subscribe(p, M1, &list).await;
    let applier = Applier::new(p.clone(), scope(), 0);
    let sync = list_sync(p, &applier, 3, lanes(1, 5400));

    let paused = hooks::arm(hooks::RUN_AFTER_SNAPSHOT, OWNER);
    let run = tokio::spawn({
        let sync = sync.clone();
        async move { sync.run_once().await }
    });
    paused.reached().await;
    // M2's mute lands after the snapshot: the running row is left running,
    // and a rerun is asked for.
    subscribe(p, M2, &list).await;
    let (state, _, rerun, ..) = job(p, OWNER).await;
    assert_eq!((state.as_str(), rerun), ("running", true));
    paused.release();
    let (_, outcome) = run.await.expect("task").expect("claim").expect("a run");
    assert_eq!(outcome, Outcome::Completed);
    assert!(effective(p, M1, &list).await, "M1 waited when the run began");
    assert!(!effective(p, M2, &list).await, "M2 was admitted after the run's snapshot");
    let (state, _, rerun, ..) = job(p, OWNER).await;
    assert_eq!((state.as_str(), rerun), ("queued", false));

    // The rerun.
    let (owner, outcome) = sync.run_once().await.expect("claim").expect("the rerun");
    assert_eq!((owner.as_str(), outcome), (OWNER, Outcome::Completed));
    assert!(effective(p, M2, &list).await);
    assert_eq!(job(p, OWNER).await.0, "done");
    drop(paused);
    db.drop_db().await;
}

/// B-65 concurrency cap (R3-137, R3-169): workers sharing the lanes never
/// run more `list_sync` jobs at once than `lists.sync_max_concurrent`
/// (the fast lane's permits, taken with `try_acquire_owned`). With two
/// permits and three owners due, two runs are held mid-way; a third worker
/// claims nothing and leaves the third row queued.
#[tokio::test]
async fn b65_workers_never_run_more_list_syncs_than_the_lane_allows() {
    const OWNERS: [&str; 3] = ["did:plc:b65capownera", "did:plc:b65capownerb", "did:plc:b65capownerc"];
    const M: &str = "did:plc:b65capmember";
    let Some(db) = fresh_db("b65_cap").await else {
        return;
    };
    let p = &db.pool;
    for owner in OWNERS {
        subscribe(p, M, &list_of(owner)).await;
    }
    let applier = Applier::new(p.clone(), scope(), 0);
    let sync = list_sync(p, &applier, 2, lanes(2, 5400));
    let paused: Vec<hooks::Armed> = OWNERS.iter().map(|owner| hooks::arm(hooks::RUN_AFTER_SNAPSHOT, owner)).collect();
    let worker = || {
        let sync = sync.clone();
        tokio::spawn(async move { sync.run_once().await })
    };
    let (first, second) = (worker(), worker());
    until(Duration::from_secs(10), "two runs held", || async { paused.iter().map(hooks::Armed::hits).sum::<usize>() == 2 }).await;

    // A third worker: no permit, so no claim at all.
    assert_eq!(sync.run_once().await.expect("claim"), None);
    let running: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs WHERE depth = 'list_sync' AND state = 'running'")
        .fetch_one(p)
        .await
        .expect("running");
    assert_eq!(running, 2);
    let idle: Vec<&str> = OWNERS.iter().zip(&paused).filter(|(_, hook)| hook.hits() == 0).map(|(owner, _)| *owner).collect();
    assert_eq!(idle.len(), 1);
    assert_eq!(job(p, idle[0]).await.0, "queued");

    for hook in &paused {
        hook.release();
    }
    for task in [first, second] {
        assert_eq!(task.await.expect("task").expect("claim").expect("a run").1, Outcome::Completed);
    }
    // The permits are back: the third owner runs.
    assert_eq!(sync.run_once().await.expect("claim").expect("a run"), (idle[0].to_string(), Outcome::Completed));
    drop(paused);
    db.drop_db().await;
}

/// B-65 priority raise (R3-145): a mute made while a weekly run of its
/// owner is in progress raises the running row's priority and asks for a
/// rerun; that rerun keeps the raised priority and is claimed ahead of the
/// weekly rows already queued, whose due times are earlier.
#[tokio::test]
async fn b65_a_mute_during_a_weekly_run_is_synced_ahead_of_the_weekly_rows() {
    const O: &str = "did:plc:b65weeklyowner";
    const P: &str = "did:plc:b65weeklyp";
    const Q: &str = "did:plc:b65weeklyq";
    const M1: &str = "did:plc:b65weeklymember1";
    const M2: &str = "did:plc:b65weeklymember2";
    let Some(db) = fresh_db("b65_weekly_priority").await else {
        return;
    };
    let p = &db.pool;
    let applier = Applier::new(p.clone(), scope(), 0);
    let sync = list_sync(p, &applier, 2, lanes(1, 5400));
    for owner in [O, P, Q] {
        subscribe(p, M1, &list_of(owner)).await;
        assert_eq!(sync.run_once().await.expect("claim").expect("a run").1, Outcome::Completed);
    }
    // A week later: the weekly run queues all three at low priority, O
    // first.
    sqlx::query("UPDATE subscribed_owners SET last_completed_at = now() - interval '8 days'").execute(p).await.expect("a week");
    assert_eq!(super::triggers::weekly(p, 0, Duration::ZERO).await.expect("weekly"), Some(3));
    sqlx::query("UPDATE backfill_jobs SET not_before = now() - interval '1 hour' WHERE did = $1").bind(O).execute(p).await.expect("O first");
    sqlx::query("UPDATE backfill_jobs SET not_before = now() - interval '30 minutes' WHERE did <> $1").bind(O).execute(p).await.expect("then P, Q");

    let paused = hooks::arm(hooks::RUN_AFTER_SNAPSHOT, O);
    let run = tokio::spawn({
        let sync = sync.clone();
        async move { sync.run_once().await }
    });
    paused.reached().await;
    assert_eq!(job(p, O).await.1, jobs::LOW);
    // M2 mutes O's list during the run; the applier runs its list phase.
    assert_eq!(mute_list(p, M2, &list_of(O), BUDGET).await.expect("mute"), Muted::Added);
    assert!(applier.run_once().await.expect("applier").is_empty());
    let (state, priority, rerun, ..) = job(p, O).await;
    assert_eq!((state.as_str(), priority, rerun), ("running", jobs::HIGH, true));
    paused.release();
    assert_eq!(run.await.expect("task").expect("claim").expect("a run").1, Outcome::Completed);
    let (state, priority, rerun, ..) = job(p, O).await;
    assert_eq!((state.as_str(), priority, rerun), ("queued", jobs::HIGH, false));

    // The rerun is claimed before P's and Q's weekly rows.
    assert_eq!(sync.run_once().await.expect("claim").expect("a run"), (O.to_string(), Outcome::Completed));
    assert!(effective(p, M2, &list_of(O)).await);
    for owner in [P, Q] {
        assert_eq!(job(p, owner).await.0, "queued");
    }
    drop(paused);
    db.drop_db().await;
}

/// B-65 generation bump on release (R3-151, §14.1): when an owner's last
/// referenced list is released, its queued `list_sync` is cancelled and
/// its generation moves on.
#[tokio::test]
async fn b65_releasing_an_owners_last_list_cancels_its_sync_with_a_new_generation() {
    const OWNER: &str = "did:plc:b65releaseowner";
    const M: &str = "did:plc:b65releasemember";
    let Some(db) = fresh_db("b65_release_generation").await else {
        return;
    };
    let p = &db.pool;
    let list = list_of(OWNER);
    subscribe(p, M, &list).await;
    let (state, _, _, _, _, before) = job(p, OWNER).await;
    assert_eq!(state, "queued");
    assert!(unmute_list(p, M, &list, BUDGET).await.expect("unmute"));
    assert!(matches!(list_phase::apply(p, &list, 0).await.expect("list phase"), Applied::Done { state: Some(ListState::Unreferenced), .. }));
    assert_eq!(list_phase::release(p, &list).await.expect("release"), list_phase::Released::Gone { items: 0 });
    let (state, _, _, _, _, after) = job(p, OWNER).await;
    assert_eq!((state.as_str(), after), ("cancelled", before + 1));
    db.drop_db().await;
}

/// An owner whose only list was subscribed and then released: its owner
/// row is left with no list and no backoff, and its `list_sync` cancelled.
async fn listless_owner(pool: &PgPool, member: &str, owner: &str) {
    let list = list_of(owner);
    subscribe(pool, member, &list).await;
    assert!(unmute_list(pool, member, &list, BUDGET).await.expect("unmute"));
    assert!(matches!(list_phase::apply(pool, &list, 0).await.expect("list phase"), Applied::Done { state: Some(ListState::Unreferenced), .. }));
    assert_eq!(list_phase::release(pool, &list).await.expect("release"), list_phase::Released::Gone { items: 0 });
    assert_eq!(job(pool, owner).await.0, "cancelled");
}

/// The owner's new first list, a second list of `owner`.
fn second_list_of(owner: &str) -> String {
    format!("at://{owner}/{NSID_LIST}/3klist2")
}

async fn owner_row(pool: &PgPool, owner: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscribed_owners WHERE owner_did = $1)")
        .bind(owner)
        .fetch_one(pool)
        .await
        .expect("owner")
}

/// B-65 owner-row race (R3-159), the sweep first: with the daily sweep's
/// owner-row delete paused after its `FOR UPDATE`, a list phase adds the
/// owner's new first list and waits on the owner's row. The sweep's delete
/// does not see that uncommitted list and removes the row; the list
/// phase's upsert then re-creates it. The owner row is present and the new
/// list's `list_sync` queued.
#[tokio::test]
async fn b65_an_owner_row_deleted_under_a_waiting_list_phase_is_recreated() {
    const OWNER: &str = "did:plc:b65sweepfirstowner";
    const M: &str = "did:plc:b65sweepfirstmember";
    let Some(db) = fresh_db("b65_sweep_first").await else {
        return;
    };
    let p = &db.pool;
    listless_owner(p, M, OWNER).await;
    let new = second_list_of(OWNER);
    assert_eq!(mute_list(p, M, &new, BUDGET).await.expect("mute"), Muted::Added);

    let paused = hooks::arm(hooks::SWEEP_OWNER_LOCKED, OWNER);
    let sweep = tokio::spawn({
        let p = p.clone();
        async move { super::sweep::orphan_owners(&p, &super::applier::Waiting::default(), 0).await }
    });
    paused.reached().await;
    let phase = tokio::spawn({
        let (p, new) = (p.clone(), new.clone());
        async move { list_phase::apply(&p, &new, 0).await }
    });
    until(Duration::from_secs(10), "the list phase waits on the owner's row", || async {
        waits_on_lock(p, "INSERT INTO subscribed_owners").await
    })
    .await;
    paused.release();
    assert_eq!(sweep.await.expect("task").expect("sweep"), 1, "the sweep deleted the row it locked");
    let applied = phase.await.expect("task").expect("list phase");
    assert!(matches!(applied, Applied::Done { state: Some(ListState::Active), .. }), "{applied:?}");
    assert!(owner_row(p, OWNER).await, "the list phase re-created the owner row");
    assert_eq!(job(p, OWNER).await.0, "queued");
    drop(paused);
    db.drop_db().await;
}

/// B-65 owner-row race (R3-159), the list phase first: a list phase adding
/// the owner's new first list holds the owner's row, paused before its
/// commit. The sweep's `FOR UPDATE` waits on it; its delete, a new
/// statement, then sees the committed list and deletes nothing. The owner
/// row is present and the new list's `list_sync` queued.
#[tokio::test]
async fn b65_a_sweep_waiting_on_a_list_phase_deletes_nothing() {
    const OWNER: &str = "did:plc:b65phasefirstowner";
    const M: &str = "did:plc:b65phasefirstmember";
    let Some(db) = fresh_db("b65_phase_first").await else {
        return;
    };
    let p = &db.pool;
    listless_owner(p, M, OWNER).await;
    let new = second_list_of(OWNER);
    assert_eq!(mute_list(p, M, &new, BUDGET).await.expect("mute"), Muted::Added);

    let paused = hooks::arm(hooks::LIST_PHASE_BEFORE_COMMIT, &new);
    let phase = tokio::spawn({
        let (p, new) = (p.clone(), new.clone());
        async move { list_phase::apply(&p, &new, 0).await }
    });
    paused.reached().await;
    let sweep = tokio::spawn({
        let p = p.clone();
        async move { super::sweep::orphan_owners(&p, &super::applier::Waiting::default(), 0).await }
    });
    until(Duration::from_secs(10), "the sweep waits on the owner's row", || async {
        waits_on_lock(p, "SELECT 1 FROM subscribed_owners").await
    })
    .await;
    paused.release();
    let applied = phase.await.expect("task").expect("list phase");
    assert!(matches!(applied, Applied::Done { state: Some(ListState::Active), .. }), "{applied:?}");
    assert_eq!(sweep.await.expect("task").expect("sweep"), 0, "the sweep saw the committed list");
    assert!(owner_row(p, OWNER).await);
    assert_eq!(job(p, OWNER).await.0, "queued");
    drop(paused);
    db.drop_db().await;
}

/// N-1501: the release of an owner's last list drops the owner's waiting
/// pairs added before its cancel; while another list of the owner is
/// left, they stay. Other owners' pairs are untouched.
#[tokio::test]
async fn b65_releasing_an_owners_last_list_drops_its_waiting_pairs() {
    const OWNER: &str = "did:plc:b65pairsowner";
    const OTHER: &str = "did:plc:b65pairsother";
    const M: &str = "did:plc:b65pairsmember";
    let Some(db) = fresh_db("b65_release_pairs").await else {
        return;
    };
    let p = &db.pool;
    let waiting = super::applier::Waiting::default();
    let (first, second) = (list_of(OWNER), second_list_of(OWNER));
    for list in [&first, &second] {
        subscribe(p, M, list).await;
    }
    waiting.add(OWNER, M, "mute");
    waiting.add(OTHER, M, "mute");
    let drop_list = |list: String| {
        let waiting = waiting.clone();
        async move {
            assert!(unmute_list(p, M, &list, BUDGET).await.expect("unmute"));
            list_phase::apply(p, &list, 0).await.expect("list phase");
            list_phase::release_with(p, &list, &waiting).await.expect("release")
        }
    };
    // Not the owner's last list: the pairs stay, and so does the job.
    assert_eq!(drop_list(first).await, list_phase::Released::Gone { items: 0 });
    assert_eq!(waiting.of(OWNER), [(M.to_string(), "mute".to_string())]);
    assert_eq!(job(p, OWNER).await.0, "queued");
    // The last one: cancelled, and the owner's pairs are gone.
    assert_eq!(drop_list(second).await, list_phase::Released::Gone { items: 0 });
    assert_eq!(job(p, OWNER).await.0, "cancelled");
    assert!(waiting.of(OWNER).is_empty());
    assert_eq!(waiting.of(OTHER), [(M.to_string(), "mute".to_string())]);
    db.drop_db().await;
}

/// B-65 stuck row (R3-169): a `settle` that fails leaves the row
/// `running` with no run behind it. A second owner's `list_sync` is still
/// claimed and runs, and the stuck-row check queues the stuck row again,
/// at a new generation, no sooner than `lists.sync_stuck_secs` and within
/// 1.25 × that after the run's claim. The check runs every quarter of the
/// bound; 0.25 s is allowed for its own statements.
#[tokio::test]
async fn b65_a_row_left_running_by_a_failed_settle_comes_back_within_the_bound() {
    const STUCK: &str = "did:plc:b65stuckowner";
    const OTHER: &str = "did:plc:b65stuckother";
    const M: &str = "did:plc:b65stuckmember";
    const STUCK_SECS: u32 = 4;
    let Some(db) = fresh_db("b65_stuck").await else {
        return;
    };
    let p = &db.pool;
    for owner in [STUCK, OTHER] {
        subscribe(p, M, &list_of(owner)).await;
    }
    sqlx::query("UPDATE backfill_jobs SET not_before = now() - interval '1 minute' WHERE did = $1").bind(STUCK).execute(p).await.expect("first");
    let applier = Applier::new(p.clone(), scope(), 0);
    let sync = list_sync(p, &applier, 2, lanes(1, STUCK_SECS));

    let failing = hooks::arm(hooks::SETTLE_FAILS, STUCK);
    let (owner, outcome) = sync.run_once().await.expect("claim").expect("a run");
    assert_eq!(owner, STUCK);
    assert!(matches!(outcome, Outcome::Failed(ref e) if e.contains("injected")), "{outcome:?}");
    assert!(failing.hits() >= 2, "the completion's settle and the failure's settle both failed");
    let (state, _, _, _, _, generation) = job(p, STUCK).await;
    assert_eq!(state, "running");
    // When the row was claimed, on the database's clock: nothing has
    // written it since.
    let at = "SELECT extract(epoch FROM updated_at)::float8 FROM backfill_jobs WHERE did = $1 AND depth = 'list_sync'";
    let claimed: f64 = sqlx::query_scalar(at).bind(STUCK).fetch_one(p).await.expect("claimed");
    // The lane is free again: the other owner runs.
    assert_eq!(sync.run_once().await.expect("claim").expect("a run"), (OTHER.to_string(), Outcome::Completed));
    assert_eq!(job(p, STUCK).await.0, "running");

    let check = tokio::spawn(sync.clone().stuck_loop());
    until(Duration::from_secs(15), "the stuck row re-queued", || async { job(p, STUCK).await.0 == "queued" }).await;
    check.abort();
    let requeued: f64 = sqlx::query_scalar(at).bind(STUCK).fetch_one(p).await.expect("re-queued");
    let waited = requeued - claimed;
    let bound = f64::from(STUCK_SECS);
    assert!(waited >= bound, "re-queued {waited:.2} s after the claim, before the stuck bound");
    assert!(waited <= 1.25 * bound + 0.25, "re-queued {waited:.2} s after the claim, past 1.25 × {bound} s");
    assert_eq!(job(p, STUCK).await.5, generation + 1);
    drop(failing);
    db.drop_db().await;
}

/// B-74 (item 12, LM-01): with L's row held `FOR UPDATE`, as a paused
/// batch holds it, and L's copy holding 50 000 rows, member A's
/// `muteActorList(L)` and `unmuteActorList(L)` answer within 200 ms, and
/// so do 20 alternating calls made while the applier is retrying L (the
/// median of the 22; each under 1 s, which a wait on the row would pass).
/// `pg_locks` taken during a member phase hold no lock of any mode on
/// `subscribed_lists`, `subscribed_owners` or `backfill_jobs`, and not the
/// `listref:total` advisory lock. After the row is let go, the list phase
/// makes the list active and enqueues its `list_sync`.
#[tokio::test]
async fn b74_a_held_list_row_never_slows_a_members_mute() {
    use crate::hydration::testing::{actor, exec, hydrator_configured, test_config};
    use crate::hydration::{HydrationConfig, Viewer};
    use crate::xrpc::pools::Requester;

    const A: &str = "did:plc:b74heldmembera";
    const OWNER: &str = "did:plc:b74heldowner";
    let Some(db) = fresh_db("b74_held_row").await else {
        return;
    };
    let p = &db.pool;
    actor(p, A, "a.test", "A").await;
    actor(p, OWNER, "owner.test", "Owner").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{A}', 'admin', now()), ('{OWNER}', 'admin', now())")).await;
    let list = list_of(OWNER);
    exec(
        p,
        &format!(
            "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES \
             ('{list}', '{OWNER}', '3klist1', 'bafylist', '3k', 'Mods', 'app.bsky.graph.defs#modlist', '{{}}', now())"
        ),
    )
    .await;
    let mut sets = ScopeSets::default();
    for did in [A, OWNER] {
        sets.members.insert(did.into());
        sets.scope.insert(did.into());
    }
    let applier = Applier::new(p.clone(), scope(), 0);
    let h = hydrator_configured(p, sets, HydrationConfig { list_applier: Some(applier.clone()), ..test_config() });
    let (me, req) = (Viewer { did: Some(A.into()), hydrate: true }, Requester::test());
    h.set_list_mute(&me, &req, &list, true).await.expect("db").expect("muted");
    applier.run_once().await.expect("applier");
    synced_copy(p, OWNER, &list, 50_000).await;
    let timed = |mute: bool| {
        let (h, me, req, list) = (&h, &me, &req, &list);
        async move {
            let began = Instant::now();
            tokio::time::timeout(Duration::from_secs(5), h.set_list_mute(me, req, list, mute))
                .await
                .expect("the request does not wait on the list's row")
                .expect("db")
                .expect("answered");
            began.elapsed()
        }
    };

    // The first sync ran.
    sqlx::query("UPDATE backfill_jobs SET state = 'done' WHERE did = $1").bind(OWNER).execute(p).await.expect("synced");

    let mut holder = p.begin().await.expect("tx");
    sqlx::query("SELECT 1 FROM subscribed_lists WHERE list_uri = $1 FOR UPDATE").bind(&list).execute(&mut *holder).await.expect("hold");
    let mut took = Vec::new();
    for mute in [false, true] {
        took.push(timed(mute).await);
    }
    // The applier, signalled by each call, finds the row busy and retries
    // it every second.
    let task = tokio::spawn(applier.clone().run());
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // Unmute, mute, …: the last call mutes.
    for i in 0..20 {
        took.push(timed(i % 2 == 1).await);
    }
    within_request_bound(&mut took);
    assert!(markers(p, &list).await > 0, "the list phase has not run while the row is held");
    assert_eq!(job(p, OWNER).await.0, "done");
    holder.rollback().await.expect("let go");
    // Let go, the list phase makes the list active and enqueues its sync.
    until(Duration::from_secs(10), "the list phase after the row is let go", || async {
        list_state(p, &list).await.as_deref() == Some("active") && markers(p, &list).await == 0
    })
    .await;
    assert_eq!(job(p, OWNER).await.0, "queued");
    assert!(!effective(p, A, &list).await, "a new admission waits on that sync");
    task.abort();

    // The locks of a member phase, read while it is held before its commit.
    let paused = hooks::arm(hooks::MEMBER_PHASE_BEFORE_COMMIT, A);
    let phase = tokio::spawn({
        let (p, list) = (p.clone(), list.clone());
        async move { unmute_list(&p, A, &list, Budget { max_references: 2, max_owners: 5 }).await }
    });
    paused.reached().await;
    // Its last statement wrote the change marker.
    let pid = idle_in_transaction_pid(p, "INSERT INTO list_reference_changes").await.expect("the member phase's backend");
    let relations = locked_relations(p, pid).await;
    assert!(relations.iter().any(|r| r == "list_references"), "the member phase's own writes show: {relations:?}");
    for shared in ["subscribed_lists", "subscribed_owners", "backfill_jobs"] {
        assert!(!relations.iter().any(|r| r == shared), "{shared} locked: {relations:?}");
    }
    assert!(holds_advisory(p, pid, &format!("listref:{A}")).await, "the member's own reference lock shows");
    assert!(!holds_advisory(p, pid, "listref:total").await);
    paused.release();
    assert!(phase.await.expect("task").expect("db"));
    drop(paused);
    db.drop_db().await;
}

/// B-74 crash safety: a process killed between a member phase's commit
/// and the list phase leaves the change marker; the next process's applier
/// processes it at startup and the reference takes effect after one sync.
/// The kill is a dropped applier that never ran; the restart a new one.
/// The recovery sweep's full reconcile also corrects a hand-made
/// `unreferenced` row that has an admitted reference.
#[tokio::test]
async fn b74_a_marker_left_by_a_kill_is_applied_after_restart() {
    const OWNER: &str = "did:plc:b74killowner";
    const M: &str = "did:plc:b74killmember";
    let Some(db) = fresh_db("b74_kill").await else {
        return;
    };
    let p = &db.pool;
    let list = list_of(OWNER);
    let before = Applier::new(p.clone(), scope(), 0);
    assert_eq!(mute_list(p, M, &list, BUDGET).await.expect("mute"), Muted::Added);
    drop(before);
    assert_eq!(markers(p, &list).await, 1);
    assert_eq!(list_state(p, &list).await, None);

    let after = Applier::new(p.clone(), scope(), 0);
    let task = tokio::spawn(after.clone().run());
    until(Duration::from_secs(10), "the startup pass", || async { list_state(p, &list).await.as_deref() == Some("active") && markers(p, &list).await == 0 }).await;
    task.abort();
    assert_eq!(job(p, OWNER).await.0, "queued");
    let sync = list_sync(p, &after, 3, lanes(1, 5400));
    assert_eq!(sync.run_once().await.expect("claim").expect("one sync"), (OWNER.to_string(), Outcome::Completed));
    assert!(effective(p, M, &list).await);

    // A row set `unreferenced` by hand, with no marker, while an admitted
    // reference exists: the recovery sweep's full reconcile corrects it.
    sqlx::query("UPDATE subscribed_lists SET state = 'unreferenced' WHERE list_uri = $1").bind(&list).execute(p).await.expect("by hand");
    sync.sweep_once().await.expect("recovery sweep");
    let (state, unsynced): (String, bool) = sqlx::query_as("SELECT state, synced_at IS NULL FROM subscribed_lists WHERE list_uri = $1")
        .bind(&list)
        .fetch_one(p)
        .await
        .expect("row");
    assert_eq!((state.as_str(), unsynced), ("active", true), "a new instance of the row, which a sync must serve");
    db.drop_db().await;
}

/// B-74 LM-03, LM-05: after the only holder unmutes a synced 50 000-item
/// list, its items and row are gone within 60 s with no retention run, and
/// the owner's row stays with its `last_run_ended_at` and `cut_until`.
#[tokio::test]
async fn b74_an_unmuted_50000_item_list_is_released_within_60s() {
    const OWNER: &str = "did:plc:b74releaseowner";
    const M: &str = "did:plc:b74releasemember";
    let Some(db) = fresh_db("b74_release_50000").await else {
        return;
    };
    let p = &db.pool;
    let list = list_of(OWNER);
    subscribe(p, M, &list).await;
    synced_copy(p, OWNER, &list, 50_000).await;
    sqlx::query("UPDATE subscribed_owners SET last_run_ended_at = '2026-10-01T00:00:00Z', cut_until = '2026-10-20T00:00:00Z' WHERE owner_did = $1")
        .bind(OWNER)
        .execute(p)
        .await
        .expect("pacing");
    let applier = Applier::new(p.clone(), scope(), 0);
    let task = tokio::spawn(applier.clone().run());

    assert!(unmute_list(p, M, &list, BUDGET).await.expect("unmute"));
    applier.signal();
    let took = until(Duration::from_secs(60), "the release", || async {
        list_state(p, &list).await.is_none() && count(p, "SELECT count(*) FROM subscribed_list_items WHERE list_uri = $1", &list).await == 0
    })
    .await;
    task.abort();
    eprintln!("b74: a 50 000-item list released in {took:?}");
    let pacing: (String, String) = sqlx::query_as(
        "SELECT to_char(last_run_ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'), to_char(cut_until AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM subscribed_owners WHERE owner_did = $1",
    )
    .bind(OWNER)
    .fetch_one(p)
    .await
    .expect("the owner's row stays");
    assert_eq!(pacing, ("2026-10-01".to_string(), "2026-10-20".to_string()));
    db.drop_db().await;
}

/// B-74 LM-10 and the re-mute during a release: with the release of a
/// synced 50 000-item list paused between two batches,
/// - a mute and an unmute made then leave a release that still finishes:
///   the list's rows are gone within 60 s of the first unmute;
/// - a re-mute made then stops the release, keeps the row `active` (a new
///   instance), and the reference applies after one sync. This half uses a
///   copy of one item more than a release batch: enough to pause between
///   batches, and the new instance's first sync stays short.
#[tokio::test]
async fn b74_a_mute_during_a_release_stops_it_and_an_unmute_lets_it_finish() {
    const OWNER: &str = "did:plc:b74remuteowner";
    const M: &str = "did:plc:b74remutemember";
    let Some(db) = fresh_db("b74_remute").await else {
        return;
    };
    let p = &db.pool;
    let list = list_of(OWNER);
    let items = |p: &PgPool, list: &str| {
        let (p, list) = (p.clone(), list.to_string());
        async move { count(&p, "SELECT count(*) FROM subscribed_list_items WHERE list_uri = $1", &list).await }
    };
    let applier = Applier::new(p.clone(), scope(), 0);
    let task = tokio::spawn(applier.clone().run());
    let toggle = |mute: bool| {
        let (applier, list) = (applier.clone(), list.clone());
        async move {
            if mute {
                assert_eq!(mute_list(p, M, &list, BUDGET).await.expect("mute"), Muted::Added);
            } else {
                assert!(unmute_list(p, M, &list, BUDGET).await.expect("unmute"));
            }
            applier.signal();
        }
    };

    // Mute and unmute during the release: it still finishes.
    subscribe(p, M, &list).await;
    synced_copy(p, OWNER, &list, 50_000).await;
    let paused = hooks::arm(hooks::RELEASE_BETWEEN_BATCHES, &list);
    let started = Instant::now();
    toggle(false).await;
    paused.reached().await;
    assert_eq!(items(p, &list).await, 40_000, "one batch done");
    toggle(true).await;
    until(Duration::from_secs(10), "active again", || async { list_state(p, &list).await.as_deref() == Some("active") }).await;
    toggle(false).await;
    until(Duration::from_secs(10), "unreferenced again", || async { list_state(p, &list).await.as_deref() == Some("unreferenced") && markers(p, &list).await == 0 }).await;
    drop(paused);
    until(Duration::from_secs(60), "the release", || async { list_state(p, &list).await.is_none() && items(p, &list).await == 0 }).await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(60), "{took:?}");
    eprintln!("b74: a 50 000-item list muted and unmuted during its release, gone in {took:?}");

    // A re-mute during the release: stopped, active, applied after a sync.
    // One item past a batch is enough to pause between two batches, and
    // keeps the new instance's first sync short.
    toggle(true).await;
    until(Duration::from_secs(10), "subscribed", || async { list_state(p, &list).await.as_deref() == Some("active") }).await;
    synced_copy(p, OWNER, &list, list_phase::RELEASE_BATCH + 1).await;
    let before: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT activated_at FROM subscribed_lists WHERE list_uri = $1").bind(&list).fetch_one(p).await.expect("instance");
    let paused = hooks::arm(hooks::RELEASE_BETWEEN_BATCHES, &list);
    toggle(false).await;
    paused.reached().await;
    toggle(true).await;
    until(Duration::from_secs(10), "active again", || async { list_state(p, &list).await.as_deref() == Some("active") && markers(p, &list).await == 0 }).await;
    paused.release();
    // The release stops at its next batch: the rest of the copy stays.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(list_state(p, &list).await.as_deref(), Some("active"));
    assert_eq!(items(p, &list).await, 1);
    let after: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT activated_at FROM subscribed_lists WHERE list_uri = $1").bind(&list).fetch_one(p).await.expect("instance");
    assert!(after > before, "a new instance of the row");
    assert!(!effective(p, M, &list).await);
    task.abort();
    let sync = list_sync(p, &applier, 3, lanes(1, 5400));
    assert_eq!(sync.run_once().await.expect("claim").expect("one sync"), (OWNER.to_string(), Outcome::Completed));
    assert!(effective(p, M, &list).await);
    assert_eq!(items(p, &list).await, 3, "the new instance's first sync keeps only what is listed");
    drop(paused);
    db.drop_db().await;
}

/// B-74 LM-08 (skipped row): with L's row held across A's
/// `unmuteActorList` of the last admitted reference, the applier keeps the
/// marker while it cannot take the row, and L becomes `unreferenced` within
/// 35 s of the row's release.
#[tokio::test]
async fn b74_a_skipped_list_becomes_unreferenced_soon_after_its_row_is_free() {
    const OWNER: &str = "did:plc:b74skippedowner";
    const A: &str = "did:plc:b74skippedmember";
    let Some(db) = fresh_db("b74_skipped").await else {
        return;
    };
    let p = &db.pool;
    let list = list_of(OWNER);
    subscribe(p, A, &list).await;
    synced_copy(p, OWNER, &list, 10).await;
    let applier = Applier::new(p.clone(), scope(), 0);
    let task = tokio::spawn(applier.clone().run());

    let mut holder = p.begin().await.expect("tx");
    sqlx::query("SELECT 1 FROM subscribed_lists WHERE list_uri = $1 FOR UPDATE").bind(&list).execute(&mut *holder).await.expect("hold");
    assert!(unmute_list(p, A, &list, BUDGET).await.expect("unmute"));
    applier.signal();
    // Two retries while it is held: the marker stays, the row is untouched.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(markers(p, &list).await, 1);
    assert_eq!(list_state(p, &list).await.as_deref(), Some("active"));
    holder.rollback().await.expect("let go");
    let took = until(Duration::from_secs(35), "unreferenced", || async { list_state(p, &list).await.as_deref() != Some("active") }).await;
    assert_eq!(markers(p, &list).await, 0);
    eprintln!("b74: a skipped list unreferenced {took:?} after its row was let go");
    task.abort();
    db.drop_db().await;
}
