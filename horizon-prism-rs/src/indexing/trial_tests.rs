//! Full trials against Postgres.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::public_mode::{Breadth, Phase, PublicIndexing, PublicMode, PublicModeSettings, ScopeValue, Spawn, SweepList, Transition, now, rfc3339};
use super::trial::{Bounds, Class, TrialState, class_of};
use crate::aggregations::ScopeState;
use crate::config::{IndexingConfig, PublicScope, ScopePolicy};
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};
use crate::private_tier::DisabledPrivateTier;
use crate::test_support::fresh_db;
use crate::test_support::posts::tid;

/// S (not a member), a member, strangers.
const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const MEMBER: &str = "did:plc:cccccccccccccccccccccccc";
const D: &str = "did:plc:dddddddddddddddddddddddd";
const STRANGER: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
const OTHER: &str = "did:plc:ffffffffffffffffffffffff";
const LATE: &str = "did:plc:gggggggggggggggggggggggg";

fn settings() -> PublicModeSettings {
    PublicModeSettings {
        check_every: Duration::from_millis(50),
        fill_batch: 2,
        sweep_window: Some(3),
        retry_base: Duration::from_millis(10),
        failure_pause: Duration::from_millis(50),
        fill_max_wait: Duration::ZERO,
        trial_check_every: Duration::from_millis(50),
        ..PublicModeSettings::from_config(&IndexingConfig::default())
    }
}

fn bounds() -> Bounds {
    Bounds { hours: 24.0, max_added_gb: 50.0, end_below_free_percent: 10, history: false, end_on_lag: true, trim_margin_minutes: 15 }
}

fn file(scope: ScopeValue) -> IndexingConfig {
    let s = match scope {
        ScopeValue::Community => PublicScope::Community,
        ScopeValue::KryphocronOnly => PublicScope::KryphocronOnly,
        ScopeValue::Full => PublicScope::Full,
    };
    IndexingConfig { public_scope: Some(s), ..IndexingConfig::default() }
}

async fn exec(pool: &PgPool, sql: &str) {
    sqlx::raw_sql(sql).execute(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn transition(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'public_transition'").fetch_one(pool).await.expect("transition")
}

async fn eventually(what: &str, mut f: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

const LIVE: ApplyOptions = ApplyOptions {
    advance_applied_rev: false,
    dead_letter_readiness: false,
    attempts: 1,
    live: true,
    subscription_rules: true,
    source: WriteSource::Firehose,
};

fn op(collection: &str, rkey: &str, record: Value) -> RawOp {
    RawOp { action: Action::Create, collection: collection.into(), rkey: rkey.into(), cid: Some("bafyop".into()), record: Some(record) }
}

fn delete(collection: &str, rkey: &str) -> RawOp {
    RawOp { action: Action::Delete, collection: collection.into(), rkey: rkey.into(), cid: None, record: None }
}

fn created() -> String {
    rfc3339(&now())
}

fn post_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

fn post(rkey: &str, text: &str) -> RawOp {
    op("app.bsky.feed.post", rkey, json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": created()}))
}

fn tagged(rkey: &str) -> RawOp {
    op("app.bsky.feed.post", rkey, json!({"$type": "app.bsky.feed.post", "text": "tagged", "tags": ["trialtag"], "createdAt": created()}))
}

fn reply(rkey: &str, parent: &str, quote: Option<&str>) -> RawOp {
    let mut r = json!({"$type": "app.bsky.feed.post", "text": "reply", "createdAt": created(),
                       "reply": {"root": {"uri": parent, "cid": "bafyc"}, "parent": {"uri": parent, "cid": "bafyc"}}});
    if let Some(q) = quote {
        r["embed"] = json!({"$type": "app.bsky.embed.record", "record": {"uri": q, "cid": "bafyq"}});
    }
    op("app.bsky.feed.post", rkey, r)
}

fn like(rkey: &str, subject: &str) -> RawOp {
    op("app.bsky.feed.like", rkey, json!({"$type": "app.bsky.feed.like", "subject": {"uri": subject, "cid": "bafyc"}, "createdAt": created()}))
}

fn follow(rkey: &str, subject: &str) -> RawOp {
    op("app.bsky.graph.follow", rkey, json!({"$type": "app.bsky.graph.follow", "subject": subject, "createdAt": created()}))
}

fn profile(name: &str) -> RawOp {
    op("app.bsky.actor.profile", "self", json!({"$type": "app.bsky.actor.profile", "displayName": name}))
}

fn list_item(rkey: &str, owner: &str) -> RawOp {
    op(
        "app.bsky.graph.listitem",
        rkey,
        json!({"$type": "app.bsky.graph.listitem", "subject": D, "list": format!("at://{owner}/app.bsky.graph.list/3kaaaaaaaaaaa"), "createdAt": created()}),
    )
}

async fn members(pool: &PgPool, dids: &[&str]) {
    for d in dids {
        sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())").bind(d).execute(pool).await.expect("member");
    }
}

async fn in_scope(pool: &PgPool, dids: &[&str]) {
    for d in dids {
        sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2) ON CONFLICT DO NOTHING").bind(d).execute(pool).await.expect("scope");
    }
}

/// A stack in `from`, settled, with S = {MEMBER, B}, a member, a writer,
/// and a database volume that is 90 % free.
async fn stack(p: &PgPool, from: ScopeValue, last_fill_since: Option<DateTime<Utc>>) -> (PublicIndexing, ScopeState, Writer) {
    members(p, &[MEMBER]).await;
    in_scope(p, &[MEMBER, B]).await;
    let phase = if from == ScopeValue::KryphocronOnly { Phase::Off } else { Phase::On };
    sqlx::query("INSERT INTO operator_settings (key, value, file_value, updated_by) VALUES ('indexing.public_scope', $1, $2, 'cli')")
        .bind(json!(from.as_str()))
        .bind(json!({"value": from.as_str(), "explicit": true}))
        .execute(p)
        .await
        .expect("setting");
    sqlx::query("INSERT INTO worker_state (key, value) VALUES ('public_transition', $1)")
        .bind(Transition::settled(phase, 3, now(), last_fill_since).to_json())
        .execute(p)
        .await
        .expect("transition");
    let (pi, scope) = boot(p, from).await;
    let writer = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    (pi, scope, writer)
}

/// A (re)start of the switch over the stored state; the task it hands
/// back is spawned.
async fn boot(p: &PgPool, file_value: ScopeValue) -> (PublicIndexing, ScopeState) {
    boot_with(p, file_value, true).await
}

async fn boot_with(p: &PgPool, file_value: ScopeValue, spawn: bool) -> (PublicIndexing, ScopeState) {
    let scope = ScopeState::load(p, &DisabledPrivateTier, ScopePolicy::MembersAndOneHop, 10_000).await.expect("scope");
    scope.load_full(p, &crate::config::FullConfig::default(), &["bsky".to_string()]).await.expect("full sets");
    let pi = PublicIndexing::new(p.clone(), scope.clone(), settings());
    *pi.hooks.volume.lock().expect("hooks") = Some((90, 100));
    let task = pi.startup(&file(file_value)).await.expect("startup");
    if let Some(task) = task.filter(|_| spawn) {
        drop(pi.spawn(task));
    }
    (pi, scope)
}

/// The record, as `(table, did, b, c, s_a did, s_b, q_a did, q_b)`.
type Recorded = (String, String, i64, i16, Option<String>, Option<i64>, Option<String>, Option<i64>);

async fn record(p: &PgPool) -> Vec<Recorded> {
    type Row = (i16, String, i64, i16, Option<String>, Option<i64>, Option<String>, Option<i64>);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT r.tbl, d.did, r.b, r.c, sd.did, r.s_b, qd.did, r.q_b FROM full_trial_rows r JOIN dids d ON d.id = r.a \
         LEFT JOIN dids sd ON sd.id = r.s_a LEFT JOIN dids qd ON qd.id = r.q_a ORDER BY 1, 2, 3, 4",
    )
    .fetch_all(p)
    .await
    .expect("record");
    rows.into_iter()
        .map(|(t, d, b, c, sa, sb, qa, qb)| (super::trial::TrialTable::from_code(t).map_or("?", |t| t.name()).to_string(), d, b, c, sa, sb, qa, qb))
        .collect()
}

fn tables_of<'a>(rec: &'a [Recorded], did: &str) -> Vec<&'a str> {
    rec.iter().filter(|r| r.1 == did).map(|r| r.0.as_str()).collect()
}

async fn rows_of(p: &PgPool, table: &str, did: &str) -> i64 {
    let col = match table {
        "feed_items" => "originator",
        "actors" | "actor_counts" => "id",
        _ => "author",
    };
    count(p, &format!("SELECT count(*) FROM {table} t JOIN dids d ON d.id = t.{col} WHERE d.did = '{did}'")).await
}

async fn last_trial(p: &PgPool) -> (String, Value) {
    sqlx::query_as("SELECT state, COALESCE(result, 'null'::jsonb) FROM full_trials ORDER BY id DESC LIMIT 1").fetch_one(p).await.expect("trial row")
}

/// Wait until the trial has left memory (passed) or failed.
async fn settled(scope: &ScopeState) {
    eventually("the trial's check", async || scope.read_guard().await.trial.as_ref().is_none_or(|t| t.state == TrialState::Failed)).await;
}

/// Set the in-memory trial's times (a clock moved on).
async fn shift(scope: &ScopeState, ends_at: Option<DateTime<Utc>>, started_at: Option<DateTime<Utc>>) {
    let mut sets = scope.write_guard().await;
    let mut t = sets.trial.clone().expect("a trial");
    if let Some(e) = ends_at {
        t.ends_at = e;
    }
    if let Some(s) = started_at {
        t.started_at = s;
    }
    scope.set_trial(&mut sets, Some(t));
}

// ---------------------------------------------------------------------
// Starts and the refused states.
// ---------------------------------------------------------------------

/// From community a trial is On, Full with the trial running and its
/// baseline; it ends back in On, Community with no fill and passes. A start
/// is refused while one runs, with an undropped record, in Full, Filling,
/// Draining, Narrowing or Held.
#[tokio::test]
async fn a_trial_from_community_returns_to_community() {
    let Some(db) = fresh_db("trial_community").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, _w) = stack(p, ScopeValue::Community, None).await;
    let started = pi.start_trial(bounds(), "cli").await.expect("start");
    assert_eq!((started.from, started.action), (ScopeValue::Community, "breadth"));
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::On, Breadth::Full));
    let t = transition(p).await;
    assert_eq!((&t["trial"]["state"], &t["trial"]["from"], &t["trial"]["to"]), (&json!("running"), &json!("community"), &json!("community")));
    let (state, _) = last_trial(p).await;
    assert_eq!(state, "running");
    let baseline: Value = sqlx::query_scalar("SELECT baseline FROM full_trials").fetch_one(p).await.expect("baseline");
    assert!(baseline["tables"]["posts"]["rows"].is_number() && baseline["database_bytes"].is_number() && baseline["worker_state_keys"].is_array());
    assert!(pi.hooks.spawned().contains(&"trial_bounds"), "{:?}", pi.hooks.spawned());
    assert!(pi.start_trial(bounds(), "cli").await.expect_err("one at a time").0.contains("running"));
    // End it: the exit is a trial narrowing, which ends in On, Community
    // with no refill, and the check passes and drops the trial.
    let applied = pi.end_trial("manual").await.expect("end").expect("ended");
    assert_eq!(applied.action, "narrowing");
    assert_eq!(transition(p).await["sweep"]["list"], "trial_narrowing");
    settled(&scope).await;
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::On, Breadth::Community));
    let t = transition(p).await;
    assert_eq!((&t["phase"], t.get("trial")), (&json!("on"), None));
    assert!(!pi.hooks.spawned().contains(&"fill"), "no refill: {:?}", pi.hooks.spawned());
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    // An undropped record refuses a start.
    exec(p, &format!("INSERT INTO dids (did) VALUES ('{D}') ON CONFLICT DO NOTHING; INSERT INTO full_trial_rows (tbl, a, b, c) SELECT 1, id, 1, 0 FROM dids WHERE did = '{D}'")).await;
    assert!(pi.start_trial(bounds(), "cli").await.expect_err("record").0.contains("record"));
    pi.dismiss_trial().await.expect("dismiss");
    // Full, Filling, Draining, Narrowing and Held refuse.
    pi.apply_scope(ScopeValue::Full, "cli").await.expect("full");
    assert!(pi.start_trial(bounds(), "cli").await.expect_err("full").0.contains("already on"));
    let narrowing = pi.apply_scope(ScopeValue::Community, "cli").await.expect("narrow");
    assert_eq!(narrowing.action, "narrowing");
    assert!(pi.start_trial(bounds(), "cli").await.expect_err("narrowing").0.contains("narrowing"));
    exec(p, "UPDATE worker_state SET value = jsonb_set(value, '{phase}', '\"filling\"') - 'sweep' - 's_filled' || '{\"breadth\": \"community\"}' WHERE key = 'public_transition'").await;
    exec(p, "UPDATE worker_state SET value = jsonb_set(value, '{fill}', '{\"stage\": \"enqueue\", \"part\": \"members\", \"after\": \"\", \"enqueued\": 0, \"since\": \"2026-11-01T00:00:00Z\", \"waiting_since\": null}') WHERE key = 'public_transition'").await;
    let (pi, _) = boot_with(p, ScopeValue::Community, false).await;
    assert!(pi.start_trial(bounds(), "cli").await.expect_err("filling").0.contains("fill"));
    exec(p, "UPDATE worker_state SET value = jsonb_set(value, '{phase}', '\"nowhere\"') WHERE key = 'public_transition'").await;
    let (pi, _) = boot(p, ScopeValue::Community).await;
    assert!(pi.start_trial(bounds(), "cli").await.expect_err("held").0.contains("Held"));
    exec(p, "UPDATE worker_state SET value = jsonb_set(jsonb_set(value, '{phase}', '\"on\"'), '{generation}', '40') WHERE key = 'public_transition'").await;
    let (pi, _) = boot(p, ScopeValue::Community).await;
    let draining = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("drain");
    assert_eq!(draining.action, "draining");
    assert!(pi.start_trial(bounds(), "cli").await.expect_err("draining").0.contains("drain"));
    db.drop_db().await;
}

/// From kryphocron_only, S's fill runs first (Filling,
/// Full); S's fill rows and a member's post row are trial rows; the exit is
/// a drain to Off, which removes them, with `last_fill_since` back; the
/// check passes.
#[tokio::test]
async fn a_trial_from_kryphocron_only_fills_then_drains() {
    let Some(db) = fresh_db("trial_off").await else {
        return;
    };
    let p = &db.pool;
    let before = now() - TimeDelta::days(3);
    let (pi, scope, w) = stack(p, ScopeValue::KryphocronOnly, Some(before)).await;
    let started = pi.start_trial(bounds(), "cli").await.expect("start");
    assert_eq!((started.from, started.action), (ScopeValue::KryphocronOnly, "filling"));
    assert_eq!(scope.public_breadth(), Breadth::Full);
    // S's fill: a backfilled post of B, and a member's post.
    let backfill = ApplyOptions { source: WriteSource::Backfill, live: false, ..LIVE };
    assert_eq!(w.apply(B, &tid(1), &[post("3kaaaaaaaab22", "filled")], backfill).await.expect("fill").outcomes, [OpOutcome::Written]);
    assert_eq!(w.apply(MEMBER, &tid(2), &[post("3kaaaaaaaab23", "member")], LIVE).await.expect("member").outcomes, [OpOutcome::Written]);
    eventually("on, full", async || scope.public_mode() == PublicMode::On).await;
    let rec = record(p).await;
    assert!(tables_of(&rec, B).contains(&"posts"), "{rec:?}");
    assert!(tables_of(&rec, MEMBER).contains(&"posts"), "a member's post row is the trial's: {rec:?}");
    assert!(tables_of(&rec, MEMBER).contains(&"feed_items"), "no feed item under the off rule: {rec:?}");
    assert_eq!(pi.end_trial("manual").await.expect("end").expect("ended").action, "draining");
    assert_eq!(transition(p).await["sweep"]["list"], "trial_full");
    settled(&scope).await;
    assert_eq!(scope.public_mode(), PublicMode::Off);
    assert_eq!(rows_of(p, "posts", B).await + rows_of(p, "posts", MEMBER).await, 0, "the drain removed S's fill rows");
    assert_eq!(count(p, "SELECT count(*) FROM member_post_refs").await, 1, "the member's reference is the start mode's");
    let t = transition(p).await;
    assert_eq!(t["last_fill_since"], json!(rfc3339(&before)), "last_fill_since is back");
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    db.drop_db().await;
}

/// A kryphocron_only stack with state of its own before a trial: a count
/// row from a member's like of a stranger's post, two trend hours, and
/// finished `resync` jobs of the member and of an account outside S.
/// Returns the writer and the count query of the stranger's post.
async fn off_with_state(p: &PgPool) -> (PublicIndexing, ScopeState, Writer, String) {
    let (pi, scope, w) = stack(p, ScopeValue::KryphocronOnly, Some(now() - TimeDelta::days(3))).await;
    let liked = post_uri(STRANGER, "3kaaaaaaaab31");
    assert_eq!(w.apply(MEMBER, &tid(1), &[like("3kaaaaaaaab30", &liked)], LIVE).await.expect("like").outcomes, [OpOutcome::Written]);
    let likes = format!("SELECT COALESCE((SELECT like_count::bigint FROM post_counts pc WHERE (pc.author, pc.rkey) = (SELECT r.author, r.rkey FROM ref_of('{liked}') r)), 0)");
    assert_eq!(count(p, &likes).await, 1, "the member's like counts");
    exec(
        p,
        &format!(
            "INSERT INTO tag_activity_hourly (tag, hour, posts) VALUES \
               ('pretag', date_trunc('hour', now(), 'UTC') - interval '2 hours', 3), ('trialtag', date_trunc('hour', now(), 'UTC'), 1); \
             INSERT INTO backfill_jobs (did, depth, state, attempts, generation, not_before, updated_at) VALUES \
               ('{MEMBER}', 'resync', 'done', 0, 4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'), \
               ('{OTHER}', 'resync', 'done', 0, 2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');"
        ),
    )
    .await;
    (pi, scope, w, likes)
}

/// `(state, generation, updated before the trial)` of a DID's `resync` job.
async fn resync_job(p: &PgPool, did: &str) -> Option<(String, i64, bool)> {
    sqlx::query_as("SELECT state, generation, updated_at < '2026-01-02T00:00:00Z' FROM backfill_jobs WHERE did = $1 AND depth = 'resync'")
        .bind(did)
        .fetch_optional(p)
        .await
        .expect("job")
}

const TRIAL_TAG: &str = "SELECT COALESCE(sum(posts), 0)::bigint FROM tag_activity_hourly WHERE tag = 'trialtag'";
const PRE_TAG: &str = "SELECT COALESCE(sum(posts), 0)::bigint FROM tag_activity_hourly WHERE tag = 'pretag'";

/// A trial from kryphocron_only gives back what the start mode held: the
/// drain leaves the count row of a member's like and the trend hours from
/// before the trial (the trial's like and tagged post are taken back by
/// their triggers), and the fill's jobs are put back as they were, the one
/// it created removed; the check compares them and passes.
#[tokio::test]
async fn a_trial_from_kryphocron_only_leaves_counts_trends_and_jobs_as_they_were() {
    off_state_trial("trial_off_state", false).await;
}

/// The same with the counts folder slowed to 5 s and reads not folding
/// first from the trial's rows on: the exit and the check settle the counts
/// themselves, so nothing the lag holds is counted as left behind.
#[tokio::test]
async fn a_trial_from_kryphocron_only_passes_with_counts_held_back() {
    off_state_trial("trial_off_state_lagged", true).await;
}

async fn off_state_trial(name: &str, lagged: bool) {
    let Some(db) = fresh_db(name).await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, w, likes) = off_with_state(p).await;
    pi.start_trial(bounds(), "cli").await.expect("start");
    eventually("on, full", async || scope.public_mode() == PublicMode::On).await;
    // The fill queued the member's job again and created S's.
    assert_eq!(resync_job(p, MEMBER).await, Some(("queued".into(), 5, false)));
    assert_eq!(resync_job(p, B).await.map(|j| (j.0, j.1)), Some(("queued".into(), 0)));
    let recorded = count(p, "SELECT count(*) FROM full_trial_jobs").await;
    let both = format!("SELECT count(*) FROM full_trial_jobs WHERE did IN ('{MEMBER}', '{B}')");
    assert_eq!(count(p, &both).await, 2, "the member's and S's");
    assert_eq!(count(p, &format!("SELECT count(*) FROM full_trial_jobs WHERE did = '{OTHER}'")).await, 0, "not a fill job");
    // The trial: a stranger's like of the same post and a tagged post.
    // And a reply to a tagged post of its own, which the drain removes
    // after the parent, so the reply's trigger cannot take its count back.
    let parent = op("app.bsky.feed.post", "3kaaaaaaaab34", json!({"$type": "app.bsky.feed.post", "text": "p", "tags": ["replytag"], "createdAt": created()}));
    let child = reply("3kaaaaaaaab35", &post_uri(STRANGER, "3kaaaaaaaab34"), None);
    w.apply(STRANGER, &tid(2), &[like("3kaaaaaaaab32", &post_uri(STRANGER, "3kaaaaaaaab31")), tagged("3kaaaaaaaab33"), parent, child], LIVE)
        .await
        .expect("trial rows");
    assert_eq!(count(p, &likes).await, 2);
    assert_eq!(count(p, TRIAL_TAG).await, 2);
    let reply_tag = "SELECT count(*) FROM tag_activity_hourly WHERE tag = 'replytag'";
    assert_eq!(count(p, reply_tag).await, 1);
    let held_back = lagged.then(|| {
        let folder = crate::counts::Folder::new(
            p.clone(),
            &crate::config::CountsConfig { fold_interval_ms: 5000, ..crate::config::CountsConfig::default() },
            std::sync::Arc::new(crate::counts::FoldStatus::default()),
        )
        .spawn();
        (crate::test_support::hooks::arm(crate::test_support::hooks::COUNTS_LAG, db.name()), folder)
    });
    assert_eq!(pi.end_trial("manual").await.expect("end").expect("ended").action, "draining");
    settled(&scope).await;
    if let Some((lag, folder)) = held_back {
        folder.abort();
        drop(lag);
    }
    assert_eq!(scope.public_mode(), PublicMode::Off);
    assert_eq!(count(p, &likes).await, 1, "the member's like still counts");
    assert_eq!((count(p, PRE_TAG).await, count(p, TRIAL_TAG).await), (3, 1), "the trend hours from before the trial stay");
    assert_eq!(count(p, reply_tag).await, 0, "a trend row the trial added is gone");
    assert_eq!(resync_job(p, MEMBER).await, Some(("done".into(), 4, true)), "the member's job is as it was");
    assert_eq!(resync_job(p, B).await, None, "the job the fill created is gone");
    assert_eq!(resync_job(p, OTHER).await, Some(("done".into(), 2, true)), "a job the fill did not touch");
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    let held = &result["start_mode"];
    assert_eq!(
        (&held["post_counts_missing"]["now"], &held["trend_rows_lost"], &held["trend_rows_added"], &held["fill_jobs"]["recorded"]),
        (&json!(0), &json!(0), &json!(0), &json!(recorded)),
        "{result}"
    );
    assert!(result["words"].as_str().unwrap_or_default().contains(&format!("the fill's {recorded} jobs put back")), "{result}");
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_jobs").await, 0, "the record is dropped");
    db.drop_db().await;
}

/// The check fails when the start mode's own state is not back: a count
/// row stored rows still need (repaired), a trend row from before the trial
/// (not repairable: it fails again until dismissed) and a fill job left
/// queued (put back).
#[tokio::test]
async fn the_check_fails_on_lost_counts_trends_and_jobs() {
    let Some(db) = fresh_db("trial_off_lost").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, _w, likes) = off_with_state(p).await;
    pi.start_trial(bounds(), "cli").await.expect("start");
    eventually("on, full", async || scope.public_mode() == PublicMode::On).await;
    let pause = pi.hooks.arm("trial_check");
    pi.end_trial("manual").await.expect("end");
    pause.0.notified().await;
    // What the exit used to do.
    exec(
        p,
        &format!(
            "DELETE FROM post_counts; DELETE FROM tag_activity_hourly WHERE tag = 'pretag'; \
             INSERT INTO tag_activity_hourly (tag, hour, replies) VALUES ('lefttag', date_trunc('hour', now(), 'UTC'), 2); \
             UPDATE backfill_jobs SET state = 'queued', generation = 5 WHERE did = '{MEMBER}' AND depth = 'resync';"
        ),
    )
    .await;
    pause.1.notify_one();
    settled(&scope).await;
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "failed", "{result}");
    let causes = result["causes"].to_string();
    for part in ["had no count row", "trend rows from before the trial are gone", "trend rows the trial added", "jobs had not been put back"] {
        assert!(causes.contains(part), "{part}: {causes}");
    }
    assert_eq!(count(p, &likes).await, 1, "the count row is repaired");
    assert_eq!(count(p, "SELECT count(*) FROM tag_activity_hourly WHERE tag = 'lefttag'").await, 0, "the added trend row is removed");
    assert_eq!(resync_job(p, MEMBER).await, Some(("done".into(), 4, true)), "the job is put back");
    pi.recheck_trial().await.expect("check again");
    settled(&scope).await;
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "failed", "the trend row is still gone: {result}");
    assert_eq!(result["causes"].as_array().map(Vec::len), Some(1), "{result}");
    pi.dismiss_trial().await.expect("dismiss");
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_jobs").await, 0);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// Full's labels at the exit.
// ---------------------------------------------------------------------

async fn add_labels(p: &PgPool, vals: &[&str]) {
    for v in vals {
        sqlx::query("INSERT INTO full_labels (src, uri, val, cts, verified) VALUES ('did:plc:examplelabeleraaaaaaaaa', $1, $2, now(), true)")
            .bind(post_uri(STRANGER, "3kaaaaaaaab22"))
            .bind(v)
            .execute(p)
            .await
            .expect("label");
    }
}

/// The table's storage file: a truncate gives it a new one, a delete keeps
/// it.
async fn labels_file(p: &PgPool) -> i64 {
    count(p, "SELECT relfilenode::bigint FROM pg_class WHERE relname = 'full_labels' AND relnamespace = current_schema()::regnamespace").await
}

/// A trial that started with no full labels: its exit truncates the table
/// it wrote, and the check passes and says so.
#[tokio::test]
async fn a_trial_that_started_without_full_labels_truncates_them() {
    let Some(db) = fresh_db("trial_labels_truncate").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, _w) = stack(p, ScopeValue::Community, None).await;
    pi.start_trial(bounds(), "cli").await.expect("start");
    add_labels(p, &["spam", "rude"]).await;
    let file = labels_file(p).await;
    assert_eq!(pi.end_trial("manual").await.expect("end").expect("ended").action, "narrowing");
    settled(&scope).await;
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    assert_eq!(count(p, "SELECT count(*) FROM full_labels").await, 0);
    assert_ne!(labels_file(p).await, file, "truncated, not deleted");
    assert_eq!(result["full_labels"], json!({"rows_at_start": 0, "emptied_by": "truncate"}), "{result}");
    let words = result["words"].as_str().unwrap_or_default();
    assert!(words.contains("Full labels: none when the trial started; the exit truncated the table.") && words.contains("Full-only tables: empty."), "{words}");
    db.drop_db().await;
}

/// A trial that started with full labels an earlier full left: its exit
/// deletes every row instead (a drain from kryphocron_only here), and the
/// check passes and says so.
#[tokio::test]
async fn a_trial_that_started_with_full_labels_deletes_them() {
    let Some(db) = fresh_db("trial_labels_delete").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, w) = stack(p, ScopeValue::KryphocronOnly, Some(now() - TimeDelta::days(3))).await;
    add_labels(p, &["old"]).await;
    pi.start_trial(bounds(), "cli").await.expect("start");
    let backfill = ApplyOptions { source: WriteSource::Backfill, live: false, ..LIVE };
    w.apply(B, &tid(1), &[post("3kaaaaaaaab22", "filled")], backfill).await.expect("fill");
    eventually("on, full", async || scope.public_mode() == PublicMode::On).await;
    add_labels(p, &["spam", "rude"]).await;
    let file = labels_file(p).await;
    assert_eq!(pi.end_trial("manual").await.expect("end").expect("ended").action, "draining");
    assert_eq!(transition(p).await["sweep"]["list"], "trial_full");
    settled(&scope).await;
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    assert_eq!(count(p, "SELECT count(*) FROM full_labels").await, 0);
    assert_eq!(labels_file(p).await, file, "deleted row by row");
    assert_eq!(result["full_labels"], json!({"rows_at_start": 1, "emptied_by": "delete"}), "{result}");
    let words = result["words"].as_str().unwrap_or_default();
    assert!(words.contains("Full labels: 1 when the trial started, left by an earlier full; the exit deleted every row.") && words.contains("Full-only tables: empty."), "{words}");
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// What is recorded, and quiet.
// ---------------------------------------------------------------------

/// A stranger's like, reply-with-quote, follow, profile and list item are
/// recorded with their keys and subjects; S's and a member's ops are not;
/// an op whose savepoint rolled back and an op LWW refused are not; a
/// stranger's post mentioning a member writes no notification.
#[tokio::test]
async fn trial_rows_are_recorded_with_their_subjects_and_are_quiet() {
    let Some(db) = fresh_db("trial_record").await else {
        return;
    };
    let p = &db.pool;
    let (pi, _scope, w) = stack(p, ScopeValue::Community, None).await;
    let s_post = post_uri(B, "3kaaaaaaaab22");
    let d_post = post_uri(D, "3kaaaaaaaab23");
    w.apply(B, &tid(1), &[post("3kaaaaaaaab22", "s")], LIVE).await.expect("s post");
    pi.start_trial(bounds(), "cli").await.expect("start");
    let r = w
        .apply(
            STRANGER,
            &tid(2),
            &[like("3kaaaaaaaab24", &s_post), reply("3kaaaaaaaab25", &s_post, Some(&d_post)), follow("3kaaaaaaaab26", D), profile("x"), list_item("3kaaaaaaaab27", STRANGER)],
            LIVE,
        )
        .await
        .expect("stranger");
    assert!(r.outcomes.iter().all(|o| *o == OpOutcome::Written), "{:?}", r.outcomes);
    // S's and a member's own ops are the start mode's.
    w.apply(B, &tid(3), &[post("3kaaaaaaaab28", "s again"), like("3kaaaaaaaab29", &d_post)], LIVE).await.expect("s");
    w.apply(MEMBER, &tid(4), &[post("3kaaaaaaaab2a", "member")], LIVE).await.expect("member");
    let rec = record(p).await;
    assert!(tables_of(&rec, B).is_empty() && tables_of(&rec, MEMBER).is_empty(), "{rec:?}");
    let mut stranger = tables_of(&rec, STRANGER);
    stranger.sort_unstable();
    assert_eq!(stranger, ["actors", "feed_items", "follows", "likes", "list_items", "posts"], "{rec:?}");
    let get = |t: &str| rec.iter().find(|r| r.1 == STRANGER && r.0 == t).cloned().expect(t);
    assert_eq!(get("likes").4.as_deref(), Some(B), "a like names its post");
    let reply_row = get("posts");
    assert_eq!((reply_row.4.as_deref(), reply_row.6.as_deref()), (Some(B), Some(D)), "a reply that quotes names both");
    assert_eq!((get("follows").4.as_deref(), get("follows").5), (Some(D), None), "a follow names its account");
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_rows WHERE tbl = 16 AND rev IS NOT NULL").await, 1, "a profile keeps its rev");
    // A failed op (its savepoint rolled back) and a refused older write
    // record nothing.
    let fails = crate::test_support::hooks::arm(crate::test_support::hooks::FOLLOW_WRITE_FAILS, &format!("at://{OTHER}/app.bsky.graph.follow/3kaaaaaaaab2b"));
    let r = w.apply(OTHER, &tid(5), &[follow("3kaaaaaaaab2b", D)], LIVE).await.expect("failed");
    assert!(matches!(r.outcomes[0], OpOutcome::Failed(_)), "{:?}", r.outcomes);
    drop(fails);
    w.apply(OTHER, &tid(9), &[delete("app.bsky.feed.post", "3kaaaaaaaab2c")], LIVE).await.expect("delete");
    let r = w.apply(OTHER, &tid(6), &[post("3kaaaaaaaab2c", "older")], LIVE).await.expect("older");
    assert_eq!(r.outcomes, [OpOutcome::Unchanged]);
    let other = tables_of(&record(p).await, OTHER).into_iter().map(str::to_string).collect::<Vec<_>>();
    assert_eq!(other, ["record_tombstones"], "only the tombstone of a delete the start mode would not tombstone");
    // Quiet: a stranger's post mentioning a member notifies nobody.
    let mention = op(
        "app.bsky.feed.post",
        "3kaaaaaaaab2d",
        json!({"$type": "app.bsky.feed.post", "text": "hi @m", "createdAt": created(),
               "facets": [{"index": {"byteStart": 3, "byteEnd": 5}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": MEMBER}]}]}),
    );
    assert_eq!(w.apply(LATE, &tid(7), &[mention], LIVE).await.expect("mention").outcomes, [OpOutcome::Written]);
    assert_eq!(count(p, "SELECT count(*) FROM notifications").await, 0, "a trial row notifies nobody");
    // Counted: the full arm's decisions and the start mode's own writes.
    pi.flush_trial_counters(false).await.expect("flush");
    let c = &transition(p).await["trial"]["counters"];
    assert!(c["full_decisions"].as_u64().unwrap_or(0) >= 6, "{c}");
    assert_eq!(c["start_writes"]["posts"], 2, "{c}");
    assert_eq!(c["recorded"]["likes"], 1, "{c}");
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// The bounds.
// ---------------------------------------------------------------------

/// Each of time, database growth and free space ends a trial; a bound
/// reached during a manual end ends it once; with the checker stopped the
/// sampler's path ends it and Health warns the checker is stale; a checker
/// that panics is spawned again.
#[tokio::test]
async fn each_bound_ends_a_trial_on_either_path() {
    let Some(db) = fresh_db("trial_bounds").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, _w) = stack(p, ScopeValue::Community, None).await;
    let run = async |pi: &PublicIndexing, b: Bounds| {
        pi.hooks.checker_stopped.store(true, Ordering::SeqCst);
        pi.start_trial(b, "cli").await.expect("start");
    };
    let back = async |scope: &ScopeState| {
        settled(scope).await;
        assert_eq!(scope.public_breadth(), Breadth::Community);
    };
    // Time, through the sampler's path.
    run(&pi, bounds()).await;
    assert_eq!(pi.check_trial_bounds("sampler").await.expect("check"), None);
    shift(&scope, Some(now() - TimeDelta::seconds(1)), None).await;
    assert_eq!(pi.check_trial_bounds("sampler").await.expect("check"), Some("time"));
    assert_eq!(pi.check_trial_bounds("checker").await.expect("again"), None, "the second path is a no-op");
    back(&scope).await;
    assert_eq!(sqlx::query_scalar::<_, String>("SELECT ended_by FROM full_trials ORDER BY id DESC LIMIT 1").fetch_one(p).await.expect("cause"), "time");
    // Growth: a limit of 0 bytes has passed at once.
    run(&pi, Bounds { max_added_gb: 0.0, ..bounds() }).await;
    assert_eq!(pi.check_trial_bounds("checker").await.expect("check"), Some("growth"));
    back(&scope).await;
    // Free space: 5 % free under a 10 % floor.
    run(&pi, bounds()).await;
    *pi.hooks.volume.lock().expect("hooks") = Some((5, 100));
    assert_eq!(pi.check_trial_bounds("checker").await.expect("check"), Some("free_space"));
    *pi.hooks.volume.lock().expect("hooks") = Some((90, 100));
    back(&scope).await;
    // A manual end first: the bound that passes after it does nothing.
    run(&pi, bounds()).await;
    assert!(pi.end_trial("manual").await.expect("end").is_some());
    shift(&scope, Some(now() - TimeDelta::seconds(1)), None).await;
    assert_eq!(pi.check_trial_bounds("sampler").await.expect("check"), None);
    assert!(pi.end_trial("manual").await.expect("again").is_none());
    back(&scope).await;
    // A stopped checker: Health warns once it is 5 minutes stale.
    run(&pi, bounds()).await;
    shift(&scope, None, Some(now() - TimeDelta::minutes(10))).await;
    let status = pi.status().await.expect("status");
    let warnings = status["trial"]["warnings"].as_array().cloned().unwrap_or_default();
    assert!(warnings.iter().any(|w| w.as_str().is_some_and(|w| w.contains("bounds checker"))), "{status}");
    // A checker that panics is started again, and stamps.
    pi.hooks.checker_stopped.store(false, Ordering::SeqCst);
    pi.hooks.checker_panics.store(true, Ordering::SeqCst);
    drop(pi.spawn_trial_bounds());
    eventually("the restarted checker's stamp", async || {
        scope.read_guard().await.trial.as_ref().is_some_and(|t| t.counters.get("checked_at").is_some_and(Value::is_string))
    })
    .await;
    assert!(!pi.hooks.checker_panics.load(Ordering::SeqCst), "it panicked once");
    assert!(pi.hooks.spawned().contains(&"trial_bounds_again"), "and was spawned again: {:?}", pi.hooks.spawned());
    let status = pi.status().await.expect("status");
    assert_eq!(status["trial"]["warnings"], json!([]), "{status}");
    pi.end_trial("manual").await.expect("end");
    back(&scope).await;
    db.drop_db().await;
}

/// A time limit that passed while Prism was down ends the
/// trial at startup, before any writer; a restart while running starts the
/// checker; a restart mid-exit keeps the trial's list; a measuring row left
/// by a restart is abandoned, and so is one whose mode changed during the
/// baseline; no "not applied" warning while a trial runs.
#[tokio::test]
async fn restarts_resume_or_end_a_trial() {
    let Some(db) = fresh_db("trial_restarts").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, _w) = stack(p, ScopeValue::Community, None).await;
    pi.hooks.checker_stopped.store(true, Ordering::SeqCst);
    pi.start_trial(bounds(), "cli").await.expect("start");
    // A restart while running: the checker starts; the file's value is
    // noted as waiting for the trial, not as not applied.
    exec(p, "UPDATE operator_settings SET file_value = NULL").await;
    let (pi, scope2) = boot(p, ScopeValue::Community).await;
    drop(scope);
    assert!(pi.hooks.spawned().contains(&"trial_bounds"), "{:?}", pi.hooks.spawned());
    assert_eq!(pi.status().await.expect("status")["file_note"], "trial_running");
    pi.hooks.checker_stopped.store(true, Ordering::SeqCst);
    // The time limit passes while Prism is down.
    exec(p, &format!("UPDATE worker_state SET value = jsonb_set(value, '{{trial,ends_at}}', '\"{}\"') WHERE key = 'public_transition'", rfc3339(&(now() - TimeDelta::minutes(1))))).await;
    let scope3 = ScopeState::load(p, &DisabledPrivateTier, ScopePolicy::MembersAndOneHop, 10_000).await.expect("scope");
    let pi3 = PublicIndexing::new(p.clone(), scope3.clone(), settings());
    *pi3.hooks.volume.lock().expect("hooks") = Some((90, 100));
    let pause = pi3.hooks.arm("trial_check");
    let task = pi3.startup(&file(ScopeValue::Community)).await.expect("startup");
    let Some(Spawn::Sweep { sweep, .. }) = task else {
        panic!("the exit's sweep, got {task:?}");
    };
    assert_eq!(sweep.list, SweepList::TrialNarrowing);
    assert_eq!(scope3.read_guard().await.trial.as_ref().map(|t| (t.state, t.ended_by.clone())), Some((TrialState::Ending, Some("startup".to_string()))));
    // Narrowing: S reads local.
    assert!(scope3.read_guard().await.own_repo_local(B) && scope3.public_reads_local(), "a trial's narrowing reads S locally");
    drop(scope2);
    drop(pi3.spawn(Spawn::Sweep { generation: scope3.read_guard().await.public.generation, sweep }));
    pause.0.notified().await;
    // A restart while checking resumes the check.
    let (pi4, scope4) = boot(p, ScopeValue::Community).await;
    assert!(pi4.hooks.spawned().contains(&"trial_check"), "{:?}", pi4.hooks.spawned());
    settled(&scope4).await;
    assert_eq!(last_trial(p).await.0, "passed");
    // The stopped start's check finds nothing left to check.
    pause.1.notify_one();
    // A measuring row left by a restart is abandoned.
    exec(p, "INSERT INTO full_trials (\"from\", \"to\", state) VALUES ('community', 'community', 'measuring')").await;
    let (pi5, _scope5) = boot(p, ScopeValue::Community).await;
    assert_eq!(last_trial(p).await.0, "abandoned");
    // A switch during the baseline abandons the trial.
    let (reached, resume) = pi5.hooks.arm("trial_measuring");
    let starting = {
        let pi5 = pi5.clone();
        tokio::spawn(async move { pi5.start_trial(bounds(), "cli").await })
    };
    reached.notified().await;
    let applied = pi5.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("switch");
    assert_eq!(applied.action, "draining");
    resume.notify_one();
    let refused = starting.await.expect("join").expect_err("abandoned");
    assert!(refused.0.contains("abandoned"), "{refused}");
    assert_eq!(last_trial(p).await.0, "abandoned");
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// The exit from community and the check.
// ---------------------------------------------------------------------

/// The exit from community: every recorded row is gone; the start mode's
/// tombstone of a stranger's removed like of a member's post stays; trends
/// of an S post come back; a kept identity row whose profile the trial wrote
/// twice loses it; a stranger's reply to a member's post keeps its row and
/// loses its feed item; a like of an S post moves its count and back; a
/// count changed by hand is repaired; the narrowing ends without a fill
/// and `last_fill_since` back, so a member's backfilled reply from the day
/// before notifies; full-only tables are empty; the result names what it
/// should.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn the_exit_from_community_removes_what_the_trial_added() {
    let Some(db) = fresh_db("trial_exit").await else {
        return;
    };
    let p = &db.pool;
    let before = now() - TimeDelta::days(3);
    let (pi, scope, w) = stack(p, ScopeValue::Community, Some(before)).await;
    let s_post = post_uri(B, "3kaaaaaaaab22");
    let m_post = post_uri(MEMBER, "3kaaaaaaaab23");
    // Community's own: B's tagged post, the member's post, a stranger's like
    // of the member's post that was removed (its tombstone), and an
    // identity row of a stranger.
    w.apply(B, &tid(1), &[tagged("3kaaaaaaaab22"), tagged("3kaaaaaaaab2a"), follow("3kaaaaaaaab2b", LATE)], LIVE).await.expect("s post");
    assert_eq!(rows_of(p, "actor_counts", LATE).await, 1, "community counts an account S follows");
    w.apply(MEMBER, &tid(2), &[post("3kaaaaaaaab23", "m")], LIVE).await.expect("m post");
    w.apply(STRANGER, &tid(3), &[like("3kaaaaaaaab24", &m_post)], LIVE).await.expect("like");
    w.apply(STRANGER, &tid(4), &[delete("app.bsky.feed.like", "3kaaaaaaaab24")], LIVE).await.expect("unlike");
    assert_eq!(rows_of(p, "record_tombstones", STRANGER).await, 1);
    exec(p, &crate::test_support::actors::put_sql(OTHER, &[("indexed_at", "now() - interval '1 hour'"), ("pds_endpoint", "'https://pds.example'")])).await;
    let trends = "SELECT COALESCE(sum(likes), 0)::bigint FROM tag_activity_hourly WHERE tag = 'trialtag'";
    let likes_before = count(p, trends).await;
    let actors_before = count(p, "SELECT count(*) FROM actors").await;
    pi.start_trial(bounds(), "cli").await.expect("start");
    // The trial: a stranger's like of B's post (count and trend move), a
    // stranger's reply to the member's post that quotes B's post, the
    // identity row's profile written twice, and a stranger's posts.
    let d_post = post_uri(D, "3kaaaaaaaab27");
    w.apply(
        D,
        &tid(5),
        &[
            like("3kaaaaaaaab25", &s_post),
            reply("3kaaaaaaaab26", &m_post, Some(&s_post)),
            post("3kaaaaaaaab27", "d"),
            reply("3kaaaaaaaab29", &s_post, Some(&d_post)),
            like("3kaaaaaaaab2c", &post_uri(B, "3kaaaaaaaab2a")),
            follow("3kaaaaaaaab2d", LATE),
            follow("3kaaaaaaaab2f", OTHER),
            post("3kaaaaaaaab2e", "deleted during the exit"),
        ],
        LIVE,
    )
    .await
    .expect("d");
    assert_eq!(count(p, trends).await, likes_before + 2, "the trial's likes count toward the tag");
    let like_count = "SELECT like_count::bigint FROM post_counts pc WHERE (pc.author, pc.rkey) = (SELECT r.author, r.rkey FROM ref_of('at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.feed.post/3kaaaaaaaab22') r)";
    assert_eq!(count(p, like_count).await, 1);
    w.apply(OTHER, &tid(6), &[profile("first")], LIVE).await.expect("profile");
    w.apply(OTHER, &tid(7), &[profile("second")], LIVE).await.expect("profile again");
    let profile_rev = "SELECT count(*) FROM actors a JOIN dids d ON d.id = a.id WHERE d.did = 'did:plc:ffffffffffffffffffffffff' AND a.profile_rev IS NOT NULL";
    assert_eq!(count(p, profile_rev).await, 1);
    let rec = record(p).await;
    let mut d_tables = tables_of(&rec, D);
    d_tables.sort_unstable();
    assert_eq!(
        d_tables,
        ["feed_items", "feed_items", "feed_items", "feed_items", "follows", "follows", "likes", "likes", "posts", "posts", "posts"],
        "the reply's row is community's; its feed item is not: {rec:?}"
    );
    let quoting = rec.iter().find(|r| r.1 == D && r.0 == "posts" && r.6.is_some()).expect("the reply that quotes");
    assert_eq!((quoting.4.as_deref(), quoting.6.as_deref()), (Some(B), Some(D)));
    // A count changed by hand during the trial.
    exec(p, "UPDATE post_counts SET quote_count = 7 WHERE (author, rkey) = (SELECT r.author, r.rkey FROM ref_of('at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.feed.post/3kaaaaaaaab22') r)").await;
    let pause = pi.hooks.arm("trial_check");
    let exit = pi.apply_scope(ScopeValue::Community, "cli").await.expect("end");
    // While the exit runs, deleting a trial row leaves no tombstone.
    let tombs = rows_of(p, "record_tombstones", D).await;
    w.apply(D, &tid(9), &[delete("app.bsky.feed.post", "3kaaaaaaaab2e")], LIVE).await.expect("delete during the exit");
    assert_eq!(rows_of(p, "record_tombstones", D).await, tombs, "no tombstone for a trial row deleted during the exit");
    drop(pi.spawn(exit.spawn.expect("the exit's sweep")));
    pause.0.notified().await;
    // The exit is done; the check waits.
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::On, Breadth::Community));
    assert_eq!(transition(p).await["last_fill_since"], json!(rfc3339(&before)), "last_fill_since is back");
    assert_eq!(rows_of(p, "likes", D).await, 0);
    assert_eq!(rows_of(p, "posts", D).await, 1, "the reply to a member's post stays");
    assert_eq!(rows_of(p, "feed_items", D).await, 0, "and loses its feed item");
    assert_eq!(rows_of(p, "record_tombstones", STRANGER).await, 1, "the start mode's tombstone stays");
    assert_eq!(count(p, trends).await, likes_before, "trends are back");
    assert_eq!(count(p, like_count).await, 0, "the like count is back");
    assert_eq!(count(p, profile_rev).await, 0, "the trial's profile is cleared");
    assert_eq!(count(p, "SELECT count(*) FROM actors").await, actors_before, "the identity row stays");
    assert_eq!(rows_of(p, "actor_counts", LATE).await, 1, "community's counts of an account S follows stay");
    assert_eq!(rows_of(p, "actor_counts", OTHER).await, 0, "a kept identity row's counts the trial brought back to zero go");
    assert_eq!(count(p, "SELECT count(*) FROM post_counts WHERE like_count + repost_count + reply_count + quote_count = 0").await, 0, "no count row left at zero");
    assert_eq!(count(p, "SELECT count(*) FROM tag_activity_hourly WHERE posts + likes + reposts + replies <= 0").await, 0, "no trend hour left at zero");
    for table in super::trial::FULL_ONLY {
        assert_eq!(count(p, &format!("SELECT count(*) FROM {table}")).await, 0, "{table}");
    }
    assert_eq!(count(p, "SELECT count(*) FROM worker_state WHERE key IN ('full_groups', 'full_since')").await, 0);
    assert!(!pi.hooks.spawned().contains(&"fill"), "no refill");
    pause.1.notify_one();
    settled(&scope).await;
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_rows").await, 0, "the record is dropped");
    assert!(result["counts_checked"]["posts"].as_u64().unwrap_or(0) >= 2, "both the parent and the quote: {result}");
    assert!(result["counts_checked"]["repaired"].as_u64().unwrap_or(0) >= 1, "the hand-changed count: {result}");
    let words = result["words"].as_str().unwrap_or_default();
    for part in ["Full trial from community", "Removed", "community's own writes during the trial", "Accounts first seen during the trial", "Database:", "reclaim step", "Missed by the exit: none", "Passed."] {
        assert!(words.contains(part), "{part}: {words}");
    }
    assert!(words.contains("posts:"), "the start mode's own writes per table: {words}");
    // After the trial: a member's backfilled reply created the day before
    // the trial notifies the member it replies to.
    members(p, &[B]).await;
    {
        let mut sets = scope.write_guard().await;
        sets.members.insert(B.to_string());
    }
    let old_reply = op(
        "app.bsky.feed.post",
        "3kaaaaaaaab28",
        json!({"$type": "app.bsky.feed.post", "text": "late", "createdAt": rfc3339(&(now() - TimeDelta::days(1))),
               "reply": {"root": {"uri": m_post, "cid": "bafyc"}, "parent": {"uri": m_post, "cid": "bafyc"}}}),
    );
    w.apply(B, &tid(8), &[old_reply], ApplyOptions { source: WriteSource::Backfill, live: false, ..LIVE }).await.expect("backfill");
    assert_eq!(
        count(p, "SELECT count(*) FROM notifications WHERE reason = 'reply' AND record_uri LIKE 'at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/%'").await,
        1,
        "a backfilled reply from before the trial notifies"
    );
    drop(pi);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// What the check finds.
// ---------------------------------------------------------------------

/// A row written with the record off is still removed by the exit's
/// predicates, and the sanity line fails the check; the failed trial keeps
/// its record until dismissed.
#[tokio::test]
async fn with_the_record_off_the_check_fails() {
    let Some(db) = fresh_db("trial_record_off").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, w) = stack(p, ScopeValue::Community, None).await;
    pi.start_trial(bounds(), "cli").await.expect("start");
    let name: String = sqlx::query_scalar("SELECT current_database()::text").fetch_one(p).await.expect("db");
    let off = crate::test_support::hooks::arm(crate::test_support::hooks::TRIAL_RECORD_OFF, &format!("{name}/*"));
    w.apply(STRANGER, &tid(1), &[post("3kaaaaaaaab22", "unrecorded")], LIVE).await.expect("post");
    drop(off);
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_rows").await, 0);
    pi.end_trial("manual").await.expect("end");
    settled(&scope).await;
    assert_eq!(rows_of(p, "posts", STRANGER).await, 0, "the exit's predicates removed it anyway");
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "failed", "{result}");
    assert!(result["causes"].to_string().contains("not being written"), "{result}");
    let status = pi.status().await.expect("status");
    assert!(status["trial"]["warnings"].to_string().contains("check failed"), "{status}");
    assert!(pi.start_trial(bounds(), "cli").await.is_err(), "a failed trial is dismissed first");
    pi.dismiss_trial().await.expect("dismiss");
    assert_eq!(last_trial(p).await.0, "dismissed");
    assert!(transition(p).await.get("trial").is_none());
    db.drop_db().await;
}

/// A row the exit leaves on purpose is found, listed and removed by the
/// check, and named on Health; a recorded row of an R author pending removal
/// is kept unexplained, removed and counted apart; rows of authors who
/// joined S during the trial, or between the baseline and the switch, are
/// kept with that reason; a removed trial account's `actor_counts` row is
/// gone.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn the_check_removes_what_the_exit_left() {
    let Some(db) = fresh_db("trial_left").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, w) = stack(p, ScopeValue::Community, None).await;
    // OTHER joins S between the baseline and the switch.
    let (reached, resume) = pi.hooks.arm("trial_measuring");
    let starting = {
        let pi = pi.clone();
        tokio::spawn(async move { pi.start_trial(bounds(), "cli").await })
    };
    reached.notified().await;
    in_scope(p, &[OTHER]).await;
    resume.notify_one();
    starting.await.expect("join").expect("start");
    assert!(!scope.read_guard().await.in_scope(OTHER), "memory has not seen it");
    // During the trial: rows of OTHER, of LATE (joins S during the trial),
    // of an R author pending removal, and of a stranger who follows.
    w.apply(OTHER, &tid(1), &[post("3kaaaaaaaab22", "o")], LIVE).await.expect("other");
    w.apply(LATE, &tid(2), &[post("3kaaaaaaaab23", "l")], LIVE).await.expect("late");
    w.apply(D, &tid(3), &[post("3kaaaaaaaab24", "r")], LIVE).await.expect("pending");
    w.apply(STRANGER, &tid(4), &[follow("3kaaaaaaaab25", D)], LIVE).await.expect("follow");
    assert_eq!(rows_of(p, "actor_counts", STRANGER).await, 1);
    in_scope(p, &[LATE]).await;
    exec(
        p,
        &format!(
            "INSERT INTO refract_dids (did, community, state, since, source) VALUES ('{D}', 'community-a', 'restored', now() - interval '1 day', 'admin'); \
             INSERT INTO refract_pending (scope) VALUES ('entry:ab'); INSERT INTO refract_pending_dids (scope, did) VALUES ('entry:ab', '{D}');"
        ),
    )
    .await;
    // A block the trial recorded whose commit clock was behind the trial's
    // start: the exit takes it by the record.
    exec(
        p,
        &format!(
            "INSERT INTO dids (did) VALUES ('{STRANGER}'), ('{OTHER}') ON CONFLICT DO NOTHING; \
             INSERT INTO blocks (author, rkey, subject, rev, created_at) \
               SELECT a.id, 78, o.id, rev_to_bigint('2kaaaaaaaaaa2'), now() FROM dids a, dids o WHERE a.did = '{STRANGER}' AND o.did = '{OTHER}'; \
             INSERT INTO full_trial_rows (tbl, a, b, c) SELECT 5, id, 78, 0 FROM dids WHERE did = '{STRANGER}';"
        ),
    )
    .await;
    // A list block the exit never saw (written after it, before the
    // check), put in the record by hand.
    let (reached, resume) = pi.hooks.arm("trial_check");
    let ending = {
        let pi = pi.clone();
        tokio::spawn(async move { pi.end_trial("manual").await })
    };
    reached.notified().await;
    exec(
        p,
        &format!(
            "INSERT INTO dids (did) VALUES ('{STRANGER}') ON CONFLICT DO NOTHING; \
             INSERT INTO list_blocks (author, rkey, cid, rev, subj_author, subj_coll, subj_rkey, created_at) \
               SELECT id, 77, '\\x00'::bytea, rev_to_bigint('{}'), NULL, 0, NULL, now() FROM dids WHERE did = '{STRANGER}'; \
             INSERT INTO full_trial_rows (tbl, a, b, c) SELECT 8, id, 77, 0 FROM dids WHERE did = '{STRANGER}';",
            tid(5)
        ),
    )
    .await;
    resume.notify_one();
    ending.await.expect("join").expect("end");
    settled(&scope).await;
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "faults are removed, then the check passes: {result}");
    let counts = &result["counts"];
    assert_eq!(counts["missed"]["list_blocks"], 1, "{result}");
    assert!(counts["missed"].get("blocks").is_none(), "the exit took the recorded block: {result}");
    assert_eq!(rows_of(p, "blocks", STRANGER).await, 0);
    assert_eq!(counts["unexplained"]["posts"], 1, "the R author pending removal: {result}");
    assert_eq!(counts["kept_author"], 4, "LATE's and OTHER's posts and feed items: {result}");
    assert!(result["words"].as_str().unwrap_or_default().contains("list_blocks 1"), "{result}");
    assert_eq!(rows_of(p, "list_blocks", STRANGER).await, 0, "removed by the check");
    assert_eq!(rows_of(p, "posts", D).await, 0, "the unexplained row is removed");
    assert_eq!(rows_of(p, "posts", OTHER).await + rows_of(p, "posts", LATE).await, 2, "kept for their authors");
    assert_eq!(rows_of(p, "actor_counts", STRANGER).await, 0, "the removed account's counts");
    assert_eq!(result["warning"], true);
    // Health names it until the operator dismisses it.
    let status = pi.status().await.expect("status");
    assert!(status["trial"]["warnings"].to_string().contains("had to remove"), "{status}");
    pi.dismiss_trial().await.expect("dismiss the warning");
    assert_eq!(pi.status().await.expect("status")["trial"]["warnings"], json!([]));
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// Switches during a trial.
// ---------------------------------------------------------------------

/// A switch to the other mode ends the trial into it and the result says
/// so, also from kryphocron_only into community during its fill; `keep`
/// keeps full and drops the record; a phase change during the check
/// supersedes it; a trial object made invalid by hand is Held with its
/// record kept, and dismissed from there.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn switches_during_a_trial() {
    let Some(db) = fresh_db("trial_switches").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, w) = stack(p, ScopeValue::Community, None).await;
    // Community → kryphocron_only.
    pi.start_trial(bounds(), "cli").await.expect("start");
    w.apply(STRANGER, &tid(1), &[post("3kaaaaaaaab22", "x")], LIVE).await.expect("post");
    let applied = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("switch");
    assert_eq!(applied.action, "draining");
    assert_eq!(transition(p).await["trial"]["to"], "kryphocron_only");
    drop(pi.spawn(applied.spawn.expect("drain")));
    settled(&scope).await;
    assert_eq!(scope.public_mode(), PublicMode::Off);
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    assert!(result["words"].as_str().unwrap_or_default().contains("did not return"), "{result}");
    // Kryphocron_only → community, during its fill.
    let fill = pi.hooks.arm("fill_write");
    let started = pi.start_trial(bounds(), "cli").await.expect("start off");
    assert_eq!(started.action, "filling");
    fill.0.notified().await;
    let switch = {
        let pi = pi.clone();
        tokio::spawn(async move { pi.apply_scope(ScopeValue::Community, "cli").await })
    };
    fill.1.notify_one();
    let applied = switch.await.expect("join").expect("switch");
    assert_eq!(applied.action, "narrowing");
    assert_eq!(transition(p).await["s_filled"], false);
    drop(pi.spawn(applied.spawn.expect("narrowing")));
    settled(&scope).await;
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::On, Breadth::Community));
    let (state, result) = last_trial(p).await;
    assert_eq!(state, "passed", "{result}");
    assert!(result["words"].as_str().unwrap_or_default().contains("ended in community"), "{result}");
    // Keep: full for real.
    pi.start_trial(bounds(), "cli").await.expect("start");
    w.apply(STRANGER, &tid(2), &[post("3kaaaaaaaab23", "kept")], LIVE).await.expect("post");
    assert!(count(p, "SELECT count(*) FROM full_trial_rows").await > 0);
    assert_eq!(pi.keep_trial("cli").await.expect("keep")["action"], "kept");
    assert_eq!((scope.public_breadth(), scope.read_guard().await.trial.is_none()), (Breadth::Full, true));
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_rows").await, 0);
    assert!(transition(p).await.get("trial").is_none());
    assert_eq!(last_trial(p).await.0, "kept");
    // A phase change during the check supersedes it.
    let applied = pi.apply_scope(ScopeValue::Community, "cli").await.expect("leave full");
    drop(pi.spawn(applied.spawn.expect("narrowing")));
    eventually("community", async || scope.public_mode() == PublicMode::On && scope.public_breadth() == Breadth::Community).await;
    pi.start_trial(bounds(), "cli").await.expect("start");
    w.apply(STRANGER, &tid(3), &[post("3kaaaaaaaab24", "superseded")], LIVE).await.expect("post");
    let pause = pi.hooks.arm("trial_check");
    pi.end_trial("manual").await.expect("end");
    pause.0.notified().await;
    let applied = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("switch while checking");
    assert_eq!(applied.action, "draining");
    pause.1.notify_one();
    assert!(scope.read_guard().await.trial.is_none());
    assert_eq!(last_trial(p).await.0, "superseded");
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_rows").await, 0);
    drop(pi.spawn(applied.spawn.expect("drain")));
    eventually("off", async || scope.public_mode() == PublicMode::Off).await;
    // A trial object made invalid by hand: Held, record kept, dismissed.
    let _fill = pi.hooks.arm("fill_write");
    pi.start_trial(bounds(), "cli").await.expect("start");
    w.apply(STRANGER, &tid(4), &[post("3kaaaaaaaab25", "held")], LIVE).await.expect("post");
    exec(p, "UPDATE worker_state SET value = jsonb_set(value, '{trial,state}', '\"sideways\"') WHERE key = 'public_transition'").await;
    let (pi, scope) = boot(p, ScopeValue::Community).await;
    assert_eq!(scope.public_mode(), PublicMode::Held);
    assert!(count(p, "SELECT count(*) FROM full_trial_rows").await > 0, "the record is kept");
    pi.dismiss_trial().await.expect("dismiss from held");
    assert_eq!(count(p, "SELECT count(*) FROM full_trial_rows").await, 0);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// The coverage table.
// ---------------------------------------------------------------------

/// Every table of the migrated schema is in one class, and a table in no
/// class is caught.
#[tokio::test]
async fn every_table_is_classified() {
    let Some(db) = fresh_db("trial_coverage").await else {
        return;
    };
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND n.nspname = current_schema()",
    )
    .fetch_all(&db.pool)
    .await
    .expect("tables");
    assert!(tables.len() > 80, "{tables:?}");
    let missing: Vec<&String> = tables.iter().filter(|t| class_of(t).is_none()).collect();
    assert!(missing.is_empty(), "tables in no class: {missing:?}");
    for t in super::trial::TrialTable::ALL {
        assert!(tables.iter().any(|x| x == t.name()), "{}", t.name());
        assert_eq!(class_of(t.name()), Some(Class::Recorded));
    }
    assert_eq!(class_of("dids"), Some(Class::Kept));
    assert_eq!(class_of("full_crawl"), Some(Class::FullOnly));
    assert_eq!(class_of("a_new_table"), None, "an unclassified table fails");
    db.drop_db().await;
}

/// A trial whose live ingest would be trimmed within its margin ends with
/// cause `lag`, from a projection that has its rates; one with
/// `end_on_lag` off runs on; the startup path never judges lag.
#[tokio::test]
async fn a_trial_ends_itself_when_live_ingest_falls_too_far_behind() {
    let Some(db) = fresh_db("trial_lag_bound").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope, _w) = stack(p, ScopeValue::Community, None).await;
    let behind = crate::firehose::projection::Projection {
        at: std::time::Instant::now(),
        input_per_sec: 1000.0,
        processed_per_sec: 400.0,
        lag: 900_000,
        pending: 0,
        length: 1_000_000,
        time_to_trim: Some(100_000.0 / 600.0),
    };
    // Off: runs on.
    pi.hooks.checker_stopped.store(true, Ordering::SeqCst);
    pi.start_trial(Bounds { end_on_lag: false, ..bounds() }, "cli").await.expect("start");
    *pi.hooks.projection.lock().expect("hooks") = Some(behind);
    assert_eq!(pi.check_trial_bounds("checker").await.expect("check"), None);
    assert!(pi.end_trial("manual").await.expect("end").is_some());
    settled(&scope).await;
    // On: the startup path does not judge it; the checker ends it.
    pi.start_trial(bounds(), "cli").await.expect("start");
    *pi.hooks.projection.lock().expect("hooks") = Some(crate::firehose::projection::Projection { time_to_trim: None, ..behind });
    assert_eq!(pi.check_trial_bounds("checker").await.expect("check"), None, "keeping up ends nothing");
    *pi.hooks.projection.lock().expect("hooks") = Some(behind);
    assert_eq!(pi.check_trial_bounds("checker").await.expect("check"), Some("lag"));
    settled(&scope).await;
    assert_eq!(sqlx::query_scalar::<_, String>("SELECT ended_by FROM full_trials ORDER BY id DESC LIMIT 1").fetch_one(p).await.expect("cause"), "lag");
    db.drop_db().await;
}

/// The capacity words: the first five minutes left out, a saturated run's
/// median, a run that kept up with its ceiling estimate, the fill's minutes
/// apart, and the largest wait class named.
#[test]
fn a_trials_capacity_is_said_in_words() {
    use crate::indexing::trial::{Sample, capacity_words};
    let s = |input: f64, processed: f64, applied: f64, busy: f64, filling: bool| Sample { input: Some(input), processed, deduped: input * 0.6, applied, lanes_busy: Some(busy), filling, counts_behind: None };
    assert_eq!(capacity_words(&[s(1.0, 1.0, 1.0, 0.1, false); 5], false, None), None, "five minutes say nothing");
    let mut kept: Vec<Sample> = vec![s(1300.0, 1300.0, 500.0, 0.9, false); 5];
    kept.extend(vec![s(1300.0, 1300.0, 430.0, 0.41, false); 20]);
    let w = capacity_words(&kept, false, None).expect("words");
    assert!(w.contains("the relays sent 1300 entries a second; 780 were copies") && w.contains("applied 430 a second with its writer lanes 41 % busy: it kept up"), "{w}");
    assert!(w.contains("ceiling near 1049 a second (an estimate)"), "{w}");
    let mut behind: Vec<Sample> = vec![s(1300.0, 300.0, 300.0, 1.0, true); 15];
    behind.extend(vec![s(1300.0, 400.0, 410.0, 1.0, false); 10]);
    let w = capacity_words(&behind, true, Some(crate::db::waits::WaitClass::DiskRead)).expect("words");
    assert!(w.contains("While community's fill ran, this server applied 300 a second") && w.contains("This server applied 410 a second"), "{w}");
    assert!(w.contains("could not keep up") && w.contains("ended itself") && w.contains("waiting for disk reads"), "{w}");
    assert!(!w.contains("Counts fell"), "the folder kept up: {w}");
    // The counts folder held back for most minutes: it, not the lanes, was
    // the limit.
    let mut held: Vec<Sample> = vec![s(1300.0, 1300.0, 500.0, 0.4, false); 5];
    held.extend((0..20).map(|i| Sample { counts_behind: Some(if i < 15 { 180.0 } else { 2.0 }), ..s(1300.0, 1300.0, 500.0, 0.4, false) }));
    let w = capacity_words(&held, false, None).expect("words");
    assert!(w.contains("Counts fell up to 3 minutes behind ingest: the counts folder, not the lanes, was the limit at this rate."), "{w}");
}
