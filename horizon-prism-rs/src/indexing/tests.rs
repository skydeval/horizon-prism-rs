//! The public-indexing switch against Postgres (public-indexing design
//! §12.1: T-9 to T-14, T-20, T-21).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::public_fill::FillBatch;
use super::public_mode::{
    Breadth, FillPart, FillStage, Phase, PublicIndexing, PublicMode, PublicModeSettings, PublicState, ScopeValue, Spawn, SweepList, SweepState,
    Transition, Validated, map, now, parse_ts, rfc3339, validate,
};
use super::public_sweep::{Batch, Cursor, Key, TABLES};
use crate::aggregations::ScopeState;
use crate::config::{IndexingConfig, PublicScope, ScopePolicy};
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};
use crate::private_tier::DisabledPrivateTier;
use crate::test_support::fresh_db;

/// A stranger, a member and another stranger, in key order.
const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const MEMBER: &str = "did:plc:cccccccccccccccccccccccc";
const D: &str = "did:plc:dddddddddddddddddddddddd";

fn settings() -> PublicModeSettings {
    PublicModeSettings {
        check_every: Duration::from_millis(50),
        fill_batch: 2,
        sweep_window: Some(3),
        retry_base: Duration::from_millis(10),
        failure_pause: Duration::from_millis(50),
        ..PublicModeSettings::from_config(&IndexingConfig::default())
    }
}

fn file(scope: Option<PublicScope>) -> IndexingConfig {
    IndexingConfig { public_scope: scope, ..IndexingConfig::default() }
}

/// The scope state as `serve` loads it, and the switch over it: a
/// (re)start.
async fn boot(pool: &PgPool, settings: PublicModeSettings) -> (PublicIndexing, ScopeState) {
    let scope = ScopeState::load(pool, &DisabledPrivateTier, ScopePolicy::MembersAndOneHop, 10_000).await.expect("scope");
    (PublicIndexing::new(pool.clone(), scope.clone(), settings), scope)
}

async fn exec(pool: &PgPool, sql: &str) {
    sqlx::raw_sql(sql).execute(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn transition(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'public_transition'")
        .fetch_one(pool)
        .await
        .expect("transition")
}

async fn setting(pool: &PgPool) -> (Value, Option<Value>, String) {
    sqlx::query_as("SELECT value, file_value, updated_by FROM operator_settings WHERE key = 'indexing.public_scope'")
        .fetch_one(pool)
        .await
        .expect("setting")
}

/// Seed the row and the transition as an earlier run would have.
async fn seed(pool: &PgPool, value: &str, file_value: Option<Value>, t: &Value) {
    sqlx::query("INSERT INTO operator_settings (key, value, file_value, updated_by) VALUES ('indexing.public_scope', $1, $2, 'cli')")
        .bind(json!(value))
        .bind(file_value)
        .execute(pool)
        .await
        .expect("setting");
    sqlx::query("INSERT INTO worker_state (key, value) VALUES ('public_transition', $1)")
        .bind(t)
        .execute(pool)
        .await
        .expect("transition");
}

async fn set_memory(pi: &PublicIndexing, state: PublicState) {
    let mut sets = pi.scope.write_guard().await;
    pi.set_state(&mut sets, state);
}

async fn eventually(what: &str, mut f: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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

fn feed_sql(n: usize) -> String {
    (1..=n).map(|i| crate::test_support::posts::feed_item_sql(B, &crate::test_support::posts::tid(i))).collect()
}

fn op(collection: &str, rkey: &str, record: Value) -> RawOp {
    RawOp { action: Action::Create, collection: collection.into(), rkey: rkey.into(), cid: Some("bafyop".into()), record: Some(record) }
}

fn like(rkey: &str) -> RawOp {
    op(
        "app.bsky.feed.like",
        rkey,
        json!({"$type": "app.bsky.feed.like", "subject": {"uri": format!("at://{MEMBER}/app.bsky.feed.post/1"), "cid": "c"}, "createdAt": "2026-10-14T00:00:00Z"}),
    )
}

const LIVE: ApplyOptions = ApplyOptions {
    advance_applied_rev: false,
    dead_letter_readiness: false,
    attempts: 1,
    live: true,
    subscription_rules: true,
    source: WriteSource::Firehose,
};

// ---------------------------------------------------------------------
// T-14: the startup sequence.
// ---------------------------------------------------------------------

/// S1: an upgrade with public data keeps it on, records the file and
/// applies nothing.
#[tokio::test]
async fn s1_an_upgrade_stays_on() {
    let Some(db) = fresh_db("public_s1").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &crate::test_support::posts::row_sql(B, "1")).await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::KryphocronOnly))).await.expect("startup"), None);
    let (value, file_value, by) = setting(p).await;
    assert_eq!((value, file_value, by.as_str()), (json!("community"), Some(json!({"value": "kryphocron_only", "explicit": true})), "upgrade"));
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["last_fill_since"]), (&json!("on"), &json!(1), &Value::Null));
    assert_eq!(scope.public_mode(), PublicMode::On);
    assert_eq!(scope.snapshot().await.public.generation, 1);
    assert!(pi.hooks.spawned().is_empty());
    db.drop_db().await;
}

/// S2: an empty database is off with no key, and on with an explicit
/// `community`; neither spawns anything.
#[tokio::test]
async fn s2_a_fresh_install_takes_the_file() {
    let Some(db) = fresh_db("public_s2").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(None)).await.expect("startup"), None);
    let (value, file_value, by) = setting(p).await;
    assert_eq!((value, file_value, by.as_str()), (json!("kryphocron_only"), Some(json!({"value": "kryphocron_only", "explicit": false})), "startup"));
    assert_eq!((&transition(p).await["phase"], &transition(p).await["generation"]), (&json!("off"), &json!(1)));
    assert_eq!(scope.public_mode(), PublicMode::Off);

    exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Community))).await.expect("startup"), None);
    assert_eq!(setting(p).await.0, json!("community"));
    assert_eq!(transition(p).await["phase"], "on");
    assert_eq!(scope.public_mode(), PublicMode::On);
    db.drop_db().await;
}

/// S3, S4 and S6 record (or keep) the file value and apply nothing.
#[tokio::test]
async fn s3_s4_s6_apply_nothing() {
    let Some(db) = fresh_db("public_s346").await else {
        return;
    };
    let p = &db.pool;
    let on = Transition::settled(Phase::On, 5, now(), None).to_json();
    // S3: first sight.
    seed(p, "community", None, &on).await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::KryphocronOnly))).await.expect("startup"), None);
    assert_eq!(setting(p).await, (json!("community"), Some(json!({"value": "kryphocron_only", "explicit": true})), "cli".to_string()));
    assert_eq!((scope.public_mode(), transition(p).await["generation"].clone()), (PublicMode::On, json!(5)));
    // S4: a CLI-set value stands while the file is unchanged.
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::KryphocronOnly))).await.expect("startup"), None);
    assert_eq!(setting(p).await.0, json!("community"));
    assert_eq!(scope.public_mode(), PublicMode::On);
    // S6: the explicit key becomes a comment.
    exec(p, "UPDATE operator_settings SET file_value = '{\"value\": \"community\", \"explicit\": true}'").await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(None)).await.expect("startup"), None);
    assert_eq!(setting(p).await, (json!("community"), Some(json!({"value": "kryphocron_only", "explicit": false})), "cli".to_string()));
    assert_eq!(scope.public_mode(), PublicMode::On);
    assert_eq!(transition(p).await["generation"], 5);
    assert!(pi.hooks.spawned().is_empty());
    db.drop_db().await;
}

/// S5 from Off: Filling with generation + 1; exactly one fill pass, then
/// exactly one checker; the order of the startup steps.
#[tokio::test]
async fn s5_from_off_spawns_one_fill_then_one_checker() {
    let Some(db) = fresh_db("public_s5_off").await else {
        return;
    };
    let p = &db.pool;
    seed(p, "kryphocron_only", Some(json!({"value": "kryphocron_only", "explicit": false})), &Transition::settled(Phase::Off, 2, now(), None).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::Community))).await.expect("startup");
    assert_eq!(*pi.hooks.steps.lock().expect("hooks"), ["record", "validate", "map", "memory", "s5"]);
    let Some(task @ Spawn::Fill { generation: 3, .. }) = task else {
        panic!("expected the fill pass at generation 3, got {task:?}");
    };
    assert_eq!(setting(p).await, (json!("community"), Some(json!({"value": "community", "explicit": true})), "file".to_string()));
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"]), (&json!("filling"), &json!(3)));
    assert_eq!(t["last_fill_since"], t["fill"]["since"]);
    drop(pi.spawn(task));
    eventually("on", async || scope.public_mode() == PublicMode::On).await;
    assert_eq!(pi.hooks.spawned(), ["fill", "checker"]);
    assert_eq!(transition(p).await["generation"], 4);
    db.drop_db().await;
}

/// S5 from Draining sets `pending`: one sweep, and no fill until it ends.
#[tokio::test]
async fn s5_from_draining_sets_pending() {
    let Some(db) = fresh_db("public_s5_draining").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &feed_sql(2)).await;
    seed(p, "kryphocron_only", Some(json!({"value": "kryphocron_only", "explicit": false})), &Transition::draining(4, now(), None).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::Community))).await.expect("startup");
    assert!(matches!(task, Some(Spawn::Sweep { generation: 4, .. })), "{task:?}");
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["pending"]), (&json!("draining"), &json!(4), &json!("community")));
    assert_eq!(setting(p).await.0, json!("kryphocron_only"));
    drop(pi.spawn(task.expect("task")));
    eventually("on", async || scope.public_mode() == PublicMode::On).await;
    assert_eq!(pi.hooks.spawned(), ["sweep", "fill", "checker"]);
    assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 0);
    assert_eq!(transition(p).await["generation"], 6);
    db.drop_db().await;
}

/// S5 from Held: an explicit `off` gives Draining with one sweep, and the
/// new generation is 1 when the stored one did not parse.
#[tokio::test]
async fn s5_from_held_drains() {
    let Some(db) = fresh_db("public_s5_held").await else {
        return;
    };
    let p = &db.pool;
    let held = json!({"phase": "draining", "generation": "x", "since": null, "pending": null, "last_fill_since": null, "sweep": null, "fill": null});
    seed(p, "kryphocron_only", Some(json!({"value": "community", "explicit": true})), &held).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::KryphocronOnly))).await.expect("startup");
    assert!(matches!(task, Some(Spawn::Sweep { generation: 1, .. })), "{task:?}");
    assert_eq!(scope.public_mode(), PublicMode::Draining);
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"]), (&json!("draining"), &json!(1)));
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// T-13: restarts, validation and Held.
// ---------------------------------------------------------------------

/// Bad sweep states reset to the first table, during validation, and the
/// sweep then completes.
#[tokio::test]
async fn bad_sweep_states_reset_and_complete() {
    let Some(db) = fresh_db("public_sweep_reset").await else {
        return;
    };
    let p = &db.pool;
    let at = rfc3339(&now());
    for bad in [
        json!({"table": "no_such_table", "after": null, "last_advanced_at": at}),
        json!({"table": "notifications", "after": [7], "last_advanced_at": at}),
        json!({"table": "notifications", "after": "7", "last_advanced_at": at}),
        json!({"table": "tag_activity_hourly", "after": ["rust"], "last_advanced_at": at}),
    ] {
        exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
        exec(p, &feed_sql(2)).await;
        let mut t = Transition::draining(7, now(), None).to_json();
        t["sweep"] = bad.clone();
        seed(p, "kryphocron_only", Some(json!({"value": "kryphocron_only", "explicit": false})), &t).await;
        let (pi, scope) = boot(p, settings()).await;
        let task = pi.startup(&file(None)).await.expect("startup");
        let Some(Spawn::Sweep { generation: 7, sweep }) = &task else {
            panic!("{bad}: expected a sweep, got {task:?}");
        };
        assert_eq!((sweep.table.as_str(), &sweep.after), ("feed_items", &Value::Null), "{bad}");
        assert_eq!(transition(p).await["sweep"]["table"], "feed_items", "{bad}: reset in the database");
        assert_eq!(*pi.hooks.steps.lock().expect("hooks"), ["record", "validate", "map", "memory"]);
        drop(pi.spawn(task.expect("task")));
        eventually("off", async || scope.public_mode() == PublicMode::Off).await;
        assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 0, "{bad}");
    }
    db.drop_db().await;
}

/// States that map to Held, and Held's health error.
#[tokio::test]
async fn invalid_states_map_to_held() {
    let Some(db) = fresh_db("public_held").await else {
        return;
    };
    let p = &db.pool;
    let at = now();
    let fill = |f: Value| {
        let mut t = Transition::filling(3, at).to_json();
        for (k, v) in f.as_object().expect("object") {
            t["fill"][k] = v.clone();
        }
        t
    };
    let mut yesterday = Transition::settled(Phase::Off, 4, at, None).to_json();
    yesterday["last_fill_since"] = json!("yesterday");
    let mut x = Transition::settled(Phase::On, 4, at, None).to_json();
    x["generation"] = json!("x");
    let mut pending = Transition::filling(3, at).to_json();
    pending["pending"] = json!("community");
    let earlier = rfc3339(&(at - TimeDelta::seconds(1)));
    for (value, t) in [
        ("community", x),
        ("kryphocron_only", yesterday),
        ("community", fill(json!({"waiting_since": rfc3339(&at)}))),
        ("community", fill(json!({"stage": "waiting", "part": "members", "waiting_since": rfc3339(&at)}))),
        ("community", fill(json!({"enqueued": -1}))),
        ("community", fill(json!({"stage": "waiting", "part": "scope", "waiting_since": earlier}))),
        ("community", pending),
        // Value and phase disagree.
        ("kryphocron_only", Transition::settled(Phase::On, 2, at, None).to_json()),
    ] {
        exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
        seed(p, value, Some(json!({"value": "kryphocron_only", "explicit": false})), &t).await;
        let (pi, scope) = boot(p, settings()).await;
        assert_eq!(pi.startup(&file(None)).await.expect("startup"), None, "{t}");
        assert_eq!(scope.public_mode(), PublicMode::Held, "{t}");
        assert_eq!(pi.status().await.expect("status")["error"], "held", "{t}");
    }
    db.drop_db().await;
}

/// Leaving Held rewrites the generation and `last_fill_since` (§3.4), and
/// the next restart is not Held; in Held, the off rule applies.
#[tokio::test]
async fn leaving_held() {
    let Some(db) = fresh_db("public_leave_held").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    in_scope(p, &[MEMBER, B]).await;
    let fv = Some(json!({"value": "kryphocron_only", "explicit": false}));
    // `last_fill_since = "yesterday"`: off writes null and stored + 1.
    let mut t = Transition::settled(Phase::Off, 4, now(), None).to_json();
    t["last_fill_since"] = json!("yesterday");
    seed(p, "kryphocron_only", fv.clone(), &t).await;
    let (pi, scope) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    assert_eq!(scope.public_mode(), PublicMode::Held);

    // In Held: a block of a member and a member's follow are stored; a
    // non-member's like is rejected.
    let writer = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    let block = op("app.bsky.graph.block", "3kblock", json!({"subject": MEMBER, "createdAt": "2026-10-14T00:00:00Z"}));
    let r = writer.apply(B, "3kaaaaaaaaaa2", &[block, like("3klike")], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Written, OpOutcome::Rejected("public_off")]);
    let follow = op("app.bsky.graph.follow", "3kfollow", json!({"subject": D, "createdAt": "2026-10-14T00:00:00Z"}));
    let r = writer.apply(MEMBER, "3kaaaaaaaaaa2", &[follow], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Written]);
    assert!(pi.hooks.spawned().is_empty(), "no sweep or fill runs in Held");

    let applied = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("apply");
    assert_eq!((applied.action, applied.mode, applied.generation), ("draining", PublicMode::Draining, 5));
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["last_fill_since"]), (&json!("draining"), &json!(5), &Value::Null));
    let (pi, scope) = boot(p, settings()).await;
    assert!(matches!(pi.startup(&file(None)).await.expect("startup"), Some(Spawn::Sweep { generation: 5, .. })));
    assert_eq!(scope.public_mode(), PublicMode::Draining);

    // A Held state whose `last_fill_since` parses keeps it.
    let kept = now() - TimeDelta::days(3);
    let mut t = Transition::filling(8, now()).to_json();
    t["pending"] = json!("community");
    t["last_fill_since"] = json!(rfc3339(&kept));
    exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
    seed(p, "community", fv.clone(), &t).await;
    let (pi, scope) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    assert_eq!(scope.public_mode(), PublicMode::Held);
    pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("apply");
    let t = transition(p).await;
    assert_eq!((&t["generation"], parse_ts(&t["last_fill_since"])), (&json!(9), Some(kept)));
    db.drop_db().await;
}

/// Restarts in Draining (after a batch, and with `pending`) and in Filling
/// (stage `enqueue` in `members`, and stage `waiting`) resume with the
/// stored generation and cursor.
#[tokio::test]
async fn restarts_resume_where_they_stopped() {
    let Some(db) = fresh_db("public_restarts").await else {
        return;
    };
    let p = &db.pool;
    let fv = Some(json!({"value": "kryphocron_only", "explicit": false}));
    exec(p, &feed_sql(5)).await;
    seed(p, "community", fv.clone(), &Transition::settled(Phase::On, 1, now(), None).to_json()).await;
    let (pi, _) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    let Some(Spawn::Sweep { generation: 2, .. }) = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn else {
        panic!("expected a sweep");
    };
    // Draining, after one batch.
    let Batch::Advanced { deleted: 3, last } = pi.sweep_batch(2, 0, None, 3).await.expect("batch") else {
        panic!("expected three rows deleted");
    };
    assert_eq!(pi.apply_scope(ScopeValue::Community, "cli").await.expect("pending").action, "pending_set");
    // Kill, and restart.
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(None)).await.expect("startup");
    let Some(Spawn::Sweep { generation: 2, sweep }) = &task else {
        panic!("expected the sweep, got {task:?}");
    };
    assert_eq!((sweep.table.as_str(), &sweep.after), ("feed_items", &last.to_json()));
    assert_eq!(transition(p).await["pending"], "community", "pending survives");
    drop(pi.spawn(task.expect("task")));
    eventually("filling", async || scope.public_mode() != PublicMode::Draining).await;
    assert_eq!(*pi.hooks.windows.lock().expect("hooks").first().expect("a window"), ("feed_items", 3));
    assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 0);
    // Filling, stage `waiting`: the checker resumes with the stored
    // `waiting_since` (and a job keeps it waiting).
    eventually("on", async || scope.public_mode() == PublicMode::On).await;
    members(p, &[MEMBER]).await;
    exec(p, &format!("INSERT INTO backfill_jobs (did, depth) VALUES ('{MEMBER}', 'resync')")).await;
    let ws = now() - TimeDelta::seconds(30);
    let mut t = Transition::filling(9, now() - TimeDelta::seconds(60));
    if let Some(f) = t.fill.as_mut() {
        f.stage = FillStage::Waiting;
        f.part = FillPart::Scope;
        f.waiting_since = Some(ws);
    }
    exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
    seed(p, "community", fv.clone(), &t.to_json()).await;
    let (pi, _) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(None)).await.expect("startup"), Some(Spawn::Checker { generation: 9, waiting_since: ws }));
    // Filling, stage `enqueue` in `members`.
    let mut t = Transition::filling(10, now());
    if let Some(f) = t.fill.as_mut() {
        f.after = MEMBER.to_string();
        f.enqueued = 1;
    }
    exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
    seed(p, "community", fv, &t.to_json()).await;
    let (pi, _) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(None)).await.expect("startup"), Some(Spawn::Fill { generation: 10, fill: t.fill.expect("fill") }));
    db.drop_db().await;
}

/// T-13's scope case: 3 members and 5 other S DIDs, two below the last
/// member, batches of 2, killed after the first `scope` batch.
#[tokio::test]
async fn a_fill_killed_in_scope_resumes_exactly() {
    let Some(db) = fresh_db("public_fill_restart").await else {
        return;
    };
    let p = &db.pool;
    let ms = ["did:plc:m1", "did:plc:m3", "did:plc:m5"];
    let others = ["did:plc:a1", "did:plc:a2", "did:plc:x1", "did:plc:x2", "did:plc:x3"];
    members(p, &ms).await;
    in_scope(p, &ms).await;
    in_scope(p, &others).await;
    seed(p, "kryphocron_only", Some(json!({"value": "kryphocron_only", "explicit": false})), &Transition::settled(Phase::Off, 1, now(), None).to_json()).await;
    let (pi, _) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    let Some(Spawn::Fill { generation: 2, mut fill }) = pi.apply_scope(ScopeValue::Community, "cli").await.expect("on").spawn else {
        panic!("expected the fill");
    };
    let since = fill.since;
    loop {
        let FillBatch::Continue(next) = pi.fill_batch(2, &fill).await.expect("batch") else {
            panic!("the pass ended early");
        };
        let scope_batch = fill.part == FillPart::Scope;
        fill = next;
        if scope_batch {
            break;
        }
    }
    assert_eq!((fill.part, fill.after.as_str(), fill.enqueued), (FillPart::Scope, "did:plc:a2", 5));
    // Kill, and restart.
    let (pi, _) = boot(p, settings()).await;
    let task = pi.startup(&file(None)).await.expect("startup");
    assert_eq!(task, Some(Spawn::Fill { generation: 2, fill: fill.clone() }));
    pi.clone().run_fill(2, fill).await;
    let jobs: Vec<(String, DateTime<Utc>)> =
        sqlx::query_as("SELECT did, not_before FROM backfill_jobs WHERE depth = 'resync' ORDER BY not_before").fetch_all(p).await.expect("jobs");
    let mut dids: Vec<&str> = jobs.iter().map(|(d, _)| d.as_str()).collect();
    dids.sort_unstable();
    let mut all: Vec<&str> = ms.iter().chain(others.iter()).copied().collect();
    all.sort_unstable();
    assert_eq!(dids, all, "every S DID has exactly one resync");
    let offsets: Vec<i64> = jobs.iter().map(|(_, nb)| (*nb - since).num_seconds()).collect();
    assert_eq!(offsets, (0..8).collect::<Vec<i64>>());
    let t = transition(p).await;
    assert_eq!((&t["fill"]["stage"], &t["fill"]["enqueued"]), (&json!("waiting"), &json!(8)));
    let ws = parse_ts(&t["fill"]["waiting_since"]).expect("waiting_since");
    assert!(ws >= since + TimeDelta::seconds(8), "{ws} vs {since}");
    assert_eq!(pi.hooks.spawned(), ["checker"]);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// T-10, T-11, T-12: the sweep.
// ---------------------------------------------------------------------

/// Every swept table and every kept one (§4, §7.2), with the members'
/// likes laid out so one window holds only kept rows.
/// Statements that write one follows or blocks row as ingest stores it:
/// the DIDs interned, an alias for a key that is not a TID, then the row.
fn graph_row_sql(table: &str, author: &str, subject: &str, rkey: &str) -> String {
    format!(
        "INSERT INTO dids (did) VALUES ('{author}'), ('{subject}') ON CONFLICT (did) DO NOTHING; \
         INSERT INTO rkey_aliases (author, rkey) SELECT id, '{rkey}' FROM dids WHERE did = '{author}' AND rkey_to_bigint('{rkey}') IS NULL \
           ON CONFLICT (author, rkey) DO NOTHING; \
         INSERT INTO {table} (author, rkey, subject, rev, created_at) \
           SELECT a.id, COALESCE(rkey_to_bigint('{rkey}'), (SELECT x.id FROM rkey_aliases x WHERE x.author = a.id AND x.rkey = '{rkey}')), \
                  s.id, rev_to_bigint('3k'), now() \
           FROM dids a, dids s WHERE a.did = '{author}' AND s.did = '{subject}';"
    )
}

fn sweep_seed() -> String {
    let post = |did: &str, r: &str| format!("at://{did}/app.bsky.feed.post/{r}");
    let mut sql = feed_sql(4);
    for i in 1..=4 {
        sql += &format!("INSERT INTO notifications (recipient_did, author_did, reason, record_uri, record_cid) VALUES ('{MEMBER}', '{B}', 'like', 'at://{B}/app.bsky.feed.like/n{i}', 'c');");
    }
    // Likes: B's (deleted), then five of the member's, then D's.
    for (did, rkeys) in [(B, &["1"][..]), (MEMBER, &["1", "2", "3", "4", "5"][..]), (D, &["1"][..])] {
        for r in rkeys {
            for t in ["likes", "reposts"] {
                sql += &crate::test_support::interactions::row_sql(t, did, r, &post(B, "p1"), "now()");
                sql += ";";
            }
        }
    }
    for (did, r) in [(B, "p1"), (B, "p2"), (MEMBER, "p3")] {
        sql += &crate::test_support::posts::row_sql(did, r);
    }
    use crate::test_support::records::row_sql;
    for t in ["threadgates", "postgates"] {
        sql += &row_sql(t, B, "p1", &[("post", &post(B, "p1"))], &[]);
    }
    let b_list = format!("at://{B}/app.bsky.graph.list/l");
    sql += &row_sql("starter_packs", B, "1", &[("list", &b_list)], &[]);
    sql += &row_sql("labeler_services", B, "self", &[], &[]);
    sql += &row_sql("nearhorizon_poll_posts", B, "1", &[], &[("visibility", "'public'".to_string())]);
    sql += &crate::test_support::posts::intern_sql(D, "1");
    for did in [B, MEMBER] {
        let list = format!("at://{did}/app.bsky.graph.list/l");
        sql += &row_sql("lists", did, "l", &[], &[("purpose", "'app.bsky.graph.defs#modlist'".to_string())]);
        sql += &row_sql("list_items", did, "1", &[], &[
            ("list_rkey", crate::test_support::posts::key_sql(did, "l")),
            ("subject", format!("(SELECT id FROM dids WHERE did = '{D}')")),
        ]);
        for t in ["list_optouts", "list_blocks"] {
            sql += &row_sql(t, did, "1", &[("subj", &list)], &[]);
        }
    }
    // Follows: B → D goes; B → member and member → D stay.
    for (author, subject, r) in [(B, D, "1"), (B, MEMBER, "2"), (MEMBER, D, "1")] {
        sql += &graph_row_sql("follows", author, subject, r);
    }
    sql += &crate::test_support::interactions::post_counts_sql(&post(D, "p9"), 1, 0, 0, 0);
    sql += ";";
    for (tag, h) in [("a", 1), ("a", 2), ("b", 1), ("c", 3), ("c", 4)] {
        sql += &format!("INSERT INTO tag_activity_hourly (tag, hour, posts) VALUES ('{tag}', '2026-10-14T0{h}:00:00Z', 1);");
    }
    for i in 1..=2 {
        sql += &crate::test_support::posts::intern_sql(B, &i.to_string());
        sql += &format!(
            "INSERT INTO starter_pack_counts (author, rkey, all_time_source, refreshed_at) SELECT id, {}, 'x', now() FROM dids WHERE did = '{B}';",
            crate::test_support::posts::key_sql(B, &i.to_string())
        );
    }
    // Kept tables.
    sql += &graph_row_sql("blocks", B, MEMBER, "1");
    sql += &crate::test_support::actors::put_sql(B, &[("display_name", "'b'")]);
    sql += ";";
    sql += &crate::test_support::actors::put_sql(MEMBER, &[("display_name", "'m'")]);
    sql += ";";
    sql += &format!("INSERT INTO labels (subject, val) VALUES ('{B}', 'porn');");
    sql += &crate::test_support::posts::tombstone_sql(&post(B, "gone"), "3kaaaaaaaaaaa");
    sql += &format!(
        "INSERT INTO member_post_refs (author, rkey, rev, created_at) SELECT id, {}, rev_to_bigint('3kaaaaaaaaaaa'), now() FROM dids WHERE did = '{MEMBER}';",
        crate::test_support::posts::key_sql(MEMBER, "p3")
    );
    sql += &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{MEMBER}', '{B}');");
    sql
}

/// The rows the sweep keeps, as text, table by table.
const KEPT: [(&str, &str); 14] = [
    ("likes", "author = (SELECT id FROM dids WHERE did = 'did:plc:cccccccccccccccccccccccc')"),
    ("reposts", "author = (SELECT id FROM dids WHERE did = 'did:plc:cccccccccccccccccccccccc')"),
    ("lists", "author = (SELECT id FROM dids WHERE did = 'did:plc:cccccccccccccccccccccccc')"),
    ("list_items", "author = (SELECT id FROM dids WHERE did = 'did:plc:cccccccccccccccccccccccc')"),
    ("list_optouts", "author = (SELECT id FROM dids WHERE did = 'did:plc:cccccccccccccccccccccccc')"),
    ("list_blocks", "author = (SELECT id FROM dids WHERE did = 'did:plc:cccccccccccccccccccccccc')"),
    ("follows", "(SELECT id FROM dids WHERE did = 'did:plc:cccccccccccccccccccccccc') IN (author, subject)"),
    ("blocks", "TRUE"),
    ("actors", "TRUE"),
    ("labels", "TRUE"),
    ("record_tombstones", "TRUE"),
    ("member_post_refs", "TRUE"),
    ("mutes", "TRUE"),
    ("members", "TRUE"),
];

async fn kept_rows(pool: &PgPool) -> Vec<String> {
    let mut out = Vec::new();
    for (table, filter) in KEPT {
        let rows: String = sqlx::query_scalar(&format!(
            "SELECT coalesce(string_agg(t::text, '|' ORDER BY t::text), '') FROM {table} t WHERE {filter}"
        ))
        .fetch_one(pool)
        .await
        .expect(table);
        out.push(format!("{table}: {rows}"));
    }
    out
}

/// T-10: batch by batch, every table's first batch deletes and stores its
/// cursor by key type; a window of kept rows advances without ending the
/// table; emptied tables end empty, kept rows are unchanged, and the end
/// is Off with generation + 1 and `last_fill_since` carried.
#[tokio::test]
async fn the_sweep_empties_and_thins_every_table() {
    let Some(db) = fresh_db("public_sweep").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    in_scope(p, &[MEMBER, B, D]).await;
    exec(p, &sweep_seed()).await;
    for t in TABLES {
        assert!(count(p, &format!("SELECT count(*) FROM {}", t.name)).await > 0, "{} is seeded", t.name);
    }
    let kept = kept_rows(p).await;
    let (pi, scope) = boot(p, settings()).await;
    let filled = now() - TimeDelta::days(2);
    set_memory(&pi, PublicState { mode: PublicMode::On, generation: 1, last_fill_since: Some(filled), ..PublicState::default() }).await;
    let Some(Spawn::Sweep { generation: 2, .. }) = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn else {
        panic!("expected the sweep");
    };
    let (mut table, mut after, mut first) = (0, None::<Cursor>, true);
    let mut likes_windows = 0;
    loop {
        let name = TABLES[table].name;
        match pi.sweep_batch(2, table, after.as_ref(), 3).await.expect("batch") {
            Batch::Advanced { deleted, last } => {
                let stored = transition(p).await["sweep"]["after"].clone();
                if first {
                    assert!(deleted >= 1, "{name}: the first batch deletes");
                    let ok = match TABLES[table].key {
                        Key::Text(_) => stored.is_string(),
                        Key::Id => stored.is_i64(),
                        Key::TagHour => stored.as_array().is_some_and(|a| a.len() == 2 && a[0].is_string() && parse_ts(&a[1]).is_some()),
                        Key::AuthorRkey => stored.as_array().is_some_and(|a| a.len() == 2 && a[0].is_i64() && a[1].is_i64()),
                        Key::FeedItem => stored.as_array().is_some_and(|a| a.len() == 3 && a[0].is_i64() && a[1].is_boolean() && a[2].is_i64()),
                        Key::Author => stored.as_array().is_some_and(|a| a.len() == 1 && a[0].is_i64()),
                    };
                    assert!(ok, "{name}: cursor {stored}");
                }
                if name == "likes" {
                    likes_windows += 1;
                    if likes_windows == 2 {
                        assert_eq!(deleted, 0, "the second likes window holds only the member's likes");
                    }
                }
                assert_eq!(stored, last.to_json());
                first = false;
                after = Some(last);
            }
            Batch::TableDone => {
                assert_eq!(transition(p).await["sweep"]["table"], TABLES[table + 1].name);
                table += 1;
                after = None;
                first = true;
            }
            Batch::SweepDone => break,
            other => panic!("{name}: {other:?}"),
        }
    }
    assert_eq!(likes_windows, 3, "a kept window did not end the table");
    pi.end_sweep(2).await.expect("end");
    for t in ["feed_items", "notifications", "posts", "threadgates", "postgates", "starter_packs", "labeler_services", "nearhorizon_poll_posts", "post_counts", "tag_activity_hourly", "starter_pack_counts"] {
        assert_eq!(count(p, &format!("SELECT count(*) FROM {t}")).await, 0, "{t} is emptied");
    }
    for t in ["likes", "reposts"] {
        assert_eq!(count(p, &format!("SELECT count(*) FROM {t} WHERE author <> (SELECT id FROM dids WHERE did = '{MEMBER}')")).await, 0, "{t} is thinned");
    }
    for t in ["lists", "list_items", "list_optouts", "list_blocks"] {
        assert_eq!(count(p, &format!("SELECT count(*) FROM {t} WHERE author <> (SELECT id FROM dids WHERE did = '{MEMBER}')")).await, 0, "{t} is thinned");
    }
    assert_eq!(count(p, "SELECT count(*) FROM follows").await, 2);
    assert_eq!(kept_rows(p).await, kept, "kept rows are unchanged");
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], parse_ts(&t["last_fill_since"])), (&json!("off"), &json!(3), Some(filled)));
    assert_eq!(scope.public_mode(), PublicMode::Off);
    db.drop_db().await;
}

/// T-11: rows deleted by others between the window and the delete; the
/// table ends only on an empty window.
#[tokio::test]
async fn a_concurrent_delete_never_ends_a_table() {
    let Some(db) = fresh_db("public_sweep_concurrent").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &feed_sql(7)).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn.expect("sweep");
    let (reached, resume) = pi.hooks.arm("sweep_window");
    drop(pi.spawn(task));
    reached.notified().await;
    exec(
        p,
        &format!(
            "DELETE FROM feed_items WHERE originator = (SELECT id FROM dids WHERE did = '{B}') AND src_rkey IN ({}, {})",
            crate::test_support::posts::key_sql(B, &crate::test_support::posts::tid(1)),
            crate::test_support::posts::key_sql(B, &crate::test_support::posts::tid(2))
        ),
    )
    .await;
    resume.notify_one();
    eventually("off", async || scope.public_mode() == PublicMode::Off).await;
    assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 0);
    let feed: Vec<u64> = pi.hooks.windows.lock().expect("hooks").iter().filter(|(t, _)| *t == "feed_items").map(|(_, n)| *n).collect();
    assert_eq!(feed.len(), 4, "three windows of 3, 3 and 1 key, then the empty one: {feed:?}");
    db.drop_db().await;
}

/// T-11: 40P01, 55P03 and 57014 are retried; 57014 halves `n` to the
/// floor, where it keeps retrying; a success doubles it back.
#[tokio::test]
async fn retries_halve_to_the_floor_and_double_back() {
    let Some(db) = fresh_db("public_sweep_retries").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &feed_sql(25)).await;
    let (pi, scope) = boot(p, PublicModeSettings { sweep_window: Some(40), ..settings() }).await;
    let task = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn.expect("sweep");
    pi.hooks.inject.lock().expect("hooks").extend(["40P01", "55P03", "57014", "57014", "57014"]);
    drop(pi.spawn(task));
    eventually("off", async || scope.public_mode() == PublicMode::Off).await;
    let feed: Vec<u64> = pi.hooks.windows.lock().expect("hooks").iter().filter(|(t, _)| *t == "feed_items").map(|(_, n)| *n).collect();
    // 40P01 and 55P03 at 40; 57014 at 40, 20 and 10 (the floor); then
    // 10 rows deleted, 15 deleted, and the empty window.
    assert_eq!(feed, [40, 40, 40, 20, 10, 10, 20, 40]);
    assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 0);
    db.drop_db().await;
}

/// T-11: with a test stall threshold the alarm is raised while batches
/// fail, and one successful batch clears it.
#[tokio::test]
async fn the_stall_alarm_rises_and_clears() {
    let Some(db) = fresh_db("public_sweep_stall").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &feed_sql(2)).await;
    let s = PublicModeSettings { stall_after: Duration::from_millis(100), retry_base: Duration::from_millis(80), ..settings() };
    let (pi, scope) = boot(p, s).await;
    let task = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn.expect("sweep");
    pi.hooks.inject.lock().expect("hooks").extend(["55P03"; 5]);
    drop(pi.spawn(task));
    eventually("the alarm", async || pi.status().await.expect("status")["error"] == "sweep_stalled").await;
    let status = pi.status().await.expect("status");
    assert_eq!(status["sweep"]["table"], "feed_items");
    assert!(status["sweep"]["last_error"].as_str().is_some_and(|e| e.contains("injected")), "{status}");
    eventually("an advance", async || !pi.progress.lock().expect("progress").sweep_stalled).await;
    eventually("off", async || scope.public_mode() == PublicMode::Off).await;
    assert_eq!(pi.status().await.expect("status")["error"], Value::Null);
    db.drop_db().await;
}

/// T-11: a hand-written phase change between the delete and the cursor
/// write rolls the batch back, and the task exits.
#[tokio::test]
async fn a_hand_written_phase_change_rolls_the_batch_back() {
    let Some(db) = fresh_db("public_sweep_superseded").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &feed_sql(2)).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn.expect("sweep");
    let (reached, resume) = pi.hooks.arm("sweep_cursor");
    let handle = pi.spawn(task);
    reached.notified().await;
    exec(p, "UPDATE worker_state SET value = jsonb_set(value, '{phase}', '\"off\"') WHERE key = 'public_transition'").await;
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), handle).await.expect("the task exits").expect("join");
    assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 2, "the delete rolled back");
    assert_eq!(scope.public_mode(), PublicMode::Draining);
    db.drop_db().await;
}

/// T-12: on while draining sets `pending` and nothing else; the sweep's
/// end moves to Filling with a fresh fill; an `off` before the end clears
/// `pending`, and the phase ends `off`.
#[tokio::test]
async fn on_while_draining_waits_for_the_sweep() {
    let Some(db) = fresh_db("public_pending").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &feed_sql(5)).await;
    let (pi, scope) = boot(p, settings()).await;
    let Some(Spawn::Sweep { generation: 1, sweep }) = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn else {
        panic!("expected the sweep");
    };
    assert!(matches!(pi.sweep_batch(1, 0, None, 3).await.expect("batch"), Batch::Advanced { deleted: 3, .. }));
    let before = transition(p).await;
    let applied = pi.apply_scope(ScopeValue::Community, "cli").await.expect("pending");
    assert_eq!((applied.action, applied.spawn), ("pending_set", None));
    let after = transition(p).await;
    assert_eq!(after["pending"], "community");
    assert_eq!((&after["sweep"], &after["generation"]), (&before["sweep"], &before["generation"]));
    assert_eq!(setting(p).await.0, json!("kryphocron_only"));
    // The sweep runs to its end, which spawns the fill.
    let resumed = SweepState { after: before["sweep"]["after"].clone(), ..sweep };
    pi.clone().run_sweep(1, resumed).await;
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["pending"]), (&json!("filling"), &json!(2), &Value::Null));
    assert_eq!((&t["fill"]["stage"], &t["fill"]["part"], &t["fill"]["after"], &t["fill"]["enqueued"]), (&json!("enqueue"), &json!("members"), &json!(""), &json!(0)));
    assert_eq!(t["last_fill_since"], t["fill"]["since"]);
    assert_eq!(setting(p).await.0, json!("community"));
    assert_eq!(pi.hooks.spawned().first(), Some(&"fill"), "the sweep's end spawned the fill");
    eventually("on", async || scope.public_mode() == PublicMode::On).await;
    assert_eq!(pi.hooks.spawned(), ["fill", "checker"]);

    // Off, on (pending), off (cleared): the phase ends off.
    let Some(Spawn::Sweep { generation, sweep }) = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").spawn else {
        panic!("expected the sweep");
    };
    assert_eq!(pi.apply_scope(ScopeValue::Community, "cli").await.expect("on").action, "pending_set");
    assert_eq!(pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").action, "pending_cleared");
    assert_eq!(pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off").action, "no_op");
    assert_eq!(transition(p).await["pending"], Value::Null);
    pi.clone().run_sweep(generation, sweep).await;
    assert_eq!(transition(p).await["phase"], "off");
    assert_eq!(scope.public_mode(), PublicMode::Off);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// T-9: switch atomicity.
// ---------------------------------------------------------------------

/// A writer holding the read guard delays the switch; its op, begun
/// before, is stored; an op after the switch is rejected with `public_off`.
#[tokio::test]
async fn the_switch_waits_for_writers() {
    let Some(db) = fresh_db("public_atomic").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    in_scope(p, &[MEMBER, B]).await;
    let (pi, scope) = boot(p, settings()).await;
    let writer = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    // Hold the writer inside its transaction: `likes` is locked.
    let mut lock = p.begin().await.expect("tx");
    sqlx::query("LOCK TABLE likes IN EXCLUSIVE MODE").execute(&mut *lock).await.expect("lock");
    let w = writer.clone();
    let before = tokio::spawn(async move { w.apply(B, "3kaaaaaaaaaa2", &[like("3kbefore")], LIVE).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let switcher = pi.clone();
    let switch = tokio::spawn(async move { switcher.apply_scope(ScopeValue::KryphocronOnly, "cli").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!switch.is_finished(), "the switch waits for the writer's read guard");
    assert_eq!(scope.public_mode(), PublicMode::On);
    lock.commit().await.expect("unlock");
    let r = before.await.expect("join").expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Written], "the op begun before the switch is stored");
    assert_eq!(switch.await.expect("join").expect("switch").mode, PublicMode::Draining);
    let r = writer.apply(B, "3kaaaaaaaaaa3", &[like("3kafter")], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("public_off")]);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// T-20: fill completion.
// ---------------------------------------------------------------------

/// Which jobs hold the fill back.
#[tokio::test]
async fn the_completion_query_waits_on_live_jobs_only() {
    let Some(db) = fresh_db("public_fill_query").await else {
        return;
    };
    let p = &db.pool;
    let dids = ["did:plc:j1", "did:plc:j2", "did:plc:j3", "did:plc:j4", "did:plc:j5", "did:plc:j6"];
    in_scope(p, &dids).await;
    exec(p, "INSERT INTO backfill_jobs (did, depth, state, attempts, last_error, updated_at) VALUES \
             ('did:plc:j1', 'resync', 'queued', 0, NULL, now()), \
             ('did:plc:j2', 'resync', 'queued', 2, 'boom', now()), \
             ('did:plc:j3', 'resync', 'queued', 0, NULL, now() - interval '30 days'), \
             ('did:plc:j4', 'resync', 'queued', 0, 'boom', now()), \
             ('did:plc:j5', 'resync', 'failed', 5, 'gone', now()), \
             ('did:plc:j6', 'resync', 'done', 0, NULL, now()), \
             ('did:plc:j1', 'scope', 'queued', 0, NULL, now()), \
             ('did:plc:gone', 'resync', 'queued', 0, NULL, now())").await;
    let (pi, _) = boot(p, settings()).await;
    assert_eq!(pi.fill_remaining().await.expect("remaining"), 3);
    db.drop_db().await;
}

/// Five DIDs due a second apart and one job that never finishes: the
/// bound fires no earlier than `fill_max_wait` after `waiting_since`, all
/// five jobs exist, and the counts are repaired before On.
#[tokio::test]
async fn the_bound_ends_a_fill_with_a_stuck_job() {
    let Some(db) = fresh_db("public_fill_bound").await else {
        return;
    };
    let p = &db.pool;
    let ms = ["did:plc:m1", "did:plc:m2", "did:plc:m3", "did:plc:m4", "did:plc:m5"];
    members(p, &ms).await;
    in_scope(p, &ms).await;
    crate::test_support::interactions::like(p, "did:plc:m1", "1", "at://did:plc:m2/app.bsky.feed.post/1").await;
    exec(p, "UPDATE post_counts SET like_count = 99").await;
    let s = PublicModeSettings { fill_max_wait: Duration::from_secs(2), fill_batch: 1_000, ..settings() };
    let (pi, scope) = boot(p, s).await;
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None, ..PublicState::default() }).await;
    let task = pi.apply_scope(ScopeValue::Community, "cli").await.expect("on").spawn.expect("fill");
    drop(pi.spawn(task));
    eventually("waiting", async || transition(p).await["fill"]["stage"] == "waiting").await;
    let ws = parse_ts(&transition(p).await["fill"]["waiting_since"]).expect("waiting_since");
    assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'resync'").await, 5);
    exec(p, "UPDATE backfill_jobs SET state = 'done' WHERE did <> 'did:plc:m5'").await;
    eventually("on", async || scope.public_mode() == PublicMode::On).await;
    let on_at = now();
    assert!(on_at >= ws + TimeDelta::seconds(2), "the bound fired early: {on_at} vs {ws}");
    assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'resync'").await, 5);
    assert_eq!(count(p, "SELECT like_count::bigint FROM post_counts").await, 1, "post_counts was repaired");
    assert_eq!(pi.progress.lock().expect("progress").fill_incomplete, Some(1));
    assert_eq!(pi.status().await.expect("status")["fill"]["incomplete"], 1);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// T-21: generations.
// ---------------------------------------------------------------------

/// An `off` issued while a fill batch holds the read guard commits after
/// the batch, and the next batch exits; a hand-written generation change
/// before the conditional write rolls the batch's enqueues back.
#[tokio::test]
async fn generations_stop_the_fill() {
    let Some(db) = fresh_db("public_generations").await else {
        return;
    };
    let p = &db.pool;
    let ms = ["did:plc:m1", "did:plc:m2", "did:plc:m3"];
    members(p, &ms).await;
    in_scope(p, &ms).await;
    let (pi, scope) = boot(p, settings()).await;
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None, ..PublicState::default() }).await;
    let task = pi.apply_scope(ScopeValue::Community, "cli").await.expect("on").spawn.expect("fill");
    let (reached, resume) = pi.hooks.arm("fill_write");
    let fill = pi.spawn(task);
    reached.notified().await;
    let switcher = pi.clone();
    let switch = tokio::spawn(async move { switcher.apply_scope(ScopeValue::KryphocronOnly, "cli").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!switch.is_finished(), "the command waits for the batch");
    resume.notify_one();
    assert_eq!(switch.await.expect("join").expect("off").generation, 3);
    tokio::time::timeout(Duration::from_secs(10), fill).await.expect("the pass exits").expect("join");
    assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'resync'").await, 2, "one batch committed, no more");
    assert_eq!(scope.public_mode(), PublicMode::Draining);

    // The database barrier.
    exec(p, "DELETE FROM backfill_jobs").await;
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 3, last_fill_since: None, ..PublicState::default() }).await;
    let task = pi.apply_scope(ScopeValue::Community, "cli").await.expect("on").spawn.expect("fill");
    let (reached, resume) = pi.hooks.arm("fill_write");
    let fill = pi.spawn(task);
    reached.notified().await;
    exec(p, "UPDATE worker_state SET value = jsonb_set(value, '{generation}', '99') WHERE key = 'public_transition'").await;
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), fill).await.expect("the pass exits").expect("join");
    assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs").await, 0, "the enqueues rolled back");
    db.drop_db().await;
}

/// ABA: a checker of g sleeps through Filling (g) → Draining (g + 1) →
/// Filling (g + 2), wakes, and exits without repairing; with the memory
/// check bypassed its write misses. A sweep of g exits at g + 1.
#[tokio::test]
async fn a_stale_checker_or_sweep_exits() {
    let Some(db) = fresh_db("public_aba").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    exec(p, &format!("INSERT INTO backfill_jobs (did, depth) VALUES ('{MEMBER}', 'resync')")).await;
    let (pi, scope) = boot(p, PublicModeSettings { check_every: Duration::from_millis(400), ..settings() }).await;
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None, ..PublicState::default() }).await;
    pi.apply_scope(ScopeValue::Community, "cli").await.expect("on");
    let checker = pi.spawn(Spawn::Checker { generation: 2, waiting_since: now() });
    tokio::time::sleep(Duration::from_millis(100)).await;
    pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("off");
    // The sweep's end with `pending`, by hand: Filling at g + 2.
    let t = Transition::filling(4, now());
    exec(p, &format!("UPDATE worker_state SET value = '{}' WHERE key = 'public_transition'", t.to_json())).await;
    set_memory(&pi, PublicState { mode: PublicMode::Filling, generation: 4, last_fill_since: t.last_fill_since, ..PublicState::default() }).await;
    exec(p, "UPDATE backfill_jobs SET state = 'done'").await;
    tokio::time::timeout(Duration::from_secs(10), checker).await.expect("the checker exits").expect("join");
    assert_eq!(pi.hooks.repairs.load(Ordering::SeqCst), 0);
    pi.finish_fill(2, false).await.expect("bypassed");
    assert_eq!(pi.hooks.repairs.load(Ordering::SeqCst), 1);
    let stored = transition(p).await;
    assert_eq!((&stored["phase"], &stored["generation"]), (&json!("filling"), &json!(4)), "the write missed");
    assert_eq!(scope.public_mode(), PublicMode::Filling);

    // A sweep of g exits once the generation is g + 1.
    exec(p, &feed_sql(2)).await;
    pi.clone().run_sweep(3, SweepState::first(SweepList::Today, now())).await;
    assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 2);
    db.drop_db().await;
}

// ---------------------------------------------------------------------
// Full indexing: breadth, transitions, startup and narrowing.
// ---------------------------------------------------------------------

/// A stranger: in no set.
const STRANGER: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";

async fn full_groups(pool: &PgPool) -> Option<Value> {
    sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'full_groups'").fetch_optional(pool).await.expect("full_groups")
}

/// On, Community → On, Full changes the breadth and nothing else: no
/// sweep, no fill, `last_fill_since` set, every group's history reset at
/// the crawl generation; a restart maps the stored state back to On, Full.
#[tokio::test]
async fn on_community_to_full_changes_breadth_only() {
    let Some(db) = fresh_db("full_breadth_only").await else {
        return;
    };
    let p = &db.pool;
    seed(p, "community", Some(json!({"value": "community", "explicit": true})), &Transition::settled(Phase::On, 5, now(), None).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Community))).await.expect("startup"), None);
    let applied = pi.apply_scope(ScopeValue::Full, "cli").await.expect("apply");
    assert_eq!((applied.action, applied.mode, applied.generation, applied.spawn), ("breadth", PublicMode::On, 6, None));
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::On, Breadth::Full));
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["breadth"], &t["crawl_generation"], &t["sweep"], &t["fill"]), (&json!("on"), &json!("full"), &json!(0), &Value::Null, &Value::Null));
    assert!(t["last_fill_since"].is_string(), "every way into full sets last_fill_since");
    assert_eq!(setting(p).await.0, json!("full"));
    let groups = full_groups(p).await.expect("groups reset");
    assert_eq!(groups["crawl_generation"], 0);
    assert_eq!(groups["groups"].as_object().map(serde_json::Map::len), Some(9));
    assert_eq!(groups["groups"]["likes"], json!({"generation": 1, "state": "crawling", "incomplete": false, "union_done": true}));
    // A second switch is a no-op; a restart keeps On, Full.
    assert_eq!(pi.apply_scope(ScopeValue::Full, "cli").await.expect("again").action, "no_op");
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Community))).await.expect("restart"), None);
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::On, Breadth::Full));
    assert!(pi.hooks.spawned().is_empty());
    db.drop_db().await;
}

/// A restart in Filling, Full resumes Filling, Full in both fill stages,
/// never Held; Filling, Community → full copies the fill state forward.
#[tokio::test]
async fn a_restart_in_filling_full_resumes_it_in_both_stages() {
    let Some(db) = fresh_db("full_filling_restart").await else {
        return;
    };
    let p = &db.pool;
    let at = now();
    let filling = Transition::filling(3, at);
    seed(p, "community", Some(json!({"value": "community", "explicit": true})), &filling.to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    assert!(matches!(pi.startup(&file(Some(PublicScope::Community))).await.expect("startup"), Some(Spawn::Fill { generation: 3, .. })));
    // Filling, Community → Filling, Full: the stored fill is carried.
    let applied = pi.apply_scope(ScopeValue::Full, "cli").await.expect("apply");
    let Some(Spawn::Fill { generation: 4, fill }) = applied.spawn else {
        panic!("expected the fill again at generation 4, got {:?}", applied.spawn);
    };
    assert_eq!(fill, filling.fill.clone().expect("fill"));
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::Filling, Breadth::Full));
    // Restart, stage enqueue.
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::Community))).await.expect("restart");
    assert!(matches!(task, Some(Spawn::Fill { generation: 4, .. })), "{task:?}");
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::Filling, Breadth::Full));
    // Restart, stage waiting.
    exec(
        p,
        &format!(
            "UPDATE worker_state SET value = jsonb_set(value, '{{fill}}', value->'fill' || '{{\"stage\": \"waiting\", \"part\": \"scope\", \"waiting_since\": \"{}\"}}'::jsonb) WHERE key = 'public_transition'",
            rfc3339(&at)
        ),
    )
    .await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::Community))).await.expect("restart");
    assert!(matches!(task, Some(Spawn::Checker { generation: 4, .. })), "{task:?}");
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::Filling, Breadth::Full));
    db.drop_db().await;
}

/// Off → full is Filling, Full with a fresh fill; the fill's end goes to
/// On, Full, with no count repair; a file value of `full` on a fresh
/// install takes the same path.
#[tokio::test]
async fn off_to_full_fills_then_turns_on_in_full() {
    let Some(db) = fresh_db("full_from_off").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::Full))).await.expect("startup");
    assert_eq!(setting(p).await.0, json!("full"));
    let Some(task @ Spawn::Fill { generation: 2, .. }) = task else {
        panic!("expected a fresh fill at generation 2, got {task:?}");
    };
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::Filling, Breadth::Full));
    drop(pi.spawn(task));
    eventually("on", async || scope.public_mode() == PublicMode::On).await;
    assert_eq!(scope.public_breadth(), Breadth::Full);
    assert_eq!(pi.hooks.repairs.load(Ordering::SeqCst), 0, "full skips the locking count repair");
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["breadth"]), (&json!("on"), &json!("full")));
    db.drop_db().await;
}

/// Seed what full stores beyond community: rows of a stranger, and rows
/// only full writes; and what community keeps: S's rows and a stranger's
/// like of a member's post.
async fn seed_full_rows(p: &PgPool) {
    members(p, &[MEMBER]).await;
    in_scope(p, &[MEMBER, B]).await;
    exec(p, &crate::test_support::posts::row_sql(STRANGER, "3kaaaaaaaaa22")).await;
    exec(p, &crate::test_support::posts::feed_item_sql(STRANGER, "3kaaaaaaaaa22")).await;
    exec(p, &crate::test_support::posts::row_sql(B, "3kaaaaaaaaa23")).await;
    crate::test_support::interactions::like(p, STRANGER, "3kaaaaaaaaa24", &format!("at://{B}/app.bsky.feed.post/3kaaaaaaaaa23")).await;
    crate::test_support::interactions::like(p, STRANGER, "3kaaaaaaaaa25", &format!("at://{MEMBER}/app.bsky.feed.post/3kaaaaaaaaa26")).await;
    crate::test_support::graph::follow(p, STRANGER, D, "3kaaaaaaaaa27").await;
    crate::test_support::graph::block(p, STRANGER, D, "3kaaaaaaaaa28").await;
    exec(
        p,
        &format!(
            "INSERT INTO dids (did) VALUES ('{D}') ON CONFLICT DO NOTHING; \
             INSERT INTO constellation_counts (target, likes, fetched_at) VALUES ('at://{STRANGER}/app.bsky.feed.post/3kaaaaaaaaa22', 1, now()), ('at://{B}/app.bsky.feed.post/3kaaaaaaaaa23', 1, now()); \
             INSERT INTO did_cache (did, doc, fetched_at, stale_at, expires_at) VALUES ('{STRANGER}', '{{}}', now(), now(), now() + interval '1 day'), ('{B}', '{{}}', now(), now(), now() + interval '1 day'); \
             INSERT INTO identity_refresh (did, reasons) VALUES ('{STRANGER}', 4), ('{B}', 4); \
             INSERT INTO record_tombstones (author, coll, rkey, rev) SELECT id, 1, 9, 0 FROM dids WHERE did IN ('{STRANGER}', '{B}'); \
             INSERT INTO full_crawl (account, generation) SELECT id, 0 FROM dids WHERE did = '{STRANGER}'; \
             INSERT INTO full_crawl_groups (account, grp, group_generation, crawl_generation) SELECT id, 'posts', 1, 0 FROM dids WHERE did = '{STRANGER}'; \
             INSERT INTO relay_account_status (account, relay, status) SELECT id, 'bsky', 'takendown' FROM dids WHERE did IN ('{STRANGER}', '{MEMBER}'); \
             INSERT INTO refract_excluded (account) SELECT id FROM dids WHERE did = '{STRANGER}'; \
             INSERT INTO worker_state (key, value) VALUES ('full_groups', '{{\"crawl_generation\": 0, \"groups\": {{}}}}');"
        ),
    )
    .await;
}

async fn rows_of(p: &PgPool, table: &str, did: &str) -> i64 {
    let col = match table {
        "feed_items" => "originator",
        "full_crawl" | "full_crawl_groups" | "relay_account_status" | "refract_excluded" => "account",
        _ => "author",
    };
    count(p, &format!("SELECT count(*) FROM {table} t JOIN dids d ON d.id = t.{col} WHERE d.did = '{did}'")).await
}

/// Narrowing: admission narrows at once, public reads answer 501 when S's
/// fill had not finished, the sweep deletes every row community would not
/// keep (from the shared tables and every table only full writes) and
/// keeps S's rows, a member's side and a stranger's like of a member's
/// post; it ends in Filling, Community, then On.
#[tokio::test]
async fn narrowing_deletes_what_full_added_and_refills() {
    let Some(db) = fresh_db("full_narrowing").await else {
        return;
    };
    let p = &db.pool;
    seed_full_rows(p).await;
    let on_full = Transition::settled(Phase::On, 7, now(), None).in_breadth(Breadth::Full, 2);
    seed(p, "full", Some(json!({"value": "full", "explicit": true})), &on_full.to_json()).await;
    // The fill after the narrowing ends at once: no backfill worker runs.
    let quick = PublicModeSettings { fill_max_wait: Duration::ZERO, ..settings() };
    let (pi, scope) = boot(p, quick).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Full))).await.expect("startup"), None);
    assert_eq!(scope.public_breadth(), Breadth::Full);
    let applied = pi.apply_scope(ScopeValue::Community, "cli").await.expect("narrow");
    assert_eq!((applied.action, applied.mode), ("narrowing", PublicMode::On), "S was filled: served");
    assert_eq!(scope.public_breadth(), Breadth::Narrowing);
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["crawl_generation"], &t["s_filled"], &t["sweep"]["list"]), (&json!("narrowing"), &json!(3), &json!(true), &json!("narrowing")));
    // A restart mid-narrowing resumes it.
    let (pi, scope) = boot(p, quick).await;
    let task = pi.startup(&file(Some(PublicScope::Full))).await.expect("restart");
    let Some(task @ Spawn::Sweep { generation: 8, .. }) = task else {
        panic!("expected the narrowing sweep, got {task:?}");
    };
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::On, Breadth::Narrowing));
    drop(pi.spawn(task));
    eventually("on, community", async || scope.public_mode() == PublicMode::On && scope.public_breadth() == Breadth::Community).await;
    assert_eq!(pi.hooks.spawned(), ["sweep", "fill", "checker"]);
    // Deleted: everything of the stranger's that community does not keep.
    for table in ["posts", "feed_items", "follows", "blocks", "record_tombstones", "full_crawl", "full_crawl_groups", "relay_account_status", "refract_excluded"] {
        assert_eq!(rows_of(p, table, STRANGER).await, 0, "{table}");
    }
    assert_eq!(count(p, &format!("SELECT count(*) FROM constellation_counts WHERE target LIKE 'at://{STRANGER}/%'")).await, 0);
    assert_eq!(count(p, &format!("SELECT count(*) FROM did_cache WHERE did = '{STRANGER}'")).await, 0);
    assert_eq!(count(p, &format!("SELECT count(*) FROM identity_refresh WHERE did = '{STRANGER}'")).await, 0);
    assert_eq!(full_groups(p).await, None);
    // Kept: S's rows, the like of a member's post, the member's status.
    assert_eq!(rows_of(p, "posts", B).await, 1);
    assert_eq!(rows_of(p, "record_tombstones", B).await, 1);
    assert_eq!(rows_of(p, "relay_account_status", MEMBER).await, 1);
    assert_eq!(rows_of(p, "likes", STRANGER).await, 1, "only the like of a member's post stays");
    assert_eq!(count(p, &format!("SELECT count(*) FROM did_cache WHERE did = '{B}'")).await, 1);
    assert_eq!(count(p, &format!("SELECT count(*) FROM constellation_counts WHERE target LIKE 'at://{B}/%'")).await, 1);
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["breadth"]), (&json!("on"), &json!("community")));
    db.drop_db().await;
}

/// The narrowing removes only what full could have added: what community
/// held when full was entered stays (a cached document of a DID outside S,
/// an identity row of one, a block with no member side from before), and
/// the same kinds written during full go. A narrowing stopped by a switch
/// back to full keeps the first entry's time, and entering full reads the
/// excluded accounts again.
#[tokio::test]
async fn narrowing_keeps_what_community_held_before_full() {
    const OLD_ACTOR: &str = "did:plc:ffffffffffffffffffffffff";
    const OLD_DOC: &str = "did:plc:gggggggggggggggggggggggg";
    let Some(db) = fresh_db("full_narrowing_keeps").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    in_scope(p, &[MEMBER, B]).await;
    // Community's own rows, from before full.
    crate::test_support::graph::block(p, D, B, "3kaaaaaaaaa28").await;
    exec(
        p,
        &format!(
            "{}; INSERT INTO did_cache (did, doc, fetched_at, stale_at, expires_at) \
             VALUES ('{OLD_DOC}', '{{}}', now() - interval '1 hour', now(), now() + interval '1 day');",
            crate::test_support::actors::put_sql(OLD_ACTOR, &[("indexed_at", "now() - interval '1 hour'")])
        ),
    )
    .await;
    seed(p, "community", Some(json!({"value": "community", "explicit": true})), &Transition::settled(Phase::On, 3, now(), None).to_json()).await;
    let quick = PublicModeSettings { fill_max_wait: Duration::ZERO, ..settings() };
    let (pi, scope) = boot(p, quick).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Community))).await.expect("startup"), None);
    assert_eq!(pi.apply_scope(ScopeValue::Full, "cli").await.expect("full").action, "breadth");
    let since: Value = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'full_since'").fetch_one(p).await.expect("entry time");
    // What full writes: a stranger's block, identity row and document.
    let t = (u64::try_from((Utc::now() + TimeDelta::minutes(1)).timestamp_micros()).expect("micros") << 10) + 1;
    let rev = crate::db::ids::rev_string(i64::from_ne_bytes((t ^ (1u64 << 63)).to_ne_bytes()));
    crate::test_support::graph::Edge::block(STRANGER, D, "3kaaaaaaaaa29").rev(&rev).put(p).await;
    exec(
        p,
        &format!(
            "{}; INSERT INTO did_cache (did, doc, fetched_at, stale_at, expires_at) VALUES ('{STRANGER}', '{{}}', now(), now(), now() + interval '1 day');",
            crate::test_support::actors::put_sql(STRANGER, &[("pds_endpoint", "'https://pds.example'")])
        ),
    )
    .await;
    // Narrow, stop it by going back to full, then narrow again: the first
    // entry's time stays, and the excluded set is read from the table.
    assert_eq!(pi.apply_scope(ScopeValue::Community, "cli").await.expect("narrow").action, "narrowing");
    scope.read_guard().await.full.excluded.insert(STRANGER);
    assert_eq!(pi.apply_scope(ScopeValue::Full, "cli").await.expect("back").action, "breadth");
    assert!(!scope.read_guard().await.full.excluded.contains(STRANGER), "entering full reads the excluded set again");
    let again: Value = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'full_since'").fetch_one(p).await.expect("entry time");
    assert_eq!(again, since, "a stopped narrowing keeps the first entry's time");
    let applied = pi.apply_scope(ScopeValue::Community, "cli").await.expect("narrow");
    drop(pi.spawn(applied.spawn.expect("the narrowing sweep")));
    eventually("on, community", async || scope.public_mode() == PublicMode::On && scope.public_breadth() == Breadth::Community).await;
    // Community's rows stay.
    assert_eq!(rows_of(p, "blocks", D).await, 1, "a block from before full stays for retention");
    assert_eq!(count(p, &format!("SELECT count(*) FROM actors a JOIN dids d ON d.id = a.id WHERE d.did = '{OLD_ACTOR}'")).await, 1);
    assert_eq!(count(p, &format!("SELECT count(*) FROM did_cache WHERE did = '{OLD_DOC}'")).await, 1);
    // What full wrote goes.
    assert_eq!(rows_of(p, "blocks", STRANGER).await, 0);
    assert_eq!(count(p, &format!("SELECT count(*) FROM actors a JOIN dids d ON d.id = a.id WHERE d.did = '{STRANGER}'")).await, 0);
    assert_eq!(count(p, &format!("SELECT count(*) FROM did_cache WHERE did = '{STRANGER}'")).await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM worker_state WHERE key IN ('full_since', 'full_groups')").await, 0);
    db.drop_db().await;
}

/// Filling, Full → community narrows with `s_filled = false`, so public
/// reads answer 501 during it; going back to full before it ends re-runs
/// S's fill at a new crawl generation (and stops the narrowing).
#[tokio::test]
async fn filling_full_to_community_and_back_refills() {
    let Some(db) = fresh_db("full_filling_narrow").await else {
        return;
    };
    let p = &db.pool;
    seed(p, "full", Some(json!({"value": "full", "explicit": true})), &Transition::filling(3, now()).in_breadth(Breadth::Full, 4).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    assert!(pi.startup(&file(Some(PublicScope::Full))).await.expect("startup").is_some());
    let applied = pi.apply_scope(ScopeValue::Community, "cli").await.expect("narrow");
    assert_eq!((applied.action, applied.mode), ("narrowing", PublicMode::Filling), "S never filled: 501");
    assert!(matches!(applied.spawn, Some(Spawn::Sweep { generation: 4, .. })));
    assert_eq!(transition(p).await["s_filled"], json!(false));
    // Back to full before the sweep ran: a fresh fill, a new crawl
    // generation, the narrowing stopped by the generation bump.
    let back = pi.apply_scope(ScopeValue::Full, "cli").await.expect("full");
    assert!(matches!(back.spawn, Some(Spawn::Fill { generation: 5, .. })), "{:?}", back.spawn);
    assert_eq!((scope.public_mode(), scope.public_breadth()), (PublicMode::Filling, Breadth::Full));
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["breadth"], &t["crawl_generation"]), (&json!("filling"), &json!("full"), &json!(6)));
    assert_eq!(full_groups(p).await.expect("groups")["crawl_generation"], 6);
    // The stopped sweep exits at once and deletes nothing.
    pi.clone().run_sweep(4, SweepState::first(SweepList::Narrowing, now())).await;
    assert_eq!(transition(p).await["phase"], "filling");
    db.drop_db().await;
}

/// full → kryphocron_only drains with the full list: today's tables, then
/// the tables full adds to; a restart on a full-only step resumes there;
/// the drain ends Off.
#[tokio::test]
async fn full_to_kryphocron_only_drains_the_added_tables() {
    let Some(db) = fresh_db("full_drain").await else {
        return;
    };
    let p = &db.pool;
    seed_full_rows(p).await;
    seed(p, "full", Some(json!({"value": "full", "explicit": true})), &Transition::settled(Phase::On, 2, now(), None).in_breadth(Breadth::Full, 1).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    pi.startup(&file(Some(PublicScope::Full))).await.expect("startup");
    let applied = pi.apply_scope(ScopeValue::KryphocronOnly, "cli").await.expect("drain");
    assert_eq!(applied.action, "draining");
    assert_eq!(scope.public_breadth(), Breadth::Community, "the off rule decides admission");
    let t = transition(p).await;
    assert_eq!((&t["sweep"]["list"], &t["crawl_generation"]), (&json!("full"), &json!(2)));
    // A restart with the cursor on a full-only step keeps the full list.
    exec(p, "UPDATE worker_state SET value = jsonb_set(value, '{sweep,table}', '\"did_cache\"') WHERE key = 'public_transition'").await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::Full))).await.expect("restart");
    let Some(Spawn::Sweep { sweep, .. }) = &task else {
        panic!("expected the drain, got {task:?}");
    };
    assert_eq!((sweep.list, sweep.table.as_str()), (SweepList::Full, "did_cache"));
    drop(pi.spawn(task.expect("task")));
    eventually("off", async || scope.public_mode() == PublicMode::Off).await;
    for table in ["full_crawl", "full_crawl_groups", "refract_excluded"] {
        assert_eq!(rows_of(p, table, STRANGER).await, 0, "{table}");
    }
    assert_eq!(count(p, &format!("SELECT count(*) FROM did_cache WHERE did = '{STRANGER}'")).await, 0);
    assert_eq!(full_groups(p).await, None);
    db.drop_db().await;
}

/// Every stored `(value, phase, breadth)` that can occur maps to its mode,
/// and anything else is Held.
#[test]
fn map_rows_for_every_breadth() {
    let at = now();
    let v = |t: Transition| match validate(&t.to_json(), at).0 {
        Validated::Valid(t) => Validated::Valid(t),
        held => panic!("{held:?}"),
    };
    let full_on = v(Transition::settled(Phase::On, 2, at, None).in_breadth(Breadth::Full, 1));
    assert_eq!(map(Some(ScopeValue::Full), &full_on).0.breadth, Breadth::Full);
    assert_eq!(map(Some(ScopeValue::Community), &full_on).0.mode, PublicMode::Held);
    let narrowing = v(Transition::narrowing(2, at, None, false));
    let (state, task) = map(Some(ScopeValue::Community), &narrowing);
    assert_eq!((state.mode, state.breadth), (PublicMode::Filling, Breadth::Narrowing));
    assert!(matches!(task, Some(Spawn::Sweep { .. })));
    let mut draining = Transition::draining(2, at, None);
    draining.sweep = Some(SweepState::first(SweepList::Full, at));
    let (state, task) = map(Some(ScopeValue::KryphocronOnly), &v(draining));
    assert_eq!(state.mode, PublicMode::Draining);
    assert!(matches!(task, Some(Spawn::Sweep { sweep, .. }) if sweep.list == SweepList::Full));
    // A narrowing phase without the narrowing breadth, or a narrowing
    // without `s_filled`, does not validate.
    let mut bad = Transition::narrowing(2, at, None, true).to_json();
    bad["breadth"] = json!("full");
    assert!(matches!(validate(&bad, at).0, Validated::Held { .. }));
    let mut bad = Transition::narrowing(2, at, None, true).to_json();
    bad.as_object_mut().expect("object").remove("s_filled");
    assert!(matches!(validate(&bad, at).0, Validated::Held { .. }));
    // A stored state from before breadth existed reads as Community / 0.
    let mut old = Transition::settled(Phase::On, 2, at, None).to_json();
    old.as_object_mut().expect("object").remove("breadth");
    old.as_object_mut().expect("object").remove("crawl_generation");
    let Validated::Valid(t) = validate(&old, at).0 else {
        panic!("old state");
    };
    assert_eq!((t.breadth, t.crawl_generation), (Breadth::Community, 0));
    // A sweep stored without a list reads as today's.
    let mut old = Transition::draining(2, at, None).to_json();
    old["sweep"].as_object_mut().expect("sweep").remove("list");
    let Validated::Valid(t) = validate(&old, at).0 else {
        panic!("old sweep");
    };
    assert_eq!(t.sweep.map(|s| s.list), Some(SweepList::Today));
    // Held keeps the crawl generation, so leaving it never reuses one.
    let mut held = Transition::settled(Phase::On, 2, at, None).in_breadth(Breadth::Full, 9).to_json();
    held["phase"] = json!("nowhere");
    assert!(matches!(validate(&held, at).0, Validated::Held { crawl_generation: 9, .. }));
}

/// A post of `author` with `rkey`.
fn post(rkey: &str, text: &str) -> RawOp {
    op("app.bsky.feed.post", rkey, json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-11-01T00:00:00Z"}))
}

/// A scope in On, Full with `kinds`, and a writer over it.
async fn full_writer(p: &PgPool, kinds: &[crate::config::FullGroup]) -> (Writer, ScopeState) {
    seed(p, "full", Some(json!({"value": "full", "explicit": true})), &Transition::settled(Phase::On, 2, now(), None).in_breadth(Breadth::Full, 1).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    pi.startup(&file(Some(PublicScope::Full))).await.expect("startup");
    let full = crate::config::FullConfig { kinds: kinds.to_vec(), ..crate::config::FullConfig::default() };
    scope.load_full(p, &full, &["bsky".to_string()]).await.expect("full sets");
    (Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier)), scope)
}

/// Rule 1's full arm: in full, any author's public record of an enabled
/// group is admitted, with its feed item and its delete's tombstone; a
/// group not enabled follows community rules; an Aside copy's full-arm op
/// is dropped while a member-involved op of the same copy is admitted; in
/// community the same ops are rejected.
#[tokio::test]
async fn full_admits_any_author_for_enabled_groups() {
    use crate::config::FullGroup;
    use crate::firehose::tier::CopyTier;
    let Some(db) = fresh_db("full_admission").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    let (writer, scope) = full_writer(p, &[FullGroup::Posts, FullGroup::Likes]).await;
    assert_eq!(scope.public_breadth(), Breadth::Full);
    let other_like = op(
        "app.bsky.feed.like",
        "3kaaaaaaaaab2",
        json!({"subject": {"uri": format!("at://{D}/app.bsky.feed.post/3kaaaaaaaaab3"), "cid": "c"}, "createdAt": "2026-11-01T00:00:00Z"}),
    );
    let follow = op("app.bsky.graph.follow", "3kaaaaaaaaab4", json!({"subject": D, "createdAt": "2026-11-01T00:00:00Z"}));
    let r = writer.apply(STRANGER, "3kaaaaaaaab22", &[post("3kaaaaaaaaab5", "hello"), other_like.clone(), follow], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Written, OpOutcome::Written, OpOutcome::Rejected("not_in_scope")], "follows are not enabled");
    assert_eq!(rows_of(p, "posts", STRANGER).await, 1);
    assert_eq!(rows_of(p, "feed_items", STRANGER).await, 1, "a feed item for any author whose posts group is on");
    assert_eq!(rows_of(p, "likes", STRANGER).await, 1);
    assert_eq!(rows_of(p, "post_search", STRANGER).await, 0, "search rows follow S only");
    // A delete of an enabled group leaves a tombstone for any author.
    let del = RawOp { action: Action::Delete, collection: "app.bsky.feed.like".into(), rkey: "3kaaaaaaaaab2".into(), cid: None, record: None };
    writer.apply(STRANGER, "3kaaaaaaaab23", &[del], LIVE).await.expect("delete");
    let missing = RawOp { action: Action::Delete, collection: "app.bsky.feed.post".into(), rkey: "3kaaaaaaaaab7".into(), cid: None, record: None };
    writer.apply(STRANGER, "3kaaaaaaaab24", &[missing], LIVE).await.expect("delete");
    assert_eq!(rows_of(p, "record_tombstones", STRANGER).await, 2, "a delete with no row tombstones too");
    // An Aside copy: the stranger's post is dropped, its like of a
    // member's post is admitted as in community.
    let member_like = like("3kaaaaaaaaab8");
    let r = writer
        .apply_copy(Some("bsky"), D, "3kaaaaaaaab25", None, &[post("3kaaaaaaaaab9", "aside"), member_like], LIVE, CopyTier::Aside)
        .await
        .expect("aside");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("aside"), OpOutcome::Written]);
    assert_eq!(rows_of(p, "posts", D).await, 0);
    // A Covered copy admits it.
    let r = writer.apply_copy(Some("bsky"), D, "3kaaaaaaaab26", None, &[post("3kaaaaaaaaaba", "covered")], LIVE, CopyTier::Covered).await.expect("covered");
    assert_eq!(r.outcomes, [OpOutcome::Written]);
    // Community admits none of it.
    {
        let mut sets = scope.write_guard().await;
        let community = PublicState { breadth: Breadth::Community, ..sets.public };
        scope.set_public(&mut sets, community);
    }
    let r = writer.apply(STRANGER, "3kaaaaaaaab27", &[post("3kaaaaaaaaabb", "again"), other_like], LIVE).await.expect("community");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("not_in_scope"), OpOutcome::Rejected("not_in_scope")]);
    db.drop_db().await;
}

/// Blocks by option: in full with `blocks_source = "local"` and the
/// `blocks` and `lists` groups on, any account's block and list block is
/// stored; an Aside copy's non-member block is dropped while its block of a
/// member is stored; with `"farsight"`, without the `lists` group, or in
/// community, a block keeps a member side.
#[tokio::test]
async fn blocks_by_option() {
    use crate::config::{BlocksSource, FullConfig, FullGroup};
    use crate::firehose::tier::CopyTier;
    let Some(db) = fresh_db("full_blocks_by_option").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    let (writer, scope) = full_writer(p, &FullGroup::ALL).await;
    let block = |rkey: &str, subject: &str| op("app.bsky.graph.block", rkey, json!({"subject": subject, "createdAt": "2026-11-01T00:00:00Z"}));
    let list_block = |rkey: &str| {
        op("app.bsky.graph.listblock", rkey, json!({"subject": format!("at://{B}/app.bsky.graph.list/3kaaaaaaaaal1"), "createdAt": "2026-11-01T00:00:00Z"}))
    };
    let r = writer.apply(STRANGER, "3kaaaaaaaac21", &[block("3kaaaaaaaaac1", D), list_block("3kaaaaaaaaac2")], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Written, OpOutcome::Written], "the member-side rule is lifted");
    assert_eq!(rows_of(p, "blocks", STRANGER).await, 1);
    assert_eq!(rows_of(p, "list_blocks", STRANGER).await, 1);
    // An Aside copy: the block of a non-member is dropped, the block of a
    // member stored as in community.
    let r = writer
        .apply_copy(Some("bsky"), D, "3kaaaaaaaac22", None, &[block("3kaaaaaaaaac3", STRANGER), block("3kaaaaaaaaac4", MEMBER)], LIVE, CopyTier::Aside)
        .await
        .expect("aside");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("aside"), OpOutcome::Written]);
    // `blocks_source = "farsight"`, or the lists group off: a member side.
    for (n, full) in [
        FullConfig { blocks_source: BlocksSource::Farsight, ..FullConfig::default() },
        FullConfig { kinds: FullGroup::ALL.iter().copied().filter(|g| *g != FullGroup::Lists).collect(), ..FullConfig::default() },
    ]
    .into_iter()
    .enumerate()
    {
        scope.load_full(p, &full, &["bsky".to_string()]).await.expect("full sets");
        let n = ["b", "c"][n];
        let ops = [block(&format!("3kaaaaaaaaad{n}"), D), block(&format!("3kaaaaaaaaae{n}"), MEMBER)];
        let r = writer.apply(B, &format!("3kaaaaaaaaf2{n}"), &ops, LIVE).await.expect("apply");
        assert_eq!(r.outcomes, [OpOutcome::Rejected("no_member_side"), OpOutcome::Written], "{:?}", full.blocks_source);
    }
    // Community keeps the member side whatever the option says.
    scope.load_full(p, &FullConfig::default(), &["bsky".to_string()]).await.expect("full sets");
    {
        let mut sets = scope.write_guard().await;
        let community = PublicState { breadth: Breadth::Community, ..sets.public };
        scope.set_public(&mut sets, community);
    }
    let r = writer.apply(B, "3kaaaaaaaac24", &[block("3kaaaaaaaaac7", D), list_block("3kaaaaaaaaac8")], LIVE).await.expect("community");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("no_member_side"), OpOutcome::Rejected("no_member_side")]);
    db.drop_db().await;
}

/// The post-lock status check: in full, a deleted account's and a
/// relay-suspended account's records are not admitted by the full arm, and
/// one Refract lists or `suspended_accounts = "all"` lets through; an
/// account whose deletion marker is set has nothing admitted.
#[tokio::test]
async fn full_arm_checks_status_after_the_locks() {
    use crate::config::FullGroup;
    let Some(db) = fresh_db("full_status_check").await else {
        return;
    };
    let p = &db.pool;
    let (writer, scope) = full_writer(p, &FullGroup::ALL).await;
    exec(
        p,
        &format!(
            "{}; INSERT INTO dids (did) VALUES ('{STRANGER}') ON CONFLICT DO NOTHING; \
             INSERT INTO relay_account_status (account, relay, status) SELECT id, 'bsky', 'takendown' FROM dids WHERE did = '{STRANGER}';",
            crate::test_support::actors::put_sql(D, &[("account_status", "'deleted'")])
        ),
    )
    .await;
    let r = writer.apply(D, "3kaaaaaaaab22", &[post("3kaaaaaaaaab2", "gone")], LIVE).await.expect("deleted");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("deleted")]);
    let r = writer.apply(STRANGER, "3kaaaaaaaab22", &[post("3kaaaaaaaaab2", "suspended")], LIVE).await.expect("suspended");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("suspended")]);
    assert_eq!(rows_of(p, "posts", STRANGER).await + rows_of(p, "posts", D).await, 0);
    // Under "all" the relay's status is recorded but not applied.
    let all = crate::config::FullConfig { suspended_accounts: crate::config::SuspendedAccounts::All, ..crate::config::FullConfig::default() };
    scope.load_full(p, &all, &["bsky".to_string()]).await.expect("all");
    let r = writer.apply(STRANGER, "3kaaaaaaaab23", &[post("3kaaaaaaaaab3", "served")], LIVE).await.expect("all");
    assert_eq!(r.outcomes, [OpOutcome::Written]);
    // A deletion marker set without the write guard is seen after the lock.
    scope.read_guard().await.full.purge_markers.insert(B);
    let r = writer.apply(B, "3kaaaaaaaab22", &[post("3kaaaaaaaaab2", "marked")], LIVE).await.expect("marked");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("account_deleted")]);
    db.drop_db().await;
}

/// The unguarded deletion purge in full: the marker is set while the
/// marker row is not yet committed and a writer is paused between
/// admission and its locks; the writer then drops the op; the purge ends
/// without the scope write guard and removes the marker.
#[tokio::test]
async fn an_unguarded_deletion_purge_stops_a_writer_past_admission() {
    use crate::config::FullGroup;
    let Some(db) = fresh_db("full_purge_protocol").await else {
        return;
    };
    let p = &db.pool;
    let (writer, scope) = full_writer(p, &FullGroup::ALL).await;
    let purger = crate::aggregations::purge::Purger::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    // Hold the write guard's readers out: a write guard taken by the purge
    // would deadlock against this read guard, so a finished purge proves it
    // took none.
    let held = scope.read_guard().await;
    tokio::time::timeout(Duration::from_secs(30), purger.purge(STRANGER, crate::aggregations::PurgeKind::AccountDeleted))
        .await
        .expect("the purge took no write guard")
        .expect("purge");
    drop(held);
    assert!(!scope.read_guard().await.full.purge_markers.contains(STRANGER), "the end removes the marker");
    assert_eq!(count(p, "SELECT count(*) FROM purging_dids").await, 0);
    // While marked, a writer drops the account's ops.
    scope.read_guard().await.full.purge_markers.insert(STRANGER);
    let r = writer.apply(STRANGER, "3kaaaaaaaab22", &[post("3kaaaaaaaaab2", "late")], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Rejected("account_deleted")]);
    db.drop_db().await;
}

/// A suspension that lasts `suspended_purge_days` removes the account's own
/// public rows and keeps its like of a member's post; a shorter one, an S
/// author's and a member's keep everything; 0 days removes nothing.
#[tokio::test]
async fn a_long_suspension_removes_the_accounts_public_rows() {
    use crate::db::retention::RetentionTask;
    let Some(db) = fresh_db("full_suspended_purge").await else {
        return;
    };
    let p = &db.pool;
    members(p, &[MEMBER]).await;
    in_scope(p, &[B]).await;
    let (_writer, scope) = full_writer(p, &crate::config::FullGroup::ALL).await;
    for (did, rkey) in [(STRANGER, "3kaaaaaaaaa22"), (D, "3kaaaaaaaaa23"), (B, "3kaaaaaaaaa24"), (MEMBER, "3kaaaaaaaaa25")] {
        exec(p, &crate::test_support::posts::row_sql(did, rkey)).await;
    }
    crate::test_support::interactions::like(p, STRANGER, "3kaaaaaaaaa26", &format!("at://{MEMBER}/app.bsky.feed.post/3kaaaaaaaaa25")).await;
    crate::test_support::graph::follow(p, STRANGER, D, "3kaaaaaaaaa27").await;
    exec(
        p,
        &format!(
            "INSERT INTO relay_account_status (account, relay, status, at) SELECT id, 'bsky', 'takendown', now() - interval '31 days' FROM dids WHERE did IN ('{STRANGER}', '{B}', '{MEMBER}'); \
             INSERT INTO relay_account_status (account, relay, status, at) SELECT id, 'bsky', 'suspended', now() - interval '1 day' FROM dids WHERE did = '{D}';"
        ),
    )
    .await;
    let settings = crate::db::retention::RetentionSettings::from(&crate::config::RetentionConfig::default());
    let off = crate::indexing::full::SuspendedPurge::new(scope.clone(), 0).run(p, &scope, &settings).await.expect("off");
    assert_eq!(off.total(), 0);
    assert_eq!(rows_of(p, "posts", STRANGER).await, 1);
    let report = crate::indexing::full::SuspendedPurge::new(scope.clone(), 30).run(p, &scope, &settings).await.expect("purge");
    assert!(report.total() >= 2, "{report:?}");
    assert_eq!(rows_of(p, "posts", STRANGER).await, 0);
    assert_eq!(rows_of(p, "follows", STRANGER).await, 0);
    assert_eq!(rows_of(p, "likes", STRANGER).await, 1, "a like of a member's post stays");
    for did in [D, B, MEMBER] {
        assert_eq!(rows_of(p, "posts", did).await, 1, "{did}");
    }
    // A repeated status keeps the suspension's start.
    crate::indexing::full::record_relay_status(p, STRANGER, "bsky", Some(false), Some("suspended")).await.expect("again");
    assert_eq!(count(p, &format!("SELECT count(*) FROM relay_account_status r JOIN dids d ON d.id = r.account WHERE d.did = '{STRANGER}' AND r.at < now() - interval '30 days'")).await, 1);
    db.drop_db().await;
}

/// The own-repo rule: local for S while On in community and full, never
/// while narrowing, and never for an account outside S without a crawl.
#[test]
fn the_own_repo_rule_by_breadth() {
    let mut sets = crate::aggregations::ScopeSets::default();
    sets.scope.insert(B.to_string());
    for (breadth, mode, b_local) in [
        (Breadth::Community, PublicMode::On, true),
        (Breadth::Full, PublicMode::On, true),
        (Breadth::Full, PublicMode::Filling, false),
        (Breadth::Narrowing, PublicMode::On, false),
    ] {
        sets.public = PublicState { mode, breadth, ..PublicState::default() };
        assert_eq!(sets.own_repo_local(B), b_local, "{breadth:?} {mode:?}");
        assert!(!sets.own_repo_local(STRANGER), "{breadth:?}");
    }
}

/// A takedown relay's deactivation or deletion sets the account's status
/// at once, so reads hide it before the refresher asks its server; a
/// takedown or a suspension does not; a stored deletion is never turned
/// back into a deactivation; an account with no identity row gets none.
#[tokio::test]
async fn a_relay_deactivation_hides_the_account_at_once() {
    let Some(db) = fresh_db("full_provisional_status").await else {
        return;
    };
    let p = &db.pool;
    crate::test_support::actors::put(p, STRANGER, &[]).await;
    let status = || async { sqlx::query_scalar::<_, Option<String>>(&format!("SELECT account_status FROM actors WHERE {}", crate::test_support::actors::is("", STRANGER))).fetch_one(p).await.expect("status") };
    let before = status().await;
    use crate::indexing::full::provisional_status;
    assert!(!provisional_status(p, STRANGER, Some("takendown")).await.expect("takendown"));
    assert!(!provisional_status(p, STRANGER, Some("suspended")).await.expect("suspended"));
    assert!(!provisional_status(p, STRANGER, None).await.expect("no status"));
    assert_eq!(status().await, before, "only a deactivation or a deletion is written");
    assert!(provisional_status(p, STRANGER, Some("deactivated")).await.expect("deactivated"));
    assert_eq!(status().await.as_deref(), Some("deactivated"));
    let gates = crate::moderation::gate_many(p, &[STRANGER.to_string()], crate::refract::RefractView::empty()).await.expect("gate");
    assert!(gates[STRANGER].is_hidden(), "hidden at once");
    assert!(provisional_status(p, STRANGER, Some("deleted")).await.expect("deleted"));
    assert!(!provisional_status(p, STRANGER, Some("deactivated")).await.expect("after deletion"));
    assert_eq!(status().await.as_deref(), Some("deleted"));
    assert!(!provisional_status(p, D, Some("deleted")).await.expect("no row"));
    assert_eq!(count(p, &format!("SELECT count(*) FROM actors WHERE {}", crate::test_support::actors::is("", D))).await, 0);
    db.drop_db().await;
}

/// A takedown relay's statuses: non-active is kept per relay and hides the
/// account in the gate, stored or not, unless it is exempt (a member, or
/// Refract lists it); active deletes the row; an event without a flag
/// writes nothing.
#[tokio::test]
async fn relay_statuses_are_recorded_and_hide_in_the_gate() {
    let Some(db) = fresh_db("full_relay_status").await else {
        return;
    };
    let p = &db.pool;
    use crate::indexing::full::record_relay_status;
    record_relay_status(p, STRANGER, "bsky", Some(false), Some("takendown")).await.expect("record");
    record_relay_status(p, D, "bsky", None, None).await.expect("no flag");
    record_relay_status(p, MEMBER, "bsky", Some(false), None).await.expect("record");
    assert_eq!(count(p, "SELECT count(*) FROM relay_account_status").await, 2);
    assert_eq!(count(p, "SELECT count(*) FROM relay_account_status WHERE status = 'inactive'").await, 1);
    let view = crate::refract::RefractView::empty();
    let relays = vec!["bsky".to_string()];
    let exempt = |did: &str| did == MEMBER;
    let post = format!("at://{STRANGER}/app.bsky.feed.post/3kaaaaaaaaab2");
    let subjects = vec![post.clone(), MEMBER.to_string(), D.to_string()];
    let gates = crate::moderation::gate_many_suspended(
        p,
        &subjects,
        Some(&std::collections::HashSet::new()),
        view,
        Some(crate::moderation::RelaySuspensions { relays: &relays, exempt: &exempt }),
    )
    .await
    .expect("gate");
    use crate::moderation::{Gate, HiddenReason};
    assert_eq!(gates[&post], Gate::Hidden(HiddenReason::Suspended), "a never-stored account is hidden too");
    assert_eq!(gates[MEMBER], Gate::Visible, "members are never affected");
    assert_eq!(gates[D], Gate::Visible);
    // Another relay's rows hide nobody; without suspensions nothing does.
    let other = vec!["microcosm".to_string()];
    let view = crate::refract::RefractView::empty();
    let gates = crate::moderation::gate_many_suspended(p, &subjects, None, view, Some(crate::moderation::RelaySuspensions { relays: &other, exempt: &exempt }))
        .await
        .expect("gate");
    assert_eq!(gates[&post], Gate::Visible);
    record_relay_status(p, STRANGER, "bsky", Some(true), None).await.expect("active");
    assert_eq!(count(p, "SELECT count(*) FROM relay_account_status").await, 1);
    db.drop_db().await;
}
