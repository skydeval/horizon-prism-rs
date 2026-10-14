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
    FillPart, FillStage, Phase, PublicIndexing, PublicMode, PublicModeSettings, PublicState, ScopeValue, Spawn, SweepState,
    Transition, now, parse_ts, rfc3339,
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
    (1..=n)
        .map(|i| format!("INSERT INTO feed_items (uri, originator_did, post_uri, kind, sort_at) VALUES ('at://{B}/app.bsky.feed.post/f{i}', '{B}', 'at://{B}/app.bsky.feed.post/f{i}', 'post', now());"))
        .collect()
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
    exec(p, &format!("INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at) VALUES ('at://{B}/app.bsky.feed.post/1', '{B}', '1', 'c', '3k', '{{}}', now())")).await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Off))).await.expect("startup"), None);
    let (value, file_value, by) = setting(p).await;
    assert_eq!((value, file_value, by.as_str()), (json!("community_scoped"), Some(json!({"value": "off", "explicit": true})), "upgrade"));
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["last_fill_since"]), (&json!("on"), &json!(1), &Value::Null));
    assert_eq!(scope.public_mode(), PublicMode::On);
    assert_eq!(scope.snapshot().await.public.generation, 1);
    assert!(pi.hooks.spawned().is_empty());
    db.drop_db().await;
}

/// S2: an empty database is off with no key, and on with an explicit
/// `community_scoped`; neither spawns anything.
#[tokio::test]
async fn s2_a_fresh_install_takes_the_file() {
    let Some(db) = fresh_db("public_s2").await else {
        return;
    };
    let p = &db.pool;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(None)).await.expect("startup"), None);
    let (value, file_value, by) = setting(p).await;
    assert_eq!((value, file_value, by.as_str()), (json!("off"), Some(json!({"value": "off", "explicit": false})), "startup"));
    assert_eq!((&transition(p).await["phase"], &transition(p).await["generation"]), (&json!("off"), &json!(1)));
    assert_eq!(scope.public_mode(), PublicMode::Off);

    exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::CommunityScoped))).await.expect("startup"), None);
    assert_eq!(setting(p).await.0, json!("community_scoped"));
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
    seed(p, "community_scoped", None, &on).await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Off))).await.expect("startup"), None);
    assert_eq!(setting(p).await, (json!("community_scoped"), Some(json!({"value": "off", "explicit": true})), "cli".to_string()));
    assert_eq!((scope.public_mode(), transition(p).await["generation"].clone()), (PublicMode::On, json!(5)));
    // S4: a CLI-set value stands while the file is unchanged.
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(Some(PublicScope::Off))).await.expect("startup"), None);
    assert_eq!(setting(p).await.0, json!("community_scoped"));
    assert_eq!(scope.public_mode(), PublicMode::On);
    // S6: the explicit key becomes a comment.
    exec(p, "UPDATE operator_settings SET file_value = '{\"value\": \"community_scoped\", \"explicit\": true}'").await;
    let (pi, scope) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(None)).await.expect("startup"), None);
    assert_eq!(setting(p).await, (json!("community_scoped"), Some(json!({"value": "off", "explicit": false})), "cli".to_string()));
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
    seed(p, "off", Some(json!({"value": "off", "explicit": false})), &Transition::settled(Phase::Off, 2, now(), None).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::CommunityScoped))).await.expect("startup");
    assert_eq!(*pi.hooks.steps.lock().expect("hooks"), ["record", "validate", "map", "memory", "s5"]);
    let Some(task @ Spawn::Fill { generation: 3, .. }) = task else {
        panic!("expected the fill pass at generation 3, got {task:?}");
    };
    assert_eq!(setting(p).await, (json!("community_scoped"), Some(json!({"value": "community_scoped", "explicit": true})), "file".to_string()));
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
    seed(p, "off", Some(json!({"value": "off", "explicit": false})), &Transition::draining(4, now(), None).to_json()).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::CommunityScoped))).await.expect("startup");
    assert!(matches!(task, Some(Spawn::Sweep { generation: 4, .. })), "{task:?}");
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["pending"]), (&json!("draining"), &json!(4), &json!("community_scoped")));
    assert_eq!(setting(p).await.0, json!("off"));
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
    seed(p, "off", Some(json!({"value": "community_scoped", "explicit": true})), &held).await;
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(Some(PublicScope::Off))).await.expect("startup");
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
        json!({"table": "likes", "after": 7, "last_advanced_at": at}),
        json!({"table": "notifications", "after": "7", "last_advanced_at": at}),
        json!({"table": "tag_activity_hourly", "after": ["rust"], "last_advanced_at": at}),
    ] {
        exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
        exec(p, &feed_sql(2)).await;
        let mut t = Transition::draining(7, now(), None).to_json();
        t["sweep"] = bad.clone();
        seed(p, "off", Some(json!({"value": "off", "explicit": false})), &t).await;
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
    pending["pending"] = json!("community_scoped");
    let earlier = rfc3339(&(at - TimeDelta::seconds(1)));
    for (value, t) in [
        ("community_scoped", x),
        ("off", yesterday),
        ("community_scoped", fill(json!({"waiting_since": rfc3339(&at)}))),
        ("community_scoped", fill(json!({"stage": "waiting", "part": "members", "waiting_since": rfc3339(&at)}))),
        ("community_scoped", fill(json!({"enqueued": -1}))),
        ("community_scoped", fill(json!({"stage": "waiting", "part": "scope", "waiting_since": earlier}))),
        ("community_scoped", pending),
        // Value and phase disagree.
        ("off", Transition::settled(Phase::On, 2, at, None).to_json()),
    ] {
        exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
        seed(p, value, Some(json!({"value": "off", "explicit": false})), &t).await;
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
    let fv = Some(json!({"value": "off", "explicit": false}));
    // `last_fill_since = "yesterday"`: off writes null and stored + 1.
    let mut t = Transition::settled(Phase::Off, 4, now(), None).to_json();
    t["last_fill_since"] = json!("yesterday");
    seed(p, "off", fv.clone(), &t).await;
    let (pi, scope) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    assert_eq!(scope.public_mode(), PublicMode::Held);

    // In Held: a block and a member's follow are stored; a non-member's
    // like is rejected.
    let writer = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    let block = op("app.bsky.graph.block", "3kblock", json!({"subject": D, "createdAt": "2026-10-14T00:00:00Z"}));
    let r = writer.apply(B, "3kaaaaaaaaaa2", &[block, like("3klike")], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Written, OpOutcome::Rejected("public_off")]);
    let follow = op("app.bsky.graph.follow", "3kfollow", json!({"subject": D, "createdAt": "2026-10-14T00:00:00Z"}));
    let r = writer.apply(MEMBER, "3kaaaaaaaaaa2", &[follow], LIVE).await.expect("apply");
    assert_eq!(r.outcomes, [OpOutcome::Written]);
    assert!(pi.hooks.spawned().is_empty(), "no sweep or fill runs in Held");

    let applied = pi.apply_scope(ScopeValue::Off, "cli").await.expect("apply");
    assert_eq!((applied.action, applied.mode, applied.generation), ("draining", PublicMode::Draining, 5));
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["last_fill_since"]), (&json!("draining"), &json!(5), &Value::Null));
    let (pi, scope) = boot(p, settings()).await;
    assert!(matches!(pi.startup(&file(None)).await.expect("startup"), Some(Spawn::Sweep { generation: 5, .. })));
    assert_eq!(scope.public_mode(), PublicMode::Draining);

    // A Held state whose `last_fill_since` parses keeps it.
    let kept = now() - TimeDelta::days(3);
    let mut t = Transition::filling(8, now()).to_json();
    t["pending"] = json!("community_scoped");
    t["last_fill_since"] = json!(rfc3339(&kept));
    exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
    seed(p, "community_scoped", fv.clone(), &t).await;
    let (pi, scope) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    assert_eq!(scope.public_mode(), PublicMode::Held);
    pi.apply_scope(ScopeValue::Off, "cli").await.expect("apply");
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
    let fv = Some(json!({"value": "off", "explicit": false}));
    exec(p, &feed_sql(5)).await;
    seed(p, "community_scoped", fv.clone(), &Transition::settled(Phase::On, 1, now(), None).to_json()).await;
    let (pi, _) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    let Some(Spawn::Sweep { generation: 2, .. }) = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn else {
        panic!("expected a sweep");
    };
    // Draining, after one batch.
    let Batch::Advanced { deleted: 3, last } = pi.sweep_batch(2, 0, None, 3).await.expect("batch") else {
        panic!("expected three rows deleted");
    };
    assert_eq!(pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("pending").action, "pending_set");
    // Kill, and restart.
    let (pi, scope) = boot(p, settings()).await;
    let task = pi.startup(&file(None)).await.expect("startup");
    let Some(Spawn::Sweep { generation: 2, sweep }) = &task else {
        panic!("expected the sweep, got {task:?}");
    };
    assert_eq!((sweep.table.as_str(), &sweep.after), ("feed_items", &last.to_json()));
    assert_eq!(transition(p).await["pending"], "community_scoped", "pending survives");
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
    seed(p, "community_scoped", fv.clone(), &t.to_json()).await;
    let (pi, _) = boot(p, settings()).await;
    assert_eq!(pi.startup(&file(None)).await.expect("startup"), Some(Spawn::Checker { generation: 9, waiting_since: ws }));
    // Filling, stage `enqueue` in `members`.
    let mut t = Transition::filling(10, now());
    if let Some(f) = t.fill.as_mut() {
        f.after = MEMBER.to_string();
        f.enqueued = 1;
    }
    exec(p, "DELETE FROM operator_settings; DELETE FROM worker_state WHERE key = 'public_transition'").await;
    seed(p, "community_scoped", fv, &t.to_json()).await;
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
    seed(p, "off", Some(json!({"value": "off", "explicit": false})), &Transition::settled(Phase::Off, 1, now(), None).to_json()).await;
    let (pi, _) = boot(p, settings()).await;
    pi.startup(&file(None)).await.expect("startup");
    let Some(Spawn::Fill { generation: 2, mut fill }) = pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("on").spawn else {
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
                let c = if t == "likes" { "like" } else { "repost" };
                sql += &format!("INSERT INTO {t} (uri, author_did, subject_uri, rev, created_at) VALUES ('at://{did}/app.bsky.feed.{c}/{r}', '{did}', '{}', '3k', now());", post(B, "p1"));
            }
        }
    }
    for (did, r) in [(B, "p1"), (B, "p2"), (MEMBER, "p3")] {
        sql += &format!("INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at) VALUES ('{}', '{did}', '{r}', 'c', '3k', '{{}}', now());", post(did, r));
    }
    for t in ["threadgates", "postgates"] {
        sql += &format!("INSERT INTO {t} (uri, author_did, rkey, cid, rev, post_uri, record, created_at) VALUES ('at://{B}/app.bsky.feed.{t}/1', '{B}', '1', 'c', '3k', '{}', '{{}}', now());", post(B, "p1"));
    }
    sql += &format!("INSERT INTO starter_packs (uri, creator_did, rkey, cid, rev, name, list_uri, record, created_at) VALUES ('at://{B}/app.bsky.graph.starterpack/1', '{B}', '1', 'c', '3k', 'n', 'at://{B}/app.bsky.graph.list/l', '{{}}', now());");
    sql += &format!("INSERT INTO labeler_services (uri, creator_did, cid, rev, policies, record, created_at) VALUES ('at://{B}/app.bsky.labeler.service/self', '{B}', 'c', '3k', '{{}}', '{{}}', now());");
    sql += &format!("INSERT INTO nearhorizon_poll_posts (uri, author_did, rkey, cid, rev, text, options, visibility, record, created_at) VALUES ('at://{B}/app.nearhorizon.poll/1', '{B}', '1', 'c', '3k', 't', '[]', 'public', '{{}}', now());");
    for did in [B, MEMBER] {
        let list = format!("at://{did}/app.bsky.graph.list/l");
        sql += &format!("INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES ('{list}', '{did}', 'l', 'c', '3k', 'n', 'app.bsky.graph.defs#modlist', '{{}}', now());");
        sql += &format!("INSERT INTO list_items (uri, author_did, rkey, cid, rev, list_uri, subject_did, created_at) VALUES ('at://{did}/app.bsky.graph.listitem/1', '{did}', '1', 'c', '3k', '{list}', '{D}', now());");
        for t in ["list_optouts", "list_blocks"] {
            sql += &format!("INSERT INTO {t} (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES ('at://{did}/app.bsky.graph.{t}/1', '{did}', '1', 'c', '3k', '{list}', now());");
        }
    }
    // Follows: B → D goes; B → member and member → D stay.
    for (author, subject, r) in [(B, D, "1"), (B, MEMBER, "2"), (MEMBER, D, "1")] {
        sql += &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{author}/app.bsky.graph.follow/{r}', '{author}', '{subject}', '3k', now());");
    }
    sql += &format!("INSERT INTO post_counts (post_uri, like_count) VALUES ('{}', 1) ON CONFLICT DO NOTHING;", post(D, "p9"));
    for (tag, h) in [("a", 1), ("a", 2), ("b", 1), ("c", 3), ("c", 4)] {
        sql += &format!("INSERT INTO tag_activity_hourly (tag, hour, posts) VALUES ('{tag}', '2026-10-14T0{h}:00:00Z', 1);");
    }
    for i in 1..=2 {
        sql += &format!("INSERT INTO starter_pack_counts (pack_uri, all_time_source, refreshed_at) VALUES ('at://{B}/app.bsky.graph.starterpack/{i}', 'x', now());");
    }
    // Kept tables.
    sql += &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{B}/app.bsky.graph.block/1', '{B}', '{MEMBER}', '3k', now());");
    sql += &format!("INSERT INTO actors (did, display_name) VALUES ('{B}', 'b'), ('{MEMBER}', 'm');");
    sql += &format!("INSERT INTO labels (subject, val) VALUES ('{B}', 'porn');");
    sql += &format!("INSERT INTO record_tombstones (uri, rev) VALUES ('{}', '3k');", post(B, "gone"));
    sql += &format!("INSERT INTO member_post_refs (uri, author_did, rev, created_at) VALUES ('{}', '{MEMBER}', '3k', now());", post(MEMBER, "p3"));
    sql += &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{MEMBER}', '{B}');");
    sql
}

/// The rows the sweep keeps, as text, table by table.
const KEPT: [(&str, &str); 14] = [
    ("likes", "author_did = 'did:plc:cccccccccccccccccccccccc'"),
    ("reposts", "author_did = 'did:plc:cccccccccccccccccccccccc'"),
    ("lists", "creator_did = 'did:plc:cccccccccccccccccccccccc'"),
    ("list_items", "author_did = 'did:plc:cccccccccccccccccccccccc'"),
    ("list_optouts", "author_did = 'did:plc:cccccccccccccccccccccccc'"),
    ("list_blocks", "author_did = 'did:plc:cccccccccccccccccccccccc'"),
    ("follows", "'did:plc:cccccccccccccccccccccccc' IN (author_did, subject_did)"),
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
    set_memory(&pi, PublicState { mode: PublicMode::On, generation: 1, last_fill_since: Some(filled) }).await;
    let Some(Spawn::Sweep { generation: 2, .. }) = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn else {
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
    for t in ["likes", "reposts", "list_items", "list_optouts", "list_blocks"] {
        assert_eq!(count(p, &format!("SELECT count(*) FROM {t} WHERE author_did <> '{MEMBER}'")).await, 0, "{t} is thinned");
    }
    assert_eq!(count(p, &format!("SELECT count(*) FROM lists WHERE creator_did <> '{MEMBER}'")).await, 0);
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
    let task = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn.expect("sweep");
    let (reached, resume) = pi.hooks.arm("sweep_window");
    drop(pi.spawn(task));
    reached.notified().await;
    exec(p, &format!("DELETE FROM feed_items WHERE uri IN ('at://{B}/app.bsky.feed.post/f1', 'at://{B}/app.bsky.feed.post/f2')")).await;
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
    let task = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn.expect("sweep");
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
    let task = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn.expect("sweep");
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
    let task = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn.expect("sweep");
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
    let Some(Spawn::Sweep { generation: 1, sweep }) = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn else {
        panic!("expected the sweep");
    };
    assert!(matches!(pi.sweep_batch(1, 0, None, 3).await.expect("batch"), Batch::Advanced { deleted: 3, .. }));
    let before = transition(p).await;
    let applied = pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("pending");
    assert_eq!((applied.action, applied.spawn), ("pending_set", None));
    let after = transition(p).await;
    assert_eq!(after["pending"], "community_scoped");
    assert_eq!((&after["sweep"], &after["generation"]), (&before["sweep"], &before["generation"]));
    assert_eq!(setting(p).await.0, json!("off"));
    // The sweep runs to its end, which spawns the fill.
    let resumed = SweepState { after: before["sweep"]["after"].clone(), ..sweep };
    pi.clone().run_sweep(1, resumed).await;
    let t = transition(p).await;
    assert_eq!((&t["phase"], &t["generation"], &t["pending"]), (&json!("filling"), &json!(2), &Value::Null));
    assert_eq!((&t["fill"]["stage"], &t["fill"]["part"], &t["fill"]["after"], &t["fill"]["enqueued"]), (&json!("enqueue"), &json!("members"), &json!(""), &json!(0)));
    assert_eq!(t["last_fill_since"], t["fill"]["since"]);
    assert_eq!(setting(p).await.0, json!("community_scoped"));
    assert_eq!(pi.hooks.spawned().first(), Some(&"fill"), "the sweep's end spawned the fill");
    eventually("on", async || scope.public_mode() == PublicMode::On).await;
    assert_eq!(pi.hooks.spawned(), ["fill", "checker"]);

    // Off, on (pending), off (cleared): the phase ends off.
    let Some(Spawn::Sweep { generation, sweep }) = pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").spawn else {
        panic!("expected the sweep");
    };
    assert_eq!(pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("on").action, "pending_set");
    assert_eq!(pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").action, "pending_cleared");
    assert_eq!(pi.apply_scope(ScopeValue::Off, "cli").await.expect("off").action, "no_op");
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
    let switch = tokio::spawn(async move { switcher.apply_scope(ScopeValue::Off, "cli").await });
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
    exec(p, "INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES \
             ('at://did:plc:m1/app.bsky.feed.like/1', 'did:plc:m1', 'at://did:plc:m2/app.bsky.feed.post/1', '3k', now())").await;
    exec(p, "UPDATE post_counts SET like_count = 99").await;
    let s = PublicModeSettings { fill_max_wait: Duration::from_secs(2), fill_batch: 1_000, ..settings() };
    let (pi, scope) = boot(p, s).await;
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None }).await;
    let task = pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("on").spawn.expect("fill");
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
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None }).await;
    let task = pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("on").spawn.expect("fill");
    let (reached, resume) = pi.hooks.arm("fill_write");
    let fill = pi.spawn(task);
    reached.notified().await;
    let switcher = pi.clone();
    let switch = tokio::spawn(async move { switcher.apply_scope(ScopeValue::Off, "cli").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!switch.is_finished(), "the command waits for the batch");
    resume.notify_one();
    assert_eq!(switch.await.expect("join").expect("off").generation, 3);
    tokio::time::timeout(Duration::from_secs(10), fill).await.expect("the pass exits").expect("join");
    assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'resync'").await, 2, "one batch committed, no more");
    assert_eq!(scope.public_mode(), PublicMode::Draining);

    // The database barrier.
    exec(p, "DELETE FROM backfill_jobs").await;
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 3, last_fill_since: None }).await;
    let task = pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("on").spawn.expect("fill");
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
    set_memory(&pi, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None }).await;
    pi.apply_scope(ScopeValue::CommunityScoped, "cli").await.expect("on");
    let checker = pi.spawn(Spawn::Checker { generation: 2, waiting_since: now() });
    tokio::time::sleep(Duration::from_millis(100)).await;
    pi.apply_scope(ScopeValue::Off, "cli").await.expect("off");
    // The sweep's end with `pending`, by hand: Filling at g + 2.
    let t = Transition::filling(4, now());
    exec(p, &format!("UPDATE worker_state SET value = '{}' WHERE key = 'public_transition'", t.to_json())).await;
    set_memory(&pi, PublicState { mode: PublicMode::Filling, generation: 4, last_fill_since: t.last_fill_since }).await;
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
    pi.clone().run_sweep(3, SweepState::first(now())).await;
    assert_eq!(count(p, "SELECT count(*) FROM feed_items").await, 2);
    db.drop_db().await;
}
