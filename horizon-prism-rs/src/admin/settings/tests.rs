//! The saved settings against a database (admin-ui design §15: T-A9,
//! T-A10, T-A10b, T-A17): `settings set`'s transaction and the startup
//! rule, with `app.toml` on disk.

use std::path::PathBuf;

use serde_json::{Value, json};
use sqlx::PgPool;

use super::store::{self, Change};
use crate::config::{AppConfig, EnvConfig};
use crate::test_support::{TestDb, fresh_db};

const OPERATOR: &str = "did:plc:oooooooooooooooooooooooo";

/// An environment for validation; `supervised` and a Farsight token as
/// asked.
pub(crate) fn env_lookup(supervised: bool, farsight: bool) -> impl Fn(&str) -> Option<String> {
    move |var: &str| match var {
        "APP_PUBLIC_HOSTNAME" => Some("prism.example.com".to_string()),
        "APP_DATABASE_URL" => Some("postgres://prism@localhost/prism".to_string()),
        "APP_REDIS_URL" => Some("redis://localhost".to_string()),
        "APP_SERVICE_SIGNING_KEY_HEX" | "APP_CORRELATION_KEY_HEX" | "APP_PRIVATE_CURSOR_KEY_HEX" => Some("11".repeat(32)),
        "APP_SUPERVISED" if supervised => Some("1".to_string()),
        "FARSIGHT_TOKEN" if farsight => Some("token".to_string()),
        _ => None,
    }
}

/// [`env_lookup`]'s environment.
pub(crate) fn env(supervised: bool, farsight: bool) -> EnvConfig {
    EnvConfig::from_lookup(&env_lookup(supervised, farsight)).expect("env")
}

pub(crate) struct Fixture {
    pub db: TestDb,
    pub dir: PathBuf,
    pub path: PathBuf,
}

impl Fixture {
    pub fn pool(&self) -> &PgPool {
        &self.db.pool
    }

    /// Write `app.toml`.
    pub fn write(&self, text: &str) {
        std::fs::write(&self.path, text).expect("write app.toml");
    }

    /// The configuration of the file on disk, and its table.
    pub fn load(&self, farsight: bool) -> (AppConfig, toml::Table) {
        let text = std::fs::read_to_string(&self.path).expect("read");
        (AppConfig::from_parts(&text, &env_lookup(false, farsight)).expect("valid file"), text.parse().expect("toml"))
    }

    pub async fn finish(self) {
        let _ = std::fs::remove_dir_all(&self.dir);
        self.db.drop_db().await;
    }
}

pub(crate) async fn fixture(name: &str, text: &str) -> Option<Fixture> {
    let db = fresh_db(name).await?;
    let dir = std::env::temp_dir().join(format!("hprs-settings-{}", rand::random::<u32>()));
    std::fs::create_dir_all(&dir).expect("dir");
    let f = Fixture { db, path: dir.join("app.toml"), dir };
    f.write(text);
    Some(f)
}

fn set(key: &str, value: Value, base_version: i64) -> Change {
    Change { key: key.to_string(), value: Some(value), base_version }
}

type RowTuple = (Value, i64, Option<Value>, bool, String, Option<Value>);

async fn row(pool: &PgPool, key: &str) -> Option<RowTuple> {
    sqlx::query_as("SELECT value, version, set_aside, file_notice, updated_by, file_value FROM operator_settings WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await
        .expect("row")
}

async fn history(pool: &PgPool, key: &str) -> Vec<(Option<Value>, Option<Value>, String, Option<i64>)> {
    sqlx::query_as("SELECT old_value, new_value, changed_by, command_id FROM operator_settings_history WHERE key = $1 ORDER BY id")
        .bind(key)
        .fetch_all(pool)
        .await
        .expect("history")
}

/// T-A9, T-A10b: a group of two keys is saved in one transaction with two
/// history rows and its writer; a stale `base_version` and a value that
/// fails validation each change nothing; a unit is completed with the
/// file's values; a reset removes the row; a broken file is named.
#[tokio::test]
async fn t_a9_settings_set() {
    let Some(f) = fixture("settings_t_a9", "").await else {
        return;
    };
    let (p, env) = (f.pool(), env(false, false));
    let saved = store::set(p, &f.path, &env, &[set("community.max_members", json!(5), 0), set("retention.tombstone_days", json!(3), 0)], OPERATOR, Some(42))
        .await
        .expect("db")
        .expect("saved");
    assert_eq!((saved.candidate.community.max_members, saved.candidate.retention.tombstone_days), (5, 3));
    assert_eq!(saved.result["changed"].as_array().map(Vec::len), Some(2));
    for (key, v) in [("community.max_members", json!(5)), ("retention.tombstone_days", json!(3))] {
        let (value, version, aside, notice, by, file_value) = row(p, key).await.expect("row");
        assert_eq!((value, version, aside, notice, by.as_str()), (v.clone(), 1, None, false, OPERATOR), "{key}");
        assert_eq!(file_value.expect("file value")["explicit"], json!(false));
        assert_eq!(history(p, key).await, [(None, Some(v), OPERATOR.to_string(), Some(42))]);
    }
    // Made against an older version: nothing changes.
    let stale = store::set(p, &f.path, &env, &[set("community.max_members", json!(6), 0)], "cli", None).await.expect("db");
    assert!(stale.as_ref().is_err_and(|e| e.contains("changed by someone else")), "{stale:?}");
    assert_eq!(row(p, "community.max_members").await.map(|r| r.0), Some(json!(5)));
    // A value the configuration refuses: nothing changes, the problem is said.
    let bad = store::set(p, &f.path, &env, &[set("private_read.max_chain", json!(500), 0), set("community.max_members", json!(7), 1)], "cli", None)
        .await
        .expect("db");
    assert!(bad.as_ref().is_err_and(|e| e.starts_with("Not saved") && e.contains("max_chain")), "{bad:?}");
    assert_eq!(row(p, "private_read.max_chain").await, None);
    assert_eq!(row(p, "community.max_members").await.map(|r| (r.0, r.1)), Some((json!(5), 1)));
    // Half of a unit: completed with the file's other value, so it
    // validates whole; the decimal is stored as the configuration reads it.
    let saved = store::set(p, &f.path, &env, &[set("rate_limits.anon_read.per_sec", json!(12), 0)], "cli", None).await.expect("db").expect("saved");
    assert_eq!((saved.candidate.rate_limits.anon_read.per_sec, saved.candidate.rate_limits.anon_read.burst), (12.0, 60));
    assert_eq!(row(p, "rate_limits.anon_read.per_sec").await.map(|r| r.0), Some(json!(12.0)));
    assert_eq!(row(p, "rate_limits.anon_read.burst").await, None, "only the changed key is saved");
    // A reset: the row goes, the history keeps the old value.
    store::set(p, &f.path, &env, &[Change { key: "community.max_members".into(), value: None, base_version: 1 }], "cli", None)
        .await
        .expect("db")
        .expect("reset");
    assert_eq!(row(p, "community.max_members").await, None);
    assert_eq!(history(p, "community.max_members").await.last(), Some(&(Some(json!(5)), None, "cli".to_string(), None)));
    // The file on the server broken: named, nothing saved.
    f.write("[community\n");
    let broken = store::set(p, &f.path, &env, &[set("community.max_members", json!(9), 0)], "cli", None).await.expect("db");
    assert!(broken.as_ref().is_err_and(|e| e.starts_with("app.toml on the server has a problem")), "{broken:?}");
    assert_eq!(row(p, "community.max_members").await, None);
    // Not a setting: refused.
    f.write("");
    let fixed = store::set(p, &f.path, &env, &[set("http.bind", json!("0.0.0.0:1"), 0)], "cli", None).await.expect("db");
    assert!(fixed.is_err());
    f.finish().await;
}

/// T-A10: the saved value wins over a file change, which is recorded,
/// flagged and kept in the history once; "use the file's value" removes
/// the override; an override a changed rule makes invalid is set aside,
/// Prism starts, the others apply, and a fix clears it; the fallback pair
/// is set aside as a pair, never split.
#[tokio::test]
async fn t_a10_startup_rule() {
    let Some(f) = fixture("settings_t_a10", "").await else {
        return;
    };
    let (p, env) = (f.pool(), env(false, false));
    store::set(p, &f.path, &env, &[set("community.max_members", json!(5), 0), set("retention.tombstone_days", json!(3), 0)], "cli", None)
        .await
        .expect("db")
        .expect("saved");
    // The file changes under the first.
    f.write("[community]\nmax_members = 7\n");
    let (cfg, table) = f.load(false);
    let started = store::at_startup(p, cfg, &table).await.expect("startup");
    assert_eq!(started.cfg.file.community.max_members, 5, "the saved value wins (C-A7)");
    assert_eq!(started.cfg.file.retention.tombstone_days, 3);
    assert_eq!(started.file_changed, ["community.max_members"]);
    let (_, _, _, notice, _, file_value) = row(p, "community.max_members").await.expect("row");
    assert!(notice);
    assert_eq!(file_value, Some(json!({"value": 7, "explicit": true})));
    assert_eq!(history(p, "community.max_members").await.last(), Some(&(Some(json!(5)), Some(json!(5)), "file".to_string(), None)));
    // The same file again: nothing new.
    let (cfg, table) = f.load(false);
    let again = store::at_startup(p, cfg, &table).await.expect("startup");
    assert!(again.file_changed.is_empty());
    assert_eq!(history(p, "community.max_members").await.len(), 2);
    // Use the file's value.
    store::set(p, &f.path, &env, &[Change { key: "community.max_members".into(), value: None, base_version: 1 }], "cli", None)
        .await
        .expect("db")
        .expect("reset");
    let (cfg, table) = f.load(false);
    assert_eq!(store::at_startup(p, cfg, &table).await.expect("startup").cfg.file.community.max_members, 7);

    // A value a rule now refuses (written as an older version could have)
    // and the fallback pair over a file with no Farsight: both set aside,
    // Prism starts, the other override still applies.
    for (key, value) in [("private_read.max_chain", json!(500)), ("farsight.fallback.enabled", json!(true)), ("farsight.fallback.network", json!("bluesky"))] {
        sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ($1, $2, 'cli')").bind(key).bind(value).execute(p).await.expect("insert");
    }
    let (cfg, table) = f.load(false);
    let started = store::at_startup(p, cfg, &table).await.expect("Prism starts");
    assert_eq!(started.cfg.file.private_read.max_chain, 16);
    assert!(!started.cfg.file.farsight.fallback.enabled);
    assert_eq!(started.cfg.file.retention.tombstone_days, 3, "the other override applies");
    let aside: Vec<&str> = started.set_aside.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(aside, ["farsight.fallback.enabled", "farsight.fallback.network", "private_read.max_chain"], "the pair together");
    for key in aside {
        assert!(row(p, key).await.expect("row").2.is_some(), "{key} is marked and kept");
    }
    let counts = store::counts(&store::rows(p).await.expect("rows"), &Value::Null, &Value::Null, &[]);
    assert_eq!(counts.set_aside, 3);
    // Fixed in the UI: no longer set aside, and applied at the next start.
    store::set(p, &f.path, &env, &[set("private_read.max_chain", json!(50), 1)], "cli", None).await.expect("db").expect("fixed");
    assert_eq!(row(p, "private_read.max_chain").await.map(|r| (r.0, r.2)), Some((json!(50), None)));
    let (cfg, table) = f.load(false);
    assert_eq!(store::at_startup(p, cfg, &table).await.expect("startup").cfg.file.private_read.max_chain, 50);
    f.finish().await;
}

/// T-A17 at the overlay: with `farsight.memory.enabled = false` in the
/// file and `true` saved, the effective configuration the startup
/// truncation reads has the memory on; with nothing saved, off.
#[tokio::test]
async fn t_a17_memory_switch_follows_the_saved_value() {
    let file = "[farsight]\nurl = \"https://farsight.example.net\"\n[farsight.memory]\nenabled = false\n";
    let Some(f) = fixture("settings_t_a17", file).await else {
        return;
    };
    let p = f.pool();
    let (cfg, table) = f.load(true);
    assert!(!store::at_startup(p, cfg, &table).await.expect("startup").cfg.file.farsight.memory.enabled);
    store::set(p, &f.path, &env(false, true), &[set("farsight.memory.enabled", json!(true), 0)], "cli", None)
        .await
        .expect("db")
        .expect("saved");
    let (cfg, table) = f.load(true);
    assert!(store::at_startup(p, cfg, &table).await.expect("startup").cfg.file.farsight.memory.enabled);
    f.finish().await;
}

/// §8.5: "waiting" is a restart key whose saved value is not the running
/// one; a live key never waits, and a set-aside one is listed apart.
#[test]
fn waiting_for_a_restart() {
    let running = super::overlay::json(&crate::config::FileConfig::default());
    let at = chrono::Utc::now();
    let r = |key: &str, value: Value, aside: bool| store::Row {
        key: key.to_string(),
        value,
        file_value: None,
        version: 1,
        set_aside: aside.then(|| json!({"problem": "x"})),
        file_notice: false,
        updated_at: at,
        updated_by: "cli".to_string(),
    };
    let rows = vec![
        r("retention.tombstone_days", json!(3), false),
        r("community.max_members", json!(5), false),
        r("private_read.max_chain", json!(500), true),
        r("rate_limits.anon_read.per_sec", json!(30), false),
    ];
    assert_eq!(store::waiting(&rows, &running, &running, &[]), ["retention.tombstone_days"]);
    let c = store::counts(&rows, &running, &running, &[]);
    assert_eq!((c.waiting, c.set_aside), (1, 1));
}
