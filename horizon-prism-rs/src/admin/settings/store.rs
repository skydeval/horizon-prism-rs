//! Saved settings in `operator_settings` (admin-ui design §8.2–§8.4): the
//! startup rule with its set-aside, and the `settings set` command's
//! transaction. `indexing.public_scope` keeps its own row and rule
//! (public-indexing design §3.3) and is never touched here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};

use super::overlay;
use super::registry;
use crate::config::{AppConfig, ConfigError, EnvConfig, FileConfig};

/// The public-indexing switch's row, which this module never touches.
pub const PUBLIC_SCOPE_KEY: &str = "indexing.public_scope";

/// `operator_settings_set_aside`: saved settings not used at the last start.
pub const METRIC_SET_ASIDE: &str = "operator_settings_set_aside";

/// One saved setting.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Row {
    pub key: String,
    pub value: Value,
    pub file_value: Option<Value>,
    pub version: i64,
    pub set_aside: Option<Value>,
    pub file_notice: bool,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}

const COLUMNS: &str = "key, value, file_value, version, set_aside, file_notice, updated_at, updated_by";

/// Every saved setting but the public-indexing switch, by key.
///
/// # Errors
///
/// Database errors.
pub async fn rows(pool: &PgPool) -> Result<Vec<Row>, sqlx::Error> {
    sqlx::query_as(&format!("SELECT {COLUMNS} FROM operator_settings WHERE key <> $1 ORDER BY key"))
        .bind(PUBLIC_SCOPE_KEY)
        .fetch_all(pool)
        .await
}

/// `file_value` as stored: the file's value at `key` (written or default)
/// and whether the file writes it.
#[must_use]
pub fn file_value(file_json: &Value, table: &toml::Table, key: &str) -> Value {
    json!({ "value": registry::value_at(file_json, key), "explicit": overlay::has(table, key) })
}

/// A set-aside's record: the problem and when.
fn aside(problem: &str) -> Value {
    json!({ "problem": problem, "at": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true) })
}

/// The configuration, validated with the environment, or its problem in
/// words.
fn usable(file: FileConfig, env: &EnvConfig) -> Result<FileConfig, String> {
    match AppConfig::from_file(file, env.clone()) {
        Ok(cfg) => Ok(cfg.file),
        Err(ConfigError::Invalid(problems)) => Err(problems.join("; ")),
        Err(e) => Err(e.to_string()),
    }
}

/// The overrides of `rows`, each unit completed with the file's values of
/// the unit's other keys, so a unit is always applied whole (§8.2).
fn completed(overrides: &BTreeMap<String, Value>, file_json: &Value, networks: &[String]) -> Vec<(String, Value)> {
    let mut out = overrides.clone();
    let units: BTreeSet<String> = overrides.keys().map(|k| registry::unit_of(k)).collect();
    for unit in units {
        for key in registry::unit_keys(&unit, networks) {
            out.entry(key.clone()).or_insert_with(|| registry::value_at(file_json, &key));
        }
    }
    out.into_iter().collect()
}

fn network_names(file: &FileConfig) -> Vec<String> {
    file.constellation.networks.iter().map(|n| n.name.clone()).collect()
}

/// What startup did with the saved settings.
#[derive(Debug, Clone, PartialEq)]
pub struct Startup {
    /// The effective configuration: the file, the overrides kept, the
    /// environment, validated.
    pub cfg: AppConfig,
    /// Keys applied.
    pub applied: Vec<String>,
    /// Keys set aside, with the problem.
    pub set_aside: Vec<(String, String)>,
    /// Keys whose file value changed under them.
    pub file_changed: Vec<String>,
}

/// The startup rule (§8.4): the saved value wins over the file (C-A7); a
/// file change under a saved value is recorded and flagged; the overrides
/// are applied unit by unit in key order, each kept only if the
/// configuration still parses and validates with it and the units kept so
/// far; a unit that does not is set aside, never deleted.
///
/// `cfg` is the file and environment, already validated; `table` the
/// file's TOML.
///
/// # Errors
///
/// Database errors.
pub async fn at_startup(pool: &PgPool, cfg: AppConfig, table: &toml::Table) -> Result<Startup, sqlx::Error> {
    let saved = rows(pool).await?;
    let file_json = overlay::json(&cfg.file);
    let networks = network_names(&cfg.file);
    let mut file_changed = Vec::new();
    for row in &saved {
        let now = file_value(&file_json, table, &row.key);
        match &row.file_value {
            Some(was) if overlay::same(was, &now) => {}
            // First sight: recorded, nothing to say.
            None => {
                sqlx::query("UPDATE operator_settings SET file_value = $2 WHERE key = $1").bind(&row.key).bind(&now).execute(pool).await?;
            }
            Some(_) => {
                let mut tx = pool.begin().await?;
                sqlx::query("UPDATE operator_settings SET file_value = $2, file_notice = true WHERE key = $1")
                    .bind(&row.key)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
                history(&mut tx, &row.key, Some(&row.value), Some(&row.value), "file", None).await?;
                tx.commit().await?;
                tracing::warn!(
                    event = "operator_setting_file_changed",
                    key = %row.key,
                    file_value = %now["value"],
                    saved = %row.value,
                    "app.toml changed a setting saved in the admin UI; the saved value still applies"
                );
                file_changed.push(row.key.clone());
            }
        }
    }
    // Units in key order.
    let mut units: BTreeMap<String, Vec<&Row>> = BTreeMap::new();
    for row in &saved {
        units.entry(registry::unit_of(&row.key)).or_default().push(row);
    }
    let mut kept: BTreeMap<String, Value> = BTreeMap::new();
    let mut effective = cfg.file.clone();
    let (mut applied, mut set_aside) = (Vec::new(), Vec::new());
    for unit_rows in units.values() {
        let unknown = unit_rows.iter().find(|r| registry::lookup(&r.key, &networks).is_none());
        let outcome = if let Some(r) = unknown {
            Err(format!("{} is no longer a setting Prism offers", r.key))
        } else {
            let mut trial = kept.clone();
            for r in unit_rows {
                trial.insert(r.key.clone(), r.value.clone());
            }
            overlay::build(table, &completed(&trial, &file_json, &networks), &cfg.file)
                .and_then(|file| usable(file, &cfg.env))
                .map(|file| (file, trial))
        };
        match outcome {
            Ok((file, trial)) => {
                kept = trial;
                effective = file;
                for r in unit_rows {
                    applied.push(r.key.clone());
                    if r.set_aside.is_some() {
                        sqlx::query("UPDATE operator_settings SET set_aside = NULL WHERE key = $1").bind(&r.key).execute(pool).await?;
                    }
                }
            }
            Err(problem) => {
                for r in unit_rows {
                    sqlx::query("UPDATE operator_settings SET set_aside = $2 WHERE key = $1").bind(&r.key).bind(aside(&problem)).execute(pool).await?;
                    tracing::error!(event = "operator_setting_set_aside", key = %r.key, %problem, "a setting saved in the admin UI could not be used and was set aside");
                    set_aside.push((r.key.clone(), problem.clone()));
                }
            }
        }
    }
    metrics::gauge!(METRIC_SET_ASIDE).set(f64::from(u32::try_from(set_aside.len()).unwrap_or(u32::MAX)));
    if !applied.is_empty() {
        tracing::info!(event = "operator_settings_applied", settings = applied.len(), "settings saved in the admin UI are in use");
    }
    Ok(Startup { cfg: AppConfig { file: effective, env: cfg.env }, applied, set_aside, file_changed })
}

async fn history(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    old: Option<&Value>,
    new: Option<&Value>,
    by: &str,
    command_id: Option<i64>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO operator_settings_history (key, old_value, new_value, changed_by, command_id) VALUES ($1, $2, $3, $4, $5)")
        .bind(key)
        .bind(old)
        .bind(new)
        .bind(by)
        .bind(command_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// One change of a `settings set`: a value, or `None` to remove the saved
/// value (back to the file's or the default).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub key: String,
    pub value: Option<Value>,
    /// The row's `version` the change was made against; 0 for no row.
    pub base_version: i64,
}

/// A `settings set` that was applied.
#[derive(Debug, Clone)]
pub struct Saved {
    /// The command's result.
    pub result: Value,
    /// The configuration with every saved value, for the live swap.
    pub candidate: FileConfig,
}

/// The `settings set` command (§8.3): check every key and base version,
/// read `app.toml` fresh, build and validate the configuration with every
/// saved value and these changes, then in one transaction write the rows
/// and their history. `Ok(Err(_))` is a refusal in plain words; nothing
/// changed.
///
/// # Errors
///
/// Database errors.
pub async fn set(
    pool: &PgPool,
    config_path: &Path,
    env: &EnvConfig,
    changes: &[Change],
    by: &str,
    command_id: Option<i64>,
) -> Result<Result<Saved, String>, sqlx::Error> {
    let text = match std::fs::read_to_string(config_path) {
        Ok(t) => t,
        Err(e) => return Ok(Err(format!("app.toml on the server could not be read: {e}; nothing was saved"))),
    };
    let file = match FileConfig::parse(&text).and_then(|f| AppConfig::from_file(f, env.clone())) {
        Ok(cfg) => cfg.file,
        Err(e) => return Ok(Err(format!("app.toml on the server has a problem: {e}; nothing was saved"))),
    };
    let table: toml::Table = match text.parse() {
        Ok(t) => t,
        Err(e) => return Ok(Err(format!("app.toml on the server has a problem: {e}; nothing was saved"))),
    };
    let networks = network_names(&file);
    for c in changes {
        if c.key == PUBLIC_SCOPE_KEY || registry::lookup(&c.key, &networks).is_none() {
            return Ok(Err(format!("{} is not a setting that can be changed here; nothing was saved", c.key)));
        }
    }
    let mut tx = pool.begin().await?;
    let saved: Vec<Row> = sqlx::query_as(&format!("SELECT {COLUMNS} FROM operator_settings WHERE key <> $1 ORDER BY key FOR UPDATE"))
        .bind(PUBLIC_SCOPE_KEY)
        .fetch_all(&mut *tx)
        .await?;
    let by_key: BTreeMap<&str, &Row> = saved.iter().map(|r| (r.key.as_str(), r)).collect();
    for c in changes {
        let version = by_key.get(c.key.as_str()).map_or(0, |r| r.version);
        if version != c.base_version {
            return Ok(Err("This setting was changed by someone else since you opened the page. Reload the page and try again; nothing was saved.".to_string()));
        }
    }
    // Every saved value in use, the set-aside units these changes touch
    // (so a fix is validated whole), then the changes.
    let touched: BTreeSet<String> = changes.iter().map(|c| registry::unit_of(&c.key)).collect();
    let mut overrides: BTreeMap<String, Value> = saved
        .iter()
        .filter(|r| r.set_aside.is_none() || touched.contains(&registry::unit_of(&r.key)))
        .filter(|r| registry::lookup(&r.key, &networks).is_some())
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect();
    for c in changes {
        match &c.value {
            Some(v) => overrides.insert(c.key.clone(), v.clone()),
            None => overrides.remove(&c.key),
        };
    }
    let file_json = overlay::json(&file);
    let candidate = match overlay::build(&table, &completed(&overrides, &file_json, &networks), &file).and_then(|f| usable(f, env)) {
        Ok(f) => f,
        Err(problem) => return Ok(Err(format!("Not saved: {problem}"))),
    };
    let candidate_json = overlay::json(&candidate);
    let mut changed = Vec::new();
    for c in changes {
        let old = by_key.get(c.key.as_str()).map(|r| &r.value);
        let applies = if super::live::is_live(&c.key) { "now" } else { "restart" };
        match &c.value {
            Some(_) => {
                // The value as the configuration reads it back.
                let value = registry::value_at(&candidate_json, &c.key);
                sqlx::query(
                    "INSERT INTO operator_settings (key, value, file_value, updated_by, version) VALUES ($1, $2, $3, $4, 1) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, file_value = EXCLUDED.file_value, \
                     updated_by = EXCLUDED.updated_by, updated_at = now(), version = operator_settings.version + 1, \
                     set_aside = NULL, file_notice = false",
                )
                .bind(&c.key)
                .bind(&value)
                .bind(file_value(&file_json, &table, &c.key))
                .bind(by)
                .execute(&mut *tx)
                .await?;
                history(&mut tx, &c.key, old, Some(&value), by, command_id).await?;
                changed.push(json!({ "key": c.key, "value": value, "applies": applies }));
            }
            None => {
                sqlx::query("DELETE FROM operator_settings WHERE key = $1").bind(&c.key).execute(&mut *tx).await?;
                if old.is_some() {
                    history(&mut tx, &c.key, old, None, by, command_id).await?;
                }
                changed.push(json!({ "key": c.key, "reset": true, "applies": applies }));
            }
        }
    }
    // A set-aside unit these changes made valid again is no longer set aside.
    for r in saved.iter().filter(|r| r.set_aside.is_some() && touched.contains(&registry::unit_of(&r.key))) {
        sqlx::query("UPDATE operator_settings SET set_aside = NULL WHERE key = $1").bind(&r.key).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    for c in &changed {
        tracing::info!(event = "operator_setting_changed", key = %c["key"], by, "a setting was changed");
    }
    Ok(Ok(Saved { result: json!({ "changed": changed }), candidate }))
}

/// What the pages show at the top: saved changes waiting for a restart,
/// and saved settings not used at the last start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub waiting: usize,
    pub set_aside: usize,
    pub file_notices: usize,
}

/// The keys whose saved value is not the one the process started with
/// (§8.5): a restart key whose saved effective value (the saved value, or
/// the file's) differs from the running one, and whose unit is not set
/// aside.
#[must_use]
pub fn waiting(saved: &[Row], file_json: &Value, running_json: &Value, networks: &[String]) -> Vec<String> {
    let by_key: BTreeMap<&str, &Row> = saved.iter().map(|r| (r.key.as_str(), r)).collect();
    registry::keys(networks)
        .into_iter()
        .filter(|k| !super::live::is_live(k))
        .filter(|k| {
            let saved_value = match by_key.get(k.as_str()) {
                Some(r) if r.set_aside.is_some() => return false,
                Some(r) => r.value.clone(),
                None => registry::value_at(file_json, k),
            };
            !overlay::same(&saved_value, &registry::value_at(running_json, k))
        })
        .collect()
}

/// [`Counts`] from the saved rows.
#[must_use]
pub fn counts(saved: &[Row], file_json: &Value, running_json: &Value, networks: &[String]) -> Counts {
    Counts {
        waiting: waiting(saved, file_json, running_json, networks).len(),
        set_aside: saved.iter().filter(|r| r.set_aside.is_some()).count(),
        file_notices: saved.iter().filter(|r| r.file_notice).count(),
    }
}
