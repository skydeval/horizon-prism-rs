//! `admin settings list` and `admin settings get <key>` (admin-ui design
//! §7, §8): the settings the admin UI edits, with the value saved there
//! and the one `app.toml` gives. Read-only; `settings set` and `settings
//! reset` go through `serve`.

use serde_json::Value;
use sqlx::PgPool;

use super::table;
use crate::admin::settings::live::is_live;
use crate::admin::settings::{overlay, registry, store};
use crate::config::FileConfig;

fn compact(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

/// The registry's context for `file`.
#[must_use]
pub fn context(file: &FileConfig) -> registry::Context {
    registry::Context {
        networks: file.constellation.networks.iter().map(|n| n.name.clone()).collect(),
        proxied: file.services.iter().filter(|(_, s)| s.proxy_url.is_some()).map(|(name, _)| name.clone()).collect(),
    }
}

/// Every setting: key, when it applies, the saved value, `app.toml`'s, and
/// a note when the saved value was set aside or the file changed under it.
///
/// # Errors
///
/// Database errors.
pub async fn render_list(pool: &PgPool, file: &FileConfig) -> Result<String, sqlx::Error> {
    let saved = store::rows(pool).await?;
    let file_json = overlay::json(file);
    let ctx = context(file);
    let rows: Vec<Vec<String>> = registry::keys(&ctx.networks)
        .into_iter()
        .map(|key| {
            let row = saved.iter().find(|r| r.key == key);
            let note = match row {
                Some(r) if r.set_aside.is_some() => "set aside",
                Some(r) if r.file_notice => "app.toml changed",
                _ => "",
            };
            vec![
                key.clone(),
                if is_live(&key) { "now" } else { "restart" }.to_string(),
                row.map_or_else(|| "-".to_string(), |r| compact(&r.value)),
                compact(&registry::value_at(&file_json, &key)),
                note.to_string(),
            ]
        })
        .collect();
    Ok(table(&["key", "applies", "saved", "app.toml", "note"], &rows))
}

/// One setting in full.
///
/// # Errors
///
/// A printable message.
pub async fn render_get(pool: &PgPool, file: &FileConfig, key: &str) -> Result<String, String> {
    let ctx = context(file);
    let (section, field) = registry::lookup(key, &ctx.networks).ok_or_else(|| format!("{key} is not a setting the admin UI changes"))?;
    let saved = store::rows(pool).await.map_err(|e| e.to_string())?;
    let row = saved.iter().find(|r| r.key == key);
    let file_json = overlay::json(file);
    let default = registry::value_at(&overlay::json(&FileConfig::default()), key);
    let words = |v: &Value| registry::words(&field.kind, v, &ctx);
    let mut out = vec![
        format!("{} ({})", section.title, field.label),
        section.help.clone(),
        format!("applies: {}", if section.live { "at once" } else { "after a restart" }),
        format!("default: {} ({})", words(&default), compact(&default)),
        format!("app.toml: {} ({})", words(&registry::value_at(&file_json, key)), compact(&registry::value_at(&file_json, key))),
    ];
    match row {
        Some(r) => {
            out.push(format!("saved: {} ({}), version {}, by {} at {}", words(&r.value), compact(&r.value), r.version, r.updated_by, super::when(Some(r.updated_at))));
            if let Some(a) = &r.set_aside {
                out.push(format!("set aside at the last start: {}", a.get("problem").and_then(Value::as_str).unwrap_or("?")));
            }
            if r.file_notice {
                out.push("app.toml changed under the saved value; the saved value still applies".to_string());
            }
        }
        None => out.push("saved: nothing (app.toml's value applies)".to_string()),
    }
    Ok(out.join("\n"))
}

/// A value given on the command line: JSON when it starts like JSON, else
/// the form's own text (a list separated by commas).
///
/// # Errors
///
/// What is wrong with it.
pub fn parse_value(file: &FileConfig, key: &str, text: &str) -> Result<Value, String> {
    let ctx = context(file);
    let (_, field) = registry::lookup(key, &ctx.networks).ok_or_else(|| format!("{key} is not a setting the admin UI changes"))?;
    let t = text.trim();
    if t.starts_with('[') || t.starts_with('{') || t.starts_with('"') {
        return serde_json::from_str(t).map_err(|e| format!("not JSON: {e}"));
    }
    registry::parse(&field.kind, text, &ctx)
}

/// The saved row's version of `key`, 0 when nothing is saved.
///
/// # Errors
///
/// Database errors.
pub async fn version(pool: &PgPool, key: &str) -> Result<i64, sqlx::Error> {
    Ok(sqlx::query_scalar("SELECT version FROM operator_settings WHERE key = $1").bind(key).fetch_optional(pool).await?.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn values_from_the_command_line() {
        let file = FileConfig::default();
        assert_eq!(parse_value(&file, "threadlens.access", "members"), Ok(json!("members")));
        assert_eq!(parse_value(&file, "community.enroll_pds_patterns", "a.example.com,*.b.example.com"), Ok(json!(["a.example.com", "*.b.example.com"])));
        assert_eq!(parse_value(&file, "trends.descriptions", "{\"x\": \"y\"}"), Ok(json!({"x": "y"})));
        assert!(parse_value(&file, "http.bind", "0.0.0.0:1").is_err());
        assert!(parse_value(&file, "private_read.max_chain", "0").is_err());
    }
}
