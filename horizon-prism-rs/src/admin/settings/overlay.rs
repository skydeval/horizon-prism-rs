//! Saved settings layered over `app.toml` (admin-ui design §8.1).
//!
//! The file is parsed into a TOML table, each saved value is inserted at
//! its key path, and the result is deserialized into `FileConfig`, so
//! `deny_unknown_fields` and every type check apply, then validated with
//! the environment. One path, one validation.
//!
//! A key path is dotted (`community.auto_enroll`); a routing-table network
//! is addressed by its name (`constellation.networks[bluesky].background_burst`).
//! A JSON `null` removes the key (an `Option` key such as
//! `honest_501.public_off_message`).

use serde_json::Value;

use crate::config::FileConfig;

/// One step of a key path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seg {
    Key(String),
    /// An element of `constellation.networks`, by its `name`.
    Network(String),
}

const NETWORKS: &str = "constellation.networks[";

/// Whether `name` can be a network name in a key path.
#[must_use]
pub fn network_name_ok(name: &str) -> bool {
    !name.is_empty() && name.len() <= 64 && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn key_ok(k: &str) -> bool {
    !k.is_empty() && k.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// The steps of a key path.
///
/// # Errors
///
/// A malformed path.
pub fn parse_key(key: &str) -> Result<Vec<Seg>, String> {
    let bad = || format!("{key:?} is not a setting's name");
    if let Some(rest) = key.strip_prefix(NETWORKS) {
        let (name, field) = rest.split_once("].").ok_or_else(bad)?;
        if !network_name_ok(name) || !key_ok(field) {
            return Err(bad());
        }
        return Ok(vec![
            Seg::Key("constellation".into()),
            Seg::Key("networks".into()),
            Seg::Network(name.into()),
            Seg::Key(field.into()),
        ]);
    }
    let segs: Vec<Seg> = key.split('.').map(|k| Seg::Key(k.to_string())).collect();
    if segs.iter().any(|s| matches!(s, Seg::Key(k) if !key_ok(k))) {
        return Err(bad());
    }
    Ok(segs)
}

/// The JSON value at `key` in a serialized configuration.
#[must_use]
pub fn get<'a>(root: &'a Value, key: &str) -> Option<&'a Value> {
    let mut at = root;
    for seg in parse_key(key).ok()? {
        at = match seg {
            Seg::Key(k) => at.get(&k)?,
            Seg::Network(name) => at.as_array()?.iter().find(|n| n.get("name").and_then(Value::as_str) == Some(name.as_str()))?,
        };
    }
    Some(at)
}

/// Whether the TOML table writes `key` itself.
#[must_use]
pub fn has(table: &toml::Table, key: &str) -> bool {
    let Ok(segs) = parse_key(key) else {
        return false;
    };
    let mut at: Option<&toml::Value> = None;
    for seg in segs {
        let next = match (seg, at) {
            (Seg::Key(k), None) => table.get(&k),
            (Seg::Key(k), Some(v)) => v.as_table().and_then(|t| t.get(&k)),
            (Seg::Network(name), Some(v)) => v
                .as_array()
                .and_then(|a| a.iter().find(|n| n.get("name").and_then(toml::Value::as_str) == Some(name.as_str()))),
            (Seg::Network(_), None) => None,
        };
        match next {
            Some(v) => at = Some(v),
            None => return false,
        }
    }
    at.is_some()
}

/// A JSON value as TOML. Nulls inside tables and arrays are dropped (an
/// absent optional field); a top-level null has no TOML form (`None`).
#[must_use]
pub fn to_toml(v: &Value) -> Option<toml::Value> {
    Some(match v {
        Value::Null => return None,
        Value::Bool(b) => toml::Value::Boolean(*b),
        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => toml::Value::Integer(i),
            (None, Some(f)) if n.is_f64() => toml::Value::Float(f),
            _ => return None,
        },
        Value::String(s) => toml::Value::String(s.clone()),
        Value::Array(a) => toml::Value::Array(a.iter().filter_map(to_toml).collect()),
        Value::Object(o) => toml::Value::Table(o.iter().filter_map(|(k, v)| to_toml(v).map(|t| (k.clone(), t))).collect()),
    })
}

/// Insert `value` at `key` (or remove the key, for `null`). `base` is the
/// file's own configuration: when the file writes no `networks` array, its
/// running array is inserted first, then the element is patched.
///
/// # Errors
///
/// A path that does not lead to a table, or a network that does not exist.
pub fn set(table: &mut toml::Table, key: &str, value: &Value, base: &FileConfig) -> Result<(), String> {
    let segs = parse_key(key)?;
    let Some((last, path)) = segs.split_last() else {
        return Err(format!("{key:?} is not a setting's name"));
    };
    let Seg::Key(last) = last else {
        return Err(format!("{key:?} is not a setting's name"));
    };
    let mut at = table;
    for (i, seg) in path.iter().enumerate() {
        at = match seg {
            Seg::Key(k) => {
                // The networks array is materialized from the running
                // configuration when the file has none (§8.1).
                if k == "networks" && matches!(path.get(i + 1), Some(Seg::Network(_))) && !at.contains_key(k) {
                    let networks = toml::Value::try_from(&base.constellation.networks).map_err(|e| e.to_string())?;
                    at.insert(k.clone(), networks);
                }
                let entry = at.entry(k.clone()).or_insert_with(|| toml::Value::Table(toml::Table::new()));
                match entry {
                    toml::Value::Table(t) => t,
                    toml::Value::Array(arr) => {
                        let Some(Seg::Network(name)) = path.get(i + 1) else {
                            return Err(format!("{key}: {k} is a list"));
                        };
                        let element = arr
                            .iter_mut()
                            .find(|n| n.get("name").and_then(toml::Value::as_str) == Some(name.as_str()))
                            .ok_or_else(|| format!("there is no network named {name:?}"))?;
                        match element {
                            toml::Value::Table(t) => t,
                            _ => return Err(format!("{key}: network {name:?} is not a table")),
                        }
                    }
                    _ => return Err(format!("{key}: {k} is not a section")),
                }
            }
            // Handled with the array above.
            Seg::Network(_) => continue,
        };
    }
    match to_toml(value) {
        Some(v) => {
            at.insert(last.clone(), v);
        }
        None => {
            at.remove(last);
        }
    }
    Ok(())
}

/// The file with `overrides` layered over it, deserialized. Not validated.
///
/// # Errors
///
/// A path that cannot be written, or a value of the wrong type or an
/// unknown key, in words.
pub fn build(table: &toml::Table, overrides: &[(String, Value)], base: &FileConfig) -> Result<FileConfig, String> {
    let mut table = table.clone();
    for (key, value) in overrides {
        set(&mut table, key, value, base)?;
    }
    FileConfig::from_table(table).map_err(|e| e.to_string())
}

/// A configuration as JSON, for reading values at key paths.
#[must_use]
pub fn json(cfg: &FileConfig) -> Value {
    serde_json::to_value(cfg).unwrap_or(Value::Null)
}

/// Whether two values are the same setting: numbers compare by value
/// (`50` and `50.0` are equal), everything else exactly.
#[must_use]
pub fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(i), Some(j)) => i == j,
            _ => x.as_f64() == y.as_f64(),
        },
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same(a, b)),
        (Value::Object(x), Value::Object(y)) => {
            // An absent field and a null one are the same.
            let keys: std::collections::BTreeSet<&String> = x.keys().chain(y.keys()).collect();
            keys.into_iter().all(|k| same(x.get(k).unwrap_or(&Value::Null), y.get(k).unwrap_or(&Value::Null)))
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn table(text: &str) -> toml::Table {
        text.parse().expect("toml")
    }

    /// T-A8: an override at a nested key, at a named network's field with no
    /// `networks` array in the file, and a `null` override of an `Option`
    /// key each give the expected configuration.
    #[test]
    fn t_a8_overrides_layer_over_the_file() {
        let base = FileConfig::parse("[honest_501]\npublic_off_message = \"gone\"\n").expect("parse");
        let file = table("[honest_501]\npublic_off_message = \"gone\"\n");
        let overrides = vec![
            ("community.auto_enroll".to_string(), json!(false)),
            ("constellation.networks[bluesky].background_burst".to_string(), json!(9)),
            ("constellation.networks[bluesky].background_rate_per_sec".to_string(), json!(2.5)),
            ("honest_501.public_off_message".to_string(), Value::Null),
            ("rate_limits.anon_read.burst".to_string(), json!(60)),
            ("rate_limits.anon_read.per_sec".to_string(), json!(12)),
        ];
        let cfg = build(&file, &overrides, &base).expect("build");
        assert!(!cfg.community.auto_enroll);
        let bluesky = cfg.constellation.networks.iter().find(|n| n.name == "bluesky").expect("bluesky");
        assert_eq!((bluesky.background_burst, bluesky.background_rate_per_sec.0), (9, 2.5));
        assert_eq!(cfg.constellation.networks.len(), FileConfig::default().constellation.networks.len(), "the running array, patched");
        assert_eq!(cfg.honest_501.public_off_message, None);
        assert!((cfg.rate_limits.anon_read.per_sec - 12.0).abs() < f64::EPSILON, "an integer is read as a decimal");
        // Everything else as the file has it.
        let mut expected = base.clone();
        expected.community.auto_enroll = false;
        expected.constellation = cfg.constellation.clone();
        expected.honest_501.public_off_message = None;
        expected.rate_limits.anon_read.per_sec = 12.0;
        assert_eq!(cfg, expected);
        // An unknown network, a wrong type and an unknown key are refused.
        assert!(build(&file, &[("constellation.networks[nope].background_burst".into(), json!(1))], &base).is_err());
        assert!(build(&file, &[("community.auto_enroll".into(), json!("yes"))], &base).is_err());
        assert!(build(&file, &[("community.auto_enrol".into(), json!(true))], &base).is_err());
    }

    #[test]
    fn paths_read_and_compare() {
        let v = json(&FileConfig::default());
        assert_eq!(get(&v, "community.max_members"), Some(&json!(1000)));
        assert_eq!(get(&v, "constellation.networks[bluesky].background_burst"), Some(&json!(2)));
        assert_eq!(get(&v, "retention.run_at_utc"), Some(&json!("04:00")));
        assert_eq!(get(&v, "constellation.networks[nope].background_burst"), None);
        let t = table("[community]\nauto_enroll = false\n[[constellation.networks]]\nname = \"x\"\nbackground_burst = 3\n");
        assert!(has(&t, "community.auto_enroll"));
        assert!(!has(&t, "community.max_members"));
        assert!(has(&t, "constellation.networks[x].background_burst"));
        assert!(!has(&t, "constellation.networks[x].background_rate_per_sec"));
        assert!(same(&json!(50), &json!(50.0)));
        assert!(!same(&json!(50), &json!(51)));
        assert!(same(&json!({"a": 1, "b": null}), &json!({"a": 1.0})));
        assert!(parse_key("constellation.networks[a.b].x").is_err());
        assert!(parse_key("Community.x").is_err());
    }
}
