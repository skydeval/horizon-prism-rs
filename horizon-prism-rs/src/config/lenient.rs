//! Startup loading that stops only for a core setting.
//!
//! Horizon-Prism is an indexer first: a bad or unknown setting for a
//! feature sets that setting aside, so its default applies, and the start
//! goes on. Only the settings indexing cannot run without stop a start
//! ([`CORE_KEYS`] and the core environment variables). Everything set aside
//! is logged, counted and shown in the admin UI.

use std::sync::Mutex;

use toml::{Table, Value};

use super::{AppConfig, ConfigError, EnvConfig, FileConfig};

/// A setting not used at this start, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAside {
    /// The key (`section.key`, `section.list[name]`) or environment variable.
    pub key: String,
    pub problem: String,
}

/// File settings that stop a start when they are wrong: without them
/// Horizon-Prism would serve or index the wrong thing.
pub const CORE_KEYS: [&str; 7] = [
    "http.bind",
    "http.trusted_proxy",
    "firehose.stream_key",
    "firehose.group",
    "firehose.consumer_name",
    "indexing.public_scope",
    "scope.policy",
];

/// Metric: settings set aside at the last start.
pub const METRIC_SET_ASIDE: &str = "config_set_aside";

/// Rounds of setting aside before giving up; each round removes at least
/// one key, and a file has far fewer bad keys than this.
const MAX_ROUNDS: usize = 64;

static NOTICES: Mutex<Vec<SetAside>> = Mutex::new(Vec::new());

/// Log one thing set aside at this start, and keep it for the admin UI.
pub fn note_at_start(item: SetAside) {
    tracing::error!(event = "config_set_aside", key = %item.key, problem = %item.problem, "a setting could not be used and was set aside; Prism started without it");
    let mut notices = NOTICES.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    notices.push(item);
    metrics::gauge!(METRIC_SET_ASIDE).set(f64::from(u32::try_from(notices.len()).unwrap_or(u32::MAX)));
}

/// What was set aside at this start.
#[must_use]
pub fn notices() -> Vec<SetAside> {
    NOTICES.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
}

/// A configuration that loaded, with what was set aside.
#[derive(Debug, Clone)]
pub struct Loaded {
    pub cfg: AppConfig,
    /// The file's TOML with the set-aside keys removed.
    pub table: Table,
    pub set_aside: Vec<SetAside>,
}

/// Load from TOML text and an environment, setting aside what is not core.
///
/// # Errors
///
/// TOML that is not TOML, a core environment variable missing or wrong, or
/// a problem with a core setting.
pub fn load(toml_text: &str, lookup: &dyn Fn(&str) -> Option<String>) -> Result<Loaded, ConfigError> {
    let original: Table = toml_text.parse().map_err(|e: toml::de::Error| ConfigError::Parse(Box::new(e)))?;
    let (env, mut set_aside) = EnvConfig::from_lookup_lenient(lookup)?;
    let mut table = original.clone();
    let mut last = Vec::new();
    for _ in 0..MAX_ROUNDS {
        let file = salvage(&mut table, &original, &mut set_aside)?;
        let cfg = AppConfig { file, env: env.clone() };
        let problems = cfg.problems();
        if problems.is_empty() {
            return Ok(Loaded { cfg, table, set_aside });
        }
        // Each problem sets aside the first key it names that the file
        // sets and that is not core; a problem that names none stops the
        // start. A problem still there next round sets aside its next key.
        let mut fatal = Vec::new();
        for problem in &problems {
            match paths_in(problem, &table).into_iter().find(|p| !touches_core(&p.text, &original)) {
                Some(path) => {
                    if remove(&mut table, &path.segs) {
                        set_aside.push(SetAside { key: path.text, problem: problem.clone() });
                    }
                }
                None => fatal.push(problem.clone()),
            }
        }
        if !fatal.is_empty() {
            return Err(ConfigError::Invalid(fatal));
        }
        last = problems;
    }
    Err(ConfigError::Invalid(last))
}

/// Parse `table`, removing each value that does not deserialize (an
/// unknown key, a wrong type) and recording it.
fn salvage(table: &mut Table, original: &Table, out: &mut Vec<SetAside>) -> Result<FileConfig, ConfigError> {
    if let Ok(file) = FileConfig::from_table(table.clone()) {
        return Ok(file);
    }
    let mut kept = Table::new();
    let mut found = Vec::new();
    for (key, value) in std::mem::take(table) {
        if let Some(v) = salvage_value(&mut vec![key.clone()], value, &mut found) {
            kept.insert(key, v);
        }
    }
    *table = kept;
    if let Some(core) = found.iter().find(|s| touches_core(&s.key, original)) {
        return Err(ConfigError::Invalid(vec![format!("{}: {}", core.key, core.problem)]));
    }
    out.extend(found);
    FileConfig::from_table(table.clone())
}

/// `value` at `path` if it deserializes there; else what of it does, with
/// the rest recorded in `out`.
fn salvage_value(path: &mut Vec<String>, value: Value, out: &mut Vec<SetAside>) -> Option<Value> {
    let problem = match parses_at(path, value.clone()) {
        Ok(()) => return Some(value),
        Err(e) => e,
    };
    let mark = out.len();
    let kept = match value {
        Value::Table(t) => {
            let mut kept = Table::new();
            for (k, v) in t {
                path.push(k.clone());
                if let Some(v) = salvage_value(path, v, out) {
                    kept.insert(k, v);
                }
                path.pop();
            }
            Value::Table(kept)
        }
        Value::Array(items) => {
            let mut kept = Vec::new();
            for (i, item) in items.into_iter().enumerate() {
                match parses_at(path, Value::Array(vec![item.clone()])) {
                    Ok(()) => kept.push(item),
                    Err(e) => out.push(SetAside { key: format!("{}[{}]", path.join("."), entry_name(&item).unwrap_or_else(|| i.to_string())), problem: e }),
                }
            }
            Value::Array(kept)
        }
        _ => {
            out.push(SetAside { key: path.join("."), problem });
            return None;
        }
    };
    if parses_at(path, kept.clone()).is_ok() {
        Some(kept)
    } else {
        // The whole value goes, as one entry.
        out.truncate(mark);
        out.push(SetAside { key: path.join("."), problem });
        None
    }
}

fn entry_name(item: &Value) -> Option<String> {
    item.get("name").and_then(Value::as_str).map(str::to_string)
}

fn parses_at(path: &[String], value: Value) -> Result<(), String> {
    let mut v = value;
    for key in path.iter().rev() {
        v = Value::Table(Table::from_iter([(key.clone(), v)]));
    }
    let Value::Table(t) = v else { return Ok(()) };
    FileConfig::from_table(t).map(|_| ()).map_err(|e| match e {
        ConfigError::Parse(e) => e.message().to_string(),
        other => other.to_string(),
    })
}

/// A key is core when it is one of [`CORE_KEYS`] or under one, or holds
/// one the file sets.
fn touches_core(key: &str, original: &Table) -> bool {
    CORE_KEYS.iter().any(|core| {
        let under = |a: &str, b: &str| a == b || a.starts_with(&format!("{b}.")) || a.starts_with(&format!("{b}["));
        under(key, core) || (under(core, key) && resolve(original, &parse_path(core)).is_some())
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Seg {
    Key(String),
    Index(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Path {
    segs: Vec<Seg>,
    text: String,
}

fn parse_path(text: &str) -> Vec<Seg> {
    read_path(text).0
}

/// The path at the start of `text` (`a.b[c].d`), and its length.
fn read_path(text: &str) -> (Vec<Seg>, usize) {
    let bytes = text.as_bytes();
    let mut segs = Vec::new();
    let mut i = 0;
    loop {
        let start = i;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        if i == start {
            break;
        }
        segs.push(Seg::Key(text[start..i].to_string()));
        while i < bytes.len() && bytes[i] == b'[' {
            let Some(close) = text[i..].find(']') else { break };
            segs.push(Seg::Index(text[i + 1..i + close].to_string()));
            i += close + 1;
        }
        if i + 1 < bytes.len() && bytes[i] == b'.' && (bytes[i + 1].is_ascii_alphanumeric() || bytes[i + 1] == b'_') {
            i += 1;
        } else {
            break;
        }
    }
    (segs, i)
}

/// Every key path named in `problem` that the file sets, at its deepest
/// level the file has (a list entry stands for what is under it).
fn paths_in(problem: &str, table: &Table) -> Vec<Path> {
    let bytes = problem.as_bytes();
    let mut out: Vec<Path> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        // Not inside a word, and not a `[[table]]` header named in passing.
        let boundary = i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || matches!(bytes[i - 1], b'_' | b'.' | b'['));
        if boundary && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_') {
            let (segs, len) = read_path(&problem[i..]);
            if let Some(found) = resolved(table, &segs)
                && !out.contains(&found)
            {
                out.push(found);
            }
            i += len.max(1);
        } else {
            i += 1;
        }
    }
    out
}

/// `segs` if every key in it is in `table`, cut after the first list
/// entry: an entry of a list is set aside whole.
fn resolved(table: &Table, segs: &[Seg]) -> Option<Path> {
    if !matches!(segs.first()?, Seg::Key(_)) {
        return None;
    }
    let mut end = segs.len();
    for n in 0..segs.len() {
        resolve(table, &segs[..=n])?;
        if matches!(segs[n], Seg::Index(_)) && matches!(resolve(table, &segs[..n]), Some(Value::Array(_))) {
            end = n + 1;
            break;
        }
    }
    let taken = segs[..end].to_vec();
    Some(Path { text: text_of(&taken), segs: taken })
}

fn text_of(segs: &[Seg]) -> String {
    let mut s = String::new();
    for seg in segs {
        match seg {
            Seg::Key(k) => {
                if !s.is_empty() {
                    s.push('.');
                }
                s.push_str(k);
            }
            Seg::Index(i) => {
                s.push('[');
                s.push_str(i);
                s.push(']');
            }
        }
    }
    s
}

fn resolve<'a>(table: &'a Table, segs: &[Seg]) -> Option<&'a Value> {
    let mut node: Option<&Value> = None;
    for seg in segs {
        node = Some(match (node, seg) {
            (None, Seg::Key(k)) => table.get(k)?,
            (None, Seg::Index(_)) => return None,
            (Some(Value::Table(t)), Seg::Key(k) | Seg::Index(k)) => t.get(k)?,
            (Some(Value::Array(items)), Seg::Index(i)) => items.iter().find(|item| entry_name(item).as_deref() == Some(i.as_str())).or_else(|| i.parse::<usize>().ok().and_then(|n| items.get(n)))?,
            _ => return None,
        });
    }
    node
}

/// Remove the value at `segs`. Returns whether anything was removed.
fn remove(table: &mut Table, segs: &[Seg]) -> bool {
    let Some((last, parent)) = segs.split_last() else { return false };
    if parent.is_empty() {
        return match last {
            Seg::Key(k) => table.remove(k).is_some(),
            Seg::Index(_) => false,
        };
    }
    let Some(node) = resolve_mut(table, parent) else { return false };
    match (node, last) {
        (Value::Table(t), Seg::Key(k) | Seg::Index(k)) => t.remove(k).is_some(),
        (Value::Array(items), Seg::Index(i)) => {
            let at = items.iter().position(|item| entry_name(item).as_deref() == Some(i.as_str())).or_else(|| i.parse::<usize>().ok().filter(|n| *n < items.len()));
            at.map(|n| items.remove(n)).is_some()
        }
        _ => false,
    }
}

fn resolve_mut<'a>(table: &'a mut Table, segs: &[Seg]) -> Option<&'a mut Value> {
    let (first, rest) = segs.split_first()?;
    let Seg::Key(k) = first else { return None };
    let mut node = table.get_mut(k)?;
    for seg in rest {
        node = match (node, seg) {
            (Value::Table(t), Seg::Key(k) | Seg::Index(k)) => t.get_mut(k)?,
            (Value::Array(items), Seg::Index(i)) => {
                let at = items.iter().position(|item| entry_name(item).as_deref() == Some(i.as_str())).or_else(|| i.parse::<usize>().ok())?;
                items.get_mut(at)?
            }
            _ => return None,
        };
    }
    Some(node)
}

#[cfg(test)]
mod tests;
