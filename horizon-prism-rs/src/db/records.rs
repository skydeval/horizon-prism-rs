//! Stored records through the record codec (`ids::encode_record`,
//! `ids::decode_record`) with the database's dictionaries.
//!
//! Dictionaries are read from `record_dicts` and kept per database, so a
//! process that talks to several (the tests) never mixes them up. A row is
//! never rewritten under an id, so a loaded dictionary is never stale; a
//! record naming an id that is not loaded reloads the table once, and an
//! id still missing is an error, never an empty record. Writes use the
//! newest dictionary of the record's family, looked up again at most once
//! a minute.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::Value;
use sqlx::PgPool;

use super::ids::{CodecError, Dict, decode_record, encode_record};

/// The record families, as `record_dicts.family` names them.
pub const FAMILY_POSTS: &str = "posts";
pub const FAMILY_PROFILES: &str = "profiles";
pub const FAMILY_OTHER: &str = "other";

/// How long the newest dictionary of a family is trusted before it is
/// looked up again.
const CURRENT_TTL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Loaded {
    dicts: HashMap<u8, Dict>,
    /// The newest dictionary id of each family.
    current: HashMap<String, u8>,
    loaded_at: Option<Instant>,
}

static LOADED: LazyLock<Mutex<HashMap<String, Arc<Loaded>>>> = LazyLock::new(Mutex::default);

fn db_key(pool: &PgPool) -> String {
    let options = pool.connect_options();
    format!("{}/{}", options.get_host(), options.get_database().unwrap_or_default())
}

fn cached(pool: &PgPool) -> Option<Arc<Loaded>> {
    LOADED.lock().unwrap_or_else(PoisonError::into_inner).get(&db_key(pool)).cloned()
}

/// Read every dictionary of the database again.
///
/// # Errors
///
/// Database errors.
pub async fn reload(pool: &PgPool) -> Result<(), sqlx::Error> {
    let rows: Vec<(i16, String, Vec<u8>)> = sqlx::query_as("SELECT id, family, bytes FROM record_dicts ORDER BY id")
        .fetch_all(pool)
        .await?;
    let mut loaded = Loaded { loaded_at: Some(Instant::now()), ..Loaded::default() };
    for (id, family, bytes) in rows {
        let Ok(byte) = u8::try_from(id) else { continue };
        loaded.dicts.insert(byte, Dict { id, bytes: Arc::new(bytes) });
        loaded.current.insert(family, byte);
    }
    LOADED.lock().unwrap_or_else(PoisonError::into_inner).insert(db_key(pool), Arc::new(loaded));
    Ok(())
}

fn codec_error(e: CodecError) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(e))
}

/// A stored record's JSON text.
///
/// # Errors
///
/// A database error while loading a dictionary, or a record that does not
/// decode (as [`sqlx::Error::Decode`]).
pub async fn decode(pool: &PgPool, stored: &[u8]) -> Result<String, sqlx::Error> {
    let id = *stored.first().ok_or_else(|| codec_error(CodecError::Empty))?;
    if id == 0 {
        return decode_record(stored, &HashMap::new()).map_err(codec_error);
    }
    let loaded = match cached(pool) {
        Some(l) if l.dicts.contains_key(&id) => l,
        _ => {
            reload(pool).await?;
            cached(pool).unwrap_or_default()
        }
    };
    decode_record(stored, &loaded.dicts).map_err(codec_error)
}

/// A stored record as JSON.
///
/// # Errors
///
/// As [`decode`], and a record whose text is not JSON.
pub async fn decode_value(pool: &PgPool, stored: &[u8]) -> Result<Value, sqlx::Error> {
    let text = decode(pool, stored).await?;
    serde_json::from_str(&text).map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

/// A record as stored: with the newest dictionary of `family`, or dict 0
/// while there is none.
///
/// # Errors
///
/// A database error while looking the dictionary up, or a compression
/// error.
pub async fn encode(pool: &PgPool, family: &str, record: &Value) -> Result<Vec<u8>, sqlx::Error> {
    encode_text(pool, family, &record.to_string()).await
}

/// [`encode`] of a record's JSON text as it is.
///
/// # Errors
///
/// As [`encode`].
pub async fn encode_text(pool: &PgPool, family: &str, json: &str) -> Result<Vec<u8>, sqlx::Error> {
    let fresh = cached(pool).filter(|l| l.loaded_at.is_some_and(|at| at.elapsed() < CURRENT_TTL));
    let loaded = match fresh {
        Some(l) => l,
        None => {
            reload(pool).await?;
            cached(pool).unwrap_or_default()
        }
    };
    let dict = loaded.current.get(family).and_then(|id| loaded.dicts.get(id));
    encode_record(json, dict).map_err(codec_error)
}

/// The newest dictionary id of `family`, as last loaded (none: 0).
///
/// # Errors
///
/// Database errors.
pub async fn current_id(pool: &PgPool, family: &str) -> Result<u8, sqlx::Error> {
    reload(pool).await?;
    Ok(cached(pool).and_then(|l| l.current.get(family).copied()).unwrap_or(0))
}

/// A record as dict 0, the JSON text uncompressed.
#[must_use]
pub fn plain(record: &Value) -> Vec<u8> {
    let text = record.to_string();
    let mut out = Vec::with_capacity(text.len() + 1);
    out.push(0);
    out.extend_from_slice(text.as_bytes());
    out
}
