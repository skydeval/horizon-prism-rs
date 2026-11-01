//! Compact storage ids and codecs.
//!
//! - Accounts are interned in `dids` ([`Ids::account_ids`]): ids are created in
//!   their own autocommit statements before any caller's transaction, select
//!   first, so a rolled-back writer never leaves a cached id that does not
//!   exist, and a known DID never spends a sequence value.
//! - Record keys are TIDs as their 64-bit value ([`rkey_value`]); any other
//!   valid record key gets a negative alias id ([`Ids::rkey_ids`]).
//! - Revs use their own, order-keeping map of all 64 bits ([`rev_value`]).
//! - CIDs are bytes when the string is a canonical CID, else `0x00 ‖ UTF-8`.
//! - References are whole only when [`whole_uri`] gives back their text.
//! - Records are `dict id ‖ payload`; dict 0 is JSON text ([`encode_record`]).
//!
//! The SQL helpers of migration 0054 must agree with this module; the tests
//! run both over the same inputs.

use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgPool;

#[cfg(test)]
mod tests;

/// An interned account (`dids.id`).
pub type AccountId = i32;

/// The fixed collection list, as migration 0054 seeds `collections`. Data
/// never grows it; a reference to any other collection is odd.
pub const COLLECTIONS: [(i16, &str); 34] = [
    (1, "app.bsky.feed.post"),
    (2, "app.bsky.feed.like"),
    (3, "app.bsky.feed.repost"),
    (4, "app.bsky.graph.follow"),
    (5, "app.bsky.graph.block"),
    (6, "app.bsky.actor.profile"),
    (7, "app.bsky.graph.list"),
    (8, "app.bsky.graph.listitem"),
    (9, "app.bsky.graph.listblock"),
    (10, "app.bsky.graph.referencelistoptout"),
    (11, "app.bsky.graph.starterpack"),
    (12, "app.bsky.labeler.service"),
    (13, "app.bsky.notification.declaration"),
    (14, "app.bsky.actor.status"),
    (15, "app.bsky.actor.contentVisibilityDeclaration"),
    (16, "app.bsky.feed.threadgate"),
    (17, "app.bsky.feed.postgate"),
    (18, "app.bsky.feed.generator"),
    (19, "app.bsky.graph.verification"),
    (20, "app.nearhorizon.poll.post"),
    (21, "app.nearhorizon.poll.vote"),
    (22, "app.nearhorizon.actor.pronouns"),
    (23, "app.nearhorizon.plural.system"),
    (24, "app.nearhorizon.plural.front"),
    (25, "app.nearhorizon.plural.member"),
    (26, "app.nearhorizon.plural.fronters"),
    (27, "tools.kryphocron.feed.postPrivate"),
    (28, "tools.kryphocron.feed.postPublic"),
    (29, "tools.kryphocron.feed.like"),
    (30, "tools.kryphocron.feed.repost"),
    (31, "tools.kryphocron.feed.threadgate"),
    (32, "tools.kryphocron.graph.mute"),
    (33, "tools.kryphocron.graph.block"),
    (34, "tools.kryphocron.policy.audience"),
];

/// The `coll` value of an odd reference.
pub const ODD_COLL: i16 = 0;

/// An owner table of `odd_refs` rows (`odd_refs.tbl`). Its id is the
/// collection id of the records it holds, so it never changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OddTable {
    Posts,
    Likes,
    Reposts,
    Follows,
}

impl OddTable {
    #[must_use]
    pub const fn id(self) -> i16 {
        match self {
            Self::Posts => 1,
            Self::Likes => 2,
            Self::Reposts => 3,
            Self::Follows => 4,
        }
    }
}

/// The collection ids SQL names most.
pub const COLL_POST: i16 = 1;
pub const COLL_LIKE: i16 = 2;
pub const COLL_REPOST: i16 = 3;
pub const COLL_THREADGATE: i16 = 16;
pub const COLL_POSTGATE: i16 = 17;
pub const COLL_PRIVATE_POST: i16 = 27;

/// Which reference of its owner row an `odd_refs` row holds
/// (`odd_refs.slot`). Stable: stored values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    Root,
    Parent,
    Quote,
    Subject,
    Via,
    List,
}

impl Slot {
    #[must_use]
    pub const fn id(self) -> i16 {
        match self {
            Self::Root => 1,
            Self::Parent => 2,
            Self::Quote => 3,
            Self::Subject => 4,
            Self::Via => 5,
            Self::List => 6,
        }
    }
}

/// A reference as its owner row stores it: whole, or odd with its text and
/// the authority's id when that is a valid DID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredRef {
    Whole { author: AccountId, coll: i16, rkey: i64 },
    Odd { author: Option<AccountId>, raw: String },
}

impl StoredRef {
    /// The owner row's `(author, coll, rkey)` columns.
    #[must_use]
    pub fn columns(&self) -> (Option<AccountId>, i16, Option<i64>) {
        match self {
            Self::Whole { author, coll, rkey } => (Some(*author), *coll, Some(*rkey)),
            Self::Odd { author, .. } => (*author, ODD_COLL, None),
        }
    }

    /// The raw text of an odd reference.
    #[must_use]
    pub fn odd_raw(&self) -> Option<&str> {
        match self {
            Self::Odd { raw, .. } => Some(raw),
            Self::Whole { .. } => None,
        }
    }
}

/// The DID a reference URI names, when it is a valid one: what to intern
/// before [`stored_ref`].
#[must_use]
pub fn ref_did(uri: &str) -> Option<String> {
    match uri_parts(uri) {
        UriParts::Whole { did, .. } => Some(did),
        UriParts::Odd { did } => did,
    }
}

/// The record key a whole reference URI names, with its authority: what to
/// give [`Ids::rkey_ids`] before [`stored_ref`].
#[must_use]
pub fn ref_rkey(uri: &str) -> Option<(String, String)> {
    match uri_parts(uri) {
        UriParts::Whole { did, rkey, .. } => Some((did, rkey)),
        UriParts::Odd { .. } => None,
    }
}

/// A reference URI as stored, from ids already interned. A whole URI whose
/// authority or key has no id here is kept odd, which reads back the same.
#[must_use]
pub fn stored_ref(uri: &str, accounts: &HashMap<String, AccountId>, rkeys: &HashMap<(AccountId, String), i64>) -> StoredRef {
    match uri_parts(uri) {
        UriParts::Whole { did, coll, rkey } => {
            let author = accounts.get(&did).copied();
            match author.and_then(|a| rkeys.get(&(a, rkey)).map(|k| (a, *k))) {
                Some((author, rkey)) => StoredRef::Whole { author, coll, rkey },
                None => StoredRef::Odd { author, raw: uri.to_string() },
            }
        }
        UriParts::Odd { did } => StoredRef::Odd {
            author: did.and_then(|d| accounts.get(&d).copied()),
            raw: uri.to_string(),
        },
    }
}

/// The key a whole reference URI names, read-only: `None` for an odd URI,
/// or one whose authority or key has no id.
///
/// # Errors
///
/// Database errors.
pub async fn whole_key<'e, E: sqlx::PgExecutor<'e>>(e: E, uri: &str) -> Result<Option<(AccountId, i16, i64)>, sqlx::Error> {
    if !matches!(uri_parts(uri), UriParts::Whole { .. }) {
        return Ok(None);
    }
    let key: Option<(Option<AccountId>, Option<i16>, Option<i64>)> =
        sqlx::query_as("SELECT author, coll, rkey FROM ref_of($1)").bind(uri).fetch_optional(e).await?;
    Ok(match key {
        Some((Some(author), Some(coll), Some(rkey))) => Some((author, coll, rkey)),
        _ => None,
    })
}

/// A collection's id in the fixed list.
#[must_use]
pub fn collection_id(nsid: &str) -> Option<i16> {
    COLLECTIONS.iter().find(|(_, n)| *n == nsid).map(|(id, _)| *id)
}

/// A collection id's NSID.
#[must_use]
pub fn collection_nsid(id: i16) -> Option<&'static str> {
    COLLECTIONS.iter().find(|(i, _)| *i == id).map(|(_, n)| *n)
}

const TID_ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";

/// A valid TID's unsigned 64-bit value.
fn tid_u64(s: &str) -> Option<u64> {
    if !proto_blue::syntax::Tid::is_valid(s) {
        return None;
    }
    let mut v: u64 = 0;
    for c in s.bytes() {
        let d = TID_ALPHABET.iter().position(|a| *a == c)?;
        v = (v << 5) | u64::try_from(d).ok()?;
    }
    Some(v)
}

fn tid_from_u64(v: u64) -> String {
    (0..13u32)
        .map(|i| char::from(TID_ALPHABET[usize::try_from((v >> (60 - 5 * i)) & 31).unwrap_or(0)]))
        .collect()
}

/// The rkey map: a TID's value when its top bit is clear (as the thread
/// outline stores keys), else `None` and the key takes an alias.
#[must_use]
pub fn rkey_value(s: &str) -> Option<i64> {
    tid_u64(s).and_then(|v| i64::try_from(v).ok())
}

/// The inverse of [`rkey_value`] for a non-negative key.
#[must_use]
pub fn rkey_string(v: i64) -> Option<String> {
    u64::try_from(v).ok().map(tid_from_u64)
}

/// The rev map: every valid TID in its text order, as `value - 2^63`; any
/// other string maps to the lowest value, the sentinel's.
#[must_use]
pub fn rev_value(s: &str) -> i64 {
    // Flipping the top bit maps unsigned order onto signed order.
    tid_u64(s).map_or(i64::MIN, |v| i64::from_ne_bytes((v ^ (1 << 63)).to_ne_bytes()))
}

/// The inverse of [`rev_value`].
#[must_use]
pub fn rev_string(v: i64) -> String {
    tid_from_u64(u64::from_ne_bytes(v.to_ne_bytes()) ^ (1 << 63))
}

/// A CID string as stored: its bytes when it is a canonical CID (re-encoding
/// gives back the input), else `0x00 ‖ UTF-8`, so every string round-trips.
#[must_use]
pub fn cid_to_db(s: &str) -> Vec<u8> {
    if let Ok(cid) = proto_blue::lex_data::Cid::from_str_multibase(s)
        && cid.to_string_base32() == s
    {
        return cid.to_bytes();
    }
    let mut out = Vec::with_capacity(s.len() + 1);
    out.push(0);
    out.extend_from_slice(s.as_bytes());
    out
}

/// The inverse of [`cid_to_db`].
#[must_use]
pub fn cid_from_db(b: &[u8]) -> String {
    match b.split_first() {
        Some((0, text)) => String::from_utf8_lossy(text).into_owned(),
        _ => proto_blue::lex_data::Cid::from_bytes(b).map_or_else(|_| String::new(), |c| c.to_string_base32()),
    }
}

/// What a reference URI is, before ids: whole when [`whole_uri`] of its parts
/// gives back its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UriParts {
    /// `at://<did>/<collection in the list>/<valid record key>`.
    Whole { did: String, coll: i16, rkey: String },
    /// Anything else; `did` is the authority when it is a valid DID.
    Odd { did: Option<String> },
}

/// Split a reference URI. Strict: no trailing slash, no extra segment, a
/// collection from the fixed list, a valid DID and record key.
#[must_use]
pub fn uri_parts(uri: &str) -> UriParts {
    let Some(rest) = uri.strip_prefix("at://") else {
        return UriParts::Odd { did: None };
    };
    let mut parts = rest.split('/');
    let authority = parts.next().unwrap_or("");
    let did = proto_blue::syntax::Did::is_valid(authority).then(|| authority.to_string());
    match (did, parts.next(), parts.next(), parts.next()) {
        (Some(did), Some(nsid), Some(rkey), None) => match collection_id(nsid) {
            Some(coll) if proto_blue::syntax::RecordKey::is_valid(rkey) => UriParts::Whole { did, coll, rkey: rkey.to_string() },
            _ => UriParts::Odd { did: Some(did) },
        },
        (did, ..) => UriParts::Odd { did },
    }
}

/// The URI of a whole reference: the inverse of [`uri_parts`].
#[must_use]
pub fn whole_uri(did: &str, coll: i16, rkey: &str) -> Option<String> {
    collection_nsid(coll).map(|nsid| format!("at://{did}/{nsid}/{rkey}"))
}

/// A record's stored form: `dict id ‖ payload`. Dict 0 is the JSON text.
#[derive(Clone)]
pub struct Dict {
    pub id: i16,
    pub bytes: Arc<Vec<u8>>,
}

const ZSTD_LEVEL: i32 = 3;

/// Errors of the record codec.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("record is empty")]
    Empty,
    #[error("record names dictionary {0}, which is not loaded")]
    MissingDict(u8),
    #[error("record does not decompress: {0}")]
    Zstd(String),
    #[error("record is not UTF-8")]
    Utf8,
}

/// Encode a record's JSON text. With no dictionary, dict 0 (uncompressed).
pub fn encode_record(json: &str, dict: Option<&Dict>) -> Result<Vec<u8>, CodecError> {
    let Some(dict) = dict else {
        let mut out = Vec::with_capacity(json.len() + 1);
        out.push(0);
        out.extend_from_slice(json.as_bytes());
        return Ok(out);
    };
    let mut c = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, &dict.bytes).map_err(|e| CodecError::Zstd(e.to_string()))?;
    let body = c.compress(json.as_bytes()).map_err(|e| CodecError::Zstd(e.to_string()))?;
    let mut out = Vec::with_capacity(body.len() + 1);
    out.push(u8::try_from(dict.id).map_err(|_| CodecError::MissingDict(0))?);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode a stored record to its JSON text. A dictionary id that is not
/// loaded is an error, never an empty record.
pub fn decode_record(stored: &[u8], dicts: &HashMap<u8, Dict>) -> Result<String, CodecError> {
    let (&id, body) = stored.split_first().ok_or(CodecError::Empty)?;
    let bytes = if id == 0 {
        body.to_vec()
    } else {
        let dict = dicts.get(&id).ok_or(CodecError::MissingDict(id))?;
        let mut d = zstd::bulk::Decompressor::with_dictionary(&dict.bytes).map_err(|e| CodecError::Zstd(e.to_string()))?;
        // The frame records its size; 16 MB bounds a corrupt one.
        d.decompress(body, 16 << 20).map_err(|e| CodecError::Zstd(e.to_string()))?
    };
    String::from_utf8(bytes).map_err(|_| CodecError::Utf8)
}

/// Train a record dictionary from sample records.
pub fn train_dict(samples: &[Vec<u8>], max_size: usize) -> Result<Vec<u8>, CodecError> {
    zstd::dict::from_samples(samples, max_size).map_err(|e| CodecError::Zstd(e.to_string()))
}

/// The interning cache and its pool. Shared by every writer.
#[derive(Clone)]
pub struct Ids {
    pool: PgPool,
    accounts: moka::sync::Cache<String, AccountId>,
    aliases: moka::sync::Cache<(AccountId, String), i64>,
}

/// Entries kept in each cache.
pub const CACHE_ENTRIES: u64 = 1_000_000;

impl Ids {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            accounts: moka::sync::Cache::new(CACHE_ENTRIES),
            aliases: moka::sync::Cache::new(CACHE_ENTRIES / 10),
        }
    }

    /// Ids for these DIDs, creating the missing ones. Strings that are not
    /// valid DIDs are skipped (they never become accounts). Runs its own
    /// autocommit statements on the pool, so call it before a writer's
    /// transaction; only committed ids enter the cache.
    pub async fn account_ids(&self, dids: &[String]) -> Result<HashMap<String, AccountId>, sqlx::Error> {
        let mut out = HashMap::with_capacity(dids.len());
        let mut missing: Vec<String> = Vec::new();
        for d in dids {
            if out.contains_key(d) || !proto_blue::syntax::Did::is_valid(d) {
                continue;
            }
            match self.accounts.get(d) {
                Some(id) => {
                    out.insert(d.clone(), id);
                }
                None => missing.push(d.clone()),
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        missing.sort();
        missing.dedup();
        let found = select_accounts(&self.pool, &missing).await?;
        let absent: Vec<String> = missing.iter().filter(|d| !found.contains_key(*d)).cloned().collect();
        let mut all = found;
        if !absent.is_empty() {
            sqlx::query(
                "INSERT INTO dids (did) SELECT d FROM unnest($1::text[]) AS d \
                 WHERE NOT EXISTS (SELECT 1 FROM dids a WHERE a.did = d) \
                 ON CONFLICT (did) DO NOTHING",
            )
            .bind(&absent)
            .execute(&self.pool)
            .await?;
            // Another writer may have won a race; read every id back.
            all.extend(select_accounts(&self.pool, &absent).await?);
        }
        for (d, id) in all {
            self.accounts.insert(d.clone(), id);
            out.insert(d, id);
        }
        Ok(out)
    }

    /// Key values for record keys: a TID's value, or an alias id (created
    /// when missing) for any other valid record key. Invalid keys are skipped.
    pub async fn rkey_ids(&self, keys: &[(AccountId, String)]) -> Result<HashMap<(AccountId, String), i64>, sqlx::Error> {
        let mut out = HashMap::with_capacity(keys.len());
        let mut missing: Vec<(AccountId, String)> = Vec::new();
        for k in keys {
            if out.contains_key(k) || !proto_blue::syntax::RecordKey::is_valid(&k.1) {
                continue;
            }
            if let Some(v) = rkey_value(&k.1) {
                out.insert(k.clone(), v);
            } else if let Some(v) = self.aliases.get(k) {
                out.insert(k.clone(), v);
            } else {
                missing.push(k.clone());
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        missing.sort();
        missing.dedup();
        let (authors, rkeys): (Vec<AccountId>, Vec<String>) = missing.iter().cloned().unzip();
        let found = select_aliases(&self.pool, &authors, &rkeys).await?;
        let absent: Vec<&(AccountId, String)> = missing.iter().filter(|k| !found.contains_key(*k)).collect();
        let mut all = found;
        if !absent.is_empty() {
            let (a, r): (Vec<AccountId>, Vec<String>) = absent.iter().map(|k| (k.0, k.1.clone())).unzip();
            sqlx::query(
                "INSERT INTO rkey_aliases (author, rkey) SELECT * FROM unnest($1::int[], $2::text[]) AS k(author, rkey) \
                 WHERE NOT EXISTS (SELECT 1 FROM rkey_aliases x WHERE x.author = k.author AND x.rkey = k.rkey) \
                 ON CONFLICT (author, rkey) DO NOTHING",
            )
            .bind(&a)
            .bind(&r)
            .execute(&self.pool)
            .await?;
            all.extend(select_aliases(&self.pool, &a, &r).await?);
        }
        for (k, v) in all {
            self.aliases.insert(k.clone(), v);
            out.insert(k, v);
        }
        Ok(out)
    }
}

async fn select_accounts(pool: &PgPool, dids: &[String]) -> Result<HashMap<String, AccountId>, sqlx::Error> {
    let rows: Vec<(AccountId, String)> = sqlx::query_as("SELECT id, did FROM dids WHERE did = ANY($1)").bind(dids).fetch_all(pool).await?;
    Ok(rows.into_iter().map(|(id, did)| (did, id)).collect())
}

async fn select_aliases(pool: &PgPool, authors: &[AccountId], rkeys: &[String]) -> Result<HashMap<(AccountId, String), i64>, sqlx::Error> {
    let rows: Vec<(i64, AccountId, String)> = sqlx::query_as(
        "SELECT a.id, a.author, a.rkey FROM rkey_aliases a \
         JOIN unnest($1::int[], $2::text[]) AS k(author, rkey) ON a.author = k.author AND a.rkey = k.rkey",
    )
    .bind(authors)
    .bind(rkeys)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(id, author, rkey)| ((author, rkey), id)).collect())
}
