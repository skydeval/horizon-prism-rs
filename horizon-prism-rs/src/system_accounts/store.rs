//! The tables (design §4). Every statement binds its values; a listed
//! account is bound only as its index and its sealed form, never in clear,
//! so neither SQL text nor a database's parameter log carries it. No
//! statement can raise a unique violation (`ON CONFLICT` throughout), so the
//! database never writes a `Key (…)=(…)` detail line either.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use super::tokens::UsedToken;
use super::{Keys, MAX_ENTRIES};

/// Expired used tokens deleted by one write, at most.
const SWEEP_BATCH: i64 = 100;

/// Why a write was refused inside its transaction (nothing committed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    BearerUsed,
    ProofUsed,
    ListFull,
}

/// A store failure. A database error keeps only its SQLSTATE (or the sqlx
/// error's kind): its text can carry values, so it is never logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreError {
    Refused(Refusal),
    Db(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(error_code(&e))
    }
}

/// The SQLSTATE of a database error, or the kind of any other sqlx error.
#[must_use]
pub(crate) fn error_code(e: &sqlx::Error) -> String {
    match e {
        sqlx::Error::Database(d) => d.code().map_or_else(|| "database".to_string(), |c| c.into_owned()),
        sqlx::Error::PoolTimedOut => "pool_timed_out".to_string(),
        sqlx::Error::PoolClosed => "pool_closed".to_string(),
        sqlx::Error::Io(_) => "io".to_string(),
        sqlx::Error::RowNotFound => "row_not_found".to_string(),
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) => "decode".to_string(),
        _ => "other".to_string(),
    }
}

/// One stored entry, sealed.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub index: Vec<u8>,
    pub sealed: Vec<u8>,
    pub added_at: DateTime<Utc>,
}

/// A's list: its `rev` (0 for a list never written) and its entries in the
/// order added. One statement, so `rev` and entries are one snapshot.
type ListRow = (i64, Option<Vec<u8>>, Option<Vec<u8>>, Option<DateTime<Utc>>);

pub(crate) async fn list(pool: &PgPool, owner: &str) -> Result<(i64, Vec<Entry>), StoreError> {
    let rows: Vec<ListRow> = sqlx::query_as(
        "SELECT l.rev, e.member_index, e.member_sealed, e.added_at \
         FROM sysacct_lists l LEFT JOIN sysacct_entries e ON e.owner_did = l.owner_did \
         WHERE l.owner_did = $1 ORDER BY e.added_at, e.member_index",
    )
    .bind(owner)
    .fetch_all(pool)
    .await?;
    let rev = rows.first().map_or(0, |r| r.0);
    let entries = rows
        .into_iter()
        .filter_map(|(_, index, sealed, added_at)| Some(Entry { index: index?, sealed: sealed?, added_at: added_at? }))
        .collect();
    Ok((rev, entries))
}

/// Whether any list holds `index`.
pub(crate) async fn listed(pool: &PgPool, index: &[u8; 32]) -> Result<bool, StoreError> {
    Ok(sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sysacct_entries WHERE member_index = $1)")
        .bind(index.as_slice())
        .fetch_one(pool)
        .await?)
}

/// Delete up to [`SWEEP_BATCH`] expired used tokens, skipping rows another
/// write is sweeping.
async fn sweep(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DELETE FROM sysacct_used_tokens WHERE token_hash IN (\
           SELECT token_hash FROM sysacct_used_tokens WHERE expires_at < now() \
           ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED)",
    )
    .bind(SWEEP_BATCH)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Record a used token; `false` when a committed request already used it.
async fn record(conn: &mut PgConnection, token: &UsedToken) -> Result<bool, sqlx::Error> {
    let n = sqlx::query("INSERT INTO sysacct_used_tokens (token_hash, expires_at) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(token.hash.as_slice())
        .bind(token.expires_at)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    Ok(n == 1)
}

/// `addSystemAccount`'s transaction (design §4.2). Returns the list's `rev`.
pub(crate) async fn add(
    pool: &PgPool,
    owner: &str,
    index: &[u8; 32],
    sealed: &[u8],
    bearer: &UsedToken,
    proof: &UsedToken,
) -> Result<i64, StoreError> {
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO sysacct_lists (owner_did) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(owner)
        .execute(&mut *tx)
        .await?;
    // Serialises A's writes: the cap and `rev` hold under concurrency.
    let rev: i64 = sqlx::query_scalar("SELECT rev FROM sysacct_lists WHERE owner_did = $1 FOR UPDATE")
        .bind(owner)
        .fetch_one(&mut *tx)
        .await?;
    sweep(&mut tx).await?;
    if !record(&mut tx, bearer).await? {
        return Err(StoreError::Refused(Refusal::BearerUsed));
    }
    if !record(&mut tx, proof).await? {
        return Err(StoreError::Refused(Refusal::ProofUsed));
    }
    let present: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sysacct_entries WHERE owner_did = $1 AND member_index = $2)")
        .bind(owner)
        .bind(index.as_slice())
        .fetch_one(&mut *tx)
        .await?;
    if present {
        // Idempotent: `addedAt` and `rev` kept, the tokens used.
        tx.commit().await?;
        return Ok(rev);
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sysacct_entries WHERE owner_did = $1")
        .bind(owner)
        .fetch_one(&mut *tx)
        .await?;
    if count >= MAX_ENTRIES {
        return Err(StoreError::Refused(Refusal::ListFull));
    }
    sqlx::query(
        "INSERT INTO sysacct_entries (owner_did, member_index, member_sealed, added_at) VALUES ($1, $2, $3, now()) \
         ON CONFLICT DO NOTHING",
    )
    .bind(owner)
    .bind(index.as_slice())
    .bind(sealed)
    .execute(&mut *tx)
    .await?;
    let rev: i64 = sqlx::query_scalar("UPDATE sysacct_lists SET rev = rev + 1 WHERE owner_did = $1 RETURNING rev")
        .bind(owner)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(rev)
}

/// `removeSystemAccount`'s transaction. Returns the list's `rev` (0 for a
/// list never written; no list row is made for it).
pub(crate) async fn remove(pool: &PgPool, owner: &str, index: &[u8; 32], bearer: &UsedToken) -> Result<i64, StoreError> {
    let mut tx = pool.begin().await?;
    let rev: Option<i64> = sqlx::query_scalar("SELECT rev FROM sysacct_lists WHERE owner_did = $1 FOR UPDATE")
        .bind(owner)
        .fetch_optional(&mut *tx)
        .await?;
    sweep(&mut tx).await?;
    if !record(&mut tx, bearer).await? {
        return Err(StoreError::Refused(Refusal::BearerUsed));
    }
    let Some(rev) = rev else {
        tx.commit().await?;
        return Ok(0);
    };
    let gone = sqlx::query("DELETE FROM sysacct_entries WHERE owner_did = $1 AND member_index = $2")
        .bind(owner)
        .bind(index.as_slice())
        .execute(&mut *tx)
        .await?
        .rows_affected();
    let rev = if gone > 0 {
        sqlx::query_scalar("UPDATE sysacct_lists SET rev = rev + 1 WHERE owner_did = $1 RETURNING rev")
            .bind(owner)
            .fetch_one(&mut *tx)
            .await?
    } else {
        rev
    };
    tx.commit().await?;
    Ok(rev)
}

/// Delete `index` from every list, after locking those lists in owner
/// order, and give each a new `rev`.
async fn drop_member(conn: &mut PgConnection, index: &[u8; 32]) -> Result<(), sqlx::Error> {
    let owners: Vec<String> = sqlx::query_scalar(
        "SELECT owner_did FROM sysacct_lists WHERE owner_did IN (SELECT owner_did FROM sysacct_entries WHERE member_index = $1) \
         ORDER BY owner_did FOR UPDATE",
    )
    .bind(index.as_slice())
    .fetch_all(&mut *conn)
    .await?;
    if owners.is_empty() {
        return Ok(());
    }
    let left: Vec<String> = sqlx::query_scalar("DELETE FROM sysacct_entries WHERE member_index = $1 RETURNING owner_did")
        .bind(index.as_slice())
        .fetch_all(&mut *conn)
        .await?;
    sqlx::query("UPDATE sysacct_lists SET rev = rev + 1 WHERE owner_did = ANY($1)")
        .bind(&left)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// `leaveSystemAccounts`' transaction.
pub(crate) async fn leave(pool: &PgPool, index: &[u8; 32], bearer: &UsedToken) -> Result<(), StoreError> {
    let mut tx = pool.begin().await?;
    drop_member(&mut tx, index).await?;
    sweep(&mut tx).await?;
    if !record(&mut tx, bearer).await? {
        return Err(StoreError::Refused(Refusal::BearerUsed));
    }
    tx.commit().await?;
    Ok(())
}

/// Whether `did` owns a list or (with an index) is on one.
pub(crate) async fn holds(pool: &PgPool, did: &str, index: Option<[u8; 32]>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sysacct_lists WHERE owner_did = $1) \
             OR EXISTS (SELECT 1 FROM sysacct_entries WHERE member_index = $2)",
    )
    .bind(did)
    .bind(index.as_ref().map(<[u8; 32]>::as_slice))
    .fetch_one(pool)
    .await
}

/// Forget `did` as an owner (its entries cascade) and, with its index, as a
/// listed account.
pub(crate) async fn forget(conn: &mut PgConnection, did: &str, index: Option<[u8; 32]>) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM sysacct_lists WHERE owner_did = $1")
        .bind(did)
        .execute(&mut *conn)
        .await?;
    if let Some(index) = index {
        drop_member(conn, &index).await?;
    }
    Ok(())
}

/// How the startup key check failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyCheck {
    /// No key, and entries are stored.
    Unset,
    /// Another key, and entries are stored.
    Mismatch,
    Db(String),
}

impl From<sqlx::Error> for KeyCheck {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(error_code(&e))
    }
}

/// The startup check (design §4.3).
pub(crate) async fn check_key(pool: &PgPool, keys: Option<Keys>) -> Result<Option<Keys>, KeyCheck> {
    let stored_entries: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sysacct_entries)")
        .fetch_one(pool)
        .await?;
    let Some(keys) = keys else {
        return if stored_entries { Err(KeyCheck::Unset) } else { Ok(None) };
    };
    let check = keys.check();
    let stored: Option<Vec<u8>> = sqlx::query_scalar("SELECT key_check FROM sysacct_key")
        .fetch_optional(pool)
        .await?;
    match stored {
        Some(c) if c.as_slice() == check.as_slice() => {}
        Some(_) if stored_entries => return Err(KeyCheck::Mismatch),
        // A first start with the key, or another key while nothing sealed
        // is stored: nothing is lost by recording this one.
        _ => {
            sqlx::query("INSERT INTO sysacct_key (id, key_check) VALUES (true, $1) ON CONFLICT (id) DO UPDATE SET key_check = EXCLUDED.key_check")
                .bind(check.as_slice())
                .execute(pool)
                .await?;
        }
    }
    Ok(Some(keys))
}

/// Test views of the tables (the source check keeps their names here).
#[cfg(test)]
pub(crate) mod test_rows {
    use sqlx::PgPool;

    /// Every byte of every `sysacct_` row, as text where it is text, for
    /// the at-rest check.
    pub async fn all_bytes(pool: &PgPool) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let lists: Vec<(String, i64)> = sqlx::query_as("SELECT owner_did, rev FROM sysacct_lists").fetch_all(pool).await.expect("lists");
        out.extend(lists.into_iter().map(|(o, r)| [o.into_bytes(), r.to_string().into_bytes()].concat()));
        let entries: Vec<(String, Vec<u8>, Vec<u8>, String)> =
            sqlx::query_as("SELECT owner_did, member_index, member_sealed, added_at::text FROM sysacct_entries").fetch_all(pool).await.expect("entries");
        out.extend(entries.into_iter().map(|(o, i, s, a)| [o.into_bytes(), i, s, a.into_bytes()].concat()));
        let tokens: Vec<(Vec<u8>, String)> =
            sqlx::query_as("SELECT token_hash, expires_at::text FROM sysacct_used_tokens").fetch_all(pool).await.expect("tokens");
        out.extend(tokens.into_iter().map(|(h, e)| [h, e.into_bytes()].concat()));
        let keys: Vec<Vec<u8>> = sqlx::query_scalar("SELECT key_check FROM sysacct_key").fetch_all(pool).await.expect("key");
        out.extend(keys);
        out
    }

    pub async fn count(pool: &PgPool, table: &str) -> i64 {
        let sql = match table {
            "lists" => "SELECT count(*) FROM sysacct_lists",
            "entries" => "SELECT count(*) FROM sysacct_entries",
            "tokens" => "SELECT count(*) FROM sysacct_used_tokens",
            "key" => "SELECT count(*) FROM sysacct_key",
            other => panic!("no table {other}"),
        };
        sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
    }

    pub async fn rev(pool: &PgPool, owner: &str) -> Option<i64> {
        sqlx::query_scalar("SELECT rev FROM sysacct_lists WHERE owner_did = $1").bind(owner).fetch_optional(pool).await.expect("rev")
    }

    /// Expire every used token (the sweep's test).
    pub async fn expire_tokens(pool: &PgPool) {
        sqlx::query("UPDATE sysacct_used_tokens SET expires_at = now() - interval '1 minute'").execute(pool).await.expect("expire");
    }
}
