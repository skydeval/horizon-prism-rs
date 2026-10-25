//! The tables (design §4). Every statement binds its values; a listed
//! account is bound only as its index and its sealed form, never in clear,
//! so neither SQL text nor a database's parameter log carries it. No
//! statement can raise a unique violation (`ON CONFLICT` throughout), so the
//! database never writes a `Key (…)=(…)` detail line either.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use super::tokens::UsedToken;
use super::{Keys, LockReason, MAX_ENTRIES, State};

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

/// The startup check (design §4.3, amendment 1 §4): the state, and whether
/// account events are kept for the catch-up. A locked state writes nothing.
/// Errors are SQLSTATEs.
pub(crate) async fn startup(pool: &PgPool, keys: Option<Keys>) -> Result<(State, bool), String> {
    startup_inner(pool, keys).await.map_err(|e| error_code(&e))
}

async fn startup_inner(pool: &PgPool, keys: Option<Keys>) -> Result<(State, bool), sqlx::Error> {
    let stored_entries: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sysacct_entries)")
        .fetch_one(pool)
        .await?;
    if !stored_entries {
        // No entry for a kept event to reach (amendment 1 R1-m1).
        sqlx::query("DELETE FROM sysacct_pending").execute(pool).await?;
    }
    let Some(keys) = keys else {
        return Ok((if stored_entries { State::Locked(LockReason::KeyUnset) } else { State::Off }, false));
    };
    let check = keys.check();
    let stored: Option<Vec<u8>> = sqlx::query_scalar("SELECT key_check FROM sysacct_key")
        .fetch_optional(pool)
        .await?;
    let record = match stored {
        Some(c) if c.as_slice() == check.as_slice() => false,
        Some(_) if stored_entries => return Ok((State::Locked(LockReason::KeyMismatch), false)),
        // Entries but no check (R1-m6): the key is right if it opens one.
        None if stored_entries => {
            let (owner, index, sealed): (String, Vec<u8>, Vec<u8>) =
                sqlx::query_as("SELECT owner_did, member_index, member_sealed FROM sysacct_entries LIMIT 1")
                    .fetch_one(pool)
                    .await?;
            if keys.open(&owner, &index, &sealed).is_none() {
                return Ok((State::Locked(LockReason::KeyMismatch), false));
            }
            true
        }
        // A first start with the key, or another key while nothing sealed
        // is stored: nothing is lost by recording this one.
        _ => true,
    };
    if record {
        sqlx::query("INSERT INTO sysacct_key (id, key_check) VALUES (true, $1) ON CONFLICT (id) DO UPDATE SET key_check = EXCLUDED.key_check")
            .bind(check.as_slice())
            .execute(pool)
            .await?;
    }
    let kept: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sysacct_pending)").fetch_one(pool).await?;
    Ok((State::On(super::Live { keys, serving: super::Serving::default() }), kept))
}

/// Keep `did` for the catch-up (amendment 1 §5). `confirmed`: Prism's
/// `account_deleted` purge ran for it; confirmed is never undone. A new event
/// moves `seen_at`, so a pass that started before it keeps the row (R2-4).
pub(crate) async fn note_pending(conn: &mut PgConnection, did: &str, confirmed: bool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sysacct_pending (did, confirmed) VALUES ($1, $2) \
         ON CONFLICT (did) DO UPDATE SET confirmed = sysacct_pending.confirmed OR EXCLUDED.confirmed, seen_at = now()",
    )
    .bind(did)
    .bind(confirmed)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Rows of `sysacct_pending` read per catch-up batch.
pub(crate) const CATCH_UP_BATCH: i64 = 10_000;

/// What one catch-up step did, in counts only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Step {
    /// Confirmed deletions of listed accounts applied this step.
    pub dropped: u64,
    /// Listed accounts queued for the identity refresher this step.
    pub queued: u64,
    /// Queued accounts the refresher has not finished with yet.
    pub waiting: u64,
    /// Rows left in `sysacct_pending` after the step.
    pub left: u64,
}

/// The catch-up (amendment 1 §6), one pass at a time. A pass:
///
/// 1. opens every entry, so the listed DIDs are known in memory only;
/// 2. reads `sysacct_pending` 10,000 rows at a time by DID; a confirmed row
///    on a list leaves every list (by its index alone, one DID per
///    transaction); an unconfirmed row on a list is queued for the identity
///    refresher, which decides and purges with the keys as with the key
///    present;
/// 3. waits, step after step, until the refresher has finished with every
///    queued DID: its row gone, or put back after a check with `settled_at`
///    after the queueing (a claim or a retry does not count; R2-1, R3-1);
/// 4. then deletes exactly the rows read (DID and `seen_at`), so a row
///    written meanwhile, or moved by a later event, stays for the next pass
///    (R2-4, R3-2).
///
/// Nothing written marks which rows are listed (R2-2). The rows stay until
/// the waits are done, so another process starting meanwhile finds them and
/// waits too (R5-1), and a restart starts the pass again.
// No `Debug`: a pass holds listed DIDs (R4-6).
#[derive(Default)]
pub(crate) struct CatchUp {
    pass: Option<Pass>,
}

/// A pass in progress, kept across steps, so a failed step loses nothing
/// already queued (R4-1).
struct Pass {
    /// Listed DIDs and their indexes, opened from the entries.
    listed: std::collections::HashMap<String, [u8; 32]>,
    /// The last DID read; `None` once every row has been read.
    after: Option<String>,
    /// Queued DIDs and when, until the refresher has finished with them.
    waiting: Vec<(String, DateTime<Utc>)>,
    /// Every row read (DID and `seen_at`), deleted only when the waits are
    /// done, so another process never finds the table empty while a check
    /// is still owed (R5-1), and a restart finds the rows again.
    read: Vec<(String, DateTime<Utc>)>,
}

impl CatchUp {
    /// One step. Errors are SQLSTATEs; the pass in progress is kept.
    pub(crate) async fn step(&mut self, pool: &PgPool, keys: &Keys) -> Result<Step, String> {
        self.step_inner(pool, keys).await.map_err(|e| error_code(&e))
    }

    async fn step_inner(&mut self, pool: &PgPool, keys: &Keys) -> Result<Step, sqlx::Error> {
        let mut step = Step::default();
        if self.pass.is_none() {
            let any: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sysacct_pending)").fetch_one(pool).await?;
            if !any {
                return Ok(step);
            }
            self.pass = Some(Pass { listed: listed_dids(pool, keys).await?, after: Some(String::new()), waiting: Vec::new(), read: Vec::new() });
        }
        let Some(pass) = self.pass.as_mut() else {
            return Ok(step);
        };
        read_rows(pool, pass, &mut step).await?;
        if !pass.waiting.is_empty() {
            // The whole queue, read and matched here: no statement names
            // the DIDs waited on.
            let rows: Vec<(String, Option<DateTime<Utc>>)> =
                sqlx::query_as("SELECT did, settled_at FROM identity_refresh").fetch_all(pool).await?;
            let queue: std::collections::HashMap<String, Option<DateTime<Utc>>> = rows.into_iter().collect();
            pass.waiting.retain(|(did, queued_at)| match queue.get(did) {
                None => false,
                Some(settled) => !settled.is_some_and(|t| t >= *queued_at),
            });
        }
        step.waiting = pass.waiting.len() as u64;
        if pass.waiting.is_empty() {
            // Exactly the rows read, a chunk at a time: one moved by a later
            // event, or written meanwhile, stays for the next pass (R3-2).
            while !pass.read.is_empty() {
                let n = pass.read.len().min(usize::try_from(CATCH_UP_BATCH).unwrap_or(10_000));
                let (dids, seen): (Vec<String>, Vec<DateTime<Utc>>) = pass.read[..n].iter().cloned().unzip();
                sqlx::query(
                    "DELETE FROM sysacct_pending p USING unnest($1::text[], $2::timestamptz[]) AS r(did, seen) \
                     WHERE p.did = r.did AND p.seen_at = r.seen",
                )
                .bind(&dids)
                .bind(&seen)
                .execute(pool)
                .await?;
                pass.read.drain(..n);
            }
            self.pass = None;
        }
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM sysacct_pending").fetch_one(pool).await?;
        step.left = u64::try_from(left).unwrap_or(0);
        Ok(step)
    }
}

/// Every listed DID, opened in memory: lists are small (31 at most each).
async fn listed_dids(pool: &PgPool, keys: &Keys) -> Result<std::collections::HashMap<String, [u8; 32]>, sqlx::Error> {
    let entries: Vec<(String, Vec<u8>, Vec<u8>)> =
        sqlx::query_as("SELECT owner_did, member_index, member_sealed FROM sysacct_entries").fetch_all(pool).await?;
    let mut listed = std::collections::HashMap::new();
    for (owner, index, sealed) in entries {
        if let (Some(did), Ok(index)) = (keys.open(&owner, &index, &sealed), <[u8; 32]>::try_from(index.as_slice())) {
            listed.insert(did, index);
        }
    }
    Ok(listed)
}

/// Step 2 of a pass: the rows not read yet, a batch at a time. Each batch's
/// waits and rows are in `pass` before the cursor moves, so a failure
/// leaves the pass where it was.
async fn read_rows(pool: &PgPool, pass: &mut Pass, step: &mut Step) -> Result<(), sqlx::Error> {
    while let Some(after) = pass.after.clone() {
        let rows: Vec<(String, bool, DateTime<Utc>)> =
            sqlx::query_as("SELECT did, confirmed, seen_at FROM sysacct_pending WHERE did > $1 ORDER BY did LIMIT $2")
                .bind(&after)
                .bind(CATCH_UP_BATCH)
                .fetch_all(pool)
                .await?;
        let Some((last, _, _)) = rows.last() else {
            pass.after = None;
            break;
        };
        let last = last.clone();
        let mut to_queue: Vec<String> = Vec::new();
        for (did, confirmed, _) in &rows {
            let Some(index) = pass.listed.get(did) else {
                continue;
            };
            if *confirmed {
                let mut tx = pool.begin().await?;
                drop_member(&mut tx, index).await?;
                tx.commit().await?;
                step.dropped += 1;
            } else {
                to_queue.push(did.clone());
            }
        }
        if !to_queue.is_empty() {
            // One statement for the batch's listed DIDs (a trace accepted
            // in amendment 1 §6: the refresher's queue shows them, as it
            // shows any DID it refreshes). A row already there keeps its
            // schedule.
            let queued_at: DateTime<Utc> = sqlx::query_scalar(
                "WITH q AS (INSERT INTO identity_refresh (did, reasons, not_before) SELECT d, $2, now() FROM unnest($1::text[]) AS d \
                 ON CONFLICT (did) DO UPDATE SET reasons = identity_refresh.reasons | EXCLUDED.reasons) SELECT now()",
            )
            .bind(&to_queue)
            .bind(crate::firehose::stream::IdentityKind::Account.reason_bit())
            .fetch_one(pool)
            .await?;
            step.queued += to_queue.len() as u64;
            pass.waiting.extend(to_queue.into_iter().map(|d| (d, queued_at)));
        }
        // Kept to delete when the waits are done (R5-1).
        pass.read.extend(rows.into_iter().map(|(d, _, t)| (d, t)));
        pass.after = Some(last);
    }
    Ok(())
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
            "pending" => "SELECT count(*) FROM sysacct_pending",
            other => panic!("no table {other}"),
        };
        sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
    }

    pub async fn rev(pool: &PgPool, owner: &str) -> Option<i64> {
        sqlx::query_scalar("SELECT rev FROM sysacct_lists WHERE owner_did = $1").bind(owner).fetch_optional(pool).await.expect("rev")
    }

    /// Keep `did` for the catch-up, as the locked paths do.
    pub async fn pending(pool: &PgPool, did: &str, confirmed: bool) {
        super::note_pending(&mut pool.acquire().await.expect("conn"), did, confirmed).await.expect("pending");
    }

    /// `did`'s kept row: whether it is confirmed.
    pub async fn pending_row(pool: &PgPool, did: &str) -> Option<bool> {
        sqlx::query_scalar("SELECT confirmed FROM sysacct_pending WHERE did = $1").bind(did).fetch_optional(pool).await.expect("pending")
    }

    /// Expire every used token (the sweep's test).
    pub async fn expire_tokens(pool: &PgPool) {
        sqlx::query("UPDATE sysacct_used_tokens SET expires_at = now() - interval '1 minute'").execute(pool).await.expect("expire");
    }
}
