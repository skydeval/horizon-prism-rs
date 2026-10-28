//! Audiences held by this AppView (private-audiences design): members'
//! `tools.kryphocron.policy.audience` lists are member state, written only
//! by `createAudience`, `updateAudience` and `deleteAudience` and read by
//! the owner through `listAudiences`. No repository record is read.
//!
//! Here: the record check (§2.1), the inputs, the three writes (§2.2-§2.4)
//! and the list's read (§2.5). The handlers are in `handlers`. Members and
//! names are never logged.

use chrono::{DateTime, SecondsFormat, Utc};
use proto_blue::common::strings::grapheme_len;
use proto_blue::syntax::{Datetime, Did, RecordKey, Tid};
use serde_json::{Map, Value, json};
use sqlx::{PgConnection, PgPool};

use super::{Applied, ingest, owners};
use crate::aggregations::scope_state::ScopeState;
use crate::firehose::classifier::NSID_AUDIENCE;
use crate::xrpc::errors::XrpcError;

/// Audiences one member may hold.
pub(crate) const AUDIENCES_MAX: i64 = 200;
/// Members one audience may list, the owner aside.
pub(crate) const MEMBERS_MAX: usize = 1000;
/// A member DID's bytes, at most (a `did:plc` is 32).
pub(crate) const MEMBER_DID_MAX: usize = 256;
/// A write's body is refused unparsed past this many bytes.
pub(crate) const BODY_MAX: usize = 512 * 1024;
/// A name's graphemes and bytes, at most, once trimmed.
const NAME_GRAPHEMES_MAX: usize = 64;
const NAME_BYTES_MAX: usize = 640;
/// The modes a record may name.
const MODES: [&str; 5] = ["list", "everyone", "followers", "following", "nobody"];
/// A `listAudiences` page ends once its audiences hold this many members.
pub(crate) const PAGE_MEMBERS: i64 = 20_000;
/// Record keys minted for one create before it fails.
const MINT_TRIES: usize = 3;

/// A checked audience record (§2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    /// `None`: no mode, read as `list`.
    pub mode: Option<&'static str>,
    pub name: Option<String>,
    pub members: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn invalid(message: impl Into<String>) -> XrpcError {
    XrpcError::invalid(message)
}

fn datetime(fields: &Map<String, Value>, key: &str) -> Result<DateTime<Utc>, XrpcError> {
    fields
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| Datetime::is_valid(s))
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
        .ok_or_else(|| invalid(format!("Error: record.{key} must be a datetime")))
}

/// The record check shared by create and update (§2.1), in its order.
/// Other keys are dropped. Nothing about any member is looked up.
///
/// # Errors
///
/// `InvalidRequest`, its message `"Error: "`-prefixed.
pub(crate) fn check_record(record: &Value, caller: &str) -> Result<Record, XrpcError> {
    let fields = record.as_object().ok_or_else(|| invalid("Error: record must be an object"))?;
    match fields.get("$type") {
        None => {}
        Some(Value::String(t)) if t == NSID_AUDIENCE => {}
        Some(_) => return Err(invalid(format!("Error: record.$type must be {NSID_AUDIENCE}"))),
    }
    let mode = match fields.get("mode") {
        None | Some(Value::Null) => None,
        Some(Value::String(m)) => Some(
            MODES
                .into_iter()
                .find(|k| k == m)
                .ok_or_else(|| invalid("Error: record.mode must be one of list, everyone, followers, following, nobody"))?,
        ),
        Some(_) => return Err(invalid("Error: record.mode must be one of list, everyone, followers, following, nobody")),
    };
    let name = match fields.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(n)) => {
            let n = n.trim();
            if n.is_empty() {
                return Err(invalid("Error: record.name must not be empty"));
            }
            if grapheme_len(n) > NAME_GRAPHEMES_MAX {
                return Err(invalid(format!("Error: record.name must not be longer than {NAME_GRAPHEMES_MAX} graphemes")));
            }
            if n.len() > NAME_BYTES_MAX {
                return Err(invalid(format!("Error: record.name must not be longer than {NAME_BYTES_MAX} bytes")));
            }
            Some(n.to_string())
        }
        Some(_) => return Err(invalid("Error: record.name must be a string")),
    };
    let members = match fields.get("members") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => {
            let mut out: Vec<String> = Vec::with_capacity(items.len().min(MEMBERS_MAX + 1));
            let mut seen = std::collections::HashSet::new();
            for item in items {
                let did = item
                    .as_str()
                    .filter(|d| d.len() <= MEMBER_DID_MAX && Did::is_valid(d))
                    .ok_or_else(|| invalid(format!("Error: record.members must hold DIDs of at most {MEMBER_DID_MAX} bytes")))?;
                if did != caller && seen.insert(did) {
                    out.push(did.to_string());
                }
            }
            if out.len() > MEMBERS_MAX {
                return Err(invalid(format!("Error: record.members must not hold more than {MEMBERS_MAX} DIDs")));
            }
            out
        }
        Some(_) => return Err(invalid("Error: record.members must be an array")),
    };
    let created_at = datetime(fields, "createdAt")?;
    let updated_at = datetime(fields, "updatedAt")?;
    Ok(Record { mode, name, members, created_at, updated_at })
}

/// A write's body as an object, refused unparsed past [`BODY_MAX`].
fn object(body: &[u8]) -> Result<Map<String, Value>, XrpcError> {
    if body.len() > BODY_MAX {
        return Err(invalid("Error: input is too large"));
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(m)) => Ok(m),
        _ => Err(invalid("Error: input must be an object")),
    }
}

/// `createAudience`'s input (§2.2): `{record}`; an `rkey` is refused.
///
/// # Errors
///
/// `InvalidRequest`.
pub(crate) fn create_input(body: &[u8], caller: &str) -> Result<Record, XrpcError> {
    let input = object(body)?;
    if input.contains_key("rkey") {
        return Err(invalid("Error: rkey is not accepted: this AppView mints the record key"));
    }
    check_record(input.get("record").unwrap_or(&Value::Null), caller)
}

/// `uri` as one of `caller`'s audiences, by parsing alone: no lookup, so
/// the answer says nothing about another account's rows (§2.3).
fn own_uri(input: &Map<String, Value>, caller: &str) -> Result<String, XrpcError> {
    let refused = || invalid("Error: uri must name one of the caller's audiences");
    let uri = input.get("uri").and_then(Value::as_str).ok_or_else(refused)?;
    let rest = uri.strip_prefix("at://").ok_or_else(refused)?;
    let mut parts = rest.split('/');
    let (Some(did), Some(collection), Some(rkey), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return Err(refused());
    };
    if did != caller || collection != NSID_AUDIENCE || !RecordKey::is_valid(rkey) {
        return Err(refused());
    }
    Ok(uri.to_string())
}

/// `updateAudience`'s input (§2.3): `(uri, record, swapCid)`, all required.
///
/// # Errors
///
/// `InvalidRequest`.
pub(crate) fn update_input(body: &[u8], caller: &str) -> Result<(String, Record, String), XrpcError> {
    let input = object(body)?;
    let uri = own_uri(&input, caller)?;
    let swap = input
        .get("swapCid")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("Error: swapCid must be a string"))?
        .to_string();
    let record = check_record(input.get("record").unwrap_or(&Value::Null), caller)?;
    Ok((uri, record, swap))
}

/// `deleteAudience`'s input (§2.4): `{uri}`.
///
/// # Errors
///
/// `InvalidRequest`.
pub(crate) fn delete_input(body: &[u8], caller: &str) -> Result<String, XrpcError> {
    own_uri(&object(body)?, caller)
}

/// A TID from this AppView's clock with a random clock id (as draft ids).
fn mint() -> String {
    let micros = u64::try_from(Utc::now().timestamp_micros()).unwrap_or_default();
    Tid::from_timestamp(micros, rand::random::<u16>() & 0x3ff).into_inner()
}

/// How a write ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Written {
    /// The URI and its new token.
    Done { uri: String, cid: String },
    NotMember,
    LimitReached,
    InvalidSwap,
}

/// The DID's shared lock, then its owner lock (§2.2 step 2), then the
/// membership read again (step 3): whether the caller is still a member.
async fn lock_member(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    crate::db::locks::lock_dids_shared(&mut *conn, &[did.to_string()]).await?;
    owners::lock(&mut *conn, did).await?;
    owners::is_eligible(&mut *conn, did).await
}

/// Owner status after a create (§2.2 step 7), as ingest decided it: a
/// member with no private rows before becomes an owner and gets its
/// `private` job; a `following` audience of an existing owner gets an
/// `authz` run.
async fn owner_effects(conn: &mut PgConnection, did: &str, had_rows: bool, following: bool) -> Result<Applied, sqlx::Error> {
    if !had_rows {
        let applied = owners::reevaluate(&mut *conn, did).await?;
        if applied.owners_joined.iter().any(|d| d == did) {
            ingest::enqueue_private(&mut *conn, did).await?;
        }
        return Ok(applied);
    }
    if following && owners::is_owner(&mut *conn, did).await? {
        ingest::enqueue_authz(&mut *conn, did).await?;
    }
    Ok(Applied::default())
}

/// The insert of a create (§2.2 step 6); `None` when the URI is taken.
async fn insert(conn: &mut PgConnection, did: &str, rkey: &str, token: &str, record: &Record) -> Result<Option<String>, sqlx::Error> {
    let uri = format!("at://{did}/{NSID_AUDIENCE}/{rkey}");
    sqlx::query_scalar(
        "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, name, display_filled, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $4, $5, $6, $7, true, $8, $9) ON CONFLICT (uri) DO NOTHING RETURNING uri",
    )
    .bind(&uri)
    .bind(did)
    .bind(rkey)
    .bind(token)
    .bind(record.mode)
    .bind(&record.members)
    .bind(&record.name)
    .bind(record.created_at)
    .bind(record.updated_at)
    .fetch_optional(conn)
    .await
}

/// Each DID's place in the in-memory owner set, from the database under the
/// write guard (§11, "The in-memory set"), so whichever task runs last
/// applies the current answer; the transaction's report when the read
/// fails. Takes the write guard only when the report names a DID.
pub(crate) async fn settle_owner_set(scope: &ScopeState, pool: &PgPool, applied: &Applied) {
    if applied.owners_joined.is_empty() && applied.owners_left.is_empty() {
        return;
    }
    let mut sets = scope.write_guard().await;
    let reported = applied.owners_joined.iter().map(|d| (d, true)).chain(applied.owners_left.iter().map(|d| (d, false)));
    for (did, joined) in reported {
        let now = owner_now(pool, did).await;
        #[cfg(test)]
        let now = if crate::test_support::hooks::injected_in(crate::test_support::hooks::OWNER_SET_FROM_REPORT, pool, did) { None } else { now };
        let owner = now.unwrap_or(joined);
        if owner {
            sets.private_owners.insert(did.clone());
        } else {
            sets.private_owners.remove(did);
        }
    }
}

/// Whether `did` is an owner now; `None` when the read fails (logged).
async fn owner_now(pool: &PgPool, did: &str) -> Option<bool> {
    let read = async {
        let mut conn = pool.acquire().await?;
        owners::is_owner(&mut conn, did).await
    };
    match read.await {
        Ok(owner) => Some(owner),
        Err(error) => {
            tracing::warn!(event = "owner_set_read_failed", %error, "owner status not read; the transaction's report applies");
            None
        }
    }
}

/// `createAudience`'s write (§2.2), in one transaction under the scope
/// read guard.
///
/// # Errors
///
/// Database errors, and a record key that stayed taken after three mints.
pub(crate) async fn create(pool: &PgPool, scope: &ScopeState, did: &str, record: &Record) -> Result<Written, sqlx::Error> {
    // Between the gate and the write (T-A8), before the guard, so that a
    // removal marker can commit here.
    #[cfg(test)]
    crate::test_support::hooks::pause_in(crate::test_support::hooks::AUDIENCE_BEFORE_LOCK, pool, did).await;
    let guard = scope.read_guard().await;
    let mut tx = pool.begin().await?;
    if !lock_member(&mut tx, did).await? {
        return Ok(Written::NotMember);
    }
    let held: i64 = sqlx::query_scalar("SELECT count(*) FROM private_audiences WHERE owner_did = $1")
        .bind(did)
        .fetch_one(&mut *tx)
        .await?;
    if held >= AUDIENCES_MAX {
        return Ok(Written::LimitReached);
    }
    let had_rows = ingest::has_owner_rows(&mut tx, did).await?;
    let mut stored = None;
    for _ in 0..MINT_TRIES {
        let rkey = mint();
        if let Some(uri) = insert(&mut tx, did, &rkey, &rkey, record).await? {
            stored = Some((uri, rkey));
            break;
        }
    }
    let Some((uri, token)) = stored else {
        return Err(sqlx::Error::Protocol("no free audience record key after three mints".to_string()));
    };
    let applied = owner_effects(&mut tx, did, had_rows, record.mode == Some("following")).await?;
    tx.commit().await?;
    // The read guard goes before the write guard: a task asking for the
    // write guard while it holds a read guard would wait for itself.
    drop(guard);
    settle_owner_set(scope, pool, &applied).await;
    Ok(Written::Done { uri, cid: token })
}

/// `updateAudience`'s write (§2.3): the row replaced if its token is
/// `swap`. Changes no owner status.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn update(pool: &PgPool, scope: &ScopeState, did: &str, uri: &str, record: &Record, swap: &str) -> Result<Written, sqlx::Error> {
    #[cfg(test)]
    let _ = crate::test_support::hooks::injected_in(crate::test_support::hooks::AUDIENCE_UPDATE_REACHED, pool, did);
    #[cfg(test)]
    crate::test_support::hooks::pause_in(crate::test_support::hooks::AUDIENCE_BEFORE_LOCK, pool, did).await;
    let guard = scope.read_guard().await;
    let mut tx = pool.begin().await?;
    if !lock_member(&mut tx, did).await? {
        return Ok(Written::NotMember);
    }
    // A fresh token, never the one it replaces.
    let token = std::iter::repeat_with(mint).find(|t| t != swap).unwrap_or_default();
    let updated: Option<String> = sqlx::query_scalar(
        "UPDATE private_audiences SET mode = $4, name = $5, members = $6, updated_at = $7, cid = $8, rev = $8, display_filled = true \
         WHERE uri = $1 AND owner_did = $2 AND cid = $3 RETURNING uri",
    )
    .bind(uri)
    .bind(did)
    .bind(swap)
    .bind(record.mode)
    .bind(&record.name)
    .bind(&record.members)
    .bind(record.updated_at)
    .bind(&token)
    .fetch_optional(&mut *tx)
    .await?;
    if updated.is_none() {
        return Ok(Written::InvalidSwap);
    }
    if record.mode == Some("following") && owners::is_owner(&mut tx, did).await? {
        ingest::enqueue_authz(&mut tx, did).await?;
    }
    tx.commit().await?;
    drop(guard);
    Ok(Written::Done { uri: uri.to_string(), cid: token })
}

/// `deleteAudience`'s write (§2.4): the row gone, if there was one, and the
/// owner re-evaluated. Whether the caller was still a member.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn delete(pool: &PgPool, scope: &ScopeState, did: &str, uri: &str) -> Result<bool, sqlx::Error> {
    let guard = scope.read_guard().await;
    let mut tx = pool.begin().await?;
    if !lock_member(&mut tx, did).await? {
        return Ok(false);
    }
    let removed = sqlx::query("DELETE FROM private_audiences WHERE uri = $1 AND owner_did = $2")
        .bind(uri)
        .bind(did)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        > 0;
    let applied = if removed { owners::reevaluate(&mut tx, did).await? } else { Applied::default() };
    #[cfg(test)]
    crate::test_support::hooks::pause_in(crate::test_support::hooks::AUDIENCE_DELETE_BEFORE_COMMIT, pool, did).await;
    tx.commit().await?;
    drop(guard);
    #[cfg(test)]
    crate::test_support::hooks::pause_in(crate::test_support::hooks::AUDIENCE_BEFORE_WRITE_GUARD, pool, did).await;
    settle_owner_set(scope, pool, &applied).await;
    Ok(true)
}

/// One of the caller's audiences, as listed.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct Listed {
    pub uri: String,
    pub rkey: String,
    pub cid: String,
    pub mode: Option<String>,
    pub name: Option<String>,
    pub members: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// The page's members through this row.
    pub through: i64,
}

impl Listed {
    /// `{uri, cid, record}` (§2.5): the record rebuilt from the columns,
    /// `mode` and `name` left out when there are none.
    #[must_use]
    pub(crate) fn view(&self) -> Value {
        let mut record = Map::new();
        record.insert("$type".into(), json!(NSID_AUDIENCE));
        if let Some(mode) = &self.mode {
            record.insert("mode".into(), json!(mode));
        }
        if let Some(name) = &self.name {
            record.insert("name".into(), json!(name));
        }
        record.insert("members".into(), json!(self.members));
        record.insert("createdAt".into(), json!(self.created_at.to_rfc3339_opts(SecondsFormat::Millis, true)));
        record.insert("updatedAt".into(), json!(self.updated_at.to_rfc3339_opts(SecondsFormat::Millis, true)));
        json!({"uri": self.uri, "cid": self.cid, "record": Value::Object(record)})
    }
}

/// A page of `did`'s audiences below `before`, newest first: at most
/// `limit` rows, cut in SQL once the members summed before a row reach
/// [`PAGE_MEMBERS`] (the row that crosses it is kept).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn list(pool: &PgPool, did: &str, before: Option<&str>, limit: i64) -> Result<Vec<Listed>, sqlx::Error> {
    sqlx::query_as(
        "SELECT uri, rkey, cid, mode, name, members, created_at, updated_at, before + cardinality(members) AS through \
         FROM (SELECT uri, rkey, cid, mode, name, members, created_at, updated_at, \
                      (sum(cardinality(members)) OVER (ORDER BY rkey DESC) - cardinality(members))::bigint AS before \
               FROM private_audiences WHERE owner_did = $1 AND ($2::text IS NULL OR rkey < $2) \
               ORDER BY rkey DESC LIMIT $3) p \
         WHERE before < $4 ORDER BY rkey DESC",
    )
    .bind(did)
    .bind(before)
    .bind(limit)
    .bind(PAGE_MEMBERS)
    .fetch_all(pool)
    .await
}

/// A page's cursor (§2.5): the last row's `rkey`, when the page is full or
/// was cut by its members.
#[must_use]
pub(crate) fn next_cursor(rows: &[Listed], limit: usize) -> Option<String> {
    let last = rows.last()?;
    (rows.len() >= limit || last.through >= PAGE_MEMBERS).then(|| last.rkey.clone())
}

/// Tests: an audience row stored as a repository write once stored it, at
/// `rev` with `cid`, last writer wins, with the owner effects a write has
/// ([`owner_effects`]; an existing owner's `following` audience queues
/// `authz` unless `backfill`). The read-rule fixtures seed and edit
/// audiences through it, so their tests keep their meaning now that no
/// repository record is read. Members are taken as given, junk dropped.
/// Call inside a transaction (the owner lock).
///
/// # Errors
///
/// Database errors.
#[cfg(test)]
pub(crate) async fn seed(
    conn: &mut PgConnection,
    owner: &str,
    rkey: &str,
    cid: &str,
    rev: &str,
    record: &Value,
    backfill: bool,
) -> Result<Applied, sqlx::Error> {
    owners::lock(&mut *conn, owner).await?;
    let had_rows = ingest::has_owner_rows(&mut *conn, owner).await?;
    let text = |k: &str| record.get(k).and_then(Value::as_str).map(str::to_string);
    let members: Vec<String> = record
        .get("members")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).filter(|d| Did::is_valid(d)).map(str::to_string).collect())
        .unwrap_or_default();
    let mode = text("mode");
    let changed = sqlx::query(
        "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, name, display_filled) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true) \
         ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, mode = EXCLUDED.mode, \
           members = EXCLUDED.members, name = EXCLUDED.name, updated_at = now() \
         WHERE private_audiences.rev < EXCLUDED.rev",
    )
    .bind(format!("at://{owner}/{NSID_AUDIENCE}/{rkey}"))
    .bind(owner)
    .bind(rkey)
    .bind(cid)
    .bind(rev)
    .bind(&mode)
    .bind(&members)
    .bind(text("name"))
    .execute(&mut *conn)
    .await?
    .rows_affected()
        > 0;
    if !changed {
        return Ok(Applied::default());
    }
    let following = mode.as_deref() == Some("following");
    if !had_rows {
        if !owners::is_eligible(&mut *conn, owner).await? {
            return Ok(Applied { changed, ..Applied::default() });
        }
        return Ok(Applied { changed, ..owner_effects(conn, owner, false, following).await? });
    }
    let effects = if backfill { Applied::default() } else { owner_effects(conn, owner, true, following).await? };
    Ok(Applied { changed, ..effects })
}

/// Tests: an audience row deleted at `rev` (`rev <= R`), and the owner
/// re-evaluated when it went. Call inside a transaction.
///
/// # Errors
///
/// Database errors.
#[cfg(test)]
pub(crate) async fn unseed(conn: &mut PgConnection, uri: &str, rev: &str) -> Result<Applied, sqlx::Error> {
    let Some(owner) = crate::db::locks::uri_author(uri) else {
        return Ok(Applied::default());
    };
    owners::lock(&mut *conn, owner).await?;
    let removed = sqlx::query("DELETE FROM private_audiences WHERE uri = $1 AND rev <= $2")
        .bind(uri)
        .bind(rev)
        .execute(&mut *conn)
        .await?
        .rows_affected()
        > 0;
    if !removed {
        return Ok(Applied::default());
    }
    Ok(Applied { changed: true, ..owners::reevaluate(conn, owner).await? })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ME: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const YOU: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    fn base() -> Value {
        json!({"createdAt": "2026-10-01T00:00:00Z", "updatedAt": "2026-10-02T00:00:00.123Z"})
    }

    #[test]
    fn a_record_is_checked_and_normalised() {
        let mut r = base();
        r["mode"] = json!("following");
        r["name"] = json!("\u{2003} Friends\n");
        r["members"] = json!([YOU, ME, YOU]);
        r["extra"] = json!(1);
        let checked = check_record(&r, ME).expect("valid");
        assert_eq!((checked.mode, checked.name.as_deref(), checked.members), (Some("following"), Some("Friends"), vec![YOU.to_string()]));
        // Absent and null mean none.
        let mut r = base();
        r["mode"] = Value::Null;
        r["members"] = Value::Null;
        let checked = check_record(&r, ME).expect("valid");
        assert_eq!((checked.mode, checked.name, checked.members.len()), (None, None, 0));
        // The name's limits: 64 graphemes of up to 10 bytes pass.
        let mut r = base();
        r["name"] = json!("\u{1F468}\u{200D}\u{1F469}x".repeat(16));
        assert!(check_record(&r, ME).is_ok());
        r["name"] = json!("x".repeat(64));
        assert!(check_record(&r, ME).is_ok());
        r["name"] = json!("x".repeat(65));
        assert!(check_record(&r, ME).is_err());
        assert!(check_record(&json!([]), ME).is_err());
    }

    #[test]
    fn uris_are_decided_by_parsing() {
        let own = |u: &str| own_uri(json!({"uri": u}).as_object().expect("object"), ME);
        assert!(own(&format!("at://{ME}/{NSID_AUDIENCE}/3kaaaaaaaaaa2")).is_ok());
        for bad in [
            format!("at://{YOU}/{NSID_AUDIENCE}/3kaaaaaaaaaa2"),
            format!("at://{ME}/app.bsky.graph.list/3kaaaaaaaaaa2"),
            format!("at://{ME}/{NSID_AUDIENCE}/.."),
            format!("at://{ME}/{NSID_AUDIENCE}/3k/x"),
            format!("at://{ME}/{NSID_AUDIENCE}"),
            "3kaaaaaaaaaa2".to_string(),
        ] {
            assert!(own(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_page_cursor_follows_the_cut() {
        let row = |rkey: &str, through: i64| Listed {
            uri: String::new(),
            rkey: rkey.into(),
            cid: String::new(),
            mode: None,
            name: None,
            members: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            through,
        };
        assert_eq!(next_cursor(&[], 50), None);
        assert_eq!(next_cursor(&[row("b", 10), row("a", 20)], 2).as_deref(), Some("a"));
        assert_eq!(next_cursor(&[row("b", 10), row("a", 20)], 3), None);
        assert_eq!(next_cursor(&[row("a", PAGE_MEMBERS)], 3).as_deref(), Some("a"));
    }
}
