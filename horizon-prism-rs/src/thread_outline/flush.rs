//! One flush (thread outline design §3.2): a batch of stamped ops, folded per
//! post and written in one transaction with the writer's progress.
//!
//! Order: actors (select, then insert the missing), the fold, creates (two
//! statements), updates, deletes, account status, holes, and progress with
//! `GREATEST`. Every write is rev-guarded, so a duplicate copy of an op
//! writes no tuple.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use chrono::{DateTime, Utc};
use sqlx::{Connection, PgConnection};

use super::id::StreamId;
use super::ops::{Key, Op, PostOp};
use super::{KEY_THROUGH, METRIC_ACTOR_IDS, METRIC_FLUSH_SECONDS, METRIC_HOLES, METRIC_MISSED, METRIC_OPS};

/// A flush failure, classed as the firehose writer classes its own (§3.4).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlushError {
    /// Connection loss, a timeout, a serialization failure: retried.
    #[error("transient outline flush failure: {0}")]
    Transient(String),
    /// SQLSTATE class 22 or 23: re-flushed op by op.
    #[error("deterministic outline flush failure: {0}")]
    Deterministic(String),
}

impl From<sqlx::Error> for FlushError {
    fn from(e: sqlx::Error) -> Self {
        let deterministic = match &e {
            sqlx::Error::Database(db) => db.code().is_some_and(|c| c.starts_with("22") || c.starts_with("23")),
            _ => false,
        };
        if deterministic { Self::Deterministic(e.to_string()) } else { Self::Transient(e.to_string()) }
    }
}

/// What a flush moves besides its ops (§3.2 step 8).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    /// `through = GREATEST(stored, this)`.
    pub through: Option<StreamId>,
    /// Set `through` to this whatever is stored: a stream reset (§3.3
    /// step 4).
    pub rebase: Option<StreamId>,
    /// `complete_since = GREATEST(stored, this)`.
    pub complete_since_ms: Option<i64>,
}

/// What a committed flush left stored.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Committed {
    pub through: Option<StreamId>,
    pub complete_since_ms: Option<i64>,
}

/// One post's ops folded in stream order (§3.2 step 2).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Folded {
    Create { op: PostOp, seen: DateTime<Utc> },
    Update { op: PostOp },
    /// A delete, with the create it beat or that a tombstone of it refused,
    /// which is still inserted, deleted, so its children are no orphans
    /// (R4-8).
    Delete { rev: i64, seen: DateTime<Utc>, create: Option<(PostOp, DateTime<Utc>)> },
}

fn fold_one(state: Option<Folded>, op: &Op, seen: DateTime<Utc>) -> Option<Folded> {
    use Folded as F;
    let next = match op {
        Op::Create(c) => F::Create { op: c.clone(), seen },
        Op::Update(u) => F::Update { op: u.clone() },
        Op::Delete { rev, .. } => F::Delete { rev: *rev, seen, create: None },
        _ => return state,
    };
    let Some(state) = state else {
        return Some(next);
    };
    Some(match (state, next) {
        // A create and an update merge into a create with the newer
        // fields (R3-7); its `seen_at` is the create's.
        (F::Create { op: c, seen }, F::Update { op: u }) => F::Create { op: if u.rev > c.rev { u } else { c }, seen },
        (F::Update { op: u }, F::Create { op: c, seen }) => F::Create { op: if c.rev > u.rev { c } else { u }, seen },
        (F::Create { op: a, seen: sa }, F::Create { op: b, seen: sb }) => {
            if b.rev > a.rev { F::Create { op: b, seen: sb } } else { F::Create { op: a, seen: sa } }
        }
        (F::Update { op: a }, F::Update { op: b }) => F::Update { op: if b.rev > a.rev { b } else { a } },
        // A delete beats a create or update of the same rev.
        (F::Create { op: c, seen: cs }, F::Delete { rev, seen, .. }) => {
            if rev >= c.rev { F::Delete { rev, seen, create: Some((c, cs)) } } else { F::Create { op: c, seen: cs } }
        }
        (F::Update { op: u }, F::Delete { rev, seen, .. }) => {
            if rev >= u.rev { F::Delete { rev, seen, create: None } } else { F::Update { op: u } }
        }
        (F::Delete { rev, seen, create }, F::Create { op: c, seen: cs }) => {
            if c.rev > rev { F::Create { op: c, seen: cs } } else { F::Delete { rev, seen, create: create.or(Some((c, cs))) } }
        }
        (F::Delete { rev, seen, create }, F::Update { op: u }) => {
            if u.rev > rev { F::Update { op: u } } else { F::Delete { rev, seen, create } }
        }
        (F::Delete { rev: a, seen: sa, create: ca }, F::Delete { rev: b, seen: sb, create: cb }) => {
            if b > a { F::Delete { rev: b, seen: sb, create: cb.or(ca) } } else { F::Delete { rev: a, seen: sa, create: ca.or(cb) } }
        }
    })
}

fn post_key(op: &Op) -> Option<(String, i64)> {
    match op {
        Op::Create(p) | Op::Update(p) => Some((p.author.clone(), p.rkey)),
        Op::Delete { author, rkey, .. } => Some((author.clone(), *rkey)),
        _ => None,
    }
}

fn seen_at(id: StreamId) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(id.ms_i64()).unwrap_or_else(Utc::now)
}

/// Every DID a batch names: authors, parents, roots, holes and statuses.
fn dids(ops: &[(StreamId, Op)]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |d: &str| out.push(d.to_string());
    for (_, op) in ops {
        match op {
            Op::Create(p) | Op::Update(p) => {
                push(&p.author);
                for k in [&p.parent, &p.root].into_iter().flatten() {
                    push(&k.did);
                }
            }
            Op::Delete { author, .. } => push(author),
            Op::Hole { node, .. } | Op::HoleAtParentOf { node } => push(&node.did),
            Op::Status { did, .. } => push(did),
            Op::Missed(_) => {}
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Actors: select the batch's ids, then insert only the missing ones.
async fn actor_ids(conn: &mut PgConnection, dids: &[String]) -> Result<HashMap<String, i32>, sqlx::Error> {
    let mut ids: HashMap<String, i32> = HashMap::with_capacity(dids.len());
    if dids.is_empty() {
        return Ok(ids);
    }
    let found: Vec<(i32, String)> = sqlx::query_as("SELECT id, did FROM outline_actors WHERE did = ANY($1)")
        .bind(dids)
        .fetch_all(&mut *conn)
        .await?;
    ids.extend(found.into_iter().map(|(id, did)| (did, id)));
    let missing: Vec<String> = dids.iter().filter(|d| !ids.contains_key(*d)).cloned().collect();
    if !missing.is_empty() {
        let added: Vec<(i32, String)> = sqlx::query_as(
            "INSERT INTO outline_actors (did) SELECT d FROM unnest($1::text[]) AS d \
             WHERE NOT EXISTS (SELECT 1 FROM outline_actors a WHERE a.did = d) \
             ON CONFLICT (did) DO NOTHING RETURNING id, did",
        )
        .bind(&missing)
        .fetch_all(&mut *conn)
        .await?;
        if let Some(max) = added.iter().map(|(id, _)| *id).max() {
            metrics::gauge!(METRIC_ACTOR_IDS).set(f64::from(max));
        }
        ids.extend(added.into_iter().map(|(id, did)| (did, id)));
    }
    Ok(ids)
}

/// Columns of a post create or update, by actor id.
#[derive(Default)]
struct PostCols {
    author: Vec<i32>,
    rkey: Vec<i64>,
    pa: Vec<Option<i32>>,
    prk: Vec<Option<i64>>,
    ra: Vec<Option<i32>>,
    rrk: Vec<Option<i64>>,
    created_at: Vec<DateTime<Utc>>,
    seen_at: Vec<DateTime<Utc>>,
    rev: Vec<i64>,
}

impl PostCols {
    fn push(&mut self, ids: &HashMap<String, i32>, p: &PostOp, seen: DateTime<Utc>) {
        let id = |d: &str| ids.get(d).copied();
        let Some(author) = id(&p.author) else {
            return;
        };
        self.author.push(author);
        self.rkey.push(p.rkey);
        self.pa.push(p.parent.as_ref().and_then(|k| id(&k.did)));
        self.prk.push(p.parent.as_ref().map(|k| k.rkey));
        self.ra.push(p.root.as_ref().and_then(|k| id(&k.did)));
        self.rrk.push(p.root.as_ref().map(|k| k.rkey));
        self.created_at.push(p.created_at);
        self.seen_at.push(seen);
        self.rev.push(p.rev);
    }
}

const POST_UNNEST: &str = "unnest($1::int[], $2::bigint[], $3::int[], $4::bigint[], $5::int[], $6::bigint[], $7::timestamptz[], $8::timestamptz[], $9::bigint[]) \
     AS v(author, rkey, pa, prk, ra, rrk, created_at, seen_at, rev)";

fn bind_posts<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    c: &'q PostCols,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    q.bind(&c.author)
        .bind(&c.rkey)
        .bind(&c.pa)
        .bind(&c.prk)
        .bind(&c.ra)
        .bind(&c.rrk)
        .bind(&c.created_at)
        .bind(&c.seen_at)
        .bind(&c.rev)
}

/// Apply `ops` and `progress` in one transaction on `conn` (§3.2). On an
/// error nothing is written.
///
/// # Errors
///
/// [`FlushError`], classed for the caller's retry or op-by-op re-flush.
pub async fn apply(conn: &mut PgConnection, ops: &[(StreamId, Op)], progress: &Progress) -> Result<Committed, FlushError> {
    let started = Instant::now();
    let mut tx = conn.begin().await?;
    #[cfg(test)]
    test_failures(&mut tx, ops).await?;

    // 1. Actors.
    let ids = actor_ids(&mut tx, &dids(ops)).await?;

    // 2. Fold per post, in stream order.
    let mut folded: BTreeMap<(String, i64), Folded> = BTreeMap::new();
    for (id, op) in ops {
        if let Some(k) = post_key(op) {
            let state = folded.remove(&k);
            if let Some(f) = fold_one(state, op, seen_at(*id)) {
                folded.insert(k, f);
            }
        }
    }
    let (mut creates, mut updates) = (PostCols::default(), PostCols::default());
    let (mut d_author, mut d_rkey, mut d_rev, mut d_seen) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for ((author, rkey), f) in &folded {
        match f {
            Folded::Create { op, seen } => creates.push(&ids, op, *seen),
            Folded::Update { op } => updates.push(&ids, op, Utc::now()),
            Folded::Delete { rev, seen, create } => {
                if let Some((c, cs)) = create {
                    creates.push(&ids, c, *cs);
                }
                if let Some(a) = ids.get(author) {
                    d_author.push(*a);
                    d_rkey.push(*rkey);
                    d_rev.push(*rev);
                    d_seen.push(*seen);
                }
            }
        }
    }

    // 3. Creates no tombstone covers; then those a tombstone refused,
    // inserted deleted with their parent and root (N1, R4-8). A tombstone
    // whose key now has a row at its rev or above is no longer needed.
    if !creates.author.is_empty() {
        let sql = format!(
            "INSERT INTO outline_posts (author, rkey, parent_author, parent_rkey, root_author, root_rkey, created_at, seen_at, rev, deleted_at) \
             SELECT v.author, v.rkey, v.pa, v.prk, v.ra, v.rrk, v.created_at, v.seen_at, v.rev, NULL FROM {POST_UNNEST} \
             WHERE NOT EXISTS (SELECT 1 FROM outline_deleted d WHERE d.author = v.author AND d.rkey = v.rkey AND d.rev >= v.rev) \
             ON CONFLICT (author, rkey) DO UPDATE SET parent_author = EXCLUDED.parent_author, parent_rkey = EXCLUDED.parent_rkey, \
               root_author = EXCLUDED.root_author, root_rkey = EXCLUDED.root_rkey, created_at = EXCLUDED.created_at, \
               rev = EXCLUDED.rev, deleted_at = NULL \
             WHERE outline_posts.rev < EXCLUDED.rev"
        );
        bind_posts(sqlx::query(&sql), &creates).execute(&mut *tx).await?;
        let sql = format!(
            "INSERT INTO outline_posts (author, rkey, parent_author, parent_rkey, root_author, root_rkey, created_at, seen_at, rev, deleted_at) \
             SELECT v.author, v.rkey, v.pa, v.prk, v.ra, v.rrk, v.created_at, v.seen_at, GREATEST(v.rev, d.rev), d.seen_at FROM {POST_UNNEST} \
             JOIN outline_deleted d ON d.author = v.author AND d.rkey = v.rkey AND d.rev >= v.rev \
             ON CONFLICT (author, rkey) DO NOTHING"
        );
        bind_posts(sqlx::query(&sql), &creates).execute(&mut *tx).await?;
        sqlx::query(
            "DELETE FROM outline_deleted d USING unnest($1::int[], $2::bigint[]) AS k(author, rkey) \
             WHERE d.author = k.author AND d.rkey = k.rkey \
               AND EXISTS (SELECT 1 FROM outline_posts o WHERE o.author = d.author AND o.rkey = d.rkey AND o.rev >= d.rev)",
        )
        .bind(&creates.author)
        .bind(&creates.rkey)
        .execute(&mut *tx)
        .await?;
    }

    // 4. Updates never insert.
    if !updates.author.is_empty() {
        let sql = format!(
            "UPDATE outline_posts o SET parent_author = v.pa, parent_rkey = v.prk, root_author = v.ra, root_rkey = v.rrk, \
               created_at = v.created_at, rev = v.rev \
             FROM {POST_UNNEST} WHERE o.author = v.author AND o.rkey = v.rkey AND o.rev < v.rev"
        );
        bind_posts(sqlx::query(&sql), &updates).execute(&mut *tx).await?;
    }

    // 5. Deletes mark the row, beating a create of the same rev (a copy of
    // a delete already applied writes no tuple); with no row, a tombstone
    // keeps the highest rev.
    if !d_author.is_empty() {
        sqlx::query(
            "UPDATE outline_posts o SET deleted_at = v.seen_at, rev = v.rev \
             FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS v(author, rkey, rev, seen_at) \
             WHERE o.author = v.author AND o.rkey = v.rkey \
               AND (o.rev < v.rev OR (o.rev = v.rev AND o.deleted_at IS NULL))",
        )
        .bind(&d_author)
        .bind(&d_rkey)
        .bind(&d_rev)
        .bind(&d_seen)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO outline_deleted (author, rkey, rev, seen_at) \
             SELECT v.author, v.rkey, v.rev, v.seen_at FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS v(author, rkey, rev, seen_at) \
             WHERE NOT EXISTS (SELECT 1 FROM outline_posts o WHERE o.author = v.author AND o.rkey = v.rkey) \
             ON CONFLICT (author, rkey) DO UPDATE SET rev = EXCLUDED.rev WHERE outline_deleted.rev < EXCLUDED.rev",
        )
        .bind(&d_author)
        .bind(&d_rkey)
        .bind(&d_rev)
        .bind(&d_seen)
        .execute(&mut *tx)
        .await?;
    }

    // 6. Account status, per (author, relay), the highest seq winning.
    let mut status: BTreeMap<(i32, &str), (bool, i64)> = BTreeMap::new();
    for (_, op) in ops {
        if let Op::Status { did, relay, active, seq } = op
            && let Some(a) = ids.get(did)
        {
            let e = status.entry((*a, relay.as_str())).or_insert((*active, *seq));
            if *seq > e.1 {
                *e = (*active, *seq);
            }
        }
    }
    if !status.is_empty() {
        let (mut author, mut relay, mut active, mut seq) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for ((a, r), (act, s)) in &status {
            author.push(*a);
            relay.push((*r).to_string());
            active.push(*act);
            seq.push(*s);
        }
        sqlx::query(
            "INSERT INTO outline_gone (author, relay, active, seq) \
             SELECT * FROM unnest($1::int[], $2::text[], $3::bool[], $4::bigint[]) \
             ON CONFLICT (author, relay) DO UPDATE SET active = EXCLUDED.active, seq = EXCLUDED.seq \
             WHERE outline_gone.seq < EXCLUDED.seq",
        )
        .bind(&author)
        .bind(&relay)
        .bind(&active)
        .bind(&seq)
        .execute(&mut *tx)
        .await?;
    }

    // 7. Holes, and bad-rev deletes' holes at their stored parents.
    let (mut h_author, mut h_rkey, mut h_seen) = (Vec::new(), Vec::new(), Vec::new());
    let (mut p_author, mut p_rkey, mut p_seen) = (Vec::new(), Vec::new(), Vec::new());
    for (id, op) in ops {
        match op {
            Op::Hole { node, .. } => push_key(&ids, node, seen_at(*id), (&mut h_author, &mut h_rkey, &mut h_seen)),
            Op::HoleAtParentOf { node } => push_key(&ids, node, seen_at(*id), (&mut p_author, &mut p_rkey, &mut p_seen)),
            _ => {}
        }
    }
    if !h_author.is_empty() {
        sqlx::query(
            "INSERT INTO outline_holes (author, rkey, seen_at) \
             SELECT * FROM unnest($1::int[], $2::bigint[], $3::timestamptz[]) ON CONFLICT DO NOTHING",
        )
        .bind(&h_author)
        .bind(&h_rkey)
        .bind(&h_seen)
        .execute(&mut *tx)
        .await?;
    }
    if !p_author.is_empty() {
        sqlx::query(
            "INSERT INTO outline_holes (author, rkey, seen_at) \
             SELECT o.parent_author, o.parent_rkey, v.seen_at \
             FROM unnest($1::int[], $2::bigint[], $3::timestamptz[]) AS v(author, rkey, seen_at) \
             JOIN outline_posts o ON o.author = v.author AND o.rkey = v.rkey \
             WHERE o.parent_author IS NOT NULL ON CONFLICT DO NOTHING",
        )
        .bind(&p_author)
        .bind(&p_rkey)
        .bind(&p_seen)
        .execute(&mut *tx)
        .await?;
    }

    // 8. Progress, in the same transaction.
    let mut committed = Committed::default();
    if let Some(at) = progress.rebase {
        committed.through = Some(set_through(&mut tx, at).await?);
    } else if let Some(at) = progress.through {
        committed.through = Some(raise_through(&mut tx, at).await?);
    }
    if let Some(ms) = progress.complete_since_ms {
        committed.complete_since_ms = Some(super::raise_stored_complete_since(&mut tx, ms).await?);
    }
    tx.commit().await?;

    metrics::histogram!(METRIC_FLUSH_SECONDS).record(started.elapsed().as_secs_f64());
    for (_, op) in ops {
        metrics::counter!(METRIC_OPS, "op" => op.label()).increment(1);
        match op {
            Op::Hole { reason, .. } => metrics::counter!(METRIC_HOLES, "reason" => reason.as_str()).increment(1),
            Op::Missed(reason) => metrics::counter!(METRIC_MISSED, "reason" => reason.as_str()).increment(1),
            _ => {}
        }
    }
    Ok(committed)
}

type KeyCols<'a> = (&'a mut Vec<i32>, &'a mut Vec<i64>, &'a mut Vec<DateTime<Utc>>);

fn push_key(ids: &HashMap<String, i32>, node: &Key, seen: DateTime<Utc>, cols: KeyCols<'_>) {
    if let Some(a) = ids.get(&node.did) {
        cols.0.push(*a);
        cols.1.push(node.rkey);
        cols.2.push(seen);
    }
}

/// `through = GREATEST(stored, at)`, compared as `(ms, seq)`; the stored id
/// after. The parts are compared as numerics: a seq can be `u64::MAX`.
async fn raise_through(conn: &mut PgConnection, at: StreamId) -> Result<StreamId, sqlx::Error> {
    let stored: String = sqlx::query_scalar(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb($2::text), now()) \
         ON CONFLICT (key) DO UPDATE SET value = CASE \
           WHEN (split_part(worker_state.value #>> '{}', '-', 1)::numeric, split_part(worker_state.value #>> '{}', '-', 2)::numeric) \
              >= (split_part($2, '-', 1)::numeric, split_part($2, '-', 2)::numeric) \
           THEN worker_state.value ELSE EXCLUDED.value END, updated_at = now() \
         RETURNING value #>> '{}'",
    )
    .bind(KEY_THROUGH)
    .bind(at.to_string())
    .fetch_one(&mut *conn)
    .await?;
    Ok(StreamId::parse(&stored).unwrap_or(at))
}

/// `through = at`, whatever is stored (a stream reset).
async fn set_through(conn: &mut PgConnection, at: StreamId) -> Result<StreamId, sqlx::Error> {
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb($2::text), now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(KEY_THROUGH)
    .bind(at.to_string())
    .execute(&mut *conn)
    .await?;
    Ok(at)
}

/// The stored `through`.
///
/// # Errors
///
/// Any database error.
pub async fn stored_through(conn: &mut PgConnection) -> Result<Option<StreamId>, sqlx::Error> {
    let v: Option<String> = sqlx::query_scalar("SELECT value #>> '{}' FROM worker_state WHERE key = $1")
        .bind(KEY_THROUGH)
        .fetch_optional(conn)
        .await?;
    Ok(v.as_deref().and_then(StreamId::parse))
}

/// Test failures (T-O4): a transient failure keyed by the database's name,
/// and a deterministic one keyed `<database>/<did>` for a batch that holds a
/// create by that DID.
#[cfg(test)]
async fn test_failures(conn: &mut PgConnection, ops: &[(StreamId, Op)]) -> Result<(), FlushError> {
    use crate::test_support::hooks;
    let db: String = sqlx::query_scalar("SELECT current_database()::text").fetch_one(&mut *conn).await.unwrap_or_default();
    if hooks::injected(hooks::OUTLINE_FLUSH_TRANSIENT, &db) {
        return Err(FlushError::Transient("injected transient failure".into()));
    }
    for (_, op) in ops {
        if let Op::Create(p) = op
            && hooks::injected(hooks::OUTLINE_FLUSH_DETERMINISTIC, &format!("{db}/{}", p.author))
        {
            return Err(FlushError::Deterministic("injected deterministic failure".into()));
        }
    }
    Ok(())
}
