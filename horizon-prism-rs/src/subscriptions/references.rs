//! A member's references to lists, and the member phase that changes them
//! (design-v3 §4.3 "References", "Budgets", "Member phase").
//!
//! A reference is one member's list mute (`mute`) or list block (`block`)
//! of one list. Each member has a budget per source: its first
//! `lists.subscribed_max_per_member` references, naming at most
//! `lists.max_owners_per_member` distinct list owners, both counted in the
//! order the references arrived. A reference inside the budget is
//! **admitted**. Admission reads the member's own rows and nothing else, so
//! it never depends on what any other member references.
//!
//! The member phase is one transaction that touches only the member's own
//! rows: it takes the member's reference lock, writes the source row and
//! the reference, recomputes the member's `admitted` flags, and appends a
//! change marker for every list whose admitted references from this member
//! changed. It takes no lock on a list, an owner or a job, so a request
//! never waits on shared state. The list phase (a background task) reads
//! the markers and does the rest.

use sqlx::{PgConnection, PgPool};

use crate::db::locks::uri_author;

pub const METRIC_OVER_MEMBER_CAP: &str = "list_subscriptions_over_member_cap_total";

/// Where a reference comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A row of `list_mutes`: private.
    Mute,
    /// A `listblock` record in the member's repository: public.
    Block,
}

impl Source {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mute => "mute",
            Self::Block => "block",
        }
    }
}

/// One member's budget for one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// `lists.subscribed_max_per_member`.
    pub max_references: u32,
    /// `lists.max_owners_per_member`.
    pub max_owners: u32,
}

/// Why a reference is outside the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverBudget {
    References,
    Owners,
}

impl OverBudget {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::References => "references",
            Self::Owners => "owners",
        }
    }
}

/// For each of one member's references of one source, in arrival order:
/// `None` when it is admitted, or why it is not.
///
/// A reference is admitted when it is among the first `max_references`
/// references and its list's owner is among the first `max_owners` distinct
/// owners, both counted over every reference in the order given, admitted
/// or not.
#[must_use]
pub fn admission(lists: &[&str], budget: Budget) -> Vec<Option<OverBudget>> {
    let max_references = usize::try_from(budget.max_references).unwrap_or(usize::MAX);
    let max_owners = usize::try_from(budget.max_owners).unwrap_or(usize::MAX);
    let mut owners: Vec<&str> = Vec::new();
    lists
        .iter()
        .enumerate()
        .map(|(position, list)| {
            let owner = uri_author(list).unwrap_or_default();
            let rank = owners.iter().position(|o| *o == owner).unwrap_or_else(|| {
                owners.push(owner);
                owners.len() - 1
            });
            if position >= max_references {
                Some(OverBudget::References)
            } else if rank >= max_owners {
                Some(OverBudget::Owners)
            } else {
                None
            }
        })
        .collect()
}

/// Take `member`'s reference lock for the rest of the transaction (lock
/// step 1). It serializes one member's references and budget.
///
/// # Errors
///
/// Database errors.
pub async fn lock_member(conn: &mut PgConnection, member: &str) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('listref:' || $1, 0))")
        .bind(member)
        .execute(conn)
        .await?;
    Ok(())
}

/// `member`'s references of `source`, in arrival order: the list and its
/// stored `admitted` flag.
async fn stored(conn: &mut PgConnection, member: &str, source: Source) -> Result<Vec<(String, bool)>, sqlx::Error> {
    sqlx::query_as("SELECT list_uri, admitted FROM list_references WHERE member_did = $1 AND source = $2 ORDER BY arrived_at, list_uri")
        .bind(member)
        .bind(source.as_str())
        .fetch_all(conn)
        .await
}

/// Recompute `member`'s `admitted` flags for `source` from its current
/// rows, and write only the rows whose flag changes, in primary-key order.
/// Returns the lists whose flag changed. The caller holds the member's
/// reference lock.
///
/// # Errors
///
/// Database errors.
pub async fn recompute(conn: &mut PgConnection, member: &str, source: Source, budget: Budget) -> Result<Vec<String>, sqlx::Error> {
    let rows = stored(&mut *conn, member, source).await?;
    let lists: Vec<&str> = rows.iter().map(|(list, _)| list.as_str()).collect();
    let verdicts = admission(&lists, budget);
    let mut changed: Vec<(&str, bool)> = rows
        .iter()
        .zip(&verdicts)
        .filter(|((_, was), verdict)| *was != verdict.is_none())
        .map(|((list, _), verdict)| (list.as_str(), verdict.is_none()))
        .collect();
    changed.sort_unstable();
    for (list, now_admitted) in &changed {
        // Becoming admitted starts the reference's wait for a sync afresh.
        sqlx::query(
            "UPDATE list_references SET admitted = $4, admitted_at = CASE WHEN $4 THEN now() END, \
               effective = false, unlisted_at = NULL \
             WHERE member_did = $1 AND list_uri = $2 AND source = $3",
        )
        .bind(member)
        .bind(list)
        .bind(source.as_str())
        .bind(now_admitted)
        .execute(&mut *conn)
        .await?;
    }
    Ok(changed.into_iter().map(|(list, _)| list.to_string()).collect())
}

/// Append a change marker for each of `lists`: some member's admitted
/// references to the list changed. A marker names only the list.
///
/// # Errors
///
/// Database errors.
pub async fn insert_markers(conn: &mut PgConnection, lists: &[String]) -> Result<(), sqlx::Error> {
    if lists.is_empty() {
        return Ok(());
    }
    let mut unique: Vec<&String> = lists.iter().collect();
    unique.sort_unstable();
    unique.dedup();
    sqlx::query("INSERT INTO list_reference_changes (list_uri) SELECT unnest($1::text[])")
        .bind(unique)
        .execute(conn)
        .await?;
    Ok(())
}

/// What `mute_list` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Muted {
    /// The mute and its reference were written.
    Added,
    /// The list was already muted: nothing changed.
    Already,
    /// The reference would be outside the member's budget: nothing written.
    Refused(OverBudget),
}

/// The member phase of `muteActorList`: one transaction on the member's
/// own rows. Returns whether markers were written, for the caller to
/// signal the list phase.
///
/// # Errors
///
/// Database errors.
pub async fn mute_list(pool: &PgPool, member: &str, list: &str, budget: Budget) -> Result<Muted, sqlx::Error> {
    let mut tx = pool.begin().await?;
    lock_member(&mut tx, member).await?;
    let already: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM list_mutes WHERE viewer_did = $1 AND list_uri = $2)")
        .bind(member)
        .bind(list)
        .fetch_one(&mut *tx)
        .await?;
    if already {
        return Ok(Muted::Already);
    }
    // The budget check: the member's stored mute references, admitted or
    // not, with the new one last.
    let rows = stored(&mut tx, member, Source::Mute).await?;
    let mut lists: Vec<&str> = rows.iter().map(|(l, _)| l.as_str()).collect();
    lists.push(list);
    if let Some(Some(reason)) = admission(&lists, budget).last().copied() {
        metrics::counter!(METRIC_OVER_MEMBER_CAP, "source" => "mute", "reason" => reason.as_str()).increment(1);
        return Ok(Muted::Refused(reason));
    }
    sqlx::query("INSERT INTO list_mutes (viewer_did, list_uri) VALUES ($1, $2)")
        .bind(member)
        .bind(list)
        .execute(&mut *tx)
        .await?;
    // `clock_timestamp()`: two references of one transaction-free moment
    // still arrive in a definite order.
    sqlx::query(
        "INSERT INTO list_references (member_did, list_uri, source, arrived_at, admitted) \
         VALUES ($1, $2, 'mute', clock_timestamp(), false) ON CONFLICT DO NOTHING",
    )
    .bind(member)
    .bind(list)
    .execute(&mut *tx)
    .await?;
    let changed = recompute(&mut tx, member, Source::Mute, budget).await?;
    insert_markers(&mut tx, &changed).await?;
    tx.commit().await?;
    Ok(Muted::Added)
}

/// The member phase of `unmuteActorList`: deletes the mute and its
/// reference, which promotes the member's next reference. Idempotent.
/// Returns whether anything was removed.
///
/// # Errors
///
/// Database errors.
pub async fn unmute_list(pool: &PgPool, member: &str, list: &str, budget: Budget) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    lock_member(&mut tx, member).await?;
    let removed = sqlx::query("DELETE FROM list_mutes WHERE viewer_did = $1 AND list_uri = $2")
        .bind(member)
        .bind(list)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        > 0;
    let mut changed = drop_reference(&mut tx, member, list, Source::Mute).await?;
    changed.extend(recompute(&mut tx, member, Source::Mute, budget).await?);
    insert_markers(&mut tx, &changed).await?;
    tx.commit().await?;
    Ok(removed)
}

/// Delete one reference. Returns its list if it was admitted, so that the
/// list gets a marker.
async fn drop_reference(conn: &mut PgConnection, member: &str, list: &str, source: Source) -> Result<Vec<String>, sqlx::Error> {
    let admitted: Option<bool> =
        sqlx::query_scalar("DELETE FROM list_references WHERE member_did = $1 AND list_uri = $2 AND source = $3 RETURNING admitted")
            .bind(member)
            .bind(list)
            .bind(source.as_str())
            .fetch_optional(conn)
            .await?;
    Ok(if admitted == Some(true) { vec![list.to_string()] } else { Vec::new() })
}

/// The startup recompute: every member's `admitted` flags are derived
/// again under the budget now configured, one member per transaction. A
/// budget that was raised admits references that waited; one that was
/// lowered stops the newest. Returns whether markers were written.
///
/// # Errors
///
/// Database errors.
pub async fn recompute_all(pool: &PgPool, budget: Budget) -> Result<bool, sqlx::Error> {
    let members: Vec<String> = sqlx::query_scalar("SELECT DISTINCT member_did FROM list_references ORDER BY member_did").fetch_all(pool).await?;
    let mut marked = false;
    for member in members {
        let mut tx = pool.begin().await?;
        lock_member(&mut tx, &member).await?;
        let mut changed = recompute(&mut tx, &member, Source::Mute, budget).await?;
        changed.extend(recompute(&mut tx, &member, Source::Block, budget).await?);
        insert_markers(&mut tx, &changed).await?;
        tx.commit().await?;
        marked |= !changed.is_empty();
    }
    Ok(marked)
}

/// The member phase of member add: references for the list mutes and list
/// blocks the new member already has stored, in the order they were made.
/// Runs in the caller's transaction. Returns whether markers were written.
///
/// # Errors
///
/// Database errors.
pub async fn adopt_member(conn: &mut PgConnection, member: &str, budget: Budget) -> Result<bool, sqlx::Error> {
    lock_member(&mut *conn, member).await?;
    // `clock_timestamp()` plus the row's rank keeps the order the mutes
    // and blocks were made in, whatever the clock's resolution.
    sqlx::query(
        "INSERT INTO list_references (member_did, list_uri, source, arrived_at, admitted) \
         SELECT $1, m.list_uri, 'mute', clock_timestamp() + make_interval(secs => (row_number() OVER (ORDER BY m.created_at, m.list_uri)) * 0.000001), false \
         FROM list_mutes m WHERE m.viewer_did = $1 ORDER BY m.created_at, m.list_uri \
         ON CONFLICT DO NOTHING",
    )
    .bind(member)
    .execute(&mut *conn)
    .await?;
    // One block reference per list, named by its earliest record. A
    // subject in any other collection makes no reference.
    sqlx::query(
        "INSERT INTO list_references (member_did, list_uri, source, source_uri, arrived_at, admitted) \
         SELECT $1, b.list_uri, 'block', b.uri, clock_timestamp() + make_interval(secs => (row_number() OVER (ORDER BY b.created_at, b.uri)) * 0.000001), false \
         FROM (SELECT DISTINCT ON (list_uri) list_uri, uri, created_at FROM list_blocks \
               WHERE author_did = $1 AND split_part(list_uri, '/', 4) = 'app.bsky.graph.list' ORDER BY list_uri, created_at, uri) b \
         ORDER BY b.created_at, b.uri \
         ON CONFLICT DO NOTHING",
    )
    .bind(member)
    .execute(&mut *conn)
    .await?;
    let mut changed = recompute(&mut *conn, member, Source::Mute, budget).await?;
    changed.extend(recompute(&mut *conn, member, Source::Block, budget).await?);
    insert_markers(conn, &changed).await?;
    Ok(!changed.is_empty())
}

/// The member phase of a purge's marker step (`member_removed`,
/// `account_deleted`): every reference of the member is deleted, and each
/// list it had an admitted reference to gets a marker, so the list phase
/// releases what nobody references any more. Runs in the caller's
/// transaction. Returns whether markers were written.
///
/// # Errors
///
/// Database errors.
pub async fn drop_member(conn: &mut PgConnection, member: &str) -> Result<bool, sqlx::Error> {
    lock_member(&mut *conn, member).await?;
    let deleted: Vec<Option<String>> =
        sqlx::query_scalar("DELETE FROM list_references WHERE member_did = $1 RETURNING CASE WHEN admitted THEN list_uri END")
            .bind(member)
            .fetch_all(&mut *conn)
            .await?;
    let mut changed: Vec<String> = deleted.into_iter().flatten().collect();
    changed.sort_unstable();
    changed.dedup();
    insert_markers(conn, &changed).await?;
    Ok(!changed.is_empty())
}

/// The member phase for a member's list blocks of one list, after a
/// `listblock` record of the member was written, changed or deleted: the
/// `block` reference is derived again from the member's remaining
/// `list_blocks` rows for the list. It is created with the first record,
/// kept in place (same arrival, same flags) while any record remains, and
/// deleted with the last. Runs in the caller's transaction.
///
/// A list block past the budget is stored and not admitted: the record is
/// the member's own and cannot be refused.
///
/// # Errors
///
/// Database errors.
pub async fn rederive_block(conn: &mut PgConnection, member: &str, list: &str, budget: Budget) -> Result<(), sqlx::Error> {
    lock_member(&mut *conn, member).await?;
    let earliest: Option<String> =
        sqlx::query_scalar("SELECT uri FROM list_blocks WHERE author_did = $1 AND list_uri = $2 ORDER BY created_at, uri LIMIT 1")
            .bind(member)
            .bind(list)
            .fetch_optional(&mut *conn)
            .await?;
    let had: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM list_references WHERE member_did = $1 AND list_uri = $2 AND source = 'block')")
            .bind(member)
            .bind(list)
            .fetch_one(&mut *conn)
            .await?;
    let created = earliest.is_some() && !had;
    let mut changed = match earliest {
        Some(source_uri) => {
            sqlx::query(
                "INSERT INTO list_references (member_did, list_uri, source, source_uri, arrived_at, admitted) \
                 VALUES ($1, $2, 'block', $3, clock_timestamp(), false) \
                 ON CONFLICT (member_did, list_uri, source) DO UPDATE SET source_uri = EXCLUDED.source_uri \
                 WHERE list_references.source_uri IS DISTINCT FROM EXCLUDED.source_uri",
            )
            .bind(member)
            .bind(list)
            .bind(source_uri)
            .execute(&mut *conn)
            .await?;
            Vec::new()
        }
        None => drop_reference(&mut *conn, member, list, Source::Block).await?,
    };
    if created {
        // Counted once, when a new list block lands outside the budget.
        let rows = stored(&mut *conn, member, Source::Block).await?;
        let lists: Vec<&str> = rows.iter().map(|(l, _)| l.as_str()).collect();
        let verdict = lists.iter().position(|l| *l == list).and_then(|i| admission(&lists, budget)[i]);
        if let Some(reason) = verdict {
            metrics::counter!(METRIC_OVER_MEMBER_CAP, "source" => "block", "reason" => reason.as_str()).increment(1);
        }
    }
    changed.extend(recompute(&mut *conn, member, Source::Block, budget).await?);
    insert_markers(conn, &changed).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    const ME: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const OTHER: &str = "did:plc:oooooooooooooooooooooooo";

    fn list(owner: char, n: u8) -> String {
        format!("at://did:plc:{}/app.bsky.graph.list/3klist{n}", owner.to_string().repeat(24))
    }

    #[test]
    fn admission_counts_references_and_owners_in_arrival_order() {
        let budget = Budget { max_references: 3, max_owners: 2 };
        let lists = [list('a', 1), list('b', 1), list('c', 1), list('a', 2), list('b', 2)];
        let refs: Vec<&str> = lists.iter().map(String::as_str).collect();
        assert_eq!(
            admission(&refs, budget),
            // The third names a third owner; the fourth and fifth are past
            // the first three references.
            [None, None, Some(OverBudget::Owners), Some(OverBudget::References), Some(OverBudget::References)]
        );
        // The owner rank counts every reference, admitted or not: a list of
        // the third owner stays out although only two are admitted.
        let budget = Budget { max_references: 5, max_owners: 2 };
        assert_eq!(admission(&refs, budget), [None, None, Some(OverBudget::Owners), None, None]);
        assert!(admission(&[], budget).is_empty());
    }

    async fn flags(pool: &PgPool, member: &str, source: &str) -> Vec<(String, bool, bool)> {
        sqlx::query_as(
            "SELECT list_uri, admitted, admitted_at IS NOT NULL FROM list_references WHERE member_did = $1 AND source = $2 ORDER BY arrived_at, list_uri",
        )
        .bind(member)
        .bind(source)
        .fetch_all(pool)
        .await
        .expect("flags")
    }

    async fn markers(pool: &PgPool) -> Vec<String> {
        let found: Vec<String> = sqlx::query_scalar("DELETE FROM list_reference_changes RETURNING list_uri").fetch_all(pool).await.expect("markers");
        let mut found = found;
        found.sort_unstable();
        found
    }

    /// A member added with list mutes and list blocks already stored gets
    /// references for them, in the order they were made and under the
    /// budget; a removed member's references all go, and the lists it
    /// really referenced are marked.
    #[tokio::test]
    async fn member_add_and_member_removal() {
        let Some(db) = fresh_db("subscriptions_member_add").await else {
            return;
        };
        let p = &db.pool;
        let budget = Budget { max_references: 2, max_owners: 5 };
        for (n, at) in [(1, "2026-01-03"), (2, "2026-01-01"), (3, "2026-01-02")] {
            sqlx::query("INSERT INTO list_mutes (viewer_did, list_uri, created_at) VALUES ($1, $2, $3::timestamptz)")
                .bind(ME)
                .bind(list('a', n))
                .bind(at)
                .execute(p)
                .await
                .expect("mute");
        }
        let block = |rkey: &'static str, subject: String, at: &'static str| async move {
            sqlx::query(
                "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) \
                 VALUES ('at://' || $1 || '/app.bsky.graph.listblock/' || $2, $1, $2, 'c', '3k', $3, $4::timestamptz)",
            )
            .bind(ME)
            .bind(rkey)
            .bind(subject)
            .bind(at)
            .execute(p)
            .await
            .expect("block");
        };
        // Two records for one list, and one whose subject is not a list.
        block("3kb2", list('b', 1), "2026-01-02").await;
        block("3kb1", list('b', 1), "2026-01-01").await;
        block("3kb3", format!("at://{OTHER}/app.bsky.feed.post/3k"), "2026-01-01").await;

        let mut conn = p.acquire().await.expect("conn");
        assert!(adopt_member(&mut conn, ME, budget).await.expect("adopt"));
        // The two oldest mutes are admitted; the newest is past the budget.
        assert_eq!(
            flags(p, ME, "mute").await,
            vec![(list('a', 2), true, true), (list('a', 3), true, true), (list('a', 1), false, false)]
        );
        assert_eq!(flags(p, ME, "block").await, vec![(list('b', 1), true, true)]);
        let source: Option<String> = sqlx::query_scalar("SELECT source_uri FROM list_references WHERE source = 'block'").fetch_one(p).await.expect("source");
        assert!(source.expect("uri").ends_with("/3kb1"), "the earliest record names the reference");
        assert_eq!(markers(p).await, vec![list('a', 2), list('a', 3), list('b', 1)]);
        // Again: nothing new.
        assert!(!adopt_member(&mut conn, ME, budget).await.expect("again"));
        // A raised budget admits the mute that waited; lowered, the
        // newest two stop being admitted. Each change is marked.
        assert!(recompute_all(p, Budget { max_references: 3, max_owners: 5 }).await.expect("raised"));
        assert_eq!(markers(p).await, vec![list('a', 1)]);
        assert!(recompute_all(p, Budget { max_references: 1, max_owners: 5 }).await.expect("lowered"));
        assert_eq!(markers(p).await, vec![list('a', 1), list('a', 3)]);
        assert!(!recompute_all(p, Budget { max_references: 1, max_owners: 5 }).await.expect("same"));
        assert!(recompute_all(p, budget).await.expect("back"));
        assert_eq!(markers(p).await, vec![list('a', 3)]);

        // Removal: every reference goes; only admitted ones are marked.
        assert!(drop_member(&mut conn, ME).await.expect("drop"));
        assert!(flags(p, ME, "mute").await.is_empty() && flags(p, ME, "block").await.is_empty());
        assert_eq!(markers(p).await, vec![list('a', 2), list('a', 3), list('b', 1)]);
        assert!(!drop_member(&mut conn, ME).await.expect("nothing left"));
        drop(conn);
        db.drop_db().await;
    }

    /// B-73: a mute inside the budget is admitted and marked; one past it
    /// is refused and writes nothing; an unmute promotes nothing it should
    /// not, and marks the list it leaves.
    #[tokio::test]
    async fn the_member_phase_of_list_mutes() {
        let Some(db) = fresh_db("subscriptions_mutes").await else {
            return;
        };
        let p = &db.pool;
        let budget = Budget { max_references: 2, max_owners: 2 };
        let (first, second, third) = (list('a', 1), list('b', 1), list('a', 2));

        assert_eq!(mute_list(p, ME, &first, budget).await.expect("db"), Muted::Added);
        assert_eq!(markers(p).await, std::slice::from_ref(&first));
        // A repeat changes nothing and writes no marker.
        assert_eq!(mute_list(p, ME, &first, budget).await.expect("db"), Muted::Already);
        assert!(markers(p).await.is_empty());
        assert_eq!(mute_list(p, ME, &second, budget).await.expect("db"), Muted::Added);
        // Past the budget: refused, nothing stored.
        assert_eq!(mute_list(p, ME, &third, budget).await.expect("db"), Muted::Refused(OverBudget::References));
        let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM list_mutes").fetch_one(p).await.expect("count");
        assert_eq!(stored, 2);
        assert_eq!(flags(p, ME, "mute").await, [(first.clone(), true, true), (second.clone(), true, true)]);
        // Another member's budget is its own.
        assert_eq!(mute_list(p, OTHER, &third, budget).await.expect("db"), Muted::Added);
        markers(p).await;

        // Unmuting marks the list; a repeat does nothing.
        assert!(unmute_list(p, ME, &first, budget).await.expect("db"));
        assert_eq!(markers(p).await, std::slice::from_ref(&first));
        assert!(!unmute_list(p, ME, &first, budget).await.expect("db"));
        assert!(markers(p).await.is_empty());
        assert_eq!(flags(p, ME, "mute").await, [(second.clone(), true, true)]);
        // There is room again.
        assert_eq!(mute_list(p, ME, &third, budget).await.expect("db"), Muted::Added);
        db.drop_db().await;
    }

    /// A list block past the budget is stored unadmitted, and is promoted
    /// when an earlier one goes; duplicate records keep the reference in
    /// place.
    #[tokio::test]
    async fn the_member_phase_of_list_blocks() {
        let Some(db) = fresh_db("subscriptions_blocks").await else {
            return;
        };
        let p = &db.pool;
        let budget = Budget { max_references: 1, max_owners: 5 };
        let (first, second) = (list('a', 1), list('b', 1));
        let block = |rkey: &'static str, list: String, at: &'static str| async move {
            sqlx::query(
                "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) \
                 VALUES ($1, $2, $3, 'bafy', '3k', $4, $5::timestamptz)",
            )
            .bind(format!("at://{ME}/app.bsky.graph.listblock/{rkey}"))
            .bind(ME)
            .bind(rkey)
            .bind(list)
            .bind(at)
            .execute(p)
            .await
            .expect("listblock");
        };
        let rederive = |list: String| async move {
            let mut tx = p.begin().await.expect("tx");
            rederive_block(&mut tx, ME, &list, budget).await.expect("rederive");
            tx.commit().await.expect("commit");
        };
        let source = |list: String| async move {
            sqlx::query_scalar::<_, Option<String>>("SELECT source_uri FROM list_references WHERE member_did = $1 AND list_uri = $2 AND source = 'block'")
                .bind(ME)
                .bind(list)
                .fetch_optional(p)
                .await
                .expect("source")
                .flatten()
        };

        block("3kb1", first.clone(), "2026-01-02T00:00:00Z").await;
        rederive(first.clone()).await;
        block("3kb2", second.clone(), "2026-01-03T00:00:00Z").await;
        rederive(second.clone()).await;
        // The second is stored, outside the budget, with no marker.
        assert_eq!(flags(p, ME, "block").await, [(first.clone(), true, true), (second.clone(), false, false)]);
        assert_eq!(markers(p).await, std::slice::from_ref(&first));

        // An earlier duplicate record of the first list: the reference
        // points at it and is otherwise untouched (no marker).
        block("3kb0", first.clone(), "2026-01-01T00:00:00Z").await;
        rederive(first.clone()).await;
        assert_eq!(source(first.clone()).await, Some(format!("at://{ME}/app.bsky.graph.listblock/3kb0")));
        assert!(markers(p).await.is_empty());
        // Deleting one of the two keeps the block.
        sqlx::query("DELETE FROM list_blocks WHERE rkey = '3kb0'").execute(p).await.expect("delete");
        rederive(first.clone()).await;
        assert_eq!(source(first.clone()).await, Some(format!("at://{ME}/app.bsky.graph.listblock/3kb1")));
        assert!(markers(p).await.is_empty());
        // Deleting the last drops the reference and promotes the next one:
        // both lists are marked.
        sqlx::query("DELETE FROM list_blocks WHERE rkey = '3kb1'").execute(p).await.expect("delete");
        rederive(first.clone()).await;
        assert_eq!(flags(p, ME, "block").await, [(second.clone(), true, true)]);
        assert_eq!(markers(p).await, [first, second]);
        db.drop_db().await;
    }
}
