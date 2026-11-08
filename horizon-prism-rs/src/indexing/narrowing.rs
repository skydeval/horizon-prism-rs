//! The steps that delete what full indexing stored beyond community's: the
//! narrowing sweep runs them all, and a drain from full runs the ones the
//! drain's tables do not cover.
//!
//! Each step is community retention's scope step for its table (the rows
//! community keeps: S, members, Refract's R and member-involved rows), or a
//! delete of a table only full writes. A step is idempotent, so the stored
//! state names only the step in progress; a restart runs it again. Every
//! batch checks the task's generation under the scope read guard, so a
//! switch back to full stops the sweep before its next delete.

use std::collections::HashSet;

use serde_json::{Value, json};

use super::public_mode::{Phase, PublicIndexing, SweepList, g64, now, rfc3339};
use crate::db::retention::{BATCH_PAUSE, BATCH_SIZE, Bind, LOCK_DID_SQL, Step, batched, batched_composite, scope_steps};

/// A DID text column the community kept-set holds: S, the members, or R.
fn kept(col: &str) -> String {
    format!(
        "(EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = {col}) \
          OR EXISTS (SELECT 1 FROM members m WHERE m.did = {col}) \
          OR EXISTS (SELECT 1 FROM refract_dids r WHERE r.did = {col} AND r.state IN ('restored', 'grace')))"
    )
}

/// The DID of an id column.
fn did_of(col: &str) -> String {
    format!("(SELECT dx.did FROM dids dx WHERE dx.id = {col})")
}

/// A DID that is tracked: a member, or a repo whose chain is followed.
fn tracked(col: &str) -> String {
    format!("(EXISTS (SELECT 1 FROM members m WHERE m.did = {col}) OR EXISTS (SELECT 1 FROM sync_state ss WHERE ss.did = {col}))")
}

/// When full was entered, as stored with its groups; NULL when it was not
/// recorded (full entered by an older version), so nothing is spared.
fn full_since() -> String {
    format!("(SELECT (fsw.value #>> '{{}}')::timestamptz FROM worker_state fsw WHERE fsw.key = '{}')", super::full::SINCE_KEY)
}

/// A timestamp column written since full was entered.
fn since_entry(col: &str) -> String {
    format!("COALESCE({col} >= {}, TRUE)", full_since())
}

/// A rev column (a TID's stored value) of a commit made since full was
/// entered: the TID of the entry instant, mapped to stored order.
fn rev_since_entry(col: &str) -> String {
    format!(
        "COALESCE({col} >= ((floor(extract(epoch FROM {}) * 1000000)::bigint << 10) + (-9223372036854775807 - 1)), TRUE)",
        full_since()
    )
}

/// `step` with `extra` added to its predicate, in the delete and in the
/// candidate query alike.
fn and_also(mut step: Step, extra: &str) -> Step {
    let add = |sql: &mut String| {
        if let Some(at) = sql.rfind(" LIMIT ") {
            sql.insert_str(at, &format!(" AND {extra}"));
        }
    };
    add(&mut step.sql);
    if let Some(c) = step.candidates.as_mut() {
        add(c);
    }
    step
}

/// Community retention's step for `table`, limited to the rows full could
/// have written. Rows community already held when full was entered are
/// community's own: its retention decides them, as if full had not run.
/// Admission keeps only blocks with a member side in every breadth, so the
/// block steps find only older rows; identity rows are dated by their
/// creation.
fn bounded(step: Step) -> Step {
    match step.table {
        "blocks" => and_also(step, &rev_since_entry("b.rev")),
        "list_blocks" => and_also(step, &rev_since_entry("lb.rev")),
        "actors" => and_also(step, &since_entry("a.indexed_at")),
        _ => step,
    }
}

/// The tables only full writes, and the rows of shared tables full adds to
/// that no retention step covers.
fn full_only_steps() -> Vec<Step> {
    let target_did = "(CASE WHEN cc.target LIKE 'at://%' THEN split_part(substr(cc.target, 6), '/', 1) ELSE cc.target END)";
    vec![
        batched("constellation_counts", "target", "cc", &format!("NOT {}", kept(target_did)), Bind::None),
        batched_composite("record_tombstones", &["author", "coll", "rkey"], "t", &format!("NOT {}", kept(&did_of("t.author"))), Bind::None),
        // After `actors`: a DID whose identity row stays keeps its document.
        // Community persists documents of S and the members only, and keeps
        // each until it expires, so a document fetched before full was
        // entered stays for retention.
        batched(
            "did_cache",
            "did",
            "dc",
            &format!(
                "NOT {} AND NOT EXISTS (SELECT 1 FROM actors a JOIN dids ad ON ad.id = a.id WHERE ad.did = dc.did) AND {}",
                kept("dc.did"),
                since_entry("dc.fetched_at")
            ),
            Bind::None,
        ),
        batched_composite("relay_account_status", &["account", "relay"], "ras", &format!("NOT {}", tracked(&did_of("ras.account"))), Bind::None),
        batched("identity_refresh", "did", "ir", &format!("NOT {} AND NOT {}", kept("ir.did"), tracked("ir.did")), Bind::None),
        batched_composite("full_crawl_groups", &["account", "grp"], "fg", "TRUE", Bind::None),
        batched("full_crawl", "account", "fc", "TRUE", Bind::None),
        batched("refract_excluded", "account", "rx", "TRUE", Bind::None),
        // Last: the entry time bounds the steps above until here.
        batched(
            "worker_state",
            "key",
            "w",
            &format!("w.key IN ('{}', '{}')", super::full::GROUPS_KEY, super::full::SINCE_KEY),
            Bind::None,
        ),
    ]
}

/// The narrowing sweep's steps, in order: every community scope step, then
/// the full-only ones.
pub const STEP_NAMES: [&str; 30] = [
    "posts",
    "post_search",
    "likes",
    "reposts",
    "follows",
    "blocks",
    "feed_items",
    "lists",
    "list_items",
    "list_blocks",
    "list_optouts",
    "starter_packs",
    "labeler_services",
    "actor_self_records",
    "nearhorizon_poll_posts",
    "threadgates",
    "postgates",
    "starter_pack_counts",
    "post_counts",
    "actors",
    "actor_counts",
    "constellation_counts",
    "record_tombstones",
    "did_cache",
    "relay_account_status",
    "identity_refresh",
    "full_crawl_groups",
    "full_crawl",
    "refract_excluded",
    "worker_state",
];

/// The steps a drain from full runs after the drain's own tables: the
/// shared tables full adds to that the drain does not touch, then the
/// full-only ones.
pub const FULL_DRAIN_STEP_NAMES: [&str; 14] = [
    "post_search",
    "blocks",
    "actor_self_records",
    "actors",
    "actor_counts",
    "constellation_counts",
    "record_tombstones",
    "did_cache",
    "relay_account_status",
    "identity_refresh",
    "full_crawl_groups",
    "full_crawl",
    "refract_excluded",
    "worker_state",
];

/// A trial's narrowing: the narrowing's steps, with the trial's profiles
/// cleared after the identity rows.
pub const TRIAL_STEP_NAMES: [&str; 33] = [
    "posts",
    "post_search",
    "likes",
    "reposts",
    "follows",
    "blocks",
    "feed_items",
    "lists",
    "list_items",
    "list_blocks",
    "list_optouts",
    "starter_packs",
    "labeler_services",
    "actor_self_records",
    "nearhorizon_poll_posts",
    "threadgates",
    "postgates",
    "starter_pack_counts",
    "post_counts",
    TRIAL_ZERO_COUNTS,
    TRIAL_ZERO_TRENDS,
    "actors",
    TRIAL_PROFILES,
    "actor_counts",
    "constellation_counts",
    "record_tombstones",
    "did_cache",
    "relay_account_status",
    "identity_refresh",
    "full_crawl_groups",
    "full_crawl",
    "refract_excluded",
    "worker_state",
];

/// A trial's drain from full: the drain's steps, with the trial's profiles
/// cleared after the identity rows.
pub const TRIAL_FULL_DRAIN_STEP_NAMES: [&str; 15] = [
    "post_search",
    "blocks",
    "actor_self_records",
    "actors",
    TRIAL_PROFILES,
    "actor_counts",
    "constellation_counts",
    "record_tombstones",
    "did_cache",
    "relay_account_status",
    "identity_refresh",
    "full_crawl_groups",
    "full_crawl",
    "refract_excluded",
    "worker_state",
];

/// The step that clears the profiles a trial wrote on rows the exit keeps.
pub const TRIAL_PROFILES: &str = "trial_profiles";
/// The step that removes count rows the trial's removed rows left at zero.
pub const TRIAL_ZERO_COUNTS: &str = "trial_zero_counts";
/// The step that removes trend hours the trial's removed rows left at zero.
pub const TRIAL_ZERO_TRENDS: &str = "trial_zero_trends";

/// The step names of `list`.
fn names(list: SweepList) -> &'static [&'static str] {
    match list {
        SweepList::Today => &[],
        SweepList::Full => &FULL_DRAIN_STEP_NAMES,
        SweepList::Narrowing => &STEP_NAMES,
        SweepList::TrialNarrowing => &TRIAL_STEP_NAMES,
        SweepList::TrialFull => &TRIAL_FULL_DRAIN_STEP_NAMES,
    }
}

/// A trial's exit differs in these steps: tombstones go by the record, so
/// the start mode's own stay; relay statuses, which only full writes, all
/// go; a profile the trial wrote on an identity row the exit keeps, of an
/// account outside S, the members and R, is cleared while its rev is still
/// the trial's; an account's counts go only when the record names it and
/// no follow or post of anyone names it any more, so the counts community
/// keeps for accounts S follows stay, and an account's counts the record
/// names that are all back at zero go whatever else holds the account (a
/// zero row reads like no row); and the count rows and trend hours
/// the trial's removed rows brought back to zero go, as they read like no
/// row at all.
fn trial_steps() -> Vec<Step> {
    let tombstones = super::trial::TrialTable::RecordTombstones.code();
    let profiles = super::trial::TrialTable::Actors.code();
    let account = did_of("ac.id");
    vec![
        Step {
            table: "actor_counts",
            sql: format!(
                "DELETE FROM actor_counts WHERE id IN (SELECT ac.id FROM actor_counts ac \
                   JOIN (SELECT a AS id FROM full_trial_rows UNION SELECT s_a FROM full_trial_rows WHERE s_a IS NOT NULL) r ON r.id = ac.id \
                 WHERE (ac.followers_count = 0 AND ac.follows_count = 0 AND ac.posts_count = 0) \
                    OR (NOT {} AND NOT EXISTS (SELECT 1 FROM actors x WHERE x.id = ac.id) \
                   AND NOT EXISTS (SELECT 1 FROM follows x WHERE x.subject = ac.id) AND NOT EXISTS (SELECT 1 FROM follows x WHERE x.author = ac.id) \
                   AND NOT EXISTS (SELECT 1 FROM posts x WHERE x.author = ac.id)) LIMIT {BATCH_SIZE})",
                kept(&account)
            ),
            candidates: None,
            bind: Bind::None,
        },
        Step {
            table: TRIAL_ZERO_COUNTS,
            ..batched_composite(
                "post_counts",
                &["author", "rkey"],
                "pc",
                "pc.like_count = 0 AND pc.repost_count = 0 AND pc.reply_count = 0 AND pc.quote_count = 0",
                Bind::None,
            )
        },
        Step {
            table: TRIAL_ZERO_TRENDS,
            ..batched_composite(
                "tag_activity_hourly",
                &["tag", "hour"],
                "ta",
                "ta.posts <= 0 AND ta.likes <= 0 AND ta.reposts <= 0 AND ta.replies <= 0",
                Bind::None,
            )
        },
        Step {
            table: "record_tombstones",
            sql: format!(
                "DELETE FROM record_tombstones WHERE (author, coll, rkey) IN (SELECT x.author, x.coll, x.rkey FROM full_trial_rows r \
                 JOIN record_tombstones x ON x.author = r.a AND x.rkey = r.b AND x.coll = r.c WHERE r.tbl = {tombstones} LIMIT {BATCH_SIZE})"
            ),
            candidates: None,
            bind: Bind::None,
        },
        batched_composite("relay_account_status", &["account", "relay"], "ras", "TRUE", Bind::None),
        Step {
            table: TRIAL_PROFILES,
            sql: format!(
                "UPDATE actors SET display_name = NULL, description = NULL, avatar_cid = NULL, banner_cid = NULL, profile_record = NULL, \
                   profile_rev = NULL, profile_cid = NULL, pronouns = NULL, website = NULL, joined_via_starter_pack_uri = NULL, \
                   joined_via_starter_pack_cid = NULL, profile_created_at = NULL, self_labels = NULL \
                 WHERE id IN (SELECT x.id FROM full_trial_rows r JOIN actors x ON x.id = r.a AND x.profile_rev = r.rev \
                   WHERE r.tbl = {profiles} AND NOT {} LIMIT {BATCH_SIZE})",
                kept(&did_of("x.id"))
            ),
            candidates: None,
            bind: Bind::None,
        },
    ]
}

/// The index of the step `name` in `list`, if `name` is one of its steps.
#[must_use]
pub fn step_index(list: SweepList, name: &str) -> Option<usize> {
    names(list).iter().position(|n| *n == name)
}

/// The steps of `list`, built with the private-tier row authors community
/// retention keeps.
pub(super) fn steps(list: SweepList, private_row_authors: Vec<String>) -> Vec<Step> {
    let mut all: Vec<Step> = Vec::new();
    // The trial's steps first, so they stand in for the ordinary ones.
    if matches!(list, SweepList::TrialNarrowing | SweepList::TrialFull) {
        all.extend(trial_steps());
    }
    all.extend(scope_steps(private_row_authors).into_iter().map(bounded));
    all.extend(full_only_steps());
    names(list)
        .iter()
        .filter_map(|name| all.iter().find(|s| s.table == *name).cloned())
        .collect()
}

impl PublicIndexing {
    async fn generation_holds(&self, generation: u64) -> bool {
        self.scope.read_guard().await.public.generation == generation
    }

    /// Run `list`'s steps from `start`, then end the sweep. Returns early
    /// when the generation moves on.
    pub(super) async fn run_steps(self, generation: u64, list: SweepList, start: usize) {
        let private = loop {
            match crate::private_tier::retention::row_authors(&self.pool).await {
                Ok(p) => break p,
                Err(error) => {
                    tracing::error!(event = "public_sweep_failed", %error, "reading the private-tier row authors failed; retrying");
                    self.progress(|p| p.sweep_last_error = Some(error.to_string()));
                    tokio::time::sleep(self.settings.failure_pause).await;
                }
            }
        };
        let steps = steps(list, private);
        for i in start..steps.len() {
            let step = &steps[i];
            loop {
                match self.run_step(generation, step).await {
                    Ok(Some(deleted)) => {
                        metrics::counter!(super::public_sweep::METRIC_DELETED, "table" => step.table).increment(deleted);
                        self.progress(|p| {
                            p.sweep_deleted += deleted;
                            p.sweep_last_error = None;
                        });
                        tracing::info!(event = "public_sweep_table_done", table = step.table, list = list.as_str(), deleted, "swept table");
                        break;
                    }
                    Ok(None) => return,
                    Err(error) => {
                        tracing::error!(event = "public_sweep_failed", table = step.table, %error, "sweep step failed; retrying");
                        self.progress(|p| p.sweep_last_error = Some(error.to_string()));
                        tokio::time::sleep(self.settings.failure_pause).await;
                    }
                }
            }
            if let Some(next) = steps.get(i + 1) {
                match self.advance_step(list.phase(), generation, list, next.table).await {
                    Ok(true) => {}
                    Ok(false) => return Self::superseded("sweep", generation),
                    Err(error) => {
                        // The step is run again after a restart; it deletes
                        // nothing more than it did.
                        tracing::warn!(event = "public_sweep_failed", table = next.table, %error, "recording the sweep's step failed");
                    }
                }
            }
        }
        loop {
            match self.end_sweep_of(list, generation).await {
                Ok(()) => return,
                Err(error) => {
                    tracing::error!(event = "public_sweep_failed", %error, "ending the sweep failed; retrying");
                    self.progress(|p| p.sweep_last_error = Some(error.to_string()));
                    tokio::time::sleep(self.settings.failure_pause).await;
                }
            }
        }
    }

    /// Record the step in progress, conditional on the phase and the
    /// generation.
    async fn advance_step(&self, phase: Phase, generation: u64, list: SweepList, table: &str) -> Result<bool, sqlx::Error> {
        let sweep = json!({ "table": table, "after": Value::Null, "last_advanced_at": rfc3339(&now()), "list": list.as_str() });
        let n = sqlx::query(
            "UPDATE worker_state SET value = jsonb_set(value, '{sweep}', $3), updated_at = now() \
             WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2",
        )
        .bind(phase.as_str())
        .bind(g64(generation))
        .bind(sweep)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// One step to completion: the rows it deleted, or `None` when the
    /// generation moved on.
    async fn run_step(&self, generation: u64, step: &Step) -> Result<Option<u64>, sqlx::Error> {
        let mut total = 0;
        let Some(candidates) = &step.candidates else {
            loop {
                let guard = self.scope.read_guard().await;
                if guard.public.generation != generation {
                    return Ok(None);
                }
                let n = bound(sqlx::query(&step.sql), &step.bind).execute(&self.pool).await?.rows_affected();
                drop(guard);
                total += n;
                if n < BATCH_SIZE {
                    return Ok(Some(total));
                }
                tokio::time::sleep(BATCH_PAUSE).await;
            }
        };
        // Per DID, under the DID's exclusive lock, as retention deletes.
        let mut seen = HashSet::new();
        loop {
            if !self.generation_holds(generation).await {
                return Ok(None);
            }
            let query = sqlx::query_scalar::<sqlx::Postgres, String>(candidates);
            let query = match &step.bind {
                Bind::None => query,
                Bind::Days(d) => query.bind(*d),
                Bind::Dids(dids) => query.bind(dids.clone()),
            };
            let fresh: Vec<String> = query.fetch_all(&self.pool).await?.into_iter().filter(|d| seen.insert(d.clone())).collect();
            if fresh.is_empty() {
                return Ok(Some(total));
            }
            for did in &fresh {
                loop {
                    let guard = self.scope.read_guard().await;
                    if guard.public.generation != generation {
                        return Ok(None);
                    }
                    let mut tx = self.pool.begin().await?;
                    sqlx::query(LOCK_DID_SQL).bind(did).execute(&mut *tx).await?;
                    let n = bound(sqlx::query(&step.sql), &step.bind).bind(did).execute(&mut *tx).await?.rows_affected();
                    tx.commit().await?;
                    drop(guard);
                    total += n;
                    if n < BATCH_SIZE {
                        break;
                    }
                    tokio::time::sleep(BATCH_PAUSE).await;
                }
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
    }
}

/// A step's own bind.
fn bound<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    bind: &'q Bind,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match bind {
        Bind::None => query,
        Bind::Days(d) => query.bind(*d),
        Bind::Dids(dids) => query.bind(dids.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stored names are exactly the steps built, in order, so a stored
    /// step always resumes the step it names.
    #[test]
    fn step_names_are_the_built_steps() {
        let built: Vec<&str> = steps(SweepList::Narrowing, Vec::new()).iter().map(|s| s.table).collect();
        assert_eq!(built, STEP_NAMES);
        let drain: Vec<&str> = steps(SweepList::Full, Vec::new()).iter().map(|s| s.table).collect();
        assert_eq!(drain, FULL_DRAIN_STEP_NAMES);
        let trial: Vec<&str> = steps(SweepList::TrialNarrowing, Vec::new()).iter().map(|s| s.table).collect();
        assert_eq!(trial, TRIAL_STEP_NAMES);
        let trial_drain: Vec<&str> = steps(SweepList::TrialFull, Vec::new()).iter().map(|s| s.table).collect();
        assert_eq!(trial_drain, TRIAL_FULL_DRAIN_STEP_NAMES);
        // A trial's tombstones go by the record, and every relay status goes.
        let built = steps(SweepList::TrialNarrowing, Vec::new());
        let tomb = built.iter().find(|s| s.table == "record_tombstones").expect("tombstones");
        assert!(tomb.sql.contains("full_trial_rows"), "{}", tomb.sql);
        let ras = built.iter().find(|s| s.table == "relay_account_status").expect("relay statuses");
        assert!(ras.sql.contains("WHERE TRUE"), "{}", ras.sql);
        let ordinary = steps(SweepList::Narrowing, Vec::new());
        assert!(!ordinary.iter().any(|s| s.sql.contains("full_trial_rows")));
        // A drain's own tables are not run again as steps, except the
        // search rows, which no drain table covers.
        for name in FULL_DRAIN_STEP_NAMES {
            assert!(!super::super::public_sweep::TABLES.iter().any(|t| t.name == name), "{name}");
        }
    }

    /// The block and identity steps are bounded by the entry time in the
    /// delete and in the candidate query; retention's own steps are not.
    #[test]
    fn the_shared_identity_and_block_steps_are_bounded_by_the_entry() {
        let built = steps(SweepList::Narrowing, Vec::new());
        let step = |name: &str| built.iter().find(|s| s.table == name).cloned().unwrap_or_else(|| panic!("{name}"));
        for (name, col) in [("blocks", "b.rev >="), ("list_blocks", "lb.rev >="), ("actors", "a.indexed_at >="), ("did_cache", "dc.fetched_at >=")] {
            let s = step(name);
            assert!(s.sql.contains(col), "{name}: {}", s.sql);
            assert!(s.sql.contains(" LIMIT "), "{name}");
            if let Some(c) = &s.candidates {
                assert!(c.contains(col), "{name}: {c}");
            }
        }
        for s in scope_steps(Vec::new()) {
            assert!(!s.sql.contains(super::super::full::SINCE_KEY), "{}", s.table);
        }
    }
}
