//! A full trial's check: after the exit's phase ends, every recorded
//! row is looked up in its table and found removed, kept for a reason that
//! arose during the trial, kept with no such reason, or left behind; the
//! last two are removed by key and counted apart. Then the derived rows,
//! the counts the trial moved, the tables only full writes and the sanity
//! line; the result is said in plain words and the record is dropped.
//!
//! The check resumes table by table: each finished table's counts are
//! written into `trial.check`, so a restart runs only the tables left.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::public_mode::{PublicIndexing, ScopeValue, now, rfc3339};
use super::trial::{FULL_ONLY, METRIC_CHECK_ROWS, Refused, Trial, TrialState, TrialTable, bytes_words, database_bytes, table_figures};
use crate::db::retention::LOCK_DID_SQL;

/// Record keys classified per statement.
const PAGE: i64 = 2_000;
/// Keys of each fault the result lists.
const SAMPLES: usize = 3;

/// A DID column's text, from an id column.
fn did(col: &str) -> String {
    format!("(SELECT d.did FROM dids d WHERE d.id = {col})")
}

fn in_s(did: &str) -> String {
    format!("EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = {did})")
}

fn member(did: &str) -> String {
    format!("EXISTS (SELECT 1 FROM members m WHERE m.did = {did})")
}

fn in_r(did: &str) -> String {
    format!("EXISTS (SELECT 1 FROM refract_dids rf WHERE rf.did = {did} AND rf.state IN ('restored', 'grace'))")
}

/// S, the members or R.
fn kept_whole(did: &str) -> String {
    format!("({} OR {} OR {})", in_s(did), member(did), in_r(did))
}

/// Something that happened at or after `at` puts `did` among those kept:
/// it entered S, the members, or R-live.
fn joined(did: &str, at: &str) -> String {
    format!(
        "(EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = {did} AND s.added_at >= {at}) \
          OR EXISTS (SELECT 1 FROM members m WHERE m.did = {did} AND m.added_at >= {at}) \
          OR EXISTS (SELECT 1 FROM refract_dids rf WHERE rf.did = {did} AND rf.state IN ('restored', 'grace') AND rf.since >= {at} \
                     AND NOT EXISTS (SELECT 1 FROM refract_pending_dids rp WHERE rp.did = {did})))"
    )
}

fn member_joined(did: &str, at: &str) -> String {
    format!("EXISTS (SELECT 1 FROM members m WHERE m.did = {did} AND m.added_at >= {at})")
}

/// The keep rule of the mode the exit went to, over a stored row `x`, and
/// the reason it keeps the row (`author` or `member`), as SQL; `at` is the
/// parameter holding `measured_at`.
fn rules(tbl: TrialTable, to: ScopeValue, at: &str) -> (String, String) {
    let a = did(&format!("x.{}", tbl.account_column()));
    let any = |dids: &[String]| dids.iter().map(|d| member(d)).collect::<Vec<_>>().join(" OR ");
    let subjects: Vec<String> = match tbl {
        TrialTable::Posts => vec![did("x.parent_author"), did("x.root_author"), did("x.quote_author")],
        TrialTable::Likes | TrialTable::Reposts => vec![did("x.subj_author")],
        TrialTable::Follows | TrialTable::Blocks => vec![did("x.subject")],
        _ => Vec::new(),
    };
    let off = to == ScopeValue::KryphocronOnly;
    let kept = match tbl {
        TrialTable::Posts if off => in_r(&a),
        TrialTable::Posts => format!("({} OR {})", kept_whole(&a), any(&subjects)),
        TrialTable::Likes if off => format!("({} OR {})", member(&a), in_r(&a)),
        TrialTable::Likes => format!("({} OR {})", kept_whole(&a), any(&subjects)),
        TrialTable::Reposts if off => format!("({} OR {} OR {})", member(&a), in_r(&a), in_r(&subjects[0])),
        TrialTable::Reposts => format!("({} OR {} OR {})", kept_whole(&a), any(&subjects), in_r(&subjects[0])),
        TrialTable::Follows if off => format!("({} OR {} OR {})", member(&a), any(&subjects), in_r(&a)),
        TrialTable::Follows => format!("({} OR {})", kept_whole(&a), any(&subjects)),
        TrialTable::Blocks => format!("({} OR {})", member(&a), any(&subjects)),
        TrialTable::ListBlocks => member(&a),
        TrialTable::Lists | TrialTable::ListItems | TrialTable::ListOptouts if off => format!("({} OR {})", member(&a), in_r(&a)),
        TrialTable::Lists | TrialTable::ListItems | TrialTable::ListOptouts => kept_whole(&a),
        TrialTable::StarterPacks | TrialTable::LabelerServices | TrialTable::PollPosts | TrialTable::FeedItems if off => "FALSE".into(),
        TrialTable::StarterPacks | TrialTable::LabelerServices | TrialTable::PollPosts | TrialTable::FeedItems => in_s(&a),
        TrialTable::Threadgates | TrialTable::Postgates if off => in_r(&a),
        TrialTable::Threadgates | TrialTable::Postgates => format!(
            "({} OR (x.post_coll = 1 AND EXISTS (SELECT 1 FROM posts p WHERE p.author = x.post_author AND p.rkey = x.post_rkey)))",
            in_s(&a)
        ),
        TrialTable::ActorSelfRecords | TrialTable::Actors | TrialTable::RecordTombstones => kept_whole(&a),
    };
    let author_joined = if tbl == TrialTable::FeedItems {
        format!("EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = {a} AND s.added_at >= {at})")
    } else {
        joined(&a, at)
    };
    let subject_joined = if subjects.is_empty() {
        "FALSE".to_string()
    } else {
        subjects.iter().map(|d| member_joined(d, at)).collect::<Vec<_>>().join(" OR ")
    };
    let reason = format!("CASE WHEN {author_joined} THEN 'author' WHEN {subject_joined} THEN 'member' END");
    (kept, reason)
}

/// What the check counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Counts {
    recorded: BTreeMap<String, u64>,
    removed: u64,
    kept_author: u64,
    kept_member: u64,
    kept_mode: u64,
    missed: BTreeMap<String, u64>,
    unexplained: BTreeMap<String, u64>,
    samples: Vec<String>,
    done: Vec<String>,
}

impl Counts {
    fn from_json(v: &Value) -> Self {
        let map = |k: &str| -> BTreeMap<String, u64> {
            v.get(k).and_then(Value::as_object).map(|o| o.iter().filter_map(|(t, n)| n.as_u64().map(|n| (t.clone(), n))).collect()).unwrap_or_default()
        };
        let n = |k: &str| v.get(k).and_then(Value::as_u64).unwrap_or(0);
        let list = |k: &str| -> Vec<String> {
            v.get(k).and_then(Value::as_array).map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_string)).collect()).unwrap_or_default()
        };
        Self {
            recorded: map("recorded"),
            removed: n("removed"),
            kept_author: n("kept_author"),
            kept_member: n("kept_member"),
            kept_mode: n("kept_mode"),
            missed: map("missed"),
            unexplained: map("unexplained"),
            samples: list("samples"),
            done: list("done"),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "recorded": self.recorded,
            "removed": self.removed,
            "kept_author": self.kept_author,
            "kept_member": self.kept_member,
            "kept_mode": self.kept_mode,
            "missed": self.missed,
            "unexplained": self.unexplained,
            "samples": self.samples,
            "done": self.done,
        })
    }

    fn total(m: &BTreeMap<String, u64>) -> u64 {
        m.values().sum()
    }
}

/// One classified key.
type Classified = (i32, i64, i16, Option<i64>, bool, bool, Option<String>);

impl PublicIndexing {
    /// Spawn the check.
    #[must_use]
    pub fn spawn_trial_check(&self) -> tokio::task::JoinHandle<()> {
        self.trace_spawn("trial_check");
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match me.run_trial_check().await {
                    Ok(()) => return,
                    Err(error) => {
                        tracing::error!(event = "full_trial_check_failed", %error, "the full trial's check failed; retrying");
                        tokio::time::sleep(me.settings.failure_pause).await;
                    }
                }
            }
        })
    }

    /// The trial being checked, if it still is.
    async fn checking(&self, id: i64) -> Option<Trial> {
        self.trial().await.filter(|t| t.id == id && t.state == TrialState::Checking)
    }

    /// The check's progress, conditional on the trial and its state.
    async fn write_check(&self, id: i64, check: &Value) -> Result<bool, sqlx::Error> {
        let mut sets = self.scope.write_guard().await;
        let n = sqlx::query(
            "UPDATE worker_state SET value = jsonb_set(value, '{trial,check}', $2), updated_at = now() \
             WHERE key = 'public_transition' AND (value->'trial'->>'id')::bigint = $1 AND value->'trial'->>'state' = 'checking'",
        )
        .bind(id)
        .bind(check)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 1
            && let Some(mut t) = sets.trial.clone()
        {
            t.check = check.clone();
            self.scope.set_trial(&mut sets, Some(t));
        }
        Ok(n == 1)
    }

    /// The whole check, from where it stopped.
    #[allow(clippy::too_many_lines)]
    async fn run_trial_check(&self) -> Result<(), sqlx::Error> {
        let Some(t) = self.trial().await.filter(|t| t.state == TrialState::Checking) else {
            return Ok(());
        };
        let id = t.id;
        self.pause("trial_check").await;
        let mut counts = Counts::from_json(&t.check);
        // The sanity line's input: whether anything was recorded at all,
        // read once, before any delete.
        let recorded_total: i64 = match t.check.get("recorded_total").and_then(Value::as_i64) {
            Some(n) => n,
            None => sqlx::query_scalar("SELECT count(*) FROM full_trial_rows").fetch_one(&self.pool).await?,
        };
        let progress = |counts: &Counts| {
            let mut v = counts.to_json();
            v["recorded_total"] = json!(recorded_total);
            v
        };
        if !self.write_check(id, &progress(&counts)).await? {
            return Ok(());
        }
        // 1. Every recorded row, table by table.
        for tbl in TrialTable::ALL {
            if counts.done.iter().any(|d| d == tbl.name()) {
                continue;
            }
            if self.checking(id).await.is_none() {
                return Ok(());
            }
            self.check_table(&t, tbl, &mut counts, true).await?;
            counts.done.push(tbl.name().to_string());
            if !self.write_check(id, &progress(&counts)).await? {
                return Ok(());
            }
        }
        // 2. Derived rows.
        let derived = self.check_derived(&t, true).await?;
        for (table, n) in &derived {
            if *n > 0 {
                *counts.missed.entry((*table).to_string()).or_default() += n;
            }
        }
        // 1–2 again: what is left after the removals.
        let mut again = Counts::default();
        for tbl in TrialTable::ALL {
            self.check_table(&t, tbl, &mut again, false).await?;
        }
        let left_after: u64 = Counts::total(&again.missed) + Counts::total(&again.unexplained) + self.check_derived(&t, false).await?.iter().map(|(_, n)| n).sum::<u64>();
        // 3. Counts of the subjects the trial moved.
        let (posts, accounts, repaired) = self.check_counts().await?;
        // 4. The tables only full writes.
        let mut full_left = Vec::new();
        for table in FULL_ONLY {
            let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(&self.pool).await?;
            if n > 0 {
                full_left.push(format!("{table} {n}"));
            }
        }
        let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_state WHERE key IN ($1, $2, $3, $4)")
            .bind(super::full::GROUPS_KEY)
            .bind(super::full::SINCE_KEY)
            .bind(super::labels::STATE_KEY)
            .bind(super::crawl::plc_export::EXPORT_KEY)
            .fetch_one(&self.pool)
            .await?;
        if keys > 0 {
            full_left.push(format!("worker_state {keys} keys"));
        }
        // 5. Sanity: full admitted ops, yet nothing was recorded.
        let full_decisions = t.counters.get("full_decisions").and_then(Value::as_u64).unwrap_or(0).max(self.trial_counters().full_decisions());
        let sane = !(full_decisions > 0 && recorded_total == 0);
        // 6. What compact storage keeps, and the baseline.
        let report = self.trial_report(&t).await?;
        let mut causes = Vec::new();
        if left_after > 0 {
            causes.push(format!("{left_after} rows the trial added are still there after the check removed what it found"));
        }
        if !full_left.is_empty() {
            causes.push(format!("full-only tables are not empty: {}", full_left.join(", ")));
        }
        if !sane {
            causes.push(format!("full admitted {full_decisions} ops but the record is empty: the record was not being written"));
        }
        let passed = causes.is_empty();
        let words = result_words(&t, &counts, (posts, accounts, repaired), &full_left, &report, &causes);
        let faults = Counts::total(&counts.missed) + Counts::total(&counts.unexplained);
        for (result, n) in [
            ("removed", counts.removed),
            ("kept", counts.kept_author + counts.kept_member + counts.kept_mode),
            ("missed", Counts::total(&counts.missed)),
            ("unexplained", Counts::total(&counts.unexplained)),
        ] {
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!(METRIC_CHECK_ROWS, "result" => result).set(n as f64);
        }
        let result = json!({
            "verdict": if passed { "passed" } else { "failed" },
            "words": words,
            "causes": causes,
            "counts": counts.to_json(),
            "recorded_total": recorded_total,
            "full_decisions": full_decisions,
            "counts_checked": { "posts": posts, "accounts": accounts, "repaired": repaired },
            "full_only_left": full_left,
            "report": report,
            "faults": faults,
            "warning": faults > 0,
            "checked_at": rfc3339(&now()),
        });
        // The verdict, under the write guard, conditional on the trial.
        let mut sets = self.scope.write_guard().await;
        if sets.trial.as_ref().is_none_or(|m| m.id != id || m.state != TrialState::Checking) {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let n = if passed {
            sqlx::query("TRUNCATE full_trial_rows").execute(&mut *tx).await?;
            sqlx::query(
                "UPDATE worker_state SET value = value - 'trial', updated_at = now() \
                 WHERE key = 'public_transition' AND (value->'trial'->>'id')::bigint = $1 AND value->'trial'->>'state' = 'checking'",
            )
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
        } else {
            sqlx::query(
                "UPDATE worker_state SET value = jsonb_set(jsonb_set(value, '{trial,state}', '\"failed\"'), '{trial,check}', $2), updated_at = now() \
                 WHERE key = 'public_transition' AND (value->'trial'->>'id')::bigint = $1 AND value->'trial'->>'state' = 'checking'",
            )
            .bind(id)
            .bind(&result)
            .execute(&mut *tx)
            .await?
            .rows_affected()
        };
        if n != 1 {
            return Ok(());
        }
        sqlx::query("UPDATE full_trials SET state = $2, result = $3, ended_at = COALESCE(ended_at, now()) WHERE id = $1")
            .bind(id)
            .bind(if passed { "passed" } else { "failed" })
            .bind(&result)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        let next = if passed {
            None
        } else {
            let mut f = t.clone();
            f.state = TrialState::Failed;
            f.check = result.clone();
            Some(f)
        };
        self.scope.set_trial(&mut sets, next);
        drop(sets);
        self.trial_gauges().await;
        tracing::info!(event = "full_trial_checked", id, passed, faults, "{words}");
        Ok(())
    }

    /// Step 1 for one table: classify its recorded keys; with `remove`, the
    /// rows left behind or kept with no reason are removed by key, and the
    /// counts say what was found. Without, only what is still there is
    /// counted.
    async fn check_table(&self, t: &Trial, tbl: TrialTable, counts: &mut Counts, remove: bool) -> Result<(), sqlx::Error> {
        let (kept, reason) = rules(tbl, t.to, "$5");
        let sql = format!(
            "SELECT r.a, r.b, r.c, r.rev, x.present IS NOT NULL, COALESCE(x.kept, FALSE), x.reason \
             FROM full_trial_rows r LEFT JOIN LATERAL (SELECT TRUE AS present, {kept} AS kept, {reason} AS reason \
               FROM {table} x WHERE {key} LIMIT 1) x ON TRUE \
             WHERE r.tbl = $1 AND (r.a, r.b, r.c) > ($2, $3, $4) ORDER BY r.a, r.b, r.c LIMIT $6",
            table = tbl.name(),
            key = tbl.key_match(),
        );
        let switched = t.to != t.from;
        let mut after = (i32::MIN, i64::MIN, i16::MIN);
        loop {
            let page: Vec<Classified> = sqlx::query_as(&sql)
                .bind(tbl.code())
                .bind(after.0)
                .bind(after.1)
                .bind(after.2)
                .bind(t.measured_at)
                .bind(PAGE)
                .fetch_all(&self.pool)
                .await?;
            let Some(last) = page.last() else {
                return Ok(());
            };
            after = (last.0, last.1, last.2);
            for (a, b, c, rev, present, kept, reason) in page {
                if remove {
                    *counts.recorded.entry(tbl.name().to_string()).or_default() += 1;
                }
                let fault = match (present, kept, reason.as_deref()) {
                    (false, ..) => {
                        counts.removed += u64::from(remove);
                        None
                    }
                    (true, true, _) if switched => {
                        counts.kept_mode += u64::from(remove);
                        None
                    }
                    (true, true, Some("author")) => {
                        counts.kept_author += u64::from(remove);
                        None
                    }
                    (true, true, Some(_)) => {
                        counts.kept_member += u64::from(remove);
                        None
                    }
                    (true, true, None) => Some(&mut counts.unexplained),
                    (true, false, _) => Some(&mut counts.missed),
                };
                let Some(fault) = fault else {
                    continue;
                };
                *fault.entry(tbl.name().to_string()).or_default() += 1;
                if remove {
                    if counts.samples.len() < SAMPLES * 4 {
                        counts.samples.push(format!("{} ({a}, {b}, {c})", tbl.name()));
                    }
                    self.remove_left(t, tbl, (a, b, c, rev)).await?;
                }
            }
        }
    }

    /// Remove one recorded row the exit left, by key, under its DID's lock
    /// and the scope read guard, re-testing that nothing keeps it for a
    /// reason. A profile is cleared, not its identity row.
    async fn remove_left(&self, t: &Trial, tbl: TrialTable, key: (i32, i64, i16, Option<i64>)) -> Result<(), sqlx::Error> {
        let (kept, reason) = rules(tbl, t.to, "$5");
        let still = if t.to == t.from { format!("NOT ({kept} AND ({reason}) IS NOT NULL)") } else { format!("NOT {kept}") };
        let r = "(SELECT $1::integer AS a, $2::bigint AS b, $3::smallint AS c, $4::bigint AS rev) r";
        let sql = if tbl == TrialTable::Actors {
            format!(
                "UPDATE actors x SET display_name = NULL, description = NULL, avatar_cid = NULL, banner_cid = NULL, profile_record = NULL, \
                   profile_rev = NULL, profile_cid = NULL, pronouns = NULL, website = NULL, joined_via_starter_pack_uri = NULL, \
                   joined_via_starter_pack_cid = NULL, profile_created_at = NULL, self_labels = NULL \
                 FROM {r} WHERE {} AND {still}",
                tbl.key_match()
            )
        } else {
            format!("DELETE FROM {} x USING {r} WHERE {} AND {still}", tbl.name(), tbl.key_match())
        };
        let guard = self.scope.read_guard().await;
        let mut tx = self.pool.begin().await?;
        let author: Option<String> = sqlx::query_scalar("SELECT did FROM dids WHERE id = $1").bind(key.0).fetch_optional(&mut *tx).await?;
        if let Some(author) = &author {
            sqlx::query(LOCK_DID_SQL).bind(author).execute(&mut *tx).await?;
        }
        sqlx::query(&sql).bind(key.0).bind(key.1).bind(key.2).bind(key.3).bind(t.measured_at).execute(&mut *tx).await?;
        tx.commit().await?;
        drop(guard);
        Ok(())
    }

    /// Step 2: derived rows the trial's removed rows left. With `remove`,
    /// they are removed. Returns the count per table.
    async fn check_derived(&self, t: &Trial, remove: bool) -> Result<Vec<(&'static str, u64)>, sqlx::Error> {
        let posts = TrialTable::Posts.code();
        let gone = "NOT EXISTS (SELECT 1 FROM posts p WHERE p.author = r.a AND p.rkey = r.b)";
        let unreferenced = "NOT EXISTS (SELECT 1 FROM likes y WHERE y.subj_author = x.author AND y.subj_coll = 1 AND y.subj_rkey = x.rkey) \
             AND NOT EXISTS (SELECT 1 FROM reposts y WHERE y.subj_author = x.author AND y.subj_coll = 1 AND y.subj_rkey = x.rkey) \
             AND NOT EXISTS (SELECT 1 FROM posts y WHERE y.parent_author = x.author AND y.parent_coll = 1 AND y.parent_rkey = x.rkey) \
             AND NOT EXISTS (SELECT 1 FROM posts y WHERE y.quote_author = x.author AND y.quote_coll = 1 AND y.quote_rkey = x.rkey)";
        let author = did("x.author");
        let id = did("x.id");
        let checks: [(&'static str, String, &'static str); 3] = [
            (
                "post_search",
                format!("FROM full_trial_rows r JOIN post_search x ON x.author = r.a AND x.rkey = r.b WHERE r.tbl = {posts} AND {gone}"),
                "(author, rkey)",
            ),
            (
                "post_counts",
                format!(
                    "FROM full_trial_rows r JOIN post_counts x ON x.author = r.a AND x.rkey = r.b WHERE r.tbl = {posts} AND {gone} AND {unreferenced} AND NOT {}",
                    kept_whole(&author)
                ),
                "(author, rkey)",
            ),
            (
                "actor_counts",
                format!(
                    "FROM (SELECT DISTINCT a FROM full_trial_rows) r JOIN actor_counts x ON x.id = r.a WHERE NOT {} \
                     AND NOT EXISTS (SELECT 1 FROM actors y WHERE y.id = x.id) \
                     AND NOT EXISTS (SELECT 1 FROM posts y WHERE y.author = x.id) AND NOT EXISTS (SELECT 1 FROM likes y WHERE y.author = x.id) \
                     AND NOT EXISTS (SELECT 1 FROM reposts y WHERE y.author = x.id) AND NOT EXISTS (SELECT 1 FROM follows y WHERE y.author = x.id) \
                     AND NOT EXISTS (SELECT 1 FROM follows y WHERE y.subject = x.id)",
                    kept_whole(&id)
                ),
                "(id)",
            ),
        ];
        let _ = t;
        let mut out = Vec::new();
        for (table, from, key) in checks {
            let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) {from}")).fetch_one(&self.pool).await?;
            if remove && n > 0 {
                let cols = key.trim_matches(|c| c == '(' || c == ')').split(", ").map(|c| format!("x.{c}")).collect::<Vec<_>>().join(", ");
                let guard = self.scope.read_guard().await;
                sqlx::query(&format!("DELETE FROM {table} WHERE {key} IN (SELECT {cols} {from})")).execute(&self.pool).await?;
                drop(guard);
            }
            out.push((table, u64::try_from(n).unwrap_or(0)));
        }
        Ok(out)
    }

    /// Step 3: for every distinct subject the record names, its count rows
    /// recomputed from stored rows; a difference is repaired and counted.
    /// Returns the posts and the accounts checked, and the rows repaired.
    async fn check_counts(&self) -> Result<(u64, u64, u64), sqlx::Error> {
        let posts: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT DISTINCT a, b FROM (SELECT s_a AS a, s_b AS b FROM full_trial_rows WHERE s_a IS NOT NULL AND s_b IS NOT NULL \
             UNION SELECT q_a, q_b FROM full_trial_rows WHERE q_a IS NOT NULL AND q_b IS NOT NULL) s",
        )
        .fetch_all(&self.pool)
        .await?;
        let accounts: Vec<i32> = sqlx::query_scalar("SELECT DISTINCT s_a FROM full_trial_rows WHERE s_a IS NOT NULL AND s_b IS NULL").fetch_all(&self.pool).await?;
        let mut repaired = 0;
        for (a, k) in &posts {
            let mut tx = self.pool.begin().await?;
            let stored: Option<(i32, i32, i32, i32)> =
                sqlx::query_as("SELECT like_count, repost_count, reply_count, quote_count FROM post_counts WHERE author = $1 AND rkey = $2 FOR UPDATE")
                    .bind(a)
                    .bind(k)
                    .fetch_optional(&mut *tx)
                    .await?;
            let fresh: (i64, i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(DISTINCT author) FROM likes WHERE subj_author = $1 AND subj_coll = 1 AND subj_rkey = $2), \
                        (SELECT count(DISTINCT author) FROM reposts WHERE subj_author = $1 AND subj_coll = 1 AND subj_rkey = $2), \
                        (SELECT count(*) FROM posts WHERE parent_author = $1 AND parent_coll = 1 AND parent_rkey = $2), \
                        (SELECT count(*) FROM posts WHERE quote_author = $1 AND quote_coll = 1 AND quote_rkey = $2)",
            )
            .bind(a)
            .bind(k)
            .fetch_one(&mut *tx)
            .await?;
            let have = stored.map_or((0, 0, 0, 0), |s| (i64::from(s.0), i64::from(s.1), i64::from(s.2), i64::from(s.3)));
            if have != fresh {
                if fresh == (0, 0, 0, 0) {
                    sqlx::query("DELETE FROM post_counts WHERE author = $1 AND rkey = $2").bind(a).bind(k).execute(&mut *tx).await?;
                } else {
                    sqlx::query(
                        "INSERT INTO post_counts (author, rkey, like_count, repost_count, reply_count, quote_count) VALUES ($1, $2, $3, $4, $5, $6) \
                         ON CONFLICT (author, rkey) DO UPDATE SET like_count = EXCLUDED.like_count, repost_count = EXCLUDED.repost_count, \
                           reply_count = EXCLUDED.reply_count, quote_count = EXCLUDED.quote_count",
                    )
                    .bind(a)
                    .bind(k)
                    .bind(i32::try_from(fresh.0).unwrap_or(i32::MAX))
                    .bind(i32::try_from(fresh.1).unwrap_or(i32::MAX))
                    .bind(i32::try_from(fresh.2).unwrap_or(i32::MAX))
                    .bind(i32::try_from(fresh.3).unwrap_or(i32::MAX))
                    .execute(&mut *tx)
                    .await?;
                }
                repaired += 1;
            }
            tx.commit().await?;
        }
        for a in &accounts {
            let mut tx = self.pool.begin().await?;
            let stored: Option<i32> = sqlx::query_scalar("SELECT followers_count FROM actor_counts WHERE id = $1 FOR UPDATE").bind(a).fetch_optional(&mut *tx).await?;
            let fresh: i64 = sqlx::query_scalar("SELECT count(*) FROM follows WHERE subject = $1").bind(a).fetch_one(&mut *tx).await?;
            if i64::from(stored.unwrap_or(0)) != fresh {
                sqlx::query(
                    "INSERT INTO actor_counts (id, followers_count) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET followers_count = EXCLUDED.followers_count",
                )
                .bind(a)
                .bind(i32::try_from(fresh).unwrap_or(i32::MAX))
                .execute(&mut *tx)
                .await?;
                repaired += 1;
            }
            tx.commit().await?;
        }
        Ok((u64::try_from(posts.len()).unwrap_or(0), u64::try_from(accounts.len()).unwrap_or(0), repaired))
    }

    /// Step 6: what compact storage keeps, every table before and now beside
    /// the start mode's own writes, and the database before and now.
    async fn trial_report(&self, t: &Trial) -> Result<Value, sqlx::Error> {
        let base: Value = sqlx::query_scalar("SELECT baseline FROM full_trials WHERE id = $1").bind(t.id).fetch_optional(&self.pool).await?.unwrap_or(Value::Null);
        let dids_max = base.get("dids_max").and_then(Value::as_i64).unwrap_or(0);
        let aliases_max = base.get("rkey_aliases_max").and_then(Value::as_i64).unwrap_or(0);
        let (dids_new, dids_bytes): (i64, i64) = sqlx::query_as("SELECT count(*), COALESCE(sum(pg_column_size(d.*)), 0)::bigint FROM dids d WHERE id > $1")
            .bind(dids_max)
            .fetch_one(&self.pool)
            .await?;
        let (aliases_new, aliases_bytes): (i64, i64) =
            sqlx::query_as("SELECT count(*), COALESCE(sum(pg_column_size(a.*)), 0)::bigint FROM rkey_aliases a WHERE id > $1")
                .bind(aliases_max)
                .fetch_one(&self.pool)
                .await?;
        let now_tables = table_figures(&self.pool).await?;
        let start = t.counters.get("start_writes").cloned().unwrap_or_else(|| json!({}));
        let start_mem = self.trial_counters().to_json(None)["start_writes"].clone();
        let start = if start_mem.as_object().is_some_and(|m| !m.is_empty()) { start_mem } else { start };
        let mut tables = serde_json::Map::new();
        for (name, now_fig) in &now_tables {
            let before = base.pointer(&format!("/tables/{name}")).cloned().unwrap_or(Value::Null);
            tables.insert(
                name.clone(),
                json!({
                    "rows_before": before.get("rows"),
                    "bytes_before": before.get("bytes"),
                    "rows_now": now_fig.get("rows"),
                    "bytes_now": now_fig.get("bytes"),
                    "start_writes": start.get(name).cloned().unwrap_or(json!(0)),
                }),
            );
        }
        Ok(json!({
            "dids_added": dids_new,
            "dids_bytes": dids_bytes + aliases_bytes,
            "rkey_aliases_added": aliases_new,
            "tables": tables,
            "database_before": base.get("database_bytes"),
            "database_now": database_bytes(&self.pool).await?,
        }))
    }

    /// `full-trial check`: run a failed trial's check again.
    ///
    /// # Errors
    ///
    /// [`Refused`] when no failed trial is held.
    pub async fn recheck_trial(&self) -> Result<Value, Refused> {
        let db = |e: sqlx::Error| Refused(e.to_string());
        let mut sets = self.scope.write_guard().await;
        let Some(mut t) = sets.trial.clone().filter(|t| t.state == TrialState::Failed) else {
            return Err(Refused("No failed trial to check again; a trial's check runs by itself when its exit ends.".into()));
        };
        t.state = TrialState::Checking;
        t.check = json!({});
        let n = sqlx::query(
            "UPDATE worker_state SET value = jsonb_set(jsonb_set(value, '{trial,state}', '\"checking\"'), '{trial,check}', '{}'::jsonb), updated_at = now() \
             WHERE key = 'public_transition' AND (value->'trial'->>'id')::bigint = $1",
        )
        .bind(t.id)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        if n != 1 {
            return Err(Refused("The stored trial changed; nothing was done.".into()));
        }
        sqlx::query("UPDATE full_trials SET state = 'checking' WHERE id = $1").bind(t.id).execute(&self.pool).await.map_err(db)?;
        self.scope.set_trial(&mut sets, Some(t));
        drop(sets);
        drop(self.spawn_trial_check());
        Ok(json!({ "action": "checking" }))
    }
}

fn day_time(t: &DateTime<Utc>) -> String {
    t.format("%-d %b %H:%M").to_string()
}

fn n_words(n: u64) -> String {
    #[allow(clippy::cast_precision_loss)]
    let f = n as f64;
    if n >= 1_000_000 {
        format!("{:.2}M", f / 1e6)
    } else if n >= 10_000 {
        let s = n.to_string();
        let (a, b) = s.split_at(s.len() - 3);
        format!("{a} {b}")
    } else {
        n.to_string()
    }
}

fn cause_words(c: Option<&str>) -> &'static str {
    match c {
        Some("time") => "time limit",
        Some("growth") => "database growth limit",
        Some("free_space") => "free-space floor",
        Some("manual") => "ended by hand",
        Some("startup") => "a bound passed while Prism was down",
        Some("switch") => "a switch to another mode",
        _ => "ended",
    }
}

/// The result in plain words.
fn result_words(t: &Trial, c: &Counts, counted: (u64, u64, u64), full_left: &[String], report: &Value, causes: &[String]) -> String {
    let from = t.from.as_str();
    let to = t.to.as_str();
    let recorded_total: u64 = c.recorded.values().sum();
    let mut out = vec![format!(
        "Full trial from {from}, {} to {} ({}).",
        day_time(&t.started_at),
        day_time(&now()),
        cause_words(t.ended_by.as_deref())
    )];
    out.push(format!("Recorded {} rows in {} tables. Removed {}.", n_words(recorded_total), c.recorded.len(), n_words(c.removed)));
    // The start mode's own writes, per table, with the table's rows before
    // and now.
    let tables = report.get("tables").and_then(Value::as_object);
    let mut own: Vec<(String, u64)> = tables
        .into_iter()
        .flatten()
        .filter_map(|(name, v)| v.get("start_writes").and_then(Value::as_u64).filter(|n| *n > 0).map(|n| (name.clone(), n)))
        .collect();
    own.sort_by(|a, b| b.1.cmp(&a.1));
    let own_total: u64 = own.iter().map(|(_, n)| n).sum();
    let details: Vec<String> = own
        .iter()
        .take(5)
        .map(|(name, n)| {
            let v = &tables.and_then(|t| t.get(name)).cloned().unwrap_or(Value::Null);
            let rows = |k: &str| v.get(k).and_then(Value::as_u64).map_or_else(|| "?".to_string(), n_words);
            format!("{name}: {} before, {} now; {from} wrote {}", rows("rows_before"), rows("rows_now"), n_words(*n))
        })
        .collect();
    if own_total > 0 {
        out.push(format!("{from}'s own writes during the trial: {} rows ({}).", n_words(own_total), details.join("; ")));
    } else {
        out.push(format!("{from}'s own writes during the trial: none."));
    }
    if t.to != t.from {
        out.push(format!("The trial did not return to its start mode: it ended in {to}, and kept {} rows by {to}'s rules.", n_words(c.kept_mode)));
    }
    let kept = c.kept_author + c.kept_member;
    if kept > 0 {
        out.push(format!(
            "Kept by {to}'s own rules: {} (their authors joined S during the trial: {}; they name a member who joined during the trial: {}).",
            n_words(kept),
            n_words(c.kept_author),
            n_words(c.kept_member)
        ));
    } else {
        out.push(format!("Kept by {to}'s own rules: none."));
    }
    let fault = |m: &BTreeMap<String, u64>| {
        if m.is_empty() {
            "none".to_string()
        } else {
            let by: Vec<String> = m.iter().map(|(t, n)| format!("{t} {n}")).collect();
            format!("{} ({}), removed by the check", n_words(m.values().sum()), by.join(", "))
        }
    };
    out.push(format!("Missed by the exit: {}.", fault(&c.missed)));
    out.push(format!("Kept by the exit with no reason: {}.", fault(&c.unexplained)));
    if !c.samples.is_empty() {
        out.push(format!("Keys: {}.", c.samples.join("; ")));
    }
    let (posts, accounts, repaired) = counted;
    out.push(format!(
        "Counts checked for {} posts and {} accounts: {}.",
        n_words(posts),
        n_words(accounts),
        if repaired == 0 { "all match".to_string() } else { format!("{repaired} repaired") }
    ));
    out.push(if full_left.is_empty() { "Full-only tables: empty.".to_string() } else { format!("Full-only tables: not empty ({}).", full_left.join(", ")) });
    let dids = report.get("dids_added").and_then(Value::as_u64).unwrap_or(0);
    let dids_bytes = report.get("dids_bytes").and_then(Value::as_u64).unwrap_or(0);
    out.push(format!("Accounts first seen during the trial and kept by design: {} ({}).", n_words(dids), bytes_words(dids_bytes)));
    let before = report.get("database_before").and_then(Value::as_u64).unwrap_or(0);
    let now_b = report.get("database_now").and_then(Value::as_u64).unwrap_or(0);
    out.push(format!(
        "Database: {} before, {} now. Postgres keeps the {} for reuse; the reclaim step (operations doc) returns it to the disk.",
        bytes_words(before),
        bytes_words(now_b),
        bytes_words(now_b.saturating_sub(before))
    ));
    if causes.is_empty() {
        out.push("Passed.".to_string());
    } else {
        out.push(format!("Failed: {}.", causes.join("; ")));
    }
    out.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every record table has a keep rule and a reason for both modes the
    /// exit can go to, over the stored row.
    #[test]
    fn every_record_table_has_its_rules() {
        for tbl in TrialTable::ALL {
            for to in [ScopeValue::Community, ScopeValue::KryphocronOnly] {
                let (kept, reason) = rules(tbl, to, "$5");
                assert!(!kept.is_empty() && reason.contains("$5"), "{tbl:?} {to:?}");
            }
        }
        // To kryphocron_only, feed items and starter packs are never kept.
        assert_eq!(rules(TrialTable::FeedItems, ScopeValue::KryphocronOnly, "$5").0, "FALSE");
        assert!(rules(TrialTable::Likes, ScopeValue::Community, "$5").0.contains("x.subj_author"));
    }

    #[test]
    fn numbers_in_words() {
        assert_eq!(n_words(412_000), "412 000");
        assert_eq!(n_words(31_200_000), "31.20M");
        assert_eq!(n_words(7), "7");
    }
}
