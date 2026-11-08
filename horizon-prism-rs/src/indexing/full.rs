//! Full indexing's record groups: which collections each one stores for
//! every account.

use std::collections::BTreeSet;

use crate::config::FullGroup;
use crate::firehose::classifier::{
    NSID_ACTOR_STATUS, NSID_BLOCK, NSID_CONTENT_VISIBILITY, NSID_FOLLOW, NSID_LABELER_SERVICE, NSID_LIKE, NSID_LIST,
    NSID_LIST_OPTOUT, NSID_LISTBLOCK, NSID_LISTITEM, NSID_NOTIFICATION_DECLARATION, NSID_POLL_POST, NSID_POST,
    NSID_POSTGATE, NSID_PROFILE, NSID_REPOST, NSID_STARTER_PACK, NSID_THREADGATE, PLURAL_COLLECTIONS,
};

/// The collections of one group.
#[must_use]
pub fn collections(group: FullGroup) -> &'static [&'static str] {
    match group {
        FullGroup::Posts => &[NSID_POST, NSID_THREADGATE, NSID_POSTGATE],
        FullGroup::Likes => &[NSID_LIKE],
        FullGroup::Reposts => &[NSID_REPOST],
        FullGroup::Follows => &[NSID_FOLLOW],
        FullGroup::Blocks => &[NSID_BLOCK, NSID_LISTBLOCK],
        FullGroup::Lists => &[NSID_LIST, NSID_LISTITEM, NSID_LIST_OPTOUT],
        FullGroup::Profiles => &[
            NSID_PROFILE,
            NSID_LABELER_SERVICE,
            NSID_NOTIFICATION_DECLARATION,
            NSID_ACTOR_STATUS,
            NSID_CONTENT_VISIBILITY,
        ],
        FullGroup::StarterPacks => &[NSID_STARTER_PACK],
        FullGroup::Polls => &[NSID_POLL_POST],
    }
}

/// The group a collection belongs to, if any.
#[must_use]
pub fn group_of(nsid: &str) -> Option<FullGroup> {
    FullGroup::ALL.into_iter().find(|g| collections(*g).contains(&nsid))
}

/// The full set: every collection of every enabled group, plus the plural
/// and pronoun records, which are kept for any author.
#[must_use]
pub fn full_set(kinds: &[FullGroup]) -> BTreeSet<String> {
    kinds
        .iter()
        .flat_map(|g| collections(*g).iter())
        .chain(PLURAL_COLLECTIONS.iter())
        .map(|s| (*s).to_string())
        .collect()
}

/// The content tables of the enabled groups whose rows name their author
/// in an `author` column.
#[must_use]
pub fn authored_tables(kinds: &[FullGroup]) -> Vec<&'static str> {
    let mut out = Vec::new();
    for g in kinds {
        out.extend_from_slice(match g {
            FullGroup::Posts => &["posts", "threadgates", "postgates"][..],
            FullGroup::Likes => &["likes"],
            FullGroup::Reposts => &["reposts"],
            FullGroup::Follows => &["follows"],
            FullGroup::Blocks => &["blocks", "list_blocks"],
            FullGroup::Lists => &["lists", "list_items", "list_optouts"],
            FullGroup::Profiles => &["actor_self_records", "labeler_services"],
            FullGroup::StarterPacks => &["starter_packs"],
            FullGroup::Polls => &["nearhorizon_poll_posts"],
        });
    }
    out
}

/// The `worker_state` key of each group's history.
pub const GROUPS_KEY: &str = "full_groups";
/// The `worker_state` key of when full was entered: kept through an
/// interrupted narrowing, removed when the narrowing or the drain ends.
pub const SINCE_KEY: &str = "full_since";

/// Whether community retention's scope step over `table` is skipped in
/// full: public scope steps delete nothing of an enabled group. Search rows
/// still follow S, and the orphan-only count sweep still runs.
#[must_use]
pub fn retention_skips(table: &str, kinds: &[FullGroup]) -> bool {
    let on = |g| kinds.contains(&g);
    match table {
        "posts" | "threadgates" | "postgates" => on(FullGroup::Posts),
        "feed_items" => on(FullGroup::Posts) || on(FullGroup::Reposts),
        "likes" => on(FullGroup::Likes),
        "reposts" => on(FullGroup::Reposts),
        "follows" => on(FullGroup::Follows),
        "blocks" | "list_blocks" => on(FullGroup::Blocks),
        "lists" | "list_items" | "list_optouts" => on(FullGroup::Lists),
        "starter_packs" | "starter_pack_counts" => on(FullGroup::StarterPacks),
        "labeler_services" | "actor_self_records" | "actors" | "actor_counts" => on(FullGroup::Profiles),
        "nearhorizon_poll_posts" => on(FullGroup::Polls),
        _ => false,
    }
}

/// Entering full: every enabled group's history starts again at Crawling,
/// at `crawl_generation`, with a new group generation, in the caller's
/// transaction.
///
/// # Errors
///
/// Any database error.
pub async fn reset_groups(conn: &mut sqlx::PgConnection, crawl_generation: u64, kinds: &[FullGroup]) -> Result<(), sqlx::Error> {
    let old: Option<serde_json::Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1 FOR UPDATE")
        .bind(GROUPS_KEY)
        .fetch_optional(&mut *conn)
        .await?;
    let mut groups = serde_json::Map::new();
    for g in kinds {
        let previous = old
            .as_ref()
            .and_then(|v| v.pointer(&format!("/groups/{}/generation", g.as_str())))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        groups.insert(
            g.as_str().to_string(),
            serde_json::json!({ "generation": previous + 1, "state": "crawling", "incomplete": false, "union_done": true }),
        );
    }
    let value = serde_json::json!({ "crawl_generation": crawl_generation, "groups": groups });
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(GROUPS_KEY)
    .bind(value)
    .execute(&mut *conn)
    .await?;
    // The first entry's time stays: a narrowing stopped by a switch back
    // has not removed what full wrote before it.
    sqlx::query("INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb(now()), now()) ON CONFLICT (key) DO NOTHING")
        .bind(SINCE_KEY)
        .execute(conn)
        .await?;
    Ok(())
}

/// The accounts excluded after their Refract restoration ended, read in
/// the caller's transaction.
///
/// # Errors
///
/// Any database error.
pub async fn excluded_dids(conn: &mut sqlx::PgConnection) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT d.did FROM refract_excluded x JOIN dids d ON d.id = x.account").fetch_all(conn).await
}

/// Record one takedown relay's `#account` status: a non-active status is
/// kept per relay, from when the suspension began; a return to active
/// deletes it. An event without an
/// `active` flag says nothing and writes nothing. The DID is interned in
/// its own statement first.
///
/// # Errors
///
/// Any database error.
pub async fn record_relay_status(pool: &sqlx::PgPool, did: &str, relay: &str, active: Option<bool>, status: Option<&str>) -> Result<(), sqlx::Error> {
    match active {
        None => Ok(()),
        Some(true) => {
            sqlx::query("DELETE FROM relay_account_status r USING dids d WHERE d.id = r.account AND d.did = $1 AND r.relay = $2")
                .bind(did)
                .bind(relay)
                .execute(pool)
                .await?;
            Ok(())
        }
        Some(false) => {
            sqlx::query("INSERT INTO dids (did) VALUES ($1) ON CONFLICT (did) DO NOTHING").bind(did).execute(pool).await?;
            sqlx::query(
                "INSERT INTO relay_account_status (account, relay, status) SELECT id, $2, $3 FROM dids WHERE did = $1 \
                 ON CONFLICT (account, relay) DO UPDATE SET status = EXCLUDED.status",
            )
            .bind(did)
            .bind(relay)
            .bind(status.unwrap_or("inactive"))
            .execute(pool)
            .await?;
            metrics::counter!(METRIC_SUSPENDED, "relay" => relay.to_string()).increment(1);
            Ok(())
        }
    }
}

/// The accounts whose takedown-relay suspension has lasted `days`: outside
/// S, not members and not listed by Refract.
const LONG_SUSPENDED_SQL: &str = "SELECT DISTINCT d.did FROM relay_account_status r JOIN dids d ON d.id = r.account \
     WHERE r.relay = ANY($1) AND r.at < now() - make_interval(days => $2) \
       AND NOT EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = d.did) \
       AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did = d.did) \
       AND NOT EXISTS (SELECT 1 FROM refract_dids rf WHERE rf.did = d.did AND rf.state IN ('restored', 'grace')) \
     ORDER BY d.did LIMIT 1000";

/// Metric: accounts whose stored public rows were removed after a long
/// suspension.
pub const METRIC_SUSPENDED_PURGED: &str = "full_suspended_purged_total";

/// The daily removal of long-suspended accounts' public rows: in full under
/// `suspended_accounts = "refract_only"`, an account a takedown relay has
/// suspended for `[full] suspended_purge_days` (0: never) loses its own
/// public rows, by the per-account steps community retention uses, so its
/// likes, replies and quotes of members' posts stay, as a narrowing keeps
/// them.
pub struct SuspendedPurge {
    scope: crate::aggregations::ScopeState,
    days: u32,
}

impl SuspendedPurge {
    #[must_use]
    pub fn new(scope: crate::aggregations::ScopeState, days: u32) -> Self {
        Self { scope, days }
    }
}

#[async_trait::async_trait]
impl crate::db::retention::RetentionTask for SuspendedPurge {
    fn name(&self) -> &'static str {
        "full_suspended"
    }

    async fn run(
        &self,
        pool: &sqlx::PgPool,
        guard: &dyn crate::db::retention::ScopeGuard,
        _settings: &crate::db::retention::RetentionSettings,
    ) -> Result<crate::db::retention::RetentionReport, sqlx::Error> {
        let mut report = crate::db::retention::RetentionReport::default();
        let relays = {
            let sets = self.scope.read_guard().await;
            if sets.public.breadth == crate::indexing::Breadth::Full { sets.full.takedown_relays.clone() } else { Vec::new() }
        };
        if relays.is_empty() || self.days == 0 {
            return Ok(report);
        }
        let days = i32::try_from(self.days).unwrap_or(i32::MAX);
        let dids: Vec<String> = sqlx::query_scalar(LONG_SUSPENDED_SQL).bind(&relays).bind(days).fetch_all(pool).await?;
        if dids.is_empty() {
            return Ok(report);
        }
        let steps = crate::db::retention::scope_steps(crate::private_tier::retention::row_authors(pool).await?);
        let mut totals: std::collections::BTreeMap<&'static str, u64> = std::collections::BTreeMap::new();
        for did in &dids {
            for step in steps.iter().filter(|s| s.candidates.is_some()) {
                let n = crate::db::retention::delete_did_rows(pool, guard, step, did, false).await?;
                *totals.entry(step.table).or_default() += n;
            }
        }
        metrics::counter!(METRIC_SUSPENDED_PURGED).increment(u64::try_from(dids.len()).unwrap_or(u64::MAX));
        tracing::info!(event = "full_suspended_purged", accounts = dids.len(), days = self.days, "long-suspended accounts' public rows removed");
        report.deleted = totals.into_iter().collect();
        Ok(report)
    }
}

/// Metric: non-active `#account` statuses recorded, by relay.
pub const METRIC_SUSPENDED: &str = "account_suspended_total";

/// Measured growth of live ingest with every group on, in compact storage,
/// in bytes a day.
pub const GROWTH_BYTES_PER_DAY: u64 = 12 * 1_000_000_000;

/// The database volume's free space: the Postgres mount when it is
/// visible, else the data directory's disk.
#[must_use]
pub fn database_free_bytes(data_dir: &std::path::Path) -> Option<u64> {
    crate::admin::stats::space(std::path::Path::new(crate::admin::stats::POSTGRES_MOUNT))
        .or_else(|| crate::admin::stats::space(data_dir))
        .map(|s| s.free)
}

/// The warnings shown before full is switched on (never a refusal): free
/// disk and the days it lasts at the measured growth, the storage budget
/// against the volume, the writer and the crawl.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn warnings(kinds: &[FullGroup], database_bytes: Option<u64>, free_bytes: Option<u64>, budget_bytes: Option<u64>, writer_lanes: u32) -> Vec<String> {
    let gb = |b: u64| format!("{:.1} GB", b as f64 / 1e9);
    // Groups other than likes, follows and reposts are small; likes are
    // about half of the growth.
    let share: u64 = kinds
        .iter()
        .map(|g| match g {
            FullGroup::Likes => 50,
            FullGroup::Posts => 25,
            FullGroup::Reposts => 8,
            FullGroup::Follows => 7,
            FullGroup::Blocks => 2,
            FullGroup::Lists | FullGroup::Profiles | FullGroup::StarterPacks | FullGroup::Polls => 2,
        })
        .sum::<u64>()
        .min(100);
    let per_day = GROWTH_BYTES_PER_DAY * share / 100;
    let mut out = Vec::new();
    match free_bytes {
        Some(free) => out.push(format!(
            "The database volume has {} free. Full indexing of the enabled groups grows the database by about {} a day, so the volume fills in about {} days.",
            gb(free),
            gb(per_day),
            free.checked_div(per_day).unwrap_or(0)
        )),
        None => out.push(format!(
            "The database volume's free space is not visible to Prism. Full indexing of the enabled groups grows the database by about {} a day.",
            gb(per_day)
        )),
    }
    match (budget_bytes, database_bytes, free_bytes) {
        (Some(budget), Some(used), Some(free)) if budget < used + free => out.push(format!(
            "storage.budget_bytes is {}, less than the volume ({} used and free); raise it if the database may use the whole volume.",
            gb(budget),
            gb(used + free)
        )),
        (None, _, _) => out.push("storage.budget_bytes is not set: Health shows no budget warnings.".to_string()),
        _ => {}
    }
    out.push(if writer_lanes <= 1 {
        "One writer lane applies the firehose (firehose.writer_lanes = 1); at full's rate it may fall behind the relays. `admin indexing full-check` advises how many lanes this machine needs.".to_string()
    } else {
        format!("{writer_lanes} writer lanes apply the firehose (firehose.writer_lanes); `admin indexing full-check` advises whether they keep up with the relays.")
    });
    out.push(
        "The history crawl is not part of this version: full stores the network from now on, and accounts outside S read as in community until their history is crawled."
            .to_string(),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::firehose::classifier::STORED_SET;

    /// Every group's collections are stored collections, no collection is
    /// in two groups, and the full set of every group is the stored set
    /// without the private collection.
    #[test]
    fn groups_partition_the_public_stored_set() {
        let mut seen = BTreeSet::new();
        for g in FullGroup::ALL {
            for c in collections(g) {
                assert!(STORED_SET.contains(c), "{c}");
                assert!(seen.insert(*c), "{c} is in two groups");
                assert_eq!(group_of(c), Some(g));
            }
        }
        let all = full_set(&FullGroup::ALL);
        let public: BTreeSet<String> = STORED_SET
            .iter()
            .filter(|c| !c.starts_with("tools.kryphocron."))
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(all, public);
        let posts_only = full_set(&[FullGroup::Posts]);
        assert!(posts_only.contains(NSID_POST) && posts_only.contains(PLURAL_COLLECTIONS[0]));
        assert!(!posts_only.contains(NSID_LIKE));
    }

    /// The warnings never refuse; they name the days left and a budget
    /// smaller than the volume.
    #[test]
    fn warnings_name_days_and_budget() {
        let w = warnings(&FullGroup::ALL, Some(100_000_000_000), Some(1_200_000_000_000), Some(500_000_000_000), 1);
        assert!(w[0].contains("about 100 days"), "{w:?}");
        assert!(w[1].contains("storage.budget_bytes is 500.0 GB"), "{w:?}");
        let likes_only = warnings(&[FullGroup::Likes], None, Some(600_000_000_000), None, 4);
        assert!(likes_only[0].contains("about 100 days"), "{likes_only:?}");
        assert!(w.iter().any(|l| l.contains("One writer lane")) && likes_only.iter().any(|l| l.starts_with("4 writer lanes")));
        assert!(likes_only.iter().any(|l| l.contains("not set")));
    }
}
