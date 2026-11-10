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
    record_relay_status_in(&mut *pool.acquire().await?, did, relay, active, status).await
}

/// [`record_relay_status`] on `conn`, in its transaction if it has one.
///
/// # Errors
///
/// Any database error.
pub async fn record_relay_status_in(conn: &mut sqlx::PgConnection, did: &str, relay: &str, active: Option<bool>, status: Option<&str>) -> Result<(), sqlx::Error> {
    match active {
        None => Ok(()),
        Some(true) => {
            sqlx::query("DELETE FROM relay_account_status r USING dids d WHERE d.id = r.account AND d.did = $1 AND r.relay = $2")
                .bind(did)
                .bind(relay)
                .execute(&mut *conn)
                .await?;
            Ok(())
        }
        Some(false) => {
            sqlx::query("INSERT INTO dids (did) VALUES ($1) ON CONFLICT (did) DO NOTHING").bind(did).execute(&mut *conn).await?;
            sqlx::query(
                "INSERT INTO relay_account_status (account, relay, status) SELECT id, $2, $3 FROM dids WHERE did = $1 \
                 ON CONFLICT (account, relay) DO UPDATE SET status = EXCLUDED.status",
            )
            .bind(did)
            .bind(relay)
            .bind(status.unwrap_or("inactive"))
            .execute(&mut *conn)
            .await?;
            metrics::counter!(METRIC_SUSPENDED, "relay" => relay.to_string()).increment(1);
            Ok(())
        }
    }
}

/// A takedown relay's `deactivated` or `deleted` for an account with an
/// identity row: its `actors.account_status` is set at once, so reads hide
/// it, and the identity refresher, which reads the account's own server,
/// confirms or undoes it; a deletion's purge waits for that. `takendown` and
/// `suspended` never write it: they stay per relay. The caller leaves out
/// accounts Refract lists. Returns whether a row changed.
///
/// # Errors
///
/// Any database error.
pub async fn provisional_status(pool: &sqlx::PgPool, did: &str, status: Option<&str>) -> Result<bool, sqlx::Error> {
    provisional_status_in(&mut *pool.acquire().await?, did, status).await
}

/// [`provisional_status`] on `conn`, in its transaction if it has one.
///
/// # Errors
///
/// Any database error.
pub async fn provisional_status_in(conn: &mut sqlx::PgConnection, did: &str, status: Option<&str>) -> Result<bool, sqlx::Error> {
    let Some(status) = status.filter(|s| matches!(*s, "deactivated" | "deleted")) else {
        return Ok(false);
    };
    let n = sqlx::query(
        "UPDATE actors a SET account_status = $2 FROM dids d \
         WHERE d.did = $1 AND a.id = d.id AND a.account_status IS DISTINCT FROM $2 AND a.account_status IS DISTINCT FROM 'deleted'",
    )
    .bind(did)
    .bind(status)
    .execute(conn)
    .await?
    .rows_affected();
    if n > 0 {
        metrics::counter!(METRIC_PROVISIONAL, "status" => status.to_string()).increment(1);
        tracing::info!(event = "account_status_provisional", did, status, "a takedown relay reported the account inactive; hidden until its server is asked");
    }
    Ok(n > 0)
}

/// Metric: account statuses set at once from a takedown relay's
/// `deactivated` or `deleted`, by status.
pub const METRIC_PROVISIONAL: &str = "account_status_provisional_total";

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

/// Metric: options of full indexing set aside at startup because they need
/// something the configuration does not give (a gauge).
pub const METRIC_SET_ASIDE: &str = "full_options_set_aside";

/// The combinations of full indexing's options that cannot hold as written,
/// each with the fallback that applies instead, in words. None of them stops
/// the start; each is logged once and shown on Health. The directory
/// export's check (`[full_crawl] identity_source = "plc_export"` with a
/// directory that serves none) needs the directory, so the crawl makes it.
#[must_use]
pub fn set_aside(file: &crate::config::FileConfig) -> Vec<String> {
    let (full, crawl) = (&file.full, &file.full_crawl);
    let on = |g: FullGroup| full.kinds.contains(&g);
    let mut out = Vec::new();
    if full.blocks_source == crate::config::BlocksSource::Local && !(on(FullGroup::Blocks) && on(FullGroup::Lists)) {
        out.push(if file.farsight.url.is_some() {
            "full.blocks_source = \"local\" needs the blocks and lists groups: blocks keep a member side, and other accounts' blocks are read from Farsight.".to_string()
        } else {
            "full.blocks_source = \"local\" needs the blocks and lists groups: blocks keep a member side, and with no Farsight set up only those apply.".to_string()
        });
    }
    let cutoff = crawl.skip_older_than_days > 0 || file.retention.max_age_days > 0;
    if cutoff {
        out.push("A history cutoff is set (full_crawl.skip_older_than_days or retention.max_age_days): counts and other interaction data keep coming from Constellation and today's sources.".to_string());
    } else if full.counts == crate::config::FullCounts::Auto {
        for (g, what) in [(FullGroup::Likes, "Like counts"), (FullGroup::Reposts, "Repost counts"), (FullGroup::Posts, "Reply and quote counts"), (FullGroup::Follows, "Follower counts")] {
            if !on(g) {
                out.push(format!("{what} come from Constellation: the {} group is not in full.kinds.", g.as_str()));
            }
        }
    }
    if full.labels == crate::config::FullLabels::Subscribe && full.label_sources.is_empty() {
        out.push("full.labels = \"subscribe\" with no label_sources: each labeler's labels are read per request instead.".to_string());
    }
    let labels: std::collections::BTreeSet<&String> = file.firehose.relay_labels.values().collect();
    let unknown: Vec<&String> = full.takedown_relays.iter().filter(|r| !labels.contains(r)).collect();
    for r in &unknown {
        out.push(format!("full.takedown_relays names {r}, which is not a label in firehose.relay_labels: set aside."));
    }
    if full.takedown_relays.len() == unknown.len() {
        out.push("full.takedown_relays names no configured relay: suspended_accounts has no effect, and no relay's account statuses are recorded.".to_string());
    }
    out
}

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
        "The history crawl ([full_crawl]) fetches every listed account's repository at repos_per_sec; accounts outside S read as in community until their history is crawled, and the crawl pauses when the volume's free space falls under [full] pause_free_percent."
            .to_string(),
    );
    out
}

/// The confirm page's lines beyond the disk and the writer: how labels
/// arrive, the last `full-check`, and the options set aside.
#[must_use]
pub fn more_warnings(file: &crate::config::FileConfig, last_check: Option<String>) -> Vec<String> {
    let full = &file.full;
    let mut out = Vec::new();
    if full.labels == crate::config::FullLabels::Subscribe && !full.label_sources.is_empty() {
        out.push(if full.label_backfill {
            "Each subscribed labeler's history is replayed from its first label ([full] label_backfill). For Bluesky's moderation labeler that takes hours, at about 700 labels a second, and several GB, at about 770 bytes a label; until it ends, that labeler's labels are read from it per request.".to_string()
        } else {
            "Labelers are subscribed from now on ([full] label_backfill is off): labels of older posts and of accounts are read from each labeler per request, cached.".to_string()
        });
    }
    out.push(last_check.unwrap_or_else(|| "No full-check has been run on this database: `admin indexing full-check` advises how many writer lanes this machine needs.".to_string()));
    out.extend(set_aside(file));
    out
}

/// The Postgres settings full's warnings read, never write.
pub const POSTGRES_SETTINGS: [&str; 6] = ["shared_buffers", "synchronous_commit", "max_wal_size", "checkpoint_timeout", "effective_io_concurrency", "random_page_cost"];

/// One `pg_settings` row: its name, value and unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgSetting {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
}

impl PgSetting {
    /// The value in bytes or seconds, for a numeric setting.
    fn base(&self) -> Option<f64> {
        let v: f64 = self.setting.parse().ok()?;
        let scale = match self.unit.as_deref() {
            None | Some("") | Some("s") => 1.0,
            Some("8kB") => 8192.0,
            Some("kB") => 1024.0,
            Some("MB") => 1024.0 * 1024.0,
            Some("ms") => 0.001,
            Some("min") => 60.0,
            Some(_) => return None,
        };
        Some(v * scale)
    }
}

/// Lines for the Postgres settings that are at Postgres's default (or
/// worse for full), with advice in words. Prism never changes them; the
/// operator tunes them for the machine.
#[must_use]
pub fn settings_warnings(settings: &[PgSetting]) -> Vec<String> {
    const MB: f64 = 1024.0 * 1024.0;
    let get = |name: &str| settings.iter().find(|s| s.name == name);
    let mut out = Vec::new();
    if let Some(v) = get("shared_buffers").and_then(PgSetting::base)
        && v <= 128.0 * MB
    {
        out.push(format!("Postgres's shared_buffers is {:.0} MB, Postgres's default; for full, a quarter of the machine's memory is usual.", v / MB));
    }
    if let Some(v) = get("max_wal_size").and_then(PgSetting::base)
        && v <= 1024.0 * MB
    {
        out.push(format!("Postgres's max_wal_size is {:.0} MB, Postgres's default; full writes WAL fast, and 4 to 16 GB spaces checkpoints out.", v / MB));
    }
    if let Some(v) = get("checkpoint_timeout").and_then(PgSetting::base)
        && v <= 300.0
    {
        out.push(format!("Postgres's checkpoint_timeout is {:.0} minutes, Postgres's default; 15 to 30 minutes is usual for a write-heavy database.", v / 60.0));
    }
    if let Some(v) = get("effective_io_concurrency").and_then(PgSetting::base)
        && v <= 1.0
    {
        out.push("Postgres's effective_io_concurrency is 1, Postgres's default; on an SSD, 200 is usual.".to_string());
    }
    if let Some(v) = get("random_page_cost").and_then(PgSetting::base)
        && v >= 4.0
    {
        out.push("Postgres's random_page_cost is 4, Postgres's default for spinning disks; on an SSD, 1.1 is usual.".to_string());
    }
    if get("synchronous_commit").is_some_and(|s| s.setting == "off") {
        out.push("Postgres's synchronous_commit is off: a database crash can lose the last commits Prism acknowledged to the stream, which it does not read again.".to_string());
    }
    out
}

/// [`settings_warnings`] for the database `pool` reaches; none when the
/// settings cannot be read.
pub async fn postgres_warnings(pool: &sqlx::PgPool) -> Vec<String> {
    let names: Vec<String> = POSTGRES_SETTINGS.iter().map(|s| (*s).to_string()).collect();
    let rows: Result<Vec<(String, String, Option<String>)>, sqlx::Error> =
        sqlx::query_as("SELECT name, setting, unit FROM pg_settings WHERE name = ANY($1)").bind(&names).fetch_all(pool).await;
    match rows {
        Ok(rows) => settings_warnings(&rows.into_iter().map(|(name, setting, unit)| PgSetting { name, setting, unit }).collect::<Vec<_>>()),
        Err(error) => {
            tracing::debug!(event = "full_postgres_settings_unread", %error, "Postgres settings could not be read");
            Vec::new()
        }
    }
}

/// The live writer against the relays, over the last minutes, in words:
/// entries the relays added a second and entries Prism applied a second.
#[must_use]
pub fn rate_words(applied_per_sec: Option<f64>, added_per_sec: Option<f64>) -> Option<String> {
    let (applied, added) = (applied_per_sec?, added_per_sec?);
    let verdict = if added <= 0.0 {
        "the relays sent nothing"
    } else if applied >= added * 0.98 {
        "it keeps up"
    } else {
        "it falls behind"
    };
    Some(format!("Over the last 10 minutes the relays added about {added:.0} entries a second and Prism applied about {applied:.0} a second: {verdict}. Full indexing adds work to every entry."))
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

    /// Impossible combinations of full's options are set aside, each with
    /// its fallback in words, and the defaults set nothing aside.
    #[test]
    fn options_that_cannot_hold_are_set_aside_with_their_fallback() {
        use crate::config::{BlocksSource, FileConfig, FullLabels};
        let mut file = FileConfig::default();
        file.firehose.relay_labels.insert("wss://relay.example".into(), "bsky".into());
        assert!(set_aside(&file).is_empty(), "{:?}", set_aside(&file));
        let mut f = file.clone();
        f.full.kinds = vec![FullGroup::Posts, FullGroup::Blocks];
        f.full.blocks_source = BlocksSource::Local;
        f.full.labels = FullLabels::Subscribe;
        f.full.label_sources.clear();
        f.full.takedown_relays = vec!["nowhere".into()];
        let words = set_aside(&f).join(" | ");
        for needle in [
            "needs the blocks and lists groups",
            "with no Farsight set up only those apply",
            "Like counts come from Constellation: the likes group",
            "Follower counts come from Constellation",
            "read per request instead",
            "names nowhere, which is not a label",
            "names no configured relay",
        ] {
            assert!(words.contains(needle), "{needle}: {words}");
        }
        assert!(!words.contains("Reply and quote counts"), "posts is on");
        f.farsight.url = Some("https://farsight.example".into());
        f.retention.max_age_days = 30;
        let words = set_aside(&f).join(" | ");
        assert!(words.contains("read from Farsight") && words.contains("A history cutoff is set") && !words.contains("Like counts"), "{words}");
    }

    /// The confirm page's further lines: labels by `label_backfill`, the
    /// last full-check or its absence, and the writer against the relays.
    #[test]
    fn more_warnings_name_labels_the_last_check_and_the_rate() {
        let mut file = crate::config::FileConfig::default();
        file.firehose.relay_labels.insert("wss://relay.example".into(), "bsky".into());
        let w = more_warnings(&file, None).join(" | ");
        assert!(w.contains("subscribed from now on") && w.contains("No full-check has been run"), "{w}");
        file.full.label_backfill = true;
        let w = more_warnings(&file, Some("Last full-check x.".into())).join(" | ");
        assert!(w.contains("replayed from its first label") && w.contains("Last full-check x."), "{w}");
        assert!(rate_words(Some(400.0), Some(480.0)).is_some_and(|r| r.contains("about 480 entries a second") && r.contains("falls behind")));
        assert!(rate_words(Some(480.0), Some(480.0)).is_some_and(|r| r.contains("keeps up")));
        assert_eq!(rate_words(None, Some(1.0)), None);
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

    #[test]
    fn postgres_settings_at_their_defaults_are_named() {
        let s = |name: &str, setting: &str, unit: Option<&str>| PgSetting { name: name.into(), setting: setting.into(), unit: unit.map(str::to_string) };
        // Postgres 16's defaults as `pg_settings` reports them.
        let defaults = [
            s("shared_buffers", "16384", Some("8kB")),
            s("synchronous_commit", "on", None),
            s("max_wal_size", "1024", Some("MB")),
            s("checkpoint_timeout", "300", Some("s")),
            s("effective_io_concurrency", "1", None),
            s("random_page_cost", "4", None),
        ];
        let w = settings_warnings(&defaults);
        assert_eq!(w.len(), 5, "{w:?}");
        assert!(w[0].contains("shared_buffers is 128 MB, Postgres's default; for full, a quarter of the machine's memory is usual"), "{w:?}");
        assert!(w.iter().any(|l| l.contains("max_wal_size is 1024 MB")));
        assert!(w.iter().any(|l| l.contains("checkpoint_timeout is 5 minutes")));
        // Tuned values are not named; synchronous_commit only when off.
        let tuned = [
            s("shared_buffers", "262144", Some("8kB")),
            s("synchronous_commit", "off", None),
            s("max_wal_size", "8192", Some("MB")),
            s("checkpoint_timeout", "900", Some("s")),
            s("effective_io_concurrency", "200", None),
            s("random_page_cost", "1.1", None),
        ];
        let w = settings_warnings(&tuned);
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].contains("synchronous_commit is off"));
        assert!(settings_warnings(&[]).is_empty());
    }
}
