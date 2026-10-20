//! Stats (admin-ui design §10): the disks Prism's data lives on, the other
//! storage figures, and how much is indexed. Endpoint usage is `usage`.
//!
//! **Disks (§10.1).** `statvfs` of three paths: the app's data directory
//! (the `app-data` volume; named volumes of the default driver share its
//! filesystem, so it stands for the host disk) and the read-only mounts of
//! the Postgres and Redis volumes that the shipped compose adds (C-A5 (a)).
//! `statvfs` reads only the filesystem's counters and opens no file, so the
//! mounts give the app no access to the data. A path that is not there is
//! "not visible to Prism". Paths on one filesystem are shown once. Disks are
//! read when a page asks, never kept.
//!
//! **Counts (§10.2).** Small tables are counted exactly; the scope sets are
//! read in memory; everything else comes from the storage sampler's
//! per-table series, which are Postgres's own estimates. The private
//! tables' names and the media counts come from `private_tier::stats`, so
//! this module never names a private table.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use sqlx::PgPool;

use super::health::{Snapshot, parse};

/// Where the shipped compose mounts the Postgres volume, read-only.
pub const POSTGRES_MOUNT: &str = "/app/volumes/postgres";
/// Where the shipped compose mounts the Redis volume, read-only.
pub const REDIS_MOUNT: &str = "/app/volumes/redis";

/// The three paths the disk figures read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskPaths {
    /// The app's data directory (`APP_DATA_DIR`).
    pub data: PathBuf,
    pub postgres: PathBuf,
    pub redis: PathBuf,
}

impl DiskPaths {
    /// The data directory and the two standard mounts.
    #[must_use]
    pub fn standard(data_dir: &Path) -> Self {
        Self { data: data_dir.to_path_buf(), postgres: PathBuf::from(POSTGRES_MOUNT), redis: PathBuf::from(REDIS_MOUNT) }
    }
}

/// What lives on a disk, in words.
pub const HOLDS_DATA: &str = "Prism's own files";
pub const HOLDS_POSTGRES: &str = "the database (Postgres)";
pub const HOLDS_REDIS: &str = "Redis";

/// One filesystem's space, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Space {
    pub total: u64,
    pub used: u64,
    /// Free for an ordinary user (`f_bavail`), as `df` shows it.
    pub free: u64,
    /// The filesystem's identity: `f_fsid` with its size, so two paths on
    /// one filesystem are shown once.
    pub id: (u64, u64, u64),
}

impl Space {
    /// The share in use, as `df` reckons it: used over used plus free.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn used_ratio(&self) -> f64 {
        let seen = self.used + self.free;
        if seen == 0 { 0.0 } else { self.used as f64 / seen as f64 }
    }
}

/// `statvfs` of `path`; `None` when the path is not there or not readable.
#[must_use]
pub fn space(path: &Path) -> Option<Space> {
    let s = rustix::fs::statvfs(path).ok()?;
    let unit = s.f_frsize.max(1);
    Some(Space {
        total: s.f_blocks.saturating_mul(unit),
        used: s.f_blocks.saturating_sub(s.f_bfree).saturating_mul(unit),
        free: s.f_bavail.saturating_mul(unit),
        id: (s.f_fsid, s.f_blocks, unit),
    })
}

/// A disk and what lives on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disk {
    pub holds: Vec<&'static str>,
    pub space: Space,
}

impl Disk {
    /// What it holds, in one phrase.
    #[must_use]
    pub fn holds_words(&self) -> String {
        join_words(&self.holds)
    }
}

/// "a", "a and b", "a, b and c".
#[must_use]
pub fn join_words(parts: &[&str]) -> String {
    match parts {
        [] => String::new(),
        [one] => (*one).to_string(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// The disks, each once, and what could not be seen.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Disks {
    pub disks: Vec<Disk>,
    pub not_visible: Vec<&'static str>,
}

impl Disks {
    /// The disk with the least room, by share in use.
    #[must_use]
    pub fn fullest(&self) -> Option<&Disk> {
        self.disks.iter().max_by(|a, b| a.space.used_ratio().total_cmp(&b.space.used_ratio()))
    }
}

/// Read the three paths now.
#[must_use]
pub fn read_disks(paths: &DiskPaths) -> Disks {
    let mut out = Disks::default();
    for (path, holds) in [(&paths.data, HOLDS_DATA), (&paths.postgres, HOLDS_POSTGRES), (&paths.redis, HOLDS_REDIS)] {
        match space(path) {
            None => out.not_visible.push(holds),
            Some(s) => match out.disks.iter_mut().find(|d| d.space.id == s.id) {
                Some(d) => d.holds.push(holds),
                None => out.disks.push(Disk { holds: vec![holds], space: s }),
            },
        }
    }
    out
}

/// The write-ahead log's size: `pg_ls_waldir()` needs `pg_monitor` or a
/// superuser, so `None` when it is refused or slow.
pub async fn wal_bytes(pool: &PgPool) -> Option<i64> {
    let run = async {
        let mut tx = pool.begin().await?;
        sqlx::query("SET LOCAL statement_timeout = '2s'").execute(&mut *tx).await?;
        let n: Option<i64> = sqlx::query_scalar("SELECT sum(size)::bigint FROM pg_ls_waldir()").fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(n)
    };
    match run.await {
        Ok(n) => n,
        Err(error) => {
            tracing::debug!(event = "admin_stats_wal_unknown", %error, "the write-ahead log's size could not be read");
            None
        }
    }
}

/// `aof_current_size` of `INFO persistence`; `None` when append-only
/// saving is off or the field is absent.
#[must_use]
pub fn parse_aof_size(info: &str) -> Option<u64> {
    let mut enabled = false;
    let mut size = None;
    for line in info.lines() {
        match line.trim().split_once(':') {
            Some(("aof_enabled", v)) => enabled = v.trim() == "1",
            Some(("aof_current_size", v)) => size = v.trim().parse().ok(),
            _ => {}
        }
    }
    if enabled { size } else { None }
}

/// The Redis append-only file's size, asked now.
pub async fn aof_bytes(redis: Option<&ConnectionManager>) -> Option<u64> {
    let mut conn = redis?.clone();
    let info = tokio::time::timeout(Duration::from_secs(2), redis::cmd("INFO").arg("persistence").query_async::<String>(&mut conn)).await;
    match info {
        Ok(Ok(text)) => parse_aof_size(&text),
        _ => None,
    }
}

/// The series the stats page reads besides the health ring's. Each is
/// emitted somewhere in `src/` (test T-A13).
pub const STATS_SERIES: &[&str] = &[
    crate::db::sampler::METRIC_TABLE_ROWS,
    crate::db::sampler::METRIC_TABLE_BYTES,
    crate::db::sampler::METRIC_DB_SIZE,
    crate::db::sampler::METRIC_REDIS_USED,
    crate::db::sampler::METRIC_REDIS_MAX,
    crate::aggregations::rebuild::METRIC_SCOPE_BY_HOLD,
    crate::identity::lodestar::METRIC_CALLS,
    crate::identity::lodestar::METRIC_SKIPPED,
    super::health::PRIVATE_READ_CONTENT,
];

/// The stats series in `text`, as of now.
#[must_use]
pub fn series(text: &str) -> Snapshot {
    Snapshot { at: Utc::now(), series: parse(text, |n| STATS_SERIES.contains(&n)) }
}

/// The public tables the page names, with their names in words; the
/// first eight are the headline counts (§10.2).
pub const PUBLIC_TABLES: &[(&str, &str)] = &[
    ("posts", "Posts"),
    ("likes", "Likes"),
    ("reposts", "Reposts"),
    ("follows", "Follows"),
    ("blocks", "Blocks"),
    ("lists", "Lists"),
    ("notifications", "Notifications"),
    ("bookmarks", "Bookmarks"),
    ("actors", "Profiles"),
    ("list_items", "List members"),
    ("list_blocks", "Blocks of whole lists"),
    ("feed_items", "Timeline entries"),
    ("starter_packs", "Starter packs"),
    ("threadgates", "Who may reply settings"),
    ("postgates", "Who may quote settings"),
    ("labels", "Labels"),
    ("drafts", "Drafts"),
    ("scope_dids", "Accounts Prism follows"),
    ("members", "Members"),
    ("backfill_jobs", "Sync jobs"),
    ("record_tombstones", "Deleted records remembered"),
];

/// How many of [`PUBLIC_TABLES`] are headline counts.
pub const HEADLINE: usize = 8;

/// One table on the page.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFigure {
    /// Its name in words, or the table's own name when it has none.
    pub label: String,
    pub table: String,
    /// About this many rows (Postgres's estimate).
    pub rows: f64,
    pub bytes: Option<f64>,
}

/// A table's name in words: the private tier's label, a public one, or
/// `None`.
#[must_use]
pub fn table_label(table: &str) -> Option<&'static str> {
    crate::private_tier::stats::admin_table_labels()
        .iter()
        .chain(PUBLIC_TABLES)
        .find(|(t, _)| *t == table)
        .map(|(_, l)| *l)
}

/// Every table the sampler reported, largest first.
#[must_use]
pub fn tables(s: &Snapshot) -> Vec<TableFigure> {
    let bytes = s.by(crate::db::sampler::METRIC_TABLE_BYTES, "table");
    let mut out: Vec<TableFigure> = s
        .by(crate::db::sampler::METRIC_TABLE_ROWS, "table")
        .into_iter()
        .map(|(table, rows)| TableFigure {
            label: table_label(&table).map_or_else(|| table.clone(), str::to_string),
            bytes: bytes.iter().find(|(t, _)| *t == table).map(|(_, b)| *b),
            table,
            rows,
        })
        .collect();
    out.sort_by(|a, b| b.bytes.unwrap_or(0.0).total_cmp(&a.bytes.unwrap_or(0.0)).then_with(|| a.table.cmp(&b.table)));
    out
}

/// The scope sets' sizes, read in memory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScopeSizes {
    pub scope: usize,
    pub tracked: usize,
    pub owners: usize,
}

/// Read them now.
pub async fn scope_sizes(scope: &crate::aggregations::ScopeState) -> ScopeSizes {
    let sets = scope.read_guard().await;
    let mut tracked = sets.members.clone();
    tracked.extend(sets.private_owners.iter().cloned());
    ScopeSizes { scope: sets.scope.len(), tracked: tracked.len(), owners: sets.private_owners.len() }
}

/// When the sampler last measured the tables in this process.
#[must_use]
pub fn sampled_at() -> Option<DateTime<Utc>> {
    crate::db::sampler::last_db_sample()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disks_on_one_filesystem_are_shown_once_and_missing_ones_named() {
        let dir = std::env::temp_dir();
        let paths = DiskPaths { data: dir.clone(), postgres: dir.join("."), redis: PathBuf::from("/nonexistent/hprs-redis-volume") };
        let d = read_disks(&paths);
        assert_eq!(d.disks.len(), 1, "{d:?}");
        assert_eq!(d.disks[0].holds, vec![HOLDS_DATA, HOLDS_POSTGRES]);
        assert_eq!(d.disks[0].holds_words(), "Prism's own files and the database (Postgres)");
        assert_eq!(d.not_visible, vec![HOLDS_REDIS]);
        let s = d.disks[0].space;
        assert!(s.total > 0 && s.used <= s.total && s.free <= s.total, "{s:?}");
        assert_eq!(d.fullest().map(|x| x.holds.len()), Some(2));
        assert_eq!(join_words(&["a", "b", "c"]), "a, b and c");
    }

    #[test]
    fn aof_size_needs_aof_on() {
        assert_eq!(parse_aof_size("# Persistence\r\naof_enabled:1\r\naof_current_size:12345\r\n"), Some(12_345));
        assert_eq!(parse_aof_size("aof_enabled:0\r\naof_current_size:0\r\n"), None);
        assert_eq!(parse_aof_size("aof_enabled:1\r\n"), None);
    }

    #[test]
    fn tables_are_labelled_and_largest_first() {
        let text = "db_table_rows{table=\"posts\"} 10\ndb_table_bytes{table=\"posts\"} 100\n\
            db_table_rows{table=\"worker_state\"} 3\ndb_table_bytes{table=\"worker_state\"} 900\n";
        let t = tables(&series(text));
        assert_eq!(t.iter().map(|f| f.label.as_str()).collect::<Vec<_>>(), ["worker_state", "Posts"]);
        assert_eq!(t[1].rows, 10.0);
        let private = crate::private_tier::stats::admin_posts_table();
        assert_eq!(table_label(private), Some("Private posts"));
    }
}
