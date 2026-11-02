//! Storage (`/admin/storage`): the database against its budget, its change
//! log, Redis, the account ids, the disks, and the stored records still to
//! compress with the dead rows compression leaves; then its settings: the
//! budget, what is kept and for how long, with the rarer ones folded.
//! Nothing but the settings changes anything, and nothing counts a large
//! table.

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use super::health::{Figure, database_words};
use super::{Nav, bytes, css, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::{self, Snapshot, count};
use crate::admin::menu::PageId;
use crate::admin::pages::settings::OpenQuery;
use crate::admin::settings::view::{self, Place, SettingsBlock};
use crate::admin::stats::{self, Disks};
use crate::db::schema_backfill as sb;

const UNKNOWN: &str = "unknown";

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into(), link: None }
}

/// A disk as the page shows it.
pub struct DiskView {
    pub holds: String,
    pub total: String,
    pub used: String,
    pub free: String,
    pub used_share: String,
    /// `fine`, `amber` or `red`, as the Disk check judges it.
    pub class: &'static str,
}

#[derive(Template)]
#[template(path = "storage.html")]
struct StoragePage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    /// The database, its change log, Redis and the account ids.
    space: Vec<Figure>,
    disks: Vec<DiskView>,
    /// What Prism cannot see the disk of, in words; empty when it sees all.
    not_visible: String,
    /// Records still to compress, and the dead rows left behind.
    records: Vec<Figure>,
    /// The budget, what is kept and for how long.
    settings: SettingsBlock,
}

/// The disks in words, each judged as the Disk check does.
#[allow(clippy::cast_precision_loss)]
fn disk_views(d: &Disks) -> Vec<DiskView> {
    use crate::admin::health::{DISK_AMBER_BYTES, DISK_AMBER_FREE, DISK_RED_FREE};
    d.disks
        .iter()
        .map(|disk| {
            let s = disk.space;
            let free_share = 1.0 - s.used_ratio();
            let class = if free_share < DISK_RED_FREE {
                "red"
            } else if free_share < DISK_AMBER_FREE || s.free < DISK_AMBER_BYTES {
                "amber"
            } else {
                "fine"
            };
            DiskView {
                holds: disk.holds_words(),
                total: bytes(s.total as f64),
                used: bytes(s.used as f64),
                free: bytes(s.free as f64),
                used_share: format!("{:.0}% used", s.used_ratio() * 100.0),
                class,
            }
        })
        .collect()
}

fn or_unknown(v: Option<f64>) -> String {
    v.map_or_else(|| UNKNOWN.to_string(), count)
}

/// Redis's memory against its limit.
#[allow(clippy::cast_precision_loss)]
fn redis_words(s: &Snapshot) -> String {
    match (s.sum(crate::db::sampler::METRIC_REDIS_USED), s.sum(crate::db::sampler::METRIC_REDIS_MAX)) {
        (Some(used), Some(max)) if max > 0.0 => format!("{} of {} ({:.0}%)", bytes(used), bytes(max), used / max * 100.0),
        (Some(used), _) => format!("{} (no limit set)", bytes(used)),
        _ => UNKNOWN.to_string(),
    }
}

/// `GET /admin/storage`.
#[allow(clippy::cast_precision_loss)]
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(PageId::Storage), &state.settings.networks());
    let settings = match view::settings_block(&state, &op.csrf, &sections, q.open.as_deref() == Some("advanced")).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    let s = state.health.metrics.as_ref().map_or_else(|| stats::series(""), |m| stats::series(&m.render()));
    let (wal, aof, account_ids, dead) = tokio::join!(
        stats::wal_bytes(&state.pool),
        stats::aof_bytes(state.health.redis.as_ref()),
        health::timed_count(&state.pool, health::ACCOUNT_IDS_SQL),
        health::record_dead_rows(&state.pool),
    );
    let disks = stats::read_disks(&state.disks);
    let ids = account_ids.map_or_else(
        || UNKNOWN.to_string(),
        |n| format!("{} of {} ({})", count(n as f64), count(health::ACCOUNT_ID_LIMIT as f64), health::id_share(n)),
    );
    let space = vec![
        fig("Database (of its budget)", database_words(&state, Some(&s))),
        fig("Database change log (write-ahead log)", wal.map_or_else(|| "unknown (Prism's database account may not read it)".to_string(), |n| bytes(n as f64))),
        fig("Redis memory (of its limit)", redis_words(&s)),
        fig("Redis copy on disk (append-only file)", aof.map_or_else(|| UNKNOWN.to_string(), |n| bytes(n as f64))),
        fig("Account ids used", ids),
    ];
    let get = |name: &str| s.sum(name);
    let records = vec![
        fig("Post records left to compress", or_unknown(get(sb::METRIC_RECOMPRESS_REMAINING))),
        fig("Profile records left to compress", or_unknown(get(sb::METRIC_PROFILE_RECOMPRESS_REMAINING))),
        fig("Other records left to compress", or_unknown(get(sb::METRIC_OTHER_RECOMPRESS_REMAINING))),
        fig("Dead rows in posts", or_unknown(dead.map(|d| d.posts as f64).or_else(|| get(sb::METRIC_POSTS_DEAD_ROWS)))),
        fig("Dead rows in profiles", or_unknown(dead.map(|d| d.profiles as f64))),
        fig("Dead rows in other records", or_unknown(dead.map(|d| d.other as f64))),
    ];
    render(
        StatusCode::OK,
        &StoragePage {
            css: css(),
            nav: Some(Nav::page(&op, PageId::Storage)),
            title: PageId::Storage.page().name.to_string(),
            space,
            disks: disk_views(&disks),
            not_visible: stats::join_words(&disks.not_visible),
            records,
            settings,
        },
    )
}
