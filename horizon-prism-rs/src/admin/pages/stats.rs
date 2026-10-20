//! Stats (`/admin/stats`, admin-ui design §6.5, §10): the disks, the other
//! storage figures, how much is indexed, and the endpoints in use since
//! Prism started. Nothing here changes anything, and nothing counts a large
//! table: big tables are Postgres's own estimates from the storage sampler,
//! marked "about".

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use super::health::{Figure, FigureGroup, database_words};
use super::{Nav, ago, bytes, css, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::{PRIVATE_READ_CONTENT, Snapshot, count, timed_count};
use crate::admin::stats::{self, Disks, PUBLIC_TABLES, HEADLINE};
use crate::admin::usage::{self, Class};
use crate::firehose::feed::{Kind, Outcome, Source, Totals};

const UNKNOWN: &str = "unknown";

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into() }
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

/// A table's size.
pub struct TableRow {
    pub label: String,
    pub table: String,
    pub rows: String,
    pub size: String,
}

/// A method and its use.
pub struct EndpointRow {
    pub name: String,
    pub nsid: String,
    pub total: String,
    pub counts: Vec<String>,
    pub last: String,
}

/// A method never used since the start.
pub struct NeverRow {
    pub name: String,
    pub nsid: String,
}

#[derive(Template)]
#[template(path = "stats.html")]
struct StatsPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    disks: Vec<DiskView>,
    /// What Prism cannot see the disk of, in words; empty when it sees all.
    not_visible: String,
    storage: Vec<Figure>,
    groups: Vec<FigureGroup>,
    /// When the sampler last measured the tables.
    as_of: String,
    tables: Vec<TableRow>,
    /// When endpoint counting began.
    counting_since: String,
    used: Vec<EndpointRow>,
    never: Vec<NeverRow>,
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

fn about(v: Option<f64>) -> String {
    v.map_or_else(|| UNKNOWN.to_string(), |n| format!("about {}", count(n)))
}

fn exact(v: Option<i64>) -> String {
    #[allow(clippy::cast_precision_loss)]
    v.map_or_else(|| UNKNOWN.to_string(), |n| count(n as f64))
}

/// The rows of `table` the sampler last reported.
fn rows_of(s: &Snapshot, table: &str) -> Option<f64> {
    s.sum_where(crate::db::sampler::METRIC_TABLE_ROWS, "table", table)
}

/// Kept and refused blocks since the start, from the live feed's totals:
/// every block kind, every source.
fn blocks_since_start(t: &Totals) -> (u64, u64) {
    let kinds = [Kind::Block, Kind::ListBlock, Kind::PrivateBlock];
    let sum = |outcomes: &[Outcome]| -> u64 {
        Source::ALL.iter().flat_map(|s| kinds.iter().flat_map(move |k| outcomes.iter().map(move |o| t.get(*s, *k, *o)))).sum()
    };
    (sum(&[Outcome::Written, Outcome::ReferenceOnly]), sum(&[Outcome::NoMemberSide]))
}

/// The scope by its first hold, as of the last nightly clean-up.
fn holds(s: &Snapshot) -> Vec<Figure> {
    const HOLDS: [(&str, &str); 5] = [
        ("member", "Members themselves"),
        ("follow", "Followed by a member"),
        ("interaction", "A member replied, quoted, liked or reposted lately"),
        ("subscription", "On a list a member subscribes to"),
        ("none", "Kept for now (waiting period or the clean-up limit)"),
    ];
    HOLDS
        .iter()
        .map(|(hold, label)| {
            let v = s.sum_where(crate::aggregations::rebuild::METRIC_SCOPE_BY_HOLD, "hold", hold);
            fig(label, v.map_or_else(|| "not measured yet".to_string(), count))
        })
        .collect()
}

/// Lodestar's calls since the start, in three words.
fn lodestar(state: &AdminState, s: &Snapshot) -> FigureGroup {
    use crate::identity::lodestar::{METRIC_CALLS, METRIC_SKIPPED};
    let figures = if let Some(l) = &state.settings.lodestar {
        let answered: f64 = ["answered", "not_found", "tombstoned"].iter().filter_map(|o| s.sum_where(METRIC_CALLS, "outcome", o)).sum();
        let all = s.sum(METRIC_CALLS).unwrap_or(0.0);
        vec![
            fig("Server", l.host().to_string()),
            fig("Answered by lodestar", count(answered)),
            fig("Not answered (Prism asked the PLC directory)", count((all - answered).max(0.0))),
            fig("Not asked (lodestar behind, or over its rate)", count(s.sum(METRIC_SKIPPED).unwrap_or(0.0))),
        ]
    } else {
        vec![fig("Lodestar", "not set up; Prism asks the PLC directory")]
    };
    FigureGroup { title: "Account lookups (lodestar)", figures, note: "Counted since Prism started.".to_string() }
}

fn endpoint_rows(rows: Vec<usage::Row>) -> (Vec<EndpointRow>, Vec<NeverRow>) {
    let (mut used, never): (Vec<usage::Row>, Vec<usage::Row>) = rows.into_iter().partition(|r| r.total > 0);
    used.sort_by(|a, b| b.total.cmp(&a.total).then_with(|| a.nsid.cmp(&b.nsid)));
    #[allow(clippy::cast_precision_loss)]
    let n = |v: u64| count(v as f64);
    let used = used
        .into_iter()
        .map(|r| EndpointRow {
            name: usage::plain_name(&r.nsid),
            total: n(r.total),
            counts: Class::ALL.iter().map(|c| n(r.by_class[*c as usize])).collect(),
            last: r.last.map_or_else(|| "—".to_string(), ago),
            nsid: r.nsid,
        })
        .collect();
    let never = never.into_iter().filter(|r| r.registered).map(|r| NeverRow { name: usage::plain_name(&r.nsid), nsid: r.nsid }).collect();
    (used, never)
}

/// `GET /admin/stats`.
#[allow(clippy::too_many_lines)]
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let s = state.health.metrics.as_ref().map_or_else(|| stats::series(""), |m| stats::series(&m.render()));
    let (members, ready, denied, wal, aof, media, sizes) = tokio::join!(
        timed_count(&state.pool, "SELECT count(*) FROM members"),
        timed_count(&state.pool, "SELECT count(*) FROM members WHERE ready_at IS NOT NULL"),
        timed_count(&state.pool, "SELECT count(*) FROM member_denials"),
        stats::wal_bytes(&state.pool),
        stats::aof_bytes(state.health.redis.as_ref()),
        crate::private_tier::stats::media_post_counts(&state.pool),
        stats::scope_sizes(&state.scope),
    );
    let disks = stats::read_disks(&state.disks);

    #[allow(clippy::cast_precision_loss)]
    let redis = match (s.sum(crate::db::sampler::METRIC_REDIS_USED), s.sum(crate::db::sampler::METRIC_REDIS_MAX)) {
        (Some(used), Some(max)) if max > 0.0 => format!("{} of {} ({:.0}%)", bytes(used), bytes(max), used / max * 100.0),
        (Some(used), _) => format!("{} (no limit set)", bytes(used)),
        _ => UNKNOWN.to_string(),
    };
    #[allow(clippy::cast_precision_loss)]
    let storage = vec![
        fig("Database (of its budget)", database_words(&state, Some(&s))),
        fig(
            "Database change log (write-ahead log)",
            wal.map_or_else(|| "unknown (Prism's database account may not read it)".to_string(), |n| bytes(n as f64)),
        ),
        fig("Redis memory (of its limit)", redis),
        fig("Redis copy on disk (append-only file)", aof.map_or_else(|| UNKNOWN.to_string(), |n| bytes(n as f64))),
    ];

    let mut groups = vec![FigureGroup {
        title: "People",
        figures: vec![
            fig("Members", exact(members)),
            fig("Members ready (first sync done)", exact(ready)),
            fig("Denied accounts", exact(denied)),
            fig("Accounts with private posts", count_usize(sizes.owners)),
            fig("Accounts kept fully in sync (members and private posters)", count_usize(sizes.tracked)),
        ],
        note: "Exact counts.".to_string(),
    }];

    let mut scope = vec![fig("Accounts Prism follows", count_usize(sizes.scope))];
    scope.extend(holds(&s));
    groups.push(FigureGroup {
        title: "Accounts Prism follows",
        figures: scope,
        note: "The members and the accounts around them. Each is counted under its first reason; the reasons are measured at the nightly clean-up.".to_string(),
    });

    groups.push(FigureGroup {
        title: "Records",
        figures: PUBLIC_TABLES[..HEADLINE].iter().chain(PUBLIC_TABLES.iter().filter(|(t, _)| *t == "actors")).map(|(t, l)| fig(l, about(rows_of(&s, t)))).collect(),
        note: String::new(),
    });

    let (images, video) = match media {
        Ok((i, v)) => (exact(Some(i)), exact(Some(v))),
        Err(error) => {
            tracing::warn!(event = "admin_stats_media_failed", %error, "the private media counts did not finish");
            (UNKNOWN.to_string(), UNKNOWN.to_string())
        }
    };
    let form = |f: &str| s.sum_where(PRIVATE_READ_CONTENT, "form", f).map_or_else(|| "none yet".to_string(), count);
    groups.push(FigureGroup {
        title: "Private posts",
        figures: vec![
            fig("Stored", about(rows_of(&s, crate::private_tier::stats::admin_posts_table()))),
            fig("With images", images),
            fig("With a video", video),
            fig("Read in the new form since Prism started", form("content")),
            fig("Read in the older form since Prism started", form("text")),
        ],
        note: "Images and video are exact. A post stored before Prism recorded media counts as having none until its server sends it again.".to_string(),
    });

    let (kept, refused) = blocks_since_start(&state.live.tap().totals());
    #[allow(clippy::cast_precision_loss)]
    let blocks = vec![
        fig("Kept since Prism started (a member is involved)", count(kept as f64)),
        fig("Not kept since Prism started (no member involved)", count(refused as f64)),
        fig("Blocks stored", about(rows_of(&s, "blocks"))),
        fig("Blocks of whole lists stored", about(rows_of(&s, "list_blocks"))),
        fig("Private blocks stored", about(rows_of(&s, crate::private_tier::stats::admin_blocks_table()))),
    ];
    groups.push(FigureGroup { title: "Blocks", figures: blocks, note: "Prism keeps only blocks made by or against a member.".to_string() });
    groups.push(lodestar(&state, &s));

    #[allow(clippy::cast_precision_loss)]
    let tables = stats::tables(&s)
        .into_iter()
        .map(|t| TableRow {
            rows: format!("about {}", count(t.rows)),
            size: t.bytes.map_or_else(|| UNKNOWN.to_string(), bytes),
            label: t.label,
            table: t.table,
        })
        .collect();
    let as_of = stats::sampled_at().map_or_else(|| "not measured yet since Prism started (every 5 minutes)".to_string(), |t| format!("{} ({})", when(t), ago(t)));

    let started = state.usage.started();
    let (used, never) = endpoint_rows(state.usage.rows());
    render(
        StatusCode::OK,
        &StatsPage {
            css: css(),
            nav: Some(Nav::new(&op, "stats")),
            title: "Stats".to_string(),
            disks: disk_views(&disks),
            not_visible: stats::join_words(&disks.not_visible),
            storage,
            groups,
            as_of,
            tables,
            counting_since: format!("{} ({})", when(started), ago(started)),
            used,
            never,
        },
    )
}

#[allow(clippy::cast_precision_loss)]
fn count_usize(n: usize) -> String {
    count(n as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_rows_split_used_and_never() {
        let row = |nsid: &str, total: u64, registered: bool| usage::Row {
            nsid: nsid.to_string(),
            by_class: [total, 0, 0, 0, 0],
            total,
            last: (total > 0).then(chrono::Utc::now),
            registered,
        };
        let (used, never) = endpoint_rows(vec![row("a.b.getOne", 3, true), row("a.b.getTwo", 0, true), row("a.b.getMany", 9, false), row("a.b.gone", 0, false)]);
        assert_eq!(used.iter().map(|r| r.nsid.as_str()).collect::<Vec<_>>(), ["a.b.getMany", "a.b.getOne"]);
        assert_eq!(used[1].counts[0], "3");
        assert_eq!(used[1].last, "just now");
        assert_eq!(never.iter().map(|r| r.nsid.as_str()).collect::<Vec<_>>(), ["a.b.getTwo"]);
    }
}
