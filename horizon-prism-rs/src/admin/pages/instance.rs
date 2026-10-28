//! Instance (`/admin/instance`): everything about this instance on one page
//! for its operators. What it is and its version; its status, with each
//! relay by label and host, the input rate, account lookups, block lists
//! and notices; what it does here (public data, the labels it applies, the
//! refracted communities, the moderation policy link); the methods it
//! serves and the custom lexicons; and a few figures. The public page's
//! content (public-page design §2), with names, hosts and exact figures,
//! and without its live feed.
//!
//! No query runs per request but the two pings Health also runs: the rest
//! is one render of the in-process metrics, the Health ring, the settings
//! and the compiled-in lists.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use askama::Template;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, TimeDelta, Utc};

use super::health::{Figure, farsight_words, lookups_words};
use super::settings::phase_words;
use super::{Nav, ago, css, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::{self, Check, Level, Ring, Snapshot, count, parse};
use crate::admin::lexicons::{self, Lexicon};
use crate::admin::usage;
use crate::config::{FileConfig, RefractCommunity};
use crate::indexing::PublicMode;

const UNKNOWN: &str = "unknown";
const FIVE_MIN: Duration = Duration::from_secs(300);

/// What Horizon-Prism is, the same for every operator.
pub const ABOUT: &str = "Horizon-Prism, an AT Protocol AppView written in Rust.";
/// What this instance does.
pub const DOES: &str = "It reads the AT Protocol network through its relays and answers the apps of its community, with private posts for its members. \
     It never writes to anyone's account: apps write to their own server.";

/// The series the page reads from one render of the metrics.
const SERIES: &[&str] = &[
    crate::firehose::watermark::METRIC_DELAY,
    crate::firehose::watermark::METRIC_DEGRADED,
    crate::firehose::consumer::METRIC_LAG,
    crate::db::sampler::METRIC_TABLE_ROWS,
    crate::db::sampler::METRIC_SCOPE_DIDS,
    crate::db::sampler::METRIC_MEMBERS,
];

/// A notice above the status.
pub struct Notice {
    /// `bad` or empty.
    pub class: &'static str,
    pub text: String,
}

/// A check shown as Health shows it.
pub struct StatusRow {
    pub name: &'static str,
    pub class: &'static str,
    pub word: &'static str,
    pub text: String,
}

impl From<Check> for StatusRow {
    fn from(c: Check) -> Self {
        Self { name: c.name, class: c.level.class(), word: c.level.word(), text: c.text }
    }
}

/// One relay.
pub struct RelayRow {
    pub label: String,
    /// The hosts of the URLs configured under this label.
    pub hosts: String,
    /// Private reads wait for it.
    pub private: bool,
    pub class: &'static str,
    pub behind: String,
    pub last_heard: String,
}

/// A labeler whose labels apply here.
pub struct LabelerRow {
    pub name: String,
    pub did: String,
    pub how: String,
    /// In use now: refract is on and the community enabled.
    pub active: bool,
}

/// A refracted community.
pub struct CommunityRow {
    pub name: String,
    pub tier: u8,
    pub patterns: Vec<String>,
    pub appview: String,
    pub enabled: bool,
}

/// The methods of one namespace.
pub struct MethodGroup {
    pub namespace: String,
    pub methods: Vec<String>,
}

/// A method answered "not offered here".
pub struct NotOffered {
    pub nsid: String,
    pub message: String,
}

/// Everything the page shows, built from state already in memory.
pub struct View {
    pub about: &'static str,
    pub does: &'static str,
    pub version: &'static str,
    pub hostname: String,
    pub service_did: String,
    pub started: String,
    pub notices: Vec<Notice>,
    pub status: Vec<StatusRow>,
    pub relays: Vec<RelayRow>,
    pub input_rate: String,
    pub lookups: String,
    pub plc: String,
    pub blocks: String,
    pub public_phase: String,
    pub public_text: String,
    pub refract_on: bool,
    pub labelers: Vec<LabelerRow>,
    pub communities: Vec<CommunityRow>,
    pub policy: String,
    pub methods: Vec<MethodGroup>,
    pub served: usize,
    pub not_offered: Vec<NotOffered>,
    /// The public methods answered 501 while public data is not on.
    pub public_off: Vec<String>,
    pub lexicons: Vec<Lexicon>,
    pub figures: Vec<Figure>,
}

#[derive(Template)]
#[template(path = "instance.html")]
struct InstancePage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    v: View,
}

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into() }
}

/// A URL's host, else the text itself.
fn host_of(url: &str) -> String {
    url::Url::parse(url).ok().and_then(|u| u.host_str().map(str::to_string)).unwrap_or_else(|| url.to_string())
}

/// Each relay label with the hosts configured under it, plus any label the
/// metric reports that the configuration does not name.
fn relay_rows(cfg: &FileConfig, now: Option<&Snapshot>, at: DateTime<Utc>) -> Vec<RelayRow> {
    let fh = &cfg.firehose;
    let mut hosts: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (url, label) in &fh.relay_labels {
        hosts.entry(label.clone()).or_default().push(host_of(url));
    }
    if let Some(s) = now {
        for (label, _) in s.by(crate::firehose::watermark::METRIC_DELAY, "relay") {
            hosts.entry(label).or_default();
        }
    }
    hosts
        .into_iter()
        .map(|(label, mut h)| {
            h.sort();
            h.dedup();
            let private = fh.private_relays.contains(&label);
            let delay = now.and_then(|s| s.sum_where(crate::firehose::watermark::METRIC_DELAY, "relay", &label));
            let (class, behind, last_heard) = match delay {
                None => (Level::NoData.class(), UNKNOWN.to_string(), UNKNOWN.to_string()),
                Some(d) if d.is_infinite() => (
                    if private { Level::Red.class() } else { Level::Amber.class() },
                    "never reported on this database".to_string(),
                    "never".to_string(),
                ),
                Some(d) if d.is_nan() => (Level::NoData.class(), UNKNOWN.to_string(), UNKNOWN.to_string()),
                Some(d) => {
                    let d = d.max(0.0);
                    #[allow(clippy::cast_possible_truncation)]
                    let heard = at - TimeDelta::milliseconds((d * 1000.0) as i64);
                    let (words, class) = if d <= 120.0 {
                        (format!("{d:.0} s behind"), Level::Ok.class())
                    } else {
                        (format!("{:.0} minutes behind", (d / 60.0).round()), Level::Amber.class())
                    };
                    (class, words, format!("{} ({})", when(heard), ago(heard)))
                }
            };
            RelayRow {
                hosts: if h.is_empty() { "not in the configuration".to_string() } else { h.join(", ") },
                label,
                private,
                class,
                behind,
                last_heard,
            }
        })
        .collect()
}

/// The relays' input: entries added to the stream per second over the ring's
/// last five minutes.
fn input_rate(ring: &Ring) -> String {
    ring.rate(FIVE_MIN, |s| s.sum(crate::firehose::consumer::METRIC_STREAM_ADDED))
        .map_or_else(|| "unknown (Prism needs two readings first, a minute apart)".to_string(), |r| format!("{r:.1} events a second, over the last 5 minutes"))
}

/// The public-data sentence (public-page design §2.3).
fn public_text(mode: PublicMode) -> &'static str {
    match mode {
        PublicMode::On => "Prism keeps public posts of its members and the accounts around them, and answers public requests.",
        PublicMode::Filling => "Public data is being turned on: Prism is refilling it, and answers public requests when that is done.",
        PublicMode::Off | PublicMode::Draining | PublicMode::Held => {
            "Public data is off: Prism keeps only what private posts need, and apps use their usual AppView for public reads."
        }
    }
}

fn labeler_rows(communities: &[RefractCommunity], refract_on: bool) -> Vec<LabelerRow> {
    communities
        .iter()
        .filter_map(|c| {
            let did = c.labeler_did.clone()?;
            Some(LabelerRow {
                name: c.name.clone(),
                did,
                how: c.label_source.map_or_else(|| "not set".to_string(), |s| format!("read through its {}", s.as_str())),
                active: refract_on && c.enabled,
            })
        })
        .collect()
}

fn community_rows(communities: &[RefractCommunity]) -> Vec<CommunityRow> {
    communities
        .iter()
        .map(|c| CommunityRow {
            name: c.name.clone(),
            tier: c.tier,
            patterns: c.pds_patterns.clone(),
            appview: c.appview_network.clone().unwrap_or_default(),
            enabled: c.enabled,
        })
        .collect()
}

/// Every registered method but the "not offered" ones, by namespace.
fn method_groups(registered: &[(String, &'static str)]) -> (Vec<MethodGroup>, usize) {
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut served = 0;
    for (nsid, router) in registered {
        if *router == "not_offered" {
            continue;
        }
        served += 1;
        let namespace = nsid.rsplit_once('.').map_or_else(|| "other".to_string(), |(ns, _)| ns.to_string());
        groups.entry(namespace).or_default().push(nsid.clone());
    }
    let groups = groups
        .into_iter()
        .map(|(namespace, mut methods)| {
            methods.sort();
            MethodGroup { namespace, methods }
        })
        .collect();
    (groups, served)
}

fn not_offered(cfg: &FileConfig) -> Vec<NotOffered> {
    crate::honest_501::DEFERRALS
        .iter()
        .filter(|d| crate::honest_501::registered(d, cfg))
        .map(|d| NotOffered { nsid: d.nsid.to_string(), message: crate::honest_501::message(d, cfg) })
        .collect()
}

/// The notices (public-page design §2.2), with their causes: each problem
/// in Health's own words, then the states that pass by themselves.
fn notices(checks: &[Check], i: &health::Inputs, now: Option<&Snapshot>, mode: PublicMode) -> Vec<Notice> {
    let schema = i.schema;
    let mut out: Vec<Notice> = checks
        .iter()
        .filter(|c| c.level == Level::Red && matches!(c.name, "Database" | "Redis" | "Audit guard" | "Stream reader" | "Held gap" | "Trim gap"))
        .map(|c| Notice { class: "bad", text: c.text.clone() })
        .collect();
    #[allow(clippy::cast_precision_loss)]
    let lagging = now.and_then(|s| s.sum(crate::firehose::consumer::METRIC_LAG)).is_some_and(|l| l > i.lag_warn_entries as f64);
    let behind = now.is_some_and(|s| s.by(crate::firehose::watermark::METRIC_DELAY, "relay").iter().any(|(_, d)| d.is_finite() && *d > 120.0));
    if lagging || behind {
        out.push(Notice {
            class: "",
            text: "Prism is catching up with the network: a relay is over two minutes behind, or more records are waiting than the backlog warning allows.".to_string(),
        });
    }
    if now.and_then(|s| s.sum(crate::firehose::watermark::METRIC_DEGRADED)).is_some_and(|v| v >= 0.5) {
        out.push(Notice {
            class: "",
            text: "Private posts are catching up: a relay private posts wait for is behind, so newer private posts may not load until it catches up. Apps retry by themselves.".to_string(),
        });
    }
    if mode == PublicMode::Filling {
        out.push(Notice { class: "", text: "Public data is being turned on.".to_string() });
    }
    if schema.is_some_and(|(rows, indexes)| !(rows && indexes)) {
        out.push(Notice { class: "", text: "Search is still being prepared.".to_string() });
    }
    out
}

/// The figures: methods, lexicons, the version and what is stored.
fn figures(now: Option<&Snapshot>, served: usize, not_offered: usize, lexicons: usize) -> Vec<Figure> {
    #[allow(clippy::cast_precision_loss)]
    let n = |v: usize| count(v as f64);
    let rows = |table: &str| now.and_then(|s| s.sum_where(crate::db::sampler::METRIC_TABLE_ROWS, "table", table));
    let about = |v: Option<f64>| v.map_or_else(|| "not measured yet".to_string(), |v| format!("about {}", count(v)));
    let exact = |name: &str| now.and_then(|s| s.sum(name)).map_or_else(|| "not measured yet".to_string(), count);
    vec![
        fig("Version", env!("CARGO_PKG_VERSION")),
        fig("Methods served", n(served)),
        fig("Methods answered \"not offered here\"", n(not_offered)),
        fig("Custom lexicons", n(lexicons)),
        fig("Posts stored", about(rows("posts"))),
        fig("Private posts stored", about(rows(crate::private_tier::stats::admin_posts_table()))),
        fig("Profiles stored", about(rows("actors"))),
        fig("Likes stored", about(rows("likes"))),
        fig("Reposts stored", about(rows("reposts"))),
        fig("Follows stored", about(rows("follows"))),
        fig("Accounts Prism follows", exact(crate::db::sampler::METRIC_SCOPE_DIDS)),
        fig("Members", exact(crate::db::sampler::METRIC_MEMBERS)),
    ]
}

/// The page's content. `inputs` are [`health::probes`]'s; `now` the
/// metrics as rendered for this request.
#[must_use]
pub fn view(state: &AdminState, inputs: &health::Inputs, now: Option<&Snapshot>, at: DateTime<Utc>) -> View {
    let cfg = &state.settings.running;
    let live = state.settings.live.load();
    let mode = state.scope.public_mode();
    let checks = health::checks(inputs, &state.health.ring);
    let notices = notices(&checks, inputs, now, mode);
    let status = checks.into_iter().filter(|c| matches!(c.name, "Database" | "Redis" | "Audit guard")).map(StatusRow::from).collect();
    let (methods, served) = method_groups(&usage::registered());
    let not_offered = not_offered(cfg);
    let lexicons = lexicons::all();
    let blocks = match &cfg.farsight.url {
        None => farsight_words(state).to_string(),
        Some(url) => {
            // As Health judges it: five minutes of readings.
            let withheld =
                state.health.ring.held_for(FIVE_MIN, |s| s.max(crate::farsight::gate::METRIC_WITHHELD).map(|v| v >= 1.0)) == health::Held::Yes;
            let fallback = if withheld { "; the fallback is on but not being used" } else { "" };
            format!("Farsight at {}: {}{fallback}", host_of(url), farsight_words(state))
        }
    };
    let public_off = if mode == PublicMode::On {
        Vec::new()
    } else {
        crate::honest_501::PUBLIC_METHODS.iter().map(|m| m.nsid.to_string()).collect()
    };
    View {
        about: ABOUT,
        does: DOES,
        version: env!("CARGO_PKG_VERSION"),
        hostname: state.public_hostname.clone(),
        service_did: format!("did:web:{}", state.public_hostname),
        started: format!("{} ({})", when(state.settings.started_at), ago(state.settings.started_at)),
        notices,
        status,
        relays: relay_rows(cfg, now, at),
        input_rate: input_rate(&state.health.ring),
        lookups: lookups_words(state),
        plc: host_of(&cfg.identity.plc_url),
        blocks,
        public_phase: phase_words(mode).to_string(),
        public_text: public_text(mode).to_string(),
        refract_on: live.refract_enabled,
        labelers: labeler_rows(&live.refract_communities, live.refract_enabled),
        communities: community_rows(&live.refract_communities),
        policy: "Not configured: this build has no moderation policy setting yet.".to_string(),
        figures: figures(now, served, not_offered.len(), lexicons.len()),
        methods,
        served,
        not_offered,
        public_off,
        lexicons,
    }
}

/// `GET /admin/instance`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let inputs = health::probes(&state).await;
    let at = Utc::now();
    let now = state.health.metrics.as_ref().map(|m| Snapshot { at, series: parse(&m.render(), |n| SERIES.contains(&n)) });
    render(
        StatusCode::OK,
        &InstancePage { css: css(), nav: Some(Nav::new(&op, "instance")), title: "Instance".to_string(), v: view(&state, &inputs, now.as_ref(), at) },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relays_by_label_with_hosts_and_delay() {
        let mut cfg = FileConfig::default();
        cfg.firehose.relay_labels = [
            ("wss://relay-a.example/xrpc/com.atproto.sync.subscribeRepos", "relay-a"),
            ("wss://relay-a2.example/xrpc/com.atproto.sync.subscribeRepos", "relay-a"),
            ("wss://relay-b.example/xrpc/com.atproto.sync.subscribeRepos", "relay-b"),
        ]
        .into_iter()
        .map(|(u, l)| (u.to_string(), l.to_string()))
        .collect();
        cfg.firehose.private_relays = vec!["relay-b".to_string()];
        let at = Utc::now();
        let snap = Snapshot {
            at,
            series: parse(
                "firehose_worker_delay_seconds{relay=\"relay-a\"} 3\nfirehose_worker_delay_seconds{relay=\"relay-b\"} +Inf\nfirehose_worker_delay_seconds{relay=\"other\"} 600\n",
                |_| true,
            ),
        };
        let rows = relay_rows(&cfg, Some(&snap), at);
        let labels: Vec<&str> = rows.iter().map(|r| r.label.as_str()).collect();
        assert_eq!(labels, ["other", "relay-a", "relay-b"]);
        assert_eq!(rows[1].hosts, "relay-a.example, relay-a2.example");
        assert_eq!((rows[1].behind.as_str(), rows[1].class), ("3 s behind", "fine"));
        assert!(rows[1].last_heard.ends_with("(just now)"), "{}", rows[1].last_heard);
        assert_eq!((rows[2].behind.as_str(), rows[2].class, rows[2].private), ("never reported on this database", "red", true));
        assert_eq!((rows[0].hosts.as_str(), rows[0].behind.as_str(), rows[0].class), ("not in the configuration", "10 minutes behind", "amber"));
        let none = relay_rows(&cfg, None, at);
        assert!(none.iter().all(|r| r.behind == UNKNOWN));
    }

    #[test]
    fn methods_group_by_namespace_without_the_not_offered() {
        let reg = vec![
            ("a.b.getOne".to_string(), "app"),
            ("a.b.getTwo".to_string(), "index"),
            ("a.c.doIt".to_string(), "private"),
            ("a.b.planned".to_string(), "not_offered"),
            ("_health".to_string(), "health"),
        ];
        let (groups, served) = method_groups(&reg);
        assert_eq!(served, 4);
        let flat: Vec<(String, Vec<String>)> = groups.into_iter().map(|g| (g.namespace, g.methods)).collect();
        assert_eq!(
            flat,
            vec![
                ("a.b".to_string(), vec!["a.b.getOne".to_string(), "a.b.getTwo".to_string()]),
                ("a.c".to_string(), vec!["a.c.doIt".to_string()]),
                ("other".to_string(), vec!["_health".to_string()]),
            ]
        );
    }
}
