//! Relays (`/admin/relays`): each relay by label and host, how far behind
//! it is and when it was last heard, the input from the relays, the records
//! waiting and processed, the relays private posts wait for (set in
//! `app.toml`, shown read-only), and the network stream's state. Nothing
//! here changes anything.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use askama::Template;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, TimeDelta, Utc};

use super::health::Figure;
use super::{Nav, ago, css, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::{Level, Ring, Snapshot, count, parse};
use crate::admin::menu::PageId;
use crate::cli::commands::stream_info::{self, StreamInfo};
use crate::config::FileConfig;

const UNKNOWN: &str = "unknown";
const FIVE_MIN: Duration = Duration::from_secs(300);

/// The series the page reads from one render of the metrics.
const SERIES: &[&str] = &[crate::firehose::watermark::METRIC_DELAY, crate::firehose::watermark::METRIC_DEGRADED, crate::firehose::consumer::METRIC_LAG];

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

/// A notice above the relays.
pub struct Notice {
    pub text: String,
}

#[derive(Template)]
#[template(path = "relays.html")]
struct RelaysPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    notices: Vec<Notice>,
    relays: Vec<RelayRow>,
    /// The input rate, records waiting and processed per second.
    figures: Vec<Figure>,
    /// `firehose.private_relays`, in words.
    private: String,
    stream: Vec<String>,
}

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into(), link: None }
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

/// Records processed per second over the ring's last five minutes.
fn processed(ring: &Ring) -> String {
    ring.rate(FIVE_MIN, |s| s.sum(crate::firehose::consumer::METRIC_PROCESSED)).map_or_else(|| UNKNOWN.to_string(), |r| format!("{r:.1}"))
}

/// The relays catching up, in sentences.
fn notices(lag_warn: u64, now: Option<&Snapshot>) -> Vec<Notice> {
    let mut out = Vec::new();
    #[allow(clippy::cast_precision_loss)]
    let lagging = now.and_then(|s| s.sum(crate::firehose::consumer::METRIC_LAG)).is_some_and(|l| l > lag_warn as f64);
    let behind = now.is_some_and(|s| s.by(crate::firehose::watermark::METRIC_DELAY, "relay").iter().any(|(_, d)| d.is_finite() && *d > 120.0));
    if lagging || behind {
        out.push(Notice {
            text: "Prism is catching up with the network: a relay is over two minutes behind, or more records are waiting than the backlog warning allows.".to_string(),
        });
    }
    if now.and_then(|s| s.sum(crate::firehose::watermark::METRIC_DEGRADED)).is_some_and(|v| v >= 0.5) {
        out.push(Notice {
            text: "Private posts are catching up: a relay private posts wait for is behind, so newer private posts may not load until it catches up. Apps retry by themselves."
                .to_string(),
        });
    }
    out
}

/// The stream's state in sentences.
#[must_use]
pub fn stream_words(info: &StreamInfo) -> Vec<String> {
    #[allow(clippy::cast_precision_loss)]
    let n = |v: i64| count(v as f64);
    let mut out = vec![match info.length {
        Some(len) => format!("The network stream holds {} records waiting for Prism or kept for a while after.", n(len)),
        None => "The network stream's length is unknown.".to_string(),
    }];
    match &info.group {
        None => out.push("Prism's reader is missing, so Prism is not reading the stream. The server's log says why.".to_string()),
        Some(g) => {
            out.push(match g.lag {
                Some(0) => "Prism has read everything in it.".to_string(),
                Some(lag) => format!("{} records are not read yet.", n(lag)),
                None => "Redis cannot say how far behind Prism is: part of the stream was dropped before Prism read it.".to_string(),
            });
            if let Some(p) = g.pending.filter(|p| *p > 0) {
                out.push(format!("{} records are being processed now.", n(p)));
            }
            if let Some(id) = &g.last_delivered_id {
                out.push(format!("The last record read arrived at {}.", super::problems::id_time(id)));
            }
        }
    }
    out
}

/// The stream's state in one phrase, for Problems.
#[must_use]
pub fn stream_summary(info: &StreamInfo) -> String {
    #[allow(clippy::cast_precision_loss)]
    let n = |v: i64| count(v as f64);
    match &info.group {
        None => "Prism's reader is missing".to_string(),
        Some(g) => match g.lag {
            Some(0) => "everything is read".to_string(),
            Some(lag) => format!("{} records not read yet", n(lag)),
            None => "how far behind is unknown".to_string(),
        },
    }
}

/// The network stream as Redis reports it: `Err` says why it could not be
/// read.
pub async fn read_stream(state: &AdminState) -> Result<StreamInfo, &'static str> {
    let fh = &state.settings.running.firehose;
    let Some(redis) = &state.health.redis else {
        return Err("Redis is not connected to these pages");
    };
    stream_info::read(&mut redis.clone(), &fh.stream_key, &fh.group).await.map_err(|_| "it could not be read just now")
}

/// `GET /admin/relays`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let at = Utc::now();
    let now = state.health.metrics.as_ref().map(|m| Snapshot { at, series: parse(&m.render(), |n| SERIES.contains(&n)) });
    let cfg = &state.settings.running;
    let figures = vec![
        fig("Input from the relays", input_rate(&state.health.ring)),
        fig("Records waiting", now.as_ref().and_then(|s| s.sum(crate::firehose::consumer::METRIC_LAG)).map_or_else(|| UNKNOWN.to_string(), count)),
        fig("Processed per second (last 5 minutes)", processed(&state.health.ring)),
    ];
    let private = if cfg.firehose.private_relays.is_empty() {
        "None: private reads wait for no relay.".to_string()
    } else {
        cfg.firehose.private_relays.join(", ")
    };
    let stream = match read_stream(&state).await {
        Ok(info) => stream_words(&info),
        Err(why) => vec![format!("The network stream's state is unknown: {why}.")],
    };
    render(
        StatusCode::OK,
        &RelaysPage {
            css: css(),
            nav: Some(Nav::page(&op, PageId::Relays)),
            title: PageId::Relays.page().name.to_string(),
            notices: notices(cfg.firehose.lag_warn_entries, now.as_ref()),
            relays: relay_rows(cfg, now.as_ref(), at),
            figures,
            private,
            stream,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands::stream_info::GroupInfo;

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
    fn the_stream_in_words() {
        let behind = StreamInfo {
            length: Some(5000),
            first_entry_id: Some("1-0".into()),
            group: Some(GroupInfo { pending: Some(2), last_delivered_id: Some("1760918400000-0".into()), lag: Some(1234) }),
        };
        let words = stream_words(&behind).join(" ");
        assert!(words.contains("5,000 records") && words.contains("1,234 records are not read yet") && words.contains("2 records are being processed"), "{words}");
        assert_eq!(stream_summary(&behind), "1,234 records not read yet");
        let missing = StreamInfo { length: Some(1), first_entry_id: None, group: None };
        assert!(stream_words(&missing).join(" ").contains("reader is missing"));
        assert_eq!(stream_summary(&missing), "Prism's reader is missing");
    }
}
