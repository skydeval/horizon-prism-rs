//! `GET /admin/live` (admin-ui-live-feed design §3.3, §4.4): one browser's
//! server-sent events.
//!
//! - At open: a `page` of 32 lowercase hex characters (else `400`), then
//!   [`Need::Poll`]. A refusal is never a redirect: `401`, `429` or `503`
//!   with an empty body. Then a slot (§3.4), or `429` at the global cap.
//! - First, one `backlog` event: each ticker's kept lines through the
//!   filter, oldest first (second pass), so a reload repaints the tickers.
//! - While open: lines through the stream's filter at most
//!   [`LINES_PER_SEC`] a second per ticker, in batches; `counts` every second; the
//!   session checked every minute and at its own expiry instant; `end`
//!   with a reason on a failed check, a replacement, the 30-minute
//!   renewal or shutdown.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{ConnectInfo, Query, State};
use axum::http::header::{CACHE_CONTROL, RETRY_AFTER, SET_COOKIE};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::{broadcast, watch};
use tokio::time::{Instant, Interval, MissedTickBehavior};

use super::{Hub, LINES_PER_SEC, Slot};
use crate::admin::AdminState;
use crate::admin::auth::{
    Need, Refresh, check_session, load_session, request_ip, require, session_bytes, with_retry_after,
};
use crate::xrpc::rate_limit::retry_after_secs;
use crate::firehose::feed::{GROUPS, KINDS, Kind, Lanes, Line, METRIC_NOT_SHOWN, Outcome, SOURCES, Source, Totals, wanted_bit};

/// The stream's `Cache-Control`, over axum's `no-cache` (§3.2).
pub const STREAM_CACHE_CONTROL: &str = "no-store, no-transform";
/// Metric: handle lookups by outcome.
pub const METRIC_HANDLES: &str = "admin_live_handle_lookups_total";
/// A batch's handle lookup waits at most this long (§4.4).
const HANDLE_TIMEOUT: Duration = Duration::from_secs(1);
/// What the browser waits before reconnecting a dropped stream.
const RETRY: Duration = Duration::from_millis(5000);

#[derive(Debug, Default, Deserialize)]
pub struct LiveQuery {
    pub page: Option<String>,
    /// Comma-separated kind filters (`Kind::filter`); unknown ones ignored.
    pub kinds: Option<String>,
    /// Comma-separated sources (`Source::slug`); unknown ones ignored.
    pub sources: Option<String>,
}

/// A page id: 32 lowercase hex characters (§3.3).
#[must_use]
pub fn valid_page(page: &str) -> bool {
    page.len() == 32 && page.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A stream's kind and source filter. Empty, or naming nothing known, is
/// everything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Filter {
    kinds: u32,
    sources: u8,
}

impl Filter {
    #[must_use]
    pub fn parse(kinds: Option<&str>, sources: Option<&str>) -> Self {
        let values = |s: Option<&str>| -> Vec<String> {
            s.unwrap_or_default().split(',').map(|v| v.trim().to_ascii_lowercase()).filter(|v| !v.is_empty()).collect()
        };
        let wanted_kinds = values(kinds);
        let mut k = Kind::ALL.iter().filter(|kind| wanted_kinds.iter().any(|v| v == kind.filter())).fold(0u32, |acc, kind| acc | (1 << kind.index()));
        if k == 0 {
            k = (1u32 << KINDS) - 1;
        }
        let wanted_sources = values(sources);
        let mut s = Source::ALL.iter().filter(|src| wanted_sources.iter().any(|v| v == src.slug())).fold(0u8, |acc, src| acc | (1 << src.index()));
        if s == 0 {
            s = (1u8 << SOURCES) - 1;
        }
        Self { kinds: k, sources: s }
    }

    /// The (bucket group, kind) bits this filter wants from the tap.
    #[must_use]
    pub fn wanted(self) -> u64 {
        let mut bits = 0;
        for group in 0..GROUPS {
            if !Source::ALL.iter().any(|s| s.group() == group && self.sources & (1 << s.index()) != 0) {
                continue;
            }
            for kind in Kind::ALL {
                if self.kinds & (1 << kind.index()) != 0 {
                    bits |= wanted_bit(group, kind);
                }
            }
        }
        bits
    }

    #[must_use]
    pub fn admits(self, line: &Line) -> bool {
        self.kinds & (1 << line.kind.index()) != 0 && self.sources & (1 << line.source.index()) != 0
    }
}

/// A line as the `lines` and `backlog` events carry it: no field for what a
/// line may not show (§6), and no `text` key at all without a snippet. `n`
/// is the line's number, which the page dedupes on. `lanes` are
/// the tickers this stream admitted it to: `"f": 1` for "On the firehose",
/// `"i": 1` for "Indexed here"; a snippet goes only with `i`. A line that
/// was not indexed carries its outcome as `o`.
#[must_use]
pub fn line_json(line: &Line, handle: Option<&str>, lanes: Lanes) -> Value {
    let mut m = Map::new();
    m.insert("n".into(), json!(line.seq));
    m.insert("t".into(), json!(line.at.format("%H:%M:%S").to_string()));
    m.insert("src".into(), json!(line.source.slug()));
    if let Some(relay) = &line.relay {
        m.insert("relay".into(), json!(relay));
    }
    m.insert("act".into(), json!(line.action.words()));
    m.insert("k".into(), json!(line.kind.slug()));
    if !line.outcome.indexed() {
        m.insert("o".into(), json!(line.outcome.slug()));
    }
    if lanes.arriving {
        m.insert("f".into(), json!(1));
    }
    if lanes.indexed {
        m.insert("i".into(), json!(1));
    }
    m.insert("did".into(), json!(line.did));
    if let Some(h) = handle {
        m.insert("who".into(), json!(h));
    }
    if let Some(text) = line.text.as_ref().filter(|_| lanes.indexed && line.outcome.indexed()) {
        m.insert("text".into(), json!(text));
    }
    Value::Object(m)
}

/// The `counts` event (§6.4): numbers only, keyed by slugs.
#[must_use]
pub fn counts_json(delta: &Totals, secs: f64, shown: u64, not_shown: u64, total: &Totals, since: &str) -> Value {
    let nonzero = |pairs: Vec<(&'static str, u64)>| -> Map<String, Value> {
        pairs.into_iter().filter(|(_, n)| *n > 0).map(|(k, n)| (k.to_string(), json!(n))).collect()
    };
    json!({
        "secs": secs,
        "indexed": delta.indexed(),
        "kinds": nonzero(delta.indexed_by_kind().into_iter().map(|(k, n)| (k.slug(), n)).collect()),
        "sources": nonzero(delta.indexed_by_source().into_iter().map(|(s, n)| (s.slug(), n)).collect()),
        "reasons": nonzero(delta.reasons().into_iter().map(|(o, n)| (o.slug(), n)).collect()),
        "failed": delta.outcome(Outcome::Failed),
        "unchanged": delta.outcome(Outcome::Unchanged),
        "events": delta.identity_events(),
        "shown": shown,
        "not_shown": not_shown,
        "totals": {
            "indexed": total.indexed(),
            "skipped": total.skipped(),
            "failed": total.outcome(Outcome::Failed),
            "unchanged": total.outcome(Outcome::Unchanged),
            "events": total.identity_events(),
            "since": since,
        },
    })
}

fn empty(status: StatusCode) -> Response {
    status.into_response()
}

/// A refusal at open, never a redirect (§3.3): `401` for no or an ended
/// session, `429`, `503` otherwise; the body empty, the cleared cookie of
/// a cut-off and the `Retry-After` of a `429` kept.
fn refusal(page: &Response) -> Response {
    let status = match page.status() {
        s if s.is_redirection() || s == StatusCode::UNAUTHORIZED => StatusCode::UNAUTHORIZED,
        StatusCode::TOO_MANY_REQUESTS => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut out = empty(status);
    for name in [SET_COOKIE, RETRY_AFTER] {
        if let Some(value) = page.headers().get(&name)
            && (name != RETRY_AFTER || status == StatusCode::TOO_MANY_REQUESTS)
        {
            out.headers_mut().insert(name, value.clone());
        }
    }
    out
}

/// `GET /admin/live`.
pub async fn open(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<LiveQuery>,
) -> Response {
    let Some(page) = q.page.filter(|p| valid_page(p)) else {
        return empty(StatusCode::BAD_REQUEST);
    };
    if let Err(page) = require(&state, &headers, peer, Need::Poll, None).await {
        return refusal(&page);
    }
    let Some(bytes) = session_bytes(&headers) else {
        return empty(StatusCode::UNAUTHORIZED);
    };
    let id_hash = *blake3::hash(&bytes).as_bytes();
    let session = match load_session(&state, &id_hash).await {
        Ok(s) => s,
        Err(_) => return empty(StatusCode::UNAUTHORIZED),
    };
    let hub = state.live.clone();
    let filter = Filter::parse(q.kinds.as_deref(), q.sources.as_deref());
    let rx = hub.tap().subscribe();
    // At the global cap a slot frees when a stream ends; the soonest is a
    // renewal's reserved slot left unclaimed (§3.4).
    let Ok(slot) = hub.admit(id_hash, &page, filter.wanted()) else {
        return with_retry_after(empty(StatusCode::TOO_MANY_REQUESTS), retry_after_secs(hub.timing().reserve));
    };
    tracing::debug!(event = "admin_live_stream_opened", did = %session.did, "live feed stream opened");
    let timing = hub.timing();
    let deadline = instant_at(session.deadline(state.session_idle));
    let mut live = Live::new(state.clone(), hub, rx, slot, filter, id_hash, request_ip(&state, &headers, peer), deadline);
    live.backlog().await;
    let events = futures::stream::unfold(live, |mut live| async move {
        let event = live.next().await?;
        Some((Ok::<Event, Infallible>(event), live))
    });
    let mut response = Sse::new(events).keep_alive(KeepAlive::new().interval(timing.keepalive).text("keepalive")).into_response();
    let h = response.headers_mut();
    h.insert(CACHE_CONTROL, HeaderValue::from_static(STREAM_CACHE_CONTROL));
    h.insert(HeaderName::from_static("x-accel-buffering"), HeaderValue::from_static("no"));
    response
}

/// The tokio instant of a wall-clock time (now, if it has passed).
fn instant_at(t: chrono::DateTime<Utc>) -> Instant {
    Instant::now() + (t - Utc::now()).to_std().unwrap_or(Duration::ZERO)
}

fn interval(period: Duration) -> Interval {
    let mut i = tokio::time::interval_at(Instant::now() + period, period);
    i.set_missed_tick_behavior(MissedTickBehavior::Delay);
    i
}

/// One stream's state.
struct Live {
    state: AdminState,
    hub: Hub,
    rx: broadcast::Receiver<Arc<Line>>,
    slot: Slot,
    closing: watch::Receiver<bool>,
    filter: Filter,
    id_hash: [u8; 32],
    ip: IpAddr,
    /// The per-stream rate (C-F4), per ticker (arriving, indexed): tokens,
    /// and when last refilled.
    tokens: [f64; 2],
    refilled: Instant,
    batch: Vec<(Arc<Line>, Lanes)>,
    /// Live lines up to this number are covered by the backlog.
    skip_upto: u64,
    /// Since the last `counts`.
    shown: u64,
    not_shown: u64,
    prev: Totals,
    prev_at: Instant,
    since: String,
    deadline: Instant,
    renew_at: Instant,
    batch_tick: Interval,
    counts_tick: Interval,
    check_tick: Interval,
    pending: VecDeque<Event>,
    done: bool,
}

impl Live {
    #[allow(clippy::too_many_arguments)]
    fn new(
        state: AdminState,
        hub: Hub,
        rx: broadcast::Receiver<Arc<Line>>,
        slot: Slot,
        filter: Filter,
        id_hash: [u8; 32],
        ip: IpAddr,
        deadline: Instant,
    ) -> Self {
        let timing = hub.timing();
        let now = Instant::now();
        let mut pending = VecDeque::new();
        pending.push_back(Event::default().retry(RETRY));
        Self {
            closing: hub.closing(),
            prev: hub.tap().totals(),
            since: super::since_words(hub.started()),
            state,
            rx,
            slot,
            filter,
            id_hash,
            ip,
            tokens: [burst(); 2],
            refilled: now,
            batch: Vec::new(),
            skip_upto: 0,
            shown: 0,
            not_shown: 0,
            prev_at: now,
            deadline,
            renew_at: now + timing.renew,
            batch_tick: interval(timing.batch),
            counts_tick: interval(timing.counts),
            check_tick: interval(timing.check),
            pending,
            done: false,
            hub,
        }
    }

    async fn next(&mut self) -> Option<Event> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(event);
            }
            if self.done {
                return None;
            }
            if *self.closing.borrow() {
                self.end("shutting_down");
                continue;
            }
            tokio::select! {
                biased;
                changed = self.closing.changed() => {
                    if changed.is_err() || *self.closing.borrow() {
                        self.end("shutting_down");
                    }
                }
                changed = self.slot.end.changed() => {
                    let reason = if changed.is_ok() { *self.slot.end.borrow() } else { Some("replaced") };
                    if let Some(reason) = reason {
                        self.end(reason);
                    }
                }
                () = tokio::time::sleep_until(self.renew_at) => {
                    self.slot.renewing = true;
                    self.end("renew");
                }
                _ = self.check_tick.tick() => self.check().await,
                () = tokio::time::sleep_until(self.deadline) => self.check().await,
                _ = self.batch_tick.tick() => self.flush().await,
                _ = self.counts_tick.tick() => self.counts(),
                received = self.rx.recv() => match received {
                    Ok(line) => self.offer(line),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        self.not_shown += n;
                        metrics::counter!(METRIC_NOT_SHOWN, "why" => "lagged").increment(n);
                    }
                    Err(broadcast::error::RecvError::Closed) => self.end("shutting_down"),
                },
            }
        }
    }

    fn end(&mut self, reason: &'static str) {
        self.pending.push_back(Event::default().event("end").data(json!({ "reason": reason }).to_string()));
        self.done = true;
        tracing::debug!(event = "admin_live_stream_ended", reason, "live feed stream ended");
    }

    /// The session check (§3.3), every interval and at the expiry instant,
    /// which re-arms from the row as it is now.
    async fn check(&mut self) {
        match check_session(&self.state, &self.id_hash, Refresh::No, Some(self.ip)).await {
            Ok(session) => self.deadline = instant_at(session.deadline(self.state.session_idle)),
            Err(refusal) => self.end(refusal.reason()),
        }
    }

    /// The `backlog` event, once, before any live line (second pass): each
    /// ticker's kept lines through this stream's filter, younger than
    /// [`BACKLOG_MAX_AGE`](crate::firehose::feed::BACKLOG_MAX_AGE), oldest
    /// first, with stored handles. Not paced and not counted against the
    /// rate. A line kept for both tickers is one entry with both marks.
    async fn backlog(&mut self) {
        let backlog = self.hub.tap().backlog(Utc::now());
        self.skip_upto = backlog.last;
        let mut merged: BTreeMap<u64, (Arc<Line>, Lanes)> = BTreeMap::new();
        for line in backlog.arriving.into_iter().filter(|l| self.filter.admits(l)) {
            merged.entry(line.seq).or_insert_with(|| (line, Lanes::default())).1.arriving = true;
        }
        for line in backlog.indexed.into_iter().filter(|l| self.filter.admits(l)) {
            merged.entry(line.seq).or_insert_with(|| (line, Lanes::default())).1.indexed = true;
        }
        let entries: Vec<(Arc<Line>, Lanes)> = merged.into_values().collect();
        let dids: Vec<&str> = entries.iter().map(|(l, _)| l.did.as_str()).collect();
        let handles = if dids.is_empty() { Vec::new() } else { handles(&self.state, &self.hub, &dids).await };
        let lines: Vec<Value> = entries.iter().zip(&handles).map(|((l, lanes), h)| line_json(l, h.as_deref(), *lanes)).collect();
        self.pending.push_back(Event::default().event("backlog").data(Value::Array(lines).to_string()));
    }

    fn offer(&mut self, line: Arc<Line>) {
        if line.seq <= self.skip_upto || !self.filter.admits(&line) {
            return;
        }
        let now = Instant::now();
        let refill = now.duration_since(self.refilled).as_secs_f64() * f64::from(LINES_PER_SEC);
        for t in &mut self.tokens {
            *t = (*t + refill).min(burst());
        }
        self.refilled = now;
        let mut take = |lane: usize, offered: bool| {
            let ok = offered && self.tokens[lane] >= 1.0;
            if ok {
                self.tokens[lane] -= 1.0;
            }
            ok
        };
        let lanes = Lanes { arriving: take(0, line.lanes.arriving), indexed: take(1, line.lanes.indexed) };
        if lanes.arriving || lanes.indexed {
            self.batch.push((line, lanes));
        } else {
            self.not_shown += 1;
            metrics::counter!(METRIC_NOT_SHOWN, "why" => "stream_rate").increment(1);
        }
    }

    /// One `lines` event of the batch, newest last, with stored handles.
    async fn flush(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.batch);
        let dids: Vec<&str> = batch.iter().map(|(l, _)| l.did.as_str()).collect();
        let handles = handles(&self.state, &self.hub, &dids).await;
        let lines: Vec<Value> = batch.iter().zip(&handles).map(|((l, lanes), h)| line_json(l, h.as_deref(), *lanes)).collect();
        self.shown += lines.len() as u64;
        self.pending.push_back(Event::default().event("lines").data(Value::Array(lines).to_string()));
    }

    fn counts(&mut self) {
        let now = Instant::now();
        let totals = self.hub.tap().totals();
        let delta = totals.since(&self.prev);
        let secs = now.duration_since(self.prev_at).as_secs_f64();
        let body = counts_json(&delta, secs, self.shown, self.not_shown, &totals, &self.since);
        self.pending.push_back(Event::default().event("counts").data(body.to_string()));
        self.prev = totals;
        self.prev_at = now;
        self.shown = 0;
        self.not_shown = 0;
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        if !self.done {
            tracing::debug!(event = "admin_live_stream_ended", reason = "gone", "live feed stream ended");
        }
    }
}

/// A batch's most lines: the per-stream rate over one batch, rounded up.
fn burst() -> f64 {
    (f64::from(LINES_PER_SEC) / 4.0).ceil()
}

/// Stored handles of a batch's DIDs (§4.4): the hub's cache, then one
/// query for the misses on an idle connection, bounded by a timeout. With
/// no idle connection, an error or a timeout, the line shows the DID.
async fn handles(state: &AdminState, hub: &Hub, dids: &[&str]) -> Vec<Option<String>> {
    let (cached, misses) = hub.cached_handles(dids).await;
    let mut looked: HashMap<String, Option<String>> = HashMap::new();
    if !misses.is_empty() {
        let outcome = match state.pool.try_acquire() {
            None => "skipped",
            Some(mut conn) => {
                let query = sqlx::query_as::<_, (String, Option<String>)>("SELECT did, handle FROM actors WHERE did = ANY($1)")
                    .bind(&misses)
                    .fetch_all(&mut *conn);
                match tokio::time::timeout(HANDLE_TIMEOUT, query).await {
                    Ok(Ok(rows)) => {
                        for (did, handle) in rows {
                            looked.insert(did, handle);
                        }
                        for did in misses {
                            let handle = looked.entry(did.clone()).or_insert(None).clone();
                            hub.remember_handle(did, handle).await;
                        }
                        "ok"
                    }
                    Ok(Err(_)) => "failed",
                    Err(_) => "timeout",
                }
            }
        };
        metrics::counter!(METRIC_HANDLES, "outcome" => outcome).increment(1);
    }
    dids.iter()
        .zip(cached)
        .map(|(did, hit)| match hit {
            Some(h) => h,
            None => looked.get(*did).cloned().flatten(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::firehose::feed::{LineAction, WANT_ALL};

    #[test]
    fn page_ids_and_filters() {
        assert!(valid_page(&"0123456789abcdef".repeat(2)));
        for bad in ["", "abc", &"0123456789ABCDEF".repeat(2), &"g".repeat(32), &"a".repeat(33)] {
            assert!(!valid_page(bad), "{bad}");
        }
        assert_eq!(Filter::parse(None, None).wanted(), WANT_ALL);
        assert_eq!(Filter::parse(Some("nonsense"), Some("nowhere")).wanted(), WANT_ALL, "unknown values ignored");
        let blocks = Filter::parse(Some("block"), None);
        assert_eq!(blocks.wanted(), wanted_bit(0, Kind::Block) | wanted_bit(1, Kind::Block));
        let backfill_private = Filter::parse(Some("private, junk"), Some("backfill"));
        let line = |kind, source| Line {
            seq: 1,
            at: Utc::now(),
            source,
            relay: None,
            action: LineAction::Created,
            kind,
            outcome: Outcome::Written,
            lanes: BOTH,
            did: "did:plc:a".into(),
            text: None,
        };
        assert!(backfill_private.admits(&line(Kind::PrivatePost, Source::Backfill)));
        assert!(backfill_private.admits(&line(Kind::PrivateGate, Source::Backfill)));
        assert!(!backfill_private.admits(&line(Kind::PrivatePost, Source::Network)));
        assert!(!backfill_private.admits(&line(Kind::Post, Source::Backfill)));
        assert_eq!(backfill_private.wanted() & wanted_bit(0, Kind::PrivatePost), 0, "the network group is not wanted");
        // `requested` and `catch-up` share the backfill group's budget.
        assert_eq!(Filter::parse(None, Some("catch-up")).wanted(), Filter::parse(None, Some("backfill")).wanted());
    }

    const BOTH: Lanes = Lanes { arriving: true, indexed: true };

    #[test]
    fn a_line_without_a_snippet_has_no_text_key() {
        let mut l = Line {
            seq: 7,
            at: Utc::now(),
            source: Source::Network,
            relay: Some("bsky".into()),
            action: LineAction::Created,
            kind: Kind::PrivatePost,
            outcome: Outcome::Written,
            lanes: BOTH,
            did: "did:plc:a".into(),
            text: None,
        };
        let v = line_json(&l, None, BOTH);
        assert!(v.get("text").is_none() && v.get("who").is_none() && v.get("o").is_none());
        assert_eq!(v["n"], 7, "the line's number, for the page to dedupe on");
        assert_eq!((v["k"].as_str(), v["src"].as_str(), v["relay"].as_str()), (Some("private-post"), Some("network"), Some("bsky")));
        assert_eq!((v["f"].as_u64(), v["i"].as_u64()), (Some(1), Some(1)));
        l.text = Some("hi".into());
        assert_eq!(line_json(&l, Some("alice.test"), BOTH)["who"], "alice.test");
        // A snippet goes only with the indexed ticker.
        let arriving = Lanes { arriving: true, indexed: false };
        let v = line_json(&l, None, arriving);
        assert!(v.get("text").is_none() && v.get("i").is_none(), "{v}");
        // A line that was not indexed: its outcome, never text.
        l.outcome = Outcome::NotInScope;
        let v = line_json(&l, None, BOTH);
        assert_eq!(v["o"], "not_in_scope");
        assert!(v.get("text").is_none(), "{v}");
        l.outcome = Outcome::Unchanged;
        assert_eq!(line_json(&l, None, arriving)["o"], "unchanged");
    }
}
