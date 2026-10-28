//! The live feed's publisher (public-page design §6.5): one task that takes
//! the reader's decoded entries and releases them by entry time.
//!
//! - **Release.** Each line goes out at the first 100 ms tick, in Redis
//!   time, at or after its `entry_ms` + 500 ms; each entry second's figures
//!   at or after its end + 500 ms. A line read later goes out at the next
//!   tick and counts in `public_feed_late_total`.
//! - **Viewers or not**, it counts every released line in `c`, keeps the
//!   newest 20 for the backlog, the per-second arrivals, the totals, each
//!   relay's delay and silence, and the input rate, and stores the summary
//!   the status block reads.
//! - **Only while someone watches** it frames each tick's lines as `lines`
//!   events and each second as a `counts` event, each framed once as a
//!   complete server-sent event in `Bytes`, on one broadcast for every
//!   viewer. Every event carries `e`, its own counter; `lines` carry `c`
//!   through their newest line.
//!
//! The wire form is dictionary-coded, since every byte is multiplied by the
//! full relay rate and the viewers: `{"e","c","t0","n":[nsids],
//! "l":[[s, a, n, m, id], …]}`, with `s` seconds after `t0` and `m` the
//! DID's method (`p`, `w`, or `""` for the whole string in `id`).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Bytes;
use redis::aio::ConnectionManager;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

use super::decode::{Decoded, Labels, PublicLine};
use super::reader::{Msg, Probe, Reader, ReaderSettings, Skip, local_ms};
use super::status::{Delay, FeedHandle, FeedSummary, RelayFigure};
use crate::firehose::feed::{KINDS, Kind};

/// `public_feed_late_total`: lines released after their tick.
pub const METRIC_LATE: &str = "public_feed_late_total";

/// The publisher's settings, shortened in some tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedSettings {
    /// Lines and seconds are released this long after their entry time.
    pub release_ms: i64,
    pub tick_ms: i64,
    /// Lines kept for the backlog, and how old they may be.
    pub ring: usize,
    pub ring_age_ms: i64,
    /// Frames the viewers' broadcast holds.
    pub broadcast: usize,
    /// A `lines` event past this many bytes is split.
    pub event_max: usize,
    /// Batches between the reader and the publisher.
    pub channel: usize,
}

impl Default for FeedSettings {
    fn default() -> Self {
        Self {
            release_ms: 500,
            tick_ms: 100,
            ring: 20,
            ring_age_ms: 15 * 60 * 1000,
            broadcast: 64,
            event_max: 256 * 1024,
            channel: 64,
        }
    }
}

/// One framed event.
#[derive(Debug, Clone)]
pub struct Frame {
    pub e: u64,
    /// `c` through the newest line framed so far.
    pub c: u64,
    pub bytes: Bytes,
}

/// A skipped span, as `skipped` and every `backlog` carry it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub from: i64,
    pub to: i64,
    pub why: &'static str,
}

impl Span {
    fn json(self) -> String {
        format!(r#"{{"from":{},"to":{},"why":"{}"}}"#, self.from, self.to, self.why)
    }
}

/// What the publisher shares with the viewers.
#[derive(Default)]
struct Published {
    e: u64,
    c: u64,
    ring: VecDeque<PublicLine>,
    skipped: Option<Span>,
    skipped_since_start: bool,
    /// Redis time of the last tick.
    now_ms: i64,
}

/// The publisher's handle. Cheap to clone.
#[derive(Clone)]
pub struct PublicFeed(Arc<Shared>);

struct Shared {
    tx: broadcast::Sender<Arc<Frame>>,
    state: Mutex<Published>,
    framed_c: AtomicU64,
    closing: watch::Sender<bool>,
    /// A random id, made once per process.
    boot: String,
    /// The start, rounded to the hour ("YYYY-MM-DD HH:00", UTC).
    since: String,
    s: FeedSettings,
    /// Events framed (T-P19: each framed once).
    framed: AtomicU64,
    /// Ops of entries for seconds already published (T-P18).
    late_ops: AtomicU64,
    #[cfg(test)]
    released: Mutex<Vec<(i64, i64, String)>>,
}

impl PublicFeed {
    /// The reader and the publisher on `conn`, a connection of their own.
    #[must_use]
    pub fn start(conn: ConnectionManager, key: &str, labels: Labels, summary: FeedHandle, rs: ReaderSettings, s: FeedSettings, probe: Arc<Probe>) -> (Self, Vec<JoinHandle<()>>) {
        let (tx, rx) = mpsc::channel(s.channel);
        let feed = Self::new(s);
        let mut tasks = Reader::new(conn, key, labels, tx, rs, probe).spawn();
        tasks.push(tokio::spawn(Publisher::new(feed.clone(), summary).run(rx)));
        (feed, tasks)
    }

    #[must_use]
    pub fn new(s: FeedSettings) -> Self {
        let (tx, _) = broadcast::channel(s.broadcast);
        Self(Arc::new(Shared {
            tx,
            state: Mutex::default(),
            framed_c: AtomicU64::new(0),
            closing: watch::channel(false).0,
            boot: hex::encode(rand::random::<[u8; 8]>()),
            since: chrono::Utc::now().format("%Y-%m-%d %H:00").to_string(),
            s,
            framed: AtomicU64::new(0),
            late_ops: AtomicU64::new(0),
            #[cfg(test)]
            released: Mutex::default(),
        }))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, Published> {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Subscribe a viewer, then take the backlog under the same lock, so no
    /// line is both in it and in a later event: the receiver, the `backlog`
    /// frame, its `e` and `c`.
    #[must_use]
    pub fn subscribe(&self) -> (broadcast::Receiver<Arc<Frame>>, Bytes, u64, u64) {
        let st = self.state();
        let rx = self.0.tx.subscribe();
        let oldest = st.now_ms - self.0.s.ring_age_ms;
        let lines: Vec<&PublicLine> = st.ring.iter().filter(|l| l.entry_ms >= oldest).collect();
        let mut body = String::with_capacity(64 * lines.len() + 128);
        let _ = write!(body, r#"{{"e":{},"c":{},"boot":"{}","since":"{}","#, st.e, st.c, self.0.boot, self.0.since);
        wire_lines(&mut body, &lines);
        let _ = write!(
            body,
            r#","skipped":{},"skipped_since_start":{}}}"#,
            st.skipped.map_or_else(|| "null".to_string(), Span::json),
            st.skipped_since_start
        );
        (rx, sse("backlog", &body), st.e, st.c)
    }

    /// `c` through the newest line framed.
    #[must_use]
    pub fn framed_c(&self) -> u64 {
        self.0.framed_c.load(Ordering::Acquire)
    }

    /// The running count of published lines.
    #[must_use]
    pub fn c(&self) -> u64 {
        self.state().c
    }

    /// Events framed since the start.
    #[must_use]
    pub fn framed(&self) -> u64 {
        self.0.framed.load(Ordering::Relaxed)
    }

    /// Ops of entries that came for a second already published.
    #[must_use]
    pub fn late_ops(&self) -> u64 {
        self.0.late_ops.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn boot(&self) -> &str {
        &self.0.boot
    }

    /// Shutdown: every viewer stream closes with a random `retry`.
    pub fn close(&self) {
        self.0.closing.send_replace(true);
    }

    #[must_use]
    pub fn closing(&self) -> watch::Receiver<bool> {
        self.0.closing.subscribe()
    }

    /// `(entry_ms, the tick it was released at, did)` of every released
    /// line, in order.
    #[cfg(test)]
    #[must_use]
    pub fn released(&self) -> Vec<(i64, i64, String)> {
        self.0.released.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

/// A complete server-sent event.
#[must_use]
pub fn sse(event: &str, data: &str) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {data}\n\n"))
}

fn json_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// `"t0":…,"r":[…],"n":[…],"l":[…]` for `lines`.
fn wire_lines(out: &mut String, lines: &[&PublicLine]) {
    let t0 = lines.first().map_or(0, |l| l.entry_ms.div_euclid(1000));
    let mut nsids: Vec<&str> = Vec::new();
    let mut nsid_at: HashMap<&str, usize> = HashMap::new();
    let mut l = String::with_capacity(44 * lines.len());
    for (i, line) in lines.iter().enumerate() {
        let n = *nsid_at.entry(line.nsid.as_str()).or_insert_with(|| {
            nsids.push(&line.nsid);
            nsids.len() - 1
        });
        let (m, id) = if let Some(rest) = line.did.strip_prefix("did:plc:") {
            ("p", rest)
        } else if let Some(rest) = line.did.strip_prefix("did:web:") {
            ("w", rest)
        } else {
            ("", line.did.as_str())
        };
        if i > 0 {
            l.push(',');
        }
        let _ = write!(l, r#"[{},"{}",{n},"{m}",{}]"#, line.entry_ms.div_euclid(1000) - t0, line.action.letter(), json_str(id));
    }
    let list = |v: &[&str]| v.iter().map(|s| json_str(s)).collect::<Vec<_>>().join(",");
    let _ = write!(out, r#""t0":{t0},"n":[{}],"l":[{l}]"#, list(&nsids));
}

/// The size of a line in the wire form, before the dictionary: an
/// estimate for splitting.
fn line_size(line: &PublicLine) -> usize {
    32 + line.did.len() * 2 + line.nsid.len() * 2
}

/// A relay's delays over one second: 100 ms buckets up to 60 s, 1 s buckets
/// up to 10 minutes, one bucket over.
#[derive(Debug, Clone, Default)]
struct Hist {
    buckets: Vec<u32>,
    n: u32,
}

const OVER: usize = 600 + 540;

impl Hist {
    fn bucket(ms: i64) -> usize {
        let ms = usize::try_from(ms.max(0)).unwrap_or(usize::MAX);
        if ms < 60_000 {
            ms / 100
        } else if ms < 600_000 {
            600 + (ms - 60_000) / 1000
        } else {
            OVER
        }
    }

    fn add(&mut self, ms: i64) {
        if self.buckets.is_empty() {
            self.buckets = vec![0; OVER + 1];
        }
        self.buckets[Self::bucket(ms)] += 1;
        self.n += 1;
    }

    /// The lower edge of the bucket holding the median.
    fn median(&self) -> Option<Delay> {
        if self.n == 0 {
            return None;
        }
        let target = (self.n - 1) / 2;
        let mut seen = 0;
        for (i, &count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen > target {
                return Some(match i {
                    OVER => Delay::OverTenMinutes,
                    i if i < 600 => Delay::Ms(i as u64 * 100),
                    i => Delay::Ms(60_000 + (i as u64 - 600) * 1000),
                });
            }
        }
        None
    }
}

/// One entry second, until it is published.
#[derive(Debug, Default)]
struct Second {
    arrivals: [u64; KINDS],
    events: u64,
    /// By relay: heard in this second, with its delays.
    relays: HashMap<Arc<str>, Hist>,
}

/// What waits for its release tick.
enum Item {
    Line(PublicLine),
    Skip(Skip),
}

/// The publisher task's own state.
struct Publisher {
    feed: PublicFeed,
    summary: FeedHandle,
    queue: VecDeque<(i64, Item)>,
    seconds: BTreeMap<i64, Second>,
    last_published: Option<i64>,
    last_tick: Option<i64>,
    offset: i64,
    totals: [u64; KINDS],
    events_total: u64,
    relays: BTreeMap<String, RelayFigure>,
    inputs: VecDeque<(i64, u64)>,
    last_line_ms: Option<i64>,
    lines_since_anchor: bool,
}

impl Publisher {
    fn new(feed: PublicFeed, summary: FeedHandle) -> Self {
        Self {
            feed,
            summary,
            queue: VecDeque::new(),
            seconds: BTreeMap::new(),
            last_published: None,
            last_tick: None,
            offset: 0,
            totals: [0; KINDS],
            events_total: 0,
            relays: BTreeMap::new(),
            inputs: VecDeque::new(),
            last_line_ms: None,
            lines_since_anchor: false,
        }
    }

    fn s(&self) -> FeedSettings {
        self.feed.0.s
    }

    /// The tick a time is released at: the first at or after it.
    fn tick_of(&self, ms: i64) -> i64 {
        let t = self.s().tick_ms;
        (ms + t - 1).div_euclid(t) * t
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Msg>) {
        let tick = self.s().tick_ms;
        // The next grid point, kept across wakes: every point runs once and
        // in order, whatever the messages' timing, so a line's tick never
        // moves with the reader's latency.
        let mut next = ((local_ms() + self.offset).div_euclid(tick) + 1) * tick;
        loop {
            while local_ms() + self.offset >= next {
                self.tick(next);
                next += tick;
            }
            let wait = u64::try_from(next - self.offset - local_ms()).unwrap_or(0);
            tokio::select! {
                biased;
                () = tokio::time::sleep(Duration::from_millis(wait)) => {}
                msg = rx.recv() => match msg {
                    Some(m) => self.absorb(m),
                    None => return,
                },
            }
        }
    }

    fn absorb(&mut self, msg: Msg) {
        match msg {
            Msg::Entries(entries) => {
                for d in entries {
                    self.entry(d);
                }
            }
            Msg::Skipped(skip) => {
                let at = self.tick_of(skip.to_ms + self.s().release_ms);
                self.queue.push_back((at, Item::Skip(skip)));
            }
            Msg::Input { at_ms, entries_added } => {
                if self.inputs.back().is_some_and(|(_, n)| entries_added < *n) {
                    self.inputs.clear();
                }
                self.inputs.push_back((at_ms, entries_added));
                while self.inputs.front().is_some_and(|(t, _)| *t < at_ms - 10_500) {
                    self.inputs.pop_front();
                }
            }
            Msg::Offset(offset) => self.offset = offset,
        }
    }

    fn entry(&mut self, d: Decoded) {
        let second = d.entry_ms.div_euclid(1000);
        if self.last_published.is_some_and(|p| second <= p) {
            // Its second is out: the totals only, once.
            for k in &d.kinds {
                self.totals[k.index()] += 1;
            }
            self.events_total += u64::from(d.identity);
            self.feed.0.late_ops.fetch_add(d.kinds.len() as u64, Ordering::Relaxed);
        } else {
            let acc = self.seconds.entry(second).or_default();
            for k in &d.kinds {
                acc.arrivals[k.index()] += 1;
            }
            acc.events += u64::from(d.identity);
            let hist = acc.relays.entry(Arc::clone(&d.relay)).or_default();
            if let Some(ms) = d.delay_ms {
                hist.add(ms);
            }
        }
        if let Some(line) = d.line {
            let at = self.tick_of(line.entry_ms + self.s().release_ms);
            if self.last_tick.is_some_and(|t| at <= t) {
                metrics::counter!(METRIC_LATE).increment(1);
            }
            self.queue.push_back((at, Item::Line(line)));
        }
    }

    fn input_rate(&self) -> Option<f64> {
        let (Some((t0, n0)), Some((t1, n1))) = (self.inputs.front(), self.inputs.back()) else {
            return None;
        };
        // No reading for a while (Redis away, no stream): not known now.
        if t1 <= t0 || local_ms() - t1 > 3000 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        Some((n1 - n0) as f64 / ((t1 - t0) as f64 / 1000.0))
    }

    fn tick(&mut self, now: i64) {
        let s = self.s();
        let shared = Arc::clone(&self.feed.0);
        // Under the lock `subscribe` takes: a viewer joins either before
        // this tick (and gets its frames) or after (and its backlog has it).
        let mut st = shared.state.lock().unwrap_or_else(PoisonError::into_inner);
        let watching = shared.tx.receiver_count() > 0;
        st.now_ms = now;
        let mut batch: Vec<PublicLine> = Vec::new();
        while self.queue.front().is_some_and(|(at, _)| *at <= now) {
            let Some((_, item)) = self.queue.pop_front() else {
                break;
            };
            match item {
                Item::Line(line) => {
                    st.c += 1;
                    self.last_line_ms = Some(line.entry_ms);
                    self.lines_since_anchor = true;
                    #[cfg(test)]
                    shared.released.lock().unwrap_or_else(PoisonError::into_inner).push((line.entry_ms, now, line.did.clone()));
                    st.ring.push_back(line.clone());
                    while st.ring.len() > s.ring {
                        st.ring.pop_front();
                    }
                    batch.push(line);
                }
                Item::Skip(skip) => {
                    let base = st.c - batch.len() as u64;
                    self.frame_lines(&mut st, &batch, base, watching);
                    batch.clear();
                    let from = match (self.lines_since_anchor, self.last_line_ms) {
                        (true, Some(ms)) => ms,
                        _ => skip.prev_anchor_ms,
                    };
                    let span = Span { from, to: skip.to_ms, why: skip.why.slug() };
                    self.lines_since_anchor = false;
                    st.skipped = Some(span);
                    st.skipped_since_start = true;
                    if watching {
                        st.e += 1;
                        let body = format!(r#"{{"e":{},"from":{},"to":{},"why":"{}"}}"#, st.e, span.from, span.to, span.why);
                        let frame = Frame { e: st.e, c: st.c, bytes: sse("skipped", &body) };
                        shared.framed.fetch_add(1, Ordering::Relaxed);
                        let _ = shared.tx.send(Arc::new(frame));
                    }
                }
            }
        }
        let base = st.c - batch.len() as u64;
        self.frame_lines(&mut st, &batch, base, watching);
        // Each entry second, once its end + the release has passed.
        let due = (now - s.release_ms).div_euclid(1000) - 1;
        let first = self.last_published.map_or(due, |p| p + 1);
        for second in first..=due {
            self.publish(&mut st, second, watching);
        }
        if first <= due {
            self.store_summary(&st);
        }
        self.last_tick = Some(now);
    }

    /// Frame released lines as `lines` events, split past the size bound;
    /// `base` is `c` before the first of them.
    fn frame_lines(&self, st: &mut Published, lines: &[PublicLine], base: u64, watching: bool) {
        if lines.is_empty() {
            return;
        }
        let shared = &self.feed.0;
        if !watching {
            shared.framed_c.store(st.c.max(shared.framed_c.load(Ordering::Relaxed)), Ordering::Release);
            return;
        }
        let max = self.s().event_max;
        let mut start = 0;
        while start < lines.len() {
            let mut size = 256;
            let mut end = start;
            while end < lines.len() && (end == start || size + line_size(&lines[end]) <= max) {
                size += line_size(&lines[end]);
                end += 1;
            }
            let chunk: Vec<&PublicLine> = lines[start..end].iter().collect();
            st.e += 1;
            let c = base + end as u64;
            let mut body = String::with_capacity(size);
            let _ = write!(body, r#"{{"e":{},"c":{c},"#, st.e);
            wire_lines(&mut body, &chunk);
            body.push('}');
            let frame = Frame { e: st.e, c, bytes: sse("lines", &body) };
            shared.framed.fetch_add(1, Ordering::Relaxed);
            shared.framed_c.store(c, Ordering::Release);
            let _ = shared.tx.send(Arc::new(frame));
            start = end;
        }
    }

    fn publish(&mut self, st: &mut Published, second: i64, watching: bool) {
        let acc = self.seconds.remove(&second).unwrap_or_default();
        // Anything older that is still held (it cannot be, but never keep
        // it): into this second.
        let older: Vec<i64> = self.seconds.range(..second).map(|(k, _)| *k).collect();
        let mut acc = acc;
        for k in older {
            if let Some(o) = self.seconds.remove(&k) {
                for (i, n) in o.arrivals.iter().enumerate() {
                    acc.arrivals[i] += n;
                }
                acc.events += o.events;
            }
        }
        for (i, n) in acc.arrivals.iter().enumerate() {
            self.totals[i] += n;
        }
        self.events_total += acc.events;
        for (relay, hist) in &acc.relays {
            let f = self.relays.entry(relay.to_string()).or_insert(RelayFigure { last_heard_sec: second, delay: None });
            f.last_heard_sec = second;
            if let Some(d) = hist.median() {
                f.delay = Some(d);
            }
        }
        self.last_published = Some(second);
        if watching {
            st.e += 1;
            let kinds = |counts: &[u64; KINDS]| {
                let mut m = String::new();
                for k in Kind::ALL {
                    let n = counts[k.index()];
                    if n > 0 {
                        if !m.is_empty() {
                            m.push(',');
                        }
                        let _ = write!(m, r#""{}":{n}"#, k.slug());
                    }
                }
                m
            };
            let input = self.input_rate().map_or_else(|| "null".to_string(), |r| format!("{r:.1}"));
            let body = format!(
                r#"{{"e":{},"t":{second},"arrivals":{{{}}},"totals":{{{}}},"events":{{"now":{},"total":{}}},"input":{input}}}"#,
                st.e,
                kinds(&acc.arrivals),
                kinds(&self.totals),
                acc.events,
                self.events_total
            );
            let frame = Frame { e: st.e, c: st.c, bytes: sse("counts", &body) };
            self.feed.0.framed.fetch_add(1, Ordering::Relaxed);
            let _ = self.feed.0.tx.send(Arc::new(frame));
        }
    }

    fn store_summary(&self, st: &Published) {
        let totals = Kind::ALL.iter().filter(|k| self.totals[k.index()] > 0).map(|k| (k.slug().to_string(), self.totals[k.index()])).collect();
        self.summary.store(Arc::new(FeedSummary {
            relays: self.relays.clone(),
            newest_published_sec: self.last_published,
            input_per_sec: self.input_rate(),
            totals,
            events_total: self.events_total,
            since: self.feed.0.since.clone(),
            skipped_since_start: st.skipped_since_start,
        }));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::public_page::decode::Action;
    use serde_json::Value;

    pub(crate) fn line(ms: i64, nsid: &str, did: &str) -> PublicLine {
        PublicLine { entry_ms: ms, action: Action::Create, nsid: nsid.to_string(), did: did.to_string() }
    }

    pub(crate) fn decoded(ms: i64, kinds: Vec<Kind>, line: Option<PublicLine>) -> Decoded {
        Decoded { entry_ms: ms, relay: Arc::from("one"), kinds, identity: false, delay_ms: None, line }
    }

    /// The `data` of a framed event, parsed.
    pub(crate) fn data(frame: &Bytes) -> (String, Value) {
        let text = std::str::from_utf8(frame).expect("utf8");
        let name = text.lines().next().and_then(|l| l.strip_prefix("event: ")).expect("event").to_string();
        let data = text.lines().nth(1).and_then(|l| l.strip_prefix("data: ")).expect("data");
        (name, serde_json::from_str(data).expect("json"))
    }

    /// A wire-form event's lines, decoded as the page decodes them:
    /// `(unix seconds, action, nsid, did)`. Each line has exactly those four
    /// fields, the DID in two; there is no relay field and no relay
    /// dictionary.
    pub(crate) fn lines_of(v: &Value) -> Vec<(i64, String, String, String)> {
        assert!(v.get("r").is_none(), "no relay dictionary: {v}");
        let t0 = v["t0"].as_i64().expect("t0");
        let n = v["n"].as_array().expect("n");
        v["l"]
            .as_array()
            .expect("l")
            .iter()
            .map(|l| {
                assert_eq!(l.as_array().map(Vec::len), Some(5), "{l}");
                let m = l[3].as_str().expect("m");
                let id = l[4].as_str().expect("id");
                let did = match m {
                    "p" => format!("did:plc:{id}"),
                    "w" => format!("did:web:{id}"),
                    _ => id.to_string(),
                };
                (
                    t0 + l[0].as_i64().expect("s"),
                    l[1].as_str().expect("a").to_string(),
                    n[usize::try_from(l[2].as_u64().expect("n")).expect("n")].as_str().expect("n").to_string(),
                    did,
                )
            })
            .collect()
    }

    fn publisher() -> (Publisher, broadcast::Receiver<Arc<Frame>>, FeedHandle) {
        let feed = PublicFeed::new(FeedSettings::default());
        let summary: FeedHandle = Arc::default();
        let (rx, _, _, _) = feed.subscribe();
        (Publisher::new(feed, Arc::clone(&summary)), rx, summary)
    }

    fn drain(rx: &mut broadcast::Receiver<Arc<Frame>>) -> Vec<(String, Value)> {
        let mut out = Vec::new();
        while let Ok(f) = rx.try_recv() {
            out.push(data(&f.bytes));
        }
        out
    }

    /// T-P17's release rule and T-P18's clock: each line at the first 100 ms
    /// tick at or after `entry_ms + 500`, in Redis time; a line read after
    /// its tick goes at the next one and is counted late.
    #[test]
    fn released_by_entry_time_on_the_redis_grid() {
        let (mut p, mut rx, _) = publisher();
        // Redis is 3 s ahead of this host: ticks and seconds follow Redis.
        p.absorb(Msg::Offset(3000));
        let base = 1_800_000_000_000;
        p.tick(base);
        p.absorb(Msg::Entries(vec![
            decoded(base + 1, vec![Kind::Like], Some(line(base + 1, "app.bsky.feed.like", "did:plc:a"))),
            decoded(base + 400, vec![Kind::Like], Some(line(base + 400, "app.bsky.feed.like", "did:plc:b"))),
        ]));
        p.tick(base + 500);
        assert!(p.feed.released().is_empty(), "nothing before its tick");
        p.tick(base + 600);
        p.tick(base + 900);
        let released = p.feed.released();
        assert_eq!(released.iter().map(|(ms, at, _)| (*ms - base, *at - base)).collect::<Vec<_>>(), vec![(1, 600), (400, 900)]);
        // Read after its tick: released at the next one.
        p.absorb(Msg::Entries(vec![decoded(base + 100, vec![Kind::Like], Some(line(base + 100, "app.bsky.feed.like", "did:plc:c")))]));
        p.tick(base + 1000);
        assert_eq!(p.feed.released().last().map(|(ms, at, _)| (*ms - base, *at - base)), Some((100, 1000)));
        let frames = drain(&mut rx);
        let lines: Vec<_> = frames.iter().filter(|(n, _)| n == "lines").flat_map(|(_, v)| lines_of(v)).collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(p.offset, 3000);
    }

    /// T-P18: `counts` has exactly its fields; a second's arrivals; an entry
    /// for a second already published goes to the totals once, through
    /// `late`.
    #[test]
    fn counts_and_late_entries() {
        let (mut p, mut rx, summary) = publisher();
        let base = 1_800_000_000_000;
        p.tick(base);
        // 3 likes written and 5 refused are 8 arrivals: the reader never
        // sees an outcome.
        p.absorb(Msg::Entries((0..8).map(|i| decoded(base + 1000 + i, vec![Kind::Like], None)).collect()));
        let now = local_ms();
        p.absorb(Msg::Input { at_ms: now - 1000, entries_added: 0 });
        p.absorb(Msg::Input { at_ms: now, entries_added: 4500 });
        p.tick(base + 2500);
        let counts: Vec<Value> = drain(&mut rx).into_iter().filter(|(n, _)| n == "counts").map(|(_, v)| v).collect();
        let second = counts.iter().find(|v| v["t"] == (base + 1000) / 1000).expect("the second");
        let mut keys: Vec<&str> = second.as_object().expect("object").keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["arrivals", "e", "events", "input", "t", "totals"]);
        assert_eq!(second["arrivals"], serde_json::json!({"like": 8}));
        assert_eq!(second["input"], 4500.0);
        // Late: a second already out.
        p.absorb(Msg::Entries(vec![decoded(base + 1500, vec![Kind::Like, Kind::Post], None)]));
        assert_eq!(p.feed.late_ops(), 2);
        p.tick(base + 3500);
        let s = summary.load();
        assert_eq!(s.totals, vec![("post".to_string(), 1), ("like".to_string(), 9)]);
        assert_eq!(s.input_per_sec, Some(4500.0));
    }

    /// T-P5: a relay's figure is the lower edge of its median's bucket, over
    /// one second's commits; an hour-old marker does not move it.
    #[test]
    fn relay_median() {
        let mut h = Hist::default();
        for i in 0..300 {
            h.add(2000 + i % 41);
        }
        h.add(3_600_000);
        assert_eq!(h.median(), Some(Delay::Ms(2000)));
        let mut h = Hist::default();
        h.add(90_500);
        assert_eq!(h.median(), Some(Delay::Ms(90_000)));
        let mut h = Hist::default();
        h.add(700_000);
        assert_eq!(h.median(), Some(Delay::OverTenMinutes));
        assert_eq!(Hist::default().median(), None);
    }

    /// T-P19: a tick of 1,000 lines with 600-byte fields is split, none
    /// dropped, no event over the bound; a tick across a minute and a
    /// backlog over three minutes decode to the right times.
    #[test]
    fn split_and_times() {
        let (mut p, mut rx, _) = publisher();
        let base = 1_800_000_000_000;
        p.tick(base);
        let fat = "x".repeat(600);
        let entries: Vec<Decoded> = (0..1000)
            .map(|i| decoded(base + 1, vec![Kind::Other], Some(line(base + 1, &format!("a.b.{fat}"), &format!("did:plc:{i}{fat}")))))
            .collect();
        p.absorb(Msg::Entries(entries));
        p.tick(base + 600);
        let frames: Vec<Arc<Frame>> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let lines: Vec<&Arc<Frame>> = frames.iter().filter(|f| data(&f.bytes).0 == "lines").collect();
        assert!(lines.len() > 1, "split");
        let mut all = 0;
        for f in &lines {
            assert!(f.bytes.len() <= 256 * 1024, "{}", f.bytes.len());
            all += lines_of(&data(&f.bytes).1).len();
        }
        assert_eq!(all, 1000);
        assert_eq!(lines.last().map(|f| f.c), Some(1000));

        // Across a minute.
        let minute = 1_800_000_060_000 - 400;
        let (mut p, mut rx, _) = publisher();
        p.tick(minute - 1000);
        p.absorb(Msg::Entries(vec![
            decoded(minute, vec![Kind::Like], Some(line(minute, "app.bsky.feed.like", "did:plc:a"))),
            decoded(minute + 600, vec![Kind::Like], Some(line(minute + 600, "app.bsky.feed.like", "did:web:b.example"))),
        ]));
        p.tick(minute + 1100);
        let got: Vec<_> = drain(&mut rx).into_iter().filter(|(n, _)| n == "lines").flat_map(|(_, v)| lines_of(&v)).collect();
        assert_eq!(got.iter().map(|l| l.0).collect::<Vec<_>>(), vec![minute / 1000, (minute + 600) / 1000]);
        assert_eq!(got[1].3, "did:web:b.example");
        // A backlog over three minutes.
        let (mut p, _rx, _) = publisher();
        p.tick(base);
        let at = [base + 1000, base + 61_000, base + 121_000, base + 181_000];
        p.absorb(Msg::Entries(at.iter().map(|ms| decoded(*ms, vec![], Some(line(*ms, "#identity", "did:plc:x")))).collect()));
        p.tick(base + 182_000);
        let (_, backlog, _, c) = p.feed.subscribe();
        let (_, v) = data(&backlog);
        assert_eq!(c, 4);
        assert_eq!(lines_of(&v).iter().map(|l| l.0).collect::<Vec<_>>(), at.iter().map(|ms| ms / 1000).collect::<Vec<_>>());
        assert_eq!(v["boot"], p.feed.boot());
    }

    /// T-P19: `backlog` holds the newest 20 published lines; a new process
    /// has another `boot`.
    #[test]
    fn backlog_is_the_newest_twenty() {
        let (mut p, _rx, _) = publisher();
        let base = 1_800_000_000_000;
        p.tick(base);
        p.absorb(Msg::Entries((0..80).map(|i| decoded(base + i, vec![], Some(line(base + i, "#handle", &format!("did:plc:{i}"))))).collect()));
        p.tick(base + 1000);
        let (_, backlog, _, c) = p.feed.subscribe();
        let (_, v) = data(&backlog);
        let dids: Vec<String> = lines_of(&v).into_iter().map(|l| l.3).collect();
        assert_eq!(dids.len(), 20);
        assert_eq!(dids.first().map(String::as_str), Some("did:plc:60"));
        assert_eq!(dids.last().map(String::as_str), Some("did:plc:79"));
        assert_eq!((c, v["c"].as_u64()), (80, Some(80)));
        assert_ne!(PublicFeed::new(FeedSettings::default()).boot(), p.feed.boot());
    }

    /// The `skipped` span: framed after every pre-skip line; `from` is the
    /// last line's time, or the previous anchor's with none since.
    #[test]
    fn skipped_after_its_lines() {
        let (mut p, mut rx, _) = publisher();
        let base = 1_800_000_000_000;
        p.tick(base);
        p.absorb(Msg::Entries(vec![decoded(base + 10, vec![], Some(line(base + 10, "#handle", "did:plc:a")))]));
        p.absorb(Msg::Skipped(Skip { to_ms: base + 40_000, why: super::super::reader::Why::Behind, prev_anchor_ms: base - 5 }));
        p.absorb(Msg::Entries(vec![decoded(base + 40_000, vec![], Some(line(base + 40_000, "#handle", "did:plc:b")))]));
        p.tick(base + 40_500);
        let names: Vec<(String, Value)> = drain(&mut rx).into_iter().filter(|(n, _)| n != "counts").collect();
        assert_eq!(names.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), vec!["lines", "skipped", "lines"]);
        assert_eq!((names[1].1["from"].as_i64(), names[1].1["to"].as_i64(), names[1].1["why"].as_str()), (Some(base + 10), Some(base + 40_000), Some("behind")));
        // A second skip with no line since: from the previous anchor.
        p.absorb(Msg::Skipped(Skip { to_ms: base + 90_000, why: super::super::reader::Why::Reset, prev_anchor_ms: base + 40_000 }));
        p.tick(base + 91_000);
        let skipped: Vec<Value> = drain(&mut rx).into_iter().filter(|(n, _)| n == "skipped").map(|(_, v)| v).collect();
        assert_eq!(skipped[0]["from"].as_i64(), Some(base + 40_000));
        let (_, backlog, _, _) = p.feed.subscribe();
        let (_, v) = data(&backlog);
        assert_eq!((v["skipped"]["why"].as_str(), v["skipped_since_start"].as_bool()), (Some("reset"), Some(true)));
    }

    /// Nothing is framed while nobody watches; `c` and the ring run anyway.
    #[test]
    fn framed_only_while_watched() {
        let feed = PublicFeed::new(FeedSettings::default());
        let mut p = Publisher::new(feed.clone(), Arc::default());
        let base = 1_800_000_000_000;
        p.tick(base);
        p.absorb(Msg::Entries(vec![decoded(base, vec![], Some(line(base, "#handle", "did:plc:a")))]));
        p.tick(base + 2000);
        assert_eq!((feed.framed(), feed.c()), (0, 1));
    }
}
