//! `GET /live` and its viewers (public-page design §6.5, §6.6): anyone may
//! watch, within a cap set for bandwidth, since every viewer gets every
//! event.
//!
//! - **Who may open it.** With `Sec-Fetch-Site`, only `same-origin`,
//!   `cors`, `empty`; without it, an `Origin` that is present must be this
//!   instance's. A client with neither (curl) is admitted within the caps.
//! - **Caps.** `live_max_viewers` in all, 2 per client address (an IPv6
//!   /48 as one). At the cap, the stream first opened longest ago is ended
//!   (`rotated`) if that was over 10 minutes ago; otherwise the open is
//!   refused with `end`/`busy`, as a `200`. The same page id replaces its own
//!   stream. Reservations (after a renewal, or a `slow`) hold a slot.
//! - **One pump per viewer** keeps reading the broadcast whatever the socket
//!   does and forwards into a body channel of 8 frames, full at 6, so the
//!   last two places always hold `gap` and `end`. A viewer that falls behind
//!   is ended with a `gap` of the lines it lost and `end`/`slow`; nobody else
//!   is slowed, and nothing is ever sampled.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ORIGIN};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Instant;

use super::PageState;
use super::address_key;
use super::live::{Frame, PublicFeed};

/// The stream's `Cache-Control`, as the admin stream's.
pub const STREAM_CACHE: &str = "no-store, no-transform";

/// The viewers' settings, shortened in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewerSettings {
    pub max: usize,
    pub per_address: usize,
    /// At the cap, a stream first opened longer ago than this is rotated.
    pub rotate_after: Duration,
    pub renew: Duration,
    pub renew_reserve: Duration,
    pub slow_reserve: Duration,
    pub keepalive: Duration,
    /// The body channel, and the places kept for the terminal frames.
    pub body: usize,
    pub kept: usize,
}

impl ViewerSettings {
    /// The design's values with `max` viewers.
    #[must_use]
    pub fn with_max(max: usize) -> Self {
        Self {
            max,
            per_address: 2,
            rotate_after: Duration::from_secs(600),
            renew: Duration::from_secs(1800),
            renew_reserve: Duration::from_secs(10),
            slow_reserve: Duration::from_secs(45),
            keepalive: Duration::from_secs(15),
            body: 8,
            kept: 2,
        }
    }
}

struct Open {
    addr: IpAddr,
    first_open: Instant,
    id: u64,
    end: watch::Sender<Option<&'static str>>,
}

struct Reserved {
    addr: IpAddr,
    first_open: Instant,
    until: Instant,
}

#[derive(Default)]
struct Registry {
    open: HashMap<String, Open>,
    reserved: HashMap<String, Reserved>,
    next_id: u64,
}

/// The public viewers: a registry of their own, apart from the admin one.
pub struct Viewers {
    reg: Mutex<Registry>,
    pub s: ViewerSettings,
}

/// An admitted stream.
pub struct Admitted {
    pub id: u64,
    pub first_open: Instant,
    pub end: watch::Receiver<Option<&'static str>>,
}

impl Viewers {
    #[must_use]
    pub fn new(s: ViewerSettings) -> Self {
        Self { reg: Mutex::default(), s }
    }

    fn reg(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.reg.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Open and reserved streams.
    #[must_use]
    pub fn held(&self) -> (usize, usize) {
        let r = self.reg();
        (r.open.len(), r.reserved.len())
    }

    /// Admit `page` from `addr`, or `None` (busy).
    pub fn admit(&self, page: &str, addr: IpAddr) -> Option<Admitted> {
        let key = address_key(addr);
        let now = Instant::now();
        let mut r = self.reg();
        r.reserved.retain(|_, v| v.until > now);
        r.next_id += 1;
        let id = r.next_id;
        let (tx, rx) = watch::channel(None);
        if let Some(old) = r.open.remove(page) {
            let _ = old.end.send(Some("replaced"));
            let first_open = old.first_open;
            r.open.insert(page.to_string(), Open { addr: key, first_open, id, end: tx });
            return Some(Admitted { id, first_open, end: rx });
        }
        if let Some(res) = r.reserved.remove(page) {
            r.open.insert(page.to_string(), Open { addr: key, first_open: res.first_open, id, end: tx });
            return Some(Admitted { id, first_open: res.first_open, end: rx });
        }
        let from_here = r.open.values().filter(|o| o.addr == key).count() + r.reserved.values().filter(|v| v.addr == key).count();
        if from_here >= self.s.per_address {
            return None;
        }
        if r.open.len() + r.reserved.len() >= self.s.max {
            let oldest = r.open.iter().min_by_key(|(_, o)| o.first_open).map(|(p, o)| (p.clone(), o.first_open));
            match oldest {
                Some((p, first)) if now.duration_since(first) > self.s.rotate_after => {
                    if let Some(o) = r.open.remove(&p) {
                        let _ = o.end.send(Some("rotated"));
                    }
                }
                _ => return None,
            }
        }
        r.open.insert(page.to_string(), Open { addr: key, first_open: now, id, end: tx });
        Some(Admitted { id, first_open: now, end: rx })
    }

    /// The stream `id` of `page` is gone; its slot is reserved for
    /// `reserve` when given, keeping its first-open time.
    pub fn leave(&self, page: &str, id: u64, reserve: Option<Duration>) {
        let mut r = self.reg();
        if r.open.get(page).is_some_and(|o| o.id == id)
            && let Some(o) = r.open.remove(page)
            && let Some(d) = reserve
        {
            r.reserved.insert(page.to_string(), Reserved { addr: o.addr, first_open: o.first_open, until: Instant::now() + d });
        }
    }
}

/// `/live`'s state.
#[derive(Clone)]
pub struct LiveState {
    pub feed: PublicFeed,
    pub viewers: Arc<Viewers>,
    pub page: PageState,
    pub hostname: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct LiveQuery {
    pub page: Option<String>,
}

/// Fetch Metadata and `Origin` (§6.6).
#[must_use]
pub fn allowed(headers: &HeaderMap, hostname: &str) -> bool {
    let h = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(site) = h("sec-fetch-site") {
        return site == "same-origin" && h("sec-fetch-mode") == Some("cors") && h("sec-fetch-dest") == Some("empty");
    }
    match headers.get(ORIGIN) {
        None => true,
        Some(o) => o.to_str().is_ok_and(|o| o == format!("https://{hostname}")),
    }
}

fn stream_response(body: Body) -> Response {
    let mut response = (StatusCode::OK, body).into_response();
    let h = response.headers_mut();
    h.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    h.insert(CACHE_CONTROL, HeaderValue::from_static(STREAM_CACHE));
    h.insert(HeaderName::from_static("x-accel-buffering"), HeaderValue::from_static("no"));
    response
}

/// A refused open still answers `200`: a retry, `end`/`busy`, and close.
fn busy() -> Response {
    stream_response(Body::from("retry: 30000\n\nevent: end\ndata: {\"reason\":\"busy\"}\n\n"))
}

/// `GET /live`.
pub async fn open(State(l): State<LiveState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<LiveQuery>) -> Response {
    if let Err(refused) = l.page.charge(&headers, peer) {
        return refused;
    }
    if !allowed(&headers, &l.hostname) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(page) = q.page.filter(|p| crate::admin::live::stream::valid_page(p)) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(admitted) = l.viewers.admit(&page, l.page.client(&headers, peer)) else {
        return busy();
    };
    // Subscribed before the backlog is read, under one lock.
    let (rx, backlog, _e, c) = l.feed.subscribe();
    let (tx, body_rx) = mpsc::channel::<Bytes>(l.viewers.s.body);
    let _ = tx.try_send(Bytes::from_static(b"retry: 5000\n\n"));
    let _ = tx.try_send(backlog);
    tokio::spawn(pump(l.feed.clone(), Arc::clone(&l.viewers), page, admitted, rx, tx, c));
    let stream = futures::stream::unfold(body_rx, |mut rx| async move { rx.recv().await.map(|b| (Ok::<Bytes, Infallible>(b), rx)) });
    stream_response(Body::from_stream(stream))
}

fn end_frame(reason: &str) -> Bytes {
    super::live::sse("end", &format!(r#"{{"reason":"{reason}"}}"#))
}

/// One viewer's pump: renewal, rotation, the keepalive, shutdown and slow
/// viewers run here, not in the body's poll.
async fn pump(
    feed: PublicFeed,
    viewers: Arc<Viewers>,
    page: String,
    mut admitted: Admitted,
    mut rx: broadcast::Receiver<Arc<Frame>>,
    tx: mpsc::Sender<Bytes>,
    mut last_c: u64,
) {
    let s = viewers.s;
    let mut closing = feed.closing();
    // Every stream ends after `renew`; its first-open time is kept by the
    // reservation.
    let renew_at = Instant::now() + s.renew;
    let mut last_frame = Instant::now();
    // Places beyond these two are for frames; at `body - kept` used, the
    // viewer is behind.
    let behind = |tx: &mpsc::Sender<Bytes>| tx.capacity() <= s.kept;
    let slow = |tx: &mpsc::Sender<Bytes>, last_c: u64| {
        let through = feed.framed_c().max(last_c);
        let gap = super::live::sse("gap", &format!(r#"{{"lines":{},"c":{through}}}"#, through - last_c));
        let _ = tx.try_send(gap);
        let _ = tx.try_send(end_frame("slow"));
    };
    loop {
        if *closing.borrow() {
            let secs = 10 + rand::random::<u64>() % 31;
            let _ = tx.try_send(Bytes::from(format!("retry: {}\n\n", secs * 1000)));
            viewers.leave(&page, admitted.id, None);
            return;
        }
        tokio::select! {
            biased;
            _ = closing.changed() => continue,
            changed = admitted.end.changed() => {
                let reason = if changed.is_ok() { *admitted.end.borrow() } else { Some("replaced") };
                if let Some(reason) = reason {
                    let _ = tx.try_send(end_frame(reason));
                    return;
                }
            }
            () = tokio::time::sleep_until(renew_at) => {
                let _ = tx.try_send(end_frame("renew"));
                viewers.leave(&page, admitted.id, Some(s.renew_reserve));
                return;
            }
            () = tokio::time::sleep_until(last_frame + s.keepalive) => {
                if behind(&tx) {
                    slow(&tx, last_c);
                    viewers.leave(&page, admitted.id, Some(s.slow_reserve));
                    return;
                }
                if tx.try_send(Bytes::from_static(b":keepalive\n\n")).is_err() && tx.is_closed() {
                    viewers.leave(&page, admitted.id, None);
                    return;
                }
                last_frame = Instant::now();
            }
            received = rx.recv() => match received {
                Ok(frame) => {
                    if tx.is_closed() {
                        viewers.leave(&page, admitted.id, None);
                        return;
                    }
                    if behind(&tx) {
                        slow(&tx, last_c);
                        viewers.leave(&page, admitted.id, Some(s.slow_reserve));
                        return;
                    }
                    match tx.try_send(frame.bytes.clone()) {
                        Ok(()) => {
                            last_c = frame.c;
                            last_frame = Instant::now();
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            viewers.leave(&page, admitted.id, None);
                            return;
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            slow(&tx, last_c);
                            viewers.leave(&page, admitted.id, Some(s.slow_reserve));
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    slow(&tx, last_c);
                    viewers.leave(&page, admitted.id, Some(s.slow_reserve));
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    viewers.leave(&page, admitted.id, None);
                    return;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::public_page::live::FeedSettings;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("ip")
    }

    fn quick(max: usize) -> ViewerSettings {
        ViewerSettings { rotate_after: Duration::from_millis(200), ..ViewerSettings::with_max(max) }
    }

    /// T-P20: 2 per address (an IPv6 /48 as one), refusing and ending no
    /// one; the same page id replaces its own stream.
    #[test]
    fn per_address_and_replacement() {
        let v = Viewers::new(quick(50));
        let a = v.admit("p1", ip("2001:db8:1:1::1")).expect("first");
        let _b = v.admit("p2", ip("2001:db8:1:2::1")).expect("second");
        assert!(v.admit("p3", ip("2001:db8:1:ffff::1")).is_none(), "a third from one /48");
        assert!(a.end.borrow().is_none() && v.held() == (2, 0), "nobody ended");
        assert!(v.admit("p3", ip("2001:db8:2::1")).is_some(), "another /48");
        let again = v.admit("p1", ip("2001:db8:1:1::1")).expect("replaces");
        assert_eq!(*a.end.borrow(), Some("replaced"));
        assert_eq!(again.first_open, a.first_open);
        assert_eq!(v.held(), (3, 0));
    }

    /// T-P20: at the cap, busy unless the oldest stream opened over the
    /// rotation age ago, which then ends `rotated`; a renewal keeps its
    /// first-open time and its reconnect is admitted at the cap.
    #[test]
    fn the_cap() {
        let v = Viewers::new(quick(2));
        let a = v.admit("p1", ip("198.51.100.1")).expect("a");
        let _b = v.admit("p2", ip("198.51.100.2")).expect("b");
        assert!(v.admit("p3", ip("198.51.100.3")).is_none(), "busy");
        assert!(a.end.borrow().is_none());
        std::thread::sleep(Duration::from_millis(250));
        let _c = v.admit("p3", ip("198.51.100.3")).expect("rotation");
        assert_eq!(*a.end.borrow(), Some("rotated"));
        // A renewal: the slot reserved, the first-open time kept.
        let (open, _) = v.held();
        assert_eq!(open, 2);
        let b2 = v.admit("p2", ip("198.51.100.2")).expect("replace");
        v.leave("p2", b2.id, Some(Duration::from_secs(10)));
        assert_eq!(v.held(), (1, 1));
        assert!(v.admit("p9", ip("198.51.100.9")).is_none(), "the reservation holds the slot");
        let back = v.admit("p2", ip("198.51.100.2")).expect("renewed");
        assert_eq!(back.first_open, b2.first_open);
        // A stale id leaves nothing behind.
        v.leave("p2", b2.id, None);
        assert_eq!(v.held(), (2, 0));
    }

    /// T-P20: Fetch Metadata and `Origin`.
    #[test]
    fn who_may_open() {
        let h = |pairs: &[(&'static str, &'static str)]| {
            let mut m = HeaderMap::new();
            for (k, v) in pairs {
                m.insert(*k, HeaderValue::from_static(v));
            }
            m
        };
        let host = "prism.example.test";
        assert!(allowed(&h(&[("sec-fetch-site", "same-origin"), ("sec-fetch-mode", "cors"), ("sec-fetch-dest", "empty")]), host));
        assert!(!allowed(&h(&[("sec-fetch-site", "cross-site")]), host));
        assert!(!allowed(&h(&[("sec-fetch-site", "same-origin"), ("sec-fetch-mode", "cors"), ("sec-fetch-dest", "image")]), host));
        assert!(!allowed(&h(&[("origin", "https://evil.example")]), host));
        assert!(allowed(&h(&[("origin", "https://prism.example.test")]), host));
        assert!(allowed(&HeaderMap::new(), host), "neither header: a non-browser client");
    }

    /// A viewer pump over a feed whose broadcast the test fills.
    struct Rig {
        feed: PublicFeed,
        viewers: Arc<Viewers>,
        body: mpsc::Receiver<Bytes>,
        task: tokio::task::JoinHandle<()>,
        sender: broadcast::Sender<Arc<Frame>>,
    }

    fn rig(s: ViewerSettings) -> Rig {
        let feed = PublicFeed::new(FeedSettings::default());
        let viewers = Arc::new(Viewers::new(s));
        let admitted = viewers.admit("page", ip("198.51.100.1")).expect("admit");
        let (sender, rx) = broadcast::channel(64);
        let (tx, body) = mpsc::channel(s.body);
        let task = tokio::spawn(pump(feed.clone(), Arc::clone(&viewers), "page".to_string(), admitted, rx, tx, 0));
        Rig { feed, viewers, body, task, sender }
    }

    fn frame(e: u64, c: u64) -> Arc<Frame> {
        Arc::new(Frame { e, c, bytes: Bytes::from(format!("event: lines\ndata: {{\"e\":{e},\"c\":{c}}}\n\n")) })
    }

    /// T-P19, slow viewers: a viewer that stops reading is ended once its
    /// body channel holds 6, with `gap` (in lines, through the newest framed
    /// line) and `end`/`slow` its last two frames, and a 45 s reservation;
    /// the frames all viewers share are the same `Bytes`.
    #[tokio::test]
    async fn a_slow_viewer_gets_gap_then_slow() {
        let mut r = rig(ViewerSettings::with_max(50));
        let mut sent = Vec::new();
        for e in 1..=10 {
            let f = frame(e, e * 10);
            sent.push(Arc::clone(&f));
            let _ = r.sender.send(f);
        }
        tokio::time::timeout(Duration::from_secs(2), &mut r.task).await.expect("pump ends").expect("join");
        let mut got = Vec::new();
        while let Ok(b) = r.body.try_recv() {
            got.push(b);
        }
        assert_eq!(got.len(), 8, "6 frames, then gap and end");
        for (i, b) in got[..6].iter().enumerate() {
            assert_eq!(b.as_ptr(), sent[i].bytes.as_ptr(), "the shared bytes, not a copy");
        }
        let gap = std::str::from_utf8(&got[6]).expect("utf8");
        assert!(gap.starts_with("event: gap\n"), "{gap}");
        // The feed framed nothing itself; the gap counts from the last line
        // delivered (c 60) to the newest framed (none newer here).
        let _ = r.feed.framed_c();
        assert!(gap.contains("\"c\":60") && gap.contains("\"lines\":0"), "{gap}");
        assert_eq!(std::str::from_utf8(&got[7]).expect("utf8"), "event: end\ndata: {\"reason\":\"slow\"}\n\n");
        assert_eq!(r.viewers.held(), (0, 1), "a reservation for the page's return");
    }

    /// T-P19: a closed client frees its slot at once, with no reservation;
    /// with no frame for the keepalive's span, `:keepalive` is written.
    #[tokio::test]
    async fn keepalive_and_a_closed_client() {
        let s = ViewerSettings { keepalive: Duration::from_millis(200), ..ViewerSettings::with_max(50) };
        let mut r = rig(s);
        let b = tokio::time::timeout(Duration::from_secs(2), r.body.recv()).await.expect("keepalive").expect("frame");
        assert_eq!(&b[..], b":keepalive\n\n");
        drop(r.body);
        let _ = r.sender.send(frame(1, 1));
        tokio::time::timeout(Duration::from_secs(2), &mut r.task).await.expect("pump ends").expect("join");
        assert_eq!(r.viewers.held(), (0, 0));
    }

    /// Shutdown: a random `retry` of 10 to 40 s and no `end`.
    #[tokio::test]
    async fn shutdown_sends_retry_and_no_end() {
        let mut r = rig(ViewerSettings::with_max(50));
        r.feed.close();
        tokio::time::timeout(Duration::from_secs(2), &mut r.task).await.expect("pump ends").expect("join");
        let b = r.body.try_recv().expect("retry");
        let text = std::str::from_utf8(&b).expect("utf8");
        let ms: u64 = text.trim().strip_prefix("retry: ").expect("retry").parse().expect("ms");
        assert!((10_000..=40_000).contains(&ms), "{ms}");
        assert!(r.body.try_recv().is_err(), "no end");
    }

    /// Renewal: `end`/`renew`, the slot reserved.
    #[tokio::test]
    async fn renewal() {
        let s = ViewerSettings { renew: Duration::from_millis(200), ..ViewerSettings::with_max(50) };
        let mut r = rig(s);
        tokio::time::timeout(Duration::from_secs(2), &mut r.task).await.expect("pump ends").expect("join");
        let b = r.body.try_recv().expect("end");
        assert_eq!(&b[..], b"event: end\ndata: {\"reason\":\"renew\"}\n\n");
        assert_eq!(r.viewers.held(), (0, 1));
    }
}
