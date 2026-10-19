//! The live feed's stream side (admin-ui-live-feed design §3.4, §4.4,
//! §5.5): the hub that owns the writer's tap, the registry of open
//! streams and their slots, the handle cache, the closing flag, and the
//! ring of recent totals Home shows with JavaScript off. The tap itself is
//! `crate::firehose::feed`; the SSE handler is [`stream`].

pub mod stream;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::firehose::feed::{FeedTap, Totals};

/// Pages one session may stream at once (§3.4).
pub const PAGES_PER_SESSION: usize = 2;
/// Open streams in all (§3.4); beyond, `429`. Never evicts.
pub const MAX_STREAMS: usize = 8;
/// Lines per second to one browser, per ticker (C-F4; Changed by skydeval
/// 2026-10-19): the arriving and the indexed ticker each get this many, so
/// one viewer gets at most twice it. The page shows far fewer: it paces.
pub const LINES_PER_SEC: u32 = 25;
/// Slots of the JavaScript-off ring, one per [`RING_EVERY`] (§5.5).
pub const RING_SLOTS: usize = 12;
/// How often the ring samples the counters.
pub const RING_EVERY: Duration = Duration::from_secs(5);
/// Gauge: open streams.
pub const METRIC_STREAMS: &str = "admin_live_streams";

/// Rows each column of the live figures shows, whatever is counted
/// (second pass), so the block never changes height.
pub const STAT_SLOTS: usize = 3;

/// When counting began, short, as Home's "since" line shows it:
/// `Oct 19, 19:02 UTC`.
#[must_use]
pub fn since_words(t: DateTime<Utc>) -> String {
    t.format("%b %-d, %H:%M UTC").to_string()
}

/// The stream's timings (§10), shortened by tests.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    /// Session re-check while open.
    pub check: Duration,
    /// Every stream ends with `renew` after this long.
    pub renew: Duration,
    /// A renewed page's slot stays reserved this long.
    pub reserve: Duration,
    /// Lines are sent in batches this often.
    pub batch: Duration,
    /// `counts` events this often.
    pub counts: Duration,
    /// `: keepalive` comments this often.
    pub keepalive: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            check: Duration::from_secs(60),
            renew: Duration::from_secs(30 * 60),
            reserve: Duration::from_secs(10),
            batch: Duration::from_millis(250),
            counts: Duration::from_secs(1),
            keepalive: Duration::from_secs(15),
        }
    }
}

/// An open stream's registry entry.
struct Open {
    id: u64,
    session: [u8; 32],
    page: String,
    opened: Instant,
    wanted: u64,
    end: watch::Sender<Option<&'static str>>,
}

/// A renewed page's held slot.
struct Reserved {
    session: [u8; 32],
    page: String,
    until: Instant,
}

#[derive(Default)]
struct Registry {
    open: Vec<Open>,
    reserved: Vec<Reserved>,
}

struct Inner {
    tap: FeedTap,
    registry: Mutex<Registry>,
    closing: watch::Sender<bool>,
    /// Stored handles by DID, misses too (§4.4).
    handles: moka::future::Cache<String, Option<String>>,
    /// Totals sampled every [`RING_EVERY`], oldest first.
    ring: Mutex<VecDeque<(Instant, Totals)>>,
    timing: Timing,
    next_id: AtomicU64,
}

/// The hub. Cheap to clone.
#[derive(Clone)]
pub struct Hub(Arc<Inner>);

/// A refused open: the global cap is reached (§3.4).
#[derive(Debug, PartialEq, Eq)]
pub struct Full;

/// A stream's slot, freed when dropped (§3.4): the response body is
/// dropped when the client goes, after `end`, or at shutdown.
pub struct Slot {
    hub: Hub,
    id: u64,
    session: [u8; 32],
    page: String,
    /// Set before an `end`/`renew`: the page's slot is then reserved.
    pub renewing: bool,
    /// Ends this stream with a reason: a replacement by the same page or a
    /// third page of the session.
    pub end: watch::Receiver<Option<&'static str>>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut reg = self.hub.registry();
        if let Some(i) = reg.open.iter().position(|o| o.id == self.id) {
            reg.open.remove(i);
            if self.renewing {
                reg.reserved.push(Reserved {
                    session: self.session,
                    page: self.page.clone(),
                    until: Instant::now() + self.hub.0.timing.reserve,
                });
            }
        }
        self.hub.publish(&reg);
    }
}

impl Hub {
    /// A hub whose tap shows snippets when `snippets` (C-F1).
    #[must_use]
    pub fn new(snippets: bool) -> Self {
        Self::with_timing(snippets, Timing::default())
    }

    /// [`Hub::new`] with other timings (tests).
    #[must_use]
    pub fn with_timing(snippets: bool, timing: Timing) -> Self {
        let (closing, _) = watch::channel(false);
        Self(Arc::new(Inner {
            tap: FeedTap::new(snippets),
            registry: Mutex::new(Registry::default()),
            closing,
            handles: moka::future::Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(600))
                .build(),
            ring: Mutex::new(VecDeque::new()),
            timing,
            next_id: AtomicU64::new(1),
        }))
    }

    /// The writer's tap (§4.1).
    #[must_use]
    pub fn tap(&self) -> FeedTap {
        self.0.tap.clone()
    }

    #[must_use]
    pub fn timing(&self) -> Timing {
        self.0.timing
    }

    /// End every stream with `shutting_down` (§3.5).
    pub fn close(&self) {
        self.0.closing.send_replace(true);
    }

    /// The closing flag.
    #[must_use]
    pub fn closing(&self) -> watch::Receiver<bool> {
        self.0.closing.subscribe()
    }

    /// Open streams now.
    #[must_use]
    pub fn open_streams(&self) -> usize {
        self.registry().open.len()
    }

    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.0.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The tap's wanted mask and the gauge from the registry.
    fn publish(&self, reg: &Registry) {
        let wanted = reg.open.iter().fold(0, |acc, o| acc | o.wanted);
        self.0.tap.set_wanted(wanted);
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(METRIC_STREAMS).set(reg.open.len() as f64);
    }

    /// Take a slot for `page` of `session` (§3.4): the same page replaces
    /// its own stream; a reserved renewal takes its slot back; a third page
    /// of the session ends the session's oldest; past [`MAX_STREAMS`] the
    /// open is refused and nobody is ended.
    ///
    /// # Errors
    ///
    /// [`Full`] at the global cap.
    pub fn admit(&self, session: [u8; 32], page: &str, wanted: u64) -> Result<Slot, Full> {
        let now = Instant::now();
        let mut reg = self.registry();
        reg.reserved.retain(|r| r.until > now);
        let same = |s: &[u8; 32], p: &str| *s == session && p == page;
        if let Some(i) = reg.open.iter().position(|o| same(&o.session, &o.page)) {
            let old = reg.open.remove(i);
            old.end.send_replace(Some("replaced"));
        } else if let Some(i) = reg.reserved.iter().position(|r| same(&r.session, &r.page)) {
            reg.reserved.remove(i);
        } else if reg.open.iter().filter(|o| o.session == session).count() >= PAGES_PER_SESSION {
            let oldest = reg
                .open
                .iter()
                .enumerate()
                .filter(|(_, o)| o.session == session)
                .min_by_key(|(_, o)| o.opened)
                .map(|(i, _)| i);
            if let Some(i) = oldest {
                let old = reg.open.remove(i);
                old.end.send_replace(Some("replaced"));
            }
        } else if reg.open.len() + reg.reserved.len() >= MAX_STREAMS {
            return Err(Full);
        }
        let id = self.0.next_id.fetch_add(1, Ordering::Relaxed);
        let (end, end_rx) = watch::channel(None);
        reg.open.push(Open { id, session, page: page.to_string(), opened: now, wanted, end });
        self.publish(&reg);
        Ok(Slot { hub: self.clone(), id, session, page: page.to_string(), renewing: false, end: end_rx })
    }

    /// Stored handles of `dids` from the cache; the rest are `None`.
    pub async fn cached_handles(&self, dids: &[&str]) -> (Vec<Option<Option<String>>>, Vec<String>) {
        let mut found = Vec::with_capacity(dids.len());
        let mut misses = Vec::new();
        for did in dids {
            let hit = self.0.handles.get(*did).await;
            if hit.is_none() && !misses.iter().any(|m: &String| m == did) {
                misses.push((*did).to_string());
            }
            found.push(hit);
        }
        (found, misses)
    }

    /// Remember a looked-up handle (or its absence).
    pub async fn remember_handle(&self, did: String, handle: Option<String>) {
        self.0.handles.insert(did, handle).await;
    }

    /// Sample the counters into the ring now.
    pub fn sample(&self) {
        let mut ring = self.0.ring.lock().unwrap_or_else(PoisonError::into_inner);
        ring.push_back((Instant::now(), self.0.tap.totals()));
        while ring.len() > RING_SLOTS + 1 {
            ring.pop_front();
        }
    }

    /// The last minute's change and its length (§5.5), from the ring; `None`
    /// before two samples.
    #[must_use]
    pub fn last_minute(&self) -> Option<(Totals, Duration)> {
        let ring = self.0.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let (first, last) = (ring.front()?, ring.back()?);
        let span = last.0.duration_since(first.0);
        (!span.is_zero()).then(|| (last.1.since(&first.1), span))
    }

    /// When counting began.
    #[must_use]
    pub fn started(&self) -> DateTime<Utc> {
        self.0.tap.started()
    }

    /// The ring's sampler, for as long as `serve` runs (§5.5). It runs
    /// whether or not anyone watches.
    #[must_use]
    pub fn spawn_sampler(&self) -> JoinHandle<()> {
        let hub = self.clone();
        tokio::spawn(async move {
            loop {
                hub.sample();
                tokio::time::sleep(RING_EVERY).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::firehose::feed::WANT_ALL;

    fn session(n: u8) -> [u8; 32] {
        [n; 32]
    }

    /// T-F6 (registry part): per-page replacement, two pages a session, a
    /// global cap that refuses and never evicts, freed slots, and the
    /// renewal's reservation.
    #[tokio::test]
    async fn t_f6_slots() {
        let hub = Hub::with_timing(false, Timing { reserve: Duration::from_millis(300), ..Timing::default() });
        let a1 = hub.admit(session(1), "p1", WANT_ALL).expect("open");
        let a2 = hub.admit(session(1), "p2", WANT_ALL).expect("open");
        // The same page replaces its own stream, and only it.
        let a1b = hub.admit(session(1), "p1", WANT_ALL).expect("reconnect");
        assert_eq!(*a1.end.borrow(), Some("replaced"));
        assert_eq!(*a2.end.borrow(), None);
        drop(a1);
        assert_eq!(hub.open_streams(), 2, "the replaced slot's drop frees nothing twice");
        // A third page ends the session's oldest page.
        let a3 = hub.admit(session(1), "p3", WANT_ALL).expect("third page");
        assert_eq!(*a2.end.borrow(), Some("replaced"));
        assert_eq!(*a1b.end.borrow(), None);
        drop(a2);
        // Fill the global cap with other sessions.
        let mut others: Vec<Slot> = (2..8).map(|n| hub.admit(session(n), "p", WANT_ALL).expect("open")).collect();
        assert_eq!(hub.open_streams(), MAX_STREAMS);
        assert_eq!(hub.admit(session(99), "p", WANT_ALL).err(), Some(Full));
        assert!(others.iter().all(|s| s.end.borrow().is_none()), "nobody was ended");
        // A closed stream frees its slot.
        drop(others.pop());
        let ninth = hub.admit(session(99), "p", WANT_ALL).expect("a freed slot");
        // A renewing page keeps its slot reserved for a moment.
        let mut renewing = ninth;
        renewing.renewing = true;
        drop(renewing);
        assert_eq!(hub.admit(session(100), "p", WANT_ALL).err(), Some(Full), "the slot is reserved");
        let back = hub.admit(session(99), "p", WANT_ALL).expect("the renewed page takes it back");
        drop(back);
        drop(a3);
        drop(a1b);
        drop(others);
        assert_eq!(hub.open_streams(), 0);
        assert_eq!(hub.tap().receivers(), 0);
    }

    #[tokio::test]
    async fn the_reservation_expires() {
        let hub = Hub::with_timing(false, Timing { reserve: Duration::from_millis(100), ..Timing::default() });
        let slots: Vec<Slot> = (0..MAX_STREAMS).map(|n| hub.admit(session(u8::try_from(n).expect("n")), "p", 1)).collect::<Result<_, _>>().expect("open");
        let mut slots = slots;
        let mut last = slots.pop().expect("slot");
        last.renewing = true;
        drop(last);
        assert!(hub.admit(session(50), "p", 1).is_err());
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(hub.admit(session(50), "p", 1).is_ok(), "past the reservation the slot is free");
    }
}
