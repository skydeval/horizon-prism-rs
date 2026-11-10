//! The consumer's backlog against the stream's bound, from the lag
//! sampler's readings: the input and processed rates over the last minute,
//! and the time until entries the consumer has not acked would be trimmed.
//!
//! The stream is bounded by its relay readers' `MAXLEN`, which Redis does
//! not report; its length stands in for it, so while the stream still fills
//! the projection errs early. Pending (delivered, unacked) entries count:
//! they are trimmed too. A time to trim counts only once two consecutive
//! ticks give one; a reading without a lag is unknown, and so is a
//! projection older than two minutes.

use std::collections::VecDeque;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// Metric: seconds until unread entries would be trimmed at the current
/// rates; +Inf while the consumer keeps up.
pub const METRIC_TIME_TO_TRIM: &str = "firehose_time_to_trim_seconds";
/// The span the rates are taken over.
pub const RATE_SPAN: Duration = Duration::from_secs(60);
/// A projection older than this is unknown.
pub const STALE_AFTER: Duration = Duration::from_secs(120);

/// One projection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Projection {
    pub at: Instant,
    /// Entries a second added to the stream, and booked by the consumer.
    pub input_per_sec: f64,
    pub processed_per_sec: f64,
    pub lag: u64,
    pub pending: u64,
    pub length: u64,
    /// Seconds until the unread entries would be trimmed; `None` while the
    /// consumer processes at least what comes in.
    pub time_to_trim: Option<f64>,
}

impl Projection {
    /// The backlog in seconds of input.
    #[must_use]
    pub fn backlog_secs(&self) -> Option<f64> {
        #[allow(clippy::cast_precision_loss)]
        let behind = (self.lag + self.pending) as f64;
        (self.input_per_sec > 0.0).then(|| behind / self.input_per_sec)
    }

    /// Whether the time to trim is under `margin`.
    #[must_use]
    pub fn trims_within(&self, margin: Duration) -> bool {
        self.time_to_trim.is_some_and(|t| t < margin.as_secs_f64())
    }
}

/// What one sampler tick read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reading {
    /// The stream's `entries-added`.
    pub added: Option<u64>,
    pub length: Option<u64>,
    /// The group's lag; `None` when Redis gave none.
    pub lag: Option<u64>,
    pub pending: Option<u64>,
    /// Entries the consumer booked since the start.
    pub processed: u64,
}

/// The sampler's state between ticks.
#[derive(Debug, Default)]
pub struct Projector {
    ring: VecDeque<(Instant, u64, u64)>,
    /// The previous tick gave a time to trim.
    previous_trims: bool,
}

impl Projector {
    /// One tick: the projection, or `None` when unknown.
    pub fn tick(&mut self, now: Instant, r: Reading) -> Option<Projection> {
        if let Some(added) = r.added {
            // A recreated stream starts the rates over.
            if self.ring.back().is_some_and(|(_, a, p)| added < *a || r.processed < *p) {
                self.ring.clear();
            }
            self.ring.push_back((now, added, r.processed));
            while self.ring.get(1).is_some_and(|(t, ..)| now.duration_since(*t) >= RATE_SPAN) {
                self.ring.pop_front();
            }
        }
        let (Some(length), Some(lag), Some(pending)) = (r.length, r.lag, r.pending) else {
            self.previous_trims = false;
            return None;
        };
        let (&(t0, a0, p0), &(t1, a1, p1)) = (self.ring.front()?, self.ring.back()?);
        let dt = t1.duration_since(t0).as_secs_f64();
        if dt < RATE_SPAN.as_secs_f64() / 2.0 {
            self.previous_trims = false;
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        let (input, processed) = ((a1 - a0) as f64 / dt, (p1 - p0) as f64 / dt);
        #[allow(clippy::cast_precision_loss)]
        let room = length.saturating_sub(lag + pending) as f64;
        let raw = (input > processed).then(|| room / (input - processed));
        // Two consecutive ticks must agree before a time to trim counts.
        let time_to_trim = raw.filter(|_| self.previous_trims);
        self.previous_trims = raw.is_some();
        Some(Projection { at: now, input_per_sec: input, processed_per_sec: processed, lag, pending, length, time_to_trim })
    }
}

/// The process's latest projection, published by the lag sampler.
#[derive(Debug, Default)]
pub struct Published(Mutex<Option<Projection>>);

impl Published {
    /// Publish `p` (or that the projection is unknown).
    pub fn set(&self, p: Option<Projection>) {
        if let Some(p) = &p {
            metrics::gauge!(METRIC_TIME_TO_TRIM).set(p.time_to_trim.unwrap_or(f64::INFINITY));
        }
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = p;
    }

    /// The latest projection, unless it is unknown or stale.
    #[must_use]
    pub fn get(&self) -> Option<Projection> {
        self.get_at(Instant::now())
    }

    #[must_use]
    pub fn get_at(&self, now: Instant) -> Option<Projection> {
        let p = *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        p.filter(|p| now.saturating_duration_since(p.at) < STALE_AFTER)
    }
}

/// Live ingest's running totals, for a trial's per-minute samples: entries
/// booked, of them duplicates, shed and applied in a transaction; the
/// writer lanes' busy time and their time in rounds; records the crawl
/// applied.
#[derive(Debug, Default)]
pub struct Totals {
    pub processed: std::sync::atomic::AtomicU64,
    pub deduped: std::sync::atomic::AtomicU64,
    pub shed: std::sync::atomic::AtomicU64,
    pub applied: std::sync::atomic::AtomicU64,
    pub lanes_busy_us: std::sync::atomic::AtomicU64,
    pub lanes_capacity_us: std::sync::atomic::AtomicU64,
    pub crawl_records: std::sync::atomic::AtomicU64,
}

/// One reading of [`Totals`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TotalsNow {
    pub processed: u64,
    pub deduped: u64,
    pub shed: u64,
    pub applied: u64,
    pub lanes_busy_us: u64,
    pub lanes_capacity_us: u64,
    pub crawl_records: u64,
}

impl Totals {
    #[must_use]
    pub fn now(&self) -> TotalsNow {
        use std::sync::atomic::Ordering::Relaxed;
        TotalsNow {
            processed: self.processed.load(Relaxed),
            deduped: self.deduped.load(Relaxed),
            shed: self.shed.load(Relaxed),
            applied: self.applied.load(Relaxed),
            lanes_busy_us: self.lanes_busy_us.load(Relaxed),
            lanes_capacity_us: self.lanes_capacity_us.load(Relaxed),
            crawl_records: self.crawl_records.load(Relaxed),
        }
    }
}

/// The process's ingest totals.
#[must_use]
pub fn totals() -> &'static Totals {
    static T: LazyLock<Totals> = LazyLock::new(Totals::default);
    &T
}

/// The process's projection.
#[must_use]
pub fn published() -> &'static Published {
    static P: LazyLock<Published> = LazyLock::new(Published::default);
    &P
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(added: u64, processed: u64, lag: Option<u64>) -> Reading {
        Reading { added: Some(added), length: Some(100_000), lag, pending: Some(1_000), processed }
    }

    #[test]
    fn the_time_to_trim_needs_two_ticks_a_lag_and_a_minute_of_rates() {
        let t = Instant::now();
        let mut p = Projector::default();
        // 1 000 in, 600 processed a second, from a 10 s tick.
        assert_eq!(p.tick(t, reading(0, 0, Some(9_000))), None, "no rates yet");
        let second = p.tick(t + Duration::from_secs(40), reading(40_000, 24_000, Some(25_000))).expect("rates");
        assert_eq!(second.time_to_trim, None, "one tick does not count");
        assert!((second.input_per_sec - 1000.0).abs() < 1e-6 && (second.processed_per_sec - 600.0).abs() < 1e-6);
        let third = p.tick(t + Duration::from_secs(50), reading(50_000, 30_000, Some(29_000))).expect("rates");
        // Room: 100 000 - (29 000 + 1 000) = 70 000; 400 a second closing.
        let ttt = third.time_to_trim.expect("two ticks agree");
        assert!((ttt - 70_000.0 / 400.0).abs() < 1.0, "{ttt}");
        assert!(third.trims_within(Duration::from_secs(600)));
        assert!((third.backlog_secs().expect("backlog") - 30.0).abs() < 0.5);
        // A nil lag makes it unknown, and resets the agreement.
        assert_eq!(p.tick(t + Duration::from_secs(60), reading(60_000, 36_000, None)), None);
        assert_eq!(p.tick(t + Duration::from_secs(70), reading(70_000, 42_000, Some(30_000))).expect("rates").time_to_trim, None);
        // Keeping up: no time to trim.
        let mut q = Projector::default();
        q.tick(t, reading(0, 0, Some(0)));
        q.tick(t + Duration::from_secs(40), reading(40_000, 40_000, Some(0)));
        assert_eq!(q.tick(t + Duration::from_secs(50), reading(50_000, 50_000, Some(0))).expect("rates").time_to_trim, None);
    }

    #[test]
    fn a_stale_projection_is_unknown() {
        let t = Instant::now();
        let shared = Published::default();
        let proj = Projection { at: t, input_per_sec: 1.0, processed_per_sec: 1.0, lag: 0, pending: 0, length: 0, time_to_trim: None };
        shared.set(Some(proj));
        assert!(shared.get_at(t + Duration::from_secs(60)).is_some());
        assert!(shared.get_at(t + Duration::from_secs(121)).is_none());
    }
}
