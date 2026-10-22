//! Where a private read's time goes (fast-reads design §7.2):
//! `private_read_stage_seconds{endpoint, stage}`.
//!
//! - `gate`: `prepare_private` (verification, the bucket, the member check),
//!   from handler entry.
//! - `reads`: the wall time during which at least one record read through
//!   the pipeline (or a head round) was in flight: head checks, the row,
//!   bind and decode. Concurrent reads count once.
//! - `context`: the reply context of feed items, read after the page's
//!   items (§5.2 B-2), with its decoration. Reads inside it count here, not
//!   in `reads`. Recorded only by a request that ran it.
//! - `rest`: the remainder of the unfloored time: listing, prefilter,
//!   hydration, decoration.
//!
//! The stages sum to the request's `private_read_unfloored_seconds`. They
//! are kept in a task-local for the handler body, so a read deep in a
//! helper needs no extra argument; outside a handler nothing is recorded.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::handlers::{METRIC_EMPTY_PAGE, METRIC_STAGES};

tokio::task_local! {
    static STAGES: Arc<Stages>;
}

/// One request's stage times.
#[derive(Debug, Default)]
pub(super) struct Stages {
    reads_ns: AtomicU64,
    context_ns: AtomicU64,
    /// Reads in flight, and since when the first of them was.
    open: Mutex<(usize, Option<Instant>)>,
    in_context: AtomicBool,
    context_ran: AtomicBool,
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

impl Stages {
    fn open(&self) -> std::sync::MutexGuard<'_, (usize, Option<Instant>)> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn enter_read(&self) {
        let mut open = self.open();
        if open.0 == 0 {
            open.1 = Some(Instant::now());
        }
        open.0 += 1;
    }

    fn leave_read(&self) {
        let mut open = self.open();
        open.0 = open.0.saturating_sub(1);
        if open.0 == 0
            && let Some(since) = open.1.take()
            && !self.in_context.load(Ordering::Relaxed)
        {
            self.reads_ns.fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
        }
    }

    /// The `reads` stage so far.
    pub(super) fn reads(&self) -> Duration {
        Duration::from_nanos(self.reads_ns.load(Ordering::Relaxed))
    }

    /// The `context` stage, when it ran.
    pub(super) fn context(&self) -> Option<Duration> {
        self.context_ran
            .load(Ordering::Relaxed)
            .then(|| Duration::from_nanos(self.context_ns.load(Ordering::Relaxed)))
    }
}

/// Leaves the read on drop, so a read dropped unfinished (a full page stops
/// polling the rest) still closes its interval.
struct ReadGuard(Arc<Stages>);

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.0.leave_read();
    }
}

/// Run `body` with stage timing; returns its output and the times.
pub(super) async fn run<F: Future>(body: F) -> (F::Output, Arc<Stages>) {
    let stages = Arc::new(Stages::default());
    let out = STAGES.scope(Arc::clone(&stages), body).await;
    (out, stages)
}

/// Time `read` in the `reads` stage, from its first poll to its end.
pub(super) async fn reading<F: Future>(read: F) -> F::Output {
    let guard = STAGES
        .try_with(|s| {
            s.enter_read();
            ReadGuard(Arc::clone(s))
        })
        .ok();
    let out = read.await;
    drop(guard);
    out
}

/// Time `step` as the `context` stage; reads inside it are not `reads`.
pub(super) async fn context<F: Future>(step: F) -> F::Output {
    let stages = STAGES.try_with(Arc::clone).ok();
    if let Some(s) = &stages {
        s.in_context.store(true, Ordering::Relaxed);
    }
    let started = Instant::now();
    let out = step.await;
    if let Some(s) = stages {
        s.in_context.store(false, Ordering::Relaxed);
        s.context_ns.fetch_add(nanos(started.elapsed()), Ordering::Relaxed);
        s.context_ran.store(true, Ordering::Relaxed);
    }
    out
}

/// Record the stages of a request whose unfloored time is `unfloored` and
/// whose gate took `gate`: `rest` is what the others leave.
pub(super) fn record(endpoint: &'static str, unfloored: Duration, gate: Duration, stages: &Stages) {
    let reads = stages.reads();
    let context = stages.context();
    let rest = unfloored.saturating_sub(gate + reads + context.unwrap_or_default());
    let mut observed = vec![("gate", gate), ("reads", reads), ("rest", rest)];
    observed.extend(context.map(|c| ("context", c)));
    for (stage, d) in observed {
        metrics::histogram!(METRIC_STAGES, "endpoint" => endpoint, "stage" => stage).record(d.as_secs_f64());
    }
}

/// Count a paged read that returned no items and a cursor (§5.2, C-11).
pub(super) fn count_empty_page(endpoint: &'static str, empty_with_cursor: bool) {
    if empty_with_cursor {
        metrics::counter!(METRIC_EMPTY_PAGE, "endpoint" => endpoint).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Overlapping reads count once; context reads count as context.
    #[tokio::test]
    async fn reads_count_their_union_and_context_apart() {
        let pause = |ms| tokio::time::sleep(Duration::from_millis(ms));
        let ((), s) = run(async {
            let a = reading(pause(200));
            let b = reading(pause(200));
            futures::join!(a, b);
            context(async { reading(pause(20)).await }).await;
        })
        .await;
        // Two 200 ms reads side by side: about 200 ms, never their 400 ms sum.
        let reads = s.reads();
        assert!(reads >= Duration::from_millis(200) && reads < Duration::from_millis(350), "{reads:?}");
        let ctx = s.context().expect("ran");
        assert!(ctx >= Duration::from_millis(20), "{ctx:?}");
        // Outside a run nothing is kept, and nothing panics.
        reading(pause(1)).await;
        context(async {}).await;
    }

    /// C-11: only a page with no items and a cursor is counted.
    #[test]
    fn an_empty_page_has_a_cursor_and_no_items() {
        use super::super::emit::PrivateResponse;
        assert!(PrivateResponse::feed(Vec::new(), Some("c".into())).is_empty_page());
        assert!(!PrivateResponse::feed(Vec::new(), None).is_empty_page());
        assert!(PrivateResponse::notifications(Vec::new(), Some("c".into()), None).is_empty_page());
        assert!(!PrivateResponse::posts(Vec::new()).is_empty_page());
        assert!(!PrivateResponse::unread_count(0).is_empty_page());
    }

    #[tokio::test]
    async fn a_dropped_read_closes_its_interval() {
        let ((), s) = run(async {
            let slow = Box::pin(reading(tokio::time::sleep(Duration::from_secs(60))));
            let _ = tokio::time::timeout(Duration::from_millis(10), slow).await;
        })
        .await;
        assert!(s.reads() >= Duration::from_millis(10), "{:?}", s.reads());
        assert_eq!(s.open().0, 0);
        assert_eq!(s.context(), None);
    }
}
