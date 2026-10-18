//! The read gate (design-v3 §5.14.17.2): the breaker of the read call
//! together with the state of **the run**, under one lock.
//!
//! The run is every outcome of a `checkBlocks` call since Farsight's last
//! answer. It is **clean** while every one of them was a service failure:
//! a timeout, a transport error, or a `5xx`.
//!
//! **Farsight is down** when all three hold:
//! 1. it has answered this process at least once;
//! 2. the read breaker is open (a half-open breaker is not);
//! 3. the run is clean.
//!
//! Only then may the fallback be asked. So no single failed call brings
//! the fallback, a configuration error never does (a Farsight that has
//! never answered is never "down"), and one outcome that is neither an
//! answer nor a service failure (a `401`, a `429`, a `400`) keeps it off
//! until Farsight answers again.

use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use crate::constellation::Class;
use crate::constellation::cascade::{Admission, SharedBreaker};

/// Gauge `farsight_down`: 1 while Farsight is down (design-v4 §14.2).
pub const METRIC_DOWN: &str = "farsight_down";
/// Gauge `farsight_fallback_withheld{reason}`: 1 while the read breaker is
/// open and Farsight is not down, so the fallback stays off. `reason` is
/// `never_answered` or `unclean` (design-v4 §14.2).
pub const METRIC_WITHHELD: &str = "farsight_fallback_withheld";

/// Why an open read breaker does not bring the fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Withheld {
    /// Farsight has not answered this process: there is no run.
    NeverAnswered,
    /// Something other than a service failure came since the last answer.
    Unclean,
}

impl Withheld {
    pub const ALL: [Self; 2] = [Self::NeverAnswered, Self::Unclean];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NeverAnswered => "never_answered",
            Self::Unclean => "unclean",
        }
    }
}

/// The gate's state for its gauges, at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateState {
    pub down: bool,
    /// `Some` while the breaker is open and Farsight is not down.
    pub withheld: Option<Withheld>,
}

/// What one call's outcome does to the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// A `200` whose body parsed: a new, clean run starts.
    Answer,
    /// A timeout of a call that had its whole time, a transport error, or
    /// a `5xx`: the run stays as it is.
    ServiceFailure,
    /// Anything else that reached Farsight or was refused on the way: the
    /// run is unclean until the next answer.
    Unclean,
    /// Says nothing about whether Farsight works: a call cut short by its
    /// request's budget, an answer over the size cap, or any failure of a
    /// call an anonymous caller could have chosen.
    None,
}

#[derive(Debug)]
struct Run {
    /// Answers since the process started.
    answers: u64,
    clean: bool,
}

/// One read of the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateRead {
    /// Farsight is down.
    pub down: bool,
    /// Answers since the process started, as of this read: a fallback
    /// result is used only if this has not moved by the time it arrives.
    pub answers: u64,
}

#[derive(Debug)]
pub struct ReadGate {
    breaker: SharedBreaker,
    run: Mutex<Run>,
}

impl Default for ReadGate {
    fn default() -> Self {
        Self {
            breaker: SharedBreaker::default(),
            // No run before the first answer; `clean` is read only after.
            run: Mutex::new(Run { answers: 0, clean: true }),
        }
    }
}

impl ReadGate {
    /// Admit one read call, or refuse it (the breaker is open, or its
    /// probe is out). Dropping the admission without recording gives a
    /// probe slot back.
    pub fn admit(&self, now: Instant) -> Option<Admission<'_>> {
        self.breaker.admit(now)
    }

    /// Admit one read call that must never be the half-open probe: a
    /// visitor's (a Thread Lens caller that is not a ready member). Its
    /// failures are not recorded, so as the probe it could only give the
    /// slot back, and visitors would hold the breaker half-open through an
    /// outage: never open, so never "down". A closed breaker admits it;
    /// an open or half-open one refuses it.
    pub fn admit_unless_probe(&self) -> Option<Admission<'_>> {
        self.breaker.admit_unless_probe()
    }

    /// Whether the read breaker is open now.
    #[must_use]
    pub fn is_open(&self, now: Instant) -> bool {
        self.breaker.is_open(now)
    }

    /// Put the read breaker in the half-open state, with no probe out.
    #[cfg(test)]
    pub(crate) fn force_half_open(&self) {
        self.breaker.force_half_open();
    }

    /// Record one call's outcome: on the run, and (with `class`) on the
    /// breaker, both under the run's lock so that a read never sees one
    /// without the other.
    pub fn record(&self, admission: Admission<'_>, class: Option<Class>, effect: Effect, now: Instant) {
        let mut run = self.run.lock().unwrap_or_else(PoisonError::into_inner);
        match effect {
            Effect::Answer => {
                run.answers += 1;
                run.clean = true;
            }
            Effect::Unclean => run.clean = false,
            Effect::ServiceFailure | Effect::None => {}
        }
        match class {
            Some(class) => admission.record(class, now),
            None => drop(admission),
        }
    }

    /// The run as it stands, for the operator: whether Farsight has
    /// answered this process, and whether nothing but service failures
    /// came since its last answer.
    #[must_use]
    pub fn run(&self) -> (bool, bool) {
        let run = self.run.lock().unwrap_or_else(PoisonError::into_inner);
        (run.answers > 0, run.clean)
    }

    /// One read: whether Farsight is down now, and the answer count.
    #[must_use]
    pub fn read(&self, now: Instant) -> GateRead {
        let run = self.run.lock().unwrap_or_else(PoisonError::into_inner);
        GateRead {
            down: run.answers > 0 && run.clean && self.breaker.is_open(now),
            answers: run.answers,
        }
    }

    /// The state the gauges show, read under the run's lock.
    #[must_use]
    pub fn state(&self, now: Instant) -> GateState {
        let run = self.run.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.breaker.is_open(now) {
            return GateState { down: false, withheld: None };
        }
        if run.answers == 0 {
            // Before the first answer there is no run, whatever came since.
            return GateState { down: false, withheld: Some(Withheld::NeverAnswered) };
        }
        if run.clean {
            GateState { down: true, withheld: None }
        } else {
            GateState { down: false, withheld: Some(Withheld::Unclean) }
        }
    }

    /// Set `farsight_down` and both `farsight_fallback_withheld` series
    /// from the gate as it is now. The gauges are computed when sampled,
    /// never set on an outcome: an open period ends by the clock, with no
    /// call and no event (design-v4 §14.2, FB4-21).
    pub fn sample(&self, now: Instant) {
        let state = self.state(now);
        metrics::gauge!(METRIC_DOWN).set(if state.down { 1.0 } else { 0.0 });
        for reason in Withheld::ALL {
            let on = state.withheld == Some(reason);
            metrics::gauge!(METRIC_WITHHELD, "reason" => reason.as_str()).set(if on { 1.0 } else { 0.0 });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(gate: &ReadGate, class: Option<Class>, effect: Effect) -> bool {
        let now = Instant::now();
        match gate.admit(now) {
            Some(admission) => {
                gate.record(admission, class, effect, now);
                true
            }
            None => false,
        }
    }

    fn fail(gate: &ReadGate, times: usize) {
        for _ in 0..times {
            outcome(gate, Some(Class::Transient), Effect::ServiceFailure);
        }
    }

    #[test]
    fn farsight_is_down_only_after_an_answer_and_a_clean_run_of_failures() {
        // Never answered: five failures open the breaker, and it is not
        // "down". A wrong URL or a revoked key never brings the fallback.
        let gate = ReadGate::default();
        fail(&gate, 5);
        assert!(gate.is_open(Instant::now()));
        assert_eq!(gate.read(Instant::now()), GateRead { down: false, answers: 0 });

        // Answered once, then four failures: the breaker is still closed.
        let gate = ReadGate::default();
        assert!(outcome(&gate, Some(Class::Hit), Effect::Answer));
        fail(&gate, 4);
        assert!(!gate.read(Instant::now()).down);
        // The fifth opens it with a clean run: down.
        fail(&gate, 1);
        assert_eq!(gate.read(Instant::now()), GateRead { down: true, answers: 1 });
        // While open nothing is admitted.
        assert!(!outcome(&gate, Some(Class::Hit), Effect::Answer));

        // One outcome that is not a service failure makes the run unclean,
        // even where the breaker records nothing for it (a 404): four
        // timeouts, a 404 and a fifth timeout open the breaker, not "down".
        let gate = ReadGate::default();
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        fail(&gate, 4);
        outcome(&gate, None, Effect::Unclean);
        fail(&gate, 1);
        assert!(gate.is_open(Instant::now()) && !gate.read(Instant::now()).down);

        // An outcome with no effect leaves the run clean.
        let gate = ReadGate::default();
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        outcome(&gate, None, Effect::None);
        fail(&gate, 5);
        assert!(gate.read(Instant::now()).down);
        // An answer starts a new run and counts.
        let gate = ReadGate::default();
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        outcome(&gate, None, Effect::Unclean);
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        fail(&gate, 5);
        assert_eq!(gate.read(Instant::now()), GateRead { down: true, answers: 2 });
    }

    /// A call that must not probe leaves the probe to a call whose outcome
    /// is recorded: with the breaker half-open it is refused, an ordinary
    /// call made at the same time is the probe, and that probe's failure
    /// re-opens the breaker.
    #[test]
    fn a_half_open_gate_refuses_a_call_that_must_not_probe() {
        let gate = ReadGate::default();
        assert!(gate.admit_unless_probe().is_some(), "closed: admitted");
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        fail(&gate, 5);
        assert!(gate.read(Instant::now()).down);
        assert!(gate.admit_unless_probe().is_none(), "open: refused");

        gate.force_half_open();
        let now = Instant::now();
        assert!(gate.admit_unless_probe().is_none(), "half-open: refused");
        let probe = gate.admit(now).expect("the slot was not taken");
        assert!(gate.admit_unless_probe().is_none() && gate.admit(now).is_none());
        gate.record(probe, Some(Class::Transient), Effect::ServiceFailure, now);
        assert!(gate.is_open(now) && gate.read(now).down, "a failed probe re-opens");
    }

    /// `farsight_down` and `farsight_fallback_withheld{reason}` as one
    /// sample of the gate shows them: (down, never_answered, unclean).
    fn gauges(gate: &ReadGate, handle: &metrics_exporter_prometheus::PrometheusHandle) -> (f64, f64, f64) {
        use crate::test_support::series;
        gate.sample(Instant::now());
        let text = handle.render();
        let value = |name: &str, labels: &[&str]| series(&text, name, labels).unwrap_or_else(|| panic!("{name} {labels:?} missing:\n{text}"));
        (
            value(METRIC_DOWN, &[]),
            value(METRIC_WITHHELD, &["reason=\"never_answered\""]),
            value(METRIC_WITHHELD, &["reason=\"unclean\""]),
        )
    }

    /// design-v4 §14.2: the gauges follow the gate. Every series exists
    /// from the first sample, before any call; never-answered and unclean
    /// runs are withheld, a clean run after an answer is down; a half-open
    /// breaker shows neither, with no event in between (FB4-21).
    #[test]
    fn the_gauges_follow_the_gate_when_sampled() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);

        // Before any call: all three series exist, at 0.
        let gate = ReadGate::default();
        assert_eq!(gauges(&gate, &handle), (0.0, 0.0, 0.0));
        // Never answered: five failures open the breaker; withheld.
        fail(&gate, 5);
        assert_eq!(gauges(&gate, &handle), (0.0, 1.0, 0.0));
        // An unclean outcome before the first answer is still "never answered".
        let gate = ReadGate::default();
        outcome(&gate, Some(Class::Transient), Effect::Unclean);
        fail(&gate, 4);
        assert_eq!(gauges(&gate, &handle), (0.0, 1.0, 0.0));
        // Half-open, by the clock alone: both off.
        gate.force_half_open();
        assert_eq!(gauges(&gate, &handle), (0.0, 0.0, 0.0));

        // Answered, then four failures: closed, nothing.
        let gate = ReadGate::default();
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        fail(&gate, 4);
        assert_eq!(gauges(&gate, &handle), (0.0, 0.0, 0.0));
        // The fifth: down.
        fail(&gate, 1);
        assert_eq!(gauges(&gate, &handle), (1.0, 0.0, 0.0));
        // The open period ends: half-open is not down, with no event.
        gate.force_half_open();
        assert_eq!(gauges(&gate, &handle), (0.0, 0.0, 0.0));
        // The probe fails: down again.
        fail(&gate, 1);
        assert_eq!(gauges(&gate, &handle), (1.0, 0.0, 0.0));
        // Half-open, and a probe answered 401: the breaker opens on an
        // unclean run; withheld.
        gate.force_half_open();
        outcome(&gate, Some(Class::Transient), Effect::Unclean);
        assert_eq!(gauges(&gate, &handle), (0.0, 0.0, 1.0));
        // Half-open again and a probe answered: closed, nothing.
        gate.force_half_open();
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        assert_eq!(gauges(&gate, &handle), (0.0, 0.0, 0.0));

        // Four timeouts, a 404 the breaker does not record, one more
        // timeout: open with an unclean run.
        let gate = ReadGate::default();
        outcome(&gate, Some(Class::Hit), Effect::Answer);
        fail(&gate, 4);
        outcome(&gate, None, Effect::Unclean);
        fail(&gate, 1);
        assert_eq!(gauges(&gate, &handle), (0.0, 0.0, 1.0));
    }
}
