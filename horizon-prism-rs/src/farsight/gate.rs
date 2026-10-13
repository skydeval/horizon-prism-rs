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

    /// Whether the read breaker is open now.
    #[must_use]
    pub fn is_open(&self, now: Instant) -> bool {
        self.breaker.is_open(now)
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
}
