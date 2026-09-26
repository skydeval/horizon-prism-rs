//! Per-PDS-host request budgets (design-v2 §6.11.3): three independent token
//! buckets per host, so no class can starve or delay another (#158).
//!
//! | Class | Rate | When empty |
//! |---|---|---|
//! | [`Class::Probe`] | `ceil(tracked_on_host / (0.75 × head_max_age_secs))`, capped at `sync.max_probe_rate_per_host` | waits |
//! | [`Class::ReadFetch`] | `sync.read_fetch_rate_per_host` | never waits: the caller misses |
//! | [`Class::Backfill`] | `backfill.per_pds_rate_per_sec` | waits |

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A budget class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    Probe,
    ReadFetch,
    Backfill,
}

/// One token bucket. Capacity equals the per-second rate (at least 1).
#[derive(Debug, Clone, Copy)]
struct Bucket {
    rate: f64,
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn new(rate: f64, now: Instant) -> Self {
        let rate = rate.max(f64::MIN_POSITIVE);
        Self {
            rate,
            tokens: capacity(rate),
            last: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(capacity(self.rate));
        self.last = now;
    }

    /// Take a token, or return how long until one is available.
    fn take(&mut self, now: Instant) -> Result<(), Duration> {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - self.tokens) / self.rate))
        }
    }
}

fn capacity(rate: f64) -> f64 {
    rate.max(1.0)
}

/// The configured rates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BudgetRates {
    /// `sync.max_probe_rate_per_host`: the cap on the probe rate.
    pub max_probe_per_host: f64,
    /// `sync.head_max_age_secs`.
    pub head_max_age_secs: f64,
    /// `sync.read_fetch_rate_per_host`.
    pub read_fetch_per_host: f64,
    /// `backfill.per_pds_rate_per_sec`.
    pub backfill_per_host: f64,
}

impl BudgetRates {
    #[must_use]
    pub fn from_app(cfg: &crate::config::AppConfig) -> Self {
        let sync = &cfg.file.sync;
        #[allow(clippy::cast_precision_loss)]
        Self {
            max_probe_per_host: f64::from(sync.max_probe_rate_per_host),
            head_max_age_secs: sync.head_max_age_secs as f64,
            read_fetch_per_host: f64::from(sync.read_fetch_rate_per_host),
            backfill_per_host: f64::from(cfg.file.backfill.per_pds_rate_per_sec),
        }
    }
}

/// The probe rate for a host with `tracked` DIDs: every head is refreshed
/// within 0.75 × the maximum head age, capped.
#[must_use]
pub fn probe_rate(tracked: usize, rates: &BudgetRates) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let needed = (tracked as f64 / (0.75 * rates.head_max_age_secs.max(1.0))).ceil();
    needed.clamp(1.0, rates.max_probe_per_host.max(1.0))
}

/// The shared per-host tracker. Cheap to clone.
#[derive(Clone)]
pub struct HostBudgets {
    rates: BudgetRates,
    buckets: Arc<Mutex<HashMap<(String, Class), Bucket>>>,
    probe_rates: Arc<Mutex<HashMap<String, f64>>>,
}

impl HostBudgets {
    #[must_use]
    pub fn new(rates: BudgetRates) -> Self {
        Self {
            rates,
            buckets: Arc::new(Mutex::new(HashMap::new())),
            probe_rates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub fn rates(&self) -> &BudgetRates {
        &self.rates
    }

    /// The lower-cased host of a PDS endpoint URL (the bucket key).
    #[must_use]
    pub fn host_of(endpoint: &str) -> String {
        url::Url::parse(endpoint)
            .ok()
            .and_then(|u| u.host_str().map(str::to_ascii_lowercase))
            .unwrap_or_else(|| endpoint.to_ascii_lowercase())
    }

    /// Set a host's probe rate from its tracked-DID count.
    pub fn set_probe_tracked(&self, host: &str, tracked: usize) {
        let rate = probe_rate(tracked, &self.rates);
        let changed = {
            let mut rates = self.probe_rates.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            rates.insert(host.to_string(), rate) != Some(rate)
        };
        if changed {
            let mut buckets = self.buckets.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(b) = buckets.get_mut(&(host.to_string(), Class::Probe)) {
                b.rate = rate;
                b.tokens = b.tokens.min(capacity(rate));
            }
        }
    }

    fn rate(&self, host: &str, class: Class) -> f64 {
        match class {
            Class::Probe => self
                .probe_rates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(host)
                .copied()
                .unwrap_or(1.0),
            Class::ReadFetch => self.rates.read_fetch_per_host,
            Class::Backfill => self.rates.backfill_per_host,
        }
    }

    fn take(&self, host: &str, class: Class) -> Result<(), Duration> {
        let rate = self.rate(host, class);
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        buckets
            .entry((host.to_string(), class))
            .or_insert_with(|| Bucket::new(rate, now))
            .take(now)
    }

    /// Take a token now or not at all (the read path never waits).
    #[must_use]
    pub fn try_acquire(&self, host: &str, class: Class) -> bool {
        self.take(host, class).is_ok()
    }

    /// Wait for a token (background classes).
    pub async fn acquire(&self, host: &str, class: Class) {
        loop {
            match self.take(host, class) {
                Ok(()) => return,
                Err(wait) => tokio::time::sleep(wait).await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rates() -> BudgetRates {
        BudgetRates {
            max_probe_per_host: 25.0,
            head_max_age_secs: 60.0,
            read_fetch_per_host: 20.0,
            backfill_per_host: 5.0,
        }
    }

    #[test]
    fn probe_rate_formula_and_cap() {
        let r = rates();
        // 1000 tracked DIDs, 45 s refresh window: 23 req/s.
        assert!((probe_rate(1000, &r) - 23.0).abs() < f64::EPSILON);
        assert!((probe_rate(10_000, &r) - 25.0).abs() < f64::EPSILON);
        assert!((probe_rate(1, &r) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn classes_and_hosts_are_independent() {
        let b = HostBudgets::new(rates());
        // Drain the read-fetch bucket on one host.
        let mut granted = 0;
        while b.try_acquire("pds.a", Class::ReadFetch) {
            granted += 1;
        }
        assert_eq!(granted, 20);
        assert!(!b.try_acquire("pds.a", Class::ReadFetch), "the read path never waits");
        // The other classes and the other host are untouched.
        assert!(b.try_acquire("pds.a", Class::Backfill));
        assert!(b.try_acquire("pds.a", Class::Probe));
        assert!(b.try_acquire("pds.b", Class::ReadFetch));
    }

    #[tokio::test]
    async fn acquire_waits_for_a_refill() {
        let b = HostBudgets::new(rates());
        for _ in 0..5 {
            b.acquire("pds.a", Class::Backfill).await;
        }
        let start = Instant::now();
        b.acquire("pds.a", Class::Backfill).await;
        let waited = start.elapsed();
        assert!(waited >= Duration::from_millis(150), "{waited:?}");
    }

    #[test]
    fn probe_rate_updates_apply_to_the_bucket() {
        let b = HostBudgets::new(rates());
        b.set_probe_tracked("pds.a", 1000);
        let mut granted = 0;
        while b.try_acquire("pds.a", Class::Probe) {
            granted += 1;
        }
        assert_eq!(granted, 23);
        assert_eq!(HostBudgets::host_of("https://PDS.Example.com:443/"), "pds.example.com");
    }
}
