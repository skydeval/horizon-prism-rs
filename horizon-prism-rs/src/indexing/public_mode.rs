//! The public-indexing mode (public-indexing design §3.5).
//!
//! The mode lives in `ScopeSets.public`, behind the scope state's lock, so a
//! writer that holds the read guard sees one mode for its whole transaction.
//! The off rule (§5) applies in Draining, Off and Held; reads that need
//! complete public data ask whether the mode is On.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::config::IndexingConfig;

/// One of the five modes (§3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PublicMode {
    /// Everything is stored and served, as before the switch existed.
    On,
    /// Everything is stored; public endpoints wait for the fill (§8).
    Filling,
    /// The off rule applies and the sweep deletes public data (§7).
    Draining,
    /// The off rule applies; nothing is left to delete.
    Off,
    /// The stored state did not validate: the off rule applies and nothing
    /// is deleted or filled until a command writes a valid state.
    Held,
}

impl PublicMode {
    /// The off rule (§5) applies: only the needed set (§4) is stored, and
    /// no notification or feed item is written.
    #[must_use]
    pub fn off_rule(self) -> bool {
        matches!(self, Self::Draining | Self::Off | Self::Held)
    }

    /// Public data is complete. Filling is not On.
    #[must_use]
    pub fn is_on(self) -> bool {
        self == Self::On
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::On => "on",
            Self::Filling => "filling",
            Self::Draining => "draining",
            Self::Off => "off",
            Self::Held => "held",
        }
    }

    /// The value of the lock-free mirror the endpoint gate reads.
    #[must_use]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// The inverse of [`Self::as_u8`]. An unknown value reads as Held,
    /// which serves nothing public and deletes nothing.
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::On,
            1 => Self::Filling,
            2 => Self::Draining,
            3 => Self::Off,
            _ => Self::Held,
        }
    }
}

/// `ScopeSets.public`: the mode, the generation of its phase (§3.4), and
/// the start of the most recent fill (§8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicState {
    pub mode: PublicMode,
    pub generation: u64,
    pub last_fill_since: Option<DateTime<Utc>>,
}

/// `(On, 0, None)`: today's behaviour. Startup overwrites it before any
/// writer runs, so production never sees it; tests that build the scope
/// sets by default keep their behaviour (§3.5).
impl Default for PublicState {
    fn default() -> Self {
        Self { mode: PublicMode::On, generation: 0, last_fill_since: None }
    }
}

/// The settings of the switch's background work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicModeSettings {
    /// `indexing.fill_max_wait_hours`: how long a fill waits on its last
    /// jobs before it ends anyway (§8.5).
    pub fill_max_wait: Duration,
}

impl PublicModeSettings {
    #[must_use]
    pub fn from_config(cfg: &IndexingConfig) -> Self {
        Self { fill_max_wait: Duration::from_secs(u64::from(cfg.fill_max_wait_hours) * 3600) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [PublicMode; 5] = [PublicMode::On, PublicMode::Filling, PublicMode::Draining, PublicMode::Off, PublicMode::Held];

    #[test]
    fn the_off_rule_applies_in_the_off_modes_only() {
        let off: Vec<PublicMode> = ALL.into_iter().filter(|m| m.off_rule()).collect();
        assert_eq!(off, [PublicMode::Draining, PublicMode::Off, PublicMode::Held]);
        let on: Vec<PublicMode> = ALL.into_iter().filter(|m| m.is_on()).collect();
        assert_eq!(on, [PublicMode::On]);
    }

    #[test]
    fn the_mirror_round_trips() {
        for m in ALL {
            assert_eq!(PublicMode::from_u8(m.as_u8()), m);
        }
        assert_eq!(PublicMode::from_u8(200), PublicMode::Held);
    }

    /// T-1: the in-memory default is today's behaviour, and the fill bound
    /// defaults to a day.
    #[test]
    fn defaults() {
        assert_eq!(PublicState::default(), PublicState { mode: PublicMode::On, generation: 0, last_fill_since: None });
        let settings = PublicModeSettings::from_config(&IndexingConfig::default());
        assert_eq!(settings.fill_max_wait, Duration::from_secs(24 * 3600));
    }
}
