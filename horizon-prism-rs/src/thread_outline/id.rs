//! Stream ids as two numbers (thread outline design §3.1, R3-14): Redis ids
//! are `"<ms>-<seq>"`, and they order as `(ms, seq)`, never as text.

use std::fmt;

/// A Redis stream id, ordered as `(ms, seq)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    #[must_use]
    pub const fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Parse `"<ms>-<seq>"`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let (ms, seq) = s.split_once('-')?;
        Some(Self { ms: ms.parse().ok()?, seq: seq.parse().ok()? })
    }

    /// The id just before this one: `first_id − 1`. With `seq = 0` it is
    /// `(ms − 1, u64::MAX)` (N9). `0-0` has none and stays `0-0`.
    #[must_use]
    pub fn prev(self) -> Self {
        match (self.ms, self.seq) {
            (0, 0) => self,
            (ms, 0) => Self { ms: ms - 1, seq: u64::MAX },
            (ms, seq) => Self { ms, seq: seq - 1 },
        }
    }

    /// The id just after this one: `through + 1`. At `seq = u64::MAX` it
    /// carries into `(ms + 1, 0)` (R4-4).
    #[must_use]
    pub fn next(self) -> Self {
        if self.seq == u64::MAX {
            Self { ms: self.ms.saturating_add(1), seq: 0 }
        } else {
            Self { ms: self.ms, seq: self.seq + 1 }
        }
    }

    /// The millisecond part as an `i64` (stream time), saturating.
    #[must_use]
    pub fn ms_i64(self) -> i64 {
        i64::try_from(self.ms).unwrap_or(i64::MAX)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T-O4: ids compare as `(ms, seq)`, so `…-10` is after `…-9`; the
    /// `− 1` and `+ 1` arithmetic borrows and carries.
    #[test]
    fn ids_compare_as_two_numbers() {
        let nine = StreamId::parse("1700000000000-9").expect("id");
        let ten = StreamId::parse("1700000000000-10").expect("id");
        assert!(ten > nine);
        assert!("1700000000000-10" < "1700000000000-9", "as text they would order the other way");
        assert!(StreamId::parse("1700000000001-0").expect("id") > ten);
        assert_eq!(ten.to_string(), "1700000000000-10");
        assert_eq!(StreamId::new(5, 0).prev(), StreamId::new(4, u64::MAX));
        assert_eq!(StreamId::new(5, 3).prev(), StreamId::new(5, 2));
        assert_eq!(StreamId::new(0, 0).prev(), StreamId::new(0, 0));
        assert_eq!(StreamId::new(4, u64::MAX).next(), StreamId::new(5, 0));
        assert_eq!(StreamId::new(5, 0).prev().next(), StreamId::new(5, 0));
        assert_eq!(StreamId::new(4, u64::MAX).to_string(), "4-18446744073709551615");
        for bad in ["", "5", "a-1", "1-b", "-1"] {
            assert_eq!(StreamId::parse(bad), None, "{bad}");
        }
    }
}
