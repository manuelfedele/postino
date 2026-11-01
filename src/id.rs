use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Entry id in `<milliseconds>-<sequence>` form. Ids are
/// totally ordered and strictly increasing, even when the wall clock steps back.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id {
    pub ms: u64,
    pub seq: u64,
}

impl Id {
    pub const MAX: Id = Id {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    /// The id for a new entry: the clock, or the next id after `last` when the
    /// clock has not passed it. `None` once `last` is the largest id.
    pub fn next_after(last: Id, now_ms: u64) -> Option<Id> {
        if now_ms > last.ms {
            Some(Id { ms: now_ms, seq: 0 })
        } else {
            Id::after(last)
        }
    }

    /// The smallest id greater than `id`; the largest id has none.
    pub fn after(id: Id) -> Option<Id> {
        match id.seq.checked_add(1) {
            Some(seq) => Some(Id { ms: id.ms, seq }),
            None => id.ms.checked_add(1).map(|ms| Id { ms, seq: 0 }),
        }
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl FromStr for Id {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ms, seq) = s.split_once('-').unwrap_or((s, "0"));
        match (ms.parse(), seq.parse()) {
            (Ok(ms), Ok(seq)) => Ok(Id { ms, seq }),
            _ => Err(format!("invalid id: {s}")),
        }
    }
}

impl Serialize for Id {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_monotonic_when_the_clock_goes_back() {
        let first = Id::next_after(Id::default(), 1000).unwrap();
        let second = Id::next_after(first, 1000).unwrap();
        let third = Id::next_after(second, 900).unwrap();
        assert!(first < second && second < third);
        assert_eq!(third.to_string(), "1000-2");
        assert_eq!("1000-2".parse::<Id>().unwrap(), third);
        assert!("abc".parse::<Id>().is_err());
    }

    #[test]
    fn the_sequence_carries_into_the_next_millisecond() {
        let full = Id {
            ms: 7,
            seq: u64::MAX,
        };
        assert_eq!(Id::after(full), Some(Id { ms: 8, seq: 0 }));
        assert_eq!(Id::next_after(full, 3), Some(Id { ms: 8, seq: 0 }));
        assert_eq!(Id::after(Id::MAX), None);
        assert_eq!(Id::next_after(Id::MAX, 3), None);
    }
}
