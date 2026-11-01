//! A map that a snapshot can write out as it was at one moment, a run of
//! values at a time, while it goes on changing: what holds the forum's rooms
//! and messages. Reading goes straight to the map; every change goes through
//! here, so that a value about to change is written first, as it was, when
//! the capture in progress has not reached it yet.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::ops::Deref;

use serde::Serialize;

/// How much one part of a capture holds. The parts are written to disk one
/// after another, so the capture never copies what it has already written to
/// make room for more. Tests fill many, to see them come out in order.
const CAPTURE_PART: usize = if cfg!(test) { 64 } else { 1 << 20 };

pub struct Captured<K, V> {
    map: BTreeMap<K, V>,
    capture: Option<Capture<K>>,
}

/// The values as they were when it began, as JSON array elements.
struct Capture<K> {
    /// The last key when the capture began: any key after it is new since.
    /// With none, there is nothing to write.
    bound: Option<K>,
    /// The last key written in order; keys up to it are done.
    cursor: Option<K>,
    /// Keys written out of order because they were about to change, or
    /// noted as new then: the in-order pass skips them.
    early: BTreeSet<K>,
    /// Complete parts not yet handed out, and the one being filled.
    parts: Vec<Vec<u8>>,
    out: Vec<u8>,
    /// Bytes moved from `out` to `parts` so far.
    filled: usize,
    written: usize,
    done: bool,
    broken: bool,
}

impl<K: Ord> Capture<K> {
    fn size(&self) -> usize {
        self.filled + self.out.len()
    }

    fn passed(&self, key: &K) -> bool {
        self.done
            || self.bound.as_ref().is_none_or(|bound| key > bound)
            || self.cursor.as_ref().is_some_and(|c| key <= c)
    }

    fn write(&mut self, value: &impl Serialize) {
        if self.out.len() >= CAPTURE_PART {
            let mut full = std::mem::replace(&mut self.out, Vec::with_capacity(2 * CAPTURE_PART));
            full.shrink_to_fit();
            self.filled += full.len();
            self.parts.push(full);
        }
        let mark = self.out.len();
        if self.written > 0 {
            self.out.push(b',');
        }
        if serde_json::to_writer(&mut self.out, value).is_ok() {
            self.written += 1;
        } else {
            self.out.truncate(mark);
            self.broken = true;
        }
    }
}

impl<K, V> Default for Captured<K, V> {
    fn default() -> Self {
        Captured {
            map: BTreeMap::new(),
            capture: None,
        }
    }
}

impl<K, V> Deref for Captured<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &BTreeMap<K, V> {
        &self.map
    }
}

impl<K: Ord + Clone, V: Serialize> Captured<K, V> {
    /// Before `key` changes: a capture that has not reached it writes it now,
    /// as it was when the capture began, or notes that it did not exist.
    fn preserve(&mut self, key: &K) {
        let Some(capture) = self.capture.as_mut() else {
            return;
        };
        if capture.passed(key) || !capture.early.insert(key.clone()) {
            return;
        }
        if let Some(value) = self.map.get(key) {
            capture.write(value);
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.preserve(key);
        self.map.get_mut(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.preserve(&key);
        self.map.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.preserve(key);
        self.map.remove(key)
    }

    /// Every value, for changing: a capture in progress writes all it has not
    /// reached first.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.capture_step(usize::MAX);
        self.map.values_mut()
    }

    /// Starts a capture of the values as they are now, after `out`, which
    /// holds what comes before them.
    pub fn begin_capture(&mut self, out: Vec<u8>) {
        let bound = self.map.keys().next_back().cloned();
        self.capture = Some(Capture {
            done: bound.is_none(),
            bound,
            cursor: None,
            early: BTreeSet::new(),
            parts: Vec::new(),
            out,
            filled: 0,
            written: 0,
            broken: false,
        });
    }

    /// Writes the next run of values, about `budget` bytes of them. Returns
    /// whether every value is written.
    pub fn capture_step(&mut self, budget: usize) -> bool {
        let Captured { map, capture } = self;
        let Some(capture) = capture.as_mut() else {
            return true;
        };
        if capture.done {
            return true;
        }
        let Some(bound) = capture.bound.clone() else {
            capture.done = true;
            return true;
        };
        let start = capture.size();
        let lower = match &capture.cursor {
            Some(cursor) => Bound::Excluded(cursor.clone()),
            None => Bound::Unbounded,
        };
        let mut run = map.range((lower, Bound::Included(bound)));
        let mut last = None;
        while capture.size() - start < budget {
            let Some((key, value)) = run.next() else {
                capture.done = true;
                break;
            };
            last = Some(key);
            if !capture.early.remove(key) {
                capture.write(value);
            }
        }
        if let Some(key) = last {
            capture.cursor = Some(key.clone());
        }
        capture.done
    }

    /// The parts of the capture ready to be written out, and whether it is
    /// over: once every value is written, the rest of it comes too, and the
    /// capture ends.
    pub fn drain_capture(&mut self) -> Result<(Vec<Vec<u8>>, bool), String> {
        let Some(capture) = self.capture.as_mut() else {
            return Ok((Vec::new(), true));
        };
        if !capture.done {
            return Ok((std::mem::take(&mut capture.parts), false));
        }
        self.end_capture().map(|parts| (parts, true))
    }

    /// Ends the capture, writing whatever it has not reached, and returns
    /// what of it was not handed out yet.
    pub fn end_capture(&mut self) -> Result<Vec<Vec<u8>>, String> {
        self.capture_step(usize::MAX);
        let Some(capture) = self.capture.take() else {
            return Ok(Vec::new());
        };
        if capture.broken {
            return Err("a value could not be serialized".into());
        }
        let mut parts = capture.parts;
        parts.push(capture.out);
        Ok(parts)
    }

    pub fn abandon_capture(&mut self) {
        self.capture = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values carry their key, as messages and rooms do.
    type Values = Captured<u32, (u32, u64)>;

    fn read(parts: Vec<Vec<u8>>) -> BTreeMap<u32, (u32, u64)> {
        let mut json = parts.concat();
        json.push(b']');
        let values: Vec<(u32, u64)> = serde_json::from_slice(&json).unwrap();
        let read: BTreeMap<_, _> = values.iter().map(|v| (v.0, *v)).collect();
        assert_eq!(read.len(), values.len(), "each value is written once");
        read
    }

    #[test]
    fn a_capture_holds_the_values_as_they_were_when_it_began() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };
        for _ in 0..200 {
            let mut values = Values::default();
            for _ in 0..next(60) {
                let key = next(100) as u32;
                values.insert(key, (key, 0));
            }
            let before: BTreeMap<_, _> = values.iter().map(|(k, v)| (*k, *v)).collect();
            values.begin_capture(b"[".to_vec());
            let mut parts = Vec::new();
            let mut over = false;
            for round in 1..=next(80) {
                let key = next(120) as u32;
                match next(5) {
                    0 => {
                        values.insert(key, (key, round));
                    }
                    1 => {
                        values.remove(&key);
                    }
                    2 => {
                        if let Some(value) = values.get_mut(&key) {
                            value.1 = round;
                        }
                    }
                    3 if next(10) == 0 => values.values_mut().for_each(|v| v.1 += 1),
                    _ => {
                        values.capture_step([0, 1, 8, 64, 4096][next(5) as usize] as usize);
                    }
                }
                if !over {
                    let (drained, ended) = values.drain_capture().unwrap();
                    parts.extend(drained);
                    over = ended;
                }
            }
            parts.extend(values.end_capture().unwrap());
            assert_eq!(read(parts), before);
        }
    }

    #[test]
    fn an_empty_map_captures_as_nothing_and_new_values_stay_out() {
        let mut values = Values::default();
        values.begin_capture(b"[".to_vec());
        values.insert(1, (1, 1));
        let (parts, over) = values.drain_capture().unwrap();
        assert!(over);
        assert!(read(parts).is_empty());
        assert_eq!(values.len(), 1);
    }
}
