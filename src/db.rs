//! The in-memory keyspace.
//!
//! Keys hold strings, lists, hashes, sets, sorted sets, or streams, each with an
//! optional expiry. Expired keys read as absent everywhere; they are removed
//! lazily when touched and by the periodic sweep. Commands live in
//! [`crate::commands`]; this module only holds the data and its invariants.
//!
//! A snapshot of the keyspace is taken while requests go on: a [`Capture`]
//! writes the keys in order, a run at a time, and every change to a key goes
//! through a method here that first writes the key, as it was when the
//! capture began, if the capture has not reached it yet.
//!
//! A large value the keyspace lets go of is freed on a thread of its own:
//! freeing a collection of a million elements takes up to a tenth of a second,
//! which every request would otherwise wait out.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

use crate::id::Id;

/// A sorted-set score with a total order (NaN scores are rejected on input).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Score(pub f64);

impl Eq for Score {}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// A member with its score, in the order a sorted set keeps them.
pub type Ranked = (Score, Arc<str>);

/// Members ordered by (score, member), with an index from member to score.
/// Each member's name is kept once, shared by the two, so that changing a
/// score or removing a member copies no name.
#[derive(Clone, Debug, Default)]
pub struct ZSet {
    pub scores: HashMap<Arc<str>, f64>,
    pub order: BTreeSet<Ranked>,
}

impl ZSet {
    /// Sets a member's score; returns true when the member is new.
    pub fn insert(&mut self, member: &str, score: f64) -> bool {
        // -0 and +0 are the same score.
        let score = if score == 0.0 { 0.0 } else { score };
        if let Some((name, &old)) = self.scores.get_key_value(member) {
            let name = Arc::clone(name);
            self.order.remove(&(Score(old), Arc::clone(&name)));
            self.order.insert((Score(score), Arc::clone(&name)));
            self.scores.insert(name, score);
            return false;
        }
        let name: Arc<str> = Arc::from(member);
        self.order.insert((Score(score), Arc::clone(&name)));
        self.scores.insert(name, score);
        true
    }

    pub fn remove(&mut self, member: &str) -> bool {
        match self.scores.remove_entry(member) {
            Some((name, score)) => {
                self.order.remove(&(Score(score), name));
                true
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Members with a score between `min` and `max`, each given as
    /// `(score, exclusive)`, in score order. Only those members are visited.
    pub fn by_score(
        &self,
        min: (f64, bool),
        max: (f64, bool),
    ) -> impl DoubleEndedIterator<Item = &Ranked> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        // A score's first member sorts with the empty name.
        let first = |score: f64| (Score(score), Arc::<str>::from(""));
        let lower = match min {
            (score, true) if score == f64::INFINITY => None,
            (score, true) => Some(first(score.next_up())),
            (score, false) => Some(first(score)),
        };
        let upper = match max {
            (score, false) if score == f64::INFINITY => Unbounded,
            (score, false) => Excluded(first(score.next_up())),
            (score, true) => Excluded(first(score)),
        };
        // A range that ends before it starts is empty, not a panic.
        lower
            .filter(|lo| match &upper {
                Excluded(hi) => lo < hi,
                _ => true,
            })
            .map(|lo| self.order.range((Included(lo), upper)))
            .into_iter()
            .flatten()
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let (name, &score) = self.scores.get_key_value(member)?;
        Some(self.order.range(..(Score(score), Arc::clone(name))).count())
    }
}

impl Serialize for ZSet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let pairs: Vec<(&str, Stored)> = self
            .order
            .iter()
            .map(|(s, m)| (&**m, Stored(s.0)))
            .collect();
        pairs.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ZSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let pairs = Vec::<(String, Stored)>::deserialize(deserializer)?;
        let mut zset = ZSet::default();
        for (member, Stored(score)) in pairs {
            zset.insert(&member, score);
        }
        Ok(zset)
    }
}

/// A score as stored: a number, or `"inf"` and `"-inf"`, which JSON has no
/// numbers for.
struct Stored(f64);

impl Serialize for Stored {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            f64::INFINITY => serializer.serialize_str("inf"),
            f64::NEG_INFINITY => serializer.serialize_str("-inf"),
            score => serializer.serialize_f64(score),
        }
    }
}

impl<'de> Deserialize<'de> for Stored {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Stored;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a score: a number, \"inf\", or \"-inf\"")
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Stored, E> {
                Ok(Stored(v))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Stored, E> {
                Ok(Stored(v as f64))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Stored, E> {
                Ok(Stored(v as f64))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Stored, E> {
                match v {
                    "inf" | "+inf" => Ok(Stored(f64::INFINITY)),
                    "-inf" => Ok(Stored(f64::NEG_INFINITY)),
                    _ => Err(E::invalid_value(serde::de::Unexpected::Str(v), &self)),
                }
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pending {
    pub consumer: String,
    pub delivered: u64,
    pub count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Consumer {
    pub seen: u64,
    #[serde(default)]
    pub active: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Group {
    pub last_delivered: Id,
    #[serde(default)]
    pub read: u64,
    #[serde(default)]
    pub pending: BTreeMap<Id, Pending>,
    #[serde(default)]
    pub consumers: BTreeMap<String, Consumer>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Stream {
    pub entries: BTreeMap<Id, Vec<(String, String)>>,
    pub last_id: Id,
    #[serde(default)]
    pub added: u64,
    #[serde(default)]
    pub max_deleted: Id,
    #[serde(default)]
    pub groups: BTreeMap<String, Group>,
}

impl Stream {
    pub fn trim_to(&mut self, keep: usize) -> usize {
        let excess = self.entries.len().saturating_sub(keep);
        self.drop_oldest(excess)
    }

    pub fn trim_before(&mut self, min: Id) -> usize {
        let excess = self.entries.range(..min).count();
        self.drop_oldest(excess)
    }

    fn drop_oldest(&mut self, count: usize) -> usize {
        for _ in 0..count {
            if let Some((id, _)) = self.entries.pop_first() {
                self.max_deleted = self.max_deleted.max(id);
            }
        }
        count
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "lowercase")]
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Hash(BTreeMap<String, String>),
    Set(BTreeSet<String>),
    ZSet(ZSet),
    Stream(Stream),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
            Value::Stream(_) => "stream",
        }
    }

    /// Collections are deleted when they become empty; streams are kept.
    /// How many elements it holds, which is what freeing it costs.
    fn elements(&self) -> usize {
        match self {
            Value::String(_) => 1,
            Value::List(l) => l.len(),
            Value::Hash(h) => h.len(),
            Value::Set(s) => s.len(),
            Value::ZSet(z) => z.len(),
            Value::Stream(s) => s.entries.len(),
        }
    }

    pub fn is_empty_collection(&self) -> bool {
        match self {
            Value::List(l) => l.is_empty(),
            Value::Hash(h) => h.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::ZSet(z) => z.is_empty(),
            Value::String(_) | Value::Stream(_) => false,
        }
    }

    /// Approximate memory footprint in bytes, for INFO.
    pub fn footprint(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::List(l) => l.iter().map(|v| v.len() + 24).sum(),
            Value::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 48).sum(),
            Value::Set(s) => s.iter().map(|m| m.len() + 32).sum(),
            Value::ZSet(z) => z.scores.keys().map(|m| m.len() + 64).sum(),
            Value::Stream(s) => s
                .entries
                .values()
                .map(|fields| {
                    32 + fields
                        .iter()
                        .map(|(f, v)| f.len() + v.len() + 48)
                        .sum::<usize>()
                })
                .sum(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Entry {
    #[serde(flatten)]
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<u64>,
}

/// Reads an entry, `{"type": ..., "data": ..., "expires": ...}`, in one pass:
/// the data goes straight into the type named before it, rather than through
/// the copy a flattened field needs. Data that comes first is read after.
impl<'de> Deserialize<'de> for Entry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{DeserializeSeed, Error, MapAccess, Visitor};

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Type,
            Data,
            Expires,
            #[serde(other)]
            Other,
        }

        #[derive(Clone, Copy, Deserialize)]
        #[serde(rename_all = "lowercase")]
        enum Kind {
            String,
            List,
            Hash,
            Set,
            ZSet,
            Stream,
        }

        struct Data(Kind);

        impl<'de> DeserializeSeed<'de> for Data {
            type Value = Value;
            fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
                Ok(match self.0 {
                    Kind::String => Value::String(String::deserialize(d)?),
                    Kind::List => Value::List(VecDeque::deserialize(d)?),
                    Kind::Hash => Value::Hash(BTreeMap::deserialize(d)?),
                    Kind::Set => Value::Set(BTreeSet::deserialize(d)?),
                    Kind::ZSet => Value::ZSet(ZSet::deserialize(d)?),
                    Kind::Stream => Value::Stream(Stream::deserialize(d)?),
                })
            }
        }

        struct EntryVisitor;

        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = Entry;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a key's entry")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Entry, A::Error> {
                let (mut kind, mut value, mut early, mut expires) =
                    (None::<Kind>, None, None::<serde_json::Value>, None);
                while let Some(field) = map.next_key()? {
                    match field {
                        Field::Type => kind = Some(map.next_value()?),
                        Field::Data => match kind {
                            Some(kind) => value = Some(map.next_value_seed(Data(kind))?),
                            None => early = Some(map.next_value()?),
                        },
                        Field::Expires => expires = map.next_value()?,
                        Field::Other => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                let kind = kind.ok_or_else(|| A::Error::missing_field("type"))?;
                let value = match (value, early) {
                    (Some(value), _) => value,
                    (None, Some(data)) => Data(kind).deserialize(data).map_err(A::Error::custom)?,
                    (None, None) => return Err(A::Error::missing_field("data")),
                };
                Ok(Entry { value, expires })
            }
        }

        deserializer.deserialize_map(EntryVisitor)
    }
}

impl Entry {
    pub fn new(value: Value) -> Entry {
        Entry {
            value,
            expires: None,
        }
    }

    pub fn expired(&self, now: u64) -> bool {
        self.expires.is_some_and(|at| at <= now)
    }
}

pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// Values with more elements than this are freed on the freeing thread.
const FREE_ELSEWHERE: usize = 1000;

#[cfg(test)]
thread_local! {
    /// Elements this thread handed to the freeing thread, for tests.
    static FREED_ELSEWHERE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Frees a value on the freeing thread, or here if it cannot be started.
fn free_elsewhere(value: impl Send + 'static, elements: usize) {
    static FREER: OnceLock<Option<Sender<Box<dyn Send>>>> = OnceLock::new();
    let freer = FREER.get_or_init(|| {
        let (tx, rx) = channel::<Box<dyn Send>>();
        std::thread::Builder::new()
            .name("postino-free".into())
            .spawn(move || {
                for value in rx {
                    drop(value);
                    crate::memory::flush();
                }
            })
            .ok()
            .map(|_| tx)
    });
    #[cfg(test)]
    FREED_ELSEWHERE.with(|n| n.set(n.get() + elements));
    let _ = elements;
    if let Some(tx) = freer {
        let _ = tx.send(Box::new(value));
    }
}

/// Lets go of an entry the keyspace no longer holds.
fn discard(entry: Entry) {
    let elements = entry.value.elements();
    if elements > FREE_ELSEWHERE {
        free_elsewhere(entry, elements);
    }
}

/// Keys removed per batch of the expire cycle at most, about a millisecond's
/// work, so a mass expiry is spread over batches with requests in between;
/// reads already treat expired keys as gone.
const EXPIRE_BATCH: usize = 1000;

#[derive(Default)]
pub struct Db {
    keys: BTreeMap<String, Entry>,
    /// When keys expire, soonest first. An entry goes stale when its key's
    /// expiry changes or the key goes away; the expire cycle skips those.
    deadlines: BinaryHeap<Reverse<(u64, String)>>,
    capture: Option<Capture>,
    captures: u64,
}

/// How much one part of a capture holds. The parts are written to disk one
/// after another, so the capture never copies what it has already written to
/// make room for more. Tests fill many, to see them come out in order.
const CAPTURE_PART: usize = if cfg!(test) { 64 } else { 1 << 20 };

/// The keyspace as it was at one moment, being written out as JSON while it
/// goes on changing.
struct Capture {
    /// Which capture this is, so whoever drives one never drives another.
    number: u64,
    /// Complete parts not yet handed out, and the one being filled.
    parts: Vec<Vec<u8>>,
    out: Vec<u8>,
    /// Bytes moved from `out` to `parts` so far.
    filled: usize,
    /// The last key written in order; keys up to it are done.
    cursor: Option<String>,
    /// Keys written out of order because they were about to change, or found
    /// missing then: the in-order pass skips them.
    early: HashSet<String>,
    /// Every key is written.
    done: bool,
    written: usize,
    broken: bool,
}

impl Capture {
    /// Bytes serialized so far.
    fn size(&self) -> usize {
        self.filled + self.out.len()
    }

    fn passed(&self, key: &str) -> bool {
        self.done || self.cursor.as_deref().is_some_and(|c| key <= c)
    }

    fn write(&mut self, key: &str, entry: &Entry) {
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
        let ok = serde_json::to_writer(&mut self.out, key).is_ok() && {
            self.out.push(b':');
            serde_json::to_writer(&mut self.out, entry).is_ok()
        };
        if ok {
            self.written += 1;
        } else {
            self.out.truncate(mark);
            self.broken = true;
        }
    }
}

/// Reads the keyspace of a snapshot. The keys come in order but for those
/// changed while it was taken, which come early, so sorting them costs little
/// and the map is then built in one pass rather than by inserting each.
pub fn sorted_keys<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, Entry>, D::Error> {
    struct Keys;
    impl<'de> serde::de::Visitor<'de> for Keys {
        type Value = BTreeMap<String, Entry>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a map of keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut pairs = Vec::with_capacity(map.size_hint().unwrap_or(0));
            while let Some(pair) = map.next_entry::<String, Entry>()? {
                pairs.push(pair);
            }
            Ok(pairs.into_iter().collect())
        }
    }
    deserializer.deserialize_map(Keys)
}

impl Db {
    pub fn load(keys: BTreeMap<String, Entry>) -> Db {
        let mut db = Db {
            keys,
            ..Db::default()
        };
        db.reindex_deadlines();
        db
    }

    /// Every key, expired ones included, for reading.
    pub fn entries(&self) -> &BTreeMap<String, Entry> {
        &self.keys
    }

    /// Before `key` changes: a capture that has not reached it writes it now,
    /// as it was when the capture began, or notes that it did not exist.
    fn preserve(&mut self, key: &str) {
        let Some(capture) = self.capture.as_mut() else {
            return;
        };
        if capture.passed(key) || !capture.early.insert(key.to_string()) {
            return;
        }
        if let Some(entry) = self.keys.get(key) {
            capture.write(key, entry);
        }
    }

    pub fn insert(&mut self, key: String, entry: Entry) {
        self.preserve(&key);
        if let Some(old) = self.keys.insert(key, entry) {
            discard(old);
        }
    }

    /// Replaces what `key` holds, expired or not, finding it once and keeping
    /// its name. When it holds nothing, `entry` is handed back.
    pub fn replace(&mut self, key: &str, entry: Entry) -> Option<Entry> {
        self.preserve(key);
        match self.keys.get_mut(key) {
            Some(held) => {
                discard(std::mem::replace(held, entry));
                None
            }
            None => Some(entry),
        }
    }

    /// Removes a key, expired or not.
    pub fn take(&mut self, key: &str) -> Option<Entry> {
        self.preserve(key);
        self.keys.remove(key)
    }

    pub fn clear(&mut self) {
        // A capture in progress writes everything it has not reached first.
        self.capture_step(usize::MAX);
        let keys = std::mem::take(&mut self.keys);
        let elements = keys.values().map(|e| e.value.elements()).sum();
        if elements > FREE_ELSEWHERE {
            free_elsewhere(keys, elements);
        }
    }

    /// Starts a capture of the keyspace, after `out`, which holds what comes
    /// before the keys. Returns its number.
    pub fn begin_capture(&mut self, out: Vec<u8>) -> u64 {
        self.captures += 1;
        self.capture = Some(Capture {
            number: self.captures,
            parts: Vec::new(),
            filled: 0,
            out,
            cursor: None,
            early: HashSet::new(),
            done: false,
            written: 0,
            broken: false,
        });
        self.captures
    }

    pub fn capturing(&self) -> bool {
        self.capture.is_some()
    }

    /// The number of the capture in progress.
    pub fn capture_number(&self) -> Option<u64> {
        self.capture.as_ref().map(|c| c.number)
    }

    /// The parts of the capture that are complete, for writing out.
    pub fn drain_capture(&mut self) -> Vec<Vec<u8>> {
        self.capture
            .as_mut()
            .map(|c| std::mem::take(&mut c.parts))
            .unwrap_or_default()
    }

    /// Writes the next run of keys, about `budget` bytes of them, to the
    /// capture. Returns whether every key is written.
    pub fn capture_step(&mut self, budget: usize) -> bool {
        let Db { keys, capture, .. } = self;
        let Some(capture) = capture.as_mut() else {
            return true;
        };
        if capture.done {
            return true;
        }
        let start = capture.size();
        let mut run = match capture.cursor.as_deref() {
            Some(cursor) => keys.range::<str, _>((Bound::Excluded(cursor), Bound::Unbounded)),
            None => keys.range::<str, _>(..),
        };
        let mut last = None;
        while capture.size() - start < budget {
            let Some((key, entry)) = run.next() else {
                capture.done = true;
                break;
            };
            last = Some(key);
            if !capture.early.remove(key.as_str()) {
                capture.write(key, entry);
            }
        }
        if let Some(key) = last {
            capture.cursor = Some(key.clone());
        }
        capture.done
    }

    /// Ends the capture, writing whatever it has not reached, and returns the
    /// keys as one JSON object after what came before them, in parts.
    pub fn end_capture(&mut self) -> Result<Vec<Vec<u8>>, String> {
        self.capture_step(usize::MAX);
        let capture = self.capture.take().ok_or("no snapshot is being taken")?;
        if capture.broken {
            return Err("a key could not be serialized".into());
        }
        let mut parts = capture.parts;
        let mut out = capture.out;
        out.push(b'}');
        parts.push(out);
        Ok(parts)
    }

    /// Drops a capture without finishing it.
    pub fn abandon_capture(&mut self) {
        self.capture = None;
    }

    /// A live key, or `None` when it is missing or expired.
    pub fn get(&self, key: &str, now: u64) -> Option<&Entry> {
        self.keys.get(key).filter(|e| !e.expired(now))
    }

    /// A live key for writing; an expired key is removed first.
    pub fn get_mut(&mut self, key: &str, now: u64) -> Option<&mut Entry> {
        self.preserve(key);
        if self.keys.get(key).is_some_and(|e| e.expired(now))
            && let Some(expired) = self.keys.remove(key)
        {
            discard(expired);
        }
        self.keys.get_mut(key)
    }

    pub fn exists(&self, key: &str, now: u64) -> bool {
        self.get(key, now).is_some()
    }

    pub fn remove(&mut self, key: &str, now: u64) -> Option<Entry> {
        let entry = self.take(key)?;
        (!entry.expired(now)).then_some(entry)
    }

    /// Deletes a key, returning whether it was live.
    pub fn delete(&mut self, key: &str, now: u64) -> bool {
        let Some(entry) = self.take(key) else {
            return false;
        };
        let live = !entry.expired(now);
        discard(entry);
        live
    }

    /// Drops a collection key that has become empty.
    pub fn prune(&mut self, key: &str) {
        if self
            .keys
            .get(key)
            .is_some_and(|e| e.value.is_empty_collection())
        {
            self.take(key);
        }
    }

    pub fn live_keys(&self, now: u64) -> impl Iterator<Item = (&String, &Entry)> {
        self.keys.iter().filter(move |(_, e)| !e.expired(now))
    }

    pub fn len(&self, now: u64) -> usize {
        self.live_keys(now).count()
    }

    /// Live keys after `after` that start with one of `prefixes`, in key order.
    /// The prefixes must be sorted and none may start with another, so each
    /// names its own run of keys and only those runs are visited.
    pub fn keys_with_prefixes<'a>(
        &'a self,
        prefixes: Vec<String>,
        after: Option<&'a str>,
        now: u64,
    ) -> impl Iterator<Item = (&'a String, &'a Entry)> + 'a {
        prefixes.into_iter().flat_map(move |prefix| {
            let start = match after {
                Some(after) if after >= prefix.as_str() => Bound::Excluded(after.to_string()),
                _ => Bound::Included(prefix.clone()),
            };
            self.keys
                .range::<String, _>((start, Bound::Unbounded))
                .take_while(move |(k, _)| k.starts_with(prefix.as_str()))
                .filter(move |(_, e)| !e.expired(now))
        })
    }

    pub fn is_empty(&self, now: u64) -> bool {
        self.live_keys(now).next().is_none()
    }

    /// Notes that `key` now expires at `at`, for the expire cycle. Every place
    /// that gives a key an expiry calls this.
    pub fn deadline(&mut self, key: &str, at: u64) {
        self.deadlines.push(Reverse((at, key.to_string())));
        // Changing a key's expiry leaves its old deadline behind; rebuild once
        // stale entries outnumber the keys.
        if self.deadlines.len() > 2 * self.keys.len() + 1024 {
            self.reindex_deadlines();
        }
    }

    /// Rebuilds the deadlines from the keys, after loading a snapshot.
    pub fn reindex_deadlines(&mut self) {
        self.deadlines = self
            .keys
            .iter()
            .filter_map(|(k, e)| e.expires.map(|at| Reverse((at, k.clone()))))
            .collect();
    }

    /// Removes a batch of keys whose time has come, returning their names
    /// (the active expire cycle). It visits only keys that are due, and a
    /// bounded number of deadlines left behind by changed expiries.
    pub fn expire_keys(&mut self, now: u64) -> Vec<String> {
        let mut expired = Vec::new();
        let mut visited = 0;
        while expired.len() < EXPIRE_BATCH
            && visited < 4 * EXPIRE_BATCH
            && let Some(Reverse((at, _))) = self.deadlines.peek()
            && *at <= now
        {
            visited += 1;
            let Some(Reverse((at, key))) = self.deadlines.pop() else {
                break;
            };
            if self.keys.get(&key).is_some_and(|e| e.expires == Some(at))
                && let Some(entry) = self.take(&key)
            {
                discard(entry);
                expired.push(key);
            }
        }
        expired
    }

    /// Whether a key, or a stale deadline, is due by `now`: the expire cycle
    /// has more to do.
    pub fn expiry_due(&self, now: u64) -> bool {
        self.deadlines
            .peek()
            .is_some_and(|Reverse((at, _))| *at <= now)
    }
}

/// The text every match of a glob pattern starts with: everything before its
/// first wildcard or escape.
pub fn literal_prefix(pattern: &str) -> &str {
    let end = pattern.find(['*', '?', '[', '\\']).unwrap_or(pattern.len());
    &pattern[..end]
}

/// Formats a float for replies: integral values without a fraction, no exponent.
pub fn format_float(value: f64) -> String {
    if value.is_infinite() {
        return if value > 0.0 {
            "inf".into()
        } else {
            "-inf".into()
        };
    }
    let text = format!("{value}");
    if text.contains('e') || text.contains('E') {
        format!("{value:.17}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_values_the_keyspace_lets_go_of_are_freed_elsewhere() {
        use crate::commands::Ctx;
        let mut db = Db::default();
        let run = |db: &mut Db, at: u64, words: &[&str]| {
            let argv: Vec<String> = words.iter().map(|w| w.to_string()).collect();
            db.exec(&argv, &Ctx::replay(at));
        };
        let big = |db: &mut Db, key: &str| {
            let mut argv = vec!["RPUSH".to_string(), key.to_string()];
            argv.extend((0..2000).map(|i| i.to_string()));
            db.exec(&argv, &Ctx::replay(0));
        };
        let freed = |db: &mut Db, change: &dyn Fn(&mut Db)| {
            let before = FREED_ELSEWHERE.with(|n| n.get());
            change(db);
            FREED_ELSEWHERE.with(|n| n.get()) - before
        };
        for key in ["deleted", "replaced", "past", "swept", "lazy"] {
            big(&mut db, key);
        }
        run(&mut db, 0, &["RPUSH", "small", "a", "b"]);
        run(&mut db, 0, &["PEXPIRE", "swept", "1"]);
        run(&mut db, 0, &["PEXPIRE", "lazy", "7"]);
        assert_eq!(freed(&mut db, &|db| run(db, 0, &["DEL", "deleted"])), 2000);
        assert_eq!(
            freed(&mut db, &|db| run(db, 0, &["SET", "replaced", "x"])),
            2000
        );
        assert_eq!(
            freed(&mut db, &|db| run(db, 0, &["EXPIRE", "past", "-1"])),
            2000
        );
        assert_eq!(
            freed(&mut db, &|db| drop(db.expire_keys(5))),
            2000,
            "expired by the cycle"
        );
        assert_eq!(
            freed(&mut db, &|db| run(db, 9, &["RPUSH", "lazy", "x"])),
            2000,
            "expired when touched"
        );
        assert_eq!(
            freed(&mut db, &|db| run(db, 9, &["DEL", "small"])),
            0,
            "small values are freed at once"
        );
        big(&mut db, "flushed");
        assert_eq!(freed(&mut db, &|db| run(db, 9, &["FLUSHALL"])), 2002);
    }

    #[test]
    fn entries_read_back_whatever_the_order_of_their_fields() {
        let entry: Entry =
            serde_json::from_str(r#"{"data":["a","b"],"expires":5,"type":"list"}"#).unwrap();
        assert!(matches!(&entry.value, Value::List(l) if l.len() == 2));
        assert_eq!(entry.expires, Some(5));
        assert_eq!(
            serde_json::to_string(&entry).unwrap(),
            r#"{"type":"list","data":["a","b"],"expires":5}"#
        );
        let zset: Entry = serde_json::from_str(r#"{"type":"zset","data":[["m","inf"]]}"#).unwrap();
        assert!(matches!(&zset.value, Value::ZSet(z) if z.len() == 1));
        assert!(serde_json::from_str::<Entry>(r#"{"type":"list"}"#).is_err());
        assert!(serde_json::from_str::<Entry>(r#"{"type":"tree","data":1}"#).is_err());
        let keys: BTreeMap<String, Entry> = sorted_keys(&mut serde_json::Deserializer::from_str(
            r#"{"b":{"type":"string","data":"2"},"a":{"type":"string","data":"1"}}"#,
        ))
        .unwrap();
        assert_eq!(keys.keys().collect::<Vec<_>>(), ["a", "b"]);
    }

    #[test]
    fn infinite_scores_survive_a_snapshot() {
        let mut z = ZSet::default();
        z.insert("low", f64::NEG_INFINITY);
        z.insert("mid", 1.5);
        z.insert("high", f64::INFINITY);
        let raw = serde_json::to_string(&z).unwrap();
        assert_eq!(raw, r#"[["low","-inf"],["mid",1.5],["high","inf"]]"#);
        let back: ZSet = serde_json::from_str(&raw).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), raw);
        assert!(serde_json::from_str::<ZSet>(r#"[["m",null]]"#).is_err());
    }

    #[test]
    fn a_sorted_set_keeps_each_name_once_however_its_scores_change() {
        let mut z = ZSet::default();
        for (score, member) in ["a", "b", "c"].iter().enumerate() {
            z.insert(member, score as f64);
        }
        assert!(!z.insert("b", 7.0));
        assert!(!z.insert("a", 7.0));
        assert!(z.remove("c"));
        let ranked: Vec<(f64, &str)> = z.order.iter().map(|(s, m)| (s.0, &**m)).collect();
        assert_eq!(ranked, [(7.0, "a"), (7.0, "b")]);
        // Held by the index and the order, and by nothing else.
        for name in z.scores.keys() {
            assert_eq!(Arc::strong_count(name), 2, "{name}");
        }
    }

    #[test]
    fn zsets_order_by_score_then_member() {
        let mut z = ZSet::default();
        z.insert("b", 1.0);
        z.insert("a", 1.0);
        z.insert("c", 0.5);
        let order: Vec<&str> = z.order.iter().map(|(_, m)| &**m).collect();
        assert_eq!(order, ["c", "a", "b"]);
        assert!(!z.insert("c", 2.0));
        assert_eq!(z.rank("c"), Some(2));
        let json = serde_json::to_string(&z).unwrap();
        let back: ZSet = serde_json::from_str(&json).unwrap();
        assert_eq!(back.rank("b"), Some(1));
    }

    #[test]
    fn floats_format_without_noise() {
        assert_eq!(format_float(3.0), "3");
        assert_eq!(format_float(10.5), "10.5");
        assert_eq!(format_float(f64::INFINITY), "inf");
    }
}
