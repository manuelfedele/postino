//! The command language of the keyspace.
//!
//! A command is an array of strings such as `["SET", "plan", "v1", "EX", "60"]`
//! and returns a [`Reply`]. A write reports what it changed, so the broker can
//! journal it (with its timestamp, which makes replay deterministic), wake
//! blocked readers, and publish key-change notifications. Commands that cannot
//! be replayed as written, such as SPOP or a blocking pop, report the
//! deterministic commands to journal instead.

use std::collections::btree_map::Entry as Slot;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use serde_json::{Map, Value as Json, json};

use crate::db::{Consumer, Db, Entry, Group, Pending, Ranked, Stream, Value, WRONGTYPE, ZSet};
use crate::db::{format_float, literal_prefix};
use crate::glob::Glob;
use crate::id::Id;
use crate::scope::{Access, Scope};
use crate::util::{from_hex, random_below, to_hex};

#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Simple(String),
    Int(i64),
    Bulk(String),
    Nil,
    Array(Vec<Reply>),
    Map(Vec<(String, Reply)>),
    Error(String),
}

impl Reply {
    pub fn ok() -> Reply {
        Reply::Simple("OK".into())
    }

    /// About how many bytes the reply takes to send.
    pub fn size(&self) -> usize {
        match self {
            Reply::Simple(s) | Reply::Bulk(s) | Reply::Error(s) => s.len() + 4,
            Reply::Int(_) => 8,
            Reply::Nil => 4,
            Reply::Array(items) => items.iter().map(Reply::size).sum::<usize>() + 2,
            Reply::Map(pairs) => {
                pairs
                    .iter()
                    .map(|(k, v)| k.len() + 4 + v.size())
                    .sum::<usize>()
                    + 2
            }
        }
    }

    fn bulks<I: IntoIterator<Item = S>, S: Into<String>>(items: I) -> Reply {
        Reply::Array(items.into_iter().map(|s| Reply::Bulk(s.into())).collect())
    }

    fn int(n: usize) -> Reply {
        Reply::Int(i64::try_from(n).unwrap_or(i64::MAX))
    }

    fn bool(b: bool) -> Reply {
        Reply::Int(i64::from(b))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }

    pub fn to_json(&self) -> Json {
        match self {
            Reply::Simple(s) | Reply::Bulk(s) => json!(s),
            Reply::Int(n) => json!(n),
            Reply::Nil => Json::Null,
            Reply::Array(items) => Json::Array(items.iter().map(Reply::to_json).collect()),
            Reply::Map(pairs) => Json::Object(
                pairs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect::<Map<String, Json>>(),
            ),
            Reply::Error(e) => json!({ "error": e }),
        }
    }
}

/// The same JSON as [`Reply::to_json`], written without building it first:
/// a map's keys in order, the last of any repeated one.
impl serde::Serialize for Reply {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Reply::Simple(s) | Reply::Bulk(s) => serializer.serialize_str(s),
            Reply::Int(n) => serializer.serialize_i64(*n),
            Reply::Nil => serializer.serialize_unit(),
            Reply::Array(items) => serializer.collect_seq(items),
            Reply::Map(pairs) => {
                let sorted: BTreeMap<&str, &Reply> =
                    pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();
                serializer.collect_map(sorted)
            }
            Reply::Error(e) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("error", e)?;
                map.end()
            }
        }
    }
}

/// How a command runs: its clock, and whether it may block or enforce limits.
pub struct Ctx {
    pub now: u64,
    /// Replaying the journal: limits are not enforced, nothing blocks.
    pub replay: bool,
    pub blocking: bool,
    /// Who runs the command: whether it may flush, and which keys it sees.
    pub access: Access,
    pub max_keys: usize,
    pub max_elements: usize,
}

impl Ctx {
    pub fn replay(now: u64) -> Ctx {
        Ctx {
            now,
            replay: true,
            blocking: false,
            access: Access::admin(),
            max_keys: usize::MAX,
            max_elements: usize::MAX,
        }
    }
}

/// A blocking command that found nothing: the broker retries `retry` (with
/// any `$` already resolved) until it yields a value or `timeout_ms` passes.
pub struct Block {
    pub retry: Vec<String>,
    /// 0 waits as long as the transport allows.
    pub timeout_ms: u64,
}

pub struct Outcome {
    pub reply: Reply,
    pub dirty: bool,
    /// Commands to journal instead of the original, when it is not replayable.
    pub propagate: Option<Vec<Vec<String>>>,
    /// Changed keys with the event name, for key-change notifications.
    pub events: Vec<(String, &'static str)>,
    pub block: Option<Block>,
}

impl Outcome {
    pub fn of(reply: Reply) -> Outcome {
        Outcome {
            reply,
            dirty: false,
            propagate: None,
            events: Vec::new(),
            block: None,
        }
    }

    /// The commands to journal for this execution, if it changed anything.
    pub fn journal<'a>(&'a self, argv: &'a [String]) -> Vec<&'a [String]> {
        if !self.dirty || self.reply.is_error() {
            return Vec::new();
        }
        match &self.propagate {
            Some(commands) => commands.iter().map(Vec::as_slice).collect(),
            None => vec![argv],
        }
    }
}

type R = Result<Reply, String>;

const SYNTAX: &str = "ERR syntax error";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const NOT_FLOAT: &str = "ERR value is not a valid float";
const BAD_ID: &str = "ERR Invalid stream ID specified as stream command argument";
const MAX_STRING: usize = 16 << 20;
/// The most one response may carry. A read past it is refused instead of
/// copying the data, so a command that names the same data many times over,
/// or a batch of them, cannot make the server run out of memory.
pub const MAX_REPLY_BYTES: usize = 64 << 20;

/// Refuses a reply that has grown to `bytes`, once past [`MAX_REPLY_BYTES`].
pub fn within_reply(bytes: usize) -> Result<(), String> {
    if bytes > MAX_REPLY_BYTES {
        return Err(format!(
            "ERR the reply would exceed {} MiB; read less at a time, with a range, COUNT, or SCAN",
            MAX_REPLY_BYTES >> 20
        ));
    }
    Ok(())
}

/// The replies `items` makes, one at a time, refused as soon as together
/// they would be more than one response may carry: a value too large to
/// send is not copied whole first.
fn collect_within(items: impl IntoIterator<Item = Reply>) -> Result<Reply, String> {
    let mut bytes = 0usize;
    let mut out = Vec::new();
    for item in items {
        bytes += item.size();
        within_reply(bytes)?;
        out.push(item);
    }
    Ok(Reply::Array(out))
}

/// Every command the keyspace understands, for `COMMAND` and the docs.
pub const COMMANDS: &[&str] = &[
    "APPEND",
    "BLMOVE",
    "BLPOP",
    "BRPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "COMMAND",
    "DBSIZE",
    "DECR",
    "DECRBY",
    "DEL",
    "DELIFEQ",
    "ECHO",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "FLUSHALL",
    "FLUSHDB",
    "GET",
    "GETDEL",
    "GETEX",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HSET",
    "HSETNX",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "INFO",
    "KEYS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LMOVE",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MGET",
    "MSET",
    "MSETNX",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PING",
    "PSETEX",
    "PTTL",
    "PUBLISH",
    "PUBSUB",
    "RENAME",
    "RENAMENX",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCAN",
    "SCARD",
    "SDIFF",
    "SET",
    "SETEX",
    "SETNX",
    "SINTER",
    "SISMEMBER",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "STRLEN",
    "SUNION",
    "TIME",
    "TTL",
    "TYPE",
    "UNLINK",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XCLAIM",
    "XDEL",
    "XGROUP",
    "XINFO",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREADGROUP",
    "XREVRANGE",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZINCRBY",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCORE",
];

/// Where a command names its keys, and whether it may change them.
enum Keys {
    None,
    /// The first argument.
    First(bool),
    /// Every argument.
    All(bool),
    /// `key value [key value ...]`.
    Pairs,
    /// A source and a destination, both changed.
    Two,
    /// Every argument but the trailing timeout.
    AllButLast,
    /// The keys after `STREAMS`, reading options from this argument on.
    Streams(usize, bool),
    /// The argument after a subcommand.
    Second(bool),
}

fn key_spec(name: &str) -> Option<Keys> {
    Some(match name {
        "COMMAND" | "DBSIZE" | "ECHO" | "FLUSHALL" | "FLUSHDB" | "INFO" | "KEYS" | "PING"
        | "PUBLISH" | "PUBSUB" | "SCAN" | "TIME" => Keys::None,
        "EXISTS" | "MGET" | "SDIFF" | "SINTER" | "SUNION" => Keys::All(false),
        "DEL" | "UNLINK" => Keys::All(true),
        "MSET" | "MSETNX" => Keys::Pairs,
        "BLMOVE" | "LMOVE" | "RENAME" | "RENAMENX" | "RPOPLPUSH" | "SMOVE" => Keys::Two,
        "BLPOP" | "BRPOP" | "BZPOPMAX" | "BZPOPMIN" => Keys::AllButLast,
        "XREAD" => Keys::Streams(1, false),
        "XREADGROUP" => Keys::Streams(4, true),
        "XGROUP" => Keys::Second(true),
        "XINFO" => Keys::Second(false),
        "EXPIRETIME" | "GET" | "HEXISTS" | "HGET" | "HGETALL" | "HKEYS" | "HLEN" | "HMGET"
        | "HVALS" | "LINDEX" | "LLEN" | "LRANGE" | "PEXPIRETIME" | "PTTL" | "SCARD"
        | "SISMEMBER" | "SMEMBERS" | "SMISMEMBER" | "SRANDMEMBER" | "STRLEN" | "TTL" | "TYPE"
        | "XLEN" | "XPENDING" | "XRANGE" | "XREVRANGE" | "ZCARD" | "ZCOUNT" | "ZMSCORE"
        | "ZRANGE" | "ZRANGEBYSCORE" | "ZRANK" | "ZREVRANGE" | "ZREVRANGEBYSCORE" | "ZREVRANK"
        | "ZSCORE" => Keys::First(false),
        "APPEND" | "DECR" | "DECRBY" | "DELIFEQ" | "EXPIRE" | "EXPIREAT" | "GETDEL" | "GETEX"
        | "GETSET" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" | "HMSET" | "HSET" | "HSETNX" | "INCR"
        | "INCRBY" | "INCRBYFLOAT" | "LINSERT" | "LPOP" | "LPUSH" | "LPUSHX" | "LREM" | "LSET"
        | "LTRIM" | "PERSIST" | "PEXPIRE" | "PEXPIREAT" | "PSETEX" | "RPOP" | "RPUSH"
        | "RPUSHX" | "SADD" | "SET" | "SETEX" | "SETNX" | "SPOP" | "SREM" | "XACK" | "XADD"
        | "XAUTOCLAIM" | "XCLAIM" | "XDEL" | "XTRIM" | "ZADD" | "ZINCRBY" | "ZPOPMAX"
        | "ZPOPMIN" | "ZREM" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" => Keys::First(true),
        _ => return None,
    })
}

/// Live keys starting with `prefix` (after `after`, if given) that `scope` can
/// read, in key order. Only the runs of keys the scope can reach are visited,
/// so an agent listing its own namespace never walks anyone else's.
pub fn visible_keys<'a>(
    db: &'a Db,
    scope: &'a Scope,
    prefix: &str,
    after: Option<&'a str>,
    now: u64,
) -> impl Iterator<Item = (&'a String, &'a Entry)> + 'a {
    db.keys_with_prefixes(scope.search_prefixes(prefix), after, now)
        .filter(|(k, _)| scope.can_read(k))
}

/// Keys one SCAN looks at, whatever its COUNT: the lock is held meanwhile.
pub const SCAN_MOST: usize = 1000;
/// Keys a KEYS looks at with the lock held, before letting other requests
/// run (see [`crate::broker::Broker::command`]).
pub const KEYS_RUN: usize = 1000;
/// Keys a KEYS that has to run whole, as in a batch, may look at.
pub const KEYS_WHOLE: usize = 10_000;

/// The keys after `after` that `pattern` matches, among the next `run` keys
/// the caller may read; and the last one looked at, when the run ended
/// before the keys did.
pub fn keys_run(
    db: &Db,
    scope: &Scope,
    pattern: &Glob,
    after: Option<&str>,
    run: usize,
    now: u64,
) -> (Vec<String>, Option<String>) {
    let mut found = Vec::new();
    let keys = visible_keys(db, scope, literal_prefix(pattern.as_str()), after, now);
    for (looked, (key, _)) in keys.enumerate() {
        if pattern.matches(key) {
            found.push(key.clone());
        }
        if looked + 1 == run {
            return (found, Some(key.clone()));
        }
    }
    (found, None)
}

/// A command's name in upper case, kept on the stack: each request's is
/// compared several times over, and none is long enough to need the heap.
#[derive(Clone, Copy)]
pub struct Name {
    bytes: [u8; 24],
    len: u8,
}

impl Name {
    /// The name of the command `argv` runs. One longer than any command's
    /// reads as empty, which no command is called.
    pub fn of(argv: &[String]) -> Name {
        let word = argv.first().map_or("", String::as_str);
        let mut bytes = [0; 24];
        let len = if word.len() <= bytes.len() {
            for (to, from) in bytes.iter_mut().zip(word.bytes()) {
                *to = from.to_ascii_uppercase();
            }
            word.len()
        } else {
            0
        };
        Name {
            bytes,
            len: len as u8,
        }
    }

    pub fn as_str(&self) -> &str {
        // Upper-casing ASCII letters leaves UTF-8 whole.
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or("")
    }
}

impl PartialEq<&str> for Name {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Whether a command can make the keyspace bigger, and so is refused when the
/// server is out of memory. Commands that only read, move, or remove data still run.
pub fn grows(argv: &[String]) -> bool {
    match Name::of(argv).as_str() {
        "APPEND" | "DECR" | "DECRBY" | "GETSET" | "HINCRBY" | "HINCRBYFLOAT" | "HMSET" | "HSET"
        | "HSETNX" | "INCR" | "INCRBY" | "INCRBYFLOAT" | "LINSERT" | "LPUSH" | "LPUSHX"
        | "LSET" | "MSET" | "MSETNX" | "PSETEX" | "RPUSH" | "RPUSHX" | "SADD" | "SET" | "SETEX"
        | "SETNX" | "XADD" | "XREADGROUP" | "ZADD" | "ZINCRBY" => true,
        "XGROUP" => argv.get(1).is_some_and(|sub| {
            ["CREATE", "CREATECONSUMER", "SETID"]
                .iter()
                .any(|s| sub.eq_ignore_ascii_case(s))
        }),
        _ => false,
    }
}

/// Whether a command may change the keyspace.
pub fn writes(argv: &[String]) -> bool {
    matches!(Name::of(argv).as_str(), "FLUSHALL" | "FLUSHDB")
        || key_args(argv).is_some_and(|keys| keys.iter().any(|(_, write)| *write))
}

/// The stream keys of `[COUNT n] [BLOCK ms] [NOACK] STREAMS k1.. id1..`,
/// walked exactly as the stream readers parse it.
fn stream_keys(argv: &[String], from: usize) -> std::ops::Range<usize> {
    let mut i = from;
    while let Some(word) = argv.get(i) {
        match word.to_ascii_uppercase().as_str() {
            "COUNT" | "BLOCK" => i += 2,
            "NOACK" => i += 1,
            "STREAMS" => {
                let start = i + 1;
                return start..start + (argv.len() - start) / 2;
            }
            _ => break,
        }
    }
    0..0
}

/// The arguments of a command that are keys, each with whether the command may
/// change it; `None` for a command the keyspace does not know.
pub fn key_args(argv: &[String]) -> Option<Vec<(usize, bool)>> {
    // No command is called by an empty name, or by one too long to keep.
    let name = Name::of(argv);
    let len = argv.len();
    let keys = |range: std::ops::Range<usize>, write: bool| {
        range.filter(|&i| i < len).map(|i| (i, write)).collect()
    };
    Some(match key_spec(name.as_str())? {
        Keys::None => Vec::new(),
        Keys::First(write) => keys(1..2, write),
        Keys::All(write) => keys(1..len, write),
        Keys::Pairs => (1..len).step_by(2).map(|i| (i, true)).collect(),
        Keys::Two => keys(1..3, true),
        Keys::AllButLast => keys(1..len.saturating_sub(1).max(1), true),
        Keys::Streams(from, write) => keys(stream_keys(argv, from), write),
        // Checked after HELP too: an argument a subcommand ignores must not
        // become a way around the scope.
        Keys::Second(write) => keys(2..3, write),
    })
}

// ---- argument parsing ------------------------------------------------------

fn wrong_args(name: &str) -> String {
    format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    )
}

fn int(s: &str) -> Result<i64, String> {
    s.parse::<i64>().map_err(|_| NOT_INT.to_string())
}

fn count(s: &str) -> Result<usize, String> {
    let n = int(s)?;
    usize::try_from(n).map_err(|_| "ERR value is out of range, must be positive".to_string())
}

fn float(s: &str) -> Result<f64, String> {
    let value = match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        text => text.parse::<f64>().map_err(|_| NOT_FLOAT.to_string())?,
    };
    if value.is_nan() {
        Err(NOT_FLOAT.into())
    } else {
        Ok(value)
    }
}

/// When a key given `amount` seconds (`unit` 1000) or milliseconds (`unit` 1),
/// from now or as a Unix time, expires; `None` when that time does not fit.
fn expiry_at(now: u64, amount: i64, unit: i64, absolute: bool) -> Option<u64> {
    let ms = amount.checked_mul(unit)?;
    let at = if absolute {
        ms
    } else {
        ms.checked_add(i64::try_from(now).ok()?)?
    };
    u64::try_from(at).ok()
}

fn is(arg: &str, word: &str) -> bool {
    arg.eq_ignore_ascii_case(word)
}

/// Whether `key` may be written: the key limit counts every key held in
/// memory, expired ones included until they are removed.
fn admit(db: &Db, key: &str, ctx: &Ctx) -> Result<(), String> {
    admit_all(db, std::iter::once(key), ctx)
}

/// [`admit`] for several keys written together: all fit, or none is written.
fn admit_all<'a>(db: &Db, keys: impl Iterator<Item = &'a str>, ctx: &Ctx) -> Result<(), String> {
    if ctx.replay {
        return Ok(());
    }
    let mut new: Vec<&str> = keys.filter(|k| !db.entries().contains_key(*k)).collect();
    new.sort_unstable();
    new.dedup();
    if db.entries().len().saturating_add(new.len()) > ctx.max_keys && !new.is_empty() {
        return Err(format!(
            "OOM key limit reached ({} keys); raise POSTINO_MAX_KEYS or delete keys",
            ctx.max_keys
        ));
    }
    Ok(())
}

fn room_for(ctx: &Ctx, len: usize, adding: usize) -> Result<(), String> {
    if !ctx.replay && len.saturating_add(adding) > ctx.max_elements {
        return Err(format!(
            "OOM a collection holds at most {} elements (POSTINO_MAX_ELEMENTS)",
            ctx.max_elements
        ));
    }
    Ok(())
}

macro_rules! typed {
    ($get:ident, $get_mut:ident, $variant:ident, $ty:ty, $new:expr) => {
        #[allow(dead_code)]
        fn $get<'a>(db: &'a Db, key: &str, now: u64) -> Result<Option<&'a $ty>, String> {
            match db.get(key, now).map(|e| &e.value) {
                None => Ok(None),
                Some(Value::$variant(v)) => Ok(Some(v)),
                Some(_) => Err(WRONGTYPE.into()),
            }
        }

        #[allow(dead_code)]
        fn $get_mut<'a>(
            db: &'a mut Db,
            key: &str,
            ctx: &Ctx,
            create: bool,
        ) -> Result<Option<&'a mut $ty>, String> {
            match db.get(key, ctx.now).map(|e| &e.value) {
                Some(Value::$variant(_)) => {}
                Some(_) => return Err(WRONGTYPE.into()),
                None if create => {
                    admit(db, key, ctx)?;
                    db.insert(key.to_string(), Entry::new(Value::$variant($new)));
                }
                None => return Ok(None),
            }
            match db.get_mut(key, ctx.now).map(|e| &mut e.value) {
                Some(Value::$variant(v)) => Ok(Some(v)),
                _ => Ok(None),
            }
        }
    };
}

typed!(string_ref, string_mut, String, String, String::new());
typed!(list_ref, list_mut, List, VecDeque<String>, VecDeque::new());
typed!(hash_ref, hash_mut, Hash, BTreeMap<String, String>, BTreeMap::new());
typed!(set_ref, set_mut, Set, BTreeSet<String>, BTreeSet::new());
typed!(zset_ref, zset_mut, ZSet, ZSet, ZSet::default());
typed!(stream_ref, stream_mut, Stream, Stream, Stream::default());

/// Normalizes a range with negative indexes counting from the end into `0..len`.
/// `n` distinct indices below `len`, in random order, without copying the
/// collection: a partial shuffle when most are wanted, sampling otherwise.
fn sample_distinct(len: usize, n: usize) -> Vec<usize> {
    let n = n.min(len);
    if n * 2 >= len {
        let mut all: Vec<usize> = (0..len).collect();
        for i in 0..n {
            let j = i + random_below(len - i);
            all.swap(i, j);
        }
        all.truncate(n);
        all
    } else {
        let mut seen = std::collections::HashSet::with_capacity(n);
        let mut picked = Vec::with_capacity(n);
        while picked.len() < n {
            let i = random_below(len);
            if seen.insert(i) {
                picked.push(i);
            }
        }
        picked
    }
}

fn range_bounds(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        len + stop
    } else {
        stop.min(len - 1)
    };
    (start <= stop && start < len).then_some((start as usize, stop as usize))
}

// ---- stream ids ------------------------------------------------------------

fn parse_id(s: &str, missing_seq: u64) -> Result<Id, String> {
    let (ms, seq) = match s.split_once('-') {
        Some((ms, seq)) => (ms, seq.parse::<u64>().map_err(|_| BAD_ID.to_string())?),
        None => (s, missing_seq),
    };
    Ok(Id {
        ms: ms.parse().map_err(|_| BAD_ID.to_string())?,
        seq,
    })
}

fn range_start(s: &str) -> Result<Id, String> {
    match s {
        "-" => Ok(Id::default()),
        "+" => Ok(Id::MAX),
        _ => match s.strip_prefix('(') {
            Some(rest) => Id::after(parse_id(rest, 0)?)
                .ok_or_else(|| "ERR invalid start ID for the interval".to_string()),
            None => parse_id(s, 0),
        },
    }
}

fn range_end(s: &str) -> Result<Option<Id>, String> {
    match s {
        "-" => Ok(Some(Id::default())),
        "+" => Ok(Some(Id::MAX)),
        _ => match s.strip_prefix('(') {
            Some(rest) => {
                let id = parse_id(rest, u64::MAX)?;
                Ok(match (id.ms, id.seq) {
                    (0, 0) => None,
                    (ms, 0) => Some(Id {
                        ms: ms - 1,
                        seq: u64::MAX,
                    }),
                    (ms, seq) => Some(Id { ms, seq: seq - 1 }),
                })
            }
            None => parse_id(s, u64::MAX).map(Some),
        },
    }
}

fn entry_size(fields: &[(String, String)]) -> usize {
    fields.iter().map(|(f, v)| f.len() + v.len()).sum::<usize>() + 24
}

fn entry_reply(id: &Id, fields: &[(String, String)]) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(id.to_string()),
        Reply::Array(
            fields
                .iter()
                .flat_map(|(f, v)| [Reply::Bulk(f.clone()), Reply::Bulk(v.clone())])
                .collect(),
        ),
    ])
}

fn nogroup(key: &str, group: &str, command: &str) -> String {
    format!(
        "NOGROUP No such key '{key}' or consumer group '{group}' in {command} with GROUP option"
    )
}

// ---- execution -------------------------------------------------------------

struct Exec<'a> {
    db: &'a mut Db,
    ctx: &'a Ctx,
    argv: &'a [String],
    dirty: bool,
    propagate: Option<Vec<Vec<String>>>,
    events: Vec<(String, &'static str)>,
    block: Option<Block>,
}

impl Db {
    pub fn exec(&mut self, argv: &[String], ctx: &Ctx) -> Outcome {
        let mut exec = Exec {
            db: self,
            ctx,
            argv,
            dirty: false,
            propagate: None,
            events: Vec::new(),
            block: None,
        };
        let reply = match argv.first() {
            Some(_) => exec.dispatch(Name::of(argv).as_str()),
            None => Err("ERR empty command".into()),
        };
        let reply = reply.unwrap_or_else(Reply::Error);
        Outcome {
            dirty: exec.dirty && !reply.is_error(),
            reply,
            propagate: exec.propagate,
            events: exec.events,
            block: exec.block,
        }
    }
}

impl Exec<'_> {
    fn arg(&self, i: usize) -> &str {
        &self.argv[i]
    }

    fn arity(&self, min: usize) -> Result<(), String> {
        if self.argv.len() < min {
            Err(wrong_args(&self.argv[0]))
        } else {
            Ok(())
        }
    }

    fn exactly(&self, n: usize) -> Result<(), String> {
        if self.argv.len() != n {
            Err(wrong_args(&self.argv[0]))
        } else {
            Ok(())
        }
    }

    fn touch(&mut self, key: &str, event: &'static str) {
        self.dirty = true;
        self.events.push((key.to_string(), event));
    }

    fn now(&self) -> u64 {
        self.ctx.now
    }

    fn dispatch(&mut self, name: &str) -> R {
        match name {
            "PING" => match self.argv.len() {
                1 => Ok(Reply::Simple("PONG".into())),
                2 => Ok(Reply::Bulk(self.arg(1).to_string())),
                _ => Err(wrong_args("ping")),
            },
            "ECHO" => {
                self.exactly(2)?;
                Ok(Reply::Bulk(self.arg(1).to_string()))
            }
            "TIME" => {
                let micros = self.now() * 1000;
                Ok(Reply::bulks([
                    (micros / 1_000_000).to_string(),
                    (micros % 1_000_000).to_string(),
                ]))
            }
            "DBSIZE" => {
                let scope = &self.ctx.access.scope;
                // Every key held, counted without visiting them; one that has
                // just expired counts until the expire cycle removes it.
                Ok(Reply::int(if scope.is_all() {
                    self.db.entries().len()
                } else {
                    visible_keys(self.db, scope, "", None, self.now()).count()
                }))
            }
            "FLUSHDB" | "FLUSHALL" => {
                if !self.ctx.access.admin {
                    return Err("NOPERM this command requires the administrator token".into());
                }
                if !self.db.entries().is_empty() {
                    self.db.clear();
                    self.dirty = true;
                }
                Ok(Reply::ok())
            }
            "COMMAND" => match self.argv.get(1).map(|s| s.to_ascii_uppercase()) {
                Some(sub) if sub == "COUNT" => Ok(Reply::int(COMMANDS.len())),
                _ => Ok(Reply::bulks(
                    COMMANDS.iter().map(|c| c.to_ascii_lowercase()),
                )),
            },
            "DEL" | "UNLINK" => self.del(),
            "EXISTS" => {
                self.arity(2)?;
                let now = self.now();
                Ok(Reply::int(
                    self.argv[1..]
                        .iter()
                        .filter(|k| self.db.exists(k, now))
                        .count(),
                ))
            }
            "TYPE" => {
                self.exactly(2)?;
                Ok(Reply::Simple(
                    self.db
                        .get(self.arg(1), self.now())
                        .map_or("none", |e| e.value.type_name())
                        .to_string(),
                ))
            }
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => self.expire(name),
            "TTL" | "PTTL" | "EXPIRETIME" | "PEXPIRETIME" => self.ttl(name),
            "PERSIST" => {
                self.exactly(2)?;
                let key = self.arg(1).to_string();
                let now = self.now();
                match self.db.get_mut(&key, now) {
                    Some(entry) if entry.expires.is_some() => {
                        entry.expires = None;
                        self.touch(&key, "persist");
                        Ok(Reply::Int(1))
                    }
                    _ => Ok(Reply::Int(0)),
                }
            }
            "RENAME" | "RENAMENX" => self.rename(name == "RENAMENX"),
            // Sent alone, KEYS runs a run of keys at a time instead: see
            // [`crate::broker::Broker::command`].
            "KEYS" => {
                self.exactly(2)?;
                let pattern = Glob::client(self.arg(1))?;
                let scope = &self.ctx.access.scope;
                let (keys, more) =
                    keys_run(self.db, scope, &pattern, None, KEYS_WHOLE + 1, self.now());
                if more.is_some() {
                    return Err(format!(
                        "ERR KEYS in a batch looks at {KEYS_WHOLE} keys at most, while every other request waits; send it alone, or page through the keys with SCAN"
                    ));
                }
                collect_within(keys.into_iter().map(Reply::Bulk))
            }
            "SCAN" => self.scan(),

            "GET" => {
                self.exactly(2)?;
                Ok(string_ref(self.db, self.arg(1), self.now())?
                    .map_or(Reply::Nil, |s| Reply::Bulk(s.clone())))
            }
            "SET" => self.set(),
            "SETNX" => {
                self.exactly(3)?;
                let (key, value) = (self.arg(1).to_string(), self.arg(2).to_string());
                if self.db.exists(&key, self.now()) {
                    return Ok(Reply::Int(0));
                }
                self.store_string(&key, value, None)?;
                Ok(Reply::Int(1))
            }
            "SETEX" | "PSETEX" => {
                self.exactly(4)?;
                let amount = int(self.arg(2))?;
                if amount <= 0 {
                    return Err(format!(
                        "ERR invalid expire time in '{}' command",
                        name.to_ascii_lowercase()
                    ));
                }
                let unit = if name == "SETEX" { 1000 } else { 1 };
                let expires = expiry_at(self.now(), amount, unit, false).ok_or_else(|| {
                    format!(
                        "ERR invalid expire time in '{}' command",
                        name.to_ascii_lowercase()
                    )
                })?;
                let (key, value) = (self.arg(1).to_string(), self.arg(3).to_string());
                self.store_string(&key, value, Some(expires))?;
                Ok(Reply::ok())
            }
            "GETSET" => {
                self.exactly(3)?;
                let key = self.arg(1).to_string();
                let old = string_ref(self.db, &key, self.now())?.cloned();
                self.store_string(&key, self.arg(2).to_string(), None)?;
                Ok(old.map_or(Reply::Nil, Reply::Bulk))
            }
            "GETDEL" => {
                self.exactly(2)?;
                let key = self.arg(1).to_string();
                let old = string_ref(self.db, &key, self.now())?.cloned();
                if old.is_some() {
                    self.db.take(&key);
                    self.touch(&key, "del");
                }
                Ok(old.map_or(Reply::Nil, Reply::Bulk))
            }
            "GETEX" => self.getex(),
            "MGET" => {
                self.arity(2)?;
                let now = self.now();
                let mut bytes = 0;
                let mut values = Vec::with_capacity(self.argv.len() - 1);
                for key in &self.argv[1..] {
                    values.push(match self.db.get(key, now).map(|e| &e.value) {
                        Some(Value::String(s)) => {
                            bytes += s.len();
                            within_reply(bytes)?;
                            Reply::Bulk(s.clone())
                        }
                        _ => Reply::Nil,
                    });
                }
                Ok(Reply::Array(values))
            }
            "MSET" | "MSETNX" => {
                if self.argv.len() < 3 || self.argv.len().is_multiple_of(2) {
                    return Err(wrong_args(name));
                }
                let pairs: Vec<(String, String)> = self.argv[1..]
                    .chunks(2)
                    .map(|p| (p[0].clone(), p[1].clone()))
                    .collect();
                if name == "MSETNX" && pairs.iter().any(|(k, _)| self.db.exists(k, self.now())) {
                    return Ok(Reply::Int(0));
                }
                if let Some((key, _)) = pairs.iter().find(|(_, v)| v.len() > MAX_STRING) {
                    return Err(format!(
                        "ERR string for {key:?} exceeds maximum allowed size (16 MiB)"
                    ));
                }
                admit_all(self.db, pairs.iter().map(|(k, _)| k.as_str()), self.ctx)?;
                for (key, value) in pairs {
                    self.store_string(&key, value, None)?;
                }
                Ok(if name == "MSET" {
                    Reply::ok()
                } else {
                    Reply::Int(1)
                })
            }
            "APPEND" => {
                self.exactly(3)?;
                let key = self.arg(1).to_string();
                let suffix = self.arg(2).to_string();
                let current = string_ref(self.db, &key, self.now())?.map_or(0, String::len);
                if current + suffix.len() > MAX_STRING {
                    return Err("ERR string exceeds maximum allowed size (16 MiB)".into());
                }
                let value = string_mut(self.db, &key, self.ctx, true)?.ok_or(SYNTAX)?;
                value.push_str(&suffix);
                let len = value.len();
                self.touch(&key, "append");
                Ok(Reply::int(len))
            }
            "STRLEN" => {
                self.exactly(2)?;
                Ok(Reply::int(
                    string_ref(self.db, self.arg(1), self.now())?.map_or(0, String::len),
                ))
            }
            "INCR" | "DECR" | "INCRBY" | "DECRBY" => {
                let by = match name {
                    "INCR" | "DECR" => {
                        self.exactly(2)?;
                        1
                    }
                    _ => {
                        self.exactly(3)?;
                        int(self.arg(2))?
                    }
                };
                let by = if name.starts_with('D') {
                    by.checked_neg().ok_or(NOT_INT)?
                } else {
                    by
                };
                self.incr(by)
            }
            "INCRBYFLOAT" => {
                self.exactly(3)?;
                let key = self.arg(1).to_string();
                let by = float(self.arg(2))?;
                let current = match string_ref(self.db, &key, self.now())? {
                    Some(s) => float(s).map_err(|_| NOT_FLOAT.to_string())?,
                    None => 0.0,
                };
                let next = current + by;
                if !next.is_finite() {
                    return Err("ERR increment would produce NaN or Infinity".into());
                }
                let text = format_float(next);
                self.write_string_keep_ttl(&key, text.clone())?;
                self.touch(&key, "incrbyfloat");
                Ok(Reply::Bulk(text))
            }
            "DELIFEQ" => {
                self.exactly(3)?;
                let key = self.arg(1).to_string();
                let matches =
                    string_ref(self.db, &key, self.now())?.is_some_and(|v| v == self.arg(2));
                if matches {
                    self.db.take(&key);
                    self.touch(&key, "del");
                }
                Ok(Reply::bool(matches))
            }

            "HSET" | "HMSET" => {
                if self.argv.len() < 4 || self.argv.len() % 2 == 1 {
                    return Err(wrong_args(name));
                }
                let key = self.arg(1).to_string();
                let pairs: Vec<(String, String)> = self.argv[2..]
                    .chunks(2)
                    .map(|p| (p[0].clone(), p[1].clone()))
                    .collect();
                let current = hash_ref(self.db, &key, self.now())?.map_or(0, BTreeMap::len);
                room_for(self.ctx, current, pairs.len())?;
                let hash = hash_mut(self.db, &key, self.ctx, true)?.ok_or(SYNTAX)?;
                let added = pairs
                    .into_iter()
                    .filter(|(f, v)| hash.insert(f.clone(), v.clone()).is_none())
                    .count();
                self.touch(&key, "hset");
                Ok(if name == "HMSET" {
                    Reply::ok()
                } else {
                    Reply::int(added)
                })
            }
            "HSETNX" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                if hash_ref(self.db, &key, self.now())?.is_some_and(|h| h.contains_key(self.arg(2)))
                {
                    return Ok(Reply::Int(0));
                }
                let current = hash_ref(self.db, &key, self.now())?.map_or(0, BTreeMap::len);
                room_for(self.ctx, current, 1)?;
                let (field, value) = (self.arg(2).to_string(), self.arg(3).to_string());
                hash_mut(self.db, &key, self.ctx, true)?
                    .ok_or(SYNTAX)?
                    .insert(field, value);
                self.touch(&key, "hset");
                Ok(Reply::Int(1))
            }
            "HGET" => {
                self.exactly(3)?;
                Ok(hash_ref(self.db, self.arg(1), self.now())?
                    .and_then(|h| h.get(self.arg(2)))
                    .map_or(Reply::Nil, |v| Reply::Bulk(v.clone())))
            }
            "HMGET" => {
                self.arity(3)?;
                let hash = hash_ref(self.db, self.arg(1), self.now())?;
                let mut bytes = 0;
                let mut values = Vec::with_capacity(self.argv.len() - 2);
                for field in &self.argv[2..] {
                    values.push(match hash.and_then(|h| h.get(field)) {
                        Some(v) => {
                            bytes += v.len();
                            within_reply(bytes)?;
                            Reply::Bulk(v.clone())
                        }
                        None => Reply::Nil,
                    });
                }
                Ok(Reply::Array(values))
            }
            "HGETALL" => {
                self.exactly(2)?;
                let mut bytes = 0usize;
                let mut pairs = Vec::new();
                for (f, v) in hash_ref(self.db, self.arg(1), self.now())?
                    .into_iter()
                    .flatten()
                {
                    bytes += f.len() + v.len() + 8;
                    within_reply(bytes)?;
                    pairs.push((f.clone(), Reply::Bulk(v.clone())));
                }
                Ok(Reply::Map(pairs))
            }
            "HDEL" => {
                self.arity(3)?;
                let key = self.arg(1).to_string();
                let fields: Vec<String> = self.argv[2..].to_vec();
                let Some(hash) = hash_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let removed = fields.iter().filter(|f| hash.remove(*f).is_some()).count();
                self.db.prune(&key);
                if removed > 0 {
                    self.touch(&key, "hdel");
                }
                Ok(Reply::int(removed))
            }
            "HEXISTS" => {
                self.exactly(3)?;
                Ok(Reply::bool(
                    hash_ref(self.db, self.arg(1), self.now())?
                        .is_some_and(|h| h.contains_key(self.arg(2))),
                ))
            }
            "HLEN" => {
                self.exactly(2)?;
                Ok(Reply::int(
                    hash_ref(self.db, self.arg(1), self.now())?.map_or(0, BTreeMap::len),
                ))
            }
            "HKEYS" | "HVALS" => {
                self.exactly(2)?;
                let hash = hash_ref(self.db, self.arg(1), self.now())?;
                collect_within(
                    hash.into_iter()
                        .flatten()
                        .map(|(f, v)| Reply::Bulk(if name == "HKEYS" { f } else { v }.clone())),
                )
            }
            "HINCRBY" | "HINCRBYFLOAT" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                let field = self.arg(2).to_string();
                let current = hash_ref(self.db, &key, self.now())?;
                let existing = current.and_then(|h| h.get(&field)).cloned();
                let len = current.map_or(0, BTreeMap::len);
                if existing.is_none() {
                    room_for(self.ctx, len, 1)?;
                }
                let (text, reply) = if name == "HINCRBY" {
                    let current = match &existing {
                        Some(v) => {
                            int(v).map_err(|_| "ERR hash value is not an integer".to_string())?
                        }
                        None => 0,
                    };
                    let next = current
                        .checked_add(int(self.arg(3))?)
                        .ok_or("ERR increment or decrement would overflow")?;
                    (next.to_string(), Reply::Int(next))
                } else {
                    let current = match &existing {
                        Some(v) => {
                            float(v).map_err(|_| "ERR hash value is not a float".to_string())?
                        }
                        None => 0.0,
                    };
                    let next = current + float(self.arg(3))?;
                    if !next.is_finite() {
                        return Err("ERR increment would produce NaN or Infinity".into());
                    }
                    let text = format_float(next);
                    (text.clone(), Reply::Bulk(text))
                };
                hash_mut(self.db, &key, self.ctx, true)?
                    .ok_or(SYNTAX)?
                    .insert(field, text);
                self.touch(
                    &key,
                    if name == "HINCRBY" {
                        "hincrby"
                    } else {
                        "hincrbyfloat"
                    },
                );
                Ok(reply)
            }

            "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" => {
                self.arity(3)?;
                let key = self.arg(1).to_string();
                let items: Vec<String> = self.argv[2..].to_vec();
                let only_existing = name.ends_with('X');
                let current = list_ref(self.db, &key, self.now())?;
                if only_existing && current.is_none() {
                    return Ok(Reply::Int(0));
                }
                room_for(self.ctx, current.map_or(0, VecDeque::len), items.len())?;
                let list = list_mut(self.db, &key, self.ctx, true)?.ok_or(SYNTAX)?;
                let left = name.starts_with('L');
                for item in items {
                    if left {
                        list.push_front(item);
                    } else {
                        list.push_back(item);
                    }
                }
                let len = list.len();
                self.touch(&key, if left { "lpush" } else { "rpush" });
                Ok(Reply::int(len))
            }
            "LPOP" | "RPOP" => {
                if !(2..=3).contains(&self.argv.len()) {
                    return Err(wrong_args(name));
                }
                let key = self.arg(1).to_string();
                let wanted = self.argv.get(2).map(|c| count(c)).transpose()?;
                let left = name == "LPOP";
                let Some(list) = list_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Nil);
                };
                let mut popped = Vec::new();
                for _ in 0..wanted.unwrap_or(1) {
                    match if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    } {
                        Some(item) => popped.push(item),
                        None => break,
                    }
                }
                self.db.prune(&key);
                if !popped.is_empty() {
                    self.touch(&key, if left { "lpop" } else { "rpop" });
                }
                Ok(match wanted {
                    Some(_) => Reply::bulks(popped),
                    None => popped.pop().map_or(Reply::Nil, Reply::Bulk),
                })
            }
            "LLEN" => {
                self.exactly(2)?;
                Ok(Reply::int(
                    list_ref(self.db, self.arg(1), self.now())?.map_or(0, VecDeque::len),
                ))
            }
            "LRANGE" => {
                self.exactly(4)?;
                let (start, stop) = (int(self.arg(2))?, int(self.arg(3))?);
                let list = list_ref(self.db, self.arg(1), self.now())?;
                let Some((l, (a, b))) =
                    list.and_then(|l| Some((l, range_bounds(start, stop, l.len())?)))
                else {
                    return Ok(Reply::Array(Vec::new()));
                };
                collect_within(l.range(a..=b).map(|v| Reply::Bulk(v.clone())))
            }
            "LINDEX" => {
                self.exactly(3)?;
                let index = int(self.arg(2))?;
                let list = list_ref(self.db, self.arg(1), self.now())?;
                Ok(list
                    .and_then(|l| {
                        let i = if index < 0 {
                            l.len() as i64 + index
                        } else {
                            index
                        };
                        usize::try_from(i).ok().and_then(|i| l.get(i))
                    })
                    .map_or(Reply::Nil, |v| Reply::Bulk(v.clone())))
            }
            "LSET" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                let index = int(self.arg(2))?;
                let value = self.arg(3).to_string();
                let list = list_mut(self.db, &key, self.ctx, false)?.ok_or("ERR no such key")?;
                let i = if index < 0 {
                    list.len() as i64 + index
                } else {
                    index
                };
                let slot = usize::try_from(i)
                    .ok()
                    .and_then(|i| list.get_mut(i))
                    .ok_or("ERR index out of range")?;
                *slot = value;
                self.touch(&key, "lset");
                Ok(Reply::ok())
            }
            "LREM" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                let limit = int(self.arg(2))?;
                let value = self.arg(3).to_string();
                let Some(list) = list_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let max = if limit == 0 {
                    usize::MAX
                } else {
                    limit.unsigned_abs() as usize
                };
                let mut removed = 0;
                if limit >= 0 {
                    let mut i = 0;
                    while i < list.len() && removed < max {
                        if list[i] == value {
                            list.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    let mut i = list.len();
                    while i > 0 && removed < max {
                        i -= 1;
                        if list[i] == value {
                            list.remove(i);
                            removed += 1;
                        }
                    }
                }
                self.db.prune(&key);
                if removed > 0 {
                    self.touch(&key, "lrem");
                }
                Ok(Reply::int(removed))
            }
            "LTRIM" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                let (start, stop) = (int(self.arg(2))?, int(self.arg(3))?);
                let Some(list) = list_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::ok());
                };
                let before = list.len();
                match range_bounds(start, stop, list.len()) {
                    Some((a, b)) => {
                        list.truncate(b + 1);
                        list.drain(..a);
                    }
                    None => list.clear(),
                }
                let changed = list.len() != before;
                self.db.prune(&key);
                if changed {
                    self.touch(&key, "ltrim");
                }
                Ok(Reply::ok())
            }
            "LINSERT" => {
                self.exactly(5)?;
                let key = self.arg(1).to_string();
                let before = match self.arg(2) {
                    w if is(w, "BEFORE") => true,
                    w if is(w, "AFTER") => false,
                    _ => return Err(SYNTAX.into()),
                };
                let (pivot, value) = (self.arg(3).to_string(), self.arg(4).to_string());
                let len = list_ref(self.db, &key, self.now())?.map_or(0, VecDeque::len);
                let Some(list) = list_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let Some(at) = list.iter().position(|v| *v == pivot) else {
                    return Ok(Reply::Int(-1));
                };
                room_for(self.ctx, len, 1)?;
                list.insert(if before { at } else { at + 1 }, value);
                let len = list.len();
                self.touch(&key, "linsert");
                Ok(Reply::int(len))
            }
            "LMOVE" => {
                self.exactly(5)?;
                let (from_left, to_left) = (self.side(3)?, self.side(4)?);
                self.lmove(
                    self.arg(1).to_string(),
                    self.arg(2).to_string(),
                    from_left,
                    to_left,
                )
            }
            "RPOPLPUSH" => {
                self.exactly(3)?;
                self.lmove(
                    self.arg(1).to_string(),
                    self.arg(2).to_string(),
                    false,
                    true,
                )
            }
            "BLPOP" | "BRPOP" => self.blpop(name == "BLPOP"),
            "BLMOVE" => {
                self.exactly(6)?;
                let (from_left, to_left) = (self.side(3)?, self.side(4)?);
                let timeout = self.seconds_timeout(5)?;
                let (src, dst) = (self.arg(1).to_string(), self.arg(2).to_string());
                if list_ref(self.db, &src, self.now())?.is_some_and(|l| !l.is_empty()) {
                    let reply = self.lmove(src.clone(), dst.clone(), from_left, to_left)?;
                    let side = |left: bool| if left { "LEFT" } else { "RIGHT" }.to_string();
                    self.propagate = Some(vec![vec![
                        "LMOVE".into(),
                        src,
                        dst,
                        side(from_left),
                        side(to_left),
                    ]]);
                    return Ok(reply);
                }
                self.wait(timeout);
                Ok(Reply::Nil)
            }

            "SADD" => {
                self.arity(3)?;
                let key = self.arg(1).to_string();
                let members: Vec<String> = self.argv[2..].to_vec();
                let current = set_ref(self.db, &key, self.now())?;
                let new = members
                    .iter()
                    .filter(|m| !current.is_some_and(|s| s.contains(*m)))
                    .collect::<BTreeSet<_>>()
                    .len();
                room_for(self.ctx, current.map_or(0, BTreeSet::len), new)?;
                let set = set_mut(self.db, &key, self.ctx, true)?.ok_or(SYNTAX)?;
                let added = members
                    .into_iter()
                    .filter(|m| set.insert(m.clone()))
                    .count();
                if added > 0 {
                    self.touch(&key, "sadd");
                }
                self.db.prune(&key);
                Ok(Reply::int(added))
            }
            "SREM" => {
                self.arity(3)?;
                let key = self.arg(1).to_string();
                let members: Vec<String> = self.argv[2..].to_vec();
                let Some(set) = set_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let removed = members.iter().filter(|m| set.remove(*m)).count();
                self.db.prune(&key);
                if removed > 0 {
                    self.touch(&key, "srem");
                }
                Ok(Reply::int(removed))
            }
            "SMEMBERS" => {
                self.exactly(2)?;
                collect_within(
                    set_ref(self.db, self.arg(1), self.now())?
                        .into_iter()
                        .flatten()
                        .map(|m| Reply::Bulk(m.clone())),
                )
            }
            "SISMEMBER" => {
                self.exactly(3)?;
                Ok(Reply::bool(
                    set_ref(self.db, self.arg(1), self.now())?
                        .is_some_and(|s| s.contains(self.arg(2))),
                ))
            }
            "SMISMEMBER" => {
                self.arity(3)?;
                let set = set_ref(self.db, self.arg(1), self.now())?;
                Ok(Reply::Array(
                    self.argv[2..]
                        .iter()
                        .map(|m| Reply::bool(set.is_some_and(|s| s.contains(m))))
                        .collect(),
                ))
            }
            "SCARD" => {
                self.exactly(2)?;
                Ok(Reply::int(
                    set_ref(self.db, self.arg(1), self.now())?.map_or(0, BTreeSet::len),
                ))
            }
            "SPOP" | "SRANDMEMBER" => self.spop(name == "SPOP"),
            "SINTER" | "SUNION" | "SDIFF" => {
                self.arity(2)?;
                let now = self.now();
                // Each set is read where it is, and once however often it is
                // named: a copy of each would cost the memory of all of them.
                let mut named = Vec::with_capacity(self.argv.len() - 1);
                for key in &self.argv[1..] {
                    named.push(set_ref(self.db, key, now)?);
                }
                let empty = BTreeSet::new();
                let first = named[0].unwrap_or(&empty);
                let mut others: Vec<&BTreeSet<String>> = Vec::new();
                for set in named[1..].iter().flatten() {
                    if !others.iter().any(|seen| std::ptr::eq(*seen, *set)) {
                        others.push(set);
                    }
                }
                let members: Vec<&String> = match name {
                    "SINTER" if named.iter().any(Option::is_none) => Vec::new(),
                    "SINTER" => {
                        // The smallest set is the one to go through.
                        let mut all = vec![first];
                        all.extend(others.iter().filter(|s| !std::ptr::eq(**s, first)));
                        all.sort_by_key(|s| s.len());
                        let (smallest, rest) = all.split_first().expect("one set at least");
                        smallest
                            .iter()
                            .filter(|m| rest.iter().all(|s| s.contains(*m)))
                            .collect()
                    }
                    "SUNION" => {
                        // Refused as soon as the members found would make too
                        // long a reply, before the rest are gone through.
                        let mut union: BTreeSet<&String> = BTreeSet::new();
                        let mut bytes = 0;
                        let distinct = others.iter().filter(|s| !std::ptr::eq(**s, first));
                        for set in std::iter::once(&first).chain(distinct) {
                            for member in set.iter() {
                                if union.insert(member) {
                                    bytes += member.len();
                                    within_reply(bytes)?;
                                }
                            }
                        }
                        union.into_iter().collect()
                    }
                    _ if others.iter().any(|s| std::ptr::eq(*s, first)) => Vec::new(),
                    _ => first
                        .iter()
                        .filter(|m| !others.iter().any(|s| s.contains(*m)))
                        .collect(),
                };
                collect_within(members.into_iter().map(|m| Reply::Bulk(m.clone())))
            }
            "SMOVE" => {
                self.exactly(4)?;
                let (src, dst, member) = (
                    self.arg(1).to_string(),
                    self.arg(2).to_string(),
                    self.arg(3).to_string(),
                );
                let now = self.now();
                let Some(source) = set_ref(self.db, &src, now)? else {
                    return Ok(Reply::Int(0));
                };
                let (source_len, present) = (source.len(), source.contains(&member));
                let dest = set_ref(self.db, &dst, now)?;
                if src == dst || !present {
                    return Ok(Reply::bool(present));
                }
                // Whatever can refuse the move is checked before anything moves.
                let creating = dest.is_none();
                if !dest.is_some_and(|d| d.contains(&member)) {
                    room_for(self.ctx, dest.map_or(0, BTreeSet::len), 1)?;
                    if creating && source_len > 1 {
                        admit(self.db, &dst, self.ctx)?;
                    }
                }
                set_mut(self.db, &src, self.ctx, false)?
                    .ok_or(SYNTAX)?
                    .remove(&member);
                self.db.prune(&src);
                if creating {
                    // Admitted above, or in the place of the emptied source.
                    self.db
                        .insert(dst.clone(), Entry::new(Value::Set(BTreeSet::new())));
                }
                set_mut(self.db, &dst, self.ctx, false)?
                    .ok_or(SYNTAX)?
                    .insert(member);
                self.touch(&src, "srem");
                self.touch(&dst, "sadd");
                Ok(Reply::Int(1))
            }

            "ZADD" => self.zadd(),
            "ZREM" => {
                self.arity(3)?;
                let key = self.arg(1).to_string();
                let members: Vec<String> = self.argv[2..].to_vec();
                let Some(zset) = zset_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let removed = members.iter().filter(|m| zset.remove(m)).count();
                self.db.prune(&key);
                if removed > 0 {
                    self.touch(&key, "zrem");
                }
                Ok(Reply::int(removed))
            }
            "ZSCORE" => {
                self.exactly(3)?;
                Ok(zset_ref(self.db, self.arg(1), self.now())?
                    .and_then(|z| z.scores.get(self.arg(2)))
                    .map_or(Reply::Nil, |s| Reply::Bulk(format_float(*s))))
            }
            "ZMSCORE" => {
                self.arity(3)?;
                let zset = zset_ref(self.db, self.arg(1), self.now())?;
                Ok(Reply::Array(
                    self.argv[2..]
                        .iter()
                        .map(|m| {
                            zset.and_then(|z| z.scores.get(m.as_str()))
                                .map_or(Reply::Nil, |s| Reply::Bulk(format_float(*s)))
                        })
                        .collect(),
                ))
            }
            "ZINCRBY" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                let by = float(self.arg(2))?;
                let member = self.arg(3).to_string();
                let current = zset_ref(self.db, &key, self.now())?;
                let old = current.and_then(|z| z.scores.get(member.as_str())).copied();
                if old.is_none() {
                    room_for(self.ctx, current.map_or(0, ZSet::len), 1)?;
                }
                let next = old.unwrap_or(0.0) + by;
                if next.is_nan() {
                    return Err("ERR resulting score is not a number (NaN)".into());
                }
                zset_mut(self.db, &key, self.ctx, true)?
                    .ok_or(SYNTAX)?
                    .insert(&member, next);
                self.touch(&key, "zincr");
                Ok(Reply::Bulk(format_float(next)))
            }
            "ZCARD" => {
                self.exactly(2)?;
                Ok(Reply::int(
                    zset_ref(self.db, self.arg(1), self.now())?.map_or(0, ZSet::len),
                ))
            }
            "ZCOUNT" => {
                self.exactly(4)?;
                let (min, max) = (score_bound(self.arg(2))?, score_bound(self.arg(3))?);
                Ok(Reply::int(
                    zset_ref(self.db, self.arg(1), self.now())?
                        .map_or(0, |z| z.by_score(min, max).count()),
                ))
            }
            "ZRANK" | "ZREVRANK" => {
                self.exactly(3)?;
                let zset = zset_ref(self.db, self.arg(1), self.now())?;
                Ok(zset
                    .and_then(|z| {
                        z.rank(self.arg(2)).map(
                            |r| {
                                if name == "ZRANK" { r } else { z.len() - 1 - r }
                            },
                        )
                    })
                    .map_or(Reply::Nil, Reply::int))
            }
            "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" => self.zrange(name),
            "ZPOPMIN" | "ZPOPMAX" => {
                if !(2..=3).contains(&self.argv.len()) {
                    return Err(wrong_args(name));
                }
                let key = self.arg(1).to_string();
                let n = self.argv.get(2).map(|c| count(c)).transpose()?.unwrap_or(1);
                Ok(Reply::Array(self.zpop(&key, name == "ZPOPMIN", n)?))
            }
            "BZPOPMIN" | "BZPOPMAX" => {
                self.arity(3)?;
                let timeout = self.seconds_timeout(self.argv.len() - 1)?;
                let min = name == "BZPOPMIN";
                let keys: Vec<String> = self.argv[1..self.argv.len() - 1].to_vec();
                for key in keys {
                    if zset_ref(self.db, &key, self.now())?.is_some_and(|z| !z.is_empty()) {
                        let mut popped = self.zpop(&key, min, 1)?;
                        self.propagate = Some(vec![vec![
                            if min { "ZPOPMIN" } else { "ZPOPMAX" }.into(),
                            key.clone(),
                        ]]);
                        let mut reply = vec![Reply::Bulk(key)];
                        reply.append(&mut popped);
                        return Ok(Reply::Array(reply));
                    }
                }
                self.wait(timeout);
                Ok(Reply::Nil)
            }
            "ZREMRANGEBYSCORE" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                let (min, max) = (score_bound(self.arg(2))?, score_bound(self.arg(3))?);
                let Some(zset) = zset_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let doomed: Vec<Arc<str>> =
                    zset.by_score(min, max).map(|(_, m)| m.clone()).collect();
                for member in &doomed {
                    zset.remove(member);
                }
                self.db.prune(&key);
                if !doomed.is_empty() {
                    self.touch(&key, "zremrangebyscore");
                }
                Ok(Reply::int(doomed.len()))
            }
            "ZREMRANGEBYRANK" => {
                self.exactly(4)?;
                let key = self.arg(1).to_string();
                let (start, stop) = (int(self.arg(2))?, int(self.arg(3))?);
                let Some(zset) = zset_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let doomed: Vec<Arc<str>> = match range_bounds(start, stop, zset.len()) {
                    Some((a, b)) => zset
                        .order
                        .iter()
                        .skip(a)
                        .take(b - a + 1)
                        .map(|(_, m)| m.clone())
                        .collect(),
                    None => Vec::new(),
                };
                for member in &doomed {
                    zset.remove(member);
                }
                self.db.prune(&key);
                if !doomed.is_empty() {
                    self.touch(&key, "zremrangebyrank");
                }
                Ok(Reply::int(doomed.len()))
            }

            "XADD" => self.xadd(),
            "XLEN" => {
                self.exactly(2)?;
                Ok(Reply::int(
                    stream_ref(self.db, self.arg(1), self.now())?.map_or(0, |s| s.entries.len()),
                ))
            }
            "XRANGE" | "XREVRANGE" => self.xrange(name == "XREVRANGE"),
            "XDEL" => {
                self.arity(3)?;
                let key = self.arg(1).to_string();
                let ids = self.argv[2..]
                    .iter()
                    .map(|s| parse_id(s, 0))
                    .collect::<Result<Vec<Id>, String>>()?;
                let Some(stream) = stream_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let mut removed = 0;
                for id in ids {
                    if stream.entries.remove(&id).is_some() {
                        stream.max_deleted = stream.max_deleted.max(id);
                        removed += 1;
                    }
                }
                if removed > 0 {
                    self.touch(&key, "xdel");
                }
                Ok(Reply::int(removed))
            }
            "XTRIM" => {
                self.arity(4)?;
                let key = self.arg(1).to_string();
                let (trim, next) = self.trim_spec(2)?;
                if next != self.argv.len() {
                    return Err(SYNTAX.into());
                }
                let Some(stream) = stream_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let removed = trim.apply(stream);
                if removed > 0 {
                    self.touch(&key, "xtrim");
                }
                Ok(Reply::int(removed))
            }
            "XREAD" => self.xread(),
            "XGROUP" => self.xgroup(),
            "XREADGROUP" => self.xreadgroup(),
            "XACK" => {
                self.arity(4)?;
                let (key, group) = (self.arg(1).to_string(), self.arg(2).to_string());
                let ids = self.argv[3..]
                    .iter()
                    .map(|s| parse_id(s, 0))
                    .collect::<Result<Vec<Id>, String>>()?;
                let Some(stream) = stream_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let Some(group) = stream.groups.get_mut(&group) else {
                    return Ok(Reply::Int(0));
                };
                let acked = ids
                    .iter()
                    .filter(|id| group.pending.remove(id).is_some())
                    .count();
                if acked > 0 {
                    self.dirty = true;
                }
                Ok(Reply::int(acked))
            }
            "XPENDING" => self.xpending(),
            "XCLAIM" => self.xclaim(),
            "XAUTOCLAIM" => self.xautoclaim(),
            "XINFO" => self.xinfo(),
            _ => Err(format!(
                "ERR unknown command '{}', with args beginning with: {}",
                self.argv[0],
                self.argv[1..]
                    .iter()
                    .take(3)
                    .map(|a| format!("'{a}'"))
                    .collect::<Vec<_>>()
                    .join(" ")
            )),
        }
    }

    // ---- keys --------------------------------------------------------------

    fn del(&mut self) -> R {
        self.arity(2)?;
        let now = self.now();
        let mut removed = 0;
        let argv = self.argv;
        for key in &argv[1..] {
            if self.db.delete(key, now) {
                removed += 1;
                self.touch(key, "del");
            }
        }
        Ok(Reply::int(removed))
    }

    fn expire(&mut self, name: &str) -> R {
        self.arity(3)?;
        let key = self.arg(1).to_string();
        let amount = int(self.arg(2))?;
        let now = self.now() as i64;
        let at = match name {
            "EXPIRE" => amount.checked_mul(1000).and_then(|ms| ms.checked_add(now)),
            "PEXPIRE" => amount.checked_add(now),
            "EXPIREAT" => amount.checked_mul(1000),
            _ => Some(amount),
        }
        .ok_or_else(|| {
            format!(
                "ERR invalid expire time in '{}' command",
                name.to_ascii_lowercase()
            )
        })?;
        let mut flags = (false, false, false, false);
        for flag in &self.argv[3..] {
            match flag.to_ascii_uppercase().as_str() {
                "NX" => flags.0 = true,
                "XX" => flags.1 = true,
                "GT" => flags.2 = true,
                "LT" => flags.3 = true,
                _ => return Err(format!("ERR Unsupported option {flag}")),
            }
        }
        let (nx, xx, gt, lt) = flags;
        if (nx && (xx || gt || lt)) || (gt && lt) {
            return Err(
                "ERR NX and XX, GT or LT options at the same time are not compatible".into(),
            );
        }
        let now = self.now();
        let Some(entry) = self.db.get_mut(&key, now) else {
            return Ok(Reply::Int(0));
        };
        let current = entry.expires;
        let allowed = match current {
            None => !xx && !gt,
            Some(existing) => {
                let existing = i64::try_from(existing).unwrap_or(i64::MAX);
                !nx && (!gt || at > existing) && (!lt || at < existing)
            }
        };
        if !allowed {
            return Ok(Reply::Int(0));
        }
        if at <= now as i64 {
            self.db.delete(&key, now);
            self.touch(&key, "del");
        } else {
            entry.expires = Some(at as u64);
            self.db.deadline(&key, at as u64);
            self.touch(&key, "expire");
        }
        Ok(Reply::Int(1))
    }

    fn ttl(&self, name: &str) -> R {
        self.exactly(2)?;
        let now = self.now();
        Ok(Reply::Int(match self.db.get(self.arg(1), now) {
            None => -2,
            Some(Entry { expires: None, .. }) => -1,
            Some(Entry {
                expires: Some(at), ..
            }) => {
                let (at, now) = (i128::from(*at), i128::from(now));
                let value = match name {
                    "TTL" => (at - now + 500) / 1000,
                    "PTTL" => at - now,
                    "EXPIRETIME" => at / 1000,
                    _ => at,
                };
                i64::try_from(value).unwrap_or(i64::MAX)
            }
        }))
    }

    fn rename(&mut self, nx: bool) -> R {
        self.exactly(3)?;
        let (from, to) = (self.arg(1).to_string(), self.arg(2).to_string());
        let now = self.now();
        if !self.db.exists(&from, now) {
            return Err("ERR no such key".into());
        }
        if from == to {
            return Ok(if nx { Reply::Int(0) } else { Reply::ok() });
        }
        if nx && self.db.exists(&to, now) {
            return Ok(Reply::Int(0));
        }
        let entry = self.db.remove(&from, now).ok_or("ERR no such key")?;
        if let Some(at) = entry.expires {
            self.db.deadline(&to, at);
        }
        self.db.insert(to.clone(), entry);
        self.touch(&from, "rename_from");
        self.touch(&to, "rename_to");
        Ok(if nx { Reply::Int(1) } else { Reply::ok() })
    }

    /// Walks the keyspace in key order. The cursor names the last key of the
    /// previous page, so a walk costs one pass however many pages it takes.
    fn scan(&self) -> R {
        self.arity(2)?;
        let after = match self.arg(1) {
            "0" => None,
            cursor => Some(
                from_hex(cursor)
                    .and_then(|bytes| String::from_utf8(bytes).ok())
                    .ok_or("ERR invalid cursor")?,
            ),
        };
        let mut pattern = None;
        let mut batch = 10;
        let mut kind = None;
        let mut i = 2;
        while i < self.argv.len() {
            let value = self.argv.get(i + 1).ok_or(SYNTAX)?;
            match self.arg(i).to_ascii_uppercase().as_str() {
                "MATCH" => pattern = Some(Glob::client(value)?),
                "COUNT" => match int(value)? {
                    n if n >= 1 => batch = (n as usize).min(SCAN_MOST),
                    _ => return Err(SYNTAX.into()),
                },
                "TYPE" => kind = Some(value.to_ascii_lowercase()),
                _ => return Err(SYNTAX.into()),
            }
            i += 2;
        }
        let now = self.now();
        let scope = &self.ctx.access.scope;
        let prefix = pattern.as_ref().map_or("", |p| literal_prefix(p.as_str()));
        let keys: Vec<(&String, &Entry)> =
            visible_keys(self.db, scope, prefix, after.as_deref(), now)
                .take(batch)
                .collect();
        let next = match keys.last() {
            Some((last, _)) if keys.len() == batch => to_hex(last.as_bytes()),
            _ => "0".to_string(),
        };
        let found = keys
            .into_iter()
            .filter(|(k, _)| pattern.as_ref().is_none_or(|p| p.matches(k)))
            .filter(|(_, e)| kind.as_deref().is_none_or(|t| e.value.type_name() == t))
            .map(|(k, _)| k.clone());
        Ok(Reply::Array(vec![Reply::Bulk(next), Reply::bulks(found)]))
    }

    // ---- strings -----------------------------------------------------------

    fn store_string(
        &mut self,
        key: &str,
        value: String,
        expires: Option<u64>,
    ) -> Result<(), String> {
        if value.len() > MAX_STRING {
            return Err("ERR string exceeds maximum allowed size (16 MiB)".into());
        }
        let entry = Entry {
            value: Value::String(value),
            expires,
        };
        // A key already held needs no room, and keeps its name.
        if let Some(entry) = self.db.replace(key, entry) {
            admit(self.db, key, self.ctx)?;
            self.db.insert(key.to_string(), entry);
        }
        if let Some(at) = expires {
            self.db.deadline(key, at);
        }
        self.touch(key, "set");
        Ok(())
    }

    fn write_string_keep_ttl(&mut self, key: &str, value: String) -> Result<(), String> {
        let expires = self.db.get(key, self.now()).and_then(|e| e.expires);
        admit(self.db, key, self.ctx)?;
        self.db.insert(
            key.to_string(),
            Entry {
                value: Value::String(value),
                expires,
            },
        );
        Ok(())
    }

    fn incr(&mut self, by: i64) -> R {
        let key = self.arg(1).to_string();
        let current = match string_ref(self.db, &key, self.now())? {
            Some(s) => int(s)?,
            None => 0,
        };
        let next = current
            .checked_add(by)
            .ok_or("ERR increment or decrement would overflow")?;
        self.write_string_keep_ttl(&key, next.to_string())?;
        self.touch(&key, "incrby");
        Ok(Reply::Int(next))
    }

    /// SET key value [NX | XX | IFEQ current] [GET] [EX s | PX ms | EXAT ts |
    /// PXAT ms-ts | KEEPTTL]. IFEQ makes the write a compare-and-set.
    fn set(&mut self) -> R {
        self.arity(3)?;
        let argv = self.argv;
        let key = argv[1].as_str();
        let value = argv[2].clone();
        let now = self.now();
        let (mut nx, mut xx, mut get, mut keep) = (false, false, false, false);
        let mut ifeq: Option<&str> = None;
        let mut expires: Option<u64> = None;
        let mut i = 3;
        while i < self.argv.len() {
            let word = self.arg(i).to_ascii_uppercase();
            let mut amount = || -> Result<i64, String> {
                i += 1;
                let n = int(self.argv.get(i).ok_or(SYNTAX)?)?;
                if n <= 0 {
                    return Err("ERR invalid expire time in 'set' command".into());
                }
                Ok(n)
            };
            match word.as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GET" => get = true,
                "KEEPTTL" => keep = true,
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    let unit = if word.starts_with('E') { 1000 } else { 1 };
                    expires = Some(
                        expiry_at(now, amount()?, unit, word.ends_with("AT"))
                            .ok_or("ERR invalid expire time in 'set' command")?,
                    );
                }
                "IFEQ" => {
                    i += 1;
                    ifeq = Some(argv.get(i).ok_or(SYNTAX)?);
                }
                _ => return Err(SYNTAX.into()),
            }
            i += 1;
        }
        if [nx, xx, ifeq.is_some()].iter().filter(|f| **f).count() > 1
            || (keep && expires.is_some())
        {
            return Err(SYNTAX.into());
        }
        // Nothing to look at first: whatever the key holds is replaced.
        if !(nx || xx || get || keep || ifeq.is_some()) {
            self.store_string(key, value, expires)?;
            return Ok(Reply::ok());
        }
        let current = self.db.get(key, now);
        let old = match current.map(|e| &e.value) {
            Some(Value::String(s)) => Some(s.as_str()),
            Some(_) if get || ifeq.is_some() => return Err(WRONGTYPE.into()),
            _ => None,
        };
        let exists = current.is_some();
        let allowed =
            (!nx || !exists) && (!xx || exists) && ifeq.is_none_or(|want| old == Some(want));
        let reply = match (get, allowed) {
            (true, _) => old.map_or(Reply::Nil, |old| Reply::Bulk(old.to_string())),
            (false, true) => Reply::ok(),
            (false, false) => Reply::Nil,
        };
        if allowed {
            let expires = if keep {
                current.and_then(|e| e.expires)
            } else {
                expires
            };
            self.store_string(key, value, expires)?;
        }
        Ok(reply)
    }

    fn getex(&mut self) -> R {
        self.arity(2)?;
        let key = self.arg(1).to_string();
        let now = self.now();
        let value = string_ref(self.db, &key, now)?.cloned();
        let mut change: Option<Option<u64>> = None;
        let mut i = 2;
        while i < self.argv.len() {
            let word = self.arg(i).to_ascii_uppercase();
            if word == "PERSIST" {
                change = Some(None);
                i += 1;
                continue;
            }
            let n = int(self.argv.get(i + 1).ok_or(SYNTAX)?)?;
            if n <= 0 {
                return Err("ERR invalid expire time in 'getex' command".into());
            }
            if !matches!(word.as_str(), "EX" | "PX" | "EXAT" | "PXAT") {
                return Err(SYNTAX.into());
            }
            let unit = if word.starts_with('E') { 1000 } else { 1 };
            let at = expiry_at(now, n, unit, word.ends_with("AT"))
                .ok_or("ERR invalid expire time in 'getex' command")?;
            change = Some(Some(at));
            i += 2;
        }
        if let (Some(expires), Some(_)) = (change, &value)
            && let Some(entry) = self.db.get_mut(&key, now)
        {
            entry.expires = expires;
            if let Some(at) = expires {
                self.db.deadline(&key, at);
            }
            self.touch(
                &key,
                if expires.is_some() {
                    "expire"
                } else {
                    "persist"
                },
            );
        }
        Ok(value.map_or(Reply::Nil, Reply::Bulk))
    }

    // ---- lists -------------------------------------------------------------

    fn side(&self, i: usize) -> Result<bool, String> {
        match self.arg(i) {
            w if is(w, "LEFT") => Ok(true),
            w if is(w, "RIGHT") => Ok(false),
            _ => Err(SYNTAX.into()),
        }
    }

    /// A timeout in seconds, as milliseconds rounded up: one shorter than a
    /// millisecond still ends, rather than wait as long as 0 does.
    fn seconds_timeout(&self, i: usize) -> Result<u64, String> {
        let seconds = float(self.arg(i))
            .map_err(|_| "ERR timeout is not a float or out of range".to_string())?;
        if seconds < 0.0 {
            return Err("ERR timeout is negative".into());
        }
        let ms = (seconds * 1000.0).ceil();
        if ms > i64::MAX as f64 {
            return Err("ERR timeout is out of range".into());
        }
        Ok(ms as u64)
    }

    /// Asks the broker to retry this command when the keyspace changes.
    fn wait(&mut self, timeout_ms: u64) {
        if self.ctx.blocking {
            self.block = Some(Block {
                retry: self.argv.to_vec(),
                timeout_ms,
            });
        }
    }

    fn lmove(&mut self, src: String, dst: String, from_left: bool, to_left: bool) -> R {
        let now = self.now();
        let Some(source_len) = list_ref(self.db, &src, now)?.map(VecDeque::len) else {
            return Ok(Reply::Nil);
        };
        let dest_len = list_ref(self.db, &dst, now)?.map(VecDeque::len);
        if source_len == 0 {
            return Ok(Reply::Nil);
        }
        // Whatever can refuse the move is checked before anything moves. An
        // emptied source frees its key for a new destination.
        if src != dst {
            room_for(self.ctx, dest_len.unwrap_or(0), 1)?;
            if dest_len.is_none() && source_len > 1 {
                admit(self.db, &dst, self.ctx)?;
            }
        }
        let source = list_mut(self.db, &src, self.ctx, false)?.ok_or(SYNTAX)?;
        let item = if from_left {
            source.pop_front()
        } else {
            source.pop_back()
        }
        .ok_or(SYNTAX)?;
        // A list rotated onto itself keeps its key, and its expiry.
        if src != dst {
            self.db.prune(&src);
        }
        if dest_len.is_none() {
            // Admitted above, or in the place of the emptied source.
            self.db
                .insert(dst.clone(), Entry::new(Value::List(VecDeque::new())));
        }
        let dest = list_mut(self.db, &dst, self.ctx, false)?.ok_or(SYNTAX)?;
        if to_left {
            dest.push_front(item.clone());
        } else {
            dest.push_back(item.clone());
        }
        self.touch(&src, if from_left { "lpop" } else { "rpop" });
        self.touch(&dst, if to_left { "lpush" } else { "rpush" });
        Ok(Reply::Bulk(item))
    }

    fn blpop(&mut self, left: bool) -> R {
        self.arity(3)?;
        let timeout = self.seconds_timeout(self.argv.len() - 1)?;
        let keys: Vec<String> = self.argv[1..self.argv.len() - 1].to_vec();
        for key in keys {
            if let Some(list) = list_mut(self.db, &key, self.ctx, false)?
                && let Some(item) = if left {
                    list.pop_front()
                } else {
                    list.pop_back()
                }
            {
                self.db.prune(&key);
                self.touch(&key, if left { "lpop" } else { "rpop" });
                self.propagate = Some(vec![vec![
                    if left { "LPOP" } else { "RPOP" }.into(),
                    key.clone(),
                ]]);
                return Ok(Reply::bulks([key, item]));
            }
        }
        self.wait(timeout);
        Ok(Reply::Nil)
    }

    // ---- sets --------------------------------------------------------------

    fn spop(&mut self, pop: bool) -> R {
        if !(2..=3).contains(&self.argv.len()) {
            return Err(wrong_args(&self.argv[0]));
        }
        let key = self.arg(1).to_string();
        let wanted = self.argv.get(2).map(|c| int(c)).transpose()?;
        let Some(set) = set_ref(self.db, &key, self.now())? else {
            return Ok(if wanted.is_some() {
                Reply::Array(Vec::new())
            } else {
                Reply::Nil
            });
        };
        let len = set.len();
        let picked: Vec<String> = match wanted {
            None => set
                .iter()
                .nth(random_below(len))
                .cloned()
                .into_iter()
                .collect(),
            Some(n) if n >= 0 || pop => {
                if n < 0 {
                    return Err("ERR value is out of range, must be positive".into());
                }
                let n = usize::try_from(n).unwrap_or(usize::MAX).min(len);
                // One walk over the set picks every sampled position, in
                // member order.
                let mut positions = sample_distinct(len, n);
                positions.sort_unstable();
                let mut positions = positions.into_iter().peekable();
                let mut picked = Vec::with_capacity(n);
                for (i, member) in set.iter().enumerate() {
                    if positions.peek().is_none() {
                        break;
                    }
                    if positions.next_if_eq(&i).is_some() {
                        picked.push(member.clone());
                    }
                }
                picked
            }
            // A negative count for SRANDMEMBER allows repeats, so the reply can
            // be far larger than the set: bound it.
            Some(n) => {
                let count = n.unsigned_abs();
                if count > self.ctx.max_elements as u64 {
                    return Err(format!(
                        "ERR count exceeds the element limit ({})",
                        self.ctx.max_elements
                    ));
                }
                let members: Vec<&String> = set.iter().collect();
                let mut bytes = 0usize;
                let mut picked = Vec::new();
                for _ in 0..count {
                    let member = members[random_below(members.len())];
                    bytes += member.len();
                    within_reply(bytes)?;
                    picked.push(member.clone());
                }
                picked
            }
        };
        if pop && !picked.is_empty() {
            let set = set_mut(self.db, &key, self.ctx, false)?.ok_or(SYNTAX)?;
            for member in &picked {
                set.remove(member);
            }
            self.db.prune(&key);
            self.touch(&key, "spop");
            let mut srem = vec!["SREM".to_string(), key.clone()];
            srem.extend(picked.iter().cloned());
            self.propagate = Some(vec![srem]);
        }
        Ok(match wanted {
            None => Reply::Bulk(picked.into_iter().next().unwrap_or_default()),
            Some(_) => Reply::bulks(picked),
        })
    }

    // ---- sorted sets -------------------------------------------------------

    fn zadd(&mut self) -> R {
        self.arity(4)?;
        let key = self.arg(1).to_string();
        let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
            (false, false, false, false, false, false);
        let mut i = 2;
        while i < self.argv.len() {
            match self.arg(i).to_ascii_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GT" => gt = true,
                "LT" => lt = true,
                "CH" => ch = true,
                "INCR" => incr = true,
                _ => break,
            }
            i += 1;
        }
        let rest = &self.argv[i..];
        if rest.is_empty() || rest.len() % 2 == 1 {
            return Err(SYNTAX.into());
        }
        if nx && xx {
            return Err("ERR XX and NX options at the same time are not compatible".into());
        }
        if (gt && lt) || (nx && (gt || lt)) {
            return Err("ERR GT, LT, and/or NX options at the same time are not compatible".into());
        }
        if incr && rest.len() != 2 {
            return Err("ERR INCR option supports a single increment-element pair".into());
        }
        let pairs = rest
            .chunks(2)
            .map(|p| Ok((float(&p[0])?, p[1].clone())))
            .collect::<Result<Vec<(f64, String)>, String>>()?;
        let current = zset_ref(self.db, &key, self.now())?;
        let new = pairs
            .iter()
            .filter(|(_, m)| !current.is_some_and(|z| z.scores.contains_key(m.as_str())))
            .count();
        if !xx {
            room_for(self.ctx, current.map_or(0, ZSet::len), new)?;
        }
        if xx && current.is_none() {
            return Ok(if incr { Reply::Nil } else { Reply::Int(0) });
        }
        let zset = zset_mut(self.db, &key, self.ctx, true)?.ok_or(SYNTAX)?;
        let (mut added, mut changed) = (0, 0);
        let mut last = None;
        for (score, member) in pairs {
            let old = zset.scores.get(member.as_str()).copied();
            let score = if incr {
                old.unwrap_or(0.0) + score
            } else {
                score
            };
            if score.is_nan() {
                return Err("ERR resulting score is not a number (NaN)".into());
            }
            let allowed = match old {
                None => !xx,
                Some(old) => !nx && (!gt || score > old) && (!lt || score < old),
            };
            if !allowed {
                continue;
            }
            last = Some(score);
            if old.is_none() {
                added += 1;
            } else if old != Some(score) {
                changed += 1;
            }
            zset.insert(&member, score);
        }
        self.db.prune(&key);
        if added + changed > 0 {
            self.touch(&key, if incr { "zincr" } else { "zadd" });
        }
        Ok(if incr {
            last.map_or(Reply::Nil, |s| Reply::Bulk(format_float(s)))
        } else {
            Reply::int(if ch { added + changed } else { added })
        })
    }

    fn zpop(&mut self, key: &str, min: bool, n: usize) -> Result<Vec<Reply>, String> {
        let Some(zset) = zset_mut(self.db, key, self.ctx, false)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for _ in 0..n {
            let next = if min {
                zset.order.first().cloned()
            } else {
                zset.order.last().cloned()
            };
            let Some((score, member)) = next else { break };
            zset.remove(&member);
            out.push(Reply::Bulk(member.to_string()));
            out.push(Reply::Bulk(format_float(score.0)));
        }
        self.db.prune(key);
        if !out.is_empty() {
            self.touch(key, if min { "zpopmin" } else { "zpopmax" });
        }
        Ok(out)
    }

    /// ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES],
    /// plus the older ZREVRANGE, ZRANGEBYSCORE, and ZREVRANGEBYSCORE forms.
    fn zrange(&mut self, name: &str) -> R {
        self.arity(4)?;
        let mut by_score = name.contains("BYSCORE");
        let mut rev = name.starts_with("ZREV");
        let (mut with_scores, mut limit) = (false, None);
        let mut i = 4;
        while i < self.argv.len() {
            match self.arg(i).to_ascii_uppercase().as_str() {
                "BYSCORE" if name == "ZRANGE" => by_score = true,
                "REV" if name == "ZRANGE" => rev = true,
                "WITHSCORES" => with_scores = true,
                "LIMIT" => {
                    let offset = int(self.argv.get(i + 1).ok_or(SYNTAX)?)?;
                    let count = int(self.argv.get(i + 2).ok_or(SYNTAX)?)?;
                    limit = Some((offset.max(0) as usize, count));
                    i += 2;
                }
                _ => return Err(SYNTAX.into()),
            }
            i += 1;
        }
        let Some(zset) = zset_ref(self.db, self.arg(1), self.now())? else {
            return Ok(Reply::Array(Vec::new()));
        };
        let ordered: Box<dyn Iterator<Item = &Ranked>> = if rev {
            Box::new(zset.order.iter().rev())
        } else {
            Box::new(zset.order.iter())
        };
        let picked: Vec<&Ranked> = if by_score {
            // Reverse ranges list the maximum before the minimum.
            let (lo, hi) = if rev {
                (self.arg(3), self.arg(2))
            } else {
                (self.arg(2), self.arg(3))
            };
            let (min, max) = (score_bound(lo)?, score_bound(hi)?);
            let matching: Box<dyn Iterator<Item = &Ranked>> = if rev {
                Box::new(zset.by_score(min, max).rev())
            } else {
                Box::new(zset.by_score(min, max))
            };
            match limit {
                Some((offset, count)) if count >= 0 => {
                    matching.skip(offset).take(count as usize).collect()
                }
                Some((offset, _)) => matching.skip(offset).collect(),
                None => matching.collect(),
            }
        } else {
            if limit.is_some() {
                return Err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX".into());
            }
            let (start, stop) = (int(self.arg(2))?, int(self.arg(3))?);
            match range_bounds(start, stop, zset.len()) {
                Some((a, b)) => ordered.skip(a).take(b - a + 1).collect(),
                None => Vec::new(),
            }
        };
        collect_within(picked.into_iter().flat_map(|(score, member)| {
            let mut items = vec![Reply::Bulk(member.to_string())];
            if with_scores {
                items.push(Reply::Bulk(format_float(score.0)));
            }
            items
        }))
    }

    // ---- streams -----------------------------------------------------------

    fn trim_spec(&self, at: usize) -> Result<(Trim, usize), String> {
        let kind = self.arg(at).to_ascii_uppercase();
        let mut i = at + 1;
        if matches!(self.argv.get(i).map(String::as_str), Some("=" | "~")) {
            i += 1;
        }
        let threshold = self.argv.get(i).ok_or(SYNTAX)?;
        let trim = match kind.as_str() {
            "MAXLEN" => Trim::MaxLen(count(threshold)?),
            "MINID" => Trim::MinId(parse_id(threshold, 0)?),
            _ => return Err(SYNTAX.into()),
        };
        i += 1;
        if self.argv.get(i).is_some_and(|w| is(w, "LIMIT")) {
            i += 2;
        }
        Ok((trim, i))
    }

    fn xadd(&mut self) -> R {
        self.arity(5)?;
        let key = self.arg(1).to_string();
        let mut i = 2;
        let mut nomkstream = false;
        let mut trim = None;
        loop {
            let word = self.argv.get(i).ok_or(SYNTAX)?.to_ascii_uppercase();
            match word.as_str() {
                "NOMKSTREAM" => {
                    nomkstream = true;
                    i += 1;
                }
                "MAXLEN" | "MINID" => {
                    let (spec, next) = self.trim_spec(i)?;
                    trim = Some(spec);
                    i = next;
                }
                _ => break,
            }
        }
        let id_arg = self.argv.get(i).ok_or(SYNTAX)?.clone();
        let fields = &self.argv[i + 1..];
        if fields.is_empty() || fields.len() % 2 == 1 {
            return Err(wrong_args("xadd"));
        }
        let fields: Vec<(String, String)> = fields
            .chunks(2)
            .map(|p| (p[0].clone(), p[1].clone()))
            .collect();
        let now = self.now();
        let existing = stream_ref(self.db, &key, now)?;
        if existing.is_none() && nomkstream {
            return Ok(Reply::Nil);
        }
        let last = existing.map_or(Id::default(), |s| s.last_id);
        let id = match id_arg.as_str() {
            "*" => Id::next_after(last, now).ok_or(
                "ERR The stream has exhausted the last possible ID, unable to add more items",
            )?,
            arg => match arg.strip_suffix("-*") {
                Some(ms) => {
                    let ms: u64 = ms.parse().map_err(|_| BAD_ID.to_string())?;
                    match ms.cmp(&last.ms) {
                        std::cmp::Ordering::Greater => Id { ms, seq: 0 },
                        std::cmp::Ordering::Equal => Id {
                            ms,
                            seq: last.seq.checked_add(1).ok_or(BAD_ID)?,
                        },
                        std::cmp::Ordering::Less => {
                            return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
                        }
                    }
                }
                None => parse_id(arg, 0)?,
            },
        };
        if id == Id::default() {
            return Err("ERR The ID specified in XADD must be greater than 0-0".into());
        }
        if id <= last {
            return Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .into(),
            );
        }
        // What has to fit is the stream once trimmed: an entry dropped by the
        // trim makes room for the new one.
        let len = existing.map_or(0, |s| s.entries.len());
        let dropped = match &trim {
            None => 0,
            Some(Trim::MaxLen(n)) => (len + 1).saturating_sub(*n),
            Some(Trim::MinId(min)) => {
                existing.map_or(0, |s| s.entries.range(..*min).count()) + usize::from(id < *min)
            }
        };
        if dropped == 0 {
            room_for(self.ctx, len, 1)?;
        }
        let stream = stream_mut(self.db, &key, self.ctx, true)?.ok_or(SYNTAX)?;
        stream.entries.insert(id, fields);
        stream.last_id = id;
        stream.added += 1;
        if let Some(trim) = trim {
            trim.apply(stream);
        }
        self.touch(&key, "xadd");
        Ok(Reply::Bulk(id.to_string()))
    }

    fn xrange(&self, rev: bool) -> R {
        self.arity(4)?;
        let (lo, hi) = if rev { (3, 2) } else { (2, 3) };
        let start = range_start(self.arg(lo))?;
        let end = range_end(self.arg(hi))?;
        let limit = match self.argv.get(4) {
            Some(w) if is(w, "COUNT") => count(self.argv.get(5).ok_or(SYNTAX)?)?,
            Some(_) => return Err(SYNTAX.into()),
            None => usize::MAX,
        };
        let Some(stream) = stream_ref(self.db, self.arg(1), self.now())? else {
            return Ok(Reply::Array(Vec::new()));
        };
        let Some(end) = end.filter(|end| start <= *end) else {
            return Ok(Reply::Array(Vec::new()));
        };
        let range = stream.entries.range(start..=end);
        if rev {
            collect_within(range.rev().take(limit).map(|(id, f)| entry_reply(id, f)))
        } else {
            collect_within(range.take(limit).map(|(id, f)| entry_reply(id, f)))
        }
    }

    /// Splits `[COUNT n] [BLOCK ms] [NOACK] STREAMS k1.. id1..` options.
    fn read_options(&self, from: usize) -> Result<ReadOptions, String> {
        let mut options = ReadOptions::default();
        let mut i = from;
        loop {
            let word = self.argv.get(i).ok_or(SYNTAX)?.to_ascii_uppercase();
            match word.as_str() {
                "COUNT" => {
                    options.count = Some(count(self.argv.get(i + 1).ok_or(SYNTAX)?)?);
                    i += 2;
                }
                "BLOCK" => {
                    let ms = int(self.argv.get(i + 1).ok_or(SYNTAX)?)
                        .map_err(|_| "ERR timeout is not an integer or out of range".to_string())?;
                    if ms < 0 {
                        return Err("ERR timeout is negative".into());
                    }
                    options.block = Some(ms as u64);
                    options.block_at = Some(i);
                    i += 2;
                }
                "NOACK" => {
                    options.noack = true;
                    i += 1;
                }
                "STREAMS" => {
                    let rest = &self.argv[i + 1..];
                    if rest.is_empty() || rest.len() % 2 == 1 {
                        return Err("ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.".into());
                    }
                    let half = rest.len() / 2;
                    options.keys = rest[..half].to_vec();
                    options.ids = rest[half..].to_vec();
                    options.ids_at = i + 1 + half;
                    return Ok(options);
                }
                _ => return Err(SYNTAX.into()),
            }
        }
    }

    fn xread(&mut self) -> R {
        self.arity(4)?;
        let options = self.read_options(1)?;
        let now = self.now();
        let mut after = Vec::new();
        for (key, id) in options.keys.iter().zip(&options.ids) {
            let stream = stream_ref(self.db, key, now)?;
            after.push(match id.as_str() {
                "$" => stream.map_or(Id::default(), |s| s.last_id),
                id => parse_id(id, 0)?,
            });
        }
        let limit = options.count.unwrap_or(usize::MAX).max(1);
        let mut out = Vec::new();
        // A stream named several times is read as often.
        let mut bytes = 0;
        for (key, after) in options.keys.iter().zip(&after) {
            let Some(stream) = stream_ref(self.db, key, now)? else {
                continue;
            };
            let mut entries = Vec::new();
            for (id, fields) in stream
                .entries
                .range((Excluded(*after), Unbounded))
                .take(limit)
            {
                bytes += entry_size(fields);
                within_reply(bytes)?;
                entries.push(entry_reply(id, fields));
            }
            if !entries.is_empty() {
                out.push(Reply::Array(vec![
                    Reply::Bulk(key.clone()),
                    Reply::Array(entries),
                ]));
            }
        }
        if !out.is_empty() {
            return Ok(Reply::Array(out));
        }
        if let Some(timeout) = options.block
            && self.ctx.blocking
        {
            // Retry with `$` resolved, so entries added while waiting are seen.
            let mut retry: Vec<String> = self.argv[..options.ids_at].to_vec();
            if let Some(at) = options.block_at {
                retry.drain(at..at + 2);
            }
            retry.extend(after.iter().map(Id::to_string));
            self.block = Some(Block {
                retry,
                timeout_ms: timeout,
            });
        }
        Ok(Reply::Nil)
    }

    fn xgroup(&mut self) -> R {
        self.arity(2)?;
        let sub = self.arg(1).to_ascii_uppercase();
        if sub == "HELP" {
            return Ok(Reply::bulks([
                "XGROUP CREATE <key> <group> <id|$> [MKSTREAM] [ENTRIESREAD <n>]",
                "XGROUP SETID <key> <group> <id|$>",
                "XGROUP DESTROY <key> <group>",
                "XGROUP CREATECONSUMER <key> <group> <consumer>",
                "XGROUP DELCONSUMER <key> <group> <consumer>",
            ]));
        }
        self.arity(4)?;
        let (key, group) = (self.arg(2).to_string(), self.arg(3).to_string());
        let now = self.now();
        let resolve = |stream: &Stream, id: &str| -> Result<Id, String> {
            if id == "$" {
                Ok(stream.last_id)
            } else {
                parse_id(id, 0)
            }
        };
        match sub.as_str() {
            "CREATE" => {
                self.arity(5)?;
                let mkstream = self.argv[5..].iter().any(|w| is(w, "MKSTREAM"));
                let id_arg = self.arg(4).to_string();
                // A bad id must fail before MKSTREAM creates anything.
                if id_arg != "$" {
                    parse_id(&id_arg, 0)?;
                }
                if stream_ref(self.db, &key, now)?.is_none() {
                    if !mkstream {
                        return Err("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".into());
                    }
                    stream_mut(self.db, &key, self.ctx, true)?;
                }
                let stream = stream_mut(self.db, &key, self.ctx, false)?.ok_or(SYNTAX)?;
                if stream.groups.contains_key(&group) {
                    return Err("BUSYGROUP Consumer Group name already exists".into());
                }
                let last_delivered = resolve(stream, &id_arg)?;
                stream.groups.insert(
                    group,
                    Group {
                        last_delivered,
                        ..Group::default()
                    },
                );
                self.touch(&key, "xgroup-create");
                Ok(Reply::ok())
            }
            "SETID" => {
                self.arity(5)?;
                let id_arg = self.arg(4).to_string();
                let stream = stream_mut(self.db, &key, self.ctx, false)?
                    .ok_or_else(|| nogroup(&key, &group, "XGROUP"))?;
                let id = resolve(stream, &id_arg)?;
                let record = stream
                    .groups
                    .get_mut(&group)
                    .ok_or_else(|| nogroup(&key, &group, "XGROUP"))?;
                record.last_delivered = id;
                self.touch(&key, "xgroup-setid");
                Ok(Reply::ok())
            }
            "DESTROY" => {
                let Some(stream) = stream_mut(self.db, &key, self.ctx, false)? else {
                    return Ok(Reply::Int(0));
                };
                let removed = stream.groups.remove(&group).is_some();
                if removed {
                    self.touch(&key, "xgroup-destroy");
                }
                Ok(Reply::bool(removed))
            }
            "CREATECONSUMER" | "DELCONSUMER" => {
                self.exactly(5)?;
                let consumer = self.arg(4).to_string();
                let stream = stream_mut(self.db, &key, self.ctx, false)?
                    .ok_or_else(|| nogroup(&key, &group, "XGROUP"))?;
                let record = stream
                    .groups
                    .get_mut(&group)
                    .ok_or_else(|| nogroup(&key, &group, "XGROUP"))?;
                if sub == "CREATECONSUMER" {
                    if record.consumers.contains_key(&consumer) {
                        return Ok(Reply::Int(0));
                    }
                    record.consumers.insert(
                        consumer,
                        Consumer {
                            seen: now,
                            active: None,
                        },
                    );
                    self.touch(&key, "xgroup-createconsumer");
                    Ok(Reply::Int(1))
                } else {
                    let before = record.pending.len();
                    record.pending.retain(|_, p| p.consumer != consumer);
                    let dropped = before - record.pending.len();
                    if record.consumers.remove(&consumer).is_some() {
                        self.touch(&key, "xgroup-delconsumer");
                    }
                    Ok(Reply::int(dropped))
                }
            }
            _ => Err(format!(
                "ERR unknown subcommand '{}'. Try XGROUP HELP.",
                self.arg(1)
            )),
        }
    }

    fn xreadgroup(&mut self) -> R {
        self.arity(7)?;
        if !is(self.arg(1), "GROUP") {
            return Err(SYNTAX.into());
        }
        let (group, consumer) = (self.arg(2).to_string(), self.arg(3).to_string());
        let options = self.read_options(4)?;
        let now = self.now();
        let limit = options.count.unwrap_or(usize::MAX).max(1);
        // Validate every stream and group, and the size of the pending
        // entries asked for again, before changing anything. Named twice, a
        // stream would hand out again what this command just delivered.
        let mut history = 0;
        for (i, (key, id)) in options.keys.iter().zip(&options.ids).enumerate() {
            if options.keys[..i].contains(key) {
                return Err(format!(
                    "ERR stream {key:?} is named twice; name each stream once"
                ));
            }
            let stream = stream_ref(self.db, key, now)?;
            let Some((entries, record)) =
                stream.and_then(|s| Some((&s.entries, s.groups.get(&group)?)))
            else {
                return Err(nogroup(key, &group, "XREADGROUP"));
            };
            if id != ">" {
                let after = parse_id(id, 0)?;
                for (id, _) in record
                    .pending
                    .range((Excluded(after), Unbounded))
                    .filter(|(_, p)| p.consumer == consumer)
                    .take(limit)
                {
                    history += entries.get(id).map_or(0, |fields| entry_size(fields));
                    within_reply(history)?;
                }
            }
        }
        let mut out = Vec::new();
        let mut any_new = false;
        for (key, id) in options.keys.iter().zip(&options.ids) {
            let history = if id == ">" {
                None
            } else {
                Some(parse_id(id, 0)?)
            };
            let stream = stream_mut(self.db, key, self.ctx, false)?.ok_or(SYNTAX)?;
            let Stream {
                entries, groups, ..
            } = stream;
            let record = groups.get_mut(&group).ok_or(SYNTAX)?;
            let created = !record.consumers.contains_key(&consumer);
            let seat = record.consumers.entry(consumer.clone()).or_default();
            seat.seen = now;
            let mut items = Vec::new();
            match history {
                None => {
                    let fresh: Vec<Id> = entries
                        .range((Excluded(record.last_delivered), Unbounded))
                        .take(limit)
                        .map(|(id, _)| *id)
                        .collect();
                    if !fresh.is_empty() {
                        seat.active = Some(now);
                    }
                    for id in fresh {
                        record.last_delivered = id;
                        record.read += 1;
                        if !options.noack {
                            record.pending.insert(
                                id,
                                Pending {
                                    consumer: consumer.clone(),
                                    delivered: now,
                                    count: 1,
                                },
                            );
                        }
                        items.push(entry_reply(&id, &entries[&id]));
                    }
                    any_new |= !items.is_empty();
                }
                Some(after) => {
                    for (id, _) in record
                        .pending
                        .range((Excluded(after), Unbounded))
                        .filter(|(_, p)| p.consumer == consumer)
                        .take(limit)
                    {
                        items.push(match entries.get(id) {
                            Some(fields) => entry_reply(id, fields),
                            None => Reply::Array(vec![Reply::Bulk(id.to_string()), Reply::Nil]),
                        });
                    }
                }
            }
            if created || (!items.is_empty() && history.is_none()) {
                self.dirty = true;
            }
            if history.is_some() || !items.is_empty() {
                out.push(Reply::Array(vec![
                    Reply::Bulk(key.clone()),
                    Reply::Array(items),
                ]));
            }
        }
        let all_new = options.ids.iter().all(|id| id == ">");
        if !any_new && all_new {
            if let Some(timeout) = options.block
                && self.ctx.blocking
            {
                let mut retry = self.argv.to_vec();
                if let Some(at) = options.block_at {
                    retry.drain(at..at + 2);
                }
                self.block = Some(Block {
                    retry,
                    timeout_ms: timeout,
                });
            }
            return Ok(Reply::Nil);
        }
        Ok(Reply::Array(out))
    }

    fn xpending(&self) -> R {
        self.arity(3)?;
        let (key, group) = (self.arg(1), self.arg(2));
        let now = self.now();
        let record = stream_ref(self.db, key, now)?
            .and_then(|s| s.groups.get(group))
            .ok_or_else(|| nogroup(key, group, "XPENDING"))?;
        if self.argv.len() == 3 {
            if record.pending.is_empty() {
                return Ok(Reply::Array(vec![
                    Reply::Int(0),
                    Reply::Nil,
                    Reply::Nil,
                    Reply::Nil,
                ]));
            }
            let mut per: BTreeMap<&str, usize> = BTreeMap::new();
            for pending in record.pending.values() {
                *per.entry(pending.consumer.as_str()).or_default() += 1;
            }
            return Ok(Reply::Array(vec![
                Reply::int(record.pending.len()),
                Reply::Bulk(
                    record
                        .pending
                        .keys()
                        .next()
                        .map(Id::to_string)
                        .unwrap_or_default(),
                ),
                Reply::Bulk(
                    record
                        .pending
                        .keys()
                        .next_back()
                        .map(Id::to_string)
                        .unwrap_or_default(),
                ),
                Reply::Array(
                    per.into_iter()
                        .map(|(c, n)| Reply::bulks([c.to_string(), n.to_string()]))
                        .collect(),
                ),
            ]));
        }
        let mut i = 3;
        let mut min_idle = 0;
        if is(self.arg(i), "IDLE") {
            min_idle = count(self.argv.get(i + 1).ok_or(SYNTAX)?)? as u64;
            i += 2;
        }
        if self.argv.len() < i + 3 {
            return Err(SYNTAX.into());
        }
        let start = range_start(self.arg(i))?;
        let Some(end) = range_end(self.arg(i + 1))? else {
            return Ok(Reply::Array(Vec::new()));
        };
        let limit = count(self.arg(i + 2))?;
        let consumer = self.argv.get(i + 3);
        if start > end {
            return Ok(Reply::Array(Vec::new()));
        }
        Ok(Reply::Array(
            record
                .pending
                .range(start..=end)
                .filter(|(_, p)| consumer.is_none_or(|c| &p.consumer == c))
                .filter(|(_, p)| now.saturating_sub(p.delivered) >= min_idle)
                .take(limit)
                .map(|(id, p)| {
                    Reply::Array(vec![
                        Reply::Bulk(id.to_string()),
                        Reply::Bulk(p.consumer.clone()),
                        Reply::Int(now.saturating_sub(p.delivered) as i64),
                        Reply::Int(p.count as i64),
                    ])
                })
                .collect(),
        ))
    }

    /// XCLAIM key group consumer min-idle-time id.. [IDLE ms] [TIME ms]
    /// [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]
    fn xclaim(&mut self) -> R {
        self.arity(6)?;
        let (key, group, consumer) = (
            self.arg(1).to_string(),
            self.arg(2).to_string(),
            self.arg(3).to_string(),
        );
        let min_idle = count(self.arg(4))? as u64;
        let now = self.now();
        let mut ids = Vec::new();
        let mut i = 5;
        while i < self.argv.len() {
            match parse_id(self.arg(i), 0) {
                Ok(id) => ids.push(id),
                Err(_) => break,
            }
            i += 1;
        }
        let (mut delivered, mut retry, mut force, mut justid) = (now, None, false, false);
        while i < self.argv.len() {
            let word = self.arg(i).to_ascii_uppercase();
            let value = self.argv.get(i + 1).map(String::as_str).ok_or(SYNTAX);
            match word.as_str() {
                "IDLE" => {
                    delivered = now.saturating_sub(count(value?)? as u64);
                    i += 1;
                }
                "TIME" => {
                    delivered = count(value?)? as u64;
                    i += 1;
                }
                "RETRYCOUNT" => {
                    retry = Some(count(value?)? as u64);
                    i += 1;
                }
                "LASTID" => i += 1,
                "FORCE" => force = true,
                "JUSTID" => justid = true,
                _ => return Err(SYNTAX.into()),
            }
            i += 1;
        }
        let stream = stream_mut(self.db, &key, self.ctx, false)?
            .ok_or_else(|| nogroup(&key, &group, "XCLAIM"))?;
        let Stream {
            entries, groups, ..
        } = stream;
        let record = groups
            .get_mut(&group)
            .ok_or_else(|| nogroup(&key, &group, "XCLAIM"))?;
        let mut out = Vec::new();
        for id in ids {
            match record.pending.entry(id) {
                Slot::Occupied(_) => {}
                Slot::Vacant(slot) if force && entries.contains_key(&id) => {
                    slot.insert(Pending {
                        consumer: consumer.clone(),
                        delivered,
                        count: 0,
                    });
                }
                Slot::Vacant(_) => continue,
            }
            let Some(fields) = entries.get(&id) else {
                record.pending.remove(&id);
                self.dirty = true;
                continue;
            };
            let pending = record.pending.get_mut(&id).ok_or(SYNTAX)?;
            if now.saturating_sub(pending.delivered) < min_idle && pending.count > 0 {
                continue;
            }
            pending.consumer = consumer.clone();
            pending.delivered = delivered;
            pending.count = retry.unwrap_or(pending.count + u64::from(!justid));
            out.push(if justid {
                Reply::Bulk(id.to_string())
            } else {
                entry_reply(&id, fields)
            });
        }
        if !out.is_empty() {
            record.consumers.entry(consumer).or_default().seen = now;
            self.dirty = true;
        }
        Ok(Reply::Array(out))
    }

    /// XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]
    fn xautoclaim(&mut self) -> R {
        self.arity(6)?;
        let (key, group, consumer) = (
            self.arg(1).to_string(),
            self.arg(2).to_string(),
            self.arg(3).to_string(),
        );
        let min_idle = count(self.arg(4))? as u64;
        let start = range_start(self.arg(5))?;
        let (mut limit, mut justid) = (100, false);
        let mut i = 6;
        while i < self.argv.len() {
            match self.arg(i).to_ascii_uppercase().as_str() {
                "COUNT" => {
                    limit = count(self.argv.get(i + 1).ok_or(SYNTAX)?)?.max(1);
                    i += 1;
                }
                "JUSTID" => justid = true,
                _ => return Err(SYNTAX.into()),
            }
            i += 1;
        }
        let now = self.now();
        let stream = stream_mut(self.db, &key, self.ctx, false)?
            .ok_or_else(|| nogroup(&key, &group, "XAUTOCLAIM"))?;
        let Stream {
            entries, groups, ..
        } = stream;
        let record = groups
            .get_mut(&group)
            .ok_or_else(|| nogroup(&key, &group, "XAUTOCLAIM"))?;
        let candidates: Vec<Id> = record
            .pending
            .range(start..)
            .take(limit)
            .map(|(id, _)| *id)
            .collect();
        let next = candidates
            .last()
            .and_then(|last| record.pending.range((Excluded(*last), Unbounded)).next())
            .map_or(Id::default(), |(id, _)| *id);
        let (mut claimed, mut deleted) = (Vec::new(), Vec::new());
        for id in candidates {
            let Some(fields) = entries.get(&id) else {
                record.pending.remove(&id);
                deleted.push(Reply::Bulk(id.to_string()));
                continue;
            };
            let pending = record.pending.get_mut(&id).ok_or(SYNTAX)?;
            if now.saturating_sub(pending.delivered) < min_idle {
                continue;
            }
            pending.consumer = consumer.clone();
            pending.delivered = now;
            if !justid {
                pending.count += 1;
            }
            claimed.push(if justid {
                Reply::Bulk(id.to_string())
            } else {
                entry_reply(&id, fields)
            });
        }
        if !claimed.is_empty() || !deleted.is_empty() {
            record.consumers.entry(consumer).or_default().seen = now;
            self.dirty = true;
        }
        Ok(Reply::Array(vec![
            Reply::Bulk(next.to_string()),
            Reply::Array(claimed),
            Reply::Array(deleted),
        ]))
    }

    fn xinfo(&self) -> R {
        self.arity(2)?;
        let sub = self.arg(1).to_ascii_uppercase();
        match sub.as_str() {
            "HELP" => {
                return Ok(Reply::bulks([
                    "XINFO STREAM <key>",
                    "XINFO GROUPS <key>",
                    "XINFO CONSUMERS <key> <group>",
                ]));
            }
            "STREAM" | "GROUPS" | "CONSUMERS" => self.arity(3)?,
            _ => return Err(format!("ERR unknown subcommand '{}'", self.arg(1))),
        }
        let key = self.arg(2);
        let now = self.now();
        let stream = stream_ref(self.db, key, now)?.ok_or("ERR no such key")?;
        match sub.as_str() {
            "STREAM" => {
                let edge = |entry: Option<(&Id, &Vec<(String, String)>)>| {
                    entry.map_or(Reply::Nil, |(id, f)| entry_reply(id, f))
                };
                Ok(Reply::Map(vec![
                    ("length".into(), Reply::int(stream.entries.len())),
                    (
                        "last-generated-id".into(),
                        Reply::Bulk(stream.last_id.to_string()),
                    ),
                    (
                        "max-deleted-entry-id".into(),
                        Reply::Bulk(stream.max_deleted.to_string()),
                    ),
                    ("entries-added".into(), Reply::Int(stream.added as i64)),
                    ("groups".into(), Reply::int(stream.groups.len())),
                    ("first-entry".into(), edge(stream.entries.iter().next())),
                    ("last-entry".into(), edge(stream.entries.iter().next_back())),
                ]))
            }
            "GROUPS" => {
                let lags = lags(stream);
                Ok(Reply::Array(
                    stream
                        .groups
                        .iter()
                        .map(|(name, g)| {
                            let lag = lags[&g.last_delivered];
                            Reply::Map(vec![
                                ("name".into(), Reply::Bulk(name.clone())),
                                ("consumers".into(), Reply::int(g.consumers.len())),
                                ("pending".into(), Reply::int(g.pending.len())),
                                (
                                    "last-delivered-id".into(),
                                    Reply::Bulk(g.last_delivered.to_string()),
                                ),
                                ("entries-read".into(), Reply::Int(g.read as i64)),
                                ("lag".into(), Reply::int(lag)),
                            ])
                        })
                        .collect(),
                ))
            }
            "CONSUMERS" => {
                self.exactly(4)?;
                let group = self.arg(3);
                let record = stream
                    .groups
                    .get(group)
                    .ok_or_else(|| nogroup(key, group, "XINFO"))?;
                // One pass over the pending entries for all consumers.
                let mut pending: HashMap<&str, usize> = HashMap::new();
                for entry in record.pending.values() {
                    *pending.entry(entry.consumer.as_str()).or_default() += 1;
                }
                Ok(Reply::Array(
                    record
                        .consumers
                        .iter()
                        .map(|(name, c)| {
                            Reply::Map(vec![
                                ("name".into(), Reply::Bulk(name.clone())),
                                (
                                    "pending".into(),
                                    Reply::int(pending.get(name.as_str()).copied().unwrap_or(0)),
                                ),
                                ("idle".into(), Reply::Int(now.saturating_sub(c.seen) as i64)),
                                (
                                    "inactive".into(),
                                    Reply::Int(
                                        c.active.map_or(-1, |a| now.saturating_sub(a) as i64),
                                    ),
                                ),
                            ])
                        })
                        .collect(),
                ))
            }
            _ => Err(format!("ERR unknown subcommand '{}'", self.arg(1))),
        }
    }
}

/// How many entries each group of `stream` has yet to read, by the last id it
/// read, from one walk over the entries however many groups there are.
fn lags(stream: &Stream) -> HashMap<Id, usize> {
    let mut marks: Vec<Id> = stream.groups.values().map(|g| g.last_delivered).collect();
    marks.sort_unstable();
    marks.dedup();
    let mut lags = HashMap::with_capacity(marks.len());
    let Some(first) = marks.first() else {
        return lags;
    };
    let after = |id: &Id| {
        stream
            .entries
            .range((Excluded(*id), Unbounded))
            .map(|(id, _)| *id)
    };
    let mut remaining = after(first).count();
    let mut walk = after(first).peekable();
    for mark in marks {
        while walk.next_if(|id| *id <= mark).is_some() {
            remaining -= 1;
        }
        lags.insert(mark, remaining);
    }
    lags
}

#[derive(Default)]
struct ReadOptions {
    count: Option<usize>,
    block: Option<u64>,
    block_at: Option<usize>,
    noack: bool,
    keys: Vec<String>,
    ids: Vec<String>,
    ids_at: usize,
}

enum Trim {
    MaxLen(usize),
    MinId(Id),
}

impl Trim {
    fn apply(&self, stream: &mut Stream) -> usize {
        match self {
            Trim::MaxLen(n) => stream.trim_to(*n),
            Trim::MinId(id) => stream.trim_before(*id),
        }
    }
}

/// A score bound: a number, `-inf`/`+inf`, or `(n` for an exclusive bound.
fn score_bound(s: &str) -> Result<(f64, bool), String> {
    let err = || "ERR min or max is not a float".to_string();
    match s.strip_prefix('(') {
        Some(rest) => Ok((float(rest).map_err(|_| err())?, true)),
        None => Ok((float(s).map_err(|_| err())?, false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(now: u64) -> Ctx {
        Ctx {
            now,
            replay: false,
            blocking: false,
            access: Access::admin(),
            max_keys: 1000,
            max_elements: 1000,
        }
    }

    fn argv(command: &str) -> Vec<String> {
        command.split_whitespace().map(String::from).collect()
    }

    fn run(db: &mut Db, now: u64, command: &str) -> Reply {
        db.exec(&argv(command), &ctx(now)).reply
    }

    fn bulk(s: &str) -> Reply {
        Reply::Bulk(s.into())
    }

    fn limited(now: u64, keys: usize, elements: usize) -> Ctx {
        Ctx {
            max_keys: keys,
            max_elements: elements,
            ..ctx(now)
        }
    }

    #[test]
    fn score_ranges_visit_only_the_members_in_range() {
        fn in_range(score: f64, min: (f64, bool), max: (f64, bool)) -> bool {
            let above = if min.1 { score > min.0 } else { score >= min.0 };
            let below = if max.1 { score < max.0 } else { score <= max.0 };
            above && below
        }
        let scores = [
            f64::NEG_INFINITY,
            -f64::MAX,
            -2.5,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            1.5,
            f64::MAX,
            f64::INFINITY,
        ];
        let mut zset = ZSet::default();
        for (i, score) in scores.iter().enumerate() {
            zset.insert(&format!("m{i}"), *score);
            zset.insert(&format!("n{i}"), *score);
        }
        for &lo in &scores {
            for &hi in &scores {
                for (lx, hx) in [(false, false), (false, true), (true, false), (true, true)] {
                    let (min, max) = ((lo, lx), (hi, hx));
                    let expected: Vec<&Ranked> = zset
                        .order
                        .iter()
                        .filter(|(s, _)| in_range(s.0, min, max))
                        .collect();
                    let got: Vec<&Ranked> = zset.by_score(min, max).collect();
                    assert_eq!(got, expected, "{min:?} to {max:?}");
                    let back: Vec<&Ranked> = zset.by_score(min, max).rev().collect();
                    assert_eq!(back.len(), expected.len());
                }
            }
        }
    }

    #[test]
    fn data_named_many_times_over_does_not_make_a_huge_reply() {
        let mut db = Db::default();
        let big = "x".repeat(MAX_STRING);
        let exec = |db: &mut Db, words: Vec<String>| db.exec(&words, &ctx(0)).reply;
        let words = |parts: &[&str]| parts.iter().map(|w| w.to_string()).collect::<Vec<_>>();
        let mut set = words(&["SET", "big"]);
        set.push(big.clone());
        assert_eq!(exec(&mut db, set), Reply::ok());
        let refused = |reply: Reply| matches!(reply, Reply::Error(e) if e.contains("64 MiB"));
        let five = |first: &[&str], key: &str| {
            let mut argv = words(first);
            argv.extend(std::iter::repeat_n(key.to_string(), 5));
            argv
        };
        assert!(refused(exec(&mut db, five(&["MGET"], "big"))));
        assert!(!refused(exec(&mut db, words(&["MGET", "big", "big"]))));
        let mut hset = words(&["HSET", "h", "f"]);
        hset.push(big.clone());
        exec(&mut db, hset);
        assert!(refused(exec(&mut db, five(&["HMGET", "h"], "f"))));
        let mut sadd = words(&["SADD", "members"]);
        sadd.extend((0..5).map(|i| format!("{i}{}", &big[1..])));
        exec(&mut db, sadd);
        assert!(refused(exec(&mut db, five(&["SUNION"], "members"))));
        assert!(refused(exec(&mut db, five(&["SINTER"], "members"))));
        assert_eq!(
            exec(&mut db, words(&["SDIFF", "members", "members"])),
            Reply::Array(Vec::new())
        );
        // A reply is refused at the size a response may carry, counted as
        // it is made: three values of the largest size fit, four do not.
        let items = |n: usize| (0..n).map(|_| Reply::Bulk(big.clone()));
        assert!(collect_within(items(3)).is_ok());
        assert!(collect_within(items(4)).is_err_and(|e| e.contains("64 MiB")));
        assert!(refused(exec(&mut db, words(&["SMEMBERS", "members"]))));
        let mut xadd = words(&["XADD", "s", "*", "f"]);
        xadd.push(big);
        exec(&mut db, xadd);
        let mut xread = five(&["XREAD", "STREAMS"], "s");
        xread.extend(std::iter::repeat_n("0".to_string(), 5));
        assert!(refused(exec(&mut db, xread)));
        exec(&mut db, words(&["XGROUP", "CREATE", "s", "g", "0"]));
        let twice = words(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "STREAMS",
            "s",
            "s",
            ">",
            "0",
        ]);
        let named = exec(&mut db, twice);
        assert!(
            matches!(&named, Reply::Error(e) if e.contains("named twice")),
            "{named:?}"
        );
        assert_eq!(
            run(&mut db, 0, "XPENDING s g"),
            Reply::Array(vec![Reply::Int(0), Reply::Nil, Reply::Nil, Reply::Nil]),
            "a refused read delivers nothing"
        );
        for _ in 0..4 {
            let mut xadd = words(&["XADD", "s", "*", "f"]);
            xadd.push("y".repeat(MAX_STRING));
            exec(&mut db, xadd);
        }
        let delivered = exec(
            &mut db,
            words(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]),
        );
        assert!(
            !refused(delivered),
            "new entries are handed over, not copied"
        );
        let again = words(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "0"]);
        assert!(refused(exec(&mut db, again)));
    }

    #[test]
    fn random_members_are_distinct_and_cover_the_set() {
        let mut db = Db::default();
        let members: Vec<String> = (0..100).map(|i| format!("m{i:03}")).collect();
        run(&mut db, 0, &format!("SADD s {}", members.join(" ")));
        let strings = |reply: Reply| match reply {
            Reply::Array(items) => items
                .into_iter()
                .map(|r| match r {
                    Reply::Bulk(s) => s,
                    other => panic!("{other:?}"),
                })
                .collect::<Vec<_>>(),
            other => panic!("{other:?}"),
        };
        let some = strings(run(&mut db, 0, "SRANDMEMBER s 10"));
        let distinct: BTreeSet<&String> = some.iter().collect();
        assert_eq!(distinct.len(), 10);
        assert!(some.iter().all(|m| members.contains(m)));
        assert_eq!(strings(run(&mut db, 0, "SRANDMEMBER s -300")).len(), 300);
        let popped = strings(run(&mut db, 0, "SPOP s 10"));
        assert_eq!(run(&mut db, 0, "SCARD s"), Reply::Int(90));
        assert!(
            popped
                .iter()
                .all(|m| run(&mut db, 0, &format!("SISMEMBER s {m}")) == Reply::Int(0))
        );
        assert_eq!(strings(run(&mut db, 0, "SRANDMEMBER s 1000")).len(), 90);
        let mut seen = BTreeSet::new();
        for _ in 0..10_000 {
            if let Reply::Bulk(m) = run(&mut db, 0, "SRANDMEMBER s") {
                seen.insert(m);
            }
        }
        assert_eq!(seen.len(), 90, "every member can be picked");
    }

    #[test]
    fn a_refused_write_changes_nothing() {
        let mut db = Db::default();
        run(&mut db, 0, "RPUSH list a b");
        run(&mut db, 0, "SADD set x y");
        let full = limited(0, 2, 2);
        let refused = |db: &mut Db, command: &str| {
            let reply = db.exec(&argv(command), &full).reply;
            assert!(reply.is_error(), "{command}: {reply:?}");
        };
        // A move whose destination would be a third key keeps the element.
        refused(&mut db, "LMOVE list other LEFT LEFT");
        refused(&mut db, "RPOPLPUSH list other");
        refused(&mut db, "SMOVE set other x");
        assert_eq!(run(&mut db, 0, "LLEN list"), Reply::Int(2));
        assert_eq!(run(&mut db, 0, "SCARD set"), Reply::Int(2));
        // So does a move into a full collection.
        run(&mut db, 0, "DEL list");
        run(&mut db, 0, "SADD small z");
        refused(&mut db, "SMOVE small set z");
        assert_eq!(run(&mut db, 0, "SCARD small"), Reply::Int(1));
        // An emptied source makes room for the destination.
        assert_eq!(
            db.exec(&argv("SMOVE small fresh z"), &full).reply,
            Reply::Int(1)
        );
        // MSET writes every key or none.
        refused(&mut db, "MSET a 1 b 2");
        assert_eq!(run(&mut db, 0, "EXISTS a b"), Reply::Int(0));
        // A trim that keeps more than the limit allows does not get around it.
        run(&mut db, 0, "DEL set fresh");
        db.exec(&argv("XADD s 1-1 f v"), &full);
        db.exec(&argv("XADD s 1-2 f v"), &full);
        refused(&mut db, "XADD s MAXLEN 1000 1-3 f v");
        assert_eq!(
            db.exec(&argv("XADD s MAXLEN 2 1-3 f v"), &full).reply,
            bulk("1-3")
        );
        assert_eq!(run(&mut db, 0, "XLEN s"), Reply::Int(2));
        // A group whose start id is wrong creates no stream.
        refused(&mut db, "XGROUP CREATE t g bad MKSTREAM");
        assert_eq!(run(&mut db, 0, "EXISTS t"), Reply::Int(0));
    }

    #[test]
    fn expiry_times_that_do_not_fit_are_refused() {
        let mut db = Db::default();
        for command in [
            "SET k v EX 9223372036854775807",
            "SET k v PX 9223372036854775807",
            "SETEX k 9223372036854775807 v",
            "PSETEX k 9223372036854775807 v",
            "SET k v EXAT 9223372036854775807",
        ] {
            assert!(run(&mut db, 1000, command).is_error(), "{command}");
        }
        run(&mut db, 1000, "SET k v PXAT 9223372036854775807");
        assert!(run(&mut db, 1000, "GETEX k EX 9223372036854775807").is_error());
        assert_eq!(
            run(&mut db, 1000, "PTTL k"),
            Reply::Int(9223372036854774807)
        );
        assert_eq!(run(&mut db, 1000, "TTL k"), Reply::Int(9223372036854775));
    }

    #[test]
    fn the_largest_stream_id_has_nothing_after_it() {
        let mut db = Db::default();
        let max = "18446744073709551615-18446744073709551615";
        assert_eq!(run(&mut db, 0, &format!("XADD s {max} f v")), bulk(max));
        assert!(run(&mut db, 0, "XADD s * f v").is_error());
        assert_eq!(
            run(
                &mut db,
                0,
                "XRANGE s (18446744073709551614-18446744073709551615 +"
            ),
            Reply::Array(vec![Reply::Array(vec![
                bulk(max),
                Reply::Array(vec![bulk("f"), bulk("v")])
            ])])
        );
        assert!(run(&mut db, 0, &format!("XRANGE s ({max} +")).is_error());
        run(&mut db, 0, "XGROUP CREATE s g $");
        assert_eq!(
            run(&mut db, 0, "XREADGROUP GROUP g c STREAMS s >"),
            Reply::Nil
        );
    }

    #[test]
    fn strings_counters_and_expiry() {
        let mut db = Db::default();
        assert_eq!(run(&mut db, 0, "SET plan v1 EX 10"), Reply::ok());
        assert_eq!(run(&mut db, 1, "GET plan"), bulk("v1"));
        assert_eq!(run(&mut db, 1, "TTL plan"), Reply::Int(10));
        assert_eq!(run(&mut db, 1, "SET plan v2 NX"), Reply::Nil);
        assert_eq!(run(&mut db, 1, "SET plan v2 XX GET"), bulk("v1"));
        assert_eq!(
            run(&mut db, 1, "TTL plan"),
            Reply::Int(-1),
            "SET clears the TTL"
        );
        assert_eq!(run(&mut db, 1, "SET plan v3 IFEQ stale"), Reply::Nil);
        assert_eq!(run(&mut db, 1, "SET plan v3 IFEQ v2"), Reply::ok());
        assert_eq!(run(&mut db, 1, "DELIFEQ plan other"), Reply::Int(0));
        assert_eq!(run(&mut db, 1, "DELIFEQ plan v3"), Reply::Int(1));
        assert_eq!(run(&mut db, 1, "INCRBY hits 5"), Reply::Int(5));
        assert_eq!(run(&mut db, 1, "DECR hits"), Reply::Int(4));
        assert_eq!(run(&mut db, 1, "INCRBYFLOAT ratio 0.5"), bulk("0.5"));
        assert!(run(&mut db, 1, "INCR ratio").is_error());
        assert_eq!(run(&mut db, 1, "SET lock me PX 100"), Reply::ok());
        assert_eq!(run(&mut db, 100, "PTTL lock"), Reply::Int(1));
        assert_eq!(
            run(&mut db, 101, "GET lock"),
            Reply::Nil,
            "expired keys read as absent"
        );
        assert_eq!(run(&mut db, 101, "EXISTS lock hits"), Reply::Int(1));
        assert_eq!(run(&mut db, 100, "APPEND log ab"), Reply::Int(2));
        assert_eq!(run(&mut db, 100, "APPEND log cd"), Reply::Int(4));
        assert_eq!(
            run(&mut db, 100, "MGET log missing"),
            Reply::Array(vec![bulk("abcd"), Reply::Nil])
        );
    }

    #[test]
    fn a_plain_set_replaces_whatever_the_key_holds() {
        let mut db = Db::default();
        run(&mut db, 0, "SET plan v1 EX 10");
        assert_eq!(run(&mut db, 1, "SET plan v2"), Reply::ok());
        assert_eq!(
            run(&mut db, 1, "TTL plan"),
            Reply::Int(-1),
            "SET clears the TTL"
        );
        run(&mut db, 1, "SET plan v3 PX 5");
        assert_eq!(run(&mut db, 10, "SET plan v4"), Reply::ok());
        assert_eq!(
            run(&mut db, 10, "GET plan"),
            bulk("v4"),
            "an expired key is replaced"
        );
        assert_eq!(run(&mut db, 10, "PTTL plan"), Reply::Int(-1));
        run(&mut db, 10, "RPUSH queue a b");
        assert_eq!(run(&mut db, 10, "SET queue v"), Reply::ok());
        assert_eq!(
            run(&mut db, 10, "GET queue"),
            bulk("v"),
            "so is one of another type"
        );
        assert_eq!(db.entries().len(), 2);
    }

    #[test]
    fn wrong_types_and_unknown_commands_are_errors() {
        let mut db = Db::default();
        run(&mut db, 0, "LPUSH queue a");
        assert_eq!(
            run(&mut db, 0, "GET queue"),
            Reply::Error(crate::db::WRONGTYPE.into())
        );
        assert!(
            matches!(run(&mut db, 0, "NOPE x"), Reply::Error(e) if e.starts_with("ERR unknown command"))
        );
        // A name longer than any command's, or with letters beyond ASCII,
        // matches none, and the error names what was sent.
        let long = "SETSETSETSETSETSETSETSETSET";
        assert!(
            matches!(run(&mut db, 0, &format!("{long} k v")), Reply::Error(e) if e.contains(long))
        );
        assert!(matches!(run(&mut db, 0, "gét k"), Reply::Error(e) if e.contains("'gét'")));
        assert_eq!(
            Name::of(&argv("zRemRangeByScore")).as_str(),
            "ZREMRANGEBYSCORE"
        );
        assert_eq!(Name::of(&[]).as_str(), "");
        assert!(matches!(run(&mut db, 0, "GET"), Reply::Error(e) if e.contains("wrong number")));
        assert_eq!(run(&mut db, 0, "TYPE queue"), Reply::Simple("list".into()));
    }

    #[test]
    fn hashes_lists_and_sets() {
        let mut db = Db::default();
        assert_eq!(
            run(&mut db, 0, "HSET task:1 state open owner planner"),
            Reply::Int(2)
        );
        assert_eq!(run(&mut db, 0, "HSET task:1 state done"), Reply::Int(0));
        assert_eq!(run(&mut db, 0, "HINCRBY task:1 attempts 2"), Reply::Int(2));
        assert_eq!(
            run(&mut db, 0, "HGETALL task:1"),
            Reply::Map(vec![
                ("attempts".into(), bulk("2")),
                ("owner".into(), bulk("planner")),
                ("state".into(), bulk("done")),
            ])
        );
        assert_eq!(
            run(&mut db, 0, "HDEL task:1 attempts owner state"),
            Reply::Int(3)
        );
        assert_eq!(
            run(&mut db, 0, "EXISTS task:1"),
            Reply::Int(0),
            "empty hashes are removed"
        );

        assert_eq!(run(&mut db, 0, "RPUSH jobs a b c"), Reply::Int(3));
        assert_eq!(run(&mut db, 0, "LMOVE jobs doing LEFT RIGHT"), bulk("a"));
        assert_eq!(
            run(&mut db, 0, "LRANGE jobs 0 -1"),
            Reply::bulks(["b", "c"])
        );
        assert_eq!(run(&mut db, 0, "LPOP jobs 5"), Reply::bulks(["b", "c"]));
        assert_eq!(run(&mut db, 0, "LLEN jobs"), Reply::Int(0));
        assert_eq!(
            run(&mut db, 0, "BLPOP jobs doing 1"),
            Reply::bulks(["doing", "a"])
        );

        assert_eq!(run(&mut db, 0, "SADD seen x y z"), Reply::Int(3));
        assert_eq!(run(&mut db, 0, "SADD seen x"), Reply::Int(0));
        assert_eq!(run(&mut db, 0, "SISMEMBER seen y"), Reply::Int(1));
        run(&mut db, 0, "SADD other y q");
        assert_eq!(run(&mut db, 0, "SINTER seen other"), Reply::bulks(["y"]));
        assert_eq!(run(&mut db, 0, "SCARD seen"), Reply::Int(3));
        assert!(matches!(run(&mut db, 0, "SPOP seen"), Reply::Bulk(_)));
        assert_eq!(run(&mut db, 0, "SCARD seen"), Reply::Int(2));
    }

    #[test]
    fn sets_combine_as_copies_of_them_would_however_they_are_named() {
        let mut db = Db::default();
        run(&mut db, 0, "SADD a 1 2 3 4");
        run(&mut db, 0, "SADD b 3 4 5");
        run(&mut db, 0, "SADD c 4 5 6");
        run(&mut db, 0, "SET text x");
        let set = |key: &str| -> BTreeSet<String> {
            let members: &[&str] = match key {
                "a" => &["1", "2", "3", "4"],
                "b" => &["3", "4", "5"],
                "c" => &["4", "5", "6"],
                _ => &[],
            };
            members.iter().map(|m| m.to_string()).collect()
        };
        // Every way of naming up to three of the keys, a key with no set and
        // the same key more than once among them.
        let keys = ["a", "b", "c", "none"];
        for length in 1..=3u32 {
            for mut pick in 0..keys.len().pow(length) {
                let mut named = Vec::new();
                for _ in 0..length {
                    named.push(keys[pick % keys.len()]);
                    pick /= keys.len();
                }
                for op in ["SINTER", "SUNION", "SDIFF"] {
                    let mut expected = set(named[0]);
                    for key in &named[1..] {
                        let other = set(key);
                        expected = match op {
                            "SINTER" => expected.intersection(&other).cloned().collect(),
                            "SUNION" => expected.union(&other).cloned().collect(),
                            _ => expected.difference(&other).cloned().collect(),
                        };
                    }
                    let command = format!("{op} {}", named.join(" "));
                    assert_eq!(
                        run(&mut db, 0, &command),
                        Reply::bulks(expected),
                        "{command}"
                    );
                }
            }
        }
        for command in ["SUNION a text", "SINTER none text", "SDIFF text a"] {
            assert!(
                matches!(run(&mut db, 0, command), Reply::Error(e) if e.starts_with("WRONGTYPE")),
                "{command}"
            );
        }
    }

    #[test]
    fn sorted_sets_rank_and_range() {
        let mut db = Db::default();
        assert_eq!(run(&mut db, 0, "ZADD prio 3 c 1 a 2 b"), Reply::Int(3));
        assert_eq!(
            run(&mut db, 0, "ZRANGE prio 0 -1"),
            Reply::bulks(["a", "b", "c"])
        );
        assert_eq!(
            run(&mut db, 0, "ZRANGE prio (1 +inf BYSCORE WITHSCORES"),
            Reply::bulks(["b", "2", "c", "3"])
        );
        assert_eq!(
            run(&mut db, 0, "ZRANGE prio +inf 2 BYSCORE REV"),
            Reply::bulks(["c", "b"])
        );
        assert_eq!(
            run(&mut db, 0, "ZREVRANGEBYSCORE prio 3 2"),
            Reply::bulks(["c", "b"])
        );
        assert_eq!(run(&mut db, 0, "ZRANK prio c"), Reply::Int(2));
        assert_eq!(run(&mut db, 0, "ZINCRBY prio 10 a"), bulk("11"));
        assert_eq!(run(&mut db, 0, "ZPOPMIN prio"), Reply::bulks(["b", "2"]));
        assert_eq!(run(&mut db, 0, "ZADD prio GT 1 c"), Reply::Int(0));
        assert_eq!(run(&mut db, 0, "ZSCORE prio c"), bulk("3"));
        assert_eq!(run(&mut db, 0, "ZCOUNT prio -inf +inf"), Reply::Int(2));
    }

    #[test]
    fn streams_with_consumer_groups() {
        let mut db = Db::default();
        assert_eq!(run(&mut db, 5, "XADD events * kind start"), bulk("5-0"));
        assert_eq!(run(&mut db, 5, "XADD events * kind step"), bulk("5-1"));
        assert!(run(&mut db, 5, "XADD events 5-1 kind dup").is_error());
        assert_eq!(run(&mut db, 6, "XLEN events"), Reply::Int(2));
        assert_eq!(
            run(&mut db, 6, "XRANGE events - + COUNT 1"),
            Reply::Array(vec![entry_reply(
                &Id { ms: 5, seq: 0 },
                &[("kind".into(), "start".into())]
            )])
        );
        assert_eq!(run(&mut db, 6, "XREAD STREAMS events 5-1"), Reply::Nil);
        assert_eq!(
            run(&mut db, 6, "XGROUP CREATE events workers 0"),
            Reply::ok()
        );
        assert!(
            matches!(run(&mut db, 6, "XGROUP CREATE events workers 0"), Reply::Error(e) if e.starts_with("BUSYGROUP"))
        );
        let first = run(
            &mut db,
            10,
            "XREADGROUP GROUP workers w1 COUNT 1 STREAMS events >",
        );
        assert_eq!(
            first,
            Reply::Array(vec![Reply::Array(vec![
                bulk("events"),
                Reply::Array(vec![entry_reply(
                    &Id { ms: 5, seq: 0 },
                    &[("kind".into(), "start".into())]
                )]),
            ])])
        );
        run(&mut db, 10, "XREADGROUP GROUP workers w2 STREAMS events >");
        assert_eq!(
            run(&mut db, 11, "XREADGROUP GROUP workers w3 STREAMS events >"),
            Reply::Nil
        );
        let Reply::Array(summary) = run(&mut db, 11, "XPENDING events workers") else {
            panic!("summary");
        };
        assert_eq!(summary[0], Reply::Int(2));
        assert_eq!(run(&mut db, 11, "XACK events workers 5-1"), Reply::Int(1));
        // w1 went quiet; w3 takes over its entry once it has been idle long enough.
        let Reply::Array(claim) = run(&mut db, 20, "XAUTOCLAIM events workers w3 5 0 JUSTID")
        else {
            panic!("claim");
        };
        assert_eq!(claim[1], Reply::bulks(["5-0"]));
        let Reply::Array(pending) = run(&mut db, 20, "XPENDING events workers - + 10") else {
            panic!("pending");
        };
        assert_eq!(
            pending[0],
            Reply::Array(vec![bulk("5-0"), bulk("w3"), Reply::Int(0), Reply::Int(1)])
        );
        assert!(matches!(
            run(&mut db, 20, "XREADGROUP GROUP nope w1 STREAMS events >"),
            Reply::Error(e) if e.starts_with("NOGROUP")
        ));
        assert_eq!(run(&mut db, 20, "XTRIM events MAXLEN 1"), Reply::Int(1));
    }

    #[test]
    fn replies_serialize_as_their_json_does() {
        let replies = [
            Reply::Simple("OK".into()),
            Reply::Bulk("a \"quoted\"\nvalue \u{1F4EE}".into()),
            Reply::Int(-7),
            Reply::Nil,
            Reply::Array(vec![
                Reply::Int(1),
                Reply::Nil,
                Reply::Array(vec![Reply::Bulk("x".into())]),
            ]),
            Reply::Map(vec![
                ("b".into(), Reply::Int(2)),
                ("a".into(), Reply::Array(vec![])),
                ("b".into(), Reply::Int(3)),
            ]),
            Reply::Error("ERR no".into()),
        ];
        for reply in replies {
            let direct = serde_json::to_string(&reply).unwrap();
            assert_eq!(direct, reply.to_json().to_string());
        }
    }

    #[test]
    fn blocking_reads_report_what_to_retry() {
        let mut db = Db::default();
        let mut blocking = ctx(0);
        blocking.blocking = true;
        run(&mut db, 0, "XADD s 1-0 a b");
        let out = db.exec(&argv("XREAD BLOCK 500 STREAMS s $"), &blocking);
        assert_eq!(out.reply, Reply::Nil);
        let block = out.block.expect("blocks");
        assert_eq!(
            block.retry,
            argv("XREAD STREAMS s 1-0"),
            "$ is resolved once"
        );
        assert_eq!(block.timeout_ms, 500);
        let out = db.exec(&argv("BLPOP q 0"), &blocking);
        assert_eq!(out.block.unwrap().timeout_ms, 0);
        assert!(
            db.exec(&argv("BLPOP q 0"), &ctx(0)).block.is_none(),
            "only when allowed"
        );
    }

    #[test]
    fn a_timeout_under_a_millisecond_still_ends() {
        let mut db = Db::default();
        let mut blocking = ctx(0);
        blocking.blocking = true;
        for (timeout, ms) in [
            ("0", 0),
            ("0.0001", 1),
            ("1e-300", 1),
            ("0.0015", 2),
            ("2", 2000),
        ] {
            let out = db.exec(&argv(&format!("BLPOP q {timeout}")), &blocking);
            assert_eq!(out.block.expect("blocks").timeout_ms, ms, "{timeout}");
        }
        for timeout in ["inf", "1e400", "1e17"] {
            let out = db.exec(&argv(&format!("BLPOP q {timeout}")), &blocking);
            assert_eq!(
                out.reply,
                Reply::Error("ERR timeout is out of range".into()),
                "{timeout}"
            );
        }
    }

    #[test]
    fn keys_scan_and_limits() {
        let mut db = Db::default();
        for i in 0..5 {
            run(&mut db, 0, &format!("SET task:{i} x"));
        }
        run(&mut db, 0, "SET other x");
        let Reply::Array(keys) = run(&mut db, 0, "KEYS task:*") else {
            panic!()
        };
        assert_eq!(keys.len(), 5);
        let mut cursor = "0".to_string();
        let mut seen = Vec::new();
        loop {
            let Reply::Array(page) = run(&mut db, 0, &format!("SCAN {cursor} COUNT 4")) else {
                panic!()
            };
            let (Reply::Bulk(next), Reply::Array(found)) = (&page[0], &page[1]) else {
                panic!()
            };
            seen.extend(found.iter().cloned());
            cursor = next.clone();
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen.len(), 6, "a walk returns every key once: {seen:?}");
        assert!(run(&mut db, 0, "SCAN zz").is_error(), "cursors are opaque");
        for count in ["0", "-1"] {
            assert_eq!(
                run(&mut db, 0, &format!("SCAN 0 COUNT {count}")),
                Reply::Error(SYNTAX.into()),
                "a page of {count} keys"
            );
        }
        let mut tight = ctx(0);
        tight.max_keys = 6;
        assert!(
            matches!(db.exec(&argv("SET new x"), &tight).reply, Reply::Error(e) if e.starts_with("OOM"))
        );
        assert_eq!(
            db.exec(&argv("SET other y"), &tight).reply,
            Reply::ok(),
            "existing keys can change"
        );
        let mut user = ctx(0);
        user.access.admin = false;
        assert!(db.exec(&argv("FLUSHALL"), &user).reply.is_error());
    }

    #[test]
    fn the_expire_cycle_finds_every_way_to_set_an_expiry() {
        let mut db = Db::default();
        for command in [
            "SET a v EX 1",
            "SET b v PX 1000",
            "SETEX c 1 v",
            "PSETEX d 1000 v",
            "SET e v",
            "EXPIRE e 1",
            "SET f v",
            "GETEX f EX 1",
            "SET g v EX 1",
            "RENAME g h",
            "SET i v EX 1",
            "PERSIST i",
            "SET j v EX 1",
            "EXPIRE j 100",
        ] {
            assert!(!run(&mut db, 0, command).is_error(), "{command}");
        }
        let mut expired = db.expire_keys(1_000);
        expired.sort();
        assert_eq!(expired, ["a", "b", "c", "d", "e", "f", "h"]);
        assert!(db.entries().contains_key("i") && db.entries().contains_key("j"));
        assert_eq!(db.expire_keys(100_000), ["j"]);
    }

    #[test]
    fn every_command_names_its_keys() {
        for name in COMMANDS {
            assert!(key_spec(name).is_some(), "{name} has no key specification");
        }
        assert!(key_args(&argv("NOSUCH a b")).is_none());
        let keys = |command: &str| key_args(&argv(command)).unwrap();
        assert_eq!(keys("MSET a 1 b 2"), [(1, true), (3, true)]);
        assert_eq!(keys("BZPOPMIN a b 5"), [(1, true), (2, true)]);
        assert_eq!(
            keys("XREAD COUNT 2 BLOCK 0 STREAMS a b 0 0"),
            [(6, false), (7, false)]
        );
        assert_eq!(keys("XREADGROUP GROUP g c NOACK STREAMS s >"), [(6, true)]);
        assert_eq!(keys("XGROUP HELP"), []);
        assert_eq!(keys("SMOVE a b m"), [(1, true), (2, true)]);
        assert!(grows(&argv("XGROUP CREATE s g $")));
        assert!(!grows(&argv("XGROUP DESTROY s g")));
        assert!(!grows(&argv("DEL a")));
    }

    #[test]
    fn scoped_listing_sees_only_reachable_keys() {
        let mut db = Db::default();
        for key in ["alice:a", "alice:b", "bob:a", "shared:x", "zeta"] {
            run(&mut db, 0, &format!("SET {key} v"));
        }
        let mut alice = ctx(0);
        alice.access = Access {
            admin: false,
            scope: std::sync::Arc::new(Scope::agent("alice")),
        };
        let listed = db.exec(&argv("KEYS *"), &alice).reply;
        assert_eq!(listed, Reply::bulks(["alice:a", "alice:b", "shared:x"]));
        assert_eq!(db.exec(&argv("DBSIZE"), &alice).reply, Reply::Int(3));
        let Reply::Array(page) = db.exec(&argv("SCAN 0 COUNT 2"), &alice).reply else {
            panic!()
        };
        let Reply::Bulk(cursor) = &page[0] else {
            panic!()
        };
        assert_eq!(
            from_hex(cursor).unwrap(),
            b"alice:b",
            "the cursor names a key the caller can see"
        );
    }

    /// A small deterministic generator, so a failure can be replayed.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
            items[self.next() as usize % items.len()]
        }
    }

    /// The keys `argv` names, each once.
    fn named_keys(argv: &[String]) -> Vec<String> {
        let mut keys: Vec<String> = key_args(argv)
            .unwrap_or_default()
            .into_iter()
            .map(|(index, _)| argv[index].clone())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    /// An entry as the journal must reproduce it. When a consumer last tried
    /// to read is left out: reads that deliver nothing update it without
    /// being journaled, so after a restart it dates from the last change.
    fn durable(entry: &Entry) -> serde_json::Value {
        let mut value = serde_json::to_value(entry).unwrap();
        if let Some(groups) = value
            .pointer_mut("/data/groups")
            .and_then(|g| g.as_object_mut())
        {
            for group in groups.values_mut() {
                if let Some(consumers) = group
                    .pointer_mut("/consumers")
                    .and_then(|c| c.as_object_mut())
                {
                    for consumer in consumers.values_mut() {
                        consumer.as_object_mut().unwrap().remove("seen");
                    }
                }
            }
        }
        value
    }

    /// What a client can see of `keys`, leaving out keys that have expired
    /// but are not removed yet.
    fn observe(db: &Db, keys: &[String], now: u64) -> Vec<Option<serde_json::Value>> {
        keys.iter()
            .map(|key| db.get(key, now).map(durable))
            .collect()
    }

    /// How many elements a collection holds; strings have none.
    fn elements(entry: &Entry) -> usize {
        match &entry.value {
            Value::String(_) => 0,
            Value::List(l) => l.len(),
            Value::Hash(h) => h.len(),
            Value::Set(s) => s.len(),
            Value::ZSet(z) => z.len(),
            Value::Stream(s) => s.entries.len(),
        }
    }

    /// Random commands over a handful of keys, under limits that change
    /// every round: none may panic, push the keyspace past its limits, or
    /// change what it does not journal, and replaying what was journaled must
    /// rebuild the same keyspace.
    fn fuzz(seed: u64, rounds: usize) {
        const WORDS: &[&str] = &[
            "a",
            "b",
            "s",
            "",
            "0",
            "1",
            "-1",
            "2",
            "10",
            "-10",
            "3.5",
            "-0",
            "inf",
            "-inf",
            "+inf",
            "nan",
            "9223372036854775807",
            "-9223372036854775808",
            "99999999999999999999",
            "0-0",
            "0-1",
            "1-0",
            "18446744073709551615-18446744073709551615",
            "$",
            ">",
            "*",
            "+",
            "-",
            "(1",
            "[a",
            "(",
            "[",
            "COUNT",
            "BLOCK",
            "STREAMS",
            "NOACK",
            "NX",
            "XX",
            "GT",
            "LT",
            "CH",
            "INCR",
            "EX",
            "PX",
            "EXAT",
            "PXAT",
            "KEEPTTL",
            "GET",
            "IFEQ",
            "LIMIT",
            "WITHSCORES",
            "BYSCORE",
            "REV",
            "MATCH",
            "TYPE",
            "MAXLEN",
            "MINID",
            "~",
            "=",
            "NOMKSTREAM",
            "MKSTREAM",
            "GROUP",
            "CREATE",
            "SETID",
            "DESTROY",
            "CREATECONSUMER",
            "DELCONSUMER",
            "ENTRIESREAD",
            "IDLE",
            "TIME",
            "RETRYCOUNT",
            "FORCE",
            "JUSTID",
            "LASTID",
            "LEFT",
            "RIGHT",
            "BEFORE",
            "AFTER",
            "STREAM",
            "GROUPS",
            "CONSUMERS",
            "FULL",
            "HELP",
            "g",
            "c",
            "x",
            "y",
            "z",
            "*?[a-",
            "\\",
        ];
        let mut rng = Rng(seed.max(1));
        let mut live = Db::default();
        let mut replayed = Db::default();
        let mut now = 1_000;
        // A snapshot taken while commands go on holds the keyspace as it was
        // when the snapshot began.
        let mut taken: Option<(usize, BTreeMap<String, Entry>)> = None;
        let mut snapshots = 0;
        for round in 0..rounds {
            if taken.is_none() && rng.next().is_multiple_of(300) {
                taken = Some((round, live.entries().clone()));
                live.begin_capture(b"{".to_vec());
            }
            if let Some((began, expected)) = &taken {
                live.capture_step([0, 1, 1, 64, 4096][rng.next() as usize % 5]);
                if rng.next().is_multiple_of(100) || round + 1 == rounds {
                    let raw = live.end_capture().unwrap().concat();
                    let captured: BTreeMap<String, Entry> = serde_json::from_slice(&raw).unwrap();
                    assert_eq!(
                        serde_json::to_string(&captured).unwrap(),
                        serde_json::to_string(expected).unwrap(),
                        "seed {seed}: the snapshot begun at round {began} drifted by round {round}"
                    );
                    snapshots += 1;
                    taken = None;
                }
            }
            now += rng.next() % 50;
            let argv = if rng.next().is_multiple_of(4) {
                let mut argv = vec![rng.pick(COMMANDS).to_string()];
                for _ in 0..rng.next() % 9 {
                    argv.push(rng.pick(WORDS).to_string());
                }
                argv
            } else {
                let shape = rng.pick(SHAPES);
                expand(&mut rng, shape)
            };
            let mut wide = ctx(now);
            let mut ctx = ctx(now);
            ctx.max_keys = [1, 2, 4, 64, 64, 64][rng.next() as usize % 6];
            ctx.max_elements = [1, 2, 4, 64, 64, 64][rng.next() as usize % 6];
            let named = named_keys(&argv);
            let snapshot = live.entries().clone();
            let before = observe(&live, &named, now);
            let keys_before = live.entries().len();
            let sizes_before: Vec<usize> = named
                .iter()
                .map(|key| live.get(key, now).map_or(0, elements))
                .collect();
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| live.exec(&argv, &ctx)))
                    .unwrap_or_else(|_| panic!("seed {seed}, round {round}: {argv:?} panicked"));
            // Keys a command does not name make no difference to it: that is
            // what lets the scope check look at the named keys alone.
            let upper = argv[0].to_ascii_uppercase();
            let enumerates = matches!(
                upper.as_str(),
                "DBSIZE" | "FLUSHALL" | "FLUSHDB" | "INFO" | "KEYS" | "SCAN"
            );
            if key_args(&argv).is_some() && !enumerates {
                wide.max_elements = ctx.max_elements;
                let mut whole = Db::load(snapshot.clone());
                let mut alone = Db::load(
                    snapshot
                        .into_iter()
                        .filter(|(key, _)| named.contains(key))
                        .collect(),
                );
                let (a, b) = (
                    whole.exec(&argv, &wide).reply,
                    alone.exec(&argv, &wide).reply,
                );
                if matches!(upper.as_str(), "SPOP" | "SRANDMEMBER") {
                    assert_eq!(
                        a.is_error(),
                        b.is_error(),
                        "seed {seed}, round {round}: {argv:?}"
                    );
                } else {
                    assert_eq!(
                        a, b,
                        "seed {seed}, round {round}: {argv:?} depends on keys it does not name"
                    );
                }
            }
            let journaled = outcome.journal(&argv);
            if journaled.is_empty() {
                assert_eq!(
                    observe(&live, &named, now),
                    before,
                    "seed {seed}, round {round}: {argv:?} changed keys it did not journal ({:?})",
                    outcome.reply
                );
            }
            let keys_after = live.entries().len();
            assert!(
                keys_after <= keys_before.max(ctx.max_keys),
                "seed {seed}, round {round}: {argv:?} went past {} keys",
                ctx.max_keys
            );
            // RENAME moves a whole value; nothing grows.
            let moves =
                argv[0].eq_ignore_ascii_case("RENAME") || argv[0].eq_ignore_ascii_case("RENAMENX");
            for (key, before) in named.iter().zip(sizes_before).filter(|_| !moves) {
                let after = live.get(key, now).map_or(0, elements);
                assert!(
                    after <= before.max(ctx.max_elements),
                    "seed {seed}, round {round}: {argv:?} grew {key:?} past {} elements",
                    ctx.max_elements
                );
            }
            for entry in &journaled {
                replayed.exec(entry, &Ctx::replay(now));
            }
            assert_eq!(
                observe(&live, &named, now),
                observe(&replayed, &named, now),
                "seed {seed}, round {round}: {argv:?} journaled as {journaled:?} replays differently"
            );
        }
        assert!(
            rounds < 1000 || snapshots > 0,
            "seed {seed}: no snapshot was checked"
        );
        let everything = |db: &Db| -> Vec<(String, serde_json::Value)> {
            db.live_keys(now)
                .map(|(key, entry)| (key.clone(), durable(entry)))
                .collect()
        };
        assert_eq!(
            everything(&live),
            everything(&replayed),
            "seed {seed}: replay diverged"
        );
    }

    /// Well-formed commands, so the fuzzer gets past argument checks. `{k}` is
    /// a key, `[a|b]` an optional choice, `<a|b>` a required one, `_` joins
    /// words, and a trailing `+` repeats a token one to three times.
    const SHAPES: &[&str] = &[
        "SET {k} {v} [EX_{n}|PX_{n}|EXAT_{n}|PXAT_{n}|KEEPTTL] [NX|XX] [GET] [IFEQ_{v}]",
        "GETEX {k} [EX_{n}|PX_{n}|PERSIST]",
        "<GET|GETDEL|STRLEN|INCR|DECR|TYPE|TTL|PTTL|PERSIST|EXPIRETIME> {k}",
        "<INCRBY|DECRBY> {k} {n}",
        "INCRBYFLOAT {k} {f}",
        "<APPEND|GETSET|SETNX|DELIFEQ> {k} {v}",
        "<SETEX|PSETEX> {k} {n} {v}",
        "<MSET|MSETNX> {k}_{v}+",
        "<MGET|DEL|EXISTS|UNLINK> {k}+",
        "<EXPIRE|PEXPIRE|EXPIREAT|PEXPIREAT> {k} {n} [NX|XX|GT|LT]",
        "<RENAME|RENAMENX> {k} {k}",
        "<HSET|HMSET> {k} {m}_{v}+",
        "<HSETNX> {k} {m} {v}",
        "<HGET|HEXISTS> {k} {m}",
        "<HDEL|HMGET> {k} {m}+",
        "HINCRBY {k} {m} {n}",
        "HINCRBYFLOAT {k} {m} {f}",
        "<HGETALL|HKEYS|HVALS|HLEN|LLEN|SCARD|ZCARD|XLEN|SMEMBERS> {k}",
        "<LPUSH|RPUSH|LPUSHX|RPUSHX|SADD|SREM|SMISMEMBER> {k} {m}+",
        "<LPOP|RPOP|SPOP|SRANDMEMBER|ZPOPMIN|ZPOPMAX> {k} [{n}]",
        "<LRANGE|LTRIM|ZREMRANGEBYRANK> {k} {n} {n}",
        "LINDEX {k} {n}",
        "LSET {k} {n} {m}",
        "LINSERT {k} <BEFORE|AFTER> {m} {m}",
        "LREM {k} {n} {m}",
        "<LMOVE|BLMOVE> {k} {k} <LEFT|RIGHT> <LEFT|RIGHT> [0]",
        "RPOPLPUSH {k} {k}",
        "<BLPOP|BRPOP|BZPOPMIN|BZPOPMAX> {k}+ 0",
        "<SISMEMBER> {k} {m}",
        "SMOVE {k} {k} {m}",
        "<SINTER|SUNION|SDIFF> {k}+",
        "ZADD {k} [NX|XX] [GT|LT] [CH] [INCR] {f}_{m}+",
        "<ZREM|ZMSCORE> {k} {m}+",
        "<ZSCORE|ZRANK|ZREVRANK> {k} {m}",
        "ZINCRBY {k} {f} {m}",
        "<ZCOUNT|ZREMRANGEBYSCORE> {k} {s} {s}",
        "ZRANGE {k} {n} {n} [REV] [WITHSCORES]",
        "ZRANGE {k} {s} {s} BYSCORE [REV] [LIMIT_{n}_{n}] [WITHSCORES]",
        "<ZRANGEBYSCORE|ZREVRANGEBYSCORE> {k} {s} {s} [WITHSCORES] [LIMIT_{n}_{n}]",
        "ZREVRANGE {k} {n} {n} [WITHSCORES]",
        "XADD {k} [NOMKSTREAM] [MAXLEN_{n}|MAXLEN_~_{n}|MINID_{id}] <*|{id}> {m}_{v}+",
        "<XRANGE|XREVRANGE> {k} {id} {id} [COUNT_{n}]",
        "XDEL {k} {id}+",
        "XTRIM {k} <MAXLEN|MINID> [=|~] <{n}|{id}>",
        "XREAD [COUNT_{n}] STREAMS {k} {id}",
        "XREAD STREAMS {k} {k} {id} {id}",
        "XGROUP CREATE {k} {g} <$|0|{id}> [MKSTREAM] [ENTRIESREAD_{n}]",
        "XGROUP SETID {k} {g} <$|{id}>",
        "XGROUP <DESTROY> {k} {g}",
        "XGROUP <CREATECONSUMER|DELCONSUMER> {k} {g} {c}",
        "XREADGROUP GROUP {g} {c} [COUNT_{n}] [NOACK] STREAMS {k} <>|0|{id}>",
        "XACK {k} {g} {id}+",
        "XPENDING {k} {g} [{id}_{id}_{n}] [{c}]",
        "XCLAIM {k} {g} {c} {n} {id}+ [JUSTID] [FORCE]",
        "XAUTOCLAIM {k} {g} {c} {n} {id} [COUNT_{n}] [JUSTID]",
        "XINFO <STREAM|GROUPS|HELP> {k}",
        "<XRANGE|XREVRANGE> {k} <({id}|-> <({id}|+>",
        "XADD {k} <18446744073709551615-18446744073709551615|18446744073709551615-*|*> {m} {v}",
        "XINFO CONSUMERS {k} {g}",
        "KEYS <*|a*|?|[ab]*|\\*>",
        "SCAN <0|{v}> [MATCH_{v}] [COUNT_{n}] [TYPE_<string|list|hash|set|zset|stream>]",
        "<DBSIZE|TIME|PING|COMMAND>",
    ];

    fn expand(rng: &mut Rng, shape: &str) -> Vec<String> {
        fn word(rng: &mut Rng, token: &str, out: &mut Vec<String>) {
            for part in token.split('_') {
                let value = match part {
                    "{k}" => rng.pick(&["a", "b", "c", "s"]).to_string(),
                    "{v}" => rng
                        .pick(&["x", "y", "1", "-2", "3.5", "", "x*"])
                        .to_string(),
                    "{m}" => rng.pick(&["x", "y", "z", "w", "1"]).to_string(),
                    "{n}" => match rng.next() % 12 {
                        0 => rng
                            .pick(&["-9223372036854775808", "9223372036854775807", "99999999999"])
                            .to_string(),
                        r => (r as i64 - 4).to_string(),
                    },
                    "{f}" => rng
                        .pick(&["1.5", "-2", "0", "inf", "-inf", "1e3", "nan", "0.1"])
                        .to_string(),
                    "{s}" => rng
                        .pick(&["-inf", "+inf", "0", "(1", "1", "2.5", "(3", "x"])
                        .to_string(),
                    "{id}" => rng
                        .pick(&[
                            "0-0",
                            "0-1",
                            "1-0",
                            "1-1",
                            "5",
                            "5-0",
                            "-",
                            "+",
                            "$",
                            ">",
                            "18446744073709551615-18446744073709551615",
                        ])
                        .to_string(),
                    "{g}" => rng.pick(&["g", "h"]).to_string(),
                    "{c}" => rng.pick(&["c1", "c2"]).to_string(),
                    literal => literal.to_string(),
                };
                out.push(value);
            }
        }
        let mut out = Vec::new();
        for token in shape.split(' ') {
            let (token, repeat) = match token.strip_suffix('+') {
                Some(t) => (t, 1 + rng.next() % 3),
                None => (token, 1),
            };
            for _ in 0..repeat {
                if let Some(inner) = token.strip_prefix('[').and_then(|t| t.strip_suffix(']')) {
                    if rng.next().is_multiple_of(2) {
                        let options: Vec<&str> = inner.split('|').collect();
                        let choice = options[rng.next() as usize % options.len()];
                        let choice = choice
                            .strip_prefix('<')
                            .and_then(|c| c.strip_suffix('>'))
                            .unwrap_or(choice);
                        let options: Vec<&str> = choice.split('|').collect();
                        let pick = options[rng.next() as usize % options.len()];
                        word(rng, pick, &mut out);
                    }
                } else if let Some(inner) =
                    token.strip_prefix('<').and_then(|t| t.strip_suffix('>'))
                {
                    let options: Vec<&str> = inner.split('|').collect();
                    let pick = options[rng.next() as usize % options.len()];
                    word(rng, pick, &mut out);
                } else {
                    word(rng, token, &mut out);
                }
            }
        }
        out
    }

    #[test]
    fn random_commands_never_panic_and_replay_exactly() {
        for seed in 1..=8 {
            fuzz(seed * 7919, 20_000);
        }
    }

    #[test]
    #[ignore = "long; run with --ignored"]
    fn random_commands_never_panic_and_replay_exactly_long() {
        for seed in 1..=200 {
            fuzz(seed * 104_729, 50_000);
        }
    }

    #[test]
    fn journaled_commands_replay_to_the_same_keyspace() {
        let mut live = Db::default();
        let mut journal: Vec<(u64, Vec<String>)> = Vec::new();
        let script = [
            (1, "SET a 1 EX 100"),
            (2, "INCR a"),
            (3, "SADD s x y z w"),
            (4, "SPOP s 2"),
            (5, "RPUSH q j1 j2"),
            (6, "BLPOP q 0"),
            (7, "XADD st * f v"),
            (8, "XGROUP CREATE st g 0"),
            (9, "XREADGROUP GROUP g c STREAMS st >"),
            (10, "ZADD z 1 m 2 n"),
            (11, "ZPOPMAX z"),
            (12, "HSET h f v"),
            (13, "GET a"),
            (14, "DEL missing"),
        ];
        for (now, command) in script {
            let argv = argv(command);
            let out = live.exec(&argv, &ctx(now));
            assert!(!out.reply.is_error(), "{command}: {:?}", out.reply);
            for entry in out.journal(&argv) {
                journal.push((now, entry.to_vec()));
            }
        }
        assert!(
            journal
                .iter()
                .all(|(_, c)| c[0] != "SPOP" && c[0] != "BLPOP" && c[0] != "GET")
        );
        assert_eq!(
            journal.len(),
            12,
            "reads and no-op writes are not journaled"
        );
        let mut replayed = Db::default();
        for (at, command) in &journal {
            replayed.exec(command, &Ctx::replay(*at));
        }
        assert_eq!(
            serde_json::to_string(live.entries()).unwrap(),
            serde_json::to_string(replayed.entries()).unwrap()
        );
    }
}
