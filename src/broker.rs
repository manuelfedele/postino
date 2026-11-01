//! Broker operations on top of [`Store`].
//!
//! Every operation runs under one mutex, so each is atomic. Effects are
//! journaled before they are applied: a failed disk write leaves memory unchanged.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError, Weak};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::commands::{
    Ctx, KEYS_RUN, Name, Outcome, Reply, grows, key_args, keys_run, visible_keys, within_reply,
    writes,
};
use crate::config::OPERATOR;
use crate::db::literal_prefix;
use crate::glob::Glob;
use crate::id::Id;
use crate::journal::{Fsync, Journal, Snapshot, Writer};
use crate::memory;
use crate::scope::{Access, Scope};
use crate::store::{
    Ask, Broadcast, Counts, Effect, Entry, Kind, Limits, Member, Message, Post, Role, Room,
    Session, State, Status, Store, broadcast_json, message_json, post_json,
};
use crate::util::{VERSION, iso8601, now_ms, random_hex};

const EVENT_HISTORY: usize = 1000;
/// About how much of the keyspace a snapshot serializes per hold of the
/// lock, little enough that requests waiting for it meanwhile are held up
/// for well under a millisecond.
const SNAPSHOT_RUN: usize = 1 << 16;
/// The rest between the runs of work that takes the lock a run at a time,
/// a snapshot or a KEYS, long enough for requests waiting for the lock to
/// take it: a thread that only yields takes it straight back.
const REST: Duration = Duration::from_micros(50);
/// How much of each second the tick may spend settling what fell due.
const EXPIRY_BUDGET: Duration = Duration::from_millis(250);
/// What one sweep settles of each kind that fell due. The rest waits for
/// the next, with requests let in between, so that many falling due at
/// once do not hold the lock for long.
const SWEEP_BATCH: usize = 1000;
/// The longest channel name, as for subscriptions.
const MAX_CHANNEL: usize = 256;
/// Patterns one scoped token may listen with at once.
const MAX_PATTERNS_PER_SCOPE: usize = 64;
const SUBSCRIBER_BUFFER: usize = 256;
/// Bytes of messages a subscription may have waiting, beyond one message:
/// one further behind is dropped, as one with [`SUBSCRIBER_BUFFER`] messages
/// waiting is.
const SUBSCRIBER_BYTES: usize = 4 << 20;
const MAX_SESSIONS_PER_AGENT: usize = 32;
const MAX_MEMBERS: usize = 256;
const MAX_BOARD_KEYS: usize = 256;
const MAX_ADDRESSEES: usize = 32;
const MAX_OPTIONS: usize = 16;
/// Questions one room holds open at once.
const MAX_OPEN_QUESTIONS: usize = 128;

/// The part of a shared limit one agent may take: a quarter, so one agent
/// filling it leaves the rest to everyone else.
fn share(limit: usize) -> usize {
    (limit / 4).max(1)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    BadRequest(String),
    Forbidden(String),
    NotFound(String),
    Conflict(String),
    Full(String),
    /// The server is at its memory limit and refuses to grow.
    OutOfMemory(String),
    Storage(String),
    /// A keyspace change is made, but the journal could not take it yet: it
    /// reaches the disk once it can. Carries what the command returned, so a
    /// value it removed is not lost with the error.
    Unsaved(String, Box<Reply>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (Error::BadRequest(m)
        | Error::Forbidden(m)
        | Error::NotFound(m)
        | Error::Conflict(m)
        | Error::Full(m)
        | Error::OutOfMemory(m)
        | Error::Storage(m)
        | Error::Unsaved(m, _)) = self;
        f.write_str(m)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Event {
    pub id: u64,
    /// Agents the event concerns; `None` means everyone (broadcasts).
    pub scope: Option<Vec<String>>,
    pub json: String,
}

struct Subscriber {
    tx: SyncSender<Arc<Event>>,
    agent: Option<String>,
    /// Dropped by the subscriber when it disconnects.
    alive: Weak<()>,
}

/// The events coming to one subscriber, for as long as it holds this.
pub struct Feed {
    rx: Receiver<Arc<Event>>,
    _alive: Arc<()>,
}

impl Feed {
    /// The next event, waiting up to `timeout` for one.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<Arc<Event>, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }
}

fn visible(event: &Event, agent: &Option<String>) -> bool {
    match agent {
        None => true,
        Some(agent) => event
            .scope
            .as_ref()
            .is_none_or(|scope| scope.iter().any(|a| a == agent)),
    }
}

struct Events {
    next: u64,
    ring: VecDeque<Arc<Event>>,
    subscribers: Vec<Subscriber>,
    /// The first event id after a name last changed hands, by rename or by
    /// being forgotten. Earlier events concerned someone else, so they are not
    /// replayed to whoever holds the name now.
    handovers: HashMap<String, u64>,
}

impl Events {
    fn new() -> Self {
        // Seeded from the clock so ids keep increasing across broker restarts and a
        // client's Last-Event-ID from a previous process reads as a gap.
        Events {
            next: now_ms() * 1000,
            ring: VecDeque::new(),
            subscribers: Vec::new(),
            handovers: HashMap::new(),
        }
    }
}

/// A message published to a channel, as delivered to one subscription. Its
/// channel and message are shared by every subscription it reaches.
pub struct Delivery {
    pub channel: Arc<str>,
    /// The pattern that matched, for pattern subscriptions.
    pub pattern: Option<String>,
    pub message: Arc<str>,
}

impl Delivery {
    /// What it adds to what its subscription has waiting.
    fn size(&self) -> usize {
        self.channel.len() + self.message.len() + self.pattern.as_ref().map_or(0, String::len)
    }
}

/// The messages waiting for one subscription, for as long as it holds this.
pub struct Deliveries {
    rx: Receiver<Arc<Delivery>>,
    /// Their bytes, counted up as they are sent and down as they are taken.
    waiting: Arc<AtomicUsize>,
    _alive: Arc<()>,
}

impl Deliveries {
    /// The next message, waiting up to `timeout` for one.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<Arc<Delivery>, RecvTimeoutError> {
        let delivery = self.rx.recv_timeout(timeout)?;
        self.waiting.fetch_sub(delivery.size(), Ordering::Relaxed);
        Ok(delivery)
    }
}

struct Subscription {
    channels: Vec<String>,
    patterns: Vec<Glob>,
    /// What the subscriber may receive; patterns never reach past it.
    scope: Arc<Scope>,
    tx: SyncSender<Arc<Delivery>>,
    waiting: Arc<AtomicUsize>,
    /// Whether it may hear of key changes, worked out once rather than on
    /// every change: see [`hears_keys`].
    keys: bool,
    /// Dropped by the subscriber when it disconnects.
    alive: Weak<()>,
}

/// Whether a subscription to `channels` and `patterns` may hear of key
/// changes. Their channels all start with `__key`, so a pattern may match
/// one when its literal start and `__key` agree as far as both go.
fn hears_keys(channels: &[String], patterns: &[Glob]) -> bool {
    channels.iter().any(|c| c.starts_with("__key"))
        || patterns.iter().any(|p| {
            let start = literal_prefix(p.as_str());
            "__key".starts_with(start) || start.starts_with("__key")
        })
}

pub enum Storage {
    Memory,
    Disk { dir: PathBuf, fsync: Fsync },
}

pub struct SendRequest {
    pub from: String,
    pub to: Option<String>,
    pub body: String,
    pub reply_to: Option<Id>,
    pub key: Option<String>,
    /// Reject unknown recipients instead of creating their inbox.
    pub strict: bool,
    /// Sent with the administrator token: not held to one sender's share of
    /// the recipient's inbox.
    pub admin: bool,
}

pub struct Leased {
    pub message: Message,
    pub receipt: String,
}

pub struct AckOutcome {
    pub acknowledged: Vec<String>,
    pub rejected: Vec<String>,
}

#[derive(Default)]
pub struct HistoryQuery {
    pub agent: Option<String>,
    pub peer: Option<String>,
    pub thread: Option<Id>,
    pub before: Option<Id>,
    pub limit: usize,
}

pub struct PresenceRequest {
    pub agent: String,
    pub session: String,
    pub client: String,
    pub pid: Option<u32>,
    pub host: Option<String>,
    pub ttl_ms: u64,
}

/// Who performs a room operation. Administrators manage every room and read
/// private ones; agents manage the rooms they own.
#[derive(Clone, Debug)]
pub struct Actor {
    pub name: String,
    pub admin: bool,
    /// An agent rather than the administrator token: it names only agents the
    /// server knows, and keeps to its share of the rooms and of a room's open
    /// questions.
    pub strict: bool,
}

pub struct CreateRoom {
    pub name: String,
    pub topic: String,
    pub private: bool,
    pub members: Vec<String>,
    /// Join the room instead of failing when it already exists.
    pub join: bool,
}

pub struct PostRequest {
    pub room: String,
    pub body: String,
    pub reply_to: Option<Id>,
    pub about: Option<String>,
    pub key: Option<String>,
    pub ask: Option<AskRequest>,
}

pub struct AnswerRequest {
    pub room: String,
    pub question: Id,
    /// May be empty when `choice` says it all.
    pub body: String,
    pub choice: Option<String>,
    pub key: Option<String>,
}

pub struct AskRequest {
    pub to: Vec<String>,
    pub options: Vec<String>,
    pub ttl_ms: Option<u64>,
}

#[derive(Default)]
pub struct PostQuery {
    pub before: Option<Id>,
    pub about: Option<String>,
    pub kind: Option<Kind>,
    pub limit: usize,
}

/// Refuses a read whose reply would take the response, `sent` bytes so far,
/// past [`crate::commands::MAX_REPLY_BYTES`]: the copy is dropped before it
/// is encoded. A command that changed data keeps its reply, which hands over
/// what it removed. Returns the bytes the reply adds.
fn cap_reply(out: &mut Outcome, sent: usize) -> usize {
    let size = out.reply.size();
    if out.dirty {
        return size;
    }
    match within_reply(sent.saturating_add(size)) {
        Ok(()) => size,
        Err(refused) => {
            out.reply = Reply::Error(refused);
            0
        }
    }
}

/// What a blocked request waits for: a change to it wakes the request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Topic {
    Inbox(String),
    Room(String),
    Key(String),
}

/// Requests blocked until something they wait for changes. Each has its own
/// condition variable, so a change wakes only the requests it concerns, not
/// every one blocked anywhere.
#[derive(Default)]
struct Waiters {
    next: u64,
    signals: HashMap<u64, Arc<Condvar>>,
    by_topic: HashMap<Topic, HashSet<u64>>,
}

impl Waiters {
    fn add(&mut self, topics: &[Topic], signal: Arc<Condvar>) -> u64 {
        let id = self.next;
        self.next += 1;
        self.signals.insert(id, signal);
        for topic in topics {
            self.by_topic.entry(topic.clone()).or_default().insert(id);
        }
        id
    }

    fn remove(&mut self, id: u64, topics: &[Topic]) {
        self.signals.remove(&id);
        for topic in topics {
            if let Some(ids) = self.by_topic.get_mut(topic) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.by_topic.remove(topic);
                }
            }
        }
    }

    fn wake(&self, topics: &[Topic]) {
        for topic in topics {
            for id in self.by_topic.get(topic).into_iter().flatten() {
                if let Some(signal) = self.signals.get(id) {
                    signal.notify_one();
                }
            }
        }
    }
}

pub struct Core {
    pub store: Store,
    pub limits: Limits,
    journal: Option<Journal>,
    events: Events,
    channels: Vec<Subscription>,
    /// What changed in the current operation, for the waiters to see.
    wake: Vec<Topic>,
    waiters: Waiters,
    /// Agents with a token, known before they first connect.
    token_agents: HashSet<String>,
    /// Changes journaled but not written yet: see [`batch_journal_writes`].
    deferred: Arc<AtomicBool>,
    /// A thread panicked while it held the lock: see [`Broker::lock`].
    halted: bool,
}

/// What a change is refused with once a request has failed partway through
/// one (see [`Broker::lock`]).
pub const HALTED: &str = "READONLY a request failed partway through a change, which may be left half made; no more changes are taken until the server restarts";

impl Core {
    pub fn new(store: Store, limits: Limits, journal: Option<Journal>) -> Core {
        Core {
            store,
            limits,
            journal,
            events: Events::new(),
            channels: Vec::new(),
            wake: Vec::new(),
            waiters: Waiters::default(),
            token_agents: HashSet::new(),
            deferred: Arc::new(AtomicBool::new(false)),
            halted: false,
        }
    }

    pub fn set_token_agents(&mut self, names: impl IntoIterator<Item = String>) {
        self.token_agents = names.into_iter().collect();
    }

    fn commit(&mut self, effect: Effect) -> Result<()> {
        if self.halted {
            return Err(Error::Storage(HALTED.into()));
        }
        if let Some(journal) = self.journal.as_mut() {
            // On a thread that batches, written with the rest of the round.
            let journaled = if BATCHING.get() && journal.defers() {
                self.deferred.store(true, Ordering::Release);
                journal.defer_applied(&effect)
            } else {
                journal.append(&effect)
            };
            journaled.map_err(|e| Error::Storage(format!("journal write failed: {e}")))?;
        }
        self.store.apply(&effect);
        Ok(())
    }

    fn emit(&mut self, kind: &str, scope: Vec<String>, data: Value, now: u64) {
        self.publish_event(kind, Some(scope), data, now);
    }

    fn emit_all(&mut self, kind: &str, data: Value, now: u64) {
        self.publish_event(kind, None, data, now);
    }

    fn publish_event(&mut self, kind: &str, scope: Option<Vec<String>>, data: Value, now: u64) {
        let id = self.events.next;
        self.events.next += 1;
        let mut object = json!({ "id": id, "type": kind, "at": iso8601(now) });
        if let (Some(target), Value::Object(fields)) = (object.as_object_mut(), data) {
            target.extend(fields);
        }
        let event = Arc::new(Event {
            id,
            scope,
            json: object.to_string(),
        });
        if self.events.ring.len() == EVENT_HISTORY {
            self.events.ring.pop_front();
        }
        self.events.ring.push_back(event.clone());
        // A subscriber that falls behind is dropped; it reconnects with
        // Last-Event-ID and replays from the ring instead of losing events.
        self.events.subscribers.retain(|sub| {
            if !visible(&event, &sub.agent) {
                return true;
            }
            match sub.tx.try_send(event.clone()) {
                Ok(()) => true,
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => false,
            }
        });
    }

    pub fn subscribe(
        &mut self,
        after: Option<u64>,
        agent: Option<String>,
    ) -> (Vec<Arc<Event>>, Feed, bool) {
        let oldest = self.events.ring.front().map(|e| e.id);
        let resync = match (after, oldest) {
            (Some(after), Some(oldest)) => {
                after.saturating_add(1) < oldest || after >= self.events.next
            }
            (Some(after), None) => {
                after.saturating_add(1) < self.events.next || after >= self.events.next
            }
            (None, _) => false,
        };
        let since = agent
            .as_ref()
            .and_then(|a| self.events.handovers.get(a))
            .copied()
            .unwrap_or(0);
        let replay = match after {
            Some(after) if !resync => self
                .events
                .ring
                .iter()
                .filter(|e| e.id > after && e.id >= since && visible(e, &agent))
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        // Those that left go first, though no event has come to find them
        // gone: subscribers that come and go cannot pile up meanwhile.
        self.events
            .subscribers
            .retain(|s| s.alive.strong_count() > 0);
        let (tx, rx) = sync_channel(SUBSCRIBER_BUFFER);
        let alive = Arc::new(());
        self.events.subscribers.push(Subscriber {
            tx,
            agent,
            alive: Arc::downgrade(&alive),
        });
        (replay, Feed { rx, _alive: alive }, resync)
    }

    pub fn subscriber_count(&self) -> usize {
        self.events.subscribers.len()
    }

    /// `name` now belongs to someone else, or to nobody: its open event
    /// streams end, and what happened under it before is not replayed.
    fn hand_over(&mut self, name: &str) {
        let events = &mut self.events;
        let oldest = events.ring.front().map_or(events.next, |e| e.id);
        events.handovers.retain(|_, since| *since > oldest);
        events.handovers.insert(name.to_string(), events.next);
        events
            .subscribers
            .retain(|s| s.agent.as_deref() != Some(name));
    }

    // ---- agents ----------------------------------------------------------

    fn is_online(&self, agent: &str, now: u64) -> bool {
        self.store.live_sessions(agent, now) > 0
    }

    fn ensure_agents(&mut self, names: &[&str], now: u64) -> Result<()> {
        let mut missing: Vec<String> = Vec::new();
        for name in names {
            if !self.store.agents.contains_key(*name) && !missing.iter().any(|m| m == name) {
                missing.push(name.to_string());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let overflow =
            (self.store.agents.len() + missing.len()).saturating_sub(self.limits.max_agents);
        if overflow > 0 {
            let removable: Vec<String> = self
                .removable(now)
                .into_iter()
                .filter(|name| !names.contains(&name.as_str()))
                .collect();
            if removable.len() < overflow {
                return Err(Error::Full(format!(
                    "agent namespace is full (maximum {})",
                    self.limits.max_agents
                )));
            }
            for name in removable.into_iter().take(overflow) {
                self.forget_agent(&name, now)?;
            }
        }
        for name in missing {
            self.commit(Effect::Agent {
                name: name.clone(),
                at: now,
            })?;
            self.emit("agent", vec![name.clone()], json!({ "agent": name }), now);
        }
        Ok(())
    }

    fn removable(&self, now: u64) -> Vec<String> {
        let online = self.store.online_counts(now);
        let members = self.store.room_members();
        let mut candidates: Vec<(&String, u64)> = self
            .store
            .agents
            .values()
            .filter(|a| {
                !online.contains_key(a.name.as_str())
                    && !members.contains(a.name.as_str())
                    && self.store.counts(&a.name).pending() == 0
            })
            .map(|a| (&a.name, a.last_seen))
            .collect();
        candidates.sort_by_key(|(_, seen)| *seen);
        candidates
            .into_iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn forget_agent(&mut self, name: &str, now: u64) -> Result<()> {
        self.commit(Effect::Forget {
            agent: name.to_string(),
        })?;
        self.emit(
            "removed",
            vec![name.to_string()],
            json!({ "agent": name }),
            now,
        );
        self.hand_over(name);
        Ok(())
    }

    pub fn cleanup(&mut self, now: u64) -> Result<Vec<String>> {
        let removed = self.removable(now);
        for name in &removed {
            self.forget_agent(name, now)?;
        }
        Ok(removed)
    }

    pub fn forget(&mut self, name: &str, now: u64) -> Result<()> {
        if !self.store.agents.contains_key(name) {
            return Err(Error::NotFound(format!("unknown agent {name:?}")));
        }
        self.forget_agent(name, now)
    }

    pub fn presence(&mut self, req: PresenceRequest, now: u64) -> Result<u64> {
        if let Some(existing) = self.store.sessions.get(&req.session)
            && existing.agent != req.agent
        {
            return Err(Error::Conflict(
                "session id belongs to another agent".into(),
            ));
        }
        self.ensure_agents(&[&req.agent], now)?;
        let was_online = self.is_online(&req.agent, now);
        if !self.store.sessions.contains_key(&req.session) {
            let mut own: Vec<(u64, String)> = self
                .store
                .sessions
                .iter()
                .filter(|(_, s)| s.agent == req.agent)
                .map(|(id, s)| (s.expires, id.clone()))
                .collect();
            if own.len() >= MAX_SESSIONS_PER_AGENT {
                own.sort();
                for (_, id) in &own[..=own.len() - MAX_SESSIONS_PER_AGENT] {
                    self.store.sessions.remove(id);
                }
            }
        }
        let expires = now + req.ttl_ms;
        let started = self
            .store
            .sessions
            .get(&req.session)
            .filter(|s| s.agent == req.agent)
            .map_or(now, |s| s.started);
        self.store.sessions.insert(
            req.session,
            Session {
                agent: req.agent.clone(),
                client: req.client,
                started,
                expires,
                pid: req.pid,
                host: req.host,
            },
        );
        if let Some(agent) = self.store.agents.get_mut(&req.agent) {
            agent.last_seen = now;
        }
        if !was_online {
            self.emit(
                "presence",
                vec![req.agent.clone()],
                json!({ "agent": req.agent, "online": true }),
                now,
            );
        }
        Ok(expires)
    }

    pub fn leave(&mut self, agent: &str, session: &str, now: u64) {
        if self
            .store
            .sessions
            .get(session)
            .is_some_and(|s| s.agent == agent)
        {
            self.store.sessions.remove(session);
            if let Some(record) = self.store.agents.get_mut(agent) {
                record.last_seen = now;
            }
            if !self.is_online(agent, now) {
                self.emit(
                    "presence",
                    vec![agent.to_string()],
                    json!({ "agent": agent, "online": false }),
                    now,
                );
            }
        }
    }

    pub fn rename(&mut self, from: &str, to: &str, now: u64) -> Result<()> {
        if from == to {
            return Ok(());
        }
        if !self.store.agents.contains_key(from) {
            return Err(Error::NotFound(format!("unknown agent {from:?}")));
        }
        let live = self.store.live_sessions(from, now);
        if live > 1 {
            return Err(Error::Conflict(format!(
                "cannot rename {from:?} while {live} instances are online"
            )));
        }
        if self.store.agents.contains_key(to) {
            if self.is_online(to, now) || self.store.counts(to).pending() > 0 {
                return Err(Error::Conflict(format!(
                    "agent name {to:?} is already in use"
                )));
            }
            self.commit(Effect::Forget {
                agent: to.to_string(),
            })?;
        }
        self.commit(Effect::Rename {
            from: from.to_string(),
            to: to.to_string(),
        })?;
        // The messages queued for the old name now wait under the new one.
        self.wake.push(Topic::Inbox(to.to_string()));
        self.emit(
            "rename",
            vec![from.to_string(), to.to_string()],
            json!({ "from": from, "to": to }),
            now,
        );
        self.hand_over(from);
        self.hand_over(to);
        Ok(())
    }

    pub fn agent_json(&self, name: &str, online: usize) -> Value {
        let counts = self.store.counts(name);
        let agent = &self.store.agents[name];
        json!({
            "name": name,
            "online": online > 0,
            "sessions": online,
            "queued": counts.queued,
            "leased": counts.leased,
            "dead": counts.dead,
            "pending": counts.pending(),
            "messageCount": counts.pending(),
            "unseenBroadcasts": self.store.unseen_broadcasts(name),
            "lastSeen": iso8601(agent.last_seen),
        })
    }

    /// Every agent and its presence. How many messages wait in an inbox is
    /// shown to the administrator, and to a `viewer` only for its own.
    pub fn agents_json(&self, now: u64, viewer: Option<&str>) -> Vec<Value> {
        let online = self.store.online_counts(now);
        self.store
            .agents
            .values()
            .map(|agent| {
                let sessions = online.get(agent.name.as_str()).copied().unwrap_or(0);
                if viewer.is_none_or(|v| v == agent.name) {
                    self.agent_json(&agent.name, sessions)
                } else {
                    json!({
                        "name": agent.name,
                        "online": sessions > 0,
                        "sessions": sessions,
                        "lastSeen": iso8601(agent.last_seen),
                    })
                }
            })
            .collect()
    }

    /// Refuses to store more once the process is past its memory limit.
    /// Reads, deletes, and acknowledgements still work, so clients can make room.
    fn room_to_grow(&self) -> Result<()> {
        match self.over_memory() {
            Some(message) => Err(Error::OutOfMemory(message)),
            None => Ok(()),
        }
    }

    /// Why the journal cannot take changes now, after trying once more to
    /// write what it holds back.
    fn journal_failing(&mut self) -> Option<String> {
        let journal = self.journal.as_mut()?;
        journal.failing()?;
        let _ = journal.flush_pending();
        journal.failing()
    }

    fn over_memory(&self) -> Option<String> {
        let (limit, used) = (self.limits.max_memory, memory::in_use());
        (limit > 0 && used > limit).then(|| {
            format!(
                "OOM the server is using {} of its {} limit (POSTINO_MAX_MEMORY); delete data or raise the limit",
                memory::mib(used),
                memory::mib(limit)
            )
        })
    }

    pub fn known_agent(&self, name: &str) -> bool {
        self.store.agents.contains_key(name)
    }

    /// Refuses a name the server does not know, listing some it does. It
    /// knows the agents it has seen, those with a token, and the operator.
    /// Only the administrator token brings in new names: an agent that could
    /// name anyone would fill the agent namespace and push out agents that
    /// went quiet.
    fn require_known(&self, name: &str) -> Result<()> {
        if name == OPERATOR
            || self.store.agents.contains_key(name)
            || self.token_agents.contains(name)
        {
            return Ok(());
        }
        let known: Vec<&str> = self
            .store
            .agents
            .keys()
            .take(25)
            .map(String::as_str)
            .collect();
        Err(Error::NotFound(if known.is_empty() {
            format!("unknown agent {name:?}; no agents are registered yet")
        } else {
            format!("unknown agent {name:?}; known agents: {}", known.join(", "))
        }))
    }

    /// Whether anything refers to `name`: an inbox, a message to or from it, a
    /// broadcast it sent, a room membership or creation, a post, or a question
    /// addressed to it.
    pub fn name_in_use(&self, name: &str) -> bool {
        self.store.agents.contains_key(name)
            || self
                .store
                .messages
                .values()
                .any(|m| m.from == name || m.to == name)
            || self.store.broadcasts.values().any(|b| b.from == name)
            || self.store.rooms.values().any(|room| {
                room.created_by == name
                    || room.members.contains_key(name)
                    || room.posts.values().any(|post| {
                        post.from == name
                            || post
                                .ask
                                .as_ref()
                                .is_some_and(|ask| ask.to.iter().any(|n| n == name))
                    })
            })
    }

    // ---- direct messages -------------------------------------------------

    pub fn send(&mut self, req: SendRequest, now: u64) -> Result<(Message, bool)> {
        self.room_to_grow()?;
        // Only a message's sender and recipient can reply to it; to anyone
        // else it does not exist, so ids reveal nothing about other threads.
        let parent = req
            .reply_to
            .and_then(|id| self.store.messages.get(&id))
            .filter(|p| p.to == req.from || p.from == req.from)
            .cloned();
        let to = match (req.to, &parent) {
            (Some(to), _) => to,
            (None, Some(parent)) => parent.from.clone(),
            (None, None) => {
                return Err(match req.reply_to {
                    Some(id) => Error::NotFound(format!(
                        "message {id} not found; pass \"to\" to reply to a message that has been trimmed"
                    )),
                    None => Error::BadRequest("\"to\" is required".into()),
                });
            }
        };
        if let Some(key) = &req.key
            && let Some(existing) = self.store.message_by_key(&to, &req.from, key)
        {
            return Ok((existing.clone(), false));
        }
        if req.strict {
            self.require_known(&to)?;
        }
        if self.store.counts(&to).pending() >= self.limits.max_inbox {
            return Err(Error::Full(format!(
                "inbox {to:?} is full ({} pending messages)",
                self.limits.max_inbox
            )));
        }
        let quota = share(self.limits.max_inbox);
        if !req.admin && to != req.from && self.store.pending_from(&to, &req.from) >= quota {
            return Err(Error::Full(format!(
                "{} already has {quota} messages waiting in {to:?}'s inbox, a quarter of it; wait until {to} reads some",
                req.from
            )));
        }
        self.ensure_agents(&[&req.from, &to], now)?;
        let id = self.store.next_id(now);
        let message = Message {
            id,
            from: req.from,
            to: to.clone(),
            body: req.body,
            created: now,
            expires: now + self.limits.msg_ttl_ms,
            // A reply joins its parent's thread only when the sender can see
            // the parent; any other id starts a new thread.
            thread: parent.map_or(id, |p| p.thread),
            reply_to: req.reply_to,
            key: req.key,
            state: State::Queued,
            deliveries: 0,
            lease: None,
            settled: None,
        };
        self.commit(Effect::Message {
            message: message.clone(),
        })?;
        self.trim_history(&to)?;
        self.emit(
            "message",
            vec![message.to.clone(), message.from.clone()],
            json!({ "message": message_json(&message) }),
            now,
        );
        self.wake.push(Topic::Inbox(message.to.clone()));
        Ok((message, true))
    }

    fn trim_history(&mut self, agent: &str) -> Result<()> {
        let excess = self
            .store
            .counts(agent)
            .settled
            .saturating_sub(self.limits.history_limit);
        if excess > 0 {
            let ids: Vec<Id> = self.store.settled(agent).take(excess).collect();
            self.commit(Effect::Drop { ids })?;
        }
        Ok(())
    }

    /// Returns a batch of expired leases to the queue, dead-lettering entries
    /// that reached the delivery limit (run before each lease and by the
    /// sweep). Only leases that are due are visited. Returns whether more are.
    fn reclaim(&mut self, now: u64) -> Result<bool> {
        let expired: Vec<(Id, String, u32)> = self
            .store
            .leases_due(now)
            .take(SWEEP_BATCH)
            .filter_map(|id| self.store.messages.get(&id))
            .map(|m| (m.id, m.to.clone(), m.deliveries))
            .collect();
        for (id, inbox, deliveries) in expired {
            let dead = deliveries >= self.limits.max_deliveries;
            let state = if dead { State::Dead } else { State::Queued };
            self.commit(Effect::Settle { id, state, at: now })?;
            let kind = if dead { "dead" } else { "requeue" };
            self.emit(
                kind,
                vec![inbox.clone()],
                json!({ "inbox": inbox, "ids": [id.to_string()], "deliveries": deliveries }),
                now,
            );
            if dead {
                self.trim_history(&inbox)?;
            } else {
                self.wake.push(Topic::Inbox(inbox));
            }
        }
        Ok(self.store.leases_due(now).next().is_some())
    }

    /// Settles a batch of pending messages that expired. Returns whether
    /// more did.
    fn expire(&mut self, now: u64) -> Result<bool> {
        let expired: Vec<(Id, String)> = self
            .store
            .expired_due(now)
            .take(SWEEP_BATCH)
            .filter_map(|id| self.store.messages.get(&id))
            .map(|m| (m.id, m.to.clone()))
            .collect();
        for (id, inbox) in expired {
            self.commit(Effect::Settle {
                id,
                state: State::Expired,
                at: now,
            })?;
            self.emit(
                "expired",
                vec![inbox.clone()],
                json!({ "inbox": inbox, "ids": [id.to_string()] }),
                now,
            );
            self.trim_history(&inbox)?;
        }
        Ok(self.store.expired_due(now).next().is_some())
    }

    pub fn lease(
        &mut self,
        inbox: &str,
        consumer: &str,
        limit: usize,
        now: u64,
    ) -> Result<Vec<Leased>> {
        self.lease_where(inbox, consumer, limit, None, now)
    }

    /// Leases specific queued messages, for a reader that handles one at a time.
    pub fn lease_ids(
        &mut self,
        inbox: &str,
        consumer: &str,
        ids: &[Id],
        now: u64,
    ) -> Result<Vec<Leased>> {
        self.lease_where(inbox, consumer, ids.len(), Some(ids), now)
    }

    fn lease_where(
        &mut self,
        inbox: &str,
        consumer: &str,
        limit: usize,
        only: Option<&[Id]>,
        now: u64,
    ) -> Result<Vec<Leased>> {
        self.reclaim(now)?;
        self.expire(now)?;
        // One that expired but that no sweep has settled yet is not handed out.
        let live = |id: &Id| self.store.messages.get(id).is_some_and(|m| m.expires > now);
        let ids: Vec<Id> = match only {
            None => self.store.queued(inbox).filter(live).take(limit).collect(),
            Some(wanted) => {
                let mut ids: Vec<Id> = wanted
                    .iter()
                    .copied()
                    .filter(|id| {
                        self.store
                            .messages
                            .get(id)
                            .is_some_and(|m| m.to == inbox && m.state == State::Queued)
                            && live(id)
                    })
                    .collect();
                ids.sort_unstable();
                ids.dedup();
                ids.truncate(limit);
                ids
            }
        };
        let until = now + self.limits.lease_ms;
        let mut leased = Vec::with_capacity(ids.len());
        for id in ids {
            let token = random_hex(8);
            self.commit(Effect::Lease {
                id,
                consumer: consumer.to_string(),
                token: token.clone(),
                at: now,
                until,
            })?;
            leased.push(Leased {
                message: self.store.messages[&id].clone(),
                receipt: format!("{id}.{token}"),
            });
        }
        if !leased.is_empty() {
            let ids: Vec<String> = leased.iter().map(|l| l.message.id.to_string()).collect();
            self.emit(
                "lease",
                vec![inbox.to_string()],
                json!({ "inbox": inbox, "ids": ids, "consumer": consumer, "until": iso8601(until) }),
                now,
            );
        }
        Ok(leased)
    }

    /// Acknowledges by receipt (`<id>.<token>`). A bare id is accepted when the
    /// caller names the consumer that holds the lease.
    pub fn ack(
        &mut self,
        inbox: &str,
        receipts: &[String],
        consumer: Option<&str>,
        now: u64,
    ) -> Result<AckOutcome> {
        let mut outcome = AckOutcome {
            acknowledged: Vec::new(),
            rejected: Vec::new(),
        };
        let mut acked_ids = Vec::new();
        for receipt in receipts {
            let (raw_id, token) = match receipt.split_once('.') {
                Some((id, token)) => (id, Some(token)),
                None => (receipt.as_str(), None),
            };
            let Some(message) = raw_id
                .parse::<Id>()
                .ok()
                .and_then(|id| self.store.messages.get(&id))
            else {
                outcome.rejected.push(receipt.clone());
                continue;
            };
            if message.to != inbox {
                outcome.rejected.push(receipt.clone());
                continue;
            }
            if message.state == State::Acked {
                outcome.acknowledged.push(receipt.clone());
                continue;
            }
            let matches = message.state.is_pending()
                && message.lease.as_ref().is_some_and(|lease| match token {
                    Some(token) => {
                        crate::util::constant_time_eq(lease.token.as_bytes(), token.as_bytes())
                    }
                    None => consumer == Some(lease.consumer.as_str()),
                });
            if !matches {
                outcome.rejected.push(receipt.clone());
                continue;
            }
            let id = message.id;
            self.commit(Effect::Settle {
                id,
                state: State::Acked,
                at: now,
            })?;
            acked_ids.push(id.to_string());
            outcome.acknowledged.push(receipt.clone());
        }
        if !acked_ids.is_empty() {
            self.trim_history(inbox)?;
            self.emit(
                "ack",
                vec![inbox.to_string()],
                json!({ "inbox": inbox, "ids": acked_ids }),
                now,
            );
        }
        Ok(outcome)
    }

    pub fn pending(&self, inbox: &str) -> Vec<&Message> {
        self.store
            .inbox_messages(inbox)
            .filter(|m| m.state.is_pending())
            .collect()
    }

    pub fn counts(&self, inbox: &str) -> Counts {
        self.store.counts(inbox)
    }

    pub fn purge(&mut self, inbox: &str, now: u64) -> Result<usize> {
        let ids: Vec<Id> = self.store.inbox_messages(inbox).map(|m| m.id).collect();
        let count = ids.len();
        if count > 0 {
            self.commit(Effect::Drop { ids })?;
        }
        self.emit(
            "purge",
            vec![inbox.to_string()],
            json!({ "inbox": inbox }),
            now,
        );
        Ok(count)
    }

    /// Newest-last page of history, scanning backwards from `before`.
    pub fn history(&self, query: &HistoryQuery) -> (Vec<&Message>, Option<Id>) {
        let matches = |m: &&Message| {
            let agent_ok = match (&query.agent, &query.peer) {
                (Some(a), Some(p)) => (&m.from == a && &m.to == p) || (&m.from == p && &m.to == a),
                (Some(a), None) => &m.from == a || &m.to == a,
                (None, Some(p)) => &m.from == p || &m.to == p,
                (None, None) => true,
            };
            agent_ok && query.thread.is_none_or(|t| m.thread == t)
        };
        let range = match query.before {
            Some(before) => self.store.messages.range(..before),
            None => self.store.messages.range(..),
        };
        let mut items: Vec<&Message> = range
            .rev()
            .map(|(_, m)| m)
            .filter(matches)
            .take(query.limit + 1)
            .collect();
        let next = if items.len() > query.limit {
            items.truncate(query.limit);
            items.last().map(|m| m.id)
        } else {
            None
        };
        items.reverse();
        (items, next)
    }

    pub fn message(&self, id: Id) -> Option<&Message> {
        self.store.messages.get(&id)
    }

    // ---- broadcasts ------------------------------------------------------

    pub fn broadcast(
        &mut self,
        from: &str,
        body: String,
        key: Option<String>,
        now: u64,
    ) -> Result<(Broadcast, bool)> {
        if let Some(key) = &key
            && let Some(existing) = self.store.broadcast_by_key(from, key)
        {
            return Ok((existing.clone(), false));
        }
        self.room_to_grow()?;
        self.ensure_agents(&[from], now)?;
        let broadcast = Broadcast {
            id: self.store.next_id(now),
            from: from.to_string(),
            body,
            created: now,
            expires: now + self.limits.msg_ttl_ms,
            key,
        };
        self.commit(Effect::Broadcast {
            broadcast: broadcast.clone(),
        })?;
        self.trim_broadcasts()?;
        self.emit_all(
            "broadcast",
            json!({ "broadcast": broadcast_json(&broadcast) }),
            now,
        );
        Ok((broadcast, true))
    }

    /// Trims the broadcasts to their limit, oldest first. Senders holding more
    /// than a quarter of them lose theirs first, so one agent's flood does not
    /// push out everyone else's.
    fn trim_broadcasts(&mut self) -> Result<()> {
        let excess = self
            .store
            .broadcasts
            .len()
            .saturating_sub(self.limits.max_broadcasts);
        if excess == 0 {
            return Ok(());
        }
        let quota = share(self.limits.max_broadcasts);
        let mut held: HashMap<&str, usize> = HashMap::new();
        for b in self.store.broadcasts.values() {
            *held.entry(b.from.as_str()).or_default() += 1;
        }
        let mut ids: Vec<Id> = Vec::new();
        for b in self.store.broadcasts.values() {
            if ids.len() == excess {
                break;
            }
            let count = held.entry(b.from.as_str()).or_default();
            if *count > quota {
                *count -= 1;
                ids.push(b.id);
            }
        }
        for id in self.store.broadcasts.keys() {
            if ids.len() == excess {
                break;
            }
            if !ids.contains(id) {
                ids.push(*id);
            }
        }
        self.commit(Effect::DropBroadcasts { ids })
    }

    pub fn broadcasts(&self, after: Option<Id>, limit: usize) -> (Vec<&Broadcast>, usize) {
        let items: Vec<&Broadcast> = match after {
            Some(after) => self
                .store
                .broadcasts
                .range((Excluded(after), Unbounded))
                .map(|(_, b)| b)
                .collect(),
            None => self.store.broadcasts.values().collect(),
        };
        let skip = items.len().saturating_sub(limit);
        (items[skip..].to_vec(), self.store.broadcasts.len())
    }

    /// Unseen broadcasts for `agent` (all retained when `all`), optionally moving
    /// its cursor past the newest entry. Cursors compare by id, so trimming the
    /// stream never hides or replays entries.
    pub fn read_broadcasts(
        &mut self,
        agent: &str,
        all: bool,
        advance: bool,
    ) -> Result<(Vec<Broadcast>, usize)> {
        let cursor = self.store.agents.get(agent).and_then(|a| a.cursor);
        let items: Vec<Broadcast> = self
            .store
            .broadcasts
            .values()
            .filter(|b| all || (cursor.is_none_or(|c| b.id > c) && b.from != agent))
            .cloned()
            .collect();
        let newest = self.store.broadcasts.keys().next_back().copied();
        if advance
            && self.store.agents.contains_key(agent)
            && let Some(newest) = newest
            && cursor != Some(newest)
        {
            self.commit(Effect::Cursor {
                agent: agent.to_string(),
                id: newest,
            })?;
        }
        Ok((items, self.store.broadcasts.len()))
    }

    pub fn clear_broadcasts(&mut self, now: u64) -> Result<()> {
        let ids: Vec<Id> = self.store.broadcasts.keys().copied().collect();
        if !ids.is_empty() {
            self.commit(Effect::DropBroadcasts { ids })?;
        }
        self.emit_all("broadcasts_cleared", json!({}), now);
        Ok(())
    }

    // ---- rooms -----------------------------------------------------------

    fn room(&self, name: &str) -> Result<&Room> {
        self.store
            .rooms
            .get(name)
            .ok_or_else(|| Error::NotFound(format!("unknown room {name:?}")))
    }

    fn open_room(&self, name: &str) -> Result<&Room> {
        let room = self.room(name)?;
        if room.is_open() {
            Ok(room)
        } else {
            Err(Error::Conflict(format!("room {name:?} is closed")))
        }
    }

    fn manages(room: &Room, actor: &Actor) -> bool {
        actor.admin || room.is_owner(&actor.name)
    }

    fn require_owner(room: &Room, actor: &Actor) -> Result<()> {
        if Core::manages(room, actor) {
            Ok(())
        } else {
            Err(Error::Forbidden(format!(
                "only owners of room {:?} can do that",
                room.name
            )))
        }
    }

    /// A room as `actor` may see it. Outsiders learn only that a private room
    /// exists, whatever they ask of it, so nothing about its members, posts,
    /// or questions can be read off the errors they get. Every operation on a
    /// room checks this first.
    fn visible_room(&self, name: &str, actor: &Actor) -> Result<&Room> {
        let room = self.room(name)?;
        if actor.admin || !room.private || room.members.contains_key(&actor.name) {
            Ok(room)
        } else {
            Err(Error::Forbidden(format!(
                "room {name:?} is private to its members; an owner has to add you"
            )))
        }
    }

    pub fn can_view(&self, room: &str, actor: &Actor) -> Result<()> {
        self.visible_room(room, actor).map(|_| ())
    }

    fn room_scope(&self, room: &str) -> Vec<String> {
        self.store
            .rooms
            .get(room)
            .map(|r| r.members.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn trim_room(&mut self, room: &str) -> Result<()> {
        let Some(record) = self.store.rooms.get(room) else {
            return Ok(());
        };
        let excess = record.posts.len().saturating_sub(self.limits.room_history);
        // Open questions, and the answers they count, stay until the question
        // resolves, is withdrawn, or expires.
        let ids: Vec<Id> = record
            .posts
            .values()
            .filter(|p| !record.keeps(p))
            .take(excess)
            .map(|p| p.id)
            .collect();
        if !ids.is_empty() {
            self.commit(Effect::DropPosts {
                room: room.to_string(),
                ids,
            })?;
        }
        Ok(())
    }

    fn append(&mut self, room: &str, post: Post, now: u64) -> Result<()> {
        self.commit(Effect::Post {
            room: room.to_string(),
            post: post.clone(),
        })?;
        self.trim_room(room)?;
        let data = json!({ "room": room, "post": post_json(&self.store.rooms[room], &post) });
        let scope = self.room_scope(room);
        self.emit("post", scope, data, now);
        self.wake.push(Topic::Room(room.to_string()));
        Ok(())
    }

    /// Records a change to the room in its timeline, so members reading the
    /// room learn about joins, topics, and board updates in order.
    fn log(&mut self, room: &str, from: &str, body: String, now: u64) -> Result<()> {
        let post = Post {
            id: self.store.next_id(now),
            from: from.to_string(),
            kind: Kind::Event,
            body,
            created: now,
            reply_to: None,
            about: None,
            choice: None,
            ask: None,
            key: None,
        };
        self.append(room, post, now)
    }

    fn set_question(&mut self, room: &str, id: Id, status: Status, now: u64) -> Result<()> {
        self.commit(Effect::Question {
            room: room.to_string(),
            id,
            status,
            at: now,
        })?;
        let scope = self.room_scope(room);
        self.emit(
            "question",
            scope,
            json!({ "room": room, "id": id.to_string(), "status": status }),
            now,
        );
        self.wake.push(Topic::Room(room.to_string()));
        Ok(())
    }

    fn drop_room(&mut self, room: &str, now: u64) -> Result<()> {
        let scope = self.room_scope(room);
        self.commit(Effect::DropRoom {
            room: room.to_string(),
        })?;
        self.emit(
            "room",
            scope,
            json!({ "room": room, "change": "deleted" }),
            now,
        );
        self.wake.push(Topic::Room(room.to_string()));
        Ok(())
    }

    pub fn create_room(&mut self, req: CreateRoom, actor: &Actor, now: u64) -> Result<bool> {
        self.room_to_grow()?;
        if self.store.rooms.contains_key(&req.name) {
            if !req.join {
                return Err(Error::Conflict(format!(
                    "room {:?} already exists; join it instead",
                    req.name
                )));
            }
            self.join(&req.name, &actor.name, None, actor, now)?;
            return Ok(false);
        }
        let mut names: Vec<&str> = vec![actor.name.as_str()];
        for name in &req.members {
            if !names.contains(&name.as_str()) {
                names.push(name);
            }
        }
        if names.len() > MAX_MEMBERS {
            return Err(Error::BadRequest(format!(
                "a room holds at most {MAX_MEMBERS} members"
            )));
        }
        if actor.strict {
            for name in &names[1..] {
                self.require_known(name)?;
            }
            let quota = share(self.limits.max_rooms);
            let created = self
                .store
                .rooms
                .values()
                .filter(|r| r.is_open() && r.created_by == actor.name)
                .count();
            if created >= quota {
                return Err(Error::Full(format!(
                    "{} already created {created} open rooms, a quarter of the {} the server holds; close one first",
                    actor.name, self.limits.max_rooms
                )));
            }
        }
        // At the limit, the room closed longest ago makes way, once nothing
        // else can fail.
        let evict = if self.store.rooms.len() >= self.limits.max_rooms {
            let oldest_closed = self
                .store
                .rooms
                .values()
                .filter_map(|r| r.closed.map(|at| (at, r.name.clone())))
                .min();
            match oldest_closed {
                Some((_, name)) => Some(name),
                None => {
                    return Err(Error::Full(format!(
                        "room limit reached (maximum {}); close or delete a room first",
                        self.limits.max_rooms
                    )));
                }
            }
        } else {
            None
        };
        self.ensure_agents(&names, now)?;
        if let Some(name) = evict {
            self.drop_room(&name, now)?;
        }
        let members = names
            .iter()
            .map(|name| {
                let role = if *name == actor.name {
                    Role::Owner
                } else {
                    Role::Member
                };
                (
                    name.to_string(),
                    Member {
                        role,
                        joined: now,
                        cursor: None,
                    },
                )
            })
            .collect();
        self.commit(Effect::Room {
            room: Room {
                name: req.name.clone(),
                topic: req.topic.clone(),
                created: now,
                created_by: actor.name.clone(),
                private: req.private,
                closed: None,
                members,
                board: BTreeMap::new(),
                posts: BTreeMap::new(),
                open: Default::default(),
            },
        })?;
        let scope = self.room_scope(&req.name);
        self.emit(
            "room",
            scope,
            json!({ "room": req.name, "change": "created" }),
            now,
        );
        let body = if req.topic.is_empty() {
            "created the room".to_string()
        } else {
            format!("created the room: {}", req.topic)
        };
        self.log(&req.name, &actor.name, body, now)?;
        Ok(true)
    }

    /// Adds `agent` to a room. Agents join public rooms by themselves; owners
    /// add anyone and change roles. Returns whether anything changed.
    pub fn join(
        &mut self,
        room: &str,
        agent: &str,
        role: Option<Role>,
        actor: &Actor,
        now: u64,
    ) -> Result<bool> {
        self.visible_room(room, actor)?;
        let record = self.open_room(room)?;
        let manages = Core::manages(record, actor);
        if agent != actor.name && !manages {
            return Err(Error::Forbidden(format!(
                "only owners of room {room:?} can add other agents"
            )));
        }
        if role == Some(Role::Owner) && !manages {
            return Err(Error::Forbidden(format!(
                "only owners of room {room:?} can appoint owners"
            )));
        }
        let existing = record.members.get(agent).cloned();
        if existing.is_none() && record.private && !manages {
            return Err(Error::Forbidden(format!(
                "room {room:?} is private; an owner has to add you"
            )));
        }
        if existing.is_none() && record.members.len() >= MAX_MEMBERS {
            return Err(Error::Full(format!(
                "room {room:?} is full ({MAX_MEMBERS} members)"
            )));
        }
        let member = match existing {
            Some(member) => match role {
                Some(role) if role != member.role => {
                    if !manages {
                        return Err(Error::Forbidden(format!(
                            "only owners of room {room:?} can change roles"
                        )));
                    }
                    Member { role, ..member }
                }
                _ => return Ok(false),
            },
            None => {
                if agent != actor.name && actor.strict {
                    self.require_known(agent)?;
                }
                self.ensure_agents(&[agent], now)?;
                Member {
                    role: role.unwrap_or(Role::Member),
                    joined: now,
                    cursor: None,
                }
            }
        };
        let joined = !self.store.rooms[room].members.contains_key(agent);
        let role = member.role;
        self.commit(Effect::Member {
            room: room.to_string(),
            agent: agent.to_string(),
            member: Some(member),
        })?;
        let scope = self.room_scope(room);
        self.emit(
            "member",
            scope,
            json!({ "room": room, "agent": agent, "joined": true, "role": role }),
            now,
        );
        let body = match (joined, agent == actor.name, role) {
            (true, true, _) => "joined".to_string(),
            (true, false, _) => format!("added {agent}"),
            (false, _, Role::Owner) => format!("made {agent} an owner"),
            (false, _, Role::Member) => format!("made {agent} a member"),
        };
        self.log(room, &actor.name, body, now)?;
        Ok(true)
    }

    pub fn leave_room(&mut self, room: &str, agent: &str, actor: &Actor, now: u64) -> Result<()> {
        let record = self.visible_room(room, actor)?;
        if agent != actor.name {
            Core::require_owner(record, actor)?;
        }
        if !record.members.contains_key(agent) {
            return Err(Error::NotFound(format!(
                "{agent:?} is not a member of room {room:?}"
            )));
        }
        let open = record.is_open();
        let scope = self.room_scope(room);
        self.commit(Effect::Member {
            room: room.to_string(),
            agent: agent.to_string(),
            member: None,
        })?;
        self.emit(
            "member",
            scope,
            json!({ "room": room, "agent": agent, "joined": false }),
            now,
        );
        if open {
            let body = if agent == actor.name {
                "left".to_string()
            } else {
                format!("removed {agent}")
            };
            self.log(room, &actor.name, body, now)?;
        }
        Ok(())
    }

    /// Changes the topic, or closes or reopens the room. Closing withdraws open
    /// questions so agents waiting on them are released.
    pub fn update_room(
        &mut self,
        room: &str,
        topic: Option<String>,
        closed: Option<bool>,
        actor: &Actor,
        now: u64,
    ) -> Result<()> {
        let record = self.visible_room(room, actor)?;
        Core::require_owner(record, actor)?;
        if closed == Some(false) && !record.is_open() {
            self.commit(Effect::Close {
                room: room.to_string(),
                at: None,
            })?;
            self.log(room, &actor.name, "reopened the room".into(), now)?;
        }
        if let Some(topic) = topic
            && topic != self.store.rooms[room].topic
        {
            self.open_room(room)?;
            self.commit(Effect::Topic {
                room: room.to_string(),
                topic: topic.clone(),
            })?;
            let body = if topic.is_empty() {
                "cleared the topic".to_string()
            } else {
                format!("set the topic: {topic}")
            };
            self.log(room, &actor.name, body, now)?;
        }
        if closed == Some(true) && self.store.rooms[room].is_open() {
            self.log(room, &actor.name, "closed the room".into(), now)?;
            let open: Vec<Id> = self.store.rooms[room].open.iter().copied().collect();
            for id in open {
                self.set_question(room, id, Status::Withdrawn, now)?;
            }
            self.commit(Effect::Close {
                room: room.to_string(),
                at: Some(now),
            })?;
        }
        let scope = self.room_scope(room);
        self.emit(
            "room",
            scope,
            json!({ "room": room, "change": "updated" }),
            now,
        );
        Ok(())
    }

    pub fn delete_room(&mut self, room: &str, actor: &Actor, now: u64) -> Result<()> {
        Core::require_owner(self.visible_room(room, actor)?, actor)?;
        self.drop_room(room, now)
    }

    /// Posts a note, or a question when `ask` is set. Posting to a public room
    /// joins it; addressing a question to someone brings them into the room.
    pub fn post(&mut self, req: PostRequest, actor: &Actor, now: u64) -> Result<(Post, bool)> {
        self.visible_room(&req.room, actor)?;
        self.room_to_grow()?;
        let record = self.open_room(&req.room)?;
        if let Some(key) = &req.key
            && let Some(existing) = self.store.post_by_key(&req.room, &actor.name, key)
        {
            return Ok((existing.clone(), false));
        }
        let parent = match req.reply_to {
            Some(id) => Some(record.posts.get(&id).ok_or_else(|| {
                Error::NotFound(format!("post {id} is not in room {:?}", req.room))
            })?),
            None => None,
        };
        let about = req
            .about
            .clone()
            .or_else(|| parent.and_then(|p| p.about.clone()));
        let ask = match &req.ask {
            Some(ask) => {
                let mut to: Vec<String> = Vec::new();
                for name in &ask.to {
                    if name == &actor.name {
                        return Err(Error::BadRequest(
                            "a question cannot be addressed to its asker".into(),
                        ));
                    }
                    if !to.contains(name) {
                        to.push(name.clone());
                    }
                }
                let mut options: Vec<String> = Vec::new();
                for option in &ask.options {
                    let option = option.trim();
                    if option.is_empty() || option.chars().count() > 200 {
                        return Err(Error::BadRequest(
                            "options must be 1 to 200 characters".into(),
                        ));
                    }
                    if !options.iter().any(|o| o.eq_ignore_ascii_case(option)) {
                        options.push(option.to_string());
                    }
                }
                if to.len() > MAX_ADDRESSEES || options.len() > MAX_OPTIONS {
                    return Err(Error::BadRequest(format!(
                        "a question names at most {MAX_ADDRESSEES} members and {MAX_OPTIONS} options"
                    )));
                }
                let ttl = ask
                    .ttl_ms
                    .unwrap_or(self.limits.msg_ttl_ms)
                    .min(self.limits.msg_ttl_ms);
                Some(Ask {
                    to,
                    options,
                    expires: now + ttl,
                    status: Status::Open,
                    closed: None,
                })
            }
            None => None,
        };
        if let Some(ask) = &ask {
            self.admit_question(record, ask, actor)?;
        }
        // Posting joins a public room, and a question brings in whoever has
        // to answer it; in a private room only owners get this far with
        // outsiders named.
        let joining: Vec<&str> = std::iter::once(actor.name.as_str())
            .chain(ask.iter().flat_map(|a| a.to.iter().map(String::as_str)))
            .filter(|name| !record.members.contains_key(*name))
            .collect();
        if record.members.len() + joining.len() > MAX_MEMBERS {
            return Err(Error::Full(format!(
                "room {:?} is full ({MAX_MEMBERS} members)",
                req.room
            )));
        }
        self.ensure_agents(&joining, now)?;
        let inviter = Actor {
            name: actor.name.clone(),
            admin: true,
            strict: false,
        };
        for name in joining {
            let by = if name == actor.name { actor } else { &inviter };
            self.join(&req.room, name, None, by, now)?;
        }
        let post = Post {
            id: self.store.next_id(now),
            from: actor.name.clone(),
            kind: if ask.is_some() {
                Kind::Question
            } else {
                Kind::Note
            },
            body: req.body,
            created: now,
            reply_to: req.reply_to,
            about,
            choice: None,
            ask,
            key: req.key,
        };
        self.append(&req.room, post.clone(), now)?;
        Ok((post, true))
    }

    /// Checks a new question before anything changes: whom it names, and how
    /// many questions the room and the asker have open.
    fn admit_question(&self, room: &Room, ask: &Ask, actor: &Actor) -> Result<()> {
        if room.private
            && !Core::manages(room, actor)
            && let Some(outsider) = ask.to.iter().find(|n| !room.members.contains_key(*n))
        {
            return Err(Error::Forbidden(format!(
                "{outsider} is not a member of private room {:?}; only owners can add agents",
                room.name
            )));
        }
        if actor.strict {
            for name in &ask.to {
                self.require_known(name)?;
            }
        }
        let (mut open, mut own) = (0, 0);
        for post in room.open_questions() {
            open += 1;
            own += usize::from(post.from == actor.name);
        }
        if open >= MAX_OPEN_QUESTIONS {
            return Err(Error::Full(format!(
                "room {:?} has {MAX_OPEN_QUESTIONS} open questions; answer or withdraw some first",
                room.name
            )));
        }
        let quota = share(MAX_OPEN_QUESTIONS);
        if actor.strict && own >= quota {
            return Err(Error::Full(format!(
                "{} has {own} open questions in room {:?}, a quarter of what it holds; withdraw one first",
                actor.name, room.name
            )));
        }
        Ok(())
    }

    /// Answers an open question. It resolves when every addressee has answered,
    /// or on the first answer when it names nobody.
    pub fn answer(&mut self, req: AnswerRequest, actor: &Actor, now: u64) -> Result<(Post, bool)> {
        self.room_to_grow()?;
        let AnswerRequest {
            room,
            question,
            body,
            choice,
            key,
        } = req;
        let room = room.as_str();
        self.visible_room(room, actor)?;
        let record = self.open_room(room)?;
        if let Some(key) = &key
            && let Some(existing) = self.store.post_by_key(room, &actor.name, key)
        {
            return Ok((existing.clone(), false));
        }
        let asked = record
            .posts
            .get(&question)
            .filter(|p| p.kind == Kind::Question)
            .ok_or_else(|| {
                Error::NotFound(format!("question {question} is not in room {room:?}"))
            })?;
        let Some(ask) = &asked.ask else {
            return Err(Error::NotFound(format!("question {question} has no ask")));
        };
        if ask.status != Status::Open {
            return Err(Error::Conflict(format!(
                "question {question} is already {}",
                json!(ask.status).as_str().unwrap_or("closed")
            )));
        }
        if asked.from == actor.name {
            return Err(Error::Conflict(
                "you cannot answer your own question; withdraw it instead".into(),
            ));
        }
        if !ask.to.is_empty() && !ask.to.contains(&actor.name) {
            return Err(Error::Forbidden(format!(
                "question {question} is addressed to {}",
                ask.to.join(", ")
            )));
        }
        if record.answers(question).any(|a| a.from == actor.name) {
            return Err(Error::Conflict(format!(
                "you already answered question {question}"
            )));
        }
        let choice = match (ask.options.as_slice(), choice) {
            ([], choice) => choice,
            (options, Some(choice)) => Some(
                options
                    .iter()
                    .find(|o| o.eq_ignore_ascii_case(choice.trim()))
                    .cloned()
                    .ok_or_else(|| {
                        Error::BadRequest(format!("choice must be one of: {}", options.join(", ")))
                    })?,
            ),
            (options, None) => {
                return Err(Error::BadRequest(format!(
                    "pick a choice: {}",
                    options.join(", ")
                )));
            }
        };
        let body = if body.is_empty() {
            choice.clone().unwrap_or_default()
        } else {
            body
        };
        if body.is_empty() {
            return Err(Error::BadRequest("\"body\" is required".into()));
        }
        let about = asked.about.clone();
        if !record.members.contains_key(&actor.name) {
            self.join(room, &actor.name, None, actor, now)?;
        }
        let post = Post {
            id: self.store.next_id(now),
            from: actor.name.clone(),
            kind: Kind::Answer,
            body,
            created: now,
            reply_to: Some(question),
            about,
            choice,
            ask: None,
            key,
        };
        self.append(room, post.clone(), now)?;
        let record = &self.store.rooms[room];
        let asked = &record.posts[&question];
        let resolved = asked.ask.as_ref().is_some_and(|a| a.to.is_empty())
            || record.waiting_on(asked).is_empty();
        if resolved {
            self.set_question(room, question, Status::Resolved, now)?;
        }
        Ok((post, resolved))
    }

    pub fn withdraw(&mut self, room: &str, question: Id, actor: &Actor, now: u64) -> Result<()> {
        let record = self.visible_room(room, actor)?;
        let asked = record
            .posts
            .get(&question)
            .filter(|p| p.kind == Kind::Question)
            .ok_or_else(|| {
                Error::NotFound(format!("question {question} is not in room {room:?}"))
            })?;
        if asked.from != actor.name {
            Core::require_owner(record, actor)?;
        }
        if !asked.is_open_question() {
            return Err(Error::Conflict(format!(
                "question {question} is no longer open"
            )));
        }
        self.set_question(room, question, Status::Withdrawn, now)
    }

    pub fn question(&self, room: &str, id: Id) -> Result<Value> {
        let record = self.room(room)?;
        record
            .posts
            .get(&id)
            .filter(|p| p.kind == Kind::Question)
            .map(|p| post_json(record, p))
            .ok_or_else(|| Error::NotFound(format!("question {id} is not in room {room:?}")))
    }

    /// Unread posts for a member, oldest first, moving its cursor past what was
    /// returned. The member's own posts are skipped but count as read. Returns
    /// the posts and how many unread posts remain.
    pub fn read_room(
        &mut self,
        room: &str,
        agent: &str,
        limit: usize,
    ) -> Result<(Vec<Value>, usize)> {
        let record = self.room(room)?;
        let member = record.members.get(agent).ok_or_else(|| {
            Error::Forbidden(format!(
                "{agent:?} is not a member of room {room:?}; join it first"
            ))
        })?;
        let range = match member.cursor {
            Some(cursor) => record.posts.range((Excluded(cursor), Unbounded)),
            None => record.posts.range(..),
        };
        let mut items = Vec::new();
        let mut last = None;
        let mut remaining = 0;
        for (id, post) in range {
            if items.len() == limit {
                remaining += usize::from(post.from != agent);
                continue;
            }
            last = Some(*id);
            if post.from != agent {
                items.push(post_json(record, post));
            }
        }
        if let Some(id) = last {
            self.commit(Effect::RoomCursor {
                room: room.to_string(),
                agent: agent.to_string(),
                id,
            })?;
        }
        Ok((items, remaining))
    }

    /// A newest-last page of a room's timeline, scanning back from `before`.
    pub fn room_posts(&self, room: &str, query: &PostQuery) -> Result<(Vec<Value>, Option<Id>)> {
        let record = self.room(room)?;
        let range = match query.before {
            Some(before) => record.posts.range(..before),
            None => record.posts.range(..),
        };
        let mut items: Vec<&Post> = range
            .rev()
            .map(|(_, p)| p)
            .filter(|p| {
                query
                    .about
                    .as_ref()
                    .is_none_or(|a| p.about.as_ref() == Some(a))
            })
            .filter(|p| query.kind.is_none_or(|k| p.kind == k))
            .take(query.limit + 1)
            .collect();
        let next = if items.len() > query.limit {
            items.truncate(query.limit);
            items.last().map(|p| p.id)
        } else {
            None
        };
        items.reverse();
        Ok((
            items.into_iter().map(|p| post_json(record, p)).collect(),
            next,
        ))
    }

    pub fn board_set(
        &mut self,
        room: &str,
        key: &str,
        value: Option<String>,
        expect: Option<u64>,
        actor: &Actor,
        now: u64,
    ) -> Result<Option<Entry>> {
        self.visible_room(room, actor)?;
        if value.is_some() {
            self.room_to_grow()?;
        }
        let record = self.open_room(room)?;
        if !actor.admin && !record.members.contains_key(&actor.name) {
            return Err(Error::Forbidden(format!(
                "join room {room:?} before writing its board"
            )));
        }
        let current = record.board.get(key);
        let version = current.map_or(0, |e| e.version);
        if let Some(expect) = expect
            && expect != version
        {
            return Err(Error::Conflict(format!(
                "board key {key:?} is at version {version}, not {expect}"
            )));
        }
        let (entry, body) = match value {
            None if current.is_none() => {
                return Err(Error::NotFound(format!(
                    "board key {key:?} is not set in room {room:?}"
                )));
            }
            None => (None, format!("removed {key} from the board")),
            Some(value) if current.is_some_and(|e| e.value == value) => {
                return Ok(current.cloned());
            }
            Some(_) if current.is_none() && record.board.len() >= MAX_BOARD_KEYS => {
                return Err(Error::Full(format!(
                    "the board of room {room:?} holds at most {MAX_BOARD_KEYS} keys"
                )));
            }
            Some(value) => (
                Some(Entry {
                    value,
                    by: actor.name.clone(),
                    at: now,
                    version: version + 1,
                }),
                format!("set {key} on the board (version {})", version + 1),
            ),
        };
        self.commit(Effect::Board {
            room: room.to_string(),
            key: key.to_string(),
            entry: entry.clone(),
        })?;
        let scope = self.room_scope(room);
        self.emit(
            "board",
            scope,
            json!({ "room": room, "key": key, "version": entry.as_ref().map(|e| e.version) }),
            now,
        );
        self.log(room, &actor.name, body, now)?;
        Ok(entry)
    }

    pub fn board_json(&self, room: &str) -> Result<Vec<Value>> {
        Ok(self
            .room(room)?
            .board
            .iter()
            .map(|(key, entry)| {
                json!({
                    "key": key,
                    "value": entry.value,
                    "by": entry.by,
                    "version": entry.version,
                    "updatedAt": iso8601(entry.at),
                })
            })
            .collect())
    }

    fn room_summary(&self, room: &Room, viewer: &str, online: &HashMap<&str, usize>) -> Value {
        let member = room.members.get(viewer);
        let last = room
            .posts
            .values()
            .next_back()
            .map_or(room.created, |p| p.created);
        json!({
            "name": room.name,
            "topic": room.topic,
            "private": room.private,
            "open": room.is_open(),
            "memberCount": room.members.len(),
            "onlineCount": room.members.keys().filter(|m| online.contains_key(m.as_str())).count(),
            "postCount": room.posts.len(),
            "openQuestions": room.open.len(),
            "member": member.is_some(),
            "role": member.map(|m| m.role),
            "unread": if member.is_some() { room.unread(viewer) } else { 0 },
            "waiting": room.waiting_for(viewer),
            "lastActivity": iso8601(last),
        })
    }

    /// Rooms the actor can see: all of them for administrators, otherwise
    /// public rooms and private rooms it belongs to.
    pub fn rooms_json(&self, actor: &Actor, now: u64) -> Vec<Value> {
        let online = self.store.online_counts(now);
        self.store
            .rooms
            .values()
            .filter(|r| actor.admin || !r.private || r.members.contains_key(&actor.name))
            .map(|r| self.room_summary(r, &actor.name, &online))
            .collect()
    }

    pub fn room_json(&self, room: &str, viewer: &str, now: u64) -> Result<Value> {
        let record = self.room(room)?;
        let online = self.store.online_counts(now);
        let mut value = self.room_summary(record, viewer, &online);
        value["createdAt"] = json!(iso8601(record.created));
        value["createdBy"] = json!(record.created_by);
        value["closedAt"] = json!(record.closed.map(iso8601));
        value["members"] = record
            .members
            .iter()
            .map(|(name, m)| {
                json!({
                    "name": name,
                    "role": m.role,
                    "online": online.contains_key(name.as_str()),
                    "unread": record.unread(name),
                    "joinedAt": iso8601(m.joined),
                })
            })
            .collect();
        value["board"] = json!(self.board_json(room)?);
        value["questions"] = record
            .open_questions()
            .map(|p| post_json(record, p))
            .collect();
        Ok(value)
    }

    /// Rooms where `agent` has something to read or answer.
    pub fn room_activity(&self, agent: &str) -> Vec<Value> {
        self.store
            .rooms
            .values()
            .filter(|r| r.members.contains_key(agent))
            .map(|r| (r, r.unread(agent), r.waiting_for(agent)))
            .filter(|(_, unread, waiting)| unread + waiting > 0)
            .map(|(r, unread, waiting)| json!({ "room": r.name, "unread": unread, "waiting": waiting }))
            .collect()
    }

    fn sweep_rooms(&mut self, now: u64) -> Result<()> {
        let expired: Vec<(String, Id)> = self
            .store
            .rooms
            .values()
            .flat_map(|r| {
                r.open_questions()
                    .filter(|p| p.ask.as_ref().is_some_and(|a| a.expires <= now))
                    .map(|p| (r.name.clone(), p.id))
            })
            .collect();
        for (room, id) in expired {
            self.set_question(&room, id, Status::Expired, now)?;
        }
        let ttl = self.limits.msg_ttl_ms;
        let stale: Vec<String> = self
            .store
            .rooms
            .values()
            .filter(|r| r.closed.is_some_and(|at| at + ttl <= now))
            .map(|r| r.name.clone())
            .collect();
        for room in stale {
            self.drop_room(&room, now)?;
        }
        Ok(())
    }

    // ---- keyspace and channels -------------------------------------------

    /// Runs one command without journaling it. Channel and server commands are
    /// answered here; everything else goes to the keyspace.
    fn run_command(
        &mut self,
        argv: &[String],
        access: &Access,
        blocking: bool,
        now: u64,
    ) -> Outcome {
        if let Err(denied) = access.permit(argv) {
            return Outcome::of(Reply::Error(denied));
        }
        if writes(argv) && self.halted {
            return Outcome::of(Reply::Error(HALTED.into()));
        }
        if grows(argv)
            && let Some(message) = self.over_memory()
        {
            return Outcome::of(Reply::Error(message));
        }
        if writes(argv)
            && let Some(why) = self.journal_failing()
        {
            return Outcome::of(Reply::Error(format!(
                "IOERR the journal cannot be written ({why}); changes are refused until it can"
            )));
        }
        let name = Name::of(argv);
        let wrong = |name: &str| {
            Outcome::of(Reply::Error(format!(
                "ERR wrong number of arguments for '{}' command",
                name.to_ascii_lowercase()
            )))
        };
        match name.as_str() {
            "PUBLISH" if argv.len() != 3 => wrong(name.as_str()),
            "PUBLISH" if argv[1].len() > MAX_CHANNEL => Outcome::of(Reply::Error(format!(
                "ERR channel names are at most {MAX_CHANNEL} bytes long"
            ))),
            "PUBLISH" => Outcome::of(Reply::Int(self.publish(&argv[1], &argv[2]) as i64)),
            "PUBSUB" => Outcome::of(self.pubsub(argv, &access.scope)),
            "INFO" => Outcome::of(self.info(access, now)),
            _ => {
                let ctx = Ctx {
                    now,
                    replay: false,
                    blocking,
                    access: access.clone(),
                    max_keys: self.limits.max_keys,
                    max_elements: self.limits.max_elements,
                };
                let out = self.store.db.exec(argv, &ctx);
                if out.dirty {
                    // Only a write that touches a key can give a blocked
                    // command what it waits for. A flush touches them all.
                    if matches!(name.as_str(), "FLUSHALL" | "FLUSHDB") {
                        let keys = self.waiters.by_topic.keys();
                        let keys = keys.filter(|t| matches!(t, Topic::Key(_))).cloned();
                        self.wake.extend(keys.collect::<Vec<_>>());
                    }
                    if !self.waiters.by_topic.is_empty() {
                        self.wake
                            .extend(out.events.iter().map(|(key, _)| Topic::Key(key.clone())));
                    }
                    self.notify(&out.events);
                }
                out
            }
        }
    }

    /// Journals keyspace changes already made. When the journal cannot take
    /// them yet, the error hands back what the commands returned.
    fn journal_commands(
        &mut self,
        at: u64,
        commands: &[&[String]],
        reply: impl FnOnce() -> Reply,
    ) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }
        let Some(journal) = self.journal.as_mut() else {
            return Ok(());
        };
        let journaled = if BATCHING.get() && journal.defers() {
            self.deferred.store(true, Ordering::Release);
            journal.defer_commands(at, commands)
        } else {
            journal.append_commands(at, commands)
        };
        if let Err(e) = journaled {
            return Err(Error::Unsaved(
                format!(
                    "IOERR journal write failed: {e}; the change is made, and reaches the disk once it can be written"
                ),
                Box::new(reply()),
            ));
        }
        Ok(())
    }

    /// Runs a command and journals it if it changed the keyspace.
    pub fn command(
        &mut self,
        argv: &[String],
        access: &Access,
        blocking: bool,
        now: u64,
    ) -> Result<Outcome> {
        let mut out = self.run_command(argv, access, blocking, now);
        cap_reply(&mut out, 0);
        let commands = out.journal(argv);
        self.journal_commands(now, &commands, || out.reply.clone())?;
        Ok(out)
    }

    /// Runs commands back to back with nothing in between, journaled as one
    /// entry so a crash keeps all of them or none. Blocking commands do not
    /// wait inside a batch. A batch that names anything out of the caller's
    /// reach is refused before any of it runs.
    pub fn multi(
        &mut self,
        batch: &[Vec<String>],
        access: &Access,
        now: u64,
    ) -> Result<Vec<Reply>> {
        for (i, argv) in batch.iter().enumerate() {
            access.permit(argv).map_err(|e| {
                let (code, why) = e.split_once(' ').unwrap_or(("ERR", &e));
                Error::Forbidden(format!("{code} command {} of the batch: {why}", i + 1))
            })?;
        }
        let mut replies = Vec::with_capacity(batch.len());
        let mut journal = Vec::new();
        let mut sent = 0;
        for argv in batch {
            let mut out = self.run_command(argv, access, false, now);
            sent += cap_reply(&mut out, sent);
            journal.extend(out.journal(argv).into_iter().map(<[String]>::to_vec));
            replies.push(out.reply);
        }
        let journal: Vec<&[String]> = journal.iter().map(Vec::as_slice).collect();
        self.journal_commands(now, &journal, || Reply::Array(replies.clone()))?;
        Ok(replies)
    }

    /// Subscribes to channels and patterns. A token with a scope holds at
    /// most [`MAX_PATTERNS_PER_SCOPE`] patterns across its subscriptions:
    /// every key change is matched against every pattern, so they bound what
    /// one agent's writes cost everyone.
    pub fn subscribe_channels(
        &mut self,
        channels: Vec<String>,
        patterns: Vec<String>,
        scope: Arc<Scope>,
    ) -> Result<Deliveries> {
        if !scope.is_all() && !patterns.is_empty() {
            let held: usize = self
                .live_channels()
                .filter(|s| Arc::ptr_eq(&s.scope, &scope))
                .map(|s| s.patterns.len())
                .sum();
            if held + patterns.len() > MAX_PATTERNS_PER_SCOPE {
                return Err(Error::Full(format!(
                    "this token already listens with {held} patterns; it can hold at most {MAX_PATTERNS_PER_SCOPE}"
                )));
            }
        }
        // Those that left go first, though nothing published has come to
        // find them gone: subscribers that come and go cannot pile up.
        self.channels.retain(|s| s.alive.strong_count() > 0);
        let (tx, rx) = sync_channel(SUBSCRIBER_BUFFER);
        let waiting = Arc::new(AtomicUsize::new(0));
        let alive = Arc::new(());
        let patterns: Vec<Glob> = patterns.iter().map(|p| Glob::new(p)).collect();
        self.channels.push(Subscription {
            keys: hears_keys(&channels, &patterns),
            channels,
            patterns,
            scope,
            tx,
            waiting: Arc::clone(&waiting),
            alive: Arc::downgrade(&alive),
        });
        Ok(Deliveries {
            rx,
            waiting,
            _alive: alive,
        })
    }

    /// Delivers to every matching subscription and returns how many received
    /// it. A subscriber too slow to keep up is dropped, as pub/sub never
    /// queues: one with [`SUBSCRIBER_BUFFER`] messages or [`SUBSCRIBER_BYTES`]
    /// waiting. The message is copied once, however many it reaches.
    pub fn publish(&mut self, channel: &str, message: &str) -> usize {
        let mut receivers = 0;
        let mut shared: Option<(Arc<str>, Arc<str>)> = None;
        let key_change = channel.starts_with("__key");
        self.channels.retain(|sub| {
            if sub.alive.strong_count() == 0 {
                return false;
            }
            if (key_change && !sub.keys) || !sub.scope.can_receive(channel, message) {
                return true;
            }
            let direct = sub.channels.iter().any(|c| c == channel).then_some(None);
            let patterns = sub
                .patterns
                .iter()
                .filter(|p| p.matches(channel))
                .map(|p| Some(p.as_str().to_string()));
            for pattern in direct.into_iter().chain(patterns) {
                let (shared_channel, shared_message) =
                    shared.get_or_insert_with(|| (channel.into(), message.into()));
                let delivery = Delivery {
                    channel: Arc::clone(shared_channel),
                    pattern,
                    message: Arc::clone(shared_message),
                };
                let (size, waiting) = (delivery.size(), sub.waiting.load(Ordering::Relaxed));
                if waiting > 0 && waiting + size > SUBSCRIBER_BYTES {
                    return false;
                }
                sub.waiting.fetch_add(size, Ordering::Relaxed);
                if sub.tx.try_send(Arc::new(delivery)).is_err() {
                    return false;
                }
                receivers += 1;
            }
            true
        });
        receivers
    }

    fn live_channels(&self) -> impl Iterator<Item = &Subscription> {
        self.channels.iter().filter(|s| s.alive.strong_count() > 0)
    }

    fn pubsub(&self, argv: &[String], scope: &Scope) -> Reply {
        let sub = argv
            .get(1)
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default();
        match sub.as_str() {
            "CHANNELS" => {
                let pattern = match Glob::client(argv.get(2).map_or("*", String::as_str)) {
                    Ok(pattern) => pattern,
                    Err(e) => return Reply::Error(e),
                };
                let names: std::collections::BTreeSet<&str> = self
                    .live_channels()
                    .flat_map(|s| s.channels.iter().map(String::as_str))
                    .filter(|c| pattern.matches(c) && scope.can_use_channel(c))
                    .collect();
                Reply::Array(names.into_iter().map(|c| Reply::Bulk(c.into())).collect())
            }
            "NUMSUB" => Reply::Array(
                argv[2..]
                    .iter()
                    .flat_map(|channel| {
                        let n = self
                            .live_channels()
                            .filter(|s| s.channels.contains(channel))
                            .count();
                        [Reply::Bulk(channel.clone()), Reply::Int(n as i64)]
                    })
                    .collect(),
            ),
            // A pattern can match any channel, so a scoped client counts only
            // the patterns of clients with its own reach.
            "NUMPAT" => Reply::Int(
                self.live_channels()
                    .filter(|s| scope.is_all() || *s.scope == *scope)
                    .map(|s| s.patterns.len())
                    .sum::<usize>() as i64,
            ),
            _ => Reply::Error(
                "ERR PUBSUB takes CHANNELS [pattern], NUMSUB [channel ...], or NUMPAT".into(),
            ),
        }
    }

    /// Publishes key changes to `__keyspace__:<key>` (the event name) and
    /// `__keyevent__:<event>` (the key), only when someone listens for them.
    fn notify(&mut self, events: &[(String, &'static str)]) {
        if !self.live_channels().any(|s| s.keys) {
            return;
        }
        for (key, event) in events {
            self.publish(&format!("__keyspace__:{key}"), event);
            self.publish(&format!("__keyevent__:{event}"), key);
        }
    }

    /// Server facts. Callers with a scope count only the keys they reach and
    /// see nothing of other agents.
    fn info(&self, access: &Access, now: u64) -> Reply {
        let db = &self.store.db;
        let (mut keys, mut expiring, mut bytes, mut streams) = (0usize, 0usize, 0usize, 0usize);
        for (key, entry) in visible_keys(db, &access.scope, "", None, now) {
            keys += 1;
            expiring += usize::from(entry.expires.is_some());
            bytes += key.len() + entry.value.footprint();
            streams += usize::from(entry.value.type_name() == "stream");
        }
        let int = |n: usize| Reply::Int(n as i64);
        let mut fields = vec![
            ("version".into(), Reply::Bulk(VERSION.into())),
            ("keys".into(), int(keys)),
            ("expiring_keys".into(), int(expiring)),
            ("streams".into(), int(streams)),
            ("approximate_bytes".into(), int(bytes)),
            ("max_keys".into(), int(self.limits.max_keys)),
        ];
        if access.admin {
            fields.extend([
                ("used_memory".into(), int(memory::in_use())),
                ("max_memory".into(), int(self.limits.max_memory)),
                (
                    "channel_subscriptions".into(),
                    int(self.live_channels().count()),
                ),
                ("agents".into(), int(self.store.agents.len())),
                ("rooms".into(), int(self.store.rooms.len())),
                (
                    "persistence".into(),
                    Reply::Bulk(
                        if self.journal.is_some() {
                            "journal"
                        } else {
                            "memory"
                        }
                        .into(),
                    ),
                ),
            ]);
        }
        Reply::Map(fields)
    }

    // ---- maintenance -----------------------------------------------------

    /// The active expire cycle: presence, leases, message TTLs, and retention.
    /// Returns whether more fell due than one sweep settles.
    pub fn sweep(&mut self, now: u64) -> Result<bool> {
        let expired: Vec<(String, String)> = self
            .store
            .sessions
            .iter()
            .filter(|(_, s)| s.expires <= now)
            .map(|(id, s)| (id.clone(), s.agent.clone()))
            .collect();
        for (session, agent) in expired {
            self.store.sessions.remove(&session);
            if !self.is_online(&agent, now) {
                self.emit(
                    "presence",
                    vec![agent.clone()],
                    json!({ "agent": agent, "online": false }),
                    now,
                );
            }
        }
        let more = self.sweep_due(now)?;
        let old: Vec<Id> = self
            .store
            .broadcasts
            .values()
            .filter(|b| b.expires <= now)
            .map(|b| b.id)
            .collect();
        if !old.is_empty() {
            self.commit(Effect::DropBroadcasts { ids: old })?;
        }
        self.sweep_rooms(now)?;
        Ok(more)
    }

    /// Settles a batch of each kind of what fell due: leases that ran out,
    /// messages that expired or were kept long enough once settled, and keys
    /// that expired. Returns whether more are due.
    pub fn sweep_due(&mut self, now: u64) -> Result<bool> {
        let leases = self.reclaim(now)?;
        let expired = self.expire(now)?;
        let kept_until = now.saturating_sub(self.limits.msg_ttl_ms);
        let stale: Vec<Id> = self
            .store
            .settled_before(kept_until)
            .take(SWEEP_BATCH)
            .collect();
        if !stale.is_empty() {
            self.commit(Effect::Drop { ids: stale })?;
        }
        let settled = self.store.settled_before(kept_until).next().is_some();
        let keys = self.expire_keys(now);
        Ok(leases || expired || settled || keys)
    }

    /// Removes a batch of expired keys. Returns whether more are due.
    fn expire_keys(&mut self, now: u64) -> bool {
        let expired: Vec<(String, &'static str)> = self
            .store
            .db
            .expire_keys(now)
            .into_iter()
            .map(|key| (key, "expired"))
            .collect();
        self.notify(&expired);
        // A request blocked on a stream that expires learns it is gone.
        self.wake
            .extend(expired.into_iter().map(|(key, _)| Topic::Key(key)));
        self.store.db.expiry_due(now)
    }

    pub fn stats(&self, now: u64) -> Value {
        let online = self.store.online_counts(now);
        let mut totals = Counts::default();
        for name in self.store.agents.keys() {
            let counts = self.store.counts(name);
            totals.queued += counts.queued;
            totals.leased += counts.leased;
            totals.dead += counts.dead;
            totals.settled += counts.settled;
        }
        json!({
            "agentCount": self.store.agents.len(),
            "onlineCount": online.len(),
            "messageCount": totals.pending(),
            "queuedCount": totals.queued,
            "leasedCount": totals.leased,
            "deadCount": totals.dead,
            "settledCount": totals.settled,
            "broadcastCount": self.store.broadcasts.len(),
            "roomCount": self.store.rooms.len(),
            "keyCount": self.store.db.entries().len(),
        })
    }

    pub fn live_session_count(&self, now: u64) -> usize {
        self.store
            .sessions
            .values()
            .filter(|s| s.expires > now)
            .count()
    }

    /// Starts a snapshot when the journal has grown enough. Only serializing
    /// the state happens here, under the lock; the disk work runs on its own thread.
    /// Starts a rewrite once the journal has grown enough, returning the
    /// snapshot it began for the broker to carry on.
    fn after_operation(&mut self) -> Option<Snapshot> {
        let journal = self.journal.as_mut()?;
        // A snapshot would keep what a failed request left half made.
        if self.halted || !journal.needs_rewrite() {
            return None;
        }
        let started = Instant::now();
        match journal.start_rewrite(&mut self.store) {
            Ok(snapshot) => {
                let paused = started.elapsed();
                if paused >= Duration::from_millis(50) {
                    eprintln!(
                        "postino: starting a snapshot took {} ms; requests waited meanwhile",
                        paused.as_millis()
                    );
                }
                Some(snapshot)
            }
            Err(e) => {
                eprintln!("postino: snapshot rewrite failed: {e}");
                None
            }
        }
    }

    fn finish_rewrite(&mut self, snapshot: &mut Snapshot) -> std::io::Result<()> {
        let journal = self
            .journal
            .as_mut()
            .ok_or_else(|| std::io::Error::other("no journal"))?;
        journal.finish_rewrite(&mut self.store, snapshot)
    }
}

pub struct Broker {
    core: Mutex<Core>,
    /// The core's flag for changes journaled but not written yet, readable
    /// without its lock.
    deferred: Arc<AtomicBool>,
    /// The journal's tail, to write changes after releasing the lock.
    writer: Option<Writer>,
    /// Itself, for the threads it starts.
    this: Weak<Broker>,
    pub instance: String,
    pub started: u64,
    pub storage: String,
}

/// Attempts at the broker's lock before sleeping on it, each after twice
/// as long a pause as the one before: about a microsecond in all.
const SPIN_ROUNDS: u32 = 8;

thread_local! {
    /// Whether this thread's keyspace changes wait to be journaled together.
    static BATCHING: Cell<bool> = const { Cell::new(false) };
}

/// Makes the changes this thread makes wait in the journal, to be
/// written together by [`Broker::flush_journal`], which the thread must call
/// before it sends a reply. A thread that serves many connections answers
/// all the requests it has read, then writes the journal once, then the
/// replies: one write for many changes, and no reply before the changes it
/// reflects are in the journal, except while a sync runs in the background
/// (see [`Journal::flush_soon`]). Other threads write what waits before they
/// reply, and so does a rewrite before it sets the journal aside.
pub fn batch_journal_writes() {
    BATCHING.set(true);
}

thread_local! {
    /// Whether requests on this thread may wait. A thread that serves many
    /// connections must not: it runs requests [without
    /// waiting](without_waiting), and learns which would have.
    static PATIENCE: Cell<Patience> = const { Cell::new(Patience::Wait) };
}

#[derive(Clone, Copy, PartialEq)]
enum Patience {
    Wait,
    Refuse,
    Refused,
}

/// Runs `op` without letting anything in it wait. A request that would wait
/// is handed nothing instead, and `None` comes back so that it can be run
/// again on a thread that may wait: its first attempt found nothing, so it
/// changed nothing.
pub fn without_waiting<R>(op: impl FnOnce() -> R) -> Option<R> {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            PATIENCE.set(Patience::Wait);
        }
    }
    let _reset = Reset;
    PATIENCE.set(Patience::Refuse);
    let result = op();
    (PATIENCE.get() != Patience::Refused).then_some(result)
}

/// Whether a request that is about to wait must not; if so, it is recorded
/// that it would have.
fn refuse_to_wait() -> bool {
    let refuse = PATIENCE.get() != Patience::Wait;
    if refuse {
        PATIENCE.set(Patience::Refused);
    }
    refuse
}

impl Broker {
    pub fn open(limits: Limits, storage: Storage) -> std::result::Result<Arc<Broker>, String> {
        let (core, description) = match storage {
            Storage::Memory => (
                Core::new(Store::default(), limits, None),
                "memory".to_string(),
            ),
            Storage::Disk { dir, fsync } => {
                let (journal, store) = Journal::open(&dir, fsync)?;
                let description = journal.dir().display().to_string();
                (Core::new(store, limits, Some(journal)), description)
            }
        };
        let deferred = Arc::clone(&core.deferred);
        let writer = core.journal.as_ref().map(Journal::writer);
        Ok(Arc::new_cyclic(|this| Broker {
            deferred,
            writer,
            core: Mutex::new(core),
            this: this.clone(),
            instance: random_hex(8),
            started: now_ms(),
            storage: description,
        }))
    }

    /// The broker's lock. Each request holds it for microseconds, so it is
    /// tried a few times before sleeping on it: a lock about to be released
    /// is cheaper to wait for awake than to be woken for.
    ///
    /// A thread that panics while it holds the lock may leave what it was
    /// changing half changed, in memory and unlike the journal. The broker
    /// then halts: it takes no change from then on, nor a snapshot of memory,
    /// so the journal keeps the last whole changes for a restart to restore.
    /// Reads go on, for what they are worth.
    pub fn lock(&self) -> MutexGuard<'_, Core> {
        for round in 0..SPIN_ROUNDS {
            match self.core.try_lock() {
                Ok(core) => return core,
                Err(TryLockError::Poisoned(poisoned)) => return self.halt(poisoned.into_inner()),
                Err(TryLockError::WouldBlock) => {
                    for _ in 0..1 << round {
                        std::hint::spin_loop();
                    }
                }
            }
        }
        self.core
            .lock()
            .unwrap_or_else(|poisoned| self.halt(poisoned.into_inner()))
    }

    /// The data, from a lock that a thread panicked holding: the broker halts
    /// (see [`Broker::lock`]).
    fn halt<'a>(&'a self, mut core: MutexGuard<'a, Core>) -> MutexGuard<'a, Core> {
        self.core.clear_poison();
        if !core.halted {
            core.halted = true;
            eprintln!("postino: {}", &HALTED["READONLY ".len()..]);
        }
        core
    }

    /// Whether changes are refused because a request failed partway through
    /// one (see [`Broker::lock`]).
    pub fn halted(&self) -> bool {
        self.lock().halted
    }

    /// Writes the changes waiting in the journal, in one write, once the
    /// lock is released, so requests go on meanwhile; changes another thread
    /// took to write are written first. On failure they wait on, and writes
    /// are refused until the disk takes them. While a sync runs in the
    /// background they wait for it to end, for the next flush or tick after
    /// it to write.
    pub fn flush_journal(&self) {
        let Some(writer) = &self.writer else {
            return;
        };
        if !self.deferred.load(Ordering::Acquire) {
            writer.wait();
            return;
        }
        // A write on its way goes before these changes, so it is waited for
        // here rather than with the broker's lock held, which would hold up
        // every request.
        writer.wait();
        let mut core = self.lock();
        let Some(pending) = core.journal.as_mut().and_then(Journal::take_pending) else {
            self.flush_deferred(&mut core);
            return;
        };
        // Locked before the broker's lock is released, so that changes taken
        // after these are not written before them. A thread that finds the
        // flag down waits for the tail, its changes being among these.
        let mut tail = writer.lock();
        self.deferred.store(false, Ordering::Release);
        drop(core);
        if tail.write(pending).is_err() {
            self.deferred.store(true, Ordering::Release);
        }
    }

    fn flush_deferred(&self, core: &mut Core) {
        if let Some(journal) = core.journal.as_mut()
            && (journal.flush_soon().is_err() || journal.waiting())
        {
            return;
        }
        self.deferred.store(false, Ordering::Release);
    }

    fn settle(&self, core: &mut Core) {
        // A thread that does not batch replies as soon as it is done: what
        // others batched, which its reply may reflect, is written first.
        if !BATCHING.get() {
            if self.deferred.load(Ordering::Acquire) {
                self.flush_deferred(core);
            } else if let Some(writer) = &self.writer {
                writer.wait();
            }
        }
        let changed = std::mem::take(&mut core.wake);
        core.waiters.wake(&changed);
        if let Some(snapshot) = core.after_operation() {
            self.take_snapshot(core, snapshot);
        }
    }

    /// Takes the snapshot a rewrite started on a thread of its own: a run of
    /// keys at a time under the lock, written to disk without it, so requests
    /// go on meanwhile.
    fn take_snapshot(&self, core: &mut Core, snapshot: Snapshot) {
        let Some(broker) = self.this.upgrade() else {
            return Broker::finish_snapshot(core, snapshot);
        };
        // Handed over once the thread runs, so a thread that cannot start
        // leaves the snapshot to be finished here.
        let (tx, rx) = sync_channel::<Snapshot>(1);
        let spawned = std::thread::Builder::new()
            .name("postino-snapshot".into())
            .spawn(move || {
                let _flush = crate::memory::FlushOnDrop;
                let Ok(mut snapshot) = rx.recv() else { return };
                loop {
                    let mut core = broker.lock();
                    // Shutdown takes over a snapshot still being taken.
                    if core.store.db.capture_number() != Some(snapshot.capture) {
                        drop(core);
                        return snapshot.abandon();
                    }
                    if core.halted {
                        core.store.abandon_capture();
                        drop(core);
                        return snapshot.abandon();
                    }
                    let done = core.store.capture_step(SNAPSHOT_RUN);
                    let finished = match core.store.drain_capture() {
                        Ok(parts) => {
                            snapshot.add(parts);
                            if done {
                                core.finish_rewrite(&mut snapshot)
                            } else {
                                Ok(())
                            }
                        }
                        Err(e) => Err(std::io::Error::other(e)),
                    };
                    drop(core);
                    if let Err(e) = finished.and_then(|()| snapshot.flush()) {
                        eprintln!(
                            "postino: snapshot rewrite failed: {e}; the journal keeps every change"
                        );
                        let mut core = broker.lock();
                        if core.store.db.capture_number() == Some(snapshot.capture) {
                            core.store.abandon_capture();
                        }
                        drop(core);
                        return snapshot.abandon();
                    }
                    if done {
                        return snapshot.write();
                    }
                    std::thread::sleep(REST);
                }
            });
        match spawned {
            Ok(_) => {
                let _ = tx.send(snapshot);
            }
            Err(e) => {
                eprintln!("postino: cannot start the snapshot writer: {e}");
                Broker::finish_snapshot(core, snapshot);
            }
        }
    }

    /// Finishes a snapshot here and now, when no thread can take it.
    fn finish_snapshot(core: &mut Core, mut snapshot: Snapshot) {
        match core.finish_rewrite(&mut snapshot) {
            Ok(()) => snapshot.write(),
            Err(e) => {
                eprintln!("postino: snapshot rewrite failed: {e}");
                snapshot.abandon();
            }
        }
    }

    /// Runs an operation atomically with the current time.
    pub fn run<R>(&self, op: impl FnOnce(&mut Core, u64) -> R) -> R {
        let mut core = self.lock();
        let result = op(&mut core, now_ms());
        self.settle(&mut core);
        result
    }

    /// Retries `attempt` until it yields a value, `wait` runs out, or `alive`
    /// reports that the client went away. Between attempts the request sleeps
    /// until one of `topics` changes, or for a second at most.
    fn wait_for<R>(
        &self,
        wait: Duration,
        topics: &[Topic],
        alive: &dyn Fn() -> bool,
        mut attempt: impl FnMut(&mut Core, u64) -> Result<Option<R>>,
    ) -> Result<Option<R>> {
        let deadline = Instant::now() + wait;
        let mut core = self.lock();
        let result = attempt(&mut core, now_ms());
        self.settle(&mut core);
        if !matches!(result, Ok(None)) || wait.is_zero() || refuse_to_wait() {
            return result;
        }
        // Registered while the lock is still held, so no change between the
        // attempt and the sleep goes unseen.
        let signal = Arc::new(Condvar::new());
        let id = core.waiters.add(topics, Arc::clone(&signal));
        let outcome = loop {
            let now = Instant::now();
            if now >= deadline {
                break Ok(None);
            }
            let slice = (deadline - now).min(Duration::from_secs(1));
            core = match signal.wait_timeout(core, slice) {
                Ok((core, _)) => core,
                Err(poisoned) => self.halt(poisoned.into_inner().0),
            };
            if !alive() {
                break Ok(None);
            }
            let result = attempt(&mut core, now_ms());
            self.settle(&mut core);
            if !matches!(result, Ok(None)) {
                break result;
            }
        };
        core.waiters.remove(id, topics);
        outcome
    }

    /// Leases from `inbox`, blocking up to `wait` for entries to arrive. `alive`
    /// lets the caller abandon the wait when its client disconnects, so nothing is leased to a socket that is gone.
    pub fn lease_wait(
        &self,
        inbox: &str,
        consumer: &str,
        limit: usize,
        wait: Duration,
        alive: &dyn Fn() -> bool,
    ) -> Result<Vec<Leased>> {
        let topics = [Topic::Inbox(inbox.to_string())];
        let leased = self.wait_for(wait, &topics, alive, |core, now| {
            let leased = core.lease(inbox, consumer, limit, now)?;
            Ok((!leased.is_empty()).then_some(leased))
        })?;
        Ok(leased.unwrap_or_default())
    }

    /// Runs a command; a blocking command that finds nothing waits until the
    /// keyspace gives it something, its own timeout passes, or `max_wait` does.
    pub fn command(
        &self,
        argv: &[String],
        access: &Access,
        max_wait: Duration,
        alive: &dyn Fn() -> bool,
    ) -> Result<Reply> {
        if argv.len() == 2 && Name::of(argv).as_str() == "KEYS" {
            return Ok(self.keys(argv, access));
        }
        let out = self.run(|core, now| core.command(argv, access, true, now))?;
        let Some(block) = out.block else {
            return Ok(out.reply);
        };
        if refuse_to_wait() {
            return Ok(Reply::Nil);
        }
        let wait = match block.timeout_ms {
            0 => max_wait,
            ms => Duration::from_millis(ms).min(max_wait),
        };
        let topics: Vec<Topic> = key_args(&block.retry)
            .unwrap_or_default()
            .into_iter()
            .map(|(index, _)| Topic::Key(block.retry[index].clone()))
            .collect();
        let found = self.wait_for(wait, &topics, alive, |core, now| {
            let out = core.command(&block.retry, access, false, now)?;
            Ok((out.reply != Reply::Nil).then_some(out.reply))
        })?;
        Ok(found.unwrap_or(Reply::Nil))
    }

    /// KEYS, sent alone: [`KEYS_RUN`] keys at a time, letting go of the lock
    /// between runs, so that a pattern every key has to be checked against
    /// holds up other requests no longer than a run does. One with more than
    /// a run to look at moves off a connection loop to a thread of its own,
    /// as a request that waits does. A key there all along is listed once;
    /// one added or removed meanwhile may or may not be, as with SCAN.
    fn keys(&self, argv: &[String], access: &Access) -> Reply {
        if let Err(denied) = access.permit(argv) {
            return Reply::Error(denied);
        }
        let pattern = match Glob::client(&argv[1]) {
            Ok(pattern) => pattern,
            Err(e) => return Reply::Error(e),
        };
        let (mut listed, mut bytes) = (Vec::new(), 0);
        let mut after = None;
        loop {
            let (found, last) = self.run(|core, now| {
                keys_run(
                    &core.store.db,
                    &access.scope,
                    &pattern,
                    after.as_deref(),
                    KEYS_RUN,
                    now,
                )
            });
            for key in found {
                let key = Reply::Bulk(key);
                bytes += key.size();
                if let Err(e) = within_reply(bytes) {
                    return Reply::Error(e);
                }
                listed.push(key);
            }
            match last {
                None => return Reply::Array(listed),
                // Its first run changed nothing: it starts over on its thread.
                Some(_) if refuse_to_wait() => return Reply::Nil,
                Some(last) => {
                    after = Some(last);
                    std::thread::sleep(REST);
                }
            }
        }
    }

    /// Reads unread room posts, blocking up to `wait` until one arrives.
    pub fn room_wait(
        &self,
        room: &str,
        agent: &str,
        limit: usize,
        wait: Duration,
        alive: &dyn Fn() -> bool,
    ) -> Result<(Vec<Value>, usize)> {
        let topics = [Topic::Room(room.to_string())];
        let read = self.wait_for(wait, &topics, alive, |core, _| {
            let (items, remaining) = core.read_room(room, agent, limit)?;
            Ok((!items.is_empty()).then_some((items, remaining)))
        })?;
        Ok(read.unwrap_or_default())
    }

    /// Returns a question once it is resolved, withdrawn, or expired, or as it
    /// stands when `wait` runs out.
    pub fn question_wait(
        &self,
        room: &str,
        id: Id,
        wait: Duration,
        alive: &dyn Fn() -> bool,
    ) -> Result<Value> {
        let topics = [Topic::Room(room.to_string())];
        let settled = self.wait_for(wait, &topics, alive, |core, _| {
            let question = core.question(room, id)?;
            Ok((question["question"]["status"] != "open").then_some(question))
        })?;
        match settled {
            Some(question) => Ok(question),
            None => self.run(|core, _| core.question(room, id)),
        }
    }

    pub fn tick(&self) {
        // What the tick settles is written as a round of requests is: each
        // sweep in one write, once the lock is released.
        let batching = BATCHING.replace(true);
        let (sync, mut more) = {
            let mut core = self.lock();
            let more = core.sweep(now_ms()).unwrap_or_else(|e| {
                eprintln!("postino: sweep failed: {e}");
                false
            });
            core.journal_failing();
            let sync = core.journal.as_mut().and_then(Journal::take_sync);
            self.settle(&mut core);
            (sync, more)
        };
        self.flush_journal();
        // The once-a-second fsync runs without the lock, as requests go on.
        if let Some(mut sync) = sync {
            let result = sync.run();
            let mut core = self.lock();
            if let Some(journal) = core.journal.as_mut() {
                journal.synced(sync, result);
            }
            // What waited for the sync to end is written now.
            self.flush_deferred(&mut core);
        }
        // What falls due faster than one sweep a second settles: more
        // sweeps, with requests in between, for up to a quarter of the second.
        let started = Instant::now();
        while more && started.elapsed() < EXPIRY_BUDGET {
            std::thread::yield_now();
            more = self
                .run(|core, now| core.sweep_due(now))
                .unwrap_or_else(|e| {
                    eprintln!("postino: sweep failed: {e}");
                    false
                });
            self.flush_journal();
        }
        BATCHING.set(batching);
    }

    pub fn shutdown(&self) {
        let mut core = self.lock();
        self.flush_deferred(&mut core);
        let core = &mut *core;
        // Memory may hold what a failed request left half made: the journal
        // stays as it is, for the next start to restore.
        if core.halted {
            return;
        }
        if let Some(journal) = core.journal.as_mut()
            && let Err(e) = journal.rewrite(&mut core.store)
        {
            eprintln!("postino: final snapshot failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core() -> Core {
        Core::new(Store::default(), Limits::default(), None)
    }

    fn send(core: &mut Core, from: &str, to: &str, body: &str, now: u64) -> Message {
        core.send(
            SendRequest {
                from: from.into(),
                to: Some(to.into()),
                body: body.into(),
                reply_to: None,
                key: None,
                strict: false,
                admin: true,
            },
            now,
        )
        .unwrap()
        .0
    }

    #[test]
    fn lease_ack_keeps_history_and_clears_pending() {
        let mut core = core();
        send(&mut core, "a", "b", "one", 1000);
        send(&mut core, "a", "b", "two", 1000);
        let leased = core.lease("b", "worker", 10, 1001).unwrap();
        assert_eq!(leased.len(), 2);
        assert_eq!(leased[0].message.body, "one");
        assert!(core.lease("b", "other", 10, 1002).unwrap().is_empty());
        let receipts: Vec<String> = leased.iter().map(|l| l.receipt.clone()).collect();
        let outcome = core.ack("b", &receipts, None, 1003).unwrap();
        assert_eq!(outcome.acknowledged.len(), 2);
        assert_eq!(core.counts("b").pending(), 0);
        let (history, _) = core.history(&HistoryQuery {
            agent: Some("b".into()),
            limit: 10,
            ..HistoryQuery::default()
        });
        assert!(history.iter().all(|m| m.state == State::Acked));
        // Acknowledging again is idempotent.
        assert_eq!(
            core.ack("b", &receipts, None, 1004).unwrap().rejected.len(),
            0
        );
    }

    #[test]
    fn expired_leases_are_redelivered_then_dead_lettered() {
        let mut core = core();
        core.limits.max_deliveries = 2;
        send(&mut core, "a", "b", "poison", 0);
        let lease = core.limits.lease_ms;
        let first = core.lease("b", "w1", 1, 10).unwrap();
        assert_eq!(first[0].message.deliveries, 1);
        let second = core.lease("b", "w2", 1, 10 + lease).unwrap();
        assert_eq!(second[0].message.deliveries, 2);
        // The stale receipt from the first delivery no longer matches.
        let stale = core
            .ack("b", &[first[0].receipt.clone()], None, 11 + lease)
            .unwrap();
        assert_eq!(stale.rejected.len(), 1);
        assert!(core.lease("b", "w3", 1, 10 + 2 * lease).unwrap().is_empty());
        assert_eq!(core.counts("b").dead, 1);
    }

    #[test]
    fn late_ack_after_requeue_is_accepted_until_redelivered() {
        let mut core = core();
        send(&mut core, "a", "b", "slow", 0);
        let leased = core.lease("b", "w1", 1, 1).unwrap();
        core.sweep(1 + core.limits.lease_ms).unwrap();
        assert_eq!(core.counts("b").queued, 1);
        let outcome = core
            .ack(
                "b",
                &[leased[0].receipt.clone()],
                None,
                2 + core.limits.lease_ms,
            )
            .unwrap();
        assert_eq!(outcome.acknowledged.len(), 1);
        assert_eq!(core.counts("b").pending(), 0);
    }

    #[test]
    fn lease_ids_takes_only_the_requested_messages() {
        let mut core = core();
        send(&mut core, "a", "b", "first", 0);
        let second = send(&mut core, "a", "b", "second", 0);
        let leased = core.lease_ids("b", "console", &[second.id], 1).unwrap();
        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].message.body, "second");
        assert_eq!(core.counts("b").queued, 1);
        assert!(
            core.lease_ids("b", "console", &[second.id], 2)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn bare_ids_need_the_lease_consumer() {
        let mut core = core();
        let message = send(&mut core, "a", "b", "x", 0);
        core.lease("b", "worker-1", 1, 1).unwrap();
        let id = message.id.to_string();
        assert_eq!(
            core.ack("b", std::slice::from_ref(&id), Some("worker-2"), 2)
                .unwrap()
                .rejected
                .len(),
            1
        );
        assert_eq!(
            core.ack("b", &[id], Some("worker-1"), 2)
                .unwrap()
                .acknowledged
                .len(),
            1
        );
    }

    #[test]
    fn idempotency_keys_and_replies() {
        let mut core = core();
        let request = |key: &str| SendRequest {
            from: "a".into(),
            to: Some("b".into()),
            body: "task".into(),
            reply_to: None,
            key: Some(key.into()),
            strict: false,
            admin: true,
        };
        let (first, created) = core.send(request("k"), 1).unwrap();
        assert!(created);
        let (again, created) = core.send(request("k"), 2).unwrap();
        assert!(!created);
        assert_eq!(first.id, again.id);
        let (reply, _) = core
            .send(
                SendRequest {
                    from: "b".into(),
                    to: None,
                    body: "done".into(),
                    reply_to: Some(first.id),
                    key: None,
                    strict: true,
                    admin: false,
                },
                3,
            )
            .unwrap();
        assert_eq!(reply.to, "a");
        assert_eq!(reply.thread, first.id);
        let (thread, _) = core.history(&HistoryQuery {
            thread: Some(first.id),
            limit: 10,
            ..HistoryQuery::default()
        });
        assert_eq!(thread.len(), 2);
    }

    #[test]
    fn strict_sends_reject_unknown_agents_with_suggestions() {
        let mut core = core();
        send(&mut core, "a", "reviewer", "x", 1);
        let err = core
            .send(
                SendRequest {
                    from: "a".into(),
                    to: Some("reveiwer".into()),
                    body: "x".into(),
                    reply_to: None,
                    key: None,
                    strict: true,
                    admin: false,
                },
                2,
            )
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(ref m) if m.contains("reviewer")));
    }

    #[test]
    fn inbox_capacity_applies_backpressure() {
        let mut core = core();
        core.limits.max_inbox = 2;
        send(&mut core, "a", "b", "1", 1);
        send(&mut core, "a", "b", "2", 1);
        let err = core
            .send(
                SendRequest {
                    from: "a".into(),
                    to: Some("b".into()),
                    body: "3".into(),
                    reply_to: None,
                    key: None,
                    strict: false,
                    admin: true,
                },
                1,
            )
            .unwrap_err();
        assert!(matches!(err, Error::Full(_)));
    }

    #[test]
    fn history_is_trimmed_to_the_limit() {
        let mut core = core();
        core.limits.history_limit = 2;
        for i in 0..5 {
            send(&mut core, "a", "b", &i.to_string(), i);
        }
        let leased = core.lease("b", "w", 10, 10).unwrap();
        let receipts: Vec<String> = leased.iter().map(|l| l.receipt.clone()).collect();
        core.ack("b", &receipts, None, 11).unwrap();
        let (history, _) = core.history(&HistoryQuery {
            agent: Some("b".into()),
            limit: 10,
            ..HistoryQuery::default()
        });
        assert_eq!(
            history.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
            ["3", "4"]
        );
    }

    #[test]
    fn messages_expire_after_ttl() {
        let mut core = core();
        core.limits.msg_ttl_ms = 100;
        send(&mut core, "a", "b", "old", 0);
        core.sweep(100).unwrap();
        assert_eq!(core.counts("b").pending(), 0);
        core.sweep(200).unwrap();
        assert!(core.store.messages.is_empty());
    }

    #[test]
    fn broadcast_cursors_survive_trimming() {
        let mut core = core();
        core.limits.max_broadcasts = 2;
        core.ensure_agents(&["reader"], 0).unwrap();
        core.broadcast("ops", "one".into(), None, 1).unwrap();
        let (seen, _) = core.read_broadcasts("reader", false, true).unwrap();
        assert_eq!(seen.len(), 1);
        core.broadcast("ops", "two".into(), None, 2).unwrap();
        core.broadcast("ops", "three".into(), None, 3).unwrap();
        assert_eq!(core.store.unseen_broadcasts("reader"), 2);
        let (unseen, total) = core.read_broadcasts("reader", false, true).unwrap();
        assert_eq!(
            unseen.iter().map(|b| b.body.as_str()).collect::<Vec<_>>(),
            ["two", "three"]
        );
        assert_eq!(total, 2);
        assert_eq!(core.store.unseen_broadcasts("reader"), 0);
        assert_eq!(
            core.store.unseen_broadcasts("ops"),
            0,
            "own broadcasts are not unseen"
        );
    }

    #[test]
    fn presence_expires_and_rename_requires_single_instance() {
        let mut core = core();
        let presence = |session: &str| PresenceRequest {
            agent: "worker".into(),
            session: session.into(),
            client: "test".into(),
            pid: None,
            host: None,
            ttl_ms: 100,
        };
        core.presence(presence("s1"), 0).unwrap();
        core.presence(presence("s2"), 0).unwrap();
        assert!(matches!(
            core.rename("worker", "builder", 1),
            Err(Error::Conflict(_))
        ));
        core.leave("worker", "s2", 1);
        core.rename("worker", "builder", 2).unwrap();
        assert_eq!(core.store.live_sessions("builder", 2), 1);
        core.sweep(200).unwrap();
        assert_eq!(core.store.live_sessions("builder", 200), 0);
        assert_eq!(core.cleanup(200).unwrap(), ["builder"]);
    }

    #[test]
    fn sessions_are_capped_per_agent_and_not_shared() {
        let mut core = core();
        for i in 0..40 {
            core.presence(
                PresenceRequest {
                    agent: "busy".into(),
                    session: format!("s{i}"),
                    client: "test".into(),
                    pid: None,
                    host: None,
                    ttl_ms: 1000 + i,
                },
                0,
            )
            .unwrap();
        }
        assert_eq!(core.store.live_sessions("busy", 0), MAX_SESSIONS_PER_AGENT);
        assert!(core.store.sessions.contains_key("s39"));
        assert!(!core.store.sessions.contains_key("s0"));
        let hijack = core.presence(
            PresenceRequest {
                agent: "other".into(),
                session: "s39".into(),
                client: "test".into(),
                pid: None,
                host: None,
                ttl_ms: 1000,
            },
            0,
        );
        assert!(matches!(hijack, Err(Error::Conflict(_))));
    }

    #[test]
    fn namespace_prunes_inactive_agents_when_full() {
        let mut core = core();
        core.limits.max_agents = 2;
        core.ensure_agents(&["idle"], 0).unwrap();
        send(&mut core, "a", "busy", "x", 1);
        assert!(!core.store.agents.contains_key("idle"));
        let err = core.ensure_agents(&["c", "d"], 2).unwrap_err();
        assert!(matches!(err, Error::Full(_)));
    }

    fn agent(name: &str) -> Actor {
        Actor {
            name: name.into(),
            admin: false,
            strict: true,
        }
    }

    fn operator() -> Actor {
        Actor {
            name: "operator".into(),
            admin: true,
            strict: false,
        }
    }

    fn room(core: &mut Core, owner: &str, members: &[&str]) {
        // The members have connected, so an agent may name them.
        core.ensure_agents(members, 0).unwrap();
        core.create_room(
            CreateRoom {
                name: "release".into(),
                topic: "ship 2.0".into(),
                private: false,
                members: members.iter().map(|m| m.to_string()).collect(),
                join: false,
            },
            &agent(owner),
            1,
        )
        .unwrap();
    }

    fn note(body: &str) -> PostRequest {
        PostRequest {
            room: "release".into(),
            body: body.into(),
            reply_to: None,
            about: None,
            key: None,
            ask: None,
        }
    }

    fn ask(to: &[&str], options: &[&str]) -> PostRequest {
        PostRequest {
            ask: Some(AskRequest {
                to: to.iter().map(|m| m.to_string()).collect(),
                options: options.iter().map(|o| o.to_string()).collect(),
                ttl_ms: None,
            }),
            ..note("ship it?")
        }
    }

    fn bodies(items: &[Value]) -> Vec<&str> {
        items.iter().map(|p| p["body"].as_str().unwrap()).collect()
    }

    #[test]
    fn room_members_read_every_post_with_their_own_cursor() {
        let mut core = core();
        room(&mut core, "planner", &["builder"]);
        core.post(note("plan is ready"), &agent("planner"), 2)
            .unwrap();
        core.post(note("building"), &agent("builder"), 3).unwrap();
        assert!(matches!(
            core.read_room("nope", "builder", 10),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            core.read_room("release", "tester", 10),
            Err(Error::Forbidden(_))
        ));
        let (items, remaining) = core.read_room("release", "builder", 10).unwrap();
        assert_eq!(
            bodies(&items),
            ["created the room: ship 2.0", "plan is ready"]
        );
        assert_eq!(remaining, 0);
        assert!(
            core.read_room("release", "builder", 10)
                .unwrap()
                .0
                .is_empty()
        );
        // A late joiner catches up on the retained history.
        core.join("release", "tester", None, &agent("tester"), 4)
            .unwrap();
        let (items, remaining) = core.read_room("release", "tester", 2).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(remaining, 1, "its own join notice does not count");
        let (items, _) = core.read_room("release", "planner", 10).unwrap();
        assert_eq!(bodies(&items), ["building", "joined"]);
        assert_eq!(core.store.rooms["release"].unread("planner"), 0);
    }

    #[test]
    fn private_rooms_and_roles_are_enforced() {
        let mut core = core();
        core.ensure_agents(&["builder", "outsider"], 0).unwrap();
        core.create_room(
            CreateRoom {
                name: "secret".into(),
                topic: String::new(),
                private: true,
                members: vec!["builder".into()],
                join: false,
            },
            &agent("lead"),
            1,
        )
        .unwrap();
        let outsider = agent("outsider");
        assert!(matches!(
            core.join("secret", "outsider", None, &outsider, 2),
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            core.can_view("secret", &outsider),
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            core.join("secret", "other", None, &agent("builder"), 2),
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            core.update_room("secret", None, Some(true), &agent("builder"), 2),
            Err(Error::Forbidden(_))
        ));
        core.join("secret", "outsider", None, &agent("lead"), 2)
            .unwrap();
        core.can_view("secret", &outsider).unwrap();
        core.update_room("secret", None, Some(true), &agent("lead"), 3)
            .unwrap();
        assert!(matches!(
            core.post(
                PostRequest {
                    room: "secret".into(),
                    ..note("x")
                },
                &outsider,
                4
            ),
            Err(Error::Conflict(_))
        ));
    }

    #[test]
    fn questions_resolve_when_every_addressee_answers() {
        let mut core = core();
        room(&mut core, "planner", &["reviewer"]);
        core.ensure_agents(&["security"], 0).unwrap();
        let (question, _) = core
            .post(
                ask(&["reviewer", "security"], &["approve", "reject"]),
                &agent("planner"),
                2,
            )
            .unwrap();
        assert!(
            core.store.rooms["release"].members.contains_key("security"),
            "addressees are brought in"
        );
        assert_eq!(core.store.rooms["release"].waiting_for("security"), 1);
        let answer = |core: &mut Core, who: &str, choice: Option<&str>| {
            core.answer(
                AnswerRequest {
                    room: "release".into(),
                    question: question.id,
                    body: String::new(),
                    choice: choice.map(String::from),
                    key: None,
                },
                &agent(who),
                3,
            )
        };
        assert!(matches!(
            answer(&mut core, "planner", Some("approve")),
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            answer(&mut core, "builder", Some("approve")),
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            answer(&mut core, "reviewer", Some("maybe")),
            Err(Error::BadRequest(_))
        ));
        let (_, resolved) = answer(&mut core, "reviewer", Some("APPROVE")).unwrap();
        assert!(!resolved);
        assert!(matches!(
            answer(&mut core, "reviewer", Some("approve")),
            Err(Error::Conflict(_))
        ));
        let (post, resolved) = answer(&mut core, "security", Some("reject")).unwrap();
        assert!(resolved);
        assert_eq!(post.choice.as_deref(), Some("reject"));
        let value = core.question("release", question.id).unwrap();
        assert_eq!(value["question"]["status"], "resolved");
        let choices: Vec<&str> = value["question"]["answers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["choice"].as_str().unwrap())
            .collect();
        assert_eq!(choices, ["approve", "reject"]);
    }

    #[test]
    fn messages_falling_due_together_are_settled_a_batch_at_a_time() {
        let mut core = core();
        for i in 0..2500 {
            send(&mut core, "a", &format!("b{}", i % 5), "x", 1);
        }
        let due = 1 + core.limits.msg_ttl_ms;
        let left = |core: &Core| core.store.expired_due(due).count();
        // A lease settles a batch too, and hands out none that expired.
        assert!(core.lease("b0", "worker", 10, due).unwrap().is_empty());
        assert_eq!(left(&core), 2500 - SWEEP_BATCH);
        assert!(core.sweep(due).unwrap(), "more are due");
        assert_eq!(left(&core), 2500 - 2 * SWEEP_BATCH);
        assert!(!core.sweep(due).unwrap());
        assert_eq!(left(&core), 0);
        assert_eq!(core.store.counts("b0").queued, 0);
    }

    #[test]
    fn open_questions_expire_and_survive_trimming() {
        let mut core = core();
        core.limits.room_history = 3;
        room(&mut core, "planner", &[]);
        let (question, _) = core
            .post(
                PostRequest {
                    about: Some("task-7".into()),
                    ..ask(&["operator"], &[])
                },
                &agent("planner"),
                2,
            )
            .unwrap();
        for i in 0..5 {
            core.post(note(&i.to_string()), &agent("planner"), 3)
                .unwrap();
        }
        let posts = &core.store.rooms["release"].posts;
        assert_eq!(posts.len(), 3);
        assert!(posts.contains_key(&question.id));
        let (about, _) = core
            .room_posts(
                "release",
                &PostQuery {
                    about: Some("task-7".into()),
                    limit: 10,
                    ..PostQuery::default()
                },
            )
            .unwrap();
        assert_eq!(about.len(), 1);
        core.sweep(2 + core.limits.msg_ttl_ms).unwrap();
        assert_eq!(
            core.question("release", question.id).unwrap()["question"]["status"],
            "expired"
        );
    }

    #[test]
    fn rooms_know_their_open_questions_through_every_change() {
        let dir = std::env::temp_dir().join(format!(
            "postino-open-{}-{}",
            std::process::id(),
            random_hex(4)
        ));
        let reopen = || {
            let (journal, store) = Journal::open(&dir, Fsync::Always).unwrap();
            Core::new(store, Limits::default(), Some(journal))
        };
        // The sweep and the listings read the index instead of every post, so
        // it must hold exactly the open questions a walk over the posts finds.
        let open = |core: &Core| {
            let record = &core.store.rooms["release"];
            let walked: std::collections::BTreeSet<Id> = record
                .posts
                .values()
                .filter(|p| p.is_open_question())
                .map(|p| p.id)
                .collect();
            assert_eq!(record.open, walked);
            walked.len()
        };
        let mut core = reopen();
        core.limits.room_history = 3;
        room(&mut core, "planner", &["builder"]);
        let mut asked = Vec::new();
        for ttl_ms in [None, None, None, Some(60_000)] {
            let mut req = ask(&["builder"], &[]);
            req.ask.as_mut().unwrap().ttl_ms = ttl_ms;
            asked.push(core.post(req, &agent("planner"), 2).unwrap().0.id);
        }
        assert_eq!(open(&core), 4);
        let answer = AnswerRequest {
            room: "release".into(),
            question: asked[0],
            body: "done".into(),
            choice: None,
            key: None,
        };
        core.answer(answer, &agent("builder"), 3).unwrap();
        core.withdraw("release", asked[1], &agent("planner"), 3)
            .unwrap();
        for i in 0..5 {
            core.post(note(&i.to_string()), &agent("planner"), 3)
                .unwrap();
        }
        assert_eq!(open(&core), 2, "answered and withdrawn questions leave it");
        core.sweep(2 + 60_000).unwrap();
        assert_eq!(open(&core), 1, "expired ones too");
        drop(core);
        let mut core = reopen();
        assert_eq!(open(&core), 1, "the journal rebuilds it");
        core.journal
            .as_mut()
            .unwrap()
            .rewrite(&mut core.store)
            .unwrap();
        drop(core);
        let mut core = reopen();
        assert_eq!(open(&core), 1, "and so does a snapshot");
        core.update_room("release", None, Some(true), &agent("planner"), 4)
            .unwrap();
        assert_eq!(open(&core), 0, "closing the room withdraws the rest");
        drop(core);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn renames_flushes_and_expiries_wake_the_requests_they_concern() {
        let broker = Broker::open(Limits::default(), Storage::Memory).unwrap();
        let words = |line: &str| line.split(' ').map(String::from).collect::<Vec<_>>();
        broker.run(|core, now| {
            send(core, "planner", "old", "build 7", now);
            for line in [
                "XGROUP CREATE flushed g $ MKSTREAM",
                "XGROUP CREATE expiring g $ MKSTREAM",
                "PEXPIRE expiring 100",
            ] {
                core.command(&words(line), &Access::admin(), false, now)
                    .unwrap();
            }
        });
        let wait = Duration::from_secs(5);
        let timed = |broker: &Arc<Broker>, f: fn(&Broker, Duration) -> bool| {
            let broker = Arc::clone(broker);
            std::thread::spawn(move || {
                let started = Instant::now();
                assert!(f(&broker, wait));
                started.elapsed()
            })
        };
        // A blocked group read on a stream that disappears fails with NOGROUP.
        fn gone(b: &Broker, wait: Duration, stream: &str) -> bool {
            let line = format!("XREADGROUP GROUP g c BLOCK 5000 STREAMS {stream} >");
            let argv: Vec<String> = line.split(' ').map(String::from).collect();
            matches!(
                b.command(&argv, &Access::admin(), wait, &|| true),
                Ok(Reply::Error(e)) if e.starts_with("NOGROUP")
            )
        }
        let renamed = timed(&broker, |b, wait| {
            !b.lease_wait("new", "w", 1, wait, &|| true)
                .unwrap()
                .is_empty()
        });
        let expired = timed(&broker, |b, wait| gone(b, wait, "expiring"));
        let flushed = timed(&broker, |b, wait| gone(b, wait, "flushed"));
        let woke = |waiter: std::thread::JoinHandle<Duration>| {
            let took = waiter.join().unwrap();
            assert!(
                took < Duration::from_millis(700),
                "a waiter woke after {took:?}, not when its change came"
            );
        };
        std::thread::sleep(Duration::from_millis(150));
        broker.tick();
        broker
            .run(|core, now| core.rename("old", "new", now))
            .unwrap();
        // Before the flush, which would wake them all.
        woke(renamed);
        woke(expired);
        broker.run(|core, now| {
            core.command(&words("FLUSHALL"), &Access::admin(), false, now)
                .unwrap()
        });
        woke(flushed);
    }

    #[test]
    fn a_change_wakes_the_requests_waiting_for_it_at_once() {
        let broker = Broker::open(Limits::default(), Storage::Memory).unwrap();
        let question = broker.run(|core, now| {
            room(core, "planner", &["builder"]);
            let (question, _) = core
                .post(ask(&["builder"], &[]), &agent("planner"), now)
                .unwrap();
            core.read_room("release", "planner", 100).unwrap();
            question.id
        });
        let wait = Duration::from_secs(5);
        let timed = |broker: &Arc<Broker>, f: fn(&Broker, Duration) -> bool| {
            let broker = Arc::clone(broker);
            std::thread::spawn(move || {
                let started = Instant::now();
                assert!(f(&broker, wait));
                started.elapsed()
            })
        };
        let waiters = [
            timed(&broker, |b, wait| {
                let argv = ["BLPOP", "jobs", "5"].map(String::from);
                b.command(&argv, &Access::admin(), wait, &|| true).unwrap() != Reply::Nil
            }),
            timed(&broker, |b, wait| {
                !b.lease_wait("builder", "w", 1, wait, &|| true)
                    .unwrap()
                    .is_empty()
            }),
            timed(&broker, |b, wait| {
                !b.room_wait("release", "planner", 10, wait, &|| true)
                    .unwrap()
                    .0
                    .is_empty()
            }),
        ];
        let answered = {
            let broker = Arc::clone(&broker);
            std::thread::spawn(move || {
                let started = Instant::now();
                let settled = broker.question_wait("release", question, wait, &|| true);
                assert_eq!(settled.unwrap()["question"]["status"], "resolved");
                started.elapsed()
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        broker.run(|core, now| {
            for i in 0..100 {
                let argv = ["LPUSH".to_string(), format!("other:{i}"), "x".into()];
                core.command(&argv, &Access::admin(), false, now).unwrap();
            }
            let push = ["LPUSH", "jobs", "build"].map(String::from);
            core.command(&push, &Access::admin(), false, now).unwrap();
            send(core, "planner", "builder", "build 7", now);
            core.answer(
                AnswerRequest {
                    room: "release".into(),
                    question,
                    body: "yes".into(),
                    choice: None,
                    key: None,
                },
                &agent("builder"),
                now,
            )
            .unwrap();
        });
        for waiter in waiters.into_iter().chain([answered]) {
            let took = waiter.join().unwrap();
            assert!(
                took < Duration::from_millis(700),
                "a waiter woke after {took:?}, not when its change came"
            );
        }
        broker.run(|core, _| {
            assert!(core.waiters.signals.is_empty() && core.waiters.by_topic.is_empty());
        });
    }

    #[test]
    fn reads_too_large_to_send_are_refused_whole() {
        let mut core = core();
        let admin = Access::admin();
        let words = |line: &str| line.split(' ').map(String::from).collect::<Vec<_>>();
        let run = |core: &mut Core, argv: Vec<String>| {
            core.command(&argv, &admin, false, 0).unwrap().reply
        };
        let big = "x".repeat(16 << 20);
        for _ in 0..5 {
            run(&mut core, vec!["RPUSH".into(), "list".into(), big.clone()]);
        }
        let refused = |reply: &Reply| matches!(reply, Reply::Error(e) if e.contains("64 MiB"));
        assert!(refused(&run(&mut core, words("LRANGE list 0 -1"))));
        assert!(!refused(&run(&mut core, words("LRANGE list 0 1"))));
        let batch: Vec<Vec<String>> = (0..5).map(|_| words("LINDEX list 0")).collect();
        let replies = core.multi(&batch, &admin, 0).unwrap();
        assert!(!refused(&replies[0]));
        assert!(refused(&replies[4]), "a batch's replies count together");
        let popped = run(&mut core, words("LPOP list 5"));
        assert!(
            matches!(&popped, Reply::Array(items) if items.len() == 5),
            "what a command removes is handed over, whatever its size"
        );
    }

    #[test]
    fn a_snapshot_taken_while_writes_go_on_restores_every_one() {
        let dir = crate::journal::tests::temp_dir("live-snapshot");
        let open = || {
            Broker::open(
                Limits::default(),
                Storage::Disk {
                    dir: dir.clone(),
                    fsync: Fsync::Never,
                },
            )
            .unwrap()
        };
        let admin = Access::admin();
        let set = |broker: &Broker, key: String, value: String| {
            broker
                .command(&["SET".into(), key, value], &admin, Duration::ZERO, &|| {
                    true
                })
                .unwrap();
        };
        let mut expected = BTreeMap::new();
        {
            let broker = open();
            let value = "v".repeat(1024);
            // Past the journal's rewrite threshold, so a snapshot begins.
            for i in 0..6000 {
                set(&broker, format!("k{i:05}"), value.clone());
                expected.insert(format!("k{i:05}"), value.clone());
            }
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut round = 0;
            while !dir.join("snapshot.json").exists() {
                assert!(Instant::now() < deadline, "no snapshot was written");
                let key = format!("k{:05}", (round * 7919) % 6000);
                set(&broker, key.clone(), format!("round {round}"));
                expected.insert(key, format!("round {round}"));
                round += 1;
            }
            let captured = broker.run(|core, _| core.store.capturing());
            assert!(!captured);
            for i in 0..100 {
                set(&broker, format!("after{i}"), "x".into());
                expected.insert(format!("after{i}"), "x".into());
            }
            // Dropped without a shutdown, as in a crash.
        }
        let deadline = Instant::now() + Duration::from_secs(20);
        let broker = loop {
            match Broker::open(
                Limits::default(),
                Storage::Disk {
                    dir: dir.clone(),
                    fsync: Fsync::Never,
                },
            ) {
                Ok(broker) => break broker,
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("{e}"),
            }
        };
        let restored: BTreeMap<String, String> = broker.run(|core, now| {
            core.store
                .db
                .live_keys(now)
                .map(|(key, entry)| match &entry.value {
                    crate::db::Value::String(s) => (key.clone(), s.clone()),
                    other => panic!("{key} holds {other:?}"),
                })
                .collect()
        });
        assert_eq!(restored.len(), expected.len());
        assert!(restored == expected, "the restored keyspace differs");
        drop(broker);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shutdown_takes_over_a_snapshot_still_being_taken() {
        let dir = crate::journal::tests::temp_dir("takeover");
        let disk = || Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::Never,
        };
        let broker = Broker::open(Limits::default(), disk()).unwrap();
        broker.run(|core, _| {
            for i in 0..200_000 {
                let argv = ["SET".to_string(), format!("k{i:06}"), "v".repeat(100)];
                core.store.db.exec(&argv, &Ctx::replay(0));
            }
            let snapshot = core
                .journal
                .as_mut()
                .unwrap()
                .start_rewrite(&mut core.store)
                .unwrap();
            broker.take_snapshot(core, snapshot);
        });
        broker.shutdown();
        drop(broker);
        let deadline = Instant::now() + Duration::from_secs(20);
        let reopened = loop {
            match Broker::open(Limits::default(), disk()) {
                Ok(broker) => break broker,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("{e}"),
            }
        };
        assert_eq!(
            reopened.run(|core, _| core.store.db.entries().len()),
            200_000
        );
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|name| name.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_request_that_fails_partway_through_a_change_stops_all_changes() {
        let dir = crate::journal::tests::temp_dir("halted");
        let disk = || Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::Never,
        };
        let words = |line: &str| line.split(' ').map(String::from).collect::<Vec<_>>();
        let command = |broker: &Broker, line: &str| {
            broker
                .run(|core, now| core.command(&words(line), &Access::admin(), false, now))
                .unwrap()
                .reply
        };
        let broker = Broker::open(Limits::default(), disk()).unwrap();
        assert_eq!(command(&broker, "SET before v"), Reply::ok());
        // The request panics with its change made in memory, not journaled.
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            broker.run(|core, _| {
                core.store.db.exec(&words("SET half v"), &Ctx::replay(0));
                panic!("partway through a change");
            })
        }));
        assert!(failed.is_err());
        assert!(broker.halted());
        let refused = command(&broker, "SET after v");
        assert!(
            matches!(&refused, Reply::Error(e) if e.starts_with("READONLY")),
            "{refused:?}"
        );
        let sent = broker.run(|core, now| {
            core.send(
                SendRequest {
                    from: "a".into(),
                    to: Some("b".into()),
                    body: "hi".into(),
                    reply_to: None,
                    key: None,
                    strict: false,
                    admin: true,
                },
                now,
            )
        });
        assert!(matches!(sent, Err(Error::Storage(e)) if e.starts_with("READONLY")));
        assert_eq!(command(&broker, "GET half"), Reply::Bulk("v".into()));
        // Shutting down takes no snapshot of memory: the next start restores
        // the changes made whole, and takes changes again.
        broker.shutdown();
        drop(broker);
        let deadline = Instant::now() + Duration::from_secs(20);
        let reopened = loop {
            match Broker::open(Limits::default(), disk()) {
                Ok(broker) => break broker,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("{e}"),
            }
        };
        assert!(!reopened.halted());
        assert_eq!(command(&reopened, "GET before"), Reply::Bulk("v".into()));
        assert_eq!(command(&reopened, "GET half"), Reply::Nil);
        assert_eq!(command(&reopened, "SET after v"), Reply::ok());
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn keys_looks_at_a_run_of_keys_at_a_time_and_lists_each_once() {
        let broker = Broker::open(Limits::default(), Storage::Memory).unwrap();
        let total = crate::commands::KEYS_WHOLE + 1;
        broker.run(|core, _| {
            for i in 0..total {
                let argv = ["SET".to_string(), format!("k{i:05}"), "v".into()];
                core.store.db.exec(&argv, &Ctx::replay(0));
            }
        });
        let words = |line: &str| line.split(' ').map(String::from).collect::<Vec<_>>();
        let names = |reply: Reply| -> Vec<String> {
            let Reply::Array(items) = reply else {
                panic!("{reply:?}")
            };
            items
                .into_iter()
                .map(|item| match item {
                    Reply::Bulk(name) => name,
                    other => panic!("{other:?}"),
                })
                .collect()
        };
        let command = |line: &str| {
            broker
                .command(&words(line), &Access::admin(), Duration::ZERO, &|| true)
                .unwrap()
        };
        // Sent alone, it looks through every run.
        let all = names(command("KEYS *"));
        assert_eq!(all.len(), total);
        assert!(
            all.windows(2).all(|w| w[0] < w[1]),
            "each key once, in order"
        );
        assert_eq!(names(command("KEYS *7")).len(), total / 10);
        // On a connection loop, one with more than a run to look at moves to
        // a thread of its own, and one with less does not.
        assert!(without_waiting(|| command("KEYS *")).is_none());
        let few = without_waiting(|| command("KEYS k000*")).expect("one run stays");
        assert_eq!(names(few).len(), 100);
        // A batch runs whole, and so does a KEYS in it, up to a limit.
        let batch = broker
            .run(|core, now| {
                core.multi(
                    &[words("KEYS *"), words("KEYS k0000*")],
                    &Access::admin(),
                    now,
                )
            })
            .unwrap();
        assert!(
            matches!(&batch[0], Reply::Error(e) if e.contains("SCAN")),
            "{:?}",
            batch[0]
        );
        assert_eq!(names(batch[1].clone()).len(), 10);
        // SCAN looks at no more than SCAN_MOST keys a call, whatever COUNT
        // asks, and pages through each key once.
        let (mut cursor, mut seen) = ("0".to_string(), Vec::new());
        loop {
            let Reply::Array(mut parts) = command(&format!("SCAN {cursor} COUNT 5000")) else {
                panic!("SCAN replies with a cursor and keys")
            };
            let page = names(parts.pop().unwrap());
            assert!(page.len() <= crate::commands::SCAN_MOST);
            seen.extend(page);
            let Some(Reply::Bulk(next)) = parts.pop() else {
                panic!("no cursor")
            };
            cursor = next;
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen, all);
    }

    #[test]
    fn changes_batched_on_a_thread_are_written_before_anyone_replies() {
        let dir = crate::journal::tests::temp_dir("batched");
        let journaled = |key: &str| {
            let line = format!(r#"["SET","{key}","v"]"#);
            std::fs::read_dir(&dir).unwrap().any(|entry| {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                name.starts_with("journal")
                    && std::fs::read_to_string(&path).unwrap().contains(&line)
            })
        };
        let set = |broker: &Broker, key: &str| {
            let argv = ["SET".to_string(), key.to_string(), "v".to_string()];
            broker
                .command(&argv, &Access::admin(), Duration::ZERO, &|| true)
                .unwrap();
        };
        let storage = Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::EverySec,
        };
        let broker = Broker::open(Limits::default(), storage).unwrap();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                batch_journal_writes();
                set(&broker, "a");
                assert!(!journaled("a"), "batched, so not written yet");
                broker.flush_journal();
                assert!(journaled("a"));
                set(&broker, "b");
            });
        });
        assert!(!journaled("b"));
        // A thread that does not batch writes what waits before its reply,
        // even to a read: the reply may show the change.
        let argv = ["GET".to_string(), "b".to_string()];
        let got = broker.command(&argv, &Access::admin(), Duration::ZERO, &|| true);
        assert_eq!(got.unwrap(), Reply::Bulk("v".into()));
        assert!(journaled("b"));

        // A rewrite sets the journal aside with what was batched in it.
        std::thread::scope(|scope| {
            scope.spawn(|| {
                batch_journal_writes();
                set(&broker, "c");
                broker.run(|core, _| {
                    let journal = core.journal.as_mut().unwrap();
                    let snapshot = journal.start_rewrite(&mut core.store).unwrap();
                    broker.take_snapshot(core, snapshot);
                });
                assert!(journaled("c"));
            });
        });
        while broker.run(|core, _| core.store.capturing()) {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn messages_sent_on_a_thread_that_batches_are_written_with_its_round() {
        let dir = crate::journal::tests::temp_dir("batched-forum");
        let journaled = |body: &str| {
            std::fs::read_to_string(dir.join("journal.log"))
                .unwrap()
                .contains(body)
        };
        let storage = Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::EverySec,
        };
        let broker = Broker::open(Limits::default(), storage).unwrap();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                batch_journal_writes();
                broker.run(|core, now| send(core, "a", "b", "first", now));
                assert!(!journaled("first"), "batched, so not written yet");
                broker.flush_journal();
                assert!(journaled("first"));
                broker.run(|core, now| send(core, "a", "b", "second", now));
            });
        });
        assert!(!journaled("second"));
        // A thread that does not batch writes it before its reply, even to a
        // read.
        let counts = broker.run(|core, _| core.store.counts("b"));
        assert_eq!(counts.queued, 2);
        assert!(journaled("second"));
        drop(broker);
        let reopened = Broker::open(
            Limits::default(),
            Storage::Disk {
                dir: dir.clone(),
                fsync: Fsync::EverySec,
            },
        )
        .unwrap();
        assert_eq!(reopened.run(|core, _| core.store.counts("b")).queued, 2);
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn changes_synced_one_by_one_are_never_batched() {
        let dir = crate::journal::tests::temp_dir("synced");
        let storage = Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::Always,
        };
        let broker = Broker::open(Limits::default(), storage).unwrap();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                batch_journal_writes();
                let argv = ["SET".to_string(), "d".to_string(), "v".to_string()];
                broker
                    .command(&argv, &Access::admin(), Duration::ZERO, &|| true)
                    .unwrap();
                let journal = std::fs::read_to_string(dir.join("journal.log")).unwrap();
                assert!(journal.contains(r#"["SET","d","v"]"#));
            });
        });
    }

    #[test]
    fn an_idle_shutdown_after_a_failed_snapshot_keeps_every_write() {
        let dir = crate::journal::tests::temp_dir("failed-snapshot");
        let disk = || Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::Never,
        };
        let broker = Broker::open(Limits::default(), disk()).unwrap();
        broker.run(|core, now| {
            for i in 0..100 {
                let argv = ["SET".to_string(), format!("k{i}"), "v".into()];
                core.command(&argv, &Access::admin(), false, now).unwrap();
            }
        });
        // The background snapshot cannot write its file, as on a full disk,
        // and neither can the final one.
        let blocked = [dir.join("snapshot.100.1.tmp"), dir.join("snapshot.json")];
        for path in &blocked {
            std::fs::create_dir_all(path.join("x")).unwrap();
        }
        broker.run(|core, _| {
            let snapshot = core
                .journal
                .as_mut()
                .unwrap()
                .start_rewrite(&mut core.store)
                .unwrap();
            broker.take_snapshot(core, snapshot);
        });
        while broker.run(|core, _| core.store.capturing()) {
            std::thread::sleep(Duration::from_millis(5));
        }
        broker.shutdown();
        drop(broker);
        for path in &blocked {
            std::fs::remove_dir_all(path).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(20);
        let reopened = loop {
            match Broker::open(Limits::default(), disk()) {
                Ok(broker) => break broker,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("{e}"),
            }
        };
        assert_eq!(reopened.run(|core, _| core.store.db.entries().len()), 100);
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_mass_expiry_takes_more_than_one_batch_a_second() {
        let broker = Broker::open(Limits::default(), Storage::Memory).unwrap();
        broker.run(|core, now| {
            for i in 0..10_000 {
                let argv = [
                    "SET".into(),
                    format!("k{i}"),
                    "v".into(),
                    "PX".into(),
                    "1".into(),
                ];
                core.command(&argv, &Access::admin(), false, now).unwrap();
            }
        });
        std::thread::sleep(Duration::from_millis(5));
        broker.tick();
        let left = broker.run(|core, _| core.store.db.entries().len());
        assert!(left <= 7_000, "{left} expired keys are left after a tick");
    }

    #[test]
    fn answers_to_an_open_question_survive_trimming() {
        let mut core = core();
        core.limits.room_history = 3;
        room(&mut core, "planner", &["reviewer", "security"]);
        let (question, _) = core
            .post(ask(&["reviewer", "security"], &[]), &agent("planner"), 2)
            .unwrap();
        let answer = |core: &mut Core, who: &str| {
            core.answer(
                AnswerRequest {
                    room: "release".into(),
                    question: question.id,
                    body: format!("{who} approves"),
                    choice: None,
                    key: None,
                },
                &agent(who),
                3,
            )
        };
        answer(&mut core, "reviewer").unwrap();
        for i in 0..5 {
            core.post(note(&i.to_string()), &agent("planner"), 4)
                .unwrap();
        }
        assert!(
            matches!(answer(&mut core, "reviewer"), Err(Error::Conflict(_))),
            "the first answer still counts"
        );
        let (_, resolved) = answer(&mut core, "security").unwrap();
        assert!(resolved);
        let asked = core.question("release", question.id).unwrap();
        let answers = asked["question"]["answers"].as_array().unwrap();
        assert_eq!(answers.len(), 2, "{asked}");
    }

    #[test]
    fn only_the_operator_brings_in_names_the_server_has_not_seen() {
        let mut core = core();
        core.set_token_agents(["ci".to_string()]);
        let planner = agent("planner");
        let create = |core: &mut Core, name: &str, members: &[&str]| {
            core.create_room(
                CreateRoom {
                    name: name.into(),
                    topic: String::new(),
                    private: false,
                    members: members.iter().map(|m| m.to_string()).collect(),
                    join: false,
                },
                &planner,
                1,
            )
        };
        let refused = create(&mut core, "a", &["ghost"]).unwrap_err();
        assert!(matches!(refused, Error::NotFound(_)), "{refused}");
        create(&mut core, "b", &["ci", "operator"]).unwrap();
        assert!(matches!(
            core.join("b", "ghost", None, &planner, 2),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            core.post(
                PostRequest {
                    room: "b".into(),
                    ..ask(&["ghost"], &[])
                },
                &planner,
                3
            ),
            Err(Error::NotFound(_))
        ));
        assert!(!core.store.agents.contains_key("ghost"));
        let strict = SendRequest {
            from: "planner".into(),
            to: Some("ci".into()),
            body: "build 7 is ready".into(),
            reply_to: None,
            key: None,
            strict: true,
            admin: false,
        };
        core.send(strict, 4)
            .expect("an agent with a token is known before it connects");
        let operator = operator();
        core.join("b", "ghost", None, &operator, 5).unwrap();
        assert!(core.store.agents.contains_key("ghost"));
    }

    #[test]
    fn a_room_holds_a_bounded_number_of_open_questions() {
        let mut core = core();
        room(&mut core, "planner", &["reviewer"]);
        for _ in 0..share(MAX_OPEN_QUESTIONS) {
            core.post(ask(&["reviewer"], &[]), &agent("planner"), 2)
                .unwrap();
        }
        assert!(
            matches!(
                core.post(ask(&["reviewer"], &[]), &agent("planner"), 2),
                Err(Error::Full(_))
            ),
            "one asker holds a quarter of the room's questions"
        );
        core.post(ask(&["planner"], &[]), &agent("reviewer"), 2)
            .unwrap();
        let operator = operator();
        let open = |core: &Core| {
            core.store.rooms["release"]
                .posts
                .values()
                .filter(|p| p.is_open_question())
                .count()
        };
        while open(&core) < MAX_OPEN_QUESTIONS {
            core.post(ask(&["reviewer"], &[]), &operator, 3).unwrap();
        }
        assert!(matches!(
            core.post(ask(&["reviewer"], &[]), &operator, 3),
            Err(Error::Full(_))
        ));
        assert_eq!(core.store.rooms["release"].waiting_for("reviewer"), 127);
    }

    #[test]
    fn one_sender_fills_at_most_a_quarter_of_an_inbox() {
        let mut core = core();
        core.limits.max_inbox = 8;
        let request = |from: &str, admin: bool| SendRequest {
            from: from.into(),
            to: Some("worker".into()),
            body: "task".into(),
            reply_to: None,
            key: None,
            strict: false,
            admin,
        };
        for _ in 0..2 {
            core.send(request("flood", false), 1).unwrap();
        }
        assert!(matches!(
            core.send(request("flood", false), 1),
            Err(Error::Full(_))
        ));
        core.send(request("planner", false), 1).unwrap();
        let leased = core.lease("worker", "w", 1, 2).unwrap();
        assert_eq!(leased[0].message.from, "flood");
        let receipts = [leased[0].receipt.clone()];
        core.ack("worker", &receipts, None, 3).unwrap();
        core.send(request("flood", false), 4)
            .expect("an acknowledged message frees its sender's share");
        for _ in 0..5 {
            core.send(request("operator", true), 5).unwrap();
        }
        assert!(
            matches!(core.send(request("operator", true), 5), Err(Error::Full(_))),
            "the inbox limit holds for everyone"
        );
    }

    #[test]
    fn a_flood_of_broadcasts_pushes_out_only_its_own() {
        let mut core = core();
        core.limits.max_broadcasts = 8;
        core.broadcast("planner", "release at noon".into(), None, 1)
            .unwrap();
        for i in 0..20 {
            core.broadcast("flood", format!("spam {i}"), None, 2)
                .unwrap();
        }
        let kept: Vec<&str> = core
            .store
            .broadcasts
            .values()
            .map(|b| b.from.as_str())
            .collect();
        assert_eq!(kept.len(), 8);
        assert_eq!(kept[0], "planner", "{kept:?}");
    }

    #[test]
    fn an_agent_creates_at_most_a_quarter_of_the_rooms() {
        let mut core = core();
        core.limits.max_rooms = 8;
        let create = |core: &mut Core, name: &str, who: &Actor| {
            core.create_room(
                CreateRoom {
                    name: name.into(),
                    topic: String::new(),
                    private: false,
                    members: Vec::new(),
                    join: false,
                },
                who,
                1,
            )
        };
        let planner = agent("planner");
        create(&mut core, "a", &planner).unwrap();
        create(&mut core, "b", &planner).unwrap();
        assert!(matches!(
            create(&mut core, "c", &planner),
            Err(Error::Full(_))
        ));
        core.update_room("a", None, Some(true), &planner, 2)
            .unwrap();
        create(&mut core, "c", &planner).expect("closed rooms do not count");
        create(&mut core, "d", &agent("builder")).unwrap();
        let operator = operator();
        for name in ["e", "f", "g"] {
            create(&mut core, name, &operator).unwrap();
        }
    }

    #[test]
    fn a_reply_to_an_unseen_message_starts_its_own_thread() {
        let mut core = core();
        let private = send(&mut core, "alice", "bob", "the plan", 1);
        let reply = SendRequest {
            from: "mallory".into(),
            to: Some("bob".into()),
            body: "about that plan".into(),
            reply_to: Some(private.id),
            key: None,
            strict: true,
            admin: false,
        };
        let (message, _) = core.send(reply, 2).unwrap();
        assert_eq!(message.thread, message.id);
    }

    #[test]
    fn a_name_stays_in_use_while_its_messages_remain() {
        let mut core = core();
        send(&mut core, "bob", "alice", "hello", 1);
        core.forget_agent("bob", 2).unwrap();
        assert!(!core.known_agent("bob"));
        assert!(core.name_in_use("bob"), "alice could still reply to bob");
        core.broadcast("carol", "hi all".into(), None, 3).unwrap();
        core.forget_agent("carol", 4).unwrap();
        assert!(core.name_in_use("carol"));
        assert!(!core.name_in_use("dave"));
    }

    #[test]
    fn board_writes_are_versioned() {
        let mut core = core();
        room(&mut core, "planner", &["builder"]);
        let planner = agent("planner");
        let first = core
            .board_set("release", "plan", Some("v1".into()), Some(0), &planner, 2)
            .unwrap();
        assert_eq!(first.unwrap().version, 1);
        let stale = core.board_set(
            "release",
            "plan",
            Some("v2".into()),
            Some(0),
            &agent("builder"),
            3,
        );
        assert!(matches!(stale, Err(Error::Conflict(_))));
        core.board_set(
            "release",
            "plan",
            Some("v2".into()),
            Some(1),
            &agent("builder"),
            3,
        )
        .unwrap();
        assert!(matches!(
            core.board_set(
                "release",
                "plan",
                Some("x".into()),
                None,
                &agent("outsider"),
                4
            ),
            Err(Error::Forbidden(_))
        ));
        assert_eq!(core.board_json("release").unwrap()[0]["by"], "builder");
        core.board_set("release", "plan", None, None, &planner, 5)
            .unwrap();
        assert!(core.board_json("release").unwrap().is_empty());
    }

    #[test]
    fn rename_carries_room_membership_and_authorship() {
        let mut core = core();
        room(&mut core, "agent-1", &[]);
        core.post(note("hello"), &agent("agent-1"), 2).unwrap();
        core.rename("agent-1", "planner", 3).unwrap();
        let record = &core.store.rooms["release"];
        assert!(record.is_owner("planner"));
        assert!(record.posts.values().all(|p| p.from == "planner"));
        assert_eq!(
            core.cleanup(4).unwrap(),
            Vec::<String>::new(),
            "room members are not pruned"
        );
    }

    #[test]
    fn rooms_survive_replay_and_snapshots() {
        let dir = std::env::temp_dir().join(format!(
            "postino-rooms-{}-{}",
            std::process::id(),
            random_hex(4)
        ));
        let reopen = || {
            let (journal, store) = Journal::open(&dir, Fsync::Always).unwrap();
            Core::new(store, Limits::default(), Some(journal))
        };
        let question = {
            let mut core = reopen();
            room(&mut core, "planner", &["builder"]);
            core.board_set(
                "release",
                "plan",
                Some("v1".into()),
                None,
                &agent("planner"),
                2,
            )
            .unwrap();
            let (question, _) = core
                .post(
                    PostRequest {
                        key: Some("q1".into()),
                        ..ask(&["builder"], &[])
                    },
                    &agent("planner"),
                    3,
                )
                .unwrap();
            core.read_room("release", "builder", 1).unwrap();
            question.id
        };
        let check = |core: &mut Core| {
            let record = &core.store.rooms["release"];
            assert_eq!(record.board["plan"].value, "v1");
            assert!(record.posts[&question].is_open_question());
            assert_eq!(record.unread("builder"), 2);
            let (again, created) = core
                .post(
                    PostRequest {
                        key: Some("q1".into()),
                        ..ask(&["builder"], &[])
                    },
                    &agent("planner"),
                    4,
                )
                .unwrap();
            assert!(!created && again.id == question);
        };
        let mut core = reopen();
        check(&mut core);
        core.journal
            .as_mut()
            .unwrap()
            .rewrite(&mut core.store)
            .unwrap();
        drop(core);
        check(&mut reopen());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn subscribers_replay_after_last_event_id() {
        let mut core = core();
        let (_, feed, _) = core.subscribe(None, Some("b".into()));
        send(&mut core, "a", "b", "x", 1);
        send(&mut core, "a", "c", "y", 1);
        let events: Vec<Arc<Event>> = feed.rx.try_iter().collect();
        assert!(
            events
                .iter()
                .all(|e| e.scope.as_ref().unwrap().iter().any(|a| a == "b"))
        );
        assert!(
            events
                .iter()
                .any(|e| e.json.contains("\"type\":\"message\""))
        );
        assert!(!events.iter().any(|e| e.json.contains("\"to\":\"c\"")));
        let first = &events[0];
        let (replay, _, resync) = core.subscribe(Some(first.id), None);
        assert!(!resync);
        assert!(replay.iter().all(|e| e.id > first.id));
        let (_, _, resync) = core.subscribe(Some(1), None);
        assert!(resync);
        let (_, _, resync) = core.subscribe(Some(u64::MAX), None);
        assert!(resync, "an id from the future reads as a gap");
    }

    /// What waits for a subscription, and whether it is still subscribed.
    fn drain(deliveries: &Deliveries) -> (Vec<Arc<Delivery>>, bool) {
        let mut taken = Vec::new();
        loop {
            match deliveries.recv_timeout(Duration::ZERO) {
                Ok(delivery) => taken.push(delivery),
                Err(RecvTimeoutError::Timeout) => return (taken, true),
                Err(RecvTimeoutError::Disconnected) => return (taken, false),
            }
        }
    }

    #[test]
    fn a_message_is_copied_once_for_every_subscription_it_reaches() {
        let mut core = core();
        let patterns: Vec<String> = (1..=64).map(|n| "*".repeat(n)).collect();
        let wide = core
            .subscribe_channels(vec!["news".into()], patterns, Scope::everything())
            .unwrap();
        let one = core
            .subscribe_channels(vec!["news".into()], Vec::new(), Scope::everything())
            .unwrap();
        assert_eq!(core.publish("news", &"x".repeat(1000)), 66);
        let (mut taken, _) = drain(&wide);
        assert_eq!(taken.len(), 65, "the channel, then each pattern");
        taken.extend(drain(&one).0);
        assert!(
            taken
                .iter()
                .all(|d| Arc::ptr_eq(&d.message, &taken[0].message)
                    && Arc::ptr_eq(&d.channel, &taken[0].channel))
        );
    }

    #[test]
    fn a_subscriber_that_falls_behind_is_dropped_before_it_holds_back_more_than_4_mib() {
        let mut core = core();
        let subscribe = |core: &mut Core| {
            core.subscribe_channels(vec!["news".into()], Vec::new(), Scope::everything())
                .unwrap()
        };
        let message = "x".repeat(64 << 10);
        let size = "news".len() + message.len();

        // One that reads nothing is sent messages until the next would pass
        // the bytes it may have waiting, then dropped.
        let idle = subscribe(&mut core);
        let sent = (0..100)
            .take_while(|_| core.publish("news", &message) == 1)
            .count();
        assert!(sent * size <= SUBSCRIBER_BYTES && (sent + 1) * size > SUBSCRIBER_BYTES);
        assert!(sent < SUBSCRIBER_BUFFER, "bytes, not the count, bound it");
        let (taken, subscribed) = drain(&idle);
        assert_eq!(taken.len(), sent, "what it was sent, it gets");
        assert!(!subscribed);

        // One that keeps up is never dropped.
        let reader = subscribe(&mut core);
        for _ in 0..200 {
            assert_eq!(core.publish("news", &message), 1);
            assert_eq!(drain(&reader).0.len(), 1);
        }

        // A message larger than that still reaches one with nothing waiting.
        let large = "x".repeat(SUBSCRIBER_BYTES + 1);
        assert_eq!(core.publish("news", &large), 1);
        assert_eq!(core.publish("news", &message), 0);
        let (taken, subscribed) = drain(&reader);
        assert_eq!(taken.len(), 1);
        assert!(!subscribed);
    }

    #[test]
    fn only_subscriptions_that_can_name_a_key_change_hear_of_one() {
        let hears = |channels: &[&str], patterns: &[&str]| {
            let channels: Vec<String> = channels.iter().map(|c| c.to_string()).collect();
            let patterns: Vec<Glob> = patterns.iter().map(|p| Glob::new(p)).collect();
            hears_keys(&channels, &patterns)
        };
        assert!(hears(&["__keyspace__:task"], &[]));
        assert!(hears(&[], &["*"]));
        assert!(hears(&[], &["?_keyspace__:*"]));
        assert!(hears(&[], &["__k*"]));
        assert!(hears(&[], &["__keyevent__:[sd]*"]));
        assert!(!hears(&["news", "__ke"], &[]));
        assert!(!hears(&[], &["news.*", "_x*", "__kez*"]));

        let mut core = core();
        let news = core
            .subscribe_channels(Vec::new(), vec!["news.*".into()], Scope::everything())
            .unwrap();
        let all = core
            .subscribe_channels(Vec::new(), vec!["*".into()], Scope::everything())
            .unwrap();
        let set = ["SET", "k", "v"].map(String::from);
        core.command(&set, &Access::admin(), false, 1).unwrap();
        assert_eq!(core.publish("news.today", "hello"), 2);
        let heard = |deliveries: &Deliveries| -> Vec<String> {
            drain(deliveries)
                .0
                .iter()
                .map(|d| d.channel.to_string())
                .collect()
        };
        assert_eq!(heard(&news), ["news.today"]);
        assert_eq!(
            heard(&all),
            ["__keyspace__:k", "__keyevent__:set", "news.today"]
        );
    }

    #[test]
    fn subscribers_that_left_do_not_pile_up_while_nothing_comes_to_them() {
        let mut core = core();
        for _ in 0..1000 {
            drop(core.subscribe_channels(vec!["news".into()], Vec::new(), Scope::everything()));
            drop(core.subscribe(None, None));
        }
        let kept = core
            .subscribe_channels(vec!["news".into()], Vec::new(), Scope::everything())
            .unwrap();
        let (_, feed, _) = core.subscribe(None, None);
        assert_eq!(core.channels.len(), 1);
        assert_eq!(core.subscriber_count(), 1);
        assert_eq!(core.publish("news", "still heard"), 1);
        assert_eq!(&*drain(&kept).0[0].message, "still heard");
        drop(feed);
    }

    #[test]
    fn writes_wait_for_a_journal_that_cannot_be_written() {
        let dir = crate::journal::tests::temp_dir("broker-full");
        let run = |core: &mut Core, words: &[&str]| -> Result<Reply> {
            let argv: Vec<String> = words.iter().map(|w| w.to_string()).collect();
            core.command(&argv, &Access::admin(), false, 1)
                .map(|out| out.reply)
        };
        {
            let (journal, store) = Journal::open(&dir, Fsync::Always).unwrap();
            let mut core = Core::new(store, Limits::default(), Some(journal));
            run(&mut core, &["SET", "a", "1"]).unwrap();
            core.journal.as_mut().unwrap().fail_writes_after(Some(3));
            let unsaved = run(&mut core, &["SET", "b", "2"]).unwrap_err();
            assert!(
                matches!(&unsaved, Error::Unsaved(e, reply) if e.starts_with("IOERR") && **reply == Reply::ok()),
                "the change is made, but not yet on disk: {unsaved:?}"
            );
            let refused = run(&mut core, &["SET", "c", "3"]).unwrap();
            assert!(
                matches!(&refused, Reply::Error(e) if e.starts_with("IOERR")),
                "{refused:?}"
            );
            assert_eq!(
                run(&mut core, &["GET", "b"]).unwrap(),
                Reply::Bulk("2".into())
            );
            core.journal.as_mut().unwrap().fail_writes_after(None);
            assert_eq!(run(&mut core, &["SET", "c", "3"]).unwrap(), Reply::ok());
        }
        let (_, store) = Journal::open(&dir, Fsync::Always).unwrap();
        let mut core = Core::new(store, Limits::default(), None);
        for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
            assert_eq!(
                run(&mut core, &["GET", key]).unwrap(),
                Reply::Bulk(value.into())
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn batched_changes_wait_for_a_sync_to_end_then_the_tick_writes_them() {
        let dir = crate::journal::tests::temp_dir("postponed-batch");
        let journaled = |key: &str| {
            let line = format!(r#"["SET","{key}","v"]"#);
            std::fs::read_dir(&dir).unwrap().any(|entry| {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                name.starts_with("journal")
                    && std::fs::read_to_string(&path).unwrap().contains(&line)
            })
        };
        let storage = Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::EverySec,
        };
        let broker = Broker::open(Limits::default(), storage).unwrap();
        let syncs = broker.run(|core, _| core.journal.as_ref().unwrap().syncs());
        syncs.run(|| {
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    batch_journal_writes();
                    let argv = ["SET".to_string(), "a".to_string(), "v".to_string()];
                    let set = broker.command(&argv, &Access::admin(), Duration::ZERO, &|| true);
                    assert_eq!(set.unwrap(), Reply::ok());
                    broker.flush_journal();
                    assert!(!journaled("a"), "the write waits for the sync");
                });
            });
        });
        assert!(
            broker.deferred.load(Ordering::Acquire),
            "still to be written"
        );
        broker.tick();
        assert!(journaled("a"));
        assert!(!broker.deferred.load(Ordering::Acquire));
        drop(broker);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn run_as_admin(broker: &Broker, words: &[&str]) -> Reply {
        let argv: Vec<String> = words.iter().map(|w| w.to_string()).collect();
        broker
            .command(&argv, &Access::admin(), Duration::ZERO, &|| true)
            .unwrap()
    }

    #[test]
    fn a_round_the_disk_refuses_waits_and_is_written_once_it_can() {
        let dir = crate::journal::tests::temp_dir("refused-round");
        let storage = || Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::EverySec,
        };
        let broker = Broker::open(Limits::default(), storage()).unwrap();
        let fail_after =
            |n| broker.run(|core, _| core.journal.as_mut().unwrap().fail_writes_after(n));
        std::thread::scope(|scope| {
            scope.spawn(|| {
                batch_journal_writes();
                fail_after(Some(3));
                assert_eq!(run_as_admin(&broker, &["SET", "a", "1"]), Reply::ok());
                assert_eq!(run_as_admin(&broker, &["APPEND", "a", "2"]), Reply::Int(2));
                broker.flush_journal();
                let refused = run_as_admin(&broker, &["SET", "b", "1"]);
                assert!(
                    matches!(&refused, Reply::Error(e) if e.starts_with("IOERR")),
                    "{refused:?}"
                );
                assert_eq!(
                    run_as_admin(&broker, &["GET", "a"]),
                    Reply::Bulk("12".into()),
                    "the changes refused are made"
                );
                fail_after(None);
                assert_eq!(run_as_admin(&broker, &["APPEND", "a", "3"]), Reply::Int(3));
                broker.flush_journal();
            });
        });
        drop(broker);
        let broker = Broker::open(Limits::default(), storage()).unwrap();
        assert_eq!(
            run_as_admin(&broker, &["GET", "a"]),
            Reply::Bulk("123".into()),
            "written once the disk takes them, in the order they were made"
        );
        assert_eq!(run_as_admin(&broker, &["EXISTS", "b"]), Reply::Int(0));
        drop(broker);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn writes_go_on_once_the_disk_takes_them_after_one_it_refused_before_it_was_made() {
        let dir = crate::journal::tests::temp_dir("refused-unmade");
        let (journal, store) = Journal::open(&dir, Fsync::EverySec).unwrap();
        let mut core = Core::new(store, Limits::default(), Some(journal));
        core.journal.as_mut().unwrap().fail_writes_after(Some(3));
        let request = SendRequest {
            from: "bob".into(),
            to: Some("alice".into()),
            body: "hello".into(),
            reply_to: None,
            key: None,
            strict: false,
            admin: true,
        };
        assert!(core.send(request, 1).is_err(), "refused, so not made");
        core.journal.as_mut().unwrap().fail_writes_after(None);
        let argv = ["SET".to_string(), "a".into(), "1".into()];
        let set = core.command(&argv, &Access::admin(), false, 1).unwrap();
        assert_eq!(set.reply, Reply::ok(), "nothing waited for the disk");
        drop(core);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_reply_waits_for_its_changes_while_another_thread_writes_them() {
        let dir = crate::journal::tests::temp_dir("written-elsewhere");
        let storage = Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::EverySec,
        };
        let broker = Broker::open(Limits::default(), storage).unwrap();
        let journaled = |key: &str| {
            std::fs::read_to_string(dir.join("journal.log"))
                .unwrap()
                .contains(&format!(r#"["SET","{key}","v"]"#))
        };
        let stall = Some(Duration::from_millis(300));
        broker.run(|core, _| core.journal.as_mut().unwrap().stall_writes(stall));
        let (set, theirs) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let (broker, journaled) = (&broker, &journaled);
            scope.spawn(move || {
                batch_journal_writes();
                assert_eq!(run_as_admin(broker, &["SET", "mine", "v"]), Reply::ok());
                set.send(()).unwrap();
                // Another thread's round takes this change along to the disk.
                while broker.deferred.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                broker.flush_journal();
                assert!(journaled("mine"), "the reply waits for the write");
            });
            scope.spawn(move || {
                batch_journal_writes();
                theirs.recv().unwrap();
                assert_eq!(run_as_admin(broker, &["SET", "theirs", "v"]), Reply::ok());
                broker.flush_journal();
                assert!(journaled("theirs"));
            });
        });
        drop(broker);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rounds_on_many_threads_are_journaled_in_the_order_they_ran() {
        let dir = crate::journal::tests::temp_dir("concurrent-rounds");
        let storage = || Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::EverySec,
        };
        let values = |broker: &Broker| -> Vec<Reply> {
            (0..4)
                .map(|k| run_as_admin(broker, &["GET", &format!("k{k}")]))
                .collect()
        };
        let broker = Broker::open(Limits::default(), storage()).unwrap();
        std::thread::scope(|scope| {
            for thread in 0..4 {
                let broker = &broker;
                scope.spawn(move || {
                    batch_journal_writes();
                    for round in 0..200 {
                        for i in 0..3 {
                            let key = format!("k{}", (round + i) % 4);
                            let piece = format!("{thread}.{round}.{i};");
                            run_as_admin(broker, &["APPEND", &key, &piece]);
                        }
                        broker.flush_journal();
                    }
                });
            }
        });
        let live = values(&broker);
        drop(broker);
        let broker = Broker::open(Limits::default(), storage()).unwrap();
        assert_eq!(values(&broker), live, "replayed in the order they ran");
        drop(broker);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn changes_wait_for_a_disk_that_cannot_sync_then_are_written_again() {
        let dir = crate::journal::tests::temp_dir("broker-unsynced");
        let set = |broker: &Broker, key: &str| {
            let argv = ["SET".to_string(), key.to_string(), "v".to_string()];
            broker
                .command(&argv, &Access::admin(), Duration::ZERO, &|| true)
                .unwrap()
        };
        let storage = Storage::Disk {
            dir: dir.clone(),
            fsync: Fsync::EverySec,
        };
        let broker = Broker::open(Limits::default(), storage).unwrap();
        assert_eq!(set(&broker, "a"), Reply::ok());
        broker.run(|core, _| core.journal.as_mut().unwrap().fail_syncs(true));
        broker.tick();
        let refused = set(&broker, "b");
        assert!(
            matches!(&refused, Reply::Error(e) if e.starts_with("IOERR") && e.contains("sync")),
            "{refused:?}"
        );
        broker.run(|core, _| core.journal.as_mut().unwrap().fail_syncs(false));
        broker.tick();
        assert!(!dir.join("snapshot.json").exists());
        assert_eq!(set(&broker, "b"), Reply::ok());
        // What the failed sync was to sync is written again, in a snapshot.
        let deadline = Instant::now() + Duration::from_secs(20);
        while !dir.join("snapshot.json").exists() {
            assert!(Instant::now() < deadline, "no snapshot was written");
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(broker);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_pop_the_journal_cannot_take_yet_still_hands_over_its_value() {
        let dir = crate::journal::tests::temp_dir("unsaved-pop");
        let argv =
            |words: &[&str]| -> Vec<String> { words.iter().map(|w| w.to_string()).collect() };
        let (journal, store) = Journal::open(&dir, Fsync::Always).unwrap();
        let mut core = Core::new(store, Limits::default(), Some(journal));
        let admin = Access::admin();
        core.command(
            &argv(&["RPUSH", "jobs", "job-1", "job-2", "job-3"]),
            &admin,
            false,
            1,
        )
        .unwrap();
        core.journal.as_mut().unwrap().fail_writes_after(Some(3));
        let popped = core.command(&argv(&["LPOP", "jobs"]), &admin, false, 1);
        assert!(
            matches!(&popped, Err(Error::Unsaved(_, reply)) if **reply == Reply::Bulk("job-1".into())),
            "{:?}",
            popped.err()
        );
        // The disk takes writes again, then fills up during a batch.
        core.journal.as_mut().unwrap().fail_writes_after(None);
        core.journal.as_mut().unwrap().flush_pending().unwrap();
        core.journal.as_mut().unwrap().fail_writes_after(Some(3));
        let batch = [argv(&["LPOP", "jobs"]), argv(&["LLEN", "jobs"])];
        let popped = core.multi(&batch, &admin, 1);
        assert!(
            matches!(&popped, Err(Error::Unsaved(_, reply)) if matches!(&**reply, Reply::Array(r) if r[0] == Reply::Bulk("job-2".into()))),
            "a batch hands back its replies too: {:?}",
            popped.err()
        );
        drop(core);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_name_that_changes_hands_does_not_bring_its_past_along() {
        let mut core = core();
        let (_, before, _) = core.subscribe(None, Some("alice".into()));
        send(&mut core, "bob", "alice", "the password is hunter2", 1);
        let start = before.rx.try_iter().map(|e| e.id).min().unwrap() - 1;
        core.rename("alice", "alice2", 2).unwrap();
        assert!(
            before.rx.recv().is_ok_and(|e| e.json.contains("rename")) && before.rx.recv().is_err(),
            "the old stream hears of the rename, then ends"
        );
        core.ensure_agents(&["mallory"], 3).unwrap();
        core.rename("mallory", "alice", 3).unwrap();
        let (replay, _, resync) = core.subscribe(Some(start), Some("alice".into()));
        assert!(!resync);
        assert!(
            replay.iter().all(|e| !e.json.contains("hunter2")),
            "the new alice does not get the old alice's mail: {:?}",
            replay.iter().map(|e| &e.json).collect::<Vec<_>>()
        );
        core.forget("alice", 4).unwrap();
        let (replay, _, _) = core.subscribe(Some(start), Some("alice".into()));
        assert!(replay.is_empty(), "nor does anyone who takes it later");
    }
}
