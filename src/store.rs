//! In-memory state of the forum: inboxes, broadcasts, rooms, and the keyspace.
//!
//! Every inbox is an ordered stream of entries with `ms-seq` ids. Consumers lease
//! entries into a pending list with a delivery counter, and acknowledged entries
//! stay in the stream as history until trimmed.
//! All mutations go through [`Store::apply`] with an [`Effect`], which is also the
//! unit written to the journal, so live operation and crash recovery share one path.
//!
//! Rooms are shared streams for the agents of one workflow: every member reads
//! every post and keeps its own cursor. Questions are posts that wait for
//! answers from named members, and the board is a small versioned key-value map
//! the members share. The keyspace ([`crate::db`]) is persisted the same way:
//! each write command is journaled with its timestamp and replayed on start.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::capture::Captured;
use crate::commands::Ctx;
use crate::db::{Db, Entry as KeyEntry};
use crate::id::Id;
use crate::util::iso8601;

#[derive(Clone, Debug)]
pub struct Limits {
    pub msg_ttl_ms: u64,
    pub lease_ms: u64,
    pub presence_ttl_ms: u64,
    pub max_inbox: usize,
    pub max_agents: usize,
    pub max_broadcasts: usize,
    pub history_limit: usize,
    pub max_deliveries: u32,
    pub max_rooms: usize,
    pub room_history: usize,
    pub max_keys: usize,
    pub max_elements: usize,
    /// Bytes the process may hold before writes that grow it are refused; 0
    /// means no limit.
    pub max_memory: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            msg_ttl_ms: 86_400_000,
            lease_ms: 30_000,
            presence_ttl_ms: 30_000,
            max_inbox: 1000,
            max_agents: 1000,
            max_broadcasts: 500,
            history_limit: 200,
            max_deliveries: 10,
            max_rooms: 200,
            room_history: 1000,
            max_keys: 1_000_000,
            max_elements: 1_000_000,
            max_memory: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Queued,
    Leased,
    Acked,
    Dead,
    Expired,
}

impl State {
    pub fn is_pending(self) -> bool {
        matches!(self, State::Queued | State::Leased)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lease {
    pub consumer: String,
    pub token: String,
    pub at: u64,
    pub until: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: Id,
    pub from: String,
    pub to: String,
    pub body: String,
    pub created: u64,
    pub expires: u64,
    pub thread: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub state: State,
    #[serde(default)]
    pub deliveries: u32,
    /// The most recent delivery. Kept after settlement so a late acknowledgement
    /// for a requeued entry still matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<Lease>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Broadcast {
    pub id: Id,
    pub from: String,
    pub body: String,
    pub created: u64,
    pub expires: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Agent {
    pub name: String,
    pub created: u64,
    #[serde(default)]
    pub last_seen: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Id>,
    /// Ids of entries addressed to this agent; rebuilt from messages on load.
    #[serde(skip)]
    pub inbox: BTreeSet<Id>,
    /// The queued and the settled part of the inbox, and its counts, kept in
    /// step with every change so no operation has to scan the inbox.
    #[serde(skip)]
    queued: BTreeSet<Id>,
    #[serde(skip)]
    settled: BTreeSet<Id>,
    #[serde(skip)]
    counts: Counts,
    /// Pending messages in the inbox, by sender.
    #[serde(skip)]
    senders: BTreeMap<String, usize>,
}

impl Agent {
    fn new(name: &str, at: u64) -> Self {
        Agent {
            name: name.to_string(),
            created: at,
            last_seen: at,
            cursor: None,
            inbox: BTreeSet::new(),
            queued: BTreeSet::new(),
            settled: BTreeSet::new(),
            counts: Counts::default(),
            senders: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Owner,
    Member,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Member {
    pub role: Role,
    pub joined: u64,
    /// The newest post this member has read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Id>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Note,
    Question,
    Answer,
    /// Written by the broker when the room changes: joins, topic, board.
    Event,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Open,
    Resolved,
    Withdrawn,
    Expired,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ask {
    /// Members who must answer; empty means any member but the asker.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    pub expires: u64,
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Post {
    pub id: Id,
    pub from: String,
    pub kind: Kind,
    pub body: String,
    pub created: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<Id>,
    /// What the post concerns (a task, a file, a ticket), so whoever picks that
    /// item up later can find every decision about it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub choice: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask: Option<Ask>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

impl Post {
    pub fn is_open_question(&self) -> bool {
        self.ask.as_ref().is_some_and(|a| a.status == Status::Open)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub value: String,
    pub by: String,
    pub at: u64,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Room {
    pub name: String,
    #[serde(default)]
    pub topic: String,
    pub created: u64,
    pub created_by: String,
    /// Private rooms are joined by invitation from an owner.
    #[serde(default)]
    pub private: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed: Option<u64>,
    #[serde(default)]
    pub members: BTreeMap<String, Member>,
    #[serde(default)]
    pub board: BTreeMap<String, Entry>,
    #[serde(default)]
    pub posts: BTreeMap<Id, Post>,
    /// Ids of the questions still open, kept by [`Store::apply`].
    #[serde(skip)]
    pub open: BTreeSet<Id>,
}

impl Room {
    /// The questions still open, oldest first.
    pub fn open_questions(&self) -> impl Iterator<Item = &Post> {
        self.open.iter().filter_map(|id| self.posts.get(id))
    }

    pub fn is_open(&self) -> bool {
        self.closed.is_none()
    }

    pub fn is_owner(&self, agent: &str) -> bool {
        self.members
            .get(agent)
            .is_some_and(|m| m.role == Role::Owner)
    }

    pub fn answers<'a>(&'a self, question: Id) -> impl Iterator<Item = &'a Post> + 'a {
        self.posts
            .range(question..)
            .map(|(_, p)| p)
            .filter(move |p| p.kind == Kind::Answer && p.reply_to == Some(question))
    }

    /// Addressees of an open question who have not answered yet.
    pub fn waiting_on<'a>(&'a self, question: &'a Post) -> Vec<&'a str> {
        let Some(ask) = &question.ask else {
            return Vec::new();
        };
        let answered: HashSet<&str> = self.answers(question.id).map(|a| a.from.as_str()).collect();
        ask.to
            .iter()
            .map(String::as_str)
            .filter(|name| !answered.contains(name))
            .collect()
    }

    pub fn unread(&self, agent: &str) -> usize {
        let cursor = self.members.get(agent).and_then(|m| m.cursor);
        let range = match cursor {
            Some(c) => self.posts.range((Excluded(c), Unbounded)),
            None => self.posts.range(..),
        };
        range.filter(|(_, p)| p.from != agent).count()
    }

    /// Open questions that name `agent` and still wait for its answer, in one
    /// pass over the posts.
    pub fn waiting_for(&self, agent: &str) -> usize {
        let answered: HashSet<Id> = self
            .posts
            .values()
            .filter(|p| p.kind == Kind::Answer && p.from == agent)
            .filter_map(|p| p.reply_to)
            .collect();
        self.posts
            .values()
            .filter(|p| {
                p.ask
                    .as_ref()
                    .is_some_and(|a| a.status == Status::Open && a.to.iter().any(|n| n == agent))
                    && !answered.contains(&p.id)
            })
            .count()
    }

    /// Whether a trim must keep `post`: an open question, or an answer to one,
    /// which its question still counts.
    pub fn keeps(&self, post: &Post) -> bool {
        post.is_open_question()
            || (post.kind == Kind::Answer
                && post
                    .reply_to
                    .and_then(|q| self.posts.get(&q))
                    .is_some_and(Post::is_open_question))
    }
}

/// Presence session. Ephemeral like a key with a TTL: never journaled,
/// re-created by the next heartbeat after a broker restart.
#[derive(Clone, Debug)]
pub struct Session {
    pub agent: String,
    pub client: String,
    pub started: u64,
    pub expires: u64,
    pub pid: Option<u32>,
    pub host: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Effect {
    Agent {
        name: String,
        at: u64,
    },
    Message {
        message: Message,
    },
    Lease {
        id: Id,
        consumer: String,
        token: String,
        at: u64,
        until: u64,
    },
    Settle {
        id: Id,
        state: State,
        at: u64,
    },
    Drop {
        ids: Vec<Id>,
    },
    Broadcast {
        broadcast: Broadcast,
    },
    DropBroadcasts {
        ids: Vec<Id>,
    },
    Cursor {
        agent: String,
        id: Id,
    },
    Rename {
        from: String,
        to: String,
    },
    Forget {
        agent: String,
    },
    Room {
        room: Room,
    },
    Member {
        room: String,
        agent: String,
        member: Option<Member>,
    },
    RoomCursor {
        room: String,
        agent: String,
        id: Id,
    },
    Post {
        room: String,
        post: Post,
    },
    Question {
        room: String,
        id: Id,
        status: Status,
        at: u64,
    },
    DropPosts {
        room: String,
        ids: Vec<Id>,
    },
    Topic {
        room: String,
        topic: String,
    },
    Close {
        room: String,
        at: Option<u64>,
    },
    Board {
        room: String,
        key: String,
        entry: Option<Entry>,
    },
    DropRoom {
        room: String,
    },
    /// Write commands against the keyspace, replayed with the time they ran.
    Db {
        at: u64,
        commands: Vec<Vec<String>>,
    },
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counts {
    pub queued: usize,
    pub leased: usize,
    pub dead: usize,
    pub settled: usize,
}

impl Counts {
    pub fn pending(&self) -> usize {
        self.queued + self.leased
    }
}

/// When messages fall due, soonest first, so the sweep visits only those.
#[derive(Default)]
struct Due {
    /// Leased messages by lease end.
    leases: BTreeSet<(u64, Id)>,
    /// Pending messages by expiry.
    expiry: BTreeSet<(u64, Id)>,
    /// Settled messages by settlement time.
    settled: BTreeSet<(u64, Id)>,
}

/// Adds (or, with `add` false, removes) what one message contributes to its
/// inbox's sets and counts and to the due indexes. Every change to a message
/// removes its old contribution and adds its new one.
fn track(agents: &mut BTreeMap<String, Agent>, due: &mut Due, message: &Message, add: bool) {
    let id = message.id;
    let flip = |set: &mut BTreeSet<(u64, Id)>, at: u64| {
        if add {
            set.insert((at, id));
        } else {
            set.remove(&(at, id));
        }
    };
    match message.state {
        State::Leased => {
            if let Some(lease) = &message.lease {
                flip(&mut due.leases, lease.until);
            }
            flip(&mut due.expiry, message.expires);
        }
        State::Queued => flip(&mut due.expiry, message.expires),
        State::Acked | State::Dead | State::Expired => {
            flip(&mut due.settled, message.settled.unwrap_or(message.created));
        }
    }
    let Some(agent) = agents.get_mut(&message.to) else {
        return;
    };
    let counter = match message.state {
        State::Queued => &mut agent.counts.queued,
        State::Leased => &mut agent.counts.leased,
        State::Dead => &mut agent.counts.dead,
        State::Acked | State::Expired => &mut agent.counts.settled,
    };
    if add {
        *counter += 1;
    } else {
        *counter = counter.saturating_sub(1);
    }
    if message.state.is_pending() {
        let sent = agent.senders.entry(message.from.clone()).or_insert(0);
        if add {
            *sent += 1;
        } else {
            *sent = sent.saturating_sub(1);
            if *sent == 0 {
                agent.senders.remove(&message.from);
            }
        }
    }
    let set = match message.state {
        State::Queued => Some(&mut agent.queued),
        State::Leased => None,
        State::Acked | State::Dead | State::Expired => Some(&mut agent.settled),
    };
    if let Some(set) = set {
        if add {
            set.insert(id);
        } else {
            set.remove(&id);
        }
    }
}

#[derive(Default)]
pub struct Store {
    pub last_id: Id,
    pub messages: Captured<Id, Message>,
    pub agents: BTreeMap<String, Agent>,
    pub broadcasts: BTreeMap<Id, Broadcast>,
    pub sessions: HashMap<String, Session>,
    pub rooms: Captured<String, Room>,
    pub db: Db,
    // Idempotency keys belong to whoever used them: (inbox, sender, key),
    // (sender, key), and (room, author, key). One agent's key never matches
    // another agent's message or post.
    msg_keys: HashMap<(String, String, String), Id>,
    bc_keys: HashMap<(String, String), Id>,
    post_keys: HashMap<(String, String, String), Id>,
    due: Due,
}

#[derive(Serialize)]
pub struct SnapshotRef<'a> {
    pub last_id: Id,
    pub agents: Vec<&'a Agent>,
    pub broadcasts: Vec<&'a Broadcast>,
    /// These three are left out when captured a run at a time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rooms: Option<Vec<&'a Room>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<&'a Message>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<&'a BTreeMap<String, KeyEntry>>,
}

/// A snapshot file as read back: every field at the top level, so it is
/// read in one pass.
#[derive(Deserialize)]
pub struct SnapshotOwned {
    /// The file format.
    #[serde(default)]
    pub version: u32,
    /// The last journal line the snapshot covers.
    #[serde(default)]
    pub seq: u64,
    pub last_id: Id,
    pub agents: Vec<Agent>,
    pub messages: Vec<Message>,
    pub broadcasts: Vec<Broadcast>,
    #[serde(default)]
    pub rooms: Vec<Room>,
    #[serde(default, deserialize_with = "crate::db::sorted_keys")]
    pub keys: BTreeMap<String, KeyEntry>,
}

impl Store {
    pub fn snapshot(&self) -> SnapshotRef<'_> {
        SnapshotRef {
            rooms: Some(self.rooms.values().collect()),
            messages: Some(self.messages.values().collect()),
            keys: Some(self.db.entries()),
            ..self.head()
        }
    }

    /// What a snapshot writes at once: everything but the rooms, the
    /// messages, and the keyspace, which it captures a run at a time.
    pub fn head(&self) -> SnapshotRef<'_> {
        SnapshotRef {
            last_id: self.last_id,
            agents: self.agents.values().collect(),
            broadcasts: self.broadcasts.values().collect(),
            rooms: None,
            messages: None,
            keys: None,
        }
    }

    /// Starts a capture of the rooms, the messages, and the keyspace as they
    /// are now, in that order, after `head`, which holds what comes before
    /// them inside the same object. Returns its number.
    pub fn begin_capture(&mut self, mut head: Vec<u8>) -> u64 {
        head.extend_from_slice(b",\"rooms\":[");
        self.rooms.begin_capture(head);
        self.messages.begin_capture(b"],\"messages\":[".to_vec());
        self.db.begin_capture(b"],\"keys\":{".to_vec())
    }

    pub fn capturing(&self) -> bool {
        self.db.capturing()
    }

    /// Writes the next run of the capture, about `budget` bytes of it.
    /// Returns whether all of it is written.
    pub fn capture_step(&mut self, budget: usize) -> bool {
        self.rooms.capture_step(budget)
            && self.messages.capture_step(budget)
            && self.db.capture_step(budget)
    }

    /// The parts of the capture ready to be written out, in order: none of
    /// one before all of the one ahead of it.
    pub fn drain_capture(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let (mut parts, over) = self.rooms.drain_capture()?;
        if over {
            let (messages, over) = self.messages.drain_capture()?;
            parts.extend(messages);
            if over {
                parts.extend(self.db.drain_capture());
            }
        }
        Ok(parts)
    }

    /// Ends the capture, writing whatever it has not reached, and returns
    /// the rest of it, the keyspace closed.
    pub fn end_capture(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let mut parts = self.rooms.end_capture()?;
        parts.extend(self.messages.end_capture()?);
        parts.extend(self.db.end_capture()?);
        Ok(parts)
    }

    pub fn abandon_capture(&mut self) {
        self.rooms.abandon_capture();
        self.messages.abandon_capture();
        self.db.abandon_capture();
    }

    pub fn from_snapshot(snapshot: SnapshotOwned) -> Store {
        let mut store = Store {
            last_id: snapshot.last_id,
            ..Store::default()
        };
        for agent in snapshot.agents {
            store.agents.insert(agent.name.clone(), agent);
        }
        for message in snapshot.messages {
            store.apply(&Effect::Message { message });
        }
        for broadcast in snapshot.broadcasts {
            store.apply(&Effect::Broadcast { broadcast });
        }
        for room in snapshot.rooms {
            store.apply(&Effect::Room { room });
        }
        store.db = Db::load(snapshot.keys);
        store
    }

    pub fn next_id(&self, now: u64) -> Id {
        // Ids follow the clock, which never comes near the largest one.
        Id::next_after(self.last_id, now).unwrap_or(Id::MAX)
    }

    /// The message `from` sent to `to` under an idempotency key.
    pub fn message_by_key(&self, to: &str, from: &str, key: &str) -> Option<&Message> {
        self.msg_keys
            .get(&(to.to_string(), from.to_string(), key.to_string()))
            .and_then(|id| self.messages.get(id))
    }

    pub fn broadcast_by_key(&self, from: &str, key: &str) -> Option<&Broadcast> {
        self.bc_keys
            .get(&(from.to_string(), key.to_string()))
            .and_then(|id| self.broadcasts.get(id))
    }

    pub fn post_by_key(&self, room: &str, from: &str, key: &str) -> Option<&Post> {
        self.post_keys
            .get(&(room.to_string(), from.to_string(), key.to_string()))
            .and_then(|id| self.rooms.get(room)?.posts.get(id))
    }

    /// Agents that belong to at least one open room, so pruning keeps them.
    pub fn room_members(&self) -> BTreeSet<&str> {
        self.rooms
            .values()
            .filter(|room| room.is_open())
            .flat_map(|room| room.members.keys().map(String::as_str))
            .collect()
    }

    pub fn apply(&mut self, effect: &Effect) {
        match effect {
            Effect::Agent { name, at } => {
                self.agents
                    .entry(name.clone())
                    .or_insert_with(|| Agent::new(name, *at));
            }
            Effect::Message { message } => {
                self.last_id = self.last_id.max(message.id);
                if let Some(old) = self.messages.get(&message.id) {
                    track(&mut self.agents, &mut self.due, old, false);
                }
                self.agents
                    .entry(message.to.clone())
                    .or_insert_with(|| Agent::new(&message.to, message.created))
                    .inbox
                    .insert(message.id);
                if let Some(key) = &message.key {
                    self.msg_keys.insert(
                        (message.to.clone(), message.from.clone(), key.clone()),
                        message.id,
                    );
                }
                track(&mut self.agents, &mut self.due, message, true);
                self.messages.insert(message.id, message.clone());
            }
            Effect::Lease {
                id,
                consumer,
                token,
                at,
                until,
            } => {
                if let Some(message) = self.messages.get_mut(id) {
                    track(&mut self.agents, &mut self.due, message, false);
                    message.state = State::Leased;
                    message.deliveries += 1;
                    message.settled = None;
                    message.lease = Some(Lease {
                        consumer: consumer.clone(),
                        token: token.clone(),
                        at: *at,
                        until: *until,
                    });
                    track(&mut self.agents, &mut self.due, message, true);
                }
            }
            Effect::Settle { id, state, at } => {
                if let Some(message) = self.messages.get_mut(id) {
                    track(&mut self.agents, &mut self.due, message, false);
                    message.state = *state;
                    message.settled = (!state.is_pending()).then_some(*at);
                    track(&mut self.agents, &mut self.due, message, true);
                }
            }
            Effect::Drop { ids } => {
                for id in ids {
                    self.remove_message(id);
                }
            }
            Effect::Broadcast { broadcast } => {
                self.last_id = self.last_id.max(broadcast.id);
                if let Some(key) = &broadcast.key {
                    self.bc_keys
                        .insert((broadcast.from.clone(), key.clone()), broadcast.id);
                }
                self.broadcasts.insert(broadcast.id, broadcast.clone());
            }
            Effect::DropBroadcasts { ids } => {
                for id in ids {
                    if let Some(broadcast) = self.broadcasts.remove(id)
                        && let Some(key) = broadcast.key
                    {
                        self.bc_keys.remove(&(broadcast.from, key));
                    }
                }
            }
            Effect::Cursor { agent, id } => {
                if let Some(agent) = self.agents.get_mut(agent) {
                    agent.cursor = Some(*id);
                }
            }
            Effect::Rename { from, to } => self.rename(from, to),
            Effect::Forget { agent } => {
                if let Some(record) = self.agents.remove(agent) {
                    for id in &record.inbox {
                        self.remove_message(id);
                    }
                }
                self.sessions.retain(|_, session| &session.agent != agent);
                for room in self.rooms.values_mut() {
                    room.members.remove(agent);
                }
            }
            Effect::Room { room } => {
                if let Some(last) = room.posts.keys().next_back() {
                    self.last_id = self.last_id.max(*last);
                }
                for post in room.posts.values() {
                    if let Some(key) = &post.key {
                        self.post_keys
                            .insert((room.name.clone(), post.from.clone(), key.clone()), post.id);
                    }
                }
                let mut room = room.clone();
                room.open = room
                    .posts
                    .values()
                    .filter(|p| p.is_open_question())
                    .map(|p| p.id)
                    .collect();
                self.rooms.insert(room.name.clone(), room);
            }
            Effect::Member {
                room,
                agent,
                member,
            } => {
                if let Some(room) = self.rooms.get_mut(room) {
                    match member {
                        Some(member) => room.members.insert(agent.clone(), member.clone()),
                        None => room.members.remove(agent),
                    };
                }
            }
            Effect::RoomCursor { room, agent, id } => {
                if let Some(member) = self
                    .rooms
                    .get_mut(room)
                    .and_then(|r| r.members.get_mut(agent))
                {
                    member.cursor = Some(*id);
                }
            }
            Effect::Post { room, post } => {
                if let Some(record) = self.rooms.get_mut(room) {
                    self.last_id = self.last_id.max(post.id);
                    if let Some(key) = &post.key {
                        self.post_keys
                            .insert((room.clone(), post.from.clone(), key.clone()), post.id);
                    }
                    if post.is_open_question() {
                        record.open.insert(post.id);
                    }
                    record.posts.insert(post.id, post.clone());
                }
            }
            Effect::Question {
                room,
                id,
                status,
                at,
            } => {
                if let Some(record) = self.rooms.get_mut(room)
                    && let Some(ask) = record.posts.get_mut(id).and_then(|p| p.ask.as_mut())
                {
                    ask.status = *status;
                    ask.closed = (*status != Status::Open).then_some(*at);
                    if *status == Status::Open {
                        record.open.insert(*id);
                    } else {
                        record.open.remove(id);
                    }
                }
            }
            Effect::DropPosts { room, ids } => {
                if let Some(record) = self.rooms.get_mut(room) {
                    for id in ids {
                        record.open.remove(id);
                        if let Some(post) = record.posts.remove(id)
                            && let Some(key) = post.key
                        {
                            self.post_keys.remove(&(room.clone(), post.from, key));
                        }
                    }
                }
            }
            Effect::Topic { room, topic } => {
                if let Some(room) = self.rooms.get_mut(room) {
                    room.topic = topic.clone();
                }
            }
            Effect::Close { room, at } => {
                if let Some(room) = self.rooms.get_mut(room) {
                    room.closed = *at;
                }
            }
            Effect::Board { room, key, entry } => {
                if let Some(room) = self.rooms.get_mut(room) {
                    match entry {
                        Some(entry) => room.board.insert(key.clone(), entry.clone()),
                        None => room.board.remove(key),
                    };
                }
            }
            Effect::DropRoom { room } => {
                self.rooms.remove(room);
                self.post_keys.retain(|(name, _, _), _| name != room);
            }
            Effect::Db { at, commands } => {
                let ctx = Ctx::replay(*at);
                for argv in commands {
                    self.db.exec(argv, &ctx);
                }
            }
        }
    }

    fn remove_message(&mut self, id: &Id) {
        let Some(message) = self.messages.remove(id) else {
            return;
        };
        track(&mut self.agents, &mut self.due, &message, false);
        if let Some(agent) = self.agents.get_mut(&message.to) {
            agent.inbox.remove(id);
        }
        if let Some(key) = message.key {
            self.msg_keys.remove(&(message.to, message.from, key));
        }
    }

    fn rename(&mut self, from: &str, to: &str) {
        let Some(mut agent) = self.agents.remove(from) else {
            return;
        };
        if let Some(existing) = self.agents.remove(to) {
            agent.inbox.extend(existing.inbox);
        }
        agent.name = to.to_string();
        self.agents.insert(to.to_string(), agent);
        for message in self.messages.values_mut() {
            if message.to == from {
                message.to = to.to_string();
            }
            if message.from == from {
                message.from = to.to_string();
            }
        }
        for broadcast in self.broadcasts.values_mut() {
            if broadcast.from == from {
                broadcast.from = to.to_string();
            }
        }
        for session in self.sessions.values_mut() {
            if session.agent == from {
                session.agent = to.to_string();
            }
        }
        let rename = |name: &mut String| {
            if name == from {
                *name = to.to_string();
            }
        };
        for room in self.rooms.values_mut() {
            rename(&mut room.created_by);
            if let Some(mut member) = room.members.remove(from) {
                if let Some(existing) = room.members.remove(to) {
                    if existing.role == Role::Owner {
                        member.role = Role::Owner;
                    }
                    member.cursor = member.cursor.max(existing.cursor);
                }
                room.members.insert(to.to_string(), member);
            }
            for post in room.posts.values_mut() {
                rename(&mut post.from);
                if let Some(ask) = post.ask.as_mut() {
                    ask.to.iter_mut().for_each(rename);
                }
            }
            for entry in room.board.values_mut() {
                rename(&mut entry.by);
            }
        }
        self.reindex();
        self.retrack();
    }

    /// Rebuilds every inbox's sets and counts and the due indexes.
    fn retrack(&mut self) {
        self.due = Due::default();
        for agent in self.agents.values_mut() {
            agent.queued.clear();
            agent.settled.clear();
            agent.counts = Counts::default();
            agent.senders.clear();
        }
        for message in self.messages.values() {
            track(&mut self.agents, &mut self.due, message, true);
        }
    }

    /// Rebuilds the idempotency indexes after names change.
    fn reindex(&mut self) {
        self.msg_keys = self
            .messages
            .values()
            .filter_map(|m| Some(((m.to.clone(), m.from.clone(), m.key.clone()?), m.id)))
            .collect();
        self.bc_keys = self
            .broadcasts
            .values()
            .filter_map(|b| Some(((b.from.clone(), b.key.clone()?), b.id)))
            .collect();
        self.post_keys = self
            .rooms
            .values()
            .flat_map(|room| {
                room.posts.values().filter_map(|post| {
                    Some((
                        (room.name.clone(), post.from.clone(), post.key.clone()?),
                        post.id,
                    ))
                })
            })
            .collect();
    }

    pub fn inbox_messages<'a>(&'a self, agent: &str) -> impl Iterator<Item = &'a Message> + 'a {
        self.agents
            .get(agent)
            .into_iter()
            .flat_map(|record| record.inbox.iter())
            .filter_map(|id| self.messages.get(id))
    }

    /// An inbox's counts; `settled` includes the dead letters.
    pub fn counts(&self, agent: &str) -> Counts {
        let Some(record) = self.agents.get(agent) else {
            return Counts::default();
        };
        Counts {
            settled: record.counts.settled + record.counts.dead,
            ..record.counts
        }
    }

    /// The oldest queued messages of an inbox, in order.
    pub fn queued(&self, agent: &str) -> impl Iterator<Item = Id> + '_ {
        self.agents
            .get(agent)
            .into_iter()
            .flat_map(|record| record.queued.iter().copied())
    }

    /// How many of `agent`'s pending messages came from `sender`.
    pub fn pending_from(&self, agent: &str, sender: &str) -> usize {
        self.agents
            .get(agent)
            .and_then(|record| record.senders.get(sender))
            .copied()
            .unwrap_or(0)
    }

    /// An inbox's handled messages, oldest first.
    pub fn settled(&self, agent: &str) -> impl Iterator<Item = Id> + '_ {
        self.agents
            .get(agent)
            .into_iter()
            .flat_map(|record| record.settled.iter().copied())
    }

    /// Leased messages whose lease ended by `now`.
    pub fn leases_due(&self, now: u64) -> impl Iterator<Item = Id> + '_ {
        self.due
            .leases
            .iter()
            .take_while(move |(at, _)| *at <= now)
            .map(|(_, id)| *id)
    }

    /// Pending messages that expired by `now`.
    pub fn expired_due(&self, now: u64) -> impl Iterator<Item = Id> + '_ {
        self.due
            .expiry
            .iter()
            .take_while(move |(at, _)| *at <= now)
            .map(|(_, id)| *id)
    }

    /// Handled messages settled at or before `at`.
    pub fn settled_before(&self, at: u64) -> impl Iterator<Item = Id> + '_ {
        self.due
            .settled
            .iter()
            .take_while(move |(when, _)| *when <= at)
            .map(|(_, id)| *id)
    }

    pub fn live_sessions(&self, agent: &str, now: u64) -> usize {
        self.sessions
            .values()
            .filter(|session| session.agent == agent && session.expires > now)
            .count()
    }

    pub fn online_counts(&self, now: u64) -> HashMap<&str, usize> {
        let mut online = HashMap::new();
        for session in self.sessions.values().filter(|s| s.expires > now) {
            *online.entry(session.agent.as_str()).or_insert(0) += 1;
        }
        online
    }

    pub fn unseen_broadcasts(&self, agent: &str) -> usize {
        let cursor = self.agents.get(agent).and_then(|record| record.cursor);
        let after = cursor.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        self.broadcasts
            .range((after, std::ops::Bound::Unbounded))
            .filter(|(_, b)| b.from != agent)
            .count()
    }
}

fn insert_opt(map: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        map.insert(key.to_string(), value);
    }
}

pub fn message_json(message: &Message) -> Value {
    let mut map = Map::new();
    map.insert("id".into(), json!(message.id.to_string()));
    map.insert("from".into(), json!(message.from));
    map.insert("to".into(), json!(message.to));
    map.insert("body".into(), json!(message.body));
    map.insert("timestamp".into(), json!(iso8601(message.created)));
    map.insert("thread".into(), json!(message.thread.to_string()));
    insert_opt(
        &mut map,
        "replyTo",
        message.reply_to.map(|id| json!(id.to_string())),
    );
    map.insert("state".into(), json!(message.state));
    map.insert("deliveries".into(), json!(message.deliveries));
    if let Some(lease) = &message.lease {
        map.insert("consumer".into(), json!(lease.consumer));
        if message.state == State::Leased {
            map.insert("leaseExpiresAt".into(), json!(iso8601(lease.until)));
        }
    }
    insert_opt(
        &mut map,
        "settledAt",
        message.settled.map(|at| json!(iso8601(at))),
    );
    if message.state.is_pending() {
        map.insert("expiresAt".into(), json!(iso8601(message.expires)));
    }
    Value::Object(map)
}

pub fn post_json(room: &Room, post: &Post) -> Value {
    let mut map = Map::new();
    map.insert("id".into(), json!(post.id.to_string()));
    map.insert("room".into(), json!(room.name));
    map.insert("from".into(), json!(post.from));
    map.insert("kind".into(), json!(post.kind));
    map.insert("body".into(), json!(post.body));
    map.insert("timestamp".into(), json!(iso8601(post.created)));
    insert_opt(
        &mut map,
        "replyTo",
        post.reply_to.map(|id| json!(id.to_string())),
    );
    insert_opt(&mut map, "about", post.about.as_ref().map(|a| json!(a)));
    insert_opt(&mut map, "choice", post.choice.as_ref().map(|c| json!(c)));
    if let Some(ask) = &post.ask {
        let answers: Vec<Value> = room
            .answers(post.id)
            .map(|a| json!({ "id": a.id.to_string(), "from": a.from, "body": a.body, "choice": a.choice }))
            .collect();
        let mut question = json!({
            "status": ask.status,
            "to": ask.to,
            "options": ask.options,
            "answers": answers,
            "expiresAt": iso8601(ask.expires),
        });
        if ask.status == Status::Open {
            question["waitingOn"] = json!(room.waiting_on(post));
        }
        if let Some(at) = ask.closed {
            question["closedAt"] = json!(iso8601(at));
        }
        map.insert("question".into(), question);
    }
    Value::Object(map)
}

pub fn broadcast_json(broadcast: &Broadcast) -> Value {
    json!({
        "id": broadcast.id.to_string(),
        "from": broadcast.from,
        "body": broadcast.body,
        "timestamp": iso8601(broadcast.created),
        "expiresAt": iso8601(broadcast.expires),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: u64, from: &str, to: &str) -> Message {
        Message {
            id: Id { ms: id, seq: 0 },
            from: from.into(),
            to: to.into(),
            body: "hello".into(),
            created: id,
            expires: id + 1000,
            thread: Id { ms: id, seq: 0 },
            reply_to: None,
            key: Some(format!("k{id}")),
            state: State::Queued,
            deliveries: 0,
            lease: None,
            settled: None,
        }
    }

    /// Everything the incremental tracking maintains, for comparison.
    fn tracked(store: &Store) -> String {
        let agents: Vec<String> = store
            .agents
            .values()
            .map(|a| {
                format!(
                    "{} {:?} {:?} {:?} {:?}",
                    a.name, a.counts, a.queued, a.settled, a.senders
                )
            })
            .collect();
        format!(
            "{agents:?} {:?} {:?} {:?}",
            store.due.leases, store.due.expiry, store.due.settled
        )
    }

    #[test]
    fn incremental_tracking_matches_a_rebuild() {
        let mut store = Store::default();
        let id = |ms| Id { ms, seq: 0 };
        let lease = |ms, until| Effect::Lease {
            id: id(ms),
            consumer: "c".into(),
            token: format!("t{ms}"),
            at: ms,
            until,
        };
        let settle = |ms, state, at| Effect::Settle {
            id: id(ms),
            state,
            at,
        };
        for n in 1..=8 {
            store.apply(&Effect::Message {
                message: message(n, "a", if n % 2 == 0 { "b" } else { "c" }),
            });
        }
        let script = [
            lease(1, 50),
            lease(2, 60),
            settle(2, State::Acked, 70),
            lease(3, 80),
            settle(3, State::Queued, 90),
            lease(3, 95),
            settle(4, State::Dead, 100),
            settle(5, State::Expired, 110),
            Effect::Drop { ids: vec![id(6)] },
            lease(7, 120),
            Effect::Rename {
                from: "c".into(),
                to: "d".into(),
            },
            Effect::Forget { agent: "b".into() },
        ];
        for effect in &script {
            store.apply(effect);
            let incremental = tracked(&store);
            store.retrack();
            assert_eq!(incremental, tracked(&store), "after {effect:?}");
        }
        assert_eq!(store.counts("d").leased, 3);
        assert_eq!(
            store.counts("d").settled,
            1,
            "the expired message is handled"
        );
        assert_eq!(store.leases_due(100).count(), 2, "leases ending by 100");
    }

    #[test]
    fn rename_moves_inbox_history_and_keys() {
        let mut store = Store::default();
        store.apply(&Effect::Agent {
            name: "old".into(),
            at: 1,
        });
        store.apply(&Effect::Message {
            message: message(10, "peer", "old"),
        });
        store.apply(&Effect::Message {
            message: message(11, "old", "peer"),
        });
        store.apply(&Effect::Rename {
            from: "old".into(),
            to: "new".into(),
        });
        assert!(!store.agents.contains_key("old"));
        assert_eq!(store.counts("new").queued, 1);
        assert_eq!(
            store.message_by_key("new", "peer", "k10").unwrap().to,
            "new"
        );
        assert!(store.message_by_key("new", "old", "k10").is_none());
        // The renamed agent's own keys follow it as a sender too.
        assert_eq!(
            store.message_by_key("peer", "new", "k11").unwrap().from,
            "new"
        );
        assert_eq!(store.messages[&Id { ms: 11, seq: 0 }].from, "new");
    }

    #[test]
    fn idempotency_keys_belong_to_their_sender() {
        let mut store = Store::default();
        store.apply(&Effect::Message {
            message: message(10, "bob", "alice"),
        });
        assert!(store.message_by_key("alice", "bob", "k10").is_some());
        assert!(store.message_by_key("alice", "mallory", "k10").is_none());
        store.apply(&Effect::Drop {
            ids: vec![Id { ms: 10, seq: 0 }],
        });
        assert!(store.message_by_key("alice", "bob", "k10").is_none());
    }

    #[test]
    fn snapshot_round_trip_rebuilds_indexes() {
        let mut store = Store::default();
        store.apply(&Effect::Message {
            message: message(10, "a", "b"),
        });
        let json = serde_json::to_string(&store.snapshot()).unwrap();
        let restored = Store::from_snapshot(serde_json::from_str(&json).unwrap());
        assert_eq!(restored.counts("b").queued, 1);
        assert_eq!(restored.last_id, Id { ms: 10, seq: 0 });
        assert!(restored.message_by_key("b", "a", "k10").is_some());
    }
}
