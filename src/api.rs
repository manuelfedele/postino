//! HTTP API and Server-Sent Events.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::io::{self, BufReader, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::broker::{
    self, Actor, AnswerRequest, AskRequest, Broker, CreateRoom, HistoryQuery, PostQuery,
    PostRequest, PresenceRequest, SendRequest,
};
use crate::commands::Reply;
use crate::config::{Auth, OPERATOR, is_loopback, loopback_ip, reserved};
use crate::http::{self, ReadError, Request, Response};
use crate::id::Id;
use crate::scope::{Access, Scope};
use crate::store::{Kind, Role, broadcast_json, message_json, post_json};
use crate::util::{VERSION, constant_time_eq, iso8601, now_ms, random_hex, valid_key, valid_name};

const MAX_CONNECTIONS: usize = 512;
/// Open files kept for everything but connections: the journal and the
/// snapshot, the listener, the loops' pollers and bells.
const RESERVED_FILES: usize = 64;
/// How long a connection may send nothing, between requests or within one.
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a reply may wait without the client taking any of it.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// How often a stream with nothing to send looks for its client, so that a
/// stream whose client left ends soon and gives back what it holds.
const PEER_CHECK: Duration = Duration::from_secs(1);
/// How long a stream sends nothing before it pings its client.
const PING_EVERY: Duration = Duration::from_secs(15);
/// Connections only this machine may use, so a flood from the network
/// cannot lock out local agents.
const LOCAL_RESERVE: usize = 64;
const MAX_STREAMS: usize = 128;
/// Requests and streams one agent may have open at once, so a single agent
/// cannot hold every connection with blocking reads.
const MAX_AGENT_SLOTS: usize = 32;
const MAX_BODY_CHARS: usize = 32_768;
const MAX_WAIT_SECS: u64 = 60;
const MAX_TOPIC_CHARS: usize = 500;
const MAX_QUESTION_SECS: u64 = 30 * 86_400;
/// Failed authentications a remote address may make per window before it is
/// refused outright until the window ends.
const MAX_AUTH_FAILURES: u32 = 20;
const AUTH_WINDOW_MS: u64 = 60_000;
const MAX_TRACKED_ADDRESSES: usize = 4096;

pub struct ApiConfig {
    pub auth: Auth,
    pub cors_origin: Option<String>,
    pub allowed_hosts: Vec<String>,
    pub max_body: usize,
}

pub struct Server {
    pub broker: Arc<Broker>,
    pub cfg: ApiConfig,
    connections: AtomicUsize,
    /// Connections from other machines, held to fewer than all of them (see
    /// [`connection_limits`]).
    remote: AtomicUsize,
    streams: AtomicUsize,
    last_activity: AtomicU64,
    /// Open requests and streams per agent.
    slots: Mutex<HashMap<String, usize>>,
    lockout: Lockout,
    tokens: Tokens,
}

/// The configured tokens, found by a keyed hash of the token a request
/// presents rather than by comparing it with each in turn, which made every
/// request of a server with many agents compare it with all of theirs. The
/// key is random, so how long a lookup takes says nothing about any token:
/// changing one byte of a guess changes its hash entirely.
struct Tokens {
    keys: RandomState,
    by_hash: HashMap<u64, Vec<(String, Principal)>>,
    /// Each agent token's scope, by agent name.
    scopes: HashMap<String, Arc<Scope>>,
}

impl Tokens {
    fn new(auth: &Auth) -> Tokens {
        let keys = RandomState::new();
        let mut by_hash: HashMap<u64, Vec<(String, Principal)>> = HashMap::new();
        let admin = auth.api_token.iter().map(|t| (t, Principal::Admin));
        let agents = auth
            .agent_tokens
            .iter()
            .map(|t| (&t.token, Principal::Agent(t.name.clone())));
        for (token, principal) in admin.chain(agents) {
            by_hash
                .entry(keys.hash_one(token.as_bytes()))
                .or_default()
                .push((token.clone(), principal));
        }
        // A name given more than one token keeps the first one's scope.
        let mut scopes = HashMap::new();
        for agent in &auth.agent_tokens {
            scopes
                .entry(agent.name.clone())
                .or_insert_with(|| Arc::clone(&agent.scope));
        }
        Tokens {
            keys,
            by_hash,
            scopes,
        }
    }

    /// Whose token this is. The few that share its hash are each compared
    /// in full, in constant time.
    fn principal(&self, token: &[u8]) -> Option<Principal> {
        let mut found = None;
        for (candidate, principal) in self
            .by_hash
            .get(&self.keys.hash_one(token))
            .into_iter()
            .flatten()
        {
            if constant_time_eq(candidate.as_bytes(), token) {
                found = Some(principal.clone());
            }
        }
        found
    }
}

/// Failed authentications per remote address: (count, window start).
#[derive(Default)]
struct Lockout(Mutex<HashMap<IpAddr, (u32, u64)>>);

impl Lockout {
    /// Seconds left in an address's lockout, if it is locked out.
    fn wait(&self, ip: IpAddr, now: u64) -> Option<u64> {
        let failures = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let (count, since) = failures.get(&ip)?;
        let elapsed = now.saturating_sub(*since);
        (*count >= MAX_AUTH_FAILURES && elapsed < AUTH_WINDOW_MS)
            .then(|| (AUTH_WINDOW_MS - elapsed).div_ceil(1000))
    }

    fn fail(&self, ip: IpAddr, now: u64) {
        let mut failures = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if failures.len() >= MAX_TRACKED_ADDRESSES && !failures.contains_key(&ip) {
            failures.retain(|_, (_, since)| now.saturating_sub(*since) < AUTH_WINDOW_MS);
            if failures.len() >= MAX_TRACKED_ADDRESSES {
                return;
            }
        }
        let entry = failures.entry(ip).or_insert((0, now));
        if now.saturating_sub(entry.1) >= AUTH_WINDOW_MS {
            *entry = (0, now);
        }
        entry.0 += 1;
    }
}

/// One agent's claim on a request or stream slot, released on drop.
struct Slot<'a> {
    server: &'a Server,
    agent: Option<String>,
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if let Some(agent) = &self.agent {
            let mut slots = self.server.slots.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(count) = slots.get_mut(agent) {
                *count -= 1;
                if *count == 0 {
                    slots.remove(agent);
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Principal {
    Admin,
    Agent(String),
}

type ReplyResult = Result<Response, Response>;

fn ok(value: Value) -> ReplyResult {
    Ok(Response::json(200, &value))
}

/// A command reply as JSON: `{"result": ...}`, or the error: 403 when the
/// caller's scope does not reach it, 507 when memory is full, 503 when the
/// journal cannot be written or no change is taken, otherwise 400.
fn reply(reply: Reply) -> ReplyResult {
    match reply {
        Reply::Error(e) if e.starts_with("NOPERM") => Err(fail(403, e)),
        Reply::Error(e) if e.starts_with("OOM") => Err(fail(507, e)),
        Reply::Error(e) if e.starts_with("IOERR") || e.starts_with("READONLY") => Err(fail(503, e)),
        Reply::Error(e) => Err(fail(400, e)),
        r => {
            #[derive(Serialize)]
            struct Body<'a> {
                result: &'a Reply,
            }
            match serde_json::to_vec(&Body { result: &r }) {
                Ok(body) => Ok(Response::new(200, "application/json", body)),
                Err(e) => Err(fail(500, e.to_string())),
            }
        }
    }
}

/// What the head of a request lets it do.
enum Admission {
    Preflight,
    Health,
    Principal(Principal),
}

/// A batch's replies, each as `{"result": ...}` or `{"error": ...}`.
fn batch_json(replies: &[Reply]) -> Value {
    Value::Array(
        replies
            .iter()
            .map(|r| match r {
                Reply::Error(e) => json!({ "error": e }),
                r => json!({ "result": r.to_json() }),
            })
            .collect(),
    )
}

fn fail(status: u16, message: impl Into<String>) -> Response {
    Response::json(status, &json!({ "ok": false, "error": message.into() }))
}

fn broker_error(error: broker::Error) -> Response {
    let status = match error {
        broker::Error::BadRequest(_) => 400,
        broker::Error::Forbidden(_) => 403,
        broker::Error::NotFound(_) => 404,
        broker::Error::Conflict(_) => 409,
        broker::Error::Full(_) => 429,
        broker::Error::OutOfMemory(_) => 507,
        broker::Error::Storage(_) => 503,
        broker::Error::Unsaved(message, reply) => {
            return Response::json(
                503,
                &json!({ "ok": false, "error": message, "result": reply.to_json() }),
            );
        }
    };
    fail(status, error.to_string())
}

fn parse_body<T: DeserializeOwned + Default>(req: &Request) -> Result<T, Response> {
    if req.body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(&req.body).map_err(|e| fail(400, format!("invalid JSON body: {e}")))
}

fn name(value: &str) -> Result<String, Response> {
    if valid_name(value) {
        Ok(value.to_string())
    } else {
        Err(fail(400, format!("invalid agent name {value:?}")))
    }
}

fn opt_name(value: Option<&str>) -> Result<Option<String>, Response> {
    value.map(name).transpose()
}

fn opt_id(value: Option<&str>, field: &str) -> Result<Option<Id>, Response> {
    value
        .map(|v| {
            v.parse::<Id>()
                .map_err(|_| fail(400, format!("invalid {field} {v:?}")))
        })
        .transpose()
}

fn message_body(body: Option<String>) -> Result<String, Response> {
    match body {
        Some(body) if !body.is_empty() && body.chars().count() <= MAX_BODY_CHARS => Ok(body),
        Some(body) if !body.is_empty() => Err(fail(
            400,
            format!("body exceeds {MAX_BODY_CHARS} characters"),
        )),
        _ => Err(fail(400, "\"body\" is required")),
    }
}

fn room_name(value: &str) -> Result<String, Response> {
    if valid_name(value) {
        Ok(value.to_string())
    } else {
        Err(fail(400, format!("invalid room name {value:?}")))
    }
}

fn names(values: Option<Vec<String>>) -> Result<Vec<String>, Response> {
    values.unwrap_or_default().iter().map(|v| name(v)).collect()
}

fn reference(value: Option<String>) -> Result<Option<String>, Response> {
    match value.filter(|v| !v.is_empty()) {
        Some(v) if valid_key(&v) => Ok(Some(v)),
        Some(_) => Err(fail(400, "invalid \"about\" (1-128 printable characters)")),
        None => Ok(None),
    }
}

fn topic(value: Option<String>) -> Result<Option<String>, Response> {
    match value {
        Some(topic) if topic.chars().count() > MAX_TOPIC_CHARS => Err(fail(
            400,
            format!("topic exceeds {MAX_TOPIC_CHARS} characters"),
        )),
        topic => Ok(topic.map(|t| t.trim().to_string())),
    }
}

/// The identity a room request acts under. Agent tokens always act as their own
/// agent. Administrators name one, defaulting to the human operator: as the
/// operator they manage every room, as an agent they have that agent's rights,
/// so room roles hold even when every local client is trusted.
fn actor(principal: &Principal, requested: Option<&str>) -> Result<Actor, Response> {
    match principal {
        Principal::Agent(own) => Ok(Actor {
            name: own.clone(),
            admin: false,
            strict: true,
        }),
        Principal::Admin => {
            let name = opt_name(requested)?.unwrap_or_else(|| OPERATOR.into());
            Ok(Actor {
                admin: name == OPERATOR,
                name,
                strict: false,
            })
        }
    }
}

/// Like [`actor`], for reads: administrators see every room under any name.
fn viewer(principal: &Principal, requested: Option<&str>) -> Result<Actor, Response> {
    let mut actor = actor(principal, requested)?;
    actor.admin |= *principal == Principal::Admin;
    Ok(actor)
}

fn wait_secs(value: Option<u64>, raw: Option<&str>) -> Result<Duration, Response> {
    let seconds = match (value, raw) {
        (Some(v), _) => v,
        (None, Some(raw)) => raw.parse().map_err(|_| fail(400, "invalid wait"))?,
        (None, None) => 0,
    };
    Ok(Duration::from_secs(seconds.min(MAX_WAIT_SECS)))
}

fn limit(
    value: Option<usize>,
    raw: Option<&str>,
    default: usize,
    max: usize,
) -> Result<usize, Response> {
    let parsed = match (value, raw) {
        (Some(v), _) => Some(v),
        (None, Some(raw)) => Some(raw.parse().map_err(|_| fail(400, "invalid limit"))?),
        (None, None) => None,
    };
    match parsed.unwrap_or(default) {
        n if (1..=max).contains(&n) => Ok(n),
        _ => Err(fail(400, format!("limit must be between 1 and {max}"))),
    }
}

fn require_admin(principal: &Principal) -> Result<(), Response> {
    match principal {
        Principal::Admin => Ok(()),
        Principal::Agent(_) => Err(fail(403, "this endpoint requires the administrator token")),
    }
}

fn require_self(principal: &Principal, agent: &str) -> Result<(), Response> {
    match principal {
        Principal::Agent(own) if own != agent => {
            Err(fail(403, "agent tokens may only act on their own inbox"))
        }
        _ => Ok(()),
    }
}

/// The name a Host header gives, without its port, in the case it was
/// sent in: names are compared without regard to case.
fn host_name(host: &str) -> &str {
    if host.starts_with('[') {
        return host.split(']').next().unwrap_or("").trim_start_matches('[');
    }
    match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    }
}

/// The token of an `Authorization` header of the Bearer scheme. The name of
/// the scheme is read without regard to case, and one or more spaces part it
/// from the token, as RFC 9110 and RFC 6750 have it.
fn bearer(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim_start_matches(' '))
}

fn origin_authority(origin: &str) -> Option<&str> {
    origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
}

/// Liveness probe for a client parked in a blocking read: a readable socket with
/// zero bytes means the peer closed the connection.
fn peer_open(stream: &TcpStream) -> bool {
    if stream.set_nonblocking(true).is_err() {
        return true;
    }
    let mut byte = [0u8; 1];
    let open = match stream.peek(&mut byte) {
        Ok(0) => false,
        Ok(_) => true,
        Err(e) => e.kind() == std::io::ErrorKind::WouldBlock,
    };
    let _ = stream.set_nonblocking(false);
    open
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SendBody {
    to: Option<String>,
    from: Option<String>,
    body: Option<String>,
    #[serde(alias = "reply_to")]
    reply_to: Option<String>,
    key: Option<String>,
    id: Option<String>,
    strict: Option<bool>,
}

#[derive(Deserialize, Default)]
struct ReadBody {
    limit: Option<usize>,
    wait: Option<u64>,
    consumer: Option<String>,
    ids: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
struct AckBody {
    receipts: Option<Vec<String>>,
    ids: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
struct PresenceBody {
    session: Option<String>,
    client: Option<String>,
    pid: Option<u32>,
    host: Option<String>,
    ttl: Option<u64>,
}

#[derive(Deserialize, Default)]
struct RenameBody {
    #[serde(alias = "name")]
    to: Option<String>,
}

#[derive(Deserialize, Default)]
struct BroadcastBody {
    from: Option<String>,
    body: Option<String>,
    key: Option<String>,
    id: Option<String>,
}

#[derive(Deserialize, Default)]
struct BroadcastReadBody {
    all: Option<bool>,
    advance: Option<bool>,
}

#[derive(Deserialize, Default)]
struct RoomBody {
    name: Option<String>,
    from: Option<String>,
    topic: Option<String>,
    private: Option<bool>,
    members: Option<Vec<String>>,
    join: Option<bool>,
    closed: Option<bool>,
}

#[derive(Deserialize, Default)]
struct MemberBody {
    agent: Option<String>,
    from: Option<String>,
    role: Option<Role>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct PostBody {
    from: Option<String>,
    body: Option<String>,
    #[serde(alias = "reply_to")]
    reply_to: Option<String>,
    about: Option<String>,
    key: Option<String>,
    to: Option<Vec<String>>,
    options: Option<Vec<String>>,
    #[serde(alias = "expires_in")]
    expires_in: Option<u64>,
    choice: Option<String>,
}

#[derive(Deserialize, Default)]
struct RoomReadBody {
    agent: Option<String>,
    limit: Option<usize>,
    wait: Option<u64>,
}

#[derive(Deserialize, Default)]
struct BoardBody {
    from: Option<String>,
    value: Option<String>,
    version: Option<u64>,
}

impl Server {
    /// Serves `broker`, telling it which agents have tokens: those names are
    /// known to it before they first connect.
    pub fn new(broker: Arc<Broker>, cfg: ApiConfig) -> Arc<Server> {
        let names = cfg.auth.agent_tokens.iter().map(|t| t.name.clone());
        broker.run(|core, _| core.set_token_agents(names));
        Arc::new(Server {
            broker,
            tokens: Tokens::new(&cfg.auth),
            cfg,
            connections: AtomicUsize::new(0),
            remote: AtomicUsize::new(0),
            streams: AtomicUsize::new(0),
            last_activity: AtomicU64::new(now_ms()),
            slots: Mutex::new(HashMap::new()),
            lockout: Lockout::default(),
        })
    }

    pub fn idle_for(&self) -> Duration {
        Duration::from_millis(now_ms().saturating_sub(self.last_activity.load(Ordering::Relaxed)))
    }

    pub fn open_streams(&self) -> usize {
        self.streams.load(Ordering::Relaxed)
    }

    pub fn serve(self: &Arc<Server>, listener: TcpListener) {
        #[cfg(unix)]
        let loops = match crate::event::Loops::start(
            Arc::clone(self),
            loop_threads(),
            self.limits(),
        ) {
            Ok(loops) => Some(loops),
            Err(e) => {
                eprintln!(
                    "postino: cannot start the connection loops ({e}); each connection gets a thread"
                );
                None
            }
        };
        let (most, most_remote) = connection_limits();
        let mut refusing = false;
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(stream) => {
                    refusing = false;
                    stream
                }
                // Out of open files, say: the connection stays queued, and
                // asking for it again at once would spin.
                Err(e) => {
                    if !std::mem::replace(&mut refusing, true) {
                        eprintln!("postino: cannot accept a connection: {e}");
                    }
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
            };
            let remote = !stream.peer_addr().is_ok_and(|a| loopback_ip(a.ip()));
            let full = self.connections.load(Ordering::Relaxed) >= most
                || (remote && self.remote.load(Ordering::Relaxed) >= most_remote);
            if full {
                let _ = http::write_response(
                    &mut stream,
                    &fail(503, "too many connections"),
                    false,
                    false,
                );
                continue;
            }
            self.opened(remote);
            #[cfg(unix)]
            if let Some(loops) = &loops {
                let server = Arc::clone(self);
                let on_close = crate::event::OnClose::new(move || server.closed(remote));
                let _ = loops.adopt(stream, on_close);
                continue;
            }
            let server = Arc::clone(self);
            let spawned = thread::Builder::new()
                .name("postino-conn".into())
                .spawn(move || {
                    server.connection(stream, None, Vec::new(), Vec::new());
                    server.closed(remote);
                    crate::memory::flush();
                });
            if spawned.is_err() {
                self.closed(remote);
            }
        }
    }

    #[cfg(unix)]
    fn limits(&self) -> crate::event::Limits {
        let memory = self.broker.run(|core, _| core.limits.max_memory);
        crate::event::Limits {
            max_body: self.cfg.max_body,
            idle: IDLE_TIMEOUT,
            head: http::HEAD_DEADLINE,
            write: WRITE_TIMEOUT,
            unsent: unsent_room(memory),
        }
    }

    fn opened(&self, remote: bool) {
        self.connections.fetch_add(1, Ordering::Relaxed);
        if remote {
            self.remote.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn closed(&self, remote: bool) {
        self.connections.fetch_sub(1, Ordering::Relaxed);
        if remote {
            self.remote.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Serves a connection on a thread of its own, from `next` if a request
    /// was read for it already: `output` holds replies to the requests before
    /// it, which go out first, and `input` what arrived after it.
    fn connection(
        &self,
        stream: TcpStream,
        mut next: Option<Request>,
        output: Vec<u8>,
        input: Vec<u8>,
    ) {
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(IDLE_TIMEOUT));
        let _ = stream.set_write_timeout(Some(WRITE_TIMEOUT));
        // Read and written through the one descriptor, which a stream takes
        // over once it is reached.
        let mut reader = BufReader::new(io::Cursor::new(input).chain(&stream));
        let mut writer = &stream;
        if writer.write_all(&output).is_err() {
            return;
        }
        let peer = stream.peer_addr().ok().map(|a| a.ip());
        loop {
            let request = match next.take() {
                Some(request) => request,
                None => {
                    let admit = |head: &Request| self.refusal(head, peer);
                    match http::read_request(&mut reader, self.cfg.max_body, admit) {
                        Ok(request) => request,
                        Err(ReadError::Closed) => return,
                        Err(ReadError::Status(status, message)) => {
                            let response = fail(status, message);
                            let _ = http::write_response(&mut writer, &response, false, false);
                            return;
                        }
                        Err(ReadError::Refused(response)) => {
                            let _ = http::write_response(&mut writer, &response, false, false);
                            return;
                        }
                    }
                }
            };
            self.last_activity.store(now_ms(), Ordering::Relaxed);
            let segments = request.segments();
            if request.method() == "GET" {
                match segments.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                    ["api", "events"] => return self.events(&request, stream),
                    ["api", "subscribe"] => return self.subscribe(&request, stream),
                    ["api", "tail", key] => {
                        let key = key.to_string();
                        return self.tail(&request, &key, stream);
                    }
                    _ => {}
                }
            }
            let response = self.respond(&request, segments, &|| peer_open(&stream), peer);
            let keep_alive = request.keep_alive();
            if http::write_response(
                &mut writer,
                &response,
                keep_alive,
                request.method() == "HEAD",
            )
            .is_err()
                || !keep_alive
            {
                return;
            }
        }
    }

    /// The response that refuses a request from its head alone, if it is
    /// refused: its body is then read past, not kept.
    fn refusal(&self, head: &Request, peer: Option<IpAddr>) -> Option<Response> {
        let segments = head.segments();
        let segs: Vec<&str> = segments.iter().map(String::as_str).collect();
        let refused = match self.admit(head, &segs, peer) {
            Err(refused) => refused,
            // Open to anyone, and neither takes a body: none is kept for them.
            Ok(Admission::Preflight | Admission::Health) => fail(400, "this request takes no body"),
            Ok(Admission::Principal(_)) => return None,
        };
        Some(self.finish(head, refused))
    }

    fn host_allowed(&self, req: &Request) -> bool {
        // Without credentials the API trusts loopback callers, so a DNS-rebinding
        // page must not be able to reach it under a foreign Host name.
        if self.cfg.auth.enabled() && self.cfg.allowed_hosts.is_empty() {
            return true;
        }
        let Some(host) = req.header("host") else {
            return req.http10;
        };
        let name = host_name(host);
        if self.cfg.allowed_hosts.is_empty() {
            is_loopback(name)
        } else {
            self.cfg
                .allowed_hosts
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(name))
                || is_loopback(name)
        }
    }

    fn same_origin(&self, req: &Request) -> bool {
        let Some(origin) = req.header("origin") else {
            return true;
        };
        if self.cfg.cors_origin.as_deref() == Some(origin) {
            return true;
        }
        match (origin_authority(origin), req.header("host")) {
            (Some(authority), Some(host)) => authority.eq_ignore_ascii_case(host),
            _ => false,
        }
    }

    pub fn principal_for(&self, token: Option<&str>) -> Option<Principal> {
        if !self.cfg.auth.enabled() {
            return Some(Principal::Admin);
        }
        self.tokens.principal(token?.as_bytes())
    }

    /// Who is calling: the token's principal, narrowed to one agent when the
    /// request names it in `X-Postino-Agent`. Narrowing only ever removes rights,
    /// so an administrator's token can be handed to an agent's adapter safely.
    fn principal(&self, req: &Request, peer: Option<IpAddr>) -> Result<Principal, Response> {
        let token = req
            .header("authorization")
            .and_then(bearer)
            .or_else(|| req.header("x-postino-token"));
        // Loopback callers are this machine's own agents; locking them out
        // would let one misconfigured agent shut out every other.
        let remote = peer.filter(|ip| !loopback_ip(*ip));
        if let Some(ip) = remote
            && let Some(wait) = self.lockout.wait(ip, now_ms())
        {
            return Err(fail(
                429,
                format!("too many failed authentications from this address; retry in {wait}s"),
            )
            .with_header("Retry-After", wait.to_string()));
        }
        let principal = match self.principal_for(token) {
            Some(principal) => principal,
            None => {
                if let (Some(ip), Some(_)) = (remote, &token) {
                    self.lockout.fail(ip, now_ms());
                }
                return Err(fail(401, "authentication required"));
            }
        };
        let Some(agent) = req.header("x-postino-agent") else {
            return Ok(principal);
        };
        let agent = name(agent)?;
        if reserved(&agent) {
            return Err(fail(
                403,
                "\"operator\" is the human operator; an agent cannot act under that name",
            ));
        }
        match principal {
            Principal::Agent(own) if own != agent => Err(fail(
                403,
                format!("this token belongs to agent {own:?}; it cannot act as {agent:?}"),
            )),
            _ => Ok(Principal::Agent(agent)),
        }
    }

    /// What a principal reaches in the keyspace. An agent with a token has the
    /// token's scope; an agent the server knows no token for (an administrator
    /// narrowed to it, or any local agent of an open server) reaches every key,
    /// but cannot flush them or act as an administrator.
    pub fn access(&self, principal: &Principal) -> Access {
        match principal {
            Principal::Admin => Access::admin(),
            Principal::Agent(name) => Access {
                admin: false,
                scope: self
                    .tokens
                    .scopes
                    .get(name)
                    .map_or_else(Scope::everything, Arc::clone),
            },
        }
    }

    /// Claims one of an agent's request slots; administrators are not limited.
    fn slot(&self, principal: &Principal) -> Result<Slot<'_>, Response> {
        let Principal::Agent(agent) = principal else {
            return Ok(Slot {
                server: self,
                agent: None,
            });
        };
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        let count = slots.entry(agent.clone()).or_insert(0);
        if *count >= MAX_AGENT_SLOTS {
            return Err(fail(
                429,
                format!(
                    "agent {agent:?} already has {MAX_AGENT_SLOTS} requests and streams open; close some first"
                ),
            ));
        }
        *count += 1;
        Ok(Slot {
            server: self,
            agent: Some(agent.clone()),
        })
    }

    fn cors_headers(&self, req: &Request, mut response: Response) -> Response {
        if let (Some(allowed), Some(origin)) = (&self.cfg.cors_origin, req.header("origin"))
            && allowed == origin
        {
            response = response
                .with_header("Access-Control-Allow-Origin", origin.to_string())
                .with_header("Vary", "Origin");
        }
        response
    }

    /// What the head of a request decides: its Host, path, origin, and
    /// credentials. A request with a body is checked before the body is read.
    fn admit(
        &self,
        req: &Request,
        segs: &[&str],
        peer: Option<IpAddr>,
    ) -> Result<Admission, Response> {
        if !self.host_allowed(req) {
            return Err(fail(
                403,
                "Host header not allowed; set POSTINO_ALLOWED_HOSTS to serve this name",
            ));
        }
        if segs.first() != Some(&"api") {
            return Err(fail(
                404,
                "not found; the API lives under /api (see /api/health)",
            ));
        }
        if req.method() == "OPTIONS" {
            return Ok(Admission::Preflight);
        }
        if matches!(req.method(), "POST" | "PUT" | "PATCH" | "DELETE") && !self.same_origin(req) {
            return Err(fail(403, "cross-origin request denied"));
        }
        if req.method() == "GET" && segs[1..] == ["health"] {
            return Ok(Admission::Health);
        }
        self.principal(req, peer).map(Admission::Principal)
    }

    fn respond(
        &self,
        req: &Request,
        segments: &[String],
        alive: &dyn Fn() -> bool,
        peer: Option<IpAddr>,
    ) -> Response {
        let segs: Vec<&str> = segments.iter().map(String::as_str).collect();
        let result = self
            .admit(req, &segs, peer)
            .and_then(|admission| match admission {
                Admission::Preflight => Ok(Response::empty(204)
                    .with_header(
                        "Access-Control-Allow-Methods",
                        "GET, POST, PUT, PATCH, DELETE, OPTIONS",
                    )
                    .with_header(
                        "Access-Control-Allow-Headers",
                        "Authorization, Content-Type, Idempotency-Key, Last-Event-ID, X-Postino-Token, X-Postino-Agent, X-Postino-Consumer",
                    )
                    .with_header("Access-Control-Max-Age", "600")),
                // Unhealthy once no change is taken, so that whatever watches
                // the server knows to restart it.
                Admission::Health if self.broker.halted() => Ok(Response::json(
                    503,
                    &json!({
                        "ok": false,
                        "service": "postino",
                        "version": VERSION,
                        "instance": self.broker.instance,
                        "error": broker::HALTED,
                    }),
                )),
                Admission::Health => ok(json!({
                    "ok": true,
                    "service": "postino",
                    "version": VERSION,
                    "instance": self.broker.instance,
                })),
                Admission::Principal(principal) => {
                    let _slot = self.slot(&principal)?;
                    self.route(req, &principal, &segs[1..], alive)
                }
            });
        self.finish(req, result.unwrap_or_else(|response| response))
    }

    /// The headers every API response carries.
    fn finish(&self, req: &Request, response: Response) -> Response {
        self.cors_headers(req, response)
            .with_header("Cache-Control", "no-store")
            .with_header("X-Content-Type-Options", "nosniff")
    }

    fn route(
        &self,
        req: &Request,
        principal: &Principal,
        segs: &[&str],
        alive: &dyn Fn() -> bool,
    ) -> ReplyResult {
        let broker = &self.broker;
        match (req.method(), segs) {
            ("GET", ["whoami"]) => {
                let access = self.access(principal);
                ok(json!({
                    "role": role(principal),
                    "agent": match principal { Principal::Agent(a) => Some(a.as_str()), Principal::Admin => None },
                    "auth": self.cfg.auth.enabled(),
                    "scope": {
                        "keys": access.scope.keys,
                        "read": access.scope.read,
                        "channels": access.scope.channels,
                    },
                }))
            }
            ("GET", ["overview"]) => {
                require_admin(principal)?;
                let viewer = viewer(principal, req.query("agent"))?;
                broker.run(|core, now| {
                    let (broadcasts, _) = core.broadcasts(None, 50);
                    ok(json!({
                        "broker": {
                            "version": VERSION,
                            "instance": broker.instance,
                            "storage": broker.storage,
                            "uptimeSeconds": now.saturating_sub(broker.started) / 1000,
                            "ttlSeconds": core.limits.msg_ttl_ms / 1000,
                            "leaseSeconds": core.limits.lease_ms / 1000,
                            "maxDeliveries": core.limits.max_deliveries,
                            "historyLimit": core.limits.history_limit,
                            "auth": self.cfg.auth.enabled(),
                        },
                        "stats": core.stats(now),
                        "agents": core.agents_json(now, None),
                        "broadcasts": broadcasts.into_iter().map(broadcast_json).collect::<Vec<_>>(),
                        "rooms": core.rooms_json(&viewer, now),
                    }))
                })
            }
            ("GET", ["stats"]) => {
                require_admin(principal)?;
                broker.run(|core, now| {
                    let mut stats = core.stats(now);
                    stats["version"] = json!(VERSION);
                    stats["messageTtl"] = json!(core.limits.msg_ttl_ms / 1000);
                    ok(stats)
                })
            }
            ("GET", ["agents"]) => broker.run(|core, now| {
                let viewer = match principal {
                    Principal::Agent(own) => Some(own.as_str()),
                    Principal::Admin => None,
                };
                let items = core.agents_json(now, viewer);
                ok(json!({ "total": items.len(), "items": items }))
            }),
            ("POST", ["agents", agent, "presence"]) => {
                let agent = name(agent)?;
                require_self(principal, &agent)?;
                let body: PresenceBody = parse_body(req)?;
                let session = match body.session {
                    Some(session) if valid_key(&session) => session,
                    Some(_) => return Err(fail(400, "invalid session id")),
                    None => random_hex(12),
                };
                let ttl_ms = match body.ttl {
                    Some(seconds) => seconds.clamp(5, 300) * 1000,
                    None => broker.run(|core, _| core.limits.presence_ttl_ms),
                };
                let request = PresenceRequest {
                    agent,
                    session: session.clone(),
                    client: body
                        .client
                        .unwrap_or_else(|| "http".into())
                        .chars()
                        .take(64)
                        .collect(),
                    pid: body.pid,
                    host: body.host.map(|h| h.chars().take(128).collect()),
                    ttl_ms,
                };
                let expires = broker
                    .run(|core, now| core.presence(request, now))
                    .map_err(broker_error)?;
                ok(
                    json!({ "ok": true, "session": session, "ttlSeconds": ttl_ms / 1000, "expiresAt": iso8601(expires) }),
                )
            }
            ("DELETE", ["agents", agent, "presence", session]) => {
                let agent = name(agent)?;
                require_self(principal, &agent)?;
                broker.run(|core, now| core.leave(&agent, session, now));
                ok(json!({ "ok": true }))
            }
            ("POST", ["agents", agent, "rename"]) => {
                let from = name(agent)?;
                let body: RenameBody = parse_body(req)?;
                let to = name(body.to.as_deref().unwrap_or(""))?;
                // Who may rename comes first, so the answer tells an agent
                // nothing about the names of others.
                match principal {
                    Principal::Agent(own) if *own != from => {
                        return Err(fail(403, "an agent can only rename itself"));
                    }
                    Principal::Agent(own) if self.cfg.auth.agent(own).is_some() => {
                        return Err(fail(403, "agent tokens are bound to a fixed name"));
                    }
                    _ => {}
                }
                if reserved(&to) {
                    return Err(fail(400, "\"operator\" is reserved for the human operator"));
                }
                if self.cfg.auth.agent(&to).is_some() {
                    return Err(fail(409, format!("agent name {to:?} belongs to a token")));
                }
                // An agent taking a name someone else used would inherit their
                // rooms and questions; only the operator may merge identities.
                let fresh_only = *principal != Principal::Admin;
                broker
                    .run(|core, now| {
                        if fresh_only && core.name_in_use(&to) {
                            return Err(broker::Error::Conflict(format!(
                                "agent name {to:?} is already in use"
                            )));
                        }
                        core.rename(&from, &to, now)
                    })
                    .map_err(broker_error)?;
                ok(json!({ "ok": true, "from": from, "to": to }))
            }
            ("DELETE", ["agents", agent]) => {
                require_admin(principal)?;
                let agent = name(agent)?;
                broker
                    .run(|core, now| core.forget(&agent, now))
                    .map_err(broker_error)?;
                ok(json!({ "ok": true }))
            }
            ("POST", ["cleanup"]) => {
                require_admin(principal)?;
                let removed = broker
                    .run(|core, now| core.cleanup(now))
                    .map_err(broker_error)?;
                ok(json!({ "ok": true, "removed": removed }))
            }
            ("GET", ["check", agent]) => {
                let agent = name(agent)?;
                require_self(principal, &agent)?;
                broker.run(|core, _| {
                    let counts = core.counts(&agent);
                    ok(json!({
                        "agent": agent,
                        "known": core.known_agent(&agent),
                        "unreadMessages": counts.pending(),
                        "queued": counts.queued,
                        "leased": counts.leased,
                        "dead": counts.dead,
                        "unseenBroadcasts": core.store.unseen_broadcasts(&agent),
                        "rooms": core.room_activity(&agent),
                    }))
                })
            }
            ("POST", ["messages"]) => self.send(req, principal),
            ("GET", ["messages", inbox]) => {
                let inbox = name(inbox)?;
                require_self(principal, &inbox)?;
                let page = limit(None, req.query("limit"), 100, 200)?;
                let offset: usize = req.query("offset").map_or(Ok(0), |v| {
                    v.parse().map_err(|_| fail(400, "invalid offset"))
                })?;
                broker.run(|core, _| {
                    let pending = core.pending(&inbox);
                    let items: Vec<Value> = pending.iter().skip(offset).take(page).map(|m| message_json(m)).collect();
                    ok(json!({ "items": items, "total": pending.len(), "offset": offset, "limit": page }))
                })
            }
            ("POST", ["messages", inbox, "read"]) => {
                let inbox = name(inbox)?;
                require_self(principal, &inbox)?;
                let body: ReadBody = parse_body(req)?;
                let count = limit(body.limit, req.query("limit"), 10, 100)?;
                let wait = Duration::from_secs(body.wait.unwrap_or(0).min(MAX_WAIT_SECS));
                let consumer = req
                    .header("x-postino-consumer")
                    .map(str::to_string)
                    .or(body.consumer)
                    .filter(|c| valid_key(c))
                    .unwrap_or_else(|| format!("http-{}", random_hex(6)));
                let leased = match body.ids {
                    Some(ids) if ids.is_empty() || ids.len() > 100 => {
                        return Err(fail(400, "\"ids\" must list 1 to 100 message ids"));
                    }
                    Some(ids) => {
                        let ids = ids
                            .iter()
                            .map(|id| {
                                id.parse::<Id>()
                                    .map_err(|_| fail(400, format!("invalid id {id:?}")))
                            })
                            .collect::<Result<Vec<Id>, Response>>()?;
                        broker.run(|core, now| core.lease_ids(&inbox, &consumer, &ids, now))
                    }
                    None => broker.lease_wait(&inbox, &consumer, count, wait, alive),
                }
                .map_err(broker_error)?;
                let lease_seconds = broker.run(|core, _| core.limits.lease_ms / 1000);
                let items: Vec<Value> = leased
                    .iter()
                    .map(|l| {
                        let mut item = message_json(&l.message);
                        item["receipt"] = json!(l.receipt);
                        item
                    })
                    .collect();
                ok(json!({ "items": items, "consumer": consumer, "leaseSeconds": lease_seconds }))
            }
            ("POST", ["messages", inbox, "ack"]) => {
                let inbox = name(inbox)?;
                require_self(principal, &inbox)?;
                let body: AckBody = parse_body(req)?;
                let receipts = body.receipts.or(body.ids).unwrap_or_default();
                if receipts.is_empty()
                    || receipts.len() > 100
                    || !receipts.iter().all(|r| r.len() <= 200)
                {
                    return Err(fail(400, "\"receipts\" must list 1 to 100 receipts"));
                }
                let consumer = req.header("x-postino-consumer").map(str::to_string);
                let outcome = broker
                    .run(|core, now| core.ack(&inbox, &receipts, consumer.as_deref(), now))
                    .map_err(broker_error)?;
                let status = if outcome.rejected.is_empty() {
                    200
                } else {
                    409
                };
                Ok(Response::json(
                    status,
                    &json!({
                        "ok": outcome.rejected.is_empty(),
                        "acknowledged": outcome.acknowledged,
                        "rejected": outcome.rejected,
                    }),
                ))
            }
            ("DELETE", ["messages", inbox]) => {
                let inbox = name(inbox)?;
                require_self(principal, &inbox)?;
                let removed = broker
                    .run(|core, now| core.purge(&inbox, now))
                    .map_err(broker_error)?;
                ok(json!({ "ok": true, "removed": removed }))
            }
            ("GET", ["history"]) => {
                let mut query = HistoryQuery {
                    agent: opt_name(req.query("agent"))?,
                    peer: opt_name(req.query("peer"))?,
                    thread: opt_id(req.query("thread"), "thread")?,
                    before: opt_id(req.query("before"), "cursor")?,
                    limit: limit(None, req.query("limit"), 50, 200)?,
                };
                if let Principal::Agent(own) = principal {
                    if query.agent.as_ref().is_some_and(|a| a != own) {
                        return Err(fail(403, "agent tokens may only read their own history"));
                    }
                    query.agent = Some(own.clone());
                }
                broker.run(|core, _| {
                    let (items, next) = core.history(&query);
                    ok(json!({
                        "items": items.into_iter().map(message_json).collect::<Vec<_>>(),
                        "next": next.map(|id| id.to_string()),
                    }))
                })
            }
            ("GET", ["broadcasts"]) => {
                let after = opt_id(req.query("after"), "cursor")?;
                let count = limit(None, req.query("limit"), 100, 1000)?;
                broker.run(|core, _| {
                    let (items, total) = core.broadcasts(after, count);
                    ok(json!({ "items": items.into_iter().map(broadcast_json).collect::<Vec<_>>(), "total": total }))
                })
            }
            ("POST", ["broadcasts"]) => {
                let body: BroadcastBody = parse_body(req)?;
                let from = match principal {
                    Principal::Agent(own) => own.clone(),
                    Principal::Admin => {
                        opt_name(body.from.as_deref())?.unwrap_or_else(|| OPERATOR.into())
                    }
                };
                let text = message_body(body.body)?;
                let key = idempotency_key(req, body.key.or(body.id))?;
                let (broadcast, created) = broker
                    .run(|core, now| core.broadcast(&from, text, key, now))
                    .map_err(broker_error)?;
                ok(json!({
                    "ok": true,
                    "id": broadcast.id.to_string(),
                    "created": created,
                    "broadcast": broadcast_json(&broadcast),
                }))
            }
            ("DELETE", ["broadcasts"]) => {
                require_admin(principal)?;
                broker
                    .run(|core, now| core.clear_broadcasts(now))
                    .map_err(broker_error)?;
                ok(json!({ "ok": true }))
            }
            ("POST", ["broadcasts", agent, "read"]) => {
                let agent = name(agent)?;
                require_self(principal, &agent)?;
                let body: BroadcastReadBody = parse_body(req)?;
                let all = body.all.unwrap_or(false);
                let (items, total) = broker
                    .run(|core, _| core.read_broadcasts(&agent, all, body.advance.unwrap_or(true)))
                    .map_err(broker_error)?;
                ok(json!({
                    "items": items.iter().map(broadcast_json).collect::<Vec<_>>(),
                    "unseen": if all { None } else { Some(items.len()) },
                    "total": total,
                }))
            }
            (_, ["rooms", ..]) => self.rooms(req, principal, segs, alive),
            ("POST", ["db"]) => self.db(req, principal, alive),
            ("POST", ["db", "multi"]) => self.db_multi(req, principal),
            _ => Err(fail(
                404,
                format!("no route for {} {}", req.method(), req.path()),
            )),
        }
    }

    fn rooms(
        &self,
        req: &Request,
        principal: &Principal,
        segs: &[&str],
        alive: &dyn Fn() -> bool,
    ) -> ReplyResult {
        let broker = &self.broker;
        let run = |op: &dyn Fn(&mut broker::Core, u64) -> broker::Result<Value>| {
            broker
                .run(|core, now| op(core, now))
                .map_err(broker_error)
                .and_then(ok)
        };
        match (req.method(), segs) {
            ("GET", ["rooms"]) => {
                let actor = viewer(principal, req.query("agent"))?;
                run(&|core, now| {
                    let items = core.rooms_json(&actor, now);
                    Ok(json!({ "total": items.len(), "items": items }))
                })
            }
            ("POST", ["rooms"]) => {
                let body: RoomBody = parse_body(req)?;
                let actor = actor(principal, body.from.as_deref())?;
                let room = room_name(body.name.as_deref().unwrap_or(""))?;
                let request = CreateRoom {
                    name: room.clone(),
                    topic: topic(body.topic)?.unwrap_or_default(),
                    private: body.private.unwrap_or(false),
                    members: names(body.members)?,
                    join: body.join.unwrap_or(false),
                };
                let (created, value) = broker
                    .run(|core, now| -> broker::Result<(bool, Value)> {
                        let created = core.create_room(request, &actor, now)?;
                        Ok((created, core.room_json(&room, &actor.name, now)?))
                    })
                    .map_err(broker_error)?;
                ok(json!({ "ok": true, "created": created, "room": value }))
            }
            ("GET", ["rooms", room]) => {
                let room = room_name(room)?;
                let actor = viewer(principal, req.query("agent"))?;
                run(&|core, now| {
                    core.can_view(&room, &actor)?;
                    core.room_json(&room, &actor.name, now)
                })
            }
            ("PATCH", ["rooms", room]) => {
                let room = room_name(room)?;
                let body: RoomBody = parse_body(req)?;
                let actor = actor(principal, body.from.as_deref())?;
                let topic = topic(body.topic)?;
                let value = broker
                    .run(|core, now| -> broker::Result<Value> {
                        core.update_room(&room, topic, body.closed, &actor, now)?;
                        core.room_json(&room, &actor.name, now)
                    })
                    .map_err(broker_error)?;
                ok(json!({ "ok": true, "room": value }))
            }
            ("DELETE", ["rooms", room]) => {
                let room = room_name(room)?;
                let actor = actor(principal, req.query("from"))?;
                run(&|core, now| {
                    core.delete_room(&room, &actor, now)?;
                    Ok(json!({ "ok": true }))
                })
            }
            ("POST", ["rooms", room, "members"]) => {
                let room = room_name(room)?;
                let body: MemberBody = parse_body(req)?;
                let actor = actor(principal, body.from.as_deref())?;
                let agent = opt_name(body.agent.as_deref())?.unwrap_or_else(|| actor.name.clone());
                let changed = broker
                    .run(|core, now| core.join(&room, &agent, body.role, &actor, now))
                    .map_err(broker_error)?;
                ok(json!({ "ok": true, "changed": changed }))
            }
            ("DELETE", ["rooms", room, "members", agent]) => {
                let (room, agent) = (room_name(room)?, name(agent)?);
                let actor = actor(principal, req.query("from"))?;
                run(&|core, now| {
                    core.leave_room(&room, &agent, &actor, now)?;
                    Ok(json!({ "ok": true }))
                })
            }
            ("GET", ["rooms", room, "posts"]) => {
                let room = room_name(room)?;
                let actor = viewer(principal, req.query("agent"))?;
                let kind = match req.query("kind") {
                    Some(kind) => Some(
                        serde_json::from_value::<Kind>(json!(kind))
                            .map_err(|_| fail(400, "kind is note, question, answer, or event"))?,
                    ),
                    None => None,
                };
                let query = PostQuery {
                    before: opt_id(req.query("before"), "cursor")?,
                    about: reference(req.query("about").map(str::to_string))?,
                    kind,
                    limit: limit(None, req.query("limit"), 50, 200)?,
                };
                run(&|core, _| {
                    core.can_view(&room, &actor)?;
                    let (items, next) = core.room_posts(&room, &query)?;
                    Ok(json!({ "items": items, "next": next.map(|id| id.to_string()) }))
                })
            }
            ("POST", ["rooms", room, kind @ ("posts" | "questions")]) => {
                let room = room_name(room)?;
                let body: PostBody = parse_body(req)?;
                let actor = actor(principal, body.from.as_deref())?;
                let ask = match *kind {
                    "questions" => Some(AskRequest {
                        to: names(body.to)?,
                        options: body.options.unwrap_or_default(),
                        ttl_ms: body
                            .expires_in
                            .map(|s| s.clamp(10, MAX_QUESTION_SECS) * 1000),
                    }),
                    _ => None,
                };
                let request = PostRequest {
                    room: room.clone(),
                    body: message_body(body.body)?,
                    reply_to: opt_id(body.reply_to.as_deref(), "replyTo")?,
                    about: reference(body.about)?,
                    key: idempotency_key(req, body.key)?,
                    ask,
                };
                let (value, created) = broker
                    .run(|core, now| -> broker::Result<(Value, bool)> {
                        let (post, created) = core.post(request, &actor, now)?;
                        Ok((post_json(&core.store.rooms[&room], &post), created))
                    })
                    .map_err(broker_error)?;
                ok(json!({ "ok": true, "id": value["id"], "created": created, "post": value }))
            }
            ("GET", ["rooms", room, "questions", id]) => {
                let room = room_name(room)?;
                let id = opt_id(Some(id), "question id")?.unwrap_or_default();
                let actor = viewer(principal, req.query("agent"))?;
                let wait = wait_secs(None, req.query("wait"))?;
                broker
                    .run(|core, _| core.can_view(&room, &actor))
                    .map_err(broker_error)?;
                let question = broker
                    .question_wait(&room, id, wait, alive)
                    .map_err(broker_error)?;
                ok(json!({ "question": question }))
            }
            ("POST", ["rooms", room, "questions", id, "answers"]) => {
                let room = room_name(room)?;
                let id = opt_id(Some(id), "question id")?.unwrap_or_default();
                let body: PostBody = parse_body(req)?;
                let actor = actor(principal, body.from.as_deref())?;
                let text = match body.body {
                    Some(text) if !text.is_empty() => message_body(Some(text))?,
                    _ => String::new(),
                };
                let request = AnswerRequest {
                    room: room.clone(),
                    question: id,
                    body: text,
                    choice: body.choice,
                    key: idempotency_key(req, body.key)?,
                };
                let (answer, resolved, question) = broker
                    .run(|core, now| -> broker::Result<(Value, bool, Value)> {
                        let (post, resolved) = core.answer(request, &actor, now)?;
                        let answer = post_json(&core.store.rooms[&room], &post);
                        Ok((answer, resolved, core.question(&room, id)?))
                    })
                    .map_err(broker_error)?;
                ok(
                    json!({ "ok": true, "resolved": resolved, "answer": answer, "question": question }),
                )
            }
            ("DELETE", ["rooms", room, "questions", id]) => {
                let room = room_name(room)?;
                let id = opt_id(Some(id), "question id")?.unwrap_or_default();
                let actor = actor(principal, req.query("from"))?;
                run(&|core, now| {
                    core.withdraw(&room, id, &actor, now)?;
                    Ok(json!({ "ok": true }))
                })
            }
            ("POST", ["rooms", room, "read"]) => {
                let room = room_name(room)?;
                let body: RoomReadBody = parse_body(req)?;
                let actor = actor(principal, body.agent.as_deref())?;
                let count = limit(body.limit, req.query("limit"), 50, 200)?;
                let wait = wait_secs(body.wait, None)?;
                let (items, remaining) = broker
                    .room_wait(&room, &actor.name, count, wait, alive)
                    .map_err(broker_error)?;
                ok(json!({ "items": items, "remaining": remaining }))
            }
            ("GET", ["rooms", room, "board"]) => {
                let room = room_name(room)?;
                let actor = viewer(principal, req.query("agent"))?;
                run(&|core, _| {
                    core.can_view(&room, &actor)?;
                    Ok(json!({ "items": core.board_json(&room)? }))
                })
            }
            (method @ ("PUT" | "DELETE"), ["rooms", room, "board", key]) => {
                let room = room_name(room)?;
                if !valid_key(key) {
                    return Err(fail(400, "invalid board key (1-128 printable characters)"));
                }
                let (from, value, version) = if method == "PUT" {
                    let body: BoardBody = parse_body(req)?;
                    (body.from, Some(message_body(body.value)?), body.version)
                } else {
                    let version = req
                        .query("version")
                        .map(|v| v.parse::<u64>().map_err(|_| fail(400, "invalid version")))
                        .transpose()?;
                    (req.query("from").map(str::to_string), None, version)
                };
                let actor = actor(principal, from.as_deref())?;
                let entry = broker
                    .run(|core, now| core.board_set(&room, key, value, version, &actor, now))
                    .map_err(broker_error)?;
                ok(json!({
                    "ok": true,
                    "key": key,
                    "version": entry.as_ref().map(|e| e.version),
                    "value": entry.map(|e| e.value),
                }))
            }
            _ => Err(fail(
                404,
                format!("no route for {} {}", req.method(), req.path()),
            )),
        }
    }

    fn send(&self, req: &Request, principal: &Principal) -> ReplyResult {
        let body: SendBody = parse_body(req)?;
        let (from, strict) = match principal {
            Principal::Agent(own) => (own.clone(), true),
            Principal::Admin => (
                opt_name(body.from.as_deref())?.unwrap_or_else(|| OPERATOR.into()),
                body.strict.unwrap_or(false),
            ),
        };
        let request = SendRequest {
            from,
            to: opt_name(body.to.as_deref())?,
            body: message_body(body.body)?,
            reply_to: opt_id(body.reply_to.as_deref(), "replyTo")?,
            key: idempotency_key(req, body.key.or(body.id))?,
            strict,
            admin: *principal == Principal::Admin,
        };
        let (message, created) = self
            .broker
            .run(|core, now| core.send(request, now))
            .map_err(broker_error)?;
        ok(json!({
            "ok": true,
            "id": message.id.to_string(),
            "created": created,
            "to": message.to,
            "thread": message.thread.to_string(),
            "message": message_json(&message),
        }))
    }

    /// Checks Host and credentials for a long-lived stream, answering the
    /// client itself when they fail.
    fn stream_principal(&self, req: &Request, stream: &mut TcpStream) -> Option<Principal> {
        if !self.host_allowed(req) {
            reject(stream, fail(403, "Host header not allowed"));
            return None;
        }
        let peer = stream.peer_addr().ok().map(|a| a.ip());
        match self.principal(req, peer) {
            Ok(principal) => Some(principal),
            Err(response) => {
                reject(stream, response);
                None
            }
        }
    }

    /// Starts a Server-Sent Events response, within the stream limit.
    fn open_stream<'a>(
        &'a self,
        req: &Request,
        principal: &Principal,
        mut stream: TcpStream,
    ) -> Option<Sse<'a>> {
        let slot = match self.slot(principal) {
            Ok(slot) => slot,
            Err(response) => {
                reject(&mut stream, response);
                return None;
            }
        };
        if self.streams.fetch_add(1, Ordering::Relaxed) >= MAX_STREAMS {
            self.streams.fetch_sub(1, Ordering::Relaxed);
            reject(&mut stream, fail(503, "too many open streams"));
            return None;
        }
        let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
        let mut headers: Vec<http::Header> = vec![
            ("Content-Type", "text/event-stream".into()),
            ("Cache-Control", "no-store".into()),
            ("X-Accel-Buffering", "no".into()),
            ("Connection", "close".into()),
        ];
        headers.extend(self.cors_headers(req, Response::empty(200)).headers);
        let mut sse = Sse {
            server: self,
            stream,
            alive: true,
            _slot: slot,
        };
        sse.alive = http::write_head(&mut sse.stream, 200, &headers).is_ok();
        sse.send("retry: 2000\n\n");
        Some(sse)
    }

    fn events(&self, req: &Request, mut stream: TcpStream) {
        let Some(principal) = self.stream_principal(req, &mut stream) else {
            return;
        };
        let filter = match opt_name(req.query("agent")) {
            Ok(filter) => filter,
            Err(response) => return reject(&mut stream, response),
        };
        let filter = match (&principal, filter) {
            (Principal::Agent(own), Some(requested)) if &requested != own => {
                return reject(
                    &mut stream,
                    fail(403, "agent tokens may only subscribe to their own events"),
                );
            }
            (Principal::Agent(own), _) => Some(own.clone()),
            (Principal::Admin, filter) => filter,
        };
        let after = req
            .header("last-event-id")
            .or_else(|| req.query("lastEventId"))
            .and_then(|v| v.parse::<u64>().ok());
        let Some(mut sse) = self.open_stream(req, &principal, stream) else {
            return;
        };
        let (replay, events, resync) = self.broker.run(|core, _| core.subscribe(after, filter));
        if resync {
            sse.send("data: {\"type\":\"resync\"}\n\n");
        }
        for event in &replay {
            sse.event(Some(&event.id.to_string()), &event.json);
        }
        sse.send("data: {\"type\":\"ready\"}\n\n");
        while let Some(event) = sse.next(|wait| events.recv_timeout(wait)) {
            sse.event(Some(&event.id.to_string()), &event.json);
        }
    }

    /// Pub/sub delivery: `?channel=a&channel=b&pattern=jobs.*`.
    fn subscribe(&self, req: &Request, mut stream: TcpStream) {
        let Some(principal) = self.stream_principal(req, &mut stream) else {
            return;
        };
        let scope = self.access(&principal).scope;
        let channels: Vec<String> = req.query_all("channel").map(str::to_string).collect();
        let patterns: Vec<String> = req.query_all("pattern").map(str::to_string).collect();
        let total = channels.len() + patterns.len();
        if total == 0 || total > 64 || channels.iter().chain(&patterns).any(|c| c.len() > 256) {
            return reject(
                &mut stream,
                fail(
                    400,
                    "pass 1 to 64 channel or pattern parameters of up to 256 bytes",
                ),
            );
        }
        // Patterns are allowed, but only deliver what the scope reaches.
        if let Some(channel) = channels.iter().find(|c| !scope.can_subscribe(c)) {
            return reject(&mut stream, fail(403, scope.denied_channel(channel)));
        }
        let ready = json!({ "type": "subscribed", "channels": channels, "patterns": patterns });
        let subscribed = self
            .broker
            .run(|core, _| core.subscribe_channels(channels, patterns, scope));
        let deliveries = match subscribed {
            Ok(subscription) => subscription,
            Err(e) => return reject(&mut stream, broker_error(e)),
        };
        let Some(mut sse) = self.open_stream(req, &principal, stream) else {
            return;
        };
        sse.event(None, &ready.to_string());
        while let Some(delivery) = sse.next(|wait| deliveries.recv_timeout(wait)) {
            let mut event = json!({
                "type": "message",
                "channel": &*delivery.channel,
                "message": &*delivery.message,
            });
            if let Some(pattern) = &delivery.pattern {
                event["type"] = json!("pmessage");
                event["pattern"] = json!(pattern);
            }
            sse.event(None, &event.to_string());
        }
    }

    /// Live entries of a stream key. Each event id is the entry id, so a client
    /// that reconnects with Last-Event-ID resumes exactly where it stopped.
    fn tail(&self, req: &Request, key: &str, mut stream: TcpStream) {
        let Some(principal) = self.stream_principal(req, &mut stream) else {
            return;
        };
        let access = self.access(&principal);
        if let Err(denied) = access.permit(&["XLEN".into(), key.to_string()]) {
            return reject(&mut stream, fail(403, denied));
        }
        let from = req
            .header("last-event-id")
            .or_else(|| req.query("from"))
            .unwrap_or("$")
            .to_string();
        let start = if from == "$" {
            let last = self.broker.command(
                &["XINFO".into(), "STREAM".into(), key.to_string()],
                &access,
                Duration::ZERO,
                &|| true,
            );
            match last {
                Ok(Reply::Map(fields)) => fields
                    .into_iter()
                    .find(|(name, _)| name == "last-generated-id")
                    .and_then(|(_, id)| match id {
                        Reply::Bulk(id) => Some(id),
                        _ => None,
                    })
                    .unwrap_or_else(|| "0-0".into()),
                Ok(Reply::Error(e)) if e.starts_with("WRONGTYPE") => {
                    return reject(&mut stream, fail(400, e));
                }
                _ => "0-0".into(),
            }
        } else {
            match from.parse::<Id>() {
                Ok(id) => id.to_string(),
                Err(_) => return reject(&mut stream, fail(400, "invalid \"from\" id")),
            }
        };
        let Some(mut sse) = self.open_stream(req, &principal, stream) else {
            return;
        };
        let mut after = start;
        while sse.alive {
            let read = self.broker.command(
                &[
                    "XREAD".into(),
                    "COUNT".into(),
                    "100".into(),
                    "BLOCK".into(),
                    "15000".into(),
                    "STREAMS".into(),
                    key.to_string(),
                    after.clone(),
                ],
                &access,
                Duration::from_secs(15),
                &|| peer_open(&sse.stream),
            );
            match read {
                Ok(Reply::Array(streams)) => {
                    for entry in stream_entries(&streams) {
                        sse.event(Some(&entry.0), &entry.1.to_string());
                        after = entry.0;
                    }
                }
                Ok(Reply::Error(e)) => {
                    sse.event(None, &json!({ "type": "error", "error": e }).to_string());
                    sse.alive = false;
                }
                Ok(_) => sse.send(": ping\n\n"),
                Err(e) => {
                    sse.event(
                        None,
                        &json!({ "type": "error", "error": e.to_string() }).to_string(),
                    );
                    sse.alive = false;
                }
            }
        }
    }

    fn db(&self, req: &Request, principal: &Principal, alive: &dyn Fn() -> bool) -> ReplyResult {
        // Most commands come as an array of strings, read straight into one;
        // anything else, and every error, takes the general path.
        let argv = match serde_json::from_slice::<Vec<String>>(&req.body) {
            Ok(argv) if !argv.is_empty() => argv,
            _ => {
                let body: Value = match serde_json::from_slice(&req.body) {
                    Ok(body) => body,
                    Err(e) => return reply(Reply::Error(format!("ERR invalid JSON body: {e}"))),
                };
                match command_argv(body.get("command").unwrap_or(&body)) {
                    Ok(argv) => argv,
                    Err(e) => return reply(Reply::Error(e)),
                }
            }
        };
        let result = self
            .broker
            .command(
                &argv,
                &self.access(principal),
                Duration::from_secs(MAX_WAIT_SECS),
                alive,
            )
            .map_err(broker_error)?;
        reply(result)
    }

    fn db_multi(&self, req: &Request, principal: &Principal) -> ReplyResult {
        let body: Value = match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(e) => return reply(Reply::Error(format!("ERR invalid JSON body: {e}"))),
        };
        let list = body.get("commands").unwrap_or(&body);
        let Some(items) = list.as_array().filter(|a| !a.is_empty() && a.len() <= 1000) else {
            return reply(Reply::Error(
                "ERR pass 1 to 1000 commands, each an array".into(),
            ));
        };
        let batch = match items
            .iter()
            .map(command_argv)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(batch) => batch,
            Err(e) => return reply(Reply::Error(e)),
        };
        let access = self.access(principal);
        match self
            .broker
            .run(|core, now| core.multi(&batch, &access, now))
        {
            Ok(replies) => ok(batch_json(&replies)),
            Err(broker::Error::Forbidden(e)) => reply(Reply::Error(e)),
            Err(broker::Error::Unsaved(message, replies)) => Err(Response::json(
                503,
                &json!({
                    "ok": false,
                    "error": message,
                    "result": match *replies {
                        Reply::Array(replies) => batch_json(&replies),
                        other => other.to_json(),
                    },
                }),
            )),
            Err(e) => Err(broker_error(e)),
        }
    }
}

struct Sse<'a> {
    server: &'a Server,
    stream: TcpStream,
    alive: bool,
    _slot: Slot<'a>,
}

impl Sse<'_> {
    fn send(&mut self, chunk: &str) {
        self.alive = self.alive
            && self
                .stream
                .write_all(chunk.as_bytes())
                .and_then(|()| self.stream.flush())
                .is_ok();
        self.server.last_activity.store(now_ms(), Ordering::Relaxed);
    }

    fn event(&mut self, id: Option<&str>, data: &str) {
        match id {
            Some(id) => self.send(&format!("id: {id}\ndata: {data}\n\n")),
            None => self.send(&format!("data: {data}\n\n")),
        }
    }

    /// What `recv` gives next, or None once the stream is to end. It waits
    /// a second at a time to learn whether the client is still there,
    /// rather than until a ping fails to reach it, and pings it while
    /// nothing comes.
    fn next<T>(&mut self, recv: impl Fn(Duration) -> Result<T, RecvTimeoutError>) -> Option<T> {
        let mut quiet = Instant::now();
        while self.alive {
            match recv(PEER_CHECK) {
                Ok(item) => return Some(item),
                Err(RecvTimeoutError::Disconnected) => self.alive = false,
                Err(RecvTimeoutError::Timeout) if !peer_open(&self.stream) => self.alive = false,
                Err(RecvTimeoutError::Timeout) if quiet.elapsed() >= PING_EVERY => {
                    self.send(": ping\n\n");
                    quiet = Instant::now();
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
        None
    }
}

impl Drop for Sse<'_> {
    fn drop(&mut self) {
        self.server.streams.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Threads that serve connections in loops. Each answers the requests of
/// many connections, and all of them take turns at the broker's lock: with
/// two, one reads and writes while the other holds it, and more than two
/// mostly wait for it.
#[cfg(unix)]
fn loop_threads() -> usize {
    thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(2)
}

/// The room replies may take, all together, while their clients have not
/// read them: a quarter of the memory limit, or of a gigabyte without one,
/// and never less than the largest reply.
#[cfg(unix)]
fn unsent_room(memory_limit: usize) -> usize {
    let limit = if memory_limit == 0 {
        1 << 30
    } else {
        memory_limit
    };
    (limit / 4).max(crate::commands::MAX_REPLY_BYTES)
}

/// Whether a request opens a stream of events, which holds its connection
/// for as long as the client listens.
fn opens_stream(req: &Request, segments: &[String]) -> bool {
    req.method() == "GET"
        && match segments {
            [api, route] => api == "api" && (route == "events" || route == "subscribe"),
            [api, route, _] => api == "api" && route == "tail",
            _ => false,
        }
}

#[cfg(unix)]
impl crate::event::Service for Server {
    fn refuse(&self, head: &Request, peer: Option<IpAddr>) -> Option<Response> {
        self.refusal(head, peer)
    }

    /// Answers on the loop's thread, where nothing may wait: a request that
    /// would, and one that opens a stream, moves to a thread of its own.
    fn answer(&self, request: &Request, peer: Option<IpAddr>) -> Option<Response> {
        self.last_activity.store(now_ms(), Ordering::Relaxed);
        let segments = request.segments();
        if opens_stream(request, segments) {
            // Only a client let in gets a thread of its own.
            let segs: Vec<&str> = segments.iter().map(String::as_str).collect();
            return match self.admit(request, &segs, peer) {
                Ok(_) => None,
                Err(refused) => Some(self.finish(request, refused)),
            };
        }
        broker::without_waiting(|| self.respond(request, segments, &|| true, peer))
    }

    fn hand_off(self: Arc<Self>, moved: crate::event::Moved) {
        // A thread that cannot start drops the connection, which closes it.
        let _ = thread::Builder::new()
            .name("postino-conn".into())
            .spawn(move || {
                let crate::event::Moved {
                    stream,
                    request,
                    output,
                    input,
                    on_close,
                } = moved;
                self.connection(stream, Some(request), output, input);
                drop(on_close);
                crate::memory::flush();
            });
    }

    fn failure(&self, status: u16, message: &'static str) -> Response {
        fail(status, message)
    }

    fn serving(&self) {
        broker::batch_journal_writes();
    }

    /// The changes behind the replies reach the journal before they do.
    fn before_replies(&self) {
        self.broker.flush_journal();
    }
}

/// How many connections to take, and how many of them from other machines:
/// as many as the limit on open files leaves room for, once raised as far as
/// it goes, and the same share kept for this machine.
fn connection_limits() -> (usize, usize) {
    let limit = crate::util::open_files_limit((MAX_CONNECTIONS + RESERVED_FILES) as u64);
    let (most, most_remote) = connection_shares(limit);
    if most < MAX_CONNECTIONS {
        eprintln!(
            "postino: the limit on open files ({}) leaves room for {most} connections",
            limit.unwrap_or_default()
        );
    }
    (most, most_remote)
}

fn connection_shares(open_files: Option<u64>) -> (usize, usize) {
    let most = match open_files {
        Some(limit) => (limit as usize)
            .saturating_sub(RESERVED_FILES)
            .clamp(1, MAX_CONNECTIONS),
        None => MAX_CONNECTIONS,
    };
    let local = (most * LOCAL_RESERVE / MAX_CONNECTIONS).max(1);
    (most, most - local)
}

fn reject(stream: &mut TcpStream, response: Response) {
    let _ = http::write_response(stream, &response, false, false);
}

/// A command from JSON: an array of strings and numbers.
fn command_argv(value: &Value) -> Result<Vec<String>, String> {
    let items = value
        .as_array()
        .filter(|a| !a.is_empty())
        .ok_or("ERR a command is a non-empty array such as [\"GET\", \"key\"]")?;
    items
        .iter()
        .map(|item| match item {
            Value::String(s) => Ok(s.clone()),
            Value::Number(n) => Ok(n.to_string()),
            Value::Bool(b) => Ok(if *b { "1" } else { "0" }.to_string()),
            _ => Err("ERR command arguments must be strings or numbers".to_string()),
        })
        .collect()
}

/// `(id, {"id", "fields"})` pairs from an XREAD reply.
fn stream_entries(streams: &[Reply]) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for stream in streams {
        let Reply::Array(parts) = stream else {
            continue;
        };
        let Some(Reply::Array(entries)) = parts.get(1) else {
            continue;
        };
        for entry in entries {
            let Reply::Array(pair) = entry else { continue };
            let (Some(Reply::Bulk(id)), Some(Reply::Array(fields))) = (pair.first(), pair.get(1))
            else {
                continue;
            };
            let mut map = serde_json::Map::new();
            for chunk in fields.chunks(2) {
                if let [Reply::Bulk(field), Reply::Bulk(value)] = chunk {
                    map.insert(field.clone(), json!(value));
                }
            }
            out.push((id.clone(), json!({ "id": id, "fields": map })));
        }
    }
    out
}

fn role(principal: &Principal) -> &'static str {
    match principal {
        Principal::Admin => "admin",
        Principal::Agent(_) => "agent",
    }
}

fn idempotency_key(req: &Request, body_key: Option<String>) -> Result<Option<String>, Response> {
    match req
        .header("idempotency-key")
        .map(str::to_string)
        .or(body_key)
    {
        Some(key) if valid_key(&key) => Ok(Some(key)),
        Some(_) => Err(fail(
            400,
            "invalid idempotency key (1-128 printable characters)",
        )),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_change_not_yet_saved_answers_503_with_what_it_returned() {
        let unsaved = broker::Error::Unsaved(
            "IOERR journal write failed".into(),
            Box::new(Reply::Bulk("job-1".into())),
        );
        let response = broker_error(unsaved);
        assert_eq!(response.status, 503);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"], "job-1");
        assert!(body["error"].as_str().unwrap().starts_with("IOERR"));
    }

    #[cfg(unix)]
    #[test]
    fn each_of_many_tokens_finds_its_principal_and_nothing_else_does() {
        use crate::config::AgentToken;
        let token = |i: usize| format!("{:064x}", (i as u128 + 1) * 0x9e37_79b9_7f4a_7c15);
        let auth = Auth {
            api_token: Some(token(0)),
            agent_tokens: (1..=1000)
                .map(|i| AgentToken::new(&format!("agent-{i}"), &token(i)))
                .collect(),
        };
        let tokens = Tokens::new(&auth);
        assert_eq!(
            tokens.principal(token(0).as_bytes()),
            Some(Principal::Admin)
        );
        for i in [1, 2, 500, 1000] {
            assert_eq!(
                tokens.principal(token(i).as_bytes()),
                Some(Principal::Agent(format!("agent-{i}")))
            );
            assert!(tokens.scopes.contains_key(&format!("agent-{i}")));
        }
        let mut close = token(7).into_bytes();
        close[63] ^= 1;
        for wrong in [
            &close[..],
            &token(7).as_bytes()[..63],
            b"",
            token(1001).as_bytes(),
        ] {
            assert_eq!(tokens.principal(wrong), None);
        }
        // A name given two tokens keeps the first one's scope, as looking
        // the name up in the configuration finds it.
        let first = AgentToken::new("twice", &token(2001));
        let scope = Arc::clone(&first.scope);
        let second = AgentToken {
            scope: Scope::everything(),
            ..AgentToken::new("twice", &token(2002))
        };
        let tokens = Tokens::new(&Auth {
            api_token: None,
            agent_tokens: vec![first, second],
        });
        assert!(Arc::ptr_eq(&tokens.scopes["twice"], &scope));
        assert_eq!(
            tokens.principal(token(2002).as_bytes()),
            Some(Principal::Agent("twice".into()))
        );
    }

    fn server_with(api_token: Option<&str>, allowed_hosts: &[&str]) -> Arc<Server> {
        let broker = Broker::open(
            crate::store::Limits::default(),
            crate::broker::Storage::Memory,
        )
        .unwrap();
        Server::new(
            broker,
            ApiConfig {
                auth: Auth {
                    api_token: api_token.map(String::from),
                    agent_tokens: Vec::new(),
                },
                cors_origin: None,
                allowed_hosts: allowed_hosts.iter().map(|h| h.to_string()).collect(),
                max_body: 1 << 20,
            },
        )
    }

    fn head(lines: &str) -> Request {
        let head = format!("GET /api/agents HTTP/1.1\r\n{lines}\r\n");
        http::parse_head(head.as_bytes(), 1 << 20).ok().unwrap().0
    }

    #[test]
    fn a_host_is_matched_in_any_case_and_without_its_port() {
        let open = server_with(None, &[]);
        for host in [
            "localhost",
            "LocalHost:2790",
            "127.0.0.1:2790",
            "[::1]:2790",
            "[::FFFF:127.0.0.1]",
        ] {
            assert!(
                open.host_allowed(&head(&format!("Host: {host}\r\n"))),
                "{host}"
            );
        }
        for host in [
            "evil.example",
            "localhost.evil.example:2790",
            "127.0.0.1.evil.example",
            "[::2]:2790",
        ] {
            assert!(
                !open.host_allowed(&head(&format!("Host: {host}\r\n"))),
                "{host}"
            );
        }
        let named = server_with(None, &["postino.internal"]);
        for host in [
            "postino.internal",
            "Postino.INTERNAL:2790",
            "localhost:2790",
        ] {
            assert!(
                named.host_allowed(&head(&format!("Host: {host}\r\n"))),
                "{host}"
            );
        }
        for host in ["postino.internal.evil.example", "evil.example:2790"] {
            assert!(
                !named.host_allowed(&head(&format!("Host: {host}\r\n"))),
                "{host}"
            );
        }
    }

    #[test]
    fn a_token_is_read_from_either_header_and_a_bearer_one_comes_first() {
        let server = server_with(Some("admin-token-0123456789"), &[]);
        let peer = Some(IpAddr::from([127, 0, 0, 1]));
        let who = |lines: &str| server.principal(&head(lines), peer).ok();
        let good = "admin-token-0123456789";
        assert_eq!(
            who(&format!("Authorization: Bearer {good}\r\n")),
            Some(Principal::Admin)
        );
        assert_eq!(
            who(&format!("X-Postino-Token: {good}\r\n")),
            Some(Principal::Admin)
        );
        assert_eq!(
            who(&format!(
                "Authorization: Basic eDp5\r\nX-Postino-Token: {good}\r\n"
            )),
            Some(Principal::Admin),
            "an Authorization header of another kind leaves the token header to be read"
        );
        assert_eq!(
            who(&format!(
                "Authorization: Bearer wrong-token\r\nX-Postino-Token: {good}\r\n"
            )),
            None,
            "a bearer token is the one judged"
        );
        // The scheme's name in any case, and more than one space after it.
        for scheme in ["bearer", "BEARER", "bEaReR"] {
            assert_eq!(
                who(&format!("Authorization: {scheme} {good}\r\n")),
                Some(Principal::Admin),
                "{scheme}"
            );
        }
        assert_eq!(
            who(&format!("Authorization: Bearer   {good}\r\n")),
            Some(Principal::Admin)
        );
        assert_eq!(who(&format!("Authorization: Bearer{good}\r\n")), None);
        assert_eq!(who("Authorization: Bearer\r\n"), None);
        assert_eq!(who(""), None);
    }

    #[test]
    fn a_stream_without_a_token_is_answered_on_the_loop_not_handed_off() {
        use crate::event::Service;
        let broker = Broker::open(
            crate::store::Limits::default(),
            crate::broker::Storage::Memory,
        )
        .unwrap();
        let server = Server::new(
            broker,
            ApiConfig {
                auth: Auth {
                    api_token: Some("admin-token-0123456789".into()),
                    agent_tokens: Vec::new(),
                },
                cors_origin: None,
                allowed_hosts: Vec::new(),
                max_body: 1 << 20,
            },
        );
        let peer = Some(IpAddr::from([127, 0, 0, 1]));
        let request = |token: &str| {
            let head = format!(
                "GET /api/events HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {token}\r\n\r\n"
            );
            http::parse_head(head.as_bytes(), 1 << 20).ok().unwrap().0
        };
        let refused = server.answer(&request("wrong-token-0123456789"), peer);
        assert_eq!(refused.map(|r| r.status), Some(401));
        assert!(
            server
                .answer(&request("admin-token-0123456789"), peer)
                .is_none(),
            "a client let in moves to a thread for its stream"
        );
    }

    #[test]
    fn connections_fit_in_the_limit_on_open_files() {
        assert_eq!(connection_shares(None), (512, 448));
        assert_eq!(connection_shares(Some(1 << 20)), (512, 448));
        // The default of a service launchd starts on macOS.
        assert_eq!(connection_shares(Some(256)), (192, 168));
        assert_eq!(connection_shares(Some(10)), (1, 0));
    }

    #[test]
    fn repeated_failures_lock_an_address_out_for_the_window() {
        let lockout = Lockout::default();
        let (ip, other): (IpAddr, IpAddr) = (
            "203.0.113.7".parse().unwrap(),
            "203.0.113.8".parse().unwrap(),
        );
        for _ in 0..MAX_AUTH_FAILURES - 1 {
            lockout.fail(ip, 1_000);
        }
        assert_eq!(lockout.wait(ip, 1_000), None);
        lockout.fail(ip, 1_000);
        assert_eq!(lockout.wait(ip, 1_000), Some(60));
        assert_eq!(lockout.wait(ip, 31_000), Some(30));
        assert_eq!(
            lockout.wait(other, 1_000),
            None,
            "other addresses are unaffected"
        );
        assert_eq!(
            lockout.wait(ip, 61_000),
            None,
            "the lockout ends with its window"
        );
        lockout.fail(ip, 61_000);
        assert_eq!(
            lockout.wait(ip, 61_000),
            None,
            "a new window starts from zero"
        );
    }
}
