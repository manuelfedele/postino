//! Serving many connections from a few threads. Each loop owns a set of
//! connections whose sockets never block, and waits on all of them at once
//! with a [`Poller`]. Each time it wakes, it reads what has arrived on every
//! connection that is ready and answers each request that is complete;
//! then the service sees to what the replies depend on (the journal, for
//! Postino: one write for every change they reflect); then the replies go
//! out. A busy server so answers many requests each time a thread wakes,
//! where a thread per connection wakes once for every request. A request
//! that has to wait for something (a blocking pop, a stream of events)
//! leaves the loop with its connection, for a thread of its own.

use std::io::{self, Read, Write};
use std::net::{IpAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::http::{self, HeadScan, ReadError, Request, Response};
use crate::poll::{self, Poller};

/// Bytes a connection reads at a time.
const READ_SIZE: usize = 64 * 1024;
/// The token the poller reports the bell under; connections are reported
/// under their slot's number.
const BELL: usize = usize::MAX;
/// Reply bytes a connection may have waiting to be written before it stops
/// answering the requests behind them, so a client that sends requests
/// without reading the replies cannot make the server hold more for it. A
/// larger reply still goes out whole.
const MAX_OUTPUT: usize = 64 * 1024;
/// What a connection's buffers keep of their room once emptied, so one large
/// request or reply does not hold memory for as long as the connection lasts.
const KEEP: usize = 64 * 1024;
/// How long a connection goes without a byte in or out before it gives back
/// the room its emptied buffers kept.
const QUIET: Duration = Duration::from_secs(2);
/// How fast, in bytes a second, a client must take a large reply once
/// those that clients have not read take more room than [`Limits::unsent`]
/// leaves them: one slower is closed, and its reply dropped.
const MIN_DRAIN: f64 = 64.0 * 1024.0;
/// How long a client has, from when a large reply began to wait for it,
/// before it is judged by how fast it takes it.
const DRAIN_GRACE: Duration = Duration::from_secs(1);
/// How many times a loop with nothing to do looks again for ready
/// connections before it sleeps until one is. Sleeping and being woken cost
/// the kernel some microseconds: more than the few looks that find the next
/// request a moment after the last was answered.
const LOOKS: u32 = 8;

/// What a server does with requests; the loops do everything else.
pub trait Service: Send + Sync + 'static {
    /// Judges a request with a body from its head alone. A refusal is sent
    /// once the body has been read past, without keeping it, and the
    /// connection closes.
    fn refuse(&self, head: &Request, peer: Option<IpAddr>) -> Option<Response>;

    /// Answers a request, unless it has to wait for something: then `None`,
    /// and the request goes with its connection to [`Service::hand_off`].
    fn answer(&self, request: &Request, peer: Option<IpAddr>) -> Option<Response>;

    /// Serves a connection taken out of the loops, from the request that has
    /// to wait on, on a thread of its own. It must not block the loop.
    fn hand_off(self: Arc<Self>, moved: Moved);

    /// The response to a request that cannot be read, or took too long.
    fn failure(&self, status: u16, message: &'static str) -> Response;

    /// Runs on each loop's thread before it serves anything.
    fn serving(&self) {}

    /// Runs after a loop has answered the requests it read and before it
    /// writes the replies, or hands a connection off with replies unwritten.
    fn before_replies(&self) {}
}

/// A connection leaving the loops for a thread of its own.
pub struct Moved {
    /// Back in blocking mode.
    pub stream: TcpStream,
    pub request: Request,
    /// Replies to earlier requests not yet written: they go out first.
    pub output: Vec<u8>,
    /// What arrived after the request: the start of the next ones.
    pub input: Vec<u8>,
    /// Runs when the connection closes.
    pub on_close: OnClose,
}

/// Something to run once, when dropped: what a connection's owner does when
/// it closes, wherever that happens.
pub struct OnClose(Option<Box<dyn FnOnce() + Send>>);

impl OnClose {
    pub fn new(run: impl FnOnce() + Send + 'static) -> OnClose {
        OnClose(Some(Box::new(run)))
    }
}

impl Drop for OnClose {
    fn drop(&mut self) {
        if let Some(run) = self.0.take() {
            run();
        }
    }
}

#[derive(Clone, Copy)]
pub struct Limits {
    pub max_body: usize,
    /// How long a connection may send nothing, between requests or in one.
    pub idle: Duration,
    /// How long a request head may take to arrive, from its first byte. A
    /// body gets longer, by its size (see [`http::body_time`]).
    pub head: Duration,
    /// How long a reply may wait without the client taking any of it.
    pub write: Duration,
    /// The room the large replies of all connections may take while their
    /// clients have not read them. Past it, the connections that take
    /// theirs slowest are closed until the rest fit.
    pub unsent: usize,
}

/// The loops, and the connections on their way to them.
pub struct Loops {
    inboxes: Vec<Arc<Inbox>>,
    next: AtomicUsize,
    /// The room large replies take in all the loops' connections.
    unsent: Arc<AtomicUsize>,
}

/// Connections handed to a loop, and the bell that wakes it to take them.
struct Inbox {
    arrivals: Mutex<Vec<Conn>>,
    bell: UnixStream,
}

impl Loops {
    pub fn start<S: Service>(service: Arc<S>, threads: usize, limits: Limits) -> io::Result<Loops> {
        let mut inboxes = Vec::new();
        let unsent = Arc::new(AtomicUsize::new(0));
        for n in 0..threads.max(1) {
            let (bell, ear) = UnixStream::pair()?;
            bell.set_nonblocking(true)?;
            ear.set_nonblocking(true)?;
            let mut poller = Poller::new()?;
            poller.watch(ear.as_raw_fd(), BELL, 0, poll::READ)?;
            let inbox = Arc::new(Inbox {
                arrivals: Mutex::new(Vec::new()),
                bell,
            });
            let mut serving = Loop {
                service: Arc::clone(&service),
                inbox: Arc::clone(&inbox),
                ear,
                poller,
                limits,
                slots: Vec::new(),
                free: Vec::new(),
                unsent: Arc::clone(&unsent),
            };
            thread::Builder::new()
                .name(format!("postino-loop-{n}"))
                .spawn(move || {
                    while panic::catch_unwind(AssertUnwindSafe(|| serving.run())).is_err() {
                        eprintln!(
                            "postino: a connection loop failed; its connections are closed, and it starts again"
                        );
                        if serving.restart().is_err() {
                            return;
                        }
                    }
                })?;
            inboxes.push(inbox);
        }
        Ok(Loops {
            inboxes,
            next: AtomicUsize::new(0),
            unsent,
        })
    }

    /// Gives a connection to the loop whose turn it is. On error the
    /// connection is dropped, and closes.
    pub fn adopt(&self, stream: TcpStream, on_close: OnClose) -> io::Result<()> {
        stream.set_nonblocking(true)?;
        let _ = stream.set_nodelay(true);
        let conn = Conn::new(stream, on_close, Arc::clone(&self.unsent));
        let inbox = &self.inboxes[self.next.fetch_add(1, Ordering::Relaxed) % self.inboxes.len()];
        inbox
            .arrivals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(conn);
        // A full socket means the bell is ringing already.
        let _ = (&inbox.bell).write(&[1]);
        Ok(())
    }
}

/// Whether looking before sleeping pays (see [`LOOKS`]). Each time a loop's
/// looks find nothing, it looks half as many times the next, down to none;
/// then it looks once every sixteen waits, so that it starts looking again
/// when it is busy again. An idle or lightly loaded loop so sleeps at once.
struct Looking {
    looks: u32,
    waits: u32,
}

impl Looking {
    fn new() -> Looking {
        Looking {
            looks: LOOKS,
            waits: 0,
        }
    }

    /// How many times to look before this wait sleeps.
    fn looks(&mut self) -> u32 {
        self.waits = self.waits.wrapping_add(1);
        match self.looks {
            0 if self.waits.is_multiple_of(16) => 1,
            looks => looks,
        }
    }

    /// Whether the looks found a connection ready.
    fn found(&mut self, found: bool) {
        self.looks = if found { LOOKS } else { self.looks / 2 };
    }
}

/// One loop: its connections, each in a slot, and the poller that watches them.
struct Loop<S: Service> {
    service: Arc<S>,
    inbox: Arc<Inbox>,
    ear: UnixStream,
    poller: Poller,
    limits: Limits,
    slots: Vec<Option<Conn>>,
    free: Vec<usize>,
    unsent: Arc<AtomicUsize>,
}

impl<S: Service> Loop<S> {
    /// Closes every connection and watches afresh, for the bell alone.
    fn restart(&mut self) -> io::Result<()> {
        self.slots.clear();
        self.free.clear();
        self.poller = Poller::new()?;
        self.poller.watch(self.ear.as_raw_fd(), BELL, 0, poll::READ)
    }

    fn run(&mut self) {
        self.service.serving();
        let mut ready = Vec::new();
        let mut answered = Vec::new();
        let mut again = Vec::new();
        let mut scratch = vec![0; READ_SIZE];
        // Deadlines are seconds long: looking at them a few times a second
        // keeps each wake's work to the connections that are ready.
        let limits = self.limits;
        let sweep =
            (limits.idle.min(limits.head).min(limits.write) / 4).min(Duration::from_millis(250));
        let mut next_sweep = Instant::now() + sweep;
        let mut looking = Looking::new();
        loop {
            let timeout = match again.is_empty() {
                true => next_sweep.saturating_duration_since(Instant::now()),
                false => Duration::ZERO,
            };
            if let Err(e) = self.wait(&mut ready, timeout, &mut looking) {
                eprintln!("postino: waiting on connections failed: {e}");
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            let now = Instant::now();
            let mut bell = false;
            // Connections whose replies filled up answer the rest of what
            // they read, now that the replies are written.
            for slot in std::mem::take(&mut again) {
                self.receive(slot, false, now, &mut scratch, &mut answered);
            }
            for event in &ready {
                if event.token == BELL {
                    bell = true;
                } else {
                    self.receive(
                        event.token,
                        event.readable,
                        now,
                        &mut scratch,
                        &mut answered,
                    );
                }
            }
            if !answered.is_empty() {
                self.service.before_replies();
            }
            // Answering can take a while: what goes out is stamped with when
            // it went, so that a client taking it is not judged to be idle.
            let now = Instant::now();
            for slot in answered.drain(..) {
                let Some(conn) = self.slots[slot].as_mut() else {
                    continue;
                };
                conn.queued = false;
                let step = conn.reply(now, &*self.service, &limits);
                conn.count(now);
                if let Step::Again = step {
                    again.push(slot);
                }
                self.settle(slot, step);
            }
            if now >= next_sweep {
                next_sweep = now + sweep;
                for slot in 0..self.slots.len() {
                    let Some(conn) = self.slots[slot].as_mut() else {
                        continue;
                    };
                    if now >= conn.deadline(&limits) {
                        let step = conn.expire(now, &*self.service, &limits);
                        conn.count(now);
                        self.settle(slot, step);
                    } else {
                        conn.rest(now);
                        conn.count(now);
                    }
                }
            }
            if self.unsent.load(Ordering::Relaxed) > limits.unsent {
                self.close_slow_readers(now, limits.unsent);
            }
            if bell {
                self.welcome();
            }
        }
    }

    /// Waits for connections to be ready, or for the timeout to pass:
    /// looking a few times without sleeping first, while that finds them.
    fn wait(
        &mut self,
        ready: &mut Vec<poll::Event>,
        timeout: Duration,
        looking: &mut Looking,
    ) -> io::Result<()> {
        let looks = match timeout.is_zero() {
            true => 0,
            false => looking.looks(),
        };
        if looks > 0 {
            let found = (0..looks)
                .any(|_| self.poller.wait(ready, Duration::ZERO).is_ok() && !ready.is_empty());
            looking.found(found);
            if found {
                return Ok(());
            }
        }
        self.poller.wait(ready, timeout)
    }

    /// Reads and answers on a ready connection, and queues it to reply.
    fn receive(
        &mut self,
        slot: usize,
        readable: bool,
        now: Instant,
        scratch: &mut [u8],
        answered: &mut Vec<usize>,
    ) {
        let Some(conn) = self.slots.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        let received = conn.receive(readable, now, &*self.service, &self.limits, scratch);
        conn.count(now);
        match received {
            Ok(None) => {
                if !std::mem::replace(&mut conn.queued, true) {
                    answered.push(slot);
                }
            }
            Ok(Some(request)) => {
                self.service.before_replies();
                self.settle(slot, Step::HandOff(request));
            }
            Err(_) => self.settle(slot, Step::Close),
        }
    }

    /// Closes the connections that take large replies too slowly, slowest
    /// first, until what all the loops' connections hold for replies their
    /// clients have not read fits in `room` again.
    fn close_slow_readers(&mut self, now: Instant, room: usize) {
        let mut slow: Vec<(f64, usize)> = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(slot, conn)| Some((conn.as_ref()?.slow_drain(now)?, slot)))
            .collect();
        slow.sort_by(|a, b| a.0.total_cmp(&b.0));
        for (_, slot) in slow {
            if self.unsent.load(Ordering::Relaxed) <= room {
                break;
            }
            self.settle(slot, Step::Close);
        }
    }

    /// Takes the connections handed to this loop.
    fn welcome(&mut self) {
        let mut rings = [0; 64];
        while matches!((&self.ear).read(&mut rings), Ok(n) if n > 0) {}
        let arrivals = std::mem::take(
            &mut *self
                .inbox
                .arrivals
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        for mut conn in arrivals {
            let slot = self.free.pop().unwrap_or_else(|| {
                self.slots.push(None);
                self.slots.len() - 1
            });
            let interest = conn.interest();
            if self
                .poller
                .watch(conn.stream.as_raw_fd(), slot, 0, interest)
                .is_err()
            {
                self.free.push(slot);
                continue;
            }
            conn.registered = interest;
            self.slots[slot] = Some(conn);
        }
    }

    /// Does what a connection's step calls for: watches it for what it
    /// waits on now, closes it, or hands it off.
    fn settle(&mut self, slot: usize, step: Step) {
        let Some(conn) = self.slots[slot].as_mut() else {
            return;
        };
        let fd = conn.stream.as_raw_fd();
        if let Step::Keep | Step::Again = step {
            let interest = conn.interest();
            if interest == conn.registered {
                return;
            }
            if self
                .poller
                .watch(fd, slot, conn.registered, interest)
                .is_ok()
            {
                conn.registered = interest;
                return;
            }
        }
        let _ = self.poller.watch(fd, slot, conn.registered, 0);
        let conn = self.slots[slot].take().expect("the slot was full");
        self.free.push(slot);
        if let Step::HandOff(request) = step {
            Arc::clone(&self.service).hand_off(conn.hand_off(request));
        }
    }
}

enum Step {
    Keep,
    /// Kept, and answering again at once: requests it read wait for it.
    Again,
    Close,
    HandOff(Request),
}

/// Why a connection stopped answering for now.
enum Stop {
    /// It needs more bytes from the client.
    Input,
    /// Its replies must be written before it answers more.
    Output,
    /// It closes once its replies are written.
    Closing,
    HandOff(Request),
}

/// Where a connection is in the request it is reading.
enum Phase {
    /// Between requests.
    Idle,
    Head {
        started: Instant,
        scan: HeadScan,
    },
    Body {
        request: Request,
        length: usize,
        by: Instant,
    },
    /// The body of a refused request, read past and dropped.
    Refused {
        remaining: usize,
        response: Response,
        by: Instant,
    },
}

struct Conn {
    stream: TcpStream,
    peer: Option<IpAddr>,
    input: Vec<u8>,
    /// Where the part of `input` still to be answered starts.
    start: usize,
    output: Vec<u8>,
    written: usize,
    phase: Phase,
    /// When bytes last went in or out.
    since: Instant,
    /// The client closed its end: it sends nothing more.
    ended: bool,
    /// Close once the replies are written.
    closing: bool,
    /// What the poller watches this connection for.
    registered: u8,
    /// Waiting to write the replies its loop answered.
    queued: bool,
    /// Stopped answering because its replies filled up.
    full: bool,
    /// A request to hand off once the replies before it are written: until
    /// then the connection stays, and answers nothing more.
    moving: Option<Request>,
    on_close: OnClose,
    /// Bytes written to the client, ever.
    sent: u64,
    /// The room its replies take, once large, in what all connections hold.
    held: Held,
}

/// The room a connection's buffer of replies takes once it outgrows what
/// every connection keeps, counted with that of all the loops' connections
/// until the connection gives it back or ends.
struct Held {
    unsent: Arc<AtomicUsize>,
    bytes: usize,
    /// Since when it has held large replies, and what the connection had
    /// sent by then.
    since: Option<(Instant, u64)>,
}

impl Held {
    fn set(&mut self, bytes: usize, now: Instant, sent: u64) {
        if bytes > self.bytes {
            self.unsent.fetch_add(bytes - self.bytes, Ordering::Relaxed);
        } else if bytes < self.bytes {
            self.unsent.fetch_sub(self.bytes - bytes, Ordering::Relaxed);
        }
        self.bytes = bytes;
        self.since = match bytes {
            0 => None,
            _ => self.since.or(Some((now, sent))),
        };
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.unsent.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl Conn {
    fn new(stream: TcpStream, on_close: OnClose, unsent: Arc<AtomicUsize>) -> Conn {
        Conn {
            peer: stream.peer_addr().ok().map(|a| a.ip()),
            stream,
            input: Vec::new(),
            start: 0,
            output: Vec::new(),
            written: 0,
            phase: Phase::Idle,
            since: Instant::now(),
            ended: false,
            closing: false,
            registered: 0,
            queued: false,
            full: false,
            moving: None,
            on_close,
            sent: 0,
            held: Held {
                unsent,
                bytes: 0,
                since: None,
            },
        }
    }

    /// Counts the room its buffer of replies takes, once larger than every
    /// connection keeps, with what all the loops' connections hold.
    fn count(&mut self, now: Instant) {
        let room = self.output.capacity();
        self.held
            .set(if room > KEEP { room } else { 0 }, now, self.sent);
    }

    /// How fast the client takes the large replies waiting for it, in bytes
    /// a second, when that is too slow: it has had them for longer than
    /// [`DRAIN_GRACE`], and has taken none of them for as long, or takes
    /// them slower than [`MIN_DRAIN`]. The system takes the first of a
    /// reply at once, however slowly the client reads.
    fn slow_drain(&self, now: Instant) -> Option<f64> {
        let (since, sent) = self.held.since?;
        let waited = now.saturating_duration_since(since);
        if waited < DRAIN_GRACE {
            return None;
        }
        // Nothing comes in while replies wait, so the last byte to move
        // was one the client took.
        if now.saturating_duration_since(self.since) >= DRAIN_GRACE {
            return Some(0.0);
        }
        let rate = (self.sent - sent) as f64 / waited.as_secs_f64();
        (rate < MIN_DRAIN).then_some(rate)
    }

    fn pending(&self) -> bool {
        self.written < self.output.len()
    }

    /// Whether to read now. A connection with replies to write reads nothing
    /// until they are written, and one reads no more than its request needs,
    /// so a client cannot make the server hold more than that for it.
    fn wants_read(&self) -> bool {
        let need = match &self.phase {
            Phase::Body { length, .. } => length + READ_SIZE,
            Phase::Refused { .. } => READ_SIZE,
            Phase::Idle | Phase::Head { .. } => READ_SIZE.max(http::MAX_HEAD + 1),
        };
        !self.ended
            && !self.closing
            && self.moving.is_none()
            && !self.pending()
            && self.input.len() - self.start < need
    }

    fn interest(&self) -> u8 {
        let read = if self.wants_read() { poll::READ } else { 0 };
        let write = if self.pending() { poll::WRITE } else { 0 };
        read | write
    }

    /// When to give up on the client: it has sent nothing for too long, has
    /// not finished a request in time, or takes no part of a reply.
    fn deadline(&self, limits: &Limits) -> Instant {
        if self.pending() {
            return self.since + limits.write;
        }
        let silent = self.since + limits.idle;
        match &self.phase {
            Phase::Idle => silent,
            Phase::Head { started, .. } => silent.min(*started + limits.head),
            Phase::Body { by, .. } | Phase::Refused { by, .. } => silent.min(*by),
        }
    }

    /// Reads what has arrived and answers what it can, writing no reply
    /// yet: those go out once the service has seen to what they depend on.
    /// A request that has to wait comes back, to be handed off.
    fn receive<S: Service>(
        &mut self,
        readable: bool,
        now: Instant,
        service: &S,
        limits: &Limits,
        scratch: &mut [u8],
    ) -> io::Result<Option<Request>> {
        if readable && self.wants_read() {
            self.read(now, scratch)?;
        }
        match self.answer(service, limits, now) {
            // A thread is not left to write, blocking, what the loop can.
            Stop::HandOff(request) if self.pending() => {
                self.moving = Some(request);
                Ok(None)
            }
            Stop::HandOff(request) => Ok(Some(request)),
            Stop::Output => {
                self.full = true;
                Ok(None)
            }
            Stop::Input | Stop::Closing => Ok(None),
        }
    }

    /// Writes the replies answered so far, and says what comes next.
    fn reply<S: Service>(&mut self, now: Instant, service: &S, limits: &Limits) -> Step {
        if self.write(now).is_err() {
            return Step::Close;
        }
        if self.pending() {
            return Step::Keep;
        }
        if let Some(request) = self.moving.take() {
            return Step::HandOff(request);
        }
        if self.closing || self.ended {
            return Step::Close;
        }
        if std::mem::take(&mut self.full) {
            return Step::Again;
        }
        if now >= self.deadline(limits) {
            return self.expire(now, service, limits);
        }
        Step::Keep
    }

    /// A deadline passed. A client that went silent, or takes none of a
    /// reply, is dropped; one that is still sending a request too slowly is
    /// told so first.
    fn expire<S: Service>(&mut self, now: Instant, service: &S, limits: &Limits) -> Step {
        let message = match self.phase {
            _ if self.pending() || now >= self.since + limits.idle => return Step::Close,
            Phase::Idle => return Step::Close,
            Phase::Head { .. } => "request head took too long",
            Phase::Body { .. } | Phase::Refused { .. } => "request body took too long",
        };
        self.fail(service.failure(408, message));
        match self.write(now) {
            Ok(()) if self.pending() => Step::Keep,
            _ => Step::Close,
        }
    }

    fn read(&mut self, now: Instant, scratch: &mut [u8]) -> io::Result<()> {
        if self.start == self.input.len() {
            self.input.clear();
            self.input.shrink_to(KEEP);
            self.start = 0;
        } else if self.start >= READ_SIZE {
            self.input.drain(..self.start);
            self.start = 0;
        }
        loop {
            match (&self.stream).read(scratch) {
                Ok(0) => {
                    self.ended = true;
                    return Ok(());
                }
                Ok(n) => {
                    self.input.extend_from_slice(&scratch[..n]);
                    self.since = now;
                    // A short read took everything there was; poll reports
                    // what comes next, without another read to learn it.
                    if n < scratch.len() || !self.wants_read() {
                        return Ok(());
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    fn write(&mut self, now: Instant) -> io::Result<()> {
        while self.pending() {
            match (&self.stream).write(&self.output[self.written..]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    self.written += n;
                    self.sent += n as u64;
                    self.since = now;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        self.output.clear();
        self.output.shrink_to(KEEP);
        self.written = 0;
        Ok(())
    }

    /// Gives back the room of the buffers a quiet connection holds empty: a
    /// client that sends a request now and then would otherwise keep what
    /// its busiest moment took for as long as it stays.
    fn rest(&mut self, now: Instant) {
        if now < self.since + QUIET || !matches!(self.phase, Phase::Idle) || self.moving.is_some() {
            return;
        }
        if self.start == self.input.len() && self.input.capacity() > 0 {
            self.input = Vec::new();
            self.start = 0;
        }
        if !self.pending() && self.output.capacity() > 0 {
            self.output = Vec::new();
            self.written = 0;
        }
    }

    /// Queues a response that ends the connection.
    fn fail(&mut self, response: Response) {
        http::encode_response(&mut self.output, &response, false, false);
        self.closing = true;
    }

    /// Answers the requests that have arrived whole, in order, until one
    /// needs more bytes, replies pile up, or one has to wait.
    fn answer<S: Service>(&mut self, service: &S, limits: &Limits, now: Instant) -> Stop {
        loop {
            if self.closing {
                return Stop::Closing;
            }
            if self.moving.is_some() {
                return Stop::Input;
            }
            if self.output.len() - self.written >= MAX_OUTPUT {
                return Stop::Output;
            }
            let unread = self.input.len() - self.start;
            match &mut self.phase {
                Phase::Idle if unread == 0 => return Stop::Input,
                Phase::Idle => {
                    self.phase = Phase::Head {
                        started: now,
                        scan: HeadScan::default(),
                    }
                }
                Phase::Head { started, scan } => {
                    let started = *started;
                    let Some(end) = scan.end(&self.input[self.start..]) else {
                        if http::head_too_long(unread) {
                            self.fail(service.failure(431, "request head too large"));
                        }
                        return Stop::Input;
                    };
                    let head = &self.input[self.start..self.start + end];
                    let (request, length) = match http::parse_head(head, limits.max_body) {
                        Ok(parsed) => parsed,
                        Err(ReadError::Status(status, message)) => {
                            self.fail(service.failure(status, message));
                            continue;
                        }
                        Err(_) => {
                            self.closing = true;
                            continue;
                        }
                    };
                    self.start += end;
                    let by = started + limits.head + http::body_time(length);
                    let refusal = if length > 0 {
                        let peer = self.peer;
                        match panic::catch_unwind(AssertUnwindSafe(|| {
                            service.refuse(&request, peer)
                        })) {
                            Ok(refusal) => refusal,
                            Err(_) => Some(service.failure(500, "internal error")),
                        }
                    } else {
                        None
                    };
                    self.phase = match refusal {
                        Some(response) => Phase::Refused {
                            remaining: length,
                            response,
                            by,
                        },
                        None => Phase::Body {
                            request,
                            length,
                            by,
                        },
                    };
                }
                Phase::Body { length, .. } if unread < *length => return Stop::Input,
                Phase::Body { .. } => {
                    let Phase::Body {
                        mut request,
                        length,
                        ..
                    } = std::mem::replace(&mut self.phase, Phase::Idle)
                    else {
                        unreachable!()
                    };
                    request.body = self.input[self.start..self.start + length].to_vec();
                    self.start += length;
                    let peer = self.peer;
                    let answered =
                        panic::catch_unwind(AssertUnwindSafe(|| service.answer(&request, peer)));
                    let response = match answered {
                        Ok(Some(response)) => response,
                        Ok(None) => return Stop::HandOff(request),
                        Err(_) => {
                            self.fail(service.failure(500, "internal error"));
                            continue;
                        }
                    };
                    let keep_alive = request.keep_alive();
                    let head_only = request.method() == "HEAD";
                    http::encode_response(&mut self.output, &response, keep_alive, head_only);
                    self.closing = !keep_alive;
                }
                Phase::Refused { remaining, .. } => {
                    let skipped = (*remaining).min(unread);
                    self.start += skipped;
                    *remaining -= skipped;
                    if *remaining > 0 {
                        return Stop::Input;
                    }
                    let Phase::Refused { response, .. } =
                        std::mem::replace(&mut self.phase, Phase::Idle)
                    else {
                        unreachable!()
                    };
                    self.fail(response);
                }
            }
        }
    }

    fn hand_off(self, request: Request) -> Moved {
        let _ = self.stream.set_nonblocking(false);
        Moved {
            request,
            output: self.output[self.written..].to_vec(),
            input: self.input[self.start..].to_vec(),
            stream: self.stream,
            on_close: self.on_close,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::{SocketAddr, TcpListener};

    /// Answers with each request's path and body, padded to `?size=`;
    /// hands `/wait` off and refuses bodies sent to `/refuse`.
    #[derive(Default)]
    struct Echo {
        handed: Mutex<Vec<Moved>>,
    }

    impl Service for Echo {
        fn refuse(&self, head: &Request, _: Option<IpAddr>) -> Option<Response> {
            (head.path() == "/refuse").then(|| Response::new(401, "text/plain", "refused"))
        }

        fn answer(&self, request: &Request, _: Option<IpAddr>) -> Option<Response> {
            if request.path() == "/wait" {
                return None;
            }
            let size = request
                .query("size")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let mut body = format!(
                "{} {}",
                request.path(),
                String::from_utf8_lossy(&request.body)
            );
            body.extend(std::iter::repeat_n('.', size));
            Some(Response::new(200, "text/plain", body))
        }

        fn hand_off(self: Arc<Self>, moved: Moved) {
            self.handed.lock().unwrap().push(moved);
        }

        fn failure(&self, status: u16, message: &'static str) -> Response {
            Response::new(status, "text/plain", message)
        }
    }

    const LIMITS: Limits = Limits {
        max_body: 1 << 20,
        idle: Duration::from_secs(5),
        head: Duration::from_secs(5),
        write: Duration::from_secs(5),
        unsent: usize::MAX,
    };

    fn serve(limits: Limits) -> (Arc<Echo>, SocketAddr) {
        let echo = Arc::new(Echo::default());
        let loops = Loops::start(Arc::clone(&echo), 1, limits).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                loops.adopt(stream.unwrap(), OnClose::new(|| {})).unwrap();
            }
        });
        (echo, address)
    }

    fn connect(address: SocketAddr) -> (TcpStream, BufReader<TcpStream>) {
        let stream = TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        (stream, reader)
    }

    /// One response: its status and its body.
    fn response(reader: &mut BufReader<TcpStream>) -> (u16, String) {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let status = line.split(' ').nth(1).unwrap().parse().unwrap();
        let mut length = 0;
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if line.trim().is_empty() {
                break;
            }
            if let Some(value) = line.strip_prefix("Content-Length: ") {
                length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        (status, String::from_utf8(body).unwrap())
    }

    fn closed(reader: &mut BufReader<TcpStream>) -> bool {
        matches!(reader.read(&mut [0; 1]), Ok(0))
    }

    #[test]
    fn answers_requests_in_order_however_their_bytes_arrive() {
        let (_, address) = serve(LIMITS);
        let (mut stream, mut reader) = connect(address);
        let post = |path: &str, body: &str| {
            format!(
                "POST {path} HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        // Three in one write, then one a byte at a time.
        let three = post("/a", "1") + &post("/b", "") + "\r\n" + &post("/c", "33");
        stream.write_all(three.as_bytes()).unwrap();
        for byte in post("/d", "4444").bytes() {
            stream.write_all(&[byte]).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        for expected in ["/a 1", "/b ", "/c 33", "/d 4444"] {
            assert_eq!(response(&mut reader), (200, expected.to_string()));
        }
    }

    #[test]
    fn a_loop_looks_before_sleeping_only_while_that_finds_connections() {
        let mut looking = Looking::new();
        // Each wait whose looks find nothing halves the next one's.
        for expected in [LOOKS, LOOKS / 2, LOOKS / 4, LOOKS / 8] {
            assert_eq!(looking.looks(), expected);
            looking.found(false);
        }
        // An idle loop then looks once every sixteen waits.
        let mut looked = 0;
        for _ in 0..160 {
            let looks = looking.looks();
            if looks > 0 {
                looking.found(false);
            }
            looked += looks;
        }
        assert_eq!(looked, 10);
        // A look that finds a connection makes the next wait look as long as ever.
        looking.found(true);
        assert_eq!(looking.looks(), LOOKS);
    }

    #[test]
    fn a_quiet_connection_gives_back_the_room_its_buffers_took() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();
        let mut conn = Conn::new(stream, OnClose::new(|| {}), Arc::default());
        let since = Instant::now();
        conn.since = since;
        conn.input.extend_from_slice(&[b'x'; 50_000]);
        conn.output.extend_from_slice(&[b'y'; 50_000]);
        conn.rest(since + QUIET);
        assert!(
            conn.input.capacity() >= 50_000,
            "requests not answered stay"
        );
        assert!(conn.output.capacity() >= 50_000, "and replies not written");
        conn.start = conn.input.len();
        conn.written = conn.output.len();
        conn.rest(since + QUIET / 2);
        assert!(
            conn.input.capacity() > 0,
            "until the connection is quiet a while"
        );
        conn.rest(since + QUIET);
        assert_eq!((conn.input.capacity(), conn.output.capacity()), (0, 0));
        assert_eq!((conn.start, conn.written), (0, 0));
    }

    #[test]
    fn replies_a_client_has_not_read_hold_back_the_requests_behind_them() {
        let (_, address) = serve(LIMITS);
        let (mut stream, mut reader) = connect(address);
        // Forty replies of 100 KB: more than a connection may hold at once,
        // so the server stops answering until the client reads.
        let request = "GET /big?size=100000 HTTP/1.1\r\n\r\n".repeat(40);
        stream.write_all(request.as_bytes()).unwrap();
        thread::sleep(Duration::from_millis(200));
        for _ in 0..40 {
            let (status, body) = response(&mut reader);
            assert_eq!((status, body.len()), (200, "/big ".len() + 100_000));
        }
    }

    /// Whether the server has closed the connection, once what it sent
    /// before is read; one still open sends nothing more for a while.
    fn ends(reader: &mut BufReader<TcpStream>) -> bool {
        reader
            .get_ref()
            .set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        let mut buf = [0; 64 * 1024];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => return true,
                Ok(_) => {}
                Err(e) => return e.kind() == io::ErrorKind::ConnectionReset,
            }
        }
    }

    #[test]
    fn clients_that_leave_large_replies_unread_are_closed_once_those_take_too_much_room() {
        // Replies larger than the system's buffers for a connection hold,
        // so that part of each stays with the server.
        let room = 32 << 20;
        let echo = Arc::new(Echo::default());
        let loops = Loops::start(
            echo,
            1,
            Limits {
                unsent: room,
                ..LIMITS
            },
        )
        .unwrap();
        let unsent = Arc::clone(&loops.unsent);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                loops.adopt(stream.unwrap(), OnClose::new(|| {})).unwrap();
            }
        });
        // Six clients ask for 16 MB each and read none of it: together their
        // replies would take three times the room there is.
        let ask = "GET /big?size=16000000 HTTP/1.1\r\n\r\n";
        let mut idle: Vec<_> = (0..6)
            .map(|_| {
                let (mut stream, reader) = connect(address);
                stream.write_all(ask.as_bytes()).unwrap();
                (stream, reader)
            })
            .collect();
        // One that reads its reply as it comes is left to.
        let (mut stream, mut reader) = connect(address);
        stream.write_all(ask.as_bytes()).unwrap();
        let (status, body) = response(&mut reader);
        assert_eq!((status, body.len()), (200, "/big ".len() + 16_000_000));
        // Once they have taken nothing for a while, the clients reading
        // nothing are closed, until the replies left fit in the room.
        thread::sleep(DRAIN_GRACE + Duration::from_millis(600));
        let held = unsent.load(Ordering::Relaxed);
        assert!(held > 0 && held <= room, "{held} bytes held");
        let closed = idle
            .iter_mut()
            .map(|(_, reader)| ends(reader))
            .filter(|&ended| ended)
            .count();
        assert_eq!(closed, 4);
        // Those left are taken now, and give their room back.
        let started = Instant::now();
        while unsent.load(Ordering::Relaxed) > 0 {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "room not given back"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn a_refused_body_is_read_past_and_the_connection_closed() {
        let (_, address) = serve(LIMITS);
        let (mut stream, mut reader) = connect(address);
        let body = vec![b'x'; 300_000];
        let head = format!(
            "POST /refuse HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(&body).unwrap();
        assert_eq!(response(&mut reader), (401, "refused".to_string()));
        assert!(closed(&mut reader));
    }

    #[test]
    fn a_request_that_waits_leaves_once_the_replies_before_it_are_written() {
        let (echo, address) = serve(LIMITS);
        let (mut stream, mut reader) = connect(address);
        let requests = "GET /a HTTP/1.1\r\n\r\nGET /wait HTTP/1.1\r\n\r\nGET /b HTTP/1.1\r\n\r\n";
        stream.write_all(requests.as_bytes()).unwrap();
        assert_eq!(response(&mut reader), (200, "/a ".to_string()));
        let deadline = Instant::now() + Duration::from_secs(5);
        while echo.handed.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "nothing was handed off");
            thread::sleep(Duration::from_millis(5));
        }
        let moved = echo.handed.lock().unwrap().pop().unwrap();
        assert_eq!(moved.request.path(), "/wait");
        // The loop wrote the reply before it, so no thread writes it, blocking.
        assert!(moved.output.is_empty());
        assert_eq!(moved.input, b"GET /b HTTP/1.1\r\n\r\n");
    }

    #[test]
    fn a_malformed_or_oversized_head_is_answered_and_the_connection_closed() {
        let (_, address) = serve(LIMITS);
        let (mut stream, mut reader) = connect(address);
        stream.write_all(b"BOGUS\r\n\r\n").unwrap();
        assert_eq!(response(&mut reader).0, 400);
        assert!(closed(&mut reader));

        let (mut stream, mut reader) = connect(address);
        let long = format!("GET / HTTP/1.1\r\nX: {}\r\n", "y".repeat(20_000));
        stream.write_all(long.as_bytes()).unwrap();
        assert_eq!(response(&mut reader).0, 431);
        assert!(closed(&mut reader));
    }

    #[test]
    fn a_silent_client_is_dropped_and_a_slow_one_told_so() {
        let limits = Limits {
            idle: Duration::from_millis(300),
            head: Duration::from_millis(600),
            ..LIMITS
        };
        let (_, address) = serve(limits);
        let (_stream, mut reader) = connect(address);
        let started = Instant::now();
        assert!(closed(&mut reader));
        assert!(started.elapsed() >= Duration::from_millis(250));

        // A byte every 100 ms is never silent for long, but never ends the head.
        let (mut stream, mut reader) = connect(address);
        stream.write_all(b"GET / HTTP/1.1\r\n").unwrap();
        let trickle = thread::spawn(move || {
            for _ in 0..12 {
                thread::sleep(Duration::from_millis(100));
                if stream.write_all(b"x").is_err() {
                    break;
                }
            }
        });
        let (status, body) = response(&mut reader);
        assert_eq!((status, body.as_str()), (408, "request head took too long"));
        trickle.join().unwrap();
    }
}
