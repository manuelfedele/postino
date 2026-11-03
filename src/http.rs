//! A deliberately small HTTP/1.1 implementation: request parsing and response
//! writing for the broker, and a plain-HTTP client for the CLI and MCP adapter.
//! Limits are strict (16 KiB head, 64 headers, Content-Length bodies only), and
//! anything outside that subset is rejected rather than interpreted loosely.

use std::borrow::Cow;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::ops::Range;
use std::time::{Duration, Instant};

pub const MAX_HEAD: usize = 16 * 1024;
const MAX_HEADERS: usize = 64;
/// Time allowed to receive a request head once its first byte has arrived, so a
/// client trickling bytes cannot hold a connection open indefinitely.
pub const HEAD_DEADLINE: Duration = Duration::from_secs(15);
/// A body must arrive at no less than this rate, after the head's allowance.
const BODY_MIN_BYTES_PER_SEC: usize = 64 * 1024;

pub struct Request {
    /// The head as it came, with its header names in lower case: the
    /// method, the path, and each header are read where they are in it.
    head: String,
    method: Range<usize>,
    path: Range<usize>,
    /// Where each header's name and value are in the head.
    headers: Vec<(Range<usize>, Range<usize>)>,
    pub query: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub http10: bool,
    /// The path's segments, decoded once: a request's head is judged before
    /// its body is read, and again with it.
    segments: Vec<String>,
}

impl Request {
    pub fn method(&self) -> &str {
        &self.head[self.method.clone()]
    }

    pub fn path(&self) -> &str {
        &self.head[self.path.clone()]
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        values(&self.head, &self.headers, name).next()
    }

    pub fn query(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
            .filter(|value| !value.is_empty())
    }

    /// Every non-empty value of a repeated query parameter.
    pub fn query_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.query
            .iter()
            .filter(move |(key, value)| key == name && !value.is_empty())
            .map(|(_, value)| value.as_str())
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn keep_alive(&self) -> bool {
        let connection = self.header("connection").unwrap_or("").as_bytes();
        let says = |option: &str| {
            connection
                .windows(option.len())
                .any(|word| word.eq_ignore_ascii_case(option.as_bytes()))
        };
        if self.http10 {
            says("keep-alive")
        } else {
            !says("close")
        }
    }
}

/// The values of every header called `name`, a name in lower case.
fn values<'a>(
    head: &'a str,
    headers: &'a [(Range<usize>, Range<usize>)],
    name: &str,
) -> impl Iterator<Item = &'a str> {
    headers
        .iter()
        .filter(move |(key, _)| head[key.clone()] == *name)
        .map(move |(_, value)| &head[value.clone()])
}

pub enum ReadError {
    Closed,
    Status(u16, &'static str),
    /// Refused from its head; the body was read past, not kept.
    Refused(Box<Response>),
}

impl From<io::Error> for ReadError {
    fn from(_: io::Error) -> Self {
        ReadError::Closed
    }
}

fn is_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// Reads one line of the head onto the end of `head`, checking the deadline
/// between socket reads, so a line sent a byte at a time cannot outlast it (by
/// more than one read timeout). A line ends only at its newline, as
/// [`HeadScan`] reads it: one the client stops sending partway is none.
fn read_line(
    reader: &mut impl BufRead,
    head: &mut Vec<u8>,
    budget: &mut usize,
    deadline: Option<Instant>,
) -> Result<(), ReadError> {
    let start = head.len();
    loop {
        if deadline.is_some_and(|deadline| Instant::now() > deadline) {
            return Err(ReadError::Status(408, "request head took too long"));
        }
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Err(ReadError::Closed);
        }
        let (chunk, done) = match available.iter().position(|&b| b == b'\n') {
            Some(end) => (&available[..=end], true),
            None => (available, false),
        };
        if head.len() - start + chunk.len() > *budget {
            return Err(ReadError::Status(431, "request head too large"));
        }
        head.extend_from_slice(chunk);
        let used = chunk.len();
        reader.consume(used);
        if done {
            *budget -= head.len() - start;
            return Ok(());
        }
    }
}

fn trim_eol(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// Reads one request. A request with a body is first shown to `admit`, which
/// may refuse it from its head: its body is then read past without being
/// kept, so a client that will be refused can't make the server hold it.
pub fn read_request(
    reader: &mut impl BufRead,
    max_body: usize,
    admit: impl FnOnce(&Request) -> Option<Response>,
) -> Result<Request, ReadError> {
    // An idle keep-alive connection waits here, bounded by the socket's read
    // timeout; the head's deadline starts with its first byte.
    if reader.fill_buf()?.is_empty() {
        return Err(ReadError::Closed);
    }
    let started = Instant::now();
    let (mut request, length) = read_head(reader, max_body, Some(started + HEAD_DEADLINE))?;
    let refused = if length > 0 { admit(&request) } else { None };

    let body_deadline = started + HEAD_DEADLINE + body_time(length);
    let mut body = vec![0; if refused.is_some() { 0 } else { length }];
    let mut scratch = [0; 8192];
    let mut filled = 0;
    while filled < length {
        if Instant::now() > body_deadline {
            return Err(ReadError::Status(408, "request body took too long"));
        }
        let into = match refused {
            Some(_) => &mut scratch[..(length - filled).min(8192)],
            None => &mut body[filled..],
        };
        match reader.read(into)? {
            0 => return Err(ReadError::Closed),
            n => filled += n,
        }
    }
    if let Some(response) = refused {
        return Err(ReadError::Refused(Box::new(response)));
    }
    request.body = body;
    Ok(request)
}

/// How much longer than its head a body of `length` bytes may take to arrive.
pub fn body_time(length: usize) -> Duration {
    Duration::from_secs((length / BODY_MIN_BYTES_PER_SEC) as u64)
}

/// Reads a request's head through the blank line that ends it, checking
/// `deadline`, if it has one, between socket reads: the request, its body
/// not read yet, and the length the body has.
fn read_head(
    reader: &mut impl BufRead,
    max_body: usize,
    deadline: Option<Instant>,
) -> Result<(Request, usize), ReadError> {
    let mut budget = MAX_HEAD;
    // The head goes whole into one buffer, which the request keeps. Room for
    // most heads, so that it is not grown one step after another as their
    // first lines come in.
    let mut head = Vec::with_capacity(256);
    loop {
        head.clear();
        read_line(reader, &mut head, &mut budget, deadline)?;
        if !trim_eol(&head).is_empty() {
            break;
        }
    }
    loop {
        let start = head.len();
        read_line(reader, &mut head, &mut budget, deadline)?;
        if trim_eol(&head[start..]).is_empty() {
            break;
        }
    }
    parse(head, max_body)
}

/// The request a whole head describes, and the length its body has.
fn parse(head: Vec<u8>, max_body: usize) -> Result<(Request, usize), ReadError> {
    let first = head
        .iter()
        .position(|&b| b == b'\n')
        .map_or(head.len(), |at| at + 1);
    let mut head = String::from_utf8(head).map_err(|e| {
        ReadError::Status(
            400,
            match e.utf8_error().valid_up_to() < first {
                true => "invalid request line",
                false => "invalid header",
            },
        )
    })?;
    // Where the line from `at` to `end` ends once its line break is left off.
    let line_end =
        |head: &str, at: usize, end: usize| at + trim_eol(&head.as_bytes()[at..end]).len();
    let request_line = &head[..line_end(&head, 0, first)];
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(ReadError::Status(400, "invalid request line"));
    };
    let http10 = match version {
        "HTTP/1.1" => false,
        "HTTP/1.0" => true,
        _ => return Err(ReadError::Status(505, "HTTP version not supported")),
    };
    if !is_token(method) || !target.starts_with('/') || has_control(target) {
        return Err(ReadError::Status(400, "invalid request line"));
    }
    let method = 0..method.len();
    let target = method.end + 1..method.end + 1 + target.len();

    let mut headers = Vec::with_capacity(8);
    let mut at = first;
    loop {
        let end = head[at..].find('\n').map_or(head.len(), |n| at + n + 1);
        let line = &head[at..line_end(&head, at, end)];
        if line.is_empty() {
            break;
        }
        if headers.len() == MAX_HEADERS {
            return Err(ReadError::Status(431, "too many headers"));
        }
        let (name, value) = line
            .split_once(':')
            .filter(|(name, value)| is_token(name) && !has_control(value))
            .ok_or(ReadError::Status(400, "invalid header"))?;
        let from = at + name.len() + 1 + value.len() - value.trim_start_matches([' ', '\t']).len();
        let value = value.trim_matches([' ', '\t']);
        headers.push((at..at + name.len(), from..from + value.len()));
        at = end;
    }
    for (name, _) in &headers {
        head[name.clone()].make_ascii_lowercase();
    }

    if values(&head, &headers, "transfer-encoding")
        .next()
        .is_some()
    {
        return Err(ReadError::Status(
            501,
            "chunked request bodies are not supported; send Content-Length",
        ));
    }
    // Framing a proxy in front might read differently is refused: a length
    // that is not plain digits, or more than one Host.
    if values(&head, &headers, "host").count() > 1 {
        return Err(ReadError::Status(400, "more than one Host header"));
    }
    let mut length: Option<usize> = None;
    for value in values(&head, &headers, "content-length") {
        let parsed = Some(value)
            .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or(ReadError::Status(400, "invalid Content-Length"))?;
        if length.is_some_and(|l| l != parsed) {
            return Err(ReadError::Status(400, "conflicting Content-Length"));
        }
        length = Some(parsed);
    }
    let length = length.unwrap_or(0);
    if length > max_body {
        return Err(ReadError::Status(413, "request body too large"));
    }
    let (path, query) = head[target.clone()]
        .split_once('?')
        .unwrap_or((&head[target.clone()], ""));
    let query = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(key, true), percent_decode(value, true))
        })
        .collect();
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| percent_decode(segment, false))
        .collect();
    let path = target.start..target.start + path.len();
    let request = Request {
        head,
        method,
        path,
        headers,
        query,
        body: Vec::new(),
        http10,
        segments,
    };
    Ok((request, length))
}

/// Finds where a request head ends in bytes that arrive a few at a time,
/// for a server that reads without blocking. Each byte is looked at once,
/// however it is split across reads.
#[derive(Default)]
pub struct HeadScan {
    /// Where the line being read starts.
    line: usize,
    /// How far the search for its end has looked.
    scanned: usize,
    /// Whether the request line has been seen: blank lines before it are
    /// skipped, as [`read_request`] skips them.
    started: bool,
}

impl HeadScan {
    /// The length of the head at the start of `buf` once its blank line has
    /// arrived. `buf` only ever grows between calls.
    pub fn end(&mut self, buf: &[u8]) -> Option<usize> {
        while let Some(at) = buf[self.scanned..].iter().position(|&b| b == b'\n') {
            let newline = self.scanned + at;
            let blank = trim_eol(&buf[self.line..=newline]).is_empty();
            (self.line, self.scanned) = (newline + 1, newline + 1);
            if blank && self.started {
                return Some(newline + 1);
            }
            self.started |= !blank;
        }
        self.scanned = buf.len();
        None
    }
}

/// Whether bytes that have not ended a head yet already run past the size
/// a head may have.
pub fn head_too_long(bytes: usize) -> bool {
    bytes > MAX_HEAD
}

/// Whether `text` holds a control character other than a tab, such as a
/// bare CR or a NUL, which a proxy in front might read differently.
fn has_control(text: &str) -> bool {
    text.bytes().any(|b| (b < 0x20 && b != b'\t') || b == 0x7f)
}

/// Parses a head that has arrived whole, as [`HeadScan`] measured it: the
/// request without its body, and the length the body has. Leading blank
/// lines are skipped, as [`read_request`] skips them.
pub fn parse_head(head: &[u8], max_body: usize) -> Result<(Request, usize), ReadError> {
    if head.len() > MAX_HEAD {
        return Err(ReadError::Status(431, "request head too large"));
    }
    let mut start = 0;
    while let Some(at) = head[start..].iter().position(|&b| b == b'\n') {
        if !trim_eol(&head[start..=start + at]).is_empty() {
            break;
        }
        start += at + 1;
    }
    // A head is complete only once its blank line has arrived, as
    // `read_head` requires: a client that stops partway has sent none.
    let rest = &head[start..];
    let Some(last) = rest.len().checked_sub(1) else {
        return Err(ReadError::Closed);
    };
    if rest[last] != b'\n' {
        return Err(ReadError::Closed);
    }
    let start_of_line = rest[..last]
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |at| at + 1);
    if !trim_eol(&rest[start_of_line..]).is_empty() {
        return Err(ReadError::Closed);
    }
    parse(rest.to_vec(), max_body)
}

pub fn percent_decode(input: &str, plus_as_space: bool) -> String {
    // Most of what comes in has nothing to decode.
    if !input
        .bytes()
        .any(|b| b == b'%' || (plus_as_space && b == b'+'))
    {
        return input.to_string();
    }
    let hex = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'%'
            && let (Some(hi), Some(lo)) = (
                bytes.get(i + 1).copied().and_then(hex),
                bytes.get(i + 2).copied().and_then(hex),
            )
        {
            out.push(hi << 4 | lo);
            i += 3;
            continue;
        }
        out.push(if byte == b'+' && plus_as_space {
            b' '
        } else {
            byte
        });
        i += 1;
    }
    // What decodes to UTF-8, as nearly everything does, keeps its buffer.
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

pub fn percent_encode(input: &str) -> String {
    input
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// A response header: a name the server chose, and its value.
pub type Header = (&'static str, Cow<'static, str>);

pub struct Response {
    pub status: u16,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: u16, content_type: &'static str, body: impl Into<Vec<u8>>) -> Response {
        // Room for the headers every reply is given on its way out.
        let mut headers = Vec::with_capacity(4);
        headers.push(("Content-Type", Cow::Borrowed(content_type)));
        Response {
            status,
            headers,
            body: body.into(),
        }
    }

    pub fn json(status: u16, value: &serde_json::Value) -> Response {
        Response::new(status, "application/json", value.to_string())
    }

    pub fn empty(status: u16) -> Response {
        Response {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn with_header(
        mut self,
        name: &'static str,
        value: impl Into<Cow<'static, str>>,
    ) -> Response {
        self.headers.push((name, value.into()));
        self
    }
}

pub fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Content Too Large",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        _ => "Unknown",
    }
}

/// Header text with any line break taken out, so that no value can end its
/// header early and start another.
fn clean(value: &str) -> String {
    value.chars().filter(|c| *c != '\r' && *c != '\n').collect()
}

/// Appends header text as [`clean`] has it, without a copy.
fn push_clean(out: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    if bytes.iter().any(|&b| b == b'\r' || b == b'\n') {
        out.extend(bytes.iter().filter(|&&b| b != b'\r' && b != b'\n'));
    } else {
        out.extend_from_slice(bytes);
    }
}

fn push_number(out: &mut Vec<u8>, mut n: usize) {
    let mut digits = [0; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&digits[at..]);
}

fn push_head(out: &mut Vec<u8>, status: u16, headers: &[Header]) {
    out.extend_from_slice(b"HTTP/1.1 ");
    push_number(out, status as usize);
    out.push(b' ');
    out.extend_from_slice(reason(status).as_bytes());
    out.extend_from_slice(b"\r\n");
    for (name, value) in headers {
        push_clean(out, name);
        out.extend_from_slice(b": ");
        push_clean(out, value);
        out.extend_from_slice(b"\r\n");
    }
}

pub fn write_head(out: &mut impl Write, status: u16, headers: &[Header]) -> io::Result<()> {
    let mut head = Vec::with_capacity(256);
    push_head(&mut head, status, headers);
    head.extend_from_slice(b"\r\n");
    out.write_all(&head)
}

/// Appends a response's head: its headers, then its length and whether the
/// connection stays open.
fn push_response_head(out: &mut Vec<u8>, response: &Response, keep_alive: bool) {
    push_head(out, response.status, &response.headers);
    out.extend_from_slice(b"Content-Length: ");
    push_number(out, response.body.len());
    out.extend_from_slice(match keep_alive {
        true => b"\r\nConnection: keep-alive\r\n\r\n",
        false => b"\r\nConnection: close\r\n\r\n",
    });
}

/// Appends a whole response to `out`, as [`write_response`] would send it,
/// for a server that writes later.
pub fn encode_response(out: &mut Vec<u8>, response: &Response, keep_alive: bool, head_only: bool) {
    push_response_head(out, response, keep_alive);
    if !head_only {
        out.extend_from_slice(&response.body);
    }
}

pub fn write_response(
    out: &mut impl Write,
    response: &Response,
    keep_alive: bool,
    head_only: bool,
) -> io::Result<()> {
    let mut head = Vec::with_capacity(256);
    push_response_head(&mut head, response, keep_alive);
    let body: &[u8] = if head_only { &[] } else { &response.body };
    // Head and body in one write, so the client wakes once for the whole
    // response rather than for each part.
    let (mut head, mut body) = (head.as_slice(), body);
    while !head.is_empty() || !body.is_empty() {
        let written = out.write_vectored(&[io::IoSlice::new(head), io::IoSlice::new(body)])?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let from_head = written.min(head.len());
        head = &head[from_head..];
        body = &body[written - from_head..];
    }
    out.flush()
}

// ---- client ---------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub base: String,
}

impl Target {
    pub fn parse(url: &str) -> Result<Target, String> {
        let rest = url.strip_prefix("http://").ok_or_else(|| {
            if url.starts_with("https://") {
                "https URLs are not supported by the built-in client; run a local TLS proxy or use the broker over a private network".to_string()
            } else {
                format!("invalid broker URL {url:?}: expected http://host:port")
            }
        })?;
        let (authority, base) = match rest.split_once('/') {
            Some((authority, path)) => (authority, format!("/{}", path.trim_end_matches('/'))),
            None => (rest, String::new()),
        };
        let base = if base == "/" { String::new() } else { base };
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let (host, tail) = v6.split_once(']').ok_or("invalid IPv6 host")?;
            (host.to_string(), tail.strip_prefix(':').unwrap_or("80"))
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (host.to_string(), port),
                None => (authority.to_string(), "80"),
            }
        };
        let port = port
            .parse()
            .map_err(|_| format!("invalid port in {url:?}"))?;
        if host.is_empty() {
            return Err(format!("missing host in {url:?}"));
        }
        Ok(Target { host, port, base })
    }

    pub fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}{}", self.authority(), self.base)
    }
}

#[derive(Debug)]
pub enum ClientError {
    /// No connection was established; the request was not sent.
    Connect(io::Error),
    /// The request may have been sent; its outcome is unknown.
    Transport(io::Error),
}

pub struct ClientResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

fn connect(target: &Target, timeout: Duration) -> io::Result<TcpStream> {
    let mut last = io::Error::new(io::ErrorKind::NotFound, "host did not resolve");
    for addr in (target.host.as_str(), target.port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => last = e,
        }
    }
    Err(last)
}

fn write_request(
    stream: &mut TcpStream,
    target: &Target,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Option<&[u8]>,
) -> io::Result<()> {
    let mut head = format!(
        "{method} {}{path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: postino/{}\r\nConnection: close\r\n",
        target.base,
        target.authority(),
        crate::util::VERSION
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {}\r\n", clean(value)));
    }
    if let Some(body) = body {
        head.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    if let Some(body) = body {
        stream.write_all(body)?;
    }
    stream.flush()
}

fn read_status_and_headers(reader: &mut impl BufRead) -> io::Result<(u16, Vec<(String, String)>)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP response");
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let status = line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(invalid)?;
    let mut headers = Vec::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(invalid());
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            return Ok((status, headers));
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }
}

fn read_chunked(reader: &mut impl BufRead) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))?;
        if size == 0 {
            loop {
                line.clear();
                if reader.read_line(&mut line)? == 0 || line.trim().is_empty() {
                    return Ok(body);
                }
            }
        }
        let start = body.len();
        body.resize(start + size, 0);
        reader.read_exact(&mut body[start..])?;
        line.clear();
        reader.read_line(&mut line)?;
    }
}

pub fn send(
    target: &Target,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Option<&[u8]>,
    timeout: Duration,
) -> Result<ClientResponse, ClientError> {
    let mut stream = connect(target, Duration::from_secs(3)).map_err(ClientError::Connect)?;
    let transport = ClientError::Transport;
    stream.set_read_timeout(Some(timeout)).map_err(transport)?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(transport)?;
    write_request(&mut stream, target, method, path, headers, body).map_err(transport)?;
    let mut reader = BufReader::new(stream);
    let (status, response_headers) = read_status_and_headers(&mut reader).map_err(transport)?;
    let header = |name: &str| {
        response_headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    let body = if header("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        read_chunked(&mut reader).map_err(transport)?
    } else if let Some(length) = header("content-length").and_then(|v| v.parse::<usize>().ok()) {
        let mut body = vec![0; length];
        reader.read_exact(&mut body).map_err(transport)?;
        body
    } else {
        let mut body = Vec::new();
        reader.read_to_end(&mut body).map_err(transport)?;
        body
    };
    Ok(ClientResponse { status, body })
}

/// Opens a streaming GET (used for Server-Sent Events) and returns the reader
/// positioned at the start of the body.
pub fn open_stream(
    target: &Target,
    path: &str,
    headers: &[(&str, String)],
) -> Result<(u16, BufReader<TcpStream>), ClientError> {
    let mut stream = connect(target, Duration::from_secs(3)).map_err(ClientError::Connect)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .map_err(ClientError::Transport)?;
    write_request(&mut stream, target, "GET", path, headers, None)
        .map_err(ClientError::Transport)?;
    let mut reader = BufReader::new(stream);
    let (status, _) = read_status_and_headers(&mut reader).map_err(ClientError::Transport)?;
    Ok((status, reader))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> Result<Request, ReadError> {
        read_request(&mut raw.as_bytes(), 1024, |_| None)
    }

    #[test]
    fn responses_are_framed_and_keep_line_breaks_out_of_headers() {
        let response = Response::new(200, "application/json", "{}")
            .with_header("X-Note", "a\r\nX-Injected: 1".to_string());
        let mut out = Vec::new();
        encode_response(&mut out, &response, true, false);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Note: aX-Injected: 1\r\n\
             Content-Length: 2\r\nConnection: keep-alive\r\n\r\n{}"
        );
        let mut sent = Vec::new();
        write_response(&mut sent, &response, false, true).unwrap();
        let sent = String::from_utf8(sent).unwrap();
        assert!(
            sent.ends_with("Content-Length: 2\r\nConnection: close\r\n\r\n"),
            "{sent}"
        );
    }

    #[test]
    fn parses_requests_with_bodies_and_queries() {
        let request = parse(
            "POST /api/messages/a%20b/read?limit=5&x=a+b HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}",
        )
        .ok()
        .unwrap();
        assert_eq!(request.method(), "POST");
        assert_eq!(request.segments(), ["api", "messages", "a b", "read"]);
        assert_eq!(request.query("limit"), Some("5"));
        assert_eq!(request.query("x"), Some("a b"));
        assert_eq!(request.body, b"{}");
        assert!(request.keep_alive());
    }

    #[test]
    fn a_head_is_read_where_it_is_with_names_in_any_case() {
        let request = parse(
            "\r\n\nGET /api/db?x=1 HTTP/1.1\nHOST: example\r\nX-Pad:\t  two words \t\r\nx-empty:\r\n\r\n",
        )
        .ok()
        .unwrap();
        assert_eq!((request.method(), request.path()), ("GET", "/api/db"));
        assert_eq!(request.query("x"), Some("1"));
        assert_eq!(request.header("host"), Some("example"));
        assert_eq!(request.header("x-pad"), Some("two words"));
        assert_eq!(request.header("x-empty"), Some(""));
        assert_eq!(
            request.header("Host"),
            None,
            "names are looked up in lower case"
        );
        assert_eq!(request.header("x"), None);

        let keeps = |version: &str, connection: &str| {
            let raw = format!("GET / {version}\r\nConnection: {connection}\r\n\r\n");
            parse(&raw).ok().unwrap().keep_alive()
        };
        assert!(!keeps("HTTP/1.1", "CLOSE"));
        assert!(keeps("HTTP/1.1", "Keep-Alive"));
        assert!(keeps("HTTP/1.0", "Keep-Alive"));
        assert!(!keeps("HTTP/1.0", "close"));

        // Bytes that are not UTF-8 are refused, for the line they are in.
        let refused = |raw: &[u8]| match read_request(&mut &raw[..], 1024, |_| None) {
            Err(ReadError::Status(code, why)) => (code, why),
            _ => (0, ""),
        };
        assert_eq!(
            refused(b"GET /\xff HTTP/1.1\r\n\r\n"),
            (400, "invalid request line")
        );
        assert_eq!(
            refused(b"GET / HTTP/1.1\r\nX-A: \xff\r\n\r\n"),
            (400, "invalid header")
        );
    }

    #[test]
    fn rejects_unsupported_framing() {
        let status = |raw: &str| match parse(raw) {
            Err(ReadError::Status(code, _)) => code,
            _ => 0,
        };
        assert_eq!(
            status("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"),
            501
        );
        assert_eq!(
            status("POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n"),
            400
        );
        assert_eq!(
            status("POST / HTTP/1.1\r\nContent-Length: 4096\r\n\r\n"),
            413
        );
        for length in ["+2", "-0", "0x2", "2 2", ""] {
            let raw = format!("POST / HTTP/1.1\r\nContent-Length:{length}\r\n\r\n{{}}");
            assert_eq!(status(&raw), 400, "Content-Length {length:?}");
        }
        assert_eq!(status("GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n"), 400);
        assert_eq!(status("GET / HTTP/1.1\r\n folded\r\n\r\n"), 400);
        assert_eq!(status("GET http://evil/ HTTP/1.1\r\n\r\n"), 400);
        assert_eq!(status("GET / HTTP/2\r\n\r\n"), 505);
        let huge = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "a".repeat(MAX_HEAD));
        assert_eq!(status(&huge), 431);
        // Only spaces and tabs pad a value; other whitespace and control
        // characters are not read past.
        for length in ["5\u{0b}", "\u{a0}5", "5\u{85}", "5\u{3000}", "5\u{0}"] {
            let raw = format!("POST / HTTP/1.1\r\nContent-Length: {length}\r\n\r\nhello");
            assert_eq!(status(&raw), 400, "Content-Length {length:?}");
        }
        assert_eq!(
            status("POST / HTTP/1.1\r\nX-A: a\rContent-Length: 5\r\n\r\nhello"),
            400
        );
        assert_eq!(status("GET /a\u{1}b HTTP/1.1\r\n\r\n"), 400);
        let padded = parse("POST / HTTP/1.1\r\nContent-Length: \t5 \t\r\n\r\nhello");
        assert_eq!(padded.ok().unwrap().body, b"hello");
    }

    #[test]
    fn a_body_refused_from_its_head_is_read_past_not_kept() {
        /// Hands out bytes as a socket would, noting the largest read asked of it.
        struct Socket {
            data: io::Cursor<Vec<u8>>,
            largest: usize,
        }
        impl Read for Socket {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.largest = self.largest.max(buf.len());
                self.data.read(buf)
            }
        }
        impl BufRead for Socket {
            fn fill_buf(&mut self) -> io::Result<&[u8]> {
                self.data.fill_buf()
            }
            fn consume(&mut self, n: usize) {
                self.data.consume(n)
            }
        }
        let length = 1 << 20;
        let mut raw = format!("POST /api/db HTTP/1.1\r\nContent-Length: {length}\r\n\r\n");
        raw.push_str(&"x".repeat(length));
        raw.push_str("GET /next HTTP/1.1\r\n\r\n");
        let mut socket = Socket {
            data: io::Cursor::new(raw.into_bytes()),
            largest: 0,
        };
        let refused = read_request(&mut socket, length, |_| Some(Response::empty(401)));
        assert!(matches!(refused, Err(ReadError::Refused(r)) if r.status == 401));
        assert!(
            socket.largest <= 8192,
            "the body was read into a buffer of {} bytes",
            socket.largest
        );
        let next = read_request(&mut socket, length, |_| None).ok().unwrap();
        assert_eq!(next.path(), "/next", "the connection stays in step");
    }

    #[test]
    fn random_requests_never_panic() {
        const PIECES: &[&[u8]] = &[
            b"GET ",
            b"POST ",
            b"/api/db",
            b"/api/rooms/%2F/posts?x=%zz&y=+&",
            b" HTTP/1.1",
            b" HTTP/1.0",
            b"\r\n",
            b"\n",
            b"\r",
            b"Host: localhost",
            b"Host: a",
            b"Content-Length: ",
            b"Content-Length: 3",
            b"Content-Length: 99999999999999999999",
            b"Transfer-Encoding: chunked",
            b"X-Postino-Agent: alice",
            b"Connection: close",
            b": empty",
            b"Bad Header",
            b"\xff\xfe",
            b"\0",
            b"%",
            b"%4",
            b"{}",
            b"\t",
            b" ",
        ];
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut parsed = 0;
        for _ in 0..100_000 {
            let mut raw = Vec::new();
            if next() % 2 == 0 {
                raw.extend_from_slice(b"POST /api/db?a=1 HTTP/1.1\r\n");
            }
            for _ in 0..next() % 16 {
                if next() % 8 == 0 {
                    raw.push(next() as u8);
                } else {
                    raw.extend_from_slice(PIECES[next() as usize % PIECES.len()]);
                }
            }
            let refuse = next() % 2 == 0;
            let admit = |_: &Request| refuse.then(|| Response::empty(401));
            if let Ok(request) = read_request(&mut raw.as_slice(), 64, admit) {
                assert!(request.body.len() <= 64);
                assert!(!refuse || request.body.is_empty(), "a refused body is kept");
                let _ = (request.segments(), request.keep_alive(), request.query("x"));
                parsed += 1;
            }
        }
        assert!(parsed > 1000, "only {parsed} requests parsed");
    }

    #[test]
    fn decodes_percent_sequences_safely() {
        assert_eq!(percent_decode("a%2Fb", false), "a/b");
        assert_eq!(percent_decode("100%", false), "100%");
        assert_eq!(percent_decode("%zz%4", false), "%zz%4");
        assert_eq!(percent_decode("plain", false), "plain");
        assert_eq!(percent_decode("a+b", false), "a+b");
        assert_eq!(percent_decode("a+b", true), "a b");
        assert_eq!(percent_decode("%C3%A9", false), "é");
        assert_eq!(
            percent_decode("%FF", false),
            "\u{FFFD}",
            "never invalid UTF-8"
        );
        assert_eq!(percent_encode("a b/é"), "a%20b%2F%C3%A9");
    }

    #[test]
    fn parses_broker_urls() {
        let target = Target::parse("http://127.0.0.1:2790/").unwrap();
        assert_eq!(
            (target.host.as_str(), target.port, target.base.as_str()),
            ("127.0.0.1", 2790, "")
        );
        let v6 = Target::parse("http://[::1]:4000/relay").unwrap();
        assert_eq!(v6.authority(), "[::1]:4000");
        assert_eq!(v6.base, "/relay");
        assert!(Target::parse("https://example.com").is_err());
    }

    #[test]
    fn a_head_ends_at_the_same_byte_however_it_arrives_and_whichever_reads_it() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = |bound: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % bound as u64) as usize
        };
        let pieces: &[&[u8]] = &[
            b"GET ",
            b"POST ",
            b"/api/db",
            b"?a=%zz&b=+%41&",
            b"%",
            b" HTTP/1.1",
            b" HTTP/1.0",
            b" HTTP/2",
            b"\r\n",
            b"\n",
            b"\r",
            b"Host: h",
            b"Content-Length: 5",
            b"Content-Length: 99999999999999999999999",
            b"Content-Length: -1",
            b"Transfer-Encoding: chunked",
            b": no name",
            b"X: \x00",
            b"\x7f",
            b"\xff\xfe",
            b"\t",
            b" ",
            b"Connection: close",
            b"\xc3\xa9",
        ];
        // A head whose client stops partway through its blank line has not ended.
        let cut = b"GET / HTTP/1.1\r\nHost: h\r\n\r";
        assert_eq!(HeadScan::default().end(cut), None);
        assert!(matches!(
            read_head(&mut &cut[..], 1 << 20, None),
            Err(ReadError::Closed)
        ));
        assert!(matches!(parse_head(cut, 1 << 20), Err(ReadError::Closed)));
        assert!(
            matches!(
                parse_head(b"GET / HTTP/1.1\r\nHost: h\r\n", 1 << 20),
                Err(ReadError::Closed)
            ),
            "a head without its blank line has not ended"
        );
        for _ in 0..20_000 {
            let mut head = Vec::new();
            // Often a request line that parses, so that whole heads do too.
            if next(2) == 0 {
                head.extend_from_slice(b"GET /api/db HTTP/1.1\r\n");
            }
            for _ in 0..next(14) {
                head.extend_from_slice(pieces[next(pieces.len())]);
            }
            if next(2) == 0 {
                head.extend_from_slice(b"\r\n\r\n");
            }
            let whole = HeadScan::default().end(&head);
            let (mut scan, mut arrived, mut end) = (HeadScan::default(), 0, None);
            while end.is_none() && arrived < head.len() {
                arrived = (arrived + 1 + next(8)).min(head.len());
                end = scan.end(&head[..arrived]);
            }
            assert_eq!(end, whole, "{:?}", String::from_utf8_lossy(&head));
            // The parser reads a head to the same byte, and none that has not
            // ended, so a loop and a connection's own thread stay in step.
            let mut rest = &head[..];
            if read_head(&mut rest, 1 << 20, None).is_ok() {
                let read = head.len() - rest.len();
                assert_eq!(Some(read), end, "{:?}", String::from_utf8_lossy(&head));
            }
            if let Some(end) = end {
                let _ = parse_head(&head[..end], 1 << 20);
            }
        }
    }
}
