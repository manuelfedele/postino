//! A load generator for comparing servers on one workload. Each client holds
//! one connection and sends one command at a time, as an agent does, timing
//! every reply from the moment its request is written.
//!
//! ```text
//! cargo run --release --example bench -- http://127.0.0.1:2790 --op set --clients 16
//! cargo run --release --example bench -- resp://127.0.0.1:6390 --op get --clients 16
//! ```
//!
//! An `http://` target is a Postino server, sent `POST /api/db`; a `resp://`
//! target is any server that speaks RESP. Prints one JSON line: requests a
//! second, and latency percentiles in microseconds.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;

struct Options {
    target: String,
    op: String,
    clients: usize,
    seconds: f64,
    warmup: f64,
    keys: u64,
    value: usize,
    preload: bool,
}

fn options() -> Result<Options, String> {
    let mut args = std::env::args().skip(1);
    let mut options = Options {
        target: args.next().ok_or("usage: bench <http://host:port | resp://host:port> [--op set|get] [--clients N] [--seconds S] [--warmup S] [--keys N] [--value BYTES] [--preload]")?,
        op: "set".into(),
        clients: 16,
        seconds: 5.0,
        warmup: 1.0,
        keys: 100_000,
        value: 64,
        preload: false,
    };
    while let Some(flag) = args.next() {
        if flag == "--preload" {
            options.preload = true;
            continue;
        }
        let value = args.next().ok_or(format!("{flag} needs a value"))?;
        let number = || value.parse::<f64>().map_err(|e| format!("{flag}: {e}"));
        match flag.as_str() {
            "--op" => options.op = value.clone(),
            "--clients" => options.clients = number()? as usize,
            "--seconds" => options.seconds = number()?,
            "--warmup" => options.warmup = number()?,
            "--keys" => options.keys = number()? as u64,
            "--value" => options.value = number()? as usize,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if !matches!(options.op.as_str(), "set" | "get") {
        return Err(format!("unknown op {:?}: use set or get", options.op));
    }
    Ok(options)
}

/// One connection, speaking the target's protocol.
struct Connection {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    http: Option<String>,
    request: Vec<u8>,
    json: Vec<u8>,
    line: String,
    body: Vec<u8>,
}

impl Connection {
    fn open(target: &str) -> Result<Connection, String> {
        let (http, address) = match target.split_once("://") {
            Some(("http", address)) => (true, address),
            Some(("resp", address)) => (false, address),
            _ => return Err(format!("{target}: use http:// or resp://")),
        };
        let address = address.trim_end_matches('/');
        let stream = TcpStream::connect(address).map_err(|e| format!("{address}: {e}"))?;
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        Ok(Connection {
            reader: BufReader::new(stream.try_clone().map_err(|e| e.to_string())?),
            writer: stream,
            http: http.then(|| address.to_string()),
            request: Vec::new(),
            json: Vec::new(),
            line: String::new(),
            body: Vec::new(),
        })
    }

    /// Sends one command and reads its whole reply; returns whether it succeeded.
    fn call(&mut self, argv: &[&str]) -> std::io::Result<bool> {
        self.request.clear();
        match &self.http {
            Some(host) => {
                // Written straight into a buffer, as each RESP argument is.
                self.json.clear();
                serde_json::to_writer(&mut self.json, argv)?;
                write!(
                    self.request,
                    "POST /api/db HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    self.json.len()
                )?;
                self.request.extend_from_slice(&self.json);
            }
            None => {
                write!(self.request, "*{}\r\n", argv.len())?;
                for arg in argv {
                    write!(self.request, "${}\r\n{arg}\r\n", arg.len())?;
                }
            }
        }
        self.writer.write_all(&self.request)?;
        if self.http.is_some() {
            self.http_reply()
        } else {
            self.resp_reply()
        }
    }

    fn read_line(&mut self) -> std::io::Result<()> {
        self.line.clear();
        if self.reader.read_line(&mut self.line)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        Ok(())
    }

    fn http_reply(&mut self) -> std::io::Result<bool> {
        self.read_line()?;
        let ok = self.line.split(' ').nth(1) == Some("200");
        let mut length = 0;
        loop {
            self.read_line()?;
            let header = self.line.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap_or(0);
            }
        }
        self.body.resize(length, 0);
        self.reader.read_exact(&mut self.body)?;
        Ok(ok)
    }

    fn resp_reply(&mut self) -> std::io::Result<bool> {
        self.read_line()?;
        let line = self.line.trim_end();
        match line.as_bytes().first() {
            Some(b'$') => {
                let length: i64 = line[1..].parse().unwrap_or(-1);
                if length >= 0 {
                    self.body.resize(length as usize + 2, 0);
                    self.reader.read_exact(&mut self.body)?;
                }
                Ok(true)
            }
            Some(b'+' | b':') => Ok(true),
            _ => Ok(false),
        }
    }
}

/// A small, fast generator, seeded per client.
struct Random(u64);

impl Random {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

struct Tally {
    latencies: Vec<u64>,
    errors: u64,
}

fn main() {
    let options = match options() {
        Ok(options) => Arc::new(options),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let value = "x".repeat(options.value);
    if options.preload {
        preload(&options, &value);
    }
    let start = Instant::now();
    let from = start + Duration::from_secs_f64(options.warmup);
    let until = from + Duration::from_secs_f64(options.seconds);
    let failed = Arc::new(AtomicBool::new(false));
    let clients: Vec<_> = (0..options.clients)
        .map(|client| {
            let (options, value, failed) =
                (Arc::clone(&options), value.clone(), Arc::clone(&failed));
            thread::spawn(move || {
                let mut connection = match Connection::open(&options.target) {
                    Ok(connection) => connection,
                    Err(e) => {
                        eprintln!("{e}");
                        failed.store(true, Ordering::Relaxed);
                        return None;
                    }
                };
                let mut random =
                    Random(0x9e37_79b9_7f4a_7c15 ^ ((client as u64 + 1) * 0x2545_f491));
                let mut tally = Tally {
                    latencies: Vec::with_capacity(1 << 20),
                    errors: 0,
                };
                loop {
                    let key = format!("bench:{}", random.below(options.keys));
                    let argv: &[&str] = if options.op == "set" {
                        &["SET", &key, &value]
                    } else {
                        &["GET", &key]
                    };
                    let sent = Instant::now();
                    let ok = match connection.call(argv) {
                        Ok(ok) => ok,
                        Err(e) => {
                            eprintln!("connection failed: {e}");
                            failed.store(true, Ordering::Relaxed);
                            return None;
                        }
                    };
                    let done = Instant::now();
                    if done >= until {
                        return Some(tally);
                    }
                    if done >= from {
                        tally.latencies.push((done - sent).as_nanos() as u64);
                        tally.errors += u64::from(!ok);
                    }
                }
            })
        })
        .collect();
    let mut latencies = Vec::new();
    let mut errors = 0;
    for client in clients {
        if let Some(tally) = client.join().expect("a client panicked") {
            latencies.extend(tally.latencies);
            errors += tally.errors;
        }
    }
    if failed.load(Ordering::Relaxed) || latencies.is_empty() {
        std::process::exit(1);
    }
    latencies.sort_unstable();
    let percentile = |p: f64| {
        let index = ((latencies.len() as f64 * p).ceil() as usize).clamp(1, latencies.len());
        latencies[index - 1] as f64 / 1000.0
    };
    let report = json!({
        "target": options.target,
        "op": options.op,
        "clients": options.clients,
        "seconds": options.seconds,
        "requests": latencies.len(),
        "errors": errors,
        "throughput": latencies.len() as f64 / options.seconds,
        "latency_us": {
            "p50": percentile(0.50),
            "p90": percentile(0.90),
            "p99": percentile(0.99),
            "p999": percentile(0.999),
            "max": percentile(1.0),
        },
    });
    println!("{report}");
}

/// Writes every key once, from as many connections as there are clients, so
/// that reads find what they look for.
fn preload(options: &Arc<Options>, value: &str) {
    let workers: Vec<_> = (0..options.clients as u64)
        .map(|worker| {
            let (options, value) = (Arc::clone(options), value.to_string());
            thread::spawn(move || {
                let mut connection = Connection::open(&options.target).unwrap_or_else(|e| {
                    eprintln!("{e}");
                    std::process::exit(1);
                });
                let step = options.clients as u64;
                for n in (worker..options.keys).step_by(step as usize) {
                    let key = format!("bench:{n}");
                    if !matches!(connection.call(&["SET", &key, &value]), Ok(true)) {
                        eprintln!("preloading {key} failed");
                        std::process::exit(1);
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("a preloading client panicked");
    }
}
