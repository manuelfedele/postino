//! Broker process lifecycle: storage, listener, maintenance, and discovery.

use std::fs;
use std::io::{self, ErrorKind, Write};
use std::net::{TcpListener, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde_json::json;

use crate::api::{ApiConfig, Server};
use crate::broker::{Broker, Storage};
use crate::config::{MIN_TOKEN_LEN, ServeConfig, StorageMode};
use crate::util::{VERSION, iso8601, now_ms, private_dir, private_file, random_hex, secret_hex};

const PORT_ATTEMPTS: u16 = 10;

pub struct Handle {
    pub url: String,
    pub broker: Arc<Broker>,
    pub server: Arc<Server>,
    pub data_dir: Option<PathBuf>,
}

pub fn broker_file(dir: &Path) -> PathBuf {
    dir.join("broker.json")
}

/// The administrator token of a broker that keeps it in its data directory,
/// created on first use (readable by this user only) when `create` is set.
pub fn local_token(dir: &Path, create: bool) -> io::Result<String> {
    let path = dir.join("token");
    match fs::read_to_string(&path) {
        Ok(token) if token.trim().len() >= MIN_TOKEN_LEN => return Ok(token.trim().to_string()),
        Ok(_) if !create => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "the token file is too short",
            ));
        }
        Err(e) if !create || e.kind() != ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    let token = secret_hex(32)?;
    let tmp = dir.join(format!("token.{}.tmp", random_hex(4)));
    private_file()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .and_then(|mut file| file.write_all(token.as_bytes()))?;
    // Two clients starting a broker at once: the first token written wins.
    match fs::hard_link(&tmp, &path) {
        Ok(()) => {
            let _ = fs::remove_file(&tmp);
            Ok(token)
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&tmp);
            local_token(dir, false)
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

fn bind(host: &str, port: u16, explicit: bool) -> Result<TcpListener, String> {
    let attempts = if explicit || port == 0 {
        1
    } else {
        PORT_ATTEMPTS
    };
    let mut last = String::new();
    for offset in 0..attempts {
        let candidate = port + offset;
        let addrs: Vec<_> = (host, candidate)
            .to_socket_addrs()
            .map_err(|e| format!("cannot resolve {host}: {e}"))?
            .collect();
        match TcpListener::bind(&addrs[..]) {
            Ok(listener) => {
                if offset > 0 {
                    eprintln!("postino: port {port} is in use, listening on {candidate}");
                }
                return Ok(listener);
            }
            Err(e) if e.kind() == ErrorKind::AddrInUse => {
                last = format!("port {candidate} is in use")
            }
            Err(e) => return Err(format!("cannot listen on {host}:{candidate}: {e}")),
        }
    }
    Err(format!("{last}; set POSTINO_PORT or --port"))
}

fn local_url(host: &str, port: u16) -> String {
    let host = match host {
        "0.0.0.0" => "127.0.0.1".to_string(),
        "::" | "[::]" => "[::1]".to_string(),
        h if h.contains(':') && !h.starts_with('[') => format!("[{h}]"),
        h => h.to_string(),
    };
    format!("http://{host}:{port}")
}

fn write_broker_file(
    dir: &Path,
    url: &str,
    broker: &Broker,
    local_token: bool,
) -> Result<(), String> {
    let info = json!({
        "url": url,
        "instance": broker.instance,
        "localToken": local_token,
        "pid": std::process::id(),
        "version": VERSION,
        "startedAt": iso8601(now_ms()),
    });
    let tmp = dir.join("broker.json.tmp");
    private_file()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .and_then(|mut file| file.write_all(info.to_string().as_bytes()))
        .and_then(|()| fs::rename(&tmp, broker_file(dir)))
        .map_err(|e| format!("cannot write broker.json: {e}"))
}

/// Opens storage, binds the listener, and starts serving on background threads.
pub fn spawn(mut cfg: ServeConfig) -> Result<Handle, String> {
    cfg.validate()?;
    if let StorageMode::Disk(dir) = &cfg.storage
        && cfg.local_token
        && cfg.auth.api_token.is_none()
    {
        private_dir(dir).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        let token = local_token(dir, true)
            .map_err(|e| format!("cannot read or create {}: {e}", dir.join("token").display()))?;
        cfg.auth.api_token = Some(token);
        cfg.auth.check()?;
    }
    let uses_local_token = cfg.local_token && cfg.auth.api_token.is_some();
    let (storage, data_dir) = match &cfg.storage {
        StorageMode::Memory => (Storage::Memory, None),
        StorageMode::Disk(dir) => (
            Storage::Disk {
                dir: dir.clone(),
                fsync: cfg.fsync,
            },
            Some(dir.clone()),
        ),
    };
    let broker = Broker::open(cfg.limits.clone(), storage)?;
    let listener = bind(&cfg.host, cfg.port, cfg.port_explicit)?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let url = local_url(&cfg.host, port);
    let server = Server::new(
        Arc::clone(&broker),
        ApiConfig {
            auth: cfg.auth.clone(),
            cors_origin: cfg.cors_origin.clone(),
            allowed_hosts: cfg.allowed_hosts.clone(),
            max_body: cfg.max_body,
        },
    );
    if let Some(dir) = &data_dir {
        write_broker_file(dir, &url, &broker, uses_local_token)?;
    }

    let maintenance = (Arc::clone(&broker), Arc::clone(&server), data_dir.clone());
    thread::Builder::new()
        .name("postino-tick".into())
        .spawn(move || {
            let (broker, server, data_dir) = maintenance;
            loop {
                thread::sleep(Duration::from_secs(1));
                broker.tick();
                if let Some(limit) = cfg.idle_timeout
                    && server.idle_for() >= limit
                    && server.open_streams() == 0
                    && broker.run(|core, now| core.live_session_count(now)) == 0
                {
                    eprintln!("postino: idle for {}s, shutting down", limit.as_secs());
                    broker.shutdown();
                    if let Some(dir) = &data_dir {
                        let _ = fs::remove_file(broker_file(dir));
                    }
                    std::process::exit(0);
                }
            }
        })
        .map_err(|e| e.to_string())?;

    let accept = Arc::clone(&server);
    thread::Builder::new()
        .name("postino-accept".into())
        .spawn(move || accept.serve(listener))
        .map_err(|e| e.to_string())?;

    Ok(Handle {
        url,
        broker,
        server,
        data_dir,
    })
}

pub fn run(cfg: ServeConfig) -> Result<(), String> {
    let host = cfg.host.clone();
    let handle = spawn(cfg)?;
    eprintln!("postino {VERSION}");
    if !crate::config::is_loopback(&host) {
        eprintln!(
            "  listen   {host}:{}",
            handle.url.rsplit(':').next().unwrap_or("")
        );
    }
    eprintln!("  api      {}/api", handle.url);
    eprintln!("  storage  {}", handle.broker.storage);
    eprintln!(
        "  auth     {}",
        if handle.server.cfg.auth.enabled() {
            "token required"
        } else {
            "open (loopback only)"
        }
    );
    loop {
        thread::park();
    }
}
