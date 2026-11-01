#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;

use postino::client::Client;
use postino::config::{Auth, ServeConfig, StorageMode};
use postino::serve::{self, Handle};

pub fn start(configure: impl FnOnce(&mut ServeConfig)) -> Handle {
    let mut cfg = ServeConfig::from_env().expect("config");
    cfg.host = "127.0.0.1".into();
    cfg.port = 0;
    cfg.port_explicit = true;
    cfg.storage = StorageMode::Memory;
    cfg.auth = Auth::default();
    cfg.allowed_hosts = Vec::new();
    cfg.cors_origin = None;
    cfg.idle_timeout = None;
    configure(&mut cfg);
    serve::spawn(cfg).expect("broker starts")
}

pub fn client(handle: &Handle) -> Client {
    Client::new(&handle.url, None).unwrap()
}

pub fn port(handle: &Handle) -> u16 {
    handle.url.rsplit(':').next().unwrap().parse().unwrap()
}

/// Sends a raw request (for headers the client sets itself, like Host) and
/// returns the status code and body.
pub fn raw(handle: &Handle, request: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port(handle))).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    let status = response
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

pub fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "postino-it-{name}-{}-{}",
        std::process::id(),
        postino::util::random_hex(4)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
