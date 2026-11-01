//! HTTP client for the broker, with local discovery and auto-start.

use std::fs;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::config::ClientConfig;
use crate::http::{self, ClientError, Target};
use crate::serve::{broker_file, local_token};
use crate::util::{private_dir, private_file};

const AUTOSTART_IDLE_SECS: &str = "1800";

#[derive(Debug)]
pub enum Error {
    /// The broker could not be reached; the request was not sent.
    Unreachable(String),
    /// The connection failed mid-request; the outcome is unknown.
    Transport(String),
    /// The broker answered with an error.
    Api {
        status: u16,
        message: String,
        body: Value,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Unreachable(m) | Error::Transport(m) => f.write_str(m),
            Error::Api { message, .. } => f.write_str(message),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    target: Target,
    token: Option<String>,
    /// Sent as `X-Postino-Agent`: the server then treats the token, even an
    /// administrator's, as this one agent.
    agent: Option<String>,
}

pub struct EventStream {
    reader: BufReader<TcpStream>,
}

pub struct SseEvent {
    pub id: Option<String>,
    pub data: String,
}

impl Iterator for EventStream {
    type Item = SseEvent;

    fn next(&mut self) -> Option<SseEvent> {
        let mut id = None;
        let mut data = String::new();
        let mut line = String::new();
        loop {
            line.clear();
            if self.reader.read_line(&mut line).ok()? == 0 {
                return None;
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if !data.is_empty() {
                    return Some(SseEvent { id, data });
                }
                continue;
            }
            if let Some(value) = line.strip_prefix("data:") {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(value.trim_start());
            } else if let Some(value) = line.strip_prefix("id:") {
                id = Some(value.trim().to_string());
            }
        }
    }
}

impl Client {
    pub fn new(url: &str, token: Option<String>) -> Result<Client, String> {
        Ok(Client {
            target: Target::parse(url)?,
            token,
            agent: None,
        })
    }

    /// This client acting as `agent`, with no more than that agent's rights.
    pub fn as_agent(&self, agent: &str) -> Client {
        Client {
            agent: Some(agent.to_string()),
            ..self.clone()
        }
    }

    pub fn url(&self) -> String {
        self.target.url()
    }

    fn headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![("Accept", "application/json".to_string())];
        if let Some(token) = &self.token {
            headers.push(("Authorization", format!("Bearer {token}")));
        }
        if let Some(agent) = &self.agent {
            headers.push(("X-Postino-Agent", agent.clone()));
        }
        headers
    }

    pub fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<Value, Error> {
        let payload = body.map(Value::to_string);
        let response = http::send(
            &self.target,
            method,
            path,
            &self.headers(),
            payload.as_deref().map(str::as_bytes),
            timeout,
        )
        .map_err(|e| match e {
            ClientError::Connect(e) => {
                Error::Unreachable(format!("cannot reach postino at {}: {e}", self.url()))
            }
            ClientError::Transport(e) => {
                Error::Transport(format!("connection to postino failed: {e}"))
            }
        })?;
        let value: Value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
        if (200..300).contains(&response.status) {
            return Ok(value);
        }
        let message = value
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("HTTP {}", response.status));
        Err(Error::Api {
            status: response.status,
            message,
            body: value,
        })
    }

    pub fn get(&self, path: &str) -> Result<Value, Error> {
        self.request("GET", path, None, Duration::from_secs(30))
    }

    pub fn post(&self, path: &str, body: &Value) -> Result<Value, Error> {
        self.request("POST", path, Some(body), Duration::from_secs(30))
    }

    pub fn delete(&self, path: &str) -> Result<Value, Error> {
        self.request("DELETE", path, None, Duration::from_secs(30))
    }

    pub fn health(&self) -> Option<Value> {
        self.request("GET", "/api/health", None, Duration::from_secs(2))
            .ok()
            .filter(|v| v.get("service").and_then(Value::as_str) == Some("postino"))
    }

    pub fn events(&self, path: &str) -> Result<EventStream, Error> {
        let mut headers = self.headers();
        headers[0] = ("Accept", "text/event-stream".to_string());
        let (status, reader) =
            http::open_stream(&self.target, path, &headers).map_err(|e| match e {
                ClientError::Connect(e) => {
                    Error::Unreachable(format!("cannot reach postino at {}: {e}", self.url()))
                }
                ClientError::Transport(e) => Error::Transport(e.to_string()),
            })?;
        if status != 200 {
            return Err(Error::Api {
                status,
                message: format!("event stream rejected with HTTP {status}"),
                body: Value::Null,
            });
        }
        Ok(EventStream { reader })
    }
}

/// The broker this data directory points to, if it still answers. A broker
/// that keeps its token in the directory gets that token, and only that broker:
/// the directory is private, so nobody else can have planted the address.
fn from_broker_file(cfg: &ClientConfig) -> Option<Client> {
    let raw = fs::read(broker_file(&cfg.data_dir)).ok()?;
    let info: Value = serde_json::from_slice(&raw).ok()?;
    let token = if info.get("localToken") == Some(&Value::Bool(true)) {
        cfg.token
            .clone()
            .or_else(|| local_token(&cfg.data_dir, false).ok())
    } else {
        cfg.explicit_token()
    };
    let client = Client::new(info.get("url")?.as_str()?, token).ok()?;
    let health = client.health()?;
    (health.get("instance") == info.get("instance")).then_some(client)
}

fn spawn_broker(cfg: &ClientConfig) -> Result<(), String> {
    let exe =
        std::env::current_exe().map_err(|e| format!("cannot locate the postino binary: {e}"))?;
    private_dir(&cfg.data_dir)
        .map_err(|e| format!("cannot create {}: {e}", cfg.data_dir.display()))?;
    local_token(&cfg.data_dir, true).map_err(|e| format!("cannot create the broker token: {e}"))?;
    let log = private_file()
        .create(true)
        .append(true)
        .open(cfg.data_dir.join("broker.log"))
        .map_err(|e| format!("cannot open broker.log: {e}"))?;
    let mut command = Command::new(exe);
    command
        .arg("serve")
        .arg("--data-dir")
        .arg(&cfg.data_dir)
        .arg("--local-token");
    if std::env::var_os("POSTINO_IDLE_TIMEOUT").is_none() {
        command.args(["--idle-timeout", AUTOSTART_IDLE_SECS]);
    }
    command
        .env_remove("POSTINO_URL")
        .env_remove("POSTINO_API_TOKEN")
        .env_remove("POSTINO_API_TOKEN_FILE")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Own process group: the broker outlives the agent session that started it.
        command.process_group(0);
    }
    let mut child = command
        .spawn()
        .map_err(|e| format!("cannot start broker: {e}"))?;
    thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}

/// Finds the broker: `POSTINO_URL`, then the local `broker.json`, then the
/// loopback port; starts a local broker when none answers and autostart is on.
pub fn connect(cfg: &ClientConfig) -> Result<Client, String> {
    if let Some(url) = &cfg.url {
        return Client::new(url, cfg.explicit_token());
    }
    if let Some(client) = from_broker_file(cfg) {
        return Ok(client);
    }
    let default = Client::new(
        &format!("http://127.0.0.1:{}", cfg.port),
        cfg.explicit_token(),
    )?;
    if default.health().is_some() {
        return Ok(default);
    }
    if !cfg.autostart {
        return Err(
            "no postino broker is running: start one with `postino serve` or set POSTINO_URL"
                .into(),
        );
    }
    spawn_broker(cfg)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        thread::sleep(Duration::from_millis(100));
        if let Some(client) = from_broker_file(cfg) {
            return Ok(client);
        }
    }
    Err(format!(
        "the broker did not start; see {}",
        cfg.data_dir.join("broker.log").display()
    ))
}
