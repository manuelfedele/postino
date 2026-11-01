use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::journal::Fsync;
use crate::memory;
use crate::scope::Scope;
use crate::store::Limits;
use crate::util::{fnv1a, home_dir, hostname, valid_name};

pub const DEFAULT_PORT: u16 = 2790;

/// The human operator's name. Only an administrator acts under it.
pub const OPERATOR: &str = "operator";

/// Whether `name` would pass for the operator: the name in any case, which no
/// agent may take.
pub fn reserved(name: &str) -> bool {
    name.eq_ignore_ascii_case(OPERATOR)
}

/// Tokens shorter than this are refused: they could be guessed.
pub const MIN_TOKEN_LEN: usize = 16;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// A secret from `NAME`, or from the file `NAME_FILE` points to, so tokens can
/// come from a secrets mount instead of the environment.
fn secret(name: &str) -> Result<Option<String>, String> {
    if let Some(value) = env(name) {
        return Ok(Some(value));
    }
    let Some(path) = env(&format!("{name}_FILE")) else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("cannot read {name}_FILE {path}: {e}"))?;
    Ok(Some(text.trim().to_string()).filter(|t| !t.is_empty()))
}

/// Tokens travel in an HTTP header: visible ASCII, long enough not to guess.
fn check_token(token: &str, what: &str) -> Result<(), String> {
    if !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(format!(
            "invalid {what}: use printable ASCII without spaces"
        ));
    }
    if token.len() < MIN_TOKEN_LEN {
        return Err(format!(
            "invalid {what}: use at least {MIN_TOKEN_LEN} characters; `postino token` prints a random one"
        ));
    }
    Ok(())
}

fn number(name: &str, value: &str, min: u64, max: u64) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|n| (min..=max).contains(n))
        .ok_or_else(|| format!("invalid {name}: expected an integer between {min} and {max}"))
}

fn env_number(name: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
    env(name).map_or(Ok(default), |value| number(name, &value, min, max))
}

/// Whether `host` names the loopback interface: `localhost` or a loopback
/// address. A DNS name that merely starts with `127.` is not loopback; treating
/// it as such would let a rebinding page reach an open server.
pub fn is_loopback(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost") || host.parse().is_ok_and(loopback_ip)
}

/// Whether `ip` is this machine, including IPv4 loopback seen through an
/// IPv6 socket as `::ffff:127.0.0.1`.
pub fn loopback_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback(),
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

pub fn default_data_dir() -> PathBuf {
    env("POSTINO_DATA_DIR")
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|home| home.join(".postino")))
        .unwrap_or_else(|| PathBuf::from(".postino"))
}

/// An agent's credential, bound to one name, and what it reaches.
#[derive(Clone, Debug)]
pub struct AgentToken {
    pub name: String,
    pub token: String,
    pub scope: Arc<Scope>,
}

impl AgentToken {
    pub fn new(name: &str, token: &str) -> AgentToken {
        AgentToken {
            name: name.to_string(),
            token: token.to_string(),
            scope: Arc::new(Scope::agent(name)),
        }
    }
}

#[derive(Clone, Default)]
pub struct Auth {
    pub api_token: Option<String>,
    pub agent_tokens: Vec<AgentToken>,
}

impl Auth {
    pub fn enabled(&self) -> bool {
        self.api_token.is_some() || !self.agent_tokens.is_empty()
    }

    /// The token bound to `name`, if one is configured.
    pub fn agent(&self, name: &str) -> Option<&AgentToken> {
        self.agent_tokens.iter().find(|t| t.name == name)
    }

    fn from_env() -> Result<Auth, String> {
        let auth = Auth {
            api_token: secret("POSTINO_API_TOKEN")?,
            agent_tokens: match secret("POSTINO_AGENT_TOKENS")? {
                Some(raw) => parse_agent_tokens(&raw)?,
                None => Vec::new(),
            },
        };
        auth.check()?;
        Ok(auth)
    }

    /// Every token long enough, and no token shared between two principals:
    /// a shared token would make the server pick one of them silently.
    pub fn check(&self) -> Result<(), String> {
        if let Some(token) = &self.api_token {
            check_token(token, "POSTINO_API_TOKEN")?;
        }
        let mut seen: Vec<&str> = self.api_token.iter().map(String::as_str).collect();
        for agent in &self.agent_tokens {
            check_token(&agent.token, &format!("token for agent {:?}", agent.name))?;
            if seen.contains(&agent.token.as_str()) {
                return Err(format!(
                    "the token for agent {:?} is also used by another principal; give every agent its own token",
                    agent.name
                ));
            }
            seen.push(&agent.token);
        }
        Ok(())
    }
}

/// `POSTINO_AGENT_TOKENS`: a JSON object from agent name to its token, or to
/// `{"token": ..., "keys": [...], "read": [...], "channels": [...]}`.
pub fn parse_agent_tokens(raw: &str) -> Result<Vec<AgentToken>, String> {
    let invalid = |why: String| format!("invalid POSTINO_AGENT_TOKENS: {why}");
    let parsed: serde_json::Map<String, Value> = serde_json::from_str(raw).map_err(|_| {
        invalid("expected a JSON object from agent name to token, or to {\"token\", \"keys\", \"read\", \"channels\"}".into())
    })?;
    let mut tokens = Vec::new();
    for (name, entry) in parsed {
        if !valid_name(&name) {
            return Err(invalid(format!("{name:?} is not a valid agent name")));
        }
        if reserved(&name) {
            return Err(invalid(format!(
                "{OPERATOR:?} is the human operator; it uses POSTINO_API_TOKEN"
            )));
        }
        let mut agent = AgentToken::new(&name, "");
        match entry {
            Value::String(token) => agent.token = token,
            Value::Object(fields) => {
                let patterns = |field: &str, default: &[String]| -> Result<Vec<String>, String> {
                    match fields.get(field) {
                        None => Ok(default.to_vec()),
                        Some(Value::Array(items)) => items
                            .iter()
                            .map(|item| match item.as_str() {
                                Some(p) if !p.is_empty() && p.len() <= 256 => Ok(p.to_string()),
                                _ => Err(invalid(format!(
                                    "{name}.{field} must list patterns of 1 to 256 characters"
                                ))),
                            })
                            .collect(),
                        Some(_) => Err(invalid(format!("{name}.{field} must be an array"))),
                    }
                };
                if let Some(unknown) = fields
                    .keys()
                    .find(|k| !["token", "keys", "read", "channels"].contains(&k.as_str()))
                {
                    return Err(invalid(format!(
                        "unknown field {name}.{unknown}; use token, keys, read, and channels"
                    )));
                }
                agent.token = fields
                    .get("token")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid(format!("{name}.token is required")))?
                    .to_string();
                let own = &agent.scope;
                agent.scope = Arc::new(Scope::new(
                    patterns("keys", &own.keys)?,
                    patterns("read", &own.read)?,
                    patterns("channels", &own.channels)?,
                ));
            }
            _ => {
                return Err(invalid(format!(
                    "the entry for {name:?} must be a token or an object"
                )));
            }
        }
        tokens.push(agent);
    }
    Ok(tokens)
}

#[derive(Clone)]
pub enum StorageMode {
    Memory,
    Disk(PathBuf),
}

#[derive(Clone)]
pub struct ServeConfig {
    pub host: String,
    pub port: u16,
    pub port_explicit: bool,
    pub storage: StorageMode,
    pub fsync: Fsync,
    pub limits: Limits,
    pub auth: Auth,
    pub cors_origin: Option<String>,
    pub allowed_hosts: Vec<String>,
    pub max_body: usize,
    pub idle_timeout: Option<Duration>,
    /// Protect a loopback broker with the administrator token kept in the data
    /// directory, as the clients do for a broker they start in the background.
    pub local_token: bool,
}

impl ServeConfig {
    pub fn from_env() -> Result<ServeConfig, String> {
        let port = env("POSTINO_PORT");
        let storage = match env("POSTINO_DATA_DIR").as_deref() {
            Some(":memory:") => StorageMode::Memory,
            _ => StorageMode::Disk(default_data_dir()),
        };
        let defaults = Limits::default();
        let seconds = |name: &str, default_ms: u64, min: u64, max: u64| {
            env_number(name, default_ms / 1000, min, max).map(|s| s * 1000)
        };
        let idle = env_number("POSTINO_IDLE_TIMEOUT", 0, 0, 30 * 86_400)?;
        Ok(ServeConfig {
            host: env("POSTINO_HOST").unwrap_or_else(|| "127.0.0.1".into()),
            port: match &port {
                Some(value) => number("POSTINO_PORT", value, 0, 65_535)? as u16,
                None => DEFAULT_PORT,
            },
            port_explicit: port.is_some(),
            storage,
            fsync: env("POSTINO_FSYNC").map_or(Ok(Fsync::EverySec), |v| v.parse())?,
            limits: Limits {
                msg_ttl_ms: seconds("POSTINO_MSG_TTL", defaults.msg_ttl_ms, 60, 365 * 86_400)?,
                lease_ms: seconds("POSTINO_MESSAGE_LEASE", defaults.lease_ms, 1, 3600)?,
                presence_ttl_ms: defaults.presence_ttl_ms,
                max_inbox: env_number("POSTINO_MAX_INBOX", defaults.max_inbox as u64, 1, 1_000_000)?
                    as usize,
                max_agents: env_number(
                    "POSTINO_MAX_AGENTS",
                    defaults.max_agents as u64,
                    1,
                    1_000_000,
                )? as usize,
                max_broadcasts: env_number(
                    "POSTINO_MAX_BROADCASTS",
                    defaults.max_broadcasts as u64,
                    1,
                    1_000_000,
                )? as usize,
                history_limit: env_number(
                    "POSTINO_HISTORY_LIMIT",
                    defaults.history_limit as u64,
                    0,
                    1_000_000,
                )? as usize,
                max_deliveries: env_number(
                    "POSTINO_MAX_DELIVERIES",
                    defaults.max_deliveries as u64,
                    1,
                    1000,
                )? as u32,
                max_rooms: env_number("POSTINO_MAX_ROOMS", defaults.max_rooms as u64, 1, 100_000)?
                    as usize,
                room_history: env_number(
                    "POSTINO_ROOM_HISTORY",
                    defaults.room_history as u64,
                    10,
                    1_000_000,
                )? as usize,
                max_keys: env_number(
                    "POSTINO_MAX_KEYS",
                    defaults.max_keys as u64,
                    1,
                    1_000_000_000,
                )? as usize,
                max_elements: env_number(
                    "POSTINO_MAX_ELEMENTS",
                    defaults.max_elements as u64,
                    1,
                    1_000_000_000,
                )? as usize,
                max_memory: match env("POSTINO_MAX_MEMORY") {
                    None => memory::default_limit(),
                    Some(value) => match memory::parse_size(&value) {
                        Some(0) => 0,
                        Some(bytes) if bytes >= 4 << 20 => bytes,
                        _ => {
                            return Err(
                                "invalid POSTINO_MAX_MEMORY: expected a size of at least 4mb, such as 512mb or 2gb, or 0 for no limit"
                                    .into(),
                            );
                        }
                    },
                },
            },
            auth: Auth::from_env()?,
            cors_origin: env("POSTINO_CORS_ORIGIN"),
            allowed_hosts: env("POSTINO_ALLOWED_HOSTS")
                .map(|v| {
                    v.split(',')
                        .map(|h| h.trim().to_ascii_lowercase())
                        .filter(|h| !h.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            max_body: env_number("POSTINO_MAX_BODY_BYTES", 65_536, 1024, 16 * 1024 * 1024)?
                as usize,
            idle_timeout: (idle > 0).then(|| Duration::from_secs(idle)),
            local_token: false,
        })
    }

    pub fn apply_args(&mut self, args: &[String]) -> Result<(), String> {
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            let mut value = |flag: &str| {
                iter.next()
                    .cloned()
                    .ok_or_else(|| format!("{flag} requires a value"))
            };
            match arg.as_str() {
                "--host" => self.host = value("--host")?,
                "--port" => {
                    self.port = number("--port", &value("--port")?, 0, 65_535)? as u16;
                    self.port_explicit = true;
                }
                "--data-dir" => {
                    self.storage = StorageMode::Disk(PathBuf::from(value("--data-dir")?))
                }
                "--memory" => self.storage = StorageMode::Memory,
                "--local-token" => self.local_token = true,
                "--fsync" => self.fsync = value("--fsync")?.parse()?,
                "--idle-timeout" => {
                    let secs = number("--idle-timeout", &value("--idle-timeout")?, 0, 30 * 86_400)?;
                    self.idle_timeout = (secs > 0).then(|| Duration::from_secs(secs));
                }
                other => return Err(format!("unknown option for serve: {other}")),
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.local_token && matches!(self.storage, StorageMode::Memory) {
            return Err("--local-token keeps the token in the data directory; it cannot be used with --memory".into());
        }
        if !is_loopback(&self.host) && !self.auth.enabled() {
            return Err(format!(
                "refusing to listen on {} without authentication: set POSTINO_API_TOKEN or POSTINO_AGENT_TOKENS",
                self.host
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ClientConfig {
    pub url: Option<String>,
    /// `POSTINO_TOKEN`: the token this client presents.
    pub token: Option<String>,
    /// `POSTINO_API_TOKEN`, which the command line falls back to. The MCP
    /// adapter never does: an agent must not inherit the operator's token.
    pub admin_token: Option<String>,
    pub data_dir: PathBuf,
    pub autostart: bool,
    /// Port probed on loopback when no discovery file is found.
    pub port: u16,
}

impl ClientConfig {
    /// The command line's view: it acts for the operator.
    pub fn from_env() -> ClientConfig {
        ClientConfig {
            url: env("POSTINO_URL"),
            token: env("POSTINO_TOKEN"),
            admin_token: env("POSTINO_API_TOKEN"),
            data_dir: default_data_dir(),
            autostart: env("POSTINO_AUTOSTART").is_none_or(|v| v != "false" && v != "0"),
            port: env("POSTINO_PORT")
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_PORT),
        }
    }

    /// The MCP adapter's view: only an agent's own token.
    pub fn for_agent() -> ClientConfig {
        ClientConfig {
            admin_token: None,
            ..ClientConfig::from_env()
        }
    }

    /// The token for a broker the configuration names or the loopback probe found.
    pub fn explicit_token(&self) -> Option<String> {
        self.token.clone().or_else(|| self.admin_token.clone())
    }
}

/// The agent identity for this process: `POSTINO_AGENT_NAME`, else the terminal
/// session, else a stable hash of host and working directory.
pub fn agent_name() -> Result<String, String> {
    if let Some(name) = env("POSTINO_AGENT_NAME") {
        return if valid_name(&name) {
            Ok(name)
        } else {
            Err(format!(
                "invalid POSTINO_AGENT_NAME {name:?}: use 1-64 letters, digits, '-', '_' or '.', starting with a letter or digit"
            ))
        };
    }
    if let Some(session) = env("TERM_SESSION_ID").or_else(|| env("ITERM_SESSION_ID")) {
        let short: String = session
            .rsplit(':')
            .next()
            .unwrap_or("")
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .take(8)
            .collect();
        if !short.is_empty() {
            return Ok(format!("agent-{}", short.to_ascii_lowercase()));
        }
    }
    let cwd = std::env::current_dir().unwrap_or_default();
    let seed = format!("{}:{}", hostname(), cwd.display());
    Ok(format!("agent-{:08x}", fnv1a(seed.as_bytes()) as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_means_an_address_not_a_name_prefix() {
        for host in [
            "127.0.0.1",
            "127.8.9.10",
            "localhost",
            "LOCALHOST",
            "::1",
            "[::1]",
            "::ffff:127.0.0.1",
        ] {
            assert!(is_loopback(host), "{host}");
        }
        for host in [
            "127.0.0.1.evil.example",
            "localhost.evil.example",
            "10.0.0.1",
            "evil",
        ] {
            assert!(!is_loopback(host), "{host}");
        }
    }

    #[test]
    fn agent_tokens_carry_a_scope() {
        let tokens = parse_agent_tokens(
            r#"{"alice": "alice-token-0123456789",
                "ci": {"token": "ci-token-0123456789", "keys": ["build:*"], "read": ["*"], "channels": ["builds"]}}"#,
        )
        .unwrap();
        let alice = tokens.iter().find(|t| t.name == "alice").unwrap();
        assert!(alice.scope.can_write("alice:plan") && alice.scope.can_write("shared:x"));
        assert!(!alice.scope.can_read("ci:log"));
        let ci = tokens.iter().find(|t| t.name == "ci").unwrap();
        assert!(ci.scope.can_write("build:42") && !ci.scope.can_write("alice:plan"));
        assert!(ci.scope.can_read("alice:plan"), "read patterns allow reads");
        assert!(ci.scope.can_use_channel("builds") && !ci.scope.can_use_channel("ci:x"));
    }

    #[test]
    fn weak_shared_and_reserved_tokens_are_refused() {
        assert!(parse_agent_tokens(r#"{"operator": "operator-token-0123456789"}"#).is_err());
        assert!(parse_agent_tokens(r#"{"Operator": "operator-token-0123456789"}"#).is_err());
        assert!(parse_agent_tokens(r#"{"a": {"token": "t", "scopes": []}}"#).is_err());
        assert!(parse_agent_tokens(r#"{"a": {"keys": ["x"]}}"#).is_err());
        assert!(parse_agent_tokens(r#"["not", "an", "object"]"#).is_err());
        let short = Auth {
            api_token: None,
            agent_tokens: parse_agent_tokens(r#"{"a": "change-me"}"#).unwrap(),
        };
        assert!(short.check().unwrap_err().contains("at least 16"));
        let shared = Auth {
            api_token: Some("same-token-0123456789".into()),
            agent_tokens: vec![AgentToken::new("a", "same-token-0123456789")],
        };
        assert!(shared.check().is_err());
        let spaced = Auth {
            api_token: Some("has a space in it 0123".into()),
            agent_tokens: Vec::new(),
        };
        assert!(spaced.check().is_err());
    }
}
