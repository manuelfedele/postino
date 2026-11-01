use std::io::{IsTerminal, Read};
use std::process::ExitCode;

use serde_json::{Value, json};

use postino::client::{self, Client};
use postino::config::{ClientConfig, ServeConfig, agent_name, default_data_dir};
use postino::http::percent_encode;
use postino::util::{VERSION, secret_hex, valid_name};
use postino::{mcp, serve};

#[global_allocator]
static ALLOCATOR: postino::memory::Counting = postino::memory::Counting;

const HELP: &str = "postino - in-memory data and messaging server for agents

Usage:
  postino serve [--host H] [--port P] [--data-dir DIR | --memory]
                [--fsync always|everysec|never] [--idle-timeout SECS]
                [--local-token]        Run the server (HTTP API and events)
  postino mcp [--agent NAME]           MCP server over stdio for one agent
  postino db <command> [arg...]        Run a database command (lines from stdin without one)
  postino publish <channel> <text|->   Publish to a channel
  postino subscribe <channel>... [--pattern P]...
                                       Print messages published to channels
  postino tail <stream> [--from ID]    Follow a stream as entries are added
  postino send <to> <text|-> [--from NAME] [--reply-to ID] [--key KEY] [--strict]
  postino broadcast <text|-> [--from NAME] [--key KEY]
  postino read <agent> [--limit N] [--wait SECS] [--ack]
  postino ack <agent> <receipt>...
  postino rooms                        List rooms
  postino room <command> ...           Talk in the room of a workflow (postino room help)
  postino agents                       List agents and presence
  postino cleanup                      Forget offline agents with nothing pending
  postino status                       Broker health and totals
  postino watch [--agent NAME]         Stream live events
  postino config [--agent NAME]        Print an MCP server entry for this binary
  postino service <systemd|launchd>    Print a service definition
  postino repair [--data-dir DIR]      Cut a damaged journal at its first bad entry
  postino token                        Print a new random token for configuration
  postino version

Clients find the server through POSTINO_URL, then the local data directory
(~/.postino), and start a local server on demand. Documentation, including
every environment variable: https://manuelfedele.github.io/postino/";

const ROOM_HELP: &str = "postino room - rooms where the agents of a workflow talk

Usage:
  postino room show <room>
  postino room create <room> [--topic T] [--private] [--members a,b]
  postino room join|leave <room>
  postino room post <room> <text|-> [--reply-to ID] [--about REF]
  postino room read <room> [--wait SECS] [--limit N]
  postino room ask <room> <text|-> [--to a,b] [--options x,y] [--about REF] [--wait SECS]
  postino room answer <room> <question-id> [text|-] [--choice C]
  postino room board <room> [key [value|-]] [--version N] [--delete]
  postino room close|reopen|delete <room>

Every command accepts --as NAME to act as an agent (default POSTINO_AGENT_NAME,
else operator).";

type CliResult = Result<(), String>;

struct Flags {
    positional: Vec<String>,
    options: Vec<(String, Option<String>)>,
}

impl Flags {
    fn parse(args: &[String], switches: &[&str]) -> Flags {
        let mut flags = Flags {
            positional: Vec::new(),
            options: Vec::new(),
        };
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            if let Some(flag) = arg.strip_prefix("--") {
                if switches.contains(&flag) {
                    flags.options.push((flag.to_string(), None));
                } else {
                    flags.options.push((flag.to_string(), iter.next().cloned()));
                }
            } else {
                flags.positional.push(arg.clone());
            }
        }
        flags
    }

    fn value(&self, name: &str) -> Result<Option<String>, String> {
        match self.options.iter().find(|(flag, _)| flag == name) {
            Some((_, Some(value))) => Ok(Some(value.clone())),
            Some((_, None)) => Err(format!("--{name} requires a value")),
            None => Ok(None),
        }
    }

    fn values(&self, name: &str) -> Vec<String> {
        self.options
            .iter()
            .filter(|(flag, _)| flag == name)
            .filter_map(|(_, value)| value.clone())
            .collect()
    }

    fn has(&self, name: &str) -> bool {
        self.options.iter().any(|(flag, _)| flag == name)
    }

    fn check(&self, allowed: &[&str]) -> CliResult {
        match self
            .options
            .iter()
            .find(|(flag, _)| !allowed.contains(&flag.as_str()))
        {
            Some((flag, _)) => Err(format!("unknown option --{flag}")),
            None => Ok(()),
        }
    }

    fn number(&self, name: &str) -> Result<Option<u64>, String> {
        self.value(name)?
            .map(|v| v.parse().map_err(|_| format!("--{name} expects a number")))
            .transpose()
    }
}

fn client() -> Result<Client, String> {
    client::connect(&ClientConfig::from_env())
}

fn api(result: Result<Value, client::Error>) -> Result<Value, String> {
    result.map_err(|e| e.to_string())
}

fn text(arg: Option<&String>) -> Result<String, String> {
    match arg.map(String::as_str) {
        Some("-") | None => {
            let mut buffer = String::new();
            std::io::stdin()
                .read_to_string(&mut buffer)
                .map_err(|e| format!("cannot read stdin: {e}"))?;
            let trimmed = buffer.trim_end_matches('\n').to_string();
            if trimmed.is_empty() {
                Err("message text is empty".into())
            } else {
                Ok(trimmed)
            }
        }
        Some(text) => Ok(text.to_string()),
    }
}

fn sender(flags: &Flags) -> Result<Option<String>, String> {
    Ok(flags.value("from")?.or_else(|| {
        std::env::var("POSTINO_AGENT_NAME")
            .ok()
            .filter(|v| !v.is_empty())
    }))
}

fn list(value: Option<String>) -> Vec<String> {
    value
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn print(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

fn require_name(value: Option<&String>, what: &str) -> Result<String, String> {
    match value {
        Some(name) if valid_name(name) => Ok(name.clone()),
        Some(name) => Err(format!("invalid {what} {name:?}")),
        None => Err(format!("missing {what}")),
    }
}

fn mcp_config(flags: &Flags) -> CliResult {
    flags.check(&["agent"])?;
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut env = serde_json::Map::new();
    if let Some(agent) = flags.value("agent")? {
        require_name(Some(&agent), "agent name")?;
        env.insert("POSTINO_AGENT_NAME".into(), json!(agent));
    }
    print(&json!({ "postino": { "command": exe, "args": ["mcp"], "env": env } }));
    Ok(())
}

fn service(kind: Option<&String>) -> CliResult {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let exe = exe.display();
    match kind.map(String::as_str) {
        Some("systemd") => println!(
            "# /etc/systemd/system/postino.service
[Unit]
Description=Postino, an in-memory data and messaging server for agents
After=network-online.target
Wants=network-online.target

[Service]
ExecStart={exe} serve
Environment=POSTINO_DATA_DIR=/var/lib/postino
# Every local user can reach a server without a token. Create one with
#   postino token | sudo install -m 600 /dev/stdin /etc/postino/token
# and uncomment these lines. Listening beyond loopback requires it; terminate
# TLS at a reverse proxy.
# LoadCredential=api-token:/etc/postino/token
# Environment=POSTINO_API_TOKEN_FILE=%d/api-token
# Environment=POSTINO_HOST=0.0.0.0
DynamicUser=yes
StateDirectory=postino
Restart=on-failure
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallArchitectures=native

[Install]
WantedBy=multi-user.target"
        ),
        Some("launchd") => {
            let data = default_data_dir();
            println!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<!-- ~/Library/LaunchAgents/dev.postino.broker.plist
     launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dev.postino.broker.plist -->
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>dev.postino.broker</string>
  <key>ProgramArguments</key>
  <array><string>{exe}</string><string>serve</string><string>--local-token</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>{}</string>
</dict>
</plist>"#,
                data.join("broker.log").display()
            )
        }
        _ => return Err("usage: postino service <systemd|launchd>".into()),
    }
    Ok(())
}

fn watch(flags: &Flags) -> CliResult {
    flags.check(&["agent"])?;
    let path = match flags.value("agent")? {
        Some(agent) => format!("/api/events?agent={}", percent_encode(&agent)),
        None => "/api/events".to_string(),
    };
    let client = client()?;
    eprintln!("watching {} (Ctrl-C to stop)", client.url());
    for event in client.events(&path).map_err(|e| e.to_string())? {
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        let kind = value["type"].as_str().unwrap_or("?");
        let at = value["at"]
            .as_str()
            .map(|t| t.get(11..19).unwrap_or(t))
            .unwrap_or("        ");
        let detail = match kind {
            "message" => {
                let m = &value["message"];
                format!(
                    "{} -> {}  {}",
                    m["from"].as_str().unwrap_or("?"),
                    m["to"].as_str().unwrap_or("?"),
                    m["body"]
                        .as_str()
                        .unwrap_or("")
                        .replace('\n', " ")
                        .chars()
                        .take(100)
                        .collect::<String>()
                )
            }
            "broadcast" => {
                let b = &value["broadcast"];
                format!(
                    "{} -> *  {}",
                    b["from"].as_str().unwrap_or("?"),
                    b["body"].as_str().unwrap_or("")
                )
            }
            "post" => {
                let p = &value["post"];
                format!(
                    "{} @ {}  {}",
                    p["from"].as_str().unwrap_or("?"),
                    value["room"].as_str().unwrap_or("?"),
                    p["body"]
                        .as_str()
                        .unwrap_or("")
                        .replace('\n', " ")
                        .chars()
                        .take(100)
                        .collect::<String>()
                )
            }
            "presence" => format!(
                "{} {}",
                value["agent"].as_str().unwrap_or("?"),
                if value["online"].as_bool() == Some(true) {
                    "online"
                } else {
                    "offline"
                }
            ),
            "ready" | "resync" => continue,
            _ => {
                let mut fields = value.clone();
                if let Some(map) = fields.as_object_mut() {
                    for key in ["id", "type", "at"] {
                        map.remove(key);
                    }
                }
                fields.to_string()
            }
        };
        println!("{at}  {kind:<9} {detail}");
    }
    Err("event stream closed".into())
}

/// Prints a reply for people: strings as is, `(integer) n`, `(nil)`, and
/// numbered lists.
fn render(value: &Value, depth: usize) -> String {
    let pad = "   ".repeat(depth);
    match value {
        Value::Null => "(nil)".into(),
        Value::Number(n) => format!("(integer) {n}"),
        Value::String(s) if depth == 0 => s.clone(),
        Value::String(s) => format!("{s:?}"),
        Value::Bool(b) => b.to_string(),
        Value::Array(items) if items.is_empty() => "(empty list)".into(),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let inner = render(item, depth + 1);
                format!("{}{}) {}", if i == 0 { "" } else { &pad }, i + 1, inner)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(map) if map.is_empty() => "(empty map)".into(),
        Value::Object(map) => map
            .iter()
            .enumerate()
            .map(|(i, (k, v))| {
                format!(
                    "{}{k}: {}",
                    if i == 0 { "" } else { &pad },
                    render(v, depth + 1)
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

/// Splits a command line into arguments, honoring single and double quotes.
fn split_command(line: &str) -> Result<Vec<String>, String> {
    let (mut args, mut current) = (Vec::new(), String::new());
    let (mut open, mut quote) = (false, None);
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) if c == q => quote = None,
            Some('"') if c == '\\' => match chars.next() {
                Some('n') => current.push('\n'),
                Some('t') => current.push('\t'),
                Some(other) => current.push(other),
                None => return Err("dangling escape".into()),
            },
            Some(_) => current.push(c),
            None if c.is_whitespace() => {
                if open {
                    args.push(std::mem::take(&mut current));
                    open = false;
                }
            }
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                open = true;
            }
            None => {
                current.push(c);
                open = true;
            }
        }
    }
    if quote.is_some() {
        return Err("unbalanced quotes".into());
    }
    if open {
        args.push(current);
    }
    Ok(args)
}

fn db(args: &[String]) -> CliResult {
    let client = client()?;
    let run = |argv: &[String]| -> CliResult {
        let result = client.request(
            "POST",
            "/api/db",
            Some(&json!(argv)),
            std::time::Duration::from_secs(75),
        );
        match result {
            Ok(reply) => {
                println!("{}", render(&reply["result"], 0));
                Ok(())
            }
            Err(client::Error::Api { message, body, .. }) => {
                // The change was made though not yet saved: what it returned
                // (a popped value, say) is still the caller's.
                if let Some(result) = body.get("result") {
                    println!("{}", render(result, 0));
                }
                Err(message)
            }
            Err(e) => Err(e.to_string()),
        }
    };
    if !args.is_empty() {
        return run(args);
    }
    let mut failed = false;
    let interactive = std::io::stdin().is_terminal();
    loop {
        if interactive {
            eprint!("postino> ");
        }
        let mut line = String::new();
        if std::io::stdin()
            .read_line(&mut line)
            .map_err(|e| e.to_string())?
            == 0
        {
            break;
        }
        let argv = match split_command(&line) {
            Ok(argv) => argv,
            Err(e) => {
                eprintln!("(error) {e}");
                failed = true;
                continue;
            }
        };
        if argv.is_empty() || argv[0].starts_with('#') {
            continue;
        }
        if let Err(e) = run(&argv) {
            eprintln!("(error) {e}");
            failed = true;
        }
    }
    if failed {
        Err("some commands failed".into())
    } else {
        Ok(())
    }
}

fn subscribe(flags: &Flags) -> CliResult {
    flags.check(&["pattern"])?;
    let mut query: Vec<String> = flags
        .positional
        .iter()
        .map(|c| format!("channel={}", percent_encode(c)))
        .collect();
    query.extend(
        flags
            .values("pattern")
            .iter()
            .map(|p| format!("pattern={}", percent_encode(p))),
    );
    if query.is_empty() {
        return Err("usage: postino subscribe <channel>... [--pattern P]...".into());
    }
    let client = client()?;
    for event in client
        .events(&format!("/api/subscribe?{}", query.join("&")))
        .map_err(|e| e.to_string())?
    {
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        if value["type"] == "subscribed" {
            eprintln!("subscribed (Ctrl-C to stop)");
            continue;
        }
        println!(
            "{}  {}",
            value["channel"].as_str().unwrap_or("?"),
            value["message"].as_str().unwrap_or("")
        );
    }
    Err("subscription closed".into())
}

fn tail(flags: &Flags) -> CliResult {
    flags.check(&["from"])?;
    let key = flags
        .positional
        .first()
        .ok_or("usage: postino tail <stream> [--from ID]")?;
    let from = flags.value("from")?.unwrap_or_else(|| "$".into());
    let client = client()?;
    for event in client
        .events(&format!(
            "/api/tail/{}?from={}",
            percent_encode(key),
            percent_encode(&from)
        ))
        .map_err(|e| e.to_string())?
    {
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        if value["type"] == "error" {
            return Err(value["error"]
                .as_str()
                .unwrap_or("stream error")
                .to_string());
        }
        let fields: Vec<String> = value["fields"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
            .collect();
        println!(
            "{}  {}",
            value["id"].as_str().unwrap_or("?"),
            fields.join(" ")
        );
    }
    Err("stream closed".into())
}

fn rooms() -> CliResult {
    let result = api(client()?.get("/api/rooms"))?;
    let items = result["items"].as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        println!("no rooms");
    }
    for room in items {
        let questions = room["openQuestions"].as_u64().unwrap_or(0);
        println!(
            "{} {:<24} {:>3} members ({} online)  {:>4} posts{}  {}",
            if room["open"] == true { "●" } else { "○" },
            room["name"].as_str().unwrap_or("?"),
            room["memberCount"],
            room["onlineCount"],
            room["postCount"],
            if questions > 0 {
                format!("  {questions} open questions")
            } else {
                String::new()
            },
            room["topic"].as_str().unwrap_or(""),
        );
    }
    Ok(())
}

fn room(args: &[String]) -> CliResult {
    let command = args.first().map(String::as_str);
    if matches!(command, None | Some("help" | "--help" | "-h")) {
        println!("{ROOM_HELP}");
        return Ok(());
    }
    let rest = &args[1..];
    let flags = Flags::parse(rest, &["private", "delete"]);
    let name = require_name(flags.positional.first(), "room")?;
    let me = flags.value("as")?.or_else(|| {
        std::env::var("POSTINO_AGENT_NAME")
            .ok()
            .filter(|v| !v.is_empty())
    });
    let base = format!("/api/rooms/{}", percent_encode(&name));
    let me_query = me
        .as_deref()
        .map(|m| format!("from={0}&agent={0}", percent_encode(m)))
        .unwrap_or_default();
    let client = client()?;
    let timeout = |wait: u64| std::time::Duration::from_secs(wait + 15);
    match command.unwrap_or_default() {
        "show" => {
            flags.check(&["as"])?;
            print(&api(client.get(&format!("{base}?{me_query}")))?);
        }
        "create" => {
            flags.check(&["as", "topic", "private", "members"])?;
            let result = api(client.post(
                "/api/rooms",
                &json!({
                    "name": name,
                    "from": me,
                    "topic": flags.value("topic")?,
                    "private": flags.has("private"),
                    "members": list(flags.value("members")?),
                }),
            ))?;
            println!("{}", result["room"]["name"].as_str().unwrap_or(""));
        }
        "join" => {
            flags.check(&["as"])?;
            api(client.post(&format!("{base}/members"), &json!({ "from": me })))?;
        }
        "leave" => {
            flags.check(&["as"])?;
            let agent = me.clone().unwrap_or_else(|| "operator".into());
            api(client.delete(&format!(
                "{base}/members/{}?{me_query}",
                percent_encode(&agent)
            )))?;
        }
        "post" => {
            flags.check(&["as", "reply-to", "about"])?;
            let result = api(client.post(
                &format!("{base}/posts"),
                &json!({
                    "from": me,
                    "body": text(flags.positional.get(1))?,
                    "replyTo": flags.value("reply-to")?,
                    "about": flags.value("about")?,
                }),
            ))?;
            println!("{}", result["id"].as_str().unwrap_or(""));
        }
        "read" => {
            flags.check(&["as", "wait", "limit"])?;
            let wait = flags.number("wait")?.unwrap_or(0).min(60);
            let result = api(client.request(
                "POST",
                &format!("{base}/read"),
                Some(&json!({ "agent": me, "wait": wait, "limit": flags.number("limit")?.unwrap_or(50) })),
                timeout(wait),
            ))?;
            print(&result["items"]);
        }
        "ask" => {
            flags.check(&["as", "to", "options", "about", "wait"])?;
            let result = api(client.post(
                &format!("{base}/questions"),
                &json!({
                    "from": me,
                    "body": text(flags.positional.get(1))?,
                    "to": list(flags.value("to")?),
                    "options": list(flags.value("options")?),
                    "about": flags.value("about")?,
                }),
            ))?;
            let id = result["id"].as_str().unwrap_or_default().to_string();
            match flags.number("wait")? {
                Some(wait) => {
                    let wait = wait.min(60);
                    let settled = api(client.request(
                        "GET",
                        &format!("{base}/questions/{id}?{me_query}&wait={wait}"),
                        None,
                        timeout(wait),
                    ))?;
                    print(&settled["question"]);
                }
                None => println!("{id}"),
            }
        }
        "answer" => {
            flags.check(&["as", "choice"])?;
            let question = flags
                .positional
                .get(1)
                .ok_or("usage: postino room answer <room> <question-id> [text|-] [--choice C]")?;
            let choice = flags.value("choice")?;
            let body = match (flags.positional.get(2), &choice) {
                (None, Some(_)) => None,
                (arg, _) => Some(text(arg)?),
            };
            let result = api(client.post(
                &format!("{base}/questions/{}/answers", percent_encode(question)),
                &json!({ "from": me, "body": body, "choice": choice }),
            ))?;
            println!(
                "{}",
                if result["resolved"] == true {
                    "resolved"
                } else {
                    "answered"
                }
            );
        }
        "board" => {
            flags.check(&["as", "version", "delete"])?;
            let version = flags.number("version")?;
            match flags.positional.get(1) {
                None => print(&api(client.get(&format!("{base}/board?{me_query}")))?["items"]),
                Some(key) if flags.has("delete") => {
                    let mut path = format!("{base}/board/{}?{me_query}", percent_encode(key));
                    if let Some(version) = version {
                        path.push_str(&format!("&version={version}"));
                    }
                    api(client.delete(&path))?;
                }
                Some(key) if flags.positional.len() > 2 => {
                    let result = api(client.request(
                        "PUT",
                        &format!("{base}/board/{}", percent_encode(key)),
                        Some(&json!({
                            "from": me,
                            "value": text(flags.positional.get(2))?,
                            "version": version,
                        })),
                        timeout(0),
                    ))?;
                    println!("{}", result["version"]);
                }
                Some(key) => {
                    let board = api(client.get(&format!("{base}/board?{me_query}")))?;
                    let entry = board["items"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|e| e["key"] == key.as_str())
                        .ok_or_else(|| format!("{key} is not on the board"))?;
                    println!("{}", entry["value"].as_str().unwrap_or(""));
                }
            }
        }
        command @ ("close" | "reopen") => {
            flags.check(&["as"])?;
            api(client.request(
                "PATCH",
                &base,
                Some(&json!({ "from": me, "closed": command == "close" })),
                timeout(0),
            ))?;
        }
        "delete" => {
            flags.check(&["as"])?;
            api(client.delete(&format!("{base}?{me_query}")))?;
        }
        other => return Err(format!("unknown room command {other:?}\n\n{ROOM_HELP}")),
    }
    Ok(())
}

fn run(args: &[String]) -> CliResult {
    let command = args.first().map(String::as_str);
    let rest = if args.is_empty() { &[][..] } else { &args[1..] };
    match command {
        Some("serve") => {
            let mut cfg = ServeConfig::from_env()?;
            cfg.apply_args(rest)?;
            serve::run(cfg)
        }
        None if std::io::stdin().is_terminal() => {
            println!("{HELP}");
            Ok(())
        }
        Some("mcp") | None => {
            let flags = Flags::parse(rest, &[]);
            flags.check(&["agent"])?;
            let name = match flags.value("agent")? {
                Some(name) => require_name(Some(&name), "agent name")?,
                None => agent_name()?,
            };
            mcp::run(ClientConfig::for_agent(), name)
        }
        Some("send") => {
            let flags = Flags::parse(rest, &["strict"]);
            flags.check(&["from", "reply-to", "key", "strict"])?;
            let to = require_name(flags.positional.first(), "recipient")?;
            let body = text(flags.positional.get(1))?;
            let result = api(client()?.post(
                "/api/messages",
                &json!({
                    "to": to,
                    "from": sender(&flags)?,
                    "body": body,
                    "replyTo": flags.value("reply-to")?,
                    "key": flags.value("key")?,
                    "strict": flags.has("strict"),
                }),
            ))?;
            println!("{}", result["id"].as_str().unwrap_or(""));
            Ok(())
        }
        Some("broadcast") => {
            let flags = Flags::parse(rest, &[]);
            flags.check(&["from", "key"])?;
            let body = text(flags.positional.first())?;
            let result = api(client()?.post(
                "/api/broadcasts",
                &json!({ "from": sender(&flags)?, "body": body, "key": flags.value("key")? }),
            ))?;
            println!("{}", result["id"].as_str().unwrap_or(""));
            Ok(())
        }
        Some("read") => {
            let flags = Flags::parse(rest, &["ack"]);
            flags.check(&["limit", "wait", "ack"])?;
            let agent = require_name(flags.positional.first(), "agent")?;
            let wait = flags.number("wait")?.unwrap_or(0).min(60);
            let client = client()?;
            let leased = api(client.request(
                "POST",
                &format!("/api/messages/{}/read", percent_encode(&agent)),
                Some(&json!({ "limit": flags.number("limit")?.unwrap_or(10), "wait": wait })),
                std::time::Duration::from_secs(wait + 15),
            ))?;
            if flags.has("ack") {
                let receipts: Vec<Value> = leased["items"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|m| m["receipt"].clone())
                    .collect();
                if !receipts.is_empty() {
                    api(client.post(
                        &format!("/api/messages/{}/ack", percent_encode(&agent)),
                        &json!({ "receipts": receipts }),
                    ))?;
                }
            }
            print(&leased["items"]);
            Ok(())
        }
        Some("ack") => {
            let agent = require_name(rest.first(), "agent")?;
            if rest.len() < 2 {
                return Err("usage: postino ack <agent> <receipt>...".into());
            }
            let result = api(client()?.post(
                &format!("/api/messages/{}/ack", percent_encode(&agent)),
                &json!({ "receipts": &rest[1..] }),
            ))?;
            print(&result);
            Ok(())
        }
        Some("agents") => {
            let result = api(client()?.get("/api/agents"))?;
            let items = result["items"].as_array().cloned().unwrap_or_default();
            if items.is_empty() {
                println!("no agents");
            }
            for agent in items {
                let presence = if agent["online"].as_bool() == Some(true) {
                    "●"
                } else {
                    "○"
                };
                let name = agent["name"].as_str().unwrap_or("?");
                // An agent token sees counts only for its own inbox.
                if agent.get("pending").is_none() {
                    println!("{presence} {name}");
                    continue;
                }
                println!(
                    "{presence} {name:<32} {:>4} pending  {:>3} leased  {:>3} dead",
                    agent["pending"], agent["leased"], agent["dead"],
                );
            }
            Ok(())
        }
        Some("cleanup") => {
            let result = api(client()?.post("/api/cleanup", &json!({})))?;
            let removed: Vec<&str> = result["removed"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            if removed.is_empty() {
                println!("no stale agents");
            } else {
                println!("removed {}", removed.join(", "));
            }
            Ok(())
        }
        Some("token") => {
            println!("{}", secret_hex(32).map_err(|e| e.to_string())?);
            Ok(())
        }
        Some("repair") => {
            let flags = Flags::parse(rest, &[]);
            flags.check(&["data-dir"])?;
            let dir = flags
                .value("data-dir")?
                .map_or_else(default_data_dir, std::path::PathBuf::from);
            match postino::journal::repair(&dir)? {
                Some(report) => println!("{report}"),
                None => println!("{}: the journal is whole; nothing to repair", dir.display()),
            }
            Ok(())
        }
        Some("status") => {
            let client = client()?;
            let health = api(client.get("/api/health"))?;
            println!("server      {}", client.url());
            println!("version     {}", health["version"].as_str().unwrap_or("?"));
            match client.get("/api/stats") {
                Ok(stats) => println!(
                    "keys        {}\nagents      {} ({} online)\ninboxes     {} pending, {} leased, {} dead\nbroadcasts  {}\nrooms       {}",
                    stats["keyCount"],
                    stats["agentCount"],
                    stats["onlineCount"],
                    stats["messageCount"],
                    stats["leasedCount"],
                    stats["deadCount"],
                    stats["broadcastCount"],
                    stats["roomCount"]
                ),
                Err(e) => println!("stats       unavailable ({e})"),
            }
            Ok(())
        }
        Some("db") => db(rest),
        Some("publish") => {
            let channel = rest
                .first()
                .ok_or("usage: postino publish <channel> <text|->")?;
            let message = text(rest.get(1))?;
            let result = api(client()?.post("/api/db", &json!(["PUBLISH", channel, message])))?;
            println!("{}", result["result"]);
            Ok(())
        }
        Some("subscribe") => subscribe(&Flags::parse(rest, &[])),
        Some("tail") => tail(&Flags::parse(rest, &[])),
        Some("rooms") => rooms(),
        Some("room") => room(rest),
        Some("watch") => watch(&Flags::parse(rest, &[])),
        Some("config") => mcp_config(&Flags::parse(rest, &[])),
        Some("service") => service(rest.first()),
        Some("version" | "--version" | "-V") => {
            println!("postino {VERSION}");
            Ok(())
        }
        Some("help" | "--help" | "-h") => {
            println!("{HELP}");
            Ok(())
        }
        Some(other) => Err(format!("unknown command {other:?}\n\n{HELP}")),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("postino: {message}");
            ExitCode::FAILURE
        }
    }
}
