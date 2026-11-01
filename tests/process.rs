//! End-to-end tests against the real binary.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use postino::client::Client;
use serde_json::{Value, json};

const BIN: &str = env!("CARGO_BIN_EXE_postino");

fn serve(dir: &Path) -> (Server, Client) {
    let mut command = Command::new(BIN);
    command.args(["serve", "--data-dir"]).arg(dir);
    listen(command)
}

/// A broker that is killed when the test ends, failed or not, so that it
/// does not outlive the test holding the test's output open.
struct Server(Child);

impl std::ops::Deref for Server {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for Server {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts a broker on a port the system picks, so no other process can take
/// it first, and connects to the address the broker reports.
fn listen(mut command: Command) -> (Server, Client) {
    let mut child = command
        .args(["--port", "0"])
        .env_remove("POSTINO_URL")
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = BufReader::new(child.stderr.take().unwrap());
    let (found, url) = mpsc::channel();
    // Reads to the end, so the broker never blocks on a full pipe.
    thread::spawn(move || {
        for line in stderr.lines().map_while(Result::ok) {
            if let Some(api) = line.trim().strip_prefix("api") {
                let _ = found.send(api.trim().trim_end_matches("/api").to_string());
            }
        }
    });
    let url: String = url
        .recv_timeout(Duration::from_secs(10))
        .expect("the broker reports where it listens");
    (Server(child), healthy(&url))
}

fn healthy(url: &str) -> Client {
    let client = Client::new(url, None).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while client.health().is_none() {
        assert!(Instant::now() < deadline, "broker at {url} did not come up");
        thread::sleep(Duration::from_millis(50));
    }
    client
}

#[test]
fn messages_and_keys_survive_a_killed_server() {
    let dir = common::temp_dir("crash");
    let (mut broker, api) = serve(&dir);
    for i in 0..20 {
        api.post(
            "/api/messages",
            &json!({ "from": "load", "to": "durable", "body": format!("job {i}") }),
        )
        .unwrap();
    }
    let leased = api
        .post("/api/messages/durable/read", &json!({ "limit": 5 }))
        .unwrap();
    let receipts: Vec<&Value> = leased["items"]
        .as_array()
        .unwrap()
        .iter()
        .take(2)
        .map(|m| &m["receipt"])
        .collect();
    api.post(
        "/api/messages/durable/ack",
        &json!({ "receipts": receipts }),
    )
    .unwrap();

    let db =
        |api: &Client, command: Value| api.post("/api/db", &command).unwrap()["result"].clone();
    db(&api, json!(["SET", "plan", "v2", "EX", 3600]));
    db(&api, json!(["RPUSH", "queue", "a", "b", "c"]));
    db(&api, json!(["SPOP", "missing"]));
    db(&api, json!(["XADD", "events", "*", "kind", "start"]));
    db(&api, json!(["XGROUP", "CREATE", "events", "g", "0"]));
    db(
        &api,
        json!(["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "events", ">"]),
    );
    api.post(
        "/api/db/multi",
        &json!([["INCR", "n"], ["INCR", "n"], ["LPOP", "queue"]]),
    )
    .unwrap();
    broker.kill().unwrap();
    broker.wait().unwrap();

    let (mut broker, api) = serve(&dir);
    assert_eq!(db(&api, json!(["GET", "plan"])), "v2");
    assert!(db(&api, json!(["TTL", "plan"])).as_i64().unwrap() > 3500);
    assert_eq!(
        db(&api, json!(["LRANGE", "queue", 0, -1])),
        json!(["b", "c"])
    );
    assert_eq!(db(&api, json!(["GET", "n"])), "2");
    assert_eq!(db(&api, json!(["XPENDING", "events", "g"]))[0], 1);
    let check = api.get("/api/check/durable").unwrap();
    assert_eq!(check["unreadMessages"], 18);
    assert_eq!(check["leased"], 3);
    let history = api.get("/api/history?agent=durable&limit=200").unwrap();
    let acked = history["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["state"] == "acked")
        .count();
    assert_eq!(acked, 2);
    broker.kill().unwrap();
    let _ = broker.wait();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn every_change_answered_survives_a_server_killed_under_load() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    let dir = common::temp_dir("crash-load");
    let mut command = Command::new(BIN);
    command
        .args(["serve", "--fsync", "never", "--data-dir"])
        .arg(&dir);
    let (mut broker, api) = listen(command);
    // Each client counts up a key of its own: the last count it was told is
    // the least the key may hold after the crash, and the one it was sending
    // when the server died the most. Without syncs nothing holds a write
    // back, and the journal stays too small for a snapshot to start one.
    let answered = Arc::new(AtomicU64::new(0));
    let clients: Vec<_> = (0..8)
        .map(|i| {
            let (api, answered) = (api.clone(), Arc::clone(&answered));
            thread::spawn(move || {
                let mut told = 0;
                for _ in 0..1500 {
                    match api.post("/api/db", &json!(["INCR", format!("count:{i}")])) {
                        Ok(reply) => told = reply["result"].as_i64().unwrap(),
                        Err(_) => break,
                    }
                    answered.fetch_add(1, Ordering::Relaxed);
                }
                told
            })
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(60);
    while answered.load(Ordering::Relaxed) < 4000 {
        assert!(Instant::now() < deadline, "the clients got too few answers");
        thread::sleep(Duration::from_millis(1));
    }
    broker.kill().unwrap();
    broker.wait().unwrap();
    let told: Vec<i64> = clients.into_iter().map(|c| c.join().unwrap()).collect();

    let (mut broker, api) = serve(&dir);
    for (i, told) in told.iter().enumerate() {
        let held = api
            .post("/api/db", &json!(["GET", format!("count:{i}")]))
            .unwrap()["result"]
            .as_str()
            .map_or(0, |n| n.parse::<i64>().unwrap());
        assert!(
            (*told..=told + 1).contains(&held),
            "count:{i} was told {told} and holds {held}"
        );
    }
    broker.kill().unwrap();
    let _ = broker.wait();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_second_broker_cannot_share_a_data_dir() {
    let dir = common::temp_dir("exclusive");
    let (mut first, _) = serve(&dir);
    let second = Command::new(BIN)
        .args(["serve", "--port", "0", "--data-dir"])
        .arg(&dir)
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("already using"));
    first.kill().unwrap();
    let _ = first.wait();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_damaged_journal_is_cut_by_repair_and_the_server_starts_from_before_it() {
    let dir = common::temp_dir("repair");
    let (mut broker, api) = serve(&dir);
    for i in 0..20 {
        api.post("/api/db", &json!(["SET", format!("k{i}"), "v"]))
            .unwrap();
    }
    broker.kill().unwrap();
    broker.wait().unwrap();
    let journal = dir.join("journal.log");
    let text = std::fs::read_to_string(&journal).unwrap();
    let mut lines: Vec<&str> = text.lines().collect();
    let damaged = lines.len() - 5;
    lines[damaged] = "not a journal line";
    std::fs::write(&journal, lines.join("\n") + "\n").unwrap();

    let refused = Command::new(BIN)
        .args(["serve", "--port", "0", "--data-dir"])
        .arg(&dir)
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("postino repair"));
    let repaired = Command::new(BIN)
        .args(["repair", "--data-dir"])
        .arg(&dir)
        .output()
        .unwrap();
    assert!(repaired.status.success());
    assert!(String::from_utf8_lossy(&repaired.stdout).contains("journal.log.corrupt"));

    let (_broker, api) = serve(&dir);
    let held = |key: &str| api.post("/api/db", &json!(["GET", key])).unwrap()["result"].clone();
    assert_eq!(held("k0"), "v");
    assert_eq!(held("k14"), "v", "every change before the damaged line");
    assert_eq!(held("k15"), Value::Null);
    assert_eq!(held("k19"), Value::Null);
    let _ = std::fs::remove_dir_all(dir);
}

struct Mcp {
    child: Child,
    input: std::process::ChildStdin,
    output: BufReader<std::process::ChildStdout>,
}

impl Mcp {
    fn start(url: &str, agent: &str) -> Mcp {
        let mut child = Command::new(BIN)
            .args(["mcp", "--agent", agent])
            .env("POSTINO_URL", url)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Mcp {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
        }
    }

    fn send(&mut self, message: Value) {
        writeln!(self.input, "{message}").unwrap();
    }

    fn rpc(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        loop {
            let mut line = String::new();
            assert!(
                self.output.read_line(&mut line).unwrap() > 0,
                "adapter closed stdout"
            );
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == id {
                return value;
            }
        }
    }

    fn tool(&mut self, id: u64, name: &str, arguments: Value) -> (String, bool) {
        let response = self.rpc(
            id,
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        );
        let result = &response["result"];
        (
            result["content"][0]["text"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            result["isError"].as_bool().unwrap_or(false),
        )
    }
}

#[test]
fn mcp_agents_share_a_room() {
    let broker = common::start(|_| {});
    let api = common::client(&broker);
    let mut planner = Mcp::start(&broker.url, "planner");
    let mut builder = Mcp::start(&broker.url, "builder");

    let (text, error) = planner.tool(
        1,
        "room_join",
        json!({ "room": "release", "topic": "ship 4.2" }),
    );
    assert!(!error, "{text}");
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["created"],
        true
    );
    let (text, _) = builder.tool(1, "room_join", json!({ "room": "release" }));
    let joined: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(joined["created"], false);
    assert_eq!(joined["topic"], "ship 4.2");

    let (text, error) = planner.tool(
        2,
        "room_post",
        json!({ "room": "release", "body": "build the CLI first", "about": "task-1" }),
    );
    assert!(!error && text.starts_with("Posted"), "{text}");
    let (text, _) = builder.tool(2, "msg_check", json!({}));
    assert!(text.contains("room release: 2 unread posts"), "{text}");
    let (text, _) = builder.tool(3, "room_read", json!({ "room": "release" }));
    let read: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(read["posts"][1]["body"], "build the CLI first");
    assert_eq!(read["posts"][1]["about"], "task-1");

    let (text, _) = planner.tool(
        3,
        "room_board",
        json!({ "room": "release", "key": "owner/task-1", "value": "builder", "version": 0 }),
    );
    assert_eq!(text, "Set owner/task-1 (version 1)");
    let (text, error) = builder.tool(
        4,
        "room_board",
        json!({ "room": "release", "key": "owner/task-1", "value": "me", "version": 0 }),
    );
    assert!(error && text.contains("version 1"), "{text}");

    let (text, error) = builder.tool(
        5,
        "room_ask",
        json!({ "room": "release", "body": "publish to crates.io?", "to": ["operator"], "options": ["yes", "no"] }),
    );
    assert!(!error, "{text}");
    let id = text.split_whitespace().nth(2).unwrap().to_string();
    let operator = common::client(&broker);
    let path = format!("/api/rooms/release/questions/{id}/answers");
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        operator.post(&path, &json!({ "choice": "no" })).unwrap();
    });
    let (text, error) = builder.tool(
        6,
        "room_ask",
        json!({ "room": "release", "question": id, "wait_seconds": 10 }),
    );
    assert!(!error, "{text}");
    let settled: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(settled["question"]["question"]["status"], "resolved");
    assert_eq!(
        settled["question"]["question"]["answers"][0]["choice"],
        "no"
    );

    let rooms = api.get("/api/rooms").unwrap();
    assert_eq!(
        rooms["items"][0]["memberCount"], 3,
        "the operator was brought in"
    );
}

#[test]
fn mcp_adapter_round_trip() {
    let broker = common::start(|_| {});
    let api = common::client(&broker);
    api.post("/api/agents/peer/presence", &json!({})).unwrap();
    let mut mcp = Mcp::start(&broker.url, "tester");

    let init = mcp.rpc(1, "initialize", json!({ "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "test", "version": "1" } }));
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(init["result"]["serverInfo"]["name"], "postino");
    mcp.send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));

    let tools = mcp.rpc(2, "tools/list", json!({}));
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"msg_send") && names.contains(&"msg_read") && names.contains(&"msg_ack")
    );

    let (text, error) = mcp.tool(
        3,
        "msg_send",
        json!({ "to": "peer", "body": "hello from mcp" }),
    );
    assert!(!error && text.starts_with("Sent"), "{text}");
    let (text, error) = mcp.tool(4, "msg_send", json!({ "to": "pere", "body": "typo" }));
    assert!(error && text.contains("known agents"), "{text}");

    let peer = api.post("/api/messages/peer/read", &json!({})).unwrap();
    let question = &peer["items"][0];
    assert_eq!(question["from"], "tester");
    api.post(
        "/api/messages",
        &json!({ "from": "peer", "replyTo": question["id"], "body": "got it" }),
    )
    .unwrap();

    let (text, error) = mcp.tool(5, "msg_read", json!({ "wait_seconds": 2 }));
    assert!(!error, "{text}");
    let leased: Value = serde_json::from_str(&text).unwrap();
    let message = &leased["messages"][0];
    assert_eq!(message["body"], "got it");
    assert_eq!(message["thread"], question["id"]);

    let (text, error) = mcp.tool(6, "msg_ack", json!({ "receipts": [message["receipt"]] }));
    assert!(!error && text == "Acknowledged 1 message", "{text}");
    let (text, _) = mcp.tool(7, "msg_check", json!({}));
    assert_eq!(text, "No new messages, broadcasts, or room posts");
    let (text, _) = mcp.tool(8, "msg_history", json!({ "with": "peer" }));
    assert_eq!(
        serde_json::from_str::<Value>(&text)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );

    mcp.tool(9, "msg_broadcast", json!({ "body": "freeze at 5pm" }));
    let (text, _) = mcp.tool(10, "msg_broadcasts", json!({}));
    assert_eq!(
        text, "No new broadcasts (1 broadcast retained, all seen)",
        "own broadcasts are not unseen"
    );

    let (text, error) = mcp.tool(11, "msg_rename", json!({ "name": "reviewer" }));
    assert!(!error, "{text}");
    let (text, _) = mcp.tool(12, "msg_whoami", json!({}));
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["me"],
        "reviewer"
    );

    assert_eq!(
        mcp.rpc(13, "tools/call", json!({ "name": "nope" }))["error"]["code"],
        -32602
    );
    assert_eq!(
        mcp.rpc(14, "resources/list", json!({}))["error"]["code"],
        -32601
    );
    assert_eq!(mcp.rpc(15, "ping", json!({}))["result"], json!({}));

    drop(mcp.input);
    let status = mcp.child.wait().unwrap();
    assert!(status.success());
    let agents = api.get("/api/agents").unwrap();
    let me = agents["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "reviewer")
        .unwrap();
    assert_eq!(me["online"], false, "closing stdin ends the session");
}

#[cfg(unix)]
#[test]
fn clients_start_a_local_broker_on_demand() {
    let dir = common::temp_dir("autostart");
    let run = |args: &[&str]| {
        Command::new(BIN)
            .args(args)
            .env("POSTINO_DATA_DIR", &dir)
            .env("POSTINO_PORT", "0")
            .env_remove("POSTINO_URL")
            .env_remove("POSTINO_AGENT_NAME")
            .output()
            .unwrap()
    };
    let sent = run(&["send", "worker", "hello"]);
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    let status = run(&["status"]);
    let text = String::from_utf8_lossy(&status.stdout);
    assert!(text.contains("1 pending"), "{text}");
    let read = run(&["read", "worker", "--ack"]);
    let items: Value = serde_json::from_slice(&read.stdout).unwrap();
    assert_eq!(items[0]["body"], "hello");
    assert_eq!(items[0]["from"], "operator");

    let info: Value =
        serde_json::from_slice(&std::fs::read(dir.join("broker.json")).unwrap()).unwrap();
    // A broker nobody started by hand protects itself with a token that only
    // this user can read; the clients find it next to broker.json.
    assert_eq!(info["localToken"], true);
    let anonymous = Client::new(info["url"].as_str().unwrap(), None).unwrap();
    assert!(matches!(
        anonymous.get("/api/agents"),
        Err(postino::client::Error::Api { status: 401, .. })
    ));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |name: &str| {
            std::fs::metadata(dir.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode(""), 0o700, "the data directory is private");
        for file in ["token", "broker.json", "journal.log", "broker.log"] {
            assert_eq!(mode(file), 0o600, "{file} is private");
        }
    }
    Command::new("kill")
        .arg(info["pid"].to_string())
        .status()
        .unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn writes_stop_at_the_memory_limit_and_deletes_make_room() {
    let mut command = Command::new(BIN);
    command
        .args(["serve", "--memory"])
        .env("POSTINO_MAX_MEMORY", "16mb")
        .env("POSTINO_MAX_BODY_BYTES", "2097152");
    let (mut child, api) = listen(command);
    let chunk = "x".repeat(1 << 20);
    let mut stored = 0;
    let refused = loop {
        assert!(stored < 64, "the limit never applied");
        match api.post("/api/db", &json!(["SET", format!("blob:{stored}"), chunk])) {
            Ok(_) => stored += 1,
            Err(e) => break e,
        }
    };
    match refused {
        postino::client::Error::Api {
            status, message, ..
        } => {
            assert_eq!(status, 507, "{message}");
            assert!(message.starts_with("OOM"), "{message}");
        }
        other => panic!("{other}"),
    }
    assert!(stored >= 4, "only {stored} MiB fit under a 16 MiB limit");
    assert_eq!(
        api.post("/api/db", &json!(["GET", "blob:0"])).unwrap()["result"]
            .as_str()
            .map(str::len),
        Some(1 << 20),
        "reads still work"
    );
    let keys: Vec<String> = (0..stored).map(|i| format!("blob:{i}")).collect();
    let mut del = vec!["DEL".to_string()];
    del.extend(keys);
    api.post("/api/db", &json!(del)).unwrap();
    api.post("/api/db", &json!(["SET", "small", "fits again"]))
        .unwrap();
    let message = api.post(
        "/api/messages",
        &json!({ "to": "someone", "body": "the forum works again too" }),
    );
    assert!(
        message.is_ok(),
        "{:?}",
        message.err().map(|e| e.to_string())
    );
    child.kill().unwrap();
    let _ = child.wait();
}
