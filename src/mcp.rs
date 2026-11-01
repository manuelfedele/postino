//! Model Context Protocol adapter: JSON-RPC 2.0 over stdio, one message per line.
//! Each tool is a thin call to the broker's HTTP API under this agent's identity:
//! every request names the agent in `X-Postino-Agent`, so even when the adapter
//! holds the local administrator token the server grants it one agent's rights.

use std::io::{self, BufRead, Stdout, Write};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{Value, json};

use crate::client::{self, Client};
use crate::config::ClientConfig;
use crate::http::percent_encode;
use crate::util::{VERSION, hostname, random_hex, valid_name};

const PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const HEARTBEAT: Duration = Duration::from_secs(10);
const PRESENCE_TTL_SECS: u64 = 30;
const TIMEOUT: Duration = Duration::from_secs(30);

struct Link {
    cfg: ClientConfig,
    client: Mutex<Option<Client>>,
    name: Mutex<String>,
    session: String,
    consumer: String,
    /// 0 before the first heartbeat, then 1 connected / 2 unavailable.
    connected: AtomicU8,
}

impl Link {
    fn name(&self) -> String {
        self.name.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn client(&self) -> Result<Client, String> {
        let mut slot = self.client.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(client) = slot.as_ref() {
            return Ok(client.clone());
        }
        let client = client::connect(&self.cfg)?;
        *slot = Some(client.clone());
        Ok(client)
    }

    /// Calls the broker, looking it up again (and if needed restarting it) once when it
    /// cannot be reached. Requests that may have been delivered are not retried.
    fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        timeout: Duration,
    ) -> Result<Value, client::Error> {
        let mut retried = false;
        loop {
            let client = self
                .client()
                .map_err(client::Error::Unreachable)?
                .as_agent(&self.name());
            match client.request(method, path, body.as_ref(), timeout) {
                Err(client::Error::Unreachable(_)) if !retried => {
                    *self.client.lock().unwrap_or_else(|p| p.into_inner()) = None;
                    retried = true;
                }
                result => return result,
            }
        }
    }

    fn heartbeat(&self) {
        let path = format!("/api/agents/{}/presence", percent_encode(&self.name()));
        let body = json!({
            "session": self.session,
            "client": "mcp",
            "pid": std::process::id(),
            "host": hostname(),
            "ttl": PRESENCE_TTL_SECS,
        });
        let result = self.call("POST", &path, Some(body), Duration::from_secs(5));
        let state = if result.is_ok() { 1 } else { 2 };
        if self.connected.swap(state, Ordering::Relaxed) != state {
            match result {
                Ok(_) => eprintln!("postino mcp: connected as {}", self.name()),
                Err(e) => eprintln!("postino mcp: broker unavailable: {e}"),
            }
        }
    }

    fn leave(&self) {
        let path = format!(
            "/api/agents/{}/presence/{}",
            percent_encode(&self.name()),
            percent_encode(&self.session)
        );
        let _ = self.call("DELETE", &path, None, Duration::from_secs(2));
    }
}

fn tool(
    name: &str,
    title: &str,
    description: &str,
    properties: Value,
    required: &[&str],
    read_only: bool,
) -> Value {
    json!({
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false,
        },
        "annotations": { "readOnlyHint": read_only },
    })
}

fn tools() -> Value {
    let string = |description: &str| json!({ "type": "string", "description": description });
    let integer = |description: &str, min: u64, max: u64| json!({ "type": "integer", "minimum": min, "maximum": max, "description": description });
    let names = |description: &str| json!({ "type": "array", "items": { "type": "string" }, "maxItems": 32, "description": description });
    let room = string("Room name.");
    json!([
        tool(
            "msg_whoami",
            "Status overview",
            "Your agent name, unread messages, unseen broadcasts, and every known agent with presence. Call this first in a session.",
            json!({}),
            &[],
            true,
        ),
        tool(
            "msg_check",
            "Check for activity",
            "Count unread messages and unseen broadcasts without consuming anything.",
            json!({}),
            &[],
            true,
        ),
        tool(
            "msg_list_agents",
            "List agents",
            "Every known agent, whether it is online, and how many messages wait in your own inbox. Use it to find recipients.",
            json!({}),
            &[],
            true,
        ),
        tool(
            "msg_send",
            "Send a message",
            "Send a direct message to one agent. To answer a message pass reply_to with its id: the recipient defaults to the original sender and the conversation thread is kept.",
            json!({
                "to": string("Recipient agent name. Optional when reply_to is set."),
                "body": string("Message text (up to 32768 characters)."),
                "reply_to": string("Id of the message you are answering."),
                "key": string("Idempotency key: resending with the same key does not create a duplicate."),
            }),
            &["body"],
            false,
        ),
        tool(
            "msg_read",
            "Read messages",
            "Lease the next messages from your inbox. Each comes with a receipt: call msg_ack with it once you have acted on the message, otherwise it is delivered again when the lease expires. Set wait_seconds to block until a message arrives, for example while waiting for a reply.",
            json!({
                "limit": integer("Maximum messages to lease (default 10).", 1, 100),
                "wait_seconds": integer("Seconds to wait when the inbox is empty (default 0).", 0, 60),
            }),
            &[],
            false,
        ),
        tool(
            "msg_ack",
            "Acknowledge messages",
            "Confirm leased messages are handled so they are not delivered again.",
            json!({
                "receipts": {
                    "type": "array",
                    "items": { "type": "string" },
                    "minItems": 1,
                    "maxItems": 100,
                    "description": "Receipts returned by msg_read.",
                },
            }),
            &["receipts"],
            false,
        ),
        tool(
            "msg_history",
            "Conversation history",
            "Recent messages you sent or received, including ones already acknowledged. Filter by another agent or by thread id to recover the context of a conversation.",
            json!({
                "with": string("Only messages exchanged with this agent."),
                "thread": string("Only messages in this thread."),
                "limit": integer("Maximum messages (default 20).", 1, 200),
            }),
            &[],
            true,
        ),
        tool(
            "msg_broadcast",
            "Broadcast",
            "Announce something to every agent: deploy freezes, finished migrations, shared decisions. Broadcasts are not consumed; each agent tracks what it has seen.",
            json!({
                "body": string("Announcement text (up to 32768 characters)."),
                "key": string("Idempotency key."),
            }),
            &["body"],
            false,
        ),
        tool(
            "msg_broadcasts",
            "Read broadcasts",
            "Broadcasts you have not seen yet, marking them as seen. Set all to list every retained broadcast.",
            json!({ "all": { "type": "boolean", "description": "Include broadcasts already seen." } }),
            &[],
            false,
        ),
        tool(
            "msg_rename",
            "Rename",
            "Change your agent name to something meaningful (for example 'reviewer'). Your inbox and history move with you.",
            json!({ "name": string("New agent name: letters, digits, '-', '_' or '.'.") }),
            &["name"],
            false,
        ),
        tool(
            "db",
            "Shared database",
            "Run a command against the shared in-memory database. \
             Strings and counters: SET key value [EX seconds] [NX] [IFEQ old], GET, INCR, DEL; SET lock token NX EX 30 takes a lock and DELIFEQ lock token releases it. \
             Hashes: HSET key field value.., HGETALL. Lists as queues: RPUSH, LPOP, BLPOP key timeout. Sets: SADD, SISMEMBER, SMEMBERS. \
             Sorted sets as priority queues: ZADD key score member, ZPOPMIN, BZPOPMIN. \
             Streams for event logs and work sharing: XADD key * field value, XRANGE, XREAD BLOCK ms STREAMS key $, \
             XGROUP CREATE key group $ MKSTREAM, XREADGROUP GROUP group me COUNT n BLOCK ms STREAMS key >, XACK, XAUTOCLAIM. \
             Channels: PUBLISH channel message. Keys: EXPIRE, TTL, KEYS pattern, SCAN, TYPE. Blocking commands wait up to 60 seconds. \
             Pass commands instead of command to run several atomically. COMMAND lists everything. \
             A server with agent tokens may limit the keys and channels you reach (by default <your-name>:* and shared:*); a NOPERM error says what yours reaches.",
            json!({
                "command": { "type": "array", "items": { "type": ["string", "number"] }, "minItems": 1, "description": "The command and its arguments, for example [\"HSET\", \"task:42\", \"state\", \"done\"]." },
                "commands": { "type": "array", "items": { "type": "array", "items": { "type": ["string", "number"] } }, "maxItems": 100, "description": "Several commands to run atomically, in order." },
            }),
            &[],
            false,
        ),
        tool(
            "room_join",
            "Join a room",
            "Join the room for a workflow, creating it if it does not exist (you become its owner). Every member reads every post in a room. Returns the topic, members, board, and open questions.",
            json!({
                "room": room,
                "topic": string("What the room is for, used when creating it."),
                "private": { "type": "boolean", "description": "When creating: only members you add can join." },
                "members": names("When creating: agents to add right away."),
            }),
            &["room"],
            false,
        ),
        tool(
            "room_leave",
            "Leave a room",
            "Stop following a room.",
            json!({ "room": room }),
            &["room"],
            false,
        ),
        tool(
            "room_read",
            "Read a room",
            "Posts in a room you have not read yet, oldest first, marking them read. Set wait_seconds to block until someone posts. Set about to get every post about one item instead, read or not.",
            json!({
                "room": room,
                "limit": integer("Maximum posts (default 50).", 1, 200),
                "wait_seconds": integer("Seconds to wait when nothing is unread (default 0).", 0, 60),
                "about": string("Only posts about this item, from the retained history."),
            }),
            &["room"],
            false,
        ),
        tool(
            "room_post",
            "Post to a room",
            "Write to every member of a room. Tag the post with about (a task, file, or ticket) so whoever works on that item later finds it.",
            json!({
                "room": room,
                "body": string("Post text (up to 32768 characters)."),
                "reply_to": string("Id of the post you are answering."),
                "about": string("The item this post concerns."),
                "key": string("Idempotency key."),
            }),
            &["room", "body"],
            false,
        ),
        tool(
            "room_ask",
            "Ask a question",
            "Ask members of a room something that blocks your work, for example a decision for the operator or approvals from several reviewers. The question resolves when everyone in to has answered (or on the first answer when to is empty). Set wait_seconds to wait for the answers; to keep waiting on a question you asked earlier, pass question instead of body.",
            json!({
                "room": room,
                "body": string("The question."),
                "to": names("Who has to answer; they are added to the room. Use 'operator' for the human."),
                "options": names("Allowed answers, for example [\"approve\", \"reject\"]."),
                "about": string("The item the question concerns."),
                "expires_in_seconds": integer("Seconds before the question expires (default: the message TTL).", 10, 2_592_000),
                "wait_seconds": integer("Seconds to wait for the answers (default 0).", 0, 60),
                "question": string("Id of a question you already asked, to check or keep waiting on it."),
            }),
            &["room"],
            false,
        ),
        tool(
            "room_answer",
            "Answer a question",
            "Answer an open question in a room. When the question lists options, pass one as choice.",
            json!({
                "room": room,
                "question": string("Id of the question."),
                "body": string("Your answer. Optional when choice says it all."),
                "choice": string("One of the question's options."),
            }),
            &["room", "question"],
            false,
        ),
        tool(
            "room_board",
            "Room board",
            "The shared state of a room: the plan, decisions, owners of each task. Without key, lists the board. With key and value, sets it; pass version (from a previous read, 0 for a new key) to fail instead of overwriting a concurrent change. Set delete to remove a key.",
            json!({
                "room": room,
                "key": string("Board key."),
                "value": string("New value."),
                "version": { "type": "integer", "minimum": 0, "description": "Expected current version." },
                "delete": { "type": "boolean", "description": "Remove the key." },
            }),
            &["room"],
            false,
        ),
    ])
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_default()
}

fn text_arg(args: &Value, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("{key} must be a string")),
    }
}

fn int_arg(args: &Value, key: &str, min: u64, max: u64) -> Result<Option<u64>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .or_else(|| {
                value
                    .as_f64()
                    .filter(|f| f.fract() == 0.0 && *f >= 0.0)
                    .map(|f| f as u64)
            })
            .filter(|n| (min..=max).contains(n))
            .map(Some)
            .ok_or_else(|| format!("{key} must be an integer between {min} and {max}")),
    }
}

fn compact(message: &Value, fields: &[&str]) -> Value {
    let mut out = serde_json::Map::new();
    for field in fields {
        if let Some(value) = message.get(*field) {
            out.insert((*field).to_string(), value.clone());
        }
    }
    Value::Object(out)
}

fn plural(count: u64, word: &str) -> String {
    format!("{count} {word}{}", if count == 1 { "" } else { "s" })
}

fn call_tool(link: &Link, tool: &str, args: &Value) -> Result<String, String> {
    let me = link.name();
    let me_path = percent_encode(&me);
    let call = |method: &str, path: &str, body: Option<Value>| {
        link.call(method, path, body, TIMEOUT)
            .map_err(|e| e.to_string())
    };
    match tool {
        "msg_whoami" => {
            let check = call("GET", &format!("/api/check/{me_path}"), None)?;
            let agents = call("GET", "/api/agents", None)?;
            let agents: Vec<Value> = agents["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|a| {
                    let mut agent = compact(a, &["name", "online", "pending"]);
                    agent["isMe"] = json!(a["name"] == me.as_str());
                    agent
                })
                .collect();
            let rooms = call("GET", &format!("/api/rooms?agent={me_path}"), None)?;
            let rooms: Vec<Value> = rooms["items"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|r| r["open"] == true)
                .map(|r| {
                    compact(
                        r,
                        &[
                            "name",
                            "topic",
                            "member",
                            "unread",
                            "waiting",
                            "openQuestions",
                            "memberCount",
                        ],
                    )
                })
                .collect();
            Ok(pretty(&json!({
                "me": me,
                "unreadMessages": check["unreadMessages"],
                "leased": check["leased"],
                "unseenBroadcasts": check["unseenBroadcasts"],
                "rooms": rooms,
                "agents": agents,
                "broker": link.client()?.url(),
            })))
        }
        "msg_check" => {
            let check = call("GET", &format!("/api/check/{me_path}"), None)?;
            let unread = check["unreadMessages"].as_u64().unwrap_or(0);
            let unseen = check["unseenBroadcasts"].as_u64().unwrap_or(0);
            let mut lines = Vec::new();
            if unread > 0 {
                lines.push(format!(
                    "{} (use msg_read)",
                    plural(unread, "unread message")
                ));
            }
            if unseen > 0 {
                lines.push(format!(
                    "{} (use msg_broadcasts)",
                    plural(unseen, "unseen broadcast")
                ));
            }
            for room in check["rooms"].as_array().into_iter().flatten() {
                let name = room["room"].as_str().unwrap_or("?");
                let posts = room["unread"].as_u64().unwrap_or(0);
                let waiting = room["waiting"].as_u64().unwrap_or(0);
                let mut parts = Vec::new();
                if posts > 0 {
                    parts.push(plural(posts, "unread post"));
                }
                if waiting > 0 {
                    parts.push(format!("{} for you", plural(waiting, "question")));
                }
                lines.push(format!("room {name}: {} (use room_read)", parts.join(", ")));
            }
            Ok(if lines.is_empty() {
                "No new messages, broadcasts, or room posts".to_string()
            } else {
                lines.join("\n")
            })
        }
        "msg_list_agents" => {
            let agents = call("GET", "/api/agents", None)?;
            let items: Vec<Value> = agents["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|a| {
                    let mut agent = compact(a, &["name", "online", "pending", "lastSeen"]);
                    agent["isMe"] = json!(a["name"] == me.as_str());
                    agent
                })
                .collect();
            Ok(if items.is_empty() {
                "No agents are registered yet".to_string()
            } else {
                pretty(&Value::Array(items))
            })
        }
        "msg_send" => {
            let body = text_arg(args, "body")?.ok_or("body is required")?;
            let to = text_arg(args, "to")?;
            let reply_to = text_arg(args, "reply_to")?;
            if to.is_none() && reply_to.is_none() {
                return Err("pass \"to\", or \"reply_to\" to answer a message".into());
            }
            let key = text_arg(args, "key")?.unwrap_or_else(|| random_hex(12));
            let result = call(
                "POST",
                "/api/messages",
                Some(
                    json!({ "from": me, "to": to, "body": body, "replyTo": reply_to, "key": key, "strict": true }),
                ),
            )?;
            Ok(format!(
                "{} {} to {} (thread {})",
                if result["created"].as_bool() == Some(false) {
                    "Already sent"
                } else {
                    "Sent"
                },
                result["id"].as_str().unwrap_or("?"),
                result["to"].as_str().unwrap_or("?"),
                result["thread"].as_str().unwrap_or("?"),
            ))
        }
        "msg_read" => {
            let limit = int_arg(args, "limit", 1, 100)?.unwrap_or(10);
            let wait = int_arg(args, "wait_seconds", 0, 60)?.unwrap_or(0);
            let result = link
                .call(
                    "POST",
                    &format!("/api/messages/{me_path}/read"),
                    Some(json!({ "limit": limit, "wait": wait, "consumer": link.consumer })),
                    Duration::from_secs(wait + 15),
                )
                .map_err(|e| e.to_string())?;
            let messages: Vec<Value> = result["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|m| {
                    compact(
                        m,
                        &[
                            "receipt",
                            "id",
                            "from",
                            "body",
                            "timestamp",
                            "thread",
                            "replyTo",
                            "deliveries",
                        ],
                    )
                })
                .collect();
            if messages.is_empty() {
                return Ok(if wait > 0 {
                    format!("No messages for {me} after waiting {wait}s")
                } else {
                    format!("No messages for {me}")
                });
            }
            Ok(pretty(&json!({
                "leaseSeconds": result["leaseSeconds"],
                "messages": messages,
                "next": "After acting on these messages call msg_ack with their receipts. Reply with msg_send reply_to=<id>.",
            })))
        }
        "msg_ack" => {
            let receipts = args
                .get("receipts")
                .or_else(|| args.get("ids"))
                .and_then(Value::as_array)
                .filter(|list| !list.is_empty() && list.iter().all(Value::is_string))
                .ok_or("receipts must be a non-empty list of strings")?;
            match link.call(
                "POST",
                &format!("/api/messages/{me_path}/ack"),
                Some(json!({ "receipts": receipts })),
                TIMEOUT,
            ) {
                Ok(result) => Ok(format!(
                    "Acknowledged {}",
                    plural(
                        result["acknowledged"].as_array().map_or(0, Vec::len) as u64,
                        "message"
                    )
                )),
                Err(client::Error::Api {
                    status: 409, body, ..
                }) => Err(format!(
                    "Some receipts were rejected (lease expired and the message was redelivered, or the receipt is wrong): {}",
                    pretty(
                        &json!({ "acknowledged": body["acknowledged"], "rejected": body["rejected"] })
                    )
                )),
                Err(e) => Err(e.to_string()),
            }
        }
        "msg_history" => {
            let mut query = format!(
                "agent={me_path}&limit={}",
                int_arg(args, "limit", 1, 200)?.unwrap_or(20)
            );
            if let Some(peer) = text_arg(args, "with")? {
                query.push_str(&format!("&peer={}", percent_encode(&peer)));
            }
            if let Some(thread) = text_arg(args, "thread")? {
                query.push_str(&format!("&thread={}", percent_encode(&thread)));
            }
            let result = call("GET", &format!("/api/history?{query}"), None)?;
            let items: Vec<Value> = result["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|m| {
                    compact(
                        m,
                        &[
                            "id",
                            "from",
                            "to",
                            "body",
                            "timestamp",
                            "thread",
                            "replyTo",
                            "state",
                        ],
                    )
                })
                .collect();
            Ok(if items.is_empty() {
                "No messages in history".to_string()
            } else {
                pretty(&Value::Array(items))
            })
        }
        "msg_broadcast" => {
            let body = text_arg(args, "body")?.ok_or("body is required")?;
            let key = text_arg(args, "key")?.unwrap_or_else(|| random_hex(12));
            let result = call(
                "POST",
                "/api/broadcasts",
                Some(json!({ "from": me, "body": body, "key": key })),
            )?;
            Ok(format!(
                "Broadcast {} sent to all agents",
                result["id"].as_str().unwrap_or("?")
            ))
        }
        "msg_broadcasts" => {
            let all = args.get("all").and_then(Value::as_bool).unwrap_or(false);
            let result = call(
                "POST",
                &format!("/api/broadcasts/{me_path}/read"),
                Some(json!({ "all": all })),
            )?;
            let items: Vec<Value> = result["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|b| compact(b, &["id", "from", "body", "timestamp"]))
                .collect();
            let total = result["total"].as_u64().unwrap_or(0);
            Ok(match (items.is_empty(), total) {
                (true, 0) => "No broadcasts".to_string(),
                (true, total) => format!(
                    "No new broadcasts ({} retained, all seen)",
                    plural(total, "broadcast")
                ),
                (false, _) => pretty(&json!({ "total": total, "broadcasts": items })),
            })
        }
        "msg_rename" => {
            let name = text_arg(args, "name")?.ok_or("name is required")?;
            if !valid_name(&name) {
                return Err("names use 1-64 letters, digits, '-', '_' or '.', starting with a letter or digit".into());
            }
            call(
                "POST",
                &format!("/api/agents/{me_path}/rename"),
                Some(json!({ "to": name })),
            )?;
            *link.name.lock().unwrap_or_else(|p| p.into_inner()) = name.clone();
            Ok(format!("Renamed from {me} to {name}"))
        }
        "db" => {
            let (path, body) = match (args.get("command"), args.get("commands")) {
                (Some(command), None) => ("/api/db", command.clone()),
                (None, Some(commands)) => ("/api/db/multi", commands.clone()),
                _ => return Err("pass either command or commands".into()),
            };
            match link.call("POST", path, Some(body), Duration::from_secs(75)) {
                Ok(result) if path == "/api/db" => Ok(pretty(&result["result"])),
                Ok(results) => Ok(pretty(&results)),
                Err(client::Error::Api { message, body, .. }) => match body.get("result") {
                    Some(result) => Err(format!("{message}\nIt returned: {}", pretty(result))),
                    None => Err(message),
                },
                Err(e) => Err(e.to_string()),
            }
        }
        _ => room_tool(link, &me, tool, args),
    }
}

fn names_arg(args: &Value, key: &str) -> Result<Vec<String>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("{key} must be a list of strings"))
            })
            .collect(),
        Some(_) => Err(format!("{key} must be a list of strings")),
    }
}

/// The fields of a room post an agent needs, without delivery bookkeeping.
fn post_view(post: &Value) -> Value {
    let mut view = compact(
        post,
        &[
            "id",
            "from",
            "kind",
            "body",
            "timestamp",
            "replyTo",
            "about",
            "choice",
        ],
    );
    if let Some(question) = post.get("question") {
        view["question"] = compact(
            question,
            &[
                "status",
                "to",
                "options",
                "waitingOn",
                "answers",
                "expiresAt",
            ],
        );
    }
    view
}

fn posts_view(items: &Value) -> Vec<Value> {
    items
        .as_array()
        .into_iter()
        .flatten()
        .map(post_view)
        .collect()
}

fn room_tool(link: &Link, me: &str, tool: &str, args: &Value) -> Result<String, String> {
    let call = |method: &str, path: &str, body: Option<Value>| {
        link.call(method, path, body, TIMEOUT)
            .map_err(|e| e.to_string())
    };
    let room = text_arg(args, "room")?.ok_or("room is required")?;
    if !valid_name(&room) {
        return Err(
            "room names use 1-64 letters, digits, '-', '_' or '.', starting with a letter or digit"
                .into(),
        );
    }
    let base = format!("/api/rooms/{}", percent_encode(&room));
    let me_path = percent_encode(me);
    match tool {
        "room_join" => {
            let private = args
                .get("private")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let result = call(
                "POST",
                "/api/rooms",
                Some(json!({
                    "name": room,
                    "from": me,
                    "topic": text_arg(args, "topic")?,
                    "private": private,
                    "members": names_arg(args, "members")?,
                    "join": true,
                })),
            )?;
            let detail = &result["room"];
            let members: Vec<Value> = detail["members"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|m| compact(m, &["name", "role", "online"]))
                .collect();
            let questions: Vec<Value> = detail["questions"]
                .as_array()
                .into_iter()
                .flatten()
                .map(post_view)
                .collect();
            Ok(pretty(&json!({
                "room": room,
                "created": result["created"],
                "topic": detail["topic"],
                "members": members,
                "board": detail["board"],
                "openQuestions": questions,
                "unread": detail["unread"],
                "next": "Catch up with room_read, write with room_post, and use room_ask when you need a decision.",
            })))
        }
        "room_leave" => {
            call(
                "DELETE",
                &format!("{base}/members/{me_path}?from={me_path}"),
                None,
            )?;
            Ok(format!("Left room {room}"))
        }
        "room_read" => {
            let limit = int_arg(args, "limit", 1, 200)?.unwrap_or(50);
            if let Some(about) = text_arg(args, "about")? {
                let result = call(
                    "GET",
                    &format!(
                        "{base}/posts?agent={me_path}&about={}&limit={limit}",
                        percent_encode(&about)
                    ),
                    None,
                )?;
                let posts = posts_view(&result["items"]);
                return Ok(if posts.is_empty() {
                    format!("No posts about {about} in room {room}")
                } else {
                    pretty(&json!({ "about": about, "posts": posts }))
                });
            }
            let wait = int_arg(args, "wait_seconds", 0, 60)?.unwrap_or(0);
            let result = link
                .call(
                    "POST",
                    &format!("{base}/read"),
                    Some(json!({ "agent": me, "limit": limit, "wait": wait })),
                    Duration::from_secs(wait + 15),
                )
                .map_err(|e| e.to_string())?;
            let posts = posts_view(&result["items"]);
            if posts.is_empty() {
                return Ok(if wait > 0 {
                    format!("Nothing new in room {room} after waiting {wait}s")
                } else {
                    format!("Nothing new in room {room}")
                });
            }
            let mut out = json!({ "room": room, "posts": posts });
            if result["remaining"].as_u64().unwrap_or(0) > 0 {
                out["remaining"] = result["remaining"].clone();
            }
            Ok(pretty(&out))
        }
        "room_post" => {
            let body = text_arg(args, "body")?.ok_or("body is required")?;
            let key = text_arg(args, "key")?.unwrap_or_else(|| random_hex(12));
            let result = call(
                "POST",
                &format!("{base}/posts"),
                Some(json!({
                    "from": me,
                    "body": body,
                    "replyTo": text_arg(args, "reply_to")?,
                    "about": text_arg(args, "about")?,
                    "key": key,
                })),
            )?;
            Ok(format!(
                "Posted {} to room {room}",
                result["id"].as_str().unwrap_or("?")
            ))
        }
        "room_ask" => {
            let wait = int_arg(args, "wait_seconds", 0, 60)?.unwrap_or(0);
            let id = match text_arg(args, "question")? {
                Some(id) => id,
                None => {
                    let body = text_arg(args, "body")?
                        .ok_or("pass body to ask, or question to check one")?;
                    let result = call(
                        "POST",
                        &format!("{base}/questions"),
                        Some(json!({
                            "from": me,
                            "body": body,
                            "to": names_arg(args, "to")?,
                            "options": names_arg(args, "options")?,
                            "about": text_arg(args, "about")?,
                            "expiresIn": int_arg(args, "expires_in_seconds", 10, 2_592_000)?,
                            "key": random_hex(12),
                        })),
                    )?;
                    let id = result["id"].as_str().unwrap_or_default().to_string();
                    if wait == 0 {
                        return Ok(format!(
                            "Asked question {id} in room {room}. Check it with room_ask question={id} wait_seconds=60."
                        ));
                    }
                    id
                }
            };
            let result = link
                .call(
                    "GET",
                    &format!(
                        "{base}/questions/{}?agent={me_path}&wait={wait}",
                        percent_encode(&id)
                    ),
                    None,
                    Duration::from_secs(wait + 15),
                )
                .map_err(|e| e.to_string())?;
            let question = post_view(&result["question"]);
            let status = question["question"]["status"].as_str().unwrap_or("?");
            Ok(pretty(&json!({
                "question": question,
                "next": if status == "open" {
                    format!("Still open. Keep waiting with room_ask question={id} wait_seconds=60.")
                } else {
                    format!("The question is {status}.")
                },
            })))
        }
        "room_answer" => {
            let question = text_arg(args, "question")?.ok_or("question is required")?;
            let result = call(
                "POST",
                &format!("{base}/questions/{}/answers", percent_encode(&question)),
                Some(json!({
                    "from": me,
                    "body": text_arg(args, "body")?,
                    "choice": text_arg(args, "choice")?,
                    "key": random_hex(12),
                })),
            )?;
            Ok(if result["resolved"] == true {
                format!("Answered; question {question} is resolved")
            } else {
                let waiting: Vec<&str> = result["question"]["question"]["waitingOn"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect();
                format!(
                    "Answered; question {question} still waits for {}",
                    waiting.join(", ")
                )
            })
        }
        "room_board" => {
            let Some(key) = text_arg(args, "key")? else {
                let result = call("GET", &format!("{base}/board?agent={me_path}"), None)?;
                return Ok(match result["items"].as_array() {
                    Some(items) if !items.is_empty() => pretty(&result["items"]),
                    _ => format!("The board of room {room} is empty"),
                });
            };
            let path = format!("{base}/board/{}", percent_encode(&key));
            let version = int_arg(args, "version", 0, u64::MAX)?;
            if args.get("delete").and_then(Value::as_bool) == Some(true) {
                let mut query = format!("?from={me_path}");
                if let Some(version) = version {
                    query.push_str(&format!("&version={version}"));
                }
                call("DELETE", &format!("{path}{query}"), None)?;
                return Ok(format!("Removed {key} from the board"));
            }
            match text_arg(args, "value")? {
                Some(value) => {
                    let result = call(
                        "PUT",
                        &path,
                        Some(json!({ "from": me, "value": value, "version": version })),
                    )?;
                    Ok(format!("Set {key} (version {})", result["version"]))
                }
                None => {
                    let result = call("GET", &format!("{base}/board?agent={me_path}"), None)?;
                    let entry = result["items"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|e| e["key"] == key.as_str())
                        .cloned();
                    Ok(match entry {
                        Some(entry) => pretty(&entry),
                        None => format!("{key} is not on the board (set it with version 0)"),
                    })
                }
            }
        }
        _ => Err(format!("unknown tool {tool}")),
    }
}

fn instructions(name: &str) -> String {
    format!(
        "Postino delivers messages between agents. You are \"{name}\". Start with msg_whoami. \
         msg_read leases messages: act on them, then msg_ack their receipts, or they are delivered again. \
         Answer with msg_send reply_to=<message id> to keep the thread, and use msg_read wait_seconds to wait for a reply. \
         Use msg_broadcast for announcements every agent should see. \
         Agents working on the same workflow share a room: room_join it, follow it with room_read, \
         and use room_ask when you need a decision from someone, such as the operator. \
         For shared state, counters, locks, queues, and event streams use the db tool."
    )
}

fn write(out: &Mutex<Stdout>, value: &Value) {
    let mut out = out.lock().unwrap_or_else(|p| p.into_inner());
    let _ = writeln!(out, "{value}");
    let _ = out.flush();
}

fn result(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error(id: &Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn handle(link: &Arc<Link>, out: &Arc<Mutex<Stdout>>, line: &str) {
    let Ok(message) = serde_json::from_str::<Value>(line) else {
        return write(out, &error(&Value::Null, -32700, "Parse error"));
    };
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return; // A response to a server-initiated request; none are sent.
    };
    let Some(id) = message.get("id").filter(|id| !id.is_null()).cloned() else {
        return; // Notifications (initialized, cancelled, ...) need no reply.
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    match method {
        "initialize" => {
            let requested = params["protocolVersion"].as_str().unwrap_or("");
            let version = PROTOCOL_VERSIONS
                .iter()
                .find(|v| **v == requested)
                .unwrap_or(&PROTOCOL_VERSIONS[0]);
            write(
                out,
                &result(
                    &id,
                    json!({
                        "protocolVersion": version,
                        "capabilities": { "tools": { "listChanged": false } },
                        "serverInfo": { "name": "postino", "title": "Postino", "version": VERSION },
                        "instructions": instructions(&link.name()),
                    }),
                ),
            );
        }
        "ping" => write(out, &result(&id, json!({}))),
        "tools/list" => write(out, &result(&id, json!({ "tools": tools() }))),
        "tools/call" => {
            let tool = params["name"].as_str().unwrap_or("").to_string();
            if !tools()
                .as_array()
                .is_some_and(|all| all.iter().any(|t| t["name"] == tool.as_str()))
            {
                return write(out, &error(&id, -32602, &format!("Unknown tool: {tool}")));
            }
            let (link, out) = (Arc::clone(link), Arc::clone(out));
            // Tools run on their own thread so a blocking msg_read does not stall pings.
            thread::spawn(move || {
                let args = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let (text, is_error) = match call_tool(&link, &tool, &args) {
                    Ok(text) => (text, false),
                    Err(text) => (text, true),
                };
                write(
                    &out,
                    &result(
                        &id,
                        json!({ "content": [{ "type": "text", "text": text }], "isError": is_error }),
                    ),
                );
            });
        }
        _ => write(
            out,
            &error(&id, -32601, &format!("Method not found: {method}")),
        ),
    }
}

pub fn run(cfg: ClientConfig, name: String) -> Result<(), String> {
    let link = Arc::new(Link {
        cfg,
        client: Mutex::new(None),
        name: Mutex::new(name),
        session: random_hex(12),
        consumer: format!("mcp-{}", random_hex(6)),
        connected: AtomicU8::new(0),
    });
    link.heartbeat();
    let beat = Arc::clone(&link);
    thread::spawn(move || {
        loop {
            thread::sleep(HEARTBEAT);
            beat.heartbeat();
        }
    });
    let out = Arc::new(Mutex::new(io::stdout()));
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if !line.trim().is_empty() {
            handle(&link, &out, &line);
        }
    }
    link.leave();
    Ok(())
}
