mod common;

use std::io::BufRead;
use std::thread;
use std::time::{Duration, Instant};

use postino::client::{Client, Error};
use postino::config::{AgentToken, Auth};
use serde_json::{Value, json};

use common::{client, raw, start};

#[test]
fn send_lease_ack_and_history() {
    let broker = start(|_| {});
    let api = client(&broker);
    let sent = api
        .post(
            "/api/messages",
            &json!({ "from": "planner", "to": "builder", "body": "compile the docs" }),
        )
        .unwrap();
    assert_eq!(sent["created"], true);
    let id = sent["id"].as_str().unwrap().to_string();

    let check = api.get("/api/check/builder").unwrap();
    assert_eq!(check["unreadMessages"], 1);

    let leased = api
        .post("/api/messages/builder/read", &json!({ "limit": 5 }))
        .unwrap();
    let items = leased["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["state"], "leased");
    let receipt = items[0]["receipt"].as_str().unwrap();
    assert!(receipt.starts_with(&id));

    let acked = api
        .post(
            "/api/messages/builder/ack",
            &json!({ "receipts": [receipt] }),
        )
        .unwrap();
    assert_eq!(acked["acknowledged"].as_array().unwrap().len(), 1);
    assert_eq!(api.get("/api/check/builder").unwrap()["unreadMessages"], 0);

    let history = api.get("/api/history?agent=builder").unwrap();
    assert_eq!(history["items"][0]["state"], "acked");
    assert_eq!(history["items"][0]["consumer"], leased["consumer"]);
}

#[test]
fn replies_infer_the_recipient_and_keep_the_thread() {
    let broker = start(|_| {});
    let api = client(&broker);
    let question = api
        .post(
            "/api/messages",
            &json!({ "from": "planner", "to": "reviewer", "body": "ok to merge?" }),
        )
        .unwrap();
    let reply = api
        .post(
            "/api/messages",
            &json!({ "from": "reviewer", "replyTo": question["id"], "body": "yes", "strict": true }),
        )
        .unwrap();
    assert_eq!(reply["to"], "planner");
    assert_eq!(reply["thread"], question["id"]);
    let thread = api
        .get(&format!(
            "/api/history?thread={}",
            question["id"].as_str().unwrap()
        ))
        .unwrap();
    assert_eq!(thread["items"].as_array().unwrap().len(), 2);
}

#[test]
fn blocking_read_wakes_when_a_message_arrives() {
    let broker = start(|_| {});
    let api = client(&broker);
    api.post("/api/agents/waiter/presence", &json!({})).unwrap();
    let sender = client(&broker);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        sender
            .post(
                "/api/messages",
                &json!({ "from": "x", "to": "waiter", "body": "ping" }),
            )
            .unwrap();
    });
    let started = Instant::now();
    let leased = api
        .request(
            "POST",
            "/api/messages/waiter/read",
            Some(&json!({ "wait": 10 })),
            Duration::from_secs(20),
        )
        .unwrap();
    assert_eq!(leased["items"][0]["body"], "ping");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "woke after {:?}",
        started.elapsed()
    );

    let started = Instant::now();
    let empty = api
        .request(
            "POST",
            "/api/messages/waiter/read",
            Some(&json!({ "wait": 1 })),
            Duration::from_secs(10),
        )
        .unwrap();
    assert!(empty["items"].as_array().unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_millis(900));
}

#[test]
fn idempotency_key_prevents_duplicates() {
    let broker = start(|_| {});
    let api = client(&broker);
    let body = json!({ "to": "dedupe", "body": "once", "key": "job-42" });
    let first = api.post("/api/messages", &body).unwrap();
    let second = api.post("/api/messages", &body).unwrap();
    assert_eq!(first["id"], second["id"]);
    assert_eq!(second["created"], false);
    assert_eq!(api.get("/api/messages/dedupe").unwrap()["total"], 1);
}

#[test]
fn rejects_invalid_input() {
    let broker = start(|cfg| cfg.max_body = 2048);
    let api = client(&broker);
    let status = |result: Result<Value, Error>| match result {
        Err(Error::Api { status, .. }) => status,
        other => panic!(
            "expected an API error, got {:?}",
            other.map(|v| v.to_string())
        ),
    };
    assert_eq!(
        status(api.post("/api/messages", &json!({ "to": "a<b", "body": "x" }))),
        400
    );
    assert_eq!(
        status(api.post("/api/messages", &json!({ "to": "a", "body": "" }))),
        400
    );
    assert_eq!(
        status(api.post(
            "/api/messages",
            &json!({ "to": "a", "body": "x".repeat(4096) })
        )),
        413
    );
    assert_eq!(
        status(api.post(
            "/api/messages",
            &json!({ "to": "ghost", "body": "x", "strict": true })
        )),
        404
    );
    assert_eq!(status(api.get("/api/messages/ok?limit=0")), 400);
    assert_eq!(status(api.get("/api/nope")), 404);
}

#[test]
fn a_request_without_a_valid_token_is_answered_without_keeping_its_body() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin-secret".into()),
            agent_tokens: Vec::new(),
        };
        cfg.max_body = 1 << 20;
    });
    let body = "x".repeat(256 * 1024);
    let request = |token: &str| {
        format!(
            "POST /api/messages HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    // The body is read past rather than left unread, so the client gets
    // its answer instead of a reset connection.
    let (status, text) = raw(&broker, &request("wrong-secret"));
    assert_eq!(status, 401, "{text}");
    let (status, text) = raw(&broker, &request("admin-secret"));
    assert_eq!(status, 400, "a body the token may send is read: {text}");
}

#[test]
fn tokens_scope_agents_to_their_own_inbox() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin-secret".into()),
            agent_tokens: vec![
                AgentToken::new("worker", "worker-secret"),
                AgentToken::new("other", "other-secret"),
            ],
        };
    });
    let anonymous = client(&broker);
    let admin = Client::new(&broker.url, Some("admin-secret".into())).unwrap();
    let worker = Client::new(&broker.url, Some("worker-secret".into())).unwrap();
    let status = |result: Result<Value, Error>| match result {
        Err(Error::Api { status, .. }) => status,
        Ok(_) => 200,
        Err(e) => panic!("{e}"),
    };

    assert_eq!(anonymous.get("/api/health").unwrap()["service"], "postino");
    assert_eq!(status(anonymous.get("/api/agents")), 401);
    admin
        .post("/api/agents/other/presence", &json!({}))
        .unwrap();

    let sent = worker
        .post(
            "/api/messages",
            &json!({ "from": "spoofed", "to": "other", "body": "hi" }),
        )
        .unwrap();
    assert_eq!(
        sent["message"]["from"], "worker",
        "agents cannot choose their sender"
    );
    assert_eq!(status(worker.get("/api/messages/other")), 403);
    assert_eq!(
        status(worker.post("/api/messages/other/read", &json!({}))),
        403
    );
    assert_eq!(status(worker.get("/api/overview")), 403);
    assert_eq!(
        status(worker.post("/api/messages", &json!({ "to": "nobody", "body": "x" }))),
        404
    );
    assert_eq!(status(worker.get("/api/history?agent=other")), 403);
    assert_eq!(status(worker.get("/api/check/worker")), 200);
    assert_eq!(status(admin.get("/api/overview")), 200);
}

#[test]
fn blocks_dns_rebinding_and_cross_origin_writes() {
    let broker = start(|_| {});
    let port = common::port(&broker);
    let (status, _) = raw(
        &broker,
        "GET /api/agents HTTP/1.1\r\nHost: evil.example\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(status, 403);
    let (status, _) = raw(
        &broker,
        &format!("GET /api/agents HTTP/1.1\r\nHost: localhost:{port}\r\nConnection: close\r\n\r\n"),
    );
    assert_eq!(status, 200);
    let body = r#"{"to":"victim","body":"x"}"#;
    let post = |origin: &str| {
        raw(
            &broker,
            &format!(
                "POST /api/messages HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: {origin}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
        )
        .0
    };
    assert_eq!(post("http://evil.example"), 403);
    assert_eq!(post(&format!("http://127.0.0.1:{port}")), 200);
}

#[test]
fn rejects_malformed_http() {
    let broker = start(|_| {});
    let (status, _) = raw(
        &broker,
        "POST /api/messages HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
    );
    assert_eq!(status, 501);
    let (status, _) = raw(&broker, "GET / HTTP/9\r\n\r\n");
    assert_eq!(status, 505);
}

#[test]
fn event_stream_delivers_and_replays() {
    let broker = start(|_| {});
    let api = client(&broker);
    let mut stream = api.events("/api/events?agent=listener").unwrap();
    let ready = stream.next().unwrap();
    assert!(ready.data.contains("ready"));
    api.post(
        "/api/messages",
        &json!({ "from": "x", "to": "listener", "body": "hello" }),
    )
    .unwrap();
    api.post(
        "/api/messages",
        &json!({ "from": "x", "to": "someone-else", "body": "private" }),
    )
    .unwrap();
    let mut first_id = None;
    let message = loop {
        let event = stream.next().unwrap();
        let value: Value = serde_json::from_str(&event.data).unwrap();
        first_id.get_or_insert(event.id.clone().unwrap());
        assert_ne!(value["message"]["to"], "someone-else");
        if value["type"] == "message" {
            break value;
        }
    };
    assert_eq!(message["message"]["body"], "hello");

    // Reconnecting with Last-Event-ID replays what came after it.
    let replay = api
        .events(&format!("/api/events?lastEventId={}", first_id.unwrap()))
        .unwrap();
    let kinds: Vec<String> = replay
        .take_while(|e| !e.data.contains("\"ready\""))
        .map(|e| {
            serde_json::from_str::<Value>(&e.data).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert!(kinds.iter().any(|k| k == "message"), "replayed {kinds:?}");
}

#[test]
fn presence_rename_and_cleanup() {
    let broker = start(|_| {});
    let api = client(&broker);
    let session = api
        .post("/api/agents/agent-1234/presence", &json!({ "ttl": 30 }))
        .unwrap();
    let agents = api.get("/api/agents").unwrap();
    assert_eq!(agents["items"][0]["online"], true);
    api.post(
        "/api/agents/agent-1234/rename",
        &json!({ "to": "reviewer" }),
    )
    .unwrap();
    let agents = api.get("/api/agents").unwrap();
    assert_eq!(agents["items"][0]["name"], "reviewer");
    assert_eq!(agents["items"][0]["online"], true);
    api.delete(&format!(
        "/api/agents/reviewer/presence/{}",
        session["session"].as_str().unwrap()
    ))
    .unwrap();
    let removed = api.post("/api/cleanup", &json!({})).unwrap();
    assert_eq!(removed["removed"], json!(["reviewer"]));
}

#[test]
fn keep_alive_serves_several_requests_per_connection() {
    let broker = start(|_| {});
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", common::port(&broker))).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
    for _ in 0..3 {
        std::io::Write::write_all(
            &mut stream,
            b"GET /api/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .unwrap();
        let mut status = String::new();
        reader.read_line(&mut status).unwrap();
        assert!(status.starts_with("HTTP/1.1 200"));
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.strip_prefix("Content-Length: ") {
                length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; length];
        std::io::Read::read_exact(&mut reader, &mut body).unwrap();
    }
}

#[test]
fn leasing_by_id_settles_exactly_one_message() {
    let broker = start(|_| {});
    let api = client(&broker);
    api.post(
        "/api/messages",
        &json!({ "from": "a", "to": "operator", "body": "first" }),
    )
    .unwrap();
    let second = api
        .post(
            "/api/messages",
            &json!({ "from": "a", "to": "operator", "body": "second" }),
        )
        .unwrap();
    let leased = api
        .post(
            "/api/messages/operator/read",
            &json!({ "ids": [second["id"]] }),
        )
        .unwrap();
    assert_eq!(leased["items"].as_array().unwrap().len(), 1);
    assert_eq!(leased["items"][0]["body"], "second");
    api.post(
        "/api/messages/operator/ack",
        &json!({ "receipts": [leased["items"][0]["receipt"]] }),
    )
    .unwrap();
    let pending = api.get("/api/messages/operator").unwrap();
    assert_eq!(pending["total"], 1);
    assert_eq!(pending["items"][0]["body"], "first");
    assert_eq!(pending["items"][0]["state"], "queued");
}

fn status(result: Result<Value, Error>) -> u16 {
    match result {
        Err(Error::Api { status, .. }) => status,
        Ok(_) => 200,
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn rooms_carry_a_workflow_conversation() {
    let broker = start(|_| {});
    let api = client(&broker);
    let created = api
        .post(
            "/api/rooms",
            &json!({ "name": "release-42", "from": "planner", "topic": "ship 4.2", "members": ["builder"] }),
        )
        .unwrap();
    assert_eq!(created["created"], true);
    assert_eq!(created["room"]["members"][0]["role"], "member");
    assert_eq!(
        status(api.post("/api/rooms", &json!({ "name": "release-42", "from": "x" }))),
        409
    );
    let joined = api
        .post(
            "/api/rooms",
            &json!({ "name": "release-42", "from": "tester", "join": true }),
        )
        .unwrap();
    assert_eq!(joined["created"], false);

    api.post(
        "/api/rooms/release-42/posts",
        &json!({ "from": "planner", "body": "plan is on the board", "about": "task-1" }),
    )
    .unwrap();
    let read = api
        .post("/api/rooms/release-42/read", &json!({ "agent": "builder" }))
        .unwrap();
    let bodies: Vec<&str> = read["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["body"].as_str().unwrap())
        .collect();
    assert_eq!(
        bodies,
        [
            "created the room: ship 4.2",
            "joined",
            "plan is on the board"
        ]
    );
    assert_eq!(read["items"][1]["from"], "tester");

    // The board rejects writes based on a stale version.
    let first = api
        .request(
            "PUT",
            "/api/rooms/release-42/board/plan",
            Some(&json!({ "from": "planner", "value": "1. build 2. test", "version": 0 })),
            Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(first["version"], 1);
    let stale = api.request(
        "PUT",
        "/api/rooms/release-42/board/plan",
        Some(&json!({ "from": "builder", "value": "other", "version": 0 })),
        Duration::from_secs(5),
    );
    assert_eq!(status(stale), 409);
    let board = api.get("/api/rooms/release-42/board").unwrap();
    assert_eq!(board["items"][0]["value"], "1. build 2. test");

    // A question to the operator blocks the asker until it is answered.
    let asked = api
        .post(
            "/api/rooms/release-42/questions",
            &json!({ "from": "builder", "body": "deploy to production?", "to": ["operator"], "options": ["yes", "no"] }),
        )
        .unwrap();
    let id = asked["id"].as_str().unwrap().to_string();
    let check = api.get("/api/check/operator").unwrap();
    assert_eq!(check["rooms"][0]["waiting"], 1);
    let operator = client(&broker);
    let path = format!("/api/rooms/release-42/questions/{id}/answers");
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        operator
            .post(&path, &json!({ "choice": "yes", "body": "go ahead" }))
            .unwrap();
    });
    let started = Instant::now();
    let settled = api
        .request(
            "GET",
            &format!("/api/rooms/release-42/questions/{id}?agent=builder&wait=10"),
            None,
            Duration::from_secs(20),
        )
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(settled["question"]["question"]["status"], "resolved");
    assert_eq!(
        settled["question"]["question"]["answers"][0]["from"],
        "operator"
    );
    assert_eq!(
        status(api.post(
            &format!("/api/rooms/release-42/questions/{id}/answers"),
            &json!({ "from": "tester", "choice": "no" }),
        )),
        409
    );

    let about = api.get("/api/rooms/release-42/posts?about=task-1").unwrap();
    assert_eq!(about["items"].as_array().unwrap().len(), 1);
    let rooms = api.get("/api/rooms?agent=builder").unwrap();
    assert_eq!(rooms["items"][0]["memberCount"], 4);

    api.request(
        "PATCH",
        "/api/rooms/release-42",
        Some(&json!({ "from": "planner", "closed": true })),
        Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(
        status(api.post(
            "/api/rooms/release-42/posts",
            &json!({ "from": "planner", "body": "late" }),
        )),
        409
    );
}

#[test]
fn blocking_room_read_wakes_on_a_post() {
    let broker = start(|_| {});
    let api = client(&broker);
    api.post(
        "/api/rooms",
        &json!({ "name": "pair", "from": "left", "members": ["right"] }),
    )
    .unwrap();
    api.post("/api/rooms/pair/read", &json!({ "agent": "right" }))
        .unwrap();
    let poster = client(&broker);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        poster
            .post(
                "/api/rooms/pair/posts",
                &json!({ "from": "left", "body": "your turn" }),
            )
            .unwrap();
    });
    let started = Instant::now();
    let read = api
        .request(
            "POST",
            "/api/rooms/pair/read",
            Some(&json!({ "agent": "right", "wait": 10 })),
            Duration::from_secs(20),
        )
        .unwrap();
    assert_eq!(read["items"][0]["body"], "your turn");
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[test]
fn agent_tokens_respect_private_rooms_and_roles() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin-secret".into()),
            agent_tokens: vec![
                AgentToken::new("lead", "lead-secret"),
                AgentToken::new("outsider", "outsider-secret"),
            ],
        };
    });
    let admin = Client::new(&broker.url, Some("admin-secret".into())).unwrap();
    let lead = Client::new(&broker.url, Some("lead-secret".into())).unwrap();
    let outsider = Client::new(&broker.url, Some("outsider-secret".into())).unwrap();
    let created = lead
        .post(
            "/api/rooms",
            &json!({ "name": "incident", "from": "spoofed", "private": true }),
        )
        .unwrap();
    assert_eq!(created["room"]["createdBy"], "lead");
    assert_eq!(status(outsider.get("/api/rooms/incident")), 403);
    assert_eq!(status(outsider.get("/api/rooms/incident/posts")), 403);
    assert_eq!(
        status(outsider.post("/api/rooms/incident/members", &json!({}))),
        403
    );
    assert_eq!(
        outsider.get("/api/rooms").unwrap()["total"],
        0,
        "private rooms are not listed to outsiders"
    );
    assert_eq!(
        status(outsider.post("/api/rooms/incident/posts", &json!({ "body": "let me in" }))),
        403
    );
    lead.post(
        "/api/rooms/incident/members",
        &json!({ "agent": "outsider" }),
    )
    .unwrap();
    assert_eq!(status(outsider.get("/api/rooms/incident")), 200);
    assert_eq!(
        status(outsider.request(
            "PATCH",
            "/api/rooms/incident",
            Some(&json!({ "closed": true })),
            Duration::from_secs(5)
        )),
        403
    );
    assert_eq!(status(admin.delete("/api/rooms/incident")), 200);
}

fn code(result: Result<Value, Error>) -> u16 {
    match result {
        Err(Error::Api { status, .. }) => status,
        Ok(_) => 200,
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn hosts_that_only_start_like_loopback_are_refused() {
    let broker = start(|_| {});
    for host in [
        "127.0.0.1.evil.example",
        "localhost.evil.example",
        "127.0.0.1.nip.io:2790",
    ] {
        let (status, _) = raw(
            &broker,
            &format!("GET /api/agents HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"),
        );
        assert_eq!(status, 403, "{host}");
    }
}

#[test]
fn the_agent_header_narrows_any_token_to_one_agent() {
    let broker = start(|_| {});
    let operator = client(&broker);
    let alice = client(&broker).as_agent("alice");
    operator
        .post(
            "/api/messages",
            &json!({ "to": "bob", "body": "for bob only" }),
        )
        .unwrap();
    operator
        .post(
            "/api/messages",
            &json!({ "to": "alice", "body": "for alice" }),
        )
        .unwrap();
    assert_eq!(code(alice.get("/api/messages/bob")), 403);
    assert_eq!(alice.get("/api/messages/alice").unwrap()["total"], 1);
    assert_eq!(code(alice.post("/api/cleanup", &json!({}))), 403);
    assert_eq!(code(alice.post("/api/db", &json!(["FLUSHALL"]))), 403);
    let sent = alice
        .post(
            "/api/messages",
            &json!({ "from": "bob", "to": "bob", "body": "spoof" }),
        )
        .unwrap();
    assert_eq!(
        sent["message"]["from"], "alice",
        "a narrowed caller cannot pick its sender"
    );
    assert_eq!(whoami(&alice)["role"], "agent");
    assert_eq!(
        code(client(&broker).as_agent("operator").get("/api/agents")),
        403
    );
    assert_eq!(
        code(client(&broker).as_agent("not a name").get("/api/agents")),
        400
    );
}

fn whoami(api: &Client) -> Value {
    api.get("/api/whoami").unwrap()
}

#[test]
fn agent_tokens_cannot_narrow_to_someone_else() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: None,
            agent_tokens: vec![AgentToken::new("worker", "worker-token-0123456789")],
        };
    });
    let worker = Client::new(&broker.url, Some("worker-token-0123456789".into())).unwrap();
    assert_eq!(whoami(&worker.as_agent("worker"))["agent"], "worker");
    assert_eq!(code(worker.as_agent("other").get("/api/agents")), 403);
    assert_eq!(
        code(worker.post("/api/agents/worker/rename", &json!({ "to": "boss" }))),
        403,
        "agent tokens are bound to their name"
    );
}

#[test]
fn agents_rename_only_themselves_and_only_to_unused_names() {
    let broker = start(|_| {});
    let operator = client(&broker);
    let alice = client(&broker).as_agent("alice");
    alice
        .post("/api/agents/alice/presence", &json!({}))
        .unwrap();
    operator
        .post(
            "/api/rooms",
            &json!({ "name": "ops", "private": true, "members": ["planner"] }),
        )
        .unwrap();
    // "planner" is a member of a private room it has never visited: taking
    // the name would hand over the room.
    assert_eq!(
        code(alice.post("/api/agents/alice/rename", &json!({ "to": "planner" }))),
        409
    );
    assert_eq!(
        code(alice.post("/api/agents/bob/rename", &json!({ "to": "carol" }))),
        403
    );
    assert_eq!(
        code(alice.post("/api/agents/alice/rename", &json!({ "to": "operator" }))),
        400
    );
    alice
        .post("/api/agents/alice/rename", &json!({ "to": "reviewer" }))
        .unwrap();
    let names: Vec<Value> = operator.get("/api/agents").unwrap()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].clone())
        .collect();
    assert!(names.contains(&json!("reviewer")), "{names:?}");
}

#[test]
fn idempotency_keys_do_not_cross_senders() {
    let broker = start(|_| {});
    let bob = client(&broker).as_agent("bob");
    let mallory = client(&broker).as_agent("mallory");
    client(&broker)
        .post("/api/agents/alice/presence", &json!({}))
        .unwrap();
    let first = bob
        .post(
            "/api/messages",
            &json!({ "to": "alice", "body": "the secret", "key": "deploy-1" }),
        )
        .unwrap();
    let second = mallory
        .post(
            "/api/messages",
            &json!({ "to": "alice", "body": "hi", "key": "deploy-1" }),
        )
        .unwrap();
    assert_eq!(second["created"], true, "mallory's message is her own");
    assert_ne!(second["id"], first["id"]);
    assert_eq!(second["message"]["body"], "hi");
    let inbox = client(&broker).get("/api/messages/alice").unwrap();
    assert_eq!(inbox["total"], 2, "both messages were delivered");
}

#[test]
fn members_cannot_widen_a_private_room_with_a_question() {
    let broker = start(|_| {});
    let lead = client(&broker).as_agent("lead");
    let member = client(&broker).as_agent("member");
    let outsider = client(&broker).as_agent("outsider");
    for (agent, name) in [(&member, "member"), (&outsider, "outsider")] {
        agent
            .post(&format!("/api/agents/{name}/presence"), &json!({}))
            .unwrap();
    }
    lead.post(
        "/api/rooms",
        &json!({ "name": "incident", "private": true, "members": ["member"] }),
    )
    .unwrap();
    let asked = member.post(
        "/api/rooms/incident/questions",
        &json!({ "body": "rotate the keys?", "to": ["outsider"] }),
    );
    assert_eq!(code(asked), 403);
    assert_eq!(code(outsider.get("/api/rooms/incident")), 403);
    // The owner can still bring someone in by asking them.
    lead.post(
        "/api/rooms/incident/questions",
        &json!({ "body": "can you help?", "to": ["outsider"] }),
    )
    .unwrap();
    assert_eq!(code(outsider.get("/api/rooms/incident")), 200);
}

#[test]
fn outsiders_learn_nothing_about_a_private_room() {
    let broker = start(|_| {});
    let lead = client(&broker).as_agent("lead");
    client(&broker)
        .as_agent("bob")
        .post("/api/agents/bob/presence", &json!({}))
        .unwrap();
    lead.post(
        "/api/rooms",
        &json!({ "name": "deal", "private": true, "members": ["bob"] }),
    )
    .unwrap();
    let ask = |body: Value| {
        lead.post("/api/rooms/deal/questions", &body).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let open = ask(json!({ "body": "approve?", "to": ["bob"], "options": ["yes", "no"] }));
    let resolved = ask(json!({ "body": "which bank?" }));
    client(&broker)
        .as_agent("bob")
        .post(
            &format!("/api/rooms/deal/questions/{resolved}/answers"),
            &json!({ "body": "first" }),
        )
        .unwrap();
    let missing = "1-0";
    let outsider = client(&broker).as_agent("outsider");
    let refusal = |result: Result<Value, Error>| match result {
        Err(Error::Api {
            status, message, ..
        }) => (status, message),
        other => panic!("expected a refusal, got {other:?}"),
    };
    let put = |path: &str, body: Value| {
        outsider.request("PUT", path, Some(&body), Duration::from_secs(5))
    };
    let patch = |path: &str, body: Value| {
        outsider.request("PATCH", path, Some(&body), Duration::from_secs(5))
    };
    // Whatever exists, whoever is a member, and whatever the questions ask,
    // the answer is the same.
    let probes = [
        outsider.post(
            "/api/rooms/deal/posts",
            &json!({ "body": "x", "replyTo": open }),
        ),
        outsider.post(
            "/api/rooms/deal/posts",
            &json!({ "body": "x", "replyTo": missing }),
        ),
        outsider.post(
            "/api/rooms/deal/questions",
            &json!({ "body": "x", "to": ["bob"] }),
        ),
        outsider.post(
            "/api/rooms/deal/questions",
            &json!({ "body": "x", "to": ["dave"] }),
        ),
        outsider.post(
            &format!("/api/rooms/deal/questions/{open}/answers"),
            &json!({}),
        ),
        outsider.post(
            &format!("/api/rooms/deal/questions/{resolved}/answers"),
            &json!({ "body": "x" }),
        ),
        outsider.post(
            &format!("/api/rooms/deal/questions/{missing}/answers"),
            &json!({ "body": "x" }),
        ),
        outsider.delete(&format!("/api/rooms/deal/questions/{open}")),
        outsider.delete(&format!("/api/rooms/deal/questions/{missing}")),
        outsider.get(&format!("/api/rooms/deal/questions/{open}")),
        outsider.get(&format!("/api/rooms/deal/questions/{missing}")),
        outsider.delete("/api/rooms/deal/members/bob"),
        outsider.delete("/api/rooms/deal/members/dave"),
        outsider.post("/api/rooms/deal/members", &json!({})),
        put("/api/rooms/deal/board/plan", json!({ "value": "x" })),
        patch("/api/rooms/deal", json!({ "topic": "x" })),
        outsider.delete("/api/rooms/deal"),
    ];
    let expected = refusal(outsider.get("/api/rooms/deal"));
    assert_eq!(expected.0, 403);
    for (i, probe) in probes.into_iter().enumerate() {
        assert_eq!(refusal(probe), expected, "probe {i}");
    }
}

#[test]
fn agents_do_not_see_where_their_peers_run() {
    let broker = start(|_| {});
    let operator = client(&broker);
    operator
        .post(
            "/api/agents/builder/presence",
            &json!({ "host": "build-7.internal", "pid": 4242, "client": "mcp" }),
        )
        .unwrap();
    let seen_by_peer = client(&broker)
        .as_agent("alice")
        .get("/api/agents")
        .unwrap();
    assert!(
        !seen_by_peer.to_string().contains("build-7.internal"),
        "{seen_by_peer}"
    );
    assert!(!seen_by_peer.to_string().contains("4242"), "{seen_by_peer}");
}

#[test]
fn one_agent_cannot_hold_every_connection() {
    let broker = start(|_| {});
    let port = common::port(&broker);
    let open = |agent: &str| {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        std::io::Write::write_all(
            &mut stream,
            format!(
                "GET /api/events HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nX-Postino-Agent: {agent}\r\n\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
        let mut reader = std::io::BufReader::new(stream);
        let mut status = String::new();
        reader.read_line(&mut status).unwrap();
        (status, reader)
    };
    let mut held = Vec::new();
    for _ in 0..32 {
        let (status, reader) = open("greedy");
        assert!(status.contains(" 200 "), "{status}");
        held.push(reader);
    }
    let (status, _) = open("greedy");
    assert!(status.contains(" 429 "), "{status}");
    let (status, _) = open("polite");
    assert!(
        status.contains(" 200 "),
        "other agents still get in: {status}"
    );
    drop(held);
}

#[test]
fn a_stream_whose_client_left_gives_its_place_back_within_seconds() {
    let broker = start(|_| {});
    let port = common::port(&broker);
    let open = |agent: &str, route: &str| {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        std::io::Write::write_all(
            &mut stream,
            format!(
                "GET {route} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nX-Postino-Agent: {agent}\r\n\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
        let mut reader = std::io::BufReader::new(stream);
        let mut status = String::new();
        reader.read_line(&mut status).unwrap();
        (status, reader)
    };
    for (agent, route) in [
        ("restless", "/api/events"),
        ("listener", "/api/subscribe?channel=news"),
    ] {
        let held: Vec<_> = (0..32).map(|_| open(agent, route)).collect();
        assert!(held.iter().all(|(status, _)| status.contains(" 200 ")));
        assert!(
            open(agent, route).0.contains(" 429 "),
            "{route}: the agent is at its limit"
        );
        drop(held);
        // Nothing comes on these streams to find their clients gone.
        let left = Instant::now();
        while !open(agent, route).0.contains(" 200 ") {
            assert!(
                left.elapsed() < Duration::from_secs(5),
                "{route}: the streams of clients that left still hold their places"
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
}

#[test]
fn agents_name_only_agents_the_server_knows() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin-secret".into()),
            agent_tokens: vec![
                AgentToken::new("lead", "lead-secret"),
                AgentToken::new("ci", "ci-secret"),
            ],
        };
    });
    let admin = Client::new(&broker.url, Some("admin-secret".into())).unwrap();
    let lead = Client::new(&broker.url, Some("lead-secret".into())).unwrap();
    let room = |api: &Client, name: &str, members: &[&str]| {
        api.post(
            "/api/rooms",
            &json!({ "name": name, "from": "lead", "members": members }),
        )
    };
    assert_eq!(code(room(&lead, "a", &["ghost"])), 404);
    room(&lead, "b", &["ci"]).expect("ci has a token, so the server knows it");
    assert_eq!(
        code(lead.post(
            "/api/rooms/b/questions",
            &json!({ "body": "ship it?", "to": ["ghost"] }),
        )),
        404
    );
    let names = |api: &Client| -> Vec<String> {
        api.get("/api/agents").unwrap()["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["name"].as_str().unwrap().to_string())
            .collect()
    };
    assert!(!names(&admin).contains(&"ghost".to_string()));
    room(&admin, "c", &["ghost"]).expect("the administrator token names anyone");
    assert!(names(&admin).contains(&"ghost".to_string()));
}

#[test]
fn agents_see_only_their_own_inbox_counts() {
    let broker = start(|_| {});
    let operator = client(&broker);
    let alice = client(&broker).as_agent("alice");
    alice
        .post("/api/agents/alice/presence", &json!({}))
        .unwrap();
    operator
        .post("/api/messages", &json!({ "to": "bob", "body": "for bob" }))
        .unwrap();
    let listed = alice.get("/api/agents").unwrap();
    let entry = |name: &str| -> Value {
        listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["name"] == name)
            .cloned()
            .unwrap()
    };
    assert_eq!(entry("alice")["pending"], 0);
    assert_eq!(entry("bob").get("pending"), None, "{listed}");
    assert_eq!(entry("bob")["online"], false);
    let all = operator.get("/api/agents").unwrap();
    assert!(
        all["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["name"] == "bob" && a["pending"] == 1),
        "{all}"
    );
}

#[test]
fn no_agent_passes_for_the_operator() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin-secret".into()),
            agent_tokens: vec![
                AgentToken::new("lead", "lead-secret"),
                AgentToken::new("ci", "ci-secret"),
            ],
        };
    });
    let admin = Client::new(&broker.url, Some("admin-secret".into())).unwrap();
    for name in ["Operator", "OPERATOR"] {
        assert_eq!(code(admin.as_agent(name).get("/api/whoami")), 403);
    }
    let alice = admin.as_agent("alice");
    alice
        .post("/api/agents/alice/presence", &json!({}))
        .unwrap();
    assert_eq!(
        code(alice.post("/api/agents/alice/rename", &json!({ "to": "Operator" }))),
        400
    );
    // An agent token learns nothing about which names have tokens.
    let lead = Client::new(&broker.url, Some("lead-secret".into())).unwrap();
    for to in ["ci", "nobody"] {
        assert_eq!(
            code(lead.post("/api/agents/alice/rename", &json!({ "to": to }))),
            403
        );
    }
}

#[test]
fn requests_anyone_may_send_take_no_body() {
    let broker = start(|_| {});
    let head = |request: &str, body: &str| {
        format!(
            "{request} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    // Bodies on these would be held for anyone, so none is read.
    assert_eq!(raw(&broker, &head("OPTIONS /api/messages", "hello")).0, 400);
    assert_eq!(raw(&broker, &head("GET /api/health", "{}")).0, 400);
    assert_eq!(raw(&broker, &head("GET /api/health", "")).0, 200);
}

#[test]
fn streams_need_a_token() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin-token-0123456789".into()),
            agent_tokens: Vec::new(),
        };
    });
    for route in ["events", "subscribe?channel=c", "tail/k"] {
        let request =
            format!("GET /api/{route} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        assert_eq!(raw(&broker, &request).0, 401, "{route}");
    }
}
