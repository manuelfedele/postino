mod common;

use std::thread;
use std::time::{Duration, Instant};

use postino::client::{Client, Error};
use postino::config::{AgentToken, Auth};
use serde_json::{Value, json};

use common::{client, start};

fn db(api: &Client, command: Value) -> Result<Value, Error> {
    api.request("POST", "/api/db", Some(&command), Duration::from_secs(20))
        .map(|reply| reply["result"].clone())
}

#[test]
fn commands_run_over_http() {
    let broker = start(|_| {});
    let api = client(&broker);
    assert_eq!(
        db(&api, json!(["SET", "plan", "v1", "EX", 60])).unwrap(),
        "OK"
    );
    assert_eq!(
        db(&api, json!({ "command": ["GET", "plan"] })).unwrap(),
        "v1"
    );
    assert_eq!(db(&api, json!(["INCRBY", "hits", 3])).unwrap(), 3);
    db(
        &api,
        json!(["HSET", "task:1", "state", "open", "owner", "planner"]),
    )
    .unwrap();
    assert_eq!(
        db(&api, json!(["HGETALL", "task:1"])).unwrap(),
        json!({ "owner": "planner", "state": "open" })
    );
    assert_eq!(db(&api, json!(["GET", "missing"])).unwrap(), Value::Null);
    match db(&api, json!(["LPUSH", "plan", "x"])) {
        Err(Error::Api {
            status, message, ..
        }) => {
            assert_eq!(status, 400);
            assert!(message.starts_with("WRONGTYPE"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        db(&api, json!([])),
        Err(Error::Api { status: 400, .. })
    ));
    let stats = api.get("/api/stats").unwrap();
    assert_eq!(stats["keyCount"], 3);
}

#[test]
fn batches_run_atomically_and_report_each_result() {
    let broker = start(|_| {});
    let api = client(&broker);
    let results = api
        .post(
            "/api/db/multi",
            &json!([
                ["SET", "a", "1"],
                ["INCR", "a"],
                ["LPUSH", "a", "x"],
                ["GET", "a"]
            ]),
        )
        .unwrap();
    assert_eq!(
        results,
        json!([
            { "result": "OK" },
            { "result": 2 },
            { "error": "WRONGTYPE Operation against a key holding the wrong kind of value" },
            { "result": "2" },
        ])
    );
}

#[test]
fn blocking_pops_wake_when_work_arrives() {
    let broker = start(|_| {});
    let api = client(&broker);
    let producer = client(&broker);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        db(&producer, json!(["RPUSH", "jobs", "build"])).unwrap();
    });
    let started = Instant::now();
    assert_eq!(
        db(&api, json!(["BLPOP", "jobs", 10])).unwrap(),
        json!(["jobs", "build"])
    );
    assert!(started.elapsed() < Duration::from_secs(3));
    let started = Instant::now();
    assert_eq!(
        db(&api, json!(["BLPOP", "jobs", 0.5])).unwrap(),
        Value::Null
    );
    assert!(started.elapsed() >= Duration::from_millis(450));

    // A consumer group reader parked on an empty stream wakes on XADD.
    db(
        &api,
        json!(["XGROUP", "CREATE", "events", "workers", "$", "MKSTREAM"]),
    )
    .unwrap();
    let producer = client(&broker);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        db(&producer, json!(["XADD", "events", "*", "kind", "deploy"])).unwrap();
    });
    let read = db(
        &api,
        json!([
            "XREADGROUP",
            "GROUP",
            "workers",
            "w1",
            "BLOCK",
            10000,
            "STREAMS",
            "events",
            ">"
        ]),
    )
    .unwrap();
    assert_eq!(read[0][0], "events");
    assert_eq!(read[0][1][0][1], json!(["kind", "deploy"]));
}

#[test]
fn pubsub_streams_messages_and_key_changes() {
    let broker = start(|_| {});
    let api = client(&broker);
    let mut events = api
        .events("/api/subscribe?channel=builds&pattern=__keyspace__:task:*")
        .unwrap();
    let subscribed: Value = serde_json::from_str(&events.next().unwrap().data).unwrap();
    assert_eq!(subscribed["type"], "subscribed");
    assert_eq!(db(&api, json!(["PUBLISH", "builds", "green"])).unwrap(), 1);
    assert_eq!(db(&api, json!(["PUBLISH", "nobody", "x"])).unwrap(), 0);
    let message: Value = serde_json::from_str(&events.next().unwrap().data).unwrap();
    assert_eq!(
        message,
        json!({ "type": "message", "channel": "builds", "message": "green" })
    );
    db(&api, json!(["HSET", "task:9", "state", "done"])).unwrap();
    let change: Value = serde_json::from_str(&events.next().unwrap().data).unwrap();
    assert_eq!(change["type"], "pmessage");
    assert_eq!(change["channel"], "__keyspace__:task:9");
    assert_eq!(change["message"], "hset");
    assert_eq!(
        db(&api, json!(["PUBSUB", "NUMSUB", "builds"])).unwrap(),
        json!(["builds", 1])
    );
}

#[test]
fn patterns_and_channel_names_are_bounded() {
    let broker = start(|_| {});
    let api = client(&broker);
    let long = "*".repeat(257);
    for command in [
        json!(["KEYS", long]),
        json!(["SCAN", "0", "MATCH", long]),
        json!(["PUBSUB", "CHANNELS", long]),
        json!(["PUBLISH", "c".repeat(257), "x"]),
    ] {
        assert_eq!(status(db(&api, command.clone())), 400, "{command}");
    }
    assert_eq!(
        db(&api, json!(["KEYS", "*".repeat(256)])).unwrap(),
        json!([])
    );
}

#[test]
fn any_pattern_that_can_match_a_notification_receives_it() {
    let broker = start(|_| {});
    let api = client(&broker);
    let mut events = api.events("/api/subscribe?pattern=?_keyspace__:*").unwrap();
    events.next().unwrap();
    db(&api, json!(["SET", "plan", "v1"])).unwrap();
    let change: Value = serde_json::from_str(&events.next().unwrap().data).unwrap();
    assert_eq!(change["channel"], "__keyspace__:plan");
}

#[test]
fn tail_follows_a_stream_and_resumes_by_id() {
    let broker = start(|_| {});
    let api = client(&broker);
    let first = db(&api, json!(["XADD", "log", "*", "line", "one"])).unwrap();
    let mut tail = api.events("/api/tail/log?from=0").unwrap();
    let event = tail.next().unwrap();
    assert_eq!(event.id.as_deref(), first.as_str());
    let entry: Value = serde_json::from_str(&event.data).unwrap();
    assert_eq!(entry["fields"], json!({ "line": "one" }));
    let producer = client(&broker);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        db(&producer, json!(["XADD", "log", "*", "line", "two"])).unwrap();
    });
    let live: Value = serde_json::from_str(&tail.next().unwrap().data).unwrap();
    assert_eq!(live["fields"]["line"], "two");
    let mut resumed = api
        .events(&format!("/api/tail/log?from={}", first.as_str().unwrap()))
        .unwrap();
    let again: Value = serde_json::from_str(&resumed.next().unwrap().data).unwrap();
    assert_eq!(again["fields"]["line"], "two", "resumes after the given id");
}

#[test]
fn scoped_agents_cannot_flush() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin".into()),
            agent_tokens: vec![AgentToken::new("worker", "worker-secret")],
        };
    });
    let worker = Client::new(&broker.url, Some("worker-secret".into())).unwrap();
    let admin = Client::new(&broker.url, Some("admin".into())).unwrap();
    assert_eq!(
        db(&worker, json!(["SET", "shared:flag", "1"])).unwrap(),
        "OK"
    );
    assert!(matches!(
        db(&worker, json!(["FLUSHALL"])),
        Err(Error::Api { status: 403, .. })
    ));
    assert_eq!(db(&admin, json!(["FLUSHALL"])).unwrap(), "OK");
    assert_eq!(db(&admin, json!(["DBSIZE"])).unwrap(), 0);
}

fn status(result: Result<Value, Error>) -> u16 {
    match result {
        Err(Error::Api { status, .. }) => status,
        Ok(_) => 200,
        Err(e) => panic!("{e}"),
    }
}

fn scoped_broker() -> (postino::serve::Handle, Client, Client) {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: Some("admin-token-0123456789".into()),
            agent_tokens: vec![
                AgentToken::new("alice", "alice-token-0123456789"),
                AgentToken::new("bob", "bob-token-0123456789"),
            ],
        };
    });
    let alice = Client::new(&broker.url, Some("alice-token-0123456789".into())).unwrap();
    let bob = Client::new(&broker.url, Some("bob-token-0123456789".into())).unwrap();
    (broker, alice, bob)
}

#[test]
fn agent_tokens_reach_only_their_namespace_and_shared_keys() {
    let (_broker, alice, bob) = scoped_broker();
    assert_eq!(
        db(&bob, json!(["SET", "bob:secret", "hunter2"])).unwrap(),
        "OK"
    );
    assert_eq!(
        db(&alice, json!(["SET", "alice:plan", "v1"])).unwrap(),
        "OK"
    );
    assert_eq!(
        db(&alice, json!(["SET", "shared:config", "eu"])).unwrap(),
        "OK"
    );
    assert_eq!(db(&bob, json!(["GET", "shared:config"])).unwrap(), "eu");
    for command in [
        json!(["GET", "bob:secret"]),
        json!(["SET", "bob:secret", "pwned"]),
        json!(["DEL", "bob:secret"]),
        json!(["RENAME", "bob:secret", "alice:loot"]),
        json!(["MGET", "alice:plan", "bob:secret"]),
        json!([
            "XREADGROUP",
            "GROUP",
            "STREAMS",
            "c",
            "STREAMS",
            "bob:secret",
            ">"
        ]),
        json!(["SET", "unscoped", "x"]),
    ] {
        match db(&alice, command.clone()) {
            Err(Error::Api {
                status, message, ..
            }) => {
                assert_eq!(status, 403, "{command}: {message}");
                assert!(message.starts_with("NOPERM"), "{message}");
            }
            other => panic!("{command}: {other:?}"),
        }
    }
    assert_eq!(db(&bob, json!(["GET", "bob:secret"])).unwrap(), "hunter2");
    assert_eq!(
        db(&alice, json!(["KEYS", "*"])).unwrap(),
        json!(["alice:plan", "shared:config"])
    );
    assert_eq!(db(&alice, json!(["DBSIZE"])).unwrap(), 2);
    let page = db(&alice, json!(["SCAN", "0", "COUNT", 10])).unwrap();
    assert_eq!(page, json!(["0", ["alice:plan", "shared:config"]]));
    assert!(
        db(&alice, json!(["INFO"])).unwrap().get("agents").is_none(),
        "INFO tells an agent nothing about others"
    );
}

#[test]
fn a_batch_with_one_forbidden_command_runs_nothing() {
    let (_broker, alice, bob) = scoped_broker();
    let batch = json!([["SET", "alice:a", "1"], ["SET", "bob:b", "1"]]);
    let refused = alice.request(
        "POST",
        "/api/db/multi",
        Some(&batch),
        Duration::from_secs(5),
    );
    match refused {
        Err(Error::Api {
            status, message, ..
        }) => {
            assert_eq!(status, 403);
            assert!(message.contains("command 2 of the batch"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(db(&alice, json!(["EXISTS", "alice:a"])).unwrap(), 0);
    assert_eq!(db(&bob, json!(["EXISTS", "bob:b"])).unwrap(), 0);
}

#[test]
fn channels_and_key_notifications_follow_the_scope() {
    let (broker, alice, bob) = scoped_broker();
    // Alice listens to everything she is allowed to: other agents' traffic
    // and other agents' key changes never reach her.
    let mut events = alice.events("/api/subscribe?pattern=*").unwrap();
    let subscribed: Value = serde_json::from_str(&events.next().unwrap().data).unwrap();
    assert_eq!(subscribed["type"], "subscribed");
    assert_eq!(db(&bob, json!(["PUBLISH", "bob:private", "x"])).unwrap(), 0);
    db(&bob, json!(["SET", "bob:secret", "x"])).unwrap();
    db(&alice, json!(["SET", "alice:mine", "x"])).unwrap();
    let first: Value = serde_json::from_str(&events.next().unwrap().data).unwrap();
    assert!(
        first["channel"].as_str().unwrap().ends_with("alice:mine")
            || first["message"] == "alice:mine",
        "the first delivery is alice's own change: {first}"
    );
    assert_eq!(
        status(
            alice
                .events("/api/subscribe?channel=bob:private")
                .map(|_| Value::Null)
        ),
        403
    );
    assert_eq!(
        status(
            alice
                .events("/api/subscribe?channel=__keyspace__:bob:secret")
                .map(|_| Value::Null)
        ),
        403
    );
    assert_eq!(
        status(db(&alice, json!(["PUBLISH", "bob:private", "spoof"]))),
        403
    );
    let admin = Client::new(&broker.url, Some("admin-token-0123456789".into())).unwrap();
    assert_eq!(
        status(db(
            &admin,
            json!(["PUBLISH", "__keyspace__:alice:mine", "del"])
        )),
        400,
        "nobody can forge key-change notifications"
    );
    assert_eq!(
        status(alice.events("/api/tail/bob:secret").map(|_| Value::Null)),
        403
    );
    // Bob does not learn that alice listens with a pattern; she does.
    assert_eq!(db(&bob, json!(["PUBSUB", "NUMPAT"])).unwrap(), 0);
    assert_eq!(db(&alice, json!(["PUBSUB", "NUMPAT"])).unwrap(), 1);
    assert_eq!(db(&admin, json!(["PUBSUB", "NUMPAT"])).unwrap(), 1);
    // A subcommand cannot carry a key past the scope check.
    db(&bob, json!(["XADD", "bob:stream", "*", "f", "v"])).unwrap();
    assert_eq!(
        status(db(&alice, json!(["XINFO", "HELP", "bob:stream"]))),
        403
    );
    assert_eq!(
        db(&alice, json!(["XINFO", "HELP"])).unwrap()[0],
        "XINFO STREAM <key>"
    );
}

#[test]
fn a_scoped_token_listens_with_a_bounded_number_of_patterns() {
    let (_broker, alice, _bob) = scoped_broker();
    let patterns: Vec<String> = (0..64).map(|i| format!("pattern=alice:{i}*")).collect();
    let mut held = alice
        .events(&format!("/api/subscribe?{}", patterns.join("&")))
        .unwrap();
    held.next().unwrap();
    assert_eq!(
        status(
            alice
                .events("/api/subscribe?pattern=alice:x*")
                .map(|_| Value::Null)
        ),
        429
    );
    assert_eq!(
        status(
            alice
                .events("/api/subscribe?channel=alice:x")
                .map(|_| Value::Null)
        ),
        200,
        "channels need no pattern budget"
    );
}

#[test]
fn scopes_can_grant_read_only_and_wider_reach() {
    let broker = start(|cfg| {
        cfg.auth = Auth {
            api_token: None,
            agent_tokens: postino::config::parse_agent_tokens(
                r#"{"ci": {"token": "ci-token-0123456789", "keys": ["build:*"], "read": ["config:*"]},
                    "ops": {"token": "ops-token-0123456789", "keys": ["*"]}}"#,
            )
            .unwrap(),
        };
    });
    let ci = Client::new(&broker.url, Some("ci-token-0123456789".into())).unwrap();
    let ops = Client::new(&broker.url, Some("ops-token-0123456789".into())).unwrap();
    assert_eq!(
        db(&ops, json!(["SET", "config:region", "eu"])).unwrap(),
        "OK"
    );
    assert_eq!(db(&ci, json!(["GET", "config:region"])).unwrap(), "eu");
    assert_eq!(status(db(&ci, json!(["SET", "config:region", "us"]))), 403);
    assert_eq!(db(&ci, json!(["SET", "build:7", "green"])).unwrap(), "OK");
    assert_eq!(
        status(db(&ops, json!(["FLUSHALL"]))),
        403,
        "a token that reaches every key still cannot flush"
    );
}

#[test]
fn a_request_that_fails_partway_through_a_change_leaves_the_server_read_only() {
    let broker = start(|_| {});
    let api = client(&broker);
    assert_eq!(db(&api, json!(["SET", "plan", "v1"])).unwrap(), "OK");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        broker.broker.run(|_, _| panic!("partway through a change"))
    }));
    assert!(failed.is_err());
    let refused = db(&api, json!(["SET", "plan", "v2"]));
    assert_eq!(status(refused), 503);
    assert_eq!(db(&api, json!(["GET", "plan"])).unwrap(), "v1");
    let health = api.request("GET", "/api/health", None, Duration::from_secs(20));
    assert_eq!(status(health), 503);
}
