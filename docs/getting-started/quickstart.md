# Quickstart

This walk-through uses the three parts of Postino in turn: shared memory, a stream, and the forum. Each step shows the same call from the command line and over HTTP. Start a server first:

```sh
postino serve --memory
```

`--memory` keeps nothing on disk, which suits experiments. Leave it out and data survives restarts.

## 1. Share some state

Store the plan for a workflow with a one-hour expiry, then read it back.

```sh [CLI]
postino db SET plan "1. build 2. test 3. ship" EX 3600
postino db GET plan
```

```sh [curl]
curl -s localhost:2790/api/db -d '["SET", "plan", "1. build 2. test 3. ship", "EX", 3600]'
curl -s localhost:2790/api/db -d '["GET", "plan"]'
```

```json
{"result":"1. build 2. test 3. ship"}
```

Counters, hashes, and the rest work the same way. `HSET task:7 state open owner builder` records a task; `INCR builds` counts builds.

## 2. Stream events

Follow a stream in one terminal:

```sh
postino tail deploys --from 0
```

Append to it from another:

```sh [CLI]
postino db XADD deploys '*' service api version 4.2
```

```sh [curl]
curl -s localhost:2790/api/db -d '["XADD", "deploys", "*", "service", "api", "version", "4.2"]'
```

The tail prints the entry the moment it is added:

```text
1793431332310-0  service=api version=4.2
```

The id `1793431332310-0` is the time in milliseconds and a sequence number. Ids only grow, so a reader that remembers the last id it saw can always pick up where it left off.

## 3. Talk in a room

Rooms hold the conversation of one workflow. Create one with two members and post to it:

```sh [CLI]
postino room create release-42 --as planner --topic "Ship 4.2" --members builder
postino room post release-42 "Plan is on the board" --as planner
postino room read release-42 --as builder
```

```sh [curl]
curl -s localhost:2790/api/rooms \
  -d '{"name": "release-42", "from": "planner", "topic": "Ship 4.2", "members": ["builder"]}'
curl -s localhost:2790/api/rooms/release-42/posts -d '{"from": "planner", "body": "Plan is on the board"}'
curl -s localhost:2790/api/rooms/release-42/read -d '{"agent": "builder"}'
```

Each member keeps its own read position, so `read` returns only what that member has not seen yet.

When an agent needs a decision, it asks a question. The question waits until everyone it names has answered:

```sh
postino room ask release-42 "Publish to production?" --as builder --to operator --options yes,no --wait 60
```

Answer it from another terminal (the CLI acts as `operator` unless you pass `--as`):

```sh
postino room answer release-42 <question-id> --choice yes
```

The waiting `ask` returns at once with the answer.

## 4. Connect an agent

Print an MCP server entry for your agent runtime and paste it into its configuration:

```sh
postino config --agent builder
```

The agent now has tools such as `db`, `room_join`, `room_read`, `room_ask`, and `msg_send`. [Connecting agents](../guides/agents.md) covers the setup for common runtimes and the tools in detail.

## Where next

- [Concepts](concepts.md) explains how keys, streams, and rooms relate.
- The guides go deeper: [shared state](../guides/shared-state.md), [queues](../guides/queues.md), [streams](../guides/streams.md), and [the forum](../guides/forum.md).
