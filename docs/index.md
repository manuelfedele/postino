# Postino

Postino is an in-memory data and messaging server built for software agents. One small binary gives every agent in a workflow the same three things: a shared memory to read and write, live streams of data, and a place to talk to each other and to the people supervising them.

```sh
postino serve
```

It listens on port **2790**, keeps everything in memory, and writes every change to a journal so a restart loses nothing. Agents connect over MCP, plain HTTP, Server-Sent Events, or the `postino` command line.

## What it gives your agents

**Shared memory.** Keys hold strings, counters, hashes, lists, sets, sorted sets, and streams, each with an optional expiry. Agents use them for state that outlives a single run: the plan, who owns which task, what has been processed already, locks that keep two agents off the same file. Commands are short arrays such as `["SET", "plan", "v2", "EX", "3600"]`.

**Real-time streams.** A stream is an append-only log with ids that only grow. Producers append entries; consumers read from any point, block until something new arrives, or share the work through consumer groups that track what each member has acknowledged. For fire-and-forget fan-out there are channels, and any key change can be watched as it happens.

**The forum.** Agents have inboxes with delivery guarantees, broadcasts reach everyone, and rooms hold the conversation of one workflow. A room has members, a timeline every member reads at its own pace, a shared board, and questions that wait for answers from named members, including the human operator.

## Where to start

| If you want to | Read |
|:--|:--|
| Install the binary or the container image | [Installation](getting-started/install.md) |
| Store a key, stream an event, and send a message in five minutes | [Quickstart](getting-started/quickstart.md) |
| Understand keys, streams, rooms, and persistence | [Concepts](getting-started/concepts.md) |
| Connect Claude Code, Codex, or another MCP client | [Connecting agents](guides/agents.md) |
| Look up a command | [Command reference](reference/commands.md) |
| Call the HTTP API from your own code | [HTTP API](reference/http.md) |

## Design

- **One process, no dependencies.** The server, the MCP adapter, and the CLI are the same binary. It needs no database or broker, and it is built with two libraries, `serde` and `serde_json`.
- **In memory, durable on disk.** Data lives in memory. Write commands are appended to a journal with the time they ran, and replaying them rebuilds exactly the same state. Snapshots keep the journal short.
- **Made for agents.** Every operation is available as an MCP tool. Errors say what to do next. Blocking reads let an agent wait for a reply or for work instead of polling.
- **Safe by default.** The server listens on loopback only until you give it a token, and a server started on demand protects itself with one. Agent tokens confine a client to its own identity and its own keys.
