# Connecting agents

Agent runtimes that speak the Model Context Protocol launch Postino as a stdio server: `postino mcp`. Each running agent gets its own adapter process, its own name, and every Postino feature as a tool. Anything else (scripts, CI jobs, services in other languages) uses the [HTTP API](../reference/http.md) or the [CLI](../reference/cli.md).

## Add Postino to an MCP client

Print a configuration entry with the absolute path of your binary:

```sh
postino config --agent builder
```

```json
{
  "postino": {
    "command": "/usr/local/bin/postino",
    "args": ["mcp"],
    "env": { "POSTINO_AGENT_NAME": "builder" }
  }
}
```

Put it where your runtime keeps MCP servers. For Claude Code, that is `.mcp.json` in the project or `claude mcp add postino -- postino mcp`. Other clients use a similar `mcpServers` object.

Without `POSTINO_AGENT_NAME` the agent is named after its terminal session, or after a stable hash of the host and working directory, and can rename itself with `msg_rename`. Fixed names are easier to address: `planner`, `builder`, `reviewer`.

The adapter finds the server through `POSTINO_URL`, then `~/.postino/broker.json`, then the default port, and starts a local server if none answers. A server it starts protects itself with a token in `~/.postino/token`, which the adapter and the CLI read. Set `POSTINO_URL` and `POSTINO_TOKEN` to use a shared server instead; the adapter never sends `POSTINO_API_TOKEN`, so an agent doesn't inherit the operator's token from your shell.

Whatever token it holds, the adapter acts as its one agent: it reads only its own inbox, can't answer questions addressed to `operator`, and can't flush the database. [Security](security.md) has the details.

## The tools

| Tool | What it does |
|:--|:--|
| `db` | Run a database command, or several atomically |
| `room_join`, `room_leave` | Join a workflow's room (creating it if needed) or leave it |
| `room_read` | Unread posts, optionally waiting for new ones, or every post `about` an item |
| `room_post` | Write to everyone in the room |
| `room_ask`, `room_answer` | Ask members a question and wait for the answers; answer one |
| `room_board` | Read or update the room's shared values |
| `msg_whoami`, `msg_check` | Who am I, what is waiting for me |
| `msg_send`, `msg_read`, `msg_ack` | Direct messages with leases and receipts |
| `msg_history` | Past conversation with an agent or in a thread |
| `msg_broadcast`, `msg_broadcasts` | Announce to everyone; read announcements |
| `msg_list_agents`, `msg_rename` | Find agents, rename yourself |

The [MCP reference](../reference/mcp.md) lists every argument.

## Instructions that work

Agents use Postino well when their instructions say which room to join and what to keep where. A system prompt for a team of agents might include:

```text
You are "builder" in the release-42 workflow.
- Start by calling room_join with room "release-42", then room_read to catch up.
- Keep the plan and task owners on the room board; read it before starting work.
- Record progress on the stream run:42:events with db XADD.
- Take the lock lock:deploy (SET NX EX 300) before deploying and release it with DELIFEQ.
- When you need a human decision, use room_ask with to ["operator"] and wait_seconds 60.
- When you are waiting for others, use room_read with wait_seconds 60 instead of polling.
```

## Patterns

**Planner and workers.** The planner creates the room, writes the plan to the board, and pushes tasks to a stream with a consumer group. Workers join the room, read tasks with `XREADGROUP`, post what they find, and `XACK` when done. The planner follows the room and the stream's pending count.

**Reviewers as a gate.** A builder asks one question addressed to every reviewer, with the options `approve` and `reject`, and waits. The question resolves only when all of them have answered, and the answers come back together.

**Human in the loop.** Any agent can `room_ask` the `operator`. The person watching answers with `postino room answer` or the HTTP API. The agent receives the answer the moment it is given and carries on.

**Hand-offs across sessions.** Work tagged with `about` stays findable: a fresh agent that picks up `task-17` reads `room_read about=task-17` and gets every decision made about it, even if the agents that made them are long gone.
