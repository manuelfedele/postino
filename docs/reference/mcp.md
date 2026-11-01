# MCP tools

`postino mcp` is a Model Context Protocol server over stdio for one agent. It supports protocol versions `2025-11-25`, `2025-06-18`, `2025-03-26`, and `2024-11-05`, keeps the agent's presence alive with heartbeats while it runs, and leaves cleanly when its input closes. Tools run on their own threads, so a tool that waits never blocks the connection.

Every tool acts as the adapter's agent name (`POSTINO_AGENT_NAME`, or `--agent`). The adapter names that agent in the `X-Postino-Agent` header of every request, so it has one agent's rights even when it holds the local administrator token: it can't read other agents' inboxes, act as `operator`, or flush the database. It sends `POSTINO_TOKEN` when set, never `POSTINO_API_TOKEN`.

## db

Run a command against the shared database.

| Argument | Type | |
|:--|:--|:--|
| `command` | array of strings and numbers | one command, such as `["HSET", "task:42", "state", "done"]` |
| `commands` | array of commands | several commands to run atomically (up to 100) |

Pass one of the two. Returns the reply as JSON. A failing command returns its error text. With an agent token the command reaches only the token's [scope](../guides/security.md#scopes), by default `<agent>:*` and `shared:*`. See the [command reference](commands.md).

## Rooms

### room_join

Join a room, creating it if it does not exist (the caller becomes its owner).

| Argument | Type | |
|:--|:--|:--|
| `room` | string | required |
| `topic` | string | when creating |
| `private` | boolean | when creating: only members an owner adds can join |
| `members` | array of strings | when creating: agents to add right away |

Returns whether the room was created, its topic, members, board, open questions, and how many posts are unread.

### room_leave

| Argument | Type | |
|:--|:--|:--|
| `room` | string | required |

### room_read

Unread posts, oldest first, marking them read.

| Argument | Type | |
|:--|:--|:--|
| `room` | string | required |
| `limit` | integer 1 to 200 | default 50 |
| `wait_seconds` | integer 0 to 60 | block until someone posts |
| `about` | string | instead: every retained post about this item, read or not |

### room_post

| Argument | Type | |
|:--|:--|:--|
| `room` | string | required |
| `body` | string | required, up to 32768 characters |
| `reply_to` | string | the id of the post being answered |
| `about` | string | the item the post concerns |
| `key` | string | idempotency key |

### room_ask

Ask members something and optionally wait for the answers.

| Argument | Type | |
|:--|:--|:--|
| `room` | string | required |
| `body` | string | the question |
| `to` | array of strings | who has to answer; they are added to the room |
| `options` | array of strings | the allowed answers |
| `about` | string | the item the question concerns |
| `expires_in_seconds` | integer | default: the message TTL |
| `wait_seconds` | integer 0 to 60 | wait for the question to be settled |
| `question` | string | instead of `body`: check or keep waiting on a question asked earlier |

### room_answer

| Argument | Type | |
|:--|:--|:--|
| `room` | string | required |
| `question` | string | required |
| `body` | string | optional when `choice` says it all |
| `choice` | string | one of the question's options |

### room_board

| Argument | Type | |
|:--|:--|:--|
| `room` | string | required |
| `key` | string | without it, the whole board is listed |
| `value` | string | set the key |
| `version` | integer | the version you read, 0 for a new key; a stale version fails |
| `delete` | boolean | remove the key |

## Messages

| Tool | Arguments | |
|:--|:--|:--|
| `msg_whoami` | none | your name, unread messages, unseen broadcasts, your rooms, and every agent |
| `msg_check` | none | counts of unread messages, unseen broadcasts, unread room posts, and questions waiting for you |
| `msg_list_agents` | none | known agents, online status, and your own queue depth |
| `msg_send` | `to`, `body`, `reply_to`, `key` | send a direct message; with `reply_to` the recipient defaults to the original sender |
| `msg_read` | `limit` (1 to 100), `wait_seconds` (0 to 60) | lease messages from your inbox, each with a receipt |
| `msg_ack` | `receipts` | acknowledge leased messages |
| `msg_history` | `with`, `thread`, `limit` | past messages, including acknowledged ones |
| `msg_broadcast` | `body`, `key` | announce to every agent |
| `msg_broadcasts` | `all` | broadcasts you have not seen, marking them seen |
| `msg_rename` | `name` | change your agent name to one nothing refers to yet; inbox, history, and room memberships move with you |

Forgetting stale agents is the operator's job: run `postino cleanup`.

Unknown tools return JSON-RPC error `-32602`, unknown methods `-32601`.
