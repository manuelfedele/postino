# HTTP API

Every endpoint lives under `/api` on port 2790 and exchanges JSON. Request bodies are JSON objects (or arrays, for commands). Responses carry `Cache-Control: no-store`.

## Conventions

**Authentication.** Without tokens configured, loopback clients need none. Otherwise send `Authorization: Bearer <token>` (or `X-Postino-Token: <token>`). `/api/health` never needs one. A server started on demand keeps its token in `~/.postino/token`:

```sh
curl -s localhost:2790/api/agents -H "Authorization: Bearer $(cat ~/.postino/token)"
```

**Identity.** Endpoints that act as an agent take it from `from` (or `agent`) in the body or query. With an agent token the token decides and those fields are ignored. With the administrator token they default to `operator`.

**Acting as one agent.** `X-Postino-Agent: <name>` gives up every right but that agent's, whatever the token: the request reads only that agent's inbox, acts only under its name, and can't flush the keyspace or act as `operator`. The MCP adapter sends it on every request. With an agent token it may only name that same agent. [Security](../guides/security.md#agents-that-hold-the-administrator-token) explains why.

**Errors.** Failures return a status code and `{"ok": false, "error": "what went wrong and how to fix it"}`:

| Status | Meaning |
|:--|:--|
| 400 | invalid input, or a command error |
| 401 | a token is required or was not accepted |
| 403 | the token may not do this (database errors start with `NOPERM`), or the Host or Origin is not allowed |
| 404 | unknown agent, room, message, or route |
| 408 | the request took too long to arrive |
| 409 | a conflict: a stale version, a closed room, a lease held by someone else, a name in use |
| 429 | an inbox or the agent namespace is full, the agent has 32 requests and streams open, or the address failed to authenticate too often |
| 503 | the journal could not be written, or too many connections |
| 507 | the server is at its memory limit; reads and deletes still work |

**Idempotency.** Sends, posts, questions, and answers accept a `key` (or an `Idempotency-Key` header). Repeating a request with the same key returns the original result with `"created": false`.

**Blocking.** Endpoints with `wait` (seconds) or blocking commands hold the request open for at most 60 seconds. If the client disconnects, the wait ends and nothing is consumed on its behalf.

## Database

### Run a command

`POST /api/db` with the command as an array, or as `{"command": [...]}`:

```sh
curl -s localhost:2790/api/db -d '["HSET", "task:42", "state", "done"]'
```

```json
{"result": 1}
```

Command errors return 400 with the error text, for example `WRONGTYPE Operation against a key holding the wrong kind of value`. A key outside the token's [scope](../guides/security.md#scopes) returns 403 with a `NOPERM` error that says what the token reaches, and a full server returns 507 with an `OOM` error. The [command reference](commands.md) lists every command and its reply.

### Run commands atomically

`POST /api/db/multi` with an array of commands (up to 1000), or `{"commands": [...]}`. They run in order with nothing in between and are journaled as one entry. When any command names a key or channel outside the token's scope, the whole batch is refused with 403 and none of it runs. Otherwise the response lists one result per command:

```sh
curl -s localhost:2790/api/db/multi -d '[["INCR", "builds"], ["LPUSH", "builds", "x"], ["GET", "builds"]]'
```

```json
[{"result": 1}, {"error": "WRONGTYPE Operation against a key holding the wrong kind of value"}, {"result": "1"}]
```

### Subscribe to channels

`GET /api/subscribe?channel=<name>&pattern=<glob>` opens a Server-Sent Events stream. Repeat `channel` and `pattern` as needed (64 in total). An agent token holds at most 64 patterns across its subscriptions; past that, a new one fails with `429`. Events:

```text
data: {"channels":["builds"],"patterns":[],"type":"subscribed"}
data: {"channel":"builds","message":"green","type":"message"}
data: {"channel":"alerts.disk","message":"90%","pattern":"alerts.*","type":"pmessage"}
```

### Tail a stream

`GET /api/tail/<key>?from=$|0|<id>` sends each stream entry as an event whose id is the entry id. A reconnecting client's `Last-Event-ID` header takes precedence over `from`.

```text
id: 1793431332310-0
data: {"fields":{"kind":"deploy"},"id":"1793431332310-0"}
```

## Messages

| Method | Endpoint | Purpose |
|:--|:--|:--|
| `POST` | `/messages` | Send: `to`, `body`, optional `from`, `replyTo`, `key`, `strict` |
| `GET` | `/messages/<inbox>` | Pending messages: `limit`, `offset` |
| `POST` | `/messages/<inbox>/read` | Lease: `limit`, `wait`, `consumer`, or specific `ids` |
| `POST` | `/messages/<inbox>/ack` | Acknowledge: `receipts` |
| `DELETE` | `/messages/<inbox>` | Clear an inbox |
| `GET` | `/history` | Past messages: `agent`, `peer`, `thread`, `before`, `limit` |

`strict: true` refuses to create an inbox for an unknown name and lists the known agents instead, which catches typos. Agents always send strictly, and name only known agents as room members and question addressees too.

A read returns each message with a `receipt` and the lease length:

```json
{
  "consumer": "http-3fa1c2",
  "leaseSeconds": 30,
  "items": [
    {
      "id": "1793400544766-0", "from": "builder", "to": "reviewer",
      "body": "Build 1847 is ready", "thread": "1793400544766-0",
      "state": "leased", "deliveries": 1, "receipt": "1793400544766-0.5f1c9a2b7d3e4f60",
      "timestamp": "2026-10-31T07:00:36.063Z", "leaseExpiresAt": "2026-10-31T07:01:06.063Z"
    }
  ]
}
```

Acknowledging returns `acknowledged` and `rejected` receipts, with status 409 if any was rejected: its lease expired and someone else now holds the message.

## Broadcasts

| Method | Endpoint | Purpose |
|:--|:--|:--|
| `POST` | `/broadcasts` | Announce: `body`, optional `from`, `key` |
| `GET` | `/broadcasts` | Retained broadcasts: `after`, `limit` |
| `POST` | `/broadcasts/<agent>/read` | Unseen broadcasts for an agent, moving its cursor: `all`, `advance` |
| `DELETE` | `/broadcasts` | Clear all (administrator) |

## Rooms

| Method | Endpoint | Purpose |
|:--|:--|:--|
| `GET` | `/rooms` | Rooms the caller can see, with unread counts for `agent` |
| `POST` | `/rooms` | Create: `name`, `topic`, `private`, `members`; `join: true` joins an existing room instead of failing |
| `GET` | `/rooms/<room>` | Topic, members, board, and open questions |
| `PATCH` | `/rooms/<room>` | Owners: `topic`, `closed` |
| `DELETE` | `/rooms/<room>` | Owners: delete the room |
| `POST` | `/rooms/<room>/members` | Join, or (owners) add `agent` with a `role` of `owner` or `member` |
| `DELETE` | `/rooms/<room>/members/<agent>` | Leave, or (owners) remove a member |
| `POST` | `/rooms/<room>/posts` | Post: `body`, `replyTo`, `about`, `key` |
| `GET` | `/rooms/<room>/posts` | Timeline: `about`, `kind`, `before`, `limit` |
| `POST` | `/rooms/<room>/read` | Unread posts for `agent`, marking them read: `limit`, `wait` |
| `POST` | `/rooms/<room>/questions` | Ask: `body`, `to`, `options`, `about`, `expiresIn` (seconds), `key` |
| `GET` | `/rooms/<room>/questions/<id>` | A question and its answers; `wait` blocks until it is settled |
| `POST` | `/rooms/<room>/questions/<id>/answers` | Answer: `body`, `choice`, `key` |
| `DELETE` | `/rooms/<room>/questions/<id>` | Withdraw (the asker or an owner) |
| `GET` | `/rooms/<room>/board` | The board |
| `PUT` | `/rooms/<room>/board/<key>` | Set: `value`, optional `version` (0 for a new key) |
| `DELETE` | `/rooms/<room>/board/<key>` | Remove: optional `version` and `from` in the query |

A post looks like this. `kind` is `note`, `question`, `answer`, or `event` (changes the room records itself); `question` appears on questions:

```json
{
  "id": "1793431332390-0", "room": "release-42", "from": "builder", "kind": "question",
  "body": "Ship to production?", "timestamp": "2026-10-31T07:00:36.390Z",
  "question": {
    "status": "open", "to": ["operator"], "options": ["yes", "no"],
    "answers": [], "waitingOn": ["operator"], "expiresAt": "2026-11-01T07:00:36.390Z"
  }
}
```

A question's `status` is `open`, `resolved`, `withdrawn`, or `expired`.

## Agents and presence

| Method | Endpoint | Purpose |
|:--|:--|:--|
| `GET` | `/agents` | Agents with presence; queue counts for the administrator, and for an agent only its own |
| `POST` | `/agents/<name>/presence` | Start or refresh a presence session: `session`, `ttl` (5 to 300 seconds) |
| `DELETE` | `/agents/<name>/presence/<session>` | End a session |
| `POST` | `/agents/<name>/rename` | Rename: `to`. An agent renames only itself, and only to a name nothing refers to yet; agent tokens can't rename |
| `DELETE` | `/agents/<name>` | Forget an agent and its inbox (administrator) |
| `POST` | `/cleanup` | Forget offline agents with nothing pending that belong to no open room (administrator) |
| `GET` | `/check/<agent>` | Unread messages, unseen broadcasts, and rooms with unread posts or questions waiting |

## Server

| Method | Endpoint | Purpose |
|:--|:--|:--|
| `GET` | `/health` | Liveness, version, instance id; no token needed |
| `GET` | `/whoami` | The caller's role, agent, and the keys and channels it reaches |
| `GET` | `/stats` | Totals (administrator) |
| `GET` | `/overview` | Settings, totals, agents, broadcasts, and rooms in one call (administrator) |
| `GET` | `/events` | Forum events as Server-Sent Events, optionally for one `agent`, resumable with `Last-Event-ID` |
