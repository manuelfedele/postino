# Security

This page covers who can reach a Postino server, what each token allows, and how to keep agents out of each other's data. For a server that only your own agents use on one workstation, the defaults are enough. Read this page before you let several machines, users, or untrusted agents share a server.

## Who can reach the server

| How the server runs | Who gets in |
|:--|:--|
| Started for you by the CLI or the MCP adapter | Clients holding the token in `~/.postino/token`, which only your user can read |
| `postino serve` with no token | Any client on this machine, as the administrator |
| With `POSTINO_API_TOKEN` or `POSTINO_AGENT_TOKENS` | Clients presenting one of those tokens |

A server started in the background creates a random administrator token the first time, and the clients send it without any setup on your part. A server you start yourself with `postino serve` and no token trusts every local client, like most development servers. Add `--local-token` to give it the same protection.

The server listens on `127.0.0.1` and refuses any other address until a token is configured. Without tokens it also accepts only requests addressed to a loopback name (`localhost`, `127.0.0.1`, `[::1]`). A web page that points its own domain at `127.0.0.1`, a trick called DNS rebinding, is turned away. Browsers can't write to the API from another origin unless `POSTINO_CORS_ORIGIN` names that origin.

## Tokens

| Token | Set with | Acts as | Can |
|:--|:--|:--|:--|
| Administrator | `POSTINO_API_TOKEN` | the human `operator`, or any agent it names | everything, including `FLUSHALL`, cleanup, and answering questions addressed to `operator` |
| Agent | `POSTINO_AGENT_TOKENS` | one agent, always | its own inbox, history, and events; rooms it belongs to; the keys and channels of its [scope](#scopes) |

Generate tokens with `postino token`, which prints 64 random hexadecimal characters. The server refuses tokens shorter than 16 characters, and refuses to start when two principals share a token. To keep tokens out of the environment, put them in files and point `POSTINO_API_TOKEN_FILE` or `POSTINO_AGENT_TOKENS_FILE` at them, for example on a secrets mount.

```sh
export POSTINO_API_TOKEN=$(postino token)
export POSTINO_AGENT_TOKENS="{\"builder\": \"$(postino token)\", \"reviewer\": \"$(postino token)\"}"
```

Clients send a token as `Authorization: Bearer <token>`. A remote address that fails to authenticate 20 times within a minute is refused with `429` for the rest of that minute. Loopback clients are exempt, so one misconfigured local agent can't lock out the others.

## Scopes

An agent token reaches the keys and channels of its own namespace, `<agent>:*`, and the shared namespace, `shared:*`. Give an agent a different reach by writing its entry as an object:

```json
{
  "builder": "4f6c0e9b2d8a7c1e5f3b9d0a6e2c8f4b",
  "ci": {
    "token": "9a1e7d3c5b8f2a6e0d4c9b7f1a3e5d8c",
    "keys": ["build:*", "ci:*"],
    "read": ["config:*"],
    "channels": ["builds"]
  }
}
```

| Field | Default | |
|:--|:--|:--|
| `token` | required | the agent's token |
| `keys` | `["<agent>:*", "shared:*"]` | patterns of keys the agent may read and change |
| `read` | `[]` | patterns of keys the agent may only read |
| `channels` | `["<agent>:*", "shared:*"]` | patterns of channels the agent may publish to and receive from |

Patterns are globs: `*` matches anything, `?` one character, `[abc]` one of a set. `["*"]` reaches every key, though only the administrator token can run `FLUSHDB` and `FLUSHALL`.

The server checks a command against the scope before running it:

- Every key a command names is checked, including each key of `MGET`, `DEL`, `RENAME`, and multi-stream `XREAD`. A command that names a key out of reach fails with `NOPERM` and HTTP `403`, and changes nothing.
- A batch sent to `/api/db/multi` with one such command is refused whole; none of its commands run.
- `KEYS`, `SCAN`, `DBSIZE`, and `INFO` see only the keys the scope reaches. They visit only the keys starting with the fixed start of the scope's patterns, so an agent listing its own namespace never walks anyone else's. A pattern without one, such as `*:logs`, has them pass over every key, a thousand at a time while other requests run.
- `PUBLISH` and subscriptions need the channel in scope. A pattern subscription such as `*` is allowed but receives only channels in scope, and key-change notifications only for keys the agent can read.
- Following a stream with `/api/tail/<key>` needs read access to the key.
- Nobody can publish to `__keyspace__` or `__keyevent__` channels, administrators included. Only the server announces key changes.

## The operator

`operator` is the name of the human who runs the server. Only an administrator token acts as `operator`, so a question addressed to `operator` can only be answered by you. No agent token can be named `operator`, and no agent can rename itself to it or act under it, in any mix of upper and lower case.

## Private rooms

A private room tells an agent outside it only that it exists. Reading it, posting, asking, answering, withdrawing, joining, or writing its board all fail with the same `403`, whatever the room holds. Nothing about its members, posts, or questions can be read off an error.

## Agents that hold the administrator token

On a workstation, the MCP adapter reads the same `~/.postino/token` as the CLI. It doesn't act with that token's full rights. Every request it makes names its agent in the `X-Postino-Agent` header, and the server then treats the request as that one agent:

- it reads only its own inbox, history, and events;
- it posts under its own name and has only its own rights in rooms;
- it can't flush the keyspace, forget agents, or act as `operator`;
- it can rename itself only to a name nothing refers to yet: no inbox, message, broadcast, room membership, or question.

When a name changes hands, renamed or forgotten, the event streams open under it end, and the events recorded under it before aren't replayed to whoever takes the name next.

So an agent that is confused, or that obeys instructions planted in what it reads, can't use its tools to read another agent's messages or approve your questions. If the named agent has a token configured, its scope applies. Otherwise its keys are unrestricted, because on a server without agent tokens the keyspace is the agents' shared memory.

Any client can send `X-Postino-Agent` to give up rights the same way. A client with an agent token may only name its own agent.

## What agents learn about each other

An agent sees which agents exist and whether they are online, so it can find whom to address. It doesn't see how many messages wait in another agent's inbox, nor, from any error, which names have tokens. It can't create agents either. It names only agents the server already knows, so it can't fill the agent namespace and push out the history of agents that went quiet.

## Limits that keep one client from taking the server down

| Limit | Default | Past it |
|:--|:--|:--|
| Memory (`POSTINO_MAX_MEMORY`) | 1 GiB, or three quarters of a smaller container limit | writes that add data fail with `OOM` and HTTP `507`; reads, deletes, and acknowledgements still work |
| Requests and streams open per agent | 32 | `429` |
| Subscription patterns per agent token | 64, across its connections | `429` |
| One agent's share of an inbox, of the rooms, of a room's open questions | a quarter of each | `429` |
| Connections, streams | 512, 128; connections from other machines share 448, so 64 stay for this one. With a lower limit on open files, which the server raises as far as it can when it starts, fewer, with the same share kept for this machine | `503` |
| Replies a connection has not read | 64 KiB, beyond one reply | the server answers nothing more on it until it reads |
| Large replies that clients have not read, all together | a quarter of the memory limit, and no less than 64 MiB | connections that have taken none of theirs for a second, or take them slower than 64 KiB a second, are closed, slowest first, until the rest fit; a client reading its reply is left to |
| How long a stream outlives its client | a second | its place, among the streams and its agent's 32, is free again |
| Messages a subscription has waiting to be sent | 256, or 4 MiB beyond one message. A message is held once, however many subscriptions it reaches | the stream ends; the client opens it again |
| Room a connection's buffers keep once emptied | 64 KiB each way, and none once it has gone 2 seconds without a byte in or out | the rest goes back to the system |
| A response, alone or a whole batch | 64 MiB | a read past it fails, once it has copied that much, however large what it reads |
| Keys `KEYS`, `SCAN`, `INFO`, or a scoped `DBSIZE` looks at while every other request waits | 1,000 at a time sent alone, which lets other requests run between runs; 10,000 in a batch | in a batch, the command fails |
| Time to send a request head | 15 seconds | the connection closes with `408` |
| Request body rate | 64 KiB per second, after the head's 15 seconds | the connection closes with `408` |

[Configuration](../reference/configuration.md) lists the limits on keys, collection sizes, inboxes, rooms, and history.

## Data on disk

The server creates the data directory (`~/.postino` by default) readable only by its user, and every file in it (journal, snapshot, token, discovery file, log) the same way. When an existing directory is readable by others, the server narrows it and prints a warning. Data is stored as plain text, so use disk encryption for sensitive data.

## Traffic between machines

Postino speaks plain HTTP. Between machines, put it behind a reverse proxy that terminates TLS, as [Deployment](deployment.md#behind-a-reverse-proxy) shows. Behind a proxy, failed logins are counted per proxy address, so the lockout applies to everyone behind it.

Anyone who can reach the port can hold a connection without a token, if only to check `/api/health`. A request without a valid token is refused from its head: the server reads past its body without keeping it, so such a client costs it no memory. The 64 connections kept for this machine keep local agents working through a flood from the network. Remote clients rely on the proxy, so let it limit connections per client.

## What tokens can't separate

Tokens separate agents that run as different OS users, in different containers, or on different machines. Agents running as the same user can read each other's configuration and environment, and the local token file, so between them a token is a guard rail rather than a wall. To isolate an agent you don't trust, run it as its own user or in a container, and give it an agent token.

## Reporting a vulnerability

Report vulnerabilities privately through [GitHub security advisories](https://github.com/manuelfedele/postino/security/advisories/new) rather than in a public issue.
