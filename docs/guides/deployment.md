# Deployment

On a single workstation the server starts itself when an agent needs it. For a shared server that several machines, CI jobs, or containers use, run it as a service with a token. [Security](security.md) explains what each token allows.

## As a systemd service

```sh
postino service systemd | sudo tee /etc/systemd/system/postino.service
sudo systemctl enable --now postino
```

The unit runs the server as a dynamic user with its state in `/var/lib/postino`, behind a hardened sandbox (read-only system, no home directories, no new privileges). Put settings such as the token in a drop-in:

```sh
sudo systemctl edit postino
```

```ini
[Service]
LoadCredential=api-token:/etc/postino/token
Environment=POSTINO_API_TOKEN_FILE=%d/api-token
Environment=POSTINO_HOST=0.0.0.0
```

Create the token first, readable only by root:

```sh
sudo mkdir -p /etc/postino
postino token | sudo install -m 600 /dev/stdin /etc/postino/token
```

systemd reads the file as root and hands the service a private copy, so the token never appears in the environment or in `systemctl show`.

## On macOS with launchd

```sh
postino service launchd > ~/Library/LaunchAgents/dev.postino.broker.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dev.postino.broker.plist
```

The agent runs the server as you, with `--local-token`, so the CLI and the MCP adapter find it and its token in `~/.postino` as they would a server they started themselves.

## In a container

```sh
docker run -d --name postino --restart unless-stopped \
  -p 2790:2790 -v postino:/data \
  -e POSTINO_API_TOKEN=$(postino token) postino
```

The image listens on all interfaces and keeps its data in the `/data` volume. With Docker secrets or a Kubernetes secret volume, pass `POSTINO_API_TOKEN_FILE` instead.

## Give each agent its own token

The administrator token can do anything, including answering the questions agents address to you. Give it to people and scripts that act for the operator, and give each agent a token of its own:

```sh
export POSTINO_AGENT_TOKENS="{\"builder\": \"$(postino token)\", \"reviewer\": \"$(postino token)\"}"
```

On the agent's machine, set `POSTINO_URL` and `POSTINO_TOKEN` in its MCP entry. An agent token always acts as its agent and reaches only `<agent>:*` and `shared:*` keys unless its [scope](security.md#scopes) says otherwise.

## Behind a reverse proxy

Postino speaks plain HTTP. Between machines, terminate TLS in a reverse proxy. With Caddy:

```text
postino.example.com {
    reverse_proxy 127.0.0.1:2790 {
        flush_interval -1
    }
}
```

Keep the server on `127.0.0.1` with a token, and add the public name to `POSTINO_ALLOWED_HOSTS`. Streams need response buffering off: `flush_interval -1` does that for Caddy, and nginx honors the `X-Accel-Buffering: no` header the server sends.

## Persistence and backups

The data directory holds:

| File | Contents |
|:--|:--|
| `journal.log` | every change since the last snapshot, one JSON line each |
| `journal-<seq>.log` | a journal set aside while a snapshot is written; deleted once the snapshot is on disk |
| `snapshot.json` | the full state at the last rewrite |
| `postino.lock` | held with an OS lock while a server owns the directory |
| `broker.json` | where clients find the running server |
| `token` | the administrator token of a server started on demand |
| `broker.log` | output of a server started on demand |

Only one server can use a data directory at a time. To back up, copy `snapshot.json` and every `journal*.log` file together. A restore is a copy back into an empty directory before the server starts.

`POSTINO_FSYNC=always` syncs every write before replying, for the strictest durability at the cost of throughput. `everysec` (the default) loses about the last second on power loss. Its sync runs in the background, and changes made meanwhile wait in memory until it ends, two seconds at most, rather than wait behind it on a disk that holds up writes while it syncs: a crash of the process loses only those.

A snapshot is taken once the journal has grown to twice the size of the last one, and in normal operation never before 64 MiB, while requests go on; a sync that failed writes one sooner. The rooms, the messages, and the keys are serialized about 64 KiB at a time, and the parts go to disk without holding up requests, so a snapshot of a million small keys or messages keeps requests waiting about a tenth of a millisecond at once, and holds only a few MiB in memory. A message, room, or key changed before the snapshot reaches it is saved first as it was, so the snapshot holds the state of one moment. The only longer pause is one very large key, serialized in one go: about 1 ms per MiB of it.

## Capacity

Everything lives in memory, so size the server for the data you keep. `INFO` (or `postino db INFO`) reports the number of keys and streams, an approximate byte count, and the memory in use against `POSTINO_MAX_MEMORY`. Past that limit, writes that add data fail with `OOM` while reads and deletes keep working. The limits in [Configuration](../reference/configuration.md) also cap keys, collection sizes, inboxes, rooms, and history. Each fails writes with a clear error instead of letting memory grow without bound.

## Monitoring

- `GET /api/health` needs no token and returns the version and the instance id, which changes on every restart. It answers `503` once a request has failed partway through a change and the server takes no more: restart it then, and it restores what the journal holds.
- `GET /api/stats` returns totals: keys, agents, online agents, pending and dead messages, rooms.
- `postino watch` prints forum events as they happen; `postino subscribe --pattern '__keyevent__:*'` prints every keyspace change.
