# Pub/sub and change notifications

Channels deliver a message to whoever is listening at that moment and keep nothing. Use them for live signals: a build finished, a run was cancelled, a key just changed. For anything a late reader must still see, use a [stream](streams.md).

## Publish

```sh [CLI]
postino publish builds "4.2 is green"
```

```sh [curl]
curl -s localhost:2790/api/db -d '["PUBLISH", "builds", "4.2 is green"]'
```

```json
{"result":1}
```

The result is how many subscribers received the message. `0` means nobody was listening, and the message is gone.

## Subscribe

Subscribers hold a Server-Sent Events connection. Name channels exactly, or match them with glob patterns:

```sh [CLI]
postino subscribe builds deploys --pattern 'alerts.*'
```

```sh [curl]
curl -N 'http://127.0.0.1:2790/api/subscribe?channel=builds&channel=deploys&pattern=alerts.*'
```

```text
data: {"channels":["builds","deploys"],"patterns":["alerts.*"],"type":"subscribed"}

data: {"channel":"builds","message":"4.2 is green","type":"message"}

data: {"channel":"alerts.disk","message":"90% full","pattern":"alerts.*","type":"pmessage"}
```

A connection can name up to 64 channels and patterns, and an agent token listens with at most 64 patterns across all its connections. A subscriber that cannot keep up is disconnected rather than allowed to hold messages back for everyone else: one with 256 messages, or 4 MiB, waiting to be sent. Reconnect and carry on.

`PUBSUB CHANNELS`, `PUBSUB NUMSUB builds`, and `PUBSUB NUMPAT` report who is listening.

## Key change notifications

Every change to the keyspace is published on two channels:

- `__keyspace__:<key>`, with the event name as the message: `set`, `del`, `expire`, `expired`, `hset`, `lpush`, `xadd`, and so on.
- `__keyevent__:<event>`, with the key name as the message.

Notifications cost nothing until someone subscribes to them.

```sh
postino subscribe --pattern '__keyspace__:task:*'
```

```text
__keyspace__:task:42  hset
__keyspace__:task:42  expired
```

An agent waiting for the plan to change subscribes to `__keyspace__:plan` and reads the key when the notification arrives. To find out when a lock is released, subscribe to `__keyspace__:lock:deploy` and wait for `del` or `expired`.
