# Streams

A stream is an append-only log stored under a key. Agents use streams to report progress, record what happened in a run, feed results to the next stage of a pipeline, and share work between several workers. Unlike a channel, a stream keeps its entries, so a reader that joins late or reconnects loses nothing.

## Entries and ids

Each entry is a set of fields and values, and has an id:

```sh
postino db XADD run:7:events '*' kind started step build
```

```text
1793431332310-0
```

`*` asks the server for the next id: the current time in milliseconds and a sequence number for entries in the same millisecond. Ids always increase, even if the clock steps back, so they double as a cursor.

Fields are free-form. A common convention for agent workflows:

```sh
postino db XADD run:7:events '*' kind progress step test percent 40
postino db XADD run:7:events '*' kind result step test outcome passed
```

## Reading a range

```sh
postino db XRANGE run:7:events - +              # everything, oldest first
postino db XRANGE run:7:events - + COUNT 10     # the first ten
postino db XREVRANGE run:7:events + - COUNT 1   # the newest entry
postino db XRANGE run:7:events 1793431332310 +  # from a point in time
```

`-` and `+` stand for the smallest and largest ids. An id without a sequence part, such as `1793431332310`, means the start of that millisecond. Prefix an id with `(` to exclude it.

## Waiting for new entries

`XREAD` returns entries after the ids you give it. `$` means "after the newest entry right now", and `BLOCK` waits up to that many milliseconds for one to arrive:

```sh
postino db XREAD BLOCK 30000 STREAMS run:7:events '$'
```

To follow a stream, remember the last id you received and pass it to the next call. That way no entry is missed between calls:

```sh
postino db XREAD COUNT 100 BLOCK 30000 STREAMS run:7:events 1793431332310-1
```

One call can wait on several streams: `XREAD BLOCK 30000 STREAMS a b $ $`.

## Following a stream live

For dashboards, scripts, and anything that prefers push to polling, `GET /api/tail/<key>` sends each new entry as a Server-Sent Event as soon as it is added:

```sh [CLI]
postino tail run:7:events --from 0
```

```sh [curl]
curl -N 'http://127.0.0.1:2790/api/tail/run:7:events?from=0'
```

```text
id: 1793431332310-0
data: {"fields":{"kind":"started","step":"build"},"id":"1793431332310-0"}
```

`from` is `$` (only new entries, the default), `0` (everything), or an id to start after. Each event's id is the entry id, so a browser `EventSource` that reconnects sends `Last-Event-ID` and resumes exactly where it stopped.

## Sharing work: consumer groups

A consumer group lets several agents split one stream so each entry is handled once, with acknowledgements and recovery when a worker stalls. [Queues and work distribution](queues.md#streams-with-consumer-groups) walks through it step by step. The commands involved are:

| Command | Purpose |
|:--|:--|
| `XGROUP CREATE key group $ MKSTREAM` | create a group starting at the end (or `0` for the beginning) |
| `XREADGROUP GROUP group me COUNT n BLOCK ms STREAMS key >` | receive entries no member has received yet |
| `XACK key group id...` | mark entries done |
| `XPENDING key group` | how many entries are pending, and for whom |
| `XAUTOCLAIM key group me min-idle-ms 0` | take over entries another member left idle |
| `XINFO GROUPS key` | members, pending counts, and how far behind each group is |

## Keeping streams small

Streams grow until you trim them. Cap the length as you write:

```sh
postino db XADD run:7:events MAXLEN 1000 '*' kind progress percent 50
```

or trim afterwards with `XTRIM run:7:events MAXLEN 1000`, or by age with `XTRIM run:7:events MINID <id>`. `XDEL` removes single entries. Deleting the key removes the whole stream and its groups. Give per-run streams an expiry with `EXPIRE run:7:events 86400` so they disappear on their own.
