# Shared state

Agents in one workflow constantly need the same facts: the current plan, which task each agent owns, how many attempts a step has had, whether a file has been reviewed. Keep those facts in keys instead of repeating them in messages. Every agent reads the latest value, and nobody has to ask.

The examples use the CLI. The same command arrays work with the [`db` MCP tool](../reference/mcp.md#db) and [`POST /api/db`](../reference/http.md#run-a-command).

## Name keys by what they describe

Keys are free-form text. A colon-separated hierarchy keeps them easy to list and clean up:

```text
plan                      the plan for this workflow
task:42                   a hash describing task 42
run:7:attempts            a counter
run:7:events              a stream of what happened in run 7
reviewed:files            a set of files already reviewed
```

`KEYS run:7:*` lists every key of run 7, and `SCAN` walks large keyspaces in pages. Both take glob patterns: `*` matches anything, `?` one character, `[abc]` one of a set.

## Strings, numbers, and expiry

```sh
postino db SET plan "1. build 2. test 3. ship"
postino db GET plan
postino db SET build:status green EX 600        # expires in ten minutes
postino db TTL build:status                     # seconds left
postino db INCR run:7:attempts                  # 1, then 2, ...
postino db INCRBYFLOAT budget:spent 0.42
```

A value is text up to 16 MiB. Store JSON in a string when an agent needs a structured document. To update single fields without rewriting the whole value, use a hash.

`EX`, `PX`, `EXAT`, and `PXAT` set an expiry in seconds, milliseconds, or as a Unix timestamp. `EXPIRE` adds one to an existing key and `PERSIST` removes it. A plain `SET` replaces the value and clears any expiry; add `KEEPTTL` to keep it.

## Records in hashes

```sh
postino db HSET task:42 title "Fix the login test" state open owner builder
postino db HSET task:42 state done
postino db HINCRBY task:42 attempts 1
postino db HGETALL task:42
```

```text
attempts: "1"
owner: "builder"
state: "done"
title: "Fix the login test"
```

Over HTTP and MCP, `HGETALL` returns a JSON object.

## Update without losing a concurrent change

Two agents that read a value, change it, and write it back can overwrite each other. Make the write conditional on the value you read:

```sh
postino db GET plan                             # "v1"
postino db SET plan "v2" IFEQ "v1"              # OK, or (nil) if someone changed it first
```

`IFEQ` writes only if the key still holds exactly the given string. On `(nil)`, read again and retry. `SET ... NX` writes only if the key is missing, and `XX` only if it exists.

For several keys at once, send the commands as one [atomic batch](../reference/http.md#run-commands-atomically). Nothing else runs between them.

## Locks

A lock is a key that exists while someone holds it. Take it with `NX` and an expiry, so a holder that crashes does not block everyone forever. The value is a token only the holder knows:

```sh
postino db SET lock:deploy builder-7f3a NX EX 30    # OK means you hold it; (nil) means someone else does
# ... do the work, renewing with EXPIRE lock:deploy 30 if it takes longer ...
postino db DELIFEQ lock:deploy builder-7f3a          # 1: released; 0: it expired and someone else holds it now
```

`DELIFEQ` deletes the key only if it still holds your token, so you never release a lock another agent has since taken.

## Remember what has been done

Sets make deduplication a single call:

```sh
postino db SADD reviewed:files src/api.rs           # 1 the first time, 0 after that
postino db SISMEMBER reviewed:files src/api.rs
postino db SMEMBERS reviewed:files
```

An agent that receives work twice, because a message was delivered again or a stream entry was claimed by another worker, checks the set before acting.

## Watch for changes

Every change to a key is published on the channel `__keyspace__:<key>` with the event name (`set`, `hset`, `del`, `expired`, ...). Subscribe to learn when the plan changes instead of polling it:

```sh
postino subscribe __keyspace__:plan
```

See [Pub/sub](pubsub.md) for patterns and the HTTP interface.

## Limits

A server holds at most `POSTINO_MAX_KEYS` keys (one million by default) and a collection holds at most `POSTINO_MAX_ELEMENTS` elements. Writes beyond that fail with an `OOM` error and change nothing. Existing keys can still be updated or deleted. Expire what you no longer need, and trim streams with `MAXLEN`.
