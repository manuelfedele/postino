# Command reference

Every command is an array: the name (case does not matter), then its arguments. Send it to `POST /api/db`, the `db` MCP tool, or `postino db`. Arguments may be strings or numbers. Keys and values are text: a JSON string holds any Unicode text but not arbitrary bytes, so encode binary data first, as base64 say.

Square brackets mark optional parts, `|` separates alternatives, and `...` means the previous part repeats.

## Replies

| Reply | JSON | CLI |
|:--|:--|:--|
| status | `"OK"` | `OK` |
| text | `"value"` | `value` |
| integer | `3` | `(integer) 3` |
| nothing | `null` | `(nil)` |
| list | `["a", "b"]` | numbered lines |
| field-value map | `{"field": "value"}` | `field: "value"` lines |
| error | HTTP 400, `{"ok": false, "error": "..."}` | `(error) ...` |

Errors begin with a code word: `ERR` for invalid usage, `WRONGTYPE` when the key holds another type, `NOGROUP` when a consumer group is missing, `BUSYGROUP` when it already exists, `OOM` when a [limit](configuration.md#keyspace) or the memory limit is reached, `IOERR` (HTTP `503`) when the journal can't be written, `READONLY` (HTTP `503`) when a request failed partway through a change and the server takes no more until it restarts, and `NOPERM` when a key or channel is outside the token's [scope](../guides/security.md#scopes) or the command needs the administrator token.

A command that fails changes nothing. A move whose destination can't take the element leaves it in the source, and an `MSET` that would pass the key limit writes none of its keys. The one exception is a write made as the journal starts failing, on a full disk say: its change is made, and reaches the disk once the disk takes it. What it returned still comes back, so the item an `LPOP` removed isn't lost: in the `result` of an `IOERR` response, or as an ordinary reply when the server was journaling it together with other changes. The writes after it are refused until the disk takes them.

A response carries at most 64 MiB. A read that would return more, alone or with the rest of its [batch](http.md#run-commands-atomically), fails instead, so read big collections in parts with a range, `COUNT`, or `SCAN`. Commands that remove what they return, such as `LPOP` with a count or `XREADGROUP` with `>`, always return it.

## Keys

| Command | Reply |
|:--|:--|
| `DEL key [key ...]`, `UNLINK key [key ...]` | number of keys removed |
| `EXISTS key [key ...]` | number of the keys that exist |
| `TYPE key` | `string`, `list`, `hash`, `set`, `zset`, `stream`, or `none` |
| `EXPIRE key seconds [NX \| XX \| GT \| LT]` | 1 if the expiry was set, 0 otherwise |
| `PEXPIRE key milliseconds [NX \| XX \| GT \| LT]` | as `EXPIRE` |
| `EXPIREAT key unix-seconds [...]`, `PEXPIREAT key unix-ms [...]` | as `EXPIRE` |
| `TTL key`, `PTTL key` | seconds or milliseconds left; -1 without expiry; -2 if missing |
| `EXPIRETIME key`, `PEXPIRETIME key` | the expiry as a Unix time; -1 or -2 as above |
| `PERSIST key` | 1 if an expiry was removed |
| `RENAME key newkey` | `OK`; replaces `newkey` |
| `RENAMENX key newkey` | 1, or 0 if `newkey` exists |
| `KEYS pattern` | matching key names, in order; a pattern with a fixed start such as `task:*` visits only those keys. Sent alone it looks at a thousand keys at a time, letting other requests run in between, so a key added or removed meanwhile may or may not be listed; in a [batch](http.md#run-commands-atomically) it runs whole, and fails past 10,000 keys |
| `SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]` | `[next-cursor, [keys...]]`; start with `0`, pass back the cursor you get, and stop when it is `0` again. Each call looks at `COUNT` keys (10 by default, 1,000 at most) |
| `DBSIZE` | number of keys; one that has just expired may count until the server removes it, usually within a second |
| `FLUSHDB`, `FLUSHALL` | `OK`; removes every key (administrator only) |

`NX` sets an expiry only when there is none, `XX` only when there is one, `GT` only if the new one is later, `LT` only if earlier. An expiry in the past deletes the key. An expiry that doesn't fit in 64 bits of milliseconds fails with `ERR invalid expire time`.

Patterns use `*`, `?`, `[abc]`, `[^a]`, `[a-z]`, and `\` to escape; a `[` that is never closed matches nothing. A pattern is at most 256 bytes. Matching takes time proportional to the key, whatever the pattern.

## Strings and counters

| Command | Reply |
|:--|:--|
| `GET key` | the value, or `null` |
| `SET key value [NX \| XX \| IFEQ current] [GET] [EX s \| PX ms \| EXAT ts \| PXAT ms-ts \| KEEPTTL]` | `OK`, or `null` if a condition failed; with `GET`, the old value |
| `SETNX key value` | 1 if set, 0 if the key exists |
| `SETEX key seconds value`, `PSETEX key ms value` | `OK` |
| `GETSET key value` | the old value |
| `GETDEL key` | the value, then deletes the key |
| `GETEX key [EX s \| PX ms \| EXAT ts \| PXAT ms-ts \| PERSIST]` | the value, changing its expiry |
| `MGET key [key ...]` | values, with `null` for missing keys |
| `MSET key value [key value ...]` | `OK` |
| `MSETNX key value [key value ...]` | 1 if all were set, 0 if any key existed (then none is set) |
| `APPEND key value` | the new length |
| `STRLEN key` | the length in bytes |
| `INCR key`, `DECR key` | the new value |
| `INCRBY key n`, `DECRBY key n` | the new value |
| `INCRBYFLOAT key n` | the new value as text |
| `DELIFEQ key value` | 1 if the key held exactly `value` and was deleted, otherwise 0 |

`IFEQ` and `DELIFEQ` make compare-and-set and safe lock release a single command. `SET` without `KEEPTTL` clears an existing expiry. Values are limited to 16 MiB. Counters are 64-bit signed integers.

## Hashes

| Command | Reply |
|:--|:--|
| `HSET key field value [field value ...]` | number of new fields |
| `HMSET key field value [field value ...]` | `OK` |
| `HSETNX key field value` | 1 if set, 0 if the field exists |
| `HGET key field` | the value, or `null` |
| `HMGET key field [field ...]` | values, with `null` for missing fields |
| `HGETALL key` | a map of every field |
| `HDEL key field [field ...]` | number of fields removed |
| `HEXISTS key field` | 1 or 0 |
| `HLEN key` | number of fields |
| `HKEYS key`, `HVALS key` | field names or values, sorted by field |
| `HINCRBY key field n` | the new value |
| `HINCRBYFLOAT key field n` | the new value as text |

## Lists

| Command | Reply |
|:--|:--|
| `LPUSH key value [value ...]`, `RPUSH key value [value ...]` | the new length |
| `LPUSHX key value [...]`, `RPUSHX key value [...]` | the new length, or 0 if the list does not exist |
| `LPOP key [count]`, `RPOP key [count]` | the value (or a list with `count`), `null` if empty |
| `LLEN key` | the length |
| `LRANGE key start stop` | the elements in the range; negative indexes count from the end |
| `LINDEX key index` | the element, or `null` |
| `LSET key index value` | `OK` |
| `LREM key count value` | number removed: the first `count` matches, the last ones if negative, all if 0 |
| `LTRIM key start stop` | `OK`; keeps only the range |
| `LINSERT key BEFORE \| AFTER pivot value` | the new length, or -1 if `pivot` is missing |
| `LMOVE source destination LEFT \| RIGHT LEFT \| RIGHT` | the moved element, or `null` |
| `RPOPLPUSH source destination` | as `LMOVE source destination RIGHT LEFT` |
| `BLPOP key [key ...] timeout`, `BRPOP key [key ...] timeout` | `[key, value]` from the first non-empty list, or `null` after `timeout` seconds |
| `BLMOVE source destination LEFT \| RIGHT LEFT \| RIGHT timeout` | the moved element, or `null` |

## Sets

| Command | Reply |
|:--|:--|
| `SADD key member [member ...]` | number of new members |
| `SREM key member [member ...]` | number removed |
| `SMEMBERS key` | every member, sorted |
| `SISMEMBER key member` | 1 or 0 |
| `SMISMEMBER key member [member ...]` | 1 or 0 for each |
| `SCARD key` | number of members |
| `SPOP key [count]` | removes and returns random members |
| `SRANDMEMBER key [count]` | random members without removing them; a negative count allows repeats, up to `POSTINO_MAX_ELEMENTS` of them |
| `SINTER key [key ...]`, `SUNION key [key ...]`, `SDIFF key [key ...]` | the intersection, union, or difference |
| `SMOVE source destination member` | 1 if moved, or if the member is already in a set moved onto itself |

## Sorted sets

| Command | Reply |
|:--|:--|
| `ZADD key [NX \| XX] [GT \| LT] [CH] [INCR] score member [score member ...]` | number added (with `CH`, added or changed); with `INCR`, the new score |
| `ZREM key member [member ...]` | number removed |
| `ZSCORE key member` | the score, or `null` |
| `ZMSCORE key member [member ...]` | the scores |
| `ZINCRBY key increment member` | the new score |
| `ZCARD key` | number of members |
| `ZCOUNT key min max` | members with a score in the range |
| `ZRANK key member`, `ZREVRANK key member` | the position from the lowest or the highest score |
| `ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES]` | members by position, or by score with `BYSCORE` |
| `ZREVRANGE key start stop [WITHSCORES]` | members by position, highest first |
| `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | members by score |
| `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` | members by score, highest first |
| `ZPOPMIN key [count]`, `ZPOPMAX key [count]` | `[member, score, ...]` |
| `BZPOPMIN key [key ...] timeout`, `BZPOPMAX key [key ...] timeout` | `[key, member, score]`, or `null` after `timeout` seconds |
| `ZREMRANGEBYSCORE key min max` | number removed |
| `ZREMRANGEBYRANK key start stop` | number removed |

Members with equal scores are ordered by name. Score bounds accept `-inf`, `+inf`, and `(` before a number to exclude it. With `REV`, `ZRANGE ... BYSCORE` takes the maximum first.

Score ranges visit only the members in range. Ranks are counted from the lowest score, so `ZRANK`, `ZREVRANK`, `ZRANGE` by index, and `ZREMRANGEBYRANK` take time in proportion to the rank they reach.

## Streams

| Command | Reply |
|:--|:--|
| `XADD key [NOMKSTREAM] [MAXLEN \| MINID [= \| ~] threshold] * \| id field value [field value ...]` | the new entry's id |
| `XLEN key` | number of entries |
| `XRANGE key start end [COUNT n]` | `[[id, [field, value, ...]], ...]`, oldest first |
| `XREVRANGE key end start [COUNT n]` | the same, newest first |
| `XDEL key id [id ...]` | number removed |
| `XTRIM key MAXLEN \| MINID [= \| ~] threshold` | number removed |
| `XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]` | `[[key, entries], ...]` after each id, or `null` |
| `XGROUP CREATE key group id \| $ [MKSTREAM]` | `OK` |
| `XGROUP SETID key group id \| $` | `OK` |
| `XGROUP DESTROY key group` | 1 if removed |
| `XGROUP CREATECONSUMER key group consumer` | 1 if created |
| `XGROUP DELCONSUMER key group consumer` | number of pending entries it held |
| `XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]` | as `XREAD` |
| `XACK key group id [id ...]` | number acknowledged |
| `XPENDING key group` | `[count, first-id, last-id, [[consumer, count], ...]]` |
| `XPENDING key group [IDLE ms] start end count [consumer]` | `[[id, consumer, idle-ms, deliveries], ...]` |
| `XCLAIM key group consumer min-idle-ms id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID]` | the claimed entries (or ids) |
| `XAUTOCLAIM key group consumer min-idle-ms start [COUNT n] [JUSTID]` | `[next-start, claimed, deleted-ids]` |
| `XINFO STREAM key` | a map: length, last id, entries added, groups, first and last entry |
| `XINFO GROUPS key` | a map per group: consumers, pending, last delivered id, lag |
| `XINFO CONSUMERS key group` | a map per consumer: pending, idle, inactive |
| `XINFO HELP` | the `XINFO` subcommands |

Ids are `<milliseconds>-<sequence>`. In `XADD`, `*` generates the next id and `<ms>-*` generates the sequence. In ranges, `-` and `+` are the smallest and largest ids, an id without a sequence covers that whole millisecond, and `(` excludes the id. In `XREAD`, `$` means the newest entry at the time of the call.

In `XREADGROUP`, the id `>` asks for entries no member of the group has received. They become pending for this consumer until acknowledged, unless `NOACK` is given. Any other id replays this consumer's pending entries after it, which is how a restarted worker recovers what it had started. Groups and their consumers are created and tracked per stream; reading creates the consumer. Each stream may be named once per command.

`MAXLEN` and `MINID` trim exactly; `~` is accepted and behaves like `=`. `XADD` checks the stream's length after the trim against `POSTINO_MAX_ELEMENTS`, so a `MAXLEN` above the limit doesn't get around it. The largest id is `18446744073709551615-18446744073709551615`; once a stream holds it, `XADD` with `*` fails.

A consumer's idle time, reported by `XINFO CONSUMERS`, isn't journaled when a read delivers nothing new. After a restart it counts from the consumer's last change.

## Channels

| Command | Reply |
|:--|:--|
| `PUBLISH channel message` | number of subscribers that received it |
| `PUBSUB CHANNELS [pattern]` | channels with at least one subscriber |
| `PUBSUB NUMSUB [channel ...]` | `[channel, count, ...]` |
| `PUBSUB NUMPAT` | number of pattern subscriptions; for a scoped token, those of clients with the same scope |

Channel names and patterns are at most 256 bytes. Subscribing is a long-lived connection: `GET /api/subscribe` (see [HTTP API](http.md#subscribe-to-channels)) or `postino subscribe`.

## Server

| Command | Reply |
|:--|:--|
| `PING [message]` | `PONG`, or the message |
| `ECHO message` | the message |
| `TIME` | `[unix-seconds, microseconds]` |
| `INFO` | a map: version, keys, keys with an expiry, streams, approximate bytes, subscriptions, agents, rooms |
| `COMMAND` | the names of every command |
| `COMMAND COUNT` | how many there are |

## Blocking

`BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX`, `XREAD BLOCK`, and `XREADGROUP BLOCK` (with `>`) wait when there is nothing to return. Their timeout is in seconds, except `BLOCK`, which takes milliseconds; a fraction of a second is rounded up to the next millisecond. A timeout of `0` waits as long as the transport allows, which over HTTP and MCP is 60 seconds. Inside an [atomic batch](http.md#run-commands-atomically) they never wait.
