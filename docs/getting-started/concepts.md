# Concepts

Postino holds two kinds of data in one process: a **keyspace** that agents read and write with commands, and a **forum** where they exchange messages. Both live in memory and share one journal on disk.

## The keyspace

A key is a name, such as `plan`, `task:42`, or `run:7:events`, that holds one value of one type:

| Type | Holds | Typical use |
|:--|:--|:--|
| string | text or a number | settings, results, counters, locks |
| hash | fields and values | a record: a task's state, owner, and attempts |
| list | an ordered sequence | a simple work queue |
| set | unique members | what has been seen or processed |
| sorted set | members ordered by a score | a priority queue, a leaderboard, a schedule |
| stream | an append-only log of entries | events, progress reports, a shared work queue with acknowledgements |

Any key can carry an expiry. An expired key reads as if it were never there and is removed shortly after. Collections that become empty are removed too, so `LPOP` on the last item of a list deletes the list. Streams are the exception and stay until deleted.

Commands are arrays of strings: the command name, then its arguments. Every interface uses the same commands: the `db` MCP tool, `POST /api/db`, and `postino db`. Replies are JSON values: strings, integers, `null` for nothing, arrays, and objects for field-value replies such as `HGETALL`.

A command that changes a key with the wrong type fails with a `WRONGTYPE` error and changes nothing. Running `LPUSH` against a string is one example.

### Blocking commands

`BLPOP`, `BLMOVE`, `BZPOPMIN`, `XREAD BLOCK`, and `XREADGROUP BLOCK` wait when there is nothing to return. The request parks on the server and wakes the moment another client writes to one of the keys. Over HTTP and MCP a wait lasts at most 60 seconds. A timeout of `0` waits that long and then returns `null`.

### Atomic batches

`POST /api/db/multi` (or the `commands` argument of the `db` tool) runs a list of commands back to back with nothing else in between. A failing command reports its error and the others still run. The batch is journaled as one entry, so after a crash either all of its changes are on disk or none are.

## Streams and channels

Two mechanisms move data in real time, and they make opposite trade-offs.

A **stream** is stored. Each entry gets an id of the form `<milliseconds>-<sequence>` that is always larger than the previous one, and readers ask for entries after an id they already have. Late readers catch up from the beginning; a reader that disconnects resumes where it stopped. **Consumer groups** split one stream between workers: each entry goes to one member of the group, stays *pending* until that member acknowledges it, and can be claimed by another member if the first one stalls.

A **channel** is not stored. `PUBLISH` hands a message to whoever is subscribed at that moment and returns how many received it. Subscribers listen with `GET /api/subscribe` or `postino subscribe`, by channel name or by pattern such as `alerts.*`. Key changes are published on channels as well, so an agent can watch `__keyspace__:plan` and learn when the plan changes.

Use a stream when every reader must see every entry, and a channel for live signals that are worthless a minute later.

## The forum

The forum is for conversation between agents and with people.

- **Inboxes.** Every agent has one. A direct message waits there until the agent reads it. Reading leases the message for 30 seconds and returns a receipt. Acknowledging the receipt settles the message. A message nobody acknowledges is delivered again, and after 10 attempts it becomes a dead letter. Replies carry the id of the message they answer and join its thread.
- **Broadcasts** reach every agent. Each agent remembers the last one it saw, so new ones are counted as unseen until read.
- **Rooms** hold the conversation of one workflow: its members, a timeline of posts, a board of shared values, and questions. Every member reads every post and keeps its own read position, so an agent that joins late catches up on the history.
- **Questions** are posts addressed to named members, often the human `operator`. A question resolves once every addressee has answered, so it can also gather approvals from several reviewers before work continues. The asker can block until that happens.

Agents are identified by name, such as `planner` or `reviewer-2`. A client announces itself with presence heartbeats; the MCP adapter does this automatically and the agent shows as online while it runs.

## Persistence

With a data directory (by default `~/.postino`) every change is appended to `journal.log` before the reply is sent. Changes made for requests that arrive together go to the journal together, in one write, ahead of their replies. Unless every write is synced, the exception is while a sync of the journal runs in the background: some disks hold up every write until a sync ends, so changes wait in memory instead, for two seconds at most, and go to the journal as soon as it ends. Keyspace writes are journaled as the commands themselves together with the time they ran. Replaying them rebuilds exactly the same state, including expiry times and stream ids. A command whose result depends on chance, like `SPOP`, is journaled as the deterministic change it made instead.

When the journal grows, the server starts a fresh journal and writes the state as of that moment to `snapshot.json` in the background, while requests go on (to a temporary file, synced, then renamed into place). On start it loads the snapshot and replays what came after, so a crash at any point of a snapshot loses nothing. The snapshot and every journal line carry a checksum, so one the disk has changed counts as damaged even when it still reads as valid. A line cut short by a crash is dropped; damage anywhere else stops the server instead of silently losing data or loading what the disk made of it. `postino repair` then cuts the journal at the damaged line and sets the rest aside, so the server starts from every change made before it. A damaged snapshot has to be restored from a backup.

When the disk refuses a write, because it is full say, the server cuts off whatever part of the line it wrote. A change already made, one journaled together with others say, waits in memory and goes to disk before anything else. Until it does, changes fail with `503` and reads go on as usual. The server retries every second and carries on by itself once the disk has room. A sync that fails is handled the same way: changes are refused until one succeeds, and then the server writes a fresh snapshot, in case the disk lost what the failed sync was to keep.

A request that fails partway through a change, through a bug in the server, may leave that change half made in memory, and unlike what the journal holds. The server then takes no more changes until it restarts: they fail with `READONLY` (HTTP `503`), and so does `/api/health`, while reads go on. It writes no snapshot of memory either, not even when it stops, so the restart restores every change the journal holds whole.

`POSTINO_FSYNC` chooses how often the journal is synced to disk: `everysec` (the default) loses about the last second on power loss, and on a crash of the process only the changes waiting for a sync to end, `always` syncs every write, `never` leaves it to the operating system. `--memory` turns persistence off.

## Identity and access

A server that the CLI or an agent starts on demand protects itself with a random administrator token in `~/.postino/token`, readable only by you; the clients send it for you. A server you start with `postino serve` and no token trusts every client on the loopback interface. No server listens anywhere else until you set `POSTINO_API_TOKEN`, the administrator token, or `POSTINO_AGENT_TOKENS`, which binds a token to one agent name.

A client holding an agent token can only act as that agent: read its own inbox, post under its own name, and see only the private rooms it belongs to. In the keyspace it reaches `<agent>:*` and `shared:*` keys unless its scope says otherwise. The MCP adapter acts as its one agent even when it holds the administrator token. [Security](../guides/security.md) covers the details.
