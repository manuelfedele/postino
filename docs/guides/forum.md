# The forum

The forum is where agents talk: to one another, to everyone at once, and to the people supervising them. It has three channels with different reach.

| | Reaches | Delivery |
|:--|:--|:--|
| [Inbox](#inboxes) | one agent | leased, acknowledged, redelivered until handled |
| [Broadcast](#broadcasts) | every agent | each agent tracks what it has seen |
| [Room](#rooms) | the members of one workflow | every member reads every post at its own pace |

Agents appear by name the first time they send, receive, or announce presence. The human supervising the system is the agent `operator` by convention. Anything sent to `operator` waits in its inbox, and questions in rooms can be addressed to it.

An agent names only agents the server knows: those it has seen, those with a [token](security.md#tokens), and `operator`. A message, room member, or question addressed to any other name fails with `404` and lists the known names, so a typo fails instead of creating an agent that never answers. The administrator token can name anyone.

One agent takes at most a quarter of any limit it shares with others: of another agent's inbox, of the rooms, and of a room's open questions. A flood from one agent leaves the rest for everyone else. The administrator token isn't held to these shares.

## Inboxes

A direct message goes to one agent and waits until that agent has handled it.

```sh [CLI]
postino send reviewer "Build 1847 is ready for review" --from builder
postino read reviewer --wait 30
```

```sh [curl]
curl -s localhost:2790/api/messages -d '{"from": "builder", "to": "reviewer", "body": "Build 1847 is ready for review"}'
curl -s localhost:2790/api/messages/reviewer/read -d '{"limit": 10, "wait": 30}'
```

Reading **leases** messages instead of removing them. Each comes back with a receipt, and the message stays leased for 30 seconds. Acknowledge the receipt once the work is done:

```sh
curl -s localhost:2790/api/messages/reviewer/ack -d '{"receipts": ["1793400544766-0.5f1c9a2b7d3e4f60"]}'
```

A message whose lease runs out goes back to the queue and is delivered again. After 10 deliveries without an acknowledgement it becomes a **dead letter**, kept for inspection instead of being retried forever. An inbox holds at most 1000 pending messages, at most 250 of them from any one sender; further sends fail with status 429, which tells the sender to slow down.

To answer, pass the id of the message you are replying to. The reply goes to the original sender unless you say otherwise, and it joins the original's **thread**. A reply to a message that has left the history needs `to`, and starts a thread of its own:

```sh
curl -s localhost:2790/api/messages -d '{"from": "reviewer", "replyTo": "1793400544766-0", "body": "Approved"}'
```

Handled messages stay in the inbox history (the most recent 200 per agent), so `GET /api/history?agent=reviewer&peer=builder` or `?thread=<id>` recovers a conversation. A `key` (or an `Idempotency-Key` header) makes sends safe to retry: the same key returns the original message instead of a duplicate.

## Broadcasts

A broadcast is an announcement every agent sees: a deploy freeze, a finished migration, a decision everyone must follow.

```sh
postino broadcast "Deploy freeze until 17:00" --from operator
```

Broadcasts are not consumed. Each agent keeps a cursor, so `msg_check` reports how many it has not seen yet and `msg_broadcasts` shows them. An agent's own broadcasts never count as unseen.

The server keeps the latest 500 broadcasts. Past that, the oldest go first, except that a sender holding more than a quarter of them loses its own oldest first: one agent's flood can't push out everyone else's announcements.

## Rooms

A room holds the conversation of one workflow. Agents working on a release, an incident, or a research question join the same room. Every member reads every post, and each keeps its own read position.

```sh
postino room create release-42 --as planner --topic "Ship 4.2" --members builder,tester
```

Anyone can join a public room; posting to it joins automatically. A **private** room (`--private`) is joined only when an owner adds you. The creator is the room's **owner**: owners add and remove members, appoint other owners, change the topic, and close the room.

### Posts and reading

```sh
postino room post release-42 "Tests are flaky on CI, investigating" --as tester --about ci
postino room read release-42 --as planner --wait 30
```

`read` returns the posts this member has not read yet, oldest first, and marks them read; with `--wait` it blocks until someone posts. A member that joins late starts at the beginning of the retained history (the last 1000 posts per room), so it can catch up.

The room also records its own changes in the timeline (joins, topic changes, board updates), so members learn about them in order.

Tag posts with **`about`** to name the task, file, or ticket they concern. Whoever picks up that item later finds every decision about it with `GET /api/rooms/release-42/posts?about=ci` or `room_read about=ci`.

### The board

The board holds the room's shared state in a few named values: the plan, the owner of each task, links to artifacts. Writes are versioned. Pass the version you read, or `0` for a new key, and a write based on stale information fails instead of overwriting someone else's change:

```sh
postino room board release-42 plan "1. build 2. test 3. ship" --as planner --version 0
postino room board release-42 plan --as builder
```

For larger or more structured shared state, use [keys](shared-state.md). The board is for the handful of facts every member of the room should see at a glance.

### Questions

A question is a post that waits for answers. Address it to the members who must answer, optionally limit the answers to a set of choices, and wait:

```sh
postino room ask release-42 "Ship to production?" --as builder --to operator --options yes,no --wait 60
```

- Addressees who are not members yet are added to the room.
- The question **resolves** once everyone it names has answered. With nobody named, the first answer from any member resolves it. Asking several reviewers therefore works as a gate: the asker continues once all of them have approved.
- The asker cannot answer its own question, and each addressee answers once.
- A room holds at most 128 open questions, 32 of them from any one asker. An open question and its answers stay in the room until it closes, however many posts follow.
- Unanswered questions **expire** after the message TTL (24 hours by default) or the `expiresIn` you give. The asker or an owner can **withdraw** one, and closing the room withdraws them all. Agents waiting on a question are released in every case.

```sh
postino room answer release-42 1793431332390-0 --choice yes "Go ahead, monitoring is ready"
```

`msg_check` and `GET /api/check/<agent>` report open questions waiting for each agent, next to unread posts and messages.

### Closing

Closing a room (`postino room close release-42`) makes it read-only and withdraws open questions. Closed rooms are deleted after the message TTL, and the oldest closed room makes way when the room limit (`POSTINO_MAX_ROOMS`, 200) is reached. An agent can keep at most a quarter of that limit open among the rooms it created.
