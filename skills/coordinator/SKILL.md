---
name: coordinator
description: "Use when the user mentions coordinating agents, delegating tasks, sending messages to other agents, broadcasting announcements, checking an inbox, waiting for another agent, sharing state between agents, workflow rooms, or setting up multi-agent workflows. Activates on keywords: coordinate, delegate, broadcast, send message, check messages, reply, room, shared state, agent communication, multi-tab."
---

# Agent Coordinator

Postino connects you with other agents and the human operator. Its tools cover three things: direct messages and broadcasts (`msg_*`), rooms where the agents of one workflow talk (`room_*`), and a shared database (`db`).

## Tools
- `msg_whoami`: your name, unread messages, unseen broadcasts, your rooms, and every agent. Call it first.
- `msg_check`: count new activity (messages, broadcasts, room posts, questions for you) without consuming it.
- `msg_send` / `msg_read` / `msg_ack`: direct messages. Reading leases a message; acknowledge its receipt once you have acted on it. `reply_to` answers a message and keeps its thread; `wait_seconds` waits for a reply.
- `msg_broadcast` / `msg_broadcasts`: announcements for every agent.
- `msg_history`, `msg_list_agents`, `msg_rename`: look back, find recipients, name yourself.
- `room_join` / `room_leave`: join the room of a workflow, creating it if needed.
- `room_read` / `room_post`: the room's timeline. Each member has its own read position; `wait_seconds` waits for new posts, `about` keeps posts about one item together.
- `room_ask` / `room_answer`: questions addressed to named members (including `operator`); `wait_seconds` waits until everyone named has answered.
- `room_board`: the room's shared key-value board. Pass the `version` you read to update safely.
- `db`: commands on the shared database, such as `["HSET", "task:42", "state", "done"]`, `["SET", "lock:deploy", "me", "NX", "EX", 300]`, or `["XADD", "events", "*", "kind", "deploy"]`. `commands` runs several atomically.

## Workflow
1. Call `msg_whoami`. If your name is generic (`agent-1a2b3c4d`), rename yourself with `msg_rename`.
2. For work shared by several agents, `room_join` the workflow's room, read the board and open questions, and post progress there instead of messaging each agent.
3. Use direct messages for work aimed at one agent: read with `msg_read`, act, then `msg_ack` the receipt. Unacknowledged messages come back after the lease expires, so acknowledge only what you finished.
4. Keep state other agents rely on (plans, ownership, counters, queues) in `db` keys, and take a lock with `SET ... NX EX` before touching something only one agent should change.
5. Use `msg_broadcast` for things every agent should know: deploy freezes, finished migrations, shared decisions.
6. When you need a human decision, `room_ask` with `to: ["operator"]`, or send a message to `operator`.

## Practices
- Keep messages and posts self-contained: say what you need, by when, and where the artifacts are.
- Handle redelivered messages idempotently; a message can arrive twice if a lease expired. Pass a `key` to make retries safe.
- Use `msg_history` and `room_read about=<item>` to recover context instead of asking another agent to repeat itself.
