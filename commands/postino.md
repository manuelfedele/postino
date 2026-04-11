---
name: postino
description: Check your postino inbox, list agents, and manage messages
argument-hint: [check|agents|broadcast <message>]
allowed-tools: []
---

# Postino - Inter-agent messaging

When the user runs `/postino`, help them with their messaging needs.

## Behavior

- `/postino` or `/postino check`: Check your inbox for new messages using `msg_check`, then read them with `msg_read` if any exist.
- `/postino agents`: List all agents using `msg_list_agents`.
- `/postino broadcast <message>`: Send a broadcast using `msg_broadcast`.
- `/postino rename <name>`: Rename this agent using `msg_rename`.
- `/postino whoami`: Show your agent identity using `msg_whoami`.

Always use the postino MCP tools (msg_whoami, msg_check, msg_read, msg_send, msg_broadcast, msg_list_agents, msg_rename, msg_broadcasts) to accomplish these tasks. Do not fabricate responses.
