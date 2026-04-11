---
name: coordinator
description: "Use when the user mentions coordinating agents, delegating tasks, sending messages to other agents, broadcasting announcements, checking inbox, or setting up multi-agent workflows. Activates on keywords: coordinate, delegate, broadcast, send message, check messages, agent communication, multi-tab."
---

# Agent Coordinator

You have access to postino messaging tools for inter-agent communication.

## Available Tools
- msg_whoami: Get your identity and full status overview (call this first)
- msg_check: Quick check for new messages and broadcasts
- msg_send: Send a 1-to-1 message to another agent
- msg_read: Read and consume messages from your inbox
- msg_broadcast: Broadcast an announcement to all agents
- msg_broadcasts: Read broadcasts you haven't seen yet
- msg_list_agents: Discover all agents and their online status
- msg_rename: Rename yourself to a meaningful name
- msg_cleanup: Remove stale offline agents

## Workflow
1. Always call msg_whoami first to orient yourself
2. If there are unread messages, read them with msg_read
3. If there are unseen broadcasts, check them with msg_broadcasts
4. For outgoing messages, use msg_list_agents to find the target
5. For announcements, use msg_broadcast

## Best Practices
- Rename yourself to something meaningful early (e.g., "code-reviewer", "frontend-dev")
- Check messages before starting work in case another agent left instructions
- Broadcast important status changes (deploys, freezes, completed migrations)
- Use 1-to-1 messages for task assignments and responses
