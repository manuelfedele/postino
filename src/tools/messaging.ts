import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { completable } from "@modelcontextprotocol/sdk/server/completable.js";
import { valkey, keys, publishEvent, getOnlineAgents, renameAgent, cleanupStaleAgents } from "../valkey.js";
import { loadConfig, AGENT_NAME, MSG_BODY } from "../types.js";
import type { Message, Broadcast } from "../types.js";

const config = loadConfig();

// Mutable agent identity so msg_rename can update it
const identity = { name: "" };

export function registerMessagingTools(server: McpServer, initialName: string): void {
  identity.name = initialName;

  server.registerTool("msg_whoami", {
    title: "Status Overview",
    description: [
      "Get a full status overview: your identity, unread messages, unseen broadcasts, and all online agents.",
      "Call this at the start of a session to orient yourself.",
      "Returns everything you need in one call, no need to call msg_check or msg_list_agents separately.",
    ].join(" "),
    inputSchema: {},
  }, async () => {
    // Unread messages
    const unread = await valkey.llen(keys.inbox(identity.name));

    // Unseen broadcasts
    const allBc = await valkey.lrange(keys.broadcasts(), 0, -1);
    const cursorStr = await valkey.get(keys.broadcastCursor(identity.name));
    const cursor = cursorStr ? parseInt(cursorStr, 10) : 0;
    const unseenBc = allBc.length - cursor;

    // Online agents
    const allAgents = await valkey.smembers(keys.agents());
    const onlineSet = new Set(await getOnlineAgents());
    const agentList = [];
    for (const name of allAgents.sort()) {
      const msgCount = await valkey.llen(keys.inbox(name));
      agentList.push({
        name,
        online: onlineSet.has(name),
        messages: msgCount,
        isMe: name === identity.name,
      });
    }

    const status = {
      me: identity.name,
      unreadMessages: unread,
      unseenBroadcasts: Math.max(0, unseenBc),
      totalBroadcasts: allBc.length,
      agents: agentList,
    };

    return {
      content: [{
        type: "text" as const,
        text: JSON.stringify(status, null, 2),
      }],
    };
  });

  server.registerTool("msg_rename", {
    title: "Rename Agent",
    description: [
      "Rename this agent to a meaningful name (e.g. 'devops-agent', 'frontend-dev', 'reviewer').",
      "This moves your inbox and updates your identity across the system.",
      "Other agents will see your new name immediately.",
    ].join(" "),
    inputSchema: {
      name: AGENT_NAME.describe("New agent name (e.g. 'devops-agent', 'code-reviewer')"),
    },
  }, async ({ name }) => {
    const oldName = identity.name;
    if (name === oldName) {
      return {
        content: [{ type: "text" as const, text: `Already named "${name}"` }],
      };
    }

    // Check if name is taken by an online agent
    const online = await getOnlineAgents();
    if (online.includes(name)) {
      return {
        content: [{ type: "text" as const, text: `Name "${name}" is taken by an online agent` }],
        isError: true,
      };
    }

    await renameAgent(oldName, name);
    identity.name = name;

    server.sendLoggingMessage({
      level: "info",
      logger: "postino",
      data: { event: "agent_renamed", from: oldName, to: name },
    }).catch(() => {});

    return {
      content: [{ type: "text" as const, text: `Renamed from "${oldName}" to "${name}"` }],
    };
  });

  server.registerTool("msg_list_agents", {
    title: "List Agents",
    description: [
      "List all known agents and their status.",
      "Shows which agents are currently online (have an active MCP server running)",
      "and how many messages are in each agent's inbox.",
      "Use this to discover who you can send messages to.",
    ].join(" "),
    inputSchema: {},
  }, async () => {
    const allAgents = await valkey.smembers(keys.agents());
    const onlineSet = new Set(await getOnlineAgents());

    const result = [];
    for (const name of allAgents.sort()) {
      const msgCount = await valkey.llen(keys.inbox(name));
      result.push({
        name,
        online: onlineSet.has(name),
        messages: msgCount,
        isMe: name === identity.name,
      });
    }

    return {
      content: [{
        type: "text" as const,
        text: result.length > 0
          ? JSON.stringify(result, null, 2)
          : "No agents registered yet. Send a message to create an inbox.",
      }],
    };
  });

  server.registerTool("msg_check", {
    title: "Check for New Activity",
    description: [
      "Quick check for new messages and broadcasts without consuming them.",
      "Returns unread message count and unseen broadcast count.",
      "Use this to decide whether to call msg_read or msg_broadcasts.",
    ].join(" "),
    inputSchema: {},
  }, async () => {
    const msgCount = await valkey.llen(keys.inbox(identity.name));

    const allBc = await valkey.lrange(keys.broadcasts(), 0, -1);
    const cursorStr = await valkey.get(keys.broadcastCursor(identity.name));
    const cursor = cursorStr ? parseInt(cursorStr, 10) : 0;
    const unseenBc = Math.max(0, allBc.length - cursor);

    const parts: string[] = [];
    if (msgCount > 0) parts.push(`${msgCount} unread message${msgCount !== 1 ? "s" : ""}`);
    if (unseenBc > 0) parts.push(`${unseenBc} unseen broadcast${unseenBc !== 1 ? "s" : ""}`);

    return {
      content: [{
        type: "text" as const,
        text: parts.length > 0
          ? parts.join(", ")
          : "No new messages or broadcasts",
      }],
    };
  });

  server.registerTool("msg_send", {
    title: "Send Message",
    description: [
      "Send a 1-to-1 message to another agent's inbox.",
      "Messages are queued until consumed by msg_read (work queue pattern).",
      "Use msg_whoami or msg_list_agents to discover available agents.",
      "For announcements to ALL agents, use msg_broadcast instead.",
    ].join(" "),
    inputSchema: {
      to: completable(
        AGENT_NAME.describe("Target agent name. Use msg_list_agents to see available agents."),
        async (partial) => {
          const agents = await valkey.smembers(keys.agents());
          return agents.filter(a => a.startsWith(partial.toString())).sort();
        }
      ),
      body: MSG_BODY.describe("Message body"),
    },
  }, async ({ to, body }) => {
    const msg: Message = {
      id: crypto.randomUUID(),
      from: identity.name,
      to,
      body,
      timestamp: new Date().toISOString(),
    };

    await valkey.rpush(keys.inbox(to), JSON.stringify(msg));
    await valkey.expire(keys.inbox(to), config.msgTtl);
    // Enforce inbox size limit: keep only the newest messages
    const inboxLen = await valkey.llen(keys.inbox(to));
    if (inboxLen > config.maxInbox) {
      await valkey.ltrim(keys.inbox(to), inboxLen - config.maxInbox, -1);
    }
    await valkey.sadd(keys.agents(), to);
    await valkey.sadd(keys.agents(), identity.name);

    await valkey.publish(keys.notifyChannel(to), JSON.stringify(msg));
    await publishEvent("msg_send", { from: identity.name, to, messageId: msg.id });

    server.sendLoggingMessage({
      level: "info",
      logger: "postino",
      data: { event: "message_sent", from: identity.name, to, messageId: msg.id },
    }).catch(() => {});

    return {
      content: [{ type: "text" as const, text: `Message sent to "${to}" (id: ${msg.id})` }],
    };
  });

  server.registerTool("msg_read", {
    title: "Read Messages",
    description: [
      "Read and consume messages from your inbox (messages are removed after reading).",
      "Use msg_check first to see if there are messages without consuming them.",
      "Call this when msg_whoami or msg_check reports unread messages.",
    ].join(" "),
    inputSchema: {
      inbox: AGENT_NAME.optional().describe("Inbox to read. Defaults to this agent's own inbox."),
      limit: z.number().optional().default(20).describe("Maximum messages to read"),
    },
  }, async ({ inbox, limit }) => {
    const target = inbox ?? identity.name;
    const maxMessages = limit ?? 20;

    // Atomic read-and-consume with MULTI/EXEC
    const multi = valkey.multi();
    multi.lrange(keys.inbox(target), 0, maxMessages - 1);
    multi.ltrim(keys.inbox(target), maxMessages, -1);
    const results = await multi.exec();
    const raw = results?.[0]?.[1] as string[] ?? [];

    if (raw.length === 0) {
      return {
        content: [{ type: "text" as const, text: `No messages in inbox "${target}"` }],
      };
    }

    const messages: Message[] = raw.map((r: string) => JSON.parse(r));
    await publishEvent("msg_read", { inbox: target, count: raw.length });

    await valkey.sadd(keys.agents(), target);

    server.sendLoggingMessage({
      level: "info",
      logger: "postino",
      data: { event: "messages_read", inbox: target, count: raw.length },
    }).catch(() => {});

    return {
      content: [{
        type: "text" as const,
        text: JSON.stringify(messages, null, 2),
      }],
    };
  });

  server.registerTool("msg_broadcast", {
    title: "Broadcast Message",
    description: [
      "Broadcast a message to ALL agents.",
      "Unlike msg_send, broadcasts are not consumed on read. Every agent sees them.",
      "Broadcasts expire after the configured TTL (default 24h).",
      "Use this for announcements: deploy freezes, CI status, completed migrations.",
    ].join(" "),
    inputSchema: {
      body: MSG_BODY.describe("Broadcast message body"),
    },
  }, async ({ body }) => {
    const bc: Broadcast = {
      id: crypto.randomUUID(),
      from: identity.name,
      body,
      timestamp: new Date().toISOString(),
    };

    await valkey.rpush(keys.broadcasts(), JSON.stringify(bc));
    await valkey.expire(keys.broadcasts(), config.msgTtl);
    // Enforce broadcast list size limit: keep only the newest entries
    const bcLen = await valkey.llen(keys.broadcasts());
    if (bcLen > config.maxBroadcasts) {
      await valkey.ltrim(keys.broadcasts(), bcLen - config.maxBroadcasts, -1);
    }

    await publishEvent("broadcast", { from: identity.name, messageId: bc.id });

    server.sendLoggingMessage({
      level: "info",
      logger: "postino",
      data: { event: "broadcast_sent", from: identity.name, messageId: bc.id },
    }).catch(() => {});

    return {
      content: [{ type: "text" as const, text: `Broadcast sent (id: ${bc.id})` }],
    };
  });

  server.registerTool("msg_broadcasts", {
    title: "Read Broadcasts",
    description: [
      "Read broadcast messages. Shows new broadcasts since your last check.",
      "Broadcasts are shared across all agents and expire by TTL (not consumed on read).",
      "Use all=true to see all broadcasts, not just unseen ones.",
    ].join(" "),
    inputSchema: {
      all: z.boolean().optional().default(false).describe("Show all broadcasts, not just unseen"),
    },
  }, async ({ all: showAll }) => {
    const raw = await valkey.lrange(keys.broadcasts(), 0, -1);

    if (raw.length === 0) {
      return {
        content: [{ type: "text" as const, text: "No broadcasts" }],
      };
    }

    const allBroadcasts: Broadcast[] = raw.map((r: string) => JSON.parse(r));

    // Per-broadcast TTL: filter out expired entries
    const now = Date.now();
    const cutoff = now - config.msgTtl * 1000;

    // Clean expired from head of list (chronologically ordered)
    let expiredFromHead = 0;
    for (const bc of allBroadcasts) {
      if (new Date(bc.timestamp).getTime() <= cutoff) {
        expiredFromHead++;
      } else {
        break;
      }
    }
    if (expiredFromHead > 0) {
      await valkey.ltrim(keys.broadcasts(), expiredFromHead, -1);
    }

    const broadcasts = allBroadcasts.slice(expiredFromHead);

    if (broadcasts.length === 0) {
      return {
        content: [{ type: "text" as const, text: "No broadcasts" }],
      };
    }

    // Adjust cursor for trimmed entries
    const cursorStr = await valkey.get(keys.broadcastCursor(identity.name));
    const rawCursor = cursorStr ? parseInt(cursorStr, 10) : 0;
    const cursor = Math.max(0, rawCursor - expiredFromHead);

    if (showAll) {
      await valkey.set(keys.broadcastCursor(identity.name), String(broadcasts.length), "EX", config.msgTtl);
      return {
        content: [{
          type: "text" as const,
          text: JSON.stringify({ total: broadcasts.length, broadcasts }, null, 2),
        }],
      };
    }

    const unseen = broadcasts.slice(cursor);

    // Update cursor relative to the (potentially trimmed) list
    await valkey.set(keys.broadcastCursor(identity.name), String(broadcasts.length), "EX", config.msgTtl);

    if (unseen.length === 0) {
      return {
        content: [{
          type: "text" as const,
          text: `No new broadcasts (${broadcasts.length} total, all seen)`,
        }],
      };
    }

    return {
      content: [{
        type: "text" as const,
        text: JSON.stringify({ unseen: unseen.length, total: broadcasts.length, broadcasts: unseen }, null, 2),
      }],
    };
  });

  server.registerTool("msg_cleanup", {
    title: "Cleanup Stale Agents",
    description: [
      "Remove offline agents with empty inboxes from the agents list.",
      "Agents are auto-cleaned on shutdown, but this tool handles stragglers",
      "(e.g. crashed processes that never ran their shutdown handler).",
    ].join(" "),
    inputSchema: {},
  }, async () => {
    const removed = await cleanupStaleAgents();

    return {
      content: [{
        type: "text" as const,
        text: removed.length > 0
          ? `Removed ${removed.length} stale agent(s): ${removed.join(", ")}`
          : "No stale agents to clean up",
      }],
    };
  });
}
