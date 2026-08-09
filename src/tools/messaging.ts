import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { completable } from "@modelcontextprotocol/sdk/server/completable.js";
import {
  appendBroadcast,
  appendMessage,
  acknowledgeMessages,
  cleanupStaleAgents,
  getOnlineAgents,
  getInboxCount,
  keys,
  leaseMessages,
  listBroadcasts,
  publishEvent,
  readBroadcastsForAgent,
  renameAgent,
  unseenBroadcastCount,
  valkey,
} from "../valkey.js";
import { loadConfig, AGENT_NAME, MSG_BODY } from "../types.js";
import type { Message, Broadcast } from "../types.js";

const config = loadConfig();

// Mutable agent identity so msg_rename can update it
const identity = { name: "" };
const consumerId = crypto.randomUUID();

export function registerMessagingTools(
  server: McpServer,
  initialName: string,
  onRename?: (name: string) => void,
): void {
  identity.name = initialName;

  server.registerTool(
    "msg_whoami",
    {
      title: "Status Overview",
      description: [
        "Get a full status overview: your identity, unread messages, unseen broadcasts, and all online agents.",
        "Call this at the start of a session to orient yourself.",
        "Returns everything you need in one call, no need to call msg_check or msg_list_agents separately.",
      ].join(" "),
      inputSchema: {},
    },
    async () => {
      // Unread messages
      const unread = await getInboxCount(identity.name);

      // Unseen broadcasts
      const unseenBc = await unseenBroadcastCount(identity.name);
      const allBc = await listBroadcasts(0, config.maxBroadcasts);

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
        totalBroadcasts: allBc.total,
        agents: agentList,
      };

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(status, null, 2),
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_rename",
    {
      title: "Rename Agent",
      description: [
        "Rename this agent to a meaningful name (e.g. 'devops-agent', 'frontend-dev', 'reviewer').",
        "This moves your inbox and updates your identity across the system.",
        "Other agents will see your new name immediately.",
      ].join(" "),
      inputSchema: {
        name: AGENT_NAME.describe(
          "New agent name (e.g. 'devops-agent', 'code-reviewer')",
        ),
      },
    },
    async ({ name }) => {
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
          content: [
            {
              type: "text" as const,
              text: `Name "${name}" is taken by an online agent`,
            },
          ],
          isError: true,
        };
      }

      await renameAgent(oldName, name);
      identity.name = name;
      onRename?.(name);

      server
        .sendLoggingMessage({
          level: "info",
          logger: "postino",
          data: { event: "agent_renamed", from: oldName, to: name },
        })
        .catch(() => {});

      return {
        content: [
          {
            type: "text" as const,
            text: `Renamed from "${oldName}" to "${name}"`,
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_list_agents",
    {
      title: "List Agents",
      description: [
        "List all known agents and their status.",
        "Shows which agents are currently online (have an active connection)",
        "and how many messages are in each agent's inbox.",
        "Use this to discover who you can send messages to.",
      ].join(" "),
      inputSchema: {},
    },
    async () => {
      const allAgents = await valkey.smembers(keys.agents());
      const onlineSet = new Set(await getOnlineAgents());

      const result = [];
      for (const name of allAgents.sort()) {
        const msgCount = await getInboxCount(name);
        result.push({
          name,
          online: onlineSet.has(name),
          messages: msgCount,
          isMe: name === identity.name,
        });
      }

      return {
        content: [
          {
            type: "text" as const,
            text:
              result.length > 0
                ? JSON.stringify(result, null, 2)
                : "No agents registered yet. Send a message to create an inbox.",
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_check",
    {
      title: "Check for New Activity",
      description: [
        "Quick check for new messages and broadcasts without consuming them.",
        "Returns unread message count and unseen broadcast count.",
        "Use this to decide whether to call msg_read or msg_broadcasts.",
      ].join(" "),
      inputSchema: {},
    },
    async () => {
      const msgCount = await getInboxCount(identity.name);
      const unseenBc = await unseenBroadcastCount(identity.name);

      const parts: string[] = [];
      if (msgCount > 0)
        parts.push(`${msgCount} unread message${msgCount !== 1 ? "s" : ""}`);
      if (unseenBc > 0)
        parts.push(`${unseenBc} unseen broadcast${unseenBc !== 1 ? "s" : ""}`);

      return {
        content: [
          {
            type: "text" as const,
            text:
              parts.length > 0
                ? parts.join(", ")
                : "No new messages or broadcasts",
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_send",
    {
      title: "Send Message",
      description: [
        "Send a 1-to-1 message to another agent's inbox.",
        "Messages are queued until consumed by msg_read (work queue pattern).",
        "Use msg_whoami or msg_list_agents to discover available agents.",
        "For announcements to ALL agents, use msg_broadcast instead.",
      ].join(" "),
      inputSchema: {
        to: completable(
          AGENT_NAME.describe(
            "Target agent name. Use msg_list_agents to see available agents.",
          ),
          async (partial) => {
            const agents = await valkey.smembers(keys.agents());
            return agents
              .filter((a) => a.startsWith(partial.toString()))
              .sort();
          },
        ),
        body: MSG_BODY.describe("Message body"),
        id: z
          .string()
          .min(1)
          .max(128)
          .optional()
          .describe("Optional idempotency key"),
      },
    },
    async ({ to, body, id }) => {
      const msg: Message = {
        id: id ?? crypto.randomUUID(),
        from: identity.name,
        to,
        body,
        timestamp: new Date().toISOString(),
      };

      const result = await appendMessage(msg);
      if (result.created) {
        await publishEvent("msg_send", {
          from: identity.name,
          to,
          messageId: msg.id,
        });
        await valkey.publish(keys.notifyChannel(to), JSON.stringify(msg));
      }

      server
        .sendLoggingMessage({
          level: "info",
          logger: "postino",
          data: {
            event: "message_sent",
            from: identity.name,
            to,
            messageId: msg.id,
          },
        })
        .catch(() => {});

      return {
        content: [
          {
            type: "text" as const,
            text: `${result.created ? "Message sent" : "Message already exists"} for "${to}" (id: ${result.message.id})`,
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_read",
    {
      title: "Read Messages",
      description: [
        "Lease messages from your inbox for processing. Leases expire and are redelivered if not acknowledged.",
        "Call msg_ack after successful processing. Use msg_check first to see if there are messages.",
      ].join(" "),
      inputSchema: {
        inbox: AGENT_NAME.optional().describe(
          "Inbox to read. Must be this agent's own inbox.",
        ),
        limit: z
          .number()
          .int()
          .min(1)
          .max(100)
          .optional()
          .default(20)
          .describe("Maximum messages to lease"),
      },
    },
    async ({ inbox, limit }) => {
      const target = inbox ?? identity.name;
      const maxMessages = limit ?? 20;
      if (target !== identity.name) {
        return {
          content: [
            {
              type: "text" as const,
              text: "Agents may only read their own inbox",
            },
          ],
          isError: true,
        };
      }
      const messages = await leaseMessages(target, consumerId, maxMessages);
      if (messages.length === 0) {
        return {
          content: [
            { type: "text" as const, text: `No messages in inbox "${target}"` },
          ],
        };
      }
      await publishEvent("msg_read", {
        inbox: target,
        count: messages.length,
        leased: true,
      });

      server
        .sendLoggingMessage({
          level: "info",
          logger: "postino",
          data: {
            event: "messages_read",
            inbox: target,
            count: messages.length,
          },
        })
        .catch(() => {});

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(
              {
                consumer: consumerId,
                leaseSeconds: config.messageLease,
                messages,
              },
              null,
              2,
            ),
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_ack",
    {
      title: "Acknowledge Messages",
      description:
        "Acknowledge leased messages after processing them. Unacknowledged leases are redelivered after the lease expires.",
      inputSchema: {
        ids: z
          .array(z.string().min(1).max(128))
          .min(1)
          .max(100)
          .describe("Message receipt ids returned by msg_read"),
      },
    },
    async ({ ids }) => {
      const result = await acknowledgeMessages(identity.name, consumerId, ids);
      await publishEvent("msg_ack", {
        inbox: identity.name,
        count: result.acknowledged.length,
      });
      return {
        content: [
          { type: "text" as const, text: JSON.stringify(result, null, 2) },
        ],
        isError: result.rejected.length > 0,
      };
    },
  );

  server.registerTool(
    "msg_broadcast",
    {
      title: "Broadcast Message",
      description: [
        "Broadcast a message to ALL agents.",
        "Unlike msg_send, broadcasts are not consumed on read. Every agent sees them.",
        "Broadcasts expire after the configured TTL (default 24h).",
        "Use this for announcements: deploy freezes, CI status, completed migrations.",
      ].join(" "),
      inputSchema: {
        body: MSG_BODY.describe("Broadcast message body"),
        id: z
          .string()
          .min(1)
          .max(128)
          .optional()
          .describe("Optional idempotency key"),
      },
    },
    async ({ body, id }) => {
      const bc: Broadcast = {
        id: id ?? crypto.randomUUID(),
        from: identity.name,
        body,
        timestamp: new Date().toISOString(),
      };

      const result = await appendBroadcast(bc);
      if (result.created)
        await publishEvent("broadcast", {
          from: identity.name,
          messageId: bc.id,
        });

      server
        .sendLoggingMessage({
          level: "info",
          logger: "postino",
          data: {
            event: "broadcast_sent",
            from: identity.name,
            messageId: bc.id,
          },
        })
        .catch(() => {});

      return {
        content: [
          {
            type: "text" as const,
            text: `${result.created ? "Broadcast sent" : "Broadcast already exists"} (id: ${bc.id})`,
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_broadcasts",
    {
      title: "Read Broadcasts",
      description: [
        "Read broadcast messages. Shows new broadcasts since your last check.",
        "Broadcasts are shared across all agents and expire by TTL (not consumed on read).",
        "Use all=true to see all broadcasts, not just unseen ones.",
      ].join(" "),
      inputSchema: {
        all: z
          .boolean()
          .optional()
          .default(false)
          .describe("Show all broadcasts, not just unseen"),
      },
    },
    async ({ all: showAll }) => {
      const result = await readBroadcastsForAgent(
        identity.name,
        Boolean(showAll),
        true,
      );
      if (result.total === 0) {
        return {
          content: [{ type: "text" as const, text: "No broadcasts" }],
        };
      }

      if (result.broadcasts.length === 0) {
        return {
          content: [
            {
              type: "text" as const,
              text: `No new broadcasts (${result.total} total, all seen)`,
            },
          ],
        };
      }

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(
              {
                unseen: result.broadcasts.length,
                total: result.total,
                broadcasts: result.broadcasts,
              },
              null,
              2,
            ),
          },
        ],
      };
    },
  );

  server.registerTool(
    "msg_cleanup",
    {
      title: "Cleanup Stale Agents",
      description: [
        "Remove offline agents with empty inboxes from the agents list.",
        "Agents are auto-cleaned on shutdown, but this tool handles stragglers",
        "(e.g. crashed processes that never ran their shutdown handler).",
      ].join(" "),
      inputSchema: {},
    },
    async () => {
      const removed = await cleanupStaleAgents();

      return {
        content: [
          {
            type: "text" as const,
            text:
              removed.length > 0
                ? `Removed ${removed.length} stale agent(s): ${removed.join(", ")}`
                : "No stale agents to clean up",
          },
        ],
      };
    },
  );
}
