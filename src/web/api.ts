import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { valkey, valkeySub, keys, publishEvent, getOnlineAgents } from "../valkey.js";
import { loadConfig } from "../types.js";
import type { Message, Broadcast } from "../types.js";

const config = loadConfig();

export const api = new Hono();

// --- Agents ---

api.get("/agents", async (c) => {
  const agents = await valkey.smembers(keys.agents());
  agents.sort();
  const onlineSet = new Set(await getOnlineAgents());

  const result = [];
  for (const agent of agents) {
    const count = await valkey.llen(keys.inbox(agent));
    result.push({ name: agent, messageCount: count, online: onlineSet.has(agent) });
  }

  return c.json(result);
});

// --- Messages ---

api.get("/messages/:inbox", async (c) => {
  const inbox = c.req.param("inbox");
  const raw = await valkey.lrange(keys.inbox(inbox), 0, -1);
  const messages: Message[] = raw.map((r: string) => JSON.parse(r));
  return c.json(messages);
});

api.post("/messages", async (c) => {
  const body = await c.req.json<{ to: string; from?: string; body: string }>();
  const msg: Message = {
    id: crypto.randomUUID(),
    from: body.from || "web-ui",
    to: body.to,
    body: body.body,
    timestamp: new Date().toISOString(),
  };

  await valkey.rpush(keys.inbox(msg.to), JSON.stringify(msg));
  await valkey.expire(keys.inbox(msg.to), config.msgTtl);
  await valkey.sadd(keys.agents(), msg.to);
  if (msg.from !== "anonymous") {
    await valkey.sadd(keys.agents(), msg.from);
  }

  await valkey.publish(keys.notifyChannel(msg.to), JSON.stringify(msg));
  await publishEvent("msg_send", { from: msg.from, to: msg.to, messageId: msg.id });

  return c.json({ ok: true, id: msg.id });
});

api.delete("/messages/:inbox", async (c) => {
  const inbox = c.req.param("inbox");
  await valkey.del(keys.inbox(inbox));
  await publishEvent("msg_read", { inbox, count: "all" });
  return c.json({ ok: true });
});

// --- Broadcasts ---

api.get("/broadcasts", async (c) => {
  const all = await valkey.lrange(keys.broadcasts(), 0, -1);
  const broadcasts: Broadcast[] = all.map((r: string) => JSON.parse(r));
  return c.json(broadcasts);
});

api.post("/broadcasts", async (c) => {
  const body = await c.req.json<{ from?: string; body: string }>();
  const bc: Broadcast = {
    id: crypto.randomUUID(),
    from: body.from || "web-ui",
    body: body.body,
    timestamp: new Date().toISOString(),
  };

  await valkey.rpush(keys.broadcasts(), JSON.stringify(bc));
  await valkey.expire(keys.broadcasts(), config.msgTtl);
  await publishEvent("broadcast", { from: bc.from, messageId: bc.id });

  return c.json({ ok: true, id: bc.id });
});

api.delete("/broadcasts", async (c) => {
  await valkey.del(keys.broadcasts());
  await publishEvent("broadcasts_clear", {});
  return c.json({ ok: true });
});

// --- Stats ---

api.get("/stats", async (c) => {
  const agents = await valkey.smembers(keys.agents());
  let messageCount = 0;
  for (const a of agents) {
    messageCount += await valkey.llen(keys.inbox(a));
  }
  const broadcastCount = await valkey.llen(keys.broadcasts());

  return c.json({ agentCount: agents.length, messageCount, broadcastCount });
});

// --- Agent-specific check (for hooks, zero-token) ---

api.get("/check/:agent", async (c) => {
  const agent = c.req.param("agent");

  const unread = await valkey.llen(keys.inbox(agent));

  const allBc = await valkey.lrange(keys.broadcasts(), 0, -1);
  const cursorStr = await valkey.get(keys.broadcastCursor(agent));
  const cursor = cursorStr ? parseInt(cursorStr, 10) : 0;
  const unseenBc = Math.max(0, allBc.length - cursor);

  return c.json({ agent, unreadMessages: unread, unseenBroadcasts: unseenBc });
});

// --- SSE Events ---

type SSEClient = {
  id: string;
  send: (data: string) => void;
  close: () => void;
};

const sseClients = new Set<SSEClient>();
let subscribedToEvents = false;

function ensureEventSubscription(): void {
  if (subscribedToEvents) return;
  subscribedToEvents = true;

  valkeySub.subscribe(keys.eventsChannel()).catch(() => {
    subscribedToEvents = false;
  });

  valkeySub.on("message", (_channel: string, message: string) => {
    for (const client of sseClients) {
      try {
        client.send(message);
      } catch {
        sseClients.delete(client);
      }
    }
  });
}

api.get("/events", (c) => {
  ensureEventSubscription();

  return streamSSE(c, async (stream) => {
    const clientId = crypto.randomUUID();
    let alive = true;

    const client: SSEClient = {
      id: clientId,
      send: (data: string) => {
        if (alive) {
          stream.writeSSE({ data, event: "update" }).catch(() => {
            alive = false;
          });
        }
      },
      close: () => {
        alive = false;
      },
    };

    sseClients.add(client);

    await stream.writeSSE({ data: JSON.stringify({ type: "connected" }), event: "update" });

    while (alive) {
      await stream.sleep(15000);
      if (alive) {
        await stream.writeSSE({ data: "", event: "ping" }).catch(() => {
          alive = false;
        });
      }
    }

    sseClients.delete(client);
  });
});
