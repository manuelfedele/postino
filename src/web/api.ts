import { timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { bodyLimit } from "hono/body-limit";
import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { z } from "zod";
import {
  acknowledgeMessages,
  appendBroadcast,
  appendMessage,
  clearBroadcasts,
  clearInbox,
  getEventsAfter,
  getInboxCount,
  getOnlineAgents,
  keys,
  leaseMessages,
  listBroadcasts,
  listInbox,
  publishEvent,
  unseenBroadcastCount,
  valkey,
  valkeySub,
} from "../valkey.js";
import { AGENT_NAME, loadConfig, MSG_BODY } from "../types.js";
import type { Broadcast, Message } from "../types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const config = loadConfig();
const startTime = Date.now();
const MAX_PAGE_SIZE = 200;
const MAX_SSE_CLIENTS = 100;

function readVersion(): string {
  try {
    const pkgPath = join(__dirname, "..", "..", "package.json");
    const pkg = JSON.parse(readFileSync(pkgPath, "utf-8"));
    return pkg.version || "0.0.0";
  } catch {
    return "0.0.0";
  }
}

export const VERSION = readVersion();

const PostMessageSchema = z.object({
  id: z.string().min(1).max(128).optional(),
  to: AGENT_NAME,
  from: AGENT_NAME.optional(),
  body: MSG_BODY,
});

const PostBroadcastSchema = z.object({
  id: z.string().min(1).max(128).optional(),
  from: AGENT_NAME.optional(),
  body: MSG_BODY,
});

const AckSchema = z.object({
  ids: z.array(z.string().min(1).max(128)).min(1).max(100),
});

export const api = new Hono();

export function isAuthorizedToken(token: string | undefined): boolean {
  if (!config.apiToken) return true;
  if (!token) return false;
  const expected = Buffer.from(config.apiToken);
  const supplied = Buffer.from(token);
  return (
    expected.length === supplied.length && timingSafeEqual(expected, supplied)
  );
}

export function requestToken(request: {
  header(name: string): string | undefined;
  url: string;
}): string | undefined {
  const authorization = request.header("Authorization");
  const bearer = authorization?.startsWith("Bearer ")
    ? authorization.slice(7)
    : undefined;
  const rawCookie = request
    .header("Cookie")
    ?.match(/(?:^|;\s*)postino_token=([^;]+)/)?.[1];
  const cookie = rawCookie ? decodeURIComponent(rawCookie) : undefined;
  const query = new URL(request.url).searchParams.get("token") ?? undefined;
  return bearer ?? request.header("X-Postino-Token") ?? query ?? cookie;
}

api.use("*", async (c, next) => {
  if (c.req.method === "OPTIONS") return next();
  if (!isAuthorizedToken(requestToken(c.req)))
    return c.json({ ok: false, error: "Unauthorized" }, 401);
  await next();
});

api.use(
  "*",
  bodyLimit({
    maxSize: config.maxBodyBytes,
    onError: (c) => c.json({ ok: false, error: "Request body too large" }, 413),
  }),
);

function page(c: {
  req: { query(name: string): string | undefined };
}): { limit: number; offset: number } | null {
  const limitRaw = c.req.query("limit") ?? "100";
  const offsetRaw = c.req.query("offset") ?? "0";
  const limit = Number(limitRaw);
  const offset = Number(offsetRaw);
  if (
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > MAX_PAGE_SIZE ||
    !Number.isInteger(offset) ||
    offset < 0
  )
    return null;
  return { limit, offset };
}

async function jsonBody(c: any): Promise<unknown | null> {
  try {
    return await c.req.json();
  } catch {
    return null;
  }
}

function consumerId(c: any): string {
  return c.req.header("X-Postino-Consumer") || `http-${crypto.randomUUID()}`;
}

api.get("/agents", async (c) => {
  const requested = page(c);
  if (!requested)
    return c.json({ ok: false, error: "Invalid pagination" }, 400);
  const agents = (await valkey.smembers(keys.agents())).sort();
  const onlineSet = new Set(await getOnlineAgents());
  const items = [];
  for (const agent of agents) {
    items.push({
      name: agent,
      messageCount: await getInboxCount(agent),
      online: onlineSet.has(agent),
    });
  }
  return c.json({
    items: items.slice(requested.offset, requested.offset + requested.limit),
    total: items.length,
    ...requested,
  });
});

api.get("/messages/:inbox", async (c) => {
  const requested = page(c);
  if (!requested)
    return c.json({ ok: false, error: "Invalid pagination" }, 400);
  const inbox = AGENT_NAME.safeParse(c.req.param("inbox"));
  if (!inbox.success)
    return c.json({ ok: false, error: "Invalid inbox name" }, 400);
  const result = await listInbox(inbox.data, requested.offset, requested.limit);
  return c.json({ ...result, ...requested });
});

api.post("/messages", async (c) => {
  const raw = await jsonBody(c);
  const parsed = PostMessageSchema.safeParse(raw);
  if (!parsed.success)
    return c.json(
      { ok: false, error: parsed.error.flatten().fieldErrors },
      400,
    );
  const msg: Message = {
    id:
      c.req.header("Idempotency-Key") || parsed.data.id || crypto.randomUUID(),
    from: parsed.data.from || "web-ui",
    to: parsed.data.to,
    body: parsed.data.body,
    timestamp: new Date().toISOString(),
  };
  let result: Awaited<ReturnType<typeof appendMessage>>;
  try {
    result = await appendMessage(msg);
  } catch (error) {
    if (error instanceof Error && error.message.endsWith("is full")) {
      return c.json({ ok: false, error: error.message }, 429);
    }
    throw error;
  }
  if (result.created) {
    await valkey.publish(keys.notifyChannel(msg.to), JSON.stringify(msg));
    await publishEvent("msg_send", {
      from: msg.from,
      to: msg.to,
      messageId: msg.id,
    });
  }
  return c.json({ ok: true, id: result.message.id, created: result.created });
});

api.post("/messages/:inbox/read", async (c) => {
  const requested = page(c);
  if (!requested)
    return c.json({ ok: false, error: "Invalid pagination" }, 400);
  const inbox = AGENT_NAME.safeParse(c.req.param("inbox"));
  if (!inbox.success)
    return c.json({ ok: false, error: "Invalid inbox name" }, 400);
  const consumer = consumerId(c);
  const messages = await leaseMessages(inbox.data, consumer, requested.limit);
  if (messages.length > 0)
    await publishEvent("msg_read", {
      inbox: inbox.data,
      count: messages.length,
      leased: true,
    });
  return c.json({
    items: messages,
    consumer,
    leaseSeconds: config.messageLease,
    ...requested,
  });
});

api.post("/messages/:inbox/ack", async (c) => {
  const inbox = AGENT_NAME.safeParse(c.req.param("inbox"));
  const raw = await jsonBody(c);
  const parsed = AckSchema.safeParse(raw);
  if (!inbox.success || !parsed.success)
    return c.json({ ok: false, error: "Invalid acknowledgement" }, 400);
  const result = await acknowledgeMessages(
    inbox.data,
    consumerId(c),
    parsed.data.ids,
  );
  if (result.acknowledged.length > 0)
    await publishEvent("msg_ack", {
      inbox: inbox.data,
      count: result.acknowledged.length,
    });
  return c.json(
    { ok: result.rejected.length === 0, ...result },
    result.rejected.length === 0 ? 200 : 409,
  );
});

api.delete("/messages/:inbox", async (c) => {
  const inbox = AGENT_NAME.safeParse(c.req.param("inbox"));
  if (!inbox.success)
    return c.json({ ok: false, error: "Invalid inbox name" }, 400);
  await clearInbox(inbox.data);
  await publishEvent("msg_read", { inbox: inbox.data, count: "all" });
  return c.json({ ok: true });
});

api.get("/broadcasts", async (c) => {
  const requested = page(c);
  if (!requested)
    return c.json({ ok: false, error: "Invalid pagination" }, 400);
  const result = await listBroadcasts(requested.offset, requested.limit);
  return c.json({ ...result, ...requested });
});

api.post("/broadcasts", async (c) => {
  const raw = await jsonBody(c);
  const parsed = PostBroadcastSchema.safeParse(raw);
  if (!parsed.success)
    return c.json(
      { ok: false, error: parsed.error.flatten().fieldErrors },
      400,
    );
  const broadcast: Broadcast = {
    id:
      c.req.header("Idempotency-Key") || parsed.data.id || crypto.randomUUID(),
    from: parsed.data.from || "web-ui",
    body: parsed.data.body,
    timestamp: new Date().toISOString(),
  };
  const result = await appendBroadcast(broadcast);
  if (result.created)
    await publishEvent("broadcast", {
      from: broadcast.from,
      messageId: broadcast.id,
    });
  return c.json({ ok: true, id: result.broadcast.id, created: result.created });
});

api.delete("/broadcasts", async (c) => {
  await clearBroadcasts();
  await publishEvent("broadcasts_clear", {});
  return c.json({ ok: true });
});

api.get("/stats", async (c) => {
  const agents = await valkey.smembers(keys.agents());
  let messageCount = 0;
  for (const agent of agents) messageCount += await getInboxCount(agent);
  const broadcasts = await listBroadcasts(0, 1);
  return c.json({
    version: VERSION,
    messageTtl: config.msgTtl,
    agentCount: agents.length,
    messageCount,
    broadcastCount: broadcasts.total,
  });
});

api.get("/check/:agent", async (c) => {
  const agent = AGENT_NAME.safeParse(c.req.param("agent"));
  if (!agent.success)
    return c.json({ ok: false, error: "Invalid agent name" }, 400);
  return c.json({
    agent: agent.data,
    unreadMessages: await getInboxCount(agent.data),
    unseenBroadcasts: await unseenBroadcastCount(agent.data),
  });
});

api.get("/health", async (c) => {
  let valkeyOk = false;
  let subscriberOk = false;
  try {
    await valkey.ping();
    valkeyOk = true;
  } catch {}
  try {
    await valkeySub.ping();
    subscriberOk = true;
  } catch {}
  return c.json({
    ok: valkeyOk && subscriberOk,
    valkey: valkeyOk,
    subscriber: subscriberOk,
    uptime: Math.floor((Date.now() - startTime) / 1000),
    version: VERSION,
  });
});

type SSEClient = {
  enqueue: (data: string) => void;
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
  valkeySub.on("message", (channel: string, message: string) => {
    if (channel !== keys.eventsChannel()) return;
    for (const client of sseClients) client.enqueue(message);
  });
}

api.get("/events", (c) => {
  if (sseClients.size >= MAX_SSE_CLIENTS)
    return c.json({ ok: false, error: "Too many event subscribers" }, 503);
  ensureEventSubscription();
  return streamSSE(c, async (stream) => {
    let alive = true;
    let wake: (() => void) | null = null;
    const queue: string[] = [];
    const client: SSEClient = {
      enqueue: (data) => {
        if (!alive) return;
        if (queue.length >= 100) queue.shift();
        queue.push(data);
        wake?.();
        wake = null;
      },
      close: () => {
        alive = false;
        wake?.();
        wake = null;
      },
    };
    sseClients.add(client);
    stream.onAbort(() => client.close());
    try {
      const lastId = Number(c.req.header("Last-Event-ID") || 0);
      for (const event of await getEventsAfter(
        Number.isFinite(lastId) ? lastId : 0,
      ))
        client.enqueue(event);
      await stream.writeSSE({
        data: JSON.stringify({ type: "connected" }),
        event: "update",
      });
      while (alive) {
        if (queue.length === 0) {
          await Promise.race([
            new Promise<void>((resolve) => {
              wake = resolve;
            }),
            stream.sleep(15_000),
          ]);
        }
        if (!alive) break;
        const data = queue.shift();
        if (data) {
          const parsed = JSON.parse(data) as { id?: number };
          await stream.writeSSE({
            data,
            event: "update",
            id: parsed.id ? String(parsed.id) : undefined,
          });
        } else {
          await stream.writeSSE({ data: "", event: "ping" });
        }
      }
    } finally {
      sseClients.delete(client);
      client.close();
    }
  });
});
