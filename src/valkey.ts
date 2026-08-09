import { Redis } from "ioredis";
import { loadConfig } from "./types.js";
import type { Broadcast, Message } from "./types.js";

const config = loadConfig();

export const valkey = new Redis(config.valkeyUrl, {
  lazyConnect: true,
  maxRetriesPerRequest: 3,
});
valkey.on("error", (err) => {
  process.stderr.write(`postino storage: ${err.message}\n`);
});

export const valkeySub = new Redis(config.valkeyUrl, {
  lazyConnect: true,
  maxRetriesPerRequest: 3,
});
valkeySub.on("error", (err) => {
  process.stderr.write(`postino storage subscriber: ${err.message}\n`);
});

const prefix = config.keyPrefix;
const LOCK_TTL = 10;
const HEARTBEAT_TTL = 30;
const HEARTBEAT_INTERVAL = 15_000;
const EVENT_HISTORY_LIMIT = 1000;

export const keys = {
  inbox: (agent: string) => `${prefix}inbox:${agent}`,
  inboxPending: (agent: string) => `${prefix}pending:${agent}`,
  inboxPendingDue: (agent: string) => `${prefix}pending-due:${agent}`,
  inboxAcked: (agent: string) => `${prefix}acked:${agent}`,
  messageDedupe: (agent: string, id: string) =>
    `${prefix}dedupe:message:${agent}:${id}`,
  agents: () => `${prefix}agents`,
  agentInfo: (agent: string, instance?: string) =>
    instance
      ? `${prefix}agent:${agent}:${instance}`
      : `${prefix}agent:${agent}`,
  agentInstances: (agent: string) => `${prefix}agent-instances:${agent}`,
  broadcasts: () => `${prefix}broadcasts`,
  broadcastCursor: (agent: string) => `${prefix}bcursor:${agent}`,
  broadcastDedupe: (id: string) => `${prefix}dedupe:broadcast:${id}`,
  notifyChannel: (agent: string) => `${prefix}notify:${agent}`,
  eventsChannel: () => `${prefix}events`,
  eventSequence: () => `${prefix}event-sequence`,
  eventHistory: () => `${prefix}event-history`,
  guiTakeoverChannel: () => `${prefix}gui:takeover`,
  globalLock: () => `${prefix}lock:global`,
};

type LockToken = string;

const RELEASE_LOCK = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`;

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, Math.min(ms, 30_000)));
}

async function acquireLock(key: string, timeoutMs = 5_000): Promise<LockToken> {
  const token = crypto.randomUUID();
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const acquired = await valkey.set(key, token, "EX", LOCK_TTL, "NX");
    if (acquired === "OK") return token;
    await sleep(25);
  }
  throw new Error(`Timed out acquiring storage lock: ${key}`);
}

async function releaseLock(key: string, token: LockToken): Promise<void> {
  await valkey.eval(RELEASE_LOCK, 1, key, token);
}

export async function withStorageLock<T>(fn: () => Promise<T>): Promise<T> {
  const key = keys.globalLock();
  const token = await acquireLock(key);
  try {
    return await fn();
  } finally {
    await releaseLock(key, token).catch(() => {});
  }
}

export async function connect(): Promise<void> {
  if (valkey.status !== "ready") {
    await valkey.connect();
    await valkey.ping();
  }
  if (valkeySub.status !== "ready") await valkeySub.connect();
}

export async function disconnect(): Promise<void> {
  if (valkey.status !== "end") await valkey.quit();
  if (valkeySub.status !== "end") await valkeySub.quit();
}

export async function publishEvent(
  type: string,
  data: Record<string, unknown>,
): Promise<void> {
  const id = await valkey.incr(keys.eventSequence());
  const event = JSON.stringify({
    id,
    type,
    ...data,
    timestamp: new Date().toISOString(),
  });
  await valkey.rpush(keys.eventHistory(), event);
  await valkey.ltrim(keys.eventHistory(), -EVENT_HISTORY_LIMIT, -1);
  await valkey.expire(keys.eventHistory(), config.msgTtl);
  await valkey.publish(keys.eventsChannel(), event);
}

export async function getEventsAfter(lastId: number): Promise<string[]> {
  const raw = await valkey.lrange(keys.eventHistory(), 0, -1);
  return raw.filter((entry) => {
    try {
      return Number(JSON.parse(entry).id) > lastId;
    } catch {
      return false;
    }
  });
}

type HeartbeatState = {
  name: string;
  timer: ReturnType<typeof setInterval>;
};

const heartbeatStates = new Map<string, HeartbeatState>();

export async function registerAgent(
  name: string,
  instance = crypto.randomUUID(),
): Promise<string> {
  const infoKey = keys.agentInfo(name, instance);
  const info = JSON.stringify({
    name,
    instance,
    pid: process.pid,
    started_at: new Date().toISOString(),
  });
  await withStorageLock(async () => {
    await valkey.set(infoKey, info, "EX", HEARTBEAT_TTL);
    await valkey.sadd(keys.agentInstances(name), instance);
    await valkey.sadd(keys.agents(), name);
  });

  const previous = heartbeatStates.get(instance);
  if (previous) clearInterval(previous.timer);
  const state: HeartbeatState = {
    name,
    timer: setInterval(async () => {
      try {
        await valkey.expire(
          keys.agentInfo(state.name, instance),
          HEARTBEAT_TTL,
        );
      } catch {
        // A later heartbeat or the process shutdown will handle recovery.
      }
    }, HEARTBEAT_INTERVAL),
  };
  heartbeatStates.set(instance, state);
  await publishEvent("agent_online", { agent: name, instance });
  return instance;
}

async function removeAgentIfCleanable(name: string): Promise<boolean> {
  const instances = await valkey.smembers(keys.agentInstances(name));
  let online = false;
  for (const instance of instances) {
    if ((await valkey.exists(keys.agentInfo(name, instance))) === 1) {
      online = true;
      break;
    }
  }
  const inboxLength = await valkey.llen(keys.inbox(name));
  if (online || inboxLength > 0) return false;
  await valkey.srem(keys.agents(), name);
  await valkey.del(
    keys.agentInstances(name),
    keys.broadcastCursor(name),
    keys.inboxPending(name),
    keys.inboxPendingDue(name),
    keys.inboxAcked(name),
  );
  return true;
}

export async function deregisterAgent(
  name: string,
  instance?: string,
): Promise<void> {
  const instances = instance
    ? [instance]
    : await valkey.smembers(keys.agentInstances(name));
  for (const current of instances) {
    const state = heartbeatStates.get(current);
    if (state) {
      clearInterval(state.timer);
      heartbeatStates.delete(current);
    }
  }
  await withStorageLock(async () => {
    for (const current of instances) {
      await valkey.del(keys.agentInfo(name, current));
      await valkey.srem(keys.agentInstances(name), current);
    }
    await removeAgentIfCleanable(name);
  });
  await publishEvent("agent_offline", {
    agent: name,
    instance: instance ?? "all",
  });
}

export async function cleanupStaleAgents(): Promise<string[]> {
  return withStorageLock(async () => {
    const allAgents = await valkey.smembers(keys.agents());
    const removed: string[] = [];
    for (const name of allAgents) {
      const instances = await valkey.smembers(keys.agentInstances(name));
      for (const instance of instances) {
        if ((await valkey.exists(keys.agentInfo(name, instance))) === 0) {
          await valkey.srem(keys.agentInstances(name), instance);
        }
      }
      if (
        (await valkey.scard(keys.agentInstances(name))) === 0 &&
        (await valkey.llen(keys.inbox(name))) === 0
      ) {
        await valkey.srem(keys.agents(), name);
        await valkey.del(keys.broadcastCursor(name));
        removed.push(name);
      }
    }
    for (const name of removed)
      await publishEvent("agent_offline", { agent: name, stale: true });
    return removed;
  });
}

async function compactInboxUnlocked(name: string): Promise<void> {
  const raw = await valkey.lrange(keys.inbox(name), 0, -1);
  if (raw.length === 0) return;
  const acked = new Set(await valkey.smembers(keys.inboxAcked(name)));
  const now = Date.now();
  for (const serialized of raw) {
    let message: Message;
    try {
      message = JSON.parse(serialized) as Message;
    } catch {
      await valkey.lrem(keys.inbox(name), 1, serialized);
      continue;
    }
    const expired = Date.parse(message.timestamp) + config.msgTtl * 1000 <= now;
    if (expired || acked.has(message.id)) {
      await valkey.lrem(keys.inbox(name), 1, serialized);
      await valkey.hdel(keys.inboxPending(name), message.id);
      await valkey.zrem(keys.inboxPendingDue(name), message.id);
      await valkey.srem(keys.inboxAcked(name), message.id);
    }
  }
  if ((await valkey.llen(keys.inbox(name))) === 0) {
    await valkey.del(
      keys.inbox(name),
      keys.inboxPending(name),
      keys.inboxPendingDue(name),
      keys.inboxAcked(name),
    );
  }
}

type PendingMessage = {
  message: Message;
  consumer: string;
  expiresAt: number;
};

function parseStoredMessage(value: string): Message | null {
  try {
    return JSON.parse(value) as Message;
  } catch {
    return null;
  }
}

async function reclaimExpiredLeasesUnlocked(name: string): Promise<void> {
  const now = Date.now();
  const expired = new Set(
    await valkey.zrangebyscore(keys.inboxPendingDue(name), 0, now),
  );
  const pending = await valkey.hgetall(keys.inboxPending(name));
  for (const [id, serialized] of Object.entries(pending)) {
    try {
      if ((JSON.parse(serialized) as PendingMessage).expiresAt <= now)
        expired.add(id);
    } catch {
      expired.add(id);
    }
  }
  if (expired.size === 0) return;
  const ids = [...expired];
  await valkey.zrem(keys.inboxPendingDue(name), ...ids);
  await valkey.hdel(keys.inboxPending(name), ...ids);
}

export async function appendMessage(
  message: Message,
): Promise<{ created: boolean; message: Message }> {
  return withStorageLock(async () => {
    await compactInboxUnlocked(message.to);
    const existing = await valkey.get(
      keys.messageDedupe(message.to, message.id),
    );
    if (existing)
      return {
        created: false,
        message: parseStoredMessage(existing) ?? message,
      };
    if ((await valkey.llen(keys.inbox(message.to))) >= config.maxInbox) {
      throw new Error(`Inbox "${message.to}" is full`);
    }
    const serialized = JSON.stringify(message);
    await valkey.rpush(keys.inbox(message.to), serialized);
    await valkey.set(
      keys.messageDedupe(message.to, message.id),
      serialized,
      "EX",
      config.msgTtl,
    );
    await valkey.expire(keys.inbox(message.to), config.msgTtl);
    await valkey.sadd(keys.agents(), message.to, message.from);
    return { created: true, message };
  });
}

export async function listInbox(
  name: string,
  offset: number,
  limit: number,
): Promise<{ items: Message[]; total: number }> {
  return withStorageLock(async () => {
    await compactInboxUnlocked(name);
    const raw = await valkey.lrange(keys.inbox(name), 0, -1);
    const acked = new Set(await valkey.smembers(keys.inboxAcked(name)));
    const items = raw
      .map(parseStoredMessage)
      .filter(
        (message): message is Message =>
          message !== null && !acked.has(message.id),
      );
    return { items: items.slice(offset, offset + limit), total: items.length };
  });
}

export async function getInboxCount(name: string): Promise<number> {
  const result = await listInbox(name, 0, config.maxInbox);
  return result.total;
}

export async function leaseMessages(
  name: string,
  consumer: string,
  limit: number,
): Promise<Message[]> {
  return withStorageLock(async () => {
    await compactInboxUnlocked(name);
    await reclaimExpiredLeasesUnlocked(name);
    const raw = await valkey.lrange(keys.inbox(name), 0, -1);
    const acked = new Set(await valkey.smembers(keys.inboxAcked(name)));
    const pending = await valkey.hgetall(keys.inboxPending(name));
    const expiresAt = Date.now() + config.messageLease * 1000;
    const selected: Message[] = [];
    for (const serialized of raw) {
      const message = parseStoredMessage(serialized);
      if (!message || acked.has(message.id) || pending[message.id]) continue;
      const leased: PendingMessage = { message, consumer, expiresAt };
      await valkey.hset(
        keys.inboxPending(name),
        message.id,
        JSON.stringify(leased),
      );
      await valkey.zadd(keys.inboxPendingDue(name), expiresAt, message.id);
      selected.push({
        ...message,
        receipt: message.id,
        leaseExpiresAt: new Date(expiresAt).toISOString(),
      });
      if (selected.length >= limit) break;
    }
    await valkey.expire(keys.inboxPending(name), config.messageLease);
    await valkey.expire(keys.inboxPendingDue(name), config.messageLease);
    return selected;
  });
}

export async function acknowledgeMessages(
  name: string,
  consumer: string,
  ids: string[],
): Promise<{ acknowledged: string[]; rejected: string[] }> {
  return withStorageLock(async () => {
    const acknowledged: string[] = [];
    const rejected: string[] = [];
    for (const id of ids) {
      const rawPending = await valkey.hget(keys.inboxPending(name), id);
      if (!rawPending) {
        if (await valkey.sismember(keys.inboxAcked(name), id))
          acknowledged.push(id);
        else rejected.push(id);
        continue;
      }
      const pending = JSON.parse(rawPending) as PendingMessage;
      if (pending.consumer !== consumer) {
        rejected.push(id);
        continue;
      }
      await valkey.sadd(keys.inboxAcked(name), id);
      await valkey.hdel(keys.inboxPending(name), id);
      await valkey.zrem(keys.inboxPendingDue(name), id);
      await valkey.lrem(keys.inbox(name), 1, JSON.stringify(pending.message));
      acknowledged.push(id);
    }
    await compactInboxUnlocked(name);
    return { acknowledged, rejected };
  });
}

export async function clearInbox(name: string): Promise<void> {
  await withStorageLock(async () => {
    await valkey.del(
      keys.inbox(name),
      keys.inboxPending(name),
      keys.inboxPendingDue(name),
      keys.inboxAcked(name),
    );
  });
}

export async function renameAgent(
  oldName: string,
  newName: string,
): Promise<void> {
  await withStorageLock(async () => {
    if (oldName === newName) return;
    if (
      (await valkey.scard(keys.agentInstances(newName))) > 0 ||
      (await valkey.exists(keys.inbox(newName))) === 1
    ) {
      throw new Error(`Agent name "${newName}" is already in use`);
    }
    const oldInbox = keys.inbox(oldName);
    if ((await valkey.exists(oldInbox)) === 1)
      await valkey.rename(oldInbox, keys.inbox(newName));
    const inboxKeys = [
      [keys.inboxPending(oldName), keys.inboxPending(newName)],
      [keys.inboxPendingDue(oldName), keys.inboxPendingDue(newName)],
      [keys.inboxAcked(oldName), keys.inboxAcked(newName)],
    ] as const;
    for (const [oldKey, newKey] of inboxKeys) {
      if (await valkey.exists(oldKey)) await valkey.rename(oldKey, newKey);
    }
    const cursor = await valkey.get(keys.broadcastCursor(oldName));
    if (cursor)
      await valkey.set(
        keys.broadcastCursor(newName),
        cursor,
        "EX",
        config.msgTtl,
      );
    await valkey.del(keys.broadcastCursor(oldName));
    const instances = await valkey.smembers(keys.agentInstances(oldName));
    for (const instance of instances) {
      await valkey.rename(
        keys.agentInfo(oldName, instance),
        keys.agentInfo(newName, instance),
      );
      await valkey.sadd(keys.agentInstances(newName), instance);
      const state = heartbeatStates.get(instance);
      if (state) state.name = newName;
    }
    await valkey.del(keys.agentInstances(oldName));
    await valkey.srem(keys.agents(), oldName);
    await valkey.sadd(keys.agents(), newName);
  });
  await publishEvent("agent_rename", { oldName, newName });
}

export async function getOnlineAgents(): Promise<string[]> {
  const allAgents = await valkey.smembers(keys.agents());
  const online: string[] = [];
  for (const name of allAgents) {
    const instances = await valkey.smembers(keys.agentInstances(name));
    let isOnline = false;
    for (const instance of instances) {
      if ((await valkey.exists(keys.agentInfo(name, instance))) === 1) {
        isOnline = true;
      }
    }
    if (isOnline) online.push(name);
  }
  return online.sort();
}

async function activeBroadcastsUnlocked(): Promise<Broadcast[]> {
  const raw = await valkey.lrange(keys.broadcasts(), 0, -1);
  const cutoff = Date.now() - config.msgTtl * 1000;
  let expired = 0;
  const parsed: Broadcast[] = [];
  for (const serialized of raw) {
    try {
      const broadcast = JSON.parse(serialized) as Broadcast;
      if (Date.parse(broadcast.timestamp) <= cutoff) expired++;
      else parsed.push(broadcast);
    } catch {
      expired++;
    }
  }
  if (expired > 0) await valkey.ltrim(keys.broadcasts(), expired, -1);
  return parsed;
}

export async function listBroadcasts(
  offset: number,
  limit: number,
): Promise<{ items: Broadcast[]; total: number }> {
  return withStorageLock(async () => {
    const items = await activeBroadcastsUnlocked();
    return { items: items.slice(offset, offset + limit), total: items.length };
  });
}

export async function appendBroadcast(
  broadcast: Broadcast,
): Promise<{ created: boolean; broadcast: Broadcast }> {
  return withStorageLock(async () => {
    await activeBroadcastsUnlocked();
    const dedupeKey = keys.broadcastDedupe(broadcast.id);
    if (await valkey.exists(dedupeKey)) return { created: false, broadcast };
    await valkey.rpush(keys.broadcasts(), JSON.stringify(broadcast));
    await valkey.set(dedupeKey, "1", "EX", config.msgTtl);
    await valkey.ltrim(keys.broadcasts(), -config.maxBroadcasts, -1);
    await valkey.expire(keys.broadcasts(), config.msgTtl);
    return { created: true, broadcast };
  });
}

function broadcastStartIndex(
  items: Broadcast[],
  cursor: string | null,
): number {
  if (!cursor) return 0;
  const index = items.findIndex((item) => item.id === cursor);
  if (index >= 0) return index + 1;
  // Numeric cursors from pre-0.6 are not safely mappable after trimming. Replay
  // the retained window rather than silently dropping a broadcast.
  if (/^\d+$/.test(cursor)) return 0;
  // A missing stable cursor means the item was trimmed. Replay the retained window.
  return 0;
}

export async function readBroadcastsForAgent(
  agent: string,
  showAll: boolean,
  advance: boolean,
): Promise<{ broadcasts: Broadcast[]; total: number }> {
  return withStorageLock(async () => {
    const items = await activeBroadcastsUnlocked();
    const cursor = await valkey.get(keys.broadcastCursor(agent));
    const start = showAll ? 0 : broadcastStartIndex(items, cursor);
    if (advance) {
      if (items.length > 0)
        await valkey.set(
          keys.broadcastCursor(agent),
          items[items.length - 1].id,
          "EX",
          config.msgTtl,
        );
      else await valkey.del(keys.broadcastCursor(agent));
    }
    return { broadcasts: items.slice(start), total: items.length };
  });
}

export async function unseenBroadcastCount(agent: string): Promise<number> {
  return withStorageLock(async () => {
    const items = await activeBroadcastsUnlocked();
    const cursor = await valkey.get(keys.broadcastCursor(agent));
    return items.length - broadcastStartIndex(items, cursor);
  });
}

async function deleteMatching(pattern: string): Promise<void> {
  let cursor = "0";
  do {
    const result = await valkey.scan(cursor, "MATCH", pattern, "COUNT", 100);
    cursor = result[0];
    if (result[1].length > 0) await valkey.del(...result[1]);
  } while (cursor !== "0");
}

export async function clearBroadcasts(): Promise<void> {
  await withStorageLock(async () => {
    await valkey.del(keys.broadcasts());
    await deleteMatching(`${prefix}bcursor:*`);
    await deleteMatching(`${prefix}dedupe:broadcast:*`);
  });
}
