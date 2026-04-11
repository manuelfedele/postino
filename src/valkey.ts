import { Redis } from "ioredis";
import { loadConfig } from "./types.js";

const config = loadConfig();

export const valkey = new Redis(config.valkeyUrl, {
  lazyConnect: true,
  maxRetriesPerRequest: 3,
});

export const valkeySub = new Redis(config.valkeyUrl, {
  lazyConnect: true,
  maxRetriesPerRequest: 3,
});

const prefix = config.keyPrefix;

export const keys = {
  inbox: (agent: string) => `${prefix}inbox:${agent}`,
  agents: () => `${prefix}agents`,
  agentInfo: (agent: string) => `${prefix}agent:${agent}`,
  broadcasts: () => `${prefix}broadcasts`,
  broadcastCursor: (agent: string) => `${prefix}bcursor:${agent}`,
  notifyChannel: (agent: string) => `${prefix}notify:${agent}`,
  eventsChannel: () => `${prefix}events`,
};

export async function connect(): Promise<void> {
  await valkey.connect();
  await valkeySub.connect();
}

export async function disconnect(): Promise<void> {
  await valkey.quit();
  await valkeySub.quit();
}

export async function publishEvent(
  type: string,
  data: Record<string, unknown>
): Promise<void> {
  const event = JSON.stringify({ type, ...data, timestamp: new Date().toISOString() });
  await valkey.publish(keys.eventsChannel(), event);
}

const HEARTBEAT_TTL = 30;
const HEARTBEAT_INTERVAL = 15_000;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;

export async function registerAgent(name: string): Promise<void> {
  const info = JSON.stringify({
    name,
    pid: process.pid,
    started_at: new Date().toISOString(),
  });
  await valkey.set(keys.agentInfo(name), info, "EX", HEARTBEAT_TTL);
  await valkey.sadd(keys.agents(), name);
  await publishEvent("agent_online", { agent: name });

  heartbeatTimer = setInterval(async () => {
    try {
      await valkey.expire(keys.agentInfo(name), HEARTBEAT_TTL);
    } catch {
      // Connection lost
    }
  }, HEARTBEAT_INTERVAL);
}

export async function deregisterAgent(name: string): Promise<void> {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  await valkey.del(keys.agentInfo(name));
  await publishEvent("agent_offline", { agent: name });
}

export async function renameAgent(oldName: string, newName: string): Promise<void> {
  // Stop heartbeat for old name
  if (heartbeatTimer) clearInterval(heartbeatTimer);

  // Move inbox messages
  const messages = await valkey.lrange(keys.inbox(oldName), 0, -1);
  if (messages.length > 0) {
    await valkey.rpush(keys.inbox(newName), ...messages);
  }
  await valkey.del(keys.inbox(oldName));

  // Move broadcast cursor
  const cursor = await valkey.get(keys.broadcastCursor(oldName));
  if (cursor) {
    const ttl = await valkey.ttl(keys.broadcastCursor(oldName));
    await valkey.set(keys.broadcastCursor(newName), cursor, "EX", ttl > 0 ? ttl : HEARTBEAT_TTL);
    await valkey.del(keys.broadcastCursor(oldName));
  }

  // Update agents set
  await valkey.srem(keys.agents(), oldName);
  await valkey.sadd(keys.agents(), newName);

  // Deregister old, register new
  await valkey.del(keys.agentInfo(oldName));
  const info = JSON.stringify({
    name: newName,
    pid: process.pid,
    started_at: new Date().toISOString(),
  });
  await valkey.set(keys.agentInfo(newName), info, "EX", HEARTBEAT_TTL);

  // Restart heartbeat for new name
  heartbeatTimer = setInterval(async () => {
    try {
      await valkey.expire(keys.agentInfo(newName), HEARTBEAT_TTL);
    } catch {
      // Connection lost
    }
  }, HEARTBEAT_INTERVAL);

  await publishEvent("agent_rename", { oldName, newName });
}

export async function getOnlineAgents(): Promise<string[]> {
  const allAgents = await valkey.smembers(keys.agents());
  const online: string[] = [];
  for (const name of allAgents) {
    const exists = await valkey.exists(keys.agentInfo(name));
    if (exists) online.push(name);
  }
  return online.sort();
}
