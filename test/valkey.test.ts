import { describe, it, expect, beforeAll, afterAll } from "vitest";
import "./setup.js";
import { valkey, valkeySub, keys, connect, disconnect, registerAgent, deregisterAgent, renameAgent, getOnlineAgents, publishEvent } from "../src/valkey.js";

beforeAll(async () => {
  await connect();
});

afterAll(async () => {
  await disconnect();
});

describe("keys", () => {
  it("generates prefixed keys", () => {
    expect(keys.inbox("alice")).toContain("inbox:alice");
    expect(keys.agents()).toContain("agents");
    expect(keys.agentInfo("alice")).toContain("agent:alice");
    expect(keys.broadcasts()).toContain("broadcasts");
    expect(keys.broadcastCursor("alice")).toContain("bcursor:alice");
  });

});

describe("agent registration", () => {
  it("registers and shows as online", async () => {
    await registerAgent("agent-a");
    const online = await getOnlineAgents();
    expect(online).toContain("agent-a");

    const agents = await valkey.smembers(keys.agents());
    expect(agents).toContain("agent-a");
  });

  it("deregisters and goes offline", async () => {
    await registerAgent("agent-b");
    expect(await getOnlineAgents()).toContain("agent-b");

    await deregisterAgent("agent-b");
    expect(await getOnlineAgents()).not.toContain("agent-b");
  });
});

describe("renameAgent", () => {
  it("moves inbox and updates identity", async () => {
    await registerAgent("old-name");

    // Put a message in the old inbox
    await valkey.rpush(keys.inbox("old-name"), JSON.stringify({ body: "hello" }));
    await valkey.sadd(keys.agents(), "old-name");

    await renameAgent("old-name", "new-name");

    // Old inbox should be empty
    const oldLen = await valkey.llen(keys.inbox("old-name"));
    expect(oldLen).toBe(0);

    // New inbox should have the message
    const newLen = await valkey.llen(keys.inbox("new-name"));
    expect(newLen).toBe(1);

    // New name should be online, old should not
    const online = await getOnlineAgents();
    expect(online).toContain("new-name");
    expect(online).not.toContain("old-name");

    // Agents set should have new name
    const agents = await valkey.smembers(keys.agents());
    expect(agents).toContain("new-name");
    expect(agents).not.toContain("old-name");

    await deregisterAgent("new-name");
  });

  it("moves broadcast cursor", async () => {
    await registerAgent("cursor-test");
    await valkey.set(keys.broadcastCursor("cursor-test"), "5", "EX", 100);

    await renameAgent("cursor-test", "cursor-new");

    const cursor = await valkey.get(keys.broadcastCursor("cursor-new"));
    expect(cursor).toBe("5");

    const oldCursor = await valkey.get(keys.broadcastCursor("cursor-test"));
    expect(oldCursor).toBeNull();

    await deregisterAgent("cursor-new");
  });
});

describe("publishEvent", () => {
  it("publishes to events channel", async () => {
    const received: string[] = [];
    await valkeySub.subscribe(keys.eventsChannel());
    valkeySub.on("message", (_ch: string, msg: string) => received.push(msg));

    await publishEvent("test_event", { foo: "bar" });

    // Give pub/sub a moment
    await new Promise((r) => setTimeout(r, 100));

    expect(received.length).toBe(1);
    const parsed = JSON.parse(received[0]);
    expect(parsed.type).toBe("test_event");
    expect(parsed.foo).toBe("bar");
    expect(parsed.timestamp).toBeDefined();

    await valkeySub.unsubscribe(keys.eventsChannel());
  });
});

