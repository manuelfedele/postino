import { describe, it, expect, beforeAll, afterAll } from "vitest";
import "./setup.js";
import { connect, disconnect, valkey, keys, registerAgent, deregisterAgent } from "../src/valkey.js";
import { api } from "../src/web/api.js";
import { Hono } from "hono";

const app = new Hono();
app.route("/api", api);

beforeAll(async () => {
  await connect();
});

afterAll(async () => {
  await disconnect();
});

async function req(path: string, opts: RequestInit = {}) {
  const res = await app.request(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  return { status: res.status, body: await res.json() };
}

describe("messages API", () => {
  it("POST /messages sends a message", async () => {
    const { status, body } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "alice", from: "bob", body: "hello" }),
    });
    expect(status).toBe(200);
    expect(body.ok).toBe(true);
    expect(body.id).toBeDefined();
  });

  it("GET /messages/:inbox returns messages", async () => {
    await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "reader", from: "writer", body: "msg1" }),
    });
    await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "reader", from: "writer", body: "msg2" }),
    });

    const { body } = await req("/messages/reader");
    expect(body).toHaveLength(2);
    expect(body[0].from).toBe("writer");
    expect(body[0].body).toBe("msg1");
    expect(body[1].body).toBe("msg2");
  });

  it("DELETE /messages/:inbox clears inbox", async () => {
    await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "cleaner", from: "x", body: "bye" }),
    });

    const { body: delResult } = await req("/messages/cleaner", { method: "DELETE" });
    expect(delResult.ok).toBe(true);

    const { body: msgs } = await req("/messages/cleaner");
    expect(msgs).toHaveLength(0);
  });
});

describe("broadcasts API", () => {
  it("POST /broadcasts creates a broadcast", async () => {
    const { status, body } = await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ from: "ci", body: "green" }),
    });
    expect(status).toBe(200);
    expect(body.ok).toBe(true);
  });

  it("GET /broadcasts returns all broadcasts", async () => {
    await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ from: "a", body: "first" }),
    });
    await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ from: "b", body: "second" }),
    });

    const { body } = await req("/broadcasts");
    expect(body.length).toBeGreaterThanOrEqual(2);
    expect(body.some((b: { body: string }) => b.body === "first")).toBe(true);
  });

  it("DELETE /broadcasts clears all", async () => {
    await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ body: "temp" }),
    });

    await req("/broadcasts", { method: "DELETE" });

    const { body } = await req("/broadcasts");
    expect(body).toHaveLength(0);
  });
});

describe("agents API", () => {
  it("GET /agents shows agents with online status", async () => {
    await registerAgent("online-agent");
    // Send a message to create the agent in the set
    await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "online-agent", from: "x", body: "hi" }),
    });

    const { body } = await req("/agents");
    const agent = body.find((a: { name: string }) => a.name === "online-agent");
    expect(agent).toBeDefined();
    expect(agent.online).toBe(true);
    expect(agent.messageCount).toBe(1);

    await deregisterAgent("online-agent");
  });
});

describe("check API", () => {
  it("GET /check/:agent returns unread and unseen counts", async () => {
    // Send a direct message
    await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "checker", from: "x", body: "hey" }),
    });
    // Send a broadcast
    await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ body: "announcement" }),
    });

    const { body } = await req("/check/checker");
    expect(body.agent).toBe("checker");
    expect(body.unreadMessages).toBe(1);
    expect(body.unseenBroadcasts).toBeGreaterThanOrEqual(1);
  });

  it("returns 0 unseen after cursor is set", async () => {
    // Clear broadcasts first
    await req("/broadcasts", { method: "DELETE" });

    await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ body: "bc1" }),
    });

    // Set cursor to mark as seen
    await valkey.set(keys.broadcastCursor("seen-agent"), "1", "EX", 100);

    const { body } = await req("/check/seen-agent");
    expect(body.unseenBroadcasts).toBe(0);
  });
});

describe("stats API", () => {
  it("GET /stats returns counts", async () => {
    const { body } = await req("/stats");
    expect(body).toHaveProperty("agentCount");
    expect(body).toHaveProperty("messageCount");
    expect(body).toHaveProperty("broadcastCount");
  });
});
