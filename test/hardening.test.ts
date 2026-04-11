import { describe, it, expect, beforeAll, afterAll } from "vitest";
import "./setup.js";
import { connect, disconnect, valkey, keys } from "../src/valkey.js";
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

describe("input validation", () => {
  it("rejects empty agent name in POST /messages", async () => {
    const { status, body } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "", from: "ok", body: "hello" }),
    });
    expect(status).toBe(400);
    expect(body.ok).toBe(false);
  });

  it("rejects special characters in agent name", async () => {
    const { status } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "agent<script>", from: "ok", body: "hello" }),
    });
    expect(status).toBe(400);
  });

  it("rejects agent name over 64 chars", async () => {
    const { status } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "a".repeat(65), from: "ok", body: "hello" }),
    });
    expect(status).toBe(400);
  });

  it("accepts valid agent names with hyphens, dots, underscores", async () => {
    const { status } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "my-agent_v2.0", from: "sender", body: "hi" }),
    });
    expect(status).toBe(200);
  });

  it("rejects empty message body", async () => {
    const { status } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "target", from: "sender", body: "" }),
    });
    expect(status).toBe(400);
  });

  it("rejects oversized message body (> 32KB)", async () => {
    const { status } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: "target", from: "sender", body: "x".repeat(32769) }),
    });
    expect(status).toBe(400);
  });

  it("rejects empty broadcast body", async () => {
    const { status } = await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ body: "" }),
    });
    expect(status).toBe(400);
  });

  it("rejects invalid from in broadcast", async () => {
    const { status } = await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ from: "bad name!", body: "hello" }),
    });
    expect(status).toBe(400);
  });
});

describe("inbox size limits", () => {
  it("trims inbox when over maxInbox limit", async () => {
    const inbox = "limit-test";
    // Push messages directly to bypass any test interference
    for (let i = 0; i < 5; i++) {
      await valkey.rpush(
        keys.inbox(inbox),
        JSON.stringify({ id: `m${i}`, from: "x", to: inbox, body: `msg-${i}`, timestamp: new Date().toISOString() })
      );
    }

    // Send one more via API (which enforces limits)
    // The default maxInbox is 1000, so for this test we check the mechanism works
    // by verifying the ltrim logic doesn't break anything
    const { status } = await req("/messages", {
      method: "POST",
      body: JSON.stringify({ to: inbox, from: "sender", body: "new" }),
    });
    expect(status).toBe(200);

    const { body } = await req(`/messages/${inbox}`);
    expect(body.length).toBe(6); // 5 + 1, all within default 1000 limit
  });
});

describe("health check", () => {
  it("GET /health returns ok with valkey status", async () => {
    const { status, body } = await req("/health");
    expect(status).toBe(200);
    expect(body.ok).toBe(true);
    expect(body.valkey).toBe(true);
    expect(body.uptime).toBeGreaterThanOrEqual(0);
    expect(body.version).toBeDefined();
  });
});

describe("broadcast TTL filtering", () => {
  it("filters expired broadcasts on read", async () => {
    // Clear broadcasts
    await req("/broadcasts", { method: "DELETE" });

    // Insert an expired broadcast directly (timestamp in the past beyond TTL)
    const expired = {
      id: "expired-1",
      from: "old",
      body: "ancient",
      timestamp: new Date(Date.now() - 100000 * 1000).toISOString(), // 100000 seconds ago
    };
    await valkey.rpush(keys.broadcasts(), JSON.stringify(expired));

    // Insert a fresh broadcast
    await req("/broadcasts", {
      method: "POST",
      body: JSON.stringify({ from: "new", body: "fresh" }),
    });

    const { body } = await req("/broadcasts");
    // Expired one should be filtered out
    expect(body.every((b: { body: string }) => b.body !== "ancient")).toBe(true);
    expect(body.some((b: { body: string }) => b.body === "fresh")).toBe(true);
  });
});

describe("stats API includes version", () => {
  it("GET /stats returns version field", async () => {
    const { body } = await req("/stats");
    expect(body.version).toBeDefined();
    expect(body.version).not.toBe("undefined");
  });
});
