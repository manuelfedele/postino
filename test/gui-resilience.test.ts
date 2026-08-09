import { describe, it, expect, beforeAll, afterAll } from "vitest";
import "./setup.js";
import { valkey, valkeySub, keys, connect, disconnect } from "../src/valkey.js";
import {
  getGuiState,
  restartOnPort,
  stopWebServer,
} from "../src/web/server.js";

beforeAll(async () => {
  await connect();
});

afterAll(async () => {
  await stopWebServer();
  await disconnect();
});

describe("gui_takeover channel", () => {
  it("publishes gui_released event with the correct port", async () => {
    const channel = keys.guiTakeoverChannel();
    const received: string[] = [];

    await valkeySub.subscribe(channel);
    valkeySub.on("message", (_ch: string, msg: string) => received.push(msg));

    const releasedPort = 3333;
    await valkey.publish(channel, JSON.stringify({ port: releasedPort }));

    // Give pub/sub a moment
    await new Promise((r) => setTimeout(r, 100));

    expect(received.length).toBe(1);
    const parsed = JSON.parse(received[0]);
    expect(parsed.port).toBe(releasedPort);

    valkeySub.removeAllListeners("message");
    await valkeySub.unsubscribe(channel);
  });

  it("subscriber receives gui_released events from another publisher", async () => {
    const channel = keys.guiTakeoverChannel();
    const received: string[] = [];

    await valkeySub.subscribe(channel);
    valkeySub.on("message", (ch: string, msg: string) => {
      if (ch === channel) received.push(msg);
    });

    // Simulate two different ports being released
    await valkey.publish(channel, JSON.stringify({ port: 4000 }));
    await valkey.publish(channel, JSON.stringify({ port: 4001 }));

    await new Promise((r) => setTimeout(r, 100));

    expect(received.length).toBe(2);
    expect(JSON.parse(received[0]).port).toBe(4000);
    expect(JSON.parse(received[1]).port).toBe(4001);

    valkeySub.removeAllListeners("message");
    await valkeySub.unsubscribe(channel);
  });
});

describe("guiTakeoverChannel key", () => {
  it("includes the test prefix", () => {
    const channel = keys.guiTakeoverChannel();
    expect(channel).toContain("gui:takeover");
    // Verify it uses the test prefix (set in setup.ts)
    expect(channel).toMatch(/^po-test-\d+:/);
  });
});

describe("web server restart", () => {
  it("can start on a free port", async () => {
    // Pick a high ephemeral port unlikely to be in use
    const port = 19876;
    const ok = await restartOnPort(port);
    expect(ok).toBe(true);

    const state = getGuiState();
    expect(state.running).toBe(true);
    expect(state.port).toBe(port);
  });

  it("can stop and restart on a different port", async () => {
    const portA = 19877;
    const portB = 19878;

    // Start on portA
    const okA = await restartOnPort(portA);
    expect(okA).toBe(true);
    expect(getGuiState()).toEqual({ running: true, port: portA });

    // Restart on portB (should close portA first)
    const okB = await restartOnPort(portB);
    expect(okB).toBe(true);
    expect(getGuiState()).toEqual({ running: true, port: portB });
  });

  it("returns false when the target port is already in use", async () => {
    // Occupy a port with a raw net server, then ask restartOnPort to bind there
    const blockedPort = 19880;
    const net = await import("node:net");
    const blocker = net.createServer();
    await new Promise<void>((resolve) => {
      blocker.listen(blockedPort, "127.0.0.1", () => resolve());
    });

    try {
      const ok = await restartOnPort(blockedPort);
      expect(ok).toBe(false);
    } finally {
      await new Promise<void>((resolve) => blocker.close(() => resolve()));
    }
  });
});
