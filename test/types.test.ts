import { describe, it, expect, afterEach } from "vitest";

describe("loadConfig", () => {
  const originalEnv = { ...process.env };

  afterEach(() => {
    // Restore env
    process.env = { ...originalEnv };
  });

  it("uses defaults when no env vars set", async () => {
    delete process.env.POSTINO_VALKEY_URL;
    delete process.env.POSTINO_WEB_PORT;
    delete process.env.POSTINO_WEB_ENABLED;
    delete process.env.POSTINO_KEY_PREFIX;
    delete process.env.POSTINO_MSG_TTL;
    delete process.env.POSTINO_AGENT_NAME;
    delete process.env.TERM_SESSION_ID;
    delete process.env.ITERM_SESSION_ID;

    // Dynamic import to pick up fresh env
    const { loadConfig } = await import("../src/types.js");
    const config = loadConfig();

    expect(config.valkeyUrl).toBe("redis://127.0.0.1:6379");
    expect(config.webPort).toBe(3333);
    expect(config.webEnabled).toBe(true);
    expect(config.msgTtl).toBe(86400);
    // Agent name should be PID-based fallback
    expect(config.agentName).toMatch(/^agent-\d+$/);
  });

  it("reads env vars", async () => {
    process.env.POSTINO_VALKEY_URL = "redis://custom:1234";
    process.env.POSTINO_WEB_PORT = "4444";
    process.env.POSTINO_WEB_ENABLED = "false";
    process.env.POSTINO_KEY_PREFIX = "test:";
    process.env.POSTINO_MSG_TTL = "3600";
    process.env.POSTINO_AGENT_NAME = "my-agent";

    const { loadConfig } = await import("../src/types.js");
    const config = loadConfig();

    expect(config.valkeyUrl).toBe("redis://custom:1234");
    expect(config.webPort).toBe(4444);
    expect(config.webEnabled).toBe(false);
    expect(config.keyPrefix).toBe("test:");
    expect(config.msgTtl).toBe(3600);
    expect(config.agentName).toBe("my-agent");
  });

  it("derives agent name from TERM_SESSION_ID", async () => {
    delete process.env.POSTINO_AGENT_NAME;
    process.env.TERM_SESSION_ID = "w0t1p0:ABCD1234-5678";

    const { loadConfig } = await import("../src/types.js");
    const config = loadConfig();

    expect(config.agentName).toBe("agent-ABCD1234");
  });
});
