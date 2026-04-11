import { Redis } from "ioredis";
import { afterAll, beforeAll, beforeEach } from "vitest";

// Use a unique test prefix to avoid colliding with real data
const TEST_PREFIX = `po-test-${process.pid}:`;

// Set env before any imports that read config
process.env.POSTINO_KEY_PREFIX = TEST_PREFIX;
process.env.POSTINO_AGENT_NAME = "test-agent";
process.env.POSTINO_WEB_ENABLED = "false";

let cleanupRedis: Redis;

beforeAll(async () => {
  cleanupRedis = new Redis({ lazyConnect: true });
  await cleanupRedis.connect();
});

beforeEach(async () => {
  // Clear all test keys
  const keys = await cleanupRedis.keys(`${TEST_PREFIX}*`);
  if (keys.length > 0) {
    await cleanupRedis.del(...keys);
  }
});

afterAll(async () => {
  // Final cleanup
  const keys = await cleanupRedis.keys(`${TEST_PREFIX}*`);
  if (keys.length > 0) {
    await cleanupRedis.del(...keys);
  }
  await cleanupRedis.quit();
});

export { TEST_PREFIX };
