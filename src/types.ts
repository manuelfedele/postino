import { z } from "zod";
import { createHash } from "node:crypto";
import { hostname } from "node:os";

export const AGENT_NAME = z
  .string()
  .min(1)
  .max(64)
  .regex(
    /^[a-zA-Z0-9][a-zA-Z0-9\-_.]*$/,
    "Agent name must be alphanumeric with hyphens, dots, or underscores",
  );
export const MSG_BODY = z.string().min(1).max(32768);

export interface Message {
  id: string;
  from: string;
  to: string;
  body: string;
  timestamp: string;
  receipt?: string;
  leaseExpiresAt?: string;
}

export interface Broadcast {
  id: string;
  from: string;
  body: string;
  timestamp: string;
}

export interface Config {
  valkeyUrl: string;
  webHost: string;
  webPort: number;
  webEnabled: boolean;
  apiToken?: string;
  corsOrigin?: string;
  keyPrefix: string;
  agentName: string;
  msgTtl: number;
  messageLease: number;
  maxInbox: number;
  maxBroadcasts: number;
  maxBodyBytes: number;
}

function resolveAgentName(): string {
  const configured = process.env.POSTINO_AGENT_NAME;
  if (configured) return AGENT_NAME.parse(configured);

  const sessionId = process.env.TERM_SESSION_ID || process.env.ITERM_SESSION_ID;
  if (sessionId) {
    const short = sessionId.split(":").pop()?.slice(0, 8) ?? "";
    return `agent-${short}`;
  }

  const stableSeed = `${hostname()}:${process.cwd()}`;
  const stableId = createHash("sha256")
    .update(stableSeed)
    .digest("hex")
    .slice(0, 8);
  return `agent-${stableId}`;
}

export function loadConfig(): Config {
  const numberEnv = (
    name: string,
    fallback: number,
    min: number,
    max?: number,
  ): number => {
    const value = z.coerce
      .number()
      .int()
      .min(min)
      .max(max ?? Number.MAX_SAFE_INTEGER)
      .safeParse(process.env[name] ?? fallback);
    if (!value.success) {
      throw new Error(
        `Invalid ${name}: ${value.error.issues[0]?.message ?? "expected an integer"}`,
      );
    }
    return value.data;
  };

  const webHost = process.env.POSTINO_WEB_HOST ?? "127.0.0.1";
  const apiToken = process.env.POSTINO_API_TOKEN || undefined;
  if (
    process.env.POSTINO_WEB_ENABLED !== "false" &&
    !["127.0.0.1", "localhost", "::1"].includes(webHost) &&
    !apiToken
  ) {
    throw new Error(
      "POSTINO_API_TOKEN is required when POSTINO_WEB_HOST is not loopback",
    );
  }

  return {
    valkeyUrl: process.env.POSTINO_VALKEY_URL ?? "redis://127.0.0.1:6379",
    webHost,
    webPort: numberEnv("POSTINO_WEB_PORT", 3333, 1, 65535),
    webEnabled: process.env.POSTINO_WEB_ENABLED !== "false",
    apiToken,
    corsOrigin: process.env.POSTINO_CORS_ORIGIN || undefined,
    keyPrefix: process.env.POSTINO_KEY_PREFIX ?? "po:",
    agentName: resolveAgentName(),
    msgTtl: numberEnv("POSTINO_MSG_TTL", 86400, 1),
    messageLease: numberEnv("POSTINO_MESSAGE_LEASE", 30, 1, 3600),
    maxInbox: numberEnv("POSTINO_MAX_INBOX", 1000, 1),
    maxBroadcasts: numberEnv("POSTINO_MAX_BROADCASTS", 500, 1),
    maxBodyBytes: numberEnv("POSTINO_MAX_BODY_BYTES", 65536, 1024),
  };
}
