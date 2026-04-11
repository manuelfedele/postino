export interface Message {
  id: string;
  from: string;
  to: string;
  body: string;
  timestamp: string;
}

export interface Broadcast {
  id: string;
  from: string;
  body: string;
  timestamp: string;
}

export interface Config {
  valkeyUrl: string;
  webPort: number;
  webEnabled: boolean;
  keyPrefix: string;
  agentName: string;
  msgTtl: number;
}

function resolveAgentName(): string {
  if (process.env.POSTINO_AGENT_NAME) return process.env.POSTINO_AGENT_NAME;

  const sessionId = process.env.TERM_SESSION_ID || process.env.ITERM_SESSION_ID;
  if (sessionId) {
    const short = sessionId.split(":").pop()?.slice(0, 8) ?? "";
    return `agent-${short}`;
  }

  return `agent-${process.pid}`;
}

export function loadConfig(): Config {
  return {
    valkeyUrl: process.env.POSTINO_VALKEY_URL ?? "redis://127.0.0.1:6379",
    webPort: parseInt(process.env.POSTINO_WEB_PORT ?? "3333", 10),
    webEnabled: process.env.POSTINO_WEB_ENABLED !== "false",
    keyPrefix: process.env.POSTINO_KEY_PREFIX ?? "po:",
    agentName: resolveAgentName(),
    msgTtl: parseInt(process.env.POSTINO_MSG_TTL ?? "86400", 10),
  };
}
