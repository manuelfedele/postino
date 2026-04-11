#!/usr/bin/env node

import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { loadConfig } from "./types.js";
import { connect, disconnect, valkey, valkeySub, keys, registerAgent, deregisterAgent, getOnlineAgents } from "./valkey.js";
import { registerMessagingTools } from "./tools/messaging.js";
import { startWebServer, getGuiState, restartOnPort } from "./web/server.js";
import type { Message } from "./types.js";

const config = loadConfig();

const server = new McpServer(
  {
    name: "postino",
    version: "0.1.0",
  },
  {
    capabilities: {
      tools: {},
      resources: {},
      prompts: {},
      logging: {},
    },
  }
);

registerMessagingTools(server, config.agentName);

// --- MCP Resources ---

async function getFormattedAgents() {
  const allAgents = await valkey.smembers(keys.agents());
  const onlineSet = new Set(await getOnlineAgents());
  const result = [];
  for (const name of allAgents.sort()) {
    const msgCount = await valkey.llen(keys.inbox(name));
    result.push({ name, online: onlineSet.has(name), messages: msgCount });
  }
  return result;
}

async function getInboxMessages(agent: string) {
  const raw = await valkey.lrange(keys.inbox(agent), 0, -1);
  return raw.map((r: string) => JSON.parse(r) as Message);
}

server.registerResource("agents", "postino://agents", {
  description: "List of all registered postino agents with online status",
  mimeType: "application/json",
}, async () => ({
  contents: [{
    uri: "postino://agents",
    mimeType: "application/json",
    text: JSON.stringify(await getFormattedAgents()),
  }],
}));

server.registerResource(
  "inbox",
  new ResourceTemplate("postino://inbox/{agent}", {
    list: async () => {
      const agents = await valkey.smembers(keys.agents());
      return {
        resources: agents.sort().map(name => ({
          uri: `postino://inbox/${name}`,
          name: `${name}'s inbox`,
          mimeType: "application/json",
        })),
      };
    },
    complete: {
      agent: async (value) => {
        const agents = await valkey.smembers(keys.agents());
        return agents.filter(a => a.startsWith(value)).sort();
      },
    },
  }),
  {
    description: "Messages in an agent's inbox",
    mimeType: "application/json",
  },
  async (uri, { agent }) => ({
    contents: [{
      uri: uri.href,
      mimeType: "application/json",
      text: JSON.stringify(await getInboxMessages(agent as string)),
    }],
  })
);

// --- MCP Prompts ---

server.registerPrompt("check-messages", {
  description: "Check your postino inbox for new messages and broadcasts",
}, async () => ({
  messages: [{
    role: "user" as const,
    content: {
      type: "text" as const,
      text: "Check my postino inbox for new messages and broadcasts. Use msg_whoami to get a full status overview.",
    },
  }],
}));

server.registerPrompt("send-message", {
  description: "Send a message to another postino agent",
  argsSchema: {
    to: { type: "string", description: "Target agent name" },
    body: { type: "string", description: "Message content" },
  } as any,
}, async (args: any) => ({
  messages: [{
    role: "user" as const,
    content: {
      type: "text" as const,
      text: `Send a message to "${args.to}" with the following content: ${args.body}`,
    },
  }],
}));

function subscribeGuiTakeover(): void {
  const channel = keys.guiTakeoverChannel();

  valkeySub.subscribe(channel).catch(() => {
    process.stderr.write("postino: failed to subscribe to GUI takeover channel\n");
  });

  valkeySub.on("message", (ch: string, message: string) => {
    if (ch !== channel) return;

    let data: { port: number };
    try {
      data = JSON.parse(message);
    } catch {
      return;
    }

    const state = getGuiState();
    // Skip if we already have a GUI on an equal or lower port
    if (state.running && state.port !== null && state.port <= data.port) return;

    // Random jitter (100-500ms) so not all instances race at once
    const jitter = 100 + Math.random() * 400;
    setTimeout(async () => {
      const ok = await restartOnPort(data.port);
      if (ok) {
        process.stderr.write(`postino: took over GUI on port ${data.port}\n`);
      }
    }, jitter);
  });
}

async function main(): Promise<void> {
  try {
    await connect();
  } catch (err) {
    const url = config.valkeyUrl;
    process.stderr.write(`\n  postino: cannot connect to Valkey/Redis at ${url}\n\n`);
    process.stderr.write(`  Postino requires Valkey or Redis. Start one with:\n\n`);
    process.stderr.write(`    docker run -d --name valkey -p 6379:6379 valkey/valkey:8\n\n`);
    process.stderr.write(`  Or install natively:\n\n`);
    process.stderr.write(`    macOS:  brew install valkey && brew services start valkey\n`);
    process.stderr.write(`    Linux:  apt install valkey-server\n\n`);
    process.stderr.write(`  To use a different host/port:\n\n`);
    process.stderr.write(`    POSTINO_VALKEY_URL=redis://host:port\n\n`);
    process.exit(1);
  }

  await registerAgent(config.agentName);

  const transport = new StdioServerTransport();
  await server.connect(transport);

  if (config.webEnabled) {
    startWebServer(config.webPort);
    subscribeGuiTakeover();
  }

  process.stderr.write(`postino agent: ${config.agentName}\n`);

  const shutdown = async () => {
    // If this instance had a GUI, notify others so they can take over the port
    const state = getGuiState();
    if (state.running && state.port !== null) {
      try {
        await valkey.publish(
          keys.guiTakeoverChannel(),
          JSON.stringify({ port: state.port })
        );
      } catch {
        // Best-effort, connection may already be closing
      }
    }

    await deregisterAgent(config.agentName);
    await disconnect();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  process.stderr.write(`postino: ${err.message || err}\n`);
  process.exit(1);
});
