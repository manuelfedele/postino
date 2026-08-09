#!/usr/bin/env node

import {
  McpServer,
  ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { loadConfig } from "./types.js";
import {
  connect,
  disconnect,
  getInboxCount,
  getOnlineAgents,
  keys,
  listInbox,
  registerAgent,
  deregisterAgent,
  valkey,
  valkeySub,
} from "./valkey.js";
import { registerMessagingTools } from "./tools/messaging.js";
import { VERSION } from "./web/api.js";
import {
  getGuiState,
  restartOnPort,
  startWebServer,
  stopWebServer,
} from "./web/server.js";

const config = loadConfig();
let currentAgentName = config.agentName;
const instanceId = crypto.randomUUID();

const server = new McpServer(
  {
    name: "postino",
    version: VERSION,
  },
  {
    capabilities: {
      tools: {},
      resources: {},
      prompts: {},
      logging: {},
    },
  },
);

registerMessagingTools(server, config.agentName, (name) => {
  currentAgentName = name;
});

// --- MCP Resources ---

async function getFormattedAgents() {
  const allAgents = await valkey.smembers(keys.agents());
  const onlineSet = new Set(await getOnlineAgents());
  const result = [];
  for (const name of allAgents.sort()) {
    const msgCount = await getInboxCount(name);
    result.push({ name, online: onlineSet.has(name), messages: msgCount });
  }
  return result;
}

async function getInboxMessages(agent: string) {
  if (agent !== currentAgentName)
    throw new Error("Agents may only inspect their own inbox");
  return (await listInbox(agent, 0, config.maxInbox)).items;
}

server.registerResource(
  "agents",
  "postino://agents",
  {
    description: "List of all registered agents with online status",
    mimeType: "application/json",
  },
  async () => ({
    contents: [
      {
        uri: "postino://agents",
        mimeType: "application/json",
        text: JSON.stringify(await getFormattedAgents()),
      },
    ],
  }),
);

server.registerResource(
  "inbox",
  new ResourceTemplate("postino://inbox/{agent}", {
    list: async () => {
      return {
        resources: [currentAgentName].map((name) => ({
          uri: `postino://inbox/${name}`,
          name: `${name}'s inbox`,
          mimeType: "application/json",
        })),
      };
    },
    complete: {
      agent: async (value) => {
        return [currentAgentName].filter((a) => a.startsWith(value)).sort();
      },
    },
  }),
  {
    description: "Messages in an agent's inbox",
    mimeType: "application/json",
  },
  async (uri, { agent }) => ({
    contents: [
      {
        uri: uri.href,
        mimeType: "application/json",
        text: JSON.stringify(await getInboxMessages(agent as string)),
      },
    ],
  }),
);

// --- MCP Prompts ---

server.registerPrompt(
  "check-messages",
  {
    description: "Check your postino inbox for new messages and broadcasts",
  },
  async () => ({
    messages: [
      {
        role: "user" as const,
        content: {
          type: "text" as const,
          text: "Check my postino inbox for new messages and broadcasts. Use msg_whoami, msg_read, and msg_ack to process activity safely.",
        },
      },
    ],
  }),
);

server.registerPrompt(
  "send-message",
  {
    description: "Send a message to another postino agent",
    argsSchema: {
      to: { type: "string", description: "Target agent name" },
      body: { type: "string", description: "Message content" },
    } as any,
  },
  async (args: any) => ({
    messages: [
      {
        role: "user" as const,
        content: {
          type: "text" as const,
          text: `Send a message to "${args.to}" with the following content: ${args.body}`,
        },
      },
    ],
  }),
);

function subscribeGuiTakeover(): void {
  const channel = keys.guiTakeoverChannel();

  valkeySub.subscribe(channel).catch(() => {
    process.stderr.write(
      "postino: failed to subscribe to GUI takeover channel\n",
    );
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
    process.stderr.write(
      `\n  postino: cannot connect to Valkey/Redis at ${url}\n\n`,
    );
    process.stderr.write(
      `  Postino requires Valkey or Redis. Start one with:\n\n`,
    );
    process.stderr.write(
      `    docker run -d --name valkey -p 6379:6379 valkey/valkey:8\n\n`,
    );
    process.stderr.write(`  Or install natively:\n\n`);
    process.stderr.write(
      `    macOS:  brew install valkey && brew services start valkey\n`,
    );
    process.stderr.write(`    Linux:  apt install valkey-server\n\n`);
    process.stderr.write(`  To use a different host/port:\n\n`);
    process.stderr.write(`    POSTINO_VALKEY_URL=redis://host:port\n\n`);
    process.exit(1);
  }

  await registerAgent(config.agentName, instanceId);

  const transport = new StdioServerTransport();
  await server.connect(transport);

  if (config.webEnabled) {
    await startWebServer(config.webPort);
    subscribeGuiTakeover();
  }

  process.stderr.write(`postino agent: ${config.agentName}\n`);

  let stopping = false;
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    // If this instance had a GUI, notify others so they can take over the port
    const state = getGuiState();
    if (state.running && state.port !== null) {
      try {
        await stopWebServer();
        await valkey.publish(
          keys.guiTakeoverChannel(),
          JSON.stringify({ port: state.port }),
        );
      } catch {
        // Best-effort, connection may already be closing
      }
    }

    try {
      await deregisterAgent(currentAgentName, instanceId);
    } finally {
      await disconnect().catch(() => {});
      process.exit(0);
    }
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  process.stderr.write(`postino: ${err.message || err}\n`);
  process.exit(1);
});
