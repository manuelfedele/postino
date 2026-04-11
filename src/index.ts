#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { loadConfig } from "./types.js";
import { connect, disconnect, valkey, valkeySub, keys, registerAgent, deregisterAgent } from "./valkey.js";
import { registerMessagingTools } from "./tools/messaging.js";
import { startWebServer, getGuiState, restartOnPort } from "./web/server.js";

const config = loadConfig();

const server = new McpServer(
  {
    name: "postino",
    version: "0.1.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

registerMessagingTools(server, config.agentName);

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
