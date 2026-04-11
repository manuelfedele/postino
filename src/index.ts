#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { loadConfig } from "./types.js";
import { connect, disconnect, registerAgent, deregisterAgent } from "./valkey.js";
import { registerMessagingTools } from "./tools/messaging.js";
import { startWebServer } from "./web/server.js";

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

async function main(): Promise<void> {
  await connect();
  await registerAgent(config.agentName);

  const transport = new StdioServerTransport();
  await server.connect(transport);

  if (config.webEnabled) {
    startWebServer(config.webPort);
  }

  process.stderr.write(`postino agent: ${config.agentName}\n`);

  const shutdown = async () => {
    await deregisterAgent(config.agentName);
    await disconnect();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("Failed to start postino:", err);
  process.exit(1);
});
