#!/usr/bin/env node

import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const serverPath = join(__dirname, "index.js");
const args = process.argv.slice(2);
const command = args[0];

function printMcpConfig(agentName?: string): void {
  console.log(
    JSON.stringify(
      {
        postino: {
          command: process.execPath,
          args: [serverPath],
          env: agentName ? { POSTINO_AGENT_NAME: agentName } : {},
        },
      },
      null,
      2,
    ),
  );
}

async function serve(): Promise<void> {
  const { connect, disconnect } = await import("./valkey.js");
  const { startWebServer } = await import("./web/server.js");
  const { loadConfig } = await import("./types.js");

  const config = loadConfig();

  try {
    await connect();
  } catch {
    console.error(
      `\n  postino: cannot connect to Valkey/Redis at ${config.valkeyUrl}\n`,
    );
    process.exit(1);
  }

  const actualPort = await startWebServer(config.webPort);
  if (actualPort === null) {
    await disconnect();
    process.exit(1);
  }
  console.log(
    `\n  postino GUI running at http://${config.webHost}:${actualPort}\n`,
  );

  let stopping = false;
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    await disconnect();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

function printHelp(): void {
  console.log(`
  postino - agent-to-agent messaging

  Usage:
    npx postino mcp          Start the MCP server over stdio
    npx postino serve        Run the web interface as a standalone daemon
    npx postino tui          Open the terminal control surface
    npx postino config [--agent NAME]  Print an MCP server configuration entry
    npx postino help         Show this help

  Postino exposes the same messaging system through MCP tools and a REST/SSE
  API, so any compatible agent, process, or user interface can participate.

  MCP clients must register the generated server entry in their own config.
  Docs: https://github.com/manuelfedele/postino
`);
}

switch (command) {
  case "mcp":
  case undefined:
    import("./index.js");
    break;
  case "serve":
    serve();
    break;
  case "tui":
    import("./tui.js").then(({ runTui }) => runTui());
    break;
  case "config":
    printMcpConfig(args[args.indexOf("--agent") + 1]);
    break;
  case "help":
  case "--help":
  case "-h":
    printHelp();
    break;
  default:
    printHelp();
    process.exitCode = 1;
}
