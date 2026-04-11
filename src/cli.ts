#!/usr/bin/env node

import { execSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const command = args[0];

function run(cmd: string): string {
  try {
    return execSync(cmd, { encoding: "utf-8", stdio: ["pipe", "pipe", "pipe"] }).trim();
  } catch (e) {
    const err = e as { stderr?: string; message?: string };
    return err.stderr?.trim() || err.message || "";
  }
}

function hasClaude(): boolean {
  const result = run("which claude");
  return result.length > 0 && !result.includes("not found");
}

function install(): void {
  console.log("\n  postino - message broker for Claude Code agents\n");

  if (!hasClaude()) {
    console.error("  Error: 'claude' CLI not found. Install Claude Code first:");
    console.error("  https://docs.anthropic.com/en/docs/claude-code\n");
    process.exit(1);
  }

  // Find the MCP server entry point (relative to this CLI script)
  const serverPath = join(__dirname, "index.js");

  // Check if already registered
  const existing = run("claude mcp list 2>&1");
  if (existing.includes("postino")) {
    console.log("  Updating existing postino registration...\n");
    run("claude mcp remove postino -s user 2>&1");
  }

  // Register globally (user scope)
  const result = run(`claude mcp add postino -s user -- node ${serverPath}`);

  if (result.includes("Added")) {
    console.log("  Installed successfully!\n");
    console.log("  Restart Claude Code to activate postino.");
    console.log("  Web GUI will be at http://localhost:3333\n");
    console.log("  Prerequisites:");
    console.log("    - Valkey or Redis running on localhost:6379\n");
    console.log("  To uninstall:  npx postino uninstall\n");
  } else {
    console.log("  Registration output:", result);
    console.log("\n  If this failed, try manually:");
    console.log(`  claude mcp add postino -s user -- node ${serverPath}\n`);
  }
}

function uninstall(): void {
  console.log("\n  Removing postino...\n");

  if (!hasClaude()) {
    console.error("  Error: 'claude' CLI not found.\n");
    process.exit(1);
  }

  const result = run("claude mcp remove postino -s user 2>&1");
  if (result.includes("Removed") || result.includes("removed")) {
    console.log("  Uninstalled successfully. Restart Claude Code.\n");
  } else {
    console.log("  " + (result || "postino was not registered.") + "\n");
  }
}

async function serve(): Promise<void> {
  const { connect } = await import("./valkey.js");
  const { startWebServer } = await import("./web/server.js");
  const { loadConfig } = await import("./types.js");

  const config = loadConfig();

  try {
    await connect();
  } catch {
    console.error(`\n  postino: cannot connect to Valkey/Redis at ${config.valkeyUrl}\n`);
    process.exit(1);
  }

  startWebServer(config.webPort);
  console.log(`\n  postino GUI running standalone (no MCP)\n`);

  // Keep the process alive
  process.on("SIGINT", () => process.exit(0));
  process.on("SIGTERM", () => process.exit(0));
}

function printHelp(): void {
  console.log(`
  postino - message broker for Claude Code agents

  Usage:
    npx postino install       Register postino with Claude Code
    npx postino uninstall     Remove postino from Claude Code
    npx postino serve         Run the web GUI standalone (no MCP)

  After installing, restart Claude Code. Your agents will have
  access to messaging, broadcasts, and a web GUI.

  Docs: https://github.com/manuelfedele/postino
`);
}

switch (command) {
  case "install":
    install();
    break;
  case "uninstall":
    uninstall();
    break;
  case "serve":
    serve();
    break;
  case "help":
  case "--help":
  case "-h":
    printHelp();
    break;
  default:
    // No CLI command: start the MCP server (normal operation)
    import("./index.js");
    break;
}
