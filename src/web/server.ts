import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import type { ServerType } from "@hono/node-server";
import { api, isAuthorizedToken, requestToken } from "./api.js";
import { readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadConfig } from "../types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const config = loadConfig();

// Resolve a file from the public directory (checks dist and src locations)
function findPublicFile(filename: string): string | null {
  const candidates = [
    join(__dirname, "public", filename),
    join(__dirname, "..", "web", "public", filename),
    join(__dirname, "..", "..", "src", "web", "public", filename),
  ];
  for (const p of candidates) {
    if (existsSync(p)) return p;
  }
  return null;
}

const MIME: Record<string, string> = {
  ".css": "text/css",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".json": "application/json",
};

const app = new Hono();

if (config.corsOrigin) app.use("/*", cors({ origin: config.corsOrigin }));

// Mount API
app.route("/api", api);

// Serve static files (favicon, logo, etc.)
app.get("/:file{.+\\.(css|svg|png|ico|json)$}", (c) => {
  if (!isAuthorizedToken(requestToken(c.req)))
    return c.json({ ok: false, error: "Unauthorized" }, 401);
  const file = c.req.param("file");
  if (!/^[a-zA-Z0-9._-]+$/.test(file)) return c.notFound();
  const filePath = findPublicFile(file);
  if (!filePath) return c.notFound();
  const ext = file.substring(file.lastIndexOf("."));
  const content = readFileSync(filePath);
  return c.body(content, {
    headers: { "Content-Type": MIME[ext] || "application/octet-stream" },
  });
});

// Serve the GUI
app.get("/", (c) => {
  const token = requestToken(c.req);
  if (!isAuthorizedToken(token))
    return c.json({ ok: false, error: "Unauthorized" }, 401);
  if (
    config.apiToken &&
    token === new URL(c.req.url).searchParams.get("token")
  ) {
    c.header(
      "Set-Cookie",
      `postino_token=${encodeURIComponent(token)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400`,
    );
  }
  const htmlPath = findPublicFile("index.html");
  if (!htmlPath) {
    return c.text(
      "GUI not found. Make sure index.html is in the web/public directory.",
      500,
    );
  }
  const html = readFileSync(htmlPath, "utf-8");
  return c.html(html.replace("__POSTINO_API_TOKEN__", JSON.stringify("")));
});

const MAX_PORT_ATTEMPTS = 10;

export interface GuiState {
  running: boolean;
  port: number | null;
}

let currentServer: ServerType | null = null;
let currentPort: number | null = null;

export function getGuiState(): GuiState {
  return { running: currentServer !== null, port: currentPort };
}

export function startWebServer(
  port: number,
  attempt = 0,
): Promise<number | null> {
  return new Promise((resolve) => {
    try {
      const server = serve(
        { fetch: app.fetch, port, hostname: config.webHost },
        () => {
          currentServer = server;
          currentPort = port;
          process.stderr.write(
            `postino GUI: http://${config.webHost}:${port}\n`,
          );
          resolve(port);
        },
      );
      server.on("error", (err: NodeJS.ErrnoException) => {
        if (err.code === "EADDRINUSE" && attempt < MAX_PORT_ATTEMPTS) {
          process.stderr.write(
            `postino: port ${port} in use, trying ${port + 1}...\n`,
          );
          startWebServer(port + 1, attempt + 1).then(resolve);
        } else {
          process.stderr.write(`postino GUI failed to start: ${err.message}\n`);
          process.stderr.write(
            `Agent tools are still available, but the web GUI is not.\n`,
          );
          resolve(null);
        }
      });
    } catch (err) {
      process.stderr.write(`postino GUI failed to start: ${err}\n`);
      resolve(null);
    }
  });
}

export function stopWebServer(): Promise<void> {
  const server = currentServer;
  currentServer = null;
  currentPort = null;
  if (!server) return Promise.resolve();
  return new Promise((resolve) => server.close(() => resolve()));
}

export async function restartOnPort(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    try {
      const server = serve(
        { fetch: app.fetch, port, hostname: config.webHost },
        () => {
          const oldServer = currentServer;
          currentServer = server;
          currentPort = port;
          process.stderr.write(
            `postino GUI: takeover http://localhost:${port}\n`,
          );
          if (oldServer && oldServer !== server) oldServer.close();
          resolve(true);
        },
      );
      server.on("error", (err: NodeJS.ErrnoException) => {
        if (err.code === "EADDRINUSE") {
          process.stderr.write(
            `postino: takeover port ${port} already claimed\n`,
          );
        } else {
          process.stderr.write(`postino: takeover failed: ${err.message}\n`);
        }
        // Old server stays running, state unchanged
        resolve(false);
      });
    } catch {
      resolve(false);
    }
  });
}
