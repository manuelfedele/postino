import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { api } from "./api.js";
import { readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

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
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".json": "application/json",
};

const app = new Hono();

app.use("/*", cors());

// Mount API
app.route("/api", api);

// Serve static files (favicon, logo, etc.)
app.get("/:file{.+\\.(svg|png|ico|json)$}", (c) => {
  const file = c.req.param("file");
  const filePath = findPublicFile(file);
  if (!filePath) return c.notFound();
  const ext = file.substring(file.lastIndexOf("."));
  const content = readFileSync(filePath);
  return c.body(content, { headers: { "Content-Type": MIME[ext] || "application/octet-stream" } });
});

// Serve the GUI
app.get("/", (c) => {
  const htmlPath = findPublicFile("index.html");
  if (!htmlPath) {
    return c.text("GUI not found. Make sure index.html is in the web/public directory.", 500);
  }
  const html = readFileSync(htmlPath, "utf-8");
  return c.html(html);
});

const MAX_PORT_ATTEMPTS = 10;

export function startWebServer(port: number, attempt = 0): void {
  try {
    const server = serve({ fetch: app.fetch, port }, () => {
      process.stderr.write(`postino GUI: http://localhost:${port}\n`);
    });
    server.on("error", (err: NodeJS.ErrnoException) => {
      if (err.code === "EADDRINUSE" && attempt < MAX_PORT_ATTEMPTS) {
        process.stderr.write(`postino: port ${port} in use, trying ${port + 1}...\n`);
        startWebServer(port + 1, attempt + 1);
      } else {
        process.stderr.write(`postino GUI failed to start: ${err.message}\n`);
        process.stderr.write(`MCP tools are still available, but the web GUI is not.\n`);
      }
    });
  } catch (err) {
    process.stderr.write(`postino GUI failed to start: ${err}\n`);
  }
}
