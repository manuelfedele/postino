import { execFileSync } from "node:child_process";
import { cp, rm } from "node:fs/promises";

await rm("dist", { recursive: true, force: true });
const tsc = process.platform === "win32" ? "node_modules/.bin/tsc.cmd" : "node_modules/.bin/tsc";
execFileSync(tsc, [], { stdio: "inherit" });
await cp("src/web/public", "dist/web/public", { recursive: true, force: true });
