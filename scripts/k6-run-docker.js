#!/usr/bin/env node
/**
 * Run K6 insert-one script via Docker (no local k6 install needed).
 * Usage: node scripts/k6-run-docker.js
 *        K6_API_URL=http://94.26.236.51:3000 node scripts/k6-run-docker.js
 *        K6_API_URL=... K6_VUS=20 K6_DURATION=60s node scripts/k6-run-docker.js
 *
 * Requires: Docker. Passes K6_API_URL, K6_VUS, K6_DURATION from env.
 */
import { mkdirSync } from "fs";
import { spawnSync } from "child_process";
import { join } from "path";

const cwd = process.cwd();
const env = { ...process.env };
if (!env.K6_API_URL) {
  console.error("Set K6_API_URL (e.g. export K6_API_URL=http://94.26.236.51:3000)");
  process.exit(1);
}

// Ensure k6-results exists so the container can write the summary JSON
mkdirSync(join(cwd, "k6-results"), { recursive: true });

// Bind-mount project so k6 can read script and write k6-results/
const volume = `${cwd}:/work`;

const args = [
  "run",
  "--rm",
  "-v",
  volume,
  "-w",
  "/work",
  "-e",
  `K6_API_URL=${env.K6_API_URL}`,
  "-e",
  `K6_VUS=${env.K6_VUS || "10"}`,
  "-e",
  `K6_DURATION=${env.K6_DURATION || "30s"}`,
  "grafana/k6:latest",
  "run",
  "k6-scripts/insert-one.js",
];

console.log("Running: docker", args.join(" "));
const r = spawnSync("docker", args, { stdio: "inherit", cwd });
process.exit(r.status ?? 1);
