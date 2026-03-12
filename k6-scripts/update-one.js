/**
 * K6 script: concurrent single-row updates via API.
 * Server picks a random row and updates a safe field (amount — not in any unique index).
 * Writes summary to k6-results/update-one-<timestamp>.json
 * Usage: k6 run --vus 10 --duration 30s k6-scripts/update-one.js
 *        K6_API_URL=http://host:3000 k6 run ...
 */

import http from "k6/http";
import { check } from "k6";
import { sleep } from "k6";

const API_URL = __ENV.K6_API_URL || "http://localhost:3000";
const VUS = __ENV.K6_VUS ? parseInt(__ENV.K6_VUS, 10) : 10;
const DURATION = __ENV.K6_DURATION || "30s";
const HTTP_TIMEOUT = __ENV.K6_HTTP_TIMEOUT || "120s";

/** New value for amount (safe to update — not in unique constraints). */
function randomAmount() {
  return Math.floor(Math.random() * 2000) - 500;
}

export const options = {
  vus: VUS,
  duration: DURATION,
  httpReqTimeout: HTTP_TIMEOUT,
  thresholds: {
    http_req_duration: ["p(95)<5000"],
  },
};

export default function () {
  const payload = JSON.stringify({ amount: randomAmount() });
  const res = http.patch(`${API_URL}/api/update-one`, payload, {
    tags: { name: "update-one" },
    headers: { "Content-Type": "application/json" },
    timeout: HTTP_TIMEOUT,
  });
  check(res, {
    "status 200": (r) => r.status === 200,
    "body ok": (r) => {
      try {
        const b = JSON.parse(r.body);
        return b && b.ok === true;
      } catch (e) {
        return false;
      }
    },
  });
  sleep(0.1);
}

export function handleSummary(data) {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const filename = "k6-results/update-one-" + ts + ".json";
  return {
    [filename]: JSON.stringify(data, null, 2),
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}

function textSummary(data, opts) {
  const indent = (opts && opts.indent) || "";
  let out = "\n" + indent + "Summary\n" + indent + "------\n";
  if (data.metrics) {
    const m = data.metrics;
    if (m.http_reqs) out += indent + "  http_reqs: " + (m.http_reqs.values && m.http_reqs.values.count !== undefined ? m.http_reqs.values.count : 0) + "\n";
    if (m.http_req_duration) {
      const d = m.http_req_duration.values;
      if (d) out += indent + "  http_req_duration avg: " + (d.avg || 0).toFixed(2) + "ms\n";
      if (d) out += indent + "  http_req_duration p95: " + (d["p(95)"] || 0).toFixed(2) + "ms\n";
    }
    if (m.http_req_failed) out += indent + "  http_req_failed: " + (m.http_req_failed.values && m.http_req_failed.values.rate !== undefined ? m.http_req_failed.values.rate : 0) + "\n";
  }
  return out;
}
