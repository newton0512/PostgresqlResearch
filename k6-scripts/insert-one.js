/**
 * K6 script: concurrent single-row inserts via API.
 * Writes summary to k6-results/insert-one-<timestamp>.json
 * Usage: k6 run --vus 10 --duration 30s k6-scripts/insert-one.js
 *        K6_API_URL=http://host:3000 k6 run ...
 */

import http from "k6/http";
import { check } from "k6";
import { sleep } from "k6";

const API_URL = __ENV.K6_API_URL || "http://localhost:3000";
const VUS = __ENV.K6_VUS ? parseInt(__ENV.K6_VUS, 10) : 10;
const DURATION = __ENV.K6_DURATION || "30s";

export const options = {
  vus: VUS,
  duration: DURATION,
  thresholds: {
    http_req_duration: ["p(95)<5000"],
    http_req_failed: ["rate<0.1"],
  },
};

export default function () {
  const res = http.post(`${API_URL}/api/insert-one`, null, {
    tags: { name: "insert-one" },
  });
  check(res, {
    "status 201": (r) => r.status === 201,
    "body ok": (r) => {
      try {
        const b = JSON.parse(r.body);
        return b && b.ok === true;
      } catch {
        return false;
      }
    },
  });
  sleep(0.1);
}

export function handleSummary(data) {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  return {
    "k6-results/insert-one-" + ts + ".json": JSON.stringify(data, null, 2),
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}

function textSummary(data, opts) {
  const indent = opts?.indent || "";
  let out = "\n" + indent + "Summary\n" + indent + "------\n";
  if (data.metrics) {
    const m = data.metrics;
    if (m.http_reqs) out += indent + "  http_reqs: " + (m.http_reqs.values?.count || 0) + "\n";
    if (m.http_req_duration) {
      const d = m.http_req_duration.values;
      if (d) out += indent + "  http_req_duration avg: " + (d.avg || 0).toFixed(2) + "ms\n";
      if (d) out += indent + "  http_req_duration p95: " + (d["p(95)"] || 0).toFixed(2) + "ms\n";
    }
    if (m.http_req_failed) out += indent + "  http_req_failed: " + (m.http_req_failed.values?.rate ?? 0) + "\n";
  }
  return out;
}
