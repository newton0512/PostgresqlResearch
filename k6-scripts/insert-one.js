/**
 * K6 script: concurrent single-row inserts via API.
 * Sends a partial record (main fields); API merges with defaults and inserts into bench.bonus_registry_*.
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

/** Build partial row payload (main fields). API fills the rest and inserts. */
function buildPartialRow() {
  const now = new Date();
  const iso = now.toISOString().slice(0, 10);
  const id = "k6-" + __VU + "-" + now.getTime() + "-" + Math.floor(Math.random() * 10000);
  return {
    id: id,
    date: now.toISOString().slice(0, 19).replace("T", " "),
    amount: Math.floor(Math.random() * 2000) - 500,
    accounted_for_bs_profile_id: "k6-profile-" + (__VU % 10),
    bs_profile_id: "k6-bs-" + (__VU % 5),
    bonus_type_id: __VU % 2 === 0 ? "premial" : "qualification",
    cancelled: false,
    doc_to_track_id: "k6-doc-" + id.slice(0, 12),
    doc_to_track_type_id: "bsBonusDocument",
    registrar_type_id: "bsBonusDocument",
    registrar_id: "k6-reg-" + __VU,
    row: 1,
    action_source_id: "operator",
    date_of_expire: iso,
    doc_to_track_date: iso,
  };
}

export const options = {
  vus: VUS,
  duration: DURATION,
  thresholds: {
    http_req_duration: ["p(95)<5000"],
  },
};

export default function () {
  const payload = JSON.stringify(buildPartialRow());
  const res = http.post(`${API_URL}/api/insert-one`, payload, {
    tags: { name: "insert-one" },
    headers: { "Content-Type": "application/json" },
  });
  check(res, {
    "status 201": (r) => r.status === 201,
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
  const filename = "k6-results/insert-one-" + ts + ".json";
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
