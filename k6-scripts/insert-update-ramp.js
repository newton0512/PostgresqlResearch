/**
 * K6 script: mixed insert + update (50/50) with ramping VUs.
 * Schedule:
 *   20 min — 50 VUs
 *   20 min — 100 VUs
 *   20 min — 150 VUs
 *   5 min  — 200 VUs
 *   1 min  — 300 VUs
 *   14 min — 15 VUs
 * Total: 80 min.
 *
 * Usage: K6_API_URL=http://host:3000 pnpm run k6:insert-update-ramp
 */

import http from "k6/http";
import { check } from "k6";
import { sleep } from "k6";

const API_URL = __ENV.K6_API_URL || "http://localhost:3000";
const HTTP_TIMEOUT = __ENV.K6_HTTP_TIMEOUT || "120s";

function buildPartialRow() {
  const now = new Date();
  const iso = now.toISOString().slice(0, 10);
  const suffix = now.getTime() + "-" + Math.floor(Math.random() * 10000);
  const id = "k6-" + __VU + "-" + suffix;
  const registrarId = "k6-reg-" + __VU + "-" + suffix;
  const docToTrackId = "k6-doc-" + id.slice(0, 20);
  return {
    id,
    date: now.toISOString().slice(0, 19).replace("T", " "),
    amount: Math.floor(Math.random() * 2000) - 500,
    accounted_for_bs_profile_id: "k6-profile-" + (__VU % 10),
    bs_profile_id: "k6-bs-" + (__VU % 5),
    bonus_type_id: __VU % 2 === 0 ? "premial" : "qualification",
    cancelled: false,
    doc_to_track_id: docToTrackId,
    doc_to_track_type_id: "bsBonusDocument",
    registrar_type_id: "bsBonusDocument",
    registrar_id: registrarId,
    row: 1,
    action_source_id: "operator",
    date_of_expire: iso,
    doc_to_track_date: iso,
  };
}

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "20m", target: 50 },
        { duration: "20m", target: 100 },
        { duration: "20m", target: 150 },
        { duration: "5m", target: 200 },
        { duration: "1m", target: 300 },
        { duration: "14m", target: 15 },
      ],
      gracefulRampDown: "30s",
      exec: "default",
    },
  },
  httpReqTimeout: HTTP_TIMEOUT,
  thresholds: {
    http_req_duration: ["p(95)<5000"],
  },
};

export default function () {
  if (Math.random() < 0.5) {
    doInsert();
  } else {
    doUpdate();
  }
}

function doInsert() {
  const payload = JSON.stringify(buildPartialRow());
  const res = http.post(`${API_URL}/api/insert-one`, payload, {
    tags: { name: "insert-one" },
    headers: { "Content-Type": "application/json" },
  });
  check(res, {
    "insert status 201": (r) => r.status === 201,
    "insert body ok": (r) => {
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

function doUpdate() {
  const payload = JSON.stringify({ amount: Math.floor(Math.random() * 2000) - 500 });
  const res = http.patch(`${API_URL}/api/update-one`, payload, {
    tags: { name: "update-one" },
    headers: { "Content-Type": "application/json" },
    timeout: HTTP_TIMEOUT,
  });
  check(res, {
    "update status 200": (r) => r.status === 200,
    "update body ok": (r) => {
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
  const filename = "k6-results/insert-update-ramp-" + ts + ".json";
  return {
    [filename]: JSON.stringify(data, null, 2),
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}

function textSummary(data, opts) {
  const indent = (opts && opts.indent) || "";
  let out = "\n" + indent + "Summary (insert-update ramp, 80 min)\n" + indent + "------\n";
  if (data.metrics) {
    const m = data.metrics;
    if (m.http_reqs) out += indent + "  http_reqs: " + (m.http_reqs.values?.count ?? 0) + "\n";
    if (m.http_req_duration?.values) {
      const d = m.http_req_duration.values;
      out += indent + "  http_req_duration avg: " + (d.avg || 0).toFixed(2) + "ms\n";
      out += indent + "  http_req_duration p95: " + (d["p(95)"] || 0).toFixed(2) + "ms\n";
    }
    if (m.http_req_failed?.values) out += indent + "  http_req_failed rate: " + (m.http_req_failed.values.rate ?? 0) + "\n";
  }
  return out;
}
