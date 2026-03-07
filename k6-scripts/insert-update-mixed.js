/**
 * K6 script: mixed load — concurrent inserts and updates via API.
 * Ratio is configured by scenario VUs: insert_vus vs update_vus (or by weight).
 * Writes summary to k6-results/insert-update-mixed-<timestamp>.json
 *
 * Usage:
 *   k6 run --duration 30s k6-scripts/insert-update-mixed.js
 *   K6_API_URL=http://host:3000 K6_INSERT_VUS=25 K6_UPDATE_VUS=25 k6 run --duration 1m k6-scripts/insert-update-mixed.js
 *   K6_INSERT_WEIGHT=70 K6_UPDATE_WEIGHT=30 k6 run --vus 50 --duration 1m k6-scripts/insert-update-mixed.js
 *
 * Env:
 *   K6_API_URL, K6_DURATION — same as insert-one/update-one
 *   K6_VUS — total VUs (default 50); used with INSERT_WEIGHT/UPDATE_WEIGHT if set
 *   K6_INSERT_VUS, K6_UPDATE_VUS — VUs per scenario (override weight)
 *   K6_INSERT_WEIGHT, K6_UPDATE_WEIGHT — e.g. 50 and 50 → 50% insert, 50% update (used when INSERT_VUS/UPDATE_VUS not set)
 */

import http from "k6/http";
import { check } from "k6";
import { sleep } from "k6";

const API_URL = __ENV.K6_API_URL || "http://localhost:3000";
const DURATION = __ENV.K6_DURATION || "30s";
const TOTAL_VUS = __ENV.K6_VUS ? parseInt(__ENV.K6_VUS, 10) : 50;
const INSERT_WEIGHT = __ENV.K6_INSERT_WEIGHT ? parseInt(__ENV.K6_INSERT_WEIGHT, 10) : 50;
const UPDATE_WEIGHT = __ENV.K6_UPDATE_WEIGHT ? parseInt(__ENV.K6_UPDATE_WEIGHT, 10) : 50;
const INSERT_VUS_ENV = __ENV.K6_INSERT_VUS;
const UPDATE_VUS_ENV = __ENV.K6_UPDATE_VUS;

const insertVus = INSERT_VUS_ENV != null ? parseInt(INSERT_VUS_ENV, 10) : Math.round((TOTAL_VUS * INSERT_WEIGHT) / (INSERT_WEIGHT + UPDATE_WEIGHT));
const updateVus = UPDATE_VUS_ENV != null ? parseInt(UPDATE_VUS_ENV, 10) : TOTAL_VUS - insertVus;

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
    insert: {
      executor: "constant-vus",
      vus: Math.max(1, insertVus),
      duration: DURATION,
      exec: "doInsert",
      startTime: "0s",
    },
    update: {
      executor: "constant-vus",
      vus: Math.max(1, updateVus),
      duration: DURATION,
      exec: "doUpdate",
      startTime: "0s",
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<5000"],
  },
};

export function doInsert() {
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

export function doUpdate() {
  const payload = JSON.stringify({ amount: Math.floor(Math.random() * 2000) - 500 });
  const res = http.patch(`${API_URL}/api/update-one`, payload, {
    tags: { name: "update-one" },
    headers: { "Content-Type": "application/json" },
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
  const filename = "k6-results/insert-update-mixed-" + ts + ".json";
  return {
    [filename]: JSON.stringify(data, null, 2),
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}

function textSummary(data, opts) {
  const indent = (opts && opts.indent) || "";
  let out = "\n" + indent + "Summary (mixed insert + update)\n" + indent + "------\n";
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
