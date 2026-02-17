/**
 * Queries benchmark: run standard queries, write results to file.
 * Usage: pnpm run bench:queries [--table plain|part|idx|idx_part] [--runs 5]
 */

import "dotenv/config";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import postgres from "postgres";
import { BasicAuth, Trino } from "trino-client";
import { config } from "../src/config.js";
import { getTableName, TABLE_VARIANTS, type TableVariant } from "../src/bench/create-tables.js";

const DEFAULT_RUNS = 5;
const DISCOVERY_LIMIT = 50;

interface QueryDef {
  id: number;
  name: string;
  sql: string;
  params: (string | number)[];
}

/** Prepared params for running queries with different values per run (from discovery + optional UPDATEs). */
export interface PreparedParams {
  q1Rows: (string | number)[][];
  profileIds: string[];
  bsProfileIds: string[];
  q5Rows: [string, string][];
  q6Rows: [string, string][];
  registrarTypeIds: string[];
  q8Rows: [string, string][];
  q9Rows: [number, number][];
  ids: string[];
}

function parseArgs(): { table: TableVariant; runs: number } {
  const args = process.argv.slice(2);
  let table = config.bench.tableVariant as TableVariant;
  let runs = DEFAULT_RUNS;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--table" && args[i + 1] && TABLE_VARIANTS.includes(args[i + 1] as TableVariant)) {
      table = args[++i] as TableVariant;
    } else if (args[i] === "--runs" && args[i + 1]) {
      runs = Math.max(1, Number(args[++i]));
    }
  }
  return { table, runs };
}

function buildQueryDefs(fullTable: string, params: { profileId: string; bsProfileId: string; id: string }): QueryDef[] {
  const T = fullTable;
  const p = params;
  return [
    { id: 1, name: "Списания по документу", sql: `SELECT amount FROM ${T} WHERE doc_to_track_id = $1 AND doc_to_track_type_id = $2 AND accounted_for_bs_profile_id = $3 AND bonus_type_id = $4 AND amount < 0 AND cancelled = false AND (date_of_expire IS NULL OR date_of_expire >= $5)`, params: ["bench_doc_1", "bench_type_1", p.profileId, "premial", "2099-01-01"] },
    { id: 2, name: "Пагинация по профилю", sql: `SELECT * FROM ${T} WHERE accounted_for_bs_profile_id = $1 ORDER BY "date" DESC OFFSET $2 LIMIT $3`, params: [p.profileId, 0, 20] },
    { id: 3, name: "Записи по профилю без отменённых", sql: `SELECT * FROM ${T} WHERE accounted_for_bs_profile_id = $1 AND cancelled = false`, params: [p.profileId] },
    { id: 4, name: "Записи по bs_profile_id", sql: `SELECT * FROM ${T} WHERE bs_profile_id = $1 AND cancelled = false`, params: [p.bsProfileId] },
    { id: 5, name: "Профиль + тип бонуса", sql: `SELECT * FROM ${T} WHERE accounted_for_bs_profile_id = $1 AND cancelled = false AND bonus_type_id = $2`, params: [p.profileId, "premial"] },
    { id: 6, name: "Действующие по дате", sql: `SELECT * FROM ${T} WHERE accounted_for_bs_profile_id = $1 AND date_of_expire >= $2`, params: [p.profileId, "2020-01-01"] },
    { id: 7, name: "GROUP BY bs_quota_id", sql: `SELECT bs_quota_id, COUNT(bs_quota_id) FROM ${T} WHERE registrar_type_id = $1 AND cancelled = false AND bs_quota_id IS NOT NULL AND "row" = 1 GROUP BY bs_quota_id`, params: ["bsBonusDocument"] },
    { id: 8, name: "По документу (registrar)", sql: `SELECT * FROM ${T} WHERE registrar_type_id = $1 AND registrar_id = $2`, params: ["bsBonusDocument", "bench_reg_1"] },
    { id: 9, name: "Общая пагинация", sql: `SELECT * FROM ${T} ORDER BY "date" DESC OFFSET $1 LIMIT $2`, params: [0, 20] },
    { id: 10, name: "По id", sql: `SELECT * FROM ${T} WHERE id = $1 LIMIT 1`, params: [p.id] },
  ];
}

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? (s[m] ?? 0) : ((s[m - 1] ?? 0) + (s[m] ?? 0)) / 2;
}

async function discoverParamsPostgres(fullTable: string): Promise<{ profileId: string; bsProfileId: string; id: string }> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  let profileId = "bench_profile_1";
  let bsProfileId = "bench_bs_1";
  let id = "";
  try {
    const row = await sql.unsafe<{ accounted_for_bs_profile_id: string; bs_profile_id: string; id: string }[]>(
      `SELECT accounted_for_bs_profile_id, bs_profile_id, id FROM ${fullTable} LIMIT 1`
    );
    if (row[0]) {
      profileId = row[0].accounted_for_bs_profile_id ?? profileId;
      bsProfileId = row[0].bs_profile_id ?? bsProfileId;
      id = row[0].id ?? id;
    }
  } finally {
    await sql.end();
  }
  return { profileId, bsProfileId, id };
}

function getParamsForRun(prep: PreparedParams, queryId: number, runIndex: number): (string | number)[] {
  switch (queryId) {
    case 1: {
      const row = prep.q1Rows[runIndex % prep.q1Rows.length];
      return row ? [...row] : [];
    }
    case 2: {
      const profileId = prep.profileIds[runIndex % prep.profileIds.length];
      return [profileId ?? "bench", (runIndex * 10) % 5000, 20];
    }
    case 3:
      return [prep.profileIds[runIndex % prep.profileIds.length] ?? "bench"];
    case 4:
      return [prep.bsProfileIds[runIndex % prep.bsProfileIds.length] ?? "bench"];
    case 5: {
      const pair = prep.q5Rows[runIndex % prep.q5Rows.length];
      return pair ? [pair[0], pair[1]] : ["bench", "premial"];
    }
    case 6: {
      const pair = prep.q6Rows[runIndex % prep.q6Rows.length];
      return pair ? [pair[0], pair[1]] : ["bench", "2020-01-01"];
    }
    case 7:
      return [prep.registrarTypeIds[runIndex % prep.registrarTypeIds.length] ?? "bsBonusDocument"];
    case 8: {
      const pair = prep.q8Rows[runIndex % prep.q8Rows.length];
      return pair ? [pair[0], pair[1]] : ["bsBonusDocument", "bench_reg_1"];
    }
    case 9: {
      const pair = prep.q9Rows[runIndex % prep.q9Rows.length];
      return pair ? [pair[1], pair[0]] : [0, 20];
    }
    case 10: {
      const id = prep.ids.length > 0 ? prep.ids[runIndex % prep.ids.length] : "";
      return [id ?? ""];
    }
    default:
      return [];
  }
}

async function runPreparationPostgres(
  fullTable: string,
  skipUpdates: boolean,
  log: (msg: string) => void
): Promise<PreparedParams> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  const profileIds: string[] = [];
  const bsProfileIds: string[] = [];
  const q1Rows: (string | number)[][] = [];
  const q5Rows: [string, string][] = [];
  const q6Rows: [string, string][] = [];
  const registrarTypeIds: string[] = [];
  const q8Rows: [string, string][] = [];
  const ids: string[] = [];

  try {
    log("Preparation: starting discovery queries...");

    log("  Discovery: accounted_for_bs_profile_id (cancelled = false)...");
    const rProfiles = await sql.unsafe<{ accounted_for_bs_profile_id: string }[]>(
      `SELECT DISTINCT accounted_for_bs_profile_id FROM ${fullTable} WHERE cancelled = false LIMIT ${DISCOVERY_LIMIT}`
    );
    for (const r of rProfiles) if (r.accounted_for_bs_profile_id != null) profileIds.push(r.accounted_for_bs_profile_id);
    log(`    Found ${profileIds.length} profile id(s).`);

    log("  Discovery: bs_profile_id (cancelled = false)...");
    const rBs = await sql.unsafe<{ bs_profile_id: string }[]>(
      `SELECT DISTINCT bs_profile_id FROM ${fullTable} WHERE cancelled = false LIMIT ${DISCOVERY_LIMIT}`
    );
    for (const r of rBs) if (r.bs_profile_id != null) bsProfileIds.push(r.bs_profile_id);
    log(`    Found ${bsProfileIds.length} bs_profile id(s).`);

    log("  Discovery: rows for query 1 (doc + amount < 0, cancelled = false)...");
    const rQ1 = await sql.unsafe<
      { doc_to_track_id: string; doc_to_track_type_id: string; accounted_for_bs_profile_id: string; bonus_type_id: string; date_of_expire: Date | null }[]
    >(
      `SELECT doc_to_track_id, doc_to_track_type_id, accounted_for_bs_profile_id, bonus_type_id, date_of_expire FROM ${fullTable} WHERE amount < 0 AND cancelled = false LIMIT ${DISCOVERY_LIMIT}`
    );
    for (const r of rQ1) {
      if (r.doc_to_track_id != null && r.doc_to_track_type_id != null && r.accounted_for_bs_profile_id != null && r.bonus_type_id != null)
        q1Rows.push([r.doc_to_track_id, r.doc_to_track_type_id, r.accounted_for_bs_profile_id, r.bonus_type_id, r.date_of_expire ? r.date_of_expire.toISOString().slice(0, 10) : "2099-01-01"]);
    }
    log(`    Found ${q1Rows.length} row(s) for query 1.`);

    log("  Discovery: (profile_id, bonus_type_id) for query 5...");
    const rQ5 = await sql.unsafe<{ accounted_for_bs_profile_id: string; bonus_type_id: string }[]>(
      `SELECT DISTINCT accounted_for_bs_profile_id, bonus_type_id FROM ${fullTable} WHERE cancelled = false LIMIT ${DISCOVERY_LIMIT}`
    );
    for (const r of rQ5) if (r.accounted_for_bs_profile_id != null && r.bonus_type_id != null) q5Rows.push([r.accounted_for_bs_profile_id, r.bonus_type_id]);
    log(`    Found ${q5Rows.length} pair(s) for query 5.`);

    log("  Discovery: (profile_id, date_of_expire) for query 6...");
    const rQ6 = await sql.unsafe<{ accounted_for_bs_profile_id: string; date_of_expire: Date | null }[]>(
      `SELECT DISTINCT accounted_for_bs_profile_id, date_of_expire FROM ${fullTable} WHERE date_of_expire IS NOT NULL LIMIT ${DISCOVERY_LIMIT}`
    );
    for (const r of rQ6) if (r.accounted_for_bs_profile_id != null && r.date_of_expire != null) q6Rows.push([r.accounted_for_bs_profile_id, r.date_of_expire.toISOString().slice(0, 10)]);
    log(`    Found ${q6Rows.length} pair(s) for query 6.`);

    log("  Discovery: registrar_type_id for query 7...");
    const rQ7 = await sql.unsafe<{ registrar_type_id: string }[]>(
      `SELECT DISTINCT registrar_type_id FROM ${fullTable} WHERE cancelled = false AND bs_quota_id IS NOT NULL AND "row" = 1 LIMIT ${DISCOVERY_LIMIT}`
    );
    for (const r of rQ7) if (r.registrar_type_id != null) registrarTypeIds.push(r.registrar_type_id);
    log(`    Found ${registrarTypeIds.length} registrar_type_id(s) for query 7.`);

    log("  Discovery: (registrar_type_id, registrar_id) for query 8...");
    const rQ8 = await sql.unsafe<{ registrar_type_id: string; registrar_id: string }[]>(
      `SELECT DISTINCT registrar_type_id, registrar_id FROM ${fullTable} WHERE registrar_type_id IS NOT NULL AND registrar_id IS NOT NULL LIMIT ${DISCOVERY_LIMIT}`
    );
    for (const r of rQ8) if (r.registrar_type_id != null && r.registrar_id != null) q8Rows.push([r.registrar_type_id, r.registrar_id]);
    log(`    Found ${q8Rows.length} pair(s) for query 8.`);

    log("  Discovery: id for query 10...");
    const rIds = await sql.unsafe<{ id: string }[]>(`SELECT id FROM ${fullTable} LIMIT ${DISCOVERY_LIMIT}`);
    for (const r of rIds) if (r.id != null) ids.push(r.id);
    log(`    Found ${ids.length} id(s) for query 10.`);

    if (!skipUpdates && ids.length > 0) {
      if (q1Rows.length === 0) {
        log("  Update: no rows for query 1; updating one row to satisfy query 1...");
        await sql.unsafe(
          `UPDATE ${fullTable} SET amount = -1, cancelled = false, date_of_expire = DATE '2099-01-01', doc_to_track_id = 'bench_doc_1', doc_to_track_type_id = 'bench_type_1', bonus_type_id = 'premial' WHERE id = $1`,
          [ids[0]!]
        );
        q1Rows.push(["bench_doc_1", "bench_type_1", profileIds[0] ?? "bench_profile_1", "premial", "2099-01-01"]);
        log("    Updated one row.");
      }
      if (registrarTypeIds.length === 0) {
        log("  Update: no rows for query 7; updating a few rows (row=1, bs_quota_id NOT NULL, registrar_type_id)...");
        for (let i = 0; i < Math.min(3, ids.length); i++) {
          await sql.unsafe(
            `UPDATE ${fullTable} SET "row" = 1, bs_quota_id = 1, cancelled = false, registrar_type_id = 'bsBonusDocument' WHERE id = $1`,
            [ids[i]!]
          );
        }
        registrarTypeIds.push("bsBonusDocument");
        log("    Updated up to 3 rows.");
      }
      if (q8Rows.length === 0) {
        log("  Update: no rows for query 8; updating one row (registrar_type_id, registrar_id)...");
        await sql.unsafe(
          `UPDATE ${fullTable} SET registrar_type_id = 'bsBonusDocument', registrar_id = 'bench_reg_1' WHERE id = $1`,
          [ids[0]!]
        );
        q8Rows.push(["bsBonusDocument", "bench_reg_1"]);
        log("    Updated one row.");
      }
    }

    const q9Rows: [number, number][] = [];
    const limits = [10, 20, 50];
    const offsets = [0, 100, 500, 1000, 2000];
    for (let i = 0; i < Math.max(limits.length, offsets.length) * 2; i++) {
      q9Rows.push([limits[i % limits.length]!, offsets[i % offsets.length]!]);
    }

    return {
      q1Rows: q1Rows.length > 0 ? q1Rows : [["bench_doc_1", "bench_type_1", "bench_profile_1", "premial", "2099-01-01"]],
      profileIds: profileIds.length > 0 ? profileIds : ["bench_profile_1"],
      bsProfileIds: bsProfileIds.length > 0 ? bsProfileIds : ["bench_bs_1"],
      q5Rows: q5Rows.length > 0 ? q5Rows : [[profileIds[0] ?? "bench_profile_1", "premial"]],
      q6Rows: q6Rows.length > 0 ? q6Rows : [[profileIds[0] ?? "bench_profile_1", "2020-01-01"]],
      registrarTypeIds: registrarTypeIds.length > 0 ? registrarTypeIds : ["bsBonusDocument"],
      q8Rows: q8Rows.length > 0 ? q8Rows : [["bsBonusDocument", "bench_reg_1"]],
      q9Rows,
      ids,
    };
  } finally {
    await sql.end();
  }
}

async function runPostgres(
  table: TableVariant,
  runs: number,
  logStream?: NodeJS.WritableStream
): Promise<{ id: number; name: string; min: number; max: number; avg: number; median: number; n: number }[]> {
  const tableName = getTableName(table);
  const fullTable = `"bench"."${tableName}"`;
  const log = (msg: string) => {
    const line = msg.endsWith("\n") ? msg : msg + "\n";
    if (logStream) (logStream as NodeJS.WriteStream).write(line);
    console.log(msg);
  };
  const prepStart = performance.now();
  const prep = await runPreparationPostgres(fullTable, false, log);
  const prepMs = Math.round(performance.now() - prepStart);
  log(`Preparation total: ${prepMs} ms\n`);
  const defs = buildQueryDefs(fullTable, {
    profileId: prep.profileIds[0] ?? "bench_profile_1",
    bsProfileId: prep.bsProfileIds[0] ?? "bench_bs_1",
    id: prep.ids[0] ?? "",
  });
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  const results: { id: number; name: string; min: number; max: number; avg: number; median: number; n: number }[] = [];
  for (const q of defs) {
    const times: number[] = [];
    for (let r = 0; r < runs; r++) {
      const params = getParamsForRun(prep, q.id, r);
      if (q.id === 10 && (params[0] === "" || params[0] == null)) continue;
      const start = performance.now();
      await sql.unsafe(q.sql, params);
      times.push(performance.now() - start);
    }
    if (times.length === 0) continue;
    results.push({
      id: q.id,
      name: q.name,
      min: Math.min(...times),
      max: Math.max(...times),
      avg: times.reduce((a, b) => a + b, 0) / times.length,
      median: median(times),
      n: times.length,
    });
  }
  await sql.end();
  return results;
}

async function runTrino(table: TableVariant, runs: number): Promise<{ id: number; name: string; min: number; max: number; avg: number; median: number; n: number }[]> {
  const trino = Trino.create({
    server: `http://${config.trino.host}:${config.trino.port}`,
    catalog: config.trino.catalog,
    schema: config.trino.schema,
    auth: new BasicAuth(config.trino.user),
  });
  const tableName = getTableName(table);
  const fullTable = `"${config.trino.catalog}"."bench"."${tableName}"`;
  const params = { profileId: "bench_profile_1", bsProfileId: "bench_bs_1", id: "" };
  const qDiscover = await trino.query(`SELECT accounted_for_bs_profile_id, bs_profile_id, id FROM ${fullTable} LIMIT 1`);
  for await (const r of qDiscover) {
    const row = r as { data?: unknown[][]; error?: { message?: string } };
    if (row?.error) break;
    if (row?.data?.[0]) {
      const r0 = row.data[0];
      params.profileId = String(r0[0] ?? params.profileId);
      params.bsProfileId = String(r0[1] ?? params.bsProfileId);
      params.id = String(r0[2] ?? params.id);
    }
  }

  const defs = buildQueryDefs(fullTable, params);
  const esc = (v: string | number): string => {
    if (typeof v === "number") return String(v);
    const s = String(v).replace(/'/g, "''");
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return `DATE '${s}'`;
    return `'${s}'`;
  };
  const results: { id: number; name: string; min: number; max: number; avg: number; median: number; n: number }[] = [];
  for (const q of defs) {
    const times: number[] = [];
    for (let r = 0; r < runs; r++) {
      const sqlStr = q.sql.replace(/\$(\d+)/g, (_, i) => esc(q.params[Number(i) - 1] ?? ""));
      const start = performance.now();
      const qq = await trino.query(sqlStr);
      for await (const row of qq) {
        const err = (row as { error?: { message?: string } }).error;
        if (err) throw new Error(err.message);
      }
      times.push(performance.now() - start);
    }
    results.push({
      id: q.id,
      name: q.name,
      min: Math.min(...times),
      max: Math.max(...times),
      avg: times.reduce((a, b) => a + b, 0) / times.length,
      median: median(times),
      n: times.length,
    });
  }
  return results;
}

const DEFAULT_RUNS_QUERIES = DEFAULT_RUNS;

/** Run queries benchmark and write results to file. If logStream is provided (e.g. from bench:full), appends same output there. Returns path to written file. */
export async function runQueriesBenchmark(
  table: TableVariant,
  runs: number = DEFAULT_RUNS_QUERIES,
  logStream?: NodeJS.WritableStream
): Promise<string> {
  console.log(`Queries benchmark table=${table} runs=${runs} mode=${config.bench.mode}`);
  const stats = config.bench.mode === "trino"
    ? await runTrino(table, runs)
    : await runPostgres(table, runs, logStream);
  const lines = [
    "# Queries benchmark",
    `table=${getTableName(table)} mode=${config.bench.mode} runs=${runs}`,
    "",
    "Query | Name                          |  Min    Max    Avg  Median | N",
    "-".repeat(70),
    ...stats.map((s) => `${String(s.id).padStart(5)} | ${s.name.padEnd(30)} | ${s.min.toFixed(2).padStart(6)} ${s.max.toFixed(2).padStart(6)} ${s.avg.toFixed(2).padStart(6)} ${s.median.toFixed(2).padStart(6)} | ${s.n}`),
  ];
  const out = lines.join("\n");
  if (logStream) (logStream as NodeJS.WriteStream).write(out + "\n");
  const dir = join(process.cwd(), "results");
  mkdirSync(dir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join(dir, `queries-benchmark-${table}-${ts}.txt`);
  writeFileSync(outPath, out);
  console.log(out);
  console.log("\nWritten to", outPath);
  return outPath;
}

async function main(): Promise<void> {
  const { table, runs } = parseArgs();
  await runQueriesBenchmark(table, runs);
}

// Run main only when this file is the entry point (not when imported by bench-full)
if (process.argv[1]?.includes("bench-queries")) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
