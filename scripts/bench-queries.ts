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

interface QueryDef {
  id: number;
  name: string;
  sql: string;
  params: (string | number)[];
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

async function runPostgres(table: TableVariant, runs: number): Promise<{ id: number; name: string; min: number; max: number; avg: number; median: number; n: number }[]> {
  const tableName = getTableName(table);
  const fullTable = `"bench"."${tableName}"`;
  const params = await discoverParamsPostgres(fullTable);
  const defs = buildQueryDefs(fullTable, params);
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
      const start = performance.now();
      await sql.unsafe(q.sql, q.params);
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
  const esc = (v: string | number): string => (typeof v === "number" ? String(v) : `'${String(v).replace(/'/g, "''")}'`);
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
    : await runPostgres(table, runs);
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
