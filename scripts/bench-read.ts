/**
 * Read benchmark: SELECT by accounted_for_bs_profile_id. Writes results to file.
 * Usage: pnpm run bench:read [--table plain|part|idx|idx_part] [--samples 100]
 */

import "dotenv/config";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import postgres from "postgres";
import { BasicAuth, Trino } from "trino-client";
import { config } from "../src/config.js";
import { getTableName, TABLE_VARIANTS, type TableVariant } from "../src/bench/create-tables.js";

const PARTITION_COLUMN = "accounted_for_bs_profile_id";
const DEFAULT_SAMPLES = 100;

function parseArgs(): { table: TableVariant; samples: number } {
  const args = process.argv.slice(2);
  let table = config.bench.tableVariant as TableVariant;
  let samples = DEFAULT_SAMPLES;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--table" && args[i + 1] && TABLE_VARIANTS.includes(args[i + 1] as TableVariant)) {
      table = args[++i] as TableVariant;
    } else if (args[i] === "--samples" && args[i + 1]) {
      samples = Math.max(1, Number(args[++i]));
    }
  }
  return { table, samples };
}

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? (s[m] ?? 0) : ((s[m - 1] ?? 0) + (s[m] ?? 0)) / 2;
}

async function runPostgres(table: TableVariant, samples: number): Promise<{ min: number; max: number; avg: number; median: number; n: number }> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  const tableName = getTableName(table);
  const fullTable = `"bench"."${tableName}"`;

  const countResult = await sql.unsafe<{ count: string }[]>(`SELECT count(*) AS cnt FROM ${fullTable}`);
  const rowCount = Number(countResult[0]?.count ?? 0);
  if (rowCount === 0) throw new Error(`Table ${tableName} is empty`);

  const percent = Math.min(100, Math.max(0.5, (samples / rowCount) * 100 * 1.5));
  const sampleResult = await sql.unsafe<{ val: string }[]>(
    `SELECT ${PARTITION_COLUMN} AS val FROM ${fullTable} TABLESAMPLE BERNOULLI(${percent}) LIMIT ${samples}`
  );
  let values = sampleResult.map((r) => r.val);
  if (values.length === 0) {
    const alt = await sql.unsafe<{ val: string }[]>(
      `SELECT ${PARTITION_COLUMN} AS val FROM ${fullTable} ORDER BY random() LIMIT ${samples}`
    );
    values = alt.map((r) => r.val);
  }
  if (values.length === 0) throw new Error("No partition key values sampled");

  const times: number[] = [];
  for (const v of values) {
    const start = performance.now();
    await sql.unsafe(
      `SELECT * FROM ${fullTable} WHERE ${PARTITION_COLUMN} = $1 LIMIT 1`,
      [v]
    );
    times.push(performance.now() - start);
  }
  await sql.end();

  return {
    min: Math.min(...times),
    max: Math.max(...times),
    avg: times.reduce((a, b) => a + b, 0) / times.length,
    median: median(times),
    n: times.length,
  };
}

async function runTrino(table: TableVariant, samples: number): Promise<{ min: number; max: number; avg: number; median: number; n: number }> {
  const trino = Trino.create({
    server: `http://${config.trino.host}:${config.trino.port}`,
    catalog: config.trino.catalog,
    schema: config.trino.schema,
    auth: new BasicAuth(config.trino.user),
  });
  const tableName = getTableName(table);
  const fullTable = `"${config.trino.catalog}"."bench"."${tableName}"`;
  const esc = (s: string) => `'${s.replace(/'/g, "''")}'`;

  async function query(sql: string): Promise<unknown[][]> {
    const q = await trino.query(sql);
    const rows: unknown[][] = [];
    for await (const r of q) {
      const row = r as { data?: unknown[][]; error?: { message?: string } };
      if (row?.error) throw new Error(row.error.message);
      if (row?.data) rows.push(...row.data);
    }
    return rows;
  }

  const countRows = await query(`SELECT count(*) AS cnt FROM ${fullTable}`);
  const rowCount = Number(countRows[0]?.[0] ?? 0);
  if (rowCount === 0) throw new Error(`Table ${tableName} is empty`);

  const percent = Math.min(100, Math.max(0.5, (samples / rowCount) * 100 * 1.5));
  let sampleRows = await query(`SELECT ${PARTITION_COLUMN} FROM ${fullTable} TABLESAMPLE BERNOULLI(${percent}) LIMIT ${samples}`);
  let values = sampleRows.map((r) => String(r[0] ?? ""));
  if (values.length === 0) {
    sampleRows = await query(`SELECT ${PARTITION_COLUMN} FROM ${fullTable} ORDER BY random() LIMIT ${samples}`);
    values = sampleRows.map((r) => String(r[0] ?? ""));
  }
  if (values.length === 0) throw new Error("No partition key values sampled");

  const times: number[] = [];
  for (const v of values) {
    const start = performance.now();
    await query(`SELECT * FROM ${fullTable} WHERE ${PARTITION_COLUMN} = ${esc(v)} LIMIT 1`);
    times.push(performance.now() - start);
  }

  return {
    min: Math.min(...times),
    max: Math.max(...times),
    avg: times.reduce((a, b) => a + b, 0) / times.length,
    median: median(times),
    n: times.length,
  };
}

async function main(): Promise<void> {
  const { table, samples } = parseArgs();
  console.log(`Read benchmark table=${table} samples=${samples} mode=${config.bench.mode}`);

  const stats = config.bench.mode === "trino"
    ? await runTrino(table, samples)
    : await runPostgres(table, samples);

  const lines = [
    `# Read benchmark (by ${PARTITION_COLUMN})`,
    `table=${getTableName(table)} mode=${config.bench.mode} samples=${samples}`,
    "",
    "Min (ms)\tMax (ms)\tAvg (ms)\tMedian (ms)\tN",
    `${stats.min.toFixed(2)}\t${stats.max.toFixed(2)}\t${stats.avg.toFixed(2)}\t${stats.median.toFixed(2)}\t${stats.n}`,
  ];
  const out = lines.join("\n");
  const dir = join(process.cwd(), "results");
  mkdirSync(dir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const path = join(dir, `read-benchmark-${table}-${ts}.txt`);
  writeFileSync(path, out);
  console.log(out);
  console.log("\nWritten to", path);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
