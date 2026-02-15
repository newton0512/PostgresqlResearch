/**
 * Batch load rows into bonus_registry table. Writes timing to log file.
 * Usage: pnpm run bench:fill [--table plain|part|idx|idx_part] [--count N] [--chunk 5000]
 * BENCH_MODE=postgres|trino. BATCH_SIZE / RECORD_MAX from env (count default = BATCH_SIZE).
 */

import "dotenv/config";
import { mkdirSync, createWriteStream } from "fs";
import { join } from "path";
import postgres from "postgres";
import { BasicAuth, Trino } from "trino-client";
import { config } from "../src/config.js";
import { getTableName, type TableVariant } from "../src/bench/create-tables.js";
import {
  BONUS_REGISTRY_INSERT_COLUMNS,
  generateRow,
} from "../src/bench/row-generator.js";

const CHUNK_SIZE = 5000; // rows per INSERT to avoid OOM / query size limits

function parseArgs(): {
  table: TableVariant;
  count: number;
  chunk: number;
} {
  const args = process.argv.slice(2);
  let table = config.bench.tableVariant as TableVariant;
  let count = config.bench.batchSize;
  let chunk = CHUNK_SIZE;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--table" && args[i + 1]) {
      table = args[++i] as TableVariant;
    } else if (args[i] === "--count" && args[i + 1]) {
      count = Number(args[++i]);
    } else if (args[i] === "--chunk" && args[i + 1]) {
      chunk = Number(args[++i]);
    }
  }
  return { table, count, chunk };
}

function ensureLogDir(): string {
  const dir = join(process.cwd(), "logs");
  mkdirSync(dir, { recursive: true });
  return dir;
}

function openLogStream(variant: string): { stream: NodeJS.WritableStream; path: string } {
  const dir = ensureLogDir();
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const path = join(dir, `write-${variant}-${ts}.log`);
  const stream = createWriteStream(path, { flags: "a" });
  return { stream, path };
}

async function fillPostgresValues(
  table: TableVariant,
  count: number,
  chunk: number,
  logStream: NodeJS.WritableStream
): Promise<{ inserted: number; totalMs: number }> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  const tableName = getTableName(table);
  const fullTable = sql.unsafe(`"bench"."${tableName}"`);
  let inserted = 0;
  const start = Date.now();

  try {
    while (inserted < count) {
      const batchSize = Math.min(chunk, count - inserted);
      const rows = Array.from({ length: batchSize }, () => generateRow());
      const batchStart = Date.now();
      await sql`INSERT INTO ${fullTable} ${sql(rows)}`;
      const batchMs = Date.now() - batchStart;
      inserted += batchSize;
      const cumulativeMs = Date.now() - start;
      const line = `variant=${tableName} batchSize=${batchSize} batchMs=${batchMs} cumulativeMs=${cumulativeMs} cumulativeRows=${inserted}\n`;
      logStream.write(line);
      process.stdout.write(line);
    }
  } finally {
    await sql.end();
  }

  const totalMs = Date.now() - start;
  const summary = `\ntotalRows=${inserted} totalMs=${totalMs} rowsPerSec=${(inserted / (totalMs / 1000)).toFixed(0)}\n`;
  logStream.write(summary);
  console.log(summary);
  return { inserted, totalMs };
}

async function main(): Promise<void> {
  const { table, count, chunk } = parseArgs();
  const { stream: logStream, path: logPath } = openLogStream(table);
  logStream.write(`# bench-fill table=${table} count=${count} chunk=${chunk} mode=${config.bench.mode}\n`);

  if (config.bench.mode === "trino") {
    // Trino path: INSERT via Trino (smaller chunks to avoid query size)
    const trino = Trino.create({
      server: `http://${config.trino.host}:${config.trino.port}`,
      catalog: config.trino.catalog,
      schema: config.trino.schema,
      auth: new BasicAuth(config.trino.user),
    });
    const tableName = getTableName(table);
    const fullTable = `"${config.trino.catalog}"."bench"."${tableName}"`;
    let inserted = 0;
    const start = Date.now();
    const trinoChunk = Math.min(chunk, 1000);
    while (inserted < count) {
      const batchSize = Math.min(trinoChunk, count - inserted);
      const rows = Array.from({ length: batchSize }, () => generateRow());
      const batchStart = Date.now();
      const esc = (v: string | number | boolean | Date | null): string => {
        if (v == null) return "NULL";
        if (v instanceof Date) return `TIMESTAMP '${v.toISOString().slice(0, 19).replace("T", " ")}'`;
        if (typeof v === "boolean") return v ? "true" : "false";
        if (typeof v === "number") return String(v);
        return `'${String(v).replace(/'/g, "''")}'`;
      };
      const values = rows
        .map((r) =>
          BONUS_REGISTRY_INSERT_COLUMNS.map((c) => esc(r[c] ?? null)).join(", ")
        )
        .map((rowStr) => `(${rowStr})`)
        .join(", ");
      const cols = BONUS_REGISTRY_INSERT_COLUMNS.map((c) =>
        c === "date" || c === "row" ? `"${c}"` : c
      ).join(", ");
      const sql = `INSERT INTO ${fullTable} (${cols}) VALUES ${values}`;
      const q = await trino.query(sql);
      for await (const row of q) {
        const r = row as { error?: { message?: string } };
        if (r?.error) throw new Error(`Trino: ${r.error.message}`);
      }
      const batchMs = Date.now() - batchStart;
      inserted += batchSize;
      const cumulativeMs = Date.now() - start;
      const line = `variant=${tableName} batchSize=${batchSize} batchMs=${batchMs} cumulativeMs=${cumulativeMs} cumulativeRows=${inserted}\n`;
      logStream.write(line);
      process.stdout.write(line);
    }
    const totalMs = Date.now() - start;
    const summary = `\ntotalRows=${inserted} totalMs=${totalMs} rowsPerSec=${(inserted / (totalMs / 1000)).toFixed(0)}\n`;
    logStream.write(summary);
    console.log(summary);
    logStream.end();
    console.log("Log written to", logPath);
    return;
  }

  await fillPostgresValues(table, count, chunk, logStream);
  logStream.end();
  console.log("Log written to", logPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
