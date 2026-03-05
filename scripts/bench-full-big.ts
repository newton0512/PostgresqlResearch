/**
 * bench-full_big: single table (idx), extended indexes from indexes.txt, one pass.
 * Create table (no indexes) -> fill to recordMax -> create extended indexes -> ANALYZE -> read benchmark -> queries benchmark.
 * State in logs/bench-full-big-state.json. On re-run: if table exists, drop indexes then continue.
 * Usage: pnpm run bench:full-big [--batch N] [--record-max M]
 * K6 insert-one is NOT part of this flow (run separately).
 */

import "dotenv/config";
import { spawn } from "child_process";
import { createWriteStream, existsSync } from "fs";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { config } from "../src/config.js";
import { createIdx, getFullTableName, getTableName } from "../src/bench/create-tables.js";
import {
  createExtendedIndexesForIdx,
  dropExtendedIndexesForIdx,
} from "../src/bench/extended-indexes.js";
import postgres from "postgres";
import { runReadBenchmark } from "./bench-read.js";
import { runQueriesBenchmark } from "./bench-queries.js";

const TABLE_VARIANT = "idx" as const;
const DEFAULT_FILL_BATCH = 5_000_000;
const STATE_FILE = "logs/bench-full-big-state.json";

function openRunLog(): { stream: NodeJS.WritableStream; path: string } {
  const dir = join(process.cwd(), "logs");
  mkdirSync(dir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const path = join(dir, `bench-full-big-${ts}.log`);
  const stream = createWriteStream(path, { flags: "w" });
  return { stream, path };
}

function logLine(logStream: NodeJS.WritableStream, msg: string): void {
  const s = msg.endsWith("\n") ? msg : msg + "\n";
  (logStream as NodeJS.WriteStream).write(s);
  process.stdout.write(s);
}

function logError(logStream: NodeJS.WritableStream, err: unknown): void {
  const msg = err instanceof Error ? err.message : String(err);
  const stack = err instanceof Error ? err.stack : undefined;
  logLine(logStream, `[ERROR] ${msg}`);
  if (stack) logLine(logStream, stack);
}

interface BenchFullBigState {
  recordMax: number;
  fillBatch: number;
  totalRows: number;
  completed: {
    create: boolean;
    fill: boolean;
    indexes: boolean;
    analyze: boolean;
    read: boolean;
    queries: boolean;
  };
}

function parseArgs(): { batch: number; recordMax: number } {
  const args = process.argv.slice(2);
  let batch = DEFAULT_FILL_BATCH;
  let recordMax = config.bench.recordMaxBig;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--batch" && args[i + 1]) {
      batch = Math.max(1, Number(args[++i]));
    } else if (args[i] === "--record-max" && args[i + 1]) {
      recordMax = Math.max(1, Number(args[++i]));
    }
  }
  return { batch, recordMax };
}

function loadState(): BenchFullBigState | null {
  try {
    const path = join(process.cwd(), STATE_FILE);
    const raw = readFileSync(path, "utf-8");
    return JSON.parse(raw) as BenchFullBigState;
  } catch {
    return null;
  }
}

function saveState(state: BenchFullBigState): void {
  const dir = join(process.cwd(), "logs");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(process.cwd(), STATE_FILE), JSON.stringify(state, null, 2), "utf-8");
}

function initialState(recordMax: number, fillBatch: number): BenchFullBigState {
  return {
    recordMax,
    fillBatch,
    totalRows: 0,
    completed: {
      create: false,
      fill: false,
      indexes: false,
      analyze: false,
      read: false,
      queries: false,
    },
  };
}

function getTsxPath(): string {
  const binDir = join(process.cwd(), "node_modules", ".bin");
  const name = process.platform === "win32" ? "tsx.cmd" : "tsx";
  const path = join(binDir, name);
  return existsSync(path) ? path : "npx";
}

function runScript(
  script: string,
  extraArgs: string[] = [],
  envOverrides: NodeJS.ProcessEnv = {}
): Promise<void> {
  return new Promise((resolve, reject) => {
    const env = { ...process.env, ...envOverrides };
    const tsxPath = getTsxPath();
    const args = tsxPath === "npx" ? ["tsx", script, ...extraArgs] : [script, ...extraArgs];
    const child = spawn(tsxPath, args, {
      stdio: "inherit",
      shell: true,
      cwd: process.cwd(),
      env,
    });
    child.on("exit", (code) => (code === 0 ? resolve() : reject(new Error(`Exit ${code}`))));
    child.on("error", reject);
  });
}

async function getRowCount(): Promise<number> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  try {
    const rows = await sql.unsafe<{ count: string }[]>(
      `SELECT count(*) AS count FROM "bench"."${getTableName(TABLE_VARIANT)}"`
    );
    return Number(rows[0]?.count ?? 0);
  } finally {
    await sql.end();
  }
}

/** Returns true if table exists. */
async function tableExists(): Promise<boolean> {
  try {
    await getRowCount();
    return true;
  } catch (err) {
    if ((err as { code?: string })?.code === "42P01") return false;
    throw err;
  }
}

async function withPg<T>(fn: (sql: ReturnType<typeof postgres>) => Promise<T>): Promise<T> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  try {
    return await fn(sql);
  } finally {
    await sql.end();
  }
}

function formatMs(ms: number): string {
  const sec = Math.floor(ms / 1000);
  const rem = Math.round(ms % 1000);
  return rem > 0 ? `${sec}s ${rem}ms` : `${sec}s`;
}

async function main(): Promise<void> {
  const { batch, recordMax } = parseArgs();
  const { stream: runLogStream, path: runLogPath } = openRunLog();
  const log = (msg: string) => logLine(runLogStream, msg);

  log(`# bench:full-big table=idx RECORD_MAX=${recordMax.toLocaleString()} fill_batch=${batch.toLocaleString()}`);
  log(`Started at ${new Date().toISOString()}`);
  log("Fill uses PostgreSQL directly. K6 insert-one is not part of this flow.\n");

  let state = loadState();
  const paramsMatch =
    state && state.recordMax === recordMax && state.fillBatch === batch;
  if (!state || !paramsMatch) {
    state = initialState(recordMax, batch);
    log("State: new run (no state or params changed).\n");
  } else {
    log(
      `State: resuming (${state.totalRows.toLocaleString()} rows, completed: create=${state.completed.create} fill=${state.completed.fill} indexes=${state.completed.indexes} analyze=${state.completed.analyze} read=${state.completed.read} queries=${state.completed.queries}).\n`
    );
  }

  try {
    const exists = await tableExists();
    if (exists) {
      log("Table bench.bonus_registry_idx exists. Dropping extended indexes...");
      await withPg(dropExtendedIndexesForIdx);
      state.completed.indexes = false;
      state.completed.analyze = false;
      state.completed.read = false;
      state.completed.queries = false;
      saveState(state);
      log("Extended indexes dropped.\n");
    } else {
      state.completed.create = false;
      state.totalRows = 0;
      saveState(state);
    }

    if (!state.completed.create) {
      log("Step 1: Create table (no indexes)...");
      await withPg((sql) => createIdx(sql));
      state.totalRows = await getRowCount();
      state.completed.create = true;
      saveState(state);
      log("Step 1: Create table... ok\n");
    } else {
      log("Step 1: Create table... skipped (already done).");
      state.totalRows = await getRowCount();
    }

    if (state.totalRows < recordMax) {
      const toAdd = recordMax - state.totalRows;
      log(`Step 2: Fill... adding ${toAdd.toLocaleString()} rows (batch ${batch.toLocaleString()})`);
      await withPg(async (sql) => {
        const fullTable = getFullTableName(TABLE_VARIANT);
        await sql.unsafe(`ALTER TABLE ${fullTable} SET UNLOGGED`);
      });
      await runScript(
        "scripts/bench-fill.ts",
        ["--table", TABLE_VARIANT, "--count", String(toAdd), "--batch", String(batch)],
        { BENCH_FULL_LOG_PATH: runLogPath }
      );
      state.totalRows = await getRowCount();
      await withPg(async (sql) => {
        const fullTable = getFullTableName(TABLE_VARIANT);
        await sql.unsafe(`ALTER TABLE ${fullTable} SET LOGGED`);
      });
      state.completed.fill = true;
      saveState(state);
      log(`  Total rows after fill: ${state.totalRows.toLocaleString()}\n`);
    } else {
      log("Step 2: Fill... skipped (already at or above recordMax).");
    }

    if (!state.completed.indexes && state.totalRows > 0) {
      log("Step 3: Create extended indexes...");
      const totalIndexMs = await withPg(async (sql) => {
        return createExtendedIndexesForIdx(sql, (indexName, ms) => {
          log(`  ${indexName}: ${formatMs(ms)} (${Math.round(ms)} ms)`);
        });
      });
      log(`Extended indexes total: ${formatMs(totalIndexMs)} (${Math.round(totalIndexMs)} ms)\n`);
      state.completed.indexes = true;
      saveState(state);
    } else {
      log("Step 3: Create extended indexes... skipped (already done).");
    }

    if (!state.completed.analyze && state.totalRows > 0) {
      log("Step 4: ANALYZE...");
      const fullTable = getFullTableName(TABLE_VARIANT);
      const analyzeMs = await withPg(async (sql) => {
        const t0 = performance.now();
        await sql.unsafe(`ANALYZE ${fullTable}`);
        return performance.now() - t0;
      });
      log(`ANALYZE ${fullTable} executed in ${formatMs(analyzeMs)} (${Math.round(analyzeMs)} ms)\n`);
      state.completed.analyze = true;
      saveState(state);
    } else {
      log("Step 4: ANALYZE... skipped (already done).");
    }

    if (!state.completed.read && state.totalRows > 0) {
      log("Step 5: Read benchmark...");
      await runReadBenchmark(TABLE_VARIANT, undefined, runLogStream);
      log("");
      state.completed.read = true;
      saveState(state);
    } else {
      log("Step 5: Read benchmark... skipped (already done).");
    }

    if (!state.completed.queries && state.totalRows > 0) {
      log("Step 6: Queries benchmark...");
      await runQueriesBenchmark(TABLE_VARIANT, undefined, runLogStream);
      log("");
      state.completed.queries = true;
      saveState(state);
    } else {
      log("Step 6: Queries benchmark... skipped (already done).");
    }

    log(`\nDone. Total rows: ${state.totalRows.toLocaleString()}. Run log: ${runLogPath}`);
  } catch (err) {
    logError(runLogStream, err);
    log(`Run log: ${runLogPath}`);
    throw err;
  } finally {
    (runLogStream as NodeJS.WriteStream).end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
