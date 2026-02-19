/**
 * Full benchmark cycle: create table -> fill (batches up to RECORD_MAX) -> write log -> read benchmark -> queries benchmark.
 * Repeats fill + benchmarks until total rows >= RECORD_MAX.
 * All output (steps, fill details, read/queries results, errors) is written to a single run log: logs/bench-full-{table}-{timestamp}.log
 * Usage: pnpm run bench:full [--table plain|part|idx|idx_part] [--batch N]
 * State is saved to logs/bench-full-state.json so reruns skip already completed steps.
 * K6 insert-one is NOT part of this flow (run separately).
 */

import "dotenv/config";
import { spawn } from "child_process";
import { createWriteStream, existsSync } from "fs";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { config } from "../src/config.js";
import {
  createIndexForVariant,
  dropIndexForVariant,
  getFullTableName,
  getTableName,
  TABLE_VARIANTS,
  type TableVariant,
} from "../src/bench/create-tables.js";
import postgres from "postgres";
import { runReadBenchmark } from "./bench-read.js";
import { runQueriesBenchmark } from "./bench-queries.js";

const DEFAULT_FILL_BATCH = 5_000_000;
const STATE_FILE = "logs/bench-full-state.json";

/** One log file per bench:full run: all steps, fill output, read/queries results, errors. */
function openRunLog(table: TableVariant): { stream: NodeJS.WritableStream; path: string } {
  const dir = join(process.cwd(), "logs");
  mkdirSync(dir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const path = join(dir, `bench-full-${table}-${ts}.log`);
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

interface BenchFullState {
  table: TableVariant;
  recordMax: number;
  batchSize: number;
  fillBatch: number;
  currentRound: number;
  totalRows: number;
  completedThisRound: { create: boolean; fill: boolean; read: boolean; queries: boolean };
}

function parseArgs(): { table: TableVariant; batch: number } {
  const args = process.argv.slice(2);
  let table = config.bench.tableVariant as TableVariant;
  let batch = DEFAULT_FILL_BATCH;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--table" && args[i + 1] && TABLE_VARIANTS.includes(args[i + 1] as TableVariant)) {
      table = args[++i] as TableVariant;
    } else if (args[i] === "--batch" && args[i + 1]) {
      batch = Math.max(1, Number(args[++i]));
    }
  }
  return { table, batch };
}

function loadState(): BenchFullState | null {
  try {
    const path = join(process.cwd(), STATE_FILE);
    const raw = readFileSync(path, "utf-8");
    return JSON.parse(raw) as BenchFullState;
  } catch {
    return null;
  }
}

function saveState(state: BenchFullState): void {
  const dir = join(process.cwd(), "logs");
  mkdirSync(dir, { recursive: true });
  const path = join(process.cwd(), STATE_FILE);
  writeFileSync(path, JSON.stringify(state, null, 2), "utf-8");
}

function initialState(table: TableVariant, recordMax: number, batchSize: number, fillBatch: number): BenchFullState {
  return {
    table,
    recordMax,
    batchSize,
    fillBatch,
    currentRound: 1,
    totalRows: 0,
    completedThisRound: { create: false, fill: false, read: false, queries: false },
  };
}

/** Path to tsx so child runs directly and stdout is not buffered by pnpm. */
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

async function getRowCount(table: TableVariant): Promise<number> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  const tableName = getTableName(table);
  try {
    const rows = await sql.unsafe<{ count: string }[]>(`SELECT count(*) AS count FROM "bench"."${tableName}"`);
    return Number(rows[0]?.count ?? 0);
  } finally {
    await sql.end();
  }
}

/** Get row count; if table does not exist (42P01), create it and retry. Handles resume after drop:tables. */
async function getRowCountOrEnsureTable(table: TableVariant): Promise<number> {
  try {
    return await getRowCount(table);
  } catch (err) {
    const code = (err as { code?: string })?.code;
    if (code === "42P01") {
      console.log("  Table missing (e.g. after drop:tables), creating...");
      await runScript("scripts/setup-tables.ts", ["--table", table]);
      return await getRowCount(table);
    }
    throw err;
  }
}

function formatMs(ms: number): string {
  const sec = Math.floor(ms / 1000);
  const rem = Math.round(ms % 1000);
  return rem > 0 ? `${sec}s ${rem}ms` : `${sec}s`;
}

/** Run DDL/ANALYZE with a short-lived postgres connection. */
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

async function main(): Promise<void> {
  const { table, batch } = parseArgs();
  const batchSize = config.bench.batchSize;
  const recordMax = config.bench.recordMax;

  const { stream: runLogStream, path: runLogPath } = openRunLog(table);
  const log = (msg: string) => logLine(runLogStream, msg);
  log(`# bench:full table=${table} BATCH_SIZE=${batchSize} RECORD_MAX=${recordMax} fill_batch=${batch.toLocaleString()} mode=${config.bench.mode}`);
  log(`Started at ${new Date().toISOString()}`);
  log("(K6 insert-one is not part of this flow; run it separately.)\n");

  let state = loadState();
  const paramsMatch =
    state &&
    state.table === table &&
    state.recordMax === recordMax &&
    state.batchSize === batchSize &&
    state.fillBatch === batch;
  if (!state || !paramsMatch) {
    state = initialState(table, recordMax, batchSize, batch);
    log("State: new run (no state or params changed).\n");
  } else {
    log(`State: resuming (round ${state.currentRound}, ${state.totalRows.toLocaleString()} rows, completed: create=${state.completedThisRound.create} fill=${state.completedThisRound.fill} read=${state.completedThisRound.read} queries=${state.completedThisRound.queries}).\n`);
  }

  try {
    // 1. Create table (or ensure it exists after e.g. drop:tables)
    if (!state.completedThisRound.create) {
      log("Step 1: Create table...");
      await runScript("scripts/setup-tables.ts", ["--table", table]);
      state.completedThisRound.create = true;
      state.totalRows = await getRowCountOrEnsureTable(table);
      saveState(state);
      log("Step 1: Create table... ok\n");
    } else {
      log("Step 1: Create table... skipped (already done).");
      if (state.totalRows === 0) state.totalRows = await getRowCountOrEnsureTable(table);
    }

    while (state.totalRows < recordMax || !state.completedThisRound.read || !state.completedThisRound.queries) {
      const toAdd = Math.min(batchSize, Math.max(0, recordMax - state.totalRows));
      log(`\n--- Round ${state.currentRound}: current rows ${state.totalRows.toLocaleString()}, adding ${toAdd.toLocaleString()} ---`);

      // 2. Batch fill
      if (!state.completedThisRound.fill) {
        // plain/idx only: SET UNLOGGED before fill (PostgreSQL does not allow UNLOGGED on partitioned tables)
        if (table === "plain" || table === "idx") {
          await withPg(async (sql) => {
            const fullTable = getFullTableName(table);
            await sql.unsafe(`ALTER TABLE ${fullTable} SET UNLOGGED`);
          });
        }
        // idx/idx_part only: drop index so fill runs without index
        if (table === "idx" || table === "idx_part") {
          await withPg((sql) => dropIndexForVariant(sql, table));
        }
        log("Step 2: Batch fill...");
        await runScript(
          "scripts/bench-fill.ts",
          ["--table", table, "--count", String(toAdd), "--batch", String(batch)],
          { BENCH_FULL_LOG_PATH: runLogPath }
        );
        state.totalRows = await getRowCountOrEnsureTable(table);
        state.completedThisRound.fill = true;
        saveState(state);
        log(`  Total rows after fill: ${state.totalRows.toLocaleString()}\n`);
      } else {
        log("Step 2: Batch fill... skipped (already done this round).");
        state.totalRows = await getRowCountOrEnsureTable(table);
      }

      // 3. SET LOGGED (plain/idx only; partitioned tables cannot be UNLOGGED), CREATE INDEX (idx/idx_part), ANALYZE (all)
      if (state.totalRows > 0 && !state.completedThisRound.read) {
        if (table === "plain" || table === "idx") {
          await withPg(async (sql) => {
            const fullTable = getFullTableName(table);
            await sql.unsafe(`ALTER TABLE ${fullTable} SET LOGGED`);
          });
        }
        if (table === "idx" || table === "idx_part") {
          log("Step 3: Create index...");
          const indexMs = await withPg(async (sql) => {
            const t0 = performance.now();
            await createIndexForVariant(sql, table);
            return performance.now() - t0;
          });
          log(`Index created in ${formatMs(indexMs)} (${Math.round(indexMs)} ms)`);
        }
        log(table === "idx" || table === "idx_part" ? "Step 3b: ANALYZE..." : "Step 3: ANALYZE...");
        const fullTable = getFullTableName(table);
        const analyzeMs = await withPg(async (sql) => {
          const t0 = performance.now();
          await sql.unsafe(`ANALYZE ${fullTable}`);
          return performance.now() - t0;
        });
        log(`ANALYZE ${fullTable} executed in ${formatMs(analyzeMs)} (${Math.round(analyzeMs)} ms)\n`);
      }

      // 4. Read benchmark
      if (!state.completedThisRound.read) {
        if (state.totalRows > 0) {
          log("Step 4: Read benchmark...");
          await runReadBenchmark(table, undefined, runLogStream);
          log("");
        } else {
          log("Step 4: Read benchmark... skipped (table empty).");
        }
        state.completedThisRound.read = true;
        saveState(state);
      } else {
        log("Step 4: Read benchmark... skipped (already done this round).");
      }

      // 5. Queries benchmark
      if (!state.completedThisRound.queries) {
        if (state.totalRows > 0) {
          log("Step 5: Queries benchmark...");
          await runQueriesBenchmark(table, undefined, runLogStream);
          log("");
        } else {
          log("Step 5: Queries benchmark... skipped (table empty).");
        }
        state.completedThisRound.queries = true;
        saveState(state);
      } else {
        log("Step 5: Queries benchmark... skipped (already done this round).");
      }

      if (state.totalRows >= recordMax) {
        log(`\nReached RECORD_MAX (${recordMax.toLocaleString()}). Done.`);
        log(`Run log: ${runLogPath}`);
        break;
      }
      // idx/idx_part: drop index before next fill round (so next fill runs without index)
      if (table === "idx" || table === "idx_part") {
        await withPg((sql) => dropIndexForVariant(sql, table));
        log("Dropped index for next round.");
      }
      log(`\nTotal rows ${state.totalRows.toLocaleString()} < RECORD_MAX ${recordMax.toLocaleString()}. Next round: add BATCH_SIZE.`);
      state.currentRound += 1;
      state.completedThisRound = { create: true, fill: false, read: false, queries: false };
      saveState(state);
    }
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
