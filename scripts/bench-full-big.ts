/**
 * bench-full_big: single table (idx), extended indexes from indexes.txt, one pass.
 * Create table (no indexes) -> fill to recordMax -> create extended indexes -> ANALYZE -> read benchmark -> queries benchmark.
 * State in logs/bench-full-big-state.json.
 *
 * Re-run behavior:
 * - default: if table exists, drop extended indexes and rebuild them
 * - optional: keep existing indexes and only build missing ones (--no-drop-indexes)
 *
 * Usage: pnpm run bench:full-big [--batch N] [--record-max M] [--drop-indexes true|false] [--no-drop-indexes]
 * K6 insert-one is NOT part of this flow (run separately).
 */

import "dotenv/config";
import { spawn } from "child_process";
import { createWriteStream, existsSync } from "fs";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { hostname } from "os";
import { join } from "path";
import { config } from "../src/config.js";
import { createIdx, getFullTableName, getTableName } from "../src/bench/create-tables.js";
import {
  EXTENDED_INDEX_DEFS,
  dropExtendedIndexesForIdx,
} from "../src/bench/extended-indexes.js";
import postgres from "postgres";
import { runReadBenchmark } from "./bench-read.js";
import { runQueriesBenchmark } from "./bench-queries.js";

const TABLE_VARIANT = "idx" as const;
const DEFAULT_FILL_BATCH = 5_000_000;
const STATE_FILE = "logs/bench-full-big-state.json";

function openRunLog(): { stream: NodeJS.WritableStream; path: string; runId: string } {
  const dir = join(process.cwd(), "logs");
  mkdirSync(dir, { recursive: true });
  const runId = new Date().toISOString().replace(/[:.]/g, "-");
  const path = join(dir, `bench-full-big-${runId}.log`);
  const stream = createWriteStream(path, { flags: "w" });
  return { stream, path, runId };
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

type PhaseName =
  | "init"
  | "drop_indexes"
  | "create_table"
  | "fill"
  | "create_indexes"
  | "analyze"
  | "read_benchmark"
  | "queries_benchmark"
  | "done"
  | "error"
  | "interrupted";

interface IndexBuildStat {
  status: "pending" | "running" | "done" | "failed" | "skipped";
  startedAt?: string;
  finishedAt?: string;
  ms?: number;
  error?: string;
  note?: string;
}

interface BenchFullBigState {
  recordMax: number;
  fillBatch: number;
  totalRows: number;
  dropIndexesOnStart: boolean;
  previous?: {
    run?: BenchFullBigState["run"];
    phase?: BenchFullBigState["phase"];
    lastError?: BenchFullBigState["lastError"];
  };
  run?: {
    pid: number;
    host: string;
    argv: string[];
    runId: string;
    runLogPath: string;
    startedAt: string;
    lastHeartbeatAt: string;
    lastExit?: { at: string; code?: number; signal?: string; reason?: string };
  };
  phase?: { name: PhaseName; detail?: string; startedAt: string; updatedAt: string };
  lastError?: { at: string; message: string; stack?: string };
  indexStats: Record<string, IndexBuildStat>;
  completed: {
    create: boolean;
    fill: boolean;
    indexes: boolean;
    analyze: boolean;
    read: boolean;
    queries: boolean;
  };
}

function parseBool(s: string | undefined): boolean | null {
  if (!s) return null;
  const v = s.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return null;
}

function parseArgs(): { batch: number; recordMax: number; dropIndexesOnStart: boolean } {
  const args = process.argv.slice(2);
  let batch = DEFAULT_FILL_BATCH;
  let recordMax = config.bench.recordMaxBig;
  let dropIndexesOnStart = true;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--batch" && args[i + 1]) {
      batch = Math.max(1, Number(args[++i]));
    } else if (args[i] === "--record-max" && args[i + 1]) {
      recordMax = Math.max(1, Number(args[++i]));
    } else if (args[i] === "--no-drop-indexes") {
      dropIndexesOnStart = false;
    } else if (args[i] === "--drop-indexes") {
      const parsed = parseBool(args[i + 1]);
      if (parsed != null) {
        dropIndexesOnStart = parsed;
        i += 1;
      }
    }
  }
  return { batch, recordMax, dropIndexesOnStart };
}

function newIndexStats(): Record<string, IndexBuildStat> {
  const out: Record<string, IndexBuildStat> = {};
  for (const def of EXTENDED_INDEX_DEFS) out[def.name] = { status: "pending" };
  return out;
}

function normalizeState(raw: BenchFullBigState, recordMax: number, fillBatch: number, dropIndexesOnStart: boolean): BenchFullBigState {
  const s: BenchFullBigState = {
    ...raw,
    recordMax: raw.recordMax ?? recordMax,
    fillBatch: raw.fillBatch ?? fillBatch,
    totalRows: raw.totalRows ?? 0,
    dropIndexesOnStart,
    indexStats: raw.indexStats && typeof raw.indexStats === "object" ? raw.indexStats : newIndexStats(),
    completed: raw.completed ?? { create: false, fill: false, indexes: false, analyze: false, read: false, queries: false },
  };
  // Ensure all indexes have a stat entry
  for (const def of EXTENDED_INDEX_DEFS) {
    if (!s.indexStats[def.name]) s.indexStats[def.name] = { status: "pending" };
  }
  return s;
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

function initialState(recordMax: number, fillBatch: number, dropIndexesOnStart: boolean): BenchFullBigState {
  return {
    recordMax,
    fillBatch,
    totalRows: 0,
    dropIndexesOnStart,
    indexStats: newIndexStats(),
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

function indexStatsPath(runId: string): string {
  return join(process.cwd(), "logs", `bench-full-big-index-stats-${runId}.json`);
}

function writeIndexStatsSnapshot(state: BenchFullBigState, runId: string): void {
  const path = indexStatsPath(runId);
  const payload = {
    run: state.run,
    recordMax: state.recordMax,
    fillBatch: state.fillBatch,
    totalRows: state.totalRows,
    indexStats: state.indexStats,
    completed: state.completed,
    phase: state.phase,
    lastError: state.lastError,
    writtenAt: new Date().toISOString(),
  };
  writeFileSync(path, JSON.stringify(payload, null, 2), "utf-8");
}

function updatePhase(state: BenchFullBigState, name: PhaseName, detail?: string): void {
  const now = new Date().toISOString();
  if (!state.phase || state.phase.name !== name || state.phase.detail !== detail) {
    state.phase = { name, detail, startedAt: now, updatedAt: now };
  } else {
    state.phase.updatedAt = now;
  }
  if (state.run) state.run.lastHeartbeatAt = now;
}

function summarizePreviousRun(prevRun?: BenchFullBigState["run"], prevPhase?: BenchFullBigState["phase"]): string | null {
  if (!prevRun && !prevPhase) return null;
  const exit = prevRun?.lastExit;
  if (exit?.reason === "completed" && exit?.code === 0) return null;
  if (exit?.reason) {
    return `Previous run ended: reason=${exit.reason}${exit.signal ? ` signal=${exit.signal}` : ""}${exit.code != null ? ` code=${exit.code}` : ""}${prevPhase ? ` phase=${prevPhase.name}${prevPhase.detail ? ` detail=${prevPhase.detail}` : ""}` : ""}`;
  }
  // No recorded exit; likely hard kill (SIGKILL/OOM) or host reboot.
  if (prevPhase && prevPhase.name !== "done") {
    return `Previous run ended unexpectedly (no exit recorded). Last known phase=${prevPhase.name}${prevPhase.detail ? ` detail=${prevPhase.detail}` : ""}. Possible causes: SIGKILL/OOM killer, SSH session hangup (SIGHUP) without handler, or host reboot.`;
  }
  return null;
}

async function main(): Promise<void> {
  const { batch, recordMax, dropIndexesOnStart } = parseArgs();
  const { stream: runLogStream, path: runLogPath, runId } = openRunLog();
  const log = (msg: string) => logLine(runLogStream, msg);

  log(`# bench:full-big table=idx RECORD_MAX=${recordMax.toLocaleString()} fill_batch=${batch.toLocaleString()} drop_indexes_on_start=${dropIndexesOnStart}`);
  log(`Started at ${new Date().toISOString()}`);
  log(`State file: ${join(process.cwd(), STATE_FILE)}`);
  log(`Index stats file: ${indexStatsPath(runId)}`);
  log("Fill uses PostgreSQL directly. K6 insert-one is not part of this flow.\n");

  let state = loadState();
  const paramsMatch = state && state.recordMax === recordMax && state.fillBatch === batch;
  if (!state || !paramsMatch) {
    state = initialState(recordMax, batch, dropIndexesOnStart);
    log("State: new run (no state or params changed).\n");
  } else {
    state = normalizeState(state, recordMax, batch, dropIndexesOnStart);
    log(
      `State: resuming (${state.totalRows.toLocaleString()} rows, completed: create=${state.completed.create} fill=${state.completed.fill} indexes=${state.completed.indexes} analyze=${state.completed.analyze} read=${state.completed.read} queries=${state.completed.queries}).\n`
    );
  }

  const prevSummary = summarizePreviousRun(state.run, state.phase);
  if (prevSummary) log(prevSummary + "\n");

  // Preserve previous run info in-state (useful when last run ended abruptly).
  state.previous = state.run || state.phase || state.lastError
    ? { run: state.run, phase: state.phase, lastError: state.lastError }
    : undefined;

  // Attach current run metadata and write an initial heartbeat.
  state.run = {
    pid: process.pid,
    host: hostname(),
    argv: process.argv,
    runId,
    runLogPath,
    startedAt: new Date().toISOString(),
    lastHeartbeatAt: new Date().toISOString(),
  };
  updatePhase(state, "init");
  saveState(state);
  writeIndexStatsSnapshot(state, runId);

  const heartbeat = setInterval(() => {
    try {
      updatePhase(state!, state!.phase?.name ?? "init", state!.phase?.detail);
      saveState(state!);
      writeIndexStatsSnapshot(state!, runId);
    } catch {
      // ignore heartbeat failures
    }
  }, 30_000);
  // don't keep process alive just for heartbeat
  (heartbeat as unknown as { unref?: () => void }).unref?.();

  const recordExit = (reason: string, code?: number, signal?: string) => {
    state!.run = state!.run ?? {
      pid: process.pid,
      host: hostname(),
      argv: process.argv,
      runId,
      runLogPath,
      startedAt: new Date().toISOString(),
      lastHeartbeatAt: new Date().toISOString(),
    };
    state!.run.lastExit = { at: new Date().toISOString(), reason, code, signal };
    if (reason === "signal") updatePhase(state!, "interrupted", signal);
    saveState(state!);
    writeIndexStatsSnapshot(state!, runId);
  };

  const onFatal = (err: unknown, where: string) => {
    const e = err instanceof Error ? err : new Error(String(err));
    state!.lastError = { at: new Date().toISOString(), message: `${where}: ${e.message}`, stack: e.stack };
    updatePhase(state!, "error", where);
    saveState(state!);
    writeIndexStatsSnapshot(state!, runId);
  };

  process.on("SIGINT", () => {
    recordExit("signal", undefined, "SIGINT");
    process.exit(130);
  });
  process.on("SIGTERM", () => {
    recordExit("signal", undefined, "SIGTERM");
    process.exit(143);
  });
  process.on("SIGHUP", () => {
    recordExit("signal", undefined, "SIGHUP");
    process.exit(129);
  });
  process.on("uncaughtException", (err) => {
    onFatal(err, "uncaughtException");
    recordExit("uncaughtException", 1);
    process.exit(1);
  });
  process.on("unhandledRejection", (reason) => {
    onFatal(reason, "unhandledRejection");
    recordExit("unhandledRejection", 1);
    process.exit(1);
  });

  try {
    const exists = await tableExists();
    if (exists && dropIndexesOnStart) {
      updatePhase(state, "drop_indexes");
      saveState(state);
      log("Table bench.bonus_registry_idx exists. Dropping extended indexes...");
      await withPg(dropExtendedIndexesForIdx);
      state.indexStats = newIndexStats();
      state.completed.indexes = false;
      state.completed.analyze = false;
      state.completed.read = false;
      state.completed.queries = false;
      saveState(state);
      writeIndexStatsSnapshot(state, runId);
      log("Extended indexes dropped.\n");
    } else if (exists && !dropIndexesOnStart) {
      // Mark already-existing indexes as skipped (pre-existing).
      updatePhase(state, "init", "detect_existing_indexes");
      const existing = await withPg(async (sql) => {
        const names = EXTENDED_INDEX_DEFS.map((d) => d.name);
        const rows = await sql.unsafe<{ indexname: string }[]>(
          `SELECT indexname FROM pg_indexes WHERE schemaname = 'bench' AND tablename = $1 AND indexname = ANY($2::text[])`,
          ["bonus_registry_idx", names]
        );
        return new Set(rows.map((r) => r.indexname));
      });
      for (const def of EXTENDED_INDEX_DEFS) {
        if (existing.has(def.name)) {
          state.indexStats[def.name] = { status: "skipped", note: "already exists" };
        }
      }
      saveState(state);
      writeIndexStatsSnapshot(state, runId);
    } else {
      state.completed.create = false;
      state.totalRows = 0;
      saveState(state);
      writeIndexStatsSnapshot(state, runId);
    }

    if (!state.completed.create) {
      updatePhase(state, "create_table");
      saveState(state);
      log("Step 1: Create table (no indexes)...");
      await withPg((sql) => createIdx(sql));
      state.totalRows = await getRowCount();
      state.completed.create = true;
      saveState(state);
      writeIndexStatsSnapshot(state, runId);
      log("Step 1: Create table... ok\n");
    } else {
      log("Step 1: Create table... skipped (already done).");
      state.totalRows = await getRowCount();
    }

    if (state.totalRows < recordMax) {
      updatePhase(state, "fill");
      saveState(state);
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
      writeIndexStatsSnapshot(state, runId);
      log(`  Total rows after fill: ${state.totalRows.toLocaleString()}\n`);
    } else {
      log("Step 2: Fill... skipped (already at or above recordMax).");
    }

    if (!state.completed.indexes && state.totalRows > 0) {
      updatePhase(state, "create_indexes");
      saveState(state);
      log("Step 3: Create extended indexes...");
      const totalIndexMs = await withPg(async (sql) => {
        let total = 0;
        for (const def of EXTENDED_INDEX_DEFS) {
          const current = state.indexStats[def.name] ?? { status: "pending" as const };
          if (current.status === "done" || current.status === "skipped") continue;

          updatePhase(state, "create_indexes", def.name);
          state.indexStats[def.name] = { status: "running", startedAt: new Date().toISOString() };
          saveState(state);
          writeIndexStatsSnapshot(state, runId);

          const t0 = performance.now();
          try {
            await sql.unsafe(def.createSql);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            state.indexStats[def.name] = {
              status: "failed",
              startedAt: state.indexStats[def.name]?.startedAt,
              finishedAt: new Date().toISOString(),
              ms: performance.now() - t0,
              error: msg,
            };
            state.lastError = { at: new Date().toISOString(), message: `create_index ${def.name}: ${msg}`, stack: err instanceof Error ? err.stack : undefined };
            updatePhase(state, "error", `create_index:${def.name}`);
            saveState(state);
            writeIndexStatsSnapshot(state, runId);
            throw err;
          }
          const ms = performance.now() - t0;
          total += ms;
          state.indexStats[def.name] = {
            status: "done",
            startedAt: state.indexStats[def.name]?.startedAt,
            finishedAt: new Date().toISOString(),
            ms,
          };
          saveState(state);
          writeIndexStatsSnapshot(state, runId);
          log(`  ${def.name}: ${formatMs(ms)} (${Math.round(ms)} ms)`);
        }
        return total;
      });
      const allOk = EXTENDED_INDEX_DEFS.every((d) => {
        const st = state.indexStats[d.name]?.status;
        return st === "done" || st === "skipped";
      });
      state.completed.indexes = allOk;
      saveState(state);
      writeIndexStatsSnapshot(state, runId);
      log(`Extended indexes total: ${formatMs(totalIndexMs)} (${Math.round(totalIndexMs)} ms)\n`);
    } else {
      log("Step 3: Create extended indexes... skipped (already done).");
    }

    if (!state.completed.analyze && state.totalRows > 0) {
      updatePhase(state, "analyze");
      saveState(state);
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
      writeIndexStatsSnapshot(state, runId);
    } else {
      log("Step 4: ANALYZE... skipped (already done).");
    }

    if (!state.completed.read && state.totalRows > 0) {
      updatePhase(state, "read_benchmark");
      saveState(state);
      log("Step 5: Read benchmark...");
      await runReadBenchmark(TABLE_VARIANT, undefined, runLogStream);
      log("");
      state.completed.read = true;
      saveState(state);
      writeIndexStatsSnapshot(state, runId);
    } else {
      log("Step 5: Read benchmark... skipped (already done).");
    }

    if (!state.completed.queries && state.totalRows > 0) {
      updatePhase(state, "queries_benchmark");
      saveState(state);
      log("Step 6: Queries benchmark...");
      await runQueriesBenchmark(TABLE_VARIANT, undefined, runLogStream);
      log("");
      state.completed.queries = true;
      saveState(state);
      writeIndexStatsSnapshot(state, runId);
    } else {
      log("Step 6: Queries benchmark... skipped (already done).");
    }

    updatePhase(state, "done");
    saveState(state);
    writeIndexStatsSnapshot(state, runId);
    recordExit("completed", 0);
    log(`\nDone. Total rows: ${state.totalRows.toLocaleString()}. Run log: ${runLogPath}`);
  } catch (err) {
    state.lastError = {
      at: new Date().toISOString(),
      message: err instanceof Error ? err.message : String(err),
      stack: err instanceof Error ? err.stack : undefined,
    };
    updatePhase(state, "error");
    saveState(state);
    writeIndexStatsSnapshot(state, runId);
    logError(runLogStream, err);
    log(`Run log: ${runLogPath}`);
    recordExit("error", 1);
    throw err;
  } finally {
    clearInterval(heartbeat);
    (runLogStream as NodeJS.WriteStream).end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
