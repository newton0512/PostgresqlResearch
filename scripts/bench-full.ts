/**
 * Full benchmark cycle: create table -> fill (batches up to RECORD_MAX) -> write log -> read benchmark -> queries benchmark.
 * Repeats fill + benchmarks until total rows >= RECORD_MAX.
 * Usage: pnpm run bench:full [--table plain|part|idx|idx_part]
 * K6 insert-one is NOT part of this flow (run separately).
 */

import "dotenv/config";
import { spawn } from "child_process";
import { config } from "../src/config.js";
import { getTableName, TABLE_VARIANTS, type TableVariant } from "../src/bench/create-tables.js";
import postgres from "postgres";

function parseArgs(): { table: TableVariant } {
  const args = process.argv.slice(2);
  let table = config.bench.tableVariant as TableVariant;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--table" && args[i + 1] && TABLE_VARIANTS.includes(args[i + 1] as TableVariant)) {
      table = args[++i] as TableVariant;
    }
  }
  return { table };
}

function runScript(script: string, extraArgs: string[] = []): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn("pnpm", ["exec", "tsx", script, ...extraArgs], {
      stdio: "inherit",
      shell: true,
      cwd: process.cwd(),
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

async function main(): Promise<void> {
  const { table } = parseArgs();
  const batchSize = config.bench.batchSize;
  const recordMax = config.bench.recordMax;
  console.log(`bench:full table=${table} BATCH_SIZE=${batchSize} RECORD_MAX=${recordMax} mode=${config.bench.mode}`);
  console.log("(K6 insert-one is not part of this flow; run it separately.)\n");

  // 1. Create table (user choice)
  console.log("Step 1: Create table...");
  await runScript("scripts/setup-tables.ts", ["--table", table]);

  let totalRows = await getRowCount(table);
  let round = 0;

  while (totalRows < recordMax) {
    round++;
    const toAdd = Math.min(batchSize, recordMax - totalRows);
    console.log(`\n--- Round ${round}: current rows ${totalRows.toLocaleString()}, adding ${toAdd.toLocaleString()} ---`);

    // 2. Batch fill
    console.log("Step 2: Batch fill...");
    await runScript("scripts/bench-fill.ts", ["--table", table, "--count", String(toAdd)]);

    totalRows = await getRowCount(table);
    console.log(`  Total rows after fill: ${totalRows.toLocaleString()}`);

    // 3. Write log is done inside bench-fill
    // 4. Read benchmark
    console.log("Step 4: Read benchmark...");
    await runScript("scripts/bench-read.ts", ["--table", table]);

    // 5. Queries benchmark
    console.log("Step 5: Queries benchmark...");
    await runScript("scripts/bench-queries.ts", ["--table", table]);

    if (totalRows >= recordMax) {
      console.log(`\nReached RECORD_MAX (${recordMax.toLocaleString()}). Done.`);
      break;
    }
    console.log(`\nTotal rows ${totalRows.toLocaleString()} < RECORD_MAX ${recordMax.toLocaleString()}. Next round: add BATCH_SIZE.`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
