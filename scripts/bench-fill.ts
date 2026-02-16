/**
 * Батчевая загрузка в bonus_registry (как в samples-generation).
 * PostgreSQL: INSERT...SELECT с generate_series — данные генерируются в БД.
 * Trino: INSERT...SELECT с sequence + CROSS JOIN (как write-trino-mass) — большие батчи без лимита VALUES.
 * Usage: pnpm run bench:fill [--table plain|part|idx|idx_part] [--count N] [--batch N]
 */

import "dotenv/config";
import { mkdirSync, createWriteStream } from "fs";
import { join } from "path";
import postgres from "postgres";
import { BasicAuth, Trino } from "trino-client";
import { config } from "../src/config.js";
import { createTable, getTableName, type TableVariant } from "../src/bench/create-tables.js";
import { getPgInsertExpressions } from "../src/bench/pg-insert-expressions.js";
import { getTrinoInsertExpressions } from "../src/bench/trino-insert-expressions.js";
import { BONUS_REGISTRY_INSERT_COLUMNS } from "../src/bench/row-generator.js";

const DEFAULT_BATCH_SIZE = 5_000_000;

function parseArgs(): { table: TableVariant; count: number; batch: number } {
  const args = process.argv.slice(2);
  let table = config.bench.tableVariant as TableVariant;
  let count = config.bench.batchSize;
  let batch = DEFAULT_BATCH_SIZE;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--table" && args[i + 1]) {
      table = args[++i] as TableVariant;
    } else if (args[i] === "--count" && args[i + 1]) {
      count = Number(args[++i]);
    } else if ((args[i] === "--batch" || args[i] === "--chunk") && args[i + 1]) {
      batch = Math.max(1, Number(args[++i]));
    }
  }
  return { table, count, batch };
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

function formatMs(ms: number): string {
  const sec = Math.floor(ms / 1000);
  const rem = Math.round(ms % 1000);
  return rem > 0 ? `${sec}s ${rem}ms` : `${sec}s`;
}

/** PostgreSQL: INSERT INTO table SELECT ... FROM generate_series(1, batchSize). */
async function fillPostgres(
  table: TableVariant,
  count: number,
  batchSize: number,
  logStream: NodeJS.WritableStream
): Promise<void> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  const tableName = getTableName(table);
  const fullTable = `"bench"."${tableName}"`;
  const columns = BONUS_REGISTRY_INSERT_COLUMNS.map((c) =>
    c === "date" || c === "row" ? `"${c}"` : c
  ).join(", ");
  const expressions = getPgInsertExpressions();
  const selectList = expressions.join(", ");
  let inserted = 0;
  const start = Date.now();
  try {
    while (inserted < count) {
      const currentBatch = Math.min(batchSize, count - inserted);
      const batchStart = Date.now();
      await sql.unsafe(
        `INSERT INTO ${fullTable} (${columns}) SELECT ${selectList} FROM generate_series(1, ${currentBatch}) AS n`
      );
      inserted += currentBatch;
      const batchMs = Date.now() - batchStart;
      const cumulativeMs = Date.now() - start;
      const line = `variant=${tableName} batchSize=${currentBatch} batchTime=${formatMs(batchMs)} cumulativeTime=${formatMs(cumulativeMs)} cumulativeRows=${inserted}\n`;
      logStream.write(line);
      process.stdout.write(line);
    }
  } finally {
    await sql.end();
  }
  const totalMs = Date.now() - start;
  const summary = `\ntotalRows=${inserted} totalTime=${formatMs(totalMs)} rowsPerSec=${(inserted / (totalMs / 1000)).toFixed(0)}\n`;
  logStream.write(summary);
  console.log(summary);
}

/** Trino: INSERT...SELECT с sequence + CROSS JOIN (как write-trino-mass). Батч до миллионов строк. */
async function fillTrino(
  table: TableVariant,
  count: number,
  batchSize: number,
  logStream: NodeJS.WritableStream
): Promise<void> {
  const trino = Trino.create({
    server: `http://${config.trino.host}:${config.trino.port}`,
    catalog: config.trino.catalog,
    schema: config.trino.schema,
    auth: new BasicAuth(config.trino.user),
  });
  const tableName = getTableName(table);
  const fullTable = `"${config.trino.catalog}"."bench"."${tableName}"`;
  const columns = BONUS_REGISTRY_INSERT_COLUMNS.map((c) =>
    c === "date" || c === "row" ? `"${c}"` : c
  ).join(", ");
  const columnExpressions = getTrinoInsertExpressions();
  const SEQUENCE_LIMIT = 10_000; // Trino sequence() лимит
  let inserted = 0;
  const start = Date.now();

  while (inserted < count) {
    const currentBatchSize = Math.min(batchSize, count - inserted);
    const level1Size = Math.ceil(currentBatchSize / SEQUENCE_LIMIT);
    const selectList = columnExpressions.join(", ");

    const insertSql = `
      INSERT INTO ${fullTable} (${columns})
      SELECT ${selectList}
      FROM UNNEST(sequence(0, ${level1Size - 1})) AS t1(level1)
      CROSS JOIN UNNEST(sequence(1, ${SEQUENCE_LIMIT})) AS t2(level2)
      WHERE (CAST(level1 AS BIGINT) * BIGINT '${SEQUENCE_LIMIT}' + level2) <= ${currentBatchSize}
    `;

    const batchStart = Date.now();
    const q = await trino.query(insertSql);
    for await (const row of q) {
      const r = row as { error?: { message?: string } };
      if (r?.error) throw new Error(`Trino: ${r.error.message}`);
    }
    inserted += currentBatchSize;
    const batchMs = Date.now() - batchStart;
    const cumulativeMs = Date.now() - start;
    const line = `variant=${tableName} batchSize=${currentBatchSize} batchTime=${formatMs(batchMs)} cumulativeTime=${formatMs(cumulativeMs)} cumulativeRows=${inserted}\n`;
    logStream.write(line);
    process.stdout.write(line);
  }

  const totalMs = Date.now() - start;
  const summary = `\ntotalRows=${inserted} totalTime=${formatMs(totalMs)} rowsPerSec=${(inserted / (totalMs / 1000)).toFixed(0)}\n`;
  logStream.write(summary);
  console.log(summary);
}

/** Таблица создаётся в PostgreSQL; при Trino мы пишем в ту же БД. */
async function ensureTableExists(table: TableVariant): Promise<void> {
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  try {
    await createTable(sql, table);
  } finally {
    await sql.end();
  }
}

async function main(): Promise<void> {
  const { table, count, batch } = parseArgs();
  const { stream: logStream, path: logPath } = openLogStream(table);
  logStream.write(
    `# bench-fill table=${table} count=${count} batch=${batch} mode=${config.bench.mode}\n`
  );

  await ensureTableExists(table);

  if (config.bench.mode === "trino") {
    await fillTrino(table, count, batch, logStream);
  } else {
    await fillPostgres(table, count, batch, logStream);
  }
  logStream.end();
  console.log("Log written to", logPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
