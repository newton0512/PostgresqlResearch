/**
 * API server for K6 insert-one benchmark.
 * POST /api/insert-one — insert one row into configured table (or all variants).
 * Query or body: table=plain|part|idx|idx_part (optional, default from env).
 */

import "dotenv/config";
import express from "express";
import postgres from "postgres";
import { BasicAuth, Trino } from "trino-client";
import { config } from "../config.js";
import { getTableName, TABLE_VARIANTS, type TableVariant } from "../bench/create-tables.js";
import { BONUS_REGISTRY_INSERT_COLUMNS, generateRow } from "../bench/row-generator.js";

type RowRecord = ReturnType<typeof generateRow>;

const DATE_COLUMNS = new Set([
  "date",
  "departure_date",
  "date_of_expire",
  "doc_to_track_date",
  "active_date",
  "date_to_cancelled",
  "merged_date",
  "created_at",
  "ingested_at",
]);

/** Merge partial row from request body with generateRow(); normalize date strings to Date. */
function buildRowFromBody(body: Record<string, unknown>): RowRecord {
  const base = generateRow();
  const row = { ...base } as Record<string, string | number | boolean | Date | null>;
  for (const key of BONUS_REGISTRY_INSERT_COLUMNS) {
    if (!Object.prototype.hasOwnProperty.call(body, key)) continue;
    const v = body[key];
    if (DATE_COLUMNS.has(key) && typeof v === "string") {
      const d = new Date(v);
      row[key] = isNaN(d.getTime()) ? base[key] : d;
    } else if (v === null || typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
      row[key] = v;
    }
  }
  return row as RowRecord;
}

const app = express();
app.use(express.json({ limit: "1mb" }));

let pgSql: ReturnType<typeof postgres> | null = null;

function getPg(): ReturnType<typeof postgres> {
  if (!pgSql) {
    pgSql = postgres({
      host: config.pg.host,
      port: config.pg.port,
      user: config.pg.user,
      password: config.pg.password,
      database: config.pg.database,
    });
  }
  return pgSql;
}

let trinoClient: ReturnType<typeof Trino.create> | null = null;

function getTrino(): ReturnType<typeof Trino.create> {
  if (!trinoClient) {
    trinoClient = Trino.create({
      server: `http://${config.trino.host}:${config.trino.port}`,
      catalog: config.trino.catalog,
      schema: config.trino.schema,
      auth: new BasicAuth(config.trino.user),
    });
  }
  return trinoClient;
}

async function insertOnePostgres(table: TableVariant, row: RowRecord): Promise<void> {
  const sql = getPg();
  const tableName = getTableName(table);
  const fullTable = sql.unsafe(`"bench"."${tableName}"`);
  await sql`INSERT INTO ${fullTable} ${sql([row])}`;
}

async function insertOneTrino(table: TableVariant, row: RowRecord): Promise<void> {
  const trino = getTrino();
  const tableName = getTableName(table);
  const fullTable = `"${config.trino.catalog}"."bench"."${tableName}"`;
  const esc = (v: string | number | boolean | Date | null): string => {
    if (v == null) return "NULL";
    if (v instanceof Date) return `TIMESTAMP '${v.toISOString().slice(0, 19).replace("T", " ")}'`;
    if (typeof v === "boolean") return v ? "true" : "false";
    if (typeof v === "number") return String(v);
    return `'${String(v).replace(/'/g, "''")}'`;
  };
  const values = BONUS_REGISTRY_INSERT_COLUMNS.map((c) => esc(row[c] ?? null)).join(", ");
  const cols = BONUS_REGISTRY_INSERT_COLUMNS.map((c) => (c === "date" || c === "row" ? `"${c}"` : c)).join(", ");
  const sql = `INSERT INTO ${fullTable} (${cols}) VALUES (${values})`;
  const q = await trino.query(sql);
  for await (const r of q) {
    const err = (r as { error?: { message?: string } }).error;
    if (err) throw new Error(err.message);
  }
}

app.post("/api/insert-one", async (req, res) => {
  const table = (req.query.table ?? req.body?.table ?? config.bench.tableVariant) as string;
  const variant = TABLE_VARIANTS.includes(table as TableVariant) ? (table as TableVariant) : (config.bench.tableVariant as TableVariant);
  const body = req.body && typeof req.body === "object" ? (req.body as Record<string, unknown>) : {};
  const hasRowData = Object.prototype.hasOwnProperty.call(body, "id") || Object.prototype.hasOwnProperty.call(body, "amount");
  const row = hasRowData ? buildRowFromBody(body) : generateRow();
  try {
    if (config.bench.mode === "trino") {
      await insertOneTrino(variant, row);
    } else {
      await insertOnePostgres(variant, row);
    }
    res.status(201).json({ ok: true, table: getTableName(variant) });
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: e instanceof Error ? e.message : String(e),
    });
  }
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

const port = config.api.port;
app.listen(port, () => {
  console.log(`API server listening on port ${port} (BENCH_MODE=${config.bench.mode})`);
});
