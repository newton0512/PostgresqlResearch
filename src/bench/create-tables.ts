/**
 * DDL for bonus_registry table variants in PostgreSQL.
 * Four variants: plain, part (hash 64), idx, idx_part.
 */

import type { Sql } from "postgres";

const SCHEMA = "bench";

/** Column definitions for bonus_registry (PostgreSQL types). No PRIMARY KEY here; added per variant. */
export const BONUS_REGISTRY_COLUMNS_DDL = `
  id VARCHAR(255),
  "date" TIMESTAMP,
  registrar_type_id VARCHAR(255),
  registrar_id VARCHAR(255),
  "row" INTEGER,
  manager_id INTEGER,
  bs_profile_id VARCHAR(255) NOT NULL,
  accounted_for_bs_profile_id VARCHAR(255) NOT NULL,
  first_name VARCHAR(255),
  first_name_latin VARCHAR(255),
  last_name VARCHAR(255),
  last_name_latin VARCHAR(255),
  departure_id INTEGER,
  arrival_id INTEGER,
  departure_date DATE,
  currency_entry_id INTEGER,
  bonus_type_id VARCHAR(255) NOT NULL,
  action_source_id VARCHAR(255) NOT NULL,
  bs_bonus_ticket_id VARCHAR(255),
  validity_time INTEGER,
  date_of_expire DATE,
  car_type_id VARCHAR(255),
  express_carrier_id INTEGER,
  carrier_id VARCHAR(255),
  bs_partner_id INTEGER,
  bs_train_number_id VARCHAR(255),
  bs_tourism_train_id VARCHAR(255),
  accounted_in_calculation BOOLEAN,
  cancelled BOOLEAN,
  bs_quota_id INTEGER,
  doc_to_track_type_id VARCHAR(255) NOT NULL,
  doc_to_track_id VARCHAR(255) NOT NULL,
  doc_to_track_date DATE,
  active_date DATE,
  trip_for_another_person BOOLEAN,
  ticket_number VARCHAR(255),
  currency_amount INTEGER,
  amount INTEGER NOT NULL,
  bs_partner_bonus_type_id VARCHAR(255),
  express_service_class_id INTEGER,
  date_to_cancelled TIMESTAMP,
  prolongable BOOLEAN,
  active_by_trips BOOLEAN,
  is_empty BOOLEAN,
  amount_calculation VARCHAR(255),
  distance INTEGER,
  addition_amount INTEGER,
  operation_doc_type_id VARCHAR(255),
  is_merged BOOLEAN,
  merged_date DATE,
  created_at TIMESTAMP,
  ingested_at TIMESTAMP
`.trim();

export const TABLE_VARIANTS = ["plain", "part", "idx", "idx_part"] as const;
export type TableVariant = (typeof TABLE_VARIANTS)[number];

function escapeId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

export async function ensureSchema(sql: Sql): Promise<void> {
  await sql.unsafe(`CREATE SCHEMA IF NOT EXISTS ${escapeId(SCHEMA)}`);
}

/**
 * Create bonus_registry_plain: no indexes, no partitioning.
 */
export async function createPlain(sql: Sql): Promise<void> {
  await ensureSchema(sql);
  const table = `${escapeId(SCHEMA)}.${escapeId("bonus_registry_plain")}`;
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS ${table} (${BONUS_REGISTRY_COLUMNS_DDL}, PRIMARY KEY (id))
  `);
}

/**
 * Create bonus_registry_part: hash partitioning on accounted_for_bs_profile_id (64 buckets), no extra index.
 * Primary key must include partition key in PostgreSQL.
 */
export async function createPart(sql: Sql): Promise<void> {
  await ensureSchema(sql);
  const table = `${escapeId(SCHEMA)}.${escapeId("bonus_registry_part")}`;
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS ${table} (${BONUS_REGISTRY_COLUMNS_DDL}, PRIMARY KEY (id, accounted_for_bs_profile_id))
    PARTITION BY HASH (accounted_for_bs_profile_id)
  `);
  for (let r = 0; r < 64; r++) {
    const partName = `${escapeId(SCHEMA)}.${escapeId("bonus_registry_part_" + r)}`;
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS ${partName}
      PARTITION OF ${table} FOR VALUES WITH (MODULUS 64, REMAINDER ${r})
    `);
  }
}

/**
 * Create bonus_registry_idx: index on accounted_for_bs_profile_id, no partitioning.
 */
export async function createIdx(sql: Sql): Promise<void> {
  await ensureSchema(sql);
  const table = `${escapeId(SCHEMA)}.${escapeId("bonus_registry_idx")}`;
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS ${table} (${BONUS_REGISTRY_COLUMNS_DDL}, PRIMARY KEY (id))
  `);
  await sql.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_bonus_registry_idx_accounted
    ON ${table} (accounted_for_bs_profile_id)
  `);
}

/**
 * Create bonus_registry_idx_part: index + hash partitioning (64 buckets).
 * Primary key must include partition key.
 */
export async function createIdxPart(sql: Sql): Promise<void> {
  await ensureSchema(sql);
  const table = `${escapeId(SCHEMA)}.${escapeId("bonus_registry_idx_part")}`;
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS ${table} (${BONUS_REGISTRY_COLUMNS_DDL}, PRIMARY KEY (id, accounted_for_bs_profile_id))
    PARTITION BY HASH (accounted_for_bs_profile_id)
  `);
  for (let r = 0; r < 64; r++) {
    const partName = `${escapeId(SCHEMA)}.${escapeId("bonus_registry_idx_part_" + r)}`;
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS ${partName}
      PARTITION OF ${table} FOR VALUES WITH (MODULUS 64, REMAINDER ${r})
    `);
  }
  await sql.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_bonus_registry_idx_part_accounted
    ON ${table} (accounted_for_bs_profile_id)
  `);
}

export function getTableName(variant: TableVariant): string {
  return `bonus_registry_${variant}`;
}

export function getFullTableName(variant: TableVariant): string {
  return `${escapeId(SCHEMA)}.${escapeId(getTableName(variant))}`;
}

const creators: Record<TableVariant, (sql: Sql) => Promise<void>> = {
  plain: createPlain,
  part: createPart,
  idx: createIdx,
  idx_part: createIdxPart,
};

export async function createTable(
  sql: Sql,
  variant: TableVariant
): Promise<void> {
  await creators[variant](sql);
}

export async function createAllTables(sql: Sql): Promise<void> {
  await ensureSchema(sql);
  for (const v of TABLE_VARIANTS) {
    await createTable(sql, v);
  }
}

/**
 * Drop one bonus_registry table (and its partitions if partitioned). CASCADE to remove dependent objects.
 */
export async function dropTable(sql: Sql, variant: TableVariant): Promise<void> {
  const tableName = getTableName(variant);
  const fullName = `${escapeId(SCHEMA)}.${escapeId(tableName)}`;
  await sql.unsafe(`DROP TABLE IF EXISTS ${fullName} CASCADE`);
}

/**
 * Drop all four bonus_registry table variants in schema bench.
 */
export async function dropAllTables(sql: Sql): Promise<void> {
  for (const v of TABLE_VARIANTS) {
    await dropTable(sql, v);
  }
}

export { SCHEMA };
