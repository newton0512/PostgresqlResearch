/**
 * Extended indexes for bench.bonus_registry_idx (bench-full_big).
 * Definitions derived from indexes.txt, adapted to snake_case column names.
 * Primary key (id) is not included — table already has it.
 */

import type { Sql } from "postgres";

const SCHEMA = "bench";
const TABLE = "bonus_registry_idx";

function escapeId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

const FULL_TABLE = `${escapeId(SCHEMA)}.${escapeId(TABLE)}`;

/** Index name (in bench schema) -> CREATE INDEX SQL. All for bench.bonus_registry_idx, snake_case columns. */
export const EXTENDED_INDEX_DEFS: { name: string; unique: boolean; createSql: string }[] = [
  {
    name: "br_idx_registrarTypeId_registrarId_row_manage_key",
    unique: true,
    createSql: `CREATE UNIQUE INDEX IF NOT EXISTS ${escapeId("br_idx_registrarTypeId_registrarId_row_manage_key")} ON ${FULL_TABLE} USING btree (registrar_type_id, registrar_id, "row", manager_id, bs_profile_id, accounted_for_bs_profile_id, first_name, first_name_latin, last_name, last_name_latin, departure_id, arrival_id, departure_date, currency_entry_id, bonus_type_id, action_source_id, bs_bonus_ticket_id, validity_time, date_of_expire, car_type_id, express_carrier_id, carrier_id, bs_partner_id, bs_train_number_id, bs_tourism_train_id, accounted_in_calculation, cancelled, bs_quota_id, doc_to_track_type_id, doc_to_track_id, doc_to_track_date)`,
  },
  {
    name: "br_idx_registrarTypeId_registrarId_row_key",
    unique: true,
    createSql: `CREATE UNIQUE INDEX IF NOT EXISTS ${escapeId("br_idx_registrarTypeId_registrarId_row_key")} ON ${FULL_TABLE} USING btree (registrar_type_id, registrar_id, "row")`,
  },
  {
    name: "br_idx_accountedForBsProfileId_bonusTypeId_ca",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_accountedForBsProfileId_bonusTypeId_ca")} ON ${FULL_TABLE} USING btree (accounted_for_bs_profile_id, bonus_type_id, cancelled, date_of_expire)`,
  },
  {
    name: "br_idx_registrarTypeId_registrarId",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_registrarTypeId_registrarId")} ON ${FULL_TABLE} USING btree (registrar_type_id, registrar_id)`,
  },
  {
    name: "br_idx_docToTrackTypeId_docToTrackId",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_docToTrackTypeId_docToTrackId")} ON ${FULL_TABLE} USING btree (doc_to_track_type_id, doc_to_track_id)`,
  },
  {
    name: "br_idx_date_cancelled_bonusTypeId_registrarTy",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_date_cancelled_bonusTypeId_registrarTy")} ON ${FULL_TABLE} USING btree ("date", cancelled, bonus_type_id, registrar_type_id)`,
  },
  {
    name: "br_idx_registrarTypeId_cancelled_row_bsQuotaI",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_registrarTypeId_cancelled_row_bsQuotaI")} ON ${FULL_TABLE} USING btree (registrar_type_id, cancelled, "row", bs_quota_id)`,
  },
  {
    name: "br_idx_dateOfExpire_cancelled_bonusTypeId",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_dateOfExpire_cancelled_bonusTypeId")} ON ${FULL_TABLE} USING btree (date_of_expire, cancelled, bonus_type_id)`,
  },
  {
    name: "br_idx_bsProfileId_cancelled",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_bsProfileId_cancelled")} ON ${FULL_TABLE} USING btree (bs_profile_id, cancelled)`,
  },
  {
    name: "br_idx_accountedForBsProfileId_cancelled",
    unique: false,
    createSql: `CREATE INDEX IF NOT EXISTS ${escapeId("br_idx_accountedForBsProfileId_cancelled")} ON ${FULL_TABLE} USING btree (accounted_for_bs_profile_id, cancelled)`,
  },
];

export function getExtendedIndexNames(): string[] {
  return EXTENDED_INDEX_DEFS.map((d) => d.name);
}

/** Create all extended indexes on bench.bonus_registry_idx. Returns total time in ms. */
export async function createExtendedIndexesForIdx(
  sql: Sql,
  onProgress?: (indexName: string, ms: number) => void
): Promise<number> {
  let totalMs = 0;
  for (const def of EXTENDED_INDEX_DEFS) {
    const t0 = performance.now();
    await sql.unsafe(def.createSql);
    const ms = performance.now() - t0;
    totalMs += ms;
    onProgress?.(def.name, ms);
  }
  return totalMs;
}

/** Drop all extended indexes (by name). PK is not dropped. */
export async function dropExtendedIndexesForIdx(sql: Sql): Promise<void> {
  for (const name of getExtendedIndexNames()) {
    await sql.unsafe(`DROP INDEX IF EXISTS ${escapeId(SCHEMA)}.${escapeId(name)}`);
  }
}
