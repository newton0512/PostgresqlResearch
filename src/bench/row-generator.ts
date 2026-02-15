/**
 * Generate one bonus_registry row as plain values (for INSERT).
 * Column order matches BONUS_REGISTRY_INSERT_COLUMNS / DDL.
 */

const REGISTRAR_TYPE_IDS = [
  "bsBonusReceiveForTrip",
  "bsRecoveryRequestDoc",
  "bsTripForBonusDoc",
  "bsBonusDocument",
  "bsCustomTransaction",
  "bsCharityDocument",
  "bsExpirationDocument",
  "bsReturnDocument",
  "bsSurveyDoc",
  "bsCompensationDoc",
  "bsSouvenirRequest",
  "bsAdvanceDoc",
  "bsReturnAdvanceDoc",
];

const BONUS_TYPE_IDS = ["premial", "qualification"];
const ACTION_SOURCE_IDS = ["operator", "auto"];
const CARRIER_IDS = ["fpk", "tver", "rzd"];
const OPERATION_DOC_TYPE_IDS = [
  "operation_transfer",
  "operation_status_assignment",
  "operation_manual_bonus",
];

export const BONUS_REGISTRY_INSERT_COLUMNS = [
  "id",
  "date",
  "registrar_type_id",
  "registrar_id",
  "row",
  "manager_id",
  "bs_profile_id",
  "accounted_for_bs_profile_id",
  "first_name",
  "first_name_latin",
  "last_name",
  "last_name_latin",
  "departure_id",
  "arrival_id",
  "departure_date",
  "currency_entry_id",
  "bonus_type_id",
  "action_source_id",
  "bs_bonus_ticket_id",
  "validity_time",
  "date_of_expire",
  "car_type_id",
  "express_carrier_id",
  "carrier_id",
  "bs_partner_id",
  "bs_train_number_id",
  "bs_tourism_train_id",
  "accounted_in_calculation",
  "cancelled",
  "bs_quota_id",
  "doc_to_track_type_id",
  "doc_to_track_id",
  "doc_to_track_date",
  "active_date",
  "trip_for_another_person",
  "ticket_number",
  "currency_amount",
  "amount",
  "bs_partner_bonus_type_id",
  "express_service_class_id",
  "date_to_cancelled",
  "prolongable",
  "active_by_trips",
  "is_empty",
  "amount_calculation",
  "distance",
  "addition_amount",
  "operation_doc_type_id",
  "is_merged",
  "merged_date",
  "created_at",
  "ingested_at",
] as const;

function uuid(): string {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randBool(probabilityNull: number): boolean | null {
  if (Math.random() < probabilityNull) return null;
  return Math.random() < 0.5;
}

function randStr(minLen: number, maxLen: number): string {
  const len = randInt(minLen, maxLen);
  const s = uuid().replace(/-/g, "").slice(0, len);
  return s + "x".repeat(Math.max(0, len - s.length)).slice(0, len);
}

function randDate(start: Date, end: Date): Date {
  const ts = start.getTime() + Math.random() * (end.getTime() - start.getTime());
  return new Date(ts);
}

function choice<T>(arr: readonly T[]): T {
  return arr[randInt(0, arr.length - 1)]!;
}

/** Generate one row as array of values in column order. */
export function generateRowArray(): (string | number | boolean | Date | null)[] {
  const ts2020 = new Date("2020-01-01").getTime();
  const tsNow = Date.now();
  const ts2025 = new Date("2025-12-31").getTime();
  const nullable = <T>(v: T, probNull: number): T | null =>
    Math.random() < probNull ? null : v;

  return [
    uuid(),
    nullable(new Date(ts2020 + Math.random() * (tsNow - ts2020)), 0.2),
    nullable(choice(REGISTRAR_TYPE_IDS), 0.2),
    nullable(uuid(), 0.2),
    nullable(randInt(1, 10), 0.2),
    nullable(randInt(1, 1000), 0.3),
    uuid(),
    uuid(),
    nullable(uuid().slice(0, 20), 0.4),
    nullable(uuid().slice(0, 20), 0.4),
    nullable(uuid().slice(0, 20), 0.4),
    nullable(uuid().slice(0, 20), 0.4),
    nullable(randInt(1, 100), 0.5),
    nullable(randInt(1, 100), 0.5),
    nullable(randDate(new Date(ts2020), new Date(tsNow)), 0.5),
    nullable(randInt(1, 10), 0.5),
    choice(BONUS_TYPE_IDS),
    choice(ACTION_SOURCE_IDS),
    nullable(uuid(), 0.8),
    nullable(randInt(30, 366), 0.6),
    nullable(randDate(new Date(tsNow), new Date(ts2025)), 0.6),
    nullable(randStr(5, 10), 0.8),
    nullable(randInt(1, 50), 0.7),
    nullable(choice(CARRIER_IDS), 0.6),
    nullable(randInt(1, 20), 0.7),
    nullable(randStr(5, 15), 0.8),
    nullable(randStr(5, 15), 0.8),
    randBool(0.7),
    randBool(0.7),
    nullable(randInt(1, 100), 0.8),
    choice(REGISTRAR_TYPE_IDS),
    uuid(),
    nullable(randDate(new Date(ts2020), new Date(tsNow)), 0.5),
    nullable(randDate(new Date(ts2020), new Date(tsNow)), 0.7),
    randBool(0.7),
    nullable(randStr(10, 20), 0.8),
    nullable(randInt(100, 10000), 0.8),
    randInt(-1000, 10000),
    nullable(randStr(5, 15), 0.8),
    nullable(randInt(1, 5), 0.8),
    nullable(randDate(new Date(tsNow), new Date(ts2025)), 0.9),
    randBool(0.7),
    randBool(0.7),
    randBool(0.7),
    nullable(uuid(), 0.7),
    nullable(randInt(100, 5000), 0.7),
    nullable(randInt(10, 500), 0.8),
    nullable(choice(OPERATION_DOC_TYPE_IDS), 0.3),
    randBool(0.7),
    nullable(randDate(new Date(ts2020), new Date(tsNow)), 0.9),
    new Date(ts2020 + Math.random() * (tsNow - ts2020)),
    new Date(),
  ];
}

/** Generate one row as object (column name -> value). */
export function generateRow(): Record<(typeof BONUS_REGISTRY_INSERT_COLUMNS)[number], string | number | boolean | Date | null> {
  const arr = generateRowArray();
  const obj = {} as Record<string, string | number | boolean | Date | null>;
  for (let i = 0; i < BONUS_REGISTRY_INSERT_COLUMNS.length; i++) {
    obj[BONUS_REGISTRY_INSERT_COLUMNS[i]!] = arr[i]!;
  }
  return obj as Record<(typeof BONUS_REGISTRY_INSERT_COLUMNS)[number], string | number | boolean | Date | null>;
}
