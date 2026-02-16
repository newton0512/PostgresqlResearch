/**
 * SQL-выражения Trino для INSERT...SELECT в bonus_registry.
 * Как в samples-generation write-trino-mass: данные генерируются в Trino (sequence + CROSS JOIN),
 * один INSERT вставляет весь батч (до миллионов строк). Порядок колонок = BONUS_REGISTRY_INSERT_COLUMNS.
 */

const REGISTRAR_TYPE_IDS = [
  "bsBonusReceiveForTrip", "bsRecoveryRequestDoc", "bsTripForBonusDoc", "bsBonusDocument",
  "bsCustomTransaction", "bsCharityDocument", "bsExpirationDocument", "bsReturnDocument",
  "bsSurveyDoc", "bsCompensationDoc", "bsSouvenirRequest", "bsAdvanceDoc", "bsReturnAdvanceDoc",
];
const BONUS_TYPE_IDS = ["premial", "qualification"];
const ACTION_SOURCE_IDS = ["operator", "auto"];
const CARRIER_IDS = ["fpk", "tver", "rzd"];
const OPERATION_DOC_TYPE_IDS = [
  "operation_transfer", "operation_status_assignment", "operation_manual_bonus",
];

function esc(s: string): string {
  return "'" + s.replace(/'/g, "''") + "'";
}

function nullable(expr: string, probability: number): string {
  if (probability <= 0) return expr;
  if (probability >= 1) return "NULL";
  return `CASE WHEN random() < ${probability} THEN NULL ELSE (${expr}) END`;
}

function arrChoice(literals: string[]): string {
  const arr = literals.map(esc).join(", ");
  return `element_at(ARRAY[${arr}], CAST(floor(random() * ${literals.length}) + 1 AS INTEGER))`;
}

function randomString(minLen: number, maxLen: number): string {
  const len = maxLen - minLen + 1;
  return `substr(replace(cast(uuid() as varchar), '-', ''), 1, CAST(floor(random() * ${len} + ${minLen}) AS INTEGER))`;
}

const ts2020 = Math.floor(new Date("2020-01-01").getTime() / 1000);
const tsNow = Math.floor(Date.now() / 1000);
const ts2025 = Math.floor(new Date("2025-12-31").getTime() / 1000);

/** Выражения для одной строки (Trino). Не используют row_num; для CROSS JOIN подойдут как есть. */
export function getTrinoInsertExpressions(): string[] {
  return [
    "CAST(uuid() AS VARCHAR)",
    nullable(`from_unixtime(CAST(floor(random() * (${tsNow - ts2020}) + ${ts2020}) AS BIGINT))`, 0.2),
    nullable(arrChoice(REGISTRAR_TYPE_IDS), 0.2),
    nullable("CAST(uuid() AS VARCHAR)", 0.2),
    nullable("CAST(floor(random() * 10 + 1) AS INTEGER)", 0.2),
    nullable("CAST(floor(random() * 1000 + 1) AS INTEGER)", 0.3),
    "CAST(uuid() AS VARCHAR)",
    "CAST(uuid() AS VARCHAR)",
    nullable("CAST(uuid() AS VARCHAR)", 0.4),
    nullable("CAST(uuid() AS VARCHAR)", 0.4),
    nullable("CAST(uuid() AS VARCHAR)", 0.4),
    nullable("CAST(uuid() AS VARCHAR)", 0.4),
    nullable("CAST(floor(random() * 100 + 1) AS INTEGER)", 0.5),
    nullable("CAST(floor(random() * 100 + 1) AS INTEGER)", 0.5),
    nullable(`CAST(from_unixtime(CAST(floor(random() * (${tsNow - ts2020}) + ${ts2020}) AS BIGINT)) AS DATE)`, 0.5),
    nullable("CAST(floor(random() * 10 + 1) AS INTEGER)", 0.5),
    arrChoice(BONUS_TYPE_IDS),
    arrChoice(ACTION_SOURCE_IDS),
    nullable("CAST(uuid() AS VARCHAR)", 0.8),
    nullable("CAST(floor(random() * 336 + 30) AS INTEGER)", 0.6),
    nullable(`CAST(from_unixtime(CAST(floor(random() * (${ts2025 - tsNow}) + ${tsNow}) AS BIGINT)) AS DATE)`, 0.6),
    nullable(randomString(5, 10), 0.8),
    nullable("CAST(floor(random() * 50 + 1) AS INTEGER)", 0.7),
    nullable(arrChoice(CARRIER_IDS), 0.6),
    nullable("CAST(floor(random() * 20 + 1) AS INTEGER)", 0.7),
    nullable(randomString(5, 15), 0.8),
    nullable(randomString(5, 15), 0.8),
    "CAST(floor(random() * 2) AS INTEGER) = 1",
    "CAST(floor(random() * 2) AS INTEGER) = 1",
    nullable("CAST(floor(random() * 100 + 1) AS INTEGER)", 0.8),
    arrChoice(REGISTRAR_TYPE_IDS),
    "CAST(uuid() AS VARCHAR)",
    nullable(`CAST(from_unixtime(CAST(floor(random() * (${tsNow - ts2020}) + ${ts2020}) AS BIGINT)) AS DATE)`, 0.5),
    nullable(`CAST(from_unixtime(CAST(floor(random() * (${tsNow - ts2020}) + ${ts2020}) AS BIGINT)) AS DATE)`, 0.7),
    "CAST(floor(random() * 2) AS INTEGER) = 1",
    nullable(randomString(10, 20), 0.8),
    nullable("CAST(floor(random() * 9901 + 100) AS INTEGER)", 0.8),
    "CAST(floor(random() * 11001 - 1000) AS INTEGER)",
    nullable(randomString(5, 15), 0.8),
    nullable("CAST(floor(random() * 5 + 1) AS INTEGER)", 0.8),
    nullable(`from_unixtime(CAST(floor(random() * (${ts2025 - tsNow}) + ${tsNow}) AS BIGINT))`, 0.9),
    "CAST(floor(random() * 2) AS INTEGER) = 1",
    "CAST(floor(random() * 2) AS INTEGER) = 1",
    "CAST(floor(random() * 2) AS INTEGER) = 1",
    nullable("CAST(uuid() AS VARCHAR)", 0.7),
    nullable("CAST(floor(random() * 4901 + 100) AS INTEGER)", 0.7),
    nullable("CAST(floor(random() * 491 + 10) AS INTEGER)", 0.8),
    nullable(arrChoice(OPERATION_DOC_TYPE_IDS), 0.3),
    "CAST(floor(random() * 2) AS INTEGER) = 1",
    nullable(`CAST(from_unixtime(CAST(floor(random() * (${tsNow - ts2020}) + ${ts2020}) AS BIGINT)) AS DATE)`, 0.9),
    `from_unixtime(CAST(floor(random() * (${tsNow - ts2020}) + ${ts2020}) AS BIGINT))`,
    "current_timestamp",
  ];
}
