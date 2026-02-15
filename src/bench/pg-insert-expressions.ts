/**
 * SQL-выражения PostgreSQL для INSERT...SELECT в bonus_registry.
 * Данные генерируются в БД (generate_series + random/gen_random_uuid), без параметров из Node.
 * Как в samples-generation: один INSERT вставляет батч строк (например 50_000).
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
  return `(ARRAY[${arr}])[floor(random() * ${literals.length} + 1)::int]`;
}

/** Выражения для одной строки: порядок колонок как в BONUS_REGISTRY_INSERT_COLUMNS. Используют n из generate_series(1, N) AS n. */
export function getPgInsertExpressions(): string[] {
  const ts2020 = Math.floor(new Date("2020-01-01").getTime() / 1000);
  const tsNow = Math.floor(Date.now() / 1000);
  const ts2025 = Math.floor(new Date("2025-12-31").getTime() / 1000);
  const randomStr = (minLen: number, maxLen: number) =>
    `substr(replace(gen_random_uuid()::text, '-', ''), 1, (floor(random() * (${maxLen - minLen + 1}) + ${minLen})::int))`;

  return [
    "gen_random_uuid()::text",
    nullable(`to_timestamp(${ts2020} + floor(random() * (${tsNow - ts2020}))::bigint)`, 0.2),
    nullable(arrChoice(REGISTRAR_TYPE_IDS), 0.2),
    nullable("gen_random_uuid()::text", 0.2),
    nullable("(floor(random() * 10 + 1)::int)", 0.2),
    nullable("(floor(random() * 1000 + 1)::int)", 0.3),
    "gen_random_uuid()::text",
    "gen_random_uuid()::text",
    nullable("gen_random_uuid()::text", 0.4),
    nullable("gen_random_uuid()::text", 0.4),
    nullable("gen_random_uuid()::text", 0.4),
    nullable("gen_random_uuid()::text", 0.4),
    nullable("(floor(random() * 100 + 1)::int)", 0.5),
    nullable("(floor(random() * 100 + 1)::int)", 0.5),
    nullable(`(to_timestamp(${ts2020} + floor(random() * (${tsNow - ts2020}))::bigint))::date`, 0.5),
    nullable("(floor(random() * 10 + 1)::int)", 0.5),
    arrChoice(BONUS_TYPE_IDS),
    arrChoice(ACTION_SOURCE_IDS),
    nullable("gen_random_uuid()::text", 0.8),
    nullable("(floor(random() * 336 + 30)::int)", 0.6),
    nullable(`(to_timestamp(${tsNow} + floor(random() * (${ts2025 - tsNow}))::bigint))::date`, 0.6),
    nullable(randomStr(5, 10), 0.8),
    nullable("(floor(random() * 50 + 1)::int)", 0.7),
    nullable(arrChoice(CARRIER_IDS), 0.6),
    nullable("(floor(random() * 20 + 1)::int)", 0.7),
    nullable(randomStr(5, 15), 0.8),
    nullable(randomStr(5, 15), 0.8),
    "(floor(random() * 2)::int)::boolean",
    "(floor(random() * 2)::int)::boolean",
    nullable("(floor(random() * 100 + 1)::int)", 0.8),
    arrChoice(REGISTRAR_TYPE_IDS),
    "gen_random_uuid()::text",
    nullable(`(to_timestamp(${ts2020} + floor(random() * (${tsNow - ts2020}))::bigint))::date`, 0.5),
    nullable(`(to_timestamp(${ts2020} + floor(random() * (${tsNow - ts2020}))::bigint))::date`, 0.7),
    "(floor(random() * 2)::int)::boolean",
    nullable(randomStr(10, 20), 0.8),
    nullable("(floor(random() * 9901 + 100)::int)", 0.8),
    "(floor(random() * 11001 - 1000)::int)",
    nullable(randomStr(5, 15), 0.8),
    nullable("(floor(random() * 5 + 1)::int)", 0.8),
    nullable(`to_timestamp(${tsNow} + floor(random() * (${ts2025 - tsNow}))::bigint)`, 0.9),
    "(floor(random() * 2)::int)::boolean",
    "(floor(random() * 2)::int)::boolean",
    "(floor(random() * 2)::int)::boolean",
    nullable("gen_random_uuid()::text", 0.7),
    nullable("(floor(random() * 4901 + 100)::int)", 0.7),
    nullable("(floor(random() * 491 + 10)::int)", 0.8),
    nullable(arrChoice(OPERATION_DOC_TYPE_IDS), 0.3),
    "(floor(random() * 2)::int)::boolean",
    nullable(`(to_timestamp(${ts2020} + floor(random() * (${tsNow - ts2020}))::bigint))::date`, 0.9),
    `to_timestamp(${ts2020} + floor(random() * (${tsNow - ts2020}))::bigint)`,
    "CURRENT_TIMESTAMP",
  ];
}
