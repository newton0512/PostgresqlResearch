/**
 * Configuration from environment.
 */

export const config = {
  pg: {
    host: process.env.PG_HOST ?? "localhost",
    port: Number(process.env.PG_PORT ?? "5432"),
    user: process.env.PG_USER ?? "postgres",
    password: process.env.PG_PASSWORD ?? "postgres",
    database: process.env.PG_DATABASE ?? "appdb",
  },
  trino: {
    host: process.env.TRINO_HOST ?? "localhost",
    port: Number(process.env.TRINO_PORT ?? "8080"),
    catalog: process.env.TRINO_CATALOG ?? "postgres",
    schema: process.env.TRINO_SCHEMA ?? "bench",
    user: process.env.TRINO_USER ?? "trino",
  },
  bench: {
    mode: (process.env.BENCH_MODE ?? "postgres") as "postgres" | "trino",
    batchSize: Number(process.env.BATCH_SIZE ?? "100000000"),
    recordMax: Number(process.env.RECORD_MAX ?? "500000000"),
    tableVariant: (process.env.TABLE_VARIANT ?? "plain") as
      | "plain"
      | "part"
      | "idx"
      | "idx_part",
  },
  api: {
    port: Number(process.env.API_PORT ?? "3000"),
  },
};

export type TableVariant = "plain" | "part" | "idx" | "idx_part";

export const TABLE_VARIANTS: TableVariant[] = [
  "plain",
  "part",
  "idx",
  "idx_part",
];
