/**
 * Create bonus_registry table(s). One variant or all four.
 * Usage: pnpm run setup:tables [--table plain|part|idx|idx_part] [--all]
 * Tables are always created in PostgreSQL (visible to Trino when using postgres catalog).
 */

import "dotenv/config";
import postgres from "postgres";
import {
  createTable,
  createAllTables,
  TABLE_VARIANTS,
  type TableVariant,
} from "../src/bench/create-tables.js";
import { config } from "../src/config.js";

function parseArgs(): { table?: TableVariant; all: boolean } {
  const args = process.argv.slice(2);
  let table: TableVariant | undefined;
  let all = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--table" && args[i + 1]) {
      const v = args[++i] as TableVariant;
      if (TABLE_VARIANTS.includes(v)) table = v;
    } else if (args[i] === "--all") {
      all = true;
    }
  }
  const variant = (table ?? config.bench.tableVariant) as TableVariant;
  return { table: variant, all };
}

async function main(): Promise<void> {
  const { table, all } = parseArgs();
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  try {
    if (all) {
      await createAllTables(sql);
      console.log("Created all 4 table variants: plain, part, idx, idx_part.");
    } else {
      await createTable(sql, table!);
      console.log(`Created table: bench.${table}`);
    }
  } finally {
    await sql.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
