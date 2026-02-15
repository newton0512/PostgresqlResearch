/**
 * Drop bonus_registry table(s) in schema bench.
 * Usage: pnpm run drop:tables [--table plain|part|idx|idx_part] [--all]
 */

import "dotenv/config";
import postgres from "postgres";
import {
  dropTable,
  dropAllTables,
  TABLE_VARIANTS,
  getTableName,
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
      await dropAllTables(sql);
      console.log("Dropped all 4 table variants: plain, part, idx, idx_part.");
    } else {
      await dropTable(sql, table!);
      console.log(`Dropped table: bench.${getTableName(table!)}`);
    }
  } finally {
    await sql.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
