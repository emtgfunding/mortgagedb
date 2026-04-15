/**
 * Schema migration runner.
 *
 * Reads schema.sql and executes it against DATABASE_URL. The schema file is
 * written to be fully idempotent (CREATE ... IF NOT EXISTS, ALTER TABLE ADD
 * COLUMN IF NOT EXISTS, DROP TRIGGER IF EXISTS before CREATE TRIGGER), so it
 * is safe to run on every boot.
 *
 * Usage:
 *   node migrate.js            # apply schema, exit
 *   require('./migrate').run() # called by index.js on startup
 */

const fs = require('fs');
const path = require('path');
const pool = require('./db');

const SCHEMA_PATH = path.join(__dirname, 'schema.sql');

async function run({ verbose = true } = {}) {
  if (!fs.existsSync(SCHEMA_PATH)) {
    throw new Error(`schema.sql not found at ${SCHEMA_PATH}`);
  }
  const sql = fs.readFileSync(SCHEMA_PATH, 'utf8');
  const started = Date.now();

  if (verbose) console.log('🗄️  Applying schema.sql...');

  // pg supports multi-statement queries in a single .query() call. The
  // schema file uses $$ ... $$ for the trigger function body, and the node
  // pg driver handles those correctly.
  await pool.query(sql);

  const ms = Date.now() - started;
  if (verbose) console.log(`✅ Schema applied (${ms}ms)`);
}

if (require.main === module) {
  run()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('❌ Migration failed:', err.message);
      process.exit(1);
    });
}

module.exports = { run };
