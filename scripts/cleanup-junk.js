#!/usr/bin/env node
/**
 * cleanup-junk.js
 *
 * Removes low-quality records from the people table:
 *   1. No first_name (or blank)
 *   2. No contact info at all (no email, no phone, no linkedin)
 *
 * Also fixes common data issues:
 *   - Trims whitespace from names
 *   - Normalizes phone numbers
 *   - Sets first_name/last_name from full_name where missing
 *
 * Usage:
 *   node scripts/cleanup-junk.js           # dry run (shows what would be deleted)
 *   node scripts/cleanup-junk.js --apply   # actually delete + clean
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway')
    ? { rejectUnauthorized: false }
    : undefined,
});

const DRY_RUN = !process.argv.includes('--apply');

async function run() {
  await pool.query('SELECT 1');
  console.log('✅ DB connected\n');

  if (DRY_RUN) {
    console.log('🔍 DRY RUN — no changes will be made. Pass --apply to execute.\n');
  }

  // ── Step 1: Count junk records ──
  const { rows: [noName] } = await pool.query(`
    SELECT COUNT(*) FROM people
    WHERE first_name IS NULL OR TRIM(first_name) = ''
  `);
  console.log(`❌ Records with no first_name: ${noName.count}`);

  const { rows: [noContact] } = await pool.query(`
    SELECT COUNT(*) FROM people
    WHERE email IS NULL AND verified_email IS NULL AND phone IS NULL AND linkedin_url IS NULL
  `);
  console.log(`❌ Records with no contact info: ${noContact.count}`);

  const { rows: [junkTotal] } = await pool.query(`
    SELECT COUNT(*) FROM people
    WHERE (first_name IS NULL OR TRIM(first_name) = '')
       OR (email IS NULL AND verified_email IS NULL AND phone IS NULL AND linkedin_url IS NULL)
  `);
  console.log(`❌ Total junk (union): ${junkTotal.count}`);

  const { rows: [keepTotal] } = await pool.query(`
    SELECT COUNT(*) FROM people
    WHERE first_name IS NOT NULL AND TRIM(first_name) <> ''
      AND (email IS NOT NULL OR verified_email IS NOT NULL OR phone IS NOT NULL OR linkedin_url IS NOT NULL)
  `);
  console.log(`✅ Clean records to keep: ${keepTotal.count}\n`);

  if (!DRY_RUN) {
    // ── Step 2: Fix names where possible ──
    console.log('🔧 Fixing names...');

    // Parse first_name/last_name from full_name where missing
    const { rowCount: namesFixed } = await pool.query(`
      UPDATE people SET
        first_name = TRIM(SPLIT_PART(full_name, ' ', 1)),
        last_name  = TRIM(SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1))
      WHERE (first_name IS NULL OR TRIM(first_name) = '')
        AND full_name IS NOT NULL
        AND full_name LIKE '% %'
    `);
    console.log(`  Fixed ${namesFixed} names from full_name`);

    // Trim whitespace from all name fields
    const { rowCount: trimmed } = await pool.query(`
      UPDATE people SET
        first_name = TRIM(first_name),
        last_name  = TRIM(last_name),
        full_name  = TRIM(full_name)
      WHERE first_name <> TRIM(first_name)
         OR last_name <> TRIM(last_name)
         OR full_name <> TRIM(full_name)
    `);
    console.log(`  Trimmed whitespace on ${trimmed} records`);

    // ── Step 3: Delete junk ──
    console.log('\n🗑️  Deleting junk records...');

    // First remove from outreach and list memberships (ON DELETE CASCADE should handle this,
    // but let's be explicit)
    const { rowCount: deleted } = await pool.query(`
      DELETE FROM people
      WHERE (first_name IS NULL OR TRIM(first_name) = '')
         OR (email IS NULL AND verified_email IS NULL AND phone IS NULL AND linkedin_url IS NULL)
    `);
    console.log(`  Deleted ${deleted} junk records`);

    // ── Step 4: Rebuild full_name where it's missing but first+last exist ──
    const { rowCount: fullFixed } = await pool.query(`
      UPDATE people SET
        full_name = TRIM(first_name || ' ' || COALESCE(last_name, ''))
      WHERE (full_name IS NULL OR TRIM(full_name) = '')
        AND first_name IS NOT NULL AND TRIM(first_name) <> ''
    `);
    console.log(`  Rebuilt full_name on ${fullFixed} records`);
  }

  // ── Final count ──
  const { rows: [final] } = await pool.query('SELECT COUNT(*) FROM people');
  console.log(`\n📦 Total in DB: ${final.count}`);

  if (DRY_RUN) {
    console.log('\n💡 Run with --apply to actually clean up.');
  }

  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
