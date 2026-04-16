#!/usr/bin/env node
/**
 * populate-companies.js
 *
 * Extracts unique companies from the people table, counts LOs per company,
 * derives HQ state (mode of LO states), and upserts into the companies table.
 *
 * Safe to run multiple times — uses ON CONFLICT (name) DO UPDATE.
 *
 * Usage:
 *   node scripts/populate-companies.js
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway')
    ? { rejectUnauthorized: false }
    : undefined,
});

async function run() {
  await pool.query('SELECT 1');
  console.log('✅ DB connected\n');

  // Step 1: Get distinct companies with LO counts, HQ state, and NMLS IDs
  const { rows: companies } = await pool.query(`
    WITH company_stats AS (
      SELECT
        TRIM(company_name) AS name,
        COUNT(*)           AS lo_count,
        -- HQ state = the state with the most LOs for this company
        MODE() WITHIN GROUP (ORDER BY state) AS hq_state,
        -- Grab company NMLS ID if any LO has one
        MAX(company_nmls_id) FILTER (WHERE company_nmls_id IS NOT NULL AND company_nmls_id != '') AS nmls_id,
        -- Collect distinct states as array
        ARRAY_AGG(DISTINCT state) FILTER (WHERE state IS NOT NULL AND state != '') AS active_states
      FROM people
      WHERE company_name IS NOT NULL
        AND TRIM(company_name) != ''
        AND LENGTH(TRIM(company_name)) > 1
      GROUP BY TRIM(company_name)
    )
    SELECT * FROM company_stats
    ORDER BY lo_count DESC
  `);

  console.log(`Found ${companies.length} distinct companies in people table\n`);

  if (companies.length === 0) {
    console.log('Nothing to do.');
    process.exit(0);
  }

  // Show top 20
  console.log('Top 20 by LO count:');
  companies.slice(0, 20).forEach((c, i) =>
    console.log(`  ${(i + 1).toString().padStart(2)}. ${c.lo_count.toString().padStart(5)} LOs  ${c.name} (${c.hq_state || '?'})`)
  );

  // Step 2: Upsert into companies table
  console.log('\nUpserting into companies table...');
  let inserted = 0, updated = 0;

  for (const c of companies) {
    const { rowCount } = await pool.query(`
      INSERT INTO companies (name, nmls_id, state, lo_count, active_states, license_status)
      VALUES ($1, $2, $3, $4, $5, 'Active')
      ON CONFLICT (nmls_id) DO UPDATE SET
        name          = COALESCE(EXCLUDED.name, companies.name),
        lo_count      = EXCLUDED.lo_count,
        state         = COALESCE(EXCLUDED.state, companies.state),
        active_states = EXCLUDED.active_states,
        updated_at    = NOW()
    `, [
      c.name,
      c.nmls_id || null,
      c.hq_state || null,
      parseInt(c.lo_count),
      c.active_states || [],
    ]);

    // If no NMLS ID, the ON CONFLICT won't fire — try name-based upsert
    if (!c.nmls_id) {
      // Check if company exists by name
      const { rows } = await pool.query(
        `SELECT id FROM companies WHERE name = $1 LIMIT 1`, [c.name]
      );
      if (rows.length > 0) {
        await pool.query(`
          UPDATE companies SET
            lo_count      = $1,
            state         = COALESCE($2, state),
            active_states = $3,
            updated_at    = NOW()
          WHERE id = $4
        `, [parseInt(c.lo_count), c.hq_state, c.active_states || [], rows[0].id]);
        updated++;
      } else {
        await pool.query(`
          INSERT INTO companies (name, state, lo_count, active_states, license_status)
          VALUES ($1, $2, $3, $4, 'Active')
        `, [c.name, c.hq_state || null, parseInt(c.lo_count), c.active_states || []]);
        inserted++;
      }
    } else {
      inserted++;
    }

    if ((inserted + updated) % 200 === 0) process.stdout.write(`  [${inserted + updated}] `);
  }

  const { rows: [total] } = await pool.query('SELECT COUNT(*) FROM companies');
  console.log(`\n\n✅ Done. Companies in DB: ${total.count} (new: ${inserted}, updated: ${updated})`);
  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
