/**
 * MortgageDB API Server
 * 
 * Search and filter the mortgage industry contact database.
 * Powers both the internal tool and future B2B API product.
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const pool = require('./db');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(helmet());
app.use(cors());
app.use(express.json());

// ─── Health ───────────────────────────────────────────────────────────────────
app.get('/health', async (req, res) => {
  const { rows } = await pool.query('SELECT COUNT(*) FROM people');
  res.json({ status: 'ok', total_people: parseInt(rows[0].count) });
});

// ─── Stats ────────────────────────────────────────────────────────────────────
app.get('/api/stats', async (req, res) => {
  try {
    const [people, companies, byState, byTier, withEmail, withLinkedIn] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM people'),
      pool.query('SELECT COUNT(*) FROM companies'),
      pool.query(`
        SELECT state, COUNT(*) as count 
        FROM people WHERE state IS NOT NULL
        GROUP BY state ORDER BY count DESC LIMIT 20
      `),
      pool.query(`
        SELECT production_tier, COUNT(*) as count
        FROM people GROUP BY production_tier
      `),
      pool.query(`SELECT COUNT(*) FROM people WHERE email IS NOT NULL OR verified_email IS NOT NULL`),
      pool.query(`SELECT COUNT(*) FROM people WHERE linkedin_url IS NOT NULL`)
    ]);

    res.json({
      total_people:    parseInt(people.rows[0].count),
      total_companies: parseInt(companies.rows[0].count),
      with_email:      parseInt(withEmail.rows[0].count),
      with_linkedin:   parseInt(withLinkedIn.rows[0].count),
      by_state:        byState.rows,
      by_production:   byTier.rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── People Search ────────────────────────────────────────────────────────────
app.get('/api/people', async (req, res) => {
  try {
    const {
      q,                    // name or company search
      state,                // filter by state
      states,               // comma-separated states
      title_category,       // loan_officer, hr, it, trainer, operations, etc
      license_status,       // Active, Inactive
      has_email,            // true/false
      has_phone,            // true/false
      has_linkedin,         // true/false
      nmls_id,              // exact NMLS ID
      company,              // company name search
      production_tier,      // top, mid, active
      page = 1,
      per_page = 25
    } = req.query;

    const conditions = [];
    const params = [];
    let p = 1;

    if (q) {
      conditions.push(`(
        p.full_name ILIKE $${p} OR 
        p.company_name ILIKE $${p} OR
        p.nmls_id = $${p+1}
      )`);
      params.push(`%${q}%`, q);
      p += 2;
    }

    if (nmls_id) {
      conditions.push(`p.nmls_id = $${p++}`);
      params.push(nmls_id);
    }

    if (state) {
      conditions.push(`p.state = $${p++}`);
      params.push(state.toUpperCase());
    }

    if (states) {
      const stateList = states.split(',').map(s => s.trim().toUpperCase());
      conditions.push(`p.state = ANY($${p++})`);
      params.push(stateList);
    }

    if (title_category) {
      conditions.push(`p.title_category = $${p++}`);
      params.push(title_category);
    }

    if (license_status) {
      conditions.push(`p.license_status ILIKE $${p++}`);
      params.push(`%${license_status}%`);
    }

    if (has_email === 'true') {
      conditions.push(`(p.email IS NOT NULL OR p.verified_email IS NOT NULL)`);
    }

    if (has_phone === 'true') {
      conditions.push(`p.phone IS NOT NULL`);
    }

    if (has_linkedin === 'true') {
      conditions.push(`p.linkedin_url IS NOT NULL`);
    }

    if (company) {
      conditions.push(`p.company_name ILIKE $${p++}`);
      params.push(`%${company}%`);
    }

    if (production_tier) {
      conditions.push(`p.production_tier = $${p++}`);
      params.push(production_tier);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const offset = (parseInt(page) - 1) * parseInt(per_page);
    const limit  = Math.min(parseInt(per_page), 100);

    const [results, countResult] = await Promise.all([
      pool.query(`
        SELECT 
          p.id, p.nmls_id, p.first_name, p.last_name, p.full_name,
          p.title, p.title_category,
          p.company_name, p.company_nmls_id,
          p.phone, p.email, p.verified_email, p.email_verified,
          p.city, p.state, p.zip,
          p.license_status, p.regulatory_actions,
          p.linkedin_url, p.photo_url, p.headline,
          p.production_tier, p.vol_12mo_usd, p.vol_12mo_units,
          p.data_quality_score,
          p.source_nmls, p.source_linkedin,
          p.created_at,
          -- Aggregate licensed states
          ARRAY(
            SELECT l.state FROM licenses l 
            WHERE l.person_id = p.id AND l.status ILIKE '%approv%'
            ORDER BY l.state
          ) as licensed_states
        FROM people p
        ${where}
        ORDER BY p.data_quality_score DESC, p.full_name ASC
        LIMIT ${limit} OFFSET ${offset}
      `, params),
      pool.query(`SELECT COUNT(*) FROM people p ${where}`, params)
    ]);

    res.json({
      total:    parseInt(countResult.rows[0].count),
      page:     parseInt(page),
      per_page: limit,
      results:  results.rows
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Single person ────────────────────────────────────────────────────────────
app.get('/api/people/:id', async (req, res) => {
  try {
    const isNmls = /^\d+$/.test(req.params.id);
    const field  = isNmls ? 'nmls_id' : 'id';

    const { rows } = await pool.query(`
      SELECT p.*,
        ARRAY(
          SELECT row_to_json(l) FROM licenses l WHERE l.person_id = p.id
        ) as licenses,
        ARRAY(
          SELECT row_to_json(eh) FROM employment_history eh WHERE eh.person_id = p.id
          ORDER BY eh.start_date DESC
        ) as employment
      FROM people p
      WHERE p.${field} = $1
    `, [req.params.id]);

    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Companies ────────────────────────────────────────────────────────────────
app.get('/api/companies', async (req, res) => {
  try {
    const { q, state, page = 1, per_page = 25 } = req.query;
    const conditions = [];
    const params = [];
    let p = 1;

    if (q) { conditions.push(`c.name ILIKE $${p++}`); params.push(`%${q}%`); }
    if (state) { conditions.push(`c.state = $${p++}`); params.push(state.toUpperCase()); }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const offset = (parseInt(page) - 1) * parseInt(per_page);

    const { rows } = await pool.query(`
      SELECT c.*, 
        (SELECT COUNT(*) FROM people WHERE company_nmls_id = c.nmls_id) as lo_count
      FROM companies c ${where}
      ORDER BY lo_count DESC, c.name ASC
      LIMIT ${Math.min(parseInt(per_page), 100)} OFFSET ${offset}
    `, params);

    res.json({ results: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── CSV Export ───────────────────────────────────────────────────────────────
app.get('/api/export/csv', async (req, res) => {
  try {
    // Reuse same filter logic as /api/people but no pagination
    const { state, states, title_category, has_email, company } = req.query;
    const conditions = [];
    const params = [];
    let p = 1;

    if (state) { conditions.push(`state = $${p++}`); params.push(state.toUpperCase()); }
    if (states) { 
      conditions.push(`state = ANY($${p++})`); 
      params.push(states.split(',').map(s=>s.trim().toUpperCase())); 
    }
    if (title_category) { conditions.push(`title_category = $${p++}`); params.push(title_category); }
    if (has_email === 'true') { conditions.push(`(email IS NOT NULL OR verified_email IS NOT NULL)`); }
    if (company) { conditions.push(`company_name ILIKE $${p++}`); params.push(`%${company}%`); }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const { rows } = await pool.query(`
      SELECT nmls_id, full_name, first_name, last_name, title, company_name,
             phone, COALESCE(verified_email, email) as email,
             city, state, zip, license_status, regulatory_actions,
             linkedin_url, production_tier, vol_12mo_usd, vol_12mo_units,
             data_quality_score
      FROM people ${where}
      ORDER BY data_quality_score DESC
      LIMIT 10000
    `, params);

    const fields = Object.keys(rows[0] || {});
    const csv = [
      fields.join(','),
      ...rows.map(r => fields.map(f => {
        const v = r[f] == null ? '' : String(r[f]).replace(/"/g,'""');
        return `"${v}"`;
      }).join(','))
    ].join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="mortgagedb-export.csv"');
    res.send(csv);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Ingest job status ────────────────────────────────────────────────────────
app.get('/api/jobs', async (req, res) => {
  const { rows } = await pool.query(`
    SELECT * FROM ingest_jobs ORDER BY created_at DESC LIMIT 50
  `);
  res.json(rows);
});

app.listen(PORT, () => {
  console.log(`\n🚀 MortgageDB API running on port ${PORT}`);
  console.log(`   Health: http://localhost:${PORT}/health`);
  console.log(`   People: http://localhost:${PORT}/api/people`);
  console.log(`   Stats:  http://localhost:${PORT}/api/stats\n`);
});

module.exports = app;
