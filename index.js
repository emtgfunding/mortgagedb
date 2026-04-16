/**
 * MortgageDB API Server
 * 
 * Search and filter the mortgage industry contact database.
 * Powers both the internal tool and future B2B API product.
 */

require('dotenv').config();
const path = require('path');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const pool = require('./db');
const migrate = require('./migrate');

const app = express();
const PORT = process.env.PORT || 3000;

// Relax helmet's default CSP so the inline dashboard at / can render without extra work.
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());

// Serve static assets (recruiter app lives under /public)
app.use(express.static(path.join(__dirname, 'public')));

// Recruiter app — /app (Express 5 uses named wildcards: /app/*splat)
app.get('/app', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'app.html'));
});
app.get('/app/*splat', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'app.html'));
});

// ─── Landing page / dashboard ─────────────────────────────────────────────────
app.get('/', async (req, res) => {
  let stats = null;
  try {
    const [{ rows: p }, { rows: c }, { rows: e }, { rows: l }, { rows: st }] = await Promise.all([
      pool.query('SELECT COUNT(*)::int AS n FROM people'),
      pool.query('SELECT COUNT(*)::int AS n FROM companies'),
      pool.query("SELECT COUNT(*)::int AS n FROM people WHERE email IS NOT NULL AND email <> ''"),
      pool.query("SELECT COUNT(*)::int AS n FROM people WHERE linkedin_url IS NOT NULL AND linkedin_url <> ''"),
      pool.query("SELECT state, COUNT(*)::int AS n FROM people WHERE state IS NOT NULL GROUP BY state ORDER BY n DESC LIMIT 10")
    ]);
    stats = {
      people: p[0].n,
      companies: c[0].n,
      with_email: e[0].n,
      with_linkedin: l[0].n,
      by_state: st
    };
  } catch (err) {
    console.error('dashboard stats failed:', err.message);
  }

  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  const stateRows = stats && stats.by_state.length
    ? stats.by_state.map(r => `<tr><td>${esc(r.state)}</td><td style="text-align:right">${r.n.toLocaleString()}</td></tr>`).join('')
    : '<tr><td colspan="2" style="color:#888">no data</td></tr>';

  res.type('html').send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MortgageDB</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body { font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 860px; margin: 2.5rem auto; padding: 0 1.25rem; }
  h1 { margin: 0 0 .25rem; font-size: 1.75rem; }
  .sub { color: #888; margin: 0 0 1.75rem; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: .75rem; margin-bottom: 2rem; }
  .card { border: 1px solid #8884; border-radius: 10px; padding: 1rem; }
  .card .n { font-size: 1.75rem; font-weight: 600; }
  .card .k { color: #888; font-size: .85rem; text-transform: uppercase; letter-spacing: .04em; }
  h2 { font-size: 1.1rem; margin: 1.75rem 0 .5rem; }
  table { width: 100%; border-collapse: collapse; }
  td { padding: .35rem .5rem; border-bottom: 1px solid #8882; }
  .endpoints a { display: inline-block; margin: .15rem .35rem .15rem 0; padding: .3rem .6rem; border: 1px solid #8884; border-radius: 6px; text-decoration: none; color: inherit; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .85rem; }
  .endpoints a:hover { background: #8881; }
  footer { margin-top: 2.5rem; color: #888; font-size: .85rem; }
</style>
</head>
<body>
  <h1>MortgageDB</h1>
  <p class="sub">NMLS-sourced mortgage industry contact database</p>

  <p style="margin:1rem 0 2rem">
    <a href="/app" style="display:inline-block; padding:.6rem 1rem; background:linear-gradient(135deg,#4f46e5,#0ea5e9); color:white; text-decoration:none; border-radius:8px; font-weight:600">
      Open Recruiter App →
    </a>
  </p>

  ${stats ? `
  <div class="cards">
    <div class="card"><div class="k">People</div><div class="n">${stats.people.toLocaleString()}</div></div>
    <div class="card"><div class="k">Companies</div><div class="n">${stats.companies.toLocaleString()}</div></div>
    <div class="card"><div class="k">With Email</div><div class="n">${stats.with_email.toLocaleString()}</div></div>
    <div class="card"><div class="k">With LinkedIn</div><div class="n">${stats.with_linkedin.toLocaleString()}</div></div>
  </div>

  <h2>Top states</h2>
  <table>${stateRows}</table>
  ` : `<p style="color:#c33">Stats unavailable — check /health for service status.</p>`}

  <h2>API endpoints</h2>
  <div class="endpoints">
    <a href="/health">GET /health</a>
    <a href="/api/stats">GET /api/stats</a>
    <a href="/api/people?limit=25">GET /api/people</a>
    <a href="/api/companies?limit=25">GET /api/companies</a>
    <a href="/api/jobs">GET /api/jobs</a>
    <a href="/api/export/csv">GET /api/export/csv</a>
  </div>

  <footer>Deploy: Railway · Source: NMLS Consumer Access · Last updated ${new Date().toISOString()}</footer>
</body>
</html>`);
});

// ─── Health ───────────────────────────────────────────────────────────────────
app.get('/health', async (req, res) => {
  const { rows } = await pool.query('SELECT COUNT(*) FROM people');
  res.json({ status: 'ok', total_people: parseInt(rows[0].count) });
});

// ─── Stats ────────────────────────────────────────────────────────────────────
// Quality gate: only count people with first_name + at least one contact method
const QUALITY_GATE = `first_name IS NOT NULL AND TRIM(first_name) <> '' AND (email IS NOT NULL OR verified_email IS NOT NULL OR phone IS NOT NULL OR linkedin_url IS NOT NULL)`;

app.get('/api/stats', async (req, res) => {
  try {
    const [people, companies, byState, byTier, withEmail, withLinkedIn] = await Promise.all([
      pool.query(`SELECT COUNT(*) FROM people WHERE ${QUALITY_GATE}`),
      pool.query('SELECT COUNT(*) FROM companies'),
      pool.query(`
        SELECT state, COUNT(*) as count
        FROM people WHERE state IS NOT NULL AND ${QUALITY_GATE}
        GROUP BY state ORDER BY count DESC LIMIT 20
      `),
      pool.query(`
        SELECT production_tier, COUNT(*) as count
        FROM people WHERE ${QUALITY_GATE} GROUP BY production_tier
      `),
      pool.query(`SELECT COUNT(*) FROM people WHERE ${QUALITY_GATE} AND (email IS NOT NULL OR verified_email IS NOT NULL)`),
      pool.query(`SELECT COUNT(*) FROM people WHERE ${QUALITY_GATE} AND linkedin_url IS NOT NULL`)
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
      list_id,              // filter to members of a saved list
      outreach_status,      // filter by outreach status (comma-separated ok)
      sort = 'quality',     // quality | name | volume | contacted
      page = 1,
      per_page = 25
    } = req.query;

    // ── Baseline quality gates (always enforced) ──
    // 1. Must have a first name
    // 2. Must have at least one contact channel (email, phone, or linkedin)
    const conditions = [
      `(p.first_name IS NOT NULL AND TRIM(p.first_name) <> '')`,
      `(p.email IS NOT NULL OR p.verified_email IS NOT NULL OR p.phone IS NOT NULL OR p.linkedin_url IS NOT NULL)`,
    ];
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

    if (list_id) {
      conditions.push(`p.id IN (SELECT person_id FROM saved_list_members WHERE list_id = $${p++})`);
      params.push(list_id);
    }

    if (outreach_status) {
      const statuses = String(outreach_status).split(',').map(s => s.trim()).filter(Boolean);
      if (statuses.length) {
        conditions.push(`COALESCE(o.status, 'new') = ANY($${p++})`);
        params.push(statuses);
      }
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const offset = (parseInt(page) - 1) * parseInt(per_page);
    const limit  = Math.min(parseInt(per_page), 100);

    const orderBy = {
      quality:   'p.data_quality_score DESC, p.full_name ASC',
      name:      'p.full_name ASC',
      volume:    'p.vol_12mo_usd DESC NULLS LAST, p.full_name ASC',
      contacted: 'o.last_contacted_at DESC NULLS LAST, p.full_name ASC'
    }[sort] || 'p.data_quality_score DESC, p.full_name ASC';

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
          COALESCE(o.status, 'new') as outreach_status,
          o.note                    as outreach_note,
          o.last_contacted_at,
          -- Aggregate licensed states
          ARRAY(
            SELECT l.state FROM licenses l
            WHERE l.person_id = p.id AND l.status ILIKE '%approv%'
            ORDER BY l.state
          ) as licensed_states
        FROM people p
        LEFT JOIN outreach o ON o.person_id = p.id
        ${where}
        ORDER BY ${orderBy}
        LIMIT ${limit} OFFSET ${offset}
      `, params),
      pool.query(`
        SELECT COUNT(*) FROM people p
        LEFT JOIN outreach o ON o.person_id = p.id
        ${where}
      `, params)
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
    // Mirror /api/people filter logic so the UI's "Export" matches the on-screen view.
    const {
      q, state, states, title_category, license_status,
      has_email, has_phone, has_linkedin, company, production_tier,
      list_id, outreach_status
    } = req.query;

    // Same baseline quality gates as /api/people
    const conditions = [
      `(p.first_name IS NOT NULL AND TRIM(p.first_name) <> '')`,
      `(p.email IS NOT NULL OR p.verified_email IS NOT NULL OR p.phone IS NOT NULL OR p.linkedin_url IS NOT NULL)`,
    ];
    const params = [];
    let p = 1;

    if (q) {
      conditions.push(`(p.full_name ILIKE $${p} OR p.company_name ILIKE $${p} OR p.nmls_id = $${p+1})`);
      params.push(`%${q}%`, q); p += 2;
    }
    if (state) { conditions.push(`p.state = $${p++}`); params.push(state.toUpperCase()); }
    if (states) {
      conditions.push(`p.state = ANY($${p++})`);
      params.push(states.split(',').map(s=>s.trim().toUpperCase()));
    }
    if (title_category)   { conditions.push(`p.title_category = $${p++}`); params.push(title_category); }
    if (license_status)   { conditions.push(`p.license_status ILIKE $${p++}`); params.push(`%${license_status}%`); }
    if (has_email === 'true')    conditions.push(`(p.email IS NOT NULL OR p.verified_email IS NOT NULL)`);
    if (has_phone === 'true')    conditions.push(`p.phone IS NOT NULL`);
    if (has_linkedin === 'true') conditions.push(`p.linkedin_url IS NOT NULL`);
    if (company)          { conditions.push(`p.company_name ILIKE $${p++}`); params.push(`%${company}%`); }
    if (production_tier)  { conditions.push(`p.production_tier = $${p++}`); params.push(production_tier); }
    if (list_id) {
      conditions.push(`p.id IN (SELECT person_id FROM saved_list_members WHERE list_id = $${p++})`);
      params.push(list_id);
    }
    if (outreach_status) {
      const statuses = String(outreach_status).split(',').map(s => s.trim()).filter(Boolean);
      if (statuses.length) {
        conditions.push(`COALESCE(o.status, 'new') = ANY($${p++})`);
        params.push(statuses);
      }
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const { rows } = await pool.query(`
      SELECT p.nmls_id, p.full_name, p.first_name, p.last_name, p.title, p.company_name,
             p.phone, COALESCE(p.verified_email, p.email) as email,
             p.city, p.state, p.zip, p.license_status, p.regulatory_actions,
             p.linkedin_url, p.production_tier, p.vol_12mo_usd, p.vol_12mo_units,
             p.data_quality_score,
             COALESCE(o.status, 'new') as outreach_status,
             o.last_contacted_at
      FROM people p
      LEFT JOIN outreach o ON o.person_id = p.id
      ${where}
      ORDER BY p.data_quality_score DESC
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


// ─── Saved Lists ──────────────────────────────────────────────────────────────
app.get('/api/lists', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT l.id, l.name, l.description, l.color, l.created_at, l.updated_at,
        (SELECT COUNT(*) FROM saved_list_members m WHERE m.list_id = l.id) AS member_count
      FROM saved_lists l
      ORDER BY l.updated_at DESC
    `);
    res.json({ results: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/lists', async (req, res) => {
  try {
    const { name, description, color } = req.body || {};
    if (!name || !String(name).trim()) return res.status(400).json({ error: 'name required' });
    const { rows } = await pool.query(`
      INSERT INTO saved_lists (name, description, color)
      VALUES ($1, $2, $3)
      RETURNING id, name, description, color, created_at, updated_at
    `, [String(name).trim().slice(0, 200), description || null, color || null]);
    res.json(rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/lists/:id', async (req, res) => {
  try {
    const { rows: list } = await pool.query('SELECT * FROM saved_lists WHERE id = $1', [req.params.id]);
    if (!list[0]) return res.status(404).json({ error: 'not found' });
    const { rows: members } = await pool.query(`
      SELECT p.id, p.full_name, p.company_name, p.state, p.phone,
             COALESCE(p.verified_email, p.email) as email,
             p.linkedin_url, p.production_tier,
             COALESCE(o.status, 'new') as outreach_status
      FROM saved_list_members m
      JOIN people p ON p.id = m.person_id
      LEFT JOIN outreach o ON o.person_id = p.id
      WHERE m.list_id = $1
      ORDER BY m.added_at DESC
    `, [req.params.id]);
    res.json({ ...list[0], members });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/lists/:id', async (req, res) => {
  try {
    const { name, description, color } = req.body || {};
    const { rows } = await pool.query(`
      UPDATE saved_lists SET
        name        = COALESCE($1, name),
        description = COALESCE($2, description),
        color       = COALESCE($3, color)
      WHERE id = $4
      RETURNING *
    `, [name || null, description || null, color || null, req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'not found' });
    res.json(rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/lists/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM saved_lists WHERE id = $1', [req.params.id]);
    res.json({ deleted: result.rowCount });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Bulk add members. Body: { person_ids: [uuid, ...] }
app.post('/api/lists/:id/members', async (req, res) => {
  try {
    const ids = Array.isArray(req.body?.person_ids) ? req.body.person_ids : [];
    if (!ids.length) return res.json({ added: 0 });
    const values = ids.map((_, i) => `($1, $${i + 2})`).join(',');
    const { rowCount } = await pool.query(
      `INSERT INTO saved_list_members (list_id, person_id) VALUES ${values}
       ON CONFLICT DO NOTHING`,
      [req.params.id, ...ids]
    );
    // Touch list updated_at
    await pool.query('UPDATE saved_lists SET updated_at = NOW() WHERE id = $1', [req.params.id]);
    res.json({ added: rowCount });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/lists/:id/members/:person_id', async (req, res) => {
  try {
    const result = await pool.query(
      'DELETE FROM saved_list_members WHERE list_id = $1 AND person_id = $2',
      [req.params.id, req.params.person_id]
    );
    await pool.query('UPDATE saved_lists SET updated_at = NOW() WHERE id = $1', [req.params.id]);
    res.json({ deleted: result.rowCount });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Outreach Status ──────────────────────────────────────────────────────────
const OUTREACH_STATUSES = new Set([
  'new','queued','contacted','replied','interviewing','hired','not_interested','do_not_contact'
]);

app.patch('/api/people/:id/outreach', async (req, res) => {
  try {
    const { status, note, mark_contacted } = req.body || {};
    if (status && !OUTREACH_STATUSES.has(status)) {
      return res.status(400).json({ error: `invalid status. allowed: ${[...OUTREACH_STATUSES].join(', ')}` });
    }
    const touchContacted = mark_contacted === true || status === 'contacted';
    const { rows } = await pool.query(`
      INSERT INTO outreach (person_id, status, note, last_contacted_at)
      VALUES ($1, COALESCE($2, 'new'), $3, CASE WHEN $4 THEN NOW() ELSE NULL END)
      ON CONFLICT (person_id) DO UPDATE SET
        status            = COALESCE(EXCLUDED.status, outreach.status),
        note              = COALESCE(EXCLUDED.note, outreach.note),
        last_contacted_at = CASE WHEN $4 THEN NOW() ELSE outreach.last_contacted_at END,
        updated_at        = NOW()
      RETURNING *
    `, [req.params.id, status || null, note ?? null, touchContacted]);
    res.json(rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Bulk outreach status update ─────────────────────────────────────────────
app.patch('/api/outreach/bulk', async (req, res) => {
  try {
    const { person_ids, status } = req.body;
    if (!Array.isArray(person_ids) || person_ids.length === 0)
      return res.status(400).json({ error: 'person_ids array required' });
    if (!OUTREACH_STATUSES.has(status))
      return res.status(400).json({ error: `Invalid status: ${status}` });

    // Limit batch to 500 at a time
    const batch = person_ids.slice(0, 500);
    const { rowCount } = await pool.query(`
      INSERT INTO outreach (person_id, status)
      SELECT unnest($1::uuid[]), $2
      ON CONFLICT (person_id) DO UPDATE SET
        status     = EXCLUDED.status,
        updated_at = NOW()
    `, [batch, status]);

    res.json({ updated: rowCount });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/outreach/summary', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT COALESCE(status, 'new') AS status, COUNT(*)::int AS n
      FROM outreach
      GROUP BY COALESCE(status, 'new')
      ORDER BY n DESC
    `);
    res.json({ by_status: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Ingest single person (from browser scraper) ──────────────────────────────
app.post('/api/people/ingest', async (req, res) => {
  try {
    const d = req.body;
    if (!d.full_name && !d.linkedin_url) {
      return res.status(400).json({ error: 'Need full_name or linkedin_url' });
    }

    const score = [d.nmls_id, d.email, d.phone, d.company_name, d.full_name, d.linkedin_url]
      .filter(Boolean).length * 15;

    // Check for existing by linkedin_url
    if (d.linkedin_url) {
      const existing = await pool.query(
        'SELECT id FROM people WHERE linkedin_url = $1', [d.linkedin_url]
      );
      if (existing.rows[0]) {
        await pool.query(`
          UPDATE people SET
            headline = COALESCE($1, headline),
            company_name = COALESCE($2, company_name),
            source_linkedin = true,
            updated_at = NOW()
          WHERE id = $3
        `, [d.headline || null, d.company_name || null, existing.rows[0].id]);
        return res.json({ action: 'updated', id: existing.rows[0].id });
      }
    }

    const { rows } = await pool.query(`
      INSERT INTO people (
        nmls_id, first_name, last_name, full_name,
        company_name, phone, email, city, state,
        title, title_category, license_status,
        linkedin_url, photo_url, headline,
        source_nmls, source_linkedin, source_web,
        data_quality_score
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
      ON CONFLICT (nmls_id) DO UPDATE SET
        linkedin_url = COALESCE(EXCLUDED.linkedin_url, people.linkedin_url),
        headline     = COALESCE(EXCLUDED.headline, people.headline),
        source_linkedin = true,
        updated_at   = NOW()
      RETURNING id
    `, [
      d.nmls_id || null,
      d.first_name || '', d.last_name || '', d.full_name || '',
      d.company_name || null, d.phone || null, d.email || null,
      d.city || null, d.state || null,
      d.title || 'Loan Officer', d.title_category || 'loan_officer',
      d.license_status || 'Active',
      d.linkedin_url || null, d.photo_url || null, d.headline || null,
      d.source_nmls || false, d.source_linkedin || false, d.source_web || false,
      Math.min(score, 100)
    ]);

    res.json({ action: 'inserted', id: rows[0]?.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Apply schema on boot (idempotent). Fire-and-forget so app.listen() always
// fires immediately — Railway's health check can't wait 30+ seconds for a
// potentially slow DDL, and a hung migration shouldn't take the API down.
// Set SKIP_MIGRATE=1 to disable entirely.
function start() {
  if (process.env.SKIP_MIGRATE !== '1') {
    const timeoutMs = parseInt(process.env.MIGRATE_TIMEOUT_MS) || 60000;
    const timeout = new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`migration timed out after ${timeoutMs}ms`)), timeoutMs)
    );
    Promise.race([migrate.run(), timeout])
      .then(() => { /* logged inside migrate */ })
      .catch((err) => {
        console.error('⚠️  Schema migration failed — API is still up:', err.message);
      });
  }

  // Bind to 0.0.0.0 explicitly so Railway's edge proxy can reach us on
  // container network interfaces, not just loopback. The diagnostic block
  // below surfaces everything we need to debug Railway routing from a log
  // snapshot — no Railway CLI / dashboard access required.
  const portSource = process.env.PORT
    ? `env PORT=${process.env.PORT}`
    : 'env PORT UNSET — Railway must target port ' + PORT;

  app.listen(PORT, '0.0.0.0', () => {
    console.log('\n════════════════════════════════════════════════════');
    console.log(`🚀 MortgageDB API listening on 0.0.0.0:${PORT}`);
    console.log(`   ${portSource}`);
    console.log('   --- Railway env snapshot ---');
    for (const k of Object.keys(process.env).filter(k => k.startsWith('RAILWAY_')).sort()) {
      // RAILWAY_* vars are public metadata (service ID, project ID, public
      // domain) — safe to log. We deliberately do NOT echo DATABASE_URL or
      // any secrets.
      console.log(`   ${k}=${process.env[k]}`);
    }
    console.log(`   NODE_ENV=${process.env.NODE_ENV || '(unset)'}`);
    console.log(`   HOSTNAME=${process.env.HOSTNAME || '(unset)'}`);
    console.log('════════════════════════════════════════════════════\n');
    console.log('   Endpoints: /health  /api/stats  /api/people\n');
  });
}

if (require.main === module) {
  start();
}

module.exports = app;