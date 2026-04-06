/**
 * NMLS Consumer Access Ingester
 * Pulls every licensed individual from NMLS Consumer Access by state.
 * Data is 100% public by federal law (SAFE Act of 2008).
 * 
 * Usage: node ingester/nmls.js MI OH IN FL TX
 *        node ingester/nmls.js   (runs all 50 states)
 */

require('dotenv').config();
const axios = require('axios');
const cheerio = require('cheerio');
const pool = require('../db');

const DELAY = parseInt(process.env.SCRAPE_DELAY_MS) || 2500;
const BASE = 'https://nmlsconsumeraccess.org';

const ALL_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','ID',
  'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO',
  'MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA',
  'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

const http = axios.create({
  baseURL: BASE,
  timeout: 30000,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': BASE
  }
});

// ─── Search NMLS by state ─────────────────────────────────────────────────────
// NMLS uses a WCF/ASMX web service endpoint
async function searchByState(state, page = 1, pageSize = 50) {
  // Try the JSON search endpoint first
  try {
    const res = await http.post('/Home.aspx/Search', JSON.stringify({
      SearchCriteria: JSON.stringify({
        SearchType: 'Individual',
        Name: '',
        State: state,
        City: '',
        Zip: '',
        LicenseNumber: '',
        Regulator: state
      }),
      PageNumber: page,
      PageSize: pageSize,
      SortColumn: 'Name',
      SortOrder: 'ASC'
    }), {
      headers: { 
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest'
      }
    });
    return res.data;
  } catch {
    // Fallback: GET with query params
    const res = await http.get('/Home.aspx/SubSearch', {
      params: {
        SearchType: 'I',
        State: state,
        PageNumber: page,
        PageSize: pageSize,
        SortColumn: 'Name',
        SortOrder: 'ASC'
      },
      headers: {
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
      }
    });
    return res.data;
  }
}

// ─── Get individual NMLS profile page ────────────────────────────────────────
async function getProfile(nmlsId) {
  try {
    const res = await http.get(`/EntityDetails.aspx/INDIVIDUAL/${nmlsId}`, {
      headers: { 'Accept': 'text/html' }
    });
    const $ = cheerio.load(res.data);
    const d = { nmls_id: String(nmlsId), licenses: [] };

    // ── Name ──
    d.full_name = (
      $('[id*="NameLabel"]').first().text() ||
      $('h1').first().text() ||
      $('[class*="entity-name"]').first().text()
    ).trim();

    if (d.full_name) {
      const parts = d.full_name.split(/\s+/);
      d.first_name = parts[0] || '';
      d.last_name  = parts[parts.length - 1] || '';
    }

    // ── Employer ──
    d.company_name   = $('[id*="EmployerName"], [id*="CompanyLabel"]').first().text().trim();
    d.company_nmls_id = $('[id*="EmployerNMLS"], [id*="CompanyNMLS"]').first().text()
                         .replace(/[^0-9]/g, '') || null;

    // ── Contact ──
    d.phone = $('[id*="Phone"]').first().text().trim();
    d.email = $('[id*="Email"], a[href^="mailto:"]').first().text().trim() ||
              $('a[href^="mailto:"]').first().attr('href')?.replace('mailto:', '') || null;

    // ── Address ──
    d.address = $('[id*="Address1"], [id*="StreetAddress"]').first().text().trim();
    d.city    = $('[id*="City"]').first().text().trim();
    d.state   = $('[id*="State"]').first().text().trim().toUpperCase();
    d.zip     = $('[id*="Zip"], [id*="PostalCode"]').first().text().trim();

    // ── License status ──
    d.license_status = $('[id*="Status"], [class*="license-status"]').first().text().trim() || 'Active';

    // ── Regulatory actions ──
    const regSection = $('[id*="Regulatory"], [id*="Actions"]').text();
    d.regulatory_actions = regSection.length > 10 && 
                            !/no regulatory|none/i.test(regSection);

    // ── State licenses table ──
    $('table tr').each((i, row) => {
      const cells = $(row).find('td');
      if (cells.length >= 3) {
        const stateCell = $(cells[0]).text().trim();
        if (stateCell.length === 2 && /^[A-Z]{2}$/.test(stateCell)) {
          d.licenses.push({
            state:         stateCell,
            regulator:     $(cells[1]).text().trim(),
            license_type:  $(cells[2]).text().trim(),
            license_number:$(cells[3])?.text().trim() || null,
            status:        $(cells[4])?.text().trim() || 'Active'
          });
        }
      }
    });

    return d;
  } catch (err) {
    return null;
  }
}

// ─── Upsert person ────────────────────────────────────────────────────────────
async function upsertPerson(d, homeState) {
  const score = [d.nmls_id,d.email,d.phone,d.company_name,d.full_name,d.city]
    .filter(Boolean).length * 15 + (d.licenses?.length > 0 ? 10 : 0);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows } = await client.query(`
      INSERT INTO people (
        nmls_id, first_name, last_name, full_name,
        company_name, company_nmls_id,
        phone, email, address, city, state, zip,
        license_status, title, title_category,
        regulatory_actions, source_nmls,
        data_quality_score, nmls_last_synced
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'Loan Officer','loan_officer',$14,true,$15,NOW())
      ON CONFLICT (nmls_id) DO UPDATE SET
        full_name          = COALESCE(EXCLUDED.full_name, people.full_name),
        first_name         = COALESCE(EXCLUDED.first_name, people.first_name),
        last_name          = COALESCE(EXCLUDED.last_name, people.last_name),
        company_name       = COALESCE(EXCLUDED.company_name, people.company_name),
        company_nmls_id    = COALESCE(EXCLUDED.company_nmls_id, people.company_nmls_id),
        phone              = COALESCE(EXCLUDED.phone, people.phone),
        email              = COALESCE(EXCLUDED.email, people.email),
        city               = COALESCE(EXCLUDED.city, people.city),
        state              = COALESCE(EXCLUDED.state, people.state),
        zip                = COALESCE(EXCLUDED.zip, people.zip),
        license_status     = EXCLUDED.license_status,
        regulatory_actions = EXCLUDED.regulatory_actions,
        source_nmls        = true,
        data_quality_score = GREATEST(people.data_quality_score, EXCLUDED.data_quality_score),
        nmls_last_synced   = NOW(),
        updated_at         = NOW()
      RETURNING id
    `, [
      d.nmls_id,
      d.first_name || '', d.last_name || '', d.full_name || '',
      d.company_name || null, d.company_nmls_id || null,
      d.phone || null, d.email || null, d.address || null,
      d.city || null, d.state || homeState, d.zip || null,
      d.license_status || 'Active',
      d.regulatory_actions || false,
      Math.min(score, 100)
    ]);

    const personId = rows[0].id;

    // Upsert licenses
    for (const lic of (d.licenses || [])) {
      if (!lic.state || lic.state.length !== 2) continue;
      await client.query(`
        INSERT INTO licenses (person_id, state, license_type, license_number, status, regulator)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (person_id, state, license_type) DO UPDATE SET
          status = EXCLUDED.status, regulator = EXCLUDED.regulator, updated_at = NOW()
      `, [personId, lic.state, lic.license_type || 'MLO', 
          lic.license_number || null, lic.status || 'Active', lic.regulator || null]);
    }

    await client.query('COMMIT');
    return personId;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ─── Ingest one state ─────────────────────────────────────────────────────────
async function ingestState(state) {
  console.log(`\n━━━ ${state} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

  const { rows: [job] } = await pool.query(`
    INSERT INTO ingest_jobs (job_type, target, status, started_at)
    VALUES ('nmls_individual', $1, 'running', NOW()) RETURNING id
  `, [state]);

  let added = 0, errors = 0, page = 1;
  const pageSize = 50;

  try {
    // Since NMLS doesn't expose a clean bulk API, we use a strategy:
    // Search with alphabetical name ranges to paginate through everyone in a state
    // A-Z first letter combinations give us all names without pagination limits
    const prefixes = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

    for (const prefix of prefixes) {
      let prefixPage = 1;
      let prefixTotal = Infinity;

      while (added < prefixTotal) {
        await sleep(DELAY);

        try {
          // Use NMLS individual search filtered by state + name prefix
          const res = await http.get('/Home.aspx/MainSearch', {
            params: {
              SearchValue: prefix,
              Regulator: state,
              Individual: 'true',
              Company: 'false',
              pageNumber: prefixPage,
              pageSize: pageSize
            },
            headers: {
              'Accept': 'application/json',
              'X-Requested-With': 'XMLHttpRequest'
            }
          });

          const data = res.data;
          let records = [];
          let total = 0;

          // Parse various response formats NMLS uses
          if (data?.d) {
            const parsed = typeof data.d === 'string' ? JSON.parse(data.d) : data.d;
            records = parsed?.SearchResults || parsed?.Results || parsed || [];
            total = parsed?.TotalRecords || records.length;
          } else if (Array.isArray(data)) {
            records = data;
            total = data.length;
          } else if (data?.SearchResults) {
            records = data.SearchResults;
            total = data.TotalRecords || records.length;
          }

          prefixTotal = total;

          if (records.length === 0) break;

          console.log(`  ${prefix}[${prefixPage}] → ${records.length} records`);

          for (const rec of records) {
            const nmlsId = rec.NMLSId || rec.NMLS_Id || rec.Id || rec.id;
            if (!nmlsId) continue;

            await sleep(DELAY);

            try {
              const profile = await getProfile(nmlsId);
              if (!profile) continue;

              // Fill in from search result if profile parse missed something
              if (!profile.full_name && rec.Name) profile.full_name = rec.Name;
              if (!profile.company_name && rec.CompanyName) profile.company_name = rec.CompanyName;

              await upsertPerson(profile, state);
              added++;
              process.stdout.write(added % 100 === 0 ? `\n  [${added}] ` : '·');
            } catch (err) {
              errors++;
              if (errors % 10 === 0) console.error(`\n  ${errors} errors so far`);
            }
          }

          const totalPages = Math.ceil(total / pageSize);
          if (prefixPage >= totalPages || records.length < pageSize) break;
          prefixPage++;

        } catch (err) {
          console.error(`\n  Search error ${prefix}[${prefixPage}]:`, err.message);
          break;
        }
      }

      await sleep(DELAY * 3);
    }

    await pool.query(`
      UPDATE ingest_jobs SET status='done', records_added=$1, completed_at=NOW() WHERE id=$2
    `, [added, job.id]);

    console.log(`\n  ✅ ${state}: ${added} added, ${errors} errors`);
    return { state, added, errors };

  } catch (err) {
    await pool.query(`
      UPDATE ingest_jobs SET status='failed', error_message=$1, completed_at=NOW() WHERE id=$2
    `, [err.message, job.id]);
    console.error(`\n  ❌ ${state} failed:`, err.message);
    return { state, added, errors, failed: true };
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  const states = process.argv.slice(2).length
    ? process.argv.slice(2).map(s => s.toUpperCase())
    : ALL_STATES;

  console.log(`\n🏦 MortgageDB — NMLS Ingester`);
  console.log(`📍 States: ${states.join(', ')}`);
  console.log(`⏱  Delay: ${DELAY}ms\n`);

  // Test DB connection
  await pool.query('SELECT 1');
  console.log('✅ DB connected\n');

  const results = [];
  for (const state of states) {
    const r = await ingestState(state);
    results.push(r);
    await sleep(DELAY * 10); // long pause between states
  }

  console.log('\n\n📊 Summary:');
  let total = 0;
  results.forEach(r => {
    console.log(`  ${r.state}: ${r.added} added${r.failed ? ' ❌ FAILED' : ''}`);
    total += r.added || 0;
  });

  const { rows } = await pool.query('SELECT COUNT(*) FROM people');
  console.log(`\n📦 Total in DB: ${rows[0].count}`);
  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
