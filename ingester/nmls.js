/**
 * NMLS Consumer Access Ingester v2
 * 
 * Properly handles:
 * - CSRF token extraction from the home page
 * - Cookie session management (keeps cookies across requests)
 * - Correct POST endpoint and headers that mirror a real browser
 * 
 * Usage: node ingester/nmls.js MI OH IN
 */

require('dotenv').config();
const axios = require('axios');
const { CookieJar } = require('tough-cookie');
const { wrapper } = require('axios-cookiejar-support');
const cheerio = require('cheerio');
const pool = require('../db');

const DELAY = parseInt(process.env.SCRAPE_DELAY_MS) || 3000;
const BASE = 'https://nmlsconsumeraccess.org';

// FAST_MODE=1 skips per-profile detail fetch. Uses only what search returns
// (NMLS ID + Name + sometimes company/city). ~30x faster — good for seeding
// a state, then running a second pass later to enrich profiles.
const FAST_MODE = process.env.FAST_MODE === '1' || process.argv.includes('--fast');

const ALL_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','ID',
  'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO',
  'MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA',
  'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

function createSession() {
  const jar = new CookieJar();
  const client = wrapper(axios.create({
    jar,
    baseURL: BASE,
    timeout: 30000,
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
      'Connection': 'keep-alive',
    }
  }));
  return { client, jar };
}

async function initSession(client) {
  console.log('  Initializing session with NMLS...');
  const res = await client.get('/', {
    headers: { 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' }
  });

  const $ = cheerio.load(res.data);
  const csrf = $('input[name="__RequestVerificationToken"]').val() || '';
  const viewState = $('input[name="__VIEWSTATE"]').val() || '';
  const viewStateGen = $('input[name="__VIEWSTATEGENERATOR"]').val() || '';
  const eventValidation = $('input[name="__EVENTVALIDATION"]').val() || '';

  console.log(`  Session: cookies=${res.headers['set-cookie']?.length||0} csrf=${csrf?'✓':'✗'} viewstate=${viewState?'✓':'✗'}`);
  await sleep(1500);
  return { csrf, viewState, viewStateGen, eventValidation };
}

async function searchNMLS(client, sessionData, state, namePrefix, pageNum = 1) {
  const { viewState, viewStateGen, eventValidation } = sessionData;

  try {
    const res = await client.get('/Home.aspx/MainSearch', {
      params: {
        SearchValue: namePrefix,
        Regulator: state,
        Individual: 'true',
        Company: 'false',
        pageNumber: pageNum,
        pageSize: 50
      },
      headers: {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': BASE + '/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
      }
    });

    const data = res.data;
    if (data?.d) {
      const parsed = typeof data.d === 'string' ? JSON.parse(data.d) : data.d;
      return {
        records: parsed?.SearchResults || parsed?.Results || (Array.isArray(parsed) ? parsed : []),
        total: parsed?.TotalRecords || 0
      };
    }
    if (Array.isArray(data)) return { records: data, total: data.length };
    if (data?.SearchResults) return { records: data.SearchResults, total: data.TotalRecords || 0 };
    return { records: [], total: 0 };

  } catch (err) {
    if (err.response?.status === 403) {
      // Fallback: POST form submission
      const formData = new URLSearchParams({
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': viewState,
        '__VIEWSTATEGENERATOR': viewStateGen,
        '__EVENTVALIDATION': eventValidation,
        'ctl00$main$SearchControl1$txtSearchValue': namePrefix,
        'ctl00$main$SearchControl1$ddlRegulator': state,
        'ctl00$main$SearchControl1$chkIndividual': 'on',
        'ctl00$main$SearchControl1$btnSearch': 'Search',
      });

      const res2 = await client.post('/', formData.toString(), {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Referer': BASE + '/',
          'Origin': BASE,
        }
      });

      const $ = cheerio.load(res2.data);
      const records = [];
      $('a[href*="EntityDetails"]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const match = href.match(/INDIVIDUAL\/(\d+)/i);
        if (match) {
          records.push({
            NMLSId: match[1],
            Name: $(el).text().trim()
          });
        }
      });
      return { records, total: records.length };
    }
    throw err;
  }
}

async function fetchProfile(client, nmlsId) {
  try {
    const res = await client.get(`/EntityDetails.aspx/INDIVIDUAL/${nmlsId}`, {
      headers: {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': BASE + '/'
      }
    });

    const $ = cheerio.load(res.data);
    const d = { nmls_id: String(nmlsId), licenses: [] };
    const t = (sel) => $(sel).first().text().trim();

    d.full_name = t('[id*="NameLabel"], h1');
    if (d.full_name) {
      const parts = d.full_name.split(/\s+/);
      d.first_name = parts[0] || '';
      d.last_name  = parts[parts.length - 1] || '';
    }
    d.company_name    = t('[id*="EmployerName"], [id*="Employer"]');
    d.company_nmls_id = t('[id*="EmployerNMLS"]').replace(/\D/g, '') || null;
    d.phone           = t('[id*="Phone"]');
    d.email           = $('a[href^="mailto:"]').first().attr('href')?.replace('mailto:','') || null;
    d.city            = t('[id*="City"]');
    d.state           = t('[id*="StateLabel"]').toUpperCase();
    d.zip             = t('[id*="Zip"]');
    d.license_status  = t('[id*="StatusLabel"]') || 'Active';

    $('table tr').each((i, row) => {
      if (i === 0) return;
      const cells = $(row).find('td');
      if (cells.length >= 3) {
        const st = $(cells[0]).text().trim();
        if (/^[A-Z]{2}$/.test(st)) {
          d.licenses.push({
            state:          st,
            regulator:      $(cells[1]).text().trim(),
            license_type:   $(cells[2]).text().trim(),
            license_number: $(cells[3])?.text().trim() || null,
            status:         $(cells[4])?.text().trim() || 'Active'
          });
        }
      }
    });

    return d;
  } catch { return null; }
}

async function upsertPerson(d, homeState) {
  const score = [d.nmls_id, d.email, d.phone, d.company_name, d.full_name, d.city].filter(Boolean).length * 15;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(`
      INSERT INTO people (
        nmls_id, first_name, last_name, full_name,
        company_name, company_nmls_id, phone, email,
        city, state, zip, license_status,
        title, title_category, regulatory_actions,
        source_nmls, data_quality_score, nmls_last_synced
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                'Loan Officer','loan_officer',$13,true,$14,NOW())
      ON CONFLICT (nmls_id) DO UPDATE SET
        full_name        = COALESCE(EXCLUDED.full_name, people.full_name),
        first_name       = COALESCE(EXCLUDED.first_name, people.first_name),
        last_name        = COALESCE(EXCLUDED.last_name, people.last_name),
        company_name     = COALESCE(EXCLUDED.company_name, people.company_name),
        phone            = COALESCE(EXCLUDED.phone, people.phone),
        email            = COALESCE(EXCLUDED.email, people.email),
        city             = COALESCE(EXCLUDED.city, people.city),
        state            = COALESCE(EXCLUDED.state, people.state),
        license_status   = EXCLUDED.license_status,
        source_nmls      = true,
        data_quality_score = GREATEST(people.data_quality_score, EXCLUDED.data_quality_score),
        nmls_last_synced = NOW(), updated_at = NOW()
      RETURNING id
    `, [
      d.nmls_id, d.first_name||'', d.last_name||'', d.full_name||'',
      d.company_name||null, d.company_nmls_id||null,
      d.phone||null, d.email||null,
      d.city||null, d.state||homeState, d.zip||null,
      d.license_status||'Active', d.regulatory_actions||false,
      Math.min(score, 100)
    ]);

    const personId = rows[0].id;
    for (const lic of (d.licenses||[])) {
      if (!lic.state || lic.state.length !== 2) continue;
      await client.query(`
        INSERT INTO licenses (person_id, state, license_type, license_number, status, regulator)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (person_id, state, license_type) DO UPDATE SET status=EXCLUDED.status, updated_at=NOW()
      `, [personId, lic.state, lic.license_type||'MLO', lic.license_number||null, lic.status||'Active', lic.regulator||null]);
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

async function ingestState(state) {
  console.log(`\n━━━ ${state} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

  const { rows: [job] } = await pool.query(`
    INSERT INTO ingest_jobs (job_type, target, status, started_at)
    VALUES ('nmls_individual', $1, 'running', NOW()) RETURNING id
  `, [state]);

  const { client } = createSession();
  let added = 0, errors = 0;

  try {
    const sessionData = await initSession(client);
    await sleep(DELAY);

    for (const prefix of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')) {
      let page = 1;
      while (true) {
        await sleep(DELAY);
        let result;
        try {
          result = await searchNMLS(client, sessionData, state, prefix, page);
        } catch (err) {
          console.log(`\n  Search error ${prefix}[${page}]: ${err.message}`);
          break;
        }

        const { records, total } = result;
        if (!records.length) { if (page === 1) process.stdout.write(`  ${prefix}:0 `); break; }

        console.log(`\n  ${prefix}[${page}]: ${records.length} records`);

        for (const rec of records) {
          const nmlsId = rec.NMLSId || rec.nmlsId || rec.id;
          if (!nmlsId) continue;

          if (FAST_MODE) {
            // Skip profile fetch — just upsert search-result-level data
            try {
              const name = rec.Name || rec.FullName || rec.name || '';
              const parts = name.split(/\s+/).filter(Boolean);
              const profile = {
                nmls_id:      String(nmlsId),
                full_name:    name,
                first_name:   parts[0] || '',
                last_name:    parts.length > 1 ? parts[parts.length - 1] : '',
                company_name: rec.Employer || rec.Company || rec.EmployerName || null,
                city:         rec.City || null,
                state:        rec.State || state,
                license_status: rec.Status || 'Active',
                licenses:     [],
              };
              if (!profile.full_name) continue;
              await upsertPerson(profile, state);
              added++;
              process.stdout.write('·');
            } catch { errors++; }
          } else {
            await sleep(DELAY);
            try {
              const profile = await fetchProfile(client, nmlsId);
              if (!profile) continue;
              if (!profile.full_name && rec.Name) {
                profile.full_name = rec.Name;
                const parts = rec.Name.split(/\s+/);
                profile.first_name = parts[0]||'';
                profile.last_name  = parts[parts.length-1]||'';
              }
              await upsertPerson(profile, state);
              added++;
              process.stdout.write('·');
            } catch { errors++; }
          }
        }

        if (records.length < 50 || page >= Math.ceil(total/50)) break;
        page++;
        await sleep(DELAY * 2);
      }
      await sleep(DELAY);
    }

    await pool.query(`UPDATE ingest_jobs SET status='done', records_added=$1, completed_at=NOW() WHERE id=$2`, [added, job.id]);
    console.log(`\n  ✅ ${state}: ${added} added, ${errors} errors`);
    return { state, added, errors };

  } catch (err) {
    await pool.query(`UPDATE ingest_jobs SET status='failed', error_message=$1, completed_at=NOW() WHERE id=$2`, [err.message, job.id]);
    console.error(`\n  ❌ ${state} failed:`, err.message);
    return { state, added, errors, failed: true };
  }
}

async function run() {
  const states = process.argv.slice(2).filter(a => !a.startsWith('--')).length
    ? process.argv.slice(2).filter(a => !a.startsWith('--')).map(s => s.toUpperCase())
    : ALL_STATES;

  console.log(`\n🏦 MortgageDB — NMLS Ingester v2`);
  console.log(`📍 States: ${states.join(', ')}`);
  console.log(`⏱  Delay: ${DELAY}ms`);
  console.log(`⚡ Fast mode: ${FAST_MODE ? 'ON (search-only, no profile fetch)' : 'off'}\n`);

  await pool.query('SELECT 1');
  console.log('✅ DB connected\n');

  const results = [];
  for (const state of states) {
    results.push(await ingestState(state));
    await sleep(DELAY * 8);
  }

  console.log('\n\n📊 Summary:');
  results.forEach(r => console.log(`  ${r.state}: ${r.added} added${r.failed?' ❌':''}`));
  const { rows } = await pool.query('SELECT COUNT(*) FROM people');
  console.log(`\n📦 Total in DB: ${rows[0].count}`);
  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });