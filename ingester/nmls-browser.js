#!/usr/bin/env node
/**
 * NMLS Consumer Access — Browser-based Ingester
 *
 * Uses puppeteer-core with your installed Chrome to bypass
 * Cloudflare/bot protection that blocks plain axios requests.
 *
 * Usage:
 *   npm install puppeteer-core
 *   node ingester/nmls-browser.js --fast TX FL CA
 *   node ingester/nmls-browser.js TX          (full profile fetch, slower)
 *
 * Finds Chrome automatically on Windows/Mac/Linux.
 */

require('dotenv').config();
const pool = require('../db');

const DELAY = parseInt(process.env.SCRAPE_DELAY_MS) || 2500;
const FAST_MODE = process.env.FAST_MODE === '1' || process.argv.includes('--fast');

const ALL_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','ID',
  'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO',
  'MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA',
  'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── Find Chrome on the system ───────────────────────────────────────────────
function findChrome() {
  const fs = require('fs');
  const paths = [
    // Windows
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    process.env.LOCALAPPDATA && `${process.env.LOCALAPPDATA}\\Google\\Chrome\\Application\\chrome.exe`,
    // macOS
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    // Linux
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium-browser',
    '/snap/bin/chromium',
  ].filter(Boolean);

  for (const p of paths) {
    if (fs.existsSync(p)) return p;
  }
  return null;
}

// ─── Upsert person (same logic as nmls.js) ───────────────────────────────────
async function upsertPerson(d, state) {
  const score = [d.nmls_id, d.email, d.phone, d.company_name, d.full_name, d.city]
    .filter(Boolean).length * 12;

  await pool.query(`
    INSERT INTO people (
      nmls_id, first_name, last_name, full_name,
      company_name, phone, email, city, state, zip,
      title, title_category, license_status,
      source_nmls, data_quality_score, nmls_last_synced
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'loan_officer',$12,
              true, $13, NOW())
    ON CONFLICT (nmls_id) DO UPDATE SET
      full_name    = COALESCE(EXCLUDED.full_name, people.full_name),
      first_name   = COALESCE(EXCLUDED.first_name, people.first_name),
      last_name    = COALESCE(EXCLUDED.last_name, people.last_name),
      company_name = COALESCE(EXCLUDED.company_name, people.company_name),
      phone        = COALESCE(EXCLUDED.phone, people.phone),
      email        = COALESCE(EXCLUDED.email, people.email),
      city         = COALESCE(EXCLUDED.city, people.city),
      state        = COALESCE(EXCLUDED.state, people.state),
      license_status = COALESCE(EXCLUDED.license_status, people.license_status),
      source_nmls  = true,
      data_quality_score = GREATEST(people.data_quality_score, EXCLUDED.data_quality_score),
      nmls_last_synced = NOW(),
      updated_at   = NOW()
  `, [
    d.nmls_id,
    d.first_name || '', d.last_name || '', d.full_name || '',
    d.company_name || null, d.phone || null, d.email || null,
    d.city || null, d.state || state, d.zip || null,
    d.title || 'Loan Officer',
    d.license_status || 'Active',
    Math.min(score, 100)
  ]);
}

// ─── Main browser scraper ────────────────────────────────────────────────────
async function run() {
  const states = process.argv.slice(2)
    .filter(a => !a.startsWith('--'))
    .map(s => s.toUpperCase());
  const target = states.length ? states : ALL_STATES;

  console.log(`\n🏦 MortgageDB — NMLS Browser Ingester`);
  console.log(`📍 States: ${target.join(', ')}`);
  console.log(`⏱  Delay: ${DELAY}ms`);
  console.log(`⚡ Fast mode: ${FAST_MODE ? 'ON' : 'off'}\n`);

  // Connect to DB
  await pool.query('SELECT 1');
  console.log('✅ DB connected');

  // Launch browser
  let puppeteer;
  try {
    puppeteer = require('puppeteer-core');
  } catch {
    console.error('❌ puppeteer-core not installed. Run: npm install puppeteer-core');
    process.exit(1);
  }

  const chromePath = findChrome();
  if (!chromePath) {
    console.error('❌ Could not find Chrome. Set CHROME_PATH env var.');
    process.exit(1);
  }
  console.log(`🌐 Using Chrome: ${chromePath}`);

  const browser = await puppeteer.launch({
    executablePath: process.env.CHROME_PATH || chromePath,
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36');
  await page.setViewport({ width: 1280, height: 800 });

  // Navigate to NMLS Consumer Access and wait for any challenge to clear
  console.log('  Navigating to NMLS Consumer Access...');
  await page.goto('https://nmlsconsumeraccess.org', { waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(3000);
  console.log('  ✅ Page loaded, session established\n');

  const results = [];

  for (const state of target) {
    console.log(`\n━━━ ${state} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

    const { rows: [job] } = await pool.query(`
      INSERT INTO ingest_jobs (job_type, target, status, started_at)
      VALUES ('nmls_browser', $1, 'running', NOW()) RETURNING id
    `, [state]);

    let added = 0, errors = 0;

    try {
      for (const prefix of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('')) {
        let pageNum = 1;
        while (true) {
          await sleep(DELAY);

          // Use page.evaluate to call the NMLS search API from within the browser context
          let result;
          try {
            result = await page.evaluate(async (searchValue, regulator, pNum) => {
              const url = `/Home.aspx/MainSearch?SearchValue=${encodeURIComponent(searchValue)}&Regulator=${encodeURIComponent(regulator)}&Individual=true&Company=false&pageNumber=${pNum}&pageSize=50`;
              const resp = await fetch(url, {
                headers: { 'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest' }
              });
              if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
              return await resp.json();
            }, prefix, state, pageNum);
          } catch (err) {
            console.log(`\n  Search error ${prefix}[${pageNum}]: ${err.message}`);
            break;
          }

          const records = result?.d?.results || result?.results || result?.d || [];
          const total = result?.d?.total || result?.total || 0;

          if (!Array.isArray(records) || !records.length) {
            if (pageNum === 1) process.stdout.write(`  ${prefix}:0 `);
            break;
          }

          console.log(`\n  ${prefix}[${pageNum}]: ${records.length} records (total: ${total})`);

          for (const rec of records) {
            const nmlsId = rec.NMLSId || rec.nmlsId || rec.Id || rec.id;
            if (!nmlsId) continue;

            try {
              if (FAST_MODE) {
                const name = rec.Name || rec.FullName || rec.name || '';
                const parts = name.split(/\s+/).filter(Boolean);
                await upsertPerson({
                  nmls_id:      String(nmlsId),
                  full_name:    name,
                  first_name:   parts[0] || '',
                  last_name:    parts.length > 1 ? parts[parts.length - 1] : '',
                  company_name: rec.Employer || rec.Company || rec.EmployerName || null,
                  city:         rec.City || null,
                  state:        rec.State || state,
                  license_status: rec.Status || 'Active',
                }, state);
              } else {
                // Full profile fetch via browser
                await sleep(DELAY);
                const profile = await page.evaluate(async (id) => {
                  const resp = await fetch(`/EntityDetails.aspx/Get/${id}`, {
                    headers: { 'Accept': 'application/json' }
                  });
                  if (!resp.ok) return null;
                  return await resp.json();
                }, nmlsId);

                if (!profile) { errors++; continue; }

                const p = profile.d || profile;
                const name = p.Name || p.FullName || rec.Name || '';
                const parts = name.split(/\s+/).filter(Boolean);

                await upsertPerson({
                  nmls_id:      String(nmlsId),
                  full_name:    name,
                  first_name:   parts[0] || '',
                  last_name:    parts.length > 1 ? parts[parts.length - 1] : '',
                  company_name: p.Employer || p.Company || null,
                  phone:        p.Phone || null,
                  email:        p.Email || null,
                  city:         p.City || null,
                  state:        p.State || state,
                  zip:          p.ZipCode || null,
                  license_status: p.Status || 'Active',
                  title:        p.Title || 'Loan Officer',
                }, state);
              }

              added++;
              process.stdout.write('·');
            } catch { errors++; }
          }

          if (records.length < 50 || pageNum >= Math.ceil(total / 50)) break;
          pageNum++;
          await sleep(DELAY * 2);
        }
        await sleep(DELAY);
      }

      await pool.query(`UPDATE ingest_jobs SET status='done', records_added=$1, completed_at=NOW() WHERE id=$2`, [added, job.id]);
      console.log(`\n  ✅ ${state}: ${added} added, ${errors} errors`);
      results.push({ state, added, errors });

    } catch (err) {
      await pool.query(`UPDATE ingest_jobs SET status='failed', error_message=$1, completed_at=NOW() WHERE id=$2`, [err.message, job.id]);
      console.error(`\n  ❌ ${state} failed:`, err.message);
      results.push({ state, added, errors, failed: true });
    }

    await sleep(DELAY * 5);
  }

  await browser.close();

  console.log('\n\n📊 Summary:');
  results.forEach(r => console.log(`  ${r.state}: ${r.added} added${r.failed ? ' ❌' : ''}`));
  const { rows } = await pool.query('SELECT COUNT(*) FROM people');
  console.log(`\n📦 Total in DB: ${rows[0].count}`);
  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
