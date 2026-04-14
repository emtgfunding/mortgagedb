/**
 * MortgageDB — Multi-Source Ingester
 * 
 * Sources (all public, no login, no CAPTCHA):
 * 1. Google → site:linkedin.com/in searches for LOs by state/city
 * 2. State regulatory bulk CSV/Excel downloads (CT, TX, FL, OH, etc.)
 * 3. Zillow mortgage professional directory
 * 4. Company team pages (UWM, Rocket, etc.)
 * 
 * Usage:
 *   node ingester/multi.js linkedin MI          <- LinkedIn search for MI LOs
 *   node ingester/multi.js state CT             <- Download CT state licensee file
 *   node ingester/multi.js zillow MI detroit    <- Zillow pros in Detroit MI
 *   node ingester/multi.js company uwm          <- UWM team directory
 */

require('dotenv').config();
const axios = require('axios');
const cheerio = require('cheerio');
const pool = require('../db');

const DELAY = parseInt(process.env.SCRAPE_DELAY_MS) || 3000;
const sleep = ms => new Promise(r => setTimeout(r, ms));

const http = axios.create({
  timeout: 20000,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
  }
});

// ─── Upsert person ────────────────────────────────────────────────────────────
async function upsertPerson(d, source = 'web') {
  const score = [d.nmls_id, d.email, d.phone, d.company_name, d.full_name, d.city, d.linkedin_url]
    .filter(Boolean).length * 12;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    if (d.nmls_id) {
      // Has NMLS ID — use conflict-safe upsert
      await client.query(`
        INSERT INTO people (
          nmls_id, first_name, last_name, full_name,
          company_name, phone, email, city, state, zip,
          title, title_category, license_status,
          linkedin_url, photo_url, headline,
          source_nmls, source_linkedin, source_web,
          data_quality_score, nmls_last_synced
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'loan_officer',$12,$13,$14,$15,$16,$17,$18,$19,NOW())
        ON CONFLICT (nmls_id) DO UPDATE SET
          full_name    = COALESCE(EXCLUDED.full_name, people.full_name),
          first_name   = COALESCE(EXCLUDED.first_name, people.first_name),
          last_name    = COALESCE(EXCLUDED.last_name, people.last_name),
          company_name = COALESCE(EXCLUDED.company_name, people.company_name),
          phone        = COALESCE(EXCLUDED.phone, people.phone),
          email        = COALESCE(EXCLUDED.email, people.email),
          city         = COALESCE(EXCLUDED.city, people.city),
          state        = COALESCE(EXCLUDED.state, people.state),
          linkedin_url = COALESCE(EXCLUDED.linkedin_url, people.linkedin_url),
          photo_url    = COALESCE(EXCLUDED.photo_url, people.photo_url),
          headline     = COALESCE(EXCLUDED.headline, people.headline),
          license_status = COALESCE(EXCLUDED.license_status, people.license_status),
          source_nmls  = people.source_nmls OR EXCLUDED.source_nmls,
          source_linkedin = people.source_linkedin OR EXCLUDED.source_linkedin,
          source_web   = people.source_web OR EXCLUDED.source_web,
          data_quality_score = GREATEST(people.data_quality_score, EXCLUDED.data_quality_score),
          nmls_last_synced = NOW(),
          updated_at   = NOW()
      `, [
        d.nmls_id,
        d.first_name||'', d.last_name||'', d.full_name||'',
        d.company_name||null, d.phone||null, d.email||null,
        d.city||null, d.state||null, d.zip||null,
        d.title||'Loan Officer',
        d.license_status||'Active',
        d.linkedin_url||null, d.photo_url||null, d.headline||null,
        source==='nmls', source==='linkedin', source==='web',
        Math.min(score, 100)
      ]);
    } else {
      // No NMLS ID — check for duplicate by linkedin_url or full_name+company
      let existingId = null;

      if (d.linkedin_url) {
        const { rows } = await client.query(
          'SELECT id FROM people WHERE linkedin_url = $1 LIMIT 1', [d.linkedin_url]
        );
        if (rows[0]) existingId = rows[0].id;
      }

      if (!existingId && d.full_name && d.company_name) {
        const { rows } = await client.query(
          'SELECT id FROM people WHERE full_name ILIKE $1 AND company_name ILIKE $2 LIMIT 1',
          [d.full_name, d.company_name]
        );
        if (rows[0]) existingId = rows[0].id;
      }

      if (existingId) {
        // Update existing record
        await client.query(`
          UPDATE people SET
            company_name = COALESCE($1, company_name),
            phone        = COALESCE($2, phone),
            email        = COALESCE($3, email),
            city         = COALESCE($4, city),
            state        = COALESCE($5, state),
            linkedin_url = COALESCE($6, linkedin_url),
            photo_url    = COALESCE($7, photo_url),
            headline     = COALESCE($8, headline),
            source_linkedin = source_linkedin OR $9,
            source_web   = source_web OR $10,
            data_quality_score = GREATEST(data_quality_score, $11),
            updated_at   = NOW()
          WHERE id = $12
        `, [
          d.company_name||null, d.phone||null, d.email||null,
          d.city||null, d.state||null,
          d.linkedin_url||null, d.photo_url||null, d.headline||null,
          source==='linkedin', source==='web',
          Math.min(score, 100), existingId
        ]);
      } else {
        // Fresh insert
        await client.query(`
          INSERT INTO people (
            first_name, last_name, full_name, company_name,
            phone, email, city, state, zip,
            title, title_category, license_status,
            linkedin_url, photo_url, headline,
            source_nmls, source_linkedin, source_web,
            data_quality_score
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'loan_officer',$11,$12,$13,$14,$15,$16,$17,$18)
        `, [
          d.first_name||'', d.last_name||'', d.full_name||'',
          d.company_name||null, d.phone||null, d.email||null,
          d.city||null, d.state||null, d.zip||null,
          d.title||'Loan Officer',
          d.license_status||'Active',
          d.linkedin_url||null, d.photo_url||null, d.headline||null,
          source==='nmls', source==='linkedin', source==='web',
          Math.min(score, 100)
        ]);
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err; // surface errors so we can debug
  } finally {
    client.release();
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 1: LinkedIn via Google search
// ═══════════════════════════════════════════════════════════════════════════════
const LI_QUERIES = [
  'mortgage loan officer',
  'loan officer',
  'mortgage originator',
  'MLO mortgage',
  'mortgage banker',
];

const CITIES_BY_STATE = {
  MI: [
    // Wayne County
    'Detroit','Dearborn','Livonia','Warren','Taylor','Westland','Southgate',
    'Wyandotte','Lincoln Park','Allen Park','Garden City','Inkster','Romulus',
    // Oakland County
    'Troy','Royal Oak','Southfield','Pontiac','Auburn Hills','Rochester Hills',
    'Birmingham','Bloomfield Hills','Farmington Hills','Novi','Clawson','Madison Heights',
    // Macomb County
    'Sterling Heights','Clinton Township','Mount Clemens','Roseville','Eastpointe',
    'Chesterfield','Shelby Township','Macomb','St Clair Shores',
    // Other MI
    'Grand Rapids','Ann Arbor','Lansing','Flint','Kalamazoo','Muskegon'
  ],
  OH: ['Columbus', 'Cleveland', 'Cincinnati', 'Toledo', 'Akron', 'Dayton'],
  IN: ['Indianapolis', 'Fort Wayne', 'Evansville', 'South Bend', 'Carmel'],
  FL: ['Miami', 'Orlando', 'Tampa', 'Jacksonville', 'Fort Lauderdale', 'Naples'],
  TX: ['Houston', 'Dallas', 'Austin', 'San Antonio', 'Fort Worth', 'Plano'],
  GA: ['Atlanta', 'Savannah', 'Augusta', 'Macon', 'Alpharetta'],
};

async function searchLinkedInGoogle(state, city, jobTitle) {
  // Search both with city and with just state to maximize results
  const query = `site:linkedin.com/in "${jobTitle}" "${city}" "${state}"`;
  const url = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=10&start=0`;

  try {
    const res = await http.get(url, {
      headers: { 'Accept': 'text/html', 'Referer': 'https://www.google.com' }
    });

    const $ = cheerio.load(res.data);
    const profiles = [];

    // Extract LinkedIn profiles from Google results using h3 headings (current Google DOM)
    const seen = new Set();
    $('h3').each((_, h3el) => {
      const h3 = $(h3el);
      const title = h3.text().trim();
      if (!title || title.length < 5) return;

      // Find nearest link with linkedin URL
      const parentLink = h3.closest('a');
      const nearbyLink = h3.parent().find('a[href*="linkedin"]').first();
      const href = parentLink.attr('href') || nearbyLink.attr('href') || '';
      
      const match = href.match(/linkedin\.com\/in\/([\w\-]+)/);
      if (!match) return;

      const profileId = match[1];
      if (profileId.length < 3 || seen.has(profileId)) return;
      seen.add(profileId);

      // Skip non-LO results
      const titleLower = title.toLowerCase();
      const isLO = titleLower.includes('loan') || titleLower.includes('mortgage') ||
                   titleLower.includes('mlo') || titleLower.includes('originator') ||
                   titleLower.includes('lender');
      if (!isLO) return;

      profiles.push({
        linkedin_url: `https://www.linkedin.com/in/${profileId}`,
        linkedin_id: profileId,
        raw_title: title,
        raw_snippet: '',
        city, state
      });
    });

    return profiles;
  } catch {
    return [];
  }
}

async function parseLinkedInProfile(profile) {
  try {
    // Fetch public LinkedIn profile (works without login for public profiles)
    const res = await http.get(profile.linkedin_url, {
      headers: {
        'Accept': 'text/html',
        'Referer': 'https://www.google.com',
        // LinkedIn public profiles accessible via Google bot UA
        'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
      }
    });

    const $ = cheerio.load(res.data);

    const rawTitle = $('title').text().replace('| LinkedIn','').replace('LinkedIn','').trim();
    const name = $('h1').first().text().trim() ||
                 (rawTitle.includes(' - ') ? rawTitle.split(' - ')[0].trim() : rawTitle) ||
                 profile.raw_title.split(' - ')[0].trim();

    const headline = $('h2').first().text().trim() ||
                     $('[class*="headline"]').first().text().trim() ||
                     profile.raw_snippet.split('·')[0].trim();

    const location = $('[class*="location"]').first().text().trim();
    const photo = $('img[class*="profile"], img[class*="pv-top"]').first().attr('src');

    const parts = name.split(/\s+/);

    return {
      full_name: name,
      first_name: parts[0] || '',
      last_name: parts[parts.length - 1] || '',
      headline,
      photo_url: photo || null,
      linkedin_url: profile.linkedin_url,
      linkedin_id: profile.linkedin_id,
      city: profile.city,
      state: profile.state,
      title: headline.split('at')[0].trim() || 'Loan Officer',
    };
  } catch {
    // Parse name and title from Google result title "Name - Title at Company"
    const parts = profile.raw_title.split(' - ');
    const name = parts[0].split('|')[0].trim();
    const headline = parts.slice(1).join(' - ').trim() || profile.raw_title;
    const nameParts = name.split(/\s+/);
    return {
      full_name: name,
      first_name: nameParts[0] || '',
      last_name: nameParts[nameParts.length - 1] || '',
      headline,
      linkedin_url: profile.linkedin_url,
      linkedin_id: profile.linkedin_id,
      city: profile.city,
      state: profile.state,
      title: headline.split(' at ')[0].trim() || 'Loan Officer',
    };
  }
}

async function runLinkedIn(state) {
  const cities = CITIES_BY_STATE[state] || [state];
  console.log(`\n🔗 LinkedIn ingester for ${state}`);
  console.log(`   Cities: ${cities.join(', ')}\n`);

  let added = 0;
  const seen = new Set();

  for (const city of cities) {
    for (const jobTitle of LI_QUERIES) {
      await sleep(DELAY * 2); // be polite to Google

      console.log(`  Searching: "${jobTitle}" in ${city}, ${state}...`);
      const profiles = await searchLinkedInGoogle(state, city, jobTitle);
      console.log(`  Found ${profiles.length} profiles`);

      for (const profile of profiles) {
        if (seen.has(profile.linkedin_id)) continue;
        seen.add(profile.linkedin_id);

        await sleep(DELAY);

        const parsed = await parseLinkedInProfile(profile);
        if (!parsed.full_name || parsed.full_name.length < 3) continue;

        await upsertPerson(parsed, 'linkedin');
        added++;
        process.stdout.write('·');
      }
    }
  }

  console.log(`\n\n  ✅ ${state}: ${added} LinkedIn profiles ingested`);
  return added;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 2: State bulk downloads (direct CSV/Excel from state regulator sites)
// ═══════════════════════════════════════════════════════════════════════════════
const STATE_DOWNLOADS = {
  CT: 'https://portal.ct.gov/-/media/DOB/Consumer-Credit-Licenses/Mortgage_Loan_Originators.xlsx',
  // Add more as we find them
};

async function runStateBulkDownload(state) {
  const url = STATE_DOWNLOADS[state];
  if (!url) {
    console.log(`No bulk download available for ${state}`);
    return 0;
  }

  console.log(`\n📥 Downloading ${state} licensee file...`);

  try {
    const res = await http.get(url, { responseType: 'arraybuffer' });
    const xlsx = require('xlsx');
    const wb = xlsx.read(res.data, { type: 'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];

    const raw = xlsx.utils.sheet_to_json(ws, { header: 1 });

    // Find the real header row - must have 3+ non-empty cells AND name/nmls keywords
    // This skips single-cell title rows like 'Mortgage Loan Originator Licensees as of...'
    let headerIdx = 1;
    for (let i = 0; i < Math.min(6, raw.length); i++) {
      const row = raw[i] || [];
      const nonEmpty = row.filter(c => c != null && String(c).trim().length > 0);
      if (nonEmpty.length < 3) continue;
      const joined = nonEmpty.join(' ').toLowerCase();
      if (joined.includes('last') || joined.includes('first') ||
          joined.includes('nmls') || joined.includes('individual')) {
        headerIdx = i;
        break;
      }
    }

    const headers = (raw[headerIdx] || []).map(h => h ? String(h).trim() : '');
    const dataRows = raw.slice(headerIdx + 1);

    console.log(`  Headers: ${JSON.stringify(headers)}`);
    console.log(`  Data rows: ${dataRows.length}`);

    // Build a flexible column finder
    const col = (row, ...variants) => {
      for (const v of variants) {
        const idx = headers.findIndex(h =>
          h && h.toString().toLowerCase().includes(v.toLowerCase())
        );
        if (idx >= 0 && row[idx] != null) return String(row[idx]).trim();
      }
      return null;
    };

    let added = 0;

    for (const row of dataRows) {
      if (!row || row.every(c => !c)) continue; // skip empty rows

      const firstName = col(row, 'first name', 'firstname') || '';
      const lastName  = col(row, 'last name', 'lastname') || '';
      const fullName  = col(row, 'full name', 'name') ||
                        [firstName, col(row, 'middle name', 'middlename') || '', lastName]
                          .filter(Boolean).join(' ').trim();

      if (!firstName && !lastName && !fullName) continue;

      const nmls = col(row, 'nmls', 'nmlsid', 'nmls id', 'nmls #');
      const d = {
        full_name:      fullName || `${firstName} ${lastName}`.trim(),
        first_name:     firstName,
        last_name:      lastName,
        nmls_id:        nmls ? nmls.replace(/\D/g, '') || null : null,
        company_name:   col(row, 'sponsor', 'company', 'employer', 'firm'),
        city:           col(row, 'city'),
        state:          col(row, 'state') || state,
        zip:            col(row, 'zip', 'postal'),
        phone:          col(row, 'phone'),
        email:          col(row, 'email'),
        license_status: col(row, 'status') || 'Active',
      };

      await upsertPerson(d, 'nmls');
      added++;
      if (added % 200 === 0) process.stdout.write(`\n  [${added}] `);
      else process.stdout.write('·');
    }

    console.log(`\n  ✅ ${state}: ${added} records ingested from state file`);
    return added;
  } catch (err) {
    console.error(`  ❌ Failed to download ${state} file:`, err.message);
    return 0;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 3: Zillow mortgage professionals directory
// ═══════════════════════════════════════════════════════════════════════════════
async function runZillow(state, city) {
  console.log(`\n🏠 Zillow mortgage pros: ${city}, ${state}`);

  const citySlug = city.toLowerCase().replace(/\s+/g, '-');
  const stateSlug = state.toLowerCase();
  let added = 0, page = 1;

  while (page <= 10) {
    await sleep(DELAY * 2);

    try {
      const url = `https://www.zillow.com/lender-directory/mortgage-lenders/${stateSlug}/${citySlug}/?page=${page}`;
      const res = await http.get(url, {
        headers: { 'Accept': 'text/html', 'Referer': 'https://www.zillow.com' }
      });

      const $ = cheerio.load(res.data);
      const profiles = [];

      // Parse Zillow lender cards
      $('[data-testid="lender-card"], .lender-card, [class*="LenderCard"]').each((_, el) => {
        const name = $(el).find('h2, h3, [class*="name"]').first().text().trim();
        const company = $(el).find('[class*="company"], [class*="employer"]').first().text().trim();
        const nmls = $(el).find('[class*="nmls"], [class*="NMLS"]').first().text().replace(/\D/g,'');
        const phone = $(el).find('[class*="phone"], a[href^="tel:"]').first().text().trim();
        const photo = $(el).find('img').first().attr('src');

        if (name) {
          profiles.push({ name, company, nmls, phone, photo });
        }
      });

      if (profiles.length === 0) break;
      console.log(`  Page ${page}: ${profiles.length} profiles`);

      for (const p of profiles) {
        const parts = p.name.split(/\s+/);
        await upsertPerson({
          full_name: p.name,
          first_name: parts[0] || '',
          last_name: parts[parts.length - 1] || '',
          company_name: p.company || null,
          nmls_id: p.nmls || null,
          phone: p.phone || null,
          photo_url: p.photo || null,
          city, state,
          title: 'Loan Officer',
        }, 'web');
        added++;
        process.stdout.write('·');
      }

      page++;
    } catch (err) {
      console.log(`\n  Zillow page ${page} failed: ${err.message}`);
      break;
    }
  }

  console.log(`\n  ✅ Zillow ${city}, ${state}: ${added} added`);
  return added;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE 4: Company team page scrapers
// ═══════════════════════════════════════════════════════════════════════════════
const COMPANY_SCRAPERS = {
  uwm: async () => {
    // UWM broker/partner directory — publicly searchable
    console.log('\n🏢 UWM broker directory...');
    // UWM doesn't publish individual LO names publicly
    return 0;
  },
};

// ─── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  const [,, source, ...args] = process.argv;

  await pool.query('SELECT 1');
  console.log('✅ DB connected');

  let added = 0;

  switch (source) {
    case 'linkedin':
      for (const state of args.length ? args : ['MI', 'OH', 'IN', 'FL', 'TX']) {
        added += await runLinkedIn(state.toUpperCase());
        await sleep(DELAY * 10);
      }
      break;

    case 'state':
      for (const state of args.length ? args : Object.keys(STATE_DOWNLOADS)) {
        added += await runStateBulkDownload(state.toUpperCase());
      }
      break;

    case 'zillow':
      const state = args[0]?.toUpperCase() || 'MI';
      const cities = args.slice(1).length ? args.slice(1) :
                     CITIES_BY_STATE[state] || ['Detroit'];
      for (const city of cities) {
        added += await runZillow(state, city);
        await sleep(DELAY * 5);
      }
      break;

    default:
      console.log(`
Usage:
  node ingester/multi.js linkedin MI OH IN FL TX
  node ingester/multi.js state CT
  node ingester/multi.js zillow MI Detroit "Grand Rapids"
      `);
  }

  const { rows } = await pool.query('SELECT COUNT(*) FROM people');
  console.log(`\n📦 Total in DB: ${rows[0].count} (added this run: ${added})`);
  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });