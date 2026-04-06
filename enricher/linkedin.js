/**
 * LinkedIn Public Profile Enricher
 * 
 * Searches Google for LinkedIn profile URLs for people in our DB,
 * then fetches publicly visible data from those profiles.
 * Uses only publicly accessible data - no login required.
 * 
 * Usage: node enricher/linkedin.js
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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
  }
});

// ─── Find LinkedIn URL via Google ─────────────────────────────────────────────
async function findLinkedInUrl(firstName, lastName, company, state) {
  const query = `site:linkedin.com/in "${firstName} ${lastName}" mortgage ${company || ''} ${state || ''}`;
  const url = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=3`;
  
  try {
    const res = await http.get(url);
    const $ = cheerio.load(res.data);
    
    // Extract LinkedIn URLs from Google results
    const links = [];
    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const match = href.match(/linkedin\.com\/in\/([a-zA-Z0-9\-_]+)/);
      if (match) {
        links.push(`https://www.linkedin.com/in/${match[1]}`);
      }
    });

    return links[0] || null;
  } catch {
    return null;
  }
}

// ─── Scrape public LinkedIn profile (no login) ────────────────────────────────
async function scrapeLinkedInProfile(linkedinUrl) {
  try {
    // LinkedIn public profile - accessible without login
    const res = await http.get(linkedinUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
      }
    });
    
    const $ = cheerio.load(res.data);
    const profile = { linkedin_url: linkedinUrl };

    // Name
    profile.full_name = $('h1.top-card-layout__title, h1[class*="name"]').first().text().trim();
    
    // Headline
    profile.headline = $('[class*="headline"], [class*="title"]').first().text().trim();
    
    // Location
    profile.location = $('[class*="location"]').first().text().trim();

    // Photo
    profile.photo_url = $('img[class*="profile-photo"], img[class*="pv-top-card-profile-picture"]')
                          .first().attr('src') || null;

    // Current company
    profile.company = $('[class*="company"], [class*="workplace"]').first().text().trim();

    // Extract state from location
    if (profile.location) {
      const stateMatch = profile.location.match(/,\s*([A-Z]{2})\b/);
      if (stateMatch) profile.state = stateMatch[1];
    }

    return profile;
  } catch {
    return null;
  }
}

// ─── Enrich a person's LinkedIn ────────────────────────────────────────────────
async function enrichLinkedIn(person) {
  // Step 1: Find LinkedIn URL if we don't have it
  let linkedinUrl = person.linkedin_url;
  
  if (!linkedinUrl) {
    await sleep(DELAY);
    linkedinUrl = await findLinkedInUrl(
      person.first_name, person.last_name,
      person.company_name, person.state
    );
  }

  if (!linkedinUrl) return false;

  // Step 2: Scrape public profile
  await sleep(DELAY);
  const profile = await scrapeLinkedInProfile(linkedinUrl);
  
  if (!profile) {
    // At least save the URL
    await pool.query(`
      UPDATE people SET linkedin_url = $1, source_linkedin = true, updated_at = NOW()
      WHERE id = $2
    `, [linkedinUrl, person.id]);
    return true;
  }

  // Step 3: Update DB with enriched data
  await pool.query(`
    UPDATE people SET
      linkedin_url       = $1,
      headline           = COALESCE($2, headline),
      photo_url          = COALESCE($3, photo_url),
      source_linkedin    = true,
      data_quality_score = LEAST(data_quality_score + 15, 100),
      linkedin_last_synced = NOW(),
      updated_at         = NOW()
    WHERE id = $4
  `, [
    linkedinUrl,
    profile.headline || null,
    profile.photo_url || null,
    person.id
  ]);

  return true;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log('🔗 LinkedIn Enricher starting...');
  await pool.query('SELECT 1');
  console.log('✅ DB connected\n');

  const { rows: people } = await pool.query(`
    SELECT id, first_name, last_name, company_name, state, linkedin_url
    FROM people
    WHERE source_linkedin = false
      AND first_name IS NOT NULL AND last_name IS NOT NULL
      AND LENGTH(first_name) > 1 AND LENGTH(last_name) > 1
    ORDER BY data_quality_score DESC
    LIMIT 500
  `);

  console.log(`Processing ${people.length} people...\n`);
  let enriched = 0;

  for (const person of people) {
    const ok = await enrichLinkedIn(person);
    if (ok) {
      enriched++;
      console.log(`  ✓ ${person.first_name} ${person.last_name}`);
    }
    await sleep(DELAY);
  }

  console.log(`\n✅ Done: ${enriched}/${people.length} enriched with LinkedIn`);
  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
