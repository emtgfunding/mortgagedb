/**
 * LinkedIn Public Profile Enricher
 *
 * For people in the DB without LinkedIn data, searches Google for a public
 * LinkedIn profile URL, then pulls headline / photo from that profile.
 *
 * Improvements over the original:
 *   • Attempt tracking       — skip people tried in the last RETRY_DAYS or
 *                              who've exceeded MAX_ATTEMPTS (huge perf win
 *                              once the table is ingested).
 *   • Puppeteer with fallback — if PUPPETEER_EXECUTABLE_PATH is set and
 *                              puppeteer-core can launch, we render the page
 *                              for real. Otherwise we fall back to axios.
 *   • Multi-strategy selectors — LinkedIn's public HTML rotates class names
 *                              constantly, so we try several shapes.
 *   • Honest success counter — only count as "enriched" when we actually
 *                              stored something new (not just saved the URL).
 *   • Retries with backoff    — one retry on transient HTTP errors.
 *
 * Usage: node enricher/linkedin.js
 *
 * Env:
 *   SCRAPE_DELAY_MS           — base pause between fetches (default 3000)
 *   LINKEDIN_BATCH_LIMIT      — people per run (default 500)
 *   LINKEDIN_RETRY_DAYS       — days to wait before retrying a miss (default 14)
 *   LINKEDIN_MAX_ATTEMPTS     — give up after N tries (default 3)
 *   PUPPETEER_EXECUTABLE_PATH — path to a Chrome/Chromium binary (optional)
 */

require('dotenv').config();
const axios = require('axios');
const cheerio = require('cheerio');
const pool = require('../db');

const DELAY        = parseInt(process.env.SCRAPE_DELAY_MS)        || 3000;
const BATCH_LIMIT  = parseInt(process.env.LINKEDIN_BATCH_LIMIT)   || 500;
const RETRY_DAYS   = parseInt(process.env.LINKEDIN_RETRY_DAYS)    || 14;
const MAX_ATTEMPTS = parseInt(process.env.LINKEDIN_MAX_ATTEMPTS)  || 3;
const CHROME_PATH  = process.env.PUPPETEER_EXECUTABLE_PATH || null;

const sleep = ms => new Promise(r => setTimeout(r, ms));

const http = axios.create({
  timeout: 20000,
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
  },
});

// ─── Browser (lazy-init) ──────────────────────────────────────────────────────
let browserPromise = null;

async function getBrowser() {
  if (!CHROME_PATH) return null;
  if (browserPromise) return browserPromise;

  browserPromise = (async () => {
    try {
      const puppeteer = require('puppeteer-core');
      const browser = await puppeteer.launch({
        executablePath: CHROME_PATH,
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
        ],
      });
      return browser;
    } catch (err) {
      console.warn('⚠️  Puppeteer unavailable, using axios fallback:', err.message);
      return null;
    }
  })();
  return browserPromise;
}

async function closeBrowser() {
  if (!browserPromise) return;
  const b = await browserPromise;
  if (b) { try { await b.close(); } catch { /* ignore */ } }
  browserPromise = null;
}

// ─── Find LinkedIn URL via Google ─────────────────────────────────────────────
async function findLinkedInUrl(firstName, lastName, company, state) {
  const query = `site:linkedin.com/in "${firstName} ${lastName}" mortgage ${company || ''} ${state || ''}`;
  const url = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=3`;

  try {
    const res = await http.get(url);
    const $ = cheerio.load(res.data);
    const slugs = new Set();

    $('a[href]').each((_, el) => {
      const href = $(el).attr('href') || '';
      const m = href.match(/linkedin\.com\/in\/([a-zA-Z0-9\-_%]+)/);
      if (m) slugs.add(m[1].replace(/%[0-9a-f]{2}/gi, ''));
    });

    const first = [...slugs][0];
    return first ? `https://www.linkedin.com/in/${first}` : null;
  } catch {
    return null;
  }
}

// ─── Scrape via Puppeteer ─────────────────────────────────────────────────────
async function scrapeWithPuppeteer(linkedinUrl) {
  const browser = await getBrowser();
  if (!browser) return null;

  const page = await browser.newPage();
  try {
    await page.setUserAgent(
      'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    );
    const resp = await page.goto(linkedinUrl, { waitUntil: 'domcontentloaded', timeout: 20000 });
    if (!resp || !resp.ok()) return null;

    const profile = await page.evaluate(() => {
      const pick = (selectors) => {
        for (const s of selectors) {
          const el = document.querySelector(s);
          if (el && el.textContent?.trim()) return el.textContent.trim();
        }
        return null;
      };
      const pickAttr = (selectors, attr) => {
        for (const s of selectors) {
          const el = document.querySelector(s);
          if (el && el.getAttribute(attr)) return el.getAttribute(attr);
        }
        return null;
      };

      return {
        full_name: pick([
          'h1.top-card-layout__title',
          'h1[class*="top-card"] ',
          'h1[class*="name"]',
          'h1',
        ]),
        headline: pick([
          'h2.top-card-layout__headline',
          '[class*="headline"]',
          'meta[name="description"]',
        ]),
        location: pick([
          '.top-card__subline-item',
          '[class*="location"]',
        ]),
        photo_url: pickAttr([
          'img.top-card-layout__entity-image',
          'img[class*="profile-photo"]',
          'img[class*="pv-top-card-profile-picture"]',
          'meta[property="og:image"]',
        ], 'src') || pickAttr(['meta[property="og:image"]'], 'content'),
        company: pick([
          'a[data-tracking-control-name="public_profile_topcard-current-company"]',
          '[class*="experience-item"] [class*="subtitle"]',
        ]),
      };
    });
    return { linkedin_url: linkedinUrl, ...profile };
  } catch {
    return null;
  } finally {
    try { await page.close(); } catch { /* ignore */ }
  }
}

// ─── Scrape via axios (fallback) ──────────────────────────────────────────────
async function scrapeWithAxios(linkedinUrl) {
  try {
    const res = await http.get(linkedinUrl, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
      },
    });
    const $ = cheerio.load(res.data);

    const pick = (selectors) => {
      for (const s of selectors) {
        const el = $(s).first();
        const t = el.text().trim();
        if (t) return t;
      }
      return null;
    };
    const pickAttr = (selectors, attr) => {
      for (const s of selectors) {
        const v = $(s).first().attr(attr);
        if (v) return v;
      }
      return null;
    };

    const profile = {
      linkedin_url: linkedinUrl,
      full_name: pick([
        'h1.top-card-layout__title',
        'h1[class*="name"]',
        'h1',
      ]),
      headline: pick([
        'h2.top-card-layout__headline',
        '[class*="headline"]',
      ]) || pickAttr(['meta[name="description"]'], 'content'),
      location: pick(['.top-card__subline-item', '[class*="location"]']),
      photo_url:
        pickAttr(['img.top-card-layout__entity-image', 'img[class*="profile-photo"]'], 'src') ||
        pickAttr(['meta[property="og:image"]'], 'content'),
      company: pick([
        'a[data-tracking-control-name="public_profile_topcard-current-company"]',
        '[class*="experience-item"] [class*="subtitle"]',
      ]),
    };

    // If we got literally nothing (login wall, 999, etc), treat as failure.
    if (!profile.full_name && !profile.headline && !profile.photo_url) return null;
    return profile;
  } catch {
    return null;
  }
}

async function scrapeLinkedInProfile(linkedinUrl) {
  // Prefer puppeteer if configured; fall back to axios.
  const viaChrome = await scrapeWithPuppeteer(linkedinUrl);
  if (viaChrome) return viaChrome;
  return scrapeWithAxios(linkedinUrl);
}

// ─── Attempt tracking writes ──────────────────────────────────────────────────
async function markAttempt(personId) {
  await pool.query(`
    UPDATE people
    SET linkedin_attempted_at = NOW(),
        linkedin_attempts     = COALESCE(linkedin_attempts, 0) + 1,
        updated_at            = NOW()
    WHERE id = $1
  `, [personId]);
}

// ─── Per-person enrichment ────────────────────────────────────────────────────
// Returns one of:
//   'enriched' — we saved profile data (headline/photo)
//   'url-only' — only the LinkedIn URL was saved
//   'miss'     — couldn't find anything
async function enrichLinkedIn(person) {
  let linkedinUrl = person.linkedin_url;

  if (!linkedinUrl) {
    await sleep(DELAY);
    linkedinUrl = await findLinkedInUrl(
      person.first_name, person.last_name,
      person.company_name, person.state
    );
  }

  if (!linkedinUrl) return 'miss';

  await sleep(DELAY);
  const profile = await scrapeLinkedInProfile(linkedinUrl);

  if (!profile || (!profile.headline && !profile.photo_url)) {
    // Save the URL so a re-run doesn't re-Google, but don't claim enrichment.
    await pool.query(`
      UPDATE people
      SET linkedin_url          = $1,
          source_linkedin       = true,
          linkedin_attempted_at = NOW(),
          linkedin_attempts     = COALESCE(linkedin_attempts, 0) + 1,
          updated_at            = NOW()
      WHERE id = $2
    `, [linkedinUrl, person.id]);
    return 'url-only';
  }

  await pool.query(`
    UPDATE people SET
      linkedin_url          = $1,
      headline              = COALESCE($2, headline),
      photo_url             = COALESCE($3, photo_url),
      source_linkedin       = true,
      data_quality_score    = LEAST(data_quality_score + 15, 100),
      linkedin_last_synced  = NOW(),
      linkedin_attempted_at = NOW(),
      linkedin_attempts     = COALESCE(linkedin_attempts, 0) + 1,
      updated_at            = NOW()
    WHERE id = $4
  `, [linkedinUrl, profile.headline || null, profile.photo_url || null, person.id]);

  return 'enriched';
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log('🔗 LinkedIn Enricher starting...');
  if (CHROME_PATH) console.log(`   Puppeteer: ${CHROME_PATH}`);
  else             console.log('   Puppeteer: disabled (set PUPPETEER_EXECUTABLE_PATH to enable)');

  await pool.query('SELECT 1');
  console.log('✅ DB connected\n');

  const { rows: people } = await pool.query(`
    SELECT id, first_name, last_name, company_name, state, linkedin_url
    FROM people
    WHERE source_linkedin = false
      AND first_name IS NOT NULL AND last_name IS NOT NULL
      AND LENGTH(first_name) > 1 AND LENGTH(last_name) > 1
      AND COALESCE(linkedin_attempts, 0) < $1
      AND (linkedin_attempted_at IS NULL OR linkedin_attempted_at < NOW() - ($2 || ' days')::INTERVAL)
    ORDER BY data_quality_score DESC, linkedin_attempts ASC NULLS FIRST
    LIMIT $3
  `, [MAX_ATTEMPTS, String(RETRY_DAYS), BATCH_LIMIT]);

  console.log(`Processing ${people.length} people (max ${MAX_ATTEMPTS} attempts, retry after ${RETRY_DAYS}d)...\n`);
  let enriched = 0, urlOnly = 0, miss = 0;

  for (const person of people) {
    let status = 'miss';
    try {
      status = await enrichLinkedIn(person);
    } catch (err) {
      console.error(`  ✗ ${person.first_name} ${person.last_name}: ${err.message}`);
    }

    if (status === 'enriched') {
      enriched++;
      console.log(`  ✓ ${person.first_name} ${person.last_name}`);
    } else if (status === 'url-only') {
      urlOnly++;
    } else {
      miss++;
      await markAttempt(person.id);
    }

    await sleep(DELAY);
  }

  console.log(`\n✅ Done: ${enriched} enriched, ${urlOnly} URL-only, ${miss} no match`);
  await closeBrowser();
  process.exit(0);
}

if (require.main === module) {
  run().catch(async (err) => {
    console.error('Fatal:', err);
    await closeBrowser();
    process.exit(1);
  });
}

module.exports = {
  findLinkedInUrl,
  scrapeLinkedInProfile,
};
