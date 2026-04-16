#!/usr/bin/env node
/**
 * Profile Enricher — LinkedIn + Google
 *
 * For each person in the DB who is missing LinkedIn/employment data,
 * searches Google for their public LinkedIn profile, scrapes what's
 * publicly visible (headline, current company, past positions), and
 * writes it into people + employment_history.
 *
 * Uses puppeteer-core (your installed Chrome) to bypass bot detection.
 *
 * Usage:
 *   npm install puppeteer-core
 *   node ingester/enrich-profiles.js                    # enrich up to 200 people
 *   node ingester/enrich-profiles.js --limit 500        # enrich 500
 *   node ingester/enrich-profiles.js --state FL         # only FL people
 *   node ingester/enrich-profiles.js --force            # re-enrich even if already attempted
 */

require('dotenv').config();
const pool = require('../db');
const fs = require('fs');

const DELAY = parseInt(process.env.SCRAPE_DELAY_MS) || 4000;
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── Find Chrome ─────────────────────────────────────────────────────────────
function findChrome() {
  const paths = [
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    process.env.LOCALAPPDATA && `${process.env.LOCALAPPDATA}\\Google\\Chrome\\Application\\chrome.exe`,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium-browser',
  ].filter(Boolean);
  for (const p of paths) { if (fs.existsSync(p)) return p; }
  return null;
}

// ─── Parse command-line args ─────────────────────────────────────────────────
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { limit: 200, state: null, force: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--limit' && args[i+1]) { opts.limit = parseInt(args[++i]); }
    else if (args[i] === '--state' && args[i+1]) { opts.state = args[++i].toUpperCase(); }
    else if (args[i] === '--force') { opts.force = true; }
  }
  return opts;
}

// ─── Google search for LinkedIn profile ──────────────────────────────────────
async function findLinkedInUrl(page, person) {
  const query = `site:linkedin.com/in "${person.first_name} ${person.last_name}" mortgage`;
  const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=5`;

  try {
    await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await sleep(1500);

    const linkedinUrl = await page.evaluate(() => {
      const links = document.querySelectorAll('a[href*="linkedin.com/in/"]');
      for (const link of links) {
        const href = link.href;
        const match = href.match(/https?:\/\/[a-z]+\.linkedin\.com\/in\/[\w-]+/);
        if (match) return match[0];
        // Google wraps URLs — check data attributes or redirect URLs
        const googleHref = link.getAttribute('href') || '';
        const decoded = decodeURIComponent(googleHref);
        const m2 = decoded.match(/https?:\/\/[a-z]+\.linkedin\.com\/in\/[\w-]+/);
        if (m2) return m2[0];
      }
      return null;
    });

    return linkedinUrl;
  } catch {
    return null;
  }
}

// ─── Scrape public LinkedIn profile ──────────────────────────────────────────
async function scrapeLinkedInProfile(page, url) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20000 });
    await sleep(2000);

    const profile = await page.evaluate(() => {
      const getText = (sel) => {
        const el = document.querySelector(sel);
        return el ? el.textContent.trim() : null;
      };

      // Name & headline
      const name = getText('h1') || '';
      const headline = getText('.top-card-layout__headline') ||
                       getText('[data-section="headline"]') ||
                       getText('h2') || '';
      const location = getText('.top-card-layout__first-subline') ||
                       getText('[class*="location"]') || '';
      const photo = document.querySelector('img.top-card__profile-image, img[class*="profile-photo"]');

      // Experience section
      const experiences = [];
      const expSection = document.querySelector('#experience') ||
                         document.querySelector('[data-section="experience"]') ||
                         document.querySelector('section.experience');

      if (expSection) {
        const items = expSection.querySelectorAll('li, [class*="experience-item"], [class*="position"]');
        items.forEach(item => {
          const title = item.querySelector('h3, [class*="title"]')?.textContent?.trim() || '';
          const company = item.querySelector('h4, [class*="subtitle"], [class*="company"]')?.textContent?.trim() || '';
          const dates = item.querySelector('[class*="date-range"], [class*="dates"], span.date-range')?.textContent?.trim() || '';

          if (title || company) {
            // Parse dates like "Jan 2020 - Present" or "2018 - 2020"
            const dateMatch = dates.match(/(\w+\s*\d{4})\s*[-–]\s*(\w+\s*\d{4}|Present)/i);
            experiences.push({
              title: title || null,
              company: company || null,
              start_date: dateMatch ? dateMatch[1] : null,
              end_date: dateMatch ? dateMatch[2] : null,
              is_current: /present/i.test(dates),
              raw_dates: dates || null,
            });
          }
        });
      }

      return {
        name,
        headline,
        location,
        photo_url: photo?.src || null,
        experiences,
      };
    });

    return profile;
  } catch (err) {
    console.log(`    ⚠ Scrape failed: ${err.message}`);
    return null;
  }
}

// ─── Save enrichment data ────────────────────────────────────────────────────
async function saveEnrichment(personId, linkedinUrl, profile) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Update the person record
    await client.query(`
      UPDATE people SET
        linkedin_url  = COALESCE($1, linkedin_url),
        headline      = COALESCE($2, headline),
        photo_url     = COALESCE($3, photo_url),
        source_linkedin = true,
        linkedin_last_synced = NOW(),
        linkedin_attempted_at = NOW(),
        linkedin_attempts = linkedin_attempts + 1,
        updated_at = NOW()
      WHERE id = $4
    `, [linkedinUrl, profile.headline, profile.photo_url, personId]);

    // Insert employment history
    if (profile.experiences && profile.experiences.length > 0) {
      // Clear old employment history for this person
      await client.query('DELETE FROM employment_history WHERE person_id = $1', [personId]);

      for (const exp of profile.experiences) {
        const startDate = parseDate(exp.start_date);
        const endDate = exp.is_current ? null : parseDate(exp.end_date);

        await client.query(`
          INSERT INTO employment_history (person_id, company_name, start_date, end_date, is_current)
          VALUES ($1, $2, $3, $4, $5)
        `, [personId, exp.company || exp.title, startDate, endDate, exp.is_current || false]);
      }
    }

    await client.query('COMMIT');
    return true;
  } catch (err) {
    await client.query('ROLLBACK');
    console.log(`    ⚠ DB save failed: ${err.message}`);
    return false;
  } finally {
    client.release();
  }
}

function parseDate(str) {
  if (!str || /present/i.test(str)) return null;
  try {
    // Handle "Jan 2020", "2020", "January 2020"
    const d = new Date(str);
    return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
  } catch { return null; }
}

// Mark that we attempted enrichment (even if it failed)
async function markAttempted(personId) {
  await pool.query(`
    UPDATE people SET
      linkedin_attempted_at = NOW(),
      linkedin_attempts = linkedin_attempts + 1
    WHERE id = $1
  `, [personId]);
}

// ─── Main ────────────────────────────────────────────────────────────────────
async function run() {
  const opts = parseArgs();

  console.log(`\n🔍 MortgageDB — Profile Enricher`);
  console.log(`   Limit: ${opts.limit}`);
  console.log(`   State: ${opts.state || 'all'}`);
  console.log(`   Force: ${opts.force}\n`);

  await pool.query('SELECT 1');
  console.log('✅ DB connected');

  let puppeteer;
  try { puppeteer = require('puppeteer-core'); }
  catch { console.error('❌ puppeteer-core not installed. Run: npm install puppeteer-core'); process.exit(1); }

  const chromePath = process.env.CHROME_PATH || findChrome();
  if (!chromePath) { console.error('❌ Chrome not found. Set CHROME_PATH.'); process.exit(1); }
  console.log(`🌐 Chrome: ${chromePath}`);

  // Fetch candidates to enrich
  const stateFilter = opts.state ? `AND p.state = '${opts.state}'` : '';
  const attemptFilter = opts.force ? '' : `AND (p.linkedin_attempted_at IS NULL OR p.linkedin_attempts < 2)`;

  const { rows: candidates } = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.full_name,
           p.company_name, p.state, p.linkedin_url
    FROM people p
    WHERE p.first_name IS NOT NULL AND TRIM(p.first_name) <> ''
      AND (p.email IS NOT NULL OR p.verified_email IS NOT NULL OR p.phone IS NOT NULL OR p.linkedin_url IS NOT NULL)
      ${stateFilter}
      ${attemptFilter}
    ORDER BY
      -- Prioritize people who already have a LinkedIn URL but no employment history
      CASE WHEN p.linkedin_url IS NOT NULL AND p.linkedin_last_synced IS NULL THEN 0
           WHEN p.linkedin_url IS NULL THEN 1
           ELSE 2 END,
      p.data_quality_score DESC
    LIMIT ${opts.limit}
  `);

  console.log(`\n📋 ${candidates.length} candidates to enrich\n`);

  if (candidates.length === 0) {
    console.log('Nothing to do.');
    process.exit(0);
  }

  const browser = await puppeteer.launch({
    executablePath: chromePath,
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36');

  let enriched = 0, failed = 0;

  for (let i = 0; i < candidates.length; i++) {
    const person = candidates[i];
    const pct = ((i + 1) / candidates.length * 100).toFixed(0);
    process.stdout.write(`\n[${i + 1}/${candidates.length} ${pct}%] ${person.full_name} (${person.state || '?'}) `);

    try {
      let linkedinUrl = person.linkedin_url;

      // Step 1: Find LinkedIn URL if missing
      if (!linkedinUrl) {
        process.stdout.write('→ searching...');
        linkedinUrl = await findLinkedInUrl(page, person);
        await sleep(DELAY);

        if (!linkedinUrl) {
          process.stdout.write(' not found');
          await markAttempted(person.id);
          failed++;
          continue;
        }
        process.stdout.write(` found`);
      }

      // Step 2: Scrape the LinkedIn profile
      process.stdout.write(' → scraping...');
      await sleep(DELAY);
      const profile = await scrapeLinkedInProfile(page, linkedinUrl);

      if (!profile) {
        await markAttempted(person.id);
        failed++;
        continue;
      }

      // Step 3: Save to DB
      const ok = await saveEnrichment(person.id, linkedinUrl, profile);
      if (ok) {
        const expCount = profile.experiences?.length || 0;
        process.stdout.write(` ✅ (${expCount} positions)`);
        enriched++;
      } else {
        failed++;
      }

      await sleep(DELAY);
    } catch (err) {
      process.stdout.write(` ❌ ${err.message}`);
      await markAttempted(person.id);
      failed++;
    }
  }

  await browser.close();

  console.log(`\n\n📊 Summary:`);
  console.log(`   Enriched: ${enriched}`);
  console.log(`   Failed:   ${failed}`);

  const { rows: [stats] } = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE linkedin_url IS NOT NULL) as with_linkedin,
      COUNT(*) FILTER (WHERE linkedin_last_synced IS NOT NULL) as synced,
      (SELECT COUNT(*) FROM employment_history) as total_positions
    FROM people
  `);
  console.log(`\n📦 DB totals:`);
  console.log(`   With LinkedIn URL: ${stats.with_linkedin}`);
  console.log(`   LinkedIn synced:   ${stats.synced}`);
  console.log(`   Employment records: ${stats.total_positions}`);

  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
