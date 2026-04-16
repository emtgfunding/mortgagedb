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

// ─── Google search for LinkedIn profile + extract snippet data ───────────────
// Returns { url, title, snippet } or null
// If knownUrl is provided, searches for that exact URL to get Google's snippet.
async function findLinkedInViaGoogle(page, person, knownUrl = null) {
  let query;
  if (knownUrl) {
    // Extract the slug and search for it directly
    const slug = knownUrl.match(/linkedin\.com\/in\/([\w-]+)/)?.[1] || '';
    query = `site:linkedin.com/in/${slug}`;
  } else {
    const name = `${person.first_name} ${person.last_name}`.trim();
    const company = person.company_name ? ` "${person.company_name}"` : '';
    query = `site:linkedin.com/in "${name}"${company} mortgage loan`;
  }
  const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=5`;

  try {
    await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await sleep(2000);

    const result = await page.evaluate((personName) => {
      // Google's DOM changes frequently. The most reliable anchors are:
      // - h3 elements contain the clean result title
      // - The h3's parent <a> has the (sometimes truncated) LinkedIn URL
      // - The h3's ancestor container has the full snippet text
      const h3s = document.querySelectorAll('h3');
      let bestMatch = null;

      for (const h3 of h3s) {
        const title = h3.textContent?.trim() || '';
        if (!title) continue;

        // Find the parent link
        const link = h3.closest('a');
        if (!link) continue;
        const href = link.href || '';
        if (!href.includes('linkedin.com/in/')) continue;

        // Extract LinkedIn URL (may be truncated by Google)
        const urlMatch = decodeURIComponent(href).match(/(https?:\/\/[a-z]+\.linkedin\.com\/in\/[\w-]+)/);
        const url = urlMatch ? urlMatch[1] : href;

        // Get the containing result block — walk up to find a large container
        let container = h3;
        for (let j = 0; j < 8; j++) {
          container = container.parentElement;
          if (!container) break;
          if (container.getAttribute('data-hveid') || container.getAttribute('data-sokoban-container')) break;
        }

        // Get ALL text in the container — this includes the snippet
        const fullText = container?.textContent || '';

        if (!bestMatch) {
          bestMatch = { url, title, snippet: fullText.substring(0, 600) };
        }

        // Prefer results whose title contains our person's name
        const nameWords = personName.toLowerCase().split(/\s+/);
        const titleLower = title.toLowerCase();
        if (nameWords.filter(w => w.length > 2).some(w => titleLower.includes(w))) {
          bestMatch = { url, title, snippet: fullText.substring(0, 600) };
          break;
        }
      }

      return bestMatch;
    }, name);

    return result;
  } catch {
    return null;
  }
}

// ─── Scrape LinkedIn profile data ────────────────────────────────────────────
// Strategy: LinkedIn serves JSON-LD structured data even on the login-gated
// page. We extract that first, then fall back to visible DOM elements.
async function scrapeLinkedInProfile(page, url) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20000 });
    await sleep(2500);

    const profile = await page.evaluate(() => {
      const getText = (sel) => {
        const el = document.querySelector(sel);
        return el ? el.textContent.trim() : null;
      };

      let headline = '';
      let name = '';
      let location = '';
      let photo_url = null;
      const experiences = [];

      // ── Strategy 1: JSON-LD structured data ──
      // LinkedIn embeds Schema.org Person/ProfilePage JSON-LD even on gated pages
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const script of scripts) {
        try {
          const data = JSON.parse(script.textContent);
          const items = Array.isArray(data) ? data : [data];

          for (const item of items) {
            // Person schema
            if (item['@type'] === 'Person' || item['@type']?.includes?.('Person')) {
              name = item.name || name;
              headline = item.jobTitle || item.description || headline;
              location = item.address?.addressLocality || location;
              photo_url = item.image?.contentUrl || item.image || photo_url;

              // worksFor can be an array of organizations
              const worksFor = Array.isArray(item.worksFor) ? item.worksFor : item.worksFor ? [item.worksFor] : [];
              for (const org of worksFor) {
                experiences.push({
                  title: item.jobTitle || null,
                  company: org.name || null,
                  start_date: null,
                  end_date: null,
                  is_current: true,
                  raw_dates: null,
                });
              }

              // alumniOf for past companies/education
              const alumni = Array.isArray(item.alumniOf) ? item.alumniOf : [];
              for (const org of alumni) {
                if (org['@type'] === 'Organization') {
                  experiences.push({
                    title: null,
                    company: org.name || null,
                    start_date: null,
                    end_date: null,
                    is_current: false,
                    raw_dates: null,
                  });
                }
              }
            }

            // ProfilePage might have mainEntity
            if (item.mainEntity) {
              const me = item.mainEntity;
              name = me.name || name;
              headline = me.jobTitle || me.description || headline;
              const meWorks = Array.isArray(me.worksFor) ? me.worksFor : me.worksFor ? [me.worksFor] : [];
              for (const org of meWorks) {
                if (!experiences.some(e => e.company === org.name)) {
                  experiences.push({
                    title: me.jobTitle || null,
                    company: org.name || null,
                    start_date: null,
                    end_date: null,
                    is_current: true,
                    raw_dates: null,
                  });
                }
              }
            }
          }
        } catch {}
      }

      // ── Strategy 2: Visible DOM (works on non-gated public profiles) ──
      if (!name) {
        name = getText('h1') || '';
      }
      if (!headline) {
        headline = getText('.top-card-layout__headline') ||
                   getText('[data-section="headline"]') ||
                   getText('.text-body-medium.break-words') || '';
      }
      if (!location) {
        location = getText('.top-card-layout__first-subline') ||
                   getText('[class*="location"]') || '';
      }
      if (!photo_url) {
        const img = document.querySelector('img.top-card__profile-image, img[class*="profile-photo"], img.pv-top-card-profile-picture__image');
        photo_url = img?.src || null;
      }

      // Experience from DOM (if visible — logged-in or truly public profiles)
      if (experiences.length === 0) {
        const expSection = document.querySelector('#experience, [data-section="experience"], section.experience');
        if (expSection) {
          const items = expSection.querySelectorAll('li, [class*="experience-item"], [class*="position"]');
          items.forEach(item => {
            const title = item.querySelector('h3, [class*="title"]')?.textContent?.trim() || '';
            const company = item.querySelector('h4, [class*="subtitle"], [class*="company"]')?.textContent?.trim() || '';
            const dates = item.querySelector('[class*="date-range"], [class*="dates"], span.date-range')?.textContent?.trim() || '';
            if (title || company) {
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
      }

      // ── Strategy 3: Parse the page title (always available) ──
      // LinkedIn page titles: "Name - Title - Company | LinkedIn"
      if (!headline && document.title) {
        const parts = document.title.replace(/\s*[|·]\s*LinkedIn.*$/, '').split(/\s*[-–]\s*/);
        if (parts.length >= 2) {
          if (!name) name = parts[0].trim();
          headline = parts.slice(1).join(' - ').trim();
          // Extract company from "Title at Company" pattern
          const atMatch = headline.match(/(.+?)\s+at\s+(.+)/i);
          if (atMatch && experiences.length === 0) {
            experiences.push({
              title: atMatch[1].trim(),
              company: atMatch[2].trim(),
              start_date: null, end_date: null,
              is_current: true, raw_dates: null,
            });
          }
        }
      }

      return { name, headline, location, photo_url, experiences };
    });

    return profile;
  } catch (err) {
    console.log(`    ⚠ Scrape failed: ${err.message}`);
    return null;
  }
}

// ─── Enrich from Google snippet when LinkedIn is fully gated ─────────────────
// Google search results for LinkedIn profiles typically look like:
//   Title: "First Last - Senior Loan Officer - UWM | LinkedIn"
//   Snippet: "Senior Loan Officer at United Wholesale Mortgage. Greater Detroit Area.
//             Experience: UWM · 2020 - Present. Previous: Quicken Loans, Flagstar Bank"
function parseGoogleSnippet(googleResult) {
  if (!googleResult) return [];
  const experiences = [];
  const seen = new Set();

  const addExp = (title, company, isCurrent) => {
    if (!company && !title) return;
    const key = `${title||''}|${company||''}`;
    if (seen.has(key)) return;
    seen.add(key);
    experiences.push({
      title: title || null, company: company || null,
      start_date: null, end_date: null,
      is_current: isCurrent, raw_dates: null,
    });
  };

  const title = googleResult.title || '';
  const snippet = googleResult.snippet || '';

  // ── Parse the h3 title ──
  // Common formats:
  //   "Alex Farhat - Swift Home Loans"
  //   "John Smith - Loan Officer at UWM"
  //   "Jane Doe - Senior MLO - Rocket Mortgage"
  const cleanTitle = title
    .replace(/\s*[|·]\s*LinkedIn.*$/i, '')  // strip "| LinkedIn"
    .replace(/LinkedIn.*$/i, '')             // strip trailing "LinkedIn..."
    .trim();

  const titleParts = cleanTitle.split(/\s*[-–]\s*/);
  if (titleParts.length >= 2) {
    const rest = titleParts.slice(1).join(' - ');
    const atMatch = rest.match(/(.+?)\s+at\s+(.+)/i);
    if (atMatch) {
      addExp(atMatch[1].trim(), atMatch[2].trim(), true);
    } else if (titleParts.length >= 3) {
      addExp(titleParts[1].trim(), titleParts[2].trim(), true);
    } else if (titleParts.length === 2) {
      const val = titleParts[1].trim();
      if (val && val.length > 1 && !val.toLowerCase().includes('linkedin')) {
        addExp(null, val, true);
      }
    }
  }

  // ── Parse the full container text (snippet) ──
  // Google's container text for LinkedIn results looks like:
  //   "Alex Farhat - Swift Home LoansLinkedIn · Alex Farhat670+ followers
  //    Canton, Michigan, United States · Swift Home Loans
  //    Alex Farhat. Swift Home Loans Wayne State University..."
  //
  // Or for richer profiles:
  //   "... Mortgage Loan Officer · Elite Loans
  //    Experience: UWM · 2020 - Present · Previous: Quicken Loans..."

  // "Title at Company" anywhere in text
  const atMatches = snippet.matchAll(/([A-Z][a-zA-Z\s]+?)\s+at\s+([A-Z][a-zA-Z\s&,.]+?)(?:\.|,|·|\d|$)/g);
  for (const m of atMatches) {
    const t = m[1].trim();
    const c = m[2].trim();
    // Filter out junk matches
    if (t.length > 2 && t.length < 60 && c.length > 2 && c.length < 80) {
      addExp(t, c, true);
    }
  }

  // "· Company Name" pattern (Google shows "Location · Company" for LinkedIn)
  const dotCompany = snippet.matchAll(/·\s+([A-Z][a-zA-Z\s&]+?)(?:\s*\d|\s*·|\s*$)/g);
  for (const m of dotCompany) {
    const c = m[1].trim();
    // Filter: must look like a company name (not a location, not "followers")
    if (c.length > 3 && c.length < 80
        && !c.match(/United States|Michigan|California|followers|connections|Read more/i)
        && !c.match(/^(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)$/)) {
      addExp(null, c, true);
    }
  }

  // "Experience:" section
  const expMatch = snippet.match(/Experience[:\s]+(.+?)(?:Education|Skills|$)/is);
  if (expMatch) {
    const expParts = expMatch[1].split(/[·,]\s*/);
    for (const part of expParts) {
      const clean = part.replace(/\d{4}\s*[-–]\s*(Present|\d{4})/gi, '').trim();
      if (clean && clean.length > 2 && clean.length < 100) {
        const atM = clean.match(/(.+?)\s+at\s+(.+)/i);
        if (atM) addExp(atM[1].trim(), atM[2].trim(), /present/i.test(part));
        else addExp(null, clean, /present/i.test(part));
      }
    }
  }

  // "Previous:" pattern
  const prevMatch = snippet.match(/Previous(?:ly)?[:\s]+(.+?)(?:\.|Education|Skills|$)/i);
  if (prevMatch) {
    const prevParts = prevMatch[1].split(/[,·]\s*/);
    for (const part of prevParts) {
      const clean = part.trim();
      if (clean && clean.length > 2) {
        const atM = clean.match(/(.+?)\s+at\s+(.+)/i);
        if (atM) addExp(atM[1].trim(), atM[2].trim(), false);
        else addExp(null, clean, false);
      }
    }
  }

  return experiences;
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
      let googleData = null;

      // Step 1: ALWAYS Google search — even if we already have the LinkedIn URL.
      // LinkedIn gates almost everything behind login now, so Google's cached
      // snippet ("Title at Company · Previous: ...") is our best data source.
      process.stdout.write('→ googling...');
      if (linkedinUrl) {
        // Search for the exact LinkedIn profile to get Google's snippet
        googleData = await findLinkedInViaGoogle(page, person, linkedinUrl);
      } else {
        googleData = await findLinkedInViaGoogle(page, person);
      }
      await sleep(DELAY);

      if (!linkedinUrl && googleData) {
        linkedinUrl = googleData.url;
        process.stdout.write(' found');
      } else if (!linkedinUrl && !googleData) {
        process.stdout.write(' not found');
        await markAttempted(person.id);
        failed++;
        continue;
      }

      // Step 2: Scrape the LinkedIn profile (JSON-LD + title parsing)
      process.stdout.write(' → scraping...');
      await sleep(DELAY);
      const profile = await scrapeLinkedInProfile(page, linkedinUrl);

      if (!profile) {
        // Even if scraping failed, try to save Google snippet data
        if (googleData) {
          const snippetExps = parseGoogleSnippet(googleData);
          if (snippetExps.length > 0) {
            const fallback = { name: '', headline: googleData.title || '', location: '', photo_url: null, experiences: snippetExps };
            await saveEnrichment(person.id, linkedinUrl, fallback);
            process.stdout.write(` 📎 (${snippetExps.length} from Google)`);
            enriched++;
            continue;
          }
        }
        await markAttempted(person.id);
        failed++;
        continue;
      }

      // Step 3: If LinkedIn returned 0 experiences, merge in Google snippet data
      if (profile.experiences.length === 0 && googleData) {
        const snippetExps = parseGoogleSnippet(googleData);
        profile.experiences.push(...snippetExps);
      }

      // Also try to parse headline for employment if still empty
      if (profile.experiences.length === 0 && profile.headline) {
        const atMatch = profile.headline.match(/(.+?)\s+at\s+(.+)/i);
        if (atMatch) {
          profile.experiences.push({
            title: atMatch[1].trim(),
            company: atMatch[2].trim(),
            start_date: null, end_date: null,
            is_current: true, raw_dates: null,
          });
        }
      }

      // Step 4: Save to DB
      const ok = await saveEnrichment(person.id, linkedinUrl, profile);
      if (ok) {
        const expCount = profile.experiences?.length || 0;
        const src = expCount > 0 ? '✅' : '📎';
        process.stdout.write(` ${src} (${expCount} positions)`);
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
