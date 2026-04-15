/**
 * Email Enricher
 *
 * For each person in `people`, figure out a likely email address based on
 * name + company domain, verify it via SMTP (no mail actually sent), and
 * store the result.
 *
 * Improvements over the original:
 *   • Attempt tracking         — skip people tried in the last RETRY_DAYS days
 *                                or who've hit MAX_ATTEMPTS, so a run doesn't
 *                                re-process the same 1000 misses every time.
 *   • Company pattern learning — once we confirm first.last@acme.com works,
 *                                we cache the pattern per-domain and try it
 *                                FIRST for every other employee of acme.com.
 *   • Seed from NMLS email     — if NMLS already has an email for someone at
 *                                the company, we learn the pattern from that
 *                                before guessing.
 *   • MX cache                 — resolve each domain's MX once per run
 *                                (persisted in domain_mx_cache).
 *   • Catch-all detection      — if a random mailbox at the domain gets
 *                                accepted, we mark the domain catch-all and
 *                                refuse to trust verifies there.
 *   • Better domain guesser    — keeps keywords like "mortgage" and tries
 *                                common multi-word + TLD variants.
 *
 * Usage: node enricher/email.js
 *
 * Env:
 *   SCRAPE_DELAY_MS     — pause between people (default 500ms)
 *   EMAIL_BATCH_LIMIT   — how many people per run (default 1000)
 *   EMAIL_RETRY_DAYS    — days to wait before retrying a miss (default 14)
 *   EMAIL_MAX_ATTEMPTS  — give up after this many tries (default 3)
 *   SMTP_TIMEOUT_MS     — per-mailbox SMTP timeout (default 10000)
 */

require('dotenv').config();
const dns = require('dns').promises;
const net = require('net');
const crypto = require('crypto');
const pool = require('../db');

const DELAY          = parseInt(process.env.SCRAPE_DELAY_MS)    || 500;
const BATCH_LIMIT    = parseInt(process.env.EMAIL_BATCH_LIMIT)  || 1000;
const RETRY_DAYS     = parseInt(process.env.EMAIL_RETRY_DAYS)   || 14;
const MAX_ATTEMPTS   = parseInt(process.env.EMAIL_MAX_ATTEMPTS) || 3;
const SMTP_TIMEOUT   = parseInt(process.env.SMTP_TIMEOUT_MS)    || 10000;

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── Pattern utilities ────────────────────────────────────────────────────────
// Canonical tokens we replace in learned patterns.
//   {first}  → john
//   {last}   → smith
//   {f}      → j
//   {l}      → s
function applyPattern(pattern, firstName, lastName, domain) {
  const f  = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l  = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!f || !l) return null;
  const local = pattern
    .replace(/\{first\}/g, f)
    .replace(/\{last\}/g,  l)
    .replace(/\{f\}/g,     f[0])
    .replace(/\{l\}/g,     l[0]);
  return `${local}@${domain}`;
}

// Infer the pattern from a known-good email (e.g. the one NMLS published).
function inferPattern(email, firstName, lastName) {
  if (!email || !email.includes('@')) return null;
  const [localRaw, domain] = email.toLowerCase().split('@');
  const f = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
  if (!f || !l) return null;

  // Substitute longest-first so {first} wins over {f} when both could match.
  // Using literal string replace() (single-pass) keeps substitutions from
  // re-matching the placeholder tokens themselves.
  let local = localRaw;
  const replaceOnce = (needle, token) => {
    const i = local.indexOf(needle);
    if (i === -1) return false;
    local = local.slice(0, i) + token + local.slice(i + needle.length);
    return true;
  };

  const firstSubbed = replaceOnce(f, '{first}') || replaceOnce(f[0], '{f}');
  const lastSubbed  = replaceOnce(l, '{last}')  || replaceOnce(l[0], '{l}');

  if (!firstSubbed && !lastSubbed) return null;              // role-based alias
  if (local === localRaw) return null;                        // no change (safety)

  // Require at least one full name token, otherwise we'd "learn" {f}{l} from
  // everyone which is too generic and would mislead pattern matching.
  if (!/\{first\}|\{last\}/.test(local)) return null;

  return { pattern: local, domain };
}

// Generic guesses in priority order. Used when no learned pattern exists.
const DEFAULT_PATTERNS = [
  '{first}.{last}',
  '{first}{last}',
  '{f}{last}',
  '{first}{l}',
  '{first}',
  '{f}.{last}',
  '{last}.{first}',
  '{last}{f}',
  '{first}-{last}',
  '{first}_{last}',
];

// ─── Domain guessing ──────────────────────────────────────────────────────────
// Returns an ordered list of candidate domains. We'll try each until one has
// MX records. Unlike the old version, this keeps industry words like
// "mortgage" / "lending" because removing them often makes the domain wrong
// (Guild Mortgage → guildmortgage.com, not guild.com).
function guessDomains(companyName) {
  if (!companyName) return [];

  // Strip pure suffixes/legal entities — NOT industry words.
  const base = companyName
    .toLowerCase()
    .replace(/\b(llc|l\.l\.c\.|inc|inc\.|corp|corporation|co\.|co|ltd|p\.c\.|pllc|plc|the)\b/g, '')
    .replace(/[&,.']/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!base) return [];

  const words = base.split(' ').filter(Boolean);
  const joined = words.join('');                         // guildmortgage
  const hyphen = words.join('-');                        // guild-mortgage
  const firstOnly = words[0];                            // guild
  const firstTwo  = words.slice(0, 2).join('');          // guildmortgage
  const tlds = ['com', 'net'];

  const candidates = new Set();
  for (const tld of tlds) {
    if (joined)    candidates.add(`${joined}.${tld}`);
    if (hyphen && hyphen !== joined) candidates.add(`${hyphen}.${tld}`);
    if (firstTwo && firstTwo !== joined) candidates.add(`${firstTwo}.${tld}`);
    if (firstOnly && firstOnly !== joined) candidates.add(`${firstOnly}.${tld}`);
  }
  return [...candidates];
}

// ─── MX cache (one DB row per domain) ─────────────────────────────────────────
async function getMxCached(domain) {
  const { rows } = await pool.query(
    'SELECT mx_host, has_mx, is_catch_all FROM domain_mx_cache WHERE domain = $1',
    [domain]
  );
  if (rows[0]) return rows[0];

  // Resolve + persist.
  let mxHost = null;
  let hasMx  = false;
  try {
    const mxRecords = await dns.resolveMx(domain);
    if (mxRecords && mxRecords.length) {
      mxHost = mxRecords.sort((a, b) => a.priority - b.priority)[0].exchange;
      hasMx  = true;
    }
  } catch { /* no MX — leave defaults */ }

  await pool.query(`
    INSERT INTO domain_mx_cache (domain, mx_host, has_mx, last_checked_at)
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT (domain) DO UPDATE SET
      mx_host = EXCLUDED.mx_host,
      has_mx = EXCLUDED.has_mx,
      last_checked_at = NOW()
  `, [domain, mxHost, hasMx]);

  return { mx_host: mxHost, has_mx: hasMx, is_catch_all: null };
}

async function markCatchAll(domain, isCatchAll) {
  await pool.query(`
    UPDATE domain_mx_cache
    SET is_catch_all = $2, last_checked_at = NOW()
    WHERE domain = $1
  `, [domain, isCatchAll]);
}

// ─── SMTP verification (shared socket flow) ───────────────────────────────────
function smtpCheck(mxHost, email) {
  return new Promise((resolve) => {
    const socket = net.createConnection(25, mxHost);
    let response = '';
    let step = 0;
    let resolved = false;

    const finish = (result) => {
      if (resolved) return;
      resolved = true;
      try { socket.destroy(); } catch { /* ignore */ }
      resolve(result);
    };

    socket.setTimeout(SMTP_TIMEOUT);
    socket.on('timeout', () => finish(null));
    socket.on('error',   () => finish(null));

    socket.on('data', (data) => {
      response += data.toString();

      if (step === 0 && response.includes('220')) {
        socket.write('EHLO mortgagedb.app\r\n');
        step = 1;
      } else if (step === 1 && /^250|^220/m.test(response)) {
        socket.write('MAIL FROM:<verify@mortgagedb.app>\r\n');
        step = 2;
      } else if (step === 2 && /^250/m.test(response)) {
        socket.write(`RCPT TO:<${email}>\r\n`);
        step = 3;
        response = ''; // start fresh for the RCPT answer
      } else if (step === 3 && response.length > 0) {
        const recent = response.slice(-300);
        if (/(^|\n)250\b/.test(recent))                            finish(true);
        else if (/(^|\n)(550|551|552|553|554|450|451)\b/.test(recent)) finish(false);
        else if (response.length > 1000)                           finish(null);
      }
    });
  });
}

async function verifyEmail(email) {
  const domain = email.split('@')[1];
  if (!domain) return false;
  const mx = await getMxCached(domain);
  if (!mx.has_mx || !mx.mx_host) return false;
  return smtpCheck(mx.mx_host, email);
}

// ─── Catch-all detection ──────────────────────────────────────────────────────
// Send a random-local-part RCPT; if the server accepts, the domain accepts
// anything and per-address verification is useless here.
async function detectCatchAll(domain) {
  const mx = await getMxCached(domain);
  if (!mx.has_mx) return false;
  if (mx.is_catch_all !== null && mx.is_catch_all !== undefined) return mx.is_catch_all;

  const noise = crypto.randomBytes(12).toString('hex');
  const result = await smtpCheck(mx.mx_host, `${noise}@${domain}`);
  const isCatchAll = result === true;
  await markCatchAll(domain, isCatchAll);
  return isCatchAll;
}

// ─── Pattern store (company_email_patterns) ──────────────────────────────────
const patternCache = new Map(); // domain → { pattern, confidence, is_catch_all }

async function loadPatternsFromDb() {
  const { rows } = await pool.query(
    'SELECT domain, pattern, confidence, is_catch_all FROM company_email_patterns'
  );
  for (const r of rows) patternCache.set(r.domain, r);
}

async function recordPatternSuccess(domain, pattern) {
  const existing = patternCache.get(domain);
  if (existing && existing.pattern === pattern) {
    await pool.query(`
      UPDATE company_email_patterns
      SET confidence   = LEAST(confidence + 10, 100),
          sample_count = sample_count + 1,
          last_seen_at = NOW()
      WHERE domain = $1
    `, [domain]);
    existing.confidence = Math.min(existing.confidence + 10, 100);
    existing.sample_count = (existing.sample_count || 0) + 1;
  } else {
    await pool.query(`
      INSERT INTO company_email_patterns (domain, pattern, confidence, sample_count, last_seen_at)
      VALUES ($1, $2, 40, 1, NOW())
      ON CONFLICT (domain) DO UPDATE SET
        pattern      = EXCLUDED.pattern,
        confidence   = LEAST(company_email_patterns.confidence + 10, 100),
        sample_count = company_email_patterns.sample_count + 1,
        last_seen_at = NOW()
    `, [domain, pattern]);
    patternCache.set(domain, { domain, pattern, confidence: 40, is_catch_all: false });
  }
}

// ─── Seed from NMLS email on the same person ──────────────────────────────────
async function seedPatternFromNmlsEmail(person) {
  if (!person.email || !person.first_name || !person.last_name) return;
  const inferred = inferPattern(person.email, person.first_name, person.last_name);
  if (!inferred) return;
  if (!patternCache.has(inferred.domain)) {
    await recordPatternSuccess(inferred.domain, inferred.pattern);
  }
}

// ─── Attempt tracking writes ──────────────────────────────────────────────────
async function markAttempt(personId) {
  await pool.query(`
    UPDATE people
    SET email_attempted_at = NOW(),
        email_attempts     = COALESCE(email_attempts, 0) + 1,
        updated_at         = NOW()
    WHERE id = $1
  `, [personId]);
}

async function markVerified(personId, email, pattern) {
  await pool.query(`
    UPDATE people SET
      verified_email      = $1,
      email_verified      = true,
      email_verified_at   = NOW(),
      email_pattern       = $2,
      data_quality_score  = LEAST(data_quality_score + 20, 100),
      email_attempted_at  = NOW(),
      email_attempts      = COALESCE(email_attempts, 0) + 1,
      updated_at          = NOW()
    WHERE id = $3
  `, [email, pattern, personId]);
}

// ─── Per-person enrichment ────────────────────────────────────────────────────
async function enrichPersonEmail(person) {
  if (!person.first_name || !person.last_name) return null;

  // 1. Seed the domain pattern cache from any existing NMLS email on this row.
  await seedPatternFromNmlsEmail(person);

  // 2. Resolve candidate domains.
  const candidateDomains = [];
  if (person.company_nmls_id) {
    const { rows } = await pool.query(
      'SELECT website FROM companies WHERE nmls_id = $1',
      [person.company_nmls_id]
    );
    const site = rows[0]?.website;
    if (site) {
      const d = site.replace(/https?:\/\//, '').replace(/\/.*/, '').toLowerCase();
      if (d) candidateDomains.push(d);
    }
  }
  for (const d of guessDomains(person.company_name)) {
    if (!candidateDomains.includes(d)) candidateDomains.push(d);
  }
  if (!candidateDomains.length) return null;

  // 3. Pick the first domain that has MX records.
  let workingDomain = null;
  for (const d of candidateDomains) {
    const mx = await getMxCached(d);
    if (mx.has_mx) { workingDomain = d; break; }
  }
  if (!workingDomain) return null;

  // 4. Build attempt list: learned pattern first (if any), then defaults.
  const attempted = new Set();
  const ordered = [];
  const learned = patternCache.get(workingDomain);
  if (learned?.pattern) ordered.push(learned.pattern);
  for (const p of DEFAULT_PATTERNS) if (!ordered.includes(p)) ordered.push(p);

  // 5. Catch-all check gate — if this domain is catch-all, we can't trust
  //    SMTP, so only rely on an already-learned pattern with confidence ≥ 60.
  const catchAll = await detectCatchAll(workingDomain);
  if (catchAll) {
    if (learned && learned.pattern && (learned.confidence || 0) >= 60) {
      const email = applyPattern(learned.pattern, person.first_name, person.last_name, workingDomain);
      if (email) {
        await markVerified(person.id, email, learned.pattern);
        return email;
      }
    }
    return null;
  }

  // 6. Try each pattern.
  for (const pattern of ordered) {
    const email = applyPattern(pattern, person.first_name, person.last_name, workingDomain);
    if (!email || attempted.has(email)) continue;
    attempted.add(email);

    await sleep(DELAY);
    const result = await verifyEmail(email);
    if (result === true) {
      await markVerified(person.id, email, pattern);
      await recordPatternSuccess(workingDomain, pattern);
      return email;
    }
    // result === false → definite reject, keep trying
    // result === null  → soft fail (server didn't answer), move on
  }

  return null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log('📧 Email Enricher starting...');
  await pool.query('SELECT 1');
  console.log('✅ DB connected');

  await loadPatternsFromDb();
  console.log(`   Loaded ${patternCache.size} known company email patterns\n`);

  const { rows: people } = await pool.query(`
    SELECT id, first_name, last_name, company_name, company_nmls_id, email
    FROM people
    WHERE email_verified = false
      AND first_name IS NOT NULL
      AND last_name IS NOT NULL
      AND company_name IS NOT NULL
      AND LENGTH(first_name) > 1
      AND LENGTH(last_name) > 1
      AND COALESCE(email_attempts, 0) < $1
      AND (email_attempted_at IS NULL OR email_attempted_at < NOW() - ($2 || ' days')::INTERVAL)
    ORDER BY data_quality_score DESC, email_attempts ASC NULLS FIRST
    LIMIT $3
  `, [MAX_ATTEMPTS, String(RETRY_DAYS), BATCH_LIMIT]);

  console.log(`Processing ${people.length} people (max ${MAX_ATTEMPTS} attempts, retry after ${RETRY_DAYS}d)...\n`);
  let verified = 0, failed = 0;

  for (const person of people) {
    let email = null;
    try {
      email = await enrichPersonEmail(person);
    } catch (err) {
      console.error(`  ✗ ${person.first_name} ${person.last_name}: ${err.message}`);
    }

    if (email) {
      verified++;
      console.log(`  ✓ ${person.first_name} ${person.last_name} → ${email}`);
    } else {
      failed++;
      await markAttempt(person.id);
    }

    if ((verified + failed) % 50 === 0) {
      console.log(`\n  Progress: ${verified} verified, ${failed} not found\n`);
    }
  }

  console.log(`\n✅ Done: ${verified} emails verified, ${failed} not found`);
  process.exit(0);
}

if (require.main === module) {
  run().catch(err => { console.error('Fatal:', err); process.exit(1); });
}

module.exports = {
  guessDomains,
  inferPattern,
  applyPattern,
  DEFAULT_PATTERNS,
};
