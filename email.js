/**
 * Email Enricher
 * 
 * Generates likely email addresses based on name + company domain,
 * then verifies them via SMTP without sending an actual email.
 * This is 100% legal - same technique all major data companies use.
 * 
 * Usage: node enricher/email.js
 */

require('dotenv').config();
const dns = require('dns').promises;
const net = require('net');
const pool = require('../db');

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── Email pattern generators ──────────────────────────────────────────────────
function generatePatterns(firstName, lastName, domain) {
  const f = firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l = lastName.toLowerCase().replace(/[^a-z]/g, '');
  const fi = f[0] || '';
  const li = l[0] || '';

  return [
    `${f}.${l}@${domain}`,           // john.smith@
    `${f}${l}@${domain}`,            // johnsmith@
    `${fi}${l}@${domain}`,           // jsmith@
    `${f}${li}@${domain}`,           // johns@
    `${f}@${domain}`,                // john@
    `${fi}.${l}@${domain}`,          // j.smith@
    `${l}.${f}@${domain}`,           // smith.john@
    `${l}${fi}@${domain}`,           // smithj@
    `${f}-${l}@${domain}`,           // john-smith@
    `${f}_${l}@${domain}`,           // john_smith@
  ];
}

// ─── Get company domain from name ─────────────────────────────────────────────
function guessDomain(companyName) {
  if (!companyName) return null;
  
  // Clean company name
  const clean = companyName
    .toLowerCase()
    .replace(/\b(llc|inc|corp|co|ltd|mortgage|lending|financial|home|loans|bank|federal|national|american|united|first|premier)\b/g, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
  
  if (!clean || clean.length < 2) return null;
  return `${clean}.com`;
}

// ─── SMTP verification (check if email exists without sending) ────────────────
async function verifyEmailSMTP(email) {
  const domain = email.split('@')[1];
  if (!domain) return false;

  try {
    // Step 1: Get MX records
    const mxRecords = await dns.resolveMx(domain);
    if (!mxRecords || mxRecords.length === 0) return false;

    // Sort by priority, get primary mail server
    const mx = mxRecords.sort((a, b) => a.priority - b.priority)[0].exchange;

    // Step 2: SMTP handshake to verify mailbox
    return new Promise((resolve) => {
      const socket = net.createConnection(25, mx);
      let response = '';
      let step = 0;
      let resolved = false;

      const finish = (result) => {
        if (!resolved) {
          resolved = true;
          socket.destroy();
          resolve(result);
        }
      };

      socket.setTimeout(10000);
      socket.on('timeout', () => finish(false));
      socket.on('error', () => finish(false));

      socket.on('data', (data) => {
        response += data.toString();

        if (step === 0 && response.includes('220')) {
          // Server ready - send EHLO
          socket.write(`EHLO mortgagedb.app\r\n`);
          step = 1;
        } else if (step === 1 && (response.includes('250') || response.includes('220'))) {
          // EHLO accepted - send MAIL FROM
          socket.write(`MAIL FROM:<verify@mortgagedb.app>\r\n`);
          step = 2;
        } else if (step === 2 && response.includes('250')) {
          // MAIL FROM accepted - check RCPT TO
          socket.write(`RCPT TO:<${email}>\r\n`);
          step = 3;
        } else if (step === 3) {
          // 250 = exists, 550/551/553 = doesn't exist
          if (response.match(/^250/m)) {
            finish(true);
          } else if (response.match(/^(550|551|552|553|450|451)/m)) {
            finish(false);
          } else {
            // 421, 452, etc - can't verify, assume valid
            finish(null);
          }
        }
      });
    });
  } catch {
    return null; // Can't verify, not confirmed invalid
  }
}

// ─── Enrich a single person's email ──────────────────────────────────────────
async function enrichPersonEmail(person) {
  if (!person.first_name || !person.last_name) return null;

  // Get company domain
  let domain = null;

  // Try to get from company website in DB
  if (person.company_nmls_id) {
    const { rows } = await pool.query(
      'SELECT website FROM companies WHERE nmls_id = $1',
      [person.company_nmls_id]
    );
    if (rows[0]?.website) {
      domain = rows[0].website.replace(/https?:\/\//,'').replace(/\/.*/,'');
    }
  }

  if (!domain) domain = guessDomain(person.company_name);
  if (!domain) return null;

  const patterns = generatePatterns(person.first_name, person.last_name, domain);
  
  for (const email of patterns) {
    await sleep(500); // Don't hammer SMTP servers
    try {
      const valid = await verifyEmailSMTP(email);
      if (valid === true) {
        // Found a verified email!
        await pool.query(`
          UPDATE people SET
            verified_email = $1,
            email_verified = true,
            email_verified_at = NOW(),
            email_pattern = $2,
            data_quality_score = LEAST(data_quality_score + 20, 100),
            updated_at = NOW()
          WHERE id = $3
        `, [email, email.split('@')[0].replace(person.first_name.toLowerCase(), '{first}')
                                     .replace(person.last_name.toLowerCase(), '{last}'),
            person.id]);
        return email;
      }
    } catch {
      continue;
    }
  }

  return null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log('📧 Email Enricher starting...');
  
  await pool.query('SELECT 1');
  console.log('✅ DB connected');

  // Get people without verified emails, prioritize those with company info
  const { rows: people } = await pool.query(`
    SELECT id, first_name, last_name, company_name, company_nmls_id, email
    FROM people
    WHERE email_verified = false
      AND first_name IS NOT NULL
      AND last_name IS NOT NULL
      AND company_name IS NOT NULL
      AND LENGTH(first_name) > 1
      AND LENGTH(last_name) > 1
    ORDER BY data_quality_score DESC
    LIMIT 1000
  `);

  console.log(`\nProcessing ${people.length} people...\n`);
  let verified = 0, failed = 0;

  for (const person of people) {
    const email = await enrichPersonEmail(person);
    if (email) {
      verified++;
      console.log(`  ✓ ${person.first_name} ${person.last_name} → ${email}`);
    } else {
      failed++;
    }

    if ((verified + failed) % 50 === 0) {
      console.log(`\n  Progress: ${verified} verified, ${failed} not found\n`);
    }
  }

  console.log(`\n✅ Done: ${verified} emails verified, ${failed} not found`);
  process.exit(0);
}

run().catch(err => { console.error('Fatal:', err); process.exit(1); });
