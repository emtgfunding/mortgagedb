-- ============================================================
-- MortgageDB - The Mortgage Industry Contact Database
-- Combines NMLS + LinkedIn + Email Verification
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- CORE PEOPLE TABLE
CREATE TABLE IF NOT EXISTS people (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  first_name            VARCHAR(100),
  last_name             VARCHAR(100),
  full_name             VARCHAR(200),
  nmls_id               VARCHAR(20) UNIQUE,
  license_status        VARCHAR(50),
  license_type          VARCHAR(100),
  regulatory_actions    BOOLEAN DEFAULT FALSE,
  reg_action_detail     TEXT,
  company_name          VARCHAR(200),
  company_nmls_id       VARCHAR(20),
  branch_nmls_id        VARCHAR(20),
  phone                 VARCHAR(30),
  email                 VARCHAR(200),
  address               VARCHAR(300),
  city                  VARCHAR(100),
  state                 VARCHAR(10),
  zip                   VARCHAR(20),
  verified_email        VARCHAR(200),
  email_verified        BOOLEAN DEFAULT FALSE,
  email_verified_at     TIMESTAMP,
  email_pattern         VARCHAR(100),
  linkedin_url          VARCHAR(500),
  linkedin_id           VARCHAR(100),
  photo_url             VARCHAR(500),
  headline              VARCHAR(500),
  summary               TEXT,
  title                 VARCHAR(200),
  title_category        VARCHAR(50),
  vol_12mo_usd          BIGINT,
  vol_12mo_units        INTEGER,
  vol_ytd_usd           BIGINT,
  vol_ytd_units         INTEGER,
  production_tier       VARCHAR(20),
  production_updated_at TIMESTAMP,
  source_nmls           BOOLEAN DEFAULT FALSE,
  source_linkedin       BOOLEAN DEFAULT FALSE,
  source_web            BOOLEAN DEFAULT FALSE,
  source_deed           BOOLEAN DEFAULT FALSE,
  data_quality_score    INTEGER DEFAULT 0,
  created_at            TIMESTAMP DEFAULT NOW(),
  updated_at            TIMESTAMP DEFAULT NOW(),
  nmls_last_synced      TIMESTAMP,
  linkedin_last_synced  TIMESTAMP,
  linkedin_attempted_at TIMESTAMP,
  linkedin_attempts     INTEGER DEFAULT 0,
  email_attempted_at    TIMESTAMP,
  email_attempts        INTEGER DEFAULT 0
);

-- Idempotent ALTERs so existing DBs pick up new columns without recreation
ALTER TABLE people ADD COLUMN IF NOT EXISTS linkedin_attempted_at TIMESTAMP;
ALTER TABLE people ADD COLUMN IF NOT EXISTS linkedin_attempts     INTEGER DEFAULT 0;
ALTER TABLE people ADD COLUMN IF NOT EXISTS email_attempted_at    TIMESTAMP;
ALTER TABLE people ADD COLUMN IF NOT EXISTS email_attempts        INTEGER DEFAULT 0;

-- STATE LICENSES
CREATE TABLE IF NOT EXISTS licenses (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id       UUID REFERENCES people(id) ON DELETE CASCADE,
  state           VARCHAR(10) NOT NULL,
  license_number  VARCHAR(50),
  license_type    VARCHAR(100),
  status          VARCHAR(50),
  regulator       VARCHAR(200),
  issued_date     DATE,
  expires_date    DATE,
  created_at      TIMESTAMP DEFAULT NOW(),
  updated_at      TIMESTAMP DEFAULT NOW(),
  UNIQUE(person_id, state, license_type)
);

-- COMPANIES
CREATE TABLE IF NOT EXISTS companies (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  nmls_id         VARCHAR(20) UNIQUE,
  name            VARCHAR(300) NOT NULL,
  trade_name      VARCHAR(300),
  license_status  VARCHAR(50),
  company_type    VARCHAR(100),
  phone           VARCHAR(30),
  email           VARCHAR(200),
  website         VARCHAR(500),
  address         VARCHAR(300),
  city            VARCHAR(100),
  state           VARCHAR(10),
  zip             VARCHAR(20),
  linkedin_url    VARCHAR(500),
  logo_url        VARCHAR(500),
  description     TEXT,
  employee_count  INTEGER,
  lo_count        INTEGER,
  active_states   TEXT[],
  created_at      TIMESTAMP DEFAULT NOW(),
  updated_at      TIMESTAMP DEFAULT NOW()
);

-- EMPLOYMENT HISTORY
CREATE TABLE IF NOT EXISTS employment_history (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id       UUID REFERENCES people(id) ON DELETE CASCADE,
  company_nmls_id VARCHAR(20),
  company_name    VARCHAR(200),
  start_date      DATE,
  end_date        DATE,
  is_current      BOOLEAN DEFAULT FALSE,
  created_at      TIMESTAMP DEFAULT NOW()
);

-- COMPANY EMAIL PATTERNS (learned per-domain, reused across employees)
CREATE TABLE IF NOT EXISTS company_email_patterns (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  domain          VARCHAR(200) UNIQUE NOT NULL,
  pattern         VARCHAR(100),          -- e.g. "{first}.{last}", "{f}{last}"
  confidence      INTEGER DEFAULT 0,     -- 0-100, increases with confirmations
  sample_count    INTEGER DEFAULT 0,     -- how many verified emails produced this pattern
  is_catch_all    BOOLEAN DEFAULT FALSE, -- domain accepts any recipient (SMTP verify unreliable)
  catch_all_checked_at TIMESTAMP,
  last_seen_at    TIMESTAMP DEFAULT NOW(),
  created_at      TIMESTAMP DEFAULT NOW(),
  updated_at      TIMESTAMP DEFAULT NOW()
);

-- DOMAIN MX CACHE (avoid re-resolving MX + store catch-all decision)
CREATE TABLE IF NOT EXISTS domain_mx_cache (
  domain          VARCHAR(200) PRIMARY KEY,
  mx_host         VARCHAR(300),
  has_mx          BOOLEAN DEFAULT FALSE,
  is_catch_all    BOOLEAN,               -- NULL = unknown
  last_checked_at TIMESTAMP DEFAULT NOW()
);

-- SAVED LISTS (recruiter-curated segments)
CREATE TABLE IF NOT EXISTS saved_lists (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name          VARCHAR(200) NOT NULL,
  description   TEXT,
  color         VARCHAR(20),
  created_at    TIMESTAMP DEFAULT NOW(),
  updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS saved_list_members (
  list_id       UUID REFERENCES saved_lists(id) ON DELETE CASCADE,
  person_id     UUID REFERENCES people(id) ON DELETE CASCADE,
  added_at      TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (list_id, person_id)
);

-- OUTREACH TRACKING (one row per person, upserted on edit)
-- status values: new | queued | contacted | replied | interviewing | hired | not_interested | do_not_contact
CREATE TABLE IF NOT EXISTS outreach (
  person_id     UUID PRIMARY KEY REFERENCES people(id) ON DELETE CASCADE,
  status        VARCHAR(40) DEFAULT 'new',
  note          TEXT,
  last_contacted_at TIMESTAMP,
  updated_at    TIMESTAMP DEFAULT NOW()
);

-- INGEST JOBS
CREATE TABLE IF NOT EXISTS ingest_jobs (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  job_type        VARCHAR(50),
  target          VARCHAR(100),
  status          VARCHAR(20) DEFAULT 'pending',
  records_found   INTEGER DEFAULT 0,
  records_added   INTEGER DEFAULT 0,
  records_updated INTEGER DEFAULT 0,
  error_message   TEXT,
  started_at      TIMESTAMP,
  completed_at    TIMESTAMP,
  created_at      TIMESTAMP DEFAULT NOW()
);

-- INDEXES
CREATE INDEX IF NOT EXISTS idx_people_name_trgm ON people USING gin(full_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_people_nmls_id ON people(nmls_id);
CREATE INDEX IF NOT EXISTS idx_people_company_nmls ON people(company_nmls_id);
CREATE INDEX IF NOT EXISTS idx_people_state ON people(state);
CREATE INDEX IF NOT EXISTS idx_people_city ON people(city);
CREATE INDEX IF NOT EXISTS idx_people_title_category ON people(title_category);
CREATE INDEX IF NOT EXISTS idx_people_license_status ON people(license_status);
CREATE INDEX IF NOT EXISTS idx_people_production_tier ON people(production_tier);
CREATE INDEX IF NOT EXISTS idx_people_email ON people(email);
CREATE INDEX IF NOT EXISTS idx_companies_nmls ON companies(nmls_id);
CREATE INDEX IF NOT EXISTS idx_companies_name_trgm ON companies USING gin(name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_companies_state ON companies(state);
CREATE INDEX IF NOT EXISTS idx_licenses_person ON licenses(person_id);
CREATE INDEX IF NOT EXISTS idx_licenses_state ON licenses(state);
CREATE INDEX IF NOT EXISTS idx_ingest_status ON ingest_jobs(status);
CREATE INDEX IF NOT EXISTS idx_people_linkedin_attempt ON people(linkedin_attempted_at);
CREATE INDEX IF NOT EXISTS idx_people_email_attempt ON people(email_attempted_at);
CREATE INDEX IF NOT EXISTS idx_company_email_patterns_domain ON company_email_patterns(domain);
CREATE INDEX IF NOT EXISTS idx_saved_list_members_person ON saved_list_members(person_id);
CREATE INDEX IF NOT EXISTS idx_outreach_status ON outreach(status);
CREATE INDEX IF NOT EXISTS idx_outreach_last_contacted ON outreach(last_contacted_at DESC);

-- AUTO UPDATE TIMESTAMPS
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS people_updated_at ON people;
CREATE TRIGGER people_updated_at BEFORE UPDATE ON people
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS companies_updated_at ON companies;
CREATE TRIGGER companies_updated_at BEFORE UPDATE ON companies
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS company_email_patterns_updated_at ON company_email_patterns;
CREATE TRIGGER company_email_patterns_updated_at BEFORE UPDATE ON company_email_patterns
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS saved_lists_updated_at ON saved_lists;
CREATE TRIGGER saved_lists_updated_at BEFORE UPDATE ON saved_lists
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS outreach_updated_at ON outreach;
CREATE TRIGGER outreach_updated_at BEFORE UPDATE ON outreach
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
