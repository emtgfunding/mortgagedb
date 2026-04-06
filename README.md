# MortgageDB

The only mortgage industry contact database combining NMLS licensing data + LinkedIn enrichment + email verification. A competitor to Apollo + MMI built specifically for the mortgage vertical.

## What it does

- **Ingests every licensed mortgage professional** from NMLS Consumer Access (~500K+ people)
- **Enriches with LinkedIn** profile data (photo, bio, current role)
- **Verifies emails** via SMTP pattern detection (no emails sent)
- **Discovers non-LO roles** — HR, IT, Sales Trainers, Operations at mortgage companies
- **Exposes a search API** with filters by state, role, company, license status
- **Syncs nightly** to keep data fresh

## Data Sources

| Source | Data | Cost |
|--------|------|------|
| NMLS Consumer Access | Name, NMLS ID, company, phone, email, license status, licensed states | Free (public) |
| LinkedIn (public pages) | Photo, bio, title, LinkedIn URL | Free (public) |
| SMTP verification | Verified email addresses | Free |
| Google search | LinkedIn URL discovery | Free |

## Setup

### 1. Railway Postgres
1. Go to [railway.app](https://railway.app)
2. New Project → Add PostgreSQL
3. Copy the `DATABASE_URL` from the Variables tab

### 2. Deploy to Railway
```bash
# Clone and push to Railway
git init
git add .
git commit -m "initial"
railway login
railway link
railway up
```

### 3. Set environment variables in Railway
```
DATABASE_URL=<from Railway Postgres>
PORT=3000
NODE_ENV=production
```

### 4. Initialize database
The schema is auto-created on first run.

### 5. Start ingestion
```bash
# In Railway console or locally:
npm run ingest:states    # Start with your key states
# or
npm run pipeline         # Full pipeline (takes hours for all 50 states)
```

## API Endpoints

```
GET /health                          — Status + record count
GET /api/stats                       — Database statistics
GET /api/people?q=John&state=MI&role=loan_officer&page=1
GET /api/people/:nmls_id             — Single person
GET /api/companies?state=MI          — Company search
GET /api/export/csv?state=MI&role=hr — CSV download
```

### People search filters
| Param | Description | Example |
|-------|-------------|---------|
| q | Name or company search | `q=John Smith` |
| state | State of office | `state=MI` |
| role | Role type | `role=loan_officer` |
| licensed_state | Licensed to originate in state | `licensed_state=FL` |
| company | Company name | `company=Rocket` |
| has_email | Only with email | `has_email=true` |
| has_linkedin | Only with LinkedIn | `has_linkedin=true` |
| status | License status | `status=Active` |

### Role types
- `loan_officer` — Licensed MLOs
- `processor` — Loan processors
- `underwriter` — Underwriters
- `branch_manager` — Branch/regional managers
- `hr` — Human Resources
- `it` — Information Technology
- `sales_trainer` — Sales trainers / L&D
- `ops` — Operations
- `marketing` — Marketing
- `compliance` — Compliance
- `executive` — C-suite / executives
- `other` — Other mortgage industry roles

## Architecture

```
Railway Postgres
    ↑
NMLS Ingester (nightly)     → people table
LinkedIn Enricher (6hr)     → linkedin_url, photo, bio
Email Verifier (4hr)        → verified_email
Non-LO Discoverer (weekly)  → HR, IT, Trainers
    ↓
Express API → Your frontend / CRM / export
```

## Roadmap

- [ ] Phase 1: NMLS ingestion (this)
- [ ] Phase 2: LinkedIn + email enrichment (this)
- [ ] Phase 3: County deed data for LO production volume (MI, OH, IN first)
- [ ] Phase 4: Frontend search UI
- [ ] Phase 5: CRM export integrations (Salesforce, HubSpot)
- [ ] Phase 6: Productize + pricing tiers
