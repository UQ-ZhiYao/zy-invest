# ZY-Invest — Setup Guide  v1.0.0

## Overview
This document walks you through setting up the entire ZY-Invest system
from scratch: database, backend API, and frontend on GitHub Pages.

---

## STEP 1 — Create Supabase project (database)

1. Go to https://supabase.com → Sign up (free)
2. Click "New project" → Name: `zy-invest` → Choose a strong DB password → Region: Singapore
3. Wait ~2 minutes for project to provision
4. Go to **SQL Editor** (left sidebar)
5. Run these 4 files IN ORDER — copy/paste each one:
   - `database/01_schema.sql`
   - `database/02_views.sql`
   - `database/03_rls.sql`
   - `database/04_seed.sql`
6. After running 04_seed.sql, go to **Table Editor** → `users` table
   and add your admin user manually (or use the SQL below):

```sql
-- First create the investor record
INSERT INTO investors (name, joined_date)
VALUES ('ZY Admin', '2021-12-13')
RETURNING id;  -- copy this UUID

-- Then create the user (replace UUID and hash)
-- Generate hash: python -c "import bcrypt; print(bcrypt.hashpw(b'YourPassword123!', bcrypt.gensalt()).decode())"
INSERT INTO users (name, email, password_hash, role, investor_id)
VALUES (
  'ZY Admin',
  'admin@zy-invest.com',
  '$2b$12$YOUR_BCRYPT_HASH_HERE',
  'admin',
  'PASTE-INVESTOR-UUID-HERE'
);
```

7. Go to **Settings → Database** → copy the "Connection String (URI)" — you will need this later

---

## STEP 2 — Create Supabase Storage bucket (documents)

1. In Supabase → **Storage** → New bucket
2. Name: `zy-invest-docs`
3. Public: NO (private bucket)
4. Go to **Settings → API** → copy your `service_role` key (keep this secret)

---

## STEP 3 — Deploy backend to Render.com

1. Go to https://render.com → Sign up (free)
2. Click "New" → "Web Service"
3. Connect your GitHub account → select the `zy-invest` repo
4. Configure:
   - **Name:** `zy-invest-api`
   - **Root directory:** `backend`
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Plan:** Free
5. Add environment variables (from your `.env.example`):
   - `DATABASE_URL` → paste Supabase connection string
   - `JWT_SECRET` → generate: `python -c "import secrets; print(secrets.token_hex(32))"`
   - `FRONTEND_URL` → `https://yourusername.github.io/zy-invest`
   - `SUPABASE_URL` → from Supabase Settings → API
   - `SUPABASE_SERVICE_KEY` → from Supabase Settings → API
   - `ENV` → `production`
   - `TZ` → `Asia/Kuala_Lumpur`
6. Click "Create Web Service"
7. Wait for deployment → you will get a URL like: `https://zy-invest-api.onrender.com`
8. Test: visit `https://zy-invest-api.onrender.com/health` → should return `{"status":"healthy"}`

**Note on Render free tier:** The service sleeps after 15 minutes of inactivity.
First request after sleep takes ~30 seconds (cold start). This is acceptable
for a private fund with known users. Upgrade to Starter ($7/month) to eliminate cold starts.

---

## STEP 4 — Configure GitHub Pages

1. Push the entire `zy-invest/` folder to a GitHub repository
2. Go to repo → **Settings → Pages**
3. Source: `Deploy from a branch` → Branch: `main` → Folder: `/frontend`
4. Save → your site will be live at: `https://yourusername.github.io/zy-invest`

### Update the API URL in the frontend

Edit `frontend/assets/js/api.js` and set:
```javascript
const API_BASE = 'https://zy-invest-api.onrender.com';
```

---

## STEP 5 — Import your Excel data

1. Log in to the website with your admin account
2. Go to **Admin → Upload Excel**
3. Upload your `Portfolio_Dashboard.xlsm`
4. The system will import:
   - Historical NTA (all locked as read-only)
   - Investor names and balances
   - Transactions, settlement, dividends, distributions, others
5. After import, go to **Admin → Account Management**
6. For each investor, create a user account (email + temporary password)
7. Link each user to their investor profile

---

## STEP 6 — Set up fee schedules

1. Go to **Admin → Fee Schedule**
2. Add your base fee: e.g. 1% p.a. from 2021-12-13
3. Add your performance fee: e.g. 20% above 8% hurdle from 2021-12-13

---

## STEP 7 — Verify ticker mappings

1. Go to **Admin → Ticker Mapping**
2. Check all instruments have correct Yahoo Finance tickers
3. For warrants and OTC instruments, ensure `is_manual = TRUE`
4. Trigger a test price fetch: **Admin → Prices → Fetch Now**
5. For any missing prices, use **Admin → Prices → Override** to set manually

---

## Daily operations (automated)

The backend scheduler runs automatically at **6:00 PM MYT** on trading days:
1. Fetches closing prices from Yahoo Finance
2. Computes daily NTA
3. Updates all investor market values and IRR

You can also trigger these manually from the Admin portal at any time.

---

## File structure reference

```
zy-invest/
├── frontend/           → GitHub Pages (deployed as static site)
│   ├── index.html
│   ├── team.html
│   ├── about.html
│   ├── login.html
│   ├── dashboard/
│   │   ├── index.html          (Overview Dashboard)
│   │   ├── account-summary.html
│   │   ├── distributions.html
│   │   ├── transactions.html
│   │   ├── statement.html
│   │   ├── performance.html
│   │   ├── analysis.html
│   │   ├── documents.html
│   │   └── admin/
│   │       ├── index.html
│   │       ├── fee-schedule.html
│   │       ├── ticker-map.html
│   │       ├── price-override.html
│   │       ├── transactions.html
│   │       ├── settlement.html
│   │       ├── dividends.html
│   │       ├── distributions.html
│   │       ├── others.html
│   │       ├── holdings.html
│   │       ├── investors.html
│   │       ├── users.html
│   │       ├── documents.html
│   │       └── upload.html
│   └── assets/
│       ├── css/
│       │   ├── main.css
│       │   ├── dashboard.css
│       │   └── components.css
│       ├── js/
│       │   ├── api.js
│       │   ├── auth.js
│       │   └── charts.js
│       └── img/
│           └── logo.png
├── backend/            → FastAPI (deployed to Render.com)
│   ├── main.py
│   ├── database.py
│   ├── requirements.txt
│   ├── render.yaml
│   ├── .env.example
│   ├── routers/
│   │   ├── auth.py
│   │   ├── public.py
│   │   ├── member.py
│   │   └── admin.py
│   └── services/
│       ├── irr.py
│       ├── nta_engine.py
│       ├── price_fetcher.py
│       └── excel_parser.py
└── database/           → SQL scripts (run in Supabase)
    ├── 01_schema.sql
    ├── 02_views.sql
    ├── 03_rls.sql
    └── 04_seed.sql
```

---

## Version history

| Version | Date       | Changes                    |
|---------|------------|----------------------------|
| v1.0.0  | 2026-03-26 | Initial backend + DB schema |

Next: v1.1.0 — External pages (Home, Team, About, Login)
