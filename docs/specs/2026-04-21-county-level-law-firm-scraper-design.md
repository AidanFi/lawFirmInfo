# County-Level Law Firm Data Scraper — Design Spec

## Context

The existing statewide scraper produces a broad dataset of Kansas law firms for the main directory. The user needs a separate, much more detailed county-by-county pipeline that produces downloadable CSV files with complete data for every law firm in a given county — including Google Business Profile URLs, legal directory listings, and priority scoring. The first target is Johnson County, KS.

This system is entirely separate from the main scraper and data file. The main `app/firms_data.js` and `app/index.html` core functionality are not modified.

---

## Data Pipeline (3 Stages)

### Stage 1: Google Places API (Discovery)

Search for law firms in each city within the target county. Query variants: "law firm", "attorney", "lawyer" per city to maximize coverage past the 60-result-per-query limit.

**Extracts:** firm name, address (street, city, state, zip), phone, website, coordinates, Google Business Profile URL (constructed from `place_id`).

**Cost:** ~$15-30 per large county. Likely covered by Google's $200/mo free credit. API key already in `scraper/.env`.

### Stage 2: Foursquare Places API (Backup Discovery)

Independent second pass using Foursquare's business database. Searches same city list with category filter for legal services (category 12100).

**Extracts:** firm name, address, phone, website, coordinates.

**Cost:** Free (1,000 calls/day free tier). API key provided by user, stored in `scraper/.env` as `FOURSQUARE_API_KEY`.

### Stage 3: Enhancement Pass (Free Sources)

Enriches the merged discovery results and cross-checks for completeness:

1. **KS Courts cross-check** — Load existing `firms_data.js`, filter to county cities, flag any firms not already discovered.
2. **Martindale** — Get legal directory listing URLs, enrich practice areas. Priority 1 for `legal_directory_listing` field.
3. **Justia** — Get directory listing URLs, practice areas, phone, website.
4. **Avvo** — Get directory listing URLs.
5. **FindLaw** — Get directory listing URLs.
6. **Website scraping** — Scrape firm websites for email addresses and practice area confirmation.
7. **Legal directory URL selection** — Pick best URL: Martindale > Justia > Avvo > FindLaw.

### Deduplication

Runs twice during the pipeline using existing `normalize.py`:
- After merging Google + Foursquare results (before enhancement)
- After enhancement pass adds any new firms from KS Courts cross-check

Multi-pass strategy: exact name match → fuzzy match (rapidfuzz, 88% threshold) → domain/phone dedup → validation log.

---

## CSV Output

### Columns (14 fields)

| Column | Source | Notes |
|--------|--------|-------|
| law_firm_name | All sources | Primary identifier |
| website | Google Places / Foursquare / directories | Firm website URL |
| google_business_profile | Google Places API | URL from place_id |
| legal_directory_listing | Martindale > Justia > Avvo > FindLaw | Best single URL |
| city | Address parsing | City within the county |
| state | Fixed per county | "KS" for Kansas counties |
| county | Fixed per run | "Johnson" for first run |
| phone_number | Google Places / Foursquare / directories | Primary phone |
| email | Website scraping | Scraped from firm website |
| practice_area | Directories / website scraping | One main area, "General" if not found |
| street_address | Google Places / Foursquare | Street portion of address |
| zip_code | Address parsing | 5-digit ZIP |
| msa | Config lookup | Metropolitan Statistical Area (e.g., "Kansas City") |
| priority | Practice area scoring | 1-5 score (see below) |

### Priority Scoring

| Score | Practice Areas |
|-------|---------------|
| 5 | Criminal Defense, DUI, Personal Injury, Medical Malpractice, Workers Compensation |
| 4 | Sexual Assault, Family Law, General Practice |
| 3 | Employment Law, Nursing Home, Civil Litigation, Insurance Defense, Divorce |
| 2 | Estate Planning, Probate, Bankruptcy, Real Estate Law, Business Law, Immigration |
| 1 | All unlisted practice areas |

Firms with practice_area "General" get score 4 (General Practice tier). Firms with no practice area found default to "General" → score 4.

---

## File Structure

### New files (scraper)

```
scraper/county/
    __init__.py
    pipeline.py          # CLI entry point: python -m scraper.county.pipeline --county johnson
    config.py            # County definitions, city lists, MSA map, priority scores
    google_places.py     # Stage 1: Google Places discovery
    foursquare.py        # Stage 2: Foursquare discovery
    enhance.py           # Stage 3: Enhancement coordinator
    csv_output.py        # Flatten firm records to CSV
    manifest.py          # Generate/update manifest.json
```

### New files (frontend)

```
app/county-data.html     # County data download page
app/county-data.js       # JS for the county data page
app/county-data/
    manifest.json        # Index of available county CSVs
    johnson-county-ks.csv  # Output from first run
```

### Modified files

- `app/index.html` — Add one `<a>` tag in nav for "County Data" button. No other changes.

### Not modified

- `app/firms_data.js` — Untouched
- `app/app.js` — Untouched
- `scraper/scraper.py` — Untouched
- All existing scraper phases — Untouched (county pipeline imports their functions but doesn't modify them)

---

## Frontend

### Main page change

Add navigation link in existing `<nav class="tabs">`:
```html
<a href="county-data.html" class="tab county-data-btn">County Data</a>
```

### County Data page (`county-data.html`)

- Same dark theme (loads `styles.css`)
- Header with title and "Back to Directory" link
- Fetches `county-data/manifest.json`
- Renders a card per county: name, state, firm count, last updated, Download CSV button
- Download is a plain `<a download>` link to the static CSV file
- Empty state if no counties scraped yet

### manifest.json format

```json
{
  "counties": [{
    "slug": "johnson-county-ks",
    "name": "Johnson County",
    "state": "KS",
    "firm_count": 245,
    "last_updated": "2026-04-21",
    "csv_file": "johnson-county-ks.csv"
  }]
}
```

---

## Replicability

Adding a new county requires:
1. Add county definition to `scraper/county/config.py` (name, state, city list, MSA)
2. Run `python -m scraper.county.pipeline --county <slug>`
3. Commit the new CSV and updated manifest
4. Deploy to gh-pages

### CLI

```
python -m scraper.county.pipeline --county johnson
python -m scraper.county.pipeline --county johnson --skip-foursquare
python -m scraper.county.pipeline --county johnson --skip-enhance
python -m scraper.county.pipeline --county johnson --resume
python -m scraper.county.pipeline --county johnson --test
```

---

## Johnson County, KS — First Run

**Cities to search (~19):** Overland Park, Olathe, Shawnee, Lenexa, Leawood, Prairie Village, Merriam, Mission, Gardner, Spring Hill, De Soto, Edgerton, Roeland Park, Fairway, Westwood, Lake Quivira, Mission Hills, Mission Woods, Westwood Hills.

**MSA:** Kansas City

**Expected firm count:** 200-500+ (Johnson County is the most populous county in Kansas)

**Estimated cost:** $15-30 (Google Places, likely within free tier) + $0 (Foursquare free tier) = $15-30 max.

---

## Prerequisites

1. **Google Cloud billing** — Verify billing is enabled on the Google Cloud project tied to the existing API key (unlocks $200/mo free credit)
2. **Foursquare API key** — Provided by user, add to `scraper/.env` as `FOURSQUARE_API_KEY`

---

## Verification Plan

1. Run pipeline with `--test` flag (limit results per city) to verify end-to-end flow
2. Check CSV output has all 14 columns with correct data
3. Verify zero duplicates: check firm count vs unique name count
4. Spot-check 10 firms: verify website URL works, Google Business Profile URL resolves, address is in Johnson County
5. Open `county-data.html` in browser, verify manifest loads, download button works
6. Deploy to gh-pages, verify live download works
