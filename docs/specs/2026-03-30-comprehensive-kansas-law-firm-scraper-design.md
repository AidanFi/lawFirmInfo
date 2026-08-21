# Comprehensive Kansas Law Firm Scraper — Design Spec

## Context

The current scraper finds 707 law firms across 58 Kansas cities using Google Places API, the Kansas Bar Association directory, and website scraping. This misses a large portion of active Kansas law firms, particularly in rural areas and among solo practitioners.

**Goal:** Capture every active practicing law firm in Kansas with as much data as possible — name, practice areas, contact info (phone, email, website), address, and GPS coordinates for mapping.

**Use case:** Referral network building. Practice areas and contact information are the highest-priority data fields.

**Constraints:**
- Free/cheap — minimize or eliminate paid API costs
- Runtime is not a concern (can run overnight)
- Zero duplicates in final output
- Map feature must work (all firms need coordinates where possible)

---

## Architecture Overview

The scraper expands from 4 phases to 7, with a clear separation between **discovery** (finding firms) and **enrichment** (filling in details):

| Phase | Source | Role | Status |
|-------|--------|------|--------|
| 1 | KS Supreme Court Attorney Registration | Discovery | New |
| 2 | Justia Lawyer Directory | Discovery + Enrichment | New |
| 3 | Kansas Bar Association | Enrichment | Rewritten |
| 4 | Google Places API | Enrichment (optional) | Modified |
| 5 | Firm Website Scraping | Enrichment | Kept |
| 6 | Nominatim Geocoding | Enrichment | New |
| 7 | Finalization | Dedup, scoring, output | Enhanced |

Checkpoint system saves state after each phase for resumability.

---

## Phase 1: KS Supreme Court Attorney Registration

**Source:** `directory-kard.kscourts.gov`

**Purpose:** Authoritative discovery of every licensed attorney in Kansas. This is the single source that guarantees completeness — every attorney practicing law in Kansas must be registered here.

**Strategy:**
1. Enumerate registration numbers (sequential integers, estimated range 1–30,000)
2. Fetch details page for each: `GET /Home/Details?regNum={n}`
3. Parse HTML for: attorney name, registration status, city, state, firm/employer name
4. Filter to Active status with Kansas addresses only
5. Group attorneys by normalized firm name + city to create firm records
6. Store attorney count per firm for later use

**Rate limiting:** 1 request/second with polite User-Agent header.

**Estimated runtime:** ~8 hours for 30,000 registration numbers.

**Error handling:**
- 404/empty responses: skip (unassigned registration number)
- Rate limiting/blocking: exponential backoff, retry 3 times, then log warning and continue
- CAPTCHA detection: log warning, pause for 60 seconds, retry; if persistent, skip remaining numbers and proceed to Phase 2

**Output per firm:**
```python
{
    "name": str,              # Firm/employer name from registration
    "attorneys": [str],       # List of attorney names at this firm
    "address": {"city": str, "state": "KS"},
    "sources": ["ks_courts"]
}
```

---

## Phase 2: Justia Lawyer Directory

**Source:** `lawyers.justia.com/kansas`

**Purpose:** Secondary discovery source + rich enrichment data. Justia has practice areas, phone numbers, website URLs, and addresses that the Supreme Court registration lacks.

**Strategy:**
1. Fetch the Kansas state page to get all city/county listing URLs
2. For each city/county, paginate through all attorney listing pages
3. Extract per attorney: name, firm name, practice areas, address (full), phone, website URL
4. Group by firm name + city
5. Merge into Phase 1 data:
   - Existing firms: enrich with practice areas, phone, website, full address
   - New firms (not in Phase 1): create new records

**Rate limiting:** 1 request/second.

**Estimated runtime:** 2–4 hours.

**Output enrichment:**
```python
{
    "practiceAreas": [str],   # From Justia profiles
    "phone": str,             # From Justia listing
    "website": str,           # From Justia listing
    "address": {"street": str, "city": str, "state": str, "zip": str},
    "sources": [..., "justia"]
}
```

---

## Phase 3: Kansas Bar Association (Rewritten)

**Source:** `ksbar.org/search/members`

**Purpose:** Enrich with practice areas and attorney-firm associations. KSBar has ~5,000–6,900 members with self-reported practice areas.

**Current problem:** The page may be JavaScript-rendered, which breaks the current BeautifulSoup-only scraper.

**Fix:** Rewrite using Playwright (headless Chromium) to handle JS rendering:
1. Launch headless browser
2. Navigate to member search page
3. Iterate through search results (handle pagination)
4. Extract: attorney name, firm name, practice areas, city
5. Merge via fuzzy matching into existing data

**Fallback:** If Playwright fails (e.g., CAPTCHA, site structure change), log warning and continue to Phase 4. KSBar enrichment is valuable but not essential since Justia provides similar data.

**Rate limiting:** 2-second delay between page navigations.

**New dependency:** `playwright` (Python package).

---

## Phase 4: Google Places API (Optional)

**Source:** Google Maps Places API

**Purpose:** Enrichment with validated addresses, phone numbers, and GPS coordinates.

**Changes from current:**
- Made optional via `--skip-google` CLI flag (default: skip, to keep costs at zero)
- When enabled, search list expanded from 58 cities to all 105 Kansas county seats
- Only searches for firms that are missing coordinates or phone numbers (targeted enrichment, not blind discovery)

**Cost when enabled:** ~$30–60 per run.

---

## Phase 5: Firm Website Scraping (Kept)

**Source:** Each firm's website URL (from Justia, Google Places, or KSBar)

**Purpose:** Extract email addresses, firm description/summary, and detect practice areas from website content.

**No changes to logic**, but:
- Now processes more firms (since we discover more)
- Timeout per request stays at 5 seconds
- 1-second delay between requests
- Skips firms without a website URL

---

## Phase 6: Nominatim Geocoding (New)

**Source:** Nominatim (OpenStreetMap) — `nominatim.openstreetmap.org`

**Purpose:** Geocode addresses to GPS coordinates for firms missing them. This ensures the map feature works for all firms, not just those found via Google Places.

**Strategy:**
1. Filter to firms that have an address but no coordinates
2. For each, send structured query to Nominatim: `street + city + state + zip`
3. Parse lat/lng from response
4. Fall back to city-level geocoding if full address fails

**Rate limiting:** 1 request/second (Nominatim policy).

**New dependency:** `geopy` (Python package, provides Nominatim client with built-in rate limiting).

---

## Phase 7: Finalization (Enhanced)

### Deduplication (Critical — Zero Duplicates Requirement)

Multi-pass deduplication to ensure no firm appears twice in the final output:

**Pass 1 — Exact match:**
- Normalize firm names (strip LLC/LLP/PC/PA suffixes, normalize "&"/"and", lowercase, remove punctuation)
- Group by exact normalized name + city
- Merge all records in each group into one

**Pass 2 — Fuzzy match:**
- For all remaining firms, compute pairwise similarity using `rapidfuzz.fuzz.token_sort_ratio`
- Threshold: 88% similarity (raised from current 85% to be more aggressive)
- Additionally check: same city, OR same phone number, OR same website domain
- Merge matches

**Pass 3 — Domain/phone dedup:**
- Group firms by website domain (strip www, compare base domain)
- Group firms by phone number
- Merge any groups found

**Pass 4 — Final validation:**
- Sort all firms by normalized name
- Log any remaining potential duplicates (>80% similarity) for manual review
- Write potential duplicates to `data/potential_duplicates.log`

### Merge Priority

When two sources disagree on the same field, use this priority:
1. **Address:** Google Places > Justia > KS Courts > KSBar
2. **Phone:** Google Places > Justia > Website scraping
3. **Email:** Website scraping > Justia
4. **Practice areas:** Union of all sources (deduplicated via canonical mapping)
5. **Website:** Justia > Google Places > KSBar
6. **Coordinates:** Google Places > Nominatim
7. **Summary:** Website scraping (only source)

### Referral Score Calculation

Unchanged from current — uses `scraper/utils/referral.py` based on practice area compatibility.

### Output

Write to `app/firms_data.js` in existing format. Include updated `meta.totalFirms` and `meta.lastScraped`.

---

## New Dependencies

| Package | Purpose | Install |
|---------|---------|---------|
| `playwright` | Headless browser for KSBar JS rendering | `pip install playwright && playwright install chromium` |
| `geopy` | Nominatim geocoding client with rate limiting | `pip install geopy` |

Existing dependencies (`requests`, `beautifulsoup4`, `lxml`, `rapidfuzz`, `python-dotenv`, `googlemaps`) remain.

---

## CLI Changes

```
python -m scraper.scraper [options]

Options:
  --use-google        Enable Google Places API phase (default: skipped to avoid costs)
  --skip-ksbar        Skip KSBar phase (if site is down)
  --skip-justia       Skip Justia phase
  --skip-websites     Skip website scraping phase
  --max-reg-num N     Override max registration number for KS Courts (default: 30000)
  --resume            Resume from last checkpoint
  --test              Quick test mode (limit to 100 records per phase)
```

---

## Expected Results

| Metric | Current | Expected |
|--------|---------|----------|
| Total firms | 707 | 2,000–4,000+ |
| Cities covered | 58 | All 627 incorporated cities |
| Firms with practice areas | ~400 | ~2,500+ |
| Firms with email | ~300 | ~1,500+ |
| Firms with coordinates | ~600 | ~3,500+ |
| Firms with phone | ~600 | ~3,000+ |
| Runtime (full scrape) | 30–60 min | 10–14 hours |
| API cost | $15–40 | $0 (Google skipped) |

---

## Verification Plan

1. **Unit tests:** Add tests for each new phase (mock HTTP responses)
2. **Integration test:** Run with `--test` flag to verify pipeline works end-to-end with limited data
3. **Dedup verification:** After full run, check `data/potential_duplicates.log` for false negatives
4. **Map verification:** Open `app/index.html`, verify all firms appear on map with correct locations
5. **Coverage check:** Compare total firms against known attorney count (~8,000 attorneys / ~2-3 attorneys per firm average = ~3,000-4,000 firms expected)
6. **Data quality spot check:** Randomly sample 20 firms, verify data accuracy against their websites

---

## Files to Modify/Create

**New files:**
- `scraper/phases/ks_courts.py` — Phase 1: KS Supreme Court registration scraper
- `scraper/phases/justia.py` — Phase 2: Justia directory scraper
- `scraper/utils/geocode.py` — Nominatim geocoding utility
- `tests/scraper/test_ks_courts.py` — Tests for Phase 1
- `tests/scraper/test_justia.py` — Tests for Phase 2
- `tests/scraper/test_geocode.py` — Tests for geocoding

**Modified files:**
- `scraper/scraper.py` — Orchestrate 7 phases, add CLI flags
- `scraper/phases/ksbar.py` — Rewrite with Playwright
- `scraper/utils/normalize.py` — Enhanced deduplication (multi-pass)
- `scraper/requirements.txt` — Add playwright, geopy
- `tests/scraper/test_ksbar.py` — Update for Playwright-based scraper
- `tests/scraper/test_normalize.py` — Tests for enhanced dedup
