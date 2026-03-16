# Kansas Law Firm Directory

A static web app for browsing and filtering every law firm in Kansas, designed for referral partner outreach.

## Setup

### 1. Get a Google Maps API Key

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a project → Enable "Places API"
3. Create an API key

**Cost estimate:** ~$15–40 per full scrape (180 text_search + ~500 Place Details calls). The free $200/month credit typically covers this.

### 2. Configure

```bash
cp scraper/.env.example scraper/.env
# Edit scraper/.env and set GOOGLE_MAPS_API_KEY=your_key_here
```

### 3. Install Python dependencies

The project uses a virtual environment at `.venv/`. Activate it first:

```bash
source .venv/bin/activate
pip install -r scraper/requirements.txt
```

Or use the venv directly without activating:

```bash
.venv/bin/pip install -r scraper/requirements.txt
```

### 4. Run the scraper

```bash
# Quick test (5 cities, ~2 min, no API cost beyond ~10 calls)
python -m scraper.scraper --test

# Full run (~30-60 min, see cost note above)
python -m scraper.scraper
```

If using the venv directly:

```bash
.venv/bin/python -m scraper.scraper --test
```

Output is written directly to `app/firms_data.js`.

If the scraper is interrupted, re-run the same command — it will resume from the last checkpoint.

### 5. Open the app

Open `app/index.html` in any browser. No server required.

## Refreshing Data

Re-run `python -m scraper.scraper` monthly to pick up new firms.

## Features

- **Directory:** Browse all firms with sidebar filters (practice area, city, county, referral match, contact status)
- **Map:** Color-coded pins by referral match — green=high, yellow=medium, gray=low, red=competitor
- **Starred:** Bookmark shortlist of target referral partners
- **Settings:** Toggle default view, change your practice area (updates all referral scores live), export CSV

All notes, stars, and contact statuses are saved in your browser and persist across sessions.
