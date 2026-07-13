#!/usr/bin/env python3
"""
Martindale discovery sweep for 17 central KS counties.
Fetches the KS city index, crawls listing pages for target cities,
and appends net-new law firms to the county CSVs.

Usage: python3 martindale_central_ks.py
"""
import csv
import json
import re
import sys
import time
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, ".")
import requests
from scraper.phases.martindale import _get, _extract_listings, _extract_next_page_url

DATA_DIR = Path("app/county-data")

PRACTICE_KEYWORDS = {
    "Personal Injury": ["personal injury", "car accident", "auto accident", "wrongful death"],
    "Family Law": ["family law", "divorce", "child custody"],
    "Criminal Defense": ["criminal defense", "criminal attorney", "dui", "dwi"],
    "DUI": ["dui attorney", "dwi attorney", "drunk driving"],
    "Estate Planning": ["estate planning", "wills and trusts", "probate"],
    "Workers' Compensation": ["workers comp", "workers' compensation", "work injury"],
    "Bankruptcy": ["bankruptcy", "chapter 7", "chapter 13"],
    "Business Law": ["business attorney", "corporate attorney", "business law"],
    "Real Estate": ["real estate attorney", "real estate law"],
    "Immigration": ["immigration attorney", "immigration lawyer"],
    "Employment Law": ["employment attorney", "wrongful termination"],
}
PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Family Law": 4, "Workers' Compensation": 5,
    "Employment Law": 3, "Estate Planning": 2,
    "Bankruptcy": 2, "Real Estate": 2, "Business Law": 2, "Immigration": 2,
}
_FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,*/*;q=0.8",
}


def _guess_practice(text: str) -> tuple[str, str]:
    lower = text.lower()
    for area, kws in PRACTICE_KEYWORDS.items():
        if any(kw in lower for kw in kws):
            return area, str(PRIORITY_SCORES.get(area, 2))
    return "General", "2"


def _fetch_practice(url: str) -> tuple[str, str]:
    try:
        r = requests.get(url, headers=_FETCH_HEADERS, timeout=6, verify=False, allow_redirects=True)
        if r.status_code == 200:
            return _guess_practice(r.text)
    except Exception:
        pass
    return "General", "2"
MANIFEST_PATH = DATA_DIR / "manifest.json"
_BASE = "https://www.martindale.com"

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

# City → (slug, county_name, msa, area_code)
CITY_TO_COUNTY = {
    "salina":       ("saline-county-ks",    "Saline",    "Salina", "785"),
    "hutchinson":   ("reno-county-ks",      "Reno",      "Wichita", "620"),
    "newton":       ("harvey-county-ks",    "Harvey",    "Wichita", "316"),
    "mcpherson":    ("mcpherson-county-ks", "McPherson", "", "620"),
    "mc pherson":   ("mcpherson-county-ks", "McPherson", "", "620"),
    "great bend":   ("barton-county-ks",    "Barton",    "", "620"),
    "abilene":      ("dickinson-county-ks", "Dickinson", "", "785"),
    "beloit":       ("mitchell-county-ks",  "Mitchell",  "", "785"),
    "concordia":    ("cloud-county-ks",     "Cloud",     "", "785"),
    "russell":      ("russell-county-ks",   "Russell",   "", "785"),
    "lyons":        ("rice-county-ks",      "Rice",      "", "620"),
    "sterling":     ("rice-county-ks",      "Rice",      "", "620"),
    "kingman":      ("kingman-county-ks",   "Kingman",   "", "620"),
    "marion":       ("marion-county-ks",    "Marion",    "", "620"),
    "clay center":  ("clay-county-ks",      "Clay",      "", "785"),
    "ellsworth":    ("ellsworth-county-ks", "Ellsworth", "", "785"),
    "saint john":   ("stafford-county-ks",  "Stafford",  "", "620"),
    "st. john":     ("stafford-county-ks",  "Stafford",  "", "620"),
    "st john":      ("stafford-county-ks",  "Stafford",  "", "620"),
    "minneapolis":  ("ottawa-county-ks",    "Ottawa",    "", "785"),
    "lincoln":      ("lincoln-county-ks",   "Lincoln",   "", "785"),
    "hillsboro":    ("marion-county-ks",    "Marion",    "", "620"),
    "peabody":      ("marion-county-ks",    "Marion",    "", "620"),
    "hesston":      ("harvey-county-ks",    "Harvey",    "Wichita", "316"),
    "halstead":     ("harvey-county-ks",    "Harvey",    "Wichita", "316"),
    "ellinwood":    ("barton-county-ks",    "Barton",    "", "620"),
    "hoisington":   ("barton-county-ks",    "Barton",    "", "620"),
    "herington":    ("dickinson-county-ks", "Dickinson", "", "785"),
    "chapman":      ("dickinson-county-ks", "Dickinson", "", "785"),
    "buhler":       ("reno-county-ks",      "Reno",      "Wichita", "620"),
    "nickerson":    ("reno-county-ks",      "Reno",      "Wichita", "620"),
    "inman":        ("mcpherson-county-ks", "McPherson", "", "620"),
    "lindsborg":    ("mcpherson-county-ks", "McPherson", "", "620"),
    "south hutchinson": ("reno-county-ks", "Reno",      "Wichita", "620"),
}

# Junk patterns to filter from Martindale (government/non-law employers)
JUNK_RE = re.compile(
    r"\b(county\s+attorney|district\s+court|clerk\s+of\s+(the\s+)?court|"
    r"judicial\s+district|department\s+of|office\s+of\s+(the\s+)?|"
    r"state\s+of\s+kansas|board\s+of\s+(county\s+)?|"
    r"USD\s*\d+|unified\s+school|community\s+college|"
    r"medical\s+center|regional\s+medical|memorial\s+hospital|"
    r"health\s+system|healthcare|HF\s*Sinclair|Holly\s*Frontier|"
    r"Tyson\s+Foods|Cargill|Dillons|Walmart|USD\b)\b",
    re.I,
)


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(
        r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|"
        r"attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd|co|inc|"
        r"corp|company|limited|ltd|incorporated|associates?)\b",
        "", name,
    )
    return re.sub(r"[^a-z0-9]", "", name)


def load_existing(slug: str) -> tuple[list[dict], set[str]]:
    """Load existing CSV rows and build dedup set (normalized_name|city)."""
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        return [], set()
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    seen = set()
    for r in rows:
        key = normalize(r.get("law_firm_name", "")) + "|" + r.get("city", "").lower().strip()
        seen.add(key)
    return rows, seen


def fix_phone(phone: str, area_code: str) -> str:
    """Normalize phone; add area code if missing."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 7:
        digits = area_code + digits
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


def save_csv(slug: str, rows: list[dict]) -> None:
    path = DATA_DIR / f"{slug}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def update_manifest(counts: dict[str, int]) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())
    for entry in manifest["counties"]:
        if entry["slug"] in counts:
            entry["firm_count"] = counts[entry["slug"]]
            entry["last_updated"] = "2026-07-12"
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    # Load all existing CSVs up front for cross-county dedup
    all_data: dict[str, tuple[list, set]] = {}
    for city_lc, (slug, *_) in CITY_TO_COUNTY.items():
        if slug not in all_data:
            all_data[slug] = load_existing(slug)
    print(f"Loaded {len(all_data)} county CSVs")

    # Fetch KS city index
    print("\n[martindale] Fetching Kansas city index...")
    soup = _get(f"{_BASE}/by-location/kansas-lawyers/")
    if not soup:
        print("[martindale] Could not load city index — aborting")
        return

    # Extract all city listing URLs
    from scraper.phases.martindale import _extract_city_urls
    city_urls = _extract_city_urls(soup)
    print(f"[martindale] Found {len(city_urls)} city pages on index")

    # Filter to central KS cities
    want_cities = set(CITY_TO_COUNTY.keys())
    target_urls = [(name, url) for name, url in city_urls if name.lower() in want_cities]
    print(f"[martindale] Filtered to {len(target_urls)} target cities: "
          f"{[n for n, _ in target_urls]}")

    new_firms_by_slug: dict[str, list[dict]] = {}
    total_new = 0

    for city_name, city_url in target_urls:
        city_lc = city_name.lower().strip()
        slug, county_name, msa, area_code = CITY_TO_COUNTY[city_lc]
        rows, seen = all_data[slug]
        city_new = 0
        page_url = city_url
        city_entries = 0

        for page_i in range(8):  # up to 8 pages per city
            soup = _get(page_url)
            if not soup:
                break
            entries = _extract_listings(soup)
            if not entries:
                break
            city_entries += len(entries)

            for entry in entries:
                # Must have a firm or attorney name
                raw_name = (entry.get("firm_name") or entry.get("attorney_name") or "").strip()
                if not raw_name or len(raw_name) < 3:
                    continue
                # Filter government / non-law entities
                if JUNK_RE.search(raw_name):
                    continue
                # Confirm KS city match
                entry_city = (entry.get("city") or city_name).strip()
                entry_city_lc = entry_city.lower()
                if entry_city_lc not in CITY_TO_COUNTY and city_lc not in CITY_TO_COUNTY:
                    continue
                actual_city_lc = entry_city_lc if entry_city_lc in CITY_TO_COUNTY else city_lc
                actual_slug, actual_county, actual_msa, actual_area = CITY_TO_COUNTY[actual_city_lc]

                # Dedup check
                key = normalize(raw_name) + "|" + actual_city_lc
                rows_for_slug, seen_for_slug = all_data[actual_slug]
                if key in seen_for_slug or not normalize(raw_name):
                    continue

                phone = fix_phone(entry.get("phone") or "", actual_area)
                website = entry.get("website") or ""

                practice, priority = "General", "2"
                if website:
                    practice, priority = _fetch_practice(website)

                new_row = {
                    "law_firm_name": raw_name,
                    "website": website,
                    "google_business_profile": "",
                    "legal_directory_listing": "",
                    "city": entry_city.title() if entry_city.isupper() else entry_city,
                    "state": "KS",
                    "county": actual_county,
                    "phone_number": phone,
                    "email": "",
                    "practice_area": practice,
                    "street_address": "",
                    "zip_code": "",
                    "msa": actual_msa,
                    "priority": priority,
                    "number_of_lawyers": "",
                }
                rows_for_slug.append(new_row)
                seen_for_slug.add(key)
                new_firms_by_slug.setdefault(actual_slug, []).append(new_row)
                city_new += 1
                total_new += 1

            next_url = _extract_next_page_url(soup, page_url)
            if not next_url or next_url == page_url:
                break
            page_url = next_url

        print(f"  {city_name}: +{city_new} new firms ({city_entries} entries scanned)")

    print(f"\nTotal new firms: {total_new}")

    if total_new == 0:
        print("No new firms found — skipping CSV writes")
        return

    # Write updated CSVs for slugs that got new firms
    import central_ks_cleanup as cleanup
    import importlib
    importlib.reload(cleanup)

    counts = {}
    for slug in set(new_firms_by_slug.keys()):
        rows, _ = all_data[slug]
        save_csv(slug, rows)
        # Run cleanup to remove any junk that slipped through
        n = cleanup.process_county(slug)
        if n is not None:
            counts[slug] = n
        print(f"  [{slug}] after cleanup: {n}")

    if counts:
        update_manifest(counts)
        print(f"\nManifest updated for {len(counts)} counties")

    print("\n=== Final counts after Martindale sweep ===")
    for slug, n in counts.items():
        print(f"  {slug}: {n}")


if __name__ == "__main__":
    main()
