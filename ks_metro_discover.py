#!/usr/bin/env python3
"""
Standalone KC Metro KS law firm discovery using Martindale + FindLaw.
Both sources use curl_cffi browser impersonation (Cloudflare bypass).

Usage: python ks_metro_discover.py [--findlaw-only | --martindale-only]
"""
import csv
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests as std_requests
from scraper.phases.martindale import _get as _mget, _extract_listings, _extract_city_urls
from scraper.phases.findlaw import _get as _fget, _extract_firms_from_listing, _TOP_PRACTICE_AREAS
from scraper.utils.normalize import normalize_firm_name

# Load Yelp API key from scraper/.env
_YELP_KEY = ""
_env_path = Path("scraper/.env")
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        if line.startswith("YELP_API_KEY="):
            _YELP_KEY = line.split("=", 1)[1].strip()
            break

DATA_DIR = Path("app/county-data")

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email",
    "practice_area", "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

# County → city lists (canonical Title Case)
COUNTIES = {
    "johnson-county-ks": {
        "county": "Johnson", "state": "KS", "msa": "Kansas City",
        "cities": [
            "Overland Park", "Olathe", "Shawnee", "Lenexa", "Leawood",
            "Prairie Village", "Merriam", "Mission", "Gardner", "Spring Hill",
            "De Soto", "Roeland Park", "Fairway", "Westwood", "Edgerton",
        ],
    },
    "wyandotte-county-ks": {
        "county": "Wyandotte", "state": "KS", "msa": "Kansas City",
        "cities": ["Kansas City", "Bonner Springs", "Edwardsville"],
    },
    "leavenworth-county-ks": {
        "county": "Leavenworth", "state": "KS", "msa": "Kansas City",
        "cities": ["Leavenworth", "Lansing", "Basehor", "Tonganoxie", "Linwood", "Easton"],
    },
    "miami-county-ks": {
        "county": "Miami", "state": "KS", "msa": "Kansas City",
        "cities": ["Paola", "Osawatomie", "Louisburg", "Fontana"],
    },
    "linn-county-ks": {
        "county": "Linn", "state": "KS", "msa": "Kansas City",
        "cities": ["Pleasanton", "La Cygne", "Mound City", "Prescott", "Blue Mound"],
    },
}

# Flat city → county_slug mapping for fast lookup
CITY_TO_COUNTY = {}
for slug, cfg in COUNTIES.items():
    for city in cfg["cities"]:
        # Use lowercase for matching
        CITY_TO_COUNTY[city.lower()] = slug

ALL_TARGET_CITIES = set(CITY_TO_COUNTY.keys())


def gbp_link(name: str, city: str, state: str) -> str:
    query = f"{name} {city} {state}"
    return f"https://www.google.com/maps/search/?api=1&query={quote(query)}"


def _load_csv(path: Path) -> tuple[list[dict], set[str]]:
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    seen = set()
    for r in rows:
        name = r.get("law_firm_name", "").strip()
        city = r.get("city", "").strip().lower()
        if name:
            seen.add(normalize_firm_name(name) + "|" + city)
    return rows, seen


def _write_csv(path: Path, rows: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)


# ── Martindale ──────────────────────────────────────────────────────────────

def run_martindale() -> dict[str, list[dict]]:
    """Return dict: county_slug → list of firm dicts."""
    print("\n[martindale] Fetching Kansas city index...")
    soup = _mget("https://www.martindale.com/by-location/kansas-lawyers/", delay=1.5)
    if not soup:
        print("[martindale] ERROR: Could not load city index")
        return {}

    city_urls = _extract_city_urls(soup)
    print(f"[martindale] Found {len(city_urls)} Kansas cities")

    # Filter to only target cities
    target_city_urls = [
        (name, url) for name, url in city_urls
        if name.strip().lower() in ALL_TARGET_CITIES
    ]
    print(f"[martindale] Filtered to {len(target_city_urls)} target cities: "
          f"{[n for n, _ in target_city_urls]}")

    results: dict[str, list[dict]] = {slug: [] for slug in COUNTIES}

    for city_name, city_url in target_city_urls:
        city_key = city_name.strip().lower()
        county_slug = CITY_TO_COUNTY.get(city_key)
        if not county_slug:
            continue

        cfg = COUNTIES[county_slug]
        print(f"  [martindale] Scraping {city_name} ({county_slug})...", end=" ", flush=True)

        page_url = city_url
        city_firms = []

        for page_i in range(10):
            soup = _mget(page_url, delay=1.5)
            if not soup:
                break

            entries = _extract_listings(soup)
            if not entries:
                break

            for entry in entries:
                name = entry.get("firm_name") or entry.get("attorney_name")
                if not name or len(name.strip()) < 3:
                    continue
                city_firms.append({
                    "law_firm_name": name.strip(),
                    "website": entry.get("website") or "",
                    "google_business_profile": "",
                    "legal_directory_listing": city_url,
                    "city": city_name.strip(),
                    "state": cfg["state"],
                    "county": cfg["county"],
                    "phone_number": entry.get("phone") or "",
                    "email": "",
                    "practice_area": "General",
                    "street_address": "",
                    "zip_code": "",
                    "msa": cfg["msa"],
                    "priority": "",
                    "number_of_lawyers": "",
                })

            # Next page
            next_url = None
            for a in soup.find_all("a", href=True):
                rel = a.get("rel") or []
                if "next" in rel:
                    h = a["href"]
                    next_url = h if h.startswith("http") else f"https://www.martindale.com{h}"
                    break
            if not next_url:
                for a in soup.find_all("a", href=True):
                    t = a.get_text(strip=True).lower()
                    if t in ("next", "next »", "»", "next page"):
                        h = a["href"]
                        next_url = h if h.startswith("http") else f"https://www.martindale.com{h}"
                        break

            if not next_url or next_url == page_url:
                break
            page_url = next_url

        print(f"{len(city_firms)} entries", flush=True)
        results[county_slug].extend(city_firms)

    return results


# ── FindLaw ─────────────────────────────────────────────────────────────────

def _city_to_slug(city: str) -> str:
    """Convert city name to FindLaw URL slug."""
    return re.sub(r"[^a-z0-9]+", "-", city.lower()).strip("-")


def _extract_with_city_check(soup, pa_name: str, expected_city: str) -> list[dict]:
    """Extract FindLaw cards, keeping ONLY firms whose address city matches expected_city.

    FindLaw city searches return statewide attorneys alongside local ones. The card
    location element shows the firm's actual office address (e.g. "123 Main St,
    Overland Park, KS 66223"). We parse that actual city and reject anything that
    doesn't match the searched city.
    """
    firms = []
    seen = set()
    expected = expected_city.lower().strip()

    cards = soup.find_all(class_="fl-serp-card")
    for card in cards:
        title_el = card.find(class_="fl-serp-card-title")
        if not title_el:
            continue
        name = re.sub(r"Sponsored$", "", title_el.get_text(strip=True)).strip()
        if not name or name in seen:
            continue

        street = ""
        actual_city = ""
        loc_el = card.find(class_="fl-serp-card-location")
        if loc_el:
            loc_text = loc_el.get_text(strip=True)
            # Format: "123 Main St, Overland Park, KS 66223"
            parts = loc_text.rsplit(",", 2)
            if len(parts) == 3:
                street = parts[0].strip()
                actual_city = parts[1].strip().lower()
            elif len(parts) == 2:
                # "City, KS ZIP" (no street)
                actual_city = parts[0].strip().lower()

        # Require the card to have a city AND that city must match the searched city
        if not actual_city or actual_city != expected:
            continue

        seen.add(name)
        firms.append({"name": name, "street": street, "practice_area": pa_name})

    return firms


def run_findlaw() -> dict[str, list[dict]]:
    """Targeted FindLaw sweep with address-city validation.

    Only includes firms whose card location city matches the searched city,
    filtering out statewide attorneys who appear on every small-city page.
    """
    _FL_BASE = "https://lawyers.findlaw.com"

    targets = []
    for county_slug, cfg in COUNTIES.items():
        for city in cfg["cities"]:
            targets.append((county_slug, city, _city_to_slug(city)))

    total_pa = len(_TOP_PRACTICE_AREAS)
    total_combos = len(targets) * total_pa
    print(f"\n[findlaw] Targeted scrape (city-validated): {len(targets)} cities × "
          f"{total_pa} practice areas = {total_combos} combinations")

    # firm_map per county: normalized_name|city → firm dict
    firm_maps: dict[str, dict] = {slug: {} for slug in COUNTIES}
    requests_made = 0
    firms_found = 0
    skipped_wrong_city = 0

    for pa_idx, (pa_name, pa_slug) in enumerate(_TOP_PRACTICE_AREAS):
        for county_slug, city, city_slug in targets:
            cfg = COUNTIES[county_slug]
            url = f"{_FL_BASE}/{pa_slug}/kansas/{city_slug}/"
            page = 1

            while page <= 3:
                soup = _fget(url, delay=0.4)
                requests_made += 1
                if not soup:
                    break

                # Use validated extraction — only keep firms actually in this city
                raw_cards = soup.find_all(class_="fl-serp-card")
                entries = _extract_with_city_check(soup, pa_name, city)
                if not raw_cards:
                    break
                skipped_wrong_city += len(raw_cards) - len(entries)

                for entry in entries:
                    name = entry["name"].strip()
                    if not name:
                        continue
                    key = normalize_firm_name(name) + "|" + city.lower()
                    if key in firm_maps[county_slug]:
                        existing_pas = set(firm_maps[county_slug][key]["practice_area"].split(" | "))
                        if pa_name not in existing_pas:
                            firm_maps[county_slug][key]["practice_area"] += f" | {pa_name}"
                    else:
                        firm_maps[county_slug][key] = {
                            "law_firm_name": name,
                            "website": "",
                            "google_business_profile": "",
                            "legal_directory_listing": url,
                            "city": city,
                            "state": cfg["state"],
                            "county": cfg["county"],
                            "phone_number": "",
                            "email": "",
                            "practice_area": pa_name,
                            "street_address": entry.get("street") or "",
                            "zip_code": "",
                            "msa": cfg["msa"],
                            "priority": "",
                            "number_of_lawyers": "",
                        }
                        firms_found += 1

                # Next page
                next_link = soup.find("a", string=re.compile(r"Next", re.I), href=True)
                if next_link:
                    href = next_link["href"]
                    url = href if href.startswith("http") else f"{_FL_BASE}{href}"
                    page += 1
                else:
                    break

        if (pa_idx + 1) % 5 == 0:
            print(f"  [findlaw] {pa_idx+1}/{total_pa} practice areas, "
                  f"{firms_found} local firms kept, {skipped_wrong_city} wrong-city skipped, "
                  f"{requests_made} requests", flush=True)

    results = {slug: list(fm.values()) for slug, fm in firm_maps.items()}
    print(f"[findlaw] Done: {firms_found} validated local firms, {requests_made} requests")
    for slug, firms in results.items():
        if firms:
            print(f"  {slug}: {len(firms)} firms", flush=True)
    return results


# ── Yelp ─────────────────────────────────────────────────────────────────────

_YELP_SEARCH = "https://api.yelp.com/v3/businesses/search"
_LAW_KEYWORDS = ("law", "legal", "attorney", "lawyer", "counsel", "firm",
                 "llc", "llp", "pllc", "p.a.", " pa ", " pc ", "chartered")


def _is_law_business(name: str, categories: list) -> bool:
    aliases = {c.get("alias", "") for c in categories}
    if "lawyers" in aliases or "legalservices" in aliases:
        return True
    return any(kw in f" {name.lower()} " for kw in _LAW_KEYWORDS)


def run_yelp() -> dict[str, list[dict]]:
    """Discover law firms via Yelp Fusion API. Uses real geographic search."""
    if not _YELP_KEY:
        print("[yelp] No YELP_API_KEY found — skipping")
        return {slug: [] for slug in COUNTIES}

    headers = {"Authorization": f"Bearer {_YELP_KEY}", "Accept": "application/json"}
    results: dict[str, list[dict]] = {slug: [] for slug in COUNTIES}
    total_calls = 0
    total_found = 0

    for county_slug, cfg in COUNTIES.items():
        county_cities_lower = {c.lower() for c in cfg["cities"]}
        county_firms = []

        for city in cfg["cities"]:
            location = f"{city}, {cfg['state']}"
            offset = 0

            while True:
                params = {"categories": "lawyers", "location": location,
                          "limit": 50, "offset": offset}
                try:
                    resp = std_requests.get(_YELP_SEARCH, headers=headers,
                                            params=params, timeout=15)
                    total_calls += 1
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    print(f"  [yelp] Error for {location}: {e}", flush=True)
                    break

                businesses = data.get("businesses", [])
                if not businesses:
                    break

                for biz in businesses:
                    name = biz.get("name", "").strip()
                    if not name:
                        continue
                    cats = biz.get("categories", [])
                    if not _is_law_business(name, cats):
                        continue

                    loc = biz.get("location", {})
                    actual_city = loc.get("city", "").strip()

                    # Only include firms physically in this county's cities
                    if actual_city.lower() not in county_cities_lower:
                        continue

                    phone = biz.get("display_phone") or biz.get("phone") or ""
                    street = loc.get("address1", "")
                    zip_code = loc.get("zip_code", "")

                    county_firms.append({
                        "law_firm_name": name,
                        "website": "",
                        "google_business_profile": "",
                        "legal_directory_listing": "",
                        "city": actual_city,
                        "state": cfg["state"],
                        "county": cfg["county"],
                        "phone_number": phone,
                        "email": "",
                        "practice_area": "General",
                        "street_address": street,
                        "zip_code": zip_code,
                        "msa": cfg["msa"],
                        "priority": "",
                        "number_of_lawyers": "",
                    })

                offset += len(businesses)
                total = data.get("total", 0)
                if offset >= min(total, 240):
                    break
                time.sleep(0.3)

        results[county_slug] = county_firms
        if county_firms:
            print(f"  [yelp] {county_slug}: {len(county_firms)} firms raw", flush=True)

    print(f"[yelp] Done: {total_calls} API calls", flush=True)
    return results


# ── Merge ────────────────────────────────────────────────────────────────────

def merge_into_county(county_slug: str, new_firms: list[dict]):
    cfg = COUNTIES[county_slug]
    csv_path = DATA_DIR / f"{county_slug}.csv"

    if not csv_path.exists():
        print(f"  WARNING: {csv_path} not found — skipping")
        return

    rows, seen = _load_csv(csv_path)
    before = len(rows)
    added = 0

    for firm in new_firms:
        name = firm.get("law_firm_name", "").strip()
        city = firm.get("city", "").strip()
        if not name:
            continue

        key = normalize_firm_name(name) + "|" + city.lower()
        if key in seen:
            continue

        # Fill google_business_profile if no website
        if not firm.get("website") and not firm.get("google_business_profile"):
            firm["google_business_profile"] = gbp_link(name, city, cfg["state"])

        # Ensure all required fields present
        row = {f: firm.get(f, "") for f in FIELDNAMES}
        rows.append(row)
        seen.add(key)
        added += 1

    if added > 0:
        _write_csv(csv_path, rows)

    print(f"  {county_slug}: {before} → {before + added} firms (+{added} new)")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = set(sys.argv[1:])
    only_yelp = "--yelp-only" in args
    only_findlaw = "--findlaw-only" in args
    only_martindale = "--martindale-only" in args

    run_m = not only_yelp and not only_findlaw
    run_f = not only_yelp and not only_martindale
    run_y = not only_martindale and not only_findlaw

    # Collect all new firms per county from all sources
    all_new: dict[str, list[dict]] = {slug: [] for slug in COUNTIES}

    if run_m:
        m_results = run_martindale()
        for slug, firms in m_results.items():
            all_new[slug].extend(firms)

    if run_f:
        f_results = run_findlaw()
        for slug, firms in f_results.items():
            all_new[slug].extend(firms)

    if run_y:
        y_results = run_yelp()
        for slug, firms in y_results.items():
            all_new[slug].extend(firms)

    print("\n── Merging into CSVs ──")
    for county_slug in COUNTIES:
        merge_into_county(county_slug, all_new[county_slug])

    print("\nDone.")


if __name__ == "__main__":
    main()
