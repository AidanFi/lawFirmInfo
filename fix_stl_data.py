"""
Comprehensive cleanup for St. Louis City and County CSV files.
Fixes:
1. Cross-file phone duplicates (23 cases, resolved via GBP place_id comparison)
2. No-contact entries removed
3. ZIP accuracy improved via street_address extraction
4. Practice area re-scraping for "General" entries with websites
"""

import csv
import json
import re
import sys
import time
from pathlib import Path

CITY_CSV = Path("app/county-data/st-louis-city-mo.csv")
COUNTY_CSV = Path("app/county-data/st-louis-county-mo.csv")
MANIFEST = Path("app/county-data/manifest.json")

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority",
]

# ---------------------------------------------------------------------------
# Fix 1: Cross-file phone duplicate resolution
# ---------------------------------------------------------------------------

# Phones where BOTH files have different GBP place_ids → keep both entries
KEEP_BOTH_PHONES = {
    "(866) 381-4442",  # A Bankruptcy Law Firm (two offices)
    "(636) 916-4040",  # Traffic Law Counselors (two offices)
    "(314) 961-8200",  # Hacking Immigration Law / Hacking Law Practice
    "(314) 740-2989",  # AKS Law Firm / AKS Law
}

# Phones where county entry has GBP and city doesn't → remove from city
REMOVE_FROM_CITY_PHONES = {
    "(314) 443-2578",  # Millan Law Firm → county has GBP
    "(314) 534-3534",  # Paul R. Hales → county has GBP (Hales Law Group)
    "(314) 669-0048",  # Sumner Law Group → county has GBP
    "(314) 696-2262",  # The Manus Law Firm → county has GBP
    "(314) 724-5059",  # The Rogers Law Firm → county has GBP
    "(314) 421-5829",  # Luby Edward F → county has GBP
    "(314) 531-1000",  # Lewis Evelyn → county has GBP
    "(314) 621-9900",  # Neuman Law Firm → county has GBP
    "(314) 920-2634",  # Doskocil Law Firm → county has GBP
    "(636) 352-2030",  # The Kline Law Firm → county has GBP
    "(314) 664-5500",  # Finley Law Firm → county has GBP
}

# Phones where city entry has valid city ZIP and neither/city has GBP → remove from county
REMOVE_FROM_COUNTY_PHONES = {
    "(314) 378-1149",  # Eisenhauer Law (city 63101) vs Philip Eisenhauer (county 63105)
    "(314) 436-2889",  # Philip C. Denton (city 63104) vs county duplicate
    "(314) 481-7778",  # Swaney Law Firm (city 63139) vs county 63044
    "(314) 542-2222",  # Burger Law (city 63102) vs county 63005
    "(314) 655-1448",  # Parmele Law Firm (city 63101) vs county 63146
    "(314) 724-9884",  # Eric V. Barnhart (city 63103) vs county 63105
    "(314) 842-4445",  # Robert J Reinhold (city 63108) vs Reinhold & Reinhold (county)
    "(888) 661-0213",  # IRS Federal Tax Relief (city 63110) vs county 63143
}

# ---------------------------------------------------------------------------
# Fix 3: ZIP accuracy — MO ZIPs by city name for bad/missing ZIPs
# ---------------------------------------------------------------------------

# Authoritative ZIP per city (first/primary ZIP used when extracting from address fails)
CITY_PRIMARY_ZIP = {
    "Affton": "63123",
    "Ballwin": "63011",
    "Berkeley": "63134",
    "Black Jack": "63033",
    "Breckenridge Hills": "63114",
    "Brentwood": "63144",
    "Bridgeton": "63044",
    "Calverton Park": "63135",
    "Charlack": "63114",
    "Chesterfield": "63017",
    "Clayton": "63105",
    "Clarkson Valley": "63011",
    "Cool Valley": "63121",
    "Country Club Hills": "63136",
    "Country Life Acres": "63131",
    "Creve Coeur": "63141",
    "Crystal Lake Park": "63131",
    "Dellwood": "63136",
    "Des Peres": "63131",
    "Edmundson": "63134",
    "Ellisville": "63011",
    "Fenton": "63026",
    "Ferguson": "63135",
    "Flordell Hills": "63136",
    "Florissant": "63031",
    "Frontenac": "63131",
    "Glendale": "63122",
    "Grantwood Village": "63123",
    "Green Park": "63123",
    "Greendale": "63121",
    "Hanley Hills": "63133",
    "Hazelwood": "63042",
    "Hillsdale": "63136",
    "Huntleigh": "63131",
    "Jennings": "63136",
    "Kirkwood": "63122",
    "Ladue": "63124",
    "Lakeshire": "63123",
    "Lemay": "63125",
    "Mackenzie": "63123",
    "Manchester": "63021",
    "Maplewood": "63143",
    "Marlborough": "63119",
    "Maryland Heights": "63043",
    "Mehlville": "63125",
    "Moline Acres": "63136",
    "Normandy": "63121",
    "Norwood Court": "63121",
    "Oakland": "63122",
    "Oakville": "63129",
    "Olivette": "63132",
    "Overland": "63114",
    "Pagedale": "63133",
    "Pasadena Hills": "63121",
    "Pasadena Park": "63121",
    "Pine Lawn": "63120",
    "Richmond Heights": "63117",
    "Riverview": "63137",
    "Rock Hill": "63119",
    "Shrewsbury": "63119",
    "Spanish Lake": "63138",
    "Sunset Hills": "63127",
    "Sycamore Hills": "63114",
    "Town and Country": "63131",
    "Twin Oaks": "63021",
    "University City": "63130",
    "Valley Park": "63088",
    "Velda City": "63121",
    "Velda Village Hills": "63121",
    "Vinita Park": "63114",
    "Vinita Terrace": "63121",
    "Warson Woods": "63122",
    "Webster Groves": "63119",
    "Wellston": "63133",
    "Westwood": "63131",
    "Wilbur Park": "63123",
    "Wildwood": "63040",
    "Winchester": "63021",
    "Woodson Terrace": "63134",
    # St. Louis City
    "St. Louis": "63101",
}

# Valid MO ZIP prefixes for St. Louis area
MO_STL_ZIPS = set(
    [str(z) for z in range(63001, 63200)]
)

_ZIP_RE = re.compile(r'\b(\d{5})\b')


def _fix_zip(row: dict) -> str:
    """Return best ZIP for this row: extract from street_address or use city fallback."""
    current = row["zip_code"].strip()
    # If current ZIP is valid MO STL ZIP, keep it
    if current in MO_STL_ZIPS:
        return current
    # Try to extract from street_address
    street = row.get("street_address", "").strip()
    m = _ZIP_RE.search(street)
    if m:
        z = m.group(1)
        if z in MO_STL_ZIPS:
            return z
    # Fall back to city-based ZIP
    city = row["city"].strip()
    return CITY_PRIMARY_ZIP.get(city, current)


# ---------------------------------------------------------------------------
# Fix 4: Practice area re-scraping
# ---------------------------------------------------------------------------

def _rescrape_practice_areas(rows: list, label: str) -> int:
    """Re-scrape websites for 'General' entries and update practice_area + priority."""
    try:
        from scraper.phases.website_scraper import scrape_firm_website
        from scraper.utils.normalize import normalize_practice_area
        from scraper.county.config import get_priority
    except ImportError as e:
        print(f"  [rescrape] Import error: {e}")
        return 0

    targets = [r for r in rows if r["practice_area"] == "General" and r["website"].strip()]
    print(f"  [rescrape] {label}: re-scraping {len(targets)} General entries with websites")

    updated = 0
    for i, row in enumerate(targets, 1):
        try:
            result = scrape_firm_website(
                row["website"], row["law_firm_name"], row["city"]
            )
        except Exception:
            continue

        areas = result.get("practiceAreas") or []
        if areas:
            # Normalize all areas
            norm_areas = []
            for a in areas:
                n = normalize_practice_area(a)
                if n not in norm_areas:
                    norm_areas.append(n)

            # Pick highest priority
            best_area = max(norm_areas, key=lambda a: get_priority(a))
            best_priority = get_priority(best_area)

            if best_area != "General" or best_priority > 4:
                row["practice_area"] = best_area
                row["priority"] = str(best_priority)
                updated += 1

        if i % 25 == 0 or i == len(targets):
            print(f"  [rescrape] Progress: {i}/{len(targets)}, updated {updated} so far")

        time.sleep(0.8)

    print(f"  [rescrape] {label}: {updated}/{len(targets)} practice areas updated")
    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_csv(path: Path, rows: list):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    skip_scrape = "--skip-scrape" in sys.argv

    print("Loading CSVs...")
    city_rows = load_csv(CITY_CSV)
    county_rows = load_csv(COUNTY_CSV)
    print(f"  City: {len(city_rows)} rows, County: {len(county_rows)} rows")

    # ------------------------------------------------------------------
    # Fix 1: Cross-file phone duplicates
    # ------------------------------------------------------------------
    print("\nFix 1: Resolving cross-file phone duplicates...")
    city_before = len(city_rows)
    county_before = len(county_rows)

    city_rows = [r for r in city_rows if r["phone_number"].strip() not in REMOVE_FROM_CITY_PHONES]
    county_rows = [r for r in county_rows if r["phone_number"].strip() not in REMOVE_FROM_COUNTY_PHONES]

    print(f"  Removed from city: {city_before - len(city_rows)}")
    print(f"  Removed from county: {county_before - len(county_rows)}")

    # ------------------------------------------------------------------
    # Fix 2: Remove no-contact entries
    # ------------------------------------------------------------------
    print("\nFix 2: Removing no-contact entries...")

    def has_contact(r):
        return bool(r["website"].strip() or r["phone_number"].strip() or r["email"].strip())

    city_before = len(city_rows)
    county_before = len(county_rows)
    city_rows = [r for r in city_rows if has_contact(r)]
    county_rows = [r for r in county_rows if has_contact(r)]
    print(f"  Removed from city: {city_before - len(city_rows)}")
    print(f"  Removed from county: {county_before - len(county_rows)}")

    # ------------------------------------------------------------------
    # Fix 3: ZIP accuracy
    # ------------------------------------------------------------------
    print("\nFix 3: Fixing ZIP codes...")
    city_fixed = 0
    county_fixed = 0
    for r in city_rows:
        new_zip = _fix_zip(r)
        if new_zip != r["zip_code"]:
            r["zip_code"] = new_zip
            city_fixed += 1
    for r in county_rows:
        new_zip = _fix_zip(r)
        if new_zip != r["zip_code"]:
            r["zip_code"] = new_zip
            county_fixed += 1
    print(f"  City ZIPs fixed: {city_fixed}")
    print(f"  County ZIPs fixed: {county_fixed}")

    # ------------------------------------------------------------------
    # Fix 4: Re-scrape practice areas
    # ------------------------------------------------------------------
    if not skip_scrape:
        print("\nFix 4: Re-scraping practice areas for General entries...")
        _rescrape_practice_areas(city_rows, "city")
        _rescrape_practice_areas(county_rows, "county")
    else:
        print("\nFix 4: Skipping practice area re-scrape (--skip-scrape)")

    # ------------------------------------------------------------------
    # Save and update manifest
    # ------------------------------------------------------------------
    print("\nSaving updated CSVs...")
    save_csv(CITY_CSV, city_rows)
    save_csv(COUNTY_CSV, county_rows)
    print(f"  City: {len(city_rows)} rows → {CITY_CSV}")
    print(f"  County: {len(county_rows)} rows → {COUNTY_CSV}")

    # Update manifest
    manifest = json.loads(MANIFEST.read_text())
    for entry in manifest.get("counties", []):
        if entry["slug"] == "st-louis-city-mo":
            entry["count"] = len(city_rows)
        elif entry["slug"] == "st-louis-county-mo":
            entry["count"] = len(county_rows)
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"  Manifest updated")

    # Summary
    print("\n=== Summary ===")
    print(f"  City:   {len(city_rows)} firms")
    print(f"  County: {len(county_rows)} firms")

    # Practice area stats
    city_gen = sum(1 for r in city_rows if r["practice_area"] == "General")
    county_gen = sum(1 for r in county_rows if r["practice_area"] == "General")
    print(f"  City 'General': {city_gen}/{len(city_rows)} ({100*city_gen//len(city_rows)}%)")
    print(f"  County 'General': {county_gen}/{len(county_rows)} ({100*county_gen//len(county_rows)}%)")


if __name__ == "__main__":
    main()
