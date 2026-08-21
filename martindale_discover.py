#!/usr/bin/env python3
"""Martindale attorney discovery for KC Metro KS counties.

Uses scraper/phases/martindale.py with longer delays to avoid rate limiting.
Returns firm names (not just attorney names), making it complementary to Avvo/Justia.
"""
import csv, re, sys, time
from pathlib import Path

from scraper.phases.martindale import _get, _extract_listings

DATA_DIR = Path(__file__).parent / "app/county-data"

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

COUNTIES = {
    "johnson-county-ks": {
        "county": "Johnson", "state": "KS", "msa": "Kansas City",
        "cities": {
            "Overland Park": "overland-park",
            "Olathe": "olathe",
            "Shawnee": "shawnee",
            "Lenexa": "lenexa",
            "Leawood": "leawood",
            "Prairie Village": "prairie-village",
            "Merriam": "merriam",
            "Mission": "mission",
            "Gardner": "gardner",
            "Spring Hill": "spring-hill",
            "De Soto": "de-soto",
            "Roeland Park": "roeland-park",
            "Fairway": "fairway",
            "Westwood": "westwood",
            "Edgerton": "edgerton",
        },
    },
    "wyandotte-county-ks": {
        "county": "Wyandotte", "state": "KS", "msa": "Kansas City",
        "cities": {
            "Kansas City": "kansas-city",
            "Bonner Springs": "bonner-springs",
            "Edwardsville": "edwardsville",
        },
    },
    "leavenworth-county-ks": {
        "county": "Leavenworth", "state": "KS", "msa": "Kansas City",
        "cities": {
            "Leavenworth": "leavenworth",
            "Lansing": "lansing",
            "Basehor": "basehor",
            "Tonganoxie": "tonganoxie",
            "Linwood": "linwood",
            "Easton": "easton",
        },
    },
    "miami-county-ks": {
        "county": "Miami", "state": "KS", "msa": "Kansas City",
        "cities": {
            "Paola": "paola",
            "Osawatomie": "osawatomie",
            "Louisburg": "louisburg",
            "Fontana": "fontana",
        },
    },
    "linn-county-ks": {
        "county": "Linn", "state": "KS", "msa": "Kansas City",
        "cities": {
            "Pleasanton": "pleasanton",
            "La Cygne": "la-cygne",
            "Mound City": "mound-city",
            "Prescott": "prescott",
            "Blue Mound": "blue-mound",
        },
    },
}


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd)\b", "", name)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def load_existing(county_slug: str) -> tuple[list[dict], set[str]]:
    path = DATA_DIR / f"{county_slug}.csv"
    rows = []
    seen = set()
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
                key = normalize(row.get("law_firm_name", "")) + "|" + row.get("city", "").lower().strip()
                seen.add(key)
    return rows, seen


def save_csv(county_slug: str, rows: list[dict]) -> None:
    path = DATA_DIR / f"{county_slug}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def scrape_city(city_name: str, city_slug: str, county_info: dict, delay: float = 3.5) -> list[dict]:
    base_url = f"https://www.martindale.com/all-lawyers/{city_slug}/kansas/"
    all_listings = []
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}?page={page}"
        soup = _get(url, delay=delay)
        if not soup:
            if page == 1:
                print(f"  {city_name} p{page}: rate limited, waiting 180s")
                time.sleep(180)
                soup = _get(url, delay=delay)
                if not soup:
                    print(f"  {city_name}: still blocked, skipping")
                    break
            else:
                break

        listings = _extract_listings(soup)
        if not listings:
            break

        # Filter to correct city (Martindale sometimes returns nearby cities)
        city_listings = [l for l in listings if l.get("city", "").lower() == city_name.lower()]
        all_listings.extend(city_listings)

        # Check if there's a next page
        page_nums = re.findall(r"page=(\d+)", str(soup))
        max_page = max(int(p) for p in page_nums) if page_nums else page
        if page >= max_page:
            break
        page += 1
        time.sleep(delay)

    print(f"  {city_name}: {page} pages → {len(all_listings)} listings")
    return all_listings


def run_county(county_slug: str) -> int:
    info = COUNTIES[county_slug]
    print(f"\n{'='*60}")
    print(f"  {county_slug}")
    print(f"{'='*60}")

    existing_rows, seen = load_existing(county_slug)
    before = len(existing_rows)
    added = 0

    for city_name, city_slug in info["cities"].items():
        listings = scrape_city(city_name, city_slug, info)
        for listing in listings:
            firm = listing.get("firm_name", "").strip()
            atty = listing.get("attorney_name", "").strip()
            name = firm if firm else atty
            city = listing.get("city", city_name)

            if not name:
                continue

            key = normalize(name) + "|" + city.lower().strip()
            if key in seen or not normalize(name):
                continue
            seen.add(key)

            existing_rows.append({
                "law_firm_name": name,
                "website": listing.get("website", "") or "",
                "google_business_profile": "",
                "legal_directory_listing": f"https://www.martindale.com/all-lawyers/{city_slug}/kansas/",
                "city": city,
                "state": info["state"],
                "county": info["county"],
                "phone_number": listing.get("phone", "") or "",
                "email": "",
                "practice_area": "General",
                "street_address": "",
                "zip_code": "",
                "msa": info["msa"],
                "priority": "2",
                "number_of_lawyers": "",
            })
            added += 1

    if added > 0:
        save_csv(county_slug, existing_rows)

    print(f"  {county_slug}: {before} → {before + added} (+{added} new)")
    return added


def main():
    target_slugs = list(COUNTIES.keys())
    if len(sys.argv) > 1:
        target_slugs = [s for s in sys.argv[1:] if s in COUNTIES]

    total = 0
    for slug in target_slugs:
        total += run_county(slug)

    print(f"\nTotal new firms from Martindale: {total}")


if __name__ == "__main__":
    main()
