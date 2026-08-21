#!/usr/bin/env python3
"""Targeted FindLaw discovery for KC Metro KS counties.

Scrapes FindLaw for all practice areas × all target cities, filtered to our counties.
Uses scraper/phases/findlaw.py helpers directly.
"""
import csv, re, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from scraper.phases.findlaw import _get, _TOP_PRACTICE_AREAS
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "app/county-data"
BASE = "https://lawyers.findlaw.com"

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

PA_PRIORITY = {
    "Personal Injury": 5, "Family Law": 5, "Criminal Defense": 5,
    "Criminal Law": 5, "Divorce": 5, "Bankruptcy": 4, "Estate Planning": 4,
    "Probate": 4, "Workers Compensation": 4, "Real Estate": 3,
    "Business Law": 3, "Employment Law": 3, "Immigration": 3, "General": 2,
}

COUNTIES = {
    "johnson-county-ks": {
        "county": "Johnson", "state": "KS", "msa": "Kansas City",
        "city_slugs": {
            "overland-park": "Overland Park", "olathe": "Olathe", "shawnee": "Shawnee",
            "lenexa": "Lenexa", "leawood": "Leawood", "prairie-village": "Prairie Village",
            "merriam": "Merriam", "mission": "Mission", "gardner": "Gardner",
            "spring-hill": "Spring Hill", "de-soto": "De Soto",
            "roeland-park": "Roeland Park", "fairway": "Fairway",
            "westwood": "Westwood", "edgerton": "Edgerton",
        },
    },
    "wyandotte-county-ks": {
        "county": "Wyandotte", "state": "KS", "msa": "Kansas City",
        "city_slugs": {
            "kansas-city": "Kansas City",
            "bonner-springs": "Bonner Springs",
            "edwardsville": "Edwardsville",
        },
    },
    "leavenworth-county-ks": {
        "county": "Leavenworth", "state": "KS", "msa": "Kansas City",
        "city_slugs": {
            "leavenworth": "Leavenworth", "lansing": "Lansing",
            "basehor": "Basehor", "tonganoxie": "Tonganoxie",
            "linwood": "Linwood", "easton": "Easton",
        },
    },
    "miami-county-ks": {
        "county": "Miami", "state": "KS", "msa": "Kansas City",
        "city_slugs": {
            "paola": "Paola", "osawatomie": "Osawatomie",
            "louisburg": "Louisburg", "fontana": "Fontana",
        },
    },
    "linn-county-ks": {
        "county": "Linn", "state": "KS", "msa": "Kansas City",
        "city_slugs": {
            "pleasanton": "Pleasanton", "la-cygne": "La Cygne",
            "mound-city": "Mound City", "prescott": "Prescott",
            "blue-mound": "Blue Mound",
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
            for row in csv.DictReader(f):
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


def extract_firms_with_address_validation(soup: BeautifulSoup, pa_name: str, target_cities: set[str]) -> list[dict]:
    """Extract firms from FindLaw listing, validating city from actual address."""
    results = []
    seen_names = set()
    for card in soup.find_all(class_="fl-serp-card"):
        title_el = card.find(class_="fl-serp-card-title")
        if not title_el:
            continue
        name = re.sub(r"Sponsored$", "", title_el.get_text(strip=True)).strip()
        if not name or normalize(name) in seen_names:
            continue

        loc_el = card.find(class_="fl-serp-card-location")
        city = ""
        street = ""
        zipcode = ""
        if loc_el:
            loc_text = loc_el.get_text(strip=True)
            # Pattern: "street, City, ST ZIP" — split from right
            parts = loc_text.rsplit(",", 2)
            if len(parts) == 3:
                street = parts[0].strip()
                city = parts[1].strip()
                state_zip = parts[2].strip()
                zip_m = re.search(r"(\d{5})", state_zip)
                zipcode = zip_m.group(1) if zip_m else ""
                # Validate city against target
                if city.lower() not in target_cities:
                    continue
            elif len(parts) == 2:
                # Could be "City, ST ZIP" (no street)
                city = parts[0].strip()
                if city.lower() not in target_cities:
                    continue
        else:
            # No address info — skip (too risky to assume city)
            continue

        seen_names.add(normalize(name))
        results.append({
            "name": name,
            "city": city,
            "street": street,
            "zip": zipcode,
            "pa": pa_name,
        })
    return results


def run_county(county_slug: str) -> int:
    info = COUNTIES[county_slug]
    target_cities = {c.lower() for c in info["city_slugs"].values()}
    print(f"\n{'='*60}\n  {county_slug}\n{'='*60}")

    # Load existing to build the seen set (dedup), but track new entries separately
    _, seen = load_existing(county_slug)
    new_entries: list[dict] = []  # only entries found THIS session
    pa_added = 0

    for pa_name, pa_slug in _TOP_PRACTICE_AREAS:
        for city_slug, city_name in info["city_slugs"].items():
            url = f"{BASE}/{pa_slug}/kansas/{city_slug}/"
            page = 1
            while page <= 10:
                soup = _get(url, delay=0.8)
                if not soup:
                    break

                firms = extract_firms_with_address_validation(soup, pa_name, target_cities)
                for firm in firms:
                    name = firm["name"].strip()
                    city = firm["city"]
                    if not name:
                        continue
                    key = normalize(name) + "|" + city.lower()
                    if key in seen or not normalize(name):
                        continue
                    seen.add(key)
                    priority = PA_PRIORITY.get(pa_name, 2)
                    new_entries.append({
                        "law_firm_name": name,
                        "website": "",
                        "google_business_profile": "",
                        "legal_directory_listing": url,
                        "city": city,
                        "state": info["state"],
                        "county": info["county"],
                        "phone_number": "",
                        "email": "",
                        "practice_area": pa_name,
                        "street_address": firm.get("street", ""),
                        "zip_code": firm.get("zip", ""),
                        "msa": info["msa"],
                        "priority": str(priority),
                        "number_of_lawyers": "",
                    })
                    pa_added += 1

                next_link = soup.find("a", string=re.compile(r"Next", re.I), href=True)
                if next_link:
                    href = next_link["href"]
                    if not href.startswith("http"):
                        href = f"{BASE}{href}"
                    url = href
                    page += 1
                else:
                    break

        # Merge-safe save: read fresh from disk, append only new entries
        if pa_added > 0:
            disk_rows, disk_seen = load_existing(county_slug)
            additions_to_save = [
                r for r in new_entries
                if (normalize(r.get("law_firm_name", "")) + "|" + r.get("city", "").lower().strip()) not in disk_seen
            ]
            if additions_to_save:
                save_csv(county_slug, disk_rows + additions_to_save)

    total_added = len(new_entries)
    print(f"  {county_slug}: +{total_added} new from FindLaw")
    return total_added


def main():
    target_slugs = list(COUNTIES.keys())
    if len(sys.argv) > 1:
        target_slugs = [s for s in sys.argv[1:] if s in COUNTIES]

    total = 0
    for slug in target_slugs:
        total += run_county(slug)

    print(f"\nTotal new firms from FindLaw: {total}")


if __name__ == "__main__":
    main()
