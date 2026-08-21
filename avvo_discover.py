#!/usr/bin/env python3
"""Avvo attorney discovery for KC Metro KS counties using chrome120 impersonation."""
import csv, json, re, sys, time
from pathlib import Path

from curl_cffi import requests as creq
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "app/county-data"

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

PRIORITY_MAP = {
    "Personal Injury": 5, "Medical Malpractice": 5, "Workers Compensation": 5,
    "Criminal Defense": 5, "DUI/DWI": 5, "Family": 5, "Divorce": 5,
    "Bankruptcy": 4, "Estate Planning": 4, "Probate": 4, "Real Estate": 4,
    "Business": 4, "Employment": 4, "Immigration": 4,
    "Civil Rights": 3, "Social Security Disability": 3, "Tax": 3,
    "Intellectual Property": 3, "General": 2,
}

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
    "douglas-county-ks": {
        "county": "Douglas", "state": "KS", "msa": "Lawrence",
        "cities": ["Lawrence", "Eudora", "Baldwin City", "Lecompton"],
    },
    "franklin-county-ks": {
        "county": "Franklin", "state": "KS", "msa": "Kansas City",
        "cities": ["Ottawa", "Wellsville", "Williamsburg", "Richmond", "Lane"],
    },
    "jefferson-county-ks": {
        "county": "Jefferson", "state": "KS", "msa": "Topeka",
        "cities": ["Oskaloosa", "Winchester", "Valley Falls", "Meriden", "McLouth", "Perry", "Nortonville"],
    },
    "osage-county-ks": {
        "county": "Osage", "state": "KS", "msa": "Topeka",
        "cities": ["Lyndon", "Osage City", "Burlingame", "Overbrook", "Scranton"],
    },
    "shawnee-county-ks": {
        "county": "Shawnee", "state": "KS", "msa": "Topeka",
        "cities": ["Topeka", "Silver Lake", "Rossville", "Willard", "Auburn", "Wakarusa", "Tecumseh"],
    },
}

# All cities across all counties (lowercase) → county slug
ALL_CITY_TO_COUNTY = {}
for slug, info in COUNTIES.items():
    for city in info["cities"]:
        ALL_CITY_TO_COUNTY[city.lower()] = slug

# Avvo city slug: lowercase, spaces to underscores
# Special case: "Kansas City" on Avvo searches include KS and MO — we validate by state
CITY_SLUGS = {
    "Overland Park": "overland_park",
    "Olathe": "olathe",
    "Shawnee": "shawnee",
    "Lenexa": "lenexa",
    "Leawood": "leawood",
    "Prairie Village": "prairie_village",
    "Merriam": "merriam",
    "Mission": "mission",
    "Gardner": "gardner",
    "Spring Hill": "spring_hill",
    "De Soto": "de_soto",
    "Roeland Park": "roeland_park",
    "Fairway": "fairway",
    "Westwood": "westwood",
    "Edgerton": "edgerton",
    "Kansas City": "kansas_city",
    "Bonner Springs": "bonner_springs",
    "Edwardsville": "edwardsville",
    "Leavenworth": "leavenworth",
    "Lansing": "lansing",
    "Basehor": "basehor",
    "Tonganoxie": "tonganoxie",
    "Linwood": "linwood",
    "Easton": "easton",
    "Paola": "paola",
    "Osawatomie": "osawatomie",
    "Louisburg": "louisburg",
    "Fontana": "fontana",
    "Pleasanton": "pleasanton",
    "La Cygne": "la_cygne",
    "Mound City": "mound_city",
    "Prescott": "prescott",
    "Blue Mound": "blue_mound",
    # Douglas County
    "Lawrence": "lawrence",
    "Eudora": "eudora",
    "Baldwin City": "baldwin_city",
    "Lecompton": "lecompton",
    # Franklin County
    "Ottawa": "ottawa",
    "Wellsville": "wellsville",
    "Williamsburg": "williamsburg",
    "Richmond": "richmond",
    "Lane": "lane",
    # Jefferson County
    "Oskaloosa": "oskaloosa",
    "Winchester": "winchester",
    "Valley Falls": "valley_falls",
    "Meriden": "meriden",
    "McLouth": "mclouth",
    "Perry": "perry",
    "Nortonville": "nortonville",
    # Osage County KS
    "Lyndon": "lyndon",
    "Osage City": "osage_city",
    "Burlingame": "burlingame",
    "Overbrook": "overbrook",
    "Scranton": "scranton",
    # Shawnee County
    "Topeka": "topeka",
    "Silver Lake": "silver_lake",
    "Rossville": "rossville",
    "Willard": "willard",
    "Auburn": "auburn",
    "Wakarusa": "wakarusa",
    "Tecumseh": "tecumseh",
}


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers)\b", "", name)
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


def extract_attorneys_from_page(html: str) -> list[dict]:
    """Extract attorney data from JSON-LD blocks in Avvo page HTML."""
    soup = BeautifulSoup(html, "lxml")
    attorneys = []
    for block in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(block.string)
            if data.get("@type") == "Person" and data.get("name"):
                addr = data.get("worksFor", {}).get("address", {})
                attorneys.append({
                    "name": data["name"],
                    "firm": data.get("worksFor", {}).get("name", data["name"]),
                    "phone": data.get("worksFor", {}).get("telephone") or "",
                    "street": addr.get("streetAddress") or "",
                    "city": addr.get("addressLocality") or "",
                    "state": addr.get("addressRegion") or "",
                    "zip": addr.get("postalCode") or "",
                })
        except Exception:
            continue
    return attorneys


def scrape_city(session, city: str, target_cities_lower: set[str], delay: float = 0.8) -> list[dict]:
    """Fetch all Avvo pages for a city and return city-validated attorneys."""
    slug = CITY_SLUGS.get(city)
    if not slug:
        print(f"  No Avvo slug for {city}, skipping")
        return []

    base_url = f"https://www.avvo.com/all-lawyers/ks/{slug}.html"
    results = []
    page = 1
    consecutive_empty = 0

    while True:
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 429:
                wait = 90
                print(f"  {city} p{page}: rate limited, waiting {wait}s")
                time.sleep(wait)
                r = session.get(url, timeout=20)
            if r.status_code != 200:
                print(f"  {city} p{page}: HTTP {r.status_code}, stopping")
                break
            attorneys = extract_attorneys_from_page(r.text)
            if not attorneys:
                break

            valid = [
                a for a in attorneys
                if a["city"].lower().strip() in target_cities_lower
                and a["state"].upper() == "KS"
            ]
            results.extend(valid)

            if len(valid) == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
            else:
                consecutive_empty = 0

            page += 1
            time.sleep(delay)
        except Exception as e:
            print(f"  Error on {city} p{page}: {e}")
            break

    print(f"  {city}: {page-1} pages → {len(results)} valid attorneys")
    return results


def attorney_to_row(a: dict, county_info: dict) -> dict:
    city = a["city"].title() if a["city"].isupper() else a["city"]
    firm = a["firm"].strip()
    return {
        "law_firm_name": firm,
        "website": "",
        "google_business_profile": "",
        "legal_directory_listing": f"https://www.avvo.com/all-lawyers/ks/{CITY_SLUGS.get(city, city.lower().replace(' ', '_'))}.html",
        "city": city,
        "state": county_info["state"],
        "county": county_info["county"],
        "phone_number": a["phone"],
        "email": "",
        "practice_area": "General",
        "street_address": a["street"],
        "zip_code": a["zip"],
        "msa": county_info["msa"],
        "priority": str(PRIORITY_MAP.get("General", 2)),
        "number_of_lawyers": "",
    }


def run_county(county_slug: str, session) -> int:
    info = COUNTIES[county_slug]
    cities = info["cities"]
    target_cities_lower = {c.lower() for c in cities}

    print(f"\n{'='*60}")
    print(f"  {county_slug}")
    print(f"{'='*60}")

    existing_rows, seen = load_existing(county_slug)
    before = len(existing_rows)
    added = 0

    for city in cities:
        attorneys = scrape_city(session, city, target_cities_lower)
        for a in attorneys:
            firm = a["firm"].strip()
            city_norm = a["city"].lower().strip()
            key = normalize(firm) + "|" + city_norm
            if key in seen or not normalize(firm):
                continue
            seen.add(key)
            existing_rows.append(attorney_to_row(a, info))
            added += 1

    if added > 0:
        save_csv(county_slug, existing_rows)

    print(f"  {county_slug}: {before} → {before + added} (+{added} new)")
    return added


def main():
    target_slugs = list(COUNTIES.keys())
    if len(sys.argv) > 1:
        target_slugs = [s for s in sys.argv[1:] if s in COUNTIES]

    session = creq.Session(impersonate="chrome120")
    total_added = 0

    for slug in target_slugs:
        added = run_county(slug, session)
        total_added += added

    print(f"\nTotal new firms added: {total_added}")


if __name__ == "__main__":
    main()
