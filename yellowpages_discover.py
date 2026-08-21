#!/usr/bin/env python3
"""Yellow Pages attorney/law firm discovery for KC Metro KS counties."""
import csv, re, time, sys
from pathlib import Path
from curl_cffi import requests as creq
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "app/county-data"

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

# city_slug → (county_slug, county_name, canonical_city_name)
CITY_TARGETS = {
    # Johnson County
    "overland-park-ks": ("johnson-county-ks", "Johnson", "Overland Park"),
    "olathe-ks": ("johnson-county-ks", "Johnson", "Olathe"),
    "shawnee-ks": ("johnson-county-ks", "Johnson", "Shawnee"),
    "lenexa-ks": ("johnson-county-ks", "Johnson", "Lenexa"),
    "leawood-ks": ("johnson-county-ks", "Johnson", "Leawood"),
    "prairie-village-ks": ("johnson-county-ks", "Johnson", "Prairie Village"),
    "merriam-ks": ("johnson-county-ks", "Johnson", "Merriam"),
    "mission-ks": ("johnson-county-ks", "Johnson", "Mission"),
    "gardner-ks": ("johnson-county-ks", "Johnson", "Gardner"),
    "spring-hill-ks": ("johnson-county-ks", "Johnson", "Spring Hill"),
    "de-soto-ks": ("johnson-county-ks", "Johnson", "De Soto"),
    "roeland-park-ks": ("johnson-county-ks", "Johnson", "Roeland Park"),
    "fairway-ks": ("johnson-county-ks", "Johnson", "Fairway"),
    "westwood-ks": ("johnson-county-ks", "Johnson", "Westwood"),
    "edgerton-ks": ("johnson-county-ks", "Johnson", "Edgerton"),
    # Wyandotte County
    "kansas-city-ks": ("wyandotte-county-ks", "Wyandotte", "Kansas City"),
    "bonner-springs-ks": ("wyandotte-county-ks", "Wyandotte", "Bonner Springs"),
    "edwardsville-ks": ("wyandotte-county-ks", "Wyandotte", "Edwardsville"),
    # Leavenworth County
    "leavenworth-ks": ("leavenworth-county-ks", "Leavenworth", "Leavenworth"),
    "lansing-ks": ("leavenworth-county-ks", "Leavenworth", "Lansing"),
    "basehor-ks": ("leavenworth-county-ks", "Leavenworth", "Basehor"),
    "tonganoxie-ks": ("leavenworth-county-ks", "Leavenworth", "Tonganoxie"),
    # Miami County
    "paola-ks": ("miami-county-ks", "Miami", "Paola"),
    "osawatomie-ks": ("miami-county-ks", "Miami", "Osawatomie"),
    "louisburg-ks": ("miami-county-ks", "Miami", "Louisburg"),
    # Linn County
    "mound-city-ks": ("linn-county-ks", "Linn", "Mound City"),
    "pleasanton-ks": ("linn-county-ks", "Linn", "Pleasanton"),
    "la-cygne-ks": ("linn-county-ks", "Linn", "La Cygne"),
    # Douglas County
    "lawrence-ks": ("douglas-county-ks", "Douglas", "Lawrence"),
    "eudora-ks": ("douglas-county-ks", "Douglas", "Eudora"),
    "baldwin-city-ks": ("douglas-county-ks", "Douglas", "Baldwin City"),
    # Franklin County
    "ottawa-ks": ("franklin-county-ks", "Franklin", "Ottawa"),
    "wellsville-ks": ("franklin-county-ks", "Franklin", "Wellsville"),
    # Jefferson County
    "oskaloosa-ks": ("jefferson-county-ks", "Jefferson", "Oskaloosa"),
    "valley-falls-ks": ("jefferson-county-ks", "Jefferson", "Valley Falls"),
    "perry-ks": ("jefferson-county-ks", "Jefferson", "Perry"),
    "meriden-ks": ("jefferson-county-ks", "Jefferson", "Meriden"),
    # Osage County KS
    "lyndon-ks": ("osage-county-ks", "Osage", "Lyndon"),
    "osage-city-ks": ("osage-county-ks", "Osage", "Osage City"),
    "burlingame-ks": ("osage-county-ks", "Osage", "Burlingame"),
    "overbrook-ks": ("osage-county-ks", "Osage", "Overbrook"),
    # Shawnee County
    "topeka-ks": ("shawnee-county-ks", "Shawnee", "Topeka"),
    "silver-lake-ks": ("shawnee-county-ks", "Shawnee", "Silver Lake"),
    "rossville-ks": ("shawnee-county-ks", "Shawnee", "Rossville"),
    "auburn-ks": ("shawnee-county-ks", "Shawnee", "Auburn"),
}

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd)\b", "", name)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def load_all_counties() -> dict[str, tuple[list[dict], set[str]]]:
    counties = {}
    for county_slug in set(v[0] for v in CITY_TARGETS.values()):
        path = DATA_DIR / f"{county_slug}.csv"
        rows, seen = [], set()
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    rows.append(row)
                    key = normalize(row.get("law_firm_name", "")) + "|" + row.get("city", "").lower().strip()
                    seen.add(key)
        counties[county_slug] = (rows, seen)
        print(f"  Loaded {county_slug}: {len(rows)} existing")
    return counties


def load_county_fresh(county_slug: str) -> tuple[list[dict], set[str]]:
    return load_all_counties_slug(county_slug)


def load_all_counties_slug(county_slug: str) -> tuple[list[dict], set[str]]:
    path = DATA_DIR / f"{county_slug}.csv"
    rows, seen = [], set()
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append(row)
                key = normalize(row.get("law_firm_name", "")) + "|" + row.get("city", "").lower().strip()
                seen.add(key)
    return rows, seen


def save_county(county_slug: str, rows: list[dict]) -> None:
    """Merge-safe save: reload disk state, append only new entries."""
    disk_rows, disk_seen = load_all_counties_slug(county_slug)
    additions = [
        r for r in rows
        if (normalize(r.get("law_firm_name", "")) + "|" + r.get("city", "").lower().strip()) not in disk_seen
    ]
    merged = disk_rows + additions
    path = DATA_DIR / f"{county_slug}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)


def scrape_city(session, city_slug: str, county_slug: str, county_name: str, city_name: str,
                seen: set, new_entries: list, delay: float = 2.5) -> int:
    base_url = f"https://www.yellowpages.com/{city_slug}/attorneys"
    added = 0
    rows = new_entries  # alias for appending

    for page in range(1, 8):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            r = session.get(url, headers=HEADERS, timeout=15)
        except Exception as e:
            print(f"    {city_slug} p{page}: request error {e}")
            break

        if r.status_code == 429:
            print(f"    {city_slug} p{page}: 429, waiting 90s")
            time.sleep(90)
            try:
                r = session.get(url, headers=HEADERS, timeout=15)
            except Exception:
                break
            if r.status_code != 200:
                break

        if r.status_code != 200:
            break

        # Extract business data from raw HTML (regex since BeautifulSoup shows empty on bot protection)
        # Try HTML parse first
        soup = BeautifulSoup(r.text, "lxml")
        v_cards = soup.find_all(class_="v-card")

        if v_cards:
            for card in v_cards:
                name_el = card.find("a", class_="business-name")
                phone_el = card.find(class_="phone")
                street_el = card.find(class_="street-address")
                locality_el = card.find(class_="locality")
                website_el = card.find("a", class_=re.compile(r"website|visit-website"), href=re.compile(r"^http"))

                name = name_el.get_text(strip=True) if name_el else ""
                if not name:
                    continue

                phone = phone_el.get_text(strip=True) if phone_el else ""
                street = street_el.get_text(strip=True) if street_el else ""
                locality_raw = locality_el.get_text(strip=True) if locality_el else ""
                website = website_el.get("href", "") if website_el else ""

                # Parse locality "City, ST ZIP"
                loc_m = re.match(r"^(.+),\s*([A-Z]{2})\s*([\d-]+)?", locality_raw)
                if loc_m:
                    card_city = loc_m.group(1).strip()
                    card_state = loc_m.group(2).strip()
                    if card_state != "KS":
                        continue
                    if card_city.lower() != city_name.lower():
                        continue
                else:
                    card_city = city_name

                key = normalize(name) + "|" + card_city.lower()
                if key in seen or not normalize(name):
                    continue
                seen.add(key)
                rows.append({
                    "law_firm_name": name,
                    "website": website,
                    "google_business_profile": "",
                    "legal_directory_listing": f"https://www.yellowpages.com/{city_slug}/attorneys",
                    "city": card_city,
                    "state": "KS",
                    "county": county_name,
                    "phone_number": phone,
                    "email": "",
                    "practice_area": "General",
                    "street_address": street,
                    "zip_code": "",
                    "msa": "Kansas City",
                    "priority": "2",
                    "number_of_lawyers": "",
                })
                added += 1
        else:
            # Fallback: regex extraction from raw HTML
            names = re.findall(r'class="business-name"[^>]*>[^<]*<[^>]*>([^<]+)<', r.text)
            phones = re.findall(r'class="phone"[^>]*>([^<]+)<', r.text)
            streets = re.findall(r'class="street-address"[^>]*>([^<]+)<', r.text)
            localities = re.findall(r'class="locality"[^>]*>([^<]+)<', r.text)

            for i, name in enumerate(names):
                name = name.strip()
                if not name:
                    continue
                phone = phones[i].strip() if i < len(phones) else ""
                street = streets[i].strip() if i < len(streets) else ""
                locality_raw = localities[i].strip() if i < len(localities) else ""

                loc_m = re.match(r"^(.+),\s*([A-Z]{2})", locality_raw)
                if loc_m and loc_m.group(2) != "KS":
                    continue

                key = normalize(name) + "|" + city_name.lower()
                if key in seen or not normalize(name):
                    continue
                seen.add(key)
                rows.append({
                    "law_firm_name": name,
                    "website": "",
                    "google_business_profile": "",
                    "legal_directory_listing": f"https://www.yellowpages.com/{city_slug}/attorneys",
                    "city": city_name,
                    "state": "KS",
                    "county": county_name,
                    "phone_number": phone,
                    "email": "",
                    "practice_area": "General",
                    "street_address": street,
                    "zip_code": "",
                    "msa": "Kansas City",
                    "priority": "2",
                    "number_of_lawyers": "",
                })
                added += 1

        # Check for next page
        if "Next" not in r.text and f"page={page+1}" not in r.text:
            break

        time.sleep(delay)

    return added


def main():
    print("Loading existing county data...")
    # Build dedup sets from current disk state
    county_seen: dict[str, set] = {}
    county_new: dict[str, list] = {}
    all_slugs = set(v[0] for v in CITY_TARGETS.values())
    for slug in all_slugs:
        path = DATA_DIR / f"{slug}.csv"
        seen = set()
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    key = normalize(row.get("law_firm_name", "")) + "|" + row.get("city", "").lower().strip()
                    seen.add(key)
        county_seen[slug] = seen
        county_new[slug] = []
        print(f"  Loaded {slug}: {len(seen)} existing")

    session = creq.Session(impersonate="chrome120")
    added_by_county: dict[str, int] = {slug: 0 for slug in all_slugs}

    for city_slug, (county_slug, county_name, city_name) in CITY_TARGETS.items():
        print(f"  Scraping {city_slug}...")
        n = scrape_city(session, city_slug, county_slug, county_name, city_name,
                        county_seen[county_slug], county_new[county_slug])
        added_by_county[county_slug] += n
        if n > 0:
            save_county(county_slug, county_new[county_slug])
        time.sleep(2.5)

    print("\n--- Results ---")
    for slug in all_slugs:
        print(f"  {slug}: +{added_by_county[slug]} new from Yellow Pages")

    print(f"\nTotal new: {sum(added_by_county.values())}")


if __name__ == "__main__":
    main()
