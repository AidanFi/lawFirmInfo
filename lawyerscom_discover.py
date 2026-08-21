#!/usr/bin/env python3
"""Lawyers.com (Martindale-Hubbell) law firm discovery for KC Metro KS counties."""
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

# (city_slug, county_slug, county_name, canonical_city_name)
CITY_TARGETS = [
    # Johnson County
    ("overland-park", "johnson-county-ks", "Johnson", "Overland Park"),
    ("olathe", "johnson-county-ks", "Johnson", "Olathe"),
    ("shawnee", "johnson-county-ks", "Johnson", "Shawnee"),
    ("lenexa", "johnson-county-ks", "Johnson", "Lenexa"),
    ("leawood", "johnson-county-ks", "Johnson", "Leawood"),
    ("prairie-village", "johnson-county-ks", "Johnson", "Prairie Village"),
    ("merriam", "johnson-county-ks", "Johnson", "Merriam"),
    ("mission", "johnson-county-ks", "Johnson", "Mission"),
    ("gardner", "johnson-county-ks", "Johnson", "Gardner"),
    ("spring-hill", "johnson-county-ks", "Johnson", "Spring Hill"),
    ("de-soto", "johnson-county-ks", "Johnson", "De Soto"),
    ("roeland-park", "johnson-county-ks", "Johnson", "Roeland Park"),
    ("fairway", "johnson-county-ks", "Johnson", "Fairway"),
    ("westwood", "johnson-county-ks", "Johnson", "Westwood"),
    ("edgerton", "johnson-county-ks", "Johnson", "Edgerton"),
    ("shawnee-mission", "johnson-county-ks", "Johnson", "Shawnee"),
    # Wyandotte County
    ("kansas-city", "wyandotte-county-ks", "Wyandotte", "Kansas City"),
    ("bonner-springs", "wyandotte-county-ks", "Wyandotte", "Bonner Springs"),
    ("edwardsville", "wyandotte-county-ks", "Wyandotte", "Edwardsville"),
    # Leavenworth County
    ("leavenworth", "leavenworth-county-ks", "Leavenworth", "Leavenworth"),
    ("lansing", "leavenworth-county-ks", "Leavenworth", "Lansing"),
    ("basehor", "leavenworth-county-ks", "Leavenworth", "Basehor"),
    ("tonganoxie", "leavenworth-county-ks", "Leavenworth", "Tonganoxie"),
    ("fort-leavenworth", "leavenworth-county-ks", "Leavenworth", "Fort Leavenworth"),
    # Miami County
    ("paola", "miami-county-ks", "Miami", "Paola"),
    ("osawatomie", "miami-county-ks", "Miami", "Osawatomie"),
    ("louisburg", "miami-county-ks", "Miami", "Louisburg"),
    # Linn County
    ("mound-city", "linn-county-ks", "Linn", "Mound City"),
    ("pleasanton", "linn-county-ks", "Linn", "Pleasanton"),
    ("la-cygne", "linn-county-ks", "Linn", "La Cygne"),
    # Douglas County
    ("lawrence", "douglas-county-ks", "Douglas", "Lawrence"),
    ("eudora", "douglas-county-ks", "Douglas", "Eudora"),
    ("baldwin-city", "douglas-county-ks", "Douglas", "Baldwin City"),
    # Franklin County
    ("ottawa", "franklin-county-ks", "Franklin", "Ottawa"),
    ("wellsville", "franklin-county-ks", "Franklin", "Wellsville"),
    # Jefferson County
    ("oskaloosa", "jefferson-county-ks", "Jefferson", "Oskaloosa"),
    ("valley-falls", "jefferson-county-ks", "Jefferson", "Valley Falls"),
    ("perry", "jefferson-county-ks", "Jefferson", "Perry"),
    # Osage County KS
    ("lyndon", "osage-county-ks", "Osage", "Lyndon"),
    ("osage-city", "osage-county-ks", "Osage", "Osage City"),
    ("burlingame", "osage-county-ks", "Osage", "Burlingame"),
    ("overbrook", "osage-county-ks", "Osage", "Overbrook"),
    # Shawnee County
    ("topeka", "shawnee-county-ks", "Shawnee", "Topeka"),
    ("auburn", "shawnee-county-ks", "Shawnee", "Auburn"),
    ("rossville", "shawnee-county-ks", "Shawnee", "Rossville"),
    ("silver-lake", "shawnee-county-ks", "Shawnee", "Silver Lake"),
]

COUNTY_CITIES = {
    "johnson-county-ks": {"overland park","olathe","shawnee","lenexa","leawood","prairie village","merriam","mission","gardner","spring hill","de soto","roeland park","fairway","westwood","edgerton"},
    "wyandotte-county-ks": {"kansas city","bonner springs","edwardsville"},
    "leavenworth-county-ks": {"leavenworth","lansing","basehor","tonganoxie","linwood","easton","fort leavenworth"},
    "miami-county-ks": {"paola","osawatomie","louisburg","fontana"},
    "linn-county-ks": {"pleasanton","la cygne","mound city","prescott","blue mound"},
    "douglas-county-ks": {"lawrence","eudora","baldwin city","lecompton"},
    "franklin-county-ks": {"ottawa","wellsville","williamsburg","richmond","lane"},
    "jefferson-county-ks": {"oskaloosa","winchester","valley falls","meriden","mclouth","perry","nortonville"},
    "osage-county-ks": {"lyndon","osage city","burlingame","overbrook","scranton"},
    "shawnee-county-ks": {"topeka","silver lake","rossville","willard","auburn","wakarusa","tecumseh"},
}

COUNTY_INFO = {
    "johnson-county-ks": ("Johnson", "Kansas City"),
    "wyandotte-county-ks": ("Wyandotte", "Kansas City"),
    "leavenworth-county-ks": ("Leavenworth", "Kansas City"),
    "miami-county-ks": ("Miami", "Kansas City"),
    "linn-county-ks": ("Linn", "Kansas City"),
    "douglas-county-ks": ("Douglas", "Lawrence"),
    "franklin-county-ks": ("Franklin", "Kansas City"),
    "jefferson-county-ks": ("Jefferson", "Topeka"),
    "osage-county-ks": ("Osage", "Topeka"),
    "shawnee-county-ks": ("Shawnee", "Topeka"),
}


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd)\b", "", name)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def load_fresh(county_slug: str) -> tuple[list[dict], set[str]]:
    path = DATA_DIR / f"{county_slug}.csv"
    rows, seen = [], set()
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append(row)
                key = normalize(row.get("law_firm_name", "")) + "|" + row.get("city", "").lower().strip()
                seen.add(key)
    return rows, seen


def save_merged(county_slug: str, new_entries: list[dict]) -> int:
    """Merge-safe save: reload disk, append only new entries."""
    disk_rows, disk_seen = load_fresh(county_slug)
    adds = [e for e in new_entries if (normalize(e.get("law_firm_name","")) + "|" + e.get("city","").lower()) not in disk_seen]
    if not adds:
        return 0
    path = DATA_DIR / f"{county_slug}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(disk_rows + adds)
    return len(adds)


def scrape_city(session, city_slug: str, county_slug: str, city_name: str, seen: set, new_entries: list) -> int:
    base_url = f"https://www.lawyers.com/all-legal-issues/{city_slug}/kansas/law-firms/"
    target_cities = COUNTY_CITIES[county_slug]
    added = 0

    for page in range(1, 50):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            r = session.get(url, timeout=15)
        except Exception as e:
            print(f"    Error {city_slug} p{page}: {e}")
            break

        if r.status_code == 429:
            print(f"    {city_slug} p{page}: 429, waiting 90s")
            time.sleep(90)
            try:
                r = session.get(url, timeout=15)
            except:
                break
            if r.status_code != 200:
                break

        if r.status_code == 404:
            break
        if r.status_code != 200:
            print(f"    {city_slug} p{page}: {r.status_code}")
            break

        soup = BeautifulSoup(r.text, "lxml")
        cards = soup.find_all("div", class_="profile-card-container")

        if not cards:
            break

        for card in cards:
            text = card.get_text(separator="|", strip=True)

            # Get firm name from profile-link
            name_el = card.find("a", class_=re.compile(r"profile-link"))
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or not normalize(name):
                continue

            # Parse address: "Street, City, KS ZIP" format
            addr_m = re.search(r"\|([^|]+),\s*([A-Za-z][A-Za-z\s]+),\s*KS\s+(\d{5})\|", text)
            if not addr_m:
                continue  # Skip "Serving County" entries with no actual address

            street = addr_m.group(1).strip()
            city = addr_m.group(2).strip()
            zipcode = addr_m.group(3)

            # Validate city
            if city.lower() not in target_cities:
                continue

            # Phone
            phone_m = re.search(r"\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}", text)
            phone = phone_m.group() if phone_m else ""

            # Website
            web_link = card.find("a", href=re.compile(r"^https?://(?!www\.lawyers\.com)"))
            website = web_link.get("href", "") if web_link else ""

            # Practice area
            pa_m = re.search(r"Law Firm with \d+ lawyer[s]?\|([^|]+)\|", text)
            pa = pa_m.group(1).strip() if pa_m else "General"
            if len(pa) > 100:
                pa = "General"

            key = normalize(name) + "|" + city.lower()
            if key in seen or not normalize(name):
                continue
            seen.add(key)

            county_name, msa = COUNTY_INFO[county_slug]
            new_entries.append({
                "law_firm_name": name,
                "website": website,
                "google_business_profile": "",
                "legal_directory_listing": card.find("a", class_=re.compile(r"profile-link"), href=True).get("href", url) if card.find("a", class_=re.compile(r"profile-link"), href=True) else url,
                "city": city,
                "state": "KS",
                "county": county_name,
                "phone_number": phone,
                "email": "",
                "practice_area": pa[:100],
                "street_address": street,
                "zip_code": zipcode,
                "msa": msa,
                "priority": "2",
                "number_of_lawyers": "",
            })
            added += 1

        # Check for next page
        has_next = bool(soup.find("a", href=re.compile(rf"page={page+1}")))
        if not has_next:
            break
        time.sleep(2.0)

    return added


def main():
    session = creq.Session(impersonate="chrome120")

    # Load initial dedup state
    county_seen: dict[str, set] = {}
    county_new: dict[str, list] = {}
    for slug in COUNTY_INFO:
        _, seen = load_fresh(slug)
        county_seen[slug] = seen
        county_new[slug] = []
        print(f"  Loaded {slug}: {len(seen)} existing")

    added_by_county: dict[str, int] = {slug: 0 for slug in COUNTY_INFO}

    for city_slug, county_slug, county_name, city_name in CITY_TARGETS:
        print(f"  Scraping {city_slug}...")
        n = scrape_city(session, city_slug, county_slug, city_name,
                        county_seen[county_slug], county_new[county_slug])
        added_by_county[county_slug] += n
        if n > 0:
            saved = save_merged(county_slug, county_new[county_slug])
            print(f"    {city_slug}: +{n} new (saved {saved})")
        time.sleep(2.0)

    print("\n--- Results ---")
    for slug in COUNTY_INFO:
        if added_by_county[slug] > 0:
            print(f"  {slug}: +{added_by_county[slug]} from Lawyers.com")

    print(f"\nTotal: +{sum(added_by_county.values())}")


if __name__ == "__main__":
    main()
