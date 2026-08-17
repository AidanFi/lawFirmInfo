#!/usr/bin/env python3
"""KS Supreme Court Attorney Registry scraper.

Fetches ALL active/inactive attorneys from directory-kard.kscourts.gov,
then filters by city to our 5 target counties. This is the authoritative source.
"""
import csv, re, sys, time
from pathlib import Path

from curl_cffi import requests as creq
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "app/county-data"
CACHE_DIR = Path(__file__).parent / ".kscourts_cache"
CACHE_DIR.mkdir(exist_ok=True)

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

BASE_URL = "https://directory-kard.kscourts.gov"
VALID_STATUSES = {"Active", "Inactive"}

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
    # ---- SE Kansas counties ----
    "allen-county-ks": {
        "county": "Allen", "state": "KS", "msa": "",
        "cities": ["Iola", "Humboldt", "Moran", "Gas", "Elsmore", "Laharpe", "La Harpe", "Savonburg", "Carlyle"],
    },
    "bourbon-county-ks": {
        "county": "Bourbon", "state": "KS", "msa": "",
        "cities": ["Fort Scott", "Uniontown", "Bronson", "Mapleton", "Garland", "Redfield"],
    },
    "chautauqua-county-ks": {
        "county": "Chautauqua", "state": "KS", "msa": "",
        "cities": ["Sedan", "Cedar Vale", "Niotaze", "Peru", "Hewins", "Elgin", "Chautauqua"],
    },
    "cherokee-county-ks": {
        "county": "Cherokee", "state": "KS", "msa": "",
        "cities": ["Columbus", "Baxter Springs", "Galena", "Riverton", "Weir", "Scammon", "Crestline", "Treece"],
    },
    "coffey-county-ks": {
        "county": "Coffey", "state": "KS", "msa": "",
        "cities": ["Burlington", "Lebo", "Le Roy", "LeRoy", "New Strawn", "Waverly", "Gridley"],
    },
    "crawford-county-ks": {
        "county": "Crawford", "state": "KS", "msa": "Pittsburg",
        "cities": ["Pittsburg", "Girard", "Frontenac", "Cherokee", "Arma", "Mc Cune", "McCune", "Mulberry", "Hepler", "Arcadia", "Franklin", "West Mineral"],
    },
    "elk-county-ks": {
        "county": "Elk", "state": "KS", "msa": "",
        "cities": ["Howard", "Longton", "Moline", "Grenola", "Elk Falls", "Busby"],
    },
    "greenwood-county-ks": {
        "county": "Greenwood", "state": "KS", "msa": "",
        "cities": ["Eureka", "Fall River", "Hamilton", "Madison", "Severy", "Virgil"],
    },
    "labette-county-ks": {
        "county": "Labette", "state": "KS", "msa": "",
        "cities": ["Parsons", "Oswego", "Altamont", "Chetopa", "Edna", "Labette", "Mound Valley", "Dennis", "Bartlett"],
    },
    "montgomery-county-ks": {
        "county": "Montgomery", "state": "KS", "msa": "",
        "cities": ["Independence", "Coffeyville", "Caney", "Cherryvale", "Elk City", "Havana", "Sycamore", "Tyro", "Dearing", "Dora", "Liberty", "Lenapah"],
    },
    "neosho-county-ks": {
        "county": "Neosho", "state": "KS", "msa": "",
        "cities": ["Erie", "Chanute", "St. Paul", "Galesburg", "Stark", "Thayer", "Earlton", "Dennis", "Furley"],
    },
    "wilson-county-ks": {
        "county": "Wilson", "state": "KS", "msa": "",
        "cities": ["Fredonia", "Neodesha", "Altoona", "Buffalo", "Coyville", "Benedict", "Roper"],
    },
    "woodson-county-ks": {
        "county": "Woodson", "state": "KS", "msa": "",
        "cities": ["Yates Center", "Toronto", "Neosho Falls", "Piqua", "Kalida"],
    },
    # ---- Previously-missed counties ----
    "marshall-county-ks": {
        "county": "Marshall", "state": "KS", "msa": "",
        "cities": ["Marysville", "Frankfort", "Blue Rapids", "Waterville", "Beattie",
                   "Axtell", "Summerfield", "Vermillion", "Bremen", "Home"],
    },
    "harper-county-ks": {
        "county": "Harper", "state": "KS", "msa": "",
        "cities": ["Anthony", "Attica", "Harper", "Danville"],
    },
    "wichita-county-ks": {
        "county": "Wichita", "state": "KS", "msa": "",
        "cities": ["Leoti", "Marienthal", "Selkirk"],
    },
}

# Build city → county index (lowercase)
CITY_TO_COUNTY = {}
for slug, info in COUNTIES.items():
    for city in info["cities"]:
        CITY_TO_COUNTY[city.lower()] = slug


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


def get_all_attorney_ids(session) -> list[tuple[str, str]]:
    """Get all Active/Inactive attorney registration numbers by searching A-Z."""
    # Search for letters that cover all last names
    letters = "abcdefghijklmnopqrstuvwxyz"
    seen_ids = set()
    results = []

    for letter in letters:
        # Get fresh token each time
        r1 = session.get(f"{BASE_URL}/", timeout=15)
        soup1 = BeautifulSoup(r1.text, "lxml")
        token = soup1.find("input", {"name": "__RequestVerificationToken"})
        if not token:
            print(f"  Could not get token for letter {letter}, skipping")
            continue

        r2 = session.post(
            f"{BASE_URL}/Search",
            data={"__RequestVerificationToken": token["value"], "RegNum": "", "LastName": letter, "FirstName": ""},
            timeout=60,
        )
        if r2.status_code != 200:
            print(f"  Letter {letter}: HTTP {r2.status_code}")
            time.sleep(5)
            continue

        soup2 = BeautifulSoup(r2.text, "lxml")
        table = soup2.find("table")
        if not table:
            print(f"  Letter {letter}: no table")
            continue

        rows = table.find_all("tr")[1:]  # skip header
        letter_count = 0
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            name = cells[0].get_text(strip=True)
            status = cells[1].get_text(strip=True)
            if status not in VALID_STATUSES:
                continue
            link = cells[2].find("a")
            if not link:
                continue
            href = link.get("href", "")
            reg_num = re.search(r"regNum=(\d+)", href)
            if not reg_num:
                continue
            reg_id = reg_num.group(1)
            if reg_id not in seen_ids:
                seen_ids.add(reg_id)
                results.append((reg_id, name))
                letter_count += 1

        print(f"  Letter {letter}: {letter_count} new active/inactive (total: {len(results)})")
        time.sleep(1)

    return results


def fetch_attorney_details(session, reg_num: str) -> dict | None:
    """Fetch attorney detail page and extract firm name, address, phone."""
    cache_file = CACHE_DIR / f"{reg_num}.txt"
    if cache_file.exists():
        html = cache_file.read_text(encoding="utf-8")
    else:
        url = f"{BASE_URL}/Home/Details?regNum={reg_num}"
        try:
            r = session.get(url, timeout=15)
            if r.status_code != 200:
                return None
            html = r.text
            cache_file.write_text(html, encoding="utf-8")
            time.sleep(0.3)
        except Exception as e:
            return None

    soup = BeautifulSoup(html, "lxml")
    data = {}
    rows = soup.find_all("div", class_="row")
    for row in rows:
        label_el = row.find("strong")
        if not label_el:
            continue
        label = label_el.get_text(strip=True)
        # Find value (sibling div without strong)
        divs = row.find_all("div")
        val_div = next((d for d in divs if not d.find("strong") and d.get_text(strip=True)), None)
        if not val_div:
            continue
        val = val_div.get_text(separator="|", strip=True)
        data[label] = val

    # Parse Business Mailing Address
    addr_raw = data.get("Business Mailing Address", "")
    addr_parts = [p.strip() for p in addr_raw.split("|") if p.strip()]

    firm_name = ""
    street = ""
    city = ""
    state = ""
    zipcode = ""

    if addr_parts:
        # Try to identify: firm name (if present), street, "City, ST XXXXX"
        for i, part in enumerate(addr_parts):
            m = re.match(r"^(.+),\s*([A-Z]{2})\s*(\d{5})?$", part)
            if m:
                city = m.group(1).strip()
                state = m.group(2).strip()
                zipcode = (m.group(3) or "").strip()
                # Everything before this is street (and maybe firm)
                before = addr_parts[:i]
                if len(before) >= 2:
                    # First part might be firm name, second is street
                    firm_name = before[0]
                    street = " ".join(before[1:])
                elif len(before) == 1:
                    street = before[0]
                break

    phone = data.get("Business Phone", "").replace("|", "").strip()
    atty_name_raw = data.get("Attorney Name", "")

    # Parse attorney name from "LastName, FirstName" format
    if "," in atty_name_raw:
        last, first = atty_name_raw.split(",", 1)
        atty_name = f"{first.strip()} {last.strip()}"
    else:
        atty_name = atty_name_raw

    return {
        "atty_name": atty_name,
        "firm_name": firm_name or atty_name,
        "street": street,
        "city": city,
        "state": state,
        "zip": zipcode,
        "phone": phone,
        "reg_num": reg_num,
    }


def main():
    session = creq.Session(impersonate="chrome120")

    # Load all existing data
    county_data = {}
    for slug in COUNTIES:
        rows, seen = load_existing(slug)
        county_data[slug] = {"rows": rows, "seen": seen}
        print(f"Loaded {slug}: {len(rows)} existing firms")

    print("\nFetching all Active/Inactive attorney IDs from KS Courts registry...")

    # Check for cached ID list
    id_cache = CACHE_DIR / "all_ids.csv"
    if id_cache.exists():
        with open(id_cache) as f:
            all_ids = [(row[0], row[1]) for row in csv.reader(f)]
        print(f"Loaded {len(all_ids)} IDs from cache")
    else:
        all_ids = get_all_attorney_ids(session)
        with open(id_cache, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(all_ids)
        print(f"Saved {len(all_ids)} IDs to cache")

    print(f"\nFetching details for {len(all_ids)} attorneys...")
    added_counts = {slug: 0 for slug in COUNTIES}

    for i, (reg_num, name) in enumerate(all_ids):
        if i % 500 == 0 and i > 0:
            print(f"  Progress: {i}/{len(all_ids)} | Added: {dict(added_counts)}")
            # Merge with current on-disk data to avoid clobbering concurrent writers
            for slug in COUNTIES:
                if added_counts[slug] > 0:
                    disk_rows, disk_seen = load_existing(slug)
                    # Find rows we added that aren't already on disk
                    our_additions = [
                        r for r in county_data[slug]["rows"]
                        if (normalize(r.get("law_firm_name", "")) + "|" + r.get("city", "").lower().strip()) not in disk_seen
                    ]
                    merged = disk_rows + our_additions
                    save_csv(slug, merged)

        details = fetch_attorney_details(session, reg_num)
        if not details:
            continue

        city = details.get("city", "")
        state = details.get("state", "")
        if not city or state.upper() != "KS":
            continue

        county_slug = CITY_TO_COUNTY.get(city.lower())
        if not county_slug:
            # Try title case
            county_slug = CITY_TO_COUNTY.get(city.title().lower())
        if not county_slug:
            continue

        firm = details["firm_name"].strip()
        if not firm:
            firm = details["atty_name"]

        key = normalize(firm) + "|" + city.lower().strip()
        if key in county_data[county_slug]["seen"] or not normalize(firm):
            continue

        county_data[county_slug]["seen"].add(key)
        county_info = COUNTIES[county_slug]
        county_data[county_slug]["rows"].append({
            "law_firm_name": firm,
            "website": "",
            "google_business_profile": "",
            "legal_directory_listing": f"{BASE_URL}/Home/Details?regNum={reg_num}",
            "city": city.title() if city.isupper() else city,
            "state": county_info["state"],
            "county": county_info["county"],
            "phone_number": details.get("phone", ""),
            "email": "",
            "practice_area": "General",
            "street_address": details.get("street", ""),
            "zip_code": details.get("zip", ""),
            "msa": county_info["msa"],
            "priority": "2",
            "number_of_lawyers": "",
        })
        added_counts[county_slug] += 1

    # Save all — merge with disk to avoid clobbering concurrent writers
    print("\nFinal save...")
    for slug in COUNTIES:
        disk_rows, disk_seen = load_existing(slug)
        our_additions = [
            r for r in county_data[slug]["rows"]
            if (normalize(r.get("law_firm_name", "")) + "|" + r.get("city", "").lower().strip()) not in disk_seen
        ]
        merged = disk_rows + our_additions
        save_csv(slug, merged)
        print(f"  {slug}: {len(disk_rows)} on disk + {len(our_additions)} new from registry = {len(merged)}")

    total = sum(added_counts.values())
    print(f"\nTotal new firms from KS Courts registry: {total}")


if __name__ == "__main__":
    main()
