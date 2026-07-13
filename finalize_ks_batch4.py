#!/usr/bin/env python3
"""
Final batch — remaining small KS counties (NW/SW/rural).
All have 1-6 kscourts entries. For completeness.

Usage: python3 finalize_ks_batch4.py
"""
import csv
import json
import re
from pathlib import Path
from bs4 import BeautifulSoup

DATA_DIR = Path("app/county-data")
MANIFEST_PATH = DATA_DIR / "manifest.json"
CACHE_DIR = Path(".kscourts_cache")
BASE_URL = "https://directory-kard.kscourts.gov"

BATCH4_COUNTIES = {
    "anderson-county-ks": {
        "county": "Anderson", "state": "KS", "msa": "",
        "cities": ["Garnett", "Greeley", "Westphalia", "Colony", "Kincaid", "Harris"],
    },
    "barber-county-ks": {
        "county": "Barber", "state": "KS", "msa": "",
        "cities": ["Medicine Lodge", "Sharon", "Kiowa", "Hazelton", "Sun City"],
    },
    "rooks-county-ks": {
        "county": "Rooks", "state": "KS", "msa": "",
        "cities": ["Stockton", "Plainville", "Woodston", "Palco"],
    },
    "trego-county-ks": {
        "county": "Trego", "state": "KS", "msa": "",
        "cities": ["WaKeeney", "Wa Keeney", "Wakeeney", "Collyer", "Ogallah"],
    },
    "grant-county-ks": {
        "county": "Grant", "state": "KS", "msa": "",
        "cities": ["Ulysses", "Surprise"],
    },
    "osborne-county-ks": {
        "county": "Osborne", "state": "KS", "msa": "",
        "cities": ["Osborne", "Downs", "Portis", "Natoma"],
    },
    "sherman-county-ks": {
        "county": "Sherman", "state": "KS", "msa": "",
        "cities": ["Goodland", "Kanorado"],
    },
    "graham-county-ks": {
        "county": "Graham", "state": "KS", "msa": "",
        "cities": ["Hill City", "Bogue", "Morland", "Edmond"],
    },
    "greeley-county-ks": {
        "county": "Greeley", "state": "KS", "msa": "",
        "cities": ["Tribune", "Horace"],
    },
    "hamilton-county-ks": {
        "county": "Hamilton", "state": "KS", "msa": "",
        "cities": ["Syracuse", "Coolidge", "Kendall"],
    },
    "clark-county-ks": {
        "county": "Clark", "state": "KS", "msa": "",
        "cities": ["Ashland", "Minneola"],
    },
    "lane-county-ks": {
        "county": "Lane", "state": "KS", "msa": "",
        "cities": ["Dighton", "Healy"],
    },
    "morton-county-ks": {
        "county": "Morton", "state": "KS", "msa": "",
        "cities": ["Elkhart", "Richfield", "Rolla"],
    },
    "gray-county-ks": {
        "county": "Gray", "state": "KS", "msa": "",
        "cities": ["Cimarron", "Ingalls", "Copeland"],
    },
    "meade-county-ks": {
        "county": "Meade", "state": "KS", "msa": "",
        "cities": ["Meade", "Fowler", "Plains"],
    },
    "rawlins-county-ks": {
        "county": "Rawlins", "state": "KS", "msa": "",
        "cities": ["Atwood", "McDonald"],
    },
    "haskell-county-ks": {
        "county": "Haskell", "state": "KS", "msa": "",
        "cities": ["Sublette", "Satanta", "Santa Fe"],
    },
    "logan-county-ks": {
        "county": "Logan", "state": "KS", "msa": "",
        "cities": ["Oakley", "Winona", "Russell Springs"],
    },
    "sheridan-county-ks": {
        "county": "Sheridan", "state": "KS", "msa": "",
        "cities": ["Hoxie", "Lucerne"],
    },
    "wallace-county-ks": {
        "county": "Wallace", "state": "KS", "msa": "",
        "cities": ["Sharon Springs", "Weskan"],
    },
    "cheyenne-county-ks": {
        "county": "Cheyenne", "state": "KS", "msa": "",
        "cities": ["St. Francis", "Saint Francis", "St Francis", "Wheeler"],
    },
    "comanche-county-ks": {
        "county": "Comanche", "state": "KS", "msa": "",
        "cities": ["Coldwater", "Protection"],
    },
    "hodgeman-county-ks": {
        "county": "Hodgeman", "state": "KS", "msa": "",
        "cities": ["Jetmore", "Hanston"],
    },
    "edwards-county-ks": {
        "county": "Edwards", "state": "KS", "msa": "",
        "cities": ["Kinsley", "Offerle", "Lewis"],
    },
    "kiowa-county-ks": {
        "county": "Kiowa", "state": "KS", "msa": "",
        "cities": ["Greensburg", "Haviland", "Belvidere", "Mullinville"],
    },
    "rush-county-ks": {
        "county": "Rush", "state": "KS", "msa": "",
        "cities": ["La Crosse", "LaCrosse", "Lacrosse", "Bison", "Alexander"],
    },
    "stanton-county-ks": {
        "county": "Stanton", "state": "KS", "msa": "",
        "cities": ["Johnson", "Johnson City"],  # "Johnson" is ambiguous but for KS context
    },
    "kearny-county-ks": {
        "county": "Kearny", "state": "KS", "msa": "",
        "cities": ["Lakin", "Deerfield", "Hartland"],
    },
    "ness-county-ks": {
        "county": "Ness", "state": "KS", "msa": "",
        "cities": ["Ness City", "Utica", "Bazine", "Ransom"],
    },
    "gove-county-ks": {
        "county": "Gove", "state": "KS", "msa": "",
        "cities": ["Quinter", "Grainfield", "Grinnell", "Monument"],
    },
    "wichita-county-ks": {
        "county": "Wichita", "state": "KS", "msa": "",
        "cities": ["Leoti", "Marienthal", "Selkirk"],
    },
}

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]

# Default area codes: most of these western/rural counties use 620 or 785
COUNTY_AREA_CODE = {
    "anderson-county-ks": "785",
    "barber-county-ks": "620",
    "rooks-county-ks": "785",
    "trego-county-ks": "785",
    "grant-county-ks": "620",
    "osborne-county-ks": "785",
    "sherman-county-ks": "785",
    "graham-county-ks": "785",
    "greeley-county-ks": "620",
    "hamilton-county-ks": "620",
    "clark-county-ks": "620",
    "lane-county-ks": "620",
    "morton-county-ks": "620",
    "gray-county-ks": "620",
    "meade-county-ks": "620",
    "rawlins-county-ks": "785",
    "haskell-county-ks": "620",
    "logan-county-ks": "785",
    "sheridan-county-ks": "785",
    "wallace-county-ks": "785",
    "cheyenne-county-ks": "785",
    "comanche-county-ks": "620",
    "hodgeman-county-ks": "620",
    "edwards-county-ks": "620",
    "kiowa-county-ks": "620",
    "rush-county-ks": "785",
    "stanton-county-ks": "620",
    "kearny-county-ks": "620",
    "ness-county-ks": "785",
    "gove-county-ks": "785",
    "wichita-county-ks": "620",
}


def normalize(name):
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|"
                  r"attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd|co|inc|"
                  r"corp|company|limited|ltd|incorporated|associates?)\b", "", name)
    return re.sub(r"[^a-z0-9]", "", name)


def read_cache(reg_num):
    f = CACHE_DIR / f"{reg_num}.txt"
    if not f.exists():
        return None
    soup = BeautifulSoup(f.read_text(encoding="utf-8"), "lxml")
    data = {}
    for row in soup.find_all("div", class_="row"):
        label_el = row.find("strong")
        if not label_el:
            continue
        label = label_el.get_text(strip=True)
        divs = row.find_all("div")
        val_div = next((d for d in divs if not d.find("strong") and d.get_text(strip=True)), None)
        if not val_div:
            continue
        data[label] = val_div.get_text(separator="|", strip=True)
    return data


def parse_all_cache(all_ids):
    print("  Pre-parsing kscourts cache (one pass)...")
    parsed = []
    for reg_num, _ in all_ids:
        data = read_cache(reg_num)
        if data:
            parsed.append((reg_num, data))
    print(f"  Parsed {len(parsed)} attorney records")
    return parsed


def ensure_csv(slug, info):
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
    return path


def merge_kscourts(slug, info, parsed_cache):
    path = ensure_csv(slug, info)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or FIELDNAMES)
    if "number_of_lawyers" not in fieldnames:
        fieldnames = fieldnames + ["number_of_lawyers"]

    seen = set()
    for r in rows:
        key = normalize(r.get("law_firm_name", "")) + "|" + r.get("city", "").lower().strip()
        seen.add(key)

    city_lc = {c.lower() for c in info["cities"]}
    added = 0

    for reg_num, data in parsed_cache:
        addr_raw = data.get("Business Mailing Address", "")
        addr_parts = [p.strip() for p in addr_raw.split("|") if p.strip()]
        city = state = zipcode = firm_name = street = ""
        for i, part in enumerate(addr_parts):
            m = re.match(r"^(.+),\s*(KS)\s*(\d{5}(?:-\d{4})?)?\s*$", part)
            if m:
                city = m.group(1).strip()
                state = m.group(2).strip()
                zipcode = (m.group(3) or "").strip()
                before = addr_parts[:i]
                if len(before) >= 2:
                    firm_name = before[0]
                    street = " ".join(before[1:])
                elif len(before) == 1:
                    street = before[0]
                break
        if not city or state.upper() != "KS":
            continue
        if city.lower() not in city_lc:
            continue

        phone = data.get("Business Phone", "").replace("|", "").strip()
        atty_name_raw = data.get("Attorney Name", "")
        if "," in atty_name_raw:
            last, first = atty_name_raw.split(",", 1)
            atty_name = f"{first.strip()} {last.strip()}"
        else:
            atty_name = atty_name_raw
        firm = (firm_name or atty_name).strip()
        if not firm:
            continue

        key = normalize(firm) + "|" + city.lower().strip()
        if key in seen or not normalize(firm):
            continue
        seen.add(key)
        rows.append({
            "law_firm_name": firm,
            "website": "",
            "google_business_profile": "",
            "legal_directory_listing": f"{BASE_URL}/Home/Details?regNum={reg_num}",
            "city": city.title() if city.isupper() else city,
            "state": info["state"],
            "county": info["county"],
            "phone_number": phone,
            "email": "",
            "practice_area": "General",
            "street_address": street,
            "zip_code": zipcode,
            "msa": info["msa"],
            "priority": "2",
            "number_of_lawyers": "",
        })
        added += 1

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows), added


def update_manifest(counts):
    manifest = json.loads(MANIFEST_PATH.read_text())
    existing = {e["slug"]: e for e in manifest["counties"]}
    updated = 0
    for slug, info in BATCH4_COUNTIES.items():
        if slug not in existing:
            manifest["counties"].append({
                "slug": slug,
                "name": f"{info['county']} County",
                "state": "KS",
                "firm_count": counts.get(slug, 0),
                "last_updated": "2026-07-12",
                "msa": info["msa"],
            })
        else:
            existing[slug]["firm_count"] = counts.get(slug, 0)
            existing[slug]["last_updated"] = "2026-07-12"
            existing[slug]["state"] = "KS"
        updated += 1
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"  Updated/added {updated} counties in manifest.json")


def main():
    all_ids_file = CACHE_DIR / "all_ids.csv"
    all_ids = [(r[0], r[1]) for r in csv.reader(open(all_ids_file))]
    print(f"Loaded {len(all_ids)} cached attorney IDs\n")

    print("=== Step 1: Merge kscourts registry data ===")
    parsed_cache = parse_all_cache(all_ids)
    merge_stats = {}
    for slug, info in BATCH4_COUNTIES.items():
        total, added = merge_kscourts(slug, info, parsed_cache)
        merge_stats[slug] = (total, added)
        if added > 0:
            print(f"  {slug}: +{added} entries (total {total})")

    print("\n=== Step 2: Run cleanup ===")
    import ks_next_cleanup as cleanup
    import importlib
    importlib.reload(cleanup)

    # Patch area codes into cleanup module
    for slug, ac in COUNTY_AREA_CODE.items():
        cleanup.COUNTY_AREA_CODE[slug] = ac

    counts = {}
    for slug in BATCH4_COUNTIES:
        n = cleanup.process_county(slug)
        if n is not None:
            counts[slug] = n

    print("\n=== Step 3: Update manifest ===")
    update_manifest(counts)

    print("\n=== Final counts ===")
    total = sum(counts.values())
    for slug, n in counts.items():
        if n > 0:
            print(f"  {slug}: {n}")
    print(f"  TOTAL NEW: {total}")


if __name__ == "__main__":
    main()
