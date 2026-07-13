#!/usr/bin/env python3
"""
Post-pipeline finalization for Sedgwick + additional KS counties.
Same single-pass kscourts cache pattern as finalize_central_ks.py.

Usage: python3 finalize_ks_next.py
"""
import csv
import json
import re
import sys
from pathlib import Path
from bs4 import BeautifulSoup

DATA_DIR = Path("app/county-data")
MANIFEST_PATH = DATA_DIR / "manifest.json"
CACHE_DIR = Path(".kscourts_cache")
BASE_URL = "https://directory-kard.kscourts.gov"

# Counties: slug → {county, state, msa, cities}
NEXT_KS_COUNTIES = {
    "sedgwick-county-ks": {
        "county": "Sedgwick", "state": "KS", "msa": "Wichita",
        "cities": [
            "Wichita", "Derby", "Andover", "Haysville", "Valley Center",
            "Bel Aire", "Mulvane", "Clearwater", "Cheney", "Maize",
            "Goddard", "Park City", "Mount Hope", "Garden Plain",
            "Andale", "Viola", "Colwich", "Eastborough", "Kechi",
            "Bentley", "Sedgwick",
        ],
    },
    "riley-county-ks": {
        "county": "Riley", "state": "KS", "msa": "Manhattan",
        "cities": [
            "Manhattan", "Riley", "Ogden", "Leonardville",
            "Randolph", "Stockdale", "Cleburne", "Zeandale",
        ],
    },
    "ellis-county-ks": {
        "county": "Ellis", "state": "KS", "msa": "",
        "cities": [
            "Hays", "Ellis", "Victoria", "Catharine", "Munjor",
            "Schoenchen", "Walker", "Antonino", "Pfeifer",
        ],
    },
    "finney-county-ks": {
        "county": "Finney", "state": "KS", "msa": "",
        "cities": [
            "Garden City", "Holcomb", "Deerfield", "Pierceville",
            "Kalvesta",
        ],
    },
    "geary-county-ks": {
        "county": "Geary", "state": "KS", "msa": "",
        "cities": [
            "Junction City", "Milford", "Fort Riley", "Grandview Plaza",
            "Wakefield", "Ogden",  # Ogden is in both Riley and Geary area
        ],
    },
    "ford-county-ks": {
        "county": "Ford", "state": "KS", "msa": "",
        "cities": [
            "Dodge City", "Ford", "Spearville", "Bucklin", "Bloom",
            "Offerle", "Wright",
        ],
    },
    "seward-county-ks": {
        "county": "Seward", "state": "KS", "msa": "",
        "cities": ["Liberal", "Kismet", "Arkalon", "Plains"],
    },
    "sumner-county-ks": {
        "county": "Sumner", "state": "KS", "msa": "Wichita",
        "cities": [
            "Wellington", "Caldwell", "Argonia", "Belle Plaine",
            "Conway Springs", "South Haven", "Oxford", "Mulvane",  # shared border
            "Milan", "Mayfield", "Rago",
        ],
    },
    "atchison-county-ks": {
        "county": "Atchison", "state": "KS", "msa": "",
        "cities": [
            "Atchison", "Effingham", "Muscotah", "Huron",
            "Lancaster", "Monrovia", "Potter",
        ],
    },
    "pottawatomie-county-ks": {
        "county": "Pottawatomie", "state": "KS", "msa": "Manhattan",
        "cities": [
            "Wamego", "St. Marys", "St Marys", "Westmoreland",
            "Olsburg", "Louisville", "St. George", "St George",
            "Emmett", "Havensville", "Belvue",
        ],
    },
    "crawford-county-ks": {
        "county": "Crawford", "state": "KS", "msa": "",
        "cities": [
            "Pittsburg", "Frontenac", "Girard", "Columbus",
            "Galena", "Cherokee", "McCune", "Scammon", "Arma",
            "Mulberry", "Walnut", "Farlington", "Hepler",
        ],
    },
}

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
]


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
        print(f"  [created] {slug}.csv (empty)")
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

    print(f"  {slug}: {len(rows)-added} → {len(rows)} (+{added} from kscourts)")


def update_manifest(counts):
    manifest = json.loads(MANIFEST_PATH.read_text())
    existing = {e["slug"]: e for e in manifest["counties"]}
    updated = 0
    for slug, info in NEXT_KS_COUNTIES.items():
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
    for slug, info in NEXT_KS_COUNTIES.items():
        merge_kscourts(slug, info, parsed_cache)

    print("\n=== Step 2: Run cleanup ===")
    import ks_next_cleanup as cleanup
    import importlib
    importlib.reload(cleanup)
    counts = {}
    for slug in NEXT_KS_COUNTIES:
        print(f"\n[{slug}]")
        n = cleanup.process_county(slug)
        if n is not None:
            counts[slug] = n

    print("\n=== Step 3: Update manifest ===")
    update_manifest(counts)

    print("\n=== Final counts ===")
    total = sum(counts.values())
    for slug, n in counts.items():
        print(f"  {slug}: {n}")
    print(f"  TOTAL: {total}")


if __name__ == "__main__":
    main()
