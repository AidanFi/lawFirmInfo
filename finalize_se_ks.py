#!/usr/bin/env python3
"""
Post-pipeline finalization for 13 SE KS counties.
Run after all pipelines complete:
  1. Merge kscourts registry data back in (pipeline overwrote it)
  2. Run se_ks_cleanup.py cleanup
  3. Update manifest with final counts

Usage: python3 finalize_se_ks.py
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

SE_KS_COUNTIES = {
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


def merge_kscourts(slug, info, all_ids):
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        print(f"  [MISSING] {slug}.csv not found — skipping")
        return

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

    for reg_num, _ in all_ids:
        data = read_cache(reg_num)
        if not data:
            continue
        addr_raw = data.get("Business Mailing Address", "")
        addr_parts = [p.strip() for p in addr_raw.split("|") if p.strip()]
        city = state = zipcode = firm_name = street = ""
        for i, part in enumerate(addr_parts):
            m = re.match(r"^(.+),\s*([A-Z]{2})\s*(\d{5})?$", part)
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
    updated = 0
    for entry in manifest["counties"]:
        slug = entry["slug"]
        if slug in counts:
            entry["firm_count"] = counts[slug]
            entry["last_updated"] = "2026-07-11"
            updated += 1
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"  Updated {updated} counties in manifest.json")


def main():
    # Load all cached attorney IDs
    all_ids_file = CACHE_DIR / "all_ids.csv"
    all_ids = [(r[0], r[1]) for r in csv.reader(open(all_ids_file))]
    print(f"Loaded {len(all_ids)} cached attorney IDs\n")

    # Step 1: Merge kscourts data back into pipeline-overwritten CSVs
    print("=== Step 1: Merge kscourts registry data ===")
    for slug, info in SE_KS_COUNTIES.items():
        merge_kscourts(slug, info, all_ids)

    # Step 2: Run cleanup
    print("\n=== Step 2: Run SE KS cleanup ===")
    import se_ks_cleanup as cleanup
    import importlib
    importlib.reload(cleanup)
    counts = {}
    for slug in SE_KS_COUNTIES:
        print(f"\n[{slug}]")
        n = cleanup.process_county(slug)
        if n is not None:
            counts[slug] = n

    # Step 3: Update manifest
    print("\n=== Step 3: Update manifest ===")
    update_manifest(counts)

    print("\n=== Final counts ===")
    total = sum(counts.values())
    for slug, n in counts.items():
        print(f"  {slug}: {n}")
    print(f"  TOTAL: {total}")


if __name__ == "__main__":
    main()
