#!/usr/bin/env python3
"""
Third batch of KS counties: Butler, Lyon, Cowley, Pratt, Jackson, Brown,
Thomas, Doniphan, Nemaha, Stevens, Norton, Pawnee, Scott, Morris, Trego,
Rush, Wabaunsee, Chase, Washington, Republic, Jewell, Smith, Decatur, Phillips.
Also re-runs kscourts merge for existing SE KS counties (Neosho, Wilson) to
pick up any attorneys the pipeline missed.

Usage: python3 finalize_ks_batch3.py
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

BATCH3_COUNTIES = {
    "butler-county-ks": {
        "county": "Butler", "state": "KS", "msa": "Wichita",
        "cities": [
            "El Dorado", "Augusta", "Andover", "Rose Hill", "Leon",
            "Benton", "Towanda", "Potwin", "Burns", "Cassoday",
            "Latham", "Douglass", "Eureka", "Whitewater", "Elbing",
            "Severy", "Chelsea",
        ],
    },
    "lyon-county-ks": {
        "county": "Lyon", "state": "KS", "msa": "",
        "cities": [
            "Emporia", "Allen", "Americus", "Hartford", "Reading",
            "Admire", "Olpe", "Lebo", "Neosho Rapids",
        ],
    },
    "cowley-county-ks": {
        "county": "Cowley", "state": "KS", "msa": "",
        "cities": [
            "Winfield", "Arkansas City", "Udall", "Burden", "Dexter",
            "Cambridge", "Atlanta", "Oxford", "Maple City",
        ],
    },
    "pratt-county-ks": {
        "county": "Pratt", "state": "KS", "msa": "",
        "cities": ["Pratt", "Iuka", "Comet", "Preston", "Sawyer", "Cullison"],
    },
    "jackson-county-ks": {
        "county": "Jackson", "state": "KS", "msa": "",
        "cities": [
            "Holton", "Mayetta", "Whiting", "Netawaka", "Soldier",
            "Circleville", "Hoyt", "Delia",
        ],
    },
    "brown-county-ks": {
        "county": "Brown", "state": "KS", "msa": "",
        "cities": [
            "Hiawatha", "Horton", "Sabetha", "Fairview", "Reserve",
            "Everest", "Willis",
        ],
    },
    "thomas-county-ks": {
        "county": "Thomas", "state": "KS", "msa": "",
        "cities": ["Colby", "Brewster", "Rexford", "Menlo"],
    },
    "doniphan-county-ks": {
        "county": "Doniphan", "state": "KS", "msa": "",
        "cities": ["Troy", "Elwood", "Highland", "White Cloud", "Wathena", "Severance"],
    },
    "nemaha-county-ks": {
        "county": "Nemaha", "state": "KS", "msa": "",
        "cities": ["Seneca", "Sabetha", "Centralia", "Baileyville", "Wetmore"],
    },
    "stevens-county-ks": {
        "county": "Stevens", "state": "KS", "msa": "",
        "cities": ["Hugoton", "Moscow", "Satanta"],
    },
    "norton-county-ks": {
        "county": "Norton", "state": "KS", "msa": "",
        "cities": ["Norton", "Almena", "Lenora", "Clayton"],
    },
    "pawnee-county-ks": {
        "county": "Pawnee", "state": "KS", "msa": "",
        "cities": ["Larned", "Burdett", "Rozel", "Garfield"],
    },
    "scott-county-ks": {
        "county": "Scott", "state": "KS", "msa": "",
        "cities": ["Scott City", "Modoc"],
    },
    "morris-county-ks": {
        "county": "Morris", "state": "KS", "msa": "",
        "cities": ["Council Grove", "Dunlap", "Dwight", "White City"],
    },
    "wabaunsee-county-ks": {
        "county": "Wabaunsee", "state": "KS", "msa": "",
        "cities": ["Alma", "Eskridge", "Maple Hill", "Wabaunsee"],
    },
    "chase-county-ks": {
        "county": "Chase", "state": "KS", "msa": "",
        "cities": ["Cottonwood Falls", "Strong City", "Matfield Green"],
    },
    "washington-county-ks": {
        "county": "Washington", "state": "KS", "msa": "",
        "cities": ["Washington", "Haddam", "Barnes", "Clifton", "Greenleaf", "Palmer"],
    },
    "republic-county-ks": {
        "county": "Republic", "state": "KS", "msa": "",
        "cities": ["Belleville", "Courtland", "Scandia", "Cuba", "Narka"],
    },
    "jewell-county-ks": {
        "county": "Jewell", "state": "KS", "msa": "",
        "cities": ["Mankato", "Jewell", "Esbon", "Formoso"],
    },
    "smith-county-ks": {
        "county": "Smith", "state": "KS", "msa": "",
        "cities": ["Smith Center", "Gaylord", "Athol"],
    },
    "decatur-county-ks": {
        "county": "Decatur", "state": "KS", "msa": "",
        "cities": ["Oberlin", "Norcatur", "Clayton"],
    },
    "phillips-county-ks": {
        "county": "Phillips", "state": "KS", "msa": "",
        "cities": ["Phillipsburg", "Logan", "Agra", "Prairie View"],
    },
    # Existing SE KS counties — kscourts top-up for pipeline-missed attorneys
    "neosho-county-ks": {
        "county": "Neosho", "state": "KS", "msa": "",
        "cities": ["Chanute", "Erie", "Thayer", "Galesburg", "Parsons",
                   "St. Paul", "St Paul"],
    },
    "wilson-county-ks": {
        "county": "Wilson", "state": "KS", "msa": "",
        "cities": ["Fredonia", "Altoona", "Buffalo", "Benedict",
                   "Coyville", "Fall River", "Roper"],
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
    for slug, info in BATCH3_COUNTIES.items():
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
    for slug, info in BATCH3_COUNTIES.items():
        merge_kscourts(slug, info, parsed_cache)

    print("\n=== Step 2: Run cleanup ===")
    import ks_next_cleanup as cleanup
    import importlib
    importlib.reload(cleanup)
    counts = {}
    for slug in BATCH3_COUNTIES:
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
