#!/usr/bin/env python3
"""
Post-pipeline finalization for 17 central KS counties.
Run after all pipelines complete:
  1. Merge kscourts registry data back in (pipeline overwrote it)
  2. Run central_ks_cleanup.py cleanup
  3. Update manifest with final counts

Usage: python3 finalize_central_ks.py
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

CENTRAL_KS_COUNTIES = {
    "barton-county-ks": {
        "county": "Barton", "state": "KS", "msa": "",
        "cities": ["Great Bend", "Ellinwood", "Hoisington", "Claflin", "Albert",
                   "Olmitz", "Pawnee Rock", "Galatia", "Susank"],
    },
    "clay-county-ks": {
        "county": "Clay", "state": "KS", "msa": "",
        "cities": ["Clay Center", "Wakefield", "Green", "Clifton", "Morganville",
                   "Leonardville", "Idana"],
    },
    "cloud-county-ks": {
        "county": "Cloud", "state": "KS", "msa": "",
        "cities": ["Concordia", "Miltonvale", "Glasco", "Clyde", "Jamestown",
                   "Aurora", "Ames", "Tipton"],
    },
    "dickinson-county-ks": {
        "county": "Dickinson", "state": "KS", "msa": "",
        "cities": ["Abilene", "Chapman", "Solomon", "Herington", "Detroit",
                   "Hope", "Enterprise", "Elmo", "Carlton", "Navarre", "Woodbine"],
    },
    "ellsworth-county-ks": {
        "county": "Ellsworth", "state": "KS", "msa": "",
        "cities": ["Ellsworth", "Kanopolis", "Wilson", "Lorraine", "Holyrood", "Carneiro"],
    },
    "harvey-county-ks": {
        "county": "Harvey", "state": "KS", "msa": "Wichita",
        "cities": ["Newton", "Halstead", "Hesston", "Burrton", "Sedgwick", "Walton",
                   "North Newton"],
    },
    "kingman-county-ks": {
        "county": "Kingman", "state": "KS", "msa": "",
        "cities": ["Kingman", "Norwich", "Nashville", "Cunningham", "Zenda",
                   "Penalosa", "Spivey", "Rago", "Murdock"],
    },
    "lincoln-county-ks": {
        "county": "Lincoln", "state": "KS", "msa": "",
        "cities": ["Lincoln", "Sylvan Grove", "Barnard", "Beverly", "Vesper",
                   "Luray", "Denmark"],
    },
    "marion-county-ks": {
        "county": "Marion", "state": "KS", "msa": "",
        "cities": ["Marion", "Hillsboro", "Peabody", "Florence", "Burns", "Durham",
                   "Goessel", "Lehigh", "Lost Springs", "Ramona", "Tampa",
                   "Lincolnville", "Antelope"],
    },
    "mcpherson-county-ks": {
        "county": "McPherson", "state": "KS", "msa": "",
        "cities": ["McPherson", "Mc Pherson", "Lindsborg", "Marquette", "Inman",
                   "Canton", "Moundridge", "Galva", "Windom", "Buhler", "Roxbury", "Elyria"],
    },
    "mitchell-county-ks": {
        "county": "Mitchell", "state": "KS", "msa": "",
        "cities": ["Beloit", "Cawker City", "Glen Elder", "Tipton", "Hunter", "Scottus"],
    },
    "ottawa-county-ks": {
        "county": "Ottawa", "state": "KS", "msa": "",
        "cities": ["Minneapolis", "Delphos", "Tescott", "Bennington", "Culver",
                   "Simpson", "Markley"],
    },
    "reno-county-ks": {
        "county": "Reno", "state": "KS", "msa": "Wichita",
        "cities": ["Hutchinson", "South Hutchinson", "Nickerson", "Pretty Prairie",
                   "Haven", "Partridge", "Burrton", "Turon", "Yoder", "Arlington",
                   "Abbyville", "Sylvia", "Medora", "Plevna", "Willowbrook"],
    },
    "rice-county-ks": {
        "county": "Rice", "state": "KS", "msa": "",
        "cities": ["Lyons", "Sterling", "Little River", "Chase", "Alden",
                   "Bushton", "Geneseo", "Raymond"],
    },
    "russell-county-ks": {
        "county": "Russell", "state": "KS", "msa": "",
        "cities": ["Russell", "Lucas", "Dorrance", "Gorham", "Bunker Hill",
                   "Paradise", "Waldo"],
    },
    "saline-county-ks": {
        "county": "Saline", "state": "KS", "msa": "Salina",
        "cities": ["Salina", "Assaria", "Brookville", "Gypsum", "Mentor",
                   "New Cambria", "Smolan", "Falun"],
    },
    "stafford-county-ks": {
        "county": "Stafford", "state": "KS", "msa": "",
        "cities": ["Saint John", "St. John", "St John", "Stafford", "Macksville",
                   "Seward", "Hudson", "Radium", "Zenith"],
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
    """Pre-parse all cache files once. Returns list of (reg_num, parsed_data) tuples."""
    print("  Pre-parsing kscourts cache (one pass)...")
    parsed = []
    for reg_num, _ in all_ids:
        data = read_cache(reg_num)
        if data:
            parsed.append((reg_num, data))
    print(f"  Parsed {len(parsed)} attorney records")
    return parsed


def ensure_csv(slug, info):
    """Create empty CSV if it doesn't exist."""
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
        print(f"  [created] {slug}.csv (empty)")
    return path


def merge_kscourts(slug, info, parsed_cache):
    """Merge kscourts data into county CSV using pre-parsed cache."""
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
    updated = 0
    existing_slugs = {e["slug"] for e in manifest["counties"]}
    for slug, info in CENTRAL_KS_COUNTIES.items():
        if slug not in existing_slugs:
            manifest["counties"].append({
                "slug": slug,
                "name": f"{info['county']} County",
                "state": "KS",
                "firm_count": counts.get(slug, 0),
                "last_updated": "2026-07-12",
                "msa": info["msa"],
            })
            updated += 1
        else:
            for entry in manifest["counties"]:
                if entry["slug"] == slug:
                    entry["firm_count"] = counts.get(slug, 0)
                    entry["last_updated"] = "2026-07-12"
                    updated += 1
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"  Updated/added {updated} counties in manifest.json")


def main():
    all_ids_file = CACHE_DIR / "all_ids.csv"
    all_ids = [(r[0], r[1]) for r in csv.reader(open(all_ids_file))]
    print(f"Loaded {len(all_ids)} cached attorney IDs\n")

    print("=== Step 1: Merge kscourts registry data ===")
    parsed_cache = parse_all_cache(all_ids)
    for slug, info in CENTRAL_KS_COUNTIES.items():
        merge_kscourts(slug, info, parsed_cache)

    print("\n=== Step 2: Run Central KS cleanup ===")
    import central_ks_cleanup as cleanup
    import importlib
    importlib.reload(cleanup)
    counts = {}
    for slug in CENTRAL_KS_COUNTIES:
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
