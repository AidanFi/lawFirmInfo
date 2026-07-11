#!/usr/bin/env python3
"""Add 13 new SE Kansas counties to the manifest."""
import csv, json
from pathlib import Path
from datetime import date

DATA_DIR = Path(__file__).parent / "app/county-data"
MANIFEST_PATH = DATA_DIR / "manifest.json"

SE_KS_COUNTIES = [
    {"slug": "allen-county-ks",     "name": "Allen County",      "state": "KS"},
    {"slug": "bourbon-county-ks",   "name": "Bourbon County",    "state": "KS"},
    {"slug": "chautauqua-county-ks","name": "Chautauqua County", "state": "KS"},
    {"slug": "cherokee-county-ks",  "name": "Cherokee County",   "state": "KS"},
    {"slug": "coffey-county-ks",    "name": "Coffey County",     "state": "KS"},
    {"slug": "crawford-county-ks",  "name": "Crawford County",   "state": "KS"},
    {"slug": "elk-county-ks",       "name": "Elk County",        "state": "KS"},
    {"slug": "greenwood-county-ks", "name": "Greenwood County",  "state": "KS"},
    {"slug": "labette-county-ks",   "name": "Labette County",    "state": "KS"},
    {"slug": "montgomery-county-ks","name": "Montgomery County", "state": "KS"},
    {"slug": "neosho-county-ks",    "name": "Neosho County",     "state": "KS"},
    {"slug": "wilson-county-ks",    "name": "Wilson County",     "state": "KS"},
    {"slug": "woodson-county-ks",   "name": "Woodson County",    "state": "KS"},
]

manifest = json.loads(MANIFEST_PATH.read_text())
existing_slugs = {c["slug"] for c in manifest["counties"]}
today = date.today().isoformat()

added = 0
for county in SE_KS_COUNTIES:
    slug = county["slug"]
    if slug in existing_slugs:
        print(f"  [skip] {slug} already in manifest")
        continue

    # Count firms in CSV
    csv_path = DATA_DIR / f"{slug}.csv"
    firm_count = 0
    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            firm_count = sum(1 for _ in csv.DictReader(f))

    manifest["counties"].append({
        "slug": slug,
        "name": county["name"],
        "state": county["state"],
        "firm_count": firm_count,
        "last_updated": today,
        "csv_file": f"{slug}.csv",
    })
    print(f"  Added {slug}: {firm_count} firms")
    added += 1

MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")
print(f"\nAdded {added} counties to manifest. Total: {len(manifest['counties'])} counties.")
