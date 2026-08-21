#!/usr/bin/env python3
"""Update firm_count/last_updated for specific slugs in manifest.json (adds entry if missing)."""
import csv
import json
import sys
from pathlib import Path

DATA_DIR = Path("app/county-data")
TODAY = "2026-08-17"


def main(slugs):
    mpath = DATA_DIR / "manifest.json"
    manifest = json.loads(mpath.read_text())
    by_slug = {c["slug"]: c for c in manifest["counties"]}

    for slug in slugs:
        path = DATA_DIR / f"{slug}.csv"
        if not path.exists():
            print(f"  [skip] {slug}: csv not found")
            continue
        rows = list(csv.DictReader(open(path)))
        county_name = rows[0]["county"] if rows else slug.replace("-county-ks", "").replace("-", " ").title()
        entry = by_slug.get(slug, {
            "slug": slug,
            "name": f"{county_name} County",
            "state": "KS",
            "csv_file": f"{slug}.csv",
        })
        entry["firm_count"] = len(rows)
        entry["last_updated"] = TODAY
        by_slug[slug] = entry
        print(f"  {slug}: firm_count={len(rows)}")

    manifest["counties"] = list(by_slug.values())
    mpath.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Manifest updated with {len(manifest['counties'])} total counties")


if __name__ == "__main__":
    main(sys.argv[1:])
