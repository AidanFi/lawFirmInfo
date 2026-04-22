import csv
import json
import os
from datetime import date


def update_manifest(county_data_dir: str) -> dict:
    manifest_path = os.path.join(county_data_dir, "manifest.json")

    existing = {"counties": []}
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f:
            existing = json.load(f)

    existing_by_file = {c["csv_file"]: c for c in existing["counties"]}

    for fname in sorted(os.listdir(county_data_dir)):
        if not fname.endswith(".csv"):
            continue
        csv_path = os.path.join(county_data_dir, fname)
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            continue

        first = rows[0]
        county_name = first.get("county", "")
        state = first.get("state", "")
        slug = fname.replace(".csv", "")

        existing_by_file[fname] = {
            "slug": slug,
            "name": f"{county_name} County",
            "state": state,
            "firm_count": len(rows),
            "last_updated": date.today().isoformat(),
            "csv_file": fname,
        }

    manifest = {"counties": list(existing_by_file.values())}

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[manifest] Updated {manifest_path} with {len(manifest['counties'])} counties")
    return manifest
