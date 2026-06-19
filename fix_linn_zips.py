#!/usr/bin/env python3
"""Fix bad ZIP codes in linn-county-ks.csv (KS courts addresses often carry
suite/street numbers in the ZIP field — remap by city)."""

import csv
import re
from pathlib import Path

CSV = Path("app/county-data/linn-county-ks.csv")

# City → most common/representative ZIP for Linn County, KS
CITY_ZIP = {
    "Pleasanton": "66075",
    "La Cygne": "66040",
    "Mound City": "66056",
    "Prescott": "66767",
    "Blue Mound": "66010",
    "Parker": "66072",
    "Linn Valley": "66040",
    "Centerville": "66014",
}

REMOVE_EXACT = set()

def is_valid_ks_zip(z: str) -> bool:
    z = z.strip()
    return bool(re.match(r'^66[0-9]{3}(-[0-9]{4})?$', z))

def normalize(name: str) -> str:
    return name.lower().strip()

rows = list(csv.DictReader(CSV.open()))
fieldnames = list(rows[0].keys())

removed = []
zip_fixed = []
kept = []

for r in rows:
    name = r.get("law_firm_name", "")
    if normalize(name) in REMOVE_EXACT:
        removed.append(name)
        continue

    z = r.get("zip_code", "").strip()
    if z and not is_valid_ks_zip(z):
        city = r.get("city", "").strip()
        new_zip = CITY_ZIP.get(city, "")
        if new_zip:
            zip_fixed.append(f"  {name[:45]:<45} | {city:<15} | {z} → {new_zip}")
            r["zip_code"] = new_zip
        else:
            zip_fixed.append(f"  {name[:45]:<45} | {city:<15} | {z} → UNKNOWN CITY (kept as-is)")

    kept.append(r)

with CSV.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(kept)

print(f"Removed {len(removed)} non-law entries:")
for n in removed:
    print(f"  {n}")
print(f"\nFixed {len(zip_fixed)} bad ZIPs:")
for line in zip_fixed:
    print(line)
print(f"\nKept {len(kept)} firms")
