#!/usr/bin/env python3
"""Fix bad ZIP codes and remove non-law entries in johnson-county-ks.csv."""

import csv
import re
from pathlib import Path

CSV = Path("app/county-data/johnson-county-ks.csv")

# City → most common/representative ZIP for Johnson County
CITY_ZIP = {
    "Overland Park": "66210",
    "Olathe": "66062",
    "Shawnee": "66203",
    "Lenexa": "66215",
    "Leawood": "66211",
    "Prairie Village": "66208",
    "Merriam": "66202",
    "Mission": "66202",
    "Gardner": "66030",
    "Spring Hill": "66083",
    "De Soto": "66018",
    "Edgerton": "66021",
    "Roeland Park": "66202",
    "Fairway": "66205",
    "Westwood": "66205",
    "Lake Quivira": "66217",
    "Mission Hills": "66208",
    "Mission Woods": "66208",
    "Westwood Hills": "66205",
}

REMOVE_EXACT = {
    "pdf franchise disclosure document",
    "gieringers family farm",
    "wellsky",
}

def is_valid_ks_zip(z: str) -> bool:
    z = z.strip()
    if re.match(r'^66[0-9]{3}(-[0-9]{4})?$', z):
        return True
    return False

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
