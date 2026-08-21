#!/usr/bin/env python3
"""Deep quality clean for wyandotte-county-ks.csv — remove confirmed non-law entities
and fix out-of-county ZIPs."""

import csv
import re
from pathlib import Path

CSV = Path("app/county-data/wyandotte-county-ks.csv")

# Wyandotte County valid ZIP prefixes
# KCK: 661xx; Bonner Springs: 66012; Edwardsville: 66113; Lake Quivira: 66217
VALID_ZIP_PREFIXES = ("661", "66012", "66113", "66217")

# City → default Wyandotte ZIP (for fixing out-of-county ZIPs on legit firms)
CITY_ZIP = {
    "Kansas City": "66101",
    "Bonner Springs": "66012",
    "Edwardsville": "66113",
    "Lake Quivira": "66217",
}

REMOVE_EXACT = {
    # Auto parts stores — not law firms
    "advance auto parts",
    "midway auto parts",
    # Crane rental — not a law firm
    "wilkerson crane rental, inc.",
    "wilkerson crane rental inc",
    # Tax prep chain — not a law firm
    "h & r block",
    "h&r block",
    # Marketing artifact with city name in it
    "hire the best blue springs truck acciden",
}

_NON_LAW_PATTERNS = re.compile(
    r'\b(?:'
    r'auto parts|auto shop|auto repair|car wash|tire shop|'
    r'crane rental|crane service|'
    r'grocery|convenience store|'
    r'h\s*&\s*r\s*block|jackson hewitt|liberty tax|'
    r'insurance agent|insurance agency(?! law)(?! legal)|'
    r'real estate(?! law)(?! legal)|realty(?! law)(?! legal)|'
    r'hospital(?! law)(?! legal)|medical center|'
    r'church|parish|ministry'
    r')\b',
    re.IGNORECASE,
)


def normalize(name: str) -> str:
    return re.sub(r"[,.']+", "", name.lower()).strip()


def is_valid_wyandotte_zip(z: str) -> bool:
    z = z.strip()
    return any(z.startswith(p) for p in VALID_ZIP_PREFIXES)


def should_remove(row: dict) -> bool:
    name = row.get("law_firm_name", "")
    n = normalize(name)
    if n in REMOVE_EXACT:
        return True
    if _NON_LAW_PATTERNS.search(name):
        law_override = re.search(
            r'\b(law|legal|attorney|lawyers|counsel|llp|pllc|p\.c\.|p\.a\.)\b',
            name, re.IGNORECASE,
        )
        if not law_override:
            return True
    return False


rows = list(csv.DictReader(CSV.open()))
fieldnames = list(rows[0].keys())

removed = []
zip_fixed = []
out_of_county_removed = []
kept = []

for r in rows:
    if should_remove(r):
        removed.append(r["law_firm_name"])
        continue

    z = r.get("zip_code", "").strip()
    city = r.get("city", "").strip()

    if z and not is_valid_wyandotte_zip(z):
        new_zip = CITY_ZIP.get(city, "")
        if new_zip:
            zip_fixed.append(f"  {r['law_firm_name'][:45]:<45} | {city} | {z} → {new_zip}")
            r["zip_code"] = new_zip
        else:
            # Out-of-county firm with no mapping — keep but flag
            zip_fixed.append(f"  {r['law_firm_name'][:45]:<45} | {city} | {z} → NO FIX (kept)")

    kept.append(r)

with CSV.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(kept)

print(f"Removed {len(removed)} non-law entries:")
for n in sorted(removed):
    print(f"  {n}")
if zip_fixed:
    print(f"\nFixed/checked {len(zip_fixed)} out-of-Wyandotte ZIPs:")
    for line in zip_fixed:
        print(line)
print(f"\nKept {len(kept)} firms")
