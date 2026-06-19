#!/usr/bin/env python3
"""Deep quality clean for leavenworth-county-ks.csv."""

import csv
import re
from pathlib import Path

CSV = Path("app/county-data/leavenworth-county-ks.csv")

REMOVE_EXACT = {
    # US Army / military legal offices — not civilian referral firms (normalized: no ,./’)
    "united stated army combined arms center and fort l",
    "united stated army combined arms center and fort leavenworth",
    "united states army",
    "united states army combined arms command osja",
    "fort leavenworth legal office",
    "staff judge advocate office",
    "fort leavenworth parade field",
    # County government offices — not law firms
    "leavenworth county",
    "leavenworth county attorneys office",
    "leavenworth county counselor",
    # Non-law businesses (normalized: apostrophes/commas stripped by normalize())
    "oreilly auto parts",
    "just for paws",
    "mole patrol llc",
    "paws parlor",
    "peruvian connection llc",
    "sams liquor & party shop",
    "puros panchos",
    "zephyr products inc",
    "old food for less parking lot",
    # Religious institution
    "st lawrence catholic church",
}

_NON_LAW_PATTERNS = re.compile(
    r'\b(?:'
    r'auto parts|auto shop|car wash|'
    r'h\s*&\s*r\s*block|jackson hewitt|liberty tax|'
    r'insurance agent|insurance agency(?! law)(?! legal)|'
    r'real estate(?! law)(?! legal)|realty(?! law)(?! legal)|'
    r'hospital(?! law)(?! legal)|'
    r'school district|public school'
    r')\b',
    re.IGNORECASE,
)

# Normalize "General Practice" → "General"
GENERAL_ALIASES = {"general practice", "general practitioner"}


def normalize(name: str) -> str:
    return re.sub(r"[,.']+", "", name.lower()).strip()


def should_remove(row: dict) -> bool:
    name = row.get("law_firm_name", "")
    n = normalize(name)
    if n in REMOVE_EXACT:
        return True
    if _NON_LAW_PATTERNS.search(name):
        if not re.search(r'\b(law|legal|attorney|lawyers|counsel|llp|pllc|p\.c\.|p\.a\.)\b', name, re.IGNORECASE):
            return True
    return False


rows = list(csv.DictReader(CSV.open()))
fieldnames = list(rows[0].keys())

removed, pa_fixed, kept = [], 0, []

for r in rows:
    if should_remove(r):
        removed.append(r["law_firm_name"])
        continue
    if r.get("practice_area", "").strip().lower() in GENERAL_ALIASES:
        r["practice_area"] = "General"
        pa_fixed += 1
    kept.append(r)

with CSV.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(kept)

print(f"Removed {len(removed)} non-law entries:")
for n in sorted(removed):
    print(f"  {n}")
print(f"Normalized {pa_fixed} 'General Practice' → 'General'")
print(f"Kept {len(kept)} firms")
