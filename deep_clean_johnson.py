#!/usr/bin/env python3
"""Deep quality clean for johnson-county-ks.csv — remove confirmed non-law entities."""

import csv
import re
from pathlib import Path

CSV = Path("app/county-data/johnson-county-ks.csv")

# Confirmed non-law businesses (exact normalized name match)
REMOVE_EXACT = {
    # Packaging company — has in-house counsel but not a law firm
    "huhtamaki americas",
    "huhtamaki inc",
    # Manufacturing
    "ferroworks",
    # Real estate development
    "great plains developments llc",
    # Tech companies
    "trueml technologies llc",
    # Financial services (not law firms)
    "lpl financial",
    "bok financial",
    "mccaffree financial corp",
    "baystone financial llc",
    # Trust companies (not law firms)
    "great plains trust company",
    "midwest trust company",
    # Telecom
    "lumen",
    "lumen technologies",
    # School district
    "olathe public schools",
    "olathe public schools usd #233",
    # Healthcare non-profit
    "planned parenthood great plains",
    # Crane company
    "fairbanks crane llc",
    # Non-law advisory
    "ace advising llc",
    # Clear geographic artifacts (California/PA city names in firm names)
    "rancho cucamonga bankruptcy attorney",
    "harrisburg york carlisle camp hill pa bankruptcy attorney",
}

# Pattern-based removals
_NON_LAW_PATTERNS = re.compile(
    r'\b(?:'
    r'public school|school district|school board|'
    r'crane llc|crane inc|'
    r'packaging company|packaging inc|'
    r'telecom|telecommunications|'
    r'financial corp|financial inc(?! law)(?! legal)|'
    r'trust company(?! law)(?! legal)|'
    r'planned parenthood|'
    r'developments llc(?! law)(?! legal)|'
    r'technologies llc(?! law)(?! legal)'
    r')\b',
    re.IGNORECASE,
)

# Practice area normalization: merge "General Practice" → "General"
GENERAL_PRACTICE_ALIASES = {"general practice", "general practitioner"}


def normalize(name: str) -> str:
    return re.sub(r"[,.']+", "", name.lower()).strip()


def should_remove(row: dict) -> bool:
    name = row.get("law_firm_name", "")
    n = normalize(name)
    if n in REMOVE_EXACT:
        return True
    if _NON_LAW_PATTERNS.search(name):
        # Don't remove if the firm name clearly has law indicators
        law_override = re.search(
            r'\b(law|legal|attorney|lawyers|counsel|llp|pllc|p\.c\.|p\.a\.)\b',
            name, re.IGNORECASE
        )
        if not law_override:
            return True
    return False


rows = list(csv.DictReader(CSV.open()))
fieldnames = list(rows[0].keys())

removed = []
pa_fixed = 0
kept = []

for r in rows:
    if should_remove(r):
        removed.append(r["law_firm_name"])
        continue

    # Normalize "General Practice" → "General"
    if r.get("practice_area", "").strip().lower() in GENERAL_PRACTICE_ALIASES:
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
print(f"\nNormalized {pa_fixed} 'General Practice' → 'General'")
print(f"\nKept {len(kept)} firms")
