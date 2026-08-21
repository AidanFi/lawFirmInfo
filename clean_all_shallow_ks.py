#!/usr/bin/env python3
"""
Generalized non-law-entity removal + dedupe for the 70 "shallow" KS counties
(bulk kscourts-import only, never individually deep-cleaned).

Reuses the patterns from final_cleanup.py (GOVT_PATTERNS, LAW_INDICATORS,
NON_LAW_EXACT) but applies them to every target county, and adds a broader
set of exact non-law names discovered across rural counties.
"""
import csv, re
from pathlib import Path

DATA_DIR = Path("app/county-data")

NON_LAW_EXACT = {
    "temps disposal service",
    "harper county counselor",
    "harper county district court",
    "county counselor",
    "county attorney",
    "county attorney's office",
    "district attorney",
    "district attorney's office",
    "county treasurer",
    "county clerk",
    "register of deeds",
    "clerk of the district court",
    "public defender",
    "public defender's office",
    "board of county commissioners",
}

GOVT_PATTERNS = re.compile(
    r'\b(city clerk|city hall|city of |county clerk|county treasurer|county counselor|'
    r'department of |dept\. of |public library|fire station|police department|fire department|'
    r'sheriff(?!.*(law|attorney|legal))|district court|circuit court|municipal court|probate court|'
    r'judicial district|us district court|u\.s\. district|united states district|'
    r'board of (county )?commissioners|register of deeds|unified school district|school district)\b',
    re.IGNORECASE
)

LAW_INDICATORS = ['law', 'legal', 'attorney', 'attorneys', 'counsel', 'llp', 'pllc',
                   'p.c.', 'p.a.', 'esq', 'mediator', 'mediation', 'arbitr', 'counselor at law']

NON_LAW_KEYWORDS = re.compile(
    r'\b(disposal service|trucking|construction|insurance agency|real estate|realty|'
    r'bank\b|credit union|veterinary|auto parts|hardware|restaurant|dental|medical center|'
    r'hospital|health system|chiropractic|physical therapy)\b',
    re.IGNORECASE
)


def is_non_law(name: str) -> bool:
    lname = name.strip().lower()
    if lname in NON_LAW_EXACT:
        return True
    has_law_indicator = any(kw in lname for kw in LAW_INDICATORS)
    if not has_law_indicator and GOVT_PATTERNS.search(name):
        return True
    if not has_law_indicator and NON_LAW_KEYWORDS.search(name):
        return True
    return False


def deduplicate(rows):
    seen = set()
    kept = []
    for row in rows:
        key = (row.get("law_firm_name", "").strip().lower(),
               row.get("city", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        kept.append(row)
    return kept


def process_county(slug):
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        return None
    rows = list(csv.DictReader(open(path)))
    if not rows:
        return (slug, 0, 0, 0, 0, [])
    fieldnames = list(rows[0].keys())
    original = len(rows)

    before = len(rows)
    removed_names = [r["law_firm_name"] for r in rows if is_non_law(r.get("law_firm_name", ""))]
    rows = [r for r in rows if not is_non_law(r.get("law_firm_name", ""))]
    removed_non_law = before - len(rows)

    before = len(rows)
    rows = deduplicate(rows)
    removed_dups = before - len(rows)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    return (slug, original, len(rows), removed_non_law, removed_dups, removed_names)


if __name__ == "__main__":
    import sys
    slugs = sys.argv[1:]
    total_removed = 0
    for slug in slugs:
        result = process_county(slug)
        if result is None:
            print(f"  [skip] {slug}: not found")
            continue
        slug, original, final, removed_non_law, removed_dups, removed_names = result
        total_removed += removed_non_law + removed_dups
        print(f"  {slug}: {original} -> {final} (removed {removed_non_law} non-law, {removed_dups} dups)")
        for n in removed_names:
            print(f"      - removed: {n}")
    print(f"\nTotal rows removed across {len(slugs)} counties: {total_removed}")
