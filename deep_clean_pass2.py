#!/usr/bin/env python3
"""Second-pass cleanup: remove remaining non-law firms found in manual review."""

import csv, json, re
from pathlib import Path

def normalize(name: str) -> str:
    """Lowercase, strip commas/punctuation for exact matching."""
    return re.sub(r'[,\.\'\']+', '', name.lower()).strip()

# Exact names to remove (use normalize() for comparison)
REMOVE_EXACT = {
    # Jackson County - non-law businesses confirmed
    "big dog family tree service llc",
    "big parts & equipment",
    "buckner tax services",
    "child's play llc",
    "cohen-esrey",
    "edco aire llc",
    "foxwood services llc",
    "grass pad lee's summit",
    "h & h motors group llc",
    "hcfs inc",
    "ipfs corporation",
    "lawco security co",
    "leo steam llc",
    "lewis earl anderson",
    "luxury leasing & sales co",
    "mr cb llc",
    "my financial home llc",
    "phil good to work llc",
    "sherwin-williams paint store",
    "sleep matters llc",
    "stewart n processing llc",
    "the house of kensington llc",
    "thompson design consultants pc",
    "transit pros",
    "willie lawrence md",
    # Greene County
    "fair grove marine repair llc",
    "r&l envirocare llc",
    # St. Charles County
    "804 technology llc",
    "asher career coaching llc",
    "gateway turf llc",
    "jbloom",
    "jones air & water treatment llc",
    "miller business associates",
    "st paul city office",
    "st peters family medicine llc",
    "the child advocacy center of northeast missouri",
}

# Pattern-based removals for anything not caught by normalize
_EXTRA_PATTERNS = re.compile(
    r'\b(?:tree service|auto parts|tax service|tax prep|security co|'
    r'steam cleaning|insurance agent|leasing.sales|design consultant|'
    r'paint store|sleep.clinic|marine repair|envirocare|technology llc|'
    r'career coaching|turf llc|water treatment|city office|family medicine|'
    r'child advocacy center)\b',
    re.IGNORECASE,
)

def should_remove(name: str) -> bool:
    n = normalize(name)
    if n in REMOVE_EXACT:
        return True
    if _EXTRA_PATTERNS.search(name):
        return True
    return False

def load_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def update_manifest(slug, count):
    mpath = Path("app/county-data/manifest.json")
    data = json.loads(mpath.read_text())
    from datetime import date
    today = date.today().isoformat()
    for c in data["counties"]:
        if c["slug"] == slug:
            c["firm_count"] = count
            c["last_updated"] = today
            break
    mpath.write_text(json.dumps(data, indent=2) + "\n")

slugs = ["jackson-county-mo", "greene-county-mo", "st-charles-county-mo"]

for slug in slugs:
    path = Path(f"app/county-data/{slug}.csv")
    rows = load_csv(path)
    fieldnames = list(rows[0].keys()) if rows else []
    before = len(rows)

    kept, removed = [], []
    for r in rows:
        name = r["law_firm_name"]
        if should_remove(name):
            removed.append(name)
        else:
            kept.append(r)

    save_csv(path, kept, fieldnames)
    update_manifest(slug, len(kept))
    print(f"\n{slug}: {before} → {len(kept)} (removed {len(removed)})")
    for n in sorted(removed):
        print(f"  - {n}")

print("\nDone.")
