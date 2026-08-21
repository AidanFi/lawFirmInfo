#!/usr/bin/env python3
"""
Second-pass removal of government/institutional legal offices that are not
private referral law firms — prosecutors, public defenders, AG/Governor
offices, legislative/regulatory bodies, bar associations, etc. These slipped
past the original GOVT_PATTERNS regex (which didn't cover prosecutor/AG/PD/
legislative-office naming) and, in the search-merge pass, some got a real
government website attached instead of being excluded.
"""
import csv, re, sys
from pathlib import Path

DATA_DIR = Path("app/county-data")

GOVT_LEGAL_ENTITY_RE = re.compile(
    r'\b(district attorney|attorney general|public defenders?|county counselor|'
    r'county attorney|governor|legislative research|revisor of statutes|'
    r'child advocate|board of indigents?|bar association|ratepayer board|'
    r'u\.?\s?s\.?\s?attorney|united states attorney|appellate defenders?|'
    r'conflicts?\s+(public\s+)?defenders?|conflicts office|death penalty defense|'
    r'trial lawyers association|department for children|dept for children|'
    r'department of revenue|dept of revenue|state treasurer|'
    r'board of nursing|sentencing commission|division of post audit|'
    r'chief clerk|judicial administration|kdads)\b',
    re.IGNORECASE,
)

NAMED_GOVT_OFFICIALS = {"kris william kobach"}

LAW_FIRM_EXEMPT_RE = re.compile(
    r'\b(pllc|p\.l\.l\.c\.|llp|p\.a\.(?!\s)|chartered|chtd|law firm|law office|attorneys at law)\b',
    re.IGNORECASE,
)


def is_govt_legal_entity(name: str) -> bool:
    if name.strip().lower() in NAMED_GOVT_OFFICIALS:
        return True
    return bool(GOVT_LEGAL_ENTITY_RE.search(name))


def process(slug):
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(open(path)))
    fieldnames = list(rows[0].keys())
    before = len(rows)
    removed = [r["law_firm_name"] for r in rows if is_govt_legal_entity(r.get("law_firm_name", ""))]
    kept = [r for r in rows if not is_govt_legal_entity(r.get("law_firm_name", ""))]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(kept)

    print(f"[{slug}] {before} -> {len(kept)} (removed {len(removed)})")
    for n in removed:
        print(f"  - {n}")


if __name__ == "__main__":
    for slug in sys.argv[1:]:
        process(slug)
