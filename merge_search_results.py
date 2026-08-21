#!/usr/bin/env python3
"""
Merge verified real-web-search results (from the ks-firm-website-search workflow)
into the actual county CSVs. Only touches shawnee-county-ks and douglas-county-ks
(the only counties the partial run covered before it was stopped).

Two actions per flagged firm, using row_index to target the exact row:
1. If the agent's note clearly identifies the row as a non-law entity
   (government agency, court office, bank, insurance co, etc.) -> remove the row.
2. Else if a website was found -> merge it in (+ email/practice_area if given),
   and stamp source/date_pulled to reflect real-search verification.
"""
import csv
import json
import re
from pathlib import Path

DATA_DIR = Path("app/county-data")
RESULTS_PATH = Path("/tmp/ks_search_results.json")
TODAY = "2026-08-21"

NON_LAW_NOTE_RE = re.compile(
    r'not a law firm|not an independent|government agency|not a private law|public defense|'
    r'courthouse|bank, not|federal agency|state corrections|insurance company|'
    r'not a business entity|state government|not a business|municipal|teachers.? union|'
    r'military unit|nonprofit|state-affiliated|state judicial|legislative|public transit|'
    r'advocacy association|state law enforcement|not the.*firm|state administrative tribunal|'
    r'court staff|non-law|not a law|court office|not a referral|miscategorized non-legal',
    re.IGNORECASE,
)

PRIORITY_SCORES = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5, "Medical Malpractice": 5,
    "Workers' Compensation": 5, "Workers Compensation": 5,
    "Sexual Assault": 4, "Family Law": 4, "General": 4, "Litigation": 4,
    "Employment Law": 3, "Civil Rights": 3, "Civil Litigation": 3,
    "Estate Planning": 2, "Bankruptcy": 2, "Real Estate": 2, "Business Law": 2,
    "Immigration": 2, "Military Law": 2,
}


def load_results():
    data = json.loads(RESULTS_PATH.read_text())
    by_slug = {}
    for chunk in data:
        by_slug.setdefault(chunk["slug"], []).extend(chunk["results"])
    return by_slug


def merge_county(slug, firm_results):
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(open(path)))
    fieldnames = list(rows[0].keys())

    removed = []
    merged = []
    by_index = {r["row_index"]: r for r in firm_results}

    kept_rows = []
    for i, row in enumerate(rows):
        result = by_index.get(i)
        if result is None:
            kept_rows.append(row)
            continue

        note = result.get("note", "") or ""
        if NON_LAW_NOTE_RE.search(note):
            removed.append((row.get("law_firm_name", ""), note[:100]))
            continue  # drop this row

        website = result.get("website")
        if website:
            row["website"] = website
            if result.get("email"):
                row["email"] = result["email"]
            pa = result.get("practice_area")
            if pa:
                row["practice_area"] = pa
                row["priority"] = str(PRIORITY_SCORES.get(pa, row.get("priority", "4")))
            if "source" in row:
                row["source"] = "KS Courts Registry; website verified via web search"
            if "date_pulled" in row:
                row["date_pulled"] = TODAY
            merged.append((row.get("law_firm_name", ""), website))

        kept_rows.append(row)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(kept_rows)

    print(f"\n[{slug}] {len(rows)} -> {len(kept_rows)} rows")
    print(f"  Removed (confirmed non-law): {len(removed)}")
    for name, note in removed:
        print(f"    - {name}: {note}")
    print(f"  Websites merged: {len(merged)}")
    for name, url in merged:
        print(f"    + {name}: {url}")

    return len(kept_rows), len(removed), len(merged)


if __name__ == "__main__":
    by_slug = load_results()
    for slug, results in by_slug.items():
        merge_county(slug, results)
