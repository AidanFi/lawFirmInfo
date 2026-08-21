#!/usr/bin/env python3
"""
Add date_pulled + source columns to the 70 shallow-KS-county CSVs touched in
this session's deep-scrape sweep. Compares each row against the git HEAD
baseline (pre-sweep state) to describe where phone/email actually came from.

Usage: python3 stamp_provenance.py <slug> [<slug> ...]
"""
import csv
import subprocess
import sys
from io import StringIO
from pathlib import Path

DATA_DIR = Path("app/county-data")
TODAY = "2026-08-17"


def load_baseline(slug: str) -> dict:
    """name|city -> row, from git HEAD. Empty dict if file is new/untracked."""
    try:
        text = subprocess.run(
            ["git", "show", f"HEAD:app/county-data/{slug}.csv"],
            capture_output=True, text=True, check=True,
        ).stdout
    except subprocess.CalledProcessError:
        return {}
    rows = list(csv.DictReader(StringIO(text)))
    out = {}
    for r in rows:
        key = (r.get("law_firm_name", "").strip().lower(), r.get("city", "").strip().lower())
        out[key] = r
    return out


def stamp(slug: str):
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        print(f"  [skip] {slug}: not found")
        return
    rows = list(csv.DictReader(open(path)))
    baseline = load_baseline(slug)

    for row in rows:
        key = (row.get("law_firm_name", "").strip().lower(), row.get("city", "").strip().lower())
        base = baseline.get(key, {})
        had_email = bool(base.get("email", "").strip())
        had_website = bool(base.get("website", "").strip())
        has_email = bool(row.get("email", "").strip())
        has_website = bool(row.get("website", "").strip())

        parts = ["KS Courts Registry"]
        if has_website and not had_website:
            parts.append("website via DuckDuckGo/Google Business Profile search")
        if has_email:
            parts.append("email via firm website scrape")

        row["source"] = "; ".join(parts)
        row["date_pulled"] = TODAY

    fieldnames = list(rows[0].keys()) if rows else [
        "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
        "city", "state", "county", "phone_number", "email", "practice_area",
        "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
        "date_pulled", "source",
    ]
    if rows and "date_pulled" not in fieldnames:
        fieldnames = fieldnames + ["date_pulled", "source"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  {slug}: stamped {len(rows)} rows")


if __name__ == "__main__":
    for slug in sys.argv[1:]:
        stamp(slug)
