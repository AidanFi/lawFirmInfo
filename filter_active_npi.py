#!/usr/bin/env python3
"""Remove deactivated NPI records from all provider CSVs."""
import csv, time, requests
from pathlib import Path

DATA_DIR = Path("app/county-data")
NPI_URL = "https://npiregistry.cms.hhs.gov/api/"

def check_npi_status(npi):
    """Returns 'A' (active), 'D' (deactivated), or None (not found/error)."""
    try:
        r = requests.get(NPI_URL, params={"version":"2.1","number":npi}, timeout=15)
        results = r.json().get("results", [])
        if not results:
            return None
        return results[0].get("basic", {}).get("status")
    except Exception:
        return None

def process_file(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    total = len(rows)
    keep = []
    removed = []

    for i, row in enumerate(rows):
        npi = row.get("npi_number", "").strip()
        if not npi:
            keep.append(row)
            continue

        status = check_npi_status(npi)
        if status == "A" or status is None:  # keep if active or unknown
            keep.append(row)
        else:
            removed.append(row)

        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{total} checked — {len(removed)} deactivated so far")
        time.sleep(0.1)

    if removed:
        print(f"  Removing {len(removed)} deactivated:")
        for r in removed:
            print(f"    {r['provider_name']} — NPI {r['npi_number']}")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(keep)

    return total, len(removed)

def main():
    files = sorted(DATA_DIR.glob("providers-*.csv"))
    grand_total = grand_removed = 0

    for path in files:
        print(f"\n{path.name}")
        total, removed = process_file(path)
        grand_total += total
        grand_removed += removed
        print(f"  {total - removed}/{total} active providers kept")

    print(f"\nDone. {grand_total - grand_removed} active providers across all counties "
          f"({grand_removed} deactivated removed)")

if __name__ == "__main__":
    main()
