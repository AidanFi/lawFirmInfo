"""Append a round of new verified finds to an existing county CSV, with dedup.

Usage: python3 scraper/insurance/append_round.py <slug> <new_records_json> <county_name>
"""
import csv
import json
import re
import sys
from rapidfuzz import fuzz

DATE_PULLED = "2026-08-23"
COLS = ["agent_name", "agency_name", "company", "agent_type", "website", "phone_number",
        "email", "street_address", "city", "state", "county", "zip_code", "date_pulled", "source"]


def digits(s):
    return re.sub(r"\D", "", s or "")


def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\b(insurance|agency|agencies|group|ins|inc|llc|co|the|associates|services)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


def main():
    slug, new_json, county_name = sys.argv[1:4]
    csv_path = f"app/county-data/insurance-{slug}.csv"

    with open(csv_path) as f:
        existing = list(csv.DictReader(f))

    with open(new_json) as f:
        new_rows = json.load(f)
    for r in new_rows:
        r["date_pulled"] = DATE_PULLED

    existing_phones = {digits(r["phone_number"]) for r in existing if digits(r["phone_number"])}

    def is_dupe(r):
        p = digits(r.get("phone_number"))
        if p and p in existing_phones:
            return True
        fn = norm_name(r.get("agency_name") or r.get("agent_name"))
        addr = (r.get("street_address") or "").strip().lower()
        city = (r.get("city") or "").strip().lower()
        for e in existing:
            if (e.get("city") or "").strip().lower() != city:
                continue
            if addr and addr == (e.get("street_address") or "").strip().lower():
                return True
            if fuzz.token_sort_ratio(fn, norm_name(e.get("agency_name") or e.get("agent_name"))) >= 92:
                return True
        return False

    added = []
    for r in new_rows:
        if is_dupe(r):
            continue
        added.append(r)
        p = digits(r.get("phone_number"))
        if p:
            existing_phones.add(p)

    print(f"{slug}: new_considered={len(new_rows)} added={len(added)} duplicates_skipped={len(new_rows) - len(added)}")

    final = existing + added
    final.sort(key=lambda r: ((r.get("agency_name") or r.get("agent_name") or "").lower()))

    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        for r in final:
            row = {c: (r.get(c) or "") for c in COLS}
            row["state"] = "KS"
            row["county"] = county_name
            w.writerow(row)

    captive_count = sum(1 for r in final if r.get("agent_type") == "Captive")
    independent_count = sum(1 for r in final if r.get("agent_type") == "Independent")
    print(f"{slug}: final={len(final)} ({captive_count} Captive, {independent_count} Independent)")
    print(json.dumps({"captive_count": captive_count, "independent_count": independent_count, "total": len(final)}))


if __name__ == "__main__":
    main()
