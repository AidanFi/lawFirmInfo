"""Round 2: fold verified gap-fill finds (BBB/chamber directory sweep +
Google Places sweep) into the existing Johnson County insurance CSV."""
import csv
import json
import re
from rapidfuzz import fuzz

OUT = "scraper/insurance/output"
CSV_PATH = "app/county-data/insurance-johnson-county-ks.csv"
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


with open(CSV_PATH) as f:
    existing = list(csv.DictReader(f))

with open(f"{OUT}/round2_directory_sweep.json") as f:
    new_rows = json.load(f)
with open(f"{OUT}/round2_google_places_final.json") as f:
    new_rows += json.load(f)

for r in new_rows:
    r["date_pulled"] = DATE_PULLED

existing_phones = {digits(r["phone_number"]) for r in existing if digits(r["phone_number"])}


def is_dupe(r):
    p = digits(r.get("phone_number"))
    if p and p in existing_phones:
        return True
    fn = norm_name(r.get("agency_name") or r.get("agent_name"))
    city = (r.get("city") or "").strip().lower()
    for e in existing:
        if (e.get("city") or "").strip().lower() != city:
            continue
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

print(f"new_rows_considered={len(new_rows)} added={len(added)} duplicates_skipped={len(new_rows) - len(added)}")

final = existing + added
final.sort(key=lambda r: ((r.get("agency_name") or r.get("agent_name") or "").lower()))

with open(CSV_PATH, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=COLS)
    w.writeheader()
    for r in final:
        row = {c: (r.get(c) or "") for c in COLS}
        row["state"] = "KS"
        row["county"] = "Johnson"
        w.writerow(row)

captive_count = sum(1 for r in final if r.get("agent_type") == "Captive")
independent_count = sum(1 for r in final if r.get("agent_type") == "Independent")
print(f"CSV written: {len(final)} rows ({captive_count} Captive, {independent_count} Independent)")

manifest = {
    "insurance": [
        {
            "slug": "johnson-county-ks",
            "name": "Johnson County",
            "state": "KS",
            "csv_file": "insurance-johnson-county-ks.csv",
            "captive_count": captive_count,
            "independent_count": independent_count,
        }
    ]
}
with open("app/county-data/insurance-manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)
    f.write("\n")
print("manifest written")
