"""Consolidate known_merged.json + classified_chunk*.json into the final
Johnson County insurance-agent list: fix the Nationwide-captive contradiction
(Nationwide has sold exclusively through independent agents nationwide since
2020 - confirmed during batch2 research), dedupe across chunks, and write
the final CSV + manifest.
"""
import csv
import json
import re
from rapidfuzz import fuzz

OUT = "scraper/insurance/output"
DATE_PULLED = "2026-08-22"


def load(name):
    with open(f"{OUT}/{name}") as f:
        return json.load(f)


def digits(s):
    return re.sub(r"\D", "", s or "")


def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\b(insurance|agency|agencies|group|ins|inc|llc|co|the|associates|services)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


rows = load("known_merged.json")
for chunk in ["classified_chunk1.json", "classified_chunk2.json", "classified_chunk3.json",
              "classified_chunk4.json", "classified_chunk5.json"]:
    rows.extend(load(chunk))

# Fix Nationwide-captive contradiction: Nationwide has had zero captive/exclusive
# agents anywhere in the US since its July 2020 transition to independent-only
# distribution (confirmed via agency.nationwide.com + industry press in batch2).
fixed = 0
for r in rows:
    if r.get("agent_type") == "Captive" and "nationwide" in (r.get("company") or "").lower():
        r["agent_type"] = "Independent"
        r["company"] = "Independent / Multiple Carriers (incl. Nationwide)"
        fixed += 1
print(f"reclassified {fixed} mislabeled Nationwide 'captive' rows as Independent")

# Final dedup pass across chunks (phone match, or fuzzy name+city match)
final = []
seen_phones = set()
for r in rows:
    p = digits(r.get("phone_number"))
    fn = norm_name(r.get("agency_name") or r.get("agent_name"))
    city = (r.get("city") or "").strip().lower()

    is_dupe = False
    if p and p in seen_phones:
        is_dupe = True
    else:
        for kept in final:
            if (kept.get("city") or "").strip().lower() != city:
                continue
            if fuzz.token_sort_ratio(fn, norm_name(kept.get("agency_name") or kept.get("agent_name"))) >= 92:
                is_dupe = True
                break
    if is_dupe:
        continue
    final.append(r)
    if p:
        seen_phones.add(p)

print(f"rows before final dedup: {len(rows)}  after: {len(final)}")

# Write CSV
cols = ["agent_name", "agency_name", "company", "agent_type", "website", "phone_number",
        "email", "street_address", "city", "state", "county", "zip_code", "date_pulled", "source"]

final.sort(key=lambda r: ((r.get("agency_name") or r.get("agent_name") or "").lower()))

with open("app/county-data/insurance-johnson-county-ks.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for r in final:
        row = {c: (r.get(c) or "") for c in cols}
        row["state"] = "KS"
        row["county"] = "Johnson"
        row["date_pulled"] = DATE_PULLED
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
