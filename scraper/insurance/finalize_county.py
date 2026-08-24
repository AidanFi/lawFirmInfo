"""Generalized version of finalize.py: consolidates known_merged.json +
classified_chunk*.json for any county into its final CSV + manifest entry.

Usage: python3 scraper/insurance/finalize_county.py <county_key> <county_name> <slug> <n_chunks> <valid_cities_csv>
Example: python3 scraper/insurance/finalize_county.py sedgwick "Sedgwick County" sedgwick-county-ks 5 "Wichita,Derby,..."
"""
import csv
import json
import re
import sys
from rapidfuzz import fuzz

DATE_PULLED = "2026-08-24"
COLS = ["agent_name", "agency_name", "company", "agent_type", "offers_auto", "offers_home",
        "website", "phone_number", "email", "street_address", "city", "state", "county",
        "zip_code", "date_pulled", "source"]


TOLL_FREE_PREFIXES = ("800", "888", "877", "866", "855", "844", "833")


def digits(s):
    return re.sub(r"\D", "", s or "")


def is_toll_free(phone_digits: str) -> bool:
    # A shared national toll-free number does NOT imply the same physical
    # office - multi-branch orgs (e.g. Armed Forces Insurance) route several
    # distinct locations through one member-service line. Only treat a
    # toll-free match as a duplicate if the city also matches.
    d = phone_digits[1:] if len(phone_digits) == 11 and phone_digits.startswith("1") else phone_digits
    return d[:3] in TOLL_FREE_PREFIXES


def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\b(insurance|agency|agencies|group|ins|inc|llc|co|the|associates|services)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def main():
    county_dir, county_name, slug, n_chunks, valid_cities = sys.argv[1:6]
    n_chunks = int(n_chunks)
    valid_cities = {c.strip().lower() for c in valid_cities.split(",")}
    out = f"scraper/insurance/output/{county_dir}"
    csv_path = f"app/county-data/insurance-{slug}.csv"

    rows = load(f"{out}/known_merged.json")
    for i in range(1, n_chunks + 1):
        rows.extend(load(f"{out}/classified_chunk{i}.json"))

    # Nationwide-captive contradiction fix (Nationwide has been independent-only nationwide since 2020)
    fixed = 0
    for r in rows:
        if r.get("agent_type") == "Captive" and "nationwide" in (r.get("company") or "").lower():
            r["agent_type"] = "Independent"
            r["company"] = "Independent / Multiple Carriers (incl. Nationwide)"
            fixed += 1

    # Strict city whitelist: drop anything geocoded/verified outside the county's real cities
    in_county = []
    out_of_county = []
    for r in rows:
        if (r.get("city") or "").strip().lower() in valid_cities:
            in_county.append(r)
        else:
            out_of_county.append(r)

    # Dedup pass
    final = []
    seen_phones = {}  # phone -> city, so a toll-free match must also share a city
    for r in in_county:
        p = digits(r.get("phone_number"))
        fn = norm_name(r.get("agency_name") or r.get("agent_name"))
        city = (r.get("city") or "").strip().lower()

        is_dupe = False
        if p and p in seen_phones and (not is_toll_free(p) or seen_phones[p] == city):
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
            seen_phones[p] = city

    final.sort(key=lambda r: ((r.get("agency_name") or r.get("agent_name") or "").lower()))

    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        for r in final:
            row = {c: (r.get(c) or "") for c in COLS}
            row["state"] = "KS"
            row["county"] = county_name.replace(" County", "")
            row["date_pulled"] = DATE_PULLED
            w.writerow(row)

    captive_count = sum(1 for r in final if r.get("agent_type") == "Captive")
    independent_count = sum(1 for r in final if r.get("agent_type") == "Independent")

    print(f"rows_considered={len(rows)} nationwide_fixed={fixed} "
          f"dropped_out_of_county={len(out_of_county)} final={len(final)} "
          f"({captive_count} Captive, {independent_count} Independent)")
    if out_of_county:
        sample = [f"{r.get('agency_name') or r.get('agent_name')} ({r.get('city')})" for r in out_of_county[:15]]
        print("out_of_county sample:", sample)

    return {
        "slug": slug,
        "name": county_name,
        "state": "KS",
        "csv_file": f"insurance-{slug}.csv",
        "captive_count": captive_count,
        "independent_count": independent_count,
    }


if __name__ == "__main__":
    result = main()
    print(json.dumps(result))
