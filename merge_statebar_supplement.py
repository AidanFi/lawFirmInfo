#!/usr/bin/env python3
"""
Merge the Foursquare/Yelp supplement cache into the State-Bar-derived
Harris County CSV: backfill website/phone on matching rows (by
name-similarity + city), and append any firm-level listing discovered
there that has no match among the individual-attorney-derived rows.
"""
import csv
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, ".")
from scraper.utils.normalize import are_same_firm
from statebar_to_csv import is_non_law
from scraper.county.enhance import (
    _LEGAL_RE, _LEGAL_SUFFIX_RE, _NON_LEGAL_INDICATORS, _NONLEGAL_RE,
)


_EXTRA_NON_LEGAL = (
    "bookkeeping", "accounting", "cpa", "notary", "tax service",
    "tax preparation", "consulting", "escrow", "title company",
)


def _is_plausible_law_name(name: str) -> bool:
    """Strict check for appending a BRAND NEW firm sourced only from
    Foursquare — deliberately does NOT trust "came from a legal-category
    API search" the way scraper.county.enhance's merge logic does for
    established multi-source pipelines, since that same category
    filtering was independently found to include consumer-finance/retail
    chains that Foursquare mistags. A bare entity suffix (LLC/PC/PA) is
    NOT enough on its own — that matches any small business — so this
    requires an explicit legal keyword in the name."""
    lower = name.lower()
    if any(ind in lower for ind in _NON_LEGAL_INDICATORS) or any(ind in lower for ind in _EXTRA_NON_LEGAL):
        return False
    if _NONLEGAL_RE.search(lower):
        return False
    return bool(_LEGAL_RE.search(lower))

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]


def main(slug: str):
    csv_path = DATA_DIR / f"{slug}.csv"
    supp_path = CACHE_DIR / f"{slug}_supplement_cache.json"

    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    supplement = json.loads(supp_path.read_text())
    print(f"Base CSV: {len(rows)} rows. Supplement: {len(supplement)} firms.")

    today = date.today().isoformat()
    backfilled_website = 0
    backfilled_phone = 0
    appended = 0

    rows_by_city = {}
    for r in rows:
        rows_by_city.setdefault(r["city"].lower(), []).append(r)

    for sf in supplement:
        addr = sf.get("address") or {}
        city = (addr.get("city") or "").lower()
        name = sf.get("name", "")
        if not name:
            continue

        matched = None
        for r in rows_by_city.get(city, []):
            if are_same_firm(name, r["law_firm_name"]):
                matched = r
                break

        if matched:
            if not matched.get("website") and sf.get("website"):
                matched["website"] = sf["website"]
                backfilled_website += 1
            if not matched.get("phone_number") and sf.get("phone"):
                matched["phone_number"] = sf["phone"]
                backfilled_phone += 1
            src = matched.get("source", "")
            extra = "+".join(sf.get("sources", []))
            if extra and extra not in src:
                matched["source"] = f"{src}; {extra}" if src else extra
        else:
            if not sf.get("phone") and not sf.get("website"):
                continue
            if is_non_law(name) or not _is_plausible_law_name(name):
                continue
            new_row = {
                "law_firm_name": name,
                "website": sf.get("website") or "",
                "google_business_profile": sf.get("google_business_profile") or "",
                "legal_directory_listing": "",
                "city": addr.get("city", ""),
                "state": addr.get("state", "TX"),
                "county": "Harris",
                "phone_number": sf.get("phone") or "",
                "email": sf.get("email") or "",
                "practice_area": "General",
                "street_address": addr.get("street", ""),
                "zip_code": addr.get("zip", ""),
                "msa": "Houston",
                "priority": "4",
                "number_of_lawyers": "",
                "date_pulled": today,
                "source": "+".join(sf.get("sources", [])) or "Foursquare/Yelp",
            }
            rows.append(new_row)
            rows_by_city.setdefault(city, []).append(new_row)
            appended += 1

    rows.sort(key=lambda r: (r["city"], r["law_firm_name"]))

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Backfilled {backfilled_website} websites, {backfilled_phone} phones.")
    print(f"Appended {appended} new firms not in the State Bar registry pass.")
    print(f"Final: {len(rows)} rows written to {csv_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 merge_statebar_supplement.py <slug>")
        sys.exit(1)
    main(sys.argv[1])
