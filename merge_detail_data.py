#!/usr/bin/env python3
"""
Merge the State Bar detail-page harvest (self-reported website +
practice areas per attorney, texasbar_detail_harvest.py) into the county
CSV, using the row_contact_ids sidecar (statebar_to_csv.py) to know which
attorneys fed which row.

For a row with multiple attorneys (a real firm), if ANY member
self-reported a website, use it — prefer one that looks like a firm
homepage over a specific attorney's bio-page URL when multiple exist.
"""
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, ".")
from statebar_to_csv import PRIORITY_MAP

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]

# Self-reported free-text practice areas -> the project's canonical set.
_PA_MAP = [
    (r"criminal", "Criminal Defense"),
    (r"\bdui\b|\bdwi\b|driving while intoxicated", "DUI"),
    (r"personal injury|car accident|auto accident|wrongful death", "Personal Injury"),
    (r"medical malpractice", "Medical Malpractice"),
    (r"workers.?\s*comp", "Workers' Compensation"),
    (r"sexual assault", "Sexual Assault"),
    (r"family law|divorce|child custody|domestic relations", "Family Law"),
    (r"employment|labor law", "Employment Law"),
    (r"civil rights", "Civil Rights"),
    (r"civil litigation|litigation", "Civil Litigation"),
    (r"estate planning|wills.?trusts.?probate|probate|elder law", "Estate Planning"),
    (r"bankruptcy", "Bankruptcy"),
    (r"real estate", "Real Estate"),
    (r"business law|corporate law", "Business Law"),
    (r"immigration", "Immigration"),
    (r"military law", "Military Law"),
]


def normalize_practice_area(raw: str) -> str:
    lower = raw.lower()
    for pattern, canonical in _PA_MAP:
        if re.search(pattern, lower):
            return canonical
    return "General"


def _looks_like_firm_homepage(url: str) -> bool:
    """A bio-page URL (deep path with a person's name / many path segments)
    is less useful as a FIRM website than the site's root — prefer roots,
    but still accept a deep link if that's all we have."""
    parsed = urlparse(url)
    path_segments = [p for p in parsed.path.split("/") if p]
    return len(path_segments) <= 1


def _to_root(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/"


def main(slug: str):
    csv_path = DATA_DIR / f"{slug}.csv"
    sidecar_path = CACHE_DIR / f"{slug}_row_contact_ids.json"
    detail_path = CACHE_DIR / f"{slug}_detail_cache.json"

    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    sidecar = json.loads(sidecar_path.read_text())
    detail = json.loads(detail_path.read_text())

    website_added = 0
    practice_updated = 0

    for r in rows:
        key = f"{r['law_firm_name']}|||{r['city']}"
        contact_ids = sidecar.get(key, [])
        if not contact_ids:
            continue

        member_details = [detail[cid] for cid in contact_ids if cid in detail]

        if not r["website"]:
            websites = [m["website"] for m in member_details if m.get("website")]
            if websites:
                homepages = [w for w in websites if _looks_like_firm_homepage(w)]
                chosen = Counter(homepages).most_common(1)[0][0] if homepages else websites[0]
                if not homepages:
                    chosen = _to_root(chosen)
                r["website"] = chosen
                r["source"] = (r["source"] + "; State Bar self-reported website"
                                if "self-reported" not in r["source"] else r["source"])
                website_added += 1

        if r["practice_area"] == "General":
            areas = [normalize_practice_area(m["practice_areas"]) for m in member_details if m.get("practice_areas")]
            areas = [a for a in areas if a != "General"]
            if areas:
                best = Counter(areas).most_common(1)[0][0]
                r["practice_area"] = best
                r["priority"] = str(PRIORITY_MAP.get(best, PRIORITY_MAP["General"]))
                practice_updated += 1

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"Websites added from self-reported bar profiles: {website_added}")
    print(f"Practice areas updated from self-reported bar profiles: {practice_updated}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 merge_detail_data.py <slug>")
        sys.exit(1)
    main(sys.argv[1])
