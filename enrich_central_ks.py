#!/usr/bin/env python3
"""Run enrich_no_website for all 17 central KS counties."""
import sys
sys.path.insert(0, ".")
from enrich_no_website import enrich_no_website

CENTRAL_KS_SLUGS = [
    "barton-county-ks",
    "clay-county-ks",
    "cloud-county-ks",
    "dickinson-county-ks",
    "ellsworth-county-ks",
    "harvey-county-ks",
    "kingman-county-ks",
    "lincoln-county-ks",
    "marion-county-ks",
    "mcpherson-county-ks",
    "mitchell-county-ks",
    "ottawa-county-ks",
    "reno-county-ks",
    "rice-county-ks",
    "russell-county-ks",
    "saline-county-ks",
    "stafford-county-ks",
]

total_web = 0
total_pa = 0
for slug in CENTRAL_KS_SLUGS:
    print(f"\n{'='*50}")
    print(f"  {slug}")
    print(f"{'='*50}")
    s = enrich_no_website(slug)
    total_web += s["website_found"]
    total_pa += s["practice_updated"]
    print(f"  Summary: websites_found={s['website_found']}, practice_updated={s['practice_updated']}")

print(f"\n{'='*50}")
print(f"GRAND TOTAL: websites_found={total_web}, practice_updated={total_pa}")
