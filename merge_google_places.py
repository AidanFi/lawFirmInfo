#!/usr/bin/env python3
"""Run Google Places discovery and MERGE into the already-curated county CSVs.

Non-destructive: enriches existing firms with missing website/phone/GBP/email
and adds genuinely-new in-county law firms. Reuses the deep_clean removal
denylist so previously-removed contamination does not return. For Spring Hill
(straddles Johnson/Miami) only EXISTING entries are enriched — no new adds —
to avoid pulling in Johnson-County firms.

Usage: python3 merge_google_places.py <county_key> [<county_key> ...]
"""
import csv
import sys
from pathlib import Path

from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent))
from scraper.county.config import get_county_config, get_priority
from scraper.county.google_places import discover_google
from scraper.utils.normalize import are_same_firm, normalize_firm_name
from clean_county import _is_likely_law_firm
from deep_clean_miami_linn import CONFIG as DC

DATA_DIR = Path("app/county-data")
NO_ADD_CITIES = {"spring hill"}  # enrich-only (Johnson/Miami overlap)

# extra government / non-referral name guards (Google can surface these)
_GOV = ("district court", "courthouse", "county attorney", "attorney general",
        "city of ", "clerk of", "register of deeds", "public defender")


def _norm(s: str) -> str:
    return normalize_firm_name(s or "")


def merge_county(key: str):
    cfg = get_county_config(key)
    slug = cfg["slug"]
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(path.open()))
    fieldnames = list(rows[0].keys())

    # denylist of names removed during deep clean (normalized)
    removed = {_norm(n) for n in DC.get(slug, {}).get("remove", set())}

    google = discover_google(cfg, os.getenv("GOOGLE_MAPS_API_KEY"))

    enriched = 0
    added = 0
    add_names = []
    for g in google:
        gname = g.get("name", "")
        gcity = (g.get("address") or {}).get("city", "")
        if not gname:
            continue

        # find existing match (same city, fuzzy name)
        match = None
        for r in rows:
            if r["city"].lower() == gcity.lower() and are_same_firm(gname, r["law_firm_name"]):
                match = r
                break

        if match:
            changed = False
            if not match["website"].strip() and g.get("website"):
                match["website"] = g["website"]; changed = True
            if not match["phone_number"].strip() and g.get("phone"):
                match["phone_number"] = g["phone"]; changed = True
            if not match["google_business_profile"].strip() and g.get("google_business_profile"):
                match["google_business_profile"] = g["google_business_profile"]; changed = True
            addr = g.get("address") or {}
            if not match["street_address"].strip() and addr.get("street"):
                match["street_address"] = addr["street"]; changed = True
            if not match["zip_code"].strip() and addr.get("zip"):
                match["zip_code"] = addr["zip"]; changed = True
            if changed:
                enriched += 1
            continue

        # new firm — apply guards
        if _norm(gname) in removed:
            continue
        if gcity.lower() in NO_ADD_CITIES:
            continue
        if not _is_likely_law_firm(gname):
            continue
        if any(p in gname.lower() for p in _GOV):
            continue

        addr = g.get("address") or {}
        new = {k: "" for k in fieldnames}
        new.update({
            "law_firm_name": gname,
            "website": g.get("website") or "",
            "google_business_profile": g.get("google_business_profile") or "",
            "legal_directory_listing": "",
            "city": gcity,
            "state": "KS",
            "county": cfg["name"].replace(" County", ""),
            "phone_number": g.get("phone") or "",
            "email": "",
            "practice_area": "General",
            "street_address": addr.get("street") or "",
            "zip_code": addr.get("zip") or "",
            "msa": cfg["msa"],
            "priority": "4",
            "number_of_lawyers": "",
        })
        rows.append(new)
        added += 1
        add_names.append(f"{gname} ({gcity})")

    # recompute priority, re-sort
    for r in rows:
        r["priority"] = str(get_priority(r.get("practice_area", "").strip() or "General"))
    rows.sort(key=lambda r: (r["city"], r["law_firm_name"]))

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"\n=== {slug}: enriched {enriched}, added {added}, total {len(rows)} ===")
    for n in add_names:
        print(f"    + {n}")


if __name__ == "__main__":
    load_dotenv("scraper/.env")
    keys = sys.argv[1:] or ["miami", "linn"]
    for k in keys:
        merge_county(k)
