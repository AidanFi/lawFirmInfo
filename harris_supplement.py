#!/usr/bin/env python3
"""
Supplementary free-tier discovery (Foursquare + Yelp) for Harris County, TX.

Runs independently of the State Bar registry harvest (texasbar_discover.py)
and writes raw results to a JSON cache. merge_statebar_supplement.py later
folds these in to backfill website/phone on matching State Bar firm rows
and appends any firm found here that isn't already in the registry-derived
CSV (e.g. firm-level listings rather than individual attorneys).
"""
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from scraper.county.config import get_county_config
from scraper.county.foursquare import discover_foursquare
from scraper.county.yelp_places import discover_yelp

CACHE_DIR = Path("data/county")


def main():
    load_dotenv("scraper/.env")
    county_config = get_county_config("harris_tx")
    slug = county_config["slug"]

    firms = []

    fsq_key = os.getenv("FOURSQUARE_API_KEY")
    if fsq_key:
        print("[foursquare] discovering...")
        fsq_firms = discover_foursquare(county_config, fsq_key)
        firms.extend(fsq_firms)
    else:
        print("[foursquare] no API key, skipping")

    yelp_key = os.getenv("YELP_API_KEY")
    if yelp_key:
        print("[yelp] discovering...")
        yelp_firms = discover_yelp(county_config, yelp_key)
        firms.extend(yelp_firms)
    else:
        print("[yelp] no API key, skipping")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CACHE_DIR / f"{slug}_supplement_cache.json"
    out_path.write_text(json.dumps(firms, indent=1))
    print(f"\nWrote {len(firms)} supplementary firm records to {out_path}")


if __name__ == "__main__":
    main()
