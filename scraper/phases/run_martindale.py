#!/usr/bin/env python3
"""Runner for Martindale-Hubbell enrichment against the existing firm dataset.

Crawls Martindale city pages → attorney profiles → extracts website links,
then fuzzy-matches back to our firms. Free (no API cost).

Usage:
    python -m scraper.phases.run_martindale --test
    python -m scraper.phases.run_martindale
    python -m scraper.phases.run_martindale --cities Wichita,Topeka
"""
import argparse
import json
import shutil
from collections import Counter
from datetime import datetime

from scraper.phases.martindale import scrape_martindale

INPUT_PATH = "app/firms_data.js"
BACKUP_PATH = f"/tmp/firms_data_martindale_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.js"


def _load_firms():
    with open(INPUT_PATH) as f:
        content = f.read()
    return json.loads(content[len("const FIRMS_DATA = "):-1])


def _save_firms(data):
    with open(INPUT_PATH, "w") as f:
        f.write("const FIRMS_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";")


def _build_city_list(firms, min_firms=3):
    """Cities with at least min_firms entries missing a website."""
    cities = Counter()
    for f in firms:
        if f.get("website"):
            continue
        c = (f.get("address") or {}).get("city")
        if c:
            cities[c] += 1
    return sorted(c for c, n in cities.items() if n >= min_firms)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="3 cities only")
    parser.add_argument("--cities", type=str, default=None,
                        help="Comma-separated city list")
    parser.add_argument("--min-firms", type=int, default=3,
                        help="Include cities with >=N firms missing a website")
    parser.add_argument("--max-pages", type=int, default=5,
                        help="Max listing pages per city (pagination cap)")
    parser.add_argument("--delay", type=float, default=1.2,
                        help="Seconds between HTTP requests")
    args = parser.parse_args()

    data = _load_firms()
    firms = data["firms"]
    before_total = len(firms)
    before_web = sum(1 for f in firms if f.get("website"))

    if args.cities:
        cities = [c.strip() for c in args.cities.split(",")]
    elif args.test:
        cities = ["Wichita", "Topeka", "Lawrence"]
    else:
        cities = _build_city_list(firms, min_firms=args.min_firms)

    print(f"[martindale] Will crawl {len(cities)} cities")
    print(f"[martindale] Starting: {before_total} firms, {before_web} with websites")

    shutil.copy(INPUT_PATH, BACKUP_PATH)
    print(f"[martindale] Backed up current data to {BACKUP_PATH}")

    added_websites, new_firms = scrape_martindale(
        firms, cities=cities, delay=args.delay,
        max_pages_per_city=args.max_pages,
        test_mode=args.test,
    )

    after_web = sum(1 for f in firms if f.get("website"))
    data["meta"]["lastMartindale"] = datetime.now().isoformat()
    _save_firms(data)

    print()
    print("=" * 50)
    print("  Martindale Complete")
    print("=" * 50)
    print(f"  Firms:    {before_total}")
    print(f"  Websites: {before_web} -> {after_web} (+{after_web - before_web})")
    print(f"  Reported adds: {added_websites}")
    print("=" * 50)


if __name__ == "__main__":
    main()
